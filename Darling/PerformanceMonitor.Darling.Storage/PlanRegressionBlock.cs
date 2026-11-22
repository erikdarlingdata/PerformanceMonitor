/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The rolling pre-aggregate of <c>collect.plan_regression_daily</c> (#9225): the sums of whole blocks of days at the grain the
/// PLAN_REGRESSION read already groups by (database, query, plan, replica, plan hash), kept by the same builder that keeps the
/// daily rows. The read takes a whole block where it took <see cref="BlockDays"/> sets of daily rows, and the daily rows only for the days
/// at the edges of its window. The daily rows stay the source and the fallback: a store with no block, a missing table or a block
/// that is not valid reads exactly as it did before.
///
/// <para><b>Why <see cref="BlockDays"/> is 4.</b> The read's window is the 14 days before the analysis window plus the day the edge falls
/// in, so up to 15 days, and an anchored read's first day moves with its as_of, so the blocks cannot line up with the window. Blocks are
/// aligned to a fixed day number (<see cref="BlockStart"/>), and a window of 15 days always holds at least three whole blocks of 4
/// (a block of 5 only guarantees two, a block of 7 only one). The worst case reads 3 block sets and 3 loose days where it read 15 day sets;
/// the average window reads about 6. A longer block saves less on the edges, a shorter one saves less in the middle.</para>
///
/// <para><b>Validity.</b> A block row set is the sum of the daily rows of its days, and it is stamped (<c>day_seqs</c>) with each day's
/// <c>built_seq</c> at the moment it was summed. It is valid only while every one of its days is still a whole build
/// (<c>built_seq = late_seq</c>) and still carries the <c>built_seq</c> the block was summed from: a late row moves <c>late_seq</c>, and
/// the day's rebuild stamps a new <c>built_seq</c>, so either one makes the block stale until the next pass sums it again. This is the
/// rule <c>plan_regression_daily_built</c> applies to a day, applied to a block of days. Only whole days count, so a block never holds an
/// open day.</para>
///
/// <para><b>Same values.</b> The call count and the CPU and duration totals are sums of the same numerics, the last execution is the
/// maximum, the forced flag is a boolean or and the failure count a maximum. The rows figure keeps the daily rule: it is NULL when any day
/// of the plan has none, so a block's figure is NULL exactly when one of its days' is.</para>
/// </summary>
public static class PlanRegressionBlock
{
    /// <summary>The block table, as its sibling spells its own (<see cref="PlanRegressionDaily.TableName"/>).</summary>
    public const string TableName = "collect.plan_regression_block";

    /// <summary>The per-block stamp table.</summary>
    public const string BuiltTableName = "collect.plan_regression_block_built";

    /// <summary>The store schema version of the rung that creates the two tables (V295, #9225).</summary>
    public const int Version = 295;

    /// <summary>The days in one block. Why 4 is in the type's summary.</summary>
    public const int BlockDays = 4;

    /// <summary>The most blocks one tick sums. A block is the sum of four days of one server's daily rows, about a second on a large
    /// store, so this is a bounded share of the tick. An existing store has up to four blocks a server to fill from its daily rows,
    /// and 24 a tick (the tick is hourly) fills 100 servers in about a day without a long start.</summary>
    public const int MaxBuildsPerTick = 24;

    // The client deadline sits past the server's statement_timeout so the server's limit decides (#9225).
    private const int CommandTimeoutSeconds = PlanRegressionDaily.BuildStatementTimeoutSeconds + 30;

    /// <summary>
    /// The two tables as the V295 rung runs them. Idempotent and empty when created: no data moves, the builder fills the blocks as it
    /// runs. Editing this text edits a shipped rung: add a new rung instead. The column types follow <c>plan_regression_daily</c>, whose
    /// sums are numeric.
    /// </summary>
    public const string CreateSql = @"
/* V295 (#9225): the rolling pre-aggregate of collect.plan_regression_daily, blocks of whole days at the grain the PLAN_REGRESSION read
   groups by, and the stamp that says which daily builds each block was summed from. Naive UTC. The builder owns every write. */
CREATE TABLE IF NOT EXISTS collect.plan_regression_block
(
    server_id integer NOT NULL,
    block_start date NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    replica_role text,
    query_plan_hash text,
    execs numeric,
    cpu_us_sum numeric,
    dur_us_sum numeric,
    last_exec timestamp,
    is_forced_plan boolean,
    force_failure_count bigint,
    rowcount_sum numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_plan_regression_block
ON collect.plan_regression_block (server_id, block_start, database_name, query_id, plan_id, replica_role, query_plan_hash)
NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS collect.plan_regression_block_built
(
    server_id integer NOT NULL,
    block_start date NOT NULL,
    day_seqs bigint[] NOT NULL,
    built_at timestamp,
    source_rows bigint,
    CONSTRAINT pk_plan_regression_block_built PRIMARY KEY (server_id, block_start)
);";

    /// <summary>The start of the block <paramref name="day"/> belongs to: blocks are aligned to the day number, so every reader and the
    /// builder cut the calendar the same way.</summary>
    public static DateOnly BlockStart(DateOnly day) => day.AddDays(-(day.DayNumber % BlockDays));

    /// <summary>
    /// The blocks due now. $1 the first block start (date), $2 the last (date), $3 the cap. A block is due when all of its
    /// <see cref="BlockDays"/> days are whole valid builds, at or after the day the server's coverage claim starts, and the block has no stamp or its stamp
    /// no longer equals the days' <c>built_seq</c>. Blocks never summed first, then the oldest stamp, then the newest block. This one
    /// statement is also the backfill of an existing store: its complete blocks have no stamp, so they are due, <see cref="MaxBuildsPerTick"/>
    /// at a time.
    /// </summary>
    public static readonly string PlanSql = $@"
WITH s AS
(
    SELECT c.server_id, c.filled_since
    FROM collect.query_store_interval_latest_coverage AS c
    JOIN collect.servers AS sv ON sv.server_id = c.server_id
    WHERE sv.is_enabled
    AND   NOT EXISTS (SELECT 1 FROM collect.query_store_interval_latest_pending AS p WHERE p.server_id = c.server_id)
),
cand AS
(
    SELECT s.server_id, s.filled_since, g::date AS block_start
    FROM s
    CROSS JOIN LATERAL generate_series($1::date, $2::date, interval '{BlockDays} days') AS g
),
cur AS
(
    SELECT c.server_id, c.block_start, k.built_at, k.day_seqs AS recorded,
           (SELECT array_agg(b.built_seq ORDER BY b.day)
            FROM collect.plan_regression_daily_built AS b
            WHERE b.server_id = c.server_id
            AND   b.day >= c.block_start
            AND   b.day < c.block_start + {BlockDays}
            AND   b.built_seq = b.late_seq
            AND   b.day >= c.filled_since::date + 1) AS current_seqs
    FROM cand AS c
    LEFT JOIN collect.plan_regression_block_built AS k
      ON k.server_id = c.server_id AND k.block_start = c.block_start
),
due AS
(
    SELECT x.server_id, x.block_start,
           row_number() OVER (ORDER BY x.built_at ASC NULLS FIRST, x.block_start DESC, x.server_id) AS slot
    FROM cur AS x
    WHERE cardinality(x.current_seqs) = {BlockDays}
    AND   x.recorded IS DISTINCT FROM x.current_seqs
)
SELECT due.server_id, due.block_start
FROM due
WHERE due.slot <= $3::integer
ORDER BY due.slot;";

    /// <summary>The <c>built_seq</c> of each of the block's days as the builder sees it under the server's lock, and how many of them are
    /// whole valid builds. $1 server_id, $2 block start. The count is rechecked here because the plan was read before the lock.</summary>
    public static readonly string SeqsSql = $@"
SELECT array_agg(b.built_seq ORDER BY b.day), count(*)::integer
FROM collect.plan_regression_daily_built AS b
WHERE b.server_id = $1::integer
AND   b.day >= $2::date
AND   b.day < $2::date + {BlockDays}
AND   b.built_seq = b.late_seq;";

    /// <summary>Clears one block's sums before they are summed again. $1 server_id, $2 block start.</summary>
    public const string DeleteBlockSql = @"
DELETE FROM collect.plan_regression_block
WHERE server_id = $1::integer
AND   block_start = $2::date;";

    /// <summary>
    /// Sums one block from the daily rows of its days. $1 server_id, $2 block start. Returns the rows inserted. The grain is the unique key
    /// of <c>plan_regression_daily</c> without its day, so a plan that ran on several of the days is one row. The rows figure is NULL
    /// when any day's row has none, the rule the read's <c>plan_agg</c> applies across days.
    /// </summary>
    public static readonly string BuildBlockSql = $@"
WITH built AS
(
    INSERT INTO collect.plan_regression_block
    (
        server_id, block_start, database_name, query_id, plan_id, replica_role, query_plan_hash,
        execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count, rowcount_sum
    )
    SELECT
        $1::integer, $2::date, d.database_name, d.query_id, d.plan_id, d.replica_role, d.query_plan_hash,
        SUM(d.execs), SUM(d.cpu_us_sum), SUM(d.dur_us_sum), MAX(d.last_exec), bool_or(d.is_forced_plan), MAX(d.force_failure_count),
        CASE WHEN COUNT(*) = COUNT(d.rowcount_sum) THEN SUM(d.rowcount_sum) END
    FROM collect.plan_regression_daily AS d
    WHERE d.server_id = $1::integer
    AND   d.day >= $2::date
    AND   d.day < $2::date + {BlockDays}
    GROUP BY d.database_name, d.query_id, d.plan_id, d.replica_role, d.query_plan_hash
    RETURNING 1
)
SELECT count(*)::bigint FROM built;";

    /// <summary>Stamps a summed block. $1 server_id, $2 block start, $3 the days' <c>built_seq</c> (bigint[]), $4 built_at, $5 the rows inserted.</summary>
    public const string MarkBuiltSql = @"
INSERT INTO collect.plan_regression_block_built (server_id, block_start, day_seqs, built_at, source_rows)
VALUES ($1::integer, $2::date, $3::bigint[], $4::timestamp, $5::bigint)
ON CONFLICT (server_id, block_start) DO UPDATE
SET day_seqs = EXCLUDED.day_seqs,
    built_at = EXCLUDED.built_at,
    source_rows = EXCLUDED.source_rows;";

    /// <summary>Removes the stamps of expired blocks (a block that starts before the oldest day the daily cleanup keeps, and every stamp of a
    /// server that is not enabled) and returns their keys, which name the block rows to remove (<see cref="GcBlockSql"/>). $1 now. Driven from
    /// the small stamp table for the reason <see cref="PlanRegressionDaily.GcTotalsSql"/> gives.</summary>
    public static readonly string GcBuiltSql = $@"
DELETE FROM collect.plan_regression_block_built
WHERE block_start < ($1::timestamp - interval '{PlanRegressionDaily.MarkLateWindowDays} days')::date
   OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
RETURNING server_id, block_start;";

    /// <summary>Removes one expired block's sums. $1 server_id, $2 block start.</summary>
    public const string GcBlockSql = DeleteBlockSql;

    /// <summary>
    /// The blocks the PLAN_REGRESSION read may take, as a list of block starts. $1 server_id, $2 the window's day-aligned start. A block is valid
    /// when each of its days is still a whole build and still carries the <c>built_seq</c> the block was summed from. The read runs this in
    /// the snapshot of the built-days read and the daily read, so the stamp, the sums and the days are one view.
    /// </summary>
    public static readonly string ValidBlocksSql = $@"
SELECT k.block_start
FROM collect.plan_regression_block_built AS k
WHERE k.server_id = $1::integer
AND   k.block_start >= $2::date
AND   (SELECT count(*)
       FROM collect.plan_regression_daily_built AS b
       WHERE b.server_id = k.server_id
       AND   b.day >= k.block_start
       AND   b.day < k.block_start + {BlockDays}
       AND   b.built_seq = b.late_seq
       AND   b.built_seq = k.day_seqs[(b.day - k.block_start) + 1]) = {BlockDays}
ORDER BY k.block_start;";

    /// <summary>What one pass of block sums did.</summary>
    public readonly record struct BlockStepResult(int Built, int Skipped, int Failed, int Removed);

    /// <summary>
    /// Which blocks the read takes and which days it still reads from the daily rows. A block is taken only when every one of its days is a
    /// whole built day in <paramref name="builtDays"/> (the list the read already filtered: valid, past the coverage claim and, for an anchored
    /// read, whole by its end), so a block never reaches a day the read would not take from the totals. The days the blocks cover leave the daily list.
    /// </summary>
    public static (List<DateOnly> Blocks, List<DateOnly> Days) Select(
        IReadOnlyList<PlanRegressionDaily.BuiltDay> builtDays, IReadOnlyCollection<DateOnly> validBlocks)
    {
        ArgumentNullException.ThrowIfNull(builtDays);
        ArgumentNullException.ThrowIfNull(validBlocks);

        var whole = builtDays.Where(b => b.Through is null).Select(b => b.Day).ToHashSet();
        var covered = new HashSet<DateOnly>();
        var blocks = new List<DateOnly>();
        foreach (var start in validBlocks.OrderBy(b => b))
        {
            var days = Enumerable.Range(0, BlockDays).Select(start.AddDays).ToList();
            if (days.All(whole.Contains) && !days.Any(covered.Contains))
            {
                blocks.Add(start);
                covered.UnionWith(days);
            }
        }

        return (blocks, builtDays.Select(b => b.Day).Where(d => !covered.Contains(d)).ToList());
    }

    /// <summary>
    /// Reads the valid block starts inside a savepoint of <paramref name="transaction"/>: a failed read (a store whose rung has not run
    /// has no stamp table, or a timeout) rolls back to the savepoint and answers with none, so the read goes on with the daily rows as it did
    /// before blocks existed and the snapshot is not aborted. A cancelled pass is not swallowed.
    /// </summary>
    public static async Task<List<DateOnly>> ReadValidBlocksAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime windowFloor, int commandTimeoutSeconds,
        ILogger? logger, Func<Exception, bool> isExpectedAbandon, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(isExpectedAbandon);

        const string savepoint = "plan_regression_blocks";
        await transaction.SaveAsync(savepoint, cancellationToken);
        var blocks = new List<DateOnly>();
        try
        {
            await using var cmd = new NpgsqlCommand(ValidBlocksSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = DateOnly.FromDateTime(windowFloor) });
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    blocks.Add(reader.GetFieldValue<DateOnly>(0));
                }
            }

            await transaction.ReleaseAsync(savepoint, cancellationToken);
            return blocks;
        }
        catch (Exception ex) when (ex is not OperationCanceledException && !isExpectedAbandon(ex))
        {
            logger?.LogWarning(ex, "PLAN_REGRESSION blocks for server {ServerId}: the block read failed, so the daily rows are read as before: {Message}", serverId, ex.Message);
            await transaction.RollbackAsync(savepoint, cancellationToken);
            return [];
        }
    }

    /// <summary>
    /// Sums one block under the server's advisory lock, in one transaction: rechecks that its days are whole valid builds and reads their
    /// <c>built_seq</c>, clears the block's rows, sums the days, stamps the block. Returns the rows inserted, or null when the lock was not free
    /// or a day stopped being a whole valid build since the plan (the next pass plans it again).
    /// </summary>
    public static async Task<long?> BuildBlockAsync(
        NpgsqlConnection connection, int serverId, DateOnly blockStart, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand($"SET LOCAL statement_timeout = '{PlanRegressionDaily.BuildStatementTimeoutSeconds}s'", connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var @lock = new NpgsqlCommand(PlanRegressionDaily.LockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            @lock.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            if (!(bool)(await @lock.ExecuteScalarAsync(cancellationToken))!)
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        long[]? seqs;
        int whole;
        await using (var read = new NpgsqlCommand(SeqsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerBlock(read, serverId, blockStart);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            seqs = reader.IsDBNull(0) ? null : reader.GetFieldValue<long[]>(0);
            whole = reader.GetInt32(1);
        }

        if (whole != BlockDays || seqs is null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return null;
        }

        await using (var delete = new NpgsqlCommand(DeleteBlockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerBlock(delete, serverId, blockStart);
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        long rows;
        await using (var insert = new NpgsqlCommand(BuildBlockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerBlock(insert, serverId, blockStart);
            rows = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken));
        }

        await using (var mark = new NpgsqlCommand(MarkBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerBlock(mark, serverId, blockStart);
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = seqs });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified) });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = rows });
            await mark.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return rows;
    }

    /// <summary>
    /// The block half of a builder tick: removes the expired blocks, then sums the blocks that are due (<see cref="PlanSql"/>) while
    /// <paramref name="elapsed"/> is under <see cref="PlanRegressionDaily.MaxTickDuration"/>. A failed block is logged and the tick goes on: the daily rows
    /// are the source and the fallback, so a block that is not summed costs the read nothing but the gain.
    /// </summary>
    public static async Task<BlockStepResult> RunStepAsync(
        NpgsqlConnection connection, DateTime nowUtc, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, skipped = 0, failed = 0;
        var removed = await GcAsync(connection, nowUtc, cancellationToken);

        var today = DateOnly.FromDateTime(nowUtc);
        var due = new List<(int ServerId, DateOnly BlockStart)>();
        await using (var plan = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            plan.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = BlockStart(today.AddDays(-PlanRegressionDaily.MarkLateWindowDays)) });
            plan.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = BlockStart(today.AddDays(PlanRegressionDaily.NewestBuiltDayAhead)) });
            plan.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
            await using var reader = await plan.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                due.Add((reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            }
        }

        foreach (var (serverId, blockStart) in due)
        {
            if (elapsed() >= PlanRegressionDaily.MaxTickDuration)
            {
                break;
            }

            try
            {
                if (await BuildBlockAsync(connection, serverId, blockStart, nowUtc, cancellationToken) is null)
                {
                    skipped++;
                }
                else
                {
                    built++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "PLAN_REGRESSION blocks: summing server {ServerId} block {Block:yyyy-MM-dd} failed and is retried next tick (the read uses the daily rows): {Message}",
                    serverId, blockStart, ex.Message);
            }
        }

        return new BlockStepResult(built, skipped, failed, removed);
    }

    /// <summary>Removes the expired blocks in ONE transaction: the stamps first (they name the blocks), then each block's sums.</summary>
    public static async Task<int> GcAsync(NpgsqlConnection connection, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var expired = new List<(int ServerId, DateOnly BlockStart)>();
        await using (var command = new NpgsqlCommand(GcBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified) });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                expired.Add((reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            }
        }

        foreach (var (serverId, blockStart) in expired)
        {
            await using var sums = new NpgsqlCommand(GcBlockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            AddServerBlock(sums, serverId, blockStart);
            await sums.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return expired.Count;
    }

    private static void AddServerBlock(NpgsqlCommand command, int serverId, DateOnly blockStart)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = blockStart });
    }
}
