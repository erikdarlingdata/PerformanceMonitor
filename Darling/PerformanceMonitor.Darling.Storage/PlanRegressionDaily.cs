/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL and day arithmetic for the per-day per-plan totals of <c>collect.query_store_interval_latest</c>
/// (<c>collect.plan_regression_daily</c>, #5448), which PLAN_REGRESSION reads for CLOSED days so a run no longer
/// re-aggregates 14 days of the interval table. This file is the storage half: the rung that creates the tables and the
/// trigger is V167 (<see cref="PgMigrations"/>), the builder is the hourly-tick tenant at the bottom of this file
/// (<see cref="RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/>), and the read follows in its own lane.
///
/// <para><b>Row day is the day of <c>last_execution_time</c></b> (naive UTC), the column PLAN_REGRESSION's window filters
/// on. A day's row is the plan's totals over the interval rows whose <c>last_execution_time</c> falls in that day, so
/// summing the closed days and the live days reproduces the 14-day aggregate. Gain is about 4-5x fewer blocks read, not
/// 14x: the last three days stay live (see <see cref="ClosedDayLagDays"/>), and they hold about a fifth of the rows.</para>
///
/// <para><b>Closing a day.</b> A day D is closed, and so built and read from the table, once <c>D &lt;= T - 3</c> for
/// today T: one day for D to have passed, <see cref="PlanRegressionSkewMarginDays"/> for a monitored server whose clock
/// runs ahead of the collector's, and one for the longest span of a Query Store interval, which is what lets a row's
/// <c>first_execution_time</c> sit a day before its <c>last_execution_time</c>.</para>
/// </summary>
public static class PlanRegressionDaily
{
    /// <summary>The window PLAN_REGRESSION describes, in days; also how far back the builder fills closed days.</summary>
    public const int WindowDays = 14;

    /// <summary>Days of clock skew between a monitored server and the collector that the closing of a day tolerates.</summary>
    public const int PlanRegressionSkewMarginDays = 1;

    /// <summary>
    /// A day D is closed when <c>D &lt;= T - ClosedDayLagDays</c>: one day past, <see cref="PlanRegressionSkewMarginDays"/>
    /// of skew, and one day for an interval's span. The three newest days always come from the live table.
    /// </summary>
    public const int ClosedDayLagDays = 1 + PlanRegressionSkewMarginDays + 1;

    /// <summary>How far back (in days) V167's trigger marks a day stale: a row older than this can never be read, because
    /// the read's window is <see cref="WindowDays"/> and a row's day can be a day after the day of its first execution.</summary>
    public const int MarkLateWindowDays = WindowDays + 2;

    /// <summary>
    /// Builds one closed day of one server's totals: the plan-level aggregate PLAN_REGRESSION's
    /// <c>plan_agg</c> step computes, summed rather than divided so days recombine exactly. $1 server_id (integer),
    /// $2 the day D (date), $3 D + 1 day (timestamp), $4 D - 1 day (timestamp). Returns the number of rows inserted,
    /// which the builder records as <c>source_rows</c>. The caller deletes the day's rows first, in the same
    /// transaction (the unique index would refuse a second build of a day).
    ///
    /// <para><b>The bounds are the day's own, and the parity argument is in two parts.</b> A row belongs to D when its
    /// <c>last_execution_time</c> is in <c>[D, D + 1 day)</c>, which is the filter the read applies to a window whose edge
    /// is day-aligned. Its <c>first_execution_time</c> is then in <c>[D - 1 day, D + 1 day)</c>, because one Query Store
    /// interval spans at most a day (the read's <c>first_execution_time &gt;= $4</c> is implied by its
    /// <c>last_execution_time</c> filter for the same reason, and is kept to bound the table's partitioning column for chunk
    /// exclusion). <c>collection_time &gt;= $4</c> mirrors the read's own collection bound, so the build and the live read
    /// keep the same snapshots. The two routes can differ only for a row whose <c>collection_time</c> is more than a day
    /// before its <c>last_execution_time</c>.</para>
    ///
    /// <para>Every parameter is cast where it is used, so each has one type however many times it appears. The sums are
    /// <c>numeric</c>, so the product of an average and an execution count cannot overflow <c>bigint</c>.</para>
    /// </summary>
    public const string BuildDaySql = @"
WITH built AS
(
    INSERT INTO collect.plan_regression_daily
    (
        server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash,
        execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count
    )
    SELECT
        $1::integer, $2::date, l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash,
        SUM(l.execution_count),
        SUM(l.avg_cpu_time_us::numeric * l.execution_count),
        SUM(l.avg_duration_us::numeric * l.execution_count),
        MAX(l.last_execution_time),
        bool_or(l.is_forced_plan),
        MAX(l.force_failure_count)
    FROM collect.query_store_interval_latest AS l
    WHERE l.server_id = $1::integer
    AND   l.first_execution_time >= $4::timestamp
    AND   l.first_execution_time < $3::timestamp
    AND   l.last_execution_time >= $2::date
    AND   l.last_execution_time < $3::timestamp
    AND   l.collection_time >= $4::timestamp
    GROUP BY l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash
    RETURNING 1
)
SELECT count(*)::bigint FROM built;";

    /// <summary>The most (server, day) builds one tick runs. The first fill of a store is about 520 builds (a handful of
    /// ticks); until it is done a read pays today's cost for the days not yet built.</summary>
    public const int MaxBuildsPerTick = 120;

    /// <summary>The wall-clock budget of one tick: no new build starts once this much time has passed since the tick
    /// began. A build already running finishes (or hits its own timeout), so a tick can overrun by one build.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(10);

    /// <summary>The server-side <c>statement_timeout</c> each build's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    /// <summary>The client-side deadline for each command: just above the server-side one, so the store's own cancel is the
    /// one that fires.</summary>
    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>Makes sure the day has a built row, so the tick's transaction has a row to read <c>late_seq</c> from and to
    /// stamp. The trigger may have created it already (a late row marks its days), and then this does nothing. $1 server_id,
    /// $2 day.</summary>
    public const string EnsureBuiltSql = @"
INSERT INTO collect.plan_regression_daily_built (server_id, day)
VALUES ($1::integer, $2::date)
ON CONFLICT (server_id, day) DO NOTHING;";

    /// <summary>The day's <c>late_seq</c> as the builder sees it before it aggregates: the value the build is stamped with
    /// (<c>built_seq</c>), so a late apply that lands during the build leaves the day invalid and it is rebuilt. $1 server_id,
    /// $2 day.</summary>
    public const string SeqSql = @"
SELECT late_seq
FROM collect.plan_regression_daily_built
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>Stamps a finished build. $1 server_id, $2 day, $3 the <c>late_seq</c> read before the build
    /// (<c>built_seq</c>), $4 built_at, $5 the rows inserted (<c>source_rows</c>). The day is valid for a reader exactly while
    /// <c>built_seq = late_seq</c>.</summary>
    public const string MarkBuiltSql = @"
UPDATE collect.plan_regression_daily_built
SET built_seq = $3::bigint,
    built_at = $4::timestamp,
    source_rows = $5::bigint
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>Clears one (server, day) of totals before it is rebuilt: <see cref="BuildDaySql"/> inserts, and the unique
    /// index would refuse a second build. $1 server_id, $2 day.</summary>
    public const string DeleteDaySql = @"
DELETE FROM collect.plan_regression_daily
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>One builder per server at a time: a transaction-scoped advisory lock keyed by the server. False means
    /// another builder (a second service, or a tick that overran) holds it, and this one skips the item. $1 server_id.</summary>
    public const string LockSql = @"
SELECT pg_try_advisory_xact_lock(hashtextextended('plan_regression_daily:' || $1::integer::text, 0));";

    /// <summary>
    /// The builds due now, oldest day first. $1 now (timestamp), $2 the cap. Servers are the enabled ones with an interval
    /// coverage claim and no pending batch: a pending batch means some raw rows are not yet applied, so the day's totals
    /// could not be trusted. A server's days start the day after its <c>filled_since</c>, since the table cannot speak for
    /// earlier days. Days run from <see cref="WindowDays"/> back through <see cref="ClosedDayLagDays"/> back (a day is
    /// closed when <c>D &lt;= T - 3</c>). A day is due when it has no stamped build (<c>built_seq</c> NULL) or its
    /// <c>late_seq</c> has moved since the stamp.
    /// </summary>
    public static readonly string PlanSql = $@"
SELECT s.server_id, d.day
FROM
(
    SELECT c.server_id, c.filled_since
    FROM collect.query_store_interval_latest_coverage AS c
    JOIN collect.servers AS sv ON sv.server_id = c.server_id
    WHERE sv.is_enabled
    AND   NOT EXISTS (SELECT 1 FROM collect.query_store_interval_latest_pending AS p WHERE p.server_id = c.server_id)
) AS s
CROSS JOIN LATERAL
(
    SELECT g::date AS day
    FROM generate_series(($1::timestamp::date - {WindowDays})::timestamp, ($1::timestamp::date - {ClosedDayLagDays})::timestamp, interval '1 day') AS g
) AS d
LEFT JOIN collect.plan_regression_daily_built AS b
  ON b.server_id = s.server_id AND b.day = d.day
WHERE d.day >= s.filled_since::date + 1
AND   (b.built_seq IS NULL OR b.built_seq <> b.late_seq)
ORDER BY d.day, s.server_id
LIMIT $2::integer;";

    /// <summary>
    /// Removes days older than the largest day the trigger can mark (<see cref="MarkLateWindowDays"/>), and every row of a
    /// server that is not enabled, from both tables in ONE statement (two data-modifying CTEs), so the totals and their
    /// built rows never disagree about which days exist. $1 now (timestamp). Returns the built rows removed.
    /// </summary>
    public static readonly string GcSql = $@"
WITH totals AS
(
    DELETE FROM collect.plan_regression_daily
    WHERE day < ($1::timestamp - interval '{MarkLateWindowDays} days')::date
       OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
    RETURNING 1
),
built AS
(
    DELETE FROM collect.plan_regression_daily_built
    WHERE day < ($1::timestamp - interval '{MarkLateWindowDays} days')::date
       OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
    RETURNING 1
)
SELECT (SELECT count(*) FROM built)::bigint;";

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateOnly Day);

    /// <summary>What one tick did. <paramref name="Skipped"/> counts items another builder held the server's lock for.</summary>
    public readonly record struct TickResult(int Built, int Failed, int Skipped, long DaysRemoved, int Deferred = 0);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The builds due at <paramref name="nowUtc"/>, oldest day first, at most <see cref="MaxBuildsPerTick"/>.</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
        }

        return builds;
    }

    /// <summary>Removes the expired days and the days of servers that are not enabled from both tables; returns how many
    /// built (server, day) rows went.</summary>
    public static async Task<long> GcAsync(NpgsqlConnection connection, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(GcSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
    }

    /// <summary>
    /// Builds one (server, day): makes sure the built row exists, then in ONE transaction takes the server's advisory lock
    /// (false: another builder has it, nothing is written and the result is null), reads <c>late_seq</c> as s0, deletes the
    /// day's totals, inserts them, and stamps <c>built_seq = s0</c>. A late apply that bumps <c>late_seq</c> after s0 was read
    /// leaves the day invalid for a reader (<c>built_seq &lt;&gt; late_seq</c>) and due again, so a race costs one more build,
    /// never a wrong total. Returns the rows inserted, or null when the lock was not free.
    /// </summary>
    /// <param name="afterSeqRead">A test seam, called between reading s0 and aggregating, so a pin can land a late apply at
    /// the point the race matters. Null in the service.</param>
    public static async Task<long?> BuildDayAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, DateTime nowUtc, CancellationToken cancellationToken,
        Func<CancellationToken, Task>? afterSeqRead = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using (var ensure = new NpgsqlCommand(EnsureBuiltSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(ensure, serverId, day);
            await ensure.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var @lock = new NpgsqlCommand(LockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            @lock.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            if (!(bool)(await @lock.ExecuteScalarAsync(cancellationToken))!)
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        long seq;
        await using (var read = new NpgsqlCommand(SeqSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(read, serverId, day);
            seq = Convert.ToInt64(await read.ExecuteScalarAsync(cancellationToken));
        }

        if (afterSeqRead is not null)
        {
            await afterSeqRead(cancellationToken);
        }

        await using (var delete = new NpgsqlCommand(DeleteDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(delete, serverId, day);
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        long sourceRows;
        var dayStart = day.ToDateTime(TimeOnly.MinValue);
        await using (var insert = new NpgsqlCommand(BuildDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(dayStart.AddDays(1)) });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(dayStart.AddDays(-1)) });
            sourceRows = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken));
        }

        await using (var mark = new NpgsqlCommand(MarkBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(mark, serverId, day);
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = seq });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sourceRows });
            await mark.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return sourceRows;
    }

    private static void AddServerDay(NpgsqlCommand command, int serverId, DateOnly day)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
    }

    /// <summary>
    /// One maintenance tick, the hourly store-maintenance tick's seventh tenant (#5448): remove expired days, plan, then build
    /// each item in its own transaction, oldest day first. A build that fails is logged as a WARNING (naming the server and
    /// day) and the loop goes on: the day stays live for readers and the next tick plans it again. Once
    /// <see cref="MaxTickDuration"/> has passed no further build starts, and the planned items left are reported as deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/> with the elapsed time since
    /// the tick began supplied by <paramref name="elapsed"/>, read before each build starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, failed = 0, skipped = 0, started = 0;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var removed = await GcAsync(connection, nowUtc, cancellationToken);
        var plan = await PlanBuildsAsync(connection, nowUtc, cancellationToken);

        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                if (await BuildDayAsync(connection, item.ServerId, item.Day, nowUtc, cancellationToken) is null)
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
                logger.LogWarning(ex, "PLAN_REGRESSION daily totals: building server {ServerId} day {Day:yyyy-MM-dd} failed and is retried next tick (the day stays live for readers): {Message}",
                    item.ServerId, item.Day, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        var result = new TickResult(built, failed, skipped, removed, deferred);
        logger.LogInformation("PLAN_REGRESSION daily totals: built {Built}, failed {Failed}, skipped {Skipped}, deferred {Deferred}, removed {Removed} day(s)",
            built, failed, skipped, deferred, removed);
        return result;
    }
}
