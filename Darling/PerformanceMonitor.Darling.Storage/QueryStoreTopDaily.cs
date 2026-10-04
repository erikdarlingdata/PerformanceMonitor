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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Builds the per-(server, UTC day) summary of <c>collect.query_store_interval_wide</c> into
/// <c>collect.query_store_top_daily</c> (#5094), the table <c>get_query_store_top</c>'s long windows will read whole
/// days from.
///
/// <para><b>The summary is APPROXIMATE by design.</b> It stores exactly recombinable parts (row counts, sums, and the
/// counts of non-NULL values behind each average), so a built day reproduces the unweighted mean of the interval
/// averages and the sum of <c>execution_count</c> for that day. The only approximation is staleness: a day is summarized
/// once it has ended, and a wide-table row that changes after that is missed (a backfill or a replayed batch into a
/// built day) or, for a row whose <c>collection_time</c> moves from day D to D+1 after D was built, counted in both
/// days. Rebuilding every day a second time, a day later, clears the second case, so the window in which a total can be
/// off is about one day. A build also drops a row whose <c>first_execution_time</c> is more than <see cref="SkewSlack"/>
/// past the end of its own day (see <see cref="BuildDaySql"/>), which takes a monitored server whose clock runs more than
/// an hour ahead of the collector's: one more case of the same approximation. A reader that uses this table says so in
/// its answer.</para>
///
/// <para><b>Two passes.</b> Pass 1 builds day D once D+1 00:00 plus <see cref="BuildAfter"/> has passed, so the
/// collector's catch-up has settled. Pass 2 rebuilds it once D+1 00:00 plus <see cref="FinalBuildAfter"/> has passed,
/// which is longer than any interval can span (<see cref="QueryStoreIntervalWide.IntervalSpanMargin"/>) plus the
/// longest catch-up (<see cref="WatermarkPolicy.MaxCatchup"/>), so no row can still move into or out of D by then.
/// A day first seen after that threshold has already passed (a store's first tick, or a long outage) is built once,
/// straight at pass 2: a pass 1 would only be redone a tick later.
/// <c>collect.query_store_top_daily_built</c> records which pass each day is at. A build is one transaction: the day's
/// rows are deleted and re-inserted whole, and the built row is upserted with them, so a reader never sees a half-built
/// day marked as built.</para>
///
/// <para>The service is the only writer. One tick builds at most <see cref="MaxBuildsPerTick"/> (server, day) items,
/// oldest day first, and stops starting new builds once <see cref="MaxTickDuration"/> has passed; what it did not reach
/// is reported as deferred and planned again next tick. A failing build is logged and skipped: the next tick plans it
/// again. Only enabled servers are planned, and only days from the day after the server's wide-table coverage claim
/// (<c>filled_since</c>), because the summary cannot speak for earlier days.</para>
/// </summary>
public static class QueryStoreTopDaily
{
    /// <summary>Pass 1 builds day D once D+1 00:00 plus this has passed.</summary>
    public static readonly TimeSpan BuildAfter = TimeSpan.FromHours(2);

    /// <summary>Pass 2 rebuilds day D once D+1 00:00 plus this has passed: the longest interval a row can span plus the
    /// longest catch-up, so nothing can still be moving into or out of D.</summary>
    public static readonly TimeSpan FinalBuildAfter = QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup;

    /// <summary>The most (server, day, pass) builds one tick runs.</summary>
    public const int MaxBuildsPerTick = 60;

    /// <summary>The wall-clock budget of one tick: no new build starts once this much time has passed since the tick
    /// began. A build already running finishes (or hits its own timeout), so a tick can overrun by one build.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(10);

    /// <summary>How far past the end of its own day a row's <c>first_execution_time</c> may sit and still be summarized
    /// into that day (see <see cref="BuildDaySql"/>).</summary>
    public static readonly TimeSpan SkewSlack = TimeSpan.FromHours(1);

    /// <summary><see cref="SkewSlack"/> as a Postgres interval literal.</summary>
    private static readonly string SkewSlackSql = $"interval '{(long)Math.Ceiling(SkewSlack.TotalMinutes)} minutes'";

    /// <summary>The server-side <c>statement_timeout</c> each build's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    /// <summary>The client-side deadline for each command: just above the server-side one, so the store's own
    /// cancel is the one that fires.</summary>
    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>Clears one (server, day) before it is rebuilt. $1 server_id, $2 day.</summary>
    public const string DeleteDaySql = @"
DELETE FROM collect.query_store_top_daily
WHERE server_id = $1
  AND day = $2;";

    /// <summary>
    /// Summarizes one (server, day) of the wide table and returns the number of wide rows it read. $1 server_id,
    /// $2 day (a <c>date</c>).
    /// <para>The <c>first_execution_time</c> range is for the <c>(server_id, first_execution_time)</c> btree and the
    /// unique key, neither of which serves <c>collection_time</c>; without it a build range-scans every row of the
    /// server from the floor up to now. The floor drops no row: every stored row has
    /// <c>first_execution_time &gt; collection_time - (IntervalSpanMargin + MaxCatchup)</c> (the argument is in
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/>'s summary, and the MCP read applies the same floor), so a
    /// row inside the day is inside the floor too. The upper edge, the day's end plus <see cref="SkewSlack"/>, drops only
    /// a row whose <c>first_execution_time</c> is more than that past its own <c>collection_time</c>, which needs a
    /// monitored server whose clock runs more than an hour ahead of the collector's; that is one more case of the
    /// summary's documented approximation.</para>
    /// </summary>
    public static readonly string BuildDaySql = $@"
WITH built AS
(
    INSERT INTO collect.query_store_top_daily
    (
        server_id, day, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
        interval_rows, execution_count_sum,
        avg_duration_us_sum, avg_duration_us_n,
        avg_cpu_time_us_sum, avg_cpu_time_us_n,
        avg_logical_io_reads_sum, avg_logical_io_reads_n,
        avg_logical_io_writes_sum, avg_logical_io_writes_n,
        avg_physical_io_reads_sum, avg_physical_io_reads_n,
        avg_rowcount_sum, avg_rowcount_n,
        last_execution_time_max, query_plan_hash_max, first_execution_time_min
    )
    SELECT
        w.server_id, $2, w.database_name, w.query_id, w.plan_id, w.query_hash, w.execution_type_desc, w.replica_role, w.module_name,
        count(*), sum(w.execution_count),
        sum(w.avg_duration_us::numeric), count(w.avg_duration_us),
        sum(w.avg_cpu_time_us::numeric), count(w.avg_cpu_time_us),
        sum(w.avg_logical_io_reads::numeric), count(w.avg_logical_io_reads),
        sum(w.avg_logical_io_writes::numeric), count(w.avg_logical_io_writes),
        sum(w.avg_physical_io_reads::numeric), count(w.avg_physical_io_reads),
        sum(w.avg_rowcount::numeric), count(w.avg_rowcount),
        max(w.last_execution_time), max(w.query_plan_hash), min(w.first_execution_time)
    FROM collect.query_store_interval_wide AS w
    WHERE w.server_id = $1
      AND w.collection_time >= $2::timestamp
      AND w.collection_time < $2::timestamp + interval '1 day'
      AND w.first_execution_time >= $2::timestamp - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
      AND w.first_execution_time < $2::timestamp + interval '1 day' + {SkewSlackSql}
    GROUP BY w.server_id, w.database_name, w.query_id, w.plan_id, w.query_hash, w.execution_type_desc, w.replica_role, w.module_name
    RETURNING interval_rows
)
SELECT COALESCE(sum(interval_rows), 0)::bigint FROM built;";

    /// <summary>Records the build. $1 server_id, $2 day, $3 pass, $4 built_at, $5 source_rows.</summary>
    public const string UpsertBuiltSql = @"
INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, day) DO UPDATE
SET pass = EXCLUDED.pass,
    built_at = EXCLUDED.built_at,
    source_rows = EXCLUDED.source_rows;";

    /// <summary>
    /// The builds due now, oldest day first. $1 now, $2 retention days, $3 <see cref="BuildAfter"/>,
    /// $4 <see cref="FinalBuildAfter"/>, $5 the cap. Servers are the enabled ones with a wide-table coverage claim, and a
    /// server's days start the day after its <c>filled_since</c>, since the summary cannot speak for earlier days. Days
    /// run from two days after the retention horizon through yesterday: the purge deletes on
    /// <c>first_execution_time</c> at the horizon, and a row collected on the horizon's next day can have started up to
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> earlier, so that day may already be cut. A day with no built row is due once its end plus $3 has
    /// passed, at pass 1, or straight at pass 2 when its end plus $4 has passed too (nothing more could change it); a day
    /// built at pass 1 is due for pass 2 once its end plus $4 has passed. Oldest day first, so a backlog works through the
    /// oldest days once each and the next tick reaches newer ones.
    /// </summary>
    public const string PlanSql = @"
SELECT s.server_id, d.day, CASE WHEN b.server_id IS NULL AND d.day + interval '1 day' + $4 > $1 THEN 1 ELSE 2 END AS pass
FROM
(
    SELECT c.server_id, c.filled_since
    FROM collect.query_store_interval_wide_coverage AS c
    JOIN collect.servers AS sv ON sv.server_id = c.server_id
    WHERE sv.is_enabled
) AS s
CROSS JOIN LATERAL
(
    SELECT g::date AS day
    FROM generate_series((($1 - make_interval(days => $2))::date + 2)::timestamp, ($1::date - 1)::timestamp, interval '1 day') AS g
) AS d
LEFT JOIN collect.query_store_top_daily_built AS b
  ON b.server_id = s.server_id AND b.day = d.day
WHERE d.day >= s.filled_since::date + 1
  AND ((b.server_id IS NULL AND d.day + interval '1 day' + $3 <= $1)
    OR (b.pass = 1 AND d.day + interval '1 day' + $4 <= $1))
ORDER BY d.day, s.server_id
LIMIT $5;";

    /// <summary>Removes days older than the wide table's retention, less a day, and every row of a server that is not
    /// enabled. $1 now, $2 retention days. Returns the built rows removed.</summary>
    public const string GcSql = @"
WITH summary AS
(
    DELETE FROM collect.query_store_top_daily
    WHERE day < ($1 - make_interval(days => $2))::date - 1
       OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
    RETURNING 1
),
built AS
(
    DELETE FROM collect.query_store_top_daily_built
    WHERE day < ($1 - make_interval(days => $2))::date - 1
       OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
    RETURNING 1
)
SELECT (SELECT count(*) FROM built)::bigint;";

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateOnly Day, int Pass);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int BuiltPass1, int BuiltPass2, int Failed, long DaysRemoved, int Deferred = 0)
    {
        public int Built => BuiltPass1 + BuiltPass2;
    }

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The builds due at <paramref name="nowUtc"/>, oldest day first, at most <see cref="MaxBuildsPerTick"/>.</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = BuildAfter });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = FinalBuildAfter });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1), reader.GetInt32(2)));
        }

        return builds;
    }

    /// <summary>Removes the days below the retention horizon, and every row of a server that is not enabled, from both
    /// tables; returns how many built (server, day) rows went.</summary>
    public static async Task<long> GcAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(GcSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
    }

    /// <summary>
    /// Builds one (server, day) in one transaction: the day's summary rows are deleted and re-inserted, and the built
    /// row is upserted. A day with no wide rows still gets its built row, with <c>source_rows</c> 0. Returns the wide
    /// rows read.
    /// </summary>
    public static async Task<long> BuildDayAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, int pass, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var delete = new NpgsqlCommand(DeleteDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        long sourceRows;
        await using (var insert = new NpgsqlCommand(BuildDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            sourceRows = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken));
        }

        await using (var built = new NpgsqlCommand(UpsertBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Smallint, Value = (short)pass });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sourceRows });
            await built.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return sourceRows;
    }

    /// <summary>
    /// One maintenance tick: remove expired days, plan, then build each item in its own transaction. A build that
    /// fails is logged (naming the server and day) and the loop goes on; the next tick plans it again. Once
    /// <see cref="MaxTickDuration"/> has passed no further build starts, and the planned items left are reported as
    /// deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, retentionDays, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, int, ILogger, CancellationToken)"/> with the elapsed
    /// time since the tick began supplied by <paramref name="elapsed"/>, read before each build starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int pass1 = 0, pass2 = 0, failed = 0, started = 0;
        long removed;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        removed = await GcAsync(connection, nowUtc, retentionDays, cancellationToken);
        var plan = await PlanBuildsAsync(connection, nowUtc, retentionDays, cancellationToken);

        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                await BuildDayAsync(connection, item.ServerId, item.Day, item.Pass, nowUtc, cancellationToken);
                if (item.Pass == 1)
                {
                    pass1++;
                }
                else
                {
                    pass2++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Query Store top daily summary: building server {ServerId} day {Day:yyyy-MM-dd} (pass {Pass}) failed and is retried next tick: {Message}",
                    item.ServerId, item.Day, item.Pass, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        var result = new TickResult(pass1, pass2, failed, removed, deferred);
        logger.LogInformation("Query Store top daily summary: built {Built} (pass1 {Pass1}, pass2 {Pass2}), failed {Failed}, deferred {Deferred}, removed {Removed} day(s)",
            result.Built, pass1, pass2, failed, deferred, removed);
        return result;
    }
}
