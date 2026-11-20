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
/// The per-(server, job, server-local day) summary of <c>collect.job_history</c>'s successful job-outcome rows (<c>step_id = 0 AND
/// run_status = 1</c>) that the Job History read's <c>job_stats</c> (a job's average successful duration and its last success) reads
/// whole days from (#9110). Without it that average decompresses every compressed <c>job_history</c> chunk of the window.
///
/// <para><b>The columns.</b> <c>run_day</c> is <c>run_datetime::date</c>, the day on the monitored server's own clock (the column holds
/// server-local time), so it does not depend on the server's UTC offset. <c>success_count</c> is the number of those rows whose
/// <c>run_duration_seconds</c> is not NULL and <c>success_duration_sum</c> is the sum of those durations, so
/// <c>sum / count</c> over any set of days equals <c>AVG(run_duration_seconds)</c> over the same rows, NULL durations left out just as
/// <c>AVG</c> leaves them out. A day whose successes all have a NULL duration therefore has a count of 0: read the average as
/// <c>sum::numeric / NULLIF(count, 0)</c>. <c>last_success_run_local</c> is the newest <c>run_datetime</c> of every row of the day,
/// including a row with a NULL duration.</para>
///
/// <para><b>The built table.</b> <c>job_outcome_daily_built</c> has one row per (server, day) that was built, including a day with no
/// success row (then no <c>job_outcome_daily</c> row exists and <c>source_through</c> is NULL). <c>source_through</c> is the newest
/// <c>collection_time</c> (naive UTC, stored as a <c>timestamptz</c> at UTC) of the rows the build read. A row of a built day with a
/// later <c>collection_time</c> reached the store after the build, so the rollup does not hold it: a reader takes the rollup for the
/// day and adds those rows from <c>job_history</c>, or reads the day raw. Compare with
/// <c>(b.source_through AT TIME ZONE 'UTC')</c> against <c>collection_time</c>, so the session time zone cannot move the comparison,
/// and treat a NULL <c>source_through</c> as "every row is late". A row is collected after its run starts, so a day's rows all have a
/// <c>collection_time</c> at or after the day's UTC start minus the largest UTC offset.</para>
///
/// <para>Plain tables, no hypertable. An older build ignores them. The service builds a day once it has ended on the server's clock
/// (<see cref="BuildAfter"/> past midnight) and once more a day later (<see cref="FinalBuildAfter"/>), newest day first, at most
/// <see cref="MaxBuildsPerTick"/> a tick, each in its own transaction. The first ticks after the upgrade backfill every day the store
/// holds, newest first. A build reads <c>job_history</c> only from the day's collection window (see <see cref="BuildDaySql"/>), and the
/// days after job_history's retention edge go on the same tick.</para>
/// </summary>
public static class JobOutcomeDaily
{
    /// <summary>The per-(server, day, job) summary table.</summary>
    public const string Table = "collect.job_outcome_daily";

    /// <summary>The per-(server, day) record of which days are built, and how far their source reached.</summary>
    public const string BuiltTable = "collect.job_outcome_daily_built";

    /// <summary>The migration rung that creates both tables.</summary>
    public const int Version = 282;

    /// <summary>Creates both tables. Idempotent. No GRANT: the <c>collect</c> schema's blanket default privileges cover a new table.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.job_outcome_daily
(
    server_id integer NOT NULL,
    job_id text NOT NULL,
    run_day date NOT NULL,
    success_count bigint NOT NULL,
    success_duration_sum bigint NOT NULL,
    last_success_run_local timestamp NOT NULL,
    CONSTRAINT pk_job_outcome_daily PRIMARY KEY (server_id, run_day, job_id)
);

CREATE TABLE IF NOT EXISTS collect.job_outcome_daily_built
(
    server_id integer NOT NULL,
    run_day date NOT NULL,
    built_at timestamptz NOT NULL,
    source_through timestamptz,
    CONSTRAINT pk_job_outcome_daily_built PRIMARY KEY (server_id, run_day)
);";

    /// <summary>A day is built once it has ended on the server's clock and this has passed, so the collector's catch-up has settled.</summary>
    public static readonly TimeSpan BuildAfter = TimeSpan.FromHours(2);

    /// <summary>A day is built once more when its end plus this has passed (a day later), which picks up a row that reached the store
    /// after the first build. A reader does not depend on it: it reads a row newer than <c>source_through</c> from <c>job_history</c>.</summary>
    public static readonly TimeSpan FinalBuildAfter = TimeSpan.FromHours(26);

    /// <summary>The most (server, day) builds one tick runs.</summary>
    public const int MaxBuildsPerTick = 60;

    /// <summary>No new build starts once a tick has run this long. A build already running finishes or hits its own timeout.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    /// <summary>The server-side <c>statement_timeout</c> each build's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>job_history's retention in days: the fleet override when set, else the default (the horizon the raw table's own purge uses).</summary>
    internal static readonly string RetentionDaysSql = "SELECT " + DarlingRetentionHorizons.FleetRetentionDaysSql("job_history");

    /// <summary>
    /// The builds due now, newest day first. $1 now (naive UTC), $2 retention days, $3 <see cref="BuildAfter"/>, $4
    /// <see cref="FinalBuildAfter"/>, $5 the cap. A server is planned when it is enabled and holds a <c>job_history</c> row. Its
    /// days run from the retention edge (now less the retention, as a date) through its own yesterday, where "now" on its clock is now plus
    /// its latest collected UTC offset (0 when none). A day with no built row is due once its end plus $3 has passed. A built day is due
    /// once more when its end plus $4 has passed and its build is older than that instant: the instant is how the second build is
    /// told apart from the first, so the built table needs no pass column, and a day built after it is not built again. A server whose
    /// offset moves an hour (DST) shifts the instant by an hour, which at worst repeats one build.
    /// </summary>
    internal static readonly string PlanSql = $@"
SELECT s.server_id, d.day
FROM
(
    SELECT sv.server_id, COALESCE(o.utc_offset_minutes, 0) AS offset_minutes
    FROM collect.servers AS sv
    LEFT JOIN LATERAL ({DarlingJobHistoryReader.LatestOffsetSql("sv.server_id")}) AS o ON true
    WHERE sv.is_enabled
    AND   EXISTS (SELECT 1 FROM collect.job_history AS jh WHERE jh.server_id = sv.server_id)
) AS s
CROSS JOIN LATERAL
(
    SELECT g::date AS day
    FROM generate_series
    (
        ($1::date - $2)::timestamp,
        (($1 + make_interval(mins => s.offset_minutes))::date - 1)::timestamp,
        interval '1 day'
    ) AS g
) AS d
LEFT JOIN collect.job_outcome_daily_built AS b
  ON b.server_id = s.server_id AND b.run_day = d.day
WHERE (b.server_id IS NULL AND d.day + interval '1 day' + $3 <= $1 + make_interval(mins => s.offset_minutes))
   OR (b.server_id IS NOT NULL
       AND d.day + interval '1 day' + $4 <= $1 + make_interval(mins => s.offset_minutes)
       AND (b.built_at AT TIME ZONE 'UTC') < d.day + interval '1 day' + $4 - make_interval(mins => s.offset_minutes))
ORDER BY d.day DESC, s.server_id
LIMIT $5;";

    /// <summary>Clears one (server, day) before it is rebuilt. $1 server_id, $2 day.</summary>
    internal const string DeleteDaySql = @"
DELETE FROM collect.job_outcome_daily
WHERE server_id = $1
  AND run_day = $2;";

    /// <summary>
    /// Summarizes one (server, day) of <c>job_history</c> and returns the newest <c>collection_time</c> it read (NULL when none) and the
    /// number of job rows written. $1 server_id, $2 day (a <c>date</c>).
    ///
    /// <para>The <c>collection_time</c> range keeps the read to the chunks that can hold the day. Its floor drops nothing: a run is
    /// collected after it starts, and the day starts, in UTC, no earlier than the largest UTC offset (14 hours) before the UTC date. Its
    /// ceiling is two days past the UTC date, which is more than a day past the end of the day on any server clock. A row collected
    /// later is not read, and for that reason has a <c>collection_time</c> past <c>source_through</c>, so a reader takes it from
    /// <c>job_history</c> (the contract of the built table). A job with no id has no key to roll up under and a row with no
    /// <c>run_datetime</c> has no day, and neither can match the read's per-job, per-window predicates either.</para>
    /// </summary>
    internal const string BuildDaySql = @"
WITH src AS MATERIALIZED
(
    SELECT jh.job_id, jh.run_duration_seconds, jh.run_datetime, jh.collection_time
    FROM collect.job_history AS jh
    WHERE jh.server_id = $1
    AND   jh.step_id = 0
    AND   jh.run_status = 1
    AND   jh.job_id IS NOT NULL
    AND   jh.run_datetime >= $2::timestamp
    AND   jh.run_datetime < $2::timestamp + interval '1 day'
    AND   jh.collection_time >= $2::timestamp - interval '14 hours'
    AND   jh.collection_time < $2::timestamp + interval '2 days'
),
rolled AS
(
    INSERT INTO collect.job_outcome_daily
    (
        server_id, job_id, run_day, success_count, success_duration_sum, last_success_run_local
    )
    SELECT
        $1, s.job_id, $2, count(s.run_duration_seconds), COALESCE(sum(s.run_duration_seconds), 0)::bigint, max(s.run_datetime)
    FROM src AS s
    GROUP BY s.job_id
    RETURNING 1
)
SELECT max(src.collection_time), (SELECT count(*) FROM rolled)
FROM src;";

    /// <summary>Records the build. $1 server_id, $2 day, $3 built_at, $4 source_through (UTC, NULL when the day had no success row).</summary>
    internal const string UpsertBuiltSql = @"
INSERT INTO collect.job_outcome_daily_built (server_id, run_day, built_at, source_through)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, run_day) DO UPDATE
SET built_at = EXCLUDED.built_at,
    source_through = EXCLUDED.source_through;";

    /// <summary>
    /// Removes the expired built rows (days before the retention edge, and every row of a server that is not enabled) and returns their
    /// keys. $1 the first day kept. The summary rows go key by key with <see cref="GcSummarySql"/>, in the same transaction: the built
    /// table is one row a day per server, the summary table one per job as well, and a delete on the summary table with its own
    /// predicate cannot use its key (the shape <c>QueryStoreTopDaily</c> took in #5507).
    /// </summary>
    internal const string GcBuiltSql = @"
DELETE FROM collect.job_outcome_daily_built
WHERE run_day < $1
   OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
RETURNING server_id, run_day;";

    /// <summary>Removes one expired (server, day) of summary rows by the two leading columns of its key. $1 server_id, $2 day.</summary>
    internal const string GcSummarySql = @"
DELETE FROM collect.job_outcome_daily
WHERE server_id = $1::integer
AND   run_day = $2::date;";

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateOnly Day);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int Built, int Failed, long DaysRemoved, int Deferred = 0);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The first day kept for <paramref name="nowUtc"/>: the retention edge as a date.</summary>
    internal static DateOnly RetentionEdge(DateTime nowUtc, int retentionDays) => DateOnly.FromDateTime(nowUtc.Date.AddDays(-retentionDays));

    /// <summary>The builds due at <paramref name="nowUtc"/>, newest day first, at most <see cref="MaxBuildsPerTick"/>.</summary>
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
            builds.Add(new Build(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
        }

        return builds;
    }

    /// <summary>Removes the days before the retention edge, and every row of a server that is not enabled, from both tables in ONE
    /// transaction; returns how many built (server, day) rows went.</summary>
    public static async Task<long> GcAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var expired = new List<(int ServerId, DateOnly Day)>();
        await using (var command = new NpgsqlCommand(GcBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = RetentionEdge(nowUtc, retentionDays) });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                expired.Add((reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            }
        }

        foreach (var (serverId, day) in expired)
        {
            await using var summary = new NpgsqlCommand(GcSummarySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            summary.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            summary.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await summary.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return expired.Count;
    }

    /// <summary>
    /// Builds one (server, day) in one transaction: the day's summary rows are deleted and re-inserted, and the built row is upserted. A
    /// day with no success row still gets its built row, with a NULL <c>source_through</c>. Returns the number of job rows written.
    /// </summary>
    public static async Task<long> BuildDayAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, DateTime nowUtc, CancellationToken cancellationToken)
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

        DateTime? sourceThrough;
        long written;
        await using (var insert = new NpgsqlCommand(BuildDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await using var reader = await insert.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            sourceThrough = reader.IsDBNull(0) ? null : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
            written = reader.GetInt64(1);
        }

        await using (var built = new NpgsqlCommand(UpsertBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc) });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = (object?)sourceThrough ?? DBNull.Value });
            await built.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return written;
    }

    /// <summary>
    /// One maintenance tick: read job_history's retention, remove expired days, plan, then build each item in its own transaction. A build
    /// that fails is logged (naming the server and day) and the loop goes on; the next tick plans it again. Once
    /// <see cref="MaxTickDuration"/> has passed no further build starts, and the planned items left are reported as deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/> with the elapsed time since the tick
    /// began supplied by <paramref name="elapsed"/>, read before each build starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, failed = 0, started = 0;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        int retentionDays;
        await using (var retention = new NpgsqlCommand(RetentionDaysSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            retentionDays = Convert.ToInt32(await retention.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
        }

        var removed = await GcAsync(connection, nowUtc, retentionDays, cancellationToken);
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
                await BuildDayAsync(connection, item.ServerId, item.Day, nowUtc, cancellationToken);
                built++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Job outcome daily summary: building server {ServerId} day {Day:yyyy-MM-dd} failed and is retried next tick: {Message}",
                    item.ServerId, item.Day, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        var result = new TickResult(built, failed, removed, deferred);
        logger.LogInformation("Job outcome daily summary: built {Built}, failed {Failed}, deferred {Deferred}, removed {Removed} day(s), {ElapsedMs} ms",
            built, failed, deferred, removed, (long)elapsed().TotalMilliseconds);
        return result;
    }
}
