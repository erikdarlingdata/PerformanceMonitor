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
using System.Globalization;
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
/// <para><b>Three tables.</b> <c>job_outcome_daily</c>: <c>run_day</c> is <c>run_datetime::date</c>, the day on the monitored server's own
/// clock. <c>success_count</c> counts the rows whose <c>run_duration_seconds</c> is not NULL and <c>success_duration_sum</c> sums those
/// durations, so <c>sum::numeric / NULLIF(count, 0)</c> over any set of days is <c>AVG(run_duration_seconds)</c> over the same rows.
/// <c>last_success_run_local</c> is the newest <c>run_datetime</c> of every row of the day, a NULL-duration row included.
/// <c>job_outcome_daily_days</c>: per (server, <c>run_day</c>), <c>max_collection_time</c> (timestamptz at UTC), the newest
/// <c>collection_time</c> of any row ever added to that day. <c>job_outcome_daily_watermark</c>: per server, <c>collected_through</c>
/// (timestamptz at UTC) and <c>gc_before</c> (a date, NULL until a retention pass has run): the newest first-day-kept the retention pass has
/// used, so every day before it may have been deleted from the rollup, and a day at or after it is whole.</para>
///
/// <para><b>The invariant.</b> Every qualifying <c>job_history</c> row of a server whose <c>collection_time</c> (naive UTC) is at or before
/// that server's <c>collected_through</c> is counted in the rollup exactly once, and no row above it is counted. A reader takes the rollup
/// for whole days plus the raw rows with <c>collection_time</c> above the watermark, and reads a day's late rows (a row whose run day is
/// older than its collection) from <c>job_history</c> between the day's start and <c>max_collection_time</c>. Compare with
/// <c>(w.collected_through AT TIME ZONE 'UTC')</c> against <c>collection_time</c> so the session time zone cannot move the comparison.</para>
///
/// <para><b>The tick.</b> The rollup is built by <c>collection_time</c>, the column the chunks are partitioned on, and never by re-reading a
/// run day. Per server, <see cref="StepAsync"/> aggregates the rows with <c>collection_time</c> in <c>(W_old, W_new]</c>, where
/// <c>W_new = least(W_old + 1 day, now - 1 hour)</c>, adds them into the rollup and moves the watermark to <c>W_new</c>, in one
/// transaction. The hour covers a collector transaction that commits after the tick with an earlier <c>collection_time</c>. A server with no
/// watermark starts at its oldest <c>collection_time</c> and works forward a day at a time, oldest first, so the watermark only moves
/// forward. A tick runs rounds of one step per server until nothing is due, <see cref="MaxStepsPerTick"/> steps or
/// <see cref="MaxTickDuration"/> have passed. Retention deletes the days more than <see cref="GcMarginDays"/> before job_history's retention
/// edge from the rollup and the days table, and moves <c>gc_before</c> of every watermark row up to that first day kept in the same
/// transaction (it never moves back, so a retention that was lowered and raised again does not make the deleted days look whole). The
/// watermark row stays.</para>
///
/// <para><b>Retention and the reader (#9110).</b> The raw rows go by <c>collection_time</c> (a chunk at a time, or a time-sliced delete), the
/// rollup by run day, so the two can disagree about a day near the window's start. The margin keeps the window's partial first day, whatever
/// the server's UTC offset. The reader takes a server's summary only when <c>gc_before</c> is not past the window's first day and the window
/// starts no earlier than the oldest <c>collection_time</c> that job_history's retention is sure to keep; otherwise it reads that server raw.</para>
/// </summary>
public static class JobOutcomeDaily
{
    /// <summary>The per-(server, day, job) summary table.</summary>
    public const string Table = "collect.job_outcome_daily";

    /// <summary>The per-(server, day) newest collection_time table.</summary>
    public const string DaysTable = "collect.job_outcome_daily_days";

    /// <summary>The per-server watermark table.</summary>
    public const string WatermarkTable = "collect.job_outcome_daily_watermark";

    /// <summary>The migration rung that creates the three tables.</summary>
    public const int Version = 282;

    /// <summary>Creates the three tables, empty. Idempotent. No GRANT: the <c>collect</c> schema's blanket default privileges cover a new table.</summary>
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

CREATE TABLE IF NOT EXISTS collect.job_outcome_daily_days
(
    server_id integer NOT NULL,
    run_day date NOT NULL,
    max_collection_time timestamptz NOT NULL,
    CONSTRAINT pk_job_outcome_daily_days PRIMARY KEY (server_id, run_day)
);

CREATE TABLE IF NOT EXISTS collect.job_outcome_daily_watermark
(
    server_id integer NOT NULL,
    collected_through timestamptz NOT NULL,
    gc_before date NULL,
    CONSTRAINT pk_job_outcome_daily_watermark PRIMARY KEY (server_id)
);

/* The rung is unreleased: a store that already ran it before gc_before was added gets the column here. */
ALTER TABLE collect.job_outcome_daily_watermark ADD COLUMN IF NOT EXISTS gc_before date NULL;";

    /// <summary>The wall-clock margin behind now that a step never reaches, so a collector transaction that commits late is still ahead of it.</summary>
    public static readonly TimeSpan Margin = TimeSpan.FromHours(1);

    /// <summary>The most collection_time one step covers.</summary>
    public static readonly TimeSpan StepSpan = TimeSpan.FromDays(1);

    /// <summary>A server with a watermark is stepped only when it can move at least this far, so the margin's slow drift never makes a tick loop.</summary>
    public static readonly TimeSpan MinAdvance = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The most steps one tick runs: high enough that <see cref="MaxTickDuration"/> is the bound that holds (#9110). A step reads one
    /// server-day of <c>collection_time</c> and is cheap: on a seeded 19.3M-row, 172-chunk store the whole backfill took 20,576
    /// steps in 35 s (median 0.7 ms a step). A cap of 60 would have taken about 14 days of hourly ticks to catch that store up.
    /// </summary>
    public const int MaxStepsPerTick = 100_000;

    /// <summary>No new step starts once a tick has run this long.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    /// <summary>The server-side <c>statement_timeout</c> each step's transaction runs under.</summary>
    public const int StepStatementTimeoutSeconds = 60;

    private const int CommandTimeoutSeconds = StepStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{StepStatementTimeoutSeconds}s'";

    /// <summary>job_history's retention in days: the fleet override when set, else the default (the horizon the raw table's own purge uses).</summary>
    internal static readonly string RetentionDaysSql = "SELECT " + DarlingRetentionHorizons.FleetRetentionDaysSql("job_history");

    /// <summary>The database clock.</summary>
    internal const string DatabaseNowSql = "SELECT now()";

    /// <summary>
    /// The steps due, oldest watermark first. $1 now (timestamptz), $2 the cap, $3 <see cref="Margin"/>, $4 <see cref="StepSpan"/>, $5
    /// <see cref="MinAdvance"/>. Enabled servers only. <c>w_old</c> is the watermark; for a server with none it is the instant just before its
    /// oldest <c>collection_time</c>, or the margin behind now when it holds no row (it then gets a watermark with nothing to add). A server
    /// with a watermark is due when <c>w_new</c> is at least $5 past it.
    /// </summary>
    internal const string PlanSql = @"
SELECT s.server_id, s.w_old, least(s.w_old + $4, $1 - $3) AS w_new
FROM
(
    SELECT sv.server_id, (w.server_id IS NOT NULL) AS has_wm,
           COALESCE(
               w.collected_through,
               (SELECT min(jh.collection_time) FROM collect.job_history AS jh WHERE jh.server_id = sv.server_id) AT TIME ZONE 'UTC' - interval '1 microsecond',
               $1 - $3) AS w_old
    FROM collect.servers AS sv
    LEFT JOIN collect.job_outcome_daily_watermark AS w ON w.server_id = sv.server_id
    WHERE sv.is_enabled
) AS s
WHERE NOT s.has_wm OR least(s.w_old + $4, $1 - $3) >= s.w_old + $5
ORDER BY s.w_old, s.server_id
LIMIT $2;";

    /// <summary>
    /// Adds the rows with <c>collection_time</c> in <c>($2, $3]</c> (timestamptz, read at UTC) to the rollup and the days table, and returns the
    /// number of rollup rows touched. $1 server_id. The range is two <c>collection_time</c> bounds, so a chunk outside it is never read.
    /// A job with no id has no key to roll up under and a row with no <c>run_datetime</c> has no day; neither can match the read's per-job,
    /// per-window predicates either.
    /// </summary>
    internal const string StepSql = @"
WITH src AS MATERIALIZED
(
    SELECT jh.job_id, jh.run_datetime::date AS run_day, jh.run_duration_seconds, jh.run_datetime, jh.collection_time
    FROM collect.job_history AS jh
    WHERE jh.server_id = $1
    AND   jh.collection_time > ($2 AT TIME ZONE 'UTC')
    AND   jh.collection_time <= ($3 AT TIME ZONE 'UTC')
    AND   jh.step_id = 0
    AND   jh.run_status = 1
    AND   jh.job_id IS NOT NULL
    AND   jh.run_datetime IS NOT NULL
),
rolled AS
(
    INSERT INTO collect.job_outcome_daily AS t (server_id, job_id, run_day, success_count, success_duration_sum, last_success_run_local)
    SELECT $1, s.job_id, s.run_day, count(s.run_duration_seconds), COALESCE(sum(s.run_duration_seconds), 0)::bigint, max(s.run_datetime)
    FROM src AS s
    GROUP BY s.job_id, s.run_day
    ON CONFLICT (server_id, run_day, job_id) DO UPDATE
    SET success_count = t.success_count + EXCLUDED.success_count,
        success_duration_sum = t.success_duration_sum + EXCLUDED.success_duration_sum,
        last_success_run_local = GREATEST(t.last_success_run_local, EXCLUDED.last_success_run_local)
    RETURNING 1
),
days AS
(
    INSERT INTO collect.job_outcome_daily_days AS d (server_id, run_day, max_collection_time)
    SELECT $1, s.run_day, max(s.collection_time) AT TIME ZONE 'UTC'
    FROM src AS s
    GROUP BY s.run_day
    ON CONFLICT (server_id, run_day) DO UPDATE
    SET max_collection_time = GREATEST(d.max_collection_time, EXCLUDED.max_collection_time)
    RETURNING 1
)
SELECT (SELECT count(*) FROM rolled), (SELECT count(*) FROM days);";

    /// <summary>Moves a server's watermark to $2 (timestamptz). $1 server_id. Runs in the step's transaction.</summary>
    internal const string UpsertWatermarkSql = @"
INSERT INTO collect.job_outcome_daily_watermark (server_id, collected_through)
VALUES ($1, $2)
ON CONFLICT (server_id) DO UPDATE
SET collected_through = GREATEST(collect.job_outcome_daily_watermark.collected_through, EXCLUDED.collected_through);";

    /// <summary>
    /// Removes the days table's rows before the first day kept and returns their keys. $1 the first day kept (<see cref="GcFirstDayKept"/>). The rollup rows go key by key with
    /// <see cref="GcRolledSql"/> in the same transaction: the days table is one row a day per server, the rollup one per job as well, and a
    /// delete on the rollup with its own predicate cannot use its key (the shape <c>QueryStoreTopDaily</c> took in #5507).
    /// </summary>
    internal const string GcDaysSql = @"
DELETE FROM collect.job_outcome_daily_days
WHERE run_day < $1
RETURNING server_id, run_day;";

    /// <summary>Removes one expired (server, day) of rollup rows by the two leading columns of its key. $1 server_id, $2 day.</summary>
    internal const string GcRolledSql = @"
DELETE FROM collect.job_outcome_daily
WHERE server_id = $1::integer
AND   run_day = $2::date;";

    /// <summary>
    /// Moves <c>gc_before</c> of every watermark row up to $1 (the first day kept), never back. Runs in <see cref="GcAsync"/>'s transaction, with
    /// the deletes, so a reader never sees the deletes without it.
    /// </summary>
    internal const string GcMarkSql = @"
UPDATE collect.job_outcome_daily_watermark
SET gc_before = GREATEST(COALESCE(gc_before, $1::date), $1::date);";

    /// <summary>One planned step: the server and the range <c>(WOld, WNew]</c> of <c>collection_time</c>.</summary>
    public readonly record struct DueStep(int ServerId, DateTime WOld, DateTime WNew);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int Steps, int Failed, long DaysRemoved, bool Deferred = false);

    /// <summary>The first day kept for <paramref name="nowUtc"/>: the retention edge as a date.</summary>
    internal static DateOnly RetentionEdge(DateTime nowUtc, int retentionDays) => DateOnly.FromDateTime(nowUtc.Date.AddDays(-retentionDays));

    /// <summary>
    /// How many days before the retention edge retention still keeps (#9110). The window's first day is the day of <c>since + the server's UTC
    /// offset</c>, which is up to 14 hours before the UTC day of the edge and so one day earlier at most, and the edge itself moves with the UTC
    /// hour; two days keep that first day whole at any offset.
    /// </summary>
    internal const int GcMarginDays = 2;

    /// <summary>The first day <see cref="GcAsync"/> keeps for <paramref name="nowUtc"/>: <see cref="GcMarginDays"/> before the retention edge.</summary>
    internal static DateOnly GcFirstDayKept(DateTime nowUtc, int retentionDays) => RetentionEdge(nowUtc, retentionDays).AddDays(-GcMarginDays);

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    /// <summary>The steps due at <paramref name="nowUtc"/>, oldest watermark first, at most <paramref name="cap"/>.</summary>
    public static async Task<IReadOnlyList<DueStep>> PlanStepsAsync(
        NpgsqlConnection connection, DateTime nowUtc, int cap, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var steps = new List<DueStep>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = Utc(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = cap });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = Margin });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = StepSpan });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = MinAdvance });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            steps.Add(new DueStep(reader.GetInt32(0), reader.GetDateTime(1), reader.GetDateTime(2)));
        }

        return steps;
    }

    /// <summary>Removes the days before <see cref="GcFirstDayKept"/> from the rollup and the days table, and moves every watermark row's
    /// <c>gc_before</c> up to it, in ONE transaction; returns how many (server, day) rows went. The watermark rows stay.</summary>
    public static async Task<long> GcAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var firstDayKept = GcFirstDayKept(nowUtc, retentionDays);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var expired = new List<(int ServerId, DateOnly Day)>();
        await using (var command = new NpgsqlCommand(GcDaysSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = firstDayKept });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                expired.Add((reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            }
        }

        foreach (var (serverId, day) in expired)
        {
            await using var rolled = new NpgsqlCommand(GcRolledSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            rolled.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            rolled.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await rolled.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var mark = new NpgsqlCommand(GcMarkSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = firstDayKept });
            await mark.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return expired.Count;
    }

    /// <summary>
    /// One step in one transaction: adds the rows with <c>collection_time</c> in <c>(wOld, wNew]</c> to the rollup and the days table and moves the
    /// server's watermark to <paramref name="wNew"/>. Returns the number of rollup rows touched.
    /// </summary>
    public static async Task<long> StepAsync(
        NpgsqlConnection connection, int serverId, DateTime wOld, DateTime wNew, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        long touched;
        await using (var step = new NpgsqlCommand(StepSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            step.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            step.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = Utc(wOld) });
            step.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = Utc(wNew) });
            await using var reader = await step.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            touched = reader.GetInt64(0);
        }

        await using (var watermark = new NpgsqlCommand(UpsertWatermarkSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            watermark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            watermark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = Utc(wNew) });
            await watermark.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return touched;
    }

    /// <summary>
    /// One maintenance tick: read job_history's retention, remove expired days, then run rounds of one step per due server until nothing is due,
    /// <see cref="MaxStepsPerTick"/> steps have run or <see cref="MaxTickDuration"/> has passed. "Now" is <paramref name="nowUtc"/> when given
    /// and the database clock otherwise. A step that fails is logged (naming the server) and the next tick plans it again.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime? nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime?, ILogger, CancellationToken)"/> with the elapsed time since the tick began
    /// supplied by <paramref name="elapsed"/>, read before each step starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime? nowUtc, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int steps = 0, failed = 0;
        var deferred = false;

        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.JobOutcomeDaily, cancellationToken);
        DateTime now;
        if (nowUtc is { } given)
        {
            now = Utc(given);
        }
        else
        {
            await using var clock = new NpgsqlCommand(DatabaseNowSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            now = Utc(Convert.ToDateTime(await clock.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture));
        }

        int retentionDays;
        await using (var retention = new NpgsqlCommand(RetentionDaysSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            retentionDays = Convert.ToInt32(await retention.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        var removed = await GcAsync(connection, now, retentionDays, cancellationToken);

        var failedServers = new HashSet<int>();
        var progressed = true;
        while (progressed && !deferred)
        {
            progressed = false;
            var plan = await PlanStepsAsync(connection, now, MaxStepsPerTick, cancellationToken);
            foreach (var item in plan)
            {
                if (failedServers.Contains(item.ServerId))
                {
                    continue;
                }

                if (steps >= MaxStepsPerTick || elapsed() >= MaxTickDuration)
                {
                    deferred = true;
                    break;
                }

                try
                {
                    await StepAsync(connection, item.ServerId, item.WOld, item.WNew, cancellationToken);
                    steps++;
                    progressed = true;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failed++;
                    failedServers.Add(item.ServerId);
                    logger.LogWarning(ex, "Job outcome daily summary: stepping server {ServerId} failed and is retried next tick: {Message}",
                        item.ServerId, ex.Message);
                }
            }
        }

        var result = new TickResult(steps, failed, removed, deferred);
        logger.LogInformation("Job outcome daily summary: {Steps} step(s), failed {Failed}, deferred {Deferred}, removed {Removed} day(s), {ElapsedMs} ms",
            steps, failed, deferred, removed, (long)elapsed().TotalMilliseconds);
        return result;
    }
}
