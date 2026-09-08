/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store self-metrics reads for the MCP/web surface (#2068) — over the series the hourly
/// <c>StoreSelfMetrics</c> sweep persists into <c>collect.store_metrics</c>. Two reads: the latest
/// snapshot per object (one row per hypertable / dimension / the store itself, at each object's newest
/// metric_time), and the daily series — the LAST sample of each object per day, which is what a
/// growth/forecast question wants (the hourly grain exists so a single missed run costs nothing, not
/// because anyone forecasts by the hour) and what a MAXIMUM question cannot use: the day's largest
/// reading is dropped rather than smoothed, so a peak that crossed a threshold is invisible at this
/// grain (#3119). Plus the one derivable number the issue called out: the
/// per-server daily ingest rate (whole-store daily growth divided by the enabled-server count), computed in
/// <see cref="ComputeDailyGrowth"/> — pure, so it is unit-tested without a store.
///
/// <para>Plus one read that is not a metric at all: <see cref="JobExecutionLoggingSql"/> asks whether
/// <c>timescaledb_information.job_history</c> — the route the tool's description sends a maximum question
/// to, since this series cannot answer one — is actually recording (#3175). It qualifies the redirect, so
/// a caller who follows it can tell a census from an empty table.</para>
/// </summary>
internal static class DarlingStoreMetricsReader
{
    /// <summary>Latest snapshot per object — DISTINCT ON takes each (kind, name)'s newest row. No
    /// parameters: the newest row per object is wanted regardless of window.</summary>
    public const string StoreMetricsLatestSql = @"
SELECT DISTINCT ON (object_kind, object_name)
    object_kind,
    object_name,
    metric_time,
    total_bytes,
    compressed_before_bytes,
    compressed_after_bytes,
    chunk_count,
    row_count,
    enabled_server_count,
    last_run_duration_ms,
    schedule_interval_ms,
    total_runs,
    total_failures
FROM collect.store_metrics
ORDER BY object_kind, object_name, metric_time DESC";

    /// <summary>The daily series — the LAST sample of each object per day (DISTINCT ON over the day
    /// bucket, newest first within it), so each day contributes one settled point per object rather than
    /// 24 near-duplicates. A last snapshot and not a per-day maximum: the tiebreak takes the newest row
    /// in the bucket, so the day's largest value is discarded unless it happens to be the last one, and
    /// <c>get_store_metrics</c>' description says so where a caller reads it (#3119). $1 window start
    /// (naive UTC).</summary>
    public const string StoreMetricsDailySql = @"
SELECT DISTINCT ON (object_kind, object_name, date_trunc('day', metric_time))
    object_kind,
    object_name,
    date_trunc('day', metric_time) AS day,
    total_bytes,
    compressed_before_bytes,
    compressed_after_bytes,
    chunk_count,
    row_count,
    enabled_server_count,
    last_run_duration_ms,
    schedule_interval_ms,
    total_runs,
    total_failures
FROM collect.store_metrics
WHERE metric_time >= $1
ORDER BY object_kind, object_name, date_trunc('day', metric_time), metric_time DESC";

    /// <summary>
    /// Whether <c>timescaledb_information.job_history</c> — the route this tool's description sends a
    /// MAXIMUM question to, because the daily series cannot answer one — is actually recording (#3175).
    ///
    /// <para><b>Why this read exists at all.</b> An empty <c>job_history</c> and a quiet fleet are the same
    /// result set. TimescaleDB records nothing there unless
    /// <c>timescaledb.enable_job_execution_logging</c> is on, and it defaults OFF, so a maximum over the
    /// table returns zero rows on an unhealed store and that reads as <i>"no run exceeded the line"</i>.
    /// Redirecting a caller to an instrument without telling them whether it is switched on is how the
    /// wrong conclusion gets drawn from a correct query.</para>
    ///
    /// <para><b>The EFFECTIVE value and its source, never the presence of the managed conf block.</b>
    /// <c>postgresql.auto.conf</c> is read after <c>postgresql.conf</c>, so an
    /// <c>ALTER SYSTEM SET timescaledb.enable_job_execution_logging = off</c> beats the v11 append —
    /// measured, with the appended block last in <c>postgresql.conf</c> and the effective value still
    /// <c>off</c>, <c>sourcefile</c> naming <c>postgresql.auto.conf</c>. A marker check would have called
    /// that store healed. <c>source</c> and <c>sourcefile</c> ride along so an <c>off</c> that somebody
    /// chose is distinguishable from an <c>off</c> nobody ever touched (<c>source = default</c>, which is
    /// the shape the unhealed field store showed).</para>
    ///
    /// <para><b>ZERO ROWS is a real answer here, not a failure.</b> The GUC is registered by the
    /// TimescaleDB library, so <c>pg_settings</c> has no row for it on a plain-PostgreSQL store — measured
    /// on PG17 with no extension: no <c>pg_settings</c> row, <c>current_setting(name, true)</c> NULL, and
    /// <c>timescaledb_information.job_history</c> absent. That is
    /// <see cref="JobExecutionLoggingStatus.NotRegistered"/> and it is reported as such, distinct from
    /// <see cref="JobExecutionLoggingStatus.Off"/>. <c>current_setting</c> was not used: its non-missing_ok
    /// form RAISES on an unregistered parameter and its missing_ok form flattens "unregistered" into the
    /// same NULL an error would produce, where a row count of zero says exactly one thing. $1 setting name.
    /// </para>
    ///
    /// <para><b><c>sourcefile</c> IS EXPECTED TO BE NULL HERE, and that is PostgreSQL, not a fault.</b>
    /// <c>pg_settings.sourcefile</c> and <c>sourceline</c> are visible only to a superuser or a role with
    /// <c>pg_read_all_settings</c>; the MCP host connects as the least-privilege <c>mcp</c> role, so it
    /// gets a NULL there. Measured on 2.30.0/PG17 with a plain LOGIN role: <c>setting</c>, <c>source</c>,
    /// <c>boot_val</c> and <c>context</c> all came back, <c>sourcefile</c> came back empty. This is why
    /// <see cref="JobExecutionLoggingReading.OffByExplicitOverride"/> keys on <c>source</c> rather than on
    /// <c>sourcefile</c> — the derived answer must not depend on a column the caller may not be allowed to
    /// see. Do not "fix" the NULL by escalating the read's privileges.</para>
    /// </summary>
    public const string JobExecutionLoggingSql = @"
SELECT
    setting,
    source,
    sourcefile
FROM pg_settings
WHERE name = $1";

    /// <summary>
    /// The four distinguishable states of the <c>job_history</c> precondition. Four rather than a bool
    /// because three of them would otherwise collapse into "not on", and the whole defect being reported
    /// is one absence being mistaken for another: a store that was told not to record, a server that has
    /// no such setting, and a probe that did not complete call for three different readings of an empty
    /// <c>job_history</c>.
    /// </summary>
    public enum JobExecutionLoggingStatus
    {
        /// <summary>The probe did not complete. Whether <c>job_history</c> is recording is UNKNOWN —
        /// never reported as off, which would be an answer this read did not obtain.</summary>
        Unreadable,

        /// <summary>No <c>pg_settings</c> row: the TimescaleDB library is not loaded, so the GUC does not
        /// exist and neither does <c>timescaledb_information.job_history</c>.</summary>
        NotRegistered,

        /// <summary>Registered and off. <c>job_history</c> is not recording; an empty result from it means
        /// the instrument is off, not that nothing happened.</summary>
        Off,

        /// <summary>Registered and on. <c>job_history</c> carries one row per run <b>from the point logging
        /// was turned on</b> — never before it, because nothing was written to recover.</summary>
        On,
    }

    /// <summary>
    /// One reading of the <c>job_history</c> precondition: the state, plus the raw
    /// <c>pg_settings</c> columns behind it so a caller can see WHICH file won. One value carrying the
    /// whole answer, so a caller cannot take the state and drop the provenance.
    /// </summary>
    public sealed record JobExecutionLoggingReading(
        JobExecutionLoggingStatus Status,
        string? Setting,
        string? Source,
        string? SourceFile)
    {
        /// <summary><c>true</c> only for <see cref="JobExecutionLoggingStatus.On"/> — the one state in
        /// which a maximum over <c>job_history</c> is a census rather than an artefact.</summary>
        public bool Recording => Status == JobExecutionLoggingStatus.On;

        /// <summary>
        /// <c>true</c> when the GUC is off and something SET it off — a source other than
        /// <c>default</c>. The v11 conf append cannot win against that (an <c>ALTER SYSTEM</c> lands in
        /// <c>postgresql.auto.conf</c>, which PostgreSQL reads last), so the two cases need different
        /// advice: one heals itself on the next service-owned start, the other needs the override removed.
        /// </summary>
        public bool OffByExplicitOverride =>
            Status == JobExecutionLoggingStatus.Off
            && Source is not null
            && !string.Equals(Source, "default", StringComparison.Ordinal);
    }

    /// <summary>
    /// Reads <see cref="JobExecutionLoggingSql"/>. Failure-isolated to
    /// <see cref="JobExecutionLoggingStatus.Unreadable"/> rather than to a thrown exception or to a
    /// plausible-looking <c>Off</c>: a precondition check must never be able to fail the read it qualifies,
    /// and must never report a state it did not measure.
    ///
    /// <para>Takes no logger, unlike the failure-isolated reads in <c>TimescaleSupport</c>, because the
    /// failure is reported to the one consumer that exists: <c>Unreadable</c> and its note go into the
    /// response the caller is already reading. A log line would put the fault somewhere the person asking
    /// the question is not looking.</para>
    /// </summary>
    public static async Task<JobExecutionLoggingReading> GetJobExecutionLoggingAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = postgres.CreateCommand(JobExecutionLoggingSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(StoreSelfMetrics.JobExecutionLoggingSetting);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                /* No row = the GUC is not registered on this server. A real answer, and the reason this
                   probe counts rows instead of calling current_setting(). */
                return new JobExecutionLoggingReading(JobExecutionLoggingStatus.NotRegistered, null, null, null);
            }

            var setting = reader.IsDBNull(0) ? null : reader.GetString(0);
            var source = reader.IsDBNull(1) ? null : reader.GetString(1);
            var sourceFile = reader.IsDBNull(2) ? null : reader.GetString(2);

            /* PostgreSQL renders a bool GUC as exactly "on" or "off" in pg_settings.setting. Anything else
               is a shape this read does not understand, and claiming "off" for it would be inventing a
               measurement — so it reports Unreadable and keeps the raw value for the caller to see. */
            var status = setting switch
            {
                "on" => JobExecutionLoggingStatus.On,
                "off" => JobExecutionLoggingStatus.Off,
                _ => JobExecutionLoggingStatus.Unreadable,
            };

            return new JobExecutionLoggingReading(status, setting, source, sourceFile);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Cancellation is deliberately NOT isolated, matching every other broad catch in this codebase.
               A caller who cancelled did not ask "is logging on?" and get no answer — they asked us to stop,
               and reporting that as Unreadable would put a measurement-shaped word on an act of the caller's
               own. Everything else becomes Unreadable, which the response says out loud. */
            return new JobExecutionLoggingReading(JobExecutionLoggingStatus.Unreadable, null, null, null);
        }
    }

    /// <summary>One object's newest self-metrics row. The four job fields (#2136, V56) are non-null only
    /// on <c>background_job</c> rows — every other kind leaves them NULL, as the sweep writes them.</summary>
    public sealed record StoreMetricRow(
        string ObjectKind,
        string ObjectName,
        DateTime MetricTime,
        long? TotalBytes,
        long? CompressedBeforeBytes,
        long? CompressedAfterBytes,
        int? ChunkCount,
        long? RowCount,
        int? EnabledServerCount,
        long? LastRunDurationMs = null,
        long? ScheduleIntervalMs = null,
        long? TotalRuns = null,
        long? TotalFailures = null);

    /// <summary>One object's settled point for one day (the day's last sample). Job fields as on
    /// <see cref="StoreMetricRow"/>.</summary>
    public sealed record StoreMetricDailyPoint(
        string ObjectKind,
        string ObjectName,
        DateTime Day,
        long? TotalBytes,
        long? CompressedBeforeBytes,
        long? CompressedAfterBytes,
        int? ChunkCount,
        long? RowCount,
        int? EnabledServerCount,
        long? LastRunDurationMs = null,
        long? ScheduleIntervalMs = null,
        long? TotalRuns = null,
        long? TotalFailures = null);

    /// <summary>One day's whole-store growth: the byte delta from the previous day's settled point, and
    /// that delta divided by the day's enabled-server count — the number onboarding N servers multiplies.
    /// <c>PerServerBytes</c> is null when the server count is unknown or zero (a delta over no servers is
    /// not a rate).</summary>
    public sealed record DailyGrowthPoint(DateTime Day, long DeltaBytes, double? PerServerBytes);

    public static async Task<List<StoreMetricRow>> GetLatestAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var rows = new List<StoreMetricRow>();
        await using var command = postgres.CreateCommand(StoreMetricsLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreMetricRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetInt64(12)));
        }

        return rows;
    }

    public static async Task<List<StoreMetricDailyPoint>> GetDailyAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<StoreMetricDailyPoint>();
        await using var command = postgres.CreateCommand(StoreMetricsDailySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreMetricDailyPoint(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11),
                reader.IsDBNull(12) ? null : reader.GetInt64(12)));
        }

        return rows;
    }

    /// <summary>
    /// The whole-store daily growth series from the store-kind daily points, ordered by day: each day's
    /// byte delta from the previous day's settled point, plus the per-server rate (delta divided by THAT
    /// day's enabled-server count — the day being measured, not the baseline day). Pure. The first day has
    /// no predecessor and yields no point; a day whose total or predecessor's total is unrecorded is
    /// skipped rather than invented; the per-server rate is null (never zero, never infinity) when the
    /// server count is missing or zero. Deltas can be NEGATIVE — retention drops and compression passes
    /// shrink the store, and hiding that would misstate the trend a forecast extrapolates.
    /// </summary>
    public static List<DailyGrowthPoint> ComputeDailyGrowth(IReadOnlyList<StoreMetricDailyPoint> storePoints)
    {
        if (storePoints is null)
        {
            throw new ArgumentNullException(nameof(storePoints));
        }

        var growth = new List<DailyGrowthPoint>();
        for (var i = 1; i < storePoints.Count; i++)
        {
            var previous = storePoints[i - 1];
            var current = storePoints[i];
            if (previous.TotalBytes is not { } before || current.TotalBytes is not { } after)
            {
                continue;
            }

            var delta = after - before;
            double? perServer = current.EnabledServerCount is > 0
                ? delta / (double)current.EnabledServerCount.Value
                : null;

            growth.Add(new DailyGrowthPoint(current.Day, delta, perServer));
        }

        return growth;
    }
}
