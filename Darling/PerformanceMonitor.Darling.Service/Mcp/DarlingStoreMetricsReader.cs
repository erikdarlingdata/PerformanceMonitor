/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
/// <para>Plus two reads that are not metrics at all: <see cref="JobExecutionLoggingSql"/> asks whether
/// <c>timescaledb_information.job_history</c> — the route the tool's description sends a maximum question
/// to, since this series cannot answer one — is switched on (#3175), and <see cref="JobHistoryEvidenceSql"/>
/// asks whether the connection doing the asking would SEE its rows and how many it does see (#3574). Together
/// they qualify the redirect, so a caller who follows it can tell a census from an empty table — and can
/// tell an empty table from a table the view is hiding from them. Where that connection is one the view
/// shows nothing (managed mode's <c>mcp</c> role), <see cref="OwnerJobHistoryEvidence"/> decodes the
/// OWNER's reading that the hourly sweep persisted into the series, so the block can still carry a
/// measurement and say whose it is.</para>
///
/// <para>And, since #3582, the reads that let the tool state its own COVERAGE: <see cref="ComputeInventory"/>
/// reconciles the per-object rows of one sweep against that sweep's <c>pg_database_size</c>,
/// <see cref="ContinuousAggregateStateSql"/> reads the catalog facts about each aggregate the series does
/// not carry (compression enabled, which policies exist), and <see cref="LargestUnenumeratedSql"/> names
/// the biggest relations inside the <c>other</c> row so that number is something a reader can act on.</para>
///
/// <para>And, since #3783, two readings that are about the store's own physical health rather than its
/// size: <see cref="ToastFacts"/> turns a dimension row's <c>toast_bytes</c> / <c>toast_live_bytes</c> pair
/// (V137) into a utilisation percentage and a sentence that says what it means — or why it cannot be
/// computed — and <see cref="CheckpointerReading"/> turns the two newest <c>checkpointer</c> rows'
/// CUMULATIVE counters into the interval's write-phase and sync-phase milliseconds and requested-checkpoint
/// count, the differences the sweep deliberately does not store (the reasoning is on
/// <see cref="StoreSelfMetrics.CheckpointerInsertSql"/>). Both are pure over rows already in hand and both
/// are what the self-alert evaluator judges, so the tool and the alert cannot disagree about a number. Since
/// #3955 the checkpointer pair also carries each row's postmaster start time, and an interval that spans a
/// restart states no delta at all (<see cref="PostmasterRestart"/> says why and how that is decided).</para>
///
/// <para>And, since #3903, the pure half of the summary-first payload: <see cref="SelectObjects"/> resolves the
/// tool's two filters to a view, <see cref="OrderForList"/> and <see cref="OrderByGrowth"/> rank its lists, and
/// <see cref="ComputeWindowDeltas"/> turns each object's daily series into the one change over the window its
/// row carries in place of the series. No new SQL: the two reads above feed all three views.</para>
/// </summary>
internal static class DarlingStoreMetricsReader
{
    /// <summary>
    /// Latest snapshot per object — each (kind, name)'s newest row. No parameters: the newest row per object
    /// is wanted regardless of window. The two trailing TOAST columns (V137, #3783) are non-NULL on
    /// <c>dimension</c> rows only; the checkpointer's three are deliberately NOT projected here — they are
    /// cumulative counters that mean nothing on one row, and <see cref="CheckpointerPairSql"/> reads the two
    /// rows a difference needs.
    ///
    /// <para><b>#3934: a skip-scan over <c>idx_store_metrics_kind_name_time</c>
    /// (<c>PgTableTuning.Statements</c>), not <c>DISTINCT ON</c> over the whole table.</b> The table keeps
    /// 400 days of hourly sweeps at ~250 objects/sweep, and the only index used to be <c>idx_store_metrics_time
    /// (metric_time)</c> alone — nothing to satisfy <c>ORDER BY object_kind, object_name, metric_time DESC</c>,
    /// so <c>DISTINCT ON</c> sorted every retained row: 7,772 ms and an external merge spilling ~180 MB at full
    /// retention on a CI-sized rig (2.4M rows), 225 ms on DARLING01 today (83,355 rows) — a cost that is a
    /// function of store AGE, not fleet size, and arrives gradually. The recursive CTE below is the standard
    /// PostgreSQL "loose index scan": <c>objects</c> walks the DISTINCT (kind, name) pairs by repeatedly asking
    /// the index for the next pair strictly greater than the last (a handful of index descents for ~250
    /// objects, never a scan of the data), and the outer <c>LATERAL</c> asks the SAME index for that pair's
    /// newest row (<c>ORDER BY metric_time DESC LIMIT 1</c>, one descent each). Both queries the index can
    /// answer directly — the composite's three columns are exactly the ORDER BY this read has always had.
    /// Measured on the same seed: 7,772 ms to 15 ms, identical 251 rows.</para>
    ///
    /// <para><b>Why an index rather than a migration.</b> Results-invariant — every existing row already
    /// carries these three columns, so there is no backfill and no version to gate the Viewer's connect-time
    /// schema check on. Erik's ruling on the issue: this is exactly what the store-object convergence
    /// registry's Tuning stage exists for (#3817), which already creates the composer's covering indexes
    /// idempotently at every start and hourly, on every store shape (#3913).</para>
    /// </summary>
    public const string StoreMetricsLatestSql = @"
WITH RECURSIVE objects AS (
    (
        SELECT object_kind, object_name
        FROM collect.store_metrics
        ORDER BY object_kind, object_name
        LIMIT 1
    )
    UNION ALL
    SELECT next_object.object_kind, next_object.object_name
    FROM objects
    CROSS JOIN LATERAL
    (
        SELECT object_kind, object_name
        FROM collect.store_metrics
        WHERE (object_kind, object_name) > (objects.object_kind, objects.object_name)
        ORDER BY object_kind, object_name
        LIMIT 1
    ) AS next_object
)
SELECT
    latest.object_kind,
    latest.object_name,
    latest.metric_time,
    latest.total_bytes,
    latest.compressed_before_bytes,
    latest.compressed_after_bytes,
    latest.chunk_count,
    latest.row_count,
    latest.enabled_server_count,
    latest.last_run_duration_ms,
    latest.schedule_interval_ms,
    latest.total_runs,
    latest.total_failures,
    latest.toast_bytes,
    latest.toast_live_bytes
FROM objects
CROSS JOIN LATERAL
(
    SELECT
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
        total_failures,
        toast_bytes,
        toast_live_bytes
    FROM collect.store_metrics
    WHERE object_kind = objects.object_kind
    AND   object_name = objects.object_name
    ORDER BY metric_time DESC
    LIMIT 1
) AS latest
ORDER BY latest.object_kind, latest.object_name";

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
    total_failures,
    toast_bytes,
    toast_live_bytes
FROM collect.store_metrics
WHERE metric_time >= $1
ORDER BY object_kind, object_name, date_trunc('day', metric_time), metric_time DESC";

    /// <summary>
    /// Whether <c>timescaledb_information.job_history</c> — the route this tool's description sends a
    /// MAXIMUM question to, because the daily series cannot answer one — is actually recording (#3175).
    ///
    /// <para><b>Why this read exists at all.</b> An empty <c>job_history</c> and a quiet fleet are the same
    /// result set. TimescaleDB records a SUCCESSFUL run there only while
    /// <c>timescaledb.enable_job_execution_logging</c> is on, and it defaults OFF — a FAILED run's row is
    /// written regardless (2.28.1 <c>job_stat_history.c</c>: the failure path logs unconditionally, the
    /// success path is gated; measured on a fresh rig with the GUC off, the telemetry job's one failure was
    /// the view's one row) — so a maximum over the table on an unhealed store is a census of FAILURES
    /// that reads as <i>"no run exceeded the line"</i>. Redirecting a caller to an instrument without telling
    /// them which runs it is writing is how the wrong conclusion gets drawn from a correct query.</para>
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
    /// <para><b>ZERO ROWS is a real answer here, not a failure</b> — and it has TWO causes with one
    /// consequence, which is why the note says so rather than naming one. Plain PostgreSQL has no such GUC
    /// (measured on PG17: no <c>pg_settings</c> row, <c>current_setting(name, true)</c> NULL,
    /// <c>timescaledb_information.job_history</c> absent). But so does a cluster that HAS the library
    /// preloaded while the connected database has no <c>timescaledb</c> extension: the preload loads the
    /// LOADER, and the loader pulls in the versioned library — the one that defines this GUC — only for a
    /// database that has the extension. Measured on 2.30.0/PG17 from a database created
    /// <c>TEMPLATE template0</c>: ZERO rows for this GUC, while <c>timescaledb.max_background_workers</c>
    /// (loader-defined) still had one. Both cases mean <c>job_history</c> does not exist on this
    /// connection, so both are <see cref="JobExecutionLoggingStatus.NotRegistered"/> — reported distinctly
    /// from <see cref="JobExecutionLoggingStatus.Off"/>, which is the distinction that matters. A second
    /// probe to split the two causes was considered and left out: no case is known where they lead to
    /// different advice.</para>
    ///
    /// <para><c>current_setting</c> was not used: its non-missing_ok form RAISES on an unregistered
    /// parameter and its missing_ok form flattens "unregistered" into the same NULL an error would produce,
    /// where a row count of zero says exactly one thing. $1 setting name.</para>
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
    /// The evidence behind <see cref="JobExecutionLoggingReading.Recording"/>: what THIS connection actually
    /// sees in <c>timescaledb_information.job_history</c>, and whether the view would show it anything at all
    /// (#3574). The GUC read above answers <i>"is the instrument switched on"</i>; the reader's real question
    /// is <i>"will I see its output"</i>, and between those two sits a role-membership filter nothing else on
    /// this surface mentioned.
    ///
    /// <para><b>THE FALSE-QUIET ARM #3175 DID NOT KNOW ABOUT: recording on, rows present, reader filtered.</b>
    /// <c>job_history</c> is a <c>security_barrier</c> view whose definition on TimescaleDB 2.28.1 ends with
    /// <code>
    /// WHERE (pg_catalog.pg_has_role(current_user,
    ///            (SELECT pg_catalog.pg_get_userbyid(datdba)
    ///               FROM pg_catalog.pg_database
    ///              WHERE datname = current_database()),
    ///            'MEMBER') IS TRUE
    ///     OR pg_catalog.pg_has_role(current_user, owner, 'MEMBER') IS TRUE);
    /// </code>
    /// so a row is visible only to a member of the database owner's role or of the job's owner role
    /// (<c>_timescaledb_catalog.bgw_job.owner</c> is <c>regrole NOT NULL DEFAULT current_role</c> — a job
    /// belongs to whoever created it, which for every policy this product adds is the service's owner role).
    /// The base table is not a back door: <c>pre_install/tables.sql</c> ends with
    /// <c>REVOKE ALL ON _timescaledb_internal.bgw_job_stat_history FROM PUBLIC</c>, so the two filtered views
    /// (<c>job_history</c>, <c>job_errors</c>) are the only way in. <b>The trap is that the two views a reader
    /// checks FIRST are not filtered.</b> <c>timescaledb_information.jobs</c> has no WHERE clause at all and
    /// <c>job_stats</c> has none either, so a role that can see all 110 jobs and every one of their
    /// <c>total_runs</c> reasonably assumes it can see their history — and reads zero rows, forever, on a
    /// store that is recording perfectly. Measured on a production store on 2.28.1: the GUC effective
    /// <c>on</c> (sighup context, <c>pending_restart = false</c>), all 110 jobs owned by the service's owner
    /// role, two independent reads as the least-privilege <c>admin</c> role counted <b>0</b> rows ever, and
    /// the owner role's read of the same view returned every hourly run. That zero was declared "unknowable"
    /// in a real postmortem before anyone read the view's definition. Recording was never broken and the GUC
    /// never lied; the block just never said whose eyes the rows are visible to, and never proved rows exist.</para>
    ///
    /// <para><b>THIS READ IS ITSELF SUBJECT TO THE FILTER, and that is why it evaluates the predicate rather
    /// than assuming it passes.</b> In managed mode the MCP host connects as the dedicated least-privilege
    /// <c>mcp</c> role (<c>DarlingMcpHostService</c>; <c>DarlingManagedRoles</c> grants it SELECT and a few
    /// narrow writes, never membership in the owner role) — so on a managed store THIS connection is exactly
    /// the kind of reader the view shows nothing to, and a bare <c>count(*)</c> here would have reported
    /// <c>rows_observed = 0</c> on every managed store and manufactured the very contradiction this issue is
    /// about. The read therefore also returns <c>current_user</c> and evaluates the view's own two
    /// <c>pg_has_role</c> tests for it: membership in the database owner (which sees everything) and, per
    /// job, membership in that job's owner. From those the caller knows whether the count that follows is a
    /// census, a partial census, or a zero the view produced by construction — and the note says which, in
    /// so many words, naming the role. On a bring-your-own store whose MCP host connects as the owner (no
    /// postgres.mcpConnectionString, #3914) the count IS the census and the flag becomes self-proving; on a managed store the block says it
    /// cannot see, and says who can, which is the sentence that would have ended the postmortem in a minute.</para>
    ///
    /// <para><b>The population half rides in the same statement, from the UNFILTERED view.</b> A zero is
    /// readable only when the instrument would have caught the event AND the event had a chance to occur, so
    /// beside the history count the read takes, from <c>job_stats</c>, how many jobs started a run inside the
    /// same window and the newest start it knows of. TimescaleDB writes the history row at job START when
    /// the GUC is on (<c>bgw_job_stat_history_mark_start</c> inserts it with finish, pid and outcome NULL and
    /// the finish updates it; a failure is written regardless of the GUC), so a job that started inside the
    /// window while logging was on has a row with <c>start_time</c> inside the window — no waiting on a
    /// finish. <c>recording = on</c>, a reader the predicate admits, zero rows, and jobs that started in the
    /// window is the contradiction, and the note calls it one. It does not manufacture certainty about the
    /// cause: logging switched on AFTER the last of those starts is the benign shape (the v11 heal lands on a
    /// service-owned server start, and nothing before that point was written to recover), and the note says
    /// how to settle it — re-read after the next hourly run — rather than pronouncing.</para>
    ///
    /// <para><b>A FIXED 24-HOUR WINDOW, not the tool's <c>days_back</c>.</b> Three reasons, in order of
    /// weight. The question this answers is CURRENT — is the instrument writing now — and a 30-to-400-day
    /// forecasting window would let ten days of rows written after a heal hide a recording that stopped
    /// yesterday. Every job this product schedules runs at least daily (CAGG refreshes and the compression
    /// tick hourly, retention daily), so any 24-hour window on a live store contains starts, which is what
    /// lets the <c>job_stats</c> count prove the population half instead of assuming it. And the view's
    /// own <c>Job History Log Retention Policy</c> drops rows after one month by default, so a window past
    /// that would count a table the retention job had already trimmed and call the trimming "no rows".
    /// $1 is the window start.</para>
    ///
    /// <para><b>$1 IS BOUND AS <c>timestamptz</c> WITH <c>Kind = Utc</c>, WHICH IS THE INVERSE OF THIS
    /// CODEBASE'S RULE, AND DELIBERATELY SO.</b> Every collector column in the store is naive UTC and the
    /// discipline everywhere else is to strip Kind before binding, because a timestamptz parameter against a
    /// naive column makes PostgreSQL convert the naive side at the session's TimeZone. These columns are the
    /// other way round: <c>bgw_job_stat_history.execution_start</c> and <c>bgw_job_stat.last_start</c> are
    /// declared <c>TIMESTAMPTZ</c> in TimescaleDB's own catalog, so here a NAIVE bind would be the bug — the
    /// parameter, not the column, would be converted at the session zone and the window would skew by the
    /// host's offset. The parameter type is stated explicitly rather than inferred so the intent survives a
    /// caller passing a DateTime of the wrong Kind.</para>
    ///
    /// <para><c>'-infinity'</c> is TimescaleDB's never-ran sentinel in <c>last_run_started_at</c> (not NULL —
    /// the <see cref="TimescaleSupport.CompressionActivitySql"/> lesson from #1760), so the newest start
    /// NULLIFs it away; the window predicate needs no guard because <c>-infinity &gt;= $1</c> is simply false.
    /// <c>IS TRUE</c> on each <c>pg_has_role</c> mirrors the view, whose second test can meet a NULL owner
    /// (it LEFT JOINs the job catalog, so a history row whose job has since been deleted has none, and the
    /// strict function yields NULL) — a NULL must read as "not a member" rather than poison a count. Here
    /// the owner comes from the <c>jobs</c> view and cannot be NULL; the guard is kept so the two predicates
    /// stay textually the view's own.</para>
    ///
    /// <para><b>The text itself lives on <see cref="StoreSelfMetrics.JobHistoryEvidenceSql"/> and this is
    /// an alias</b>, because the string gained a second consumer with the managed-mode self-proof: the
    /// hourly sweep embeds it verbatim in <see cref="StoreSelfMetrics.JobHistoryInsertSql"/> to run it as
    /// the OWNER role and persist the answer (the Storage project cannot reference this one). Two copies of
    /// a nine-column predicate would drift without erroring; one string cannot. The reasoning stays here,
    /// beside the record that interprets the columns.</para>
    /// </summary>
    public const string JobHistoryEvidenceSql = StoreSelfMetrics.JobHistoryEvidenceSql;

    /// <summary>The evidence window <see cref="JobHistoryEvidenceSql"/> counts over, in hours. Fixed, not
    /// <c>days_back</c> — the paragraph on that constant says why. Published in the response beside the
    /// count so the number never travels without its denominator. An alias of the sweep's constant for the
    /// reason the SQL is: the owner's persisted count and this connection's live one must be over the same
    /// window or the block would compare unlike things.</summary>
    public const int JobHistoryEvidenceWindowHours = StoreSelfMetrics.JobHistoryEvidenceWindowHours;

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

        /// <summary>No <c>pg_settings</c> row. TWO causes, one consequence: the TimescaleDB library is not
        /// loaded at all, or it is loaded and the connected database has no <c>timescaledb</c> extension, so
        /// the versioned library defining this GUC was never pulled in. Either way
        /// <c>timescaledb_information.job_history</c> does not exist on this connection. See the
        /// zero-rows paragraph on <see cref="JobExecutionLoggingSql"/> for the measurement.</summary>
        NotRegistered,

        /// <summary>Registered and off. <c>job_history</c> records FAILED runs only — TimescaleDB writes a
        /// failure's row regardless of this setting and a success's only while it is on — so a result from
        /// it is a census of failures, not of runs: an empty one means no failure was recorded, not that
        /// nothing happened, and a non-empty one is not a sign the setting is secretly on.</summary>
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

    /// <summary>
    /// Whether <see cref="JobHistoryEvidenceSql"/> produced a reading (#3574). Three states, not a nullable
    /// count, for the reason <see cref="JobExecutionLoggingStatus"/> has four: a count this read did not
    /// obtain, a count there was nothing to obtain, and a count of zero are three different facts about an
    /// empty <c>job_history</c>, and the whole defect class is one of them being read as another.
    /// </summary>
    public enum JobHistoryEvidenceStatus
    {
        /// <summary>The read did not complete. Every evidence field is null and the flag stays a GUC echo
        /// — reported as such, never as "zero rows".</summary>
        Unreadable,

        /// <summary>Not attempted, because the GUC read said the view does not exist on this connection
        /// (<see cref="JobExecutionLoggingStatus.NotRegistered"/>). A plain-PostgreSQL store has no
        /// <c>job_history</c> to count, and a failed read of an absent view would report as Unreadable —
        /// a fault-shaped word for a store that has no fault.</summary>
        NotApplicable,

        /// <summary>The read completed. The counts are what this connection saw, and
        /// <see cref="JobHistoryEvidence.Visibility"/> says whether what it saw is what is there.</summary>
        Observed,
    }

    /// <summary>
    /// How much of <c>job_history</c> the view's ownership predicate lets THIS reader see (#3574) — derived
    /// from the two <c>pg_has_role</c> facts the read evaluates, never assumed.
    /// </summary>
    public enum JobHistoryVisibility
    {
        /// <summary>Not established: the evidence read did not complete, or there are no jobs to be a
        /// member of the owner of.</summary>
        Unknown,

        /// <summary>The reader is a member of neither the database owner nor any job's owner. The view
        /// returns it NOTHING by construction, so a zero count here is the filter, not the table. This is
        /// the managed-mode <c>mcp</c> role's reading on every store.</summary>
        None,

        /// <summary>The reader is a member of some jobs' owners but not all, and not of the database owner.
        /// The count is a census of those jobs only.</summary>
        Partial,

        /// <summary>The reader is a member of the database owner, or of every job's owner. The count is a
        /// census — the one state in which a zero says something about recording.</summary>
        All,
    }

    /// <summary>
    /// One reading of <see cref="JobHistoryEvidenceSql"/>: who read, what the view's predicate lets them
    /// see, what they saw, and whether anything happened for them to see (#3574). One value, so a caller
    /// cannot take the count and drop the role it was counted through — which is precisely the omission
    /// this exists to close.
    /// </summary>
    /// <param name="Status">Whether the read completed; every other field is null unless
    /// <see cref="JobHistoryEvidenceStatus.Observed"/>.</param>
    /// <param name="ReaderRole"><c>current_user</c> on the connection that counted.</param>
    /// <param name="ReaderIsDatabaseOwnerMember">The view's first <c>pg_has_role</c> test, evaluated for
    /// this reader: membership in the database owner's role, which sees every row regardless of job owner.</param>
    /// <param name="JobCount">Every job in <c>timescaledb_information.jobs</c> — the UNFILTERED view, so
    /// this is what any role sees and the denominator the visibility fraction is stated against.</param>
    /// <param name="OwnerMemberJobCount">The view's second test, evaluated per job: how many jobs' owner
    /// roles this reader is a member of.</param>
    /// <param name="RowsObserved">History rows with a start inside the window that the view showed this
    /// reader. A census only when <see cref="Visibility"/> is <see cref="JobHistoryVisibility.All"/>.</param>
    /// <param name="NewestRowAt">The newest history start the view showed this reader, over all time —
    /// null when it showed none. UTC.</param>
    /// <param name="JobsRunInWindow">Jobs whose <c>job_stats.last_run_started_at</c> falls inside the
    /// window — the population half, from the unfiltered view, so it holds whatever the reader's
    /// visibility.</param>
    /// <param name="NewestRunStartedAt">The newest job start <c>job_stats</c> knows of, over all time —
    /// null when no job has ever run. UTC.</param>
    public sealed record JobHistoryEvidence(
        JobHistoryEvidenceStatus Status,
        string? ReaderRole,
        bool? ReaderIsDatabaseOwnerMember,
        long? JobCount,
        long? OwnerMemberJobCount,
        long? RowsObserved,
        DateTime? NewestRowAt,
        long? JobsRunInWindow,
        DateTime? NewestRunStartedAt)
    {
        /// <summary>The reading for a connection on which the view does not exist — every field null,
        /// status <see cref="JobHistoryEvidenceStatus.NotApplicable"/>.</summary>
        public static JobHistoryEvidence NotApplicable { get; } =
            new(JobHistoryEvidenceStatus.NotApplicable, null, null, null, null, null, null, null, null);

        /// <summary>The reading for a read that did not complete — every field null, status
        /// <see cref="JobHistoryEvidenceStatus.Unreadable"/>.</summary>
        public static JobHistoryEvidence Unreadable { get; } =
            new(JobHistoryEvidenceStatus.Unreadable, null, null, null, null, null, null, null, null);

        /// <summary>
        /// How many jobs' history the view lets this reader see: every job when the reader is a member of
        /// the database owner (the view's first test short-circuits the second), otherwise the per-job
        /// membership count. Null unless observed.
        /// </summary>
        public long? HistoryVisibleJobCount =>
            Status != JobHistoryEvidenceStatus.Observed ? null
            : ReaderIsDatabaseOwnerMember == true ? JobCount
            : OwnerMemberJobCount;

        /// <summary>
        /// The reader's standing under the view's predicate, derived from the two membership facts and the
        /// job count. <see cref="JobHistoryVisibility.Unknown"/> when the read did not complete or there are
        /// no jobs — with nothing to be an owner of, "none" and "all" would both be vacuously true, and a
        /// note built on either would be inventing a measurement.
        /// </summary>
        public JobHistoryVisibility Visibility
        {
            get
            {
                if (Status != JobHistoryEvidenceStatus.Observed || JobCount is not > 0)
                {
                    return JobHistoryVisibility.Unknown;
                }

                if (ReaderIsDatabaseOwnerMember == true)
                {
                    return JobHistoryVisibility.All;
                }

                return OwnerMemberJobCount switch
                {
                    null or 0 => JobHistoryVisibility.None,
                    var n when n >= JobCount => JobHistoryVisibility.All,
                    _ => JobHistoryVisibility.Partial,
                };
            }
        }

        /// <summary>
        /// The new finding class (#3574): the GUC says recording, the view admits this reader to every job's
        /// history, jobs started runs inside the window, and the reader saw NO rows for them. True only when
        /// all four hold — a zero read through a filtered role contradicts nothing, a zero with no runs in
        /// the window proves nothing, and a zero with the GUC off is the #3175 arm, not this one. The caller
        /// supplies <paramref name="recording"/> because this record deliberately does not carry the GUC
        /// reading; the two are read separately and fail separately.
        /// </summary>
        public bool ContradictsRecording(bool recording) =>
            recording
            && Status == JobHistoryEvidenceStatus.Observed
            && Visibility == JobHistoryVisibility.All
            && RowsObserved == 0
            && JobsRunInWindow is > 0;
    }

    /// <summary>
    /// Reads <see cref="JobHistoryEvidenceSql"/> over the window ending now and starting
    /// <see cref="JobHistoryEvidenceWindowHours"/> ago. Failure-isolated to
    /// <see cref="JobHistoryEvidence.Unreadable"/>, independently of the GUC read: the two are separate
    /// statements on separate checkouts, so either can fail while the other answers, and a reading that
    /// collapsed both into one status would report the GUC as unknown because a count timed out. Takes the
    /// GUC reading only to skip the view a plain-PostgreSQL store does not have — the read is not attempted
    /// for <see cref="JobExecutionLoggingStatus.NotRegistered"/>, and the response says NotApplicable rather
    /// than dressing an absent view up as a failed read. No logger, for the reason
    /// <see cref="GetJobExecutionLoggingAsync"/> gives.
    /// </summary>
    public static async Task<JobHistoryEvidence> GetJobHistoryEvidenceAsync(
        NpgsqlDataSource postgres,
        JobExecutionLoggingReading logging,
        CancellationToken cancellationToken = default)
    {
        if (logging is null)
        {
            throw new ArgumentNullException(nameof(logging));
        }

        if (logging.Status == JobExecutionLoggingStatus.NotRegistered)
        {
            return JobHistoryEvidence.NotApplicable;
        }

        try
        {
            await using var command = postgres.CreateCommand(JobHistoryEvidenceSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;

            /* Kind = Utc and an EXPLICIT timestamptz, against timestamptz columns — the inverse of every
               other bind on this surface, and the paragraph on JobHistoryEvidenceSql says why. Stated rather
               than inferred so that a caller's DateTime of another Kind cannot quietly turn this into the
               naive bind that would skew the window by the session zone. */
            var windowStartUtc = DateTime.SpecifyKind(
                DateTime.UtcNow.AddHours(-JobHistoryEvidenceWindowHours), DateTimeKind.Utc);
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz,
                Value = windowStartUtc,
            });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                /* A single-row SELECT of scalar subqueries always yields one row; no row is a shape this
                   read does not understand, and Unreadable is the only honest word for it. */
                return JobHistoryEvidence.Unreadable;
            }

            return new JobHistoryEvidence(
                JobHistoryEvidenceStatus.Observed,
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetBoolean(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Same cancellation discipline as the GUC read: a caller's own stop is not a measurement. */
            return JobHistoryEvidence.Unreadable;
        }
    }

    /// <summary>
    /// Whether the series held an OWNER's reading of <c>job_history</c> for the block to lean on (#3574).
    /// Four states rather than a nullable count, for the reason every other status on this surface has
    /// more than two: "the sweep has never recorded one", "it recorded one but could not see", "it
    /// recorded one too long ago to speak for now" and "here is the count" are four different facts about
    /// the same absent-or-present number.
    /// </summary>
    public enum OwnerJobHistoryEvidenceStatus
    {
        /// <summary>No <c>job_history</c> row in the series. A store whose sweep predates this row, a
        /// plain-PostgreSQL store (the arm is TimescaleDB-gated), or a sweep that has not completed since
        /// the service started on this build.</summary>
        Absent,

        /// <summary>A row exists, but the sweep's role was itself not admitted to every job's history, so
        /// it recorded no count (<c>row_count</c> NULL). The role is named so the reader knows which
        /// connection to fix; the population half is still carried.</summary>
        Filtered,

        /// <summary>A row exists and carries a census, but its <c>metric_time</c> is older than
        /// <see cref="OwnerEvidenceFreshHours"/>. Shown with its age; NOT used for a verdict about now.</summary>
        Stale,

        /// <summary>A fresh census from an admitted role. The one state in which the owner's zero says
        /// something about recording.</summary>
        Observed,
    }

    /// <summary>
    /// How old the owner's <c>job_history</c> row may be and still speak for the present, in hours. The
    /// sweep is hourly; over 30 days on one production store the whole-store row's gaps had a mean of
    /// 58.6 minutes and a largest-that-happened of 2.47 hours (the <see cref="StoreSelfMetrics.LatestStoreSizeSql"/>
    /// paragraph, with its caveat that the maximum is a window artefact). Three hours is the first whole
    /// hour past that observed worst case: a row older than this means at least two consecutive sweeps
    /// did not land, which the sweep's own Warning line already reports, and a 24-hour count taken that
    /// long ago describes a window that no longer overlaps the present enough to adjudicate a
    /// contradiction about it — the GUC may have been healed since. The row is still shown when stale,
    /// with its age; only the verdict is withheld.
    /// </summary>
    public const int OwnerEvidenceFreshHours = 3;

    /// <summary>
    /// The OWNER's reading of <c>job_history</c>, decoded from the <c>object_kind = 'job_history'</c> row
    /// the hourly sweep persists (#3574) — the managed-mode self-proof. The MCP host reads as the <c>mcp</c>
    /// role, which the view's ownership filter shows nothing; the sweep runs as the owner, which it admits.
    /// This record carries what the owner saw, WHEN it saw it, and whether that is recent enough to stand
    /// beside this connection's own verdict. One value, so the count cannot travel without its instant or
    /// its role — the omission #3574 is about, one level down.
    /// </summary>
    /// <param name="Status">Whether there is a usable reading; see <see cref="OwnerJobHistoryEvidenceStatus"/>.</param>
    /// <param name="ReaderRole">The role the sweep counted as (<c>object_name</c>). Null when Absent.</param>
    /// <param name="ObservedAt">The sweep's <c>metric_time</c>, UTC — the instant the count is true of. Null when Absent.</param>
    /// <param name="AgeHours">How long before <c>nowUtc</c> the sweep ran. Null when Absent.</param>
    /// <param name="WindowHours">The evidence window the row counted over, from <c>schedule_interval_ms</c>.</param>
    /// <param name="RowsObserved">History rows with a start inside the window, as the owner saw them
    /// (<c>row_count</c>). Null when Absent or Filtered — never a plausible zero for a count nobody made.</param>
    /// <param name="NewestRowAt">The newest history start the owner had ever seen at the sweep, UTC —
    /// <c>metric_time</c> minus the persisted age. Null when the owner had seen none.</param>
    /// <param name="JobsRunInWindow">Jobs whose newest start fell inside the window (<c>total_runs</c>),
    /// from the unfiltered <c>job_stats</c> — the population half.</param>
    public sealed record OwnerJobHistoryEvidence(
        OwnerJobHistoryEvidenceStatus Status,
        string? ReaderRole,
        DateTime? ObservedAt,
        double? AgeHours,
        double? WindowHours,
        long? RowsObserved,
        DateTime? NewestRowAt,
        long? JobsRunInWindow)
    {
        /// <summary>The reading when the series holds no <c>job_history</c> row — every field null.</summary>
        public static OwnerJobHistoryEvidence Absent { get; } =
            new(OwnerJobHistoryEvidenceStatus.Absent, null, null, null, null, null, null, null);

        /// <summary>
        /// The contradiction (#3574), judged from the OWNER's numbers: the GUC says recording NOW, the owner
        /// — a reader the view admits to every job's history — saw NO rows over its window, and jobs started
        /// runs inside that window. Only for a fresh, admitted reading: a stale row cannot say what the
        /// instrument is doing now (the GUC may have been healed since it was taken), and a filtered row
        /// made no count. The benign cause and how to settle it are the same as for the connection's own
        /// contradiction and the note names them.
        /// </summary>
        public bool ContradictsRecording(bool recording) =>
            recording
            && Status == OwnerJobHistoryEvidenceStatus.Observed
            && RowsObserved == 0
            && JobsRunInWindow is > 0;

        /// <summary>
        /// Decodes the newest <c>job_history</c> row out of the latest-per-object read. Pure, so the
        /// mapping the sweep's column overloads define (<see cref="StoreSelfMetrics"/> class summary) is
        /// unit-tested without a store: <c>row_count</c> is the count, <c>total_runs</c> the population,
        /// <c>schedule_interval_ms</c> the window, and <c>last_run_duration_ms</c> the newest row's AGE at
        /// the sweep, turned back into an instant here so nothing downstream ever sees the overload.
        /// <paramref name="nowUtc"/> is taken rather than read so the staleness verdict is testable.
        /// </summary>
        public static OwnerJobHistoryEvidence FromLatest(IReadOnlyList<StoreMetricRow> latest, DateTime nowUtc)
        {
            if (latest is null)
            {
                throw new ArgumentNullException(nameof(latest));
            }

            /* The latest read is DISTINCT ON (kind, name), so a store whose owner role was renamed could hold
               two job_history rows under two names; the newest sweep's is the one that speaks for now. */
            StoreMetricRow? row = null;
            foreach (var candidate in latest)
            {
                if (candidate.ObjectKind == StoreSelfMetrics.JobHistoryObjectKind
                    && (row is null || candidate.MetricTime > row.MetricTime))
                {
                    row = candidate;
                }
            }

            if (row is null)
            {
                return Absent;
            }

            /* metric_time is naive UTC by the store contract; it is the instant the count is true of. */
            var observedAt = DateTime.SpecifyKind(row.MetricTime, DateTimeKind.Utc);
            var ageHours = (DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc) - observedAt).TotalHours;
            double? windowHours = row.ScheduleIntervalMs is { } w ? w / 3_600_000.0 : null;
            DateTime? newestRowAt = row.LastRunDurationMs is { } age ? observedAt.AddMilliseconds(-age) : null;

            var status = row.RowCount is null ? OwnerJobHistoryEvidenceStatus.Filtered
                : ageHours > OwnerEvidenceFreshHours ? OwnerJobHistoryEvidenceStatus.Stale
                : OwnerJobHistoryEvidenceStatus.Observed;

            return new OwnerJobHistoryEvidence(
                status,
                row.ObjectName,
                observedAt,
                Math.Round(ageHours, 2),
                windowHours,
                row.RowCount,
                newestRowAt,
                row.TotalRuns);
        }
    }

    /* ---------------- #3582: coverage, reconciled ---------------- */

    /// <summary>
    /// How far the inventory's rows may miss <c>pg_database_size</c> and still be called reconciled, as a
    /// fraction of the database: 1%. Below this the residual is the database directory's non-relation
    /// files plus whatever moved between the sweep's statements; above it something is not being
    /// attributed to any row and the note says so as a finding. Paired with
    /// <see cref="ReconciliationToleranceFloorBytes"/> so a small store is not failed on a fixed overhead.
    /// </summary>
    public const double ReconciliationTolerancePercent = 1.0;

    /// <summary>
    /// The absolute floor under <see cref="ReconciliationTolerancePercent"/>: 64 MiB. Measured on a fresh
    /// PG18/TimescaleDB 2.28.1 rig the residual was 161,471 bytes on a 17 MiB database — exactly
    /// <c>pg_database_size</c> minus the sum of every local relation, i.e. <c>pg_internal.init</c>,
    /// <c>pg_filenode.map</c>, <c>PG_VERSION</c> and friends — which is already 0.9% of that store and
    /// would trip a percent-only bar on anything smaller. A fixed floor sized generously above that
    /// overhead lets the percentage do the work where the percentage means something.
    /// </summary>
    public const long ReconciliationToleranceFloorBytes = 64L * 1024 * 1024;

    /// <summary>
    /// One sweep's inventory, reconciled against its own <c>pg_database_size</c> (#3582). Computed over the
    /// rows that share the store row's <c>metric_time</c> — the same sweep — because the latest read takes
    /// each object's newest row and a sweep that died half-way leaves older rows behind for the kinds it
    /// never reached; summing those against a newer database figure would manufacture a gap.
    /// </summary>
    /// <param name="SweepAt">The store row's <c>metric_time</c> — the sweep every figure here comes from.</param>
    /// <param name="DatabaseBytes"><c>pg_database_size</c> as that sweep recorded it.</param>
    /// <param name="BytesByKind">Byte totals per byte-bearing kind in that sweep, in the sweep's kind
    /// order. Kinds with no row in the sweep are absent from the map, never zero.</param>
    /// <param name="EnumeratedBytes">Bytes under NAMED objects: hypertables, continuous aggregates, payload
    /// dimensions and named tables. The issue's "inventory covers N%" numerator.</param>
    /// <param name="AttributedBytes">Every byte the sweep put in some row: the enumerated bytes plus the
    /// two catch-all rows. The reconciliation numerator.</param>
    /// <param name="UnenumeratedBytes">The <c>other</c> row's bytes; null when that row is missing from the sweep.</param>
    /// <param name="UnenumeratedRelationCount">The <c>other</c> row's relation count; null likewise.</param>
    /// <param name="SystemBytes">The <c>system</c> row's bytes; null when missing.</param>
    /// <param name="SystemRelationCount">The <c>system</c> row's relation count; null likewise.</param>
    /// <param name="ResidualBytes"><c>DatabaseBytes - AttributedBytes</c>. Expected small and non-zero (the
    /// directory's non-relation files, plus movement between statements); can be negative.</param>
    /// <param name="StaleRowCount">Latest rows whose <c>metric_time</c> is NOT the store row's — objects the
    /// newest sweep did not reach. Non-zero means the sweep is not completing and the note says so.</param>
    public sealed record InventoryReconciliation(
        DateTime SweepAt,
        long DatabaseBytes,
        IReadOnlyDictionary<string, long> BytesByKind,
        long EnumeratedBytes,
        long AttributedBytes,
        long? UnenumeratedBytes,
        int? UnenumeratedRelationCount,
        long? SystemBytes,
        int? SystemRelationCount,
        long ResidualBytes,
        int StaleRowCount)
    {
        /// <summary>Percent of the database under named objects — the coverage statement. Null on a zero-byte
        /// database, never a division by zero dressed as a hundred.</summary>
        public double? EnumeratedPercent => DatabaseBytes > 0 ? Math.Round(100.0 * EnumeratedBytes / DatabaseBytes, 2) : null;

        /// <summary>Percent of the database some row attributes — the reconciliation statement.</summary>
        public double? AttributedPercent => DatabaseBytes > 0 ? Math.Round(100.0 * AttributedBytes / DatabaseBytes, 2) : null;

        /// <summary>Both catch-all rows present in the sweep, so the attribution is complete enough to judge.
        /// Without them the residual is the un-enumerated bytes themselves and says nothing.</summary>
        public bool CatchAllPresent => UnenumeratedBytes is not null && SystemBytes is not null;

        /// <summary>The bar the residual is judged against: the larger of the percent and the floor.</summary>
        public long ToleranceBytes => Math.Max(
            (long)Math.Ceiling(DatabaseBytes * ReconciliationTolerancePercent / 100.0),
            ReconciliationToleranceFloorBytes);

        /// <summary><c>true</c> when the catch-all rows are present and the residual is inside the bar. A
        /// <c>false</c> is a finding: bytes the database holds that no row of the inventory accounts for.</summary>
        public bool Reconciled => CatchAllPresent && Math.Abs(ResidualBytes) <= ToleranceBytes;
    }

    /// <summary>
    /// The kinds whose <c>total_bytes</c> are bytes under a NAMED object. The catch-all kinds are not here
    /// by definition; the store row is the denominator; the job kinds carry no bytes.
    /// </summary>
    private static readonly string[] EnumeratedKinds =
    {
        StoreSelfMetrics.HypertableObjectKind,
        StoreSelfMetrics.ContinuousAggregateObjectKind,
        StoreSelfMetrics.DimensionObjectKind,
        StoreSelfMetrics.TableObjectKind,
    };

    /// <summary>
    /// Reconciles the newest sweep's inventory against its own database figure (#3582). Pure. Null when
    /// there is no store row to reconcile against — the tool then says coverage is unknown rather than
    /// computing a percentage of nothing. Rows from other sweeps are counted, not summed.
    /// </summary>
    public static InventoryReconciliation? ComputeInventory(IReadOnlyList<StoreMetricRow> latest)
    {
        if (latest is null)
        {
            throw new ArgumentNullException(nameof(latest));
        }

        StoreMetricRow? store = null;
        foreach (var row in latest)
        {
            if (row.ObjectKind == StoreSelfMetrics.StoreObjectKind && row.TotalBytes is not null
                && (store is null || row.MetricTime > store.MetricTime))
            {
                store = row;
            }
        }

        if (store is null)
        {
            return null;
        }

        var byKind = new Dictionary<string, long>(StringComparer.Ordinal);
        long? other = null, system = null;
        int? otherCount = null, systemCount = null;
        var stale = 0;

        foreach (var row in latest)
        {
            if (row.ObjectKind == StoreSelfMetrics.StoreObjectKind)
            {
                continue;
            }

            if (row.MetricTime != store.MetricTime)
            {
                stale++;
                continue;
            }

            if (row.TotalBytes is not { } bytes)
            {
                continue;
            }

            byKind[row.ObjectKind] = byKind.TryGetValue(row.ObjectKind, out var soFar) ? soFar + bytes : bytes;

            if (row.ObjectKind == StoreSelfMetrics.OtherObjectKind)
            {
                other = bytes;
                otherCount = row.ChunkCount;
            }
            else if (row.ObjectKind == StoreSelfMetrics.SystemObjectKind)
            {
                system = bytes;
                systemCount = row.ChunkCount;
            }
        }

        long enumerated = 0;
        foreach (var kind in EnumeratedKinds)
        {
            if (byKind.TryGetValue(kind, out var bytes))
            {
                enumerated += bytes;
            }
        }

        var attributed = enumerated + (other ?? 0) + (system ?? 0);

        return new InventoryReconciliation(
            store.MetricTime,
            store.TotalBytes!.Value,
            byKind,
            enumerated,
            attributed,
            other,
            otherCount,
            system,
            systemCount,
            store.TotalBytes.Value - attributed,
            stale);
    }

    /// <summary>
    /// The catalog facts about each continuous aggregate the series does not carry (#3582), read LIVE at
    /// tool time the way #2813 reads retention holds: whether compression is enabled on its
    /// materialization, and which of the three policies exist for it, by job id. These are the fields the
    /// sibling investigation (#3581) assembled by hand — twenty aggregates, all with compression disabled,
    /// holding 57% of a production store — and they are STATE rather than series, which is why they are
    /// not persisted (the aggregate row's paragraph on <see cref="StoreSelfMetrics.ContinuousAggregateInsertSql"/>).
    ///
    /// <para><c>timescaledb_information.jobs</c> reports a policy on an aggregate under the aggregate's
    /// USER-FACING view name (<c>COALESCE(ca.user_view_schema, ht.schema_name)</c> in the view's own
    /// definition), which is what the three correlated lookups join on. The compression predicate carries
    /// the 2.18+ <c>columnstore</c> rebrand the rest of the codebase hedges on. <c>hypertable_name</c> in
    /// the aggregates view is the aggregate's SOURCE; for a hierarchical aggregate that is another
    /// aggregate's materialization under its internal name, so the source is resolved back to that
    /// parent's view name where one exists. Readable by the least-privilege <c>mcp</c> role: the
    /// information views are granted to PUBLIC (measured on 2.28.1 with a bare LOGIN role). No parameters.</para>
    /// </summary>
    public const string ContinuousAggregateStateSql = @"
SELECT
    ca.view_name,
    ca.compression_enabled,
    ca.materialized_only,
    coalesce(parent.view_name, ca.hypertable_name) AS source_name,
    (SELECT min(j.job_id) FROM timescaledb_information.jobs j
      WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
      AND   j.hypertable_schema = ca.view_schema AND j.hypertable_name = ca.view_name) AS refresh_job_id,
    (SELECT min(j.job_id) FROM timescaledb_information.jobs j
      WHERE (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')
      AND   j.hypertable_schema = ca.view_schema AND j.hypertable_name = ca.view_name) AS compression_job_id,
    (SELECT min(j.job_id) FROM timescaledb_information.jobs j
      WHERE j.proc_name = 'policy_retention'
      AND   j.hypertable_schema = ca.view_schema AND j.hypertable_name = ca.view_name) AS retention_job_id
FROM timescaledb_information.continuous_aggregates ca
LEFT JOIN timescaledb_information.continuous_aggregates parent
       ON parent.materialization_hypertable_schema = ca.hypertable_schema
      AND parent.materialization_hypertable_name = ca.hypertable_name";

    /// <summary>One aggregate's live catalog state. Job ids null where no such policy exists.</summary>
    public sealed record ContinuousAggregateState(
        string ViewName,
        bool CompressionEnabled,
        bool MaterializedOnly,
        string? SourceName,
        int? RefreshJobId,
        int? CompressionJobId,
        int? RetentionJobId);

    /// <summary>
    /// Reads <see cref="ContinuousAggregateStateSql"/>. Failure-isolated to NULL — not an empty list, which
    /// would read as "no aggregates" and drop the flags from every aggregate row without a word. Not
    /// attempted where the GUC probe said TimescaleDB is not loaded for this database
    /// (<see cref="JobExecutionLoggingStatus.NotRegistered"/>): the view does not exist there and there are
    /// no aggregate rows to decorate. No logger, for the reason <see cref="GetJobExecutionLoggingAsync"/> gives.
    /// </summary>
    public static async Task<IReadOnlyList<ContinuousAggregateState>?> GetContinuousAggregateStatesAsync(
        NpgsqlDataSource postgres,
        JobExecutionLoggingReading logging,
        CancellationToken cancellationToken = default)
    {
        if (logging is null)
        {
            throw new ArgumentNullException(nameof(logging));
        }

        if (logging.Status == JobExecutionLoggingStatus.NotRegistered)
        {
            return Array.Empty<ContinuousAggregateState>();
        }

        try
        {
            var states = new List<ContinuousAggregateState>();
            await using var command = postgres.CreateCommand(ContinuousAggregateStateSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                states.Add(new ContinuousAggregateState(
                    reader.GetString(0),
                    reader.GetBoolean(1),
                    reader.GetBoolean(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    reader.IsDBNull(6) ? null : reader.GetInt32(6)));
            }

            return states;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>How many of the un-enumerated relations the tool names, largest first.</summary>
    public const int LargestUnenumeratedLimit = 10;

    /// <summary>
    /// The biggest relations inside the <c>other</c> row, by name (#3582) — read LIVE at tool time over
    /// the SAME census predicate the sweep sums with (the fragments on <see cref="StoreSelfMetrics"/>), so
    /// the list and the number it explains cannot disagree about what counts. Without this the <c>other</c>
    /// row is a figure nobody can act on; with it, a growth investigation reads the name of the table that
    /// grew instead of running the <c>pg_class</c> census by hand — which is the expedition this whole
    /// tool exists to replace. Named <c>schema.relation</c>, the form the <c>table</c> rows use, so a
    /// relation the product later names by that kind keeps its name. TimescaleDB variant; readable by the
    /// <c>mcp</c> role (<c>pg_total_relation_size</c> needs no privilege on the relation, and the
    /// TimescaleDB catalogs are granted to PUBLIC — both measured). $1 the limit.
    /// </summary>
    public const string LargestUnenumeratedSql = $@"
SELECT
    n.nspname || '.' || c.relname AS relation,
    c.relkind::text,
    pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE {StoreSelfMetrics.CensusRelationPredicateSql}
AND   NOT {StoreSelfMetrics.SystemSchemaPredicateSql}
AND   NOT {StoreSelfMetrics.NamedRelationPredicateSql}
AND   {StoreSelfMetrics.TimescaleInventoriedPredicateSql}
ORDER BY pg_total_relation_size(c.oid) DESC, 1
LIMIT $1";

    /// <summary>The plain-PostgreSQL variant of <see cref="LargestUnenumeratedSql"/>: the same census minus
    /// the TimescaleDB catalogs it cannot name. On such a store the collector tables lead this list, which
    /// is the honest answer (see <see cref="StoreSelfMetrics.UnenumeratedPlainInsertSql"/>). $1 the limit.</summary>
    public const string LargestUnenumeratedPlainSql = $@"
SELECT
    n.nspname || '.' || c.relname AS relation,
    c.relkind::text,
    pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE {StoreSelfMetrics.CensusRelationPredicateSql}
AND   NOT {StoreSelfMetrics.SystemSchemaPredicateSql}
AND   NOT {StoreSelfMetrics.NamedRelationPredicateSql}
ORDER BY pg_total_relation_size(c.oid) DESC, 1
LIMIT $1";

    /// <summary>One un-enumerated relation, sized live.</summary>
    public sealed record UnenumeratedRelation(string Relation, string RelKind, long TotalBytes);

    /// <summary>
    /// Reads the live top-N of un-enumerated relations, choosing the variant by the same signal the other
    /// TimescaleDB-only reads use (<see cref="JobExecutionLoggingStatus.NotRegistered"/> means the
    /// TimescaleDB catalogs do not exist for this database). Failure-isolated to NULL, not an empty list,
    /// for the reason <see cref="GetContinuousAggregateStatesAsync"/> gives.
    /// </summary>
    public static async Task<IReadOnlyList<UnenumeratedRelation>?> GetLargestUnenumeratedAsync(
        NpgsqlDataSource postgres,
        JobExecutionLoggingReading logging,
        CancellationToken cancellationToken = default)
    {
        if (logging is null)
        {
            throw new ArgumentNullException(nameof(logging));
        }

        try
        {
            var rows = new List<UnenumeratedRelation>();
            await using var command = postgres.CreateCommand(
                logging.Status == JobExecutionLoggingStatus.NotRegistered ? LargestUnenumeratedPlainSql : LargestUnenumeratedSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(LargestUnenumeratedLimit);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new UnenumeratedRelation(reader.GetString(0), reader.GetString(1), reader.GetInt64(2)));
            }

            return rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>One object's newest self-metrics row. The four job fields (#2136, V56) are non-null only
    /// on <c>background_job</c> rows and, since #3574, on the <c>job_history</c> row under the column
    /// mapping the <see cref="StoreSelfMetrics"/> class summary states — every other kind leaves them NULL,
    /// as the sweep writes them. The two TOAST fields (V137, #3783) are non-null on <c>dimension</c> rows
    /// only, and <c>ToastLiveBytes</c> is NULL even there unless the store carries <c>pg_freespacemap</c>
    /// (<see cref="ToastFacts"/> says so in words).</summary>
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
        long? TotalFailures = null,
        long? ToastBytes = null,
        long? ToastLiveBytes = null);

    /// <summary>One object's settled point for one day (the day's last sample). Job and TOAST fields as on
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
        long? TotalFailures = null,
        long? ToastBytes = null,
        long? ToastLiveBytes = null);

    /* ---------------- #3783: TOAST utilisation on the dimension rows ---------------- */

    /// <summary>
    /// What a dimension row's TOAST pair means (#3783), computed once for the tool's object rows and series
    /// points and for the self-alert, so the percentage an operator reads and the one the alert judged
    /// are the same arithmetic. <c>null</c> for every kind but <c>dimension</c>: the hypertable, aggregate,
    /// table and catch-all rows carry no TOAST columns (the sweep leaves them NULL by the per-kind convention),
    /// and publishing a note on them would be prose about a measurement nobody took.
    ///
    /// <para><b>The quotient.</b> <c>toast_live_bytes / toast_bytes × 100</c>, one decimal — a ratio of two
    /// stored byte counts from the same sweep, not a rate, so no interval is involved and no per-second key
    /// is published. NULL when either byte count is NULL (unmeasured is unmeasured) and when the file is
    /// empty (a percentage of zero bytes is not a number). Never computed from <c>total_bytes</c> or from a
    /// tuple share: the rung's measurement showed the tuple-share proxy reading 100 % on exactly the shape
    /// this exists to catch.</para>
    ///
    /// <para><b>The note says one of four things</b>, in words, so a reader never has to infer why a field is
    /// null: the row predates the column; the file is real but live bytes are not measured and what would
    /// measure them; the file is under-utilised past the bars the self-alert judges, with the reclaim path
    /// and its cost; or the file's utilisation, plainly. The bars are the evaluator's constants
    /// (<see cref="DarlingSelfAlertEvaluator.ToastSlackUtilisationBarPercent"/>,
    /// <see cref="DarlingSelfAlertEvaluator.ToastSlackFileFloorBytes"/>), read from one place so the sentence
    /// and the alert cannot name different lines.</para>
    /// </summary>
    /// <param name="ToastBytes">The TOAST relation's main-fork size as the sweep recorded it.</param>
    /// <param name="ToastLiveBytes">Bytes of that file holding live data, from the free-space map; NULL where
    /// the store has no <c>pg_freespacemap</c>.</param>
    /// <param name="UtilisationPercent">The quotient above, or NULL.</param>
    /// <param name="Note">The sentence.</param>
    public sealed record ToastFacts(
        long? ToastBytes,
        long? ToastLiveBytes,
        double? UtilisationPercent,
        string Note)
    {
        /// <summary>The facts for one latest row, or null for a kind that carries none.</summary>
        public static ToastFacts? For(StoreMetricRow row)
        {
            if (row is null)
            {
                throw new ArgumentNullException(nameof(row));
            }

            return row.ObjectKind == StoreSelfMetrics.DimensionObjectKind
                ? Compute(row.ObjectName, row.ToastBytes, row.ToastLiveBytes)
                : null;
        }

        /// <summary>The facts for one daily point, on the same terms.</summary>
        public static ToastFacts? For(StoreMetricDailyPoint point)
        {
            if (point is null)
            {
                throw new ArgumentNullException(nameof(point));
            }

            return point.ObjectKind == StoreSelfMetrics.DimensionObjectKind
                ? Compute(point.ObjectName, point.ToastBytes, point.ToastLiveBytes)
                : null;
        }

        /// <summary>The quotient, exactly as stated on the record: NULL when either side is NULL or the file
        /// is empty; otherwise live over file, one decimal.</summary>
        public static double? UtilisationPercentOf(long? toastBytes, long? toastLiveBytes)
        {
            if (toastBytes is not long file || toastLiveBytes is not long live || file <= 0)
            {
                return null;
            }

            return Math.Round(100.0 * live / file, 1);
        }

        /// <summary>
        /// Whether the row is the finding the self-alert fires on (#3783): a measured utilisation under the
        /// bar on a file over the floor. False — never "unknown" — when the utilisation is NULL: an
        /// unmeasured file is not a finding, and that is what keeps the alert arm DORMANT on a store without
        /// the extension.
        /// </summary>
        public static bool IsSlack(long? toastBytes, double? utilisationPercent) =>
            toastBytes is long file
            && utilisationPercent is double pct
            && file > DarlingSelfAlertEvaluator.ToastSlackFileFloorBytes
            && pct < DarlingSelfAlertEvaluator.ToastSlackUtilisationBarPercent;

        internal static ToastFacts Compute(string dimension, long? toastBytes, long? toastLiveBytes)
        {
            var pct = UtilisationPercentOf(toastBytes, toastLiveBytes);
            return new ToastFacts(toastBytes, toastLiveBytes, pct, NoteFor(dimension, toastBytes, toastLiveBytes, pct));
        }

        /// <summary>The sentence, pure so the words a reader gets are assertable.</summary>
        internal static string NoteFor(string dimension, long? toastBytes, long? toastLiveBytes, double? pct)
        {
            if (toastBytes is not long file)
            {
                return "toast_bytes is not recorded on this row: the sweep that wrote it predates V137, or the table has no "
                    + "TOAST relation. The next hourly sweep on a V137+ store records the file size.";
            }

            if (file == 0)
            {
                return "The TOAST file is empty, so there is no utilisation to state.";
            }

            if (toastLiveBytes is null || pct is not double utilisation)
            {
                return $"toast_utilisation_pct is not measured: the {DarlingMcpStoreMetricsTools.Gib(file)} TOAST file size is real, but the live "
                    + $"bytes inside it need the {StoreSelfMetrics.FreespacemapExtensionName} extension, which the bundled store image "
                    + "ships and does not install — installing it is the maintainer's call (CREATE EXTENSION IF NOT EXISTS "
                    + $"{StoreSelfMetrics.FreespacemapExtensionName} on the store; the next hourly sweep then fills toast_live_bytes and "
                    + "this reads as a percentage, and the slack self-alert arms itself). Nothing here is computed from "
                    + "total_bytes or from a tuple share: after a VACUUM the dead tuples are gone and the file keeps its "
                    + "pages, so a tuple share reads 100 % on exactly the file this column exists to judge.";
            }

            var live = toastLiveBytes.Value;
            var slackBytes = Math.Max(file - live, 0);
            var head = $"{utilisation.ToString("0.0", CultureInfo.InvariantCulture)} % of the {DarlingMcpStoreMetricsTools.Gib(file)} TOAST file behind "
                + $"{dimension} holds live data ({DarlingMcpStoreMetricsTools.Gib(live)} live, {DarlingMcpStoreMetricsTools.Gib(slackBytes)} free inside the file).";

            if (IsSlack(file, utilisation))
            {
                return head + " That is under the "
                    + $"{DarlingSelfAlertEvaluator.ToastSlackUtilisationBarPercent.ToString("0", CultureInfo.InvariantCulture)} % bar on a file over "
                    + $"{DarlingMcpStoreMetricsTools.Gib(DarlingSelfAlertEvaluator.ToastSlackFileFloorBytes)}: the free pages are slack that ordinary VACUUM returns to the table "
                    + "and never to the OS, and the store self-alert says so. --recompress-plan-dim --vacuum-full compacts the "
                    + "file, at the maintainer's word in a maintenance window: it takes an ACCESS EXCLUSIVE lock on the "
                    + "dimension for the rebuild and needs free disk for a full copy of the live data while it runs. This "
                    + "tool never reclaims anything by itself.";
            }

            if (utilisation < DarlingSelfAlertEvaluator.ToastSlackUtilisationBarPercent)
            {
                return head + " Under the "
                    + $"{DarlingSelfAlertEvaluator.ToastSlackUtilisationBarPercent.ToString("0", CultureInfo.InvariantCulture)} % bar but the file is under the "
                    + $"{DarlingMcpStoreMetricsTools.Gib(DarlingSelfAlertEvaluator.ToastSlackFileFloorBytes)} floor the self-alert judges, so the slack is real and small: no finding.";
            }

            return head;
        }
    }

    /* ---------------- #3783: the store's own checkpointer, differenced ---------------- */

    /// <summary>
    /// The two newest <c>checkpointer</c> rows, newest first (#3783) — the pair a difference needs. The
    /// sweep stores the server's CUMULATIVE counters (the reasoning is on
    /// <see cref="StoreSelfMetrics.CheckpointerInsertSql"/>), so one row says nothing about any interval and
    /// this read is the ONLY shape that can: the newest row minus the row before it, over the two stamps'
    /// span. <c>checkpoint_write_ms IS NOT NULL</c> because the columns are nullable for every other kind's
    /// sake and a checkpointer row the sweep wrote always carries all three; the guard costs nothing and
    /// keeps a hypothetically half-written row from becoming the "previous" and NULLing a good delta. The
    /// newest pair is wanted regardless of window; $1 is <see cref="CheckpointerPairRows"/>, bound rather than
    /// written as a literal for the reason <see cref="LargestUnenumeratedSql"/> binds its cap — a terminal
    /// literal <c>LIMIT</c> on a reader is the shape the page census inventories, and this is not a page.
    /// <c>postmaster_start_time</c> (V139, #3955) rides along so <see cref="CheckpointerReading.From"/> can tell an
    /// interval that spans a restart; it is NULL on a row written before the rung, which the rule reads as no
    /// evidence on the newer row and as "compare its time" on the older one.
    /// </summary>
    public const string CheckpointerPairSql = $@"
SELECT
    metric_time,
    checkpoint_write_ms,
    checkpoint_sync_ms,
    checkpoints_requested,
    postmaster_start_time,
    checkpoints_timed
FROM collect.store_metrics
WHERE object_kind = '{StoreSelfMetrics.CheckpointerObjectKind}'
AND   checkpoint_write_ms IS NOT NULL
ORDER BY metric_time DESC
LIMIT $1";

    /// <summary>How many checkpointer rows <see cref="CheckpointerPairSql"/> reads: two, because a difference
    /// needs exactly the newest row and the one before it and nothing a third row could add.</summary>
    public const int CheckpointerPairRows = 2;

    /// <summary>One checkpointer row as stored: the sweep's stamp (naive UTC), the three cumulative counters as
    /// the server reported them at that instant, and (V139, #3955) the <c>pg_postmaster_start_time()</c> of the
    /// postmaster that reported them, naive UTC, null on a row written before the rung.</summary>
    /// <param name="Timed">(V140, #4037) The cumulative COUNT of TIMED checkpoints as the server reported it,
    /// null on a row written before the rung. <see cref="CheckpointerReading.From"/> reads a null on either
    /// sample as no evidence for the average-per-checkpoint arm, never as zero.</param>
    public sealed record CheckpointerSample(DateTime MetricTime, long WriteMs, long SyncMs, long Requested, DateTime? PostmasterStartTime = null, long? Timed = null);

    /// <summary>
    /// Whether the pair yielded an interval (#3783). Five states rather than a nullable delta, for the reason
    /// every other status on this surface has more than two: "the sweep has never written the row", "it has
    /// written one and there is nothing yet to subtract", "the counters went backwards between the two", "a
    /// restart between the two put the shutdown checkpoint's own work into the counters" (#3955) and "here is
    /// the interval" are five different facts about the same absent-or-present number, and the self-alert must
    /// fire on exactly one of them.
    /// </summary>
    public enum CheckpointerDeltaStatus
    {
        /// <summary>No checkpointer row in the series: a store whose sweep predates the row, or a service that
        /// has not completed a sweep since starting on this build.</summary>
        Absent,

        /// <summary>Exactly one row. The counters are real but there is no earlier sample to difference
        /// against; the next hourly sweep makes the first interval.</summary>
        NoPrevious,

        /// <summary>At least one counter reads LOWER on the newest row than on the one before it:
        /// <c>pg_stat_reset_shared('checkpointer')</c> (or <c>'bgwriter'</c> before 17), or a server restart
        /// without statistics persistence, ran between the two sweeps. The interval is unmeasurable and every
        /// delta is NULL with this reason — the #3705 discontinuity idiom, never a negative and never a clamped
        /// zero. The row after the next sweep will difference cleanly against the post-reset row.</summary>
        Reset,

        /// <summary>The store's postmaster restarted between the two sweeps (<see cref="PostmasterRestart.Spans"/>,
        /// #3955) and the counters did not go backwards: a clean stop writes the statistics out, and the shutdown
        /// checkpoint lands in all three of them — one more REQUESTED checkpoint, plus its own write and sync
        /// phases, which nothing can separate from the live checkpoints' work. So no delta is stated, the Reset
        /// shape, and the self-alert judges neither arm: a standing alert neither fires nor recovers. The cost is
        /// one skipped hourly interval after each restart; the next sweep differences cleanly against the
        /// post-restart row and is judged normally.</summary>
        Restarted,

        /// <summary>A measured interval on one postmaster. The one state the self-alert judges.</summary>
        Observed,
    }

    /// <summary>
    /// The checkpointer's last interval, differenced from the two newest rows (#3783). One value carrying the
    /// deltas, the span they cover and the instants they cover it between, so a caller cannot take a
    /// millisecond figure and drop the hour it belongs to. Pure, so the arithmetic the tool publishes and the
    /// self-alert judges is unit-tested without a store.
    /// </summary>
    /// <param name="Status">Whether there is an interval; the delta fields are null unless <see cref="CheckpointerDeltaStatus.Observed"/>.</param>
    /// <param name="ObservedAt">The newest row's stamp, UTC — the END of the interval. Null when Absent.</param>
    /// <param name="PreviousAt">The row before it, UTC — the START of the interval. Null when Absent or NoPrevious.</param>
    /// <param name="IntervalSeconds">The span, MEASURED from the two stamps rather than assumed from the sweep's cadence:
    /// an hour on a healthy store, longer across a skipped tick. Null unless Observed.</param>
    /// <param name="WriteMs">Milliseconds the checkpointer spent in the write phase inside the interval.</param>
    /// <param name="SyncMs">Milliseconds it spent in the sync (fsync) phase inside the interval — the phase the
    /// production read kills sat inside.</param>
    /// <param name="Requested">Checkpoints inside the interval that were REQUESTED (WAL-forced) rather than timed.</param>
    /// <param name="CumulativeWriteMs">The newest row's raw counter, for a reader who wants the lifetime figure. Null when Absent.</param>
    /// <param name="CumulativeSyncMs">Likewise.</param>
    /// <param name="CumulativeRequested">Likewise.</param>
    /// <param name="PostmasterRestarted">The interval spans a postmaster restart by <see cref="PostmasterRestart.Spans"/>
    /// (V139, #3955): true on every <see cref="CheckpointerDeltaStatus.Restarted"/> reading, and on a
    /// <see cref="CheckpointerDeltaStatus.Reset"/> one whose counters fell because the restart was unclean. False
    /// when there is no interval to span (Absent, NoPrevious) and on every Observed one.</param>
    /// <param name="PostmasterStartTime">When the postmaster that produced the newest row started, UTC; null when the
    /// newest row predates V139 or there is no row.</param>
    /// <param name="Timed">(V140, #4037) TIMED checkpoints inside the interval. Null when either sample predates
    /// the rung — <see cref="IsPressure"/> then has no denominator for the average arm and states no pressure
    /// from sync alone, never falling back to the old summed-sync rule.</param>
    /// <param name="CumulativeTimed">The newest row's raw timed-checkpoint counter. Null when Absent or the row predates V140.</param>
    public sealed record CheckpointerReading(
        CheckpointerDeltaStatus Status,
        DateTime? ObservedAt,
        DateTime? PreviousAt,
        double? IntervalSeconds,
        long? WriteMs,
        long? SyncMs,
        long? Requested,
        long? CumulativeWriteMs,
        long? CumulativeSyncMs,
        long? CumulativeRequested,
        bool PostmasterRestarted = false,
        DateTime? PostmasterStartTime = null,
        long? Timed = null,
        long? CumulativeTimed = null)
    {
        /// <summary>The reading when the series holds no checkpointer row — every field null.</summary>
        public static CheckpointerReading Absent { get; } =
            new(CheckpointerDeltaStatus.Absent, null, null, null, null, null, null, null, null, null);

        /// <summary>
        /// Differences the pair. <paramref name="newest"/> null is <see cref="CheckpointerDeltaStatus.Absent"/>;
        /// <paramref name="previous"/> null is <see cref="CheckpointerDeltaStatus.NoPrevious"/>; any counter
        /// lower on the newest row is <see cref="CheckpointerDeltaStatus.Reset"/>; a pair whose stamps do not
        /// advance (two rows at one instant cannot happen from one sweep, but the arithmetic must not divide
        /// by it) is treated as no interval. A pair that spans a postmaster restart
        /// (<see cref="PostmasterRestart.Spans"/>, #3955) is <see cref="CheckpointerDeltaStatus.Restarted"/>, with no
        /// delta: the shutdown checkpoint is in all three counters. Otherwise the three subtractions and the
        /// measured span.
        /// </summary>
        public static CheckpointerReading From(CheckpointerSample? newest, CheckpointerSample? previous)
        {
            if (newest is null)
            {
                return Absent;
            }

            var observedAt = DateTime.SpecifyKind(newest.MetricTime, DateTimeKind.Utc);
            DateTime? startedAt = newest.PostmasterStartTime is DateTime started
                ? DateTime.SpecifyKind(started, DateTimeKind.Utc)
                : null;

            if (previous is null)
            {
                return new CheckpointerReading(
                    CheckpointerDeltaStatus.NoPrevious, observedAt, null, null, null, null, null,
                    newest.WriteMs, newest.SyncMs, newest.Requested,
                    PostmasterRestarted: false, PostmasterStartTime: startedAt,
                    Timed: null, CumulativeTimed: newest.Timed);
            }

            var previousAt = DateTime.SpecifyKind(previous.MetricTime, DateTimeKind.Utc);
            var span = (observedAt - previousAt).TotalSeconds;
            var restarted = PostmasterRestart.Spans(previous.MetricTime, previous.PostmasterStartTime, newest.PostmasterStartTime);

            /* Timed goes backwards only when both samples carry it; a null on either side is "no evidence",
               never treated as a fall from something to nothing (#4037). */
            var timedReset = newest.Timed is long nt && previous.Timed is long pt && nt < pt;

            if (newest.WriteMs < previous.WriteMs || newest.SyncMs < previous.SyncMs || newest.Requested < previous.Requested || timedReset || span <= 0)
            {
                return new CheckpointerReading(
                    CheckpointerDeltaStatus.Reset, observedAt, previousAt, null, null, null, null,
                    newest.WriteMs, newest.SyncMs, newest.Requested,
                    restarted, startedAt,
                    Timed: null, CumulativeTimed: newest.Timed);
            }

            /* #3955: the shutdown checkpoint is one more requested checkpoint and its own write and sync phases,
               in counters that kept running across the restart, and nothing separates it from the live
               checkpoints' work. No delta, never a zero: the next interval is judged normally. */
            if (restarted)
            {
                return new CheckpointerReading(
                    CheckpointerDeltaStatus.Restarted, observedAt, previousAt, null, null, null, null,
                    newest.WriteMs, newest.SyncMs, newest.Requested,
                    PostmasterRestarted: true, PostmasterStartTime: startedAt,
                    Timed: null, CumulativeTimed: newest.Timed);
            }

            /* (V140, #4037) Timed only when BOTH samples carry it; one row from before the rung leaves the
               average-per-checkpoint arm with no denominator for this interval, stated as null rather than
               guessed at from the requested count alone. */
            long? timedDelta = newest.Timed is long newestTimed && previous.Timed is long previousTimed
                ? newestTimed - previousTimed
                : null;

            return new CheckpointerReading(
                CheckpointerDeltaStatus.Observed, observedAt, previousAt, Math.Round(span, 1),
                newest.WriteMs - previous.WriteMs,
                newest.SyncMs - previous.SyncMs,
                newest.Requested - previous.Requested,
                newest.WriteMs, newest.SyncMs, newest.Requested,
                PostmasterRestarted: false, PostmasterStartTime: startedAt,
                Timed: timedDelta, CumulativeTimed: newest.Timed);
        }

        /// <summary>(V140, #4037) Checkpoints inside the interval, timed plus requested — the average arm's
        /// denominator. Null when <see cref="Timed"/> is null (a pre-rung sample on either side of the pair).</summary>
        public long? CheckpointCount => Timed is long timed ? timed + (Requested ?? 0) : null;

        /// <summary>(V140, #4037) <see cref="SyncMs"/> divided by <see cref="CheckpointCount"/> — the figure the
        /// self-alert and the MCP block judge against <see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>,
        /// in place of the interval's summed sync milliseconds. Null when there is no checkpoint count to divide
        /// by, or the interval held zero checkpoints (nothing to average).</summary>
        public double? AverageSyncMsPerCheckpoint =>
            CheckpointCount is long count && count > 0 && SyncMs is long sync ? (double)sync / count : null;

        /// <summary>
        /// The self-alert's condition (#3783, judged per-checkpoint average since #4037), on an Observed
        /// interval only: the AVERAGE sync milliseconds per checkpoint in the interval
        /// (<see cref="AverageSyncMsPerCheckpoint"/> = SyncMs / (timed + requested)) held more than
        /// <see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>, OR at least one checkpoint was WAL-forced.
        /// The old rule judged the interval's SUMMED sync milliseconds against the same bar, which is a
        /// PER-CHECKPOINT bar (the MCP read deadline) — an hourly interval covers about twelve timed checkpoints
        /// on the default five-minute checkpoint_timeout, so a healthy store whose checkpoints synced five to
        /// eight seconds each summed past the bar every interval and never recovered. A pre-V140 row leaves
        /// <see cref="Timed"/> null; the average arm then states no pressure from sync alone rather than falling
        /// back to the old sum, and the requested arm still fires on any WAL-forced checkpoint. Zero checkpoints
        /// in the interval judges neither arm. False on every other status — an unmeasured interval is not a
        /// finding, and that includes one that spans a postmaster restart (#3955).
        /// </summary>
        public bool IsPressure =>
            Status == CheckpointerDeltaStatus.Observed
            && ((Requested is long requested && requested > 0)
                || (AverageSyncMsPerCheckpoint is double average && average > DarlingSelfAlertEvaluator.CheckpointSyncBarMs));
    }

    /// <summary>
    /// Reads <see cref="CheckpointerPairSql"/> and differences it. NOT failure-isolated, unlike the
    /// qualifier reads above: this is a primary reading the tool publishes as a block and the self-alert
    /// judges, and both callers already own the isolation — the tool's outer catch turns a throw into the
    /// shared error envelope, and the evaluator's <c>Evaluate*</c> wrapper logs it and counts it as a swallowed
    /// read (#3013). Swallowing here would hand both a plausible <c>Absent</c> for a store whose read timed out.
    /// </summary>
    public static async Task<CheckpointerReading> GetCheckpointerAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(CheckpointerPairSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(CheckpointerPairRows);

        CheckpointerSample? newest = null, previous = null;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var sample = new CheckpointerSample(
                reader.GetDateTime(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5));
            if (newest is null)
            {
                newest = sample;
            }
            else
            {
                previous = sample;
            }
        }

        return CheckpointerReading.From(newest, previous);
    }

    /// <summary>One day's whole-store growth: the byte delta from the previous day's settled point, and
    /// that delta divided by the day's enabled-server count — the number onboarding N servers multiplies.
    /// <c>PerServerBytes</c> is null when the server count is unknown or zero (a delta over no servers is
    /// not a rate).</summary>
    public sealed record DailyGrowthPoint(DateTime Day, long DeltaBytes, double? PerServerBytes);

    /// <summary>
    /// The index <see cref="StoreMetricsLatestSql"/>'s skip-scan walks (#3934), created by the Tuning stage
    /// (<c>PgTableTuning</c>), not by a migration.
    /// </summary>
    public const string StoreMetricsLatestIndexName = "idx_store_metrics_kind_name_time";

    /// <summary>Whether that index exists: <c>to_regclass</c>, a catalog lookup with no table scan.</summary>
    public const string StoreMetricsLatestIndexProbeSql =
        "SELECT pg_catalog.to_regclass('collect." + StoreMetricsLatestIndexName + "') IS NOT NULL";

    /// <summary>
    /// The pre-#3934 read, kept for a store that does not have <see cref="StoreMetricsLatestIndexName"/> yet:
    /// the Tuning stage has not run since the upgrade, or could not create it. Without the index, every step of
    /// the skip-scan re-reads the table, so its cost grows with objects times rows. CI measured it past the
    /// 30-second MCP read deadline on a production-shaped seed where this form's single sort answers. The
    /// answers are identical either way; only the plan differs.
    /// </summary>
    public const string StoreMetricsLatestWithoutIndexSql = @"
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
    total_failures,
    toast_bytes,
    toast_live_bytes
FROM collect.store_metrics
ORDER BY object_kind, object_name, metric_time DESC";

    public static async Task<List<StoreMetricRow>> GetLatestAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        bool indexed;
        await using (var probe = postgres.CreateCommand(StoreMetricsLatestIndexProbeSql))
        {
            probe.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            indexed = await probe.ExecuteScalarAsync(cancellationToken) is true;
        }

        var rows = new List<StoreMetricRow>();
        await using var command = postgres.CreateCommand(indexed ? StoreMetricsLatestSql : StoreMetricsLatestWithoutIndexSql);
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
                reader.IsDBNull(12) ? null : reader.GetInt64(12),
                reader.IsDBNull(13) ? null : reader.GetInt64(13),
                reader.IsDBNull(14) ? null : reader.GetInt64(14)));
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
                reader.IsDBNull(12) ? null : reader.GetInt64(12),
                reader.IsDBNull(13) ? null : reader.GetInt64(13),
                reader.IsDBNull(14) ? null : reader.GetInt64(14)));
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

    /* ---------------- #3903: the summary-first payload ---------------- */

    /// <summary>
    /// The kinds <c>get_store_metrics</c> lists as rows (#3903), which is also the closed set its
    /// <c>object_kind</c> filter accepts: the six byte-bearing kinds and <c>background_job</c>. The
    /// <c>store</c>, <c>job_history</c> and <c>checkpointer</c> rows are never list rows: each is a block of
    /// its own, because its columns are overloaded or cumulative and, rendered as a row, would read as
    /// something they are not.
    /// </summary>
    public static readonly IReadOnlyList<string> ListedKinds = new[]
    {
        StoreSelfMetrics.HypertableObjectKind,
        StoreSelfMetrics.ContinuousAggregateObjectKind,
        StoreSelfMetrics.DimensionObjectKind,
        StoreSelfMetrics.TableObjectKind,
        StoreSelfMetrics.OtherObjectKind,
        StoreSelfMetrics.SystemObjectKind,
        StoreSelfMetrics.BackgroundJobObjectKind,
    };

    /// <summary>A listed kind whose rows carry bytes: every listed kind but <c>background_job</c>.</summary>
    public static bool IsByteBearing(string objectKind) =>
        objectKind != StoreSelfMetrics.BackgroundJobObjectKind && ListedKinds.Contains(objectKind, StringComparer.Ordinal);

    /// <summary>How much of its own schedule interval a job's last run took, unrounded, or null when either
    /// figure is missing or not positive. The ranking reads it whole; the payload rounds it to one decimal.</summary>
    public static double? CadencePercent(long? lastRunDurationMs, long? scheduleIntervalMs) =>
        lastRunDurationMs is > 0 && scheduleIntervalMs is > 0
            ? 100.0 * lastRunDurationMs.Value / scheduleIntervalMs.Value
            : null;

    /// <summary>
    /// One object's change across the window, from its first daily point to its last (#3903). This is what the
    /// object lists carry in place of the per-object daily series the default response no longer does: a
    /// growth or cadence question reads the difference, and the points stay one call away.
    ///
    /// <para><b>Daily points, not raw samples.</b> Each daily point is that day's LAST snapshot
    /// (<see cref="StoreMetricsDailySql"/>), so the delta is what a caller would compute from the series an
    /// exact <c>object_name</c> returns. <see cref="Since"/> is the first point's day.</para>
    ///
    /// <para><b>Null, never zero, when there is no delta to state</b>: a null reading at either end, or a job
    /// counter that went backwards. The job counters are cumulative, so a negative count of runs is a reset,
    /// not a measurement, the <see cref="CheckpointerReading"/> discipline. An object with fewer than two
    /// points has no entry at all.</para>
    /// </summary>
    public sealed record WindowDelta(DateTime Since, long? GrowthBytes, long? RunsInWindow, long? FailuresInWindow);

    /// <summary>
    /// Every object's <see cref="WindowDelta"/> from the daily series (#3903). Pure. Keyed by (kind, name)
    /// because the name alone is not unique across kinds: the store row and the job_history row can share one.
    /// </summary>
    public static Dictionary<(string Kind, string Name), WindowDelta> ComputeWindowDeltas(IReadOnlyList<StoreMetricDailyPoint> daily)
    {
        if (daily is null)
        {
            throw new ArgumentNullException(nameof(daily));
        }

        var deltas = new Dictionary<(string Kind, string Name), WindowDelta>();
        foreach (var series in daily.GroupBy(p => (p.ObjectKind, p.ObjectName)))
        {
            var points = series.OrderBy(p => p.Day).ToList();
            if (points.Count < 2)
            {
                continue;
            }

            var first = points[0];
            var last = points[^1];
            deltas[series.Key] = new WindowDelta(
                first.Day,
                last.TotalBytes - first.TotalBytes,
                Counted(first.TotalRuns, last.TotalRuns),
                Counted(first.TotalFailures, last.TotalFailures));
        }

        return deltas;
    }

    /// <summary>A cumulative counter's increase, or null when either end is missing or it went backwards.</summary>
    private static long? Counted(long? first, long? last)
    {
        if (first is null || last is null || last < first)
        {
            return null;
        }

        return last - first;
    }

    /// <summary>Which of the three shapes a <c>get_store_metrics</c> call resolved to (#3903).</summary>
    public enum StoreMetricsView
    {
        /// <summary>No filter: the store-level blocks and three ranked lists.</summary>
        Summary,

        /// <summary>A filter matched objects: every match, ranked, as one list bounded by <c>limit</c>.</summary>
        List,

        /// <summary>An exact <c>object_name</c> matched one object: its row and its daily series.</summary>
        Object,
    }

    /// <summary>What the filters selected: the view, and the listed rows it covers (every listed row for the
    /// summary; possibly none for a filter, which the tool answers as <c>empty</c>).</summary>
    public sealed record ObjectSelection(StoreMetricsView View, IReadOnlyList<StoreMetricRow> Matched);

    /// <summary>
    /// Resolves the two filters against the latest rows (#3903). Pure. <paramref name="objectKind"/> narrows
    /// to one kind; <paramref name="objectName"/> then matches case-insensitively, EXACT first: one exact
    /// match is the object view, because a name that is also a substring of others (wait_stats, and the
    /// jobs and aggregates named after it) must still be reachable on its own. With no exact match, every
    /// name that contains it is listed. Only <see cref="ListedKinds"/> are ever candidates.
    /// </summary>
    public static ObjectSelection SelectObjects(IReadOnlyList<StoreMetricRow> latest, string? objectKind, string? objectName)
    {
        if (latest is null)
        {
            throw new ArgumentNullException(nameof(latest));
        }

        var candidates = latest
            .Where(r => ListedKinds.Contains(r.ObjectKind, StringComparer.Ordinal))
            .Where(r => objectKind is null || string.Equals(r.ObjectKind, objectKind, StringComparison.Ordinal))
            .ToList();

        if (objectName is null)
        {
            return new ObjectSelection(objectKind is null ? StoreMetricsView.Summary : StoreMetricsView.List, candidates);
        }

        var exact = candidates.Where(r => string.Equals(r.ObjectName, objectName, StringComparison.OrdinalIgnoreCase)).ToList();
        if (exact.Count == 1)
        {
            return new ObjectSelection(StoreMetricsView.Object, exact);
        }

        return new ObjectSelection(
            StoreMetricsView.List,
            exact.Count > 1
                ? exact
                : candidates.Where(r => r.ObjectName.Contains(objectName, StringComparison.OrdinalIgnoreCase)).ToList());
    }

    /// <summary>
    /// The one order every object list uses (#3903): byte-bearing objects largest first, then background
    /// jobs, those whose failure count grew in the window first and then the closest to their own cadence.
    /// A failing job leads because a failure is the more urgent signal, and one the Store Job Over Cadence
    /// alert never judges: it reads successful runs only (<see cref="TimescaleSupport.JobCadenceReadSql"/>).
    /// Ties break on the last run, kind and name, so two calls page the same way. Pure.
    /// </summary>
    public static List<StoreMetricRow> OrderForList(
        IEnumerable<StoreMetricRow> rows, IReadOnlyDictionary<(string Kind, string Name), WindowDelta> deltas)
    {
        return rows
            .OrderBy(r => IsByteBearing(r.ObjectKind) ? 0 : 1)
            .ThenByDescending(r => r.TotalBytes ?? long.MinValue)
            .ThenByDescending(r => Delta(deltas, r)?.FailuresInWindow ?? 0)
            .ThenByDescending(r => CadencePercent(r.LastRunDurationMs, r.ScheduleIntervalMs) ?? double.MinValue)
            .ThenByDescending(r => r.LastRunDurationMs ?? long.MinValue)
            .ThenBy(r => r.ObjectKind, StringComparer.Ordinal)
            .ThenBy(r => r.ObjectName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>The byte-bearing rows that have a growth figure, largest growth first (#3903). A shrinking
    /// object ranks by its negative delta, so it trails rather than vanishing. Pure.</summary>
    public static List<StoreMetricRow> OrderByGrowth(
        IEnumerable<StoreMetricRow> rows, IReadOnlyDictionary<(string Kind, string Name), WindowDelta> deltas)
    {
        return rows
            .Where(r => IsByteBearing(r.ObjectKind) && Delta(deltas, r)?.GrowthBytes is not null)
            .OrderByDescending(r => Delta(deltas, r)!.GrowthBytes)
            .ThenByDescending(r => r.TotalBytes ?? long.MinValue)
            .ThenBy(r => r.ObjectKind, StringComparer.Ordinal)
            .ThenBy(r => r.ObjectName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>One row's window delta, or null when its series has fewer than two points.</summary>
    public static WindowDelta? Delta(IReadOnlyDictionary<(string Kind, string Name), WindowDelta> deltas, StoreMetricRow row) =>
        deltas.TryGetValue((row.ObjectKind, row.ObjectName), out var delta) ? delta : null;

    /// <summary>One object's daily points, oldest first (#3903): the object view's series, matched on kind AND
    /// name for the reason <see cref="ComputeWindowDeltas"/> keys on both.</summary>
    public static List<StoreMetricDailyPoint> SeriesFor(IReadOnlyList<StoreMetricDailyPoint> daily, StoreMetricRow row)
    {
        if (daily is null)
        {
            throw new ArgumentNullException(nameof(daily));
        }

        return daily
            .Where(p => string.Equals(p.ObjectKind, row.ObjectKind, StringComparison.Ordinal)
                        && string.Equals(p.ObjectName, row.ObjectName, StringComparison.Ordinal))
            .OrderBy(p => p.Day)
            .ToList();
    }

    /// <summary>
    /// #4251's managed-store part: the store's own settings, from <c>collect.managed_conf_verdicts</c> (V146,
    /// #4215) rather than a live <c>pg_settings.pending_restart</c> read — the same connection this
    /// reader already uses (<c>mcp</c> role in managed mode) cannot see <c>pg_file_settings</c> or a
    /// <c>sourcefile</c> at all, and <c>pending_restart</c> itself reads <c>f</c> from any connection opened
    /// after the reload on Windows (#4251), which an MCP host's pooled connection always is. The PG-TARGET
    /// collectors (<c>PgServerConfigCollector</c>, <c>get_pg_server_config</c>) are a different surface —
    /// monitored servers, not the store itself — and are untouched here.
    /// </summary>
    public static async Task<IReadOnlyList<PerformanceMonitor.Darling.Service.ManagedConfVerdictRow>> GetManagedConfVerdictsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        return await PerformanceMonitor.Darling.Service.DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync(connection, cancellationToken);
    }
}
