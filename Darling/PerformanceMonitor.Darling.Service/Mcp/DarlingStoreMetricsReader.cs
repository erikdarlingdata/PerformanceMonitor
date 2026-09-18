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
/// <para>Plus two reads that are not metrics at all: <see cref="JobExecutionLoggingSql"/> asks whether
/// <c>timescaledb_information.job_history</c> — the route the tool's description sends a maximum question
/// to, since this series cannot answer one — is switched on (#3175), and <see cref="JobHistoryEvidenceSql"/>
/// asks whether the connection doing the asking would SEE its rows and how many it does see (#3574). Together
/// they qualify the redirect, so a caller who follows it can tell a census from an empty table — and can
/// tell an empty table from a table the view is hiding from them.</para>
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
    /// so many words, naming the role. On a bring-your-own store whose connection string is the owner role
    /// the count IS the census and the flag becomes self-proving; on a managed store the block says it
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
    /// </summary>
    public const string JobHistoryEvidenceSql = @"
SELECT
    current_user::text AS reader_role,
    pg_has_role(
        current_user,
        (SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()),
        'MEMBER') IS TRUE AS reader_is_database_owner_member,
    (SELECT count(*) FROM timescaledb_information.jobs) AS job_count,
    (SELECT count(*)
       FROM timescaledb_information.jobs AS j
      WHERE pg_has_role(current_user, j.owner, 'MEMBER') IS TRUE) AS owner_member_job_count,
    (SELECT count(*)
       FROM timescaledb_information.job_history AS h
      WHERE h.start_time >= $1) AS rows_observed,
    (SELECT max(h.start_time) FROM timescaledb_information.job_history AS h) AS newest_row_at,
    (SELECT count(*)
       FROM timescaledb_information.job_stats AS js
      WHERE js.last_run_started_at >= $1) AS jobs_run_in_window,
    (SELECT max(NULLIF(js.last_run_started_at, '-infinity'::timestamptz))
       FROM timescaledb_information.job_stats AS js) AS newest_run_started_at";

    /// <summary>The evidence window <see cref="JobHistoryEvidenceSql"/> counts over, in hours. Fixed, not
    /// <c>days_back</c> — the paragraph on that constant says why. Published in the response beside the
    /// count so the number never travels without its denominator.</summary>
    public const int JobHistoryEvidenceWindowHours = 24;

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
