/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store self-metrics MCP surface (#2068) — "how fast is the monitoring store growing and what's
/// driving it" as a query instead of an expedition. Reads the series the hourly <c>StoreSelfMetrics</c>
/// sweep persists: the latest size/compression snapshot per object (each hypertable, each payload
/// dimension table, the whole store) plus the daily series for the window, with the whole-store daily
/// growth and the derived per-server ingest rate — the number onboarding N servers multiplies. Store-level
/// by nature, so unlike almost every other read tool it takes no <c>server_name</c>: the store is the
/// server.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreMetricsTools
{
    /// <summary>The window ceiling — the sweep's own retention (<see cref="StoreSelfMetrics.RetentionDays"/>),
    /// past which there is nothing to read.</summary>
    public const int MaxDaysBack = StoreSelfMetrics.RetentionDays;

    [McpServerTool(Name = "get_store_metrics"), Description(
        "Gets the monitoring store's OWN size and growth metrics — not a monitored SQL Server's. The service records an hourly self-metrics snapshot: per-hypertable total size, pre/post-compression bytes and chunk count; the query-text and query-plan payload dimension tables' total size (the store's dominant payloads) and row counts; the whole store's size with the enabled-server count; and one row per TimescaleDB background job (CAGG refresh, compression, retention) with its last run duration, schedule interval, duration-vs-cadence percent, and run/failure totals — the jobs whose runtimes scale with fleet size. Returns the latest snapshot per object plus a daily series over the window, with the whole-store daily growth in bytes and the derived per-server ingest rate (daily growth / enabled servers). Each daily point is that day's LAST snapshot, never its maximum or its mean — the settled figure a growth question wants, but it means a MAXIMUM question (what was this job's longest run that day, did it enter its warning band) cannot be answered from this series: the day's peak is DROPPED rather than smoothed, so a day whose worst run crossed a threshold reads as a day that never approached it. The route that carries one row per run is TimescaleDB's own job history (timescaledb_information.job_history) — but ONLY while timescaledb.enable_job_execution_logging is on, and it defaults OFF, so on a store that has never had it turned on a maximum over that table returns zero rows, which reads as 'no run exceeded the line' rather than 'this instrument is off'. The job_history block in every response reports that setting's EFFECTIVE value and its source (plus the file that set it, where the connection is privileged enough to see it), so this redirect is never issued blind: read it before treating an empty job_history as an answer, and note that logging covers runs only from the point it was switched on because nothing earlier was recorded to recover. The setting answers 'is it on'; whether YOU will see rows is a second question, because job_history is ownership-filtered — its rows are visible only to members of the job's owner role or of the database owner, while the jobs and job_stats views show every role every job — so a role that can list all the jobs can still read an empty history on a store that is recording perfectly. The same block therefore also reports which role it read as (reader_role), that role's standing under the view's own predicate (visibility: All, Partial or None, with the job counts behind it), the rows it actually observed over a fixed 24-hour window (rows_observed, newest_row_at) beside how many jobs job_stats says started a run in that window (jobs_run_in_window), and a contradiction flag that is true only when recording is on, the reader can see every job's history, jobs ran, and the view showed no rows — the finding to investigate. In managed mode this tool reads as the least-privilege mcp role, which the view filters out (visibility None), so its rows_observed is zero by construction and the note names the role that can see; only a connection with owner standing makes the flag self-proving. The hourly snapshot behind the series has the same limit one grain down: it samples last_run_duration once an hour at a fixed offset, so a run longer than that offset is never recorded at all. Also reports, read LIVE from the catalog rather than from the recorded series, every retention policy the rollup-coverage gate is holding PAUSED, with the tier's actual data span and how many times its configured drop_after horizon it is really holding — a held policy records zero failures and a normal-looking last run, so it is invisible in the stored job telemetry and is a common cause of unexplained store growth. Use for capacity forecasting: what is driving store growth, how fast, what adding N servers would multiply, which background job is closest to outgrowing its own cadence, and whether retention is actually running.")]
    public static async Task<string> GetStoreMetrics(
        NpgsqlDataSource postgres,
        [Description("Days of daily-series history. Default 30; max 400 (the series' own retention).")] int days_back = 30)
    {
        if (days_back <= 0 || days_back > MaxDaysBack)
        {
            return $"Invalid days_back value '{days_back}'. Must be a positive integer (1-{MaxDaysBack}).";
        }

        try
        {
            var latest = await DarlingStoreMetricsReader.GetLatestAsync(postgres);
            if (latest.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No store self-metrics recorded yet. The service records a snapshot hourly (the first lands " +
                    "within an hour of starting on a store at schema V53 or later).");
            }

            var daily = await DarlingStoreMetricsReader.GetDailyAsync(
                postgres, DateTime.UtcNow.AddDays(-days_back));

            var storeDaily = daily
                .Where(p => p.ObjectKind == StoreSelfMetrics.StoreObjectKind)
                .OrderBy(p => p.Day)
                .ToList();
            var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(storeDaily);

            var storeLatest = latest.FirstOrDefault(r => r.ObjectKind == StoreSelfMetrics.StoreObjectKind);

            /* #2813: retention holds are read LIVE from the catalog, not from the series, because the
               series does not carry them. StoreSelfMetrics records total_runs and total_failures but not
               j.scheduled, so a policy the coverage gate has PAUSED reports zero failures and a plausible
               last run — indistinguishable from a healthy job in every stored column. On the production
               store that shape hid five held policies for 16 days while the tier grew to 4.5x its horizon.
               One catalog round trip answers it for the current moment; persisting it into the series is a
               migration rung's worth of work and the better long-term answer, tracked separately.

               This checks out its OWN connection (review catch — an earlier comment here claimed it reused
               one, which was not true of any code path in this method: the two readers above go through
               postgres.CreateCommand and leave nothing open). That is a third pooled checkout per call,
               which is cheap but is not free, and saying so is the point — a comment that overstates what
               the code does is worse than none. */
            List<RetentionHoldReading> holds;
            await using (var connection = await postgres.OpenConnectionAsync())
            {
                holds = (await TimescaleSupport.ReadRetentionHoldReadingsAsync(connection, logger: null))
                    .ToList();
            }

            var heldPolicies = holds
                .Where(h => !h.Armed)
                .OrderByDescending(h => h.OverHorizonRatio ?? 0)
                .ToList();

            /* #3175: the state of the instrument this tool's own description redirects a MAXIMUM question
               to. Read here rather than left to the caller because an empty job_history and a quiet fleet
               are the same result set, so a reader who follows the redirect cannot tell whether the answer
               they get back is a census or an artefact. Failure-isolated inside the reader, so this cannot
               fail the response it qualifies.

               #3574: and then the EVIDENCE behind that state — what this connection actually sees in the
               view, and whether the view's ownership predicate would show it anything. The GUC answers "is
               it on"; the reader's question is "will I see rows", and job_history filters by role membership
               where the jobs view a reader checks first does not. On a managed store this very connection is
               the least-privilege mcp role, which that predicate filters OUT, so the second read has to
               evaluate the predicate for itself rather than count and assume. A second statement rather than
               a column on the first: the two fail independently, and a count that timed out must not make
               the GUC read unknown. */
            var jobLogging = await DarlingStoreMetricsReader.GetJobExecutionLoggingAsync(postgres);
            var jobEvidence = await DarlingStoreMetricsReader.GetJobHistoryEvidenceAsync(postgres, jobLogging);

            return JsonSerializer.Serialize(new
            {
                as_of = latest.Max(r => r.MetricTime).ToString("o"),
                days_back,
                store = storeLatest is null ? null : new
                {
                    name = storeLatest.ObjectName,
                    total_bytes = storeLatest.TotalBytes,
                    enabled_server_count = storeLatest.EnabledServerCount,
                    daily_growth = growth.Select(g => new
                    {
                        day = g.Day.ToString("yyyy-MM-dd"),
                        delta_bytes = g.DeltaBytes,
                        per_server_bytes = g.PerServerBytes is { } rate ? Math.Round(rate) : (double?)null,
                    }),
                },
                /* #2813. Present on EVERY response, including when nothing is held — an absent block and
                   "nothing is held" must not look alike, which is the entire failure this reports on. */
                retention = new
                {
                    policy_count = holds.Count,
                    held_count = heldPolicies.Count,
                    note = holds.Count == 0
                        ? "No retention policies found (a plain-PostgreSQL store, or TimescaleDB is unavailable)."
                        : heldPolicies.Count == 0
                            ? "Every retention policy is armed."
                            : "HELD policies are PAUSED by the rollup-coverage gate so retention cannot drop history a "
                              + "rollup has never materialized. They arm themselves once the consumer catches up; the "
                              + "missing step is a backfill (--backfill-rollups). Arming one by hand drops the only copy "
                              + "of that history. over_horizon_ratio is how many times its configured depth the tier is "
                              + "actually holding — the cost of the hold.",
                    held = heldPolicies.Select(h => new
                    {
                        hypertable = h.HypertableName,
                        job_id = h.JobId,
                        drop_after = h.DropAfter,
                        chunk_count = h.ChunkCount,
                        actual_span_days = h.SpanSeconds is { } sec ? Math.Round(sec / 86400.0, 1) : (double?)null,
                        over_horizon_ratio = h.OverHorizonRatio is { } r ? Math.Round(r, 2) : (double?)null,
                    }),
                },
                /* #3175. Present on EVERY response, for the #2813 reason one line up and one step further:
                   here the absence being guarded is the absence of ROWS in the instrument this description
                   sends a maximum question to, so an absent block would leave the redirect unqualified
                   exactly when it is wrong. Reported as four states, never a bool — "the probe did not
                   run", "this server has no such setting" and "it is switched off" call for three
                   different readings of an empty job_history. */
                job_history = new
                {
                    execution_logging = jobLogging.Status.ToString(),
                    recording = jobLogging.Recording,
                    setting = jobLogging.Setting,
                    source = jobLogging.Source,
                    source_file = jobLogging.SourceFile,
                    /* #3574. The evidence fields, all null unless evidence = Observed. reader_role first,
                       because every count that follows is a count through THAT role's eyes and the view
                       decides per role what it shows; visibility is the predicate's verdict on that role,
                       derived from the two membership facts beside it rather than asserted. rows_observed
                       travels with its window so the number never leaves without its denominator, and
                       jobs_run_in_window is the population half from the UNFILTERED job_stats view — the
                       proof that there was something to see. contradiction is the new finding class as one
                       bool, true only when all four of its conditions hold; the note spells them out. */
                    evidence = jobEvidence.Status.ToString(),
                    reader_role = jobEvidence.ReaderRole,
                    reader_is_database_owner_member = jobEvidence.ReaderIsDatabaseOwnerMember,
                    visibility = jobEvidence.Status == DarlingStoreMetricsReader.JobHistoryEvidenceStatus.Observed
                        ? jobEvidence.Visibility.ToString()
                        : null,
                    job_count = jobEvidence.JobCount,
                    history_visible_job_count = jobEvidence.HistoryVisibleJobCount,
                    observed_window_hours = DarlingStoreMetricsReader.JobHistoryEvidenceWindowHours,
                    rows_observed = jobEvidence.RowsObserved,
                    newest_row_at = jobEvidence.NewestRowAt?.ToString("o", CultureInfo.InvariantCulture),
                    jobs_run_in_window = jobEvidence.JobsRunInWindow,
                    newest_run_started_at = jobEvidence.NewestRunStartedAt?.ToString("o", CultureInfo.InvariantCulture),
                    contradiction = jobEvidence.ContradictsRecording(jobLogging.Recording),
                    note = JobHistoryNote(jobLogging, jobEvidence),
                },
                objects = latest
                    .Where(r => r.ObjectKind != StoreSelfMetrics.StoreObjectKind)
                    .OrderByDescending(r => r.TotalBytes ?? 0)
                    .Select(r => new
                    {
                        object_kind = r.ObjectKind,
                        object_name = r.ObjectName,
                        metric_time = r.MetricTime.ToString("o"),
                        total_bytes = r.TotalBytes,
                        compressed_before_bytes = r.CompressedBeforeBytes,
                        compressed_after_bytes = r.CompressedAfterBytes,
                        /* How many times smaller compression made what it compressed — before/after, the
                           way the operator already talks about it (6-36x measured on the motivating store). */
                        compression_ratio = r.CompressedBeforeBytes is > 0 && r.CompressedAfterBytes is > 0
                            ? Math.Round(r.CompressedBeforeBytes.Value / (double)r.CompressedAfterBytes.Value, 1)
                            : (double?)null,
                        chunk_count = r.ChunkCount,
                        row_count = r.RowCount,
                        /* #2136 background_job rows only (NULL elsewhere): last run duration, the job's own
                           cadence, and how much of that cadence the run consumed — the ceiling-proximity
                           number an onboarding wave moves first. */
                        last_run_duration_ms = r.LastRunDurationMs,
                        schedule_interval_ms = r.ScheduleIntervalMs,
                        duration_vs_cadence_percent = r.LastRunDurationMs is > 0 && r.ScheduleIntervalMs is > 0
                            ? Math.Round(100.0 * r.LastRunDurationMs.Value / r.ScheduleIntervalMs.Value, 1)
                            : (double?)null,
                        total_runs = r.TotalRuns,
                        total_failures = r.TotalFailures,
                    }),
                daily = daily
                    .Where(p => p.ObjectKind != StoreSelfMetrics.StoreObjectKind)
                    .GroupBy(p => (p.ObjectKind, p.ObjectName))
                    .OrderBy(g => g.Key.ObjectKind, StringComparer.Ordinal)
                    .ThenBy(g => g.Key.ObjectName, StringComparer.Ordinal)
                    .Select(g => new
                    {
                        object_kind = g.Key.ObjectKind,
                        object_name = g.Key.ObjectName,
                        points = g.OrderBy(p => p.Day).Select(p => new
                        {
                            day = p.Day.ToString("yyyy-MM-dd"),
                            total_bytes = p.TotalBytes,
                            compressed_before_bytes = p.CompressedBeforeBytes,
                            compressed_after_bytes = p.CompressedAfterBytes,
                            chunk_count = p.ChunkCount,
                            row_count = p.RowCount,
                            last_run_duration_ms = p.LastRunDurationMs,
                            schedule_interval_ms = p.ScheduleIntervalMs,
                            total_runs = p.TotalRuns,
                            total_failures = p.TotalFailures,
                        }),
                    }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_metrics", ex);
        }
    }

    /// <summary>
    /// What an empty <c>timescaledb_information.job_history</c> means on THIS store (#3175), and for WHOM
    /// (#3574). Two halves, concatenated. The first is the GUC's: one sentence per state, and the states
    /// deliberately do not share one — the whole defect is that "off" and "nothing happened" produce the
    /// same empty result, so a note that hedged across both would reproduce it in prose; <c>Off</c> splits
    /// again on whether anything SET it off, because the two need different actions (one heals itself, the
    /// other needs an override removed). The second half is the evidence's, from
    /// <see cref="JobHistoryVisibilityNote"/>: the ownership rule the view enforces, which role this block
    /// read as, what that role is allowed to see, what it saw, and whether the four conditions of the
    /// contradiction hold. It is appended to every arm on which the view exists — including the GUC-off
    /// arms, because a reader who heals the GUC and then checks as the wrong role walks into the same trap
    /// one step later — and omitted only where there is no view to be filtered.
    /// </summary>
    internal static string JobHistoryNote(
        DarlingStoreMetricsReader.JobExecutionLoggingReading reading,
        DarlingStoreMetricsReader.JobHistoryEvidence evidence)
        => GucNote(reading) + JobHistoryVisibilityNote(reading, evidence);

    private static string GucNote(DarlingStoreMetricsReader.JobExecutionLoggingReading reading)
        => reading.Status switch
        {
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.On =>
                "timescaledb.enable_job_execution_logging is ON, so timescaledb_information.job_history holds one row "
                + "per background-job run and a MAXIMUM over it is a census of the runs it covers. It covers runs from "
                + "the moment logging was turned on, never before: nothing was written for earlier runs, so an empty "
                + "window that predates that point is expected and is not evidence about those runs.",

            DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off when reading.OffByExplicitOverride =>
                "timescaledb.enable_job_execution_logging is OFF and something SET it off — 'source' is not 'default'. "
                + "timescaledb_information.job_history is NOT recording, so a maximum over it returns zero rows and "
                + "that means 'this instrument is off', NOT 'no run exceeded the line'. The service's managed conf "
                + "block does not correct this one, because whatever set it is winning by last-occurrence: either an "
                + "ALTER SYSTEM (which lands in postgresql.auto.conf, read after postgresql.conf and so beating the "
                + "managed block outright, cleared with ALTER SYSTEM RESET) or a hand-added line placed after the "
                + "managed block in postgresql.conf itself. 'source_file' names which file won when the connection is "
                + "privileged enough to see it — that column is superuser-only, so it is normally null here. Until the "
                + "override is removed the only surface is job_stats — the last_run_duration_ms in this response — "
                + "which carries one sample per job, not one row per run.",

            DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off =>
                "timescaledb.enable_job_execution_logging is OFF, so timescaledb_information.job_history is NOT "
                + "recording. A maximum over it returns zero rows, and that means 'this instrument is off', NOT 'no "
                + "run exceeded the line' — do not read an empty job_history on this store as a clean result. A "
                + "managed store turns it on by gaining the v11 postgresql.conf block on the next server start the "
                + "service owns; runs before that point wrote nothing and cannot be recovered. Until then the only "
                + "surface is job_stats — the last_run_duration_ms in this response — which carries one sample per "
                + "job, not one row per run.",

            DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered =>
                "timescaledb.enable_job_execution_logging is not a setting this connection knows about, which means "
                + "either this store is plain PostgreSQL or the timescaledb extension is not installed in this "
                + "database — the GUC is defined by the VERSIONED TimescaleDB library, and the preloaded loader only "
                + "pulls that in for a database that has the extension. Both cases have the same consequence here: "
                + "timescaledb_information.job_history does not exist on this connection, so there is no per-run "
                + "surface at all, and the series above carries no background-job rows either.",

            _ =>
                "The pg_settings probe for timescaledb.enable_job_execution_logging did not complete, so whether "
                + "timescaledb_information.job_history is recording is UNKNOWN — which is not the same as off. Treat "
                + "an empty job_history on this store as unexplained rather than as a clean result until this reads.",
        };

    /// <summary>
    /// The visibility rule, stated with a measurement (#3574). The rule itself is one sentence and it is the
    /// same on every arm: history rows are visible only to members of the job's owner role or of the database
    /// owner, and the <c>jobs</c> / <c>job_stats</c> views a reader checks first are NOT filtered, which is
    /// the trap. What follows it depends on what the evidence read established about THIS reader:
    /// <list type="bullet">
    /// <item><b>None</b> — the managed-mode <c>mcp</c> role's reading on every store: the view shows this
    /// connection nothing by construction, so its zero is the filter and not the table, and the note names
    /// the role that can see and says what the unfiltered <c>job_stats</c> saw in the meantime.</item>
    /// <item><b>All</b> — the count is a census, and the flag is self-proving: rows seen means recording is
    /// a measurement; zero rows with runs in the window and the GUC on is the CONTRADICTION, named as such,
    /// with the one benign cause and how to settle it; zero rows with no runs proves nothing either way and
    /// the note says so rather than calling it clean.</item>
    /// <item><b>Partial</b> — a census of the jobs this reader owns, stated as a fraction, no verdict.</item>
    /// <item><b>Unknown</b> after an <c>Observed</c> read — no jobs exist, so there is no owner to be a
    /// member of; after an <c>Unreadable</c> one — the flag stays a GUC echo and the note says so.</item>
    /// </list>
    /// Empty for <see cref="DarlingStoreMetricsReader.JobHistoryEvidenceStatus.NotApplicable"/>: the view
    /// does not exist on that connection, and a rule about who may read a view that is not there would be
    /// noise on the one arm whose existing text already says everything true.
    ///
    /// <para>Counts are formatted invariant and timestamps as ISO 8601 UTC so the sentence a caller reads
    /// agrees byte-for-byte with the fields beside it.</para>
    /// </summary>
    internal static string JobHistoryVisibilityNote(
        DarlingStoreMetricsReader.JobExecutionLoggingReading reading,
        DarlingStoreMetricsReader.JobHistoryEvidence evidence)
    {
        const string rule =
            " WHO CAN SEE THE ROWS: timescaledb_information.job_history shows a row only to a member of the job's "
            + "owner role or of the database owner (its own WHERE clause: pg_has_role(current_user, <database "
            + "owner>, 'MEMBER') OR pg_has_role(current_user, <job owner>, 'MEMBER')), while the jobs and "
            + "job_stats views show every role every job — so a role that can list all the jobs can still read an "
            + "empty history on a store that is recording perfectly, and a zero from a role outside those "
            + "memberships contradicts nothing.";

        switch (evidence.Status)
        {
            case DarlingStoreMetricsReader.JobHistoryEvidenceStatus.NotApplicable:
                return "";

            case DarlingStoreMetricsReader.JobHistoryEvidenceStatus.Unreadable:
                return rule
                    + " The evidence read behind this block did not complete, so what THIS connection sees in "
                    + "job_history is UNKNOWN and 'recording' above is the GUC's word alone, not a measurement.";
        }

        var role = evidence.ReaderRole ?? "(unknown role)";
        var window = Invariant(DarlingStoreMetricsReader.JobHistoryEvidenceWindowHours);
        var rows = Invariant(evidence.RowsObserved ?? 0);
        var ran = Invariant(evidence.JobsRunInWindow ?? 0);
        var newestRow = evidence.NewestRowAt is { } nr ? nr.ToString("o", CultureInfo.InvariantCulture) : null;
        var newestRun = evidence.NewestRunStartedAt is { } ns ? ns.ToString("o", CultureInfo.InvariantCulture) : null;

        switch (evidence.Visibility)
        {
            case DarlingStoreMetricsReader.JobHistoryVisibility.None:
                return rule
                    + $" This block read as '{role}', which is a member of NEITHER, so the view shows this connection "
                    + $"NOTHING by construction: rows_observed = {rows} here is the filter, not the table, and says "
                    + "nothing about whether recording works. The service's owner role — the role that created the "
                    + "jobs — sees the rows; read job_history as that role before concluding anything from an empty "
                    + $"result. Meanwhile job_stats, which is not filtered, says {ran} job(s) started a run in the "
                    + $"last {window} hours"
                    + (newestRun is null ? "." : $" (newest start {newestRun}).");

            case DarlingStoreMetricsReader.JobHistoryVisibility.Partial:
                return rule
                    + $" This block read as '{role}', which is a member of the owner of "
                    + $"{Invariant(evidence.HistoryVisibleJobCount ?? 0)} of {Invariant(evidence.JobCount ?? 0)} jobs "
                    + "and not of the database owner, so the count is a census of THOSE jobs only: "
                    + $"{rows} row(s) with a start in the last {window} hours"
                    + (newestRow is null ? ", none ever." : $", newest {newestRow}.")
                    + " A job outside that set shows this role nothing, whatever it recorded.";

            case DarlingStoreMetricsReader.JobHistoryVisibility.All:
            {
                var standing = evidence.ReaderIsDatabaseOwnerMember == true
                    ? "a member of the database owner"
                    : "a member of every job's owner";
                var census =
                    $" This block read as '{role}', which is {standing}, so the count is a census: {rows} row(s) with a "
                    + $"start in the last {window} hours"
                    + (newestRow is null ? ", none ever" : $", newest {newestRow}")
                    + $"; job_stats says {ran} job(s) started a run in the same window"
                    + (newestRun is null ? "." : $" (newest start {newestRun}).");

                if (evidence.ContradictsRecording(reading.Recording))
                {
                    return rule + census
                        + " CONTRADICTION: the GUC says recording, this role can see every job's history, jobs "
                        + "started runs inside the window, and the view showed NONE of them — do not read this zero "
                        + "as quiet. The one benign cause is logging switched on AFTER the last of those starts (the "
                        + "managed v11 heal lands on a service-owned server start and writes nothing for earlier "
                        + "runs), which the next hourly run settles: re-read after it. A zero that persists while "
                        + "jobs_run_in_window climbs means the instrument is not writing what the GUC says it is, "
                        + "and that is a finding, not a quiet hour.";
                }

                if (reading.Recording && evidence.RowsObserved is > 0)
                {
                    return rule + census
                        + " 'recording' is therefore a measurement here, not a GUC echo.";
                }

                if (reading.Recording)
                {
                    /* Zero rows, zero runs: nothing happened for the instrument to catch, so the zero is not
                       evidence either way — and saying "clean" here would be the exact mis-reading #3175 and
                       #3574 exist to prevent, one level down. */
                    return rule + census
                        + " No job started a run in the window, so there was nothing to record and this zero "
                        + "proves nothing either way; it is not a clean result, it is an absence of information.";
                }

                if (reading.Status == DarlingStoreMetricsReader.JobExecutionLoggingStatus.Unreadable)
                {
                    /* The GUC could not be read but the view could: the rows are real and this reader sees
                       them all, but whether they are the successes logging records or the failures TimescaleDB
                       writes regardless cannot be said without the setting — so it is not said. */
                    return rule + census
                        + " Whether logging is on could not be read, so these rows are not classified: TimescaleDB "
                        + "writes a FAILED run's row regardless of the setting and a successful run's only while "
                        + "it is on.";
                }

                /* GUC off, reader admitted: say what it will see once logging is on, and what it sees even
                   now — TimescaleDB writes a FAILED run's row regardless of the setting (the
                   bgw_job_stat_history_update path in 2.28.1 logs failures unconditionally), so a non-zero
                   count with the GUC off is failures, not a sign that logging is secretly on. */
                return rule + census
                    + " Once logging is on this connection will see the rows; any it sees now are FAILED runs, "
                    + "which TimescaleDB writes regardless of the setting — successes need it on.";
            }

            default:
                return rule
                    + $" This block read as '{role}'; timescaledb_information.jobs lists no jobs on this connection, "
                    + "so there is no owner to be a member of and nothing for history to record yet.";
        }
    }

    private static string Invariant(long value) => value.ToString(CultureInfo.InvariantCulture);
}
