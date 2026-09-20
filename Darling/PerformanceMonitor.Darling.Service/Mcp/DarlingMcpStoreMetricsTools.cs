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
/// sweep persists: the latest size/compression snapshot per object (each hypertable, each continuous
/// aggregate, each payload dimension table, each named plain table, the two catch-all rows, the whole
/// store) plus the daily series for the window, with the whole-store daily growth and the derived
/// per-server ingest rate — the number onboarding N servers multiplies. Since #3582 it also states its own
/// coverage: how much of <c>pg_database_size</c> the named rows account for, how much sits in relations
/// nobody named, and whether the whole reconciles — because on the largest production store the previous
/// inventory answered "here is the store" for 38% of it and a growth investigation trusted it. Store-level
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
        "Gets the monitoring store's OWN size and growth metrics — not a monitored SQL Server's. The service records an hourly self-metrics snapshot: per-hypertable total size, pre/post-compression bytes and chunk count; per-continuous-aggregate total size, pre/post-compression bytes and chunk count, sized through the aggregate's materialization hypertable and reported under the aggregate's view name (object_kind continuous_aggregate) — on one production store the twenty aggregates were 57% of the database and the previous inventory showed none of them, because timescaledb_information.hypertables never lists a materialization; the query-text and query-plan payload dimension tables' total size (the store's dominant payloads) and row counts; the product's named plain tables (object_kind table: collect.query_store_text, which stores statement text inline by design, collect.query_store_plan_map, config.config_alert_log) with total size and the planner's row-count estimate; two catch-all rows — object_kind other, every relation in a user schema that no named row accounts for, and object_kind system, the PostgreSQL catalogs and TimescaleDB's own bookkeeping — each with its byte total and relation count; the whole store's size with the enabled-server count; and one row per TimescaleDB background job (CAGG refresh, compression, retention) with its last run duration, schedule interval, duration-vs-cadence percent, and run/failure totals — the jobs whose runtimes scale with fleet size. The inventory block RECONCILES the newest sweep against its own pg_database_size and states coverage in so many words: enumerated_percent is the share under named objects, attributed_percent the share any row accounts for, residual_bytes what no row explains, and reconciled is false when that residual exceeds the larger of 1% and 64 MiB — a gap that does not close is itself a finding, and the note says so. bytes_by_kind gives the per-kind subtotals (which is where 'what is driving growth' is answered in one glance), and largest_unenumerated names the biggest relations inside the other row, read live, so that figure is something to act on. Each continuous_aggregate object also carries, read LIVE from the catalog, compression_enabled and the job ids of its refresh, compression and retention policies (null where none exists) — the facts a growth investigation into the aggregates otherwise assembles by hand. Returns the latest snapshot per object plus a daily series over the window, with the whole-store daily growth in bytes and the derived per-server ingest rate (daily growth / enabled servers). Each daily point is that day's LAST snapshot, never its maximum or its mean — the settled figure a growth question wants, but it means a MAXIMUM question (what was this job's longest run that day, did it enter its warning band) cannot be answered from this series: the day's peak is DROPPED rather than smoothed, so a day whose worst run crossed a threshold reads as a day that never approached it. The route that carries one row per run is TimescaleDB's own job history (timescaledb_information.job_history) — but it records a SUCCESSFUL run only while timescaledb.enable_job_execution_logging is on, and that setting defaults OFF (a FAILED run's row is written regardless of it), so on a store that has never had it turned on a maximum over that table covers zero rows of successful runs, which reads as 'no run exceeded the line' rather than 'this instrument is off'. The job_history block in every response reports that setting's EFFECTIVE value and its source (plus the file that set it, where the connection is privileged enough to see it), so this redirect is never issued blind: read it before treating an empty job_history as an answer, and note that logging covers runs only from the point it was switched on because nothing earlier was recorded to recover. The setting answers 'is it on'; whether YOU will see rows is a second question, because job_history is ownership-filtered — its rows are visible only to members of the job's owner role or of the database owner, while the jobs and job_stats views show every role every job — so a role that can list all the jobs can still read an empty history on a store that is recording perfectly. The same block therefore also reports which role it read as (reader_role), that role's standing under the view's own predicate (visibility: All, Partial or None, with the job counts behind it), the rows it actually observed over a fixed 24-hour window (rows_observed, newest_row_at) beside how many jobs job_stats says started a run in that window (jobs_run_in_window), and a contradiction flag that is true only when recording is on, a reader the view admits to every job's history saw no rows, and jobs ran — the finding to investigate. In managed mode this tool reads as the least-privilege mcp role, which the view filters out (visibility None), so its own rows_observed is zero by construction and the note names the role that can see. That role is the service's owner, and the service's hourly self-metrics sweep runs as it: the sweep persists the owner's own count over the same 24-hour window into the series, and the block reports it beside this connection's verdict as owner_evidence (Observed, Stale, Filtered or Absent), owner_role, owner_observed_at, owner_rows_observed, owner_newest_row_at and owner_jobs_run_in_window — so on a managed store 'recording' is a measurement after all, taken by the service's sweep rather than by this connection, the note says which, and the contradiction flag is computed from the owner's numbers when a fresh owner reading exists. The hourly snapshot behind the series has the same limit one grain down: it samples last_run_duration once an hour at a fixed offset, so a run longer than that offset is never recorded at all. Also reports, read LIVE from the catalog rather than from the recorded series, every retention policy the rollup-coverage gate is holding PAUSED, with the tier's actual data span and how many times its configured drop_after horizon it is really holding — a held policy records zero failures and a normal-looking last run, so it is invisible in the stored job telemetry and is a common cause of unexplained store growth. Use for capacity forecasting: what is driving store growth, how fast, what adding N servers would multiply, which background job is closest to outgrowing its own cadence, whether retention is actually running, and how much of the store the inventory can actually see. TOAST UTILISATION (V137, #3783): each payload dimension object (object_kind dimension) also carries toast_bytes, toast_live_bytes, toast_utilisation_pct and toast_utilisation_note, because the plan XML and statement text do not live in the dimension's heap — they live in its TOAST file, and pg_total_relation_size says how big that file is and nothing about how FULL it is. A dimension that cycles rows (~755 k new distinct plans a day on one production store class, purged in row-capped slices) leaves free space inside the TOAST file that ordinary VACUUM returns to the table and never to the operating system, so the file sits at its high-water mark: the V54 text-to-gzip conversion plus that churn left one production store's query_plan_dim TOAST file at ~40 % utilisation (154 GB holding ~61 GB of live chunks) — ~93 GB of slack reported as store size. toast_utilisation_pct is toast_live_bytes / toast_bytes (one decimal) and is null when either is null or the file is empty; toast_live_bytes needs the pg_freespacemap extension, which the bundled store image ships and does not install, so on a store without it the note says 'not measured' in so many words and NOTHING is computed from total_bytes or a tuple share (after a VACUUM the dead tuples are gone and the file keeps its pages, so a tuple share reads 100 % on exactly the file in question). When the percentage IS measured and reads under 50 % on a file over 10 GiB, the note carries the reclaim path: --recompress-plan-dim --vacuum-full, at the MAINTAINER's word in a maintenance window, because VACUUM FULL takes an ACCESS EXCLUSIVE lock on the dimension for the rebuild and needs free disk for a full copy of the live data; the same condition fires the informational Store TOAST Slack self-alert once a day. This tool never reclaims anything by itself. Hypertable, aggregate, table and catch-all objects carry the four TOAST fields as null: they have no TOAST columns to measure. CHECKPOINTER (V137, #3783): the checkpointer block is the store's OWN checkpointer over the last self-metrics interval — write_ms and sync_ms (milliseconds the checkpointer spent in its write and sync/fsync phases inside the interval), requested (checkpoints in the interval forced by WAL volume reaching max_wal_size rather than by checkpoint_timeout), interval_seconds (MEASURED between the two sweeps that bound it, not the assumed cadence), observed_at / previous_at, and the cumulative counters beside them. The sweep stores the server's cumulative counters and this block differences the two newest rows, so status says whether there IS an interval: Observed (judged), NoPrevious (one row so far), Reset (a counter went backwards — pg_stat_reset_shared or a restart between sweeps, so no delta is stated rather than a negative one), or Absent. It exists because three unattributed read kills on a production store in one day all sat inside checkpoint sync phases of 25.2 s and 14.0 s and nothing in the store recorded that; the informational Store Checkpointer Pressure self-alert fires when sync_ms exceeds 10,000 or requested exceeds 0 in an interval, naming WAL sizing (#3802) and refresh slicing (#3745) as the levers. The checkpointer row is not in objects[] or daily[]: it carries no bytes.")]
    public static async Task<string> GetStoreMetrics(
        NpgsqlDataSource postgres,
        [Description("Days of daily-series history. Default 30; max 400 (the series' own retention).")] int days_back = 30)
    {
        /* #3653: the shared day-grained refusal, in ValidateHoursBack's sentence; the ceiling stays this
           tool's (the daily series' own retention). */
        var daysError = McpHelpers.ValidateDaysBack(days_back, MaxDaysBack);
        if (daysError != null)
        {
            return daysError;
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

            /* #3574, the managed-mode half: the OWNER's reading, decoded from the row the hourly sweep
               persisted. Pure over rows already in hand — no fourth read — and dated, so the note can say
               how old the owner's count is rather than presenting an hour-old measurement as this instant's. */
            var ownerEvidence = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(latest, DateTime.UtcNow);

            /* #3582: the coverage statement. Pure over the same rows; null only when no store row exists to
               reconcile against, in which case the block says coverage is unknown instead of computing a
               percentage of nothing. */
            var inventory = DarlingStoreMetricsReader.ComputeInventory(latest);

            /* #3783: the store's own checkpointer, differenced from the two newest checkpointer rows. A third
               read (the latest read takes one row per object and a difference needs two), not failure-isolated
               inside the reader: this is a primary block, and a throw here is a tool error the outer catch
               envelopes rather than a plausible 'Absent' dressed as a reading. */
            var checkpointer = await DarlingStoreMetricsReader.GetCheckpointerAsync(postgres);

            /* #3582: two more LIVE catalog reads, both failure-isolated to null (never to an empty list,
               which would read as "no aggregates" / "nothing un-enumerated" and drop a section without a
               word). The aggregate flags are STATE the series deliberately does not carry — the same
               reasoning as the #2813 retention holds one screen up — and the top-N is what makes the
               other row's byte count actionable: a growth investigation reads the table's name here
               instead of running the pg_class census by hand. Both skip the TimescaleDB catalogs where the
               GUC probe established they do not exist for this database. */
            var aggregateStates = await DarlingStoreMetricsReader.GetContinuousAggregateStatesAsync(postgres, jobLogging);
            var largestUnenumerated = await DarlingStoreMetricsReader.GetLargestUnenumeratedAsync(postgres, jobLogging);
            var aggregateStateByView = (aggregateStates ?? Array.Empty<DarlingStoreMetricsReader.ContinuousAggregateState>())
                .GroupBy(s => s.ViewName, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

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
                /* #3582. Present on EVERY response: the coverage statement is the qualifier on everything
                   in objects[] below, and a response without it is the response that answered for 38% of
                   a store and let a growth investigation trust it. Null fields where the newest sweep did
                   not produce the row they come from, never a zero standing in for a missing reading. */
                inventory = inventory is null ? null : new
                {
                    sweep_at = inventory.SweepAt.ToString("o"),
                    database_bytes = inventory.DatabaseBytes,
                    enumerated_bytes = inventory.EnumeratedBytes,
                    enumerated_percent = inventory.EnumeratedPercent,
                    attributed_bytes = inventory.AttributedBytes,
                    attributed_percent = inventory.AttributedPercent,
                    residual_bytes = inventory.ResidualBytes,
                    tolerance_bytes = inventory.ToleranceBytes,
                    reconciled = inventory.Reconciled,
                    stale_object_rows = inventory.StaleRowCount,
                    bytes_by_kind = inventory.BytesByKind
                        .OrderByDescending(kv => kv.Value)
                        .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal),
                    unenumerated = inventory.UnenumeratedBytes is null ? null : new
                    {
                        total_bytes = inventory.UnenumeratedBytes,
                        relation_count = inventory.UnenumeratedRelationCount,
                    },
                    system = inventory.SystemBytes is null ? null : new
                    {
                        total_bytes = inventory.SystemBytes,
                        relation_count = inventory.SystemRelationCount,
                    },
                    /* Live, not from the sweep: what the other row is MADE of, largest first. Null when
                       the read did not complete; an empty list means the census found nothing. */
                    largest_unenumerated = largestUnenumerated?.Select(r => new
                    {
                        relation = r.Relation,
                        relkind = r.RelKind,
                        total_bytes = r.TotalBytes,
                    }),
                    aggregate_state = aggregateStates is null ? "Unreadable" : "Observed",
                    note = InventoryNote(inventory, latest, aggregateStates, largestUnenumerated),
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
                    /* #3574, managed-mode self-proof. The OWNER's reading from the hourly sweep, beside this
                       connection's. owner_evidence says whether there is one to lean on (Observed / Stale /
                       Filtered / Absent); the rest are null unless a row exists. owner_observed_at is the
                       instant the count is true of — an hour-old measurement is reported as one. */
                    owner_evidence = ownerEvidence.Status.ToString(),
                    owner_role = ownerEvidence.ReaderRole,
                    owner_observed_at = ownerEvidence.ObservedAt?.ToString("o", CultureInfo.InvariantCulture),
                    owner_observed_age_hours = ownerEvidence.AgeHours,
                    owner_observed_window_hours = ownerEvidence.WindowHours,
                    owner_rows_observed = ownerEvidence.RowsObserved,
                    owner_newest_row_at = ownerEvidence.NewestRowAt?.ToString("o", CultureInfo.InvariantCulture),
                    owner_jobs_run_in_window = ownerEvidence.JobsRunInWindow,
                    /* True from EITHER admitted reader's numbers: this connection's where it has owner
                       standing, the sweep's where it does not. Each conjunction is its own record's, and
                       the note says which one fired. */
                    contradiction = jobEvidence.ContradictsRecording(jobLogging.Recording)
                        || ownerEvidence.ContradictsRecording(jobLogging.Recording),
                    note = JobHistoryNote(jobLogging, jobEvidence, ownerEvidence),
                },
                /* #3783. Present on EVERY response, four states never a bool, for the reason every block
                   above is: 'no row yet', 'one row', 'the counters reset' and 'here is the hour' are four
                   different facts about an absent-or-present number, and the read kills this exists to
                   explain were unattributed precisely because nothing said which. Deltas null unless
                   Observed; the cumulative counters ride beside them for a reader who wants the lifetime
                   figure and so nobody differences the stored columns by hand. */
                checkpointer = new
                {
                    status = checkpointer.Status.ToString(),
                    observed_at = checkpointer.ObservedAt?.ToString("o", CultureInfo.InvariantCulture),
                    previous_at = checkpointer.PreviousAt?.ToString("o", CultureInfo.InvariantCulture),
                    interval_seconds = checkpointer.IntervalSeconds,
                    write_ms = checkpointer.WriteMs,
                    sync_ms = checkpointer.SyncMs,
                    requested = checkpointer.Requested,
                    cumulative_write_ms = checkpointer.CumulativeWriteMs,
                    cumulative_sync_ms = checkpointer.CumulativeSyncMs,
                    cumulative_requested = checkpointer.CumulativeRequested,
                    pressure = checkpointer.IsPressure,
                    sync_bar_ms = DarlingSelfAlertEvaluator.CheckpointSyncBarMs,
                    note = CheckpointerNote(checkpointer),
                },
                /* The job_history row is deliberately NOT in objects[] or daily[]: it carries no bytes, and
                   its columns are the overloaded ones the reader decodes into the block above — rendered
                   raw here, last_run_duration_ms would read as a run's duration. The checkpointer row
                   (#3783) is excluded for the same reason: no bytes, and its columns are cumulative counters
                   the block above differences — rendered raw they would read as an hour's figure. */
                objects = latest
                    .Where(r => r.ObjectKind != StoreSelfMetrics.StoreObjectKind
                             && r.ObjectKind != StoreSelfMetrics.JobHistoryObjectKind
                             && r.ObjectKind != StoreSelfMetrics.CheckpointerObjectKind)
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
                        /* #3582: on the two catch-all rows chunk_count is the RELATION count the sum spans
                           (the column-mapping paragraph on StoreSelfMetrics); surfaced under its own name
                           so a reader is not left inferring it. Null on every other kind. */
                        relation_count = r.ObjectKind == StoreSelfMetrics.OtherObjectKind || r.ObjectKind == StoreSelfMetrics.SystemObjectKind
                            ? r.ChunkCount
                            : null,
                        row_count = r.RowCount,
                        /* #3582: the live catalog state of a continuous aggregate, joined by view name —
                           null on every other kind, and null on an aggregate when the live read did not
                           complete (inventory.aggregate_state says so) or the view has since been dropped.
                           Policy fields are job ids, null where no such policy exists: the three facts a
                           growth investigation into the aggregates otherwise assembles by hand. */
                        compression_enabled = AggregateState(r, aggregateStateByView)?.CompressionEnabled,
                        materialized_only = AggregateState(r, aggregateStateByView)?.MaterializedOnly,
                        source = AggregateState(r, aggregateStateByView)?.SourceName,
                        refresh_policy_job_id = AggregateState(r, aggregateStateByView)?.RefreshJobId,
                        compression_policy_job_id = AggregateState(r, aggregateStateByView)?.CompressionJobId,
                        retention_policy_job_id = AggregateState(r, aggregateStateByView)?.RetentionJobId,
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
                        /* #3783 dimension rows only (null on every other kind, which has no TOAST column to
                           measure): the TOAST file's size, the live bytes inside it where pg_freespacemap is
                           installed, their quotient, and the sentence that says what the numbers mean or
                           why one of them is null. Trailing, so the existing shape is untouched. */
                        toast_bytes = ToastFacts(r)?.ToastBytes,
                        toast_live_bytes = ToastFacts(r)?.ToastLiveBytes,
                        toast_utilisation_pct = ToastFacts(r)?.UtilisationPercent,
                        toast_utilisation_note = ToastFacts(r)?.Note,
                    }),
                daily = daily
                    .Where(p => p.ObjectKind != StoreSelfMetrics.StoreObjectKind
                             && p.ObjectKind != StoreSelfMetrics.JobHistoryObjectKind
                             && p.ObjectKind != StoreSelfMetrics.CheckpointerObjectKind)
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
                            /* #3783: the utilisation as a SERIES, dimension rows only — a file that is 64 %
                               today and was 80 % a month ago is the trend the reclaim decision reads. */
                            toast_bytes = DarlingStoreMetricsReader.ToastFacts.For(p)?.ToastBytes,
                            toast_live_bytes = DarlingStoreMetricsReader.ToastFacts.For(p)?.ToastLiveBytes,
                            toast_utilisation_pct = DarlingStoreMetricsReader.ToastFacts.For(p)?.UtilisationPercent,
                        }),
                    }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_metrics", ex);
        }
    }

    private static DarlingStoreMetricsReader.ContinuousAggregateState? AggregateState(
        DarlingStoreMetricsReader.StoreMetricRow row,
        Dictionary<string, DarlingStoreMetricsReader.ContinuousAggregateState> byView)
        => row.ObjectKind == StoreSelfMetrics.ContinuousAggregateObjectKind
           && byView.TryGetValue(row.ObjectName, out var state)
            ? state
            : null;

    /// <summary>The #3783 TOAST facts for one latest row — null for every kind but <c>dimension</c>, so the
    /// four trailing fields read null there. A thin alias so the projection reads as four fields of one fact.</summary>
    private static DarlingStoreMetricsReader.ToastFacts? ToastFacts(DarlingStoreMetricsReader.StoreMetricRow row)
        => DarlingStoreMetricsReader.ToastFacts.For(row);

    /// <summary>
    /// What the checkpointer block's numbers mean, or why there are none (#3783) — one sentence per status, and
    /// the statuses deliberately do not share one: an absent row, a first row, a reset pair and a measured hour
    /// call for four different readings of a null <c>sync_ms</c>. On an Observed interval the sentence states the
    /// two figures, the bar each is judged against, and — when either is over its line — the two levers by
    /// issue number, in the same words the self-alert uses, so a reader who saw the alert and a reader who saw
    /// this block were told one thing.
    /// </summary>
    internal static string CheckpointerNote(DarlingStoreMetricsReader.CheckpointerReading reading)
    {
        const string source =
            "The store's hourly self-metrics sweep records its own checkpointer's CUMULATIVE counters (pg_stat_checkpointer "
            + "on PostgreSQL 17+, pg_stat_bgwriter before) as the object_kind = 'checkpointer' row; this block is the difference "
            + "between the two newest rows, over the span their stamps measure.";

        switch (reading.Status)
        {
            case DarlingStoreMetricsReader.CheckpointerDeltaStatus.Absent:
                return source + " The series holds no checkpointer row yet (it lands within an hour of a service start on a "
                    + "V137+ store), so there is no interval to state.";

            case DarlingStoreMetricsReader.CheckpointerDeltaStatus.NoPrevious:
                return source + $" One row so far, at {Stamp(reading.ObservedAt)}: the counters are real but there is nothing "
                    + "earlier to subtract, so the first interval arrives with the next hourly sweep.";

            case DarlingStoreMetricsReader.CheckpointerDeltaStatus.Reset:
                return source + $" Between {Stamp(reading.PreviousAt)} and {Stamp(reading.ObservedAt)} at least one counter read "
                    + "LOWER than before — pg_stat_reset_shared('checkpointer') or a server restart without statistics persistence "
                    + "ran between the sweeps — so no delta is stated for this interval rather than a negative or a clamped zero; "
                    + "the next sweep differences cleanly against the post-reset row.";
        }

        var syncMs = reading.SyncMs ?? 0;
        var requested = reading.Requested ?? 0;
        var minutes = ((reading.IntervalSeconds ?? 0) / 60.0).ToString("0.0", CultureInfo.InvariantCulture);
        var measured = $" Over the {minutes} minutes ending {Stamp(reading.ObservedAt)} the checkpointer spent "
            + $"{(syncMs / 1000.0).ToString("0.0", CultureInfo.InvariantCulture)}s in its sync (fsync) phase and "
            + $"{((reading.WriteMs ?? 0) / 1000.0).ToString("0.0", CultureInfo.InvariantCulture)}s in its write phase, and "
            + $"{requested} checkpoint(s) were REQUESTED (WAL-forced by max_wal_size) rather than timed.";

        if (!reading.IsPressure)
        {
            return source + measured + $" Both under the lines the self-alert judges (sync over "
                + $"{(DarlingSelfAlertEvaluator.CheckpointSyncBarMs / 1000.0).ToString("0", CultureInfo.InvariantCulture)}s in an interval, or any requested checkpoint).";
        }

        return source + measured + " That is CHECKPOINTER PRESSURE: "
            + (syncMs > DarlingSelfAlertEvaluator.CheckpointSyncBarMs
                ? "a sync phase past the MCP host's own read deadline is an I/O stall every reader on the store shares — on one "
                  + "production store, three otherwise-unexplained read kills in a day all sat inside 25.2 s and 14.0 s sync phases. "
                : "a requested checkpoint means the store wrote more WAL between checkpoints than max_wal_size allows, so the "
                  + "checkpointer ran early and the next one is closer. ")
            + "The levers are configuration: WAL sizing (#3802 — max_wal_size and checkpoint_completion_target; the managed "
            + "store gained them as the v12 postgresql.conf block) and refresh slicing (#3745 — smaller continuous-aggregate "
            + "refreshes write less WAL per tick). The informational Store Checkpointer Pressure self-alert says the same.";
    }

    /// <summary>
    /// The coverage statement (#3582), in the issue's own words: "inventory covers N% of the database; X in
    /// K un-enumerated relations" — then what the reconciliation found. One sentence per fact, and the
    /// facts that are FINDINGS say so: a residual over the bar, catch-all rows missing from the newest
    /// sweep, object rows the newest sweep never reached, aggregates holding bytes with compression off.
    /// Bytes are stated in GiB to one decimal for a reader, beside the exact fields; counts invariant.
    /// </summary>
    internal static string InventoryNote(
        DarlingStoreMetricsReader.InventoryReconciliation inventory,
        IReadOnlyList<DarlingStoreMetricsReader.StoreMetricRow> latest,
        IReadOnlyList<DarlingStoreMetricsReader.ContinuousAggregateState>? aggregateStates,
        IReadOnlyList<DarlingStoreMetricsReader.UnenumeratedRelation>? largestUnenumerated)
    {
        var sb = new System.Text.StringBuilder();

        sb.Append("The inventory's named rows (hypertables, continuous aggregates, payload dimensions, named tables) ")
          .Append("account for ").Append(Gib(inventory.EnumeratedBytes)).Append(" of the ")
          .Append(Gib(inventory.DatabaseBytes)).Append(" database")
          .Append(inventory.EnumeratedPercent is { } ep ? $" ({Invariant(ep)}%)" : "")
          .Append(" as of the sweep at ").Append(inventory.SweepAt.ToString("o", CultureInfo.InvariantCulture)).Append('.');

        if (inventory.UnenumeratedBytes is { } otherBytes)
        {
            sb.Append(' ').Append(Gib(otherBytes)).Append(" sits in ")
              .Append(Invariant(inventory.UnenumeratedRelationCount ?? 0))
              .Append(" un-enumerated user-schema relation(s) (object_kind other)");
            if (largestUnenumerated is { Count: > 0 })
            {
                sb.Append(", the largest being ")
                  .Append(string.Join(", ", largestUnenumerated.Take(3).Select(r => $"{r.Relation} ({Gib(r.TotalBytes)})")));
            }
            else if (largestUnenumerated is null)
            {
                sb.Append(" — the live census naming them did not complete");
            }

            sb.Append("; ");
        }
        else
        {
            sb.Append(" The 'other' catch-all row is MISSING from this sweep, so the un-enumerated share is unknown; ");
        }

        if (inventory.SystemBytes is { } systemBytes)
        {
            sb.Append(Gib(systemBytes)).Append(" is PostgreSQL catalog and TimescaleDB bookkeeping in ")
              .Append(Invariant(inventory.SystemRelationCount ?? 0)).Append(" relation(s) (object_kind system).");
        }
        else
        {
            sb.Append("the 'system' catch-all row is MISSING from this sweep.");
        }

        if (inventory.Reconciled)
        {
            sb.Append(" RECONCILED: every row together accounts for ")
              .Append(inventory.AttributedPercent is { } ap ? $"{Invariant(ap)}%" : "an unknown share")
              .Append(" of pg_database_size; the residual of ").Append(Invariant(inventory.ResidualBytes))
              .Append(" bytes is the database directory's non-relation files plus whatever moved between the ")
              .Append("sweep's statements, inside the ").Append(Gib(inventory.ToleranceBytes)).Append(" bar.");
        }
        else if (!inventory.CatchAllPresent)
        {
            sb.Append(" NOT RECONCILED: without both catch-all rows the residual cannot be judged — the newest sweep ")
              .Append("did not produce them, which is a sweep failure to look at before reading any coverage figure here.");
        }
        else
        {
            sb.Append(" NOT RECONCILED — a finding: ").Append(Gib(Math.Abs(inventory.ResidualBytes)))
              .Append(inventory.ResidualBytes >= 0
                  ? " of pg_database_size is attributed to NO row of this inventory"
                  : " MORE is attributed to rows than pg_database_size holds")
              .Append(", past the ").Append(Gib(inventory.ToleranceBytes))
              .Append(" bar. Bytes the database holds that nothing here names are exactly the shape this block exists to ")
              .Append("catch; compare a pg_class census against objects[] before trusting any per-object figure.");
        }

        if (inventory.StaleRowCount > 0)
        {
            sb.Append(' ').Append(Invariant(inventory.StaleRowCount))
              .Append(" object row(s) in objects[] are from an OLDER sweep than the store row and are excluded from these ")
              .Append("sums: the newest sweep did not reach them, so the sweep is not completing — its own Warning line says why.");
        }

        var hasTimescaleRows = latest.Any(r =>
            r.ObjectKind == StoreSelfMetrics.HypertableObjectKind || r.ObjectKind == StoreSelfMetrics.ContinuousAggregateObjectKind);
        if (!hasTimescaleRows)
        {
            sb.Append(" No hypertable or continuous-aggregate rows were recorded (a plain-PostgreSQL store, or TimescaleDB ")
              .Append("unavailable to the sweep): on such a store the collector tables are ordinary tables and are counted ")
              .Append("under 'other' rather than by name, so a low enumerated share here is that, not a fault.");
        }

        if (aggregateStates is { Count: > 0 })
        {
            var uncompressed = aggregateStates.Where(s => !s.CompressionEnabled).Select(s => s.ViewName).ToHashSet(StringComparer.Ordinal);
            var uncompressedBytes = latest
                .Where(r => r.ObjectKind == StoreSelfMetrics.ContinuousAggregateObjectKind
                            && r.MetricTime == inventory.SweepAt
                            && uncompressed.Contains(r.ObjectName))
                .Sum(r => r.TotalBytes ?? 0);
            if (uncompressed.Count > 0)
            {
                sb.Append(' ').Append(Invariant(uncompressed.Count)).Append(" of ").Append(Invariant(aggregateStates.Count))
                  .Append(" continuous aggregate(s) have compression DISABLED and hold ").Append(Gib(uncompressedBytes))
                  .Append(" between them (compression_enabled on each continuous_aggregate object; the policy job ids beside it).");
            }
        }
        else if (aggregateStates is null)
        {
            sb.Append(" The live read of each aggregate's compression and policy state did not complete, so those fields are null on the continuous_aggregate objects.");
        }

        return sb.ToString();
    }

    /// <summary>Bytes for a reader, in the largest binary unit that gives a whole-number part: a 235 GiB
    /// aggregate family reads as GiB, a 73 KiB registry table as KiB, and neither as "0.0 GiB". The exact
    /// byte fields sit beside every sentence this decorates; this is for the sentence.</summary>
    internal static string Gib(long bytes)
    {
        var magnitude = Math.Abs(bytes);
        return magnitude >= 1L << 30 ? (bytes / (double)(1L << 30)).ToString("0.0", CultureInfo.InvariantCulture) + " GiB"
            : magnitude >= 1L << 20 ? (bytes / (double)(1L << 20)).ToString("0.0", CultureInfo.InvariantCulture) + " MiB"
            : magnitude >= 1L << 10 ? (bytes / (double)(1L << 10)).ToString("0.0", CultureInfo.InvariantCulture) + " KiB"
            : bytes.ToString(CultureInfo.InvariantCulture) + " bytes";
    }

    private static string Invariant(double value) => value.ToString(CultureInfo.InvariantCulture);

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
        => JobHistoryNote(reading, evidence, DarlingStoreMetricsReader.OwnerJobHistoryEvidence.Absent);

    /// <summary>
    /// The full note (#3175 + #3574 + the managed-mode self-proof): the GUC's half, the connection's own
    /// visibility half, then — wherever this connection is NOT itself an admitted reader — the OWNER's
    /// half from <see cref="OwnerEvidenceNote"/>. Omitted where the view does not exist
    /// (<c>NotApplicable</c>) and where this connection already has <c>All</c> standing, because there the
    /// connection's own count IS the census and a second census would be noise beside it.
    /// </summary>
    internal static string JobHistoryNote(
        DarlingStoreMetricsReader.JobExecutionLoggingReading reading,
        DarlingStoreMetricsReader.JobHistoryEvidence evidence,
        DarlingStoreMetricsReader.OwnerJobHistoryEvidence owner)
    {
        var note = GucNote(reading) + JobHistoryVisibilityNote(reading, evidence);

        if (evidence.Status == DarlingStoreMetricsReader.JobHistoryEvidenceStatus.NotApplicable
            || evidence.Visibility == DarlingStoreMetricsReader.JobHistoryVisibility.All)
        {
            return note;
        }

        return note + OwnerEvidenceNote(reading, owner);
    }

    private static string GucNote(DarlingStoreMetricsReader.JobExecutionLoggingReading reading)
        => reading.Status switch
        {
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.On =>
                "timescaledb.enable_job_execution_logging is ON, so timescaledb_information.job_history holds one row "
                + "per background-job run and a MAXIMUM over it is a census of the runs it covers. It covers runs from "
                + "the moment logging was turned on, never before: nothing was written for earlier runs, so an empty "
                + "window that predates that point is expected and is not evidence about those runs.",

            /* #3175 arms, corrected (#3582 follow-up): an OFF setting does not make the view EMPTY. TimescaleDB
               2.28.1 writes a FAILED run's history row regardless of this GUC (job_stat_history.c gates only
               the success path; measured on a fresh rig with the GUC off, the telemetry job's one failure was
               the view's one row), so the honest reading of an OFF store's job_history is "a census of
               failures, not of runs". The earlier wording — "returns zero rows" — contradicted the visibility
               arm appended right after it, which correctly says any rows an admitted reader sees now are
               failures. */
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off when reading.OffByExplicitOverride =>
                "timescaledb.enable_job_execution_logging is OFF and something SET it off — 'source' is not 'default'. "
                + "timescaledb_information.job_history is recording FAILED runs only: TimescaleDB writes a failure's row "
                + "regardless of this setting and a successful run's only while it is on, so a maximum over it is a "
                + "census of failures and NOT of runs — an empty result means no failure was recorded while the setting "
                + "was off, NOT 'no run exceeded the line', and a non-empty one is failures, not a sign logging is secretly "
                + "on. The service's managed conf "
                + "block does not correct this one, because whatever set it is winning by last-occurrence: either an "
                + "ALTER SYSTEM (which lands in postgresql.auto.conf, read after postgresql.conf and so beating the "
                + "managed block outright, cleared with ALTER SYSTEM RESET) or a hand-added line placed after the "
                + "managed block in postgresql.conf itself. 'source_file' names which file won when the connection is "
                + "privileged enough to see it — that column is superuser-only, so it is normally null here. Until the "
                + "override is removed the only surface is job_stats — the last_run_duration_ms in this response — "
                + "which carries one sample per job, not one row per run.",

            DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off =>
                "timescaledb.enable_job_execution_logging is OFF, so timescaledb_information.job_history is recording "
                + "FAILED runs only: TimescaleDB writes a failure's row regardless of this setting and a successful run's "
                + "only while it is on. A maximum over it is therefore a census of failures and NOT of runs — an empty "
                + "result means no failure was recorded while the setting was off, NOT 'no run exceeded the line', so do "
                + "not read an empty job_history on this store as a clean result, and do not read a non-empty one as "
                + "logging being secretly on. A "
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

    /// <summary>
    /// The OWNER's half of the note (#3574, managed-mode self-proof) — appended only where this connection
    /// is not itself an admitted reader, which in managed mode is every response. It says where the number
    /// came from in so many words: the service's hourly self-metrics sweep, running as the owner role, not
    /// this connection. Then, per what the series holds:
    /// <list type="bullet">
    /// <item><b>Observed</b> — the owner's count, its instant, and the population beside it; rows seen with
    /// the GUC on is <c>recording</c> proven by the owner's measurement; zero rows with runs and the GUC on
    /// is the CONTRADICTION, from the owner's numbers, with the same benign cause and the same way to
    /// settle it; zero rows with nothing run proves nothing; GUC off classifies any rows as failures.</item>
    /// <item><b>Stale</b> — the count and its age, and that it is not read as current: the sweep is hourly
    /// and this row is past <see cref="DarlingStoreMetricsReader.OwnerEvidenceFreshHours"/>, which means
    /// the sweep itself is not landing.</item>
    /// <item><b>Filtered</b> — the sweep's role was not admitted either, named, so the reader knows the
    /// connection string to look at; no count was recorded.</item>
    /// <item><b>Absent</b> — the series holds no owner row yet, so nothing on this block is a measurement.</item>
    /// </list>
    /// </summary>
    internal static string OwnerEvidenceNote(
        DarlingStoreMetricsReader.JobExecutionLoggingReading reading,
        DarlingStoreMetricsReader.OwnerJobHistoryEvidence owner)
    {
        const string source =
            " THE OWNER'S OWN COUNT: the service's hourly self-metrics sweep runs on the owner pool — the role that created "
            + "the jobs and that the view admits — and persists what it sees into the series, so the owner_* fields here "
            + "come from the service's sweep, not from this connection.";

        switch (owner.Status)
        {
            case DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Absent:
                return source
                    + " The series holds no such row yet (it lands within an hour of a service start on a store at this "
                    + "build, and only where TimescaleDB is available), so nothing in this block is a measurement of "
                    + "recording until it does.";

            case DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Filtered:
                return source
                    + $" The sweep's role '{owner.ReaderRole}' was itself NOT admitted to every job's history at "
                    + $"{Stamp(owner.ObservedAt)}, so it recorded no count — the service's own connection string names a role "
                    + "outside the owner memberships, and that, not recording, is the thing to fix"
                    + (owner.JobsRunInWindow is { } filteredRan
                        ? $"; the unfiltered job_stats saw {Invariant(filteredRan)} job(s) start a run in the {Window(owner)}-hour window before it."
                        : ".");
        }

        var rows = Invariant(owner.RowsObserved ?? 0);
        var ran = Invariant(owner.JobsRunInWindow ?? 0);
        var census =
            $" Reading as '{owner.ReaderRole}' at {Stamp(owner.ObservedAt)}, the sweep saw {rows} row(s) with a start in the "
            + $"{Window(owner)} hours before that instant"
            + (owner.NewestRowAt is { } newest ? $" (newest {Stamp(newest)})" : ", none ever")
            + $", while job_stats says {ran} job(s) started a run in the same window.";

        if (owner.Status == DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Stale)
        {
            return source + census
                + $" That reading is {Invariant(owner.AgeHours ?? 0)} hours old — the sweep is hourly, so at least two "
                + "consecutive sweeps have not landed (its own Warning line says why) — and it describes a window that no "
                + "longer speaks for now: it is shown, not read as current evidence, and no contradiction is judged from it.";
        }

        if (owner.ContradictsRecording(reading.Recording))
        {
            return source + census
                + " CONTRADICTION (from the owner's numbers): the GUC says recording, the owner sees every job's history, "
                + "jobs started runs inside the window, and the view showed the owner NONE of them — do not read this zero "
                + "as quiet. The one benign cause is logging switched on AFTER the last of those starts (the managed v11 "
                + "heal lands on a service-owned server start and writes nothing for earlier runs), which the next hourly "
                + "sweep settles: re-read after it. A zero that persists while owner_jobs_run_in_window climbs means the "
                + "instrument is not writing what the GUC says it is, and that is a finding, not a quiet hour.";
        }

        if (reading.Recording && owner.RowsObserved is > 0)
        {
            return source + census
                + " 'recording' is therefore a measurement on this store after all — the service's, taken hourly as the "
                + "owner — not a GUC echo.";
        }

        if (reading.Recording)
        {
            return source + census
                + " No job started a run in that window, so there was nothing to record and the owner's zero proves "
                + "nothing either way; it is not a clean result, it is an absence of information.";
        }

        if (reading.Status == DarlingStoreMetricsReader.JobExecutionLoggingStatus.Unreadable)
        {
            return source + census
                + " Whether logging is on could not be read, so the owner's rows are not classified: TimescaleDB writes a "
                + "FAILED run's row regardless of the setting and a successful run's only while it is on.";
        }

        return source + census
            + " With the setting off, any rows the owner saw are FAILED runs, which TimescaleDB writes regardless of it — "
            + "successes need it on.";
    }

    private static string Stamp(DateTime? at) =>
        at is { } value ? value.ToString("o", CultureInfo.InvariantCulture) : "(unknown instant)";

    private static string Window(DarlingStoreMetricsReader.OwnerJobHistoryEvidence owner) =>
        owner.WindowHours is { } hours
            ? hours.ToString("0.##", CultureInfo.InvariantCulture)
            : Invariant(DarlingStoreMetricsReader.JobHistoryEvidenceWindowHours);

    private static string Invariant(long value) => value.ToString(CultureInfo.InvariantCulture);
}
