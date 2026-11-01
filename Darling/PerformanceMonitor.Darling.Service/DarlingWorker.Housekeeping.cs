/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using ServerClock = PerformanceMonitor.Analysis.Baselines.ServerClock;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingWorker
{
    /// <summary>
    /// Gathers the store disk-pressure sample and hands it to the Stage 4 evaluator (fleet-level). The store
    /// size is context only, read from the hourly self-metrics series rather than measured here (#3199); the
    /// store volume's free/total space is
    /// read for the volume that holds the MANAGED data directory (<see cref="ReadStoreVolumeSpace"/>) — the
    /// bundled store this service owns and must protect. In bring-your-own mode the store can be a remote
    /// Postgres whose disk the service cannot see, so free/total stay null and the evaluator no-ops (never a
    /// false alarm — the operator owns their own PostgreSQL's disk monitoring, consistent with the BYO
    /// posture elsewhere). Failure-isolated: a bad sample logs at Debug and skips this tick, never breaking
    /// the loop.
    /// </summary>
    private async Task EvaluateStoreDiskPressureAsync(DarlingConfig config, CancellationToken cancellationToken)
    {
        var (storeSizeBytes, storeSizeEstimated) = await ReadStoreSizeBytesAsync(cancellationToken);

        long? freeBytes = null;
        long? totalBytes = null;
        if (config.Postgres.Managed && OperatingSystem.IsWindows())
        {
            try
            {
                var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(config.Postgres);
                (freeBytes, totalBytes) = ReadStoreVolumeSpace(dataDirectory);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* Best-effort: an unreadable drive just means no disk signal this tick. */
                /* NOT counted by #3013's swallowed-read counter: a local filesystem read, not a store read. #3013's
                   mechanism is store latency crossing the alert pass's deadline, which has no bearing on a volume read. */
                _logger.LogDebug("Store disk-pressure check: could not read the store volume free space: {Message}", ex.Message);
            }
        }

        /* EvaluateDiskPressureAsync (not ApplyDiskPressureAsync) so a throwing seam — e.g. a mute rule's
           Matches() in the pre-deliver mute check — is isolated inside the evaluator, exactly like the
           per-server EvaluateStoreAlertsAsync. This sweep-loop body has no catch-all of its own, so an
           un-isolated throw here would stop collection for the whole fleet. */
        await _selfAlerts!.EvaluateDiskPressureAsync(freeBytes, totalBytes, storeSizeBytes, cancellationToken, storeSizeEstimated);
    }

    /// <summary>
    /// The free and total bytes of the volume that holds the store's data directory, for the disk-pressure
    /// alert. Asked for the data directory's own volume, not for its drive letter: a data directory on a volume
    /// mounted at a folder is on a different volume from its drive root, and the alert exists to protect the
    /// store's own disk. The free figure is what is available to the service account, the figure the store
    /// upgrade and the WAL sizing use, so a quota on the account counts; the drive-letter read paired the
    /// volume's total free space, which a quota does not reduce, with a total that a quota does. A read that
    /// fails throws, and <see cref="EvaluateStoreDiskPressureAsync"/> turns that into no signal this tick.
    /// <paramref name="readVolumeSpace"/> replaces the read, so a test can say what the volume holds.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static (long FreeBytes, long TotalBytes) ReadStoreVolumeSpace(
        string dataDirectory, Func<string, (long AvailableFreeBytes, long TotalBytes)>? readVolumeSpace = null)
        => (readVolumeSpace ?? DarlingStoreUpgrade.ReadVolumeSpace)(dataDirectory);

    /// <summary>
    /// #4215: builds the <see cref="DarlingSelfAlertEvaluator.StoreSettingsReport"/> and hands it to
    /// <see cref="DarlingSelfAlertEvaluator.EvaluateStoreSettingsAsync"/>, mirroring
    /// <see cref="EvaluateStoreDiskPressureAsync"/>'s split between "gather the facts here" and "judge them in
    /// the evaluator". <paramref name="managedConfWriteResult"/> and <paramref name="managedUsedLastGoodConf"/>
    /// are this start's own in-process facts, carried down unchanged from <c>ExecuteAsync</c>; the rejected-value
    /// list is the one genuine store read, isolated in <see cref="ReadRejectedManagedConfSettingNamesAsync"/> so
    /// a throw there degrades to <c>null</c> (unknown) rather than stopping the fleet loop:
    /// unlike <see cref="UsedLastGoodConf"/>/<see cref="HandEdited"/>, a rejected-value row is one
    /// of the three conditions the alert fires on, so "empty" and "unknown" cannot share a representation.
    /// </summary>
    private async Task EvaluateStoreSettingsAsync(
        DarlingConfig config, ManagedConfWriteResult? managedConfWriteResult, bool managedUsedLastGoodConf,
        ManagedConfMigrationOutcome? managedConfVerification,
        CancellationToken cancellationToken)
    {
        var isManagedStore = config.Postgres.Managed && OperatingSystem.IsWindows();
        IReadOnlyList<string>? rejectedSettingNames = isManagedStore
            ? await ReadRejectedManagedConfSettingNamesAsync(cancellationToken)
            : [];

        var report = new DarlingSelfAlertEvaluator.StoreSettingsReport(
            isManagedStore, managedUsedLastGoodConf, managedConfWriteResult?.HandEdited ?? false, rejectedSettingNames,
            managedConfVerification);

        /* EvaluateStoreSettingsAsync (not ApplyStoreSettingsAsync) so a throwing seam — the shared mute check —
           is isolated inside the evaluator, exactly like the disk-pressure call just above. */
        await _selfAlerts!.EvaluateStoreSettingsAsync(report, cancellationToken);
    }

    /// <summary>Every setting name <c>collect.managed_conf_verdicts</c> (V146) currently holds as
    /// <see cref="HostSettingVerdict.RejectedValue"/> — the store-settings self-alert's one real store read,
    /// and one of the three conditions the alert fires on. Returns <c>null</c> (unknown), not an empty list,
    /// on a failed read: a <c>RejectedValue</c> row is judgeable evidence, not context, so this read is counted
    /// on #3013's swallowed-read counter like the other alert reads — <see cref="ReadStoreSizeBytesAsync"/>'s
    /// Debug/empty posture is for a read that is ONLY context and does not apply here.</summary>
    private async Task<IReadOnlyList<string>?> ReadRejectedManagedConfSettingNamesAsync(CancellationToken cancellationToken)
    {
        var readClock = Stopwatch.StartNew();
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            readClock.Restart();
            var stored = await DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync(connection, cancellationToken);
            return stored
                .Where(static row => row.Verdict == HostSettingVerdict.RejectedValue)
                .Select(static row => row.SettingName)
                .ToArray();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Store settings self-alert: could not read stored managed-conf verdicts after {ElapsedMs} ms — "
                + "the rejected-settings condition stays unresolved this tick: {Message}",
                readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(null, "store settings self-alert (managed-conf verdicts)", readClock.ElapsedMilliseconds);
            return null;
        }
    }

    /// <summary>
    /// #3304: the fleet-level custom-alert-rule integrity check. Asks the <see cref="CustomAlertEvaluator"/> to
    /// re-parse the enabled rules against the live catalog and flag broken / never-firing ones (the current
    /// monitored (server_id, storage name) set feeds the 0-server-scope case, including #3350 tag scope), then hands the report to
    /// <see cref="DarlingSelfAlertEvaluator.EvaluateCustomRuleHealthAsync"/>, which raises ONE aggregated
    /// self-health alert. Both halves are failure-isolated internally (BuildHealthReportAsync returns null on a
    /// load error and this method then HOLDS the standing alert rather than resolving it; the Evaluate* wrapper
    /// contains a throwing mute seam), matching the disk-pressure posture — this sweep-loop body has no catch-all
    /// of its own. Only called when both evaluators are non-null (guarded at the call site).
    /// </summary>
    private async Task EvaluateCustomAlertRuleHealthAsync(List<ServerLoopState> servers, CancellationToken cancellationToken)
    {
        // #3350: (server_id, storage name) pairs so the tag-scope 0-server case can be resolved by id (the tag
        // membership is keyed on server_id) while the 'servers' scope still matches on the storage name.
        var monitoredServers = servers
            .Where(s => !s.Retired)
            .Select(s => (s.Config.ServerId, s.Config.StorageName))
            .ToList();

        var report = await _customAlertEvaluator!.BuildHealthReportAsync(monitoredServers, cancellationToken);

        // Null = the rule load failed this tick; HOLD the standing alert (never resolve on uncertainty).
        if (report is not null)
        {
            await _selfAlerts!.EvaluateCustomRuleHealthAsync(report, cancellationToken);
        }
    }

    /// <summary>
    /// #5654: reads the count of external alert rows the drain gave up on in the last
    /// <see cref="DarlingSelfAlertEvaluator.ExternalAlertsWindow"/> and hands it to
    /// <see cref="DarlingSelfAlertEvaluator.EvaluateExternalAlertsAsync"/>. A failed read is counted on #3013's swallowed-read
    /// counter and sends no report, so the standing alert is held, never resolved on uncertainty.
    /// </summary>
    private async Task EvaluateExternalAlertsAsync(ExternalAlertStore store, CancellationToken cancellationToken)
    {
        IReadOnlyList<ExternalAlertUndeliveredCount> counts;
        var readClock = Stopwatch.StartNew();
        try
        {
            counts = await store.CountUndeliveredAsync(DarlingSelfAlertEvaluator.ExternalAlertsWindow, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "External alert self-alert: could not count undelivered rows after {ElapsedMs} ms - the standing alert is held this tick: {Message}",
                readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(null, "external alert self-alert (undelivered count)", readClock.ElapsedMilliseconds);
            return;
        }

        await _selfAlerts!.EvaluateExternalAlertsAsync(new DarlingSelfAlertEvaluator.ExternalAlertsReport(counts), cancellationToken);
    }

    /// <summary>
    /// #3305: the fleet-level custom-alert-state reconcile. Force-resolves and cleans the persisted state of
    /// DISABLED rules (whose open incidents the evaluator's enabled-only sweep would otherwise orphan forever)
    /// and of servers that have left a rule's scope. Deleted rules are handled on the delete path instead
    /// (their state cascades away). Hands the evaluator the current fleet — server id → (storage name for the
    /// scope check, display name for the recovery row); a server absent from it has left monitoring. Runs on
    /// the fleet-global health cadence; the evaluator's method is failure-isolated per row so this never stops
    /// the sweep. Only called when the custom-alert evaluator is non-null (guarded at the call site).
    /// </summary>
    private async Task ReconcileCustomAlertStateAsync(List<ServerLoopState> servers, CancellationToken cancellationToken)
    {
        var monitored = servers
            .Where(s => !s.Retired)
            .ToDictionary(s => s.Config.ServerId, s => (s.Config.StorageName, s.Config.DisplayName));

        await _customAlertEvaluator!.ReconcileStateAsync(monitored, cancellationToken);
    }

    /// <summary>
    /// The store database's on-disk size in bytes — context for the disk-pressure alert text, read from the
    /// whole-store row the hourly self-metrics sweep records
    /// (<see cref="StoreSelfMetrics.LatestStoreSizeWithTimeSql"/>, the same predicate and ordering as
    /// <see cref="StoreSelfMetrics.LatestStoreSizeSql"/>). #6245: the same row's <c>size_estimated</c> comes back with
    /// the size, in the one statement, so the alert text can say when the figure is the hourly catalog estimate.
    /// Failure-isolated to null (Debug) so a transient store hiccup never breaks the disk-pressure check.
    ///
    /// <para><b>Why this is not <c>pg_database_size_stats</c> (#3199).</b> This method is one of the ten commands
    /// awaited inline on the collection loop's serial thread, under
    /// <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/> — and running <c>pg_database_size_stats</c> here
    /// made it the only one of the ten whose cost scaled with the store, against a bound floored on 6.2 ms
    /// measured on a 4.05 GB fixture. On a 225 GiB production store the same call measured
    /// <b>3,177 ms</b> — 64% of the 5 s bound, 1.57x headroom where the derivation claimed ~806x.</para>
    ///
    /// <para><b>The cancelled statements are the visible 2%; the cost is the other 98%.</b> Thirteen
    /// samples on that store spanned 2,090-3,745 ms (mean ~2.5 s) and NONE crossed 5 s, while cancels for
    /// this statement ran 0-9 a day over six days — mean 5.8 against a nominal ~288 iterations, so 2.0%.
    /// The remaining 98% cost ~2.5 s each and leave no trace at all: under the deadline so no cancel, not a
    /// collector run so no <c>collection_log</c> row. That is ~11.9 minutes a day of this loop's wall time
    /// spent computing a number the store already holds, and it is the finding — not the eight log lines
    /// that started it, and it does not depend on knowing what ends a cluster, the question #3199's own
    /// correction left open.</para>
    ///
    /// <para><b>What that stall does and does not displace, because the difference matters.</b> This check
    /// runs AFTER the per-server launches fan out in the same tick, and
    /// <see cref="SweepWatchdogSeconds"/> clocks per-server BODIES from their own launch rather than
    /// clocking this loop — so 2.5 s here consumes no watchdog budget and delays no already-launched body.
    /// What it does is stretch one collection tick in twenty from <see cref="s_sweepInterval"/> to ~17.5 s,
    /// pushing that tick's remaining maintenance and the NEXT tick's launches out by that much on a
    /// single-threaded loop whose delivered cadence already runs behind its schedule at fleet scale. The
    /// bound genuinely at risk is the command's own: at 42-75% of a 5 s <c>CommandTimeout</c> floored on
    /// millisecond reads, anything competing takes the rest.</para>
    ///
    /// <para><b>The walk is relocated, not eliminated.</b> The row this reads is written by
    /// <see cref="StoreSelfMetrics.StoreInsertSql"/>, which runs <c>pg_database_size_stats</c> itself — so the
    /// store-wide walk still happens, hourly, under
    /// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/> (300 s, ~120x the mean measured cost, sized by
    /// #2317 against a production store rather than a fixture). That sweep is awaited on this same thread,
    /// so what changes is the frequency and the budget, not the isolation: ~312 executions a day become 24,
    /// and the ~11.9 min/day leaves the 5 s regime specifically. The ~31,000x is the single read's latency
    /// on this path (3,177 ms against 0.101 ms cold, same store) and is never the change's overall
    /// effect.</para>
    /// </summary>
    internal async Task<(long? Bytes, bool Estimated)> ReadStoreSizeBytesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(StoreSelfMetrics.LatestStoreSizeWithTimeSql, connection) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0))
            {
                return (null, false);
            }

            /* size_estimated is NULL on a row from before the column: that figure is not marked, so it reads as exact. */
            return (reader.GetInt64(0), !reader.IsDBNull(2) && reader.GetBoolean(2));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* NOT counted by #3013's swallowed-read counter: this read is CONTEXT for the alert text, not the
               evidence the alert is judged on - that is freeBytes/totalBytes above. Losing it costs the message a
               number; it does not make the condition unjudgeable. */
            _logger.LogDebug("Store disk-pressure check: could not read the recorded store size: {Message}", ex.Message);
            return (null, false);
        }
    }

    /// <summary>
    /// The #1581 policy-job self-heal check (fleet-level, hourly, Timescale-only): read every stuck POLICY
    /// job (<see cref="TimescaleSupport.ReadStuckPolicyJobsAsync(NpgsqlConnection, DateTime, ILogger, CancellationToken)"/>)
    /// and hand them to the self-alert evaluator's re-arm-once/escalate machine, wired to
    /// <see cref="TimescaleSupport.TryRearmJobAsync"/> on the SAME open connection. One stuck job whose
    /// <c>next_start</c> went <c>-infinity</c> silently halts a whole tier of the store — the field incident — so
    /// this makes it visible AND self-heals it. Failure-isolated at the worker level too (the connection open is
    /// OUTSIDE the evaluator's own isolation): a store hiccup logs and skips this check, never aborting the sweep —
    /// mirroring the purge / disk-check isolation.
    ///
    /// <para><b>The method KEEPS its #1581 name while the check it performs is no longer compression-only
    /// (#3816), and that is deliberate.</b> Its name and its POSITION on this tick are pinned as source text
    /// by three sibling issues' tests — <c>RefreshCeilingStalenessTests</c> holds the signature,
    /// <c>RetentionReevaluationTests</c> holds that #3812's retention re-evaluation is awaited AFTER it, and
    /// <c>TimescaleAvailabilityReprobeTests</c> holds that #3815's re-probe runs BEFORE it and outside the
    /// availability gate. Those three facts are the tick's ordering contract; renaming the method to match
    /// this issue's vocabulary would rewrite all three pins to say the same thing about a different
    /// identifier, and the ordering is what they are for.</para>
    ///
    /// <para>What the widening means here: compression, continuous-aggregate refresh and retention policies
    /// all reach the same machine on the same pass, banded per family by the evaluator (different metric
    /// name, severity and text each) — and every pass now writes one unconditional summary line, so a tick
    /// that found nothing can still be shown to have run.</para>
    ///
    /// <para><b>The stuck-job read may hold this method for <see cref="TimescaleSupport.StuckPolicyJobConfirmDelay"/>
    /// (#3575)</b>, and only on a pass where a job's <c>-infinity</c> arm tripped: the read re-executes its query
    /// after that delay and reports the job only if the arm still trips, because TimescaleDB's view assembles
    /// <c>next_start</c> and <c>job_status</c> from independent sources and reads the dead-job shape for a few
    /// milliseconds at either edge of every healthy run. This method is awaited on the serial sweep loop, so
    /// that five seconds is a once-an-hour worst case paid only when there was something to confirm; the
    /// budgeting argument is on the constant. The evaluator downstream receives a list that has already been
    /// confirmed and does not second-guess it.</para>
    /// </summary>
    private async Task EvaluateCompressionJobHealthAsync(CancellationToken cancellationToken)
    {
        var readClock = Stopwatch.StartNew();
        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(cancellationToken);
            readClock.Restart();

            /* #1778: report what compression is DOING before deciding whether anything is stuck. The field
               could see hours-long compressions only in hindsight, by their effect on disk; this puts a
               running compression, its elapsed time, and any eligible-chunk backlog in the log while it is
               still happening. Read on the same connection and the same hourly cadence — the check that
               already exists for this exact subsystem, rather than a second timer for one log line. */
            TimescaleSupport.LogCompressionActivity(
                await TimescaleSupport.ReadCompressionActivityAsync(connection, _logger, cancellationToken),
                DateTime.UtcNow,
                _logger);
            readClock.Restart();

            /* #3044: the heaviest hourly refresh's runtime against the SLOT it has to fit inside, which is a
               different bound from #2136's below and the one the compression phase grid rests on. The build-time
               assertion on HeaviestHourlyRefreshObservedCeilingSeconds bounds a CONSTANT; the thing it bounds is
               a runtime that moves with data volume, so it fires when someone edits the constant and never when
               reality changes underneath it. This is that bound applied to the live figure, on the sweep that
               already reads the job catalog — no new collector, no new timer, and the per-run history is already
               in collect.store_metrics. A log line rather than a band or an alert: see
               TimescaleSupport.LogHeaviestRefreshSlotHeadroom for why, including why #2136's knob happening to
               equal one slot today is not a substitute. */
            var heaviestRefresh = await TimescaleSupport.ReadHeaviestRefreshRuntimeAsync(
                connection, _logger, cancellationToken);
            TimescaleSupport.LogHeaviestRefreshSlotHeadroom(heaviestRefresh, _logger);

            /* #3182: whether either ceiling CONSTANT has been overtaken, which is a different finding from
               the band above and is levelled and rate-limited separately. The band answers "does this run
               fit in the window the grid gives it"; this answers "is the number the grid was DERIVED from
               still a maximum". The defect it exists for is that the answer to the second can be NO while
               the first says InsideSlot and logs at Debug — a recorded ceiling was overtaken on roughly half
               the runs of its own job and nothing anywhere said so. Called BESIDE the band watch rather than
               inside its switch precisely so it is reachable from every band. */
            if (heaviestRefresh is not null)
            {
                TimescaleSupport.LogRefreshCeilingStaleness(
                    TimescaleSupport.HeaviestRefreshCeilingConstantName,
                    TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds,
                    heaviestRefresh.View,
                    heaviestRefresh.LastRunSeconds,
                    _refreshCeilingStaleness,
                    _logger);
            }

            /* One restart per I/O boundary, not per statement: the two log calls above are synchronous, so a
               restart between them would hand the catch below a ~0 ms elapsed for the READ that actually
               faulted. Raised by review. */
            readClock.Restart();

            /* And the same question for the OTHER twelve hourly refreshes, which had no live reading keyed
               to a view anywhere in the product before #3182. Their ceiling is what
               CompressionPhaseGuardMinutes' DECLARED width is checked against (#3188), so a light refresh
               running past that width leaves a compression policy able to start while the refresh still
               holds AccessShareLock — #3012's convoy. It used to be the width's INPUT, rounded up to a whole
               minute, and the difference is which way the failure goes: a longer light refresh widened the
               width silently, and now it goes red. One constant covers all twelve, so the rate limit is
               keyed on the constant and the loop cannot make it twelve times looser than it reads. */
            foreach (var lightRefresh in await TimescaleSupport.ReadOtherHourlyRefreshRuntimesAsync(
                connection, _logger, cancellationToken))
            {
                TimescaleSupport.LogRefreshCeilingStaleness(
                    TimescaleSupport.OtherRefreshCeilingConstantName,
                    TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds,
                    lightRefresh.View,
                    lightRefresh.LastRunSeconds,
                    _refreshCeilingStaleness,
                    _logger);

                /* #3185: and whether the run outran the gap the band gave its own COST CLASS, which is a
                   third fact with a third remedy. The band separates the members whose runs are long enough
                   for CPU and I/O contention to matter and leaves the rest one minute apart, and which
                   members those are is derived from each view's GROUP BY rather than listed — so the one
                   thing that can go silently wrong is a view being slow for a reason its group key does not
                   show. That is not visible to any build-time rule, and it is not visible to the ceiling
                   watch above either: a bounded member at 100 s outran its 60 s step while sitting a long
                   way under the 226.8 s ceiling. Same read, same reading, same rate limiter — keyed on the
                   constant the reading falsified, so the class questions do not suppress each other. */
                TimescaleSupport.LogLightRefreshSpacingBreach(
                    lightRefresh.View,
                    lightRefresh.LastRunSeconds,
                    _refreshCeilingStaleness,
                    _logger);
            }

            readClock.Restart();

            /* #3816: one read, every policy family this product owns — compression (unscoped, as since
               #1581), plus collect-scoped continuous-aggregate refresh and retention. Same statement, same
               confirm-read, same hourly cadence and the SAME connection: the widening is in the WHERE and in
               what the evaluator does with the family, not in what this tick costs. The reading carries the
               stuck list AND the census of every job it looked at, because the unconditional summary line
               and the total_failures arm are both about the jobs that are fine. */
            var policyJobs = await TimescaleSupport.ReadStuckPolicyJobsAsync(
                connection, DateTime.UtcNow, _logger, cancellationToken);
            readClock.Restart();
            await _selfAlerts!.EvaluatePolicyJobsAsync(
                policyJobs,
                jobId => TimescaleSupport.TryRearmJobAsync(connection, jobId, _logger, cancellationToken),
                cancellationToken);
            readClock.Restart();

            /* #2136: the Store Job Over Cadence check rides the same connection and hourly cadence — a
               background job whose last successful run reached the warning share of its own schedule
               interval is the store outgrowing its job schedule, the number an onboarding wave moves
               first. Same isolation posture: the evaluator wraps itself, and this whole method's catch
               is the backstop. */
            var cadenceReadings = await TimescaleSupport.ReadJobCadenceReadingsAsync(
                connection, _logger, cancellationToken);
            readClock.Restart();

            /* #2813: the Retention Held check rides the same connection and hourly cadence. A retention
               policy the #1680/#1877 coverage gate has paused reports total_failures = 0 and a plausible
               last run — it is not failing, it is stopped — so it is invisible to every stored metric and
               went unnoticed on the production store for 16 days while that tier grew to 4.5x its horizon.
               Judged on the CONSEQUENCE (held AND the tier past its own horizon), never on the paused flag
               alone, which is the normal state of every freshly created policy. Same isolation posture. */
            var retentionHolds = await TimescaleSupport.ReadRetentionHoldReadingsAsync(
                connection, _logger, cancellationToken);
            readClock.Restart();

            /* #4299: the three raw jobs are excluded from JobCadenceReadSql (they are never
               timescaledb_information.jobs.scheduled, so job_stats carries no meaningful last_run_duration
               for them) and take their #2136 cadence from the SERVICE'S OWN trigger instead — the #2136
               cadence check takes its cadence from the trigger. The duration is the last RECORDED
               "ran" outcome's elapsed_ms (TimescaleSupport.RawLastPurgeRecord, read below via the SAME
               rawPurgeOverHorizon reading the over-horizon check judges — reused, not re-read); the interval
               is the worker's own hourly Periodic pass, s_compressionCheckInterval, because that pass is the
               ONLY caller of the raw trigger (TriggerRawPurgeCoreAsync). No record, or a last outcome other
               than "ran", yields no cadence reading — being over horizon with no successful run is the #4299
               alert's job, not cadence's; a raw job that has never run at all must not read as running at
               0% of its interval, and a job whose last recorded attempt failed must not be graded on a
               duration that never happened. Merged into the list #2136's evaluator already iterates rather
               than a parallel check, so the raw jobs share JobOverCadence's exact firing, cooldown and
               resolution machinery instead of a second copy of it. */
            var rawPurgeOverHorizon = await TimescaleSupport.ReadRawPurgeOverHorizonReadingsAsync(
                connection, retentionHolds, _logger, cancellationToken);
            readClock.Restart();

            var rawCadenceReadings = RawCadenceReadings(rawPurgeOverHorizon, s_compressionCheckInterval);

            await _selfAlerts!.EvaluateStoreJobCadenceAsync(
                cadenceReadings.Concat(rawCadenceReadings).ToList(), cancellationToken);
            readClock.Restart();

            await _selfAlerts!.EvaluateRetentionHoldsAsync(retentionHolds, cancellationToken);
            readClock.Restart();

            /* #4299: the raw purge over-horizon check rides the SAME connection, hourly cadence and
               retentionHolds/rawPurgeOverHorizon readings as the checks just above — reused, not re-read, so
               this tick's raw rows are the exact ones EvaluateRetentionHoldsAsync and the cadence merge just
               judged. */
            await _selfAlerts!.EvaluateRawPurgeOverHorizonAsync(rawPurgeOverHorizon, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* WHAT THIS SITE'S ELAPSED CAN AND CANNOT CLAIM, stated because the shared finding sentence
               frames every entry against the alert pass's own command deadline and this one does not fit
               that frame.

               Each TimescaleSupport.Read*Async above catches `Exception ex when (ex is not
               OperationCanceledException)` internally, logs at Debug and returns an empty result, and so
               do the _selfAlerts Evaluate* wrappers. So a timeout on one of the reads this entry is NAMED
               for never reaches here — it is swallowed one level down. What reaches here is the connection
               open, a cancellation, or a genuine bug. And those reads run on
               TimescaleSupport.JobCatalogReadTimeoutSeconds (30 s), not
               DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds (10 s), so even when one did surface
               the 10 s bound would be the wrong thing to compare it to.

               The measurement stays, because a figure for the operation that actually faulted is still
               worth having and the clock boundaries above make it one operation's. What does not stay is
               any claim that it discriminates a client cutoff from a store fault at this site. */
            _logger.LogError("Compression-job health check failed after {ElapsedMs} ms: {Message}", readClock.ElapsedMilliseconds, ex.Message);
            _readFailures.RecordReadFailure(
                null, "store background-job health reads (compression, job cadence, retention holds)", readClock.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// #4299: the raw-cadence merge pulled out of <see cref="EvaluateCompressionJobHealthAsync"/> as its
    /// own seam so it pins directly, without a connection. Behaviour is unchanged from the inline LINQ it
    /// replaces: only readings whose last recorded purge outcome is <c>"ran"</c> AND carries an elapsed-ms
    /// value become a cadence reading, at the given interval.
    /// </summary>
    internal static IReadOnlyList<StoreJobCadenceReading> RawCadenceReadings(
        IReadOnlyList<RawPurgeOverHorizonReading> readings, TimeSpan interval) =>
        readings
            .Where(reading => reading.LastPurge is { Outcome: "ran", ElapsedMs: long })
            .Select(reading => new StoreJobCadenceReading(
                reading.JobId,
                $"policy_retention {reading.HypertableName}",
                reading.LastPurge!.ElapsedMs,
                (long)interval.TotalMilliseconds))
            .ToList();

    /// <summary>
    /// The #3815 TimescaleDB availability re-probe (fleet-level, on the hourly store-maintenance tick, and
    /// only while <see cref="_timescaleAvailable"/> reads <c>false</c>): re-run
    /// <see cref="TimescaleSupport.TryEnableAsync"/> on a connection of its own and flip the latch when the
    /// store answers that the extension is there after all.
    ///
    /// <para><b>Why the call site is ABOVE the flag's gate and not inside it.</b> The latch decides whether
    /// the store background-job health check runs at all, and that check is the only surface that reports a
    /// dead compression job (#1581), a job running past its cadence (#2136) or a held retention policy
    /// (#2813). Put the correction inside the block the latch gates and it is unreachable in precisely the
    /// state it exists for — the false value suppresses its own repair, and the service is left with the
    /// #1581 field incident's backstop switched off and no way to say so. So the tick's guard is the due time
    /// alone, this runs first, and the compression read and the retention pass sit one level in behind the
    /// flag this may have just flipped.</para>
    ///
    /// <para><b>A DEDICATED connection, and nothing touches it after a <c>false</c> (#1922).</b>
    /// <c>CREATE EXTENSION IF NOT EXISTS timescaledb</c> TERMINATES the backend when the library is on disk
    /// but missing from <c>shared_preload_libraries</c>, and <see cref="TimescaleSupport.TryEnableAsync"/>
    /// turns that into <c>false</c> like any other failure — so its contract reads as "carry on in
    /// plain-PostgreSQL mode" while the connection it was handed is dead. This method is written to the same
    /// rule the start-path block is: a connection of its own out of the worker's pool, the result checked
    /// before anything else uses it, and the <c>false</c> arm returning straight to the <c>await using</c>
    /// that disposes it. Adding a second statement on <c>connection</c> below that check reintroduces the
    /// masking, on an hourly cadence rather than once a start.</para>
    ///
    /// <para><b>What the flip does, and what it deliberately leaves alone.</b> It restores the READS: the
    /// job health check, the <c>drop_chunks</c> branch of the daily purge, the self-metrics sweep's
    /// per-hypertable rows and the two provider delegates all consult the field at call time, so each picks
    /// the flip up on its next pass with no further wiring. It does NOT re-run the setup sequence — hypertable
    /// conversion, compression policies, continuous aggregates, retention policies. Those stay on the start
    /// path, the recovery line says so, and putting them on a cadence is #3817's subject, whose whole content
    /// is the ordering and the fire-and-forget backfill that sequence carries. Restoring the checks is where
    /// the damage in #3815 is: a store that was converted by an earlier start and lost only the flag is fully
    /// healed by the flip alone, and a store that was never converted at least regains the surface that can
    /// report it.</para>
    ///
    /// <para><b>The logging is split by TRANSITION, which is why the logger handed down is null.</b>
    /// <see cref="TimescaleSupport.TryEnableAsync"/> writes its own Information line on every outcome, which
    /// is right once at a start and is a line an hour forever on a store that is genuinely plain PostgreSQL —
    /// a fully supported configuration. So this passes no logger and says it itself: the recovery at
    /// Information, because a service that has been silently degraded since its start must announce that it
    /// no longer is; the unchanged pass at Debug, because "still plain PostgreSQL" is the same sentence the
    /// start already wrote. Failure-isolated with the three outcomes the retention pass uses — shutdown
    /// quiet, the budget its own WARNING, anything else a WARNING naming the message — and no rethrow, so
    /// the sweep loop never sees this pass fail.</para>
    /// </summary>
    private async Task ReprobeTimescaleAvailabilityAsync(CancellationToken cancellationToken)
    {
        var probeClock = Stopwatch.StartNew();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_timescaleReprobeBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);
            var available = await TimescaleSupport.TryEnableAsync(connection, null, budget.Token);
            if (!available)
            {
                _logger.LogDebug(
                    "TimescaleDB re-probe after {ElapsedMs} ms: still unavailable, so the store stays in plain-PostgreSQL mode and the next hourly store-maintenance tick probes again.",
                    probeClock.ElapsedMilliseconds);
                return;
            }

            _timescaleAvailable = true;
            _logger.LogInformation(
                "TimescaleDB is available after all - the store reports the extension present while this service has been running in plain-PostgreSQL mode, so the availability latch flips back on this tick WITHOUT a restart (#3815). The store background-job health check that latch gates - the compression-job self-heal (#1581), Store Job Over Cadence (#2136), Retention Held (#2813) and Raw Purge Over Horizon (#4299) - has been skipped on every tick since the start-path detection came back false, and runs again immediately after this line. Hypertable conversion, compression policies, continuous aggregates and the composer's covering indexes are re-converged on this same tick by the store-object convergence pass (#3817), which runs a few statements after this line and writes its own 'Store object convergence:' summary — so anything the start path left unbuilt while the latch was false is rebuilt now, not at the next restart. This pass took {ElapsedMs} ms, connection acquisition included.",
                probeClock.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown - quiet and expected. The next start probes this from scratch. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "TimescaleDB re-probe exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short - the availability latch keeps the value it had and the next hourly tick probes again. A store that cannot answer CREATE EXTENSION IF NOT EXISTS and a one-row pg_extension read inside that budget is the finding here, not the extension.",
                (long)s_timescaleReprobeBudget.TotalSeconds, probeClock.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "TimescaleDB re-probe could not run after {ElapsedMs} ms - the availability latch keeps the value it had and the next hourly tick probes again: {Message}",
                probeClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// The hourly retry of a managed store's role provisioning that failed at start (fleet-level, on the
    /// compression tick, outside the TimescaleDB gate). Provisioning runs once per start, and a failed attempt
    /// left the roles' passwords unset and the credential files the web and MCP hosts wait for unwritten until the
    /// next restart. This runs <see cref="DarlingManagedRoles.EnsureProvisionedAsync"/> again once per tick, on a
    /// connection from the worker's pool, until one attempt succeeds.
    ///
    /// <para><b>What a retry does and does not carry over.</b> Nothing: a failed attempt writes no credential file
    /// (see <see cref="DarlingManagedRoles.EnsureProvisionedAsync"/>), so each retry generates its passwords again.
    /// A success writes the files, and the hosts, which poll for them, connect with the passwords the store now
    /// holds; it also records what the roles were given as the reload baseline, exactly as the start path does, so
    /// the first control-plane reload does not trust a value that never landed.</para>
    ///
    /// <para><b>Logged once per attempt.</b> The start path's error line is the first failure; each failed retry
    /// writes one warning naming the message (the provisioning code reports a failed attempt by throwing, so this
    /// is the only place that failure is logged). The outcomes read differently, as the other tenants' do: shutdown is quiet,
    /// running out <see cref="s_roleProvisioningRetryBudget"/> is its own warning, anything else a warning naming
    /// the message. No rethrow, so the sweep loop never sees this fail. A success clears the retry.</para>
    ///
    /// <para><b>Deadlines.</b> The commands inside carry the bootstrap deadline they always did. That is right on
    /// this cadence, unlike the control-plane reload's: it runs at most once an hour, only after a failure, on its
    /// own connection and under the whole-attempt budget above, and the batch is catalog DDL whose cost does not
    /// grow with the store's data.</para>
    /// </summary>
    private async Task RetryRoleProvisioningAsync(CancellationToken cancellationToken)
    {
        var dataDirectory = _roleProvisioningRetryDirectory;
        if (dataDirectory is null || _postgres is null || !OperatingSystem.IsWindows())
        {
            return;
        }

        var attemptClock = Stopwatch.StartNew();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_roleProvisioningRetryBudget);

        try
        {
            _appliedComposeStatementTimeoutSeconds =
                await DarlingManagedRoles.EnsureProvisionedAsync(_postgres, dataDirectory, _logger, budget.Token);
            _roleProvisioningRetryDirectory = null;
            _logger.LogInformation(
                "Least-privilege role provisioning succeeded on the hourly retry after {ElapsedMs} ms: the role credential files are written, so the web and MCP hosts connect with the passwords the store holds.",
                attemptClock.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown - quiet and expected. The next start provisions from scratch. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Least-privilege role provisioning retry exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short; the next hourly tick tries again.",
                (long)s_roleProvisioningRetryBudget.TotalSeconds, attemptClock.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Least-privilege role provisioning retry failed after {ElapsedMs} ms; the next hourly tick tries again: {Message}",
                attemptClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// The #3812 hourly retention re-evaluation (fleet-level, Timescale-only, on the compression tick): run
    /// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync(NpgsqlConnection, ILogger, TimescaleSupport.RetentionSweepPass, CancellationToken)"/>
    /// as the <see cref="TimescaleSupport.RetentionSweepPass.Periodic"/> pass on a connection from the worker's
    /// pool. The start-path pass's <c>timescaleConnection</c> is scoped to the setup block and disposed after
    /// it, which is why this cannot simply reuse it and why the method exists at all rather than a second line
    /// beside the startup call.
    ///
    /// <para><b>What the pass does is the start path's, exactly.</b> Create what is missing (paused), converge
    /// horizons, measure every policy's coverage, arm the Covered ones and hold the Short ones. Every step is
    /// idempotent and was tested as such before it was scheduled (the method's own summary says how). What
    /// differs is what it SAYS: a hold that was already a hold is Debug here, a policy ARMED this pass is
    /// Information, a policy RE-HELD this pass is Warning, and the one line every pass writes whatever it found
    /// is <c>Retention re-evaluation: N policies held, M armed this pass, K unchanged</c> — the #3756
    /// discipline, so an operator can see the gate was re-judged without a restart marker.</para>
    ///
    /// <para><b>Failure-isolated at the worker level, with three distinct outcomes that read differently.</b>
    /// The pass completed — its own tally line, inside the method. The pass would not open a connection or
    /// threw outside its per-policy isolation — the WARNING below, naming the elapsed and the message. The pass
    /// ran out its <see cref="s_retentionReevaluationBudget"/> — a different WARNING, because a store that
    /// cannot answer twenty catalog reads and twenty chunk-pruned <c>min()</c> probes in five minutes is the
    /// finding, not the retention. Shutdown cancellation is quiet: the next start's own pass re-judges every
    /// policy, so a pass cut short by stopping the service has nothing to report. Not counted by #3013's
    /// swallowed-read counter: this is a maintenance ACTION, not an alert read whose failure would leave a
    /// condition unjudged — the Retention Held alert's own read is inside the compression method and counts
    /// there.</para>
    ///
    /// <para>The budget is ONE linked token for the WHOLE pass (#2327's shape), not a per-statement deadline:
    /// the sweep's statements carry <c>TimescaleSupport</c>'s 300 s bulk-setup bound each, which is right for
    /// a first conversion and, multiplied by a hundred statements on a slow store, wrong for something awaited
    /// on the serial sweep loop. A cancellation from that budget propagates out of the sweep's per-policy
    /// isolation by design (<c>when (ex is not OperationCanceledException)</c>), so the policies it had not
    /// reached keep their state and the WARNING here is the pass's line for the hour.</para>
    /// </summary>
    private async Task ReevaluateRetentionPoliciesAsync(CancellationToken cancellationToken)
    {
        var passClock = Stopwatch.StartNew();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_retentionReevaluationBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);

            /* #4300: the seam-only repair runs BEFORE the coverage sweep below, on the SAME connection, so a
               seam it closes this tick is already gone by the time EnsureRetentionPoliciesAsync re-judges
               coverage a moment later — the gate can release in this very tick rather than waiting for next
               hour's pass to notice. It reuses the exact per-target body the start-path walk runs
               (TimescaleSupport.RepairMaterializationSeamsAsync), restricted to legacy-paired targets and
               their seam window alone; a target with an already-closed seam is a cheap skip (one span read,
               one raw-floor read) — there is no heavier scan to avoid running twice. Failure-isolated: a
               throw here must not stop the coverage sweep or the purge trigger below, both of which are the
               pass's other, independent jobs.

               Skipped entirely while _materializationHoleRepairRunning is true: the start-path (or a stale-
               epoch relaunch) full walk covers every seam this seam-only pass would, and running both at once
               would refresh the same aggregate on two connections at the same time for no gain. */
            if (_materializationHoleRepairRunning)
            {
                _logger.LogDebug("Retention re-evaluation: skipping the seam-only repair this pass — a full materialization-hole repair is already running in this process and covers the same seam.");
            }
            else
            {
                /* #4300: the seam repair's own child budget (s_seamRepairBudget), linked to the pass's budget
                   rather than sharing it unbounded — a seam wide enough to spend the whole pass budget would
                   otherwise starve the coverage sweep, the purge trigger and the epoch relaunch below for that
                   hour, on every hour the seam stays open on a slow store. */
                using var seamBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                seamBudget.CancelAfter(s_seamRepairBudget);

                try
                {
                    var seamSummary = await TimescaleSupport.RepairMaterializationSeamsAsync(connection, _logger, DateTime.UtcNow, seamBudget.Token);
                    if (seamSummary.BucketsRepaired > 0 || seamSummary.BucketsDeferred > 0 || seamSummary.Failures > 0 || seamSummary.DailyBucketsChained > 0)
                    {
                        _logger.LogInformation(
                            "Retention re-evaluation: seam repair closed {BucketsRepaired} bucket(s) across {HolesRepaired} hole(s) this pass, {BucketsDeferred} bucket(s) left for a later pass (past this pass's per-aggregate cap), {Failures} isolated failure(s), {HolesRemaining} bucket(s) still reading as holes after repair, and refreshed {DailyBucketsChained} day(s) of the dependent daily rollup.",
                            seamSummary.BucketsRepaired, seamSummary.HolesRepaired, seamSummary.BucketsDeferred, seamSummary.Failures, seamSummary.HolesRemaining, seamSummary.DailyBucketsChained);
                    }
                    else
                    {
                        _logger.LogDebug("Retention re-evaluation: seam repair found nothing to close this pass.");
                    }
                }
                /* #4300: the seam's OWN budget ran out — not the pass's outer budget, and not shutdown — so
                   the seam repair is cut short here and the rest of the pass (the coverage sweep, the purge
                   trigger, the epoch relaunch) still runs this tick. A cut-short refresh is resume-safe: the
                   batches that committed before the cut ran newest-first, so the floor sits inside ground this
                   pass already covered and next hour's seam-only pass picks up exactly where this one left off
                   (RollupBackfill.cs's own newest-first resume argument, ~:203). The count that follows is a
                   cheap re-read (span plus raw-floor per legacy-paired target, the same two probes the seam
                   repair's own skip check makes), not a re-walk, so a store stuck deferring the same seam for
                   a day is visible from this WARNING alone without anyone re-running the repair by hand. */
                catch (OperationCanceledException) when (seamBudget.IsCancellationRequested && !budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    /* #4300: the re-read itself is failure-isolated from the rest of the pass — a throw here
                       (a catalog read that times out, a connection already in a bad state after the budget
                       cut it off mid-statement) must not stop the coverage sweep, the purge trigger or the
                       epoch relaunch below, the exact isolation this whole catch exists to preserve. An
                       unreadable count logs as "unknown" rather than ending the pass here. */
                    string hoursDeferred;
                    try
                    {
                        hoursDeferred = (await TimescaleSupport.CountOpenSeamHoursAsync(connection, DateTime.UtcNow, budget.Token)).ToString(System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        hoursDeferred = "unknown";
                    }

                    _logger.LogWarning(
                        "Retention re-evaluation: seam repair paused at the {BudgetMinutes}-minute budget; resumes next hour; {HoursDeferred} hour(s) still deferred.",
                        (long)s_seamRepairBudget.TotalMinutes, hoursDeferred);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(
                        "Retention re-evaluation: the seam repair could not run this pass — any legacy/successor seam a prior outage opened stands until a later pass retries: {Message}",
                        ex.Message);
                }
            }

            await TimescaleSupport.EnsureRetentionPoliciesAsync(
                connection, _logger, TimescaleSupport.RetentionSweepPass.Periodic,
                otherRepairRunning: () => _materializationHoleRepairRunning, budget.Token);

            /* #4299: the service's own raw-purge trigger — ONLY reached from this Periodic pass, never
               from the start path. TriggerRawPurgeAsync re-reads the sweep's own verdict (config->>'darling_armed',
               written moments ago by the call above) rather than threading it through as a parameter, so a
               future caller of EnsureRetentionPoliciesAsync cannot accidentally skip the trigger by forgetting
               to wire a return value through. */
            await TriggerRawPurgeAsync(connection, budget.Token);

            /* #4299/#4391: the relaunch — when the epoch does not match the CURRENT postmaster start on
               ANY raw job and no repair this process launched is still running, start a new one. Checked
               against ALL THREE raw relations, not just the first: RunMaterializationHoleRepairAsync stamps
               all three under the SAME postmaster-start value in the same pass ON A CLEAN COMPLETION, but a
               repair that failed on (or was cancelled during) the second or third relation stamps only the
               ones it reached, leaving the rest stale — reading only relation zero would then miss that and
               never relaunch. A store that has never had a repair run under this start (every key null) reads
               the same "no match" as a store whose repair is simply stale. _materializationHoleRepairRunning is
               this PROCESS's own guard against launching a second overlapping repair; the epoch stamp in the
               store is the separate guard (RawRepairEpochStampSql's IS DISTINCT FROM) that stops a SECOND
               SERVICE from repeating the work — the two are independent and both are checked here because
               either alone is not enough: two processes each with the flag clear would otherwise both launch. */
            if (!_materializationHoleRepairRunning && TimescaleSupport.RawRelations.Count > 0)
            {
                bool epochCurrent = true;
                var staleRelations = new List<string>();
                foreach (var relation in TimescaleSupport.RawRelations)
                {
                    await using var epochCheck = new NpgsqlCommand(
                        TimescaleSupport.RawRepairEpochMatchesSql(relation), connection);
                    var value = await epochCheck.ExecuteScalarAsync(budget.Token);
                    var relationCurrent = value is bool b && b;
                    epochCurrent &= relationCurrent;
                    if (!relationCurrent)
                    {
                        staleRelations.Add(relation);
                    }
                }

                if (ShouldLaunchMaterializationHoleRepair(_materializationHoleRepairRunning, epochCurrent) && _postgres is not null)
                {
                    _logger.LogInformation(
                        "Retention re-evaluation: the repair epoch is stale under the current postmaster start for {StaleRelations} and no repair is running in this process — launching one now (Periodic pass only).",
                        string.Join(", ", staleRelations));

                    /* Deliberately NOT awaited, the same posture the start path takes for the identical call:
                       a capped repair on the heaviest aggregate is a policy run's worth of work, and this pass
                       has three more tenants to reach this hour. _materializationHoleRepairRunning is set at
                       the top of the method and cleared in its finally, so the NEXT Periodic tick (an hour on)
                       sees it running and does not launch a second one; TriggerRawPurgeAsync above already saw
                       the stale epoch this tick and skipped the purge, which is correct — the repair this
                       launches has not finished yet, so nothing this tick should have purged. */
                    _periodicHoleRepair = RunMaterializationHoleRepairAsync(_postgres, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. The next start's own pass re-judges every policy. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Retention re-evaluation exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short - the policies it had not reached keep the state they were in and are re-judged next hour (and at the next start). A store that cannot answer twenty catalog reads and twenty chunk-pruned min() probes inside that budget is the finding here, not the retention.",
                (long)s_retentionReevaluationBudget.TotalSeconds, passClock.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Retention re-evaluation could not run after {ElapsedMs} ms - every held policy stays held until the next hour retries (or the next start): {Message}",
                passClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>
    /// #4299: the Periodic pass's trigger for the three raw jobs' own purge — reached only from
    /// <see cref="ReevaluateRetentionPoliciesAsync"/>, on the SAME connection and immediately after
    /// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync(NpgsqlConnection, ILogger, TimescaleSupport.RetentionSweepPass, CancellationToken)"/>
    /// has run this pass's coverage sweep, so the drop decision below is measured fresh, not read back from a
    /// value the sweep merely wrote. For each raw relation, in the same pass:
    /// <list type="bullet">
    /// <item>a FRESH Covered verdict — <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/>, called again
    /// here rather than trusting <c>config-&gt;&gt;'darling_armed'</c> the sweep just above wrote: <c>darling_armed</c>
    /// is the readers' and alerts' standing verdict and is deliberately left as it was on anything OTHER than
    /// Covered (#1877's bounded-depth-cap posture), so an Unknown probe (one that throws, times out, or comes
    /// back empty) leaves a PRIOR pass's <c>true</c> in place; a purge gated on that stale value would drop
    /// chunks on a coverage state this pass itself could not confirm. <c>IsRawTierDropSafeAsync</c> answers
    /// true ONLY on a Covered verdict measured THIS call — Short and Unknown both answer false — so the purge
    /// can never run on a verdict older than the pass that is about to act on it;</item>
    /// <item>a repair finished under the CURRENT <c>pg_postmaster_start_time()</c> — <see cref="TimescaleSupport.RawRepairEpochMatchesSql"/>;</item>
    /// <item>no hole in the range the purge is about to drop — <see cref="TimescaleSupport.HoleFreeThroughAsync"/>
    /// over every registered aggregate whose source is this raw relation, from the oldest raw chunk's
    /// <c>range_start</c> to <c>now() - drop_after</c>. A successor <see cref="TimescaleSupport.ResolveMaterializationAsync"/>
    /// cannot resolve (mid-rebuild, renamed, or dropped) is NOT skipped as though it were hole-free: an
    /// unresolvable successor means the gate cannot tell whether the range it should be covering is intact,
    /// which is exactly the state <c>hole-free</c> must never mean. This records <c>gate_unknown</c> and blocks
    /// the purge, the same fail-closed posture as an Unknown coverage verdict.</item>
    /// </list>
    /// All hold → <see cref="TimescaleSupport.RunRetentionPurgeJobAsync"/>. One INFORMATION line per raw
    /// job either way, naming which gate it failed (or that the run itself failed).
    ///
    /// <para>Deferred ranges are passed empty: a deferred range is still a hole (the source has rows, the
    /// materialization has none), so the fresh hole scan below finds it.</para>
    /// </summary>
    private Task TriggerRawPurgeAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
        => TriggerRawPurgeCoreAsync(connection, _logger, cancellationToken);

    /// <summary>
    /// #4299: the exact body of <see cref="TriggerRawPurgeAsync"/>, extracted as an internal static
    /// method so <c>Darling.Tests</c> (already reachable via this project's <c>InternalsVisibleTo</c>) can
    /// drive it directly against a live store without standing up a whole <see cref="DarlingWorker"/>. No
    /// behaviour change: the instance method above is now a one-line forward to this, with <c>_logger</c>
    /// threaded through as the <paramref name="logger"/> parameter.
    /// </summary>
    internal static async Task TriggerRawPurgeCoreAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        var emptyDeferred = Array.Empty<(DateTime Start, DateTime End)>();

        foreach (var relation in TimescaleSupport.RawRelations)
        {
            try
            {
                /* Measured THIS call, not read back from config->>'darling_armed' — that key stays as the
                   readers' and alerts' standing verdict and can hold a PRIOR pass's true through an Unknown
                   probe (#1877). The purge decision needs this pass's own fresh answer, so it calls the same
                   probe IsRawTierDropSafeAsync arms against, again, right here. */
                var covered = await TimescaleSupport.IsRawTierDropSafeAsync(connection, relation, cancellationToken);

                if (!covered)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — not covered (a fresh measurement this pass found Short or Unknown for it).",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "not_covered", null, null, logger, cancellationToken);
                    continue;
                }

                bool epochMatches;
                await using (var epochRead = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql(relation), connection))
                {
                    var value = await epochRead.ExecuteScalarAsync(cancellationToken);
                    epochMatches = value is bool b && b;
                }

                if (!epochMatches)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — the repair epoch is stale or missing (no repair has finished under the CURRENT postmaster start yet).",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "epoch_stale", null, null, logger, cancellationToken);
                    continue;
                }

                /* The scan's window is [oldest chunk range_start, now - drop_after], which contains every
                   whole chunk drop_chunks would actually drop (a chunk drops only once its range_end <=
                   now - drop_after). Mixing range_start (timestamptz) with now() AT TIME ZONE 'UTC' is safe
                   here because this connection pins Timezone=UTC, so the subtraction and every later
                   comparison against it stay in UTC with no local-offset step. */
                long jobId;
                DateTime dropFrom;
                DateTime dropTo;
                await using (var range = new NpgsqlCommand($@"
SELECT j.job_id,
       (SELECT min(c.range_start) FROM timescaledb_information.chunks AS c WHERE c.hypertable_schema = 'collect' AND c.hypertable_name = '{relation}'),
       now() AT TIME ZONE 'UTC' - (j.config->>'drop_after')::interval
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{relation}'", connection))
                {
                    await using var reader = await range.ExecuteReaderAsync(cancellationToken);
                    if (!await reader.ReadAsync(cancellationToken) || await reader.IsDBNullAsync(1, cancellationToken))
                    {
                        /* No job row, or raw holds no chunks at all — nothing to drop, nothing to gate. */
                        logger.LogInformation(
                            "Raw retention purge for {Relation} did not run this pass — no chunks to evaluate (the job row or the oldest chunk could not be read).",
                            relation);
                        await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "no_chunks", null, null, logger, cancellationToken);
                        continue;
                    }

                    jobId = reader.GetInt64(0);
                    dropFrom = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                    dropTo = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
                }

                var holeFree = true;
                var successorUnresolved = false;
                foreach (var target in TimescaleSupport.MaterializationHoleTargets)
                {
                    if (!string.Equals(target.Source, relation, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    /* #5329: the io hourlies never hold the raw purge (the #1661 rule), the same as the coverage
                       verdict above, which leaves them out of RawTierCoverage. See RawPurgeUngatedRollups. */
                    if (!TimescaleSupport.HoldsRawPurge(target.View))
                    {
                        continue;
                    }

                    var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, cancellationToken);
                    if (materialization is null)
                    {
                        /* An unresolvable successor (mid-rebuild, renamed, or dropped) is NOT hole-free — the
                           gate cannot tell whether the range it should be covering is intact, so it must not
                           read as clean. Fail closed, same as an Unknown coverage verdict above. */
                        successorUnresolved = true;
                        holeFree = false;
                        break;
                    }

                    if (!await TimescaleSupport.HoleFreeThroughAsync(
                        connection, target, materialization.Value, dropFrom, dropTo, emptyDeferred, cancellationToken))
                    {
                        holeFree = false;
                        break;
                    }
                }

                if (successorUnresolved)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — a successor's materialization could not be resolved (mid-rebuild, renamed, or dropped), so coverage cannot be confirmed hole-free.",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "gate_unknown", null, null, logger, cancellationToken);
                    continue;
                }

                if (!holeFree)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — a hole was found in the range about to be dropped.",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "hole", null, null, logger, cancellationToken);
                    continue;
                }

                var runClock = Stopwatch.StartNew();
                var outcome = await TimescaleSupport.RunRetentionPurgeJobAsync(connection, jobId, logger, cancellationToken);
                runClock.Stop();
                if (!outcome.Ran)
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} did not run this pass — the run itself failed with SqlState {SqlState} (see the warning above naming the timeout or error).",
                        relation, outcome.SqlState ?? "(none)");
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "run_failed", outcome.SqlState, null, logger, cancellationToken);
                }
                else
                {
                    logger.LogInformation(
                        "Raw retention purge for {Relation} ran this pass — covered, epoch matched the current postmaster start, and no hole in the dropped range.",
                        relation);
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, relation, "ran", null, runClock.ElapsedMilliseconds, logger, cancellationToken);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* #4391: the trigger's own gate check failing partway through (a timeout or error reading
                   coverage, the epoch, chunk ranges, or a successor's holes) is not a routine "not covered
                   this pass" outcome the way the continues above are — it means the gate itself could not
                   answer, so it is logged at Warning rather than Information, and recorded as gate_error so
                   the record distinguishes "the gate said no" from "the gate could not run". The record call
                   is wrapped so a failure recording the outcome never escapes this catch and skips the next
                   relation in the loop. */
                logger.LogWarning(
                    "Raw retention purge for {Relation} did not run this pass — the trigger's own gate check failed: {Message}",
                    relation, ex.Message);

                try
                {
                    await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(
                        connection, relation, "gate_error", (ex as PostgresException)?.SqlState, null, logger, CancellationToken.None);
                }
                catch (Exception recordEx) when (recordEx is not OperationCanceledException)
                {
                    logger.LogDebug("Could not record the gate_error purge outcome for {Relation}: {Message}", relation, recordEx.Message);
                }
            }
        }
    }

    /// <summary>
    /// The #3817 hourly store-object convergence (fleet-level, on the store-maintenance tick): run the WHOLE
    /// <see cref="s_storeObjectConvergence"/> list, in the list's order, each step failure-isolated, and write
    /// the one summary line. The fourth tenant of this tick and the last statement in its
    /// <see cref="_timescaleAvailable"/> gate.
    ///
    /// <para><b>The lie this closes.</b> Every step in that list ran exactly once, at service start, each one
    /// failure-isolated per item — which is the right posture and is exactly what makes a single failure
    /// invisible and permanent. One missing continuous aggregate is one rollup family gone; one missing
    /// baseline relation is one anomaly family silently returning an empty baseline (the provider reads them by
    /// name and swallows the 42P01); an un-applied tuning pass is every composer query back on un-indexed
    /// scans. On a service that runs for months, "until the next restart" and "forever" are the same sentence.
    /// The code already knew: the ungated baseline-fallback block exists BECAUSE a startup step can silently
    /// leave one relation unbuilt — and it was itself a startup step that can silently leave one relation
    /// unbuilt.</para>
    ///
    /// <para><b>Why the whole list on one connection here, when the start path uses three.</b> The start path's
    /// three connections are an artefact of its three try/catch blocks, whose boundaries carry meaning there
    /// (the TimescaleDB block degrades the latch; the ungated block runs on every path; the tuning block
    /// degrades to un-tuned queries). This pass has ONE outcome — the store is converged or this hour's
    /// attempt says what it could not do — so it has one connection, one budget and one catch, and the
    /// segments collapse. The steps' own per-item isolation is untouched and is still what keeps one failed
    /// aggregate from costing the other nineteen.</para>
    ///
    /// <para><b>Cost on a converged store, which is the case that must be cheap.</b> Every step is a catalog
    /// read plus DDL only where something is missing: <c>if_not_exists</c> on the hypertable and policy
    /// creates, <c>IF NOT EXISTS</c> on the aggregate and index creates, probe-then-create on the baseline
    /// relations, and differ-only <c>alter_job</c> on the three converges. Two statements are unconditional
    /// rather than catalog-gated — the <c>ALTER TABLE ... SET (timescaledb.compress ...)</c> enable inside
    /// <see cref="TimescaleSupport.ApplyCompressionPolicyAsync"/> and its <c>collection_log</c> twin — and
    /// they are the reason this pass takes any locks at all; both are metadata-only on a table that already
    /// carries the reloption. Measured on a converged rig (TimescaleDB 2.28.1): the figure is in the PR body
    /// and the summary line carries the elapsed on every pass, which is the honest way to keep that claim
    /// current on a real store rather than on a rig.</para>
    ///
    /// <para><b>The phase grid is not moved by running this hourly.</b> Creating a missing aggregate or policy
    /// here runs the same DDL the start path would have run, and every phase it assigns is deterministic on
    /// the registry — <see cref="TimescaleSupport.RefreshPhaseMinutesFor(string)"/> and
    /// <see cref="TimescaleSupport.TryCompressionPhaseMinutesFor"/> are pure functions of the view or table name,
    /// so the slot a policy lands on does not depend on WHEN the ensure ran. Two <c>initial_start</c>
    /// expressions are clock-relative — the raw compression policy's "next hour plus this table's phase
    /// minutes" and the aggregate compression band's "N nights after the next UTC midnight" — so a policy
    /// created by an hourly pass rather than by a start takes its own first run from that pass's clock. That
    /// is the same drift a restart at a different time of day already produces, it affects only a policy this
    /// store did not have, and the MINUTE (the grid slot the phase argument protects) is identical either
    /// way.</para>
    ///
    /// <para><b>Failure-isolated with the three outcomes this tick's tenants all use</b> (#3812's shape, and
    /// the same reason): shutdown is quiet, the budget gets its own WARNING naming the budget, and anything
    /// else gets a WARNING naming the message. Nothing rethrows — the sweep loop must never see this pass
    /// fail. A pass cut short leaves the steps it had not reached exactly as they were; they are idempotent,
    /// so the next hour resumes rather than restarting the ladder. Not counted by #3013's swallowed-read
    /// counter: this is a maintenance ACTION, not an alert read whose failure would leave a condition
    /// unjudged.</para>
    /// </summary>
    private async Task ConvergeStoreObjectsAsync(CancellationToken cancellationToken, bool timescaleAvailable = true)
    {
        var passClock = Stopwatch.StartNew();
        var tally = new StoreObjectConvergenceTally();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_storeObjectConvergenceBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);
            foreach (var step in s_storeObjectConvergence)
            {
                /* #3913: the ONE filter the hourly pass has. On a store without TimescaleDB, a step that needs
                   it is skipped rather than run to fail; every other step runs, as it does at start. */
                if (!timescaleAvailable && NeedsTimescale(step.Stage))
                {
                    continue;
                }

                await RunStoreObjectConvergenceStepAsync(connection, step, tally, _logger, budget.Token, hourly: true);
            }

            /* #5574: the perfmon_stats chunk re-group, AFTER the whole list because the "compression policies" step is what
               moves the hypertable to the new grouping first (the re-group does nothing until it has). It is a background
               loop on its own connection and the service's stopping token, not a step: the rewrite of a large store's chunk
               takes minutes, and a step would hold this pass (and the sweep loop that awaits it) for them. Starting it costs
               nothing here (no database work, and nothing when a run is already going); a converged store's run is two
               catalog reads and it ends. Hourly pass only: the start path does not call it, so a restart is not held; the
               first hourly pass, about 30 s after a start (its stamp seeds at MinValue, #3812/#3817), starts it, and
               every hourly pass after. TimescaleDB-gated like the steps that need it. */
            if (timescaleAvailable)
            {
                StartPerfmonRegroupDrain(cancellationToken);
            }

            LogStoreObjectConvergence(tally, passClock.ElapsedMilliseconds, startup: false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. The next start's own pass runs every step again. */
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Store object convergence exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short — {Steps} step(s) ran, {Changed} changed, and the steps it had not reached are exactly as they were and are retried next hour (and at the next start). Every step is idempotent, so the next pass resumes at the one this pass was inside rather than redoing the ones behind it. A CONVERGED store answers this pass in well under a second; a store that spends five minutes in it is either building something real for the first time — a first hypertable conversion or a first aggregate on an adopted store — or is too slow to answer its own catalog, and which one it is shows in the per-object lines above.",
                (long)s_storeObjectConvergenceBudget.TotalSeconds, passClock.ElapsedMilliseconds, tally.Steps, tally.Changed.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Store object convergence could not run after {ElapsedMs} ms — every store object stays exactly as it is (a missing rollup family stays missing, a missing baseline relation keeps returning nothing) until the next hour retries or the service restarts: {Message}",
                passClock.ElapsedMilliseconds, ex.Message);
        }
    }

    /// <summary>The perfmon_stats chunk re-group loop (#5574); null until the first hourly pass on a store with TimescaleDB.</summary>
    private PerfmonRegroupDrain? _perfmonRegroupDrain;

    /// <summary>Starts the re-group loop unless one is running. Never throws and does no database work on this thread.</summary>
    private void StartPerfmonRegroupDrain(CancellationToken stoppingToken)
    {
        _perfmonRegroupDrain ??= new PerfmonRegroupDrain(token => _postgres!.OpenConnectionAsync(token), _logger);
        _perfmonRegroupDrain.StartIfIdle(stoppingToken);
    }

    /// <summary>
    /// One SEGMENT of <see cref="s_storeObjectConvergence"/> on the start path — the steps tagged
    /// <paramref name="stage"/>, in the list's order, on the connection that segment's try block owns
    /// (#3817). The hourly pass has no segments and walks the list whole; this exists only because the start
    /// path interleaves three non-convergence steps between the ensures and two of those positions are
    /// load-bearing.
    ///
    /// <para>NOT wrapped in a catch of its own: each segment's call site already sits inside the try that
    /// decides what a failure THERE degrades to (the TimescaleDB latch, the baseline gap, un-tuned queries),
    /// and those three outcomes read differently for reasons their own issues argued. Adding a fourth catch
    /// here would convert a TimescaleDB-block fault into a silently-skipped segment and leave
    /// <c>_timescaleAvailable</c> true.</para>
    /// </summary>
    private async Task RunStoreObjectConvergenceSegmentAsync(
        NpgsqlConnection connection,
        StoreObjectConvergenceStage stage,
        StoreObjectConvergenceTally tally,
        CancellationToken cancellationToken)
    {
        foreach (var step in s_storeObjectConvergence)
        {
            if (step.Stage != stage)
            {
                continue;
            }

            await RunStoreObjectConvergenceStepAsync(connection, step, tally, _logger, cancellationToken);
        }
    }

    /// <summary>
    /// One convergence step, failure-isolated, its outcome recorded in <paramref name="tally"/> (#3817) — the
    /// #1775 shape, and the ONE place a step's result is judged, so the start path and the hourly pass cannot
    /// disagree about what counts as changed or failed.
    ///
    /// <para>A step that THROWS writes its own WARNING here and is counted as failed. That is a second warning
    /// on top of whatever the step's own per-item isolation already wrote, and deliberately so: the step's
    /// internal warnings name one item, this one names the step that did not complete, and the difference
    /// matters — a per-aggregate failure is one family, a throw out of
    /// <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/> itself is all twenty. Cancellation is
    /// rethrown, so the budget and shutdown reach the pass's own catches rather than being recorded as a
    /// failure of every step.</para>
    ///
    /// <para>#5444: a statement whose failure made Npgsql close the connection (an ERROR in SQLSTATE classes XX, 58
    /// or 53) would leave every later step on the pass's one connection throwing "Connection is not open", each
    /// logged as a second, misleading failure until the next hourly pass. So after EVERY step, whether it threw or
    /// returned, a connection that is no longer Open is reopened through
    /// <see cref="TimescaleSupport.ReopenBrokenConnectionAsync"/> (the #5439 helper), with one additional warning
    /// naming the step. It runs after a normal return too because most steps isolate their own statements: a
    /// <c>DROP MATERIALIZED VIEW ... CASCADE</c> that breaks the connection inside
    /// <see cref="TimescaleSupport.DropStaleContinuousAggregatesAsync"/> is caught and logged there, and the step
    /// returns normally with the connection closed. A failed reopen is logged and never thrown: the pass's own
    /// catches decide what a failure degrades to, and the next step tries again. A connection that is still Open is
    /// left alone, and cancellation skips the check because it propagates.</para>
    ///
    /// <para><see cref="StoreObjectChangeSignal"/> is what keeps the changed count honest: only the six
    /// steps whose return value IS a change count can contribute to it, and the rest are counted as steps
    /// that ran. The alternative reads "changed: hypertable conversion, compression policies, continuous
    /// aggregates" on every hour of a store that has not changed since July.</para>
    ///
    /// <para><b>Static and internal, deliberately</b>, taking its logger rather than reading the field: this
    /// is the one seam in the pass whose behaviour can be DRIVEN rather than read off the source. The
    /// property that matters — a step which throws on one pass is retried on the next, counted failed then
    /// changed — needs two passes over the same step and nothing else, so
    /// <c>StoreObjectConvergenceStepBehaviourTests</c> drives it with a fake step instead of standing up a
    /// host, a store and a clock. Everything else about the pass is a call site or a list entry, which is
    /// textual and pinned as such.</para>
    /// </summary>
    internal static async Task RunStoreObjectConvergenceStepAsync(
        NpgsqlConnection connection,
        StoreObjectConvergenceStep step,
        StoreObjectConvergenceTally tally,
        ILogger logger,
        CancellationToken cancellationToken,
        bool hourly = false)
    {
        try
        {
            var ensure = hourly && step.HourlyEnsureAsync is not null ? step.HourlyEnsureAsync : step.EnsureAsync;
            var count = await ensure(connection, logger, cancellationToken);
            tally.Steps++;
            if (step.Signal == StoreObjectChangeSignal.Delta && count > 0)
            {
                tally.Changed.Add($"{step.Name} {count.ToString(CultureInfo.InvariantCulture)}");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            tally.Steps++;
            tally.Failed.Add(step.Name);
            logger.LogWarning(
                "Store object convergence step '{Step}' failed — whatever it had not yet ensured stays unbuilt until the next hourly pass or the next start retries it (the step's own lines above name any individual object it did isolate): {Message}",
                step.Name, ex.Message);
        }

        /* #5444: after every step, thrown OR returned, because most steps swallow their own per-statement errors
           and return normally with the connection already closed. A null connection is the behaviour tests' fake. */
        if (connection is not null && connection.State != System.Data.ConnectionState.Open)
        {
            if (await TimescaleSupport.ReopenBrokenConnectionAsync(
                    connection, logger, cancellationToken,
                    "Store object convergence: the store connection is closed and could not be reopened, so this step's successors fail; the next step tries the reopen again: {Message}"))
            {
                logger.LogWarning(
                    "Store object convergence step '{Step}' left the store connection closed; it was reopened so the steps after it still run",
                    step.Name);
            }
        }
    }

    /// <summary>
    /// The ONE line a convergence pass writes, on both cadences, whatever it found (#3817, the #3756
    /// discipline). One owner so the start-path and hourly lines cannot drift into two shapes; the only
    /// difference between them is which of the two literal templates the <paramref name="startup"/> switch
    /// picks, exactly as <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/>'s pass switch does.
    ///
    /// <para>A pass that changed NOTHING still writes it, because that is the pass this line exists for: a
    /// check whose negative outcome is indistinguishable from its non-execution has not reported, and before
    /// this the only evidence a convergence pass had run was eleven per-object lines and the absence of a
    /// twelfth. The changed list names the steps rather than counting them, because "2 changed" sends an
    /// operator back through the per-object lines to find out which.</para>
    /// </summary>
    private void LogStoreObjectConvergence(StoreObjectConvergenceTally tally, long elapsedMs, bool startup)
    {
        var changed = tally.Changed.Count == 0 ? "none" : string.Join(", ", tally.Changed);
        var failed = tally.Failed.Count == 0 ? "none" : string.Join(", ", tally.Failed);

        if (startup)
        {
            _logger.LogInformation(
                "Store object convergence at startup: {Steps} steps, {Changed} changed ({ChangedNames}), {Failed} failed ({FailedNames}), {ElapsedMs} ms",
                tally.Steps, tally.Changed.Count, changed, tally.Failed.Count, failed, elapsedMs);
        }
        else
        {
            _logger.LogInformation(
                "Store object convergence: {Steps} steps, {Changed} changed ({ChangedNames}), {Failed} failed ({FailedNames}), {ElapsedMs} ms",
                tally.Steps, tally.Changed.Count, changed, tally.Failed.Count, failed, elapsedMs);
        }
    }

    /// <summary>
    /// The #4823 once-a-minute checkpointer sample: one read of the store's cumulative checkpoint sync time, handed to
    /// <see cref="CheckpointSyncSampler"/>, which differences consecutive reads to find the longest single sync in the
    /// hour. Awaited on the main loop, so the whole sample - connection wait included - runs under one short budget. A
    /// failure (a store that is down, a permission the bring-your-own store lacks, the budget expiring) is a Debug line
    /// and a skipped minute: the sampler keeps its previous baseline, and the next successful read differences against
    /// it, which can only lengthen the difference it reports. Never throws except for the service stopping.
    /// </summary>
    private async Task SampleCheckpointSyncAsync(CancellationToken cancellationToken)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(s_checkpointSyncSampleBudget);

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);
            var syncMs = await StoreSelfMetrics.ReadCheckpointerSyncTimeMsAsync(connection, budget.Token);
            if (syncMs is long sampled)
            {
                _checkpointSyncSampler.Observe(DateTime.UtcNow, sampled);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            || (budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested))
        {
            _logger.LogDebug(ex, "checkpoint sync sample failed and was skipped");
        }
    }

    /// <summary>
    /// The #2068 store self-metrics sweep (fleet-level, hourly): one <see cref="StoreSelfMetrics"/> run —
    /// per-hypertable size/compression rows (Timescale stores only, gated on the cached
    /// <see cref="_timescaleAvailable"/> flag INSIDE the sweep so plain-PG stores still record their
    /// dimension + whole-store rows), the payload-dimension size/row-count rows, the whole-store summary
    /// row, and the series' own bounded retention DELETE. Failure-isolated at the worker level like the
    /// disk-pressure and compression checks: a store hiccup logs and skips this tick, never aborting the
    /// sweep loop, and the series simply gains a one-hour gap.
    ///
    /// <para>Two more self-telemetry passes ride the same tick, connection and budget: the #3021
    /// <see cref="StoreLogSweep"/> read of the store's own server log, and the #2674 collector-cost flush.
    /// A fourth pass, the #5097 <see cref="StoreStatementHistory"/> snapshot, rides the same budget.
    /// The shared budget is what bounds the whole tick — every pass on one
    /// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/> linked CTS, not one each.</para>
    /// </summary>
    /// <param name="checkpointLongestSync">(#4834) The longest single checkpoint sync the minute sampler saw since the
    /// previous tick, or null when it took no difference; the sweep stores it on the hour's checkpointer row.</param>
    /// <param name="cancellationToken">The service's stop.</param>
    private async Task SweepStoreSelfMetricsAsync(CheckpointSyncMax? checkpointLongestSync, CancellationToken cancellationToken)
    {
        /* #2327 review catch: this sweep is AWAITED on the main loop, unlike the fire-and-track
           per-server sweeps — so its worst case stalls per-server dispatch and the disk-pressure and
           compression checks with it. The budget is therefore ONE SweepTimeoutSeconds for the WHOLE
           sweep (a linked CTS), not per statement: worst-case loop block stays ~5 minutes, comparable
           to the old default's 5 x 30s, instead of the 25 minutes five sequential 300s statements
           could take against a genuinely wedged store. The per-statement CommandTimeout inside
           StoreSelfMetrics stays as the belt for callers that pass no token. */
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(TimeSpan.FromSeconds(StoreSelfMetrics.SweepTimeoutSeconds));

        try
        {
            await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);

            /* #3923: its OWN narrow catch, ahead of the store-log capture below and on the same reasoning
               that catch already carries - a PostgresException the store permanently rejects here (catalog
               drift on a bring-your-own TimescaleDB, a revoked privilege) must not cost the store-log
               census or the collector-cost flush below it every hour forever. #3918 was exactly this for
               one statement (a catch-all census failing 42703 on TimescaleDB 2.29+); the sizing pass shared
               the OUTER catch then, so fixing that one statement left the next one the store rejects able to
               take the whole tick down again. Narrower than the capture's catch below: a command TIMEOUT
               still falls through to the outer catch (unchanged one-hour-gap handling), and so does a fault
               that actually broke the connection - a server-REJECTED statement leaves the session idle and
               usable, which is what the two passes after it need. */
            try
            {
                await StoreSelfMetrics.SweepAsync(connection, _timescaleAvailable, DateTime.UtcNow, _logger,
                    checkpointLongestSync?.SyncMs, checkpointLongestSync?.SampledUtc, budget.Token);
            }
            catch (PostgresException ex) when (!PgBaselineProvider.IsCommandTimeout(ex)
                                               && connection.State == ConnectionState.Open)
            {
                _logger.LogError(
                    "Store self-metrics sweep failed ({SqlState}), so this hour's size rows are missing; the "
                    + "store-log census and collector-cost flush on the same tick still run: {Message}",
                    ex.SqlState, ex.Message);
            }

            /* #3021: the store reading its OWN server log, on the same hourly tick and the same connection.
               It rides this cadence rather than carrying its own for two reasons. The log grows slowly (a
               production day is ~1,400 entries worth classifying), so an hourly bucket is the finest grain
               the census can honestly report; and sharing the tick makes the two self-telemetry series -
               what the store WEIGHS and what it COMPLAINED about - land on the same timestamp grid, which is
               how they get read side by side.

               ITS OWN catch, unlike the two passes either side of it, and that asymmetry is the point. This
               is the only pass here whose PRIVILEGE is not guaranteed: reading the log needs
               pg_read_server_files plus an explicit GRANT on pg_read_binary_file, which the managed store's
               bootstrap superuser has and a bring-your-own store's owner may not have given. Sharing the
               outer catch would let that one permanent condition cost the collector-cost flush below it
               every hour forever - a new failure in a pass that was working. Warning rather than Error for
               the same reason: on a store that cannot grant it this repeats hourly and would otherwise
               pollute every "errors in the last hour" count with a condition nobody is going to change.
               The durable record is on the read surface, where get_store_log reports zero captures as
               not_collected and names the privilege. */
            try
            {
                await StoreLogSweep.SweepAsync(connection, DateTime.UtcNow, _logger, budget.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var unavailable = ex is PostgresException
                {
                    SqlState: PostgresErrorCodes.UndefinedFile or PostgresErrorCodes.InsufficientPrivilege,
                };
                if (unavailable && _storeLogCaptureUnavailableWarned)
                {
                    _logger.LogDebug("Store log capture is still unavailable: {Message}", ex.Message);
                }
                else
                {
                    _logger.LogWarning(
                        "Store log capture failed, so this hour's store-log census is missing (get_store_log "
                        + "reports the capture gap). Reading the store's own log needs pg_read_server_files and "
                        + "EXECUTE on pg_read_binary_file; a bring-your-own store may not grant them: {Message}",
                        ex.Message);
                    _storeLogCaptureUnavailableWarned |= unavailable;
                }
            }

            /* #3971: Npgsql closes THIS connection outright on the capture's ERROR rather than leaving it
               idle-but-usable - reproduced against a plain PostgreSQL 18.4 store on Npgsql 10.0.3, where a
               pg_ls_dir fault (58P01) takes connection.State straight to Closed. The re-mask and the
               collector-cost flush below run on this SAME connection, so a store that cannot grant the
               capture's privilege, or has no log directory at all (the Linux compose store's shape), lost
               both of them every hour before this - get_collector_cost never got a row however long that
               store ran. Reopened rather than swapped for a fresh one: this connection came from the pool
               through _postgres, so OpenAsync pulls a new physical connection under the same object the
               `await using` above already owns and disposes. A capture that succeeded, or failed WITHOUT
               closing the connection, finds it already Open here and this is a no-op. */
            if (connection.State != ConnectionState.Open)
            {
                /* A Broken connection must be closed before it can open again; closing a Closed one is a no-op. */
                await connection.CloseAsync();
                await connection.OpenAsync(budget.Token);
            }

            /* #3915, #3944: rows stored before this build kept their entries whole (an ERROR's STATEMENT line with
               its literals), for the capture's 400-day retention. Their SQL is normalized one bounded slice per
               tick until the table's end, then not again this process; a new capture is normalized on write. Its
               own catch, like the capture's, and its own time cap (#3920's review): neither a failure nor a slow
               slice may cost the collector-cost flush below. */
            if (!_storeLogRemaskDone)
            {
                using var remaskBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                remaskBudget.CancelAfter(StoreLogSweep.RemaskSliceBudget);
                try
                {
                    var (next, examined, rewritten) = await StoreLogSweep.RemaskStoredEventsAsync(
                        connection, _storeLogRemaskCursor, remaskBudget.Token);
                    _storeLogRemaskCursor = next;
                    _storeLogRemaskDone = next is null;
                    if (rewritten > 0)
                    {
                        _logger.LogInformation(
                            "Store log: re-masked {Rewritten} of {Examined} stored row(s) captured before this build, so their SQL literals no longer reach get_store_log and their messages group by shape{Remaining}.",
                            rewritten, examined, next is null ? "" : "; the rest follow on the next hourly passes");
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException
                    || (remaskBudget.IsCancellationRequested && !budget.IsCancellationRequested))
                {
                    _logger.LogWarning(
                        "Store log: re-masking rows captured before this build failed, and is retried next hour: {Message}",
                        ex.Message);
                }
            }

            /* #4012: PostgreSQL deadlock reports stored before #4005 keep their SQL raw, with a hash over the raw
               graph, for pg_deadlocks' 90 days, and a deadlock alert fired before it keeps the same in its history
               row, and an analysis finding its exemplars. Every read normalizes them; a direct SELECT does not.
               Page after page until the slice's own cap (#4012's review: a page an hour took about 11 days for the
               reports alone), alerts, then reports, then findings, with the store-log slice's own-catch, own-cap
               posture: neither a failure nor a slow slice may cost the collector-cost flush below. RunAsync ends
               quietly on its cap and keeps each walk's cursor; a stage's own failures are counted and logged there.
               Gated on Pending, not Done (#4036's round-2 review, finding 1): a stage that gave up counts as done,
               and RunAsync is where it is tried again a day later, so it must still be called. */
            if (_pgDeadlockRemask.Pending)
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.CloseAsync();
                    await connection.OpenAsync(budget.Token);
                }

                /* #4012's review, finding 1: no key is no reason to skip the pass. The key file can be unusable for
                   good (an untrusted directory or ACL, an unreadable file, DPAPI after the service moved machines),
                   and the reports, alerts, findings and finding alerts need no key; only the alert-key stage waits
                   for one, and RunAsync says so once. */
                using var remaskBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                remaskBudget.CancelAfter(PgDeadlockRemask.SliceBudget);
                try
                {
                    await PgDeadlockRemask.RunAsync(connection, _pgDeadlockRemask, _pgDeadlockRemaskKey, _logger, remaskBudget.Token);
                }
                catch (Exception ex) when (ex is not OperationCanceledException
                    || (remaskBudget.IsCancellationRequested && !budget.IsCancellationRequested))
                {
                    _logger.LogWarning(
                        "PostgreSQL deadlocks: re-masking alerts, reports and findings stored before this build failed, and is retried next hour: {Message}",
                        ex.Message);
                }
            }

            /* The deadlock text an earlier build's pass rewrote is brought to the current rules once per store
               (PgDeadlockRemask.RulesVersion); it does not wait for the pass above, and the store's marker keeps it
               from running again. Its own slice budget and catch, as the pass above has. */
            if (_pgDeadlockRemask.RepairPending)
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.CloseAsync();
                    await connection.OpenAsync(budget.Token);
                }

                using var repairBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                repairBudget.CancelAfter(PgDeadlockRemask.SliceBudget);
                try
                {
                    await PgDeadlockRemask.RunRepairAsync(connection, _pgDeadlockRemask, _logger, repairBudget.Token);
                }
                catch (Exception ex) when (ex is not OperationCanceledException
                    || (repairBudget.IsCancellationRequested && !budget.IsCancellationRequested))
                {
                    _logger.LogWarning(
                        "PostgreSQL deadlocks: repairing stored deadlock text failed, and is retried next hour: {Message}",
                        ex.Message);
                }
            }

            /* #5097: the store's own statement history, on this same hourly tick so it lands on the census' grid.
               ITS OWN catch, and a Warning once: a store whose owner cannot run the reader function (a bring-your-own
               store without the extension) repeats the same failure hourly, and none of it may cost the
               collector-cost or read-latency flush below. A snapshot that fails rolls back whole, so the baseline
               and the history never disagree. */
            if (connection.State != ConnectionState.Open)
            {
                await connection.CloseAsync();
                await connection.OpenAsync(budget.Token);
            }

            /* Its own slice of the budget, like the re-mask above: seven commands at 60 s each could otherwise use the
               whole sweep budget and a cancellation here would skip the flushes below. A slice that expired while the
               budget did not is a failed pass like any other; only the budget itself ends the sweep. */
            using var historyBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
            historyBudget.CancelAfter(StoreStatementHistory.SliceBudget);
            try
            {
                await StoreStatementHistory.SnapshotAsync(connection, DateTime.UtcNow, _logger, historyBudget.Token);
                _storeStatementHistoryWarned = false;
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                || (historyBudget.IsCancellationRequested && !budget.IsCancellationRequested))
            {
                var sqlState = (ex as PostgresException)?.SqlState ?? "n/a";
                if (_storeStatementHistoryWarned)
                {
                    _logger.LogDebug("Store statement history is still failing (SqlState {SqlState}): {Message}", sqlState, ex.Message);
                }
                else
                {
                    _logger.LogWarning(
                        "Store statement history failed (SqlState {SqlState}), so this hour's per-statement deltas are missing: {Message}",
                        sqlState, ex.Message);
                    _storeStatementHistoryWarned = true;
                }
            }

            /* The flush runs on this same connection, which a canceled slice above can leave closed. */
            if (connection.State != ConnectionState.Open)
            {
                await connection.CloseAsync();
                await connection.OpenAsync(budget.Token);
            }

            /* #2674: reuse the same hourly connection and budget — one aggregate row per (server, collector)
               for the window, plus the accumulator's own bounded retention DELETE. */
            await _collectorCost.FlushAsync(connection, DateTime.UtcNow, _logger, budget.Token);

            /* #4442 scope 2: the read-latency flush runs on the SAME tick, isolated from collector_cost's own
               failure — a bad read-latency flush must not cost the collector_cost series its hourly row, and
               vice versa (collector_cost's own catch above already isolates it from everything ABOVE this
               line). */
            /* #5506 part 3: the long-read table is rebuilt BEFORE the flush drains this hour's samples, so it reads them
               from memory and the stored hours from the table, and counts each once. A failed rebuild keeps the last one
               and never costs the flush. */
            if (_readAdmission is not null)
            {
                /* Its own slice of the budget, like the history above: the scan reads seven days, and a canceled slice must
                   not cancel the flush below, which the same connection then runs. */
                var readAdmission = _readAdmission;
                await ReadAdmission.RunRebuildSliceAsync(
                    token => readAdmission.RefreshAsync(connection, _readLatency, token),
                    ReadAdmission.RebuildSliceBudget, _logger, budget.Token);

                /* A canceled slice can leave the connection closed or broken. */
                if (connection.State != ConnectionState.Open)
                {
                    await connection.CloseAsync();
                    await connection.OpenAsync(budget.Token);
                }
            }

            try
            {
                await _readLatency.FlushAsync(connection, DateTime.UtcNow, _logger, budget.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !budget.IsCancellationRequested)
            {
                _logger.LogWarning("Read-latency flush failed; this hour's histogram rows are dropped: {Message}", ex.Message);
            }

            /* #5810: the hourly rollup get_collection_health reads for its closed hours. Last on the tick and in its own slice
               of the budget, so its first pass after the upgrade (a backfill of the whole window) cannot cost the flushes
               above their rows. A failed or canceled pass loses nothing: the next one starts from the newest rolled hour,
               and the read takes the hours after it from the raw log. */
            if (connection.State != ConnectionState.Open)
            {
                await connection.CloseAsync();
                await connection.OpenAsync(budget.Token);
            }

            using var hourlyBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
            hourlyBudget.CancelAfter(CollectionLogHourly.PassSliceBudget);
            try
            {
                await CollectionLogHourly.RollUpAsync(connection, DateTime.UtcNow, _logger, hourlyBudget.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                || (hourlyBudget.IsCancellationRequested && !budget.IsCancellationRequested))
            {
                _logger.LogWarning(
                    "Collection log hourly rollup failed (SqlState {SqlState}); the next pass retries from the newest rolled hour: {Message}",
                    (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected. */
        }
        catch (Exception ex)
        {
            /* #2317: a command TIMEOUT and a genuine fault are the same Npgsql message here ("Exception
               while reading from stream" — the #2294 lesson), and on the dogfood box that costume produced
               ~5 fake network-fault ERRORs a day. Name each cause; both are one-hour series gaps that
               self-heal on the next tick. The budget CTS surfaces as OperationCanceledException — with
               the SERVICE token untripped that can only be the sweep budget, so it takes the timeout
               arm too. */
            /* NOT counted by #3013's swallowed-read counter, in either arm: this sweep WRITES the
               self-metrics series. No alert is judged on its result — the store self-alerts read their own
               evidence in EvaluateCompressionJobHealthAsync, which IS counted. */
            if (PgBaselineProvider.IsCommandTimeout(ex) || (ex is OperationCanceledException && budget.IsCancellationRequested))
            {
                _logger.LogError(
                    "Store self-metrics sweep did not finish within its {Timeout}s command timeout — this tick's metrics are skipped and the series gains a one-hour gap (the store side logs this as 'canceling statement due to user request'). If it repeats, the store's sizing queries have outgrown the timeout: {Message}",
                    StoreSelfMetrics.SweepTimeoutSeconds, ex.Message);
            }
            else
            {
                _logger.LogError("Store self-metrics sweep failed: {Message}", ex.Message);
            }
        }
    }
}
