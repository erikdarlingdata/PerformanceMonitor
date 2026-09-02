/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;


namespace PerformanceMonitorLite.Services;

/// <summary>
/// Tracks the health state of an individual collector.
/// </summary>
public class CollectorHealthEntry
{
    public int ServerId { get; set; }
    public string CollectorName { get; set; } = "";
    public DateTime? LastSuccessTime { get; set; }
    public string? LastErrorMessage { get; set; }
    public int ConsecutiveErrors { get; set; }

    /*
     * Set when a collector hits a non-transient permission error
     * (SQL errors 229 / 297 / 300). The scheduler skips the collector
     * for the rest of the app session so we don't churn the log with
     * identical denials every interval. Cleared on app restart — if
     * permissions get granted later, the next launch retries once.
     */
    public bool IsPermissionRestricted { get; set; }

    /*
     * Set when the collector's Extended Events session could not be
     * created or started (issue #1086). Distinct from a query failure:
     * capture is non-functional even though reads would "succeed" with
     * zero rows. Cleared on the next successful run. MainWindow raises
     * a one-time tray notification on the false→true transition.
     */
    public bool XeSessionUnavailable { get; set; }
    public string? XeSessionMessage { get; set; }
}

/// <summary>
/// Thrown when an Extended Events session required by a collector cannot
/// be created or started. Raised before the collect query runs so a missing
/// session can never be masked by a zero-row "successful" read (issue #1086).
/// </summary>
public class XeSessionEnsureException : Exception
{
    public string SessionKind { get; }

    public XeSessionEnsureException(string sessionKind, SqlException inner)
        : base($"Failed to ensure {sessionKind} XE session: {inner.Message}", inner)
    {
        SessionKind = sessionKind;
    }

    public new SqlException InnerException => (SqlException)base.InnerException!;
}

/// <summary>
/// Summary of collector health across all collectors.
/// </summary>
public class CollectorHealthSummary
{
    public int TotalCollectors { get; set; }
    public int ErroringCollectors { get; set; }
    public int LoggingFailures { get; set; }
    public List<CollectorHealthEntry> Errors { get; set; } = new();

    /*
     * Collectors whose XE session couldn't be created/started (#1086).
     * Tracked separately from Errors because a PERMISSIONS-classified
     * failure deliberately does not increment ConsecutiveErrors, so it
     * would otherwise be invisible here.
     */
    public List<CollectorHealthEntry> XeSessionFailures { get; set; } = new();
}

/// <summary>
/// Base service for collecting performance data from remote SQL Servers.
/// Partial class - individual collectors are in separate files.
/// </summary>
public partial class RemoteCollectorService
{
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly ScheduleManager _scheduleManager;
    private readonly ILogger<RemoteCollectorService>? _logger;
    private readonly DeltaCalculator _deltaCalculator;
    public DeltaCalculator DeltaCalculator => _deltaCalculator;

    /// <summary>
    /// Limits how many SQL connections are <em>opened</em> at once — the semaphore is released
    /// when OpenAsync returns, not when the connection is disposed — smoothing the login storm
    /// when many servers are polled together. It does not cap the number of open connections.
    /// </summary>
    private static readonly SemaphoreSlim s_connectionThrottle = new(7, 7);

    /// <summary>
    /// Serializes MFA authentication attempts to prevent multiple popups.
    /// Only one MFA authentication can happen at a time.
    /// </summary>
    private static readonly SemaphoreSlim s_mfaAuthLock = new(1, 1);

    /// <summary>
    /// Command timeout for DMV queries in seconds.
    /// </summary>
    private const int CommandTimeoutSeconds = 30;

    /// <summary>
    /// Connection timeout for SQL Server connections in seconds. Read from App settings each call.
    /// </summary>
    private static int ConnectionTimeoutSeconds => App.ConnectionTimeoutSeconds;

    /// <summary>
    /// What one collector run has to hand its own collection_log row: the #1180 fetch/store split each
    /// collector method sets, and the #1837 note a run that SUCCEEDED but collected nothing leaves behind
    /// (an enumeration that yielded 0 items, items whose enumeration probe failed). Read by
    /// <see cref="RunCollectorAsync"/> once the collector completes.
    /// </summary>
    internal sealed class RunTelemetry
    {
        public long SqlMs { get; set; }
        public long StorageMs { get; set; }
        public string? Note { get; set; }

        /// <summary>
        /// True only for a cycle the #2673 whole-server wall-clock budget gave up on. Its own field rather
        /// than an inference from <see cref="Note"/>'s text, so the collection_log status never depends on
        /// wording that exists to be reworded. Darling's twin is CollectorRunResult.Abandoned — parity is
        /// the point, since both hosts previously recorded this as SUCCESS.
        /// </summary>
        public bool Abandoned { get; set; }

        /// <summary>The per-database rollup for a run that fanned out, null for one that did not (#2472).
        /// Lives beside the fetch/store split for the same reason it does: both are things one run has to
        /// hand its own collection_log row, and both are meaningless once the next run resets the slot.</summary>
        public FanoutCost? Fanout { get; set; }
    }

    /// <summary>
    /// Per-run telemetry keyed by SERVER, because a collection cycle runs the servers in PARALLEL (one
    /// task each, see RunCollectionCycleAsync) while the collectors within one server run sequentially.
    /// As plain instance fields these three were shared across those parallel tasks: server B's reset at
    /// the top of its run could blank server A's timings between A's write and A's collection_log read,
    /// and once #1837 gave every enumeration run a note, A's "enumeration yielded 0 items" could land on
    /// B's row for a collector that does not even enumerate. Keying by server is sufficient precisely
    /// because of the sequential-within-a-server rule — two collectors on one server never overlap.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, RunTelemetry> _runTelemetry = new();

    /// <summary>This server's telemetry slot, created on first use.</summary>
    internal RunTelemetry TelemetryFor(int serverId) =>
        _runTelemetry.GetOrAdd(serverId, static _ => new RunTelemetry());

    /// <summary>
    /// Tracks health state per collector per server.
    /// </summary>
    private readonly Dictionary<(int ServerId, string CollectorName), CollectorHealthEntry> _collectorHealth = new();
    private readonly object _healthLock = new();

    /// <summary>
    /// Tracks consecutive failures of the collection_log INSERT itself.
    /// </summary>
    private int _logInsertFailures;

    /// <summary>
    /// Per-server timestamp of the last master-enumeration failure that looked like a permission
    /// problem, so database-scoped collectors fall back to the connection's own catalog instead of
    /// re-probing master every cycle. Used on Azure SQL DB where per-database logins may not have
    /// master access (e.g. Microsoft Dynamics 365 FO). See issue #857.
    ///
    /// This is a throttle, NOT a permanent verdict. It expires after <see cref="AzureMasterRecheckInterval"/>,
    /// and is dropped outright when a server returns from an outage. Both escape hatches exist because
    /// the original version latched until the process restarted: a login's rights can be granted after
    /// the fact, and — the reason this changed — a momentary failure must never be able to wedge
    /// database-scoped collection for the life of the app. See issue #1506.
    /// </summary>
    private readonly Dictionary<int, DateTime> _azureMasterInaccessibleSince = new();
    private readonly object _azureMasterLock = new();

    /// <summary>
    /// How long a master-inaccessible verdict stands before master is probed again. Short enough that
    /// a misjudged failure costs minutes of database-scoped collection rather than the whole session;
    /// long enough that a login which genuinely cannot see master (#857) retries only 4x/hour.
    /// </summary>
    private static readonly TimeSpan AzureMasterRecheckInterval = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Servers observed offline and not yet seen online again. A server coming back is the signal to
    /// drop cached judgements about it that its outage could have poisoned (#1506).
    /// </summary>
    private readonly HashSet<int> _serversSeenOffline = new();

    public RemoteCollectorService(
        DuckDbInitializer duckDb,
        ServerManager serverManager,
        ScheduleManager scheduleManager,
        ILogger<RemoteCollectorService>? logger = null)
    {
        _duckDb = duckDb;
        _serverManager = serverManager;
        _scheduleManager = scheduleManager;
        _logger = logger;
        _deltaCalculator = new DeltaCalculator(logger);
        _ignoredWaitTypes = new Lazy<HashSet<string>>(LoadIgnoredWaitTypes);
    }

    /// <summary>
    /// Seeds the delta calculator cache from DuckDB to survive application restarts.
    /// Should be called once during application startup.
    /// </summary>
    public Task SeedDeltaCacheAsync() => _deltaCalculator.SeedFromDatabaseAsync(_duckDb);

    /// <summary>
    /// Gets a summary of collector health for a specific server connection.
    /// </summary>
    public CollectorHealthSummary GetHealthSummary(ServerConnection server)
        => GetHealthSummary(GetServerId(server));

    /// <summary>
    /// Gets a summary of collector health. When serverId is provided, filters to that server only.
    /// </summary>
    public CollectorHealthSummary GetHealthSummary(int? serverId = null)
    {
        lock (_healthLock)
        {
            var summary = new CollectorHealthSummary
            {
                LoggingFailures = _logInsertFailures
            };

            foreach (var entry in _collectorHealth.Values)
            {
                if (serverId.HasValue && entry.ServerId != serverId.Value)
                    continue;

                summary.TotalCollectors++;

                if (entry.ConsecutiveErrors > 0)
                {
                    summary.ErroringCollectors++;
                    summary.Errors.Add(entry);
                }

                if (entry.XeSessionUnavailable)
                {
                    summary.XeSessionFailures.Add(entry);
                }
            }

            return summary;
        }
    }

    /// <summary>
    /// Clears collector health entries for a server that has been removed.
    /// Prevents stale error counts from showing in the status bar.
    /// </summary>
    public void ClearHealthForServer(int serverId)
    {
        lock (_healthLock)
        {
            var keys = _collectorHealth.Keys.Where(k => k.ServerId == serverId).ToList();
            foreach (var key in keys)
                _collectorHealth.Remove(key);
        }
    }

    /// <summary>
    /// Clears collector health entries for all servers NOT in the provided set.
    /// Used after Manage Servers to purge stale entries for removed servers.
    /// </summary>
    public void ClearHealthExcept(HashSet<int> activeServerIds)
    {
        lock (_healthLock)
        {
            var keys = _collectorHealth.Keys
                .Where(k => !activeServerIds.Contains(k.ServerId))
                .ToList();
            foreach (var key in keys)
                _collectorHealth.Remove(key);
        }
    }

    /// <summary>
    /// Records a collector execution result for health tracking.
    /// </summary>
    internal void RecordCollectorResult(int serverId, string collectorName, string status, string? errorMessage = null, bool xeSessionUnavailable = false)
    {
        lock (_healthLock)
        {
            var key = (serverId, collectorName);
            if (!_collectorHealth.TryGetValue(key, out var entry))
            {
                entry = new CollectorHealthEntry { ServerId = serverId, CollectorName = collectorName };
                _collectorHealth[key] = entry;
            }

            entry.XeSessionUnavailable = xeSessionUnavailable;
            entry.XeSessionMessage = xeSessionUnavailable ? errorMessage : null;

            if (status == "SUCCESS")
            {
                entry.LastSuccessTime = DateTime.UtcNow;
                entry.ConsecutiveErrors = 0;
            }
            else if (status == "PERMISSIONS")
            {
                /* Permission errors are not transient — don't count as failures
                   (which would show FAILING) but don't count as success either.
                   Record the error message so the user can see what's wrong,
                   and flag the collector so the scheduler stops retrying for
                   the rest of the app session. */
                entry.LastErrorMessage = errorMessage;
                entry.IsPermissionRestricted = true;
            }
            else if (status == "YIELDED")
            {
                /* Deliberate 1s lock-timeout yield (#1805): evidence about the target's lock
                   contention, not the monitor — neither a success (a yield is not proof the
                   collector works, so the error streak is not reset) nor a failure (the guard
                   worked as designed, so the streak does not grow). The collection_log row is
                   the visible record. */
            }
            else if (status == EnumeratedCollectorDriver.AbandonedStatus)
            {
                /* #2801: a cycle the #2673 wall-clock budget abandoned. Exactly the YIELDED reasoning
                   above and for the same reason: the guard worked as designed, so the error streak must
                   not grow, and nothing was collected, so it is not proof the collector works and must
                   not reset the streak either. This arm is load-bearing rather than tidy -- the chain
                   ends in an ELSE, so without it a new status silently becomes an error here and shows
                   the collector FAILING. The message still reaches the collection_log row, which is the
                   visible record. */
            }
            else
            {
                entry.LastErrorMessage = errorMessage;
                entry.ConsecutiveErrors++;
            }
        }
    }

    /// <summary>
    /// Returns true if a collector has hit a permission denial this session
    /// and should be skipped without re-running. See <see cref="CollectorHealthEntry.IsPermissionRestricted"/>.
    /// </summary>
    private bool IsCollectorPermissionRestricted(int serverId, string collectorName)
    {
        lock (_healthLock)
        {
            return _collectorHealth.TryGetValue((serverId, collectorName), out var entry)
                && entry.IsPermissionRestricted;
        }
    }

    /// <summary>
    /// Runs all due collectors for all enabled servers.
    /// </summary>
    public async Task RunDueCollectorsAsync(CancellationToken cancellationToken = default)
    {
        /* Registered for the whole sweep, including the collection_log write at the end of each collector -
           that final write is the one that failed in the field when a reset landed mid-collection (#2594). */
        using var collectionScope = await CollectionResetGate.BeginCollectionAsync(cancellationToken);

        var enabledServers = _serverManager.GetEnabledServers();

        if (enabledServers.Count == 0)
        {
            return;
        }

        int skippedOffline = 0;
        var onlineServers = new List<ServerConnection>();

        foreach (var server in enabledServers)
        {
            var serverStatus = _serverManager.GetConnectionStatus(server.Id);
            if (serverStatus.IsOnline == false)
            {
                skippedOffline++;
                NoteServerOffline(server);
                AppLogger.Debug("Scheduler", $"Skipping offline server '{server.DisplayName}'");
                continue;
            }

            NoteServerOnline(server);
            onlineServers.Add(server);
        }

        if (onlineServers.Count == 0)
        {
            return;
        }

        AppLogger.Info("Scheduler", $"Checking per-server schedules for {onlineServers.Count}/{enabledServers.Count} servers ({skippedOffline} offline, skipped)");

        /* Run servers in parallel, but collectors within each server sequentially.
           DuckDB is single-writer; running all collectors in parallel causes spin-wait
           contention (50%+ CPU, multi-second stalls). Sequential per-server eliminates
           this while still allowing multi-server parallelism.
           Each server gets its own due-collector list from per-server schedules. */
        var serverTasks = onlineServers.Select(server => Task.Run(async () =>
        {
            /* Reconcile the opt-in long-query completion XE session to its enabled flag BEFORE the
               due-collector loop and regardless of whether it is due — a disabled collector is never
               dispatched, so the DROP-on-disable (#1496) has nowhere else to run. Cheap when nothing
               changed (state-tracked); creates on enable, drops on disable. */
            await ReconcileLongQueryCompletionsXeSessionAsync(server, cancellationToken);

            var dueCollectors = _scheduleManager.GetDueCollectorsForServer(server.Id);
            foreach (var collector in dueCollectors)
            {
                await RunCollectorAsync(server, collector.Name, cancellationToken);
            }
        }, cancellationToken));

        await Task.WhenAll(serverTasks);

        /* Run CHECKPOINT here after all collector connections are closed.
           Write lock ensures no UI readers have stale file offsets when
           CHECKPOINT reorganizes/truncates the database file. */
        try
        {
            using var writeLock = _duckDb.AcquireWriteLock();
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CHECKPOINT";
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            AppLogger.Debug("Collector", $"Post-collection checkpoint failed (non-critical): {ex.Message}");
        }
    }

    /// <summary>
    /// Runs all enabled collectors for a single server immediately (ignoring schedule).
    /// Used for initial data population when a server tab is first opened.
    /// </summary>
    public async Task RunAllCollectorsForServerAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        /* THE path from #2594. MainWindow.ConnectToServer calls this on a bare Task.Run when a server tab is
           opened, so unlike the scheduled sweep it was sequenced against nothing at all - and it runs EVERY
           collector for the server, which is how a 55-second index_object_stats came to straddle a reset. */
        using var collectionScope = await CollectionResetGate.BeginCollectionAsync(cancellationToken);

        var enabledSchedules = _scheduleManager.GetSchedulesForServer(server.Id)
            .Where(s => s.Enabled)
            .ToList();

        /* XE session setup happens inside RunCollectorAsync so the background
           collection loop also ensures/retries it, not just tab-open (#1086) */
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);

        /* Persist edition/version to DuckDB for the analysis engine */
        await PersistServerMetadataAsync(server, serverStatus);

        AppLogger.Info("Collector", $"Running {enabledSchedules.Count} collectors for '{server.DisplayName}' (serverId={GetServerId(server)}, initial load)");

        /* Reconcile the opt-in long-query completion XE session (#1496) on tab-open too, so enabling it
           takes effect promptly rather than waiting for the next scheduled sweep. Unconditional — the
           DROP-on-disable path must run even though a disabled collector is absent from enabledSchedules. */
        await ReconcileLongQueryCompletionsXeSessionAsync(server, cancellationToken);

        foreach (var schedule in enabledSchedules)
        {
            try
            {
                await RunCollectorAsync(server, schedule.Name, cancellationToken);
            }
            catch (Exception ex)
            {
                AppLogger.Error("Collector", $"Initial collector '{schedule.Name}' failed for server '{server.DisplayName}'", ex);
            }
        }
    }

    /// <summary>
    /// Runs a specific collector for a specific server.
    /// </summary>
    public async Task RunCollectorAsync(ServerConnection server, string collectorName, CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var status = "SUCCESS";
        string? errorMessage = null;
        int rowsCollected = 0;
        bool xeSessionUnavailable = false;

        /* Clear the per-call fields HERE, not only inside the definition runner: everything below can
           throw before the runner is ever entered — the XE session ensure for deadlocks /
           blocked_process_report is the live example — and the catches all fall through to the
           LogCollectionAsync at the end of this method. Reset only in the runner, such a row carried the
           PREVIOUS collector's sql/storage milliseconds as if they were its own. */
        var telemetry = TelemetryFor(GetServerId(server));
        telemetry.SqlMs = 0;
        telemetry.StorageMs = 0;
        telemetry.Note = null;
        telemetry.Abandoned = false;

        try
        {
            /* Target-gate collectors through the shared AppliesTo — the single authoritative gate surface
               both SKUs consult. Darling's collector runner calls CollectorCatalog.AppliesTo(definition, target)
               — the COMPOSED overload, which also requires the definition's TargetEngine to match, so a
               PostgreSQL definition is never handed a SQL Server target or vice versa;
               here it drives Lite's clean pre-dispatch SKIPPED log (a genuine skip with no collection_log
               row, vs. the SUCCESS/0-rows a gated collector would otherwise record). The gate CONDITION
               lives ONLY in each definition's AppliesTo override — never re-encoded in the host — so Lite
               and Darling can't drift on it again (the RDS/msdb/Azure gating-drift class). */
            var serverStatus = _serverManager.GetConnectionStatus(server.Id);
            var engineEdition = serverStatus.SqlEngineEdition;
            var target = new CollectorTargetInfo
            {
                IsAzureSqlDb = engineEdition == 5,
                IsAzureManagedInstance = engineEdition == 8,
                IsAwsRds = serverStatus.IsAwsRds,
                SqlMajorVersion = serverStatus.SqlMajorVersion,
                HasMsdbAccess = serverStatus.HasMsdbAccess,
            };

            if (!CollectorCatalog.AppliesTo(collectorName, target))
            {
                AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} SKIPPED (edition {engineEdition}, version {serverStatus.SqlMajorVersion})");
                return;
            }

            // Skip MFA servers if user has cancelled authentication
            // This prevents repeated popup dialogs during background data collection
            if (server.AuthenticationType == AuthenticationTypes.EntraMFA && serverStatus.UserCancelledMfa)
            {
                AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} SKIPPED - MFA authentication cancelled by user");
                return;
            }

            // Skip collectors that have already hit a non-transient permission denial
            // this session. Flag is in-memory — next app start retries once (see #857).
            if (IsCollectorPermissionRestricted(GetServerId(server), collectorName))
            {
                AppLogger.Debug("Collector", $"Skipping collector '{collectorName}' for server '{server.DisplayName}' - permission denied this session");
                return;
            }

            AppLogger.Debug("Collector", $"Running collector '{collectorName}' for server '{server.DisplayName}'");

            /* Ensure the backing XE session exists before reading its ring buffer.
               Runs on every cycle (cheap existence check when already present) so a
               failed first attempt self-heals instead of staying broken until a manual
               tab re-open (#1086). Throws XeSessionEnsureException on failure so the
               zero-row read below can never record a misleading SUCCESS. */
            if (collectorName == "blocked_process_report")
            {
                await EnsureBlockedProcessXeSessionAsync(server, engineEdition, cancellationToken);
            }
            else if (collectorName == "deadlocks")
            {
                await EnsureDeadlockXeSessionAsync(server, engineEdition, cancellationToken);
            }

            rowsCollected = collectorName switch
            {
                "wait_stats" => await CollectWaitStatsAsync(server, cancellationToken),
                "latch_stats" => await CollectLatchStatsAsync(server, cancellationToken),
                "spinlock_stats" => await CollectSpinlockStatsAsync(server, cancellationToken),
                "cpu_scheduler_stats" => await CollectCpuSchedulerStatsAsync(server, cancellationToken),
                "plan_cache_stats" => await CollectPlanCacheStatsAsync(server, cancellationToken),
                "cpu_utilization" => await CollectCpuUtilizationAsync(server, cancellationToken),
                "memory_stats" => await CollectMemoryStatsAsync(server, cancellationToken),
                "memory_clerks" => await CollectMemoryClerksAsync(server, cancellationToken),
                "memory_pressure_events" => await CollectMemoryPressureEventsAsync(server, cancellationToken),
                "file_io_stats" => await CollectFileIoStatsAsync(server, cancellationToken),
                "query_stats" => await CollectQueryStatsAsync(server, cancellationToken),
                "procedure_stats" => await CollectProcedureStatsAsync(server, cancellationToken),
                "query_snapshots" => await CollectQuerySnapshotsAsync(server, cancellationToken),
                "tempdb_stats" => await CollectTempDbStatsAsync(server, cancellationToken),
                "perfmon_stats" => await CollectPerfmonStatsAsync(server, cancellationToken),
                "deadlocks" => await CollectDeadlocksAsync(server, cancellationToken),
                "server_config" => await CollectServerConfigAsync(server, cancellationToken),
                "database_config" => await CollectDatabaseConfigAsync(server, cancellationToken),
                "database_states" => await CollectDatabaseStatesAsync(server, cancellationToken),
                "query_store" => await CollectQueryStoreAsync(server, cancellationToken),
                "memory_grant_stats" => await CollectMemoryGrantStatsAsync(server, cancellationToken),
                "waiting_tasks" => await CollectWaitingTasksAsync(server, cancellationToken),
                "dmv_blocking_snapshot" => await CollectDmvBlockingSnapshotAsync(server, cancellationToken),
                "blocked_process_report" => await CollectBlockedProcessReportsAsync(server, cancellationToken),
                "long_query_completions" => await CollectLongQueryCompletionsAsync(server, cancellationToken),
                "database_scoped_config" => await CollectDatabaseScopedConfigAsync(server, cancellationToken),
                "query_store_health" => await CollectQueryStoreHealthAsync(server, cancellationToken),
                "trace_flags" => await CollectTraceFlagsAsync(server, cancellationToken),
                "running_jobs" => await CollectRunningJobsAsync(server, cancellationToken),
                "database_size_stats" => await CollectDatabaseSizeStatsAsync(server, cancellationToken),
                "index_object_stats" => await CollectIndexObjectStatsAsync(server, cancellationToken),
                "server_properties" => await CollectServerPropertiesAsync(server, cancellationToken),
                "session_stats" => await CollectSessionStatsAsync(server, cancellationToken),
                "session_summary_stats" => await CollectSessionSummaryStatsAsync(server, cancellationToken),
                "system_health_events" => await CollectSystemHealthEventsAsync(server, cancellationToken),
                "default_trace_events" => await CollectDefaultTraceEventsAsync(server, cancellationToken),
                "job_history" => await CollectJobHistoryAsync(server, cancellationToken),
                "agent_status" => await CollectAgentStatusAsync(server, cancellationToken),
                "ag_replica_states" => await CollectAgReplicaStatesAsync(server, cancellationToken),
                "ag_database_replica_states" => await CollectAgDatabaseReplicaStatesAsync(server, cancellationToken),
                "plan_correction" => await CollectPlanCorrectionAsync(server, cancellationToken),
                "pvs_stats" => await CollectPvsStatsAsync(server, cancellationToken),
                _ => throw new ArgumentException($"Unknown collector: {collectorName}")
            };

            _scheduleManager.MarkCollectorRunForServer(server.Id, collectorName, startTime);

            /* Annotate a successful-but-empty run (#1837): errorMessage is provably null here — only the
               catches below assign it — so this carries the runner's note (an enumeration that listed
               zero databases, items whose enumeration probe failed) onto the collection_log row without
               touching the SUCCESS status. Health tracking and every band/count read key on status, never
               on error_message, so the note reaches the Collection Health Note column and the Collection
               Log detail grid, and is inert everywhere else. */
            errorMessage = telemetry.Note;

            /* ...with the ONE exception of a cycle the #2673 whole-server wall-clock budget abandoned, which
               arrives here on the same path because it returns normally rather than throwing. It stored
               nothing and advanced no watermark, so leaving it SUCCESS both claimed a collection that did not
               happen and put its message in the #1837 note channel, whose whole claim is that the run
               succeeded. Darling's twin is the same one-line branch in DarlingWorker. */
            if (telemetry.Abandoned)
            {
                status = EnumeratedCollectorDriver.ClassifyReturnedRun(abandoned: true);
            }

            var elapsed = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;
            AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} => {rowsCollected} rows in {elapsed}ms (sql:{telemetry.SqlMs}ms, duck:{telemetry.StorageMs}ms)");
        }
        catch (XeSessionEnsureException ex)
        {
            /* XE session couldn't be created/started — capture is dead even though
               the ring-buffer read would "succeed" with zero rows. Classify like a
               direct SQL failure so the health indicator stops showing OK (#1086). */
            var sqlError = ex.InnerException;
            errorMessage = ex.Message;
            /* #2512: the shared set. This copy was the narrowest of the four — no 916, no 8189, no 262
               — for no stated reason beyond having been written before the others grew. The additions
               are inert or correct here rather than merely tolerable: 8189 is sys.traces and cannot
               arise from an XE session ensure at all, while 262 and 916 both mean the login was denied
               where it asked, which is the PERMISSIONS this arm already records for 229/297/300. */
            status = SqlServerPermissionErrors.IsPermissionDenied(sqlError.Number)
                ? "PERMISSIONS"
                : "ERROR";
            xeSessionUnavailable = true;

            /* Logged at the level the CLASSIFICATION implies, not always Error. A denied XE session is a
               least-privilege choice a customer is entitled to make (#1823), and the arm above already
               records it as PERMISSIONS and flags the collector so the scheduler stops retrying it for the
               session. Logging that at Error made a deliberate posture read as a fault: a field log showed
               three consecutive Error lines - two from the XE layer, one from here - for a login that was
               simply not granted ALTER ANY EVENT SESSION, while every other permission denial in this method
               logs at Warn. Only a genuine ERROR status stays at Error. */
            if (status == "PERMISSIONS")
            {
                AppLogger.Warn("Collector", $"  [{server.DisplayName}] {collectorName} {ex.Message}");
            }
            else
            {
                AppLogger.Error("Collector", $"  [{server.DisplayName}] {collectorName} {ex.Message}");
            }
        }
        catch (SqlException ex) when (ex.Number == 1222 && CollectorCatalog.YieldsOnLockTimeout(collectorName))
        {
            /* The 1-second LOCK_TIMEOUT guard doing its job (#1805): the snapshot sweep stepped aside
               instead of joining a blocking chain on the monitored server. Not a collection failure —
               the next sweep sees current state and nothing cumulative or watermarked is lost — so it
               records as YIELDED: its own status, excluded from the error counts that feed collector
               health and the daily health band, and readable as evidence of lock contention on the
               TARGET rather than a monitoring fault. A 1222 from any collector without the guard flag
               falls through to the ERROR catch below, unchanged. */
            status = "YIELDED";
            errorMessage = $"Lock-timeout yield (SQL error #{ex.Number}): the 1-second LOCK_TIMEOUT guard fired rather than waiting in a blocking chain. One snapshot sweep skipped; evidence of lock contention on the monitored server, not a monitoring failure.";
            AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} YIELDED - 1s lock-timeout guard fired (target lock contention)");
        }
        catch (SqlException ex)
        {
            status = "ERROR";
            errorMessage = $"SQL Error #{ex.Number}: {ex.Message}"
                + AzureDmvPermissionHint.For(
                    ex.Number, _serverManager.GetConnectionStatus(server.Id).SqlEngineEdition == 5, ex.Message);
            AppLogger.Error("Collector", $"  [{server.DisplayName}] {collectorName} SQL Error #{ex.Number}: {ex.Message}");

            if (RetryHelper.IsTransient(ex))
            {
                AppLogger.Warn("Collector", $"Collector '{collectorName}' transient SQL error #{ex.Number} for server '{server.DisplayName}': {ex.Message}");
            }
            else if (ex.Number == 207) /* Invalid column name - likely version incompatibility */
            {
                AppLogger.Warn("Collector", $"Collector '{collectorName}' column not found for server '{server.DisplayName}' (possible version incompatibility): {ex.Message}");
            }
            else if (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
            {
                /* 8189 is sys.traces' own denial (ALTER TRACE missing) — a legitimate least-privilege
                   choice (#1823), so default_trace_events degrades as PERMISSIONS like every other
                   denied collector instead of erroring every cycle. #2512 moved the number set into
                   SqlServerPermissionErrors so this no longer MIRRORS Darling's classifier by
                   transcription — it IS Darling's classifier, and 262 (the tempdb denial behind the
                   collector's old Azure SQL DB gate) reaches both SKUs at once. */
                status = "PERMISSIONS";
                AppLogger.Warn("Collector", $"Collector '{collectorName}' permission denied for server '{server.DisplayName}': {ex.Message}");
            }
            else
            {
                AppLogger.Error("Collector", $"Collector '{collectorName}' SQL error #{ex.Number} for server '{server.DisplayName}'", ex);
            }
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("MFA authentication cancelled"))
        {
            // User cancelled MFA - don't log as error, this is expected
            status = "SKIPPED";
            errorMessage = "MFA authentication cancelled by user";
            AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} SKIPPED - {errorMessage}");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            status = "CANCELLED";
            errorMessage = "Collection cancelled";
            AppLogger.Debug("Collector", $"Collector '{collectorName}' cancelled for server '{server.DisplayName}'");
        }
        catch (Exception ex)
        {
            status = "ERROR";
            errorMessage = ex.Message;
            AppLogger.Error("Collector", $"  [{server.DisplayName}] {collectorName} {ex.GetType().Name}: {ex.Message}");
            AppLogger.Error("Collector", $"Collector '{collectorName}' failed for server '{server.DisplayName}'", ex);
        }

        // Track collector health
        RecordCollectorResult(GetServerId(server), collectorName, status, errorMessage, xeSessionUnavailable);

        // Log the collection attempt
        await LogCollectionAsync(GetServerId(server), server.DisplayName, collectorName, startTime, status, errorMessage, rowsCollected, telemetry.SqlMs, telemetry.StorageMs, telemetry.Fanout);
    }

    /// <summary>
    /// Persists SQL Server edition and major version to the servers table.
    /// Called once per collection cycle so the analysis engine can provide
    /// edition-specific recommendations (e.g., memory caps for Standard edition).
    /// </summary>
    private async Task PersistServerMetadataAsync(ServerConnection server, ServerConnectionStatus status)
    {
        if (status.SqlEngineEdition == 0 && status.SqlMajorVersion == 0) return;

        try
        {
            var serverId = GetServerId(server);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
UPDATE servers
SET sql_engine_edition = $1,
    sql_major_version = $2
WHERE server_id = $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = status.SqlEngineEdition });
            cmd.Parameters.Add(new DuckDBParameter { Value = status.SqlMajorVersion });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("Collector", $"Failed to persist server metadata for '{server.DisplayName}': {ex.Message}");
        }
    }

    /// <summary>
    /// Logs a collection attempt to the collection_log table.
    /// </summary>
    private async Task LogCollectionAsync(int serverId, string serverName, string collectorName, DateTime startTime, string status, string? errorMessage, int rowsCollected, long sqlMs = 0, long duckDbMs = 0, FanoutCost? fanout = null)
    {
        try
        {
            var durationMs = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;

            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms, fanout_item_count, slowest_item, slowest_item_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";

            command.Parameters.Add(new DuckDBParameter { Value = GenerateCollectionId() });
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = serverName });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = durationMs });
            command.Parameters.Add(new DuckDBParameter { Value = status });
            command.Parameters.Add(new DuckDBParameter { Value = errorMessage ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = rowsCollected });
            command.Parameters.Add(new DuckDBParameter { Value = (int)sqlMs });
            command.Parameters.Add(new DuckDBParameter { Value = (int)duckDbMs });

            /* All three NULL together or all three set (#2472): a slowest item with no count cannot be
               turned into the dominance ratio the columns exist for, so half an answer is worse than none.
               This INSERT names every column deliberately — it is a plain INSERT rather than the partial
               INSERT OR REPLACE that resets untouched columns elsewhere in this SKU, and it stays that way. */
            command.Parameters.Add(new DuckDBParameter { Value = fanout.HasValue ? fanout.Value.ItemCount : (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = fanout.HasValue ? fanout.Value.SlowestItem : (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = fanout.HasValue ? fanout.Value.SlowestItemMs : (object)DBNull.Value });

            await command.ExecuteNonQueryAsync();

            /* Reset failure counter on success */
            if (_logInsertFailures > 0)
            {
                AppLogger.Info("Collector", $"Collection logging recovered after {_logInsertFailures} failure(s)");
                _logInsertFailures = 0;
            }
        }
        catch (Exception ex)
        {
            _logInsertFailures++;

            if (_logInsertFailures <= 3)
            {
                /* First few failures: log at Error level with full detail */
                AppLogger.Error("Collector", $"COLLECTION LOGGING FAILED ({_logInsertFailures}x): {ex.GetType().Name}: {ex.Message}");
                AppLogger.Error("Collector", $"Failed to log collection for {collectorName} (failure #{_logInsertFailures})", ex);
            }
            else if (_logInsertFailures % 100 == 0)
            {
                /* Periodic reminder for ongoing failures */
                AppLogger.Error("Collector", $"COLLECTION LOGGING STILL BROKEN: {_logInsertFailures} consecutive failures. Last error: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Records that a server is currently unreachable, so that its return can be recognised.
    /// </summary>
    internal void NoteServerOffline(ServerConnection server)
    {
        lock (_azureMasterLock)
        {
            _serversSeenOffline.Add(GetServerId(server));
        }
    }

    /// <summary>
    /// Handles a server coming back after an outage by dropping the master-inaccessible verdict.
    ///
    /// An outage and a permission problem are not distinguishable from the error number alone —
    /// Azure SQL DB reports a firewall rejection (40615) and a login failure (18456) the same way
    /// whether the cause is "this login may not read master" or "you cannot reach this server right
    /// now". So a verdict formed during an outage is untrustworthy by construction, and the moment
    /// the server answers again is the moment to throw it away and re-probe. Without this, a login
    /// that CAN read master gets permanently misfiled as one that cannot, and database-scoped
    /// collection stays broken until the app restarts (issue #1506).
    /// </summary>
    internal void NoteServerOnline(ServerConnection server)
    {
        var serverId = GetServerId(server);

        bool returnedFromOutage;
        lock (_azureMasterLock)
        {
            returnedFromOutage = _serversSeenOffline.Remove(serverId);
            if (returnedFromOutage)
            {
                _azureMasterInaccessibleSince.Remove(serverId);
            }
        }

        if (returnedFromOutage)
        {
            AppLogger.Info("Scheduler", $"[{server.DisplayName}] reachable again — re-probing master for database-scoped collectors.");
        }
    }

    /// <summary>
    /// The databases one Azure SQL DB registration's per-database sweep covers.
    ///
    /// <para><b>A registration that names a database sweeps that database, and nothing else</b> (#2220) —
    /// the common case, since <c>server_id</c> hashes <c>host[:database][:RO]</c> and registering each
    /// database separately is how you get separate identities. That path returns immediately and never
    /// touches <c>master</c>. It also covers #857's own case better than #857 did: a login with access to one
    /// user database but not to master has a named database, so it no longer probes master, fails, and falls
    /// back — it simply never probes.</para>
    ///
    /// <para>Only a registration naming NO database — or naming <c>master</c>, where a catalog-less Azure
    /// connection lands — is a registration of the logical SERVER, and only that one enumerates.
    /// HAS_DBACCESS() returns false for user databases from master on Azure SQL DB, so that filter is
    /// skipped and inaccessible databases are handled by callers via try/catch. The re-probe throttle is
    /// deliberately NOT consulted on that path; see the comment at the call site.</para>
    ///
    /// <para>It read master unconditionally before #2220, sweeping every online database on the logical
    /// server into whichever registration ran the sweep — N registrations of N databases meant N² collection
    /// with every registration's history contaminated by its siblings'.</para>
    /// </summary>
    protected async Task<List<string>> GetAzureDatabaseListAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverId = GetServerId(server);
        var baseConnStr = _serverManager.CredentialResolver.GetConnectionString(server);
        var targetDb = new SqlConnectionStringBuilder(baseConnStr).InitialCatalog;

        /* #2220: a registration that NAMES a database is a registration OF that database, so its sweep
           covers exactly that one and never touches master. Before this, EVERY database-scoped collector
           enumerated master and swept every online database on the logical server, storing all of it under
           the one server_id of whichever registration ran the sweep — N registrations of N databases on one
           server meant N² collection with every registration's history contaminated by its siblings'.

           This also subsumes the #857 case it looks like it bypasses, and improves on it: a login granted
           access to one user database but not to master HAS a named database, so it now returns here without
           probing master at all, rather than probing, failing, forming a verdict and falling back. Master is
           reached only by a registration that names no database — the logical-server registration, which has
           nothing else to enumerate from. */
        var ownDatabase = AzureSweepScope.OwnDatabaseOrEmpty(targetDb);
        if (ownDatabase.Count > 0)
        {
            return ownDatabase;
        }

        /* NO throttle check here, and that is deliberate rather than an omission — restoring what the
           `hasFallback &&` guard used to achieve. This branch is reached ONLY when the registration names no
           database, so there is nothing to fall back TO: honouring the throttle would return
           FallbackDatabaseList, which throws immediately without probing, and would keep throwing for the
           whole recheck interval while never attempting the one thing that could recover. Probing master
           every cycle is the cheaper failure. (Review caught me reintroducing exactly this: I read
           `hasFallback &&` as a redundant condition when it was there to DISABLE the throttle.)

           The throttle machinery itself is left alone. It is tested behaviour from #857/#1506, and it is now
           unreachable in production for a different reason than this one: its whole purpose was to stop
           re-probing master for a registration that HAS a fallback, and such a registration no longer probes
           master at all. Retiring it is its own change, with those tests. */

        var connStr = new SqlConnectionStringBuilder(baseConnStr)
        {
            ConnectTimeout = ConnectionTimeoutSeconds,
            InitialCatalog = "master"
        }.ConnectionString;

        try
        {
            /* Retry transient failures so a blip costs a slower cycle instead of an ERROR row — this
               path had no retry at all before #1506. Note it cannot prevent a master-inaccessible
               verdict: RetryHelper only retries errors SqlErrorClassification calls transient, and that
               set is provably disjoint from the ones that form a verdict. The time-box and the
               reconnect-clear are what make a wrong verdict survivable; this just makes one less likely
               to be reached.

               The exclusion filter is rebuilt inside the lambda on purpose: a SqlParameter cannot be
               added to a second SqlCommand, so reusing one set across retries would throw on attempt 2. */
            return await RetryHelper.ExecuteWithRetryAsync(
                async () =>
                {
                    var (exclusionClause, exclusionParams) = BuildDatabaseExclusionFilter(server.ExcludedDatabases, "name");

                    var databases = new List<string>();
                    using var conn = new SqlConnection(connStr);
                    await conn.OpenAsync(cancellationToken);
                    using var cmd = new SqlCommand(
                        $"SELECT name FROM sys.databases WHERE state_desc = N'ONLINE' AND database_id > 0 {exclusionClause} ORDER BY name;",
                        conn)
                    { CommandTimeout = CommandTimeoutSeconds };
                    foreach (var p in exclusionParams) cmd.Parameters.Add(p);
                    using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                        databases.Add(reader.GetString(0));

                    ClearMasterInaccessible(serverId);
                    return databases;
                },
                _logger,
                $"enumerate databases on {server.DisplayName}",
                cancellationToken: cancellationToken);
        }
        catch (SqlException ex) when (ShouldFallBackToSingleDatabaseError(ex.Number))
        {
            MarkMasterInaccessible(serverId);

            return FallbackDatabaseList(server, targetDb, reason: $"master DB inaccessible (SQL error {ex.Number})");
        }
    }

    /// <summary>
    /// True while a recent master-inaccessible verdict still stands. The verdict expires so that a
    /// server whose access was restored (or whose login was granted master rights) recovers on its
    /// own, instead of collecting from a degraded database list until the app is restarted (#1506).
    /// </summary>
    internal bool IsMasterProbeThrottled(int serverId)
    {
        lock (_azureMasterLock)
        {
            if (!_azureMasterInaccessibleSince.TryGetValue(serverId, out var deniedAt))
            {
                return false;
            }

            if (DateTime.UtcNow - deniedAt < AzureMasterRecheckInterval)
            {
                return true;
            }

            _azureMasterInaccessibleSince.Remove(serverId);
            return false;
        }
    }

    private void ClearMasterInaccessible(int serverId)
    {
        lock (_azureMasterLock)
        {
            _azureMasterInaccessibleSince.Remove(serverId);
        }
    }

    /// <summary>
    /// Records a master-inaccessible verdict, stamped now. Used by the production catch path, and by
    /// tests to reach the expiry and reconnect logic without a live Azure SQL DB connection —
    /// deliberately the same method, so the tests exercise what actually ships.
    /// </summary>
    internal void MarkMasterInaccessible(int serverId, DateTime? deniedAtUtc = null)
    {
        lock (_azureMasterLock)
        {
            _azureMasterInaccessibleSince[serverId] = deniedAtUtc ?? DateTime.UtcNow;
        }
    }

    /// <summary>
    /// The database list to use when master cannot be enumerated: the connection's own catalog.
    ///
    /// When there isn't one, database-scoped collectors have nowhere to read from. That used to be a
    /// warning and an empty list, which made every one of them report SUCCESS with zero rows — the
    /// collection status bar kept saying "Running" while nothing at all was being collected. Throwing
    /// puts the failure in collection_log where it is visible and actionable (#1506).
    ///
    /// The message deliberately avoids the phrase RunCollectorAsync's MFA filter matches, so this
    /// lands in the general handler and is recorded as an ERROR rather than a silent SKIPPED.
    /// </summary>
    /// <param name="quiet">
    /// Set on the throttled path, which runs for every database-scoped collector on every cycle. The
    /// interesting event is forming the verdict, not re-reading it — logging both would put four lines
    /// a minute in the log of a #857 user whose setup is working exactly as intended.
    /// </param>
    internal static List<string> FallbackDatabaseList(ServerConnection server, string? targetDb, string reason, bool quiet = false)
    {
        var fallback = SingleDbOrEmpty(targetDb);

        if (fallback.Count == 0)
        {
            throw new InvalidOperationException(
                $"{reason}, and this connection has no target database to fall back to " +
                $"(it resolves to master). Set a Database for '{server.DisplayName}' so database-scoped " +
                $"collectors have something to read.");
        }

        if (quiet)
        {
            AppLogger.Debug("Collector", $"  [{server.DisplayName}] {reason} — collecting from '{targetDb}' only.");
        }
        else
        {
            AppLogger.Info("Collector", $"  [{server.DisplayName}] {reason} — collecting from '{targetDb}' only.");
        }

        return fallback;
    }

    /// <summary>
    /// Builds a SQL fragment and matching SqlParameters for excluding the supplied database names.
    /// When the list is empty, returns ("", []) so callers can splice without effect.
    /// Each name is parameterized — works on every supported SQL Server version (no STRING_SPLIT/OPENJSON
    /// compatibility-level dependency).
    /// </summary>
    /// <param name="excludedDatabaseNames">Names from server.ExcludedDatabases.</param>
    /// <param name="columnExpression">SQL column to filter, e.g. "d.name".</param>
    internal static (string Clause, List<SqlParameter> Parameters) BuildDatabaseExclusionFilter(
        IList<string>? excludedDatabaseNames, string columnExpression)
    {
        if (excludedDatabaseNames is null || excludedDatabaseNames.Count == 0)
            return (string.Empty, new List<SqlParameter>());

        var paramNames = new List<string>(excludedDatabaseNames.Count);
        var sqlParams = new List<SqlParameter>(excludedDatabaseNames.Count);
        for (int i = 0; i < excludedDatabaseNames.Count; i++)
        {
            string p = $"@excl_db_{i}";
            paramNames.Add(p);
            sqlParams.Add(new SqlParameter(p, System.Data.SqlDbType.NVarChar, 128) { Value = excludedDatabaseNames[i] });
        }
        return ($"AND {columnExpression} NOT IN ({string.Join(", ", paramNames)})", sqlParams);
    }

    /// <summary>
    /// Builds a SQL fragment with database names interpolated as literal N'...' values.
    /// Use this for dynamic SQL paths where parameter binding is awkward (e.g. inside
    /// a string passed to sp_executesql). Names come from user-picked checklists of
    /// existing databases, so literal interpolation with single-quote escaping is safe.
    /// When forNestedDynamicSql=true, doubles the escape for use inside an outer T-SQL
    /// string that itself becomes a dynamic-SQL @sql variable.
    /// </summary>
    internal static string BuildDatabaseExclusionLiteralClause(
        IList<string>? excludedDatabaseNames, string columnExpression, bool forNestedDynamicSql = false)
    {
        if (excludedDatabaseNames is null || excludedDatabaseNames.Count == 0)
            return string.Empty;

        string escapedQuote = forNestedDynamicSql ? "''" : "'";
        string Escape(string s) => forNestedDynamicSql
            ? s.Replace("'", "''''")
            : s.Replace("'", "''");

        var quoted = excludedDatabaseNames.Select(n => $"N{escapedQuote}{Escape(n)}{escapedQuote}");
        return $"AND {columnExpression} NOT IN ({string.Join(", ", quoted)})";
    }

    /* #2220: delegates to the shared rule — see AzureSweepScope for why this is not duplicated per host. */
    private static List<string> SingleDbOrEmpty(string? targetDb) =>
        AzureSweepScope.OwnDatabaseOrEmpty(targetDb);

    /// <summary>
    /// Whether master enumeration failed in a way that means database-scoped collectors should fall back
    /// to the connection's own catalog (#857). Deliberately broader than "this login cannot read master":
    /// a 40615 firewall rejection at the logical server says nothing about the login's rights, but the
    /// fallback still works, because Azure evaluates DATABASE-level firewall rules first and a user
    /// database can be reachable while master is not (#1631). The list — and the reason a reachability
    /// error must never be read as a rights verdict (#1506) — is owned by
    /// <see cref="SqlErrorClassification"/>, shared with Darling so the two cannot drift.
    /// </summary>
    internal static bool ShouldFallBackToSingleDatabaseError(int errorNumber) =>
        SqlErrorClassification.ShouldFallBackToSingleDatabase(errorNumber);

    /// <summary>
    /// Opens a SQL connection to a specific database on an Azure SQL DB logical server.
    ///
    /// Deliberately NOT retried. This runs once per database per database-scoped collector, and the
    /// caller already skips a database it cannot open. Backing off here would stall the whole cycle
    /// behind a single unavailable database — an auto-paused serverless database answers 40613, which
    /// is transient, so a retry would wait out the backoff on every paused database, every collector,
    /// every minute. The next cycle is the retry, and it costs one minute of that database's data
    /// rather than delaying every other server's.
    /// </summary>
    protected async Task<SqlConnection> OpenAzureDatabaseConnectionAsync(ServerConnection server, string databaseName, CancellationToken cancellationToken)
    {
        var baseConnStr = _serverManager.CredentialResolver.GetConnectionString(server);
        var connStr = new SqlConnectionStringBuilder(baseConnStr)
        {
            ConnectTimeout = ConnectionTimeoutSeconds,
            InitialCatalog = databaseName
        }.ConnectionString;

        var conn = new SqlConnection(connStr);
        try
        {
            await conn.OpenAsync(cancellationToken);
            return conn;
        }
        catch
        {
            conn.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Creates a SQL connection to a remote server.
    /// Throws InvalidOperationException if MFA authentication was cancelled by user.
    /// </summary>
    protected async Task<SqlConnection> CreateConnectionAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        // For MFA servers, serialize authentication attempts to prevent multiple popups
        bool isMfaServer = server.AuthenticationType == AuthenticationTypes.EntraMFA;
        bool mfaLockAcquired = false;

        try
        {
            // Acquire MFA lock first (if applicable) to serialize authentication
            if (isMfaServer)
            {
                await s_mfaAuthLock.WaitAsync(cancellationToken);
                mfaLockAcquired = true;

                // Check if user already cancelled MFA for this server
                var serverStatus = _serverManager.GetConnectionStatus(server.Id);
                if (serverStatus.UserCancelledMfa)
                {
                    AppLogger.Info("Collector", $"  [{server.DisplayName}] MFA authentication already cancelled - aborting");
                    throw new InvalidOperationException("MFA authentication cancelled by user. Please connect to the server explicitly to retry.");
                }
            }

            // Now acquire connection throttle
            await s_connectionThrottle.WaitAsync(cancellationToken);
            try
            {
                var connectionString = _serverManager.CredentialResolver.GetConnectionString(server);

            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = ConnectionTimeoutSeconds
            };

            var connStr = builder.ConnectionString;

                return await RetryHelper.ExecuteWithRetryAsync(async () =>
                {
                    var connection = new SqlConnection(connStr);
                    
                    try
                    {
                        await connection.OpenAsync(cancellationToken);
                        return connection;
                    }
                    catch (Exception ex) when (isMfaServer)
                    {
                        // Detect MFA cancellation and mark immediately so other waiting connections abort
                        if (MfaAuthenticationHelper.IsMfaCancelledException(ex))
                        {
                            var serverStatus = _serverManager.GetConnectionStatus(server.Id);
                            serverStatus.UserCancelledMfa = true;
                            AppLogger.Info("Collector", $"  [{server.DisplayName}] MFA authentication cancelled by user - flagging to abort other pending connections");
                        }
                        throw;
                    }
                }, _logger, $"Connect to {server.DisplayName}", cancellationToken: cancellationToken);
            }
            finally
            {
                s_connectionThrottle.Release();
            }
        }
        finally
        {
            // Release MFA lock if we acquired it
            if (mfaLockAcquired)
            {
                s_mfaAuthLock.Release();
            }
        }
    }

    /// <summary>
    /// Generates a unique collection ID — forwards to the shared generator so both SKUs stamp
    /// ids with the same idiom.
    /// </summary>
    protected static long GenerateCollectionId()
    {
        return PerformanceMonitor.Collectors.CollectionIdGenerator.Next();
    }

    /// <summary>
    /// Gets the server name used for DuckDB storage and hashing — forwards to the shared
    /// <see cref="PerformanceMonitor.Common.ServerIdHelper.BuildStorageName"/> (database-name and
    /// :RO suffixing live there) so every SKU derives the same server_id for the same server.
    /// </summary>
    internal static string GetServerNameForStorage(ServerConnection server)
    {
        return PerformanceMonitor.Common.ServerIdHelper.BuildStorageName(
            server.ServerName, server.DatabaseName, server.ReadOnlyIntent);
    }

    /// <summary>
    /// Gets the numeric server ID from the server connection.
    /// </summary>
    protected internal static int GetServerId(ServerConnection server)
    {
        return GetDeterministicHashCode(GetServerNameForStorage(server));
    }

    /// <summary>
    /// Gets the most recent value of a timestamp column from DuckDB for incremental collection.
    /// Returns null on first run or if the query fails (caller uses a fallback window).
    /// </summary>
    protected async Task<DateTime?> GetLastCollectedTimeAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
                return dt;
        }
        catch
        {
            /* If DuckDB query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// The database-scoped twin of <see cref="GetLastCollectedTimeAsync"/>, for definitions that
    /// declare a <see cref="PerformanceMonitor.Collectors.ICollectorDefinition{TRow}.PerDatabaseWatermarkColumn"/>
    /// (Azure SQL DB per-database XE capture): the newest already-collected value for ONE database,
    /// so each database's ring buffer dedups against its own history. Null on first run for that
    /// database or on failure — the caller falls back to the definition's documented window.
    ///
    /// <para><paramref name="collectedSince"/> bounds the read on <c>collection_time</c> (#2344). Null
    /// keeps the unbounded behaviour, correct for any reader whose watermark is NOT clamped; pass
    /// <see cref="WatermarkPolicy.ReadFloor"/> only from a caller whose value is, and read that method's
    /// remarks for why the bound provably changes no answer. Unbounded, this is a <c>MAX</c> over the
    /// whole of a database's history every cycle — measured at multiple seconds per database on the
    /// Darling twin's larger store, and the same shape here. DuckDB does not partition the way the
    /// Postgres store's hypertables do, so the win is min-max index pruning and a smaller scan rather
    /// than chunk exclusion, but the predicate is the same and so is the argument for it.</para>
    /// </summary>
    protected async Task<DateTime?> GetLastCollectedTimeForDatabaseAsync(
        int serverId, string tableName, string columnName, string databaseColumnName, string databaseName,
        CancellationToken cancellationToken, DateTime? collectedSince = null)
    {
        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = collectedSince is null
                ? $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND {databaseColumnName} = $2"
                : $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND {databaseColumnName} = $2 AND collection_time > $3";
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = databaseName });
            if (collectedSince is DateTime floor)
            {
                cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = floor });
            }

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
                return dt;
        }
        catch
        {
            /* If DuckDB query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// Gets the most recent value of a monotonic bigint identity column from DuckDB for incremental
    /// collection — the numeric twin of <see cref="GetLastCollectedTimeAsync"/> (job_history dedups on
    /// <c>instance_id</c>, sysjobhistory's IDENTITY bigint). Returns null on first run or if the query
    /// fails (caller uses its documented first-run/fallback path).
    /// </summary>
    protected async Task<long?> GetLastCollectedInstanceIdAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is not null && result != DBNull.Value)
                return Convert.ToInt64(result);
        }
        catch
        {
            /* If DuckDB query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// Whether a prior SUCCESS row exists in collection_log for this collector+server — the "has collected
    /// before" signal (see <see cref="PerformanceMonitor.Collectors.CollectorContext.HasCollectedBefore"/>),
    /// consulted only when the watermark is null. Returns false on any failure, which errs toward the
    /// all-history first run (correct for a genuinely fresh store).
    /// </summary>
    protected async Task<bool> HasPriorCollectorSuccessAsync(int serverId, string collectorName, CancellationToken cancellationToken)
    {
        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM collection_log WHERE server_id = $1 AND collector_name = $2 AND status = 'SUCCESS'";
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = collectorName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            return result is not null && result != DBNull.Value && Convert.ToInt64(result) > 0;
        }
        catch
        {
            /* Fail toward first-run (all-history) — matches a fresh store with no log yet. */
            return false;
        }
    }

    /// <summary>
    /// The stored per-server state for one collector's declared keys (#1962) — the sibling of
    /// <see cref="GetLastCollectedTimeAsync"/> for state no MAX() over the collected rows can produce.
    /// Read only for the collectors that declare keys, so it costs the rest nothing. An empty result on
    /// failure is the SAFE direction: every definition treats absent state as its conservative path
    /// (default_trace_events re-reads the whole rollover set), so a broken read costs time, never events.
    /// Darling's twin is <c>DarlingCollectorRunner.GetCollectorStateAsync</c>.
    /// </summary>
    protected async Task<Dictionary<string, string>> GetCollectorStateAsync(
        int serverId, string collectorName, CancellationToken cancellationToken)
    {
        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT state_key, state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2";
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = collectorName });
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(1))
                {
                    state[reader.GetString(0)] = reader.GetString(1);
                }
            }
        }
        catch (Exception ex)
        {
            /* Fail toward "no state" — the definition's conservative path, never a wrong-but-plausible one. */
            _logger?.LogDebug(ex, "Reading collector state for {Collector} failed; using the no-state path", collectorName);
        }
        return state;
    }

    /// <summary>
    /// Upserts what the definition observed this cycle (<see cref="PerformanceMonitor.Collectors.CollectorContext.PendingState"/>),
    /// after the cycle completed — so a cycle that collected zero rows still records what it saw, which is
    /// the whole point of keeping this state off the payload. Best-effort: a failed write leaves the older
    /// value, and the next cycle re-derives from it or falls back.
    /// </summary>
    protected async Task SaveCollectorStateAsync(
        int serverId, string collectorName, IReadOnlyDictionary<string, string> state, CancellationToken cancellationToken)
    {
        if (state.Count == 0)
        {
            return;
        }

        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            foreach (var entry in state)
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = @"
INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)";
                cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
                cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = collectorName });
                cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = entry.Key });
                cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = entry.Value });
                cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Storing collector state for {Collector} failed; next cycle uses the older value", collectorName);
        }
    }

    /// <summary>
    /// Deterministic hash code for a string. Forwards to the shared
    /// <see cref="PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode"/> so Lite,
    /// Dashboard, and the MCP paths all derive the same server id from a server name. Kept as a
    /// thin internal wrapper to avoid churning the many existing call sites.
    /// </summary>
    internal static int GetDeterministicHashCode(string value) =>
        PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(value);

    /* IsCollectorSupported was deleted in the gate-surface collapse: every target gate it re-encoded
       (query_stats/query_store version, server_config/trace_flags on Azure SQL DB, and the
       running_jobs/job_history/agent_status Azure/RDS/msdb gates) now lives ONLY in each definition's
       shared AppliesTo override. RunCollectorAsync consults CollectorCatalog.AppliesTo(name, target)
       pre-dispatch for the clean SKIPPED log; Darling's runner calls the same AppliesTo. One gate
       surface, compiler-shared — no second layer to drift. */
}
