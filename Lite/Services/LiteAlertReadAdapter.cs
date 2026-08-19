/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's <see cref="IAlertReadAdapter"/> (Phase-5 slice B): the alert loop's collected-store reads
/// behind the shared seam, wrapping the existing <see cref="LocalDataService"/> queries verbatim —
/// the SQL, windows, and LIMITs are unchanged; this class only adds the seam's client-side filters
/// that previously lived inline in <c>MainWindow.AlertEngine.cs</c> (the poison-wait threshold
/// FindAll and the long-running-query excluded-database Where). Every method keeps the loop's
/// <c>Task.Run</c> wrap so DuckDB.NET's synchronous I/O stays off the WPF dispatcher (#1202).
/// <para>
/// <c>serverKey</c> is Lite's deterministic storage-name hash rendered as a string (the same value
/// the alert loop already keys its state dictionaries on) — parsed back to the DuckDB
/// <c>server_id</c> int here.
/// </para>
/// </summary>
public sealed class LiteAlertReadAdapter : IAlertReadAdapter
{
    private readonly LocalDataService _dataService;
    private readonly Func<int, int>? _runningJobsCadenceMinutes;
    private readonly Func<int, int>? _blockingSnapshotCadenceMinutes;

    /// <param name="runningJobsCadenceMinutes">
    /// Resolves a server's EFFECTIVE running_jobs collection cadence (minutes) for the #1812
    /// snapshot-freshness bound — MainWindow supplies the ScheduleManager lookup. Null (test
    /// call sites) or a non-positive answer falls back to the shared
    /// <see cref="CollectorScheduleDefaults"/> cadence.
    /// </param>
    /// <param name="blockingSnapshotCadenceMinutes">
    /// The same resolver for the dmv_blocking_snapshot cadence, behind #1839's freshness bound.
    /// </param>
    public LiteAlertReadAdapter(
        LocalDataService dataService,
        Func<int, int>? runningJobsCadenceMinutes = null,
        Func<int, int>? blockingSnapshotCadenceMinutes = null)
    {
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _runningJobsCadenceMinutes = runningJobsCadenceMinutes;
        _blockingSnapshotCadenceMinutes = blockingSnapshotCadenceMinutes;
    }

    /// <summary>
    /// XE blocked-process reports with the always-on DMV snapshot fallback merged in —
    /// <see cref="LocalDataService.GetRecentBlockedProcessReportsAsync"/> already performs the
    /// BPR-preferred merge via the shared <see cref="BlockedProcessReportMerge"/>.
    /// </summary>
    public async Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var rows = await Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(serverId, hoursBack), cancellationToken);
        return new List<BlockedProcessAlertRow>(rows);
    }

    /// <summary>
    /// #1839: the latest blocking snapshot's summed wait time, with the #1812 freshness verdict
    /// attached at the server's effective dmv_blocking_snapshot cadence.
    /// </summary>
    public async Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var snapshot = await Task.Run(() => _dataService.GetCurrentBlockingWaitAsync(serverId), cancellationToken);
        if (snapshot is null)
        {
            return null;
        }

        var cadence = ResolveCadence(_blockingSnapshotCadenceMinutes, serverId, "dmv_blocking_snapshot");
        bool isFresh = DateTime.UtcNow - snapshot.Value.SnapshotTime <= CurrentBlockingWaitResult.MaxSnapshotAge(cadence);
        return new CurrentBlockingWaitResult(
            snapshot.Value.SnapshotTime, snapshot.Value.TotalWaitMs, snapshot.Value.BlockedSessionCount, isFresh);
    }

    public async Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var rows = await Task.Run(() => _dataService.GetRecentDeadlocksAsync(serverId, hoursBack), cancellationToken);
        return new List<DeadlockAlertRow>(rows);
    }

    public async Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(
        string serverKey, double thresholdMs, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var poisonWaits = await Task.Run(() => _dataService.GetLatestPoisonWaitAvgsAsync(serverId), cancellationToken);
        /* Fetch-then-filter, exactly like the pre-slice-B loop: the 3-row window is selected
           before thresholding (see the IAlertReadAdapter contract). */
        return poisonWaits.FindAll(w => w.AvgMsPerWait >= thresholdMs);
    }

    public async Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
        string serverKey,
        int thresholdMinutes,
        int maxResults,
        bool excludeSpServerDiagnostics,
        bool excludeWaitFor,
        bool excludeBackups,
        bool excludeMiscWaits,
        bool excludeCdc,
        IReadOnlyList<string> excludedDatabases,
        CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var longRunning = await Task.Run(() => _dataService.GetLongRunningQueriesAsync(
            serverId, thresholdMinutes, maxResults, excludeSpServerDiagnostics, excludeWaitFor,
            excludeBackups, excludeMiscWaits, excludeCdc), cancellationToken);

        if (excludedDatabases is { Count: > 0 })
        {
            longRunning = longRunning
                .Where(q => string.IsNullOrEmpty(q.DatabaseName) ||
                    !excludedDatabases.Any(e =>
                        string.Equals(e, q.DatabaseName, StringComparison.OrdinalIgnoreCase)))
                .ToList();
        }

        return longRunning;
    }

    public async Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        return await Task.Run(() => _dataService.GetVolumeFreeSpaceAsync(serverId), cancellationToken);
    }

    /// <summary>#2349: the file-growth read, delegated like every other adapter member so the alert engine
    /// stays free of DuckDB.</summary>
    public async Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(
        string serverKey, int lookbackMinutes, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        return await Task.Run(() => _dataService.GetDatabaseFileGrowthAsync(serverId, lookbackMinutes), cancellationToken);
    }

    public async Task<List<PvsPressureInfo>> GetPvsPressureAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        return await Task.Run(() => _dataService.GetPvsPressureAsync(serverId), cancellationToken);
    }

    public async Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        return await Task.Run(() => _dataService.GetLatestTempDbSpaceAsync(serverId), cancellationToken);
    }

    public async Task<AnomalousJobsResult> GetAnomalousJobsAsync(
        string serverKey, int multiplier, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        /* #1812: the latest snapshot is only evidence when fresh. A stopped collector, missed cycles,
           lost msdb access, or the 512 MB reset (with parquet still serving the newest rows through
           v_running_jobs) all leave a stale "latest" that would otherwise read as NOW — and the engine's
           per-run cooldown key expires each pass, so a stale snapshot re-fires the same historical run
           every cooldown, forever. Stale => no evidence: skip the row read entirely. */
        var snapshotTime = await Task.Run(() => _dataService.GetLatestRunningJobsSnapshotTimeAsync(serverId), cancellationToken);
        var cadence = ResolveRunningJobsCadence(serverId);
        if (snapshotTime is null || DateTime.UtcNow - snapshotTime.Value > AnomalousJobsResult.MaxSnapshotAge(cadence))
        {
            return AnomalousJobsResult.Stale;
        }

        var jobs = await Task.Run(() => _dataService.GetAnomalousJobsAsync(serverId, multiplier), cancellationToken);
        return new AnomalousJobsResult(SnapshotIsFresh: true, jobs);
    }

    /// <summary>
    /// Databases deviating from their expected (baseline/override) state — delegates to
    /// <see cref="LocalDataService.GetDatabaseStateDeviationsAsync"/>, which also auto-seeds the
    /// first-observation baseline. Task.Run-wrapped like the other reads to keep DuckDB's synchronous
    /// I/O off the WPF dispatcher (#1202).
    /// </summary>
    public async Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        return await Task.Run(() => _dataService.GetDatabaseStateDeviationsAsync(serverId), cancellationToken);
    }

    /// <summary>
    /// #2157. Off the WPF dispatcher via Task.Run for the same reason as its siblings here (#1202).
    /// </summary>
    public async Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        return await Task.Run(() => _dataService.GetForcePlanFailuresAsync(serverId), cancellationToken);
    }

    private int ResolveRunningJobsCadence(int serverId) =>
        ResolveCadence(_runningJobsCadenceMinutes, serverId, "running_jobs");

    /// <summary>
    /// A server's effective cadence for one collector: the host's resolver when it answers usefully,
    /// otherwise the shipped default, otherwise 2 minutes (an unregistered collector name).
    /// </summary>
    private static int ResolveCadence(Func<int, int>? resolver, int serverId, string collectorName)
    {
        var resolved = resolver?.Invoke(serverId) ?? 0;
        if (resolved > 0)
        {
            return resolved;
        }

        return PerformanceMonitor.Collectors.CollectorScheduleDefaults.All.TryGetValue(collectorName, out var schedule)
            ? schedule.FrequencyMinutes
            : 2;
    }

    private static int ParseServerKey(string serverKey) =>
        int.Parse(serverKey, CultureInfo.InvariantCulture);
}
