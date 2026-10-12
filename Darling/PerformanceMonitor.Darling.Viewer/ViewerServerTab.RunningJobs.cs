/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Running Jobs inner tab — a COPY of Lite's Running Jobs grid (ServerTab.xaml + the
/// <c>RefreshRunningJobsAsync</c> load), reads rewired to Postgres. The grid shows the latest snapshot
/// of currently-running SQL Agent jobs with their historical duration comparison; the column-filter
/// headers ride the shared W1g filter manager (<c>_runningJobsFilterMgr</c>, registered in
/// ViewerServerTab.Filters.cs), and the <c>IsRunningLong</c> row highlight lives in the XAML row style.
/// The one behavioral deviation from Lite is the msdb-access banner: Lite drives it from a live
/// per-connection probe; the viewer derives it from the store (see <see cref="LoadRunningJobsAsync"/>).
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// Running Jobs tab load: the latest-snapshot grid read and the collector-status read (for the
    /// msdb banner) fire concurrently — NpgsqlDataSource pools a connection for each — then the grid
    /// goes through the filter manager's UpdateData so any active column filter survives the refresh.
    /// </summary>
    private async Task LoadRunningJobsAsync()
    {
        using var readFanOut = ViewerReadFanOut.Of(2);
        _ownNoDataText.TryAdd(RunningJobsNoDataMessage, RunningJobsNoDataMessage.Text); /* the words are kept for when the banner goes away */
        RunningJobsRead read;
        string? status;
        try
        {
            var jobsTask = _dataService.ReadRunningJobsAsync(_server.ServerId);
            var statusTask = _dataService.GetLatestRunningJobsCollectorStatusAsync(_server.ServerId);
            read = await jobsTask;
            status = await statusTask;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A failed read is not "no jobs are running": the grid empties and says the read failed (the caller also reports it
               in the status bar). The grid used to keep whatever its last load said, "No SQL Agent jobs are running." included. */
            _runningJobsFilterMgr!.UpdateData(new System.Collections.Generic.List<RunningJobRow>());
            RunningJobsNoDataMessage.Text = RunningJobsReadFailedText(ex);
            RunningJobsNoDataMessage.Visibility = Visibility.Visible;
            throw;
        }

        var jobs = read.Jobs;

        /* Both reads are done, and the not-collected note below may read the store once more. Release here so that read is not
           priced against contention that has already finished. */
        readFanOut.Release();

        _runningJobsFilterMgr!.UpdateData(jobs);
        /* D20: an empty grid says no job is running, unless the collector was denied msdb (then the banner says why the grid is
           empty, and "no jobs running" would be a claim nobody checked) or the not-collected note applies. */
        var bannerShows = ShouldShowMsdbBanner(status);
        await ShowEngineGapAsync(RunningJobsNoDataMessage, "running_jobs", jobs.Count, keepsOwnEmptyText: !bannerShows);

        /* An empty grid whose collector has not collected within the freshness bound says that instead (the gap note and the banner win). */
        if (jobs.Count == 0 && !bannerShows && read.LastGoodCollection is DateTime lastGood
            && RunningJobsNoDataMessage.Text == _ownNoDataText[RunningJobsNoDataMessage])
        {
            RunningJobsNoDataMessage.Text = PerformanceMonitor.Alerting.RunningJobsCurrency.NotCurrentNote(lastGood);
            RunningJobsNoDataMessage.Visibility = Visibility.Visible;
        }

        RunningJobsMsdbWarning.Visibility = bannerShows ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>What the tab says in place of "No SQL Agent jobs are running." when the read itself failed.</summary>
    internal static string RunningJobsReadFailedText(Exception ex) =>
        $"The running jobs could not be read: {ex.Message}";

    /// <summary>
    /// The msdb-access banner rule, derived from the store (the viewer has no live msdb probe): Lite shows
    /// the "grant the login msdb access" banner strictly from its real <c>_hasMsdbAccess</c> probe, so the
    /// store-side stand-in is ONLY the running_jobs collector's <c>PERMISSIONS</c> outcome — the sole signal
    /// that the service was denied msdb. A transient <c>ERROR</c> (timeout / network blip) is NOT a
    /// permission problem and must not raise the misleading "grant access" guidance; <c>SUCCESS</c> / null
    /// (never run) also hides it. Static + internal so the PERMISSIONS-only condition is unit-testable.
    /// </summary>
    internal static bool ShouldShowMsdbBanner(string? runningJobsCollectorStatus) =>
        string.Equals(runningJobsCollectorStatus, "PERMISSIONS", StringComparison.OrdinalIgnoreCase);
}
