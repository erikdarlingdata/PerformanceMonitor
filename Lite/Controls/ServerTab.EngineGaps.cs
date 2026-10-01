/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /* Which collectors are known to have run for this server: everything the store showed on any refresh of a tab that
       reads it. A collector seen to run stays run. It is empty until the first read and after a failed one, and the
       empty history makes no claim. */
    private CollectorRunHistory _collectorRuns = CollectorRunHistory.Empty;

    /* The note the Running Jobs loader built from the shared skipped-collector wording, by collector name. */
    private readonly Dictionary<string, string> _skippedNotes = new(StringComparer.Ordinal);

    /// <summary>The tabs whose empty states read <see cref="_collectorRuns"/>: Memory, Configuration and Configuration Changes.</summary>
    private static bool TabReadsCollectorRuns(int tabIndex) => tabIndex is 5 or 11 or 19;

    /// <summary>
    /// Brings the known collector runs up to date. Called once per tab refresh, not once per surface. It reads the store only
    /// while a collector the surfaces ask about has not been seen to run, and never on an Azure SQL Database, where the
    /// engine note answers every one of them (see <see cref="CollectorRunHistory.ReadAsync"/>).
    /// </summary>
    private async System.Threading.Tasks.Task RefreshCollectorRunsAsync() =>
        _collectorRuns = await CollectorRunHistory.ReadAsync(
            _collectorRuns, () => System.Threading.Tasks.Task.Run(() => _dataService.GetCollectorRunHistoryAsync(_serverId)), _isAzureSqlDatabase);

    /// <summary>
    /// Brings the running_jobs skipped-collector note up to date for one Running Jobs refresh (see
    /// <see cref="ReadRunningJobsSkippedNoteAsync"/>).
    /// </summary>
    private async System.Threading.Tasks.Task RefreshRunningJobsSkippedNoteAsync()
    {
        var note = await ReadRunningJobsSkippedNoteAsync(
            _server.DisplayName, _isAzureSqlDatabase, () => System.Threading.Tasks.Task.Run(() => _dataService.GetCollectorLastRunAsync(_serverId, "running_jobs")));

        if (note is null)
            _skippedNotes.Remove("running_jobs");
        else
            _skippedNotes["running_jobs"] = note;
    }

    /// <summary>
    /// Reads the running_jobs collector's last run, and builds the same skipped-collector sentence the get_running_jobs MCP tool
    /// returns from the same read. It makes no read on an Azure SQL Database while the engine rules running_jobs out there,
    /// because the engine note answers the tab first, the same skip <see cref="CollectorRunHistory.ReadAsync"/> makes. A failed
    /// read leaves no note.
    /// </summary>
    internal static async System.Threading.Tasks.Task<string?> ReadRunningJobsSkippedNoteAsync(
        string serverName,
        bool isAzureSqlDatabase,
        Func<System.Threading.Tasks.Task<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)>> read)
    {
        if (isAzureSqlDatabase
            && !CollectorEngineCapability.IsCollectedOnEngineEdition("running_jobs", CollectorEngineCapability.AzureSqlDatabaseEngineEdition))
        {
            return null;
        }

        try
        {
            var (lastRun, serverLastCollected, serverFirstCollected) = await read();
            return RunningJobsSkippedNote(serverName, lastRun, serverLastCollected, serverFirstCollected);
        }
        catch (Exception)
        {
            /* A failed read makes no claim. */
            return null;
        }
    }

    /// <summary>
    /// The running_jobs skipped-collector sentence: the shared wording, with the same possible cause and the same first-run grace
    /// the MCP tool uses, so the tab and the tool say the same thing. Null while the collector has run lately, or while the server
    /// has collected nothing.
    /// </summary>
    internal static string? RunningJobsSkippedNote(
        string serverName, DateTime? collectorLastRunUtc, DateTime? serverLastCollectedUtc, DateTime? serverFirstCollectedUtc) =>
        CollectorRuntimePrecondition.GatedOffMessage(
            serverName, "running_jobs", CollectorRuntimePrecondition.RunningJobsPossibleCauses,
            collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc);

    /// <summary>
    /// The sentence a surface shows when its collector has no log row and no data row for this server in all retained
    /// history, its first-run grace is over, and the engine does not rule the collector out. It makes no claim about why.
    /// </summary>
    internal static string NeverCollectedNote(string serverName, string collectorName) =>
        $"This data is not collected for {serverName}. The {collectorName} collector has never run for it.";

    /// <summary>
    /// The note an empty surface shows because its collector has no run for this server, or null when it has run or nothing
    /// is known. The running_jobs collector takes it from its skipped-collector note alone: that read already covers a
    /// collector that never ran, first-run grace included, and the Running Jobs loader refreshes it with the tab. The
    /// collector history is refreshed only with the tabs that read it, so on the Running Jobs tab it can still say never ran
    /// after the collector has run. Every other collector takes it from the history: the shared "not run yet" sentence inside
    /// the collector's first-run grace, and <see cref="NeverCollectedNote"/> once the grace is over and it still has no run.
    /// </summary>
    /// <param name="serverName">The server as the surface names it.</param>
    /// <param name="collectorName">The collector that feeds the surface.</param>
    /// <param name="skipped">The skipped-collector note for the collector, or null when there is none.</param>
    /// <param name="runs">The collector history for the server.</param>
    internal static string? NoRunNote(string serverName, string collectorName, string? skipped, CollectorRunHistory runs)
    {
        if (skipped is not null || collectorName == "running_jobs")
        {
            return skipped;
        }

        return runs.NotYetRunNote(serverName, collectorName)
            ?? (runs.NeverRan(collectorName) ? NeverCollectedNote(serverName, collectorName) : null);
    }

    /// <summary>
    /// <see cref="EngineGapState"/> from what is known about the collector's runs, with <see cref="NoRunNote"/> as the
    /// sentence for a collector with no run. Every tab surface and the database state editor show their note through here,
    /// so they all say the same thing about a new server.
    /// </summary>
    internal static (string Text, Visibility Visibility) EngineGapStateFromRuns(
        string serverName, bool isAzureSqlDatabase, string collectorName, int rowCount, string? skipped, CollectorRunHistory runs)
    {
        var noRun = NoRunNote(serverName, collectorName, skipped, runs);
        return EngineGapState(serverName, isAzureSqlDatabase, noRun is not null, collectorName, rowCount, noRun);
    }

    /// <summary>
    /// Whether an empty surface says its collector does not collect for this server, and with what sentence. The note
    /// shows only when the surface has no rows and either of two things holds. The engine rules the collector out: the
    /// sentence the MCP tools return as <c>not_collected</c>. Or the collector has no run: <paramref name="neverRanNote"/>,
    /// else <see cref="NeverCollectedNote"/>. Where neither holds, or the surface has rows, the note stays hidden and the
    /// surface looks as it always did.
    /// </summary>
    /// <param name="collectorNeverRan">True when the store has no run of this collector for this server to show (see <see cref="NoRunNote"/>).</param>
    /// <param name="neverRanNote">A sentence that replaces the generic never-ran one, or null: the skipped-collector note, or the
    /// not-yet sentence inside the first-run grace.</param>
    internal static (string Text, Visibility Visibility) EngineGapState(
        string serverName,
        bool isAzureSqlDatabase,
        bool collectorNeverRan,
        string collectorName,
        int rowCount,
        string? neverRanNote = null)
    {
        var text = EngineGapNote(serverName, isAzureSqlDatabase, collectorName)
            ?? (collectorNeverRan ? neverRanNote ?? NeverCollectedNote(serverName, collectorName) : null);

        return text is not null && rowCount == 0 ? (text, Visibility.Visible) : ("", Visibility.Collapsed);
    }

    /// <summary>
    /// Fills the empty-state note of a grid or chart whose collector does not collect for this server, by the rule in
    /// <see cref="EngineGapStateFromRuns"/>. On every other server the message stays collapsed, and the surface looks as it always did.
    /// A change grid passes <paramref name="keepsOwnEmptyText"/>: its message already says "no changes" when it has no rows,
    /// so the gap sentence replaces that text and the text comes back once the gap is gone.
    /// </summary>
    /// <param name="message">The surface's <c>NoDataMessage</c> element, in the same cell as the grid or chart.</param>
    /// <param name="collectorName">The collector that feeds the surface, as the collector catalog names it.</param>
    /// <param name="rowCount">How many rows (or points) the surface is showing.</param>
    /// <param name="keepsOwnEmptyText">Whether the element shows its own text when the surface is empty and no gap applies.</param>
    private void ShowEngineGap(TextBlock message, string collectorName, int rowCount, bool keepsOwnEmptyText = false)
    {
        if (message.Tag is not string ownText)
        {
            ownText = message.Text;
            message.Tag = ownText;
        }

        _skippedNotes.TryGetValue(collectorName, out var skipped);
        var gap = EngineGapStateFromRuns(_server.DisplayName, _isAzureSqlDatabase, collectorName, rowCount, skipped, _collectorRuns);
        var gapShows = gap.Visibility == Visibility.Visible;

        message.Text = gapShows ? gap.Text : ownText;
        message.Visibility = gapShows || (keepsOwnEmptyText && rowCount == 0) ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>
    /// Whether the Running Jobs tab shows its "grant the login access to msdb" warning. It shows when the login lacks
    /// msdb access and the running_jobs collector can run here. On an Azure SQL Database or an AWS RDS instance the
    /// collector does not run at all, so no grant could help, and the not-collected note is all the tab says.
    /// </summary>
    internal static Visibility RunningJobsMsdbWarningVisibility(bool hasMsdbAccess, bool isAzureSqlDatabase, bool isAwsRds)
    {
        var edition = isAzureSqlDatabase ? CollectorEngineCapability.AzureSqlDatabaseEngineEdition : CollectorEngineCapability.UnknownEngineEdition;

        return !hasMsdbAccess && !isAwsRds && CollectorEngineCapability.IsCollectedOnEngineEdition("running_jobs", edition)
            ? Visibility.Visible
            : Visibility.Collapsed;
    }
}
