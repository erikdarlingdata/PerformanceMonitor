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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /* Which collectors have ever run for this server, as the store said at the last tab refresh. It is empty until the
       first read and after a failed one, and the empty history makes no claim. */
    private CollectorRunHistory _collectorRuns = CollectorRunHistory.Empty;

    /* The note the Running Jobs loader built from the shared skipped-collector wording, by collector name. */
    private readonly Dictionary<string, string> _skippedNotes = new(StringComparer.Ordinal);

    /// <summary>The tabs whose empty states read <see cref="_collectorRuns"/>: Memory, Configuration and Configuration Changes.</summary>
    private static bool TabReadsCollectorRuns(int tabIndex) => tabIndex is 5 or 11 or 19;

    /// <summary>Reads which collectors have ever run for this server. Called once per tab refresh, not once per surface.</summary>
    private async System.Threading.Tasks.Task RefreshCollectorRunsAsync()
    {
        try
        {
            _collectorRuns = await System.Threading.Tasks.Task.Run(() => _dataService.GetCollectorRunHistoryAsync(_serverId));
        }
        catch (Exception)
        {
            _collectorRuns = CollectorRunHistory.Empty;
        }
    }

    /// <summary>
    /// Reads the running_jobs collector's last run, and builds the same skipped-collector sentence the get_running_jobs MCP tool
    /// returns from the same read. A failed read leaves no note.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshRunningJobsSkippedNoteAsync()
    {
        string? note = null;

        try
        {
            var (lastRun, serverLastCollected) = await System.Threading.Tasks.Task.Run(() => _dataService.GetCollectorLastRunAsync(_serverId, "running_jobs"));
            note = RunningJobsSkippedNote(_server.DisplayName, lastRun, serverLastCollected);
        }
        catch (Exception)
        {
            /* A failed read makes no claim. */
        }

        if (note is null)
            _skippedNotes.Remove("running_jobs");
        else
            _skippedNotes["running_jobs"] = note;
    }

    /// <summary>
    /// The running_jobs skipped-collector sentence: the shared wording, with the same skip-cause text the MCP tool passes, so the tab
    /// and the tool say the same thing. Null while the collector has run lately, or while the server has collected nothing.
    /// </summary>
    internal static string? RunningJobsSkippedNote(string serverName, DateTime? collectorLastRunUtc, DateTime? serverLastCollectedUtc) =>
        CollectorRuntimePrecondition.GatedOffMessage(serverName, "running_jobs", McpJobTools.RunningJobsSkipCauses, collectorLastRunUtc, serverLastCollectedUtc);

    /// <summary>
    /// The sentence a surface shows when its collector has no log row and no data row for this server in all retained
    /// history, and the engine does not rule the collector out. It makes no claim about why.
    /// </summary>
    internal static string NeverCollectedNote(string serverName, string collectorName) =>
        $"This data is not collected for {serverName}. The {collectorName} collector has never run for it.";

    /// <summary>
    /// Whether an empty surface says its collector does not collect for this server, and with what sentence. The note
    /// shows only when the surface has no rows and either of two things holds. The engine rules the collector out: the
    /// sentence the MCP tools return as <c>not_collected</c>. Or the collector never ran: <paramref name="neverRanNote"/>,
    /// else <see cref="NeverCollectedNote"/>. Where neither holds, or the surface has rows, the note stays hidden and the
    /// surface looks as it always did.
    /// </summary>
    /// <param name="collectorNeverRan">True when the store has no run of this collector for this server (see <see cref="CollectorRunHistory.NeverRan"/>).</param>
    /// <param name="neverRanNote">A sentence for the never-ran case that replaces the generic one, or null.</param>
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
    /// <see cref="EngineGapState"/>. On every other server the message stays collapsed, and the surface looks as it always did.
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
        var gap = EngineGapState(_server.DisplayName, _isAzureSqlDatabase, skipped is not null || _collectorRuns.NeverRan(collectorName), collectorName, rowCount, skipped);
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
