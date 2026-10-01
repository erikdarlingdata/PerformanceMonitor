/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The "not collected here" note for a surface whose collector does not collect for this server. Two cases give it. The
/// collector cannot run on the server's engine (an Azure SQL Database has no SQL Server Agent, no default trace, no
/// sys.configurations list and so on). Or the collector has never run for the server, as on AWS RDS, where the collector's
/// AppliesTo rule skips it without writing a row. A grid or chart fed by such a collector is empty for good, not for lack of
/// activity, so it says so in the same words the Default Trace grid, the PostgreSQL panels and the MCP tools' not_collected
/// answer already use.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// The note a surface shows where <paramref name="collectorName"/> cannot run on this server's engine: the sentence
    /// <see cref="CollectorEngineCapability.NotCollectedMessage"/> gives. Null anywhere the collector can run, including a
    /// server whose engine has not been read yet, where the surface keeps its own empty-state text.
    /// </summary>
    internal static string? EngineGapNote(string serverName, int engineEdition, string? engineKind, string collectorName) =>
        CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, collectorName);

    /// <summary>
    /// The text <see cref="CollectorRuntimePrecondition.GatedOffMessage"/> needs for this collector, or null for a collector
    /// that does not use it. Only running_jobs does, with <see cref="CollectorRuntimePrecondition.RunningJobsPossibleCauses"/>:
    /// the text the Darling MCP service's get_running_jobs answer passes, so both say the same thing about AWS RDS.
    /// agent_status has the same AppliesTo rule, but its one viewer surface is the
    /// Job History header chip, which has no empty-state text to hold a note. A collector that runs once at load (server_config,
    /// trace_flags) must never use it: its "no longer invoked" arm compares the collector's last run with the server's last
    /// collection, so a load-time collector would read as switched off a day after it ran.
    /// </summary>
    internal static string? SwitchedOffReasonsFor(string collectorName) =>
        string.Equals(collectorName, "running_jobs", StringComparison.Ordinal) ? CollectorRuntimePrecondition.RunningJobsPossibleCauses : null;

    /// <summary>
    /// The sentence for a collector that has no <c>collection_log</c> row for this server in all retained history, on a server
    /// that has rows from other collectors. It is the one sentence every collector but running_jobs uses.
    /// </summary>
    internal static string NeverRanNote(string serverName, string collectorName) =>
        $"The {collectorName} collector has not run for {serverName}, so this data is not collected for this server.";

    /// <summary>
    /// What a surface's message element shows once its data is bound: the note text, and whether it is visible. The note shows
    /// only when the surface has no rows and either the collector cannot run on this server's engine (the sentence from
    /// <see cref="EngineGapNote"/>, which wins) or the collector has never run for it (<paramref name="collectorNeverRan"/>, with
    /// <paramref name="neverRanNote"/> or <see cref="NeverRanNote"/> as the sentence). A grid that has rows never shows it, because
    /// rows prove the collector ran. On every other server (a collector that ran, or an engine not read yet) the element stays
    /// collapsed, so nothing changes there.
    /// </summary>
    internal static (string Text, Visibility Visibility) EngineGapState(
        string serverName, int engineEdition, string? engineKind, string collectorName, int rowCount,
        bool collectorNeverRan = false, string? neverRanNote = null)
    {
        var note = EngineGapNote(serverName, engineEdition, engineKind, collectorName)
            ?? (collectorNeverRan ? neverRanNote ?? NeverRanNote(serverName, collectorName) : null);

        return (note ?? "", note is not null && rowCount == 0 ? Visibility.Visible : Visibility.Collapsed);
    }

    /// <summary>
    /// Whether a surface must read <c>collection_log</c> at all: only when it has no rows and its engine rule has nothing to say.
    /// With rows the collector plainly ran, and where the engine rule already gives the sentence there is nothing to add.
    /// </summary>
    internal static bool NeedsCollectorRunRead(string serverName, int engineEdition, string? engineKind, string collectorName, int rowCount) =>
        rowCount == 0 && EngineGapNote(serverName, engineEdition, engineKind, collectorName) is null;

    /// <summary>
    /// <see cref="EngineGapState"/> from the facts the viewer reads from <c>collection_log</c>: when the collector last ran
    /// for this server, and when the server last and first collected anything. All null mean no read was made or the read
    /// failed.
    /// <para>running_jobs asks <see cref="CollectorRuntimePrecondition.GatedOffMessage"/>, so its note is the sentence the MCP
    /// service gives for the same facts, first-run grace included. Every other collector shows the note only when it has no row
    /// at all and the server has one. A row of any age means the collector ran, so a collector that runs once at load never
    /// reads as switched off.</para>
    /// </summary>
    internal static (string Text, Visibility Visibility) EngineGapStateFromRuns(
        string serverName, int engineEdition, string? engineKind, string collectorName, int rowCount,
        DateTime? collectorLastRunUtc, DateTime? serverLastCollectedUtc, DateTime? serverFirstCollectedUtc = null)
    {
        string? switchedOff = null;
        bool neverRan;

        if (SwitchedOffReasonsFor(collectorName) is { } reasons)
        {
            switchedOff = CollectorRuntimePrecondition.GatedOffMessage(
                serverName, collectorName, reasons, collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc);
            neverRan = switchedOff is not null;
        }
        else
        {
            neverRan = collectorLastRunUtc is null && serverLastCollectedUtc is not null;
        }

        return EngineGapState(serverName, engineEdition, engineKind, collectorName, rowCount, neverRan, switchedOff);
    }

    /// <summary>
    /// <see cref="EngineGapStateFromRuns"/> for one server, reading <c>collection_log</c> only when
    /// <see cref="NeedsCollectorRunRead"/> says so, and not at all for a collector in <paramref name="collectorsSeenToRun"/>.
    /// A read that fails leaves the surface on its own empty state: this note is a diagnostic, and it must not turn an empty
    /// grid into a failed tab load. A read that sees the collector run adds it to <paramref name="collectorsSeenToRun"/> when
    /// <see cref="RunSettlesNeverRanRule"/> says one run settles it, so a later refresh of the same empty surface makes no
    /// read. <paramref name="readLastRun"/> is <see cref="ViewerDataService.GetCollectorLastRunAsync"/> on the live path.
    /// </summary>
    internal static async Task<(string Text, Visibility Visibility)> ReadEngineGapStateAsync(
        CollectorLastRunReader readLastRun, ISet<string>? collectorsSeenToRun, int serverId, string serverName,
        int engineEdition, string? engineKind, string collectorName, int rowCount)
    {
        DateTime? collectorLastRunUtc = null;
        DateTime? serverLastCollectedUtc = null;
        DateTime? serverFirstCollectedUtc = null;

        if (collectorsSeenToRun?.Contains(collectorName) != true
            && NeedsCollectorRunRead(serverName, engineEdition, engineKind, collectorName, rowCount))
        {
            try
            {
                (collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc) = await readLastRun(serverId, collectorName);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                collectorLastRunUtc = null;
                serverLastCollectedUtc = null;
                serverFirstCollectedUtc = null;
            }

            if (collectorLastRunUtc is not null && RunSettlesNeverRanRule(collectorName))
            {
                collectorsSeenToRun?.Add(collectorName);
            }
        }

        return EngineGapStateFromRuns(
            serverName, engineEdition, engineKind, collectorName, rowCount, collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc);
    }

    /// <summary>
    /// The read of when one collector last ran for one server and when that server last and first collected anything:
    /// <see cref="ViewerDataService.GetCollectorLastRunAsync"/> on the live path, and a counting stand-in in a test, which is how
    /// a test sees whether a read was made at all.
    /// </summary>
    internal delegate Task<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)> CollectorLastRunReader(
        int serverId, string collectorName);

    /// <summary>
    /// Whether one read that saw <paramref name="collectorName"/> run settles the never-ran rule for good. <c>collection_log</c>
    /// only gains rows, so a collector that has run stays "ran". Not so for a collector with a
    /// <see cref="SwitchedOffReasonsFor"/> text (running_jobs): its "no longer invoked" arm compares its last run with the
    /// server's last collection, so it needs a fresh read every time.
    /// </summary>
    internal static bool RunSettlesNeverRanRule(string collectorName) => SwitchedOffReasonsFor(collectorName) is null;

    /// <summary>The collectors a read has seen run for this tab's server. It is used on the UI thread only.</summary>
    private readonly HashSet<string> _collectorsSeenToRun = new(StringComparer.Ordinal);

    private Task<(string Text, Visibility Visibility)> EngineGapStateAsync(string collectorName, int rowCount) =>
        ReadEngineGapStateAsync(
            (serverId, collector) => _dataService.GetCollectorLastRunAsync(serverId, collector), _collectorsSeenToRun,
            _server.ServerId, _server.ServerName, _server.EngineEdition, _server.EngineKind, collectorName, rowCount);

    /// <summary>
    /// Fills a surface's message element once its data is bound, with what <see cref="EngineGapStateFromRuns"/> says for this
    /// server.
    /// </summary>
    private async Task ShowEngineGapAsync(TextBlock message, string collectorName, int rowCount)
    {
        var (text, visibility) = await EngineGapStateAsync(collectorName, rowCount);
        message.Text = text;
        message.Visibility = visibility;
    }

    /// <summary>The words each change grid's empty-state element started with, so a note that stops applying hands them back.</summary>
    private readonly Dictionary<TextBlock, string> _ownNoDataText = [];

    /// <summary>
    /// For an empty-state element that keeps its own words and its own visibility (the change grids): the note's sentence where
    /// <see cref="EngineGapStateFromRuns"/> shows one, and the element's own words otherwise.
    /// </summary>
    private async Task SetChangesNoDataTextAsync(TextBlock message, string collectorName, int rowCount)
    {
        if (!_ownNoDataText.ContainsKey(message))
        {
            _ownNoDataText[message] = message.Text;
        }

        var (text, visibility) = await EngineGapStateAsync(collectorName, rowCount);
        message.Text = visibility == Visibility.Visible ? text : _ownNoDataText[message];
    }
}
