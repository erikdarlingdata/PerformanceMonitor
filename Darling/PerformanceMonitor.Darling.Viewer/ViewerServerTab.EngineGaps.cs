/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The "not collected here" note for a surface whose collector cannot run on the monitored engine. A grid or chart fed by
/// such a collector (an Azure SQL Database has no SQL Server Agent, no default trace, no sys.configurations list and so on)
/// is empty for good, not for lack of activity, so it says so in the same words the Default Trace grid, the PostgreSQL
/// panels and the MCP tools' not_collected answer already use.
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
    /// What a surface's message element shows once its data is bound: the note text, and whether it is visible. The note is
    /// <see cref="EngineGapNote"/>'s sentence, and it shows only when the collector cannot run on this server and the
    /// surface has no rows. On every other server (an on-premises server, Managed Instance, or an engine not read yet) the
    /// text is empty and the element stays collapsed, so nothing changes there.
    /// </summary>
    internal static (string Text, Visibility Visibility) EngineGapState(
        string serverName, int engineEdition, string? engineKind, string collectorName, int rowCount)
    {
        var gap = EngineGapNote(serverName, engineEdition, engineKind, collectorName);

        return (gap ?? "", gap is not null && rowCount == 0 ? Visibility.Visible : Visibility.Collapsed);
    }

    /// <summary>
    /// Fills a surface's message element once its data is bound, with what <see cref="EngineGapState"/> says for this
    /// server.
    /// </summary>
    private void ShowEngineGap(TextBlock message, string collectorName, int rowCount)
    {
        var (text, visibility) = EngineGapState(_server.ServerName, _server.EngineEdition, _server.EngineKind, collectorName, rowCount);
        message.Text = text;
        message.Visibility = visibility;
    }
}
