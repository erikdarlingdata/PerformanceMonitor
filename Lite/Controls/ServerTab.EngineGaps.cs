/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// Fills the empty-state note of a grid or chart whose collector cannot run on this server's engine. The note is
    /// the sentence <see cref="EngineGapNote"/> returns, the same one the MCP tools return as <c>not_collected</c>.
    /// It shows only when the collector cannot run here and the surface has no rows. On every other server the
    /// message stays collapsed and the surface looks as it always did.
    /// </summary>
    /// <param name="message">The surface's <c>NoDataMessage</c> element, in the same cell as the grid or chart.</param>
    /// <param name="collectorName">The collector that feeds the surface, as the collector catalog names it.</param>
    /// <param name="rowCount">How many rows (or points) the surface is showing.</param>
    private void ShowEngineGap(TextBlock message, string collectorName, int rowCount)
    {
        var gap = EngineGapNote(_server.DisplayName, _isAzureSqlDatabase, collectorName);

        if (gap is not null)
            message.Text = gap;

        message.Visibility = gap is not null && rowCount == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>
    /// Whether the Running Jobs tab shows its "grant the login access to msdb" warning. It shows when the login lacks
    /// msdb access and the running_jobs collector can run on this server's engine. On an Azure SQL Database the
    /// collector does not run at all, so no grant could help, and the not-collected note is all the tab says.
    /// </summary>
    internal static Visibility RunningJobsMsdbWarningVisibility(bool hasMsdbAccess, bool isAzureSqlDatabase)
    {
        var edition = isAzureSqlDatabase ? CollectorEngineCapability.AzureSqlDatabaseEngineEdition : CollectorEngineCapability.UnknownEngineEdition;

        return !hasMsdbAccess && CollectorEngineCapability.IsCollectedOnEngineEdition("running_jobs", edition)
            ? Visibility.Visible
            : Visibility.Collapsed;
    }
}
