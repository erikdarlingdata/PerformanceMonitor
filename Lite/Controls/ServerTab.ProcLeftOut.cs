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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab
{
    /// <summary>
    /// #6651 (#5633 item 4): the Top Procedures grid's read. With no database chosen the ranking leaves out the databases the server
    /// DROPPED, the way get_top_procedures_by_cpu does (the exclusion is in the statement, so the top-N is not filled by a dropped
    /// database). A database the user names is read as asked, and an unknown set (no recent snapshot) leaves out nothing. A custom
    /// range or slicer window that ended in the past is anchored at its end, as the MCP tool's as_of read is, so a database dropped
    /// since still ranks for a window it existed in.
    /// </summary>
    private async Task<(List<ProcedureStatsRow> Rows, IReadOnlyList<string>? LeftOut)> ReadTopProceduresAsync(
        int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (startUtc, endUtc) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        var chosen = SelectedDatabaseFilter;
        /* Both reads run on the pool (#5457), so this is awaited from the UI thread directly. */
        var leftOut = await Task.Run(() => _dataService.GetDatabasesLeftOutOfRankingAsync(_serverId, chosen, startUtc, endUtc));
        var rows = await Task.Run(() => QueryTagGrid.ReadAsync(_dataService, _serverId, _queryTagChoice,
            tags => _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, ServerClock, chosen, excludeDatabaseNames: leftOut, tags: tags)));
        return (rows, leftOut);
    }

    /// <summary>
    /// #6651: says what the Top Procedures read left out, in the words the Darling Viewer, the web page and get_top_procedures_by_cpu use.
    /// An EMPTY page says only that no procedure ran in a database the server still has, from the collector's own run history, never
    /// "nothing was recorded". Collapsed when nothing was left out.
    /// </summary>
    private async Task UpdateProcStatsLeftOutBannerAsync(int rowCount, IReadOnlyList<string>? leftOut, DateTime startUtc, DateTime endUtc)
    {
        CollectorRunFacts? facts = null;
        if (rowCount == 0 && leftOut is { Count: > 0 })
        {
            facts = await Task.Run(() => _dataService.GetCollectorRunFactsAsync(_serverId, "procedure_stats", window: (startUtc, endUtc)));
        }

        var text = DatabasesLeftOut.ProcedureBanner(leftOut, rowCount, facts, (int)Math.Max(1, Math.Round((endUtc - startUtc).TotalHours)));
        ProcStatsLeftOutBanner.Text = text ?? "";
        ProcStatsLeftOutBanner.Visibility = text is null ? Visibility.Collapsed : Visibility.Visible;
    }
}
