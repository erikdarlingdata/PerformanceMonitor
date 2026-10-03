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
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Query Store Clutter sub-tab (#3797) — the desktop surface of <c>get_query_store_clutter</c>, sitting
/// after Query Store Regressions in the Queries group because it answers the question those grids raise:
/// which DATABASE is making the Query Store catalog expensive, and why.
///
/// <para><b>One read, three renderings.</b> The grid, the recommendation pane and the per-server overhead
/// block all come from a single <see cref="ViewerDataService.GetQueryStoreClutterAsync"/> call over the
/// toolbar's window — the same statements and the same composition the MCP tool and the web panel use. No
/// slicer and no comparison: this is a verdict over the whole window, not a series over one metric, which is
/// the same reason Query Store Regressions and Plan Corrections beside it have neither.</para>
///
/// <para><b>Clutter is per DATABASE and overhead is per SERVER</b>, and the layout keeps them apart: the
/// grid is databases, the expander below it is the instance-wide <c>QDS_*</c> waits and the Query Store
/// memory clerk, labelled as per-server, and nothing attributes one to the other.</para>
///
/// <para><b>An excluded replica is still a row.</b> The Excluded / Excluded Because columns carry the
/// architectural reason on the database's own line; a filtered-out replica would leave an operator who knows
/// the database exists unable to tell "not cluttered" from "not shown" (topology ruling, 2026-09-20).</para>
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// Loads the clutter grid, the per-server overhead block and the server-level note over the toolbar's
    /// window. The shell's <c>LoadInnerTabAsync</c> owns the try/catch that surfaces failures.
    /// </summary>
    private async Task LoadQueryStoreClutterAsync(DateTime startUtc, DateTime endUtc)
    {
        /* #4966: the probe starts beside the read, and both take the window the caller worked out once. The panel draws from two ranged
           sources, the plan churn from query_store_stats and the read cost from the collection log, so the coverage rule applies to each
           and the note names the later of the two starts; the config and memory-clerk sections show the newest snapshot only, and the
           rows are per database and name no time. */
        /* #5022: two reads in flight, the panel's own (its five arms one after another) and the probe (its two calls one after another).
           The shell's LoadInnerTabAsync declares no width, so this load does, before either starts. */
        using var readFanOut = ViewerReadFanOut.Of(2);

        var dataStartTask = _dataService.GetQueryStoreClutterDataStartAsync(_server.ServerId, startUtc, endUtc);
        var readTask = _dataService.GetQueryStoreClutterAsync(
            _server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);

        /* A read that throws skips the banner step below, so the probe would go unawaited: the helper watches it then, and a probe that
           fails later is a warning and not an unobserved-exception Error. */
        await AwaitReadWatchingProbeAsync(readTask, dataStartTask, "Query Store Clutter");
        var result = readTask.Result;

        /* The read is done: end the declared width here, before the banner step below awaits the probe. */
        readFanOut.Release();

        _queryStoreClutterFilterMgr!.UpdateData(result.Databases);

        /* NO default sort, unlike the grids beside this one, and that is the correct answer rather than an
           omission. The composition orders rows worst-first by BAND, then the read-cost share, then plans per
           query — three keys a DataGrid's single-column SortDescription cannot express. The nearest
           single-column stand-in would be the Verdict text descending, which sorts alphabetically and puts
           Warning above Critical: a default sort that silently reverses the ranking the rows arrived in. The
           arrival order IS the ranking, and it is left alone until the user sorts a column themselves. */

        QueryStoreOverheadGrid.ItemsSource = result.Overhead.Waits;
        QueryStoreOverheadNote.Text =
            result.Overhead.MemoryClerk
            + " Sleep waits excluded at collection: "
            + result.Overhead.ExcludedWaitTypes
            + ". The proxy is the non-sleep QDS_* waits plus the clerk, and it is a proxy, not the whole of Query Store's cost.";

        var serverNote = result.Overhead.ServerNote;
        QueryStoreClutterServerNote.Text = serverNote ?? string.Empty;
        QueryStoreClutterServerNote.Visibility = serverNote is null ? Visibility.Collapsed : Visibility.Visible;

        /* A reload keeps whichever row is still selected explained, and clears the pane when the selection
           did not survive the refresh — a stale paragraph under a different row is worse than no paragraph. */
        ShowSelectedQueryStoreClutterRecommendation();
        await ShowQueryStoreClutterDataStartAsync(QueryStoreClutterTruncationBanner, dataStartTask, startUtc);
    }

    /// <summary>
    /// Raises or hides the "Showing since" banner of the Query Store Clutter panel (#4966), through the shared step the other server-tab
    /// grids use: the probe's answer (<paramref name="probe"/>, started beside the read:
    /// <see cref="ViewerDataService.GetQueryStoreClutterDataStartAsync"/>, the later of the plan churn's and the read cost's starts)
    /// is compared with the window's start, so a range that reaches before it names where it starts and a covered range names
    /// nothing. The probe is awaited through <see cref="DataStartOrNullAsync"/>, so a probe that throws costs this banner and nothing
    /// after it. <c>internal static</c> so the store-backed tests run the same step as the tab.
    /// </summary>
    /// <param name="banner">The panel's banner.</param>
    /// <param name="probe">The panel's data-start probe, started beside its read.</param>
    /// <param name="startUtc">The start of the window the panel just drew, the same one its read and its probe took.</param>
    internal static async Task ShowQueryStoreClutterDataStartAsync(TextBlock banner, Task<DateTime?> probe, DateTime startUtc) =>
        UpdateTruncationBanner(banner, await DataStartOrNullAsync(probe, "Query Store Clutter"), startUtc);

    /// <summary>Selecting a database shows its recommendation whole — the composition's sentences, never
    /// re-worded here, so this pane and the MCP payload say the same thing.</summary>
    private void QueryStoreClutterGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, QueryStoreClutterGrid))
        {
            return;
        }

        ShowSelectedQueryStoreClutterRecommendation();
    }

    private void ShowSelectedQueryStoreClutterRecommendation()
    {
        var row = QueryStoreClutterGrid.SelectedItem as ViewerDataService.QueryStoreClutterRow;
        QueryStoreClutterRecommendation.Text = row is null || string.IsNullOrWhiteSpace(row.Recommendation)
            ? "Select a database above for its recommendation."
            : row.DatabaseName + ": " + row.Recommendation;
    }
}
