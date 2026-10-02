/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4231, the second slice: the WPF Queries tab's three raw-table grids (Top Queries, Top Procedures, Query
/// Store) and the web viewer's twin panels disclose a truncated window the same way <c>get_query_store_top</c>
/// does over MCP (#2364) — through the shared <see cref="PerformanceMonitor.Darling.Storage.RawWindowFloor"/>
/// probe, never a hand-copied <c>SELECT MIN(collection_time)</c>, and a "Showing since &lt;effective start&gt;"
/// grid-header banner (desktop) / <c>truncation_note</c> strip (web) when the raw tier does not reach back as
/// far as the window asked for.
/// </summary>
[Collection("gap-cache-serial")]
public sealed class RawWindowFloorViewerPortTests
{
    private static readonly DateTime RequestedStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void UpdateTruncationBanner_ShowsSinceEffectiveStart_WhenTheFloorIsPastTheSlack()
    {
        OnStaThread(() =>
        {
            var banner = new TextBlock();
            var floor = RequestedStart.AddDays(3);

            ViewerServerTab.UpdateTruncationBanner(banner, floor, RequestedStart);

            Assert.Equal(Visibility.Visible, banner.Visibility);
            Assert.StartsWith("Showing since ", banner.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void UpdateTruncationBanner_StaysCollapsed_WhenTheFloorIsInsideTheSlack()
    {
        OnStaThread(() =>
        {
            /* 30 minutes after the requested start — inside DurationTrendRouting.TruncationSlack's 90-minute
               allowance, so a normal cadence-start lag, not a retention cut (the #2364 / #4231 ruling). Seeded
               Visible/non-empty first so a no-op bug (never touching Visibility) cannot pass by accident. */
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
            var floor = RequestedStart.AddMinutes(30);

            ViewerServerTab.UpdateTruncationBanner(banner, floor, RequestedStart);

            Assert.Equal(Visibility.Collapsed, banner.Visibility);
        });
    }

    [Fact]
    public void UpdateTruncationBanner_StaysCollapsed_WhenTheProbeFoundNoFloorAtAll()
    {
        OnStaThread(() =>
        {
            var banner = new TextBlock();

            ViewerServerTab.UpdateTruncationBanner(banner, null, RequestedStart);

            Assert.Equal(Visibility.Collapsed, banner.Visibility);
        });
    }

    /// <summary>WPF objects require STA; same shape as Lite.Tests' MainWindowAccessKeyTests / Dashboard.Tests'
    /// DataGridExport tests.</summary>
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    [Theory]
    [InlineData("ViewerDataService.QueryStats.cs", "RawWindowFloor.Table.QueryStats")]
    [InlineData("ViewerDataService.ProcedureStats.cs", "RawWindowFloor.Table.ProcedureStats")]
    [InlineData("ViewerDataService.QueryStore.cs", "RawWindowFloor.Table.QueryStoreStats")]
    public void EveryQueriesTabGridRead_RoutesItsFloorThroughTheSharedHelper(string file, string table)
    {
        var source = ViewerFile(file);
        Assert.Contains($"RawWindowFloor.GetAsync(_dataSource, {table},", source, StringComparison.Ordinal);
        /* Never a second, hand-rolled floor query beside the shared probe — the defect #2364 would have
           repeated twice more, and #4231's DarlingDataReader-side pin (RawWindowFloorSharedHelperSourcePinTests)
           makes the same check on the MCP reader. */
        Assert.DoesNotContain("SELECT MIN(collection_time)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void UpdateTruncationBanner_TableServed_NamesTheEffectiveStartAndTheBound_AndTheRawSlicerFloor()
    {
        OnStaThread(() =>
        {
            var wideStart = RequestedStart.AddDays(1);
            var plan = new QueryStoreIntervalWide.WideReadPlan(
                true, RequestedStart.AddDays(4), wideStart, wideStart, QueryStoreIntervalWide.WideStartBound.FilledSince);
            var banner = new TextBlock();

            ViewerServerTab.UpdateTruncationBanner(banner, RequestedStart.AddDays(4), RequestedStart, widePlan: plan);

            Assert.Equal(Visibility.Visible, banner.Visibility);
            Assert.StartsWith("Showing since ", banner.Text, StringComparison.Ordinal);
            Assert.Contains("(interval table complete from then)", banner.Text, StringComparison.Ordinal);
            Assert.Contains(" · slicer since ", banner.Text, StringComparison.Ordinal);

            var purge = plan with { StartBound = QueryStoreIntervalWide.WideStartBound.TablePurgeEdge };
            ViewerServerTab.UpdateTruncationBanner(banner, null, RequestedStart, widePlan: purge);
            Assert.Contains("(interval table keeps 9 days)", banner.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("slicer", banner.Text, StringComparison.Ordinal);

            /* The grid shows the full window but raw (the slicer's tier) is cut: the shortfall is named. */
            var whole = plan with { ReadStart = RequestedStart, StartBound = QueryStoreIntervalWide.WideStartBound.Window };
            ViewerServerTab.UpdateTruncationBanner(banner, RequestedStart.AddDays(4), RequestedStart, widePlan: whole);
            Assert.Equal(Visibility.Visible, banner.Visibility);
            Assert.StartsWith("Slicer since ", banner.Text, StringComparison.Ordinal);
            Assert.EndsWith(" (the grid shows the full window)", banner.Text, StringComparison.Ordinal);

            ViewerServerTab.UpdateTruncationBanner(banner, RequestedStart, RequestedStart, widePlan: whole);
            Assert.Equal(Visibility.Collapsed, banner.Visibility);
        });
    }

    [Fact]
    public void UpdateTruncationBanner_WideBranch_NamesTheSlicerFloor_WhetherOrNotTheGridWasCut()
    {
        var tab = ViewerFile("ViewerServerTab.Queries.cs");
        var start = tab.IndexOf("if (widePlan?.EffectiveStart is DateTime wideStart)", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var branch = tab[start..tab.IndexOf("return;", start, StringComparison.Ordinal)];
        Assert.Contains("var slicerTruncated = RawWindowFloor.IsTruncated(floor, requestedStartUtc);", branch, StringComparison.Ordinal);
        Assert.Contains("if (wideTruncated || slicerTruncated || !string.IsNullOrEmpty(tierSuffix))", branch, StringComparison.Ordinal);
        Assert.Contains("Slicer since {slicerSince} (the grid shows the full window)", branch, StringComparison.Ordinal);
        Assert.Contains(" · slicer since {slicerSince}", branch, StringComparison.Ordinal);
        /* The slicer text is never gated on the grid's own truncation. */
        Assert.DoesNotContain("wideTruncated && RawWindowFloor.IsTruncated(floor", branch, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreGrid_BindsThePlansReadStart_AndPassesThePlanToTheBanner()
    {
        var data = ViewerFile("ViewerDataService.QueryStore.cs");
        Assert.Contains("QueryStoreIntervalWide.ResolveReadAsync(", data, StringComparison.Ordinal);
        Assert.Contains("TypedValue = DateTime.SpecifyKind(plan.ReadStart, DateTimeKind.Unspecified)", data, StringComparison.Ordinal);
        Assert.DoesNotContain("clampedStart", data, StringComparison.Ordinal);
        Assert.Contains("GetQueryStoreTopQueriesWithReachAsync(", data, StringComparison.Ordinal);

        var tab = ViewerFile("ViewerServerTab.Queries.cs");
        Assert.Contains("GetQueryStoreTopQueriesWithReachAsync(", tab, StringComparison.Ordinal);
        Assert.Contains("UpdateTruncationBanner(QueryStoreTruncationBanner, await floorTask, startUtc, widePlan: widePlan)", tab, StringComparison.Ordinal);
        Assert.Contains("QueryStoreIntervalWide.BannerReason(widePlan.Value.StartBound)", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("interval table keeps 9 days", tab, StringComparison.Ordinal);
        Assert.Contains(" · slicer since ", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void DurationTrend_StaysOnTheClamp()
    {
        var trends = ViewerFile("ViewerDataService.QueryTrends.cs");
        Assert.Contains("Stays on the clamp (ReadsTableAsync, not ResolveReadAsync)", trends, StringComparison.Ordinal);
    }

    [Fact]
    public void QueriesLoadPath_ReadsEveryGridsFloor_BesideItsRows()
    {
        var source = ViewerFile("ViewerServerTab.Queries.cs");
        Assert.Contains("_dataService.GetQueryStatsWindowFloorAsync(", source, StringComparison.Ordinal);
        Assert.Contains("_dataService.GetProcedureStatsWindowFloorAsync(", source, StringComparison.Ordinal);
        Assert.Contains("_dataService.GetQueryStoreWindowFloorAsync(", source, StringComparison.Ordinal);
    }

    private static string ServerTabsJs => ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

    private static string ViewTemplatesJs => ReadRepoFileLf(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "view-templates.js");

    /// <summary>
    /// The web viewer's disclosure goes through panels.js's existing #3278 <c>noteKey</c> opt-in
    /// (<c>loadPanel</c>: a server-authored string at <c>noteKey</c> renders as a notice strip above the
    /// table). Every panel over the three raw-only tools must name <c>"truncation_note"</c> — counted, not
    /// merely <c>Contains</c>-checked, so a panel added later without the key fails here instead of shipping
    /// quiet: the CPU tab's Top Queries + Top Procedures and the Query Store tab's Top Procedures + Query
    /// Store, four table() calls in server-tabs.js, plus the two matching panels in the starter dashboard
    /// template (view-templates.js). <c>topQueriesPanel</c> (the Query Store tab's drill-down composite for
    /// get_top_queries_by_cpu) calls VIZ.table directly rather than through loadPanel, so it renders the note
    /// itself off <c>res.data.truncation_note</c> instead of a <c>noteKey</c> string.
    /// </summary>
    [Fact]
    public void WebViewer_CarriesTheTruncationNoteKey_OnEveryRawTopPanel()
    {
        Assert.Equal(4, CountOf(ServerTabsJs, "\"truncation_note\""));
        Assert.Contains("res.data.truncation_note", ServerTabsJs, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "read-fields.js"), "noteKey: \"truncation_note\""));
        Assert.Equal(2, CountOf(ViewTemplatesJs, "...READ_FIELDS.get_top_"));
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = haystack.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }
        return count;
    }
}
