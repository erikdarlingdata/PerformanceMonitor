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
        Assert.Equal(2, CountOf(ViewTemplatesJs, "noteKey: \"truncation_note\""));
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
