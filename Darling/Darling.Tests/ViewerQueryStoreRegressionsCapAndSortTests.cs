/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9889: the Viewer's Query Store Regressions grid opens in the order its caption names (Total Impact, largest first, "ranked by total
/// added duration"), keeps a sort the user chose across a reload, and says so when its read stopped at the row cap. Each test builds a real
/// server tab on the shared STA thread, hands it the rows of a read the way the load does (<see cref="ViewerServerTab.ShowQueryStoreRegressionsRows"/>
/// then <see cref="ViewerServerTab.SetQueryStoreRegressionsDefaultSort"/>, the loader's two steps after its read) and reads the grid and the note off the controls.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerQueryStoreRegressionsCapAndSortTests
{
    private static readonly int Cap = ViewerDataService.QueryStoreRegressionsRowCap;

    /// <summary>
    /// Three rows whose order by duration change percent is not their order by total impact: the biggest percent has a small impact, the
    /// biggest impact has a small percent, which is the shape the walk saw.
    /// </summary>
    private static List<ViewerQueryStoreRegressionRow> RowsWhosePercentAndImpactDisagree(long? total = null) =>
    [
        new() { QueryId = 1, DurationRegressionPercent = 5000, AdditionalDurationMs = 30_000, TotalCount = total },
        new() { QueryId = 2, DurationRegressionPercent = 100, AdditionalDurationMs = 900_000, TotalCount = total },
        new() { QueryId = 3, DurationRegressionPercent = -5, AdditionalDurationMs = 200_000, TotalCount = total },
    ];

    private static List<ViewerQueryStoreRegressionRow> RowsUpTo(int count, long? total) =>
        Enumerable.Range(1, count)
            .Select(i => new ViewerQueryStoreRegressionRow { QueryId = i, AdditionalDurationMs = 1_000 * (count - i + 1), DurationRegressionPercent = i, TotalCount = total })
            .ToList();

    private static ViewerQueryStoreRegressionsRead Read(List<ViewerQueryStoreRegressionRow> rows) => new(rows, null);

    private sealed record Shown(long[] QueryIds, string? SortPath, ListSortDirection? SortDirection, string? SortedColumnBinding, ListSortDirection? SortedColumnDirection, int SortCount, string? Note);

    /// <summary>What the grid and the note show after the read lands and the default sort is applied.</summary>
    private static Shown After(Action<ViewerServerTab>? beforeTheLoad, params ViewerQueryStoreRegressionsRead[] reads)
    {
        Shown? shown = null;
        RunThemed(() =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "regressions-cap-test", "regressions-cap-test", true, null));
            var grid = (DataGrid)tab.FindName("QueryStoreRegressionsGrid");
            var note = (TextBlock)tab.FindName("QueryStoreRegressionsCapNote");
            for (var i = 0; i < reads.Length; i++)
            {
                if (i == reads.Length - 1)
                {
                    beforeTheLoad?.Invoke(tab);
                }

                tab.ShowQueryStoreRegressionsRows(reads[i]);
                tab.SetQueryStoreRegressionsDefaultSort();
            }

            var sorts = grid.Items.SortDescriptions;
            var sorted = grid.Columns.FirstOrDefault(c => c.SortDirection is not null);
            shown = new Shown(
                grid.Items.Cast<ViewerQueryStoreRegressionRow>().Select(r => r.QueryId).ToArray(),
                sorts.Count > 0 ? sorts[0].PropertyName : null,
                sorts.Count > 0 ? sorts[0].Direction : null,
                (sorted as DataGridBoundColumn)?.Binding is System.Windows.Data.Binding b ? b.Path.Path : sorted?.SortMemberPath,
                sorted?.SortDirection,
                sorts.Count,
                note.Visibility == Visibility.Visible ? note.Text : null);
        });
        return shown!;
    }

    [Fact]
    public void TheGrid_OpensByTotalImpactLargestFirst_TheOrderItsCaptionNames()
    {
        var shown = After(null, Read(RowsWhosePercentAndImpactDisagree()));

        Assert.Equal("AdditionalDurationMs", shown.SortPath);
        Assert.Equal(ListSortDirection.Descending, shown.SortDirection);
        /* The rows run down in total impact (query 2, 3, 1), not in duration change percent (1, 2, 3). */
        Assert.Equal(new long[] { 2, 3, 1 }, shown.QueryIds);
        /* The Total Impact column carries the arrow, so the header says what the order is. */
        Assert.Equal("AdditionalDurationMs", shown.SortedColumnBinding);
        Assert.Equal(ListSortDirection.Descending, shown.SortedColumnDirection);
    }

    [Fact]
    public void AUserSort_SurvivesTheNextRead()
    {
        var userSort = new SortDescription("DurationRegressionPercent", ListSortDirection.Ascending);
        var shown = After(
            tab =>
            {
                /* What a click on a column header does to the grid: it replaces the view's sort. */
                var sorts = ((DataGrid)tab.FindName("QueryStoreRegressionsGrid")).Items.SortDescriptions;
                sorts.Clear();
                sorts.Add(userSort);
            },
            Read(RowsWhosePercentAndImpactDisagree()),
            Read(RowsWhosePercentAndImpactDisagree()));

        Assert.Equal(1, shown.SortCount);
        Assert.Equal("DurationRegressionPercent", shown.SortPath);
        Assert.Equal(ListSortDirection.Ascending, shown.SortDirection);
        /* Ascending by percent: query 3 (-5), 2 (100), 1 (5000). */
        Assert.Equal(new long[] { 3, 2, 1 }, shown.QueryIds);
    }

    [Fact]
    public void ARead_ThatStoppedAtTheCap_ShowsTheNoteOnTheTab_WithTheCount()
    {
        var shown = After(null, Read(RowsUpTo(Cap, total: 312)));

        Assert.NotNull(shown.Note);
        Assert.StartsWith($"Showing the top {Cap} of 312", shown.Note, StringComparison.Ordinal);
        Assert.Contains("total added duration", shown.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void ARead_ThatStoppedAtTheCap_WithoutACount_ShowsTheNoteOnTheTab_WithoutOne()
    {
        var shown = After(null, Read(RowsUpTo(Cap, total: null)));

        Assert.NotNull(shown.Note);
        Assert.StartsWith($"Showing the top {Cap}, ranked by total added duration", shown.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void ARead_UnderTheCap_ShowsNoNoteOnTheTab()
    {
        Assert.Null(After(null, Read(RowsUpTo(Cap - 1, total: Cap - 1))).Note);
        Assert.Null(After(null, Read([])).Note);
    }

    [Fact]
    public void TheNote_GoesAway_WhenTheNextReadIsUnderTheCap()
    {
        var shown = After(null, Read(RowsUpTo(Cap, total: 312)), Read(RowsUpTo(7, total: 7)));

        Assert.Null(shown.Note);
        Assert.Equal(7, shown.QueryIds.Length);
    }

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    /// <summary>Runs the body on the shared STA thread with the Viewer's Dark theme merged at application level, as App.xaml supplies it (the tabs' static resources need it), and takes it back out.</summary>
    private static void RunThemed(Action body) => StaTestThread.Run(() =>
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var theme = new ResourceDictionary { Source = ThemeUri("Dark") };
        app.Resources.MergedDictionaries.Add(theme);
        try
        {
            body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Remove(theme);
        }
    });
}
