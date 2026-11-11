/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8663 and #8664: the PostgreSQL Storage page gives each grid room for several rows instead of whatever the wrapped notes leave, and the
/// Vacuum session grid's text cells end in an ellipsis with the whole value in a tooltip. Each fact builds the real tab on the shared STA thread.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerPostgresStorageAndVacuumGridsTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    private static readonly string[] StorageGrids =
    {
        "PgTableBloatGrid", "PgIndexUsageGrid", "PgColumnStatsGrid", "PgIndexBloatGrid", "PgPredicateStatsGrid",
    };

    [Fact]
    public void EveryStorageGrid_HasRoomForSeveralRows_InAPageThatScrolls()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", () =>
        {
            var found = new List<string>();
            var tab = NewTab();
            foreach (var name in StorageGrids)
            {
                var grid = (DataGrid)tab.FindName(name)!;
                var rows = ((Grid)grid.Parent).RowDefinitions;
                if (!rows[Grid.GetRow(grid)].Height.IsAuto)
                {
                    found.Add($"{name} sits in a star row, so the notes decide its height");
                }

                /* A header is about 30 px and a row about 25, so 200 px is a header and six rows. */
                if (grid.MinHeight < 200)
                {
                    found.Add($"{name} may shrink to {grid.MinHeight} px, a header and fewer than six rows");
                }

                if (grid.MaxHeight > 600)
                {
                    found.Add($"{name} has no inner scroll of its own ({grid.MaxHeight} px)");
                }
            }

            var scroll = (ScrollViewer)tab.FindName("PgStorageScroll")!;
            if (!ReferenceEquals(scroll.Content, ((DataGrid)tab.FindName("PgTableBloatGrid")!).Parent))
            {
                found.Add("the Storage grids are not inside the page's ScrollViewer");
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheVacuumSessionGrid_TrimsItsTextCellsWithAnEllipsisAndATooltip()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", () =>
        {
            var found = new List<string>();
            var tab = NewTab();
            var trimmed = (Style)tab.FindResource("TrimmedCell");
            var grid = (DataGrid)tab.FindName("PgSessionStatesGrid")!;
            foreach (var header in new[] { "Backend", "Backend Type", "Application", "Last Wait" })
            {
                var column = grid.Columns.Single(c => string.Equals(c.Header as string, header, StringComparison.Ordinal));
                if (!ReferenceEquals(((DataGridTextColumn)column).ElementStyle, trimmed))
                {
                    found.Add($"the {header} column does not use the trimmed cell style");
                }
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    private static ViewerServerTab NewTab() => new(
        new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
        new DarlingServer(1, "grid-test", "grid-test", true, null));

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>
        {
            new() { Source = new Uri(ViewerPack + theme + ".xaml") },
            new() { Source = ThemeManager.SharedStylesUri },
        };
        foreach (var dictionary in added)
        {
            app.Resources.MergedDictionaries.Add(dictionary);
        }

        try
        {
            return body();
        }
        finally
        {
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        }
    }
}
