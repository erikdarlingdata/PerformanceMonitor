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
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5329: the Top Queries and Top Procedures grid rows carry NULL, not 0, for the columns the hourly rollups keep no
/// copy of, and a null cell shows blank. A page read from the hourly rollup holds every such column null at once, so
/// the order within a page never mixes blank with numbers; the pins below still cover what a null does when a sort
/// meets it. The live half (the hourly read returns null, the raw read returns values) is
/// <see cref="ViewerHourlyRouteBlankColumnsLiveTests"/>.
/// </summary>
public sealed class ViewerStatsRowBlankCellTests
{
    /// <summary>The Top Queries row's columns the hourly rollup lacks (the grid's XAML binds each one by this name).</summary>
    private static readonly string[] QueryBlankColumns =
    {
        "TotalLogicalReads", "AvgReads", "TotalLogicalWrites", "TotalPhysicalReads", "TotalRows", "TotalSpills",
        "MinPhysicalReads", "MaxPhysicalReads", "MinRows", "MaxRows", "MinSpills", "MaxSpills", "MinDop", "MaxDop",
        "MinGrantKb", "MaxGrantKb", "MinUsedGrantKb", "MaxUsedGrantKb", "MinIdealGrantKb", "MaxIdealGrantKb",
        "MinReservedThreads", "MaxReservedThreads", "MinUsedThreads", "MaxUsedThreads", "TotalClrMs",
        "PlanGenerationNum", "WorkerTimePerSecond",
    };

    private static readonly string[] ProcedureBlankColumns =
    {
        "TotalLogicalReads", "AvgReads", "TotalLogicalWrites", "TotalPhysicalReads", "TotalSpills", "AvgSpills",
        "MinLogicalReads", "MaxLogicalReads", "MinPhysicalReads", "MaxPhysicalReads", "MinLogicalWrites", "MaxLogicalWrites",
        "MinSpills", "MaxSpills",
    };

    [Fact]
    public void AFreshRow_HoldsNullForEveryColumnTheRollupLacks_NotZero()
    {
        foreach (var name in QueryBlankColumns)
        {
            Assert.Null(typeof(ViewerQueryStatsRow).GetProperty(name)!.GetValue(new ViewerQueryStatsRow()));
        }

        foreach (var name in ProcedureBlankColumns)
        {
            Assert.Null(typeof(ViewerProcedureStatsRow).GetProperty(name)!.GetValue(new ViewerProcedureStatsRow()));
        }
    }

    /// <summary>A real zero from the raw route stays a zero: blank means "not kept", 0 means "kept and was 0".</summary>
    [Fact]
    public void ARawRowWithARealZero_StaysZero_AndItsAverageIsStillComputed()
    {
        var q = new ViewerQueryStatsRow { TotalExecutions = 4, TotalLogicalReads = 0, TotalSpills = 0 };
        Assert.Equal(0L, q.TotalLogicalReads);
        Assert.Equal(0.0, q.AvgReads);
        var p = new ViewerProcedureStatsRow { TotalExecutions = 4, TotalLogicalReads = 400 };
        Assert.Equal(100.0, p.AvgReads);
    }

    /// <summary>The grid's cell is a TextBlock bound as <c>{Binding X, StringFormat=N0}</c>: a null shows empty, a number
    /// shows formatted, and a real zero shows 0.</summary>
    [Fact]
    public void ANullCell_ShowsBlank_ANumberShowsFormatted_AZeroShowsZero()
    {
        OnStaThread(() =>
        {
            Assert.Equal("", CellText(new ViewerQueryStatsRow(), "TotalLogicalReads"));
            Assert.Equal("", CellText(new ViewerQueryStatsRow(), "AvgReads"));
            Assert.Equal("", CellText(new ViewerProcedureStatsRow(), "TotalSpills"));
            Assert.Equal("", CellText(new ViewerProcedureStatsRow(), "AvgSpills"));
            Assert.Equal(1234.ToString("N0"), CellText(new ViewerQueryStatsRow { TotalLogicalReads = 1234 }, "TotalLogicalReads"));
            Assert.Equal(0.ToString("N0"), CellText(new ViewerQueryStatsRow { TotalLogicalReads = 0 }, "TotalLogicalReads"));
        });
    }

    /// <summary>A descending sort (the click that ranks by a column) puts the blank rows after every number, so a null
    /// is never ranked as if it were the biggest value; and a page that is blank throughout keeps the order the read gave it.</summary>
    [Fact]
    public void ADescendingSort_PutsBlankRowsLast_AndAllBlankKeepsTheReadsOrder()
    {
        OnStaThread(() =>
        {
            var mixed = new List<ViewerQueryStatsRow>
            {
                new() { QueryHash = "blank-1" },
                new() { QueryHash = "big", TotalLogicalReads = 900 },
                new() { QueryHash = "blank-2" },
                new() { QueryHash = "small", TotalLogicalReads = 5 },
            };
            var view = new ListCollectionView(mixed);
            view.SortDescriptions.Add(new SortDescription("TotalLogicalReads", ListSortDirection.Descending));
            Assert.Equal(new[] { "big", "small", "blank-1", "blank-2" }, view.Cast<ViewerQueryStatsRow>().Select(r => r.QueryHash).ToArray());

            var allBlank = new List<ViewerQueryStatsRow>
            {
                new() { QueryHash = "a" }, new() { QueryHash = "b" }, new() { QueryHash = "c" },
            };
            foreach (var direction in new[] { ListSortDirection.Descending, ListSortDirection.Ascending })
            {
                var blankView = new ListCollectionView(allBlank);
                blankView.SortDescriptions.Add(new SortDescription("TotalLogicalReads", direction));
                Assert.Equal(new[] { "a", "b", "c" }, blankView.Cast<ViewerQueryStatsRow>().Select(r => r.QueryHash).ToArray());
            }
        });
    }

    /// <summary>A binding that names a fallback or a null value (<c>TargetNullValue=0</c>) would turn a blank back into a
    /// false 0, so none of the blank-able columns may carry one, on either grid.</summary>
    [Fact]
    public void NoBlankableColumnBinding_NamesATargetNullValueOrAFallback()
    {
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var checkedBindings = 0;
        foreach (var name in QueryBlankColumns.Concat(ProcedureBlankColumns).Distinct())
        {
            foreach (Match m in Regex.Matches(xaml, @"\{Binding " + name + @"[,}][^{}]*\}"))
            {
                checkedBindings++;
                Assert.DoesNotContain("TargetNullValue", m.Value, StringComparison.Ordinal);
                Assert.DoesNotContain("FallbackValue", m.Value, StringComparison.Ordinal);
            }
        }

        Assert.True(checkedBindings >= 20, $"the scan must find the grids' bindings, found {checkedBindings}");
    }

    private static string CellText(object row, string path)
    {
        var cell = new TextBlock { DataContext = row };
        cell.SetBinding(TextBlock.TextProperty, new Binding(path) { StringFormat = "N0" });
        return cell.Text;
    }

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
}
