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
using System.Windows.Data;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6104: the PostgreSQL grids bind string-only display rows, so their size, duration, age, count and percent columns
/// sorted by characters ("9 MB" above "1.2 GB", "100.0%" below "9.0%"). These tests build the real server tab, map store
/// rows through the real <see cref="PgDisplay"/> mappers, take the column the user clicks and sort the rows the way the
/// grid's own collection view does, so they fail on the column's sort key and on the mapper's raw number, not on a copy
/// of either.
/// </summary>
public sealed class ViewerPostgresGridNumericSortTests
{
    private static readonly DateTime Utc = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);

    private const long Kb = 1024;
    private const long Mb = 1024 * Kb;
    private const long Gb = 1024 * Mb;

    /* The tab's XAML resolves its styles (NumericCell and the rest) from the application's merged theme, as the viewer's
       App.xaml supplies it, so the test host gets the same dictionary before the tab is built. */
    private static ViewerServerTab NewTab()
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        if (!app.Resources.Contains("NumericCell"))
        {
            app.Resources.MergedDictionaries.Add(new ResourceDictionary
            {
                Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
            });
        }

        return new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "pg-sort-test", "pg-sort-test", true, null, engineKind: "postgres"));
    }

    private static DataGridTextColumn Column(DataGrid grid, string header) =>
        grid.Columns.OfType<DataGridTextColumn>().Single(c => c.Header is string text && text == header);

    private static List<T> Sorted<T>(DataGridTextColumn column, IEnumerable<T> rows, ListSortDirection direction = ListSortDirection.Ascending)
    {
        Assert.False(string.IsNullOrEmpty(column.SortMemberPath), $"the '{column.Header}' column has no SortMemberPath, so it sorts by its text.");
        var view = new ListCollectionView(rows.ToList());
        view.SortDescriptions.Add(new SortDescription(column.SortMemberPath, direction));
        return view.Cast<T>().ToList();
    }

    private static DarlingPgSlotReader.PgSlotRow Slot(string name, long retained, long safe, long firstRetained) =>
        new(name, Utc, "logical", "pgoutput", "appdb", true, "reserved", retained, safe, 1, 1, null, null, false, firstRetained, Utc);

    [Fact]
    public void SizeColumns_SortByBytes_NotByTheFormattedText_AndTheSentinelSortsBelowEverySize()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            var rows = new[]
            {
                PgDisplay.Slot(Slot("nineMb", 9 * Mb, 9 * Mb, 9 * Mb)),
                PgDisplay.Slot(Slot("oneGb", (long)(1.2 * Gb), PgDisplay.NotApplicable, (long)(1.2 * Gb))),
                PgDisplay.Slot(Slot("fiveHundredKb", 500 * Kb, 2 * Gb, 500 * Kb)),
            };

            Assert.Equal(new[] { "fiveHundredKb", "nineMb", "oneGb" }, Sorted(Column(tab.PgReplicationSlotsGrid, "Retained WAL"), rows).Select(r => r.SlotName));

            /* "Not applicable" is the dash, not a size: it sorts apart from every real size instead of
               landing wherever the text "—" falls among the digits. */
            Assert.Equal(new[] { "oneGb", "nineMb", "fiveHundredKb" }, Sorted(Column(tab.PgReplicationSlotsGrid, "Safe WAL Left"), rows).Select(r => r.SlotName));
            Assert.Null(rows[1].SafeWalSizeSort);
        });
    }

    [Fact]
    public void TrendColumn_SortsBySignedByteChange()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            var rows = new[]
            {
                PgDisplay.Slot(Slot("grewNineMb", 10 * Mb, 1, 1 * Mb)),
                PgDisplay.Slot(Slot("shrankOneGb", 1 * Gb, 1, 2 * Gb)),
                PgDisplay.Slot(Slot("grewOneGb", 2 * Gb, 1, 1 * Gb)),
            };

            Assert.Equal(new[] { "shrankOneGb", "grewNineMb", "grewOneGb" }, Sorted(Column(tab.PgReplicationSlotsGrid, "Retained Trend"), rows).Select(r => r.SlotName));
        });
    }

    private static DarlingPgSessionStatesReader.PgSessionStateRow Session(int pid, long xactMs, long backendMs) =>
        new(pid, pid, "appdb", "app", "psql", "127.0.0.1", "client backend", "idle in transaction", null, null, null, null,
            Utc, Utc, 10, 2, 1, xactMs, xactMs, xactMs, backendMs, -1, 5, 5, false, 10, 1, 1, 10, false);

    [Fact]
    public void DurationAndAgeColumns_SortByMilliseconds_NotByTheFormattedText()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            var rows = new[]
            {
                PgDisplay.SessionState(Session(1, 9_000, 9_000)),
                PgDisplay.SessionState(Session(2, (5 * 3600 + 45 * 60) * 1000L, (5 * 3600 + 45 * 60) * 1000L)),
                PgDisplay.SessionState(Session(3, 999, 999)),
                PgDisplay.SessionState(Session(4, 125_000, 125_000)),
                PgDisplay.SessionState(Session(5, PgDisplay.NotApplicable, PgDisplay.NotApplicable)),
            };

            /* 999 ms, 00:09, 02:05 and 0.05:45:00 are not in text order; the unmeasured one is first, not a duration. */
            var expected = new[] { 5, 3, 1, 4, 2 };
            Assert.Equal(expected, Sorted(Column(tab.PgSessionStatesGrid, "Peak Xact"), rows).Select(r => r.Pid));
            Assert.Equal(expected, Sorted(Column(tab.PgSessionStatesGrid, "Backend Age"), rows).Select(r => r.Pid));
            Assert.Equal(expected.Reverse(), Sorted(Column(tab.PgSessionStatesGrid, "Peak Xact"), rows, ListSortDirection.Descending).Select(r => r.Pid));
        });
    }

    [Fact]
    public void ShareColumn_SortsByTheFraction_NotByTheText()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            /* "9 of 10 samples" sorts after "10 of 10 samples" as text. */
            var rows = new[]
            {
                PgDisplay.SessionState(Session(1, 1, 1) with { SampleCount = 10, HorizonHolderSamples = 9 }),
                PgDisplay.SessionState(Session(2, 1, 1) with { SampleCount = 10, HorizonHolderSamples = 10 }),
                PgDisplay.SessionState(Session(3, 1, 1) with { SampleCount = 100, HorizonHolderSamples = 5 }),
            };

            Assert.Equal(new[] { 3, 1, 2 }, Sorted(Column(tab.PgSessionStatesGrid, "Held Horizon In"), rows).Select(r => r.Pid));
        });
    }

    private static DarlingPgIoReader.PgIoRow Io(string name, long reads, long hits, bool bytesTracked, long opBytes, decimal readBytes) =>
        new(name, "relation", "normal", reads, 10, hits, 0, 0, 0, 0, 0, 0, opBytes, false, null, readBytes, 0, 0, bytesTracked);

    [Fact]
    public void VolumeAndPercentColumns_SortByTheirNumbers_AndNotMeasuredIsBlankNotZero()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            var rows = PgDisplay.IoRows(new[]
            {
                Io("nineMb", 1, 9, true, 8192, 9 * Mb),
                Io("oneGb", 1, 99, true, 8192, (decimal)(1.2 * Gb)),
                Io("notMeasured", 1, 100, false, 0, 0),
                Io("estimated", 1, 1, false, 8192, 0),
            });

            /* The estimate (8 KB, from one read at 8192 bytes) is a number; "not measured" has none. */
            Assert.Equal(new[] { "notMeasured", "estimated", "nineMb", "oneGb" }, Sorted(Column(tab.PgIoStatsGrid, "Read Volume"), rows).Select(r => r.BackendType));
            Assert.Null(rows.Single(r => r.BackendType == "notMeasured").ReadVolumeSort);

            /* 50.0% / 90.0% / 99.0% / 99.0%: "100.0%"-style and single-digit percents are not in text order. */
            Assert.Equal(new[] { "estimated", "nineMb", "oneGb", "notMeasured" }, Sorted(Column(tab.PgIoStatsGrid, "Hit %"), rows).Select(r => r.BackendType));
        });
    }

    [Fact]
    public void CountColumn_SortsByTheNumber_NotByTheText()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            var rows = new[]
            {
                PgDisplay.Wait(new DarlingPgWaitReader.PgWaitRow("Lock", "9", 9, 1, 1)),
                PgDisplay.Wait(new DarlingPgWaitReader.PgWaitRow("Lock", "1204", 1204, 1, 1)),
                PgDisplay.Wait(new DarlingPgWaitReader.PgWaitRow("Lock", "100", 100, 1, 1)),
            };

            Assert.Equal(new[] { "9", "100", "1204" }, Sorted(Column(tab.PgWaitStatsGrid, "Waits"), rows).Select(r => r.WaitEvent));
        });
    }

    private static DarlingPgBlockingReader.PgBlockingChainRow Chain(long rootBackendId, long xactMs, long? samples) =>
        new(Utc, rootBackendId, 1, new[] { "appdb" }, "u", "app", "active", "select 1", false, xactMs, xactMs, 1, 1, 1, xactMs, null, samples, false, false);

    [Fact]
    public void BlockingChainColumns_SortByTheirNumbers_AndUnknownIsBlankNotZero()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            var rows = new[]
            {
                PgDisplay.Chain(Chain(7, 9_000, 9)),
                PgDisplay.Chain(Chain(7, 125_000, null)),
                PgDisplay.Chain(Chain(7, 999, 100)),
            };

            Assert.Equal(new long?[] { 999, 9_000, 125_000 }, Sorted(Column(tab.PgBlockingChainsGrid, "In Xact For"), rows).Select(r => r.RootXactDurationSort));
            /* "unknown" (the blocker had left pg_stat_activity) has no number, so it is not counted as zero or as 9. */
            Assert.Equal(new long?[] { null, 9, 100 }, Sorted(Column(tab.PgBlockingChainsGrid, "Seen As Root"), rows).Select(r => r.SamplesAsRootSort));
        });
    }

    /// <summary>#6135: the index bloat grid showed 22487040 where every other size reads "21.4 MB". The column shows the
    /// converted size and still orders by the byte count the row holds.</summary>
    [Fact]
    public void IndexBloatSizes_ShowInHumanUnits_AndSortByBytes()
    {
        StaTestThread.Run(() =>
        {
            var tab = NewTab();
            static DarlingPgIndexBloatReader.PgIndexBloatRow Bloat(string name, long bytes, long? reclaimable) => new(
                "appdb", "public", "orders", name, bytes, null, null, null, null, null, reclaimable, null,
                "estimated", 12.5, null, null, null, null, null, null, Utc);

            var rows = new[]
            {
                PgDisplay.IndexBloat(Bloat("big", 22_487_040, 5_000_000)),
                PgDisplay.IndexBloat(Bloat("small", 9_437_184, null)),
                PgDisplay.IndexBloat(Bloat("huge", 3 * Gb, 1 * Gb)),
            };

            var size = Column(tab.PgIndexBloatGrid, "Size");
            Assert.Equal(new[] { "small", "big", "huge" }, Sorted(size, rows).Select(r => r.IndexName));

            var shown = ((System.Windows.Data.Binding)size.Binding).Converter.Convert(rows[0].IndexBytes, typeof(string), null!, System.Globalization.CultureInfo.CurrentCulture);
            Assert.Equal(PgDisplay.Bytes(22_487_040), shown);
            Assert.DoesNotContain("22487040", (string)shown, StringComparison.Ordinal);

            var reclaimable = Column(tab.PgIndexBloatGrid, "Reclaimable (est.)");
            var convert = ((System.Windows.Data.Binding)reclaimable.Binding).Converter;
            Assert.Equal(PgDisplay.Bytes(5_000_000), convert.Convert(rows[0].EstimatedReclaimableBytes, typeof(string), null!, System.Globalization.CultureInfo.CurrentCulture));
            Assert.Equal("", convert.Convert(rows[1].EstimatedReclaimableBytes, typeof(string), null!, System.Globalization.CultureInfo.CurrentCulture));
            Assert.Equal(new[] { "small", "big", "huge" }, Sorted(reclaimable, rows).Select(r => r.IndexName));
        });
    }
}
