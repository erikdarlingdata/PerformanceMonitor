/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4766: the viewer grid columns that bound a DateTime with a XAML <c>StringFormat</c> (FinOps First Seen, Last Seen,
/// Inventory As Of and Last Collected; the Query Store clutter grid's Options Captured; the overhead grid's Last Seen).
/// A DateTime already converted to the display clock cannot say which of the two 01:30s of the repeated autumn hour it
/// was, and a XAML format cannot add an offset. Each row now keeps the UTC instant beside the converted DateTime and words
/// a text property from it through <see cref="ViewerTimeHelper.FormatForDisplay(DateTime, string)"/>; the column binds the
/// text and sorts by the DateTime, so the order stays chronological.
///
/// <para>US Eastern falls back at 2026-11-01 06:00Z, so 05:30Z is the first 01:30 (-04:00) and 06:30Z the second (-05:00).</para>
/// </summary>
/* Serialized with the other classes that flip the process-wide ViewerTimeHelper statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerGridTimeTextZoneTests
{
    private const string ViewerFolder = "PerformanceMonitor.Darling.Viewer";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern => ServerClock.Resolve("Eastern Standard Time", -300);

    /* Runs the body in the given display mode and server clock, on the invariant culture; all three are restored. */
    private static void WithDisplay(TimeDisplayMode mode, ServerClock clock, Action body)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        var savedCulture = CultureInfo.CurrentCulture;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.ActiveServerClock = clock;
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

            body();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
            CultureInfo.CurrentCulture = savedCulture;
        }
    }

    /// <summary>
    /// A FinOps Application Connections row: First Seen at 05:30Z and Last Seen at 06:30Z read 01:30 -04:00 and 01:30 -05:00
    /// in Server mode on a US Eastern clock, the two DateTimes the columns sort by are told apart only by the text, and the
    /// hour after is the bare wall time. UTC mode is the stored instant.
    /// </summary>
    [Fact]
    public void AFinOpsApplicationConnectionRow_InTheRepeatedHour_ReadsItsOffset()
    {
        var row = new ApplicationConnectionRow
        {
            FirstSeenUtc = Naive(2026, 11, 1, 5, 30),
            LastSeenUtc = Naive(2026, 11, 1, 6, 30),
        };
        var after = new ApplicationConnectionRow { FirstSeenUtc = Naive(2026, 11, 1, 7, 30), LastSeenUtc = Naive(2026, 11, 1, 7, 30) };

        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal("2026-11-01 01:30 -04:00", row.FirstSeenText);
            Assert.Equal("2026-11-01 01:30 -05:00", row.LastSeenText);
            Assert.Equal("2026-11-01 02:30", after.FirstSeenText);
        });

        WithDisplay(TimeDisplayMode.UTC, Eastern, () =>
        {
            Assert.Equal("2026-11-01 05:30", row.FirstSeenText);
            Assert.Equal("2026-11-01 06:30", row.LastSeenText);
        });
    }

    /// <summary>
    /// A FinOps inventory row: Inventory As Of and Last Collected read the offset in the repeated hour, and a server with no
    /// snapshot or no collection shows an empty cell, as the XAML-formatted column did.
    /// </summary>
    [Fact]
    public void AFinOpsInventoryRow_InTheRepeatedHour_ReadsItsOffset_AndAMissingTimeIsEmpty()
    {
        var row = new ServerPropertyRow
        {
            InventoryAsOfUtc = Naive(2026, 11, 1, 5, 30),
            LastCollectedUtc = Naive(2026, 11, 1, 6, 30),
        };
        var missing = new ServerPropertyRow();

        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal("2026-11-01 01:30 -04:00", row.InventoryAsOfText);
            Assert.Equal("2026-11-01 01:30 -05:00", row.LastCollectedText);
            Assert.Equal("", missing.InventoryAsOfText);
            Assert.Equal("", missing.LastCollectedText);
        });
    }

    /// <summary>
    /// The Query Store clutter grid: a database row's Options Captured and an overhead row's Last Seen read the offset in the
    /// repeated hour, and a database with no options row has no text at all, so the binding's <c>TargetNullValue</c> still
    /// draws the em-dash.
    /// </summary>
    [Fact]
    public void AClutterRow_InTheRepeatedHour_ReadsItsOffset_AndNoOptionsRowHasNoText()
    {
        var database = new ViewerDataService.QueryStoreClutterRow { OptionsCapturedUtc = Naive(2026, 11, 1, 6, 30) };
        var noOptions = new ViewerDataService.QueryStoreClutterRow();
        var wait = new ViewerDataService.QueryStoreOverheadWaitRow { LastObservedUtc = Naive(2026, 11, 1, 5, 30) };

        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal("2026-11-01 01:30 -05:00", database.OptionsCapturedText);
            Assert.Null(noOptions.OptionsCapturedText);
            Assert.Equal("2026-11-01 01:30 -04:00", wait.LastObservedText);
        });
    }

    /// <summary>
    /// Each column is a WPF column this suite does not instantiate, so the wiring is a source pin: the column binds its text
    /// property (no <c>StringFormat</c>, which cannot carry the offset), sorts by the row's UTC DateTime (the display-zone one
    /// puts the second pass of the repeated hour before the first, see <see cref="ViewerGridTimeColumnSortMemberTests"/>),
    /// and, where its header carries a filter button, keeps that button's tag on the display-zone DateTime property.
    /// </summary>
    [Theory]
    [InlineData("FinOpsTab.xaml", "FirstSeenText", "FirstSeenUtc", "FirstSeenLocal", true)]
    [InlineData("FinOpsTab.xaml", "LastSeenText", "LastSeenUtc", "LastSeenLocal", true)]
    [InlineData("FinOpsTab.xaml", "InventoryAsOfText", "InventoryAsOfUtc", "InventoryAsOf", true)]
    [InlineData("FinOpsTab.xaml", "LastCollectedText", "LastCollectedUtc", "LastCollected", true)]
    [InlineData("ViewerServerTab.xaml", "OptionsCapturedText", "OptionsCapturedUtc", "OptionsCaptured", false)]
    [InlineData("ViewerServerTab.xaml", "LastObservedText", "LastObservedUtc", "LastObserved", false)]
    public void EachColumn_BindsItsText_SortsByItsUtcDateTime_AndFiltersOnTheDateTime(
        string file, string textProperty, string sortMember, string dateProperty, bool hasFilterButton)
    {
        var xaml = ReadRepoFile("Darling", ViewerFolder, file);

        /* The element's start tag, whatever the order of its attributes; the binding may carry only a TargetNullValue. */
        var start = Regex.Match(xaml,
            @"<DataGridTextColumn(?=[^>]*\sBinding=""\{Binding " + textProperty + @"(?:, TargetNullValue=[^,}]+)?\}"")"
            + @"(?=[^>]*\sSortMemberPath=""" + sortMember + @""")[^>]*>");
        Assert.True(start.Success, $"{file}: the column bound to {textProperty} and sorted by {sortMember} is not where this pin looks.");

        Assert.DoesNotContain("StringFormat", start.Value, StringComparison.Ordinal);

        if (hasFilterButton)
        {
            var end = xaml.IndexOf("</DataGridTextColumn>", start.Index, StringComparison.Ordinal);
            Assert.True(end > start.Index, $"{file}: the end of the {textProperty} column was not found.");
            Assert.Contains($"Tag=\"{dateProperty}\"", xaml[start.Index..end], StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The readers set the UTC instant beside each converted DateTime, so a row can never show the default 0001-01-01.
    /// </summary>
    [Theory]
    [InlineData("ViewerDataService.FinOps.Workload.cs", "FirstSeenUtc = reader.GetDateTime(18)")]
    [InlineData("ViewerDataService.FinOps.Workload.cs", "LastSeenUtc = reader.GetDateTime(19)")]
    [InlineData("ViewerDataService.FinOps.Inventory.cs", "InventoryAsOfUtc = reader.IsDBNull(13) ? null : reader.GetDateTime(13)")]
    [InlineData("ViewerDataService.FinOps.Inventory.cs", "LastCollectedUtc = reader.IsDBNull(19) ? null : reader.GetDateTime(19)")]
    [InlineData("ViewerDataService.QueryStoreClutter.cs", "OptionsCapturedUtc = c?.CapturedAt")]
    [InlineData("ViewerDataService.QueryStoreClutter.cs", "LastObservedUtc = w.LastObserved")]
    public void EachReader_SetsTheUtcInstant_BesideTheConvertedDateTime(string file, string assignment)
    {
        Assert.Contains(assignment, ReadRepoFile("Darling", ViewerFolder, file), StringComparison.Ordinal);
    }
}
