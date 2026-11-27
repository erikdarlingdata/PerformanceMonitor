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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9604: a compaction row of the Store Health history shows the size part of its pass in the Moved cell and the rest of the pass in the
/// Message cell. The Moved cell stays short, and between the two cells no part of the pass's summary is lost.
/// </summary>
public sealed class StoreHealthHistoryCellsTests
{
    private static readonly DateTime T0 = new(2026, 11, 25, 22, 17, 0, DateTimeKind.Utc);

    /// <summary>A compaction pass that did every part of its work, so its summary has every part, with the largest figures a real store reaches.</summary>
    private static readonly CompactionPassDetail FullPass = new(
        T0.AddMinutes(-9), 56.1, GroupsMerged: 12, FilesIn: 12_345, FilesOut: 1_234, BytesIn: 987_654_000_000, BytesOut: 876_543_000_000,
        SortedAtSmallGroupBatches: 3, UnsortedFallbackBatches: 2, FilesRecut: 40, RecutSkippedLiveJournal: 1, RecutSkippedTimeBudget: 5,
        RecutSkips: new Dictionary<string, int> { [ArchiveService.RecutSkipReason.OverBatchBudget.ToString()] = 1, [ArchiveService.RecutSkipReason.LockTimeout.ToString()] = 2 },
        ViewRebuildMs: 768, ProcessPeakWorkingSetBytes: 1_434_200_000, DuckDbPeakMemoryBytes: 488_800_000);

    private static StoreHousekeepingEvent Compaction(string? message = null) =>
        new(T0, StoreHousekeepingHistory.KindCompaction, StoreHousekeepingHistory.OutcomeOk, UsedMb: 321, Message: message, Compaction: FullPass);

    /// <summary>The words of a text, with the brackets, commas and semicolons around them taken off, in a fixed order, so two texts that say the same words in a different order are equal.</summary>
    private static List<string> Words(string text) =>
        text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
            .Select(w => w.Trim('(', ')', ',', ';', '.'))
            .Where(w => w.Length > 0)
            .OrderBy(w => w, StringComparer.Ordinal)
            .ToList();

    [Fact]
    public void ACompactionRowsMovedText_IsShort_WhileItsWholeSummaryIsLong()
    {
        var moved = StoreHealthText.MovedText(Compaction());

        Assert.True(moved.Length <= 80, $"the Moved text is {moved.Length} characters: {moved}");
        Assert.True(StoreHealthText.CompactionSummary(FullPass).Length > 3 * moved.Length, "the pass has a long summary, so the cell would have been long");
    }

    [Fact]
    public void ACompactionRows_MovedAndMessage_CarryEveryPartOfTheSummary()
    {
        var row = Compaction();

        var cells = Words(StoreHealthText.MovedText(row) + " " + StoreHealthText.MessageText(row));

        Assert.Equal(Words(StoreHealthText.CompactionSummary(FullPass)), cells);
    }

    [Fact]
    public void ACompactionRowThatHasItsOwnMessage_CarriesTheSummaryNotesFirst_ThenTheMessage()
    {
        const string Own = "stopped early because the disk was nearly full";
        var row = Compaction(Own);

        var message = StoreHealthText.MessageText(row);

        Assert.StartsWith(StoreHealthText.CompactionNotes(FullPass), message, StringComparison.Ordinal);
        Assert.EndsWith(Own, message, StringComparison.Ordinal);
        Assert.Equal(
            Words(StoreHealthText.CompactionSummary(FullPass) + " " + Own),
            Words(StoreHealthText.MovedText(row) + " " + message));
    }

    [Fact]
    public void ARowOfAnotherKind_KeepsItsWorkTextAndItsMessageAsTheyWere()
    {
        var archive = new StoreHousekeepingEvent(T0, StoreHousekeepingHistory.KindArchive, StoreHousekeepingHistory.OutcomeIncomplete, Rows: 12_345, Files: 9, Message: "two files were busy");
        var retention = new StoreHousekeepingEvent(T0, StoreHousekeepingHistory.KindRetention, StoreHousekeepingHistory.OutcomeOk, Files: 3);

        Assert.Equal(StoreHealthText.WorkSummary(archive), StoreHealthText.MovedText(archive));
        Assert.Equal("two files were busy", StoreHealthText.MessageText(archive));
        Assert.Equal(StoreHealthText.WorkSummary(retention), StoreHealthText.MovedText(retention));
        Assert.Equal("", StoreHealthText.MessageText(retention));
    }

    [Fact]
    public void ACompactionRowWithNoPassDetail_ReadsAsAnyOtherRow()
    {
        var row = new StoreHousekeepingEvent(T0, StoreHousekeepingHistory.KindCompaction, StoreHousekeepingHistory.OutcomeFailed, Message: "the pass did not start");

        Assert.Equal(StoreHealthText.WorkSummary(row), StoreHealthText.MovedText(row));
        Assert.Equal("the pass did not start", StoreHealthText.MessageText(row));
    }
}
