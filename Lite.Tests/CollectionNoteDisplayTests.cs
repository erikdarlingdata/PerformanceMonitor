// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7670: the Collection Health grid's Note column shows a collector's counts in words, as the web page does, so a latest
/// run's "shred_gated=1" is never printed raw. The MCP note_summary text keeps the counts as stored.
/// </summary>
public sealed class CollectionNoteDisplayTests
{
    [Fact]
    public void AGridRow_ShowsTheShredGateInWords_WhileNoteSummaryKeepsTheRawCounts()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "deadlocks",
            TotalRuns = 12,
            LastNote = "shred_gated=1 events_read=0",
            NoteCount = 12,
        };

        Assert.Equal("latest run: shred gated: 1 (no new events since the last run, so the XML was not read), events read: 0", row.NoteFormatted);
        Assert.Equal("latest run: shred_gated=1 events_read=0", CollectorHealthClassifier.FormatCollectionNote(row.LastNote, row.NoteCount, row.TotalRuns, row.CollectorName));
    }

    [Fact]
    public void AGridRow_WithAPlainNote_KeepsItExactly()
    {
        var row = new CollectorHealthRow { CollectorName = "wait_stats", TotalRuns = 12, LastNote = "a plain note", NoteCount = 2 };

        Assert.Equal("a plain note (2 of 12 runs)", row.NoteFormatted);
    }
}
