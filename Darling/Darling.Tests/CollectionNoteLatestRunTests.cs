/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A measuring collector leaves label=value counts on EVERY run, but the health read keeps only the
/// newest run's note. Rendered as "counts (all N runs)" the newest cycle's figures read as the whole
/// window's. A note that carries counts is labelled as the latest run's and takes no run-count qualifier;
/// a plain note keeps its qualifiers exactly.
/// </summary>
public class CollectionNoteLatestRunTests
{
    private const string Counts = "shred_gated=2 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0";

    private static readonly CollectorMeasurement[] Measured =
    {
        new("shred_gated", 2),
        new("events_read", 0),
        new("events_stored", 0),
    };

    [Fact]
    public void Counts_Only_Note_On_All_Runs_Reads_As_The_Latest_Run()
    {
        var text = CollectorHealthClassifier.FormatCollectionNote(Counts, 174, 174, "blocked_process_report");

        Assert.Equal("latest run: " + Counts, text);
        Assert.DoesNotContain("runs)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Counts_Only_Note_On_Some_Runs_Reads_As_The_Latest_Run()
    {
        var text = CollectorHealthClassifier.FormatCollectionNote(Counts, 3, 174, "deadlocks");

        Assert.Equal("latest run: " + Counts, text);
    }

    [Fact]
    public void Host_Note_Plus_Counts_Reads_As_The_Latest_Run()
    {
        var note = CollectorMeasurementNote.Compose("budget abandoned 2 databases", Measured)!;

        var text = CollectorHealthClassifier.FormatCollectionNote(note, 96, 96, "deadlocks");

        Assert.Equal("latest run: " + note, text);
        Assert.DoesNotContain("runs)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Counts_Note_Skips_The_Empty_Enumeration_Qualifier_Too()
    {
        var note = CollectorMeasurementNote.Compose(EnumeratedCollectorDriver.EmptyEnumerationMessage, Measured)!;

        var text = CollectorHealthClassifier.FormatCollectionNote(note, 96, 96, "query_store", targetHasUserDatabases: true);

        Assert.Equal("latest run: " + note, text);
    }

    [Theory]
    [InlineData("a plain note", 3L, 96L, "a plain note (3 of 96 runs)")]
    [InlineData("a plain note", 96L, 96L, "a plain note (all 96 runs)")]
    [InlineData("1 of 3 databases failed: x=1 was denied", 96L, 96L, "1 of 3 databases failed: x=1 was denied (all 96 runs)")]
    public void Plain_Notes_Keep_Their_Run_Count_Qualifier(string note, long noteCount, long totalRuns, string expected)
    {
        Assert.Equal(expected, CollectorHealthClassifier.FormatCollectionNote(note, noteCount, totalRuns));
    }

    [Fact]
    public void Empty_Enumeration_Hint_Is_Unchanged()
    {
        var text = CollectorHealthClassifier.FormatCollectionNote(
            EnumeratedCollectorDriver.EmptyEnumerationMessage, 96, 96, "query_store", targetHasUserDatabases: true);

        Assert.Equal(
            EnumeratedCollectorDriver.EmptyEnumerationMessage + " (all 96 runs, "
                + CollectorHealthClassifier.HasUserDatabasesQualifier + ")",
            text);
    }

    [Fact]
    public void Note_Summary_Shape_Is_The_Latest_Run_Label_On_Every_Surface()
    {
        /* The MCP tools build a row's note_summary through exactly this call (source-pinned in
           DarlingEmptyEnumerationNoteTests for both SKUs); the grid row's property is the same call, so a
           row whose last_note carries counts must produce the labelled text there and in note_summary. */
        var row = new PerformanceMonitor.Darling.Viewer.CollectorHealthRow
        {
            CollectorName = "blocked_process_report",
            TotalRuns = 174,
            LastNote = Counts,
            NoteCount = 174,
        };

        Assert.Equal("latest run: " + Counts, row.NoteFormatted);
        Assert.Equal(
            row.NoteFormatted,
            CollectorHealthClassifier.FormatCollectionNote(row.LastNote, row.NoteCount, row.TotalRuns, row.CollectorName, row.TargetHasUserDatabases));
    }

    [Fact]
    public void Both_Mcp_Descriptions_Carry_The_Latest_Run_Clause()
    {
        const string clause = "when last_note carries label=value counts it is the newest run's counts, not a window total";
        foreach (var relative in new[]
        {
            Path.Combine("Lite", "Mcp", "McpHealthTools.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"),
        })
        {
            Assert.Contains(clause, ReadRepoFile(relative), StringComparison.Ordinal);
        }
    }
}
