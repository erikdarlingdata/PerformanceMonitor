/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A per-database run whose database list comes back empty records SUCCESS with 0 rows. Without a note that reads
/// as "nothing ran". Two things empty the list: every user database on a logical server is monitored as its own
/// server (the long-query read leaves those to their own registrations), or every database is excluded. The run
/// keeps SUCCESS and carries a note that says why nothing was read (#4961). The note itself is pinned for real
/// here and in <see cref="EmptyDatabaseListNoteLiveTests"/>; the wiring is pinned at the source, the way the
/// per-database read of the long-query trace already is.
/// </summary>
public sealed class EmptyDatabaseListNoteTests
{
    [Fact]
    public void TheNotes_AreFixedText_InProductWords()
    {
        Assert.Equal("no database read: every user database is monitored as its own server", EmptyDatabaseListNote.EverySeparatelyMonitored);
        Assert.Equal("no database read: every database is excluded", EmptyDatabaseListNote.EveryExcluded);
    }

    [Theory]
    [InlineData(EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData(EmptyDatabaseListNote.EveryExcluded)]
    public void AServerWhoseEveryRunCarriesTheNote_ReadsItWithItsRunCount_AndNoQualifier(string note)
    {
        /* index_object_stats is a collector the health bands expect to find user databases for, so the missing
           qualifier comes from the note, not from the collector being left out of that list. */
        Assert.True(CollectorHealthClassifier.ExpectsUserDatabases("index_object_stats"));
        Assert.DoesNotContain(CollectorHealthClassifier.EmptyEnumerationMarker, note, StringComparison.Ordinal);
        Assert.False(CollectorHealthClassifier.HasMeasurements(note));

        var formatted = CollectorHealthClassifier.FormatCollectionNote(
            note, noteCount: 96, totalRuns: 96, collectorName: "index_object_stats", targetHasUserDatabases: true);

        Assert.Equal(note + " (all 96 runs)", formatted);
        Assert.DoesNotContain(CollectorHealthClassifier.HasUserDatabasesQualifier, formatted, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, 0, false, false, false, null)]
    [InlineData(0, 0, false, true, false, EmptyDatabaseListNote.EveryExcluded)]
    [InlineData(0, 0, true, true, false, EmptyDatabaseListNote.EveryExcluded)]
    [InlineData(0, 0, false, true, true, null)]
    [InlineData(2, 0, true, false, false, EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData(2, 0, true, true, true, EmptyDatabaseListNote.EverySeparatelyMonitored)]
    [InlineData(2, 1, true, false, false, null)]
    [InlineData(2, 2, false, true, false, null)]
    public void TheNote_NamesTheReasonTheListEmptied_AndNothingItCannotName(
        int listed, int read, bool skipsSeparatelyMonitored, bool exclusionsConfigured, bool databaseScoped, string? expected)
    {
        Assert.Equal(expected, EmptyDatabaseListNote.For(listed, read, skipsSeparatelyMonitored, exclusionsConfigured, databaseScoped));
    }

    [Fact]
    public void TheRunner_TakesTheNoteFromTheSharedRule_AndCarriesNoTextOfItsOwn()
    {
        var runner = ReadLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        Assert.Contains("EmptyDatabaseListNote.For(", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("no database read", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// The assignment that follows the per-database loop is unconditional, so a note assigned anywhere earlier
    /// would be erased by it. The note rides in that assignment instead.
    /// </summary>
    [Fact]
    public void TheNote_IsMergedIntoTheAssignmentThatFollowsThePerDatabaseLoop()
    {
        var runner = ReadLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        var loopEnd = runner.IndexOf("await PrunePgPerDatabaseStateAsync(server.ServerId, definition.Name, databases, cancellationToken);", StringComparison.Ordinal);
        Assert.True(loopEnd > 0, "the per-database loop is followed by the per-database state prune");
        var assignment = runner.IndexOf("collectionNote = EnumeratedCollectorDriver.MergeNotes(", loopEnd, StringComparison.Ordinal);
        Assert.True(assignment > loopEnd, "the run note is assigned after the per-database loop");

        var statement = runner[assignment..runner.IndexOf(';', assignment)];
        Assert.Contains("emptyListNote", statement, StringComparison.Ordinal);
    }

    private static string ReadLf(params string[] relativePath)
    {
        var path = Path.Combine(relativePath);
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, path)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, path)).Replace("\r\n", "\n");
    }
}
