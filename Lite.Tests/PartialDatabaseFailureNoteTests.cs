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
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2623: a per-database collector that fails in SOME databases and succeeds in the rest must say so.
///
/// <para>
/// Both runners tolerate a per-database failure by design — one offline database must not cost the other
/// twenty-nine — and both escalate only when EVERY database failed. In between sat a hole with no
/// evidence in it at all: the cycle recorded SUCCESS, whatever the survivors produced, and a note
/// composed solely from probe failures, which a thrown exception is not.
/// </para>
///
/// <para>
/// That hole is how #2622 stayed alive. Three collectors were writing one fewer payload value than they
/// declared, so every row they produced was rejected. All three are per-database. All three failed
/// identically in the one database that had data. Only <c>pg_extension_availability</c> surfaced as an
/// ERROR, and only because it returns rows in every database including <c>postgres</c>, so all-failed
/// tripped the escalation. The other two had a second database with legitimately nothing to report, it
/// succeeded, and the cycle logged SUCCESS with zero rows — indistinguishable from a target that has no
/// large tables, which is what I assumed it was.
/// </para>
///
/// <para>
/// So the assertions here are about the ABSENCE being explained, not about the failure being prevented.
/// Skipping the database is still correct. Skipping it quietly is what turned a fixable bug into three
/// schema versions of a plausible-looking empty table.
/// </para>
/// </summary>
public class PartialDatabaseFailureNoteTests
{
    [Fact]
    public void APartialLossComposesANoteNamingWhatWasSkipped()
    {
        var note = EnumeratedCollectorDriver.BuildPartialFailureNote(
            failed: 1,
            attempted: 2,
            failedDatabases: new[] { "appdb" },
            firstError: "Collector wrote 9 payload values but declares 10 payload columns");

        Assert.NotNull(note);
        Assert.Contains("1 of 2", note, StringComparison.Ordinal);
        Assert.Contains("appdb", note, StringComparison.Ordinal);
        Assert.Contains("declares 10 payload columns", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The note's whole job. A row count is the first thing an operator reads off this cycle, and on a
    /// partial loss it is a number about the survivors wearing the shape of a number about the server.
    /// </summary>
    [Fact]
    public void TheNoteWarnsThatTheRowCountIsAboutTheSurvivorsOnly()
    {
        var note = EnumeratedCollectorDriver.BuildPartialFailureNote(
            failed: 1, attempted: 2, failedDatabases: new[] { "appdb" }, firstError: "boom");

        Assert.NotNull(note);
        Assert.Contains("survivors ONLY", note, StringComparison.Ordinal);
    }

    [Fact]
    public void NothingFailedComposesNoNote()
        => Assert.Null(EnumeratedCollectorDriver.BuildPartialFailureNote(
            failed: 0, attempted: 5, failedDatabases: Array.Empty<string>(), firstError: null));

    /// <summary>
    /// EVERYTHING failing is not this note's case: that path rethrows the first failure so the run is
    /// classified (PERMISSIONS / SESSION_MISSING / ERROR), and a note on a row about to carry an error
    /// message would only compete with it.
    /// </summary>
    [Fact]
    public void EverythingFailingComposesNoNoteBecauseItRethrowsInstead()
        => Assert.Null(EnumeratedCollectorDriver.BuildPartialFailureNote(
            failed: 4, attempted: 4, failedDatabases: new[] { "a", "b", "c", "d" }, firstError: "boom"));

    /// <summary>
    /// One login problem can fail every database on a busy server. The note column is a one-line summary,
    /// so the names are capped and the remainder counted — the opposite trade from the probe-failure note,
    /// which carries no names at all, because there the names are the same problem repeated.
    /// </summary>
    [Fact]
    public void ManyFailedDatabasesAreCappedAndCounted()
    {
        var many = Enumerable.Range(1, 30).Select(i => $"db{i}").ToArray();

        var note = EnumeratedCollectorDriver.BuildPartialFailureNote(
            failed: many.Length, attempted: many.Length + 1, failedDatabases: many, firstError: "boom");

        Assert.NotNull(note);
        Assert.Contains("db1", note, StringComparison.Ordinal);
        Assert.Contains($"and {many.Length - EnumeratedCollectorDriver.MaxNamedFailedDatabases} more", note, StringComparison.Ordinal);
        Assert.DoesNotContain("db30", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// A cycle can lose databases to thrown exceptions AND report probe failures. Two independent sources,
    /// one note column: whichever host wrote its own join would be the one that silently dropped one.
    /// </summary>
    [Fact]
    public void BothNoteSourcesSurviveTheMerge()
    {
        var probe = EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 3);
        var partial = EnumeratedCollectorDriver.BuildPartialFailureNote(
            failed: 1, attempted: 4, failedDatabases: new[] { "appdb" }, firstError: "boom");

        var merged = EnumeratedCollectorDriver.MergeNotes(probe, partial);

        Assert.NotNull(merged);
        Assert.Contains("enumeration probe", merged, StringComparison.Ordinal);
        Assert.Contains("appdb", merged, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, null, null)]
    [InlineData("a", null, "a")]
    [InlineData(null, "b", "b")]
    [InlineData("a", "b", "a; b")]
    public void MergeNotesHandlesEitherSideMissing(string? first, string? second, string? expected)
        => Assert.Equal(expected, EnumeratedCollectorDriver.MergeNotes(first, second));

    /// <summary>
    /// The source pin. Both runners compose this note in their per-database loop, and the two have already
    /// drifted apart on this exact loop once — Darling carried its own copy of the fallback predicate,
    /// which is how #857's fix missed it. Anchored on the CALL, not on a message string.
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs")]
    [InlineData("Lite/Services/RemoteCollectorService.DefinitionRunner.cs")]
    public void BothRunnersComposeThePartialFailureNote(string relativePath)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), relativePath));

        Assert.Contains("EnumeratedCollectorDriver.BuildPartialFailureNote(", source, StringComparison.Ordinal);
        Assert.Contains("EnumeratedCollectorDriver.MergeNotes(", source, StringComparison.Ordinal);

        /* Every arm that counts a failure must also NAME it, or the note reports a count with the wrong
           set of names beside it — worse than no names, because it reads as complete. */
        var counted = CountOccurrences(source, "failed++;");
        var named = CountOccurrences(source, "failedDatabases.Add(databaseName);");
        Assert.Equal(counted, named);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
