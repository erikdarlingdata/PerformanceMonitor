/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731: every window read in the two anomaly detectors ends its window the same way. The window is
/// <c>[TimeRangeStart, TimeRangeEnd)</c>: a sample stamped exactly at the end belongs to the NEXT window, so it is
/// counted once, not by this window and again by the one that starts there. Eight reads in each detector (the
/// blocking and deadlock counts, and the I/O, batch request, session count, query duration and memory tiles)
/// used a closed end, and the three beside them (the CPU tile and the two wait reads) used the open one, so a
/// sample on the boundary moved the blocking and deadlock counts, and a tile's peak and mean, while leaving the
/// CPU and wait figures alone.
///
/// <para>The pin reads both detector files as text, so a read added later with the closed spelling, or one that
/// drifts back, fails here rather than counting a boundary sample in one product and not the other. Lite's
/// behavioural half, which seeds a sample at the end and runs the real detector, is
/// <c>AnomalyTileWindowEndTests</c> in Lite.Tests.</para>
/// </summary>
public sealed class AnomalyTileWindowEndTests
{
    /// <summary>The closed spelling of the window end: a boundary sample is read.</summary>
    private const string ClosedEnd = "collection_time <= $3";

    /// <summary>The open spelling of the window end: a boundary sample is left for the next window.</summary>
    private const string OpenEnd = "collection_time < $3";

    /// <summary>The floor: at least these 11 reads end their window at $3 (three that were already open, eight that were closed). A read added later must use the open end too, which the closed-spelling check above guards.</summary>
    private const int WindowReads = 11;

    [Theory]
    [InlineData("Lite/Analysis/AnomalyDetector.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Analysis/PgAnomalyDetector.cs")]
    public void EveryWindowRead_ExcludesTheWindowEnd(string relative)
    {
        var source = ReadSource(relative);

        Assert.DoesNotContain(ClosedEnd, source, StringComparison.Ordinal);
        var found = CountOccurrences(source, OpenEnd);
        Assert.True(found >= WindowReads, $"Expected at least {WindowReads} reads ending at $3 with the open spelling, found {found}.");
    }

    private static int CountOccurrences(string text, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = text.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static string ReadSource(string relative) => File.ReadAllText(Path.Combine(RepoRoot(), relative));
}
