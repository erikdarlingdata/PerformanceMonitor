/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8522: a sentence that counts with <c>CountText</c> writes its other numbers in the same convention. <c>CountText</c>
/// writes "1,500" on every desktop; a figure beside it written in the thread's culture read "1.200" on a German desktop,
/// so one sentence carried two conventions and the count read as a decimal. Each test sets the thread culture for the
/// call and restores it afterwards; the culture is per thread, so a test running beside it is not touched.
/// </summary>
public sealed class CountTextSentenceCultureTests
{
    private static T InCulture<T>(string name, Func<T> read)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
        try
        {
            return read();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    private static T OnAGermanDesktop<T>(Func<T> read) => InCulture("de-DE", read);

    [Fact]
    public void PurgeSummary_OnAGermanDesktop_WritesRowsAndTablesInOneConvention()
    {
        var text = OnAGermanDesktop(() => ViewerServerTab.FormatPurgeSummary("""{"tablesPurged":33,"rowsPurged":1200}""", out _));

        Assert.Equal("Purged 1,200 rows/chunks across 33 tables", text);
    }

    [Fact]
    public void WaitSamplingSummary_OnAGermanDesktop_WritesEventsAndAttributedInOneConvention()
    {
        var at = new DateTime(2026, 11, 9, 12, 0, 0, DateTimeKind.Utc);
        var rows = Enumerable.Range(0, 2500)
            .Select(i => new DarlingPgWaitSamplingReader.PgWaitSamplingRow("LWLock", "BufferContent", i < 1500 ? i + 1 : null, 1, 1, 1, false, at))
            .ToList();

        var text = OnAGermanDesktop(() => PgDisplay.WaitSamplingSummary(rows, serviceTier: true, instrumentKnown: true, cadence: "every minute", reset: false));

        Assert.StartsWith("2,500 wait events, 1,500 attributed to a query. ", text, StringComparison.Ordinal);
    }

    [Fact]
    public void LockStatsRowsNote_OnAGermanDesktop_WritesBothCountsInOneConvention()
    {
        // The panel text and its hover detail count the same two figures, so both read "1,200 ... 1,500" everywhere.
        var note = OnAGermanDesktop(() => ViewerServerTab.LockStatsRowsNote(1200, 1500));

        Assert.Equal("1,200 lock queues across 1,500 captures.", note.Text);
        Assert.StartsWith("1,200 lock queues across 1,500 captures.", note.Detail, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    public void PredicateStatsSummary_WritesThePercentWithoutASpace_OnEveryDesktop(string culture)
    {
        // The sample rate reads "0.10%" as it always did on an English desktop: no culture puts a space before the sign.
        var text = InCulture(culture, () => PgDisplay.PredicateStatsSummary(1234, 0.001));

        Assert.Equal("1,234 predicates, sampled at 0.10% of executions", text);
    }

    [Fact]
    public void LockStatsRowsNote_NoQueue_OnAGermanDesktop_CountsItsCapturesInOneConvention()
    {
        // The no-queue branch counts its captures like the queued branch below it, and one capture reads as one.
        var many = OnAGermanDesktop(() => ViewerServerTab.LockStatsRowsNote(0, 1500));
        var one = ViewerServerTab.LockStatsRowsNote(0, 1);

        Assert.Equal("No lock was waiting in any of 1,500 captures. Every lock held in this window was granted.", many.Text);
        Assert.StartsWith("No lock was waiting in any of 1,500 captures", many.Detail, StringComparison.Ordinal);
        Assert.StartsWith("No lock was waiting in any of 1 capture.", one.Text, StringComparison.Ordinal);
    }
}
