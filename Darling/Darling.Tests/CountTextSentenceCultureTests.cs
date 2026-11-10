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
/// so one sentence carried two conventions and the count read as a decimal. Each test sets the thread culture to de-DE
/// for the call and restores it afterwards; the culture is per thread, so a test running beside it is not touched.
/// </summary>
public sealed class CountTextSentenceCultureTests
{
    private static T OnAGermanDesktop<T>(Func<T> read)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
        try
        {
            return read();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

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
}
