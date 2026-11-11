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
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8532: an advice sentence that counted events never writes their rate as zero. Six deadlocks in a week are 0.04 an
/// hour, which the sentence's one optional decimal place writes as "0"; it says "under 0.1 per hour" there, the
/// calendar's and the cards' "&lt;0.1" in a sentence's words. A rate the sentence can write keeps its one optional
/// decimal place ("about 0.8 per hour", "about 2 per hour"). The count, the rate and the busiest hour's count are
/// written in the invariant convention, so the sentence reads the same on every desktop. The composer is shared by
/// both apps.
/// </summary>
public sealed class AdviceRatePerHourTextTests
{
    private static AdviceBlock Compose(string key, string countKey, double count, string rateKey, double perHour, string? peakKey = null, double peak = 0)
    {
        var fact = new Fact { Key = key, Value = count };
        fact.Metadata[countKey] = count;
        fact.Metadata[rateKey] = perHour;
        if (peakKey is not null)
            fact.Metadata[peakKey] = peak;
        return FactAdvice.Compose(key, new Dictionary<string, Fact> { [key] = fact })!;
    }

    [Fact]
    public void Deadlocks_SayLessThanATenthPerHour_WhenAPositiveRateRoundsToZero()
    {
        var week = Compose("DEADLOCKS", "deadlock_count", 6, "deadlocks_per_hour", 6.0 / 168.0);
        Assert.StartsWith("6 deadlocks this window, under 0.1 per hour.", week.Investigation, StringComparison.Ordinal);

        var day = Compose("DEADLOCKS", "deadlock_count", 20, "deadlocks_per_hour", 20.0 / 24.0);
        Assert.StartsWith("20 deadlocks this window, about 0.8 per hour", day.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingEvents_SayLessThanATenthPerHour_WhenAPositiveRateRoundsToZero()
    {
        var week = Compose("BLOCKING_EVENTS", "event_count", 6, "events_per_hour", 6.0 / 168.0);
        Assert.Contains("6 blocking events this window, under 0.1 per hour.", week.Headline + " " + week.Investigation, StringComparison.Ordinal);

        var day = Compose("BLOCKING_EVENTS", "event_count", 48, "events_per_hour", 2.0);
        Assert.Contains("48 blocking events this window, about 2 per hour.", day.Headline + " " + day.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void Deadlocks_OnAGermanDesktop_WriteTheCountTheRateAndThePeakInOneConvention()
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
        try
        {
            var busy = Compose("DEADLOCKS", "deadlock_count", 1234, "deadlocks_per_hour", 1234.0 / 168.0, "peak_1h_deadlock_count", 1052);
            Assert.StartsWith("1,234 deadlocks this window, about 7.3 per hour, up to 1,052 in the busiest hour.", busy.Investigation, StringComparison.Ordinal);
            Assert.StartsWith("1,234 deadlocks this window:", busy.Headline, StringComparison.Ordinal);

            var quiet = Compose("DEADLOCKS", "deadlock_count", 6, "deadlocks_per_hour", 6.0 / 168.0);
            Assert.StartsWith("6 deadlocks this window, under 0.1 per hour.", quiet.Investigation, StringComparison.Ordinal);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    [Fact]
    public void BlockingEvents_OnAGermanDesktop_WriteTheCountAndTheRateInOneConvention()
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
        try
        {
            var busy = Compose("BLOCKING_EVENTS", "event_count", 1234, "events_per_hour", 1234.0 / 168.0);
            Assert.StartsWith("1,234 blocking events this window, about 7.3 per hour.", busy.Investigation, StringComparison.Ordinal);
            Assert.StartsWith("1,234 blocking events this window:", busy.Headline, StringComparison.Ordinal);

            var quiet = Compose("BLOCKING_EVENTS", "event_count", 6, "events_per_hour", 6.0 / 168.0);
            Assert.StartsWith("6 blocking events this window, under 0.1 per hour.", quiet.Investigation, StringComparison.Ordinal);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }
}
