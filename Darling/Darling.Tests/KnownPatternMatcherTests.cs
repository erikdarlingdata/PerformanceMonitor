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
using PerformanceMonitor.Common.KnownPatterns;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6334: the shared known-pattern matcher. Each test names a rule a wrong matcher would break: a NULL scope field that fails to mean
/// "any", a window that stops at midnight, a time of day compared in UTC when the entry names a zone, an entry judged by the reader's
/// clock instead of the alert's.
/// </summary>
public sealed class KnownPatternMatcherTests
{
    private static readonly DateTime Created = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static KnownPattern Entry(
        int? server = null, string? metric = null, string? time = null, string? zone = null, int window = 30, DateTime? expires = null) =>
        new(1, server, metric, time is null ? null : TimeOnly.Parse(time, System.Globalization.CultureInfo.InvariantCulture), zone,
            window, "note", "tester", Created, expires);

    private static DateTime At(string utc) =>
        DateTime.SpecifyKind(DateTime.Parse(utc, System.Globalization.CultureInfo.InvariantCulture), DateTimeKind.Unspecified);

    [Fact]
    public void An_entry_with_no_scope_matches_every_alert()
    {
        Assert.True(KnownPatternMatcher.Matches(Entry(), 7, "High CPU", At("2026-03-04T11:12:00")));
        Assert.True(KnownPatternMatcher.Matches(Entry(), 99, "Blocking Detected", At("2026-09-30T23:59:00")));
    }

    [Fact]
    public void A_server_scope_matches_only_that_server_and_a_null_server_matches_all()
    {
        var alert = At("2026-03-04T11:12:00");
        Assert.True(KnownPatternMatcher.Matches(Entry(server: 7), 7, "High CPU", alert));
        Assert.False(KnownPatternMatcher.Matches(Entry(server: 7), 8, "High CPU", alert));
        Assert.True(KnownPatternMatcher.Matches(Entry(server: null), 8, "High CPU", alert));
    }

    [Fact]
    public void A_metric_scope_matches_only_that_metric_ignoring_case()
    {
        var alert = At("2026-03-04T11:12:00");
        Assert.True(KnownPatternMatcher.Matches(Entry(metric: "High CPU"), 1, "high cpu", alert));
        Assert.False(KnownPatternMatcher.Matches(Entry(metric: "High CPU"), 1, "Blocking Detected", alert));
        Assert.False(KnownPatternMatcher.Matches(Entry(metric: "High CPU"), 1, null, alert));
        Assert.True(KnownPatternMatcher.Matches(Entry(metric: null), 1, null, alert));
    }

    [Fact]
    public void The_window_includes_both_edges_and_excludes_the_next_minute()
    {
        var entry = Entry(time: "02:00", window: 30);
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-03-04T01:30:00")));
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-03-04T02:30:59")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-03-04T01:29:59")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-03-04T02:31:00")));
    }

    [Fact]
    public void The_window_wraps_past_midnight_in_both_directions()
    {
        var justAfter = Entry(time: "00:10", window: 30);
        Assert.True(KnownPatternMatcher.Matches(justAfter, 1, "m", At("2026-03-04T23:50:00")));
        Assert.False(KnownPatternMatcher.Matches(justAfter, 1, "m", At("2026-03-04T23:30:00")));

        var justBefore = Entry(time: "23:50", window: 30);
        Assert.True(KnownPatternMatcher.Matches(justBefore, 1, "m", At("2026-03-05T00:10:00")));
        Assert.False(KnownPatternMatcher.Matches(justBefore, 1, "m", At("2026-03-05T00:30:00")));
    }

    [Fact]
    public void The_time_of_day_is_read_in_the_entrys_zone_across_a_daylight_saving_change()
    {
        // 02:00 America/New_York is 07:00 UTC in January (EST) and 06:00 UTC in July (EDT). A UTC-only matcher
        // would match one of the two and miss the other.
        var entry = Entry(time: "02:00", zone: "America/New_York", window: 10);
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-01-15T07:00:00")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-01-15T06:00:00")));
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-07-15T06:00:00")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-07-15T07:00:00")));
    }

    [Fact]
    public void A_zone_less_entry_reads_the_time_of_day_in_utc()
    {
        var entry = Entry(time: "02:00", window: 10);
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-07-15T02:05:00")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-07-15T06:00:00")));
    }

    [Fact]
    public void An_entry_expires_at_the_alerts_time_not_the_readers()
    {
        // The entry lapses at 2026-01-10 00:00 UTC. An alert from before then still shows it when read long after; one at or after does not.
        var entry = Entry(expires: At("2026-01-10T00:00:00"));
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-01-09T23:59:59")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-01-10T00:00:00")));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-01-11T00:00:00")));
    }

    [Fact]
    public void An_unknown_zone_is_read_as_utc_and_reported_once()
    {
        var entry = Entry(time: "02:00", zone: "Mars/Olympus_Mons_6334", window: 10);
        var lines = new List<string>();
        Assert.True(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-07-15T02:00:00"), lines.Add));
        Assert.False(KnownPatternMatcher.Matches(entry, 1, "m", At("2026-07-15T06:00:00"), lines.Add));
        Assert.Single(lines);
        Assert.Contains("Mars/Olympus_Mons_6334", lines[0], StringComparison.Ordinal);
    }

    [Fact]
    public void Time_zone_validation_accepts_iana_ids_and_null_and_refuses_the_unknown()
    {
        Assert.True(KnownPatternMatcher.IsKnownTimeZone(null));
        Assert.True(KnownPatternMatcher.IsKnownTimeZone(""));
        Assert.True(KnownPatternMatcher.IsKnownTimeZone("UTC"));
        Assert.True(KnownPatternMatcher.IsKnownTimeZone("America/New_York"));
        Assert.True(KnownPatternMatcher.IsKnownTimeZone("Europe/London"));
        Assert.False(KnownPatternMatcher.IsKnownTimeZone("Not/AZone"));
    }

    [Fact]
    public void Match_returns_only_the_matching_entries_in_the_order_given()
    {
        var entries = new List<KnownPattern>
        {
            Entry(server: 1) with { Id = 10 },
            Entry(server: 2) with { Id = 11 },
            Entry(metric: "High CPU") with { Id = 12 },
            Entry(time: "02:00", window: 5) with { Id = 13 },
        };
        var ids = KnownPatternMatcher.Match(entries, 1, "High CPU", At("2026-03-04T11:12:00")).Select(e => e.Id).ToArray();
        Assert.Equal(new long[] { 10, 12 }, ids);
        Assert.Empty(KnownPatternMatcher.Match(Array.Empty<KnownPattern>(), 1, "High CPU", At("2026-03-04T11:12:00")));
    }

    [Fact]
    public void The_default_window_is_the_one_the_investigation_recurrence_count_uses()
    {
        Assert.Equal(PerformanceMonitor.Common.InvestigationAlertRecurrence.WindowMinutes, KnownPatternMatcher.DefaultWindowMinutes);
    }
}
