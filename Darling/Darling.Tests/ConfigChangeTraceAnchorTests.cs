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
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3740: the <c>CONFIG_CHANGED</c> finding's trace anchor on the Darling side — the shared join
/// (<see cref="ConfigChangeAttribution.ResolveTraceAnchor"/>) over the default trace's sp_configure line
/// (msg 15457), and the Postgres read that feeds it
/// (<see cref="DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql"/>). The Lite twin of the pure
/// pins is <c>Lite.Tests/ConfigChangeAttributionTests</c>; the pins here are the ones Darling.Tests owns:
/// the read's dialect and its de-skew, and the join exercised once more from this assembly so the two SKUs'
/// test trees cannot drift on what "anchored" means.
///
/// <para><b>Why the read is pinned as text.</b> <c>default_trace_events.event_time</c> is the monitored
/// server's LOCAL wall clock while the span it is bounded by is two <c>server_config.capture_time</c> values
/// in naive UTC. A read that bounded the raw column would put the line OUTSIDE the span on every non-UTC
/// server — at the fleet's UTC−4 the stored value sits four hours later than its capture — so the anchor
/// would silently never resolve and the card would read exactly as it did before this lane, which is the
/// failure that looks like nothing happened. <c>ServerLocalReadFrameDisciplineTests</c> counts the three
/// de-skew sites across the tree; this pins the statement's own shape so a reviewer of THIS file sees the
/// bounds and the projection carry the same expression.</para>
/// </summary>
public sealed class ConfigChangeTraceAnchorTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 14, 0, 0, DateTimeKind.Utc);
    private const string Maxdop = "max degree of parallelism";
    private const string Ctfp = "cost threshold for parallelism";

    /* ───────────────────────── the Postgres read ───────────────────────── */

    [Fact]
    public void TheTraceRead_SelectsTheReconfigureLine_ByNumber_InsideTheSpan()
    {
        var sql = DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql;

        Assert.Contains("FROM default_trace_events AS dte, svr", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE dte.server_id = $1", sql, StringComparison.Ordinal);
        /* The NUMBER, not the text: populated on the row (measured), language-independent, and the constant
           the attribution spells. */
        Assert.Contains($"AND   dte.error_number = {ConfigChangeAttribution.ReconfigureMessageNumber}", sql, StringComparison.Ordinal);
        Assert.Equal(15457, ConfigChangeAttribution.ReconfigureMessageNumber);
        /* The span is (previous capture, this capture]: exclusive below, inclusive above. */
        Assert.Contains("> $2", sql, StringComparison.Ordinal);
        Assert.Contains("<= $3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(">= $2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("< $3", sql, StringComparison.Ordinal);
        /* Oldest first, so a caller reading the list in order reads the change history in order. */
        Assert.EndsWith("ORDER BY event_time_utc", sql.TrimEnd(), StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY event_time_utc DESC", sql, StringComparison.Ordinal);
        /* Only what the join needs. */
        Assert.Contains("dte.text_data", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("severity", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTraceRead_DeSkewsTheServerLocalEventTime_OnTheProjectionAndBothBounds()
    {
        var sql = DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql;

        /* The offset CTE, in the exact shape DarlingDefaultTraceReader.EventsByWindowSql carries: the newest
           collected non-NULL offset, or 0 — and a single row, so the cross join keeps every event. */
        Assert.Contains("WITH svr AS (", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT COALESCE((", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties AS sp", sql, StringComparison.Ordinal);
        Assert.Contains("AND   sp.utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sp.collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1), 0) AS offset_minutes", sql, StringComparison.Ordinal);

        /* Three sites, each the same expression: the value returned and the two bounds it is selected by. */
        var deSkew = new Regex(@"dte\.event_time\s*-\s*make_interval\s*\(\s*mins\s*=>\s*svr\.offset_minutes\s*\)");
        Assert.Equal(3, deSkew.Matches(sql).Count);
        Assert.Contains("dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc", sql, StringComparison.Ordinal);
        Assert.Contains("AND   dte.event_time - make_interval(mins => svr.offset_minutes) > $2", sql, StringComparison.Ordinal);
        Assert.Contains("AND   dte.event_time - make_interval(mins => svr.offset_minutes) <= $3", sql, StringComparison.Ordinal);

        /* And no bare read of the column survives anywhere in the statement. */
        var bare = new Regex(@"dte\.event_time(?!\s*-\s*make_interval)");
        Assert.Equal(0, bare.Matches(sql).Count);
    }

    /* ───────────────────────── the join, from this assembly ───────────────────────── */

    [Fact]
    public void TheJoin_AnchorsOnTheSameOptionsLineInTheSpan_AndNotOnAnotherOptions()
    {
        var previous = T0.AddHours(-30);
        var changedAt = T0.AddHours(-27);
        var evt = MaxdopEvent(T0, previous);

        var anchored = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt,
            [new ConfigChangeAttribution.TraceLine(changedAt, RawLine(Maxdop, 0, 8))]);
        Assert.NotNull(anchored);
        Assert.Equal(changedAt, anchored!.ChangedAtUtc);

        var other = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt,
            [new ConfigChangeAttribution.TraceLine(changedAt, RawLine(Ctfp, 5, 50))]);
        Assert.Null(other);

        var outside = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt,
        [
            new ConfigChangeAttribution.TraceLine(previous, RawLine(Maxdop, 0, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddMinutes(1), RawLine(Maxdop, 0, 8)),
        ]);
        Assert.Null(outside);

        /* The no-op re-run ("50 to 50", measured) is not the change; the last real move to the observed
           value is. */
        var flapped = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt,
        [
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-10), RawLine(Maxdop, 0, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-6), RawLine(Maxdop, 8, 4)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-3), RawLine(Maxdop, 4, 8)),
            new ConfigChangeAttribution.TraceLine(T0.AddHours(-1), RawLine(Maxdop, 8, 8)),
        ]);
        Assert.Equal(T0.AddHours(-3), flapped!.ChangedAtUtc);
    }

    [Fact]
    public void TheFact_SaysWhichClockItUsed_OnBothAnchors()
    {
        var previous = T0.AddHours(-30);
        var changedAt = T0.AddHours(-27);
        var evt = MaxdopEvent(T0, previous);
        var anchor = ConfigChangeAttribution.ResolveServerConfigTraceAnchor(evt,
            [new ConfigChangeAttribution.TraceLine(changedAt, RawLine(Maxdop, 0, 8))]);

        var traced = ConfigChangeAttribution.BuildFact(1, evt, 0,
            ConfigChangeAttribution.WindowsFor(ConfigChangeAttribution.AnchorTime(evt, anchor), T0), compare: null, null, null, anchor);
        Assert.Equal(ConfigChangeAttribution.AnchorSourceDefaultTrace, traced.Metadata[ConfigChangeAttribution.MetaAnchorClock]);
        Assert.Equal(new DateTimeOffset(changedAt).ToUnixTimeSeconds(), traced.Metadata[ConfigChangeAttribution.MetaChangeTimeUnix]);
        Assert.Equal(new DateTimeOffset(T0).ToUnixTimeSeconds(), traced.Metadata[ConfigChangeAttribution.MetaObservedAtUnix]);
        Assert.Equal(0, traced.Metadata[ConfigChangeAttribution.MetaObservationGapHours]);
        Assert.Equal(27.0, traced.Metadata[ConfigChangeAttribution.MetaObservedLagHours], precision: 6);
        Assert.Equal(0, traced.Metadata[ConfigChangeAttribution.MetaAfterWindowClamped]);

        var observed = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0), compare: null, null, null);
        Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, observed.Metadata[ConfigChangeAttribution.MetaAnchorClock]);
        Assert.Equal(new DateTimeOffset(T0).ToUnixTimeSeconds(), observed.Metadata[ConfigChangeAttribution.MetaChangeTimeUnix]);
        Assert.Equal(30.0, observed.Metadata[ConfigChangeAttribution.MetaObservationGapHours], precision: 6);
        Assert.False(observed.Metadata.ContainsKey(ConfigChangeAttribution.MetaObservedLagHours));
        Assert.Equal(1, observed.Metadata[ConfigChangeAttribution.MetaAfterWindowClamped]);

        /* The composer reads the same key and says the same thing in words. */
        var tracedAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { traced }.ToFactLookup())!;
        Assert.Contains("changed at 2026-09-17 11:00 UTC (default trace: the sp_configure line, msg 15457)", tracedAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("first observed it 27 h later", tracedAdvice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first observed by the configuration snapshot", tracedAdvice.Investigation, StringComparison.Ordinal);
        var observedAdvice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { observed }.ToFactLookup())!;
        Assert.Contains("first observed by the configuration snapshot at 2026-09-18 14:00 UTC", observedAdvice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("default trace", observedAdvice.Investigation, StringComparison.Ordinal);
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static ConfigChangeAttribution.ChangeEvent MaxdopEvent(DateTime observedAt, DateTime previousCapture)
    {
        var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>
        {
            new(previousCapture, Maxdop, 0, 0, true, true),
            new(observedAt, Maxdop, 8, 8, true, true),
        };
        return Assert.Single(ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffServerConfigChanges(snapshots, observedAt.AddHours(-4), observedAt)
                .Select(c => (c.ChangeTime, new ConfigChangeAttribution.SettingChange(
                    c.ConfigurationName, c.OldValueConfigured, c.NewValueConfigured, c.OldValueInUse, c.NewValueInUse, c.RequiresRestart))),
            snapshots.Select(s => s.CaptureTime)));
    }

    /// <summary>Msg 15457's TextData as the default trace stores it (measured on SQL Server 2022): the raw
    /// error-log line — timestamp, spid, six spaces, then the message.</summary>
    private static string RawLine(string option, long oldValue, long newValue) =>
        $"2026-09-17 11:00:00.91 spid95      Configuration option '{option}' changed from {oldValue} to {newValue}. Run the RECONFIGURE statement to install.";
}
