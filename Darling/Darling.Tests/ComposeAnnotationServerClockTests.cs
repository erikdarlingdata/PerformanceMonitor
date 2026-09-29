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
using System.Text.Json.Nodes;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4821, Custom Views: a server-local annotation marker (the Default Trace) is placed by the offset in force
/// on the marker's OWN date, not by the one newest offset the server reported. The compiler reads each
/// server's clock first (<see cref="ComposeCompiler.CompileServerClockRead"/>), turns every clock into
/// stretches of local time with one offset each (<see cref="ComposeCompiler.ServerLocalRanges"/>), and binds
/// those stretches into the annotation query, which subtracts the offset of the stretch each row falls in.
///
/// <para>The conversion is checked against <see cref="ServerClock.ToUtc"/> itself, the one the MCP reads
/// and the viewer use, so the chart and those reads cannot place the same event at two different
/// instants, including in the repeated and the skipped hour.</para>
/// </summary>
public sealed class ComposeAnnotationServerClockTests
{
    private const string EasternWindowsId = "Eastern Standard Time";
    private const string ServerLocalSource = "default_trace_events";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    /* ───────────────────────── the clock read ───────────────────────── */

    [Fact]
    public void ClockRead_TakesTheNewestRowWithAnOffset_AndTheZoneFromThatSameRow()
    {
        var sql = ComposeCompiler.CompileServerClockRead(Context(new[] { "SERVER-A" })).Sql;

        Assert.Contains("SELECT DISTINCT ON (server_name) server_name, time_zone_id, utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_name, collection_time DESC", sql, StringComparison.Ordinal);
    }

    /// <summary><c>server_properties</c> has no index on <c>server_name</c>, so the read's sort is bounded by
    /// the panel's own servers when it names them. A fleet-wide panel names none and reads every server.</summary>
    [Fact]
    public void ClockRead_IsScopedByThePanelsServers_AndReadsTheFleetWhenItNamesNone()
    {
        var scoped = ComposeCompiler.CompileServerClockRead(Context(new[] { "SERVER-A", "SERVER-B" }));
        Assert.Contains("AND   server_name = ANY($1)", scoped.Sql, StringComparison.Ordinal);
        Assert.Equal(new[] { "SERVER-A", "SERVER-B" }, Assert.IsType<string[]>(Assert.Single(scoped.Parameters).Value));
        Assert.DoesNotContain("SERVER-A", scoped.Sql, StringComparison.Ordinal);

        var fleet = ComposeCompiler.CompileServerClockRead(Context(null));
        Assert.DoesNotContain("ANY(", fleet.Sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.server_properties", fleet.Sql, StringComparison.Ordinal);
        Assert.Empty(fleet.Parameters);
    }

    /* ───────────────────────── the annotation query ───────────────────────── */

    /// <summary>The offset comes from the bound stretches, joined on the server AND the row's own local
    /// time, and the SAME de-skewed expression both returns and bounds. No server name, zone, time or
    /// offset is written into the SQL: every one is a bound value.</summary>
    [Fact]
    public void ServerLocalAnnotation_JoinsItsOffsetFromBoundRanges_AndWritesNoValueIntoTheSql()
    {
        var clocks = Clocks(("SERVER-A", EasternWindowsId, -240), ("SERVER-B", null, -420));
        var scoped = CompiledAnnotation(ServerLocalSource, new[] { "SERVER-A", "SERVER-B" }, clocks);

        Assert.Contains(
            "LEFT JOIN unnest($4::text[], $5::timestamp[], $6::timestamp[], $7::integer[]) AS o(server_name, local_from, local_to, utc_offset_minutes)",
            scoped.Sql, StringComparison.Ordinal);
        Assert.Contains("ON  o.server_name = f.server_name", scoped.Sql, StringComparison.Ordinal);
        Assert.Contains("AND f.event_time >= o.local_from", scoped.Sql, StringComparison.Ordinal);
        Assert.Contains("AND f.event_time < o.local_to", scoped.Sql, StringComparison.Ordinal);

        const string deSkewed = "f.event_time - make_interval(mins => COALESCE(o.utc_offset_minutes, 0))";
        Assert.Contains($"SELECT {deSkewed} AS ts", scoped.Sql, StringComparison.Ordinal);
        Assert.Contains($"WHERE {deSkewed} >= $1", scoped.Sql, StringComparison.Ordinal);
        Assert.Contains($"AND {deSkewed} <= $2", scoped.Sql, StringComparison.Ordinal);
        Assert.Contains("f.server_name = ANY($3)", scoped.Sql, StringComparison.Ordinal);
        Assert.Equal(7, scoped.Parameters.Count);

        /* The newest-offset subquery is gone from the annotation query: it is what drew a marker from before
           the last daylight saving change an hour off. */
        Assert.DoesNotContain("server_properties", scoped.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DISTINCT ON", scoped.Sql, StringComparison.Ordinal);

        /* No value in the text: no literal, no server, no zone. */
        Assert.DoesNotContain("'", scoped.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SERVER-", scoped.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("Eastern", scoped.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("240", scoped.Sql, StringComparison.Ordinal);

        /* A fleet-wide panel binds no server array, so the stretches take $3 to $6. */
        var fleet = CompiledAnnotation(ServerLocalSource, null, clocks);
        Assert.Contains("LEFT JOIN unnest($3::text[], $4::timestamp[], $5::timestamp[], $6::integer[])", fleet.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ANY(", fleet.Sql, StringComparison.Ordinal);
        Assert.Equal(6, fleet.Parameters.Count);
    }

    /// <summary>A UTC source is not touched: no join, no extra binds.</summary>
    [Fact]
    public void UtcAnnotation_BindsNoRanges()
    {
        var clocks = Clocks(("SERVER-A", EasternWindowsId, -240));
        var utc = CompiledAnnotation("system_health_events", new[] { "SERVER-A" }, clocks);

        Assert.DoesNotContain("unnest", utc.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("utc_offset_minutes", utc.Sql, StringComparison.Ordinal);
        Assert.Equal(3, utc.Parameters.Count);
    }

    /// <summary>The zone arm and the fixed-offset arm, as the values the query binds. The window crosses the
    /// 2026-11-01 fall-back (06:00Z), so the Eastern server gets two stretches that meet at 02:00 local, the
    /// end of the repeated hour; the zone-less server keeps its one fixed offset; a server with no row gets
    /// no stretch and reads as UTC through the <c>COALESCE</c>.</summary>
    [Fact]
    public void ServerLocalAnnotation_BindsOneStretchPerOffsetForAZone_AndOneForAFixedOffset()
    {
        var clocks = Clocks(("EAST", EasternWindowsId, -300), ("FIXED", null, -420));
        var compiled = CompiledAnnotation(
            ServerLocalSource, new[] { "EAST", "FIXED", "NO-ROW-YET" }, clocks,
            Naive(2026, 11, 1, 0), Naive(2026, 11, 2, 0));

        Assert.Equal(new[] { "EAST", "EAST", "FIXED" }, Assert.IsType<string[]>(compiled.Parameters[3].Value));
        Assert.Equal(
            new[] { Naive(2026, 10, 31, 0), Naive(2026, 11, 1, 2), Naive(2026, 10, 31, 0) },
            Assert.IsType<DateTime[]>(compiled.Parameters[4].Value));
        Assert.Equal(
            new[] { Naive(2026, 11, 1, 2), Naive(2026, 11, 3, 0), Naive(2026, 11, 3, 0) },
            Assert.IsType<DateTime[]>(compiled.Parameters[5].Value));
        Assert.Equal(new[] { -240, -300, -420 }, Assert.IsType<int[]>(compiled.Parameters[6].Value));
    }

    /* ───────────────────────── agreement with ServerClock ───────────────────────── */

    public static TheoryData<string, string?, int?, DateTime, DateTime> Rows() => new()
    {
        /* A winter row on a server whose newest offset is summer time: -5 h on its own date. */
        { "winter row", EasternWindowsId, -240, Naive(2026, 1, 15, 10), Naive(2026, 1, 15, 15) },
        /* A summer row: -4 h. */
        { "summer row", EasternWindowsId, -240, Naive(2026, 7, 15, 10), Naive(2026, 7, 15, 14) },
        /* 01:30 on 2026-11-01 happens twice; ServerClock takes the first occurrence (daylight, -4 h). */
        { "repeated hour", EasternWindowsId, -300, Naive(2026, 11, 1, 1, 30), Naive(2026, 11, 1, 5, 30) },
        /* 02:30 on 2026-03-08 never happened; ServerClock reads it as 03:30 daylight time. */
        { "skipped hour", EasternWindowsId, -300, Naive(2026, 3, 8, 2, 30), Naive(2026, 3, 8, 7, 30) },
        /* A server that reports no zone (before SQL Server 2022) keeps its newest offset all year. */
        { "zone-less server", null, -300, Naive(2026, 7, 15, 10), Naive(2026, 7, 15, 15) },
        /* No server_properties row at all: UTC, today's fallback. */
        { "no row", null, null, Naive(2026, 7, 15, 10), Naive(2026, 7, 15, 10) },
    };

    [Theory]
    [MemberData(nameof(Rows))]
    public void TheBoundStretches_PlaceEachRowWhereServerClockDoes(
        string row, string? timeZoneId, int? utcOffsetMinutes, DateTime local, DateTime expectedUtc)
    {
        var clock = ServerClock.Resolve(timeZoneId, utcOffsetMinutes);
        var clocks = utcOffsetMinutes is null
            ? ComposeCompiler.NoServerClocks
            : new Dictionary<string, ServerClock>(StringComparer.Ordinal) { ["S"] = clock };
        var ranges = ComposeCompiler.ServerLocalRanges(clocks, Naive(2026, 1, 1, 0), Naive(2027, 1, 1, 0));

        Assert.True(expectedUtc == clock.ToUtc(local), $"{row}: the fixture disagrees with ServerClock.");
        Assert.True(expectedUtc == AsTheQueryDoes(ranges, "S", local), $"{row}: the query would place it at {AsTheQueryDoes(ranges, "S", local):O}.");
    }

    /// <summary>Every quarter hour across both change days, and the stretches tile the covered local time
    /// with no gap and no overlap, so no row can join twice or fall between two stretches.</summary>
    [Fact]
    public void TheBoundStretches_AgreeWithServerClock_AcrossBothChangeDays_AndTileWithoutOverlap()
    {
        var clock = ServerClock.Resolve(EasternWindowsId, -240);
        var clocks = new Dictionary<string, ServerClock>(StringComparer.Ordinal) { ["S"] = clock };
        var start = Naive(2026, 1, 1, 0);
        var end = Naive(2027, 1, 1, 0);
        var ranges = ComposeCompiler.ServerLocalRanges(clocks, start, end);

        Assert.Equal(3, ranges.Count);
        Assert.Equal(start.AddDays(-1), ranges[0].LocalFrom);
        Assert.Equal(end.AddDays(1), ranges[^1].LocalTo);
        for (var i = 1; i < ranges.Count; i++)
        {
            Assert.Equal(ranges[i - 1].LocalTo, ranges[i].LocalFrom);
        }

        foreach (var day in new[] { Naive(2026, 3, 8, 0), Naive(2026, 11, 1, 0) })
        {
            for (var local = day.AddHours(-2); local < day.AddHours(26); local = local.AddMinutes(15))
            {
                Assert.True(clock.ToUtc(local) == AsTheQueryDoes(ranges, "S", local), $"{local:O}");
            }
        }
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    /// <summary>What the compiled join and <c>ts</c> expression do with one row: the offset of the one
    /// stretch holding the row's server and local time, else 0.</summary>
    private static DateTime AsTheQueryDoes(IReadOnlyList<ComposeCompiler.ServerLocalRange> ranges, string server, DateTime local)
    {
        var matches = ranges
            .Where(r => r.ServerName == server && local >= r.LocalFrom && local < r.LocalTo)
            .ToList();
        Assert.True(matches.Count <= 1, "A row joined more than one stretch.");

        return matches.Count == 0 ? local : local.AddMinutes(-matches[0].UtcOffsetMinutes);
    }

    private static Dictionary<string, ServerClock> Clocks(params (string Server, string? Zone, int Offset)[] servers) =>
        servers.ToDictionary(s => s.Server, s => ServerClock.Resolve(s.Zone, s.Offset), StringComparer.Ordinal);

    private static ComposeRunContext Context(IReadOnlyList<string>? servers) =>
        Context(servers, Naive(2026, 7, 18, 0), Naive(2026, 7, 18, 6));

    private static ComposeRunContext Context(IReadOnlyList<string>? servers, DateTime start, DateTime end) =>
        new(servers, start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, RollupCoverage.Unknown);

    private static ComposeCompiled CompiledAnnotation(
        string key, IReadOnlyList<string>? servers, IReadOnlyDictionary<string, ServerClock> clocks,
        DateTime? start = null, DateTime? end = null)
    {
        var json = "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\","
            + "\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"" + key + "\"]}";
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, Array.Empty<string>());
        Assert.True(error is null, error);

        var context = Context(servers, start ?? Naive(2026, 7, 18, 0), end ?? Naive(2026, 7, 18, 6));

        return Assert.Single(ComposeCompiler.CompileAnnotations(plan!, context, clocks)).Compiled;
    }
}
