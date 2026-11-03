/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3, lane 4: the wide-read guard counts the hours the rollup answers at <see cref="ComposeLimits.StampRowWeight"/>, through the
/// runner's own StampThrough. A fleet-sized estimate (the daily summary says every server reads millions of wide rows a day) is seeded; the
/// one-day fleet panel is then run through the shared runner twice: with the rollup built (it passes, and its text reads the rollup) and
/// with the rollup's hour ledger empty, as before the first build (StampThrough is null, every hour counts 1.0, and the panel is refused with
/// the guard's message, never run).
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class ComposeQueryStoreStampGuardLiveTests
{
    private const string PanelJson =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    [Fact]
    public async Task TheOneDayFleetPanel_PassesTheGuardWithStampThrough_AndIsRefusedWithoutIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The fleet-sized estimate: three servers at 3.2 M wide rows a day each is 9.6 M for the one-day window, over the 6.8 M limit
               for one scan of the wide table, and 0.3 weight for the 22 hours the rollup answers brings it to about a third of that. */
            foreach (var server in new[] { 1, 2, 3 })
            {
                await ComposeStampLiveSupport.ExecAsync(connection,
                    "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
                    + $"VALUES ({server}, DATE '{hourNow.AddDays(-1):yyyy-MM-dd}', 2, now() AT TIME ZONE 'UTC', 3200000)", ct);
            }

            /* The wide table's floor must lie a purge-edge margin before the window (the eligibility gate's clause 3); one old row per server,
               outside the window and outside the builder's floor, stands for the retained history a real store has. */
            foreach (var server in new[] { 1, 2, 3 })
            {
                await ComposeStampLiveSupport.InsertRowAsync(connection, server, hourNow.AddDays(-10), 8_000_000 + server, ct);
            }

            var spec = new JsonObject { ["panel"] = JsonNode.Parse(PanelJson), ["hours"] = 24 };

            /* The runner's own resolution: eligible, with the rollup answering up to the first unbuilt hour. */
            var end = DateTime.UtcNow;
            var start = end.AddHours(-24);
            var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);
            Assert.True(resolution.Eligible, "the seeded store must route the panel to the wide table, or this test proves nothing");
            Assert.Equal(hourNow.AddHours(-2), resolution.StampThrough);

            var passed = await DarlingWebEndpoints.RunComposedPanelAsync(source, (JsonObject)spec.DeepClone(), ct);
            Assert.True(passed.Payload is not null, "with the rollup built the one-day panel must run: " + passed.Error);
            Assert.Contains("query_store_compose_stamp", passed.Payload!["sql"]!.GetValue<string>(), StringComparison.Ordinal);

            /* Before the first build: no hour in the ledger, StampThrough is null, the same panel is refused. */
            await ComposeStampLiveSupport.ExecAsync(connection, "DELETE FROM collect.query_store_compose_stamp_hours", ct);
            var unbuilt = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);
            Assert.True(unbuilt.Eligible);
            Assert.Null(unbuilt.StampThrough);

            var refused = await DarlingWebEndpoints.RunComposedPanelAsync(source, (JsonObject)spec.DeepClone(), ct);
            Assert.True(refused.Payload is null, "without StampThrough the 9.6 M-row read must be refused; the panel ran");
            Assert.False(refused.IsServerError);
            Assert.StartsWith("This panel needs about ", refused.Error, StringComparison.Ordinal);
            Assert.EndsWith("Choose fewer servers or a shorter window.", refused.Error, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    private const string FleetPanelJson =
        "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":50,\"groupBy\":[\"query_hash\"],\"viz\":\"bar\"}";
    private const string FilteredFleetPanelJson =
        "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":50,\"groupBy\":[\"query_hash\"],\"viz\":\"bar\","
        + "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"like\",\"value\":\"%\"}]}";

    /// <summary>#7285: the one-day, fleet-wide, unfiltered query_hash panel the fleet rollup serves, on a store whose wide rows a day are
    /// so many that the stamp weight alone refuses it. It runs, and reads the fleet table. The same panel with a filter, which the fleet
    /// table cannot serve, is refused as before. And a store whose raw tail alone is over the limit refuses it even so.</summary>
    [Fact]
    public async Task TheOneDayFleetPanelOnALargeFleet_PassesTheGuardOnTheFleetTablesSize_AFilteredPanelAndAHugeTailAreStillRefused()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            foreach (var server in new[] { 1, 2, 3 })
            {
                await ComposeStampLiveSupport.ExecAsync(connection,
                    "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
                    + $"VALUES ({server}, DATE '{hourNow.AddDays(-1):yyyy-MM-dd}', 2, now() AT TIME ZONE 'UTC', 12000000)", ct);
                await ComposeStampLiveSupport.InsertRowAsync(connection, server, hourNow.AddDays(-10), 8_000_000 + server, ct);
            }

            var end = DateTime.UtcNow;
            var start = end.AddHours(-24);
            var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct);
            Assert.True(resolution.Eligible && resolution.FleetReady, "the seeded store must route the panel to the wide table with the fleet table built");
            Assert.Equal(hourNow.AddHours(-2), resolution.StampThrough);

            /* 36 M wide rows a day, the newest two hours (about 3 M) unstamped. At the stamp weight the day is 12 M and over the 6.8 M limit
               (the figure before #7285). The fleet table holds a few hundred rows, so the day is the tail plus almost nothing. */
            var fleetSpec = new JsonObject { ["panel"] = JsonNode.Parse(FleetPanelJson), ["hours"] = 24 };
            var passed = await DarlingWebEndpoints.RunComposedPanelAsync(source, (JsonObject)fleetSpec.DeepClone(), ct);
            Assert.True(passed.Payload is not null, "the fleet table serves the panel, so the guard must let it run: " + passed.Error);
            Assert.Contains("query_store_compose_fleet", passed.Payload!["sql"]!.GetValue<string>(), StringComparison.Ordinal);

            /* A panel outside the fleet conditions keeps the stamp estimate and is refused with the stamp branch's message. */
            var filteredSpec = new JsonObject { ["panel"] = JsonNode.Parse(FilteredFleetPanelJson), ["hours"] = 24 };
            var refused = await DarlingWebEndpoints.RunComposedPanelAsync(source, (JsonObject)filteredSpec.DeepClone(), ct);
            Assert.True(refused.Payload is null, "a filtered panel cannot read the fleet table; the stamp-weighted 12 M must refuse it");
            Assert.StartsWith("This panel reads too much Query Store history", refused.Error, StringComparison.Ordinal);

            /* A raw tail that is over the limit by itself: 180 M wide rows a day puts the newest two hours at 15 M. */
            await ComposeStampLiveSupport.ExecAsync(connection, "UPDATE collect.query_store_top_daily_built SET source_rows = 60000000", ct);
            var tail = await DarlingWebEndpoints.RunComposedPanelAsync(source, (JsonObject)fleetSpec.DeepClone(), ct);
            Assert.True(tail.Payload is null, "the tail alone is over the limit; the fleet table cannot make it fit");
            Assert.StartsWith("This panel reads too much Query Store history", tail.Error, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
