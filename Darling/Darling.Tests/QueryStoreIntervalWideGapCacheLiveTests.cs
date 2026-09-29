/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4700: the collection-gap verdict behind the below-floor decision is cached per (store, server, table-floor
/// day, cadence) over a three-day window, so a second decision issues no <c>collection_log</c> read.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. The read counter is per
   store, so tests running beside these do not disturb it. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreIntervalWideGapCacheLiveTests
{
    private const string FloorSql = QueryStoreIntervalWide.PlainTableFloorSql;

    private static async Task<DateTime> FloorAsync(QueryStoreIntervalWideBelowFloorLiveTests.Rig rig, CancellationToken ct) =>
        (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(rig.Connection, FloorSql.Replace("$1", "@server_id"), ct))!;

    [Fact]
    public async Task SecondDecision_SameServerFloorDayAndCadence_IssuesNoCollectionLogRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();

        var first = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.NotNull(first.BelowFloorStart);
        Assert.Equal(1, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));

        var second = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.Equal(1, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));
        Assert.Equal(first.BelowFloorStart, second.BelowFloorStart);
        Assert.Equal(first.StartBound, second.StartBound);
    }

    [Fact]
    public async Task CadenceChange_Recomputes()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct, queryStoreCadenceMinutes: 60);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();

        await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.Equal(1, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));

        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection,
            "UPDATE config.config_collector_schedules SET frequency_minutes = 30 WHERE server_id = @server_id AND collector_name = 'query_store'", null, ct);
        var changed = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.NotNull(changed.BelowFloorStart);
        Assert.Equal(2, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));

        await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.Equal(2, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));
    }

    [Fact]
    public async Task MovedFloorDay_Recomputes_AVerdictForDayD_NeverServesDayDPlusOne()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();

        var before = await FloorAsync(rig, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.Equal(1, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));

        /* Retention removes the oldest interval and the next-oldest lies a day later: the floor moves to the next UTC day. */
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection,
            "UPDATE collect.query_store_interval_wide SET first_execution_time = first_execution_time + INTERVAL '1 day' WHERE server_id = @server_id AND first_execution_time < @cutoff",
            before.Date.AddDays(1), ct);
        var after = await FloorAsync(rig, ct);
        Assert.True(after.Date > before.Date, $"floor day should move: {before:o} -> {after:o}");

        var plan = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.True(plan.UseTable && plan.BelowFloorStart is not null, $"{plan.UseTable} {plan.StartBound} {plan.ReadStart:o} below {plan.BelowFloorStart:o} floor {after:o}");
        Assert.Equal(2, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));
    }

    [Fact]
    public async Task WindowEndInTheFutureOrInsideTheSettleMargin_IsNotCached_TheNextDecisionRereads()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();
        try
        {
            var floor = await FloorAsync(rig, ct);
            /* The window end lies in the FUTURE (the clock is a day past the floor day). The gap read runs directly:
               a young table is clamped before the read, so the cache path is exercised on its own. */
            var now = floor.Date.AddDays(1);
            QueryStoreIntervalWide.GapCacheClock = () => now;
            Assert.True(floor.Date.AddDays(QueryStoreIntervalWide.GapCacheWindowDays) > now);
            await QueryStoreIntervalWide.MaxCollectionGapCachedAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.ServerId, floor, 5, 60, ct);
            await QueryStoreIntervalWide.MaxCollectionGapCachedAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.ServerId, floor, 5, 60, ct);
            Assert.Equal(2, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));

            /* Inside the settle margin (catch-up cap plus five minutes) it is still recomputed. */
            now = floor.Date.AddDays(QueryStoreIntervalWide.GapCacheWindowDays) + WatermarkPolicy.MaxCatchup + QueryStoreIntervalWide.GapSettleMargin - TimeSpan.FromMinutes(1);
            await QueryStoreIntervalWide.MaxCollectionGapCachedAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.ServerId, floor, 5, 60, ct);
            await QueryStoreIntervalWide.MaxCollectionGapCachedAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.ServerId, floor, 5, 60, ct);
            Assert.Equal(4, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));

            /* One minute later it is settled: read once, then served from the cache. */
            now = now.AddMinutes(1);
            await QueryStoreIntervalWide.MaxCollectionGapCachedAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.ServerId, floor, 5, 60, ct);
            await QueryStoreIntervalWide.MaxCollectionGapCachedAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.ServerId, floor, 5, 60, ct);
            Assert.Equal(5, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));
        }
        finally
        {
            QueryStoreIntervalWide.ResetGapCacheForTests();
        }
    }

    [Fact]
    public async Task SupersetWindow_ClampsOnAGapPastTwoDays_ThatTheExactWindowDoesNotSee()
    {
        var ct = TestContext.Current.CancellationToken;
        var s = QueryStoreIntervalWideBelowFloorLiveTests.S;
        /* A three-hour hole in the log 2 d 6 h after the table's floor day (the oldest interval starts 45 days before S). */
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct, logHoleFrom: s.AddDays(-45).AddDays(2).AddHours(6));
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();

        var floor = await FloorAsync(rig, ct);
        Assert.Equal(s.AddDays(-45).Date, floor.Date);
        Assert.True(floor.TimeOfDay < TimeSpan.FromHours(6), "the hole must sit past floor + 2 d");

        /* The exact [floor, floor + 2 d] window sees no hole. */
        await using (var exact = new NpgsqlCommand(QueryStoreIntervalWide.MaxCollectionGapSql, rig.Connection))
        {
            exact.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = QueryStoreIntervalWideBelowFloorLiveTests.ServerId });
            exact.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
            exact.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor.AddDays(2) });
            var seconds = (double)(await exact.ExecuteScalarAsync(ct))!;
            Assert.True(seconds <= QueryStoreIntervalWide.MaxBelowFloorCadence.TotalSeconds);
        }

        /* The cached three-day window does: the conservative trade, never a permissive one. */
        var plan = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.Null(plan.BelowFloorStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.RawFloorSlowCadence, plan.StartBound);
    }

    [Fact]
    public async Task YoungTable_WindowReachingIntoTheFuture_ClampsWithTheLogNotYetCoveringReason_AndReadsNoGap()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();
        try
        {
            var floor = await FloorAsync(rig, ct);
            var now = floor.Date.AddDays(1);
            QueryStoreIntervalWide.GapCacheClock = () => now;
            var plan = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
            Assert.True(plan.UseTable);
            Assert.Null(plan.BelowFloorStart);
            Assert.Equal(QueryStoreIntervalWide.WideStartBound.RawFloorLogNotYetCovering, plan.StartBound);
            Assert.Equal(0, QueryStoreIntervalWide.GapReadsForStoreForTests(rig.Connection));
        }
        finally
        {
            QueryStoreIntervalWide.ResetGapCacheForTests();
        }
    }

    [Fact]
    public async Task FaultingCadenceProbe_StillServesTheAtFloorTableRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(1), ct);
        QueryStoreIntervalWide.ResetGapCacheForTests();

        /* The probe's table disappears: the probe throws, and only the below-floor part is given up. */
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection, "ALTER TABLE config.config_collector_schedules RENAME TO config_collector_schedules_gone", null, ct);
        var plan = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.Null(plan.BelowFloorStart);
        Assert.Equal(plan.ClampedStart, plan.ReadStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.RawFloorSlowCadence, plan.StartBound);
    }
}
