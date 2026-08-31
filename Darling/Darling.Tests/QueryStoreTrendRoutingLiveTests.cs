/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2736 end-to-end against a REAL TimescaleDB: get_query_store_duration_trend serves the materialized
/// window portion from <c>query_store_stats_corrected_hourly</c> and ranks raw ONLY over the
/// unmaterialized tail — and says so in its payload.
///
/// <para>Live rather than a string pin because the load-bearing claims are about which RELATION serves
/// which region: that the rollup route's pre-boundary points carry the rollup's dedup (an interval
/// re-fetched N times counts once) at the rollup's COLLECTION-hour placement, that the tail keeps the
/// raw arm's interval-start placement, and that the two regions meet at the boundary with nothing counted
/// twice and nothing dropped. A string pin can only say the SQL mentions the view; only a store can say
/// what it then computes.</para>
///
/// <para><b>The routing changes the answer, and that is asserted, not hidden</b>: the same fixture read
/// down the raw-only route places a delayed-fetch interval at its interval START, while the rollup route
/// places it at its collection hour — the #1849 boundary disclosure the payload carries. Asserting both
/// routes side by side is this test's revert-proof: put the raw-only route back unconditionally and the
/// rollup-route assertions go red on placement AND on the payload's source block.</para>
///
/// <para><b>#1776 own-store</b> — mints a scratch database (it creates continuous aggregates the shared
/// fixture must never inherit), so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreTrendRoutingLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -927360;

    private const string ServerName = "qs-trend-routing-e2e";

    [Fact]
    public async Task DurationTrend_ServesTheRollupBelowTheBoundary_AndRanksOnlyTheTail()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #2736 trend-routing test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, TestServerId, ServerName, ct);

        /* Fixed instants, not now-relative: every assertion is exact arithmetic, and a window drifting
           across an hour boundary between seeding and asserting would turn a regression into a flake. */
        var hour10 = new DateTime(2026, 3, 4, 10, 0, 0, DateTimeKind.Unspecified);
        var hour11 = hour10.AddHours(1);
        var hour12 = hour10.AddHours(2);
        var hour13 = hour10.AddHours(3);

        /* ── interval P: starts AND is collected in the 10:00 hour. Two snapshots, 7 then 21 — the honest
              answer is 21, once. ── */
        await SeedSnapshotsAsync(connection, intervalId: 3100, queryId: 60, intervalStart: hour10,
            avgDurationUs: 100, [(hour10.AddMinutes(5), 7L), (hour10.AddMinutes(20), 21L)], ct);

        /* ── interval M: starts at 10:00 but is FETCHED in the 11:00 hour (a delayed fetch). The raw arm
              places it at 10:00 (interval start); the rollup buckets it at 11:00 (collection hour). This
              is the interval that PROVES which estimator served the region. ── */
        await SeedSnapshotsAsync(connection, intervalId: 3101, queryId: 61, intervalStart: hour10,
            avgDurationUs: 100, [(hour11.AddMinutes(5), 10L), (hour11.AddMinutes(10), 40L)], ct);

        /* ── interval N: starts and is collected in the 11:00 hour. 5 then 25 — honest answer 25. ── */
        await SeedSnapshotsAsync(connection, intervalId: 3102, queryId: 62, intervalStart: hour11,
            avgDurationUs: 200, [(hour11.AddMinutes(20), 5L), (hour11.AddMinutes(40), 25L)], ct);

        /* ── interval T: the TAIL — starts and is collected in the 12:00 hour, which the refresh below
              deliberately does NOT materialize. Served by the raw arm, deduped, at its interval start. ── */
        await SeedSnapshotsAsync(connection, intervalId: 3103, queryId: 63, intervalStart: hour12,
            avgDurationUs: 300, [(hour12.AddMinutes(5), 3L), (hour12.AddMinutes(20), 9L)], ct);

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        /* Materialize ONLY the 10:00 and 11:00 buckets — the 12:00 hour stays raw, the exact state a live
           store's refresh end_offset leaves the recent edge in. L1 first, then the corrected hourly that
           reads it: refreshing out of order materializes nothing and reports success. */
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, hour10, hour12, ct);
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, hour10, hour12, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* ── the route resolves from what the store actually materialized ── */
        var route = await QueryStoreTrendRouting.ResolveAsync(postgres, ct);
        Assert.True(route.UseRollup);
        Assert.Equal(hour12, route.RawStartUtc);
        Assert.Equal(hour10, route.RollupFloorUtc);

        /* ── the routed read: rollup below the boundary, raw tail above it ── */
        var points = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(
            postgres, TestServerId, hour10.AddHours(-3), hour13, route, ct);

        Assert.Equal(3, points.Count);

        /* 10:00 — rollup bucket: interval P only (21, once). Interval M ran at 10:00 but was FETCHED at
           11:00, so the rollup charges it to 11:00 — the collection-hour placement the payload discloses. */
        Assert.Equal(hour10, points[0].CollectionTime);

        /* 11:00 — rollup bucket: M's final snapshot (40) + N's (25) = 65 executions over the 3,600 seconds
           since the previous point. Un-deduped this hour would be 10+40+5+25 = 80 — the rank the rollup
           already did. */
        Assert.Equal(hour11, points[1].CollectionTime);
        Assert.Equal(65d / 3600d, points[1].ExecutionsPerSecond, 6);
        Assert.Equal(((40d * 100d + 25d * 200d) / 1000d) / 3600d, points[1].Value, 6);

        /* 12:00 — the raw tail: interval T deduped to its final snapshot (9, not 3+9), placed at its
           interval start, rated over the seam to the last rollup bucket. */
        Assert.Equal(hour12, points[2].CollectionTime);
        Assert.Equal(9d / 3600d, points[2].ExecutionsPerSecond, 6);

        /* ── the raw-only route on the SAME fixture: the estimator this replaced. Interval M lands at its
              interval START (10:00 — so that hour reads 21+40=61) and the 11:00 point carries only N. This
              pair of reads is the revert-proof: unconditionally restoring raw-only makes the assertions
              above fail on placement, not just on speed. ── */
        var rawPoints = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(
            postgres, TestServerId, hour10.AddHours(-3), hour13, ct);

        Assert.Equal(3, rawPoints.Count);
        Assert.Equal(hour10, rawPoints[0].CollectionTime);
        Assert.Equal(hour11, rawPoints[1].CollectionTime);
        Assert.Equal(25d / 3600d, rawPoints[1].ExecutionsPerSecond, 6);
        Assert.Equal(9d / 3600d, rawPoints[2].ExecutionsPerSecond, 6);

        /* ── the MCP payload discloses the routing: which relation served which region, and that the
              window's head reaches below the rollup's floor ── */
        var payload = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryStoreDurationTrend(
            postgres, ServerName, hours_back: 6, as_of: "2026-03-04T13:00:00Z")).RootElement;

        Assert.Equal(3, payload.GetProperty("trend").GetArrayLength());

        var source = payload.GetProperty("source");
        Assert.Equal("rollup+raw", source.GetProperty("tier").GetString());
        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, source.GetProperty("rollup").GetString());
        Assert.StartsWith("2026-03-04T12:00:00", source.GetProperty("raw_from").GetString()!, StringComparison.Ordinal);

        /* The window starts at 07:00, below the rollup's 10:00 floor — a degraded answer must label
           itself: points before the floor are missing, not zero, and the remedy is named. */
        Assert.StartsWith("2026-03-04T10:00:00", source.GetProperty("unserved_before").GetString()!, StringComparison.Ordinal);
        Assert.Contains("--backfill-rollups", source.GetProperty("unserved_note").GetString()!, StringComparison.Ordinal);

        /* ── a window ENTIRELY below the floor is a coverage gap, not a quiet server: the empty answer
              names the mechanism and the remedy instead of advising a wider window. ── */
        var belowFloor = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryStoreDurationTrend(
            postgres, ServerName, hours_back: 1, as_of: "2026-03-04T09:30:00Z")).RootElement;

        Assert.Equal("empty", belowFloor.GetProperty("status").GetString());
        var message = belowFloor.GetProperty("message").GetString()!;
        Assert.Contains("--backfill-rollups", message, StringComparison.Ordinal);
        Assert.Contains("2026-03-04T10:00:00", message, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Plants one interval as explicit (collection time, cumulative count) snapshots — the placement of
    /// snapshots relative to bucket boundaries IS what this class tests, so each is spelled out and the
    /// expectations read straight off the seed.
    /// </summary>
    private static async Task SeedSnapshotsAsync(
        NpgsqlConnection connection, long intervalId, long queryId, DateTime intervalStart,
        long avgDurationUs, (DateTime When, long Count)[] snapshots, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
VALUES
    ((extract(epoch FROM $1)::bigint * 100000) + $2, $1, $3, $4, 'RoutingDb', 'dbo.GetOrders', '0xROUTE',
     $5, $5, 'Regular', 'PRIMARY', $2, $6, $6, $7, $8, $8, 900, 400)";

        foreach (var (when, count) in snapshots)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(when);
            command.Parameters.AddWithValue(intervalId);
            command.Parameters.AddWithValue(TestServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(queryId);
            command.Parameters.AddWithValue(intervalStart);
            command.Parameters.AddWithValue(count);
            command.Parameters.AddWithValue(avgDurationUs);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// Builds the aggregates, then strips every refresh policy the sweep attached — this test refreshes
    /// manually over exact ranges and asserts exact boundaries, so a background refresh materializing the
    /// 12:00 bucket mid-test would move the boundary out from under the assertions. Same discipline as
    /// QueryStoreCorrectedRollupLiveTests.
    /// </summary>
    private static async Task EnsureAggregatesWithoutRefreshPoliciesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            await using var remove = new NpgsqlCommand(
                $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)", connection);
            await remove.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// Materializes a range, retrying while TimescaleDB reports a CONCURRENT REFRESH (55P03) — the policy's
    /// first check fires immediately on creation (#1564/#1567), so the scheduler can be mid-materialization
    /// when the manual refresh lands. A refresh is idempotent; the losing side is merely early. Bounded, so
    /// a genuine stall fails rather than hangs.
    /// </summary>
    private static async Task RefreshRangeAsync(
        NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        const int maxAttempts = 12;

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var refresh = new NpgsqlCommand(
                    $"CALL refresh_continuous_aggregate('collect.{view}', $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(from);
                refresh.Parameters.AddWithValue(to);
                await refresh.ExecuteNonQueryAsync(ct);
                return;
            }
            catch (PostgresException ex) when (ex.SqlState == "55P03" && attempt < maxAttempts)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }
    }
}
