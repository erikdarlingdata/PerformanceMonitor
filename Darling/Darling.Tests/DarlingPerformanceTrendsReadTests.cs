/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Performance-Trends siblings (#2484): get_procedure_duration_trend and
/// get_query_store_duration_trend, plus the two-branch empty answer all three siblings now share.
///
/// <para>Three things here are worth more than the round-trip. The procedure trend must read
/// <c>procedure_stats</c> and not <c>query_stats</c>, or it is a second name for its sibling. The Query
/// Store trend must count each runtime interval ONCE, at the hour the work ran, or a busy interval is
/// charged again to every cycle that re-fetched it. And the execution rate must survive being below one
/// per second, which the shipped integer field does not.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPerformanceTrendsReadTests
{
    private const int ServerId = -949555;
    private const string ServerName = "performance-trends-read";
    private const string Db = "AppDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheSiblings_ReadTheirOwnSources_AndSayWhichNothingTheyFound_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live performance-trends test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── never sampled: all three siblings refuse to look like a quiet server ── */
            var neverProcs = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", neverProcs.GetProperty("status").GetString());
            var neverProcsText = neverProcs.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverProcsText, StringComparison.Ordinal);
            Assert.Contains("NOT a quiet server", neverProcsText, StringComparison.Ordinal);
            Assert.DoesNotContain("widen", neverProcsText, StringComparison.OrdinalIgnoreCase);

            var neverStore = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", neverStore.GetProperty("status").GetString());

            /*
                Query Store has a cause of emptiness the other two do not: it can simply be OFF on every
                database. Naming the collector first would send someone to the wrong place.
            */
            Assert.Contains("Query Store may be OFF", neverStore.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* get_query_duration_trend used to answer "unavailable" for BOTH kinds of empty. */
            var neverQueries = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", neverQueries.GetProperty("status").GetString());
            Assert.Contains("EVER", neverQueries.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* ── sampled, but outside the window: a quiet window, and widening IS the move ── */
            await SeedProcedureAsync(connection, ct, HoursAgo(48), executions: 10, elapsedUs: 5_000_000);
            await SeedQueryAsync(connection, ct, HoursAgo(48), executions: 10, elapsedUs: 5_000_000);

            var quietProcs = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName, 1)).RootElement;
            Assert.Equal("empty", quietProcs.GetProperty("status").GetString());
            var quietProcsText = quietProcs.GetProperty("message").GetString()!;
            Assert.Contains("widen", quietProcsText, StringComparison.Ordinal);

            /* Same zero points as the branch above, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", quietProcsText, StringComparison.Ordinal);
            Assert.NotEqual(neverProcsText, quietProcsText);

            var quietQueries = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, 1)).RootElement;
            Assert.Equal("empty", quietQueries.GetProperty("status").GetString());
            Assert.Contains("widen", quietQueries.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* #3541 A2: every branch carries the disclosure block — the never-sampled and quiet envelopes
               above included — so a caller reads `source` without first checking whether it got data. On
               an empty raw answer the requested start stands and the served span is the whole window. */
            foreach (var envelope in new[] { neverProcs, neverStore, neverQueries, quietProcs, quietQueries })
            {
                AssertDisclosureBlock(envelope);
                Assert.Equal("raw", envelope.GetProperty("source").GetString());
                Assert.False(envelope.GetProperty("window_truncated").GetBoolean());
            }

            /* Not asserted: effective_hours_back on the empty raw answers. It is derived from the store's
               GLOBAL oldest raw row, and on the shared fixture that is whatever another live test left
               behind — the scratch-store class below pins it against a store it owns. */
            Assert.Equal("per-interval", neverStore.GetProperty("bucket").GetString());
            Assert.Equal("per-collection", quietQueries.GetProperty("bucket").GetString());

            /*
                ── the procedure series, at a rate BELOW one execution per second ──
                Two snapshots five minutes apart, two executions between them: 0.0067/sec. The shipped
                integer field truncates that to 0, which reads as an idle server; the double does not.
                This is the whole reason executions_per_second exists.

                These rows carry no sample_interval_seconds (the pre-V128 shape), so the read LAG-derives
                the interval — and the FIRST snapshot, which has nothing to LAG against, is UNRATED: two
                points come back, the first with null rates (#3541 A12 — kept, not dropped, so a lone
                collection is never an empty series and effective_start is the first collection the store
                held; never the fabricated 0.0 it was before #3540), the envelope counting it and saying
                why, and the second carrying the rate under test.
            */
            await SeedProcedureAsync(connection, ct, MinutesAgo(20), executions: 0, elapsedUs: 0);
            await SeedProcedureAsync(connection, ct, MinutesAgo(15), executions: 2, elapsedUs: 600_000);

            var procs = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName, 4)).RootElement;
            var procTrend = procs.GetProperty("trend");
            Assert.Equal(2, procTrend.GetArrayLength());

            var first = procTrend[0];
            Assert.Equal(JsonValueKind.Null, first.GetProperty("value").ValueKind);
            Assert.Equal(JsonValueKind.Null, first.GetProperty("elapsed_ms_per_second").ValueKind);
            Assert.Equal(JsonValueKind.Null, first.GetProperty("execution_count").ValueKind);
            Assert.Equal(JsonValueKind.Null, first.GetProperty("executions_per_second").ValueKind);
            Assert.Equal(1, procs.GetProperty("unrated_points").GetInt32());
            /* #3653: the note names both unrated reasons — the first-in-window LAG this seed exercises and the
               stored-0 restart it does not — in the one sentence Lite carries byte-identical. */
            var unratedNote = procs.GetProperty("unrated_note").GetString()!;
            Assert.Contains("rated against the PREVIOUS one and has none inside the window", unratedNote, StringComparison.Ordinal);
            Assert.Contains("STORED sample interval is 0", unratedNote, StringComparison.Ordinal);

            var second = procTrend[1];
            Assert.True(second.GetProperty("value").GetDouble() > 0, "elapsed ms/sec must be a real rate");
            Assert.Equal(0, second.GetProperty("execution_count").GetInt64());
            Assert.True(
                second.GetProperty("executions_per_second").GetDouble() > 0,
                "executions_per_second must survive a rate below 1/sec that execution_count truncates to zero");

            /* #3541 A2: the unit is in the field name now. Same quantity as `value`, kept beside it on the
               execution_count precedent. */
            Assert.Equal(second.GetProperty("value").GetDouble(), second.GetProperty("elapsed_ms_per_second").GetDouble());

            /*
                #3541 A2: the disclosure block. A 4-hour window anchored at now sits inside the raw horizon
                (the shared fixture carries no continuous aggregates — every test that builds them mints a
                ScratchPostgres — so raw is also the only tier here), and the series the store held begins
                at the 20-minutes-ago seed — the unrated first collection, kept since #3541 A12 exactly so
                effective_start can say so — and the head sits three-plus hours past the requested start,
                which is what `window_truncated` means (the window floor, #3653 item 17). The point is that the label matches the data rather than
                the request.
            */
            Assert.Equal("raw", procs.GetProperty("source").GetString());
            Assert.Equal("per-collection", procs.GetProperty("bucket").GetString());
            Assert.Equal(JsonValueKind.Null, procs.GetProperty("aggregate_note").ValueKind);
            Assert.False(procs.TryGetProperty("routing", out _));
            Assert.Equal(procTrend[0].GetProperty("time").GetString(), procs.GetProperty("effective_start").GetString());
            Assert.True(procs.GetProperty("window_truncated").GetBoolean());
            var effectiveHours = procs.GetProperty("effective_hours_back").GetDouble();
            Assert.InRange(effectiveHours, 0.2, 0.5);

            /*
                ── the Query Store series: each interval counted ONCE, at the hour the work ran ──
                Two runtime intervals, each fetched twice while it was open, the second fetch carrying the
                higher cumulative count. Charging every fetch to its collection time would give four points
                and double the work; the dedup + placement gives two, at the interval starts.
            */
            /* One base instant for both, so the two interval starts are EXACTLY an hour apart. Two
               separate UtcNow reads truncated to the second can land 3599 apart and quietly break the
               rate assertion below for a reason that has nothing to do with the read. */
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var intervalA = baseNow.AddHours(-3);
            var intervalB = baseNow.AddHours(-2);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(150), intervalId: 41, intervalStart: intervalA, executions: 10, avgDurationUs: 2000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(145), intervalId: 41, intervalStart: intervalA, executions: 40, avgDurationUs: 2000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(90), intervalId: 42, intervalStart: intervalB, executions: 5, avgDurationUs: 3000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(85), intervalId: 42, intervalStart: intervalB, executions: 25, avgDurationUs: 3000);

            var store = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, ServerName, 6)).RootElement;
            var storeTrend = store.GetProperty("trend");

            /* Four rows in, two points out — one per interval, not one per fetch. The first interval has no
               predecessor to difference against, so its rates are null, not 0 (#3541 A12). */
            Assert.Equal(2, storeTrend.GetArrayLength());
            Assert.StartsWith(intervalA.ToString("o")[..16], storeTrend[0].GetProperty("time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(JsonValueKind.Null, storeTrend[0].GetProperty("executions_per_second").ValueKind);
            Assert.StartsWith(intervalB.ToString("o")[..16], storeTrend[1].GetProperty("time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(1, store.GetProperty("unrated_points").GetInt32());

            /*
                The surviving snapshot is the FINAL one (25 executions over the 3600 seconds between the two
                interval starts), not the first (5) and not their sum (30). A dedup keeping the wrong row
                would still return two points and would still look right.
            */
            Assert.Equal(
                25d / 3600d,
                storeTrend[1].GetProperty("executions_per_second").GetDouble(),
                6);

            /* #3541 A2: the Query Store sibling speaks the same disclosure — raw-only here (no corrected
               rollup on the shared fixture), its grain named as the interval placement it uses. */
            AssertDisclosureBlock(store);
            Assert.Equal("raw", store.GetProperty("source").GetString());
            Assert.Equal("per-interval", store.GetProperty("bucket").GetString());
            Assert.False(store.TryGetProperty("routing", out _));
            Assert.Equal(storeTrend[0].GetProperty("time").GetString(), store.GetProperty("effective_start").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The six keys every Performance-Trends envelope carries since #3541 A2, in the order they are
    /// written — the same order on the data path, the empty path, and on Lite.</summary>
    internal static void AssertDisclosureBlock(JsonElement envelope)
    {
        var keys = envelope.EnumerateObject().Select(p => p.Name).ToArray();
        var block = new[] { "source", "effective_start", "effective_hours_back", "window_truncated", "bucket", "aggregate_note" };
        var at = Array.IndexOf(keys, "source");
        Assert.True(at >= 0, "the envelope has no `source`");
        Assert.Equal(block, keys.Skip(at).Take(block.Length).ToArray());
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
     delta_execution_count, delta_elapsed_time, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            Db, "dbo", "usp_Trend", executions, elapsedUs, elapsedUs / 2);

    private static async Task SeedQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_elapsed_time, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            Db, "0xTRENDSIB", executions, elapsedUs, elapsedUs / 2);

    private static async Task SeedQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long intervalId,
        DateTime intervalStart, long executions, long avgDurationUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, 7L, 9L, "Regular", executions, avgDurationUs, intervalId,
            DarlingMcpTestData.Naive(intervalStart));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM procedure_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

/// <summary>
/// #3541 A2 end-to-end against a REAL TimescaleDB: get_query_duration_trend and
/// get_procedure_duration_trend serve a window the raw tier cannot hold from the hourly rollup, and say so.
///
/// <para>Live rather than a string pin because the load-bearing claims are about which RELATION answered
/// and what it computed: that the rollup-served point is the hour's summed work over the bucket width
/// (no LAG, so no first-point question at all), that the payload's <c>source</c> / <c>effective_start</c> /
/// <c>window_truncated</c> describe the served series rather than the request, and that the empty branch on this
/// route names an unserved head instead of a quiet window. The raw-only read of the SAME fixture is
/// asserted beside it as the revert-proof: put the raw-only read back and the rates, the point count and
/// the <c>source</c> word all go red, not just the routing.</para>
///
/// <para>Fixed instants, not now-relative: <see cref="DarlingTrendReader.ResolveTier"/> measures the
/// window's age against the WALL CLOCK, so a window anchored in March 2026 is past the raw horizon on every
/// day this test can run, and every rate below is exact arithmetic off the seed.</para>
///
/// <para><b>#1776 own-store</b> — mints a scratch database (it creates continuous aggregates the shared
/// fixture must never inherit), so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class DarlingPerformanceTrendsTierRoutingLiveTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int ServerId = -935411;

    private const int NeverSampledServerId = -935412;

    private const string ServerName = "duration-trend-tier-e2e";

    private const string NeverSampledServerName = "duration-trend-tier-never";

    [Fact]
    public async Task PastTheRawHorizon_TheTrio_ServesTheHourlyRollup_AndSaysSo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #3541 A2 tier-routing test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, NeverSampledServerId, NeverSampledServerName, ct);

        var hour10 = new DateTime(2026, 3, 4, 10, 0, 0, DateTimeKind.Unspecified);
        var hour11 = hour10.AddHours(1);
        var hour12 = hour10.AddHours(2);

        /* ── query_stats: two collections in the 10:00 hour (3.6 s + 7.2 s elapsed, 36 + 72 executions),
              one in the 11:00 hour (1.8 s, 18). The hourly answer is the hour's sum over 3,600 s:
              10:00 → 10,800 ms / 3,600 = 3.0 ms/s and 108 / 3,600 = 0.03 exec/s; 11:00 → 0.5 ms/s, 0.005. ── */
        await SeedQueryAsync(connection, ct, hour10.AddMinutes(5), executions: 36, elapsedUs: 3_600_000);
        await SeedQueryAsync(connection, ct, hour10.AddMinutes(20), executions: 72, elapsedUs: 7_200_000);
        await SeedQueryAsync(connection, ct, hour11.AddMinutes(15), executions: 18, elapsedUs: 1_800_000);

        /* ── procedure_stats: 10:00 → 3.6 s / 18 (1.0 ms/s, 0.005 exec/s); 11:00 → 0.36 s / 2. ── */
        await SeedProcedureAsync(connection, ct, hour10.AddMinutes(5), executions: 9, elapsedUs: 1_800_000);
        await SeedProcedureAsync(connection, ct, hour10.AddMinutes(20), executions: 9, elapsedUs: 1_800_000);
        await SeedProcedureAsync(connection, ct, hour11.AddMinutes(15), executions: 2, elapsedUs: 360_000);

        await EnsureAggregatesWithoutRefreshPoliciesAsync(connection, ct);

        /* Materialize the 10:00 and 11:00 buckets only. */
        await RefreshRangeAsync(connection, TimescaleSupport.QueryStatsHourlyView, hour10, hour12, ct);
        await RefreshRangeAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, hour10, hour12, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* ── the route resolves from what the store has and has materialized: past the horizon by age,
              rollup present, floor at 10:00, raw no deeper than the floor → hourly. ── */
        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, ct);
        var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(hour10.AddHours(-4), rollups, coverage);
        Assert.Equal(RetentionTier.Hourly, route.Tier);
        Assert.Equal(hour10, route.Coverage.HourlyFloorUtc);
        Assert.Equal(hour10.AddMinutes(5), route.Coverage.RawOldestUtc);

        /* ── the query trend, 06:00–12:00: two hourly points, exact rates, the disclosure describing them ── */
        var queries = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryDurationTrend(
            postgres, ServerName, hours_back: 6, as_of: "2026-03-04T12:00:00Z")).RootElement;

        DarlingPerformanceTrendsReadTests.AssertDisclosureBlock(queries);
        Assert.Equal("hourly", queries.GetProperty("source").GetString());
        Assert.Equal("1 hour", queries.GetProperty("bucket").GetString());
        Assert.Contains(TimescaleSupport.QueryStatsHourlyView, queries.GetProperty("aggregate_note").GetString(), StringComparison.Ordinal);
        Assert.Contains("3,600 seconds", queries.GetProperty("aggregate_note").GetString(), StringComparison.Ordinal);
        Assert.StartsWith("2026-03-04T10:00:00", queries.GetProperty("effective_start").GetString()!, StringComparison.Ordinal);
        Assert.Equal(2.0, queries.GetProperty("effective_hours_back").GetDouble());
        Assert.True(queries.GetProperty("window_truncated").GetBoolean(), "the served series begins four hours after the requested start");

        var queryTrend = queries.GetProperty("trend");
        Assert.Equal(2, queryTrend.GetArrayLength());
        Assert.StartsWith("2026-03-04T10:00:00", queryTrend[0].GetProperty("time").GetString()!, StringComparison.Ordinal);
        Assert.Equal(3.0, queryTrend[0].GetProperty("value").GetDouble(), 9);
        Assert.Equal(3.0, queryTrend[0].GetProperty("elapsed_ms_per_second").GetDouble(), 9);
        Assert.Equal(0.03, queryTrend[0].GetProperty("executions_per_second").GetDouble(), 9);
        Assert.StartsWith("2026-03-04T11:00:00", queryTrend[1].GetProperty("time").GetString()!, StringComparison.Ordinal);
        Assert.Equal(0.5, queryTrend[1].GetProperty("value").GetDouble(), 9);
        Assert.Equal(0.005, queryTrend[1].GetProperty("executions_per_second").GetDouble(), 9);

        /* ── the procedure trend, same window, its own pair ── */
        var procedures = JsonDocument.Parse(await DarlingMcpTrendTools.GetProcedureDurationTrend(
            postgres, ServerName, hours_back: 6, as_of: "2026-03-04T12:00:00Z")).RootElement;

        Assert.Equal("hourly", procedures.GetProperty("source").GetString());
        Assert.Contains(TimescaleSupport.ProcedureStatsHourlyView, procedures.GetProperty("aggregate_note").GetString(), StringComparison.Ordinal);
        var procedureTrend = procedures.GetProperty("trend");
        Assert.Equal(2, procedureTrend.GetArrayLength());
        Assert.Equal(1.0, procedureTrend[0].GetProperty("value").GetDouble(), 9);
        Assert.Equal(0.005, procedureTrend[0].GetProperty("executions_per_second").GetDouble(), 9);
        Assert.Equal(0.1, procedureTrend[1].GetProperty("value").GetDouble(), 9);

        /* ── the raw-only read of the SAME fixture: the estimator this replaced, and the revert-proof.
              Three per-collection points, the first UNRATED (null) because the LAG idiom has no previous
              collection to difference against — before #3541 A12 that point was published as a fabricated
              0.0, the quiet instant the bucket-width denominator never produced. Restoring the raw-only
              read unconditionally fails the count, the rates and the `source` word above, not just a
              routing flag; restoring the ELSE 0 fails the null here. ── */
        var rawRoute = route with { Tier = RetentionTier.Raw };
        var raw = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, ServerId, hour10.AddHours(-4), hour12, rawRoute, ct);

        Assert.Equal(3, raw.Points.Count);
        Assert.Equal(hour10.AddMinutes(5), raw.Points[0].CollectionTime);
        Assert.False(raw.Points[0].HasRate);
        Assert.Null(raw.Points[0].Value);
        Assert.Null(raw.Points[0].ExecutionCount);
        Assert.Null(raw.Points[0].ExecutionsPerSecond);
        Assert.Equal(8.0, raw.Points[1].Value!.Value, 9);                 /* 7,200 ms over the 900 s since 10:05 */
        Assert.Equal(1800d / 3300d, raw.Points[2].Value!.Value, 9);      /* 1,800 ms over the 3,300 s since 10:20 */
        /* The unrated point is KEPT, so the series still says truthfully where the store's data begins. */
        Assert.Equal(hour10.AddMinutes(5), raw.EffectiveStartUtc);
        Assert.Equal("raw", rawRoute.Source);

        /* ── never sampled on this route: not an empty window. The raw probe finds nothing and so does the
              rollup — "unavailable", with the disclosure block still attached. ── */
        var never = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryDurationTrend(
            postgres, NeverSampledServerName, hours_back: 6, as_of: "2026-03-04T12:00:00Z")).RootElement;

        Assert.Equal("unavailable", never.GetProperty("status").GetString());
        Assert.Contains("EVER", never.GetProperty("message").GetString()!, StringComparison.Ordinal);
        DarlingPerformanceTrendsReadTests.AssertDisclosureBlock(never);
        Assert.Equal("hourly", never.GetProperty("source").GetString());

        /* ── the branch this fix exists for: sampled, nothing in a window whose head sits BELOW the rollup's
              floor. The pre-routing answer here was "genuinely quiet — widen hours_back", which is false (the
              raw rows were dropped, not absent) and harmful (widening reaches further into what nothing
              holds). The answer now names the tier, its measured floor, the unserved head and the remedy. ── */
        var unserved = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryDurationTrend(
            postgres, ServerName, hours_back: 2, as_of: "2026-03-04T09:00:00Z")).RootElement;

        Assert.Equal("empty", unserved.GetProperty("status").GetString());
        var message = unserved.GetProperty("message").GetString()!;
        Assert.Contains(TimescaleSupport.QueryStatsHourlyView, message, StringComparison.Ordinal);
        Assert.Contains("2026-03-04T10:00:00", message, StringComparison.Ordinal);
        Assert.Contains("UNSERVED rather than quiet", message, StringComparison.Ordinal);
        Assert.Contains("--backfill-rollups", message, StringComparison.Ordinal);
        Assert.Contains("Widening hours_back cannot help", message, StringComparison.Ordinal);
        Assert.DoesNotContain("genuinely quiet", message, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", message, StringComparison.Ordinal);

        /* The disclosure on that empty answer: the tier could first have served at its 10:00 floor, which is
           past the window's end, so the served span is honestly zero and the head is truncated. */
        Assert.Equal("hourly", unserved.GetProperty("source").GetString());
        Assert.StartsWith("2026-03-04T09:00:00", unserved.GetProperty("effective_start").GetString()!, StringComparison.Ordinal);
        Assert.Equal(0.0, unserved.GetProperty("effective_hours_back").GetDouble());
        Assert.True(unserved.GetProperty("window_truncated").GetBoolean());

        /* ── and get_query_trend, the read whose routing the trio now shares, over the same rows: the same
              tier, the same coverage words, the rollup's per-hour sums for the one query. ── */
        var single = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryTrend(
            postgres, "0xTIERHASH", "AppDb", ServerName, hours_back: 6, as_of: "2026-03-04T12:00:00Z")).RootElement;

        Assert.Equal("hourly", single.GetProperty("source").GetString());
        Assert.Equal(queries.GetProperty("effective_start").GetString(), single.GetProperty("effective_start").GetString());
        Assert.Equal(2, single.GetProperty("data_points").GetInt32());
        Assert.Equal(108, single.GetProperty("trend")[0].GetProperty("execution_count").GetInt64());
    }

    private static async Task SeedQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_execution_count, delta_elapsed_time, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName,
            "AppDb", "0xTIERHASH", "0xSQLH", executions, elapsedUs, elapsedUs / 2);

    private static async Task SeedProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
     delta_execution_count, delta_elapsed_time, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName,
            "AppDb", "dbo", "usp_Tier", executions, elapsedUs, elapsedUs / 2);

    /// <summary>
    /// Builds the aggregates, then strips every refresh policy the sweep attached — this test refreshes
    /// manually over exact ranges and asserts exact floors, so a background refresh materializing another
    /// bucket mid-test would move the floor out from under the assertions. Same discipline as
    /// QueryStoreTrendRoutingLiveTests.
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

    /// <summary>Materializes a range, retrying while TimescaleDB reports a CONCURRENT REFRESH (55P03) — the
    /// policy's first check fires immediately on creation, so the scheduler can be mid-materialization when
    /// the manual refresh lands. Bounded, so a genuine stall fails rather than hangs.</summary>
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
