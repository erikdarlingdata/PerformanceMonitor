/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: the live exactness proof for the hourly-raw-edges Compose route, first half. Each fact seeds its own scratch
/// store (a fresh data source too, so the per-data-source rollup and coverage cache cannot leak between facts), refreshes
/// <c>collect.query_stats_interval_hourly</c> up to the anchor's hour, and runs a fleet Ranked SUM of <c>query_worker_us</c>
/// through <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/> over a 24-hour window that ends at the anchor. Every
/// fact then asserts two things: the payload SQL names <c>query_stats_interval_hourly AS mid</c> (the hybrid really was
/// taken, so the comparison is not raw against raw), and the payload rows, serialized as JSON, equal byte for byte the rows
/// of the SAME panel compiled with no verdict (the raw route) and run on its own connection. The raw side is produced by
/// <see cref="ComposeCompiler.Compile"/> on a <see cref="ComposeRunContext"/> built from the same window, rollups and
/// coverage, and its rows are serialized with the runner's own value mapping (<see cref="DarlingWebEndpoints.DbValueToJson"/>).
/// Seeds are placed relative to the whole-hour middle <c>[h1, h2)</c> computed here the way the router does.
///
/// <para>The whole class runs on ONE fixed anchor (<see cref="Anchor"/>), passed to the runner as its <c>nowUtc</c> seam. The
/// seeds, the aggregate refresh range, h1 and h2, the window and the raw compile's context are all built from it, so no fact
/// waits for an hour edge or races the hour turning over, and a slow migration cannot push the run past the router's one-minute
/// end slack. Two things on the compose path still read the real clock: the rollup-probe cache lifetime and the coverage
/// floor-reuse lifetime. Every fact opens a fresh data source, so a fact's probe is always a first probe and neither lifetime
/// can change which route is taken. A pin below keeps this class from reading the wall clock or sleeping.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every fact reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposeHourlyRawEdgesGuardLiveTests
{
    private const int ServerIdA = -47001;
    private const int ServerIdB = -47002;
    private const string ServerA = "EdgesServerA";
    private const string ServerB = "EdgesServerB";
    private const string Hybrid = "query_stats_interval_hourly AS mid";

    private const string PanelByDatabase =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}";

    private const string PanelByDatabaseAndObject =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";

    /// <summary>The one instant every fact runs at: 37 minutes and 21 seconds into an hour, so the window's start is not on an
    /// hour and h1 is the next whole hour (23 whole hours of middle). Its kind is Unspecified on purpose: the guard binds the
    /// router's hour instants to timestamp-without-time-zone parameters, and Npgsql refuses a Kind=Utc value there.</summary>
    private static readonly DateTime Anchor = new(2026, 1, 5, 12, 37, 21, DateTimeKind.Unspecified);

    /// <summary>The on-the-hour variant: the 24-hour window starts exactly on an hour, so h1 equals the window's start.</summary>
    private static readonly DateTime OnTheHourAnchor = new(2026, 1, 5, 12, 0, 0, DateTimeKind.Unspecified);

    private static long s_collectionId = Anchor.Ticks;

    /// <summary>Where the hours fall for an anchor and a 24-hour window: h1 is the first whole hour at or after the window's
    /// start, h2 the last whole hour at or before its end (the router's CeilHour and FloorHour).</summary>
    private readonly record struct Layout(DateTime H1, DateTime H2)
    {
        public static Layout For(DateTime anchor)
        {
            var h2 = FloorHour(anchor);
            var start = anchor.AddHours(-24);
            var h1 = start == FloorHour(start) ? start : FloorHour(start).AddHours(1);
            return new Layout(h1, h2);
        }
    }

    [Fact]
    public async Task RestartRowsOnly_InTheMiddle_GiveNullValues_TheHybridKeeps_AsRawDoes()
    {
        await using var store = await StoreAsync();
        var h1 = store.Layout.H1;
        await SeedAnchorAsync(store);
        /* every middle-hour row of this group is a restart marker: raw's measured-delta filter leaves it NULL */
        await QsAsync(store, ServerA, "RestartOnlyDb", "HR", "0xR", h1.AddHours(2).AddMinutes(5), 777, 0);
        await QsAsync(store, ServerA, "RestartOnlyDb", "HR", "0xR", h1.AddHours(3).AddMinutes(5), 888, 0);
        await QsAsync(store, ServerB, "RestartOnlyDb", "HR", "0xR", h1.AddHours(5).AddMinutes(5), 999, 0);
        await QsAsync(store, ServerA, "NormalDb", "HN", "0xN", h1.AddHours(4).AddMinutes(5), 500, 300);
        await store.RefreshAsync();

        var (hybrid, raw) = await RunBothAsync(store, PanelByDatabase);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));

        var restartRow = raw.OfType<JsonObject>().Single(r => r["database_name"]?.GetValue<string>() == "RestartOnlyDb");
        Assert.Null(restartRow["value"]);
    }

    [Fact]
    public async Task EdgeOnlyGroups_OnEachSide_AndARowAtTheWindowEnd_AreCountedOnce()
    {
        await using var store = await StoreAsync();
        var (h1, h2) = (store.Layout.H1, store.Layout.H2);
        await SeedAnchorAsync(store);
        await QsAsync(store, ServerA, "LeftEdgeDb", "HL", "0xL", h1.AddSeconds(-1), 11, 300);
        await QsAsync(store, ServerB, "LeftEdgeDb", "HL", "0xL", h1.AddMinutes(-2), 13, 300);
        await QsAsync(store, ServerA, "RightEdgeDb", "HRT", "0xRT", h2, 17, 300);
        await QsAsync(store, ServerA, "RightEdgeDb", "HRT", "0xRT", h2.AddSeconds(1), 19, 300);
        await store.RefreshAsync();
        /* the row at the window's end is written after the end is fixed, exactly on it: raw's `<=` takes it, so must the hybrid */
        await QsAsync(store, ServerB, "RightEdgeDb", "HRT", "0xRT", store.WindowEnd, 23, 300);

        var (hybrid, raw) = await RunBothAsync(store, PanelByDatabase);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));

        Assert.Equal(24, ValueOf(raw, "LeftEdgeDb"), 6);
        Assert.Equal(17 + 19 + 23, ValueOf(raw, "RightEdgeDb"), 6);
    }

    [Fact]
    public async Task AGroup_StraddlingBothMiddleBoundaries_IsCountedOnceAtEachRow()
    {
        await using var store = await StoreAsync();
        var (h1, h2) = (store.Layout.H1, store.Layout.H2);
        await SeedAnchorAsync(store);
        /* h1 - 1 s (edge), exactly h1 (middle), mid-middle, h2 - 1 s (middle), exactly h2 (edge) */
        await QsAsync(store, ServerA, "StraddleDb", "HS", "0xS", h1.AddSeconds(-1), 1, 300);
        await QsAsync(store, ServerA, "StraddleDb", "HS", "0xS", h1, 10, 300);
        await QsAsync(store, ServerA, "StraddleDb", "HS", "0xS", h1.AddHours(7).AddMinutes(11), 100, 300);
        await QsAsync(store, ServerA, "StraddleDb", "HS", "0xS", h2.AddSeconds(-1), 1000, 300);
        await QsAsync(store, ServerA, "StraddleDb", "HS", "0xS", h2, 10000, 300);
        await ProcAsync(store, ServerA, "StraddleDb", "usp_Straddle", "0xS", h1.AddMinutes(10));
        await store.RefreshAsync();

        foreach (var panel in new[] { PanelByDatabase, PanelByDatabaseAndObject })
        {
            var (hybrid, raw) = await RunBothAsync(store, panel);
            AssertHybridTaken(hybrid);
            Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
            Assert.Equal(1 + 10 + 100 + 1000 + 10000, ValueOf(raw, "StraddleDb"), 6);
        }
    }

    [Fact]
    public async Task ANullSampleInterval_InTheMiddle_IsMeasured_InBothRoutes()
    {
        await using var store = await StoreAsync();
        var h1 = store.Layout.H1;
        await SeedAnchorAsync(store);
        await QsAsync(store, ServerA, "NullIntervalDb", "HI", "0xI", h1.AddHours(6).AddMinutes(20), 4242, null);
        await QsAsync(store, ServerA, "NullIntervalDb", "HI", "0xI", h1.AddHours(6).AddMinutes(40), 8, 300);
        await store.RefreshAsync();

        var (hybrid, raw) = await RunBothAsync(store, PanelByDatabase);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        Assert.Equal(4242 + 8, ValueOf(raw, "NullIntervalDb"), 6);
    }

    [Fact]
    public async Task AProcedureStatsPanel_StaysRaw()
    {
        await using var store = await StoreAsync();
        var h1 = store.Layout.H1;
        for (var hour = 0; hour < 23; hour++)
        {
            await ProcAsync(store, ServerA, "ProcDb", "usp_Proc", "0xP", h1.AddHours(hour).AddMinutes(30), workerTime: 100);
        }

        await store.RefreshAsync(TimescaleSupport.ProcedureStatsIntervalHourlyView);

        var outcome = await RunAsync(store, "{\"source\":\"procedure_stats\",\"measure\":\"proc_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}");
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        var sql = (string)outcome.Payload!["sql"]!;
        Assert.DoesNotContain("_interval_hourly", sql, StringComparison.Ordinal);
        Assert.NotEmpty((JsonArray)outcome.Payload["rows"]!);
    }

    [Fact]
    public async Task ThePlainFleetPanel_TakesTheHybridRoute()
    {
        await using var store = await StoreAsync();
        await SeedAnchorAsync(store);
        await store.RefreshAsync();

        var outcome = await RunAsync(store, PanelByDatabase);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        AssertHybridTaken(outcome.Payload!);
    }

    private const string PanelMax =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"max\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}";

    private const string PanelMin =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"min\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}";

    private const string PanelRankedSeriesAtHour =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"line\"}";

    private const string PanelScalar =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"viz\":\"stat\"}";

    /// <summary>The seed every aggregate and mode variant shares: edge rows on both sides, middle rows at distinct values
    /// (so Max and Min each have a different winner than Sum), a restart-only group, a NULL interval, and the row exactly
    /// on each middle boundary.</summary>
    private static async Task SeedMixedAsync(Store store)
    {
        var (h1, h2) = (store.Layout.H1, store.Layout.H2);
        await SeedAnchorAsync(store);
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", h1.AddSeconds(-1), 5000, 300);
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", h1, 7, 300);
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", h1.AddHours(3).AddMinutes(10), 4000, 300);
        await QsAsync(store, ServerB, "MixedDb", "HM", "0xM", h1.AddHours(9).AddMinutes(12), 3, 300);
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", h1.AddHours(9).AddMinutes(30), 2500, null);
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", h2.AddSeconds(-1), 90, 300);
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", h2, 6000, 300);
        await QsAsync(store, ServerA, "RestartDb", "HR", "0xR", h1.AddHours(4).AddMinutes(5), 777, 0);
        await QsAsync(store, ServerB, "EdgeDb", "HE", "0xE", h2.AddSeconds(2), 11, 300);
        await QsAsync(store, ServerA, "EdgeDb", "HE", "0xE", h1.AddMinutes(-30), 13, 300);
        await store.RefreshAsync();
    }

    [Fact]
    public async Task AMaxPanel_TakesTheHybridRoute_AndMatchesRaw()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        var (hybrid, raw) = await RunBothAsync(store, PanelMax);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        Assert.Equal(6000, ValueOf(raw, "MixedDb"), 6);
    }

    [Fact]
    public async Task AMinPanel_TakesTheHybridRoute_AndMatchesRaw()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        var (hybrid, raw) = await RunBothAsync(store, PanelMin);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        Assert.Equal(3, ValueOf(raw, "MixedDb"), 6);
    }

    [Fact]
    public async Task ARankedTimeSeriesPanel_AtHourGrain_TakesTheHybridRoute_AndMatchesRaw()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        var (hybrid, raw) = await RunBothAsync(store, PanelRankedSeriesAtHour);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        Assert.True(raw.Count > 1, "an hourly series over the seeded window has more than one bucket");
    }

    [Fact]
    public async Task AScalarPanel_TakesTheHybridRoute_AndMatchesRaw()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        var (hybrid, raw) = await RunBothAsync(store, PanelScalar);
        AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        Assert.Single(raw);
    }

    [Fact]
    public async Task ALateNonRestartRow_AfterTheRefresh_MakesTheGuardRefuse_AndTheRowsStillMatchRaw()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        /* one more measured row in the last middle hour, written after the aggregate was refreshed: raw now holds one
           row the aggregate does not, the counts disagree, and the guard must send the panel down the raw route */
        await QsAsync(store, ServerA, "MixedDb", "HM", "0xM", store.Layout.H2.AddHours(-1).AddMinutes(20), 123456, 300);

        var (hybrid, raw) = await RunBothAsync(store, PanelByDatabase);
        Assert.DoesNotContain("_interval_hourly", (string)hybrid["sql"]!, StringComparison.Ordinal);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        Assert.Equal(5000 + 7 + 4000 + 3 + 2500 + 90 + 6000 + 123456, ValueOf(raw, "MixedDb"), 6);
    }

    /// <summary>The premise pin for the whole route. The guard compares row COUNTS, so it can only see rows that appear
    /// or disappear. An in-place UPDATE keeps the count, passes the guard, and the hybrid serves the aggregate's stale
    /// value. This fact documents that on purpose: the route is only exact because raw query_stats is append-only.</summary>
    [Fact]
    public async Task APlantedInPlaceUpdate_PassesTheCountGuard_AndTheHybridDiffersFromRaw_ThePremisePin()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        await using (var update = new NpgsqlCommand(
            "UPDATE collect.query_stats SET delta_worker_time = delta_worker_time + 1000000 WHERE database_name = 'MixedDb' AND collection_time = $1", store.Connection))
        {
            update.Parameters.AddWithValue(DateTime.SpecifyKind(store.Layout.H1.AddHours(3).AddMinutes(10), DateTimeKind.Unspecified));
            Assert.Equal(1, await update.ExecuteNonQueryAsync(store.Ct));
        }

        var (hybrid, raw) = await RunBothAsync(store, PanelByDatabase);
        const string Premise = "the hourly-raw-edges route relies on raw query_stats being append-only (COPY-only writes, whole-row retention); an in-place UPDATE passes the count guard unseen. If this ever starts failing because the rows match, the premise or the guard changed: re-check both.";
        Assert.True(((string)hybrid["sql"]!).Contains(Hybrid, StringComparison.Ordinal), Premise);
        Assert.False(raw.ToJsonString() == hybridRows(hybrid), Premise);
    }

    [Fact]
    public async Task AScopedPanel_TakesTheRoute_WhenOnlyAnotherServerMismatches_AndTheFleetPanelDoesNot()
    {
        await using var store = await StoreAsync();
        await SeedMixedAsync(store);

        /* one more measured row in a middle hour for server B ONLY, after the refresh: B's count now disagrees with its
           aggregate, A's does not */
        await QsAsync(store, ServerB, "MixedDb", "HM", "0xM", store.Layout.H2.AddHours(-1).AddMinutes(20), 123456, 300);

        /* scoped to A: the guard binds a non-null text[] for $3, finds no mismatch in A's scope, and the route is taken */
        var (scoped, scopedRaw) = await RunBothAsync(store, PanelByDatabase, ServerA);
        AssertHybridTaken(scoped);
        Assert.Equal(scopedRaw.ToJsonString(), hybridRows(scoped));
        Assert.Equal(5000 + 7 + 4000 + 2500 + 90 + 6000, ValueOf(scopedRaw, "MixedDb"), 6);

        /* the converse, same store and same panel at fleet scope: B's mismatch is now in scope, so the route is NOT taken
           and the answer is raw's, including the late row */
        var fleet = await RunAsync(store, PanelByDatabase);
        Assert.True(fleet.Error is null, $"compose run failed: {fleet.Error}");
        Assert.DoesNotContain("_interval_hourly", (string)fleet.Payload!["sql"]!, StringComparison.Ordinal);
        var (_, fleetRaw) = await RunBothAsync(store, PanelByDatabase, null);
        Assert.Equal(fleetRaw.ToJsonString(), hybridRows(fleet.Payload!));
        Assert.Equal(5000 + 7 + 4000 + 3 + 2500 + 90 + 6000 + 123456, ValueOf(fleetRaw, "MixedDb"), 6);
    }

    [Fact]
    public async Task AWindowStartingExactlyOnTheHour_TakesTheRoute_WithH1EqualToTheStart_AndMatchesRaw()
    {
        /* hours = 24 at a whole-hour anchor: start is exactly on an hour. CeilHour leaves an exact hour alone, so h1 == start;
           FloorHour of an exact-hour end is the end itself, so h2 == end. */
        await using var store = await StoreAsync(OnTheHourAnchor);
        var (h1, h2) = (store.Layout.H1, store.Layout.H2);
        Assert.Equal(store.WindowStart, h1);
        Assert.Equal(store.WindowEnd, h2);

        await SeedAnchorAsync(store);
        await QsAsync(store, ServerA, "OnHourDb", "HH", "0xH", h1.AddSeconds(-1), 5000, 300);
        await QsAsync(store, ServerA, "OnHourDb", "HH", "0xH", h1, 10, 300);
        await QsAsync(store, ServerA, "OnHourDb", "HH", "0xH", h2, 20, 300);
        await store.RefreshAsync();

        var (hybrid, raw) = await RunBothAsync(store, PanelByDatabase, relativeWindow: true);
                AssertHybridTaken(hybrid);
        Assert.Equal(raw.ToJsonString(), hybridRows(hybrid));
        /* the row one second before the start is outside the window; the rows exactly on the start and on the end are inside */
        Assert.Equal(10 + 20, ValueOf(raw, "OnHourDb"), 6);
    }

    [Fact]
    public void ThisClass_ReadsNoWallClock_AndNeverWaits()
    {
        var source = RepoFile.ReadRepoFile("Darling", "Darling.Tests", "ComposeHourlyRawEdgesGuardLiveTests.cs");
        /* the needles are assembled so this pin does not match itself */
        foreach (var needle in new[] { "Utc" + "Now", "Date" + "Time.Now", "Offset" + ".Now", "Task." + "Delay", "Thread." + "Sleep", "Time" + "Provider" })
        {
            Assert.DoesNotContain(needle, source, StringComparison.Ordinal);
        }
    }

    /* ---- helpers ---- */

    private static string hybridRows(JsonObject payload) => payload["rows"]!.ToJsonString();

    private static void AssertHybridTaken(JsonObject payload) =>
        Assert.Contains(Hybrid, (string)payload["sql"]!, StringComparison.Ordinal);

    private static double ValueOf(JsonArray rows, string database)
    {
        var row = rows.OfType<JsonObject>().First(r => r["database_name"]?.GetValue<string>() == database);
        /* the panel answers in its display unit (ms); the seeds are microseconds */
        return row["value"]!.GetValue<double>() * 1000.0;
    }

    /// <summary>Runs the panel through the runner and through a raw compile of the same window, and returns both payloads'
    /// rows: the runner's payload, and the raw rows as a JSON array.</summary>
    private static async Task<(JsonObject Hybrid, JsonArray Raw)> RunBothAsync(Store store, string panelJson, string? scope = null, bool relativeWindow = false)
    {
        var outcome = await RunAsync(store, panelJson, scope, relativeWindow);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");

        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(store.DataSource, store.Ct);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panelJson)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        /* the route must have been a candidate for exactly the hours this test computed, or the layout is wrong */
        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(plan!, store.Anchor, store.WindowStart, store.WindowEnd, rollups, coverage);
        Assert.NotNull(candidate);
        Assert.Equal(store.Layout.H1, candidate!.HourStartUtc);
        Assert.Equal(store.Layout.H2, candidate.HourEndUtc);

        var rawContext = new ComposeRunContext(scope is null ? null : new[] { scope }, store.WindowStart, store.WindowEnd, ComposeRunContext.NoVariables, rollups, store.Anchor, coverage);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, rawContext);
        Assert.True(compileError is null, compileError);
        Assert.DoesNotContain("_interval_hourly", compiled!.Sql, StringComparison.Ordinal);

        await using var connection = await store.DataSource.OpenConnectionAsync(store.Ct);
        await using var command = new NpgsqlCommand(compiled.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var rows = new JsonArray();
        await using var reader = await command.ExecuteReaderAsync(store.Ct);
        while (await reader.ReadAsync(store.Ct))
        {
            var row = new JsonObject();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : DarlingWebEndpoints.DbValueToJson(reader.GetValue(i));
            }

            rows.Add(row);
        }

        Assert.NotEmpty(rows);
        return (outcome.Payload!, rows);
    }

    /// <summary>Runs the panel at the store's anchor. By default the window is the explicit pair; with
    /// <paramref name="relativeWindow"/> the body carries <c>hours = 24</c> instead, and the runner computes the same window
    /// itself (end = the anchor, start = 24 h before it). A scope puts the server name in the body's <c>server</c> field.</summary>
    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(Store store, string panelJson, string? scope = null, bool relativeWindow = false)
    {
        var body = new JsonObject { ["panel"] = JsonNode.Parse(panelJson) };
        if (relativeWindow)
        {
            body["hours"] = 24;
        }
        else
        {
            body["windowStart"] = store.WindowStart.ToString("o", CultureInfo.InvariantCulture);
            body["windowEnd"] = store.WindowEnd.ToString("o", CultureInfo.InvariantCulture);
        }

        if (scope is not null)
        {
            body["server"] = scope;
        }

        return DarlingWebEndpoints.RunComposedPanelAsync(store.DataSource, body, store.Ct, nowUtc: store.Anchor);
    }

    /// <summary>One non-restart row per hour in [h1, h2), on the anchor group, so the successor has a bucket at h1 and a
    /// measured ceiling at h2.</summary>
    private static async Task SeedAnchorAsync(Store store)
    {
        for (var h = store.Layout.H1; h < store.Layout.H2; h = h.AddHours(1))
        {
            await QsAsync(store, ServerA, "AnchorDb", "HA", "0xA", h.AddMinutes(30), 1000, 300);
        }
    }

    private static async Task QsAsync(Store store, string server, string database, string hash, string handle, DateTime at, long worker, int? interval)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 1, $9)", store.Connection);
        insert.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(server == ServerA ? ServerIdA : ServerIdB);
        insert.Parameters.AddWithValue(server);
        insert.Parameters.AddWithValue(database);
        insert.Parameters.AddWithValue(hash);
        insert.Parameters.AddWithValue(handle);
        insert.Parameters.AddWithValue(worker);
        insert.Parameters.AddWithValue(interval is int value ? value : DBNull.Value);
        await insert.ExecuteNonQueryAsync(store.Ct);
    }

    private static async Task ProcAsync(Store store, string server, string database, string objectName, string handle, DateTime at, long workerTime = 1)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $8, 1, 300)", store.Connection);
        insert.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(server == ServerA ? ServerIdA : ServerIdB);
        insert.Parameters.AddWithValue(server);
        insert.Parameters.AddWithValue(database);
        insert.Parameters.AddWithValue(objectName);
        insert.Parameters.AddWithValue(handle);
        insert.Parameters.AddWithValue(workerTime);
        await insert.ExecuteNonQueryAsync(store.Ct);
    }

    private static async Task<Store> StoreAsync(DateTime? anchor = null)
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #4605 hourly-raw-edges live test.");
        var ct = TestContext.Current.CancellationToken;

        var at = anchor ?? Anchor;
        var layout = Layout.For(at);

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        NpgsqlConnection? connection = null;
        NpgsqlDataSource? dataSource = null;
        try
        {
            connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The hourly-raw-edges route reads a continuous aggregate: TimescaleDB is required.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerB, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            /* a FRESH data source per fact: the rollup and coverage probe is cached per data source */
            dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            return new Store(scratch, connection, dataSource, at, layout, ct);
        }
        catch
        {
            if (dataSource is not null)
            {
                await dataSource.DisposeAsync();
            }

            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            await scratch.DisposeAsync();
            throw;
        }
    }

    private static DateTime FloorHour(DateTime value) => new(value.Year, value.Month, value.Day, value.Hour, 0, 0, DateTimeKind.Utc);

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        public Store(ScratchPostgres scratch, NpgsqlConnection connection, NpgsqlDataSource dataSource, DateTime anchor, Layout layout, CancellationToken ct)
        {
            _scratch = scratch;
            Connection = connection;
            DataSource = dataSource;
            Layout = layout;
            Ct = ct;
            /* the window: 24 h ending at the anchor, so the test and the runner bind the same instants */
            Anchor = anchor;
            WindowEnd = anchor;
            WindowStart = anchor.AddHours(-24);
        }

        public NpgsqlConnection Connection { get; }
        public NpgsqlDataSource DataSource { get; }
        public DateTime Anchor { get; }
        public Layout Layout { get; }
        public CancellationToken Ct { get; }
        public DateTime WindowStart { get; }
        public DateTime WindowEnd { get; }

        /// <summary>Materializes the successor (or the one named) for every whole hour before h2.</summary>
        public async Task RefreshAsync(string view = TimescaleSupport.QueryStatsIntervalHourlyView)
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", Connection);
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(Layout.H1.AddHours(-2), DateTimeKind.Unspecified));
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(Layout.H2, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync(Ct);
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await Connection.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
