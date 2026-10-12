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
/// #4605: the live exactness proof for the module-map overlay on the hourly-raw-edges Compose route. A panel that groups on
/// <c>object_name</c> resolves a module name from <c>collect.module_map</c> plus a recent overlay of <c>procedure_stats</c>
/// (everything at or after the map's watermark less the refresh slack) instead of ranking the whole window. Each fact seeds its
/// own scratch store, seeds <c>procedure_stats</c> and <c>query_stats</c>, refreshes the map with an EXPLICIT clock (the
/// anchor), and runs the same <c>database_name, object_name</c> panel two ways: through
/// <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/>, which reads the watermark inside its snapshot and so takes the
/// overlay route, and through a raw compile of the same window (no verdict, no watermark). The payload rows must equal, byte for
/// byte, the raw rows serialized with the runner's own value mapping, and the hybrid's SQL must name
/// <c>module_map AS mm</c> (the overlay really ran, so the comparison is not raw against raw) and the interval table. The one
/// fact where no watermark exists asserts the opposite: the SQL names no <c>module_map AS mm</c> and is today's, and the rows
/// are equal all the same. Each fact also pins the module names it expects, so two routes that were both wrong cannot agree.
///
/// <para>The whole class runs on ONE fixed anchor (<see cref="Anchor"/>), passed to the runner as its <c>nowUtc</c> seam and to the
/// map refresh as its clock. A rename that lands in the last minute between the window end and now is the one documented
/// difference of the route, and no fact seeds one: every seed is at or before the window end.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every fact reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposeHourlyRawEdgesModuleMapLiveTests
{
    private const int ServerIdA = -47001;
    private const int ServerIdB = -47002;
    private const string ServerA = "EdgesServerA";
    private const string ServerB = "EdgesServerB";
    private const string Database = "ModDb";
    private const string Hybrid = "query_stats_interval_hourly AS mid";
    private const string Overlay = "module_map AS mm";

    private const string PanelByDatabaseAndObject =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";

    /// <summary>The one instant every fact runs at: 37 minutes and 21 seconds into an hour, so h1 is the next whole hour and h2
    /// the whole hour before the anchor. Its kind is Utc, as the clock the runner reads is.</summary>
    private static readonly DateTime Anchor = new(2026, 1, 5, 12, 37, 21, DateTimeKind.Utc);

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
    public async Task ANewHandle_FirstSeenAfterTheWatermark_ReadsItsName_OnBothRoutes()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            await ProcAsync(store, ServerA, Database, "usp_Known", "0xK", h1.AddHours(2));
            await ProcAsync(store, ServerA, Database, "usp_Filler", "0xW", h2.AddHours(-1));
            await QsAsync(store, ServerA, Database, "HK", "0xK", h1.AddHours(3).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerA, Database, "HN", "0xN", h1.AddHours(5).AddMinutes(5), 1000, 300);
            await QsAsync(store, ServerA, Database, "HN", "0xN", h2.AddMinutes(10), 10000, 300);
            Assert.Equal(2, await store.RefreshMapAsync());
            Assert.Equal(h2.AddHours(-1), await store.WatermarkAsync());

            /* the new handle's first procedure_stats row lands after the refresh read: the map has never seen it */
            await ProcAsync(store, ServerA, Database, "usp_New", "0xN", h2.AddMinutes(20));
            await store.RefreshAggregateAsync();

            var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject);
            AssertOverlayTaken(hybrid);
            Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
            Assert.Equal(1000 + 10000, ValueOf(raw, Database, "usp_New"), 6);
            Assert.Equal(100, ValueOf(raw, Database, "usp_Known"), 6);
        });
    }

    [Fact]
    public async Task ARename_AfterTheWatermark_ReadsTheNewName_AndTheOldOneIsNotCountedAgain()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            await ProcAsync(store, ServerA, Database, "usp_Old", "0xR", h1.AddHours(2));
            await ProcAsync(store, ServerA, Database, "usp_Filler", "0xW", h2.AddHours(-1));
            await QsAsync(store, ServerA, Database, "HR", "0xR", h1.AddHours(4).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerA, Database, "HR", "0xR", h2.AddMinutes(10), 1000, 300);
            Assert.Equal(2, await store.RefreshMapAsync());

            /* the same handle is renamed after the refresh: the map still holds the old name, the overlay carries the new one */
            await ProcAsync(store, ServerA, Database, "usp_Renamed", "0xR", h2.AddMinutes(20));
            await store.RefreshAggregateAsync();

            var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject);
            AssertOverlayTaken(hybrid);
            Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
            Assert.Equal(100 + 1000, ValueOf(raw, Database, "usp_Renamed"), 6);
            Assert.DoesNotContain(raw.OfType<JsonObject>(), r => r["object_name"]?.GetValue<string>() == "usp_Old");
        });
    }

    [Fact]
    public async Task ARename_BeforeTheWatermark_AndPickedUpByTheRefresh_ReadsTheNewName()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            await ProcAsync(store, ServerA, Database, "usp_Old", "0xR", h1.AddHours(2));
            await ProcAsync(store, ServerA, Database, "usp_Filler", "0xW", h1.AddHours(3));
            await QsAsync(store, ServerA, Database, "HR", "0xR", h1.AddHours(4).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerA, Database, "HR", "0xR", h2.AddMinutes(10), 1000, 300);
            Assert.Equal(2, await store.RefreshMapAsync());

            /* the rename lands, then a second refresh reads it: the map now holds the new name and the watermark is past it */
            await ProcAsync(store, ServerA, Database, "usp_Renamed", "0xR", h1.AddHours(6));
            await ProcAsync(store, ServerA, Database, "usp_Filler", "0xW", h2.AddHours(-1));
            Assert.Equal(2, await store.RefreshMapAsync());
            Assert.Equal(h2.AddHours(-1), await store.WatermarkAsync());
            Assert.Equal("usp_Renamed", await store.MapObjectNameAsync(ServerA, "0xR"));
            await store.RefreshAggregateAsync();

            var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject);
            AssertOverlayTaken(hybrid);
            Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
            Assert.Equal(100 + 1000, ValueOf(raw, Database, "usp_Renamed"), 6);
            Assert.DoesNotContain(raw.OfType<JsonObject>(), r => r["object_name"]?.GetValue<string>() == "usp_Old");
        });
    }

    [Fact]
    public async Task AMapHandle_LastSeenBeforeTheWindow_WithAQueryStatsRowInIt_ReadsAdHoc_OnBothRoutes()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            /* the module's only procedure_stats row is ten hours before the window opens, so the map holds it with last_seen
               outside the window; the query_stats row of the same handle is inside it */
            await ProcAsync(store, ServerA, Database, "usp_Stale", "0xS", store.WindowStart.AddHours(-10));
            await ProcAsync(store, ServerA, Database, "usp_Filler", "0xW", h2.AddHours(-1));
            await QsAsync(store, ServerA, Database, "HS", "0xS", h1.AddHours(4).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerA, Database, "HS", "0xS", h2.AddMinutes(10), 1000, 300);
            Assert.Equal(2, await store.RefreshMapAsync());
            Assert.Equal("usp_Stale", await store.MapObjectNameAsync(ServerA, "0xS"));
            await store.RefreshAggregateAsync();

            var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject);
            AssertOverlayTaken(hybrid);
            Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
            Assert.Equal(100 + 1000, ValueOf(raw, Database, MeasureCatalog.AdHocLabel), 6);
            Assert.DoesNotContain(raw.OfType<JsonObject>(), r => r["object_name"]?.GetValue<string>() == "usp_Stale");
        });
    }

    [Fact]
    public async Task WithNoStateRow_TheHybridCompilesTodaysSql_AndReadsTheSameNamesAsRaw()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            await ProcAsync(store, ServerA, Database, "usp_Old", "0xR", h1.AddHours(2));
            await QsAsync(store, ServerA, Database, "HR", "0xR", h1.AddHours(4).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerA, Database, "HR", "0xR", h2.AddMinutes(10), 1000, 300);
            Assert.Equal(1, await store.RefreshMapAsync());

            /* the map holds the old name; the state row is gone and the module is renamed afterwards, so a route that
               leaned on the map would read the old name */
            await ProcAsync(store, ServerA, Database, "usp_Renamed", "0xR", h2.AddMinutes(20));
            await store.DeleteStateRowAsync();
            Assert.Null(await store.WatermarkAsync());
            await store.RefreshAggregateAsync();

            var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject);
            Assert.Contains(Hybrid, (string)hybrid["sql"]!, StringComparison.Ordinal);
            Assert.DoesNotContain(Overlay, (string)hybrid["sql"]!, StringComparison.Ordinal);
            Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
            Assert.Equal(100 + 1000, ValueOf(raw, Database, "usp_Renamed"), 6);
        });
    }

    [Fact]
    public async Task AStaleWatermark_OlderThanTheWindowStart_FloorsAtTheStart_AndMatchesRaw()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            /* the only row the refresh reads is before the window opens, so the watermark is older than the start */
            await ProcAsync(store, ServerA, Database, "usp_Old", "0xR", store.WindowStart.AddHours(-6));
            await QsAsync(store, ServerA, Database, "HR", "0xR", h1.AddHours(4).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerA, Database, "HR", "0xR", h2.AddMinutes(10), 1000, 300);
            await QsAsync(store, ServerA, Database, "HN", "0xN", h1.AddHours(8).AddMinutes(5), 10000, 300);
            Assert.Equal(1, await store.RefreshMapAsync());
            Assert.True(await store.WatermarkAsync() < store.WindowStart);

            /* everything inside the window is unrefreshed: a rename of a mapped handle and a handle the map has never seen */
            await ProcAsync(store, ServerA, Database, "usp_Renamed", "0xR", h1.AddHours(1));
            await ProcAsync(store, ServerA, Database, "usp_Fresh", "0xN", h1.AddHours(7));
            await store.RefreshAggregateAsync();

            var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject);
            AssertOverlayTaken(hybrid);
            Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
            Assert.Equal(100 + 1000, ValueOf(raw, Database, "usp_Renamed"), 6);
            Assert.Equal(10000, ValueOf(raw, Database, "usp_Fresh"), 6);
        });
    }

    [Fact]
    public async Task TwoServersReusingOneHandle_ForDifferentModules_EachReadItsOwn_AtFleetAndScopedRuns()
    {
        await RunLiveAsync(async store =>
        {
            var (h1, h2) = (store.Layout.H1, store.Layout.H2);
            await SeedAnchorAsync(store);
            await ProcAsync(store, ServerA, Database, "usp_A_Old", "0xH", h1.AddHours(2));
            await ProcAsync(store, ServerB, Database, "usp_B", "0xH", h1.AddHours(2));
            await ProcAsync(store, ServerA, Database, "usp_Filler", "0xW", h2.AddHours(-1));
            await QsAsync(store, ServerA, Database, "HH", "0xH", h1.AddHours(4).AddMinutes(5), 100, 300);
            await QsAsync(store, ServerB, Database, "HH", "0xH", h1.AddHours(4).AddMinutes(5), 1000, 300);
            await QsAsync(store, ServerB, Database, "HH", "0xH", h2.AddMinutes(10), 10000, 300);
            Assert.Equal(3, await store.RefreshMapAsync());

            /* server A renames the module its handle names; server B's identical handle must keep B's module */
            await ProcAsync(store, ServerA, Database, "usp_A_New", "0xH", h2.AddMinutes(20));
            await store.RefreshAggregateAsync();

            foreach (var scope in new string?[] { null, ServerA, ServerB })
            {
                var (hybrid, raw) = await RunBothAsync(store, PanelByDatabaseAndObject, scope);
                AssertOverlayTaken(hybrid);
                Assert.Equal(raw.ToJsonString(), HybridRows(hybrid));
                if (scope != ServerB)
                {
                    Assert.Equal(100, ValueOf(raw, Database, "usp_A_New"), 6);
                    Assert.DoesNotContain(raw.OfType<JsonObject>(), r => r["object_name"]?.GetValue<string>() == "usp_A_Old");
                }

                if (scope != ServerA)
                {
                    Assert.Equal(1000 + 10000, ValueOf(raw, Database, "usp_B"), 6);
                }
            }
        });
    }

    [Fact]
    public void ThisClass_ReadsNoWallClock_AndNeverWaits()
    {
        var source = RepoFile.ReadRepoFile("Darling", "Darling.Tests", "ComposeHourlyRawEdgesModuleMapLiveTests.cs");
        /* the needles are assembled so this pin does not match itself */
        foreach (var needle in new[] { "Utc" + "Now", "Date" + "Time.Now", "Offset" + ".Now", "Task." + "Delay", "Thread." + "Sleep", "Time" + "Provider" })
        {
            Assert.DoesNotContain(needle, source, StringComparison.Ordinal);
        }
    }

    /* ---- helpers ---- */

    private static string HybridRows(JsonObject payload) => payload["rows"]!.ToJsonString();

    /// <summary>The overlay route ran: the middle came from the interval table and the module names from the map plus the
    /// recent overlay.</summary>
    private static void AssertOverlayTaken(JsonObject payload)
    {
        Assert.Contains(Hybrid, (string)payload["sql"]!, StringComparison.Ordinal);
        Assert.Contains(Overlay, (string)payload["sql"]!, StringComparison.Ordinal);
    }

    private static double ValueOf(JsonArray rows, string database, string objectName)
    {
        var row = rows.OfType<JsonObject>().Single(r =>
            r["database_name"]?.GetValue<string>() == database && r["object_name"]?.GetValue<string>() == objectName);
        /* the panel answers in its display unit (ms); the seeds are microseconds */
        return row["value"]!.GetValue<double>() * 1000.0;
    }

    /// <summary>Runs one fact against its own store, tearing down through the shared cleanup helper: the scratch database
    /// itself is dropped by the store's disposal, so the cleanup has no statements of its own.</summary>
    private static async Task RunLiveAsync(Func<Store, Task> body)
    {
        await using var store = await StoreAsync();
        var bodySucceeded = false;
        try
        {
            await body(store);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(store.ConnectionString, bodySucceeded, async (_, _) => { });
        }
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
    private static async Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(Store store, string panelJson, string? scope = null, bool relativeWindow = false)
    {
        var body = new JsonObject { ["panel"] = JsonNode.Parse(panelJson) };
        if (relativeWindow)
        {
            body["hours"] = 24;
        }
        else
        {
            body["windowStart"] = DateTime.SpecifyKind(store.WindowStart, DateTimeKind.Unspecified).ToString("o", CultureInfo.InvariantCulture);
            body["windowEnd"] = DateTime.SpecifyKind(store.WindowEnd, DateTimeKind.Unspecified).ToString("o", CultureInfo.InvariantCulture);
        }

        if (scope is not null)
        {
            body["server"] = scope;
        }

        /* #4605: the count guard reads the hour ledger, and these facts plant raw rows with direct INSERTs that write none. Right
           before the panel runs, play the writer: recount the ledger from raw over every hour the facts plant into, and move
           counted_since below them (the V164 rung sets it to the next wall-clock hour, after this fixed past anchor). A row a fact
           planted AFTER its refresh is then in the ledger and not in the rollup, as the writer would leave it, so the guard fails. */
        await store.SeedLedgerAsync();
        return await DarlingWebEndpoints.RunComposedPanelAsync(store.DataSource, body, store.Ct, nowUtc: store.Anchor);
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
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #4605 module-map overlay live test.");
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
            Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));

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

        public string ConnectionString => _scratch.ConnectionString;
        public NpgsqlConnection Connection { get; }
        public NpgsqlDataSource DataSource { get; }
        public DateTime Anchor { get; }
        public Layout Layout { get; }
        public CancellationToken Ct { get; }
        public DateTime WindowStart { get; }
        public DateTime WindowEnd { get; }

        /// <summary>Materializes the successor (or the one named) for every whole hour before h2.</summary>
        public async Task RefreshAggregateAsync(string view = TimescaleSupport.QueryStatsIntervalHourlyView)
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", Connection);
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(Layout.H1.AddHours(-2), DateTimeKind.Unspecified));
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(Layout.H2, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync(Ct);
        }

        /// <summary>Runs the map's hourly refresh against the anchor as its clock and returns the rows it upserted.</summary>
        public Task<int> RefreshMapAsync() => DarlingModuleMap.RefreshRecentAsync(Connection, null, Anchor, Ct);

        public Task<DateTime?> WatermarkAsync() => DarlingModuleMap.ReadWatermarkAsync(Connection, Ct);

        public async Task DeleteStateRowAsync()
        {
            await using var delete = new NpgsqlCommand("DELETE FROM collect.module_map_state", Connection);
            Assert.Equal(1, await delete.ExecuteNonQueryAsync(Ct));
        }

        public async Task<string?> MapObjectNameAsync(string server, string handle)
        {
            await using var read = new NpgsqlCommand("SELECT object_name FROM collect.module_map WHERE server_name = $1 AND sql_handle = $2", Connection);
            read.Parameters.AddWithValue(server);
            read.Parameters.AddWithValue(handle);
            return await read.ExecuteScalarAsync(Ct) as string;
        }

        /// <summary>Makes the hour ledger what the collector's writer would have made it for the hours the facts plant into
        /// (<see cref="QueryStatsLedgerSeed.SeedAsync"/>). The scratch database is dropped whole, so the ledger needs no cleanup.</summary>
        public Task SeedLedgerAsync() => QueryStatsLedgerSeed.SeedAsync(Connection, Layout.H1.AddDays(-2), Layout.H2.AddDays(2), Ct);

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await Connection.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
