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
/// <c>collect.query_stats_interval_hourly</c> up to the current hour, and runs a fleet Ranked SUM of <c>query_worker_us</c>
/// through <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/> over an explicit 24-hour window that ends now. Every
/// fact then asserts two things: the payload SQL names <c>query_stats_interval_hourly AS mid</c> (the hybrid really was
/// taken, so the comparison is not raw against raw), and the payload rows, serialized as JSON, equal byte for byte the rows
/// of the SAME panel compiled with no verdict (the raw route) and run on its own connection. The raw side is produced by
/// <see cref="ComposeCompiler.Compile"/> on a <see cref="ComposeRunContext"/> built from the same window, rollups and
/// coverage, and its rows are serialized with the runner's own value mapping (copied below, since the runner's helper is
/// private). Seeds are placed relative to the whole-hour middle <c>[h1, h2)</c> computed here the way the router does.
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

    private static long s_collectionId = DateTime.UtcNow.Ticks;

    /// <summary>Where the hours fall: the window ends within [h2 + 3 s, h2 + 57 min], so every seed offset below stays
    /// inside the window and the hour does not turn over between the seed and the run.</summary>
    private readonly record struct Layout(DateTime H1, DateTime H2);

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
    private static async Task<(JsonObject Hybrid, JsonArray Raw)> RunBothAsync(Store store, string panelJson)
    {
        var outcome = await RunAsync(store, panelJson);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");

        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(store.DataSource, store.Ct);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panelJson)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        /* the route must have been a candidate for exactly the hours this test computed, or the layout is wrong */
        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(plan!, DateTime.UtcNow, store.WindowStart, store.WindowEnd, rollups, coverage);
        Assert.NotNull(candidate);
        Assert.Equal(store.Layout.H1, candidate!.HourStartUtc);
        Assert.Equal(store.Layout.H2, candidate.HourEndUtc);

        var rawContext = new ComposeRunContext(null, store.WindowStart, store.WindowEnd, ComposeRunContext.NoVariables, rollups, DateTime.UtcNow, coverage);
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
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : ToJson(reader.GetValue(i));
            }

            rows.Add(row);
        }

        Assert.NotEmpty(rows);
        return (outcome.Payload!, rows);
    }

    /* A copy of the runner's private DbValueToJson, so the raw rows serialize the way the payload's do. */
    private static JsonValue? ToJson(object value) => value switch
    {
        DateTime dt => JsonValue.Create(dt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture)),
        double d => JsonValue.Create(d),
        float f => JsonValue.Create((double)f),
        decimal m => JsonValue.Create((double)m),
        long l => JsonValue.Create(l),
        int n => JsonValue.Create(n),
        short s => JsonValue.Create((int)s),
        bool b => JsonValue.Create(b),
        string str => JsonValue.Create(str),
        _ => JsonValue.Create(value.ToString()),
    };

    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(Store store, string panelJson)
    {
        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(panelJson),
            ["windowStart"] = store.WindowStart.ToString("o", CultureInfo.InvariantCulture),
            ["windowEnd"] = store.WindowEnd.ToString("o", CultureInfo.InvariantCulture),
        };

        return DarlingWebEndpoints.RunComposedPanelAsync(store.DataSource, body, store.Ct);
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

    private static async Task<Store> StoreAsync()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #4605 hourly-raw-edges live test.");
        var ct = TestContext.Current.CancellationToken;

        /* The window must end between three seconds and 57 minutes into an hour; wait out the rare start of an hour. */
        var now = DateTime.UtcNow;
        while ((now - FloorHour(now)) < TimeSpan.FromSeconds(3) || (now - FloorHour(now)) > TimeSpan.FromMinutes(57))
        {
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
            now = DateTime.UtcNow;
        }

        var h2 = FloorHour(now);
        var layout = new Layout(h2.AddHours(-23), h2);

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
            return new Store(scratch, connection, dataSource, layout, ct);
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

        public Store(ScratchPostgres scratch, NpgsqlConnection connection, NpgsqlDataSource dataSource, Layout layout, CancellationToken ct)
        {
            _scratch = scratch;
            Connection = connection;
            DataSource = dataSource;
            Layout = layout;
            Ct = ct;
            /* the explicit window: 24 h ending now, whole milliseconds, so the test and the runner bind the same instants */
            var now = DateTime.UtcNow;
            WindowEnd = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMillisecond), DateTimeKind.Utc);
            WindowStart = WindowEnd.AddHours(-24);
        }

        public NpgsqlConnection Connection { get; }
        public NpgsqlDataSource DataSource { get; }
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
