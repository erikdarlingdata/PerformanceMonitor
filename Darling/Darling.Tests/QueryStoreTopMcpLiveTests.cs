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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3953's live pass for the MCP/web <c>get_query_store_top</c> read: <see cref="DarlingDataReader.GetQueryStoreTopAsync(NpgsqlDataSource,int,DateTime,DateTime,int,string,CancellationToken)"/>'s
/// own gate wiring and <see cref="DarlingDataReader.QueryStoreTopTableSql"/>, seeded through the real write path
/// on a real PostgreSQL store. Reuses <see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/> (B2's
/// shared seed, per that file's own doc comment) rather than writing a new one, and adds one extra row carrying
/// a real <c>module_name</c> so the $7 filter this read alone has has something non-null to match.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres and then works entirely inside it (the clamp test
   converts query_store_stats to a real hypertable and drops one of its chunks), so it cannot race live
   collection, and serializing it would be pure slowdown. */
public sealed class QueryStoreTopMcpLiveTests
{
    private const int ServerId = -3953931;
    private const int TestTop = 50;
    private const string ModuleName = "dbo.usp_QueryStoreTopModuleFilter";

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheTableRead_EqualsRaw_UnfilteredAndFiltered_EndToEnd_ThroughMcpAndWebDispatch_AndTheLiveGate()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 MCP top live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        await SeedOneModuleTaggedRowAsync(runner, ct);
        /* Same forcing shape as the grid's own gate test: a real apply raises filled_since only to the wall
           clock it ran at, which sits nowhere near this seed's historical span. */
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);
        await RegisterServerAsync(connection, ct);

        /* applied_through is the wide table's own apply bookkeeping, stamped at the real wall clock the seed
           ran at (like filled_since before ForceFilledSinceAsync forces IT back) -- it is NOT related to this
           seed's historical WindowStart/WindowEnd span. Clause 4 needs a literal end AT OR AFTER it, so every
           gate check below that must PASS uses queryEnd (never the bare historical WindowEnd) as both the
           read's own window end and the gate's literal end; the seed has no rows past day 2 either way, so a
           later bound changes nothing about what raw or the table can return. */
        var appliedThrough = await ScalarDateTimeAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);
        var queryEnd = appliedThrough > WindowEnd ? appliedThrough : WindowEnd;

        /* ---- equality, EXCEPT ALL both ways, unfiltered and with $6/$7 each set ---- */

        await AssertRawEqualsTableAsync(connection, WindowStart, WindowStart, null, null, ct);
        /* $6 set: day0 seeds Regular, Aborted and Exception rows for the same identity — narrowing to one
           outcome must still agree between raw and the table. */
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowStart, "Aborted", null, ct);
        /* $7 set: the one row SeedOneModuleTaggedRowAsync wrote must survive the filter identically on both
           sides, and a module nothing carries must return the empty set on both sides too. */
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowStart, null, ModuleName, ct);
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowStart, null, "dbo.usp_NoSuchModule", ct);

        /* ---- end to end: DarlingDataReader.GetQueryStoreTopAsync lands on the table (V145 is present) ---- */

        var mcpRows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, ServerId, WindowStart, queryEnd, TestTop, null, null, null, ct);
        var rawKeys = await RawTopKeysAsync(connection, WindowStart, queryEnd, null, null, ct);
        Assert.True(rawKeys.Count > 0, "the seed produced no raw rows; the end-to-end comparison would be vacuous");
        var mcpKeys = mcpRows
            .Select(r => (r.DatabaseName, r.QueryId, r.PlanId, r.ExecutionTypeDesc, r.ReplicaRole, r.TotalExecutions))
            .OrderBy(k => k)
            .ToList();
        Assert.Equal(rawKeys.OrderBy(k => k).ToList(), mcpKeys);

        /* ---- the web dispatch reaches the same gate: /api/read/get_query_store_top binds server_name and
           as_of off the query string into the SAME DarlingDataReader.GetQueryStoreTopAsync call. ---- */

        /* hours_back is capped at 168 (ValidateHoursBack) server-side, so this cannot reach back from
           queryEnd (applied_through, real wall-clock) all the way to WindowStart the way the direct calls
           above do -- it uses the historical WindowEnd as its own as_of instead, close enough to WindowStart
           to fit the cap. That may or may not clear gate clause 4 on its own (immaterial here): this test's
           job is proving the dispatch reaches DarlingDataReader.GetQueryStoreTopAsync with the SAME server
           and end the direct call used, not re-proving the gate a second time. */
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(
            $"?server_name=qsiw-grid&hours_back=72&top={TestTop}&as_of={Uri.EscapeDataString(WindowEnd.ToString("o"))}");
        var webJson = await dispatch["get_query_store_top"](context, postgres, null!);
        using var webDoc = JsonDocument.Parse(webJson);
        Assert.True(webDoc.RootElement.TryGetProperty("queries", out var webQueries),
            "web dispatch did not return the success shape: " + webJson);
        var webKeys = webQueries.EnumerateArray()
            .Select(e => (
                e.GetProperty("database_name").GetString()!,
                e.GetProperty("query_id").GetInt64(),
                e.GetProperty("plan_id").GetInt64(),
                e.GetProperty("execution_type").GetString()!,
                e.TryGetProperty("replica_role", out var rr) && rr.ValueKind != JsonValueKind.Null ? rr.GetString() : null,
                e.GetProperty("execution_count").GetInt64()))
            .OrderBy(k => k)
            .ToList();
        var rawKeysThroughWindowEnd = await RawTopKeysAsync(connection, WindowStart, WindowEnd, null, null, ct);
        Assert.True(webKeys.Count > 0, "web dispatch returned no rows; the end-to-end comparison would be vacuous");
        Assert.Equal(rawKeysThroughWindowEnd.OrderBy(k => k).ToList(), webKeys);

        /* ---- the live gate: each refusal clause on a real connection, then the passing case restored.
           Every check below uses queryEnd (>= applied_through) as the literal end unless it is deliberately
           testing clause 4 itself, so each assertion fails for the ONE clause its message names, not a
           different, earlier-firing one. ---- */

        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, queryEnd.AddHours(-1), queryEnd, queryEnd, DarlingDataReader.QueryStoreTopMinWindow, 30, null, ct)).UseTable,
            "a window under 12 hours must read raw");

        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, queryEnd, appliedThrough.AddMinutes(-1), DarlingDataReader.QueryStoreTopMinWindow, 30, null, ct)).UseTable,
            "a literal end (an MCP as_of) before applied_through must read raw");

        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @cutoff, 'qsA', @cutoff)",
            WindowStart, ct);
        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, queryEnd, queryEnd, DarlingDataReader.QueryStoreTopMinWindow, 30, null, ct)).UseTable,
            "a pending batch must read raw regardless of coverage");
        await ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id", ct);

        Assert.True((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, queryEnd, queryEnd, DarlingDataReader.QueryStoreTopMinWindow, 30, null, ct)).UseTable,
            "the passing case, restored, must read the table again");
    }

    /// <summary>
    /// The clamp (review D4R H3, the same rule the grid's own live test proves): once raw's oldest chunk is
    /// dropped, the table still holds those rows — its own retention is independent of raw's — so an unclamped
    /// table read would show rows raw no longer has. Bounding the table read at
    /// <see cref="QueryStoreIntervalWide.ClampedStart"/> instead returns exactly what raw itself can still show.
    /// </summary>
    [Fact]
    public async Task Clamp_MatchesRawOnceRawsOldestChunkIsDropped_AndFailsWithoutTheClamp()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 MCP top clamp test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster for the clamp test's chunk drop");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await ScalarBoolAsync(connection,
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats')", ct),
            "query_store_stats did not convert to a hypertable; the chunk-drop below cannot run");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        var rawFloorBefore = await ScalarDateTimeOrNullAsync(connection, ChunkFloorSql, ct);
        Assert.Equal(WindowStart, QueryStoreIntervalWide.ClampedStart(rawFloorBefore, WindowStart));

        /* Drop every chunk wholly before window day 1 — the anchor's chunk (45 days back) and day 0's own
           chunk. Day 1 and day 2 survive. */
        await ExecAsync(connection, "SELECT drop_chunks('collect.query_store_stats', older_than => @cutoff)", WindowStart.AddDays(1), ct);

        var rawFloorAfter = await ScalarDateTimeOrNullAsync(connection, ChunkFloorSql, ct);
        Assert.True(rawFloorAfter is DateTime f && f > WindowStart, $"expected the drop to move raw's floor past {WindowStart:o}; got {rawFloorAfter:o}");
        var clampedStart = QueryStoreIntervalWide.ClampedStart(rawFloorAfter, WindowStart);
        Assert.True(clampedStart > WindowStart, "expected the clamp to move forward once raw's floor rose above the window start");

        /* Without the clamp — reading the table from windowStart, raw's own (pre-drop) bound — the table still
           shows day 0's rows and raw does not any more: they differ. This is the one place this file proves a
           mismatch on purpose, to show the clamp is load-bearing rather than decorative. */
        var (_, unclampedTableOnly, _, _) = await CompareRawAndTableAsync(connection, WindowStart, WindowStart, null, null, ct);
        Assert.True(unclampedTableOnly > 0, "expected the unclamped table read to show rows raw no longer has, proving the clamp matters");

        /* With the clamp restored: equal again, over the shrunken span raw and the table can both still answer. */
        await AssertRawEqualsTableAsync(connection, WindowStart, clampedStart, null, null, ct);
    }

    private const string ChunkFloorSql = @"
SELECT MIN(range_start) AT TIME ZONE 'UTC'
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect'
AND   hypertable_name = 'query_store_stats';";

    /* ---- seed extras (base seed is QueryStoreIntervalWideGridLiveTests.SeedGridAsync) --------------------- */

    /// <summary>One extra row, an identity distinct from <c>SeedGridAsync</c>'s own six, tagged with a real
    /// <c>module_name</c> so the $7 filter — a filter <c>SeedGridAsync</c>'s shared rows never set — has
    /// something non-null to match on both the raw and the table read.</summary>
    private static async Task SeedOneModuleTaggedRowAsync(DarlingCollectorRunner runner, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "qsiw-grid-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsiw-grid", Host = "qsiw-grid-host" },
            ConnectionString = "Server=qsiw-grid-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsiw-grid-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        var moduleDay = WindowStart.AddDays(1).AddHours(2);
        var row = new QueryStoreCollector.Row
        {
            DatabaseName = "qsA",
            QueryId = 6,
            PlanId = 61,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = moduleDay,
            LastExecutionTime = moduleDay.AddMinutes(9),
            QueryHash = "0x" + 6L.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + 61L.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            ExecutionCount = 7,
            AvgCpuTimeUs = 100,
            AvgDurationUs = 200,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = 600,
            ModuleName = ModuleName,
        };

        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { row }, server, moduleDay.AddMinutes(10), context, ct);
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "qsiw-grid" });
        await command.ExecuteNonQueryAsync(ct);
    }

    /* ---- raw vs. table, over every returned column --------------------------------------------------------- */

    private static async Task AssertRawEqualsTableAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime tableStart, string? executionType, string? moduleName, CancellationToken ct)
    {
        var (rawOnly, tableOnly, rawCount, tableCount) = await CompareRawAndTableAsync(connection, windowStart, tableStart, executionType, moduleName, ct);
        Assert.Equal(rawCount, tableCount);
        Assert.Equal(0, rawOnly);
        Assert.Equal(0, tableOnly);
    }

    /// <summary>
    /// Materialises <see cref="DarlingDataReader.QueryStoreTopSql"/> and <see cref="DarlingDataReader.QueryStoreTopTableSql"/>
    /// into temp tables (so their differently-bound positional parameters never collide in one command) and
    /// diffs them both ways with <c>EXCEPT ALL</c> over every returned column. <paramref name="tableStart"/> is
    /// deliberately a caller-supplied value rather than always <see cref="QueryStoreIntervalWide.ClampedStart"/>,
    /// so the clamp test can pass the unclamped window start and observe the mismatch it causes. The window end
    /// is always <see cref="WindowEnd"/>: unlike the grid, this read's $3 is never NULL.
    /// </summary>
    private static async Task<(long RawOnly, long TableOnly, long RawCount, long TableCount)> CompareRawAndTableAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime tableStart, string? executionType, string? moduleName, CancellationToken ct)
    {
        await ExecAsync(connection, "DROP TABLE IF EXISTS tmp_raw_mcp_top, tmp_table_mcp_top", ct);

        await using (var raw = new NpgsqlCommand("CREATE TEMP TABLE tmp_raw_mcp_top AS " + DarlingDataReader.QueryStoreTopSql, connection))
        {
            raw.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            raw.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified) });
            raw.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(WindowEnd, DateTimeKind.Unspecified) });
            raw.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestTop });
            AddNullableText(raw, null);
            AddNullableText(raw, executionType);
            AddNullableText(raw, moduleName);
            await raw.ExecuteNonQueryAsync(ct);
        }

        await using (var table = new NpgsqlCommand("CREATE TEMP TABLE tmp_table_mcp_top AS " + DarlingDataReader.QueryStoreTopTableSql, connection))
        {
            table.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            table.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(tableStart, DateTimeKind.Unspecified) });
            table.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(WindowEnd, DateTimeKind.Unspecified) });
            table.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestTop });
            AddNullableText(table, null);
            AddNullableText(table, executionType);
            AddNullableText(table, moduleName);
            await table.ExecuteNonQueryAsync(ct);
        }

        var rawCount = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM tmp_raw_mcp_top", ct);
        var tableCount = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM tmp_table_mcp_top", ct);
        var rawOnly = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM ((SELECT * FROM tmp_raw_mcp_top) EXCEPT ALL (SELECT * FROM tmp_table_mcp_top)) AS d", ct);
        var tableOnly = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM ((SELECT * FROM tmp_table_mcp_top) EXCEPT ALL (SELECT * FROM tmp_raw_mcp_top)) AS d", ct);
        return (rawOnly, tableOnly, rawCount, tableCount);
    }

    /// <summary>The end-to-end oracle: <see cref="DarlingDataReader.QueryStoreTopSql"/> itself (raw, unchanged),
    /// read directly rather than through <see cref="DarlingDataReader.GetQueryStoreTopAsync(NpgsqlDataSource,int,DateTime,DateTime,int,string,CancellationToken)"/>
    /// (which would pick the table for this seed), projected to the identity + outcome + total this file
    /// compares end-to-end. Column ordinals match <c>ranked</c>'s final SELECT list exactly (0 database_name,
    /// 1 query_id, 2 plan_id, 7 total_executions, 5 execution_type_desc, 16 replica_role).</summary>
    private static async Task<List<(string DatabaseName, long QueryId, long PlanId, string ExecutionTypeDesc, string? ReplicaRole, long TotalExecutions)>> RawTopKeysAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, string? executionType, string? moduleName, CancellationToken ct)
    {
        var keys = new List<(string, long, long, string, string?, long)>();
        await using var command = new NpgsqlCommand(DarlingDataReader.QueryStoreTopSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestTop });
        AddNullableText(command, null);
        AddNullableText(command, executionType);
        AddNullableText(command, moduleName);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add((
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetString(5),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.GetInt64(7)));
        }

        return keys;
    }

    /* ---- helpers ------------------------------------------------------------------------------------------ */

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        command.Parameters.AddWithValue("cutoff", DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime> ScalarDateTimeAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime?> ScalarDateTimeOrNullAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return result is DateTime dt ? dt : null;
    }
}
