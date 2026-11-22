/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #8784 part 2: view usage from Query Store statement text. The V297 rung (the dependency list table and the per-day referenced-names
/// summary) and its probe arm; the summary's build from the daily Query Store summary and the stored text (parsed names, a CTE left out, an
/// unparsed statement searched for the object list's views, a statement with no text counted, a rebuilt day only adding, expired days
/// deleted); the reads (a view read from another database by a three-part name counts for its own database; the dependency list's traps
/// left out); and <c>get_object_usage</c> end to end: a view a statement names, one used only through it, and one nothing reaches.
/// </summary>
public sealed class ObjectReferencesLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8784 view usage tests.";
    private const string Server = "objrefs-sql1";
    private const string Db = "Sales";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(Server);

    // ── The rung ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheRungIsV297_AndTheProbeMapsItToV297()
    {
        var rung = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version);
        Assert.Equal(297, rung.Version);
        Assert.Equal("object-references", rung.Name);
        Assert.Contains("collect.object_dependencies", rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectReferencesDaily.Table, rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectReferencesDaily.BuiltTable, rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectReferencesDaily.DatabaseDaysTable, rung.Sql, StringComparison.Ordinal);
        Assert.Contains("to_regclass('collect.object_dependencies')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasObjectReferences", parameters[^1].Name);
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
        all[^1] = false;
        Assert.Equal(rung.Version - 1, (int)method.Invoke(null, all)!);
    }

    [Fact]
    public void TheDependencyListTable_IsExactlyTheSchemaGeneratorsOutput()
    {
        var rung = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var table = PgSchemaGenerator.CreateTable(ObjectDependenciesCollector.Instance)
            .Replace("EXISTS object_dependencies", "EXISTS collect.object_dependencies", StringComparison.Ordinal);
        var index = PgSchemaGenerator.CreateIndex(ObjectDependenciesCollector.Instance)!
            .Replace("ON object_dependencies", "ON collect.object_dependencies", StringComparison.Ordinal);
        Assert.Contains(table, rung, StringComparison.Ordinal);
        Assert.Contains(index, rung, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRungRunsTwice()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var sql = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version).Sql;
        await ExecuteAsync(connection, sql, ct);
        await ExecuteAsync(connection, sql, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table}", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.object_dependencies", ct));
    }

    // ── The summary ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TheSummary_ParsesTheDaysStatements_SearchesUnparsedText_OnlyAdds_AndDropsExpiredDays()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = new DateTime(2026, 11, 14, 12, 0, 0);
        var day = new DateOnly(2026, 11, 10);
        var expired = new DateOnly(2026, 5, 1);

        await PlantStatementAsync(connection, day, Db, 1, 40, "(@p1 int)SELECT a FROM dbo.vOrders WHERE id = @p1", now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, Db, 2, 2, "WITH vCte AS (SELECT 1 AS a) SELECT a FROM vCte JOIN dbo.vOrders ON 1 = 1", now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, Db, 3, 5, "SELEC broken FROM dbo.vLegacy", now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, Db, 4, 9, text: null, now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, "Reports", 5, 7, "SELECT * FROM Sales.dbo.vCrossDb", now.AddDays(-4), ct);
        await PlantBuiltAsync(connection, day, now.AddDays(-3), ct);
        await PlantInventoryAsync(connection, Db, "vLegacy", "VIEW", now.AddDays(-1), ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Old', '', 'dbo', 'vGone', false, DATE '{expired:yyyy-MM-dd}', 1, 1, NULL, false)", ct);

        var first = await ObjectReferencesDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(1, first.Built);
        Assert.Equal(0, first.Failed);

        Assert.Equal(42L, await ScalarAsync<long>(connection, RefCount("vOrders"), ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE lower(referenced_name) = 'vcte'", ct));
        Assert.Equal(5L, await ScalarAsync<long>(connection, RefCount("vLegacy"), ct));
        Assert.True(await ScalarAsync<bool>(connection, $"SELECT text_matched FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vlegacy'", ct));
        /* The key is folded once, by the aggregator (#8784 review), so two builds that meet two spellings key one row; the spelling as seen
           is kept beside it for display. */
        Assert.Equal("sales", await ScalarAsync<string>(connection,
            $"SELECT referenced_database FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vcrossdb'", ct));
        Assert.Equal("vOrders", await ScalarAsync<string>(connection,
            $"SELECT referenced_name_as_seen FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vorders'", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE lower(referenced_name) = 'vgone'", ct));

        Assert.Equal(4L, await ScalarAsync<long>(connection,
            $"SELECT statements FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT statements_without_text FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT statements_unparsed FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));

        /* Nothing new: the next tick builds nothing. */
        Assert.Equal(0, (await ObjectReferencesDaily.RunTickAsync(dataSource, now.AddHours(1), NullLogger.Instance, ct)).Built);

        /* The day is rebuilt from a source whose text has since been pruned: the build only adds, so the count stays. The rebuild also meets
           the view spelled another way first, and still adds to the one row. */
        await ExecuteAsync(connection, $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId} AND query_id = 1", ct);
        await PlantStatementAsync(connection, day, Db, 0, 1, "SELECT a FROM DBO.VORDERS", now.AddDays(-4), ct);
        await PlantBuiltAsync(connection, day, now.AddDays(-1), ct);
        Assert.Equal(1, (await ObjectReferencesDaily.RunTickAsync(dataSource, now.AddHours(2), NullLogger.Instance, ct)).Built);
        Assert.Equal(42L, await ScalarAsync<long>(connection, RefCount("vOrders"), ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE server_id = {ServerId} AND lower(referenced_name) = 'vorders'", ct));

        /* The rebuild's spelling does not replace the first one, and the read hands back the spelling as seen, not the folded key: a view
           guessed from the summary where no object list is kept keeps a name a case-sensitive collation resolves. */
        Assert.Equal("vOrders", await ScalarAsync<string>(connection,
            $"SELECT referenced_name_as_seen FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vorders'", ct));
        var read = await DarlingObjectUsageReader.ReadReferencesAsync(
            dataSource, ServerId, Db, now.AddDays(-90), ct);
        Assert.Contains(read.References, r => r.ObjectName == "vOrders");
        Assert.DoesNotContain(read.References, r => r.ObjectName == "vorders");
    }

    // ── The reads and the tool ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetObjectUsage_ReferencesAViewByText_ReachesOneThroughIt_AndCallsTheRestUnused()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Server, ct);

        var now = LiveClock.Now();
        foreach (var view in new[] { "vOuter", "vInner", "vOld", "vCrossDb", "vCustomer", "vBehindSynonym" })
        {
            await PlantInventoryAsync(connection, Db, view, "VIEW", now.AddDays(-1), ct);
        }

        /* Only a stored ALL lets a view reach Not used in window (#8784 review): an unknown capture mode holds it at Not tracked. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, query_capture_mode) VALUES ($1,$2,$3,$4,$5,$6)",
            1L, DarlingMcpTestData.Naive(now.AddDays(-5)), ServerId, Server, Db, "ALL");

        /* Statement text on 85 of the 90 days, with vOuter named on one of them and vCrossDb read from another database. */
        for (var d = 2; d <= 86; d++)
        {
            await ExecuteAsync(connection,
                $"INSERT INTO {ObjectReferencesDaily.DatabaseDaysTable} VALUES ({ServerId}, '{Db}', DATE '{now.AddDays(-d):yyyy-MM-dd}', 10, 0, 0, 100)", ct);
        }

        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, '{Db}', '', 'dbo', 'vOuter', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 12, 1, NULL, false)", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Reports', 'sales', 'dbo', 'vCrossDb', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 4, 1, NULL, false)", ct);
        /* A name another database reads with no database part is that database's object, not this one's. */
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Reports', '', 'dbo', 'vOld', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 4, 1, NULL, false)", ct);

        /* The dependency list: vOuter reads vInner; plus the traps the filter leaves out. */
        await PlantDependencyAsync(connection, "vOuter", "V", null, "dbo", "vInner", now.AddDays(-1), ct);
        await PlantDependencyAsync(connection, "vOuter", "V", null, "sys", "objects", now.AddDays(-1), ct);
        await PlantDependencyAsync(connection, "vOuter", "V", "alias", "col", "value", now.AddDays(-1), ct);
        /* A statement names the synonym synOrders; the collector ships the synonym as a row of its own, type SN. */
        await PlantDependencyAsync(connection, "synOrders", "SN", null, "dbo", "vBehindSynonym", now.AddDays(-1), ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, '{Db}', '', 'dbo', 'synorders', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 3, 1, NULL, false)", ct);
        /* A reporting database's view reads vCustomer by a three-part name, and nothing in Sales names it (#8784 review). An older capture of
           Reports, and a linked-server row, are not read. */
        await PlantDependencyAsync(connection, "vReport", "V", "SALES", "dbo", "vCustomer", now.AddDays(-1), ct, database: "Reports");
        await PlantDependencyAsync(connection, "vReportOld", "V", "Sales", "dbo", "vOld", now.AddDays(-9), ct, database: "Reports");

        var deps = await DarlingObjectUsageReader.ReadDependenciesAsync(dataSource, ServerId, Db, ct);
        Assert.NotNull(deps);
        Assert.Equal(["vBehindSynonym", "vInner"], deps!.Dependencies.Select(d => d.ReferencedName).Order(StringComparer.Ordinal));

        var inbound = Assert.Single(await DarlingObjectUsageReader.ReadInboundDependenciesAsync(dataSource, ServerId, Db, ct));
        Assert.Equal(("Reports", "vReport", "vCustomer"), (inbound.ReferencingDatabase, inbound.ReferencingName, inbound.ReferencedName));

        var json = await DarlingMcpObjectUsageTools.GetObjectUsage(dataSource, Server, Db, 90, false, 100, 0, now, ct);
        var root = JsonDocument.Parse(json).RootElement;
        JsonElement Obj(string name) => root.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == name);

        Assert.Equal(ObjectUsageReport.Referenced, Obj("vOuter").GetProperty("classification").GetString());
        Assert.Equal(12, Obj("vOuter").GetProperty("referencing_executions").GetInt64());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vInner").GetProperty("classification").GetString());
        Assert.Equal("dbo.vOuter", Obj("vInner").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vCrossDb").GetProperty("classification").GetString());
        Assert.Equal(4, Obj("vCrossDb").GetProperty("referencing_executions").GetInt64());
        Assert.Equal(ObjectUsageReport.NotUsed, Obj("vOld").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vCustomer").GetProperty("classification").GetString());
        Assert.Equal("Reports.dbo.vReport", Obj("vCustomer").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vBehindSynonym").GetProperty("classification").GetString());
        Assert.Equal("dbo.synOrders", Obj("vBehindSynonym").GetProperty("used_via").GetString());

        var views = root.GetProperty("coverage")[0].GetProperty("views");
        Assert.Equal(85, views.GetProperty("days_with_data").GetInt32());
        Assert.Equal(850, views.GetProperty("statements").GetInt64());
    }

    [Fact]
    public async Task TheCrossDatabaseRead_ReadsOnlyEachDatabasesNewestCapture()
    {
        /* #8784 review: a max(collection_time) GROUP BY database_name read every kept row for the server on every call. 40 databases with 10
           daily captures of 30 rows each is 12,000 rows kept and 1,200 in the newest captures; the read touches the newest captures, one seek
           per database name and one per database's newest time, and nothing older. */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var newest = DarlingMcpTestData.Naive(LiveClock.AnchoredEnd());
        await using (var plant = new NpgsqlCommand(
            """
            INSERT INTO collect.object_dependencies (collection_id, collection_time, server_id, server_name, database_name, referencing_schema,
                referencing_name, referencing_type, referenced_server, referenced_database, referenced_schema, referenced_name, is_bound,
                is_caller_dependent, is_ambiguous)
            SELECT 9000000000000 + (db * 1000 + cap) * 100 + r, $1 - make_interval(days => cap), $2, $3, 'Db' || lpad(db::text, 2, '0'), 'dbo',
                   'v' || r, 'V', NULL, CASE WHEN r = 1 THEN 'Sales' END, 'dbo', CASE WHEN r = 1 THEN 'vShared' || cap ELSE 't' || r END, true,
                   false, false
            FROM generate_series(1, 40) AS db, generate_series(0, 9) AS cap, generate_series(1, 30) AS r
            """, connection))
        {
            plant.Parameters.AddWithValue(newest);
            plant.Parameters.AddWithValue(ServerId);
            plant.Parameters.AddWithValue(Server);
            await plant.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE collect.object_dependencies", ct);

        /* Only the newest capture's row of each database (vShared0) comes back, not the nine older ones. */
        var inbound = await DarlingObjectUsageReader.ReadInboundDependenciesAsync(dataSource, ServerId, "Sales", ct);
        Assert.Equal(40, inbound.Count);
        Assert.All(inbound, e => Assert.Equal("vShared0", e.ReferencedName));

        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingObjectUsageReader.InboundDependenciesSql, connection);
        explain.Parameters.AddWithValue(ServerId);
        explain.Parameters.AddWithValue("Sales");
        using var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
        var read = RowsReadFrom(plan.RootElement[0].GetProperty("Plan"), "object_dependencies");
        Assert.True(read <= 1_200 + 40 * 4, $"the cross-database read touched {read:N0} rows of 12,000 kept; the newest captures hold 1,200");
    }

    /// <summary>The rows the plan's scans of <paramref name="relation"/> produced or filtered out, over all their loops.</summary>
    private static double RowsReadFrom(JsonElement node, string relation)
    {
        double rows = 0;
        if (node.TryGetProperty("Relation Name", out var name) && name.GetString() == relation)
        {
            var loops = node.TryGetProperty("Actual Loops", out var l) ? l.GetDouble() : 1;
            var produced = node.TryGetProperty("Actual Rows", out var a) ? a.GetDouble() : 0;
            var removed = node.TryGetProperty("Rows Removed by Filter", out var f) ? f.GetDouble() : 0;
            rows += (produced + removed) * loops;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                rows += RowsReadFrom(child, relation);
            }
        }

        return rows;
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    private static string RefCount(string name) =>
        $"SELECT execution_count::bigint FROM {ObjectReferencesDaily.Table} WHERE server_id = {ServerId} AND referenced_name = '{name.ToLowerInvariant()}'";

    private static async Task PlantStatementAsync(
        NpgsqlConnection connection, DateOnly day, string database, long queryId, long executions, string? text, DateTime lastExecution, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_top_daily (server_id, day, database_name, query_id, plan_id, module_name, interval_rows, execution_count_sum, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n, last_execution_time_max)
VALUES ($1,$2,$3,$4,1,'Adhoc',1,$5,1,1,1,1,1,1,$6)",
            ServerId, day, database, queryId, (decimal)executions, DarlingMcpTestData.Naive(lastExecution));
        if (text is not null)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1,$2,$3,$4,$5)",
                ServerId, database, queryId, text, DarlingMcpTestData.Naive(lastExecution));
        }
    }

    private static Task PlantBuiltAsync(NpgsqlConnection connection, DateOnly day, DateTime builtAt, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES ($1,$2,2,$3,1)
ON CONFLICT (server_id, day) DO UPDATE SET built_at = EXCLUDED.built_at",
            ServerId, day, DarlingMcpTestData.Naive(builtAt));

    private static Task PlantInventoryAsync(NpgsqlConnection connection, string database, string name, string type, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.object_inventory (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_id, type_desc, create_date, modify_date) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,$8,NULL,NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Server, database, name, name.GetHashCode(StringComparison.Ordinal), type);

    private static Task PlantDependencyAsync(
        NpgsqlConnection connection, string referencing, string type, string? referencedDatabase, string referencedSchema, string referencedName, DateTime at,
        CancellationToken ct, string database = Db) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.object_dependencies (collection_id, collection_time, server_id, server_name, database_name, referencing_schema, referencing_name, referencing_type, referenced_server, referenced_database, referenced_schema, referenced_name, is_bound, is_caller_dependent, is_ambiguous) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,NULL,$8,$9,$10,true,false,false)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Server, database, referencing, type, (object?)referencedDatabase ?? DBNull.Value,
            referencedSchema, referencedName);

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync(ct))!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }
}
