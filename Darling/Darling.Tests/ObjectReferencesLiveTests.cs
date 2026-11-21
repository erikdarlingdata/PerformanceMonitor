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
/// #8784 part 2: view usage from Query Store statement text. The V288 rung (the dependency list table and the per-day referenced-names
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
    public void TheRungIsV288_AndTheProbeMapsItToV288()
    {
        var rung = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version);
        Assert.Equal(288, rung.Version);
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
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vCte'", ct));
        Assert.Equal(5L, await ScalarAsync<long>(connection, RefCount("vLegacy"), ct));
        Assert.True(await ScalarAsync<bool>(connection, $"SELECT text_matched FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vLegacy'", ct));
        Assert.Equal("Sales", await ScalarAsync<string>(connection,
            $"SELECT referenced_database FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vCrossDb'", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vGone'", ct));

        Assert.Equal(4L, await ScalarAsync<long>(connection,
            $"SELECT statements FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT statements_without_text FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT statements_unparsed FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));

        /* Nothing new: the next tick builds nothing. */
        Assert.Equal(0, (await ObjectReferencesDaily.RunTickAsync(dataSource, now.AddHours(1), NullLogger.Instance, ct)).Built);

        /* The day is rebuilt from a source whose text has since been pruned: the build only adds, so the count stays. */
        await ExecuteAsync(connection, $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId} AND query_id = 1", ct);
        await PlantBuiltAsync(connection, day, now.AddDays(-1), ct);
        Assert.Equal(1, (await ObjectReferencesDaily.RunTickAsync(dataSource, now.AddHours(2), NullLogger.Instance, ct)).Built);
        Assert.Equal(42L, await ScalarAsync<long>(connection, RefCount("vOrders"), ct));
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

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        foreach (var view in new[] { "vOuter", "vInner", "vOld", "vCrossDb" })
        {
            await PlantInventoryAsync(connection, Db, view, "VIEW", now.AddDays(-1), ct);
        }

        /* Statement text on 85 of the 90 days, with vOuter named on one of them and vCrossDb read from another database. */
        for (var d = 2; d <= 86; d++)
        {
            await ExecuteAsync(connection,
                $"INSERT INTO {ObjectReferencesDaily.DatabaseDaysTable} VALUES ({ServerId}, '{Db}', DATE '{now.AddDays(-d):yyyy-MM-dd}', 10, 0, 0, 100)", ct);
        }

        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, '{Db}', '', 'dbo', 'vOuter', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 12, 1, NULL, false)", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Reports', 'SALES', 'dbo', 'vCrossDb', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 4, 1, NULL, false)", ct);
        /* A name another database reads with no database part is that database's object, not this one's. */
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Reports', '', 'dbo', 'vOld', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 4, 1, NULL, false)", ct);

        /* The dependency list: vOuter reads vInner; plus the traps the filter leaves out. */
        await PlantDependencyAsync(connection, "vOuter", "V", null, "dbo", "vInner", now.AddDays(-1), ct);
        await PlantDependencyAsync(connection, "vOuter", "V", null, "sys", "objects", now.AddDays(-1), ct);
        await PlantDependencyAsync(connection, "vOuter", "V", "alias", "col", "value", now.AddDays(-1), ct);

        var deps = await DarlingObjectUsageReader.ReadDependenciesAsync(dataSource, ServerId, Db, ct);
        Assert.NotNull(deps);
        Assert.Equal("vInner", Assert.Single(deps!.Dependencies).ReferencedName);

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

        var views = root.GetProperty("coverage")[0].GetProperty("views");
        Assert.Equal(85, views.GetProperty("days_with_data").GetInt32());
        Assert.Equal(850, views.GetProperty("statements").GetInt64());
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    private static string RefCount(string name) =>
        $"SELECT execution_count::bigint FROM {ObjectReferencesDaily.Table} WHERE server_id = {ServerId} AND referenced_name = '{name}'";

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
        CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.object_dependencies (collection_id, collection_time, server_id, server_name, database_name, referencing_schema, referencing_name, referencing_type, referenced_server, referenced_database, referenced_schema, referenced_name, is_bound, is_caller_dependent, is_ambiguous) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,NULL,$8,$9,$10,true,false,false)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Server, Db, referencing, type, (object?)referencedDatabase ?? DBNull.Value,
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
