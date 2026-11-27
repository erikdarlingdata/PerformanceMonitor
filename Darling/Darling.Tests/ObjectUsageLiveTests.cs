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
/// #8784: object usage over a long window. The V268 rung (the answering instance on index_object_stats, the object list table and the module
/// execution summary) and its probe arm; the summary's copy from the daily Query Store summary (ad hoc and unknown modules left out, a rebuilt
/// summary day copied again, days past the retention edge deleted); and <c>get_object_usage</c> end to end over the store: a reset inside the
/// window counted through, a module that never ran listed from the object list, and the ReadOnlyIntent partner read beside its primary.
/// </summary>
public sealed class ObjectUsageLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8784 object usage tests.";
    private const string Primary = "objusage-sql1";
    private const string Secondary = Primary + ":RO";
    private const string Db = "Sales";
    private static readonly int PrimaryId = ServerIdHelper.GetDeterministicHashCode(Primary);
    private static readonly int SecondaryId = ServerIdHelper.GetDeterministicHashCode(Secondary);

    // ── The rung ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheRungIsV268_AndTheProbeMapsItToV268WhenNoNewerRungIsThere()
    {
        var rung = PgMigrations.Scripts.Single(r => r.Version == 268);
        Assert.Equal("object-usage", rung.Name);
        Assert.Contains("ADD COLUMN IF NOT EXISTS answering_server_name text", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW collect.v_index_object_stats", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("collect.object_inventory", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS is_memory_optimized boolean", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("collect.object_executions_daily_built", rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectExecutionsDaily.DatabaseDaysTable, rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectExecutionsDaily.RetentionTable, rung.Sql, StringComparison.Ordinal);
        Assert.Contains("attname = 'answering_server_name'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var ordinal = Array.FindIndex(parameters, p => p.Name == "hasObjectUsage");
        Assert.True(ordinal >= 0);
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        for (var i = ordinal + 1; i < parameters.Length; i++)
        {
            all[i] = false;
        }

        Assert.Equal(rung.Version, (int)method.Invoke(null, all)!);
        var without = (object[])all.Clone();
        without[ordinal] = false;
        Assert.Equal(rung.Version - 1, (int)method.Invoke(null, without)!);
    }

    [Fact]
    public void TheObjectListTable_IsExactlyTheSchemaGeneratorsOutput()
    {
        /* A fresh store's V1 builds the table from the collector's payload; an upgraded one gets it from V268. Both must agree column for column. */
        var rung = PgMigrations.Scripts.Single(r => r.Version == 268).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var table = PgSchemaGenerator.CreateTable(ObjectInventoryCollector.Instance)
            .Replace("EXISTS object_inventory", "EXISTS collect.object_inventory", StringComparison.Ordinal);
        var index = PgSchemaGenerator.CreateIndex(ObjectInventoryCollector.Instance)!
            .Replace("ON object_inventory", "ON collect.object_inventory", StringComparison.Ordinal);
        Assert.Contains(table, rung, StringComparison.Ordinal);
        Assert.Contains(index, rung, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRungRunsTwice_AndTheColumnIsOnTheView()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await ExecuteAsync(connection, PgMigrations.Scripts.Single(r => r.Version == 268).Sql, ct);
        await ExecuteAsync(connection, PgMigrations.Scripts.Single(r => r.Version == 268).Sql, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'v_index_object_stats' AND column_name = 'answering_server_name'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'v_index_object_stats' AND column_name = 'is_memory_optimized'", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.object_executions_daily", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectExecutionsDaily.DatabaseDaysTable}", ct));
    }

    // ── The module execution summary ────────────────────────────────────────────────────────

    [Fact]
    public async Task TheSummary_CopiesModules_LeavesOutAdhoc_RecopiesARebuiltDay_AndDropsExpiredDays()
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
        await PlantTopDailyAsync(connection, PrimaryId, day, "dbo.usp_Daily", queryId: 1, executions: 40, now.AddDays(-4), ct);
        await PlantTopDailyAsync(connection, PrimaryId, day, "dbo.usp_Daily", queryId: 2, executions: 2, now.AddDays(-4), ct);
        await PlantTopDailyAsync(connection, PrimaryId, day, "Adhoc", queryId: 3, executions: 9_000, now.AddDays(-4), ct);
        await PlantTopDailyAsync(connection, PrimaryId, day, "Unknown", queryId: 4, executions: 5, now.AddDays(-4), ct);
        await PlantBuiltAsync(connection, PrimaryId, day, now.AddDays(-3), ct);
        await ExecuteAsync(connection,
            $"INSERT INTO collect.object_executions_daily VALUES ({PrimaryId}, 'Old', 'dbo.usp_Gone', DATE '{expired:yyyy-MM-dd}', 1, NULL)", ct);

        var first = await ObjectExecutionsDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(1, first.Copied);
        Assert.Equal(42L, await ScalarAsync<long>(connection,
            $"SELECT execution_count::bigint FROM collect.object_executions_daily WHERE server_id = {PrimaryId} AND module_name = 'dbo.usp_Daily'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.object_executions_daily WHERE server_id = {PrimaryId}", ct));

        /* Nothing new: the next tick copies nothing. */
        Assert.Equal(0, (await ObjectExecutionsDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct)).Copied);

        /* The summary day is rebuilt (pass 2) with a higher count: the copy follows it. */
        await ExecuteAsync(connection, $"UPDATE collect.query_store_top_daily SET execution_count_sum = 50 WHERE server_id = {PrimaryId} AND query_id = 1", ct);
        await PlantBuiltAsync(connection, PrimaryId, day, now.AddDays(-2), ct);
        Assert.Equal(1, (await ObjectExecutionsDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct)).Copied);
        Assert.Equal(52L, await ScalarAsync<long>(connection,
            $"SELECT execution_count::bigint FROM collect.object_executions_daily WHERE server_id = {PrimaryId} AND module_name = 'dbo.usp_Daily'", ct));

        /* The database's day is recorded with every statement's executions, the ad hoc ones included: 50 + 2 + 9,000 + 5. */
        Assert.Equal(9_057L, await ScalarAsync<long>(connection,
            $"SELECT execution_count::bigint FROM {ObjectExecutionsDaily.DatabaseDaysTable} WHERE server_id = {PrimaryId} AND database_name = '{Db}' AND day = DATE '{day:yyyy-MM-dd}'", ct));

        /* The summary day is rebuilt from a purged source: the module is gone from it and the build is newer. This table outlives its
           source, so the copy takes nothing away: the module's row, its count and the database's day all stay. */
        await ExecuteAsync(connection, $"DELETE FROM collect.query_store_top_daily WHERE server_id = {PrimaryId}", ct);
        await PlantBuiltAsync(connection, PrimaryId, day, now.AddDays(-1), ct);
        Assert.Equal(1, (await ObjectExecutionsDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct)).Copied);
        Assert.Equal(52L, await ScalarAsync<long>(connection,
            $"SELECT execution_count::bigint FROM collect.object_executions_daily WHERE server_id = {PrimaryId} AND module_name = 'dbo.usp_Daily'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM {ObjectExecutionsDaily.DatabaseDaysTable} WHERE server_id = {PrimaryId} AND database_name = '{Db}'", ct));

        /* A partly purged rebuild with a lower count keeps the larger one already copied. */
        await PlantTopDailyAsync(connection, PrimaryId, day, "dbo.usp_Daily", queryId: 1, executions: 7, now.AddDays(-4), ct);
        await PlantBuiltAsync(connection, PrimaryId, day, now.AddHours(-12), ct);
        Assert.Equal(1, (await ObjectExecutionsDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct)).Copied);
        Assert.Equal(52L, await ScalarAsync<long>(connection,
            $"SELECT execution_count::bigint FROM collect.object_executions_daily WHERE server_id = {PrimaryId} AND module_name = 'dbo.usp_Daily'", ct));

        /* The expired day went with the retention delete. */
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.object_executions_daily WHERE database_name = 'Old'", ct));
    }

    [Fact]
    public async Task ARetentionLoweredBriefly_DeletesNothing_AndOneThatStands_AppliesAfterTheGracePeriod()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = new DateTime(2026, 11, 14, 12, 0, 0);
        const string Kept = "SELECT count(*) FROM collect.object_executions_daily WHERE module_name = 'dbo.usp_YearEnd'";
        const string Waiting = "SELECT COALESCE(pending_days, -1) FROM collect.object_executions_daily_retention";

        /* A year of evidence is kept: one module's row is 200 days old, far beyond what the Query Store summary could build again. */
        await ExecuteAsync(connection,
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES (NULL, 'index_object_stats', 400)", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO collect.object_executions_daily VALUES ({PrimaryId}, '{Db}', 'dbo.usp_YearEnd', DATE '{now.AddDays(-200):yyyy-MM-dd}', 1, NULL)", ct);
        await ObjectExecutionsDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, Kept, ct));

        /* The retention is lowered to reclaim space. Ticks go by for days and the row stays: the lower value is only waiting. */
        await ExecuteAsync(connection, "UPDATE config.config_collector_schedules SET retention_days = 30 WHERE collector_name = 'index_object_stats'", ct);
        await ObjectExecutionsDaily.RunTickAsync(dataSource, now.AddHours(1), NullLogger.Instance, ct);
        await ObjectExecutionsDaily.RunTickAsync(dataSource, now.AddDays(ObjectExecutionsDaily.RetentionLoweringGraceDays - 1), NullLogger.Instance, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, Kept, ct));
        Assert.Equal(30L, await ScalarAsync<long>(connection, Waiting, ct));

        /* Put back inside the grace period: nothing was lost and nothing is waiting any more. */
        await ExecuteAsync(connection, "UPDATE config.config_collector_schedules SET retention_days = 400 WHERE collector_name = 'index_object_stats'", ct);
        await ObjectExecutionsDaily.RunTickAsync(dataSource, now.AddDays(8), NullLogger.Instance, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, Kept, ct));
        Assert.Equal(-1L, await ScalarAsync<long>(connection, Waiting, ct));

        /* Lowered again and left: once it has stood for the grace period the cleanup deletes to it. */
        await ExecuteAsync(connection, "UPDATE config.config_collector_schedules SET retention_days = 30 WHERE collector_name = 'index_object_stats'", ct);
        await ObjectExecutionsDaily.RunTickAsync(dataSource, now.AddDays(9), NullLogger.Instance, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, Kept, ct));
        await ObjectExecutionsDaily.RunTickAsync(dataSource, now.AddDays(9 + ObjectExecutionsDaily.RetentionLoweringGraceDays), NullLogger.Instance, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, Kept, ct));
        Assert.Equal(30L, await ScalarAsync<long>(connection, "SELECT applied_days FROM collect.object_executions_daily_retention", ct));
    }

    // ── get_object_usage end to end ─────────────────────────────────────────────────────────

    [Fact]
    public async Task GetObjectUsage_CountsThroughAReset_ListsANeverRunModule_AndReadsTheReadOnlyPartner()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await DarlingMcpTestData.RegisterServerAsync(connection, PrimaryId, Primary, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, SecondaryId, Secondary, ct);

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var startA = now.AddDays(-200);
        var startB = now.AddDays(-5);

        /* Primary: Orders read 100 before a restart and 30 after it; ReportOnly never read. */
        await PlantIndexAsync(connection, PrimaryId, Primary, now.AddDays(-61), startA, "NODE1", 100, "Orders", 1_000, ct);
        await PlantIndexAsync(connection, PrimaryId, Primary, now.AddDays(-61), startA, "NODE1", 200, "ReportOnly", 0, ct);
        await PlantIndexAsync(connection, PrimaryId, Primary, now.AddDays(-30), startA, "NODE1", 100, "Orders", 1_100, ct);
        await PlantIndexAsync(connection, PrimaryId, Primary, now.AddDays(-30), startA, "NODE1", 200, "ReportOnly", 0, ct);
        await PlantIndexAsync(connection, PrimaryId, Primary, now.AddDays(-1), startB, "NODE1", 100, "Orders", 30, ct);
        await PlantIndexAsync(connection, PrimaryId, Primary, now.AddDays(-1), startB, "NODE1", 200, "ReportOnly", 0, ct);

        /* The readable secondary: ReportOnly is read there, and only there. */
        await PlantIndexAsync(connection, SecondaryId, Secondary, now.AddDays(-30), startA, "NODE2", 200, "ReportOnly", 10, ct);
        await PlantIndexAsync(connection, SecondaryId, Secondary, now.AddDays(-1), startA, "NODE2", 200, "ReportOnly", 50, ct);

        /* The object list names two procedures; only one ran. */
        foreach (var (name, type) in new[] { ("usp_Daily", "SQL_STORED_PROCEDURE"), ("usp_YearEnd", "SQL_STORED_PROCEDURE") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.object_inventory (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_id, type_desc, create_date, modify_date) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,$8,NULL,NULL)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddDays(-1)), PrimaryId, Primary, Db, name, name.GetHashCode(StringComparison.Ordinal), type);
        }

        await ExecuteAsync(connection,
            $"INSERT INTO collect.object_executions_daily VALUES ({PrimaryId}, '{Db}', 'dbo.usp_Daily', DATE '{now.AddDays(-3):yyyy-MM-dd}', 90, NULL)", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO collect.object_executions_daily_built VALUES ({PrimaryId}, DATE '{now.AddDays(-3):yyyy-MM-dd}', now(), now())", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectExecutionsDaily.DatabaseDaysTable} VALUES ({PrimaryId}, '{Db}', DATE '{now.AddDays(-3):yyyy-MM-dd}', 500)", ct);

        /* Another database on the same server has Query Store data on many days; the read-only registration reads the same Query Store, so
           it holds the same module executions. */
        for (var d = 2; d <= 40; d++)
        {
            await ExecuteAsync(connection,
                $"INSERT INTO {ObjectExecutionsDaily.DatabaseDaysTable} VALUES ({PrimaryId}, 'Other', DATE '{now.AddDays(-d):yyyy-MM-dd}', 10)", ct);
        }

        await ExecuteAsync(connection,
            $"INSERT INTO collect.object_executions_daily VALUES ({SecondaryId}, '{Db}', 'dbo.usp_Daily', DATE '{now.AddDays(-3):yyyy-MM-dd}', 90, NULL)", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectExecutionsDaily.DatabaseDaysTable} VALUES ({SecondaryId}, '{Db}', DATE '{now.AddDays(-3):yyyy-MM-dd}', 500)", ct);

        /* Only a stored ALL lets a module that did not run be a candidate (#8784 review): with no health snapshot it is Not tracked. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, query_capture_mode) VALUES ($1,$2,$3,$4,$5,$6)",
            1L, DarlingMcpTestData.Naive(now.AddDays(-5)), PrimaryId, Primary, Db, "ALL");

        var json = await DarlingMcpObjectUsageTools.GetObjectUsage(dataSource, Primary, Db, 60, true, 100, 0, now, ct);
        var root = JsonDocument.Parse(json).RootElement;

        JsonElement Obj(string name) => root.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == name);

        Assert.Equal(130, Obj("Orders").GetProperty("reads").GetInt64());
        Assert.Equal("Read", Obj("ReportOnly").GetProperty("classification").GetString());
        Assert.Equal(40, Obj("ReportOnly").GetProperty("reads").GetInt64());
        Assert.Equal(2, root.GetProperty("paired_servers").GetArrayLength());
        Assert.Equal(90, Obj("usp_Daily").GetProperty("executions").GetInt64());
        Assert.Equal("Not executed in window", Obj("usp_YearEnd").GetProperty("classification").GetString());
        Assert.Equal(1, root.GetProperty("coverage")[0].GetProperty("tables").GetProperty("resets_crossed").GetInt32());

        /* The restart was found 29 days after the snapshot before it: use in that time went with the counters, and the reply says so. */
        Assert.Equal(29 * 24, root.GetProperty("coverage")[0].GetProperty("tables").GetProperty("blind_hours").GetDouble());

        /* One day of Query Store data in a 60-day window: the module that did not run says how little was looked at. */
        Assert.Contains("covers only 1 of the 60 days", Obj("usp_YearEnd").GetProperty("caveat").GetString(), StringComparison.Ordinal);

        /* Coverage is this database's own days, not the server's: one day, however many the other database has. */
        var executionCoverage = root.GetProperty("coverage")[0].GetProperty("executions");
        Assert.Equal(1, executionCoverage.GetProperty("days_with_data").GetInt32());
        Assert.True(executionCoverage.GetProperty("short_of_window").GetBoolean());

        /* A database the summary never held anything for shows no covered days at all. */
        await ExecuteAsync(connection,
            $"INSERT INTO collect.object_inventory (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_id, type_desc) VALUES (1, now(), {PrimaryId}, '{Primary}', 'NoQueryStore', 'dbo', 'usp_Hidden', 1, 'SQL_STORED_PROCEDURE')", ct);
        var dark = JsonDocument.Parse(await DarlingMcpObjectUsageTools.GetObjectUsage(dataSource, Primary, "NoQueryStore", 60, false, 100, 0, now, ct)).RootElement;
        Assert.Equal(0, dark.GetProperty("coverage")[0].GetProperty("executions").GetProperty("days_with_data").GetInt32());
        Assert.True(dark.GetProperty("coverage")[0].GetProperty("executions").GetProperty("short_of_window").GetBoolean());

        /* Nothing was looked at there, so its procedure is not called unexecuted, and is not a candidate. */
        var hidden = dark.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == "usp_Hidden");
        Assert.Equal(ObjectUsageReport.NotTracked, hidden.GetProperty("classification").GetString());
        Assert.Equal(0, dark.GetProperty("summary").GetProperty("not_executed").GetInt32());

        /* Without the partner, ReportOnly looks unused: the reason the pairing exists. */
        var alone = JsonDocument.Parse(await DarlingMcpObjectUsageTools.GetObjectUsage(dataSource, Primary, Db, 60, false, 100, 0, now, ct)).RootElement;
        var reportOnlyAlone = alone.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == "ReportOnly");
        Assert.Equal("Not used in window", reportOnlyAlone.GetProperty("classification").GetString());
    }

    [Fact]
    public async Task TheExecutionRead_CarriesTheCaptureMode_ANonAllModeInTheWindowWins_AndOneBeforeTheWindowDoesNot()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, PrimaryId, Primary, ct);

        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var windowStart = now.AddDays(-60);

        Task PlantHealthAsync(string database, DateTime at, string? mode) =>
            DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, query_capture_mode) VALUES ($1,$2,$3,$4,$5,$6)",
                1L, DarlingMcpTestData.Naive(at), PrimaryId, Primary, database, (object?)mode ?? DBNull.Value);

        /* Sales: AUTO for a while inside the window, ALL since. The AUTO stretch is enough for rare modules to have gone unrecorded. */
        await PlantHealthAsync(Db, now.AddDays(-40), "AUTO");
        await PlantHealthAsync(Db, now.AddDays(-2), "ALL");
        await PlantHealthAsync(Db, now.AddDays(-1), null);

        /* Steady: CUSTOM long before the window, ALL inside it. */
        await PlantHealthAsync("Steady", now.AddDays(-200), "CUSTOM");
        await PlantHealthAsync("Steady", now.AddDays(-10), "ALL");

        Assert.Equal("AUTO", (await DarlingObjectUsageReader.ReadExecutionsAsync(dataSource, PrimaryId, Db, windowStart, ct)).QueryCaptureMode);
        Assert.Equal("ALL", (await DarlingObjectUsageReader.ReadExecutionsAsync(dataSource, PrimaryId, "Steady", windowStart, ct)).QueryCaptureMode);
        Assert.Null((await DarlingObjectUsageReader.ReadExecutionsAsync(dataSource, PrimaryId, "NoHealth", windowStart, ct)).QueryCaptureMode);
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    private static Task PlantIndexAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, DateTime start, string node, int objectId, string table, long seeks,
        CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, is_indexed_view, answering_server_name, is_memory_optimized)
VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,1,'CLUSTERED',$9,0,0,0,false,$10,false)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, DarlingMcpTestData.Naive(start), Db, objectId, table, seeks, node);

    private static Task PlantTopDailyAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, string module, long queryId, long executions, DateTime lastExecution, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_top_daily (server_id, day, database_name, query_id, plan_id, module_name, interval_rows, execution_count_sum, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n, last_execution_time_max)
VALUES ($1,$2,$3,$4,1,$5,1,$6,1,1,1,1,1,1,$7)",
            serverId, day, Db, queryId, module, (decimal)executions, DarlingMcpTestData.Naive(lastExecution));

    private static Task PlantBuiltAsync(NpgsqlConnection connection, int serverId, DateOnly day, DateTime builtAt, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES ($1,$2,2,$3,1)
ON CONFLICT (server_id, day) DO UPDATE SET built_at = EXCLUDED.built_at",
            serverId, day, DarlingMcpTestData.Naive(builtAt));

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
