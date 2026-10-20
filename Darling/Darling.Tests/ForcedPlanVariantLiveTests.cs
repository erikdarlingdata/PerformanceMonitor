/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch store and its own SQL Server database, so the class is deliberately NOT
   [Collection("live-postgres")]. It shares "remediation-live-sql" with the other classes that force plans on the one test SQL Server. */

/// <summary>
/// #6160 against a REAL SQL Server and a real store. A procedure's query is compiled with a rare key and then a common one, the first plan is
/// forced, and the procedure runs again: SQL Server compiles the force under a NEW plan id whose stored XML carries <c>UsePlan="1"</c> (a variant).
/// The collector's real per-database statement reads that, its rows go through the real binary COPY writer, and the tool answers from the store.
/// The second case adds an OBJECT-scoped <c>USE PLAN</c> guide on the procedure. Needs <c>DARLING_TEST_PG</c> and <c>DARLING_TEST_SQL</c> (with user and
/// password); skips without them. Query Store's own stats are copied from the engine's runtime stats, with chosen times.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class ForcedPlanVariantLiveTests
{
    private const string ServerName = "darling-forced-variant";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static SqlConnectionStringBuilder Admin(string database) => new()
    {
        DataSource = Environment.GetEnvironmentVariable("DARLING_TEST_SQL"),
        InitialCatalog = database,
        UserID = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
        Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
        TrustServerCertificate = true,
        Encrypt = SqlConnectionEncryptOption.Optional,
        Pooling = false,
    };

    private static async Task SqlAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> SqlScalarAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }

    private sealed record Setup(string Database, long QueryId, long ForcedPlan, long ScanPlan);

    /// <summary>The skewed table and procedure, the rare-key plan forced, then the procedure run again so the engine compiles the variant.</summary>
    private static async Task<Setup> BuildForcedProcedureAsync(string database, CancellationToken ct)
    {
        await SqlAsync("master", $"CREATE DATABASE [{database}]; ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL, DATA_FLUSH_INTERVAL_SECONDS = 60);", ct);
        await SqlAsync(database, @"
CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, k int NOT NULL, pad char(200) NOT NULL DEFAULT 'x');
INSERT dbo.t (k) SELECT TOP (50000) 1 FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;
INSERT dbo.t (k) SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) + 1 FROM sys.all_columns;
CREATE INDEX ix_k ON dbo.t (k);", ct);
        await SqlAsync(database, "EXEC('CREATE PROCEDURE dbo.p @k int AS SELECT COUNT_BIG(*), MAX(pad) FROM dbo.t WHERE k = @k;');", ct);
        await SqlAsync(database, "EXEC dbo.p @k = 5;", ct);
        await SqlAsync(database, "EXEC sp_recompile 'dbo.p'; EXEC dbo.p @k = 1; EXEC sys.sp_query_store_flush_db;", ct);

        var query = Convert.ToInt64(await SqlScalarAsync(database, "SELECT TOP (1) q.query_id FROM sys.query_store_query AS q WHERE q.object_id = OBJECT_ID(N'dbo.p');", ct), CultureInfo.InvariantCulture);
        var plans = new List<long>();
        await using (var connection = new SqlConnection(Admin(database).ConnectionString))
        {
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand($"SELECT plan_id FROM sys.query_store_plan WHERE query_id = {query} ORDER BY plan_id;", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct)) plans.Add(reader.GetInt64(0));
        }

        Assert.True(plans.Count == 2, $"the procedure should have two plans (rare key, common key); it has {plans.Count}");
        await SqlAsync(database, $"EXEC sys.sp_query_store_force_plan {query}, {plans[0]};", ct);
        await SqlAsync(database, "EXEC sp_recompile 'dbo.p'; EXEC dbo.p @k = 1; EXEC dbo.p @k = 1; EXEC sys.sp_query_store_flush_db;", ct);
        return new Setup(database, query, plans[0], plans[1]);
    }

    /// <summary>The collector's real per-database statement and reader against the scratch database.</summary>
    private static async Task<List<QueryStoreForcedPlansCollector.Row>> CollectAsync(string database, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = ServerId, ServerName = ServerName,
            Target = new CollectorTargetInfo { SqlMajorVersion = 17 }, Deltas = new CollectorDeltaCalculator(), EnumerationProbeResult = 17,
        };
        var query = QueryStoreForcedPlansCollector.Instance.BuildPerItemQuery(database, context);
        await using var connection = new SqlConnection(Admin("master").ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 120);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<QueryStoreForcedPlansCollector.Row>();
        await QueryStoreForcedPlansCollector.Instance.ReadItemAsync(database, reader, rows, context, ct);
        return rows;
    }

    private static async Task WriteCaptureAsync(NpgsqlConnection connection, List<QueryStoreForcedPlansCollector.Row> rows, DateTime at, CancellationToken ct)
    {
        var definition = QueryStoreForcedPlansCollector.Instance;
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = at, Deltas = null! };
        var writer = new PgCollectorRowWriter();
        await using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(at).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }

        await importer.CompleteAsync(ct);
    }

    /// <summary>One Query Store interval of one plan in the store, with the engine's own execution count and a chosen last-run time.</summary>
    private static Task StatsAsync(NpgsqlConnection c, string db, long query, long plan, DateTime lastRun, long executions, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"INSERT INTO query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
 interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_rowcount, is_forced_plan, force_failure_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,'Regular',$8,$9,$9,$10,$11,5000,7000,1,FALSE,0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(lastRun.AddMinutes(1)), ServerId, ServerName, db, query, plan, plan * 1000 + lastRun.Minute,
            DarlingMcpTestData.Naive(lastRun.AddMinutes(-30)), DarlingMcpTestData.Naive(lastRun), executions);

    private static async Task<long> EngineExecutionsAsync(string database, long plan, CancellationToken ct) =>
        Convert.ToInt64(await SqlScalarAsync(database, $"SELECT COALESCE(SUM(count_executions), 0) FROM sys.query_store_runtime_stats WHERE plan_id = {plan};", ct), CultureInfo.InvariantCulture);

    private sealed record Rig(NpgsqlConnection Connection, NpgsqlDataSource Postgres, Setup Setup, DateTime Base, List<QueryStoreForcedPlansCollector.Row> Rows);

    /// <summary>Builds the forced procedure, collects it, stores the capture and the engine's post-force runs of every plan other than the forced one.</summary>
    private static async Task RunAsync(Func<string, Task>? afterForce, Func<Rig, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StoreConnectionString), "Set DARLING_TEST_PG to run the forced-plan variant live tests.");
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL")), "Set DARLING_TEST_SQL (with DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD) to run the forced-plan variant live tests.");
        var ct = TestContext.Current.CancellationToken;
        var database = "pm_6160_" + Guid.NewGuid().ToString("N")[..12];
        await using var scratch = await ScratchPostgres.CreateAsync(StoreConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            var setup = await BuildForcedProcedureAsync(database, ct);
            if (afterForce is not null) await afterForce(database);
            await SqlAsync(database, "EXEC sp_recompile 'dbo.p'; EXEC dbo.p @k = 1; EXEC sys.sp_query_store_flush_db;", ct);

            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var rows = await CollectAsync(database, ct);
            var baseTime = LiveClock.AnchoredEnd().AddHours(-2);
            await WriteCaptureAsync(connection, rows, baseTime, ct);

            /* Every plan of the query that ran after the force was first seen (base + 30 minutes), with the engine's own counts. */
            await using (var plansConnection = new SqlConnection(Admin(database).ConnectionString))
            {
                await plansConnection.OpenAsync(ct);
                await using var command = new SqlCommand($"SELECT plan_id FROM sys.query_store_plan WHERE query_id = {setup.QueryId} AND plan_id > {setup.ForcedPlan};", plansConnection);
                var ids = new List<long>();
                await using (var reader = await command.ExecuteReaderAsync(ct))
                    while (await reader.ReadAsync(ct)) ids.Add(reader.GetInt64(0));
                foreach (var id in ids.Where(i => i != setup.ScanPlan))
                {
                    await StatsAsync(connection, database, setup.QueryId, id, baseTime.AddMinutes(30), await EngineExecutionsAsync(database, id, ct), ct);

                    /* #6221: the collector reads no plan XML, so the XML the store judges a later plan by is the one the Query Store collector stores: here the engine's own. */
                    var planXml = Convert.ToString(await SqlScalarAsync(database, $"SELECT CONVERT(nvarchar(max), query_plan) FROM sys.query_store_plan WHERE plan_id = {id};", ct), CultureInfo.InvariantCulture);
                    await StoredPlanSeed.AddAsync(connection, ct, ServerId, database, id, planXml);
                }
            }

            await body(new Rig(connection, postgres, setup, baseTime, rows), ct);
            bodySucceeded = true;
        }
        finally
        {
            try
            {
                await SqlAsync("master", $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;", CancellationToken.None);
            }
            catch (SqlException)
            {
                /* The assertions report a real failure; a cleanup that cannot reach the server leaves a uniquely named scratch database. */
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<JsonElement> ForceAsync(Rig rig, CancellationToken ct)
    {
        var asOf = LiveClock.AnchoredEnd().ToString("o", CultureInfo.InvariantCulture);
        var json = await DarlingMcpForcedPlanTools.GetForcedPlans(rig.Postgres, ServerName, 4, asOf, DatabaseFilter.One(rig.Setup.Database), ct);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("forced_plans").EnumerateArray()
            .Single(e => e.GetProperty("plan_id").GetInt64() == rig.Setup.ForcedPlan).Clone();
    }

    [Fact]
    public async Task ARecompiledForce_ReadsWorking_AndListsItsVariantPlan_NotIgnored()
    {
        await RunAsync(null, async (rig, ct) =>
        {
            /* #6221: the collector reads no plan XML, so it names no variant; the store finds them in the stored plans. */
            Assert.Null(rig.Rows.Single(r => r.PlanId == rig.Setup.ForcedPlan).VariantPlanIds);

            var force = await ForceAsync(rig, ct);
            Assert.Equal("WORKING", force.GetProperty("state").GetString());
            var variants = force.GetProperty("variant_plans").EnumerateArray().Select(v => v.GetProperty("plan_id").GetInt64()).ToList();
            Assert.NotEmpty(variants);
            Assert.DoesNotContain(rig.Setup.ScanPlan, variants);
            Assert.All(variants, v => Assert.True(v > rig.Setup.ForcedPlan, "a variant is a LATER plan id of the query"));
            Assert.True(force.GetProperty("variant_plans")[0].GetProperty("execution_count").GetInt64() >= 1);
            Assert.Equal(0, force.GetProperty("other_plans_total").GetInt32());
            Assert.Equal(JsonValueKind.Null, force.GetProperty("plan_guide").ValueKind);
        });
    }

    [Fact]
    public async Task AnObjectScopedUsePlanGuide_OnTheForcedProcedure_IsNamed_BesideTheState()
    {
        await RunAsync(
            async database =>
            {
                var xml = Convert.ToString(await SqlScalarAsync(database,
                    "SELECT TOP (1) CONVERT(nvarchar(max), p.query_plan) FROM sys.query_store_plan AS p JOIN sys.query_store_query AS q ON q.query_id = p.query_id "
                    + "WHERE q.object_id = OBJECT_ID(N'dbo.p') ORDER BY p.plan_id DESC;", TestContext.Current.CancellationToken), CultureInfo.InvariantCulture)!;
                await SqlAsync(database,
                    "EXEC sp_create_plan_guide @name = N'pm_6160_guide', @stmt = N'SELECT COUNT_BIG(*), MAX(pad) FROM dbo.t WHERE k = @k', @type = N'OBJECT', "
                    + "@module_or_batch = N'dbo.p', @params = NULL, @hints = N'OPTION (USE PLAN N''" + xml.Replace("'", "''", StringComparison.Ordinal) + "'')';",
                    TestContext.Current.CancellationToken);
            },
            async (rig, ct) =>
            {
                var forced = rig.Rows.Single(r => r.PlanId == rig.Setup.ForcedPlan);
                Assert.Equal("pm_6160_guide", forced.PlanGuideName);
                Assert.True(forced.PlanGuideEnabled);
                Assert.Equal("OBJECT", forced.PlanGuideScope);

                var force = await ForceAsync(rig, ct);
                var guide = force.GetProperty("plan_guide");
                Assert.Equal("pm_6160_guide", guide.GetProperty("name").GetString());
                Assert.True(guide.GetProperty("enabled").GetBoolean());
                Assert.Equal("OBJECT", guide.GetProperty("scope").GetString());
                Assert.Equal("WORKING", force.GetProperty("state").GetString());
            });
    }
}
