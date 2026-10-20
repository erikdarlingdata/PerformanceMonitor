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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: no PostgreSQL store here, only a scratch SQL Server database per fact. It shares "remediation-live-sql" with the other
   classes that force plans on the one test SQL Server. */

/// <summary>
/// #6221 against a REAL SQL Server. The forced-plans collector used to read the stored XML of every later plan of every forced query on every
/// pass (95 percent or more of a pass at the field's size) to find variants and plan guides. It reads no plan XML now: the check is in the store.
/// These runs build forced procedures that the engine recompiles under new plan ids (so the forced queries have later plans with XML), run the
/// collector's real per-database statement, and count the plan XML the SERVER reads (the <c>lob logical reads</c> of <c>plan_persist_plan</c> in
/// <c>SET STATISTICS IO</c>) rather than anything the collector says about itself. A control statement that does read the XML proves the
/// measurement can see it. Needs <c>DARLING_TEST_SQL</c> with user and password; skips without.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class ForcedPlanCollectorNoXmlLiveTests
{
    private const string ServerName = "darling-forced-xml-once";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly Regex s_planLobReads = new(@"Table 'plan_persist_plan'\..*?\blob logical reads (\d+)", RegexOptions.Compiled | RegexOptions.CultureInvariant);

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

    private static async Task<object?> ScalarTextAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task<long> ScalarAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private const int ProcedureCount = 3;

    /// <summary>
    /// A skewed table and three procedures. Each procedure's first plan is forced, then each runs several recompile rounds, so the engine compiles
    /// the force under new plan ids (variants): every forced query ends with later plans whose stored XML carries <c>UsePlan="1"</c>.
    /// </summary>
    private static async Task BuildForcedProceduresAsync(string database, CancellationToken ct)
    {
        await SqlAsync("master", $"CREATE DATABASE [{database}]; ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL, DATA_FLUSH_INTERVAL_SECONDS = 60);", ct);
        await SqlAsync(database, @"
CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, k int NOT NULL, pad char(200) NOT NULL DEFAULT 'x');
INSERT dbo.t (k) SELECT TOP (50000) 1 FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;
INSERT dbo.t (k) SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) + 1 FROM sys.all_columns;
CREATE INDEX ix_k ON dbo.t (k);", ct);

        for (var i = 1; i <= ProcedureCount; i++)
        {
            await SqlAsync(database, $"EXEC('CREATE PROCEDURE dbo.p{i} @k int AS SELECT COUNT_BIG(*), MAX(pad), {i} AS n FROM dbo.t WHERE k = @k;');", ct);
            await SqlAsync(database, $"EXEC dbo.p{i} @k = 5; EXEC sp_recompile 'dbo.p{i}'; EXEC dbo.p{i} @k = 1;", ct);
        }

        await SqlAsync(database, "EXEC sys.sp_query_store_flush_db;", ct);
        await SqlAsync(database, @"
DECLARE @q bigint, @p bigint;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT q.query_id, MIN(p.plan_id) FROM sys.query_store_query AS q JOIN sys.query_store_plan AS p ON p.query_id = q.query_id
    WHERE q.object_id IN (SELECT object_id FROM sys.procedures WHERE name LIKE 'p[0-9]%') GROUP BY q.query_id;
OPEN c; FETCH NEXT FROM c INTO @q, @p;
WHILE @@FETCH_STATUS = 0 BEGIN EXEC sys.sp_query_store_force_plan @q, @p; FETCH NEXT FROM c INTO @q, @p; END;
CLOSE c; DEALLOCATE c;", ct);

        for (var round = 0; round < 3; round++)
        {
            for (var i = 1; i <= ProcedureCount; i++)
            {
                await SqlAsync(database, $"EXEC sp_recompile 'dbo.p{i}'; EXEC dbo.p{i} @k = 1;", ct);
            }
        }

        await SqlAsync(database, "EXEC sys.sp_query_store_flush_db;", ct);
    }

    private static Task<long> LaterPlanCountAsync(string database, CancellationToken ct) =>
        ScalarAsync(database, "SELECT COUNT(*) FROM sys.query_store_plan AS l JOIN sys.query_store_plan AS f ON f.query_id = l.query_id AND f.is_forced_plan = 1 AND l.plan_id > f.plan_id;", ct);

    private sealed record Pass(List<QueryStoreForcedPlansCollector.Row> Rows, long PlanXmlLobReads, long ElapsedMs);

    /// <summary>One pass of the collector's real per-database statement and reader, with the server's own count of plan XML read.</summary>
    private static async Task<Pass> PassAsync(QueryStoreForcedPlansCollector collector, string database, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = ServerId, ServerName = ServerName,
            Target = new CollectorTargetInfo { SqlMajorVersion = 17 }, Deltas = new CollectorDeltaCalculator(), EnumerationProbeResult = 17,
        };
        var query = collector.BuildPerItemQuery(database, context);
        await using var connection = new SqlConnection(Admin("master").ConnectionString);
        connection.StatisticsEnabled = true;
        var lobReads = 0L;
        connection.InfoMessage += (_, e) =>
        {
            foreach (SqlError error in e.Errors)
            {
                var match = s_planLobReads.Match(error.Message);
                if (match.Success) lobReads += long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            }
        };
        await connection.OpenAsync(ct);
        await using (var statistics = new SqlCommand("SET STATISTICS IO ON;", connection)) await statistics.ExecuteNonQueryAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 120);
        var rows = new List<QueryStoreForcedPlansCollector.Row>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            await collector.ReadItemAsync(database, reader, rows, context, ct);
        }

        /* The server-side execution time of the pass, from the driver's own statistics (no clock of the test's own). */
        var executionMs = Convert.ToInt64(connection.RetrieveStatistics()["ExecutionTime"], CultureInfo.InvariantCulture);
        return new Pass(rows, lobReads, executionMs);
    }

    private static async Task RunAsync(Func<string, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL")), "Set DARLING_TEST_SQL (with DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD) to run the forced-plan XML live tests.");
        var ct = TestContext.Current.CancellationToken;
        var database = "pm_6221_" + Guid.NewGuid().ToString("N")[..12];
        try
        {
            await BuildForcedProceduresAsync(database, ct);
            await body(database, ct);
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
        }
    }

    [Fact]
    public async Task APass_OverForcedQueriesWithLaterPlans_ReadsNoPlanXml_AndStillReportsEveryForce()
    {
        await RunAsync(async (database, ct) =>
        {
            var collector = QueryStoreForcedPlansCollector.Instance;
            var later = await LaterPlanCountAsync(database, ct);
            Assert.True(later >= ProcedureCount, $"the fixture must give the forced queries later plans (the plans the old pass read); it has {later}");

            var pass = await PassAsync(collector, database, ct);
            var second = await PassAsync(collector, database, ct);
            TestContext.Current.SendDiagnosticMessage($"#6221 collector pass over {later} later plans: {pass.ElapsedMs} ms first, {second.ElapsedMs} ms second, {pass.PlanXmlLobReads} plan LOB reads");

            Assert.True(pass.PlanXmlLobReads == 0, $"the first pass read {pass.PlanXmlLobReads} LOB pages of plan XML in {pass.ElapsedMs} ms over {later} later plans");
            Assert.True(second.PlanXmlLobReads == 0, $"the second pass read {second.PlanXmlLobReads} LOB pages of plan XML in {second.ElapsedMs} ms");

            var forced = pass.Rows.Where(r => r.PlanId is not null).ToList();
            Assert.Equal(ProcedureCount, forced.Count);
            Assert.Single(pass.Rows, r => r.PlanId is null);
            Assert.All(forced, r => Assert.Null(r.VariantPlanIds));
            Assert.All(forced, r => Assert.NotNull(r.QueryPlanHash));
        });
    }

    [Fact]
    public async Task TheMeasurement_SeesPlanXmlWhenAStatementReadsIt()
    {
        await RunAsync(async (database, ct) =>
        {
            /* The control: the shape of #6171's statement (a CHARINDEX over the later plans' XML). If the harness reported 0 here, the zero above would prove nothing. */
            var xmlRead = $"SELECT COUNT(*) FROM sys.query_store_plan AS l JOIN sys.query_store_plan AS f ON f.query_id = l.query_id AND f.is_forced_plan = 1 AND l.plan_id > f.plan_id WHERE CHARINDEX(N'UsePlan=\"1\"', l.query_plan) > 0;";
            var lobReads = 0L;
            await using var connection = new SqlConnection(Admin(database).ConnectionString);
            connection.InfoMessage += (_, e) =>
            {
                foreach (SqlError error in e.Errors)
                {
                    var match = s_planLobReads.Match(error.Message);
                    if (match.Success) lobReads += long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
                }
            };
            await connection.OpenAsync(ct);
            await using (var statistics = new SqlCommand("SET STATISTICS IO ON;", connection)) await statistics.ExecuteNonQueryAsync(ct);
            await using (var command = new SqlCommand(xmlRead, connection) { CommandTimeout = 120 })
                Assert.True(Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0, "the fixture's later plans carry UsePlan=1 (variants)");
            Assert.True(lobReads > 0, "a statement that reads the later plans' XML shows LOB reads of plan_persist_plan");
        });
    }

    [Fact]
    public async Task AnObjectScopedUsePlanGuide_IsStillNamed_WithoutReadingPlanXml()
    {
        await RunAsync(async (database, ct) =>
        {
            /* A USE PLAN guide on dbo.p1, scoped to the procedure: found by scope_object_id from sys.plan_guides, which holds no plan XML of a Query Store plan. */
            var xml = Convert.ToString(await ScalarTextAsync(database,
                "SELECT TOP (1) CONVERT(nvarchar(max), p.query_plan) FROM sys.query_store_plan AS p JOIN sys.query_store_query AS q ON q.query_id = p.query_id "
                + "WHERE q.object_id = OBJECT_ID(N'dbo.p1') ORDER BY p.plan_id DESC;", ct), CultureInfo.InvariantCulture)!;
            await SqlAsync(database,
                "EXEC sp_create_plan_guide @name = N'pm_6221_guide', @stmt = N'SELECT COUNT_BIG(*), MAX(pad), 1 AS n FROM dbo.t WHERE k = @k', @type = N'OBJECT', "
                + "@module_or_batch = N'dbo.p1', @params = NULL, @hints = N'OPTION (USE PLAN N''" + xml.Replace("'", "''", StringComparison.Ordinal) + "'')';", ct);

            var pass = await PassAsync(QueryStoreForcedPlansCollector.Instance, database, ct);

            Assert.Equal(0, pass.PlanXmlLobReads);
            var guided = Assert.Single(pass.Rows, r => r.PlanGuideName == "pm_6221_guide");
            Assert.True(guided.PlanGuideEnabled);
            Assert.Equal("OBJECT", guided.PlanGuideScope);
            Assert.Equal(ProcedureCount - 1, pass.Rows.Count(r => r.PlanId is not null && r.PlanGuideName is null));
        });
    }
}
