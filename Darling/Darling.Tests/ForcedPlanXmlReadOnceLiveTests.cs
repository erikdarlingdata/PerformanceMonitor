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
/// #6221 against a REAL SQL Server. The forced-plans collector's second statement read the stored XML of every later plan of every forced query
/// on every pass (95 percent or more of a pass at the field's size). A plan's XML never changes for its plan id, so each plan is read once per
/// process. These runs build forced procedures that the engine recompiles under new plan ids (variants), run the collector's real per-database
/// statement more than once, and count the plan XML the SERVER reads (the <c>lob logical reads</c> of <c>plan_persist_plan</c> in
/// <c>SET STATISTICS IO</c>) rather than anything the collector says about itself. Needs <c>DARLING_TEST_SQL</c> with user and password; skips without.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class ForcedPlanXmlReadOnceLiveTests
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

    private sealed record Pass(List<QueryStoreForcedPlansCollector.Row> Rows, long PlanXmlLobReads);

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

        return new Pass(rows, lobReads);
    }

    private static string?[] VariantsOf(Pass pass) =>
        pass.Rows.Where(r => r.PlanId is not null).OrderBy(r => r.PlanId).Select(r => r.VariantPlanIds).ToArray();

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
    public async Task ASecondPassOverUnchangedPlans_ReadsNoPlanXml_AndStillReportsTheVariants()
    {
        await RunAsync(async (database, ct) =>
        {
            var collector = QueryStoreForcedPlansCollector.Instance;
            var first = await PassAsync(collector, database, ct);
            var second = await PassAsync(collector, database, ct);

            Assert.True(first.PlanXmlLobReads > 0, "the first pass reads the later plans' XML (otherwise this test measures nothing)");
            Assert.Equal(0, second.PlanXmlLobReads);

            var firstVariants = VariantsOf(first);
            Assert.Equal(ProcedureCount, firstVariants.Length);
            Assert.All(firstVariants, v => Assert.False(string.IsNullOrEmpty(v), "the engine compiled each force under new plan ids (#6160)"));
            Assert.Equal(firstVariants, VariantsOf(second));
        });
    }

    [Fact]
    public async Task PlansThatCompileAfterTheFirstPass_AreReadOnTheNextPass_AndOnlyThey()
    {
        await RunAsync(async (database, ct) =>
        {
            var collector = QueryStoreForcedPlansCollector.Instance;
            var first = await PassAsync(collector, database, ct);
            var laterBefore = await LaterPlanCountAsync(database, ct);

            /* A new forced procedure whose force the engine recompiles under new plan ids, after the collector's first pass. */
            await SqlAsync(database, "EXEC('CREATE PROCEDURE dbo.p9 @k int AS SELECT COUNT_BIG(*), MAX(pad), 9 AS n FROM dbo.t WHERE k = @k;');", ct);
            await SqlAsync(database, "EXEC dbo.p9 @k = 5; EXEC sp_recompile 'dbo.p9'; EXEC dbo.p9 @k = 1; EXEC sys.sp_query_store_flush_db;", ct);
            await SqlAsync(database, @"
DECLARE @q bigint, @p bigint;
SELECT @q = q.query_id, @p = MIN(p.plan_id) FROM sys.query_store_query AS q JOIN sys.query_store_plan AS p ON p.query_id = q.query_id
WHERE q.object_id = OBJECT_ID(N'dbo.p9') GROUP BY q.query_id;
EXEC sys.sp_query_store_force_plan @q, @p;", ct);
            for (var round = 0; round < 3; round++)
            {
                await SqlAsync(database, "EXEC sp_recompile 'dbo.p9'; EXEC dbo.p9 @k = 1;", ct);
            }

            await SqlAsync(database, "EXEC sys.sp_query_store_flush_db;", ct);
            var laterAfter = await LaterPlanCountAsync(database, ct);
            var third = await PassAsync(collector, database, ct);
            var fourth = await PassAsync(collector, database, ct);

            Assert.True(laterAfter > laterBefore, "the new forced procedure gave the forced queries more later plans");
            Assert.True(third.PlanXmlLobReads > 0, "a plan nobody has read is read");
            Assert.True(third.PlanXmlLobReads < first.PlanXmlLobReads, "only the new plans are read, not every later plan again");
            Assert.Equal(0, fourth.PlanXmlLobReads);
            Assert.Equal(VariantsOf(first).Length + 1, VariantsOf(third).Length);
            Assert.False(string.IsNullOrEmpty(VariantsOf(third)[^1]), "the new force's variant is found on the pass that first read it");
            Assert.Equal(VariantsOf(third), VariantsOf(fourth));
        });
    }

    [Fact]
    public async Task MoreUnreadPlansThanTheCap_AreSpreadOverPasses_AndNoneIsLost()
    {
        await RunAsync(async (database, ct) =>
        {
            const int cap = 2;
            var collector = new QueryStoreForcedPlansCollector(cap);
            var uncapped = VariantsOf(await PassAsync(new QueryStoreForcedPlansCollector(QueryStoreForcedPlansCollector.MaxNewXmlPlansPerDatabasePerPass), database, ct));
            var later = await LaterPlanCountAsync(database, ct);
            Assert.True(later > cap, $"the fixture must have more later plans than the cap; it has {later}");

            var seen = 0L;
            var passes = 0;
            string?[] variants;
            do
            {
                var pass = await PassAsync(collector, database, ct);
                passes++;
                var read = collector.XmlPlansChecked - seen;
                seen = collector.XmlPlansChecked;
                Assert.True(read <= cap, $"pass {passes} read {read} plans' XML, over the cap of {cap}");
                variants = VariantsOf(pass);
            }
            while (seen < later && passes < later + 2);

            Assert.True(passes >= 2, "more unread plans than the cap take more than one pass");
            Assert.Equal(later, seen);
            Assert.Equal(uncapped, variants);

            var after = await PassAsync(collector, database, ct);
            Assert.Equal(0, after.PlanXmlLobReads);
            Assert.Equal(later, collector.XmlPlansChecked);
        });
    }

    [Fact]
    public async Task APlanThatLeavesTheForcedQuerysLaterPlans_LeavesTheCache()
    {
        await RunAsync(async (database, ct) =>
        {
            var collector = new QueryStoreForcedPlansCollector(QueryStoreForcedPlansCollector.MaxNewXmlPlansPerDatabasePerPass);
            await PassAsync(collector, database, ct);
            Assert.Equal(await LaterPlanCountAsync(database, ct), collector.CachedPlanCount(ServerId, database));

            await SqlAsync(database, @"
DECLARE @q bigint, @p bigint;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT query_id, plan_id FROM sys.query_store_plan WHERE is_forced_plan = 1;
OPEN c; FETCH NEXT FROM c INTO @q, @p;
WHILE @@FETCH_STATUS = 0 BEGIN EXEC sys.sp_query_store_unforce_plan @q, @p; FETCH NEXT FROM c INTO @q, @p; END;
CLOSE c; DEALLOCATE c;", ct);
            var after = await PassAsync(collector, database, ct);

            Assert.Equal(0, collector.CachedPlanCount(ServerId, database));
            Assert.Equal(0, after.PlanXmlLobReads);
        });
    }
}
