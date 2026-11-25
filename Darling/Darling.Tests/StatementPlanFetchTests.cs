/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9195: a bad actor's live plan is fetched for its own statement. A plan handle names a whole batch, and a batch can
/// hold many statements and megabytes of plan XML, so the fetch asks the text DMV for the statement the
/// stored byte offsets locate and falls back to the batch DMV when the offsets are not known. These tests hold the
/// parameters the fetcher sends: no connection is opened, and the command is read as it would be executed. The DMV call
/// itself runs against a real SQL Server in <see cref="StatementPlanFetchLiveSqlTests"/>.
/// </summary>
public sealed class PgPlanFetcherStatementPlanTests
{
    private const string Handle = "0x06000500FE5C5D1CB0E1DB5A0300000001000000000000000000000000000000000000000000000000000000";

    [Fact]
    public void WithOffsets_TheCommandCarriesTheHandleAndBothOffsets()
    {
        using var connection = new SqlConnection();

        using var command = PgPlanFetcher.CreatePlanCommand(connection, Handle, (4096, 8192));

        Assert.Equal(15, command.CommandTimeout);
        Assert.Equal(3, command.Parameters.Count);
        Assert.Equal(Handle, command.Parameters["@plan_handle"].Value);
        AssertIntParameter(command, "@statement_start_offset", 4096);
        AssertIntParameter(command, "@statement_end_offset", 8192);
    }

    [Fact]
    public void AnEndOffsetOfMinusOne_IsSentAsMinusOne()
    {
        using var connection = new SqlConnection();

        using var command = PgPlanFetcher.CreatePlanCommand(connection, Handle, (0, -1));

        AssertIntParameter(command, "@statement_start_offset", 0);
        AssertIntParameter(command, "@statement_end_offset", -1);
    }

    [Fact]
    public void WithoutOffsets_TheCommandCarriesOnlyTheHandle()
    {
        using var connection = new SqlConnection();

        using var command = PgPlanFetcher.CreatePlanCommand(connection, Handle, null);

        Assert.Equal(15, command.CommandTimeout);
        var only = Assert.Single(command.Parameters.OfType<SqlParameter>());
        Assert.Equal("@plan_handle", only.ParameterName);
        Assert.Equal(Handle, only.Value);
    }

    private static void AssertIntParameter(SqlCommand command, string name, int value)
    {
        var parameter = command.Parameters[name];
        Assert.Equal(SqlDbType.Int, parameter.SqlDbType);
        Assert.Equal(value, parameter.Value);
    }
}

/// <summary>
/// #9195, Darling's twin of <c>Lite.Tests.BadActorStatementPlanFetchTests</c>: <see cref="PgDrillDownCollector"/> reads the
/// bad actor's plan_handle and statement offsets from the newest <c>v_query_stats</c> row of the hash and hands them to the
/// plan fetcher together. A newest row stored before the offsets existed carries none, and the fetch is the whole batch's:
/// an older row's offsets never pair with the newest row's handle. The warnings of a statement plan are that statement's
/// own, and the statement filter judges the statement plan before it is analyzed.
///
/// <para>Live Postgres, because the claim is which row the store's view returns for the hash; the fetcher is a fake that
/// answers each call by the entry point it came in through.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class BadActorStatementPlanFetchLiveTests
{
    private const int TestServerId = -919500;
    private const string TestServerName = "StatementPlanSrv";
    private const string Database = "StatementPlanDb";
    private const string Hash = "0x9195STMT";
    private const string FactKey = "BAD_ACTOR_" + Hash;

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static AnalysisFinding Finding() => new()
    {
        RootFactKey = FactKey,
        StoryPath = FactKey,
        PathKeys = [FactKey],
        Severity = 1.0,
        DatabaseName = Database,
    };

    [Fact]
    public Task TheNewestRowsHandle_AndItsOffsets_AreFetchedTogether() => RunAsync(async (postgres, context, windowEnd, seed) =>
    {
        await seed("0x9195OLDHANDLE", 10, 20, windowEnd.AddMinutes(-90));
        await seed("0x9195NEWHANDLE", 4096, 8192, windowEnd.AddMinutes(-30));
        var fetcher = new RecordingFetcher();

        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([Finding()], context);

        Assert.Equal(new[] { "statement 0x9195NEWHANDLE 4096 8192" }, fetcher.Calls);
    });

    [Fact]
    public Task ANewestRowWithoutOffsets_FetchesTheBatch_NotAnOlderRowsOffsets() => RunAsync(async (postgres, context, windowEnd, seed) =>
    {
        await seed("0x9195OLDHANDLE", 10, 20, windowEnd.AddMinutes(-90));
        await seed("0x9195NEWHANDLE", null, null, windowEnd.AddMinutes(-30));
        var fetcher = new RecordingFetcher();

        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([Finding()], context);

        Assert.Equal(new[] { "batch 0x9195NEWHANDLE" }, fetcher.Calls);
    });

    [Fact]
    public Task AnEndOffsetOfMinusOne_ReachesTheFetcherAsMinusOne() => RunAsync(async (postgres, context, windowEnd, seed) =>
    {
        await seed("0x9195NEWHANDLE", 0, -1, windowEnd.AddMinutes(-30));
        var fetcher = new RecordingFetcher();

        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([Finding()], context);

        Assert.Equal(new[] { "statement 0x9195NEWHANDLE 0 -1" }, fetcher.Calls);
    });

    [Fact]
    public Task ANullStatementPlan_ReturnsNoPlanAnalysis_AndNoBatchFetch() => RunAsync(async (postgres, context, windowEnd, seed) =>
    {
        await seed("0x9195NEWHANDLE", 4096, 8192, windowEnd.AddMinutes(-30));
        var fetcher = new RecordingFetcher();
        var finding = Finding();

        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([finding], context);

        Assert.Equal(new[] { "statement 0x9195NEWHANDLE 4096 8192" }, fetcher.Calls);
        Assert.False(finding.DrillDown!.ContainsKey("plan_analysis"));
    });

    [Fact]
    public Task APlanAnalysisOverAStatementPlan_CountsOnlyThatStatementsWarnings() => RunAsync(async (postgres, context, windowEnd, seed) =>
    {
        var fetcher = new RecordingFetcher { BatchPlan = BatchPlanOfFourStatements(), StatementPlan = OneStatementPlan() };

        /* The statement's offsets are known: the plan is that one statement. */
        await seed("0x9195NEWHANDLE", 4096, 8192, windowEnd.AddMinutes(-30));
        var statementFinding = Finding();
        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([statementFinding], context);
        var statementWarnings = WarningCount(statementFinding);

        /* The offsets are not known: the plan is the whole batch, four statements with the same warning. */
        await seed("0x9195NEWHANDLE", null, null, windowEnd.AddMinutes(-20));
        var batchFinding = Finding();
        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([batchFinding], context);
        var batchWarnings = WarningCount(batchFinding);

        Assert.Equal(new[] { "statement 0x9195NEWHANDLE 4096 8192", "batch 0x9195NEWHANDLE" }, fetcher.Calls);
        Assert.True(statementWarnings > 0, "the statement's warning was not counted");
        Assert.Equal(statementWarnings * 4, batchWarnings);
    });

    [Fact]
    public Task TheStatementFilter_JudgesTheStatementPlan_BeforeItIsAnalyzed() => RunAsync(async (postgres, context, windowEnd, seed) =>
    {
        await seed("0x9195NEWHANDLE", 4096, 8192, windowEnd.AddMinutes(-30));
        var fetcher = new RecordingFetcher { StatementPlan = TwoStatementPlanWithACanary() };
        var finding = Finding();

        await new PgDrillDownCollector(postgres, planFetcher: fetcher).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("plan_analysis", out var raw), "plan_analysis was not collected");
        var text = JsonSerializer.Serialize(raw);
        Assert.Contains("kept-predicate-9195", text, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    });

    private static int WarningCount(AnalysisFinding finding)
    {
        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("plan_analysis", out var raw), "plan_analysis was not collected");
        return JsonSerializer.SerializeToElement(raw).GetProperty("warning_count").GetInt32();
    }

    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>One statement scanning a table under a predicate that wraps its column in a function.</summary>
    private static string Scan(string statementText, string predicate, string constant) =>
        "<StmtSimple StatementText=\"" + System.Net.WebUtility.HtmlEncode(statementText) + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"5\">"
        + "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Predicate><ScalarOperator ScalarString=\"" + System.Net.WebUtility.HtmlEncode(predicate) + "\"><Const ConstValue=\"" + constant + "\"/></ScalarOperator></Predicate>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>";

    private static string Plan(string statements) =>
        "<ShowPlanXML " + Ns + " Version=\"1.564\" Build=\"16.0.4215.2\"><BatchSequence><Batch><Statements>" + statements + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string BatchPlanOfFourStatements()
    {
        var statements = "";
        for (var i = 0; i < 4; i++)
            statements += Scan("SELECT a FROM dbo.t WHERE upper(a) = @p", "upper([db].[dbo].[t].[a])=[@p]", "1");
        return Plan(statements);
    }

    /// <summary>
    /// What <c>sys.dm_exec_text_query_plan</c> returns for one statement of a cached plan: a batch holding that one
    /// statement (SQL Server 2025, 17.0.4050.1). A cached plan never nests a procedure body under an EXEC statement. The
    /// body's statements are cached under the procedure's own plan_handle, and the statement DMVs return them one at a time.
    /// </summary>
    private static string OneStatementPlan() => Plan(
        Scan("SELECT a FROM dbo.t WHERE upper(a) = @p", "upper([db].[dbo].[t].[a])=[@p]", "1"));

    private static string TwoStatementPlanWithACanary() => Plan(
        Scan(StatementScrubCanary.CanaryStatement, "upper([db].[dbo].[t].[c])=N'const-secret-9195'", "N'const-secret-9195'")
        + Scan(StatementScrubCanary.PlainStatement, "upper([db].[dbo].[t].[c])=N'kept-predicate-9195'", "N'kept-predicate-9195'"));

    /// <summary>
    /// Answers each entry point with its own plan and records which one was called: the batch entry returns
    /// <see cref="BatchPlan"/> and the statement entry <see cref="StatementPlan"/>, either null when not set.
    /// </summary>
    private sealed class RecordingFetcher : IPlanFetcher
    {
        public List<string> Calls { get; } = [];
        public string? BatchPlan { get; init; }
        public string? StatementPlan { get; init; }

        public Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken)
        {
            Calls.Add($"batch {planHandle}");
            return Task.FromResult(BatchPlan);
        }

        public Task<string?> FetchStatementPlanXmlAsync(
            int serverId, string planHandle, int statementStartOffset, int statementEndOffset, CancellationToken cancellationToken)
        {
            Calls.Add($"statement {planHandle} {statementStartOffset} {statementEndOffset}");
            return Task.FromResult(StatementPlan);
        }
    }

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Migrates, clears the test server's rows, then runs <paramref name="body"/> with a seeding delegate.</summary>
    private static async Task RunAsync(Func<NpgsqlDataSource, AnalysisContext, DateTime, Func<string, int?, int?, DateTime, Task>, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9195 statement plan fetch test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowEnd.AddHours(-4),
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            async Task Seed(string planHandle, int? statementStart, int? statementEnd, DateTime collectionTime)
            {
                await using var connection = await OpenWithSearchPathAsync(connectionString!, ct);
                await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, statement_start_offset, statement_end_offset, creation_time, query_text,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills, min_dop, max_dop)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,10,500000,1000000,1000,0,1,1)", connection);
                command.Parameters.AddWithValue(CollectionIdGenerator.Next());
                command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
                command.Parameters.AddWithValue(TestServerId);
                command.Parameters.AddWithValue(TestServerName);
                command.Parameters.AddWithValue(Database);
                command.Parameters.AddWithValue(Hash);
                command.Parameters.AddWithValue(planHandle + "P");
                command.Parameters.AddWithValue("0x9195SQLH");
                command.Parameters.AddWithValue(planHandle);
                command.Parameters.Add(new NpgsqlParameter { Value = statementStart.HasValue ? statementStart.Value : DBNull.Value });
                command.Parameters.Add(new NpgsqlParameter { Value = statementEnd.HasValue ? statementEnd.Value : DBNull.Value });
                command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime.AddDays(-1), DateTimeKind.Unspecified));
                command.Parameters.AddWithValue("SELECT * FROM StatementPlanTable");
                await command.ExecuteNonQueryAsync(ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            await body(postgres, context, windowEnd, Seed);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }
}
