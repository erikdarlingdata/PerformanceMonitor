/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4247 ruling 7's second bullet: the five indexes <see cref="PgTableTuning.Statements"/> drops only left
/// because their slowest reader, WITHOUT the index, measured under HALF that reader's deadline (the rig numbers
/// backing that claim are the comments on each DROP statement in PgTableTuning.cs). This proves the same claim
/// live: it hand-creates the five OLD-shape indexes (the pre-#4247 shape — the same move
/// <see cref="TuningDropLockRecoveryLiveTests"/> makes for one of them), runs <see cref="PgTableTuning.ApplyAsync"/>
/// so the drop is a REAL state transition (a store that never had them would pass the absence checks below even
/// if ApplyAsync were broken), asserts all five are gone, seeds a modest row set, then times each named reader
/// and asserts it stays under HALF its stated deadline — the same bar the ruling applied.
///
/// <para>Two readers run directly off their own SQL constants, no composer involved:
/// <see cref="DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql"/> (McpCommandDeadlines.ReadSeconds = 20 s)
/// and <see cref="ViewerDataService.ProcedureStatsComparisonSql"/>'s LATERAL join
/// (ViewerCommandDeadlines.InteractiveReadSeconds = 15 s). The other three go through
/// <see cref="ComposeCompiler.Compile"/> with <see cref="RollupAvailability.None"/> and
/// <see cref="RollupCoverage.Unknown"/>, which pins the raw route regardless of window age
/// (<c>ComposeSourceRouterTests.OldWindow_NoRollupsInStore_RoutesRaw</c>) — so the compiled SQL actually touches
/// the un-indexed raw table, never a rollup.</para>
///
/// <para>Shares the serialized "live-postgres" collection (it touches the shared store's collector tables directly,
/// like every other class in this collection) and cleans up in finally via <see cref="LiveStoreCleanup"/>. Not a
/// <c>ScratchPostgres</c> own-store test: both direct-SQL readers here use UNQUALIFIED table names that only
/// resolve once the session's search_path is the migrated one, which the shared store already carries.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TuningReaderBudgetLiveTests
{
    private const int ServerId = -974247;
    private const string ServerName = "tuning-reader-budget";

    private const string DatabaseName = "BudgetDb";
    private const string ObjectName = "usp_4247Budget";
    private const string SqlHandle = "0x4247PLANHANDLE";
    private const string QueryHash = "0x4247QUERYHASH";
    private const string QsQueryHash = "0x4247QSHASH";
    private const string SamplePlanXml = "<ShowPlanXML>budget</ShowPlanXML>";

    /// <summary>#4294's table: the composer's own statement_timeout deadline for every one of its raw-tier
    /// reads (PgTableTuning.cs documents the same figure on each composer DROP comment).</summary>
    private const int ComposerStatementTimeoutSeconds = 15;

    /// <summary>
    /// The pre-#4247 shape of the five dropped indexes, hand-typed from the DROP comments in
    /// <see cref="PgTableTuning"/> — NOT derived from <see cref="PgTableTuning.Statements"/>. Deriving them would
    /// make the revert-proof (see the PR body) vacuous: reverting one DROP back to a CREATE would also remove it
    /// from a derived list, so the "still absent" assertion below would keep passing for the wrong reason.
    /// </summary>
    private static readonly IReadOnlyList<string> OldShapeIndexes = new[]
    {
        "CREATE INDEX idx_procedure_stats_object_name ON collect.procedure_stats (object_name, collection_time) " +
            "INCLUDE (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count)",
        "CREATE INDEX idx_procedure_stats_server_handle_time ON collect.procedure_stats (server_id, sql_handle, collection_time DESC)",
        "CREATE INDEX idx_query_stats_server_handle_time ON collect.query_stats (server_id, sql_handle, collection_time DESC)",
        "CREATE INDEX idx_query_stats_query_hash ON collect.query_stats (query_hash, collection_time) " +
            "INCLUDE (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count)",
        "CREATE INDEX idx_query_store_stats_query_hash ON collect.query_store_stats (query_hash, collection_time) " +
            "INCLUDE (database_name, module_name, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us, max_cpu_time_us)",
    };

    private static readonly string[] DroppedIndexNames =
    {
        "idx_procedure_stats_object_name",
        "idx_procedure_stats_server_handle_time",
        "idx_query_stats_server_handle_time",
        "idx_query_stats_query_hash",
        "idx_query_store_stats_query_hash",
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task NamedReaders_StayUnderHalfTheirDeadline_OnceTheFiveIndexesAreDropped()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4247 reader-budget live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* Hand-create the old shape so the drop below is a REAL transition, not a vacuous absent -> absent. */
            foreach (var createOldShape in OldShapeIndexes)
            {
                await using var create = new NpgsqlCommand(createOldShape, connection);
                await create.ExecuteNonQueryAsync(ct);
            }

            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);

            foreach (var indexName in DroppedIndexNames)
            {
                Assert.False(await IndexExistsAsync(connection, indexName, ct),
                    $"{indexName} should be gone after PgTableTuning.ApplyAsync — #4247 dropped it.");
            }

            var end = new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
            var currentStart = end.AddHours(-1);
            var baselineStart = currentStart.AddHours(-1);
            var collectionTime = currentStart.AddMinutes(10);

            await SeedAsync(connection, collectionTime, ct);

            /* ---- DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql — McpCommandDeadlines.ReadSeconds = 20 s. */
            await using (var planCommand = new NpgsqlCommand(DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql, connection))
            {
                planCommand.Parameters.AddWithValue(ServerId);
                planCommand.Parameters.AddWithValue(SqlHandle);

                var clock = Stopwatch.StartNew();
                var gotRow = false;
                await using (var reader = await planCommand.ExecuteReaderAsync(ct))
                {
                    gotRow = await reader.ReadAsync(ct);
                }
                clock.Stop();

                Assert.True(gotRow, "ProcedurePlanXmlBySqlHandleSql returned no rows — the seed must return a captured plan.");
                AssertUnderHalf(clock.Elapsed, McpCommandDeadlines.ReadSeconds, nameof(DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql));
            }

            /* ---- ViewerDataService.ProcedureStatsComparisonSql — ViewerCommandDeadlines.InteractiveReadSeconds = 15 s. */
            await using (var comparisonCommand = new NpgsqlCommand(ViewerDataService.ProcedureStatsComparisonSql, connection))
            {
                ViewerDataService.AddComparisonParameters(comparisonCommand, ServerId, currentStart, end, baselineStart, currentStart);
                comparisonCommand.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));

                var clock = Stopwatch.StartNew();
                var gotRow = false;
                await using (var reader = await comparisonCommand.ExecuteReaderAsync(ct))
                {
                    gotRow = await reader.ReadAsync(ct);
                }
                clock.Stop();

                Assert.True(gotRow, "ProcedureStatsComparisonSql returned no rows — the seed must produce a current-window comparison row.");
                AssertUnderHalf(clock.Elapsed, ViewerCommandDeadlines.InteractiveReadSeconds, nameof(ViewerDataService.ProcedureStatsComparisonSql));
            }

            /* ---- Composer, raw route forced by RollupAvailability.None (ComposeSourceRouterTests.OldWindow_NoRollupsInStore_RoutesRaw). */
            await RunComposerReaderAsync(connection, "procedure_stats", "proc_worker_us", "object_name", end,
                "composer object_name dimension (Procedures measures)", ct);
            await RunComposerReaderAsync(connection, "query_stats", "query_worker_us", "query_hash", end,
                "composer query_hash dimension (Queries measures)", ct);
            await RunComposerReaderAsync(connection, "query_store_stats", "qs_executions", "query_hash", end,
                "composer query_hash dimension (Query Store measures)", ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static void AssertUnderHalf(TimeSpan elapsed, double deadlineSeconds, string readerLabel)
    {
        var half = TimeSpan.FromSeconds(deadlineSeconds / 2.0);
        Assert.True(elapsed < half,
            $"{readerLabel} took {elapsed.TotalMilliseconds:F1} ms — the #4247 ruling requires staying under half " +
            $"its {deadlineSeconds}s deadline ({half.TotalMilliseconds:F0} ms) once its index is dropped.");
    }

    /// <summary>Parses, compiles (RollupAvailability.None, RollupCoverage.Unknown — pins the raw route), and RUNS
    /// one topN-by-<paramref name="dimension"/> panel on <paramref name="source"/>, timing the execution and
    /// asserting it both returns rows and stays under half <see cref="ComposerStatementTimeoutSeconds"/>.</summary>
    private static async Task RunComposerReaderAsync(
        NpgsqlConnection connection, string source, string measure, string dimension, DateTime end, string readerLabel, CancellationToken ct)
    {
        var panelJson = "{\"source\":\"" + source + "\",\"measure\":\"" + measure + "\",\"aggregate\":\"sum\"," +
            "\"topN\":10,\"groupBy\":[\"" + dimension + "\"],\"viz\":\"bar\"}";

        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panelJson)!, []);
        Assert.True(parseError is null, parseError);

        var (compiled, compileError) = ComposeCompiler.Compile(
            plan!,
            new ComposeRunContext([ServerName], end.AddHours(-3), end, ComposeRunContext.NoVariables,
                RollupAvailability.None, end, RollupCoverage.Unknown));
        Assert.True(compileError is null, compileError);
        Assert.False(compiled!.Route.IsCagg,
            $"{readerLabel}: RollupAvailability.None must pin the raw route so this exercises the un-indexed raw table.");

        await using var command = new NpgsqlCommand(compiled.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var clock = Stopwatch.StartNew();
        var rowCount = 0;
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rowCount++;
            }
        }
        clock.Stop();

        Assert.True(rowCount > 0, $"{readerLabel} returned no rows — the seed must feed this dimension.");
        AssertUnderHalf(clock.Elapsed, ComposerStatementTimeoutSeconds, readerLabel);
    }

    /// <summary>A modest five rows per table (CI time), all sharing the fixture's keys and spread a minute apart
    /// inside the current window — enough for every reader above to return rows without a large seed.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime collectionTimeUtc, CancellationToken ct)
    {
        for (var i = 0; i < 5; i++)
        {
            var t = DateTime.SpecifyKind(collectionTimeUtc.AddMinutes(i), DateTimeKind.Unspecified);

            await using (var insertProc = new NpgsqlCommand(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, query_plan_xml)
VALUES (1, $1, $2, $3, $4, 'dbo', $5, $6, 100, 200, 1, $7)", connection))
            {
                insertProc.Parameters.AddWithValue(t);
                insertProc.Parameters.AddWithValue(ServerId);
                insertProc.Parameters.AddWithValue(ServerName);
                insertProc.Parameters.AddWithValue(DatabaseName);
                insertProc.Parameters.AddWithValue(ObjectName);
                insertProc.Parameters.AddWithValue(SqlHandle);
                insertProc.Parameters.AddWithValue(SamplePlanXml);
                await insertProc.ExecuteNonQueryAsync(ct);
            }

            await using (var insertQuery = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, query_text)
VALUES (1, $1, $2, $3, $4, $5, $6, 100, 200, 1, $7)", connection))
            {
                insertQuery.Parameters.AddWithValue(t);
                insertQuery.Parameters.AddWithValue(ServerId);
                insertQuery.Parameters.AddWithValue(ServerName);
                insertQuery.Parameters.AddWithValue(DatabaseName);
                insertQuery.Parameters.AddWithValue(QueryHash);
                insertQuery.Parameters.AddWithValue(SqlHandle);
                insertQuery.Parameters.AddWithValue("SELECT 4247 /* reader budget */");
                await insertQuery.ExecuteNonQueryAsync(ct);
            }

            await using (var insertQs = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_hash, execution_count,
     avg_duration_us, avg_cpu_time_us)
VALUES (1, $1, $2, $3, $4, $5, $6, 'Regular', $7, $8, $9, 10, 1000, 500)", connection))
            {
                insertQs.Parameters.AddWithValue(t);
                insertQs.Parameters.AddWithValue(ServerId);
                insertQs.Parameters.AddWithValue(ServerName);
                insertQs.Parameters.AddWithValue(DatabaseName);
                insertQs.Parameters.AddWithValue((long)(i + 1));
                insertQs.Parameters.AddWithValue((long)(100 + i));
                insertQs.Parameters.AddWithValue(t);
                insertQs.Parameters.AddWithValue(t);
                insertQs.Parameters.AddWithValue(QsQueryHash);
                await insertQs.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", connection);
        command.Parameters.AddWithValue(indexName);
        var result = await command.ExecuteScalarAsync(ct);
        return result is not null;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
