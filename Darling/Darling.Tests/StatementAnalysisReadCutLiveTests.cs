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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: Darling's analysis readers return the WHOLE statement, the statement filter judges it, and the cut comes
/// after. Each case plants a batch whose named value (a URI's <c>user:secret@</c>) sits inside the cut and whose
/// naming at-sign sits past it, so a reader that cuts in SQL first hands the filter a prefix that reads clean and
/// the answer holds the start of the secret. A plain statement past the cut is cut exactly as before.
/// One case per reader family: blocking, queries, activity (the bad-actor read), pileup, target deadlocks.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementAnalysisReadCutLiveTests
{
    private const string ServerName = "statement-analysis-cut-e2e";
    private const string PgServerName = "statement-analysis-cut-pg-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int PgServerId = ServerIdHelper.GetDeterministicHashCode(PgServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>A plain statement of 700 characters: kept as the first 500 and nothing more.</summary>
    private static readonly string Plain = "SELECT plain_ssf " + new string('x', 683);

    private static string Json(object? value) => JsonSerializer.Serialize(value);

    private static void AssertNamedWithheldAndPlainCutAtFiveHundred(string json)
    {
        Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, json, StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, json, StringComparison.Ordinal);
        Assert.Contains(Plain[..500], json, StringComparison.Ordinal);
        Assert.DoesNotContain(Plain[..501], json, StringComparison.Ordinal);
    }

    private sealed record Run(NpgsqlConnection Connection, NpgsqlDataSource Postgres, AnalysisContext Context, CancellationToken Ct);

    private static async Task RunAsync(Func<Run, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis cut test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, PgServerId, PgServerName, ct);
            var end = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(1);
            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            await body(new Run(connection, postgres, context, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<AnalysisFinding> EnrichAsync(PgDrillDownCollector collector, Run run, string rootKey)
    {
        var finding = new AnalysisFinding { RootFactKey = rootKey, StoryPath = rootKey, PathKeys = [rootKey], Severity = 1.0 };
        await collector.EnrichFindingsAsync([finding], run.Context);
        Assert.NotNull(finding.DrillDown);
        return finding;
    }

    [Fact]
    public async Task BlockingReaders_JudgeTheWholeStatement_ThenCutAtFiveHundred()
    {
        await RunAsync(async run =>
        {
            var at = run.Context.TimeRangeStart.AddMinutes(30);
            await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text)
VALUES ($1, $2, $3, $4, $2, 'UriDb', 71, 60, 12000, 'X', $5, $6)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, StatementScrubCanary.UriStatement(500), Plain);
            await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml, database_name)
VALUES ($1, $2, $3, $4, $2, 'process1', $5, '<deadlock-list/>', 'UriDb'), ($6, $2, $3, $4, $2, 'process2', $7, '<deadlock-list/>', 'UriDb')",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, StatementScrubCanary.UriStatement(500), CollectionIdGenerator.Next(), Plain);

            var collector = new PgDrillDownCollector(run.Postgres);
            var top = await EnrichAsync(collector, run, "BLOCKING_EVENTS");
            AssertNamedWithheldAndPlainCutAtFiveHundred(Json(top.DrillDown!["top_blocking_chains"]));

            var chains = await EnrichAsync(collector, run, "BLOCKING_CHAIN");
            AssertNamedWithheldAndPlainCutAtFiveHundred(Json(chains.DrillDown!["reconstructed_blocking_chains"]));

            var deadlocks = await EnrichAsync(collector, run, "DEADLOCKS");
            AssertNamedWithheldAndPlainCutAtFiveHundred(Json(deadlocks.DrillDown!["top_deadlocks"]));
        });
    }

    [Fact]
    public async Task QueryReaders_JudgeTheWholeStatement_ThenCutAtFiveHundred()
    {
        await RunAsync(async run =>
        {
            var at = run.Context.TimeRangeStart.AddMinutes(30);
            foreach (var (hash, text) in new[] { ("0xURICUT", StatementScrubCanary.UriStatement(500)), ("0xPLAINCUT", Plain) })
            {
                await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
     creation_time, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills, min_dop, max_dop)
VALUES ($1, $2, $3, $4, 'UriDb', $5, '0xPLAN', '0xSQLH', '0xPLANH', $6, $7, 10, 500000, 900000, 1000, 5, 1, 1)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, hash, at.AddDays(-1), text);
            }

            var collector = new PgDrillDownCollector(run.Postgres);
            var spills = await EnrichAsync(collector, run, "QUERY_SPILLS");
            AssertNamedWithheldAndPlainCutAtFiveHundred(Json(spills.DrillDown!["top_spilling_queries"]));

            var cpu = await EnrichAsync(collector, run, "CPU_SQL_PERCENT");
            AssertNamedWithheldAndPlainCutAtFiveHundred(Json(cpu.DrillDown!["top_cpu_queries"]));
        });
    }

    [Fact]
    public async Task BadActorFactRead_CarriesNoStatementText()
    {
        await RunAsync(async run =>
        {
            var at = run.Context.TimeRangeStart.AddMinutes(30);
            await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
     creation_time, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills, min_dop, max_dop)
VALUES ($1, $2, $3, $4, 'UriDb', '0xBADACTOR', '0xPLAN', '0xSQLH', '0xPLANH', $5, $6, 500, 90000000, 90000000, 900000, 0, 1, 1)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, at.AddDays(-1), StatementScrubCanary.UriStatement(200));

            /* The fact is keyed on the hash and carries numbers only: its read must not fetch a cut statement it never prints. */
            Assert.DoesNotContain("query_text", PgFactCollector.BadActorSql, StringComparison.Ordinal);
            Assert.DoesNotContain("LEFT(", PgFactCollector.BadActorSql, StringComparison.Ordinal);
            await using var cmd = new NpgsqlCommand(PgFactCollector.BadActorSql, run.Connection);
            cmd.Parameters.AddWithValue(ServerId);
            cmd.Parameters.AddWithValue(run.Context.TimeRangeStart);
            cmd.Parameters.AddWithValue(run.Context.TimeRangeEnd);
            await using var reader = await cmd.ExecuteReaderAsync(run.Ct);
            Assert.True(await reader.ReadAsync(run.Ct));
            Assert.Equal(10, reader.FieldCount);
            for (var i = 0; i < reader.FieldCount; i++)
                Assert.NotEqual("query_text", reader.GetName(i));
        });
    }

    [Fact]
    public async Task PileupReader_JudgesTheWholeStatement_AndKeepsTheIdentityTextCutTheOldWay()
    {
        await RunAsync(async run =>
        {
            var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(-5);
            var named = StatementScrubCanary.UriStatement(1500);
            var plain = "SELECT plain_ssf " + new string('x', 1983);
            foreach (var (session, text) in new[] { (91, named), (92, plain) })
            {
                await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, total_elapsed_time_ms)
VALUES ($1, $2, $3, $4, $5, 'UriDb', $6, 'running', 9000)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, session, text);
            }

            var rows = await new PgPileupSnapshotReader(run.Postgres).ReadWindowAsync(ServerId, at.AddMinutes(-10), run.Ct);
            var namedRow = Assert.Single(rows, r => r.SessionId == 91);
            var plainRow = Assert.Single(rows, r => r.SessionId == 92);

            /* The shown text is judged whole: no start of the secret, the marker instead. */
            Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, namedRow.PreviewText, StringComparison.Ordinal);
            Assert.Equal(SensitiveStatements.PlaceholderText, namedRow.PreviewText);
            /* The identity text is the old SQL cut (1500 characters), unjudged, so a null-hash row groups as it did. */
            Assert.Equal(named[..1500], namedRow.QueryText);
            Assert.Equal(plain[..1500], plainRow.QueryText);
            Assert.Equal(plain[..1500], plainRow.PreviewText);
            Assert.Equal(PgPileupSnapshotReader.StatementTextCharacters, plainRow.PreviewText!.Length);
        });
    }

    [Fact]
    public async Task TargetDeadlockReader_JudgesTheWholeNormalizedStatement_ThenCuts()
    {
        await RunAsync(async run =>
        {
            /* The marker sits inside the 2,000-character cut; the statement that names it (CREATE ROLE) sits past it. */
            var marker = "SELECT marker_pg_ssf" + string.Concat(Enumerable.Repeat(", 1", 800)) + " ; CREATE ROLE zz_ssf";
            var plain = "SELECT plain_pg_ssf" + string.Concat(Enumerable.Repeat(", 1", 800));
            var at = run.Context.TimeRangeStart.AddMinutes(30);
            foreach (var (shape, statement) in new[] { ("relation a", marker), ("relation b", plain) })
            {
                await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $2, 4242, 2, upper(left(encode(sha256(convert_to($5, 'UTF8')), 'hex'), 32)), 'ShareLock', $6, $5, 'graph')",
                    CollectionIdGenerator.Next(), at, PgServerId, PgServerName, statement, shape);
            }

            var context = new AnalysisContext
            {
                ServerId = PgServerId, ServerName = PgServerName, TimeRangeStart = run.Context.TimeRangeStart, TimeRangeEnd = run.Context.TimeRangeEnd,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = run.Ct
            };
            var finding = new AnalysisFinding
            {
                RootFactKey = PgTargetFactKeys.DeadlockRate, StoryPath = PgTargetFactKeys.DeadlockRate,
                PathKeys = [PgTargetFactKeys.DeadlockRate], Severity = 1.0
            };
            await new PgTargetDrillDownCollector(run.Postgres).EnrichFindingsAsync([finding], context);
            var json = Json(finding.DrillDown![PgTargetDrillDownCollector.DeadlockExemplarsSection]);

            Assert.DoesNotContain("marker_pg_ssf", json, StringComparison.Ordinal);
            Assert.Contains(SensitiveStatements.PlaceholderText, json, StringComparison.Ordinal);
            Assert.Contains("plain_pg_ssf", json, StringComparison.Ordinal);
        });
    }

    private static string Graph(string inputBuffer, int pad = 0) =>
        "<deadlock" + (pad > 0 ? " note=\"" + new string('n', pad) + "\"" : "") + "><victim-list><victimProcess id=\"p1\"/></victim-list><process-list><process id=\"p1\"><inputbuf>"
        + inputBuffer + "</inputbuf></process></process-list></deadlock>";

    private static async Task<(string Xml, bool Truncated)> DeadlockPreviewOfAsync(Run run, string graph)
    {
        await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, "DELETE FROM deadlocks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(run.Connection, run.Ct, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, victim_sql_text)
VALUES ($1, $2, $3, $4, $2, $5, 'x')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName, graph);
        var json = await PerformanceMonitor.Darling.Service.Mcp.DarlingMcpBlockingTools.GetDeadlockDetail(run.Postgres, ServerName);
        var row = System.Text.Json.Nodes.JsonNode.Parse(json)!["deadlocks"]![0]!;
        return ((string)row["deadlock_graph_xml"]!, (bool)row["deadlock_graph_xml_truncated"]!);
    }

    [Fact]
    public async Task DeadlockDetail_PreviewTruncatedFlag_IsReadOffTheFilteredGraph_NotTheRawOne()
    {
        await RunAsync(async run =>
        {
            /* Shrink: a raw graph past the 2000-character preview whose long statement the filter replaces by the marker comes
               back SHORT, so nothing was cut and the flag is false (the raw length said true). */
            var longSecret = "CREATE LOGIN [shrink_ssf] WITH PASSWORD = N'" + new string('s', 1500) + "'";
            var shrinking = Graph(longSecret + new string(' ', 400));
            Assert.True(shrinking.Length > 2000);
            var shrunk = await DeadlockPreviewOfAsync(run, shrinking);
            Assert.False(shrunk.Truncated);
            Assert.DoesNotContain("PASSWORD", shrunk.Xml, StringComparison.OrdinalIgnoreCase);
            Assert.True(shrunk.Xml.Length < 2000);

            /* Growth: a raw graph of exactly 2000 characters whose short statement becomes the longer marker is over 2000
               filtered, so the preview cuts it and the flag is true (the raw length said false). */
            var shortSecret = "CREATE LOGIN a WITH PASSWORD='x'";
            var padded = Graph(shortSecret, pad: 2000 - Graph(shortSecret).Length - " note=\"\"".Length);
            Assert.Equal(2000, padded.Length);
            Assert.True(SensitiveStatements.Xml(padded)!.Length > 2000);
            var grown = await DeadlockPreviewOfAsync(run, padded);
            Assert.True(grown.Truncated);
            Assert.EndsWith("... (truncated)", grown.Xml, StringComparison.Ordinal);
            Assert.DoesNotContain("PASSWORD", grown.Xml, StringComparison.OrdinalIgnoreCase);

            /* A plain graph is cut once and flagged once, on both lengths. */
            Assert.True((await DeadlockPreviewOfAsync(run, Graph(new string('p', 2500)))).Truncated);
            Assert.False((await DeadlockPreviewOfAsync(run, Graph("SELECT 1"))).Truncated);
        });
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "blocked_process_reports", "deadlocks", "query_stats", "query_snapshots", "pg_deadlocks" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = ANY($1)", new[] { ServerId, PgServerId });
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = ANY($1)", new[] { ServerId, PgServerId });
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = ANY($1)", new[] { ServerId, PgServerId });
    }
}
