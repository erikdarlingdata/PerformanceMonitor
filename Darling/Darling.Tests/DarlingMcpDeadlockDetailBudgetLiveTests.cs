/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198: get_deadlock_detail's own response-budget pin. deadlock_graph_xml is the wide field — a busy
/// production store's default call (limit 5, only 3 deadlocks carrying a graph in the window) measured
/// 120,454 bytes, almost all of it this one field (about 40 KB/graph). Plants five graphs near that width
/// (the worst realistic default page) and asserts the default call stays under
/// <see cref="McpResponseBudget.DefaultBytes"/>, that <c>full_graph: true</c> opts back into the whole
/// graph, and that a <c>dedup_key</c> call (naming one incident) returns the whole graph even without
/// <c>full_graph</c>. New file (not the shared seeding in <see cref="DarlingMcpBlockingToolsLivePostgresTests"/>)
/// because #4198 ran a dozen lanes against this store tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpDeadlockDetailBudgetLiveTests
{
    private const string ServerName = "darling-mcp-deadlock-detail-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private readonly ITestOutputHelper _output;

    public DarlingMcpDeadlockDetailBudgetLiveTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task GetDeadlockDetail_Default_StaysUnderResponseBudget_WithFiveWideGraphs()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-detail budget test.");

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
            var baseTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-30);
            var graphXml = BuildDeadlockGraphXml(approxLength: 42_000);

            for (var i = 0; i < 5; i++)
            {
                var t = baseTime.AddMinutes(i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, $"process{i}", "DELETE FROM Posts", graphXml);
            }

            var defaultJson = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(defaultJson, ServerName, "deadlock_graph_xml");
            JsonAssert.Contains("\"deadlocks_returned\": 5", defaultJson);
            JsonAssert.Contains("\"deadlock_graph_xml_truncated\": true", defaultJson);

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            _output.WriteLine($"get_deadlock_detail default call: {defaultBytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}), 5 planted {graphXml.Length:N0}-char graphs.");
            Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_deadlock_detail's default call is {defaultBytes:N0} bytes over five planted {graphXml.Length:N0}-char graphs, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var defaultParsed = JsonDocument.Parse(defaultJson);
            var defaultFirstGraph = defaultParsed.RootElement.GetProperty("deadlocks")[0].GetProperty("deadlock_graph_xml").GetString();
            Assert.NotNull(defaultFirstGraph);
            Assert.True(defaultFirstGraph!.Length < graphXml.Length,
                "the default call's graph should be a preview shorter than the planted graph.");

            /* full_graph opts back into the whole XML. */
            var fullJson = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, full_graph: true);
            Assert.DoesNotContain("\"deadlock_graph_xml_truncated\": true", fullJson, StringComparison.Ordinal);
            using var fullParsed = JsonDocument.Parse(fullJson);
            Assert.Equal(graphXml, fullParsed.RootElement.GetProperty("deadlocks")[0].GetProperty("deadlock_graph_xml").GetString());

            /* Naming one incident (dedup_key) returns the whole graph even without full_graph. */
            var firstKey = defaultParsed.RootElement.GetProperty("deadlocks")[0].GetProperty("dedup_key").GetString();
            Assert.False(string.IsNullOrEmpty(firstKey));

            var byKeyJson = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, dedup_key: firstKey, full_graph: false);
            Assert.DoesNotContain("\"deadlock_graph_xml_truncated\": true", byKeyJson, StringComparison.Ordinal);
            using var byKeyParsed = JsonDocument.Parse(byKeyJson);
            Assert.Equal(graphXml, byKeyParsed.RootElement.GetProperty("deadlocks")[0].GetProperty("deadlock_graph_xml").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Builds an XML string near <paramref name="approxLength"/> characters, ASCII only so its
    /// length in .NET UTF-16 chars and its size in UTF-8 bytes stay close (production deadlock graphs are
    /// almost entirely ASCII: object names, wait-resource strings, T-SQL).</summary>
    private static string BuildDeadlockGraphXml(int approxLength)
    {
        var sb = new StringBuilder();
        sb.Append("<deadlock><victim-list><victimProcess id=\"process0\"/></victim-list><process-list>");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append($"<process id=\"process{i}\" waitresource=\"KEY: 5:72057594057{i:D8}\"><inputbuf>UPDATE dbo.Posts SET Score = Score + 1 WHERE Id = {i};</inputbuf></process>");
            i++;
        }

        sb.Append("</process-list><resource-list><keylock objectname=\"StackOverflow.dbo.Posts\"/></resource-list></deadlock>");
        return sb.ToString();
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
