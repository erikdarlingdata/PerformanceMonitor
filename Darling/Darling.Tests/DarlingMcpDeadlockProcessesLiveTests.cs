/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
/// get_deadlock_detail serves each stored graph's parsed per-process rows in <c>processes[]</c>: read from a real
/// <c>deadlocks</c> row through the tool, the victim flagged, absent values left off, the per-deadlock cap counted in
/// <c>processes_truncated</c>, and a default call with a realistic six-process graph per deadlock staying inside the
/// shared response budget.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpDeadlockProcessesLiveTests
{
    private const string ServerName = "darling-mcp-deadlock-processes-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private readonly ITestOutputHelper _output;

    public DarlingMcpDeadlockProcessesLiveTests(ITestOutputHelper output) => _output = output;

    private static string SixProcessGraph()
    {
        var sb = new StringBuilder("<deadlock><victim-list><victimProcess id=\"process0\"/></victim-list><process-list>");
        for (var i = 0; i < 6; i++)
        {
            sb.Append($"<process id=\"process{i}\" spid=\"{60 + i}\" currentdbname=\"StackOverflow\" waitresource=\"KEY: 5:72057594057{i:D8} (a1b2c3d4e5f6)\" waittime=\"{1000 + i}\" lockMode=\"X\" isolationlevel=\"read committed (2)\" logused=\"{200 + i}\" trancount=\"1\" clientapp=\"Microsoft SQL Server Management Studio - Query\" hostname=\"APPHOST{i}\" loginname=\"CORP\\svc_app{i}\" status=\"suspended\" transactionname=\"user_transaction\" priority=\"0\" lasttranstarted=\"2026-07-01T10:00:00.000\">");
            sb.Append("<executionStack><frame procname=\"StackOverflow.dbo.usp_UpdateScore\" line=\"12\">UPDATE dbo.Posts SET Score = Score + 1 WHERE Id = @Id</frame></executionStack>");
            sb.Append($"<inputbuf>UPDATE dbo.Posts SET Score = Score + 1 WHERE Id = {i};{new string(' ', 4)}-- {new string('x', 300)}</inputbuf></process>");
        }

        sb.Append("</process-list><resource-list><keylock objectname=\"StackOverflow.dbo.Posts\" mode=\"X\">");
        sb.Append("<owner-list><owner id=\"process1\" mode=\"X\"/></owner-list><waiter-list><waiter id=\"process0\" mode=\"X\" requestType=\"wait\"/></waiter-list></keylock></resource-list></deadlock>");
        return sb.ToString();
    }

    [Fact]
    public async Task GetDeadlockDetail_ServesProcesses_VictimFlagged_NullsOmitted_AndStaysInBudget()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-processes test.");

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
            var graph = SixProcessGraph();
            for (var i = 0; i < 5; i++)
            {
                var t = baseTime.AddMinutes(i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "process0", "UPDATE dbo.Posts", graph);
            }

            var json = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName);
            var bytes = Encoding.UTF8.GetByteCount(json);
            _output.WriteLine($"get_deadlock_detail default call: {bytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}), 5 deadlocks x 6 processes, {graph.Length:N0}-char graphs.");
            Assert.True(bytes < McpResponseBudget.DefaultBytes, $"default call is {bytes:N0} bytes, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var doc = JsonDocument.Parse(json);
            var all = doc.RootElement.GetProperty("deadlocks").EnumerateArray().ToArray();
            var first = all[0];
            var processes = first.GetProperty("processes").EnumerateArray().ToArray();
            Assert.Equal(6, processes.Length);
            Assert.Equal(0, first.GetProperty("processes_truncated").GetInt32());

            /* The default page shares one row budget: 24 rows, four full deadlocks, and the fifth is counted rather than sent. */
            Assert.Equal(DarlingDeadlockProcessRows.DefaultPageRowBudget, all.Sum(d => d.GetProperty("processes").GetArrayLength()));
            Assert.Equal(6, all[4].GetProperty("processes_truncated").GetInt32());

            var victims = processes.Where(p => p.GetProperty("victim").GetBoolean()).ToArray();
            var victim = Assert.Single(victims);
            Assert.Equal(60, victim.GetProperty("spid").GetInt32());
            Assert.Equal("StackOverflow", victim.GetProperty("database_name").GetString());
            Assert.Equal("StackOverflow.dbo.usp_UpdateScore", victim.GetProperty("proc_name").GetString());
            Assert.Equal("StackOverflow.dbo.Posts", victim.GetProperty("object_names").GetString());
            Assert.Equal("X", victim.GetProperty("waiter_mode").GetString());

            /* The five non-victims never name the process that waits, so the owner row has no waiter_mode. */
            var owner = processes.Single(p => p.GetProperty("spid").GetInt32() == 61);
            Assert.False(owner.TryGetProperty("waiter_mode", out _));
            Assert.Equal("X", owner.GetProperty("owner_mode").GetString());
            Assert.All(processes, p => Assert.All(p.EnumerateObject(), prop => Assert.NotEqual(JsonValueKind.Null, prop.Value.ValueKind)));
            Assert.All(processes, p => Assert.True(p.GetProperty("sql_text").GetString()!.Length <= DarlingDeadlockProcessRows.DefaultStatementPreviewLength + 20));

            /* A whole-graph call keeps every row of every deadlock, up to the per-deadlock cap, with the longer preview. */
            var full = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, full_graph: true);
            _output.WriteLine($"get_deadlock_detail full_graph call: {Encoding.UTF8.GetByteCount(full):N0} bytes.");
            using var fullDoc = JsonDocument.Parse(full);
            Assert.All(fullDoc.RootElement.GetProperty("deadlocks").EnumerateArray(), d =>
            {
                Assert.Equal(6, d.GetProperty("processes").GetArrayLength());
                Assert.Equal(0, d.GetProperty("processes_truncated").GetInt32());
            });

            /* The graph is still served beside the rows. */
            Assert.True(first.TryGetProperty("deadlock_graph_xml", out _));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
