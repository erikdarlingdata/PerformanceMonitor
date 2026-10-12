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

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5042: two availability groups, each seen from two reporting servers (four views) with tens of databases per
/// view, serialize well past the 32 KB MCP budget. The web read (no byte budget) must return all four views;
/// the MCP read (the default budget) must cut, but keep at least one view of EACH group. Plus source pins that
/// <c>/api/ag</c> passes no budget, the MCP tool does not, and the page reads the truncation flag.
/// </summary>
public sealed class AgEveryViewLiveTests
{
    private const int DatabasesPerView = 12;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WebRead_ReturnsEveryView_McpRead_KeepsAViewOfEachGroup()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AG every-view pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var when = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2));
            string[] servers = ["ag-every-view-a", "ag-every-view-b"];
            string[] groups = ["AGONE", "AGTWO"];
            string[] groupIds = ["aaaaaaaa-1111-2222-3333-abcdefabcdef", "bbbbbbbb-1111-2222-3333-abcdefabcdef"];

            foreach (var serverName in servers)
            {
                var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
                await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

                for (var g = 0; g < groups.Length; g++)
                {
                    foreach (var replica in new[] { "NODE-A", "NODE-B" })
                    {
                        await DarlingMcpTestData.ExecAsync(connection, ct,
                            @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url, group_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                            CollectionIdGenerator.Next(), when, serverId, serverName, groups[g], replica, replica == "NODE-A" ? "PRIMARY" : "SECONDARY",
                            "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://" + replica + ":5022", groupIds[g]);

                        await DarlingMcpTestData.ExecAsync(connection, ct,
                            @"INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local, synchronization_state_desc, last_hardened_lsn, last_commit_lsn, log_send_queue_size, redo_queue_size, log_send_rate, redo_rate, is_suspended, suspend_reason_desc, availability_mode_desc, secondary_lag_seconds)
SELECT $1 + n, $2, $3, $4, $5, 'ExampleDatabase_' || lpad(n::text, 4, '0'), $6, false, 'SYNCHRONIZED', '0x0003', '0x0004', 10, 20, 100, 200, false, NULL, 'SYNCHRONOUS_COMMIT', 0
FROM generate_series(1, $7) AS n",
                            CollectionIdGenerator.Next() * 1000L, when, serverId, serverName, groups[g], replica, DatabasesPerView);
                    }
                }
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var web = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, responseByteBudget: null, cancellationToken: ct);
            var webBytes = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(web, DarlingAgReader.JsonOptions));
            Assert.True(webBytes > McpResponseBudget.DefaultBytes,
                $"the seed must make a 32 KB cut drop views; the full response measured {webBytes} bytes");
            Assert.Equal(4, web.AvailabilityGroups.Count);
            Assert.False(web.GroupsTruncated);

            var mcp = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, cancellationToken: ct);
            Assert.True(mcp.GroupsTruncated);
            Assert.False(string.IsNullOrEmpty(mcp.GroupsTruncatedNote));
            Assert.True(mcp.AvailabilityGroups.Count < 4);
            Assert.Equal(2, mcp.AvailabilityGroups.Select(v => v.GroupId).Distinct().Count());
            Assert.Equal(4, mcp.GroupsTotal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public void WebEndpoint_PassesNoByteBudget_AndTheMcpToolDoesNot()
    {
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs").ReplaceLineEndings("\n");
        var start = web.IndexOf("app.MapGet(\"/api/ag\"", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var call = web.Substring(start, 400);
        Assert.Contains("responseByteBudget: null", call, StringComparison.Ordinal);

        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAgTools.cs").ReplaceLineEndings("\n");
        Assert.DoesNotContain("responseByteBudget", tool, StringComparison.Ordinal);
    }

    [Fact]
    public void AgPage_ReadsGroupsTruncated_AndRendersTheNote()
    {
        var js = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "ag.js").ReplaceLineEndings("\n");
        Assert.Contains("d.groups_truncated", js, StringComparison.Ordinal);
        Assert.Contains("d.groups_truncated_note", js, StringComparison.Ordinal);
        Assert.Contains("truncationNote(d)", js, StringComparison.Ordinal);
    }
}
