/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres. */

/// <summary>#5097: the bundle reads collection health with its error text whole, and the MCP tool's own output is unchanged. Names are synthetic.</summary>
public sealed class DiagnosticsBundleHealthUncutLiveTests
{
    private const int ServerId = 811;
    private const string ServerName = "alpha-sql-01";
    private const string Host = "sql01.contoso-corp.example.test";

    private static JsonObject Collector(string json, string collector)
    {
        var tree = JsonNode.Parse(json)!.AsObject();
        return tree["collectors"]!.AsArray().OfType<JsonObject>().Single(r => r["collector"]!.GetValue<string>() == collector);
    }

    [Fact]
    public async Task TheBundleRead_KeepsTheWholeErrorText_AndTheToolStillCutsAtFiveHundred()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live health test.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ok = false;
        try
        {
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var error = new string('x', 480) + " " + Host + " refused the login";
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await using (var server = new NpgsqlCommand(
                    "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 15, $3, $3)", connection))
                {
                    server.Parameters.AddWithValue(ServerId);
                    server.Parameters.AddWithValue(ServerName);
                    server.Parameters.AddWithValue(now);
                    await server.ExecuteNonQueryAsync(ct);
                }

                for (var i = 0; i < 4; i++)
                {
                    await using var run = new NpgsqlCommand(
                        @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, $2, $3, 'wait_stats', $4, 100, 'ERROR', $5, 0)", connection);
                    run.Parameters.AddWithValue(CollectionIdGenerator.Next());
                    run.Parameters.AddWithValue(ServerId);
                    run.Parameters.AddWithValue(ServerName);
                    run.Parameters.AddWithValue(now.AddMinutes(-10 - i));
                    run.Parameters.AddWithValue(error);
                    await run.ExecuteNonQueryAsync(ct);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var tool = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct);
            var uncut = await DarlingMcpDataTools.GetCollectionHealthUncut(postgres, ServerName, ct);

            var toolRow = Collector(tool, "wait_stats");
            var uncutRow = Collector(uncut, "wait_stats");

            /* The tool: cut to 500 characters and marked. */
            Assert.True(toolRow["last_error_truncated"]!.GetValue<bool>());
            Assert.EndsWith("... (truncated)", toolRow["last_error"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.DoesNotContain(Host, toolRow["last_error"]!.GetValue<string>(), StringComparison.Ordinal);

            /* The bundle read: the whole text, so a name that straddles character 500 is whole for the alias pass. */
            Assert.Equal(error, uncutRow["last_error"]!.GetValue<string>());
            Assert.False(uncutRow["last_error_truncated"]!.GetValue<bool>());

            /* Everything else (the finding is a sentence built from the error text, so it moves with it) is the tool's output, row for row. */
            foreach (var row in new[] { toolRow, uncutRow })
            {
                row.Remove("last_error");
                row.Remove("last_error_truncated");
                row.Remove("output_finding");
                row.Remove("output_finding_truncated");
            }

            Assert.Equal(toolRow.ToJsonString(), uncutRow.ToJsonString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
