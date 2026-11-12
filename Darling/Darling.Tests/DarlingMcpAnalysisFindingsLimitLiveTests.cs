/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7901: Darling's <c>get_analysis_findings</c> validates <c>limit</c> the way every other paged tool does
/// (<see cref="McpHelpers.ValidateTop(int, string)"/>): zero, a negative and a value over the shared ceiling each
/// answer the <c>invalid</c> envelope naming <c>limit</c>, not a data payload with an empty page. A limit inside
/// the range still answers as before. Gated on DARLING_TEST_PG because the server resolves against the store
/// before any parameter is validated. Lite's twin is <c>McpAnalysisFindingsLimitTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpAnalysisFindingsLimitLiveTests
{
    private const string ServerName = "findings-limit-7901-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task Limit_OutsideTheSharedRange_IsRefusedByName_AndInsideItStillAnswers()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live get_analysis_findings limit test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var analysisService = new DarlingAnalysisService(postgres);

            foreach (var limit in new[] { 0, -1, McpHelpers.MaxTop + 1 })
            {
                var json = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, ServerName, 24, limit: limit, cancellationToken: ct);
                Assert.True(McpHelpers.IsRefusalEnvelope(json), $"limit {limit} answered a data payload instead of the invalid envelope: {json}");
                using var doc = JsonDocument.Parse(json);
                Assert.Equal("limit", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
            }

            foreach (var limit in new[] { 1, McpHelpers.MaxTop })
            {
                var json = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, ServerName, 24, limit: limit, cancellationToken: ct);
                Assert.False(McpHelpers.IsRefusalEnvelope(json), $"limit {limit} was refused: {json}");
                using var doc = JsonDocument.Parse(json);
                /* No finding is planted, so the window's answer is the shared empty envelope. */
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
