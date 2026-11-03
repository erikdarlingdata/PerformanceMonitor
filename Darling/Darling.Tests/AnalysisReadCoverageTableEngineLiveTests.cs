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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7194: get_analysis_findings and get_analysis_facts ask "was this window collected at all" of the engine's own
/// series, as the analysis pass does: <c>pg_database_stats</c> on a PostgreSQL target, <c>wait_stats</c> on a SQL Server
/// one. Before, both always asked <c>wait_stats</c>, so a quiet PostgreSQL window answered "window_truncated true" and
/// "collection of wait_stats may have stopped" about a table that engine never writes.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisReadCoverageTableEngineLiveTests
{
    private const string PgServerName = "darling-7194-pg-target";
    private const string SqlServerName = "darling-7194-sql-target";
    private static readonly int PgServerId = ServerIdHelper.GetDeterministicHashCode(PgServerName);
    private static readonly int SqlServerId = ServerIdHelper.GetDeterministicHashCode(SqlServerName);

    [Fact]
    public async Task PostgresTarget_WithPgDatabaseStatsAndNoWaitStats_FindingsWindowIsCovered()
    {
        await RunAsync(async (postgres, analysisService, ct) =>
        {
            var json = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, PgServerName, 24, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            var hints = doc.RootElement.GetProperty("hints");
            Assert.False(hints.GetProperty("window_truncated").GetBoolean(), json);
            Assert.DoesNotContain("wait_stats", json, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task PostgresTarget_WithNoCollectedRows_NamesPgDatabaseStatsNotWaitStats()
    {
        await RunAsync(async (postgres, analysisService, ct) =>
        {
            /* The server with no rows at all: the notice still has to name the table this engine writes. */
            await using var connection = await postgres.OpenConnectionAsync(ct);
            await DeleteStatsAsync(connection, ct);

            var findings = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, PgServerName, 24, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(findings))
            {
                var note = doc.RootElement.GetProperty("hints").GetProperty("truncation_note").GetString();
                Assert.Contains("pg_database_stats", note, StringComparison.Ordinal);
                Assert.DoesNotContain("wait_stats", note, StringComparison.Ordinal);
            }

            var facts = await DarlingMcpTools.GetAnalysisFacts(analysisService, postgres, PgServerName, 24, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(facts))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var note = doc.RootElement.GetProperty("hints").GetProperty("truncation_note").GetString();
                Assert.Contains("pg_database_stats", note, StringComparison.Ordinal);
                Assert.DoesNotContain("wait_stats", note, StringComparison.Ordinal);
            }
        });
    }

    [Fact]
    public async Task SqlServerTarget_KeepsWaitStats_ForFindingsAndFacts()
    {
        await RunAsync(async (postgres, analysisService, ct) =>
        {
            var findings = await DarlingMcpTools.GetAnalysisFindings(analysisService, postgres, SqlServerName, 24, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(findings))
            {
                var hints = doc.RootElement.GetProperty("hints");
                Assert.True(hints.GetProperty("window_truncated").GetBoolean(), findings);
                Assert.Contains("wait_stats", hints.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            }

            var facts = await DarlingMcpTools.GetAnalysisFacts(analysisService, postgres, SqlServerName, 24, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(facts))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                Assert.Contains("wait_stats", doc.RootElement.GetProperty("hints").GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            }
        });
    }

    [Theory]
    [InlineData(null, "wait_stats")]
    [InlineData("sqlserver", "wait_stats")]
    [InlineData("postgres", "pg_database_stats")]
    [InlineData("aurora-postgres", "pg_database_stats")]
    public void CoverageTableFor_FollowsTheEngineMapping(string? engineKind, string expected) =>
        Assert.Equal(expected, DarlingAnalysisService.CoverageTableFor(engineKind));

    private static async Task RunAsync(Func<NpgsqlDataSource, DarlingAnalysisService, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live coverage-table test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, PgServerId, PgServerName, "postgres", ct);
            await RegisterAsync(connection, SqlServerId, SqlServerName, null, ct);

            /* Hourly pg_database_stats samples across the last 23 hours: the window's first sample lands inside the
               90-minute slack of a 24-hour window's start, so a covered window reads as covered. No wait_stats row anywhere. */
            var now = DateTime.UtcNow;
            for (var i = 0; i <= 23; i++)
            {
                using var insert = new NpgsqlCommand(@"
INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit)
VALUES ($1, $2, $3, $4, 'postgres', 100, 1, 10, 1000)", connection);
                insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
                insert.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddHours(-23 + i), DateTimeKind.Unspecified));
                insert.Parameters.AddWithValue(PgServerId);
                insert.Parameters.AddWithValue(PgServerName);
                await insert.ExecuteNonQueryAsync(ct);
            }

            await body(postgres, new DarlingAnalysisService(postgres), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, int serverId, string serverName, string? engineKind, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date, engine_kind)
VALUES ($1, $2, $2, TRUE, $3, $3, $4)
ON CONFLICT (server_id) DO UPDATE SET engine_kind = EXCLUDED.engine_kind, is_enabled = TRUE", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(engineKind is null ? DBNull.Value : engineKind);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteStatsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM pg_database_stats WHERE server_id = {PgServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {PgServerId}; " +
            $"DELETE FROM servers WHERE server_id IN ({PgServerId}, {SqlServerId});",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
