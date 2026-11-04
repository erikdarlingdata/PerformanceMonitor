/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7573: on a quiet server every Active Queries row was one of the monitor's own statements. The rows stay stored; the read leaves out a
/// row whose stored <c>program_name</c> starts with the monitor's prefix, keeps a user's application that only contains the words and a row
/// with no program name, and brings them back when asked (<c>include_monitor</c>), for the rows, the snapshot list the strip draws, and the
/// MCP tool. Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ActiveQueryMonitorStatementsLiveTests
{
    private const string ServerName = "darling-active-queries-monitor-statements-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live active-query monitor-statement test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One capture a minute for each program name, newest first, so the snapshot list holds one row per name.</summary>
    private static async Task<DateTime> SeedAsync(string cs, string?[] apps, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        for (var i = 0; i < apps.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-2 - i)), ServerId, ServerName, 60 + i, "AppDb", "00 00:00:05.000",
                "SELECT " + i, "running", 0, null, 0L, 1000L, 1000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", (object?)apps[i] ?? DBNull.Value, 1, 0);
        }

        return end;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.query_snapshots WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static string?[] AppsOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("queries").EnumerateArray()
            .Select(r => r.TryGetProperty("program_name", out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null)
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToArray();

    /* The user rows are a similar name (the words inside a longer name), an ordinary application, and a NULL program name. The old read had no
       program predicate and returned all six rows, so the default assertions fail on it. */
    [Fact]
    public async Task TheMonitorsOwnStatements_AreLeftOutByDefault_AndKeptWhenAsked_AUsersSimilarNameStays()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, new string?[]
            {
                "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
                "MyPerformanceMonitorTool", "Orders.Api", null,
            }, ct);
            var user = new string?[] { null, "MyPerformanceMonitorTool", "Orders.Api" };

            var rows = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct);
            Assert.Equal(user, rows.Rows.Select(r => r.ProgramName).OrderBy(a => a, StringComparer.Ordinal).ToArray());
            Assert.Equal(3, rows.PopulationCount);

            var all = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct, includeMonitor: true);
            Assert.Equal(6, all.Rows.Count);

            /* The strip: the snapshot list holds one capture per kept row, and all six when asked. */
            var list = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct);
            Assert.Equal(3, list.Snapshots.Count);
            var listAll = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct, includeMonitor: true);
            Assert.Equal(6, listAll.Snapshots.Count);

            var tool = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, cancellationToken: ct);
            Assert.Equal(user, AppsOf(tool));
            var toolAll = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, include_monitor: true, cancellationToken: ct);
            Assert.Equal(6, AppsOf(toolAll).Length);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* A quiet server whose only sessions are the monitor's answers empty, and the empty text says they were left out. */
    [Fact]
    public async Task OnlyMonitorRows_AnswerEmpty_AndTheTextSaysTheyAreLeftOut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, new string?[] { "PerformanceMonitorDarling" }, ct);

            using var def = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, cancellationToken: ct));
            Assert.Equal("empty", def.RootElement.GetProperty("status").GetString());
            Assert.Contains("monitor's own statements are left out", def.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            Assert.Single(AppsOf(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, include_monitor: true, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
