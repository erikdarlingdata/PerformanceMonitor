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
/// #7573: Waiting Tasks showed the monitor's own waits on a quiet server. The collector now stores the waiting task's session program name
/// (V243, a nullable column). The read leaves out a row whose program name starts with the monitor's prefix, keeps a user's application that
/// only contains the words and a row with no program name (every row stored before the column existed), and brings them back when asked
/// (<c>include_monitor</c>). Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class WaitingTasksMonitorStatementsLiveTests
{
    private const string ServerName = "darling-waiting-tasks-monitor-statements-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live waiting-task monitor-statement test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One waiting task per program name, each in its own session, all in one capture a minute back.</summary>
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
                @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name, program_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-1)), ServerId, ServerName, 60 + i, "PAGEIOLATCH_SH", 500L + i, 0, null, "AppDb", (object?)apps[i] ?? DBNull.Value);
        }

        return end;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.waiting_tasks WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static int[] SessionsOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("tasks").EnumerateArray()
            .Select(r => r.GetProperty("session_id").GetInt32())
            .OrderBy(s => s)
            .ToArray();

    /* Sessions 60 to 65 carry, in order: three monitor names, a similar user name, an ordinary application and a NULL. The old read had no program
       predicate and returned all six rows, so the default assertions fail on it. */
    [Fact]
    public async Task TheMonitorsOwnWaits_AreLeftOutByDefault_AndKeptWhenAsked_AUsersSimilarNameAndANullStay()
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

            var rows = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, ct);
            Assert.Equal(new[] { 63, 64, 65 }, rows.Select(r => r.SessionId).OrderBy(s => s).ToArray());

            var all = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, ct, includeMonitor: true);
            Assert.Equal(6, all.Count);

            Assert.Equal(new[] { 63, 64, 65 }, SessionsOf(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 24, cancellationToken: ct)));
            Assert.Equal(new[] { 60, 61, 62, 63, 64, 65 }, SessionsOf(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 24, include_monitor: true, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* A quiet server whose only waits are the monitor's answers empty, and the text says they were left out. */
    [Fact]
    public async Task OnlyMonitorWaits_AnswerEmpty_AndTheTextSaysTheyAreLeftOut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, new string?[] { "PerformanceMonitorDarling" }, ct);

            using var def = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 24, cancellationToken: ct));
            Assert.Equal("empty", def.RootElement.GetProperty("status").GetString());
            Assert.Contains("monitor's own statements are left out", def.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            Assert.Single(SessionsOf(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 24, include_monitor: true, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* The collector's payload column is the table's last column, so the positional COPY lands the program name in program_name. A row written
       the way the collector writes it (WritePayload's order) reads back its program name. */
    [Fact]
    public async Task TheCollectorsPayloadOrder_PutsTheProgramNameInTheProgramNameColumn()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = new NpgsqlConnection(cs);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteRowsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var payload = WaitingTasksCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
            Assert.Equal("program_name", payload[^1]);

            var columns = string.Join(", ", new[] { "collection_id", "collection_time", "server_id", "server_name" }.Concat(payload));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                $"INSERT INTO collect.waiting_tasks ({columns}) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow), ServerId, ServerName, 70, "LCK_M_S", 100L, 71, null, "AppDb", "Orders.Api");

            using var read = new NpgsqlCommand($"SELECT program_name FROM collect.waiting_tasks WHERE server_id = {ServerId}", connection);
            Assert.Equal("Orders.Api", (string?)await read.ExecuteScalarAsync(ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
