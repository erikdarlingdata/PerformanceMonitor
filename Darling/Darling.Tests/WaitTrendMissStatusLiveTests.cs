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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6446: <c>get_wait_trend</c> on a wait type with no rows in the window, against a real store. The status word says
/// whether the gap is permanent, so it follows the server's own rules: <c>empty</c> when the collector would store the
/// wait type and nothing waited, <c>not_collected</c> only for a wait type the collector never stores (the ignored
/// list), <c>unavailable</c> when no wait stats exist at all. Lite's twin is <c>WaitTrendMissStatusTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class WaitTrendMissStatusLiveTests
{
    private const string ServerName = "wait-trend-miss-status-e2e";
    private const int ServerId = -486446;
    private const string CollectedWait = "PAGEIOLATCH_SH";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public Task AStoredWaitTypeWithNoRowsInTheWindow_IsEmpty_AndKeepsTheTypoHint() => RunAsync(async (connection, postgres, ct) =>
    {
        await InsertWaitStatAsync(connection, ct, CollectedWait);
        Assert.DoesNotContain("CXPACKET", IgnoredWaitDefaults.All);

        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetWaitTrend(postgres, "CXPACKET", ServerName, cancellationToken: ct));

        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        var hint = doc.RootElement.GetProperty("hints").GetProperty("collected_wait_types");
        Assert.Contains(CollectedWait, hint.EnumerateArray().Select(e => e.GetString()));
    });

    [Fact]
    public Task AnIgnoredWaitType_IsNotCollected_AndSaysTheCollectorNeverStoresIt() => RunAsync(async (connection, postgres, ct) =>
    {
        await InsertWaitStatAsync(connection, ct, CollectedWait);
        Assert.Contains("SLEEP_TASK", IgnoredWaitDefaults.All);

        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetWaitTrend(postgres, "SLEEP_TASK", ServerName, cancellationToken: ct));

        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("never stores", doc.RootElement.GetProperty("message").GetString());
    });

    [Fact]
    public Task NoWaitStatsAtAll_IsUnavailable() => RunAsync(async (connection, postgres, ct) =>
    {
        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetWaitTrend(postgres, "CXPACKET", ServerName, cancellationToken: ct));

        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
    });

    private static Task InsertWaitStatAsync(NpgsqlConnection connection, CancellationToken ct, string waitType) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 50, 1000, 0, 60)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-30)), ServerId, ServerName, waitType);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.servers WHERE server_id = $1", ServerId);
    }

    private static Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE",
            ServerId, ServerName, DarlingMcpTestData.Naive(DateTime.UtcNow));

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live wait-trend status tests.");

        var ct = TestContext.Current.CancellationToken;

        await using (var migration = new NpgsqlConnection(cs))
        {
            await migration.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migration, ct);
        }

        var pinned = new NpgsqlConnectionStringBuilder(cs) { SearchPath = "collect,config" }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(pinned);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await DeleteRowsAsync(connection, ct);
        await RegisterServerAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
