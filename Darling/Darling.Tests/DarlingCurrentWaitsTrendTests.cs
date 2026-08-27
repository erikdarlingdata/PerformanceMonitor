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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_current_waits_trend (#2484): the viewer's Current Waits tab, which had no endpoint on either
/// surface. get_waiting_tasks returns the snapshot rows and can never answer "was this worse an hour ago",
/// which is the question that decides whether anything is actually wrong.
///
/// <para>The assertion that carries the most weight is the empty one, and for a sharper reason than the
/// collection log's. Here the WRONG answer is the reassuring one: "nothing was waiting" reads as an
/// all-clear and a caller who believes it stops looking. If the collector has never sampled the server
/// there is nothing to be clear about, and the read has to say so.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCurrentWaitsTrendTests
{
    private const int ServerId = -949553;
    private const string ServerName = "current-waits-trend";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnAllClear_AndNeverSampled_AreDifferentAnswers_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live current-waits trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── never sampled: NOT an all-clear, and it must refuse to read as one ── */
            var never = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, null);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("NOT an all-clear", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /* ── sampled, but everything outside the window: a genuine all-clear ── */
            await SeedWaitAsync(connection, ct, HoursAgo(48), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");

            var clear = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 1, null);
            var clearDoc = JsonDocument.Parse(clear);
            Assert.Equal("empty", clearDoc.RootElement.GetProperty("status").GetString());
            var clearText = clearDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("genuine all-clear", clearText, StringComparison.Ordinal);

            /* Same zero rows as the case above, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);

            /* ── both series, in one payload ── */
            await SeedWaitAsync(connection, ct, MinutesAgo(10), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
            await SeedWaitAsync(connection, ct, MinutesAgo(10), "PAGEIOLATCH_SH", 250, blockingSessionId: 0, database: "AppDb");

            var hit = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, null);
            var root = JsonDocument.Parse(hit).RootElement;

            var waits = root.GetProperty("waiting_tasks");
            Assert.Equal(2, waits.GetArrayLength());

            /*
                Only the LCK row is blocked (blocking_session_id > 0). The PAGEIOLATCH row waits on IO and
                blocks on nothing, so it belongs to the wait series and NOT the blocked series -- which is
                the whole reason the two are read together rather than treated as one number.
            */
            var blocked = root.GetProperty("blocked_sessions");
            Assert.Equal(1, blocked.GetArrayLength());
            Assert.Equal("AppDb", blocked[0].GetProperty("database_name").GetString());
            Assert.Equal(1, blocked[0].GetProperty("blocked_count").GetInt64());

            /* ── the database filter narrows the blocked series and leaves the waits alone ── */
            await SeedWaitAsync(connection, ct, MinutesAgo(9), "LCK_M_S", 700, blockingSessionId: 77, database: "OtherDb");

            var filtered = await DarlingMcpDataTools.GetCurrentWaitsTrend(dataSource, ServerName, 4, "OtherDb");
            var filteredRoot = JsonDocument.Parse(filtered).RootElement;
            var filteredBlocked = filteredRoot.GetProperty("blocked_sessions");
            Assert.Equal(1, filteredBlocked.GetArrayLength());
            Assert.Equal("OtherDb", filteredBlocked[0].GetProperty("database_name").GetString());

            /* The wait series is server-wide and is deliberately NOT narrowed by the database filter. */
            Assert.Equal(3, filteredRoot.GetProperty("waiting_tasks").GetArrayLength());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string waitType, long waitMs, int blockingSessionId, string database) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, 55, waitType, waitMs, blockingSessionId, null, database);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM waiting_tasks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
