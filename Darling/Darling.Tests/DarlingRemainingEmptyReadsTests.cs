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
/// The two collected reads from #2485's list that answer with a bare array: get_wait_types
/// (<c>wait_types: []</c>) and get_memory_clerks (<c>clerks: []</c>).
///
/// <para>They need DIFFERENT fixes, which is the point of testing them together. get_wait_types is windowed,
/// so its two kinds of nothing are a quiet window and a server nothing was ever stored for, and a LIMIT 1
/// probe against the same source separates them. get_memory_clerks is not windowed at all — it returns every
/// clerk at <c>MAX(collection_time)</c> — so zero rows back is logically the same statement as zero rows in
/// the table and there is nothing for a probe to add. What that read owes the caller instead is the fact that
/// an empty clerk list is NEVER a quiet period: a live SQL Server always has memory clerks.</para>
///
/// <para>Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingRemainingEmptyReadsTests
{
    private const int ServerId = -949556;
    private const string ServerName = "remaining-empty-reads";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WaitTypes_SeparateAQuietWindowFromANeverCollectedServer_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live wait-types empty test.");

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

            /* ── nothing ever stored: widening the window would never help, and the message must not
                  suggest it ── */
            var never = JsonDocument.Parse(
                await DarlingMcpDataTools.GetWaitTypes(postgres, ServerName, 4)).RootElement;
            Assert.Equal("unavailable", never.GetProperty("status").GetString());
            var neverText = never.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);
            Assert.Contains("not an empty window", neverText, StringComparison.Ordinal);

            /* The second-cycle nuance is the one thing that makes this branch self-clearing on a freshly
               added server, and dropping it would send someone hunting a fault that does not exist. */
            Assert.Contains("SECOND collection cycle", neverText, StringComparison.Ordinal);
            Assert.DoesNotContain("widen hours_back", neverText, StringComparison.Ordinal);

            /* ── collected, but outside the window: the opposite next move ── */
            await SeedWaitAsync(connection, ct, HoursAgo(48), "CXPACKET", 5000L);

            var quiet = JsonDocument.Parse(
                await DarlingMcpDataTools.GetWaitTypes(postgres, ServerName, 1)).RootElement;
            Assert.Equal("empty", quiet.GetProperty("status").GetString());
            var quietText = quiet.GetProperty("message").GetString()!;
            Assert.Contains("widen hours_back", quietText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);

            /* ── inside the window: the list, envelope gone ── */
            await SeedWaitAsync(connection, ct, MinutesAgo(10), "PAGEIOLATCH_SH", 900L);

            var hit = JsonDocument.Parse(
                await DarlingMcpDataTools.GetWaitTypes(postgres, ServerName, 4)).RootElement;
            Assert.False(hit.TryGetProperty("status", out _));
            Assert.Equal(1, hit.GetProperty("wait_types").GetArrayLength());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// One branch, on purpose. The value being pinned is what the sentence REFUSES to imply: an empty clerk
    /// list is not a quiet period and not a window that wants widening, because a live SQL Server always has
    /// memory clerks.
    /// </summary>
    [Fact]
    public async Task MemoryClerks_EmptySnapshot_IsNeverDescribedAsAQuietPeriod_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-clerks empty test.");

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

            var empty = JsonDocument.Parse(
                await DarlingMcpDataTools.GetMemoryClerks(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", empty.GetProperty("status").GetString());
            var text = empty.GetProperty("message").GetString()!;
            Assert.Contains("never a quiet period", text, StringComparison.Ordinal);
            Assert.Contains("LATEST snapshot", text, StringComparison.Ordinal);

            /* No window, so no window to widen -- offering that would be advice that cannot work. */
            Assert.DoesNotContain("widen hours_back", text, StringComparison.Ordinal);

            await SeedClerkAsync(connection, ct, MinutesAgo(5), "MEMORYCLERK_SQLBUFFERPOOL", 30000m);

            var hit = JsonDocument.Parse(
                await DarlingMcpDataTools.GetMemoryClerks(postgres, ServerName)).RootElement;
            Assert.False(hit.TryGetProperty("status", out _));
            Assert.Equal(1, hit.GetProperty("clerks").GetArrayLength());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The probe must walk the relation the read walks, or it can report "collected" for rows the
    /// read cannot see.</summary>
    [Fact]
    public void WaitStatProbe_WalksTheSameRelationAsTheRead()
    {
        Assert.Contains("FROM v_wait_stats", DarlingDataReader.HasAnyWaitStatSql, StringComparison.Ordinal);
        Assert.Contains("FROM v_wait_stats", DarlingDataReader.DistinctWaitTypesSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", DarlingDataReader.HasAnyWaitStatSql, StringComparison.Ordinal);
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string waitType, long deltaWaitMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, waitType,
            10L, deltaWaitMs, 100L);

    private static async Task SeedClerkAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string clerkType, decimal memoryMb) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO memory_clerks
    (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, $4, $5, $6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, clerkType, memoryMb);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM memory_clerks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
