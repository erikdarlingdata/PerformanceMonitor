/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5642, ruling 6: the wraparound and replication-slot alert reads return their latest row's collection time as a
/// last column, as a UTC instant, so the alert's age line counts from the evidence it judged. The vacuum-horizon read
/// is covered in <see cref="XminHorizonPersistenceReadTests"/> and Long-Running Query in
/// <see cref="PgLongRunningQueryExclusionTests"/>. <c>DARLING_TEST_PG</c>-gated: runs in CI, skips without a store.
/// </summary>
[Collection("live-postgres")]
public sealed class PgTier0NewestSampleLiveTests
{
    private const int ServerId = -564201;
    private const string ServerName = "tier0-newest-sample-read";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheWraparoundAndSlotReads_ReturnTheLatestRowsCollectionTime_AsUtc_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Tier 0 newest-sample test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var adapter = new DarlingPostgresAlertReadAdapter(postgres);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* Two readings of one database and two of one slot: the read keeps the LATEST row of each, and the
               instant it returns is that row's collection time, not the older one's. */
            await SeedWraparoundAsync(connection, ct, now.AddMinutes(-20), "appdb", 100_000_000);
            await SeedWraparoundAsync(connection, ct, now.AddMinutes(-3), "appdb", 150_000_000);
            await SeedSlotAsync(connection, ct, now.AddMinutes(-20), "slot_a", 1_000);
            await SeedSlotAsync(connection, ct, now.AddMinutes(-2), "slot_a", 5_000);

            var wraparound = await adapter.GetWraparoundRiskAsync(ServerId, ct);
            var db = Assert.Single(wraparound);
            Assert.Equal(150_000_000L, db.XidAge);
            Assert.Equal(now.AddMinutes(-3), db.NewestSampleUtc);
            Assert.Equal(DateTimeKind.Utc, db.NewestSampleUtc!.Value.Kind);

            var slots = await adapter.GetReplicationSlotRiskAsync(ServerId, ct);
            var slot = Assert.Single(slots, s => s.SlotName == "slot_a");
            Assert.Equal(5_000L, slot.RetainedWalBytes);
            Assert.Equal(now.AddMinutes(-2), slot.NewestSampleUtc);
            Assert.Equal(DateTimeKind.Utc, slot.NewestSampleUtc!.Value.Kind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedWraparoundAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string database, long xidAge) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wraparound_stats
    (collection_id, collection_time, server_id, server_name, database_name, frozen_xid_age, min_multixid_age,
     autovacuum_freeze_max_age, autovacuum_multixact_freeze_max_age, pct_toward_emergency_vacuum, pct_toward_wraparound,
     pct_toward_multixact_emergency, pct_toward_multixact_wraparound, xids_remaining, multixids_remaining, allows_connections)
VALUES ($1, $2, $3, $4, $5, $6, 1000, 200000000, 400000000, 0, 0, 0, 0, $7, 2147482648, TRUE)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName, database, xidAge, 2_147_483_648L - xidAge);

    private static Task SeedSlotAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string slotName, long retained) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_replication_slot_stats
    (collection_id, collection_time, server_id, server_name, slot_name, slot_type, plugin, database_name, is_active, active_pid,
     is_temporary, two_phase, wal_status, safe_wal_size_bytes, retained_wal_bytes, xmin_age, catalog_xmin_age, inactive_since, invalidation_reason, conflicting)
VALUES ($1, $2, $3, $4, $5, 'logical', 'pgoutput', 'appdb', FALSE, NULL, FALSE, FALSE, 'reserved', -1, $6, NULL, NULL, NULL, NULL, FALSE)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName, slotName, retained);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM pg_wraparound_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM pg_replication_slot_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
