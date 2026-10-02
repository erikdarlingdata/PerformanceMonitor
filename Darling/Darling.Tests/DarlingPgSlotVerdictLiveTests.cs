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
/// get_pg_replication_slots end to end against a real store — the #3535 verdict seam.
///
/// <para><b>The test that carries this class is the severity-versus-size pick.</b> The tool's own design
/// note says the size of the hole matters less than whether it is still being dug, and then worst_slot
/// was picked by retained bytes: an active 45 GB keeping-pace slot ("ok") headlined over an inactive
/// 2→8 GB grower ("critical_orphan_filling_disk") — the one slot the caller needed to see first. The
/// ordering and pick live inline in the tool, so only a real round-trip exercises them.</para>
///
/// <para><b>And the growth a sentinel cannot measure.</b> The collector's -1 not-applicable sentinel used
/// to become a measured zero — "no growth" — so an unmeasurable slot read as stable; a one-sample window
/// is the same fabrication from a single point. Both surface as null growth plus an unknown-growth
/// verdict, never as flat.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgSlotVerdictLiveTests
{
    private const int ServerId = -853918;
    private const string ServerName = "pg-slot-verdict-e2e";

    private const long Gb = 1024L * 1024 * 1024;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WorstSlotIsTheWorstClassified_AndUnmeasurableGrowthReadsUnknown()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live slot verdict test.");

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

            var t0 = MinutesAgo(30);
            var t1 = t0.AddMinutes(10);

            /* fat_ok: the biggest hole and the healthiest slot here — active, reserved, holding 45 GB
               steady. The size pick made this the headline (#3535's scenario, verbatim). */
            await SeedAsync(connection, ct, t0, "fat_ok", isActive: true, walStatus: "reserved", retainedWalBytes: 45 * Gb);
            await SeedAsync(connection, ct, t1, "fat_ok", isActive: true, walStatus: "reserved", retainedWalBytes: 45 * Gb);

            /* thin_orphan: a fraction of the size, inactive, extended, and GROWING — the disk bomb. */
            await SeedAsync(connection, ct, t0, "thin_orphan", isActive: false, walStatus: "extended", retainedWalBytes: 2 * Gb);
            await SeedAsync(connection, ct, t1, "thin_orphan", isActive: false, walStatus: "extended", retainedWalBytes: 8 * Gb);

            /* sentinel: retained bytes NULL in both readings (the -1 sentinel downstream) — growth is
               unknowable, and "stable" would be a fabricated claim. */
            await SeedAsync(connection, ct, t0, "sentinel", isActive: false, walStatus: "extended", retainedWalBytes: null);
            await SeedAsync(connection, ct, t1, "sentinel", isActive: false, walStatus: "extended", retainedWalBytes: null);

            /* single: one reading with a measured size — the size is real, the growth claim is not. */
            await SeedAsync(connection, ct, t1, "single", isActive: false, walStatus: "reserved", retainedWalBytes: 1 * Gb);

            var payload = JsonDocument.Parse(
                await DarlingMcpPgSlotTools.GetPgReplicationSlots(dataSource, ServerName, 4)).RootElement;

            Assert.Equal("slots_present", payload.GetProperty("status").GetString());

            /* THE assertion: the headline is the worst-CLASSIFIED slot, not the fattest, and the list
               leads with it. */
            Assert.Equal("thin_orphan", payload.GetProperty("worst_slot").GetString());
            Assert.Equal("critical_orphan_filling_disk", payload.GetProperty("worst_severity").GetString());

            var slots = payload.GetProperty("slots").EnumerateArray().ToArray();
            Assert.Equal("thin_orphan", slots[0].GetProperty("slot_name").GetString());

            /* The fat slot keeps its honest verdict and its figure — it just no longer buys the
               headline with it. */
            var fat = Row(slots, "fat_ok");
            Assert.Equal("ok", fat.GetProperty("severity").GetString());
            Assert.Equal(45.0, fat.GetProperty("retained_wal_gb").GetDouble(), 2);

            /* Sentinel: null size (raw AND _gb — the raw field used to serialize -1), null growth, and
               the unknown-growth verdict rather than the measured-flat one. */
            var sentinel = Row(slots, "sentinel");
            Assert.Equal("warning_retaining_wal_growth_unknown", sentinel.GetProperty("severity").GetString());
            Assert.Equal(JsonValueKind.Null, sentinel.GetProperty("retained_wal_bytes").ValueKind);
            Assert.Equal(JsonValueKind.Null, sentinel.GetProperty("retained_wal_gb").ValueKind);
            Assert.Equal(JsonValueKind.Null, sentinel.GetProperty("retained_wal_growth_bytes").ValueKind);
            Assert.Equal(JsonValueKind.Null, sentinel.GetProperty("retained_wal_growth_gb_per_hour").ValueKind);

            /* One sample: measured size, unmeasurable growth. */
            var single = Row(slots, "single");
            Assert.Equal("info_inactive_growth_unknown", single.GetProperty("severity").GetString());
            Assert.Equal(1.0, single.GetProperty("retained_wal_gb").GetDouble(), 2);
            Assert.Equal(JsonValueKind.Null, single.GetProperty("retained_wal_growth_bytes").ValueKind);

            /* The total sums only measured sizes — a sentinel never subtracts phantom gigabytes. */
            Assert.Equal(54.0, payload.GetProperty("total_retained_wal_gb").GetDouble(), 1);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The row for one slot, asserted present with a message naming what DID come back.</summary>
    private static JsonElement Row(JsonElement[] rows, string slotName)
    {
        var row = rows.FirstOrDefault(r => r.GetProperty("slot_name").GetString() == slotName);

        Assert.True(
            row.ValueKind == JsonValueKind.Object,
            $"no row for '{slotName}' — the read returned [{string.Join(", ", rows.Select(r => r.GetProperty("slot_name").GetString()))}]");

        return row;
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string slotName,
        bool isActive, string walStatus, long? retainedWalBytes) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.pg_replication_slot_stats
    (collection_id, collection_time, server_id, server_name, slot_name, slot_type, plugin,
     database_name, is_active, wal_status, retained_wal_bytes, conflicting)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, slotName, "logical", "pgoutput", "appdb", isActive, walStatus,
            retainedWalBytes, false);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.pg_replication_slot_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
