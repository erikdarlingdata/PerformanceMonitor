/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads replication slot state (<c>pg_replication_slots</c>) — current state plus whether retained WAL
/// is still growing.
/// </summary>
public static class DarlingPgSlotReader
{
    public sealed record PgSlotRow(
        string SlotName,
        DateTime MeasuredAt,
        string? SlotType,
        string? Plugin,
        string? DatabaseName,
        bool IsActive,
        string? WalStatus,
        long RetainedWalBytes,
        long SafeWalSizeBytes,
        long XminAge,
        long CatalogXminAge,
        DateTime? InactiveSince,
        string? InvalidationReason,
        bool Conflicting,
        long FirstRetainedWalBytes,
        DateTime FirstSeenAt);

    /// <summary>
    /// Latest state per slot, joined to that slot's earliest reading in the window.
    /// <para>The earliest reading is what makes retained WAL actionable. A slot holding 45 GB that has
    /// held 45 GB all window is a consumer that is behind but keeping pace; a slot that held 2 GB an hour
    /// ago and holds 45 GB now is a volume filling in front of you, and only the second one is an
    /// emergency. A single current figure cannot distinguish them.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgSlotsSql = """
        WITH latest AS (
            SELECT DISTINCT ON (slot_name)
                slot_name, collection_time, slot_type, plugin, database_name, is_active,
                wal_status, retained_wal_bytes, safe_wal_size_bytes, xmin_age, catalog_xmin_age,
                inactive_since, invalidation_reason, conflicting
            FROM collect.pg_replication_slot_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY slot_name, collection_time DESC
        ),
        earliest AS (
            SELECT DISTINCT ON (slot_name)
                slot_name,
                retained_wal_bytes AS first_retained_wal_bytes,
                collection_time AS first_seen_at
            FROM collect.pg_replication_slot_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY slot_name, collection_time ASC
        )
        SELECT
            l.slot_name,
            l.collection_time,
            l.slot_type,
            l.plugin,
            l.database_name,
            l.is_active,
            l.wal_status,
            l.retained_wal_bytes,
            l.safe_wal_size_bytes,
            l.xmin_age,
            l.catalog_xmin_age,
            l.inactive_since,
            l.invalidation_reason,
            l.conflicting,
            e.first_retained_wal_bytes,
            e.first_seen_at
        FROM latest AS l
        JOIN earliest AS e ON e.slot_name = l.slot_name
        ORDER BY l.retained_wal_bytes DESC
        """;

    public static async Task<List<PgSlotRow>> GetPgSlotsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgSlotRow>();
        await using var command = postgres.CreateCommand(PgSlotsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgSlotRow(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                !reader.IsDBNull(5) && reader.GetBoolean(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? -1 : reader.GetInt64(7),
                reader.IsDBNull(8) ? -1 : reader.GetInt64(8),
                reader.IsDBNull(9) ? -1 : reader.GetInt64(9),
                reader.IsDBNull(10) ? -1 : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                !reader.IsDBNull(13) && reader.GetBoolean(13),
                reader.IsDBNull(14) ? -1 : reader.GetInt64(14),
                reader.GetDateTime(15)));
        }

        return rows;
    }
}
