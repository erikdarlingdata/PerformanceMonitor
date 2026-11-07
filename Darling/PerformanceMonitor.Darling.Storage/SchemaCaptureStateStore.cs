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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7643: the capture cursor of <c>collect.schema_capture_state</c> outside a database's own write: the whole state of one server read
/// at the start of a pass, the server row stored at its end, the "seen in scope" stamp, and the clearing of a dropped session. The
/// database rows are written by <see cref="SchemaVersionStore.ApplyAsync"/>, in the transaction of the versions they describe.
/// </summary>
public static class SchemaCaptureStateStore
{
    /// <summary>Every state row of <paramref name="serverId"/>: the server row (empty database name) and one per database.</summary>
    public static async Task<IReadOnlyList<SchemaCaptureStateRow>> LoadAsync(NpgsqlConnection c, int serverId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using var cmd = new NpgsqlCommand(@"
SELECT database_name, session_create_time, last_event_sequence, last_event_time, total_events_processed, recovery_fork_guid,
       history_server_id, history_ag_group_id, was_primary, baseline_at, last_complete_read_at, last_seen_in_scope_at,
       last_reconcile_at, last_reconcile_reason
FROM collect.schema_capture_state
WHERE server_id = $1", c);
        Param(cmd, NpgsqlDbType.Integer, serverId);

        var rows = new List<SchemaCaptureStateRow>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new SchemaCaptureStateRow
            {
                ServerId = serverId,
                DatabaseName = reader.GetString(0),
                SessionCreateTime = Utc(reader, 1),
                LastEventSequence = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                LastEventTime = Utc(reader, 3),
                TotalEventsProcessed = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                RecoveryForkGuid = reader.IsDBNull(5) ? null : reader.GetGuid(5),
                HistoryServerId = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                HistoryAgGroupId = reader.IsDBNull(7) ? null : reader.GetGuid(7),
                WasPrimary = reader.IsDBNull(8) ? null : reader.GetBoolean(8),
                BaselineAt = Utc(reader, 9),
                LastCompleteReadAt = Utc(reader, 10),
                LastSeenInScopeAt = Utc(reader, 11),
                LastReconcileAt = Utc(reader, 12),
                LastReconcileReason = reader.IsDBNull(13) ? null : reader.GetString(13),
            });
        }

        return rows;
    }

    /// <summary>Stores one state row whole (the server row of a pass).</summary>
    public static Task SaveAsync(NpgsqlConnection c, SchemaCaptureStateRow row, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(row);
        return SchemaVersionStore.UpsertStateAsync(c, null, row, ct);
    }

    /// <summary>
    /// Stamps <paramref name="at"/> as the last time the pass saw each of <paramref name="databases"/> in scope, in one statement. A database
    /// with no state row is left alone: a first read stores its own.
    /// </summary>
    public static async Task StampSeenInScopeAsync(
        NpgsqlConnection c, int serverId, IReadOnlyCollection<string> databases, DateTime at, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(databases);
        if (databases.Count == 0)
        {
            return;
        }

        await using var cmd = new NpgsqlCommand(
            "UPDATE collect.schema_capture_state SET last_seen_in_scope_at = $1 WHERE server_id = $2 AND database_name = ANY($3)", c);
        Param(cmd, NpgsqlDbType.Timestamp, Naive(at));
        Param(cmd, NpgsqlDbType.Integer, serverId);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = new List<string>(databases) });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>True when the store shows the collector once held an event session for <paramref name="serverId"/> that was not dropped since.</summary>
    public static async Task<bool> HasSessionAsync(NpgsqlConnection c, int serverId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using var cmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM collect.schema_capture_state WHERE server_id = $1 AND session_create_time IS NOT NULL)", c);
        Param(cmd, NpgsqlDbType.Integer, serverId);
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// Forgets the session after it was dropped: its position is cleared on every row of the server, and the server row's "seen in scope" stamp
    /// moves to <paramref name="at"/>. Every database row's stamp is then older than the server's, so the first pass after the collector is
    /// switched on again reconciles each database for what changed while no session recorded it.
    /// </summary>
    public static async Task ClearSessionAsync(NpgsqlConnection c, int serverId, DateTime at, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using (var clear = new NpgsqlCommand(@"
UPDATE collect.schema_capture_state
SET session_create_time = NULL, last_event_sequence = NULL, last_event_time = NULL, total_events_processed = NULL
WHERE server_id = $1", c))
        {
            Param(clear, NpgsqlDbType.Integer, serverId);
            await clear.ExecuteNonQueryAsync(ct);
        }

        await using var stamp = new NpgsqlCommand(@"
INSERT INTO collect.schema_capture_state (server_id, database_name, last_seen_in_scope_at)
VALUES ($1, '', $2)
ON CONFLICT (server_id, database_name) DO UPDATE SET last_seen_in_scope_at = EXCLUDED.last_seen_in_scope_at", c);
        Param(stamp, NpgsqlDbType.Integer, serverId);
        Param(stamp, NpgsqlDbType.Timestamp, Naive(at));
        await stamp.ExecuteNonQueryAsync(ct);
    }

    private static DateTime? Utc(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static void Param(NpgsqlCommand cmd, NpgsqlDbType type, object? value)
    {
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = type, Value = value ?? DBNull.Value });
    }
}
