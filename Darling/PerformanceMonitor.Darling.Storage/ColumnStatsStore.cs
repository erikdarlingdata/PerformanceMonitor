/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The column statistics cursor of one database: where its history is kept and when it was last seen and fully read (#8798).</summary>
public sealed record ColumnStatsStateRow(
    int ServerId,
    string Database,
    int? HistoryServerId,
    Guid? HistoryAgGroupId,
    Guid? RecoveryForkGuid,
    DateTime? LastSeenInScopeAt,
    DateTime? LastFullReadAt);

/// <summary>
/// Column selectivity (#8798): the reads and writes of <c>collect.column_stats</c> (the current density vector rows, one per statistic
/// and leading-column prefix) and <c>collect.column_stats_state</c> (the per-database cursor). Only names, densities, row and sample
/// counts and update times are stored: the tables have no column for a value, a histogram or a filter expression.
/// </summary>
internal static class ColumnStatsStore
{
    private const int CommandTimeoutSeconds = SchemaVersionStore.WriteCommandTimeoutSeconds;

    /// <summary>When each stored statistic was last updated on the monitored server, by (object id, statistics id).</summary>
    internal static async Task<IReadOnlyDictionary<(int ObjectId, int StatsId), DateTime>> LoadStatsDatesAsync(
        NpgsqlConnection c, int serverId, string database, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using var cmd = new NpgsqlCommand(@"
SELECT object_id, stats_id, min(last_updated)
FROM collect.column_stats
WHERE server_id = $1 AND database_name = $2
GROUP BY object_id, stats_id", c) { CommandTimeout = CommandTimeoutSeconds };
        Param(cmd, NpgsqlDbType.Integer, serverId);
        Param(cmd, NpgsqlDbType.Text, database);

        var result = new Dictionary<(int ObjectId, int StatsId), DateTime>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[(reader.GetInt32(0), reader.GetInt32(1))] = reader.GetDateTime(2);
        }

        return result;
    }

    /// <summary>Replaces the stored rows of each statistic in the batch with its new prefixes: one transaction per batch, a delete then an insert per statistic.</summary>
    internal static async Task ReplaceStatisticsAsync(
        NpgsqlConnection c,
        int serverId,
        SchemaHistoryKey key,
        string database,
        IReadOnlyList<(StatCandidate Stat, StatHeader Header, IReadOnlyList<StatPrefix> Prefixes)> batch,
        DateTime readAt,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0)
        {
            return;
        }

        await using var tx = await c.BeginTransactionAsync(ct);
        foreach (var (stat, header, prefixes) in batch)
        {
            await using (var delete = new NpgsqlCommand(
                "DELETE FROM collect.column_stats WHERE server_id = $1 AND database_name = $2 AND object_id = $3 AND stats_id = $4", c, tx)
            { CommandTimeout = CommandTimeoutSeconds })
            {
                Param(delete, NpgsqlDbType.Integer, serverId);
                Param(delete, NpgsqlDbType.Text, database);
                Param(delete, NpgsqlDbType.Integer, stat.ObjectId);
                Param(delete, NpgsqlDbType.Integer, stat.StatsId);
                await delete.ExecuteNonQueryAsync(ct);
            }

            foreach (var prefix in prefixes)
            {
                await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.column_stats (
    server_id, history_server_id, history_ag_group_id, database_name, object_id, schema_name, object_name, stats_id, stats_name, origin,
    prefix_length, columns, includes_clustering_key, all_density, rows, rows_sampled, last_updated, persisted_sample_percent, read_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)", c, tx)
                { CommandTimeout = CommandTimeoutSeconds };
                Param(insert, NpgsqlDbType.Integer, serverId);
                Param(insert, NpgsqlDbType.Integer, key.ServerId);
                Param(insert, NpgsqlDbType.Uuid, key.AgGroupId);
                Param(insert, NpgsqlDbType.Text, database);
                Param(insert, NpgsqlDbType.Integer, stat.ObjectId);
                Param(insert, NpgsqlDbType.Text, stat.SchemaName);
                Param(insert, NpgsqlDbType.Text, stat.ObjectName);
                Param(insert, NpgsqlDbType.Integer, stat.StatsId);
                Param(insert, NpgsqlDbType.Text, stat.StatsName);
                Param(insert, NpgsqlDbType.Text, stat.Origin);
                Param(insert, NpgsqlDbType.Smallint, (short)prefix.PrefixLength);
                Param(insert, NpgsqlDbType.Array | NpgsqlDbType.Text, prefix.Columns.ToArray());
                Param(insert, NpgsqlDbType.Boolean, prefix.IncludesClusteringKey);
                Param(insert, NpgsqlDbType.Double, prefix.AllDensity);
                Param(insert, NpgsqlDbType.Bigint, header.Rows);
                Param(insert, NpgsqlDbType.Bigint, header.RowsSampled);
                Param(insert, NpgsqlDbType.Timestamp, Naive(header.LastUpdated ?? stat.StatsDate));
                Param(insert, NpgsqlDbType.Real, header.PersistedSamplePercent);
                Param(insert, NpgsqlDbType.Timestamp, Naive(readAt));
                await insert.ExecuteNonQueryAsync(ct);
            }
        }

        await tx.CommitAsync(ct);
    }

    /// <summary>Deletes every stored row of the named statistics (object id, statistics id); returns how many rows went.</summary>
    internal static async Task<int> DeleteStatisticsAsync(
        NpgsqlConnection c, int serverId, string database, IReadOnlyCollection<(int ObjectId, int StatsId)> gone, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(gone);
        if (gone.Count == 0)
        {
            return 0;
        }

        await using var cmd = new NpgsqlCommand(@"
DELETE FROM collect.column_stats AS s
USING unnest($3::integer[], $4::integer[]) AS g (object_id, stats_id)
WHERE s.server_id = $1 AND s.database_name = $2 AND s.object_id = g.object_id AND s.stats_id = g.stats_id", c)
        { CommandTimeout = CommandTimeoutSeconds };
        Param(cmd, NpgsqlDbType.Integer, serverId);
        Param(cmd, NpgsqlDbType.Text, database);
        Param(cmd, NpgsqlDbType.Array | NpgsqlDbType.Integer, gone.Select(g => g.ObjectId).ToArray());
        Param(cmd, NpgsqlDbType.Array | NpgsqlDbType.Integer, gone.Select(g => g.StatsId).ToArray());
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Deletes every stored row of one database (it left scope or was dropped); returns how many rows went.</summary>
    internal static async Task<int> DeleteDatabaseAsync(NpgsqlConnection c, int serverId, string database, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using var cmd = new NpgsqlCommand("DELETE FROM collect.column_stats WHERE server_id = $1 AND database_name = $2", c)
        { CommandTimeout = CommandTimeoutSeconds };
        Param(cmd, NpgsqlDbType.Integer, serverId);
        Param(cmd, NpgsqlDbType.Text, database);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<ColumnStatsStateRow?> LoadStateAsync(NpgsqlConnection c, int serverId, string database, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        await using var cmd = new NpgsqlCommand(@"
SELECT history_server_id, history_ag_group_id, recovery_fork_guid, last_seen_in_scope_at, last_full_read_at
FROM collect.column_stats_state
WHERE server_id = $1 AND database_name = $2", c) { CommandTimeout = CommandTimeoutSeconds };
        Param(cmd, NpgsqlDbType.Integer, serverId);
        Param(cmd, NpgsqlDbType.Text, database);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return new ColumnStatsStateRow(
            serverId,
            database,
            reader.IsDBNull(0) ? null : reader.GetInt32(0),
            reader.IsDBNull(1) ? null : reader.GetGuid(1),
            reader.IsDBNull(2) ? null : reader.GetGuid(2),
            reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            reader.IsDBNull(4) ? null : reader.GetDateTime(4));
    }

    internal static async Task SaveStateAsync(NpgsqlConnection c, ColumnStatsStateRow s, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(s);
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.column_stats_state (
    server_id, database_name, history_server_id, history_ag_group_id, recovery_fork_guid, last_seen_in_scope_at, last_full_read_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (server_id, database_name) DO UPDATE SET
    history_server_id = EXCLUDED.history_server_id, history_ag_group_id = EXCLUDED.history_ag_group_id,
    recovery_fork_guid = EXCLUDED.recovery_fork_guid, last_seen_in_scope_at = EXCLUDED.last_seen_in_scope_at,
    last_full_read_at = EXCLUDED.last_full_read_at", c) { CommandTimeout = CommandTimeoutSeconds };
        Param(cmd, NpgsqlDbType.Integer, s.ServerId);
        Param(cmd, NpgsqlDbType.Text, s.Database);
        Param(cmd, NpgsqlDbType.Integer, s.HistoryServerId);
        Param(cmd, NpgsqlDbType.Uuid, s.HistoryAgGroupId);
        Param(cmd, NpgsqlDbType.Uuid, s.RecoveryForkGuid);
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.LastSeenInScopeAt));
        Param(cmd, NpgsqlDbType.Timestamp, NaiveOrNull(s.LastFullReadAt));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Every stored prefix of one table under any of the history keys (rows of an availability group can come from either of its servers).
    /// A key names an availability group, or a server when it has no group.
    /// </summary>
    internal static async Task<IReadOnlyList<StoredPrefix>> ReadForObjectAsync(
        NpgsqlConnection c, IReadOnlyList<SchemaHistoryKey> keys, string database, string schema, string name, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(keys);
        var groups = keys.Where(k => k.AgGroupId is not null).Select(k => k.AgGroupId!.Value).Distinct().ToArray();
        var servers = keys.Where(k => k.AgGroupId is null && k.ServerId is not null).Select(k => k.ServerId!.Value).Distinct().ToArray();
        if (groups.Length == 0 && servers.Length == 0)
        {
            return Array.Empty<StoredPrefix>();
        }

        await using var cmd = new NpgsqlCommand(@"
SELECT stats_name, origin, columns, includes_clustering_key, all_density, rows, rows_sampled, last_updated, persisted_sample_percent, read_at
FROM collect.column_stats
WHERE database_name = $1 AND schema_name = $2 AND object_name = $3
  AND (history_ag_group_id = ANY($4) OR (history_ag_group_id IS NULL AND history_server_id = ANY($5)))
ORDER BY stats_name, prefix_length", c) { CommandTimeout = CommandTimeoutSeconds };
        Param(cmd, NpgsqlDbType.Text, database);
        Param(cmd, NpgsqlDbType.Text, schema);
        Param(cmd, NpgsqlDbType.Text, name);
        Param(cmd, NpgsqlDbType.Array | NpgsqlDbType.Uuid, groups);
        Param(cmd, NpgsqlDbType.Array | NpgsqlDbType.Integer, servers);

        var result = new List<StoredPrefix>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new StoredPrefix(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetFieldValue<string[]>(2),
                reader.GetBoolean(3),
                reader.GetDouble(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetDateTime(7),
                reader.IsDBNull(8) ? null : reader.GetFloat(8),
                reader.GetDateTime(9)));
        }

        return result;
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static DateTime? NaiveOrNull(DateTime? value) => value is { } v ? Naive(v) : null;

    private static void Param(NpgsqlCommand cmd, NpgsqlDbType type, object? value)
    {
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = type, Value = value ?? DBNull.Value });
    }
}
