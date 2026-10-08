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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// #5558, Darling's half of Lite's <c>SecondaryReplicaScope</c>: resolves
/// <see cref="AnalysisContext.SecondaryReplicaDatabases"/> from the store's own <c>ag_replica_states</c> and
/// <c>ag_database_replica_states</c> snapshots at or before the pass's window end, and builds the predicate the
/// replicated facts add. The decision is <see cref="AgReplicaScope"/>'s; this class only reads rows. Every failure
/// is "skip nothing".
/// </summary>
public static class PgSecondaryReplicaScope
{
    /// <summary>The marker a replicated read puts in its SQL where the secondary-database predicate goes.</summary>
    public const string Marker = "/*SEC*/";

    /// <summary>The set a pass that reads only node-local facts carries: nothing is skipped, and no AG read is paid for.</summary>
    public static IReadOnlySet<string> NoneSkipped { get; } = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>Fills the set once per context. Never throws except an abandonment: a read fault logs and leaves the pass unfiltered.</summary>
    public static async Task EnsureAsync(NpgsqlDataSource postgres, AnalysisContext context, Microsoft.Extensions.Logging.ILogger? logger)
    {
        if (context.SecondaryReplicaDatabases is not null) return;
        context.SecondaryReplicaDatabases = await ReadAsync(postgres, context.ServerId, context.TimeRangeEnd, logger, context.CancellationToken);
    }

    /// <summary>The secondary databases as of <paramref name="asOfUtc"/>; empty on every failure or unknown.</summary>
    public static async Task<IReadOnlySet<string>> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DateTime asOfUtc,
        Microsoft.Extensions.Logging.ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            var asOf = DateTime.SpecifyKind(asOfUtc, DateTimeKind.Unspecified);

            var replicaTime = await NewestAsync(connection, "ag_replica_states", serverId, asOf, cancellationToken);
            var databaseTime = await NewestAsync(connection, "ag_database_replica_states", serverId, asOf, cancellationToken);
            if (!AgReplicaScope.IsFresh(replicaTime, asOfUtc) || !AgReplicaScope.IsFresh(databaseTime, asOfUtc))
                return new HashSet<string>(StringComparer.Ordinal);

            var replicas = new List<AgReplicaReading>();
            using (var cmd = new NpgsqlCommand(
                "SELECT ag_name, replica_server_name, role_desc, is_local FROM ag_replica_states WHERE server_id = $1 AND collection_time = $2", connection)
            { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(serverId);
                cmd.Parameters.AddWithValue(DateTime.SpecifyKind(replicaTime!.Value, DateTimeKind.Unspecified));
                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    if (reader.IsDBNull(0)) continue;
                    replicas.Add(new AgReplicaReading(
                        AgName: reader.GetString(0),
                        ReplicaServerName: reader.IsDBNull(1) ? "" : reader.GetString(1),
                        RoleDesc: reader.IsDBNull(2) ? null : reader.GetString(2),
                        ConnectedStateDesc: null,
                        IsLocal: reader.IsDBNull(3) ? null : reader.GetBoolean(3)));
                }
            }

            var databases = new List<AgDatabaseMembership>();
            using (var cmd = new NpgsqlCommand(
                "SELECT ag_name, database_name, is_local FROM ag_database_replica_states WHERE server_id = $1 AND collection_time = $2", connection)
            { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(serverId);
                cmd.Parameters.AddWithValue(DateTime.SpecifyKind(databaseTime!.Value, DateTimeKind.Unspecified));
                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    if (reader.IsDBNull(0) || reader.IsDBNull(1)) continue;
                    databases.Add(new AgDatabaseMembership(
                        reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetBoolean(2)));
                }
            }

            return AgReplicaScope.SecondaryDatabases(replicas, databases);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            logger?.LogWarning(ex, "Availability Group role lookup failed for server {ServerId}; no database is skipped this pass", serverId);
            return new HashSet<string>(StringComparer.Ordinal);
        }
    }

    private static async Task<DateTime?> NewestAsync(
        NpgsqlConnection connection, string table, int serverId, DateTime asOf, CancellationToken cancellationToken)
    {
        /* The table name is a constant of this class, never input. */
        /* The lower bound is the freshness limit: an older snapshot is stale and yields the empty set anyway
           (AgReplicaScope.IsFresh), so the bound changes no answer but lets the hypertable exclude old chunks. */
        using var cmd = new NpgsqlCommand(
            "SELECT MAX(collection_time) FROM " + table
            + " WHERE server_id = $1 AND collection_time <= $2 AND collection_time >= $3", connection)
        { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(asOf);
        cmd.Parameters.AddWithValue(asOf - AgReplicaScope.SnapshotFreshness);
        var value = await cmd.ExecuteScalarAsync(cancellationToken);
        return value is DateTime at ? DateTime.SpecifyKind(at, DateTimeKind.Utc) : null;
    }

    /// <summary>The note for the viewer, web and MCP, or null when nothing is skipped. The role is the current one,
    /// or the role at <paramref name="asOfUtc"/> for an AsOf read of stored findings.</summary>
    public static async Task<string?> NoteAsync(
        NpgsqlDataSource postgres, int serverId, Microsoft.Extensions.Logging.ILogger? logger, CancellationToken cancellationToken,
        DateTime? asOfUtc = null)
    {
        var set = await ReadAsync(postgres, serverId, asOfUtc ?? DateTime.UtcNow, logger, cancellationToken);
        return AgReplicaScope.SkippedNote(set);
    }

    /// <summary>
    /// Replaces <see cref="Marker"/> in the command's text with the predicate that drops the skipped databases of
    /// <paramref name="factKey"/> (<see cref="FactReplicaScope"/>), binding the names (exact casing) as one text[]
    /// parameter numbered after the parameters already added. <paramref name="keyword"/> is AND for a clause that
    /// extends a WHERE and WHERE for one that opens it. With nothing to skip the marker just disappears: the SQL is
    /// the same as before and no parameter is added. Call it after every other parameter is bound.
    /// </summary>
    public static void Apply(NpgsqlCommand cmd, AnalysisContext context, string factKey, string column, string keyword = "AND")
    {
        var names = FactReplicaScope.SecondariesFor(context, factKey);
        if (names.Length == 0)
        {
            cmd.CommandText = cmd.CommandText.Replace(Marker, string.Empty, StringComparison.Ordinal);
            return;
        }

        var index = cmd.Parameters.Count + 1;
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = names });
        cmd.CommandText = cmd.CommandText.Replace(
            Marker, $" {keyword} ({column} IS NULL OR {column} <> ALL(${index}::text[]))", StringComparison.Ordinal);
    }
}
