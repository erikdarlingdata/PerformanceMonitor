/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// #5558: resolves <see cref="AnalysisContext.SecondaryReplicaDatabases"/> from the store's own
/// <c>ag_replica_states</c> / <c>ag_database_replica_states</c> snapshots, at or before the pass's window end
/// (so an AsOf pass and each compare_analysis window use the role at THAT time, not the current one). The decision
/// is <see cref="AgReplicaScope"/>'s; this class only reads the rows. Every failure is "skip nothing".
/// </summary>
internal static class SecondaryReplicaScope
{
    /// <summary>The set a pass that reads only node-local facts carries: nothing is skipped, and no AG read is paid for.</summary>
    internal static IReadOnlySet<string> NoneSkipped { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>Fills the set once per context. Never throws except an abandonment (#2443): a read fault logs and
    /// leaves the pass unfiltered.</summary>
    internal static async Task EnsureAsync(DuckDbInitializer duckDb, AnalysisContext context)
    {
        if (context.SecondaryReplicaDatabases is not null) return;
        context.SecondaryReplicaDatabases = await ReadAsync(duckDb, context.ServerId, context.TimeRangeEnd, context.CancellationToken);
    }

    /// <summary>The secondary databases as of <paramref name="asOfUtc"/>; empty on every failure or unknown.</summary>
    internal static async Task<IReadOnlySet<string>> ReadAsync(
        DuckDbInitializer duckDb, int serverId, DateTime asOfUtc, CancellationToken cancellationToken)
    {
        try
        {
            using var readLock = duckDb.AcquireReadLock(cancellationToken);
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            var replicaTime = await NewestAsync(connection, "ag_replica_states", serverId, asOfUtc, cancellationToken);
            var databaseTime = await NewestAsync(connection, "ag_database_replica_states", serverId, asOfUtc, cancellationToken);
            if (!AgReplicaScope.IsFresh(replicaTime, asOfUtc) || !AgReplicaScope.IsFresh(databaseTime, asOfUtc))
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var replicas = new List<AgReplicaReading>();
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
SELECT ag_name, replica_server_name, role_desc, is_local
FROM ag_replica_states
WHERE server_id = $1
AND   collection_time = $2";
                cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
                cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(replicaTime!.Value, DateTimeKind.Unspecified) });
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
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
SELECT ag_name, database_name, is_local
FROM ag_database_replica_states
WHERE server_id = $1
AND   collection_time = $2";
                cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
                cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(databaseTime!.Value, DateTimeKind.Unspecified) });
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, cancellationToken))
        {
            AppLogger.Warn("SecondaryReplicaScope",
                $"Availability Group role lookup failed for server {serverId}; no database is skipped this pass: {ex.Message}");
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
    }

    private static async Task<DateTime?> NewestAsync(
        DuckDBConnection connection, string table, int serverId, DateTime asOfUtc, CancellationToken cancellationToken)
    {
        using var cmd = connection.CreateCommand();
        /* The table name is a constant of this class, never input. */
        cmd.CommandText = "SELECT MAX(collection_time) FROM " + table + " WHERE server_id = $1 AND collection_time <= $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(asOfUtc, DateTimeKind.Unspecified) });
        var value = await cmd.ExecuteScalarAsync(cancellationToken);
        return value is DateTime at ? DateTime.SpecifyKind(at, DateTimeKind.Utc) : null;
    }

    /// <summary>The marker a replicated read puts in its SQL where the secondary-database predicate goes.</summary>
    internal const string Marker = "/*SEC*/";

    /// <summary>Replaces <see cref="Marker"/> in <paramref name="sql"/> with the predicate that drops the skipped
    /// databases of <paramref name="factKey"/> (<see cref="FactReplicaScope"/>) and adds the parameters, numbered from
    /// <paramref name="firstParameter"/>. With nothing to skip the marker just disappears, so the SQL is the same as
    /// before and no parameter is added.</summary>
    internal static string Apply(string sql, System.Data.Common.DbCommand cmd, AnalysisContext context, string factKey,
        string column, int firstParameter)
    {
        var names = FactReplicaScope.SecondariesFor(context, factKey);
        foreach (var name in names) cmd.Parameters.Add(new DuckDBParameter { Value = name });
        return sql.Replace(Marker, FactReplicaScope.PositionalFilter(names, column, firstParameter));
    }

    /// <summary>The note for the tabs (Recommendations, FinOps): the shared sentence when this node holds a secondary
    /// copy of any database as of <paramref name="asOfUtc"/> (now when null), else null. A tab reads the current role;
    /// an AsOf read of stored findings passes its anchor, so the note names the role at that time.</summary>
    internal static async Task<string?> NoteAsync(
        DuckDbInitializer duckDb, int serverId, DateTime? asOfUtc = null, CancellationToken cancellationToken = default)
    {
        var set = await ReadAsync(duckDb, serverId, asOfUtc ?? DateTime.UtcNow, cancellationToken);
        return AgReplicaScope.SkippedNote(set);
    }
}
