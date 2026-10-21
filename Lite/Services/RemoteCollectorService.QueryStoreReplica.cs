/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #6254, #6279: the Lite host's half of Query Store reads from a readable replica. The rules (routing, cutoff, ship
/// guard, capturing-secondary tracker) are the shared layer's; this file holds the per-server state the definition
/// runner keeps between runs and the replica connection.
/// </summary>
public partial class RemoteCollectorService
{
    /// <summary>Supplies the REPLICA connection's readers (enumeration, version probe, redo, item reads) in tests. Null in production.</summary>
    internal Func<CollectorQuery, DbDataReader>? ReplicaReaderOverrideForTests { get; set; }

    /// <summary>How many replica connections the runner opened (a test supplying <see cref="ReplicaReaderOverrideForTests"/> opens none, but is counted).</summary>
    internal int QueryStoreReplicaConnectionsOpened => Volatile.Read(ref _queryStoreReplicaConnectionsOpened);

    private int _queryStoreReplicaConnectionsOpened;

    /// <summary>One server's Query Store look-back state: the ship guard, the capturing-secondary tracker, and whether the tracker was seeded from the store.</summary>
    internal sealed class QueryStoreServerState
    {
        public QueryStoreShipGuard Guard { get; } = new();
        public QueryStoreCapturingSecondaryTracker Tracker { get; } = new();
        public int Seeded;
    }

    private readonly ConcurrentDictionary<int, QueryStoreServerState> _queryStoreServerStates = new();

    internal QueryStoreServerState QueryStoreStateFor(int serverId) =>
        _queryStoreServerStates.GetOrAdd(serverId, static _ => new QueryStoreServerState());

    /// <summary>
    /// Seeds the capturing-secondary tracker once per process per server, from the store: each database with a non-primary
    /// <c>replica_role</c> row in the last 24 hours, with its newest <c>collection_time</c>. Read through the view that
    /// unions the archive, as every other read of <c>query_store_stats</c> is. A failed read leaves the tracker to learn
    /// from the first reads instead; it never blocks the run.
    /// </summary>
    private async Task SeedQueryStoreTrackerAsync(int serverId, QueryStoreServerState state, DateTime nowUtc, CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref state.Seeded, 1) == 1)
        {
            return;
        }

        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = @"
                SELECT database_name, MAX(collection_time)
                FROM v_query_store_stats
                WHERE server_id = $1
                AND   collection_time > $2
                AND   replica_role IS NOT NULL
                AND   lower(replica_role) <> 'primary'
                GROUP BY database_name";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = nowUtc - QueryStoreCapturingSecondaryTracker.Lapse });

            var seeds = new List<(string, DateTime)>();
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                seeds.Add((reader.GetString(0), reader.GetDateTime(1)));
            }

            state.Tracker.Seed(seeds);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug("Query Store capturing-secondary seed for server {ServerId} failed; learning from reads instead: {Error}", serverId, ex.Message);
        }
    }

    /// <summary>One database's read, held from the read until its write succeeds (the ship guard commits then).</summary>
    private sealed record QueryStoreItemRead(QueryStoreRouteDecision Decision, DateTime? NewWatermark, bool Cut);

    /// <summary>
    /// Opens the replica connection for one run: the server's own login at <see cref="ServerConnection.QueryStoreReplica"/>
    /// with <c>ApplicationIntent=ReadOnly</c>. Null when it cannot be opened: the caller then reads every database from the
    /// primary this run (<c>replica_unavailable</c>). A test supplying <see cref="ReplicaReaderOverrideForTests"/> gets an
    /// unopened stand-in, and is counted.
    /// </summary>
    private async Task<SqlConnection?> TryOpenQueryStoreReplicaAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _queryStoreReplicaConnectionsOpened);
        if (ReplicaReaderOverrideForTests is not null)
        {
            return new SqlConnection();
        }

        try
        {
            return await CreateConnectionAsync(server, cancellationToken, replica: true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug("Query Store replica connection for '{Server}' failed: {Error}", server.DisplayName, ex.Message);
            return null;
        }
    }
}
