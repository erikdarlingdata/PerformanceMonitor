/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6254: what the runner keeps per server between runs for Query Store reads: the ship guard (the rows a look-back read
/// has already shipped) and the capturing-secondary tracker (#6279). In memory, like the open-interval stamps: a restart
/// empties the guard (one read writes its window again, healed within a flush) and re-seeds the tracker from the store.
/// </summary>
internal sealed class QueryStoreServerReplicaState
{
    public QueryStoreShipGuard Guard { get; } = new();

    public QueryStoreCapturingSecondaryTracker Tracker { get; } = new();

    /// <summary>True once the tracker was seeded from the store for this server.</summary>
    public bool Seeded { get; set; }
}

/// <summary>
/// #6254: one Query Store run's replica facts and routing decisions. Owns the replica connection when one was opened, and
/// hands the runner a decision per database from the shared <see cref="QueryStoreReplicaRouting"/>. It opens nothing itself.
/// </summary>
internal sealed class QueryStoreReplicaRun : IAsyncDisposable
{
    private readonly QueryStoreServerReplicaState _state;
    private readonly bool _isAzureSqlDb;
    private readonly Dictionary<string, QueryStoreRouteDecision> _decisions = new(StringComparer.Ordinal);

    public QueryStoreReplicaRun(QueryStoreServerReplicaState state, bool configured, bool isAzureSqlDb)
    {
        _state = state;
        Configured = configured;
        _isAzureSqlDb = isAzureSqlDb;
    }

    /// <summary>The server has a <c>query_store_replica</c> set (and is not Azure SQL Database).</summary>
    public bool Configured { get; }

    /// <summary>The replica connection, open, or null when the setting is empty or opening it failed.</summary>
    public DbConnection? Connection { get; set; }

    /// <summary>The replica connection or its enumeration failed this run: every database reads the primary.</summary>
    public bool ReplicaUnavailable { get; set; }

    public int PrimaryMajorVersion { get; set; }

    public int ReplicaMajorVersion { get; set; }

    public QueryStoreServerReplicaState State => _state;

    public Dictionary<string, QueryStorePrimaryDatabaseState> PrimaryFacts { get; } = new(StringComparer.Ordinal);

    public Dictionary<string, QueryStoreReplicaDatabaseState> ReplicaFacts { get; } = new(StringComparer.Ordinal);

    public Dictionary<string, QueryStoreAgRedoState> PrimaryRedo { get; } = new(StringComparer.Ordinal);

    public Dictionary<string, QueryStoreAgRedoState> ReplicaRedo { get; } = new(StringComparer.Ordinal);

    /// <summary>The newest decision made for each database this run, for <see cref="QueryStoreReplicaRouting.Summarize"/>.</summary>
    public IEnumerable<QueryStoreRouteDecision> Decisions => _decisions.Values;

    /// <summary>The decision made for the database this run, or null (a collector with none, or not decided yet).</summary>
    public QueryStoreRouteDecision? CurrentDecision(string database) => _decisions.GetValueOrDefault(database);

    public QueryStoreRoutingInput InputFor(string database, DateTime now) => new(
        database, Configured, _isAzureSqlDb, ReplicaUnavailable, PrimaryMajorVersion, ReplicaMajorVersion,
        PrimaryFacts.GetValueOrDefault(database), ReplicaFacts.GetValueOrDefault(database),
        PrimaryRedo.GetValueOrDefault(database), ReplicaRedo.GetValueOrDefault(database),
        _state.Tracker.HasCapturingSecondary(database, now));

    public QueryStoreRouteDecision Decide(string database, DateTime now)
        => _decisions[database] = QueryStoreReplicaRouting.Decide(InputFor(database, now));

    /// <summary>One database's replica read failed: it reads the primary this run, <c>replica_unavailable</c>.</summary>
    public QueryStoreRouteDecision FallBack(string database, DateTime now)
        => _decisions[database] = QueryStoreReplicaRouting.FallbackAfterReplicaReadFailure(InputFor(database, now));

    public async ValueTask DisposeAsync()
    {
        if (Connection is not null)
        {
            await Connection.DisposeAsync();
            Connection = null;
        }
    }
}
