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

namespace PerformanceMonitor.Analysis;

/// <summary>Whether a fact's data is the same on every replica of an Availability Group, or belongs to this node.</summary>
public enum FactReplicaKind
{
    /// <summary>Real work or state of THIS node (a query that ran here, a wait, blocking, a disk). Kept on a
    /// secondary: it happened here and no other node reports it.</summary>
    NodeLocal,

    /// <summary>Data that replicates from the primary (database settings, Query Store content, the shape of the
    /// objects), so every node would report the same finding for the same database. Skipped for the databases
    /// this node holds only as a secondary copy (<see cref="AnalysisContext.SecondaryReplicaDatabases"/>).</summary>
    Replicated,
}

/// <summary>
/// The one table that decides which per-database facts are filtered on a secondary replica (#5558). A wrong call
/// is a one-line flip here; the collectors ask <see cref="SecondariesFor"/> and never hard-code a fact key.
/// A key that is in neither list, and not in a family, is <see cref="FactReplicaKind.NodeLocal"/>: an unknown fact
/// is kept, so the filter fails open. <c>FactReplicaScopeCoverageTests</c> fails when a fact key a collector emits
/// is missing from this table, so a new fact is a deliberate choice rather than a silent default.
/// </summary>
public static class FactReplicaScope
{
    /// <summary>The replicated facts. DB_CONFIG is database settings (sys.databases); FILE_AUTOGROWTH_PERCENT is the
    /// file growth setting (sys.master_files, and the ALTER it suggests cannot run on a secondary either);
    /// PLAN_REGRESSION is Query Store content, which a secondary shows as the primary's; ANOMALY_OBJECT_GROWTH is
    /// computed from reserved_mb, which mirrors the primary.</summary>
    private static readonly string[] ReplicatedKeys =
    [
        "DB_CONFIG",
        "FILE_AUTOGROWTH_PERCENT",
        "PLAN_REGRESSION",
        "ANOMALY_OBJECT_GROWTH",
    ];

    /// <summary>The facts that stay on every node: server-level state, and per-database work that ran on this node.</summary>
    private static readonly string[] NodeLocalKeys =
    [
        // Per-database, node-local: real workload on this node (a readable secondary can run reporting).
        "MISSING_INDEX", "PLAN_WARNING", "BAD_ACTOR", "PARAMETER_SENSITIVITY", "QUERY_SPILLS", "QUERY_HIGH_DOP",
        "PROCEDURE_STATS", "PLAN_CACHE_BLOAT", "IO_READ_LATENCY_MS", "IO_WRITE_LATENCY_MS",
        "DATABASE_TOTAL_SIZE_MB", "DISK_SPACE", "TEMPDB_USAGE", "BLOCKING_EVENTS", "BLOCKING_CHAIN", "DEADLOCKS",
        "ANOMALY_OBJECT_CONTENTION",
        // Server-level.
        "TRACE_FLAGS", "SERVER_EDITION", "SERVER_MAJOR_VERSION", "SESSION_STATS", "RUNNING_JOBS", "RUNNABLE_TASKS",
        "ACTIVE_QUERIES", "CPU_SQL_PERCENT", "CPU_SPIKE", "MEMORY_TOTAL_PHYSICAL_MB", "MEMORY_TARGET_MB",
        "MEMORY_PRESSURE_EVENTS", "MEMORY_GRANT_PENDING", "MEMORY_CLERKS", "MEMORY_BUFFER_POOL_MB",
        // Server-level anomalies.
        "ANOMALY_WRITE_LATENCY", "ANOMALY_WAIT_PROFILE", "ANOMALY_SESSION_SPIKE", "ANOMALY_READ_LATENCY",
        "ANOMALY_QUERY_DURATION", "ANOMALY_MEMORY_PRESSURE", "ANOMALY_DEADLOCK_SPIKE", "ANOMALY_CPU_SPIKE",
        "ANOMALY_BLOCKING_SPIKE", "ANOMALY_BATCH_REQUESTS",
    ];

    /// <summary>Families whose keys are built at run time: server settings (CONFIG_*, server level), one fact per
    /// bad-actor query (BAD_ACTOR_*, a query that ran on this node), and DB_CONFIG sub-keys (replicated).</summary>
    private static readonly (string Prefix, FactReplicaKind Kind)[] Families =
    [
        ("DB_CONFIG_", FactReplicaKind.Replicated),
        ("BAD_ACTOR_", FactReplicaKind.NodeLocal),
        ("CONFIG_", FactReplicaKind.NodeLocal),
    ];

    private static readonly Dictionary<string, FactReplicaKind> Table = BuildTable();

    private static Dictionary<string, FactReplicaKind> BuildTable()
    {
        var table = new Dictionary<string, FactReplicaKind>(StringComparer.Ordinal);
        foreach (var key in ReplicatedKeys) table[key] = FactReplicaKind.Replicated;
        foreach (var key in NodeLocalKeys) table[key] = FactReplicaKind.NodeLocal;
        return table;
    }

    /// <summary>Every literal key in the table, for the coverage test.</summary>
    public static IReadOnlyCollection<string> KnownKeys => Table.Keys;

    /// <summary>True when the key is in the table or one of its families. Wait-type facts (one key per wait type)
    /// are not in the table: they are all node-local and fall to the default.</summary>
    public static bool IsKnown(string key) =>
        Table.ContainsKey(key) || Families.Any(f => key.StartsWith(f.Prefix, StringComparison.Ordinal));

    /// <summary>The key's kind. Unknown keys are node-local: kept, never filtered.</summary>
    public static FactReplicaKind Of(string key)
    {
        if (Table.TryGetValue(key, out var kind)) return kind;
        foreach (var (prefix, familyKind) in Families)
            if (key.StartsWith(prefix, StringComparison.Ordinal)) return familyKind;
        return FactReplicaKind.NodeLocal;
    }

    /// <summary>The lower-cased database names a <paramref name="factKey"/> read must skip on this pass: the context's
    /// secondary set when the key is <see cref="FactReplicaKind.Replicated"/>, else empty. A read builds its
    /// predicate from a non-empty answer and leaves its SQL byte-identical on an empty one.</summary>
    public static string[] SecondariesFor(AnalysisContext context, string factKey)
    {
        if (context.SecondaryReplicaDatabases is not { Count: > 0 } set) return [];
        if (Of(factKey) != FactReplicaKind.Replicated) return [];
        return LowerNames(set);
    }

    /// <summary>The set's names lower-cased, de-duplicated and ordered, for a SQL <c>lower(database_name)</c> compare.</summary>
    public static string[] LowerNames(IEnumerable<string> names) =>
        names.Select(n => n.ToLowerInvariant()).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>A DuckDB predicate fragment (leading AND) that drops rows of the skipped databases, with positional
    /// parameters starting at <paramref name="firstParameter"/>; empty when there is nothing to skip. A NULL
    /// database passes. Add the values with <see cref="LowerNames"/> in the same order.</summary>
    public static string PositionalFilter(string[] lowerNames, string column, int firstParameter)
    {
        if (lowerNames.Length == 0) return string.Empty;
        var placeholders = string.Join(", ", lowerNames.Select((_, i) => "$" + (firstParameter + i)));
        return " AND (" + column + " IS NULL OR lower(" + column + ") NOT IN (" + placeholders + "))";
    }
}
