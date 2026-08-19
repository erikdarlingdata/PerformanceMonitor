/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the composer performance-tuning statements (covering indexes + per-table autovacuum-insert override,
/// Erik's EXPLAIN-backed field fix) so the tested SQL can never silently drift. These are applied as idempotent
/// RUNTIME setup (<see cref="PgTableTuning"/>), NOT a versioned migration, so they do not bump StorageVersion or
/// gate the Viewer — the reason there is no schema-version change to pin here.
/// </summary>
public sealed class PgTableTuningTests
{
    [Fact]
    public void Statements_AreTheCoveringComposerIndexes_TheLookupIndexes_AndTheAutovacuumOverride()
    {
        var sql = string.Join("\n", PgTableTuning.Statements);

        /* Three COVERING composer indexes (INCLUDE the aggregate columns -> Index Only Scan), schema-qualified collect. */
        Assert.Contains("idx_procedure_stats_object_name ON collect.procedure_stats (object_name, collection_time) INCLUDE (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count)", sql, StringComparison.Ordinal);
        Assert.Contains("idx_query_stats_query_hash ON collect.query_stats (query_hash, collection_time) INCLUDE (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count)", sql, StringComparison.Ordinal);
        Assert.Contains("idx_query_store_stats_query_hash ON collect.query_store_stats (query_hash, collection_time) INCLUDE (database_name, module_name, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us, max_cpu_time_us)", sql, StringComparison.Ordinal);

        /* Three single-row analyze_*_plan lookup indexes (no INCLUDE — one heap fetch is cheap),
           plus the #1981 query_stats handle twin the ProcStats comparison's representative-statement
           LATERAL probes (server_id, sql_handle, newest-first — bounded by raw retention's 4 days). */
        Assert.Contains("idx_procedure_stats_server_handle_time ON collect.procedure_stats (server_id, sql_handle, collection_time DESC)", sql, StringComparison.Ordinal);
        Assert.Contains("idx_query_stats_server_handle_time ON collect.query_stats (server_id, sql_handle, collection_time DESC)", sql, StringComparison.Ordinal);
        Assert.Contains("idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)", sql, StringComparison.Ordinal);
        Assert.Contains("idx_query_store_stats_server_db_query_plan_time ON collect.query_store_stats (server_id, database_name, query_id, plan_id, collection_time DESC)", sql, StringComparison.Ordinal);

        /* Every index is idempotent (no-op where a field box already hand-applied it, or a prior start made it). */
        Assert.Equal(7, CountOccurrences(sql, "CREATE INDEX IF NOT EXISTS"));   /* +1: the #1981 handle index */
        Assert.DoesNotContain("CREATE INDEX ON", sql, StringComparison.Ordinal);

        /* Per-table autovacuum-insert override on exactly the FOUR high-rate insert tables (NOT a global GUC
           change). pg_statement_stats joined them: it is query_stats' per-minute PostgreSQL twin, same shape
           and cadence and the same pure-insert hypertable chunks, so the stock 0.2 scale factor leaves the
           day's hot chunk stale before the TimescaleDB rollover exactly as it did for the other three. */
        Assert.Contains("ALTER TABLE collect.procedure_stats SET (autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_vacuum_insert_threshold = 10000)", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE collect.query_stats SET (autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_vacuum_insert_threshold = 10000)", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE collect.query_store_stats SET (autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_vacuum_insert_threshold = 10000)", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE collect.pg_statement_stats SET (autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_vacuum_insert_threshold = 10000)", sql, StringComparison.Ordinal);

        Assert.Equal(11, PgTableTuning.Statements.Count);   /* +1 #1981 query_stats handle index, +1 pg_statement_stats */
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        int count = 0, i = 0;
        while ((i = haystack.IndexOf(needle, i, StringComparison.Ordinal)) >= 0)
        {
            count++;
            i += needle.Length;
        }

        return count;
    }
}
