/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Generates Lite's DuckDB collector-table DDL from the shared collector definitions'
/// engine-neutral column metadata (<see cref="ICollectorSchemaInfo"/>) — the SAME catalog Darling's
/// <c>PgSchemaGenerator</c> builds its Postgres schema from, so a collector column change lands in
/// exactly one place (the definition's <c>PayloadColumns</c>) and can no longer silently drift from
/// what Darling generates. This replaces the 35 hand-written <c>CREATE TABLE</c> constants that used
/// to live in <see cref="Schema"/>.
///
/// <para>The output is byte-for-byte STORAGE-equivalent to those pre-generation hand-written tables
/// (proven table-by-table by <c>DuckDbSchemaEquivalenceTests</c> against a frozen snapshot): same
/// columns, same DuckDB types, same order, same PRIMARY KEY, same NOT NULL / DEFAULT constraints,
/// same indexes. Existing DuckDB stores are unaffected regardless — every statement is
/// <c>CREATE ... IF NOT EXISTS</c>, so it is a no-op on an already-created table; this generator only
/// shapes FRESH stores, and it shapes them identically to before.</para>
///
/// <para>Two things the engine-neutral catalog does NOT carry, which Lite's DuckDB schema has always
/// enforced and which are therefore reproduced here as a small, pinned, Lite-local overlay (Darling
/// deliberately drops both — its hypertables can't carry the prefix PRIMARY KEY and its bulk COPY
/// path doesn't want per-column NOT NULL): (1) the prefix <c>collection_id</c>/<c>deadlock_id</c>/…
/// PRIMARY KEY, and (2) NOT NULL on the always-populated key/name/count payload columns plus the one
/// <c>query_snapshots.is_cdc_capture DEFAULT false</c>. See <see cref="PayloadColumnConstraints"/>.</para>
///
/// <para>Index names mirror Lite's historical (irregular) names exactly — e.g.
/// <c>idx_cpu_time</c> for <c>cpu_utilization_stats</c>, not the uniform <c>idx_&lt;table&gt;_time</c>
/// Darling uses — so on an existing store the <c>CREATE INDEX IF NOT EXISTS</c> matches the index
/// already there rather than adding a redundant second one.</para>
/// </summary>
public static class DuckDbSchemaGenerator
{
    /// <summary>Maps an engine-neutral collector column to its DuckDB storage type.</summary>
    public static string TypeFor(CollectorColumn column)
    {
        if (column is null)
        {
            throw new ArgumentNullException(nameof(column));
        }

        switch (column.Type)
        {
            case CollectorColumnType.BigInt: return "BIGINT";
            case CollectorColumnType.Integer: return "INTEGER";
            case CollectorColumnType.SmallInt: return "SMALLINT";
            case CollectorColumnType.Varchar: return "VARCHAR";
            /* UTC by convention across the product; DuckDB TIMESTAMP is tz-naive (matches Darling's). */
            case CollectorColumnType.Timestamp: return "TIMESTAMP";
            /* DuckDB DOUBLE == DOUBLE PRECISION == FLOAT8 (aliases for the same 8-byte type). The
               pre-generation schema spelled it both ways (spinlock_stats DOUBLE, query_snapshots
               DOUBLE PRECISION); either resolves to the same stored type, proven by the equivalence
               test's PRAGMA table_info comparison. */
            case CollectorColumnType.Double: return "DOUBLE";
            case CollectorColumnType.Decimal:
                if (column.Precision <= 0)
                {
                    throw new InvalidOperationException(
                        $"Decimal column '{column.Name}' has no declared precision/scale — mirror the DuckDB DECIMAL(p,s) from Lite's schema.");
                }
                return $"DECIMAL({column.Precision},{column.Scale})";
            case CollectorColumnType.Boolean: return "BOOLEAN";
            default:
                throw new ArgumentOutOfRangeException(nameof(column), column.Type, "Unmapped collector column type");
        }
    }

    /// <summary>
    /// Emits <c>CREATE TABLE IF NOT EXISTS</c> for one collector's destination table, reproducing
    /// Lite's exact pre-generation shape: the prefix id (PRIMARY KEY, when the collector includes
    /// one), the prefix timestamp, <c>server_id</c>, <c>server_name</c>, then every payload column in
    /// definition order with its DuckDB type and any NOT NULL / DEFAULT overlay.
    /// </summary>
    public static string CreateTable(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        var sb = new StringBuilder();
        sb.Append("CREATE TABLE IF NOT EXISTS ").Append(schema.TargetTable).Append(" (\n");

        if (schema.IncludesCollectionId)
        {
            sb.Append("    ").Append(schema.PrefixIdColumnName).Append(" BIGINT PRIMARY KEY,\n");
        }

        sb.Append("    ").Append(schema.PrefixTimeColumnName).Append(" TIMESTAMP NOT NULL,\n");
        sb.Append("    server_id INTEGER NOT NULL,\n");
        sb.Append("    server_name VARCHAR NOT NULL");

        foreach (var column in schema.PayloadColumns)
        {
            sb.Append(",\n    ").Append(column.Name).Append(' ').Append(TypeFor(column));

            var constraint = PayloadConstraint(schema.TargetTable, column.Name);
            if (constraint is not null)
            {
                sb.Append(' ').Append(constraint);
            }
        }

        sb.Append("\n)");
        return sb.ToString();
    }

    /// <summary>
    /// Emits the table's retrieval index, mirroring Lite's historical index NAME and COLUMNS exactly.
    /// Most tables get <c>idx_&lt;table&gt;_time</c> on <c>(server_id, &lt;prefix time&gt;)</c>; a
    /// handful carry irregular names or columns predating the uniform scheme, and
    /// <c>server_config</c>/<c>database_config</c> have no index. Returns null when the table has none.
    /// </summary>
    public static string? CreateIndex(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        switch (schema.TargetTable)
        {
            /* No index in Lite's hand-written schema. */
            case "server_config":
            case "database_config":
                return null;

            /* Irregular COLUMN: indexed on the payload sample_time, not the collection_time prefix. */
            case "memory_pressure_events":
                return "CREATE INDEX IF NOT EXISTS idx_memory_pressure_events_time ON memory_pressure_events(server_id, sample_time)";

            /* Composite object-drill index for the FinOps Index Analysis reads. */
            case "index_object_stats":
                return "CREATE INDEX IF NOT EXISTS idx_index_object_stats_object ON index_object_stats(server_id, database_name, object_id, index_id, collection_time)";

            /* Irregular NAMES (short-form, predating the uniform idx_<table>_time scheme). */
            case "cpu_utilization_stats":
                return "CREATE INDEX IF NOT EXISTS idx_cpu_time ON cpu_utilization_stats(server_id, collection_time)";
            case "file_io_stats":
                return "CREATE INDEX IF NOT EXISTS idx_file_io_time ON file_io_stats(server_id, collection_time)";
            case "memory_stats":
                return "CREATE INDEX IF NOT EXISTS idx_memory_time ON memory_stats(server_id, collection_time)";
            case "tempdb_stats":
                return "CREATE INDEX IF NOT EXISTS idx_tempdb_time ON tempdb_stats(server_id, collection_time)";
            case "perfmon_stats":
                return "CREATE INDEX IF NOT EXISTS idx_perfmon_time ON perfmon_stats(server_id, collection_time)";
            case "query_store_stats":
                return "CREATE INDEX IF NOT EXISTS idx_query_store_time ON query_store_stats(server_id, collection_time)";

            /* Default: idx_<table>_time on (server_id, <prefix time>). The prefix-time column is
               capture_time for the config snapshots (database_scoped_config, trace_flags) and
               collection_time everywhere else — both handled without special-casing. */
            default:
                return $"CREATE INDEX IF NOT EXISTS idx_{schema.TargetTable}_time ON {schema.TargetTable}(server_id, {schema.PrefixTimeColumnName})";
        }
    }

    /// <summary>
    /// The NOT NULL / DEFAULT overlay for PAYLOAD columns — the constraints Lite's hand-written DuckDB
    /// schema carried that the engine-neutral <see cref="CollectorColumn"/> does not encode. Keyed by
    /// <c>"{table}.{column}"</c>; the value is the exact DDL fragment appended after the column's type
    /// (e.g. <c>"NOT NULL"</c>, <c>"DEFAULT false"</c>). Absent = plain nullable column.
    ///
    /// <para>Frozen to reproduce the pre-generation schema exactly and PINNED by
    /// <c>DuckDbSchemaEquivalenceTests</c> (a missing or spurious entry makes a table's
    /// <c>PRAGMA table_info</c> diverge from the golden snapshot, failing the build). The prefix
    /// columns (id PRIMARY KEY, time / server_id / server_name NOT NULL) are NOT listed here — they
    /// are emitted uniformly by <see cref="CreateTable"/>.</para>
    ///
    /// <para>Only <c>NOT NULL</c> and column <c>DEFAULT</c> fragments belong here — the two things the
    /// equivalence test's <c>PRAGMA table_info</c> comparison verifies (its <c>notnull</c> / <c>dflt_value</c>
    /// columns). Do NOT add <c>CHECK</c>, <c>UNIQUE</c>, <c>COLLATE</c>, or generated-column expressions:
    /// none are needed by any collector table, and <c>table_info</c> would not fully validate them, so the
    /// equivalence proof would have a blind spot. (Such shapes are not expressible from the catalog anyway.)</para>
    /// </summary>
    public static IReadOnlyDictionary<string, string> PayloadColumnConstraints { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["wait_stats.wait_type"] = "NOT NULL",
            ["latch_stats.latch_class"] = "NOT NULL",
            ["spinlock_stats.spinlock_name"] = "NOT NULL",
            ["plan_cache_stats.cacheobjtype"] = "NOT NULL",
            ["plan_cache_stats.objtype"] = "NOT NULL",
            ["cpu_utilization_stats.sample_time"] = "NOT NULL",
            ["memory_clerks.clerk_type"] = "NOT NULL",
            ["memory_pressure_events.sample_time"] = "NOT NULL",
            ["memory_pressure_events.memory_notification"] = "NOT NULL",
            ["memory_pressure_events.memory_indicators_process"] = "NOT NULL",
            ["memory_pressure_events.memory_indicators_system"] = "NOT NULL",
            ["query_store_stats.database_name"] = "NOT NULL",
            /* The one payload column DEFAULT (not a NOT NULL): the long-running-query alert reads it. */
            ["query_snapshots.is_cdc_capture"] = "DEFAULT false",
            ["perfmon_stats.object_name"] = "NOT NULL",
            ["perfmon_stats.counter_name"] = "NOT NULL",
            ["server_config.configuration_name"] = "NOT NULL",
            ["database_config.database_name"] = "NOT NULL",
            ["database_scoped_config.database_name"] = "NOT NULL",
            ["database_scoped_config.configuration_name"] = "NOT NULL",
            /* #2319: the row is named by the database it was enumerated from; every other column is
               nullable because sys.database_query_store_options answers even for a QS-off database and
               nothing else on the row is guaranteed. */
            ["query_store_health.database_name"] = "NOT NULL",
            /* Every plan_correction row is named by the database it was enumerated from — including the
               enablement-only row a database with no recommendations produces. Nothing else on the row
               is guaranteed non-null (a database that has never had a regression carries no
               recommendation at all), so this is the only column that can carry the constraint. */
            ["plan_correction.database_name"] = "NOT NULL",
            ["trace_flags.trace_flag"] = "NOT NULL",
            ["trace_flags.status"] = "NOT NULL",
            ["trace_flags.is_global"] = "NOT NULL",
            ["trace_flags.is_session"] = "NOT NULL",
            ["dmv_blocking_snapshots.monitor_loop"] = "NOT NULL",
            ["running_jobs.job_name"] = "NOT NULL",
            ["running_jobs.job_id"] = "NOT NULL",
            ["running_jobs.job_enabled"] = "NOT NULL",
            ["running_jobs.start_time"] = "NOT NULL",
            ["running_jobs.current_duration_seconds"] = "NOT NULL",
            ["running_jobs.avg_duration_seconds"] = "NOT NULL",
            ["running_jobs.p95_duration_seconds"] = "NOT NULL",
            ["running_jobs.successful_run_count"] = "NOT NULL",
            ["running_jobs.is_running_long"] = "NOT NULL",
            ["database_size_stats.database_name"] = "NOT NULL",
            ["database_size_stats.database_id"] = "NOT NULL",
            ["database_size_stats.file_id"] = "NOT NULL",
            ["database_size_stats.file_type_desc"] = "NOT NULL",
            ["database_size_stats.file_name"] = "NOT NULL",
            ["database_size_stats.physical_name"] = "NOT NULL",
            ["database_size_stats.total_size_mb"] = "NOT NULL",
            ["index_object_stats.database_name"] = "NOT NULL",
            ["index_object_stats.database_id"] = "NOT NULL",
            ["index_object_stats.schema_name"] = "NOT NULL",
            ["index_object_stats.object_id"] = "NOT NULL",
            ["index_object_stats.table_name"] = "NOT NULL",
            ["index_object_stats.index_id"] = "NOT NULL",
            ["server_properties.edition"] = "NOT NULL",
            ["server_properties.product_version"] = "NOT NULL",
            ["server_properties.product_level"] = "NOT NULL",
            ["server_properties.engine_edition"] = "NOT NULL",
            /* cpu_count / hyperthread_ratio / physical_memory_mb are deliberately NULLABLE (#1591):
               they come from sys.dm_os_sys_info, and a login without VIEW SERVER STATE (VIEW DATABASE
               STATE on Azure SQL DB) still collects every other column. NOT NULL here would fail the
               whole insert and lose the row -- the exact bug #1591 tracks. */
            ["session_stats.program_name"] = "NOT NULL",
            ["session_stats.connection_count"] = "NOT NULL",
            ["session_stats.running_count"] = "NOT NULL",
            ["session_stats.sleeping_count"] = "NOT NULL",
            ["session_stats.dormant_count"] = "NOT NULL",
            ["session_summary_stats.total_sessions"] = "NOT NULL",
            ["session_summary_stats.running_sessions"] = "NOT NULL",
            ["session_summary_stats.sleeping_sessions"] = "NOT NULL",
            ["session_summary_stats.background_sessions"] = "NOT NULL",
            ["session_summary_stats.dormant_sessions"] = "NOT NULL",
            ["session_summary_stats.idle_sessions_over_30min"] = "NOT NULL",
            ["session_summary_stats.sessions_waiting_for_memory"] = "NOT NULL",
            ["session_summary_stats.databases_with_connections"] = "NOT NULL",
            ["job_history.instance_id"] = "NOT NULL",
            ["job_history.job_id"] = "NOT NULL",
            ["job_history.job_name"] = "NOT NULL",
            ["job_history.job_enabled"] = "NOT NULL",
            ["job_history.step_id"] = "NOT NULL",
            ["job_history.run_status"] = "NOT NULL",
            ["job_history.run_duration_seconds"] = "NOT NULL",
            ["job_history.retries_attempted"] = "NOT NULL",
            ["agent_status.agent_running"] = "NOT NULL",
        };

    private static string? PayloadConstraint(string table, string column) =>
        PayloadColumnConstraints.TryGetValue($"{table}.{column}", out var c) ? c : null;

    /// <summary>
    /// The catalog entries Lite stores, which is the SQL Server ones ONLY.
    /// <para>The collector catalog is deliberately engine-mixed and shared with Darling, but Lite has no
    /// PostgreSQL target and cannot acquire one — the engine gate in
    /// <c>CollectorCatalog.AppliesTo(definition, target)</c> means a PostgreSQL definition is never
    /// dispatched here. Emitting their tables anyway would put seven permanently-empty tables into every
    /// seat's local DuckDB file forever. Darling makes the opposite choice deliberately (its central store
    /// may gain a PostgreSQL target at any time, so it carries the tables empty); Lite's file is per-seat and
    /// disposable, so the tables would be pure noise.</para>
    /// <para>Filtering here rather than at each call site keeps table, index and name generation in step by
    /// construction — the three used to walk the catalog independently, which is exactly how one of them
    /// would come to disagree.</para>
    /// </summary>
    internal static IEnumerable<ICollectorSchemaInfo> StoredCollectors =>
        CollectorCatalog.All.Where(s => s.TargetEngine == CollectorTargetEngine.SqlServer);

    /// <summary>Every collector target table Lite stores, in catalog order (the generated table/index set).</summary>
    public static IEnumerable<string> CollectorTableNames()
    {
        foreach (var schema in StoredCollectors)
        {
            yield return schema.TargetTable;
        }
    }

    /// <summary>The generated <c>CREATE TABLE</c> statements for the collectors Lite stores, in catalog order.</summary>
    public static IEnumerable<string> CreateTableStatements()
    {
        foreach (var schema in StoredCollectors)
        {
            yield return CreateTable(schema);
        }
    }

    /// <summary>
    /// The generated <c>CREATE INDEX</c> statements for all catalog collectors that have one, in
    /// catalog order (33 of 35 — server_config and database_config have none).
    /// </summary>
    public static IEnumerable<string> CreateIndexStatements()
    {
        foreach (var schema in StoredCollectors)
        {
            var index = CreateIndex(schema);
            if (index is not null)
            {
                yield return index;
            }
        }
    }
}
