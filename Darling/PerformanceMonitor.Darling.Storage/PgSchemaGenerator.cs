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
using System.Text;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Generates Darling's Postgres schema from the collector definitions' engine-neutral column
/// metadata (<see cref="ICollectorSchemaInfo"/>) — the same metadata Lite's DuckDB schema was
/// hand-written against, so a collector change lands in exactly one place and both stores stay
/// column-for-column identical. Names and types mirror Lite's DuckDB schema (collection_id /
/// deadlock_id / config_id prefixes, capture_time on the config snapshots, per-column
/// numeric(p,s)) so analysis SQL can twin without translation. Two deliberate physical
/// deviations from Lite: (1) DuckDB's PRIMARY KEY on the prefix id column is NOT carried over —
/// TimescaleDB hypertables require the partition column in any unique constraint, and bulk COPY
/// ingest doesn't want one; the id stays a NOT NULL bigint. (2) Index names are uniform
/// (idx_&lt;table&gt;_time) where Lite's handwritten names are irregular (idx_cpu_time) — index
/// names are physical-only and never referenced by analysis SQL.
/// </summary>
public static class PgSchemaGenerator
{
    /// <summary>Maps an engine-neutral collector column to its Postgres type.</summary>
    public static string TypeFor(CollectorColumn column)
    {
        if (column is null)
        {
            throw new ArgumentNullException(nameof(column));
        }

        switch (column.Type)
        {
            case CollectorColumnType.BigInt: return "bigint";
            case CollectorColumnType.Integer: return "integer";
            case CollectorColumnType.SmallInt: return "smallint";
            case CollectorColumnType.Varchar: return "text";
            /* UTC by convention across the product; matches DuckDB TIMESTAMP (no tz). */
            case CollectorColumnType.Timestamp: return "timestamp";
            case CollectorColumnType.Double: return "double precision";
            case CollectorColumnType.Decimal:
                if (column.Precision <= 0)
                {
                    throw new InvalidOperationException(
                        $"Decimal column '{column.Name}' has no declared precision/scale — mirror the DuckDB DECIMAL(p,s) from Lite's schema.");
                }
                return $"numeric({column.Precision},{column.Scale})";
            case CollectorColumnType.Boolean: return "boolean";
            default:
                throw new ArgumentOutOfRangeException(nameof(column), column.Type, "Unmapped collector column type");
        }
    }

    /// <summary>Emits CREATE TABLE IF NOT EXISTS for one collector's destination table.</summary>
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
            sb.Append("    ").Append(schema.PrefixIdColumnName).Append(" bigint NOT NULL,\n");
        }

        sb.Append("    ").Append(schema.PrefixTimeColumnName).Append(" timestamp NOT NULL,\n");
        sb.Append("    server_id integer NOT NULL,\n");
        sb.Append("    server_name text NOT NULL");

        foreach (var column in schema.PayloadColumns)
        {
            sb.Append(",\n    ").Append(column.Name).Append(' ').Append(TypeFor(column));
        }

        sb.Append("\n);");
        return sb.ToString();
    }

    /// <summary>
    /// Emits the table's retrieval index, mirroring Lite's index COLUMNS exactly: the default is
    /// (server_id, &lt;prefix time&gt;); memory_pressure_events indexes its payload sample_time,
    /// index_object_stats has its composite object drill index, and server_config /
    /// database_config have no index in Lite. Returns null when the table has none.
    /// </summary>
    public static string? CreateIndex(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        switch (schema.TargetTable)
        {
            case "server_config":
            case "database_config":
                return null;
            case "memory_pressure_events":
                return "CREATE INDEX IF NOT EXISTS idx_memory_pressure_events_time ON memory_pressure_events(server_id, sample_time);";
            case "index_object_stats":
                return "CREATE INDEX IF NOT EXISTS idx_index_object_stats_object ON index_object_stats(server_id, database_name, object_id, index_id, collection_time);";
            default:
                return $"CREATE INDEX IF NOT EXISTS idx_{schema.TargetTable}_time ON {schema.TargetTable}(server_id, {schema.PrefixTimeColumnName});";
        }
    }

    /// <summary>
    /// The two schemas the V8 security split introduces. <c>collect</c> holds the collector
    /// hypertables plus the service-written / user-read metadata (read-only to the admin/viewer
    /// roles); <c>config</c> holds exactly the tables a human operator mutates through the
    /// Viewer/MCP (the admin role's write surface). Table NAMES are unchanged — only the schema
    /// moves; every SQL site keeps its bare table reference and resolves through
    /// <c>search_path = collect, config, public</c>. See darling-security-hardening §3.1.
    /// </summary>
    public const string CollectSchema = "collect";
    public const string ConfigSchema = "config";

    /// <summary>The database-default search path V8 establishes (see <see cref="GenerateV8Move"/>).</summary>
    public const string SearchPath = "collect, config, public";

    /// <summary>
    /// The service-written / user-read metadata tables that join the collector hypertables in
    /// <c>collect</c> (they are NOT in <see cref="CollectorCatalog"/>). <c>analysis_findings</c> is
    /// service-written and viewer-read-only (a keyless hypertable candidate), so it lives with the
    /// other collected data rather than with the operator-writable mutes (design fork #1 → collect).
    /// <c>darling_schema_version</c> is migration metadata (owner-only; no user grant needed).
    /// </summary>
    public static readonly IReadOnlyList<string> CollectMetadataTables = new[]
    {
        "servers",
        "collection_log",
        "analysis_findings",
        "darling_schema_version",
    };

    /// <summary>
    /// The operator-writable coordination + analysis-mute tables that move to <c>config</c> — the
    /// admin role's precise write surface (mute rules, alert dismissals, analysis mutes; the
    /// edge-trigger watermarks ride along with the alert-coordination family). See §3.1.
    /// </summary>
    public static readonly IReadOnlyList<string> ConfigTables = new[]
    {
        "config_alert_log",
        "config_edge_trigger_watermarks",
        "config_mute_rules",
        "analysis_muted",
    };

    /// <summary>
    /// The 24 <c>v_*</c> passthrough views (V4 ×17, V5 ×5, V6 ×2) — all read-only, all moving to
    /// <c>collect</c> with the tables they wrap. Listed explicitly because the view set is not
    /// derivable from the collector catalog (e.g. <c>v_collection_log</c> wraps a registry table,
    /// and several collector tables have no view). Pinned by test against the migration DDL.
    /// </summary>
    public static readonly IReadOnlyList<string> CollectViews = new[]
    {
        "v_wait_stats", "v_query_stats", "v_query_store_stats", "v_cpu_utilization_stats",
        "v_memory_grant_stats", "v_memory_stats", "v_perfmon_stats", "v_session_stats",
        "v_file_io_stats", "v_blocked_process_reports", "v_deadlocks", "v_dmv_blocking_snapshots",
        "v_index_object_stats", "v_database_size_stats", "v_tempdb_stats", "v_query_snapshots",
        "v_database_config", "v_running_jobs", "v_server_config", "v_database_scoped_config",
        "v_trace_flags", "v_collection_log", "v_memory_clerks", "v_memory_pressure_events",
    };

    /// <summary>
    /// The collectors whose <c>v_*</c> passthrough views were added AFTER the V8 schema split
    /// (V10 latch/spinlock, V11 cpu-scheduler/plan-cache, V12 session-summary, V13 system-health,
    /// V47 pvs_stats),
    /// so their views are NOT in <see cref="CollectViews"/> (which lists only the V4–V6 views V8
    /// moves). Referenced as the SAME collector instances the V10–V13 generators emit, so the set
    /// tracks the collector definitions rather than a parallel name list.
    /// </summary>
    private static readonly IReadOnlyList<ICollectorSchemaInfo> PostV8ViewCollectors = new ICollectorSchemaInfo[]
    {
        LatchStatsCollector.Instance,
        SpinlockStatsCollector.Instance,
        CpuSchedulerStatsCollector.Instance,
        PlanCacheStatsCollector.Instance,
        SessionSummaryStatsCollector.Instance,
        SystemHealthEventsCollector.Instance,
        PvsStatsCollector.Instance,
        QueryStoreHealthCollector.Instance,
    };

    /// <summary>
    /// The views that are NOT bare passthroughs because they RESOLVE a payload dimension
    /// (<see cref="GenerateQueryStatsResolvingView"/>, #1767) — excluded from
    /// <see cref="AllPassthroughViews"/> so the refresh-all idiom cannot silently revert them.
    ///
    /// <para>This exclusion is load-bearing, not bookkeeping. "ADD COLUMN, then
    /// <c>CREATE OR REPLACE VIEW v_x AS SELECT * FROM x</c> to re-expand the frozen column list" is
    /// the established migration idiom here (V14, V15, V27, V28). Applied to <c>v_query_stats</c> it
    /// would overwrite the COALESCE that resolves the dimension tables, and the damage would be
    /// invisible: every text and plan read would return NULL for rows written since #1767, which
    /// looks exactly like a collection outage rather than a schema regression. A migration that
    /// needs to re-expand this view must re-emit the RESOLVING definition instead, and a test pins
    /// that the last migration defining it is the resolving one.</para>
    /// </summary>
    public static readonly IReadOnlyList<string> PayloadResolvingViews = new[] { "v_query_stats" };

    /// <summary>
    /// Every <c>v_*</c> passthrough view that exists in a fully-migrated store, in creation order:
    /// the V4–V6 views (<see cref="CollectViews"/>) followed by the post-V8 collector views
    /// (<see cref="PostV8ViewCollectors"/>), MINUS the payload-resolving views
    /// (<see cref="PayloadResolvingViews"/>), which are no longer <c>SELECT *</c> passthroughs.
    /// The single authoritative refresh list — <see cref="GenerateV14RefreshViews"/> refreshes
    /// exactly this set, and a test cross-checks this set plus the resolving views against every
    /// <c>CREATE OR REPLACE VIEW</c> statement across all migrations so the two can never diverge.
    /// Every entry is <c>v_&lt;table&gt;</c>; the base table is the name minus the <c>v_</c> prefix.
    /// </summary>
    public static readonly IReadOnlyList<string> AllPassthroughViews =
        CollectViews.Concat(PostV8ViewCollectors.Select(c => "v_" + c.TargetTable))
            .Where(v => !PayloadResolvingViews.Contains(v, StringComparer.Ordinal)).ToArray();

    /// <summary>
    /// The V8 schema-split migration body — creates <c>collect</c>/<c>config</c> and moves every
    /// existing object out of <c>public</c> with <c>ALTER … SET SCHEMA</c>. Generated (not
    /// hand-listed) so a collector added to <see cref="CollectorCatalog"/> is moved automatically;
    /// the registry/config/view names that are not catalog-derived come from the explicit lists
    /// above. Every move is <c>IF EXISTS</c> (a no-op when a fresh V1–V7 run has not created the
    /// object, or a partial prior run already moved it — though the whole migration is
    /// transactional, so partial runs roll back; the guard is defense in depth).
    ///
    /// <para>Runs identically on a FRESH store (V1–V7 just created plain tables in <c>public</c>,
    /// which V8 moves before TimescaleDB converts the now-in-<c>collect</c> tables to hypertables)
    /// AND on an EXISTING store (Erik's fixture, where the collector tables are already
    /// hypertables — <c>ALTER TABLE &lt;hypertable&gt; SET SCHEMA</c> moves the hypertable and
    /// propagates to its chunks; validated live on TimescaleDB 2.28.1). The
    /// <c>ALTER DATABASE … SET search_path</c> is deliberately NOT here — it needs the live
    /// database name and is a best-effort step outside this transaction
    /// (<see cref="PgMigrations"/>), so a least-privilege BYO login that lacks it does not fail the
    /// whole migration.</para>
    /// </summary>
    public static string GenerateV8Move()
    {
        var sb = new StringBuilder();
        sb.Append("/* V8: split public into collect/config (Darling security hardening, #1262).\n");
        sb.Append("   Table NAMES are unchanged; search_path = collect, config, public resolves the bare\n");
        sb.Append("   references every SQL site already uses, so no query is re-qualified. */\n\n");

        sb.Append("CREATE SCHEMA IF NOT EXISTS ").Append(CollectSchema).Append(" AUTHORIZATION ")
            .Append(OwnerRole).Append(";\n");
        sb.Append("CREATE SCHEMA IF NOT EXISTS ").Append(ConfigSchema).Append(" AUTHORIZATION ")
            .Append(OwnerRole).Append(";\n\n");

        sb.Append("/* collect: all collector tables (from the catalog) + registry/metadata + the V4-V6 views */\n");
        foreach (var schema in CollectorCatalog.All)
        {
            AppendSetSchema(sb, "TABLE", schema.TargetTable, CollectSchema);
        }

        foreach (var table in CollectMetadataTables)
        {
            AppendSetSchema(sb, "TABLE", table, CollectSchema);
        }

        foreach (var view in CollectViews)
        {
            AppendSetSchema(sb, "VIEW", view, CollectSchema);
        }

        sb.Append("\n/* config: the operator-writable coordination + analysis-mute tables */\n");
        foreach (var table in ConfigTables)
        {
            AppendSetSchema(sb, "TABLE", table, ConfigSchema);
        }

        return sb.ToString();
    }

    /// <summary>The owner role every managed object is authored by (the bootstrap superuser).</summary>
    private const string OwnerRole = "darling";

    private static void AppendSetSchema(StringBuilder sb, string objectKind, string objectName, string targetSchema)
    {
        sb.Append("ALTER ").Append(objectKind).Append(" IF EXISTS public.").Append(objectName)
            .Append(" SET SCHEMA ").Append(targetSchema).Append(";\n");
    }

    /// <summary>
    /// The full collector-table schema script in catalog order — the body of Darling's first
    /// versioned migration. TimescaleDB hypertable conversion is deliberately NOT emitted here;
    /// it is runtime setup (<see cref="TimescaleSupport"/>), applied by the service only when
    /// the extension is detected, so this script stays valid on plain PostgreSQL.
    /// </summary>
    public static string GenerateFullSchema()
    {
        var sb = new StringBuilder();
        sb.Append("/* Darling collector tables — generated from PerformanceMonitor.Collectors definitions.\n");
        sb.Append("   Column names and types mirror Lite's DuckDB schema (see PgSchemaGenerator remarks). */\n");

        foreach (var schema in CollectorCatalog.All)
        {
            sb.Append('\n').Append(CreateTable(schema)).Append('\n');

            var index = CreateIndex(schema);
            if (index is not null)
            {
                sb.Append(index).Append('\n');
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// The V10 migration body — adds the latch_stats and spinlock_stats collector tables plus their
    /// <c>v_*</c> passthrough views. Generated from the collector definitions (not hand-listed) so
    /// the tables are column-for-column identical to the fresh-store V1 shape. <c>CREATE TABLE IF
    /// NOT EXISTS</c> makes this a harmless no-op on a fresh store (V1 already created these from the
    /// current catalog and V8 moved them to <c>collect</c>) and the real create on a store built
    /// before these collectors existed; the views are created directly in <c>collect</c> because V10
    /// runs after V8, resolving the bare names through <c>search_path = collect, config, public</c>.
    /// </summary>
    public static string GenerateV10AddLatchSpinlock()
    {
        var sb = new StringBuilder();
        sb.Append("/* V10: latch_stats + spinlock_stats collectors (Dashboard->Darling parity, #1262).\n");
        sb.Append("   Generated from the collector definitions so the tables match the fresh V1 shape. */\n");

        foreach (ICollectorSchemaInfo schema in new ICollectorSchemaInfo[]
        {
            LatchStatsCollector.Instance,
            SpinlockStatsCollector.Instance,
        })
        {
            sb.Append('\n').Append(CreateTable(schema)).Append('\n');

            var index = CreateIndex(schema);
            if (index is not null)
            {
                sb.Append(index).Append('\n');
            }

            sb.Append("CREATE OR REPLACE VIEW v_").Append(schema.TargetTable)
              .Append(" AS SELECT * FROM ").Append(schema.TargetTable).Append(";\n");
        }

        return sb.ToString();
    }

    /// <summary>
    /// The V11 migration body — adds the cpu_scheduler_stats and plan_cache_stats collector tables
    /// plus their <c>v_*</c> passthrough views. Generated from the collector definitions (not
    /// hand-listed) so the tables are column-for-column identical to the fresh-store V1 shape.
    /// <c>CREATE TABLE IF NOT EXISTS</c> makes this a harmless no-op on a fresh store (V1 already
    /// created these from the current catalog and V8 moved them to <c>collect</c>) and the real
    /// create on a store built before these collectors existed; the views are created directly in
    /// <c>collect</c> because V11 runs after V8, resolving the bare names through
    /// <c>search_path = collect, config, public</c>. The Darling service collects cpu_scheduler_stats
    /// only on non-Azure-SQL-DB targets (its AppliesTo gate); the table still exists everywhere.
    /// </summary>
    public static string GenerateV11AddCpuSchedulerPlanCache()
    {
        var sb = new StringBuilder();
        sb.Append("/* V11: cpu_scheduler_stats + plan_cache_stats collectors (Dashboard->Darling parity, #1262).\n");
        sb.Append("   Generated from the collector definitions so the tables match the fresh V1 shape. */\n");

        foreach (ICollectorSchemaInfo schema in new ICollectorSchemaInfo[]
        {
            CpuSchedulerStatsCollector.Instance,
            PlanCacheStatsCollector.Instance,
        })
        {
            sb.Append('\n').Append(CreateTable(schema)).Append('\n');

            var index = CreateIndex(schema);
            if (index is not null)
            {
                sb.Append(index).Append('\n');
            }

            sb.Append("CREATE OR REPLACE VIEW v_").Append(schema.TargetTable)
              .Append(" AS SELECT * FROM ").Append(schema.TargetTable).Append(";\n");
        }

        return sb.ToString();
    }

    /// <summary>
    /// The V12 migration body — adds the session_summary_stats collector table (server-wide session
    /// SUMMARY: the connection-leak / idle signal from sys.dm_exec_sessions + sys.dm_exec_requests)
    /// plus its <c>v_*</c> passthrough view. Generated from the collector definition (not hand-listed)
    /// so the table is column-for-column identical to the fresh-store V1 shape. <c>CREATE TABLE IF NOT
    /// EXISTS</c> makes this a harmless no-op on a fresh store (V1 already created it from the current
    /// catalog and V8 moved it to <c>collect</c>) and the real create on a store built before this
    /// collector existed; the view is created directly in <c>collect</c> because V12 runs after V8,
    /// resolving the bare names through <c>search_path = collect, config, public</c>. Distinct from the
    /// per-application session_stats table (V1), which this does not touch.
    /// </summary>
    public static string GenerateV12AddSessionSummary()
    {
        var sb = new StringBuilder();
        sb.Append("/* V12: session_summary_stats collector (Dashboard->Darling connection-leak / idle parity, #1262).\n");
        sb.Append("   Generated from the collector definition so the table matches the fresh V1 shape. */\n");

        foreach (ICollectorSchemaInfo schema in new ICollectorSchemaInfo[]
        {
            SessionSummaryStatsCollector.Instance,
        })
        {
            sb.Append('\n').Append(CreateTable(schema)).Append('\n');

            var index = CreateIndex(schema);
            if (index is not null)
            {
                sb.Append(index).Append('\n');
            }

            sb.Append("CREATE OR REPLACE VIEW v_").Append(schema.TargetTable)
              .Append(" AS SELECT * FROM ").Append(schema.TargetTable).Append(";\n");
        }

        return sb.ToString();
    }

    /// <summary>
    /// The V13 migration body — adds the system_health_events collector table (Stage 1 raw
    /// <c>system_health</c> Extended Events capture: one row per event, raw XML only, no shredding)
    /// plus its <c>v_*</c> passthrough view. Generated from the collector definition (not hand-listed)
    /// so the table is column-for-column identical to the fresh-store V1 shape. <c>CREATE TABLE IF NOT
    /// EXISTS</c> makes this a harmless no-op on a fresh store (V1 already created it from the current
    /// catalog and V8 moved it to <c>collect</c>) and the real create on a store built before this
    /// collector existed; the view is created directly in <c>collect</c> because V13 runs after V8,
    /// resolving the bare names through <c>search_path = collect, config, public</c>.
    /// </summary>
    public static string GenerateV13AddSystemHealthEvents()
    {
        var sb = new StringBuilder();
        sb.Append("/* V13: system_health_events collector (Dashboard->Darling health-parser capture parity, #1262).\n");
        sb.Append("   Generated from the collector definition so the table matches the fresh V1 shape. */\n");

        foreach (ICollectorSchemaInfo schema in new ICollectorSchemaInfo[]
        {
            SystemHealthEventsCollector.Instance,
        })
        {
            sb.Append('\n').Append(CreateTable(schema)).Append('\n');

            var index = CreateIndex(schema);
            if (index is not null)
            {
                sb.Append(index).Append('\n');
            }

            sb.Append("CREATE OR REPLACE VIEW v_").Append(schema.TargetTable)
              .Append(" AS SELECT * FROM ").Append(schema.TargetTable).Append(";\n");
        }

        return sb.ToString();
    }

    /// <summary>
    /// The V14 migration body — <c>CREATE OR REPLACE</c> every <c>v_*</c> passthrough view
    /// (<see cref="AllPassthroughViews"/>) as <c>SELECT * FROM</c> its base table, refreshing the
    /// column list a store UPGRADED across a column-adding migration would otherwise have pinned
    /// stale. Postgres (and DuckDB) freeze a view's <c>SELECT *</c> expansion at CREATE time, so the
    /// views created in V4–V6/V10–V13 do NOT include columns later <c>ADD</c>ed to their base tables
    /// — the V7 plan columns on <c>blocked_process_reports</c>/<c>deadlocks</c> and the V9 inventory
    /// columns. A FRESH store is unaffected (its views were created from the current table shapes);
    /// only an upgraded store is stale, which PR #1376 discovered and worked around for the plan
    /// columns by reading base tables — this migration closes the root cause for every view.
    ///
    /// <para>Safe because every view is a bare <c>v_&lt;table&gt;</c> passthrough and every post-V4
    /// change was <c>ADD COLUMN</c> (Postgres always appends), so the refreshed <c>SELECT *</c> only
    /// ADDs columns at the end — the one alteration <c>CREATE OR REPLACE VIEW</c> permits. Runs after
    /// V8, so the bare names resolve through <c>search_path = collect, config, public</c> to the
    /// collect-schema views/tables (the existing view is found and replaced in place). Idempotent:
    /// on an already-current view it re-installs the identical definition.</para>
    /// </summary>
    public static string GenerateV14RefreshViews()
    {
        var sb = new StringBuilder();
        sb.Append("/* V14: refresh every v_* passthrough view's pinned SELECT * column list (#1262).\n");
        sb.Append("   Postgres freezes SELECT * at CREATE, so an upgraded store's views omit columns\n");
        sb.Append("   ADDed later (V7 plan columns, V9 inventory); CREATE OR REPLACE re-expands them.\n");
        sb.Append("   Append-only ADD COLUMNs => the refresh only adds columns at the end (always legal). */\n\n");

        foreach (var view in AllPassthroughViews)
        {
            /* Every view is v_<table>; the base table is the name minus the "v_" prefix. */
            var table = view.Substring("v_".Length);
            sb.Append("CREATE OR REPLACE VIEW ").Append(view)
              .Append(" AS SELECT * FROM ").Append(table).Append(";\n");
        }

        return sb.ToString();
    }

    /// <summary>
    /// The dimension-table alias each payload dimension gets in the resolving view — stable and
    /// readable, because this view is something a DBA will read in <c>\d+</c> output.
    /// </summary>
    private static string AliasFor(string dimTable)
        => dimTable switch
        {
            PayloadDimensions.QueryTextDimTable => "qtd",
            PayloadDimensions.QueryPlanDimTable => "qpd",
            _ => throw new ArgumentOutOfRangeException(nameof(dimTable), dimTable, "No alias for dimension table"),
        };

    /// <summary>
    /// The V38 migration body — the #1767 payload dimensions.
    ///
    /// <para>Three parts, in dependency order: the two dimension tables; the digest columns APPENDED
    /// to the two fact tables (nullable, so every existing row is untouched and the migration is
    /// metadata-only — no rewrite of a ~234 GB payload, the trap recorded and rejected in #1759);
    /// and the rebuilt <c>v_query_stats</c>.</para>
    ///
    /// <para>The digest columns are NOT in the collector definitions' PayloadColumns, deliberately:
    /// they are a Darling STORAGE concern, and Lite — which shares those definitions and never
    /// captures plans at all — must not grow two columns it has no use for. That means, unlike the
    /// V7 plan columns, a fresh V1 store does NOT already have them and this ADD COLUMN is the only
    /// thing that creates them. They land at the end on fresh and upgraded stores alike, and the
    /// binary COPY names its columns explicitly, so physical order never matters.</para>
    /// </summary>
    public static string GenerateV38PayloadDimensions()
    {
        var sb = new StringBuilder();
        sb.Append("/* V38: hash-keyed dimension tables for query_stats / procedure_stats payloads (#1767).\n");
        sb.Append("   query_text/query_plan_xml stored inline per row were 94% of a 250 GB field store;\n");
        sb.Append("   a measured 1-hour window held 3,166 MB of payload across 23 MB of distinct content.\n");
        sb.Append("   ZERO-REWRITE: the inline columns stay, new rows leave them NULL, readers coalesce,\n");
        sb.Append("   and raw retention ages the inline copies out on its own. */\n\n");

        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            sb.Append(PayloadDimensions.CreateDimTable(dimTable)).Append("\n\n");
        }

        foreach (var dimension in PayloadDimensions.All)
        {
            sb.Append("ALTER TABLE ").Append(dimension.TargetTable)
              .Append(" ADD COLUMN IF NOT EXISTS ").Append(dimension.DigestColumn).Append(" bytea;\n");
        }

        /* The resolving view below is generated from the CURRENT collector definition, so it references
           every payload column the collector has TODAY — including ones whose ALTER migration sits LATER
           in the ladder (host_object_name lands at V51). A store upgrading from <38 runs this body against
           its old table, and the view would fail on the first such column. Pre-adding every payload column
           here (no-op on any store that already has them, same TypeFor the table generator uses) keeps V38
           self-sufficient from any starting version — permanently, for every future payload column too. */
        var querySchema = QueryStatsCollector.Instance;
        foreach (var column in querySchema.PayloadColumns)
        {
            sb.Append("ALTER TABLE ").Append(querySchema.TargetTable)
              .Append(" ADD COLUMN IF NOT EXISTS ").Append(column.Name)
              .Append(' ').Append(TypeFor(column)).Append(";\n");
        }

        /* #2069, same reasoning as the payload-column pre-adds above: the generated view now
           references the plan dim's compressed-content column, whose ALTER migration sits at V54 —
           pre-add it here so a store upgrading from <38 survives the ladder in order. */
        sb.Append("ALTER TABLE ").Append(PayloadDimensions.CompressedContentDimTable)
          .Append(" ADD COLUMN IF NOT EXISTS ").Append(PayloadDimensions.CompressedContentColumn).Append(" bytea;\n");

        sb.Append('\n').Append(GenerateQueryStatsResolvingView());
        return sb.ToString();
    }

    /// <summary>
    /// <c>v_query_stats</c>, rebuilt to RESOLVE each row's payload: the inline column when the row
    /// predates #1767, the dimension row when it does not.
    ///
    /// <para>This is the one <c>v_*</c> view that is not a bare passthrough, and the exception is
    /// deliberate. Six analysis/viewer readers reach query_text through this view; resolving it here
    /// fixes all six at one site AND makes every FUTURE reader correct by default — which is the
    /// actual risk in this change, since a consumer that keeps selecting the raw column does not
    /// error, it silently returns NULL for every new row.</para>
    ///
    /// <para>The column list is GENERATED from the collector definition rather than hand-written,
    /// so it cannot go stale the way an explicit list normally would — the exact failure V14 exists
    /// to fix. A payload column added to the collector appears here automatically. The two digest
    /// columns are appended last, keeping this a legal <c>CREATE OR REPLACE</c> over the previous
    /// <c>SELECT *</c> definition (same names, same types, same order, additions only at the end).</para>
    ///
    /// <para>There is deliberately no <c>v_procedure_stats</c> to match — that view has never
    /// existed (pinned by test), so procedure_stats' two plan readers resolve the dimension inline
    /// in their own SQL.</para>
    /// </summary>
    public static string GenerateQueryStatsResolvingView()
    {
        var schema = QueryStatsCollector.Instance;
        var dimensions = PayloadDimensions.ForTable(schema.TargetTable);
        var byColumn = dimensions.ToDictionary(d => d.PayloadColumn, StringComparer.Ordinal);
        /* char, not string: a one-character StringBuilder.Append(string) is a CA1834 warning, and the
           repo builds warnings-as-errors. */
        const char Fact = 'f';

        var sb = new StringBuilder();
        sb.Append("/* v_query_stats resolves each row's payload: the inline column for rows written before\n");
        sb.Append("   the dimension tables existed, the dimension row for every row written since (#1767).\n");
        sb.Append("   Column list generated from the collector definition so it can never go stale. */\n");
        sb.Append("CREATE OR REPLACE VIEW v_").Append(schema.TargetTable).Append(" AS\nSELECT\n");

        var columns = new List<string>();

        if (schema.IncludesCollectionId)
        {
            columns.Add($"    {Fact}.{schema.PrefixIdColumnName}");
        }

        columns.Add($"    {Fact}.{schema.PrefixTimeColumnName}");
        columns.Add($"    {Fact}.server_id");
        columns.Add($"    {Fact}.server_name");

        foreach (var column in schema.PayloadColumns)
        {
            columns.Add(byColumn.TryGetValue(column.Name, out var dimension)
                ? $"    COALESCE({Fact}.{column.Name}, {AliasFor(dimension.DimTable)}.{column.Name}) AS {column.Name}"
                : $"    {Fact}.{column.Name}");
        }

        /* Appended last: additions at the end are the only alteration CREATE OR REPLACE VIEW permits. */
        foreach (var dimension in dimensions)
        {
            columns.Add($"    {Fact}.{dimension.DigestColumn}");
        }

        /* #2069, after the digests (still a legal append-at-end): the gzip-compressed plan content.
           New rows carry bytes here and NULL query_plan_xml; readers take gz-else-text and inflate
           in C# — PostgreSQL cannot gunzip in SQL, so the view exposes the bytes rather than
           pretending to resolve them. */
        columns.Add($"    {AliasFor(PayloadDimensions.CompressedContentDimTable)}.{PayloadDimensions.CompressedContentColumn}");

        sb.Append(string.Join(",\n", columns)).Append('\n');
        sb.Append("FROM ").Append(schema.TargetTable).Append(" AS ").Append(Fact).Append('\n');

        foreach (var dimension in dimensions)
        {
            var alias = AliasFor(dimension.DimTable);
            sb.Append("LEFT JOIN ").Append(dimension.DimTable).Append(" AS ").Append(alias)
              .Append(" ON ").Append(alias).Append('.').Append(PayloadDimensions.DigestColumn)
              .Append(" = ").Append(Fact).Append('.').Append(dimension.DigestColumn).Append('\n');
        }

        sb.Append(";\n");
        return sb.ToString();
    }
}
