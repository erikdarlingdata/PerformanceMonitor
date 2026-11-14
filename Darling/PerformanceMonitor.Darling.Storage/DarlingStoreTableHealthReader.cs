/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The health of the monitoring store's OWN tables (#8787), read live from the catalogs. One row per store table,
/// shared by the service (the <c>get_store_table_health</c> MCP tool and its web read) and by the Darling Viewer, which
/// reads the store directly.
///
/// <para><b>Why it exists.</b> #8570 found a plain store table whose file was about ten times its live data after
/// heavy delete churn. Autovacuum had kept dead rows low, so the dead-row count looked healthy, but the pages were
/// never returned and every read of the table paid for them. A rewrite fixed it. <c>get_store_metrics</c> covers
/// hypertables, continuous aggregates, the payload dimensions and background jobs, and plain tables not at all, and
/// nothing in the product said which table carried the empty space.</para>
///
/// <para><b>The empty-space figure is an ESTIMATE from table statistics</b>, and it is the same extension-free
/// estimate the PostgreSQL target table bloat collector runs on monitored targets (<see cref="PgTableBloatStatsCollector.QueryText"/>,
/// spliced in whole, not reimplemented), with the same trust rules (<see cref="DarlingPgTableBloatReader.UnpublishableConditionSql"/>):
/// when statistics are missing, stale or unusable the estimate is NULL with a reason, never zero. It is NOT
/// <c>n_live_tup / (n_live_tup + n_dead_tup)</c>, which reads 100 percent right after a vacuum and says nothing
/// about pages a vacuum could not return. A measured figure needs <c>pgstattuple</c>, which this read never
/// installs or calls.</para>
///
/// <para><b>Roll-ups.</b> A hypertable's chunks, and the relations holding a chunk's compressed data, roll up to
/// the hypertable (<see cref="KindHypertable"/>). A continuous aggregate's materialization hypertable rolls up to
/// the aggregate's own view name (<see cref="KindContinuousAggregate"/>), the way <c>StoreSelfMetrics</c> names it.
/// A partitioned table's leaf partitions roll up to the top parent (<see cref="KindPartitioned"/>, with
/// <see cref="StoreTableHealthRow.LeafCount"/> the leaf count, found from <c>relkind = 'p'</c> roots through
/// <c>pg_partition_tree</c>, so a sub-partitioned leaf is walked). Every other relation is its own row
/// (<see cref="KindTable"/>).</para>
///
/// <para><b>What the estimate covers for a roll-up.</b> Only a relation whose heap is at least
/// <see cref="EstimateMinimumHeapBytes"/> gets an estimate (the collector's floor), and the relations that hold
/// compressed data never do: their arithmetic means nothing. <see cref="StoreTableHealthRow.EstimateRelations"/>
/// counts the heap relations the estimate was tried on and
/// <see cref="StoreTableHealthRow.EstimateRelationsUsable"/> how many returned a usable one.
/// <see cref="StoreTableHealthRow.EstimatedEmptyBytes"/> sums the usable ones and
/// <see cref="StoreTableHealthRow.EstimatedEmptyShare"/> divides that by the usable relations' own catalog heap
/// size, so a roll-up's share is the share of the part that could be estimated, not of the whole object.</para>
/// </summary>
public static class DarlingStoreTableHealthReader
{
    /// <summary>A plain or partitioned table, or a materialized view (<see cref="StoreTableHealthRow.Kind"/>).</summary>
    public const string KindTable = "table";

    /// <summary>A declarative partitioned table (for example <c>collect.query_store_interval_wide</c>), its leaf
    /// partitions rolled up under the top parent. A leaf never appears as its own row.</summary>
    public const string KindPartitioned = "partitioned";

    /// <summary>A TimescaleDB hypertable, its chunks and compressed data rolled up.</summary>
    public const string KindHypertable = "hypertable";

    /// <summary>A continuous aggregate, named by its view, its materialization hypertable rolled up.</summary>
    public const string KindContinuousAggregate = "continuous_aggregate";

    /// <summary>The collector's heap floor: a relation under this gets no estimate, because under a megabyte the
    /// figure is not actionable (see <see cref="PgTableBloatStatsCollector.MinimumHeapBytes"/>).</summary>
    public const long EstimateMinimumHeapBytes = 1048576;

    /// <summary>
    /// A table is a rewrite candidate when its estimated empty share is at least this fraction AND its estimated
    /// empty bytes are at least <see cref="RewriteCandidateMinEmptyBytes"/>. Half is the line the target table bloat
    /// tool already grades Critical at (<c>DarlingMcpPgTableBloatTools</c>, 50 percent), so the two surfaces agree
    /// on what "most of this table is empty" means.
    /// </summary>
    public const double RewriteCandidateMinShare = 0.5;

    /// <summary>
    /// The bytes line a rewrite candidate must also pass: the scorer's own heap-size floor
    /// (<see cref="PgTargetScorer.BloatSizeFloorBytes"/>, 64 MiB), below which the target bloat reads speak no
    /// percentage at all. Reused rather than invented: an empty space under it is not worth a rewrite's lock.
    /// </summary>
    public const long RewriteCandidateMinEmptyBytes = PgTargetScorer.BloatSizeFloorBytes;

    /// <summary>The relation is under <see cref="EstimateMinimumHeapBytes"/>, so the estimate was not run.</summary>
    public const string ReasonBelowSizeFloor = "below_size_floor";

    /// <summary>PostgreSQL 14+ marks a table that was never analyzed with a negative row estimate.</summary>
    public const string ReasonNeverAnalyzed = "never_analyzed";

    /// <summary>No column statistics: never analyzed on an older server, a name-typed column, or the login lacks
    /// SELECT on the table so <c>pg_stats</c> filtered every row out.</summary>
    public const string ReasonNoColumnStatistics = "no_column_statistics";

    /// <summary>Column statistics exist but PostgreSQL lost the record of when they were taken (crash recovery,
    /// immediate shutdown, <c>pg_stat_reset()</c>), so whether they are fresh cannot be judged.</summary>
    public const string ReasonAnalyzeRecordLost = "analyze_record_lost";

    /// <summary>A fifth or more of the rows changed since the last analyze, so the column widths are out of date
    /// (<see cref="DarlingPgTableBloatReader.StaleStatisticsChurnRatio"/>).</summary>
    public const string ReasonStaleStatistics = "stale_statistics";

    /// <summary>A hypertable or aggregate with no chunks has nothing to estimate.</summary>
    public const string ReasonNoChunks = "no_chunks";

    /// <summary>
    /// One store table (or roll-up). <c>EstimatedEmpty*</c> are ESTIMATES from table statistics and are NULL, never
    /// zero, when no relation of the object had a usable estimate; <see cref="EstimateUnavailableReason"/> then says
    /// why (one of the <c>Reason*</c> constants). Sizes are measured: <c>TotalBytes = HeapBytes + IndexBytes +
    /// ToastBytes</c> over every relation of the object, compressed data included.
    /// </summary>
    /// <param name="SchemaName">The object's schema (<c>collect</c> or <c>config</c> on a product store).</param>
    /// <param name="TableName">The table, hypertable, or the aggregate's view name.</param>
    /// <param name="Kind"><see cref="KindTable"/>, <see cref="KindPartitioned"/>, <see cref="KindHypertable"/> or <see cref="KindContinuousAggregate"/>.</param>
    /// <param name="TotalBytes">Heap, indexes and TOAST, measured from the files.</param>
    /// <param name="HeapBytes">The heap files only.</param>
    /// <param name="IndexBytes">Every index of every relation of the object.</param>
    /// <param name="ToastBytes">The TOAST tables and their indexes.</param>
    /// <param name="EstimatedRows">The planner's row estimate (<c>reltuples</c>) summed over the object's UNCOMPRESSED heap
    /// relations that have one. Rows inside compressed chunks are not counted, so a compressed hypertable reads low.
    /// NULL when no uncompressed relation has been analyzed or vacuumed.</param>
    /// <param name="ChunkCount">Chunks of a hypertable or aggregate, 0 for every other kind.</param>
    /// <param name="LeafCount">Leaf partitions of a partitioned table, 0 for every other kind.</param>
    /// <param name="EstimatedEmptyBytes">Estimated bytes of pages that hold no live row, summed over the relations with
    /// a usable estimate. NULL when none has one.</param>
    /// <param name="EstimatedEmptyShare">Estimated empty bytes over the same relations' catalog heap bytes, 0 to 1.
    /// NULL when <see cref="EstimatedEmptyBytes"/> is.</param>
    /// <param name="EstimateRelations">How many heap relations the estimate was tried on.</param>
    /// <param name="EstimateRelationsUsable">How many of them returned a usable estimate.</param>
    /// <param name="EstimateUnavailableReason">NULL when the object has an estimate. Otherwise the most actionable
    /// reason among its relations (a <c>Reason*</c> constant).</param>
    /// <param name="DeadRows">Dead tuples the statistics collector counts, summed.</param>
    /// <param name="RowsChangedSinceAnalyze">Rows inserted, updated or deleted since the last analyze, summed.</param>
    /// <param name="LastVacuum">Latest manual vacuum of any relation of the object (UTC), or NULL.</param>
    /// <param name="LastAutovacuum">Latest autovacuum (UTC), or NULL.</param>
    /// <param name="LastAnalyze">Latest manual analyze (UTC), or NULL.</param>
    /// <param name="LastAutoanalyze">Latest autoanalyze (UTC), or NULL.</param>
    /// <param name="AutovacuumDisabled">True when any relation of the object sets <c>autovacuum_enabled</c> off.</param>
    /// <param name="GrowthBytes7d">Latest minus earliest <c>total_bytes</c> recorded in <c>collect.store_metrics</c>
    /// in the last 7 days, over rows of one sizing form (estimate or exact). NULL when the object is not recorded
    /// there or has fewer than two rows in the window.</param>
    /// <param name="RewriteCandidate">True when <see cref="EstimatedEmptyShare"/> is at least
    /// <see cref="RewriteCandidateMinShare"/> and <see cref="EstimatedEmptyBytes"/> at least
    /// <see cref="RewriteCandidateMinEmptyBytes"/>.</param>
    public sealed record StoreTableHealthRow(
        string SchemaName,
        string TableName,
        string Kind,
        long TotalBytes,
        long HeapBytes,
        long IndexBytes,
        long ToastBytes,
        long? EstimatedRows,
        int ChunkCount,
        int LeafCount,
        long? EstimatedEmptyBytes,
        double? EstimatedEmptyShare,
        int EstimateRelations,
        int EstimateRelationsUsable,
        string? EstimateUnavailableReason,
        long DeadRows,
        long RowsChangedSinceAnalyze,
        DateTime? LastVacuum,
        DateTime? LastAutovacuum,
        DateTime? LastAnalyze,
        DateTime? LastAutoanalyze,
        bool AutovacuumDisabled,
        long? GrowthBytes7d,
        bool RewriteCandidate);

    /// <summary>The roll-up the header of every surface reads (<see cref="Summarize"/>).</summary>
    /// <param name="TableCount">Rows read.</param>
    /// <param name="TotalBytes">Sum of the rows' <c>TotalBytes</c>.</param>
    /// <param name="RewriteCandidates">Rows flagged <see cref="StoreTableHealthRow.RewriteCandidate"/>.</param>
    /// <param name="WithoutEstimate">Rows with no usable estimate.</param>
    /// <param name="WithoutEstimateByReason">Those rows, counted by <see cref="StoreTableHealthRow.EstimateUnavailableReason"/>,
    /// most common first.</param>
    public sealed record StoreTableHealthSummary(
        int TableCount,
        long TotalBytes,
        int RewriteCandidates,
        int WithoutEstimate,
        IReadOnlyList<KeyValuePair<string, int>> WithoutEstimateByReason);

    /// <summary>Counts the rows for a header line.</summary>
    public static StoreTableHealthSummary Summarize(IReadOnlyList<StoreTableHealthRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var byReason = rows
            .Where(r => r.EstimatedEmptyBytes is null)
            .GroupBy(r => r.EstimateUnavailableReason ?? "unknown", StringComparer.Ordinal)
            .Select(g => new KeyValuePair<string, int>(g.Key, g.Count()))
            .OrderByDescending(p => p.Value)
            .ThenBy(p => p.Key, StringComparer.Ordinal)
            .ToList();
        return new StoreTableHealthSummary(
            rows.Count,
            rows.Sum(r => r.TotalBytes),
            rows.Count(r => r.RewriteCandidate),
            byReason.Sum(p => p.Value),
            byReason);
    }

    /// <summary>A plain-language phrase for each <c>Reason*</c> value, for headers and tooltips.</summary>
    public static string DescribeReason(string reason) => reason switch
    {
        ReasonBelowSizeFloor => "heap under 1 MB, so no estimate is made",
        ReasonNeverAnalyzed => "never analyzed",
        ReasonNoColumnStatistics => "no column statistics (never analyzed, or this login cannot read them)",
        ReasonAnalyzeRecordLost => "the record of the last analyze is gone, so freshness cannot be judged",
        ReasonStaleStatistics => "statistics are stale (a fifth or more of the rows changed since the last analyze)",
        ReasonNoChunks => "no chunks yet",
        _ => reason,
    };

    /// <summary>
    /// Whether TimescaleDB is installed in the store's database, from <c>pg_extension</c>. A store the operator
    /// brought themselves may be plain PostgreSQL, where the chunk and aggregate catalogs do not exist.
    /// </summary>
    public const string TimescaleProbeSql =
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'timescaledb')";

    /// <summary>
    /// The statement for a TimescaleDB store: hypertables and aggregates by their public catalog views, chunks from
    /// <c>timescaledb_information.chunks</c>, compressed data through <c>compression_settings.compress_relid</c> (the
    /// two catalogs <see cref="StoreSelfMetrics.TimescaleInventoriedPredicateSql"/> documents as identical on the
    /// 2.28 and 2.30 shapes), and the plain tables by the same predicate that keeps every one of those relations out.
    /// </summary>
    public static readonly string TimescaleSql = BuildSql(timescale: true);

    /// <summary>The statement for a store without TimescaleDB: plain and partitioned tables only.</summary>
    public static readonly string PlainSql = BuildSql(timescale: false);

    private const string TimescaleTargetsCtes = @"targets AS (
    SELECT h.hypertable_schema AS obj_schema, h.hypertable_name AS obj_name, 'hypertable'::text AS kind,
           h.hypertable_schema AS root_schema, h.hypertable_name AS root_name
    FROM timescaledb_information.hypertables AS h
    UNION ALL
    SELECT ca.view_schema, ca.view_name, 'continuous_aggregate'::text,
           ca.materialization_hypertable_schema, ca.materialization_hypertable_name
    FROM timescaledb_information.continuous_aggregates AS ca
),
root_rels AS (
    SELECT t.obj_schema, t.obj_name, t.kind, pc.oid AS rel_oid
    FROM targets AS t
    JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = t.root_schema
    JOIN pg_catalog.pg_class AS pc ON pc.relnamespace = pn.oid AND pc.relname = t.root_name
),
chunk_rels AS (
    SELECT t.obj_schema, t.obj_name, t.kind, pc.oid AS rel_oid,
           EXISTS (SELECT 1 FROM _timescaledb_catalog.compression_settings AS cs
                   WHERE cs.relid::oid = pc.oid AND cs.compress_relid IS NOT NULL) AS is_compressed
    FROM targets AS t
    JOIN timescaledb_information.chunks AS ch
        ON  ch.hypertable_schema = t.root_schema
        AND ch.hypertable_name = t.root_name
    JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = ch.chunk_schema
    JOIN pg_catalog.pg_class AS pc ON pc.relnamespace = pn.oid AND pc.relname = ch.chunk_name
),
";

    private const string TimescaleMemberArms = @"    SELECT obj_schema, obj_name, kind, rel_oid, 'root'::text AS member_role, false AS grouped, false AS is_compressed FROM root_rels
    UNION ALL
    SELECT obj_schema, obj_name, kind, rel_oid, 'heap', true, is_compressed FROM chunk_rels
    UNION ALL
    SELECT r.obj_schema, r.obj_name, r.kind, cs.compress_relid::oid, 'compressed', true, true
    FROM chunk_rels AS r
    JOIN _timescaledb_catalog.compression_settings AS cs ON cs.relid::oid = r.rel_oid
    WHERE cs.compress_relid IS NOT NULL
    UNION ALL
";

    private static string BuildSql(bool timescale) => $@"
WITH est AS MATERIALIZED (
{PgTableBloatStatsCollector.QueryText}
),
{(timescale ? TimescaleTargetsCtes : string.Empty)}plain_roots AS (
    SELECT c.oid, n.nspname, c.relname, c.relkind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'm', 'p')
    AND   NOT c.relispartition
    AND   NOT c.relisshared
    AND   c.relpersistence <> 't'
    AND   NOT {StoreSelfMetrics.SystemSchemaPredicateSql}{(timescale ? "\n    AND   " + StoreSelfMetrics.TimescaleInventoriedPredicateSql : string.Empty)}
),
members AS (
{(timescale ? TimescaleMemberArms : string.Empty)}    SELECT pr.nspname AS obj_schema, pr.relname AS obj_name, CASE WHEN pr.relkind = 'p' THEN 'partitioned' ELSE 'table' END AS kind, x.rel_oid AS rel_oid, 'heap'::text AS member_role, (pr.relkind = 'p') AS grouped, false AS is_compressed
    FROM plain_roots AS pr
    CROSS JOIN LATERAL (
        SELECT pr.oid AS rel_oid WHERE pr.relkind <> 'p'
        UNION ALL
        SELECT t.relid FROM pg_catalog.pg_partition_tree(pr.oid::regclass) AS t WHERE pr.relkind = 'p' AND t.isleaf
    ) AS x
),
sized AS (
    SELECT
        m.obj_schema,
        m.obj_name,
        m.kind,
        m.member_role,
        m.grouped,
        m.is_compressed,
        pg_catalog.pg_relation_size(c.oid) AS heap_bytes,
        pg_catalog.pg_indexes_size(c.oid) AS index_bytes,
        COALESCE(pg_catalog.pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0) AS toast_bytes,
        c.reltuples,
        EXISTS (
            SELECT 1 FROM pg_catalog.unnest(c.reloptions) AS o(opt)
            WHERE o.opt ~* '^autovacuum_enabled=(f|fa|fal|fals|false|of|off|n|no|0)$') AS autovacuum_disabled,
        st.n_dead_tup,
        st.n_mod_since_analyze,
        st.last_vacuum,
        st.last_autovacuum,
        st.last_analyze,
        st.last_autoanalyze,
        (l.table_name IS NOT NULL AND NOT ({DarlingPgTableBloatReader.UnpublishableConditionSql})) AS usable,
        CASE WHEN l.table_name IS NULL THEN 1
             WHEN l.live_tuples < 0 THEN 2
             WHEN l.estimate_unavailable THEN 3
             WHEN l.last_analyzed IS NULL THEN 4
             ELSE 5 END AS reason_rank,
        l.bloat_bytes_estimate,
        l.heap_pages * pg_catalog.current_setting('block_size')::bigint AS catalog_heap_bytes
    FROM members AS m
    JOIN pg_catalog.pg_class AS c ON c.oid = m.rel_oid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_stat_all_tables AS st ON st.relid = c.oid
    LEFT JOIN est AS l ON m.member_role = 'heap' AND l.schema_name = n.nspname AND l.table_name = c.relname
),
agg AS (
    SELECT
        s.obj_schema,
        s.obj_name,
        s.kind,
        sum(s.heap_bytes + s.index_bytes + s.toast_bytes)::bigint AS total_bytes,
        sum(s.heap_bytes)::bigint AS heap_bytes,
        sum(s.index_bytes)::bigint AS index_bytes,
        sum(s.toast_bytes)::bigint AS toast_bytes,
        sum(s.reltuples) FILTER (WHERE s.member_role = 'heap' AND NOT s.is_compressed AND s.reltuples >= 0)::bigint AS estimated_rows,
        (count(*) FILTER (WHERE s.member_role = 'heap' AND s.grouped AND s.kind <> 'partitioned'))::int AS chunk_count,
        (count(*) FILTER (WHERE s.member_role = 'heap' AND s.grouped AND s.kind = 'partitioned'))::int AS leaf_count,
        (count(*) FILTER (WHERE s.member_role = 'heap'))::int AS estimate_relations,
        (count(*) FILTER (WHERE s.usable))::int AS estimate_relations_usable,
        sum(s.bloat_bytes_estimate) FILTER (WHERE s.usable)::bigint AS empty_bytes,
        sum(s.catalog_heap_bytes) FILTER (WHERE s.usable)::bigint AS basis_bytes,
        max(s.reason_rank) FILTER (WHERE s.member_role = 'heap' AND NOT s.usable) AS reason_rank,
        COALESCE(sum(s.n_dead_tup), 0)::bigint AS dead_rows,
        COALESCE(sum(s.n_mod_since_analyze), 0)::bigint AS rows_changed,
        (max(s.last_vacuum) AT TIME ZONE 'UTC') AS last_vacuum,
        (max(s.last_autovacuum) AT TIME ZONE 'UTC') AS last_autovacuum,
        (max(s.last_analyze) AT TIME ZONE 'UTC') AS last_analyze,
        (max(s.last_autoanalyze) AT TIME ZONE 'UTC') AS last_autoanalyze,
        bool_or(s.autovacuum_disabled) AS autovacuum_disabled
    FROM sized AS s
    GROUP BY s.obj_schema, s.obj_name, s.kind
),
recent AS (
    SELECT
        CASE m.object_kind WHEN 'dimension' THEN 'table' ELSE m.object_kind END AS kind,
        CASE WHEN m.object_kind IN ('table') THEN m.object_name ELSE 'collect.' || m.object_name END AS qualified_name,
        m.total_bytes,
        m.metric_time,
        (m.size_estimated IS TRUE) AS is_estimated,
        first_value(m.size_estimated IS TRUE) OVER (PARTITION BY m.object_kind, m.object_name ORDER BY m.metric_time DESC) AS newest_is_estimated
    FROM collect.store_metrics AS m
    WHERE m.metric_time >= (pg_catalog.now() AT TIME ZONE 'UTC') - interval '7 days'
    AND   m.object_kind IN ('hypertable', 'continuous_aggregate', 'table', 'dimension')
    AND   m.total_bytes IS NOT NULL
),
growth AS (
    SELECT
        r.kind,
        r.qualified_name,
        (array_agg(r.total_bytes ORDER BY r.metric_time DESC))[1] - (array_agg(r.total_bytes ORDER BY r.metric_time))[1] AS growth_bytes
    FROM recent AS r
    WHERE r.is_estimated = r.newest_is_estimated
    GROUP BY r.kind, r.qualified_name
    HAVING count(*) >= 2
)
SELECT
    a.obj_schema,
    a.obj_name,
    a.kind,
    a.total_bytes,
    a.heap_bytes,
    a.index_bytes,
    a.toast_bytes,
    a.estimated_rows,
    a.chunk_count,
    a.leaf_count,
    a.empty_bytes,
    CASE WHEN a.empty_bytes IS NOT NULL AND a.basis_bytes > 0 THEN a.empty_bytes::double precision / a.basis_bytes END AS empty_share,
    a.estimate_relations,
    a.estimate_relations_usable,
    CASE WHEN a.estimate_relations_usable > 0 THEN NULL
         WHEN a.estimate_relations = 0 THEN '{ReasonNoChunks}'
         WHEN a.reason_rank = 1 THEN '{ReasonBelowSizeFloor}'
         WHEN a.reason_rank = 2 THEN '{ReasonNeverAnalyzed}'
         WHEN a.reason_rank = 3 THEN '{ReasonNoColumnStatistics}'
         WHEN a.reason_rank = 4 THEN '{ReasonAnalyzeRecordLost}'
         ELSE '{ReasonStaleStatistics}' END AS unavailable_reason,
    a.dead_rows,
    a.rows_changed,
    a.last_vacuum,
    a.last_autovacuum,
    a.last_analyze,
    a.last_autoanalyze,
    COALESCE(a.autovacuum_disabled, false) AS autovacuum_disabled,
    g.growth_bytes,
    (a.empty_bytes IS NOT NULL
     AND a.basis_bytes > 0
     AND a.empty_bytes::double precision / a.basis_bytes >= {RewriteCandidateMinShare.ToString("R", CultureInfo.InvariantCulture)}
     AND a.empty_bytes >= {RewriteCandidateMinEmptyBytes.ToString(CultureInfo.InvariantCulture)}) AS rewrite_candidate
FROM agg AS a
LEFT JOIN growth AS g ON g.kind = CASE a.kind WHEN 'partitioned' THEN 'table' ELSE a.kind END AND g.qualified_name = a.obj_schema || '.' || a.obj_name
ORDER BY rewrite_candidate DESC, a.empty_bytes DESC NULLS LAST, a.total_bytes DESC, a.obj_schema, a.obj_name";

    /// <summary>
    /// Reads every store table's health, in the order the surfaces show it: rewrite candidates first, then
    /// estimated empty bytes descending (rows with no estimate last), then total bytes descending. Two statements: the
    /// TimescaleDB probe and the read. The read is bounded by <see cref="StorageCommandDeadlines.McpReadSeconds"/>.
    /// </summary>
    public static async Task<List<StoreTableHealthRow>> GetAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        bool timescale;
        await using (var probe = postgres.CreateCommand(TimescaleProbeSql))
        {
            probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            timescale = (bool)(await probe.ExecuteScalarAsync(cancellationToken))!;
        }

        var rows = new List<StoreTableHealthRow>();
        await using var command = postgres.CreateCommand(timescale ? TimescaleSql : PlainSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreTableHealthRow(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                Kind: reader.GetString(2),
                TotalBytes: reader.GetInt64(3),
                HeapBytes: reader.GetInt64(4),
                IndexBytes: reader.GetInt64(5),
                ToastBytes: reader.GetInt64(6),
                EstimatedRows: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                ChunkCount: reader.GetInt32(8),
                LeafCount: reader.GetInt32(9),
                EstimatedEmptyBytes: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                EstimatedEmptyShare: reader.IsDBNull(11) ? null : reader.GetDouble(11),
                EstimateRelations: reader.GetInt32(12),
                EstimateRelationsUsable: reader.GetInt32(13),
                EstimateUnavailableReason: reader.IsDBNull(14) ? null : reader.GetString(14),
                DeadRows: reader.GetInt64(15),
                RowsChangedSinceAnalyze: reader.GetInt64(16),
                LastVacuum: Utc(reader, 17),
                LastAutovacuum: Utc(reader, 18),
                LastAnalyze: Utc(reader, 19),
                LastAutoanalyze: Utc(reader, 20),
                AutovacuumDisabled: reader.GetBoolean(21),
                GrowthBytes7d: reader.IsDBNull(22) ? null : reader.GetInt64(22),
                RewriteCandidate: reader.GetBoolean(23)));
        }

        return rows;
    }

    private static DateTime? Utc(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);
}
