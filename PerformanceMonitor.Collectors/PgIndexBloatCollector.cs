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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// B-tree index bloat, MEASURED rather than estimated — <c>pgstatindex</c> from pgstattuple (#2561).
///
/// <para><b>Why exact and not an estimator.</b> #2561 proposed porting the ioguix btree estimator, which
/// derives an expected page count from <c>pg_stats</c> column widths. Measured, that route does not work for
/// the role this product runs as: <c>pg_stats</c> returns ZERO rows to a <c>pg_monitor</c>-only login,
/// because the view filters on <c>has_column_privilege</c> — the same trap that produced #2542's 88.59%
/// reported against a true 0.50%. Meanwhile <c>pgstatindex</c> DOES run for <c>pg_monitor</c> (verified),
/// because pgstattuple grants EXECUTE to <c>pg_stat_scan_tables</c>, which <c>pg_monitor</c> includes. So
/// under exactly the permissions we have, the exact function works and the estimator is blind.</para>
///
/// <para><b>The density is stored raw and NEVER converted to a bloat percentage.</b>
/// <c>100 - avg_leaf_density</c> is about 10% on a PERFECT index: measured across seven freshly-built
/// indexes, density landed between 89.98 and 91.48, and after <c>REINDEX</c> between 87.07 and 90.81. There
/// is no constant to subtract, so any stored "bloat %" would bake in a false floor that varies per index.
/// The server's own numbers are kept and the interpretation is left to the read, which is the same rule the
/// plan-readiness collector follows for GUC text.</para>
///
/// <para><b>Non-btree indexes must be excluded before the function is called, not filtered afterwards.</b>
/// <c>pgstatindex</c> RAISES on anything else — verified on GIN, BRIN and hash, all
/// <c>relation "x" is not a btree index</c> — so a single GIN index would take the whole collection down
/// every cycle. The btree filter sits behind an <c>OFFSET 0</c> optimisation fence: in testing the planner
/// applied a plain <c>WHERE</c> first and the naive form worked, but correctness here should not depend on
/// plan shape when the failure is total.</para>
///
/// <para><b>Nothing is silently skipped.</b> The cost of this function is a full read of the index —
/// measured at exactly <c>relpages</c> blocks, 62,840 for a 491 MB index — so very large indexes are passed
/// over rather than read daily. They still get a ROW, with the measurements NULL and
/// <c>skipped_reason</c> populated, because a size cap that made indexes disappear would read as "no bloat
/// here" on precisely the biggest ones.</para>
///
/// <para>Primaries only. A standby's index files are byte-identical to the primary's by replication, so
/// measuring both spends the same full-index read twice for one answer.</para>
/// </summary>
public sealed class PgIndexBloatCollector : PostgresCollectorDefinitionBase<PgIndexBloatCollector.Row>
{
    public static PgIndexBloatCollector Instance { get; } = new();

    private PgIndexBloatCollector()
    {
    }

    /// <summary>
    /// Indexes at or above this many bytes are recorded but not measured. The read is proportional to index
    /// size, so this bounds the daily cost; 20 GB is roughly 2.6 million blocks, a few seconds of I/O.
    /// </summary>
    public const long MeasureCeilingBytes = 20L * 1024 * 1024 * 1024;

    /// <param name="AvgLeafDensity">The server's own figure, 0–100. NOT a bloat percentage — a healthy
    /// index sits near 90, so subtracting from 100 invents roughly 10 points of bloat that is not there.</param>
    /// <param name="LeafFragmentation">Share of leaf pages out of physical order. Zero on a freshly built
    /// index; near 50 on the churned one measured while designing this.</param>
    /// <param name="EmptyPages">Pages holding nothing. Directly reclaimable by <c>REINDEX</c> and the most
    /// concrete number here.</param>
    /// <param name="DeletedPages">Pages marked deleted and awaiting reuse.</param>
    /// <param name="SkippedReason">Null when measured. Populated when the index was too large to read, so a
    /// cap can never masquerade as an absence of bloat.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        long IndexBytes,
        int? TreeLevel,
        long? InternalPages,
        long? LeafPages,
        long? EmptyPages,
        long? DeletedPages,
        double? AvgLeafDensity,
        double? LeafFragmentation,
        string? SkippedReason);

    /* The candidate set is fenced with OFFSET 0 so the btree filter and the size ceiling are applied BEFORE
       pgstatindex is called on anything. See the type header for why that matters more than usual here.

       LEFT JOIN LATERAL, not CROSS JOIN LATERAL: an index over the ceiling produces no function row and must
       still appear, carrying its skipped_reason. A cross join would drop exactly the rows a reader most
       wants to know about.

       System schemas are excluded - their indexes are not something an operator reindexes on our advice.

       pgstatindex is qualified public., NOT pg_catalog. - it is an EXTENSION function and lives wherever
       pgstattuple was created, which is public by convention. Qualifying it pg_catalog. simply does not
       resolve (verified: "function pg_catalog.pgstatindex(oid) does not exist"), and leaving it unqualified
       would let an object in an earlier search_path schema shadow it. An install that put pgstattuple
       somewhere else fails into ObjectMissing, which is the honest outcome and is exactly what
       pg_extension_availability (#2545) reports on. Same convention as PgBufferUsageCollector's
       public.pg_buffercache.

       The oid is cast to regclass because that is the parameter type; an unqualified oid does not match. */
    private const string QueryText = @"
WITH candidates AS (
    SELECT
        n.nspname                       AS schema_name,
        t.relname                       AS table_name,
        c.relname                       AS index_name,
        c.oid                           AS index_oid,
        pg_catalog.pg_relation_size(c.oid) AS index_bytes
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_am AS am
      ON am.oid = c.relam
    JOIN pg_catalog.pg_index AS x
      ON x.indexrelid = c.oid
    JOIN pg_catalog.pg_class AS t
      ON t.oid = x.indrelid
    JOIN pg_catalog.pg_namespace AS n
      ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
    AND   am.amname = 'btree'
    AND   x.indisvalid
    AND   x.indisready
    AND   n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    OFFSET 0
)
SELECT
    current_database()::text            AS database_name,
    k.schema_name::text                 AS schema_name,
    k.table_name::text                  AS table_name,
    k.index_name::text                  AS index_name,
    k.index_bytes::bigint               AS index_bytes,
    s.tree_level                        AS tree_level,
    s.internal_pages::bigint            AS internal_pages,
    s.leaf_pages::bigint                AS leaf_pages,
    s.empty_pages::bigint               AS empty_pages,
    s.deleted_pages::bigint             AS deleted_pages,
    s.avg_leaf_density::double precision   AS avg_leaf_density,
    s.leaf_fragmentation::double precision AS leaf_fragmentation,
    CASE
        WHEN k.index_bytes >= " + CeilingLiteral + @"
            THEN 'index is larger than the measurement ceiling; pgstatindex reads every page, so it is '
                 || 'recorded but not measured'
    END::text                           AS skipped_reason
FROM candidates AS k
LEFT JOIN LATERAL public.pgstatindex(k.index_oid::regclass) AS s
  ON k.index_bytes < " + CeilingLiteral + @"
ORDER BY k.index_bytes DESC";

    private const string CeilingLiteral = "21474836480";

    public override string Name => "pg_index_bloat";

    public override string TargetTable => "pg_index_bloat";

    /// <summary>
    /// Primaries only. A standby's index files are byte-identical to the primary's, so the answer is the
    /// same and the cost — a full read of every index — would be paid twice. Matches
    /// <see cref="PgIndexUsageStatsCollector"/>'s gate, which the two share a cadence with.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>Per-database: indexes and their catalogs are per-database.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* This collector runs once per database and pgstatindex measures the connected database only, so
           without this the same index name in two databases is one indistinguishable row (#2599). */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_bytes", CollectorColumnType.BigInt),
        /* Tree depth. A level that climbs on an index whose row count did not is a shape worth seeing, and
           it costs nothing to keep since the function already returned it. */
        new CollectorColumn("tree_level", CollectorColumnType.Integer),
        new CollectorColumn("internal_pages", CollectorColumnType.BigInt),
        new CollectorColumn("leaf_pages", CollectorColumnType.BigInt),
        /* The two concrete reclaimable numbers, and the only ones here that need no interpretation. */
        new CollectorColumn("empty_pages", CollectorColumnType.BigInt),
        new CollectorColumn("deleted_pages", CollectorColumnType.BigInt),
        /* Stored RAW. See the type header: a healthy index reads near 90, so this is not 100-minus-bloat. */
        new CollectorColumn("avg_leaf_density", CollectorColumnType.Double),
        new CollectorColumn("leaf_fragmentation", CollectorColumnType.Double),
        new CollectorColumn("skipped_reason", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                IndexName: reader.IsDBNull(3) ? null : reader.GetString(3),
                IndexBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                /* Every measurement is nullable because a skipped index has none of them, and NULL is the
                   honest representation of "not measured" where 0 would read as a measured emptiness. */
                TreeLevel: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                InternalPages: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                LeafPages: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                EmptyPages: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                DeletedPages: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                AvgLeafDensity: reader.IsDBNull(10) ? null : reader.GetDouble(10),
                LeafFragmentation: reader.IsDBNull(11) ? null : reader.GetDouble(11),
                SkippedReason: reader.IsDBNull(12) ? null : reader.GetString(12)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Bloat is a level, and the history is what distinguishes an index that has been bloated
           since it was built from one that got that way this month. */
        writer
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.IndexName)
            .Value(row.IndexBytes)
            .Value(row.TreeLevel)
            .Value(row.InternalPages)
            .Value(row.LeafPages)
            .Value(row.EmptyPages)
            .Value(row.DeletedPages)
            .Value(row.AvgLeafDensity)
            .Value(row.LeafFragmentation)
            .Value(row.SkippedReason);
    }
}
