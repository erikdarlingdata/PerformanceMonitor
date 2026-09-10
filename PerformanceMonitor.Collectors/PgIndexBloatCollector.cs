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
/// B-tree index bloat, ESTIMATED from catalog statistics — no page reads (#3234).
///
/// <para><b>Why this replaced the exact census, and it is a product decision rather than a tuning one.</b>
/// <c>pgstatindex</c> reads every page of the index it is pointed at. It is the direct analogue of SQL
/// Server's <c>sys.dm_db_index_physical_stats</c> in <c>DETAILED</c> mode, and nobody schedules
/// <c>DETAILED</c> against production: you run <c>LIMITED</c> routinely and reach for <c>DETAILED</c> on
/// the one index you are about to act on. This collector is <c>LIMITED</c>. The exact function is still
/// the right answer on request, and every row carries the command that produces it.</para>
///
/// <para><b>Measured, on a live Aurora PostgreSQL 17.7 target with 2,500 btree indexes over 461 GB.</b>
/// The exact census took <b>117,740 and 136,953 ms</b> on consecutive days and covered <b>1 GiB per run</b>,
/// so a complete pass took roughly <b>150 days</b> at daily cadence. This statement runs in
/// <b>1,662 / 1,695 / 2,118 ms</b> — and that figure was taken across an SSM tunnel, including connect,
/// authentication and shipping all 2,500 rows, so the server-side cost is lower again. It covers
/// <b>214 GB in one run</b> instead of 1 GiB.</para>
///
/// <para><b>The part that is not a trade at all.</b> 74 of those indexes are at or above the 1 GiB
/// per-index ceiling the exact path had to carry, totalling <b>351 GB — 76 percent of the whole index
/// footprint — which pgstatindex could never measure at ANY cadence.</b> This collector estimates 32 of
/// them, <b>188 GB</b>, which is 41 percent of the footprint that was previously dark. So the estimate is
/// not a cheaper substitute for the exact path; it is the only instrument that ever sees the largest
/// three quarters of the indexes.</para>
///
/// <para><b>Accuracy, against pgstatindex ground truth on the same target.</b> Compared as
/// <c>fillfactor - avg_leaf_density</c> and NOT as <c>100 - avg_leaf_density</c>: a pristine
/// fillfactor-90 index reads 89.98 to 91.48, so the latter invents about ten points of bloat on a perfect
/// index and there is no constant to subtract (#2561). On 60 indexes of 1 MB and above, mean error
/// <b>+1.38pp</b>, median absolute <b>2.79pp</b>, p90 <b>6.63pp</b>. Across 90 indexes of all sizes,
/// median absolute 3.83pp and 89 percent within 10pp.</para>
///
/// <para><b>The tuple-width model, because two wrong versions of it were measured before this one.</b>
/// A btree leaf tuple is <c>MAXALIGN(8 + null bitmap)</c> of header plus the per-attribute-aligned key
/// data, and each item also costs a 4-byte line pointer on the page. The subtlety is the null discount:
/// <c>pg_stats.avg_width</c> describes the entries that EXIST, so an attribute present in a fraction of
/// tuples contributes its ALIGNED width in that fraction of them. Discounting before aligning the summed
/// width — which is what the ioguix query does — collapsed near-fully-null columns to nothing and measured
/// a p90 error of <b>40.57pp</b>. Not discounting at all over-counted them and suppressed 901 indexes as
/// negative bloat. Applying the discount to the aligned per-attribute width is the formulation kept here
/// and it is the best on both axes.</para>
///
/// <para><b>Non-key INCLUDE columns are part of the width.</b> A leaf tuple stores them, so the scan runs
/// to <c>indnatts</c> rather than <c>indnkeyatts</c>. Omitting them was worth several points.</para>
///
/// <para><b>What it cannot answer, and says so per row rather than guessing.</b> Deduplicated indexes are
/// the big one: PostgreSQL 13+ stores duplicate keys once in a posting list, so real storage is denser
/// than per-tuple arithmetic allows and a CORRECT model still over-predicts. Measured, that cohort is 2.4
/// percent unique with a median leading-column distinctness of 0.0018, against 56.7 percent and 0.9994 for
/// the cohort that estimates cleanly — so the discriminator is real and the suppression is structural
/// rather than a bug. Partial indexes cannot be modelled either, because the row count available is the
/// parent's. Both fall out as NEGATIVE bloat, which is the cheap and reliable signal (#2561), and both are
/// exactly what the on-request exact path is for.</para>
///
/// <para><b>Two hypotheses were tested and refuted</b>, recorded so nobody re-runs them: stale
/// <c>reltuples</c> (the implied-over-actual ratio does track the error, but <c>stats_reset</c> is never on
/// that target and autovacuum reports every table under its own analyze threshold) and dead index entries
/// (<c>n_dead_tup</c> over <c>n_live_tup</c> measured 0.0021 and 0.0000 across both cohorts).</para>
///
/// <para><b>It needs no extension.</b> The exact path required pgstattuple, and a database without it
/// failed the whole collection for that database. This reads only <c>pg_class</c>, <c>pg_index</c>,
/// <c>pg_attribute</c>, <c>pg_namespace</c> and <c>pg_stats</c>. It does still REPORT whether pgstattuple
/// is present, because that decides whether the escalation command needs a <c>CREATE EXTENSION</c> in
/// front of it — the same thing <c>PgTableBloatStatsCollector</c> does.</para>
///
/// <para><b>It does need the pg_stats grant.</b> <c>pg_stats</c> filters on
/// <c>has_column_privilege</c> and <c>pg_monitor</c> does not confer SELECT, so a monitoring role without
/// <c>pg_read_all_data</c> sees zero column widths and every index reports the widths-not-visible reason
/// rather than a number. That is the runbook step under "The one grant pg_monitor does not cover", and it
/// is why #2561 rejected an estimator when the only grant on offer was pg_monitor. Measured on the first
/// production target after granting it: <c>pg_stats</c> went from 0 rows to 20,950.</para>
///
/// <para><b>No stored bloat percentage is derived from a density.</b> The estimate is a page-count
/// comparison, and the raw inputs it rests on — index pages, modelled tuple bytes, modelled leaf pages,
/// fillfactor and the parent row count — are all stored beside it so the number can be argued with rather
/// than believed. The read ranks by reclaimable BYTES and never by percentage, because a 64 kB index at 20
/// percent tops a percentage-ranked list and is worth 50 kB next to a 10 GB index at 45 percent worth 5.37
/// GB (#2561). That ranking also disposes of the small-index problem for free: 8 of the 10 worst outliers
/// in validation had between 2,000 and 8,000 rows, where page-count rounding dominates the percentage, and
/// every one of them sorts to the bottom on bytes.</para>
///
/// <para><b>The pgstatindex columns are retained and written NULL.</b> The store holds 90 days of exact
/// measurements and the migration for this change only ADDS columns, so that history stays readable and an
/// on-request measurement has somewhere to land. A NULL there means this row was estimated, not that an
/// exact measurement came back empty.</para>
/// </summary>
public sealed class PgIndexBloatCollector : PostgresCollectorDefinitionBase<PgIndexBloatCollector.Row>
{
    private PgIndexBloatCollector()
    {
    }

    public static PgIndexBloatCollector Instance { get; } = new();

    /// <summary>
    /// PostgreSQL's compile-time page size. Read from <c>current_setting('block_size')</c> in the statement
    /// rather than assumed, because a source build can change it; this constant exists for the C# side to
    /// convert a stored page count back to bytes and is pinned against the SQL by
    /// <c>ThePageArithmetic_MatchesPostgresPageLayout</c>.
    /// </summary>
    public const int BlockSizeBytes = 8192;

    /// <summary>Fixed page header, <c>PageHeaderData</c>. 24 bytes on every supported version.</summary>
    public const int PageHeaderBytes = 24;

    /// <summary>
    /// The btree-specific area at the end of every page, <c>BTPageOpaqueData</c>. 16 bytes. Together with
    /// <see cref="PageHeaderBytes"/> this is what makes a leaf page hold 8,152 usable bytes rather than
    /// 8,192, before fillfactor is applied.
    /// </summary>
    public const int BtreeSpecialAreaBytes = 16;

    /// <summary>Per-item line pointer, <c>ItemIdData</c>. Charged once per index tuple.</summary>
    public const int LinePointerBytes = 4;

    /// <summary>
    /// Index tuple header, <c>IndexTupleData</c>, before the null bitmap and before MAXALIGN.
    /// </summary>
    public const int IndexTupleHeaderBytes = 8;

    /// <summary>
    /// The fillfactor a btree gets when the index declares none. PostgreSQL's default is 90, and it is the
    /// reason a perfect index is about 10 percent free rather than 0 — which is why that 10 percent must
    /// never be read as bloat.
    /// </summary>
    public const int DefaultBtreeFillfactor = 90;

    /// <param name="IndexPages"><c>relpages</c> for the index: the measured denominator the estimate is a
    /// comparison against. Stored so a reader can recompute the percentage.</param>
    /// <param name="TableRows"><c>reltuples</c> for the PARENT table. <c>-1</c> is PostgreSQL 14+'s
    /// explicit never-analyzed marker and is why that case gets its own reason rather than a number.</param>
    /// <param name="EstTupleBytes">The modelled leaf tuple: aligned header plus null bitmap plus the
    /// per-attribute-aligned, null-weighted key and INCLUDE data. The single most load-bearing input.</param>
    /// <param name="EstLeafPages">Pages the modelled tuples would need at this fillfactor.</param>
    /// <param name="EstBloatPct">NULL whenever <paramref name="SkippedReason"/> is populated, so a
    /// suppressed estimate can never be read as zero bloat.</param>
    /// <param name="EstReclaimableBytes">The figure reads rank on. Bytes, never percentage.</param>
    /// <param name="SkippedReason">NULL when estimated. Populated with the reason and, where one exists,
    /// the remedy — an ANALYZE, a grant, or the exact command.</param>
    /// <param name="PgstattupleAvailable">Whether the escalation command needs a CREATE EXTENSION in front
    /// of it. Per-database, because <c>pg_extension</c> is per-database (#2599).</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        long IndexBytes,
        long IndexPages,
        long TableRows,
        int Fillfactor,
        long EstTupleBytes,
        long EstLeafPages,
        double? EstBloatPct,
        long? EstReclaimableBytes,
        string? SkippedReason,
        bool PgstattupleAvailable,
        long IndexOid);

    private const string QueryText = @"WITH params AS (
    SELECT
        pg_catalog.current_setting('block_size')::int AS bs,
        CASE WHEN pg_catalog.version() ~ '64-bit|x86_64|ppc64|ia64|amd64|aarch64|arm64'
             THEN 8 ELSE 4 END                        AS maxalign
),
idx AS (
    SELECT
        c.oid                                         AS index_oid,
        n.nspname                                     AS schema_name,
        t.relname                                     AS table_name,
        c.relname                                     AS index_name,
        t.oid                                         AS table_oid,
        c.relpages::bigint                            AS index_pages,
        pg_catalog.pg_relation_size(c.oid)            AS index_bytes,
        t.reltuples                                   AS table_rows,
        x.indnatts,
        x.indkey,
        x.indpred  IS NOT NULL                        AS is_partial,
        COALESCE(SUBSTRING(pg_catalog.array_to_string(c.reloptions, ' ')
                           FROM 'fillfactor=([0-9]+)')::int, 90) AS fillfactor
    FROM pg_catalog.pg_class     AS c
    JOIN pg_catalog.pg_am        AS am ON am.oid = c.relam
    JOIN pg_catalog.pg_index     AS x  ON x.indexrelid = c.oid
    JOIN pg_catalog.pg_class     AS t  ON t.oid = x.indrelid
    JOIN pg_catalog.pg_namespace AS n  ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
    AND   am.amname = 'btree'
    AND   x.indisvalid
    AND   x.indisready
    AND   n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    OFFSET 0
),
keycols AS (
    SELECT
        i.index_oid,
        ia.attnum                                     AS key_pos,
        COALESCE(pa.atttypid, ia.atttypid)            AS typid,
        COALESCE(sp.avg_width, se.avg_width)          AS avg_width,
        COALESCE(sp.null_frac, se.null_frac)          AS null_frac
    FROM idx AS i
    JOIN pg_catalog.pg_attribute AS ia
      ON  ia.attrelid = i.index_oid
      AND ia.attnum BETWEEN 1 AND i.indnatts
      AND NOT ia.attisdropped
    LEFT JOIN pg_catalog.pg_attribute AS pa
      ON  pa.attrelid = i.table_oid
      AND pa.attnum   = i.indkey[ia.attnum - 1]
      AND i.indkey[ia.attnum - 1] > 0
    LEFT JOIN pg_catalog.pg_stats AS sp
      ON  sp.schemaname = i.schema_name
      AND sp.tablename  = i.table_name
      AND sp.attname    = pa.attname
      AND sp.inherited  = false
    LEFT JOIN pg_catalog.pg_stats AS se
      ON  i.indkey[ia.attnum - 1] = 0
      AND se.schemaname = i.schema_name
      AND se.tablename  = i.index_name
      AND se.attname    = ia.attname
      AND se.inherited  = false
),
agg AS (
    SELECT
        i.index_oid, i.schema_name, i.table_name, i.index_name,
        i.index_pages, i.index_bytes, i.table_rows, i.is_partial, i.fillfactor,
        p.bs, p.maxalign,
        COUNT(k.key_pos)                                          AS key_count,
        COUNT(k.avg_width)                                        AS widths_known,
        BOOL_OR(k.typid = 'pg_catalog.name'::regtype)             AS has_name_typed,
        BOOL_OR(COALESCE(k.null_frac, 0) > 0)                     AS any_nullable,
        SUM((1 - COALESCE(k.null_frac, 0))
            * CASE WHEN COALESCE(k.avg_width, 0) = 0 THEN 0
                   ELSE p.maxalign
                        * CEIL(COALESCE(k.avg_width, 0)::numeric / p.maxalign) END) AS aligned_data_bytes
    FROM idx AS i
    CROSS JOIN params AS p
    LEFT JOIN keycols AS k ON k.index_oid = i.index_oid
    GROUP BY i.index_oid, i.schema_name, i.table_name, i.index_name,
             i.index_pages, i.index_bytes, i.table_rows, i.is_partial, i.fillfactor,
             p.bs, p.maxalign
),
calc AS (
    SELECT
        a.*,
        (a.maxalign * CEIL((8 + CASE WHEN a.any_nullable
                                     THEN CEIL(a.key_count / 8.0)
                                     ELSE 0 END)::numeric / a.maxalign)
         + a.aligned_data_bytes)::numeric              AS est_tuple_bytes,
        ((a.bs - 24 - 16) * a.fillfactor / 100.0)::numeric AS usable_page_bytes
    FROM agg AS a
),
final AS (
    SELECT
        c.*,
        CEIL(c.table_rows * (c.est_tuple_bytes + 4) / c.usable_page_bytes)::bigint AS est_leaf_pages
    FROM calc AS c
)
SELECT
    pg_catalog.current_database()::text               AS database_name,
    f.schema_name::text                               AS schema_name,
    f.table_name::text                                AS table_name,
    f.index_name::text                                AS index_name,
    f.index_bytes::bigint                             AS index_bytes,
    f.index_pages::bigint                             AS index_pages,
    f.table_rows::bigint                              AS table_rows,
    f.fillfactor::int                                 AS fillfactor,
    f.est_tuple_bytes::bigint                         AS est_tuple_bytes,
    f.est_leaf_pages::bigint                          AS est_leaf_pages,
    CASE WHEN f.index_pages > 0
          AND NOT f.is_partial
          AND f.table_rows >= 0
          AND NOT f.has_name_typed
          AND f.widths_known = f.key_count
          AND f.est_leaf_pages <= f.index_pages
         THEN ROUND(100.0 * (f.index_pages - f.est_leaf_pages) / f.index_pages, 2)
    END::double precision                             AS est_bloat_pct,
    CASE WHEN f.index_pages > 0
          AND NOT f.is_partial
          AND f.table_rows >= 0
          AND NOT f.has_name_typed
          AND f.widths_known = f.key_count
          AND f.est_leaf_pages <= f.index_pages
         THEN ((f.index_pages - f.est_leaf_pages) * f.bs)
    END::bigint                                       AS est_reclaimable_bytes,
    CASE
        WHEN f.is_partial
            THEN 'partial index: the model scales the parent reltuples, but the index holds only the rows '
                 || 'matching its predicate, which measured -56 percent on the #2561 rig. Run pgstatindex '
                 || 'on this index for an exact answer.'
        WHEN f.table_rows < 0
            THEN 'the parent table has never been analyzed (reltuples = -1), so there is no row count to '
                 || 'model from. An ANALYZE of the parent makes this index estimable.'
        WHEN f.has_name_typed
            THEN 'a key column is name-typed, whose pg_stats width is the padded 64 bytes rather than the '
                 || 'stored text, so any width model built on it is wrong. ioguix calls this row is_na.'
        WHEN f.widths_known <> f.key_count
            THEN 'column widths are not visible for every key: pg_stats filters on has_column_privilege, '
                 || 'so a monitoring role without SELECT sees nothing, and a never-analyzed parent has no '
                 || 'rows either. Grant pg_read_all_data, or ANALYZE the parent.'
        WHEN f.index_pages = 0
            THEN 'the index occupies no pages yet, so there is nothing to be bloated.'
        WHEN f.est_leaf_pages > f.index_pages
            THEN 'the model predicts more pages than the index occupies, which is negative bloat and '
                 || 'therefore not an answer. Measured on the first production target, this cohort is 2.4 '
                 || 'percent unique with a median leading-column distinctness of 0.0018 -- the profile '
                 || 'PostgreSQL 13+ btree deduplication compresses into posting lists, storing duplicate '
                 || 'keys once. Real storage is denser than per-tuple arithmetic can predict, so no '
                 || 'statistics-based model reaches these. Run pgstatindex for an exact answer.'
    END::text                                         AS skipped_reason,
    EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
            WHERE e.extname = 'pgstattuple')          AS pgstattuple_available,
    f.index_oid::bigint                               AS index_oid
FROM final AS f
ORDER BY est_reclaimable_bytes DESC NULLS LAST, f.index_bytes DESC";

    /// <summary>
    /// Sixty seconds, against a statement measured at 1.7 to 2.1 seconds including connect and result
    /// transfer on a 2,500-index target. That is roughly thirty times the measurement, which is margin for
    /// a target with far more indexes rather than a figure derived from anything — the exact census needed
    /// 300 here and spent 84 percent of it, and this one no longer reads a single index page.
    /// </summary>
    public override int? CommandTimeoutSecondsOverride => 60;

    public override string Name => "pg_index_bloat";

    public override string TargetTable => "pg_index_bloat";

    /// <summary>
    /// Writer-only. <c>reltuples</c> and <c>relpages</c> are maintained by VACUUM and ANALYZE, which run on
    /// the primary, and the catalog a replica serves is the primary's anyway.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>
    /// Per database: <c>pg_stats</c>, <c>pg_class</c> and <c>pg_index</c> all describe the CONNECTED
    /// database only, so a cluster-wide claim from one connection would silently be one database's answer.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    /// <summary>
    /// NONE, and that is a coverage change rather than housekeeping. The exact census required pgstattuple,
    /// so a database without it failed collection for that database outright — observed on the
    /// <c>postgres</c> maintenance database of the first production target. This statement reads only
    /// catalogs and <c>pg_stats</c>, so it runs wherever the grant allows. pgstattuple presence is still
    /// REPORTED per row, because it decides whether the escalation command needs a CREATE EXTENSION first.
    /// </summary>
    public override IReadOnlyList<PgExtensionDependency> RequiredPgExtensions { get; } =
        Array.Empty<PgExtensionDependency>();

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* Per-database, so the same index name in two databases is two rows rather than one
           indistinguishable one (#2599). */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_bytes", CollectorColumnType.BigInt),

        /* The pgstatindex columns, retained and written NULL by this collector. The store holds 90 days of
           exact measurements taken before #3234 and an on-request measurement needs somewhere to land, so
           the migration adds columns rather than dropping these. NULL here means ESTIMATED, not measured
           empty. */
        new CollectorColumn("tree_level", CollectorColumnType.Integer),
        new CollectorColumn("internal_pages", CollectorColumnType.BigInt),
        new CollectorColumn("leaf_pages", CollectorColumnType.BigInt),
        new CollectorColumn("empty_pages", CollectorColumnType.BigInt),
        new CollectorColumn("deleted_pages", CollectorColumnType.BigInt),
        new CollectorColumn("avg_leaf_density", CollectorColumnType.Double),
        new CollectorColumn("leaf_fragmentation", CollectorColumnType.Double),

        new CollectorColumn("skipped_reason", CollectorColumnType.Varchar),

        /* The estimate, and every input it rests on, so the number can be argued with rather than
           believed. */
        new CollectorColumn("index_pages", CollectorColumnType.BigInt),
        new CollectorColumn("table_rows", CollectorColumnType.BigInt),
        new CollectorColumn("fillfactor", CollectorColumnType.Integer),
        new CollectorColumn("est_tuple_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("est_leaf_pages", CollectorColumnType.BigInt),
        new CollectorColumn("est_bloat_pct", CollectorColumnType.Double),
        new CollectorColumn("est_reclaimable_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("pgstattuple_available", CollectorColumnType.Boolean),
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
                IndexPages: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TableRows: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                Fillfactor: reader.IsDBNull(7) ? DefaultBtreeFillfactor : reader.GetInt32(7),
                EstTupleBytes: reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                EstLeafPages: reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                /* NULL exactly when a reason is populated, which is what stops a suppressed estimate from
                   ever reading as zero bloat. */
                EstBloatPct: reader.IsDBNull(10) ? null : reader.GetDouble(10),
                EstReclaimableBytes: reader.IsDBNull(11) ? null : reader.GetInt64(11),
                SkippedReason: reader.IsDBNull(12) ? null : reader.GetString(12),
                PgstattupleAvailable: !reader.IsDBNull(13) && reader.GetBoolean(13),
                IndexOid: reader.IsDBNull(14) ? 0 : reader.GetInt64(14)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Bloat is a level, and the history is what separates an index that has been bloated
           since it was built from one that got that way this month. */
        writer
            .Value(row.DatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.IndexName)
            .Value(row.IndexBytes)

            /* The exact-measurement columns, NULL from this collector. */
            .Value((int?)null)
            .Value((long?)null)
            .Value((long?)null)
            .Value((long?)null)
            .Value((long?)null)
            .Value((double?)null)
            .Value((double?)null)

            .Value(row.SkippedReason)

            .Value(row.IndexPages)
            .Value(row.TableRows)
            .Value(row.Fillfactor)
            .Value(row.EstTupleBytes)
            .Value(row.EstLeafPages)
            .Value(row.EstBloatPct)
            .Value(row.EstReclaimableBytes)
            .Value(row.PgstattupleAvailable);
    }
}
