/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-query-shape execution statistics for an Amazon Aurora PostgreSQL target — the Postgres
/// counterpart of <see cref="QueryStatsCollector"/>. Reads <c>aurora_stat_statements()</c>, which is
/// <c>pg_stat_statements</c> plus columns only Aurora has.
/// <para>Two things Aurora adds that are worth the dependency. It decomposes the opaque
/// <c>shared_blks_read</c> into <b>where the block actually came from</b> —
/// <c>storage_blks_read</c> (the distributed storage volume), <c>orcache_blks_hit</c> (the local NVMe
/// Optimized Reads tier), and local — which means a cache-hit ratio computed the community way is
/// arithmetically misleading on Aurora, because a "read" may have been a fast local hit. And it
/// reports <b>peak memory per statement</b>, which is the closest thing PostgreSQL has to SQL Server's
/// memory-grant data; core PostgreSQL has no grant concept at all.</para>
/// <para>#2625: it reads BOTH sources. On Aurora it reads <c>aurora_stat_statements(false)</c>; on any
/// other PostgreSQL it reads the vanilla <c>pg_stat_statements</c> view and reports the Aurora-only
/// columns as NULL. One collector, one table, chosen at query-build time — because the Aurora function
/// is <c>pg_stat_statements</c> plus columns, so the vanilla read is a strict subset rather than a
/// different measurement, and splitting it in two would have meant two tables, two readers, two panels
/// and two MCP tools for one question.</para>
/// <para>It was Aurora-only until a self-hosted PostgreSQL target existed to notice. The cost of that
/// gate was not a missing column — it was that "which queries cost the most", the question a database
/// monitor exists to answer, had NO answer at all on stock PostgreSQL, while the collectors either side
/// of it happily gathered OS CPU and predicate selectivity keyed by the very queryids it was not
/// identifying.</para>
/// <para><b>NULL is load-bearing here.</b> The Aurora-only columns are written NULL rather than zero on
/// the vanilla path: zero would say Aurora measured no storage reads for this statement, which is a
/// claim about the server, not about the source. Every consumer of those columns already tolerates NULL
/// because Aurora itself returns NULL for a statement with no such activity.</para>
/// </summary>
public sealed class PgStatementStatsCollector : PostgresCollectorDefinitionBase<PgStatementStatsCollector.Row>
{
    public static PgStatementStatsCollector Instance { get; } = new();

    private PgStatementStatsCollector()
    {
    }

    public readonly record struct Row(
        long QueryId,
        long DatabaseId,
        long UserId,
        bool TopLevel,
        long Calls,
        double TotalExecTimeMs,
        double MinExecTimeMs,
        double MaxExecTimeMs,
        double MeanExecTimeMs,
        long RowsReturned,
        long SharedBlocksHit,
        long SharedBlocksRead,
        long SharedBlocksDirtied,
        long SharedBlocksWritten,
        long TempBlocksRead,
        long TempBlocksWritten,
        double BlockReadTimeMs,
        double BlockWriteTimeMs,
        /* Nullable from #2625 on: these six exist only in aurora_stat_statements(). NULL means "this
           source does not report it", which is not the same statement as zero. */
        long? StorageBlocksRead,
        long? OrcacheBlocksHit,
        double? StorageBlockReadTimeMs,
        double? OrcacheBlockReadTimeMs,
        long WalRecords,
        long WalFpi,
        long WalBytes,
        long? TotalExecPeakMemBytes,
        long? MaxExecPeakMemBytes);

    /* Column names DIFFER between PostgreSQL 16 and 17 and our fleet spans both, so the query is
       built per major rather than SELECT *-ed. Verified against live 16.11 and 17.7:

         16.11 : blk_read_time,        blk_write_time
         17.7  : shared_blk_read_time, shared_blk_write_time

       PG17 also adds jit_deform_time/_count, stats_since, and minmax_stats_since, which this
       definition does not read. A SELECT * here would not error on either version — it would silently
       shift every ordinal, which is how monitoring tools shipped broken PG17 collectors.

       Explicit casts pin the reader's types: wal_bytes is numeric in PostgreSQL (bigint cannot hold
       its declared 10^20 range, though no real statement approaches it), and Npgsql's type checking
       is strict enough that reading numeric with GetInt64 throws. */
    private static string BuildQueryText(int postgresMajorVersion, bool isAurora)
    {
        var readTime = postgresMajorVersion >= 17 ? "shared_blk_read_time" : "blk_read_time";
        var writeTime = postgresMajorVersion >= 17 ? "shared_blk_write_time" : "blk_write_time";

        /* #2625, the vanilla path. The ORDINALS are identical to Aurora's on purpose: the six columns
           only Aurora reports are selected as typed NULL literals rather than omitted, so ReadAsync,
           PayloadColumns and WritePayload stay one implementation with one ordering. A shorter SELECT
           here would mean a second reader whose ordinals could drift from this one, which is the exact
           failure the per-major column naming above already documents.

           toplevel arrived in pg_stat_statements 1.9 (PostgreSQL 14). Before that every row IS a top
           level statement - nested tracking is what 1.9 added - so `true` is the correct value on an
           older server, not a fallback. */
        var topLevel = postgresMajorVersion >= 14 ? "toplevel" : "true";

        if (!isAurora)
        {
            return $@"
SELECT
    queryid::bigint                    AS queryid,
    dbid::bigint                       AS dbid,
    userid::bigint                     AS userid,
    {topLevel}                         AS toplevel,
    calls::bigint                      AS calls,
    total_exec_time                    AS total_exec_time,
    min_exec_time                      AS min_exec_time,
    max_exec_time                      AS max_exec_time,
    mean_exec_time                     AS mean_exec_time,
    rows::bigint                       AS rows_returned,
    shared_blks_hit::bigint            AS shared_blks_hit,
    shared_blks_read::bigint           AS shared_blks_read,
    shared_blks_dirtied::bigint        AS shared_blks_dirtied,
    shared_blks_written::bigint        AS shared_blks_written,
    temp_blks_read::bigint             AS temp_blks_read,
    temp_blks_written::bigint          AS temp_blks_written,
    {readTime}                         AS blk_read_time,
    {writeTime}                        AS blk_write_time,
    NULL::bigint                       AS storage_blks_read,
    NULL::bigint                       AS orcache_blks_hit,
    NULL::double precision             AS storage_blk_read_time,
    NULL::double precision             AS orcache_blk_read_time,
    wal_records::bigint                AS wal_records,
    wal_fpi::bigint                    AS wal_fpi,
    wal_bytes::bigint                  AS wal_bytes,
    NULL::bigint                       AS total_exec_peakmem,
    NULL::bigint                       AS max_exec_peakmem
FROM public.pg_stat_statements
WHERE calls > 0";
        }

        return $@"
SELECT
    queryid::bigint                    AS queryid,
    dbid::bigint                       AS dbid,
    userid::bigint                     AS userid,
    toplevel                           AS toplevel,
    calls::bigint                      AS calls,
    total_exec_time                    AS total_exec_time,
    min_exec_time                      AS min_exec_time,
    max_exec_time                      AS max_exec_time,
    mean_exec_time                     AS mean_exec_time,
    rows::bigint                       AS rows_returned,
    shared_blks_hit::bigint            AS shared_blks_hit,
    shared_blks_read::bigint           AS shared_blks_read,
    shared_blks_dirtied::bigint        AS shared_blks_dirtied,
    shared_blks_written::bigint        AS shared_blks_written,
    temp_blks_read::bigint             AS temp_blks_read,
    temp_blks_written::bigint          AS temp_blks_written,
    {readTime}                         AS blk_read_time,
    {writeTime}                        AS blk_write_time,
    storage_blks_read::bigint          AS storage_blks_read,
    orcache_blks_hit::bigint           AS orcache_blks_hit,
    storage_blk_read_time              AS storage_blk_read_time,
    orcache_blk_read_time              AS orcache_blk_read_time,
    wal_records::bigint                AS wal_records,
    wal_fpi::bigint                    AS wal_fpi,
    wal_bytes::bigint                  AS wal_bytes,
    total_exec_peakmem::bigint         AS total_exec_peakmem,
    max_exec_peakmem::bigint           AS max_exec_peakmem
FROM aurora_stat_statements(false)
WHERE calls > 0";
    }

    public override string Name => "pg_statement_stats";

    public override string TargetTable => "pg_statement_stats";

    /// <summary>
    /// Every PostgreSQL target (#2625). The SOURCE differs by flavor - Aurora's extended function or the
    /// vanilla view - and that is a <see cref="BuildQuery"/> decision, not an applicability one.
    /// <para><c>AppliesTo</c> means permanent incapability, and stock PostgreSQL is perfectly capable of
    /// reporting per-statement execution statistics. Returning false here told every operator of a
    /// non-Aurora target that this server "does not collect per-query-shape execution statistics, and
    /// never will" - a sentence composed by the capability machinery precisely so that a real gap reads
    /// as final and is not chased. It was not a real gap.</para>
    /// <para><c>pg_stat_statements</c> is not installed by default, so a target without it fails the
    /// read with SQLSTATE 42P01 and lands in the ObjectMissing vocabulary - "the source object does not
    /// exist on this target" - which is the correct, actionable answer (CREATE EXTENSION) and is exactly
    /// how <c>pg_wait_sampling</c> and <c>pg_kernel_stats</c> already report the same situation. An
    /// uninstalled extension is a target-configuration fact, not an incapability.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion, context.Target.IsAurora));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("queryid", CollectorColumnType.BigInt),
        new CollectorColumn("database_id", CollectorColumnType.BigInt),
        new CollectorColumn("user_id", CollectorColumnType.BigInt),
        new CollectorColumn("toplevel", CollectorColumnType.Boolean),
        new CollectorColumn("calls", CollectorColumnType.BigInt),
        new CollectorColumn("total_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("min_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("max_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("mean_exec_time_ms", CollectorColumnType.Double),
        new CollectorColumn("rows_returned", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_hit", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_dirtied", CollectorColumnType.BigInt),
        new CollectorColumn("shared_blks_written", CollectorColumnType.BigInt),
        new CollectorColumn("temp_blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("temp_blks_written", CollectorColumnType.BigInt),
        new CollectorColumn("blk_read_time_ms", CollectorColumnType.Double),
        new CollectorColumn("blk_write_time_ms", CollectorColumnType.Double),
        /* Aurora-only I/O source split. Without these, blks_read is opaque: it may have been a
           network round trip to the storage volume or a hit in the local NVMe tier. */
        new CollectorColumn("storage_blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("orcache_blks_hit", CollectorColumnType.BigInt),
        new CollectorColumn("storage_blk_read_time_ms", CollectorColumnType.Double),
        new CollectorColumn("orcache_blk_read_time_ms", CollectorColumnType.Double),
        new CollectorColumn("wal_records", CollectorColumnType.BigInt),
        new CollectorColumn("wal_fpi", CollectorColumnType.BigInt),
        new CollectorColumn("wal_bytes", CollectorColumnType.BigInt),
        /* The memory-grant analog. No SQL Server DMV gives per-query WAL bytes either, so both of
           these are signals the SQL Server side cannot offer. */
        new CollectorColumn("total_exec_peakmem_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("max_exec_peakmem_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_calls", CollectorColumnType.BigInt),
        new CollectorColumn("delta_total_exec_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_rows", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                QueryId: reader.GetInt64(0),
                DatabaseId: reader.GetInt64(1),
                UserId: reader.GetInt64(2),
                TopLevel: !reader.IsDBNull(3) && reader.GetBoolean(3),
                Calls: reader.GetInt64(4),
                TotalExecTimeMs: Dbl(reader, 5),
                MinExecTimeMs: Dbl(reader, 6),
                MaxExecTimeMs: Dbl(reader, 7),
                MeanExecTimeMs: Dbl(reader, 8),
                RowsReturned: reader.GetInt64(9),
                SharedBlocksHit: reader.GetInt64(10),
                SharedBlocksRead: reader.GetInt64(11),
                SharedBlocksDirtied: reader.GetInt64(12),
                SharedBlocksWritten: reader.GetInt64(13),
                TempBlocksRead: reader.GetInt64(14),
                TempBlocksWritten: reader.GetInt64(15),
                BlockReadTimeMs: Dbl(reader, 16),
                BlockWriteTimeMs: Dbl(reader, 17),
                /* NullableLong/NullableDbl, not Dbl: on the vanilla path these six ARRIVE null, and
                   coalescing them to zero here would erase the distinction the whole change rests on. */
                StorageBlocksRead: NullableLong(reader, 18),
                OrcacheBlocksHit: NullableLong(reader, 19),
                StorageBlockReadTimeMs: NullableDbl(reader, 20),
                OrcacheBlockReadTimeMs: NullableDbl(reader, 21),
                WalRecords: reader.GetInt64(22),
                WalFpi: reader.GetInt64(23),
                WalBytes: reader.GetInt64(24),
                TotalExecPeakMemBytes: NullableLong(reader, 25),
                MaxExecPeakMemBytes: NullableLong(reader, 26)));
        }

        return rows;

        static double Dbl(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? 0 : r.GetDouble(ordinal);

        static long? NullableLong(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);

        static double? NullableDbl(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDouble(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta key is (queryid, dbid, userid, toplevel) — the full pg_stat_statements identity, not
           queryid alone. The same normalized statement run by a different user or against a different
           database is a DIFFERENT entry with its own counters, so keying on queryid alone would
           interleave several series into one and produce nonsense deltas.

           queryid itself is not stable across major versions (it is derived from a post-parse-analysis
           tree, including internal object identifiers), so a mass reset after an upgrade is expected
           behaviour rather than an anomaly — the delta calculator's counter-regression handling covers
           it the same way it covers a restart. */
        var key = string.Create(CultureInfo.InvariantCulture,
            $"{row.QueryId}|{row.DatabaseId}|{row.UserId}|{(row.TopLevel ? 1 : 0)}");

        var deltaCalls = context.Deltas.CalculateDelta(
            context.ServerId, "pg_statement_stats_calls", key, row.Calls,
            collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        /* Time is stored as double milliseconds but the delta machinery is integral, so the delta is
           taken on whole milliseconds. Sub-millisecond drift per interval is immaterial against the
           totals this feeds, and keeping one delta calculator for both engines is worth more. */
        var deltaTotalTime = context.Deltas.CalculateDelta(
            context.ServerId, "pg_statement_stats_time", key, (long)row.TotalExecTimeMs,
            collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaRows = context.Deltas.CalculateDelta(
            context.ServerId, "pg_statement_stats_rows", key, row.RowsReturned,
            collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        writer
            .Value(row.QueryId)
            .Value(row.DatabaseId)
            .Value(row.UserId)
            .Value(row.TopLevel)
            .Value(row.Calls)
            .Value(row.TotalExecTimeMs)
            .Value(row.MinExecTimeMs)
            .Value(row.MaxExecTimeMs)
            .Value(row.MeanExecTimeMs)
            .Value(row.RowsReturned)
            .Value(row.SharedBlocksHit)
            .Value(row.SharedBlocksRead)
            .Value(row.SharedBlocksDirtied)
            .Value(row.SharedBlocksWritten)
            .Value(row.TempBlocksRead)
            .Value(row.TempBlocksWritten)
            .Value(row.BlockReadTimeMs)
            .Value(row.BlockWriteTimeMs)
            .Value(row.StorageBlocksRead)
            .Value(row.OrcacheBlocksHit)
            .Value(row.StorageBlockReadTimeMs)
            .Value(row.OrcacheBlockReadTimeMs)
            .Value(row.WalRecords)
            .Value(row.WalFpi)
            .Value(row.WalBytes)
            .Value(row.TotalExecPeakMemBytes)
            .Value(row.MaxExecPeakMemBytes)
            .Value(deltaCalls)
            .Value(deltaTotalTime)
            .Value(deltaRows);
    }
}
