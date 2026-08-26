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
/// I/O broken down by who did it, to what, and why — <c>pg_stat_io</c> (PostgreSQL 16+).
/// <para>Richer than <c>sys.dm_io_virtual_file_stats</c>, which attributes I/O to a FILE. This attributes
/// it to a <c>(backend_type, object, context)</c> triple, so "the database is doing 40k reads/sec" becomes
/// "autovacuum workers are doing 40k reads/sec against relations in the vacuum context" — a sentence you
/// can act on. The context dimension in particular has no SQL Server counterpart: it separates ordinary
/// buffer-pool traffic from sequential scans that deliberately bypass it (<c>bulkread</c>), from vacuum's
/// ring buffer, from WAL replay.</para>
/// <para><b>NULL is preserved and never coalesced to zero.</b> PostgreSQL uses NULL for "this counter does
/// not apply to this combination" — the checkpointer performs no reads or hits, <c>bulkread</c> never
/// extends a relation, the <c>normal</c> context has no ring buffer to reuse. On Aurora the write-side
/// counters are NULL or permanently zero across the board, because backends there do not write data files;
/// the storage layer does. Storing 0 in any of those places would claim a measurement that was never
/// taken, and a consumer computing a write-latency average would divide by it.</para>
/// <para>Cluster-wide, so no per-database fan-out, and valid on a standby — a replica's own read traffic
/// and its <c>walreplay</c> context are exactly what you want when a reader is slow. Verified on Aurora
/// 16.11 and 17.7: 25–37 rows per snapshot, which is why a per-minute cadence is affordable here where it
/// would not be for a per-table collector.</para>
/// </summary>
public sealed class PgIoStatsCollector : PostgresCollectorDefinitionBase<PgIoStatsCollector.Row>
{
    public static PgIoStatsCollector Instance { get; } = new();

    private PgIoStatsCollector()
    {
    }

    public readonly record struct Row(
        string? BackendType,
        string? ObjectType,
        string? Context,
        long? Reads,
        double? ReadTimeMs,
        long? Writes,
        double? WriteTimeMs,
        long? Writebacks,
        double? WritebackTimeMs,
        long? Extends,
        double? ExtendTimeMs,
        long? OpBytes,
        long? Hits,
        long? Evictions,
        long? Reuses,
        long? Fsyncs,
        double? FsyncTimeMs,
        DateTime? StatsReset,
        /* PG18's measured byte totals (#2655). NULL below 18, where op_bytes is the answer instead.
           decimal, not long, because PostgreSQL declares these `numeric` while `reads` beside them is
           `bigint` — verified on 18.6. */
        decimal? ReadBytes,
        decimal? WriteBytes,
        decimal? ExtendBytes);

    /* Two version concerns, both about keeping the stored shape constant:

         PG16+  : the view itself. Gated in AppliesTo rather than here.
         PG18   : op_bytes was REMOVED and replaced by read_bytes / write_bytes / extend_bytes. Selecting
                  op_bytes on 18 would fail with "column does not exist" and take the whole collection
                  with it, so it is substituted. The replacements now have their own columns (V101, #2655),
                  decided against a real 18.6 as this note asked: they are `numeric` where the counts beside
                  them are `bigint`, and they are a DIFFERENT quantity, not a rename. op_bytes was the
                  per-operation block size that a reader multiplies by a count to estimate volume; these are
                  measured totals. 18 also introduced vectored reads, so one entry in `reads` can cover
                  several blocks and the old estimate UNDERCOUNTS — 479 reads against 4,440,064 read bytes
                  on 18.6 is 542 blocks, not 479. That is why the column went rather than being renamed, and
                  why a reader has to be able to tell which quantity it holds.

                  Appended to the SELECT rather than slotted beside op_bytes so no existing ordinal moves.

       Verified identical on Aurora 16.11 and 17.7: 18 columns, same names, same order. The enum VALUES do
       differ between them — 17.7 showed a `walreplay` context and Aurora-specific backend types
       ('aurora cache receiver process', 'aurora wal replay process', 'slotsync worker') that 16.11 did not
       — which is why nothing here filters on them. A whitelist would silently drop rows.

       stats_reset is `timestamp with time zone`; AT TIME ZONE 'UTC' rather than ::timestamp, because the
       cast renders in the SESSION's TimeZone and the store contract is naive UTC. */
    private static string BuildQueryText(int postgresMajorVersion)
    {
        var opBytes = postgresMajorVersion >= 18 ? "NULL::bigint" : "op_bytes";
        var hasMeasuredBytes = postgresMajorVersion >= 18;
        var readBytes = hasMeasuredBytes ? "read_bytes" : "NULL::numeric";
        var writeBytes = hasMeasuredBytes ? "write_bytes" : "NULL::numeric";
        var extendBytes = hasMeasuredBytes ? "extend_bytes" : "NULL::numeric";

        return $@"
SELECT
    backend_type                            AS backend_type,
    object                                  AS object_type,
    context                                 AS context,
    reads                                   AS reads,
    read_time                               AS read_time_ms,
    writes                                  AS writes,
    write_time                              AS write_time_ms,
    writebacks                              AS writebacks,
    writeback_time                          AS writeback_time_ms,
    extends                                 AS extends,
    extend_time                             AS extend_time_ms,
    {opBytes}                               AS op_bytes,
    hits                                    AS hits,
    evictions                               AS evictions,
    reuses                                  AS reuses,
    fsyncs                                  AS fsyncs,
    fsync_time                              AS fsync_time_ms,
    (stats_reset AT TIME ZONE 'UTC')        AS stats_reset,
    {readBytes}                             AS read_bytes,
    {writeBytes}                            AS write_bytes,
    {extendBytes}                           AS extend_bytes
FROM pg_stat_io
ORDER BY backend_type, object, context";
    }

    public override string Name => "pg_io_stats";

    public override string TargetTable => "pg_io_stats";

    /// <summary>
    /// <c>pg_stat_io</c> is PostgreSQL 16+. No Aurora gate and no recovery gate: it is a core view, and a
    /// standby's own read traffic is a legitimate thing to monitor — unlike
    /// <see cref="PgAutovacuumStatsCollector"/>, whose source reports zeros on a replica.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => target.PostgresMajorVersion >= 16;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* The three dimensions. Together they are the row's identity, and the delta key any read has to
           difference on. `object_type` rather than `object` because object is reserved-ish and reads badly
           in a store query. */
        new CollectorColumn("backend_type", CollectorColumnType.Varchar),
        new CollectorColumn("object_type", CollectorColumnType.Varchar),
        new CollectorColumn("context", CollectorColumnType.Varchar),
        /* Cumulative counters, stored raw with NULL intact. The windowed change is computed at read time
           with the same positive-difference-per-interval rule the statement read uses, rather than by
           keeping delta state for every triple: the row count is small but the NULL semantics matter more,
           and a stored delta would have to invent a value for "not applicable". */
        new CollectorColumn("reads", CollectorColumnType.BigInt),
        new CollectorColumn("read_time_ms", CollectorColumnType.Double),
        /* NULL on Aurora, all majors: backends do not write data files there, the storage layer does.
           Kept because a self-managed PostgreSQL target DOES populate them, and because a column that is
           NULL for a documented reason is more useful than a missing one. */
        new CollectorColumn("writes", CollectorColumnType.BigInt),
        new CollectorColumn("write_time_ms", CollectorColumnType.Double),
        new CollectorColumn("writebacks", CollectorColumnType.BigInt),
        new CollectorColumn("writeback_time_ms", CollectorColumnType.Double),
        new CollectorColumn("extends", CollectorColumnType.BigInt),
        new CollectorColumn("extend_time_ms", CollectorColumnType.Double),
        /* The block size an operation moves (8192 in practice). PG18 removed it; NULL there. */
        new CollectorColumn("op_bytes", CollectorColumnType.BigInt),
        /* hits is the buffer-pool hit count — the numerator of a real cache-hit ratio, alongside reads. */
        new CollectorColumn("hits", CollectorColumnType.BigInt),
        new CollectorColumn("evictions", CollectorColumnType.BigInt),
        /* reuses is ring-buffer reuse and applies only to the bulk/vacuum contexts. NOT eviction
           pressure — conflating the two is the standard misreading of this view. */
        new CollectorColumn("reuses", CollectorColumnType.BigInt),
        new CollectorColumn("fsyncs", CollectorColumnType.BigInt),
        new CollectorColumn("fsync_time_ms", CollectorColumnType.Double),
        /* The explicit reset signal, so a read does not have to infer one from a counter going backwards. */
        new CollectorColumn("stats_reset", CollectorColumnType.Timestamp),
        /* PG18's measured byte totals (#2655), NULL below 18. Decimal because PostgreSQL declares them
           `numeric` while the counts beside them are `bigint`; storing a byte total as bigint is a
           narrowing the catalog never promised.

           Scale 0 — these are whole bytes. Precision 28 rather than the 38 a DECIMAL can hold, because
           C# decimal tops out at 29 significant digits and a declared width the runtime type cannot carry
           would be a promise broken at the reader rather than at the store. 10^28 bytes is past absurd for
           a counter that resets on restart. */
        new CollectorColumn("read_bytes", CollectorColumnType.Decimal, 28, 0),
        new CollectorColumn("write_bytes", CollectorColumnType.Decimal, 28, 0),
        new CollectorColumn("extend_bytes", CollectorColumnType.Decimal, 28, 0),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                BackendType: reader.IsDBNull(0) ? null : reader.GetString(0),
                ObjectType: reader.IsDBNull(1) ? null : reader.GetString(1),
                Context: reader.IsDBNull(2) ? null : reader.GetString(2),
                Reads: Long(reader, 3),
                ReadTimeMs: Double(reader, 4),
                Writes: Long(reader, 5),
                WriteTimeMs: Double(reader, 6),
                Writebacks: Long(reader, 7),
                WritebackTimeMs: Double(reader, 8),
                Extends: Long(reader, 9),
                ExtendTimeMs: Double(reader, 10),
                OpBytes: Long(reader, 11),
                Hits: Long(reader, 12),
                Evictions: Long(reader, 13),
                Reuses: Long(reader, 14),
                Fsyncs: Long(reader, 15),
                FsyncTimeMs: Double(reader, 16),
                StatsReset: reader.IsDBNull(17) ? null : reader.GetDateTime(17),
                ReadBytes: Decimal(reader, 18),
                WriteBytes: Decimal(reader, 19),
                ExtendBytes: Decimal(reader, 20)));
        }

        return rows;

        /* Nullable all the way through, deliberately. Every other Postgres collector here uses a -1
           sentinel for "not applicable", which suits a LEVEL that a consumer reads directly. These are
           cumulative counters that get differenced, and -1 differenced against a real value produces a
           garbage interval — so NULL, which propagates through the subtraction and drops out of the sum. */
        static long? Long(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);
        static double? Double(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDouble(ordinal);
        static decimal? Decimal(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDecimal(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.BackendType)
            .Value(row.ObjectType)
            .Value(row.Context)
            .Value(row.Reads)
            .Value(row.ReadTimeMs)
            .Value(row.Writes)
            .Value(row.WriteTimeMs)
            .Value(row.Writebacks)
            .Value(row.WritebackTimeMs)
            .Value(row.Extends)
            .Value(row.ExtendTimeMs)
            .Value(row.OpBytes)
            .Value(row.Hits)
            .Value(row.Evictions)
            .Value(row.Reuses)
            .Value(row.Fsyncs)
            .Value(row.FsyncTimeMs)
            .Value(row.StatsReset)
            .Value(row.ReadBytes)
            .Value(row.WriteBytes)
            .Value(row.ExtendBytes);
    }
}
