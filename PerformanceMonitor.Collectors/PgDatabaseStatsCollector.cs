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
/// Four unrelated questions off one cheap view — <c>pg_stat_database</c> (#2539).
/// <para><b>Temp-file spills are the reason this exists.</b> <c>temp_files</c> / <c>temp_bytes</c> count the
/// work a query had to push to disk because <c>work_mem</c> could not hold its sort or hash. That is the
/// PostgreSQL answer to "why is this query slow", and it is the signal a stock PostgreSQL target had nowhere
/// else: <see cref="PgStatementStatsCollector"/> does carry per-query <c>temp_blks_written</c>, but it reads
/// <c>aurora_stat_statements()</c> and is Aurora-only, and even on Aurora it is a top-N over statements
/// <c>pg_stat_statements</c> still retains. This is the ground truth the per-query numbers are a share of.
/// </para>
/// <para>The other three ride along for free, because they are columns of the same row:
/// <c>blks_hit</c>/<c>blks_read</c> is the cache hit ratio every PostgreSQL DBA looks at first,
/// <c>deadlocks</c> is a server-recorded count we had no PostgreSQL source for at all, and
/// <c>xact_commit</c>/<c>xact_rollback</c> is how a rollback storm becomes visible.</para>
///
/// <para><b>Per-database rows, stored per database</b> (#2539 asked for this to be argued rather than
/// inherited). Three reasons, in order of weight:</para>
/// <list type="number">
/// <item><b><c>stats_reset</c> is per database.</b> <c>pg_stat_reset()</c> resets the database it is
/// connected to, so each row carries its OWN reset timestamp and they routinely disagree. An aggregate would
/// have to pick one, and a reset of a single database would then corrupt the whole cluster's delta with
/// nothing left in the data to say so. Aggregation does not merely lose detail here, it destroys the one
/// signal that makes the deltas trustworthy.</item>
/// <item><b>The number is only actionable with the database attached.</b> "The cluster wrote 40 GB of temp
/// files" is a fact; "the reporting database wrote 40 GB of temp files while the OLTP database wrote none"
/// is a next step. Same for deadlocks and for a rollback ratio.</item>
/// <item><b>It costs nothing.</b> <c>pg_stat_database</c> is CLUSTER-WIDE — one row per database, readable
/// from whichever single database the collector is connected to. Unlike
/// <see cref="PgAutovacuumStatsCollector"/> this is not a per-database fan-out and opens no extra
/// connections, so the per-database grain is free rather than paid for.</item>
/// </list>
///
/// <para><b>The <c>datname IS NULL</c> row is kept, not filtered.</b> PostgreSQL emits one row with a NULL
/// database name for activity against SHARED relations (<c>pg_database</c>, <c>pg_authid</c>, and the rest of
/// the cluster-wide catalog). Its temp/deadlock/transaction counters are always zero, but its block counters
/// are not, so dropping it would silently understate the cluster's total block accesses and overstate the
/// hit ratio of everything else. It is labelled by the read rather than filtered by the collector, which
/// keeps the stored rows the same set <c>pg_stat_database</c> itself returns.</para>
///
/// <para><b>Cumulative since reset, differenced at read time</b> — the same rule
/// <see cref="PgIoStatsCollector"/> follows, and for the same reason: the raw counter is what the server
/// reports, so storing it keeps every window recomputable and leaves no delta state to get out of step with
/// the samples.</para>
/// </summary>
public sealed class PgDatabaseStatsCollector : PostgresCollectorDefinitionBase<PgDatabaseStatsCollector.Row>
{
    public static PgDatabaseStatsCollector Instance { get; } = new();

    private PgDatabaseStatsCollector()
    {
    }

    public readonly record struct Row(
        string? DatabaseName,
        long? XactCommit,
        long? XactRollback,
        long? BlksRead,
        long? BlksHit,
        long? TempFiles,
        long? TempBytes,
        long? Deadlocks,
        DateTime? StatsReset);

    /* VERSION FLOORS, checked column by column rather than assumed, because getting one wrong takes the
       whole collection down every cycle with "column does not exist":

         xact_commit, xact_rollback, blks_read, blks_hit  : PostgreSQL 8.x. Present in every major anyone
                                                            could plausibly monitor.
         stats_reset                                      : 9.1.
         temp_files, temp_bytes, deadlocks                : 9.2.

       So the newest thing selected here landed well before the oldest major this repo sweeps (13), and
       there is nothing to gate on - which is why AppliesTo below is an unconditional true rather than a
       version floor copied from pg_stat_io. The columns deliberately NOT selected are the ones that would
       have needed a gate, and each was left out on its own merits rather than for the version alone:

         checksum_failures, checksum_last_failure              : 12+
         session_time, active_time, idle_in_transaction_time,
         sessions, sessions_abandoned, sessions_fatal,
         sessions_killed                                       : 14+
         parallel_workers_to_launch, parallel_workers_launched  : 18+

       blk_read_time / blk_write_time are 9.2 columns and are also left out, for a semantic reason rather
       than a version one: they read 0 both when track_io_timing is OFF and when no I/O happened, and this
       table has no third value to tell those apart. pg_stat_io - already collected on 16+ - carries the same
       measure WITH a NULL for "not tracked", so an ambiguous copy of it here would be strictly worse than
       the source we already have.

       PostgreSQL 15 moved the cumulative statistics system into shared memory and made stats survive a
       clean shutdown; it changed neither the names nor the meanings of any column selected here, so it is
       not a version break for this collector. It IS why a crash restart shows as a reset: the discarded
       counters come back at zero with a fresh stats_reset, which is exactly what the read reports.

       stats_reset is `timestamp with time zone`; AT TIME ZONE 'UTC' rather than ::timestamp, because the
       cast renders in the SESSION's TimeZone and the store contract is naive UTC. */
    internal const string QueryText = @"
SELECT
    d.datname                              AS database_name,
    d.xact_commit                          AS xact_commit,
    d.xact_rollback                        AS xact_rollback,
    d.blks_read                            AS blks_read,
    d.blks_hit                             AS blks_hit,
    d.temp_files                           AS temp_files,
    d.temp_bytes                           AS temp_bytes,
    d.deadlocks                            AS deadlocks,
    (d.stats_reset AT TIME ZONE 'UTC')     AS stats_reset
FROM pg_catalog.pg_stat_database AS d
ORDER BY d.datname";

    public override string Name => "pg_database_stats";

    /// <summary>
    /// <c>pg_database_stats</c>, NOT <c>pg_stat_database</c>. <c>pg_catalog</c> is searched implicitly and
    /// FIRST, so a store table named after the catalog view would break <c>CREATE INDEX</c> with 42809 and,
    /// far worse, make every unqualified read in the store resolve to the MONITORING store's own
    /// <c>pg_stat_database</c> — returning the store's own statistics as though they were the target's.
    /// <c>CollectorTableCatalogShadowTests</c> asserts this against a live catalog; the <c>_stats</c> suffix
    /// is the house pattern (<c>pg_replication_slots</c> → <c>pg_replication_slot_stats</c>).
    /// </summary>
    public override string TargetTable => "pg_database_stats";

    /// <summary>
    /// Every PostgreSQL target, with no gate at all — and each half of that is a decision rather than an
    /// omission.
    ///
    /// <para><b>No version floor,</b> because there is nothing to floor: the newest column selected arrived
    /// in 9.2 (see the version audit above <see cref="QueryText"/>). Copying
    /// <see cref="PgIoStatsCollector"/>'s PG16 gate by reflex would have gated off a view that has worked
    /// since 2012.</para>
    ///
    /// <para><b>No Aurora gate,</b> because this is core PostgreSQL and is the only temp-file signal a STOCK
    /// PostgreSQL target has at all — <see cref="PgStatementStatsCollector"/> reads
    /// <c>aurora_stat_statements()</c> and is Aurora-only.</para>
    ///
    /// <para><b>No recovery gate,</b> and that is the one worth arguing rather than assuming. A standby keeps
    /// its OWN <c>pg_stat_database</c> counters — its own block reads and hits, and its own
    /// <c>temp_files</c>, because a sort on a read replica spills exactly the way it does on the writer.
    /// Reporting queries are routinely aimed at readers precisely because they are heavy, so the replica is
    /// where a spill is MOST likely and gating it off would blind the collector to the condition on the
    /// instance most likely to have it. This follows <see cref="PgBlockingCollector"/> and
    /// <see cref="PgIoStatsCollector"/>, not <see cref="PgAutovacuumStatsCollector"/>, whose source really
    /// does report zeros on a replica.</para>
    ///
    /// <para>Reading no <see cref="CollectorTargetInfo"/> fact is also what keeps this clear of
    /// <c>EveryFactAPostgresGateReads_IsVariedBySweepOrFixedByKind</c>: there is no fact for the kind sweep
    /// to have to vary, so no swept shape can fail the gate and no permanent-gap claim can be manufactured
    /// from it.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* The row's identity and the key any read differences on. NULL is a legitimate value here — the
           shared-relations row — and PARTITION BY / GROUP BY both treat NULLs as one group, so it keys
           correctly without a sentinel. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        /* Commits and rollbacks, whose RATIO is the finding: a rollback storm is invisible in a rollback
           count alone, because a busy database legitimately rolls back more transactions than a quiet one. */
        new CollectorColumn("xact_commit", CollectorColumnType.BigInt),
        new CollectorColumn("xact_rollback", CollectorColumnType.BigInt),
        /* Blocks found in shared_buffers versus blocks that had to be fetched. blks_read is NOT a disk-read
           count: a "miss" here may still be served by the OS page cache or, on Aurora, by the storage
           layer's own cache. It bounds the problem rather than measuring the disk, which is what the read
           says when it reports the ratio. */
        new CollectorColumn("blks_read", CollectorColumnType.BigInt),
        new CollectorColumn("blks_hit", CollectorColumnType.BigInt),
        /* The reason this collector exists. Both are needed: temp_files without temp_bytes cannot separate
           a thousand tiny spills from one enormous one, and those want different answers (a per-query
           work_mem bump versus a plan or index fix). */
        new CollectorColumn("temp_files", CollectorColumnType.BigInt),
        new CollectorColumn("temp_bytes", CollectorColumnType.BigInt),
        /* Server-recorded, unlike pg_blocking's periodic sample: PostgreSQL's deadlock detector increments
           this whether or not anyone was looking, so a zero here really is an all-clear for the window
           rather than "none was sampled". */
        new CollectorColumn("deadlocks", CollectorColumnType.BigInt),
        /* The explicit reset signal, and the load-bearing column of the set. Counter arithmetic can only
           see a reset that made a counter go BACKWARDS; a reset followed by enough activity to climb back
           past the old value inside one collection interval is invisible to the differences and visible
           here. That case is why this is stored rather than inferred. */
        new CollectorColumn("stats_reset", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                XactCommit: Long(reader, 1),
                XactRollback: Long(reader, 2),
                BlksRead: Long(reader, 3),
                BlksHit: Long(reader, 4),
                TempFiles: Long(reader, 5),
                TempBytes: Long(reader, 6),
                Deadlocks: Long(reader, 7),
                StatsReset: reader.IsDBNull(8) ? null : reader.GetDateTime(8)));
        }

        return rows;

        /* Nullable, like pg_io_stats and for the same reason: these are cumulative counters that get
           differenced, and the -1 "not applicable" sentinel the LEVEL collectors use would produce a
           garbage interval when subtracted from a real value. NULL propagates through the subtraction and
           drops out of the sum instead. stats_reset is genuinely NULL until the first reset ever, which is
           the common case and means exactly "never reset". */
        static long? Long(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)
            .Value(row.XactCommit)
            .Value(row.XactRollback)
            .Value(row.BlksRead)
            .Value(row.BlksHit)
            .Value(row.TempFiles)
            .Value(row.TempBytes)
            .Value(row.Deadlocks)
            .Value(row.StatsReset);
    }
}
