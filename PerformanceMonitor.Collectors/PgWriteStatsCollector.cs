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
/// The write side of the cluster: checkpoints, background writing, and WAL — the first slice of #2544.
///
/// <para><b>One collector for three views, because they are one story.</b> <c>pg_stat_checkpointer</c>,
/// <c>pg_stat_bgwriter</c> and <c>pg_stat_wal</c> answer a single question between them: is this server
/// keeping up with the writes it is being given. A requested checkpoint climbing against timed ones means
/// <c>max_wal_size</c> is too small; that shows as WAL volume in the same breath, and as buffers the
/// background writer could not clean. Splitting them across three collectors would put the numerator and
/// the denominator of every useful ratio in different tables with different collection times.</para>
///
/// <para><b>All three are cluster-wide singletons</b>, so this is exactly one row per snapshot — which is
/// what makes a single wide row the right shape here, where <see cref="PgIoStatsCollector"/>'s
/// dimensioned view needs many narrow ones.</para>
///
/// <para><b>The version surface is the whole difficulty, and it is worse than a rename.</b> Measured
/// across four majors rather than read from release notes:</para>
///
/// <list type="bullet">
/// <item><b>17 gutted <c>pg_stat_bgwriter</c></b>, taking seven of its eleven columns. Five moved to the new
/// <c>pg_stat_checkpointer</c> under new names; <c>buffers_backend</c> and <c>buffers_backend_fsync</c> moved
/// nowhere — that information now lives in <c>pg_stat_io</c>, which is a different view with a different
/// shape. A rename table alone would silently drop the two columns that say whether backends are writing
/// their own buffers.</item>
/// <item><b>18 removed four columns from <c>pg_stat_wal</c></b> — <c>wal_write</c>, <c>wal_sync</c>,
/// <c>wal_write_time</c>, <c>wal_sync_time</c> — and added <c>num_done</c> and <c>slru_written</c> to
/// <c>pg_stat_checkpointer</c>. So this is a moving surface across at least three majors, not a one-time
/// 16→17 fix-up.</item>
/// </list>
///
/// <para><b>The stored shape is the UNION and never changes.</b> A column a major does not supply is NULL,
/// not zero and not absent: the table has to mean the same thing on a 16 and an 18 target in the same store,
/// and a read that differences a counter cannot tell a real zero from an unavailable one. Every reference is
/// version-gated by a <c>&gt;=</c> floor rather than an equality, so a 19 that keeps 18's shape does not fall
/// off the end into a column that no longer exists.</para>
///
/// <para><b>Post-17 names are canonical.</b> <c>num_timed</c>, not <c>checkpoints_timed</c>. The older names
/// are where most people's knowledge is, but they are the ones PostgreSQL is moving away from, and
/// translating for a reader is a documentation job rather than a reason to freeze a deprecated vocabulary
/// into a schema.</para>
///
/// <para>Runs on standbys deliberately. <c>pg_stat_checkpointer</c>'s <c>restartpoints_*</c> columns exist
/// precisely to describe a replica, and a standby that cannot keep up with restartpoints is exactly the
/// thing nobody is watching.</para>
/// </summary>
public sealed class PgWriteStatsCollector : PostgresCollectorDefinitionBase<PgWriteStatsCollector.Row>
{
    public static PgWriteStatsCollector Instance { get; } = new();

    private PgWriteStatsCollector()
    {
    }

    /// <param name="NumTimed">Checkpoints begun because <c>checkpoint_timeout</c> elapsed.</param>
    /// <param name="NumRequested">Checkpoints begun because WAL volume demanded one — the
    /// <c>max_wal_size</c> signal when it climbs against <paramref name="NumTimed"/>.</param>
    /// <param name="NumDone">Checkpoints completed (18+; NULL below).</param>
    /// <param name="BuffersBackend">Buffers written by a backend itself (≤16; NULL from 17, where the fact
    /// moved to <c>pg_stat_io</c> rather than to the checkpointer).</param>
    public readonly record struct Row(
        long? NumTimed,
        long? NumRequested,
        long? NumDone,
        long? RestartpointsTimed,
        long? RestartpointsReq,
        long? RestartpointsDone,
        double? CheckpointWriteTimeMs,
        double? CheckpointSyncTimeMs,
        long? BuffersWrittenCheckpoint,
        long? SlruWritten,
        DateTime? CheckpointerStatsReset,
        long? BuffersClean,
        long? MaxwrittenClean,
        long? BuffersAlloc,
        long? BuffersBackend,
        long? BuffersBackendFsync,
        DateTime? BgwriterStatsReset,
        long? WalRecords,
        long? WalFpi,
        decimal? WalBytes,
        long? WalBuffersFull,
        long? WalWrite,
        long? WalSync,
        double? WalWriteTimeMs,
        double? WalSyncTimeMs,
        DateTime? WalStatsReset);

    /* Three joinless scalar subqueries rather than a CROSS JOIN of three views. Each view is a guaranteed
       single row, so the result is one row either way, but a subquery per column keeps every version branch
       local to the column it affects instead of forcing the whole FROM clause to be version-shaped.

       stats_reset is timestamptz on all three; AT TIME ZONE 'UTC' rather than ::timestamp, because the cast
       renders in the SESSION's TimeZone and the store contract is naive UTC. Same rule as pg_stat_io.

       All three views are kept as separate stats_reset columns. They CAN be reset independently
       (pg_stat_reset_shared takes a target), and a read that differenced across a reset it could not see
       would report a negative interval as a huge positive one. */
    private static string BuildQueryText(int postgresMajorVersion)
    {
        var hasCheckpointer = postgresMajorVersion >= 17;
        var has18 = postgresMajorVersion >= 18;

        /* ---- checkpointer: its own view from 17, still inside pg_stat_bgwriter below that ---- */
        var numTimed = hasCheckpointer
            ? "(SELECT num_timed FROM pg_catalog.pg_stat_checkpointer)"
            : "(SELECT checkpoints_timed FROM pg_catalog.pg_stat_bgwriter)";
        var numRequested = hasCheckpointer
            ? "(SELECT num_requested FROM pg_catalog.pg_stat_checkpointer)"
            : "(SELECT checkpoints_req FROM pg_catalog.pg_stat_bgwriter)";
        var checkpointWrite = hasCheckpointer
            ? "(SELECT write_time FROM pg_catalog.pg_stat_checkpointer)"
            : "(SELECT checkpoint_write_time FROM pg_catalog.pg_stat_bgwriter)";
        var checkpointSync = hasCheckpointer
            ? "(SELECT sync_time FROM pg_catalog.pg_stat_checkpointer)"
            : "(SELECT checkpoint_sync_time FROM pg_catalog.pg_stat_bgwriter)";
        var buffersWritten = hasCheckpointer
            ? "(SELECT buffers_written FROM pg_catalog.pg_stat_checkpointer)"
            : "(SELECT buffers_checkpoint FROM pg_catalog.pg_stat_bgwriter)";
        var checkpointerReset = hasCheckpointer
            ? "(SELECT stats_reset AT TIME ZONE 'UTC' FROM pg_catalog.pg_stat_checkpointer)"
            /* Below 17 the checkpoint counters live in pg_stat_bgwriter, so its reset IS theirs. Reporting
               the same instant twice is honest here; reporting NULL would suggest the counters had no known
               reset point, which is a different and wrong claim. */
            : "(SELECT stats_reset AT TIME ZONE 'UTC' FROM pg_catalog.pg_stat_bgwriter)";

        /* restartpoints_* arrived WITH pg_stat_checkpointer. There is no pre-17 equivalent to fall back on:
           a standby's restartpoint activity was simply not exposed. */
        var restartTimed = hasCheckpointer ? "(SELECT restartpoints_timed FROM pg_catalog.pg_stat_checkpointer)" : "NULL::bigint";
        var restartReq = hasCheckpointer ? "(SELECT restartpoints_req FROM pg_catalog.pg_stat_checkpointer)" : "NULL::bigint";
        var restartDone = hasCheckpointer ? "(SELECT restartpoints_done FROM pg_catalog.pg_stat_checkpointer)" : "NULL::bigint";

        var numDone = has18 ? "(SELECT num_done FROM pg_catalog.pg_stat_checkpointer)" : "NULL::bigint";
        var slruWritten = has18 ? "(SELECT slru_written FROM pg_catalog.pg_stat_checkpointer)" : "NULL::bigint";

        /* ---- bgwriter: buffers_backend / buffers_backend_fsync were REMOVED at 17 with no successor in
               the checkpointer. pg_stat_io carries that information now, and PgIoStatsCollector already
               collects it, so it is not lost - but it is not here either, and pretending otherwise with a
               zero would be worse than a NULL a reader can ask about. ---- */
        var buffersBackend = hasCheckpointer ? "NULL::bigint" : "(SELECT buffers_backend FROM pg_catalog.pg_stat_bgwriter)";
        var buffersBackendFsync = hasCheckpointer ? "NULL::bigint" : "(SELECT buffers_backend_fsync FROM pg_catalog.pg_stat_bgwriter)";

        /* ---- WAL: 18 removed the four timing/count columns. This is the branch that would take the whole
               collection down with 42703 on an 18 target if it were selected unconditionally, which is the
               specific hazard #2544 was reopened to describe. ---- */
        var walWrite = has18 ? "NULL::bigint" : "w.wal_write";
        var walSync = has18 ? "NULL::bigint" : "w.wal_sync";
        var walWriteTime = has18 ? "NULL::double precision" : "w.wal_write_time";
        var walSyncTime = has18 ? "NULL::double precision" : "w.wal_sync_time";

        return $@"
SELECT
    {numTimed}                              AS num_timed,
    {numRequested}                          AS num_requested,
    {numDone}                               AS num_done,
    {restartTimed}                          AS restartpoints_timed,
    {restartReq}                            AS restartpoints_req,
    {restartDone}                           AS restartpoints_done,
    {checkpointWrite}                       AS checkpoint_write_time_ms,
    {checkpointSync}                        AS checkpoint_sync_time_ms,
    {buffersWritten}                        AS buffers_written_checkpoint,
    {slruWritten}                           AS slru_written,
    {checkpointerReset}                     AS checkpointer_stats_reset,
    b.buffers_clean                         AS buffers_clean,
    b.maxwritten_clean                      AS maxwritten_clean,
    b.buffers_alloc                         AS buffers_alloc,
    {buffersBackend}                        AS buffers_backend,
    {buffersBackendFsync}                   AS buffers_backend_fsync,
    (b.stats_reset AT TIME ZONE 'UTC')      AS bgwriter_stats_reset,
    w.wal_records                           AS wal_records,
    w.wal_fpi                               AS wal_fpi,
    /* wal_bytes is NUMERIC, not bigint - it is allowed to exceed 2^63 over a long uptime, which is exactly
       why upstream chose numeric. Cast to a bounded numeric so the store's column type is decidable, and
       carried through C# as decimal rather than long for the same reason. */
    w.wal_bytes::numeric(38,0)              AS wal_bytes,
    w.wal_buffers_full                      AS wal_buffers_full,
    {walWrite}                              AS wal_write,
    {walSync}                               AS wal_sync,
    {walWriteTime}                          AS wal_write_time_ms,
    {walSyncTime}                           AS wal_sync_time_ms,
    (w.stats_reset AT TIME ZONE 'UTC')      AS wal_stats_reset
FROM pg_catalog.pg_stat_bgwriter AS b
CROSS JOIN pg_catalog.pg_stat_wal AS w";
    }

    public override string Name => "pg_write_stats";

    public override string TargetTable => "pg_write_stats";

    /// <summary>
    /// <c>pg_stat_wal</c> is PostgreSQL 14+, which is the binding floor — <c>pg_stat_bgwriter</c> long
    /// predates it. Standbys included: <c>restartpoints_*</c> exists to describe exactly that case.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => target.PostgresMajorVersion >= 14;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* ---- checkpointer ---- */
        new CollectorColumn("num_timed", CollectorColumnType.BigInt),
        new CollectorColumn("num_requested", CollectorColumnType.BigInt),
        new CollectorColumn("num_done", CollectorColumnType.BigInt),
        new CollectorColumn("restartpoints_timed", CollectorColumnType.BigInt),
        new CollectorColumn("restartpoints_req", CollectorColumnType.BigInt),
        new CollectorColumn("restartpoints_done", CollectorColumnType.BigInt),
        new CollectorColumn("checkpoint_write_time_ms", CollectorColumnType.Double),
        new CollectorColumn("checkpoint_sync_time_ms", CollectorColumnType.Double),
        new CollectorColumn("buffers_written_checkpoint", CollectorColumnType.BigInt),
        new CollectorColumn("slru_written", CollectorColumnType.BigInt),
        new CollectorColumn("checkpointer_stats_reset", CollectorColumnType.Timestamp),

        /* ---- background writer ---- */
        new CollectorColumn("buffers_clean", CollectorColumnType.BigInt),
        /* Times the bgwriter stopped a cleaning round because it hit bgwriter_lru_maxpages. Sustained
           non-zero is the "the background writer is capped, not idle" signal, and it is the one bgwriter
           column people most often never look at. */
        new CollectorColumn("maxwritten_clean", CollectorColumnType.BigInt),
        new CollectorColumn("buffers_alloc", CollectorColumnType.BigInt),
        new CollectorColumn("buffers_backend", CollectorColumnType.BigInt),
        new CollectorColumn("buffers_backend_fsync", CollectorColumnType.BigInt),
        new CollectorColumn("bgwriter_stats_reset", CollectorColumnType.Timestamp),

        /* ---- WAL ---- */
        new CollectorColumn("wal_records", CollectorColumnType.BigInt),
        /* Full-page images. Climbing FPI right after each checkpoint is the classic
           checkpoint_timeout-too-low shape, and it is only legible next to the checkpoint counters above -
           which is the argument for these three views being one collector. */
        new CollectorColumn("wal_fpi", CollectorColumnType.BigInt),
        new CollectorColumn("wal_bytes", CollectorColumnType.Decimal, 38, 0),
        new CollectorColumn("wal_buffers_full", CollectorColumnType.BigInt),
        new CollectorColumn("wal_write", CollectorColumnType.BigInt),
        new CollectorColumn("wal_sync", CollectorColumnType.BigInt),
        new CollectorColumn("wal_write_time_ms", CollectorColumnType.Double),
        new CollectorColumn("wal_sync_time_ms", CollectorColumnType.Double),
        new CollectorColumn("wal_stats_reset", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                NumTimed: Long(reader, 0),
                NumRequested: Long(reader, 1),
                NumDone: Long(reader, 2),
                RestartpointsTimed: Long(reader, 3),
                RestartpointsReq: Long(reader, 4),
                RestartpointsDone: Long(reader, 5),
                CheckpointWriteTimeMs: Double(reader, 6),
                CheckpointSyncTimeMs: Double(reader, 7),
                BuffersWrittenCheckpoint: Long(reader, 8),
                SlruWritten: Long(reader, 9),
                CheckpointerStatsReset: Stamp(reader, 10),
                BuffersClean: Long(reader, 11),
                MaxwrittenClean: Long(reader, 12),
                BuffersAlloc: Long(reader, 13),
                BuffersBackend: Long(reader, 14),
                BuffersBackendFsync: Long(reader, 15),
                BgwriterStatsReset: Stamp(reader, 16),
                WalRecords: Long(reader, 17),
                WalFpi: Long(reader, 18),
                WalBytes: reader.IsDBNull(19) ? null : reader.GetDecimal(19),
                WalBuffersFull: Long(reader, 20),
                WalWrite: Long(reader, 21),
                WalSync: Long(reader, 22),
                WalWriteTimeMs: Double(reader, 23),
                WalSyncTimeMs: Double(reader, 24),
                WalStatsReset: Stamp(reader, 25)));
        }

        return rows;

        /* Nullable throughout, and never a -1 sentinel. These are cumulative counters that get differenced
           at read time, and a sentinel differenced against a real value produces a garbage interval - the
           same reasoning PgIoStatsCollector records. Here NULL additionally carries a second meaning that
           matters more: "this major does not have this column at all". */
        static long? Long(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);
        static double? Double(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDouble(ordinal);
        static DateTime? Stamp(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDateTime(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No stored deltas. Every column is a cumulative counter, and the windowed change is computed at
           read time against the recorded stats_reset - which is why all three resets are stored. */
        writer
            .Value(row.NumTimed)
            .Value(row.NumRequested)
            .Value(row.NumDone)
            .Value(row.RestartpointsTimed)
            .Value(row.RestartpointsReq)
            .Value(row.RestartpointsDone)
            .Value(row.CheckpointWriteTimeMs)
            .Value(row.CheckpointSyncTimeMs)
            .Value(row.BuffersWrittenCheckpoint)
            .Value(row.SlruWritten)
            .Value(row.CheckpointerStatsReset)
            .Value(row.BuffersClean)
            .Value(row.MaxwrittenClean)
            .Value(row.BuffersAlloc)
            .Value(row.BuffersBackend)
            .Value(row.BuffersBackendFsync)
            .Value(row.BgwriterStatsReset)
            .Value(row.WalRecords)
            .Value(row.WalFpi)
            .Value(row.WalBytes)
            .Value(row.WalBuffersFull)
            .Value(row.WalWrite)
            .Value(row.WalSync)
            .Value(row.WalWriteTimeMs)
            .Value(row.WalSyncTimeMs)
            .Value(row.WalStatsReset);
    }
}
