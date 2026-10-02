/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The data-file I/O counters of <c>pg_io_stats</c> (<c>pg_stat_io</c>, PG 16+, one-minute cadence) differenced
    /// over the window, per identity, then summed: reads and their time, writes and their time. <c>$1</c>
    /// server_id, <c>$2</c>/<c>$3</c> window. Only <c>object_type = 'relation'</c> — data files; temp relations and
    /// WAL (PG 18) are different storage paths with different latencies and would blur the one number this fact
    /// states. Every <c>context</c> (normal, vacuum, bulkread, bulkwrite) and every <c>backend_type</c> is in: the
    /// operator asks how slow the storage is, not which backend noticed.
    ///
    /// <para><b>The differencing is <c>DarlingPgDatabaseReader.PgDatabaseSql</c>'s, per identity.</b> <c>pg_stat_io</c>
    /// is one cumulative row per (backend_type, object_type, context), so the series is partitioned by that
    /// triple — the shape <c>PgTargetFactCollector.Buffer.cs</c>'s eviction CTE already takes on this table.
    /// Per-sample <c>LAG</c>, <c>GREATEST(raw, 0)</c> so a rewound counter contributes nothing, the reset detected
    /// as <c>ROW_NUMBER() OVER series &gt; 1 AND stats_reset IS DISTINCT FROM LAG(stats_reset)</c> (the NULL →
    /// timestamp first-reset trap that read records; not re-derived here). One <c>stats_reset</c> stamp guards
    /// both families because <c>pg_stat_io</c> resets as one view.</para>
    ///
    /// <para><b>NULL is "not reported", never zero — and here it is the whole write side on Aurora.</b> Aurora
    /// backends do not write data files, so <c>writes</c> / <c>write_time_ms</c> are NULL on every row there (the
    /// V69 rung's comment records it); <c>GREATEST(NULL, 0)</c> is NULL, the SUM skips it, and <c>writes_tracked</c>
    /// separates "no writes" from "no write counter". The checkpointer's <c>reads</c> is NULL for the same reason
    /// (it performs none) and drops out of the read sum the same way.</para>
    ///
    /// <para><b>Timing off is detected on the DATA, not the knob.</b> With <c>track_io_timing = off</c> the engine
    /// counts every read and stores 0 for its time, so Δreads &gt; 0 with Δread_time_ms = 0 over a whole window is
    /// the signature; the config fact, when collected, is stamped beside it as corroboration. The knob can be
    /// changed without a restart, so a window can straddle the flip — the data says what the window measured.</para>
    /// </summary>
    public const string PgTargetIoLatencySql = @"
WITH sampled AS (
    SELECT
        collection_time,
        backend_type,
        context,
        reads         - LAG(reads)         OVER series AS raw_reads,
        read_time_ms  - LAG(read_time_ms)  OVER series AS raw_read_ms,
        writes        - LAG(writes)        OVER series AS raw_writes,
        write_time_ms - LAG(write_time_ms) OVER series AS raw_write_ms,
        (ROW_NUMBER() OVER series > 1
         AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here,
        (reads IS NOT NULL)  AS reads_tracked,
        (writes IS NOT NULL) AS writes_tracked
    FROM pg_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   object_type = 'relation'
    WINDOW series AS (
        PARTITION BY backend_type, object_type, context
        ORDER BY collection_time
    )
)
SELECT
    CAST(coalesce(SUM(GREATEST(raw_reads, 0)), 0) AS bigint)  AS reads,
    coalesce(SUM(GREATEST(raw_read_ms, 0)), 0)                AS read_ms,
    CAST(coalesce(SUM(GREATEST(raw_writes, 0)), 0) AS bigint) AS writes,
    coalesce(SUM(GREATEST(raw_write_ms, 0)), 0)               AS write_ms,
    coalesce(bool_or(reads_tracked), false)                   AS reads_tracked,
    coalesce(bool_or(writes_tracked), false)                  AS writes_tracked,
    CAST(count(*) FILTER (WHERE reset_here) AS integer)       AS stats_reset_count,
    CAST(count(DISTINCT collection_time) AS integer)          AS sample_count,
    CAST(count(DISTINCT collection_time) FILTER (WHERE raw_reads IS NOT NULL OR raw_writes IS NOT NULL) AS integer) AS differenced_samples,
    CAST(count(DISTINCT ROW(backend_type, context)) FILTER (WHERE raw_reads IS NOT NULL OR raw_writes IS NOT NULL) AS integer) AS identity_count
FROM sampled";

    /// <summary>
    /// <c>PG_IO_READ_LATENCY_MS</c> / <c>PG_IO_WRITE_LATENCY_MS</c> from <c>pg_io_stats</c> (<c>pg_stat_io</c>, PostgreSQL 16+).
    /// The pattern copied is <c>PgTargetFactCollector.Write.cs</c> — reset-aware counter differencing, the NULL →
    /// timestamp first-reset trap recorded in <c>DarlingPgDatabaseReader.PgDatabaseSql</c>, never re-derived.
    /// <para>/* filled by lane 11 of #3691 (design §3.9). Two facts, each ms PER OPERATION over the window
    /// (<see cref="Fact.Value"/>), with the trackedness vocabulary of <c>PgTargetScorer.Io.cs</c> stamped so the
    /// scorer grades exactly the measured shape and the advice can say what the pass could not know. */</para>
    ///
    /// <para><b>The three-way trackedness decision, in order.</b> (1) The registry major, off the
    /// <c>PG_SERVER_MAJOR_VERSION</c> fact emitted first in the pass: below <see cref="PgTargetScorer.IoStatIoMinimumMajor"/>
    /// there is no <c>pg_stat_io</c> and the read fact is emitted <c>unavailable</c> with
    /// <see cref="PgTargetScorer.IoReasonPgStatIoAbsentKey"/> — no store read, because there is nothing to read
    /// and a window of zero rows would say the same thing less honestly. A registry with NO major (a NULL no
    /// connect has stamped) is not "old": the read proceeds and the rows decide. (2) The window: fewer than two
    /// differenced collections, or a counter never reported, and there is no fact — a difference needs two rows,
    /// and a family whose table this flavour never wrote is structurally absent (the D6 disclosure's job, not a
    /// fact's). (3) Timing: operations counted with their time summing to exactly 0 is <c>track_io_timing = off</c>
    /// — <c>unavailable</c> with <see cref="PgTargetScorer.IoReasonTrackIoTimingOffKey"/>, <see cref="Fact.Value"/>
    /// = 0 as the placeholder of a fact that makes no claim (never rendered as 0.000 ms — the #3541 A8 lesson; the
    /// scorer refuses it and the advice reads the flag). Otherwise the quotient is a measurement, graded only when
    /// the window's operations clear <see cref="PgTargetScorer.IoMinimumOps"/> (below it,
    /// <see cref="PgTargetScorer.IoInsufficientOpsKey"/> = 1 and the quotient is stated, not graded).</para>
    ///
    /// <para><b>Rates are over OBSERVED time (#3538 A7).</b> <c>ops_per_sec</c> divides by
    /// <see cref="AnalysisContext.ObservedDurationMs"/>, never the nominal window. The per-op quotient needs no
    /// divisor at all, which is why it is the value: a 24-hour and a 4-hour read of steady storage say the same
    /// milliseconds.</para>
    ///
    /// <para><b>What the write fact is in v2.</b> Emitted only when the engine reported a write counter at all
    /// (<c>writes_tracked</c>; on Aurora it is NULL throughout and the fact is absent, not zero) — same flags, same
    /// quotient, NO grade (<c>PgTargetScorer.Io.cs</c>: no measured write bar exists). It is context for the
    /// checkpoint story to read later and a number the operator can see today.</para>
    /// </summary>
    private async partial Task CollectIoFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        var registry = facts.Find(f => f.Key == PgTargetFactKeys.ServerMajorVersion);
        var major = registry is null ? 0 : (int)registry.Value;
        var isAurora = registry is not null && registry.Metadata.GetValueOrDefault("is_aurora") > 0;
        var timingKnob = facts.Find(f => f.Key == PgTargetFactKeys.ConfigTrackIoTiming);

        if (major > 0 && major < PgTargetScorer.IoStatIoMinimumMajor)
        {
            /* (1) No pg_stat_io on this major: the family cannot know, and says so without a store read. */
            var absent = new Fact
            {
                Source = PgTargetSources.IoSource,
                Key = PgTargetFactKeys.IoReadLatencyMs,
                Value = 0,
                ServerId = context.ServerId,
                Metadata =
                {
                    [PgTargetScorer.IoLatencyMeasuredKey] = 0,
                    [PgTargetScorer.IoUnavailableKey] = 1,
                    [PgTargetScorer.IoReasonPgStatIoAbsentKey] = 1,
                    [PgTargetScorer.IoServerMajorKey] = major,
                    [PgTargetScorer.IoIsAuroraKey] = isAurora ? 1 : 0,
                    [PgTargetScorer.IoObservedMsKey] = context.ObservedDurationMs,
                },
            };
            StampTimingKnob(absent, timingKnob);
            facts.Add(absent);
            return;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetIoLatencySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var sampleCount = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7));
            var differencedSamples = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8));
            /* (2) A difference needs two rows; a window with none differenced is a family with nothing to say. */
            if (sampleCount < 2 || differencedSamples < 1) return;

            var reads = ToInt64(reader.GetValue(0));
            var readMs = Convert.ToDouble(reader.GetValue(1));
            var writes = ToInt64(reader.GetValue(2));
            var writeMs = Convert.ToDouble(reader.GetValue(3));
            var readsTracked = !reader.IsDBNull(4) && reader.GetBoolean(4);
            var writesTracked = !reader.IsDBNull(5) && reader.GetBoolean(5);
            var resetCount = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6));
            var identityCount = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9));

            if (readsTracked)
            {
                facts.Add(LatencyFact(context, PgTargetFactKeys.IoReadLatencyMs, reads, readMs, resetCount, sampleCount, identityCount, major, isAurora, timingKnob));
            }

            if (writesTracked)
            {
                facts.Add(LatencyFact(context, PgTargetFactKeys.IoWriteLatencyMs, writes, writeMs, resetCount, sampleCount, identityCount, major, isAurora, timingKnob));
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_io_stats arrived in V69; a pre-migration store raises 42P01 here, which the reporter classifies
               quiet. Degrades to "no facts" so one unavailable input cannot cost this server its other facts. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// One latency fact from a differenced (ops, ms) pair — the trackedness decision (3) above, shared by the read
    /// and write facts so the two cannot disagree about what "timing off" looks like. Ops &gt; 0 with time exactly
    /// 0 is the untimed signature; ops = 0 is "no operations" and is stated as such under the floor (a measured
    /// zero rate, not a latency); anything else is a measured quotient.
    /// </summary>
    private static Fact LatencyFact(
        AnalysisContext context, string key, long ops, double opMs, int resetCount, int sampleCount, int identityCount,
        int major, bool isAurora, Fact? timingKnob)
    {
        var observedSeconds = context.ObservedDurationMs / 1000.0;
        var timingOff = ops > 0 && opMs <= 0;
        var measured = !timingOff;

        var fact = new Fact
        {
            Source = PgTargetSources.IoSource,
            Key = key,
            Value = measured && ops > 0 ? opMs / ops : 0.0,
            ServerId = context.ServerId,
            Metadata =
            {
                [PgTargetScorer.IoLatencyMeasuredKey] = measured ? 1 : 0,
                [PgTargetScorer.IoUnavailableKey] = timingOff ? 1 : 0,
                [PgTargetScorer.IoOpsKey] = ops,
                [PgTargetScorer.IoOpTimeMsKey] = opMs,
                [PgTargetScorer.IoOpsPerSecKey] = ops / observedSeconds,
                [PgTargetScorer.IoOpsTrackedKey] = 1,
                [PgTargetScorer.IoResetCountKey] = resetCount,
                [PgTargetScorer.IoSampleCountKey] = sampleCount,
                [PgTargetScorer.IoIdentityCountKey] = identityCount,
                [PgTargetScorer.IoObservedMsKey] = context.ObservedDurationMs,
                [PgTargetScorer.IoServerMajorKey] = major,
                [PgTargetScorer.IoIsAuroraKey] = isAurora ? 1 : 0,
            },
        };

        if (timingOff)
            fact.Metadata[PgTargetScorer.IoReasonTrackIoTimingOffKey] = 1;
        else
            fact.Metadata[PgTargetScorer.IoInsufficientOpsKey] = ops < PgTargetScorer.IoMinimumOps ? 1 : 0;

        StampTimingKnob(fact, timingKnob);
        return fact;
    }

    /// <summary>The <c>track_io_timing</c> knob's value beside the data-side decision, when lane 2's config
    /// snapshot was collected (emission order: Config before Io). Absent when it was not — the advice then
    /// speaks from the data alone rather than assuming the default.</summary>
    private static void StampTimingKnob(Fact fact, Fact? timingKnob)
    {
        if (timingKnob is not null)
            fact.Metadata[PgTargetScorer.IoTrackIoTimingConfigKey] = timingKnob.Value;
    }
}
