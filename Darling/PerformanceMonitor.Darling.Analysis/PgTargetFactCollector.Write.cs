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
    /// The checkpointer and WAL counters of <c>pg_write_stats</c> (PG 14+, one-minute cadence) differenced over
    /// the window: requested and timed checkpoints, checkpoint write / sync time, buffers the checkpointer
    /// wrote, WAL bytes and full-page images. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window.
    ///
    /// <para><b>The differencing is <c>DarlingPgDatabaseReader.PgDatabaseSql</c>'s, applied to this table's two
    /// reset stamps.</b> Per-sample <c>LAG</c> over the series, <c>GREATEST(raw, 0)</c> so a rewound counter
    /// contributes nothing rather than a negative, and the reset detected as <c>ROW_NUMBER() OVER series &gt; 1
    /// AND stamp IS DISTINCT FROM LAG(stamp)</c> — the comment on that read records why a <c>LAG(stamp) IS NOT
    /// NULL</c> guard misses a database's FIRST reset (NULL → timestamp) and it is not re-derived here. The
    /// checkpointer family is guarded by <c>checkpointer_stats_reset</c> and the WAL family by
    /// <c>wal_stats_reset</c>, because <c>pg_stat_reset_shared</c> resets them independently (the collector
    /// stores three stamps for exactly this reason).</para>
    ///
    /// <para><b>Why per-sample <c>LAG</c> and not the tool's first-minus-last.</b>
    /// <c>DarlingPgWriteStatsReader.PgWriteStatsSql</c> differences the window's edges and reports a family as
    /// NULL when its stamp moved anywhere inside the window — the right answer for a tool that must never
    /// show a number from two epochs. An analysis pass wants the sum of what it OBSERVED: a reset mid-window
    /// should cost one interval, not the whole window's evidence, and the count of resets is carried as
    /// metadata so the reader knows the sum is a floor. The first in-window row has no predecessor and
    /// contributes nothing, exactly as the coverage witness treats it.</para>
    ///
    /// <para><b>NULL is "not reported", never zero.</b> <c>wal_bytes</c> is NULL on every Aurora row
    /// (<c>pg_stat_wal</c> is not implemented there; the collector's comment explains the typed NULL), so
    /// <c>GREATEST(NULL, 0)</c> sums to 0 and <c>wal_tracked</c> is what separates "no WAL" from "no
    /// <c>pg_stat_wal</c>" — the fact carries it and the advice says which.</para>
    /// </summary>
    public const string PgTargetCheckpointSql = @"
WITH sampled AS (
    SELECT
        collection_time,
        num_requested              - LAG(num_requested)              OVER series AS raw_requested,
        num_timed                  - LAG(num_timed)                  OVER series AS raw_timed,
        checkpoint_write_time_ms   - LAG(checkpoint_write_time_ms)   OVER series AS raw_write_ms,
        checkpoint_sync_time_ms    - LAG(checkpoint_sync_time_ms)    OVER series AS raw_sync_ms,
        buffers_written_checkpoint - LAG(buffers_written_checkpoint) OVER series AS raw_ckpt_buffers,
        wal_bytes                  - LAG(wal_bytes)                  OVER series AS raw_wal_bytes,
        wal_fpi                    - LAG(wal_fpi)                    OVER series AS raw_wal_fpi,
        (ROW_NUMBER() OVER series > 1
         AND checkpointer_stats_reset IS DISTINCT FROM LAG(checkpointer_stats_reset) OVER series) AS ck_reset_here,
        (ROW_NUMBER() OVER series > 1
         AND wal_stats_reset IS DISTINCT FROM LAG(wal_stats_reset) OVER series) AS wal_reset_here,
        (wal_bytes IS NOT NULL) AS wal_tracked
    FROM pg_write_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (ORDER BY collection_time)
)
SELECT
    CAST(coalesce(SUM(GREATEST(raw_requested, 0)), 0) AS bigint)    AS checkpoints_requested,
    CAST(coalesce(SUM(GREATEST(raw_timed, 0)), 0) AS bigint)        AS checkpoints_timed,
    coalesce(SUM(GREATEST(raw_write_ms, 0)), 0)                     AS checkpoint_write_ms,
    coalesce(SUM(GREATEST(raw_sync_ms, 0)), 0)                      AS checkpoint_sync_ms,
    CAST(coalesce(SUM(GREATEST(raw_ckpt_buffers, 0)), 0) AS bigint) AS checkpoint_buffers_written,
    coalesce(SUM(GREATEST(raw_wal_bytes, 0)), 0)                    AS wal_bytes,
    CAST(coalesce(SUM(GREATEST(raw_wal_fpi, 0)), 0) AS bigint)      AS wal_fpi,
    CAST(count(*) FILTER (WHERE ck_reset_here) AS integer)          AS checkpointer_reset_count,
    CAST(count(*) FILTER (WHERE wal_reset_here) AS integer)         AS wal_reset_count,
    coalesce(bool_or(wal_tracked), false)                           AS wal_tracked,
    CAST(count(*) AS integer)                                       AS sample_count
FROM sampled";

    /// <summary>
    /// <c>PG_CHECKPOINT_PRESSURE</c> from <c>pg_write_stats</c> (filled by lane 2 — #3542 step 2, design §3.7):
    /// the engine's own <c>max_wal_size</c>-exhaustion report. A checkpoint is TIMED when
    /// <c>checkpoint_timeout</c> elapsed and REQUESTED when WAL reached <c>max_wal_size</c> first (or
    /// something asked for one — a base backup, a <c>CHECKPOINT</c> statement, a shutdown); a window in which
    /// requested checkpoints dominate is a window in which WAL volume, not the clock, is deciding when the
    /// server flushes every dirty buffer. <see cref="Fact.Value"/> is the REQUESTED SHARE, Δrequested /
    /// (Δrequested + Δtimed) in [0, 1], and the scorer grades that share; every raw figure rides as metadata
    /// so the advice can say what the server measured rather than what folklore expects.
    ///
    /// <para><b>Rates are over OBSERVED time (#3538 A7).</b> <c>checkpoints_per_hour</c>,
    /// <c>requested_per_hour</c> and <c>wal_bytes_per_sec</c> divide by <see cref="AnalysisContext.ObservedDurationMs"/>,
    /// never the nominal window, so a 24-hour read and a 4-hour read of the same server say the same thing.
    /// <c>expected_timed_checkpoints</c> is what <c>checkpoint_timeout</c> alone would have produced over the
    /// observed time — read off the <c>max_wal_size</c> fact the config collector emitted just before this
    /// method ran — so the advice can state "the engine ran N checkpoints where the timer alone would have
    /// run about M". That sentence is engine-defined; it is deliberately prose and metadata, not a bar.</para>
    ///
    /// <para><b>What is NOT emitted.</b> No fact when the window has no observed time, fewer than two samples
    /// (a difference needs two rows), or no checkpoints at all — an idle PostgreSQL still runs a timed
    /// checkpoint every <c>checkpoint_timeout</c>, so zero checkpoints over a window means the series is not
    /// there (below PG 14 the collector does not apply) and a fact would be a claim about nothing. WAL
    /// per-checkpoint and per-second figures are stamped only when <c>wal_tracked</c>; on Aurora they are
    /// absent, not zero.</para>
    /// </summary>
    private async partial Task CollectWriteFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetCheckpointSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var sampleCount = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10));
            if (sampleCount < 2) return;

            var requested = ToInt64(reader.GetValue(0));
            var timed = ToInt64(reader.GetValue(1));
            var total = requested + timed;
            if (total <= 0) return;

            var writeMs = Convert.ToDouble(reader.GetValue(2));
            var syncMs = Convert.ToDouble(reader.GetValue(3));
            var buffersWritten = ToInt64(reader.GetValue(4));
            var walBytes = Convert.ToDouble(reader.GetValue(5));
            var walFpi = ToInt64(reader.GetValue(6));
            var checkpointerResets = Convert.ToInt32(reader.GetValue(7));
            var walResets = Convert.ToInt32(reader.GetValue(8));
            var walTracked = !reader.IsDBNull(9) && reader.GetBoolean(9);

            var observedSeconds = context.ObservedDurationMs / 1000.0;
            var observedHours = observedSeconds / 3600.0;

            var fact = new Fact
            {
                Source = PgTargetSources.WriteSource,
                Key = PgTargetFactKeys.CheckpointPressure,
                Value = requested / (double)total,
                ServerId = context.ServerId,
                Metadata =
                {
                    ["checkpoints_requested"] = requested,
                    ["checkpoints_timed"] = timed,
                    ["checkpoints_total"] = total,
                    ["requested_share"] = requested / (double)total,
                    ["checkpoints_per_hour"] = total / observedHours,
                    ["requested_per_hour"] = requested / observedHours,
                    ["checkpoint_write_ms"] = writeMs,
                    ["checkpoint_sync_ms"] = syncMs,
                    ["checkpoint_buffers_written"] = buffersWritten,
                    ["checkpointer_reset_count"] = checkpointerResets,
                    ["wal_reset_count"] = walResets,
                    ["wal_tracked"] = walTracked ? 1 : 0,
                    ["sample_count"] = sampleCount,
                    ["observed_ms"] = context.ObservedDurationMs,
                },
            };

            if (walTracked)
            {
                fact.Metadata["wal_bytes"] = walBytes;
                fact.Metadata["wal_fpi"] = walFpi;
                fact.Metadata["wal_bytes_per_sec"] = walBytes / observedSeconds;
                fact.Metadata["wal_bytes_per_checkpoint"] = walBytes / total;
            }

            /* The checkpoint rhythm and the ceiling, off the knob fact emitted a moment ago (emission order:
               Config before Write). Absent when the config snapshot has not been collected — the advice then
               says so rather than assuming 5 min / 1 GB. */
            var maxWalSize = facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaxWalSize);
            if (maxWalSize is not null)
            {
                if (maxWalSize.Metadata.TryGetValue("bytes", out var maxWalBytes))
                    fact.Metadata["max_wal_size_bytes"] = maxWalBytes;
                if (maxWalSize.Metadata.TryGetValue("checkpoint_timeout_s", out var timeoutSeconds) && timeoutSeconds > 0)
                {
                    fact.Metadata["checkpoint_timeout_s"] = timeoutSeconds;
                    fact.Metadata["expected_timed_checkpoints"] = observedSeconds / timeoutSeconds;
                }
            }

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_write_stats arrived in V88; a pre-migration store raises 42P01 here, which the reporter
               classifies quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
