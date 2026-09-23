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
using PerformanceMonitor.Darling.Storage;

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
    /// <c>pg_stat_wal</c>" — the fact carries it and the advice says which. <c>wal_records</c> rides beside the
    /// bytes (the last column, appended so the earlier ordinals did not move) because NULL-ness is only ONE of the
    /// store's ways of saying "not reported": the trackedness decision itself is <see cref="WalIsTracked"/>, shared
    /// with the WAL-volume read, and it needs both sums.</para>
    ///
    /// <para><b>A restart-spanning interval's requested figure is unknown, not pressure (#3955).</b> A clean restart
    /// leaves every <c>stats_reset</c> stamp where it was, so the reset guards above cannot see it, and it ADDS a
    /// requested checkpoint: PostgreSQL counts the shutdown checkpoint in <c>num_requested</c> and keeps the count
    /// across the restart. <c>restart_here</c> is <see cref="PostmasterRestart.SpansSql"/> over the V139
    /// <c>postmaster_start_time</c> column, and the requested sum leaves those intervals out, the way a reset
    /// interval contributes nothing: one interval's evidence, not the window's. The timed count, the write and sync
    /// time and the WAL figures still sum every interval. <c>postmaster_restart_count</c> rides LAST, after
    /// <c>wal_records</c>, so the earlier ordinals did not move, and the fact carries it so the advice can say the
    /// requested total excludes those intervals.</para>
    /// </summary>
    public const string PgTargetCheckpointSql = $@"
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
        wal_records                - LAG(wal_records)                OVER series AS raw_wal_records,
        (ROW_NUMBER() OVER series > 1
         AND checkpointer_stats_reset IS DISTINCT FROM LAG(checkpointer_stats_reset) OVER series) AS ck_reset_here,
        (ROW_NUMBER() OVER series > 1
         AND wal_stats_reset IS DISTINCT FROM LAG(wal_stats_reset) OVER series) AS wal_reset_here,
        (wal_bytes IS NOT NULL) AS wal_tracked,
        {PostmasterRestart.SpansSql} AS restart_here
    FROM pg_write_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (ORDER BY collection_time)
)
SELECT
    CAST(coalesce(SUM(GREATEST(raw_requested, 0)) FILTER (WHERE NOT restart_here), 0) AS bigint) AS checkpoints_requested,
    CAST(coalesce(SUM(GREATEST(raw_timed, 0)), 0) AS bigint)        AS checkpoints_timed,
    coalesce(SUM(GREATEST(raw_write_ms, 0)), 0)                     AS checkpoint_write_ms,
    coalesce(SUM(GREATEST(raw_sync_ms, 0)), 0)                      AS checkpoint_sync_ms,
    CAST(coalesce(SUM(GREATEST(raw_ckpt_buffers, 0)), 0) AS bigint) AS checkpoint_buffers_written,
    coalesce(SUM(GREATEST(raw_wal_bytes, 0)), 0)                    AS wal_bytes,
    CAST(coalesce(SUM(GREATEST(raw_wal_fpi, 0)), 0) AS bigint)      AS wal_fpi,
    CAST(count(*) FILTER (WHERE ck_reset_here) AS integer)          AS checkpointer_reset_count,
    CAST(count(*) FILTER (WHERE wal_reset_here) AS integer)         AS wal_reset_count,
    coalesce(bool_or(wal_tracked), false)                           AS wal_tracked,
    CAST(count(*) AS integer)                                       AS sample_count,
    CAST(coalesce(SUM(GREATEST(raw_wal_records, 0)), 0) AS bigint)  AS wal_records,
    CAST(count(*) FILTER (WHERE restart_here) AS integer)           AS postmaster_restart_count
FROM sampled";

    /// <summary>
    /// The WAL volume of <c>pg_write_stats</c> rated PER COLLECTION (lane 15 — #3691 step 15, design §3.11):
    /// Δ<c>wal_bytes</c> over each collection's own gap, so the window's peak and mean are in the unit the
    /// <c>pg_wal_bytes_per_sec</c> baseline arm buckets (<c>PgTargetBaselineProvider.Wal.cs</c>) and the anomaly
    /// detector's window read is THIS text by alias (<c>PgTargetAnomalyDetector.WalVolumeWindowSql</c>) — one
    /// differencing, three readers, none re-derived. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window.
    ///
    /// <para><b>The differencing is <see cref="PgTargetCheckpointSql"/>'s WAL family</b> — per-sample <c>LAG</c>,
    /// <c>GREATEST(raw, 0)</c>, the reset as <c>ROW_NUMBER() OVER series &gt; 1 AND wal_stats_reset IS DISTINCT
    /// FROM LAG(wal_stats_reset)</c> — with the gap taken from the series itself (<c>LAG(collection_time)</c>,
    /// second-truncated, the <c>pg_tps</c> arm's rule) so the first in-window row, having no predecessor, rates
    /// nothing rather than a rate over an unknown interval. <c>rated</c> keeps only rows with a non-NULL
    /// difference and a positive gap, exactly as the baseline's <c>clean</c> does.</para>
    ///
    /// <para><b>Trackedness is read three ways, because the store has three ways of saying "not reported".</b>
    /// <c>wal_tracked</c> is <c>bool_or(wal_bytes IS NOT NULL)</c>: the collector types the WAL columns NULL on
    /// Aurora (<c>pg_stat_wal</c> raises <c>0A000</c> there) and below PG 14 (no view), so an all-NULL window is
    /// the flavour or major saying so. <c>wal_records</c> rides beside <c>wal_bytes</c> so a window in which BOTH
    /// summed to zero across every sample can be told from a merely idle server: a live <c>pg_stat_wal</c> writes
    /// a record for every commit, and a PostgreSQL that ran checkpoints (the sibling read proves it did) while
    /// writing zero WAL records is a counter that is not being populated, not a quiet one. The fact says which.
    /// The decision over those columns is <see cref="WalIsTracked"/>, the one predicate both reads apply.</para>
    /// </summary>
    public const string PgTargetWalVolumeSql = @"
WITH sampled AS (
    SELECT
        collection_time,
        wal_bytes   - LAG(wal_bytes)   OVER series AS raw_wal_bytes,
        wal_records - LAG(wal_records) OVER series AS raw_wal_records,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec,
        (ROW_NUMBER() OVER series > 1
         AND wal_stats_reset IS DISTINCT FROM LAG(wal_stats_reset) OVER series) AS wal_reset_here,
        (wal_bytes IS NOT NULL) AS wal_tracked
    FROM pg_write_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (ORDER BY collection_time)
),
rated AS (
    SELECT collection_time, GREATEST(raw_wal_bytes, 0)::DOUBLE PRECISION / interval_sec AS bytes_per_sec
    FROM sampled
    WHERE raw_wal_bytes IS NOT NULL
    AND   interval_sec > 0
)
SELECT
    (SELECT MAX(bytes_per_sec) FROM rated)                                                            AS peak_wal_bytes_per_sec,
    (SELECT AVG(bytes_per_sec) FROM rated)                                                            AS avg_wal_bytes_per_sec,
    (SELECT CAST(count(*) AS integer) FROM rated)                                                     AS rated_samples,
    (SELECT coalesce(SUM(GREATEST(raw_wal_bytes, 0)), 0) FROM sampled)                                AS wal_bytes,
    (SELECT CAST(coalesce(SUM(GREATEST(raw_wal_records, 0)), 0) AS bigint) FROM sampled)              AS wal_records,
    (SELECT CAST(count(*) FILTER (WHERE wal_reset_here) AS integer) FROM sampled)                     AS wal_reset_count,
    (SELECT coalesce(bool_or(wal_tracked), false) FROM sampled)                                       AS wal_tracked,
    (SELECT CAST(count(*) AS integer) FROM sampled)                                                   AS sample_count";

    /// <summary>
    /// <c>PG_CHECKPOINT_PRESSURE</c> from <c>pg_write_stats</c> (filled by lane 2 — #3542 step 2, design §3.7):
    /// the engine's own <c>max_wal_size</c>-exhaustion report. A checkpoint is TIMED when
    /// <c>checkpoint_timeout</c> elapsed and REQUESTED when WAL reached <c>max_wal_size</c> first (or
    /// something asked for one — a base backup, a <c>CHECKPOINT</c> statement, a shutdown); a window in which
    /// requested checkpoints dominate is a window in which WAL volume, not the clock, is deciding when the
    /// server flushes every dirty buffer. The shutdown one is the kind a read can recognise, by the postmaster
    /// restart it spans, and the requested count leaves those intervals out (#3955; <c>postmaster_restart_count</c>
    /// says how many). <see cref="Fact.Value"/> is the REQUESTED SHARE, Δrequested /
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
    ///
    /// <para><b>On <c>aurora-postgres</c> the fact is emitted and marked <c>not_applicable</c> (lane 15 — #3691
    /// calibration §A4).</b> Fifty Aurora clusters over fourteen days reported sixty timed "checkpoints" an hour
    /// on every one of them — one a minute, a requested share whose p99 was 0 — because Aurora's storage layer
    /// owns checkpointing and the checkpointer counters it surfaces are synthetic: <c>max_wal_size</c> does not
    /// govern them, and nothing an operator sets will move the share. So the fact is a permanent, measured 0
    /// that means nothing, and the honest thing is to SAY so rather than grade it: the fact is still emitted
    /// (with the shape the operator can see — <c>timed_per_hour</c>, <c>requested_share</c> — so
    /// <c>get_analysis_facts</c> shows WHY) and carries <c>not_applicable = 1</c>, <c>not_applicable_on_aurora =
    /// 1</c> and the reason flag <c>reason_aurora_storage_checkpointing = 1</c>; <c>PgTargetScorer.Write.cs</c>
    /// scores it 0 off the flag, so it roots nothing, opens no edge and arms no knob. The same metadata idiom as
    /// the buffer composite's <c>hit_ratio_suppressed</c> — <see cref="Fact.Metadata"/> is numeric, so "status"
    /// and "reason" are flag KEYS rather than strings; there is no parallel status vocabulary to invent. Engine
    /// is read off the registry fact's <c>is_aurora</c> (<see cref="MonitoredEngineKind"/>, never a column's
    /// presence — #2530), the same read the buffer partial takes.</para>
    ///
    /// <para><b><c>PG_WAL_VOLUME_SHIFT</c> (lane 15, the second read).</b> A CONTEXT fact, base 0, stating the
    /// window's WAL volume in the unit the anomaly is judged in — <see cref="Fact.Value"/> the MEAN bytes per
    /// second across the window's rated collections, the PEAK beside it — so an operator reading
    /// <c>get_analysis_facts</c> sees the volume the server wrote whether or not anything fired. It is graded ONLY
    /// through <c>ANOMALY_PG_WAL_VOLUME</c> (peak against the server's own hour-of-week bucket; the detector in
    /// <c>PgTargetAnomalyDetector.Wal.cs</c>): WAL volume is a server-relative quantity — 9 MiB/s is routine on
    /// one server and a deployment gone wrong on another — so there is deliberately no absolute bar and the fact
    /// carries <c>threshold_lineage = 1</c>: not "measured", but "no bar was chosen to have a lineage". Where WAL
    /// is NOT reported the fact is still emitted, as <c>unavailable = 1</c> with the reason flag: an all-NULL or
    /// all-zero <c>wal_bytes</c> AND <c>wal_records</c> window is <c>reason_wal_stats_not_reported</c> (Aurora —
    /// §A7: fifty clusters, fourteen days, zero bytes on every one); a registry major below 14 with no rows at all
    /// is <c>reason_pg_stat_wal_absent</c> (the view does not exist there). On a stock PostgreSQL 14+ with no rows
    /// the collector has not run and nothing is emitted — the coverage witness already describes that gap.</para>
    /// </summary>
    private async partial Task CollectWriteFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        var registry = facts.Find(f => f.Key == PgTargetFactKeys.ServerMajorVersion);
        var isAurora = registry is not null && registry.Metadata.GetValueOrDefault("is_aurora") > 0;
        var major = registry is null ? 0 : (int)registry.Value;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            await ReadCheckpointPressureAsync(context, facts, connection, isAurora);
            await ReadWalVolumeAsync(context, facts, connection, major);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_write_stats arrived in V88; a pre-migration store raises 42P01 here, which the reporter
               classifies quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>The checkpoint read and the <c>PG_CHECKPOINT_PRESSURE</c> fact — the body documented on
    /// <see cref="CollectWriteFactsAsync"/>, with the Aurora <c>not_applicable</c> stamp at the end.</summary>
    private async Task ReadCheckpointPressureAsync(AnalysisContext context, List<Fact> facts, NpgsqlConnection connection, bool isAurora)
    {
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
        var walAnyNonNull = !reader.IsDBNull(9) && reader.GetBoolean(9);
        var walRecords = ToInt64(reader.GetValue(11));
        var walTracked = WalIsTracked(walAnyNonNull, walBytes, walRecords);
        /* #3955: the intervals that spanned a postmaster restart, whose requested count the sum above left out. */
        var postmasterRestarts = Convert.ToInt32(reader.GetValue(12));

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
                ["postmaster_restart_count"] = postmasterRestarts,
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

        if (isAurora)
        {
            /* #3691 §A4: Aurora storage owns checkpointing; the counters are synthetic (sixty timed an hour,
               requested share 0, on all fifty measured clusters). Stamped, not dropped — the operator sees
               the shape and the reason; the scorer reads the flag and grades nothing. */
            fact.Metadata["not_applicable"] = 1;
            fact.Metadata["not_applicable_on_aurora"] = 1;
            fact.Metadata["reason_aurora_storage_checkpointing"] = 1;
            fact.Metadata["timed_per_hour"] = timed / observedHours;
        }

        facts.Add(fact);
    }

    /// <summary>The WAL-volume read and the <c>PG_WAL_VOLUME_SHIFT</c> context fact — the body documented on
    /// <see cref="CollectWriteFactsAsync"/>: tracked, or <c>unavailable</c> with the reason flag.</summary>
    private async Task ReadWalVolumeAsync(AnalysisContext context, List<Fact> facts, NpgsqlConnection connection, int major)
    {
        using var cmd = new NpgsqlCommand(PgTargetWalVolumeSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return;

        var sampleCount = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7));

        /* No rows on a major without the view: the honest "absent", not a fabricated zero. A known major of 14+
           with no rows is a collector that has not run — the coverage witness describes that; nothing here. */
        if (sampleCount == 0)
        {
            if (major is > 0 and < 14)
                facts.Add(UnavailableWalVolume(context, "reason_pg_stat_wal_absent", sampleCount));
            return;
        }
        if (sampleCount < 2) return;

        var walAnyNonNull = !reader.IsDBNull(6) && reader.GetBoolean(6);
        var walBytes = Convert.ToDouble(reader.GetValue(3));
        var walRecords = ToInt64(reader.GetValue(4));

        /* The one trackedness decision (WalIsTracked) — the same three inputs the checkpoint read handed it a moment
           ago, over the same window, so the two facts cannot disagree about whether WAL is reported. */
        if (!WalIsTracked(walAnyNonNull, walBytes, walRecords))
        {
            facts.Add(UnavailableWalVolume(context, "reason_wal_stats_not_reported", sampleCount));
            return;
        }

        var ratedSamples = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
        if (ratedSamples == 0) return;

        var peak = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
        var mean = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
        var walResets = Convert.ToInt32(reader.GetValue(5));

        facts.Add(new Fact
        {
            Source = PgTargetSources.WriteSource,
            Key = PgTargetFactKeys.WalVolumeShift,
            Value = mean,
            ServerId = context.ServerId,
            Metadata =
            {
                ["wal_tracked"] = 1,
                ["avg_wal_bytes_per_sec"] = mean,
                ["peak_wal_bytes_per_sec"] = peak,
                ["wal_bytes"] = walBytes,
                ["wal_records"] = walRecords,
                ["wal_reset_count"] = walResets,
                ["rated_samples"] = ratedSamples,
                ["sample_count"] = sampleCount,
                ["observed_ms"] = context.ObservedDurationMs,
                /* 1, not 0: no bar was chosen for this fact (it is graded only through its anomaly, against the
                   server's own baseline), so there is no unmeasured number for the flag to disclose. */
                ["threshold_lineage"] = 1,
            },
        });
    }

    /// <summary>
    /// Whether <c>pg_write_stats</c> is REPORTING WAL over a window — the one definition of "WAL tracked", applied by
    /// <see cref="ReadCheckpointPressureAsync"/> (the <c>wal_tracked</c> flag and the per-second / per-checkpoint WAL
    /// figures on <c>PG_CHECKPOINT_PRESSURE</c>) and by <see cref="ReadWalVolumeAsync"/> (the tracked vs
    /// <c>unavailable</c> shape of <c>PG_WAL_VOLUME_SHIFT</c>) alike.
    ///
    /// <para><b>Why one predicate (#3691 exit check).</b> Lane 15 shipped two: the checkpoint read decided from NULL-ness
    /// alone (<c>bool_or(wal_bytes IS NOT NULL)</c>) while the WAL-volume read also required a non-zero byte OR record
    /// sum. On a window whose <c>wal_bytes</c> is 0-not-NULL the two disagreed on one pass over one table —
    /// <c>PG_CHECKPOINT_PRESSURE</c> said <c>wal_tracked 1, wal_bytes 0, wal_bytes_per_sec 0</c> while
    /// <c>PG_WAL_VOLUME_SHIFT</c> beside it said <c>unavailable, reason_wal_stats_not_reported</c>. That shape is a
    /// planted store's: the REAL collector types the WAL columns NULL where <c>pg_stat_wal</c> is not implemented
    /// (Aurora) or does not exist (below 14), so on the fleet it is unreachable — but a read that can contradict its
    /// sibling on any input is two definitions, and the advice reads the pressure fact's flag to decide whether to
    /// state WAL figures at all.</para>
    ///
    /// <para><b>The decision.</b> Tracked when some row in the window carried a non-NULL <c>wal_bytes</c>
    /// (<paramref name="anyNonNull"/>) AND the window's reset-clamped sums are not both zero: a live
    /// <c>pg_stat_wal</c> writes a record per commit and a checkpoint record per checkpoint, so a window in which
    /// checkpoints ran (the pressure read only reaches here when they did) while the counters summed to zero bytes
    /// AND zero records is a counter nobody is populating, not an idle server. A window with bytes but no records
    /// (an older fixture, or a collector that stored one column) is tracked — either sum moving is the counter
    /// moving. Untracked leaves the pressure fact's WAL figures ABSENT, not zero — the same posture as before.</para>
    /// </summary>
    internal static bool WalIsTracked(bool anyNonNull, double walBytes, long walRecords) =>
        anyNonNull && (walBytes > 0 || walRecords > 0);

    /// <summary>The <c>PG_WAL_VOLUME_SHIFT</c> fact in its <c>unavailable</c> shape: value 0, <c>wal_tracked = 0</c>,
    /// <c>unavailable = 1</c> and exactly one reason flag (<paramref name="reason"/>, a metadata KEY — the
    /// numeric-metadata idiom). The scorer grades nothing off it; the advice reads the reason and says which.</summary>
    private static Fact UnavailableWalVolume(AnalysisContext context, string reason, int sampleCount) =>
        new()
        {
            Source = PgTargetSources.WriteSource,
            Key = PgTargetFactKeys.WalVolumeShift,
            Value = 0,
            ServerId = context.ServerId,
            Metadata =
            {
                ["wal_tracked"] = 0,
                ["unavailable"] = 1,
                [reason] = 1,
                ["sample_count"] = sampleCount,
                ["observed_ms"] = context.ObservedDurationMs,
                ["threshold_lineage"] = 1,
            },
        };
}
