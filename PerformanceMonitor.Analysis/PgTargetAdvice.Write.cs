/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for checkpoint / WAL pressure (lane 2): the requested-vs-timed ratio and <c>wal_bytes</c> per
/// interval the engine measured, and the <c>max_wal_size</c> the ratio argues for.
///
/// <para><b>Two roots, one evidence sentence.</b> <c>PG_CHECKPOINT_PRESSURE</c> roots the incident story
/// (requested checkpoints dominate → the knob at its default is the leaf); <c>CONFIG_PG_MAX_WAL_SIZE</c> roots
/// the advisory card on a quiet server (the knob at its default, no pressure measured). Both blocks state the
/// same numbers from the same facts — the measured share, the WAL bytes per second and per checkpoint, the
/// count the timer alone would have produced — so that whichever card the reader opens, the number in it is
/// the server's and not folklore's. Where WAL bytes are not reported (Aurora's <c>pg_stat_wal</c> gap) the
/// block says so instead of printing 0.</para>
///
/// <para><b>Sizing is stated as arithmetic the reader can check, not as a recommendation to copy.</b> WAL per
/// <c>checkpoint_timeout</c> = measured bytes/s × the timeout; the block names that figure and says
/// <c>max_wal_size</c> must hold it with headroom for the checkpoint to spread (<c>checkpoint_completion_target</c>)
/// — a judgment, labelled as one. Counter-objectives named: disk footprint under <c>pg_wal</c>, crash-recovery
/// replay length, and (for <c>wal_compression</c>) CPU.</para>
///
/// <para><b>Aurora says what Aurora does instead (lane 15 — #3691 §A4).</b> A pressure or knob fact the collector
/// stamped <c>not_applicable</c> composes ONE sentence: Aurora's storage layer owns checkpointing, the
/// checkpointer counters are synthetic (sixty timed an hour, requested share 0 — the shape the fact carries),
/// and where to look instead. No sizing arithmetic, no remediation: there is no knob to turn. These facts are
/// base 0 and never root a card, so the block is reached through <c>get_analysis_facts</c>' key lookup and
/// through <see cref="Static"/>; it exists so that the honest answer is on the key, not only in a comment.</para>
///
/// <para><b>WAL volume (lane 15, design §3.11): the context fact states, the anomaly grades.</b>
/// <c>PG_WAL_VOLUME_SHIFT</c>'s block states the window's mean and peak bytes per second, or — where WAL is not
/// reported — which reason (<c>pg_stat_wal</c> not implemented on Aurora; absent below PG 14) and where the
/// per-statement WAL figures still live. <c>ANOMALY_PG_WAL_VOLUME</c>'s block is the deviation family's shape
/// (<c>ComposeDeviation</c>: peak, sigmas, the hour-of-week centre, sample count; first-occurrence wording on a
/// thin bucket) plus the ratio to the routine rate and the deployment-correlation framing: WAL volume is the
/// leading edge of checkpoint pressure and slot retention, so the first question is what changed. Levers with
/// their counter-objectives: <c>wal_compression</c> trades CPU for bytes; <c>max_wal_size</c> absorbs volume at
/// the cost of <c>pg_wal</c> footprint and recovery time; batch sizing is the application's. <c>full_page_writes</c>
/// is POSTURE and is never advised off here or anywhere (D6) — it is not a lever on WAL volume, it is
/// crash-safety.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeWrite(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        return key switch
        {
            PgTargetFactKeys.CheckpointPressure => ComposeCheckpointPressure(factsByKey),
            PgTargetFactKeys.ConfigMaxWalSize => ComposeMaxWalSize(factsByKey),
            PgTargetFactKeys.WalVolumeShift => ComposeWalVolumeShift(factsByKey),
            _ => null,
        };
    }

    /// <summary>The one Aurora sentence, shared by both <c>not_applicable</c> facts; the pressure fact's shape
    /// (<c>timed_per_hour</c>, <c>requested_share</c>) is stated when it is in hand.</summary>
    private static AdviceBlock ComposeAuroraNotApplicable(string headline, Fact? pressure)
    {
        var shape = pressure is not null && KnobMeta(pressure, "timed_per_hour") is { } timedPerHour
            ? $" This window the counters reported {KnobNum(timedPerHour)} timed checkpoints an hour at a requested share of {KnobPct(pressure.Value)} — the synthetic one-a-minute rhythm, not a checkpoint schedule anyone set."
            : string.Empty;

        return new AdviceBlock(
            Headline: headline,
            Investigation:
                "On Aurora PostgreSQL the storage layer owns checkpointing: the checkpointer counters the engine surfaces are " +
                "synthetic (a timed checkpoint every minute, a requested share of zero, on every measured cluster), max_wal_size " +
                "does not govern when dirty pages reach storage, and nothing in the parameter group moves this ratio. The " +
                "finding is therefore not applicable here and is not graded." + shape,
            Remediation:
                "Read write pressure where Aurora reports it — the cluster's storage and I/O metrics (get_pg_io_stats, and " +
                "the instance CPU and wait findings) — rather than these counters; get_pg_write_stats shows the synthetic " +
                "checkpointer series beside the background-writer figures Aurora does supply.");
    }

    private static AdviceBlock ComposeCheckpointPressure(IReadOnlyDictionary<string, Fact> facts)
    {
        var pressure = KnobFact(facts, PgTargetFactKeys.CheckpointPressure);
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigMaxWalSize);

        if (pressure is not null && (KnobMeta(pressure, "not_applicable") ?? 0) > 0)
            return ComposeAuroraNotApplicable("Checkpoint pressure is not applicable on Aurora — the storage layer owns checkpointing", pressure);

        if (pressure is null)
        {
            return new AdviceBlock(
                Headline: "Checkpoints are being forced by WAL volume — requested checkpoints outnumber timed ones",
                Investigation:
                    "A checkpoint is TIMED when checkpoint_timeout elapses and REQUESTED when WAL reaches max_wal_size first. " +
                    "When requested checkpoints dominate a window, WAL volume rather than the clock is deciding when every " +
                    "dirty buffer is flushed — PostgreSQL's own report that max_wal_size is too small for this write rate. " +
                    "The measured share, WAL rate and checkpoint timing were not frozen into this finding; " +
                    "get_pg_write_stats over the same window carries them.",
                Remediation: MaxWalSizeRemediation(knob, pressure));
        }

        var requested = KnobMeta(pressure, "checkpoints_requested") ?? 0;
        var timed = KnobMeta(pressure, "checkpoints_timed") ?? 0;
        var total = KnobMeta(pressure, "checkpoints_total") ?? (requested + timed);
        var share = pressure.Value;

        var sb = new StringBuilder(640);
        sb.Append("Over the observed window the engine ran ").Append(KnobNum(total)).Append(" checkpoints: ")
          .Append(KnobNum(requested)).Append(" requested (WAL reached max_wal_size) and ").Append(KnobNum(timed))
          .Append(" timed (checkpoint_timeout elapsed) — a requested share of ").Append(KnobPct(share)).Append(". ");

        var expectedTimed = KnobMeta(pressure, "expected_timed_checkpoints");
        var timeoutSeconds = KnobMeta(pressure, "checkpoint_timeout_s");
        if (expectedTimed is not null && timeoutSeconds is not null)
        {
            sb.Append("checkpoint_timeout is ").Append(KnobSeconds(timeoutSeconds.Value))
              .Append(", so the timer alone would have produced about ").Append(KnobNum(expectedTimed.Value))
              .Append(" over that time; every checkpoint beyond it was WAL-driven. ");
        }

        AppendWalSentence(sb, pressure);

        var writeMs = KnobMeta(pressure, "checkpoint_write_ms") ?? 0;
        var syncMs = KnobMeta(pressure, "checkpoint_sync_ms") ?? 0;
        if (writeMs > 0 || syncMs > 0)
        {
            sb.Append("The checkpointer spent ").Append(KnobNum(writeMs / 1000.0)).Append(" s writing and ")
              .Append(KnobNum(syncMs / 1000.0)).Append(" s in fsync across them. ");
        }

        var resets = (KnobMeta(pressure, "checkpointer_reset_count") ?? 0) + (KnobMeta(pressure, "wal_reset_count") ?? 0);
        if (resets > 0)
            sb.Append("Statistics were reset ").Append(KnobNum(resets)).Append(" time(s) inside the window, so these totals are a floor. ");

        /* #3955: PostgreSQL counts a shutdown checkpoint as requested and keeps the count across the restart, and its own
           write and sync phases land in the same counters, so the collector left the restart-spanning intervals out of
           the checkpoint figures rather than read them as the workload's. */
        var restarts = KnobMeta(pressure, "postmaster_restart_count") ?? 0;
        if (restarts > 0)
            sb.Append("PostgreSQL restarted ").Append(KnobNum(restarts)).Append(" time(s) inside the window; a shutdown checkpoint " +
                      "counts as requested and its own write and sync phases land in the same counters, so the collection " +
                      "interval(s) spanning a restart are left out of the requested count and the checkpoint write and sync " +
                      "time rather than read as WAL pressure. ");

        sb.Append("Each requested checkpoint flushes every dirty buffer early and re-arms full-page images for the pages " +
                  "touched next, so a server in this state writes more WAL per transaction than the same server checkpointing " +
                  "on the clock.");

        return new AdviceBlock(
            Headline: $"Checkpoints are being forced by WAL volume — {KnobNum(requested)} of {KnobNum(total)} checkpoints ({KnobPct(share)}) were requested, not timed",
            Investigation: sb.ToString(),
            Remediation: MaxWalSizeRemediation(knob, pressure));
    }

    private static AdviceBlock ComposeMaxWalSize(IReadOnlyDictionary<string, Fact> facts)
    {
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigMaxWalSize);
        var pressure = KnobFact(facts, PgTargetFactKeys.CheckpointPressure);

        if (knob is not null && (KnobMeta(knob, "not_applicable") ?? 0) > 0)
            return ComposeAuroraNotApplicable($"max_wal_size is {KnobMb(knob.Value)} — not a finding on Aurora, where the engine does not consult it", pressure);

        var stated = knob is null
            ? "at the shipped default of 1 GB"
            : KnobMb(knob.Value) + (knob.Value <= PgTargetScorer.MaxWalSizeDefaultMb ? " — the shipped default" : string.Empty);

        var sb = new StringBuilder(640);
        sb.Append("`max_wal_size` is the soft ceiling on WAL between checkpoints: when the WAL written since the last checkpoint " +
                  "reaches it, a checkpoint is REQUESTED regardless of checkpoint_timeout. 1 GB is what every install ships with, " +
                  "sized for no workload in particular. ");

        if (pressure is not null && pressure.BaseSeverity > 0)
        {
            sb.Append("The engine's own exhaustion signal co-fired this window: ")
              .Append(KnobNum(KnobMeta(pressure, "checkpoints_requested") ?? 0)).Append(" of ")
              .Append(KnobNum(KnobMeta(pressure, "checkpoints_total") ?? 0)).Append(" checkpoints (")
              .Append(KnobPct(pressure.Value)).Append(") were requested. ");
            AppendWalSentence(sb, pressure);
        }
        else if (pressure is not null)
        {
            sb.Append("Checkpoints ran on the clock this window (requested share ").Append(KnobPct(pressure.Value))
              .Append("), so this is a convention finding — the default is in place, but the workload has not yet hit it. ");
            AppendWalSentence(sb, pressure);
        }
        else
        {
            sb.Append("No checkpoint counters were collected for this window (pg_write_stats needs PostgreSQL 14+), so this is a " +
                      "convention finding: the default is in place and whether the workload hits it is unmeasured here. ");
        }

        var timeout = KnobMeta(knob, "checkpoint_timeout_s");
        if (timeout is not null)
            sb.Append("checkpoint_timeout is ").Append(KnobSeconds(timeout.Value)).Append(". ");

        var walCompression = KnobFact(facts, PgTargetFactKeys.ConfigWalCompression);
        if (walCompression is not null && walCompression.Value == 0)
            sb.Append("wal_compression is off, so every post-checkpoint full-page image is written uncompressed.");

        return new AdviceBlock(
            Headline: $"max_wal_size is {stated}",
            Investigation: sb.ToString().TrimEnd(),
            Remediation: MaxWalSizeRemediation(knob, pressure));
    }

    /// <summary>The WAL-volume sentence, or the honest gap: Aurora does not implement <c>pg_stat_wal</c>.</summary>
    private static void AppendWalSentence(StringBuilder sb, Fact pressure)
    {
        if ((KnobMeta(pressure, "wal_tracked") ?? 0) <= 0)
        {
            sb.Append("WAL volume is not reported on this flavour (pg_stat_wal is not implemented on Aurora), so the bytes " +
                      "behind each requested checkpoint cannot be stated here. ");
            return;
        }

        var walBytes = KnobMeta(pressure, "wal_bytes") ?? 0;
        var perSec = KnobMeta(pressure, "wal_bytes_per_sec") ?? 0;
        var perCheckpoint = KnobMeta(pressure, "wal_bytes_per_checkpoint");
        sb.Append("It wrote ").Append(KnobBytes(walBytes)).Append(" of WAL over the window — ")
          .Append(KnobBytes(perSec)).Append("/s");
        if (perCheckpoint is not null)
            sb.Append(", about ").Append(KnobBytes(perCheckpoint.Value)).Append(" per checkpoint interval");
        sb.Append(". ");

        var fpi = KnobMeta(pressure, "wal_fpi");
        if (fpi is not null && fpi.Value > 0)
            sb.Append(KnobNum(fpi.Value)).Append(" of those records were full-page images. ");
    }

    /// <summary>
    /// One remediation for both roots, computed from the same facts: the WAL one <c>checkpoint_timeout</c>
    /// produces at the measured rate is the figure <c>max_wal_size</c> has to hold, with headroom; without a
    /// measured rate the block says how to get one instead of inventing a size.
    /// </summary>
    private static string MaxWalSizeRemediation(Fact? knob, Fact? pressure)
    {
        var sb = new StringBuilder(640);
        var current = knob is null ? "1 GB (assumed — no config snapshot)" : KnobMb(knob.Value);

        var perSec = KnobMeta(pressure, "wal_bytes_per_sec");
        var timeout = KnobMeta(pressure, "checkpoint_timeout_s") ?? KnobMeta(knob, "checkpoint_timeout_s");
        var tracked = (KnobMeta(pressure, "wal_tracked") ?? 0) > 0;

        if (pressure is not null && tracked && perSec is not null && timeout is not null && perSec.Value > 0)
        {
            var perTimeout = perSec.Value * timeout.Value;
            sb.Append("At the measured ").Append(KnobBytes(perSec.Value)).Append("/s, one checkpoint_timeout (")
              .Append(KnobSeconds(timeout.Value)).Append(") produces about ").Append(KnobBytes(perTimeout))
              .Append(" of WAL; max_wal_size is ").Append(current)
              .Append(". Raise it so that figure fits with headroom for the checkpoint to spread over checkpoint_completion_target " +
                      "— two to three times the per-timeout volume is a common judgment, stated as one, not a rule — then confirm " +
                      "the requested share falls on the next pass. ");
        }
        else if (pressure is not null && !tracked)
        {
            sb.Append("max_wal_size is ").Append(current)
              .Append(". WAL volume is not reported on this flavour, so size from the requested share: raise max_wal_size and " +
                      "confirm the share falls on the next pass rather than computing a target from bytes this product cannot see. ");
        }
        else
        {
            sb.Append("max_wal_size is ").Append(current)
              .Append(". No measured WAL rate accompanies this card, so there is no number to size against yet: read " +
                      "get_pg_write_stats over a busy window for wal_bytes and the requested-vs-timed split, then raise " +
                      "max_wal_size so one checkpoint_timeout of WAL fits inside it with headroom. ");
        }

        sb.Append("It is reloadable (no restart; a parameter-group change on a managed service). Counter-objectives: pg_wal " +
                  "grows to roughly the new size on disk, and crash recovery replays up to that much WAL — size the volume and " +
                  "the recovery-time expectation with it. wal_compression trades CPU for fewer full-page-image bytes and is the " +
                  "other lever on the same ratio.");

        return sb.ToString();
    }

    /// <summary>The levers and their counter-objectives, stated once for the context fact and the anomaly alike.
    /// <c>full_page_writes</c> is deliberately absent: posture, never a performance lever (D6).</summary>
    private const string WalVolumeLevers =
        "Correlate first: WAL volume is the write workload, so a shift is a deployment, a batch schedule, a bulk load or a " +
        "reindex before it is a setting — find what changed. Then the levers, each with its cost: wal_compression trades CPU " +
        "for fewer full-page-image bytes; a larger max_wal_size absorbs the volume between checkpoints at the cost of pg_wal " +
        "footprint and crash-recovery replay time (the checkpoint-pressure card carries the arithmetic when it co-fired); and " +
        "batch sizing — fewer, larger transactions write fewer full-page images than many small ones touching the same pages " +
        "across checkpoints — is the application's to change. Downstream, a replication slot retains every byte of this " +
        "until its consumer reads it.";

    /// <summary>
    /// <c>PG_WAL_VOLUME_SHIFT</c> (lane 15): the context fact's own block — the mean and peak WAL bytes per second
    /// the window measured, read off the fact and never assumed; or, in the <c>unavailable</c> shape, which reason
    /// and where per-statement WAL still is. Base 0, so this roots no card; reached by key through
    /// <c>get_analysis_facts</c> and <see cref="Static"/>.
    /// </summary>
    private static AdviceBlock ComposeWalVolumeShift(IReadOnlyDictionary<string, Fact> facts)
    {
        var shift = KnobFact(facts, PgTargetFactKeys.WalVolumeShift);

        if (shift is not null && (KnobMeta(shift, "unavailable") ?? 0) > 0)
        {
            var reason = (KnobMeta(shift, "reason_pg_stat_wal_absent") ?? 0) > 0
                ? "this server's registry major is below 14, and pg_stat_wal does not exist there"
                : "pg_stat_wal is not implemented on Aurora — pg_stat_get_wal() raises 0A000 — so the collector types the WAL columns NULL";
            return new AdviceBlock(
                Headline: "WAL volume is not reported by this engine",
                Investigation:
                    $"Cluster-wide WAL volume (wal_bytes / wal_records in pg_write_stats) is not available on this server: {reason}. " +
                    "No rate is stated and nothing is graded — an absent counter is not a zero. Per-statement WAL volume is still " +
                    "collected from pg_stat_statements (wal_bytes per statement) and read by get_pg_top_queries.",
                Remediation:
                    "Read write volume where this engine reports it: get_pg_top_queries for the statements generating WAL, and on " +
                    "Aurora the cluster's storage and I/O metrics for the volume the storage layer absorbed.");
        }

        if (shift is null || KnobMeta(shift, "avg_wal_bytes_per_sec") is not { } mean)
        {
            return new AdviceBlock(
                Headline: "WAL volume this window, in the unit its anomaly is judged in",
                Investigation:
                    "The mean and peak WAL bytes per second across the window's collections (wal_bytes from pg_write_stats, " +
                    "differenced per collection with wal_stats_reset honoured). Context, not a finding: WAL volume is a " +
                    "server-relative quantity with no absolute bar — it is graded only by ANOMALY_PG_WAL_VOLUME against this " +
                    "server's own hour-of-week baseline, and that anomaly folds onto the checkpoint-pressure incident it leads.",
                Remediation: WalVolumeLevers);
        }

        var peak = KnobMeta(shift, "peak_wal_bytes_per_sec") ?? mean;
        var total = KnobMeta(shift, "wal_bytes") ?? 0;
        var resets = KnobMeta(shift, "wal_reset_count") ?? 0;
        var sb = new StringBuilder(480);
        sb.Append("Over the observed window this server wrote ").Append(KnobBytes(total)).Append(" of WAL — a mean of ")
          .Append(KnobBytes(mean)).Append("/s across ").Append(KnobNum(KnobMeta(shift, "rated_samples") ?? 0))
          .Append(" rated collections, peaking at ").Append(KnobBytes(peak)).Append("/s in one collection interval. ");
        if (resets > 0)
            sb.Append("WAL statistics were reset ").Append(KnobNum(resets)).Append(" time(s) inside the window, so the total is a floor. ");
        sb.Append("Context, not a finding: WAL volume has no absolute bar — the same rate is routine on one server and a " +
                  "regression on another — so it is graded only by ANOMALY_PG_WAL_VOLUME against this server's own hour-of-week " +
                  "baseline, and that anomaly folds onto the checkpoint-pressure incident it leads.");

        return new AdviceBlock(
            Headline: $"WAL volume: {KnobBytes(mean)}/s mean, {KnobBytes(peak)}/s peak this window",
            Investigation: sb.ToString(),
            Remediation: WalVolumeLevers);
    }

    /// <summary>The static block for <c>ANOMALY_PG_WAL_VOLUME</c> — the source sentence the composed block keeps.
    /// Built on first use, NOT as a <c>static readonly</c> initializer: it concatenates <c>s_anomalyHedge</c>, which
    /// is declared in another partial file, and C# leaves the initialization order of static fields across
    /// partial files unspecified — an initializer here could read the hedge as null and silently drop it (the
    /// aliasing trap the wave-2 brief records). One instance, so <c>ReferenceEquals</c> still tells "the composer
    /// fell back" from "the composer composed".</summary>
    private static AdviceBlock WalVolumeStatic => s_walVolumeStatic ??= new(
        Headline: "WAL volume ran well above this server's normal for this time of week",
        Investigation:
            "The window's peak WAL bytes per second (wal_bytes from pg_write_stats, differenced per collection over its own " +
            "gap with wal_stats_reset honoured) was judged against this server's hour-of-week baseline of the same rate over " +
            "the last 30 days. WAL volume is the leading edge of checkpoint pressure and of replication-slot retention: when " +
            "it moves, the checkpointer and every slot consumer feel it next." + s_anomalyHedge,
        Remediation: WalVolumeLevers);

    private static AdviceBlock? s_walVolumeStatic;

    /// <summary>
    /// <c>ANOMALY_PG_WAL_VOLUME</c> (lane 15): the deviation family's composed block (peak, sigmas, centre, sample
    /// count — or first-occurrence wording on a thin bucket), plus the ratio to the routine rate when the bucket had
    /// a mean to divide by ("9.2 MB/s against a 1.1 MB/s routine for this hour"), plus the deployment-correlation
    /// framing and the levers with their costs. Delegated to from <c>ComposeAnomaly</c>'s switch.
    /// </summary>
    private static AdviceBlock ComposeWalVolumeAnomaly(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = WalVolumeStatic;
        if (!facts.TryGetValue(PgTargetFactKeys.AnomalyWalVolume, out var fact))
            return fallback;

        var block = ComposeDeviation(fact, fallback, "WAL volume", "peak_wal_bytes_per_sec", v => KnobBytes(v) + "/s");
        if (ReferenceEquals(block, fallback)) return block;

        /* The centre the detector divided by: the robust median when the bucket had one, else the mean — the same
           choice ComposeDeviation's prose makes, so the two numbers in one paragraph are one number. */
        var ratio = KnobMeta(fact, "baseline_ratio");
        var median = KnobMeta(fact, "baseline_median");
        var centre = median is > 0 ? median : KnobMeta(fact, "baseline_mean");
        var against = ratio is > 0 && centre is > 0
            ? $" That is {KnobNum(ratio.Value)}× the {KnobBytes(centre.Value)}/s this server routinely writes at this hour of the week."
            : string.Empty;
        var checkpoint = KnobFact(facts, PgTargetFactKeys.CheckpointPressure);
        var pressure = checkpoint is not null && checkpoint.BaseSeverity > 0
            ? " Checkpoint pressure co-fired this window — the volume has already reached max_wal_size ahead of the timer; that card carries the sizing arithmetic."
            : " Checkpoints were not yet forced this window: this is the leading edge, before the checkpointer or a slot consumer shows it.";

        return block with { Investigation = block.Investigation + against + pressure };
    }
}
