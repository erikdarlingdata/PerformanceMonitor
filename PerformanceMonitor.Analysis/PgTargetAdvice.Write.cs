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
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeWrite(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        return key switch
        {
            PgTargetFactKeys.CheckpointPressure => ComposeCheckpointPressure(factsByKey),
            PgTargetFactKeys.ConfigMaxWalSize => ComposeMaxWalSize(factsByKey),
            PgTargetFactKeys.WalVolumeShift => ComposeWalVolumeShiftPlaceholder(),
            _ => null,
        };
    }

    private static AdviceBlock ComposeCheckpointPressure(IReadOnlyDictionary<string, Fact> facts)
    {
        var pressure = KnobFact(facts, PgTargetFactKeys.CheckpointPressure);
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigMaxWalSize);

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

    /// <summary>v2 fact — no collector emits it yet; the block exists so the key never renders as bare text.</summary>
    private static AdviceBlock ComposeWalVolumeShiftPlaceholder() =>
        new(
            Headline: "WAL volume moved against its own baseline",
            Investigation:
                "WAL bytes per second departed from this server's hour-of-week baseline — the leading edge of both checkpoint " +
                "pressure and replication-slot retention. The deviation and its baseline are stated by the fact that fired.",
            Remediation:
                "Correlate with deployments and batch schedules; if checkpoint pressure co-fired, its card carries the sizing " +
                "arithmetic. Counter-objective: WAL volume is the write workload — reducing it means changing what is written, " +
                "not a setting.");
}
