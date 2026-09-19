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
/// Advice for the buffer-cache composite (lane 2): per-component evidence (hit ratio, evictions when tracked,
/// bgwriter), saying which arms were structurally absent on this flavour / major (D6).
///
/// <para><b>Two roots, one evidence paragraph.</b> <c>PG_BUFFER_CACHE_PRESSURE</c> roots the incident story
/// (the composite fired → the knob at its default is the leaf); <c>CONFIG_PG_SHARED_BUFFERS</c> roots the
/// advisory card on a quiet server. Both state the arms from the same fact: the miss share and block rate
/// (stated but NOT graded on Aurora, where a <c>blks_read</c> may be a local-tier hit), evictions as cache
/// turnover per hour (PG 16+ only; below that the sentence says the arm is absent), and the bgwriter's halt
/// share. A composite that stands on one leg says so.</para>
///
/// <para><b>The sizing rule is stated with its counter-objective and without a host figure.</b> This product
/// does not collect RAM, so "a quarter of memory" is named as the community's starting point and the number
/// is left to the operator; the cost of overshooting (double-caching against the OS page cache, longer
/// checkpoints, less room for <c>work_mem</c> × connections) is in the same sentence as the recommendation.
/// <c>effective_cache_size</c> is never used as a memory figure here (design §6 D). No DDL (D8).</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeBuffer(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        return key switch
        {
            PgTargetFactKeys.BufferCachePressure => ComposeBufferCachePressure(factsByKey),
            PgTargetFactKeys.ConfigSharedBuffers => ComposeSharedBuffers(factsByKey),
            _ => null,
        };
    }

    private static AdviceBlock ComposeBufferCachePressure(IReadOnlyDictionary<string, Fact> facts)
    {
        var pressure = KnobFact(facts, PgTargetFactKeys.BufferCachePressure);
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigSharedBuffers);

        if (pressure is null)
        {
            return new AdviceBlock(
                Headline: "Buffer cache under pressure — the working set does not fit in shared_buffers",
                Investigation:
                    "Three counters say the same thing three ways when shared_buffers is too small for the working set: block " +
                    "requests leave the cache (pg_stat_database blks_read against blks_hit), pages are evicted from " +
                    "shared_buffers to make room (pg_stat_io, PostgreSQL 16+), and the background writer halts on its per-round " +
                    "page limit with dirty buffers still ahead of the clock sweep (pg_stat_bgwriter maxwritten_clean). This " +
                    "finding is ONE fact over those components so the condition is counted once. The measured components were " +
                    "not frozen into this finding; get_pg_database_stats, get_pg_io_stats and get_pg_write_stats carry them.",
                Remediation: SharedBuffersRemediation(knob, pressure));
        }

        return new AdviceBlock(
            Headline: BufferHeadline(pressure),
            Investigation: BufferEvidence(pressure).ToString().TrimEnd(),
            Remediation: SharedBuffersRemediation(knob, pressure));
    }

    private static AdviceBlock ComposeSharedBuffers(IReadOnlyDictionary<string, Fact> facts)
    {
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigSharedBuffers);
        var pressure = KnobFact(facts, PgTargetFactKeys.BufferCachePressure);

        var stated = knob is null
            ? "at the initdb default of 128 MB"
            : KnobMb(knob.Value) + (knob.Value <= PgTargetScorer.SharedBuffersInitdbDefaultMb ? " — the initdb default" : string.Empty);

        var sb = new StringBuilder(720);
        sb.Append("`shared_buffers` is PostgreSQL's own page cache. initdb writes 128 MB into every new cluster's postgresql.conf " +
                  "regardless of the host, so 128 MB means nobody sized it — a 64 GB host caching 128 MB of its data itself and " +
                  "relying on the operating system's page cache for the rest. ");

        if (pressure is not null && pressure.BaseSeverity > 0)
        {
            sb.Append("The buffer-cache composite co-fired this window, so the shortage is measured, not presumed: ");
            sb.Append(BufferEvidence(pressure));
        }
        else if (pressure is not null)
        {
            sb.Append("The buffer-cache composite did not fire this window (");
            sb.Append(BufferEvidence(pressure));
            sb.Append("), so this is a convention finding: the default is in place, but the cache is not measurably short at this " +
                      "workload. ");
        }
        else
        {
            sb.Append("No block, eviction or bgwriter movement was measured this window, so this is a convention finding: the " +
                      "default is in place and whether it is short is unmeasured here. ");
        }

        var pendingRestart = KnobMeta(knob, "pending_restart") ?? 0;
        if (pendingRestart > 0)
            sb.Append("The snapshot reports pending_restart for this setting: a different value is in the file and takes effect " +
                      "at the next restart. ");

        return new AdviceBlock(
            Headline: $"shared_buffers is {stated}",
            Investigation: sb.ToString().TrimEnd(),
            Remediation: SharedBuffersRemediation(knob, pressure));
    }

    private static string BufferHeadline(Fact pressure)
    {
        var suppressed = (KnobMeta(pressure, "hit_ratio_suppressed") ?? 0) > 0;
        var evictionsTracked = (KnobMeta(pressure, "evictions_tracked") ?? 0) > 0;
        var turnovers = KnobMeta(pressure, "cache_turnovers_per_hour");

        if (!suppressed)
            return $"Buffer cache under pressure — {KnobPct(pressure.Value)} of block requests left shared_buffers";
        if (evictionsTracked && turnovers is not null)
            return $"Buffer cache under pressure — shared_buffers turned over {KnobNum(turnovers.Value)} times per hour";
        return $"Buffer cache under pressure — the background writer is halting on its page limit";
    }

    /// <summary>The per-arm evidence, each arm saying whether it was graded and why not when it was not.</summary>
    private static StringBuilder BufferEvidence(Fact pressure)
    {
        var sb = new StringBuilder(640);

        /* hit-ratio arm */
        var blocks = KnobMeta(pressure, "blocks_total") ?? 0;
        var perSec = KnobMeta(pressure, "block_requests_per_sec") ?? 0;
        var suppressed = (KnobMeta(pressure, "hit_ratio_suppressed") ?? 0) > 0;
        sb.Append(KnobNum(blocks)).Append(" block requests (").Append(KnobNum(perSec)).Append("/s), of which ")
          .Append(KnobPct(pressure.Value)).Append(" were served from outside shared_buffers");
        if (suppressed)
            sb.Append(" — stated but not graded on Aurora, where a read outside shared_buffers may be a hit in the local storage tier and the community hit-ratio arithmetic misleads");
        else if (perSec < PgTargetScorer.BufferHitArmMinimumBlocksPerSec)
            sb.Append(" — below the block-rate floor, so the hit-ratio arm is not graded");
        sb.Append(". ");

        /* eviction arm */
        var evictionsTracked = (KnobMeta(pressure, "evictions_tracked") ?? 0) > 0;
        if (!evictionsTracked)
        {
            sb.Append("Evictions are not tracked on this major (pg_stat_io arrived in PostgreSQL 16), so this finding rests on the hit ratio and the bgwriter only. ");
        }
        else
        {
            var evictions = KnobMeta(pressure, "evictions") ?? 0;
            var evictionsPerSec = KnobMeta(pressure, "evictions_per_sec") ?? 0;
            sb.Append(KnobNum(evictions)).Append(" pages were evicted from shared_buffers (").Append(KnobNum(evictionsPerSec)).Append("/s");
            var turnovers = KnobMeta(pressure, "cache_turnovers_per_hour");
            var sharedBytes = KnobMeta(pressure, "shared_buffers_bytes");
            if (turnovers is not null && sharedBytes is not null)
                sb.Append(" — the whole ").Append(KnobBytes(sharedBytes.Value)).Append(" cache replaced about ").Append(KnobNum(turnovers.Value)).Append(" times per hour");
            else
                sb.Append(" — shared_buffers was not in the config snapshot, so turnover could not be computed and this arm is not graded");
            sb.Append("). ");
        }

        /* bgwriter arm */
        var bgwriterTracked = (KnobMeta(pressure, "bgwriter_tracked") ?? 0) > 0;
        if (!bgwriterTracked)
        {
            sb.Append("Background-writer counters are not collected on this major (pg_write_stats needs PostgreSQL 14+). ");
        }
        else
        {
            var halts = KnobMeta(pressure, "maxwritten_clean") ?? 0;
            var clean = KnobMeta(pressure, "buffers_clean") ?? 0;
            var alloc = KnobMeta(pressure, "buffers_alloc") ?? 0;
            var haltShare = KnobMeta(pressure, "bgwriter_halt_share");
            sb.Append("The background writer cleaned ").Append(KnobNum(clean)).Append(" buffers against ").Append(KnobNum(alloc))
              .Append(" allocated and halted on bgwriter_lru_maxpages ").Append(KnobNum(halts)).Append(" times");
            if (haltShare is not null)
                sb.Append(" (").Append(KnobPct(haltShare.Value)).Append(" of its rounds)");
            else
                sb.Append(" (bgwriter_delay was not in the config snapshot, so the halt share could not be computed and this arm is not graded)");
            sb.Append(". ");
        }

        var arms = KnobMeta(pressure, "arms_graded");
        if (arms is not null)
            sb.Append("Graded on ").Append(KnobNum(arms.Value)).Append(arms.Value == 1 ? " arm. " : " arms. ");

        var resets = (KnobMeta(pressure, "db_reset_count") ?? 0) + (KnobMeta(pressure, "io_reset_count") ?? 0) + (KnobMeta(pressure, "bg_reset_count") ?? 0);
        if (resets > 0)
            sb.Append("Statistics were reset ").Append(KnobNum(resets)).Append(" time(s) inside the window, so these totals are a floor. ");

        return sb;
    }

    private static string SharedBuffersRemediation(Fact? knob, Fact? pressure)
    {
        var sb = new StringBuilder(720);
        var current = knob is null ? "128 MB (assumed — no config snapshot)" : KnobMb(knob.Value);
        var atDefault = knob is null || knob.Value <= PgTargetScorer.SharedBuffersInitdbDefaultMb;
        var fired = pressure is not null && pressure.BaseSeverity > 0;

        if (atDefault)
        {
            sb.Append("shared_buffers is ").Append(current)
              .Append(". Raise it toward a quarter of the host's RAM — the community starting point, and a judgment: this product " +
                      "does not collect host memory, so the number is yours — then re-check the composite on the next pass. " +
                      "It is a postmaster setting: the change takes a restart, and on Linux a large value may need huge_pages " +
                      "and kernel shared-memory limits set with it; on a managed service it is a parameter-group change. ");
        }
        else
        {
            sb.Append("shared_buffers is ").Append(current).Append(", already sized above the default");
            sb.Append(fired
                ? ", and the cache is still measurably short. Before raising it further, look at WHAT is in it (get_pg_buffer_usage): one large sequential scanner or a bloated table can churn a cache no size would satisfy, and fixing the statement or the bloat is the cheaper move. "
                : ". ");
        }

        sb.Append("Counter-objectives: every byte given to shared_buffers is taken from the operating system's page cache " +
                  "(past roughly 40 % of RAM the two cache the same pages twice) and from the memory available to work_mem × " +
                  "connections; a larger cache also holds more dirty pages, so checkpoints write more and take longer — " +
                  "raise checkpoint_completion_target's headroom and watch the checkpoint-pressure card with it.");

        return sb.ToString();
    }
}
