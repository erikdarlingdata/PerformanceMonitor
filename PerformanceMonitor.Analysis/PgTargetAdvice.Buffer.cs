/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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
///
/// <para><b>The composition sentence is a second composer (lane 29 of #3691).</b> <see cref="FactAdvice.PopulateStoryText"/>
/// freezes the card from facts alone before any drill-down runs, so what the buffer cache HOLDS — read from the
/// latest <c>pg_buffer_usage</c> capture by the Darling drill-down — is folded in afterwards through
/// <see cref="WithBufferComposition"/>, lane 16's shape: the words stay here beside the rest of the buffer prose,
/// the read stays with the other drill-downs, and the sentence in the payload's <c>note</c> and in the re-frozen
/// card is one string. Value-stated from the capture (pool fill, the largest resident relation's share, the cold
/// share), never presumed; the sizing sentence above already names <c>shared_buffers</c>' counter-objective, so
/// the composition EXTENDS it (what a larger cache would hold) rather than repeating it.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /// <summary>The phrase every composition sentence starts with; <see cref="WithBufferComposition"/> keys its idempotence on it.</summary>
    internal const string CompositionSentenceMarker = "Composition: ";

    /// <summary>
    /// The buffer-pressure card with the composition drill-down folded into its prose: the investigation gains
    /// <see cref="BufferCompositionSentence"/>; the remediation, when a capture was read, gains what the
    /// composition says about SIZING — a cold majority means a larger cache would hold pages nobody re-reads, a
    /// single relation holding a large share names the statement or the bloat to look at before the knob, and on
    /// Aurora the composition is the honest read where the hit ratio is not. A block already carrying the
    /// sentence is returned unchanged, so a re-run over the same finding cannot stack it.
    /// </summary>
    public static AdviceBlock WithBufferComposition(AdviceBlock advice, PgTargetBufferCompositionSummary summary)
    {
        ArgumentNullException.ThrowIfNull(advice);
        ArgumentNullException.ThrowIfNull(summary);

        if (advice.Investigation.Contains(CompositionSentenceMarker, StringComparison.Ordinal))
            return advice;

        var investigation = string.Concat(advice.Investigation.TrimEnd(), " ", BufferCompositionSentence(summary));
        var remediation = advice.Remediation;

        if (summary.Status == PgTargetBufferCompositionStatus.Captured)
        {
            var sb = new StringBuilder(advice.Remediation.Length + 600);
            sb.Append(advice.Remediation.TrimEnd()).Append(" What the capture says about sizing: ");

            var top = summary.TopRelations.Count > 0 ? summary.TopRelations[0] : null;
            if (top is not null && top.ShareOfPool >= BufferCompositionDominantRelationShare)
            {
                sb.Append(DescribeResident(top)).Append(" holds ").Append(KnobPct(top.ShareOfPool))
                  .Append(" of the pool on its own — before the knob, look at what reads it (get_pg_top_queries — the shape whose shared_blks_read dwarfs its shared_blks_hit) and at its bloat: one sequential scanner or one bloated relation can churn a cache no size would satisfy, and fixing the statement or the bloat is the cheaper move. ");
            }

            if (summary.ColdShare is { } cold)
            {
                sb.Append(cold >= BufferCompositionColdMajority
                    ? $"{KnobPct(cold)} of resident buffers sit in relations whose average usage count is at or below 1 — pages the clock sweep evicts first and nobody re-read — so a larger shared_buffers would mostly hold more of the same; raising it buys little here and still pays the counter-objective named above. "
                    : $"Only {KnobPct(cold)} of resident buffers are cold (average usage count at or below 1), so what is in the cache is being re-read and a larger shared_buffers would hold pages that earn their place. ");
            }

            if (summary.PoolFill is { } fill && fill < BufferCompositionPoolFillFloor)
                sb.Append("The pool is only ").Append(KnobPct(fill)).Append(" full: shared_buffers is not the constraint while it has room, and the pressure counters are better read as a scan pattern than a size. ");

            if (summary.HitRatioSuppressed)
                sb.Append("On Aurora the hit ratio is stated but not graded — a read outside shared_buffers may be a hit in the local storage tier — and this composition is the honest read there: what pg_buffercache says is resident is resident, whatever the community hit-ratio arithmetic says.");

            remediation = sb.ToString().TrimEnd();
        }

        return advice with { Investigation = investigation, Remediation = remediation };
    }

    /// <summary>
    /// One relation's share of the pool above which the remediation names it before the knob. Half is not a
    /// bar in the scorer's sense — nothing is graded on it — it is the point at which "the cache is one
    /// relation" is a fair sentence; below it the top-10 list in the payload speaks for itself. Chosen, not
    /// measured (no fleet capture of <c>pg_buffer_usage</c> was in the 2026-09-19 calibration).
    /// </summary>
    internal const double BufferCompositionDominantRelationShare = 0.5;

    /// <summary>The cold share at or above which the sentence says a larger cache would hold pages nobody re-reads: a majority. Chosen, not measured.</summary>
    internal const double BufferCompositionColdMajority = 0.5;

    /// <summary>Below this fill the pool has room and the size is not the constraint — 0.9, chosen, not measured; the sentence states the fill either way.</summary>
    internal const double BufferCompositionPoolFillFloor = 0.9;

    /// <summary>
    /// The value-stated composition sentence, the same words in the drill-down payload's <c>note</c> and in the
    /// re-frozen card. Four arms, one per <see cref="PgTargetBufferCompositionStatus"/>: a capture in the
    /// window (pool fill, the largest resident relation, kind shares, dirty share, cold share, the capture's
    /// stamp); no capture in the window with the extension present (the collector's hourly cadence, and how
    /// long before the window the last capture was); <c>pg_buffercache</c> available but not installed; and
    /// <c>pg_buffercache</c> absent from the server. Numbers are the summary's, never recomputed here.
    /// </summary>
    public static string BufferCompositionSentence(PgTargetBufferCompositionSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);

        switch (summary.Status)
        {
            case PgTargetBufferCompositionStatus.ExtensionAbsent:
                return CompositionSentenceMarker +
                       "what the cache holds cannot be shown — pg_buffercache is not offered by this server (pg_extension_availability reports it absent), so pg_buffer_usage has nothing to capture; " +
                       "the pressure counters above stand on their own. On a managed service it is a parameter-group or a menu question, not a CREATE EXTENSION.";
            case PgTargetBufferCompositionStatus.ExtensionAvailable:
                return CompositionSentenceMarker +
                       "what the cache holds cannot be shown yet — pg_buffercache is offered by this server but not installed (pg_extension_availability reports it available), " +
                       "one CREATE EXTENSION pg_buffercache away; the next hourly pg_buffer_usage capture after that fills this drill-down.";
            case PgTargetBufferCompositionStatus.NoCapture:
            {
                var sb = new StringBuilder(320);
                sb.Append(CompositionSentenceMarker).Append("no buffer-cache capture in the window (pg_buffer_usage collects hourly; ");
                sb.Append(summary.LastCaptureAt is null
                    ? "this server has no capture at all"
                    : summary.LastCaptureHoursBeforeWindow is { } before && before >= 0
                        ? $"the last capture was {KnobHours(before)} before the window"
                        : "the last capture is after the window");
                sb.Append(").");
                return sb.ToString();
            }
        }

        var text = new StringBuilder(720);
        text.Append(CompositionSentenceMarker);
        text.Append("the pool is ");
        text.Append(summary.PoolFill is { } poolFill ? KnobPct(poolFill) : "an unknown share");
        text.Append(" full (").Append(KnobNum(summary.PoolBuffersUsed)).Append(" of ").Append(KnobNum(summary.PoolBuffersTotal))
            .Append(" buffers, ").Append(KnobBytes(summary.PoolBytes)).Append(')');

        var largest = summary.TopRelations.Count > 0 ? summary.TopRelations[0] : null;
        if (largest is not null)
            text.Append("; ").Append(DescribeResident(largest)).Append(" alone holds ").Append(KnobPct(largest.ShareOfPool));

        if (summary.HeapShare is { } heap && summary.IndexShare is { } index && summary.ToastShare is { } toast && summary.OtherShare is { } other)
        {
            text.Append("; of the resident buffers, indexes hold ").Append(KnobPct(index))
                .Append(", heaps ").Append(KnobPct(heap))
                .Append(", TOAST ").Append(KnobPct(toast));
            if (other > 0)
                text.Append(", other databases and shared catalogs ").Append(KnobPct(other));
        }

        if (summary.DirtyShareOfPool is { } dirty)
            text.Append("; ").Append(KnobPct(dirty)).Append(" of the pool is dirty");

        if (summary.ColdShare is { } coldShare)
            text.Append("; ").Append(KnobPct(coldShare)).Append(" of resident buffers sit in relations whose average usage count is at or below 1 — the clock sweep evicts those first");

        text.Append('.');
        if (summary.CapturedAt is { } at)
            text.Append(" Captured at ").Append(at.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Append(" UTC — a level, not the window's counters; pg_buffer_usage collects hourly.");
        if (summary.RelationsInCapture > summary.TopRelations.Count)
            text.Append(" The drill-down carries the top ").Append(KnobNum(summary.TopRelations.Count)).Append(" of ").Append(KnobNum(summary.RelationsInCapture)).Append(" resident relations; get_pg_buffer_usage lists the rest.");
        return text.ToString();
    }

    /// <summary>"`orders_idx` (index, appdb)" — or, for a buffer the collector could not name, what that NULL means.</summary>
    private static string DescribeResident(PgTargetBufferResident resident)
    {
        if (string.IsNullOrEmpty(resident.RelationName))
            return string.IsNullOrEmpty(resident.DatabaseName)
                ? "a shared catalog (no relation name from here)"
                : $"a relation of database {resident.DatabaseName} (not nameable from the collector's database)";

        var sb = new StringBuilder(96);
        sb.Append('`').Append(resident.RelationName).Append(resident.RelationNameTruncated ? "…" : string.Empty).Append("` (").Append(resident.Kind);
        if (!string.IsNullOrEmpty(resident.DatabaseName))
            sb.Append(", ").Append(resident.DatabaseName);
        sb.Append(')');
        return sb.ToString();
    }

    private static string KnobHours(double hours) => hours switch
    {
        < 1 => $"{hours * 60:0} minutes",
        < 48 => $"{hours:0.#} h",
        _ => $"{hours / 24:0.#} days",
    };

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

/// <summary>
/// Which arm of the composition sentence applies (lane 29 of #3691). <see cref="Captured"/>: the window holds a
/// <c>pg_buffer_usage</c> capture and the summary's numbers are that capture's. <see cref="NoCapture"/>: none in
/// the window, and the extension is installed (or its state is not recorded) — a cadence statement, not a fact
/// about the cache. <see cref="ExtensionAvailable"/> / <see cref="ExtensionAbsent"/>: <c>pg_extension_availability</c>'s
/// latest word on <c>pg_buffercache</c>, which is why there is nothing to capture — "available" is one
/// <c>CREATE EXTENSION</c> away, "absent" is not offered by the server at all.
/// </summary>
public enum PgTargetBufferCompositionStatus
{
    Captured,
    NoCapture,
    ExtensionAvailable,
    ExtensionAbsent,
}

/// <summary>
/// What the buffer-composition drill-down found (lane 29 of #3691), handed from the Darling read to the advice
/// composer so the words and the numbers come from one object. Shares are fractions in [0, 1]:
/// <paramref name="PoolFill"/> is <c>pool_buffers_used / pool_buffers_total</c>; <paramref name="DirtyShareOfPool"/>
/// the listed dirty buffers over the WHOLE pool; the four kind shares and <paramref name="ColdShare"/> are over
/// the LISTED buffers (the collector drops relations under 8 buffers, so "listed" is not "used" and the payload
/// carries both) and sum to 1 when the capture has rows. <paramref name="ColdShare"/> counts buffers in relations
/// whose AVERAGE usage count is at or below 1 — the row is per relation, so the figure is a relation-level
/// reading of what the clock sweep would evict first, and is described that way. The pressure figures
/// (<paramref name="MissShare"/>, <paramref name="HitRatioSuppressed"/>, <paramref name="EvictionsPerSec"/>,
/// <paramref name="CacheTurnoversPerHour"/>, <paramref name="BuffersAllocPerSec"/>) are the ROOT FACT's metadata,
/// reused, null when the finding carried none. <paramref name="LastCaptureAt"/> / <paramref name="LastCaptureHoursBeforeWindow"/>
/// are for the no-capture arm: when the server was last captured, and how far before the window's start.
/// </summary>
public sealed record PgTargetBufferCompositionSummary(
    PgTargetBufferCompositionStatus Status,
    DateTime? CapturedAt,
    DateTime? LastCaptureAt,
    double? LastCaptureHoursBeforeWindow,
    long PoolBuffersTotal,
    long PoolBuffersUsed,
    long PoolBytes,
    double? PoolFill,
    long BuffersListed,
    long DirtyBuffersListed,
    double? DirtyShareOfPool,
    double? HeapShare,
    double? IndexShare,
    double? ToastShare,
    double? OtherShare,
    long ColdBuffers,
    double? ColdShare,
    int RelationsInCapture,
    IReadOnlyList<PgTargetBufferResident> TopRelations,
    double? MissShare,
    bool HitRatioSuppressed,
    double? EvictionsPerSec,
    double? CacheTurnoversPerHour,
    double? BuffersAllocPerSec);

/// <summary>
/// One resident relation in the latest capture, largest first. <paramref name="RelationName"/> is NULL for a
/// buffer belonging to another database or to a shared catalog (the collector cannot name it from its own
/// database — not a missing name), bounded in the read with <paramref name="RelationNameTruncated"/> saying
/// whether the bound cut it; <paramref name="RelationKind"/> is <c>pg_class.relkind</c> as stored and
/// <paramref name="Kind"/> the word for it (heap / index / toast / other). <paramref name="ShareOfPool"/> is
/// over the whole pool, <paramref name="DirtyShare"/> over this relation's own buffers.
/// </summary>
public sealed record PgTargetBufferResident(
    int Rank,
    string? DatabaseName,
    string? RelationName,
    bool RelationNameTruncated,
    string? RelationKind,
    string Kind,
    long Buffers,
    double ShareOfPool,
    long DirtyBuffers,
    double DirtyShare,
    double? AvgUsageCount);
