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

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the <c>CONFIG_PG_*</c> checks (lane 2; lanes 3/4/6 for the keys their families own): state the
/// current setting in its own unit, the measured evidence that fired beside it, and the value the evidence
/// argues for — the <c>ComposeConfigMaxdop</c> pattern.
///
/// <para><b>This file composes the PURE convention cards</b> — <c>effective_cache_size</c>,
/// <c>random_page_cost</c>, <c>track_io_timing</c> — and the two context keys the write chain states
/// (<c>checkpoint_timeout</c>, <c>wal_compression</c>). The two knobs compose in the family files whose
/// evidence they cite: <c>CONFIG_PG_SHARED_BUFFERS</c> in <c>PgTargetAdvice.Buffer.cs</c> beside the
/// composite, <c>CONFIG_PG_MAX_WAL_SIZE</c> in <c>PgTargetAdvice.Write.cs</c> beside the checkpoint fact. Every
/// block here names its counter-objective (the OtterTune rule: no knob is free), states the value read from
/// the fact rather than assuming the default, and contains no DDL (D8). The static shape — what the block says
/// with no fact to read — is the same composer over an empty set, so "the default" is named without a number
/// rather than with a fabricated one.</para>
///
/// <para><b>The one word this family polices.</b> <c>effective_cache_size</c> is a planner HINT, not memory;
/// the design's adversarial pass (§6 D) names treating it as an available-memory figure as the way v1 would
/// embarrass itself, so the string appears in ITS OWN card and nowhere else in this advice family — pinned by
/// <c>PgTargetKnobsTests</c>.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeConfig(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        return key switch
        {
            PgTargetFactKeys.ConfigEffectiveCacheSize => ComposeEffectiveCacheSize(factsByKey),
            PgTargetFactKeys.ConfigRandomPageCost => ComposeRandomPageCost(factsByKey),
            PgTargetFactKeys.ConfigTrackIoTiming => ComposeTrackIoTiming(factsByKey),
            PgTargetFactKeys.ConfigCheckpointTimeout => ComposeCheckpointTimeoutContext(factsByKey),
            PgTargetFactKeys.ConfigWalCompression => ComposeWalCompressionContext(factsByKey),
            _ => null,
        };
    }

    private static AdviceBlock ComposeEffectiveCacheSize(IReadOnlyDictionary<string, Fact> facts)
    {
        var fact = KnobFact(facts, PgTargetFactKeys.ConfigEffectiveCacheSize);
        var stated = fact is null ? "at the compiled default of 4 GB" : $"{KnobMb(fact.Value)}" + (fact.Value == PgTargetScorer.EffectiveCacheSizeDefaultMb ? " — the compiled default" : string.Empty);

        return new AdviceBlock(
            Headline: $"effective_cache_size is {stated}",
            Investigation:
                "`effective_cache_size` is the planner's assumption about how much of the data the operating system's page " +
                "cache and shared_buffers together can hold. It allocates nothing — it is a cost-model input, and at 4 GB it " +
                "tells the planner on a 256 GB host the same thing it tells a 4 GB one: that most random page fetches will " +
                "miss and index scans are expensive. initdb does not write it, so an untouched install runs at the compiled " +
                "value indefinitely. This is a convention finding: it is not amplified by any workload evidence and roots " +
                "only an advisory card (D5). It is NOT a memory figure and this product never treats it as one.",
            Remediation:
                "Set effective_cache_size to roughly 50-75 % of the host's RAM (a planner hint; reloadable, no restart). This " +
                "product does not collect host memory, so the number is yours. Counter-objective: set too high, the planner " +
                "prefers index scans on data that is in fact cold and each one pays a real random read; set too low, it " +
                "prefers sequential scans on data that was cached. Change it, then watch plan shapes on the top statements " +
                "(get_pg_statement_stats) rather than assuming the move was free.");
    }

    private static AdviceBlock ComposeRandomPageCost(IReadOnlyDictionary<string, Fact> facts)
    {
        var fact = KnobFact(facts, PgTargetFactKeys.ConfigRandomPageCost);
        var stated = fact is null ? "at the compiled default of 4.0" : fact.Value.ToString("0.0#", CultureInfo.InvariantCulture) + (fact.Value == PgTargetScorer.RandomPageCostDefault ? " — the spinning-disk default" : string.Empty);

        return new AdviceBlock(
            Headline: $"random_page_cost is {stated}",
            Investigation:
                "`random_page_cost` is the planner's ratio of a random page read to a sequential one (seq_page_cost = 1.0). 4.0 " +
                "encodes a spinning disk, where a seek costs several sequential blocks; on SSD, NVMe and cloud block storage " +
                "the ratio is close to 1, and at 4.0 the planner under-uses indexes on exactly the storage where they are " +
                "cheapest. This is a convention finding — a cost-model assumption, not a measurement — and is never " +
                "amplified: the tuning literature's example of a directionally wrong rule is an I/O-cost-class rule applied " +
                "without knowing the storage, so the card says what the value is and what the convention is, and stops.",
            Remediation:
                "On SSD-class storage the convention is 1.1 (some operators use 1.0-1.5); it is reloadable and can be set per " +
                "tablespace with ALTER TABLESPACE ... SET (random_page_cost = ...) where storage tiers differ. Counter-objective: " +
                "lowering it biases every plan toward index scans and nested loops; a workload that was well served by " +
                "sequential scans on a cached table can regress. Change it where the storage is known, then compare the top " +
                "statements' plan shapes before and after rather than assuming.");
    }

    private static AdviceBlock ComposeTrackIoTiming(IReadOnlyDictionary<string, Fact> facts)
    {
        var fact = KnobFact(facts, PgTargetFactKeys.ConfigTrackIoTiming);
        var stated = fact is null ? "off" : (fact.Value == 0 ? "off" : "on");

        return new AdviceBlock(
            Headline: $"track_io_timing is {stated} — block read and write times are not being measured",
            Investigation:
                "With `track_io_timing` off, `blk_read_time` and `blk_write_time` in pg_stat_statements and pg_stat_database " +
                "read as zero, and every I/O question this product can otherwise answer per statement — which queries wait " +
                "on storage, how much of a statement's time is disk — has no data behind it. This is a monitoring-coverage " +
                "advisory (the PostgreSQL analogue of a Query Store that is not capturing): nothing is wrong with the server, " +
                "something is missing from what can be seen of it.",
            Remediation:
                "Turn `track_io_timing` on (reloadable, no restart; on a managed service it is a parameter-group change). " +
                "Counter-objective: each I/O then takes a clock reading, which is negligible on a host with a fast clock source " +
                "and measurable on one without — run `pg_test_timing` first if the platform is unknown, and expect the " +
                "per-statement I/O columns to start from zero at the moment of the change, not to backfill.");
    }

    /// <summary>Context, never a root (base 0): what the block says if a reader asks for it directly.</summary>
    private static AdviceBlock ComposeCheckpointTimeoutContext(IReadOnlyDictionary<string, Fact> facts)
    {
        var fact = KnobFact(facts, PgTargetFactKeys.ConfigCheckpointTimeout);
        var stated = fact is null ? "the shipped 5 min" : KnobSeconds(fact.Value);

        return new AdviceBlock(
            Headline: $"checkpoint_timeout is {stated}",
            Investigation:
                "`checkpoint_timeout` is the clock that starts a checkpoint when WAL volume has not already started one; it is " +
                "stated beside a checkpoint-pressure finding so the requested-vs-timed ratio has its denominator. It is context " +
                "in this analysis — no card roots on it.",
            Remediation:
                "Tune it with max_wal_size, not instead of it: a longer timeout means fewer, larger checkpoints and a longer " +
                "crash recovery; the checkpoint-pressure card, if it fired, carries the measured WAL rate to size against.");
    }

    /// <summary>Context, never a root (base 0).</summary>
    private static AdviceBlock ComposeWalCompressionContext(IReadOnlyDictionary<string, Fact> facts)
    {
        var fact = KnobFact(facts, PgTargetFactKeys.ConfigWalCompression);
        var stated = fact is null ? "off by default" : (fact.Value == 0 ? "off" : "on");

        return new AdviceBlock(
            Headline: $"wal_compression is {stated}",
            Investigation:
                "`wal_compression` compresses the full-page images written after each checkpoint, which are often the bulk of " +
                "WAL on a write-heavy server; it is stated beside a checkpoint-pressure finding because fewer WAL bytes per " +
                "checkpoint interval is one of the two ways to reach max_wal_size less often. It is context — no card roots on it.",
            Remediation:
                "Consider enabling it (lz4 or zstd on PostgreSQL 15+, pglz before) when the checkpoint-pressure card shows a " +
                "high full-page-image share. Counter-objective: CPU per WAL record, on the backends writing it.");
    }

    /* ── helpers shared by the knob family's advice partials (Config / Write / Buffer) ── */

    private static Fact? KnobFact(IReadOnlyDictionary<string, Fact> facts, string key) =>
        facts.TryGetValue(key, out var fact) ? fact : null;

    private static double? KnobMeta(Fact? fact, string key) =>
        fact is not null && fact.Metadata.TryGetValue(key, out var value) ? value : null;

    /// <summary>Megabytes as an operator would write them: whole GB when whole, else MB.</summary>
    private static string KnobMb(double mb)
    {
        var bytes = (long)Math.Round(mb * 1024 * 1024);
        return KnobBytes(bytes);
    }

    private static string KnobBytes(double bytes) => KnobBytes((long)Math.Round(bytes));

    private static string KnobBytes(long bytes)
    {
        const long kb = 1024, mb = kb * 1024, gb = mb * 1024;
        if (bytes >= gb) return Scaled(bytes, gb, "GB");
        if (bytes >= mb) return Scaled(bytes, mb, "MB");
        if (bytes >= kb) return Scaled(bytes, kb, "kB");
        return bytes.ToString(CultureInfo.InvariantCulture) + " B";

        static string Scaled(long value, long unit, string suffix)
        {
            var scaled = value / (double)unit;
            return (value % unit == 0
                    ? scaled.ToString("0", CultureInfo.InvariantCulture)
                    : scaled.ToString("0.#", CultureInfo.InvariantCulture))
                + " " + suffix;
        }
    }

    private static string KnobSeconds(double seconds)
    {
        if (seconds >= 3600 && seconds % 3600 == 0) return (seconds / 3600).ToString("0", CultureInfo.InvariantCulture) + " h";
        if (seconds >= 60 && seconds % 60 == 0) return (seconds / 60).ToString("0", CultureInfo.InvariantCulture) + " min";
        return seconds.ToString("0.#", CultureInfo.InvariantCulture) + " s";
    }

    private static string KnobPct(double share) => (share * 100).ToString("0.#", CultureInfo.InvariantCulture) + " %";

    private static string KnobNum(double value) => value.ToString("#,0.##", CultureInfo.InvariantCulture);
}
