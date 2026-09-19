/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for data-file I/O latency (design §3.9). Value-stated from the <c>pg_io_stats</c> fact — the measured ms per read / write, the PostgreSQL major (the family is 16+ only), and <c>track_io_timing</c>'s state when the timings are absent, never folklore about storage; every recommendation names its counter-objective (filled by lane 11 of #3691).
///
/// <para><b>Four renderings of the read fact, one per trackedness shape, and none of them prints a latency it
/// does not have (the #3541 A8 lesson).</b> <c>pg_stat_io</c> absent (major &lt; 16): the block says which major
/// and what the engine offers instead. <c>track_io_timing</c> off: the block says how many reads were counted and
/// that none were timed, points at the <c>CONFIG_PG_TRACK_IO_TIMING</c> card (lane 2's advisory — this family
/// emits no duplicate), and states the knob's counter-objective. Under the operations floor: the quotient is
/// stated with its operation count and marked not graded. Measured: the ms per read, the operation count and
/// rate, the window, the bars WITH their population (Aurora storage, 50 clusters, 2026-09-19), and — when the
/// anomaly co-fired — the hour-of-week routine it departed from.</para>
///
/// <para><b>The Aurora / stock split is in the LEVER, not the number.</b> On <c>aurora-postgres</c> a data-file
/// read is a storage-tier fetch over the network; there is no disk to replace and no filesystem to tune, so the
/// remediation is the working set (<c>shared_buffers</c>, the cache-pressure finding) and the query that misses.
/// On stock PostgreSQL the same number can be the disk, the volume class, or the filesystem — the block says
/// which population the bars were read from and that a local-NVMe host sits far under them by construction.
/// Neither branch recommends <c>fsync</c> / <c>synchronous_commit</c> / <c>full_page_writes</c> (posture, never a
/// performance lever) and neither emits DDL (D8).</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_ioReadStatic = new(
        Headline: "Data-file reads are slow per operation for this storage (pg_stat_io)",
        Investigation:
            "Milliseconds per data-file read over the window — read_time divided by reads from pg_stat_io " +
            "(pg_io_stats, PostgreSQL 16+), differenced per backend type and context with the engine's own " +
            "stats_reset honoured and summed over every identity that reads relation files. The bars (10 ms " +
            "warning, 30 ms critical) were measured on Aurora storage — 14 days × 50 clusters of the dogfood " +
            "fleet, 2026-09-19, where the fleet's own worst hours sit at the warning line — so on a stock " +
            "PostgreSQL host on local NVMe a value near them is very slow, and on network block storage it may be " +
            "the volume's advertised latency.",
        Remediation:
            "Fewer reads first, faster reads second. get_pg_io_stats shows which backend type and context did the " +
            "reading; get_pg_top_queries ordered by shared_blks_read names the statements whose working set missed " +
            "the cache; get_pg_buffer_usage shows what the cache is holding instead. If the buffer-cache-pressure " +
            "finding fired in the same window, the working set is larger than shared_buffers and the lever is that " +
            "knob (which costs memory the OS page cache and work_mem no longer get) or the query. On Aurora the " +
            "storage tier's latency is not tunable from the instance — the working set and the query are the only " +
            "levers. Never trade durability for latency: fsync, synchronous_commit and full_page_writes are " +
            "posture, not performance settings.");

    private static readonly AdviceBlock s_ioWriteStatic = new(
        Headline: "Data-file write latency per operation (pg_stat_io) — stated, not graded",
        Investigation:
            "Milliseconds per data-file write over the window — write_time divided by writes from pg_stat_io " +
            "(pg_io_stats, PostgreSQL 16+), the same reset-aware difference the read fact takes. No bar exists for " +
            "it yet: the measured population is Aurora, where backends do not write data files and the counter is " +
            "not reported at all, so a bar here would be a number nobody measured. The figure is context for the " +
            "checkpoint and background-writer findings until a stock-PostgreSQL population is read.",
        Remediation:
            "Read it beside get_pg_write_stats: if checkpoint write time and this figure climb together the " +
            "storage is absorbing the checkpointer's flush; if only this climbs, backends are writing their own " +
            "dirty buffers because the background writer is behind (maxwritten_clean). Neither is a reason to " +
            "touch fsync or synchronous_commit.");

    /// <summary>The composed block for the two <c>pg_io</c> roots, or the static block when the fact set does not
    /// carry the key (the <see cref="Static"/> path). Null for any other key.</summary>
    private static partial AdviceBlock? ComposeIo(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        switch (key)
        {
            case PgTargetFactKeys.IoReadLatencyMs:
                return factsByKey.TryGetValue(key, out var read) ? ComposeIoRead(read, factsByKey) : s_ioReadStatic;
            case PgTargetFactKeys.IoWriteLatencyMs:
                return factsByKey.TryGetValue(key, out var write) ? ComposeIoWrite(write) : s_ioWriteStatic;
            default:
                return null;
        }
    }

    private static AdviceBlock ComposeIoRead(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var isAurora = fact.Metadata.GetValueOrDefault(PgTargetScorer.IoIsAuroraKey) >= 1;
        var major = (int)fact.Metadata.GetValueOrDefault(PgTargetScorer.IoServerMajorKey);
        var ops = fact.Metadata.GetValueOrDefault(PgTargetScorer.IoOpsKey);
        var opsPerSec = fact.Metadata.GetValueOrDefault(PgTargetScorer.IoOpsPerSecKey);
        var windowClause = IoWindowClause(fact);

        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.IoReasonPgStatIoAbsentKey) >= 1)
        {
            return s_ioReadStatic with
            {
                Headline = $"Data-file read latency cannot be measured — PostgreSQL {major} has no pg_stat_io (16+)",
                Investigation =
                    $"This server's registry major is {major}; pg_stat_io arrived in PostgreSQL 16, so there is no per-operation " +
                    "read time to difference and this pass makes no latency claim for the I/O family (a 0 here would be a " +
                    "measurement nobody took). What this major does offer: blk_read_time in pg_stat_statements and " +
                    "pg_stat_database, per statement and per database, when track_io_timing is on.",
                Remediation =
                    "get_pg_top_queries ordered by blk_read_time shows which statements waited on reads on this major, and " +
                    "get_pg_server_config shows whether track_io_timing is on to populate it. A major upgrade brings pg_stat_io " +
                    "and this family with it; it is a maintenance event, not a performance fix, and this finding is not a reason " +
                    "to schedule one.",
            };
        }

        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.IoReasonTrackIoTimingOffKey) >= 1)
        {
            var knobClause = fact.Metadata.TryGetValue(PgTargetScorer.IoTrackIoTimingConfigKey, out var knob)
                ? (knob >= 1
                    ? " The config snapshot reports track_io_timing = on, so the flip happened inside this window — the next window will measure."
                    : " The config snapshot confirms track_io_timing = off; the CONFIG_PG_TRACK_IO_TIMING advisory is the card for that.")
                : " The config snapshot was not collected for this window; the data alone says the timings were not taken.";
            return s_ioReadStatic with
            {
                Headline = $"Data-file read latency cannot be measured — {IoCount(ops)} reads counted{windowClause}, none timed (track_io_timing is off)",
                Investigation =
                    $"pg_stat_io counted {IoCount(ops)} data-file reads{windowClause} ({IoRate(opsPerSec)}/sec of observed time) and reported 0 " +
                    "read time for all of them: the engine only records I/O time under track_io_timing = on, so the quotient is " +
                    "undefined, not zero, and this pass makes no latency claim." + knobClause,
                Remediation =
                    "Turning track_io_timing on (reloadable, no restart) populates read_time here and blk_read_time in " +
                    "pg_stat_statements from the next window. Its cost is one clock read per I/O — run pg_test_timing on the host " +
                    "first; on a system whose clock source is slow it is a measurable overhead on I/O-heavy backends, which is why " +
                    "it ships off. get_pg_server_config shows the current value and its source.",
            };
        }

        var ms = fact.Value;
        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.IoInsufficientOpsKey) >= 1)
        {
            return s_ioReadStatic with
            {
                Headline = ops > 0
                    ? $"Data-file reads averaged {IoMs(ms)} per operation over only {IoCount(ops)} reads{windowClause} — under the floor, not graded"
                    : $"No data-file reads reached storage{windowClause} — nothing to grade",
                Investigation =
                    (ops > 0
                        ? $"pg_stat_io timed {IoCount(ops)} data-file reads{windowClause} at {IoMs(ms)} per operation on average. "
                        : $"pg_stat_io counted no data-file reads{windowClause}. ") +
                    $"Under {IoCount(PgTargetScorer.IoMinimumOps)} operations in the window a per-operation average is a quotient over a handful of " +
                    "reads and the calibration read saw exactly that tail produce noise, so the figure is stated and not graded (the " +
                    "floor is measured: 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19). A server whose " +
                    "working set fits in shared_buffers lands here by design — that is the cache working, not a gap.",
                Remediation =
                    "Nothing to do for latency. If the cache-pressure finding says otherwise, believe that one: it grades misses, " +
                    "not the speed of the few that happened.",
            };
        }

        var fired = fact.BaseSeverity > 0;
        var routineClause = IoRoutineClause(factsByKey);
        var populationClause =
            " The 10 ms warning and 30 ms critical bars were measured on Aurora storage (hourly ms per read, 14 days × 50 " +
            "clusters of the dogfood fleet, 2026-09-19: per-server routine 1–10 ms, fleet p90 of per-server p99 26 ms, fleet " +
            "max 33.7 ms)." +
            (isAurora
                ? " This is an Aurora cluster, so the bars are this storage tier's own shape."
                : " This is stock PostgreSQL: the bars are an Aurora-storage shape, and a host on local NVMe routinely reads under 1 ms — treat the warning line as generous for it, and read the number against what its volume class advertises.");

        var investigation =
            $"Data-file reads averaged {IoMs(ms)} per operation{windowClause}{routineClause} — {IoCount(ops)} reads at {IoRate(opsPerSec)}/sec of observed time, " +
            "read_time ÷ reads from pg_stat_io summed over every backend type and context that reads relation files " +
            "(a per-operation figure, so a 24-hour and a 4-hour window of steady storage say the same milliseconds)." +
            populationClause +
            (fired
                ? " At or past the warning bar each read is costing what the measured fleet's worst hours cost."
                : " Under the warning bar this fact is context: it is stated so the wait and cache findings can read it, and it roots nothing.");

        var remediation = isAurora
            ? "On Aurora the data-file read is a storage-tier fetch: there is no disk to swap and no filesystem to tune, so the lever " +
              "is the working set and the query. get_pg_top_queries ordered by shared_blks_read names the statements missing the " +
              "cache; get_pg_io_stats shows whether the reads are client backends (the workload) or autovacuum and bulk reads " +
              "(maintenance, which can be scheduled); get_pg_buffer_usage shows what shared_buffers holds instead. If the " +
              "buffer-cache-pressure finding fired, shared_buffers is the knob — larger costs the instance memory that work_mem " +
              "and the OS cache no longer get, and on Aurora is sized by the instance class. Latency alone is not a reason to " +
              "scale the instance; latency with cache pressure and a named statement is."
            : "Fewer reads first, faster reads second. get_pg_top_queries ordered by shared_blks_read names the statements whose " +
              "working set missed the cache; get_pg_io_stats separates client-backend reads (the workload) from autovacuum and " +
              "bulk reads (maintenance, which can be scheduled); get_pg_buffer_usage shows what shared_buffers holds instead. If " +
              "the buffer-cache-pressure finding fired, shared_buffers is the knob (memory the OS page cache and work_mem no longer " +
              "get). If the reads are already few and each is slow, the number is the volume's: compare it with what the storage " +
              "class advertises before touching PostgreSQL. Never trade durability for latency — fsync, synchronous_commit and " +
              "full_page_writes are posture, not performance settings.";

        return s_ioReadStatic with
        {
            Headline = fired
                ? $"Data-file reads averaged {IoMs(ms)} per operation{windowClause}{(isAurora ? " (Aurora storage tier)" : string.Empty)}"
                : $"Data-file reads averaged {IoMs(ms)} per operation{windowClause} — under the warning bar",
            Investigation = investigation,
            Remediation = remediation,
        };
    }

    private static AdviceBlock ComposeIoWrite(Fact fact)
    {
        var ops = fact.Metadata.GetValueOrDefault(PgTargetScorer.IoOpsKey);
        var opsPerSec = fact.Metadata.GetValueOrDefault(PgTargetScorer.IoOpsPerSecKey);
        var windowClause = IoWindowClause(fact);

        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.IoReasonTrackIoTimingOffKey) >= 1)
        {
            return s_ioWriteStatic with
            {
                Headline = $"Data-file write latency cannot be measured — {IoCount(ops)} writes counted{windowClause}, none timed (track_io_timing is off)",
                Investigation =
                    $"pg_stat_io counted {IoCount(ops)} data-file writes{windowClause} ({IoRate(opsPerSec)}/sec of observed time) and reported 0 write " +
                    "time for all of them: the engine records I/O time only under track_io_timing = on, so the quotient is undefined, " +
                    "not zero. The CONFIG_PG_TRACK_IO_TIMING advisory is the card for the knob.",
            };
        }

        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.IoInsufficientOpsKey) >= 1)
        {
            return s_ioWriteStatic with
            {
                Headline = ops > 0
                    ? $"Data-file writes averaged {IoMs(fact.Value)} per operation over only {IoCount(ops)} writes{windowClause} — stated, not graded"
                    : $"No backend data-file writes{windowClause} — stated, not graded",
                Investigation =
                    (ops > 0
                        ? $"pg_stat_io timed {IoCount(ops)} data-file writes{windowClause} at {IoMs(fact.Value)} per operation on average — under the {IoCount(PgTargetScorer.IoMinimumOps)}-operation floor, a quotient over too few writes to describe the storage. "
                        : $"pg_stat_io counted no data-file writes{windowClause}: the checkpointer and background writer kept ahead, or the window was read-only. ") +
                    "Write latency has no bar in this release (see the family's static text), so this is context either way.",
            };
        }

        return s_ioWriteStatic with
        {
            Headline = $"Data-file writes averaged {IoMs(fact.Value)} per operation{windowClause} — stated, not graded",
            Investigation =
                $"Data-file writes averaged {IoMs(fact.Value)} per operation{windowClause} — {IoCount(ops)} writes at {IoRate(opsPerSec)}/sec of observed time, " +
                "write_time ÷ writes from pg_stat_io summed over every backend type and context that writes relation files. No " +
                "bar grades it yet: the measured population (Aurora) does not report the counter, and a stock write bar has not " +
                "been read. Context for the checkpoint and background-writer findings.",
        };
    }

    /// <summary>The anomaly's composed block (called from <c>ComposeAnomaly</c>'s <c>ANOMALY_PG_IO_LATENCY</c> arm):
    /// the peak hourly ms per read, sigmas above the hour-of-week baseline, the baseline itself — or the
    /// first-occurrence rendering with no sigma on a low-quality bucket. The static block claims no figure.</summary>
    private static AdviceBlock ComposeIoLatencyAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = IoAnomalyStatic();
        return factsByKey.TryGetValue(PgTargetFactKeys.AnomalyIoLatency, out var anomaly)
            ? ComposeDeviation(anomaly, fallback, "Data-file read latency", "peak_ms_per_read", v => IoMs(v) + " per read")
            : fallback;
    }

    /// <summary>Built per call rather than held in a <c>static readonly</c>: it composes <c>s_anomalyHedge</c> /
    /// <c>s_anomalyRemediation</c> from <c>PgTargetAdvice.Anomaly.cs</c>, and static initialisers across partial
    /// files have no defined order — a field here could observe theirs as null.</summary>
    private static AdviceBlock IoAnomalyStatic() => new(
        Headline: "Data-file reads were far slower per operation than this server's normal for this time of week",
        Investigation:
            "The window's peak HOURLY milliseconds per data-file read (read_time ÷ reads from pg_stat_io, differenced " +
            "per backend type and context with stats_reset honoured, hours under the 1,000-read floor not rated) was " +
            "judged against this server's hour-of-week baseline of the same quantity over the last 30 days. A latency " +
            "anomaly says the storage answered this server far more slowly than it usually does at this hour — on Aurora " +
            "that is the storage tier, on stock PostgreSQL the volume or a noisy neighbour on it — and the regular " +
            "read-latency finding says whether the level is also high by the measured fleet's standard." + s_anomalyHedge,
        Remediation:
            "get_pg_io_trend shows the hour against its usual level; get_pg_io_stats shows whether the slow reads were the " +
            "workload's or autovacuum's. " + s_anomalyRemediation);

    /// <summary>"against a 1.3 ms routine for this hour" — off the co-fired anomaly's baseline median when the pass
    /// carried one, so the regular fact can say what this storage usually does; empty otherwise.</summary>
    private static string IoRoutineClause(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (!factsByKey.TryGetValue(PgTargetFactKeys.AnomalyIoLatency, out var anomaly)) return string.Empty;
        if (anomaly.Metadata.GetValueOrDefault("baseline_low_quality") >= 1) return string.Empty;
        var median = anomaly.Metadata.GetValueOrDefault("baseline_median");
        var centre = median > 0 ? median : anomaly.Metadata.GetValueOrDefault("baseline_mean");
        return centre > 0 ? $" against a {IoMs(centre)} routine for this hour of the week" : string.Empty;
    }

    private static string IoWindowClause(Fact fact)
    {
        var observedMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.IoObservedMsKey);
        if (observedMs <= 0) return string.Empty;
        var hours = observedMs / 3_600_000.0;
        return hours >= 1
            ? $" over the {hours.ToString("0.#", CultureInfo.InvariantCulture)} observed hours"
            : $" over the {(observedMs / 60_000.0).ToString("0", CultureInfo.InvariantCulture)} observed minutes";
    }

    private static string IoMs(double value) => value.ToString(value >= 100 ? "N0" : "0.0#", CultureInfo.InvariantCulture) + " ms";
    private static string IoCount(double value) => value.ToString("N0", CultureInfo.InvariantCulture);
    private static string IoRate(double value) => value.ToString(value >= 10 ? "N0" : "0.##", CultureInfo.InvariantCulture);
}
