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
using System.Linq;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the <c>ANOMALY_PG_*</c> facts (lane 9 of #3542) — the SQL Server anomaly composer's shape, which is
/// engine-neutral, with PostgreSQL lineage in every noun: "X reached V this window, Nσ above its M baseline for
/// this hour-of-week (over K baseline samples)", then the hedge — a deviation from THIS server's normal for this
/// time of week, not a proven sustained problem; check the deploy, the job, the workload change.
///
/// <para><b>Three honesty rules the composed prose keeps.</b> A <c>baseline_low_quality</c> z-fact and an
/// <c>is_new</c> ratio-fact render as "first occurrence, no baseline yet for this hour-of-week" and never print a
/// sigma or a multiple the detector did not trust — the sentinel-ratio lie the SQL Server composer retired. The
/// CPU anomaly's number is percent of the CONFIGURED capacity ceiling and the prose says so, with the raw
/// percent-of-allocated reading beside it as "a core was pinned" and never as the deviation (#3281). The
/// wait-profile anomaly exists only for the Aurora measured series in v1, so its prose says "the engine
/// measured" and never "estimated from sampling" — and never "spent" (the per-backend sum is time summed over
/// tasks, the wait family's rule).</para>
///
/// <para>The static block (an empty fact set — the read-time fallback for a finding persisted without frozen
/// story text) says what the family concludes without claiming a figure it does not have.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly string s_anomalyHedge =
        " This is a deviation from this server's own normal for this time of day and week, not necessarily a " +
        "sustained problem — check whether it lines up with a workload change, a deploy, or a one-off job before " +
        "treating it as chronic.";

    private static readonly string s_anomalyRemediation =
        "If it was a one-time event — a report run, a backfill, a deploy — no action beyond awareness. If it " +
        "recurs or sustains, it will cross the standard thresholds and surface as a first-class finding with its " +
        "own detail on a later window; treat it then with the matching PostgreSQL advice. The hour-of-week " +
        "baseline is this server's own 30 days of one-minute history, so a server whose workload changed for " +
        "good will re-baseline itself over the following weeks.";

    private static readonly AdviceBlock s_tpsStatic = new(
        Headline: "Transactions per second ran well above this server's normal for this time of week",
        Investigation:
            "The window's peak transaction rate (xact_commit + xact_rollback from pg_database_stats, differenced " +
            "per database with the engine's own stats_reset honoured, summed across databases) was judged against " +
            "this server's hour-of-week baseline of the same rate over the last 30 days. Throughput is context, not " +
            "a symptom: the finding says the server was asked to do far more than it usually is at this hour." + s_anomalyHedge,
        Remediation: s_anomalyRemediation);

    private static readonly AdviceBlock s_sessionStatic = new(
        Headline: "Far more sessions were connected than this server's normal for this time of week",
        Investigation:
            "The window's peak instance-wide session count (total_sessions from pg_session_states — every backend " +
            "pg_stat_activity reports, PostgreSQL's own background processes included) was judged against this " +
            "server's hour-of-week baseline. A session surge is often a connection-pool leak, a retry storm, or a " +
            "deploy that opened a second pool; if the connection-saturation finding fired in the same window this " +
            "anomaly is part of that story." + s_anomalyHedge,
        Remediation: s_anomalyRemediation);

    private static readonly AdviceBlock s_cpuStatic = new(
        Headline: "Instance CPU ran well above this instance's normal for this time of week (Aurora)",
        Investigation:
            "The window's peak percent of the CONFIGURED capacity ceiling (acu_utilization_percent from " +
            "pg_cpu_utilization — Performance Insights, Aurora only) was judged against this instance's " +
            "hour-of-week baseline. The raw cpu_percent is percent of the capacity currently allocated and is not " +
            "the deviation: on a serverless instance a single busy core reads 100 while the instance sits well " +
            "under its ceiling." + s_anomalyHedge,
        Remediation: s_anomalyRemediation);

    private static readonly AdviceBlock s_deadlockStatic = new(
        Headline: "The deadlock rate ran well above this server's normal for this time of week",
        Investigation:
            "Deadlocks per observed hour — the engine's own pg_stat_database.deadlocks counter, differenced with " +
            "stats_reset honoured — judged against this server's hour-of-week baseline of the same rate. The " +
            "counter counts every deadlock the engine resolved; the log capture (pg_deadlocks) holds the exemplars " +
            "and can hold fewer. If the deadlock-rate finding fired in the same window this anomaly is part of that " +
            "story." + s_anomalyHedge,
        Remediation:
            "Read the exemplars: get_pg_deadlocks shows the captured graphs for the window — the statements, the " +
            "relations and the lock modes on each side. A deadlock is a transaction-ordering fault in the " +
            "application; the durable fix is a consistent lock order or shorter transactions, and " +
            "deadlock_timeout only changes how long the engine waits before checking. If the rate recurs it will " +
            "cross the deadlock-rate threshold and surface as a first-class finding.");

    private static readonly AdviceBlock s_waitProfileStatic = new(
        Headline: "The server's wait profile shifted well above its normal for this time of week (Aurora)",
        Investigation:
            "The window's peak all-types wait rate — milliseconds of waiting per second of observed time, the " +
            "engine measured it (aurora_stat_system_waits deltas in pg_wait_stats), CPU excluded — judged against " +
            "this cluster's hour-of-week baseline. A profile shift says the cluster waited far more than it usually " +
            "does at this hour; the named wait types say on what." + s_anomalyHedge,
        Remediation:
            "get_pg_wait_stats over this window shows which wait types drove the shift; chase the dominant one " +
            "with its own playbook (Lock — lock_timeout, transaction scoping, idle_in_transaction_session_timeout; " +
            "IO — the buffer-cache and checkpoint findings; LWLock:WALWrite — commit rate and synchronous_commit " +
            "posture). If the elevated profile persists across windows the threshold-based finding for the " +
            "leading wait will fire and the standard advice applies.");

    /// <summary>The composed block for an <c>ANOMALY_PG_*</c> root, or the family's static block when the fact
    /// set does not carry the key (the <see cref="Static"/> path). Null for a key no lane has composed. The v2
    /// anomalies each add ONE delegating arm here to a composer beside their family (<c>PgTargetAdvice.Io.cs</c> for
    /// lane 11, <c>.Replication.cs</c> for lane 12, <c>.Write.cs</c> for lane 15): the shared prefix routing sends every
    /// <c>ANOMALY_PG_</c> key here, so the arm is the only way a v2 anomaly reaches its own prose.</summary>
    private static partial AdviceBlock? ComposeAnomaly(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        switch (key)
        {
            case PgTargetFactKeys.AnomalyTps:
                return factsByKey.TryGetValue(key, out var tps)
                    ? ComposeDeviation(tps, s_tpsStatic, "Transactions per second", "peak_tps", v => v.ToString("N0", CultureInfo.InvariantCulture) + "/sec")
                    : s_tpsStatic;

            case PgTargetFactKeys.AnomalySessionSpike:
                return factsByKey.TryGetValue(key, out var sessions)
                    ? ComposeDeviation(sessions, s_sessionStatic, "Session count", "peak_sessions", v => v.ToString("N0", CultureInfo.InvariantCulture) + " sessions")
                    : s_sessionStatic;

            case PgTargetFactKeys.AnomalyCpuSpike:
                return factsByKey.TryGetValue(key, out var cpu)
                    ? ComposeCpuDeviation(cpu)
                    : s_cpuStatic;

            case PgTargetFactKeys.AnomalyDeadlockRate:
                return factsByKey.TryGetValue(key, out var deadlocks)
                    ? ComposeDeadlockRatio(deadlocks)
                    : s_deadlockStatic;

            case PgTargetFactKeys.AnomalyWaitProfile:
                return factsByKey.TryGetValue(key, out var profile)
                    ? ComposeWaitProfileRatio(profile)
                    : s_waitProfileStatic;

            /* v2 (#3691) lane 11: the I/O family's anomaly composes in its family file. */
            case PgTargetFactKeys.AnomalyIoLatency:
                return ComposeIoLatencyAnomaly(factsByKey);

            /* v2 (#3691) lane 12: the replication family's anomaly composes in its family file, the same way. */
            case PgTargetFactKeys.AnomalyReplicationLag:
                return ComposeReplicationLagAnomaly(factsByKey);
            /* v2 (#3691) lane 15: the WAL-volume anomaly's composer lives with its family (PgTargetAdvice.Write.cs). */
            case PgTargetFactKeys.AnomalyWalVolume:
                return ComposeWalVolumeAnomaly(factsByKey);
            /* wave 3 (#3691, between waves): the blocking anomaly's arm, declared with the stubs so lane 17 composes in
               its family file (PgTargetAdvice.Blocking.cs) and never edits this switch. Null until then — which is what
               the delegation-equality census expects of a stub, and never the SQL Server "Anomalous spike" composer. */
            case PgTargetFactKeys.AnomalyBlocking:
                return ComposeBlockingAnomaly(factsByKey);
            /* lane 24 (#3691): the stock SAMPLED wait profile composes beside the wait family (PgTargetAdvice.Wait.cs) —
               the sampled grade's vocabulary ("estimated from sampling", the duty cycle) lives there, never here. */
            case PgTargetFactKeys.AnomalySampledWaitProfile:
                return ComposeSampledWaitAnomaly(factsByKey);

            default:
                return null;
        }
    }

    /// <summary>
    /// The z-family composer: observed value, sigmas above the hour-of-week baseline, the baseline itself and the
    /// sample count — or, on a <c>baseline_low_quality</c> fact, the first-occurrence rendering with NO sigma.
    /// </summary>
    private static AdviceBlock ComposeDeviation(Fact fact, AdviceBlock fallback, string noun, string observedKey, Func<double, string> fmt)
    {
        if (!fact.Metadata.TryGetValue(observedKey, out var observed))
            return fallback;

        var samples = fact.Metadata.GetValueOrDefault("baseline_samples");
        var samplesClause = samples > 0 ? $" (over {samples.ToString("N0", CultureInfo.InvariantCulture)} baseline samples)" : string.Empty;

        if (fact.Metadata.GetValueOrDefault("baseline_low_quality") >= 1.0)
        {
            return fallback with
            {
                Headline = $"{noun} reached {fmt(observed)} — first occurrence, no baseline yet for this time of week",
                Investigation =
                    $"{noun} reached {fmt(observed)} this window. This server's hour-of-week baseline is too thin to " +
                    $"trust a deviation against yet{samplesClause}, so this fired on its absolute level, not on a " +
                    "measured deviation — treat it as a first look at what this hour does, not a proven regression." + s_anomalyHedge,
            };
        }

        var sigma = fact.Metadata.GetValueOrDefault("deviation_sigma");
        var mean = fact.Metadata.GetValueOrDefault("baseline_mean");
        var median = fact.Metadata.GetValueOrDefault("baseline_median");
        var centre = median > 0 ? median : mean;
        var centreWord = median > 0 ? "median" : "mean";
        var inv = new StringBuilder(
            $"{noun} reached {fmt(observed)} this window, {Sigma(sigma)}σ above its {fmt(centre)} baseline {centreWord} for this hour-of-week{samplesClause}.");
        inv.Append(' ').Append(SourceSentence(fallback));
        inv.Append(s_anomalyHedge);

        return fallback with
        {
            Headline = $"{noun} spiked to {fmt(observed)} — {Sigma(sigma)}σ above its baseline for this time of week",
            Investigation = inv.ToString(),
        };
    }

    /// <summary>The CPU anomaly: the capacity percent is the deviation; the raw reading is stated beside it and
    /// named for what it is.</summary>
    private static AdviceBlock ComposeCpuDeviation(Fact fact)
    {
        var block = ComposeDeviation(fact, s_cpuStatic, "Instance CPU", "peak_capacity_pct", v => v.ToString("0.#", CultureInfo.InvariantCulture) + "% of the configured capacity ceiling");
        if (ReferenceEquals(block, s_cpuStatic)) return block;

        var raw = fact.Metadata.TryGetValue("peak_cpu_percent", out var rawPct)
            ? $" The raw cpu_percent peaked at {rawPct.ToString("0.#", CultureInfo.InvariantCulture)}% of the capacity currently allocated — a core was pinned — which is reported, not graded: on a serverless instance that figure reads 100 whenever one core is busy for a minute."
            : string.Empty;
        return block with { Investigation = block.Investigation + raw };
    }

    /// <summary>The deadlock-rate ratio: count and rate this window, the multiple of the hour-of-week baseline
    /// rate — or the first-occurrence rendering when <c>is_new</c>.</summary>
    private static AdviceBlock ComposeDeadlockRatio(Fact fact)
    {
        if (!fact.Metadata.TryGetValue("current_count", out var count) || !fact.Metadata.TryGetValue("current_rate_per_hour", out var rate))
            return s_deadlockStatic;

        var countText = count.ToString("N0", CultureInfo.InvariantCulture);
        var rateText = rate.ToString("0.#", CultureInfo.InvariantCulture);
        var hours = fact.Metadata.GetValueOrDefault("observed_hours");
        var over = hours > 0 ? $" over {hours.ToString("0.#", CultureInfo.InvariantCulture)} observed hours" : string.Empty;
        var topDb = string.IsNullOrEmpty(fact.DatabaseName) ? string.Empty : $" {fact.DatabaseName} had the most.";

        if (fact.Metadata.GetValueOrDefault("is_new") >= 1.0)
        {
            return s_deadlockStatic with
            {
                Headline = $"{countText} deadlock{(Math.Abs(count - 1) < 0.5 ? string.Empty : "s")} this window ({rateText}/hour) — first occurrence, no baseline yet",
                Investigation =
                    $"The engine counted {countText} deadlock{(Math.Abs(count - 1) < 0.5 ? string.Empty : "s")}{over} ({rateText} per observed hour, from " +
                    $"pg_stat_database.deadlocks differenced with stats_reset honoured).{topDb} This server's hour-of-week deadlock " +
                    "baseline is too thin to trust a ratio against yet, so this fired because the rate reached the deadlock-rate " +
                    "warning tier, not because it deviated from a measured normal — treat it as a new event, not a proven regression." + s_anomalyHedge,
            };
        }

        var ratio = fact.Metadata.GetValueOrDefault("ratio");
        var baselineRate = fact.Metadata.GetValueOrDefault("baseline_rate");
        return s_deadlockStatic with
        {
            Headline = $"The deadlock rate reached {rateText}/hour — about {ratio.ToString("0.#", CultureInfo.InvariantCulture)}× its baseline for this time of week",
            Investigation =
                $"The engine counted {countText} deadlock{(Math.Abs(count - 1) < 0.5 ? string.Empty : "s")}{over} — {rateText} per observed hour, about " +
                $"{ratio.ToString("0.#", CultureInfo.InvariantCulture)}× the {baselineRate.ToString("0.#", CultureInfo.InvariantCulture)}/hour this server normally " +
                $"sees at this hour-of-week (pg_stat_database.deadlocks differenced with stats_reset honoured).{topDb}" + s_anomalyHedge,
        };
    }

    /// <summary>The Aurora wait-profile ratio: peak ms/sec, the multiple or the modified z, the leading (type,
    /// event) contributors — or the first-occurrence rendering when <c>is_new</c>. Says "the engine measured";
    /// never "estimated from sampling", never "spent".</summary>
    private static AdviceBlock ComposeWaitProfileRatio(Fact fact)
    {
        if (!fact.Metadata.TryGetValue("current_ms_per_sec", out var current))
            return s_waitProfileStatic;

        var contributors = fact.Metadata
            .Where(kvp => kvp.Key.StartsWith("contrib_", StringComparison.Ordinal))
            .OrderByDescending(kvp => kvp.Value)
            .Select(kvp => kvp.Key.Substring("contrib_".Length))
            .Take(3)
            .ToList();
        var led = contributors.Count == 0 ? "the collected wait types" : string.Join(", ", contributors);
        var currentText = current.ToString("0.#", CultureInfo.InvariantCulture);

        if (fact.Metadata.GetValueOrDefault("is_new") >= 1.0)
        {
            return s_waitProfileStatic with
            {
                Headline = "The cluster's wait profile is heavy, with no baseline yet for this time of week",
                Investigation =
                    $"The all-types wait rate the engine measured peaked at about {currentText} ms of waiting per second of observed " +
                    $"time this window (CPU excluded), led by {led}. This cluster's hour-of-week wait baseline is too thin to trust " +
                    "a deviation against yet, so this fired on its absolute level — a first look at where the cluster waits, not a " +
                    "proven shift." + s_anomalyHedge,
            };
        }

        var modifiedZ = fact.Metadata.GetValueOrDefault("modified_z");
        var ratio = fact.Metadata.GetValueOrDefault("ratio");
        var mean = fact.Metadata.GetValueOrDefault("baseline_mean");
        var deviation = modifiedZ > 0
            ? $"{Sigma(modifiedZ)} robust sigmas above its {mean.ToString("0.#", CultureInfo.InvariantCulture)} ms/sec baseline"
            : $"about {ratio.ToString("0.#", CultureInfo.InvariantCulture)}× its {mean.ToString("0.#", CultureInfo.InvariantCulture)} ms/sec baseline";
        return s_waitProfileStatic with
        {
            Headline = modifiedZ > 0
                ? $"The cluster's wait profile shifted to {currentText} ms/sec — {Sigma(modifiedZ)}σ above its baseline for this time of week"
                : $"The cluster's wait profile shifted to about {ratio.ToString("0.#", CultureInfo.InvariantCulture)}× its baseline for this time of week",
            Investigation =
                $"The all-types wait rate the engine measured peaked at about {currentText} ms of waiting per second of observed " +
                $"time this window (CPU excluded) — {deviation} for this hour-of-week — led by {led}. This is a shift in the overall " +
                "wait profile, and the named contributors are where to look." + s_anomalyHedge,
        };
    }

    private static string Sigma(double sigma) => sigma.ToString("0.#", CultureInfo.InvariantCulture);

    /// <summary>The static block's source sentence — the clause before the hedge — so the composed block still
    /// names the table and the difference the number came from.</summary>
    private static string SourceSentence(AdviceBlock fallback)
    {
        var investigation = fallback.Investigation;
        var hedgeAt = investigation.IndexOf(s_anomalyHedge, StringComparison.Ordinal);
        return hedgeAt > 0 ? investigation[..hedgeAt] : investigation;
    }
}
