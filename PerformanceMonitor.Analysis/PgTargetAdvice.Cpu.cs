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
/// Advice for instance CPU (lane 9 of #3542; Aurora / Performance Insights only). Instance CPU, not process CPU —
/// there is no SQL-vs-other split, and no "SQL Server used X% of it" sentence to port.
///
/// <para><b>Value-stated, and the two percentages are never confused (#3281).</b> The composed block states the
/// peak and average percent of the CONFIGURED capacity ceiling (the graded figure), the ACU in use and configured
/// at the peak when the collector had them, and the raw <c>cpu_percent</c> as "a core was pinned" — percent of the
/// capacity currently allocated, which on the serverless class reads 100 whenever one core stays busy for a
/// minute. A fact whose window had NO capacity sample is rendered as not graded: the raw figure is stated and named
/// for what it is, and the block says why no severity was assigned ("Unknown, never Healthy" — the fleet card's
/// rule, #3271). The static block claims no figure.</para>
///
/// <para><b>Both levers carry their counter-objective</b> (OtterTune doctrine): raising the ACU ceiling buys capacity
/// at cost and hides the query that needs it; fixing the query is durable and takes finding it. No DDL (D8).</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_cpuFactStatic = new(
        Headline: "Instance CPU is near its configured capacity ceiling (Aurora)",
        Investigation:
            "Percent of the CONFIGURED capacity ceiling in use (acu_utilization_percent from pg_cpu_utilization — " +
            "AWS Performance Insights, collected for Aurora targets only), the window's peak of the collector's " +
            "five-minute averages. This is the figure the fleet card and the High CPU alert band on; the raw " +
            "cpu_percent is percent of the capacity currently allocated and is reported beside it, never graded, " +
            "because on a serverless instance it reads 100 whenever one core stays busy for a minute. The 80% / 95% " +
            "bands are the fleet ladder repeated on the same quantity and are fleet-measured: 80% is about the 99.8th " +
            "and 95% about the 99.9th percentile of the measured fleet's five-minute samples (threshold_lineage = 1).",
        Remediation:
            "Find what is consuming the capacity before buying more of it: get_pg_statement_stats over this window " +
            "ranks statements by total execution time, and the top few are where the CPU went. Raising the maximum " +
            "ACU (or the instance class) is the fast lever and costs money for every hour it is provisioned while " +
            "leaving the query that needs it in place; fixing that query is durable and takes finding it. If the " +
            "connection-saturation finding fired in the same window, arrivals are queueing on CPU and a pooler will " +
            "not buy the CPU back.");

    /// <summary>The composed block for the <c>PG_CPU_PERCENT</c> root, or the static block when the fact set does
    /// not carry it (the <see cref="Static"/> path). Null for any other key.</summary>
    private static partial AdviceBlock? ComposeCpu(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (key != PgTargetFactKeys.CpuPercent) return null;
        if (!factsByKey.TryGetValue(key, out var fact)) return s_cpuFactStatic;

        var measured = fact.Metadata.GetValueOrDefault(PgTargetScorer.CpuCapacityMeasuredKey) >= 1.0;
        var samples = fact.Metadata.GetValueOrDefault(PgTargetScorer.CpuSampleCountKey);
        var samplesClause = samples > 0 ? $" across {samples.ToString("N0", CultureInfo.InvariantCulture)} five-minute samples" : string.Empty;
        var rawPeak = fact.Metadata.TryGetValue(PgTargetScorer.CpuPeakPercentKey, out var rp) ? rp : (double?)null;
        var rawClause = rawPeak is { } raw
            ? $" The raw cpu_percent peaked at {Pct(raw)} of the capacity currently allocated — a core was pinned — which is reported, not graded."
            : string.Empty;

        if (!measured)
        {
            return s_cpuFactStatic with
            {
                Headline = rawPeak is { } r
                    ? $"Instance CPU peaked at {Pct(r)} of allocated capacity — not graded, no capacity reading in the window"
                    : "Instance CPU was collected but carried no capacity reading in the window — not graded",
                Investigation =
                    $"pg_cpu_utilization holds rows for this instance{samplesClause}, but none in the window carried " +
                    "acu_utilization_percent, so there is no percent-of-configured-ceiling to grade and this pass assigns the " +
                    "CPU family no severity: a percentage of an unknown denominator is Unknown, never Healthy (#3271). A " +
                    "provisioned instance class has no ACU concept and lands here by design; a serverless one landing here " +
                    "means Performance Insights returned no capacity points for the window." + rawClause,
            };
        }

        var peak = fact.Metadata.GetValueOrDefault(PgTargetScorer.CpuPeakCapacityPctKey, fact.Value);
        var avg = fact.Metadata.GetValueOrDefault(PgTargetScorer.CpuAvgCapacityPctKey);
        var acuClause = fact.Metadata.TryGetValue(PgTargetScorer.CpuPeakCapacityAcuKey, out var acu) && fact.Metadata.TryGetValue(PgTargetScorer.CpuMaxConfiguredAcuKey, out var max)
            ? $" — {acu.ToString("0.#", CultureInfo.InvariantCulture)} of {max.ToString("0.#", CultureInfo.InvariantCulture)} configured ACUs at the peak"
            : string.Empty;
        var fired = fact.BaseSeverity > 0;

        return s_cpuFactStatic with
        {
            Headline = fired
                ? $"Instance CPU peaked at {Pct(peak)} of its configured capacity ceiling{acuClause}"
                : $"Instance CPU peaked at {Pct(peak)} of its configured capacity ceiling — under the warning bar",
            Investigation =
                $"Percent of the CONFIGURED capacity ceiling peaked at {Pct(peak)} and averaged {Pct(avg)}{samplesClause}{acuClause} " +
                "(acu_utilization_percent from pg_cpu_utilization — AWS Performance Insights, Aurora only; the window's peak of the " +
                "collector's five-minute averages). This is the figure the fleet card and the High CPU alert band on." + rawClause +
                (fired
                    ? " At or past the 80% warning bar the instance has little headroom for a burst; the 80% / 95% bands are the fleet ladder repeated and are fleet-measured — about the 99.8th and 99.9th percentile of the measured fleet's five-minute samples (threshold_lineage = 1)."
                    : " Under the 80% warning bar this fact is context: it is stated so a sibling finding can read it, and it roots nothing."),
        };
    }

    private static string Pct(double value) => value.ToString("0.#", CultureInfo.InvariantCulture) + "%";
}
