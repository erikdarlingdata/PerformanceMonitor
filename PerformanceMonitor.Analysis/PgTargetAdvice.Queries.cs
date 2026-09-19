/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the top statements (lane 7): the share stated as the number that decided, the per-call figures
/// stated as the lever, and the caveat that <c>queryid</c> is not stable across major upgrades, so occurrence
/// tracking restarts at one.
///
/// <para><b>Value-stated, never folklore.</b> The composed block reads the fact the scorer graded —
/// <c>share_of_window_time</c>, <c>calls</c>, <c>mean_exec_ms</c>, <c>calls_per_sec</c>, <c>max_exec_ms</c>,
/// <c>database_count</c>, <c>temp_blks_written</c> — and says those numbers. The static block (an empty fact
/// set: the read-time fallback for a finding persisted without frozen story text) says what the family
/// concludes without claiming any figure it does not have.</para>
///
/// <para><b>Two levers, both stated, neither chosen by a bar.</b> The SQL Server composer permutes on an
/// execution-count constant (frequent-cheap vs rare-heavy). This one states the arithmetic instead — total =
/// calls × mean — and names both levers with the figures beside them, because the honest split point is a
/// judgment about THIS statement's mean against THIS workload, and a constant in prose would be a threshold
/// the lineage rule could not see. Every recommendation names its counter-objective (OtterTune doctrine):
/// running it less often trades result freshness; making a call cheaper with an index trades write
/// amplification on the indexed table and vacuum work; and no DDL is written here (D8).</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_badActorStatic = new(
        Headline: "One statement shape held a large share of the window's execution time",
        Investigation:
            "PostgreSQL's pg_stat_statements attributes elapsed execution time to statement shapes, and one shape " +
            "holds enough of the window's total that the window's story is largely that statement. The share is " +
            "taken over EVERY shape that ran in the window, not over the handful returned; a large share alone is " +
            "routine on an idle server, so the number that decided is the fleet-measured busy floor (the window's total " +
            "statement time as a fraction of observed time) and the share bars are read given it (threshold_lineage = 1). The statement " +
            "is identified by queryid, which is stable within a PostgreSQL major version and is RE-KEYED by a major " +
            "upgrade (and by a change to compute_query_id or the extension version), so this finding's occurrence " +
            "history restarts at one across an upgrade — the drill-down carries the normalised text and a hash of " +
            "it, which is how the same statement is recognised on the other side.",
        Remediation:
            "Read the statement's own deltas in the drill-down and in get_pg_top_queries, then decide which factor of " +
            "total = calls × mean to move. Fewer calls (caching, batching, removing a per-row round trip) trades " +
            "result freshness and application change; a cheaper call (a plan that touches fewer pages — an index " +
            "on the filtered or joined columns, a rewritten predicate, fresh statistics) trades write amplification " +
            "and vacuum work on the indexed table. get_pg_plans has the captured plan when auto_explain is " +
            "configured; get_pg_query_duration_trend shows whether the mean STEPPED (a plan change) or the calls did " +
            "(a workload change), which is the first question.");

    /// <summary>
    /// The composed block for a <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> root, or the static block when the fact set
    /// does not carry the key (the <see cref="Static"/> path, and a story whose facts were not passed).
    /// </summary>
    private static partial AdviceBlock? ComposeQueries(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (!factsByKey.TryGetValue(key, out var fact))
            return s_badActorStatic;

        var share = fact.Metadata.GetValueOrDefault("share_of_window_time");
        var calls = fact.Metadata.GetValueOrDefault("calls");
        var totalMs = fact.Metadata.GetValueOrDefault("total_exec_ms");
        var databases = fact.Metadata.GetValueOrDefault("database_count");
        var tempBlocks = fact.Metadata.GetValueOrDefault("temp_blks_written");
        var hasMean = fact.Metadata.TryGetValue("mean_exec_ms", out var meanMs);
        var hasRate = fact.Metadata.TryGetValue("calls_per_sec", out var callsPerSec);
        var hasMax = fact.Metadata.TryGetValue("max_exec_ms", out var maxMs);
        var queryId = key[PgTargetFactKeys.BadActorKeyPrefix.Length..];

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"Statement queryid {queryId} held {share * 100:0.#}% of the window's total statement execution time ({FormatMs(totalMs)} of {FormatMs(fact.Metadata.GetValueOrDefault("window_total_exec_ms"))}), across ");
        inv.Append(CultureInfo.InvariantCulture, $"{calls:N0} {(calls == 1 ? "call" : "calls")}");
        if (hasRate)
            inv.Append(CultureInfo.InvariantCulture, $" ({callsPerSec:0.##}/s over the span its deltas accrued)");
        if (hasMean)
            inv.Append(CultureInfo.InvariantCulture, $", averaging {meanMs:0.#} ms per call");
        if (hasMax)
            inv.Append(CultureInfo.InvariantCulture, $" with a worst single execution of {maxMs:0.#} ms");
        inv.Append('.');
        if (databases > 1)
            inv.Append(CultureInfo.InvariantCulture, $" The same shape ran against {databases:0} databases; the figures are the total across them.");
        if (tempBlocks > 0)
            inv.Append(CultureInfo.InvariantCulture, $" It wrote {tempBlocks:N0} temp blocks in the window — part of any work_mem spill story on this server.");
        inv.Append(" The share is taken over every statement shape that ran in the window; the busy floor that admitted it and the share bars it crossed are fleet-measured (threshold_lineage = 1) — a large share alone is routine on an idle server, which is why the floor decides.");
        inv.Append(" queryid is stable within a PostgreSQL major and is re-keyed by a major upgrade (or a compute_query_id change), so this finding's occurrence history restarts at one across an upgrade; the drill-down carries the normalised text and its hash for recognising the statement on the other side.");

        var rem = new StringBuilder();
        rem.Append(CultureInfo.InvariantCulture, $"total = calls × mean: {calls:N0}");
        rem.Append(hasMean
            ? string.Format(CultureInfo.InvariantCulture, " × {0:0.#} ms.", meanMs)
            : " calls; the mean is unknown for this window.");
        rem.Append(" If the mean is already small for what the statement does, the lever is running it less often — caching, batching, removing a per-row round trip — which trades result freshness and needs an application change.");
        rem.Append(" If the mean is large, the lever is one execution — a plan that touches fewer pages: an index on the filtered or joined columns, a rewritten predicate, fresh statistics — which trades write amplification and vacuum work on the indexed table.");
        rem.Append(" get_pg_query_duration_trend for this queryid says whether the mean STEPPED (a plan change) or the calls did (a workload change); get_pg_plans has the captured plan when auto_explain is configured.");

        return s_badActorStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "One statement shape held {0:0}% of the window's execution time", share * 100),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    /// <summary>Milliseconds rendered at the scale a reader thinks in: ms under a second, seconds under a
    /// minute, minutes above — the figure is a window total, and "2,160,000 ms" says less than "36.0 min".</summary>
    private static string FormatMs(double ms) => ms switch
    {
        < 1_000 => string.Format(CultureInfo.InvariantCulture, "{0:0} ms", ms),
        < 60_000 => string.Format(CultureInfo.InvariantCulture, "{0:0.#} s", ms / 1_000),
        _ => string.Format(CultureInfo.InvariantCulture, "{0:0.#} min", ms / 60_000),
    };
}
