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
/// Advice for the top statements (lane 7) and, since the 2026-09-20 ruling (lane 34), for the statement's
/// own-normal deviation: the share stated as the number the card carries, the statement's OWN hour-of-week routine
/// stated beside it as the number that DECIDES, the per-call figures stated as the lever, and the caveat that
/// <c>queryid</c> is not stable across major upgrades, so occurrence tracking restarts at one.
///
/// <para><b>Value-stated, never folklore.</b> The composed card reads the fact the scorer graded —
/// <c>share_of_window_time</c>, <c>share_band</c>, <c>calls</c>, <c>mean_exec_ms</c>, <c>calls_per_sec</c>,
/// <c>max_exec_ms</c>, <c>database_count</c>, <c>temp_blks_written</c> — and, when the pass emitted
/// <c>ANOMALY_PG_BAD_ACTOR_SHARE</c>, that fact's verdict on THIS statement (<c>own_normal_&lt;queryid&gt;</c>: no
/// trustworthy own-normal yet / within it / beyond it) and, for the statement the anomaly names, its routine and the
/// sigma. The anomaly's own card (<see cref="ComposeBadActorShareAnomaly"/>) leads with the deviation — "held 61 %
/// against its own routine of 22 % ± 4 % for this hour — 9.7σ" — and borrows the card's per-call figures when the
/// card is in the fact set. The static blocks (an empty fact set: the read-time fallback for a finding persisted
/// without frozen story text) say what each concludes without claiming any figure it does not have.</para>
///
/// <para><b>The share is context, said so.</b> Since the ruling the absolute share bars are measured-ROUTINE lines
/// (2026-09-20, §C4: given a busy hour, top-1 share ≥ 0.25 on 85 % of hours, ≥ 0.60 on 44 %), so every card names
/// them for what they are — "routine on this fleet" — and says which of three things is true of this statement's
/// own normal. A card with no anomaly in its pass cannot tell "within its normal" from "no normal yet" (the
/// detector's verdicts ride on the anomaly fact, which exists only when one candidate fired) and says exactly that,
/// rather than either.</para>
///
/// <para><b>Two levers, both stated, neither chosen by a bar.</b> The SQL Server composer permutes on an
/// execution-count constant (frequent-cheap vs rare-heavy). This one states the arithmetic instead — total =
/// calls × mean — and names both levers with the figures beside them, because the honest split point is a
/// judgment about THIS statement's mean against THIS workload, and a constant in prose would be a threshold
/// the lineage rule could not see. Every recommendation names its counter-objective (OtterTune doctrine):
/// running it less often trades result freshness; making a call cheaper with an index trades write
/// amplification on the indexed table and vacuum work, and a new index can change OTHER statements' plans — so
/// no DDL is written here and any index is a thing to test, never a promise.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_badActorStatic = new(
        Headline: "One statement shape held a large share of the window's execution time",
        Investigation:
            "PostgreSQL's pg_stat_statements attributes elapsed execution time to statement shapes, and one shape " +
            "holds enough of the window's total that the window's story is largely that statement. The share is " +
            "taken over EVERY shape that ran in the window, not over the handful returned. A large share alone is " +
            "routine: on the measured fleet (50 Aurora PostgreSQL clusters, 2026-09-20) the top statement of a busy " +
            "hour holds at least a quarter of it 85 % of the time and at least six tenths 44 % of the time, so this " +
            "card is CONTEXT — the fleet-measured busy floor admitted it, and the share bars are named lines, not " +
            "the grade. The grade is the statement's deviation from its OWN hour-of-week share baseline " +
            "(ANOMALY_PG_BAD_ACTOR_SHARE): a statement at its usual share is the workload; a statement far above its " +
            "usual share is the story, and that anomaly walks into this card when it fires. A statement first seen, " +
            "or on a store too young to hold its normal, has no own-normal yet and stays a context card — never " +
            "graded on the absolute share in its place. The statement is identified by queryid, which is stable " +
            "within a PostgreSQL major version and is RE-KEYED by a major upgrade (and by a change to " +
            "compute_query_id or the extension version), so this finding's occurrence history restarts at one " +
            "across an upgrade — the drill-down carries the normalised text and a hash of it, which is how the same " +
            "statement is recognised on the other side.",
        Remediation:
            "Read the statement's own deltas in the drill-down and in get_pg_top_queries, then decide which factor of " +
            "total = calls × mean to move. Fewer calls (caching, batching, removing a per-row round trip) trades " +
            "result freshness and application change; a cheaper call (a plan that touches fewer pages — an index " +
            "on the filtered or joined columns, a rewritten predicate, fresh statistics) trades write amplification " +
            "and vacuum work on the indexed table, and a new index can change other statements' plans, so it is a " +
            "thing to test. get_pg_plans has the captured plan when auto_explain is " +
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
        var ownNormal = OwnNormalVerdict(factsByKey, queryId, out var anomaly);

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

        /* The statement's own normal: the sentence that says what decided. */
        inv.Append(' ').Append(OwnNormalSentence(ownNormal, anomaly, queryId, share));

        /* The absolute share, named for what it is. */
        inv.Append(ShareContextSentence(share));

        inv.Append(" queryid is stable within a PostgreSQL major and is re-keyed by a major upgrade (or a compute_query_id change), so this finding's occurrence history restarts at one across an upgrade; the drill-down carries the normalised text and its hash for recognising the statement on the other side.");

        var rem = new StringBuilder();
        rem.Append(CultureInfo.InvariantCulture, $"total = calls × mean: {calls:N0}");
        rem.Append(hasMean
            ? string.Format(CultureInfo.InvariantCulture, " × {0:0.#} ms.", meanMs)
            : " calls; the mean is unknown for this window.");
        rem.Append(LeversSentence());

        return s_badActorStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "One statement shape held {0:0}% of the window's execution time{1}", share * 100, HeadlineSuffix(ownNormal, anomaly)),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    /* ── the own-normal anomaly (lane 34) ── */

    /// <summary>Built per call (not <c>static readonly</c>), for the reason <c>PgTargetAdvice.Io.cs</c> states: it
    /// composes the hedge and remediation strings declared in <c>PgTargetAdvice.Anomaly.cs</c>, and static
    /// initialisers across partial files have no defined order.</summary>
    private static AdviceBlock BadActorShareAnomalyStatic() => new(
        Headline: "One statement held far more of the window's execution time than is normal for it at this hour",
        Investigation:
            "One statement's share of the window's total execution time (pg_stat_statements' stored deltas, every " +
            "shape that ran) was judged against THAT STATEMENT's own hour-of-week share over the last 30 days — the " +
            "share it usually takes at this hour on this day of the week — and both the window's peak per-collection " +
            "share and its mean cleared the cutoff, so one busy minute does not fire it. This is the bad actor's " +
            "grade: a statement at its usual share, however large, is the workload; a statement far above its usual " +
            "share is the story, and the absolute share is context — on the measured fleet the top statement of a " +
            "busy hour routinely holds a quarter to six tenths of it. The statement's PG_BAD_ACTOR card carries the " +
            "share, the calls, the per-call mean and the drill-down's text, and this anomaly walks into it. queryid " +
            "is re-keyed by a major upgrade, so the statement's own baseline restarts with it." + s_anomalyHedge,
        Remediation:
            "get_pg_query_duration_trend for the statement says whether its mean STEPPED (a plan change) or its calls " +
            "did (a workload change) — the first question; get_pg_top_queries shows the window's other statements " +
            "beside it; get_pg_plans has the captured plan where auto_explain is configured. " + s_anomalyRemediation);

    /// <summary>
    /// The composed block for <see cref="PgTargetFactKeys.AnomalyBadActorShare"/>: the window share against the
    /// statement's own routine, the sigma on the peak and on the mean, and — when the statement's card is in the fact
    /// set — its calls and per-call mean, so the anomaly's story states the same numbers the card does.
    /// </summary>
    private static AdviceBlock ComposeBadActorShareAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = BadActorShareAnomalyStatic();
        if (!factsByKey.TryGetValue(PgTargetFactKeys.AnomalyBadActorShare, out var anomaly) || string.IsNullOrEmpty(anomaly.ObjectName))
            return fallback;

        var queryId = anomaly.ObjectName;
        var windowShare = anomaly.Metadata.GetValueOrDefault("window_share");
        var sigma = anomaly.Metadata.GetValueOrDefault("deviation_sigma");
        var meanSigma = anomaly.Metadata.GetValueOrDefault("mean_sigma");
        var peakShare = anomaly.Metadata.GetValueOrDefault("peak_share");
        var meanShare = anomaly.Metadata.GetValueOrDefault("mean_share");
        var samples = anomaly.Metadata.GetValueOrDefault("baseline_samples");
        var evaluated = anomaly.Metadata.GetValueOrDefault("candidates_evaluated");
        var fired = anomaly.Metadata.GetValueOrDefault("candidates_fired");
        var withoutBaseline = anomaly.Metadata.GetValueOrDefault("candidates_without_baseline");

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"Statement queryid {queryId} held {windowShare * 100:0.#}% of the window's total statement execution time against {RoutineClause(anomaly)} — {Sigma(sigma)}σ on the window's peak per-collection share ({peakShare * 100:0.#}%) and {Sigma(meanSigma)}σ on its mean ({meanShare * 100:0.#}%)");
        if (samples > 0)
            inv.Append(CultureInfo.InvariantCulture, $", over {samples:N0} baseline samples");
        inv.Append('.');

        if (factsByKey.TryGetValue(PgTargetFactKeys.BadActorKeyPrefix + queryId, out var card))
        {
            var calls = card.Metadata.GetValueOrDefault("calls");
            inv.Append(CultureInfo.InvariantCulture, $" The statement ran {calls:N0} {(calls == 1 ? "call" : "calls")}");
            if (card.Metadata.TryGetValue("mean_exec_ms", out var meanMs))
                inv.Append(CultureInfo.InvariantCulture, $" averaging {meanMs:0.#} ms per call");
            inv.Append(" this window; its PG_BAD_ACTOR card carries the rest and the drill-down's text.");
        }

        inv.Append(CultureInfo.InvariantCulture,
            $" Of the window's {evaluated:0} top statements, {fired:0} {(fired == 1 ? "was" : "were")} beyond {(fired == 1 ? "its" : "their")} own normal");
        if (withoutBaseline > 0)
            inv.Append(CultureInfo.InvariantCulture, $" and {withoutBaseline:0} {(withoutBaseline == 1 ? "has" : "have")} no trustworthy own-normal yet");
        inv.Append("; this card names the one furthest beyond it.");
        inv.Append(ShareContextSentence(windowShare));
        inv.Append(" queryid is re-keyed by a major upgrade (or a compute_query_id change), so the statement's own baseline restarts with it.");
        inv.Append(s_anomalyHedge);

        var rem = new StringBuilder();
        rem.Append(CultureInfo.InvariantCulture, $"get_pg_query_duration_trend for queryid {queryId} says whether its mean STEPPED (a plan change) or its calls did (a workload change) — the first question.");
        rem.Append(LeversSentence());
        rem.Append(' ').Append(s_anomalyRemediation);

        return fallback with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "One statement held {0:0}% of the window's execution time — {1}σ beyond its own normal for this hour", windowShare * 100, Sigma(sigma)),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    /* ── shared sentences ── */

    /// <summary>The anomaly's verdict on ONE statement, read from <c>own_normal_&lt;queryid&gt;</c>: null when the pass
    /// emitted no anomaly (the card cannot tell "within" from "none yet" then), else 0 / 1 / 2.</summary>
    private static int? OwnNormalVerdict(IReadOnlyDictionary<string, Fact> factsByKey, string queryId, out Fact? anomaly)
    {
        anomaly = null;
        if (!factsByKey.TryGetValue(PgTargetFactKeys.AnomalyBadActorShare, out var fact))
            return null;
        anomaly = fact;
        return fact.Metadata.TryGetValue("own_normal_" + queryId, out var verdict) ? (int)verdict : null;
    }

    private static string OwnNormalSentence(int? ownNormal, Fact? anomaly, string queryId, double share)
    {
        switch (ownNormal)
        {
            case 2 when anomaly is not null && string.Equals(anomaly.ObjectName, queryId, StringComparison.Ordinal):
                return string.Format(CultureInfo.InvariantCulture,
                    "This is {0}σ beyond the statement's OWN hour-of-week normal ({1}) — which is the grade (ANOMALY_PG_BAD_ACTOR_SHARE, walked into this card): the statement holds far more of the window than it usually holds of this hour.",
                    Sigma(anomaly.Metadata.GetValueOrDefault("deviation_sigma")), RoutineClause(anomaly));
            case 2:
                return string.Format(CultureInfo.InvariantCulture,
                    "This statement is also beyond its OWN hour-of-week normal this window; the pass's own-normal anomaly (ANOMALY_PG_BAD_ACTOR_SHARE) names queryid {0}, the statement furthest beyond its normal, and this card is context beside it.",
                    anomaly?.ObjectName ?? "?");
            case 1:
                return "This is WITHIN the statement's own hour-of-week normal — the share it usually takes at this hour on this day of the week — so the card is context: a routine dominant statement, not a bad actor by its own history.";
            case 0:
                return "The statement has no trustworthy own-normal yet — first seen, or on a store too young to hold its hour-of-week share — so nothing grades it: the card is context, never the absolute share standing in for a baseline.";
            default:
                return "No statement in this pass was beyond its own hour-of-week normal, so this card is context: either this statement is within the share it usually takes at this hour, or it has no trustworthy own-normal yet (first seen; a young store) — the two read the same until its history exists.";
        }
    }

    /// <summary>The routine the anomaly judged against: the bucket's centre (median when the robust frame had one,
    /// else mean) ± its effective dispersion, as percentages of the collection's execution time.</summary>
    private static string RoutineClause(Fact anomaly)
    {
        var median = anomaly.Metadata.GetValueOrDefault("baseline_median");
        var mean = anomaly.Metadata.GetValueOrDefault("baseline_mean");
        var centre = median > 0 ? median : mean;
        var stddev = anomaly.Metadata.GetValueOrDefault("baseline_stddev");
        return string.Format(CultureInfo.InvariantCulture, "its own routine of {0:0.#}% ± {1:0.#}% for this hour-of-week", centre * 100, stddev * 100);
    }

    /// <summary>The absolute share named for what it is on the measured population — context, never the grade.
    /// measured-routine (2026-09-20, §C4, 50 Aurora PostgreSQL clusters): the two lines are
    /// <c>PgTargetScorer.BadActorShareConcerning</c> / <c>BadActorShareCritical</c>, read by reference.</summary>
    private static string ShareContextSentence(double share)
    {
        var band = share >= PgTargetScorer.BadActorShareCritical
            ? string.Format(CultureInfo.InvariantCulture, "at or above {0:0}%", PgTargetScorer.BadActorShareCritical * 100)
            : share >= PgTargetScorer.BadActorShareConcerning
                ? string.Format(CultureInfo.InvariantCulture, "between {0:0}% and {1:0}%", PgTargetScorer.BadActorShareConcerning * 100, PgTargetScorer.BadActorShareCritical * 100)
                : string.Format(CultureInfo.InvariantCulture, "under {0:0}%", PgTargetScorer.BadActorShareConcerning * 100);
        return string.Format(CultureInfo.InvariantCulture,
            " The absolute share ({0}) is context: on the measured fleet a top statement's share of a busy hour is at or above {1:0}% 85% of the time and at or above {2:0}% 44% of the time — routine, which is why the share does not grade this card and the fleet-measured busy floor only admits it.",
            band, PgTargetScorer.BadActorShareConcerning * 100, PgTargetScorer.BadActorShareCritical * 100);
    }

    private static string LeversSentence() =>
        " If the mean is already small for what the statement does, the lever is running it less often — caching, batching, removing a per-row round trip — which trades result freshness and needs an application change." +
        " If the mean is large, the lever is one execution — a plan that touches fewer pages: an index on the filtered or joined columns, a rewritten predicate, fresh statistics — which trades write amplification and vacuum work on the indexed table, and a new index can change other statements' plans, so it is a thing to test, not a promise." +
        " get_pg_query_duration_trend for this queryid says whether the mean STEPPED (a plan change) or the calls did (a workload change); get_pg_plans has the captured plan when auto_explain is configured.";

    private static string HeadlineSuffix(int? ownNormal, Fact? anomaly) => ownNormal switch
    {
        2 when anomaly is not null => string.Format(CultureInfo.InvariantCulture, " — {0}σ beyond its own normal for this hour", Sigma(anomaly.Metadata.GetValueOrDefault("deviation_sigma"))),
        1 => " — within its own normal for this hour",
        0 => " — no own-normal yet",
        _ => string.Empty,
    };

    /// <summary>Milliseconds rendered at the scale a reader thinks in: ms under a second, seconds under a
    /// minute, minutes above — the figure is a window total, and "2,160,000 ms" says less than "36.0 min".</summary>
    private static string FormatMs(double ms) => ms switch
    {
        < 1_000 => string.Format(CultureInfo.InvariantCulture, "{0:0} ms", ms),
        < 60_000 => string.Format(CultureInfo.InvariantCulture, "{0:0.#} s", ms / 1_000),
        _ => string.Format(CultureInfo.InvariantCulture, "{0:0.#} min", ms / 60_000),
    };
}
