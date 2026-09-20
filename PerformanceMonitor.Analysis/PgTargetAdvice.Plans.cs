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
/// Advice for the plan family (design §6). Value-stated from the facts — the statement's <c>query_id</c>, the mean
/// ms before and after the step, the two <c>plan_hash</c> values and when the flip was captured, the skewed column
/// and its <c>top_value_frequency</c>, the scanned relation and the predicate's selectivity — with the counter-objective
/// named on every recommendation. The advice must say that <c>queryid</c> is not stable across major upgrades.
/// PostgreSQL has no plan cache to force a plan in, so a regression's remedy is the statement's or the statistics'
/// shape, never a forced plan; the Seq-Scan card is EVIDENCE only — predicate, rows, selectivity, estimate error —
/// and no card in this family may contain a <c>CREATE INDEX</c> statement (D8). Filled by lane 27 (regression,
/// sensitivity, the anomaly) and lane 30 (the Seq-Scan advisory) of #3691.
///
/// <para><b>Levers, each with its counter-objective (OtterTune doctrine), none emitted as a statement.</b>
/// <c>plan_cache_mode = force_custom_plan</c> makes a prepared statement re-plan on every execution against its
/// actual parameters — it removes the generic-plan flip and costs planning time on every call, so it is a lever
/// for a statement executed through a prepared path and a cost for a hot one. A larger per-column statistics
/// target (<c>ALTER TABLE … ALTER COLUMN … SET STATISTICS</c>, described here as the knob it is and never written
/// out — D8's spirit: the reader decides the DDL) gives the planner a longer most-common-values list, so a skewed
/// value is estimated as skewed rather than as the average — it costs ANALYZE time and <c>pg_statistic</c> space
/// on that column. Rewriting the statement so the skewed predicate is not the one the plan hinges on (two
/// statements, or a literal where the value is known) trades application change. <c>ANALYZE</c> after a bulk load
/// refreshes what the planner believes — it costs one read of the table's sample.</para>
///
/// <para><b>What the advice cannot say, and says it cannot.</b> The statement TEXT is not on the fact
/// (<c>Fact.Metadata</c> is doubles-only, and lane 7 keeps the id out of the doubles for exactness) —
/// <c>get_pg_top_queries</c> shows it. The two plan hashes travel as their leading 48 bits (12 hex characters,
/// exact in a double), which is a prefix a reader can match against <c>get_pg_plans</c>' full hashes, not the
/// hash itself. The plans' shapes (which node changed) are <c>get_pg_plans</c>' to show; the fact says only
/// whether the top node changed and how the node count moved.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_planRegressionStatic = new(
        Headline: "A statement's per-call time stepped up when its captured plan changed",
        Investigation:
            "auto_explain captured a statement (identified by query_id) under two different plan hashes within the window, " +
            "and pg_stat_statements' per-call mean execution time stepped up across the moment the second plan first " +
            "appeared — mean after the flip against mean before it, over the stored deltas on each side. Either reading " +
            "alone is not a regression: a plan can change without cost, and a mean can rise with a busier hour; the two " +
            "together at the same moment are. Every bar the step was graded on (the ratio, the absolute per-call delta, the " +
            "call floor on each side) is chosen, not measured (threshold_lineage = 0). A plan that was never captured is " +
            "not evidence of anything: auto_explain logs only executions slower than auto_explain.log_min_duration, so " +
            "absence means 'not slow enough to log', never 'no plan change'. queryid is stable within a PostgreSQL major " +
            "and is re-keyed by a major upgrade (or a compute_query_id change), so this finding's occurrence history " +
            "restarts at one across an upgrade.",
        Remediation:
            "get_pg_plans for the queryid shows both captured plans and when each first appeared; get_pg_top_queries " +
            "shows the statement's text and its neighbours. PostgreSQL has no plan cache to force the old plan back into, " +
            "so the levers are the inputs the planner chose from: fresh statistics (ANALYZE the tables the plan reads — " +
            "one sample read of each), a larger statistics target on the column whose estimate flipped the plan (more " +
            "ANALYZE time and statistics space on that column), plan_cache_mode = force_custom_plan for a prepared " +
            "statement whose generic plan is the bad one (planning cost on every execution), or a rewrite that takes the " +
            "flipping predicate out of the plan's hinge (an application change).");

    private static readonly AdviceBlock s_parameterSensitivityStatic = new(
        Headline: "A statement is planned differently as its parameter values change, and one of its predicate columns is skewed",
        Investigation:
            "One query_id was captured under several distinct plan hashes in the window, and pg_qualstats names a " +
            "predicate column of that statement whose pg_stats top_value_frequency says one value covers a large fraction " +
            "of the table. That is the PostgreSQL reading of parameter sensitivity: a parameter equal to the common value " +
            "and one equal to anything else are two different selectivities, and the planner is right to choose two " +
            "plans — the cost is that one of them is wrong for the other's parameters. This is a convention-class card " +
            "(it names a mechanism, not a fire) and sits under the story threshold unless PG_PLAN_REGRESSION fired on the " +
            "same statement in the window. Both bars (the hash count, the skew frequency) are chosen, not measured " +
            "(threshold_lineage = 0). queryid is re-keyed by a major upgrade, so occurrence history restarts there.",
        Remediation:
            "get_pg_column_stats shows the column's top_value_frequency and n_distinct; get_pg_predicate_stats shows how " +
            "often the predicate was evaluated and where the planner's estimate was worst; get_pg_plans lists every plan " +
            "hash captured for the statement. The levers: a larger statistics target on the skewed column (a longer " +
            "most-common-values list, at ANALYZE time and statistics-space cost), plan_cache_mode = force_custom_plan for " +
            "a prepared statement (re-planned per call — planning time on every execution), or two statements — one for " +
            "the common value, one for the rest — so each gets the plan its selectivity deserves (an application change). " +
            "No index is proposed here: the evidence is about estimates, not access paths.");

    /// <summary>
    /// The composed block for a plan-family root — <c>PG_PLAN_REGRESSION</c> or <c>PG_PARAMETER_SENSITIVITY</c> — or
    /// the family's static block when the fact set does not carry the key (the <see cref="Static"/> path, and a story
    /// whose facts were not passed). <c>PG_SEQ_SCAN_ADVISORY</c> is lane 30's arm and answers null until it lands
    /// (null IS the delegation the routing census pins).
    /// </summary>
    /* filled by lane 27 (PG_PLAN_REGRESSION, PG_PARAMETER_SENSITIVITY) and lane 30 (PG_SEQ_SCAN_ADVISORY) */
    private static partial AdviceBlock? ComposePlan(string key, IReadOnlyDictionary<string, Fact> factsByKey) => key switch
    {
        PgTargetFactKeys.PlanRegression => factsByKey.TryGetValue(key, out var regression)
            ? ComposePlanRegression(regression, factsByKey)
            : s_planRegressionStatic,
        PgTargetFactKeys.ParameterSensitivity => factsByKey.TryGetValue(key, out var sensitivity)
            ? ComposeParameterSensitivity(sensitivity, factsByKey)
            : s_parameterSensitivityStatic,
        /* lane 30: the PG_SEQ_SCAN_ADVISORY arm — evidence only, never a CREATE INDEX statement (D8). */
        _ => null,
    };

    /// <summary>The regression block, value-stated; the <c>unavailable</c> shape says which precondition is missing.</summary>
    private static AdviceBlock ComposePlanRegression(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanUnavailableKey) >= 1.0)
        {
            return s_planRegressionStatic with
            {
                Headline = "Plan regressions cannot be seen on this server: auto_explain is not loaded",
                Investigation =
                    "pg_plan_capture_readiness reports that auto_explain is not in shared_preload_libraries, so no execution " +
                    "plan is written to the server log and no plan_hash flip can be joined to a statement's mean-time step. " +
                    "This is a statement about what the monitoring can see, not about the workload: a regression may well " +
                    "have happened this window and gone unrecorded. Nothing is graded off this fact.",
                Remediation =
                    "Loading auto_explain needs shared_preload_libraries to name it (a restart) and auto_explain.log_min_duration " +
                    "set to the threshold above which plans are logged; the counter-objective is log volume and the per-statement " +
                    "cost of emitting a plan, which is why the threshold exists — and on a managed platform without a readable " +
                    "server log (Aurora, RDS) the capture is not available at all, so this card stays as it is there. " +
                    "get_pg_plan_capture_readiness lists every facet with its remedy.",
            };
        }

        var queryId = fact.ObjectName ?? "?";
        var before = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanMeanMsBeforeKey);
        var after = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanMeanMsAfterKey);
        var callsBefore = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanCallsBeforeKey);
        var callsAfter = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanCallsAfterKey);
        var hashes = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanHashCountKey);
        var ratio = before > 0 ? after / before : 0;
        var hasFlipAge = fact.Metadata.TryGetValue("flip_age_s", out var flipAgeS);
        var topNodeChanged = fact.Metadata.GetValueOrDefault("top_node_changed") >= 1.0;
        var nodeDelta = fact.Metadata.GetValueOrDefault("node_count_delta");
        var flipped = fact.Metadata.GetValueOrDefault("flipped_statements");

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"Statement queryid {queryId}: mean execution time per call stepped from {before:0.#} ms to {after:0.#} ms ({ratio:0.#}×, +{after - before:0.#} ms per call) at the moment its captured plan changed");
        /* FormatAge is the sessions family's (PgTargetAdvice.Sessions.cs) — one rendering of "how long ago" per class. */
        if (hasFlipAge)
            inv.Append(CultureInfo.InvariantCulture, $" ({FormatAge(flipAgeS)} before the window's end)");
        inv.Append(CultureInfo.InvariantCulture, $", over {callsBefore:N0} calls before the flip and {callsAfter:N0} after.");
        inv.Append(CultureInfo.InvariantCulture, $" The statement was captured under {hashes:0} distinct plan hashes in the window");
        inv.Append(HashPrefixClause(fact));
        inv.Append(topNodeChanged
            ? "; the plan's top node changed"
            : "; the plan's top node is the same in both");
        if (nodeDelta != 0)
            inv.Append(CultureInfo.InvariantCulture, $" and the node count moved by {nodeDelta:+0;-0}");
        inv.Append('.');
        if (flipped > 1)
            inv.Append(CultureInfo.InvariantCulture, $" {flipped:0} statements changed plan in the window; this is the largest step among those with enough calls on both sides.");
        if (PgTargetScorer.SameStatementFired(factsByKey, PgTargetFactKeys.PlanRegression, PgTargetFactKeys.BadActorKeyPrefix))
            inv.Append(" PG_BAD_ACTOR fired for the same queryid: the statement that regressed also holds a large share of the window's execution time, so the step is the workload's, not a corner's.");
        if (factsByKey.TryGetValue(PgTargetFactKeys.AnomalyPlanRegression, out var anomaly) && anomaly.BaseSeverity > 0)
            inv.Append(CultureInfo.InvariantCulture, $" ANOMALY_PG_PLAN_REGRESSION co-fired: the server-wide per-call mean reached {anomaly.Metadata.GetValueOrDefault("peak_mean_ms"):0.#} ms against this hour's baseline — the statistical reading of the same step.");
        if (PgTargetScorer.SameStatementFired(factsByKey, PgTargetFactKeys.PlanRegression, PgTargetFactKeys.ParameterSensitivity)
            && factsByKey.TryGetValue(PgTargetFactKeys.ParameterSensitivity, out var sensitivity))
            inv.Append(CultureInfo.InvariantCulture, $" PG_PARAMETER_SENSITIVITY names the likely mechanism: a predicate column of this statement has a top value covering {sensitivity.Value * 100:0}% of its table.");
        inv.Append(" The ratio, the per-call delta and the call floor it was graded on are chosen, not measured (threshold_lineage = 0). Absence of a captured plan means 'not slow enough to log', never 'no plan change'. queryid is re-keyed by a major upgrade (or a compute_query_id change), so occurrence history restarts at one across it.");

        var rem = new StringBuilder();
        rem.Append(CultureInfo.InvariantCulture, $"get_pg_plans for queryid {queryId} shows both plans and when each first appeared; get_pg_top_queries has the statement's text. ");
        rem.Append(CultureInfo.InvariantCulture, $"The step is {after - before:0.#} ms on every call, {callsAfter:N0} calls since the flip — ");
        rem.Append("PostgreSQL has no plan cache to force the earlier plan back into, so the levers are the planner's inputs: ANALYZE the tables the plan reads (one sample read each) if statistics went stale; a larger statistics target on the column whose estimate hinged the plan (more ANALYZE time and statistics space on it); plan_cache_mode = force_custom_plan when the statement is prepared and its generic plan is the bad one (planning time on every execution); or a rewrite that takes the flipping predicate out of the hinge (an application change).");

        return s_planRegressionStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "Statement queryid {0} got {1:0.#}× slower per call when its plan changed ({2:0.#} ms → {3:0.#} ms)", queryId, ratio, before, after),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    /// <summary>The sensitivity block, value-stated; the two <c>unavailable</c> shapes say which half is missing.</summary>
    private static AdviceBlock ComposeParameterSensitivity(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var queryId = fact.ObjectName ?? "?";
        var hashes = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanHashCountKey);

        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanUnavailableKey) >= 1.0)
        {
            var qualstatsAbsent = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanReasonQualstatsAbsentKey) >= 1.0;
            return s_parameterSensitivityStatic with
            {
                Headline = string.Format(CultureInfo.InvariantCulture,
                    "Statement queryid {0} was captured under {1:0} plans, but its predicate columns cannot be read", queryId, hashes),
                Investigation = string.Format(CultureInfo.InvariantCulture,
                    "Statement queryid {0} was captured under {1:0} distinct plan hashes in the window — the plan-variance half of parameter sensitivity. ", queryId, hashes)
                    + (qualstatsAbsent
                        ? "The predicate half cannot be read: pg_extension_availability reports pg_qualstats is not installed in any database, so which columns the statement filters on, and whether any is skewed, is unknown. "
                        : "The predicate half cannot be read: pg_qualstats is installed but recorded no predicate for this statement — its default sample_rate is 1 / max_connections, so a statement can run for hours and never be sampled. ")
                    + "Nothing is graded off this fact; the variance alone is a mechanism without its evidence.",
                Remediation = qualstatsAbsent
                    ? "Installing pg_qualstats (it must be in shared_preload_libraries, then created in the database the statement runs in) records which predicates are evaluated and how selectively; its counter-objective is a sampled per-query overhead governed by pg_qualstats.sample_rate. get_pg_extensions shows the state per database; get_pg_plans lists the plan hashes."
                    : "Raising pg_qualstats.sample_rate makes the sample catch more statements at a proportionally higher per-query cost. get_pg_predicate_stats shows what was sampled; get_pg_plans lists the plan hashes.",
            };
        }

        var frequency = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanTopValueFrequencyKey);
        var hasError = fact.Metadata.TryGetValue("worst_estimate_error_ratio", out var errorRatio);
        var hasDistinct = fact.Metadata.TryGetValue("n_distinct", out var nDistinct);
        var predicateColumns = fact.Metadata.GetValueOrDefault("predicate_columns");
        var skewedColumns = fact.Metadata.GetValueOrDefault("skewed_columns");
        var database = fact.DatabaseName;

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"Statement queryid {queryId} was captured under {hashes:0} distinct plan hashes in the window, and pg_qualstats names {predicateColumns:0} predicate {(predicateColumns == 1 ? "column" : "columns")} for it, of which {skewedColumns:0} {(skewedColumns == 1 ? "is" : "are")} skewed: the most skewed has a top value covering {frequency * 100:0}% of its table");
        if (database is not null)
            inv.Append(CultureInfo.InvariantCulture, $" (in {database})");
        if (hasDistinct)
            inv.Append(nDistinct < 0
                ? string.Format(CultureInfo.InvariantCulture, ", with n_distinct {0:0.###} (a ratio of the row count: distinct values ≈ {1:0.#}% of rows)", nDistinct, Math.Abs(nDistinct) * 100)
                : string.Format(CultureInfo.InvariantCulture, ", with {0:N0} distinct values", nDistinct));
        inv.Append('.');
        if (hasError)
            inv.Append(CultureInfo.InvariantCulture, $" The planner's worst row estimate on that predicate was off by {errorRatio:0.#}× (pg_qualstats' worst_estimate_error_ratio).");
        inv.Append(" A parameter equal to the common value and one equal to anything else are two different selectivities, so the planner is right to pick two plans; the cost is that each is wrong for the other's parameters.");
        if (PgTargetScorer.SameStatementFired(factsByKey, PgTargetFactKeys.ParameterSensitivity, PgTargetFactKeys.PlanRegression)
            && factsByKey.TryGetValue(PgTargetFactKeys.PlanRegression, out var regression))
            inv.Append(CultureInfo.InvariantCulture, $" PG_PLAN_REGRESSION fired on this statement in the same window ({regression.Metadata.GetValueOrDefault(PgTargetScorer.PlanMeanMsBeforeKey):0.#} ms → {regression.Metadata.GetValueOrDefault(PgTargetScorer.PlanMeanMsAfterKey):0.#} ms per call): the variance cost something, which is what lifts this card over the story threshold.");
        else
            inv.Append(" No regression fired on this statement in the window, so this is a mechanism named, not a cost measured — it sits under the story threshold.");
        inv.Append(" The hash count and the skew frequency it was graded on are chosen, not measured (threshold_lineage = 0). queryid is re-keyed by a major upgrade, so occurrence history restarts there.");

        var rem = new StringBuilder();
        rem.Append("get_pg_column_stats shows the column's top_value_frequency and n_distinct; get_pg_predicate_stats shows the predicate's evaluations and estimate error; get_pg_plans lists every plan hash captured for the statement; get_pg_top_queries has its text. ");
        rem.Append(CultureInfo.InvariantCulture, $"With one value at {frequency * 100:0}% of the table, the levers are: a larger statistics target on that column so the planner's most-common-values list holds the skew (more ANALYZE time and statistics space on the column); plan_cache_mode = force_custom_plan for a prepared statement (re-planned per call — planning time on every execution); or two statements, one for the common value and one for the rest, so each gets the plan its selectivity deserves (an application change). No index is proposed here: the evidence is about estimates, not access paths.");

        return s_parameterSensitivityStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "Statement queryid {0} was planned {1:0} different ways in the window, and a predicate column has one value covering {2:0}% of its table", queryId, hashes, frequency * 100),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    /// <summary>
    /// The anomaly's block — <c>ANOMALY_PG_PLAN_REGRESSION</c>, the SERVER-WIDE per-call mean (every statement's
    /// stored exec-time deltas over every statement's stored call deltas, per collection) against this server's
    /// hour-of-week baseline of the same quantity. The limitation is the block's first sentence: the baseline seam
    /// keys one series per (server, metric) and has no per-statement dimension, so this anomaly says "this server's
    /// statements got slower per call than this hour usually sees", not "this statement did" — the per-statement
    /// flip is <c>PG_PLAN_REGRESSION</c>'s, and this anomaly folds onto it as the corroborator.
    /// </summary>
    /* filled by lane 27 — the ANOMALY_PG_PLAN_REGRESSION arm of ComposeAnomaly delegates here (PgTargetAdvice.Anomaly.cs),
       so the anomaly's prose lives with its family and the shared prefix routing never reaches a SQL Server composer. */
    private static partial AdviceBlock? ComposePlanRegressionAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = PlanRegressionAnomalyStatic();
        return factsByKey.TryGetValue(PgTargetFactKeys.AnomalyPlanRegression, out var anomaly)
            ? ComposeDeviation(anomaly, fallback, "The server-wide mean execution time per statement call", "peak_mean_ms", v => v.ToString("0.#", CultureInfo.InvariantCulture) + " ms")
            : fallback;
    }

    /// <summary>Built per call (not <c>static readonly</c>), for the reason <c>PgTargetAdvice.Io.cs</c> states: it
    /// composes the hedge and remediation strings declared in <c>PgTargetAdvice.Anomaly.cs</c>, and static
    /// initialisers across partial files have no defined order.</summary>
    private static AdviceBlock PlanRegressionAnomalyStatic() => new(
        Headline: "Statements ran slower per call than this server's normal for this time of week",
        Investigation:
            "The window's peak SERVER-WIDE mean execution time per statement call (every statement's stored " +
            "delta_total_exec_time_ms over every statement's stored delta_calls, per pg_statement_stats collection) was " +
            "judged against this server's hour-of-week baseline of the same quantity over the last 30 days. Both the peak " +
            "and the window's mean had to clear the bar, so one slow minute does not fire it. This is a server-level " +
            "reading: the baseline keys one series per server and has no per-statement dimension, so it says the " +
            "server's statements got slower per call than this hour usually sees — not which one. PG_PLAN_REGRESSION " +
            "names the statement whose plan flipped, and this anomaly folds into that story when both fire; alone it is " +
            "as likely a heavier parameter mix or a colder cache as a plan change." + s_anomalyHedge,
        Remediation:
            "get_pg_top_queries ranks the window's statements by time and shows which mean moved; get_pg_query_duration_trend " +
            "for the heaviest shows whether its mean STEPPED (a plan change) or its calls did (a workload change); " +
            "get_pg_plans has the captured plans where auto_explain is configured. " + s_anomalyRemediation);

    /// <summary>The two plan hashes' leading 48 bits, rendered back as the 12 hex characters they were read from —
    /// a prefix the reader matches against <c>get_pg_plans</c>' full hashes. Empty when the collector could not
    /// read them (a hash that was not 12 hex characters long).</summary>
    private static string HashPrefixClause(Fact fact)
    {
        if (!fact.Metadata.TryGetValue("plan_hash_before_prefix", out var before) || !fact.Metadata.TryGetValue("plan_hash_after_prefix", out var after))
            return string.Empty;
        return string.Format(CultureInfo.InvariantCulture, " (plan hash {0}… before the flip, {1}… after)",
            ((long)before).ToString("X12", CultureInfo.InvariantCulture), ((long)after).ToString("X12", CultureInfo.InvariantCulture));
    }

}
