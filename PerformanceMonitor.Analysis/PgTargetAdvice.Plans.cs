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
/// shape, never a forced plan; the Seq-Scan card is evidence FIRST — predicate, rows, selectivity, estimate error,
/// relation size, scan rate — and carries a concrete index suggestion, with its cost beside it, only where the
/// predicate columns are KNOWN from <c>pg_qualstats</c> and every gate holds (the maintainer withdrew the
/// "never DDL" reading of D8 on 2026-09-20: the engine recommends DDL elsewhere, RCSI being the precedent; what
/// stays forbidden is folklore — an index text guessed from a plan's redacted <c>Filter</c> alone). Filled by lane 27
/// (regression, sensitivity, the anomaly) and lane 30 (the Seq-Scan advisory) of #3691.
///
/// <para><b>The Seq-Scan card's string seams.</b> <see cref="Fact.ObjectName"/> carries the top pair's relation as
/// <c>schema.table</c> (the schema from the predicate witness; the bare <c>Relation Name</c> when there is none) and,
/// when <c>pg_qualstats</c> named them, the predicate columns in parentheses, most selective first —
/// <c>public.orders (customer_id, status)</c>; <see cref="SeqScanRelationAndColumns"/> splits it back. The
/// statement ids ride as two 32-bit halves per pair (<see cref="PgTargetScorer.SeqScanQueryId"/>), exact where one
/// double is not. Lower-ranked pairs have ids and figures but no relation name (one string seam, three pairs); the
/// advice says so and points at <c>get_pg_plans</c>.</para>
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
    /// whose facts were not passed) — <c>PG_SEQ_SCAN_ADVISORY</c> included since lane 30 landed.
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
        /* lane 30: the PG_SEQ_SCAN_ADVISORY arm — evidence first; a suggestion with its cost where the predicate is known. */
        PgTargetFactKeys.SeqScanAdvisory => factsByKey.TryGetValue(key, out var scan)
            ? ComposeSeqScanAdvisory(scan, factsByKey)
            : s_seqScanStatic,
        _ => null,
    };

    private static readonly AdviceBlock s_seqScanStatic = new(
        Headline: "A large relation is read sequentially, often, by a statement whose predicate keeps few of the rows it reads",
        Investigation:
            "auto_explain captured plans in which a Seq Scan node over a named relation carries a Filter, and for the same " +
            "statement (query_id) pg_qualstats recorded a predicate on that relation that filtered nine rows in ten or more " +
            "of the rows it evaluated; pg_table_bloat_stats puts the relation's heap at a quarter gigabyte or more; and the " +
            "plan was captured at least three times per observed hour. Any one of those alone is not a finding — a " +
            "sequential scan is the right plan for a small relation, an unselective predicate, or a statement that runs " +
            "once a day — so the card fires only on all three, and even then as an advisory: it does not know the " +
            "relation's write rate or the storage budget, which decide whether an access path would pay for itself. " +
            "The captures-per-hour figure is a LOWER BOUND: auto_explain logs only executions slower than " +
            "auto_explain.log_min_duration, so every scan that finished under the threshold is invisible here. Every bar " +
            "(the selectivity, the size, the rate) is chosen, not measured (threshold_lineage = 0). queryid is re-keyed " +
            "by a major upgrade, so occurrence history restarts there.",
        Remediation:
            "get_pg_plans shows the captured plan with the Seq Scan node, its row estimate against actual rows, and the " +
            "Filter expression (literals redacted); get_pg_predicate_stats shows which columns the predicate names, how " +
            "often it was evaluated and how selectively, and where the planner's estimate was worst; get_pg_top_queries " +
            "has the statement's text, calls and mean time; get_pg_table_bloat has the relation's size. The questions " +
            "before any index: is the predicate stable across executions (a column the statement always filters on, or " +
            "one of several)? is the relation written often (every insert, update and delete would maintain the index)? " +
            "would the storage and the write cost be paid for by the reads saved? When the predicate columns are known " +
            "from pg_qualstats and every gate holds, the composed card names a candidate index with those costs beside " +
            "it; when they are not known, it does not guess one from the plan.");

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

    /// <summary>
    /// The Seq-Scan block, value-stated from the carried pairs; the two <c>unavailable</c> shapes say which instrument
    /// is missing. The suggestion sentence is composed ONLY when the top pair clears every gate AND the predicate
    /// columns rode in on <see cref="Fact.ObjectName"/> — otherwise the remediation says what is missing and asks the
    /// questions instead. Evidence first, always: the sizes, rates and shares come before any DDL.
    /// </summary>
    private static AdviceBlock ComposeSeqScanAdvisory(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var (relation, columns) = SeqScanRelationAndColumns(fact);
        var topId = PgTargetScorer.SeqScanQueryId(fact, 1);
        var queryId = topId?.ToString(CultureInfo.InvariantCulture) ?? "?";

        if (fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanUnavailableKey) >= 1.0
            && fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanReasonAutoExplainOffKey) >= 1.0)
        {
            return s_seqScanStatic with
            {
                Headline = "Sequential-scan shapes cannot be seen on this server: auto_explain is not loaded",
                Investigation =
                    "pg_plan_capture_readiness reports that auto_explain is not in shared_preload_libraries, so no execution plan " +
                    "is written to the server log and no Seq Scan node can be read. This is a statement about what the monitoring " +
                    "can see, not about the workload: large relations may well be scanned sequentially under selective predicates " +
                    "this window and go unrecorded. Nothing is graded off this fact.",
                Remediation =
                    "Loading auto_explain needs shared_preload_libraries to name it (a restart), auto_explain.log_min_duration set " +
                    "to the threshold above which plans are logged, and auto_explain.log_format = json for the capture to parse; " +
                    "auto_explain.log_analyze adds Actual Rows and Rows Removed by Filter to each node at the cost of timing every " +
                    "logged execution. The counter-objective is log volume and the per-statement cost of emitting a plan — and on a " +
                    "managed platform without a readable server log the capture is not available at all. " +
                    "get_pg_plan_capture_readiness lists every facet with its remedy.",
            };
        }

        var pairs = (int)fact.Metadata.GetValueOrDefault(PgTargetScorer.SeqScanPairsKey);
        var candidates = fact.Metadata.GetValueOrDefault(PgTargetScorer.SeqScanCandidatePairsKey);
        var observedHours = fact.Metadata.GetValueOrDefault("observed_hours");
        var captures = Pair(fact, PgTargetScorer.SeqScanCapturesKey, 1);
        var perHour = Pair(fact, PgTargetScorer.SeqScanCapturesPerHourKey, 1);
        var removedShare = Pair(fact, PgTargetScorer.SeqScanRowsRemovedShareKey, 1);
        var heapBytes = Pair(fact, PgTargetScorer.SeqScanHeapBytesKey, 1);
        var selectivity = Pair(fact, PgTargetScorer.SeqScanSelectivityKey, 1);
        var estimateError = Pair(fact, PgTargetScorer.SeqScanEstimateErrorKey, 1);
        var rowsEvaluated = Pair(fact, "rows_evaluated", 1);
        var rowsFiltered = Pair(fact, "rows_filtered", 1);
        var sampleRate = Pair(fact, "sample_rate", 1);
        var relationsNamed = Pair(fact, "relations_named", 1);
        var qualstatsAbsent = fact.Metadata.GetValueOrDefault(PgTargetScorer.PlanReasonQualstatsAbsentKey) >= 1.0;
        var cleared = PgTargetScorer.SeqScanPairClearsEveryGate(fact, 1);

        /* The evidence, in the order an operator weighs it: what was seen (the scan, how often), what it discarded (plan
           side, then the qualstats witness), how big the relation is, then the gates and the co-fires. */
        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"Statement queryid {queryId} was captured {captures ?? 0:0} times in {observedHours:0.#} observed hours ({perHour ?? 0:0.#} per hour — a lower bound: only executions slower than auto_explain.log_min_duration are logged) with a Seq Scan over {relation} carrying a Filter");
        inv.Append(removedShare is { } share
            ? string.Format(CultureInfo.InvariantCulture, "; across those captures the filter removed {0:0.#}% of the rows the scan read (Rows Removed by Filter against Actual Rows).", share * 100)
            : "; the captured plans carry no Actual Rows (auto_explain.log_analyze is off), so how many rows the filter removed is not known from the plan.");
        if (qualstatsAbsent)
            inv.Append(" The predicate's selectivity cannot be read: pg_extension_availability reports pg_qualstats is not installed in any database, so which columns the statement filters on, and what fraction of evaluated rows they discard, is unknown. Nothing is graded off this fact.");
        else if (selectivity is { } sel)
        {
            inv.Append(CultureInfo.InvariantCulture, $" pg_qualstats' most selective predicate of this statement on the relation");
            if (columns is not null)
                inv.Append(CultureInfo.InvariantCulture, $" (columns: {columns})");
            inv.Append(CultureInfo.InvariantCulture, $" filtered {sel * 100:0.#}% of the rows it evaluated ({rowsFiltered ?? 0:N0} of {rowsEvaluated ?? 0:N0}");
            if (sampleRate is { } rate && rate < 1)
                inv.Append(CultureInfo.InvariantCulture, $", sampled at {rate:0.####} — counts are of the sample, not scaled");
            inv.Append(')');
            if (estimateError is { } err)
                inv.Append(CultureInfo.InvariantCulture, $"; the planner's worst row estimate on it was off by {err:0.#}×");
            inv.Append('.');
        }
        else
            inv.Append(" pg_qualstats recorded no predicate for this statement on the relation — its default sample_rate is 1 / max_connections, so a statement can run for hours and never be sampled — and the selectivity gate cannot pass on an unknown.");
        inv.Append(heapBytes is { } heap
            ? string.Format(CultureInfo.InvariantCulture, " The relation's heap is {0} (pg_table_bloat_stats, newest sample{1}).", KnobBytes(heap),
                relationsNamed is > 1 ? string.Format(CultureInfo.InvariantCulture, "; {0:0} relations share the name across databases or schemas and the largest is stated", relationsNamed) : string.Empty)
            : " The relation's size is not known: pg_table_bloat_stats has no sample of it inside the lookback, and the size gate cannot pass on an unknown.");

        if (!qualstatsAbsent)
        {
            inv.Append(cleared
                ? string.Format(CultureInfo.InvariantCulture, " All three gates hold — selective (≥ {0:0}% filtered), large (≥ {1}), recurring (≥ {2:0.#} captures per observed hour) — so this is a recurring selective scan of a large relation, not one slow execution.",
                    PgTargetScorer.SeqScanSelectiveFraction * 100, KnobBytes(PgTargetScorer.SeqScanLargeRelationBytes), PgTargetScorer.SeqScanMinCapturesPerHour)
                : string.Format(CultureInfo.InvariantCulture, " The shape is under this card's bars (selective ≥ {0:0}% filtered, large ≥ {1}, recurring ≥ {2:0.#} captures per observed hour){3}; it is shown, not graded.",
                    PgTargetScorer.SeqScanSelectiveFraction * 100, KnobBytes(PgTargetScorer.SeqScanLargeRelationBytes), PgTargetScorer.SeqScanMinCapturesPerHour,
                    fact.Metadata.GetValueOrDefault("pairs_clearing") > 0 ? " for the top pair, though a lower-ranked pair below clears them" : string.Empty));
        }

        if (pairs > 1)
        {
            inv.Append(CultureInfo.InvariantCulture, $" {candidates:0} (relation, statement) pairs showed the shape in the window; the next by scan rate × rows removed:");
            for (var rank = 2; rank <= pairs; rank++)
            {
                var id = PgTargetScorer.SeqScanQueryId(fact, rank)?.ToString(CultureInfo.InvariantCulture) ?? "?";
                inv.Append(CultureInfo.InvariantCulture, $" queryid {id} at {Pair(fact, PgTargetScorer.SeqScanCapturesPerHourKey, rank) ?? 0:0.#} per hour");
                if (Pair(fact, PgTargetScorer.SeqScanSelectivityKey, rank) is { } s2) inv.Append(CultureInfo.InvariantCulture, $", {s2 * 100:0.#}% filtered");
                if (Pair(fact, PgTargetScorer.SeqScanHeapBytesKey, rank) is { } h2) inv.Append(CultureInfo.InvariantCulture, $", heap {KnobBytes(h2)}");
                inv.Append(PgTargetScorer.SeqScanPairClearsEveryGate(fact, rank) ? " (clears every gate)" : " (under the bars)");
                inv.Append(rank == pairs ? '.' : ';');
            }
            inv.Append(" Their relation names are on get_pg_plans (one string seam carries the top pair's).");
        }
        else if (candidates > 1)
            inv.Append(CultureInfo.InvariantCulture, $" {candidates:0} (relation, statement) pairs showed the shape in the window; this is the top by scan rate × rows removed.");

        if (PgTargetScorer.SeqScanTopStatementBadActorFired(factsByKey))
            inv.Append(" PG_BAD_ACTOR fired for the same queryid: the statement doing this scan also holds a large share of the window's execution time, so the scan has a measured cost this window.");
        if (PgTargetScorer.SeqScanTopStatementRegressed(factsByKey) && factsByKey.TryGetValue(PgTargetFactKeys.PlanRegression, out var regression))
            inv.Append(CultureInfo.InvariantCulture, $" PG_PLAN_REGRESSION fired on the same queryid ({regression.Metadata.GetValueOrDefault(PgTargetScorer.PlanMeanMsBeforeKey):0.#} ms → {regression.Metadata.GetValueOrDefault(PgTargetScorer.PlanMeanMsAfterKey):0.#} ms per call): the statement's plan changed and got slower in the window the scan was captured.");
        inv.Append(" This is an advisory-class card: it grades a shape, not a cost, and does not know the relation's write rate or the storage budget. The selectivity, the size and the rate it was graded on are chosen, not measured (threshold_lineage = 0). queryid is re-keyed by a major upgrade, so occurrence history restarts there.");

        var rem = new StringBuilder();
        rem.Append(CultureInfo.InvariantCulture, $"get_pg_plans for queryid {queryId} shows the captured plan with the Seq Scan node and its Filter (literals redacted); get_pg_predicate_stats shows the predicate columns, their evaluations and estimate error; get_pg_top_queries has the statement's text, calls and mean time; get_pg_table_bloat has {relation}'s size and dead-tuple share. ");
        rem.Append(CultureInfo.InvariantCulture, $"The questions before any access path: is the predicate stable (does the statement always filter {relation} on {(columns ?? "these columns")}, or on several)? is {relation} written often (every insert, update and delete would maintain an index on it)? would the storage and that write cost be paid for by the reads saved at {perHour ?? 0:0.#}+ scans per hour? ");
        if (qualstatsAbsent)
            rem.Append("Installing pg_qualstats (it must be in shared_preload_libraries, then created in the database the statement runs in) records which predicates are evaluated and how selectively — the witness this card needs before it names a column; its counter-objective is a sampled per-query overhead governed by pg_qualstats.sample_rate. get_pg_extensions shows the state per database.");
        else if (cleared && columns is not null)
        {
            /* The suggestion LAST, as corroboration of what the measured-slow plan and the sampled predicate already said
               — never the card's driver — with its cost and the regression caveat in the same breath (the RCSI precedent:
               a DDL suggestion names what it costs where it is made). Only here: every gate held and the columns are
               pg_qualstats' own, never read off the plan's redacted Filter. The maintainer's 2026-09-20 discipline:
               impact is an ESTIMATE from the plan's numbers, never a promised improvement; counts are labelled sampled
               and captures bounded by the log threshold; "a new index can cause regressions elsewhere — test it". */
            rem.Append(CultureInfo.InvariantCulture, $"If the answers favour it, the candidate the evidence supports is CREATE INDEX CONCURRENTLY ON {relation} ({columns}); — columns in pg_qualstats' order of selectivity, which is the order to verify against the statement's text before running it. ");
            rem.Append(CultureInfo.InvariantCulture, $"A new index can cause regressions elsewhere — plan changes on other statements that read {relation}, write amplification on every insert, update and delete to it, and storage — so test it against the workload before it reaches production. ");
            rem.Append(CultureInfo.InvariantCulture, $"Its cost: every insert, update and delete on {relation} maintains it from then on; storage in proportion to the relation's row count and the key width (measure with pg_relation_size once built — {(heapBytes is { } hb ? KnobBytes(hb) + " of heap is the upper bound a single-column btree stays well under" : "the heap size is not known here")}); CONCURRENTLY takes no write lock but runs longer, cannot run inside a transaction, and leaves an INVALID index behind if it fails. ");
            rem.Append(CultureInfo.InvariantCulture, $"What it might save is an ESTIMATE from the plan's own numbers, not a promise: the captured scans read the whole heap to keep {(1 - (selectivity ?? 0)) * 100:0.#}% of the rows evaluated, and the predicate figures are pg_qualstats' SAMPLE (sample_rate {sampleRate ?? 1:0.####}), not the full count. ");
            if (estimateError is { } e && e > 1)
                rem.Append(CultureInfo.InvariantCulture, $"The planner's estimate on the predicate was off by {e:0.#}×, so ANALYZE {relation} first (one sample read) — an index built on a misjudged column may not be chosen. ");
            rem.Append("Only executions slower than auto_explain.log_min_duration were captured, so the scan rate above is the floor of what the scan costs, not its size.");
        }
        else if (cleared)
            rem.Append("No index is named: the predicate columns are not known here (pg_qualstats recorded no predicate for this statement on the relation), and a column read off the plan's redacted Filter would be a guess. get_pg_predicate_stats after raising pg_qualstats.sample_rate, or the statement's text from get_pg_top_queries, names them.");
        else
            rem.Append("No index is named: the shape is under this card's bars, so the evidence does not yet support one — the scan may be the right plan for this relation and predicate. Watch the rate and the selectivity; if the relation grows or the statement's calls rise, the card grades it.");

        var headline = qualstatsAbsent
            ? string.Format(CultureInfo.InvariantCulture, "Statement queryid {0} scans {1} sequentially in captured plans, but the predicate's selectivity cannot be read: pg_qualstats is not installed", queryId, relation)
            : cleared
                ? string.Format(CultureInfo.InvariantCulture, "Statement queryid {0} scans {1} ({2}) sequentially at least {3:0.#}× per hour, keeping {4:0.#}% of the rows it reads",
                    queryId, relation, heapBytes is { } h ? KnobBytes(h) : "size unknown", perHour ?? 0, (1 - (selectivity ?? 0)) * 100)
                : string.Format(CultureInfo.InvariantCulture, "Statement queryid {0} scans {1} sequentially in captured plans; the shape is under this card's bars", queryId, relation);

        return s_seqScanStatic with { Headline = headline, Investigation = inv.ToString(), Remediation = rem.ToString() };
    }

    /// <summary>The <paramref name="rank"/>-th pair's value under <paramref name="key"/>, or null when the collector
    /// did not carry it (an absent source row, never 0).</summary>
    private static double? Pair(Fact fact, string key, int rank) =>
        fact.Metadata.TryGetValue(PgTargetScorer.SeqScanPairKey(key, rank), out var value) ? value : null;

    /// <summary>Splits the Seq-Scan fact's <see cref="Fact.ObjectName"/> — <c>schema.table (col1, col2)</c> — into the
    /// relation and the column list (null when no predicate columns rode along). The relation is whatever precedes
    /// the first <c>" ("</c>, so a relation whose name itself holds a parenthesis (legal, unheard of) keeps its text
    /// up to that point.</summary>
    internal static (string Relation, string? Columns) SeqScanRelationAndColumns(Fact fact)
    {
        var name = fact.ObjectName ?? "?";
        var open = name.IndexOf(" (", StringComparison.Ordinal);
        if (open < 0 || !name.EndsWith(')'))
            return (name, null);
        var columns = name[(open + 2)..^1];
        return (name[..open], columns.Length == 0 ? null : columns);
    }

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
