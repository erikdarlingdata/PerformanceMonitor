/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

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
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 27 (PG_PLAN_REGRESSION, PG_PARAMETER_SENSITIVITY) and lane 30 (PG_SEQ_SCAN_ADVISORY) */
    private static partial AdviceBlock? ComposePlan(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;

    /* filled by lane 27 — the ANOMALY_PG_PLAN_REGRESSION arm of ComposeAnomaly delegates here (PgTargetAdvice.Anomaly.cs),
       so the anomaly's prose lives with its family and the shared prefix routing never reaches a SQL Server composer. */
    private static partial AdviceBlock? ComposePlanRegressionAnomaly(IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
