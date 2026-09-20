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
/// <c>pg_plans</c> — the plan family (filled by lane 27 of #3691, design §6; lane 30 adds the Seq-Scan advisory):
/// <c>PG_PLAN_REGRESSION</c> from a statement's reset-aware mean-ms step-change in <c>pg_statement_stats</c> coinciding
/// with a <c>plan_hash</c> flip in <c>pg_plan_capture</c>, <c>PG_PARAMETER_SENSITIVITY</c> from <c>plan_hash</c>
/// variance across the window beside a skewed <c>top_value_frequency</c> in <c>pg_column_stats</c>, and
/// <c>PG_SEQ_SCAN_ADVISORY</c> from a <c>plan_json</c> Seq Scan over a large relation under a selective predicate
/// (<c>pg_predicate_stats</c>). Bars carry their lineage marker within six lines; unmeasured ones carry
/// <c>threshold_lineage = 0</c>; gates are rates or fractions of OBSERVED time, never absolute totals. No bar may
/// be a SQL Server plan-regression constant reused by value, and no fact or amplifier in this family may carry
/// <c>CREATE INDEX</c> text (D8).
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 27 */
    private static partial double ScorePlanFact(Fact fact) => 0.0;

    /* filled by lane 27 */
    private static partial List<AmplifierDefinition> PlanAmplifiers(string key) => [];
}
