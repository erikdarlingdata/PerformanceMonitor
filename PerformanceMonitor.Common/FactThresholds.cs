/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The numeric thresholds the analysis facts apply, written once for both stores (#8471). Lite's DuckDB fact collector and
/// Darling's PostgreSQL one keep their own dialect's SQL and read these numbers into it, and the drill-down reads that must
/// agree with a fact's firing rule read the same constants. One name per number. A number that differs between the two apps
/// today stays where it is spelled.
/// </summary>
public static class FactThresholds
{
    /// <summary>
    /// PARAMETER_SENSITIVITY: three floors and a ratio over a plan's lifetime worker time, applied as one contiguous predicate
    /// by <see cref="ParameterSensitivitySignature.PredicateSql"/>, and the reader-side bounds beside them.
    /// </summary>
    public static class ParameterSensitivity
    {
        /// <summary>The plan's cheapest execution must have cost at least this much CPU, in microseconds (<c>min_worker_time</c>).</summary>
        public const int MinWorkerTime = 10000;

        /// <summary>The plan's dearest execution must have cost at least this much CPU, in microseconds (<c>max_worker_time</c>).</summary>
        public const int MaxWorkerTime = 250000;

        /// <summary>The plan must have run at least this many times (<c>execution_count</c>).</summary>
        public const int MinExecutions = 20;

        /// <summary>The dearest execution must have cost at least this many times the cheapest.</summary>
        public const int WorkerTimeRatio = 10;

        /// <summary>An offender whose largest memory grant is at least this many times its smallest is reported as grant divergence.</summary>
        public const double GrantDivergenceRatio = 5;

        /// <summary>The fact's reader keeps this many offenders in the finding's metadata and stops reading there.</summary>
        public const int OffenderCap = 20;
    }

    /// <summary>
    /// QUERY_HIGH_DOP: the per-query <c>max_dop</c> floor and the staleness cross-check against the server's current MAXDOP,
    /// emitted by <see cref="QueryHighDopSignature.HighDopQueriesCountSql"/>.
    /// </summary>
    public static class QueryHighDop
    {
        /// <summary>A query counts as high-DOP when its newest plan's <c>max_dop</c> exceeds this.</summary>
        public const int DopFloor = 8;

        /// <summary>The <c>max degree of parallelism</c> value that means unlimited; it makes no claim a reading can exceed.</summary>
        public const int MaxDopUnlimited = 0;
    }
}

/// <summary>
/// The PARAMETER_SENSITIVITY firing signature as SQL (#8471): the predicate block that decides whether the fact fires, written
/// once and spliced into six reads, the fact collector's, the parameter-sensitivity drill-down's and the <c>psp_signature</c>
/// CTE of the regressed-queries read, in each store. Reusing the detector's own floors there is what keeps the co-fired flag
/// honest: a query a drill-down flags as parameter-sensitive is one the detector counts, never a looser lookalike.
/// </summary>
public static class ParameterSensitivitySignature
{
    /// <summary>
    /// The contiguous block: five <c>AND</c> lines joined with LF, the three floors, the caller's compiled-before-the-window
    /// test and the ratio, in that order. <paramref name="compiledBeforeWindow"/> is the store's own test that the plan was
    /// compiled before the window opened, with its positional bind (Lite: <c>creation_time_utc &lt;= $4</c>; Darling: the
    /// newest offset as a rough filter an hour wider than the bound), because the clock de-skew is spelled differently per
    /// dialect and the bind differs per read. <paramref name="indent"/> leads every line, so a block inside a subquery sits
    /// at that subquery's depth.
    /// </summary>
    public static string PredicateSql(string compiledBeforeWindow, string indent = "") =>
        string.Join(
            "\n",
            FormattableString.Invariant($"{indent}AND   min_worker_time >= {FactThresholds.ParameterSensitivity.MinWorkerTime}"),
            FormattableString.Invariant($"{indent}AND   max_worker_time >= {FactThresholds.ParameterSensitivity.MaxWorkerTime}"),
            FormattableString.Invariant($"{indent}AND   execution_count >= {FactThresholds.ParameterSensitivity.MinExecutions}"),
            $"{indent}AND   {compiledBeforeWindow}",
            FormattableString.Invariant($"{indent}AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= {FactThresholds.ParameterSensitivity.WorkerTimeRatio}"));
}

/// <summary>
/// The QUERY_HIGH_DOP count as SQL (#8471): the <c>high_dop_queries</c> projection of each store's query-stats fact read,
/// written once and spliced into the SELECT list after <c>total_spills</c>.
/// </summary>
public static class QueryHighDopSignature
{
    /// <summary>
    /// Three lines joined with LF: distinct (database, query_hash) pairs whose newest reading exceeds the floor, cross-checked
    /// against the server's current MAXDOP (<c>m</c>, the read's <c>current_maxdop</c> CTE) so a plan cached before MAXDOP was
    /// lowered does not count. A missing config row and an unlimited setting make no claim, so both leave the count alone.
    /// </summary>
    public static readonly string HighDopQueriesCountSql = string.Join(
        "\n",
        FormattableString.Invariant($"    COUNT(DISTINCT CASE WHEN v.max_dop > {FactThresholds.QueryHighDop.DopFloor}"),
        FormattableString.Invariant($"                         AND (m.value_in_use IS NULL OR m.value_in_use = {FactThresholds.QueryHighDop.MaxDopUnlimited} OR v.max_dop <= m.value_in_use)"),
        "                        THEN COALESCE(v.database_name, '') || '/' || COALESCE(v.query_hash, '') END) AS high_dop_queries,");
}
