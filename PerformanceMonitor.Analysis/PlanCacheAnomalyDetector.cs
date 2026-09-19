/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The pure, headlessly-testable reference implementation of the §2 plan-cache anomaly
/// detector's correctness rules. The production detector runs the equivalent logic as a
/// single T-SQL statement against <c>collect.query_stats</c> (it cannot be unit-tested
/// against a live server here); this class encodes the SAME row-level exclusion + per-exec
/// anomaly math so the catastrophic correctness rules (§2a) are guarded by headless tests.
///
/// <para>
/// The two raw-total contamination arms the delta framework (install/05_delta_framework.sql)
/// injects — first-collection-of-a-plan_handle and the first-post-restart row — are excluded
/// at the ROW level (R2-MOD-A / R2-MOD-B): a row is a REAL inter-collection delta only when
/// an EARLIER collection exists for the same (sql_handle, offsets, plan_handle) whose
/// collection_time is BOTH before this row AND after this row's server_start_time. The
/// per-exec figures and the materiality CPU sum use ONLY real-delta rows.
/// </para>
/// </summary>
public static class PlanCacheAnomalyDetector
{
    /// <summary>Default anomaly threshold (sibling of PLAN_REGRESSION's regression_factor).</summary>
    public const double DefaultThreshold = 3.0;

    /// <summary>Default materiality floor: a query must contribute at least this much CPU (ms) in the window.</summary>
    public const double DefaultMaterialCpuMsFloor = 1000.0;

    /// <summary>
    /// One collected delta row (the fields the detector reads). Mirrors a
    /// <c>collect.query_stats</c> row AFTER the delta framework has run.
    /// </summary>
    public sealed record StatRow(
        string QueryHash,
        string SqlHandle,
        int StatementStartOffset,
        int StatementEndOffset,
        string PlanHandle,
        DateTime CollectionTime,
        DateTime ServerStartTime,
        long TotalWorkerTimeDelta,   // microseconds (raw query_stats unit)
        long ExecutionCountDelta);

    /// <summary>A qualifying anomaly: per-exec CPU has jumped vs the query's own baseline.</summary>
    public sealed record AnomalyResult(
        string QueryHash,
        double CurrentCpuPerExecMs,
        double BaselineCpuPerExecMs,
        double AnomalyRatio,
        long ExecutionCount,
        double TotalCpuMs);

    /// <summary>
    /// True when this row is a REAL inter-collection delta (NOT a first-collection or
    /// first-post-restart raw-total row), i.e. some earlier collection exists for the same
    /// (sql_handle, offsets, plan_handle) whose collection_time is &lt; this row's AND
    /// &gt; this row's server_start_time. This single predicate drops BOTH contamination
    /// arms by row, without using sample_interval_seconds.
    ///
    /// <para><b>Where this predicate belongs, and where it does not (#3653 A10).</b> It is the restart test
    /// for rows held IN MEMORY with their <c>server_start_time</c> beside them, which is what this detector
    /// has, and it stays the reference statement of the rule here. The SQL baseline tiers (Lite's
    /// <c>BaselineProvider</c>, Darling's <c>PgBaselineProvider</c> over the interval-honest aggregates) do
    /// NOT restate it in SQL: the collector already answered the same question at write time — a counter that
    /// went backwards is stored as delta 0 with <c>sample_interval_seconds = 0</c>
    /// (<c>CollectorDeltaCalculator</c>: interval 0 &lt;=&gt; no delta knowable) — and that stored verdict is
    /// the stronger witness, since the collector SAW the reset where a prior-row test can only infer it and
    /// needs <c>server_start_time</c> joined in to do so. Those tiers read the column and keep their
    /// <c>LAG &gt; N</c> magnitude heuristic only for pre-column rows whose interval was never recorded. Two
    /// statements of one rule, each in the tier that has the inputs for it; neither is an orphan.</para>
    /// </summary>
    public static bool IsRealDeltaRow(StatRow row, IReadOnlyList<StatRow> all) =>
        all.Any(prior =>
            ReferenceEquals(prior, row) == false &&
            prior.SqlHandle == row.SqlHandle &&
            prior.StatementStartOffset == row.StatementStartOffset &&
            prior.StatementEndOffset == row.StatementEndOffset &&
            prior.PlanHandle == row.PlanHandle &&
            prior.CollectionTime < row.CollectionTime &&
            prior.CollectionTime > row.ServerStartTime);

    /// <summary>
    /// Evaluates the detector over a set of collected rows and returns one anomaly per
    /// qualifying query_hash. <paramref name="currentStart"/> splits the window: rows at or
    /// after it are the current window, earlier real-delta rows are the baseline. Only
    /// real-delta rows (§2a) feed the per-exec math AND the materiality sum.
    /// </summary>
    public static IReadOnlyList<AnomalyResult> Evaluate(
        IReadOnlyList<StatRow> rows,
        DateTime currentStart,
        double threshold = DefaultThreshold,
        double materialCpuMsFloor = DefaultMaterialCpuMsFloor)
    {
        var results = new List<AnomalyResult>();
        if (rows is null || rows.Count == 0)
            return results;

        // §2a: keep only genuine inter-collection delta rows.
        var real = rows.Where(r => IsRealDeltaRow(r, rows)).ToList();

        foreach (var group in real.GroupBy(r => r.QueryHash))
        {
            var current = group.Where(r => r.CollectionTime >= currentStart).ToList();
            var baseline = group.Where(r => r.CollectionTime < currentStart).ToList();

            long curExecs = current.Sum(r => r.ExecutionCountDelta);
            long baseExecs = baseline.Sum(r => r.ExecutionCountDelta);
            if (curExecs <= 0 || baseExecs <= 0) continue;

            double curWorkerMs = current.Sum(r => r.TotalWorkerTimeDelta) / 1000.0;
            double baseWorkerMs = baseline.Sum(r => r.TotalWorkerTimeDelta) / 1000.0;
            if (baseWorkerMs <= 0) continue;

            double curPerExec = curWorkerMs / curExecs;
            double basePerExec = baseWorkerMs / baseExecs;
            if (basePerExec <= 0) continue;

            // materiality: the current-window CPU contribution (real-delta rows only, R2-MIN-B).
            double totalCpuMs = curWorkerMs;
            if (totalCpuMs < materialCpuMsFloor) continue;

            double ratio = curPerExec / basePerExec;
            if (ratio < threshold) continue;

            results.Add(new AnomalyResult(
                group.Key, curPerExec, basePerExec, ratio, curExecs, totalCpuMs));
        }

        return results;
    }
}
