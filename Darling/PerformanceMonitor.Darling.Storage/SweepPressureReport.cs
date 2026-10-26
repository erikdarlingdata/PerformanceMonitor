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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One collector's cost over the health window, in the shape the sweep-pressure arithmetic reads (#6384): the columns of
/// <see cref="CollectionHealthStatements.ComposedSql"/> it needs (average, p95 and max duration) plus the cadence the
/// collector is scheduled at on the server (the effective interval, not the shipped default).
/// </summary>
/// <param name="CollectorName">The collector.</param>
/// <param name="AvgDurationMs">Average run duration over the window.</param>
/// <param name="P95DurationMs">The p95 run duration as the statement reports it.</param>
/// <param name="P95Bucketed">True when <paramref name="P95DurationMs"/> is the upper bound of a duration bucket, which is how
/// <see cref="CollectionHealthStatements.ComposedSql"/> reports it.</param>
/// <param name="MaxDurationMs">The slowest run in the window.</param>
/// <param name="FrequencyMinutes">The effective cadence on this server.</param>
public sealed record SweepCollectorCost(
    string CollectorName, double AvgDurationMs, double P95DurationMs, bool P95Bucketed, double MaxDurationMs, int FrequencyMinutes)
{
    /// <summary>The p95 the peak-cycle arithmetic charges: the value itself when exact, its bucket's lower bound when bucketed.</summary>
    public double P95ChargedMs => P95Bucketed ? CollectionLogHourly.P95FloorMs(P95DurationMs) : P95DurationMs;
}

/// <summary>One row of the Heaviest Collectors panel, rounded the way <c>get_collection_health</c> rounds its
/// <c>sweep_pressure.heaviest_collectors</c> (milliseconds to whole numbers, the budget share to one decimal).</summary>
/// <param name="Collector">The collector.</param>
/// <param name="AvgDurationMs">Average run duration.</param>
/// <param name="P95DurationMs">p95 run duration.</param>
/// <param name="MaxDurationMs">Slowest run.</param>
/// <param name="FrequencyMinutes">Cadence in minutes.</param>
/// <param name="AmortizedMsPerMinute">Average duration over cadence: the ranking key.</param>
/// <param name="PctOfSweepBudgetPerRun">One heavy run against one sweep's budget.</param>
public sealed record HeaviestCollectorRow(
    string Collector,
    double AvgDurationMs,
    double P95DurationMs,
    double MaxDurationMs,
    int FrequencyMinutes,
    double AmortizedMsPerMinute,
    double PctOfSweepBudgetPerRun);

/// <summary>
/// The sweep-pressure roll-up and the heaviest-collector ranking (#2296, #2446, #4999), moved here from the service's MCP
/// tool (#6384) so the web's <c>get_collection_health</c> and the viewer's Collection Health tab compute both from one place.
/// </summary>
/// <param name="Pressure">The roll-up over the collectors that run in the body.</param>
/// <param name="Heaviest">The heaviest body collectors, worst first.</param>
public sealed record SweepPressureReport(SweepPressure Pressure, IReadOnlyList<HeaviestCollectorRow> Heaviest)
{
    /// <summary>The effective interval, in minutes, from which a collector counts as a daily one: it runs detached from its
    /// server's sequential pass instead of inside it (#4938).</summary>
    public const int DailyCollectorIntervalMinutes = 1440;

    /// <summary>The most collectors the Heaviest Collectors panel lists.</summary>
    public const int HeaviestCount = 3;

    /// <summary>Whether the interval is a daily one (#4938), over the EFFECTIVE interval.</summary>
    public static bool IsDailyInterval(int effectiveIntervalMinutes) =>
        effectiveIntervalMinutes >= DailyCollectorIntervalMinutes;

    /// <summary>#4999: whether a collector runs detached from its server's sequential pass BY NAME: query_store (#2700),
    /// plan_correction (#2717) and pg_wait_sampling.</summary>
    public static bool IsDetachedByName(string name) =>
        string.Equals(name, QueryStoreCollector.Instance.Name, StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, PlanCorrectionCollector.Instance.Name, StringComparison.OrdinalIgnoreCase)
        || string.Equals(name, PgWaitSamplingCollector.Instance.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>#4999: THE test for whether a collector's scheduled run leaves its server's sequential pass: the three
    /// detached by name and every daily collector by cadence. The worker's dispatch and this roll-up both use it, so they
    /// cannot disagree about what runs in the body.</summary>
    public static bool RunsDetached(string name, int effectiveIntervalMinutes) =>
        IsDetachedByName(name) || IsDailyInterval(effectiveIntervalMinutes);

    /// <summary>#4999: the collectors whose single-run cost the server's sequential pass pays.</summary>
    public static IEnumerable<SweepCollectorCost> BodyRows(IEnumerable<SweepCollectorCost> rows) =>
        rows.Where(r => !RunsDetached(r.CollectorName, r.FrequencyMinutes));

    /// <summary>The roll-up over the collectors that run in the body.</summary>
    public static SweepPressure ComputePressure(IEnumerable<SweepCollectorCost> rows) =>
        SweepPressureClassifier.Compute(BodyRows(rows).Select(r => (r.CollectorName, r.AvgDurationMs, r.P95ChargedMs, r.FrequencyMinutes)));

    /// <summary>
    /// The heaviest sweep-body collectors, ranked by amortized contribution (average duration over cadence), at most
    /// <see cref="HeaviestCount"/>. "% of run budget" is the p95 floored at the mean against one sweep's budget (#2460).
    /// </summary>
    public static IReadOnlyList<HeaviestCollectorRow> HeaviestOf(IEnumerable<SweepCollectorCost> rows) =>
        BodyRows(rows)
            .Where(r => r.FrequencyMinutes > 0 && r.AvgDurationMs > 0)
            .OrderByDescending(r => r.AvgDurationMs / r.FrequencyMinutes)
            .Take(HeaviestCount)
            .Select(r => new HeaviestCollectorRow(
                r.CollectorName,
                Math.Round(r.AvgDurationMs, 0),
                Math.Round(r.P95DurationMs, 0),
                Math.Round(r.MaxDurationMs, 0),
                r.FrequencyMinutes,
                Math.Round(r.AvgDurationMs / r.FrequencyMinutes, 0),
                Math.Round(SweepPressureClassifier.PeakRunMs(r.AvgDurationMs, r.P95ChargedMs) / SweepPressureClassifier.SweepBudgetMs * 100.0, 1)))
            .ToList();

    /// <summary>Both halves from one set of rows.</summary>
    /// <param name="rows">Every collector's cost on the server.</param>
    public static SweepPressureReport Build(IEnumerable<SweepCollectorCost> rows)
    {
        var all = rows as IReadOnlyCollection<SweepCollectorCost> ?? rows.ToList();
        return new SweepPressureReport(ComputePressure(all), HeaviestOf(all));
    }
}
