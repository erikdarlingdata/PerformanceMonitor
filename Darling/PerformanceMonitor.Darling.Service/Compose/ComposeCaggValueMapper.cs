/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Rewrites a measure's aggregate expression to read a continuous aggregate's PRE-aggregated columns instead of the
/// raw per-sweep column, when <see cref="ComposeSourceRouter"/> has chosen a CAGG tier. The value stays identical
/// because the CAGG summed exactly what the raw path would.
///
/// <para>Two table families, two remaps:</para>
/// <para>• DELTA tables (query_stats, procedure_stats): <c>SUM(delta_x)</c>→<c>SUM(x_sum)</c>, MIN/MAX the
/// pre-computed <c>x_min</c>/<c>x_max</c>, <c>AVG(delta_x)</c>→<c>SUM(x_sum)/SUM(sample_count)</c> (exact — deltas
/// never NULL); Sum-ratios map both operands. The CAGG columns follow the fixed "strip <c>delta_</c>, append
/// <c>_sum</c>/<c>_min</c>/<c>_max</c>" naming.</para>
/// <para>• QUERY STORE (query_store_stats): its measures are not deltas, so each maps to a specific reshaped
/// column — <c>qs_executions</c> (Delta over execution_count) → <c>execution_count_sum</c> (Sum) or that over
/// <c>sample_count</c> (Avg); the peak gauges (<c>max_duration_us</c>/<c>max_cpu_time_us</c>) → their <c>_max</c>
/// column, MAX only; the execution-weighted average ratios → the pre-multiplied weighted sum over
/// <c>execution_count_sum</c> (the correct weighted mean, not an avg-of-avgs); and the <c>qs_total_*</c>
/// WeightedSum measures (#2732) → the pre-multiplied weighted sum alone. Aggregates the reshaped CAGG did not
/// keep a column for (MIN/MAX of executions, AVG/MIN of the peaks) return false from <see cref="CanRemap"/> and
/// fall back to raw.</para>
/// </summary>
public static class ComposeCaggValueMapper
{
    /// <summary>The compiler's fact-table alias (mirrors <c>ComposeCompiler</c>'s private <c>FactAlias</c>).</summary>
    private const string F = "f";

    private const string QueryStoreTable = "query_store_stats";

    private static bool IsDeltaCaggTable(string sourceTable) =>
        sourceTable is "query_stats" or "procedure_stats";

    /// <summary>
    /// Whether <paramref name="measure"/> at <paramref name="aggregate"/> can be re-expressed against its CAGG. The
    /// compiler MUST fall back to raw when this is false — there is no correct rollup column to read.
    /// </summary>
    public static bool CanRemap(ComposeMeasure measure, ComposeAggregate aggregate)
    {
        ArgumentNullException.ThrowIfNull(measure);

        if (IsDeltaCaggTable(measure.SourceTable))
        {
            return CanRemapDelta(measure, aggregate);
        }

        return string.Equals(measure.SourceTable, QueryStoreTable, StringComparison.Ordinal)
            && CanRemapQueryStore(measure, aggregate);
    }

    private static bool CanRemapDelta(ComposeMeasure measure, ComposeAggregate aggregate)
    {
        if (measure.Kind == MeasureKind.Ratio)
        {
            /* Only the execution-weighted "avg per execution" ratios — SUM(delta)/SUM(delta), both operands
               re-aggregate additively on the CAGG. */
            return measure.RatioMode == MeasureRatioMode.Sum;
        }

        return measure.Archetype == MeasureArchetype.Cumulative
            && aggregate is ComposeAggregate.Sum
                or ComposeAggregate.Avg
                or ComposeAggregate.Min
                or ComposeAggregate.Max;
    }

    private static bool CanRemapQueryStore(ComposeMeasure measure, ComposeAggregate aggregate)
    {
        if (measure.Kind == MeasureKind.Ratio)
        {
            /* qs_avg_* — the execution-weighted mean, reconstructable from the reshaped weighted sums — and
               qs_total_* (#2732) — the weighted sum ITSELF, which the reshaped CAGG materializes directly. */
            return measure.RatioMode is MeasureRatioMode.Weighted or MeasureRatioMode.WeightedSum;
        }

        return measure.Archetype switch
        {
            /* qs_executions: the CAGG kept a SUM (and sample_count) — so Sum and Avg reconstruct; there is no
               per-row min/max column. */
            MeasureArchetype.Delta => aggregate is ComposeAggregate.Sum or ComposeAggregate.Avg,
            /* qs_max_*: the CAGG kept only the MAX of the peak column. */
            MeasureArchetype.Gauge => aggregate is ComposeAggregate.Max,
            _ => false,
        };
    }

    /// <summary>
    /// The native (double-precision, NOT yet unit-scaled — the caller applies the same unit conversion as the raw
    /// path) aggregate expression reading the CAGG columns. Precondition: <see cref="CanRemap"/> is true.
    /// </summary>
    public static string BuildCaggNativeExpr(ComposeMeasure measure, ComposeAggregate aggregate)
    {
        ArgumentNullException.ThrowIfNull(measure);

        return string.Equals(measure.SourceTable, QueryStoreTable, StringComparison.Ordinal)
            ? BuildQueryStoreExpr(measure, aggregate)
            : BuildDeltaExpr(measure, aggregate);
    }

    private static string BuildDeltaExpr(ComposeMeasure measure, ComposeAggregate aggregate)
    {

        if (measure.Kind == MeasureKind.Ratio)
        {
            var numerator = MeasureCatalog.Measure(measure.NumeratorKey!)!;
            var denominator = MeasureCatalog.Measure(measure.DenominatorKey!)!;
            return $"(CAST(SUM({F}.{DeltaSumColumn(numerator)}) AS double precision) " +
                   $"/ NULLIF(SUM({F}.{DeltaSumColumn(denominator)}), 0))";
        }

        var baseColumn = CaggBase(measure.DeltaColumn!);
        return aggregate switch
        {
            ComposeAggregate.Sum => $"CAST(SUM({F}.{baseColumn}_sum) AS double precision)",
            ComposeAggregate.Min => $"CAST(MIN({F}.{baseColumn}_min) AS double precision)",
            ComposeAggregate.Max => $"CAST(MAX({F}.{baseColumn}_max) AS double precision)",
            /* AVG(delta) == SUM(delta)/COUNT(delta) == SUM(x_sum)/SUM(sample_count), exact (deltas never NULL). */
            ComposeAggregate.Avg =>
                $"(CAST(SUM({F}.{baseColumn}_sum) AS double precision) / NULLIF(SUM({F}.sample_count), 0))",
            _ => throw NotRemappable(aggregate),
        };
    }

    private static string BuildQueryStoreExpr(ComposeMeasure measure, ComposeAggregate aggregate)
    {

        if (measure.Kind == MeasureKind.Ratio)
        {
            /* qs_total_* (#2732): SUM(avg * execution_count) with no denominator — the reshaped CAGG stores
               exactly this product-sum, so the total re-aggregates additively across buckets. */
            if (measure.RatioMode == MeasureRatioMode.WeightedSum)
            {
                return $"CAST(SUM({F}.{QsWeightedSumColumn(measure.WeightedValueColumn!)}) AS double precision)";
            }

            /* SUM(avg * execution_count) / SUM(execution_count) — the reshaped CAGG stores the numerator's
               product-sum directly, and execution_count_sum is the denominator. */
            return $"(CAST(SUM({F}.{QsWeightedSumColumn(measure.WeightedValueColumn!)}) AS double precision) " +
                   $"/ NULLIF(SUM({F}.execution_count_sum), 0))";
        }

        if (measure.Archetype == MeasureArchetype.Gauge)
        {
            /* qs_max_* — MAX of the CAGG's per-bucket MAX column (Column + "_max"). */
            return $"CAST(MAX({F}.{measure.Column}_max) AS double precision)";
        }

        /* qs_executions (Delta over execution_count → execution_count_sum). */
        return aggregate switch
        {
            ComposeAggregate.Sum => $"CAST(SUM({F}.{measure.Column}_sum) AS double precision)",
            ComposeAggregate.Avg =>
                $"(CAST(SUM({F}.{measure.Column}_sum) AS double precision) / NULLIF(SUM({F}.sample_count), 0))",
            _ => throw NotRemappable(aggregate),
        };
    }

    /// <summary>A Cumulative measure's CAGG SUM column: its delta column with <c>delta_</c> stripped + <c>_sum</c>.</summary>
    private static string DeltaSumColumn(ComposeMeasure measure) => CaggBase(measure.AggregationColumn!) + "_sum";

    /// <summary>The CAGG base name for a <c>delta_*</c> column (the reshape's <c>sum(delta_x) AS x_sum</c> naming).</summary>
    private static string CaggBase(string deltaColumn) =>
        deltaColumn.StartsWith("delta_", StringComparison.Ordinal) ? deltaColumn["delta_".Length..] : deltaColumn;

    /// <summary>The reshaped QS CAGG's execution-weighted-sum column for a weighted measure's value column
    /// (hand-named in the CAGG DDL, not derivable by a rule).</summary>
    private static string QsWeightedSumColumn(string weightedValueColumn) => weightedValueColumn switch
    {
        "avg_duration_us" => "duration_us_weighted_sum",
        "avg_cpu_time_us" => "cpu_us_weighted_sum",
        _ => throw new InvalidOperationException(
            $"no CAGG weighted-sum column for Query Store value column '{weightedValueColumn}'."),
    };

    private static InvalidOperationException NotRemappable(ComposeAggregate aggregate) =>
        new($"aggregate {aggregate} is not CAGG-remappable — the caller must gate on CanRemap first.");
}
