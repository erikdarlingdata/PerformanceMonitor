using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Shared WS4 helper: parses already-collected query-plan XML with <see cref="ShowPlanParser"/> +
/// <see cref="PlanAnalyzer"/> and aggregates the missing indexes and actionable warnings across a
/// set of plans. Used by both apps' fact collectors (counts → MISSING_INDEX / PLAN_WARNING facts)
/// and their drill-down enrichers (details → finding.DrillDown), so the parse + dedup logic lives
/// in exactly one place. A plan that fails to parse is skipped — one bad plan never aborts the set.
/// </summary>
public static class PlanAdvisoryAggregator
{
    /// <summary>Aggregate counts for the facts (Fact.Metadata is numeric only).</summary>
    public readonly record struct Summary(
        int MissingIndexCount,
        double MaxImpact,
        int WarningCount,
        int CriticalCount);

    /// <summary>The deduped detail for the drill-down (the specific indexes + warnings).</summary>
    public readonly record struct Details(
        List<MissingIndex> MissingIndexes,
        List<PlanWarning> Warnings);

    /// <summary>Parses the plans and returns aggregate counts for the WS4 facts.</summary>
    public static Summary Summarize(IEnumerable<string> planXmls) =>
        SummarizeCancellable(planXmls, CancellationToken.None);

    /// <summary>Cancellable form of <see cref="Summarize(IEnumerable{string})"/>.</summary>
    public static Summary SummarizeCancellable(IEnumerable<string> planXmls, CancellationToken cancellationToken) =>
        SummarizeCancellable(planXmls, config: null, cancellationToken);

    /// <summary>
    /// #4535: the config-aware form. A disabled rule's findings never appear in
    /// <see cref="Summary.WarningCount"/>/<see cref="Summary.CriticalCount"/>; a severity override is
    /// reflected in <see cref="Summary.CriticalCount"/>. Null <paramref name="config"/> behaves exactly
    /// like the overload above (<see cref="AnalyzerConfig.Default"/>).
    /// </summary>
    public static Summary SummarizeCancellable(IEnumerable<string> planXmls, AnalyzerConfig? config, CancellationToken cancellationToken)
    {
        var details = ExtractCancellable(planXmls, config, cancellationToken);
        var maxImpact = details.MissingIndexes.Count > 0
            ? details.MissingIndexes.Max(i => i.Impact)
            : 0.0;
        var criticalCount = details.Warnings.Count(w => w.Severity == PlanWarningSeverity.Critical);
        return new Summary(details.MissingIndexes.Count, maxImpact, details.Warnings.Count, criticalCount);
    }

    /// <summary>
    /// Parses the plans and returns the deduped missing indexes (keyed on schema.table + the
    /// CREATE text, keeping the highest-impact instance of a duplicate suggestion) and all
    /// actionable warnings across the set.
    /// </summary>
    public static Details Extract(IEnumerable<string> planXmls) =>
        ExtractCancellable(planXmls, CancellationToken.None);

    /// <summary>Cancellable form of <see cref="Extract(IEnumerable{string})"/>.</summary>
    public static Details ExtractCancellable(IEnumerable<string> planXmls, CancellationToken cancellationToken) =>
        ExtractCancellable(planXmls, config: null, cancellationToken);

    /// <summary>
    /// #4535: the config-aware form <see cref="SummarizeCancellable(IEnumerable{string}, AnalyzerConfig?, CancellationToken)"/>
    /// delegates to. Threads <paramref name="config"/> into <see cref="PlanAnalysisPipeline.Run(ParsedPlan, AnalyzerConfig?, ServerMetadata?, CancellationToken)"/>
    /// for every plan, so a disabled rule drops out of <c>plan.AllWarnings</c> before it ever
    /// reaches this aggregator, and an override is already applied to <c>PlanWarning.Severity</c>.
    /// </summary>
    public static Details ExtractCancellable(IEnumerable<string> planXmls, AnalyzerConfig? config, CancellationToken cancellationToken)
    {
        var byKey = new Dictionary<string, MissingIndex>(StringComparer.OrdinalIgnoreCase);
        var warnings = new List<PlanWarning>();

        foreach (var xml in planXmls)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(xml))
                continue;

            ParsedPlan plan;
            try
            {
                plan = ShowPlanParser.Parse(xml, cancellationToken);
                PlanAnalysisPipeline.Run(plan, config, serverMetadata: null, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                continue; // malformed / unsupported plan XML — skip, keep the rest
            }

            // #4551: a refused or exception-terminated plan carries whatever parsed before the
            // failure (partial statements/warnings). Skip it exactly like the catch above does,
            // so a partial parse never contributes partial counts to the aggregate.
            if (plan.ParseError != null)
                continue;

            foreach (var idx in plan.AllMissingIndexes)
            {
                var key = $"{idx.Schema}.{idx.Table}|{idx.CreateStatement}";
                if (!byKey.TryGetValue(key, out var existing) || idx.Impact > existing.Impact)
                    byKey[key] = idx;
            }

            warnings.AddRange(plan.AllWarnings);
        }

        return new Details(byKey.Values.ToList(), warnings);
    }
}
