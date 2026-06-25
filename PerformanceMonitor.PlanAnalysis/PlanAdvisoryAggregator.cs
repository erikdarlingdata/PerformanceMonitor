using System;
using System.Collections.Generic;
using System.Linq;

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
    public static Summary Summarize(IEnumerable<string> planXmls)
    {
        var details = Extract(planXmls);
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
    public static Details Extract(IEnumerable<string> planXmls)
    {
        var byKey = new Dictionary<string, MissingIndex>(StringComparer.OrdinalIgnoreCase);
        var warnings = new List<PlanWarning>();

        foreach (var xml in planXmls)
        {
            if (string.IsNullOrWhiteSpace(xml))
                continue;

            ParsedPlan plan;
            try
            {
                plan = ShowPlanParser.Parse(xml);
                PlanAnalyzer.Analyze(plan);
            }
            catch
            {
                continue; // malformed / unsupported plan XML — skip, keep the rest
            }

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
