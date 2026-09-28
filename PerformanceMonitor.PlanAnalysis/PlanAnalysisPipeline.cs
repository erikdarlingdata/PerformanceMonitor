/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// #4546: the one place a parsed plan is analyzed and scored. Every entry point that used to call
/// <see cref="PlanAnalyzer.Analyze"/> directly calls <see cref="Run"/> instead, so
/// <see cref="BenefitScorer.Score"/> always runs right after the analyzer — before this, the scorer
/// existed but nothing ever called it, so <c>PlanWarning.MaxBenefitPercent</c> and
/// <c>PlanStatement.WaitBenefits</c> were always empty everywhere. Skips a plan with no statements
/// (nothing for either pass to do), same as PerformanceStudio's <c>PlanAnalysisPipeline</c>.
/// </summary>
public static class PlanAnalysisPipeline
{
    public static ParsedPlan Run(ParsedPlan plan)
    {
        if (!plan.Batches.SelectMany(batch => batch.Statements).Any())
            return plan;

        PlanAnalyzer.Analyze(plan);
        BenefitScorer.Score(plan);
        return plan;
    }
}
