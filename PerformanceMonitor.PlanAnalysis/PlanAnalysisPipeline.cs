/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// #4546: the one place a parsed plan is analyzed and scored. Every entry point that used to call
/// <see cref="PlanAnalyzer.Analyze"/> directly calls <see cref="Run"/> instead, so
/// <see cref="BenefitScorer.Score"/> always runs right after the analyzer — before this, the scorer
/// existed but nothing ever called it, so <c>PlanWarning.MaxBenefitPercent</c> and
/// <c>PlanStatement.WaitBenefits</c> were always empty everywhere. Skips a plan with no statements
/// (nothing for either pass to do), same as PerformanceStudio's <c>PlanAnalysisPipeline</c>. Also
/// skips a plan that failed to parse (#4551): a refused or exception-terminated parse leaves
/// <see cref="ParsedPlan.ParseError"/> set, and whatever content parsed before the failure is
/// partial, so neither pass runs against it.
/// </summary>
public static class PlanAnalysisPipeline
{
    public static ParsedPlan Run(ParsedPlan plan) =>
        Run(plan, CancellationToken.None);

    /// <summary>
    /// #4512: the token flows into the analyzer and the scorer, with a check between each stage
    /// and inside their own per-statement walks, so a caller cancelling mid-run stops the
    /// pipeline before the next stage rather than finishing analysis and scoring on a plan
    /// nobody is waiting for.
    /// </summary>
    public static ParsedPlan Run(ParsedPlan plan, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!string.IsNullOrWhiteSpace(plan.ParseError))
            return plan;

        if (!plan.Batches.SelectMany(batch => batch.Statements).Any())
            return plan;

        PlanAnalyzer.Analyze(plan, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        BenefitScorer.Score(plan, cancellationToken);
        return plan;
    }

    /// <summary>
    /// Parses <paramref name="xml"/> on the async path (<see cref="ShowPlanParser.ParseAsync"/>,
    /// which enforces <see cref="ShowPlanParser.MaxParseCharacters"/> via the reader rather than
    /// a string-length check), then runs the same analyze/score pipeline as <see cref="Run"/>.
    /// </summary>
    public static async Task<ParsedPlan> RunAsync(string xml, CancellationToken cancellationToken)
    {
        var plan = await ShowPlanParser.ParseAsync(xml, cancellationToken).ConfigureAwait(false);
        return Run(plan, cancellationToken);
    }
}
