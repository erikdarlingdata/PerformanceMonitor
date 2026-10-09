using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// #5630: the PLAN_REGRESSION detector must not compare two plans compiled for different inputs. A query with
/// OPTION (RECOMPILE) compiles a plan per call, and a parameter-sensitive query keeps one plan per compiled
/// parameter case, so "latest costs 2x the best" can be one input being bigger than another, and forcing the
/// "best" plan makes the big case slower. Darling's detector does the same checks over the plan XML it stores;
/// Lite stores none, so the two plans of each candidate are fetched live, in memory, for one comparison.
/// </summary>
public partial class DuckDbFactCollector
{
    /// <summary>The fact's cap on offenders (the SQL reads more, so a dropped row does not leave the fact short).</summary>
    internal const int MaxPlanRegressionOffenders = 20;

    /// <summary>Plan fetches one analysis pass may make. Past it the remaining candidates are unverified.</summary>
    internal const int MaxPlanInputFetches = 20;

    /// <summary>One fetch's time. A fetch that exceeds it is unverified, never an error.</summary>
    internal static readonly TimeSpan PlanInputFetchTimeout = TimeSpan.FromSeconds(5);

    /// <summary>All of a pass's fetches together. Past it the remaining candidates are unverified.</summary>
    internal static readonly TimeSpan PlanInputTotalTimeout = TimeSpan.FromSeconds(15);

    /// <summary>One row of the detector's read: a query whose latest plan costs 2x or more of its best plan.</summary>
    private sealed record PlanRegressionCandidate(
        long QueryId,
        double LatestCpu,
        int LatestIsForced,
        long ForceFailureCount,
        double BestCpu,
        double RegressionFactor,
        string DatabaseName,
        DateTime? BestLastExec,
        string? QueryText,
        long? LatestPlanId,
        long? BestPlanId);

    /// <summary>The candidates that stay, in the detector's order, and the two counts the fact reports.</summary>
    private sealed record PlanRegressionVerification(
        List<PlanRegressionCandidate> Kept, int CrossInputExcluded, int InputsUnverified);

    /// <summary>The fetches a pass has left, shared by every candidate of that pass.</summary>
    private sealed class PlanFetchBudget
    {
        public int Left = MaxPlanInputFetches;
    }

    /// <summary>
    /// Drops the candidates whose statement carries OPTION (RECOMPILE) or whose two plans were compiled for
    /// different inputs, and keeps the first <see cref="MaxPlanRegressionOffenders"/> of the rest. Only
    /// candidates over the 2x threshold that pass the text check are fetched, in the detector's order, within
    /// <see cref="MaxPlanInputFetches"/> fetches, <see cref="PlanInputFetchTimeout"/> each and
    /// <see cref="PlanInputTotalTimeout"/> together. A candidate whose plans cannot be fetched or compared
    /// stays, and is counted unverified. A fetch failure never fails the pass; the pass's own cancellation does
    /// propagate. Plan XML lives in locals of <see cref="CompareFetchedPlansAsync"/> and nowhere else.
    /// </summary>
    private async Task<PlanRegressionVerification> VerifyPlanRegressionInputsAsync(
        AnalysisContext context, List<PlanRegressionCandidate> candidates)
    {
        var kept = new List<PlanRegressionCandidate>();
        var excluded = 0;
        var unverified = 0;
        var verdicts = new Dictionary<(string Database, long Latest, long Best), PlanInputVerdict>();
        var budget = new PlanFetchBudget();

        using var overall = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        overall.CancelAfter(PlanInputTotalTimeout);

        foreach (var candidate in candidates)
        {
            if (kept.Count >= MaxPlanRegressionOffenders) break;
            context.CancellationToken.ThrowIfCancellationRequested();

            if (PlanInputComparison.HasRecompileHint(candidate.QueryText))
            {
                excluded++;
                continue;
            }

            var verdict = PlanInputVerdict.Unknown;
            if (_queryStorePlanSource is not null && candidate.LatestPlanId is long latestPlanId && candidate.BestPlanId is long bestPlanId)
            {
                /* Two replicas of one query are two candidates over the same two plans: one comparison. */
                var key = (candidate.DatabaseName, latestPlanId, bestPlanId);
                if (!verdicts.TryGetValue(key, out verdict))
                {
                    verdict = await CompareFetchedPlansAsync(
                        context, overall.Token, budget, candidate.DatabaseName, latestPlanId, bestPlanId);
                    verdicts[key] = verdict;
                }
            }

            if (verdict == PlanInputVerdict.Different)
            {
                excluded++;
                continue;
            }

            if (verdict == PlanInputVerdict.Unknown) unverified++;
            kept.Add(candidate);
        }

        return new PlanRegressionVerification(kept, excluded, unverified);
    }

    /// <summary>The verdict for one candidate's two plans; Unknown when either cannot be had or compared.</summary>
    private async Task<PlanInputVerdict> CompareFetchedPlansAsync(
        AnalysisContext context, CancellationToken overallToken, PlanFetchBudget budget,
        string databaseName, long latestPlanId, long bestPlanId)
    {
        var latestXml = await FetchPlanForInputsAsync(context, overallToken, budget, databaseName, latestPlanId);
        if (latestXml is null) return PlanInputVerdict.Unknown;

        var bestXml = await FetchPlanForInputsAsync(context, overallToken, budget, databaseName, bestPlanId);
        if (bestXml is null) return PlanInputVerdict.Unknown;

        try
        {
            return PlanInputComparison.Compare(latestXml, bestXml);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return PlanInputVerdict.Unknown;
        }
    }

    /// <summary>
    /// One bounded fetch: null when the budget is spent, the source has no plan, the fetch fails, or the
    /// fetch or the pass's fetch time runs out. Only the pass's own cancellation escapes. Nothing about the
    /// plan or the failure is logged.
    /// </summary>
    private async Task<string?> FetchPlanForInputsAsync(
        AnalysisContext context, CancellationToken overallToken, PlanFetchBudget budget,
        string databaseName, long planId)
    {
        if (budget.Left <= 0 || overallToken.IsCancellationRequested) return null;
        budget.Left--;

        using var one = CancellationTokenSource.CreateLinkedTokenSource(overallToken);
        one.CancelAfter(PlanInputFetchTimeout);
        try
        {
            /* WaitAsync as well as the token: a source that ignores its token still cannot hold the pass. */
            var xml = await _queryStorePlanSource!
                .FetchQueryStorePlanXmlAsync(context.ServerId, databaseName, planId, one.Token)
                .WaitAsync(one.Token);
            return string.IsNullOrWhiteSpace(xml) ? null : xml;
        }
        catch (OperationCanceledException) when (!context.CancellationToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }
}
