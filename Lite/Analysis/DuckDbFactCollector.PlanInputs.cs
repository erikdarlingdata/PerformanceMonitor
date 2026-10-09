using System;
using System.Collections.Generic;
using System.Linq;
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
        long? BestPlanId,
        bool Unverified = false);

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
    /// stays, and is counted unverified; so does one whose plans compare the same while its statement text is
    /// missing or withheld, because the OPTION (RECOMPILE) check could not run on it. A fetch failure never fails
    /// the pass; the pass's own cancellation does propagate. Plan XML lives in locals of
    /// <see cref="CompareFetchedPlansAsync"/> and nowhere else.
    ///
    /// <para>The drill-down and the force targets name a query by (database, query_id), not by replica, so a query
    /// that any replica's row excludes is out for every replica. A query is therefore judged whole, every replica
    /// row of it, the first time its key comes up: the exclusion is known before any row of it is kept, and the cut
    /// to 20 counts only rows that stay. The excluded count is distinct queries.</para>
    /// </summary>
    private async Task<PlanRegressionVerification> VerifyPlanRegressionInputsAsync(
        AnalysisContext context, List<PlanRegressionCandidate> candidates)
    {
        var kept = new List<PlanRegressionCandidate>();
        var excludedKeys = new HashSet<(string Database, long QueryId)>();
        var judgedKeys = new HashSet<(string Database, long QueryId)>();
        var judgements = new Dictionary<int, bool>();
        var verdicts = new Dictionary<(string Database, long Latest, long Best), PlanInputVerdict>();
        var budget = new PlanFetchBudget();

        using var overall = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        overall.CancelAfter(PlanInputTotalTimeout);

        for (var i = 0; i < candidates.Count; i++)
        {
            if (kept.Count >= MaxPlanRegressionOffenders) break;
            context.CancellationToken.ThrowIfCancellationRequested();

            var candidate = candidates[i];
            var queryKey = (candidate.DatabaseName, candidate.QueryId);
            if (judgedKeys.Add(queryKey))
            {
                for (var j = i; j < candidates.Count; j++)
                {
                    if (candidates[j].DatabaseName != queryKey.DatabaseName || candidates[j].QueryId != queryKey.QueryId) continue;

                    var judgement = await JudgePlanRegressionCandidateAsync(context, candidates[j], verdicts, budget, overall.Token);
                    if (judgement.Exclude)
                    {
                        excludedKeys.Add(queryKey);
                        break;
                    }

                    judgements[j] = judgement.Unverified;
                }
            }

            if (excludedKeys.Contains(queryKey)) continue;
            kept.Add(candidate with { Unverified = judgements[i] });
        }

        return new PlanRegressionVerification(kept, excludedKeys.Count, kept.Count(k => k.Unverified));
    }

    /// <summary>What the inputs check decided about one row: leave its query out, or keep it (checked or not).</summary>
    private readonly record struct PlanInputJudgement(bool Exclude, bool Unverified);

    private async Task<PlanInputJudgement> JudgePlanRegressionCandidateAsync(
        AnalysisContext context, PlanRegressionCandidate candidate,
        Dictionary<(string Database, long Latest, long Best), PlanInputVerdict> verdicts,
        PlanFetchBudget budget, CancellationToken overallToken)
    {
        if (PlanInputComparison.HasRecompileHint(candidate.QueryText))
            return new PlanInputJudgement(Exclude: true, Unverified: false);

        var verdict = PlanInputVerdict.Unknown;
        if (_queryStorePlanSource is not null && candidate.LatestPlanId is long latestPlanId && candidate.BestPlanId is long bestPlanId)
        {
            /* Two replicas of one query can be two candidates over the same two plans: one comparison. */
            var key = (candidate.DatabaseName, latestPlanId, bestPlanId);
            if (!verdicts.TryGetValue(key, out verdict))
            {
                verdict = await CompareFetchedPlansAsync(
                    context, budget, candidate.DatabaseName, latestPlanId, bestPlanId, overallToken);
                verdicts[key] = verdict;
            }
        }

        if (verdict == PlanInputVerdict.Different)
            return new PlanInputJudgement(Exclude: true, Unverified: false);

        var textUnknown = string.IsNullOrWhiteSpace(candidate.QueryText)
            || WithheldStatementMarker.IsMarker(candidate.QueryText);
        return new PlanInputJudgement(Exclude: false, Unverified: verdict == PlanInputVerdict.Unknown || textUnknown);
    }

    /// <summary>The verdict for one candidate's two plans; Unknown when either cannot be had or compared.</summary>
    private async Task<PlanInputVerdict> CompareFetchedPlansAsync(
        AnalysisContext context, PlanFetchBudget budget,
        string databaseName, long latestPlanId, long bestPlanId, CancellationToken overallToken)
    {
        var latestXml = await FetchPlanForInputsAsync(context, budget, databaseName, latestPlanId, overallToken);
        if (latestXml is null) return PlanInputVerdict.Unknown;

        var bestXml = await FetchPlanForInputsAsync(context, budget, databaseName, bestPlanId, overallToken);
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
        AnalysisContext context, PlanFetchBudget budget,
        string databaseName, long planId, CancellationToken overallToken)
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
