using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// #5630/#5631: "the latest plan costs 2x the best" is not always a plan that got worse. A query with
/// OPTION (RECOMPILE) compiles a plan per call, a newer plan can simply return far more rows, and a
/// parameter-sensitive query keeps one plan per compiled parameter case; forcing the "best" plan in those cases makes
/// the big case slower. The shared rule (<see cref="PlanInputComparison"/>) decides from the text and the rows per
/// call first, then from the two plans' compiled values and statistics. Darling's detector applies it to the plan XML
/// it stores; Lite stores none, so the two plans of each candidate the first step leaves are fetched live, in
/// memory, for one comparison.
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

    /// <summary>A plan check that takes longer than this logs one Information line with its split (#9195).</summary>
    internal static readonly TimeSpan PlanInputSlowThreshold = TimeSpan.FromSeconds(1);

    /// <summary>One row of the detector's read: a query whose latest plan costs 2x or more of its best plan.</summary>
    internal sealed record PlanRegressionCandidate(
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
        double? LatestRows = null,
        double? BestRows = null,
        bool Unverified = false,
        int InputBasis = 0);

    /// <summary>The candidates that stay, in the detector's order, and the two counts the fact reports.</summary>
    private sealed record PlanRegressionVerification(
        List<PlanRegressionCandidate> Kept, int CrossInputExcluded, int InputsUnverified);

    /// <summary>The fetches a pass has left, shared by every candidate of that pass.</summary>
    private sealed class PlanFetchBudget
    {
        public int Left = MaxPlanInputFetches;

        /// <summary>True once the pass has logged a plan-input fault, so a broken source is one event, not one per fetch.</summary>
        public bool FaultReported;

        /* #9195: what the pass did, for the one line a slow check logs and for nothing else. */
        public int Fetches;
        public int PlansRead;
        public int NoPlan;
        public int Timeouts;
        public int Faults;
        public int CacheHits;
        public long FetchTicks;
    }

    /// <summary>
    /// #5631: judges each candidate by the shared rule (<see cref="PlanInputComparison"/>) and keeps the first
    /// <see cref="MaxPlanRegressionOffenders"/> of what stays. A candidate is dropped when its statement carries
    /// OPTION (RECOMPILE), when the newer plan returns far more rows per call and its cost per row did not regress
    /// (both decided from the read alone, with no plan fetched), or when its two plans differ only in compiled values
    /// over the same statistics. Only the candidates the first two checks leave are fetched, in the detector's
    /// order, within <see cref="MaxPlanInputFetches"/> fetches, <see cref="PlanInputFetchTimeout"/> each and
    /// <see cref="PlanInputTotalTimeout"/> together. A candidate whose plans cannot be fetched or compared stays, and
    /// is counted unverified; so does one whose plans compare without a drop while its statement text is missing or
    /// withheld, because the OPTION (RECOMPILE) check could not run on it. A fetch failure never fails the pass; the
    /// pass's own cancellation does propagate. Plan XML lives in locals of <see cref="JudgeFetchedPlansAsync"/> and
    /// nowhere else.
    ///
    /// <para>The drill-down and the force targets name a query by (database, query_id), not by replica, so a query
    /// that any replica's row excludes is out for every replica. A query is therefore judged whole, every replica
    /// row of it, the first time its key comes up: the exclusion is known before any row of it is kept, and the cut
    /// to 20 counts only rows that stay. The excluded count is distinct queries, whatever the reason.</para>
    /// </summary>
    private async Task<PlanRegressionVerification> VerifyPlanRegressionInputsAsync(
        AnalysisContext context, List<PlanRegressionCandidate> candidates)
    {
        var kept = new List<PlanRegressionCandidate>();
        var excludedKeys = new HashSet<(string Database, long QueryId)>();
        var judgedKeys = new HashSet<(string Database, long QueryId)>();
        var judgements = new Dictionary<int, PlanInputJudgement>();
        var outcomes = new Dictionary<(string Database, long Latest, long Best), PlanInputOutcome>();
        var budget = new PlanFetchBudget();
        var started = Stopwatch.GetTimestamp();

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

                    var judgement = await JudgePlanRegressionCandidateAsync(context, candidates[j], outcomes, budget, overall.Token);
                    if (judgement.Exclude)
                    {
                        excludedKeys.Add(queryKey);
                        break;
                    }

                    judgements[j] = judgement;
                }
            }

            if (excludedKeys.Contains(queryKey)) continue;
            kept.Add(candidate with { Unverified = judgements[i].Unverified, InputBasis = judgements[i].Basis });
        }

        var unverified = kept.Count(k => k.Unverified);
        ReportSlowPlanCheck(context, candidates.Count, budget, unverified, Stopwatch.GetElapsedTime(started));
        return new PlanRegressionVerification(kept, excludedKeys.Count, unverified);
    }

    /// <summary>
    /// #9195: one Information line when the plan check took over <see cref="PlanInputSlowThreshold"/>, naming where the time
    /// went: the live fetches (and how each ended) or the plan pairs answered from an earlier pass. A fast check logs
    /// nothing. Counts and times only, never a plan or a statement.
    /// </summary>
    private static void ReportSlowPlanCheck(
        AnalysisContext context, int candidateCount, PlanFetchBudget budget, int unverified, TimeSpan elapsed)
    {
        if (elapsed <= PlanInputSlowThreshold) return;

        AppLogger.Info("DuckDbFactCollector",
            string.Create(CultureInfo.InvariantCulture,
                $"Plan regression input check took {(long)elapsed.TotalMilliseconds} ms for {context.ServerName} (server {context.ServerId}): " +
                $"{candidateCount} candidates, {budget.Fetches} fetches ({budget.PlansRead} plans, {budget.NoPlan} none, {budget.Timeouts} timeouts, " +
                $"{budget.Faults} faults, {(long)Stopwatch.GetElapsedTime(0, budget.FetchTicks).TotalMilliseconds} ms), {budget.CacheHits} answered from an earlier pass, {unverified} unverified"));
    }

    /// <summary>What the inputs check decided about one row: leave its query out, or keep it (checked or not) with its basis.</summary>
    private readonly record struct PlanInputJudgement(bool Exclude, bool Unverified, int Basis);

    private async Task<PlanInputJudgement> JudgePlanRegressionCandidateAsync(
        AnalysisContext context, PlanRegressionCandidate candidate,
        Dictionary<(string Database, long Latest, long Best), PlanInputOutcome> outcomes,
        PlanFetchBudget budget, CancellationToken overallToken)
    {
        /* Decided from the read alone: no plan is fetched for a recompile statement or for a plan that only returns more. */
        var outcome = PlanInputComparison.JudgeBeforePlans(
            candidate.QueryText, candidate.RegressionFactor, candidate.LatestRows, candidate.BestRows);

        if (outcome == PlanInputOutcome.NeedsPlans)
        {
            outcome = PlanInputOutcome.KeepUnverified;
            if (_queryStorePlanSource is not null && candidate.LatestPlanId is long latestPlanId && candidate.BestPlanId is long bestPlanId)
            {
                /* Two replicas of one query can be two candidates over the same two plans: one comparison. */
                var key = (candidate.DatabaseName, latestPlanId, bestPlanId);
                if (!outcomes.TryGetValue(key, out outcome))
                {
                    /* #9195: a pair an earlier pass compared is answered from the cache and uses no fetch. Only a
                       conclusive outcome is ever stored (see PlanInputOutcomeCache), so a miss is a pair never compared. */
                    var cacheKey = new PlanInputOutcomeCache.Key(context.ServerId, candidate.DatabaseName, latestPlanId, bestPlanId);
                    if (_planInputCache.TryGet(cacheKey, out outcome))
                    {
                        budget.CacheHits++;
                    }
                    else
                    {
                        outcome = await JudgeFetchedPlansAsync(
                            context, budget, candidate.DatabaseName, latestPlanId, bestPlanId, overallToken);
                        _planInputCache.Store(cacheKey, outcome);
                    }

                    outcomes[key] = outcome;
                }
            }
        }

        if (PlanInputComparison.IsDrop(outcome))
            return new PlanInputJudgement(Exclude: true, Unverified: false, Basis: 0);

        var textUnknown = string.IsNullOrWhiteSpace(candidate.QueryText)
            || WithheldStatementMarker.IsMarker(candidate.QueryText);
        return new PlanInputJudgement(
            Exclude: false,
            Unverified: outcome == PlanInputOutcome.KeepUnverified || textUnknown,
            Basis: PlanInputComparison.InputBasis(outcome));
    }

    /// <summary>The plan-based outcome for one candidate's two plans; KeepUnverified when either cannot be had or compared.</summary>
    private async Task<PlanInputOutcome> JudgeFetchedPlansAsync(
        AnalysisContext context, PlanFetchBudget budget,
        string databaseName, long latestPlanId, long bestPlanId, CancellationToken overallToken)
    {
        var latestXml = await FetchPlanForInputsAsync(context, budget, databaseName, latestPlanId, overallToken);
        if (latestXml is null) return PlanInputOutcome.KeepUnverified;

        var bestXml = await FetchPlanForInputsAsync(context, budget, databaseName, bestPlanId, overallToken);
        if (bestXml is null) return PlanInputOutcome.KeepUnverified;

        try
        {
            return PlanInputComparison.JudgePlans(latestXml, bestXml);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Only the exception's type goes to the log: the plans are customer text, and a parser's message can quote them. */
            if (!budget.FaultReported)
            {
                budget.FaultReported = true;
                ReportCollectionFailure(
                    new InvalidOperationException($"The two plans could not be compared ({ex.GetType().Name})."),
                    context, nameof(CollectPlanRegressionFactsAsync), record: false);
            }

            return PlanInputOutcome.KeepUnverified;
        }
    }

    /// <summary>
    /// One bounded fetch: null when the budget is spent, the source has no plan, the fetch fails, or the
    /// fetch or the pass's fetch time runs out. Only the pass's own cancellation escapes. The plan is never
    /// logged; the first fault of a pass (not a timeout) is logged once, through the collector's failure report, without
    /// marking the family unread.
    /// </summary>
    private async Task<string?> FetchPlanForInputsAsync(
        AnalysisContext context, PlanFetchBudget budget,
        string databaseName, long planId, CancellationToken overallToken)
    {
        if (budget.Left <= 0 || overallToken.IsCancellationRequested) return null;
        budget.Left--;
        budget.Fetches++;

        using var one = CancellationTokenSource.CreateLinkedTokenSource(overallToken);
        one.CancelAfter(PlanInputFetchTimeout);

        /* #9195: each fetch's time and how it ended goes to the Debug log with the plan's size in characters, never its text. */
        var started = Stopwatch.GetTimestamp();
        var result = "cancelled";
        var chars = 0;
        try
        {
            /* WaitAsync as well as the token: a source that ignores its token still cannot hold the pass. */
            var xml = await _queryStorePlanSource!
                .FetchQueryStorePlanXmlAsync(context.ServerId, databaseName, planId, one.Token)
                .WaitAsync(one.Token);
            if (string.IsNullOrWhiteSpace(xml))
            {
                result = "none";
                budget.NoPlan++;
                return null;
            }

            result = "plan";
            chars = xml.Length;
            budget.PlansRead++;
            return xml;
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* One fetch's own timeout is an OperationCanceledException with the pass's token unfired: expected, so unverified
               and not logged as a fault. Anything else is a fault in the plan source. */
            if (ex is OperationCanceledException)
            {
                result = "timeout";
                budget.Timeouts++;
            }
            else
            {
                result = "fault";
                budget.Faults++;
                if (!budget.FaultReported)
                {
                    budget.FaultReported = true;
                    ReportCollectionFailure(ex, context, nameof(CollectPlanRegressionFactsAsync), record: false);
                }
            }

            return null;
        }
        finally
        {
            var elapsedTicks = Stopwatch.GetTimestamp() - started;
            budget.FetchTicks += elapsedTicks;
            AppLogger.Debug("DuckDbFactCollector", string.Create(CultureInfo.InvariantCulture,
                $"Plan input fetch {result} in {(long)Stopwatch.GetElapsedTime(0, elapsedTicks).TotalMilliseconds} ms ({chars} characters) for plan {planId} in {databaseName} on {context.ServerName} (server {context.ServerId})"));
        }
    }
}
