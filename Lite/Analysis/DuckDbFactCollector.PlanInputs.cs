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

    /// <summary>
    /// #9195: why a candidate stays unverified. Every unverified candidate has exactly one, so the counts on the slow-check
    /// line add up to its unverified total. A candidate whose outcome is unverified takes the cause of that outcome. One
    /// whose plans compared conclusively and whose statement text is missing or withheld is <see cref="TextUnknown"/>.
    /// </summary>
    internal enum PlanUnverifiedCause
    {
        /// <summary>Not unverified.</summary>
        None = 0,

        /// <summary>The pass's fetch budget or its total fetch time ran out before the plan was tried.</summary>
        Unreached,

        /// <summary>Answered from a skip entry: an earlier failure or withheld compare, under an hour ago.</summary>
        Skipped,

        /// <summary>No plan source, or the detector's row has no plan id, so there was nothing to fetch.</summary>
        NoPlanToFetch,

        /// <summary>A plan fetch ended with no plan, a timeout or a fault, on either plan.</summary>
        FetchFailed,

        /// <summary>Both plans were fetched (or answered from an earlier pass) and the rule cannot judge them, with nothing withheld in either.</summary>
        Uncomparable,

        /// <summary>Both plans were fetched and the rule cannot judge them, and a plan carries text the statement filter withheld. The filter withholds on a time budget as well as on content, and the output cannot tell which.</summary>
        Withheld,

        /// <summary>The compare threw.</summary>
        CompareThrew,

        /// <summary>The plans compared conclusively, and the statement text is missing or withheld.</summary>
        TextUnknown,
    }

    private const int PlanUnverifiedCauseCount = 9;

    /// <summary>One pair's plan-based result: the outcome, and the cause when that outcome is unverified.</summary>
    private readonly record struct PairJudgement(PlanInputOutcome Outcome, PlanUnverifiedCause Cause);

    /// <summary>One bounded fetch's result: the plan (null when none), and whether the fetch was tried at all.</summary>
    private readonly record struct PlanFetch(string? Xml, bool Tried);

    /// <summary>The candidates that stay, in the detector's order, and the two counts the fact reports.</summary>
    private sealed record PlanRegressionVerification(
        List<PlanRegressionCandidate> Kept, int CrossInputExcluded, int InputsUnverified, int PlanFetches);

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

        /// <summary>#9195: pairs not judged because a plan fetch failed, or a compare was cut short by withheld text or threw, within the last hour.</summary>
        public int Skipped;
        public long FetchTicks;

        /// <summary>#9195: the longest fetch that returned a plan, in stopwatch ticks, and the longest plan fetched, in characters.</summary>
        public long SlowestPlanTicks;
        public int LargestPlanChars;
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
        var outcomes = new Dictionary<(string Database, long Latest, long Best), PairJudgement>();
        var causes = new int[PlanUnverifiedCauseCount];
        /* #9195: an interactive read has no per-pass budget. It answers from the outcomes an earlier pass cached, and for a pair with
           none it draws on the call's allowance (3 fetches, 3 s, shared by every context of the call, see FetchPlanForInputsAsync).
           A candidate the allowance does not reach stays unverified, the same as one a spent budget never reached. */
        var budget = new PlanFetchBudget { Left = context.InteractiveRead ? 0 : MaxPlanInputFetches };
        if (context.InteractiveRead) context.InteractiveFetches ??= new InteractivePlanFetchAllowance();
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
            if (judgements[i].Unverified) causes[(int)judgements[i].Cause]++;
        }

        var unverified = kept.Count(k => k.Unverified);
        ReportSlowPlanCheck(context, candidates.Count, budget, causes, unverified, Stopwatch.GetElapsedTime(started));
        return new PlanRegressionVerification(kept, excludedKeys.Count, unverified, budget.Fetches);
    }

    /// <summary>
    /// #9195: one Information line when the plan check took over <see cref="PlanInputSlowThreshold"/>, naming where the time
    /// went: the live fetches (how each ended, the slowest that returned a plan and the largest plan) or the plan pairs
    /// answered from an earlier pass, and why each unverified candidate stays unverified. A fast check logs nothing.
    /// Counts and times only, never a plan or a statement.
    /// </summary>
    private static void ReportSlowPlanCheck(
        AnalysisContext context, int candidateCount, PlanFetchBudget budget, int[] causes, int unverified, TimeSpan elapsed)
    {
        if (elapsed <= PlanInputSlowThreshold) return;

        AppLogger.Info("DuckDbFactCollector",
            string.Create(CultureInfo.InvariantCulture,
                $"Plan regression input check took {(long)elapsed.TotalMilliseconds} ms for {context.ServerName} (server {context.ServerId}): " +
                $"{candidateCount} candidates, {budget.Fetches} fetches ({budget.PlansRead} plans, {budget.NoPlan} none, {budget.Timeouts} timeouts, " +
                $"{budget.Faults} faults, {(long)Stopwatch.GetElapsedTime(0, budget.FetchTicks).TotalMilliseconds} ms, " +
                $"slowest plan fetch {(long)Stopwatch.GetElapsedTime(0, budget.SlowestPlanTicks).TotalMilliseconds} ms, largest plan {budget.LargestPlanChars} characters), " +
                $"{budget.CacheHits} answered from an earlier pass, {budget.Skipped} skipped after an earlier failure, {unverified} unverified " +
                $"({causes[(int)PlanUnverifiedCause.Unreached]} unreached, {causes[(int)PlanUnverifiedCause.Skipped]} skipped, " +
                $"{causes[(int)PlanUnverifiedCause.NoPlanToFetch]} without a plan to fetch, {causes[(int)PlanUnverifiedCause.FetchFailed]} fetch failed, " +
                $"{causes[(int)PlanUnverifiedCause.Uncomparable]} uncomparable, {causes[(int)PlanUnverifiedCause.Withheld]} withheld by the filter, " +
                $"{causes[(int)PlanUnverifiedCause.CompareThrew]} compare threw, {causes[(int)PlanUnverifiedCause.TextUnknown]} text unknown)"));
    }

    /// <summary>What the inputs check decided about one row: leave its query out, or keep it (checked or not) with its basis.</summary>
    private readonly record struct PlanInputJudgement(
        bool Exclude, bool Unverified, int Basis, PlanUnverifiedCause Cause = PlanUnverifiedCause.None);

    private async Task<PlanInputJudgement> JudgePlanRegressionCandidateAsync(
        AnalysisContext context, PlanRegressionCandidate candidate,
        Dictionary<(string Database, long Latest, long Best), PairJudgement> outcomes,
        PlanFetchBudget budget, CancellationToken overallToken)
    {
        /* Decided from the read alone: no plan is fetched for a recompile statement or for a plan that only returns more. */
        var outcome = PlanInputComparison.JudgeBeforePlans(
            candidate.QueryText, candidate.RegressionFactor, candidate.LatestRows, candidate.BestRows);

        var cause = PlanUnverifiedCause.None;
        if (outcome == PlanInputOutcome.NeedsPlans)
        {
            outcome = PlanInputOutcome.KeepUnverified;
            cause = PlanUnverifiedCause.NoPlanToFetch;
            if (_queryStorePlanSource is not null && candidate.LatestPlanId is long latestPlanId && candidate.BestPlanId is long bestPlanId)
            {
                /* Two replicas of one query can be two candidates over the same two plans: one comparison. */
                var key = (candidate.DatabaseName, latestPlanId, bestPlanId);
                if (!outcomes.TryGetValue(key, out var judged))
                {
                    /* #9195: a pair an earlier pass compared is answered from the cache and uses no fetch. A stored outcome is
                       conclusive, or the unverified answer of an uncomparable pair (see PlanInputOutcomeCache), so a miss is a
                       pair not compared, or one whose compare a withheld plan may have cut short. */
                    var cacheKey = new PlanInputOutcomeCache.Key(context.ServerId, candidate.DatabaseName, latestPlanId, bestPlanId);
                    if (_planInputCache.TryGet(cacheKey, out var cached))
                    {
                        budget.CacheHits++;
                        judged = new PairJudgement(
                            cached, cached == PlanInputOutcome.KeepUnverified ? PlanUnverifiedCause.Uncomparable : PlanUnverifiedCause.None);
                    }
                    else
                    {
                        judged = await JudgeFetchedPlansAsync(
                            context, budget, candidate.DatabaseName, latestPlanId, bestPlanId, overallToken);
                        _planInputCache.Store(cacheKey, judged.Outcome);
                    }

                    outcomes[key] = judged;
                }

                outcome = judged.Outcome;
                cause = judged.Cause;
            }
        }

        if (PlanInputComparison.IsDrop(outcome))
            return new PlanInputJudgement(Exclude: true, Unverified: false, Basis: 0);

        var textUnknown = string.IsNullOrWhiteSpace(candidate.QueryText)
            || WithheldStatementMarker.IsMarker(candidate.QueryText);
        var unverified = outcome == PlanInputOutcome.KeepUnverified || textUnknown;
        if (outcome != PlanInputOutcome.KeepUnverified) cause = textUnknown ? PlanUnverifiedCause.TextUnknown : PlanUnverifiedCause.None;
        return new PlanInputJudgement(Exclude: false, Unverified: unverified, Basis: PlanInputComparison.InputBasis(outcome), Cause: cause);
    }

    /// <summary>
    /// The plan-based outcome for one candidate's two plans; KeepUnverified when either cannot be had or compared, with the
    /// cause (#9195). A pair that compared KeepUnverified is stored for the outcome lifetime when neither fetched plan
    /// carries text the statement filter withheld: the same two stored plans through the same filter and rule give the same
    /// answer every time. The filter's withholding runs under a time budget (<c>SensitiveStatements.Xml</c> through
    /// <c>LivePlanDisplay.FilterAsync</c>: a spent budget or a match timeout withholds), and a withheld plan reads the same
    /// whatever withheld it, so such a pair keeps the one-hour skip and is judged again once it ends.
    /// </summary>
    private async Task<PairJudgement> JudgeFetchedPlansAsync(
        AnalysisContext context, PlanFetchBudget budget,
        string databaseName, long latestPlanId, long bestPlanId, CancellationToken overallToken)
    {
        /* #9195: a plan whose fetch failed in the last hour, or a pair whose compare a withheld plan cut short in it, is not
           tried again. That uses no fetch and no time, and the candidate stays unverified, as one the budget never reached does. */
        var pairKey = new PlanInputOutcomeCache.Key(context.ServerId, databaseName, latestPlanId, bestPlanId);
        if (_planInputCache.IsSkipped(pairKey)
            || _planInputCache.IsSkipped(new PlanInputOutcomeCache.PlanKey(context.ServerId, databaseName, latestPlanId))
            || _planInputCache.IsSkipped(new PlanInputOutcomeCache.PlanKey(context.ServerId, databaseName, bestPlanId)))
        {
            budget.Skipped++;
            AppLogger.Debug("DuckDbFactCollector", string.Create(CultureInfo.InvariantCulture,
                $"Plan input fetch skipped after an earlier failure for plans {latestPlanId} and {bestPlanId} in {databaseName} on {context.ServerName} (server {context.ServerId})"));
            return new PairJudgement(PlanInputOutcome.KeepUnverified, PlanUnverifiedCause.Skipped);
        }

        var latest = await FetchPlanForInputsAsync(context, budget, databaseName, latestPlanId, overallToken);
        if (latest.Xml is null) return new PairJudgement(PlanInputOutcome.KeepUnverified, FetchCause(latest));

        var best = await FetchPlanForInputsAsync(context, budget, databaseName, bestPlanId, overallToken);
        if (best.Xml is null) return new PairJudgement(PlanInputOutcome.KeepUnverified, FetchCause(best));

        try
        {
            var judged = PlanInputComparison.JudgePlans(latest.Xml, best.Xml);
            if (judged != PlanInputOutcome.KeepUnverified) return new PairJudgement(judged, PlanUnverifiedCause.None);

            if (CarriesWithheldText(latest.Xml) || CarriesWithheldText(best.Xml))
            {
                _planInputCache.Skip(pairKey);
                return new PairJudgement(judged, PlanUnverifiedCause.Withheld);
            }

            _planInputCache.StoreUncomparable(pairKey);
            return new PairJudgement(judged, PlanUnverifiedCause.Uncomparable);
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

            _planInputCache.Skip(pairKey);
            return new PairJudgement(PlanInputOutcome.KeepUnverified, PlanUnverifiedCause.CompareThrew);
        }
    }

    /// <summary>A fetch that returned no plan: unreached when it never started (the budget or the pass's time was spent), failed otherwise.</summary>
    private static PlanUnverifiedCause FetchCause(PlanFetch fetch) =>
        fetch.Tried ? PlanUnverifiedCause.FetchFailed : PlanUnverifiedCause.Unreached;

    /// <summary>True when the filtered plan holds the filter's withheld-text marker (a statement, a value or the whole plan).</summary>
    private static bool CarriesWithheldText(string planXml) =>
        planXml.Contains(WithheldStatementMarker.Text, StringComparison.Ordinal);

    /// <summary>
    /// One bounded fetch: no plan when the budget is spent, the source has no plan, the fetch fails, or the
    /// fetch or the pass's fetch time runs out. Only the pass's own cancellation escapes. The plan is never
    /// logged; the first fault of a pass (not a timeout) is logged once, through the collector's failure report, without
    /// marking the family unread.
    /// </summary>
    private async Task<PlanFetch> FetchPlanForInputsAsync(
        AnalysisContext context, PlanFetchBudget budget,
        string databaseName, long planId, CancellationToken overallToken)
    {
        var fetchLimit = PlanInputFetchTimeout;
        var allowance = context.InteractiveRead ? context.InteractiveFetches : null;
        if (overallToken.IsCancellationRequested) return new PlanFetch(null, Tried: false);
        if (context.InteractiveRead)
        {
            /* #9195: an interactive read's fetches come from the call's allowance, and none outlives what is left of its time. */
            if (allowance is null || !allowance.TryTake(out var remaining)) return new PlanFetch(null, Tried: false);
            if (remaining < fetchLimit) fetchLimit = remaining;
        }
        else if (budget.Left <= 0)
        {
            return new PlanFetch(null, Tried: false);
        }
        else
        {
            budget.Left--;
        }

        budget.Fetches++;

        using var one = CancellationTokenSource.CreateLinkedTokenSource(overallToken);
        one.CancelAfter(fetchLimit);

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
                _planInputCache.Skip(new PlanInputOutcomeCache.PlanKey(context.ServerId, databaseName, planId));
                return new PlanFetch(null, Tried: true);
            }

            result = "plan";
            chars = xml.Length;
            budget.PlansRead++;
            budget.SlowestPlanTicks = Math.Max(budget.SlowestPlanTicks, Stopwatch.GetTimestamp() - started);
            budget.LargestPlanChars = Math.Max(budget.LargestPlanChars, chars);
            return new PlanFetch(xml, Tried: true);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* One fetch's own timeout is an OperationCanceledException with the pass's token unfired: expected, so unverified
               and not logged as a fault. Anything else is a fault in the plan source. */
            var skipPlan = true;
            if (ex is OperationCanceledException)
            {
                result = "timeout";
                budget.Timeouts++;

                /* #9195: the pass's fetch time running out is the pass's limit, not this plan's: no skip entry. */
                skipPlan = !overallToken.IsCancellationRequested;

                /* #9195: an interactive call's allowance can cut a fetch short of PlanInputFetchTimeout. That is the call's limit too. */
                if (fetchLimit < PlanInputFetchTimeout) skipPlan = false;
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

            if (skipPlan) _planInputCache.Skip(new PlanInputOutcomeCache.PlanKey(context.ServerId, databaseName, planId));
            return new PlanFetch(null, Tried: true);
        }
        finally
        {
            var elapsedTicks = Stopwatch.GetTimestamp() - started;
            budget.FetchTicks += elapsedTicks;
            allowance?.RecordFetchTime(Stopwatch.GetElapsedTime(0, elapsedTicks));
            AppLogger.Debug("DuckDbFactCollector", string.Create(CultureInfo.InvariantCulture,
                $"Plan input fetch {result} in {(long)Stopwatch.GetElapsedTime(0, elapsedTicks).TotalMilliseconds} ms ({chars} characters) for plan {planId} in {databaseName} on {context.ServerName} (server {context.ServerId})"));
        }
    }
}
