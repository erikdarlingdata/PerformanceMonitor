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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3817: the store-object convergence pass runs on the hourly store-maintenance tick as well as at startup,
/// from ONE list, so a step that failed once heals on the next pass instead of at the next restart.
///
/// <para><b>The lie this file pins against.</b> Ten ensures — hypertable conversion, compression policies,
/// the collection_log conversion, the compression-schedule converge, the aggregate reshape and refresh
/// converge, the aggregate ensure, the dedup-index sweep, the aggregate-compression ensure, the retired and
/// fallback baseline steps and the composer tuning — each had exactly one non-test call site, all on the
/// service start path, and each was failure-isolated per item. That isolation is the right posture and is
/// precisely what made a single failure invisible and permanent: one missing continuous aggregate is a rollup
/// family gone, one missing baseline relation is an anomaly family silently returning an empty baseline
/// (the provider reads them by name and swallows the 42P01), an un-applied tuning pass is every composer
/// query back on un-indexed scans. The code already knew — the ungated baseline-fallback block exists
/// BECAUSE a startup step can silently leave one relation unbuilt, and it was itself a startup step that can
/// silently leave one relation unbuilt.</para>
///
/// <para><b>Why the pins are source-parsed.</b> The tick is a private block on a loop that needs a host, a
/// store and a clock to drive, and the list is private static state; what regresses is a call site or a list
/// entry written the wrong way, which is textual. The one behavioural property that CAN be driven without a
/// host — that a step which throws is retried on the next pass and counted — is driven, on the list's own
/// step shape, in <see cref="StoreObjectConvergenceStepBehaviourTests"/>.</para>
/// </summary>
public sealed class StoreObjectConvergenceTests
{
    private const string ListDeclaration = "private static readonly StoreObjectConvergenceStep[] s_storeObjectConvergence =";
    private const string HourlyCall = "await ConvergeStoreObjectsAsync(stoppingToken);";
    private const string HourlySignature = "private async Task ConvergeStoreObjectsAsync(CancellationToken cancellationToken)";
    private const string SegmentSignature = "private async Task RunStoreObjectConvergenceSegmentAsync(";
    private const string StepSignature = "internal static async Task RunStoreObjectConvergenceStepAsync(";
    private const string SummarySignature = "private void LogStoreObjectConvergence(StoreObjectConvergenceTally tally, long elapsedMs, bool startup)";

    /// <summary>
    /// THE pin the issue asks for: the hourly pass and the start path run the SAME ensure list, and they do it
    /// by iterating one shared list rather than by carrying two sequences that agree today.
    ///
    /// <para>Asserted structurally rather than by comparing two censuses, because "the two lists have the same
    /// members" is the property that drifts — it was true of the retention sweep and its startup twin until it
    /// was not. There is one list, the hourly pass walks ALL of it with no stage filter, and the start path
    /// walks it in stage slices whose union is the whole list; so a step added to the list is added to both
    /// cadences by construction, and a step that could be reached from only one of them cannot exist.</para>
    /// </summary>
    [Fact]
    public void TheHourlyPassAndTheStartPath_WalkTheSameOneList()
    {
        var worker = Worker();
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);

        /* One list, declared once. */
        Assert.Equal(1, CountOf(code, ListDeclaration));

        /* The hourly pass iterates the WHOLE list — no stage filter, no second sequence. */
        var hourly = MethodBody(code, HourlySignature);
        Assert.False(string.IsNullOrEmpty(hourly), "could not locate ConvergeStoreObjectsAsync — this pin cannot silently pass on a parse miss");
        Assert.Contains("foreach (var step in s_storeObjectConvergence)", hourly, StringComparison.Ordinal);
        Assert.DoesNotContain("Stage", hourly, StringComparison.Ordinal);

        /* The start path reaches the list ONLY through the segment walk, and the segment walk is the only
           other iteration of it. Two iterations in the product: the hourly one above and the segment one. */
        Assert.Equal(2, CountOf(code, "foreach (var step in s_storeObjectConvergence)"));
        var segment = MethodBody(code, SegmentSignature);
        Assert.False(string.IsNullOrEmpty(segment));
        Assert.Contains("foreach (var step in s_storeObjectConvergence)", segment, StringComparison.Ordinal);
        Assert.Contains("if (step.Stage != stage)", segment, StringComparison.Ordinal);

        /* Every stage the list uses is walked by the start path, and every stage the start path walks is one
           the list uses — the union property, measured both directions off the source rather than asserted.
           A step tagged with a stage no segment call names would be hourly-only, which is the exact asymmetry
           this issue exists to remove (in the other direction). */
        var declaredStages = new HashSet<string>(
            Regex.Matches(ListBody(code), @"StoreObjectConvergenceStage\.(\w+),").Select(m => m.Groups[1].Value),
            StringComparer.Ordinal);
        var walkedStages = new HashSet<string>(
            Regex.Matches(code, @"RunStoreObjectConvergenceSegmentAsync\(\s*\w+,\s*StoreObjectConvergenceStage\.(\w+),")
                .Select(m => m.Groups[1].Value),
            StringComparer.Ordinal);
        Assert.NotEmpty(declaredStages);
        Assert.Equal(declaredStages.OrderBy(s => s, StringComparer.Ordinal), walkedStages.OrderBy(s => s, StringComparer.Ordinal));

        /* And each stage is walked EXACTLY once on the start path: a stage walked twice runs its steps twice
           per start, which is the shape a copy-paste of a segment call produces. */
        foreach (var stage in declaredStages)
        {
            Assert.Equal(1, CountOf(code, $"StoreObjectConvergenceStage.{stage}, startupConvergence, stoppingToken);"));
        }
    }

    /// <summary>
    /// The list's ORDER is the start path's historical order, and the four constraints inside it that are
    /// measured rather than preferred are asserted as order relations — because the extraction's whole risk is
    /// that a list is easier to reorder than eleven commented await lines were.
    ///
    /// <para>Each relation carries its own issue: the compression-schedule converge must follow both
    /// compression steps (#1778 — it must cover collection_log in the same pass); the stale reshape must
    /// precede the aggregate ensure (it drops what the ensure rebuilds); the refresh converge must precede the
    /// aggregate ensure (#3012 — add_continuous_aggregate_policy raises 22023 against a policy whose window
    /// differs instead of skipping, so the ensure fails per-aggregate on every already-deployed store if it
    /// runs first); and the dedup-index and aggregate-compression steps must follow the aggregate ensure
    /// (#3597, #3581 — the aggregates have to exist).</para>
    /// </summary>
    [Fact]
    public void TheListOrder_KeepsTheFourMeasuredOrderingConstraints()
    {
        var list = ListBody(CSharpSourceWalker.StripCommentsAndStrings(Worker()));

        int At(string method)
        {
            var at = list.IndexOf(method, StringComparison.Ordinal);
            Assert.True(at > 0, $"{method} is no longer in the convergence list — if it was deliberately removed, this pin is where the reason belongs");
            return at;
        }

        var convert = At("ConvertToHypertablesAsync");
        var compression = At("ApplyCompressionPolicyAsync");
        var collectionLog = At("EnsureCollectionLogHypertableAsync");
        var scheduleConverge = At("ConvergeCompressionScheduleAsync");
        var reshape = At("DropStaleContinuousAggregatesAsync");
        var refreshConverge = At("ConvergeContinuousAggregateRefreshAsync");
        var aggregates = At("EnsureContinuousAggregatesAsync");
        var dedupIndexes = At("EnsureIntervalDedupMaterializationIndexesAsync");
        var aggregateCompression = At("EnsureAggregateCompressionAsync");
        var retiredBaselines = At("DropRetiredBaselineAggregatesAsync");
        var fallbackViews = At("EnsureBaselineFallbackViewsAsync");
        var tuning = At("PgTableTuning.ApplyAsync");

        Assert.True(convert < compression, "hypertables before their compression policies");
        Assert.True(compression < scheduleConverge && collectionLog < scheduleConverge,
            "#1778: the compression-schedule converge follows BOTH compression steps, so it covers collection_log in the same pass");
        Assert.True(reshape < aggregates, "the stale-shape drop precedes the ensure that rebuilds them");
        Assert.True(refreshConverge < aggregates,
            "#3012: the refresh converge precedes the aggregate ensure, or the ensure raises 22023 per aggregate on every already-deployed store");
        Assert.True(aggregates < dedupIndexes, "#3597: the dedup-index sweep needs the aggregates to exist");
        Assert.True(aggregates < aggregateCompression, "#3581: the aggregate-compression ensure needs the aggregates to exist");
        Assert.True(retiredBaselines < fallbackViews, "#2007: the retirement drop precedes the fallback ensure");
        Assert.True(fallbackViews < tuning, "the tuning pass stays last, as it was on the start path");
    }

    /// <summary>
    /// The five things deliberately left start-path-only are NOT in the list, each for its own reason, and the
    /// reasons are on the list. Pinned as absences because an absence is what a later edit undoes by adding one
    /// line, and every one of these five would be actively wrong on an hourly cadence: the retention sweep
    /// already has its own tenant (#3812) and would run twice an hour; the superseded-coverage log is an
    /// instrument that writes a line per pair, which is a wall of no-op lines hourly; the baseline backfill and
    /// the hole repair are bulk materializations the start path deliberately does not even await; and the
    /// module_map refresh is a data upsert that already rides the daily purge.
    /// </summary>
    [Fact]
    public void TheFiveStartPathOnlySteps_AreNotInTheList()
    {
        var list = ListBody(CSharpSourceWalker.StripCommentsAndStrings(Worker()));

        foreach (var excluded in new[]
        {
            "EnsureRetentionPoliciesAsync",
            "LogSupersededHourlyRollupCoverageAsync",
            "RunBaselineBackfillAsync",
            "BackfillBaselineAggregatesAsync",
            "RunMaterializationHoleRepairAsync",
            "RepairMaterializationHolesAsync",
            "DarlingModuleMap",
        })
        {
            Assert.DoesNotContain(excluded, list, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The hourly method: its own pooled connection, ONE whole-pass budget (#2327's shape, for #2327's reason —
    /// the pass is awaited on the serial sweep loop and every statement inside it carries TimescaleSupport's
    /// 300 s bulk-setup deadline), three failure outcomes that read differently, and no rethrow. The same
    /// contract the tick's other three tenants sign, asserted the same way.
    /// </summary>
    [Fact]
    public void TheHourlyMethod_TakesItsOwnConnection_UnderOneBudget_FailureIsolated()
    {
        var worker = Worker();
        var body = MethodBody(worker, HourlySignature);
        Assert.False(string.IsNullOrEmpty(body), "could not locate ConvergeStoreObjectsAsync — this pin cannot silently pass on a parse miss");

        /* Its own connection out of the worker's pool: the start path's three are scoped to their try blocks
           and disposed with them, which is why this method exists rather than a line beside those calls. */
        Assert.Contains("await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);", body, StringComparison.Ordinal);

        Assert.Contains("using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);", body, StringComparison.Ordinal);
        Assert.Contains("budget.CancelAfter(s_storeObjectConvergenceBudget);", body, StringComparison.Ordinal);
        Assert.Contains("private static readonly TimeSpan s_storeObjectConvergenceBudget = TimeSpan.FromMinutes(5);", worker, StringComparison.Ordinal);

        /* Three catches, in the order that makes each filter mean what it says: shutdown first (a shutdown
           also trips the linked budget), then the budget, then everything else. No rethrow — the sweep loop
           must never see this pass fail. */
        var shutdownAt = body.IndexOf("catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)", StringComparison.Ordinal);
        var budgetAt = body.IndexOf("catch (OperationCanceledException) when (budget.IsCancellationRequested)", StringComparison.Ordinal);
        var otherAt = body.IndexOf("catch (Exception ex)", StringComparison.Ordinal);
        Assert.True(shutdownAt > 0 && budgetAt > shutdownAt && otherAt > budgetAt, "shutdown, then budget, then everything else");
        Assert.DoesNotContain("throw", body, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(body, "_logger.LogWarning("));
    }

    /// <summary>
    /// The one summary line, on both cadences, UNCONDITIONALLY (#3756) — and from ONE owner, so the two
    /// cadences cannot drift into two shapes.
    ///
    /// <para>The #3756 discipline, pinned the way <c>MaterializationHoleRepairTests</c> and
    /// <c>RetentionReevaluationTests</c> pin theirs — on the source, because the regression it exists for is a
    /// later edit that writes the line only when something changed, and that edit is green on every
    /// behavioural test that does change something. A pass that changed nothing still logs, which is the whole
    /// point: before this, the only evidence a convergence pass had run was eleven per-object lines and the
    /// absence of a twelfth, so a start that never reached the sequence and a start that found everything in
    /// place left the same log.</para>
    /// </summary>
    [Fact]
    public void TheSummaryLine_HasTheMandatedShape_OnBothCadences_FromOneOwner_AndIsUnconditional()
    {
        var worker = Worker();
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);

        const string Periodic = "\"Store object convergence: {Steps} steps, {Changed} changed ({ChangedNames}), {Failed} failed ({FailedNames}), {ElapsedMs} ms\"";
        const string Startup = "\"Store object convergence at startup: {Steps} steps, {Changed} changed ({ChangedNames}), {Failed} failed ({FailedNames}), {ElapsedMs} ms\"";

        /* Exactly one copy of each template, and both inside the one writer — a second copy anywhere is a
           second line per pass, which is the two-owners drift #3756 closed on the hole scan. */
        Assert.Equal(1, CountOf(worker, Periodic));
        Assert.Equal(1, CountOf(worker, Startup));

        var writer = MethodBody(worker, SummarySignature);
        Assert.False(string.IsNullOrEmpty(writer), "could not locate LogStoreObjectConvergence");
        Assert.Contains(Periodic, writer, StringComparison.Ordinal);
        Assert.Contains(Startup, writer, StringComparison.Ordinal);

        /* Both at Information — the level is the point of the line — and the writer contains nothing else. */
        Assert.Equal(2, CountOf(writer, "_logger.LogInformation("));
        Assert.DoesNotContain("LogWarning(", writer, StringComparison.Ordinal);
        Assert.DoesNotContain("LogDebug(", writer, StringComparison.Ordinal);

        /* UNCONDITIONAL: the only `if` in the writer is the cadence switch, and it has an else, so every pass
           takes exactly one of the two templates. */
        var strippedWriter = MethodBody(code, SummarySignature);
        Assert.Equal(1, CountOf(strippedWriter, "if ("));
        Assert.Contains("if (startup)", strippedWriter, StringComparison.Ordinal);
        Assert.Contains("else", strippedWriter, StringComparison.Ordinal);

        /* Two callers, one each, and NEITHER is behind a count test. The hourly call sits inside the pass's
           try after the loop; the startup call sits at the end of the start path, outside the three try
           blocks, so it reports the segments that threw as well as the ones that did not. */
        Assert.Equal(1, CountOf(code, "LogStoreObjectConvergence(tally, passClock.ElapsedMilliseconds, startup: false);"));
        Assert.Equal(1, CountOf(code, "LogStoreObjectConvergence(startupConvergence, startupConvergenceClock.ElapsedMilliseconds, startup: true);"));
    }

    /// <summary>
    /// A step's outcome is judged in ONE place, so the two cadences cannot disagree about what counts as
    /// changed or failed — and the changed count can only come from a step whose return value IS a change
    /// count.
    ///
    /// <para>That distinction is the honest half of this line. Six of the twelve ensures return how many
    /// objects are IN PLACE afterwards (51 of 51 hypertables on every pass, changed or not) because they were
    /// written to answer "is the store converged", which is what a start-path line asks. Reporting those as
    /// changes would make every hourly line claim the store had just been rebuilt; the pin holds the
    /// discrimination rather than the six names, so a seventh ensure arriving has to declare which kind it
    /// is.</para>
    /// </summary>
    [Fact]
    public void AStepsOutcome_IsJudgedInOnePlace_AndOnlyDeltaStepsCanBeCountedAsChanged()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(Worker());
        var step = MethodBody(code, StepSignature);
        Assert.False(string.IsNullOrEmpty(step), "could not locate RunStoreObjectConvergenceStepAsync");

        /* The changed count is gated on the signal AND on a non-zero count, in the one place. */
        Assert.Contains("if (step.Signal == StoreObjectChangeSignal.Delta && count > 0)", step, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(code, "tally.Changed.Add("));
        Assert.Equal(1, CountOf(code, "tally.Failed.Add("));

        /* Cancellation is rethrown rather than recorded: the budget and shutdown must reach the pass's own
           catches, not be reported as twelve failed steps. */
        Assert.Contains("catch (Exception ex) when (ex is not OperationCanceledException)", step, StringComparison.Ordinal);

        /* Both signals are actually used — a pin that only ever saw one would not be measuring the
           discrimination. */
        var list = ListBody(code);
        Assert.Contains("StoreObjectChangeSignal.Delta", list, StringComparison.Ordinal);
        Assert.Contains("StoreObjectChangeSignal.InPlace", list, StringComparison.Ordinal);

        /* Every entry in the list declares a signal: the count of signal tokens equals the count of steps. */
        var steps = CountOf(list, "StoreObjectConvergenceStage.");
        var signals = CountOf(list, "StoreObjectChangeSignal.");
        Assert.True(steps > 0 && steps == signals,
            $"every convergence step must declare both a stage and a change signal (saw {steps} stages, {signals} signals)");
    }

    /// <summary>
    /// The README's cadence table names the convergence pass on the hourly tick, because that table is where an
    /// operator reads what the service does without being asked — and a table that still says these objects are
    /// converged "at startup" sends them to restart a service that would have healed itself within the hour.
    /// </summary>
    [Fact]
    public void TheReadme_NamesTheHourlyConvergence()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        Assert.Contains("store-object convergence", readme, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("#3817", readme, StringComparison.Ordinal);
        Assert.Contains("Store object convergence:", readme, StringComparison.Ordinal);
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    /// <summary>The convergence list's body — from its declaration to the brace that closes the initializer.
    /// Empty is impossible here: the caller asserts on the content, and a parse miss yields an empty string
    /// that fails every assertion rather than passing them.</summary>
    private static string ListBody(string code)
    {
        var start = code.IndexOf(ListDeclaration, StringComparison.Ordinal);
        Assert.True(start > 0, "could not locate the convergence list declaration");
        var open = code.IndexOf('{', start);
        Assert.True(open > start);

        var depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return code[start..i];
                }
            }
        }

        Assert.Fail("the convergence list's initializer is unterminated");
        return string.Empty;
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>A method's body from its signature to the brace that closes it, at nesting depth zero. Empty
    /// when the signature is not found, so a caller can FAIL rather than silently pass on a parse miss — the
    /// RetentionReevaluationTests helper, kept identical in shape for the same reason it is duplicated there:
    /// these pins read the product, not each other.</summary>
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        if (start < 0)
        {
            return string.Empty;
        }

        var open = source.IndexOf('{', start);
        if (open < 0)
        {
            return string.Empty;
        }

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return source[start..(i + 1)];
                }
            }
        }

        return string.Empty;
    }

    /// <summary>Every anchor read off this file sits on one line (the CI checkout is CRLF).</summary>
    private static string Worker() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
}

/// <summary>
/// The one #3817 property that can be DRIVEN rather than read off the source: a convergence step that throws
/// on one pass is retried on the NEXT pass, and the summary counts say which pass was which — <c>F=1</c> then
/// <c>C=1</c>.
///
/// <para><b>Why this is the behavioural pin and the rest are source pins.</b> The retry is not a mechanism
/// anywhere in the code — there is no retry loop, no backoff, no state remembering that a step failed. It is
/// an emergent property of two facts: the pass is scheduled again in an hour, and every step is idempotent.
/// That is exactly the kind of claim a source pin cannot hold and a reader cannot verify by inspection, so it
/// is driven here over the product's own step runner
/// (<see cref="DarlingWorker.RunStoreObjectConvergenceStepAsync"/>) with a fake step standing in for an
/// ensure. The fake is the FAILURE INJECTOR, not a re-implementation: the runner, the tally, the signal
/// discrimination and the warning are all the product's.</para>
///
/// <para>The connection is passed as <c>null!</c> and never dereferenced — the fake step ignores it. That is
/// deliberate rather than lazy: a step that touched a store would make this test a live test, and the
/// property under test has nothing to do with a store. Every product step DOES use its connection, which is
/// why they are exercised against a real TimescaleDB in the live suites their own issues left behind.</para>
/// </summary>
public sealed class StoreObjectConvergenceStepBehaviourTests
{
    /// <summary>
    /// A step that fails once then succeeds: pass one counts it failed and changes nothing, pass two counts it
    /// changed. Two tallies, because a pass's tally is a pass's — an hourly pass does not inherit the previous
    /// hour's failure, which is what makes the second pass's summary line readable as "it healed".
    /// </summary>
    [Fact]
    public async Task AStepThatThrowsOnce_IsRetriedOnTheNextPass_AndTheSummaryCountsSayWhichPassWasWhich()
    {
        var logger = new CapturingTestLogger();
        var attempts = 0;

        var step = new DarlingWorker.StoreObjectConvergenceStep(
            "fake ensure",
            DarlingWorker.StoreObjectConvergenceStage.Timescale,
            DarlingWorker.StoreObjectChangeSignal.Delta,
            (_, _, _) =>
            {
                attempts++;
                return attempts == 1
                    ? throw new InvalidOperationException("the store said no this hour")
                    : Task.FromResult(1);
            });

        /* ── PASS 1: the hour the step fails. Counted as a step that ran and as a failure, named in the
              failed list, and NOTHING in the changed list — a failed step must never read as a change. ── */
        var pass1 = new DarlingWorker.StoreObjectConvergenceTally();
        await DarlingWorker.RunStoreObjectConvergenceStepAsync(
            null!, step, pass1, logger, TestContext.Current.CancellationToken);

        Assert.Equal(1, pass1.Steps);
        Assert.Equal(new[] { "fake ensure" }, pass1.Failed);
        Assert.Empty(pass1.Changed);

        /* The step's own WARNING names the step and the message — the line an operator reads to find out
           which ensure did not complete, as distinct from the per-item warnings the ensure itself writes. */
        Assert.Contains("Warning: Store object convergence step 'fake ensure' failed", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("the store said no this hour", logger.Joined, StringComparison.Ordinal);

        /* ── PASS 2: the next hour. The step is called AGAIN — no state anywhere suppressed it, which is the
              retry — and this time its change is counted with its count. ── */
        var pass2 = new DarlingWorker.StoreObjectConvergenceTally();
        await DarlingWorker.RunStoreObjectConvergenceStepAsync(
            null!, step, pass2, logger, TestContext.Current.CancellationToken);

        Assert.Equal(2, attempts);
        Assert.Equal(1, pass2.Steps);
        Assert.Empty(pass2.Failed);
        Assert.Equal(new[] { "fake ensure 1" }, pass2.Changed);
    }

    /// <summary>
    /// The quiet pass: a step that succeeds having changed nothing is counted as a step and contributes to
    /// NEITHER list, so the summary line reads <c>1 steps, 0 changed (none), 0 failed (none)</c>. The shape a
    /// converged store produces every hour, and the one whose absence would be indistinguishable from the pass
    /// never running — which is why the line is unconditional (pinned on the source in
    /// <see cref="StoreObjectConvergenceTests"/>).
    /// </summary>
    [Fact]
    public async Task ANoOpPass_CountsTheStep_AndReportsNoChangeAndNoFailure()
    {
        var tally = new DarlingWorker.StoreObjectConvergenceTally();
        var step = new DarlingWorker.StoreObjectConvergenceStep(
            "converged ensure",
            DarlingWorker.StoreObjectConvergenceStage.Timescale,
            DarlingWorker.StoreObjectChangeSignal.Delta,
            (_, _, _) => Task.FromResult(0));

        await DarlingWorker.RunStoreObjectConvergenceStepAsync(
            null!, step, tally, new CapturingTestLogger(), TestContext.Current.CancellationToken);

        Assert.Equal(1, tally.Steps);
        Assert.Empty(tally.Changed);
        Assert.Empty(tally.Failed);
    }

    /// <summary>
    /// An <c>InPlace</c> step's non-zero return is NOT a change — the discrimination that keeps the hourly
    /// line from claiming a store which has not changed since July had just been rebuilt. Driven rather than
    /// only read off the list, because the honest failure here is a later edit that widens the condition to
    /// "count > 0" and is green on every pin that only ever looks at Delta steps.
    /// </summary>
    [Fact]
    public async Task AnInPlaceStepsCount_IsNeverReportedAsAChange()
    {
        var tally = new DarlingWorker.StoreObjectConvergenceTally();
        var step = new DarlingWorker.StoreObjectConvergenceStep(
            "hypertable conversion",
            DarlingWorker.StoreObjectConvergenceStage.Timescale,
            DarlingWorker.StoreObjectChangeSignal.InPlace,
            (_, _, _) => Task.FromResult(51));

        await DarlingWorker.RunStoreObjectConvergenceStepAsync(
            null!, step, tally, new CapturingTestLogger(), TestContext.Current.CancellationToken);

        Assert.Equal(1, tally.Steps);
        Assert.Empty(tally.Changed);
        Assert.Empty(tally.Failed);
    }

    /// <summary>
    /// Cancellation is RETHROWN rather than recorded as a step failure — so the pass's budget and a shutdown
    /// reach the pass's own catches (which say "cut short, the rest are retried next hour") instead of being
    /// reported as twelve broken ensures, which is the diagnosis a reader would act on wrongly.
    /// </summary>
    [Fact]
    public async Task Cancellation_IsRethrown_NotCountedAsAStepFailure()
    {
        var tally = new DarlingWorker.StoreObjectConvergenceTally();
        var step = new DarlingWorker.StoreObjectConvergenceStep(
            "interrupted ensure",
            DarlingWorker.StoreObjectConvergenceStage.Timescale,
            DarlingWorker.StoreObjectChangeSignal.Delta,
            (_, _, ct) => throw new OperationCanceledException(ct));

        using var cancelled = new System.Threading.CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(() => DarlingWorker.RunStoreObjectConvergenceStepAsync(
            null!, step, tally, new CapturingTestLogger(), cancelled.Token));

        Assert.Empty(tally.Failed);
        Assert.Empty(tally.Changed);
    }
}
