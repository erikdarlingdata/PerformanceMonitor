/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3812: a held retention policy is re-judged on the RUNNING service's hourly tick, not only at startup.
///
/// <para><b>The lie this file pins against.</b> "Held means held until the coverage gate releases it" was
/// true with an unstated clause — <c>TimescaleSupport.EnsureRetentionPoliciesAsync</c>, the only thing that
/// arms a held policy, had exactly one call site on the service start path, so the release waited for a
/// restart. A store on a stable build sat held indefinitely after a backfill that had worked, with the
/// Retention Held alert firing every hour and reading like the backfill had failed. The fix puts the same
/// sweep on the compression tick, and these pins hold the three things that make it a fix rather than a
/// second timer: the sweep's tally now distinguishes a transition from a re-assertion (so "armed this pass"
/// means something on the hourly pass), the one Information line is written whatever the pass found (#3756),
/// and the worker wires the pass failure-isolated beside the compression check without moving anything.</para>
///
/// <para>The live half is in <see cref="RetentionReevaluationLiveTests"/>: the core pin drives the sweep
/// against a real TimescaleDB with the coverage moved between passes and asserts the counts go
/// <c>(1 held, 0 armed)</c> → <c>(0 held, 1 armed)</c> → <c>(0 held, 0 armed, all unchanged)</c> with no
/// restart anywhere in the sequence.</para>
/// </summary>
public sealed class RetentionReevaluationTests
{
    /// <summary>One relation to render the statements for; the shapes pinned are not relation-specific.</summary>
    private const string Relation = "query_stats";

    /// <summary>
    /// The tally puts every policy in exactly one of four buckets, and the two counts the line names as
    /// events are the transitions. Pinned on hand-built instances so a reader of the log line can be told
    /// from a pin rather than prose how "held" relates to "re-held" and how the four buckets sum.
    /// </summary>
    [Fact]
    public void TheTally_PutsEveryPolicyInExactlyOneBucket_AndReHeldIsASubsetOfHeld()
    {
        var total = TimescaleSupport.RetentionPolicies.Count;

        /* The hour a hold released on a store with one other policy still held: one transition, one hold,
           the rest re-asserted. */
        var released = new TimescaleSupport.RetentionPolicySweepSummary(
            InPlace: total, Held: 1, Armed: 1, Unchanged: total - 2, ReHeld: 0, Indeterminate: 0, Converged: 0, Failed: 0);
        Assert.Equal(total, released.Evaluated + released.Failed);
        Assert.Equal(total - 2, released.Unchanged);

        /* The #1877 hour: coverage regressed under an armed policy. Re-held counts INSIDE held, not beside
           it — "held" is the state after the pass, "re-held" is how one of them got there. */
        var regressed = released with { Held = 2, Armed = 0, ReHeld = 1 };
        Assert.Equal(total, regressed.Evaluated + regressed.Failed);
        Assert.True(regressed.ReHeld <= regressed.Held);

        /* A probe that could not conclude leaves its policy unchanged AND is counted as indeterminate — the
           second number explains the first, it does not add to the sum. */
        var unreadable = released with { Unchanged = total - 2, Indeterminate = 1 };
        Assert.Equal(total, unreadable.Evaluated + unreadable.Failed);

        /* A per-policy failure is outside the three verdict buckets and inside InPlace when it happened after
           the create step — the reason InPlace is carried beside the buckets rather than derived from them. */
        var faulted = new TimescaleSupport.RetentionPolicySweepSummary(
            InPlace: total, Held: 0, Armed: 0, Unchanged: total - 1, ReHeld: 0, Indeterminate: 0, Converged: 0, Failed: 1);
        Assert.Equal(total, faulted.Evaluated + faulted.Failed);
        Assert.Equal(total, faulted.InPlace);

        /* The all-unchanged hour — the shape the mandated line reports on a settled store, and the one whose
           absence is indistinguishable from the pass never running, which is why the line is unconditional. */
        var quiet = new TimescaleSupport.RetentionPolicySweepSummary(
            InPlace: total, Held: 0, Armed: 0, Unchanged: total, ReHeld: 0, Indeterminate: 0, Converged: 0, Failed: 0);
        Assert.Equal(total, quiet.Unchanged);
        Assert.Equal(0, quiet.Held + quiet.Armed + quiet.ReHeld + quiet.Failed);
    }

    /// <summary>
    /// The one Information line per evaluation has EXACTLY the mandated shape on each pass, and it is written
    /// UNCONDITIONALLY: nothing between the sweep's horizon summary and its <c>return</c> decides whether to
    /// write it except the pass switch that picks which of the two literal templates to use. The #3756
    /// discipline, pinned the way <c>MaterializationHoleRepairTests</c> pins the hole scan's line — on the
    /// source, because the regression it exists for is a later edit that writes the line only when something
    /// changed, and that edit is green on every behavioural test that does change something.
    /// </summary>
    [Fact]
    public void TheEvaluationLine_HasTheMandatedShape_OnBothPasses_AndIsUnconditional()
    {
        var storage = ReadStorageSource();
        var sweep = MethodBody(storage, "public static async Task<RetentionPolicySweepSummary> EnsureRetentionPoliciesAsync(");
        Assert.False(string.IsNullOrEmpty(sweep), "could not locate the four-argument EnsureRetentionPoliciesAsync — the guard cannot silently pass on a parse miss");

        const string Periodic = "\"Retention re-evaluation: {Held} policies held, {Armed} armed this pass, {Unchanged} unchanged\"";
        const string Startup = "\"Retention evaluation at startup: {Held} policies held, {Armed} armed this pass, {Unchanged} unchanged\"";
        Assert.Contains(Periodic, sweep, StringComparison.Ordinal);
        Assert.Contains(Startup, sweep, StringComparison.Ordinal);

        /* Exactly once each: a second copy of either template is a second line per pass, which is the
           two-owners drift #3756 closed on the hole scan. */
        Assert.Equal(1, CountOf(sweep, Periodic));
        Assert.Equal(1, CountOf(sweep, Startup));

        /* UNCONDITIONAL. From the horizon summary's template to the return, the only `if (` is the pass
           switch, and it has an else — so every pass takes exactly one of the two templates. */
        var summaryAt = sweep.IndexOf("\"TimescaleDB: {Applied}/{Total} retention policies in place", StringComparison.Ordinal);
        var returnAt = sweep.IndexOf("return new RetentionPolicySweepSummary(", StringComparison.Ordinal);
        Assert.True(summaryAt > 0 && returnAt > summaryAt, "the horizon summary precedes the tally's return");
        var tail = sweep[summaryAt..returnAt];
        Assert.Equal(1, CountOf(tail, "if ("));
        Assert.Contains("if (pass == RetentionSweepPass.Startup)", tail, StringComparison.Ordinal);
        Assert.Contains("else", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("return", tail, StringComparison.Ordinal);
        Assert.True(tail.IndexOf(Startup, StringComparison.Ordinal) < tail.IndexOf(Periodic, StringComparison.Ordinal),
            "the Startup template is the if-arm and the Periodic template the else-arm");

        /* Both at Information — the level is the whole point of the line — and nothing else in the span is. */
        Assert.Equal(2, CountOf(tail, "logger?.LogInformation("));
        Assert.DoesNotContain("LogWarning(", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("LogDebug(", tail, StringComparison.Ordinal);

        /* And the horizon summary is no longer unconditionally Information: it takes its level from the pass,
           so the hourly pass has ONE Information line, not two. The level sits in the statement AHEAD of the
           template, so it is read off the statement's head rather than the tail. */
        var summaryStatement = sweep[sweep.LastIndexOf("logger?.Log(", summaryAt, StringComparison.Ordinal)..summaryAt];
        Assert.Contains("pass == RetentionSweepPass.Startup ? LogLevel.Information : LogLevel.Debug,", summaryStatement, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prior-state read that turns "armed" into a transition count: read-only, targeted at exactly one
    /// relation's retention job with the same three filters the flip statements carry, so the row it reads is
    /// the row the flip writes. Positive control on <c>alter_job</c>: the arm statement names it, this one must
    /// not.
    /// </summary>
    [Fact]
    public void TheScheduledRead_TargetsExactlyOneRelationsRetentionJob_AndOnlyReads()
    {
        var sql = TimescaleSupport.RetentionPolicyScheduledSql(Relation);

        Assert.StartsWith("SELECT j.scheduled", sql, StringComparison.Ordinal);
        foreach (var filter in new[]
        {
            "proc_name = 'policy_retention'",
            "hypertable_schema = 'collect'",
            $"hypertable_name = '{Relation}'",
        })
        {
            Assert.Contains(filter, sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("alter_job", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("=>", sql, StringComparison.Ordinal);
        Assert.Contains("alter_job", TimescaleSupport.ArmRetentionPolicySql(Relation), StringComparison.Ordinal);

        /* Keyed to the argument, not to a phrase every rendering carries. */
        var other = TimescaleSupport.RetentionPolicyScheduledSql("procedure_stats_hourly");
        Assert.DoesNotContain($"hypertable_name = '{Relation}'", other, StringComparison.Ordinal);
        Assert.Contains("hypertable_name = 'procedure_stats_hourly'", other, StringComparison.Ordinal);
    }

    /// <summary>
    /// The arm and hold statements stayed the unconditional state sets they were. #3812 needed to know whether
    /// a flip changed anything and had two ways to learn it: filter the flip on the flag (<c>AND NOT
    /// j.scheduled</c>, the <c>IS DISTINCT FROM</c> idiom the horizon converge uses) and count returned rows, or
    /// read the flag first and leave the flip alone. It took the second, so the only two statements in this
    /// family allowed to touch scheduled state keep the shape <c>RetentionHorizonConvergeCannotArmTests</c> and
    /// <c>TimescaleContinuousAggregateTests</c> pin (one statement, one boolean, mirror images). Positive control:
    /// the converge DOES carry the guard idiom, so the absence below is a measurement.
    /// </summary>
    [Fact]
    public void TheArmAndHoldStatements_StayUnconditional_TransitionsComeFromThePriorRead()
    {
        var arm = TimescaleSupport.ArmRetentionPolicySql(Relation);
        var hold = TimescaleSupport.HoldRetentionPolicySql(Relation);

        foreach (var guard in new[] { "IS DISTINCT FROM", "NOT j.scheduled", "j.scheduled =", "j.scheduled IS" })
        {
            Assert.DoesNotContain(guard, arm, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(guard, hold, StringComparison.OrdinalIgnoreCase);
        }

        Assert.Contains("IS DISTINCT FROM", TimescaleSupport.ConvergeRetentionHorizonSql(Relation), StringComparison.Ordinal);

        /* And the sweep reads the prior verdict BEFORE it measures, through the read above — the order that
           makes the transition attributable to this pass's flip and nothing else. #4299 (d′) split the read
           into raw (RawArmedStateSql) and non-raw (RetentionPolicyScheduledSql) branches of one ternary; the
           non-raw statement name still has to appear in that ternary for this pin to mean anything. */
        var sweep = MethodBody(ReadStorageSource(), "public static async Task<RetentionPolicySweepSummary> EnsureRetentionPoliciesAsync(");
        var readAt = sweep.IndexOf("isRawRelation ? RawArmedStateSql(relation) : RetentionPolicyScheduledSql(relation)", StringComparison.Ordinal);
        var measureAt = sweep.IndexOf("await MeasureRetentionCoverageAsync(connection, relation, timeColumn, coverage, cancellationToken);", StringComparison.Ordinal);
        var armAt = sweep.IndexOf("new NpgsqlCommand(ArmRetentionPolicySql(relation), connection)", StringComparison.Ordinal);
        var holdAt = sweep.IndexOf("new NpgsqlCommand(HoldRetentionPolicySql(relation), connection)", StringComparison.Ordinal);
        Assert.True(readAt > 0 && measureAt > readAt && armAt > measureAt && holdAt > armAt,
            "the sweep reads the prior verdict, then measures coverage, then flips — in that order");

        /* The transition counts are decided on the prior flag, never on the verdict alone. */
        Assert.Contains("if (wasScheduled == false)", sweep, StringComparison.Ordinal);
        Assert.Contains("if (wasScheduled == true)", sweep, StringComparison.Ordinal);
        Assert.Contains("reHeld++;", sweep, StringComparison.Ordinal);
    }

    /// <summary>
    /// The startup-shaped overload forwards to the pass-taking one with <c>Startup</c>, so every pre-#3812
    /// caller — the worker's setup block and seventeen live-test call sites — keeps its three arguments and its
    /// per-start logging without naming a pass it never knew about.
    /// </summary>
    [Fact]
    public void TheThreeArgumentOverload_IsTheStartupPass()
    {
        var storage = ReadStorageSource();
        Assert.Contains(
            "public static Task<RetentionPolicySweepSummary> EnsureRetentionPoliciesAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)",
            storage, StringComparison.Ordinal);
        Assert.Contains(
            "=> EnsureRetentionPoliciesAsync(connection, logger, RetentionSweepPass.Startup, cancellationToken);",
            storage, StringComparison.Ordinal);

        /* The bare int is gone: nothing in the product returns the policies-in-place count alone any more. */
        Assert.DoesNotContain("Task<int> EnsureRetentionPoliciesAsync", storage, StringComparison.Ordinal);
    }

    /// <summary>
    /// Per-policy noise on the hourly pass is controlled by TRANSITION, not silenced: a hold that was already a
    /// hold logs at Debug on the periodic pass and stays a Warning at startup (the line the runbook and the
    /// alert send an operator to); a policy re-held this pass is a Warning on both; a policy armed this pass is
    /// Information on both. Pinned on the source because the levels are the design — the issue's own
    /// objection to an hourly sweep was 24 identical warnings a day per held policy.
    /// </summary>
    [Fact]
    public void PerPolicyLines_AreLevelledByTransition_NotSilenced()
    {
        var sweep = MethodBody(ReadStorageSource(), "public static async Task<RetentionPolicySweepSummary> EnsureRetentionPoliciesAsync(");

        /* The steady-state hold: level by pass. */
        var heldAt = sweep.IndexOf("HELD PAUSED - {ShortConsumer} does not yet cover everything it holds", StringComparison.Ordinal);
        Assert.True(heldAt > 0);
        var heldStatement = sweep[sweep.LastIndexOf("logger?.Log(", heldAt, StringComparison.Ordinal)..heldAt];
        Assert.Contains("pass == RetentionSweepPass.Startup ? LogLevel.Warning : LogLevel.Debug,", heldStatement, StringComparison.Ordinal);

        /* The transitions: unconditional levels. The statement each template sits in is found by walking back
           to its `logger?.Log` and reading which method it is. */
        var reHeldAt = sweep.IndexOf("RE-HELD - it was armed, and {ShortConsumer} no longer covers", StringComparison.Ordinal);
        Assert.True(reHeldAt > 0);
        Assert.StartsWith("logger?.LogWarning(", sweep[sweep.LastIndexOf("logger?.Log", reHeldAt, StringComparison.Ordinal)..], StringComparison.Ordinal);

        var armedAt = sweep.IndexOf("ARMED - {Coverage} now covers everything it holds", StringComparison.Ordinal);
        Assert.True(armedAt > 0);
        Assert.StartsWith("logger?.LogInformation(", sweep[sweep.LastIndexOf("logger?.Log", armedAt, StringComparison.Ordinal)..], StringComparison.Ordinal);

        /* The three per-policy texts that promised a restart now promise the next evaluation, on both
           cadences. A "next start" left in any of them is the lie coming back one string at a time. */
        foreach (var stale in new[] { "on the next start.", "this start,", "until the next restart retries" })
        {
            Assert.DoesNotContain(stale, sweep, StringComparison.Ordinal);
        }

        Assert.Contains("hourly on the running service", sweep, StringComparison.Ordinal);
    }

    /// <summary>
    /// The worker wires the pass on the compression tick, AFTER the compression check, as its own awaited
    /// statement in the same block — and keeps the startup call. Source-order pins in the
    /// <c>MaterializationHoleRepairTests</c> shape: the tick is not drivable without a host, so the wiring is
    /// asserted where it lives.
    ///
    /// <para>The order inside the tick is load-bearing, not incidental (#3575): the compression read samples
    /// the job catalog at :30 past the minute, half a grid step from the :00 instants the compression policies
    /// start on, and twenty coverage probes ahead of it on a large store would push that sample toward one.
    /// The cost of the order is one tick of lag on the Retention Held alert's resolution edge, because that
    /// alert's read rides inside the compression method — stated in the worker's comment and in the alert
    /// text, and pinned on the latter in <c>DarlingSelfAlertTests</c>.</para>
    ///
    /// <para>#3815 moved the <c>_timescaleAvailable</c> test off the tick's outer guard and onto an inner one
    /// so the availability re-probe could run ahead of it, which is why the anchors below are the due-time
    /// guard and then the flag gate rather than one conjunction. Nothing about THIS pass changed: the
    /// re-evaluation is still the last awaited statement inside the flag gate, still after the compression
    /// read, still with nothing between the two that could skip it. The re-probe's own position — outside the
    /// gate it corrects — is pinned in <c>TimescaleAvailabilityReprobeTests</c>, which is where the argument
    /// for it lives.</para>
    /// </summary>
    [Fact]
    public void Worker_RunsTheReevaluation_OnTheCompressionTick_AfterTheCompressionCheck_AndKeepsTheStartupCall()
    {
        var worker = ReadWorkerSource();

        /* The startup call, unchanged in shape and still exactly one — and it is unchanged for a reason
           #3817 had to decide rather than inherit: that lane moved eleven neighbouring ensures off this block
           into a shared list both cadences walk, and left THIS one alone, because the retention sweep already
           has an hourly tenant (this file's subject) and running it from the convergence list as well would
           sweep it twice an hour and double every transition line it writes. The literal call site is
           therefore still the start path's own. */
        Assert.Equal(1, CountOf(worker, "await TimescaleSupport.EnsureRetentionPoliciesAsync(timescaleConnection, _logger, stoppingToken);"));
        /* And the retention sweep is NOT reachable from the convergence list, which is the other half of that
           decision and the half a later edit could quietly undo. */
        Assert.DoesNotContain("EnsureRetentionPoliciesAsync(connection, logger, ct)", worker, StringComparison.Ordinal);

        /* The tick: due-time guard, stamp, flag gate, compression check, retention pass — in that order,
           with nothing between the two awaits that could skip the second on the first's outcome. The flag gate
           is searched FROM the stamp: the start-path block carries the same `if (_timescaleAvailable)` text
           several thousand lines earlier, and a search from zero would anchor on that one and prove nothing
           about this tick. */
        var guardAt = worker.IndexOf("if (DateTime.UtcNow >= _nextCompressionCheckUtc)", StringComparison.Ordinal);
        var stampAt = worker.IndexOf("_nextCompressionCheckUtc = TimescaleSupport.NextCompressionCheckUtc(DateTime.UtcNow, s_compressionCheckInterval);", StringComparison.Ordinal);
        Assert.True(guardAt > 0 && stampAt > guardAt, "the tick's guard is the due time, stamped forward inside it");
        var flagGateAt = worker.IndexOf("if (_timescaleAvailable)", stampAt, StringComparison.Ordinal);
        var compressionAt = worker.IndexOf("await EvaluateCompressionJobHealthAsync(stoppingToken);", StringComparison.Ordinal);
        var retentionAt = worker.IndexOf("await ReevaluateRetentionPoliciesAsync(stoppingToken);", StringComparison.Ordinal);
        /* #3817 is the FOURTH tenant and sits after this pass, so the "next thing" that used to bound the
           retention call's position is now the convergence call, and the store-metrics block bounds THAT.
           Both bounds are kept: dropping the outer one would let a later edit hoist the whole gated group out
           of the tick without this pin noticing. */
        var convergenceAt = worker.IndexOf("await ConvergeStoreObjectsAsync(stoppingToken);", StringComparison.Ordinal);
        var nextBlockAt = worker.IndexOf("/* #2068: the store self-metrics sweep.", StringComparison.Ordinal);
        Assert.True(flagGateAt > stampAt && compressionAt > flagGateAt && retentionAt > compressionAt
            && convergenceAt > retentionAt && nextBlockAt > convergenceAt,
            "the retention re-evaluation is awaited after the compression check inside the tick's _timescaleAvailable gate, "
          + "and the store-object convergence after it");
        Assert.Equal(1, CountOf(worker, "await ReevaluateRetentionPoliciesAsync(stoppingToken);"));
        Assert.Equal(1, CountOf(worker, "await ConvergeStoreObjectsAsync(stoppingToken);"));

        var between = worker[compressionAt..retentionAt];
        Assert.DoesNotContain("if (", between, StringComparison.Ordinal);
        Assert.DoesNotContain("return", between, StringComparison.Ordinal);
        Assert.DoesNotContain("}", between, StringComparison.Ordinal);

        /* The pass is NOT inside the compression method: that would put it under the compression check's
           try, where a retention fault would skip nothing of the compression's (it runs last) but a
           compression fault would skip the retention pass. Two methods, two catches. */
        var compressionBody = MethodBody(worker, "private async Task EvaluateCompressionJobHealthAsync(");
        Assert.False(string.IsNullOrEmpty(compressionBody));
        Assert.DoesNotContain("EnsureRetentionPoliciesAsync", compressionBody, StringComparison.Ordinal);
        Assert.DoesNotContain("ReevaluateRetentionPoliciesAsync", compressionBody, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex)", compressionBody, StringComparison.Ordinal);
    }

    /// <summary>
    /// The worker's pass method: the Periodic pass on its own pooled connection under one whole-pass budget,
    /// with three failure outcomes that read differently — shutdown quiet, budget a WARNING that names the
    /// budget, anything else a WARNING that names the message — and nothing that can throw past it into the
    /// sweep loop. The budget is #2327's shape (a linked token for the pass, not a per-statement deadline) for
    /// #2327's reason: the pass is awaited on the serial sweep loop.
    /// </summary>
    [Fact]
    public void Worker_ReevaluationMethod_RunsThePeriodicPass_UnderOneBudget_FailureIsolated()
    {
        var worker = ReadWorkerSource();
        var body = MethodBody(worker, "private async Task ReevaluateRetentionPoliciesAsync(CancellationToken cancellationToken)");
        Assert.False(string.IsNullOrEmpty(body), "could not locate ReevaluateRetentionPoliciesAsync — the guard cannot silently pass on a parse miss");

        /* Its own connection from the worker's pool — the startup connection is disposed with the setup
           block, which is the one thing the issue said to get right. */
        Assert.Contains("await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);", body, StringComparison.Ordinal);

        /* The Periodic pass, named explicitly, on the budget token. */
        Assert.Contains("TimescaleSupport.RetentionSweepPass.Periodic, budget.Token);", body, StringComparison.Ordinal);
        Assert.DoesNotContain("RetentionSweepPass.Startup", body, StringComparison.Ordinal);

        /* One linked budget for the whole pass. */
        Assert.Contains("using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);", body, StringComparison.Ordinal);
        Assert.Contains("budget.CancelAfter(s_retentionReevaluationBudget);", body, StringComparison.Ordinal);
        Assert.Contains("private static readonly TimeSpan s_retentionReevaluationBudget = TimeSpan.FromMinutes(5);", worker, StringComparison.Ordinal);

        /* #4300: the seam-only repair runs BEFORE the coverage sweep, so a seam it closes this tick is
           already gone by the time EnsureRetentionPoliciesAsync re-judges coverage a moment later. A full
           walk (RepairMaterializationHolesAsync) must never appear on this hourly pass — that is the
           start-path's own call, and landing here by accident would repeat the whole registry every hour. */
        var seamAt = body.IndexOf("TimescaleSupport.RepairMaterializationSeamsAsync(", StringComparison.Ordinal);
        var sweepAt = body.IndexOf("await TimescaleSupport.EnsureRetentionPoliciesAsync(", StringComparison.Ordinal);
        Assert.True(seamAt > 0 && sweepAt > seamAt, "the seam-only repair runs before the coverage sweep");
        Assert.Equal(0, CountOf(body, "RepairMaterializationHolesAsync"));

        /* #4300: the seam repair skips entirely while a full walk is already running in this process (the
           two would refresh the same aggregate on two connections for no gain), and otherwise runs under its
           OWN child budget (s_seamRepairBudget) linked to the pass's budget, so a wide seam cannot starve the
           coverage sweep, the purge trigger and the epoch relaunch that follow it in the same pass. */
        Assert.Contains("if (_materializationHoleRepairRunning)", body, StringComparison.Ordinal);
        Assert.Contains("using var seamBudget = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);", body, StringComparison.Ordinal);
        Assert.Contains("seamBudget.CancelAfter(s_seamRepairBudget);", body, StringComparison.Ordinal);
        Assert.Contains("private static readonly TimeSpan s_seamRepairBudget = TimeSpan.FromMinutes(2);", worker, StringComparison.Ordinal);

        /* Four catches, in the order that makes each filter mean what it says: the seam's own inner shutdown
           catch (rethrows — a real cancellation must bubble out to the outer catches, not be swallowed
           here), then the seam's own child-budget catch (does NOT rethrow — the rest of the pass still runs
           this tick), then the OUTER shutdown catch (also does not rethrow — nothing throws past this pass
           into the sweep loop), then the outer budget catch, then everything else.

           shutdownAt below is the OUTER shutdown catch specifically: IndexOf alone would land on the seam's
           own INNER shutdown catch, which sits earlier in the body and is searched for separately as
           innerShutdownAt. The outer one is the LAST occurrence before budgetAt (the outer budget catch),
           found with LastIndexOf bounded at budgetAt so a future edit that adds yet another earlier catch
           cannot make this pin silently walk past the wrong one. */
        const string shutdownCatch = "catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)";
        var innerShutdownAt = body.IndexOf(shutdownCatch, StringComparison.Ordinal);
        var budgetAt = body.IndexOf("catch (OperationCanceledException) when (budget.IsCancellationRequested)", StringComparison.Ordinal);
        var shutdownAt = budgetAt > 0 ? body.LastIndexOf(shutdownCatch, budgetAt, StringComparison.Ordinal) : -1;
        var otherAt = body.IndexOf("catch (Exception ex)", budgetAt, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(body, shutdownCatch));
        Assert.True(innerShutdownAt > 0 && shutdownAt > innerShutdownAt && budgetAt > shutdownAt && otherAt > budgetAt,
            "seam's inner shutdown catch, then the outer shutdown catch, then the outer budget catch, then everything else");
        Assert.Contains("throw;", body[innerShutdownAt..shutdownAt], StringComparison.Ordinal);
        Assert.DoesNotContain("throw", body[shutdownAt..], StringComparison.Ordinal);

        /* Each non-quiet outcome is a distinct WARNING naming what it knows: the seam's own isolated failure,
           the seam's own child-budget cutoff (names the hours still deferred), then the two outer-pass
           warnings (the outer budget, and everything else). Five now, up from the original three. */
        Assert.Contains("\"Retention re-evaluation exceeded its {BudgetSeconds}s budget after {ElapsedMs} ms and was cut short", body, StringComparison.Ordinal);
        Assert.Contains("\"Retention re-evaluation could not run after {ElapsedMs} ms", body, StringComparison.Ordinal);
        Assert.Contains("\"Retention re-evaluation: the seam repair could not run this pass", body, StringComparison.Ordinal);
        Assert.Contains("\"Retention re-evaluation: seam repair paused at the {BudgetMinutes}-minute budget; resumes next hour; {HoursDeferred} hour(s) still deferred.\"", body, StringComparison.Ordinal);
        Assert.Equal(4, CountOf(body, "_logger.LogWarning("));
    }

    /// <summary>
    /// Every operator-facing sentence that promised a restart moved with the mechanism. The runbook's step 3,
    /// the <c>--backfill-rollups</c> verb's closing lines and the README cadence table are the three places an
    /// operator reads BEFORE the alert text (<c>DarlingSelfAlertTests</c> pins that one); a doc that still
    /// admits a gap the code has closed sends them to restart a service that would have released the hold on
    /// its own within the hour.
    /// </summary>
    [Fact]
    public void TheRunbookTheVerbAndTheReadme_NameTheHourlyReEvaluation_AndMakeTheRestartOptional()
    {
        var runbook = RepoFile.ReadRepoFile("docs", "retention-hold-runbook.md");
        Assert.DoesNotContain("This step is required", runbook, StringComparison.Ordinal);
        Assert.DoesNotContain("Why step 3 is not optional", runbook, StringComparison.Ordinal);
        Assert.Contains("Why step 3 is a wait and not a restart", runbook, StringComparison.Ordinal);
        Assert.Contains("hourly store-maintenance tick", runbook, StringComparison.Ordinal);
        Assert.Contains("Retention re-evaluation: 0 policies held, N armed this pass,", runbook, StringComparison.Ordinal);
        Assert.Contains("#3812", runbook, StringComparison.Ordinal);
        /* The one-tick lag on the resolution edge is in the runbook too, or the third confirmation reads late. */
        Assert.Contains("one tick AFTER the arm", runbook, StringComparison.Ordinal);

        var verb = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs");
        Assert.DoesNotContain("NEXT: restart the PerformanceMonitor Darling service.", verb, StringComparison.Ordinal);
        Assert.DoesNotContain("Do not delay the restart.", verb, StringComparison.Ordinal);
        Assert.DoesNotContain("will arm itself on the next service start.", verb, StringComparison.Ordinal);
        Assert.Contains("NEXT: nothing, unless you are in a hurry.", verb, StringComparison.Ordinal);
        Assert.Contains("'Retention re-evaluation: 0 policies held, N armed this pass, K unchanged'", verb, StringComparison.Ordinal);
        Assert.Contains("To arm immediately instead, restart the", verb, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        /* The cadence row names this pass and gives the cadence. Anchored on the two halves separately since
           #3817 joined the row as its fourth tenant and put its own name between them — the property is that
           the row names the re-evaluation and states the cadence, not that the two are adjacent, and a pin
           that demanded adjacency would red for every future tenant of a row built to hold them. */
        Assert.Contains("the retention coverage re-evaluation", readme, StringComparison.Ordinal);
        Assert.Contains("First sweep after startup, then hourly at :30 past the minute", readme, StringComparison.Ordinal);
        Assert.Contains("no restart needed (#3812)", readme, StringComparison.Ordinal);
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

    /// <summary>
    /// A method's body from its signature to the brace that closes it, at nesting depth zero. Empty when the
    /// signature is not found, so a caller can FAIL rather than silently pass on a parse miss. Braces inside
    /// string literals are not tracked: none of the bodies read here carry one, and a body that grew one
    /// would truncate to a prefix that still has to satisfy every anchor.
    /// </summary>
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

    /// <summary>Every anchor read off these two files sits on one line (the CI checkout is CRLF).</summary>
    private static string ReadStorageSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs");

    private static string ReadWorkerSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

    /// <summary>
    /// #4300: the seam-only repair on the hourly Periodic pass closes an outage-opened legacy/successor seam
    /// within the hour on a store that keeps running, with no restart required. The doc this pins against
    /// used to say the gate needed a SECOND start to release across the upgrade; that is no longer true, and
    /// the doc must not say it again by accident.
    /// </summary>
    [Fact]
    public void TheGateDoc_NoLongerClaimsASecondStartIsNeeded()
    {
        var storage = ReadStorageSource();
        Assert.DoesNotContain("SECOND start", storage, StringComparison.Ordinal);
    }
}

/// <summary>
/// The core #3812 pin, live: a held retention policy whose coverage crosses the gate BETWEEN two passes is
/// armed on the second pass, with no restart anywhere in the sequence — the counts go
/// <c>(1 held, 0 armed)</c> → <c>(0 held, 1 armed)</c> → <c>(0 held, 0 armed, all unchanged)</c>, the
/// mandated line is written on every pass including the quiet one, and the hand-arm the runbook forbids is
/// undone on the next pass and counted as a re-hold.
///
/// <para>The fixture is <c>EnsureRetentionPolicies_ConvergesAnOldHorizon_…</c>'s: one <c>query_stats</c> row
/// thirty days back keeps that relation's coverage measurably SHORT (it is outside the hourly aggregate's
/// refresh window, so the policy's own immediate refresh cannot materialize it), and the coverage is then
/// MOVED the way a backfill moves it — a manual <c>refresh_continuous_aggregate</c> over the hour that row is
/// in, on the hourly and then the daily so the hourly tier's own gate (whose consumer is the daily) does not
/// re-hold as a side effect. "Moved" rather than "row deleted", deliberately: an empty source arms too, but
/// that is the fresh-store path, not the backfill path this issue is about.</para>
///
/// <para>Rig figures at the time of writing (TimescaleDB 2.28.1 / PostgreSQL 18, Docker): pass 1
/// <c>1 held, 0 armed, 19 unchanged</c>; hand-arm then pass 1b <c>1 held, 0 armed, 19 unchanged, re-held 1</c>;
/// refresh then pass 2 <c>0 held, 1 armed, 19 unchanged</c>; pass 3 <c>0 held, 0 armed, 20 unchanged</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class RetentionReevaluationLiveTests
{
    private const string HeldRelation = "query_stats";
    private const string Seed = "reeval-3812";

    /// <summary>Catalog read against the live fixture, explicit rather than on Npgsql's undocumented 30-second
    /// default (#2874).</summary>
    private const int PolicyReadTimeoutSeconds = 30;

    /// <summary>Every relation the sweep attaches a policy to, DERIVED from the product's own list so the
    /// teardown cannot leave an armed policy behind on the shared fixture when a tier is added.</summary>
    private static readonly string[] RetentionRelations =
        TimescaleSupport.RetentionPolicies.Select(p => p.Relation).ToArray();

    [Fact]
    public async Task HeldPolicy_WhoseCoverageCrossesTheGateBetweenPasses_ArmsOnTheNextPass_WithoutARestart_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live retention re-evaluation test.");

        var ct = TestContext.Current.CancellationToken;
        var total = TimescaleSupport.RetentionPolicies.Count;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));
        /* #3893: collection_log too, as the worker's "collection_log hypertable" step does before its aggregate ensure
           (pinned in StoreObjectConvergenceTests). The off-grid collection-health aggregate is sourced from it. On a
           store whose migrations ran before CREATE EXTENSION (CI's bundled PostgreSQL), it is still a plain table here,
           and the aggregate's CREATE fails with 0A000. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);
        var seeded = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-30), DateTimeKind.Unspecified);

        var bodySucceeded = false;
        try
        {
            /* One row thirty days back: outside the hourly aggregate's refresh window, so the gate's SHORT
               verdict on query_stats is genuine and stays so until this test moves the coverage itself. */
            using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
VALUES (1, $1, 9138, '" + Seed + "', 'TestDb', decode(md5('reeval'), 'hex'), decode(md5('h'), 'hex'), 1, 1, 1)", connection) { CommandTimeout = PolicyReadTimeoutSeconds })
            {
                seed.Parameters.AddWithValue(seeded);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* ── PASS 0: the start path. Creates every policy paused, judges each, arms the empty ones. This is
                  the "restart" — the only pass in the sequence that is one, and the last. ── */
            var startLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, startLog, ct);
            var start = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, startLog, ct);
            Assert.True(start.InPlace == total, $"the start pass should apply all {total} policies, got {start.InPlace}; {startLog.Joined}");
            Assert.True(start.Failed == 0, $"the start pass isolated a failure; {startLog.Joined}");
            Assert.True(start.Held >= 1, $"query_stats must be HELD at creation on short coverage; {startLog.Joined}");
            /* #4299 (d′): query_stats is raw — scheduled converges to false unconditionally now, so the real
               verdict this pin needs is the armed key, not the flag. */
            Assert.False(await RawArmedAsync(connection, HeldRelation, ct), "short coverage must HOLD the policy at creation");
            Assert.False(await ScheduledAsync(connection, HeldRelation, ct), "a raw relation is never scheduled by TimescaleDB’s own runner (#4299 d′)");
            Assert.Contains($"Information: Retention evaluation at startup: {start.Held} policies held, {start.Armed} armed this pass, {start.Unchanged} unchanged", startLog.Joined, StringComparison.Ordinal);
            /* The startup pass keeps its WARNING per held policy — the line the runbook sends an operator to. */
            Assert.Contains($"Warning: Retention policy for {HeldRelation} HELD PAUSED", startLog.Joined, StringComparison.Ordinal);

            /* ── PASS 1: the first hourly tick. Nothing has changed, so nothing transitions: the hold is a
                  STATE count of 1, armed-this-pass is 0, and every other policy is unchanged. ── */
            var pass1Log = new CapturingTestLogger();
            var pass1 = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, pass1Log, TimescaleSupport.RetentionSweepPass.Periodic, ct);
            Assert.True((pass1.Held, pass1.Armed, pass1.Unchanged, pass1.ReHeld, pass1.Failed) == (1, 0, total - 1, 0, 0),
                $"pass 1 expected (held 1, armed 0, unchanged {total - 1}, re-held 0, failed 0), got ({pass1.Held}, {pass1.Armed}, {pass1.Unchanged}, {pass1.ReHeld}, {pass1.Failed}); {pass1Log.Joined}");
            Assert.Contains($"Information: Retention re-evaluation: 1 policies held, 0 armed this pass, {total - 1} unchanged", pass1Log.Joined, StringComparison.Ordinal);
            /* On the hourly pass the steady-state hold is Debug, not a 24-a-day Warning. */
            Assert.Contains($"Debug: Retention policy for {HeldRelation} HELD PAUSED", pass1Log.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("Warning:", pass1Log.Joined, StringComparison.Ordinal);
            /* And the horizon summary dropped to Debug, so the pass has exactly one Information line. */
            Assert.Contains("Debug: TimescaleDB: ", pass1Log.Joined, StringComparison.Ordinal);
            Assert.Equal(1, CountOf(pass1Log.Joined, "Information:"));

            /* ── PASS 1b: the hand-arm the runbook forbids, undone on the next pass and counted as a RE-HOLD.
                  Parked with next_start => 'infinity' so the armed job cannot run and drop the seeded chunk out
                  from under the rest of this test; for a raw relation the prior read the sweep takes is
                  config->>'darling_armed' (RawArmedStateSql), not scheduled — #4299 (d′) unconditionally
                  converges scheduled to false on every pass regardless of the hand-arm, so hand-arming via
                  scheduled alone would leave the prior-armed read false and the re-hold below would prove
                  nothing. next_start => 'infinity' keeps TimescaleDB's own scheduler from running the drop in
                  between, in case some future build re-honors scheduled for a raw relation. ── */
            using (var handArm = new NpgsqlCommand(@"
SELECT alter_job(j.job_id, scheduled => true, next_start => 'infinity'::timestamptz, config => j.config || jsonb_build_object('darling_armed', true))
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '" + HeldRelation + "'", connection) { CommandTimeout = PolicyReadTimeoutSeconds })
            {
                await handArm.ExecuteNonQueryAsync(ct);
            }

            /* The hand-arm the runbook forbids writes BOTH flags directly — that mechanism is unchanged; what
               changes under #4299 (d′) is that the NEXT pass’s unconditional converge reverts scheduled
               regardless of the coverage verdict (asserted below), while darling_armed is the flag the sweep's
               prior-state read actually consults for a raw relation, which is what makes the re-hold below a
               transition rather than a no-op. */
            Assert.True(await ScheduledAsync(connection, HeldRelation, ct), "the hand-arm must have taken, or the re-hold below proves nothing");
            Assert.True(await RawArmedAsync(connection, HeldRelation, ct), "the hand-arm must set darling_armed too, or the sweep's prior-state read for a raw relation sees no transition to re-hold");

            var pass1bLog = new CapturingTestLogger();
            var pass1b = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, pass1bLog, TimescaleSupport.RetentionSweepPass.Periodic, ct);
            Assert.True((pass1b.Held, pass1b.Armed, pass1b.Unchanged, pass1b.ReHeld, pass1b.Failed) == (1, 0, total - 1, 1, 0),
                $"pass 1b expected (held 1, armed 0, unchanged {total - 1}, re-held 1, failed 0), got ({pass1b.Held}, {pass1b.Armed}, {pass1b.Unchanged}, {pass1b.ReHeld}, {pass1b.Failed}); {pass1bLog.Joined}");
            Assert.False(await ScheduledAsync(connection, HeldRelation, ct), "the hourly pass converges every raw relation back to scheduled = false unconditionally (#4299 d′)");
            Assert.False(await RawArmedAsync(connection, HeldRelation, ct), "the hourly pass must re-hold a hand-armed policy whose coverage is still short (#1877)");
            Assert.Contains($"Warning: Retention policy for {HeldRelation} RE-HELD", pass1bLog.Joined, StringComparison.Ordinal);
            Assert.Contains($"Information: Retention re-evaluation: 1 policies held, 0 armed this pass, {total - 1} unchanged", pass1bLog.Joined, StringComparison.Ordinal);

            /* ── MOVE THE COVERAGE, the way a backfill does: materialize the hour the seeded row is in on BOTH
                  successor hourlies, then their dailies. #3653 LC: RawTierCoverage for query_stats now requires
                  BOTH query_stats_interval_hourly AND query_stats_db_interval_hourly; the legacy
                  query_stats_hourly and query_stats_daily are frozen and no longer named by the coverage gate.
                  The successor dailies must also be refreshed so the hourlies' OWN retention policies
                  (which gate on the daily consumers) do not go from ARMED to RE-HELD when the hourlies gain
                  data — a re-hold would change the (armed 1, unchanged total-1) tally to (armed 1, re-held 2,
                  unchanged total-3). This mirrors what the original code did with the legacy daily. ── */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, seeded.AddDays(-1), seeded.AddDays(1), ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, seeded.AddDays(-1), seeded.AddDays(1), ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, seeded.AddDays(-2), seeded.AddDays(2), ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDbIntervalDailyView, seeded.AddDays(-2), seeded.AddDays(2), ct);

            /* ── PASS 2: the hourly tick after the backfill. The hold releases: armed-this-pass 1, held 0,
                  everything else unchanged — and no restart happened between pass 1 and here. ── */
            var pass2Log = new CapturingTestLogger();
            var pass2 = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, pass2Log, TimescaleSupport.RetentionSweepPass.Periodic, ct);
            Assert.True((pass2.Held, pass2.Armed, pass2.Unchanged, pass2.ReHeld, pass2.Failed) == (0, 1, total - 1, 0, 0),
                $"pass 2 expected (held 0, armed 1, unchanged {total - 1}, re-held 0, failed 0), got ({pass2.Held}, {pass2.Armed}, {pass2.Unchanged}, {pass2.ReHeld}, {pass2.Failed}); {pass2Log.Joined}");
            /* #4299 (d′): query_stats never schedules again — the armed verdict is the config key. */
            Assert.False(await ScheduledAsync(connection, HeldRelation, ct), "a raw relation stays unscheduled even once armed (#4299 d′)");
            Assert.True(await RawArmedAsync(connection, HeldRelation, ct), "the policy must be ARMED once its consumer covers everything it holds");
            // #3653 LC: coverage is now string.Join(" + ", [query_stats_interval_hourly, query_stats_db_interval_hourly])
            Assert.Contains($"Information: Retention policy for {HeldRelation} ARMED - {TimescaleSupport.QueryStatsIntervalHourlyView} + {TimescaleSupport.QueryStatsDbIntervalHourlyView} now covers everything it holds", pass2Log.Joined, StringComparison.Ordinal);
            Assert.Contains($"Information: Retention re-evaluation: 0 policies held, 1 armed this pass, {total - 1} unchanged", pass2Log.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("Warning:", pass2Log.Joined, StringComparison.Ordinal);

            /* ── PASS 3: idempotence. The quiet hour — and the line is still written, with zeros, which is the
                  whole of the #3756 point: this pass's absence would look exactly like this pass never running. ── */
            var pass3Log = new CapturingTestLogger();
            var pass3 = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, pass3Log, TimescaleSupport.RetentionSweepPass.Periodic, ct);
            Assert.True((pass3.Held, pass3.Armed, pass3.Unchanged, pass3.ReHeld, pass3.Failed) == (0, 0, total, 0, 0),
                $"pass 3 expected (held 0, armed 0, unchanged {total}, re-held 0, failed 0), got ({pass3.Held}, {pass3.Armed}, {pass3.Unchanged}, {pass3.ReHeld}, {pass3.Failed}); {pass3Log.Joined}");
            Assert.Contains($"Information: Retention re-evaluation: 0 policies held, 0 armed this pass, {total} unchanged", pass3Log.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("ARMED -", pass3Log.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("HELD", pass3Log.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("Warning:", pass3Log.Joined, StringComparison.Ordinal);
            Assert.Equal(1, CountOf(pass3Log.Joined, "Information:"));

            /* The tally's invariant held on every pass. */
            foreach (var tally in new[] { start, pass1, pass1b, pass2, pass3 })
            {
                Assert.Equal(total, tally.Evaluated + tally.Failed);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);

                foreach (var relation in RetentionRelations)
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }

                await batch.DropContinuousAggregatesAsync(
                    (await ExistingCaggsAsync(cleanup, cleanupCt)).Except(preexistingCaggs, StringComparer.Ordinal), cleanupCt);

                using var unseed = new NpgsqlCommand(
                    "DELETE FROM collect.query_stats WHERE server_name = '" + Seed + "'", cleanup) { CommandTimeout = PolicyReadTimeoutSeconds };
                await unseed.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>The flag the sweep's prior read reads, through the shipped statement — so the test and the
    /// product cannot disagree about which row is the policy.</summary>
    private static async Task<bool> ScheduledAsync(NpgsqlConnection connection, string relation, System.Threading.CancellationToken ct)
    {
        using var read = new NpgsqlCommand(TimescaleSupport.RetentionPolicyScheduledSql(relation), connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        var flag = await read.ExecuteScalarAsync(ct);
        Assert.True(flag is bool, $"no policy_retention job found for collect.{relation}");
        return (bool)flag!;
    }

    /// <summary>#4299 (d′): query_stats is a raw relation now — its armed verdict is read off
    /// config->>'darling_armed' through the shipped RawArmedStateSql, never off scheduled (which this pass
    /// converges to false unconditionally). Mirrors ScheduledAsync's shape for the config-key verdict.</summary>
    private static async Task<bool> RawArmedAsync(NpgsqlConnection connection, string relation, System.Threading.CancellationToken ct)
    {
        using var read = new NpgsqlCommand(TimescaleSupport.RawArmedStateSql(relation), connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        var flag = await read.ExecuteScalarAsync(ct);
        Assert.True(flag is bool, $"no policy_retention job found for collect.{relation}");
        return (bool)flag!;
    }



    /// <summary>A manual refresh over a window — what <c>--backfill-rollups</c> does one chunk at a time. Outside
    /// a transaction because TimescaleDB requires it; the window is generous on both sides so the bucket the
    /// seeded row falls in is whole.</summary>
    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, System.Threading.CancellationToken ct)
    {
        using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string[]> ExistingCaggsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect'", connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        using var reader = await command.ExecuteReaderAsync(ct);
        var names = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
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
}
