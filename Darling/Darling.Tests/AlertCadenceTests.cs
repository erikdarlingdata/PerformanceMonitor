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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5642: the Darling alert evaluation and the same-statement-pileup finding run in the #5479 fleet pass, on the pass's
/// 30 s stamps, after the self-alerts, not after the collection sweep inside each server's body. A body that ran for
/// minutes (or never finished) used to hold every alert on its server back: the field case was a server whose sweep took
/// minutes, so its High CPU alert came a sweep late. These tests drive the real pass with seams in place of the store
/// reads and the engine; no test waits on a real interval (the stamps are reset by hand, as the #5479 tests do).
/// </summary>
public sealed class AlertCadenceTests
{
    private static string BodyOf(string source)
    {
        var bodyStart = source.IndexOf("private async Task ProcessServerSweepAsync(", StringComparison.Ordinal);
        Assert.True(bodyStart > 0, "the body was not found");
        var nextMember = Regex.Match(source.Substring(bodyStart + 100), @"\r?\n    (?:private|internal|public|protected)[^\r\n]*\(");
        Assert.True(nextMember.Success, "the member after the body was not found");
        return source.Substring(bodyStart, 100 + nextMember.Index);
    }

    private static string PassOf(string source)
    {
        var passStart = source.IndexOf("private async Task RunSelfAlertPassAsync(", StringComparison.Ordinal);
        var passEnd = source.IndexOf("Counts the collector slots that came due", passStart, StringComparison.Ordinal);
        Assert.True(passStart > 0 && passEnd > passStart, "the fleet pass was not found");
        return source.Substring(passStart, passEnd - passStart);
    }

    /// <summary>
    /// The field case: the server's body is still running (a slow sweep that has not ended) and its alert evaluation runs
    /// on the pass's cadence all the same, three times in three passes, while the body's task is still incomplete. On the
    /// old shape the evaluation was the last step of the body, so the counter stayed at zero for as long as the body ran.
    /// </summary>
    [Fact]
    public async Task AnAlertIsEvaluated_OnCadence_WhileASlowSweepIsStillRunning()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var evaluations = 0;
        worker.AlertServerOverride = async (_, _) =>
        {
            Interlocked.Increment(ref evaluations);
            await Task.Delay(50);
        };
        var server = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);
        var slowSweep = new TaskCompletionSource();
        server.InFlightSweep = slowSweep.Task;

        for (var round = 1; round <= 3; round++)
        {
            server.NextAlertSweep = DateTime.MinValue;
            Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
            await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, round);
            Assert.Equal(round, Volatile.Read(ref evaluations));
        }

        Assert.False(slowSweep.Task.IsCompleted, "the sweep must still be running for this to be the field case");

        /* Inside the 30 s window a tick evaluates nothing: the cadence is the stamp's, not the tick's. */
        Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 4);
        Assert.Equal(3, Volatile.Read(ref evaluations));
        slowSweep.SetResult();
    }

    /// <summary>
    /// At most one pass runs at a time, so a server's alert evaluation is never run by two threads: while the first
    /// evaluation is still going, the next ticks launch no second pass even though the stamp is due again.
    /// </summary>
    [Fact]
    public async Task AtMostOnePassRunsAtATime_SoAServersAlertEvaluationNeverOverlapsItself()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var release = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var concurrent = 0;
        var maxConcurrent = 0;
        var evaluations = 0;
        worker.AlertServerOverride = async (_, _) =>
        {
            var now = Interlocked.Increment(ref concurrent);
            int seen;
            while (now > (seen = Volatile.Read(ref maxConcurrent)) && Interlocked.CompareExchange(ref maxConcurrent, now, seen) != seen)
            {
            }

            Interlocked.Increment(ref evaluations);
            started.TrySetResult();
            await release.Task;
            Interlocked.Decrement(ref concurrent);
        };
        var server = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);

        Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        server.NextAlertSweep = DateTime.MinValue;
        Assert.False(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        Assert.False(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        Assert.Equal(1, Volatile.Read(ref evaluations));
        Assert.Equal(1, Volatile.Read(ref maxConcurrent));

        release.SetResult();
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 1);
        server.NextAlertSweep = DateTime.MinValue;
        Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 2);
        Assert.Equal(2, Volatile.Read(ref evaluations));
        Assert.Equal(1, Volatile.Read(ref maxConcurrent));
    }

    /// <summary>
    /// Within one server's step the order is self-alerts, then the alert evaluation, then the pileup finding: a slow
    /// alert read (a server under pressure) must never delay Collection Stopped. A server's alert step is also
    /// independent of the other servers' steps, so a stuck server leaves the rest evaluated.
    /// </summary>
    [Fact]
    public async Task SelfAlertsRunFirst_ThenTheAlertEvaluation_ThenThePileup()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var order = new List<string>();
        void Record(string step, string server)
        {
            lock (order)
            {
                order.Add(server + ":" + step);
            }
        }

        worker.SelfAlertServerOverride = (s, _) => { Record("self", s.Config.DisplayName); return Task.CompletedTask; };
        worker.AlertServerOverride = (s, _) => { Record("alert", s.Config.DisplayName); return Task.CompletedTask; };
        worker.PileupServerOverride = (s, _) => { Record("pileup", s.Config.DisplayName); return Task.CompletedTask; };
        var server = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);

        Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 1);

        Assert.Equal(["example-sql-01:self", "example-sql-01:alert", "example-sql-01:pileup"], order);
    }

    /// <summary>
    /// A self-alert that throws, or one that never returns, is the case that most needs the ordering to be real: the
    /// alert step of the same server is not reached for a never-returning self-alert (the pass is serial per server), but
    /// a throw is logged and the SAME server's alert step is still skipped only by that throw's own catch, never the
    /// other servers'. Pinned here: the other server is still evaluated.
    /// </summary>
    [Fact]
    public async Task OneServersThrowingSelfAlert_DoesNotKeepTheOtherServersAlertsFromRunning()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var alerted = new List<string>();
        worker.SelfAlertServerOverride = (s, _) =>
            s.Config.DisplayName == "example-sql-01" ? throw new InvalidOperationException("a self-alert read failed") : Task.CompletedTask;
        worker.AlertServerOverride = (s, _) =>
        {
            lock (alerted)
            {
                alerted.Add(s.Config.DisplayName);
            }

            return Task.CompletedTask;
        };
        var servers = new[]
        {
            SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true),
            SelfAlertFleetPassTests.MakeServer("example-sql-02", connected: true),
        };

        Assert.True(worker.TryStartSelfAlertPass(servers, TestContext.Current.CancellationToken));
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 1);

        Assert.Equal(["example-sql-02"], alerted);
    }

    /// <summary>
    /// A server whose runtime dropped is still evaluated, from the store, with its last known target (ruling 5): the
    /// server under enough pressure to drop is the one that needs the alert. A server that never connected has no target
    /// and is skipped. The seam sits behind the same runtime test the production call does.
    /// </summary>
    [Fact]
    public async Task AServerWhoseRuntimeDropped_IsStillEvaluated_AndOneThatNeverConnectedIsNot()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var alerted = new List<string>();
        worker.AlertServerOverride = (s, _) =>
        {
            lock (alerted)
            {
                alerted.Add(s.Config.DisplayName);
            }

            return Task.CompletedTask;
        };
        var dropped = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);
        dropped.LastKnownRuntime = dropped.Runtime;
        dropped.Runtime = null;
        var neverConnected = SelfAlertFleetPassTests.MakeServer("example-sql-02");

        Assert.Null(dropped.Runtime);
        Assert.NotNull(dropped.AlertRuntime);
        Assert.Null(neverConnected.AlertRuntime);

        Assert.True(worker.TryStartSelfAlertPass([dropped, neverConnected], TestContext.Current.CancellationToken));
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 1);

        Assert.Equal(["example-sql-01"], alerted);
    }

    /// <summary>
    /// The pileup finding rides the pass for a SQL Server target with a live runtime only: it reads the pileup shape out
    /// of sys.dm_exec_requests snapshots, which a dropped or never-connected server has no fresh copy of.
    /// </summary>
    [Fact]
    public async Task ThePileupStep_NeedsALiveRuntime()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var pileup = new List<string>();
        worker.PileupServerOverride = (s, _) =>
        {
            lock (pileup)
            {
                pileup.Add(s.Config.DisplayName);
            }

            return Task.CompletedTask;
        };
        var live = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);
        var dropped = SelfAlertFleetPassTests.MakeServer("example-sql-02", connected: true);
        dropped.LastKnownRuntime = dropped.Runtime;
        dropped.Runtime = null;

        Assert.True(worker.TryStartSelfAlertPass([live, dropped], TestContext.Current.CancellationToken));
        await SelfAlertFleetPassTests.WaitForPassToEndAsync(worker, 1);

        Assert.Equal(["example-sql-01"], pileup);
    }

    /// <summary>
    /// The pass makes store reads only (ruling 4): it names no collector, no body and no msdb read. The body reads the
    /// two msdb facts AFTER its collectors, into the per-server cache the engine's fetcher serves, and runs no alert or
    /// pileup call. A source-shape pin, RED at the base commit (where the body ran both calls after the collectors and
    /// the pass ran neither).
    /// </summary>
    [Fact]
    public void ThePassMakesNoMonitoredServerCall_AndTheBodyFillsTheMsdbCacheAfterTheCollectors()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var pass = PassOf(source);
        foreach (var forbidden in new[]
        {
            "FetchFailedJobsAsync", "FetchAgentJobStepNamesAsync", "RefreshMsdbCacheAsync", "RunDueCollectorsAsync",
            "ProcessServerSweepAsync", "TryConnectAsync", "ConnectionString",
        })
        {
            Assert.DoesNotContain(forbidden, pass, StringComparison.Ordinal);
        }

        var body = BodyOf(source);
        var collectors = body.IndexOf("await RunDueCollectorsAsync(server, runner", StringComparison.Ordinal);
        var refresh = body.IndexOf("await RefreshMsdbCacheAsync(server", StringComparison.Ordinal);
        Assert.True(collectors > 0 && refresh > collectors, "the msdb cache refresh must follow the collectors in the body");
        foreach (var gone in new[] { "EvaluateAlertsAsync", "EvaluateSameStatementPileupAsync", "NextAlertSweep", "NextPileupSweep" })
        {
            Assert.DoesNotContain(gone, body, StringComparison.Ordinal);
        }

        /* The engine's two live msdb feeds are the cache servers, never the live reads. */
        Assert.Contains("Task.FromResult(ServeFailedJobsFromCache(servers, serverKey, lookbackMinutes))", source, StringComparison.Ordinal);
        Assert.Contains("Task.FromResult(ServeStepNamesFromCache(servers, serverKey, keys))", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The cache serves the engine's failed-jobs feed with no I/O (the server's host here does not resolve, so a live
    /// read would throw or hang), records the lookback the body should read with, and degrades to an empty list once
    /// the cached list is older than ten minutes or when nothing has been read yet.
    /// </summary>
    [Fact]
    public void TheMsdbCache_ServesTheEngineWithNoIo_AndDegradesToEmptyWhenOldOrAbsent()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var server = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);
        var servers = new List<DarlingWorker.ServerLoopState> { server };
        var key = server.Runtime!.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture);

        /* Nothing read yet: empty, and the lookback is recorded so the body's next refresh reads with it. */
        Assert.Empty(worker.ServeFailedJobsFromCache(servers, key, 60));
        Assert.Equal(60, server.FailedJobsLookbackWanted);

        var job = new FailedJobInfo { JobName = "example nightly job", RunDateTime = DateTime.UtcNow };
        server.FailedJobsCache = new DarlingWorker.MsdbCacheEntry(DateTime.UtcNow.AddMinutes(-9), [job]);
        var fresh = worker.ServeFailedJobsFromCache(servers, key, 45);
        Assert.Equal("example nightly job", Assert.Single(fresh).JobName);
        Assert.Equal(45, server.FailedJobsLookbackWanted);

        server.FailedJobsCache = new DarlingWorker.MsdbCacheEntry(DateTime.UtcNow.AddMinutes(-11), [job]);
        Assert.Empty(worker.ServeFailedJobsFromCache(servers, key, 45));

        /* A server key nobody owns is an empty answer, not a throw. */
        Assert.Empty(worker.ServeFailedJobsFromCache(servers, "999999", 45));
    }

    /// <summary>The step-name resolver answers from the cache and marks an unresolved key as wanted for the body.</summary>
    [Fact]
    public void TheStepNameCache_AnswersWhatItHas_AndMarksTheRestWantedForTheBodysNextRefresh()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var server = SelfAlertFleetPassTests.MakeServer("example-sql-01", connected: true);
        var servers = new List<DarlingWorker.ServerLoopState> { server };
        var key = server.Runtime!.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture);

        var known = new AgentJobStepKey(new Guid("00000000-0000-0000-0000-000000000001"), 1);
        var unknown = new AgentJobStepKey(new Guid("00000000-0000-0000-0000-000000000002"), 2);
        server.StepNamesCache[known] = new AgentJobStepNames("example job", "example step");

        var found = worker.ServeStepNamesFromCache(servers, key, [known, unknown]);

        Assert.Single(found);
        Assert.True(found.ContainsKey(known));
        Assert.True(server.StepNamesWanted.ContainsKey(unknown));
        Assert.False(server.StepNamesWanted.ContainsKey(known));
    }

    /// <summary>
    /// Without a runtime-check ahead of the seam the dropped-server test above would pass on a pass that evaluated a
    /// never-connected server too; pin the production shape: the alert step reads <c>AlertRuntime</c> before it calls
    /// either the seam or the engine, and the engine call is the one that carries the engine and the config.
    /// </summary>
    [Fact]
    public void TheAlertStep_ChecksTheLastKnownRuntimeBeforeTheSeamAndTheEngine()
    {
        var pass = PassOf(ServerConnectBackoffTests.ReadWorkerSource());
        var runtimeCheck = pass.IndexOf("server.AlertRuntime is not null", StringComparison.Ordinal);
        var seam = pass.IndexOf("AlertServerOverride is { } evaluateAlerts", StringComparison.Ordinal);
        var engine = pass.IndexOf("EvaluateAlertsAsync(engine!, server, config!", StringComparison.Ordinal);
        Assert.True(runtimeCheck > 0 && seam > runtimeCheck && engine > seam, "the runtime test must come first, then the seam, then the engine");
        Assert.DoesNotContain("server.Runtime is null", pass.Substring(runtimeCheck, engine - runtimeCheck), StringComparison.Ordinal);
    }
}
