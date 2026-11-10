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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8528: what the Capture Down detail says, and when the alert holds or closes, from what the service's own ensure of the capture
/// sessions found. The failures are built by the product's own fault builders, so the pinned text is what a real ensure leaves.
/// </summary>
public sealed class CaptureDownEnsureFindingsTests
{
    private const int ServerId = 852810;
    private const string Name = "FINDINGS-SRV";
    private const string Grant = "ALTER ANY EVENT SESSION";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static int CountOf(string? text, string part) => (text ?? string.Empty).Split(part).Length - 1;

    [Fact]
    public async Task ASessionTheEnsureCouldNotStart_QuotesTheServersTextOnce_AndNotTheServicesOwnSentence()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        const string said = "Cannot alter the event session because the ring_buffer target is in an invalid state (error 25728).";
        var failure = DarlingXeSessions.CaptureFaultFor(new InvalidOperationException(said), "deadlock");

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" },
            CaptureDownFindingSeams.Report(failures: new[] { ("deadlocks", failure) }), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("The service found the Deadlock session stopped or absent and could not start it. The server said: " + said, fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(fired.DetailText, said));
        Assert.DoesNotContain("could not be created or started", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain(Grant, fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARefusedCreate_InTheIntervalBeforeItsPermissionsRow_NamesTheGrantOnce_AndQuotesOnlyTheServer()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        const string said = "The user does not have permission to perform this action.";
        var failure = DarlingXeSessions.CaptureFaultFor(
            LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 16, said), "blocked process");
        Assert.True(failure.IsPermission);

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Blocking" },
            CaptureDownFindingSeams.Report(failures: new[] { ("blocked_process_report", failure) }), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("lacks the permission to create Extended Events sessions", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("The server said: " + said, fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(fired.DetailText, Grant));
        Assert.Equal(1, CountOf(fired.DetailText, said));
        Assert.DoesNotContain("could not start it", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheQuotedServerText_IsOneLine_AndCut()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var said = "Cannot alter the event session.\r\nThe target is in an invalid state.\n" + new string('x', 4000);
        var failure = DarlingXeSessions.CaptureFaultFor(new InvalidOperationException(said), "deadlock");

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" },
            CaptureDownFindingSeams.Report(failures: new[] { ("deadlocks", failure) }), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("The server said: Cannot alter the event session. The target is in an invalid state. xxx", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("\n", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("\r", fired.DetailText, StringComparison.Ordinal);
        Assert.True(CountOf(fired.DetailText, "x") < 400, "The server's text is cut to a few hundred characters in the page.");
    }

    [Fact]
    public async Task TwoCapturesDownForDifferentReasons_AreSentencesOfTheirOwn()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var failure = DarlingXeSessions.CaptureFaultFor(new InvalidOperationException("Cannot alter the event session"), "deadlock");

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Blocking", "Deadlock" },
            CaptureDownFindingSeams.Report(failures: new[] { ("deadlocks", failure) }), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("The Blocking and Deadlock Extended Events sessions are missing. The service found the Deadlock session", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("could not start it. The server said: Cannot alter the event session. The collector found the Blocking session stopped, absent or unreadable.", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoCheckSinceTheRead_ClaimsNothingAboutWhatTheCheckFound()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Client timeouts" }, CaptureDownFindingSeams.Report(), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("found the Client timeouts session stopped, absent or unreadable", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("next check", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("found nothing", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACheckInTheSameRunThatFoundNothingToStart_IsSaid()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var report = CaptureDownFindingSeams.With(CaptureDownFindingSeams.Report(), "CheckedAfterMissingRead", "attention");

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Client timeouts" }, report, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("found the Client timeouts session stopped, absent or unreadable, and the service's check of the sessions in that same run found nothing to start", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASessionTheLastCheckStarted_ThatIsStoppedAgain_SaysTheServiceStartedIt()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var report = CaptureDownFindingSeams.With(CaptureDownFindingSeams.Report(), "StartedAtLastEnsure", "attention");

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Client timeouts" }, report, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("The service started the Client timeouts session at its last check, and its collector has found it stopped, absent or unreadable again since", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("starts it again at its next check", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("found nothing", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ALastCheckThatDidNotComplete_SaysSo_AndWhenItIsAskedAgain()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var report = CaptureDownFindingSeams.With(CaptureDownFindingSeams.Report(), "TransientAtLastEnsure", "deadlocks");

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" }, report, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("The service's last check of the Deadlock session did not complete", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("It checks again within 5 minutes.", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("found nothing", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("could not start it", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public void ATransientClientTimeoutEnsureFailure_IsNotAFailureToStart_AndTheOthersCarryTheServersText()
    {
        const string said = "The user does not have permission to perform this action.";

        var transient = new DarlingXeSessions.CaptureEnsureOutcome();
        Assert.True(CaptureDownFindingSeams.RecordAttentionFailure(transient, new TimeoutException("Execution Timeout Expired.")));
        Assert.Null(transient.Attention);
        Assert.True(CaptureDownFindingSeams.Flag(transient, "AttentionTransient"));
        Assert.True(transient.TransientFailure, "A transient client-timeout ensure failure is asked again after the retry interval, as the other two are.");

        var refused = new DarlingXeSessions.CaptureEnsureOutcome();
        Assert.False(CaptureDownFindingSeams.RecordAttentionFailure(refused, LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 16, said)));
        Assert.True(refused.Attention!.IsPermission);
        Assert.Equal(said, CaptureDownFindingSeams.ServerTextOf(refused.Attention));

        var broken = new DarlingXeSessions.CaptureEnsureOutcome();
        Assert.False(CaptureDownFindingSeams.RecordAttentionFailure(broken, new InvalidOperationException("Cannot alter the event session.")));
        Assert.False(broken.Attention!.IsPermission);
        Assert.Equal("Cannot alter the event session.", CaptureDownFindingSeams.ServerTextOf(broken.Attention));
    }

    [Fact]
    public async Task TheStorePass_HandsTheReportItIsGivenToTheCaptureDownJudgment()
    {
        var held = new DarlingSelfAlertTests.Harness();
        var e = held.Build();
        e.CaptureDownReadOverride = (_, _, _) => Task.FromResult<IReadOnlyList<string>>(new[] { "Client timeouts" });
        e.CaptureDownEngineReadOverride = (_, _, _) => throw new InvalidOperationException("the servers read faulted");

        /* The path the service runs: the fleet pass hands the server's report to the store pass, which judges the read with it. */
        await e.EvaluateStoreAlertsAsync(
            null!, ServerId, Name, connected: true, Ct,
            captureSessions: (CaptureSessionEnsureReport)CaptureDownSeams.Report(restarted: new[] { "attention" }));

        Assert.DoesNotContain(held.Deliverer.Outcomes, o => o.MetricName == "Capture Down");

        var fires = new DarlingSelfAlertTests.Harness();
        var e2 = fires.Build();
        e2.CaptureDownReadOverride = (_, _, _) => Task.FromResult<IReadOnlyList<string>>(new[] { "Client timeouts" });
        e2.CaptureDownEngineReadOverride = (_, _, _) => throw new InvalidOperationException("the servers read faulted");

        await e2.EvaluateStoreAlertsAsync(null!, ServerId, Name, connected: true, Ct);

        Assert.Single(fires.Deliverer.Outcomes, o => o.MetricName == "Capture Down");

        /* A report from an earlier pass does not carry over to a pass that is handed none. */
        await e.EvaluateStoreAlertsAsync(null!, ServerId, Name, connected: true, Ct);

        Assert.Single(held.Deliverer.Outcomes, o => o.MetricName == "Capture Down");
    }

    [Fact]
    public async Task TheServerScopedEnsures_ReportAStartOnlyForASessionTheyStartedOrCreated()
    {
        var calls = new List<string>();
        Func<Task> start = () => { calls.Add("start"); return Task.CompletedTask; };
        Func<Task> create = () => { calls.Add("create"); return Task.CompletedTask; };

        /* The probe's scalar: 0 is a session that exists and is not running, no row is no session, 1 is a running one. */
        Assert.True(await CaptureDownFindingSeams.StartOrCreateAsync(0, start, create));
        Assert.Equal(new[] { "start" }, calls);

        calls.Clear();
        Assert.True(await CaptureDownFindingSeams.StartOrCreateAsync(null, start, create));
        Assert.Equal(new[] { "create" }, calls);

        calls.Clear();
        Assert.False(await CaptureDownFindingSeams.StartOrCreateAsync(1, start, create));
        Assert.Empty(calls);

        /* A start the server refused reports nothing: the failure is the caller's to record. */
        Func<Task> refused = () => throw new InvalidOperationException("Cannot alter the event session.");
        await Assert.ThrowsAsync<InvalidOperationException>(() => CaptureDownFindingSeams.StartOrCreateAsync(0, refused, create));
    }

    [Fact]
    public async Task AnEnsureAtConnect_LeavesTheSessionsItStartedAwaitingTheirRead()
    {
        var server = CaptureDownFindingSeams.LoopState("findings-connect");
        var outcome = new DarlingXeSessions.CaptureEnsureOutcome();
        outcome.Started.Add("deadlocks");

        /* What the connect path and the sweep's ensure both hand the loop state. */
        CaptureDownFindingSeams.ApplyEnsure(server, outcome, DateTime.UtcNow);

        var h = new DarlingSelfAlertTests.Harness();
        await CaptureDownSeams.ApplyAsync(h.Build(), ServerId, Name, new[] { "Deadlock" }, CaptureDownSeams.ReportOf(server), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ASessionAwaitingItsRead_IsHeldForTwoOfItsCollectorsIntervals_AndNoLonger()
    {
        var server = CaptureDownFindingSeams.LoopState("findings-bound");
        CaptureDownSeams.MarkRestarted(server, "deadlocks");
        var now = DateTime.UtcNow;
        EffectiveSchedule FiveMinutes(string _) => new(FrequencyMinutes: 5, RetentionDays: 30, Enabled: true);

        var held = new DarlingSelfAlertTests.Harness();
        await CaptureDownSeams.ApplyAsync(held.Build(), ServerId, Name, new[] { "Deadlock" },
            CaptureDownFindingSeams.ReportAt(server, now.AddMinutes(9), FiveMinutes), Ct);
        Assert.Empty(held.Deliverer.Outcomes);

        /* The collector wrote no row in two of its intervals: the hold does not outlive the read it was waiting for. */
        var lapsed = new DarlingSelfAlertTests.Harness();
        await CaptureDownSeams.ApplyAsync(lapsed.Build(), ServerId, Name, new[] { "Deadlock" },
            CaptureDownFindingSeams.ReportAt(server, now.AddMinutes(11), FiveMinutes), Ct);
        Assert.Single(lapsed.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ACollectorThatIsTurnedOff_IsNeitherDownNorHeld_AndDoesNotKeepAnotherCapturesRestoreBack()
    {
        var server = CaptureDownFindingSeams.LoopState("findings-off");
        CaptureDownSeams.MarkRestarted(server, "deadlocks");
        EffectiveSchedule DeadlocksOff(string collector) => new(FrequencyMinutes: 5, RetentionDays: 30, Enabled: collector != "deadlocks");
        var report = CaptureDownFindingSeams.ReportAt(server, DateTime.UtcNow, DeadlocksOff);

        /* Its last SESSION_MISSING row stays the newest for good: that is not a capture that is down. */
        var quiet = new DarlingSelfAlertTests.Harness();
        await CaptureDownSeams.ApplyAsync(quiet.Build(), ServerId, Name, new[] { "Deadlock" }, report, Ct);
        Assert.Empty(quiet.Deliverer.Outcomes);

        /* An active Capture Down for another capture closes when that capture reads again, whatever the turned-off collector's row says. */
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Blocking" }, CaptureDownSeams.Report(), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" }, report, Ct);

        Assert.Single(h.Deliverer.Outcomes);
        var restored = Assert.Single(h.History.Records, r => r.MetricName == "Capture Restored");
        Assert.Contains("Blocking capture is running again", restored.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACaptureDownForACollectorThatIsThenTurnedOff_IsClearedNotRestored()
    {
        var server = CaptureDownFindingSeams.LoopState("findings-cleared");
        EffectiveSchedule DeadlocksOff(string collector) => new(FrequencyMinutes: 5, RetentionDays: 30, Enabled: collector != "deadlocks");

        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" }, CaptureDownSeams.Report(), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Deadlock" },
            CaptureDownFindingSeams.ReportAt(server, DateTime.UtcNow, DeadlocksOff), Ct);

        var cleared = Assert.Single(h.History.Records, r => AlertMetricClassifier.IsResolution(r.MetricName));
        Assert.Equal("Capture Cleared", cleared.MetricName);
        Assert.Contains("its collector is turned off", cleared.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("running again", cleared.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACaptureWhoseRowMovesFromSessionMissingToPermissions_IsClearedNotRestored()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, new[] { "Blocking", "Deadlock" }, CaptureDownSeams.Report(), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* The hourly ensure was refused the create of the blocked-process session, and that collector's next row is PERMISSIONS:
           the read names neither capture now. The deadlock session did come back. */
        var refused = DarlingXeSessions.CaptureFaultFor(
            LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 16, "The user does not have permission to perform this action."), "blocked process");
        await CaptureDownSeams.ApplyAsync(e, ServerId, Name, Array.Empty<string>(),
            CaptureDownFindingSeams.Report(failures: new[] { ("blocked_process_report", refused) }), Ct);

        var cleared = Assert.Single(h.History.Records, r => AlertMetricClassifier.IsResolution(r.MetricName));
        Assert.Equal("Capture Cleared", cleared.MetricName);
        Assert.Contains("The Blocking capture is not running", cleared.DetailText, StringComparison.Ordinal);
        Assert.Contains("refused the permission", cleared.DetailText, StringComparison.Ordinal);
        Assert.Contains("The Deadlock capture is running again", cleared.DetailText, StringComparison.Ordinal);
    }
}

/// <summary>
/// #8528: the capture run's own read. A run that finds its session stopped ensures the sessions and reads again before it writes
/// its row, whichever collector ran first; a session that stays stopped is ensured at the read once and not on every run; and a
/// run whose ensure failed with a transient error is held until the retry, and fires when the retry fails too. Against a test store.
/// </summary>
[Collection("live-postgres")]
public sealed class CaptureDownReadEnsureLiveTests
{
    private const int RereadServerId = -852811;
    private const int StaysStoppedServerId = -852812;
    private const int TransientServerId = -852813;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ACaptureRunThatFindsItsSessionStopped_EnsuresAndReadsAgain_BeforeItWritesItsRow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capture read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, RereadServerId, "findings-reread", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var fixture = Fixture.Build(postgres, RereadServerId, "findings-reread");

            /* The sessions were ensured inside the hour and nothing has observed the instance's new identity: no wait_stats or
               cpu_utilization run came before this one. The session is stopped until the ensure starts it. */
            fixture.Server.XeSessionsEnsuredAtUtc = DateTime.UtcNow;
            fixture.SessionStoppedUntilEnsured = true;

            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "attention", RereadServerId, fixture.Effective("attention"), ct);

            Assert.Equal(new[] { "read", "ensure", "read" }, fixture.Order);
            var row = Assert.Single(await ReadRowsAsync(connection, RereadServerId, "attention", ct));
            Assert.Equal("SUCCESS", row);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ASessionThatStaysStopped_IsEnsuredAtTheReadOnce_AndTheDetailSaysTheCheckFoundNothingToStart()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capture read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, StaysStoppedServerId, "findings-stopped", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var fixture = Fixture.Build(postgres, StaysStoppedServerId, "findings-stopped");
            fixture.Server.XeSessionsEnsuredAtUtc = DateTime.UtcNow;
            fixture.SessionStopped = true;

            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "attention", StaysStoppedServerId, fixture.Effective("attention"), ct);
            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "attention", StaysStoppedServerId, fixture.Effective("attention"), ct);

            /* The first run ensures and reads again; the second, in the same run of SESSION_MISSING rows, only reads. */
            Assert.Equal(new[] { "read", "ensure", "read", "read" }, fixture.Order);
            Assert.Equal(new[] { "SESSION_MISSING", "SESSION_MISSING" }, await ReadRowsAsync(connection, StaysStoppedServerId, "attention", ct));

            var h = new DarlingSelfAlertTests.Harness();
            await CaptureDownSeams.ApplyAsync(h.Build(), StaysStoppedServerId, "findings-stopped", new[] { "Client timeouts" }, CaptureDownSeams.ReportOf(fixture.Server), ct);
            var fired = Assert.Single(h.Deliverer.Outcomes);
            Assert.Contains("the service's check of the sessions in that same run found nothing to start", fired.DetailText, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ACaptureRunWhoseEnsureFailedTransiently_IsHeldUntilTheRetry_AndFiresWhenTheRetryFailsToo()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capture read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, TransientServerId, "findings-transient", ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);

            /* The real ensure, with a connection it cannot open: a failure that says nothing about the sessions, asked again after the retry interval. */
            var fixture = Fixture.Build(postgres, TransientServerId, "findings-transient", connectionString: "this is not a connection string");
            fixture.Runner.XeEnsureOverrideForTests = null;
            fixture.Server.XeSessionsEnsuredAtUtc = null;
            fixture.SessionStopped = true;

            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "deadlocks", TransientServerId, fixture.Effective("deadlocks"), ct);

            Assert.Equal(new[] { "SESSION_MISSING" }, await ReadRowsAsync(connection, TransientServerId, "deadlocks", ct));
            Assert.True(fixture.Server.CaptureEnsure.DeadlockTransient);

            /* Nothing is known about the session until the retry: no page for a start the service has not been able to try. */
            var held = new DarlingSelfAlertTests.Harness();
            await CaptureDownSeams.ApplyAsync(held.Build(), TransientServerId, "findings-transient", new[] { "Deadlock" }, CaptureDownSeams.ReportOf(fixture.Server), ct);
            Assert.Empty(held.Deliverer.Outcomes);

            /* The retry runs and fails the same way: the outage is real, and the capture fires. */
            await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(fixture.Server, fixture.Runner, DateTime.UtcNow.AddMinutes(6), NullLogger.Instance, ct);

            var afterRetry = new DarlingSelfAlertTests.Harness();
            await CaptureDownSeams.ApplyAsync(afterRetry.Build(), TransientServerId, "findings-transient", new[] { "Deadlock" }, CaptureDownSeams.ReportOf(fixture.Server), ct);
            var fired = Assert.Single(afterRetry.Deliverer.Outcomes);
            Assert.Contains("did not complete", fired.DetailText, StringComparison.Ordinal);

            /* The next run of the same streak is not held a second time. */
            await fixture.Worker.RunOnLoadAsync(fixture.Server, fixture.Runner, "deadlocks", TransientServerId, fixture.Effective("deadlocks"), ct);

            var nextRun = new DarlingSelfAlertTests.Harness();
            await CaptureDownSeams.ApplyAsync(nextRun.Build(), TransientServerId, "findings-transient", new[] { "Deadlock" }, CaptureDownSeams.ReportOf(fixture.Server), ct);
            Assert.Single(nextRun.Deliverer.Outcomes);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private sealed class Fixture
    {
        public required DarlingWorker Worker { get; init; }
        public required DarlingCollectorRunner Runner { get; init; }
        public required DarlingWorker.ServerLoopState Server { get; init; }
        public List<string> Order { get; } = new();

        /// <summary>The read finds the session stopped, whatever the ensure does.</summary>
        public bool SessionStopped { get; set; }

        /// <summary>The read finds the session stopped until an ensure has run, as after a failover the service has not seen.</summary>
        public bool SessionStoppedUntilEnsured { get; set; }

        public EffectiveSchedule Effective(string collector) => StoreConfigProvider.ResolveSchedule(collector, Server.Config.ServerId, Worker.ScheduleOverridesForTest);

        public static Fixture Build(NpgsqlDataSource postgres, int serverId, string host, string? connectionString = null)
        {
            var config = new MonitoredServer { Name = host, Host = host };
            var runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = connectionString ?? $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
                Target = new CollectorTargetInfo(),
                StorageName = host,
                ServerId = serverId,
            };
            var server = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
            var runner = new DarlingCollectorRunner(
                postgres,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => new List<string>(),
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => "0a1b2c3d");
            var worker = new DarlingWorker(
                NullLogger<DarlingWorker>.Instance,
                NullLoggerFactory.Instance,
                new McpRuntimeState(),
                new WebRuntimeState(),
                new MonitoredServerRegistryState(),
                new CollectorRuntimeState(),
                new WebTlsCertificateState(),
                new BaselineCache(),
                new ReadLatencyAccumulator())
            {
                StoreForTests = postgres,
            };

            /* One list records both: the ensure the runner's seam stands in for, and the read the dispatch seam stands in for. */
            var fixture = new Fixture { Worker = worker, Runner = runner, Server = server };
            runner.XeEnsureOverrideForTests = (_, _) =>
            {
                fixture.Order.Add("ensure");
                fixture.SessionStoppedUntilEnsured = false;
                return Task.CompletedTask;
            };
            worker.DispatchOverrideForTests = (_, _, _) =>
            {
                fixture.Order.Add("read");
                if (fixture.SessionStopped || fixture.SessionStoppedUntilEnsured)
                {
                    throw CaptureDownFindingSeams.SessionMissing("The XE session is not running.");
                }

                return Task.FromResult(new CollectorRunResult(0, 0, 0, Array.Empty<CollectorMeasurement>()));
            };
            return fixture;
        }
    }

    private static async Task<List<string>> ReadRowsAsync(NpgsqlConnection connection, int serverId, string collector, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(
            "SELECT status FROM collection_log WHERE server_id = $1 AND collector_name = $2 ORDER BY collection_time", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(collector);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { RereadServerId, StaysStoppedServerId, TransientServerId })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", id);
        }
    }
}

/// <summary>
/// #8528: the members these tests reach by name, so they compile against a service without them and fail at their asserts there.
/// </summary>
internal static class CaptureDownFindingSeams
{
    private const BindingFlags Any = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

    /// <summary>A report whose failures are the product's own failure records, as a real ensure builds them.</summary>
    public static object Report((string Collector, DarlingXeSessions.XeEnsureFailure Failure)[]? failures = null)
    {
        var map = new Dictionary<string, DarlingXeSessions.XeEnsureFailure>(StringComparer.OrdinalIgnoreCase);
        foreach (var (collector, failure) in failures ?? Array.Empty<(string, DarlingXeSessions.XeEnsureFailure)>())
        {
            map[collector] = failure;
        }

        return new CaptureSessionEnsureReport(Array.Empty<string>(), map);
    }

    /// <summary>Sets one of the report's collector lists: what the last ensure did for those collectors.</summary>
    public static object With(object report, string property, params string[] collectors)
    {
        var member = report.GetType().GetProperty(property, Any);
        Assert.True(member is not null, $"CaptureSessionEnsureReport has no {property}: the Capture Down detail cannot say what the service's last check of the sessions did.");
        member!.SetValue(report, (IReadOnlyCollection<string>)collectors);
        return report;
    }

    /// <summary>A loop state for a server with nothing ensured yet.</summary>
    public static DarlingWorker.ServerLoopState LoopState(string host) =>
        new() { Config = new MonitoredServer { Name = host, Host = host } };

    /// <summary>The loop state's report as of a given time, with each capture collector's effective schedule.</summary>
    public static object ReportAt(DarlingWorker.ServerLoopState server, DateTime utcNow, Func<string, EffectiveSchedule> scheduleOf)
    {
        var method = typeof(DarlingWorker.ServerLoopState).GetMethod("CaptureSessionReportAt", Any);
        Assert.True(method is not null, "ServerLoopState has no CaptureSessionReportAt(utcNow, scheduleOf): a session awaiting its read is held with no time bound, and a collector that is turned off is judged by its last row.");
        return method!.Invoke(server, new object?[] { utcNow, scheduleOf })!;
    }

    /// <summary>What every ensure of the capture sessions leaves on the loop state, as the connect path and the sweep hand it over.</summary>
    public static void ApplyEnsure(DarlingWorker.ServerLoopState server, DarlingXeSessions.CaptureEnsureOutcome outcome, DateTime utcNow)
    {
        var method = typeof(DarlingWorker).GetMethod("ApplyCaptureEnsure", Any);
        Assert.True(method is not null, "DarlingWorker has no ApplyCaptureEnsure: the connect path's ensure does not mark the sessions it started.");
        method!.Invoke(null, new object?[] { server, outcome, null, utcNow });
    }

    /// <summary>What a failed server-scoped client-timeout ensure leaves on the outcome. Returns whether the failure was transient.</summary>
    public static bool RecordAttentionFailure(DarlingXeSessions.CaptureEnsureOutcome outcome, Exception ex)
    {
        var method = typeof(DarlingXeSessions).GetMethod("RecordOnPremAttentionFailure", Any);
        Assert.True(method is not null, "DarlingXeSessions has no RecordOnPremAttentionFailure: a transient client-timeout ensure failure is recorded as a session the service could not start.");
        return (bool)method!.Invoke(null, new object?[] { outcome, DarlingXeSessions.AttentionFaultFor(ex), ex })!;
    }

    public static bool Flag(object target, string property)
    {
        var member = target.GetType().GetProperty(property, Any);
        Assert.True(member is not null, $"{target.GetType().Name} has no {property}.");
        return (bool)member!.GetValue(target)!;
    }

    /// <summary>The server's own text on a failure, apart from the service's sentence.</summary>
    public static string? ServerTextOf(DarlingXeSessions.XeEnsureFailure failure)
    {
        var member = typeof(DarlingXeSessions.XeEnsureFailure).GetProperty("ServerText", Any);
        Assert.True(member is not null, "XeEnsureFailure has no ServerText: the detail can only quote the service's own sentence as the server's.");
        return (string?)member!.GetValue(failure);
    }

    /// <summary>The step the three server-scoped ensures share after their probe, given the probe's scalar.</summary>
    public static async Task<bool> StartOrCreateAsync(object? probe, Func<Task> start, Func<Task> create)
    {
        var stateOf = typeof(DarlingXeSessions).GetMethod("ServerSessionStateOf", Any);
        var step = typeof(DarlingXeSessions).GetMethod("StartOrCreateServerSessionAsync", Any);
        Assert.True(stateOf is not null && step is not null, "The three server-scoped ensures share no probe-and-start step: nothing pins whether a start or a create is reported.");
        var state = stateOf!.Invoke(null, new[] { probe });
        try
        {
            return await (Task<bool>)step!.Invoke(null, new[] { state, start, create })!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            throw ex.InnerException;
        }
    }

    /// <summary>The exception a capture read raises for a session that is not running, as the run's own read wraps it.</summary>
    public static Exception SessionMissing(string message)
    {
        var type = typeof(DarlingWorker).GetNestedType("DarlingXeSessionMissingException", BindingFlags.NonPublic);
        Assert.True(type is not null, "DarlingWorker has no DarlingXeSessionMissingException.");
        return (Exception)Activator.CreateInstance(type!, message)!;
    }
}
