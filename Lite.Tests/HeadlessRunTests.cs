using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9142: Lite runs with no window, no tray icon and no signed-in user. The start decision, the tray as a channel that can be
/// missing, and the stop request through the upgrade handoff's exit channel.
/// </summary>
public sealed class HeadlessRunTests
{
    private const string NoTaskbar = "Failed to find an AppBar that matched the given criteria";

    private static AlertOutcome Outcome(string metric = "High CPU") =>
        new("1", "SQL01", metric, "97%", "90%", null, "detail", 97, 90, false, null,
            "Total CPU at 97% (threshold: 90%)");

    /// <summary>A deliverer whose tray toast is <paramref name="toast"/>, recording each send and what it was told about the tray.</summary>
    private static (LiteAlertDeliverer Deliverer, List<bool> SendsTrayShown, List<string> Warnings) Build(
        Func<bool> toast, AlertDelivery? answer = null)
    {
        var sends = new List<bool>();
        var warnings = new List<string>();
        var deliverer = new LiteAlertDeliverer(
            (_, _, _, _, _) => toast(),
            (_, _, _, _, _, _, _, _, _, _, _, trayShown, _) =>
            {
                sends.Add(trayShown);
                return Task.FromResult(answer);
            },
            _ => null,
            recordHeld: null,
            trayGate: new TrayChannelGate(warnings.Add));
        return (deliverer, sends, warnings);
    }

    // ---- the start decision

    [Fact]
    public void AHeadlessStart_ShowsNoWindowAndNoTray_AndStillStartsTheServices()
    {
        var plan = LiteStartupPlan.FromArgs(["--headless"]);

        Assert.False(plan.ShowsMainWindow);
        Assert.False(plan.CreatesTrayIcon);
        Assert.True(plan.StartsServices);
        Assert.True(plan.ConfirmsHandoffWithoutAsking);
    }

    [Fact]
    public void AnOrdinaryStart_ShowsTheWindowAndTheTray()
    {
        var plan = LiteStartupPlan.FromArgs(["--some-other-arg"]);

        Assert.True(plan.ShowsMainWindow);
        Assert.True(plan.CreatesTrayIcon);
        Assert.False(plan.ConfirmsHandoffWithoutAsking);
    }

    [Fact]
    public void AStopRequest_StartsNothing_EvenWithHeadlessBesideIt()
    {
        var plan = LiteStartupPlan.FromArgs(["--HEADLESS", "--Stop"]);

        Assert.False(plan.StartsServices);
        Assert.False(plan.ShowsMainWindow);
    }

    [Fact]
    public void AHeadlessStart_NeverBuildsTheTrayIcon_AndSaysSoOnce()
    {
        var built = 0;
        var info = new List<string>();

        var tray = TrayIconStarter.TryStart<object>(
            LiteStartupPlan.FromArgs(["--headless"]), () => { built++; return new object(); }, info.Add, _ => Assert.Fail("no warning expected"));

        Assert.Null(tray);
        Assert.Equal(0, built);
        Assert.Single(info);
    }

    [Fact]
    public void ATrayIconThatCannotBeCreated_IsOneWarningNamingTheCause_AndTheStartGoesOn()
    {
        var warnings = new List<string>();

        var tray = TrayIconStarter.TryStart<object>(
            LiteStartupPlan.FromArgs([]), () => throw new InvalidOperationException(NoTaskbar), _ => { }, warnings.Add);

        Assert.Null(tray);
        var warning = Assert.Single(warnings);
        Assert.Contains(NoTaskbar, warning);
    }

    [Fact]
    public void AnOrdinaryStart_KeepsTheTrayIconItBuilt()
    {
        var built = new object();

        var tray = TrayIconStarter.TryStart(LiteStartupPlan.FromArgs([]), () => built, _ => { }, _ => Assert.Fail("no warning expected"));

        Assert.Same(built, tray);
    }

    [Fact]
    public void ADialogIsSkippedAndLoggedOnce_ForEachThingSkipped_OnlyWhileHeadless()
    {
        var info = new List<string>();
        var wasHeadless = HeadlessUi.IsHeadless;
        var wasInfo = HeadlessUi.Info;
        try
        {
            HeadlessUi.Info = info.Add;

            HeadlessUi.IsHeadless = false;
            Assert.False(HeadlessUi.Skips("test dialog A"));
            Assert.Empty(info);

            HeadlessUi.IsHeadless = true;
            Assert.True(HeadlessUi.Skips("test dialog A"));
            Assert.True(HeadlessUi.Skips("test dialog A"));
            Assert.True(HeadlessUi.Skips("test dialog B"));
            Assert.Equal(2, info.Count);
        }
        finally
        {
            HeadlessUi.IsHeadless = wasHeadless;
            HeadlessUi.Info = wasInfo;
        }
    }

    // ---- the tray is one channel

    [Fact]
    public async Task ATrayThatThrows_DoesNotStopTheSend_AndIsLoggedOnceNotPerAlert()
    {
        var answer = AlertDelivery.FromFanout(new EmailFanoutResult(), muted: false, trayChannelPresent: false);
        var (deliverer, sends, warnings) = Build(() => throw new InvalidOperationException("balloon failed"), answer);

        AlertDelivery? first = null;
        for (var i = 0; i < 3; i++)
        {
            var delivery = await deliverer.DeliverAndReportAsync(Outcome(), TestContext.Current.CancellationToken);
            first ??= delivery;
            Assert.Same(answer, delivery);
        }

        Assert.Equal([false, false, false], sends);
        Assert.Single(warnings);
        Assert.Same(answer, first);
    }

    [Fact]
    public async Task WithNoTaskbar_TheSecondAlertLogsNothingNew_AndTheTrayIsNotTriedAgain()
    {
        var toastCalls = 0;
        var (deliverer, sends, warnings) = Build(() =>
        {
            toastCalls++;
            throw new Exception(NoTaskbar);
        });

        await deliverer.DeliverAsync(Outcome(), TestContext.Current.CancellationToken);
        var afterFirst = warnings.Count;
        await deliverer.DeliverAsync(Outcome(), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(Outcome("Blocking"), TestContext.Current.CancellationToken);

        Assert.Equal(1, afterFirst);
        Assert.Single(warnings);
        Assert.Contains("AppBar", warnings[0]);
        Assert.Equal(1, toastCalls);
        Assert.Equal(3, sends.Count);
    }

    [Fact]
    public async Task ASendIsToldWhetherABalloonReallyAppeared()
    {
        var shown = true;
        var (deliverer, sends, _) = Build(() => shown);

        await deliverer.DeliverAsync(Outcome(), TestContext.Current.CancellationToken);
        shown = false;
        await deliverer.DeliverAsync(Outcome(), TestContext.Current.CancellationToken);

        Assert.Equal([true, false], sends);
    }

    [Fact]
    public void AFailureThatIsNotAMissingTaskbar_IsLoggedOncePerMessage_AndTheChannelStaysOn()
    {
        var warnings = new List<string>();
        var gate = new TrayChannelGate(warnings.Add);

        Assert.False(gate.Run(() => throw new InvalidOperationException("popup race")));
        Assert.False(gate.Run(() => throw new InvalidOperationException("popup race")));
        Assert.False(gate.Unavailable);
        Assert.True(gate.Run(() => true));

        Assert.Single(warnings);
    }

    // ---- --stop

    [Fact]
    public void Stop_WithNothingListening_ExitsWithoutWaiting()
    {
        var code = StopCommand.Run(() => false, () => throw new InvalidOperationException("must not poll"),
            TimeSpan.FromSeconds(5), _ => { }, _ => { });

        Assert.Equal(StopCommand.ExitNotRunning, code);
    }

    [Fact]
    public void Stop_WaitsUntilTheInstanceHasExited()
    {
        var polls = 0;

        var code = StopCommand.Run(() => true, () => ++polls < 3, TimeSpan.FromSeconds(5), _ => { }, _ => { });

        Assert.Equal(StopCommand.ExitStopped, code);
        Assert.Equal(3, polls);
    }

    [Fact]
    public void Stop_ReportsAnInstanceThatDoesNotExit()
    {
        var pauses = 0;

        var code = StopCommand.Run(() => true, () => true, TimeSpan.FromSeconds(1), _ => pauses++, _ => { });

        Assert.Equal(StopCommand.ExitStillRunning, code);
        Assert.Equal(4, pauses);
    }

    [Fact]
    public async Task Stop_ReachesTheRunningInstancesShutdown_ThroughTheUpgradeHandoffChannel()
    {
        var unique = Guid.NewGuid().ToString("N");
        var shutdownRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var running = new SingleInstanceCoordinator(new SingleInstanceOptions
        {
            MutexName = $"HeadlessRunTests_Mutex_{unique}",
            ProcessName = "HeadlessRunTests",
            ExitEventName = $"HeadlessRunTests_Exit_{unique}",
            GracefulSelfExit = () => shutdownRan.TrySetResult(),
            Prompts = new HeadlessHandoffPrompts(),
        });

        Assert.True(running.TryBecomeOwner());
        running.EnableUpgradeHandoff();

        var code = StopCommand.Run(
            () => SingleInstanceSignal.TrySignal($"HeadlessRunTests_Exit_{unique}"),
            () => false, TimeSpan.FromSeconds(5), _ => { }, _ => { });

        Assert.Equal(StopCommand.ExitStopped, code);
        await shutdownRan.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ADisabledUpgradeHandoff_IsNotListening_AndAnEnableOpensItAgain()
    {
        var unique = Guid.NewGuid().ToString("N");
        var eventName = $"HeadlessRunTests_Exit_{unique}";
        var shutdownRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var running = new SingleInstanceCoordinator(new SingleInstanceOptions
        {
            MutexName = $"HeadlessRunTests_Mutex_{unique}",
            ProcessName = "HeadlessRunTests",
            ExitEventName = eventName,
            GracefulSelfExit = () => shutdownRan.TrySetResult(),
            Prompts = new HeadlessHandoffPrompts(),
        });
        Assert.True(running.TryBecomeOwner());

        running.DisableUpgradeHandoff();
        running.EnableUpgradeHandoff();
        running.DisableUpgradeHandoff();
        Assert.False(SingleInstanceSignal.TrySignal(eventName));
        Assert.False(shutdownRan.Task.IsCompleted);

        running.EnableUpgradeHandoff();
        Assert.True(SingleInstanceSignal.TrySignal(eventName));
        await shutdownRan.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Stop_BeforeTheInstanceIsPastItsStartup_FindsNobodyListening()
    {
        var unique = Guid.NewGuid().ToString("N");
        using var starting = new SingleInstanceCoordinator(new SingleInstanceOptions
        {
            MutexName = $"HeadlessRunTests_Mutex_{unique}",
            ProcessName = "HeadlessRunTests",
            ExitEventName = $"HeadlessRunTests_Exit_{unique}",
            Prompts = new HeadlessHandoffPrompts(),
        });
        Assert.True(starting.TryBecomeOwner());

        var code = StopCommand.Run(
            () => SingleInstanceSignal.TrySignal($"HeadlessRunTests_Exit_{unique}"),
            () => false, TimeSpan.FromSeconds(1), _ => { }, _ => { });

        Assert.Equal(StopCommand.ExitNotRunning, code);
    }

    [Fact]
    public void Stop_DoesNotWaitOnALiteInAnotherSession_ThatCannotHearTheSignal()
    {
        var processes = new[]
        {
            new StopCommand.RunningProcess(Id: 100, SessionId: 0),
            new StopCommand.RunningProcess(Id: 200, SessionId: 2),
        };

        Assert.False(StopCommand.OtherInstanceInSession(processes, selfId: 300, selfSession: 1));
    }

    [Fact]
    public void Stop_WaitsOnALiteInItsOwnSession_ButNotOnItself()
    {
        var processes = new[]
        {
            new StopCommand.RunningProcess(Id: 300, SessionId: 0),
            new StopCommand.RunningProcess(Id: 200, SessionId: 2),
        };

        Assert.False(StopCommand.OtherInstanceInSession(processes, selfId: 300, selfSession: 0));
        Assert.True(StopCommand.OtherInstanceInSession(processes.Append(new StopCommand.RunningProcess(Id: 400, SessionId: 0)), selfId: 300, selfSession: 0));
    }

    // ---- a headless start whose initialization fails

    [Fact]
    public void AHeadlessStartThatFailsToInitialize_EndsWithANonzeroExitCode_AndSkipsTheDialog()
    {
        int? exitCode = null;

        var ended = new LiteStartupPlan(Headless: true, Stop: false).EndsStartAfterInitFailure(code => exitCode = code);

        Assert.True(ended);
        Assert.NotNull(exitCode);
        Assert.NotEqual(0, exitCode);
    }

    [Fact]
    public void AStartWithAWindowThatFailsToInitialize_KeepsItsDialog_AndDoesNotShutDown()
    {
        var shutdowns = 0;

        var ended = new LiteStartupPlan(Headless: false, Stop: false).EndsStartAfterInitFailure(_ => shutdowns++);

        Assert.False(ended);
        Assert.Equal(0, shutdowns);
    }
}

/// <summary>
/// #9142: what <c>--stop</c> writes to the Lite log. It shares the process-wide log buffer with every other test.
/// </summary>
[Collection("app-logger-statics")]
public sealed class HeadlessStopLogTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "HeadlessStopLog_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_root))
                Directory.Delete(_root, recursive: true);
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    [Fact]
    public void TheStopOutcome_ReachesTheLogFile_WhenTheLogFolderExists()
    {
        var logs = Path.Combine(_root, "logs");
        Directory.CreateDirectory(logs);
        var tag = Guid.NewGuid().ToString("N");
        AppLogger.DrainBufferedLines();

        StopCommand.Run(() => false, () => false, TimeSpan.Zero, _ => { }, msg => AppLogger.Info("Stop", $"{msg} {tag}"));
        Assert.True(StopCommand.FlushLog(logs));

        var text = string.Concat(Directory.GetFiles(logs, "lite_*.log").Select(File.ReadAllText));
        Assert.Contains(tag, text);
    }

    [Fact]
    public void TheStopOutcome_NeverCreatesTheLogFolder()
    {
        var logs = Path.Combine(_root, "never");

        Assert.False(StopCommand.FlushLog(logs));
        Assert.False(Directory.Exists(logs));
    }
}
