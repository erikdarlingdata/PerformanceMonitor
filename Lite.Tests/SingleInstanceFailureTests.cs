using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9197: a start whose token is denied the single-instance mutex, with no other instance running, used to end in a quiet
/// surface-and-exit with exit code 0. These drive the coordinator through a mutex that is denied, and the start plan and log
/// that report it.
/// </summary>
public sealed class SingleInstanceFailureTests
{
    private sealed class NoPromptsOrPeers : IHandoffPrompts, IProcessInspector
    {
        public bool ConfirmCloseAndContinue(string oldVersion, string newVersion) => false;
        public bool ConfirmRestartAsAdmin(string oldVersion) => false;
        public void ShowMustCloseElevatedManually(string oldVersion) { }
        public Version? GetReleaseVersion(int pid) => null;
        public int? GetIntegrityLevel(int pid) => null;
        public int CurrentIntegrityLevel() => Win32ProcessInspector.IntegrityMedium;
        public bool CanElevateSameUser() => false;
    }

    private static Mutex DeniedMutex(string name, out bool createdNew) =>
        throw new UnauthorizedAccessException($"Access to the path '{name}' is denied.");

    private static Mutex IoFailureMutex(string name, out bool createdNew) =>
        throw new IOException("The handle is invalid.");

    private static SingleInstanceCoordinator Build(string processName, MutexFactory? factory, Action? surface = null)
    {
        var unique = Guid.NewGuid().ToString("N");
        var fake = new NoPromptsOrPeers();
        return new SingleInstanceCoordinator(new SingleInstanceOptions
        {
            MutexName = $"SingleInstanceFailureTests_Mutex_{unique}",
            ProcessName = processName,
            ExitEventName = $"SingleInstanceFailureTests_Exit_{unique}",
            SurfaceRunningInstance = surface ?? (() => { }),
            Prompts = fake,
            Inspector = fake,
            CreateMutex = factory,
        });
    }

    [Fact]
    public void ADeniedMutex_WithNoPeerRunning_IsAFailedStart_ThatNamesTheCause()
    {
        var surfaced = 0;
        using var coordinator = Build("NoSuchProcess_" + Guid.NewGuid().ToString("N"), DeniedMutex, () => surfaced++);

        Assert.False(coordinator.TryBecomeOwner());

        Assert.NotNull(coordinator.FailureReason);
        Assert.Contains("mutex", coordinator.FailureReason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("access denied", coordinator.FailureReason, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, surfaced);
    }

    [Fact]
    public void AnIoFailureOnTheMutex_WithNoPeerRunning_IsAFailedStart_ThatCarriesTheMessage()
    {
        using var coordinator = Build("NoSuchProcess_" + Guid.NewGuid().ToString("N"), IoFailureMutex);

        Assert.False(coordinator.TryBecomeOwner());

        Assert.Contains("mutex", coordinator.FailureReason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("The handle is invalid.", coordinator.FailureReason);
    }

    [Fact]
    public void ADeniedMutex_WithAPeerRunning_SurfacesIt_AndIsNotAFailure()
    {
        /* A real second process stands in for the running instance. The coordinator finds it by process name. */
        var peerInfo = new ProcessStartInfo("ping", "-n 30 127.0.0.1")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
        };
        using var peer = Process.Start(peerInfo)!;
        try
        {
            var surfaced = 0;
            using var coordinator = Build(peer.ProcessName, DeniedMutex, () => surfaced++);

            Assert.False(coordinator.TryBecomeOwner());

            Assert.Null(coordinator.FailureReason);
            Assert.Equal(1, surfaced);
        }
        finally
        {
            try { peer.Kill(); } catch (InvalidOperationException) { /* already exited */ }
        }
    }

    [Fact]
    public void ANormalStart_OwnsTheSlot_WithNoFailure()
    {
        using var coordinator = Build("NoSuchProcess_" + Guid.NewGuid().ToString("N"), factory: null);

        Assert.True(coordinator.TryBecomeOwner());

        Assert.Null(coordinator.FailureReason);
    }

    [Theory]
    [InlineData(true, 0)]
    [InlineData(false, 1)]
    public void TheStartPlan_MapsAFailedOwnership_ToTheInitFailedCode(bool headless, int dialogs)
    {
        var plan = new LiteStartupPlan(Headless: headless, Stop: false);
        var shown = new System.Collections.Generic.List<string>();

        var code = plan.ExitCodeAfterOwnershipFailure("the reason", shown.Add);

        Assert.Equal(3, code);
        Assert.Equal(LiteStartupPlan.ExitInitFailed, code);
        Assert.Equal(dialogs, shown.Count);
        Assert.All(shown, text => Assert.Equal("the reason", text));
    }
}

/// <summary>#9197: the failed start's reason reaches a log file before the logger is initialized. It shares the log buffer with every other test.</summary>
[Collection("app-logger-statics")]
public sealed class StartFailureLogTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "StartFailureLog_" + Guid.NewGuid().ToString("N"));

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
    public void TheReason_ReachesAFile_WithoutTheLoggerEverBeingInitialized_AndTheFolderIsCreated()
    {
        var logs = Path.Combine(_root, "logs");
        var reason = "could not create or open the single-instance mutex: access denied " + Guid.NewGuid().ToString("N");
        AppLogger.DrainBufferedLines();

        Assert.False(Directory.Exists(logs));
        Assert.True(StartFailureLog.Write(reason, logs));

        var text = string.Concat(Directory.GetFiles(logs, "lite_*.log").Select(File.ReadAllText));
        Assert.Contains(reason, text);
    }
}
