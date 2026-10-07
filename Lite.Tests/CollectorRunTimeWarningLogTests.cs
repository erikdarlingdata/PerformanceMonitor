using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4938: a run time that Lite ignores (a value that is not an HH:MM time, or a time on an hourly collector) is logged
/// as a warning that names the collector and the server. The shipped <see cref="ScheduleManager"/> was built with no
/// logger, so that warning was never written and the run time was dropped without a word. These pin that the app builds
/// it with its own logger, and that a warning given to that logger reaches the app log.
/// </summary>
/* AppLogger.DrainBufferedLines is destructive, so a class that reads it shares the one collection that serializes the
   others that do. */
[Collection("app-logger-statics")]
public sealed class CollectorRunTimeWarningLogTests : IDisposable
{
    private const string ServerKey = "warning-log-server";

    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-run-time-log-tests-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    /* The one place the app builds its schedule: it passes the app's logger the way the other services built beside it do. */
    [Fact]
    public void TheAppBuildsItsScheduleManagerWithTheAppsLogger()
    {
        var app = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs"));

        Assert.Equal(1, Regex.Count(app, @"new ScheduleManager\("));
        Assert.Matches(@"new ScheduleManager\(\s*App\.ConfigDirectory\s*,\s*new AppLoggerAdapter<ScheduleManager>\(\)\s*\)", app);
    }

    [Theory]
    [InlineData("25:00", "index_object_stats", 1440)]
    [InlineData("02:00", "server_config", 60)]
    public void ARunTimeThatIsIgnored_ReachesTheAppLog_ThroughTheAppsLogger(string runAt, string collector, int frequency)
    {
        var serverName = "warning-log-" + Guid.NewGuid().ToString("N")[..8];
        File.WriteAllText(Path.Combine(_dir, "collection_schedule.json"), $$"""
            {
              "version": 2,
              "default_schedule": [],
              "server_overrides": {
                "{{ServerKey}}": { "collectors": [ { "name": "{{collector}}", "enabled": true, "frequency_minutes": {{frequency}}, "retention_days": 30, "run_at": "{{runAt}}" } ] }
              }
            }
            """);
        AppLogger.DrainBufferedLines();

        var manager = new ScheduleManager(_dir, new AppLoggerAdapter<ScheduleManager>());
        manager.SetServerRunContext(ServerKey, 1_234_567_891, serverName, clock: null);
        _ = manager.GetDueCollectorsForServer(ServerKey, DateTime.UtcNow);

        var logged = Assert.Single(AppLogger.DrainBufferedLines(), l => l.Contains(serverName, StringComparison.Ordinal));
        Assert.Contains("[WARN ]", logged, StringComparison.Ordinal);
        Assert.Contains(collector, logged, StringComparison.Ordinal);
        Assert.Contains(runAt, logged, StringComparison.Ordinal);
    }
}
