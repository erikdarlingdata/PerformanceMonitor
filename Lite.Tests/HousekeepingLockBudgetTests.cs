using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5731: retention waits for the store's write lock with a budget instead of forever, leaves its daily stamp
/// alone when the budget runs out so the next housekeeping tick retries, and a write hold that lasts past the
/// watchdog threshold logs one warning naming the holder. The lock is thread-affine, so the contending holder
/// runs on its own thread, parked on an event; budgets and thresholds are tiny, so no test waits five seconds.
/// </summary>
[Collection("timing")]
public sealed class HousekeepingLockBudgetTests : IDisposable
{
    private static readonly TimeSpan HangBackstop = TimeSpan.FromSeconds(10);
    private static readonly BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
    private const string WatchdogText = "held the local database write lock";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public HousekeepingLockBudgetTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static Action HoldWriteLockOnThread(DuckDbInitializer initializer, ManualResetEventSlim held)
    {
        var release = new ManualResetEventSlim(false);
        var thread = new Thread(() =>
        {
            using (initializer.AcquireWriteLock())
            {
                held.Set();
                release.Wait(HangBackstop);
            }
        })
        { IsBackground = true };
        thread.Start();
        return () =>
        {
            release.Set();
            thread.Join(HangBackstop);
        };
    }

    [Fact]
    public async Task ARetentionTimeout_LeavesTheStampUnset_AndTheNextTickRetries()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var old = Path.Combine(_archiveDir, "200001_collection_log.parquet");
        File.WriteAllText(old, "x");
        var retention = new RetentionService(_archiveDir) { WriteLockBudget = TimeSpan.FromMilliseconds(100) };
        var service = new CollectionBackgroundService(null!, initializer, retentionService: retention);
        var due = DateTime.UtcNow.AddDays(-2);
        var stampField = typeof(CollectionBackgroundService).GetField("_lastRetentionTime", NonPublic)!;
        stampField.SetValue(service, due);
        var step = typeof(CollectionBackgroundService).GetMethod("RunRetentionIfDueAsync", NonPublic)!;

        var held = new ManualResetEventSlim(false);
        var release = HoldWriteLockOnThread(initializer, held);
        try
        {
            Assert.True(held.Wait(HangBackstop), "the holder never took the write lock");
            await (Task)step.Invoke(service, null)!;

            Assert.Equal(due, (DateTime)stampField.GetValue(service)!);
            Assert.True(File.Exists(old), "a retention pass that never got the lock must not have deleted anything");
        }
        finally
        {
            release();
        }

        await (Task)step.Invoke(service, null)!;

        Assert.True((DateTime)stampField.GetValue(service)! > due, "the retry that got the lock stamps the clock");
        Assert.False(File.Exists(old), "the retry deleted the expired archive");
    }

    [Fact]
    public async Task TheCleanup_ReportsNull_WhenTheBudgetRunsOut_AndTheCountWhenItRuns()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var retention = new RetentionService(_archiveDir) { WriteLockBudget = TimeSpan.FromMilliseconds(100) };

        var held = new ManualResetEventSlim(false);
        var release = HoldWriteLockOnThread(initializer, held);
        try
        {
            Assert.True(held.Wait(HangBackstop));
            Assert.Null(await retention.CleanupOldArchivesAndRefreshViewsAsync(initializer));
        }
        finally
        {
            release();
        }

        Assert.Equal(0, await retention.CleanupOldArchivesAndRefreshViewsAsync(initializer));
    }

    [Fact]
    public void AWriteHoldPastTheThreshold_LogsOneWarningNamingTheHolder()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        initializer.WriteHoldWarnAfter = TimeSpan.FromMilliseconds(30);
        AppLogger.DrainBufferedLines();

        using (initializer.AcquireWriteLock())
        {
            /* A wait that is never signalled: the hold lasts the timeout. */
            new ManualResetEventSlim(false).Wait(TimeSpan.FromMilliseconds(250));
        }

        var warnings = AppLogger.DrainBufferedLines()
            .Where(l => l.Contains(WatchdogText, StringComparison.Ordinal)).ToList();
        var warning = Assert.Single(warnings);
        Assert.Contains("WARN", warning, StringComparison.Ordinal);
        Assert.Contains(nameof(AWriteHoldPastTheThreshold_LogsOneWarningNamingTheHolder), warning, StringComparison.Ordinal);
    }

    [Fact]
    public void AShortWriteHold_AndALongReadHold_LogNothing()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        initializer.WriteHoldWarnAfter = TimeSpan.FromMilliseconds(500);
        AppLogger.DrainBufferedLines();

        using (initializer.AcquireWriteLock())
        {
            /* Released at once: under the threshold. */
        }

        initializer.WriteHoldWarnAfter = TimeSpan.FromMilliseconds(1);
        using (initializer.AcquireReadLock())
        {
            new ManualResetEventSlim(false).Wait(TimeSpan.FromMilliseconds(100));
        }

        Assert.DoesNotContain(AppLogger.DrainBufferedLines(),
            l => l.Contains(WatchdogText, StringComparison.Ordinal)
                && l.Contains(nameof(AShortWriteHold_AndALongReadHold_LogNothing), StringComparison.Ordinal));
    }
}
