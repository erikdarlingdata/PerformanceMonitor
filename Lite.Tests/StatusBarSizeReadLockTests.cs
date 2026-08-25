using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the status bar's used-size read against the two ways it can be wrong (#2594).
///
/// <para><b>It must not open a connection without the lock.</b> This was the one PERIODIC connection site
/// in <see cref="DuckDbInitializer"/> that did — driven by the dashboard's 30-second
/// <c>DispatcherTimer</c>, so a live handle sat on the database file at arbitrary moments, including
/// moments the archival path may be deleting and recreating it.</para>
///
/// <para><b>And it must not block the dashboard to get it.</b> The obvious fix — take the read lock like
/// every other read — runs on the dispatcher thread, so a size figure nobody is reading would freeze the
/// window behind a long archival. So the contract is specifically a BOUNDED attempt: acquire if free,
/// give up quickly otherwise, and let the caller render the file size alone. Asserting only "it takes a
/// lock" would pass a fix that hangs the UI, which is why this test measures the time.</para>
/// </summary>
public class StatusBarSizeReadLockTests
{
    /// <summary>
    /// Generous against the 100 ms budget rather than tight: this asserts the read GAVE UP rather than
    /// waited, and a CI machine under load can overshoot a 100 ms timeout considerably without the
    /// behaviour being wrong. The distinction being drawn is against a wait for the full hold below,
    /// which is an order of magnitude larger.
    /// </summary>
    private static readonly TimeSpan GaveUpCeiling = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan WriteLockHold = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task GetUsedDataSizeMb_WhenTheWriteLockIsHeld_GivesUpInsteadOfBlocking()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "pmlite-statusbar-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var initializer = new DuckDbInitializer(Path.Combine(tempDir, "test.duckdb"));

            var lockHeld = new ManualResetEventSlim(false);
            var release = new ManualResetEventSlim(false);

            /* The write lock is thread-affine, so it has to be taken and released on its own thread. */
            var holder = Task.Run(() =>
            {
                using var writeLock = initializer.AcquireWriteLock();
                lockHeld.Set();
                release.Wait(WriteLockHold);
            });

            Assert.True(lockHeld.Wait(TimeSpan.FromSeconds(5)), "the write lock was never acquired");

            var stopwatch = Stopwatch.StartNew();
            var used = initializer.GetUsedDataSizeMb();
            stopwatch.Stop();

            release.Set();
            await holder.WaitAsync(TimeSpan.FromSeconds(15));

            /* Null, not a number: the caller renders the file size alone in this state, which is the
               degraded answer this is choosing on purpose. */
            Assert.Null(used);

            Assert.True(
                stopwatch.Elapsed < GaveUpCeiling,
                $"the status-bar size read waited {stopwatch.Elapsed.TotalMilliseconds:F0} ms for the write " +
                "lock. It runs on the dispatcher thread and must give up rather than block the window.");
        }
        finally
        {
            try
            {
                Directory.Delete(tempDir, recursive: true);
            }
            catch (IOException)
            {
                /* A leftover temp directory is not worth failing a passing test over. */
            }
        }
    }
}
