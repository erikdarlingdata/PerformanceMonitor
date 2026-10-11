using System;
using System.IO;
using System.Threading;
using Xunit;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5650: a store write that gave up on the process-wide lock said "archival or maintenance may be in progress"
/// whoever held it. The timeout now names the holders and how long each held. Deterministic: the holder is parked on
/// an event, the contender's budget is tiny, and nothing sleeps.
/// </summary>
[Collection("timing")]
public sealed class StoreLockHolderTests
{
    private static readonly TimeSpan HangBackstop = TimeSpan.FromSeconds(5);

    /// <summary>Holds a lock on its own thread (the lock is thread-affine) until the returned action runs.</summary>
    private static Action HoldOnThread(Func<IDisposable> acquire, ManualResetEventSlim held)
    {
        var release = new ManualResetEventSlim(false);
        var thread = new Thread(() =>
        {
            using (acquire())
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
    public void WriteTimeout_NamesTheWriteHolder_AndNotArchival()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pmlite-lockholder-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            using var initializer = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var held = new ManualResetEventSlim(false);
            var stop = HoldOnThread(() => initializer.AcquireWriteLock(), held);
            try
            {
                Assert.True(held.Wait(HangBackstop), "the holder never took the write lock");

                var ex = Assert.ThrowsAny<TimeoutException>(
                    () => initializer.AcquireWriteLock(TimeSpan.FromMilliseconds(20)));

                Assert.Contains(nameof(WriteTimeout_NamesTheWriteHolder_AndNotArchival), ex.Message, StringComparison.Ordinal);
                Assert.Contains("write lock", ex.Message, StringComparison.Ordinal);
                Assert.DoesNotContain("archival or maintenance", ex.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                stop();
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    [Fact]
    public void WriteTimeout_NamesAReadHolder_WithHowLongItHeld()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pmlite-lockholder-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            using var initializer = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var held = new ManualResetEventSlim(false);
            var stop = HoldOnThread(() => initializer.AcquireReadLock(), held);
            try
            {
                Assert.True(held.Wait(HangBackstop), "the holder never took the read lock");

                var ex = Assert.ThrowsAny<TimeoutException>(
                    () => initializer.AcquireWriteLock(TimeSpan.FromMilliseconds(20)));

                Assert.Contains(nameof(WriteTimeout_NamesAReadHolder_WithHowLongItHeld), ex.Message, StringComparison.Ordinal);
                Assert.Contains("read lock", ex.Message, StringComparison.Ordinal);
                Assert.Matches(@"held \d+(\.\d+)? s", ex.Message);
            }
            finally
            {
                stop();
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
