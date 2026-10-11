using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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

    /// <summary>How many lines below an acquisition a StoreLockHolders.Register( may sit (#5650).</summary>
    private const int RegisterWindowLines = 20;

    /// <summary>
    /// Every store-lock acquisition in DuckDbInitializer.cs registers its holder within the next
    /// <see cref="RegisterWindowLines"/> lines, except the shutdown Dispose (#5650). A timeout names its holders from
    /// that registry, so an unregistered acquisition is a holder the message cannot name. The window is 20 rather than
    /// 12 because AcquireReadLock and AcquireWriteLock return their LockReleaser 15 to 19 lines below the Enter.
    /// </summary>
    [Fact]
    public void EveryStoreLockAcquisition_RegistersItsHolder_ExceptTheShutdownDispose()
    {
        var lines = global::Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs").Split('\n');
        var acquisition = new Regex(@"s_dbLock\.(Try)?Enter(Read|Write)Lock\(");
        var checkedCount = 0;
        var unregistered = new List<string>();
        for (var i = 0; i < lines.Length; i++)
        {
            if (!acquisition.IsMatch(lines[i]) || lines[i].Contains("DisposeWriteLockTimeout", StringComparison.Ordinal))
                continue;

            checkedCount++;
            var window = string.Join("\n", lines.Skip(i + 1).Take(RegisterWindowLines));
            if (!window.Contains("StoreLockHolders.Register(", StringComparison.Ordinal))
                unregistered.Add($"line {i + 1}: {lines[i].Trim()}");
        }

        Assert.True(checkedCount >= 5, $"Found {checkedCount} acquisitions; the pattern no longer matches the source.");
        Assert.True(unregistered.Count == 0,
            "No StoreLockHolders.Register( within " + RegisterWindowLines + " lines of: " + string.Join(" | ", unregistered));
    }

    /// <summary>
    /// A busy trim skip is counted, and the hourly Information line says how many skips the hour held (#5650).
    /// The first skip opens the hour; two skips inside it log nothing; the third, past the hour on the test clock,
    /// writes one line with all three.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_BusySkipsInAnHour_WriteOneInformationLineWithTheCount()
    {
        var dir = Path.Combine(Path.GetTempPath(), "trim-skip-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var sink = new List<(LogLevel Level, string Message)>();
            using var initializer = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"), new SkipLogger(sink));
            await initializer.InitializeAsync();
            sink.Clear();
            var now = new DateTime(2026, 10, 11, 12, 0, 0, DateTimeKind.Utc);
            initializer.UtcNow = () => now;

            var held = new ManualResetEventSlim(false);
            var release = HoldOnThread(() => initializer.AcquireWriteLock(), held);
            try
            {
                Assert.True(held.Wait(HangBackstop));
                initializer.RunMemoryTrimCycle();
                now = now.AddMinutes(10);
                initializer.RunMemoryTrimCycle();
                Assert.DoesNotContain(sink, e => e.Level == LogLevel.Information);
                now = now.AddMinutes(60);
                initializer.RunMemoryTrimCycle();
            }
            finally
            {
                release();
            }

            var infoLines = sink
                .Where(e => e.Level == LogLevel.Information && e.Message.StartsWith("Memory trim skipped", StringComparison.Ordinal))
                .ToList();
            var line = Assert.Single(infoLines);
            Assert.Equal(
                "Memory trim skipped 3 time(s) in the last 70 min because the local database was busy",
                line.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>Keeps each formatted line the initializer writes, so the test can read the Information ones.</summary>
    private sealed class SkipLogger(List<(LogLevel Level, string Message)> sink) : ILogger<DuckDbInitializer>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            sink.Add((logLevel, formatter(state, exception)));
    }
}
