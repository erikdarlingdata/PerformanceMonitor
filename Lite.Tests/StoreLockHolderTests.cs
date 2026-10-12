using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Xunit;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5650: a store write that gave up on the process-wide lock said "archival or maintenance may be in progress"
/// whoever held it. The timeout now names the holders and how long each held. Deterministic: the holder is parked on
/// an event, the contender's budget is tiny, and nothing sleeps.
/// </summary>
[Collection("timing")]
public sealed class StoreLockHolderTests
{
    private static readonly TimeSpan HangBackstop = TimeSpan.FromSeconds(3);

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

                /* The registered holder itself, not the message's preamble: "write lock" appears in BuildMessage's own
                   text with no holder at all, so asserting it would pass if the registry broke. A holder with this
                   test's name, taken as a write lock, can only come from the registry. */
                var typed = Assert.IsType<StoreLockTimeoutException>(ex);
                var holder = Assert.Single(typed.Holders, h => h.Name.Contains(nameof(WriteTimeout_NamesTheWriteHolder_AndNotArchival), StringComparison.Ordinal));
                Assert.True(holder.Write, "the holder took the write lock");
                Assert.Contains(nameof(WriteTimeout_NamesTheWriteHolder_AndNotArchival) + " (write lock, held", ex.Message, StringComparison.Ordinal);
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

    /// <summary>
    /// #5650: the user-write entry point passes the caller's name through, so the hold is recorded under the method that
    /// asked for the connection, not under <c>LocalDataService.OpenUserWriteConnectionAsync</c>.
    /// </summary>
    [Fact]
    public async Task UserWriteConnection_HolderNamesThisTest_NotTheEntryPoint()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pmlite-lockholder-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            using var initializer = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var service = new LocalDataService(initializer, TimeSpan.FromMilliseconds(150), TimeSpan.FromSeconds(3));
            await using (await service.OpenUserWriteConnectionAsync())
            {
                /* The write lock is exclusive, so while this hold is open the registry can name no other writer. */
                var holders = StoreLockHolders.Snapshot();
                Assert.Contains(holders, h => h.Write
                    && h.Name == "StoreLockHolderTests." + nameof(UserWriteConnection_HolderNamesThisTest_NotTheEntryPoint));
                Assert.DoesNotContain(holders, h => h.Name.Contains("OpenUserWriteConnectionAsync", StringComparison.Ordinal));
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

    /// <summary>
    /// A leaked releaser leaves its entry in the registry forever. Left alone it would sort first (longest held) in every
    /// later timeout and blame a hold that ended long ago. Past <see cref="StoreLockHolders.StaleAfter"/> an entry is
    /// marked and listed after every fresher holder. The entries are registered with their start times set, so nothing
    /// waits; other tests' real holds may be in the registry, so only this test's own entries are looked at.
    /// </summary>
    [Fact]
    public void AHoldFarPastAnyRealOne_IsMarkedStale_AndNeverBlamedAheadOfARealHolder()
    {
        var now = Stopwatch.GetTimestamp();
        var elevenMinutes = (long)(StoreLockHolders.StaleAfter.TotalSeconds * 1.1 * Stopwatch.Frequency);   // no TimeSpan literal: WriteLockBudgetTests counts those as deliberate holds
        var oldId = StoreLockHolders.Register("StaleProbeLeaked", "StaleProbeFile.cs", write: false, startedTimestamp: now - elevenMinutes);
        var freshId = StoreLockHolders.Register("StaleProbeReal", "StaleProbeFile.cs", write: false, startedTimestamp: now - (elevenMinutes / 100));
        try
        {
            var mine = StoreLockHolders.Snapshot(now)
                .Where(h => h.Name.StartsWith("StaleProbeFile.", StringComparison.Ordinal))
                .ToList();

            Assert.Equal(2, mine.Count);

            /* The real holder comes first although it has held for far less. */
            Assert.Equal("StaleProbeFile.StaleProbeReal", mine[0].Name);
            Assert.Equal("StaleProbeFile.StaleProbeLeaked (stale?)", mine[1].Name);
            Assert.True(mine[1].Held > StoreLockHolders.StaleAfter);

            /* And the text a timeout would carry names the real one first and carries the mark. */
            var text = StoreLockTimeoutException.FormatHolders(mine);
            Assert.True(text.IndexOf("StaleProbeReal", StringComparison.Ordinal) < text.IndexOf("StaleProbeLeaked", StringComparison.Ordinal));
            Assert.Contains("(stale?)", text, StringComparison.Ordinal);
        }
        finally
        {
            StoreLockHolders.Unregister(oldId);
            StoreLockHolders.Unregister(freshId);
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
