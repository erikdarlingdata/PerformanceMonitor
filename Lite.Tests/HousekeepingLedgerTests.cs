using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;
using Loop = PerformanceMonitorLite.Services.CollectionBackgroundService.HousekeepingLoop;
using Step = PerformanceMonitorLite.Services.CollectionBackgroundService.HousekeepingStep;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5731: housekeeping runs on its own loops, so the skipped-slots report asks a ledger which steps overlapped the gap.
/// The ledger keeps the steps of a second or more (the newest 32), is safe under three writers and a reader, and
/// <see cref="CollectionCycleSkip.Build"/> adds one sentence for the entries and none without them.
/// </summary>
public sealed class HousekeepingLedgerTests
{
    private static readonly DateTime T0 = new(2026, 10, 12, 10, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);

    /// <summary>A clock the test moves by hand; a step "takes" time by advancing it.</summary>
    private sealed class FakeClock
    {
        private long _ticks = T0.Ticks;
        public DateTime Now => new(Interlocked.Read(ref _ticks), DateTimeKind.Utc);
        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
    }

    private static DateTime At(double seconds) => T0 + TimeSpan.FromSeconds(seconds);

    private static List<CycleStep> Steps() =>
    [
        new("connection_check", 400),
        new("collectors", 61_000),
    ];

    // ── overlap math ─────────────────────────────────────────────────────────────

    [Fact]
    public void Overlapping_ReturnsStepsThatStraddleOrSitInsideTheGap_AndNotThoseOutsideIt()
    {
        var ledger = new HousekeepingLedger(() => At(10_000));
        /* The gap is 100 s to 200 s. */
        ledger.Record("before", At(10), TimeSpan.FromSeconds(50));          /* ends at 60 */
        ledger.Record("touches_start", At(40), TimeSpan.FromSeconds(60));   /* ends exactly at 100: not overlapping */
        ledger.Record("straddles_start", At(90), TimeSpan.FromSeconds(30)); /* 90 to 120 */
        ledger.Record("inside", At(130), TimeSpan.FromSeconds(20));         /* 130 to 150 */
        ledger.Record("straddles_end", At(190), TimeSpan.FromSeconds(30));  /* 190 to 220 */
        ledger.Record("touches_end", At(200), TimeSpan.FromSeconds(30));    /* starts exactly at 200: not overlapping */
        ledger.Record("covers", At(50), TimeSpan.FromSeconds(300));         /* 50 to 350 */
        ledger.Record("after", At(300), TimeSpan.FromSeconds(30));

        var names = ledger.Overlapping(At(100), At(200)).Select(e => e.Name).ToList();

        Assert.Equal(["covers", "straddles_start", "inside", "straddles_end"], names); /* oldest start first */
    }

    [Fact]
    public async Task Time_RecordsAStepOfASecondOrMore_AndIgnoresAShorterOne()
    {
        var clock = new FakeClock();
        var ledger = new HousekeepingLedger(() => clock.Now);

        await ledger.Time("quick", () => { clock.Advance(TimeSpan.FromMilliseconds(999)); return Task.CompletedTask; });
        await ledger.Time("exactly_one_second", () => { clock.Advance(TimeSpan.FromSeconds(1)); return Task.CompletedTask; });
        await ledger.Time("archival", () => { clock.Advance(TimeSpan.FromSeconds(252)); return Task.CompletedTask; });

        var entries = ledger.Overlapping(T0, T0 + TimeSpan.FromHours(1));
        Assert.Equal(["exactly_one_second", "archival"], entries.Select(e => e.Name).ToArray());
        var archival = entries[1];
        Assert.Equal(TimeSpan.FromSeconds(252), archival.Length);
        Assert.Equal(T0 + TimeSpan.FromMilliseconds(999) + TimeSpan.FromSeconds(1), archival.StartUtc);
        Assert.False(archival.Failed);
    }

    [Fact]
    public async Task Time_RecordsAStepThatThrowsAsFailed_AndLetsTheExceptionThrough()
    {
        var clock = new FakeClock();
        var ledger = new HousekeepingLedger(() => clock.Now);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ledger.Time("retention", () =>
        {
            clock.Advance(TimeSpan.FromSeconds(3));
            throw new InvalidOperationException("boom");
        }));

        Assert.Equal("boom", ex.Message);
        var entry = Assert.Single(ledger.Overlapping(T0, T0 + TimeSpan.FromHours(1)));
        Assert.Equal("retention", entry.Name);
        Assert.True(entry.Failed);
    }

    [Fact]
    public async Task AStepStillRunning_IsListedWithItsLengthSoFar()
    {
        var clock = new FakeClock();
        var ledger = new HousekeepingLedger(() => clock.Now);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = ledger.Time("archival", () => release.Task);

        clock.Advance(TimeSpan.FromSeconds(0.5));
        Assert.Empty(ledger.Overlapping(T0, T0 + TimeSpan.FromHours(1))); /* not yet a second */
        clock.Advance(TimeSpan.FromSeconds(100));
        var entry = Assert.Single(ledger.Overlapping(T0, T0 + TimeSpan.FromHours(1)));
        Assert.True(entry.Running);
        Assert.Equal(TimeSpan.FromSeconds(100.5), entry.Length);
        Assert.Equal(0, ledger.FinishedCount);

        release.SetResult();
        await running;
        var finished = Assert.Single(ledger.Overlapping(T0, T0 + TimeSpan.FromHours(1)));
        Assert.False(finished.Running);
        Assert.Equal(1, ledger.FinishedCount);
    }

    [Fact]
    public void TheLedger_KeepsTheNewest32()
    {
        var ledger = new HousekeepingLedger(() => At(100_000));
        for (var i = 0; i < 40; i++)
        {
            ledger.Record("step" + i, At(i * 10), TimeSpan.FromSeconds(5));
        }

        var kept = ledger.Overlapping(T0, At(100_000));
        Assert.Equal(32, kept.Count);
        Assert.Equal("step8", kept[0].Name);
        Assert.Equal("step39", kept[^1].Name);
    }

    [Fact]
    public async Task ThreeWritersAndAReader_RunWithoutAnExceptionAndKeepAtMost32()
    {
        var ledger = new HousekeepingLedger();
        using var stop = new CancellationTokenSource();
        var readerErrors = new List<Exception>();

        var reader = Task.Run(() =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var seen = ledger.Overlapping(DateTime.UtcNow.AddHours(-1), DateTime.UtcNow.AddHours(1));
                    Assert.True(seen.Count <= HousekeepingLedger.Capacity + 3); /* finished (at most 32) plus up to 3 running */
                    _ = ledger.FinishedCount;
                }
            }
            catch (Exception ex)
            {
                lock (readerErrors) readerErrors.Add(ex);
            }
        }, TestContext.Current.CancellationToken);

        var writers = Enumerable.Range(0, 3).Select(w => Task.Run(() =>
        {
            for (var i = 0; i < 2_000; i++)
            {
                ledger.Record($"w{w}_{i}", DateTime.UtcNow, TimeSpan.FromSeconds(2), failed: i % 7 == 0);
            }
        }, TestContext.Current.CancellationToken)).ToArray();
        var timed = Task.Run(async () =>
        {
            for (var i = 0; i < 200; i++)
            {
                await ledger.Time("timed", async () => await Task.Yield());
            }
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(writers.Append(timed)).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await stop.CancelAsync();
        await reader.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Empty(readerErrors);
        Assert.Equal(HousekeepingLedger.Capacity, ledger.FinishedCount);
    }

    // ── the loops write to it ────────────────────────────────────────────────────

    [Fact]
    public async Task TheHousekeepingLoop_RecordsEachSlowStep_AndAFailedOneAsFailed()
    {
        var clock = new FakeClock();
        var ledger = new HousekeepingLedger(() => clock.Now);
        var tick = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waits = 0;
        using var stop = new CancellationTokenSource();
        var loops = new[]
        {
            new Loop("maintenance",
            [
                new Step("archival", _ => { clock.Advance(TimeSpan.FromSeconds(252)); return Task.CompletedTask; }),
                new Step("retention", _ => { clock.Advance(TimeSpan.FromSeconds(3)); throw new InvalidOperationException("boom"); }),
                new Step("findings_cleanup", _ => { clock.Advance(TimeSpan.FromMilliseconds(20)); return Task.CompletedTask; }),
            ]),
        };

        var tasks = CollectionBackgroundService.StartHousekeepingLoops(
            loops, Interval, () => true, Task.CompletedTask,
            (wait, token) =>
            {
                Interlocked.Increment(ref waits);
                return Volatile.Read(ref waits) == 1 ? Task.CompletedTask : tick.Task.WaitAsync(token);
            },
            (loop, step, ex) => { }, stop.Token, ledger);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (ledger.FinishedCount < 2)
        {
            await Task.Delay(1, timeout.Token);
        }

        var entries = ledger.Overlapping(T0, T0 + TimeSpan.FromHours(1));
        Assert.Equal(["archival", "retention"], entries.Select(e => e.Name).ToArray()); /* findings_cleanup was 20 ms */
        Assert.False(entries[0].Failed);
        Assert.True(entries[1].Failed);

        await stop.CancelAsync();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    // ── the report's text ────────────────────────────────────────────────────────

    private static CycleSkipReport Build(IReadOnlyList<HousekeepingEntry>? housekeeping, StoreLockHolder? holder = null)
        => CollectionCycleSkip.Build(4, TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(1), Steps(), Interval,
            T0, T0 + TimeSpan.FromMinutes(5), housekeeping, holder)!;

    [Fact]
    public void WithNoLedgerEntries_TheMessageIsTheOldText()
    {
        var old = CollectionCycleSkip.Build(4, TimeSpan.FromSeconds(312), TimeSpan.FromSeconds(1), Steps(), Interval,
            T0, T0 + TimeSpan.FromMinutes(5))!;

        Assert.Equal(old.Message, Build(null).Message);
        Assert.Equal(old.Message, Build([]).Message);
        /* A holder alone is not added: it rides with the housekeeping sentence. */
        Assert.Equal(old.Message, Build([], new StoreLockHolder("ArchiveService.ExportAsync", true, TimeSpan.FromMinutes(4))).Message);
        Assert.DoesNotContain("Housekeeping", old.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithLedgerEntries_TheMessageGainsOneSentence_AndTheRowFieldsAreUnchanged()
    {
        var old = Build(null);
        var entries = new List<HousekeepingEntry>
        {
            new("retention", At(10), TimeSpan.FromSeconds(3), false),
            new("archival", At(20), TimeSpan.FromSeconds(252), false),
        };

        var report = Build(entries);

        Assert.Equal(old.Message + " Housekeeping in that window: archival 252s, retention 3s.", report.Message);
        Assert.Equal(old.SlotsSkipped, report.SlotsSkipped);
        Assert.Equal(old.CycleMs, report.CycleMs);
        Assert.Equal(old.SlowestStep, report.SlowestStep);
        Assert.Equal(old.SlowestStepMs, report.SlowestStepMs);
        Assert.DoesNotContain("=", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSentence_SumsARepeatedName_MarksFailedAndRunning_NamesTheHolder_AndListsTheLongestFour()
    {
        var entries = new List<HousekeepingEntry>
        {
            new("findings_cleanup", At(1), TimeSpan.FromSeconds(2), false),
            new("findings_cleanup", At(5), TimeSpan.FromSeconds(3), false),
            new("retention", At(10), TimeSpan.FromSeconds(7), true),
            new("analysis", At(20), TimeSpan.FromSeconds(30), false),
            new("query_store_backfill", At(30), TimeSpan.FromSeconds(1.5), false),
            new("archival", At(40), TimeSpan.FromSeconds(252), false, Running: true),
        };

        var message = Build(entries, new StoreLockHolder("ArchiveService.ExportAsync", true, TimeSpan.FromSeconds(252))).Message;

        Assert.EndsWith(
            " Housekeeping in that window: archival 252s (still running), analysis 30s, retention 7s (failed), findings cleanup 5s."
            + " The store write lock was held by ArchiveService.ExportAsync for 252s when this was written.",
            message, StringComparison.Ordinal);
        Assert.DoesNotContain("query store backfill", message, StringComparison.Ordinal); /* fifth longest */
    }

    // ── the wiring ────────────────────────────────────────────────────────────────

    private static string ServiceSource([CallerFilePath] string thisFile = "")
        => File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Services", "CollectionBackgroundService.cs")));

    [Fact]
    public void TheServiceWrapsEachStepInTheLedger_AndTheReportReadsIt()
    {
        var source = ServiceSource();

        Assert.Contains("ledger.Time(step.Name, () => step.Run(token))", source, StringComparison.Ordinal);
        Assert.Contains("stoppingToken,\r\n            _housekeepingLedger);".Replace("\r\n", "\n"), source.Replace("\r\n", "\n"), StringComparison.Ordinal);
        Assert.Contains("_housekeepingLedger.Overlapping(previousCycleStart, nextCycleStart)", source, StringComparison.Ordinal);
        Assert.Contains("StoreLockHolders.Snapshot()", source, StringComparison.Ordinal);
    }
}
