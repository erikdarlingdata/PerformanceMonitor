/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9250: once the store file reaches the hot store reset point, each housekeeping tick reads how much data the file holds. A read that
/// failed (the read lock was busy, or the query failed) fell back to the file ceiling with no log line and no count. The tracker
/// writes the first failure of a run, a count every <see cref="UsedDataReadRunLog.ReportEvery"/> failed reads, and one line when a
/// read works again. The first group drives the tracker, the second drives the real archival step over a real store whose lock is held.
/// </summary>
public sealed class UsedDataReadRunLogTests : IDisposable
{
    private static readonly HotStoreResetLimit Limit = new(512, HotStoreResetSetting.SourceDefault);

    private static readonly UsedDataRead Busy = new(null, UsedDataReadFailure.LockBusy);
    private static readonly UsedDataRead Failed = new(null, UsedDataReadFailure.QueryFailed);
    private static readonly UsedDataRead Worked = new(600, UsedDataReadFailure.None);

    private readonly List<(LogLevel Level, string Message)> _log = [];
    private readonly string _dir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public UsedDataReadRunLogTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private sealed class CapturingLogger<T>(List<(LogLevel Level, string Message)> sink) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => sink.Add((logLevel, formatter(state, exception)));
    }

    private UsedDataReadRunLog NewTracker() => new(new CapturingLogger<UsedDataReadRunLog>(_log));

    // ── the tracker ───────────────────────────────────────────────────────────────

    [Fact]
    public void ARunOfFailedReads_LogsTheFirst_ThenOnlyEveryNth_NotEveryTick()
    {
        var tracker = NewTracker();
        var reads = (UsedDataReadRunLog.ReportEvery * 2) + 5;

        for (var i = 0; i < reads; i++)
        {
            tracker.Record(Busy, 700, Limit);
        }

        /* The first failure, then the Nth and the 2Nth. 125 failed reads are three lines, not 125. */
        Assert.Equal(3, _log.Count);
        Assert.All(_log, line => Assert.Equal(LogLevel.Warning, line.Level));
        Assert.Equal(reads, tracker.FailedReads);
        Assert.Contains($"{UsedDataReadRunLog.ReportEvery} in a row", _log[1].Message);
        Assert.Contains($"{UsedDataReadRunLog.ReportEvery * 2} in a row", _log[2].Message);
    }

    [Fact]
    public void TheFirstFailure_NamesTheFileSize_TheResetPointAndItsSource_AndTheFileCeiling()
    {
        var tracker = NewTracker();

        tracker.Record(Busy, 700, new HotStoreResetLimit(512, HotStoreResetSetting.SourceSetting));

        var line = Assert.Single(_log).Message;
        Assert.Contains("700 MB", line);
        Assert.Contains("512 MB", line);
        Assert.Contains("setting", line);
        Assert.Contains("1024 MB", line);
    }

    [Fact]
    public void TheLine_SaysWhichWayTheReadFailed()
    {
        var busyTracker = NewTracker();
        busyTracker.Record(Busy, 700, Limit);
        var busyLine = Assert.Single(_log).Message;
        _log.Clear();

        var failedTracker = NewTracker();
        failedTracker.Record(Failed, 700, Limit);
        var failedLine = Assert.Single(_log).Message;

        Assert.Contains("lock was busy", busyLine);
        Assert.DoesNotContain("query failed", busyLine);
        Assert.Contains("query failed", failedLine);
        Assert.DoesNotContain("lock was busy", failedLine);
    }

    [Fact]
    public void AReadThatWorks_EndsTheRun_AndLogsHowManyFailedOnce()
    {
        var tracker = NewTracker();
        for (var i = 0; i < 7; i++)
        {
            tracker.Record(Failed, 700, Limit);
        }

        _log.Clear();
        tracker.Record(Worked, 700, Limit);
        tracker.Record(Worked, 700, Limit);

        var line = Assert.Single(_log);
        Assert.Equal(LogLevel.Information, line.Level);
        Assert.Contains("7 failed", line.Message);
        Assert.Equal(0, tracker.FailedReads);
    }

    [Fact]
    public void AReadThatWorks_WithNoRunBehindIt_LogsNothing()
    {
        var tracker = NewTracker();

        tracker.Record(Worked, 700, Limit);

        Assert.Empty(_log);
    }

    [Fact]
    public void AFailureAfterARecovery_IsANewRun_AndLogsItsOwnFirstLine()
    {
        var tracker = NewTracker();
        tracker.Record(Busy, 700, Limit);
        tracker.Record(Worked, 700, Limit);
        _log.Clear();

        tracker.Record(Busy, 700, Limit);

        Assert.Equal(LogLevel.Warning, Assert.Single(_log).Level);
        Assert.Equal(1, tracker.FailedReads);
    }

    [Fact]
    public void AStoreReset_EndsTheRun_SoTheNextFailureLogsItsFirstLine()
    {
        var tracker = NewTracker();
        tracker.Record(Busy, 1100, Limit);
        tracker.Record(Busy, 1100, Limit);
        tracker.EndRunAfterReset();
        _log.Clear();

        tracker.Record(Busy, 600, Limit);

        Assert.Equal(LogLevel.Warning, Assert.Single(_log).Level);
        Assert.Equal(1, tracker.FailedReads);
    }

    // ── the archival step ─────────────────────────────────────────────────────────

    /// <summary>Holds the store's write lock on its own thread, which makes the status-size read's 100 ms wait run out.</summary>
    private sealed class HeldWrite : IDisposable
    {
        /* A backstop only: Dispose releases the hold. */
        private static readonly TimeSpan HoldCeiling = TimeSpan.FromSeconds(10);

        private readonly ManualResetEventSlim _release = new(false);
        private readonly Thread _thread;

        public HeldWrite(DuckDbInitializer initializer)
        {
            using var taken = new ManualResetEventSlim(false);
            _thread = new Thread(() =>
            {
                /* Taken with a bounded wait, like the other tests that take the lock for a moment. The hold lasts the few
                   100 ms ticks of one test, so it adds nothing to the suite-wide bar the write-lock budget has to outlast. */
                using (initializer.AcquireWriteLock(HoldCeiling))
                {
                    taken.Set();
                    _release.Wait(HoldCeiling);
                }
            }) { IsBackground = true, Name = "UsedDataReadRunLogTests.HeldWrite" };
            _thread.Start();
            Assert.True(taken.Wait(HoldCeiling), "the write lock was not taken");
        }

        public void Dispose()
        {
            _release.Set();
            _thread.Join(TimeSpan.FromSeconds(10));
            _release.Dispose();
        }
    }

    private sealed record Host(CollectionBackgroundService Service, DuckDbInitializer DuckDb, Func<Task> Tick);

    private async Task<Host> OpenHostAsync()
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var archiveDir = Path.Combine(_dir, "archive");
        Directory.CreateDirectory(archiveDir);
        var archive = new ArchiveService(duckDb, archiveDir);
        var service = new CollectionBackgroundService(
            null!, duckDb, archiveService: archive, logger: new CapturingLogger<CollectionBackgroundService>(_log));

        /* Not time-due: only the size check can act, and it cannot reach the file ceiling. */
        const BindingFlags nonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        typeof(CollectionBackgroundService).GetField("_lastArchiveTime", nonPublic)!.SetValue(service, DateTime.UtcNow);
        var step = typeof(CollectionBackgroundService).GetMethod("RunArchivalIfDueAsync", nonPublic)!;
        var args = step.GetParameters()
            .Select(p => p.ParameterType == typeof(CancellationToken) ? (object)CancellationToken.None : null)
            .ToArray();

        return new Host(service, duckDb, () => (Task)step.Invoke(service, args)!);
    }

    /// <summary>A reset point just under the file, so the tick attempts the data read and the file ceiling (twice it) is far off.</summary>
    private static HotStoreResetLimit JustUnder(DuckDbInitializer duckDb) =>
        new(duckDb.GetDatabaseSizeMb() * 0.9, HotStoreResetSetting.SourceSetting);

    [Fact]
    public async Task TicksWhoseDataReadFails_WriteOneWarning_NotOnePerTick()
    {
        var host = await OpenHostAsync();
        host.Service.HotStoreReset = JustUnder(host.DuckDb);

        using (new HeldWrite(host.DuckDb))
        {
            for (var i = 0; i < 4; i++)
            {
                await host.Tick();
            }
        }

        var line = Assert.Single(_log);
        Assert.Equal(LogLevel.Warning, line.Level);
        Assert.Contains("lock was busy", line.Message);
        Assert.Contains("setting", line.Message);
    }

    [Fact]
    public async Task ATickWithTheFileUnderTheResetPoint_ReadsNothing_SoItNeitherStartsNorEndsARun()
    {
        var host = await OpenHostAsync();
        var attempting = JustUnder(host.DuckDb);
        var underPoint = new HotStoreResetLimit(host.DuckDb.GetDatabaseSizeMb() * 2, HotStoreResetSetting.SourceSetting);

        /* File under the reset point: no read, so no line even with the lock held. */
        host.Service.HotStoreReset = underPoint;
        using (new HeldWrite(host.DuckDb))
        {
            await host.Tick();
            await host.Tick();
        }

        Assert.Empty(_log);

        /* A run starts, a tick under the reset point passes, and the run goes on: the next failure is the run's second, not a new first. */
        host.Service.HotStoreReset = attempting;
        using (new HeldWrite(host.DuckDb))
        {
            await host.Tick();
            Assert.Single(_log);

            host.Service.HotStoreReset = underPoint;
            await host.Tick();
            host.Service.HotStoreReset = attempting;
            await host.Tick();
        }

        Assert.Single(_log);
    }

    /// <summary>A reset point at 40% of the file, so the file is over its ceiling (twice the reset point) and the tick attempts the reset.</summary>
    private static HotStoreResetLimit OverTheCeiling(DuckDbInitializer duckDb) =>
        new(duckDb.GetDatabaseSizeMb() * 0.4, HotStoreResetSetting.SourceSetting);

    private int Warnings => _log.Count(l => l.Level == LogLevel.Warning);

    [Fact]
    public async Task TicksWhoseResetDefers_KeepTheRun_SoTheFailedReadsLogOneWarning_NotOnePerTick()
    {
        var host = await OpenHostAsync();
        host.Service.HotStoreReset = OverTheCeiling(host.DuckDb);

        /* An earlier reset's restore is still pending: the reset returns without touching the store, every tick. */
        var marker = Path.Combine(_dir, "archive", PreservedTableRestore.RestoreMarkerFileName);
        File.WriteAllText(marker, "pending");

        using (new HeldWrite(host.DuckDb))
        {
            for (var i = 0; i < 4; i++)
            {
                await host.Tick();
            }
        }

        Assert.True(File.Exists(marker), "the deferred reset must not have touched the pending restore");
        Assert.Equal(1, Warnings);
    }

    [Fact]
    public async Task AResetThatReallyHappens_EndsTheRun_SoTheNextFailureLogsItsOwnFirstLine()
    {
        var host = await OpenHostAsync();
        host.Service.HotStoreReset = OverTheCeiling(host.DuckDb);

        /* The data read fails under the held lock, which logs the run's first line. The reset waits for that lock, so the
           hold is released once the line is out, and the reset then runs for real. */
        Task tick;
        using (new HeldWrite(host.DuckDb))
        {
            tick = host.Tick();
            Assert.True(SpinWait.SpinUntil(() => Warnings >= 1, TimeSpan.FromSeconds(30)), "the failed read was not logged");
        }

        await tick;
        Assert.Contains(_log, l => l.Message.Contains("Archiving all data and resetting the database", StringComparison.Ordinal));

        /* The reset left a fresh, smaller file. Under a held lock a read fails again, and it is a new run. */
        host.Service.HotStoreReset = JustUnder(host.DuckDb);
        using (new HeldWrite(host.DuckDb))
        {
            await host.Tick();
        }

        Assert.Equal(2, Warnings);
    }
}
