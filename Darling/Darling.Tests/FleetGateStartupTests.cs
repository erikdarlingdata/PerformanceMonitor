/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5597: "Collection Falling Behind" fired in the first hour after every start on a large store that was not behind. The
/// first minutes after a start (or after the sweep loop comes back from a sleep or a pause) leave out of the alert's window.
/// </summary>
public sealed class FleetGateStartupTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTime T0 = new(2026, 10, 8, 13, 37, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    /// <summary>A service start at <see cref="T0"/> and a stats clock the test moves.</summary>
    private sealed class Clock
    {
        public DateTime Now = T0;
        public FleetGateStats Stats { get; }
        public SkipCreditFloor Floor { get; } = new();

        public Clock()
        {
            Stats = new FleetGateStats(() => Now);
            Floor.Tick(T0);
        }

        public void Record(long run, long skipped)
        {
            for (long i = 0; i < run; i++)
            {
                Stats.RecordSlot(0);
            }

            Stats.RecordSkippedSlots(skipped);
        }

        public DarlingSelfAlertEvaluator.FleetGateReport Read() =>
            DarlingWorker.ReadFleetGate(Stats, Floor.Since, 4, Now).Report;
    }

    /// <summary>
    /// A model of the start: 44 servers with 20 one-minute collectors each, a gate 4 permits wide, a loop that ticks every 15
    /// seconds and launches a server's body when it has something due. A server's first body connects and seeds its due
    /// stamps (30 seconds, like the on-load snapshots); its later bodies run what is due. Bodies queue FIFO for a permit, so the
    /// first collection body of the first server to connect waits behind every other server's connect body.
    /// </summary>
    private sealed class FleetModel
    {
        private const int Servers = 44;
        private const int Collectors = 20;
        private const int Width = 4;
        private static readonly TimeSpan Tick = TimeSpan.FromSeconds(15);  /* the sweep loop's tick; the model itself steps by the second */
        private static readonly TimeSpan ConnectBody = TimeSpan.FromSeconds(30);

        private sealed class Server
        {
            public bool Connected;
            public readonly DateTime[] Due = new DateTime[Collectors];
            public long SeedFinishedTicks;
            public DateTime? RunningUntil;
            public DateTime StartedAt;
            public bool Queued;
        }

        private readonly Server[] _servers = Enumerable.Range(0, Servers).Select(_ => new Server()).ToArray();
        private readonly List<Server> _queue = [];
        private readonly Clock _clock;
        private readonly Func<DateTime, TimeSpan> _bodyPerCollector;

        public FleetModel(Clock clock, Func<DateTime, TimeSpan> bodyPerCollector, bool countFromSeedFinish = true)
        {
            _clock = clock;
            _bodyPerCollector = bodyPerCollector;
            CountFromSeedFinish = countFromSeedFinish;
        }

        /// <summary>Whether a server's slots count from the instant its seeding finished (the #5597 count fix).</summary>
        public bool CountFromSeedFinish { get; }

        /// <summary>Runs the model to <paramref name="until"/>, calling <paramref name="everyMinute"/> on each whole minute.</summary>
        public void Run(DateTime until, Action<DateTime> everyMinute)
        {
            for (var now = T0; now <= until; now += TimeSpan.FromSeconds(1))
            {
                _clock.Now = now;
                var loopTick = (now - T0).Ticks % Tick.Ticks == 0;
                if (loopTick)
                {
                    _clock.Floor.Tick(now);
                }

                Step(now, loopTick);
                if ((now - T0).Ticks % Minute.Ticks == 0)
                {
                    everyMinute(now);
                }
            }
        }

        private void Step(DateTime now, bool loopTick)
        {
            foreach (var s in _servers.Where(x => x.RunningUntil is { } u && u <= now))
            {
                s.RunningUntil = null;
                if (!s.Connected)
                {
                    s.Connected = true;
                    for (var i = 0; i < Collectors; i++)
                    {
                        s.Due[i] = s.StartedAt + TimeSpan.FromSeconds(i % 30);
                    }

                    s.SeedFinishedTicks = CountFromSeedFinish ? now.Ticks : 0;
                }
            }

            foreach (var s in loopTick ? _servers : [])
            {
                if (s.RunningUntil is null && !s.Queued && (!s.Connected || s.Due.Any(d => d <= now)))
                {
                    s.Queued = true;
                    _queue.Add(s);
                }
            }

            while (_queue.Count > 0 && _servers.Count(x => x.RunningUntil is not null) < Width)
            {
                var s = _queue[0];
                _queue.RemoveAt(0);
                s.Queued = false;
                s.StartedAt = now;
                if (!s.Connected)
                {
                    s.RunningUntil = now + ConnectBody;
                    continue;
                }

                var ran = 0;
                for (var i = 0; i < Collectors; i++)
                {
                    if (s.Due[i] > now)
                    {
                        continue;
                    }

                    var interval = Minute;
                    _clock.Stats.RecordSlot(_clock.Floor.Skipped(s.Due[i], now, interval, new DateTime(s.SeedFinishedTicks, DateTimeKind.Utc)));
                    s.Due[i] = CollectorCadence.NextDue(s.Due[i], now, interval);
                    ran++;
                }

                s.RunningUntil = now + TimeSpan.FromTicks(_bodyPerCollector(now).Ticks * ran);
            }
        }
    }

    [Fact]
    public async Task EveryCollectorDueAtOnceBehindAGateOfWidthFour_SkipsSlotsInTheFirstMinutes_AndTheAlertDoesNotJudgeThem()
    {
        var clock = new Clock();
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var model = new FleetModel(clock, _ => TimeSpan.FromMilliseconds(100));

        bool? oldRuleFiredEarly = null;
        var firstJudged = default(DateTime?);
        model.Run(T0.AddMinutes(100), now =>
        {
            h.Now = now;
            var minutes = (int)(now - T0).TotalMinutes;
            if (minutes == 8)
            {
                /* What the alert read before: the full hour, taken at face value, over its thresholds. */
                var raw = clock.Stats.Snapshot();
                oldRuleFiredEarly = new DarlingSelfAlertEvaluator.FleetGateReport(
                    raw.Run, raw.Skipped, raw.QueueWaits, raw.QueueWaitTotal, raw.QueueWaitMax, 4, now).IsBehind;
            }

            var report = clock.Read();
            if (report.IsJudged && firstJudged is null)
            {
                firstJudged = now;
            }

            e.ApplyFleetGateAsync(report, Ct).GetAwaiter().GetResult();
        });

        Assert.True(oldRuleFiredEarly, "the model must reproduce the start-up skips the old rule fired on");

        /* The minutes after the start-up window are clean, and the alert never fires. */
        Assert.Equal(T0.AddMinutes(DarlingSelfAlertEvaluator.FleetGateStartupMinutes + DarlingSelfAlertEvaluator.FleetGateMinJudgedMinutes), firstJudged);
        Assert.Equal(0, clock.Stats.SnapshotLastMinutes(60).Skipped);
        Assert.Empty(h.Deliverer.Outcomes);
        await Task.CompletedTask;
    }

    [Fact]
    public void ASlotThatCameDueWhileTheServersConnectBodyRan_IsNotCountedAsSkipped()
    {
        /* The model with the old count (every slot since the stamp) skips more than the model that counts from the end of the
           seeding: the difference is the slots that could not have run, because the server's only body was the one seeding. */
        long CountedWith(bool fromSeedFinish)
        {
            var clock = new Clock();
            var model = new FleetModel(clock, _ => TimeSpan.FromMilliseconds(100), fromSeedFinish);
            model.Run(T0.AddMinutes(12), _ => { });
            return clock.Stats.SnapshotLastMinutes(60).Skipped;
        }

        Assert.True(CountedWith(true) < CountedWith(false));
    }

    [Theory]
    [InlineData(787, 57)]
    [InlineData(613, 164)]
    [InlineData(916, 291)]
    [InlineData(1503, 285)]
    [InlineData(1277, 247)]
    [InlineData(1243, 114)]
    public async Task TheShortWindowShares_RightAfterAStart_DoNotFire(long ran, long skipped)
    {
        var clock = new Clock();
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* The field lines ("N slots ran, M skipped"), spread over the first four minutes after the start. */
        for (var m = 0; m < 4; m++)
        {
            clock.Now = T0.AddMinutes(m);
            clock.Record(ran / 4, skipped / 4);
        }

        foreach (var m in new[] { 4, 14, 20, 29 })
        {
            clock.Now = T0.AddMinutes(m);
            h.Now = clock.Now;
            var report = clock.Read();
            Assert.False(report.IsJudged);
            Assert.False(report.IsBehind);
            Assert.False(await e.EvaluateFleetGateAsync(report, Ct));
        }

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Theory]
    [InlineData(950, 50, true)]
    [InlineData(951, 49, false)]
    [InlineData(980, 20, false)]
    [InlineData(1, 19, false)]
    public async Task AFullHour_StillFiresAtFivePercentAndTwentySlots_AndNotUnderIt(long run, long skipped, bool fires)
    {
        var clock = new Clock();
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* The start is two hours back; the last hour carries the counts, spread over its minutes. */
        for (var m = 60; m < 120; m++)
        {
            clock.Now = T0.AddMinutes(m + 1);
            clock.Record(run / 60 + (m - 60 < run % 60 ? 1 : 0), skipped / 60 + (m - 60 < skipped % 60 ? 1 : 0));
        }

        clock.Now = T0.AddMinutes(120);
        h.Now = clock.Now;
        var report = clock.Read();
        Assert.Equal(FleetGateStats.WindowMinutes, report.JudgedMinutes);
        await e.ApplyFleetGateAsync(report, Ct);

        Assert.Equal(fires, h.Deliverer.Outcomes.Count == 1);
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(64, true)]
    public async Task AStartUpBurst_NeverFiresOnACleanStore_AndAStoreStillBehindAfterItFiresWhenTheWindowIsLongEnough(long backlogPerMinute, bool fires)
    {
        var clock = new Clock();
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        DateTime? firedAt = null;

        for (var m = 0; m <= 90; m++)
        {
            clock.Now = T0.AddMinutes(m);
            h.Now = clock.Now;
            clock.Floor.Tick(clock.Now);

            /* 1,000 slots a minute. The first 10 minutes: 30% skipped. Then the store either keeps up or sits at 6%. */
            var skipped = m < 10 ? 300 : backlogPerMinute;
            clock.Record(1000 - skipped, skipped);

            var before = h.Deliverer.Outcomes.Count;
            await e.ApplyFleetGateAsync(clock.Read(), Ct);
            if (h.Deliverer.Outcomes.Count > before)
            {
                firedAt ??= clock.Now;
            }
        }

        if (!fires)
        {
            Assert.Null(firedAt);
            return;
        }

        /* Behind from its start-up minutes on: it fires as soon as the window is long enough, and not a minute later. */
        var latest = T0.AddMinutes(DarlingSelfAlertEvaluator.FleetGateStartupMinutes + DarlingSelfAlertEvaluator.FleetGateMinJudgedMinutes);
        Assert.Equal(latest, firedAt);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains($"in the last {DarlingSelfAlertEvaluator.FleetGateMinJudgedMinutes} minutes", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("of 16,000 due slots", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains($"In the {DarlingSelfAlertEvaluator.FleetGateMinJudgedMinutes} minutes to", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("in the last hour", fired.ShortMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStandingAlert_NeitherResolvesNorLosesItsStanding_WhileTheWindowIsTooShort()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var start = h.Now;

        await e.ApplyFleetGateAsync(new(300, 100, 1, TimeSpan.Zero, TimeSpan.Zero, 4, start), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* A pause and resume leaves a short window: quiet counts in it do not start the quiet clock, however long it stays short. */
        h.Now = start.AddHours(3);
        var shortQuiet = new DarlingSelfAlertEvaluator.FleetGateReport(2000, 0, 1, TimeSpan.Zero, TimeSpan.Zero, 4, h.Now, JudgedMinutes: 3);
        Assert.True(await e.EvaluateFleetGateAsync(shortQuiet, Ct));
        Assert.Empty(h.History.Records);

        /* The quiet-hold logic is the same for a store past its first hour: the first judged quiet reading starts the clock. */
        var quiet = shortQuiet with { JudgedMinutes = 60 };
        await e.ApplyFleetGateAsync(quiet, Ct);
        h.Now = start.AddHours(4).AddMinutes(-1);
        await e.ApplyFleetGateAsync(quiet, Ct);
        Assert.Empty(h.History.Records);
        h.Now = start.AddHours(4);
        await e.ApplyFleetGateAsync(quiet, Ct);
        Assert.Single(h.History.Records);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(14, 0)]
    [InlineData(15, 0)]
    [InlineData(16, 1)]
    [InlineData(30, 15)]
    [InlineData(75, 60)]
    [InlineData(600, 60)]
    public void TheJudgedWindow_StartsAfterTheStartUpMinutes_AndNeverExceedsTheHour(int minutesSinceLoopStart, int expected)
    {
        Assert.Equal(expected, DarlingSelfAlertEvaluator.FleetGateJudgedMinutes(T0.AddMinutes(minutesSinceLoopStart), T0));
    }

    [Fact]
    public void TheJudgedWindow_RoundsItsStartUpToAWholeMinute_AndIsEmptyBeforeTheLoopHasTicked()
    {
        /* 13:37:20 + 15 minutes is 13:52:20, so the first whole minute is 13:53:00. */
        var since = T0.AddSeconds(20);
        Assert.Equal(0, DarlingSelfAlertEvaluator.FleetGateJudgedMinutes(T0.AddMinutes(16).AddSeconds(59), since));
        Assert.Equal(1, DarlingSelfAlertEvaluator.FleetGateJudgedMinutes(T0.AddMinutes(17), since));
        Assert.Equal(0, DarlingSelfAlertEvaluator.FleetGateJudgedMinutes(T0.AddHours(5), null));
    }

    [Fact]
    public void TheLoopsStart_IsItsFirstTick_AndEveryReturnFromAStretchItWasNotRunningRestartsIt()
    {
        var floor = new SkipCreditFloor();
        Assert.Null(floor.Since);

        floor.Tick(T0);
        Assert.Equal(T0, floor.Since);

        /* Ticks on time leave it alone. */
        floor.Tick(T0.AddSeconds(15));
        Assert.Equal(T0, floor.Since);

        /* A sleep (a gap over MaxTickGap) restarts it, and so does a pause's resume. */
        var back = T0.AddHours(2);
        floor.Tick(back);
        Assert.Equal(back, floor.Since);
        var resumed = back.AddMinutes(10);
        floor.Resume(resumed);
        Assert.Equal(resumed, floor.Since);
    }

    [Fact]
    public void ALastMinutesRead_CountsOnlyTheNewestBuckets_AndTheFullWindowEqualsTheHourSnapshot()
    {
        var clock = new Clock();
        for (var m = 0; m < 30; m++)
        {
            clock.Now = T0.AddMinutes(m);
            clock.Record(10, m < 10 ? 5 : 0);
        }

        Assert.Equal(clock.Stats.Snapshot(), clock.Stats.SnapshotLastMinutes(60));
        Assert.Equal(clock.Stats.Snapshot(), clock.Stats.SnapshotLastMinutes(500));

        var last20 = clock.Stats.SnapshotLastMinutes(20);
        Assert.Equal(200, last20.Run);
        Assert.Equal(0, last20.Skipped);
        Assert.Equal(50, clock.Stats.SnapshotLastMinutes(30).Skipped);
        Assert.Equal(default, clock.Stats.SnapshotLastMinutes(0));
    }

    [Fact]
    public void ASeedFloor_CountsASlotOnlyFromTheEndOfTheSeeding()
    {
        var floor = new SkipCreditFloor();
        floor.Tick(T0);
        var due = T0;
        var now = T0.AddSeconds(100);

        /* Due at T0 and run at +100 s: one slot stepped over. The body that seeded it ran until +70 s, so it had only +30 s to run. */
        Assert.Equal(1, floor.Skipped(due, now, Minute));
        Assert.Equal(0, floor.Skipped(due, now, Minute, T0.AddSeconds(70)));

        /* A seed floor older than the loop's own floor changes nothing, and neither does none. */
        Assert.Equal(1, floor.Skipped(due, now, Minute, T0.AddSeconds(-10)));
        Assert.Equal(1, floor.Skipped(due, now, Minute, default));
    }

    [Theory]
    [InlineData(5, 5)]
    [InlineData(4.5, 5)]
    [InlineData(0.2, 1)]
    [InlineData(59.5, 60)]
    [InlineData(60, 60)]
    [InlineData(300, 60)]
    public void TheLogLine_NamesTheRealSpan_WhileTheServiceHasRunUnderAnHour(double minutesSinceStart, int expectedSpan)
    {
        var clock = new Clock();
        clock.Record(100, 30);
        var span = FleetGateLine.SpanMinutes(T0.AddMinutes(minutesSinceStart), clock.Floor.FirstTick);
        Assert.Equal(expectedSpan, span);

        var line = FleetGateLine.Describe(clock.Stats.Snapshot(), 4, span);
        if (expectedSpan < FleetGateStats.WindowMinutes)
        {
            Assert.Contains($"last {expectedSpan} minutes, since the service started: 100 collector slots ran", line, StringComparison.Ordinal);
            Assert.DoesNotContain("last 60 minutes", line, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("last 60 minutes: 100 collector slots ran", line, StringComparison.Ordinal);
            Assert.DoesNotContain("since the service started", line, StringComparison.Ordinal);
        }

        Assert.Equal(60, FleetGateLine.SpanMinutes(T0, null));
    }

    [Fact]
    public void TheLogLinesWarningLevel_FollowsTheAlertsJudgment_NotTheRawCounts()
    {
        var clock = new Clock();
        clock.Record(700, 300);

        /* 5 minutes in: the raw hour is at 30%, and the line is not a Warning, because the alert does not judge these minutes. */
        for (var m = 1; m <= 5; m++)
        {
            clock.Floor.Tick(T0.AddMinutes(m));
        }

        clock.Now = T0.AddMinutes(5);
        var early = clock.Read();
        Assert.True(new DarlingSelfAlertEvaluator.FleetGateReport(700, 300, 0, TimeSpan.Zero, TimeSpan.Zero, 4, clock.Now).IsBehind);
        Assert.False(DarlingWorker.FleetGateLogIsBehind(false, early));

        /* A standing alert is a Warning whatever the window. */
        Assert.True(DarlingWorker.FleetGateLogIsBehind(true, early));

        /* Past the start-up minutes and behind: a Warning, with the real span in the line. */
        for (var m = 6; m <= 31; m++)
        {
            clock.Now = T0.AddMinutes(m);
            clock.Floor.Tick(clock.Now);
            if (m >= DarlingSelfAlertEvaluator.FleetGateStartupMinutes)
            {
                clock.Record(936, 64);
            }
        }

        var late = clock.Read();
        Assert.True(late.IsJudged);
        Assert.True(DarlingWorker.FleetGateLogIsBehind(false, late));
        var (full, _) = DarlingWorker.ReadFleetGate(clock.Stats, clock.Floor.Since, 4, clock.Now);
        Assert.Contains("last 31 minutes, since the service started", FleetGateLine.Describe(full, 4, FleetGateLine.SpanMinutes(clock.Now, clock.Floor.FirstTick)), StringComparison.Ordinal);
    }

    [Fact]
    public void TheConnectBody_StampsWhenItFinishedSeeding_AndTheCollectorPassCountsFromThere()
    {
        /* The connect path needs a store, so this is a source pin: the stamp is written after the seeding loop and before the
           pass counts from it. The behaviour is in ASeedFloor_CountsASlotOnlyFromTheEndOfTheSeeding. */
        var source = ServerConnectBackoffTests.ReadWorkerSource();

        var stamp = source.IndexOf("Interlocked.Exchange(ref server.SeedFinishedTicks, DateTime.UtcNow.Ticks);", StringComparison.Ordinal);
        var seeding = source.IndexOf("server.NextDue[name] = ComputeSeededNextDue(lastRun, effective.FrequencyMinutes, now, jitter);", StringComparison.Ordinal);
        Assert.True(seeding >= 0 && stamp > seeding, "the stamp must follow the seeding of the due times");
        Assert.Contains(
            "var seeded = new DateTime(Interlocked.Read(ref server.SeedFinishedTicks), DateTimeKind.Utc);",
            source, StringComparison.Ordinal);
    }
}
