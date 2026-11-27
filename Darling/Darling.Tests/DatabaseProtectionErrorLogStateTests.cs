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
using PerformanceMonitor.Collectors;
using Xunit;
using S = PerformanceMonitor.Collectors.DatabaseProtectionErrorLogState;

namespace Darling.Tests;

/// <summary>
/// #8865: the pure state logic of the per-replica CHECKDB judgement. These pin what the collector persists
/// between cycles: the newest check per database, the archive markers, the attempt counts and the instance tie.
/// </summary>
public sealed class DatabaseProtectionErrorLogStateTests
{
    private static readonly DateTime Day0 = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /* Archive n is dated n days before Day0 + 10, so a lower archive number is always newer, as in a real log set. */
    private static DateTime LogDate(int archive) => Day0.AddDays(10 - archive);

    private static List<S.ArchiveInfo> Logs(int count)
        => Enumerable.Range(0, count).Select(n => new S.ArchiveInfo(n, LogDate(n))).ToList();

    private static S.LoadedState Empty()
        => S.Load(new Dictionary<string, string>(), "instance-a");

    private static S.LoadedState WithMarkers(S.Markers markers, Dictionary<DateTime, int>? attempts = null)
        => new(markers, attempts ?? new Dictionary<DateTime, int>(), Array.Empty<DateTime>(), false);

    private static int[] Numbers(S.ErrorLogPlan plan) => plan.Read.Select(a => a.Archive).ToArray();

    [Fact]
    public void AStoredValue_RoundTrips()
    {
        var check = new S.StoredCheck(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified), "physical_only", 2);

        Assert.True(S.StoredCheck.TryParse(check.Format(), out var parsed));
        Assert.Equal(check, parsed);
        Assert.Equal(DateTimeKind.Unspecified, parsed.TimeUtc.Kind);

        Assert.False(S.StoredCheck.TryParse(null, out _));
        Assert.False(S.StoredCheck.TryParse("garbage", out _));
        Assert.False(S.StoredCheck.TryParse("2026-03-04T05:06:07.000||2", out _));

        var attempts = new Dictionary<DateTime, int> { [LogDate(2)] = 1, [LogDate(5)] = 2 };
        Assert.Equal(attempts, S.ParseAttempts(S.FormatAttempts(attempts)));
        var dates = new[] { LogDate(3), LogDate(1) };
        Assert.Equal(dates.OrderBy(d => d), S.ParseDates(S.FormatDates(dates)));
    }

    [Fact]
    public void TheNewerCheckWins_AndAnOlderLineDoesNotReplaceTheStoredOne()
    {
        var stored = new S.StoredCheck(Day0.AddDays(5), "full", 0);
        var older = new S.StoredCheck(Day0.AddDays(2), "physical_only", 3);
        var newer = new S.StoredCheck(Day0.AddDays(8), "physical_only", 1);

        Assert.Equal(stored, S.Effective(stored, older, null));
        Assert.Equal(newer, S.Effective(stored, newer, null));
        Assert.Equal(older, S.Effective(null, older, null));
        Assert.Null(S.Effective(null, null, null));
    }

    [Fact]
    public void ACheckBeforeTheCreateDate_NeverCounts()
    {
        var created = Day0.AddDays(4);
        var before = new S.StoredCheck(Day0.AddDays(1), "full", 0);
        var after = new S.StoredCheck(Day0.AddDays(6), "full", 0);

        Assert.Null(S.Effective(before, null, created));
        Assert.Null(S.Effective(null, before, created));
        Assert.Equal(after, S.Effective(before, after, created));
        Assert.Equal(before, S.Effective(before, null, null));
    }

    [Fact]
    public void AChangedInstance_LoadsEmpty_AndDropsTheStoredChecks()
    {
        var check = new S.StoredCheck(Day0, "full", 0);
        var state = new Dictionary<string, string>
        {
            [S.InstanceKey] = "instance-a",
            [S.NewestArchiveKey] = S.FormatDates(new[] { LogDate(1) }),
            [S.BackfillDoneKey] = bool.TrueString,
            [S.AttemptsKey] = S.FormatAttempts(new Dictionary<DateTime, int> { [LogDate(2)] = 1 }),
            [S.CheckKeyPrefix + "sales"] = check.Format(),
        };

        var same = S.Load(state, "instance-a");
        Assert.False(same.Reset);
        Assert.Equal(LogDate(1), same.Markers.NewestArchive);
        Assert.True(same.Markers.BackfillDone);
        Assert.Equal(1, same.Attempts[LogDate(2)]);
        Assert.Equal(check, S.StoredFor(state, "sales", same.Reset));

        var other = S.Load(state, "instance-b");
        Assert.True(other.Reset);
        Assert.Null(other.Markers.NewestArchive);
        Assert.False(other.Markers.BackfillDone);
        Assert.Empty(other.Attempts);
        Assert.Empty(other.Skipped);
        Assert.Null(S.StoredFor(state, "sales", other.Reset));

        /* A missing instance key is a first run, not a reset. */
        Assert.False(S.Load(new Dictionary<string, string>(), "instance-b").Reset);
    }

    [Fact]
    public void FirstRun_ReadsTheCurrentLogAndThreeNewestArchives()
    {
        var logs = Logs(7);

        var plan1 = S.PlanArchives(Empty(), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 2, 3 }, Numbers(plan1));
        var after1 = S.MarkersAfter(plan1, plan1.Read.Select(a => a.LogDate).ToArray(), logs);
        Assert.Equal(LogDate(3), after1.BackfillBefore);
        Assert.Equal(LogDate(1), after1.NewestArchive);
        Assert.False(after1.BackfillDone);

        /* Archive 0 plus six archives: the second cycle reads the three oldest, and that is when it is done. */
        var plan2 = S.PlanArchives(WithMarkers(after1), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 4, 5, 6 }, Numbers(plan2));
        var after2 = S.MarkersAfter(plan2, plan2.Read.Select(a => a.LogDate).ToArray(), logs);
        Assert.True(after2.BackfillDone);
        Assert.Equal(LogDate(6), after2.BackfillBefore);

        var plan3 = S.PlanArchives(WithMarkers(after2), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0 }, Numbers(plan3));
    }

    [Fact]
    public void FirstRunWithNoArchives_StillReadsTheFirstArchiveThatAppearsLater()
    {
        var onlyCurrent = Logs(1);
        var plan1 = S.PlanArchives(Empty(), onlyCurrent, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0 }, Numbers(plan1));
        var after1 = S.MarkersAfter(plan1, plan1.Read.Select(a => a.LogDate).ToArray(), onlyCurrent);
        Assert.Equal(DateTime.MinValue, after1.NewestArchive);
        Assert.True(after1.BackfillDone);

        /* Through the real state round trip: the minimum date must survive Format and Load. */
        var state = new Dictionary<string, string>
        {
            [S.InstanceKey] = "instance-a",
            [S.NewestArchiveKey] = S.FormatDates(new[] { after1.NewestArchive!.Value }),
            [S.BackfillDoneKey] = bool.TrueString,
        };
        var loaded = S.Load(state, "instance-a");
        Assert.Equal(DateTime.MinValue, loaded.Markers.NewestArchive);

        var rolled = Logs(2);
        var plan2 = S.PlanArchives(loaded, rolled, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1 }, Numbers(plan2));
        var after2 = S.MarkersAfter(plan2, plan2.Read.Select(a => a.LogDate).ToArray(), rolled);
        Assert.Equal(LogDate(1), after2.NewestArchive);
    }

    [Fact]
    public void Steady_ReadsNewArchivesOldestFirst_AndABurstContinuesNextCycle()
    {
        /* Everything older than archive 6 is already read; five archives are new since the marker. */
        var logs = Logs(12);
        var markers = new S.Markers(LogDate(6), LogDate(11), true, 1);

        var plan1 = S.PlanArchives(WithMarkers(markers), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 5, 4, 3 }, Numbers(plan1));
        var after1 = S.MarkersAfter(plan1, plan1.Read.Select(a => a.LogDate).ToArray(), logs);
        Assert.Equal(LogDate(3), after1.NewestArchive);

        var plan2 = S.PlanArchives(WithMarkers(after1), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 2, 1 }, Numbers(plan2));
        var after2 = S.MarkersAfter(plan2, plan2.Read.Select(a => a.LogDate).ToArray(), logs);
        Assert.Equal(LogDate(1), after2.NewestArchive);
        Assert.True(after2.BackfillDone);
    }

    [Fact]
    public void AMiddleArchiveFailing_HoldsTheMarkerAtIt()
    {
        var logs = Logs(7);
        var plan = S.PlanArchives(Empty(), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 2, 3 }, Numbers(plan));

        /* Archive 2 failed; 1 and 3 were read. The marker stops after 1, never jumping to 3. */
        var readOk = new[] { LogDate(0), LogDate(1), LogDate(3) };
        var after = S.MarkersAfter(plan, readOk, logs);
        Assert.Equal(LogDate(1), after.BackfillBefore);
        Assert.False(after.BackfillDone);

        var retry = S.PlanArchives(WithMarkers(after), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 2, 3, 4 }, Numbers(retry));
    }

    [Fact]
    public void AnArchiveAtTwoAttempts_IsSkipped_AndTheBackfillFinishesPastIt()
    {
        var logs = Logs(4);
        var attempts = new Dictionary<DateTime, int> { [LogDate(2)] = S.MaxArchiveAttempts };
        var loaded = WithMarkers(new S.Markers(LogDate(1), null, false, 1), attempts);

        var plan = S.PlanArchives(loaded, logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 3 }, Numbers(plan));
        Assert.Equal(new[] { LogDate(2) }, plan.NewlySkipped);

        /* Archive 2 was not read, but it is skipped, so the run goes through it and the backfill is done. */
        var after = S.MarkersAfter(plan, new[] { LogDate(0), LogDate(1), LogDate(3) }, logs);
        Assert.Equal(LogDate(3), after.BackfillBefore);
        Assert.True(after.BackfillDone);
    }

    [Fact]
    public void ARefusedEnumeration_ReadsTheCurrentLogOnly_AndMovesNoMarker()
    {
        /* #8865 I2: the list says nothing about which archives exist, so nothing it implies may be stored: not even "the backfill is done". */
        var midway = new S.Markers(LogDate(1), LogDate(3), false);
        var attempts = new Dictionary<DateTime, int> { [LogDate(4)] = 1 };
        foreach (var loaded in new[] { Empty(), WithMarkers(midway, attempts) })
        {
            var plan = S.PlanArchives(loaded, new List<S.ArchiveInfo>(), false, S.ArchivesPerCycle, 27219);

            Assert.Equal(new[] { 0 }, Numbers(plan));
            Assert.False(plan.EnumerateOk);
            Assert.Equal(27219, plan.EnumerateError);
            Assert.Empty(plan.NewlySkipped);
            Assert.Same(loaded, plan.Before);
            Assert.Equal(loaded.Markers, S.MarkersAfter(plan, new[] { default(DateTime) }, new List<S.ArchiveInfo>()));
        }
    }

    /* #8865 I1: logs cycled within one minute. Archives 1 to 5 share one date, 6 is older. */
    private static List<S.ArchiveInfo> SameMinute(int sameCount, int total)
        => Enumerable.Range(0, total)
            .Select(n => new S.ArchiveInfo(n, n == 0 ? LogDate(0) : n <= sameCount ? LogDate(1) : LogDate(n)))
            .ToList();

    private static DateTime[] ReadWhole(S.ErrorLogPlan plan) => S.DatesReadWhole(plan, _ => true).ToArray();

    [Fact]
    public void FiveArchivesInOneMinute_AreAllReadInOneCycle_AndTheDateIsNotDoneUntilEveryOneReads()
    {
        /* Before the fix the plan took three of the five, and reading them marked the date (so all five) read: archives 4 and 5 were
           never read, and a CHECKDB line only they held was lost. */
        var logs = SameMinute(5, 7);

        var plan1 = S.PlanArchives(Empty(), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, Numbers(plan1));
        Assert.Equal(new[] { LogDate(1) }, S.PlannedDates(plan1));

        /* One archive of the date failing keeps the whole date unread: the backfill marker does not move over it. */
        var partly = S.DatesReadWhole(plan1, archive => archive != 4);
        Assert.Empty(partly);
        var held = S.MarkersAfter(plan1, partly, logs);
        Assert.Null(held.BackfillBefore);
        Assert.False(held.BackfillDone);

        var after1 = S.MarkersAfter(plan1, ReadWhole(plan1), logs);
        Assert.Equal(LogDate(1), after1.BackfillBefore);
        var plan2 = S.PlanArchives(WithMarkers(after1), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 6 }, Numbers(plan2));
        Assert.True(S.MarkersAfter(plan2, ReadWhole(plan2), logs).BackfillDone);
    }

    [Fact]
    public void AStoreWithTheBackfillDoneButNoSteadyMarker_BackfillsOnItsFirstReadableList()
    {
        /* #8865: an earlier build marked the backfill done on a refused list and set no steady marker. Taken as it stands, that store
           would set the steady marker to the newest archive with the backfill already done, and no retained archive would ever be read. */
        var state = new Dictionary<string, string>
        {
            [S.InstanceKey] = "instance-a",
            [S.BackfillDoneKey] = bool.TrueString,
        };

        var plan = S.PlanArchives(S.Load(state, "instance-a"), Logs(5), true, S.ArchivesPerCycle);

        Assert.False(plan.Before.Markers.BackfillDone);
        Assert.Equal(new[] { 0, 1, 2, 3 }, Numbers(plan));
        Assert.False(S.MarkersAfter(plan, ReadWhole(plan), Logs(5)).BackfillDone);
    }

    [Fact]
    public void AMinuteHoldingMoreArchivesThanTheListCarries_IsReadInPart_AndSkippedInThatSameCycle()
    {
        /* #8865: a date read alone had no size cap, and the archive list is bound as nvarchar(128). Sixty archives in one minute made a
           list the server cut short, so the batch failed (error 537) every cycle until the collector timed out. Capped but not skipped,
           the date was never read whole and every read of it went fine, so it gained no attempt and the backfill stalled on it. */
        var logs = SameMinute(60, 62);

        var plan1 = S.PlanArchives(Empty(), logs, true, S.ArchivesPerCycle);
        Assert.True(S.ArchiveList(plan1.Read).Length <= S.MaxArchiveListLength);
        Assert.Equal(Enumerable.Range(0, plan1.Read.Count), Numbers(plan1));
        Assert.True(plan1.Read.Count > S.ArchivesPerCycle + 1);
        Assert.Equal(new[] { LogDate(1) }, plan1.NewlySkipped);

        /* Skipped, the date is done: the backfill marker moves past it although it was not read whole. */
        var after1 = S.MarkersAfter(plan1, ReadWhole(plan1), logs);
        Assert.Equal(LogDate(1), after1.BackfillBefore);
        var plan2 = S.PlanArchives(new S.LoadedState(after1, new Dictionary<DateTime, int>(), plan1.NewlySkipped.ToList(), false), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 61 }, Numbers(plan2));
        Assert.True(S.MarkersAfter(plan2, ReadWhole(plan2), logs).BackfillDone);
    }

    [Fact]
    public void ADateThatDoesNotFitTheCycle_WaitsForTheNextOne_RatherThanBeingSplit()
    {
        /* Archives 1 and 2 share a date, 3 and 4 share another: two then two, never three then one. */
        var logs = new List<S.ArchiveInfo>
        {
            new(0, LogDate(0)), new(1, LogDate(1)), new(2, LogDate(1)), new(3, LogDate(3)), new(4, LogDate(3)),
        };

        var plan1 = S.PlanArchives(Empty(), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 2 }, Numbers(plan1));
        var after1 = S.MarkersAfter(plan1, ReadWhole(plan1), logs);
        var plan2 = S.PlanArchives(WithMarkers(after1), logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 3, 4 }, Numbers(plan2));
    }

    [Fact]
    public void Steady_ANewArchiveClosedInTheMarkersMinute_IsRead()
    {
        /* The marker is archive 1's date, read when two archives carried it. A roll in that same minute adds a third: before the fix the
           steady walk took only dates after the marker, so the new archive, and the lines written just before the roll, were never read. */
        var markers = new S.Markers(LogDate(1), LogDate(4), true, 2);
        var rolled = new List<S.ArchiveInfo>
        {
            new(0, LogDate(0)), new(1, LogDate(1)), new(2, LogDate(1)), new(3, LogDate(1)), new(4, LogDate(4)),
        };

        var plan = S.PlanArchives(WithMarkers(markers), rolled, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 2, 3 }, Numbers(plan));

        var after = S.MarkersAfter(plan, ReadWhole(plan), rolled);
        Assert.Equal(LogDate(1), after.NewestArchive);
        Assert.Equal(3, after.NewestArchiveCount);
        Assert.Equal(new[] { 0 }, Numbers(S.PlanArchives(WithMarkers(after), rolled, true, S.ArchivesPerCycle)));

        /* Not read whole: the marker keeps its old count, so the next cycle tries the date again. */
        var failed = S.MarkersAfter(plan, Array.Empty<DateTime>(), rolled);
        Assert.Equal(2, failed.NewestArchiveCount);

        /* An archive of the marker's minute aging out lowers the count, and the next roll in that minute still shows as new. */
        var agedOut = rolled.Where(a => a.Archive != 3 && a.Archive != 4).ToList();
        var quiet = S.PlanArchives(WithMarkers(after), agedOut, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0 }, Numbers(quiet));
        Assert.Equal(2, S.MarkersAfter(quiet, Array.Empty<DateTime>(), agedOut).NewestArchiveCount);
    }

    [Fact]
    public void TheSteadyMarker_RoundTripsWithItsCount_AndABareDateLoadsAsCountZero()
    {
        var markers = new S.Markers(LogDate(1), null, true, 3);
        var state = new Dictionary<string, string> { [S.NewestArchiveKey] = S.FormatNewestArchive(markers) };
        var loaded = S.Load(state, "instance-a");
        Assert.Equal(LogDate(1), loaded.Markers.NewestArchive);
        Assert.Equal(3, loaded.Markers.NewestArchiveCount);

        var bare = S.Load(new Dictionary<string, string> { [S.NewestArchiveKey] = S.FormatDates(new[] { LogDate(1) }) }, "instance-a");
        Assert.Equal(LogDate(1), bare.Markers.NewestArchive);
        Assert.Equal(0, bare.Markers.NewestArchiveCount);

        Assert.Equal(string.Empty, S.FormatNewestArchive(new S.Markers(null, null, false)));
    }

    [Fact]
    public void StageIfChanged_StagesNothingForAnUnchangedValue()
    {
        var state = new Dictionary<string, string> { ["k"] = "v" };
        var pending = new Dictionary<string, string>();

        S.StageIfChanged(pending, state, "k", "v");
        Assert.Empty(pending);

        S.StageIfChanged(pending, state, "k", "w");
        S.StageIfChanged(pending, state, "new", "x");
        Assert.Equal("w", pending["k"]);
        Assert.Equal("x", pending["new"]);
    }

    [Fact]
    public void AnEmptyStoredValue_IsNoCheck()
    {
        /* #8865: an instance reset stages an empty value over each old check, which must read as none rather than as garbage. */
        var state = new Dictionary<string, string> { [S.CheckKeyPrefix + "sales"] = string.Empty };

        Assert.False(S.StoredCheck.TryParse(string.Empty, out _));
        Assert.Null(S.StoredFor(state, "sales", false));
    }

    [Fact]
    public void ANameLikeAnother_KeepsItsOwnKey()
    {
        /* A database may be called like a server-scoped key, or be a prefix of another database's name. */
        var sales = new S.StoredCheck(Day0, "full", 0);
        var salesArchive = new S.StoredCheck(Day0.AddDays(1), "physical_only", 4);
        var state = new Dictionary<string, string>
        {
            [S.CheckKeyPrefix + "sales"] = sales.Format(),
            [S.CheckKeyPrefix + "sales:archive"] = salesArchive.Format(),
            [S.InstanceKey] = "instance-a",
        };

        Assert.Equal(sales, S.StoredFor(state, "sales", false));
        Assert.Equal(salesArchive, S.StoredFor(state, "sales:archive", false));
        Assert.Null(S.StoredFor(state, "sal", false));
        Assert.Null(S.StoredFor(state, "errorlog_instance", false));

        var serverKeys = new[]
        {
            S.InstanceKey, S.NewestArchiveKey, S.BackfillBeforeKey, S.BackfillDoneKey, S.AttemptsKey, S.SkippedKey,
        };
        Assert.All(serverKeys, k => Assert.False(k.StartsWith(S.CheckKeyPrefix, StringComparison.Ordinal)));
    }
}
