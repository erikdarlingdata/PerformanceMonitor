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
        var markers = new S.Markers(LogDate(6), LogDate(11), true);

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
        var loaded = WithMarkers(new S.Markers(LogDate(1), null, false), attempts);

        var plan = S.PlanArchives(loaded, logs, true, S.ArchivesPerCycle);
        Assert.Equal(new[] { 0, 1, 3 }, Numbers(plan));
        Assert.Equal(new[] { LogDate(2) }, plan.NewlySkipped);

        /* Archive 2 was not read, but it is skipped, so the run goes through it and the backfill is done. */
        var after = S.MarkersAfter(plan, new[] { LogDate(0), LogDate(1), LogDate(3) }, logs);
        Assert.Equal(LogDate(3), after.BackfillBefore);
        Assert.True(after.BackfillDone);
    }

    [Fact]
    public void ARefusedEnumeration_ReadsTheCurrentLogOnly_AndTheBackfillIsDone()
    {
        var plan = S.PlanArchives(Empty(), new List<S.ArchiveInfo>(), false, S.ArchivesPerCycle);

        Assert.Equal(new[] { 0 }, Numbers(plan));
        Assert.False(plan.EnumerateOk);
        Assert.Empty(plan.NewlySkipped);

        var after = S.MarkersAfter(plan, new[] { default(DateTime) }, new List<S.ArchiveInfo>());
        Assert.True(after.BackfillDone);
        Assert.Null(after.NewestArchive);
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
