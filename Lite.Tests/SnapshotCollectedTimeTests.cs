/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: a surface that draws the NEWEST snapshot (of the whole store, or of the selected range) says when that snapshot
/// was collected, so a reader can tell a stale snapshot from a fresh one. A grid does it with a <c>Collected</c> column,
/// the way the Automatic Tuning grid does: the row's collection time in the display zone, to the second, sorted by the
/// stored instant (the XAML side of that is pinned in <see cref="GridTimeColumnSortMemberTests"/>, whose table lists
/// every such column). A summary strip does it with <c>Collected</c> and the time at its end.
///
/// <para>The tests here pin the text each row type words, that rows from different snapshots each show their own time, and
/// where each summary strip's figure comes from and sits.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class SnapshotCollectedTimeTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    private static string Name(Type type) => type.AssemblyQualifiedName!;

    /// <summary>(the row type, the text property its Collected column binds, the stored UTC instant it is worded from).</summary>
    public static TheoryData<string, string, string> SnapshotRows => new()
    {
        { Name(typeof(CpuSchedulerGridRow)), "CollectionTimeLocal", "CollectionTime" },
        { Name(typeof(LatchStatsSnapshotRow)), "CollectionTimeLocal", "CollectionTime" },
        { Name(typeof(SpinlockStatsSnapshotRow)), "CollectionTimeLocal", "CollectionTime" },
        { Name(typeof(ServerConfigRow)), "CaptureTimeLocal", "CaptureTime" },
        { Name(typeof(DatabaseConfigRow)), "CaptureTimeLocal", "CaptureTime" },
        { Name(typeof(DatabaseScopedConfigRow)), "CaptureTimeLocal", "CaptureTime" },
        { Name(typeof(QueryStoreHealthRow)), "CaptureTimeLocal", "CaptureTime" },
        { Name(typeof(TraceFlagRow)), "CaptureTimeLocal", "CaptureTime" },
        { Name(typeof(RunningJobRow)), "CollectionTimeLocal", "CollectionTime" },
    };

    private static object NewRow(string rowTypeName, string storedProperty, DateTime storedUtc)
    {
        var type = Type.GetType(rowTypeName, throwOnError: true)!;
        var row = Activator.CreateInstance(type)!;
        var stored = type.GetProperty(storedProperty, BindingFlags.Public | BindingFlags.Instance);
        Assert.True(stored is { CanWrite: true }, $"{type.Name} has no settable {storedProperty}.");
        stored!.SetValue(row, storedUtc);
        return row;
    }

    private static string TextOf(object row, string textProperty)
    {
        var property = row.GetType().GetProperty(textProperty, BindingFlags.Public | BindingFlags.Instance);
        Assert.True(property != null, $"{row.GetType().Name} has no public property {textProperty}, so its Collected column has no text to show.");
        return (string)property!.GetValue(row)!;
    }

    /// <summary>
    /// The text is the stored instant in the display zone, to the second: 16:27:06Z is 12:27:06 on a US Eastern clock in
    /// October (UTC-4) and 16:27:06 in UTC mode. The seconds are non-zero on purpose, so a format that drops them fails.
    /// </summary>
    [Theory]
    [MemberData(nameof(SnapshotRows))]
    public void TheCollectedText_IsTheStoredInstant_InTheDisplayZone_ToTheSecond(string rowTypeName, string textProperty, string storedProperty)
    {
        var row = NewRow(rowTypeName, storedProperty, DisplayZoneFixtures.At(2026, 10, 3, 16, 27, 6));
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-10-03 12:27:06", TextOf(row, textProperty));

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-10-03 16:27:06", TextOf(row, textProperty));
    }

    /// <summary>
    /// The second 01:30 of the autumn change day (06:30:05Z on a US Eastern clock) keeps its -05:00 suffix, which a wall
    /// clock round trip would lose: the text is worded from the stored instant, not from a server wall clock.
    /// </summary>
    [Theory]
    [MemberData(nameof(SnapshotRows))]
    public void TheCollectedText_OfTheRepeatedHour_KeepsItsOffset(string rowTypeName, string textProperty, string storedProperty)
    {
        var row = NewRow(rowTypeName, storedProperty, DisplayZoneFixtures.At(2026, 11, 1, 6, 30, 5));
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal("2026-11-01 01:30:05 -05:00", TextOf(row, textProperty));
    }

    /// <summary>
    /// A surface whose rows come from different snapshots shows each row's own time: the text is worded per row, from the
    /// row's own stored instant, and not from a time the surface holds once.
    /// </summary>
    [Theory]
    [MemberData(nameof(SnapshotRows))]
    public void RowsFromDifferentSnapshots_EachShowTheirOwnTime(string rowTypeName, string textProperty, string storedProperty)
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        var older = NewRow(rowTypeName, storedProperty, DisplayZoneFixtures.At(2026, 10, 3, 9, 15, 30));
        var newer = NewRow(rowTypeName, storedProperty, DisplayZoneFixtures.At(2026, 10, 3, 16, 27, 6));

        Assert.Equal("2026-10-03 09:15:30", TextOf(older, textProperty));
        Assert.Equal("2026-10-03 16:27:06", TextOf(newer, textProperty));
    }

    /// <summary>
    /// The three summary strips end with the snapshot's time: a "Collected" label and a named value, last in the strip,
    /// and the code that fills the strip words that value from the snapshot's own collection time. The Session Stats strip
    /// reads the NEWEST collection inside the last bucket (the one its Top Application and Top Host come from), not the
    /// bucket's grid time.
    /// </summary>
    [Theory]
    [InlineData("MemoryCollectedText", "SqlMemoryModelText", "Collected")]
    [InlineData("PlanCacheCollectedText", "PlanCacheRecommendationText", "Collected:")]
    [InlineData("SessionStatsCollectedText", "SessionStatsDatabasesText", "Collected:")]
    public void ASummaryStrip_EndsWithItsSnapshotsCollectedTime(string valueName, string previousLastValueName, string label)
    {
        var xaml = ReadRepoFile("Lite/Controls/ServerTab.xaml");

        var valueAt = xaml.IndexOf($"x:Name=\"{valueName}\"", StringComparison.Ordinal);
        var previousAt = xaml.IndexOf($"x:Name=\"{previousLastValueName}\"", StringComparison.Ordinal);
        Assert.True(valueAt > 0, $"{valueName} is not in ServerTab.xaml.");
        Assert.True(previousAt > 0 && previousAt < valueAt, $"{valueName} does not come after {previousLastValueName}, so it is not at the end of its strip.");

        /* Between the strip's old last value and the new one there is only the label of the new one. */
        var between = xaml[previousAt..valueAt];
        Assert.Contains($"Text=\"{label}\"", between);
        Assert.DoesNotContain("x:Name=", between[(between.IndexOf('>') + 1)..]);

        /* Nothing follows the new value inside its strip: the next element is a close tag. */
        var afterValue = Regex.Match(xaml[valueAt..], @"/>\s*(?<next><[^>]*>)");
        Assert.True(afterValue.Success && afterValue.Groups["next"].Value.StartsWith("</", StringComparison.Ordinal),
            $"{valueName} is followed by {afterValue.Groups["next"].Value}, not the end of its strip.");
    }

    [Theory]
    [InlineData("Lite/Controls/ServerTab.Charts.cs", @"MemoryCollectedText\.Text\s*=\s*SnapshotCollectedText\(\s*stats\?\.CollectionTime\b")]
    [InlineData("Lite/Controls/ServerTab.PlanCache.cs", @"PlanCacheCollectedText\.Text\s*=\s*SnapshotCollectedText\(\s*summary\.CollectionTime\b")]
    [InlineData("Lite/Controls/ServerTab.SessionStats.cs", @"SessionStatsCollectedText\.Text\s*=\s*SnapshotCollectedText\(\s*data\?\.LatestCollectionTime\b")]
    [InlineData("Lite/Controls/ServerTab.CpuScheduler.cs", @"CpuSchedulerGrid\.ItemsSource\s*=\s*CpuSchedulerGridRow\.Build\(")]
    public void TheCodeThatFillsASurface_WordsTheTimeFromTheSnapshotsOwnCollectionTime(string file, string expected)
    {
        var code = ReadRepoFile(file);
        Assert.True(Regex.IsMatch(code, expected), $"{file} does not match /{expected}/.");
    }

    /// <summary>The summary strips word the time with the tab's zone, to the second; a missing snapshot shows the strip's own empty marker, not a time.</summary>
    [Fact]
    public void TheStripText_IsTheSnapshotsTime_InTheGivenZone_ToTheSecond_AndTheEmptyMarkerWithoutOne()
    {
        Assert.Equal("2026-10-03 12:27:06", ServerTab.SnapshotCollectedText(DisplayZoneFixtures.At(2026, 10, 3, 16, 27, 6), DisplayZoneFixtures.Eastern, "--"));
        Assert.Equal("2026-10-03 16:27:06", ServerTab.SnapshotCollectedText(DisplayZoneFixtures.At(2026, 10, 3, 16, 27, 6), TimeZoneInfo.Utc, "--"));
        Assert.Equal("2026-11-01 01:30:05 -05:00", ServerTab.SnapshotCollectedText(DisplayZoneFixtures.At(2026, 11, 1, 6, 30, 5), DisplayZoneFixtures.Eastern, "--"));
        Assert.Equal("--", ServerTab.SnapshotCollectedText(null, DisplayZoneFixtures.Eastern, "--"));
        Assert.Equal("N/A", ServerTab.SnapshotCollectedText(null, TimeZoneInfo.Utc, "N/A"));
    }

    /// <summary>Every row of the CPU Scheduler grid carries the snapshot's collection time; a window with no snapshot has no rows, so no time.</summary>
    [Fact]
    public void TheCpuSchedulerGrid_CarriesTheSnapshotsTimeOnEveryRow_AndNothingWithoutASnapshot()
    {
        Assert.Empty(CpuSchedulerGridRow.Build(null));

        var collected = DisplayZoneFixtures.At(2026, 10, 3, 16, 27, 6);
        var rows = CpuSchedulerGridRow.Build(new CpuSchedulerSnapshot { CollectionTime = collected, SchedulerCount = 8 });
        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.Equal(collected, r.CollectionTime));
        Assert.Contains(rows, r => r.Metric == "Schedulers" && r.Value == "8");
    }

    /* Locate the repo from this file: no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, $"{relative} was not found above {thisFile}.");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
