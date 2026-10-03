/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's three Configuration Changes grids say where their data starts (#4966). Each grid diffs the snapshots one
/// collector writes (<c>server_config</c>, <c>database_config</c>, <c>trace_flags</c>), so a change's own time is a snapshot's
/// capture time and the notice keys on the store's COVERAGE of that table: the later of the server's first collection and the
/// table's retention edge, never the first change. A quiet start (the store covered the range, the first change came late) says
/// nothing. None of the three reads is capped (each reads every snapshot up to the range's end), so none passes a row cap, and a
/// range of 90 minutes or less makes no probe call. The viewer shows no PostgreSQL configuration-changes grid, which a ratchet
/// here pins.
/// </summary>
/* The banner's time text reads the process-wide display mode and server clock, which the helper sets and restores; one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerConfigChangesDataStartTests
{
    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    /* A store nothing listens on: a probe that starts a query against it fails, one that does not returns at once. */
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    private static DateTime At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    /* The three grids: the probe each one starts, the table it asks about, and the SQL its read runs (a view over that table). */
    public static TheoryData<string, string, string> Grids => new()
    {
        { "GetServerConfigChangesDataStartAsync", "server_config", "ServerConfigChangesSnapshotsSql" },
        { "GetDatabaseConfigChangesDataStartAsync", "database_config", "DatabaseConfigChangesSnapshotsSql" },
        { "GetTraceFlagChangesDataStartAsync", "trace_flags", "TraceFlagChangesSnapshotsSql" },
    };

    private static string SqlOf(string name) =>
        (string)typeof(ViewerDataService).GetField(name)!.GetValue(null)!;

    // ── The probes ──

    [Theory]
    [MemberData(nameof(Grids))]
    public void EachProbe_GoesThroughTheSharedFloor_OverTheTableItsReadDrawsFrom(string probe, string table, string readSql)
    {
        var source = ViewerFile("ViewerDataService.ConfigChanges.cs");

        Assert.Matches(
            $@"public Task<DateTime\?> {probe}\(int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default\) =>\s*"
            + $@"DataWindowFloor\.GetForServerAsync\(_dataSource, DataWindowFloor\.Source\.ForCollectorTable\(""{table}""\), serverId, startUtc, endUtc,",
            source);

        /* The table the probe asks about is the one the grid's read draws from (through its v_ view). */
        Assert.Contains($"FROM v_{table}", SqlOf(readSql), StringComparison.Ordinal);

        Assert.True(DataWindowFloor.Source.TryForCollectorTable(table, out var floor), $"{table} is not a table the probe can read");
        Assert.Equal(table, floor.Relation);
        Assert.Equal("capture_time", floor.TimeColumn);
    }

    /* The web mirror probes the same four tables for the same grids (WebDataStartNote.TableByRead); the viewer shows three of them. */
    [Fact]
    public void TheSourcesTheWebProbes_AreAllReadableByTheSharedFloor()
    {
        foreach (var table in new[] { "server_config", "database_config", "trace_flags", "pg_server_config" })
        {
            Assert.True(DataWindowFloor.Source.TryForCollectorTable(table, out _), $"{table} is not a table the probe can read");
        }
    }

    /* The Configuration tab of a PostgreSQL server lists the CURRENT settings (GetPgServerConfigAsync), a snapshot with no range to
       cut. The day a grid lists the changes (DarlingPgServerConfigReader.GetConfigChangesAsync) it needs its own note, a probe over
       pg_server_config through ShowEventDataStartAsync, as the three SQL Server grids have: this fails until it does. */
    [Fact]
    public void TheViewerShowsNoPostgresConfigurationChangesGrid_YetOrItsNotesProbeIsPinned()
    {
        var all = string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "*.cs").Select(File.ReadAllText));

        Assert.DoesNotContain("GetConfigChangesAsync(", all, StringComparison.Ordinal);
        Assert.DoesNotContain("get_pg_server_config_changes", all, StringComparison.Ordinal);
    }

    // ── The tab: the probes beside the reads, one banner per grid from the rows shown, no cap ──

    [Fact]
    public void TheTab_StartsOneProbePerGridBesideTheReads_AndAwaitsEachThroughTheSharedDecision()
    {
        var load = MethodBody(ViewerFile("ViewerServerTab.ConfigChanges.cs"), @"private async Task LoadConfigChangesAsync\(");

        /* Three reads and the three probes beside them are six reads in flight, so the width declared to the deadline is six. */
        Assert.Equal(1, Matches(load, @"using var readFanOut = ViewerReadFanOut\.Of\(6\);"));

        Assert.Equal(1, Matches(load, @"var serverStartTask = _dataService\.GetServerConfigChangesDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load, @"var databaseStartTask = _dataService\.GetDatabaseConfigChangesDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load, @"var traceFlagStartTask = _dataService\.GetTraceFlagChangesDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));

        /* The rows' change times, and no cap argument: the call ends at the row list. */
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(ServerConfigChangesTruncationBanner,\s*serverStartTask,\s*""Server Config Changes"",\s*startUtc,\s*serverChanges\.Select\(r => \(DateTime\?\)r\.ChangeTime\)\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(DatabaseConfigChangesTruncationBanner,\s*databaseStartTask,\s*""Database Config Changes"",\s*startUtc,\s*databaseChanges\.Select\(r => \(DateTime\?\)r\.ChangeTime\)\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(TraceFlagChangesTruncationBanner,\s*traceFlagStartTask,\s*""Trace Flag Changes"",\s*startUtc,\s*traceFlagChanges\.Select\(r => \(DateTime\?\)r\.ChangeTime\)\);"));

        /* A probe is only the disclosure: none is awaited bare, so one that throws costs its banner and nothing after it. */
        Assert.DoesNotContain("await serverStartTask", load, StringComparison.Ordinal);
        Assert.DoesNotContain("await databaseStartTask", load, StringComparison.Ordinal);
        Assert.DoesNotContain("await traceFlagStartTask", load, StringComparison.Ordinal);
    }

    /* A census over every server-tab file: each read of a grid is paired with its probe and its banner, so a second read path added
       later without them fails here instead of shipping with a stale banner. */
    [Fact]
    public void EveryTabRead_OfTheThreeGrids_HasItsProbeAndItsBanner()
    {
        var tabs = string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

        foreach (var (read, probe, banner) in new[]
        {
            ("GetServerConfigChangesAsync", "GetServerConfigChangesDataStartAsync", "ServerConfigChangesTruncationBanner"),
            ("GetDatabaseConfigChangesAsync", "GetDatabaseConfigChangesDataStartAsync", "DatabaseConfigChangesTruncationBanner"),
            ("GetTraceFlagChangesAsync", "GetTraceFlagChangesDataStartAsync", "TraceFlagChangesTruncationBanner"),
        })
        {
            Assert.Equal(1, Matches(tabs, $@"_dataService\.{read}\("));
            Assert.Equal(1, Matches(tabs, $@"_dataService\.{probe}\("));
            Assert.Equal(1, Matches(tabs, $@"ShowEventDataStartAsync\({banner},"));
        }
    }

    [Theory]
    [InlineData("ServerConfigChangesTruncationBanner", "ServerConfigChangesGrid", "ServerConfigChangesNoDataMessage")]
    [InlineData("DatabaseConfigChangesTruncationBanner", "DatabaseConfigChangesGrid", "DatabaseConfigChangesNoDataMessage")]
    [InlineData("TraceFlagChangesTruncationBanner", "TraceFlagChangesGrid", "TraceFlagChangesNoDataMessage")]
    public void TheBanner_SitsAboveItsGrid_InTheExistingStyle(string banner, string grid, string emptyHint)
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var declared = Regex.Match(xaml, $@"<TextBlock Grid\.Row=""0"" x:Name=""{banner}""[^>]*/>");
        Assert.True(declared.Success, $"{banner} is not declared in ViewerServerTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", declared.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", declared.Value, StringComparison.Ordinal);
        Assert.Matches($@"<DataGrid Grid\.Row=""1"" x:Name=""{grid}""", xaml);
        Assert.Matches($@"<TextBlock Grid\.Row=""1"" x:Name=""{emptyHint}""", xaml);
    }

    // ── What the banner shows: the product's own decision, on a real control ──

    /* Rows start inside the range: the server was added 3 days in, and its first change came a quarter of a day later. The notice
       names where the table's coverage starts, in the display zone. */
    [Fact]
    public void ARangeThatStartsBeforeCoverage_NamesTheCoverageStart()
    {
        Assert.Equal(DataStartBannerReadout.Since(At(3)), DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(3)), RangeStart, [At(3, 6), At(5)]));
    }

    /* The notice is written in the viewer's display zone: server time at +05:30 moves the coverage start's text, and only its text.
       The expected text comes from the product's own banner formatter (DataStartBannerReadout.Since), so the format of the time
       (to the minute or to the second) is not asserted here; the substring checks name the zone's shift in either. */
    [Fact]
    public void TheNotice_NamesItsTime_InTheDisplayZone()
    {
        var inServerTime = DataStartBannerReadout.For(
            Task.FromResult<DateTime?>(At(3)), RangeStart, [At(3, 6)], mode: TimeDisplayMode.ServerTime, serverOffsetMinutes: 330);
        var inUtc = DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(3)), RangeStart, [At(3, 6)], mode: TimeDisplayMode.UTC);

        Assert.Equal(DataStartBannerReadout.Since(At(3), TimeDisplayMode.ServerTime, 330), inServerTime);
        Assert.Contains("2026-09-04 05:30", inServerTime, StringComparison.Ordinal);
        Assert.Equal(DataStartBannerReadout.Since(At(3)), inUtc);
        Assert.Contains("2026-09-04 00:00", inUtc, StringComparison.Ordinal);
        Assert.NotEqual(inUtc, inServerTime);
    }

    /* A quiet start: the store covered the whole range (coverage 20 days before it), and the first change came 5 hours in. No notice. */
    [Fact]
    public void AQuietStart_RaisesNoNotice()
    {
        Assert.Null(DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(-20)), RangeStart, [At(0, 5), At(2)]));
    }

    /* None of the reads is capped: a long list of changes whose oldest came late in a covered range is not a cut (a capped read of the
       same rows, with a cap equal to their count, would name its oldest row). */
    [Fact]
    public void ALongList_OfAnUncappedRead_NamesNoOldestRow_WhereTheStoreCoversTheRange()
    {
        var changes = Enumerable.Range(0, 1000).Select(i => (DateTime?)At(0, 5).AddMinutes(i)).ToList();

        Assert.Null(DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(-20)), RangeStart, changes));
        Assert.Equal(DataStartBannerReadout.Since(At(0, 5)), DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(-20)), RangeStart, changes, rowCap: 1000));
    }

    /* An empty grid is the probe's to say: a server added 3 days in has no earlier snapshots, whether or not a change was found. */
    [Fact]
    public void AnEmptyGrid_StillNamesTheCoverageStart()
    {
        Assert.Equal(DataStartBannerReadout.Since(At(3)), DataStartBannerReadout.For(Task.FromResult<DateTime?>(At(3)), RangeStart, []));
        Assert.Null(DataStartBannerReadout.For(Task.FromResult<DateTime?>(null), RangeStart, []));
    }

    // ── A window no longer than the slack starts no probe ──

    /* Every probe of the three grids returns at once on a range of an hour or up to the 90-minute slack, with no query against a store
       nothing listens on; one minute over the slack starts the query, which fails there. */
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task ARangeNoLongerThanTheSlack_MakesNoProbeCall_ForAnyOfTheThreeGrids(int minutes)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);
        var end = RangeStart.AddMinutes(minutes);

        Assert.Null(await viewer.GetServerConfigChangesDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetDatabaseConfigChangesDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetTraceFlagChangesDataStartAsync(1, RangeStart, end, ct));
    }

    [Fact]
    public async Task ARangeOneMinuteOverTheSlack_StartsEachProbe_AgainstNoStore()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);
        var end = RangeStart.AddMinutes(91);

        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetServerConfigChangesDataStartAsync(1, RangeStart, end, ct));
        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetDatabaseConfigChangesDataStartAsync(1, RangeStart, end, ct));
        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetTraceFlagChangesDataStartAsync(1, RangeStart, end, ct));
    }
}

/// <summary>
/// The "Showing since" banner a surface raises, read off a real control: what the product's own decision
/// (<c>ViewerServerTab.ShowEventDataStartAsync</c>) writes for a probe's answer and the rows a grid shows. The test calls the
/// product's rule, never a copy of it, so a change to the rule fails the tests that read it here.
/// </summary>
internal static class DataStartBannerReadout
{
    /// <summary>
    /// The banner's text, or null when it is hidden. The control is seeded visible with stale text, so a no-op cannot pass as a hidden
    /// banner. The display mode and server clock are set for the call and put back, whatever it does.
    /// </summary>
    public static string? For(
        Task<DateTime?> probe, DateTime startUtc, IEnumerable<DateTime?> shownTimesUtc, int? rowCap = null,
        TimeDisplayMode mode = TimeDisplayMode.UTC, int? serverOffsetMinutes = null)
    {
        string? text = null;
        OnStaThread(() =>
        {
            var savedMode = ViewerTimeHelper.CurrentDisplayMode;
            var savedClock = ViewerTimeHelper.ActiveServerClock;
            try
            {
                ViewerTimeHelper.CurrentDisplayMode = mode;
                if (serverOffsetMinutes is int offset)
                {
                    ViewerTimeHelper.UtcOffsetMinutes = offset;
                }

                var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
                ViewerServerTab.ShowEventDataStartAsync(banner, probe, "Test Surface", startUtc, shownTimesUtc, rowCap).GetAwaiter().GetResult();
                text = banner.Visibility == Visibility.Visible ? banner.Text : null;
            }
            finally
            {
                ViewerTimeHelper.CurrentDisplayMode = savedMode;
                ViewerTimeHelper.ActiveServerClock = savedClock;
            }
        });
        return text;
    }

    /// <summary>
    /// The banner text the product writes for the instant <paramref name="utc"/>: "Showing since " and the time as the tab's own
    /// formatter (<c>ViewerServerTab.BannerTime</c>, the one formatter every "Showing since" note goes through) prints it under the
    /// same display mode and server clock <see cref="For"/> sets. An assertion built from this follows the product's time format
    /// instead of copying it.
    /// </summary>
    public static string Since(DateTime utc, TimeDisplayMode mode = TimeDisplayMode.UTC, int? serverOffsetMinutes = null)
    {
        var bannerTime = typeof(ViewerServerTab).GetMethod(
            "BannerTime", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public, [typeof(DateTime)])
            ?? throw new InvalidOperationException("ViewerServerTab.BannerTime(DateTime) was not found: the banner's formatter moved.");

        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            if (serverOffsetMinutes is int offset)
            {
                ViewerTimeHelper.UtcOffsetMinutes = offset;
            }

            return "Showing since " + (string)bannerTime.Invoke(null, [utc])!;
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
        }
    }

    /// <summary>Runs <paramref name="body"/> on an STA thread (WPF objects require one) and rethrows what it threw.</summary>
    public static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
