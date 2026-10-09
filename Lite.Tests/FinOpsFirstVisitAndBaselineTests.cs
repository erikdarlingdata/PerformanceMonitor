/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The FinOps tab loads once at start-up, before the first collection has run. A store that was closed for days (or a server enrolled
/// a minute ago) then showed "No Data", empty grids and "N idle database(s)" from that empty window until the server was reselected.
/// Showing the tab now re-runs the same whole per-server load a reselect runs, unless the last one began under 30 seconds ago; the
/// FIRST show after start-up always reloads, and a visible tab reloads when a collection for its server finishes (at most once a minute).
/// WPF cannot run here, so the tab itself is a source pin anchored on its method declarations, and the rule is a pure function.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class FinOpsShowReloadPolicyTests
{
    private static readonly DateTime Now = new(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void NoLoadYet_Reloads() => Assert.True(FinOpsShowReloadPolicy.ShouldReloadOnShow(null, Now));

    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(29)]
    public void TheFirstShowAfterStartUp_ReloadsWhateverTheAgeOfTheStartUpLoad(int secondsAgo)
    {
        // Open Lite and go straight to FinOps: the start-up load began seconds ago, before the first collection.
        Assert.True(FinOpsShowReloadPolicy.ShouldReloadOnShow(Now.AddSeconds(-secondsAgo), Now, firstShowSinceStart: true));
        Assert.False(FinOpsShowReloadPolicy.ShouldReloadOnShow(Now.AddSeconds(-secondsAgo), Now, firstShowSinceStart: false));
    }

    [Theory]
    [InlineData(120, 130, false)] // the collection finished before the last load began
    [InlineData(30, 10, false)]    // finished after the last load, but the load is 30 s old: debounced
    [InlineData(70, 10, true)]     // finished after the last load, and the load is over a minute old
    [InlineData(70, 80, false)]    // the last load began after the collection finished
    public void ACollectionFinishingWhileTheTabIsVisible_ReloadsOnceTheLastLoadIsAMinuteOld(int loadSecondsAgo, int collectionSecondsAgo, bool reload)
    {
        Assert.Equal(reload, FinOpsShowReloadPolicy.ShouldReloadAfterCollection(Now.AddSeconds(-loadSecondsAgo), Now.AddSeconds(-collectionSecondsAgo), Now));
    }

    [Fact]
    public void ACollectionBeforeAnyLoad_Reloads() => Assert.True(FinOpsShowReloadPolicy.ShouldReloadAfterCollection(null, Now, Now));

    [Theory]
    [InlineData(0, false)]
    [InlineData(5, false)]
    [InlineData(29, false)]
    [InlineData(30, true)]
    [InlineData(8 * 60, true)]          // the start-up load, eight minutes into a first collection
    [InlineData(9 * 24 * 3600, true)]
    public void ReloadsOnlyWhenTheLastWholeLoadIsAtLeastHalfAMinuteOld(int secondsAgo, bool reload)
    {
        Assert.Equal(reload, FinOpsShowReloadPolicy.ShouldReloadOnShow(Now.AddSeconds(-secondsAgo), Now));
    }

    private static string ReadFinOpsTab([CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(RepoRoot(thisFile), "Lite", "Controls", "FinOpsTab.xaml.cs")).Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string RepoRoot(string thisFile)
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Lite", "Controls", "FinOpsTab.xaml.cs")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static string BodyOf(string source, string name)
    {
        var decl = Regex.Matches(source, @"private (?:async )?[\w.<>]+ " + Regex.Escape(name) + @"\(");
        Assert.Single(decl);
        var start = decl[0].Index;
        var next = Regex.Match(source[(start + 10)..], @"\n    (?:private|public|internal|protected) ");
        return next.Success ? source.Substring(start, next.Index + 10) : source[start..];
    }

    [Fact]
    public void ShowingTheTab_RunsTheWholePerServerLoad_WhenTheLastOneIsOld()
    {
        var src = ReadFinOpsTab();
        var sub = Regex.Matches(src, @"IsVisibleChanged \+= \(_, _\) => (\w+)\(\);");
        Assert.Single(sub);

        var body = BodyOf(src, sub[0].Groups[1].Value);
        var gate = body.IndexOf("FinOpsShowReloadPolicy.ShouldReloadOnShow(_lastPerServerLoadUtc, DateTime.UtcNow, firstShow)", StringComparison.Ordinal);
        Assert.True(gate >= 0, "The show handler does not ask FinOpsShowReloadPolicy.");
        var whole = body.IndexOf("_ = LoadPerServerDataAsync();", gate, StringComparison.Ordinal);
        Assert.True(whole > gate, "The show handler does not run LoadPerServerDataAsync, the load a server reselect runs.");
        // The first show is remembered before the gate, so only the first one skips the age check.
        Assert.True(body.IndexOf("var firstShow = _firstShowPending;", StringComparison.Ordinal) is var f && f >= 0 && f < gate);
        Assert.Contains("_firstShowPending = false;", body, StringComparison.Ordinal);
        // The flagged size-grid reloads stay, after the whole-load gate, for a recent load.
        Assert.True(body.IndexOf("if (_dbSizesNeedReload) _ = LoadDatabaseSizesAsync(", whole, StringComparison.Ordinal) > whole);
    }

    [Fact]
    public void TheOverviewRefresh_TellsTheVisibleFinOpsTab_ThatACollectionFinished()
    {
        var tab = ReadFinOpsTab();
        Assert.Contains("FinOpsShowReloadPolicy.ShouldReloadAfterCollection(_lastPerServerLoadUtc, collected, DateTime.UtcNow)", tab, StringComparison.Ordinal);
        Assert.Contains("if (!IsVisible", tab, StringComparison.Ordinal);

        var main = File.ReadAllText(Path.Combine(RepoRoot(CallerFile()), "Lite", "MainWindow.xaml.cs")).Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("FinOpsContent.NoteCollection(summary.ServerId, summary.LastCollectionTime);", main, StringComparison.Ordinal);
    }

    private static string CallerFile([CallerFilePath] string thisFile = "") => thisFile;

    [Fact]
    public void ReselectingAServer_AndShowingTheTab_RunTheSameLoad_AndEachWholeLoadStampsItsStart()
    {
        var src = ReadFinOpsTab();

        // The reselect path.
        Assert.Contains("await LoadPerServerDataAsync();", BodyOf(src, "ServerSelector_SelectionChanged"), StringComparison.Ordinal);

        // The stamp the show handler reads is written by that same load, after its no-server guard.
        var load = BodyOf(src, "LoadPerServerDataAsync");
        var guard = load.IndexOf("if (serverId == 0 || _dataService == null) return;", StringComparison.Ordinal);
        var stamp = load.IndexOf("_lastPerServerLoadUtc = DateTime.UtcNow;", StringComparison.Ordinal);
        Assert.True(guard >= 0 && stamp > guard);
    }
}

/// <summary>
/// The Utilization card's health badge. A window with no CPU sample has no score: memory and storage alone can read a perfect 100
/// beside a "No Data" status, so the badge shows a dash on a gray badge, and a tooltip says why.
/// </summary>
public sealed class FinOpsHealthBadgeNoCpuTests
{
    private static UtilizationEfficiencyRow Row(string provisioningStatus) => new()
    {
        ProvisioningStatus = provisioningStatus,
        P95CpuPct = 0m,
        PhysicalMemoryMb = 1000,
        BufferPoolMb = 600,   // 60% of RAM: a memory score of 100
        FreeSpacePct = 100m   // a storage score of 100
    };

    [Fact]
    public void NoCpuSample_ShowsADash_NotAHundred()
    {
        var row = Row("");
        row.HealthScore = row.ComputeHealthScore();

        Assert.False(row.HasCpuSample);
        Assert.Equal(100, row.HealthScore);                    // the partial score the old badge printed
        Assert.Equal("Health: -", row.HealthScoreText);
        Assert.Equal(FinOpsHealthCalculator.NoScoreColor, row.HealthScoreColor);
    }

    [Fact]
    public void WithACpuSample_ShowsTheScore_InItsOwnColor()
    {
        var row = Row("RIGHT_SIZED");
        row.HealthScore = row.ComputeHealthScore();

        Assert.True(row.HasCpuSample);
        Assert.Equal($"Health: {row.HealthScore}", row.HealthScoreText);
        Assert.Equal(FinOpsHealthCalculator.ScoreColor(row.HealthScore), row.HealthScoreColor);
        Assert.NotEqual(FinOpsHealthCalculator.NoScoreColor, row.HealthScoreColor);
    }

    [Fact]
    public void TheTab_PaintsTheBadgeFromTheRow_AndNamesTheDashInItsTooltip()
    {
        var tab = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.Contains("HealthScoreText.Text = data.HealthScoreText;", tab, StringComparison.Ordinal);
        Assert.Contains("HealthScoreBorder.ToolTip = data.HasCpuSample ? null : FinOpsHealthCalculator.NoScoreNote;", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"Health: {data.HealthScore}\"", tab, StringComparison.Ordinal);
    }
}

/// <summary>
/// Storage Growth's "7d ago" and "30d ago" baselines are the snapshot NEAREST each mark, and only within one day of it. The old read
/// took the newest snapshot at or before the mark however far before, so a database with a 20-day-old sample called it "7d ago".
/// With no snapshot near the mark the size is null (n/a, tooltip "No sample from N days ago") and Growth % uses only a real baseline.
/// </summary>
public sealed class FinOpsStorageGrowthBaselineTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4977;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public FinOpsStorageGrowthBaselineTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly DateTime Collected = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute), DateTimeKind.Unspecified);

    private async Task SeedAsync(string database, double total, double daysAgo)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open) await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
     file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1, $2, $3, 'GrowthSrv', $4, 7, 1, 'ROWS', $5, $6, $7, NULL)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected.AddDays(-daysAgo) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = database + "_data" });
        cmd.Parameters.Add(new DuckDBParameter { Value = @"C:\" + database + "_data" });
        cmd.Parameters.Add(new DuckDBParameter { Value = total });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<StorageGrowthRow> ReadAsync(string database) =>
        Assert.Single(await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId), r => r.DatabaseName == database);

    [Fact]
    public async Task OldestSampleIs20DaysBack_NeitherBaselineIsFound_AndGrowthPercentIsNull()
    {
        // Old shape: the 20-day-old sample was the newest one at or before the 7-day mark, so it was shown as "Size 7d Ago".
        await SeedAsync("grow", 1000, 20);
        await SeedAsync("grow", 1200, 0);

        var row = await ReadAsync("grow");

        Assert.Equal(1200m, row.CurrentSizeMb);
        Assert.Null(row.Size7dAgoMb);
        Assert.Null(row.Size30dAgoMb);
        Assert.Null(row.Growth7dMb);
        Assert.Null(row.Growth30dMb);
        Assert.Null(row.GrowthPct30d);
        Assert.Equal("No sample from 7 days ago", row.Size7dAgoNote);
        Assert.Equal("No sample from 30 days ago", row.Size30dAgoNote);
    }

    [Fact]
    public async Task ASampleWithinADayOfEachMark_IsTheBaseline_AndTheNearestOneWins()
    {
        // 7d mark: 7.2 days back is nearer than 6.5 days back. 30d mark: 29.5 days back is within a day; 31.5 days back is not.
        await SeedAsync("grow", 100, 31.5);
        await SeedAsync("grow", 400, 29.5);
        await SeedAsync("grow", 700, 7.2);
        await SeedAsync("grow", 800, 6.5);
        await SeedAsync("grow", 1000, 0);

        var row = await ReadAsync("grow");

        Assert.Equal(700m, row.Size7dAgoMb);
        Assert.Equal(400m, row.Size30dAgoMb);
        Assert.Equal(300m, row.Growth7dMb);
        Assert.Equal(600m, row.Growth30dMb);
        Assert.Equal(150m, Math.Round(row.GrowthPct30d!.Value, 1));
        Assert.Null(row.Size7dAgoNote);
        Assert.Null(row.Size30dAgoNote);
    }

    [Fact]
    public async Task ASampleOver1DayFromTheMark_IsNotABaseline()
    {
        // 8.5 days back is 1.5 days from the 7-day mark: not a baseline. 5.9 days back is 1.1 days from it: not one either.
        await SeedAsync("grow", 500, 8.5);
        await SeedAsync("grow", 900, 5.9);
        await SeedAsync("grow", 1000, 0);

        var row = await ReadAsync("grow");

        Assert.Null(row.Size7dAgoMb);
        Assert.Null(row.Growth7dMb);
    }

    [Fact]
    public async Task Only7dBaseline_DailyRateUsesIt_AndGrowthPercentStaysNull()
    {
        await SeedAsync("grow", 700, 7);
        await SeedAsync("grow", 1000, 0);

        var row = await ReadAsync("grow");

        Assert.Equal(700m, row.Size7dAgoMb);
        Assert.Null(row.Size30dAgoMb);
        Assert.Null(row.GrowthPct30d);
        Assert.NotNull(row.DailyGrowthRateMb);
        Assert.InRange(row.DailyGrowthRateMb!.Value, 42m, 44m);   // 300 MB over 7 days
    }

    [Fact]
    public void TheGrid_ShowsNaAndTheNoteOnTheBaselineCells()
    {
        var xaml = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml");
        var start = xaml.IndexOf("<DataGrid x:Name=\"StorageGrowthDataGrid\"", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var block = xaml[start..xaml.IndexOf("</DataGrid>", start, StringComparison.Ordinal)];

        Assert.Contains("Binding=\"{Binding Size7dAgoMb, StringFormat='{}{0:N2}', TargetNullValue='n/a'}\"", block, StringComparison.Ordinal);
        Assert.Contains("Binding=\"{Binding Size30dAgoMb, StringFormat='{}{0:N2}', TargetNullValue='n/a'}\"", block, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(block, Regex.Escape("Value=\"{Binding Size7dAgoNote}\"")).Count);     // Size 7d Ago, Growth 7d
        Assert.Equal(3, Regex.Matches(block, Regex.Escape("Value=\"{Binding Size30dAgoNote}\"")).Count);    // Size 30d Ago, Growth 30d, Growth %
    }

    [Fact]
    public void TheSql_PicksTheNearestSnapshotWithinOneDay_NotTheNewestAtOrBeforeTheMark()
    {
        var sql = LocalDataService.StorageGrowthSql;


        Assert.Equal(2, Regex.Matches(sql, "<= 86400").Count);
        Assert.Contains("s.collection_time = (SELECT t FROM base_7d)", sql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time = (SELECT t FROM base_30d)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time <= $2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time <= $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASixDayHistory_GivesARealSevenDayBaseline_AndNothingFor30Days()
    {
        // A store that has run for 6 days: its oldest sample is 6 days back, exactly one day from the 7-day mark, so it is the 7-day
        // baseline (the walk's "Size 7d Ago" equal to current was a database that did not change in those 6 days). 30 days has no sample.
        foreach (var (size, daysAgo) in new[] { (600.0, 6), (700.0, 5), (800.0, 3), (900.0, 1), (1000.0, 0) })
            await SeedAsync("grow", size, daysAgo);

        var row = await ReadAsync("grow");

        Assert.Equal(600m, row.Size7dAgoMb);
        Assert.Equal(400m, row.Growth7dMb);
        Assert.Null(row.Size7dAgoNote);
        Assert.Null(row.Size30dAgoMb);
        Assert.Null(row.Growth30dMb);
        Assert.Null(row.GrowthPct30d);
        Assert.Equal("No sample from 30 days ago", row.Size30dAgoNote);
    }

    [Fact]
    public async Task ASixDayHistoryOfAnUnchangedDatabase_ReadsTheSevenDayBaselineAsCurrent_AndGrowthAsZero()
    {
        // The baseline is the sample, not a fallback: an unchanged database reads equal to current because it is.
        foreach (var daysAgo in new[] { 6, 4, 2, 0 })
            await SeedAsync("flat", 500, daysAgo);

        var row = await ReadAsync("flat");

        Assert.Equal(500m, row.Size7dAgoMb);
        Assert.Equal(0m, row.Growth7dMb);
        Assert.Null(row.Size30dAgoMb);
    }
}

/// <summary>
/// The Utilization card for a window with no CPU sample (a stale server, or one whose CPU collector is off). The read returns 0 for
/// the three CPU figures then, and the card used to print 0.00%, 0.00% and 0% and to draw the database sizes chart as if current.
/// The figures show a dash and the chart is hidden. Darling's Viewer has the same tab and the same tests.
/// </summary>
public sealed class FinOpsUtilizationNoCpuTests
{
    private static UtilizationEfficiencyRow Row(string provisioningStatus) => new()
    {
        ProvisioningStatus = provisioningStatus,
        AvgCpuPct = 0m,
        P95CpuPct = 0m,
        MaxCpuPct = 0
    };

    [Fact]
    public void NoCpuSample_ShowsDashesForEveryCpuFigure_NotZeros()
    {
        var row = Row("");

        Assert.False(row.HasCpuSample);
        Assert.Equal("-", row.AvgCpuText);
        Assert.Equal("-", row.P95CpuText);
        Assert.Equal("-", row.MaxCpuText);
    }

    [Fact]
    public void ACpuSample_ShowsTheMeasuredFigures_EvenWhenTheyAreZero()
    {
        var row = Row("OVER_PROVISIONED");
        row.AvgCpuPct = 12.5m;
        row.P95CpuPct = 40m;
        row.MaxCpuPct = 77;

        Assert.Equal($"{12.5m:N2}%", row.AvgCpuText);
        Assert.Equal($"{40m:N2}%", row.P95CpuText);
        Assert.Equal("77%", row.MaxCpuText);

        var idle = Row("OVER_PROVISIONED");
        Assert.Equal($"{0m:N2}%", idle.AvgCpuText);
        Assert.Equal("0%", idle.MaxCpuText);
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("RIGHT_SIZED", true)]
    [InlineData("NOT_APPLICABLE", true)]
    public void TheSizesChart_IsShownOnlyWhenTheWindowHeldACpuSample(string status, bool shown) =>
        Assert.Equal(shown, Row(status).ShowsDatabaseSizeChart);

    [Fact]
    public void TheTab_PaintsTheTextFromTheRow_EmptiesTheBars_AndHidesTheChart()
    {
        var tab = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.Contains("AvgCpuText.Text = data.AvgCpuText;", tab, StringComparison.Ordinal);
        Assert.Contains("P95CpuText.Text = data.P95CpuText;", tab, StringComparison.Ordinal);
        Assert.Contains("MaxCpuText.Text = data.MaxCpuText;", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"{data.AvgCpuPct:N2}%\"", tab, StringComparison.Ordinal);
        Assert.Contains("data.HasCpuSample ? (double)data.AvgCpuPct : 0", tab, StringComparison.Ordinal);
        Assert.Contains("DbSizeChartGroup.Visibility = data is { ShowsDatabaseSizeChart: true }", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"DbSizeChartGroup\"", ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml"), StringComparison.Ordinal);
    }
}
