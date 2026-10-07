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
/// Showing the tab now re-runs the same whole per-server load a reselect runs, unless the last one began under 30 seconds ago.
/// WPF cannot run here, so the tab itself is a source pin anchored on its method declarations, and the rule is a pure function.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class FinOpsShowReloadPolicyTests
{
    private static readonly DateTime Now = new(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void NoLoadYet_Reloads() => Assert.True(FinOpsShowReloadPolicy.ShouldReloadOnShow(null, Now));

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
        var gate = body.IndexOf("FinOpsShowReloadPolicy.ShouldReloadOnShow(_lastPerServerLoadUtc, DateTime.UtcNow)", StringComparison.Ordinal);
        Assert.True(gate >= 0, "The show handler does not ask FinOpsShowReloadPolicy.");
        var whole = body.IndexOf("_ = LoadPerServerDataAsync();", gate, StringComparison.Ordinal);
        Assert.True(whole > gate, "The show handler does not run LoadPerServerDataAsync, the load a server reselect runs.");
        // The flagged size-grid reloads stay, after the whole-load gate, for a recent load.
        Assert.True(body.IndexOf("if (_dbSizesNeedReload) _ = LoadDatabaseSizesAsync(", whole, StringComparison.Ordinal) > whole);
    }

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
}
