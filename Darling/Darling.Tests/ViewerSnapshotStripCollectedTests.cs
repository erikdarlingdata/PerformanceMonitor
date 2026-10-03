/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's Memory, Plan Cache and Session Stats summary strips draw the newest snapshot, so each says when that
/// snapshot was collected (#4966): in the display zone, to the second, or the strip's own empty marker. Lite words it the same way.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSnapshotStripCollectedTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public ViewerSnapshotStripCollectedTests() => CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        CultureInfo.CurrentCulture = _savedCulture;
    }

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file).ReplaceLineEndings("\n");

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\n    \}\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    [Fact]
    public void TheText_IsTheCollectionTimeInTheDisplayZone_ToTheSecond()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        var utc = new DateTime(2026, 10, 3, 14, 5, 9, 700, DateTimeKind.Unspecified);

        Assert.Equal("2026-10-03 14:05:09", HistoryTime.SnapshotCollected(utc, "--"));
        Assert.Equal(HistoryTime.CollectionLocal(utc), HistoryTime.SnapshotCollected(utc, "N/A"));
    }

    [Fact]
    public void TheText_FollowsTheDisplayMode_AndTheEmptyMarkerIsTheStripsOwn()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        var utc = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var expected = TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        Assert.Equal(expected, HistoryTime.SnapshotCollected(utc, "--"));
        Assert.Equal("--", HistoryTime.SnapshotCollected(null, "--"));
        Assert.Equal("N/A", HistoryTime.SnapshotCollected(null, "N/A"));
    }

    [Fact]
    public void EachStrip_DeclaresItsCollectedElement_WithTheStripsEmptyText()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        Assert.Matches(@"<TextBlock Text=""Collected"" Foreground=""\{DynamicResource ForegroundDimBrush\}"" FontSize=""11""/>\s*<TextBlock x:Name=""MemoryCollectedText"" Text=""--""", xaml);
        Assert.Matches(@"<TextBlock Text=""Collected:""[^>]*ForegroundDimBrush[^>]*/>\s*<TextBlock x:Name=""PlanCacheCollectedText"" Text=""--""", xaml);
        Assert.Matches(@"<TextBlock Text=""Collected:""[^>]*ForegroundDimBrush[^>]*/>\s*<TextBlock x:Name=""SessionStatsCollectedText"" Text=""N/A""", xaml);
    }

    [Fact]
    public void EachLoad_SetsItsCollectedElement_FromTheSnapshotItRendered_AndClearsItWhenThereIsNone()
    {
        var memory = MethodBody(ViewerFile("ViewerServerTab.Memory.cs"), @"private void RenderMemorySummary\(");
        Assert.Contains("MemoryCollectedText.Text = HistoryTime.SnapshotCollected(null, \"--\");", memory, StringComparison.Ordinal);
        Assert.Contains("MemoryCollectedText.Text = HistoryTime.SnapshotCollected(stats.CollectionTime, \"--\");", memory, StringComparison.Ordinal);

        var plan = MethodBody(ViewerFile("ViewerServerTab.PlanCache.cs"), @"private void RenderPlanCacheSummary\(");
        Assert.Contains("PlanCacheCollectedText.Text = HistoryTime.SnapshotCollected(null, \"--\");", plan, StringComparison.Ordinal);
        Assert.Contains("PlanCacheCollectedText.Text = HistoryTime.SnapshotCollected(summary.CollectionTime, \"--\");", plan, StringComparison.Ordinal);

        var session = MethodBody(ViewerFile("ViewerServerTab.SessionStats.cs"), @"private void UpdateSessionStatsSummary\(");
        Assert.Contains("SessionStatsCollectedText.Text = HistoryTime.SnapshotCollected(data?.LatestCollectionTime, \"N/A\");", session, StringComparison.Ordinal);
    }

    [Fact]
    public void TheReads_CarryTheSnapshotsOwnCollectionTime()
    {
        Assert.Contains("MAX(collection_time) AS collection_time", ViewerDataService.PlanCacheSummarySql, StringComparison.Ordinal);
        Assert.Contains("collection_time AS latest_collection_time", ViewerDataService.SessionStatsSql, StringComparison.Ordinal);
        Assert.Contains("latest.latest_collection_time", ViewerDataService.SessionStatsSql, StringComparison.Ordinal);
    }
}

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerSnapshotStripCollectedLiveTests
{
    private const int PlanServerId = -497701;
    private const int SessionServerId = -497702;

    [Fact]
    public async Task PlanCacheSummary_ReturnsTheNewestSnapshotsCollectionTime_AndSessionStatsCarryTheLastCollectionInTheBucket_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await QueryGridSeed.OpenScratchAsync(ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var t1 = QueryGridSeed.NowToTheMinute().AddMinutes(-30);
        var t2 = t1.AddMinutes(10);
        /* Both session collections sit inside ONE 1-minute bucket (the 20-minute window below picks 1-minute buckets), at off-minute seconds. */
        var s1 = t2.AddSeconds(10);
        var s2 = t2.AddSeconds(50);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        foreach (var t in new[] { t1, t2 })
        {
            await using var plan = new NpgsqlCommand(@"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype, total_plans, total_size_mb,
     single_use_plans, single_use_size_mb, multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1, $2, $3, 'strip-plan', 'Compiled Plan', 'Adhoc', 5, 10, 1, 2, 4, 8, 1.5, 64, $4)", connection);
            plan.Parameters.AddWithValue(t == t1 ? 1L : 2L);
            plan.Parameters.AddWithValue(t);
            plan.Parameters.AddWithValue(PlanServerId);
            plan.Parameters.AddWithValue(t.AddDays(-1));
            await plan.ExecuteNonQueryAsync(ct);

            await using var session = new NpgsqlCommand(@"
INSERT INTO session_summary_stats
    (collection_id, collection_time, server_id, server_name, total_sessions, running_sessions, sleeping_sessions,
     background_sessions, dormant_sessions, idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
     top_application_name, top_application_connections, top_host_name, top_host_connections)
VALUES ($1, $2, $3, 'strip-session', 10, 1, 8, 1, 0, 0, 0, 3, 'App', 5, 'Host', 4)", connection);
            session.Parameters.AddWithValue(t == t1 ? 1L : 2L);
            session.Parameters.AddWithValue(t == t1 ? s1 : s2);
            session.Parameters.AddWithValue(SessionServerId);
            await session.ExecuteNonQueryAsync(ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var summary = await viewer.GetPlanCacheSummaryAsync(PlanServerId, t1.AddHours(-1), t2.AddHours(1), ct);
        Assert.Equal(t2, summary.CollectionTime);

        /* One 1-minute bucket holds both collections: the strip's time is the newest collection (s2), not the bucket's grid time or its first collection. */
        var points = await viewer.GetSessionStatsAsync(SessionServerId, t1.AddMinutes(-5), t2.AddMinutes(5), ct);
        Assert.Equal(s2, points[^1].LatestCollectionTime);
        Assert.NotEqual(points[^1].CollectionTime, points[^1].LatestCollectionTime);

        var empty = await viewer.GetPlanCacheSummaryAsync(PlanServerId, t2.AddDays(1), t2.AddDays(2), ct);
        Assert.Null(empty.CollectionTime);
    }
}
