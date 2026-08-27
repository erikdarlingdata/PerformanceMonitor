/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_daily_summary_range (#2484), the twin of Darling's — the Performance Calendar's month grid,
/// which had no read on either SKU. <c>GetDailySummaryRangeAsync</c> already existed and nothing called it,
/// so get_daily_summary could only ever answer one day.
///
/// <para>What is worth pinning is not "the aggregate runs" — get_daily_summary proves that off the same SQL.
/// It is the three things only the RANGE form can get wrong: that a missing day means missing COLLECTION
/// rather than a quiet day, that the band is computed per day so two days in one result can disagree, and
/// that the anchor moves the range rather than widening it.</para>
/// </summary>
public sealed class DailySummaryRangeToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CalendarSrv";

    /* Lite DERIVES its server id from the storage name; a hardcoded one would seed rows the tool looks
       straight past and pass the never-collected assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public DailySummaryRangeToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-calendar-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task TheCalendar_BandsEachDaySeparately_AndShowsCollectionGapsAsMissingDays()
    {
        var service = new LocalDataService(_duckDb);
        var today = DateTime.UtcNow.Date;
        var twoDaysAgo = today.AddDays(-2);

        /* 1. nothing collected at all: an empty calendar is a collection fault, not a quiet fortnight. */
        var never = Root(await McpHealthTools.GetDailySummaryRange(service, _serverManager, ServerName, 3));
        Assert.Equal("unavailable", never.GetProperty("status").GetString());
        var neverText = never.GetProperty("message").GetString()!;
        Assert.Contains("nothing has been collected", neverText, StringComparison.Ordinal);
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);

        /*
            2. two collected days with a hole between them. Today collected cleanly; two days ago collected
            AND recorded an ERROR run, which bands that day Critical. Yesterday is deliberately left alone:
            it is the gap, and the point of the read is that a gap is an ABSENT day rather than a quiet one.
        */
        await SeedRunAsync(Truncate(DateTime.UtcNow), "wait_stats", "SUCCESS", error: null);
        await SeedRunAsync(twoDaysAgo.AddHours(12), "wait_stats", "SUCCESS", error: null);
        await SeedRunAsync(twoDaysAgo.AddHours(13), "query_store", "ERROR", error: "seeded failure");

        var range = Root(await McpHealthTools.GetDailySummaryRange(service, _serverManager, ServerName, 3));

        Assert.Equal(ServerName, range.GetProperty("server").GetString());
        Assert.Equal(3, range.GetProperty("days_back").GetInt32());

        /* The bounds the read used, echoed back so a caller can tell which days they asked for from which
           days they got — the whole distinction this payload exists to make. */
        Assert.Equal(twoDaysAgo.ToString("yyyy-MM-dd"), range.GetProperty("from_date").GetString());
        Assert.Equal(today.ToString("yyyy-MM-dd"), range.GetProperty("to_date").GetString());

        var days = range.GetProperty("days").EnumerateArray().ToArray();

        /* Two collected days out of three asked for; day_count counts days WITH data, and the gap between
           it and days_back is the useful number. */
        Assert.Equal(2, days.Length);
        Assert.Equal(2, range.GetProperty("day_count").GetInt32());
        Assert.DoesNotContain(days, d => d.GetProperty("summary_date").GetString() == today.AddDays(-1).ToString("yyyy-MM-dd"));

        /* Oldest first, as the aggregate returns them — a calendar read backwards is a bug nobody would
           spot in a table. */
        Assert.Equal(twoDaysAgo.ToString("yyyy-MM-dd"), days[0].GetProperty("summary_date").GetString());
        Assert.Equal(today.ToString("yyyy-MM-dd"), days[1].GetProperty("summary_date").GetString());

        /* The two days disagree, which is what makes this a calendar rather than one verdict. */
        Assert.Equal("Critical", days[0].GetProperty("overall_health").GetString());
        Assert.Equal(1, days[0].GetProperty("collection_errors").GetInt64());
        Assert.Equal("Healthy", days[1].GetProperty("overall_health").GetString());
        Assert.Equal(0, days[1].GetProperty("collection_errors").GetInt64());
    }

    /// <summary>
    /// The anchor moves the range, and a range outside this server's history is a different answer from a
    /// server that has never collected.
    /// </summary>
    [Fact]
    public async Task TheAnchor_MovesTheRange_AndAnEmptyRangeIsNotAnUncollectedServer()
    {
        var service = new LocalDataService(_duckDb);
        var today = DateTime.UtcNow.Date;
        var tenDaysAgo = today.AddDays(-10);

        await SeedRunAsync(Truncate(DateTime.UtcNow), "wait_stats", "SUCCESS", error: null);

        /* A range this server has no history for, on a server that HAS collected. Same zero rows as the
           never-collected branch, and it must not reach for the same word. */
        var beforeHistory = Root(await McpHealthTools.GetDailySummaryRange(
            service, _serverManager, ServerName, 1, tenDaysAgo.ToString("yyyy-MM-dd")));

        Assert.Equal("empty", beforeHistory.GetProperty("status").GetString());
        var beforeText = beforeHistory.GetProperty("message").GetString()!;
        Assert.Contains("A day with ANY collection appears here even when every signal was quiet", beforeText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", beforeText, StringComparison.Ordinal);

        /* Now seed that day, and the SAME anchored call sees it — proof by content that the anchor reached
           the query rather than being validated and thrown away. */
        await SeedRunAsync(tenDaysAgo.AddHours(12), "wait_stats", "SUCCESS", error: null);

        var anchored = Root(await McpHealthTools.GetDailySummaryRange(
            service, _serverManager, ServerName, 1, tenDaysAgo.ToString("yyyy-MM-dd")));
        var anchoredDay = Assert.Single(anchored.GetProperty("days").EnumerateArray().ToArray());
        Assert.Equal(tenDaysAgo.ToString("yyyy-MM-dd"), anchoredDay.GetProperty("summary_date").GetString());

        /* And the same one-day span unanchored answers about TODAY instead, so the anchor moved the range
           rather than widening it. */
        var unanchored = Root(await McpHealthTools.GetDailySummaryRange(service, _serverManager, ServerName, 1));
        var unanchoredDay = Assert.Single(unanchored.GetProperty("days").EnumerateArray().ToArray());
        Assert.Equal(today.ToString("yyyy-MM-dd"), unanchoredDay.GetProperty("summary_date").GetString());
    }

    [Fact]
    public async Task OutOfRangeKnobs_AreRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);

        Assert.StartsWith(
            "Invalid days_back value '0'",
            await McpHealthTools.GetDailySummaryRange(service, _serverManager, ServerName, 0),
            StringComparison.Ordinal);

        Assert.StartsWith(
            "Invalid days_back value '367'",
            await McpHealthTools.GetDailySummaryRange(service, _serverManager, ServerName, 367),
            StringComparison.Ordinal);

        /* An anchor we cannot use is refused, never silently treated as today. */
        Assert.StartsWith(
            "Invalid as_of",
            await McpHealthTools.GetDailySummaryRange(service, _serverManager, ServerName, 30, "last tuesday"),
            StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedRunAsync(DateTime collectionTimeUtc, string collector, string status, string? error)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)error ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }
}
