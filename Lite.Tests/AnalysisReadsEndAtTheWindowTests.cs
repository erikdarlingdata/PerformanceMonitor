/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// An analysis pass reads what the store held by the end of its window, so a pass as of a past instant gives the answer
/// that instant had. A capture stored after the window's end changes neither the database configuration fact, which
/// reads the newest capture at or before the end, nor the data span the history gate measures.
/// </summary>
public sealed class AnalysisReadsEndAtTheWindowTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -941_001;

    private static readonly DateTime End = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    private readonly DuckDbInitializer _duckDb;
    private long _id = -94_100_000;

    public AnalysisReadsEndAtTheWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task SeedAsync(Func<DuckDBConnection, Task> seed)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await seed(connection);
    }

    private async Task<Dictionary<string, Fact>> FactsAsync(DateTime end)
    {
        var context = new AnalysisContext { ServerId = ServerId, ServerName = "window-end", TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        return (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context)).ToDictionary(f => f.Key);
    }

    /// <summary>The capture a day before the end holds two databases with auto shrink on; the capture an hour after the end holds
    /// one with it off.</summary>
    private Task SeedTwoDatabaseConfigCapturesAsync() => SeedAsync(async c =>
    {
        await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddDays(-1), "DbA", autoShrink: true, rcsiOn: false);
        await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddDays(-1), "DbB", autoShrink: true, rcsiOn: false);
        await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddHours(1), "DbA", autoShrink: false, rcsiOn: true);
    });

    [Fact]
    public async Task TheDatabaseConfigFact_ReadsTheNewestCaptureAtOrBeforeTheWindowEnd()
    {
        await SeedTwoDatabaseConfigCapturesAsync();

        var config = (await FactsAsync(End))["DB_CONFIG"];

        Assert.Equal(2, config.Metadata["database_count"]);
        Assert.Equal(2, config.Metadata["auto_shrink_on_count"]);
    }

    [Fact]
    public async Task TheDatabaseConfigFact_ReadsTheLaterCapture_OnceTheWindowEndsAtIt()
    {
        await SeedTwoDatabaseConfigCapturesAsync();

        var config = (await FactsAsync(End.AddHours(1)))["DB_CONFIG"];

        Assert.Equal(1, config.Metadata["database_count"]);
        Assert.Equal(0, config.Metadata["auto_shrink_on_count"]);
    }

    private Task SeedWaitStatsAtAsync(params DateTime[] stamps) => SeedAsync(async c =>
    {
        foreach (var stamp in stamps)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type) VALUES ($1, $2, $3, 'window-end', 'SOS_SCHEDULER_YIELD')";
            cmd.Parameters.Add(new DuckDBParameter { Value = _id-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = stamp });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    });

    /// <summary>Rows sit 30 hours and 1 hour before the end and 10 hours after it: the server had 29 hours of history by the end,
    /// and 40 hours in all.</summary>
    [Fact]
    public async Task TheDataSpan_MeasuresTheHistoryTheServerHadByTheWindowEnd()
    {
        await SeedWaitStatsAtAsync(End.AddHours(-30), End.AddHours(-1), End.AddHours(10));
        var analysis = new AnalysisService(_duckDb);

        Assert.Equal(29.0, await analysis.GetTotalDataSpanHoursAsync(ServerId, End), 3);
        Assert.Equal(40.0, await analysis.GetTotalDataSpanHoursAsync(ServerId), 3);
    }

    /// <summary>A row stamped exactly on the window's end is history the server had by then.</summary>
    [Fact]
    public async Task TheDataSpan_CountsARowStampedOnTheWindowEnd()
    {
        await SeedWaitStatsAtAsync(End.AddHours(-30), End);
        var analysis = new AnalysisService(_duckDb);

        Assert.Equal(30.0, await analysis.GetTotalDataSpanHoursAsync(ServerId, End), 3);
    }
}
