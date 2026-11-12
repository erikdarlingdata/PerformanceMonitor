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
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7691: a server with several memory nodes stores one memory pressure record per node for the same event. One event is one
/// (second, notification): the daily summary and the analysis fact count it once. The same five events the PostgreSQL live test plants:
/// A is one LOW event on two nodes, B is a LOW and a STEADY event at one second, C is one severe LOW event on two nodes 1 ms apart,
/// D is a STEADY event on two nodes that never reached medium. Eight records, four at medium or worse (A, B twice, C), one severe (C).
/// </summary>
public sealed class MemoryPressureEventsCountedOncePerEventTests : IDisposable
{
    private const string ServerName = "MemoryPressureCountServer";
    private const int ServerId = 76911;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly DateTime _day;

    public MemoryPressureEventsCountedOncePerEventTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "MemoryPressureCount_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _seed = new PendingSeedSession(_duckDb);
        _day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
    }

    public void Dispose()
    {
        _seed.Dispose();
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private async Task PlantAsync()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await _seed.ExecuteAsync(@"
INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
SELECT $1 + row_number() OVER (), $2, $3, $4, $2 - t.m * INTERVAL 1 MINUTE + t.ms * INTERVAL 1 MILLISECOND, t.n, t.p, t.s
FROM (VALUES
    (10, 0, 'RESOURCE_MEMPHYSICAL_LOW', 2, 0), (10, 0, 'RESOURCE_MEMPHYSICAL_LOW', 2, 0),
    (20, 0, 'RESOURCE_MEMPHYSICAL_LOW', 2, 0), (20, 0, 'RESOURCE_MEM_STEADY', 2, 0),
    (30, 0, 'RESOURCE_MEMPHYSICAL_LOW', 3, 0), (30, 1, 'RESOURCE_MEMPHYSICAL_LOW', 3, 0),
    (40, 0, 'RESOURCE_MEM_STEADY', 1, 0), (40, 0, 'RESOURCE_MEM_STEADY', 1, 0)
) AS t(m, ms, n, p, s)",
            1L, _day.AddHours(12), ServerId, ServerName);
        _seed.Flush();
    }

    [Fact]
    public async Task TheDailySummary_CountsAnEventOnce_AcrossNodes()
    {
        await PlantAsync();

        var row = await new LocalDataService(_duckDb).GetDailySummaryAsync(ServerId, _day);

        Assert.NotNull(row);
        /* A, B's two notifications and C are four events at medium or worse; only C is severe. */
        Assert.Equal(4, row!.MemoryPressureEvents);
        Assert.Equal(1, row.MemoryCriticalEvents);
    }

    [Fact]
    public async Task TheAnalysisFact_CountsAnEventOnce_AcrossNodes()
    {
        await PlantAsync();

        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = _day,
            TimeRangeEnd = _day.AddDays(1),
        };
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);

        var fact = Assert.Single(facts, f => f.Key == "MEMORY_PRESSURE_EVENTS");
        Assert.Equal(4.0, fact.Metadata["event_count"]);
        Assert.Equal(3.0, fact.Value);
    }
}
