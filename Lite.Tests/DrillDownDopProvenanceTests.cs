/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB pin for #3648: the <c>top_cpu_queries</c> and <c>bad_actor_query</c> drill-downs must headline
/// the NEWEST plan's <c>max_dop</c> and carry the cross-plan maximum as a history with provenance, not fold
/// every plan the hash ever had into one provenance-free number.
///
/// <para><b>The fixture is the live page.</b> One <c>query_hash</c>, two plans inside the window: an old
/// parallel plan whose <c>sys.dm_exec_query_stats.max_dop</c> (a per-plan high-water mark since the plan was
/// cached) reads 16, last seen hours before the window's end, and a new serial plan reading 1 that is the one
/// spending the CPU at the newest snapshot. Before #3648 the record said <c>max_dop = 16</c> for this shape —
/// on a MAXDOP-1 instance whose stored plan was serial — and a reader recommended a MAXDOP 1 Query Store hint
/// from it. After: <c>max_dop = 1</c>, <c>max_dop_any_plan = 16</c>, <c>plan_count = 2</c>,
/// <c>max_dop_any_plan_last_seen</c> = the parallel plan's last snapshot, and <c>dop_note</c> says so in one
/// sentence.</para>
///
/// <para><b>Real DuckDB rather than a string pin.</b> The claim under test is the ENGINE's answer to a
/// <c>ROW_NUMBER() OVER (... ORDER BY collection_time DESC ...)</c> ranking combined with a hash-wide
/// <c>MAX() OVER</c> — which row the newest-plan CASE picks, and that a NULL reading arrives as NULL rather
/// than 0. <c>DrillDownDopProvenanceParityTests</c> holds the text half across both SKUs.</para>
/// </summary>
public sealed class DrillDownDopProvenanceTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 36480;
    private const string ServerName = "SynthDopProvSrv";
    private const string Db = "SynthDopProvDb";
    private const string Hash = "0x3648HASH";
    private const string OldParallelPlan = "0x3648PLANPARALLEL";
    private const string NewSerialPlan = "0x3648PLANSERIAL";

    /* One UtcNow read, truncated to the second: two reads can land on different ticks, leaving a sub-
       microsecond residue that DuckDB's microsecond timestamps drop on the way back out — and the
       last-seen assertions below compare a round-tripped timestamp for equality. */
    private static readonly DateTime WindowEnd = TruncateToSeconds(DateTime.UtcNow);

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);

    /* The parallel plan's LAST snapshot inside the window — what max_dop_any_plan_last_seen must report. */
    private static readonly DateTime ParallelLastSeen = WindowEnd.AddHours(-3);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public DrillDownDopProvenanceTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task TopCpuQueries_HeadlinesTheNewestPlansDop_AndCarriesTheParallelHistory()
    {
        /* -- RED before #3648: max_dop read 16 — the old parallel plan's counter — for a hash whose newest
              plan is serial. -- */
        await ClearAsync();
        await SeedTwoPlansOneHashAsync();

        var row = Assert.Single(await CollectTopCpuRowsAsync());

        Assert.Equal(1, row.GetProperty("max_dop").GetInt32());
        Assert.Equal(16, row.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(2, row.GetProperty("plan_count").GetInt64());
        Assert.Equal(ParallelLastSeen, DateTime.Parse(row.GetProperty("max_dop_any_plan_last_seen").GetString()!, null,
            System.Globalization.DateTimeStyles.RoundtripKind));
        Assert.Equal(
            $"DOP 1 (a parallel plan ran at 16 until {ParallelLastSeen:yyyy-MM-dd}; 2 plans in window)",
            row.GetProperty("dop_note").GetString());

        /* The windowed totals still span BOTH plans: the provenance columns narrow one reading, they do not
           filter the read. 3 parallel snapshots x 100000 + 1 serial x 400000 = 700000 us = 700 ms. */
        Assert.Equal(700.0, row.GetProperty("total_cpu_ms").GetDouble());
    }

    [Fact]
    public async Task BadActorQuery_HeadlinesTheNewestPlansDop_AndCarriesTheParallelHistory()
    {
        /* The same shape through the BAD_ACTOR drill-down, which is one hash unfiltered by CPU spent. */
        await ClearAsync();
        await SeedTwoPlansOneHashAsync();

        var row = await CollectBadActorAsync();

        Assert.Equal(1, row.GetProperty("max_dop").GetInt32());
        Assert.Equal(16, row.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(2, row.GetProperty("plan_count").GetInt64());
        Assert.Equal(
            $"DOP 1 (a parallel plan ran at 16 until {ParallelLastSeen:yyyy-MM-dd}; 2 plans in window)",
            row.GetProperty("dop_note").GetString());
    }

    [Fact]
    public async Task OneSerialPlan_SaysOne_AndHasNoHistoryToDisclose()
    {
        /* Row #3 on the live card: one serial plan, honest before and after. No note — there is no history
           that disagrees with the headline, and a note on every row would bury the one that matters. */
        await ClearAsync();
        await SeedAsync(NewSerialPlan, WindowEnd.AddMinutes(-30), maxDop: 1, workerTimeUs: 400000);

        var row = Assert.Single(await CollectTopCpuRowsAsync());

        Assert.Equal(1, row.GetProperty("max_dop").GetInt32());
        Assert.Equal(1, row.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(1, row.GetProperty("plan_count").GetInt64());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("dop_note").ValueKind);
    }

    [Fact]
    public async Task NoReading_IsNull_NotZero()
    {
        /* The DMV never reports a DOP of 0 — a serial plan is 1 — so 0 was always "no reading" rendered as
           a number. A NULL max_dop must arrive as JSON null on every DOP field, and the note must not invent
           a parallel history from nothing. */
        await ClearAsync();
        await SeedAsync(NewSerialPlan, WindowEnd.AddMinutes(-30), maxDop: null, workerTimeUs: 400000);

        var row = Assert.Single(await CollectTopCpuRowsAsync());

        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop_any_plan").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop_any_plan_last_seen").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("dop_note").ValueKind);
    }

    [Fact]
    public async Task NewestReadingUnknown_WithAParallelPlanOnRecord_SaysUnknownAndDisclosesIt()
    {
        /* The newest plan carried no reading but an older plan ran parallel: "unknown" is the headline and
           the parallel history is still disclosed, because that is exactly the case where a reader would
           otherwise fall back to the folded maximum. */
        await ClearAsync();
        await SeedAsync(OldParallelPlan, ParallelLastSeen, maxDop: 16, workerTimeUs: 100000);
        await SeedAsync(NewSerialPlan, WindowEnd.AddMinutes(-30), maxDop: null, workerTimeUs: 400000);

        var row = Assert.Single(await CollectTopCpuRowsAsync());

        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop").ValueKind);
        Assert.Equal(16, row.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(
            $"DOP unknown for the newest plan (a parallel plan ran at 16 until {ParallelLastSeen:yyyy-MM-dd}; 2 plans in window)",
            row.GetProperty("dop_note").GetString());
    }

    [Fact]
    public async Task TwoPlansAtTheSameNewestSnapshot_TheNewerCompiledOneIsTheHeadline()
    {
        /* Both plans cached at the newest snapshot — the stale parallel plan still sitting in the cache
           next to the serial one that replaced it. collection_time ties, so the compile-time tie-break
           decides: the plan compiled LATER is the newest plan, whatever its counter says. */
        await ClearAsync();
        var newest = WindowEnd.AddMinutes(-30);
        await SeedAsync(OldParallelPlan, newest, maxDop: 16, workerTimeUs: 500000, creationTime: WindowEnd.AddDays(-20));
        await SeedAsync(NewSerialPlan, newest, maxDop: 1, workerTimeUs: 100000, creationTime: WindowEnd.AddDays(-1));

        var row = Assert.Single(await CollectTopCpuRowsAsync());

        Assert.Equal(1, row.GetProperty("max_dop").GetInt32());
        Assert.Equal(16, row.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(newest, DateTime.Parse(row.GetProperty("max_dop_any_plan_last_seen").GetString()!, null,
            System.Globalization.DateTimeStyles.RoundtripKind));
    }

    private async Task<JsonElement> CollectAsync(string factKey, string drillDownKey)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = factKey,
            StoryPath = factKey,
            /* Past the 0.5 display gate in EnrichFindingsAsync — below it the expensive drill-downs are
               skipped wholesale and this collector never runs at all. */
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue(drillDownKey, out var raw), $"{drillDownKey} was not collected");
        return JsonSerializer.SerializeToElement(raw);
    }

    /// <summary>The <c>top_cpu_queries</c> rows (an array section).</summary>
    private async Task<JsonElement[]> CollectTopCpuRowsAsync()
        => [.. (await CollectAsync("CPU_SQL_PERCENT", "top_cpu_queries")).EnumerateArray()];

    /// <summary>The <c>bad_actor_query</c> record (a single-object section).</summary>
    private Task<JsonElement> CollectBadActorAsync()
        => CollectAsync("BAD_ACTOR_" + Hash, "bad_actor_query");

    /// <summary>
    /// The live page's shape: the parallel plan spent CPU at three snapshots ending three hours before the
    /// window's end, then the serial plan spent CPU at the newest snapshot.
    /// </summary>
    private async Task SeedTwoPlansOneHashAsync()
    {
        await SeedAsync(OldParallelPlan, ParallelLastSeen.AddMinutes(-20), maxDop: 16, workerTimeUs: 100000);
        await SeedAsync(OldParallelPlan, ParallelLastSeen.AddMinutes(-10), maxDop: 16, workerTimeUs: 100000);
        await SeedAsync(OldParallelPlan, ParallelLastSeen, maxDop: 16, workerTimeUs: 100000);
        await SeedAsync(NewSerialPlan, WindowEnd.AddMinutes(-30), maxDop: 1, workerTimeUs: 400000);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ClearAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM query_stats WHERE server_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedAsync(string planHash, DateTime collectionTime, long? maxDop, long workerTimeUs,
        DateTime? creationTime = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_spills, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, 10, $10, $11, 1000, 0, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = Hash });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = creationTime ?? collectionTime.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxDop.HasValue ? maxDop.Value : DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = workerTimeUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = workerTimeUs * 2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.SynthDopProvTable" });
        await cmd.ExecuteNonQueryAsync();
    }
}
