/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>
/// #9508: the buffer composition drill-down on a capture whose per-relation scan was skipped. The capture is one totals-only
/// row (buffers NULL). With the summary function the pool totals are real, and the drill-down lists no relation. Without it the
/// used count is unknown, and the drill-down says so: it used to coalesce the missing count to 0 and call the pool "0 % full".
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PgBufferUsageCapDrillDownLiveTests
{
    private const int ServerId = -950_802;

    [Fact]
    public async Task ATotalsOnlyCapture_WithTheSummaryUsedCount_ReportsThePoolFill_AndListsNoRelation()
    {
        var section = await DrillDownAsync(poolUsed: 1_000_000);

        Assert.Equal(2_500_000, section.GetProperty("pool_buffers_total").GetInt64());
        Assert.Equal(1_000_000, section.GetProperty("pool_buffers_used").GetInt64());
        Assert.Equal(0.4, section.GetProperty("pool_fill").GetDouble(), precision: 6);
        Assert.Equal(0, section.GetProperty("relations_in_capture").GetInt32());
        Assert.Equal(0, section.GetProperty("top_relations").GetArrayLength());
    }

    [Fact]
    public async Task ATotalsOnlyCapture_WithNoUsedCount_ReportsItAsUnknown_NotAnEmptyPool()
    {
        var section = await DrillDownAsync(poolUsed: null);

        Assert.Equal(2_500_000, section.GetProperty("pool_buffers_total").GetInt64());
        Assert.Equal(JsonValueKind.Null, section.GetProperty("pool_buffers_used").ValueKind);
        Assert.Equal(JsonValueKind.Null, section.GetProperty("pool_fill").ValueKind);
        Assert.Equal(0, section.GetProperty("top_relations").GetArrayLength());
    }

    [Fact]
    public void TheSentenceForAPoolWithNoUsedCount_SaysUnknown_AndNeverZeroPercentFull()
    {
        var sentence = PgTargetAdvice.BufferCompositionSentence(new PgTargetBufferCompositionSummary(
            Status: PgTargetBufferCompositionStatus.Captured,
            CapturedAt: new DateTime(2026, 9, 19, 10, 30, 0, DateTimeKind.Unspecified),
            LastCaptureAt: null,
            LastCaptureHoursBeforeWindow: null,
            PoolBuffersTotal: 2_500_000,
            PoolBuffersUsed: null,
            PoolBytes: 2_500_000L * 8192,
            PoolFill: null,
            BuffersListed: 0,
            DirtyBuffersListed: 0,
            DirtyShareOfPool: null,
            HeapShare: null,
            IndexShare: null,
            ToastShare: null,
            OtherShare: null,
            ColdBuffers: 0,
            ColdShare: null,
            RelationsInCapture: 0,
            TopRelations: [],
            MissShare: null,
            HitRatioSuppressed: false,
            Evictions: null,
            BuffersAlloc: null,
            ObservedMs: null,
            CacheTurnoversPerHour: null));

        Assert.Contains("unknown", sentence, StringComparison.Ordinal);
        Assert.DoesNotContain("0 % full", sentence, StringComparison.Ordinal);
        Assert.DoesNotContain("0 of", sentence, StringComparison.Ordinal);
    }

    private static async Task<JsonElement> DrillDownAsync(long? poolUsed)
    {
        var admin = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(admin), "Set DARLING_TEST_PG to a Postgres connection string to run the buffer cap drill-down test.");
        var ct = TestContext.Current.CancellationToken;
        await using var target = await ScratchPostgres.CreateAsync(admin!, ct);
        await using var store = NpgsqlDataSource.Create(target.ConnectionString);

        var windowEnd = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);
        await using (var insert = store.CreateCommand(@"
INSERT INTO pg_buffer_usage
    (collection_id, collection_time, server_id, server_name, database_name, relation_name, relation_kind, buffers, dirty_buffers, avg_usage_count, pool_buffers_total, pool_buffers_used)
VALUES ($1, $2, $3, 'pm9508-drill', NULL, NULL, NULL, NULL, NULL, NULL, 2500000, $4)"))
        {
            insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
            insert.Parameters.AddWithValue(windowEnd.AddMinutes(-30));
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.Add(new NpgsqlParameter { Value = (object?)poolUsed ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            await insert.ExecuteNonQueryAsync(ct);
        }

        var finding = new AnalysisFinding
        {
            ServerId = ServerId,
            ServerName = "pm9508-drill",
            Severity = 0.9,
            StoryPath = PgTargetFactKeys.BufferCachePressure,
            RootFactKey = PgTargetFactKeys.BufferCachePressure,
            PathKeys = [PgTargetFactKeys.BufferCachePressure],
        };
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            TimeRangeStart = windowEnd.AddHours(-1),
            TimeRangeEnd = windowEnd,
            CancellationToken = ct,
        };

        await new PgTargetDrillDownCollector(store).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        var json = JsonSerializer.Serialize(finding.DrillDown![PgTargetDrillDownCollector.BufferCompositionSection]);
        return JsonDocument.Parse(json).RootElement.Clone();
    }
}
