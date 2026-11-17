/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#8798: <see cref="ColumnStatsStore"/> against a real store (V270). Needs <c>DARLING_TEST_PG</c>; each test mints its own scratch database.</summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class ColumnStatsStoreLiveTests
{
    private const string SkipMessage = "Set DARLING_TEST_PG to run the column_stats store tests (each mints its own scratch database).";
    private const string Db = "Sales";
    private static readonly DateTime s_t0 = new(2026, 11, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_read = new(2026, 11, 2, 3, 0, 0, DateTimeKind.Unspecified);

    private static StatCandidate Stat(int objectId, int statsId, string table = "Orders", string name = "IX_Orders") =>
        new(objectId, "dbo", table, statsId, name, "index", s_t0, new[] { "CustomerId", "Status" }, new[] { "OrderId" }, 1000);

    private static (StatCandidate Stat, StatHeader Header, IReadOnlyList<StatPrefix> Prefixes) Entry(StatCandidate stat, DateTime updated, double densityScale = 1)
    {
        var prefixes = new List<StatPrefix>
        {
            new(stat.ObjectId, stat.StatsId, 1, new[] { "CustomerId" }, false, 0.01 * densityScale),
            new(stat.ObjectId, stat.StatsId, 2, new[] { "CustomerId", "Status" }, false, 0.002 * densityScale),
            new(stat.ObjectId, stat.StatsId, 3, new[] { "CustomerId", "Status", "OrderId" }, true, 0.001 * densityScale),
        };
        return (stat, new StatHeader(updated, 1000, 900, 12.5f), prefixes);
    }

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Pg)> OpenAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        var pg = new NpgsqlConnection(scratch.ConnectionString);
        await pg.OpenAsync(ct);
        await PgMigrations.MigrateAsync(pg, ct);
        return (scratch, pg);
    }

    private static async Task<long> CountAsync(NpgsqlConnection pg, string where, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM collect.column_stats WHERE " + where, pg);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task Replace_ThenLoadStatsDates_RoundTrips()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        var key = new SchemaHistoryKey(7, null, null);

        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, key, Db, new[] { Entry(Stat(100, 1), s_t0), Entry(Stat(100, 2, name: "IX_Other"), s_t0.AddHours(1)) }, s_read, ct);

        var dates = await ColumnStatsStore.LoadStatsDatesAsync(pg, 7, Db, ct);
        Assert.Equal(2, dates.Count);
        Assert.Equal(s_t0, dates[(100, 1)]);
        Assert.Equal(s_t0.AddHours(1), dates[(100, 2)]);
        Assert.Empty(await ColumnStatsStore.LoadStatsDatesAsync(pg, 8, Db, ct));
        Assert.Equal(6, await CountAsync(pg, "server_id = 7", ct));
    }

    [Fact]
    public async Task Replace_SameStatTwice_KeepsOnlyNewestPrefixes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        var key = new SchemaHistoryKey(7, null, null);
        var stat = Stat(100, 1);

        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, key, Db, new[] { Entry(stat, s_t0) }, s_read, ct);
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, key, Db, new[] { Entry(stat, s_t0.AddDays(1), densityScale: 2) }, s_read.AddDays(1), ct);

        Assert.Equal(3, await CountAsync(pg, "server_id = 7", ct));
        var stored = await ColumnStatsStore.ReadForObjectAsync(pg, new[] { key }, Db, "dbo", "Orders", ct);
        Assert.Equal(3, stored.Count);
        Assert.All(stored, p => Assert.Equal(s_t0.AddDays(1), p.LastUpdated));
        Assert.Equal(0.02, stored[0].AllDensity, 9);
        Assert.Equal(new[] { "CustomerId" }, stored[0].Columns);
        Assert.False(stored[0].IncludesClusteringKey);
        Assert.True(stored[2].IncludesClusteringKey);
        Assert.Equal(1000, stored[0].Rows);
        Assert.Equal(900, stored[0].RowsSampled);
        Assert.Equal(12.5f, stored[0].PersistedSamplePercent);
        Assert.Equal("IX_Orders", stored[0].StatsName);
        Assert.Equal("index", stored[0].Origin);
        Assert.Equal(s_read.AddDays(1), stored[0].ReadAt);
    }

    [Fact]
    public async Task DeleteStatistics_RemovesOnlyNamed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        var key = new SchemaHistoryKey(7, null, null);
        await ColumnStatsStore.ReplaceStatisticsAsync(
            pg, 7, key, Db, new[] { Entry(Stat(100, 1), s_t0), Entry(Stat(100, 2, name: "IX_Other"), s_t0), Entry(Stat(200, 1, "Lines", "IX_Lines"), s_t0) }, s_read, ct);

        var deleted = await ColumnStatsStore.DeleteStatisticsAsync(pg, 7, Db, new[] { (100, 2), (200, 1) }, ct);

        Assert.Equal(6, deleted);
        var left = await ColumnStatsStore.LoadStatsDatesAsync(pg, 7, Db, ct);
        Assert.Equal(new[] { (100, 1) }, left.Keys.ToArray());

        Assert.Equal(3, await ColumnStatsStore.DeleteDatabaseAsync(pg, 7, Db, ct));
        Assert.Empty(await ColumnStatsStore.LoadStatsDatesAsync(pg, 7, Db, ct));
    }

    [Fact]
    public async Task ReadForObject_FindsByAgKeyFromEitherServer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        var group = Guid.NewGuid();
        var agKey = new SchemaHistoryKey(null, group, "AG1");

        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, agKey, Db, new[] { Entry(Stat(100, 1, name: "FromA"), s_t0) }, s_read, ct);
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 8, agKey, Db, new[] { Entry(Stat(100, 2, name: "FromB"), s_t0) }, s_read, ct);
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 9, new SchemaHistoryKey(9, null, null), Db, new[] { Entry(Stat(100, 3, name: "Other"), s_t0) }, s_read, ct);

        var byAg = await ColumnStatsStore.ReadForObjectAsync(pg, new[] { agKey }, Db, "dbo", "Orders", ct);
        Assert.Equal(new[] { "FromA", "FromB" }, byAg.Select(p => p.StatsName).Distinct().OrderBy(n => n).ToArray());

        var both = await ColumnStatsStore.ReadForObjectAsync(pg, new[] { agKey, new SchemaHistoryKey(9, null, null) }, Db, "dbo", "Orders", ct);
        Assert.Equal(9, both.Count);
        Assert.Empty(await ColumnStatsStore.ReadForObjectAsync(pg, new[] { agKey }, "Other", "dbo", "Orders", ct));
        Assert.Empty(await ColumnStatsStore.ReadForObjectAsync(pg, Array.Empty<SchemaHistoryKey>(), Db, "dbo", "Orders", ct));
    }

    [Fact]
    public async Task State_UpsertRoundTrips()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        Assert.Null(await ColumnStatsStore.LoadStateAsync(pg, 7, Db, ct));

        var first = new ColumnStatsStateRow(7, Db, 7, null, Guid.NewGuid(), s_t0, null);
        await ColumnStatsStore.SaveStateAsync(pg, first, ct);
        Assert.Equal(first, await ColumnStatsStore.LoadStateAsync(pg, 7, Db, ct));

        var second = first with { HistoryServerId = null, HistoryAgGroupId = Guid.NewGuid(), LastSeenInScopeAt = s_t0.AddHours(1), LastFullReadAt = s_t0.AddHours(2) };
        await ColumnStatsStore.SaveStateAsync(pg, second, ct);
        Assert.Equal(second, await ColumnStatsStore.LoadStateAsync(pg, 7, Db, ct));
    }

    [Fact]
    public async Task NoColumnHoldsValues()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;

        var columns = new List<string>();
        await using (var cmd = new NpgsqlCommand(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'collect' AND table_name IN ('column_stats', 'column_stats_state')", pg))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                columns.Add(reader.GetString(0));
            }
        }

        Assert.NotEmpty(columns);
        Assert.DoesNotContain(columns, c => c.Contains("filter", StringComparison.OrdinalIgnoreCase)
            || c.Contains("histogram", StringComparison.OrdinalIgnoreCase)
            || c.Contains("range_hi_key", StringComparison.OrdinalIgnoreCase));
    }
}
