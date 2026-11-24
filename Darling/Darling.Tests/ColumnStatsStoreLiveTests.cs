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

/// <summary>#8798: <see cref="ColumnStatsStore"/> against a real store (V272). Needs <c>DARLING_TEST_PG</c>; each test mints its own scratch database.</summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class ColumnStatsStoreLiveTests
{
    private const string SkipMessage = "Set DARLING_TEST_PG to run the column_stats store tests (each mints its own scratch database).";
    private const string Db = "Sales";
    private static readonly DateTime s_t0 = new(2026, 11, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_read = new(2026, 11, 2, 3, 0, 0, DateTimeKind.Unspecified);

    private static StatCandidate Stat(int objectId, int statsId, string table = "Orders", string name = "IX_Orders") =>
        new(objectId, "dbo", table, statsId, name, "index", s_t0, s_t0.AddHours(5), new[] { "CustomerId", "Status" }, new[] { "OrderId" }, 1000);

    /* updated is the server-local STATS_DATE; the server runs five hours behind UTC. */
    private static (StatCandidate Stat, StatHeader Header, IReadOnlyList<StatPrefix> Prefixes) Entry(StatCandidate stat, DateTime updated, double densityScale = 1)
    {
        stat = stat with { StatsDate = updated, StatsDateUtc = updated.AddHours(5) };
        var prefixes = new List<StatPrefix>
        {
            new(stat.ObjectId, stat.StatsId, 1, new[] { "CustomerId" }, false, 0.01 * densityScale),
            new(stat.ObjectId, stat.StatsId, 2, new[] { "CustomerId", "Status" }, false, 0.002 * densityScale),
            new(stat.ObjectId, stat.StatsId, 3, new[] { "CustomerId", "Status", "OrderId" }, true, 0.001 * densityScale),
        };
        return (stat, new StatHeader(1000, 900, 12.5f), prefixes);
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
    public async Task Replace_ThenLoadStored_RoundTrips()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        const string key = "server:7";

        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, key, Db, new[] { Entry(Stat(100, 1), s_t0), Entry(Stat(100, 2, name: "IX_Other"), s_t0.AddHours(1)) }, s_read, ct);

        var stored = await ColumnStatsStore.LoadStoredAsync(pg, key, Db, ct);
        Assert.Equal(2, stored.Count);
        Assert.Equal(s_t0, stored[(100, 1)].StatsDate);
        Assert.Equal(s_t0.AddHours(1), stored[(100, 2)].StatsDate);
        /* The names it was stored under and the longest prefix's columns, for the rename check. */
        Assert.Equal(("dbo", "Orders", "IX_Other"), (stored[(100, 2)].SchemaName, stored[(100, 2)].ObjectName, stored[(100, 2)].StatsName));
        Assert.Equal(new[] { "CustomerId", "Status", "OrderId" }, stored[(100, 1)].Columns);
        Assert.Empty(await ColumnStatsStore.LoadStoredAsync(pg, "server:8", Db, ct));
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
        const string key = "server:7";
        var stat = Stat(100, 1);

        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, key, Db, new[] { Entry(stat, s_t0) }, s_read, ct);
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, key, Db, new[] { Entry(stat, s_t0.AddDays(1), densityScale: 2) }, s_read.AddDays(1), ct);

        Assert.Equal(3, await CountAsync(pg, "server_id = 7", ct));
        var stored = await ColumnStatsStore.ReadForObjectAsync(pg, new[] { key }, Db, 100, ct);
        Assert.Equal(3, stored.Count);
        Assert.All(stored, p => Assert.Equal(s_t0.AddDays(1).AddHours(5), p.LastUpdatedUtc));
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
        const string key = "server:7";
        await ColumnStatsStore.ReplaceStatisticsAsync(
            pg, 7, key, Db, new[] { Entry(Stat(100, 1), s_t0), Entry(Stat(100, 2, name: "IX_Other"), s_t0), Entry(Stat(200, 1, "Lines", "IX_Lines"), s_t0) }, s_read, ct);

        var deleted = await ColumnStatsStore.DeleteStatisticsAsync(pg, key, Db, new[] { (100, 2), (200, 1) }, ct);

        Assert.Equal(6, deleted);
        var left = await ColumnStatsStore.LoadStoredAsync(pg, key, Db, ct);
        Assert.Equal(new[] { (100, 1) }, left.Keys.ToArray());

        Assert.Equal(3, await ColumnStatsStore.DeleteDatabaseAsync(pg, key, Db, ct));
        Assert.Empty(await ColumnStatsStore.LoadStoredAsync(pg, key, Db, ct));
    }

    [Fact]
    public async Task AgKey_OneSetOfRows_TheNewPrimaryReplacesTheOldPrimarysRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        var agKey = "ag:" + Guid.NewGuid().ToString("D");

        /* Server 7 (the old primary) wrote two statistics; server 8 (the new primary) reads one of them again under the same key. */
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, agKey, Db, new[] { Entry(Stat(100, 1, name: "Kept"), s_t0), Entry(Stat(100, 2, name: "Other"), s_t0) }, s_read, ct);
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 8, agKey, Db, new[] { Entry(Stat(100, 1, name: "Kept"), s_t0.AddDays(1)) }, s_read.AddDays(1), ct);
        await ColumnStatsStore.ReplaceStatisticsAsync(pg, 9, "server:9", Db, new[] { Entry(Stat(100, 3, name: "Elsewhere"), s_t0) }, s_read, ct);

        var byAg = await ColumnStatsStore.ReadForObjectAsync(pg, new[] { agKey }, Db, 100, ct);
        Assert.Equal(6, byAg.Count);
        Assert.All(byAg.Where(p => p.StatsName == "Kept"), p => Assert.Equal(s_t0.AddDays(1).AddHours(5), p.LastUpdatedUtc));
        Assert.Equal(3, await CountAsync(pg, "stats_id = 1", ct));
        Assert.Equal(3, await CountAsync(pg, "stats_id = 1 AND server_id = 8", ct));

        /* The dates load and the deletes go by key, whichever server wrote the row. */
        Assert.Equal(2, (await ColumnStatsStore.LoadStoredAsync(pg, agKey, Db, ct)).Count);
        Assert.Equal(3, await ColumnStatsStore.DeleteStatisticsAsync(pg, agKey, Db, new[] { (100, 2) }, ct));
        Assert.Equal(3, await ColumnStatsStore.DeleteDatabaseAsync(pg, agKey, Db, ct));
        Assert.Equal(3, await CountAsync(pg, "true", ct));

        Assert.Empty(await ColumnStatsStore.ReadForObjectAsync(pg, new[] { "server:9" }, "Other", 100, ct));
        Assert.Empty(await ColumnStatsStore.ReadForObjectAsync(pg, Array.Empty<string>(), Db, 100, ct));
    }

    [Fact]
    public async Task TwoWritersOfOneHistory_ReplaceTheSameStatistic_BothSucceed_OneSetOfRowsLeft()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, pg) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = pg;
        var agKey = "ag:" + Guid.NewGuid().ToString("D");

        /* Two registrations that both see the group's database as writable (a listener and its primary node) write the same rows at once.
           Without the history lock the second DELETE waits on the first's rows, finds nothing once they commit, and its INSERT fails 23505. */
        await using var other = new NpgsqlConnection(scratch.ConnectionString);
        await other.OpenAsync(ct);
        for (var round = 0; round < 25; round++)
        {
            var updated = s_t0.AddMinutes(round);
            await Task.WhenAll(
                ColumnStatsStore.ReplaceStatisticsAsync(pg, 7, agKey, Db, new[] { Entry(Stat(100, 1), updated), Entry(Stat(100, 2, name: "IX_Other"), updated) }, s_read, ct),
                ColumnStatsStore.ReplaceStatisticsAsync(other, 8, agKey, Db, new[] { Entry(Stat(100, 1), updated), Entry(Stat(100, 2, name: "IX_Other"), updated) }, s_read, ct));
        }

        Assert.Equal(6, await CountAsync(pg, "true", ct));
        Assert.Equal(1, await CountAsync(pg, "stats_id = 1 AND prefix_length = 1", ct));
        var stored = await ColumnStatsStore.LoadStoredAsync(pg, agKey, Db, ct);
        Assert.All(stored.Values, s => Assert.Equal(s_t0.AddMinutes(24), s.StatsDate));
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

        var first = new ColumnStatsStateRow(7, Db, "server:7", Guid.NewGuid(), s_t0, null);
        await ColumnStatsStore.SaveStateAsync(pg, first, ct);
        Assert.Equal(first, await ColumnStatsStore.LoadStateAsync(pg, 7, Db, ct));

        var second = first with { HistoryKey = "ag:" + Guid.NewGuid().ToString("D"), LastSeenInScopeAt = s_t0.AddHours(1), LastFullReadAt = s_t0.AddHours(2) };
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
