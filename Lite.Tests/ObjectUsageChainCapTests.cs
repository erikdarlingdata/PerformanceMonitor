/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9596: the three chain tables stay under a share of the hot store's reset point. A small reset point is injected into the builder so the history of
/// <see cref="ObjectUsageChainHistory"/> reaches the cap: the chain never passes it, a trim keeps the newest snapshots and the builder carries on from its
/// watermark, a window inside the kept chain reads from it and a window outside folds, and every answer is the full fold's. A cap that the newest week does
/// not fit under drops the chain and leaves the fold. With two databases a pass that starts over the cap drops only the one that does not fit, and the same
/// builder rebuilds it once it fits.
/// </summary>
public sealed class ObjectUsageChainCapTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ObjectUsageChainSrv";
    private const string Database = "Sales";
    private static readonly DateTime Now = ObjectUsageChainHistory.Now;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;

    public ObjectUsageChainCapTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-usagecap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        (_serverManager, _serverId) = ObjectUsageChainTests.NewServerManager(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private static string Json(ObjectUsageWindowResult result) => JsonSerializer.Serialize(result);

    private static DateTime Start(int daysBack) => DateTime.SpecifyKind(Now.AddDays(-daysBack), DateTimeKind.Unspecified);

    private static Func<double> ResetPointFor(long capRows) =>
        () => capRows * (double)ObjectUsageChainBuilder.EstimatedBytesPerRow / (1048576.0 * ObjectUsageChainBuilder.CapShareOfResetPoint);

    private Task<string> ToolAsync(int daysBack) =>
        McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, Database, daysBack, 500, 0, Now);

    private async Task<long> ScalarAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? 0 : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<DateTime?> TimeAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? null : (DateTime)value;
    }

    private Task<long> ChainRowsAsync() => ScalarAsync(
        "SELECT (SELECT count(*) FROM object_usage_deltas) + (SELECT count(*) FROM object_usage_snapshots) + (SELECT count(*) FROM object_usage_runs)");

    private async Task ClearChainAsync()
    {
        using var writeLock = _duckDb.AcquireWriteLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        foreach (var table in new[] { "object_usage_deltas", "object_usage_snapshots", "object_usage_runs" })
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "DELETE FROM " + table;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>Seeds the history and returns how many chain rows an uncapped build of it stores. The chain is empty again on return.</summary>
    private async Task<long> SeedAndMeasureAsync()
    {
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, ObjectUsageChainHistory.Generate());
        await new ObjectUsageChainBuilder(_duckDb, () => Now).BuildPassAsync(int.MaxValue, CancellationToken.None);
        var rows = await ChainRowsAsync();
        await ClearChainAsync();
        Assert.True(rows > 300);
        return rows;
    }

    [Fact]
    public async Task TheChainStaysUnderTheCap_AndEveryWindowInsideAndOutsideTheKeptChainAnswersTheFolds()
    {
        var uncapped = await SeedAndMeasureAsync();
        var expected7 = await ToolAsync(7);
        var expected90 = await ToolAsync(90);

        var capRows = uncapped / 2;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now, ResetPointFor(capRows));
        var builtInTotal = 0;
        var passes = 0;
        while (passes++ < 80)
        {
            var built = await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
            builtInTotal += built;
            Assert.True(await ChainRowsAsync() <= capRows, $"the chain passed its cap of {capRows} rows after pass {passes}");
            if (built == 0)
            {
                break;
            }
        }

        /* Every snapshot was built once: a trim does not make the builder build again what it dropped. */
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count(), builtInTotal);
        Assert.True(passes > 2, "the cap never stopped a pass, so the test did not reach it");
        var oldestKept = await TimeAsync("SELECT min(collection_time) FROM object_usage_snapshots");
        var newestKept = await TimeAsync("SELECT max(collection_time) FROM object_usage_snapshots");
        Assert.True(oldestKept > Start(60) && oldestKept <= Start(ObjectUsageChainBuilder.MinKeptDays));
        Assert.Equal(Start(0), newestKept);

        foreach (var daysBack in new[] { 1, 3, 7, 15, 21, 30, 90, 99, 120 })
        {
            var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(daysBack));
            var (persisted, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(daysBack));
            if (persisted is not null)
            {
                Assert.Equal(Json(full), Json(persisted));
            }
            else
            {
                Assert.False(string.IsNullOrEmpty(reason));
            }

            Assert.Equal(Json(full), Json(await _dataService.GetObjectUsageWindowAsync(_serverId, Database, Start(daysBack))));
        }

        /* A window inside the kept chain reads from it, and one that starts before it folds. */
        Assert.NotNull((await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7))).Result);
        var (outside, outsideReason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(90));
        Assert.Null(outside);
        Assert.False(string.IsNullOrEmpty(outsideReason));
        Assert.Equal(expected7, await ToolAsync(7));
        Assert.Equal(expected90, await ToolAsync(90));
    }

    [Fact]
    public async Task AfterATrim_TheNextPassCarriesOnFromTheWatermark_WithNoGapInTheKeptSnapshots()
    {
        var capRows = await SeedAndMeasureAsync() / 2;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now, ResetPointFor(capRows));

        var first = await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
        Assert.InRange(first, 1, ObjectUsageChainHistory.SnapshotDays().Count() - 1);
        var watermark = await TimeAsync("SELECT max(collection_time) FROM object_usage_snapshots");
        Assert.True(await ChainRowsAsync() <= capRows);

        var second = await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
        Assert.True(second > 0);
        Assert.True(await TimeAsync("SELECT max(collection_time) FROM object_usage_snapshots") > watermark);
        Assert.True(await ChainRowsAsync() <= capRows);

        /* The kept snapshots are the raw snapshots from the oldest kept one on, with none missing. */
        var oldest = await TimeAsync("SELECT min(collection_time) FROM object_usage_snapshots");
        var kept = await ScalarAsync("SELECT count(*) FROM object_usage_snapshots");
        var raw = await ScalarAsync(
            $"SELECT count(DISTINCT collection_time) FROM index_object_stats WHERE collection_time >= TIMESTAMP '{oldest:yyyy-MM-dd HH:mm:ss}' "
            + $"AND collection_time <= (SELECT max(collection_time) FROM object_usage_snapshots)");
        Assert.Equal(raw, kept);
    }

    [Fact]
    public async Task WhenTheNewestWeekDoesNotFit_TheChainIsDropped_NotExtended_AndTheFoldAnswers()
    {
        var capRows = await SeedAndMeasureAsync() / 40;
        var expected7 = await ToolAsync(7);
        var expected90 = await ToolAsync(90);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now, ResetPointFor(capRows));

        await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
        Assert.Equal(0L, await ChainRowsAsync());
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(0L, await ChainRowsAsync());

        var (none, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7));
        Assert.Null(none);
        Assert.False(string.IsNullOrEmpty(reason));
        Assert.Equal(expected7, await ToolAsync(7));
        Assert.Equal(expected90, await ToolAsync(90));
    }

    private const string SmallDatabase = "Tiny";

    private Task<string> ToolAsync(string database, int daysBack) =>
        McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, database, daysBack, 500, 0, Now);

    private Task<long> PairRowsAsync(string database) => ScalarAsync(
        $"SELECT (SELECT count(*) FROM object_usage_deltas WHERE database_name = '{database}') "
        + $"+ (SELECT count(*) FROM object_usage_snapshots WHERE database_name = '{database}') "
        + $"+ (SELECT count(*) FROM object_usage_runs WHERE database_name = '{database}')");

    [Fact]
    public async Task APassThatStartsOverTheCap_DropsOnlyTheDatabaseThatDoesNotFit_AndItComesBackOnceItFits_WithNoRestart()
    {
        /* Two databases on one server: the full history in Sales, and one index of it in Tiny. One builder, never restarted, with a cap the test moves. */
        var history = ObjectUsageChainHistory.Generate();
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, history);
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, SmallDatabase, history.Where(r => r.ObjectId == 2), firstId: -5_000_000, logRuns: false);
        var expected = new Dictionary<(string, int), string>();
        foreach (var database in new[] { Database, SmallDatabase })
        {
            foreach (var daysBack in new[] { 7, 90 })
            {
                expected[(database, daysBack)] = await ToolAsync(database, daysBack);
            }
        }

        long capRows = long.MaxValue / 1024;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now,
            () => capRows * (double)ObjectUsageChainBuilder.EstimatedBytesPerRow / (1048576.0 * ObjectUsageChainBuilder.CapShareOfResetPoint));
        var snapshots = ObjectUsageChainHistory.SnapshotDays().Count();
        Assert.True(await builder.BuildPassAsync(int.MaxValue, CancellationToken.None) > snapshots);
        var tinyRows = await PairRowsAsync(SmallDatabase);
        var salesRows = await PairRowsAsync(Database);
        Assert.True(salesRows > tinyRows, $"Sales holds {salesRows} chain rows and Tiny {tinyRows}");

        /* The reset point is lowered: Tiny's whole chain fits under the trim target, Sales beside it does not, and the next pass starts over the cap. */
        capRows = (long)Math.Ceiling(tinyRows / ObjectUsageChainBuilder.TrimTargetShare) + 2;
        Assert.True(tinyRows + salesRows > capRows);
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(tinyRows, await PairRowsAsync(SmallDatabase));
        Assert.Equal(0L, await PairRowsAsync(Database));
        Assert.True(await ChainRowsAsync() <= capRows);

        foreach (var daysBack in new[] { 7, 90 })
        {
            var tiny = await _dataService.TryReadObjectUsageChainAsync(_serverId, SmallDatabase, Start(daysBack));
            Assert.NotNull(tiny.Result);
            Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, SmallDatabase, Start(daysBack))), Json(tiny.Result));
            var (sales, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(daysBack));
            Assert.Null(sales);
            Assert.False(string.IsNullOrEmpty(reason));
            Assert.Equal(expected[(Database, daysBack)], await ToolAsync(Database, daysBack));
            Assert.Equal(expected[(SmallDatabase, daysBack)], await ToolAsync(SmallDatabase, daysBack));
        }

        /* While the cap stays where it is, Sales is neither rebuilt nor dropped again, pass after pass, and Tiny keeps its chain. */
        for (var pass = 0; pass < 3; pass++)
        {
            Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
            Assert.Equal(0L, await PairRowsAsync(Database));
            Assert.Equal(tinyRows, await PairRowsAsync(SmallDatabase));
        }

        /* The reset point goes back up: the same builder rebuilds Sales whole on its next pass, and both databases read from the chain. */
        capRows = long.MaxValue / 1024;
        Assert.Equal(snapshots, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(salesRows, await PairRowsAsync(Database));
        Assert.Equal(tinyRows, await PairRowsAsync(SmallDatabase));
        foreach (var database in new[] { Database, SmallDatabase })
        {
            foreach (var daysBack in new[] { 7, 90 })
            {
                var persisted = await _dataService.TryReadObjectUsageChainAsync(_serverId, database, Start(daysBack));
                Assert.NotNull(persisted.Result);
                Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, database, Start(daysBack))), Json(persisted.Result));
                Assert.Equal(expected[(database, daysBack)], await ToolAsync(database, daysBack));
            }
        }
    }
}
