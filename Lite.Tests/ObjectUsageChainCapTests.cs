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
/// watermark, a window inside the kept chain reads from it and a window outside folds, and every answer is the full fold's. A cap that the minimum span
/// (<see cref="ObjectUsageChainBuilder.MinKeptDays"/>) does not fit under drops the chain and leaves the fold. With two databases a pass that starts over
/// the cap drops only the one that does not fit, and the same builder rebuilds it once it fits. A database whose backfill a trim removes is rebuilt from
/// the trim's cutoff and keeps a chain over its newest days, a chain dropped after a trim comes back from that cutoff, and with no trim each database's
/// chain starts at its own oldest snapshot. A trim to the minimum span keeps the baseline of a 7 day window, so the 7 day read comes from the chain, also
/// when the newest snapshot is earlier in its day than the snapshots a week back.
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
    public async Task WhenTheMinimumSpanDoesNotFit_TheChainIsDropped_NotExtended_AndTheFoldAnswers()
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

    private const string LightDatabase = "Ledger";

    private Task<DateTime?> OldestAsync(string database) =>
        TimeAsync($"SELECT min(collection_time) FROM object_usage_snapshots WHERE database_name = '{database}'");

    private Task<DateTime?> NewestAsync(string database) =>
        TimeAsync($"SELECT max(collection_time) FROM object_usage_snapshots WHERE database_name = '{database}'");

    [Fact]
    public async Task ADatabaseWhoseBackfillATrimRemoves_IsRebuiltFromTheTrimsCutoff_AndKeepsAChainOverItsNewestDays()
    {
        /* Two databases on one server. Ledger sorts first and holds the history. Sales holds it three times over, the copies under other object ids,
           so one day of Sales is three times the rows of one day of Ledger. */
        var history = ObjectUsageChainHistory.Generate();
        var heavy = Enumerable.Range(0, 3)
            .SelectMany(copy => history.Select(r => r with { ObjectId = r.ObjectId + (100 * copy), Table = r.Table + copy }))
            .ToList();
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, heavy);
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, LightDatabase, history, firstId: -5_000_000, logRuns: false);
        var databases = new[] { LightDatabase, Database };
        var expected = new Dictionary<(string, int), string>();
        foreach (var database in databases)
        {
            foreach (var daysBack in new[] { 7, 90 })
            {
                expected[(database, daysBack)] = await ToolAsync(database, daysBack);
            }
        }

        await new ObjectUsageChainBuilder(_duckDb, () => Now).BuildPassAsync(int.MaxValue, CancellationToken.None);
        var lightRows = await PairRowsAsync(LightDatabase);
        Assert.True(await PairRowsAsync(Database) > 2 * lightRows);
        await ClearChainAsync();

        /* The cap holds Ledger whole with a twentieth to spare, which Sales uses up in the first days of its backfill. */
        var snapshots = ObjectUsageChainHistory.SnapshotDays().Count();
        var capRows = lightRows + (lightRows / 20);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now, ResetPointFor(capRows));

        /* Pass 1: Ledger is built whole, Sales reaches the cap part way, and the trim to the newest days removes every row Sales built. */
        var first = await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
        Assert.InRange(first, snapshots + 1, (2 * snapshots) - 1);
        Assert.Equal(0L, await PairRowsAsync(Database));
        var cutoff = await OldestAsync(LightDatabase);
        Assert.True(cutoff > Start(100), "the first pass did not trim");
        Assert.Equal(Start(0), await NewestAsync(LightDatabase));
        Assert.True(await ChainRowsAsync() <= capRows);

        /* Pass 2 on: Sales is built from the trim's cutoff, never again from its oldest snapshot, and no pass leaves it without a chain. */
        var trail = new List<string>();
        var lost = 0;
        var pass = 1;
        int built;
        do
        {
            built = await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
            pass++;
            var salesRows = await PairRowsAsync(Database);
            lost += salesRows == 0 ? 1 : 0;
            trail.Add($"pass {pass} built {built} and left Sales {salesRows} rows from {await OldestAsync(Database):MM-dd}");
            Assert.True(await ChainRowsAsync() <= capRows, $"the chain passed its cap of {capRows} rows after pass {pass}");
        }
        while (built > 0 && pass < 30);

        Assert.True(lost == 0 && built == 0, "Sales did not keep a chain: " + string.Join(", ", trail));
        Assert.True(pass > 3, "Sales was built in one pass, so the cap never stopped it after the trim");

        /* Both chains end over the newest days: from the same oldest kept day to the newest snapshot, with no snapshot missing. */
        var oldestKept = await OldestAsync(Database);
        Assert.True(oldestKept >= cutoff && oldestKept <= Start(ObjectUsageChainBuilder.MinKeptDays), $"Sales' chain starts at {oldestKept}");
        Assert.Equal(oldestKept, await OldestAsync(LightDatabase));
        foreach (var database in databases)
        {
            Assert.Equal(Start(0), await NewestAsync(database));
            Assert.Equal(
                await ScalarAsync(
                    $"SELECT count(DISTINCT collection_time) FROM index_object_stats WHERE database_name = '{database}' "
                    + $"AND collection_time >= TIMESTAMP '{oldestKept:yyyy-MM-dd HH:mm:ss}'"),
                await ScalarAsync($"SELECT count(*) FROM object_usage_snapshots WHERE database_name = '{database}'"));
        }

        /* A 7 day read comes from the chain and is the fold's, a 90 day read starts before the kept chain and folds, and the tool answers the same. */
        foreach (var database in databases)
        {
            var week = await _dataService.TryReadObjectUsageChainAsync(_serverId, database, Start(7));
            Assert.NotNull(week.Result);
            Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, database, Start(7))), Json(week.Result));
            var (quarter, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, database, Start(90));
            Assert.Null(quarter);
            Assert.False(string.IsNullOrEmpty(reason));
            Assert.Equal(expected[(database, 7)], await ToolAsync(database, 7));
            Assert.Equal(expected[(database, 90)], await ToolAsync(database, 90));
        }

        /* A further pass builds nothing and removes nothing. */
        var settled = await ChainRowsAsync();
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(settled, await ChainRowsAsync());
    }

    [Fact]
    public async Task AfterATrim_ACapTheMinimumSpanDoesNotFitUnder_StillDropsTheChain_AndItComesBackFromTheTrimsCutoff()
    {
        /* One builder with a cap the test moves. At half the uncapped size the passes trim the chain to its newest days. */
        var uncapped = await SeedAndMeasureAsync();
        var expected7 = await ToolAsync(7);
        var capRows = uncapped / 2;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now,
            () => capRows * (double)ObjectUsageChainBuilder.EstimatedBytesPerRow / (1048576.0 * ObjectUsageChainBuilder.CapShareOfResetPoint));
        for (var pass = 0; pass < 80 && await builder.BuildPassAsync(int.MaxValue, CancellationToken.None) > 0; pass++)
        {
            Assert.True(await ChainRowsAsync() <= capRows);
        }

        var cutoff = await OldestAsync(Database);
        Assert.True(cutoff > Start(100) && cutoff <= Start(ObjectUsageChainBuilder.MinKeptDays), $"the chain starts at {cutoff}");
        Assert.Equal(Start(0), await NewestAsync(Database));
        var fromCutoff = await ScalarAsync(
            $"SELECT count(DISTINCT collection_time) FROM index_object_stats WHERE collection_time >= TIMESTAMP '{cutoff:yyyy-MM-dd HH:mm:ss}'");

        /* The cap drops to where the minimum span does not fit: no trim reaches it, so the pass drops the chain, and the chain is under the cap. */
        capRows = uncapped / 40;
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(0L, await ChainRowsAsync());
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(0L, await ChainRowsAsync());

        /* The cap goes back up: the chain is built from the trim's cutoff on, each snapshot once and none older, and stays under the cap. */
        capRows = uncapped / 2;
        var back = 0;
        for (var pass = 0; pass < 80; pass++)
        {
            var built = await builder.BuildPassAsync(int.MaxValue, CancellationToken.None);
            Assert.True(await ChainRowsAsync() <= capRows, $"the chain passed its cap of {capRows} rows");
            if (built == 0)
            {
                break;
            }

            back += built;
        }

        Assert.Equal(fromCutoff, (long)back);
        Assert.True(await OldestAsync(Database) >= cutoff);
        Assert.Equal(Start(0), await NewestAsync(Database));
        var week = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7));
        Assert.NotNull(week.Result);
        Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(7))), Json(week.Result));
        Assert.Equal(expected7, await ToolAsync(7));
    }

    /// <summary>
    /// A cap whose trim budget holds the delta rows of the built chain's newest <see cref="ObjectUsageChainBuilder.MinKeptDays"/> days and not a day more,
    /// so the pass that starts over it trims to the minimum span.
    /// </summary>
    private async Task<long> MinimumSpanCapAsync()
    {
        var spanDeltas = await ScalarAsync(
            "SELECT count(*) FROM object_usage_deltas WHERE collection_time >= "
            + $"(SELECT CAST(CAST(max(collection_time) AS DATE) AS TIMESTAMP) - to_days({ObjectUsageChainBuilder.MinKeptDays - 1}) FROM object_usage_deltas)");
        var other = await ScalarAsync("SELECT (SELECT count(*) FROM object_usage_snapshots) + (SELECT count(*) FROM object_usage_runs)");
        return (long)Math.Ceiling((other + spanDeltas) / ObjectUsageChainBuilder.TrimTargetShare) + 2;
    }

    [Fact]
    public async Task ATrimToTheMinimumSpan_KeepsTheBaselineOfA7DayWindow_SoThe7DayReadComesFromTheChain()
    {
        /* One builder with a cap the test moves. The chain is built whole, then the cap drops to where the trim budget holds the minimum span and no more. */
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, ObjectUsageChainHistory.Generate());
        var expected7 = await ToolAsync(7);
        long capRows = long.MaxValue / 1024;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now,
            () => capRows * (double)ObjectUsageChainBuilder.EstimatedBytesPerRow / (1048576.0 * ObjectUsageChainBuilder.CapShareOfResetPoint));
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count(), await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));

        capRows = await MinimumSpanCapAsync();
        Assert.True(await ChainRowsAsync() > builder.CapRows, "the whole chain fits under the cap, so no pass trims it");
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.True(await ChainRowsAsync() <= builder.CapRows);

        /* The trim went to the minimum span: the kept chain starts on the span's oldest day and ends at the newest snapshot. */
        Assert.Equal(Start(ObjectUsageChainBuilder.MinKeptDays - 1), await OldestAsync(Database));
        Assert.Equal(Start(0), await NewestAsync(Database));

        /* The 7 day read comes from the chain and is the fold's, and the tool answers what it answered with no chain. */
        var (week, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7));
        Assert.True(week is not null, $"the 7 day read did not come from the chain: {reason}");
        Assert.Null(reason);
        Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(7))), Json(week));
        Assert.Equal(expected7, await ToolAsync(7));

        /* A further pass builds nothing and removes nothing. */
        var settled = await ChainRowsAsync();
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(settled, await ChainRowsAsync());
    }

    [Fact]
    public async Task ATrimToTheMinimumSpan_KeepsTheBaseline_WhenTheNewestSnapshotIsEarlierInItsDayThanTheSnapshotsAWeekBack()
    {
        /* Hourly snapshots at ten past the hour for 20 days, then the newest one at five past midnight. A 7 day window read at the newest snapshot starts
           before the first snapshot of the 8th day back, so its baseline is the last snapshot of the 9th. */
        var newest = new DateTime(2026, 11, 14, 0, 5, 0);
        var first = newest.Date.AddDays(-20).AddMinutes(10);
        var times = Enumerable.Range(0, 20 * 24).Select(hour => first.AddHours(hour)).Append(newest).ToList();
        var rows = new List<ObjectUsageChainHistory.Row>();
        for (var i = 0; i < times.Count; i++)
        {
            foreach (var indexId in new[] { 1, 2 })
            {
                rows.Add(new ObjectUsageChainHistory.Row(
                    times[i], new DateTime(2026, 1, 1, 3, 0, 0), ObjectUsageChainHistory.Node1, 1, "Orders", indexId, (i + 1) * (10L + indexId), 0, 0,
                    indexId == 1 ? (i + 1) * 3L : 0, times[i].AddMinutes(-2), null, null, indexId == 1 ? times[i].AddMinutes(-1) : null));
            }
        }

        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, rows);
        var start = newest.AddDays(-7);
        Assert.Equal(
            newest.Date.AddDays(-8).AddHours(23).AddMinutes(10),
            await TimeAsync($"SELECT max(collection_time) FROM index_object_stats WHERE collection_time <= TIMESTAMP '{start:yyyy-MM-dd HH:mm:ss}'"));

        long capRows = long.MaxValue / 1024;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => DateTime.SpecifyKind(newest, DateTimeKind.Utc),
            () => capRows * (double)ObjectUsageChainBuilder.EstimatedBytesPerRow / (1048576.0 * ObjectUsageChainBuilder.CapShareOfResetPoint));
        Assert.Equal(times.Count, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));

        capRows = await MinimumSpanCapAsync();
        Assert.True(await ChainRowsAsync() > builder.CapRows, "the whole chain fits under the cap, so no pass trims it");
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.True(await ChainRowsAsync() <= builder.CapRows);

        /* The trim went to the minimum span: the kept chain starts at the first snapshot of the span's oldest day and ends at the newest snapshot. */
        Assert.Equal(newest.Date.AddDays(-(ObjectUsageChainBuilder.MinKeptDays - 1)).AddMinutes(10), await OldestAsync(Database));
        Assert.Equal(newest, await NewestAsync(Database));

        var (week, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, start);
        Assert.True(week is not null, $"the 7 day read did not come from the chain: {reason}");
        Assert.Null(reason);
        Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, start)), Json(week));
    }

    [Fact]
    public async Task WithNoTrim_EachDatabasesChainStartsAtItsOwnOldestSnapshot_WhateverTheOtherChainsHold()
    {
        /* Sales holds the newest 40 days of the history and is built first. Tiny, with the whole history, arrives after it. The cap is far above both. */
        var history = ObjectUsageChainHistory.Generate();
        var recent = history.Where(r => r.CollectionTime >= Start(40)).ToList();
        var recentSnapshots = recent.Select(r => r.CollectionTime).Distinct().Count();
        var snapshots = ObjectUsageChainHistory.SnapshotDays().Count();
        Assert.True(recentSnapshots < snapshots);
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, recent);
        var capRows = 100_000L;
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now, ResetPointFor(capRows));

        Assert.Equal(recentSnapshots, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(Start(40), await OldestAsync(Database));

        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, SmallDatabase, history, firstId: -5_000_000, logRuns: false);
        Assert.Equal(snapshots, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(Start(100), await OldestAsync(SmallDatabase));
        Assert.Equal(Start(0), await NewestAsync(SmallDatabase));
        Assert.Equal(Start(40), await OldestAsync(Database));
        Assert.Equal((long)snapshots, await ScalarAsync($"SELECT count(*) FROM object_usage_snapshots WHERE database_name = '{SmallDatabase}'"));
        Assert.Equal((long)recentSnapshots, await ScalarAsync($"SELECT count(*) FROM object_usage_snapshots WHERE database_name = '{Database}'"));
        Assert.True(await ChainRowsAsync() < capRows);

        /* Tiny's whole history reads from its chain, and a further pass builds nothing and moves neither start. */
        var quarter = await _dataService.TryReadObjectUsageChainAsync(_serverId, SmallDatabase, Start(90));
        Assert.NotNull(quarter.Result);
        Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, SmallDatabase, Start(90))), Json(quarter.Result));
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(Start(100), await OldestAsync(SmallDatabase));
        Assert.Equal(Start(40), await OldestAsync(Database));
    }
}
