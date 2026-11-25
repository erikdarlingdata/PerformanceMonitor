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
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9596: Lite's <c>get_object_usage</c> reads a persisted per-snapshot chain, and its answer is the full fold's. One history of daily snapshots
/// (<see cref="ObjectUsageChainHistory"/>: growth, two restarts, a lone index clear, a failover to a second node and back, an object that is never
/// used, one dropped, one created late, a 12 day gap) is read both ways at many window starts, built in one call and one snapshot per call, and
/// carried through a store that was written before the chain existed.
/// </summary>
public sealed class ObjectUsageChainTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ObjectUsageChainSrv";
    private const string Database = "Sales";
    private static readonly DateTime Now = ObjectUsageChainHistory.Now;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;

    public ObjectUsageChainTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-usagechain-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        (_serverManager, _serverId) = NewServerManager(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    internal static (ServerManager Manager, int ServerId) NewServerManager(string configDir)
    {
        var manager = new ServerManager(configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        manager.AddServer(server);
        return (manager, RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server)));
    }

    private static string Json(ObjectUsageWindowResult result) => JsonSerializer.Serialize(result);

    private static DateTime Start(int daysBack) => DateTime.SpecifyKind(Now.AddDays(-daysBack), DateTimeKind.Unspecified);

    private Task<string> ToolAsync(int daysBack, string database = Database) =>
        McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, database, daysBack, 500, 0, Now);

    private async Task SeedAsync(string database = Database, IEnumerable<ObjectUsageChainHistory.Row>? rows = null, long firstId = -1) =>
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, database, rows ?? ObjectUsageChainHistory.Generate(), firstId);

    private async Task<List<string>> QueryAsync(string sql, params object[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = arg });
        }

        var lines = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var cells = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells.Add(reader.IsDBNull(i) ? "~" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture) ?? "");
            }

            lines.Add(string.Join("|", cells));
        }

        return lines;
    }

    private async Task ExecAsync(string sql)
    {
        using var writeLock = _duckDb.AcquireWriteLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<long> CountAsync(string table) => long.Parse((await QueryAsync($"SELECT count(*) FROM {table}")).Single(), System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public async Task ThePersistedPath_GivesTheFullFoldsAnswer_AtEveryWindowStart_AndIsTakenAt7And90Days()
    {
        await SeedAsync();
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        var built = await builder.BuildPairAsync(_serverId, Database, int.MaxValue, CancellationToken.None);
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count(), built);

        /* 7 and 90 days, the two windows the tool is called with, then starts on either side of every kind of step: the failover, both restarts,
           the lone clear, the gap (a start inside it has a baseline 8 days back, a start after it does not), before the first snapshot and after
           the newest. */
        var starts = new[] { 1, 3, 7, 15, 21, 22, 23, 24, 26, 30, 41, 45, 48, 55, 61, 65, 69, 70, 72, 90, 99, 100, 120, 400 };
        var persistedCount = 0;
        foreach (var daysBack in starts)
        {
            var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(daysBack));
            var (persisted, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(daysBack));
            if (persisted is not null)
            {
                persistedCount++;
                Assert.Equal(Json(full), Json(persisted));
            }
            else
            {
                Assert.False(string.IsNullOrEmpty(reason), $"a window of {daysBack} days fell back with no reason");
            }

            /* Whichever path answered, the read the tool makes is the full fold's answer. */
            Assert.Equal(Json(full), Json(await _dataService.GetObjectUsageWindowAsync(_serverId, Database, Start(daysBack))));

            if (daysBack is 7 or 90)
            {
                Assert.NotNull(persisted);
                Assert.Null(reason);
                Assert.True(full.SnapshotCount > 1);
            }
        }

        Assert.True(persistedCount >= starts.Length - 4, $"only {persistedCount} of {starts.Length} windows read from the chain");

        /* The 90 day window crosses both restarts, the lone clear, the failover and the gap: the answer holds them. */
        var ninety = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(90));
        Assert.Contains(ninety.Resets, r => (r.Reasons & ObjectUsageResetReasons.Restart) != 0);
        Assert.Contains(ninety.Resets, r => (r.Reasons & ObjectUsageResetReasons.NodeChanged) != 0);
        Assert.True(ninety.IndexResets >= 1);
        Assert.True(ninety.LargestGap?.TotalDays > 10);
        Assert.Equal(2, ninety.AnsweringServerNames.Count);
    }

    [Fact]
    public async Task TheToolsReply_IsTheSameFromTheChainAndFromTheFold_At7And90Days_AndTheFoldTakesOverWithoutAChain()
    {
        await SeedAsync();
        var folded7 = await ToolAsync(7);
        var folded90 = await ToolAsync(90);
        Assert.Equal(0L, await CountAsync("object_usage_snapshots"));

        await new ObjectUsageChainBuilder(_duckDb, () => Now).BuildPairAsync(_serverId, Database, int.MaxValue, CancellationToken.None);
        Assert.True(await CountAsync("object_usage_snapshots") > 0);
        Assert.True(await CountAsync("object_usage_deltas") > 0);
        Assert.True(await CountAsync("object_usage_runs") > 0);

        Assert.Equal(folded7, await ToolAsync(7));
        Assert.Equal(folded90, await ToolAsync(90));
        Assert.Contains("\"object_name\":\"Audit\"", folded90, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABuildOneSnapshotAtATime_StoresTheChainOfOneBackfill_SoTheRestoreIsExact()
    {
        var rows = ObjectUsageChainHistory.Generate();
        await SeedAsync("Sales", rows, firstId: -1);
        await SeedAsync("SalesStepped", rows, firstId: -100000);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);

        var backfill = await builder.BuildPairAsync(_serverId, "Sales", int.MaxValue, CancellationToken.None);
        var calls = 0;
        var stepped = 0;
        int built;
        while ((built = await builder.BuildPairAsync(_serverId, "SalesStepped", 1, CancellationToken.None)) > 0)
        {
            stepped += built;
            calls++;
        }

        Assert.Equal(backfill, stepped);
        Assert.Equal(backfill, calls);

        const string SnapshotsSql = "SELECT collection_time, step_version, answering_server_name, sqlserver_start_time, node_key, compared_with, kept_start, reasons, "
            + "counted, index_resets, blind_from FROM object_usage_snapshots WHERE database_name = $1 ORDER BY collection_time";
        const string DeltasSql = "SELECT collection_time, object_id, index_id, reads, updates, last_read, last_update FROM object_usage_deltas WHERE database_name = $1 "
            + "ORDER BY collection_time, object_id, index_id";
        const string RunsSql = "SELECT object_id, index_id, first_time, last_time, schema_name, object_name, is_indexed_view, is_memory_optimized FROM object_usage_runs "
            + "WHERE database_name = $1 ORDER BY object_id, index_id, first_time";
        foreach (var sql in new[] { SnapshotsSql, DeltasSql, RunsSql })
        {
            var expected = await QueryAsync(sql, "Sales");
            Assert.NotEmpty(expected);
            Assert.Equal(expected, await QueryAsync(sql, "SalesStepped"));
        }

        /* A dropped object closes its run, a created one opens one, and the lone clear and the restarts each left their mark. */
        Assert.Contains((await QueryAsync(RunsSql, "Sales")), l => l.StartsWith("4|1|", StringComparison.Ordinal) && !l.Contains("|~|", StringComparison.Ordinal));
        Assert.Contains((await QueryAsync("SELECT count(*) FROM object_usage_snapshots WHERE database_name = $1 AND index_resets > 0", "Sales")), l => l != "0");
        Assert.Equal(await ToolAsync(90, "Sales"), (await ToolAsync(90, "SalesStepped")).Replace("SalesStepped", "Sales", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AChainBehindTheNewestSnapshot_TakesTheFold_UntilThePassCatchesUp()
    {
        var rows = ObjectUsageChainHistory.Generate();
        var newest = rows.Max(r => r.CollectionTime);
        await SeedAsync(rows: rows.Where(r => r.CollectionTime < newest));
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        await builder.BuildPairAsync(_serverId, Database, int.MaxValue, CancellationToken.None);

        await SeedAsync(rows: rows.Where(r => r.CollectionTime == newest), firstId: -500000);
        var (result, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7));
        Assert.Null(result);
        Assert.Equal("the newest facts row is before the newest snapshot", reason);
        var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(7));
        Assert.Equal(Json(full), Json(await _dataService.GetObjectUsageWindowAsync(_serverId, Database, Start(7))));

        Assert.Equal(1, await builder.BuildPassAsync(100, CancellationToken.None));
        Assert.Equal(0, await builder.BuildPassAsync(100, CancellationToken.None));
        var (caughtUp, none) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7));
        Assert.Null(none);
        Assert.Equal(Json(full), Json(caughtUp!));
    }

    [Fact]
    public async Task AChainBuiltUnderOlderRules_IsDeletedAndRebuilt_AndTheFoldAnswersMeanwhile()
    {
        await SeedAsync();
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        await builder.BuildPairAsync(_serverId, Database, int.MaxValue, CancellationToken.None);
        var snapshots = await CountAsync("object_usage_snapshots");
        var deltas = await CountAsync("object_usage_deltas");
        var runs = await CountAsync("object_usage_runs");
        var full = Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(90)));

        await ExecAsync("UPDATE object_usage_snapshots SET step_version = 0 WHERE collection_time = (SELECT max(collection_time) FROM object_usage_snapshots)");
        await ExecAsync("INSERT INTO object_usage_deltas VALUES (" + _serverId + ", 'Sales', TIMESTAMP '2026-11-14 12:00:00', 99, 1, 5, 5, NULL, NULL)");
        var (stale, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(90));
        Assert.Null(stale);
        Assert.Equal("a facts row has an older step version", reason);
        Assert.Equal(full, Json(await _dataService.GetObjectUsageWindowAsync(_serverId, Database, Start(90))));

        Assert.Equal((int)snapshots, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(snapshots, await CountAsync("object_usage_snapshots"));
        Assert.Equal(deltas, await CountAsync("object_usage_deltas"));
        Assert.Equal(runs, await CountAsync("object_usage_runs"));
        Assert.Equal(0L, long.Parse((await QueryAsync("SELECT count(*) FROM object_usage_snapshots WHERE step_version <> $1", (short)ObjectUsageChainVersion.Current)).Single()));
        var (rebuilt, none) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(90));
        Assert.Null(none);
        Assert.Equal(full, Json(rebuilt!));
    }

    [Fact]
    public async Task ThePass_ListsADatabaseWithSnapshotsAndNoChain_AndPurgeKeepsWhatTheHorizonKeeps()
    {
        await SeedAsync();
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        var expected = ObjectUsageChainHistory.SnapshotDays().Count();

        /* A step cap stops the pass part way, and the next pass carries on from the watermark. */
        Assert.Equal(10, await builder.BuildPassAsync(10, CancellationToken.None));
        Assert.Equal(10L, await CountAsync("object_usage_snapshots"));
        Assert.Equal(expected - 10, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));

        var openRuns = await CountAsync("object_usage_runs WHERE last_time IS NULL");
        var closedRuns = await CountAsync("object_usage_runs WHERE last_time IS NOT NULL");
        Assert.True(openRuns > 0);
        Assert.True(closedRuns > 0);

        /* Legacy is dropped 30 days ago, so its closed run ends before this cutoff while every open run reaches past it. */
        var cutoff = Start(20);
        var cutoffText = cutoff.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        await builder.PurgeAsync(cutoff, CancellationToken.None);
        Assert.Equal(0L, await CountAsync("object_usage_snapshots WHERE collection_time < TIMESTAMP '" + cutoffText + "'"));
        Assert.Equal(0L, await CountAsync("object_usage_deltas WHERE collection_time < TIMESTAMP '" + cutoffText + "'"));
        Assert.Equal(openRuns, await CountAsync("object_usage_runs WHERE last_time IS NULL"));
        Assert.True(await CountAsync("object_usage_runs WHERE last_time IS NOT NULL") < closedRuns);

        /* A database whose snapshots are gone loses its open runs too. */
        await ExecAsync("DELETE FROM object_usage_snapshots");
        await builder.PurgeAsync(Start(500), CancellationToken.None);
        Assert.Equal(0L, await CountAsync("object_usage_runs WHERE last_time IS NULL"));
    }

    [Fact]
    public async Task AFreshStore_CreatesTheChainTables_AtTheCurrentVersion()
    {
        var path = Path.Combine(_configDir, "fresh.duckdb");
        using var fresh = new DuckDbInitializer(path);
        await fresh.InitializeAsync();

        foreach (var table in new[] { "object_usage_snapshots", "object_usage_deltas", "object_usage_runs" })
        {
            Assert.Equal("1", Scalar(fresh, $"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"));
            Assert.Equal("0", Scalar(fresh, $"SELECT count(*) FROM {table}"));
        }

        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), Scalar(fresh, "SELECT max(version) FROM schema_version"));
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 87);
    }

    [Fact]
    public async Task AStoreAtV86_WithSnapshotsAndNoChain_UpgradesToTheCurrentVersion_GetsTheTables_AndAnswersTheSame()
    {
        var path = Path.Combine(_configDir, "v86.duckdb");
        string before7;
        string before90;
        using (var old = new DuckDbInitializer(path))
        {
            await old.InitializeAsync();
            await ObjectUsageChainHistory.SeedAsync(old, _serverId, ServerName, Database, ObjectUsageChainHistory.Generate());
            Scalar(old, "DROP TABLE object_usage_snapshots");
            Scalar(old, "DROP TABLE object_usage_deltas");
            Scalar(old, "DROP TABLE object_usage_runs");
            Scalar(old, "UPDATE schema_version SET version = 86");
            var oldService = new LocalDataService(old);
            before7 = await McpObjectUsageTools.GetObjectUsage(oldService, _serverManager, ServerName, Database, 7, 500, 0, Now);
            before90 = await McpObjectUsageTools.GetObjectUsage(oldService, _serverManager, ServerName, Database, 90, 500, 0, Now);
            Assert.Contains("\"object_name\":\"Orders\"", before90, StringComparison.Ordinal);
        }

        using var upgraded = new DuckDbInitializer(path);
        await upgraded.InitializeAsync();
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), Scalar(upgraded, "SELECT max(version) FROM schema_version"));
        foreach (var table in new[] { "object_usage_snapshots", "object_usage_deltas", "object_usage_runs" })
        {
            Assert.Equal("1", Scalar(upgraded, $"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"));
            Assert.Equal("0", Scalar(upgraded, $"SELECT count(*) FROM {table}"));
        }

        /* The snapshots are there and the chain is empty: the read folds, and says so. */
        var service = new LocalDataService(upgraded);
        var (none, reason) = await service.TryReadObjectUsageChainAsync(_serverId, Database, Start(90));
        Assert.Null(none);
        Assert.Equal("no facts row at the baseline", reason);
        Assert.Equal(before7, await McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 7, 500, 0, Now));
        Assert.Equal(before90, await McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 90, 500, 0, Now));

        /* The background pass fills the chain from the oldest stored snapshot, and the answers do not move. */
        var built = await new ObjectUsageChainBuilder(upgraded, () => Now).BuildPassAsync(int.MaxValue, CancellationToken.None);
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count(), built);
        var (persisted, none2) = await service.TryReadObjectUsageChainAsync(_serverId, Database, Start(90));
        Assert.Null(none2);
        Assert.NotNull(persisted);
        Assert.Equal(before7, await McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 7, 500, 0, Now));
        Assert.Equal(before90, await McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 90, 500, 0, Now));
    }

    private static string Scalar(DuckDbInitializer duckDb, string sql)
    {
        using var writeLock = duckDb.AcquireWriteLock();
        using var connection = duckDb.CreateConnection();
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) ?? "";
    }
}
