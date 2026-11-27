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
///
/// <para>The class runs alone, in the <c>timing</c> collection. Its held-read test keeps the process-wide store lock in read mode through a whole
/// build, and a write from a class running beside it would wait behind that read and hold up the builder's next read, so neither would finish.</para>
/// </summary>
[Collection("timing")]
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
        ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, database, daysBack, 500, 0, Now));

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
    public async Task AStoreThatGainedItsNodeNameAndMemoryFlagLate_AnswersFromTheChainAsTheFoldDoes_BuiltInOneCallAndOneSnapshotAtATime()
    {
        /* Every store written before the two columns was added holds the same two transitions: unnamed snapshots then named ones, and a null
           memory flag then false. The one-snapshot-at-a-time build restores the stepper from the stored rows between every pair of snapshots, so
           it takes the unnamed-to-named takeover through the restore as well. */
        var rows = ObjectUsageChainHistory.GenerateUpgraded();
        Assert.Contains(rows, r => r.Node is null);
        Assert.Contains(rows, r => r.Node is not null);
        Assert.Contains(rows, r => r.MemoryOptimized is null);
        Assert.Contains(rows, r => r.MemoryOptimized is not null);
        await SeedAsync("Sales", rows, firstId: -1);
        await SeedAsync("SalesStepped", rows, firstId: -100000);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);

        var backfill = await builder.BuildPairAsync(_serverId, "Sales", int.MaxValue, CancellationToken.None);
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count(), backfill);
        var stepped = 0;
        int built;
        while ((built = await builder.BuildPairAsync(_serverId, "SalesStepped", 1, CancellationToken.None)) > 0)
        {
            stepped += built;
        }

        Assert.Equal(backfill, stepped);
        const string SnapshotsSql = "SELECT collection_time, step_version, answering_server_name, sqlserver_start_time, node_key, compared_with, kept_start, reasons, "
            + "counted, index_resets, blind_from FROM object_usage_snapshots WHERE database_name = $1 ORDER BY collection_time";
        const string RunsSql = "SELECT object_id, index_id, first_time, last_time, schema_name, object_name, is_indexed_view, is_memory_optimized FROM object_usage_runs "
            + "WHERE database_name = $1 ORDER BY object_id, index_id, first_time";
        foreach (var sql in new[] { SnapshotsSql, RunsSql })
        {
            var expected = await QueryAsync(sql, "Sales");
            Assert.NotEmpty(expected);
            Assert.Equal(expected, await QueryAsync(sql, "SalesStepped"));
        }

        /* The null-to-false change reopened every index: runs that end at the change and runs that start there. */
        Assert.Contains(await QueryAsync(RunsSql, "Sales"), l => l.Split('|')[^1] == "~");
        Assert.Contains(await QueryAsync(RunsSql, "Sales"), l => l.Split('|')[^1] == "False");

        /* Windows on both sides of each transition, across the first restart and the failover, and the two the tool is called with. */
        var starts = new[]
        {
            1, 3, 7, 15, 22, 26, 30, 41, 48, 55, 61, 64, 66, 67, 68, 70, 72, 76, 79, 80, 81, 82, 84, 90, 99, 100, 120, 400,
        };
        foreach (var database in new[] { "Sales", "SalesStepped" })
        {
            var persistedCount = 0;
            foreach (var daysBack in starts)
            {
                var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, database, Start(daysBack));
                var (persisted, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, database, Start(daysBack));
                if (persisted is not null)
                {
                    persistedCount++;
                    Assert.Equal(Json(full), Json(persisted));
                }
                else
                {
                    Assert.False(string.IsNullOrEmpty(reason), $"a window of {daysBack} days of {database} fell back with no reason");
                }

                Assert.Equal(Json(full), Json(await _dataService.GetObjectUsageWindowAsync(_serverId, database, Start(daysBack))));
                if (daysBack is 7 or 90)
                {
                    Assert.NotNull(persisted);
                    Assert.True(full.SnapshotCount > 1);
                }
            }

            Assert.True(persistedCount >= starts.Length / 2, $"only {persistedCount} of {starts.Length} windows of {database} read from the chain");
        }
    }

    [Fact]
    public async Task TheFullFoldsRows_ComeInCollectionIdOrderInsideASnapshot_AsTheBuilderReadsThem()
    {
        /* The seed gives later rows lower ids, so collection_id order is the reverse of insertion order inside a snapshot. */
        await SeedAsync();
        var expected = await QueryAsync(
            "SELECT collection_time, object_id, index_id FROM index_object_stats WHERE server_id = $1 AND database_name = $2 AND collection_time >= $3 "
            + "ORDER BY collection_time, collection_id", _serverId, Database, Start(3));
        Assert.True(expected.Count > 8);

        var actual = await QueryAsync(ObjectUsageWindowRead.SqlInCollectionIdOrder, _serverId, Database, Start(3));
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            /* The statement's first three cells are collection_time, sqlserver_start_time and answering_server_name, then object_id and index_id. */
            var cells = actual[i].Split('|');
            Assert.Equal(expected[i], string.Join("|", cells[0], cells[3], cells[4]));
        }
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
    public async Task ASnapshotThatLandsInsideTheBuiltChain_IsAHoleTheFoldFills_SoTheWindowsHoldingItTakeTheFold()
    {
        var rows = ObjectUsageChainHistory.Generate();
        var late = ObjectUsageChainHistory.Day(3);
        await SeedAsync(rows: rows.Where(r => r.CollectionTime != late));
        await new ObjectUsageChainBuilder(_duckDb, () => Now).BuildPairAsync(_serverId, Database, int.MaxValue, CancellationToken.None);

        /* The snapshot lands after the chain stepped past its time (a late write, or a clock stepped back across a restart). The chain's own links
           are consistent without it: the step after the hole compares the snapshot before it. */
        await SeedAsync(rows: rows.Where(r => r.CollectionTime == late), firstId: -500000);

        var (hole, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(7));
        Assert.Null(hole);
        Assert.Equal("the chain holds a different number of snapshots than the window", reason);
        var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(7));
        Assert.Equal(8, full.SnapshotCount);
        Assert.Equal(Json(full), Json(await _dataService.GetObjectUsageWindowAsync(_serverId, Database, Start(7))));

        /* A window that ends its reach before the hole is still answered from the chain. */
        var (after, none) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(2));
        Assert.Null(none);
        Assert.Equal(Json(await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(2))), Json(after!));
    }

    [Fact]
    public async Task ASnapshotWhoseRunHasNotFinished_IsLeftUnfolded_AndFoldedWhole_OnceItsRunIsLogged()
    {
        var rows = ObjectUsageChainHistory.Generate();
        var newest = rows.Max(r => r.CollectionTime);
        var newestRows = rows.Where(r => r.CollectionTime == newest).OrderBy(r => r.ObjectId).ThenBy(r => r.IndexId).ToList();
        var landed = newestRows.Take(newestRows.Count / 2).ToList();
        var later = newestRows.Skip(newestRows.Count / 2).ToList();
        Assert.NotEmpty(landed);
        Assert.NotEmpty(later);

        /* The collector's appender flushes a database's rows without a transaction, so a read between flushes sees part of the newest snapshot, and
           its run has no collection_log row yet. */
        await SeedAsync(rows: rows.Where(r => r.CollectionTime < newest));
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, landed, firstId: -500000, logRuns: false);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count() - 1, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count() - 1L, await CountAsync("object_usage_snapshots"));
        Assert.Equal(0L, await CountAsync("object_usage_snapshots WHERE collection_time = TIMESTAMP '" + newest.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + "'"));
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));

        /* The rest lands and the run is logged: the next pass folds the whole snapshot, and the answers are the fold's. */
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, later, firstId: -600000, logRuns: false);
        await ObjectUsageChainHistory.SeedLogAsync(_duckDb, _serverId, ServerName, [newest]);
        Assert.Equal(1, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        foreach (var daysBack in new[] { 7, 90 })
        {
            var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(daysBack));
            var (persisted, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, Database, Start(daysBack));
            Assert.Null(reason);
            Assert.Equal(Json(full), Json(persisted!));
        }
    }

    [Fact]
    public async Task AServerWithNoRunLogged_HasNothingFolded()
    {
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, ObjectUsageChainHistory.Generate(), logRuns: false);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);
        Assert.Equal(0, await builder.BuildPassAsync(int.MaxValue, CancellationToken.None));
        Assert.Equal(0L, await CountAsync("object_usage_snapshots"));
        var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, Database, Start(7));
        Assert.Equal(Json(full), Json(await _dataService.GetObjectUsageWindowAsync(_serverId, Database, Start(7))));
    }

    [Fact]
    public async Task TheBuilderWritesUnderTheReadLock_SoAHeldReadDoesNotKeepItOut_AndAMassClosingIsOneSet()
    {
        /* Wide: every index of 3,000 objects changes identity at the third snapshot (the flag is null before it and false from it), so the third
           step ends 3,000 runs and starts 3,000. */
        const int Objects = 3000;
        var times = new[] { ObjectUsageChainHistory.Day(3), ObjectUsageChainHistory.Day(2), ObjectUsageChainHistory.Day(1) };
        var rows = new List<ObjectUsageChainHistory.Row>();
        for (var step = 0; step < times.Length; step++)
        {
            for (var objectId = 1; objectId <= Objects; objectId++)
            {
                rows.Add(new ObjectUsageChainHistory.Row(
                    times[step], ObjectUsageChainHistory.Day(60), ObjectUsageChainHistory.Node1, objectId, "T" + objectId, 1, (step + 1) * (1 + objectId % 5), 0, 0, step + 1,
                    times[step].AddHours(-1), null, null, times[step].AddHours(-2), step < 2 ? null : false));
            }
        }

        await SeedAsync("Wide", rows);
        var builder = new ObjectUsageChainBuilder(_duckDb, () => Now);

        /* A read held on another thread for the whole build: a builder that asked for the write lock would wait out its budget. */
        using var heldRead = new ObjectUsageChainHeldRead(_duckDb);
        try
        {
            Assert.Equal(3, await builder.BuildPairAsync(_serverId, "Wide", int.MaxValue, CancellationToken.None));
            await builder.PurgeAsync(Start(2000), CancellationToken.None);
        }
        finally
        {
            await heldRead.ReleaseAsync();
        }

        Assert.Equal(2L * Objects, await CountAsync("object_usage_runs"));
        var secondTime = times[1].ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        var thirdTime = times[2].ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal((long)Objects, await CountAsync("object_usage_runs WHERE last_time = TIMESTAMP '" + secondTime + "' AND first_time < TIMESTAMP '" + thirdTime + "'"));
        Assert.Equal((long)Objects, await CountAsync("object_usage_runs WHERE last_time IS NULL AND first_time = TIMESTAMP '" + thirdTime + "'"));
        var full = await _dataService.GetObjectUsageWindowFullAsync(_serverId, "Wide", Start(7));
        var (persisted, reason) = await _dataService.TryReadObjectUsageChainAsync(_serverId, "Wide", Start(7));
        Assert.Null(reason);
        Assert.Equal(Json(full), Json(persisted!));
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
            before7 = await ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(oldService, _serverManager, ServerName, Database, 7, 500, 0, Now));
            before90 = await ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(oldService, _serverManager, ServerName, Database, 90, 500, 0, Now));
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
        Assert.Equal(before7, await ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 7, 500, 0, Now)));
        Assert.Equal(before90, await ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 90, 500, 0, Now)));

        /* The background pass fills the chain from the oldest stored snapshot, and the answers do not move. */
        var built = await new ObjectUsageChainBuilder(upgraded, () => Now).BuildPassAsync(int.MaxValue, CancellationToken.None);
        Assert.Equal(ObjectUsageChainHistory.SnapshotDays().Count(), built);
        var (persisted, none2) = await service.TryReadObjectUsageChainAsync(_serverId, Database, Start(90));
        Assert.Null(none2);
        Assert.NotNull(persisted);
        Assert.Equal(before7, await ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 7, 500, 0, Now)));
        Assert.Equal(before90, await ObjectUsageChainHistory.UnpathedAsync(McpObjectUsageTools.GetObjectUsage(service, _serverManager, ServerName, Database, 90, 500, 0, Now)));
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
