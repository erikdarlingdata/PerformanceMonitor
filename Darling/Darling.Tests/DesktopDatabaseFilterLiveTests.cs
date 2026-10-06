/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5312: the Darling viewer's saved per-server database filter narrows the database-scoped reads the web filter narrows (object
/// locking) and the ones the web is about to narrow (file I/O latency and throughput, database sizes and storage growth, the
/// persistent version store). Every read keeps its unfiltered SQL and rows when the filter is null, and a filter keeps exactly the
/// chosen databases (the same <c>database_name = ANY($n)</c> predicate the other desktop reads use, so a NULL database never
/// matches a filter). The seed is three databases (A, B and C) per read, so [A, B] must return A's and B's rows and none of C's.
///
/// <para>The parity checks compare against Darling's MCP reader on the same seed: object locking against
/// <see cref="DarlingObjectStatsReader.GetIndexLockingAsync(NpgsqlDataSource,int,int,DatabaseFilter,CancellationToken)"/> with the
/// same set, file I/O against the MCP series list for one database, and database sizes and the PVS against the MCP reader's
/// unfiltered database set. Skips without <c>DARLING_TEST_PG</c>, like its siblings.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DesktopDatabaseFilterLiveTests
{
    /* #1776 own-store: this class owns the rows of its one deterministic server, seeds them itself and removes them. */
    private const string ServerName = "darling-desktop-dbfilter-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbA = "DeskDbA";
    private const string DbB = "DeskDbB";
    private const string DbC = "DeskDbC";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live desktop database-filter test.";

    private static readonly string[] AB = { DbA, DbB };

    [Fact]
    public async Task IndexLocking_Filter_NarrowsGridAndSelector_MatchesTheMcpReader_TheBoxAndTheFilterBothApply()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);
        var bodySucceeded = false;
        try
        {
            using var connection = await SeedAsync(cs!, ct);
            var t = Naive(DateTime.UtcNow.AddMinutes(-2));
            foreach (var db in new[] { DbA, DbB, DbC })
            {
                await InsertIndexRowAsync(connection, ct, t, db, "Orders_" + db, rowLockWaitMs: 50_000);
            }

            var all = await viewer.GetIndexLockingAsync(ServerId, 200, null, cancellationToken: ct);
            Assert.Equal(new[] { DbA, DbB, DbC }, all.Select(r => r.DatabaseName).Order().ToArray());

            /* The same rows as the MCP reader for the same set. */
            var ab = await viewer.GetIndexLockingAsync(ServerId, 200, null, AB, ct);
            var mcp = await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, ServerId, 200, DatabaseFilter.Of(AB), ct);
            Assert.Equal(new[] { DbA, DbB }, ab.Select(r => r.DatabaseName).Order().ToArray());
            Assert.Equal(
                mcp.Select(r => (r.DatabaseName, r.TableName)).Order().ToArray(),
                ab.Select(r => (r.DatabaseName, r.TableName)).Order().ToArray());

            /* The selector lists only databases inside the filter, and an unfiltered one lists all three. */
            Assert.Equal(new[] { DbA, DbB }, (await viewer.GetIndexLockingDatabasesAsync(ServerId, AB, ct)).Order().ToArray());
            Assert.Equal(new[] { DbA, DbB, DbC }, (await viewer.GetIndexLockingDatabasesAsync(ServerId, cancellationToken: ct)).Order().ToArray());

            /* The box AND the filter: a box name inside the filter shows its rows, one outside shows none. */
            var boxInside = await viewer.GetIndexLockingAsync(ServerId, 200, DbA, AB, ct);
            Assert.Equal(new[] { DbA }, boxInside.Select(r => r.DatabaseName).Distinct().ToArray());
            Assert.Empty(await viewer.GetIndexLockingAsync(ServerId, 200, DbC, AB, ct));
            Assert.Single(await viewer.GetIndexLockingAsync(ServerId, 200, DbC, null, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task FileIo_Trends_Filter_SitsInsideTheTopFiles_AndNoFilterIsUnchanged_MatchesTheMcpSeries()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);
        var bodySucceeded = false;
        try
        {
            using var connection = await SeedAsync(cs!, ct);
            var end = DateTime.UtcNow;
            var t1 = end.AddMinutes(-20);
            var t2 = end.AddMinutes(-10);

            /* A has one quiet file and B one a little busier; C has twelve busy ones, so C's files fill the unfiltered top ten and a
               filter applied AFTER the ranking would find nothing of A and B. */
            foreach (var t in new[] { t1, t2 })
            {
                await InsertFileIoAsync(connection, ct, t, DbA, "a.mdf", reads: 10);
                await InsertFileIoAsync(connection, ct, t, DbB, "b.mdf", reads: 20);
                for (var i = 0; i < 12; i++)
                {
                    await InsertFileIoAsync(connection, ct, t, DbC, $"c{i}.mdf", reads: 1000 + i);
                }
            }

            var start = end.AddHours(-1);
            var unfilteredLatency = await viewer.GetFileIoLatencyTrendAsync(ServerId, start, end, cancellationToken: ct);
            Assert.Equal(new[] { DbC }, unfilteredLatency.Select(p => p.DatabaseName).Distinct().ToArray());
            Assert.Equal(10, unfilteredLatency.Select(p => p.FileName).Distinct().Count());

            var latency = await viewer.GetFileIoLatencyTrendAsync(ServerId, start, end, AB, ct);
            Assert.Equal(new[] { DbA, DbB }, latency.Select(p => p.DatabaseName).Distinct().Order().ToArray());
            var throughput = await viewer.GetFileIoThroughputTrendAsync(ServerId, start, end, AB, ct);
            Assert.All(throughput, p => Assert.DoesNotContain(DbC, p.FileLabel));
            Assert.Equal(new[] { DbA + ".a.mdf", DbB + ".b.mdf" }, throughput.Select(p => p.FileLabel).Distinct().Order().ToArray());
            var unfilteredThroughput = await viewer.GetFileIoThroughputTrendAsync(ServerId, start, end, cancellationToken: ct);
            Assert.All(unfilteredThroughput, p => Assert.StartsWith(DbC + ".", p.FileLabel, StringComparison.Ordinal));

            /* Parity: the MCP's series for one database names the same database the viewer draws for that one database. */
            var viewerA = await viewer.GetFileIoLatencyTrendAsync(ServerId, start, end, new[] { DbA }, ct);
            var mcpA = await DarlingTrendReader.GetFileIoSeriesAsync(postgres, ServerId, start, end, DbA, ct);
            Assert.Equal(mcpA.Select(s => s.DatabaseName).Distinct().ToArray(), viewerA.Select(p => p.DatabaseName).Distinct().ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task DatabaseSizes_AndStorageGrowth_Filter_KeepTheChosen_NoFilterIsUnchanged_MatchesTheMcpDatabaseSet()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);
        var bodySucceeded = false;
        try
        {
            using var connection = await SeedAsync(cs!, ct);
            var t = DateTime.UtcNow.AddMinutes(-5);
            await InsertSizeAsync(connection, ct, t, DbA, 100m);
            await InsertSizeAsync(connection, ct, t, DbB, 200m);
            await InsertSizeAsync(connection, ct, t, DbC, 300m);

            var all = await viewer.GetDatabaseSizeLatestAsync(ServerId, cancellationToken: ct);
            Assert.Equal(new[] { DbA, DbB, DbC }, all.Select(r => r.DatabaseName).Distinct().Order().ToArray());

            /* The MCP reader (unfiltered on this surface) names the same databases the unfiltered viewer read does. */
            var mcp = await DarlingObjectStatsReader.GetLatestDatabaseSizesAsync(postgres, ServerId, ct);
            Assert.Equal(mcp.Select(r => r.DatabaseName).Distinct().Order().ToArray(), all.Select(r => r.DatabaseName).Distinct().Order().ToArray());

            var ab = await viewer.GetDatabaseSizeLatestAsync(ServerId, AB, ct);
            Assert.Equal(new[] { DbA, DbB }, ab.Select(r => r.DatabaseName).Distinct().Order().ToArray());

            var summaryAll = await viewer.GetDatabaseSizeSummaryAsync(ServerId, 10, cancellationToken: ct);
            Assert.Equal(3, summaryAll.Count);
            var summary = await viewer.GetDatabaseSizeSummaryAsync(ServerId, 10, AB, ct);
            Assert.Equal(new[] { DbA, DbB }, summary.Select(r => r.DatabaseName).Order().ToArray());
            /* The cap applies after the filter: one slot of [A, B] is the bigger of the two, not C. */
            var capped = await viewer.GetDatabaseSizeSummaryAsync(ServerId, 1, AB, ct);
            Assert.Equal(new[] { DbB }, capped.Select(r => r.DatabaseName).ToArray());

            var growthAll = await viewer.GetStorageGrowthAsync(ServerId, cancellationToken: ct);
            Assert.Equal(new[] { DbA, DbB, DbC }, growthAll.Select(r => r.DatabaseName).Order().ToArray());
            var growth = await viewer.GetStorageGrowthAsync(ServerId, AB, ct);
            Assert.Equal(new[] { DbA, DbB }, growth.Select(r => r.DatabaseName).Order().ToArray());
            Assert.Empty(await viewer.GetStorageGrowthAsync(ServerId, new[] { "NoSuchDb", DbA.ToLowerInvariant() }, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Pvs_Filter_NarrowsTheLatestGridAndTheTrend_TheTopFiveAreOfTheChosen()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(cs!);
        var bodySucceeded = false;
        try
        {
            using var connection = await SeedAsync(cs!, ct);
            var now = DateTime.UtcNow;
            foreach (var t in new[] { now.AddHours(-2), now.AddMinutes(-5) })
            {
                await InsertPvsAsync(connection, ct, t, DbA, 10m);
                await InsertPvsAsync(connection, ct, t, DbB, 20m);
                await InsertPvsAsync(connection, ct, t, DbC, 30m);
                /* Six more databases bigger than A and B: unfiltered, the top five of the trend are all of these. */
                for (var i = 0; i < 6; i++)
                {
                    await InsertPvsAsync(connection, ct, t, $"DeskBig{i}", 100m + i);
                }
            }

            var all = await viewer.GetPvsStatsLatestAsync(ServerId, cancellationToken: ct);
            Assert.Equal(9, all.Count);
            var ab = await viewer.GetPvsStatsLatestAsync(ServerId, AB, ct);
            Assert.Equal(new[] { DbB, DbA }, ab.Select(r => r.DatabaseName).ToArray());

            var trendAll = await viewer.GetPvsTrendAsync(ServerId, now.AddDays(-1), cancellationToken: ct);
            Assert.DoesNotContain(trendAll, p => p.DatabaseName is DbA or DbB);
            var trend = await viewer.GetPvsTrendAsync(ServerId, now.AddDays(-1), AB, ct);
            Assert.Equal(new[] { DbA, DbB }, trend.Select(p => p.DatabaseName).Distinct().Order().ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task<NpgsqlConnection> SeedAsync(string cs, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        return connection;
    }

    private static Task InsertIndexRowAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t, string db, string table, long rowLockWaitMs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, db, "dbo", 100, table, 1,
            "PK_" + table, "CLUSTERED", 90m, 85m, 500_000L, 80L, rowLockWaitMs, 5L, 60L, 2L, 30L, 10L);

    private static Task InsertFileIoAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t, string db, string file, long reads) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
            CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, db, file, "ROWS", "D:\\data\\" + file, 1000m, reads, 5L, reads * 8192, 40960L, reads * 2, 10L, 60);

    private static Task InsertSizeAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t, string db, decimal sizeMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, db, 5, 1, "ROWS", db + "_data", "C:\\data\\" + db + ".mdf", sizeMb, sizeMb / 2);

    private static Task InsertPvsAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t, string db, decimal pvsMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            CollectionIdGenerator.Next(), Naive(t), ServerId, ServerName, db, 7, true, pvsMb, 1000m);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM file_io_stats WHERE server_id = {ServerId}; "
            + $"DELETE FROM database_size_stats WHERE server_id = {ServerId}; DELETE FROM pvs_stats WHERE server_id = {ServerId}; "
            + $"DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #5312: no database needed. The viewer's FinOps tab reads the saved filter from the shell's server store and hands it to every
/// narrowed read, the file I/O tab hands it to both trends, and the Locking, PVS and database-size SQL carries the list-form predicate.
/// A pin on the source, so a tab that stops passing the filter fails here.
/// </summary>
public sealed class DesktopDatabaseFilterSourcePinTests
{
    private static string ViewerFile(string name)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !Directory.Exists(Path.Combine(dir, "PerformanceMonitor.Darling.Viewer")) && !Directory.Exists(Path.Combine(dir, "Darling", "PerformanceMonitor.Darling.Viewer")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var root = Directory.Exists(Path.Combine(dir!, "Darling")) ? Path.Combine(dir!, "Darling") : dir!;
        return File.ReadAllText(Path.Combine(root, "PerformanceMonitor.Darling.Viewer", name));
    }

    [Fact]
    public void TheFileIoTab_PassesTheSavedFilterIntoBothTrends()
    {
        var tab = ViewerFile("ViewerServerTab.FileIo.cs");
        Assert.Contains("GetFileIoLatencyTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter)", tab, StringComparison.Ordinal);
        Assert.Contains("GetFileIoThroughputTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter)", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFinOpsTab_PassesTheSavedFilterIntoItsNarrowedReads_AndTheShellGivesItTheStore()
    {
        var loaders = ViewerFile("FinOpsTab.Loaders.cs");
        Assert.Contains("GetDatabaseSizeLatestAsync(_server.ServerId, SelectedDatabaseFilter)", loaders, StringComparison.Ordinal);
        Assert.Contains("GetDatabaseSizeSummaryAsync(_server.ServerId, databaseNames: SelectedDatabaseFilter)", loaders, StringComparison.Ordinal);
        Assert.Contains("GetPvsStatsLatestAsync(_server.ServerId, SelectedDatabaseFilter)", loaders, StringComparison.Ordinal);
        Assert.Contains("GetPvsTrendAsync(_server.ServerId, DateTime.UtcNow.AddDays(-7), SelectedDatabaseFilter)", loaders, StringComparison.Ordinal);
        /* The free-space health score stays server-wide: its read is the one unfiltered size read, and says why. */
        Assert.Contains("dbSizes = await _dataService.GetDatabaseSizeLatestAsync(_server.ServerId);", loaders, StringComparison.Ordinal);

        var locking = ViewerFile("FinOpsTab.Locking.cs");
        Assert.Contains("GetIndexLockingDatabasesAsync(_server.ServerId, SelectedDatabaseFilter)", locking, StringComparison.Ordinal);
        Assert.Contains("GetIndexLockingAsync(_server.ServerId, 200, db, SelectedDatabaseFilter)", locking, StringComparison.Ordinal);

        Assert.Contains("GetStorageGrowthAsync(_server.ServerId, SelectedDatabaseFilter)", ViewerFile("FinOpsTab.ObjectHeatmap.cs"), StringComparison.Ordinal);

        var tab = ViewerFile("FinOpsTab.xaml.cs");
        Assert.Contains("_serverStore?.GetViewFilterDatabases(_server.ServerName)", tab, StringComparison.Ordinal);
        Assert.Contains("FinOpsContent.Initialize(_dataService, _serverStore)", ViewerFile("MainWindow.xaml.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSql_CarriesTheListFormPredicate_OnEveryNarrowedRead()
    {
        const string P = "IS NULL OR";
        Assert.Contains("$3::text[] " + P + " ios.database_name = ANY($3)", ViewerDataService.IndexLockingAllSql, StringComparison.Ordinal);
        Assert.Contains("$4::text[] " + P + " ios.database_name = ANY($4)", ViewerDataService.IndexLockingByDbSql, StringComparison.Ordinal);
        Assert.Contains("$2::text[] " + P + " ios.database_name = ANY($2)", ViewerDataService.IndexLockingDatabasesSql, StringComparison.Ordinal);
        Assert.Contains("$3::text[] " + P + " database_name = ANY($3)", ViewerDataService.DatabaseSizeLatestSql, StringComparison.Ordinal);
        Assert.Contains("$4::text[] " + P + " database_name = ANY($4)", ViewerDataService.DatabaseSizeSummarySql, StringComparison.Ordinal);
        Assert.Contains("$2::text[] " + P + " database_name = ANY($2)", ViewerDataService.PvsStatsLatestSql, StringComparison.Ordinal);
        Assert.Contains("$3::text[] " + P + " database_name = ANY($3)", ViewerDataService.PvsTrendSql, StringComparison.Ordinal);
        /* File I/O: the filter sits inside top_files, before the LIMIT, so the ten busiest files are the chosen databases' ten. */
        foreach (var sql in new[] { ViewerDataService.FileIoLatencyTrendSql, ViewerDataService.FileIoThroughputTrendSql })
        {
            var filter = sql.IndexOf("$5::text[] " + P + " database_name = ANY($5)", StringComparison.Ordinal);
            Assert.True(filter > 0 && filter < sql.IndexOf("LIMIT 10", StringComparison.Ordinal));
        }
    }
}
