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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 items 6, 15 and 16, Darling side: get_database_sizes publishes a percent-growth file's step, get_table_index_sizes
/// sorts an unsized table last, publishes it as null and leaves the collector's own tempdb temp tables out, and get_index_usage
/// never calls a heap or a clustered index Unused. Lite's twin pins are in <c>Lite.Tests/McpToolAudit5633Tests.cs</c>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpToolAudit5633PinTests
{
    [Fact]
    public void DatabaseSizesPayload_CarriesIsPercentGrowthAndGrowthPct_PerFile()
    {
        var at = new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
        var rows = new List<DarlingObjectStatsReader.DatabaseSizeRow>
        {
            new(at, "Db1", "pct_file", "ROWS", 100, 50, 10, null, null, null, null, 1, true, 10),
            new(at, "Db1", "mb_file", "LOG", 100, 50, 64, null, null, null, null, 2, false, null),
        };

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));
        var files = doc.RootElement.GetProperty("databases")[0].GetProperty("files");
        var pct = files.EnumerateArray().Single(f => f.GetProperty("file_name").GetString() == "pct_file");
        var mb = files.EnumerateArray().Single(f => f.GetProperty("file_name").GetString() == "mb_file");
        Assert.True(pct.GetProperty("is_percent_growth").GetBoolean());
        Assert.Equal(10, pct.GetProperty("growth_pct").GetDouble());
        Assert.False(mb.GetProperty("is_percent_growth").GetBoolean());
        Assert.Equal(JsonValueKind.Null, mb.GetProperty("growth_pct").ValueKind);
    }

    [Fact]
    public void TheSqlShapes_SortNullsLast_ExcludeTempTables_AndSplitTableStorageFromDropCandidates()
    {
        Assert.Contains("is_percent_growth", DarlingObjectStatsReader.DatabaseSizeLatestSql, StringComparison.Ordinal);
        Assert.Contains("growth_pct", DarlingObjectStatsReader.DatabaseSizeLatestSql, StringComparison.Ordinal);

        SqlTextPin.AssertExpresses("ORDER BY l.current_reserved_mb DESC NULLS LAST", DarlingObjectStatsReader.ObjectSizeGrowthSql,
            "an unsized table sorts ahead of sized ones (PostgreSQL puts NULLs first on DESC)");
        Assert.Contains("LEFT(table_name, 1) IS DISTINCT FROM '#'", DarlingObjectStatsReader.ObjectSizeGrowthSql, StringComparison.Ordinal);

        Assert.Contains("'Table not read'", DarlingObjectStatsReader.IndexUsageSql, StringComparison.Ordinal);
        Assert.Contains("index_type_desc LIKE 'CLUSTERED%'", DarlingObjectStatsReader.IndexUsageSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIndexObjectCollector_LeavesOutTempDbTemporaryTables()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            Deltas = new CollectorDeltaCalculator(),
            CollectionTime = DateTime.UtcNow,
            Target = new CollectorTargetInfo { SqlMajorVersion = 17 },
            CurrentDatabaseName = "tempdb",
        };
        Assert.Contains("o.name NOT LIKE N'#%'", IndexObjectStatsCollector.Instance.BuildQuery(context).Text, StringComparison.Ordinal);
        /* The on-premises shape nests the body in sp_executesql, so its quotes are doubled. */
        Assert.Contains("o.name NOT LIKE N''#%''", IndexObjectStatsCollector.Instance.BuildPerItemQuery("tempdb", context).Text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIndexUsageTail_NamesTheTableNotReadClass_AndEveryCounterReset()
    {
        var tail = McpToolGuideTests.Served("get_index_usage").Tail!;
        Assert.Contains("Table not read", tail, StringComparison.Ordinal);
        Assert.Contains("clears on an instance restart, a database restore, a detach and attach, the database going offline, and (for that index) a rebuild", tail, StringComparison.Ordinal);
    }
}

/// <summary>The three reads against a real store (items 6, 15 and 16).</summary>
[Collection("live-postgres")]
public sealed class McpToolAudit5633LiveTests
{
    private const string Db = "Audit5633";

    [Fact]
    public Task DatabaseSizes_PercentGrowthFile_ComesBackWithItsPercentStep() =>
        WithStoreAsync("audit5633-sizes", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await Exec(connection, ct, $@"INSERT INTO database_size_stats
                (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name,
                 physical_name, total_size_mb, used_size_mb, auto_growth_mb, is_percent_growth, growth_pct)
                VALUES ({CollectionIdGenerator.Next()}, '{Ts(now)}', {serverId}, '{serverName}', '{Db}', 7, 1, 'ROWS', 'pct', 'D:\a.mdf', 1000, 500, 100, TRUE, 10),
                       ({CollectionIdGenerator.Next()}, '{Ts(now)}', {serverId}, '{serverName}', '{Db}', 7, 2, 'LOG', 'mb', 'D:\a.ldf', 1000, 500, 64, FALSE, NULL)");

            var rows = await DarlingObjectStatsReader.GetLatestDatabaseSizesAsync(postgres, serverId, ct);
            var pct = rows.Single(r => r.FileName == "pct");
            var mb = rows.Single(r => r.FileName == "mb");
            Assert.True(pct.IsPercentGrowth);
            Assert.Equal(10, pct.GrowthPct);
            Assert.False(mb.IsPercentGrowth);
            Assert.Null(mb.GrowthPct);
        });

    [Fact]
    public Task TableSizes_UnsizedTableSortsLast_ReadsNull_AndTempTablesAreLeftOut() =>
        WithStoreAsync("audit5633-tables", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await PlantAsync(connection, ct, serverId, serverName, now, "Big", 1, "CLUSTERED", "500", 0, 0);
            await PlantAsync(connection, ct, serverId, serverName, now, "Small", 1, "CLUSTERED", "5", 0, 0);
            await PlantAsync(connection, ct, serverId, serverName, now, "Unsized", 1, "CLUSTERED", "NULL", 0, 0);
            await PlantAsync(connection, ct, serverId, serverName, now, "#usage_______0000001", 1, "CLUSTERED", "900", 0, 0);

            var rows = await DarlingObjectStatsReader.GetObjectSizeGrowthAsync(
                postgres, serverId, now.AddDays(-7), now.AddDays(-30), 50, DatabaseFilter.One(Db), ct);

            Assert.Equal(["Big", "Small", "Unsized"], rows.Select(r => r.TableName).ToArray());
            Assert.Null(rows[2].CurrentReservedMb);
            Assert.Equal(500, rows[0].CurrentReservedMb);

            /* The tool: the unsized table's reserved_mb is null in the payload, not 0. */
            var root = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, database_name: Db, cancellationToken: ct)).RootElement;
            var tables = root.GetProperty("tables").EnumerateArray().ToList();
            Assert.DoesNotContain(tables, t => t.GetProperty("table_name").GetString()!.StartsWith('#'));
            Assert.Equal(JsonValueKind.Null, tables.Single(t => t.GetProperty("table_name").GetString() == "Unsized").GetProperty("reserved_mb").ValueKind);
        });

    [Fact]
    public Task IndexUsage_HeapsAndClusteredIndexes_AreNeverUnused() =>
        WithStoreAsync("audit5633-usage", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await PlantAsync(connection, ct, serverId, serverName, now, "TNcUnused", 2, "NONCLUSTERED", "10", 0, 0, "IX_unused");
            await PlantAsync(connection, ct, serverId, serverName, now, "TNcWrite", 2, "NONCLUSTERED", "10", 0, 5, "IX_write");
            await PlantAsync(connection, ct, serverId, serverName, now, "TNcActive", 2, "NONCLUSTERED", "10", 9, 5, "IX_active");
            await PlantAsync(connection, ct, serverId, serverName, now, "TCx", 1, "CLUSTERED", "900", 0, 0, "PK_cx");
            await PlantAsync(connection, ct, serverId, serverName, now, "TCxWrites", 1, "CLUSTERED", "800", 0, 7, "PK_cxw");
            await PlantAsync(connection, ct, serverId, serverName, now, "TCxRead", 1, "CLUSTERED", "700", 3, 7, "PK_cxr");
            await PlantAsync(connection, ct, serverId, serverName, now, "THeap", 0, "HEAP", "600", 0, 0, null);

            var rows = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, serverId, 50, DatabaseFilter.One(Db), ct);
            string Class(string table) => rows.Single(r => r.TableName == table).Classification;

            Assert.Equal("Unused", Class("TNcUnused"));
            Assert.Equal("Write-only", Class("TNcWrite"));
            Assert.Equal("Active", Class("TNcActive"));
            Assert.Equal("Table not read", Class("TCx"));
            Assert.Equal("Table not read", Class("TCxWrites"));
            Assert.Equal("Active", Class("TCxRead"));
            Assert.Equal("Table not read", Class("THeap"));

            /* The drop candidates (nonclustered, no reads) still come first, ahead of the larger table storage. */
            Assert.Equal(["TNcUnused", "TNcWrite"], rows.Take(2).Select(r => r.TableName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        });

    private static string Ts(DateTime t) => t.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

    private static Task Exec(NpgsqlConnection connection, CancellationToken ct, string sql) => DarlingMcpTestData.ExecAsync(connection, ct, sql);

    private static Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture,
        string table, int indexId, string type, string reservedMbLiteral, long reads, long updates, string? indexName = "IX_x") =>
        Exec(connection, ct,
            $@"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                  row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms, user_seeks, user_scans, user_lookups, user_updates)
              VALUES ({CollectionIdGenerator.Next()}, '{Ts(capture)}', {serverId}, '{serverName}', '{Db}', 'dbo', {1000 + (table.Length * 37 + table[0])}, '{table}', {indexId},
                      {(indexName is null ? "NULL" : "'" + indexName + "'")}, '{type}', {reservedMbLiteral}, {reservedMbLiteral}, 1000, 0, 0, 0, 0, 0, 0, 0, {reads}, 0, 0, {updates})");

    private static async Task WithStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live #5633 object-stats tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = $"DELETE FROM index_object_stats WHERE server_id = {serverId}; DELETE FROM database_size_stats WHERE server_id = {serverId}; DELETE FROM servers WHERE server_id = {serverId}";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            await body(connection, postgres, serverId, serverName, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}
