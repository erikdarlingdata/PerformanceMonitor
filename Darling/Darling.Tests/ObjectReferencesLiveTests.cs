/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #8784 part 2: view usage from Query Store statement text. The V297 rung (the dependency list table and the per-day referenced-names
/// summary) and its probe arm; the summary's build from the daily Query Store summary and the stored text (parsed names, a CTE left out, an
/// unparsed statement searched for the object list's views, a statement with no text counted, a rebuilt day only adding, expired days
/// deleted); the reads (a view read from another database by a three-part name counts for its own database; the dependency list's traps
/// left out); the retention purge of the dependency list and of the object list, which keep each database's newest list (#9706); and
/// <c>get_object_usage</c> end to end: a view a statement names, one used only through it, and one nothing reaches.
/// </summary>
public sealed class ObjectReferencesLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8784 view usage tests.";
    private const string Server = "objrefs-sql1";
    private const string Db = "Sales";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(Server);

    // ── The rung ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheRungIsV297_AndTheProbeMapsItToV297()
    {
        var rung = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version);
        Assert.Equal(297, rung.Version);
        Assert.Equal("object-references", rung.Name);
        Assert.Contains("collect.object_dependencies", rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectReferencesDaily.Table, rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectReferencesDaily.BuiltTable, rung.Sql, StringComparison.Ordinal);
        Assert.Contains(ObjectReferencesDaily.DatabaseDaysTable, rung.Sql, StringComparison.Ordinal);
        Assert.Contains("to_regclass('collect.object_dependencies')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        /* The arm is found by its name, not by its place from the end: a newer rung adds a parameter after it. */
        var armIndex = Array.FindIndex(parameters, p => p.Name == "hasObjectReferences");
        Assert.True(armIndex >= 0, "the probe has no hasObjectReferences parameter");
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
        /* Arms are newest first, so turning off this rung and every rung after it leaves a store at the rung before. */
        for (var i = armIndex; i < all.Length; i++)
        {
            all[i] = false;
        }
        Assert.Equal(rung.Version - 1, (int)method.Invoke(null, all)!);
    }

    [Fact]
    public void TheDependencyListTable_IsExactlyTheSchemaGeneratorsOutput()
    {
        var rung = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var table = PgSchemaGenerator.CreateTable(ObjectDependenciesCollector.Instance)
            .Replace("EXISTS object_dependencies", "EXISTS collect.object_dependencies", StringComparison.Ordinal);
        var index = PgSchemaGenerator.CreateIndex(ObjectDependenciesCollector.Instance)!
            .Replace("ON object_dependencies", "ON collect.object_dependencies", StringComparison.Ordinal);
        Assert.Contains(table, rung, StringComparison.Ordinal);
        Assert.Contains(index, rung, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRungRunsTwice()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var sql = PgMigrations.Scripts.Single(r => r.Version == ObjectReferencesDaily.Version).Sql;
        await ExecuteAsync(connection, sql, ct);
        await ExecuteAsync(connection, sql, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table}", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.object_dependencies", ct));
    }

    // ── The summary ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TheSummary_ParsesTheDaysStatements_SearchesUnparsedText_OnlyAdds_AndDropsExpiredDays()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = new DateTime(2026, 11, 14, 12, 0, 0);
        var day = new DateOnly(2026, 11, 10);
        var expired = new DateOnly(2026, 5, 1);

        await PlantStatementAsync(connection, day, Db, 1, 40, "(@p1 int)SELECT a FROM dbo.vOrders WHERE id = @p1", now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, Db, 2, 2, "WITH vCte AS (SELECT 1 AS a) SELECT a FROM vCte JOIN dbo.vOrders ON 1 = 1", now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, Db, 3, 5, "SELEC broken FROM dbo.vLegacy", now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, Db, 4, 9, text: null, now.AddDays(-4), ct);
        await PlantStatementAsync(connection, day, "Reports", 5, 7, "SELECT * FROM Sales.dbo.vCrossDb", now.AddDays(-4), ct);
        await PlantBuiltAsync(connection, day, now.AddDays(-3), ct);
        await PlantInventoryAsync(connection, Db, "vLegacy", "VIEW", now.AddDays(-1), ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Old', '', 'dbo', 'vGone', false, DATE '{expired:yyyy-MM-dd}', 1, 1, NULL, false)", ct);

        var first = await ObjectReferencesDaily.RunTickAsync(dataSource, now, NullLogger.Instance, ct);
        Assert.Equal(1, first.Built);
        Assert.Equal(0, first.Failed);

        Assert.Equal(42L, await ScalarAsync<long>(connection, RefCount("vOrders"), ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE lower(referenced_name) = 'vcte'", ct));
        Assert.Equal(5L, await ScalarAsync<long>(connection, RefCount("vLegacy"), ct));
        Assert.True(await ScalarAsync<bool>(connection, $"SELECT text_matched FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vlegacy'", ct));
        /* The key is folded once, by the aggregator (#8784 review), so two builds that meet two spellings key one row; the spelling as seen
           is kept beside it for display. */
        Assert.Equal("sales", await ScalarAsync<string>(connection,
            $"SELECT referenced_database FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vcrossdb'", ct));
        Assert.Equal("vOrders", await ScalarAsync<string>(connection,
            $"SELECT referenced_name_as_seen FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vorders'", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE lower(referenced_name) = 'vgone'", ct));

        Assert.Equal(4L, await ScalarAsync<long>(connection,
            $"SELECT statements FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT statements_without_text FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT statements_unparsed FROM {ObjectReferencesDaily.DatabaseDaysTable} WHERE database_name = '{Db}'", ct));

        /* Nothing new: the next tick builds nothing. */
        Assert.Equal(0, (await ObjectReferencesDaily.RunTickAsync(dataSource, now.AddHours(1), NullLogger.Instance, ct)).Built);

        /* The day is rebuilt from a source whose text has since been pruned: the build only adds, so the count stays. The rebuild also meets
           the view spelled another way first, and still adds to the one row. */
        await ExecuteAsync(connection, $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId} AND query_id = 1", ct);
        await PlantStatementAsync(connection, day, Db, 0, 1, "SELECT a FROM DBO.VORDERS", now.AddDays(-4), ct);
        await PlantBuiltAsync(connection, day, now.AddDays(-1), ct);
        Assert.Equal(1, (await ObjectReferencesDaily.RunTickAsync(dataSource, now.AddHours(2), NullLogger.Instance, ct)).Built);
        Assert.Equal(42L, await ScalarAsync<long>(connection, RefCount("vOrders"), ct));
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM {ObjectReferencesDaily.Table} WHERE server_id = {ServerId} AND lower(referenced_name) = 'vorders'", ct));

        /* The rebuild's spelling does not replace the first one, and the read hands back the spelling as seen, not the folded key: a view
           guessed from the summary where no object list is kept keeps a name a case-sensitive collation resolves. */
        Assert.Equal("vOrders", await ScalarAsync<string>(connection,
            $"SELECT referenced_name_as_seen FROM {ObjectReferencesDaily.Table} WHERE referenced_name = 'vorders'", ct));
        var read = await DarlingObjectUsageReader.ReadReferencesAsync(
            dataSource, ServerId, Db, now.AddDays(-90), ct);
        Assert.Contains(read.References, r => r.ObjectName == "vOrders");
        Assert.DoesNotContain(read.References, r => r.ObjectName == "vorders");
    }

    // ── The reads and the tool ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetObjectUsage_ReferencesAViewByText_ReachesOneThroughIt_AndCallsTheRestUnused()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Server, ct);

        var now = LiveClock.Now();
        foreach (var view in new[] { "vOuter", "vInner", "vOld", "vCrossDb", "vCustomer", "vBehindSynonym" })
        {
            await PlantInventoryAsync(connection, Db, view, "VIEW", now.AddDays(-1), ct);
        }

        /* Only a stored ALL lets a view reach Not used in window (#8784 review): an unknown capture mode holds it at Not tracked. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, query_capture_mode) VALUES ($1,$2,$3,$4,$5,$6)",
            1L, DarlingMcpTestData.Naive(now.AddDays(-5)), ServerId, Server, Db, "ALL");

        /* Statement text on 85 of the 90 days, with vOuter named on one of them and vCrossDb read from another database. */
        for (var d = 2; d <= 86; d++)
        {
            await ExecuteAsync(connection,
                $"INSERT INTO {ObjectReferencesDaily.DatabaseDaysTable} VALUES ({ServerId}, '{Db}', DATE '{now.AddDays(-d):yyyy-MM-dd}', 10, 0, 0, 100)", ct);
        }

        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, '{Db}', '', 'dbo', 'vOuter', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 12, 1, NULL, false)", ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Reports', 'sales', 'dbo', 'vCrossDb', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 4, 1, NULL, false)", ct);
        /* A name another database reads with no database part is that database's object, not this one's. */
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, 'Reports', '', 'dbo', 'vOld', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 4, 1, NULL, false)", ct);

        /* The dependency list: vOuter reads vInner; plus the traps the filter leaves out. */
        await PlantDependencyAsync(connection, "vOuter", "V", null, "dbo", "vInner", now.AddDays(-1), ct);
        await PlantDependencyAsync(connection, "vOuter", "V", null, "sys", "objects", now.AddDays(-1), ct);
        await PlantDependencyAsync(connection, "vOuter", "V", "alias", "col", "value", now.AddDays(-1), ct);
        /* A statement names the synonym synOrders; the collector ships the synonym as a row of its own, type SN. */
        await PlantDependencyAsync(connection, "synOrders", "SN", null, "dbo", "vBehindSynonym", now.AddDays(-1), ct);
        await ExecuteAsync(connection,
            $"INSERT INTO {ObjectReferencesDaily.Table} VALUES ({ServerId}, '{Db}', '', 'dbo', 'synorders', false, DATE '{now.AddDays(-3):yyyy-MM-dd}', 3, 1, NULL, false)", ct);
        /* A reporting database's view reads vCustomer by a three-part name, and nothing in Sales names it (#8784 review). An older capture of
           Reports, and a linked-server row, are not read. */
        await PlantDependencyAsync(connection, "vReport", "V", "SALES", "dbo", "vCustomer", now.AddDays(-1), ct, database: "Reports");
        await PlantDependencyAsync(connection, "vReportOld", "V", "Sales", "dbo", "vOld", now.AddDays(-9), ct, database: "Reports");

        var deps = await DarlingObjectUsageReader.ReadDependenciesAsync(dataSource, ServerId, Db, ct);
        Assert.NotNull(deps);
        Assert.Equal(["vBehindSynonym", "vInner"], deps!.Dependencies.Select(d => d.ReferencedName).Order(StringComparer.Ordinal));

        var inbound = Assert.Single(await DarlingObjectUsageReader.ReadInboundDependenciesAsync(dataSource, ServerId, Db, ct));
        Assert.Equal(("Reports", "vReport", "vCustomer"), (inbound.ReferencingDatabase, inbound.ReferencingName, inbound.ReferencedName));

        var json = await DarlingMcpObjectUsageTools.GetObjectUsage(dataSource, Server, Db, 90, false, 100, 0, now, ct);
        var root = JsonDocument.Parse(json).RootElement;
        JsonElement Obj(string name) => root.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == name);

        Assert.Equal(ObjectUsageReport.Referenced, Obj("vOuter").GetProperty("classification").GetString());
        Assert.Equal(12, Obj("vOuter").GetProperty("referencing_executions").GetInt64());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vInner").GetProperty("classification").GetString());
        Assert.Equal("dbo.vOuter", Obj("vInner").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vCrossDb").GetProperty("classification").GetString());
        Assert.Equal(4, Obj("vCrossDb").GetProperty("referencing_executions").GetInt64());
        Assert.Equal(ObjectUsageReport.NotUsed, Obj("vOld").GetProperty("classification").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vCustomer").GetProperty("classification").GetString());
        Assert.Equal("Reports.dbo.vReport", Obj("vCustomer").GetProperty("used_via").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, Obj("vBehindSynonym").GetProperty("classification").GetString());
        Assert.Equal("dbo.synOrders", Obj("vBehindSynonym").GetProperty("used_via").GetString());

        var views = root.GetProperty("coverage")[0].GetProperty("views");
        Assert.Equal(85, views.GetProperty("days_with_data").GetInt32());
        Assert.Equal(850, views.GetProperty("statements").GetInt64());
    }

    [Fact]
    public async Task TheCrossDatabaseRead_ReadsOnlyEachDatabasesNewestCapture()
    {
        /* #8784 review: a max(collection_time) GROUP BY database_name read every kept row for the server on every call. 40 databases with 10
           daily captures of 30 rows each is 12,000 rows kept and 1,200 in the newest captures; the read touches the newest captures, one seek
           per database name and one per database's newest time, and nothing older. */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var newest = DarlingMcpTestData.Naive(LiveClock.AnchoredEnd());
        await using (var plant = new NpgsqlCommand(
            """
            INSERT INTO collect.object_dependencies (collection_id, collection_time, server_id, server_name, database_name, referencing_schema,
                referencing_name, referencing_type, referenced_server, referenced_database, referenced_schema, referenced_name, is_bound,
                is_caller_dependent, is_ambiguous)
            SELECT 9000000000000 + (db * 1000 + cap) * 100 + r, $1 - make_interval(days => cap), $2, $3, 'Db' || lpad(db::text, 2, '0'), 'dbo',
                   'v' || r, 'V', NULL, CASE WHEN r = 1 THEN 'Sales' END, 'dbo', CASE WHEN r = 1 THEN 'vShared' || cap ELSE 't' || r END, true,
                   false, false
            FROM generate_series(1, 40) AS db, generate_series(0, 9) AS cap, generate_series(1, 30) AS r
            """, connection))
        {
            plant.Parameters.AddWithValue(newest);
            plant.Parameters.AddWithValue(ServerId);
            plant.Parameters.AddWithValue(Server);
            await plant.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE collect.object_dependencies", ct);

        /* Only the newest capture's row of each database (vShared0) comes back, not the nine older ones. */
        var inbound = await DarlingObjectUsageReader.ReadInboundDependenciesAsync(dataSource, ServerId, "Sales", ct);
        Assert.Equal(40, inbound.Count);
        Assert.All(inbound, e => Assert.Equal("vShared0", e.ReferencedName));

        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingObjectUsageReader.InboundDependenciesSql, connection);
        explain.Parameters.AddWithValue(ServerId);
        explain.Parameters.AddWithValue("Sales");
        using var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
        /* And of those captures only the 40 rows that name another database (#8784 review), through the partial index: one seek per database
           name, one per newest time and one per newest capture's cross-database rows. */
        var read = RowsReadFrom(plan.RootElement[0].GetProperty("Plan"), "object_dependencies");
        Assert.True(read <= 40 * 4, $"the cross-database read touched {read:N0} rows of 12,000 kept; the newest captures hold 40 that name another database");
    }

    [Fact]
    public async Task TheSummaryRead_SeeksEachArm_NotEveryDatabasesRows()
    {
        /* #8784 review: one OR over the same-database and cross-database rows could seek only on server_id, so each call read every
           database's rows for the server across the window. 40 other databases with 30 days of 30 names each, one of them a name in Sales,
           plus Sales's own 30 days of 30 names: 36,900 rows kept, 2,100 the read wants (900 of Sales's, 1,200 from the others). */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var today = DateOnly.FromDateTime(LiveClock.AnchoredEnd());
        await using (var plant = new NpgsqlCommand(
            $"""
            INSERT INTO {ObjectReferencesDaily.Table} (server_id, database_name, referenced_database, referenced_schema, referenced_name, is_function,
                day, execution_count, statement_count, last_execution_time, text_matched)
            SELECT $1, 'Db' || lpad(db::text, 2, '0'), CASE WHEN r = 1 THEN 'sales' ELSE '' END, 'dbo', 'v' || r, false, $2 - d, 1, 1, NULL::timestamp, false
            FROM generate_series(1, 40) AS db, generate_series(0, 29) AS d, generate_series(1, 30) AS r
            UNION ALL
            SELECT $1, 'Sales', '', 'dbo', 'w' || r, false, $2 - d, 1, 1, NULL::timestamp, false
            FROM generate_series(0, 29) AS d, generate_series(1, 30) AS r
            """, connection))
        {
            plant.Parameters.AddWithValue(ServerId);
            plant.Parameters.AddWithValue(today);
            await plant.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, $"ANALYZE {ObjectReferencesDaily.Table}", ct);

        var references = await DarlingObjectUsageReader.ReadReferencesAsync(dataSource, ServerId, "Sales", LiveClock.AnchoredEnd().AddDays(-90), ct);
        Assert.Equal(31, references.References.Count);

        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingObjectUsageReader.ReferencesSql, connection);
        explain.Parameters.AddWithValue(ServerId);
        explain.Parameters.AddWithValue("Sales");
        explain.Parameters.AddWithValue(today.AddDays(-89));
        explain.Parameters.AddWithValue("sales");
        using var plan = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
        var read = RowsReadFrom(plan.RootElement[0].GetProperty("Plan"), "object_references_daily");
        Assert.True(read <= 2_100 + 100, $"the summary read touched {read:N0} rows of 36,900 kept; it wants 2,100");
    }

    [Theory]
    [InlineData(DarlingRetention.ObjectDependenciesTable)]
    [InlineData(DarlingRetention.ObjectInventoryTable)]
    public async Task TheRetentionPurge_KeepsEachDatabasesNewestList_AndDropsOnlyADroppedDatabases(string table)
    {
        /* #8784 follow-up, #9706: a view must never read "Not used in window", and a database must never read as having no object list,
           because its capture failed for longer than the retention. The purge keeps each database's newest list however old, and drops a
           list only once the server's newest database_states capture no longer names its database and the list is past the 3-day
           retention (the object list's own retention is 30 days, so the sweep runs both tables at 3). */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Server, ct);

        var now = LiveClock.Now();

        /* Reports's capture stopped 10 days ago, well past the old 3-day cutoff; its 12-day-old capture was replaced by it. */
        await PlantListRowAsync(connection, table, ServerId, Server, "Reports", "vReport", "vCustomer", now.AddDays(-10), ct);
        await PlantListRowAsync(connection, table, ServerId, Server, "Reports", "vReportOld", "vCustomer", now.AddDays(-12), ct);

        /* Gone was dropped: the newest state capture does not name it. Detached is gone from it too, but its list is inside the 3 days. */
        await PlantListRowAsync(connection, table, ServerId, Server, "Gone", "vGone", "vGoneTarget", now.AddDays(-10), ct);
        await PlantListRowAsync(connection, table, ServerId, Server, "Detached", "vDetached", "vDetachedTarget", now.AddDays(-2), ct);

        /* A server with no database_states rows (that collector off, or Azure SQL Database) keeps its lists: that errs toward used. */
        var otherServer = ServerId ^ 1;
        await PlantListRowAsync(connection, table, otherServer, "other-sql", "Old", "vOther", "t", now.AddDays(-10), ct);

        /* An older state capture still names Gone; only the newest counts. */
        foreach (var (at, databases) in new[] { (now.AddHours(-2), new[] { Db, "Reports", "Gone" }), (now.AddHours(-1), new[] { Db, "Reports" }) })
        {
            foreach (var database in databases)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',false)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Server, database);
            }
        }

        await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: false, NullLogger.Instance, ct, retentionDaysFor: ListsAtThreeDays);

        var nameColumn = NameColumn(table);
        async Task<long> Rows(string where) =>
            await ScalarAsync<long>(connection, $"SELECT count(*) FROM {table} WHERE {where}", ct);
        Assert.Equal(1L, await Rows($"server_id = {ServerId} AND database_name = 'Reports' AND {nameColumn} = 'vReport'"));
        Assert.Equal(0L, await Rows($"server_id = {ServerId} AND database_name = 'Reports' AND {nameColumn} = 'vReportOld'"));
        Assert.Equal(0L, await Rows($"server_id = {ServerId} AND database_name = 'Gone'"));
        Assert.Equal(1L, await Rows($"server_id = {ServerId} AND database_name = 'Detached'"));
        Assert.Equal(1L, await Rows($"server_id = {otherServer}"));

        if (table == DarlingRetention.ObjectInventoryTable)
        {
            /* The 10-day-old list is still Reports's newest one, so the object list read returns it. */
            var inventory = await DarlingObjectUsageReader.ReadInventoryAsync(dataSource, ServerId, "Reports", ct);
            Assert.Equal("vReport", Assert.Single(Assert.IsType<ObjectInventory>(inventory).Objects).ObjectName);
            Assert.Null(await DarlingObjectUsageReader.ReadInventoryAsync(dataSource, ServerId, "Gone", ct));
            return;
        }

        /* And the cross-database reference from the 10-day-old list still reaches vCustomer, so it reads as used. */
        var inbound = await DarlingObjectUsageReader.ReadInboundDependenciesAsync(dataSource, ServerId, Db, ct);
        var edge = Assert.Single(inbound, e => e.ReferencedName == "vCustomer");
        Assert.Equal("Reports", edge.ReferencingDatabase);
        Assert.Equal("vReport", edge.ReferencingName);
        Assert.DoesNotContain(inbound, e => e.ReferencedName == "vGoneTarget");
    }

    [Theory]
    [InlineData(DarlingRetention.ObjectDependenciesTable)]
    [InlineData(DarlingRetention.ObjectInventoryTable)]
    public async Task TheRetentionPurge_KeepsEveryListOnAServerWhoseNewestCaptureNamesNoUserDatabase(string table)
    {
        /* #8784 follow-up, #9706: a collection login that loses VIEW ANY DATABASE sees only master, tempdb and the databases it owns, so the
           newest database_states capture names no user database. Every list on that server is past the 3-day cutoff and its database is
           "not named", but dropping them all would be the lost-grant case the purge exists to survive. */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = LiveClock.Now();
        var blind = ServerId ^ 2;
        foreach (var database in new[] { "Sales", "Reports" })
        {
            await PlantListRowAsync(connection, table, blind, "blind-sql", database, "vList", "t", now.AddDays(-10), ct);
        }

        /* The server's newest capture, after both lists: only master (1) and tempdb (2). An older capture that named the user databases does not count. */
        foreach (var (at, databases) in new[]
        {
            (now.AddDays(-12), new[] { (1, "master"), (2, "tempdb"), (5, "Sales"), (6, "Reports") }),
            (now.AddHours(-1), new[] { (1, "master"), (2, "tempdb") }),
        })
        {
            foreach (var (id, name) in databases)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,'blind-sql',$4,$5,'ONLINE',false)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), blind, name, id);
            }
        }

        await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: false, NullLogger.Instance, ct, retentionDaysFor: ListsAtThreeDays);

        Assert.Equal(2L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {table} WHERE server_id = {blind}", ct));
    }

    [Theory]
    [InlineData(DarlingRetention.ObjectDependenciesTable)]
    [InlineData(DarlingRetention.ObjectInventoryTable)]
    public async Task TheRetentionPurge_DrainsEveryExpiredDayOfAReplacedList_AndNeverStopsOnAKeptOne(string table)
    {
        /* #8784 follow-up, #9706: the slice statement deletes one day per execution until a slice deletes nothing. A database whose only list is
           old (kept) must not anchor the oldest slice and stop the drain before another database's older, replaced lists are gone. */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = LiveClock.Now();

        /* Archive's single list is the oldest row in the table and is its newest: kept. Reports has captures 10, 8 and 6 days old:
           the first two are replaced and go (two slices), the 6-day one is the newest and stays. Sales is current. */
        await PlantListRowAsync(connection, table, ServerId, Server, "Archive", "vArchive", "t", now.AddDays(-20), ct);
        foreach (var days in new[] { 10, 8, 6 })
        {
            await PlantListRowAsync(connection, table, ServerId, Server, "Reports", $"vReports{days}", "t", now.AddDays(-days), ct);
        }
        await PlantListRowAsync(connection, table, ServerId, Server, Db, "vSales", "t", now.AddHours(-1), ct);

        /* The newest state capture names all three databases, so the dropped-database step has nothing to remove. */
        foreach (var (id, name) in new[] { (5, Db), (6, "Reports"), (7, "Archive") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,'ONLINE',false)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddHours(-1)), ServerId, Server, name, id);
        }

        await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: false, NullLogger.Instance, ct, retentionDaysFor: ListsAtThreeDays);

        async Task<long> Rows(string name) =>
            await ScalarAsync<long>(connection, $"SELECT count(*) FROM {table} WHERE {NameColumn(table)} = '{name}'", ct);
        Assert.Equal(1L, await Rows("vArchive"));
        Assert.Equal(0L, await Rows("vReports10"));
        Assert.Equal(0L, await Rows("vReports8"));
        Assert.Equal(1L, await Rows("vReports6"));
        Assert.Equal(1L, await Rows("vSales"));
    }

    /// <summary>The rows the plan's scans of <paramref name="relation"/> produced or filtered out, over all their loops.</summary>
    private static double RowsReadFrom(JsonElement node, string relation)
    {
        double rows = 0;
        if (node.TryGetProperty("Relation Name", out var name) && name.GetString() == relation)
        {
            var loops = node.TryGetProperty("Actual Loops", out var l) ? l.GetDouble() : 1;
            var produced = node.TryGetProperty("Actual Rows", out var a) ? a.GetDouble() : 0;
            var removed = node.TryGetProperty("Rows Removed by Filter", out var f) ? f.GetDouble() : 0;
            rows += (produced + removed) * loops;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                rows += RowsReadFrom(child, relation);
            }
        }

        return rows;
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    private static string RefCount(string name) =>
        $"SELECT execution_count::bigint FROM {ObjectReferencesDaily.Table} WHERE server_id = {ServerId} AND referenced_name = '{name.ToLowerInvariant()}'";

    private static async Task PlantStatementAsync(
        NpgsqlConnection connection, DateOnly day, string database, long queryId, long executions, string? text, DateTime lastExecution, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_top_daily (server_id, day, database_name, query_id, plan_id, module_name, interval_rows, execution_count_sum, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n, last_execution_time_max)
VALUES ($1,$2,$3,$4,1,'Adhoc',1,$5,1,1,1,1,1,1,$6)",
            ServerId, day, database, queryId, (decimal)executions, DarlingMcpTestData.Naive(lastExecution));
        if (text is not null)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1,$2,$3,$4,$5)",
                ServerId, database, queryId, text, DarlingMcpTestData.Naive(lastExecution));
        }
    }

    private static Task PlantBuiltAsync(NpgsqlConnection connection, DateOnly day, DateTime builtAt, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES ($1,$2,2,$3,1)
ON CONFLICT (server_id, day) DO UPDATE SET built_at = EXCLUDED.built_at",
            ServerId, day, DarlingMcpTestData.Naive(builtAt));

    private static Task PlantInventoryAsync(NpgsqlConnection connection, string database, string name, string type, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.object_inventory (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_id, type_desc, create_date, modify_date) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,$8,NULL,NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Server, database, name, name.GetHashCode(StringComparison.Ordinal), type);

    /// <summary>The sweep's retention for these tests: both list tables at 3 days (the dependency list's own, the object list's shortened from 30), everything else long.</summary>
    private static int ListsAtThreeDays(string collector) => collector is "object_dependencies" or "object_inventory" ? 3 : 30;

    /// <summary>The column that holds a list row's own name: the referencing object of a dependency row, the object of an inventory row.</summary>
    private static string NameColumn(string table) =>
        table == DarlingRetention.ObjectInventoryTable ? "object_name" : "referencing_name";

    /// <summary>
    /// One row of <paramref name="table"/>'s daily list for a database at <paramref name="at"/>. Each call is its own capture: the row's
    /// collection_time is the capture's, so a database's rows at different times are different captures.
    /// </summary>
    private static Task PlantListRowAsync(
        NpgsqlConnection connection, string table, int serverId, string serverName, string database, string name, string referencedName, DateTime at,
        CancellationToken ct) =>
        table == DarlingRetention.ObjectInventoryTable
            ? DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.object_inventory (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_id, type_desc, create_date, modify_date) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,'VIEW',NULL,NULL)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, database, name, name.GetHashCode(StringComparison.Ordinal))
            : DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.object_dependencies (collection_id, collection_time, server_id, server_name, database_name, referencing_schema, referencing_name, referencing_type, referenced_server, referenced_database, referenced_schema, referenced_name, is_bound, is_caller_dependent, is_ambiguous) VALUES ($1,$2,$3,$4,$5,'dbo',$6,'V',NULL,$7,'dbo',$8,true,false,false)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, database, name, (object)Db, referencedName);

    private static Task PlantDependencyAsync(
        NpgsqlConnection connection, string referencing, string type, string? referencedDatabase, string referencedSchema, string referencedName, DateTime at,
        CancellationToken ct, string database = Db) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.object_dependencies (collection_id, collection_time, server_id, server_name, database_name, referencing_schema, referencing_name, referencing_type, referenced_server, referenced_database, referenced_schema, referenced_name, is_bound, is_caller_dependent, is_ambiguous) VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,NULL,$8,$9,$10,true,false,false)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Server, database, referencing, type, (object?)referencedDatabase ?? DBNull.Value,
            referencedSchema, referencedName);

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync(ct))!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }
}
