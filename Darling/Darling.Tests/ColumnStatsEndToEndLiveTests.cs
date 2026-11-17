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
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8798, the column_stats run against a real SQL Server and a real store: a scratch database with a clustered table, a nonunique
/// nonclustered index, a unique index, an auto-created statistic, a user statistic on a column named <c>a,b</c>, a filtered statistic
/// whose filter holds a literal, and (when the edition allows) a memory-optimized table. Gated on <c>DARLING_TEST_SQL</c> and
/// <c>DARLING_TEST_PG</c>; <c>DARLING_TEST_SQL_READONLY=1</c> skips it because it creates databases, a login and an event session. It
/// shares a collection with the other live schema classes because the schema pass's session is server-wide.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
[Collection("schema-xe-live-sql")]
public sealed class ColumnStatsEndToEndLiveTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) and DARLING_TEST_PG to run the live column_stats test; DARLING_TEST_SQL_READONLY=1 skips it.";

    private const string Secret = "secret-literal-8798";
    private const string ServerName = "colstats-e2e";

    private static readonly string Setup = $"""
CREATE TABLE dbo.Orders (
    Id int NOT NULL CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED,
    CustomerId int NOT NULL,
    OrderType int NOT NULL,
    Code varchar(20) NOT NULL,
    Note varchar(50) NULL,
    Region varchar(10) NULL,
    Status int NOT NULL,
    [a,b] int NULL);
INSERT dbo.Orders (Id, CustomerId, OrderType, Code, Note, Region, Status, [a,b])
SELECT n, n % 10, (n / 10) % 3, 'C' + CONVERT(varchar(10), n), CASE WHEN n % 7 = 0 THEN '{Secret}' ELSE 'plain' END, 'r' + CONVERT(varchar(3), n % 5), n % 4, n % 25
FROM (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b) AS x;
/* The clustered key's statistic is made with the table, empty, and has no date until it is updated. */
UPDATE STATISTICS dbo.Orders PK_Orders WITH FULLSCAN;
CREATE INDEX IX_Orders_Customer_Type ON dbo.Orders (CustomerId, OrderType);
CREATE UNIQUE INDEX UX_Orders_Code ON dbo.Orders (Code);
CREATE STATISTICS ST_Orders_Ab ON dbo.Orders ([a,b], Status) WITH FULLSCAN;
CREATE STATISTICS ST_Orders_Filtered ON dbo.Orders (Region) WHERE Note = '{Secret}' WITH FULLSCAN;
SELECT COUNT(*) FROM dbo.Orders WHERE Status = 2;
/* A columnstore index's statistic is never read (index types 3 to 6 are left out and counted). */
CREATE TABLE dbo.Facts (Id int NOT NULL CONSTRAINT PK_Facts PRIMARY KEY CLUSTERED, Amount int NOT NULL);
INSERT dbo.Facts (Id, Amount) SELECT Id, Id % 13 FROM dbo.Orders;
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Facts ON dbo.Facts (Amount);
""";

    private static string? SqlConnectionString(string database = "master", string? user = null, string? password = null, string? host = null)
    {
        host ??= Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1")
        {
            return null;
        }

        user ??= Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        password ??= Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "Darling.Tests column stats",
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = password ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> SqlScalarAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        return await command.ExecuteScalarAsync(ct);
    }

    private sealed record Rig(
        DarlingCollectorRunner Runner, ServerRuntime Server, NpgsqlConnection Pg, NpgsqlDataSource Store, string Master, string Database, bool MemoryOptimized, string? Host, CancellationToken Ct)
    {
        public string Db => SqlConnectionString(Database, host: Host)!;

        public Task<string> ScalarAsync(string sql) => ActionProposalStoreLiveTests.ScalarAsync(Pg, sql, Ct);

        public Task<CollectorRunResult> SchemaPassAsync() => DarlingSchemaCapture.RunAsync(Runner, Server, Ct);

        public Task<CollectorRunResult> StatsAsync() => DarlingColumnStatsCapture.RunAsync(Runner, Server, Ct);

        public async Task<CollectorRunResult> StatsRunNowAsync()
        {
            using (DarlingSnapshotScope.Enter())
            {
                return await DarlingColumnStatsCapture.RunAsync(Runner, Server, Ct);
            }
        }

        public async Task<JsonElement> DefinitionAsync(string name)
        {
            var json = await DarlingMcpSchemaTools.ReadObjectDefinition(Store, ServerName, Database, name, null, 0, 200, Ct);
            Assert.False(McpHelpers.IsErrorEnvelope(json), json);
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.Clone();
        }

        /* The newest run row of the database, as "kind|checked|read|deleted|errors|refused|complete". */
        public Task<string> LastRunAsync() => ScalarAsync(
            "SELECT kind || '|' || checked || '|' || read || '|' || deleted || '|' || errors || '|' || coalesce(refused, '-') || '|' || complete::text "
            + $"FROM collect.column_stats_runs WHERE database_name = '{Database}' ORDER BY collection_time DESC, collection_id DESC LIMIT 1");

        public Task<string> StatCountAsync(string where) =>
            ScalarAsync($"SELECT count(DISTINCT (object_id, stats_id)) FROM collect.column_stats WHERE database_name = '{Database}' AND {where}");
    }

    private static async Task<bool> TryMakeMemoryOptimizedTableAsync(string master, string database, string? host, CancellationToken ct)
    {
        try
        {
            if (Convert.ToInt32(await SqlScalarAsync(master, "SELECT CONVERT(int, ISNULL(SERVERPROPERTY('IsXTPSupported'), 0))", ct), System.Globalization.CultureInfo.InvariantCulture) != 1)
            {
                return false;
            }

            var db = SqlConnectionString(database, host: host)!;
            await ExecuteAsync(db, "ALTER DATABASE CURRENT ADD FILEGROUP fg_mo CONTAINS MEMORY_OPTIMIZED_DATA;", ct);
            await ExecuteAsync(
                db,
                "DECLARE @dir nvarchar(512) = CONVERT(nvarchar(512), SERVERPROPERTY('InstanceDefaultDataPath')); "
                + "DECLARE @sql nvarchar(max) = N'ALTER DATABASE CURRENT ADD FILE (NAME = N''mo1'', FILENAME = N''' + REPLACE(@dir, N'''', N'''''') + DB_NAME() + N'_mo'') TO FILEGROUP fg_mo;'; EXEC (@sql);",
                ct);
            await ExecuteAsync(
                db,
                "CREATE TABLE dbo.MemTable (Id int NOT NULL PRIMARY KEY NONCLUSTERED, V int NOT NULL) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY); "
                + "INSERT dbo.MemTable (Id, V) VALUES (1, 1), (2, 1), (3, 2);",
                ct);
            return true;
        }
        catch (SqlException)
        {
            return false;
        }
    }

    private static async Task RunAsync(Func<Rig, Task> body, bool withMemoryOptimized = false, bool schemaPass = true, string? host = null)
    {
        var master = SqlConnectionString(host: host);
        var storeConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(master is null || string.IsNullOrEmpty(storeConnection), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var installId = Guid.NewGuid().ToString("N")[..8];
        var sessionName = SchemaChangesXeSession.NameFor(installId);
        var database = "ColStats_" + Guid.NewGuid().ToString("N")[..10];
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnection!, ct);
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var pg = new NpgsqlConnection(scratch.ConnectionString);
        await pg.OpenAsync(ct);
        await PgMigrations.MigrateAsync(pg, ct);

        await ExecuteAsync(master!, "CREATE DATABASE [" + database + "];", ct);
        try
        {
            await ExecuteAsync(SqlConnectionString(database, host: host)!, Setup, ct);
            var memoryOptimized = withMemoryOptimized && await TryMakeMemoryOptimizedTableAsync(master!, database, host, ct);
            Assert.SkipWhen(withMemoryOptimized && !memoryOptimized, "This SQL Server edition cannot hold a memory-optimized table here (no In-Memory OLTP, or no MEMORY_OPTIMIZED_DATA file could be added); the memory-optimized part is skipped.");

            var config = new MonitoredServer { Name = ServerName, Host = ServerName };
            var server = new ServerRuntime
            {
                Config = config,
                ConnectionString = master!,
                Target = new CollectorTargetInfo(),
                StorageName = config.StorageName,
                ServerId = config.ServerId,
            };
            var runner = new DarlingCollectorRunner(
                store,
                new CollectorDeltaCalculator(),
                databaseScope: (_, _) => new List<string> { database },
                separatelyMonitoredDatabases: _ => new List<string>(),
                installId: () => installId,
                collectorEnabled: (_, _) => true);

            await DarlingMcpTestData.RegisterServerAsync(pg, server.ServerId, ServerName, ct);
            foreach (var collector in new[] { "schema_objects", "column_stats" })
            {
                await DarlingMcpTestData.ExecAsync(
                    pg, ct, "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ($1, $2, true)", server.ServerId, collector);
            }

            var rig = new Rig(runner, server, pg, store, master!, database, memoryOptimized, host, ct);
            if (schemaPass)
            {
                await rig.SchemaPassAsync();
            }

            await body(rig);
            bodySucceeded = true;
        }
        finally
        {
            await ExecuteAsync(master!, "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'" + database + "') BEGIN ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "]; END;", CancellationToken.None);
            await ExecuteAsync(master!, SchemaChangesXeSession.BuildDropSql(sessionName, databaseScoped: false), CancellationToken.None);
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static JsonElement Column(JsonElement definition, string name) =>
        definition.GetProperty("columns").EnumerateArray().Single(c => c.GetProperty("name").GetString() == name);

    [Fact]
    public async Task EndToEnd_SelectivityAppearsInObjectDefinition()
    {
        await RunAsync(async r =>
        {
            var result = await r.StatsAsync();
            Assert.DoesNotContain("precondition", result.HostNote ?? string.Empty, StringComparison.Ordinal);

            var definition = await r.DefinitionAsync("dbo.Orders");
            Assert.Equal("current", definition.GetProperty("selectivity_status").GetString());

            var id = Column(definition, "Id").GetProperty("selectivity");
            Assert.Equal("unique", id.GetProperty("label").GetString());
            Assert.Equal("PK_Orders", id.GetProperty("statistic").GetString());
            Assert.InRange(id.GetProperty("selectivity").GetDouble(), 0.99, 1.0);

            var code = Column(definition, "Code").GetProperty("selectivity");
            Assert.Equal("unique", code.GetProperty("label").GetString());
            Assert.Equal("UX_Orders_Code", code.GetProperty("statistic").GetString());

            /* Ten teams over 1000 rows, from a full-scan index statistic. */
            var team = Column(definition, "CustomerId").GetProperty("selectivity");
            Assert.InRange(team.GetProperty("distinct_estimate").GetDouble(), 9.5, 10.5);
            Assert.InRange(team.GetProperty("rows_per_value").GetDouble(), 95, 105);
            Assert.InRange(team.GetProperty("selectivity").GetDouble(), 0.0095, 0.0105);
            Assert.Equal(100.0, team.GetProperty("sampled_percent").GetDouble(), 1);

            /* The auto-created statistic of an unindexed column the setup queried. */
            var status = Column(definition, "Status").GetProperty("selectivity");
            Assert.StartsWith("_WA_Sys_", status.GetProperty("statistic").GetString(), StringComparison.Ordinal);
            Assert.InRange(status.GetProperty("distinct_estimate").GetDouble(), 3.5, 4.5);

            /* A filtered statistic is never read: Region has no statistic of its own. */
            var region = Column(definition, "Region");
            Assert.Equal(JsonValueKind.Null, region.GetProperty("selectivity").ValueKind);
            Assert.Equal("no statistic leads with this column", region.GetProperty("selectivity_note").GetString());

            /* The user statistic on a column named a,b: its prefix is named by position, not by splitting the DBCC text. */
            var ab = Column(definition, "a,b").GetProperty("selectivity");
            Assert.Equal("ST_Orders_Ab", ab.GetProperty("statistic").GetString());
            var combinations = definition.GetProperty("column_combinations").EnumerateArray().ToList();
            Assert.Contains(combinations, c => c.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).SequenceEqual(new[] { "a,b", "Status" }));
            Assert.Contains(combinations, c => c.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).SequenceEqual(new[] { "CustomerId", "OrderType" }));
        });
    }

    [Fact]
    public async Task ClusteringKeyCombination_IsStoredAndMarked()
    {
        await RunAsync(async r =>
        {
            await r.StatsAsync();

            Assert.Equal("{CustomerId,OrderType,Id}", await r.ScalarAsync(
                $"SELECT columns::text FROM collect.column_stats WHERE database_name = '{r.Database}' AND stats_name = 'IX_Orders_Customer_Type' AND prefix_length = 3 AND includes_clustering_key"));
            Assert.Equal("0", await r.ScalarAsync(
                $"SELECT count(*) FROM collect.column_stats WHERE database_name = '{r.Database}' AND stats_name = 'IX_Orders_Customer_Type' AND prefix_length < 3 AND includes_clustering_key"));

            var definition = await r.DefinitionAsync("dbo.Orders");
            var marked = definition.GetProperty("column_combinations").EnumerateArray()
                .Single(c => c.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).SequenceEqual(new[] { "CustomerId", "OrderType", "Id" }));
            Assert.True(marked.GetProperty("includes_clustering_key").GetBoolean());
        });
    }

    [Fact]
    public async Task FilteredStatisticText_IsNowhereInTheStore()
    {
        await RunAsync(async r =>
        {
            await r.StatsRunNowAsync();
            Assert.NotEqual("0", await r.ScalarAsync($"SELECT count(*) FROM collect.column_stats WHERE database_name = '{r.Database}'"));
            Assert.Equal("1", await r.ScalarAsync($"SELECT count(*) FROM collect.column_stats_runs WHERE database_name = '{r.Database}' AND skipped_filtered = 1"));

            /* Every text column of the three tables, whole rows rendered as text. */
            foreach (var table in new[] { "column_stats", "column_stats_runs", "column_stats_state" })
            {
                Assert.Equal("0", await r.ScalarAsync($"SELECT count(*) FROM collect.{table} AS t WHERE t::text ILIKE '%{Secret}%'"));
            }

            /* The answer a client reads carries it nowhere either. */
            using var doc = JsonDocument.Parse((await r.DefinitionAsync("dbo.Orders")).GetRawText());
            Assert.DoesNotContain(Secret, doc.RootElement.GetRawText(), StringComparison.Ordinal);
            Assert.Equal("ST_Orders_Filtered", await SqlScalarAsync(r.Db, "SELECT name FROM sys.stats WHERE has_filter = 1", r.Ct) as string);
        });
    }

    [Fact]
    public async Task UpdateStatistics_OnlyThatStatisticIsReRead()
    {
        await RunAsync(async r =>
        {
            await r.StatsAsync();
            var all = await r.StatCountAsync("true");
            Assert.NotEqual("0", all);

            /* Nothing changed: nothing is read. */
            await r.StatsAsync();
            Assert.StartsWith("incremental|", await r.LastRunAsync(), StringComparison.Ordinal);
            Assert.Equal("incremental|" + all + "|0|0|0|-|true", await r.LastRunAsync());

            await Task.Delay(1500, r.Ct);
            await ExecuteAsync(r.Db, "UPDATE STATISTICS dbo.Orders IX_Orders_Customer_Type WITH FULLSCAN;", r.Ct);
            await r.StatsAsync();

            Assert.Equal("incremental|" + all + "|1|0|0|-|true", await r.LastRunAsync());
            Assert.Equal(all, await r.StatCountAsync("true"));
        });
    }

    [Fact]
    public async Task RenamedColumn_IsReadAgain_AndTheNewNameAppears()
    {
        await RunAsync(async r =>
        {
            await r.StatsAsync();
            Assert.Equal("1", await r.StatCountAsync("'OrderType' = ANY(columns)"));

            /* sp_rename keeps STATS_DATE; the stored column list no longer matches the catalog, so the statistic is read again. */
            await ExecuteAsync(r.Db, "EXEC sp_rename N'dbo.Orders.OrderType', N'OrderKind', N'COLUMN';", r.Ct);
            await r.StatsAsync();

            Assert.Equal("1", (await r.LastRunAsync()).Split('|')[2]);
            Assert.Equal("0", await r.StatCountAsync("'OrderType' = ANY(columns)"));
            Assert.Equal("{CustomerId,OrderKind}", await r.ScalarAsync(
                $"SELECT columns::text FROM collect.column_stats WHERE database_name = '{r.Database}' AND stats_name = 'IX_Orders_Customer_Type' AND prefix_length = 2"));
        });
    }

    [Fact]
    public async Task DroppedStatistic_RowIsDeleted()
    {
        await RunAsync(async r =>
        {
            await r.StatsAsync();
            Assert.Equal("1", await r.StatCountAsync("stats_name = 'ST_Orders_Ab'"));

            await ExecuteAsync(r.Db, "DROP STATISTICS dbo.Orders.ST_Orders_Ab;", r.Ct);
            await r.StatsAsync();

            Assert.Equal("0", await r.StatCountAsync("stats_name = 'ST_Orders_Ab'"));
            Assert.Equal("0", await r.ScalarAsync($"SELECT count(*) FROM collect.column_stats WHERE database_name = '{r.Database}' AND stats_name = 'ST_Orders_Ab'"));
            Assert.EndsWith("|0|1|0|-|true", await r.LastRunAsync(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ColumnstoreIndexStatistic_IsCountedAndNotRead()
    {
        await RunAsync(async r =>
        {
            await r.StatsRunNowAsync();

            Assert.NotEqual("0", await r.ScalarAsync($"SELECT coalesce(max(skipped_xml_spatial_columnstore), 0) FROM collect.column_stats_runs WHERE database_name = '{r.Database}'"));
            Assert.Equal("0", await r.StatCountAsync("stats_name = 'NCCI_Facts'"));
            Assert.Equal("0", await r.ScalarAsync($"SELECT count(*) FROM collect.column_stats WHERE database_name = '{r.Database}' AND 'Amount' = ANY(columns)"));
            Assert.Equal("NCCI_Facts", await SqlScalarAsync(r.Db, "SELECT s.name FROM sys.stats AS s JOIN sys.indexes AS i ON i.object_id = s.object_id AND i.index_id = s.stats_id WHERE i.type = 6", r.Ct) as string);
        });
    }

    [Fact]
    public async Task MemoryOptimizedTable_IsCountedAndNotRead()
    {
        await RunAsync(
            async r =>
            {
                await r.StatsAsync();
                Assert.Equal("0", await r.ScalarAsync($"SELECT count(*) FROM collect.column_stats WHERE database_name = '{r.Database}' AND object_name = 'MemTable'"));
                Assert.NotEqual("0", await r.ScalarAsync($"SELECT coalesce(max(skipped_memory_optimized), 0) FROM collect.column_stats_runs WHERE database_name = '{r.Database}'"));
                Assert.NotEqual("0", await r.StatCountAsync("object_name = 'Orders'"));
            },
            withMemoryOptimized: true);
    }

    [Fact]
    public async Task LoginWithoutSelect_GivesPrecondition_ThenDataAfterGrant()
    {
        var master = SqlConnectionString();
        Assert.SkipWhen(master is null || string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER")), SkipMessage + " The login test needs SQL authentication (DARLING_TEST_SQL_USER).");
        var login = "cs8798_" + Guid.NewGuid().ToString("N")[..8];
        var password = "Pw" + Guid.NewGuid().ToString("N")[..16] + "!9a";
        var ct = TestContext.Current.CancellationToken;

        await ExecuteAsync(master!, $"CREATE LOGIN [{login}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF; GRANT VIEW SERVER STATE TO [{login}];", ct);
        try
        {
            await RunAsync(
                async r =>
                {
                    await ExecuteAsync(r.Db, $"CREATE USER [{login}] FOR LOGIN [{login}];", r.Ct);
                    var limited = new ServerRuntime
                    {
                        Config = r.Server.Config,
                        ConnectionString = SqlConnectionString("master", login, password)!,
                        Target = r.Server.Target,
                        StorageName = r.Server.StorageName,
                        ServerId = r.Server.ServerId,
                    };

                    var first = await DarlingColumnStatsCapture.RunAsync(r.Runner, limited, r.Ct);
                    Assert.Contains("precondition", first.HostNote ?? string.Empty, StringComparison.Ordinal);
                    Assert.Contains("GRANT SELECT", first.HostNote ?? string.Empty, StringComparison.Ordinal);
                    Assert.Equal("baseline|0|0|0|0|no_select|false", await r.LastRunAsync());
                    Assert.Equal("0", await r.ScalarAsync($"SELECT count(*) FROM collect.column_stats WHERE database_name = '{r.Database}'"));

                    await ExecuteAsync(r.Db, $"GRANT SELECT TO [{login}];", r.Ct);
                    var second = await DarlingColumnStatsCapture.RunAsync(r.Runner, limited, r.Ct);
                    Assert.DoesNotContain("precondition", second.HostNote ?? string.Empty, StringComparison.Ordinal);
                    Assert.NotEqual("0", await r.StatCountAsync("true"));
                    Assert.EndsWith("|0|-|true", await r.LastRunAsync(), StringComparison.Ordinal);
                },
                schemaPass: false);
        }
        finally
        {
            await ExecuteAsync(master!, $"IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'{login}') DROP LOGIN [{login}];", CancellationToken.None);
        }
    }

    [Fact]
    public async Task CandidateRead_OverASmallCap_CapsBeforeTheNames_AndListsEveryId()
    {
        await RunAsync(async r =>
        {
            /* The candidates the read must find: every statistic of a user table or indexed view, unfiltered, not columnstore, updated. */
            var expected = Convert.ToInt32(await SqlScalarAsync(r.Db, """
SELECT COUNT(*)
FROM sys.stats AS s
JOIN sys.objects AS o ON o.object_id = s.object_id
LEFT JOIN sys.indexes AS i ON i.object_id = s.object_id AND i.index_id = s.stats_id
WHERE o.type IN ('U', 'V') AND o.is_ms_shipped = 0 AND s.has_filter = 0 AND ISNULL(i.type, 0) NOT IN (3, 4, 5, 6)
  AND STATS_DATE(s.object_id, s.stats_id) IS NOT NULL
""", r.Ct), System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(expected > 3, "The setup holds more than three candidate statistics.");

            await using var connection = new SqlConnection(r.Db);
            await connection.OpenAsync(r.Ct);
            await using var command = new SqlCommand(ColumnStatsQueries.BuildCandidatesSql(2), connection);
            await using var reader = await command.ExecuteReaderAsync(r.Ct);
            var capped = new List<(int ObjectId, int StatsId, long Rows)>();
            while (await reader.ReadAsync(r.Ct))
            {
                capped.Add((reader.GetInt32(0), reader.GetInt32(3), reader.GetInt64(7)));
            }

            Assert.True(await reader.NextResultAsync(r.Ct) && await reader.ReadAsync(r.Ct));
            var overCap = Convert.ToInt32(reader.GetValue(4), System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(await reader.NextResultAsync(r.Ct));
            var all = new List<(int ObjectId, int StatsId)>();
            while (await reader.ReadAsync(r.Ct))
            {
                all.Add((reader.GetInt32(0), reader.GetInt32(1)));
            }

            /* Two rows, largest tables first; the rest counted over the cap; every candidate's id listed beside them. */
            Assert.Equal(2, capped.Count);
            Assert.True(capped[0].Rows >= capped[1].Rows);
            Assert.Equal(expected - 2, overCap);
            Assert.Equal(expected, all.Count);
            Assert.All(capped, c => Assert.Contains((c.ObjectId, c.StatsId), all));

            /* Largest tables first across every candidate, not only within the two: no candidate left out has more rows than the
               smallest one kept. The full list with its row counts comes from the same read under a cap it does not reach. */
            await reader.CloseAsync();
            await using var full = new SqlCommand(ColumnStatsQueries.BuildCandidatesSql(ColumnStatsModel.MaxStatisticsPerDatabase), connection);
            await using var fullReader = await full.ExecuteReaderAsync(r.Ct);
            var everyCandidate = new List<(int ObjectId, int StatsId, long Rows)>();
            while (await fullReader.ReadAsync(r.Ct))
            {
                everyCandidate.Add((fullReader.GetInt32(0), fullReader.GetInt32(3), fullReader.GetInt64(7)));
            }

            Assert.Equal(expected, everyCandidate.Count);
            var smallestKept = capped.Min(c => c.Rows);
            Assert.All(
                everyCandidate.Where(c => !capped.Any(k => k.ObjectId == c.ObjectId && k.StatsId == c.StatsId)),
                c => Assert.True(c.Rows <= smallestKept, $"Statistic {c.ObjectId}/{c.StatsId} ({c.Rows} rows) was left out while one of {smallestKept} rows was kept."));
        });
    }

    [Fact]
    public async Task CandidateRead_UnderTheCap_ListsNoIds()
    {
        await RunAsync(
            async r =>
            {
                var source = new SqlColumnStatsSource(r.Runner);
                await using var connection = await source.OpenAsync(r.Server, null, r.Ct);
                var (candidates, skipped, allIds) = await connection.ReadCandidatesAsync(r.Database, r.Ct);

                Assert.NotEmpty(candidates);
                Assert.Equal(0, skipped.OverCap);
                Assert.Empty(allIds);
            },
            schemaPass: false);
    }

    [Fact]
    public async Task ReadBatch_TableUnderASchemaLock_CostsOneLockWait_ForAllItsStatistics()
    {
        await RunAsync(
            async r =>
            {
                var ids = new List<(int ObjectId, int StatsId)>();
                await using (var catalog = new SqlConnection(r.Db))
                {
                    await catalog.OpenAsync(r.Ct);
                    await using var list = new SqlCommand(
                        "SELECT s.object_id, s.stats_id FROM sys.stats AS s WHERE s.object_id = OBJECT_ID(N'dbo.Orders') AND s.has_filter = 0 ORDER BY s.stats_id", catalog);
                    await using var reader = await list.ExecuteReaderAsync(r.Ct);
                    while (await reader.ReadAsync(r.Ct))
                    {
                        ids.Add((reader.GetInt32(0), reader.GetInt32(1)));
                    }
                }

                Assert.True(ids.Count >= 3, "dbo.Orders holds at least three unfiltered statistics.");

                /* An open ALTER TABLE holds a schema modification lock on the table until it commits. */
                await using var holder = new SqlConnection(r.Db);
                await holder.OpenAsync(r.Ct);
                await using var transaction = (SqlTransaction)await holder.BeginTransactionAsync(r.Ct);
                await using (var ddl = new SqlCommand("ALTER TABLE dbo.Orders ADD Extra int NULL;", holder, transaction))
                {
                    await ddl.ExecuteNonQueryAsync(r.Ct);
                }

                try
                {
                    var source = new SqlColumnStatsSource(r.Runner);
                    await using var connection = await source.OpenAsync(r.Server, null, r.Ct);
                    var clock = System.Diagnostics.Stopwatch.StartNew();
                    var (rows, errors) = await connection.ReadStatisticsAsync(r.Database, ids, headerHasPersistedSamplePercent: true, r.Ct);
                    clock.Stop();

                    Assert.Empty(rows);
                    Assert.Equal(ids.Count, errors.Count);
                    Assert.All(errors, e => Assert.Equal(1222, e.Number));
                    /* LOCK_TIMEOUT is 5 seconds: one wait for the table (about 5 s), not one per statistic (about 25 s). The upper bound
                       leaves room for a loaded box. */
                    Assert.InRange(clock.Elapsed.TotalSeconds, 4.5, 15.0);
                }
                finally
                {
                    await transaction.RollbackAsync(CancellationToken.None);
                }
            },
            schemaPass: false);
    }

    /// <summary>
    /// A server with <c>MEMORY_OPTIMIZED TEMPDB_METADATA = ON</c> raises error 41317 on set-based reads of the statistics DMVs; the per-statistic
    /// DBCC read must not. Skipped unless <c>DARLING_TEST_SQL_MEMOPT_TEMPDB</c> names such a server (host[,port]); the credentials are the
    /// ones of the other live tests.
    /// </summary>
    [Fact]
    public async Task MemoryOptimizedTempdbMetadata_NoError41317()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_MEMOPT_TEMPDB");
        Assert.SkipWhen(string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1", "Set DARLING_TEST_SQL_MEMOPT_TEMPDB to a server with MEMORY_OPTIMIZED TEMPDB_METADATA = ON (and DARLING_TEST_PG) to run this test.");
        var master = SqlConnectionString(host: host)!;
        Assert.Equal(1, Convert.ToInt32(await SqlScalarAsync(master, "SELECT CONVERT(int, SERVERPROPERTY('IsTempdbMetadataMemoryOptimized'))", TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture));
        await RunAsync(
            async r =>
            {
                var result = await r.StatsAsync();
                Assert.DoesNotContain("41317", result.HostNote ?? string.Empty, StringComparison.Ordinal);
                Assert.Equal("0", await r.ScalarAsync($"SELECT coalesce(max(errors), 0) FROM collect.column_stats_runs WHERE database_name = '{r.Database}'"));
                Assert.NotEqual("0", await r.StatCountAsync("true"));
            },
            host: host);
    }
}
