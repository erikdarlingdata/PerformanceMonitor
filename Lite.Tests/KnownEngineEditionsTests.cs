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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="KnownEngineEditions"/>: the edition Lite applies the Azure SQL Database master scope by. A live edition
/// wins and is remembered. Without one, the stored edition answers. With neither, the server stays unscoped. The first
/// sweep after a start has no live edition yet, and a failed check or an edit blanks the status, so on the live edition
/// alone a master target counted the blocking and deadlocks of the databases that alert on their own targets.
/// </summary>
[Collection(SeparatelyMonitoredProviderCollection.Name)]
public sealed class KnownEngineEditionsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string AzureHost = "example-sql.database.windows.net";
    private const int AzureSqlDatabase = CollectorEngineCapability.AzureSqlDatabaseEngineEdition;
    private const int Enterprise = 3;
    private const int Blank = CollectorEngineCapability.UnknownEngineEdition;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public KnownEngineEditionsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose()
    {
        AnalysisService.SeparatelyMonitoredDatabasesProvider = null;
        _conn?.Dispose();
    }

    private static List<ServerConnection> AzureFleet(out ServerConnection master)
    {
        master = new ServerConnection { Id = "cfg-master", ServerName = AzureHost, DatabaseName = "master" };
        return new List<ServerConnection>
        {
            master,
            new() { Id = "cfg-gp", ServerName = AzureHost, DatabaseName = "GP" },
            new() { Id = "cfg-hs", ServerName = AzureHost, DatabaseName = "HS" },
        };
    }

    private static Dictionary<int, int> Stored(ServerConnection server, int edition) =>
        new() { [KnownEngineEditions.StorageId(server)] = edition };

    /* ---------------- the edition rule ---------------- */

    [Fact]
    public void FirstSweep_NoLiveEdition_AStoredAzureEdition_ScopesTheMaster()
    {
        var servers = AzureFleet(out var master);
        var editions = new KnownEngineEditions();
        editions.Seed(Stored(master, AzureSqlDatabase));

        Assert.True(editions.IsAzureSqlDatabase(master, Blank));
        Assert.Equal(new[] { "GP", "HS" }, editions.SeparatelyMonitoredDatabases(master, Blank, servers));
    }

    [Fact]
    public void ALiveAzureEdition_StaysInForce_WhenAFailedCheckOrAnEditBlanksTheStatus()
    {
        /* Nothing stored: a master first collected in this run. */
        var servers = AzureFleet(out var master);
        var editions = new KnownEngineEditions();

        Assert.Equal(new[] { "GP", "HS" }, editions.SeparatelyMonitoredDatabases(master, AzureSqlDatabase, servers));
        Assert.Equal(new[] { "GP", "HS" }, editions.SeparatelyMonitoredDatabases(master, Blank, servers));
        Assert.True(editions.IsAzureSqlDatabase(master, Blank));
    }

    [Fact]
    public void ALiveEdition_ReplacesTheStoredOne_AndABlankStatusDoesNotBringTheStoredOneBack()
    {
        /* A registration pointed at another engine since its rows were stored. */
        var servers = AzureFleet(out var master);
        var editions = new KnownEngineEditions();
        editions.Seed(Stored(master, AzureSqlDatabase));

        Assert.Empty(editions.SeparatelyMonitoredDatabases(master, Enterprise, servers));
        Assert.Empty(editions.SeparatelyMonitoredDatabases(master, Blank, servers));
        Assert.False(editions.IsAzureSqlDatabase(master, Blank));
    }

    [Fact]
    public void TheSeed_KeepsAnEditionThatALiveStatusAlreadyReported_AndSkipsAnUnknownStoredOne()
    {
        var servers = AzureFleet(out var master);
        var id = KnownEngineEditions.StorageId(master);
        var gp = servers.Single(s => s.DatabaseName == "GP");
        var editions = new KnownEngineEditions();
        Assert.Equal(Enterprise, editions.Resolve(id, Enterprise));

        editions.Seed(new Dictionary<int, int> { [id] = AzureSqlDatabase, [KnownEngineEditions.StorageId(gp)] = Blank });

        Assert.Equal(Enterprise, editions.Resolve(id, Blank));
        Assert.Equal(Blank, editions.Resolve(KnownEngineEditions.StorageId(gp), Blank));
    }

    /* ---------------- servers the fix must not change ---------------- */

    [Fact]
    public void ASqlServerTarget_ANeverCollectedMaster_AndAnAzureUserDatabaseTarget_StayUnscoped()
    {
        var servers = AzureFleet(out var master);
        var gp = servers.Single(s => s.DatabaseName == "GP");
        /* An on-premises instance with two database targets of its own: Azure SQL Database would scope them. */
        var onPrem = new ServerConnection { Id = "cfg-onprem", ServerName = "sql01", DatabaseName = null };
        var onPremFleet = new List<ServerConnection>
        {
            onPrem,
            new() { Id = "cfg-onprem-a", ServerName = "sql01", DatabaseName = "Sales" },
            new() { Id = "cfg-onprem-b", ServerName = "sql01", DatabaseName = "Orders" },
        };
        var editions = new KnownEngineEditions();
        editions.Seed(new Dictionary<int, int>
        {
            [KnownEngineEditions.StorageId(onPrem)] = Enterprise,
            [KnownEngineEditions.StorageId(gp)] = AzureSqlDatabase,
        });

        Assert.False(editions.IsAzureSqlDatabase(onPrem, Blank));
        Assert.Empty(editions.SeparatelyMonitoredDatabases(onPrem, Blank, onPremFleet));
        Assert.Empty(editions.SeparatelyMonitoredDatabases(onPrem, Enterprise, onPremFleet));
        Assert.False(editions.IsAzureSqlDatabase(master, Blank));
        Assert.Empty(editions.SeparatelyMonitoredDatabases(master, Blank, servers));
        Assert.Empty(editions.SeparatelyMonitoredDatabases(gp, Blank, servers));
    }

    [Fact]
    public void TheList_SkipsDisabledTargets_ReadOnlyIntentTargets_OtherHosts_AndOtherMasters()
    {
        var master = new ServerConnection { Id = "cfg-master", ServerName = AzureHost, DatabaseName = "master" };
        var servers = new List<ServerConnection>
        {
            master,
            new() { Id = "cfg-gp", ServerName = AzureHost, DatabaseName = "GP" },
            new() { Id = "cfg-off", ServerName = AzureHost, DatabaseName = "Off", IsEnabled = false },
            new() { Id = "cfg-replica", ServerName = AzureHost, DatabaseName = "Replica", ReadOnlyIntent = true },
            new() { Id = "cfg-elsewhere", ServerName = "other-sql.database.windows.net", DatabaseName = "Elsewhere" },
            new() { Id = "cfg-blank", ServerName = AzureHost, DatabaseName = "" },
        };

        Assert.Equal(new[] { "GP" }, new KnownEngineEditions().SeparatelyMonitoredDatabases(master, AzureSqlDatabase, servers));
    }

    [Fact]
    public void TheProvider_FindsTheServerByStorageId_AsksItsLiveEditionByConfigId_AndAnswersNullWithNoList()
    {
        var servers = AzureFleet(out var master);
        var gp = servers.Single(s => s.DatabaseName == "GP");
        var editions = new KnownEngineEditions();
        editions.Seed(Stored(master, AzureSqlDatabase));
        var asked = new List<string>();
        int Live(string configId)
        {
            asked.Add(configId);
            return Blank;
        }

        Assert.Equal(new[] { "GP", "HS" }, editions.SeparatelyMonitoredDatabasesOrNull(KnownEngineEditions.StorageId(master), servers, Live));
        Assert.Equal(new[] { "cfg-master" }, asked);
        Assert.Null(editions.SeparatelyMonitoredDatabasesOrNull(KnownEngineEditions.StorageId(gp), servers, Live));
        Assert.Null(editions.SeparatelyMonitoredDatabasesOrNull(KnownEngineEditions.StorageId(master) + 1, servers, Live));
    }

    /* ---------------- the store read and the provider over a real store ---------------- */

    private async Task ExecAsync(string sql, params object?[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= _duckDb.CreateConnection();
        if (_conn.State != System.Data.ConnectionState.Open) await _conn.OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedPropertiesAsync(int serverId, int engineEdition, DateTime collectedUtc) =>
        ExecAsync(
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition) VALUES ($1,$2,$3,'TestServer','Edition','16.0.4150.1','RTM',$4)",
            _nextId--, collectedUtc, serverId, engineEdition);

    [Fact]
    public async Task TheStoredEditions_AreEachServersNewestRow_TheRowTheOneServerReadAnswers()
    {
        var now = DateTime.UtcNow;
        await SeedPropertiesAsync(1, AzureSqlDatabase, now.AddHours(-1));
        await SeedPropertiesAsync(1, Enterprise, now.AddHours(-3));
        await SeedPropertiesAsync(2, Enterprise, now.AddHours(-1));
        await SeedPropertiesAsync(2, AzureSqlDatabase, now.AddHours(-2));
        await SeedPropertiesAsync(77, AzureSqlDatabase, now.AddHours(-2));

        /* Server 77's newest row is archived with no edition, as a row archived before the column existed reads
           through the view. The one-server read answers unknown, so the grouped read must leave 77 out rather
           than answer its older row's edition. */
        var archived = Path.Combine(_duckDb.ArchivePath, "209912_server_properties.parquet");
        Directory.CreateDirectory(_duckDb.ArchivePath);
        await ExecAsync(
            $"COPY (SELECT -9 AS collection_id, TIMESTAMP '{now.AddMinutes(-30):yyyy-MM-dd HH:mm:ss}' AS collection_time, 77 AS server_id, 'TestServer' AS server_name, 'Edition' AS edition, '16.0.4150.1' AS product_version, 'RTM' AS product_level) TO '{archived.Replace('\\', '/')}' (FORMAT PARQUET)");
        try
        {
            /* A view reads an archive pattern only while a file matches it, so build the views again now. */
            await _duckDb.CreateArchiveViewsAsync();

            var data = new LocalDataService(_duckDb);

            var stored = await data.GetStoredEngineEditionsAsync();

            Assert.Equal(AzureSqlDatabase, stored[1]);
            Assert.Equal(Enterprise, stored[2]);
            Assert.False(stored.ContainsKey(77));
            foreach (var id in new[] { 1, 2, 77 })
            {
                Assert.Equal(await data.GetSqlEngineEditionAsync(id), stored.TryGetValue(id, out var e) ? e : Blank);
            }
        }
        finally
        {
            /* And again once it is gone, or the view would still name a file that no longer exists. */
            File.Delete(archived);
            await _duckDb.CreateArchiveViewsAsync();
        }
    }

    private static string Graph(string database) =>
        $"<deadlock><victim-list/><process-list><process id=\"p0\" currentdbname=\"{database}\"/></process-list></deadlock>";

    [Fact]
    public async Task FirstSweep_TheMastersCard_ThroughTheProvider_SkipsTheSiblingsEvents()
    {
        /* The provider the analysis, the card, the daily summary and the MCP reads use, seeded from a real store
           and asked before any connection check (every live edition blank). */
        var servers = AzureFleet(out var master);
        var masterId = KnownEngineEditions.StorageId(master);
        var now = DateTime.UtcNow;
        await SeedPropertiesAsync(masterId, AzureSqlDatabase, now.AddHours(-2));
        foreach (var (database, count) in new[] { ("GP", 4), ("HS", 2), ("master", 3) })
        {
            for (var i = 0; i < count; i++)
            {
                await ExecAsync(
                    "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, wait_time_ms) VALUES ($1,$2,$2,$3,'TestServer',$4,1000)",
                    _nextId--, now.AddMinutes(-10 - i), masterId, database);
                await ExecAsync(
                    "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,'TestServer',$2,$4,NULL)",
                    _nextId--, now.AddMinutes(-10 - i), masterId, Graph(database));
            }
        }

        var editions = new KnownEngineEditions();
        editions.Seed(await new LocalDataService(_duckDb).GetStoredEngineEditionsAsync());
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id => editions.SeparatelyMonitoredDatabasesOrNull(id, servers, _ => Blank);

        var card = (await new LocalDataService(_duckDb).GetServerSummaryAsync(masterId, "TestServer", registeredAtUtc: null))!;

        Assert.Equal(3, card.BlockingCount);
        Assert.Equal(3, card.DeadlockCount);
    }
}
