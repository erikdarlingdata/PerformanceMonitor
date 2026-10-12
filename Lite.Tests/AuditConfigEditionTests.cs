/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <c>audit_config</c> on Lite, end to end over a seeded store. Two things it got wrong, both about the engine:
/// it named an EngineEdition with a private switch that disagreed with every other surface (6 is Azure Synapse
/// Analytics, 8 is Managed Instance, and there is no "HADR" edition), and its empty answer told every engine
/// "the config collector may not have run yet" - false on Azure SQL Database, whose engine never collects
/// <c>server_config</c> at all, and where <c>get_server_config</c> already answers <c>not_collected</c>.
///
/// <para>The edition is seeded where production holds it, as a collected <c>server_properties</c> row, with no
/// <c>servers</c> row anywhere.</para>
/// </summary>
public sealed class AuditConfigEditionTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "LiteAuditConfigEdition";
    private const string AzureServerName = "LiteAuditConfigAzure";
    private const string BoxServerName = "LiteAuditConfigBox";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly int _azureServerId;
    private readonly int _boxServerId;
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public AuditConfigEditionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-auditcfg-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        _serverId = Register(ServerName);
        _azureServerId = Register(AzureServerName);
        _boxServerId = Register(BoxServerName);
    }

    private int Register(string name)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = true };
        _serverManager.AddServer(server);
        return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>Edition 8 is Managed Instance (the old switch appended a "(HADR)" that is not an edition), 6 is
    /// Azure Synapse Analytics (the old switch called it Managed Instance), and an edition nothing has probed
    /// stays "Unknown" rather than the shared table's "Unknown (0)".</summary>
    [Theory]
    [InlineData(8, "Azure SQL Managed Instance")]
    [InlineData(6, "Azure Synapse Analytics")]
    [InlineData(5, "Azure SQL Database")]
    [InlineData(3, "Enterprise")]
    [InlineData(0, "Unknown")]
    public async Task AuditConfig_NamesTheEdition_FromTheSharedEditionTable(int engineEdition, string expected)
    {
        await SeedServerPropertiesAsync(_serverId, ServerName, engineEdition);
        await SeedServerConfigAsync(_serverId, ServerName);

        var json = await McpAnalysisTools.AuditConfig(
            new AnalysisService(_duckDb), new LocalDataService(_duckDb), _serverManager, ServerName);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(expected, doc.RootElement.GetProperty("edition").GetString());
        Assert.True(doc.RootElement.GetProperty("summary").GetProperty("settings_checked").GetInt32() > 0);
    }

    /// <summary>With no configuration rows at all, the engine decides the answer: Azure SQL Database never
    /// collects server_config so it answers <c>not_collected</c> (as get_server_config does); a box keeps the
    /// "may not have run yet" answer, because there the collector really might not have.</summary>
    [Fact]
    public async Task AuditConfig_WithNoConfigRows_AnswersNotCollectedOnAzureSqlDb_AndKeepsItsOwnMissOnABox()
    {
        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: 3);

        var analysis = new AnalysisService(_duckDb);
        var service = new LocalDataService(_duckDb);

        var azure = await McpAnalysisTools.AuditConfig(analysis, service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(azure));
        Assert.Contains("Azure SQL Database", azure, StringComparison.Ordinal);
        Assert.Contains("server_config", azure, StringComparison.Ordinal);
        Assert.DoesNotContain("may not have run yet", azure, StringComparison.Ordinal);

        var box = await McpAnalysisTools.AuditConfig(analysis, service, _serverManager, BoxServerName);
        Assert.Equal("no_config_data", StatusOf(box));
        Assert.Contains("may not have run yet", box, StringComparison.Ordinal);
    }

    /// <summary>The MAXDOP audit follows the vCores on an Azure SQL Database. The seeded row holds the host's 32 cores per socket
    /// beside 4 vCores, so a recommendation taken from cores per socket would read 8 and one taken from the vCores reads 4. An
    /// Azure SQL Database collects no <c>server_config</c> in production; the rows are seeded here to reach the recommendation.</summary>
    [Fact]
    public async Task AuditConfig_OnAzureSqlDatabase_RecommendsMaxdopFromTheVcores_NotTheHostsCoresPerSocket()
    {
        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, vcoreCount: 4, coresPerSocket: 32);
        await SeedServerConfigAsync(_azureServerId, AzureServerName);

        var json = await McpAnalysisTools.AuditConfig(new AnalysisService(_duckDb), new LocalDataService(_duckDb), _serverManager, AzureServerName);

        var maxdop = MaxdopRecommendation(json);
        Assert.Equal(4, maxdop.GetProperty("suggested_value").GetInt32());
        var text = maxdop.GetProperty("recommendation").GetString()!;
        Assert.Contains("Start with 4 (this database's vCores, capped at 8)", text, StringComparison.Ordinal);
        Assert.DoesNotContain("cores-per-socket", text, StringComparison.Ordinal);
    }

    /// <summary>The same rows on SQL Server recommend from cores per socket, as they always did.</summary>
    [Fact]
    public async Task AuditConfig_OffAzureSqlDatabase_RecommendsMaxdopFromCoresPerSocket_AsItAlwaysDid()
    {
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: 3, vcoreCount: null, coresPerSocket: 6);
        await SeedServerConfigAsync(_boxServerId, BoxServerName);

        var json = await McpAnalysisTools.AuditConfig(new AnalysisService(_duckDb), new LocalDataService(_duckDb), _serverManager, BoxServerName);

        var maxdop = MaxdopRecommendation(json);
        Assert.Equal(6, maxdop.GetProperty("suggested_value").GetInt32());
        Assert.Contains("Start with 6 (this server's cores-per-socket, capped at 8)", maxdop.GetProperty("recommendation").GetString(), StringComparison.Ordinal);
    }

    private static JsonElement MaxdopRecommendation(string json)
    {
        using var doc = JsonDocument.Parse(json);
        foreach (var r in doc.RootElement.GetProperty("recommendations").EnumerateArray())
        {
            if (r.GetProperty("setting").GetString() == "max degree of parallelism")
                return r.Clone();
        }

        throw new Xunit.Sdk.XunitException("audit_config returned no MAXDOP recommendation: " + json);
    }

    private static string StatusOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    /// <summary>One collected <c>server_properties</c> row: the NOT NULL edition and hardware columns are
    /// filled with values nothing here reads, plus the one column these tests vary.</summary>
    private async Task SeedServerPropertiesAsync(int serverId, string serverName, int engineEdition, int? vcoreCount = null, int? coresPerSocket = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name,
     edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, cores_per_socket, vcore_count)
VALUES ($1, $2, $3, $4, 'Test Edition', '16.0.4150.1', 'RTM', $5, 8, 1, 16384, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = engineEdition });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)coresPerSocket ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)vcoreCount ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The two settings audit_config grades first, so the tool has recommendations to echo.</summary>
    private async Task SeedServerConfigAsync(int serverId, string serverName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        foreach (var (name, value) in new[] { ("cost threshold for parallelism", 5), ("max degree of parallelism", 0) })
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, true)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
            cmd.Parameters.Add(new DuckDBParameter { Value = name });
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
