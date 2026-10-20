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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6179: get_server_properties and get_memory_stats name the server by its registry display name, and build their payload
/// through the engine-scoped builders. This drives the tools against a DuckDB store with one Azure SQL Database (engine
/// edition 5) and one box server: the host's hardware figures are withheld on the Azure database only, the memory note rides on
/// it only, and the <c>server</c> field is the display name, not the storage key.
/// </summary>
public sealed class ServerHardwareToolWiringTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string AzureKey = "LiteHwWireAzure";
    private const string AzureDisplay = "Lite Azure display";
    private const string BoxKey = "LiteHwWireBox";
    private const string BoxDisplay = "Lite Box display";

    private readonly SharedDuckDbFixture _fixture;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = -600_000;

    public ServerHardwareToolWiringTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-hwwire-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task ServerPropertiesAndMemoryStats_ScopeTheHostByEngine_AndNameTheServerByDisplayName()
    {
        var azureId = Register(AzureKey, AzureDisplay);
        var boxId = Register(BoxKey, BoxDisplay);
        await SeedAsync(azureId, AzureKey, engineEdition: 5);
        await SeedAsync(boxId, BoxKey, engineEdition: 3);

        var data = new LocalDataService(_fixture.DuckDb);
        string[] hostKeys = ["hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

        /* Azure SQL Database: the host's figures are withheld, the vCores and a note come back, and the server is named by its display name. */
        using (var props = JsonDocument.Parse(await McpServerInfoTools.GetServerProperties(data, _serverManager, AzureKey)))
        {
            var root = props.RootElement;
            Assert.Equal(AzureDisplay, root.GetProperty("server").GetString());
            foreach (var key in hostKeys)
                Assert.Equal(JsonValueKind.Null, root.GetProperty(key).ValueKind);
            Assert.Equal(1, root.GetProperty("vcore_count").GetInt32());
            Assert.True(root.TryGetProperty("hardware_note", out _));
        }

        using (var mem = JsonDocument.Parse(await McpMemoryTools.GetMemoryStats(data, _serverManager, AzureKey)))
        {
            var root = mem.RootElement;
            Assert.Equal(AzureDisplay, root.GetProperty("server").GetString());
            Assert.Equal(5, root.GetProperty("engine_edition").GetInt32());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("system_memory_state").ValueKind);
            Assert.True(root.TryGetProperty("memory_note", out _));
        }

        /* A box server keeps today's figures and gets no note, so the scoping is the engine's doing and not a blanket rule. */
        using (var props = JsonDocument.Parse(await McpServerInfoTools.GetServerProperties(data, _serverManager, BoxKey)))
        {
            var root = props.RootElement;
            Assert.Equal(BoxDisplay, root.GetProperty("server").GetString());
            Assert.Equal(64, root.GetProperty("hyperthread_ratio").GetInt32());
            Assert.Equal(933_888L, root.GetProperty("physical_memory_mb").GetInt64());
            Assert.False(root.TryGetProperty("hardware_note", out _));
        }

        using (var mem = JsonDocument.Parse(await McpMemoryTools.GetMemoryStats(data, _serverManager, BoxKey)))
        {
            var root = mem.RootElement;
            Assert.Equal(BoxDisplay, root.GetProperty("server").GetString());
            Assert.Equal("Available physical memory is high", root.GetProperty("system_memory_state").GetString());
            Assert.False(root.TryGetProperty("memory_note", out _));
        }
    }

    private int Register(string key, string display)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = key, DisplayName = display, IsEnabled = true };
        _serverManager.AddServer(server);
        return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    private async Task SeedAsync(int serverId, string serverName, int engineEdition)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        var azure = engineEdition == 5;
        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, service_objective, vcore_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(_nextId--); P(DateTime.UtcNow); P(serverId); P(serverName); P(azure ? "SQL Azure" : "Enterprise Edition (64-bit)");
            P("12.0.2000.8"); P("RTM"); P(engineEdition); P(2); P(64); P(933_888L); P(0); P(32);
            P(azure ? "GP_S_Gen5_1" : null); P(azure ? 1 : null);
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, system_memory_state)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(_nextId--); P(DateTime.UtcNow); P(serverId); P(serverName);
            P(65_536.0); P(16_384.0); P(49_152.0); P(49_152.0); P(40_000.0); P("Available physical memory is high");
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
