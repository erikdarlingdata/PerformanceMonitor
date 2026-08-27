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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2511 on Lite: the SAME read, on two servers that differ only in <c>servers.sql_engine_edition</c>, must
/// answer differently — <c>not_collected</c> on Azure SQL Database, where the collector serving the read
/// cannot run at all, and the read's own <c>empty</c>/<c>unavailable</c> on an engine that does collect it.
///
/// <para><b>Both directions, always.</b> A pin that only asserted the Azure branch would pass equally well
/// if the read had stopped distinguishing anything and started answering <c>not_collected</c> to everyone,
/// which is a worse defect than the one being fixed: it would hide real collection outages behind a
/// confident "this engine cannot do that".</para>
///
/// <para>Lite derives its server id from the storage name rather than storing one, so the seeded registry
/// row has to be written under the same derived value the tool resolves to — a hardcoded id would seed a row
/// the tool looks straight past, and the Azure assertion would pass for the wrong reason.</para>
/// </summary>
public sealed class EngineCapabilityMissTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string AzureServerName = "LiteEngineCapAzure";
    private const string BoxServerName = "LiteEngineCapBox";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _azureServerId;
    private readonly int _boxServerId;
    private DuckDBConnection? _seedConn;

    public EngineCapabilityMissTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-enginecap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

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

    [Fact]
    public async Task AnEmptyRead_AnswersNotCollectedOnAzureSqlDb_AndKeepsItsOwnMissOnABox()
    {
        await SeedServerRowAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        await SeedServerRowAsync(_boxServerId, BoxServerName, engineEdition: 3);

        var service = new LocalDataService(_duckDb);

        /* Neither server has a single collected row: the ONLY difference is the engine edition. */

        var azureHealth = await McpHealthParserTools.GetSystemHealth(service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(azureHealth));
        Assert.Contains("Azure SQL Database", azureHealth, StringComparison.Ordinal);
        Assert.Contains("EngineEdition 5", azureHealth, StringComparison.Ordinal);
        Assert.Contains("system_health_events", azureHealth, StringComparison.Ordinal);
        Assert.Contains(AzureServerName, azureHealth, StringComparison.Ordinal);

        /* The never-captured branch of get_health_parser_significant_waits carried the message the issue
           quoted. It must no longer tell an Azure caller to start a session that cannot exist there. */
        var azureWaits = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(azureWaits));
        Assert.DoesNotContain("system_health session is started", azureWaits, StringComparison.Ordinal);

        /* A read from a different family, sharing nothing with the above but the helper. */
        var azureFlags = await McpConfigTools.GetTraceFlags(service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(azureFlags));
        Assert.Contains("trace_flags", azureFlags, StringComparison.Ordinal);

        /* A third family, and deliberately NOT get_tempdb_trend: #2512 measured the tempdb DMVs returning
           real data on Azure SQL Database, so #2516 opens that gate and tempdb_stats stops being a permanent
           gap. Picking it as the example here would tie this test to a gate that is moving; the default
           trace is absent from the engine itself, so its gate is a durable one to demonstrate with. */
        var azureTrace = await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(azureTrace));
        Assert.Contains("default_trace_events", azureTrace, StringComparison.Ordinal);

        /* ── The box, same empty store: every one of them keeps the answer it gave before. ── */
        Assert.Equal("empty", StatusOf(await McpHealthParserTools.GetSystemHealth(service, _serverManager, BoxServerName)));

        var boxWaits = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, BoxServerName);
        Assert.Equal("unavailable", StatusOf(boxWaits));
        Assert.Contains("system_health session is started", boxWaits, StringComparison.Ordinal);

        Assert.Equal("empty", StatusOf(await McpConfigTools.GetTraceFlags(service, _serverManager, BoxServerName)));
        Assert.Equal("empty", StatusOf(await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, BoxServerName)));

        /* A read whose collector runs on every engine is untouched on BOTH servers — the helper must not
           have become a blanket "Azure gets not_collected" rule. */
        Assert.Equal("unavailable", StatusOf(await McpConfigTools.GetDatabaseConfig(service, _serverManager, AzureServerName)));
        Assert.Equal("unavailable", StatusOf(await McpConfigTools.GetDatabaseConfig(service, _serverManager, BoxServerName)));
    }

    /// <summary>
    /// A registry row with no probed edition — a server that has never completed a connect — keeps its old
    /// miss. "We do not know" rendering as "this will never work" would be the same defect wearing the fix's
    /// clothes, and it is the state every server passes through on its first cycle.
    /// </summary>
    [Fact]
    public async Task AServerWithNoProbedEdition_KeepsItsOldMiss()
    {
        await SeedServerRowAsync(_boxServerId, BoxServerName, engineEdition: CollectorEngineCapability.UnknownEngineEdition);

        var service = new LocalDataService(_duckDb);

        Assert.Equal("empty", StatusOf(await McpHealthParserTools.GetSystemHealth(service, _serverManager, BoxServerName)));
        Assert.Equal("empty", StatusOf(await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, BoxServerName)));
        Assert.Equal("empty", StatusOf(await McpConfigTools.GetTraceFlags(service, _serverManager, BoxServerName)));
    }

    /// <summary>
    /// A server the registry has no row for at all reads as unknown, not as a capability gap. The MCP
    /// surface resolves against the ServerManager, so a freshly added server can be asked about before the
    /// collector has ever written its <c>servers</c> row.
    /// </summary>
    [Fact]
    public async Task AServerWithNoRegistryRow_KeepsItsOldMiss()
    {
        var service = new LocalDataService(_duckDb);

        Assert.Equal(CollectorEngineCapability.UnknownEngineEdition, await service.GetSqlEngineEditionAsync(_azureServerId));
        Assert.Equal("empty", StatusOf(await McpHealthParserTools.GetSystemHealth(service, _serverManager, AzureServerName)));
    }

    private static string StatusOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    /// <summary>Column list copied from <c>TestDataSeeder</c>'s servers seed, plus the one column these
    /// tests exist to vary.</summary>
    private async Task SeedServerRowAsync(int serverId, string serverName, int engineEdition)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO servers (server_id, server_name, display_name, use_windows_auth, is_enabled, sql_engine_edition)
VALUES ($1, $2, $3, true, true, $4)";
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = engineEdition });
        await cmd.ExecuteNonQueryAsync();
    }
}
