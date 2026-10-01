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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2511 on Lite: the SAME read, on two servers that differ only in <c>server_properties.engine_edition</c>, must
/// answer differently — <c>not_collected</c> on Azure SQL Database, where the collector serving the read
/// cannot run at all, and the read's own <c>empty</c>/<c>unavailable</c> on an engine that does collect it.
///
/// <para><b>Both directions, always.</b> A pin that only asserted the Azure branch would pass equally well
/// if the read had stopped distinguishing anything and started answering <c>not_collected</c> to everyone,
/// which is a worse defect than the one being fixed: it would hide real collection outages behind a
/// confident "this engine cannot do that".</para>
///
/// <para>Lite derives its server id from the storage name rather than storing one, so the seeded
/// <c>server_properties</c> row has to be written under the same derived value the tool resolves to — a
/// hardcoded id would seed a row the tool looks straight past, and the Azure assertion would pass for the
/// wrong reason.</para>
///
/// <para><b>The edition is seeded where production holds it.</b> The collector writes a
/// <c>server_properties</c> row every cycle; nothing in this SKU ever inserts a <c>servers</c> row. These
/// tests used to seed that row, which is how the gate passed here for a release while never firing on a real
/// Azure SQL Database store. <see cref="TheGate_ReadsCollectedServerProperties_WithNoServersRowAnywhere"/> pins
/// the no-<c>servers</c>-row state itself.</para>
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
    private long _nextCollectionId = -1;

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
        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: 3);

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

        /* The CPU scheduler read, the answer Darling's twin gives: the cpu_scheduler_stats collector's own
           AppliesTo gate skips Azure SQL Database, so "the collector may not have run yet" would be untrue. */
        var azureScheduler = await McpPlanCacheSchedulerTools.GetCpuSchedulerPressure(service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(azureScheduler));
        Assert.Contains("cpu_scheduler_stats", azureScheduler, StringComparison.Ordinal);

        /* ── The box, same empty store: every one of them keeps its own miss — the ENGINE answer must not
              have become a blanket rule. For the health-parser family that own miss is "unavailable" since
              #3541 A12 (a server whose system_health session has never been read into the store is not a
              clean bill), the answer significant_waits alone used to give and the other eight now share. ── */
        var boxHealth = await McpHealthParserTools.GetSystemHealth(service, _serverManager, BoxServerName);
        Assert.Equal("unavailable", StatusOf(boxHealth));
        Assert.Contains("system_health session is started", boxHealth, StringComparison.Ordinal);

        var boxWaits = await McpHealthParserTools.GetSignificantWaits(service, _serverManager, BoxServerName);
        Assert.Equal("unavailable", StatusOf(boxWaits));
        Assert.Contains("system_health session is started", boxWaits, StringComparison.Ordinal);

        Assert.Equal("empty", StatusOf(await McpConfigTools.GetTraceFlags(service, _serverManager, BoxServerName)));
        Assert.Equal("empty", StatusOf(await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, BoxServerName)));
        Assert.Equal("unavailable", StatusOf(await McpPlanCacheSchedulerTools.GetCpuSchedulerPressure(service, _serverManager, BoxServerName)));

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
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: CollectorEngineCapability.UnknownEngineEdition);

        var service = new LocalDataService(_duckDb);

        /* "Old miss" is each family's own: for the health parsers a never-read session is "unavailable"
           (#3541 A12), and the point here is that it is NOT "not_collected" — unknown is not never. */
        Assert.Equal("unavailable", StatusOf(await McpHealthParserTools.GetSystemHealth(service, _serverManager, BoxServerName)));
        Assert.Equal("empty", StatusOf(await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, BoxServerName)));
        Assert.Equal("empty", StatusOf(await McpConfigTools.GetTraceFlags(service, _serverManager, BoxServerName)));
    }

    /// <summary>
    /// A server with no collected <c>server_properties</c> row at all reads as unknown, not as a capability
    /// gap. The MCP surface resolves against the ServerManager, so a freshly added server can be asked about
    /// before the collector has ever written its first row.
    /// </summary>
    [Fact]
    public async Task AServerWithNoRegistryRow_KeepsItsOldMiss()
    {
        var service = new LocalDataService(_duckDb);

        Assert.Equal(CollectorEngineCapability.UnknownEngineEdition, await service.GetSqlEngineEditionAsync(_azureServerId));
        Assert.Equal("unavailable", StatusOf(await McpHealthParserTools.GetSystemHealth(service, _serverManager, AzureServerName)));
    }

    private static string StatusOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    /// <summary>
    /// The pin for the read itself. Lite never inserts a <c>servers</c> row, so the gate has to answer from
    /// the collected <c>server_properties</c> row alone: with that row at EngineEdition 5 and the
    /// <c>servers</c> table EMPTY, the edition reads back as 5 and a never-collected read answers
    /// <c>not_collected</c>. Against the old read of <c>servers.sql_engine_edition</c> both fail: the edition
    /// reads back unknown and the tool answers <c>empty</c>.
    /// </summary>
    [Fact]
    public async Task TheGate_ReadsCollectedServerProperties_WithNoServersRowAnywhere()
    {
        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);

        Assert.Equal(0L, await CountServersRowsAsync());

        var service = new LocalDataService(_duckDb);
        Assert.Equal(CollectorEngineCapability.AzureSqlDatabaseEngineEdition, await service.GetSqlEngineEditionAsync(_azureServerId));

        var trace = await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, AzureServerName);
        Assert.Equal("not_collected", StatusOf(trace));
        Assert.Contains("default_trace_events", trace, StringComparison.Ordinal);
    }

    /// <summary>
    /// A server's edition is the NEWEST collected row's, the same row the analysis engine reads its own
    /// edition fact from: an older row at a different edition (a server re-pointed at a different instance)
    /// must not win over the latest one, in either direction.
    /// </summary>
    [Fact]
    public async Task TheNewestCollectedRow_DecidesTheEdition()
    {
        var now = DateTime.UtcNow;
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: 5, collectedAt: now.AddHours(-2));
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: 3, collectedAt: now.AddHours(-1));

        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, engineEdition: 3, collectedAt: now.AddHours(-2));
        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, engineEdition: 5, collectedAt: now.AddHours(-1));

        var service = new LocalDataService(_duckDb);
        Assert.Equal(3, await service.GetSqlEngineEditionAsync(_boxServerId));
        Assert.Equal(5, await service.GetSqlEngineEditionAsync(_azureServerId));
    }

    /// <summary>
    /// get_memory_stats HAS data on an Azure SQL Database, but its memory collector has no memory-state source there
    /// and stores the constant "Available". So the read stays data, the state is null and the note beside it says
    /// why, and an AI client never reads that constant as a healthy state. A box keeps its stored state and no note,
    /// and so does a server whose edition is unknown (no collected row), which makes no claim.
    /// </summary>
    [Fact]
    public async Task GetMemoryStats_PublishesTheStateAsNullWithItsNote_OnAzureSqlDbOnly()
    {
        await SeedServerPropertiesAsync(_azureServerId, AzureServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        await SeedServerPropertiesAsync(_boxServerId, BoxServerName, engineEdition: 3);
        await SeedMemoryStatsAsync(_azureServerId, AzureServerName, "Available");
        await SeedMemoryStatsAsync(_boxServerId, BoxServerName, "Available physical memory is high");

        var service = new LocalDataService(_duckDb);

        using (var azure = JsonDocument.Parse(await McpMemoryTools.GetMemoryStats(service, _serverManager, AzureServerName)))
        {
            Assert.False(azure.RootElement.TryGetProperty("status", out _));
            Assert.Equal(JsonValueKind.Null, azure.RootElement.GetProperty("system_memory_state").ValueKind);
            Assert.Equal(ServerHardwareScope.MemoryStateNote, azure.RootElement.GetProperty("system_memory_state_note").GetString());
            Assert.Equal(1838d, azure.RootElement.GetProperty("total_physical_memory_mb").GetDouble());
        }

        using (var box = JsonDocument.Parse(await McpMemoryTools.GetMemoryStats(service, _serverManager, BoxServerName)))
        {
            Assert.Equal("Available physical memory is high", box.RootElement.GetProperty("system_memory_state").GetString());
            Assert.Equal(JsonValueKind.Null, box.RootElement.GetProperty("system_memory_state_note").ValueKind);
        }
    }

    [Fact]
    public async Task GetMemoryStats_KeepsTheStoredState_WhenTheEditionIsUnknown()
    {
        await SeedMemoryStatsAsync(_azureServerId, AzureServerName, "Available");

        using var doc = JsonDocument.Parse(await McpMemoryTools.GetMemoryStats(new LocalDataService(_duckDb), _serverManager, AzureServerName));

        Assert.Equal("Available", doc.RootElement.GetProperty("system_memory_state").GetString());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("system_memory_state_note").ValueKind);
    }

    /// <summary>One collected <c>memory_stats</c> row: the memory state under test, the page file at 0 as the Azure SQL
    /// Database query stores it, and a memory limit nothing here varies.</summary>
    private async Task SeedMemoryStatsAsync(int serverId, string serverName, string systemMemoryState)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb, total_page_file_mb, available_page_file_mb,
     system_memory_state, sql_memory_model, target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, 1838, 412, 0, 0, $5, 'N/A', 1600, 1500, 1200, 90)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextCollectionId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = systemMemoryState });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<long> CountServersRowsAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM servers";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    /// <summary>One collected <c>server_properties</c> row (the NOT NULL columns filled with values nothing
    /// here reads), plus the one column these tests exist to vary.</summary>
    private async Task SeedServerPropertiesAsync(int serverId, string serverName, int engineEdition, DateTime? collectedAt = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name,
     edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb)
VALUES ($1, $2, $3, $4, 'Test Edition', '16.0.4150.1', 'RTM', $5, 8, 1, 16384)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextCollectionId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectedAt ?? DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = engineEdition });
        await cmd.ExecuteNonQueryAsync();
    }
}
