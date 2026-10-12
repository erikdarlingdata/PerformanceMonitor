/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Two collected tables hold a "physical memory" figure, and on an Azure SQL Database (engine edition 5) only one of them is
/// the host's. <c>server_properties.physical_memory_mb</c> comes from <c>sys.dm_os_sys_info.physical_memory_kb</c> and is the
/// HOST's (911.9 GB for a 1-vCore serverless General Purpose database). <c>memory_stats.total_physical_memory_mb</c> comes from
/// <c>committed_target_kb</c> there, which is the database's own memory limit (1,838 MB for that same database), and the
/// buffer pool and server-memory counters beside it are the database's too.
///
/// <para>So the FinOps utilization card's Physical Memory and Buffer Pool %, its verdict sentences and the health score's memory
/// term, which all read <c>memory_stats</c>, are shown and scored on an Azure SQL Database exactly as on SQL Server. What reads
/// <c>server_properties</c> (<c>get_server_properties</c>, the Server Inventory hardware cells) still hides the host's memory,
/// sockets, cores per socket and hyperthread ratio.
/// Every test seeds BOTH tables with different values (1,838 MB and 933,836 MB), so a read that takes the wrong table shows up
/// as the wrong number. The Darling.Tests twin pins the same table for the other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseMemoryScopeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -487_004;

    /// <summary>What <c>memory_stats</c> holds on the 1-vCore database: <c>committed_target_kb</c> / 1024.</summary>
    private const int DatabaseMemoryLimitMb = 1_838;

    /// <summary>What <c>server_properties</c> holds for the same database: the host's physical memory.</summary>
    private const long HostPhysicalMemoryMb = 933_836;

    private readonly SharedDuckDbFixture _fixture;
    private DuckDBConnection? _seedConn;

    public AzureSqlDatabaseMemoryScopeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
    }

    public void Dispose() => _seedConn?.Dispose();

    // ── the utilization read: memory_stats, not server_properties ──

    [Theory]
    [InlineData(5)]   // Azure SQL Database
    [InlineData(3)]   // SQL Server
    [InlineData(8)]   // Managed Instance
    public async Task UtilizationRead_TakesTheMemoryFiguresFromMemoryStats_OnEveryEdition(int engineEdition)
    {
        await SeedAsync(engineEdition);

        var row = await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(engineEdition, row!.EngineEdition);
        Assert.Equal(DatabaseMemoryLimitMb, row.PhysicalMemoryMb);
        Assert.Equal(1_100, row.BufferPoolMb);
        Assert.Equal(1_500, row.TotalMemoryMb);
        Assert.Equal(1_800, row.TargetMemoryMb);
    }

    // ── the health score keeps its memory term on an Azure SQL Database ──

    /// <summary>CPU p95 of 7% scores 95 and 50% free storage scores 100. The buffer pool is 1,100 MB. Against the database's
    /// 1,838 MB limit that is 60%, which scores 100: 95 * 0.4 + 100 * 0.3 + 100 * 0.3 = 98. Against the host's 933,836 MB it
    /// would be 0.1%, which scores 60 and gives 86. Left out altogether it gives 97.</summary>
    private static UtilizationEfficiencyRow Scored(UtilizationEfficiencyRow row)
    {
        row.ProvisioningStatus = ProvisioningVerdict.RightSized; // a measured window: a window with no CPU sample has no CPU term
        row.P95CpuPct = 7m;
        row.FreeSpacePct = 50m;
        return row;
    }

    [Fact]
    public async Task HealthScore_OnAzureSqlDatabase_CarriesTheMemoryTermScoredFromMemoryStats()
    {
        await SeedAsync(engineEdition: 5);

        var row = Scored((await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId))!);

        Assert.Equal(98, row.ComputeHealthScore());
        Assert.NotEqual(97, row.ComputeHealthScore());   // the memory term was not left out
        Assert.NotEqual(86, row.ComputeHealthScore());   // and it was not scored against the host's memory
    }

    [Theory]
    [InlineData(3)]
    [InlineData(8)]
    public async Task HealthScore_OnSqlServerAndManagedInstance_IsTheSameScoreFromTheSameMemoryStats(int engineEdition)
    {
        await SeedAsync(engineEdition);
        var service = new LocalDataService(_fixture.DuckDb);

        var row = Scored((await service.GetUtilizationEfficiencyAsync(ServerId))!);

        Assert.Equal(98, row.ComputeHealthScore());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(8)]
    public void HealthScore_DoesNotDependOnTheEngineEdition(int engineEdition)
    {
        UtilizationEfficiencyRow Row(int edition, int physicalMb) => Scored(new UtilizationEfficiencyRow
        {
            EngineEdition = edition, BufferPoolMb = 1_100, PhysicalMemoryMb = physicalMb,
        });

        var baseline = Row(3, DatabaseMemoryLimitMb).ComputeHealthScore();

        Assert.Equal(98, baseline);
        Assert.Equal(baseline, Row(engineEdition, DatabaseMemoryLimitMb).ComputeHealthScore());
        /* And the score does move with the figure memory_stats holds, on an Azure SQL Database as anywhere. */
        Assert.Equal(86, Row(engineEdition, (int)HostPhysicalMemoryMb).ComputeHealthScore());
    }

    // ── the verdict sentences cite the buffer pool's share again ──

    [Fact]
    public void RightSizedSentence_OnAzureSqlDatabase_CitesTheBufferPoolShare_OfTheDatabasesMemoryLimit()
    {
        var onAzure = ServerHardwareScope.RightSizedExplanation(40m, 62m, 71.0, azureSqlDatabase: true);
        var onBox = ServerHardwareScope.RightSizedExplanation(40m, 62m, 71.0, azureSqlDatabase: false);

        Assert.Equal(
            "CPU is moderately loaded (avg 40.0%, p95 62.0%) and memory is well-utilized (buffer pool uses 71% of the database's memory limit). No action needed.",
            onAzure);
        Assert.DoesNotContain("physical RAM", onAzure, StringComparison.Ordinal);
        Assert.Equal(
            "CPU is moderately loaded (avg 40.0%, p95 62.0%) and memory is well-utilized (buffer pool uses 71% of physical RAM). No action needed.",
            onBox);
    }

    [Fact]
    public void OverProvisionedSentence_OnAzureSqlDatabase_CitesTheBufferPoolShare_OfTheDatabasesMemoryLimit()
    {
        var onAzure = ServerHardwareScope.OverProvisionedExplanation(3.2m, 11, 40.0, azureSqlDatabase: true);
        var onBox = ServerHardwareScope.OverProvisionedExplanation(3.2m, 11, 40.0, azureSqlDatabase: false);

        Assert.Equal(
            "CPU is lightly loaded (avg 3.2%, max 11%) and buffer pool uses only 40% of the database's memory limit. This database may have more resources than it needs.",
            onAzure);
        Assert.DoesNotContain("physical RAM", onAzure, StringComparison.Ordinal);
        Assert.Equal(
            "CPU is lightly loaded (avg 3.2%, max 11%) and buffer pool uses only 40% of physical RAM. This server may have more resources than it needs.",
            onBox);
    }

    // ── what reads server_properties hides the host's four figures ──

    /// <summary>The four columns that describe the host on an Azure SQL Database. <c>cpu_count</c> is not among them.</summary>
    private static readonly string[] s_hostKeys =
        ["hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

    [Fact]
    public async Task ServerPropertiesReads_OnAzureSqlDatabase_HideTheHostsFourFigures_AndShowTheDatabasesOwnCpuCount()
    {
        await SeedAsync(engineEdition: 5);

        var stored = await new LocalDataService(_fixture.DuckDb).GetLatestServerPropertiesAsync(ServerId);

        Assert.NotNull(stored);
        /* The stored figure is the host's, which is why it is hidden: it is not memory_stats' 1,838. */
        Assert.Equal(HostPhysicalMemoryMb, stored!.PhysicalMemoryMb);

        /* get_server_properties (the payload the web Server Properties tiles read too). */
        var json = JsonDocument.Parse(McpServerInfoTools.ServerPropertiesPayload("Srv", stored)).RootElement;
        foreach (var key in s_hostKeys)
            Assert.Equal(JsonValueKind.Null, json.GetProperty(key).ValueKind);
        Assert.Equal(2, json.GetProperty("cpu_count").GetInt32());

        /* The FinOps Server Inventory row. */
        var inventory = new ServerPropertyRow
        {
            EngineEdition = stored.EngineEdition, CpuCount = stored.CpuCount, PhysicalMemoryMb = stored.PhysicalMemoryMb,
            SocketCount = stored.SocketCount, CoresPerSocket = stored.CoresPerSocket,
        };
        Assert.Equal(2, inventory.CpuCount);
        Assert.Null(inventory.PhysicalMemoryMb);
        Assert.Null(inventory.SocketCount);
        Assert.Null(inventory.CoresPerSocket);
    }

    [Fact]
    public async Task ServerPropertiesReads_OnSqlServer_KeepTheirHardware()
    {
        await SeedAsync(engineEdition: 3);

        var stored = await new LocalDataService(_fixture.DuckDb).GetLatestServerPropertiesAsync(ServerId);
        var json = JsonDocument.Parse(McpServerInfoTools.ServerPropertiesPayload("Srv", stored!)).RootElement;

        Assert.Equal(HostPhysicalMemoryMb, json.GetProperty("physical_memory_mb").GetInt64());
        Assert.Equal(2, json.GetProperty("cpu_count").GetInt32());
    }

    // ── the card and the recommendations, pinned at the source ──

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
            dir = Path.GetDirectoryName(dir);

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }

    /// <summary>Comments wrap, so a pin on their words reads them with every run of whitespace as one space.</summary>
    private static string Flatten(string text) => Regex.Replace(text, @"\s+", " ");

    [Fact]
    public void FinOpsUtilizationCard_ShowsPhysicalMemoryAndTheBufferPoolShare_OnEveryEdition()
    {
        var tab = ReadRepoFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.Contains("MemoryRatioText.Text = $\"{bpPct:N0}%\";", tab, StringComparison.Ordinal);
        Assert.Contains("SetBar(MemoryRatioBar, MemRatioFilled, MemRatioEmpty, bpPct);", tab, StringComparison.Ordinal);
        Assert.Contains("PhysicalMemoryText.Text = $\"{data.PhysicalMemoryMb:N0} MB\";", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerHardwareScope.NotApplicable", tab, StringComparison.Ordinal);
        /* The health score has its memory term everywhere, so there is no tooltip explaining an absent one. The only tooltip on it
           explains an absent CPU term (a window with no CPU sample), and it is not keyed on the edition. */
        Assert.DoesNotContain("HealthScoreWithoutMemoryNote", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("HealthScoreBorder.ToolTip = azureSqlDb", tab, StringComparison.Ordinal);
        Assert.Contains("data.HealthScore = data.ComputeHealthScore();", tab, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "Physical: ")]
    [InlineData(2, "Physical: ")]
    [InlineData(3, "Physical: ")]
    [InlineData(4, "Physical: ")]
    [InlineData(5, "Memory limit: ")]
    [InlineData(8, "Physical: ")]
    [InlineData(null, "Physical: ")]
    public void PhysicalMemoryCaption_NamesTheDatabasesLimit_OnAzureSqlDatabase_AndPhysicalMemoryEverywhereElse(int? engineEdition, string expected)
    {
        Assert.Equal(expected, ServerHardwareScope.PhysicalMemoryCaption(engineEdition));
    }

    [Fact]
    public void FinOpsUtilizationCard_ExplainsTheBufferPoolShareForADatabase_InTheWordsBothAppsUse()
    {
        var xaml = ReadRepoFile("Lite/Controls/FinOpsTab.xaml");

        Assert.Contains("On an Azure SQL Database it is the share of the database's memory limit.", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void UtilizationRead_DoesNotReadServerPropertiesForMemory()
    {
        var read = ReadRepoFile("Lite/Services/LocalDataService.FinOps.Utilization.cs");
        var start = read.IndexOf("mem_latest AS (", StringComparison.Ordinal);
        Assert.True(start > 0, "the mem_latest CTE is missing");
        var memLatest = read[start..read.IndexOf("),", start, StringComparison.Ordinal)];

        Assert.Contains("FROM v_memory_stats", memLatest, StringComparison.Ordinal);
        Assert.Contains("total_physical_memory_mb", memLatest, StringComparison.Ordinal);
        Assert.DoesNotContain("server_properties", memLatest, StringComparison.Ordinal);
        /* The one CTE that reads server_properties takes the CPU count and the edition, and nothing about memory. */
        var serverInfo = read[read.IndexOf("server_info AS (", StringComparison.Ordinal)..read.IndexOf("grants AS (", StringComparison.Ordinal)];
        Assert.DoesNotContain("physical_memory", serverInfo, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryRecommendation_NeverDividesByServerPropertiesMemory_SoItCannotPrintTheHostsGigabytes()
    {
        /* "Memory over-provisioned (P95 SQL memory uses 0% of 911GB RAM)" is the only text either app builds in the form
           "{percent} of {n}GB RAM". Its divisor is util.PhysicalMemoryMb, read from memory_stats through the utilization read,
           and the rules file reads no physical-memory column of its own. */
        var rules = ReadRepoFile("Lite/Services/LocalDataService.FinOps.Recommendations.cs");

        Assert.Contains("of {util.PhysicalMemoryMb / 1024}GB RAM", rules, StringComparison.Ordinal);
        Assert.DoesNotContain("physical_memory_mb", rules, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", rules, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryAndVmRules_OnAzureSqlDatabase_SayWhyTheyStandDown_AndDoNotCallTheMemoryTheHosts()
    {
        var rules = Flatten(ReadRepoFile("Lite/Services/LocalDataService.FinOps.Recommendations.cs"));

        Assert.DoesNotContain("reports the HOST's memory", rules, StringComparison.Ordinal);
        Assert.DoesNotContain("its memory figure is the host's", rules, StringComparison.Ordinal);
        Assert.Contains("its memory comes with its service objective and cannot be resized on its own", rules, StringComparison.Ordinal);
        Assert.Contains("its cores and memory come with its service objective", rules, StringComparison.Ordinal);
    }

    // ── get_memory_stats: the keys keep their names, so an Azure SQL Database's payload says what they mean ──

    /// <summary>The row <c>GetLatestMemoryStatsAsync</c> returns. It carries no edition: the payload takes the one the tool read.</summary>
    private static MemoryStatsRow StatsRow(double totalMb, double availableMb) => new()
    {
        CollectionTime = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc),
        TotalPhysicalMemoryMb = totalMb,
        AvailablePhysicalMemoryMb = availableMb,
        SystemMemoryState = "Available physical memory is high",
        SqlMemoryModel = "CONVENTIONAL",
        TargetServerMemoryMb = totalMb,
        TotalServerMemoryMb = totalMb - availableMb,
        BufferPoolMb = 1_100,
        PlanCacheMb = 200,
    };

    /// <summary>The payload for the ONE edition the tool reads (<see cref="McpEngineCapability.EngineEditionAsync"/>).</summary>
    private static JsonElement MemoryPayload(MemoryStatsRow stats, int engineEdition) =>
        JsonDocument.Parse(McpMemoryTools.MemoryStatsPayload("Srv", stats, engineEdition)).RootElement.Clone();

    private static JsonElement MemoryPayload(int engineEdition, double totalMb, double availableMb) =>
        MemoryPayload(StatsRow(totalMb, availableMb), engineEdition);

    [Fact]
    public void GetMemoryStats_OnAzureSqlDatabase_CarriesAMemoryNote_ThatCallsTheTotalTheDatabasesLimit_AndNearFullNormal()
    {
        /* A database that has grown to its limit has nothing left under it, so it reads 100% in use. On this edition that is the
           normal state, and the note says so, because the same figure on SQL Server is an operating system short of memory. */
        var json = MemoryPayload(5, DatabaseMemoryLimitMb, 0);

        var note = json.GetProperty("memory_note").GetString();
        Assert.Equal(ServerHardwareScope.McpMemoryNote, note);
        Assert.Contains("total_physical_memory_mb is the database's memory limit (its committed target), not the host's memory", note, StringComparison.Ordinal);
        Assert.Contains("available_physical_memory_mb is what is left under that limit", note, StringComparison.Ordinal);
        Assert.Contains("a value near 100% is normal once the database has grown to its limit", note, StringComparison.Ordinal);
        Assert.Contains("not memory pressure by itself", note, StringComparison.Ordinal);

        /* The figures and their names are as they were, and the note comes last. */
        Assert.Equal(DatabaseMemoryLimitMb, json.GetProperty("total_physical_memory_mb").GetDouble());
        Assert.Equal(0, json.GetProperty("available_physical_memory_mb").GetDouble());
        Assert.Equal(100, json.GetProperty("memory_utilization_pct").GetDouble());
        Assert.Equal(5, json.GetProperty("engine_edition").GetInt32());
        Assert.Equal("memory_note", json.EnumerateObject().Last().Name);

        /* The memory state is the constant "Available" the collector stores there, which is not a reading: null, with its note. */
        Assert.Equal(JsonValueKind.Null, json.GetProperty("system_memory_state").ValueKind);
        Assert.Equal(ServerHardwareScope.MemoryStateNote, json.GetProperty("system_memory_state_note").GetString());
    }

    [Theory]
    [InlineData(3)]
    [InlineData(8)]
    [InlineData(0)]
    public void GetMemoryStats_OffAzureSqlDatabase_KeepsTheStoredState_AndCarriesNoNotes(int engineEdition)
    {
        var json = MemoryPayload(engineEdition, 65_536, 16_384);

        Assert.False(json.TryGetProperty("memory_note", out _), "an engine that is not an Azure SQL Database gets no memory note");
        Assert.Equal("Available physical memory is high", json.GetProperty("system_memory_state").GetString());
        Assert.Equal(JsonValueKind.Null, json.GetProperty("system_memory_state_note").ValueKind);
        Assert.Equal(75, json.GetProperty("memory_utilization_pct").GetDouble());

        /* An unknown edition (0) publishes no edition at all rather than the number 0. */
        if (engineEdition == 0)
            Assert.Equal(JsonValueKind.Null, json.GetProperty("engine_edition").ValueKind);
        else
            Assert.Equal(engineEdition, json.GetProperty("engine_edition").GetInt32());

        Assert.Equal(
            new[]
            {
                "server", "captured_at", "total_physical_memory_mb", "available_physical_memory_mb", "memory_utilization_pct",
                "system_memory_state", "system_memory_state_note", "sql_memory_model", "target_server_memory_mb",
                "total_server_memory_mb", "buffer_pool_mb", "plan_cache_mb", "engine_edition",
            },
            json.EnumerateObject().Select(p => p.Name).ToArray());
    }

    [Theory]
    [InlineData(5)]
    [InlineData(3)]
    [InlineData(8)]
    [InlineData(0)]
    public void GetMemoryStats_EngineEdition_MemoryNote_AndTheStateNote_AllFollowTheOneEditionTheToolReads(int toolEdition)
    {
        /* The payload is built from the one edition the tool read from McpEngineCapability and from nothing else:
           engine_edition, the memory note, the state and the state's note all flip together with it. */
        var json = MemoryPayload(toolEdition, DatabaseMemoryLimitMb, 500);

        var azure = toolEdition == 5;
        Assert.Equal(azure, json.TryGetProperty("memory_note", out _));
        Assert.Equal(azure, json.GetProperty("system_memory_state").ValueKind == JsonValueKind.Null);
        Assert.Equal(azure, json.GetProperty("system_memory_state_note").ValueKind == JsonValueKind.String);
        Assert.Equal(toolEdition == 0 ? JsonValueKind.Null : JsonValueKind.Number, json.GetProperty("engine_edition").ValueKind);
        if (toolEdition != 0)
            Assert.Equal(toolEdition, json.GetProperty("engine_edition").GetInt32());
    }

    [Fact]
    public void TheLatestMemoryRead_CarriesNoEngineEdition_SoNoSecondSourceCanDisagreeWithTheTabsOrTheToolsOwn()
    {
        /* Each surface that names the memory figures reads ONE edition of its own: the Memory tab the tab's (the same one its
           page-file and memory-state lines read), get_memory_stats the one McpEngineCapability reads. A server_properties
           subselect beside the memory figures would be a second source for the same answer, so the read carries none and the
           row has no member to hold one. */
        var service = ReadRepoFile("Lite/Services/LocalDataService.Memory.cs");
        var start = service.IndexOf("Task<MemoryStatsRow?> GetLatestMemoryStatsAsync(", StringComparison.Ordinal);
        Assert.True(start > 0, "GetLatestMemoryStatsAsync is missing");
        var memoryRead = service[start..service.IndexOf("GetMemoryTrendAsync(", start, StringComparison.Ordinal)];

        Assert.Contains("FROM v_memory_stats", memoryRead, StringComparison.Ordinal);
        Assert.DoesNotContain("server_properties", memoryRead, StringComparison.Ordinal);
        Assert.DoesNotContain("engine_edition", memoryRead, StringComparison.Ordinal);
        Assert.Null(typeof(MemoryStatsRow).GetProperty("EngineEdition"));
    }

    [Theory]
    [InlineData(5, true)]
    [InlineData(3, false)]
    public async Task GetMemoryStats_ReadFromTheStore_CarriesTheNoteOnlyWhenTheStoredEngineEditionIsAnAzureSqlDatabase(int engineEdition, bool carriesNote)
    {
        /* The tool's one edition is McpEngineCapability.EngineEditionAsync: the newest collected server_properties row, which is
           what every Lite MCP engine gate reads, so the note follows what the collector stored and not anything the tool is told. */
        await SeedAsync(engineEdition);

        var service = new LocalDataService(_fixture.DuckDb);
        var stats = await service.GetLatestMemoryStatsAsync(ServerId);
        Assert.NotNull(stats);
        var storedEdition = await McpEngineCapability.EngineEditionAsync(service, ServerId);
        Assert.Equal(engineEdition, storedEdition);

        var json = MemoryPayload(stats!, storedEdition);

        Assert.Equal(carriesNote, json.TryGetProperty("memory_note", out _));
        Assert.Equal(carriesNote, json.GetProperty("system_memory_state").ValueKind == JsonValueKind.Null);
        Assert.Equal(DatabaseMemoryLimitMb, json.GetProperty("total_physical_memory_mb").GetDouble());
        Assert.Equal(engineEdition, json.GetProperty("engine_edition").GetInt32());
    }

    [Fact]
    public void GetMemoryStatsTool_BuildsItsPayloadThroughTheSharedNote()
    {
        var tool = ReadRepoFile("Lite/Mcp/McpMemoryTools.cs");

        Assert.Contains("return MemoryStatsPayload(resolved.ServerName, stats, engineEdition);", tool, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.WithMemoryNote(", tool, StringComparison.Ordinal);
    }

    // ── seeding ──

    private async Task SeedAsync(int engineEdition)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        /* The host's hardware, as sys.dm_os_sys_info reports it to a 1-vCore serverless General Purpose database. */
        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, service_objective, vcore_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(-487_004L); P(DateTime.UtcNow); P(ServerId); P("AzureMemoryScopeSrv"); P(engineEdition == 5 ? "SQL Azure" : "Enterprise Edition (64-bit)");
            P("12.0.2000.8"); P("RTM"); P(engineEdition); P(2); P(64); P(HostPhysicalMemoryMb); P(0); P(32);
            P(engineEdition == 5 ? "GP_S_Gen5_1" : DBNull.Value); P(engineEdition == 5 ? 1 : DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        /* What memory_stats holds for the same server: on an Azure SQL Database the database's own limit and counters. */
        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(-487_005L); P(DateTime.UtcNow); P(ServerId); P("AzureMemoryScopeSrv");
            P(DatabaseMemoryLimitMb); P(738); P(1_800); P(1_500); P(1_100);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
