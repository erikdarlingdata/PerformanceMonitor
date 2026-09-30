/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// What <see cref="AzureSqlDatabaseHostMathTests"/> left: on an Azure SQL Database (engine edition 5) <c>sys.dm_os_sys_info</c>
/// describes the HOST, so the stored <c>server_properties.cpu_count</c>, <c>hyperthread_ratio</c>, <c>socket_count</c>,
/// <c>cores_per_socket</c> and <c>physical_memory_mb</c> are the host's, and so is the <c>max_workers_count</c> that
/// <c>memory_stats</c> copies from the same DMV. Nothing may compute from them there. Where the database has its own figure
/// (<c>vcore_count</c>, parsed from the service objective) that is used; otherwise the answer is not applicable.
///
/// <para>The four things pinned here, each with an edition 5 case that has vCores, an edition 5 case without them (a DTU
/// objective), and the SQL Server / Managed Instance twin that must not move:</para>
/// <list type="number">
/// <item>the SERVER_HARDWARE analysis fact, the LPIM advisory that reads its memory, and the plan Server Context card;</item>
/// <item>the FinOps Worker Threads card and the worker ceiling the provisioning verdict reads;</item>
/// <item>the memory utilization percentage, which is NOT changed: it divides <c>memory_stats</c> columns, which the collector
/// fills from the database's own committed target, not from <c>server_properties.physical_memory_mb</c>;</item>
/// <item>the FinOps health score's CPU term, which is left out when the window holds no CPU sample (any edition).</item>
/// </list>
///
/// <para><c>memory_stats</c> seeds on edition 5 use what the collector stores there: the database's own memory limit
/// (1,838 MB for a 1-vCore General Purpose database), never the host's 911.9 GB. The Darling.Tests twin pins the same table for
/// the other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseHostLeftoversTests
{
    // ── 1. SERVER_HARDWARE fact, LPIM advisory, Server Context card ──

    [Fact]
    public void ServerHardwareFact_OnAzureSqlDatabase_CarriesTheVcoresAndHadrOnly_NotTheHostsTopologyOrMemory()
    {
        var fact = FactCollectorHelpers.BuildServerHardwareFact(
            TestDataSeeder.CreateTestContext(), hardwareIsTheHosts: true,
            cpuCount: 1, hyperthreadRatio: 64, physicalMemoryMb: 933_836, socketCount: 0, coresPerSocket: 32, hadrEnabled: false);

        Assert.NotNull(fact);
        Assert.Equal("SERVER_HARDWARE", fact!.Key);
        Assert.Equal("config", fact.Source);
        Assert.Equal(1, fact.Value);
        Assert.Equal(new[] { "cpu_count", "hadr_enabled" }, fact.Metadata.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal(1, fact.Metadata["cpu_count"]);
        Assert.Equal(0, fact.Metadata["hadr_enabled"]);
    }

    [Fact]
    public void ServerHardwareFact_OnAzureSqlDatabaseWithNoVcores_IsNotEmitted()
    {
        Assert.Null(FactCollectorHelpers.BuildServerHardwareFact(
            TestDataSeeder.CreateTestContext(), hardwareIsTheHosts: true,
            cpuCount: 0, hyperthreadRatio: 64, physicalMemoryMb: 933_836, socketCount: 0, coresPerSocket: 32, hadrEnabled: false));
    }

    [Fact]
    public void ServerHardwareFact_OffAzureSqlDatabase_IsTheStoredTopologyAsItAlwaysWas()
    {
        var fact = FactCollectorHelpers.BuildServerHardwareFact(
            TestDataSeeder.CreateTestContext(), hardwareIsTheHosts: false,
            cpuCount: 16, hyperthreadRatio: 2, physicalMemoryMb: 65_536, socketCount: 2, coresPerSocket: 4, hadrEnabled: true);

        Assert.NotNull(fact);
        Assert.Equal(16, fact!.Value);
        Assert.Equal(
            new[] { "cpu_count", "hyperthread_ratio", "physical_memory_mb", "socket_count", "cores_per_socket", "hadr_enabled" },
            fact.Metadata.Keys.ToArray());
        Assert.Equal(16, fact.Metadata["cpu_count"]);
        Assert.Equal(2, fact.Metadata["hyperthread_ratio"]);
        Assert.Equal(65_536, fact.Metadata["physical_memory_mb"]);
        Assert.Equal(2, fact.Metadata["socket_count"]);
        Assert.Equal(4, fact.Metadata["cores_per_socket"]);
        Assert.Equal(1, fact.Metadata["hadr_enabled"]);

        /* No CPU count at all is no fact, on any edition, as before. */
        Assert.Null(FactCollectorHelpers.BuildServerHardwareFact(
            TestDataSeeder.CreateTestContext(), hardwareIsTheHosts: false,
            cpuCount: 0, hyperthreadRatio: 2, physicalMemoryMb: 65_536, socketCount: 2, coresPerSocket: 4, hadrEnabled: false));
    }

    [Fact]
    public void LpimAdvisory_OnAzureSqlDatabase_IsNotRaisedFromTheHostsMemory_AndIsUnchangedElsewhere()
    {
        var context = TestDataSeeder.CreateTestContext();

        var onAzure = new List<Fact>();
        FactCollectorHelpers.EmitServerHealthFacts(
            context, onAzure, "SQL Azure", physicalMemMb: 933_836,
            lockPagesInMemory: false, instantFileInit: null, memoryDumpCount: null, hardwareIsTheHosts: true);
        Assert.DoesNotContain(onAzure, f => f.Key == "CONFIG_LPIM_DISABLED");

        var onServer = new List<Fact>();
        FactCollectorHelpers.EmitServerHealthFacts(
            context, onServer, "Enterprise Edition (64-bit)", physicalMemMb: 933_836,
            lockPagesInMemory: false, instantFileInit: null, memoryDumpCount: null);
        var lpim = Assert.Single(onServer, f => f.Key == "CONFIG_LPIM_DISABLED");
        Assert.Equal(933_836, lpim.Metadata["physical_memory_mb"]);
    }

    [Fact]
    public void ServerContextCard_ShowsTheVcoresWithNoRam_WhenNoMemoryFigureIsCarried_AndTheRamOtherwise()
    {
        static string? Hardware(ServerMetadata metadata) =>
            ServerContextCard.Rows(metadata).SingleOrDefault(r => r.Label == "Hardware").Value;

        Assert.Equal("1 CPUs", Hardware(new ServerMetadata { Edition = "SQL Azure", CpuCount = 1, PhysicalMemoryMB = 0 }));
        Assert.Null(Hardware(new ServerMetadata { Edition = "SQL Azure", CpuCount = 0, PhysicalMemoryMB = 0 }));
        Assert.Equal(
            string.Format(CultureInfo.CurrentCulture, "8 CPUs, {0:N0} MB RAM", 65_536L),
            Hardware(new ServerMetadata { Edition = "Enterprise Edition (64-bit)", CpuCount = 8, PhysicalMemoryMB = 65_536 }));
    }

    // ── 2. Worker Threads card and the worker ceiling ──

    [Theory]
    [InlineData(5, 512, 0)]      // Azure SQL Database: the ceiling follows the host's CPUs, so none is carried
    [InlineData(3, 512, 512)]    // SQL Server: as stored
    [InlineData(8, 2560, 2560)]  // Managed Instance: as stored
    [InlineData(1, 512, 512)]
    [InlineData(null, 512, 512)] // edition not read: as stored
    public void OwnMaxWorkersCount_IsNoneOnAzureSqlDatabase_AndTheStoredCeilingEverywhereElse(int? engineEdition, int stored, int expected)
    {
        Assert.Equal(expected, ServerHardwareScope.OwnMaxWorkersCount(engineEdition, stored));
    }

    [Fact]
    public void OwnPhysicalMemoryMb_IsNotApplicableOnAzureSqlDatabase_AndTheStoredFigureEverywhereElse()
    {
        Assert.Null(ServerHardwareScope.OwnPhysicalMemoryMb(5, 933_836));
        Assert.Null(ServerHardwareScope.OwnPhysicalMemoryMb(5, null));
        Assert.Equal(65_536L, ServerHardwareScope.OwnPhysicalMemoryMb(3, 65_536));
        Assert.Equal(65_536L, ServerHardwareScope.OwnPhysicalMemoryMb(8, 65_536));
        Assert.Equal(65_536L, ServerHardwareScope.OwnPhysicalMemoryMb(null, 65_536));
        Assert.Null(ServerHardwareScope.OwnPhysicalMemoryMb(3, null));
    }

    [Fact]
    public void WorkerThreadsText_IsNotApplicableOnAzureSqlDatabase_AndInUseOverMaximumEverywhereElse()
    {
        Assert.Equal("n/a", ServerHardwareScope.WorkerThreadsText(5, 0, 512));
        Assert.Equal("n/a", ServerHardwareScope.WorkerThreadsText(5, 450, 512));
        Assert.Equal("n/a", ServerHardwareScope.WorkerThreadsText(5, 0, 0));

        foreach (var engineEdition in new int?[] { 1, 2, 3, 4, 8, null })
        {
            Assert.Equal(
                string.Format(CultureInfo.CurrentCulture, "{0:N0} / {1:N0}", 1_200, 2_560),
                ServerHardwareScope.WorkerThreadsText(engineEdition, 1_200, 2_560));
        }
    }

    // ── 4. Health score: the CPU term ──

    [Fact]
    public void Overall_WithNoCpuScore_WeighsMemoryAndStorageOverTheirOwnSixtyPercent()
    {
        Assert.Equal(100, FinOpsHealthCalculator.Overall(null, 100, 100));
        Assert.Equal(0, FinOpsHealthCalculator.Overall(null, 0, 0));
        Assert.Equal(80, FinOpsHealthCalculator.Overall(null, 60, 100));
        Assert.Equal(60, FinOpsHealthCalculator.Overall(null, 80, 40));
    }

    [Fact]
    public void Overall_WithEveryTerm_IsTheLongStandingArithmetic()
    {
        Assert.Equal(86, FinOpsHealthCalculator.Overall(95, 60, 100));
        Assert.Equal(62, FinOpsHealthCalculator.Overall(80, 60, 40));
        Assert.Equal(100, FinOpsHealthCalculator.Overall(100, 100, 100));
        Assert.Equal(0, FinOpsHealthCalculator.Overall(0, 0, 0));
    }

    /// <summary>Buffer pool 10% of physical memory scores 60 and 50% free storage scores 100, so the CPU term is the only one
    /// that can tell a measured window from an empty one.</summary>
    private static UtilizationEfficiencyRow Window(int engineEdition, string provisioningStatus, decimal p95CpuPct) => new()
    {
        EngineEdition = engineEdition,
        ProvisioningStatus = provisioningStatus,
        P95CpuPct = p95CpuPct,
        PhysicalMemoryMb = 100_000,
        BufferPoolMb = 10_000,
        FreeSpacePct = 50m,
    };

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(8)]
    public void HealthScore_WithNoCpuSample_LeavesTheCpuTermOut_AndDoesNotScoreTheZeroItReadsAs(int engineEdition)
    {
        /* With the CPU term: 100 * 0.4 + 60 * 0.3 + 100 * 0.3 = 88. Without it: memory and storage over their own 60%. */
        var noSample = Window(engineEdition, provisioningStatus: "", p95CpuPct: 0m);

        Assert.False(noSample.HasCpuSample);
        Assert.Equal(80, noSample.ComputeHealthScore());
        Assert.Equal(FinOpsHealthCalculator.Overall(null, 60, 100), noSample.ComputeHealthScore());

        /* Whatever the P95 field holds, an empty window's CPU is not scored. */
        Assert.Equal(80, Window(engineEdition, "", 95m).ComputeHealthScore());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(8)]
    public void HealthScore_WithACpuSample_IsTheLongStandingScore(int engineEdition)
    {
        /* 95 * 0.4 + 60 * 0.3 + 100 * 0.3 = 86, whatever verdict the window earned. */
        foreach (var status in new[] { ProvisioningVerdict.RightSized, ProvisioningVerdict.OverProvisioned, ProvisioningVerdict.UnderProvisioned })
        {
            var measured = Window(engineEdition, status, 7m);
            Assert.True(measured.HasCpuSample);
            Assert.Equal(86, measured.ComputeHealthScore());
        }

        /* A genuinely idle measured window (p95 of 0) still scores its 100, which is the point of telling it from no sample. */
        Assert.Equal(88, Window(engineEdition, ProvisioningVerdict.OverProvisioned, 0m).ComputeHealthScore());
    }

    // ── the wiring, pinned at the source ──

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
            dir = Path.GetDirectoryName(dir);

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }

    [Fact]
    public void FinOpsTab_AsksTheSharedRules_ForTheWorkerThreadsCard_TheHealthTooltip_AndTheInventoryCpuTerm()
    {
        var tab = ReadRepoFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.Contains(
            "WorkerThreadsText.Text = ServerHardwareScope.WorkerThreadsText(data.EngineEdition, data.CurrentWorkersCount, data.MaxWorkersCount);",
            tab, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"{data.CurrentWorkersCount:N0} / {data.MaxWorkersCount:N0}\"", tab, StringComparison.Ordinal);

        Assert.Contains(
            "HealthScoreBorder.ToolTip = data.HasCpuSample ? null : ServerHardwareScope.HealthScoreWithoutCpuNote;",
            tab, StringComparison.Ordinal);

        Assert.Contains(
            "int? cpuScore = item.AvgCpuPct is decimal avgCpu ? FinOpsHealthCalculator.CpuScore(avgCpu) : null;",
            tab, StringComparison.Ordinal);
        Assert.DoesNotContain("item.AvgCpuPct ?? 0m", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void WorkerCeilingReads_ScopeTheCeilingToTheEdition_InAllThreePlaces()
    {
        var utilization = ReadRepoFile("Lite/Services/LocalDataService.FinOps.Utilization.cs");
        var fleet = ReadRepoFile("Lite/Services/LocalDataService.FinOps.ServerProperties.cs");

        Assert.Contains(
            "var maxWorkers = ServerHardwareScope.OwnMaxWorkersCount(engineEdition, reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)));",
            utilization, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN s.engine_edition = 5 THEN 0 ELSE COALESCE(m.max_workers_count, 0) END", utilization, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN server_info s ON true", utilization, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN props.engine_edition = 5 THEN NULL ELSE latest.max_workers_count END AS max_workers_count", fleet, StringComparison.Ordinal);
        Assert.Contains(") AS props ON true", fleet, StringComparison.Ordinal);
    }

    [Fact]
    public void FactCollectorAndPlanReaders_AskTheSharedRule_ForTheEditionsHardware()
    {
        var config = ReadRepoFile("Lite/Analysis/DuckDbFactCollector.Config.cs");
        Assert.Contains("lock_pages_in_memory, instant_file_initialization_enabled, memory_dump_count, engine_edition", config, StringComparison.Ordinal);
        Assert.Contains("FactCollectorHelpers.BuildServerHardwareFact(", config, StringComparison.Ordinal);
        Assert.Contains("lpim, ifi, dumpCount, hardwareIsTheHosts);", config, StringComparison.Ordinal);
        Assert.DoesNotContain("[\"hyperthread_ratio\"] = htRatio", config, StringComparison.Ordinal);

        var planMetadata = ReadRepoFile("Lite/Services/LocalDataService.PlanServerMetadata.cs");
        var drillDown = ReadRepoFile("Lite/Analysis/DrillDownCollector.Plans.cs");
        foreach (var source in new[] { planMetadata, drillDown })
        {
            Assert.Contains("ServerHardwareScope.OwnCpuCount(engineEdition, storedCpuCount, vcoreCount) ?? 0", source, StringComparison.Ordinal);
            Assert.Contains("ServerHardwareScope.OwnPhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb) ?? 0L", source, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void MemoryUtilization_IsStillComputedFromMemoryStats_NotFromTheHostsPhysicalMemory()
    {
        var read = ReadRepoFile("Lite/Services/LocalDataService.Memory.cs");
        Assert.Contains("FROM v_memory_stats", read, StringComparison.Ordinal);
        Assert.Contains(
            "public double MemoryUtilizationPercent => TotalPhysicalMemoryMb > 0 ? UsedPhysicalMemoryMb / TotalPhysicalMemoryMb * 100 : 0;",
            read, StringComparison.Ordinal);

        var tool = ReadRepoFile("Lite/Mcp/McpMemoryTools.cs");
        Assert.Contains("memory_utilization_pct = Math.Round(stats.MemoryUtilizationPercent, 1),", tool, StringComparison.Ordinal);
    }
}

/// <summary>
/// The seeded half of <see cref="AzureSqlDatabaseHostLeftoversTests"/>: real DuckDB reads against rows seeded the way the
/// collectors store them.
/// </summary>
public sealed class AzureSqlDatabaseHostLeftoversReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -488_101;
    private readonly SharedDuckDbFixture _fixture;
    private readonly DateTime _now = DateTime.UtcNow;
    private DuckDBConnection? _seedConn;
    private long _nextId = -488_101_000;

    public AzureSqlDatabaseHostLeftoversReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
    }

    public void Dispose() => _seedConn?.Dispose();

    // ── 1. SERVER_HARDWARE fact, end to end through the collector ──

    private async Task<Dictionary<string, Fact>> CollectFactsAsync(int engineEdition, int? vcoreCount)
    {
        using var seeder = new TestDataSeeder(_fixture.DuckDb);
        /* An Azure SQL Database as the collector stores it: the HOST's 2 logical CPUs, hyperthread ratio 64, 0 sockets, 32
           cores per socket and 911.9 GB, plus the service objective's vCores. */
        await seeder.SeedServerPropertiesAsync(
            cpuCount: 2, htRatio: 64, physicalMemMb: 933_836, socketCount: 0, coresPerSocket: 32,
            edition: "SQL Azure", engineEdition: engineEdition,
            serviceObjective: vcoreCount.HasValue ? "GP_S_Gen5_" + vcoreCount : "S0", vcoreCount: vcoreCount);

        var facts = await new DuckDbFactCollector(_fixture.DuckDb).CollectFactsAsync(TestDataSeeder.CreateTestContext());
        return facts.ToDictionary(f => f.Key, f => f);
    }

    [Fact]
    public async Task Collector_OnAzureSqlDatabaseWithVcores_EmitsTheVcoresAndNoHostTopologyOrMemory()
    {
        var facts = await CollectFactsAsync(engineEdition: 5, vcoreCount: 1);

        var hardware = Assert.Contains("SERVER_HARDWARE", facts);
        Assert.Equal(1, hardware.Value);
        Assert.Equal(new[] { "cpu_count", "hadr_enabled" }, hardware.Metadata.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("physical_memory_mb"));
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("cores_per_socket"));
    }

    [Fact]
    public async Task Collector_OnAzureSqlDatabaseWithNoVcores_EmitsNoServerHardwareFact_AndNothingFromTheHost()
    {
        var facts = await CollectFactsAsync(engineEdition: 5, vcoreCount: null);

        Assert.DoesNotContain("SERVER_HARDWARE", facts.Keys);
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("physical_memory_mb"));
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("cores_per_socket"));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task Collector_OnEveryOtherEdition_EmitsTheStoredTopologyAsItAlwaysDid(int engineEdition)
    {
        using var seeder = new TestDataSeeder(_fixture.DuckDb);
        await seeder.SeedServerPropertiesAsync(
            cpuCount: 16, htRatio: 2, physicalMemMb: 65_536, socketCount: 2, coresPerSocket: 4, engineEdition: engineEdition);

        var facts = await new DuckDbFactCollector(_fixture.DuckDb).CollectFactsAsync(TestDataSeeder.CreateTestContext());
        var hardware = Assert.Single(facts, f => f.Key == "SERVER_HARDWARE");

        Assert.Equal(16, hardware.Value);
        Assert.Equal(2, hardware.Metadata["hyperthread_ratio"]);
        Assert.Equal(65_536, hardware.Metadata["physical_memory_mb"]);
        Assert.Equal(2, hardware.Metadata["socket_count"]);
        Assert.Equal(4, hardware.Metadata["cores_per_socket"]);
        Assert.Equal(0, hardware.Metadata["hadr_enabled"]);
    }

    // ── 1. plan Server Context metadata: the two DuckDB readers ──

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedServerPropertiesAsync(int serverId, int engineEdition, int storedCpuCount, long storedPhysicalMemoryMb, int? vcoreCount)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, service_objective, vcore_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId--); P(_now); P(serverId); P("LeftoversSrv" + serverId);
        P(engineEdition == 5 ? "SQL Azure" : "Enterprise Edition (64-bit)"); P("12.0.2000.8"); P("RTM");
        P(engineEdition); P(storedCpuCount); P(64); P(storedPhysicalMemoryMb); P(0); P(32);
        P(vcoreCount.HasValue ? "GP_S_Gen5_" + vcoreCount : (engineEdition == 5 ? "S0" : null)); P(vcoreCount);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The database's vCores, never the host's CPU count or memory.</summary>
    public static IEnumerable<object?[]> PlanMetadataCases() =>
    [
        [5, 2, 933_836L, 1, 1, 0L],          // Azure SQL Database with vCores: the vCores, no RAM
        [5, 2, 933_836L, null, 0, 0L],       // Azure SQL Database, DTU objective: no CPU count, no RAM (the card drops its Hardware row)
        [3, 8, 65_536L, null, 8, 65_536L],   // SQL Server: as stored
        [8, 8, 65_536L, null, 8, 65_536L],   // Managed Instance: as stored
    ];

    [Theory]
    [MemberData(nameof(PlanMetadataCases))]
    public async Task PlanMetadata_LocalDataServiceRead_CarriesTheDatabasesOwnHardware(
        int engineEdition, int storedCpuCount, long storedPhysicalMemoryMb, int? vcoreCount, int expectedCpuCount, long expectedPhysicalMemoryMb)
    {
        await SeedServerPropertiesAsync(ServerId, engineEdition, storedCpuCount, storedPhysicalMemoryMb, vcoreCount);

        var metadata = await new LocalDataService(_fixture.DuckDb).GetServerMetadataForPlanAnalysisAsync(ServerId);

        Assert.NotNull(metadata);
        Assert.Equal(expectedCpuCount, metadata!.CpuCount);
        Assert.Equal(expectedPhysicalMemoryMb, metadata.PhysicalMemoryMB);
    }

    [Theory]
    [MemberData(nameof(PlanMetadataCases))]
    public async Task PlanMetadata_DrillDownRead_CarriesTheDatabasesOwnHardware(
        int engineEdition, int storedCpuCount, long storedPhysicalMemoryMb, int? vcoreCount, int expectedCpuCount, long expectedPhysicalMemoryMb)
    {
        await SeedServerPropertiesAsync(ServerId, engineEdition, storedCpuCount, storedPhysicalMemoryMb, vcoreCount);

        var read = typeof(DrillDownCollector).GetMethod("ReadServerMetadataForPlanAnalysisAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var metadata = await (Task<ServerMetadata?>)read.Invoke(new DrillDownCollector(_fixture.DuckDb), [ServerId, CancellationToken.None])!;

        Assert.NotNull(metadata);
        Assert.Equal(expectedCpuCount, metadata!.CpuCount);
        Assert.Equal(expectedPhysicalMemoryMb, metadata.PhysicalMemoryMB);
    }

    // ── 2. worker ceiling: point-in-time read, 7-day trend, fleet read ──

    /// <summary>One server with a measured CPU window (average and p95 50%, so the verdict is RIGHT_SIZED unless something else
    /// decides it) and the memory_stats the collector stores: on an Azure SQL Database the database's own memory limit
    /// (1,838 MB for a 1-vCore General Purpose database), a ceiling of 512 workers copied from the host-derived DMV, and NO
    /// in-use count (the collector stores NULL there).</summary>
    private async Task SeedServerWithMemoryAsync(int serverId, int engineEdition, int? vcoreCount, int? currentWorkers)
    {
        await SeedServerPropertiesAsync(serverId, engineEdition, storedCpuCount: 2, storedPhysicalMemoryMb: engineEdition == 5 ? 933_836 : 65_536, vcoreCount);

        using var readLock = _fixture.DuckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $2, 50, 2)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = _now });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "LeftoversSrv" + serverId });
            await cmd.ExecuteNonQueryAsync();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 512, $10)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            var physical = engineEdition == 5 ? 1_838d : 65_536d;
            P(_nextId--); P(_now); P(serverId); P("LeftoversSrv" + serverId);
            P(physical); P(physical / 2); P(physical * 0.9); P(physical * 0.8); P(physical * 0.5); P(currentWorkers);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>(edition, vCores, in-use workers stored, expected ceiling, expected verdict).</summary>
    public static IEnumerable<object?[]> WorkerCeilingCases() =>
    [
        [5, 1, null, 0, "RIGHT_SIZED"],             // Azure SQL Database with vCores, as stored: no in-use count, no ceiling
        [5, null, null, 0, "RIGHT_SIZED"],          // Azure SQL Database, DTU objective
        [5, 1, 450, 0, "RIGHT_SIZED"],              // even an in-use count against the host-derived 512 is not saturation
        [3, null, 450, 512, "UNDER_PROVISIONED"],   // SQL Server: 450 of 512 is worker saturation, as it always was
        [8, null, 450, 512, "UNDER_PROVISIONED"],   // Managed Instance: the same
        [3, null, 40, 512, "RIGHT_SIZED"],
    ];

    [Theory]
    [MemberData(nameof(WorkerCeilingCases))]
    public async Task UtilizationRead_CarriesNoWorkerCeilingOnAzureSqlDatabase_AndNeverCallsItSaturated(
        int engineEdition, int? vcoreCount, int? currentWorkers, int expectedCeiling, string expectedVerdict)
    {
        await SeedServerWithMemoryAsync(ServerId, engineEdition, vcoreCount, currentWorkers);

        var row = await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(engineEdition, row!.EngineEdition);
        Assert.Equal(expectedCeiling, row.MaxWorkersCount);
        Assert.Equal(currentWorkers ?? 0, row.CurrentWorkersCount);
        Assert.Equal(expectedVerdict, row.ProvisioningStatus);
        Assert.Equal(
            engineEdition == 5 ? "n/a" : string.Format(CultureInfo.CurrentCulture, "{0:N0} / {1:N0}", currentWorkers ?? 0, 512),
            ServerHardwareScope.WorkerThreadsText(row.EngineEdition, row.CurrentWorkersCount, row.MaxWorkersCount));
    }

    [Theory]
    [MemberData(nameof(WorkerCeilingCases))]
    public async Task TrendRead_CarriesNoWorkerCeilingOnAzureSqlDatabase_AndNeverCallsADaySaturated(
        int engineEdition, int? vcoreCount, int? currentWorkers, int expectedCeiling, string expectedVerdict)
    {
        _ = expectedCeiling;
        await SeedServerWithMemoryAsync(ServerId, engineEdition, vcoreCount, currentWorkers);

        var days = await new LocalDataService(_fixture.DuckDb).GetProvisioningTrendAsync(ServerId);

        var day = Assert.Single(days);
        Assert.Equal(expectedVerdict, day.Status);
    }

    [Fact]
    public async Task FleetRead_CarriesNoWorkerCeilingOnAzureSqlDatabase_AndNeverCallsAServerSaturated()
    {
        const int azureVcores = ServerId - 1;
        const int azureDtu = ServerId - 2;
        const int sqlServer = ServerId - 3;
        const int managedInstance = ServerId - 4;
        await SeedServerWithMemoryAsync(azureVcores, engineEdition: 5, vcoreCount: 1, currentWorkers: 450);
        await SeedServerWithMemoryAsync(azureDtu, engineEdition: 5, vcoreCount: null, currentWorkers: 450);
        await SeedServerWithMemoryAsync(sqlServer, engineEdition: 3, vcoreCount: null, currentWorkers: 450);
        await SeedServerWithMemoryAsync(managedInstance, engineEdition: 8, vcoreCount: null, currentWorkers: 450);

        var metrics = await new LocalDataService(_fixture.DuckDb).GetServerMetricsAsync();

        Assert.Equal("RIGHT_SIZED", metrics[azureVcores].ProvisioningStatus);
        Assert.Equal("RIGHT_SIZED", metrics[azureDtu].ProvisioningStatus);
        Assert.Equal("UNDER_PROVISIONED", metrics[sqlServer].ProvisioningStatus);
        Assert.Equal("UNDER_PROVISIONED", metrics[managedInstance].ProvisioningStatus);
    }

    // ── 3. memory utilization: unchanged, it is memory_stats ──

    [Theory]
    [InlineData(5, 1_838, 1_000)]     // Azure SQL Database: the database's own memory limit, as the collector stores it
    [InlineData(3, 65_536, 16_384)]   // SQL Server
    [InlineData(8, 65_536, 16_384)]   // Managed Instance
    public async Task MemoryUtilization_IsComputedFromMemoryStats_OnEveryEdition_AndNeverFromTheHostsPhysicalMemory(
        int engineEdition, double totalMb, double availableMb)
    {
        /* server_properties carries the HOST's 911.9 GB on an Azure SQL Database. Were the percentage divided by it, the
           figure would be ~100%; it is divided by the memory_stats total, so it is what it always was. */
        await SeedServerPropertiesAsync(ServerId, engineEdition, storedCpuCount: 2, storedPhysicalMemoryMb: 933_836, vcoreCount: engineEdition == 5 ? 1 : null);

        using (var readLock = _fixture.DuckDb.AcquireReadLock())
        {
            var conn = await SeedConnectionAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, $4, $5, $6, $5, $5, $6)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(_nextId--); P(_now); P(ServerId); P("LeftoversSrv"); P(totalMb); P(availableMb);
            await cmd.ExecuteNonQueryAsync();
        }

        var stats = await new LocalDataService(_fixture.DuckDb).GetLatestMemoryStatsAsync(ServerId);

        Assert.NotNull(stats);
        Assert.Equal(totalMb, stats!.TotalPhysicalMemoryMb);
        Assert.Equal((totalMb - availableMb) / totalMb * 100, stats.MemoryUtilizationPercent, precision: 6);
        Assert.InRange(stats.MemoryUtilizationPercent, 1, 99);
    }
}
