/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.PlanAnalysis;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

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
/// <para>The pure rules are run here; the PostgreSQL reads are pinned as text (their statements are public constants, and the
/// readers that consume them are pinned by the lines that route them through the shared rule), and Lite.Tests runs the same
/// table against a real DuckDB in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseHostLeftoversTests
{
    private static AnalysisContext Context() => new()
    {
        ServerId = 7,
        ServerName = "LeftoversSrv",
        TimeRangeStart = new DateTime(2026, 9, 30, 0, 0, 0, DateTimeKind.Utc),
        TimeRangeEnd = new DateTime(2026, 9, 30, 1, 0, 0, DateTimeKind.Utc),
    };

    // ── 1. SERVER_HARDWARE fact, LPIM advisory, Server Context card ──

    [Fact]
    public void ServerHardwareFact_OnAzureSqlDatabase_CarriesTheVcoresAndHadrOnly_NotTheHostsTopologyOrMemory()
    {
        var fact = FactCollectorHelpers.BuildServerHardwareFact(
            Context(), hardwareIsTheHosts: true,
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
            Context(), hardwareIsTheHosts: true,
            cpuCount: 0, hyperthreadRatio: 64, physicalMemoryMb: 933_836, socketCount: 0, coresPerSocket: 32, hadrEnabled: false));
    }

    [Fact]
    public void ServerHardwareFact_OffAzureSqlDatabase_IsTheStoredTopologyAsItAlwaysWas()
    {
        var fact = FactCollectorHelpers.BuildServerHardwareFact(
            Context(), hardwareIsTheHosts: false,
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
            Context(), hardwareIsTheHosts: false,
            cpuCount: 0, hyperthreadRatio: 2, physicalMemoryMb: 65_536, socketCount: 2, coresPerSocket: 4, hadrEnabled: false));
    }

    [Fact]
    public void LpimAdvisory_OnAzureSqlDatabase_IsNotRaisedFromTheHostsMemory_AndIsUnchangedElsewhere()
    {
        var onAzure = new List<Fact>();
        FactCollectorHelpers.EmitServerHealthFacts(
            Context(), onAzure, "SQL Azure", physicalMemMb: 933_836,
            lockPagesInMemory: false, instantFileInit: null, memoryDumpCount: null, hardwareIsTheHosts: true);
        Assert.DoesNotContain(onAzure, f => f.Key == "CONFIG_LPIM_DISABLED");

        var onServer = new List<Fact>();
        FactCollectorHelpers.EmitServerHealthFacts(
            Context(), onServer, "Enterprise Edition (64-bit)", physicalMemMb: 933_836,
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

    [Fact]
    public void FactCollectorAndServerMetadataReader_AskTheSharedRule_ForTheEditionsHardware()
    {
        /* The PostgreSQL statement reads the edition beside the count and scopes the count in SQL, identically to Lite's DuckDB read. */
        Assert.Contains(
            "SELECT CASE WHEN engine_edition = 5 THEN vcore_count ELSE COALESCE(vcore_count, cpu_count) END AS cpu_count, hyperthread_ratio",
            PgFactCollector.ServerPropertiesSql, StringComparison.Ordinal);
        Assert.Contains("memory_dump_count, engine_edition", PgFactCollector.ServerPropertiesSql, StringComparison.Ordinal);

        var config = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.Config.cs");
        Assert.Contains("FactCollectorHelpers.BuildServerHardwareFact(", config, StringComparison.Ordinal);
        Assert.Contains("lpim, ifi, dumpCount, hardwareIsTheHosts);", config, StringComparison.Ordinal);
        Assert.DoesNotContain("[\"hyperthread_ratio\"] = htRatio", config, StringComparison.Ordinal);

        Assert.Contains("engine_edition, vcore_count", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("props.engine_edition, props.vcore_count", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingServerMetadataReader.cs");
        Assert.Contains("ServerHardwareScope.OwnCpuCount(engineEdition, storedCpuCount, vcoreCount) ?? 0", reader, StringComparison.Ordinal);
        Assert.Contains("ServerHardwareScope.OwnPhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb) ?? 0L", reader, StringComparison.Ordinal);
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

    [Fact]
    public void WorkerCeilingReads_ScopeTheCeilingToTheEdition_InAllThreePlaces()
    {
        /* Point-in-time read: the ceiling goes through the shared rule before the verdict and the card see it. */
        var utilization = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Utilization.cs");
        Assert.Contains(
            "var maxWorkers = ServerHardwareScope.OwnMaxWorkersCount(engineEdition, reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)));",
            utilization, StringComparison.Ordinal);

        /* 7-day trend: each day's ceiling is 0 on an Azure SQL Database, from the newest server_properties row's edition. */
        Assert.Contains("CASE WHEN s.engine_edition = 5 THEN 0 ELSE COALESCE(m.max_workers_count, 0) END", ViewerDataService.ProvisioningTrendSql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN server_info s ON true", ViewerDataService.ProvisioningTrendSql, StringComparison.Ordinal);
        Assert.Contains("SELECT engine_edition\n    FROM server_properties\n    WHERE server_id = $1", Lf(ViewerDataService.ProvisioningTrendSql), StringComparison.Ordinal);

        /* Fleet read: the latest ceiling per server is NULL on an Azure SQL Database, which the verdict reads as unknown. */
        Assert.Contains(
            "CASE WHEN props.engine_edition = 5 THEN NULL ELSE latest.max_workers_count END AS max_workers_count",
            ViewerDataService.ServerMetricsSql, StringComparison.Ordinal);
        Assert.Contains(") AS props ON true", ViewerDataService.ServerMetricsSql, StringComparison.Ordinal);
    }

    private static string Lf(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

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

    [Fact]
    public void FinOpsTab_AsksTheSharedRules_ForTheWorkerThreadsCard_TheHealthTooltip_AndTheInventoryCpuTerm()
    {
        var tab = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.Contains(
            "FinOpsWorkerThreadsText.Text = ServerHardwareScope.WorkerThreadsText(data.EngineEdition, data.CurrentWorkersCount, data.MaxWorkersCount);",
            tab, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"{data.CurrentWorkersCount:N0} / {data.MaxWorkersCount:N0}\"", tab, StringComparison.Ordinal);

        Assert.Contains(
            "FinOpsHealthScoreBorder.ToolTip = data.HasCpuSample ? null : ServerHardwareScope.HealthScoreWithoutCpuNote;",
            tab, StringComparison.Ordinal);

        Assert.Contains(
            "int? cpuScore = item.AvgCpuPct is decimal avgCpu ? FinOpsHealthCalculator.CpuScore(avgCpu) : null;",
            tab, StringComparison.Ordinal);
        Assert.DoesNotContain("item.AvgCpuPct ?? 0m", tab, StringComparison.Ordinal);
    }

    // ── 3. memory utilization: unchanged, it is memory_stats ──

    [Fact]
    public void MemoryUtilization_IsStillComputedFromMemoryStats_NotFromTheHostsPhysicalMemory()
    {
        /* The read is the memory_stats snapshot, which the collector fills from the database's own committed target on an
           Azure SQL Database (1,838 MB for a 1-vCore General Purpose database), so dividing by it is database-scoped. */
        Assert.Contains("FROM v_memory_stats", DarlingDataReader.LatestMemoryStatsSql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_properties", DarlingDataReader.LatestMemoryStatsSql, StringComparison.Ordinal);

        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.Contains(
            "? (stats.TotalPhysicalMemoryMb - stats.AvailablePhysicalMemoryMb) / stats.TotalPhysicalMemoryMb * 100",
            tool, StringComparison.Ordinal);
        Assert.Contains("memory_utilization_pct = Math.Round(utilization, 1),", tool, StringComparison.Ordinal);
    }
}
