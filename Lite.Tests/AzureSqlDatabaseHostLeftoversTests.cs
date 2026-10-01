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
/// What <see cref="AzureSqlDatabaseHostMathTests"/> left. On an Azure SQL Database (engine edition 5) <c>sys.dm_os_sys_info</c>
/// returns four figures that describe the HOST machine and not the database: <c>physical_memory_mb</c> (about 912 GB),
/// <c>socket_count</c>, <c>cores_per_socket</c> and <c>hyperthread_ratio</c>. Nothing may compute from them there. The other two
/// columns it fills are the database's own: <c>cpu_count</c> is the number of schedulers the database can see (a 1-vCore General
/// Purpose database reads 2, and the count can be higher than the vCores) and <c>max_workers_count</c> is the database's own worker
/// ceiling, so both are shown as stored. What a CPU percent, a CPU count shown beside one, and a recommended MAXDOP are taken from
/// is NOT <c>cpu_count</c>: it is the vCores the service objective gives the database (<c>vcore_count</c>), and an objective that
/// names none (a DTU-model objective or an elastic pool) has no such count, so those answers read n/a.
///
/// <para>Pinned here, each with an edition 5 case that has vCores, an edition 5 case without them, and the SQL Server / Managed
/// Instance twin that must not move:</para>
/// <list type="number">
/// <item>the SERVER_HARDWARE analysis fact (the vCores and none of the host's figures), the MAXDOP recommendation read from it, the
/// LPIM advisory that reads its memory, and the plan Server Context card;</item>
/// <item>the FinOps Worker Threads card: the in-use count is n/a where the collector could not read it (NULL), never 0, and the
/// ceiling is shown as stored;</item>
/// <item>the memory utilization percentage, which is NOT changed: it divides <c>memory_stats</c> columns, which the collector
/// fills from the database's own committed target, not from <c>server_properties.physical_memory_mb</c>;</item>
/// <item>the FinOps health score's CPU term, which is left out when the window holds no CPU sample (any edition);</item>
/// <item>the words over the Memory tab's first two figures, which on an Azure SQL Database are the database's memory limit and the
/// room left under it.</item>
/// </list>
///
/// <para><c>memory_stats</c> seeds on edition 5 use what the collector stores there: the database's own memory limit
/// (1,838 MB for a 1-vCore General Purpose database), never the host's 911.9 GB. The Darling.Tests twin pins the same table for
/// the other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseHostLeftoversTests
{
    // ── 1. SERVER_HARDWARE fact, MAXDOP recommendation, LPIM advisory, Server Context card ──

    private static Fact HardwareFact(bool azureSqlDatabase, int cpuCount, int coresPerSocket) =>
        FactCollectorHelpers.BuildServerHardwareFact(
            TestDataSeeder.CreateTestContext(), hardwareIsTheHosts: azureSqlDatabase,
            cpuCount: cpuCount, hyperthreadRatio: 64, physicalMemoryMb: 933_836, socketCount: 0, coresPerSocket: coresPerSocket,
            hadrEnabled: false)!;

    private static Dictionary<string, Fact> Facts(params Fact[] facts) => facts.ToDictionary(f => f.Key, f => f);

    private static Fact Config(string key, double value) => new() { Source = "config", Key = key, Value = value };

    [Fact]
    public void ServerHardwareFact_OnAzureSqlDatabase_CarriesTheVcoresAndHadrOnly_NotTheHostsTopologyOrMemory()
    {
        /* The host's four figures go in (hyperthread ratio 64, 933,836 MB, 0 sockets, 32 cores per socket); none may come out. */
        var fact = FactCollectorHelpers.BuildServerHardwareFact(
            TestDataSeeder.CreateTestContext(), hardwareIsTheHosts: true,
            cpuCount: 4, hyperthreadRatio: 64, physicalMemoryMb: 933_836, socketCount: 0, coresPerSocket: 32, hadrEnabled: false);

        Assert.NotNull(fact);
        Assert.Equal("SERVER_HARDWARE", fact!.Key);
        Assert.Equal("config", fact.Source);
        Assert.Equal(4, fact.Value);
        Assert.Equal(
            new[] { "cpu_count", "hadr_enabled", "vcore_count" },
            fact.Metadata.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal(4, fact.Metadata["cpu_count"]);
        Assert.Equal(4, fact.Metadata["vcore_count"]);
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

    /// <summary>The recommended MAXDOP on an Azure SQL Database follows its vCores. The host's 32 cores per socket go into the
    /// fact builder every time, so a fact that carried them would make every row here read 8.</summary>
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(4, 4)]
    [InlineData(8, 8)]
    [InlineData(16, 8)]
    [InlineData(80, 8)]
    public void MaxdopBasis_OnAzureSqlDatabase_FollowsTheVcores_NeverTheHostsCoresPerSocket(int vcores, int expectedMaxdop)
    {
        var basis = FactRemediation.MaxdopBasisFrom(Facts(HardwareFact(azureSqlDatabase: true, cpuCount: vcores, coresPerSocket: 32)));

        Assert.True(basis.FromVcores);
        Assert.Equal(vcores, basis.Cores);
        Assert.Equal($"({vcores} vCores)", basis.Note);
        Assert.Equal(expectedMaxdop, FactRemediation.RecommendedMaxdop(basis.Cores));
    }

    [Fact]
    public void MaxdopBasis_OffAzureSqlDatabase_IsTheCoresPerSocketAsItAlwaysWas()
    {
        var basis = FactRemediation.MaxdopBasisFrom(Facts(HardwareFact(azureSqlDatabase: false, cpuCount: 16, coresPerSocket: 4)));

        Assert.False(basis.FromVcores);
        Assert.Equal(4, basis.Cores);
        Assert.Equal("(cores per socket 4)", basis.Note);
        Assert.Equal(4, FactRemediation.RecommendedMaxdop(basis.Cores));
        Assert.Equal(8, FactRemediation.RecommendedMaxdop(FactRemediation.MaxdopBasisFrom(
            Facts(HardwareFact(azureSqlDatabase: false, cpuCount: 64, coresPerSocket: 32))).Cores));
    }

    /// <summary>A DTU-model objective or an elastic pool gives the fact builder no vCores, so there is no fact: no basis, no figure
    /// to state, and the recommendation is the long-standing cap of 8 with nothing said about cores.</summary>
    [Fact]
    public void MaxdopBasis_OnAzureSqlDatabaseWithNoVcores_IsNotApplicable_AndTheAdviceIsTheStaticCap()
    {
        var basis = FactRemediation.MaxdopBasisFrom(Facts());

        Assert.Equal(0, basis.Cores);
        Assert.False(basis.FromVcores);
        Assert.Equal(string.Empty, basis.Note);
        Assert.Equal(8, FactRemediation.RecommendedMaxdop(basis.Cores));

        var advice = FactAdvice.Compose("CONFIG_MAXDOP", Facts(Config("CONFIG_MAXDOP", 0)))!;
        Assert.Contains("Set MAXDOP to 8", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("vCores", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("cores per socket", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>The words an Azure SQL Database's advice uses are its vCores, never "cores per socket", and its MAXDOP is set with
    /// the database-scoped statement (it has no instance option to configure).</summary>
    [Fact]
    public void MaxdopAdvice_OnAzureSqlDatabase_NamesTheVcores_NeverCoresPerSocket_AndTheDatabaseScopedStatement()
    {
        var hardware = HardwareFact(azureSqlDatabase: true, cpuCount: 4, coresPerSocket: 32);

        var zero = FactAdvice.Compose("CONFIG_MAXDOP", Facts(hardware, Config("CONFIG_MAXDOP", 0)))!;
        Assert.Contains("Set MAXDOP to 4", zero.Remediation, StringComparison.Ordinal);
        Assert.Contains("this database's vCores capped at 8 (4 vCores)", zero.Remediation, StringComparison.Ordinal);
        Assert.Contains("ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 4", zero.Remediation, StringComparison.Ordinal);

        var one = FactAdvice.Compose("CONFIG_MAXDOP", Facts(hardware, Config("CONFIG_MAXDOP", 1)))!;
        Assert.Contains("set MAXDOP to 4 (vCores capped at 8 (4 vCores)) with ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 4", one.Remediation, StringComparison.Ordinal);

        var above = FactAdvice.Compose("CONFIG_MAXDOP", Facts(hardware, Config("CONFIG_MAXDOP", 16)))!;
        Assert.Contains("above this database's topology-based guidance of 4", above.Headline, StringComparison.Ordinal);
        Assert.Contains("Lower MAXDOP from 16 to 4 (vCores capped at 8 (4 vCores)", above.Remediation, StringComparison.Ordinal);

        var parallel = FactAdvice.Compose("THREADPOOL_PARALLEL", Facts(hardware, Config("CONFIG_MAXDOP", 0), Config("CONFIG_CTFP", 5)))!;
        Assert.Contains("(4 vCores)", parallel.Remediation, StringComparison.Ordinal);
        Assert.Contains("cap MAXDOP at 4 (this database's vCores, capped at 8)", parallel.Remediation, StringComparison.Ordinal);

        var clause = FactAdvice.Compose("CXPACKET", Facts(hardware, Config("CONFIG_MAXDOP", 0), Config("CONFIG_CTFP", 50)))!;
        Assert.Contains("cap MAXDOP at 4 (the database's vCores, ≤ 8)", clause.Remediation, StringComparison.Ordinal);

        foreach (var text in new[] { zero.Remediation, one.Remediation, above.Remediation, above.Headline, parallel.Remediation, clause.Remediation })
        {
            Assert.DoesNotContain("cores per socket", text, StringComparison.Ordinal);
            Assert.DoesNotContain("cores-per-socket", text, StringComparison.Ordinal);
            Assert.DoesNotContain("sp_configure", text, StringComparison.Ordinal);
        }
    }

    /// <summary>The same advice off an Azure SQL Database, word for word as it always was.</summary>
    [Fact]
    public void MaxdopAdvice_OffAzureSqlDatabase_IsTheLongStandingText()
    {
        var hardware = HardwareFact(azureSqlDatabase: false, cpuCount: 16, coresPerSocket: 4);

        var zero = FactAdvice.Compose("CONFIG_MAXDOP", Facts(hardware, Config("CONFIG_MAXDOP", 0)))!;
        Assert.Contains(
            "Set MAXDOP to 4 — this server's cores-per-socket capped at 8 (cores per socket 4), the per-NUMA-node proxy; " +
            "the SKU is irrelevant to the right value. The Apply button runs sp_configure + RECONFIGURE, an online metadata change.",
            zero.Remediation, StringComparison.Ordinal);

        var one = FactAdvice.Compose("CONFIG_MAXDOP", Facts(hardware, Config("CONFIG_MAXDOP", 1)))!;
        Assert.Contains("set MAXDOP to 4 (cores-per-socket capped at 8 (cores per socket 4)) via sp_configure + RECONFIGURE, an online change.",
            one.Remediation, StringComparison.Ordinal);

        var above = FactAdvice.Compose("CONFIG_MAXDOP", Facts(hardware, Config("CONFIG_MAXDOP", 16)))!;
        Assert.Contains("above this server's topology-based guidance of 4", above.Headline, StringComparison.Ordinal);

        var parallel = FactAdvice.Compose("THREADPOOL_PARALLEL", Facts(hardware, Config("CONFIG_MAXDOP", 0), Config("CONFIG_CTFP", 5)))!;
        Assert.Contains("cost threshold for parallelism is 5 (cores per socket 4).", parallel.Remediation, StringComparison.Ordinal);
        Assert.Contains("cap MAXDOP at 4 (this server's per-NUMA-node processor count, capped at 8)", parallel.Remediation, StringComparison.Ordinal);
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
    public void ServerContextCard_OnAzureSqlDatabase_ShowsTheVcores_NeverTheSchedulerCountOrTheHostsRam_AndNaWhereThereAreNone()
    {
        static string? Hardware(ServerMetadata metadata) =>
            ServerContextCard.Rows(metadata).SingleOrDefault(r => r.Label == "Hardware").Value;

        /* The stored count (2) is the schedulers the database can see; its service objective gives it 1 vCore. The host's RAM,
           had a reader let it through, is not printed either. */
        Assert.Equal("1 vCores", Hardware(new ServerMetadata { Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, VcoreCount = 1, PhysicalMemoryMB = 0 }));
        Assert.Equal("1 vCores", Hardware(new ServerMetadata { Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, VcoreCount = 1, PhysicalMemoryMB = 933_836 }));
        Assert.Equal("4 vCores", Hardware(new ServerMetadata { Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, VcoreCount = 4 }));

        /* A DTU-model objective or an elastic pool names no vCores: the row is still there, and it reads n/a, never the stored 2. */
        Assert.Equal("n/a", Hardware(new ServerMetadata { Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, VcoreCount = null }));
        Assert.Equal("n/a", Hardware(new ServerMetadata { Edition = "SQL Azure", EngineEdition = 5, CpuCount = 2, VcoreCount = 0 }));
        Assert.Equal("n/a", Hardware(new ServerMetadata { Edition = "SQL Azure", EngineEdition = 5, CpuCount = 0, VcoreCount = null }));
    }

    [Fact]
    public void ServerContextCard_OffAzureSqlDatabase_IsTheLongStandingRow()
    {
        static string? Hardware(ServerMetadata metadata) =>
            ServerContextCard.Rows(metadata).SingleOrDefault(r => r.Label == "Hardware").Value;

        var expected = string.Format(CultureInfo.CurrentCulture, "8 CPUs, {0:N0} MB RAM", 65_536L);
        Assert.Equal(expected, Hardware(new ServerMetadata { Edition = "Enterprise Edition (64-bit)", EngineEdition = 3, CpuCount = 8, PhysicalMemoryMB = 65_536 }));
        Assert.Equal(expected, Hardware(new ServerMetadata { Edition = "Enterprise Edition (64-bit)", EngineEdition = 8, CpuCount = 8, PhysicalMemoryMB = 65_536 }));
        Assert.Equal(expected, Hardware(new ServerMetadata { Edition = "Enterprise Edition (64-bit)", EngineEdition = null, CpuCount = 8, PhysicalMemoryMB = 65_536 }));

        /* A VcoreCount a non-Azure server somehow carries changes nothing, and no CPU count drops the row as it always did. */
        Assert.Equal(expected, Hardware(new ServerMetadata { Edition = "Enterprise Edition (64-bit)", EngineEdition = 3, CpuCount = 8, VcoreCount = 2, PhysicalMemoryMB = 65_536 }));
        Assert.Null(Hardware(new ServerMetadata { Edition = "Enterprise Edition (64-bit)", EngineEdition = 3, CpuCount = 0, PhysicalMemoryMB = 0 }));
    }

    // ── 2. Worker Threads card and the verdict's worker term ──

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
    public void WorkerThreadsText_ReadsNotApplicableForAnInUseCountThatWasNotCollected_AndNeverZero()
    {
        /* NULL in use is "not collected" (an Azure SQL Database stores NULL), and it is not 0: the ceiling beside it is the
           database's own and is shown as stored. */
        Assert.Equal("n/a / 512", ServerHardwareScope.WorkerThreadsText(null, 512));
        Assert.Equal("n/a / 479", ServerHardwareScope.WorkerThreadsText(null, 479));

        /* A genuine zero in use reads 0. */
        Assert.Equal("0 / 512", ServerHardwareScope.WorkerThreadsText(0, 512));

        Assert.Equal(
            string.Format(CultureInfo.CurrentCulture, "{0:N0} / {1:N0}", 1_200, 2_560),
            ServerHardwareScope.WorkerThreadsText(1_200, 2_560));
    }

    [Fact]
    public void Verdict_NeverReadsAnUnknownInUseWorkerCountAsSaturation()
    {
        /* Quiet CPU (50%) and no memory pressure: nothing but the worker term can decide this window. */
        static string Verdict(int maxWorkers, int? currentWorkers) => ProvisioningVerdict.Evaluate(
            avgCpuPercent: 50m, maxCpuPercent: 50m, p95CpuPercent: 50m, maxGrantWaiters: 0, grantTimeouts: 0, forcedGrants: 0,
            grantUtilizationPercent: 50m, maxWorkers, currentWorkers);

        Assert.Equal(ProvisioningVerdict.RightSized, Verdict(512, null));
        Assert.Equal(ProvisioningVerdict.RightSized, Verdict(512, 0));
        Assert.Equal(ProvisioningVerdict.RightSized, Verdict(512, 40));
        Assert.Equal(ProvisioningVerdict.UnderProvisioned, Verdict(512, 450));
        Assert.Equal(ProvisioningVerdict.RightSized, Verdict(0, 450));   // no ceiling known is not saturation

        Assert.Equal(
            "No under-provisioning condition is currently met.",
            ProvisioningVerdict.UnderProvisionedReason(50m, 0, 0, 0, 512, null));
        Assert.Contains(
            "Worker threads are near the limit: 450 of 512 in use",
            ProvisioningVerdict.UnderProvisionedReason(50m, 0, 0, 0, 512, 450), StringComparison.Ordinal);
    }

    // ── 3. Health score: the CPU term ──

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

    // ── 4. Words: the CPU unit and the Memory tab ──

    [Fact]
    public void CpuCountUnit_IsVcoresOnAzureSqlDatabase_AndCpusEverywhereElse()
    {
        Assert.Equal(" vCores,", ServerHardwareScope.CpuCountUnit(5));
        foreach (var engineEdition in new int?[] { 1, 2, 3, 4, 8, null })
            Assert.Equal(" CPUs,", ServerHardwareScope.CpuCountUnit(engineEdition));
    }

    [Fact]
    public void MemoryTabLabels_NameTheDatabasesLimitOnAzureSqlDatabase_AndPhysicalMemoryEverywhereElse()
    {
        /* On an Azure SQL Database the collector stores the database's committed target as the first figure and the target
           minus what is committed as the second, so neither is physical memory. */
        Assert.Equal("Memory limit", ServerHardwareScope.MemoryTabTotalLabel(5));
        Assert.Equal("Available under limit", ServerHardwareScope.MemoryTabAvailableLabel(5));

        foreach (var engineEdition in new int?[] { 1, 2, 3, 4, 8, null })
        {
            Assert.Equal("Physical Memory", ServerHardwareScope.MemoryTabTotalLabel(engineEdition));
            Assert.Equal("Available Physical", ServerHardwareScope.MemoryTabAvailableLabel(engineEdition));
        }

        /* The utilization card's caption and the Memory tab's label are the same words. */
        Assert.Equal(ServerHardwareScope.MemoryTabTotalLabel(5) + ": ", ServerHardwareScope.PhysicalMemoryCaption(5));
        Assert.Equal("Physical: ", ServerHardwareScope.PhysicalMemoryCaption(3));
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
    public void FinOpsTab_AsksTheSharedRules_ForTheCpuUnit_TheWorkerThreadsCard_TheHealthTooltip_AndTheInventoryCpuTerm()
    {
        var tab = ReadRepoFile("Lite/Controls/FinOpsTab.xaml.cs");
        var xaml = ReadRepoFile("Lite/Controls/FinOpsTab.xaml");

        Assert.Contains("CpuCountUnitText.Text = ServerHardwareScope.CpuCountUnit(data.EngineEdition);", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"CpuCountUnitText\"", xaml, StringComparison.Ordinal);

        Assert.Contains(
            "WorkerThreadsText.Text = ServerHardwareScope.WorkerThreadsText(data.CurrentWorkersCount, data.MaxWorkersCount);",
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
    public void WorkerReads_KeepANullInUseCountNull_InAllThreePlaces()
    {
        var utilization = ReadRepoFile("Lite/Services/LocalDataService.FinOps.Utilization.cs");
        var fleet = ReadRepoFile("Lite/Services/LocalDataService.FinOps.ServerProperties.cs");

        Assert.Contains("int? currentWorkers = reader.IsDBNull(10) ? null : Convert.ToInt32(reader.GetValue(10));", utilization, StringComparison.Ordinal);
        Assert.Contains("currentWorkers: reader.IsDBNull(10) ? (int?)null : Convert.ToInt32(reader.GetValue(10)));", utilization, StringComparison.Ordinal);
        Assert.Contains("currentWorkers: reader.IsDBNull(7) ? (int?)null : Convert.ToInt32(reader.GetValue(7)));", fleet, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(m.current_workers_count, 0)", utilization, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(m.current_workers_count, 0)", fleet, StringComparison.Ordinal);
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
            Assert.Contains("PhysicalMemoryMB = ServerHardwareScope.OwnPhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb) ?? 0L,", source, StringComparison.Ordinal);
            Assert.Contains("EngineEdition = engineEdition,", source, StringComparison.Ordinal);
            Assert.Contains("VcoreCount = vcoreCount,", source, StringComparison.Ordinal);
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

    [Fact]
    public void MemoryTab_AsksTheSharedRule_ForTheNamesOfItsFirstTwoFigures()
    {
        var charts = ReadRepoFile("Lite/Controls/ServerTab.Charts.cs");
        var xaml = ReadRepoFile("Lite/Controls/ServerTab.xaml");

        Assert.Contains("PhysicalMemoryLabel.Text = ServerHardwareScope.MemoryTabTotalLabel(stats?.EngineEdition);", charts, StringComparison.Ordinal);
        Assert.Contains("AvailablePhysicalMemoryLabel.Text = ServerHardwareScope.MemoryTabAvailableLabel(stats?.EngineEdition);", charts, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"PhysicalMemoryLabel\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"AvailablePhysicalMemoryLabel\"", xaml, StringComparison.Ordinal);

        var tool = ReadRepoFile("Lite/Mcp/McpMemoryTools.cs");
        Assert.Contains("engine_edition = stats.EngineEdition", tool, StringComparison.Ordinal);
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
        /* An Azure SQL Database as the collector stores it: the database's own 2 schedulers in cpu_count, the host's hyperthread
           ratio 64, 0 sockets, 32 cores per socket and 911.9 GB, plus the service objective's vCores. */
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
        var facts = await CollectFactsAsync(engineEdition: 5, vcoreCount: 4);

        var hardware = Assert.Contains("SERVER_HARDWARE", facts);
        Assert.Equal(4, hardware.Value);
        Assert.Equal(
            new[] { "cpu_count", "hadr_enabled", "vcore_count" },
            hardware.Metadata.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal(4, hardware.Metadata["cpu_count"]);
        Assert.Equal(4, hardware.Metadata["vcore_count"]);
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("physical_memory_mb"));
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("cores_per_socket"));

        /* The recommended MAXDOP read off the collected fact follows the 4 vCores, not the 32 cores per socket stored beside them. */
        var basis = FactRemediation.MaxdopBasisFrom(facts);
        Assert.True(basis.FromVcores);
        Assert.Equal(4, basis.Cores);
        Assert.Equal(4, FactRemediation.RecommendedMaxdop(basis.Cores));
    }

    [Fact]
    public async Task Collector_OnAzureSqlDatabaseWithNoVcores_EmitsNoServerHardwareFact_AndNothingFromTheHost()
    {
        var facts = await CollectFactsAsync(engineEdition: 5, vcoreCount: null);

        Assert.DoesNotContain("SERVER_HARDWARE", facts.Keys);
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("physical_memory_mb"));
        Assert.DoesNotContain(facts.Values, f => f.Metadata.ContainsKey("cores_per_socket"));
        Assert.Equal(0, FactRemediation.MaxdopBasisFrom(facts).Cores);
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
        Assert.False(hardware.Metadata.ContainsKey("vcore_count"));
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

    /// <summary>(edition, stored cpu_count, stored physical memory, vCores, expected CpuCount, expected RAM, expected Hardware row).
    /// The stored count is carried as read (on an Azure SQL Database it is the database's own scheduler count, 2 for a 1-vCore
    /// database), the host's RAM never, and the card shows the vCores.</summary>
    public static IEnumerable<object?[]> PlanMetadataCases() =>
    [
        [5, 2, 933_836L, 1, 2, 0L, "1 vCores"],            // Azure SQL Database with vCores: the vCores, never the 2 it can see, no RAM
        [5, 2, 933_836L, null, 2, 0L, "n/a"],              // Azure SQL Database, DTU objective: the row is there and reads n/a
        [3, 8, 65_536L, null, 8, 65_536L, "8 CPUs, {RAM} MB RAM"],   // SQL Server: as stored
        [8, 8, 65_536L, null, 8, 65_536L, "8 CPUs, {RAM} MB RAM"],   // Managed Instance: as stored
    ];

    private static string HardwareRow(ServerMetadata metadata) =>
        ServerContextCard.Rows(metadata).Single(r => r.Label == "Hardware").Value;

    [Theory]
    [MemberData(nameof(PlanMetadataCases))]
    public async Task PlanMetadata_LocalDataServiceRead_CarriesTheDatabasesOwnFigures(
        int engineEdition, int storedCpuCount, long storedPhysicalMemoryMb, int? vcoreCount, int expectedCpuCount,
        long expectedPhysicalMemoryMb, string expectedHardwareRow)
    {
        await SeedServerPropertiesAsync(ServerId, engineEdition, storedCpuCount, storedPhysicalMemoryMb, vcoreCount);

        var metadata = await new LocalDataService(_fixture.DuckDb).GetServerMetadataForPlanAnalysisAsync(ServerId);

        Assert.NotNull(metadata);
        Assert.Equal(expectedCpuCount, metadata!.CpuCount);
        Assert.Equal(expectedPhysicalMemoryMb, metadata.PhysicalMemoryMB);
        Assert.Equal(engineEdition, metadata.EngineEdition);
        Assert.Equal(vcoreCount, metadata.VcoreCount);
        Assert.Equal(expectedHardwareRow.Replace("{RAM}", 65_536L.ToString("N0", CultureInfo.CurrentCulture)), HardwareRow(metadata));
    }

    [Theory]
    [MemberData(nameof(PlanMetadataCases))]
    public async Task PlanMetadata_DrillDownRead_CarriesTheDatabasesOwnFigures(
        int engineEdition, int storedCpuCount, long storedPhysicalMemoryMb, int? vcoreCount, int expectedCpuCount,
        long expectedPhysicalMemoryMb, string expectedHardwareRow)
    {
        await SeedServerPropertiesAsync(ServerId, engineEdition, storedCpuCount, storedPhysicalMemoryMb, vcoreCount);

        var read = typeof(DrillDownCollector).GetMethod("ReadServerMetadataForPlanAnalysisAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var metadata = await (Task<ServerMetadata?>)read.Invoke(new DrillDownCollector(_fixture.DuckDb), [ServerId, CancellationToken.None])!;

        Assert.NotNull(metadata);
        Assert.Equal(expectedCpuCount, metadata!.CpuCount);
        Assert.Equal(expectedPhysicalMemoryMb, metadata.PhysicalMemoryMB);
        Assert.Equal(engineEdition, metadata.EngineEdition);
        Assert.Equal(vcoreCount, metadata.VcoreCount);
        Assert.Equal(expectedHardwareRow.Replace("{RAM}", 65_536L.ToString("N0", CultureInfo.CurrentCulture)), HardwareRow(metadata));
    }

    // ── 2. in-use worker count: point-in-time read, 7-day trend, fleet read ──

    /// <summary>One server with a measured CPU window (average and p95 50%, so the verdict is RIGHT_SIZED unless something else
    /// decides it) and the memory_stats the collector stores: on an Azure SQL Database the database's own memory limit (1,838 MB
    /// for a 1-vCore General Purpose database), the database's own ceiling of 512 workers, and NO in-use count (the collector
    /// stores NULL there).</summary>
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

    /// <summary>(edition, vCores, in-use workers stored, expected verdict, expected Worker Threads text). The ceiling is the stored
    /// 512 on every edition.</summary>
    public static IEnumerable<object?[]> WorkerCases() =>
    [
        [5, 1, null, "RIGHT_SIZED", "n/a / 512"],           // Azure SQL Database with vCores, as stored: no in-use count
        [5, null, null, "RIGHT_SIZED", "n/a / 512"],        // Azure SQL Database, DTU objective
        [3, null, 450, "UNDER_PROVISIONED", "450 / 512"],   // SQL Server: 450 of 512 is worker saturation, as it always was
        [8, null, 450, "UNDER_PROVISIONED", "450 / 512"],   // Managed Instance: the same
        [3, null, 40, "RIGHT_SIZED", "40 / 512"],
        [3, null, 0, "RIGHT_SIZED", "0 / 512"],             // a genuine zero reads 0
        [3, null, null, "RIGHT_SIZED", "n/a / 512"],        // a NULL on any edition reads n/a, never 0
    ];

    [Theory]
    [MemberData(nameof(WorkerCases))]
    public async Task UtilizationRead_KeepsANullInUseCountNull_AndTheCeilingAsStored(
        int engineEdition, int? vcoreCount, int? currentWorkers, string expectedVerdict, string expectedText)
    {
        await SeedServerWithMemoryAsync(ServerId, engineEdition, vcoreCount, currentWorkers);

        var row = await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(engineEdition, row!.EngineEdition);
        Assert.Equal(512, row.MaxWorkersCount);
        Assert.Equal(currentWorkers, row.CurrentWorkersCount);
        Assert.Equal(expectedVerdict, row.ProvisioningStatus);
        Assert.Equal(expectedText, ServerHardwareScope.WorkerThreadsText(row.CurrentWorkersCount, row.MaxWorkersCount));
    }

    [Theory]
    [MemberData(nameof(WorkerCases))]
    public async Task TrendRead_KeepsANullInUseCountNull_AndNeverCallsADaySaturated(
        int engineEdition, int? vcoreCount, int? currentWorkers, string expectedVerdict, string expectedText)
    {
        _ = expectedText;
        await SeedServerWithMemoryAsync(ServerId, engineEdition, vcoreCount, currentWorkers);

        var days = await new LocalDataService(_fixture.DuckDb).GetProvisioningTrendAsync(ServerId);

        var day = Assert.Single(days);
        Assert.Equal(expectedVerdict, day.Status);
    }

    [Fact]
    public async Task FleetRead_KeepsANullInUseCountNull_AndNeverCallsAServerSaturated()
    {
        const int azureVcores = ServerId - 1;
        const int azureDtu = ServerId - 2;
        const int sqlServer = ServerId - 3;
        const int managedInstance = ServerId - 4;
        const int sqlServerNoCount = ServerId - 5;
        await SeedServerWithMemoryAsync(azureVcores, engineEdition: 5, vcoreCount: 1, currentWorkers: null);
        await SeedServerWithMemoryAsync(azureDtu, engineEdition: 5, vcoreCount: null, currentWorkers: null);
        await SeedServerWithMemoryAsync(sqlServer, engineEdition: 3, vcoreCount: null, currentWorkers: 450);
        await SeedServerWithMemoryAsync(managedInstance, engineEdition: 8, vcoreCount: null, currentWorkers: 450);
        await SeedServerWithMemoryAsync(sqlServerNoCount, engineEdition: 3, vcoreCount: null, currentWorkers: null);

        var metrics = await new LocalDataService(_fixture.DuckDb).GetServerMetricsAsync();

        Assert.Equal("RIGHT_SIZED", metrics[azureVcores].ProvisioningStatus);
        Assert.Equal("RIGHT_SIZED", metrics[azureDtu].ProvisioningStatus);
        Assert.Equal("UNDER_PROVISIONED", metrics[sqlServer].ProvisioningStatus);
        Assert.Equal("UNDER_PROVISIONED", metrics[managedInstance].ProvisioningStatus);
        Assert.Equal("RIGHT_SIZED", metrics[sqlServerNoCount].ProvisioningStatus);
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
        Assert.Equal(engineEdition, stats.EngineEdition);
    }

    [Fact]
    public async Task MemoryStatsRead_CarriesNoEngineEdition_WhenNoServerPropertiesRowIsStored()
    {
        using (var readLock = _fixture.DuckDb.AcquireReadLock())
        {
            var conn = await SeedConnectionAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, $4, 1000, 400, 1000, 1000, 400)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = _now });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "LeftoversSrv" });
            await cmd.ExecuteNonQueryAsync();
        }

        var stats = await new LocalDataService(_fixture.DuckDb).GetLatestMemoryStatsAsync(ServerId);

        Assert.NotNull(stats);
        Assert.Null(stats!.EngineEdition);
    }
}
