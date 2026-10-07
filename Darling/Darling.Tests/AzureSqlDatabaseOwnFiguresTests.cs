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
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.PlanAnalysis;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

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
/// <item>the words over the Memory tab's first two figures (the viewer, and the web tiles over <c>get_memory_stats</c>), which on an
/// Azure SQL Database are the database's memory limit and the room left under it.</item>
/// </list>
///
/// <para>The pure rules are run here; the PostgreSQL reads are pinned as text (their statements are public constants, and the
/// readers that consume them are pinned by the lines that route them through the shared rule), and Lite.Tests runs the same
/// table against a real DuckDB in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseOwnFiguresTests
{
    private static AnalysisContext Context() => new()
    {
        ServerId = 7,
        ServerName = "OwnFiguresSrv",
        TimeRangeStart = new DateTime(2026, 9, 30, 0, 0, 0, DateTimeKind.Utc),
        TimeRangeEnd = new DateTime(2026, 9, 30, 1, 0, 0, DateTimeKind.Utc),
    };

    // ── 1. SERVER_HARDWARE fact, MAXDOP recommendation, LPIM advisory, Server Context card ──

    private static Fact HardwareFact(bool azureSqlDatabase, int cpuCount, int coresPerSocket) =>
        FactCollectorHelpers.BuildServerHardwareFact(
            Context(), hardwareIsTheHosts: azureSqlDatabase,
            cpuCount: cpuCount, hyperthreadRatio: 64, physicalMemoryMb: 933_836, socketCount: 0, coresPerSocket: coresPerSocket,
            hadrEnabled: false)!;

    private static Dictionary<string, Fact> Facts(params Fact[] facts) => facts.ToDictionary(f => f.Key, f => f);

    private static Fact Config(string key, double value) => new() { Source = "config", Key = key, Value = value };

    [Fact]
    public void ServerHardwareFact_OnAzureSqlDatabase_CarriesTheVcoresAndHadrOnly_NotTheHostsTopologyOrMemory()
    {
        /* The host's four figures go in (hyperthread ratio 64, 933,836 MB, 0 sockets, 32 cores per socket); none may come out. */
        var fact = FactCollectorHelpers.BuildServerHardwareFact(
            Context(), hardwareIsTheHosts: true,
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
        var context = Context();

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
    public void CpuCoreNoun_IsVcoresOnAzureSqlDatabase_AndCoresEverywhereElse()
    {
        /* The FinOps CPU right-sizing text prints the utilization read's count: the vCores on an Azure SQL Database, which it
           names the way the utilization card does, and a CPU count everywhere else, in the word it has always used. */
        Assert.Equal("vCores", ServerHardwareScope.CpuCoreNoun(5));
        foreach (var engineEdition in new int?[] { 1, 2, 3, 4, 8, null })
            Assert.Equal("cores", ServerHardwareScope.CpuCoreNoun(engineEdition));
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
        Assert.Contains("PhysicalMemoryMB = ServerHardwareScope.OwnPhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb) ?? 0L,", reader, StringComparison.Ordinal);
        Assert.Contains("EngineEdition = engineEdition,", reader, StringComparison.Ordinal);
        Assert.Contains("VcoreCount = vcoreCount,", reader, StringComparison.Ordinal);
    }

    [Fact]
    public void FinOpsTab_AsksTheSharedRules_ForTheCpuUnit_TheWorkerThreadsCard_TheHealthTooltip_AndTheInventoryCpuTerm()
    {
        var tab = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");

        Assert.Contains("FinOpsCpuCountUnitText.Text = ServerHardwareScope.CpuCountUnit(data.EngineEdition);", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"FinOpsCpuCountUnitText\"", xaml, StringComparison.Ordinal);

        Assert.Contains(
            "FinOpsWorkerThreadsText.Text = ServerHardwareScope.WorkerThreadsText(data.CurrentWorkersCount, data.MaxWorkersCount);",
            tab, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"{data.CurrentWorkersCount:N0} / {data.MaxWorkersCount:N0}\"", tab, StringComparison.Ordinal);

        Assert.Contains(
            "FinOpsHealthScoreBorder.ToolTip = data.HasCpuSample ? null : ServerHardwareScope.HealthScoreWithoutCpuNote;",
            tab, StringComparison.Ordinal);

        // The CPU term's rule moved to Storage with the inventory figures (#4843).
        Assert.Contains("item.HealthScore = FinOpsInventoryFigures.HealthScore(item.AvgCpuPct);", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("item.AvgCpuPct ?? 0m", tab, StringComparison.Ordinal);

        var figures = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "FinOpsInventoryFigures.cs");
        Assert.Contains(
            "int? cpuScore = avgCpuPct is decimal avgCpu ? FinOpsHealthCalculator.CpuScore(avgCpu) : null;",
            figures, StringComparison.Ordinal);
        Assert.DoesNotContain("avgCpuPct ?? 0m", figures, StringComparison.Ordinal);
    }

    [Fact]
    public void WorkerReads_KeepANullInUseCountNull_InAllThreePlaces()
    {
        var utilization = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "DarlingFinOpsUtilizationReader.cs");
        var inventory = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "FinOps", "DarlingFinOpsInventoryReader.cs");

        /* Point-in-time read, 7-day trend and fleet read: the in-use count is read as NULL, not coalesced to 0. */
        Assert.Contains("int? currentWorkers = reader.IsDBNull(10) ? null : Convert.ToInt32(reader.GetValue(10));", utilization, StringComparison.Ordinal);
        Assert.Contains("currentWorkers: reader.IsDBNull(10) ? (int?)null : Convert.ToInt32(reader.GetValue(10)),", utilization, StringComparison.Ordinal);
        Assert.Contains("currentWorkers: reader.IsDBNull(7) ? (int?)null : Convert.ToInt32(reader.GetValue(7)),", inventory, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(m.current_workers_count", utilization, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(m.current_workers_count", inventory, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(m.current_workers_count", ViewerDataService.ProvisioningTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(m.current_workers_count", ViewerDataService.ServerMetricsSql, StringComparison.Ordinal);
        Assert.Contains("m.current_workers_count", ViewerDataService.ProvisioningTrendSql, StringComparison.Ordinal);

        /* The ceiling is the engine's own figure on every edition: no CASE zeroes it on an Azure SQL Database. */
        Assert.DoesNotContain("CASE WHEN s.engine_edition = 5 THEN 0 ELSE COALESCE(m.max_workers_count, 0) END", ViewerDataService.ProvisioningTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("CASE WHEN props.engine_edition = 5 THEN NULL ELSE latest.max_workers_count END", ViewerDataService.ServerMetricsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryUtilization_IsStillComputedFromMemoryStats_NotFromTheHostsPhysicalMemory()
    {
        /* The read is the memory_stats snapshot, which the collector fills from the database's own committed target on an
           Azure SQL Database (1,838 MB for a 1-vCore General Purpose database), so dividing by it is database-scoped. Neither
           statement takes anything from server_properties: both the tool and the viewer read their edition from the registry. */
        foreach (var sql in new[] { DarlingDataReader.LatestMemoryStatsSql, ViewerDataService.LatestMemoryStatsSql })
        {
            Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("sp.physical_memory_mb", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("sp.cpu_count", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("server_properties", sql, StringComparison.Ordinal);
        }

        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.Contains(
            "? (stats.TotalPhysicalMemoryMb - stats.AvailablePhysicalMemoryMb) / stats.TotalPhysicalMemoryMb * 100",
            tool, StringComparison.Ordinal);
        Assert.Contains("memory_utilization_pct = Math.Round(utilization, 1),", tool, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryTab_AsksTheSharedRule_ForTheNamesOfItsFirstTwoFigures_InTheViewer_TheMcpPayload_AndTheWebTiles()
    {
        var memory = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Memory.cs");
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        Assert.Contains("PhysicalMemoryLabel.Text = ServerHardwareScope.MemoryTabTotalLabel(_server.EngineEdition);", memory, StringComparison.Ordinal);
        Assert.Contains("AvailablePhysicalMemoryLabel.Text = ServerHardwareScope.MemoryTabAvailableLabel(_server.EngineEdition);", memory, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"PhysicalMemoryLabel\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"AvailablePhysicalMemoryLabel\"", xaml, StringComparison.Ordinal);

        /* The viewer's memory read carries no edition: the strip's names follow the registry's, the value its page-file lines read.
           The row is built from the eleven memory columns and nothing else. */
        var dataService = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Memory.cs");
        var viewerRowStart = dataService.IndexOf("return new MemoryStatsRow(", StringComparison.Ordinal);
        Assert.True(viewerRowStart > 0, "the viewer's MemoryStatsRow construction is missing");
        var viewerRow = dataService[viewerRowStart..dataService.IndexOf(");", viewerRowStart, StringComparison.Ordinal)];
        Assert.Contains("reader.IsDBNull(10) ? 0 : reader.GetDouble(10)", viewerRow, StringComparison.Ordinal);
        Assert.DoesNotContain("IsDBNull(11)", viewerRow, StringComparison.Ordinal);

        /* The MCP tool's payload names its figures from the one registry edition it read, not from a column of the memory read. */
        var serviceReader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");
        var memoryReadStart = serviceReader.IndexOf("Task<MemoryStatsRow?> GetLatestMemoryStatsAsync(", StringComparison.Ordinal);
        Assert.True(memoryReadStart > 0, "GetLatestMemoryStatsAsync is missing");
        var memoryRead = serviceReader[memoryReadStart..serviceReader.IndexOf("LatestMemoryClerksSql", memoryReadStart, StringComparison.Ordinal)];
        Assert.DoesNotContain("IsDBNull(11)", memoryRead, StringComparison.Ordinal);

        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.Contains("engine_edition = engineEdition == CollectorEngineCapability.UnknownEngineEdition ? (int?)null : engineEdition", tool, StringComparison.Ordinal);
        Assert.DoesNotContain("stats.EngineEdition", tool, StringComparison.Ordinal);

        /* The web tiles read get_memory_stats: the same two figures appear twice, each drawn only on its own side of the
           engine_edition 5 condition (panels.js visibleStats), under the words the viewer uses. */
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var templates = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "view-templates.js");
        var tabsStart = tabs.IndexOf("const MEMORY_STATS = [", StringComparison.Ordinal);
        Assert.True(tabsStart > 0, "MEMORY_STATS is missing");
        var list = tabs[tabsStart..tabs.IndexOf("];", tabsStart, StringComparison.Ordinal)];
        /* The built-in page names the condition once; the ready-made dashboard is a literal the template tests read without
           running it, so it writes the condition out in each tile. */
        foreach (var (source, condition) in new[] { (list, "AZURE_SQL_DATABASE"), (templates, "{ key: \"engine_edition\", equals: 5 }") })
        {
            Assert.Contains($"{{ key: \"total_physical_memory_mb\", label: \"Physical\", format: \"mb\", hideWhen: {condition} }}", source, StringComparison.Ordinal);
            Assert.Contains($"{{ key: \"total_physical_memory_mb\", label: \"Memory limit\", format: \"mb\", showWhen: {condition} }}", source, StringComparison.Ordinal);
            Assert.Contains($"{{ key: \"available_physical_memory_mb\", label: \"Available\", format: \"mb\", hideWhen: {condition} }}", source, StringComparison.Ordinal);
            Assert.Contains($"{{ key: \"available_physical_memory_mb\", label: \"Available under limit\", format: \"mb\", showWhen: {condition} }}", source, StringComparison.Ordinal);
        }
        Assert.Contains("const AZURE_SQL_DATABASE = { key: \"engine_edition\", equals: 5 };", tabs, StringComparison.Ordinal);
    }

    /// <summary>audit_config reads PostgreSQL, which this suite does not stand up, so the tool is pinned at the source: its MAXDOP
    /// recommendation comes from the shared basis (the vCores on an Azure SQL Database, cores per socket elsewhere) and says which.
    /// Lite.Tests runs the same tool end to end over a seeded store.</summary>
    [Fact]
    public void AuditConfig_TakesItsMaxdopRecommendationFromTheSharedBasis_AndNamesTheDatabasesVcores()
    {
        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");

        Assert.Contains("var maxdopBasis = FactRemediation.MaxdopBasisFrom(factsByKey);", tool, StringComparison.Ordinal);
        Assert.Contains("var recommended = (int)FactRemediation.RecommendedMaxdop(maxdopBasis.Cores);", tool, StringComparison.Ordinal);
        Assert.Contains("(maxdopBasis.FromVcores ? \"this database's vCores\" : \"this server's cores-per-socket\")", tool, StringComparison.Ordinal);
    }
}
