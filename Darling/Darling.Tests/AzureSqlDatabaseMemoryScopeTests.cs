/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Two collected tables hold a "physical memory" figure, and on an Azure SQL Database (engine edition 5) only one of them is
/// the host's. <c>server_properties.physical_memory_mb</c> comes from <c>sys.dm_os_sys_info.physical_memory_kb</c> and is the
/// HOST's (911.9 GB for a 1-vCore serverless General Purpose database). <c>memory_stats.total_physical_memory_mb</c> comes from
/// <c>committed_target_kb</c> there, which is the database's own memory limit (1,838 MB for that same database), and the
/// buffer pool and server-memory counters beside it are the database's too.
///
/// <para>So the FinOps utilization card's Physical Memory and Buffer Pool %, its verdict sentences and the health score's memory
/// term, which all read <c>memory_stats</c>, are shown and scored on an Azure SQL Database exactly as on SQL Server. What reads
/// <c>server_properties</c> (<c>get_server_properties</c>, the web Server Properties tiles, the Server Inventory hardware cells)
/// still hides the host's values. The figures here are the two tables' different values (1,838 MB and 933,836 MB), so a read that
/// takes the wrong table shows up as the wrong number. The Viewer's reads run against PostgreSQL, which this suite does not stand
/// up, so they are pinned as SQL text. Lite.Tests pins the same table for the other app, in the same words.</para>
/// </summary>
public sealed class AzureSqlDatabaseMemoryScopeTests
{
    /// <summary>What <c>memory_stats</c> holds on the 1-vCore database: <c>committed_target_kb</c> / 1024.</summary>
    private const int DatabaseMemoryLimitMb = 1_838;

    /// <summary>What <c>server_properties</c> holds for the same database: the host's physical memory.</summary>
    private const long HostPhysicalMemoryMb = 933_836;

    private static readonly string[] s_hostKeys =
        ["cpu_count", "hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

    /// <summary>Comments wrap, so a pin on their words reads them with every run of whitespace as one space.</summary>
    private static string Flatten(string text) => Regex.Replace(text, @"\s+", " ");

    // ── the utilization read: memory_stats, not server_properties ──

    [Fact]
    public void UtilizationRead_TakesTheMemoryFiguresFromMemoryStats_NeverFromServerProperties()
    {
        var sql = ViewerDataService.UtilizationEfficiencySql;
        var start = sql.IndexOf("mem_latest AS (", StringComparison.Ordinal);
        Assert.True(start > 0, "the mem_latest CTE is missing");
        var memLatest = sql[start..sql.IndexOf("),", start, StringComparison.Ordinal)];

        Assert.Contains("FROM v_memory_stats", memLatest, StringComparison.Ordinal);
        Assert.Contains("total_physical_memory_mb", memLatest, StringComparison.Ordinal);
        Assert.Contains("buffer_pool_mb", memLatest, StringComparison.Ordinal);
        Assert.Contains("m.total_physical_memory_mb,", sql, StringComparison.Ordinal);
        Assert.Contains("m.buffer_pool_mb,", sql, StringComparison.Ordinal);
        /* The only CTE that reads server_properties takes the CPU count and the edition, and no memory column at all. */
        Assert.False(
            Regex.IsMatch(sql, @"(?<!total_)physical_memory_mb"),
            "the utilization read must not take server_properties.physical_memory_mb, which is the host's on an Azure SQL Database");
    }

    // ── the health score keeps its memory term on an Azure SQL Database ──

    /// <summary>CPU p95 of 7% scores 95 and 50% free storage scores 100. The buffer pool is 1,100 MB. Against the database's
    /// 1,838 MB limit that is 60%, which scores 100: 95 * 0.4 + 100 * 0.3 + 100 * 0.3 = 98. Against the host's 933,836 MB it
    /// would be 0.1%, which scores 60 and gives 86. Left out altogether it gives 97.</summary>
    private static UtilizationEfficiencyRow Scored(int engineEdition, int physicalMemoryMb) => new()
    {
        EngineEdition = engineEdition,
        ProvisioningStatus = ProvisioningVerdict.RightSized, // a measured window: a window with no CPU sample has no CPU term
        P95CpuPct = 7m,
        BufferPoolMb = 1_100,
        PhysicalMemoryMb = physicalMemoryMb,
        FreeSpacePct = 50m,
    };

    [Fact]
    public void HealthScore_OnAzureSqlDatabase_CarriesTheMemoryTermScoredFromMemoryStats()
    {
        var score = Scored(5, DatabaseMemoryLimitMb).ComputeHealthScore();

        Assert.Equal(98, score);
        Assert.NotEqual(97, score);   // the memory term was not left out
        Assert.NotEqual(86, score);   // and it was not scored against the host's memory
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
        var baseline = Scored(3, DatabaseMemoryLimitMb).ComputeHealthScore();

        Assert.Equal(98, baseline);
        Assert.Equal(baseline, Scored(engineEdition, DatabaseMemoryLimitMb).ComputeHealthScore());
        /* And the score does move with the figure memory_stats holds, on an Azure SQL Database as anywhere. */
        Assert.Equal(86, Scored(engineEdition, (int)HostPhysicalMemoryMb).ComputeHealthScore());
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

    // ── what reads server_properties stays the host's, and stays hidden ──

    private static DarlingDataReader.ServerPropertiesReadRow StoredRow(int engineEdition) => new(
        new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc), engineEdition == 5 ? "SQL Azure" : "Enterprise Edition (64-bit)",
        "12.0.2000.8", "RTM", null, engineEdition, 2, 64, HostPhysicalMemoryMb, 0, 32, false, false, null,
        engineEdition == 5 ? "GP_S_Gen5_1" : null, null, null, engineEdition == 5 ? 1 : null);

    [Fact]
    public void ServerPropertiesReads_OnAzureSqlDatabase_StayTheHostsAndNull_WhateverMemoryStatsHolds()
    {
        var stored = StoredRow(5);
        /* The stored figure is the host's, which is why it is hidden: it is not memory_stats' 1,838. */
        Assert.Equal(HostPhysicalMemoryMb, stored.PhysicalMemoryMb);

        /* get_server_properties (the payload the web Server Properties tiles read through /api/read). */
        var json = JsonDocument.Parse(DarlingMcpDataTools.ServerPropertiesPayload("Srv", stored)).RootElement;
        foreach (var key in s_hostKeys)
            Assert.Equal(JsonValueKind.Null, json.GetProperty(key).ValueKind);

        /* The FinOps Server Inventory row. */
        var inventory = new ServerPropertyRow
        {
            EngineEdition = stored.EngineEdition, CpuCount = stored.CpuCount, PhysicalMemoryMb = stored.PhysicalMemoryMb,
            SocketCount = stored.SocketCount, CoresPerSocket = stored.CoresPerSocket,
        };
        Assert.Null(inventory.CpuCount);
        Assert.Null(inventory.PhysicalMemoryMb);
        Assert.Null(inventory.SocketCount);
        Assert.Null(inventory.CoresPerSocket);
    }

    [Fact]
    public void ServerPropertiesReads_OnSqlServer_KeepTheirHardware()
    {
        var json = JsonDocument.Parse(DarlingMcpDataTools.ServerPropertiesPayload("Srv", StoredRow(3))).RootElement;

        Assert.Equal(HostPhysicalMemoryMb, json.GetProperty("physical_memory_mb").GetInt64());
        Assert.Equal(2, json.GetProperty("cpu_count").GetInt32());
    }

    [Fact]
    public void ServerPropertiesReads_TakeTheirHardwareFromServerProperties_NeverFromMemoryStats()
    {
        Assert.Contains("FROM server_properties", DarlingDataReader.LatestServerPropertiesSql, StringComparison.Ordinal);
        Assert.DoesNotContain("memory_stats", DarlingDataReader.LatestServerPropertiesSql, StringComparison.Ordinal);
        Assert.Contains("sp.physical_memory_mb", ViewerDataService.ServerInventorySql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", ViewerDataService.ServerInventorySql, StringComparison.Ordinal);
        Assert.DoesNotContain("memory_stats", ViewerDataService.ServerInventorySql, StringComparison.Ordinal);
    }

    // ── the card and the recommendations, pinned at the source ──

    [Fact]
    public void FinOpsUtilizationCard_ShowsPhysicalMemoryAndTheBufferPoolShare_OnEveryEdition()
    {
        var tab = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.Contains("FinOpsMemoryRatioText.Text = $\"{bpPct:N0}%\";", tab, StringComparison.Ordinal);
        Assert.Contains("SetBar(FinOpsMemoryRatioBar, FinOpsMemRatioFilled, FinOpsMemRatioEmpty, bpPct);", tab, StringComparison.Ordinal);
        Assert.Contains("FinOpsPhysicalMemoryText.Text = $\"{data.PhysicalMemoryMb:N0} MB\";", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerHardwareScope.NotApplicable", tab, StringComparison.Ordinal);
        /* The health score has its memory term everywhere, so there is no tooltip explaining an absent one. The only tooltip on it
           explains an absent CPU term (a window with no CPU sample), and it is not keyed on the edition. */
        Assert.DoesNotContain("HealthScoreWithoutMemoryNote", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("FinOpsHealthScoreBorder.ToolTip = azureSqlDb", tab, StringComparison.Ordinal);
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
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");

        Assert.Contains("On an Azure SQL Database it is the share of the database's memory limit.", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryRecommendation_NeverDividesByServerPropertiesMemory_SoItCannotPrintTheHostsGigabytes()
    {
        /* "Memory over-provisioned (P95 SQL memory uses 0% of 911GB RAM)" is the only text either app builds in the form
           "{percent} of {n}GB RAM". Its divisor is util.PhysicalMemoryMb, read from memory_stats through the utilization read,
           and the rules file reads no physical-memory column of its own. */
        var rules = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs");

        Assert.Contains("of {util.PhysicalMemoryMb / 1024}GB RAM", rules, StringComparison.Ordinal);
        Assert.False(
            Regex.IsMatch(rules, @"physical_memory_mb"),
            "the recommendation rules must not read a physical-memory column of their own");
    }

    [Fact]
    public void MemoryAndVmRules_OnAzureSqlDatabase_SayWhyTheyStandDown_AndDoNotCallTheMemoryTheHosts()
    {
        var rules = Flatten(ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs"));

        Assert.DoesNotContain("reports the HOST's memory", rules, StringComparison.Ordinal);
        Assert.DoesNotContain("its memory figure is the host's", rules, StringComparison.Ordinal);
        Assert.Contains("its memory comes with its service objective and cannot be resized on its own", rules, StringComparison.Ordinal);
        Assert.Contains("its cores and memory come with its service objective", rules, StringComparison.Ordinal);
    }
}
