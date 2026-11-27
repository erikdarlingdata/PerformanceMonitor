/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9099: collectors whose SQL needs SQL Server 2016 or later do not run on an older server, and read as not collected with the
/// server's version in the reason, on the same path the engine gaps use. A supported server, Azure SQL Database and an unprobed
/// server (0) are unchanged.
/// </summary>
public sealed class BelowFloorCollectorGateTests
{
    public static IEnumerable<object[]> FloorGatedCollectors() =>
        new[]
        {
            "query_snapshots", "procedure_stats", "file_io_stats", "database_size_stats", "ag_replica_states", "ag_database_replica_states",
            "database_config", "database_scoped_config", "server_properties", "query_stats", "query_store", "query_store_health",
            "plan_correction", "pvs_stats",
        }.Select(name => new object[] { name });

    private static CollectorTargetInfo Target(int major, bool azureManagedInstance = false) =>
        new() { SqlMajorVersion = major, IsAzureManagedInstance = azureManagedInstance };

    [Theory]
    [MemberData(nameof(FloorGatedCollectors))]
    public void BelowFloorTarget_DoesNotRun_AndSupportedUnknownAndManagedInstanceStillDo(string name)
    {
        Assert.False(CollectorCatalog.AppliesTo(name, Target(11)));
        Assert.False(CollectorCatalog.AppliesTo(name, Target(12)));
        Assert.True(CollectorCatalog.AppliesTo(name, Target(16)));
        Assert.True(CollectorCatalog.AppliesTo(name, Target(0)));
        Assert.True(CollectorCatalog.AppliesTo(name, Target(12, azureManagedInstance: true)));
    }

    [Theory]
    [InlineData(13)]
    [InlineData(14)]
    public void Pvs_stats_NeedsSqlServer2019_SoA2017TargetDoesNotRunIt(int major)
    {
        Assert.False(CollectorCatalog.AppliesTo("pvs_stats", Target(major)));
        Assert.True(CollectorCatalog.AppliesTo("pvs_stats", Target(15)));
    }

    [Theory]
    [MemberData(nameof(FloorGatedCollectors))]
    public void BelowFloorServer_ReadsAsNotCollected_WithTheVersionReason(string name)
    {
        var message = CollectorEngineCapability.NotCollectedMessage("srv", 2, null, name, AwsRdsKind.None, sqlMajorVersion: 11);

        Assert.NotNull(message);
        Assert.Contains("SQL Server 2012", message);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, message);
    }

    [Fact]
    public void Pvs_stats_On2017_SaysItNeedsSqlServer2019()
    {
        var message = CollectorEngineCapability.NotCollectedMessage("srv", 2, null, "pvs_stats", AwsRdsKind.None, sqlMajorVersion: 14);

        Assert.NotNull(message);
        Assert.Contains("SQL Server 2017", message);
        Assert.Contains("SQL Server 2019", message);
    }

    [Theory]
    [InlineData(16, 2)]
    [InlineData(0, 2)]
    [InlineData(12, 5)]
    [InlineData(12, 8)]
    public void SupportedUnknownAndAzureServers_MakeNoVersionClaim(int major, int edition)
    {
        Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", edition, null, "query_snapshots", AwsRdsKind.None, sqlMajorVersion: major));
    }

    [Fact]
    public void CollectionHealth_ListsABelowFloorServersFailingCollectorAsNotCollected()
    {
        var rows = new List<CollectorHealthRow>
        {
            new() { CollectorName = "query_snapshots" },
            new() { CollectorName = "wait_stats" },
        };

        var (running, notCollected) = McpEngineCapability.SplitCollectionHealthByEngine("srv", 2, AwsRdsKind.None, rows, sqlMajorVersion: 11);

        Assert.Equal(new[] { "wait_stats" }, running.Select(r => r.CollectorName));
        Assert.Single(notCollected);

        var (runningOnSupported, none) = McpEngineCapability.SplitCollectionHealthByEngine("srv", 2, AwsRdsKind.None, rows, sqlMajorVersion: 16);
        Assert.Equal(2, runningOnSupported.Count);
        Assert.Empty(none);
    }

    [Fact]
    public void ServerTabHealthGrid_MarksTheRowNotCollected_OnABelowFloorServer()
    {
        var rows = new List<CollectorHealthRow> { new() { CollectorName = "file_io_stats" } };

        ServerTab.ApplyEngineGate(rows, "srv", 2, AwsRdsKind.None, sqlMajorVersion: 12);

        Assert.True(rows[0].EngineGated);
        Assert.Contains("SQL Server 2014", rows[0].EngineNotCollectedMessage);
    }
}
