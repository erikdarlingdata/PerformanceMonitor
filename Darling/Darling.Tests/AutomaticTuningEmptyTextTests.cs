/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Tests;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7813: the empty answer of get_plan_corrections names which case applies. A version before SQL Server 2017 has no automatic tuning.
/// On any other version an empty store means the collector has not stored a cycle (every cycle writes an enablement row per database,
/// Query Store on or off), so the sentence points at collection health and claims nothing about Query Store or about what has been
/// recommended. The sentence is one shared function, and the live tests (DARLING_TEST_PG) drive the tool against a store whose
/// registry sets each case up.
/// </summary>
public sealed class AutomaticTuningEmptyTextTests
{
    private const string Cleanup = "DELETE FROM plan_correction WHERE server_id = {0}; DELETE FROM collection_log WHERE server_id = {0}";

    private static readonly CollectorTargetInfo Sql2019 = new() { SqlMajorVersion = 15 };

    private static string Message(string json) => JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;

    [Theory]
    [InlineData(12, true)]
    [InlineData(13, true)]
    [InlineData(14, false)]
    [InlineData(16, false)]
    [InlineData(0, false)]
    public void TheSentenceNamesTheVersion_OnlyWhenTheVersionHasNoAutomaticTuning(int major, bool noAutomaticTuning)
    {
        var message = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = major });

        if (noAutomaticTuning)
        {
            Assert.Contains("has no automatic tuning", message, StringComparison.Ordinal);
            Assert.Contains("SQL Server 2017", message, StringComparison.Ordinal);
        }
        else
        {
            Assert.StartsWith("No plan correction data collected for this server yet", message, StringComparison.Ordinal);
            Assert.Contains("has not stored a cycle", message, StringComparison.Ordinal);
            Assert.DoesNotContain("has no automatic tuning", message, StringComparison.Ordinal);
        }
    }

    /// <summary>Azure SQL Database and Managed Instance report a low major version yet have automatic tuning.</summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void AnAzureServer_IsNeverTakenForAnOldVersion(bool azureSqlDb, bool managedInstance)
    {
        var target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDb, IsAzureManagedInstance = managedInstance, SqlMajorVersion = 12 };

        Assert.StartsWith("No plan correction data collected for this server yet", PlanCorrectionCollector.NoRowsMessage(target), StringComparison.Ordinal);
    }

    /// <summary>
    /// A 2017+ server with no rows has no stored cycle: a cycle that ran writes an enablement row for every database whatever its Query
    /// Store state, so an empty store is not a Query Store verdict and not an all-clear on recommendations (#3553).
    /// </summary>
    [Fact]
    public void OnAVersionWithAutomaticTuning_TheSentencePointsAtCollectionHealth_AndClaimsNothingAboutQueryStoreOrRecommendations()
    {
        var message = PlanCorrectionCollector.NoRowsMessage(Sql2019);

        Assert.Contains("has not stored a cycle", message, StringComparison.Ordinal);
        Assert.Contains("ollection health", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Query Store", message, StringComparison.Ordinal);
        Assert.DoesNotContain("recommended so far", message, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing has been recommended", message, StringComparison.Ordinal);
    }

    /// <summary>The page prints the sentence as it is, so it carries no system view, store column, tool name or em dash.</summary>
    [Fact]
    public void EverySentence_IsFitForThePage()
    {
        foreach (var message in new[]
        {
            PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }),
            PlanCorrectionCollector.NoRowsMessage(Sql2019),
        })
        {
            Assert.DoesNotContain("sys.", message, StringComparison.Ordinal);
            Assert.DoesNotContain("_", message, StringComparison.Ordinal);
            Assert.DoesNotContain("get_", message, StringComparison.Ordinal);
            Assert.DoesNotContain("—", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public Task ThroughTheTool_AServerBefore2017_HearsThatItsVersionHasNoAutomaticTuning() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a7813-old-version", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_major_version = 13 WHERE server_id = $1", serverId);

            var json = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, serverName, 24, 25, null, false, DatabaseFilter.All, null, ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
            Assert.Equal(PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }), Message(json));
        }, Cleanup);

    /// <summary>The reachable 2017+ case: a registered server whose collector has stored no plan correction cycle.</summary>
    [Fact]
    public Task ThroughTheTool_AServerOn2017OrLater_WithNoStoredCycle_HearsThatTheCollectorHasNotStoredOne() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a7813-no-cycle", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_major_version = 15 WHERE server_id = $1", serverId);

            var json = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, serverName, 24, 25, null, false, DatabaseFilter.All, null, ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
            Assert.Equal(PlanCorrectionCollector.NoRowsMessage(Sql2019), Message(json));
            Assert.Contains("has not stored a cycle", Message(json), StringComparison.Ordinal);
            Assert.DoesNotContain("Query Store", Message(json), StringComparison.Ordinal);
        }, Cleanup);
}
