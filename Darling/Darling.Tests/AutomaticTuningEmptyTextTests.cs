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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Tests;
using Xunit;
using static PerformanceMonitor.Collectors.CollectorRuntimePrecondition;

namespace Darling.Tests;

/// <summary>
/// #7813: the empty answer of get_plan_corrections names which case applies: the version has no automatic tuning, no database has
/// Query Store on, or nothing has been recommended so far. The sentence is one shared function, and the live tests (DARLING_TEST_PG)
/// drive the tool against a store whose registry and newest Query Store snapshot set each case up.
/// </summary>
public sealed class AutomaticTuningEmptyTextTests
{
    private const string Cleanup =
        "DELETE FROM query_store_health WHERE server_id = {0}; DELETE FROM plan_correction WHERE server_id = {0}; " +
        "DELETE FROM collection_log WHERE server_id = {0}";

    private static readonly CollectorTargetInfo Sql2019 = new() { SqlMajorVersion = 15 };

    private static QueryStoreDatabaseState State(string database, string? actualState) => new(database, actualState);

    private static string Message(string json) => JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;

    [Theory]
    [InlineData(12, true)]
    [InlineData(13, true)]
    [InlineData(14, false)]
    [InlineData(16, false)]
    [InlineData(0, false)]
    public void TheSentenceNamesTheVersion_OnlyWhenTheVersionHasNoAutomaticTuning(int major, bool noAutomaticTuning)
    {
        var message = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = major }, null);

        if (noAutomaticTuning)
        {
            Assert.Contains("has no automatic tuning", message, StringComparison.Ordinal);
            Assert.Contains("SQL Server 2017", message, StringComparison.Ordinal);
        }
        else
        {
            Assert.StartsWith("No plan correction data collected for this server yet", message, StringComparison.Ordinal);
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

        Assert.StartsWith("No plan correction data collected for this server yet", PlanCorrectionCollector.NoRowsMessage(target, null), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSentenceSaysQueryStoreIsOff_OnlyWhenEveryDatabaseInTheNewestSnapshotIsOff()
    {
        var allOff = PlanCorrectionCollector.NoRowsMessage(Sql2019, new[] { State("DbA", "OFF"), State("DbB", "off") });
        Assert.Contains("no database has Query Store on", allOff, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing has been recommended", allOff, StringComparison.Ordinal);

        var oneOn = PlanCorrectionCollector.NoRowsMessage(Sql2019, new[] { State("DbA", "OFF"), State("DbB", "READ_WRITE") });
        Assert.Contains("Query Store is on", oneOn, StringComparison.Ordinal);
        Assert.Contains("nothing has been recommended", oneOn, StringComparison.Ordinal);
        Assert.DoesNotContain("no database has Query Store on", oneOn, StringComparison.Ordinal);
    }

    /// <summary>With no snapshot, or a snapshot that does not say, the sentence claims nothing about Query Store either way.</summary>
    [Fact]
    public void WithoutAQueryStoreSnapshot_TheSentenceSaysOnlyThatNothingHasBeenRecommended()
    {
        foreach (var message in new[]
        {
            PlanCorrectionCollector.NoRowsMessage(Sql2019, null),
            PlanCorrectionCollector.NoRowsMessage(Sql2019, Array.Empty<QueryStoreDatabaseState>()),
            PlanCorrectionCollector.NoRowsMessage(Sql2019, new[] { State("DbA", null) }),
        })
        {
            Assert.Contains("nothing has been recommended", message, StringComparison.Ordinal);
            Assert.DoesNotContain("Query Store", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheVersionSentence_WinsOverAQueryStoreSnapshot()
    {
        var message = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }, new[] { State("DbA", "READ_WRITE") });

        Assert.Contains("has no automatic tuning", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Query Store", message, StringComparison.Ordinal);
    }

    /// <summary>The page prints the sentence as it is, so it carries no system view, store column, tool name or em dash.</summary>
    [Fact]
    public void EverySentence_IsFitForThePage()
    {
        foreach (var message in new[]
        {
            PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }, null),
            PlanCorrectionCollector.NoRowsMessage(Sql2019, new[] { State("DbA", "OFF") }),
            PlanCorrectionCollector.NoRowsMessage(Sql2019, new[] { State("DbA", "READ_WRITE") }),
            PlanCorrectionCollector.NoRowsMessage(Sql2019, null),
        })
        {
            Assert.DoesNotContain("sys.", message, StringComparison.Ordinal);
            Assert.DoesNotContain("_", message, StringComparison.Ordinal);
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
            Assert.Equal(PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }, null), Message(json));
        }, Cleanup);

    [Fact]
    public Task ThroughTheTool_AServerWithQueryStoreOffEverywhere_HearsThatNoDatabaseHasItOn() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a7813-qs-off", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedQueryStoreHealthAsync(connection, ct, now, serverId, serverName, ("DbA", "OFF"), ("DbB", "OFF"));

            var json = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, serverName, 24, 25, null, false, DatabaseFilter.All, null, ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
            Assert.Equal(PlanCorrectionCollector.NoRowsMessage(Sql2019, new[] { State("DbA", "OFF"), State("DbB", "OFF") }), Message(json));
            Assert.Contains("no database has Query Store on", Message(json), StringComparison.Ordinal);
        }, Cleanup);

    [Fact]
    public Task ThroughTheTool_AServerWithQueryStoreOn_HearsThatNothingHasBeenRecommended() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a7813-qs-on", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedQueryStoreHealthAsync(connection, ct, now, serverId, serverName, ("DbA", "OFF"), ("DbB", "READ_WRITE"));

            var json = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, serverName, 24, 25, null, false, DatabaseFilter.All, null, ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("Query Store is on", Message(json), StringComparison.Ordinal);
            Assert.Contains("nothing has been recommended", Message(json), StringComparison.Ordinal);
        }, Cleanup);

    private static async Task SeedQueryStoreHealthAsync(
        Npgsql.NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime now, int serverId, string serverName, params (string Database, string ActualState)[] rows)
    {
        foreach (var (database, actualState) in rows)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                CollectionIdGenerator.Next(), now, serverId, serverName, database, actualState, "READ_WRITE", 0, 50L, 1000L, "AUTO", 30L, 200L, 60L);
        }
    }
}
