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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6894: on an Amazon RDS target the collectors RDS gates off (running_jobs, agent_status, agent_alerts) never run, and the reads answered a
/// precondition that says a setup step on the monitored server can fix it. The connect now records the RDS fact on the registry (V225) and the
/// reads answer not_collected naming RDS, the way they do for an Azure SQL Database. A server with no RDS fact keeps the old answers.
/// </summary>
public sealed class AwsRdsNotCollectedPureTests
{
    [Theory]
    [InlineData("running_jobs")]
    [InlineData("agent_status")]
    [InlineData("agent_alerts")]
    public void ARdsServer_IsNotCollected_ForACollectorRdsGatesOff(string collector)
    {
        var message = CollectorEngineCapability.NotCollectedMessage("example-rds-a", 3, MonitoredEngineKind.SqlServer, collector, awsRdsKind: AwsRdsKind.Rds);

        Assert.NotNull(message);
        Assert.Contains("Amazon RDS", message, StringComparison.Ordinal);
        Assert.Contains(collector, message, StringComparison.Ordinal);
        Assert.Contains("never will", message, StringComparison.Ordinal);
        Assert.Contains("permanent engine capability gap", message, StringComparison.Ordinal);
        /* #6908: the sentence is shown to people, so it is plain words with no code name, no dashes and no semicolons. */
        Assert.Contains("collector does not run on Amazon RDS", message, StringComparison.Ordinal);
        Assert.DoesNotContain("AppliesTo", message, StringComparison.Ordinal);
        Assert.DoesNotContain("gate", message, StringComparison.Ordinal);
        Assert.DoesNotContain("—", message, StringComparison.Ordinal);
        Assert.DoesNotContain(";", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("running_jobs")]
    [InlineData("agent_alerts")]
    public void AServerWithNoRdsFact_MakesNoClaim(string collector) =>
        Assert.Null(CollectorEngineCapability.NotCollectedMessage("example-box-a", 3, MonitoredEngineKind.SqlServer, collector, awsRdsKind: AwsRdsKind.None));

    [Theory]
    [InlineData("wait_stats")]
    [InlineData("job_history")]
    [InlineData("server_properties")]
    public void ARdsServer_StillCollects_WhatRdsDoesNotGateOff(string collector) =>
        Assert.Null(CollectorEngineCapability.NotCollectedMessage("example-rds-a", 3, MonitoredEngineKind.SqlServer, collector, awsRdsKind: AwsRdsKind.Rds));

    [Fact]
    public void TheRdsClaim_IsDerivedFromTheCollectorsOwnGate_AndSkipsPostgresCollectors()
    {
        Assert.False(CollectorEngineCapability.IsCollectedOnAwsRds("running_jobs", null, AwsRdsKind.Rds));
        Assert.True(CollectorEngineCapability.IsCollectedOnAwsRds("wait_stats", null, AwsRdsKind.Rds));

        /* An RDS flag on a PostgreSQL target (set from its endpoint) is not this fact, and a PostgreSQL collector's SQL Server sweep is empty. */
        var pgCollector = CollectorCatalog.All.First(c => c.Name == "pg_database_stats").Name;
        Assert.Null(CollectorEngineCapability.NotCollectedMessage("pg-01", 0, MonitoredEngineKind.Postgres, pgCollector, awsRdsKind: AwsRdsKind.Rds));
        Assert.True(CollectorEngineCapability.IsCollectedOnAwsRds(pgCollector, null, AwsRdsKind.Rds));
    }

    [Fact]
    public void FleetLiveness_ListsAnRdsServerAsNotApplicable_AndARegularOneAsApplicable()
    {
        var now = DateTime.UtcNow;
        var json = CollectorFleetLiveness.Build(
            "running_jobs", 24, now.AddHours(-24), now,
            new[]
            {
                new CollectorFleetLiveness.ServerFacts(1, "example-rds-a", null, 3, MonitoredEngineKind.SqlServer, AwsRdsKind.Rds),
                new CollectorFleetLiveness.ServerFacts(2, "example-box-a", null, 3, MonitoredEngineKind.SqlServer, AwsRdsKind.None),
            },
            Array.Empty<CollectorFleetLiveness.RunFacts>());

        using var doc = JsonDocument.Parse(json);
        var notApplicable = doc.RootElement.GetProperty("not_applicable");
        Assert.Equal(1, notApplicable.GetArrayLength());
        Assert.Equal("example-rds-a", notApplicable[0].GetProperty("server_name").GetString());
        Assert.Contains("Amazon RDS", notApplicable[0].GetProperty("reason").GetString(), StringComparison.Ordinal);
    }
}

[Collection("live-postgres")]
public sealed class AwsRdsNotCollectedLivePostgresTests
{
    private const string RdsName = "darling-rds-fact-rds";
    private const string BoxName = "darling-rds-fact-box";
    private static readonly int RdsId = ServerIdHelper.GetDeterministicHashCode(RdsName);
    private static readonly int BoxId = ServerIdHelper.GetDeterministicHashCode(BoxName);

    [Fact]
    public async Task TheRegistryRdsFact_TurnsBothAgentReadsIntoNotCollected_AndOnlyForTheRdsServer()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live RDS fact test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            /* The connect upsert itself, so the column list and the parameter order are the shipped ones. */
            await UpsertAsync(connection, ct, RdsId, RdsName, isAwsRds: true);
            await UpsertAsync(connection, ct, BoxId, BoxName, isAwsRds: false);
            Assert.Equal(AwsRdsKind.Rds, (await DarlingEngineCapability.ReadServerEngineAsync(postgres, RdsId, ct)).AwsRdsKind);
            Assert.Equal(AwsRdsKind.None, (await DarlingEngineCapability.ReadServerEngineAsync(postgres, BoxId, ct)).AwsRdsKind);

            var rdsAlerts = await DarlingMcpAgentAlertTools.GetAgentAlerts(postgres, RdsName, cancellationToken: ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(rdsAlerts));
            Assert.Contains("Amazon RDS", rdsAlerts, StringComparison.Ordinal);
            Assert.DoesNotContain("Possible cause", rdsAlerts, StringComparison.Ordinal);

            var rdsJobs = await DarlingMcpJobTools.GetRunningJobs(postgres, RdsName, ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(rdsJobs));
            Assert.Contains("Amazon RDS", rdsJobs, StringComparison.Ordinal);

            /* The same empty reads on a server with no RDS fact keep their old answer. */
            var boxAlerts = await DarlingMcpAgentAlertTools.GetAgentAlerts(postgres, BoxName, cancellationToken: ct);
            Assert.NotEqual("not_collected", DarlingMcpTestData.StatusOf(boxAlerts));
            var boxJobs = await DarlingMcpJobTools.GetRunningJobs(postgres, BoxName, ct);
            Assert.NotEqual("not_collected", DarlingMcpTestData.StatusOf(boxJobs));

            /* A reconnect that no longer finds RDS corrects the fact: the re-connect arm writes it too. */
            await UpsertAsync(connection, ct, RdsId, RdsName, isAwsRds: false);
            Assert.Equal(AwsRdsKind.None, (await DarlingEngineCapability.ReadServerEngineAsync(postgres, RdsId, ct)).AwsRdsKind);

            /* The fleet form of collection health lists the RDS server as not applicable with the same reason. */
            await UpsertAsync(connection, ct, RdsId, RdsName, isAwsRds: true);
            var fleet = await DarlingMcpDataTools.GetCollectionHealth(postgres, "*", collector_name: "running_jobs", cancellationToken: ct);
            using var doc = JsonDocument.Parse(fleet);
            var notApplicable = doc.RootElement.GetProperty("not_applicable").EnumerateArray()
                .Where(e => e.GetProperty("server_name").GetString() == RdsName).ToList();
            Assert.Single(notApplicable);
            Assert.Contains("Amazon RDS", notApplicable[0].GetProperty("reason").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain(doc.RootElement.GetProperty("not_applicable").EnumerateArray(),
                e => e.GetProperty("server_name").GetString() == BoxName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AnRdsWebServer_KeepsItsRealEngineEdition_AndItsDeadlockReadIsNotCollected_WhileAnRdsEnterpriseServerMakesNoClaim()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live RDS fact test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            /* The Web instance reports EngineEdition 2, the same as Standard: the registry keeps it, and the Web or Express fact is its own column (V226). */
            await UpsertAsync(connection, ct, RdsId, RdsName, isAwsRds: true, webOrExpress: true, engineEdition: 2);
            await UpsertAsync(connection, ct, BoxId, BoxName, isAwsRds: true, webOrExpress: false, engineEdition: 2);
            var web = await DarlingEngineCapability.ReadServerEngineAsync(postgres, RdsId, ct);
            Assert.Equal(2, web.EngineEdition);
            Assert.Equal(AwsRdsKind.RdsWebOrExpress, web.AwsRdsKind);
            Assert.Equal(AwsRdsKind.Rds, (await DarlingEngineCapability.ReadServerEngineAsync(postgres, BoxId, ct)).AwsRdsKind);

            var webDeadlocks = await DarlingMcpBlockingTools.GetDeadlocks(postgres, RdsName, cancellationToken: ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(webDeadlocks));
            Assert.Contains("Amazon RDS for SQL Server Web or Express edition", webDeadlocks, StringComparison.Ordinal);

            var enterpriseDeadlocks = await DarlingMcpBlockingTools.GetDeadlocks(postgres, BoxName, cancellationToken: ct);
            Assert.NotEqual("not_collected", DarlingMcpTestData.StatusOf(enterpriseDeadlocks));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task UpsertAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, bool isAwsRds, bool webOrExpress = false, int engineEdition = 3)
    {
        using var command = new NpgsqlCommand(DarlingObservability.UpsertServerSql, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(engineEdition);
        command.Parameters.AddWithValue(15);
        command.Parameters.AddWithValue(MonitoredEngineKind.SqlServer);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(0m);
        command.Parameters.AddWithValue(DBNull.Value);
        command.Parameters.AddWithValue(DBNull.Value);
        command.Parameters.AddWithValue(isAwsRds);
        command.Parameters.AddWithValue(webOrExpress);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM servers WHERE server_id IN ({RdsId}, {BoxId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
