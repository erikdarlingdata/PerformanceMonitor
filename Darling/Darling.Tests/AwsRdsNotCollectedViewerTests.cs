/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6894: the Viewer's Running Jobs grid and Agent Alerts panel on an Amazon RDS server. The collectors never run there, so the empty
/// surface says what the MCP tools say (not collected, naming RDS), not the "possible cause" text that implies a setup step could fix it.
/// </summary>
public sealed class AwsRdsNotCollectedViewerTests
{
    private const string ServerName = "rds-viewer-server";
    private const int OnPremEngineEdition = 3;
    private static readonly DateTime ServerLastCollected = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("deadlocks")]
    [InlineData("blocked_process_report")]
    public void AnEmptyDeadlockSurfaceOnAnRdsWebOrExpressServer_NamesTheEditionLimit_AndAnEnterpriseRdsServerMakesNoClaim(string collector)
    {
        var web = ViewerServerTab.EngineGapNote(ServerName, 2, MonitoredEngineKind.SqlServer, collector, AwsRdsKind.RdsWebOrExpress);
        Assert.NotNull(web);
        Assert.Contains("Amazon RDS for SQL Server Web or Express edition", web, StringComparison.Ordinal);
        Assert.Equal(CollectorEngineCapability.NotCollectedMessage(ServerName, 2, MonitoredEngineKind.SqlServer, collector, AwsRdsKind.RdsWebOrExpress), web);

        Assert.Null(ViewerServerTab.EngineGapNote(ServerName, 3, MonitoredEngineKind.SqlServer, collector, AwsRdsKind.Rds));
        Assert.Null(ViewerServerTab.EngineGapNote(ServerName, 3, MonitoredEngineKind.SqlServer, collector, AwsRdsKind.None));
    }

    [Theory]
    [InlineData("running_jobs")]
    [InlineData("agent_alerts")]
    public void AnEmptySurfaceOnAnRdsServer_NamesRds_AndIsTheMcpSentence(string collector)
    {
        var state = ViewerServerTab.EngineGapStateFromRuns(
            ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, collector, 0, null, ServerLastCollected, null, awsRdsKind: AwsRdsKind.Rds);

        Assert.Equal(Visibility.Visible, state.Visibility);
        Assert.Equal(
            CollectorEngineCapability.NotCollectedMessage(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, collector, awsRdsKind: AwsRdsKind.Rds),
            state.Text);
        Assert.Contains("Amazon RDS", state.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("Possible cause", state.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSameFactsWithNoRdsFact_KeepTheOldPossibleCauseText()
    {
        var state = ViewerServerTab.EngineGapStateFromRuns(
            ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs", 0, null, ServerLastCollected, null, awsRdsKind: AwsRdsKind.None);

        Assert.Equal(Visibility.Visible, state.Visibility);
        Assert.Contains(CollectorRuntimePrecondition.RunningJobsPossibleCauses, state.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void AnRdsServer_ReadsNothingFromTheCollectionLog_AndAGridWithRowsShowsNoNote()
    {
        Assert.False(ViewerServerTab.NeedsCollectorRunRead(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs", 0, awsRdsKind: AwsRdsKind.Rds));
        Assert.True(ViewerServerTab.NeedsCollectorRunRead(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs", 0, awsRdsKind: AwsRdsKind.None));

        var withRows = ViewerServerTab.EngineGapStateFromRuns(
            ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs", 3, null, ServerLastCollected, null, awsRdsKind: AwsRdsKind.Rds);
        Assert.Equal(Visibility.Collapsed, withRows.Visibility);
    }

    [Fact]
    public async Task ReadEngineGapState_OnAnRdsServer_AnswersWithoutTouchingTheStore()
    {
        var state = await ViewerServerTab.ReadEngineGapStateAsync(
            (_, _) => throw new InvalidOperationException("collection_log must not be read"), null, 1, ServerName,
            OnPremEngineEdition, MonitoredEngineKind.SqlServer, "agent_alerts", 0,
            (_, _) => throw new InvalidOperationException("collection_log must not be read"), awsRdsKind: AwsRdsKind.Rds);

        Assert.Equal(Visibility.Visible, state.Visibility);
        Assert.Contains("Amazon RDS", state.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void AnRdsFact_DoesNotChangeACollectorRdsDoesNotGate()
    {
        Assert.Null(ViewerServerTab.EngineGapNote(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "wait_stats", awsRdsKind: AwsRdsKind.Rds));
        Assert.Null(ViewerServerTab.EngineGapNote(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "job_history", awsRdsKind: AwsRdsKind.Rds));
    }
}

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// #6894: the Viewer reads the registry's RDS fact into its server list, and the Agent Alerts read answers not collected for the RDS server
/// only. A store older than V225 has no column: the list still loads and no server reads as RDS.
/// </summary>
public sealed class AwsRdsNotCollectedViewerLiveTests
{
    private static async Task ExecAsync(NpgsqlDataSource owner, string sql, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static ServerRuntime Runtime(int id, string name, bool isAwsRds) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name + "-host" },
        ConnectionString = "Server=" + name,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16, IsAwsRds = isAwsRds },
        StorageName = name,
        ServerId = id,
        EngineEdition = OnPremEdition,
    };

    private const int OnPremEdition = 3;

    [Fact]
    public async Task TheViewerServerList_CarriesTheRdsFact_AndTheAgentAlertsReadAnswersNotCollectedForThatServerOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer RDS fact test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await using var owner = NpgsqlDataSource.Create(ownerString);
        await DarlingObservability.UpsertServerAsync(owner, Runtime(1, "viewer-rds", isAwsRds: true), null, ct);
        await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "viewer-box", isAwsRds: false), null, ct);
        await ExecAsync(owner, @"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, engine)
VALUES (1, 'viewer-rds', 'viewer-rds-host', 'integrated', TRUE, 'sqlserver'),
       (2, 'viewer-box', 'viewer-box-host', 'integrated', TRUE, 'sqlserver')", ct);

        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            foreach (var list in new[] { await viewer.GetManagedServersAsync(ct), await viewer.GetServersAsync(ct) })
            {
                Assert.Equal(AwsRdsKind.Rds, list.Find(s => s.DisplayName == "viewer-rds")!.AwsRdsKind);
                Assert.Equal(AwsRdsKind.None, list.Find(s => s.DisplayName == "viewer-box")!.AwsRdsKind);
            }

            var servers = await viewer.GetManagedServersAsync(ct);
            var rds = await viewer.ReadAgentAlertsAsync(servers.Find(s => s.DisplayName == "viewer-rds")!, DateTime.UtcNow, cancellationToken: ct);
            Assert.Equal(ViewerAgentAlertsRead.NotCollected, rds.Status);
            Assert.Contains("Amazon RDS", rds.Message, StringComparison.Ordinal);

            var box = await viewer.ReadAgentAlertsAsync(servers.Find(s => s.DisplayName == "viewer-box")!, DateTime.UtcNow, cancellationToken: ct);
            Assert.NotEqual(ViewerAgentAlertsRead.NotCollected, box.Status);
        }

        /* A store older than the column: the list still loads and nobody reads as RDS. */
        await ExecAsync(owner, "ALTER TABLE collect.servers DROP COLUMN is_aws_rds", ct);
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            var servers = await viewer.GetManagedServersAsync(ct);
            Assert.Equal(2, servers.Count);
            Assert.All(servers, s => Assert.Equal(AwsRdsKind.None, s.AwsRdsKind));
        }
    }
}
