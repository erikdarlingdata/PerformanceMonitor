/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using ViewerHealthRow = PerformanceMonitor.Darling.Viewer.CollectorHealthRow;

namespace Darling.Tests;

/// <summary>
/// #9099: a SQL Server older than the supported floor does not run the collectors whose SQL needs a newer engine, and every Darling surface
/// that says "not collected" carries the probed major, so the reason names the server's version and the supported versions, the same words
/// Lite gives. A supported major, Azure SQL Database and Managed Instance (major 12 with edition 5 or 8) and an unprobed server (0) are unchanged.
/// </summary>
public sealed class BelowFloorCollectorDarlingPureTests
{
    private const int SqlServerEdition = 2;

    public static IEnumerable<object[]> FlooredCollectors() =>
        new[] { "query_snapshots", "procedure_stats", "file_io_stats", "database_size_stats", "ag_replica_states", "server_properties" }
            .Select(name => new object[] { name });

    [Theory]
    [MemberData(nameof(FlooredCollectors))]
    public void TheMcpMessage_NamesTheVersionAndTheSupportedVersions_OnAMajor11Server(string collector)
    {
        var message = DarlingEngineCapability.NotCollectedMessage("srv", SqlServerEdition, MonitoredEngineKind.SqlServer, collector, AwsRdsKind.None, sqlMajorVersion: 11);

        Assert.NotNull(message);
        Assert.Contains("SQL Server 2012", message, StringComparison.Ordinal);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(13, SqlServerEdition)]
    [InlineData(16, SqlServerEdition)]
    [InlineData(0, SqlServerEdition)]
    [InlineData(12, 5)]
    [InlineData(12, 8)]
    public void ASupportedUnprobedOrAzureServer_MakesNoVersionClaim(int major, int edition)
    {
        Assert.Null(DarlingEngineCapability.NotCollectedMessage("srv", edition, MonitoredEngineKind.SqlServer, "query_snapshots", AwsRdsKind.None, sqlMajorVersion: major));
    }

    [Fact]
    public void TheHealthGate_ListsTheFlooredCollectorsOfAMajor11Server_AndNoneOfAMajor16OrUnprobedOne()
    {
        var below = FleetEngineGatedCollectors.For(SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 11);
        Assert.Contains("query_snapshots", below);
        Assert.Contains("file_io_stats", below);
        Assert.DoesNotContain("wait_stats", below);

        /* The PostgreSQL collectors are gated off a SQL Server target by its kind, so the set is never empty: what a major adds is the floored ones. */
        Assert.DoesNotContain("file_io_stats", FleetEngineGatedCollectors.For(SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 16));
        Assert.DoesNotContain("file_io_stats", FleetEngineGatedCollectors.For(SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 0));
        Assert.DoesNotContain("query_snapshots", FleetEngineGatedCollectors.For(5, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 12));
    }

    [Fact]
    public void TheServicesHealthRows_CarryTheVersionReason_ForAFlooredCollectorThatLogged()
    {
        var logged = new[] { new CollectorHealth { CollectorName = "file_io_stats" }, new CollectorHealth { CollectorName = "wait_stats" } };
        var names = FleetEngineGatedCollectors.For(SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 11);
        var gate = new DarlingGatedCollectorRows.EngineGate("srv", SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, names, null, 11);

        var rows = gate.NotCollectedRows(logged);

        var only = Assert.Single(rows);
        var json = JsonSerializer.Serialize(only);
        Assert.Contains("file_io_stats", json, StringComparison.Ordinal);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, json, StringComparison.Ordinal);
        Assert.Equal(new[] { "wait_stats" }, gate.RowsThatRun(logged).Select(r => r.CollectorName));
    }

    [Fact]
    public void TheViewersHealthRow_SaysNotCollectedWithTheVersionReason_OnAMajor11Server_AndKeepsItsBandOnAMajor16One()
    {
        var below = ViewerRow("file_io_stats");
        ViewerServerTab.ApplyEngineGate(new() { below }, "srv", SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 11);

        Assert.Equal("not_collected", below.DisplayStatus);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, below.NoteFormatted, StringComparison.Ordinal);

        var supported = ViewerRow("file_io_stats");
        ViewerServerTab.ApplyEngineGate(new() { supported }, "srv", SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, null, 16);
        Assert.Equal("FAILING", supported.DisplayStatus);
    }

    [Fact]
    public void ThePanelsNote_NamesTheVersion_OnAMajor11Server_AndIsAbsentOnAMajor16One()
    {
        var note = ViewerServerTab.EngineGapNote("srv", SqlServerEdition, MonitoredEngineKind.SqlServer, "file_io_stats", AwsRdsKind.None, 11);
        Assert.NotNull(note);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, note, StringComparison.Ordinal);

        Assert.Null(ViewerServerTab.EngineGapNote("srv", SqlServerEdition, MonitoredEngineKind.SqlServer, "file_io_stats", AwsRdsKind.None, 16));
    }

    [Fact]
    public void TheFleetLiveness_ListsAMajor11ServerAsNotApplicable_WithTheVersionReason()
    {
        var servers = new List<CollectorFleetLiveness.ServerFacts>
        {
            new(1, "old-box", null, SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, 11),
            new(2, "new-box", null, SqlServerEdition, MonitoredEngineKind.SqlServer, AwsRdsKind.None, 16),
        };

        var json = CollectorFleetLiveness.Build("query_snapshots", 24, DateTime.UtcNow.AddHours(-24), DateTime.UtcNow, servers, []);

        using var doc = JsonDocument.Parse(json);
        var notApplicable = doc.RootElement.GetProperty("not_applicable").EnumerateArray().ToList();
        var listed = Assert.Single(notApplicable);
        Assert.Equal("old-box", listed.GetProperty("server_name").GetString());
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, listed.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }

    private static ViewerHealthRow ViewerRow(string collector)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new ViewerHealthRow { CollectorName = collector, TotalRuns = 315, SuccessCount = 0, ErrorCount = 315, LastRunTime = last };
    }
}

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>#9099: the same facts through the registry and the real reads, against a store.</summary>
public sealed class BelowFloorCollectorDarlingLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string OldName = "below-floor-old";
    private const string NewName = "below-floor-new";
    private static readonly int OldId = ServerIdHelper.GetDeterministicHashCode(OldName);
    private static readonly int NewId = ServerIdHelper.GetDeterministicHashCode(NewName);

    [Fact]
    public async Task AMajor11Server_ReadsAsNotCollectedWithTheVersionReason_OnEveryReadPath_AndAMajor16ServerIsUnchanged()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the below-floor registry facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, OldId, OldName, 11, ct);
            await RegisterAsync(connection, NewId, NewName, 16, ct);
            await LogAsync(connection, OldId, OldName, "file_io_stats", ct);
            await LogAsync(connection, NewId, NewName, "file_io_stats", ct);

            Assert.Equal(11, (await DarlingEngineCapability.ReadServerEngineAsync(postgres, OldId, ct)).SqlMajorVersion);

            /* A tool read that came back empty. */
            var oldStatus = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, OldId, OldName, "file_io_stats", ct);
            Assert.NotNull(oldStatus);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(oldStatus!));
            Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, oldStatus, StringComparison.Ordinal);
            Assert.Null(await DarlingEngineCapability.NotCollectedStatusAsync(postgres, NewId, NewName, "file_io_stats", ct));

            /* A fleet read over servers that came back with no rows. */
            var reasons = await DarlingEngineCapability.NotCollectedReasonsAsync(postgres, [(OldId, OldName), (NewId, NewName)], "file_io_stats", ct);
            Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, reasons[OldId], StringComparison.Ordinal);
            Assert.DoesNotContain(NewId, reasons.Keys);

            /* get_collection_health for the server: the collector it logged before the floor read as not collected, with the reason. */
            using (var health = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, OldName, cancellationToken: ct)))
            {
                var row = health.RootElement.GetProperty("collectors").EnumerateArray().Single(r => r.GetProperty("collector").GetString() == "file_io_stats");
                Assert.Equal("not_collected", row.GetProperty("status").GetString());
                Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, row.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            using (var health = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, NewName, cancellationToken: ct)))
            {
                var row = health.RootElement.GetProperty("collectors").EnumerateArray().Single(r => r.GetProperty("collector").GetString() == "file_io_stats");
                Assert.NotEqual("not_collected", row.GetProperty("status").GetString());
            }

            /* get_collection_health across the fleet. */
            using (var fleet = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, "*", collector_name: "file_io_stats", cancellationToken: ct)))
            {
                var notApplicable = fleet.RootElement.GetProperty("not_applicable").EnumerateArray().Single();
                Assert.Equal(OldName, notApplicable.GetProperty("server_name").GetString());
                Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, notApplicable.GetProperty("reason").GetString(), StringComparison.Ordinal);
            }

            /* The viewer's own read of the registry fills the health gate. */
            var gated = await FleetEngineGatedCollectors.ReadAsync(postgres, 30, ct);
            Assert.Contains("file_io_stats", gated[OldId]);
            Assert.DoesNotContain("file_io_stats", gated[NewId]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, int serverId, string serverName, int major, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date)
            VALUES ($1, $2, $2, TRUE, 2, $3, $4, $5, $5)
            """, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(major);
        command.Parameters.AddWithValue(MonitoredEngineKind.SqlServer);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task LogAsync(NpgsqlConnection connection, int serverId, string serverName, string collector, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
            VALUES ($1, $2, $3, $4, $5, 'SUCCESS', 100, 10)
            """, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(collector);
        await command.ExecuteNonQueryAsync(ct);
    }
}
