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
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// #6454: the server-admin tools and the reads that follow them. <c>list_servers</c> read the observed registry, which the service
/// mirrors from the definition only on its next reload, so an edit or a remove showed up 40 to 60 seconds late; a removed server
/// left an all-null collector override row; and an edit could not clear the monthly cost.
/// </summary>
public sealed class ServerAdminStaleReadsLiveTests
{
    private static ServerRuntime Runtime(int id, string name) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name + "-host" },
        ConnectionString = "Server=" + name,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = name,
        ServerId = id,
        EngineEdition = 3,
    };

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Owner)> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-admin read tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        return (scratch, NpgsqlDataSource.Create(ownerString));
    }

    private static async Task ExecAsync(NpgsqlDataSource owner, string sql, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource owner, string sql, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task<string[]> ListedNamesAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(owner, ct));
        return doc.RootElement.TryGetProperty("servers", out var servers)
            ? servers.EnumerateArray().Select(s => s.GetProperty("display_name").GetString()!).ToArray()
            : [];
    }

    [Fact]
    public async Task ListServers_ShowsAnEditedNameAtOnce_AndDropsARemovedServerAtOnce_WithoutWaitingForTheServiceReload()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, owner) = await OpenAsync(ct);
        await using var scratchHold = scratch;
        await using var ownerHold = owner;
        {
            /* The service has registered both servers (the observed row) and the definitions exist. */
            await DarlingObservability.UpsertServerAsync(owner, Runtime(11, "keep-host"), null, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(12, "drop-host"), null, ct);
            await ExecAsync(owner, @"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, engine)
VALUES (11, 'keep-host', 'keep-host', 'integrated', TRUE, 'sqlserver'),
       (12, 'drop-host', 'drop-host', 'integrated', TRUE, 'sqlserver')", ct);
            Assert.Equal(new[] { "drop-host", "keep-host" }, (await ListedNamesAsync(owner, ct)).OrderBy(n => n, StringComparer.Ordinal).ToArray());

            /* The edit changes the definition only; the observed row still holds the old name until the service reloads. */
            await ExecAsync(owner, "UPDATE config.config_monitored_servers SET name = 'Orders' WHERE server_id = 11", ct);
            Assert.Equal("keep-host", await ScalarTextAsync(owner, "SELECT display_name FROM collect.servers WHERE server_id = 11", ct));
            Assert.Contains("Orders", await ListedNamesAsync(owner, ct));
            Assert.DoesNotContain("keep-host", await ListedNamesAsync(owner, ct));

            /* remove_server deletes the definition; the observed row is kept (history anchor) and still enabled. */
            using var removed = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(owner, "drop-host"));
            Assert.Equal("removed", removed.RootElement.GetProperty("status").GetString());
            Assert.Equal(1L, await CountAsync(owner, "SELECT count(*) FROM collect.servers WHERE server_id = 12 AND is_enabled", ct));
            Assert.Equal(new[] { "Orders" }, await ListedNamesAsync(owner, ct));

            /* disable_server flips the definition too. */
            await ExecAsync(owner, "UPDATE config.config_monitored_servers SET is_enabled = FALSE WHERE server_id = 11", ct);
            Assert.Empty(await ListedNamesAsync(owner, ct));
        }
    }

    private static async Task<string?> ScalarTextAsync(NpgsqlDataSource owner, string sql, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct));
    }

    [Fact]
    public async Task RemoveServer_ClearsTheServersCollectorOverridesAndRunTimes_AndKeepsTheFleetRowsAndOtherServers()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, owner) = await OpenAsync(ct);
        await using var scratchHold = scratch;
        await using var ownerHold = owner;
        {
            await SeedScheduleRowsAsync(owner, ct);

            using var removed = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(owner, "sched-drop"));
            Assert.Equal("removed", removed.RootElement.GetProperty("status").GetString());

            await AssertOnlyTheRemovedServersScheduleRowsAreGoneAsync(owner, ct);
        }
    }

    private static Task SeedScheduleRowsAsync(NpgsqlDataSource owner, CancellationToken ct) => ExecAsync(owner, @"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, engine)
VALUES (21, 'sched-keep', 'sched-keep', 'integrated', TRUE, 'sqlserver'),
       (22, 'sched-drop', 'sched-drop', 'integrated', TRUE, 'sqlserver');
INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, frequency_minutes, retention_days)
VALUES (NULL, 'wait_stats', TRUE, 5, NULL),
       (21, 'wait_stats', FALSE, NULL, NULL),
       (22, 'wait_stats', NULL, NULL, NULL),
       (22, 'memory_stats', FALSE, 10, 30);
INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute)
VALUES (NULL, 'daily_thing', 60), (21, 'daily_thing', 120), (22, 'daily_thing', 180);", ct);

    private static async Task AssertOnlyTheRemovedServersScheduleRowsAreGoneAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        Assert.Equal(0L, await CountAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE server_id = 22", ct));
        Assert.Equal(0L, await CountAsync(owner, "SELECT count(*) FROM config.config_collector_run_times WHERE server_id = 22", ct));
        Assert.Equal(2L, await CountAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE server_id IS NULL OR server_id = 21", ct));
        Assert.Equal(2L, await CountAsync(owner, "SELECT count(*) FROM config.config_collector_run_times WHERE server_id IS NULL OR server_id = 21", ct));
    }

    [Fact]
    public async Task TheViewersRemove_ClearsTheServersCollectorOverridesAndRunTimes_AndKeepsTheFleetRowsAndOtherServers()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, owner) = await OpenAsync(ct);
        await using var scratchHold = scratch;
        await using var ownerHold = owner;
        {
            await SeedScheduleRowsAsync(owner, ct);

            var viewerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
            await using (var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(viewerString))
            {
                await viewer.DeleteMonitoredServerAsync(22, ct);
            }

            Assert.Equal(0L, await CountAsync(owner, "SELECT count(*) FROM config.config_monitored_servers WHERE server_id = 22", ct));
            await AssertOnlyTheRemovedServersScheduleRowsAreGoneAsync(owner, ct);
        }
    }

    [Fact]
    public void EditServer_NullMonthlyCost_ClearsItToZero_AndNegativeOrTextStillRefuses()
    {
        var row = new Edit.ServerEditRow(
            ServerId: 42, Name: "sql-01", Host: "sql-01.example.test", Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
            Auth: "sql", Username: "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
            MultiSubnetFailover: false, MonthlyCostUsd: 120m, ModifiedAt: new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Unspecified));

        var (changes, error) = Edit.ParseEditChanges("{\"monthly_cost_usd\":null}");
        Assert.Null(error);
        Assert.Equal(0m, changes!.MonthlyCostUsd);
        var (plan, planError) = Edit.PlanEdit(row, changes, TestKeyRings.Healthy.Status);
        Assert.Null(planError);
        Assert.Contains("monthly_cost_usd", plan!.ChangedFields);
        Assert.Equal(0m, plan.Sets.Single(s => s.Field == "monthly_cost_usd").Value);

        Assert.NotNull(Edit.ParseEditChanges("{\"monthly_cost_usd\":-1}").Error);
        Assert.NotNull(Edit.ParseEditChanges("{\"monthly_cost_usd\":\"abc\"}").Error);
        Assert.Equal(75.5m, Edit.ParseEditChanges("{\"monthly_cost_usd\":75.5}").Changes!.MonthlyCostUsd);
    }
}
