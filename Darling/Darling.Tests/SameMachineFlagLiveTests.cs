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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// #6351: two registrations whose addresses now reach the same machine. The flag is worked out when a list is read (MCP
/// <c>list_servers</c>, the web admin grid, the Viewer's sidebar) and is never stored; the service logs one warning when a connect
/// stores an identity that another enabled registration already holds, and not again while the value stays the same.
/// </summary>
public sealed class SameMachineFlagLiveTests
{
    private static ServerRuntime Runtime(int id, string name, string? machine) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name + "-host" },
        ConnectionString = "Server=" + name,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = name,
        ServerId = id,
        EngineEdition = 3,
        MachineIdentity = machine,
    };

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Owner)> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live same-machine tests (each mints its own scratch database).");

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

    private static async Task<Dictionary<string, string[]>> McpFlagsAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(owner, ct));
        return doc.RootElement.GetProperty("servers").EnumerateArray().ToDictionary(
            s => s.GetProperty("server_name").GetString()!,
            s => s.GetProperty("same_machine_as").EnumerateArray().Select(e => e.GetString()!).ToArray());
    }

    [Fact]
    public async Task EveryServerListShowsTheOtherEnabledRegistrationsOnTheSameMachine_AndNothingIsStored()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, owner) = await OpenAsync(ct);
        await using var scratchHold = scratch;
        await using var ownerHold = owner;
        {
            /* a and b report one machine; c did too but is disabled; d and e reported none (NULL never matches, not even each other). */
            await DarlingObservability.UpsertServerAsync(owner, Runtime(1, "flag-a", "Machine-X"), null, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "flag-b", "MACHINE-X"), null, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(3, "flag-c", "machine-x"), null, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(4, "flag-d", null), null, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(5, "flag-e", null), null, ct);
            await ExecAsync(owner, "UPDATE collect.servers SET is_enabled = FALSE WHERE server_id = 3", ct);
            await ExecAsync(owner, @"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, engine)
VALUES (1, 'flag-a', 'flag-a-host', 'integrated', TRUE, 'sqlserver'),
       (2, 'flag-b', 'flag-b-host', 'integrated', TRUE, 'sqlserver'),
       (3, 'flag-c', 'flag-c-host', 'integrated', FALSE, 'sqlserver'),
       (4, 'flag-d', 'flag-d-host', 'integrated', TRUE, 'sqlserver'),
       (5, 'flag-e', 'flag-e-host', 'integrated', TRUE, 'sqlserver')", ct);

            /* MCP list_servers: each is in the other's list; the disabled one is in nobody's list (and has no row of its own). */
            var mcp = await McpFlagsAsync(owner, ct);
            Assert.Equal(new[] { "flag-b" }, mcp["flag-a"]);
            Assert.Equal(new[] { "flag-a" }, mcp["flag-b"]);
            Assert.Empty(mcp["flag-d"]);
            Assert.Empty(mcp["flag-e"]);
            Assert.DoesNotContain("flag-c", mcp.Keys);

            /* The web admin grid: the names arrive joined and display-ready; a disabled registration still sees the enabled ones. */
            var admin = JsonDocument.Parse(DarlingAdminServersReader.Render(await DarlingAdminServersReader.ReadAsync(owner, ct), DateTime.UtcNow))
                .RootElement.GetProperty("servers").EnumerateArray()
                .ToDictionary(s => s.GetProperty("display_name").GetString()!, s => s.GetProperty("same_machine_as"));
            Assert.Equal("flag-b", admin["flag-a"].GetString());
            Assert.Equal("flag-a", admin["flag-b"].GetString());
            Assert.Equal("flag-a, flag-b", admin["flag-c"].GetString());
            Assert.Equal(JsonValueKind.Null, admin["flag-d"].ValueKind);

            /* The Viewer's sidebar read. */
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var servers = (await viewer.GetManagedServersAsync(ct)).ToDictionary(s => s.DisplayName);
                Assert.Equal(new[] { "flag-b" }, servers["flag-a"].SameMachineAs);
                Assert.Contains("same machine as flag-b", servers["flag-a"].SidebarDetail, StringComparison.Ordinal);
                Assert.Empty(servers["flag-d"].SameMachineAs);
            }

            /* Nothing is stored: when b's address reaches another machine the flag is gone from every read, with nothing cleared. */
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "flag-b", "machine-y"), null, ct);
            mcp = await McpFlagsAsync(owner, ct);
            Assert.Empty(mcp["flag-a"]);
            Assert.Empty(mcp["flag-b"]);

            /* A store older than the identity column (V212) shows no flag and still lists every server. */
            await ExecAsync(owner, "ALTER TABLE collect.servers DROP COLUMN machine_identity", ct);
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var servers = await viewer.GetManagedServersAsync(ct);
                Assert.Equal(5, servers.Count);
                Assert.All(servers, s => Assert.Empty(s.SameMachineAs));
            }

        }
    }

    [Fact]
    public async Task ConnectLogsOneWarningWhenItChangesTheIdentityOntoAnotherEnabledRegistration_AndNotAgainForTheSameValue()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, owner) = await OpenAsync(ct);
        await using var scratchHold = scratch;
        await using var ownerHold = owner;
        {
            var logger = new CapturingTestLogger();
            int Warnings() => logger.Lines.Count(l => l.Contains("same machine as", StringComparison.Ordinal));

            await DarlingObservability.UpsertServerAsync(owner, Runtime(1, "warn-a", "machine-x"), logger, ct);
            Assert.Equal(0, Warnings());

            /* b first connects on a different machine, then its address moves onto a's machine: one warning, naming both. */
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-y"), logger, ct);
            Assert.Equal(0, Warnings());
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-x"), logger, ct);
            Assert.Equal(1, Warnings());
            var line = logger.Lines.Single(l => l.Contains("same machine as", StringComparison.Ordinal));
            Assert.Contains("'warn-b'", line, StringComparison.Ordinal);
            Assert.Contains("'warn-a'", line, StringComparison.Ordinal);
            Assert.Contains("does not cover the other", line, StringComparison.Ordinal);

            /* Later connects that restate the value stay quiet; so does a connect that read no identity (the stored one is kept). */
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-x"), logger, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", null), logger, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-x"), logger, ct);
            Assert.Equal(1, Warnings());

            /* Moving away and back is a new change onto a held machine: a second warning. */
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-y"), logger, ct);
            Assert.Equal(1, Warnings());
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-x"), logger, ct);
            Assert.Equal(2, Warnings());

            /* A disabled holder is not an overlap. */
            await ExecAsync(owner, "UPDATE collect.servers SET is_enabled = FALSE WHERE server_id = 1", ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-z"), logger, ct);
            await DarlingObservability.UpsertServerAsync(owner, Runtime(3, "warn-c", "machine-z"), logger, ct);
            Assert.Equal(3, Warnings());
            await DarlingObservability.UpsertServerAsync(owner, Runtime(2, "warn-b", "machine-x"), logger, ct);
            Assert.Equal(3, Warnings());

            Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        }
    }
}
