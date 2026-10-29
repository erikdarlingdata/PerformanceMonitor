/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// #6906: the Viewer's tab server pickers (Database Protection, Agent Alerts, Identity Capacity, Custom View run) read the
/// observed registry, which keeps every server ever registered. A server removed and re-added showed twice, the removed entry
/// first. The pickers now read the managed set the sidebar shows. WPF cannot run here, so this tests the read they share.
/// </summary>
public sealed class ViewerPickerServersLiveTests
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

    [Fact]
    public async Task ThePickerListsARemovedAndReAddedServerOnce_WhileTheObservedRegistryStillHoldsBoth()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live picker test (it mints its own scratch database).");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await using var owner = NpgsqlDataSource.Create(ownerString);

        /* 10 is the removed registration (observed row, no config row), 20 is the same name registered again, 30 is an
           ordinary server. */
        await DarlingObservability.UpsertServerAsync(owner, Runtime(10, "picker-a"), null, ct);
        await DarlingObservability.UpsertServerAsync(owner, Runtime(20, "picker-a"), null, ct);
        await DarlingObservability.UpsertServerAsync(owner, Runtime(30, "picker-b"), null, ct);
        /* The managed read falls back to the observed one until the service has seeded the store, so seed it as the service does. */
        await using (var command = owner.CreateCommand(@"
INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING;
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, engine)
VALUES (20, 'picker-a', 'picker-a-host', 'integrated', TRUE, 'sqlserver'),
       (30, 'picker-b', 'picker-b-host', 'integrated', TRUE, 'sqlserver')"))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var observed = await viewer.GetServersAsync(ct);
        Assert.Equal(3, observed.Count);

        var picker = await viewer.GetPickerServersAsync(ct);
        Assert.Equal(new[] { 20, 30 }, picker.Select(s => s.ServerId).OrderBy(i => i).ToArray());
        Assert.DoesNotContain(picker, s => s.ServerId == 10);
        Assert.Equal(picker.Count, picker.Select(s => s.DisplayName).Distinct().Count());

        /* The engine fields the Database Protection read hands to its engine-gap note survive the managed read. */
        Assert.All(picker, s => Assert.Equal(3, s.EngineEdition));
    }
}
