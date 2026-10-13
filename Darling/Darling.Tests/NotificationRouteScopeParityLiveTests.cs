/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Darling's half of the scoped-route parity pin (#5750). <see cref="NotificationRouteParityFixture"/> (compiled in from
/// Lite.Tests, which holds the other half) builds one fleet: servers, a tag tree three deep, a server on two tags, an empty tag,
/// a removed server and a deleted tag. Here it is stored in a scratch PostgreSQL store, read back through
/// <see cref="StoreConfigProvider.ReadRouteScopeInputsAsync"/> and the service's own view load, and every case must equal the
/// fixture's one expected table, the same table Lite's DuckDB loader is held to. This is the weaker form of a parity pin: no
/// test project opens both stores, so the two products agree through the shared table rather than by comparison.
/// Darling's server id is the registry <c>server_id</c> (1..6 here) and Lite's is a hash of the name, so ids are mapped back to
/// names through the registry table below before they are compared.
/// </summary>
public sealed class NotificationRouteScopeParityLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DarlingsLoader_ExpandsTheSharedFleet_ToTheSharedExpectedTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the scoped-route parity pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
        await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The registry: ids 1..n in the fixture's order. */
            var idByName = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var name in NotificationRouteParityFixture.Servers)
            {
                var id = idByName.Count + 1;
                idByName[name] = id;
                await ExecAsync(connection, string.Create(CultureInfo.InvariantCulture,
                    $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({id}, '{name}', '{name}', true)"), ct);
            }

            var tagIds = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var (name, parent) in NotificationRouteParityFixture.Tags)
            {
                tagIds[name] = await TagAsync(connection, name, parent is null ? null : tagIds[parent], ct);
            }

            foreach (var (server, tag) in NotificationRouteParityFixture.Assignments)
            {
                await ExecAsync(connection, string.Create(CultureInfo.InvariantCulture,
                    $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES ({idByName[server]}, {tagIds[tag]})"), ct);
            }

            /* One stored route per case; then the removed server leaves the registry and the deleted tag leaves the tree,
               while their routes stay (no FK, no cascade: ruling 10). */
            var caseByRouteId = new Dictionary<int, NotificationRouteParityFixture.Case>();
            foreach (var c in NotificationRouteParityFixture.Cases)
            {
                var scopeServer = c.Kind == NotificationRouteParityFixture.Kind.Server ? idByName[c.Target!].ToString(CultureInfo.InvariantCulture) : "NULL";
                var scopeTag = c.Kind == NotificationRouteParityFixture.Kind.Tag ? tagIds[c.Target!].ToString(CultureInfo.InvariantCulture) : "NULL";
                var routeId = Convert.ToInt32(await ScalarAsync(connection,
                    "INSERT INTO config.config_notification_routes (metric_match, teams_url, scope_server_id, scope_tag_id) "
                    + $"VALUES ('High CPU', 'https://teams.parity.test/hook', {scopeServer}, {scopeTag}) RETURNING route_id", ct), CultureInfo.InvariantCulture);
                caseByRouteId[routeId] = c;
            }

            await ExecAsync(connection, string.Create(CultureInfo.InvariantCulture,
                $"DELETE FROM config.config_monitored_servers WHERE server_id = {idByName[NotificationRouteParityFixture.RemovedServer]}"), ct);
            await ExecAsync(connection, string.Create(CultureInfo.InvariantCulture,
                $"DELETE FROM config.server_tags WHERE id = {tagIds[NotificationRouteParityFixture.DeletedTag]}"), ct);

            var nameById = idByName.ToDictionary(p => p.Value, p => p.Key);
            var expected = NotificationRouteParityFixture.ExpectedLines();

            /* (1) The gather method, then the shared expander on the stored scope columns. */
            var inputs = await StoreConfigProvider.ReadRouteScopeInputsAsync(connection, NullLogger.Instance, ct);
            Assert.True(inputs.TagsReadable);
            var stored = new List<NotificationRoute>();
            foreach (var (routeId, c) in caseByRouteId.OrderBy(p => p.Key))
            {
                stored.Add(new NotificationRoute(routeId, "High CPU", "https://teams.parity.test/hook", "", "", "", "", true)
                {
                    ScopeServerId = c.Kind == NotificationRouteParityFixture.Kind.Server ? idByName[c.Target!] : null,
                    ScopeTagId = c.Kind == NotificationRouteParityFixture.Kind.Tag ? tagIds[c.Target!] : null
                });
            }

            var viaGather = StoreConfigProvider.ExpandRoutes(stored, inputs);
            Assert.Equal(expected, viaGather.Select(r => NotificationRouteParityFixture.Describe(
                caseByRouteId[r.RouteId].Name, r.ExpandedServerIds, r.ScopeTagDepth, r.Orphaned, nameById)).ToList());

            /* (2) The service's own view load, which is what the dispatcher gets. */
            var provider = new StoreConfigProvider(dataSource);
            var loaded = (await provider.LoadViewAsync(new DarlingConfig(), ct))!.NotificationRoutes;
            Assert.Equal(expected, loaded.OrderBy(r => r.RouteId).Select(r => NotificationRouteParityFixture.Describe(
                caseByRouteId[r.RouteId].Name, r.ExpandedServerIds, r.ScopeTagDepth, r.Orphaned, nameById)).ToList());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> TagAsync(NpgsqlConnection connection, string name, int? parent, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("INSERT INTO config.server_tags (name, parent_id) VALUES ($1, $2) RETURNING id", connection);
        command.Parameters.AddWithValue(name);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)parent ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
