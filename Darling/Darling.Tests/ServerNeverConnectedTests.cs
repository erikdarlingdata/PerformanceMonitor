/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7590 part F: a server that has NEVER connected (a wrong host or a bad password on the day it is added) used to have no registry row, so
/// nothing in the store said it was down and every read answered "no servers are registered yet". The failed first connect now writes the row
/// from the configuration, with the connect columns, and the reads that assumed a row means "connected once" read it as Offline with its reason.
/// </summary>
public sealed class ServerNeverConnectedTests
{
    private const int ServerId = 91;

    [Fact]
    public async Task A_failed_first_connect_stores_the_server_and_the_reads_say_Offline_until_a_good_connect_clears_it()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live connect-state test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* The desired-state row the service seeds from darling.json at start, before any connect: the reads join it. */
            await using var seed = new NpgsqlCommand(
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (91, 'never-up', 'never-up.example', TRUE)", connection);
            await seed.ExecuteNonQueryAsync(ct);
        }

        /* The store holds no row for the server before the first connect attempt. */
        await using (var none = postgres.CreateCommand("SELECT count(*) FROM servers"))
        {
            Assert.Equal(0L, await none.ExecuteScalarAsync(ct));
        }

        /* The real connect call site, with a failed attempt, for a server that has never connected (Runtime is null, no row anywhere). */
        var worker = new DarlingWorker(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = postgres,
            SelfAlertsForTests = new DarlingSelfAlertEvaluator(
                new DarlingSelfAlertTests.FakeSettings(), new DarlingSelfAlertTests.RecordingDeliverer(),
                new DarlingSelfAlertTests.FakeHistoryStore(), _ => false),
        };
        var config = new MonitoredServer { Name = "never-up", Host = "never-up.example", StoredServerId = ServerId, Engine = "postgres", MonthlyCostUsd = 250m };
        var state = new DarlingWorker.ServerLoopState { Config = config };
        var tryConnect = typeof(DarlingWorker).GetMethod("TryConnectAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task)tryConnect.Invoke(worker, [state, null, new DarlingConfig(), new ConnectAttempt(config, null, new InvalidOperationException("Login failed for user 'pm_monitor'.")), ct])!;

        /* The row: the configuration's identity and the three connect columns; the facts only a connect learns stay NULL. */
        await using (var row = postgres.CreateCommand(
            @"SELECT server_name, display_name, is_enabled, engine_kind, monthly_cost_usd, sql_engine_edition, sql_major_version, postgres_major_version, machine_identity,
                     connect_down_since IS NOT NULL, last_connect_error, next_connect_attempt IS NOT NULL
              FROM servers WHERE server_id = $1"))
        {
            row.Parameters.AddWithValue(ServerId);
            await using var reader = await row.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "the failed first connect wrote the registry row");
            Assert.Equal(config.StorageName, reader.GetString(0));
            Assert.Equal("never-up", reader.GetString(1));
            Assert.True(reader.GetBoolean(2));
            Assert.Equal("postgres", reader.GetString(3));
            Assert.Equal(250m, reader.GetDecimal(4));
            Assert.True(reader.IsDBNull(5) && reader.IsDBNull(6) && reader.IsDBNull(7) && reader.IsDBNull(8), "version, edition and machine identity are not claimed");
            Assert.True(reader.GetBoolean(9));
            Assert.Contains("Login failed", reader.GetString(10));
            Assert.True(reader.GetBoolean(11));
        }

        /* list_servers: the server is listed, Offline, with the reason. */
        using (var list = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct)))
        {
            var server = list.RootElement.GetProperty("servers")[0];
            Assert.Equal("never-up", server.GetProperty("display_name").GetString());
            Assert.Equal("Offline", server.GetProperty("status").GetString());
            Assert.Contains("Login failed", server.GetProperty("reason").GetString());
        }

        /* get_collection_health: no collector has ever run, and the answer is still the waiting-on-connection sentence. */
        using (var health = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealthUncut(postgres, "never-up", ct)))
        {
            var root = health.RootElement;
            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            Assert.Contains("Collection is waiting on the connection", root.GetProperty("message").GetString());
            Assert.Contains("Login failed", root.GetProperty("message").GetString());
        }

        /* The fleet card: Offline with the unreachable reason, and the NULL facts do not throw. */
        var now = DateTime.UtcNow;
        var fleet = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
        var card = Assert.Single(fleet.Cards);
        Assert.Equal("Offline", card.Status);
        Assert.Contains("Login failed", DarlingFleetReader.BuildReason(card));
        Assert.Equal(1, fleet.OfflineCount);

        /* The Manage Servers grid reads it too. */
        var admin = Assert.Single(await DarlingAdminServersReader.ReadAsync(postgres, ct));
        Assert.NotNull(admin.ConnectDownSince);

        /* A good connect fills the facts and clears the connect columns, on the same row. */
        await using (var upsert = postgres.CreateCommand(DarlingObservability.UpsertServerSql))
        {
            upsert.Parameters.AddWithValue(ServerId);
            upsert.Parameters.AddWithValue(config.StorageName);
            upsert.Parameters.AddWithValue("never-up");
            upsert.Parameters.AddWithValue(3);
            upsert.Parameters.AddWithValue(16);
            upsert.Parameters.AddWithValue("sqlserver");
            upsert.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            upsert.Parameters.AddWithValue(250m);
            upsert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            upsert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
            upsert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
            await upsert.ExecuteNonQueryAsync(ct);
        }

        await using (var after = postgres.CreateCommand(
            "SELECT count(*) FROM servers WHERE server_id = $1 AND sql_major_version = 16 AND connect_down_since IS NULL AND last_connect_error IS NULL AND next_connect_attempt IS NULL"))
        {
            after.Parameters.AddWithValue(ServerId);
            Assert.Equal(1L, await after.ExecuteScalarAsync(ct));
        }

        using (var list = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct)))
        {
            Assert.False(list.RootElement.GetProperty("servers")[0].TryGetProperty("reason", out var reason) && reason.ValueKind == JsonValueKind.String);
        }
    }
}
