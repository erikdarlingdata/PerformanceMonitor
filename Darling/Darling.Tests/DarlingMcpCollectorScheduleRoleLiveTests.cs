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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// The collector schedule MCP tools (#6227) as the provisioned, NOSUPERUSER <c>mcp</c> login: the grants are the shipped
/// <c>DarlingManagedRoles.BuildProvisioningSql</c> batch run whole, so a grant the tools need but the batch lacks fails here and not on
/// a superuser owner connection, and a grant wider than the tools need shows as a statement the role can run that it must not.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class DarlingMcpCollectorScheduleRoleLiveTests
{
    private const int ServerA = 7501;
    private const int ServerB = 7502;
    private const string Daily = "index_object_stats";
    private const string Delta = "wait_stats";

    /// <summary>The columns the tools set on an override row; every other column of the table stays out of the role's reach.</summary>
    private static readonly string[] GrantedUpdateColumns = ["enabled", "frequency_minutes", "retention_days"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    /// <summary>Provisions the managed roles on a scratch store the way the service does and hands the body a data source that logs
    /// in as the real <c>mcp</c> role, plus the owner connection for seeding and for reading what landed.</summary>
    private static async Task WithMcpStoreAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the collector schedule role pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else.
               A leftover set (an earlier run that was killed before its cleanup) fails here and names the roles, so it is seen and removed
               by hand instead of every later run skipping past it. */
            var existing = (string)(await ScalarAsync(owner, "SELECT COALESCE(string_agg(rolname, ', ' ORDER BY rolname), '') FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", ct))!;
            Assert.True(
                existing.Length == 0,
                $"The cluster already has the login role(s) {existing}. They are cluster-wide and this test creates and drops all three of admin, viewer and mcp, so it will not adopt them: drop them from the rig and run it again.");
        }

        var bodySucceeded = false;
        try
        {
            await using var ownerConnection = new NpgsqlConnection(scratch.ConnectionString);
            await ownerConnection.OpenAsync(ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(ownerConnection.ConnectionString).Username ?? "darling";
            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
            await ExecAsync(ownerConnection, batch, ct);
            await ExecAsync(ownerConnection,
                $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES " +
                $"({ServerA}, 'role-alpha', 'role-alpha', TRUE, 16, now(), now()), ({ServerB}, 'role-beta', 'role-beta', TRUE, 16, now(), now())", ct);

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);

            await body(mcp, ownerConnection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<(bool? Enabled, int? Frequency, int? Retention)?> RowAsync(NpgsqlConnection owner, int? serverId, string collector, CancellationToken ct)
    {
        var server = serverId is int id ? id.ToString(CultureInfo.InvariantCulture) : "NULL";
        var where = serverId is null ? "server_id IS NULL" : $"server_id = {server}";
        await using var command = new NpgsqlCommand(
            $"SELECT enabled, frequency_minutes, retention_days FROM config.config_collector_schedules WHERE {where} AND collector_name = '{collector}'", owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return (reader.IsDBNull(0) ? null : reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetInt32(2));
    }

    [Fact]
    public async Task TheProvisionedMcpLogin_RunsEveryScheduleWriteTool_AndEachWriteLandsInTheStore()
    {
        await WithMcpStoreAsync(async (mcp, owner, ct) =>
        {
            /* The reload beacon row the service seeds at first start; a bare migrated store has none for the statement trigger to bump. */
            await ExecAsync(owner, "INSERT INTO config.config_service DEFAULT VALUES ON CONFLICT DO NOTHING", ct);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture));

            /* disable_collector, fleet-wide: a row with enabled false and nothing else. */
            var disabled = Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(mcp, Delta, null, cancellationToken: ct));
            Assert.Equal("changed", (string?)disabled["status"]);
            Assert.Equal((false, null, null), await RowAsync(owner, null, Delta, ct));

            /* enable_collector, for one server: its own row wins over the fleet row. */
            var enabled = Parse(await DarlingMcpCollectorScheduleTools.EnableCollector(mcp, Delta, "role-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)enabled["status"]);
            Assert.Equal((true, null, null), await RowAsync(owner, ServerA, Delta, ct));

            /* enable_collector with inherit=true: the UPDATE writes NULL, which the V206 column allows and the role may write. */
            var inherited = Parse(await DarlingMcpCollectorScheduleTools.EnableCollector(mcp, Delta, "role-alpha", inherit: true, cancellationToken: ct));
            Assert.Equal("changed", (string?)inherited["status"]);
            Assert.Equal((null, null, null), await RowAsync(owner, ServerA, Delta, ct));

            /* set_collector_frequency and set_collector_retention on a server with no row create one that stores no enabled state. */
            var frequency = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, 10, false, "role-beta", cancellationToken: ct));
            Assert.Equal("changed", (string?)frequency["status"]);
            Assert.Equal((null, 10, null), await RowAsync(owner, ServerB, Delta, ct));

            var retention = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(mcp, Delta, 45, false, "role-beta", cancellationToken: ct));
            Assert.Equal("changed", (string?)retention["status"]);
            Assert.Equal((null, 10, 45), await RowAsync(owner, ServerB, Delta, ct));

            /* clear=true goes through the same UPDATE grant. */
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, null, true, "role-beta", cancellationToken: ct);
            Assert.Equal((null, null, 45), await RowAsync(owner, ServerB, Delta, ct));

            /* set_collector_run_at: an insert, an update and a delete on the run-time table. */
            var set = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(mcp, Daily, "03:30", "role-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)set["status"]);
            Assert.Equal(210L, Convert.ToInt64(await ScalarAsync(owner,
                $"SELECT run_at_minute FROM config.config_collector_run_times WHERE server_id = {ServerA} AND collector_name = '{Daily}'", ct), CultureInfo.InvariantCulture));
            await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(mcp, Daily, "04:15", "role-alpha", cancellationToken: ct);
            Assert.Equal(255L, Convert.ToInt64(await ScalarAsync(owner,
                $"SELECT run_at_minute FROM config.config_collector_run_times WHERE server_id = {ServerA} AND collector_name = '{Daily}'", ct), CultureInfo.InvariantCulture));
            await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(mcp, Daily, "default", "role-alpha", cancellationToken: ct);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_run_times", ct), CultureInfo.InvariantCulture));

            /* The reload beacon every write bumps (a statement trigger that runs as the caller) was allowed: the version moved, at least once per write. */
            Assert.True(Convert.ToInt64(await ScalarAsync(owner, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture) >= 9);

            /* The read answers through the role too. */
            var read = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(mcp, Delta, null, ct));
            Assert.Null(read["error"]);
        });
    }

    [Fact]
    public async Task TheProvisionedMcpLogin_CannotInsertTheDatabaseScope_AndCannotUpdateAnyColumnItWasNotGranted()
    {
        await WithMcpStoreAsync(async (mcp, owner, ct) =>
        {
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, 10, false, null, cancellationToken: ct);
            await DarlingMcpCollectorScheduleTools.DisableCollector(mcp, Daily, "role-alpha", cancellationToken: ct);
            await using var login = await mcp.OpenConnectionAsync(ct);

            /* #6454: the role holds DELETE on the override table now, which remove_server uses to clear a removed server's rows. No schedule
               tool removes a row. */
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct), CultureInfo.InvariantCulture));

            /* The INSERT grant is the five columns the tools set (a new row is how a frequency or a retention starts): the database scope
               is not among them, so an INSERT that names it is refused, whatever else it carries. */
            var insert = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(login, "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, databases) VALUES (NULL, 'cpu_utilization', TRUE, ARRAY['x'])", ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, insert.SqlState);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE collector_name = 'cpu_utilization'", ct), CultureInfo.InvariantCulture));

            /* An UPDATE of every column the grant does not name is refused: read from the catalog, so a column added later is held to it too. */
            var columns = new List<string>();
            await using (var list = new NpgsqlCommand(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' ORDER BY ordinal_position", owner))
            await using (var reader = await list.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    columns.Add(reader.GetString(0));
                }
            }

            var refused = columns.Except(GrantedUpdateColumns).ToList();
            Assert.Contains("databases", refused);
            Assert.Contains("server_id", refused);
            Assert.Contains("collector_name", refused);
            foreach (var column in refused)
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
                    await ExecAsync(login, $"UPDATE config.config_collector_schedules SET {column} = {column}", ct));
                Assert.True(ex.SqlState == PostgresErrorCodes.InsufficientPrivilege, $"UPDATE of {column} gave {ex.SqlState}: {ex.MessageText}");
            }

            /* The three granted columns still update. */
            await ExecAsync(login, "UPDATE config.config_collector_schedules SET enabled = enabled, frequency_minutes = frequency_minutes, retention_days = retention_days", ct);

            /* The run-time table: the role may not touch its key columns either, and a row of it cannot be written past the one value. */
            await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(mcp, Daily, "03:30", "role-alpha", cancellationToken: ct);
            var runTimeKey = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(login, "UPDATE config.config_collector_run_times SET server_id = server_id", ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, runTimeKey.SqlState);
        });
    }
}
