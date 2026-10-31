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
    private static readonly string[] GrantedUpdateColumns = ["enabled", "frequency_minutes", "retention_days", "databases"];

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
                await using var drop = new NpgsqlCommand($"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
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

            /* Retention is fleet-wide: the fleet row, written with the same grant. A per-server value is refused. */
            var retention = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(mcp, Delta, 45, false, null, cancellationToken: ct));
            Assert.Equal("changed", (string?)retention["status"]);
            Assert.Equal(45, (await RowAsync(owner, null, Delta, ct))?.Retention);
            await ExecAsync(owner, $"UPDATE config.config_collector_schedules SET retention_days = 45 WHERE server_id = {ServerB} AND collector_name = '{Delta}'", ct);

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
    public async Task TheProvisionedMcpLogin_CanSetTheDatabaseScope_AndCannotUpdateTheKeyColumns()
    {
        await WithMcpStoreAsync(async (mcp, owner, ct) =>
        {
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, 10, false, null, cancellationToken: ct);
            await DarlingMcpCollectorScheduleTools.DisableCollector(mcp, Daily, "role-alpha", cancellationToken: ct);
            await using var login = await mcp.OpenConnectionAsync(ct);

            /* #6454: the role holds DELETE on the override table now, which remove_server uses to clear a removed server's rows. No schedule
               tool removes a row. */
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct), CultureInfo.InvariantCulture));

            /* The INSERT grant is the six columns the tools set (#6396 added the database scope, which set_collector_databases starts a row with):
               an INSERT that names it lands. */
            await ExecAsync(login, "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, databases) VALUES (NULL, 'cpu_utilization', NULL, ARRAY['x'])", ct);
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE collector_name = 'cpu_utilization' AND databases = ARRAY['x']", ct), CultureInfo.InvariantCulture));

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

            /* What the store grants, asked of the catalog for the mcp login (not the test's own roster): exactly the four value columns. */
            var granted = new List<string>();
            foreach (var column in columns)
            {
                if ((bool)(await ScalarAsync(owner,
                    $"SELECT has_column_privilege('mcp', 'config.config_collector_schedules', '{column}', 'UPDATE')", ct))!)
                {
                    granted.Add(column);
                }
            }

            Assert.Equal(GrantedUpdateColumns.OrderBy(c => c, StringComparer.Ordinal), granted.OrderBy(c => c, StringComparer.Ordinal));
            var refused = columns.Except(granted).ToList();
            Assert.Contains("server_id", refused);
            Assert.Contains("collector_name", refused);
            foreach (var column in refused)
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
                    await ExecAsync(login, $"UPDATE config.config_collector_schedules SET {column} = {column}", ct));
                Assert.True(ex.SqlState == PostgresErrorCodes.InsufficientPrivilege, $"UPDATE of {column} gave {ex.SqlState}: {ex.MessageText}");
            }

            /* The four granted columns still update. */
            await ExecAsync(login, "UPDATE config.config_collector_schedules SET enabled = enabled, frequency_minutes = frequency_minutes, retention_days = retention_days, databases = databases", ct);

            /* The run-time table: the role may not touch its key columns either, and a row of it cannot be written past the one value. */
            await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(mcp, Daily, "03:30", "role-alpha", cancellationToken: ct);
            var runTimeKey = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(login, "UPDATE config.config_collector_run_times SET server_id = server_id", ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, runTimeKey.SqlState);
        });
    }

    /// <summary>#6396: <c>reset_collector_schedules</c> runs its two deletes on the MCP host's own data source, which is not given the
    /// <c>config</c> search_path the role tests above pin. The database default is taken off and the login's string carries none, so the
    /// statements must name their schema themselves: unqualified they fail with 42P01.</summary>
    [Fact]
    public async Task ResetCollectorSchedules_OnADataSourceWithNoConfigSearchPath_StillResetsTheScope()
    {
        await WithMcpStoreAsync(async (mcp, owner, ct) =>
        {
            await DarlingMcpCollectorScheduleTools.DisableCollector(mcp, Delta, null, cancellationToken: ct);
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, 10, false, "role-alpha", cancellationToken: ct);

            /* The migration set a database-level search_path; the host's source cannot count on it (ALTER DATABASE needs the owner). */
            await ExecAsync(owner, $"ALTER DATABASE \"{owner.Database}\" RESET search_path", ct);
            var bare = new NpgsqlConnectionStringBuilder(owner.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                Pooling = false,
            };
            await using var noPath = NpgsqlDataSource.Create(bare.ConnectionString);

            await using (var probe = await noPath.OpenConnectionAsync(ct))
            {
                var unqualified = await Assert.ThrowsAsync<PostgresException>(async () =>
                    await ExecAsync(probe, "SELECT 1 FROM config_collector_schedules", ct));
                Assert.Equal(PostgresErrorCodes.UndefinedTable, unqualified.SqlState);
            }

            var fleet = Parse(await DarlingMcpCollectorScheduleTools.ResetCollectorSchedules(noPath, null, cancellationToken: ct));
            Assert.Null(fleet["error"]);
            Assert.Equal("changed", (string?)fleet["status"]);
            Assert.Null(await RowAsync(owner, null, Delta, ct));
            Assert.NotNull(await RowAsync(owner, ServerA, Delta, ct));

            /* The server scope goes through the store call: naming a server resolves it against the unqualified registry read, which this source cannot do either. */
            var (scheduleRows, _) = await CollectorScheduleSql.ResetScopeAsync(noPath, ServerA, 30, ct);
            Assert.Equal(1, scheduleRows);
            Assert.Null(await RowAsync(owner, ServerA, Delta, ct));
        });
    }

    private static async Task<string> DatabasesOfAsync(NpgsqlConnection owner, string collector, int? serverId, CancellationToken ct) =>
        (string)(await ScalarAsync(owner,
            "SELECT COALESCE((SELECT CASE WHEN databases IS NULL THEN 'NULL' ELSE '{' || array_to_string(databases, ',') || '}' END " +
            $"FROM config.config_collector_schedules WHERE collector_name = '{collector}' AND server_id " + (serverId is null ? "IS NULL" : $"= {serverId}") + "), 'no row')", ct))!;

    [Fact]
    public async Task SetCollectorDatabases_AsTheMcpRole_WritesTheNormalizedScopeForTheNamedCollectorAndScopeOnly_AndRefusesWhatTheViewerRefuses()
    {
        await WithMcpStoreAsync(async (mcp, owner, ct) =>
        {
            /* The Viewer's parse: split on comma, trim, drop blanks. The fleet row is created with enabled inherited. */
            var fleet = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, " Sales ,, Orders ,", false, null, cancellationToken: ct));
            Assert.Equal("changed", (string?)fleet["status"]);
            Assert.Equal("{Sales,Orders}", await DatabasesOfAsync(owner, Delta, null, ct));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE enabled IS NULL AND collector_name = 'wait_stats'", ct), CultureInfo.InvariantCulture));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct), CultureInfo.InvariantCulture));

            /* One server: its own row only, the fleet row is not touched. An empty list on a server is the explicit no-scope array. */
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, 10, false, "role-alpha", cancellationToken: ct);
            var server = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, "", false, "role-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)server["status"]);
            Assert.Equal("{}", await DatabasesOfAsync(owner, Delta, ServerA, ct));
            Assert.Equal("{Sales,Orders}", await DatabasesOfAsync(owner, Delta, null, ct));
            Assert.Equal("no row", await DatabasesOfAsync(owner, Delta, ServerB, ct));
            Assert.Equal(10L, Convert.ToInt64(await ScalarAsync(owner, $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id = {ServerA}", ct), CultureInfo.InvariantCulture));

            /* The answer's undo puts the previous scope back. */
            var set = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, "Only", false, "role-alpha", cancellationToken: ct));
            Assert.Equal("{Only}", await DatabasesOfAsync(owner, Delta, ServerA, ct));
            Assert.Equal("", (string?)set["undo"]!["arguments"]!["databases"]);
            await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, (string?)set["undo"]!["arguments"]!["databases"], false, "role-alpha", cancellationToken: ct);
            Assert.Equal("{}", await DatabasesOfAsync(owner, Delta, ServerA, ct));

            /* Clear: NULL at that level, the rest of the row stays. */
            await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, null, true, "role-alpha", cancellationToken: ct);
            Assert.Equal("NULL", await DatabasesOfAsync(owner, Delta, ServerA, ct));
            Assert.Equal(10L, Convert.ToInt64(await ScalarAsync(owner, $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id = {ServerA}", ct), CultureInfo.InvariantCulture));

            /* At the fleet an empty list clears, as the Viewer's blank fleet box writes NULL. */
            await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, "", false, null, cancellationToken: ct);
            Assert.Equal("NULL", await DatabasesOfAsync(owner, Delta, null, ct));

            /* Refused, and nothing is written: an unknown collector, neither or both of databases and clear, an unknown server. */
            var rowsBefore = await ScalarAsync(owner, "SELECT count(*) || '/' || COALESCE(sum(cardinality(databases)), 0) FROM config.config_collector_schedules", ct);
            foreach (var answer in new[]
            {
                await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, "not_a_collector", "x", false, null, cancellationToken: ct),
                await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, null, false, null, cancellationToken: ct),
                await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, "x", true, null, cancellationToken: ct),
                await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Delta, "x", false, "no-such-server", cancellationToken: ct),
            })
            {
                Assert.Equal("refused", (string?)Parse(answer)["status"]);
            }

            Assert.Equal(rowsBefore, await ScalarAsync(owner, "SELECT count(*) || '/' || COALESCE(sum(cardinality(databases)), 0) FROM config.config_collector_schedules", ct));
        });
    }

    [Fact]
    public async Task ResetCollectorSchedules_AsTheMcpRole_RemovesOneScopesRowsAndRunTimes_AndLeavesTheOtherScopesAlone()
    {
        await WithMcpStoreAsync(async (mcp, owner, ct) =>
        {
            foreach (var server in new string?[] { null, "role-alpha", "role-beta" })
            {
                await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(mcp, Delta, 7, false, server, cancellationToken: ct);
                await DarlingMcpCollectorScheduleTools.DisableCollector(mcp, Daily, server, cancellationToken: ct);
                await DarlingMcpCollectorScheduleTools.SetCollectorDatabases(mcp, Daily, "Sales", false, server, cancellationToken: ct);
                await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(mcp, Daily, "03:30", server, cancellationToken: ct);
            }

            /* One server: that server's two rows and its run time, nothing else. */
            var one = Parse(await DarlingMcpCollectorScheduleTools.ResetCollectorSchedules(mcp, "role-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)one["status"]);
            Assert.Equal(2, (int?)one["removed"]!["schedule_rows"]);
            Assert.Equal(1, (int?)one["removed"]!["run_times"]);
            Assert.Equal(2, ((JsonArray)one["removed_overrides"]!).Count);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_schedules WHERE server_id = {ServerA}", ct), CultureInfo.InvariantCulture));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_run_times WHERE server_id = {ServerA}", ct), CultureInfo.InvariantCulture));
            Assert.Equal(4L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct), CultureInfo.InvariantCulture));
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_run_times", ct), CultureInfo.InvariantCulture));

            /* A second reset finds nothing and says so. */
            Assert.Equal("unchanged", (string?)Parse(await DarlingMcpCollectorScheduleTools.ResetCollectorSchedules(mcp, "role-alpha", cancellationToken: ct))["status"]);

            /* The fleet scope: the fleet rows and the fleet run time go, as the Viewer's Reset to Defaults deletes them; the servers' rows stay. */
            var fleet = Parse(await DarlingMcpCollectorScheduleTools.ResetCollectorSchedules(mcp, null, cancellationToken: ct));
            Assert.Equal(2, (int?)fleet["removed"]!["schedule_rows"]);
            Assert.Equal(1, (int?)fleet["removed"]!["run_times"]);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE server_id IS NULL", ct), CultureInfo.InvariantCulture));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_run_times WHERE server_id IS NULL", ct), CultureInfo.InvariantCulture));
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_schedules WHERE server_id = {ServerB}", ct), CultureInfo.InvariantCulture));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_run_times WHERE server_id = {ServerB}", ct), CultureInfo.InvariantCulture));
        });
    }
}
