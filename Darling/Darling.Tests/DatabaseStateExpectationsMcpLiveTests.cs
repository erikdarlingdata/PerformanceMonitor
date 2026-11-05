/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// #6397: <c>get_database_state_expectations</c> and <c>set_database_state_expected</c> as the provisioned, NOSUPERUSER <c>mcp</c>
/// role. The grants are the shipped <c>DarlingManagedRoles.BuildProvisioningSql</c> batch run whole, so a missing grant fails here
/// and not on an owner connection. Proves a set, the read back, an undo that restores the earlier value, a clear, the refusals, and
/// that the role holds exactly the writes the tools run and no more.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class DatabaseStateExpectationsMcpLiveTests
{
    private const int ServerId = 7397;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonNode Parse(string json) => JsonNode.Parse(json)!;

    [Fact]
    public async Task TheProvisionedMcpRole_SetsReadsUndoesAndClearsAnExpectation_AndHoldsNothingWider()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the database state expectation tool test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else. */
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using (var provision = new NpgsqlCommand(batch, owner))
                {
                    await provision.ExecuteNonQueryAsync(ct);
                }

                await using var seed = new NpgsqlCommand(
                    $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({ServerId}, 'srv{ServerId}', 'srv{ServerId}', TRUE, 16, now(), now()); " +
                    "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES " +
                    $"(1, now() AT TIME ZONE 'UTC', {ServerId}, 'srv{ServerId}', 'Sales', 5, 'ONLINE', false), " +
                    $"(2, now() AT TIME ZONE 'UTC', {ServerId}, 'srv{ServerId}', 'Archive', 6, 'OFFLINE', false)", owner);
                await seed.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            var server = $"srv{ServerId}";

            /* Nothing is stored yet: both databases are pending, and the read lists what the write accepts. */
            var first = Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, cancellationToken: ct));
            var databases = first["databases"]!.AsArray();
            Assert.Equal(new[] { "Archive", "Sales" }, databases.Select(d => (string)d!["database_name"]!).ToArray());
            Assert.All(databases, d => Assert.Equal("pending", (string)d!["status"]!));
            Assert.Contains("IGNORE", first["allowed_states"]!.AsArray().Select(v => (string)v!));

            /* #6545: a latest-snapshot read says when the snapshot was taken. The seed's collection_time is "now" in UTC, and the stamp
               is a UTC instant (trailing Z), not the host's local reading of a timestamp without a zone. */
            var capturedAt = (string)first["captured_at"]!;
            Assert.EndsWith("Z", capturedAt, StringComparison.Ordinal);
            Assert.InRange(
                Math.Abs((DateTimeOffset.UtcNow - DateTimeOffset.Parse(capturedAt, System.Globalization.CultureInfo.InvariantCulture)).TotalMinutes), 0, 10);

            /* Set: an override, with no previous and an undo that clears. */
            var set = Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Sales", "online", cancellationToken: ct));
            Assert.Equal("set", (string)set["status"]!);
            Assert.Null(set["previous"]);
            Assert.Equal("CLEAR", (string)set["undo"]!["arguments"]!["expected_state"]!);
            var sales = Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Sales", ct))["databases"]![0]!;
            Assert.Equal("ONLINE", (string)sales["expected_state"]!);
            Assert.True((bool)sales["is_user_override"]!);
            Assert.Equal("as_expected", (string)sales["status"]!);

            /* Change it: previous reports the old value, and the undo it gives puts the old value back. */
            var changed = Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Sales", "OFFLINE", cancellationToken: ct));
            Assert.Equal("set", (string)changed["status"]!);
            Assert.Equal("ONLINE", (string)changed["previous"]!["expected_state"]!);
            Assert.True((bool)changed["previous"]!["is_user_override"]!);
            Assert.Equal("deviating", (string)Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Sales", ct))["databases"]![0]!["status"]!);
            var undo = changed["undo"]!["arguments"]!;
            var restored = Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(
                mcp, (string)undo["server_name"]!, (string)undo["database_name"]!, (string)undo["expected_state"]!, cancellationToken: ct));
            Assert.Equal("set", (string)restored["status"]!);
            Assert.Equal("ONLINE", (string)Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Sales", ct))["databases"]![0]!["expected_state"]!);

            /* The same override again changes nothing. */
            Assert.Equal("unchanged", (string)Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Sales", "ONLINE", cancellationToken: ct))["status"]!);

            /* IGNORE is stored as the (ignore) sentinel the alert reads, and its undo names IGNORE again. */
            var ignored = Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Archive", "IGNORE", cancellationToken: ct));
            Assert.Equal("(ignore)", (string)ignored["expected_state"]!);
            Assert.Equal("ignored", (string)Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Archive", ct))["databases"]![0]!["status"]!);

            /* Clear: the row is deleted, previous says what it was, and its undo sets it back; a second clear changes nothing. */
            var cleared = Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Sales", "CLEAR", cancellationToken: ct));
            Assert.Equal("cleared", (string)cleared["status"]!);
            Assert.Equal("ONLINE", (string)cleared["previous"]!["expected_state"]!);
            Assert.Equal("ONLINE", (string)cleared["undo"]!["arguments"]!["expected_state"]!);
            Assert.Equal("pending", (string)Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Sales", ct))["databases"]![0]!["status"]!);
            Assert.Equal("unchanged", (string)Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Sales", "CLEAR", cancellationToken: ct))["status"]!);

            /* Refusals write nothing: an unknown state names the allowed list, an unknown database and an unknown server are refused. */
            var badState = Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "Sales", "HEALTHY", cancellationToken: ct));
            Assert.Equal("invalid", (string)badState["status"]!);
            Assert.Contains("RECOVERY_PENDING", (string)badState["message"]!, StringComparison.Ordinal);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, server, "NoSuchDb", "ONLINE", cancellationToken: ct))["status"]!);
            Assert.NotEqual("set", (string)Parse(await DarlingMcpDatabaseStateTools.SetDatabaseStateExpected(mcp, "no-such-server-6397", "Sales", "ONLINE", cancellationToken: ct))["status"]!);
            Assert.Equal("pending", (string)Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Sales", ct))["databases"]![0]!["status"]!);

            /* The Viewer's Reset-to-current runs the same store: the ignore on Archive becomes the learned OFFLINE baseline, not an override. */
            await using (var reset = DatabaseStateExpectedStore.CreateResetToCurrentCommand(mcp, ServerId, "Archive", 15))
            {
                await reset.ExecuteNonQueryAsync(ct);
            }

            var archive = Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(mcp, server, "Archive", ct))["databases"]![0]!;
            Assert.Equal("OFFLINE", (string)archive["expected_state"]!);
            Assert.False((bool)archive["is_user_override"]!);

            /* The role holds exactly what the tools run on this table, and no statement outside it. */
            await using var check = await mcp.OpenConnectionAsync(ct);
            foreach (var (privilege, expected) in new[] { ("SELECT", true), ("INSERT", true), ("UPDATE", true), ("DELETE", true), ("TRUNCATE", false), ("REFERENCES", false), ("TRIGGER", false) })
            {
                await using var priv = new NpgsqlCommand("SELECT has_table_privilege('config.database_state_expected', $1)", check);
                priv.Parameters.AddWithValue(privilege);
                Assert.Equal(expected, (bool)(await priv.ExecuteScalarAsync(ct))!);
            }

            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var outside = new NpgsqlCommand("INSERT INTO config.config_command (command_type) VALUES ('x')", check);
                await outside.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, refused.SqlState);

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
}
