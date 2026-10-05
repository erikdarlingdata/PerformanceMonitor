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
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and a role with a per-run
   name, and touches nothing on the shared one, so it is not serialized against the live-postgres collection. */

/// <summary>
/// The server-onboarding write as a non-superuser holding exactly the grants the managed <c>viewer</c> role gets
/// (#4843): the web dashboard's <c>POST /api/servers</c> runs the <c>add_servers</c> core on the viewer-role pool, and
/// every other live test connects as the owner, which can write anything, so a missing grant would pass them all
/// and fail only in a real deployment with a 42501. The statements are TAKEN FROM
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/>'s output with the role name swapped, so removing the
/// grant from the product's SQL makes the add test fail.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class ServerAddViewerRoleLiveTests : IDisposable
{
    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    /* Configuration loaded, owning nothing: an unpopulated set refuses every env:/file: reference (R3-3), and the
       entries here use one. */
    public ServerAddViewerRoleLiveTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private const string ServerGrant = "GRANT INSERT ON config.config_monitored_servers TO viewer;";

    private static readonly DarlingMcpServerAdminTools.ServerProbe Reachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(
            Success: true, MajorVersion: 15, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null));

    /// <summary>Every GRANT / REVOKE in the managed batch that names <c>viewer</c> (alone or in a list), in order,
    /// retargeted at <paramref name="roleName"/>. <paramref name="skip"/> drops statements by exact text.</summary>
    private static List<string> ViewerStatements(string roleName, string? skip)
    {
        var provisioning = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var statements = new List<string>();

        /* Drop the comment lines BEFORE splitting: the provisioning prose contains semicolons. */
        var uncommented = string.Join('\n', provisioning.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));
        foreach (var raw in uncommented.Split(';'))
        {
            var statement = raw.Trim();
            var isGrant = statement.StartsWith("GRANT ", StringComparison.Ordinal);
            if (!isGrant && !statement.StartsWith("REVOKE ", StringComparison.Ordinal))
            {
                continue;
            }

            var marker = isGrant ? " TO " : " FROM ";
            var at = statement.LastIndexOf(marker, StringComparison.Ordinal);
            if (at < 0 || statement.Contains("EXECUTE ON FUNCTION", StringComparison.Ordinal))
            {
                continue;
            }

            if (!statement[(at + marker.Length)..].Split(',').Select(t => t.Trim()).Contains("viewer", StringComparer.Ordinal))
            {
                continue;
            }

            if (skip is not null && string.Equals(statement + ";", skip, StringComparison.Ordinal))
            {
                continue;
            }

            statements.Add(statement[..at] + marker + roleName);
        }

        return statements;
    }

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Owner, string OwnerString)> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer-role server-add test (it mints its own scratch database and role).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        return (scratch, NpgsqlDataSource.Create(ownerString), ownerString);
    }

    private static async Task<NpgsqlDataSource> ProvisionAsync(
        NpgsqlDataSource owner, string ownerString, string roleName, string? skip, CancellationToken ct)
    {
        await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        foreach (var statement in ViewerStatements(roleName, skip))
        {
            await ExecAsync(owner, statement, ct);
        }

        return NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ownerString) { Username = roleName, Password = RolePassword }.ConnectionString);
    }

    private const string OneServer =
        "[{\"host\":\"added-by-viewer\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"env:SOME_SECRET_VARIABLE\"}]";

    [Fact]
    public async Task TheShippedViewerGrants_LetTheAddCoreInsertAServer_AndTheBeaconTriggerFire()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_add_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;

        Assert.Contains(ServerGrant.Replace(" TO viewer;", " TO " + roleName, StringComparison.Ordinal), ViewerStatements(roleName, null));
        await using var asViewer = await ProvisionAsync(owner, ownerString, roleName, null, ct);
        var bodySucceeded = false;
        try
        {
            /* The beacon row is seeded by the running service, not by migration; the trigger updates nothing without it. */
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            long versionBefore;
            await using (var read = owner.CreateCommand("SELECT config_version FROM config_service"))
            {
                versionBefore = Convert.ToInt64(await read.ExecuteScalarAsync(ct));
            }

            var answer = JsonNode.Parse(await DarlingMcpServerAdminTools.AddServersAsync(asViewer, OneServer, Reachable, ct))!;
            Assert.Equal(1, answer["added"]!.GetValue<int>());
            Assert.Equal("added", answer["results"]![0]!["status"]!.GetValue<string>());

            await using (var count = owner.CreateCommand("SELECT count(*) FROM config_monitored_servers WHERE host = 'added-by-viewer'"))
            {
                Assert.Equal(1L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
            }

            /* The write fired trg_bump_monitored_servers AS viewer, through the two-column config_service grant. */
            await using (var read = owner.CreateCommand("SELECT config_version FROM config_service"))
            {
                Assert.True(Convert.ToInt64(await read.ExecuteScalarAsync(ct)) > versionBefore);
            }

            /* The grant is INSERT only: the viewer role cannot delete the definition it inserted (no web route removes one yet). */
            var deleteDenied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var delete = asViewer.CreateCommand("DELETE FROM config_monitored_servers WHERE host = 'added-by-viewer'");
                await delete.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", deleteDenied.SqlState);

            /* The credential column stays unreadable to this role: INSERT needed no read of it. */
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var peek = asViewer.CreateCommand("SELECT encrypted_password FROM config_monitored_servers");
                await peek.ExecuteScalarAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    [Fact]
    public async Task WithoutTheServerGrant_TheAddWrite_Fails42501_AndNothingIsSaved()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_nogrant_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;

        await using var asViewer = await ProvisionAsync(owner, ownerString, roleName, ServerGrant, ct);
        var bodySucceeded = false;
        try
        {
            /* The statement the core runs, as a role that holds every viewer grant EXCEPT this one. */
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var insert = asViewer.CreateCommand(DarlingMcpServerAdminTools.InsertServerSql);
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 7 });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "n" });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "h" });
                insert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "sql" });
                insert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "Mandatory" });
                insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });
                insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });
                insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = Array.Empty<string>() });
                insert.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = 0m });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified) });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "sqlserver" });
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 0 });
                await insert.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);

            /* Through the core, the same refusal is a typed not_saved, with no row written. */
            var answer = JsonNode.Parse(await DarlingMcpServerAdminTools.AddServersAsync(asViewer, OneServer, Reachable, ct))!;
            Assert.Equal(0, answer["added"]!.GetValue<int>());
            Assert.Equal("not_saved", answer["results"]![0]!["status"]!.GetValue<string>());
            await using (var count = owner.CreateCommand("SELECT count(*) FROM config_monitored_servers WHERE host IN ('h', 'added-by-viewer')"))
            {
                Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    [Fact]
    public async Task TheViewerRole_HoldsNoUpdate_OnTheMonitoredServersTable_AndNoWriteOnTheCredentialColumn()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_upd_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;

        await using var asViewer = await ProvisionAsync(owner, ownerString, roleName, null, ct);
        var bodySucceeded = false;
        try
        {
            await using (var seed = owner.CreateCommand("INSERT INTO config_monitored_servers (server_id, name, host) VALUES (-4843, 'seed', 'seed-host')"))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            foreach (var sql in new[]
            {
                "UPDATE config_monitored_servers SET is_enabled = FALSE WHERE server_id = -4843",
                "UPDATE config_monitored_servers SET encrypted_password = 'x' WHERE server_id = -4843",
            })
            {
                var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
                {
                    await using var update = asViewer.CreateCommand(sql);
                    await update.ExecuteNonQueryAsync(ct);
                });
                Assert.Equal("42501", denied.SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
