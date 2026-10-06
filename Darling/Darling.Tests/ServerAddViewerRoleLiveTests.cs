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

    internal static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private const string ServerGrant = "GRANT INSERT ON config.config_monitored_servers TO viewer;";

    /// <summary>Pass as <c>skip</c> to provision a role WITHOUT the edit function (a store whose roles predate it).</summary>
    internal const string MissingEditFunction = "<no edit function>";

    private static readonly DarlingMcpServerAdminTools.ServerProbe Reachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(
            Success: true, MajorVersion: 15, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null));

    /// <summary>Every GRANT / REVOKE in the managed batch that names <c>viewer</c> (alone or in a list), in order,
    /// retargeted at <paramref name="roleName"/>. <paramref name="skip"/> drops statements by exact text.</summary>
    internal static List<string> ViewerStatements(string roleName, string? skip)
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

    internal static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Owner, string OwnerString)> OpenAsync(CancellationToken ct)
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

    internal static async Task<NpgsqlDataSource> ProvisionAsync(
        NpgsqlDataSource owner, string ownerString, string roleName, string? skip, CancellationToken ct)
    {
        await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        foreach (var statement in ViewerStatements(roleName, skip))
        {
            await ExecAsync(owner, statement, ct);
        }

        /* The edit function (#5240), from the shared builder so it is the real one, owned by the owner; EXECUTE for the role. */
        if (!string.Equals(skip, MissingEditFunction, StringComparison.Ordinal))
        {
            await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config"), ct);
            await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.edit_monitored_server({DarlingManagedRoles.EditMonitoredServerSignature}) TO {roleName}", ct);
        }

        return NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ownerString) { Username = roleName, Password = RolePassword }.ConnectionString);
    }

    private const string OneServer =
        "[{\"host\":\"added-by-viewer\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"Viewer-Add-Pw-1\"}]";

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

    private static DarlingMcpServerAdminTools.EditColumnValue Col(string column, NpgsqlTypes.NpgsqlDbType type, object? value) => new(column, column, type, value);

    private static async Task<(string Outcome, DateTime? Token)> CallEditAsync(
        NpgsqlDataSource source, int id, DateTime expected, IReadOnlyList<DarlingMcpServerAdminTools.EditColumnValue> sets, CancellationToken ct)
    {
        await using var call = source.CreateCommand(DarlingMcpServerAdminTools.EditFunctionSql);
        foreach (var parameter in DarlingMcpServerAdminTools.BuildEditFunctionParameters(id, expected, sets))
        {
            call.Parameters.Add(parameter);
        }

        await using var reader = await call.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
    }

    private static async Task<string> RowAsync(NpgsqlDataSource owner, int id, string columns, CancellationToken ct)
    {
        await using var read = owner.CreateCommand($"SELECT concat_ws('|', {columns}) FROM config_monitored_servers WHERE server_id = {id}");
        return (string)(await read.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// The edit's one write as the viewer role (#5240): a direct UPDATE of host is denied, and the function refuses a move
    /// of host that keeps the stored secret on a SQL or service-principal row (the row is unchanged), saves it with a new
    /// secret, answers a stale token as a conflict, saves a name change on a secret row and a Windows-auth host move with no secret.
    /// </summary>
    [Fact]
    public async Task AsTheViewerRole_TheEditFunction_RefusesAMoveThatKeepsTheSecret_AndSavesTheRest()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_fn_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var asViewer = await ProvisionAsync(owner, ownerString, roleName, null, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, "INSERT INTO config_monitored_servers (server_id, name, host, auth, username, encrypted_password) VALUES (7101, 'a', 'a.example.test', 'sql', 'monitor', 'blob-a'), (7102, 'b', 'b.example.test', 'serviceprincipal', 'client-id', 'blob-b'), (7103, 'c', 'c.example.test', 'integrated', NULL, NULL)", ct);
            var host = new[] { Col("host", NpgsqlTypes.NpgsqlDbType.Text, "moved.example.test") };
            var secret = Col("encrypted_password", NpgsqlTypes.NpgsqlDbType.Text, "blob-new");

            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var update = asViewer.CreateCommand("UPDATE config_monitored_servers SET host = 'moved.example.test' WHERE server_id = 7101");
                await update.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);

            foreach (var id in new[] { 7101, 7102 })
            {
                var before = await RowAsync(owner, id, "host, encrypted_password, modified_at", ct);
                var token = DateTime.Parse(before.Split('|')[2], System.Globalization.CultureInfo.InvariantCulture);
                Assert.Equal("password_needed", (await CallEditAsync(asViewer, id, token, host, ct)).Outcome);
                Assert.Equal(before, await RowAsync(owner, id, "host, encrypted_password, modified_at", ct));
            }

            await using var stamp = owner.CreateCommand("SELECT modified_at FROM config_monitored_servers WHERE server_id = 7101");
            var read = (DateTime)(await stamp.ExecuteScalarAsync(ct))!;
            var saved = await CallEditAsync(asViewer, 7101, read, [host[0], secret], ct);
            Assert.Equal("saved", saved.Outcome);
            Assert.True(saved.Token > read);
            Assert.Equal("moved.example.test|blob-new", await RowAsync(owner, 7101, "host, encrypted_password", ct));

            Assert.Equal("conflict", (await CallEditAsync(asViewer, 7101, read, [Col("name", NpgsqlTypes.NpgsqlDbType.Text, "late")], ct)).Outcome);
            Assert.Equal("not_found", (await CallEditAsync(asViewer, 999999, read, host, ct)).Outcome);

            await using var stamp2 = owner.CreateCommand("SELECT modified_at FROM config_monitored_servers WHERE server_id = 7102");
            var read2 = (DateTime)(await stamp2.ExecuteScalarAsync(ct))!;
            Assert.Equal("password_needed", (await CallEditAsync(asViewer, 7102, read2, [Col("encrypt_mode", NpgsqlTypes.NpgsqlDbType.Text, "Optional")], ct)).Outcome);
            Assert.Equal("Mandatory|blob-b", await RowAsync(owner, 7102, "encrypt_mode, encrypted_password", ct));
            Assert.Equal("saved", (await CallEditAsync(asViewer, 7102, read2, [Col("name", NpgsqlTypes.NpgsqlDbType.Text, "b-renamed")], ct)).Outcome);
            Assert.Equal("b-renamed|blob-b", await RowAsync(owner, 7102, "name, encrypted_password", ct));

            await using var stamp3 = owner.CreateCommand("SELECT modified_at FROM config_monitored_servers WHERE server_id = 7103");
            var read3 = (DateTime)(await stamp3.ExecuteScalarAsync(ct))!;
            Assert.Equal("saved", (await CallEditAsync(asViewer, 7103, read3, host, ct)).Outcome);
            Assert.Equal("moved.example.test", await RowAsync(owner, 7103, "host", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    /// <summary>
    /// The edit function refuses the same connection changes the route refuses (#5240): on a secret row, a direct call as the
    /// viewer role with no new secret answers <c>password_needed</c> and leaves the row unchanged for each of port, database,
    /// read-only intent, authentication, username, encrypt mode, certificate trust and multi-subnet failover. A change of
    /// only the display name, and a change that comes with a new secret, still save.
    /// </summary>
    [Fact]
    public async Task AsTheViewerRole_TheEditFunction_RefusesEveryConnectionChangeThatKeepsTheSecret()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_fnc_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var asViewer = await ProvisionAsync(owner, ownerString, roleName, null, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, "INSERT INTO config_monitored_servers (server_id, name, host, port, database, auth, username, encrypted_password, encrypt_mode, trust_server_certificate, read_only_intent, multi_subnet_failover) VALUES (7201, 'a', 'a.example.test', 1433, 'master', 'sql', 'monitor', 'blob-a', 'Mandatory', false, false, false)", ct);
            const string cols = "name, host, port, database, auth, username, encrypted_password, encrypt_mode, trust_server_certificate, read_only_intent, multi_subnet_failover, modified_at";

            var changes = new (string Label, DarlingMcpServerAdminTools.EditColumnValue Change)[]
            {
                ("port", Col("port", NpgsqlTypes.NpgsqlDbType.Integer, 1444)),
                ("database", Col("database", NpgsqlTypes.NpgsqlDbType.Text, "other")),
                ("read_only_intent", Col("read_only_intent", NpgsqlTypes.NpgsqlDbType.Boolean, true)),
                ("auth", Col("auth", NpgsqlTypes.NpgsqlDbType.Text, "serviceprincipal")),
                ("username", Col("username", NpgsqlTypes.NpgsqlDbType.Text, "someone-else")),
                ("encrypt_mode", Col("encrypt_mode", NpgsqlTypes.NpgsqlDbType.Text, "Optional")),
                ("trust_server_certificate", Col("trust_server_certificate", NpgsqlTypes.NpgsqlDbType.Boolean, true)),
                ("multi_subnet_failover", Col("multi_subnet_failover", NpgsqlTypes.NpgsqlDbType.Boolean, true)),
            };

            foreach (var (label, change) in changes)
            {
                var before = await RowAsync(owner, 7201, cols, ct);
                var token = DateTime.Parse(before.Split('|')[^1], System.Globalization.CultureInfo.InvariantCulture);
                Assert.True("password_needed" == (await CallEditAsync(asViewer, 7201, token, [change], ct)).Outcome, $"{label} change with no new secret was not refused");
                Assert.Equal(before, await RowAsync(owner, 7201, cols, ct));
            }

            var current = await RowAsync(owner, 7201, cols, ct);
            var currentToken = DateTime.Parse(current.Split('|')[^1], System.Globalization.CultureInfo.InvariantCulture);
            var renamed = await CallEditAsync(asViewer, 7201, currentToken, [Col("name", NpgsqlTypes.NpgsqlDbType.Text, "renamed")], ct);
            Assert.Equal("saved", renamed.Outcome);
            Assert.Equal("renamed|blob-a", await RowAsync(owner, 7201, "name, encrypted_password", ct));

            var withSecret = await CallEditAsync(asViewer, 7201, renamed.Token!.Value,
                [Col("database", NpgsqlTypes.NpgsqlDbType.Text, "other"), Col("encrypted_password", NpgsqlTypes.NpgsqlDbType.Text, "blob-new")], ct);
            Assert.Equal("saved", withSecret.Outcome);
            Assert.Equal("other|blob-new", await RowAsync(owner, 7201, "database, encrypted_password", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    /// <summary>The batch's REVOKE (#5240) takes away any UPDATE an earlier build gave viewer (column-level or table-level),
    /// every run, and a second run changes nothing.</summary>
    [Fact]
    public async Task EveryRunOfTheBatch_TakesAnyViewerUpdateOnTheMonitoredServersTableAway_AndIsIdempotent()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_rst_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var asViewer = await ProvisionAsync(owner, ownerString, roleName, null, ct);
        var bodySucceeded = false;
        try
        {
            Assert.Equal("", await GrantedUpdateColumnsAsync(owner, roleName, ct));
            await ExecAsync(owner, $"GRANT UPDATE (name, host, encrypted_password, modified_at) ON config.config_monitored_servers TO {roleName}", ct);
            Assert.NotEqual("", await GrantedUpdateColumnsAsync(owner, roleName, ct));
            await ExecAsync(owner, $"GRANT UPDATE ON config.config_monitored_servers TO {roleName}", ct);
            Assert.True(await HasTableLevelUpdateAsync(owner, roleName, ct), "setup: the table-level UPDATE did not take");

            foreach (var run in new[] { 1, 2 })
            {
                await RunViewerStatementsAsync(owner, roleName, ct);
                Assert.Equal("", await GrantedUpdateColumnsAsync(owner, roleName, ct));
                Assert.False(await HasTableLevelUpdateAsync(owner, roleName, ct), $"run {run} left a table-level UPDATE");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task<string> GrantedUpdateColumnsAsync(NpgsqlDataSource owner, string roleName, CancellationToken ct)
    {
        await using var granted = owner.CreateCommand(
            "SELECT COALESCE(string_agg(column_name, ',' ORDER BY column_name COLLATE \"C\"), '') FROM information_schema.column_privileges " +
            $"WHERE grantee = '{roleName}' AND table_schema = 'config' AND table_name = 'config_monitored_servers' AND privilege_type = 'UPDATE'");
        return (string)(await granted.ExecuteScalarAsync(ct))!;
    }

    private static async Task RunViewerStatementsAsync(NpgsqlDataSource owner, string roleName, CancellationToken ct)
    {
        foreach (var statement in ViewerStatements(roleName, null))
        {
            await ExecAsync(owner, statement, ct);
        }
    }

    private static async Task<bool> HasTableLevelUpdateAsync(NpgsqlDataSource owner, string roleName, CancellationToken ct)
    {
        await using var tableLevel = owner.CreateCommand($"SELECT has_table_privilege('{roleName}', 'config.config_monitored_servers', 'UPDATE')");
        return (bool)(await tableLevel.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
