/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one. The managed role names are cluster-wide, so it shares the live-postgres collection with the other
   classes that provision them, and it skips when a set of those names already exists on the rig. */

/// <summary>
/// The store's own password rules, as the roles that meet them. The shipped provisioning batch runs whole on a scratch
/// store, then each role connects as itself: the viewer, admin and MCP roles hand the store a password and never a
/// reference (env: or file:), and a change of how a server is reached keeps no stored password. The store owner is held
/// to neither, so a configuration-file seed and --add-server keep working, and a row already in the table is never
/// touched. Each scenario runs against the managed batch's text and against the self-managed script's.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreServerPasswordRulesLiveTests
{
    private const string ReferenceSentence = "Enter the password itself. References (env: or file:) can only be set in the configuration file.";
    private const string MoveSentence = "Changing how this server is reached needs its password again: it is stored encrypted and this surface cannot read it back.";
    private const string RemediationSentence = "This server has a remediation login stored. Change how it is reached on the service host, in the configuration file or with --add-server.";
    private const string ReferenceState = "PW001";
    private const string MoveState = "PW002";
    private const string RemediationState = "PW003";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the store password rule tests (each mints its own scratch database).");
        return connectionString!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static string OwnerRoleOf(NpgsqlConnection owner)
        => new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

    private static async Task<NpgsqlConnection> ConnectAsync(string ownerString, string role, string password, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(ownerString)
        {
            Username = role,
            Password = password,
            SearchPath = "collect,config,public",
            Pooling = false,
        }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    /// <summary>The function and trigger as the self-managed script carries them, cut out of the script's text.</summary>
    private static string ByoRulesSql()
    {
        var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = script.IndexOf("CREATE OR REPLACE FUNCTION config.monitored_server_password_rules()", StringComparison.Ordinal);
        Assert.True(start >= 0, "provision-roles.sql carries no password rules function");
        const string triggerEnd = "EXECUTE FUNCTION config.monitored_server_password_rules();";
        var end = script.IndexOf(triggerEnd, start, StringComparison.Ordinal);
        Assert.True(end >= 0, "provision-roles.sql carries no password rules trigger");
        return script[start..(end + triggerEnd.Length)];
    }

    /// <summary>The edit function as the self-managed script carries it, cut out of the script's text.</summary>
    private static string ByoEditFunctionSql()
    {
        var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = script.IndexOf("CREATE OR REPLACE FUNCTION config.edit_monitored_server(", StringComparison.Ordinal);
        Assert.True(start >= 0, "provision-roles.sql carries no edit function");
        const string functionEnd = "$fn$;";
        var end = script.IndexOf(functionEnd, start, StringComparison.Ordinal);
        Assert.True(end >= 0, "provision-roles.sql carries no end for the edit function");
        return script[start..(end + functionEnd.Length)];
    }

    private static async Task InsertServerAsync(
        NpgsqlConnection connection, int id, string host, string? secret, string? remediationSecret, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, remediation_encrypted_password, engine, port) " +
            "VALUES ($1, $2, $3, 'master', 'sql', 'monitor', $4, $5, 'sqlserver', 0)", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        command.Parameters.AddWithValue("srv-" + id);
        command.Parameters.AddWithValue(host);
        command.Parameters.AddWithValue(secret is null ? DBNull.Value : secret);
        command.Parameters.AddWithValue(remediationSecret is null ? DBNull.Value : remediationSecret);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<PostgresException> RefusedAsync(Func<Task> write, string sqlState, string sentence)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(write);
        Assert.Equal(sqlState, ex.SqlState);
        Assert.Equal(sentence, ex.MessageText);
        return ex;
    }

    private static async Task<string?> EditOutcomeAsync(
        NpgsqlConnection role, int id, string columns, string? host, string? secret, NpgsqlConnection ownerConnection, CancellationToken ct)
    {
        var token = await TextAsync(ownerConnection, $"SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config_monitored_servers WHERE server_id = {id}", ct);
        string Lit(string? value) => value is null ? "NULL" : "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        return await TextAsync(role,
            $"SELECT outcome FROM config.edit_monitored_server({id}, '{token}'::timestamp, ARRAY[{columns}]::text[], NULL, {Lit(host)}, NULL, NULL, NULL, NULL, NULL, {Lit(secret)}, NULL, NULL, NULL, NULL)", ct);
    }

    private static async Task RunScenarioAsync(bool selfManagedText, Func<string, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);

        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await TextAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" }.ConnectionString);
            await owner.OpenAsync(ct);
            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName));
            await ExecAsync(owner, batch, ct);
            if (selfManagedText)
            {
                await ExecAsync(owner, ByoEditFunctionSql(), ct);
                await ExecAsync(owner, ByoRulesSql(), ct);
            }

            await body(scratch.ConnectionString, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    private const string ProtectedBlob = "AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAA-a-protected-blob";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheViewerRole_GivesThePasswordItself_NeverAReference(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await using var viewer = await ConnectAsync(ownerString, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);

            await RefusedAsync(() => InsertServerAsync(viewer, 7301, "h-env", "env:SOME_SECRET", null, ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => InsertServerAsync(viewer, 7302, "h-file", "file:/run/secrets/sql_password", null, ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => InsertServerAsync(viewer, 7303, "h-rem", ProtectedBlob, "file:/run/secrets/remediation", ct), ReferenceState, ReferenceSentence);
            Assert.Equal("0", await TextAsync(owner, "SELECT count(*) FROM config_monitored_servers", ct));

            /* The rule is the resolver's own: case-sensitive, at the start of the text. A password that merely contains the
               prefix, or spells it another way, is a password. */
            await InsertServerAsync(viewer, 7304, "h-plain", ProtectedBlob, null, ct);
            await InsertServerAsync(viewer, 7305, "h-upper", "ENV:NOT_A_REFERENCE", null, ct);
            await InsertServerAsync(viewer, 7306, "h-lead", " env:NOT_A_REFERENCE", null, ct);
            await InsertServerAsync(viewer, 7307, "h-inside", "xenv:NOT_A_REFERENCE", null, ct);
            await InsertServerAsync(viewer, 7308, "h-none", null, null, ct);
            Assert.Equal("5", await TextAsync(owner, "SELECT count(*) FROM config_monitored_servers", ct));

            /* The edit function: a reference is refused with its own outcome and nothing is written. */
            Assert.Equal("reference_refused", await EditOutcomeAsync(viewer, 7304, "'encrypted_password'", null, "env:SOME_SECRET", owner, ct));
            Assert.Equal("reference_refused", await EditOutcomeAsync(viewer, 7304, "'encrypted_password'", null, "file:/run/secrets/x", owner, ct));
            Assert.Equal("reference_refused", await EditOutcomeAsync(viewer, 7304, "'host','encrypted_password'", "moved-host", "file:/run/secrets/x", owner, ct));
            Assert.Equal(ProtectedBlob, await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7304", ct));
            Assert.Equal("h-plain", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7304", ct));

            /* What the function does for the web edit still works: a move with a new password is saved (its own UPDATE
               runs as the function's owner), a move without one is still asked for the password, an ordinary password is
               stored as given. */
            Assert.Equal("password_needed", await EditOutcomeAsync(viewer, 7304, "'host'", "moved-host", null, owner, ct));
            Assert.Equal("saved", await EditOutcomeAsync(viewer, 7304, "'host','encrypted_password'", "moved-host", "a-new-protected-blob", owner, ct));
            Assert.Equal("moved-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7304", ct));
            Assert.Equal("a-new-protected-blob", await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7304", ct));
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheAdminRole_CannotSetAReference_OrMoveAServerThatKeepsItsStoredPassword(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await InsertServerAsync(owner, 7401, "keep-host", ProtectedBlob, null, ct);
            await InsertServerAsync(owner, 7402, "ref-host", "file:/run/secrets/sql_password", null, ct);
            await InsertServerAsync(owner, 7403, "rem-host", ProtectedBlob, "AQAAANCMnd8-a-remediation-blob", ct);
            await using var admin = await ConnectAsync(ownerString, "admin", ProvisioningTestSecrets.AdminPassword, ct);

            /* A reference, set by an update or inserted. */
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET encrypted_password = 'file:/run/secrets/other' WHERE server_id = 7401", ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET encrypted_password = 'env:OTHER', host = 'own-host' WHERE server_id = 7401", ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET remediation_encrypted_password = 'env:OTHER' WHERE server_id = 7401", ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET encrypted_password = 'file:/run/secrets/changed' WHERE server_id = 7402", ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => InsertServerAsync(admin, 7404, "new-env", "env:SOME_SECRET", null, ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => InsertServerAsync(admin, 7405, "new-file", "file:/etc/secret", null, ct), ReferenceState, ReferenceSentence);

            /* A move that keeps the stored password: the host, the port, the engine, and each of the other connection fields. */
            foreach (var change in new[]
            {
                "host = 'own-host'", "port = 5432", "engine = 'postgresql'", "database = 'other'", "read_only_intent = TRUE",
                "auth = 'serviceprincipal'", "username = 'someone-else'", "encrypt_mode = 'Optional'",
                "trust_server_certificate = TRUE", "multi_subnet_failover = TRUE",
            })
            {
                await RefusedAsync(() => ExecAsync(admin, $"UPDATE config_monitored_servers SET {change} WHERE server_id = 7401", ct), MoveState, MoveSentence);
            }

            /* A new password is not enough while a remediation password is still stored with it, and a move that keeps a
               stored reference is refused like any other. */
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET host = 'own-host', encrypted_password = 'a-new-protected-blob' WHERE server_id = 7403", ct), RemediationState, RemediationSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET host = 'own-host' WHERE server_id = 7403", ct), RemediationState, RemediationSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET host = 'own-host' WHERE server_id = 7402", ct), MoveState, MoveSentence);
            Assert.Equal("keep-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7401", ct));
            Assert.Equal(ProtectedBlob, await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7401", ct));

            /* What an admin write may still do. */
            await ExecAsync(admin, "UPDATE config_monitored_servers SET host = 'new-host', encrypted_password = 'a-new-protected-blob' WHERE server_id = 7401", ct);
            Assert.Equal("new-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7401", ct));
            await ExecAsync(admin, "UPDATE config_monitored_servers SET host = 'cleared-host', encrypted_password = NULL, auth = 'integrated' WHERE server_id = 7401", ct);
            await ExecAsync(admin, "UPDATE config_monitored_servers SET name = 'renamed', is_enabled = FALSE, monthly_cost_usd = 12, excluded_databases = '{a}' WHERE server_id = 7402", ct);
            await ExecAsync(admin, "UPDATE config_monitored_servers SET trust_server_certificate = trust_server_certificate WHERE server_id = 7402", ct);
            Assert.Equal("file:/run/secrets/sql_password", await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7402", ct));
            await InsertServerAsync(admin, 7406, "admin-plain", ProtectedBlob, null, ct);

            /* The Viewer's edit is an upsert that sends the stored value back: an unchanged reference passes, a changed one
               does not, and neither lets a move keep the stored password. */
            const string upsert =
                "INSERT INTO config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, engine, port) " +
                "VALUES (7402, 'renamed again', {0}, 'master', 'sql', 'monitor', {1}, 'sqlserver', 0) " +
                "ON CONFLICT (server_id) DO UPDATE SET name = EXCLUDED.name, host = EXCLUDED.host, encrypted_password = EXCLUDED.encrypted_password";
            await ExecAsync(admin, string.Format(System.Globalization.CultureInfo.InvariantCulture, upsert, "'ref-host'", "'file:/run/secrets/sql_password'"), ct);
            Assert.Equal("renamed again", await TextAsync(owner, "SELECT name FROM config_monitored_servers WHERE server_id = 7402", ct));
            await RefusedAsync(() => ExecAsync(admin, string.Format(System.Globalization.CultureInfo.InvariantCulture, upsert, "'ref-host'", "'file:/run/secrets/changed'"), ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => ExecAsync(admin, string.Format(System.Globalization.CultureInfo.InvariantCulture, upsert, "'own-host'", "'file:/run/secrets/sql_password'"), ct), MoveState, MoveSentence);
            await RefusedAsync(() => ExecAsync(admin, string.Format(System.Globalization.CultureInfo.InvariantCulture, upsert.Replace("7402", "7499", StringComparison.Ordinal), "'ref-host'", "'file:/run/secrets/sql_password'"), ct), ReferenceState, ReferenceSentence);
            Assert.Equal("ref-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7402", ct));
        });
    }

    /// <summary>
    /// A role that may create a temporary table puts one named like a catalog or a table the store's code reads first in its
    /// own session: the rules and the edit function name what they read, and keep the temporary schema last.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ATemporaryTableNamedLikeACatalogOrAStoreTable_ChangesNothingTheRulesOrTheEditFunctionRead(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await InsertServerAsync(owner, 7901, "keep-host", ProtectedBlob, null, ct);
            await using var viewer = await ConnectAsync(ownerString, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);
            await using var mcp = await ConnectAsync(ownerString, "mcp", ProvisioningTestSecrets.McpPassword, ct);

            static async Task ShadowCatalogAsync(NpgsqlConnection role, CancellationToken token)
            {
                await ExecAsync(role, "CREATE TEMP TABLE pg_class (oid oid, relowner oid)", token);
                await ExecAsync(role,
                    "INSERT INTO pg_class VALUES ('config.config_monitored_servers'::regclass, (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user))", token);
            }

            await ShadowCatalogAsync(viewer, ct);
            await RefusedAsync(() => InsertServerAsync(viewer, 7902, "h-env", "env:SOME_SECRET", null, ct), ReferenceState, ReferenceSentence);

            await ShadowCatalogAsync(mcp, ct);
            await RefusedAsync(() => ExecAsync(mcp, "UPDATE config_monitored_servers SET host = 'moved' WHERE server_id = 7901", ct), MoveState, MoveSentence);
            Assert.Equal("keep-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7901", ct));

            /* The edit function reads and writes the store's table, not a temporary table of the same name. */
            await ExecAsync(viewer, "CREATE TEMP TABLE config_monitored_servers (server_id integer)", ct);
            Assert.Equal("saved", await EditOutcomeAsync(viewer, 7901, "'host','encrypted_password'", "moved-host", "a-new-protected-blob", owner, ct));
            Assert.Equal("moved-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7901", ct));
        });
    }

    /// <summary>
    /// A role that may create temporary objects puts a table named like each table the two definer functions use
    /// (<c>config.edit_monitored_server</c> and <c>config.record_custom_alert_resolution</c>) in its own session, with a
    /// trigger on it that would record who runs it. Called as that role, each function writes the store's table, the trigger
    /// never fires, and nothing runs in the function owner's name through it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ATemporaryTableAndTriggerNamedLikeAStoreTable_IsNeverUsedByTheDefinerFunctions(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            var next = 7950;
            foreach (var (roleName, password) in new[] { ("viewer", ProvisioningTestSecrets.ViewerPassword), ("mcp", ProvisioningTestSecrets.McpPassword) })
            {
                var serverId = next++;
                var metric = "shadow-metric-" + roleName;
                await InsertServerAsync(owner, serverId, "keep-host", ProtectedBlob, null, ct);
                await using var role = await ConnectAsync(ownerString, roleName, password, ct);

                await ExecAsync(role, "CREATE TEMP TABLE trigger_log (who text)", ct);
                await ExecAsync(role,
                    "CREATE FUNCTION pg_temp.shadow_trigger() RETURNS trigger LANGUAGE plpgsql AS $fn$ " +
                    "BEGIN INSERT INTO pg_temp.trigger_log VALUES (current_user); RETURN NEW; END $fn$", ct);

                await ExecAsync(role,
                    "CREATE TEMP TABLE config_monitored_servers (server_id integer, name text, host text, port integer, database text, " +
                    "read_only_intent boolean, auth text, username text, encrypted_password text, remediation_encrypted_password text, " +
                    "encrypt_mode text, trust_server_certificate boolean, multi_subnet_failover boolean, monthly_cost_usd numeric, modified_at timestamp)", ct);
                var token = await TextAsync(owner, $"SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config_monitored_servers WHERE server_id = {serverId}", ct);
                await ExecAsync(role,
                    $"INSERT INTO config_monitored_servers (server_id, host, auth, modified_at) VALUES ({serverId}, 'keep-host', 'sql', '{token}'::timestamp)", ct);
                await ExecAsync(role,
                    "CREATE TRIGGER shadow BEFORE UPDATE ON config_monitored_servers FOR EACH ROW EXECUTE FUNCTION pg_temp.shadow_trigger()", ct);

                await ExecAsync(role,
                    "CREATE TEMP TABLE config_alert_log (alert_time timestamp, server_id integer, server_name text, metric_name text, " +
                    "current_value double precision, threshold_value double precision, alert_sent boolean, notification_type text, " +
                    "send_error text, muted boolean, detail_text text, context_json text)", ct);
                await ExecAsync(role,
                    "CREATE TRIGGER shadow BEFORE INSERT ON config_alert_log FOR EACH ROW EXECUTE FUNCTION pg_temp.shadow_trigger()", ct);

                Assert.Equal("saved", await TextAsync(role,
                    $"SELECT outcome FROM config.edit_monitored_server({serverId}, '{token}'::timestamp, ARRAY['host','encrypted_password']::text[], " +
                    "NULL, 'moved-host', NULL, NULL, NULL, NULL, NULL, 'a-new-protected-blob', NULL, NULL, NULL, NULL)", ct));
                await ExecAsync(role, $"SELECT config.record_custom_alert_resolution({serverId}, 'srv', '{metric}', 'detail')", ct);

                Assert.Equal("0", await TextAsync(role, "SELECT count(*) FROM pg_temp.trigger_log", ct));
                Assert.Equal("moved-host", await TextAsync(owner, $"SELECT host FROM config_monitored_servers WHERE server_id = {serverId}", ct));
                Assert.Equal("1", await TextAsync(owner, $"SELECT count(*) FROM config_alert_log WHERE server_id = {serverId} AND metric_name = '{metric}'", ct));
                Assert.Equal("keep-host", await TextAsync(role, $"SELECT host FROM pg_temp.config_monitored_servers WHERE server_id = {serverId}", ct));
                Assert.Equal("0", await TextAsync(role, "SELECT count(*) FROM pg_temp.config_alert_log", ct));
            }
        });
    }

    private const string RemediationBlob = "AQAAANCMnd8-a-remediation-blob";

    /// <summary>
    /// A change to how a server is reached on a row that holds a remediation login is refused by the edit function whatever
    /// else the call sends, and nothing is written; the same call on a row without one saves. The store's own trigger refuses
    /// the same change made directly, with the same sentence.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AChangeToHowAServerIsReached_OnARowWithARemediationLogin_IsRefusedAndWritesNothing(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await InsertServerAsync(owner, 8001, "rem-viewer", ProtectedBlob, RemediationBlob, ct);
            await InsertServerAsync(owner, 8002, "plain-viewer", ProtectedBlob, null, ct);
            await InsertServerAsync(owner, 8003, "rem-mcp", ProtectedBlob, RemediationBlob, ct);
            await InsertServerAsync(owner, 8004, "plain-mcp", ProtectedBlob, null, ct);
            await using var viewer = await ConnectAsync(ownerString, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);
            await using var mcp = await ConnectAsync(ownerString, "mcp", ProvisioningTestSecrets.McpPassword, ct);

            foreach (var (role, remediationId, plainId, host) in new[] { (viewer, 8001, 8002, "rem-viewer"), (mcp, 8003, 8004, "rem-mcp") })
            {
                Assert.Equal("remediation_kept", await EditOutcomeAsync(role, remediationId, "'host','encrypted_password'", "evil-host", "typed-anything", owner, ct));
                Assert.Equal("remediation_kept", await EditOutcomeAsync(role, remediationId, "'host'", "evil-host", null, owner, ct));
                Assert.Equal(host + "|" + ProtectedBlob + "|" + RemediationBlob,
                    await TextAsync(owner, $"SELECT host || '|' || encrypted_password || '|' || remediation_encrypted_password FROM config_monitored_servers WHERE server_id = {remediationId}", ct));

                /* A change that is not to how the row is reached still saves, and leaves the remediation login alone. */
                Assert.Equal("saved", await EditOutcomeAsync(role, remediationId, "'encrypted_password'", null, "a-new-protected-blob", owner, ct));
                Assert.Equal(RemediationBlob, await TextAsync(owner, $"SELECT remediation_encrypted_password FROM config_monitored_servers WHERE server_id = {remediationId}", ct));

                Assert.Equal("saved", await EditOutcomeAsync(role, plainId, "'host','encrypted_password'", "moved-host", "a-new-protected-blob", owner, ct));
                Assert.Equal("moved-host", await TextAsync(owner, $"SELECT host FROM config_monitored_servers WHERE server_id = {plainId}", ct));
            }

            await RefusedAsync(() => ExecAsync(mcp, "UPDATE config_monitored_servers SET host = 'evil-host' WHERE server_id = 8003", ct), RemediationState, RemediationSentence);
            await RefusedAsync(() => ExecAsync(mcp, "UPDATE config_monitored_servers SET host = 'evil-host', encrypted_password = 'typed-anything' WHERE server_id = 8003", ct), RemediationState, RemediationSentence);
            Assert.Equal("rem-mcp", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 8003", ct));
        });
    }

    /// <summary>
    /// A self-managed store's rules are created on the service's start, as the role that owns its tables, when the script was
    /// not re-run after an upgrade; running it again changes nothing, and a login that may not create them gets one warning
    /// that names the script and fails nothing.
    /// </summary>
    [Fact]
    public async Task ASelfManagedStoreWithNoRules_GetsThemFromTheServicesStart_AndALoginThatMayNotCreateThemGetsOneWarning()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var login = "rules_probe_" + Guid.NewGuid().ToString("N")[..8];
        const string triggerCount = "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'config.config_monitored_servers'::regclass AND tgname = 'trg_monitored_server_password_rules'";
        const string functionCount = "SELECT count(*) FROM pg_proc WHERE oid = 'config.monitored_server_password_rules()'::regprocedure";

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.Equal("0", await TextAsync(owner, triggerCount, ct));

            var log = new CapturingTestLogger();
            await using (var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, log, ct), log.Joined);
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, log, ct), log.Joined);
            }

            Assert.Equal("1", await TextAsync(owner, triggerCount, ct));
            Assert.Equal("1", await TextAsync(owner, functionCount, ct));
            Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            await ExecAsync(owner, "DROP TRIGGER trg_monitored_server_password_rules ON config.config_monitored_servers", ct);
            await ExecAsync(owner, $"CREATE ROLE {login} LOGIN PASSWORD 'rules-probe-1'", ct);
            var restricted = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = login, Password = "rules-probe-1" }.ConnectionString;
            var warned = new CapturingTestLogger();
            await using (var restrictedSource = NpgsqlDataSource.Create(restricted))
            {
                Assert.False(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(restrictedSource, warned, ct), warned.Joined);
            }

            Assert.Equal(1, warned.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Contains("provision-roles.sql", warned.Joined, StringComparison.Ordinal);
            Assert.Equal("0", await TextAsync(owner, triggerCount, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup, $"DROP OWNED BY {login}; DROP ROLE IF EXISTS {login}", cleanupCt));
        }
    }

    [Fact]
    public async Task TheMcpRole_GivesThePasswordItself_NeverAReference()
    {
        await RunScenarioAsync(selfManagedText: false, async (ownerString, owner, ct) =>
        {
            await using var mcp = await ConnectAsync(ownerString, "mcp", ProvisioningTestSecrets.McpPassword, ct);

            await RefusedAsync(() => InsertServerAsync(mcp, 7501, "mcp-env", "env:SOME_SECRET", null, ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => InsertServerAsync(mcp, 7502, "mcp-file", "file:/run/secrets/sql_password", null, ct), ReferenceState, ReferenceSentence);
            await InsertServerAsync(mcp, 7503, "mcp-plain", ProtectedBlob, null, ct);
            await InsertServerAsync(owner, 7504, "seeded", "file:/run/secrets/seeded", null, ct);

            /* The upsert shape's pass is the admin's alone: mcp reads no secret column, so the reference it would send
               back is refused. */
            await RefusedAsync(() => ExecAsync(mcp,
                "INSERT INTO config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, engine, port) " +
                "VALUES (7504, 'x', 'seeded', 'master', 'sql', 'monitor', 'file:/run/secrets/seeded', 'sqlserver', 0) " +
                "ON CONFLICT (server_id) DO UPDATE SET name = EXCLUDED.name", ct), ReferenceState, ReferenceSentence);

            Assert.Equal("reference_refused", await EditOutcomeAsync(mcp, 7503, "'encrypted_password'", null, "env:SOME_SECRET", owner, ct));
            Assert.Equal("reference_refused", await EditOutcomeAsync(mcp, 7503, "'encrypted_password'", null, "file:/run/secrets/x", owner, ct));
            Assert.Equal("password_needed", await EditOutcomeAsync(mcp, 7503, "'host'", "moved-host", null, owner, ct));
            Assert.Equal("saved", await EditOutcomeAsync(mcp, 7503, "'host','encrypted_password'", "moved-host", "a-new-protected-blob", owner, ct));
            Assert.Equal("a-new-protected-blob", await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7503", ct));

            /* mcp may still remove a server, as before. */
            await ExecAsync(mcp, "DELETE FROM config_monitored_servers WHERE server_id = 7503", ct);
        });
    }

    [Fact]
    public async Task TheViewersEditAsAdmin_Saves_AndAChangeThatKeepsTheStoredPassword_ShowsTheMoveSentence()
    {
        await RunScenarioAsync(selfManagedText: false, async (ownerString, owner, ct) =>
        {
            await InsertServerAsync(owner, 7801, "viewer-host", ProtectedBlob, null, ct);
            await InsertServerAsync(owner, 7802, "viewer-ref-host", "file:/run/secrets/sql_password", null, ct);
            var adminString = new NpgsqlConnectionStringBuilder(ownerString)
            {
                Username = "admin",
                Password = ProvisioningTestSecrets.AdminPassword,
                SearchPath = "collect,config,public",
                Pooling = false,
            }.ConnectionString;
            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(adminString);

            PerformanceMonitor.Darling.Viewer.MonitoredServerRow Row(int id, string host, string blob, string encryptMode, string name) => new()
            {
                ServerId = id,
                Name = name,
                Host = host,
                Database = "master",
                Auth = "sql",
                Username = "monitor",
                EncryptedPassword = blob,
                EncryptMode = encryptMode,
            };

            /* A rename saves, a new blob with a change saves, and the stored reference sent back unchanged saves. */
            await viewer.UpsertMonitoredServerAsync(Row(7801, "viewer-host", ProtectedBlob, "Mandatory", "renamed"), ct);
            await viewer.UpsertMonitoredServerAsync(Row(7802, "viewer-ref-host", "file:/run/secrets/sql_password", "Mandatory", "renamed ref"), ct);
            await viewer.UpsertMonitoredServerAsync(Row(7801, "viewer-host", "a-new-protected-blob", "Optional", "renamed"), ct);
            Assert.Equal("Optional", await TextAsync(owner, "SELECT encrypt_mode FROM config_monitored_servers WHERE server_id = 7801", ct));

            /* A change the Viewer's own check does not look at, with the stored password kept: the store's refusal, in the
               sentence the Viewer shows for a moved host. */
            var refused = await Assert.ThrowsAsync<PerformanceMonitor.Darling.Viewer.MonitoredServerPasswordNeededException>(
                () => viewer.UpsertMonitoredServerAsync(Row(7801, "viewer-host", "a-new-protected-blob", "Mandatory", "renamed"), ct));
            Assert.Equal(PerformanceMonitor.Darling.Viewer.ViewerDataService.EditPasswordNeededText, refused.Message);
            Assert.Equal("Optional", await TextAsync(owner, "SELECT encrypt_mode FROM config_monitored_servers WHERE server_id = 7801", ct));
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheStoreOwner_StillSetsReferencesAndMovesServers(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            /* The configuration-file seed and --add-server: references go in, and a server moves with the stored value kept. */
            await InsertServerAsync(owner, 7601, "seed-env", "env:SQL_PASSWORD", null, ct);
            await InsertServerAsync(owner, 7602, "seed-file", "file:/run/secrets/sql_password", "file:/run/secrets/remediation", ct);
            await ExecAsync(owner, "UPDATE config_monitored_servers SET encrypted_password = 'file:/run/secrets/rotated' WHERE server_id = 7601", ct);
            await ExecAsync(owner, "UPDATE config_monitored_servers SET host = 'moved', port = 1433, engine = 'sqlserver', database = 'other' WHERE server_id = 7602", ct);
            Assert.Equal("moved", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 7602", ct));
            Assert.Equal("file:/run/secrets/rotated", await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7601", ct));
        });
    }

    [Fact]
    public async Task ARowAlreadyHoldingAReference_IsNotTouchedByProvisioning_AndStillReads()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        await using (var probe = new NpgsqlConnection(scratch.ConnectionString))
        {
            await probe.OpenAsync(ct);
            await PgMigrations.MigrateAsync(probe, ct);
            Assert.SkipWhen(
                await TextAsync(probe, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" }.ConnectionString);
            await owner.OpenAsync(ct);

            /* The reference was stored before the rules existed, with a modified_at that provisioning must leave alone. */
            await InsertServerAsync(owner, 7701, "old-ref", "file:/run/secrets/sql_password", "env:REMEDIATION", ct);
            var before = await TextAsync(owner, "SELECT encrypted_password || '|' || remediation_encrypted_password || '|' || host || '|' || modified_at::text FROM config_monitored_servers WHERE server_id = 7701", ct);

            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName));
            await ExecAsync(owner, batch, ct);
            await ExecAsync(owner, batch, ct);

            Assert.Equal(before, await TextAsync(owner, "SELECT encrypted_password || '|' || remediation_encrypted_password || '|' || host || '|' || modified_at::text FROM config_monitored_servers WHERE server_id = 7701", ct));
            Assert.Equal("1", await TextAsync(owner, "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'config.config_monitored_servers'::regclass AND tgname = 'trg_monitored_server_password_rules'", ct));

            /* The collection read the owner makes still resolves the stored reference. */
            Assert.Equal("file:/run/secrets/sql_password", await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 7701", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }
}
