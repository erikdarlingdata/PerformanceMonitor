/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one. The managed role names are cluster-wide, so it runs in the pg-cluster-roles collection (#5602), alone,
   with the other classes that provision them, and it skips when a set of those names already exists on the rig. */

/// <summary>
/// The store's own password rules, as the roles that meet them. The shipped provisioning batch runs whole on a scratch
/// store, then each role connects as itself: the viewer, admin and MCP roles hand the store a password and never a
/// reference (env: or file:), and a change of how a server is reached keeps no stored password. The store owner is held
/// to neither, so a configuration-file seed and --add-server keep working, and a row already in the table is never
/// touched. Each scenario runs against the managed batch's text and against the self-managed script's.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class StoreServerPasswordRulesLiveTests
{
    private const string ReferenceSentence = "Enter the password itself. References (env: or file:) can only be set in the configuration file.";
    private const string MoveSentence = "Changing the host, port or any other connection setting of this server needs its password sent with the change, because a saved password is only ever sent to the server it was saved for. Send the password together with the new settings.";
    private const string RemediationSentence = "This server has a remediation or hypothetical-index login stored. Change how it is reached on the service host, in the configuration file or with --add-server.";
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

    /// <summary>The edit function as the self-managed script carries it, cut out of the script's text: the 18-argument
    /// function and the two wrappers that follow it, the 17-argument one and the 15-argument one (#5452, #6254).</summary>
    private static string ByoEditFunctionSql()
    {
        var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = script.IndexOf("CREATE OR REPLACE FUNCTION config.edit_monitored_server(", StringComparison.Ordinal);
        Assert.True(start >= 0, "provision-roles.sql carries no edit function");
        const string functionEnd = "$fn$;";
        var end = script.IndexOf(functionEnd, start, StringComparison.Ordinal);
        Assert.True(end >= 0, "provision-roles.sql carries no end for the edit function");
        var seventeenEnd = script.IndexOf(functionEnd, end + functionEnd.Length, StringComparison.Ordinal);
        Assert.True(seventeenEnd >= 0, "provision-roles.sql carries no end for the edit function's 17-argument wrapper");
        var wrapperEnd = script.IndexOf(functionEnd, seventeenEnd + functionEnd.Length, StringComparison.Ordinal);
        Assert.True(wrapperEnd >= 0, "provision-roles.sql carries no end for the edit function's legacy wrapper");
        return script[start..(wrapperEnd + functionEnd.Length)];
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
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);

        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await TextAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" }.ConnectionString);
            await owner.OpenAsync(ct);
            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName), ProvisioningTestSecrets.Web);
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
                    $"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));
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
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);

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
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);
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
    /// trigger on it that would record who runs it. Called as that role, each function writes the store's table and the trigger
    /// never fires.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ATemporaryTableAndTriggerNamedLikeAStoreTable_IsNeverUsedByTheDefinerFunctions(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            var next = 7950;
            foreach (var (roleName, password) in new[] { ("web", ProvisioningTestSecrets.WebPassword), ("mcp", ProvisioningTestSecrets.McpPassword) })
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
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);
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

            /* The login name the remediation secret was stored for is part of how the row is reached: it does not change
               while the secret stays, and does on a row that holds none. */
            var before = await TextAsync(owner, "SELECT COALESCE(remediation_username, '(none)') FROM config_monitored_servers WHERE server_id = 8003", ct);
            await RefusedAsync(() => ExecAsync(mcp, "UPDATE config_monitored_servers SET remediation_username = 'another-login' WHERE server_id = 8003", ct), RemediationState, RemediationSentence);
            Assert.Equal(before, await TextAsync(owner, "SELECT COALESCE(remediation_username, '(none)') FROM config_monitored_servers WHERE server_id = 8003", ct));
            await ExecAsync(mcp, "UPDATE config_monitored_servers SET remediation_username = 'another-login' WHERE server_id = 8004", ct);
            Assert.Equal("another-login", await TextAsync(owner, "SELECT remediation_username FROM config_monitored_servers WHERE server_id = 8004", ct));
        });
    }

    private static async Task InsertHypotheticalServerAsync(
        NpgsqlConnection connection, int id, string host, string? hypotheticalUser, string? hypotheticalSecret, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, " +
            "hypothetical_index_username, hypothetical_index_encrypted_password, engine, port) " +
            "VALUES ($1, $2, $3, 'postgres', 'sql', 'monitor', $4, $5, $6, 'postgresql', 5432)", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        command.Parameters.AddWithValue("srv-" + id);
        command.Parameters.AddWithValue(host);
        command.Parameters.AddWithValue(ProtectedBlob);
        command.Parameters.AddWithValue(hypotheticalUser is null ? DBNull.Value : hypotheticalUser);
        command.Parameters.AddWithValue(hypotheticalSecret is null ? DBNull.Value : hypotheticalSecret);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The hypothetical-index login's password column (V203, #6170) is held to the rule the remediation one is: the viewer and mcp roles,
    /// and a direct admin write, never set an <c>env:</c> or <c>file:</c> reference in it, so a reference to a file or variable on the
    /// service host cannot be put there from those surfaces. The store's owner (the configuration-file seed) still can.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheViewerAndMcpRoles_NeverSetAReferenceInTheHypotheticalIndexPasswordColumn(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);
            await using var mcp = await ConnectAsync(ownerString, "mcp", ProvisioningTestSecrets.McpPassword, ct);
            await using var admin = await ConnectAsync(ownerString, "admin", ProvisioningTestSecrets.AdminPassword, ct);

            var id = 8100;
            foreach (var role in new[] { viewer, mcp, admin })
            {
                await RefusedAsync(() => InsertHypotheticalServerAsync(role, ++id, "h-env", "check_login", "env:SOME_SECRET", ct), ReferenceState, ReferenceSentence);
                await RefusedAsync(() => InsertHypotheticalServerAsync(role, ++id, "h-file", "check_login", "file:/run/secrets/check", ct), ReferenceState, ReferenceSentence);
            }

            Assert.Equal("0", await TextAsync(owner, "SELECT count(*) FROM config_monitored_servers", ct));

            /* The configuration-file seed is the store's owner, and sets what the file declares. */
            await InsertHypotheticalServerAsync(owner, 8190, "seeded", "check_login", "env:PM_INDEX_CHECK_PASSWORD", ct);
            Assert.Equal("env:PM_INDEX_CHECK_PASSWORD",
                await TextAsync(owner, "SELECT hypothetical_index_encrypted_password FROM config_monitored_servers WHERE server_id = 8190", ct));

            /* A direct write by a role that may update the table does not change a held reference to another, or set one where there is none. */
            await InsertHypotheticalServerAsync(owner, 8191, "no-pair", null, null, ct);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET hypothetical_index_encrypted_password = 'env:OTHER' WHERE server_id = 8190", ct), ReferenceState, ReferenceSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET hypothetical_index_encrypted_password = 'file:/etc/other' WHERE server_id = 8191", ct), ReferenceState, ReferenceSentence);
            Assert.Equal("env:PM_INDEX_CHECK_PASSWORD",
                await TextAsync(owner, "SELECT hypothetical_index_encrypted_password FROM config_monitored_servers WHERE server_id = 8190", ct));
        });
    }

    /// <summary>
    /// A change to how a server is reached on a row that holds a hypothetical-index login is refused by the edit function whatever else the
    /// call sends, and nothing is written, exactly as for a row with a remediation login (#6170); the same call on a row without one
    /// saves. The store's own trigger refuses the same change made directly, and a change to the login name the secret was stored for.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AChangeToHowAServerIsReached_OnARowWithAHypotheticalIndexLogin_IsRefusedAndWritesNothing(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await InsertHypotheticalServerAsync(owner, 8201, "hyp-viewer", "check_login", "env:PM_INDEX_CHECK_PASSWORD", ct);
            await InsertHypotheticalServerAsync(owner, 8202, "plain-viewer", null, null, ct);
            await InsertHypotheticalServerAsync(owner, 8203, "hyp-mcp", "check_login", "env:PM_INDEX_CHECK_PASSWORD", ct);
            await InsertHypotheticalServerAsync(owner, 8204, "plain-mcp", null, null, ct);
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);
            await using var mcp = await ConnectAsync(ownerString, "mcp", ProvisioningTestSecrets.McpPassword, ct);
            await using var admin = await ConnectAsync(ownerString, "admin", ProvisioningTestSecrets.AdminPassword, ct);

            foreach (var (role, heldId, plainId, host) in new[] { (viewer, 8201, 8202, "hyp-viewer"), (mcp, 8203, 8204, "hyp-mcp") })
            {
                Assert.Equal("remediation_kept", await EditOutcomeAsync(role, heldId, "'host','encrypted_password'", "moved-host", "typed-anything", owner, ct));
                Assert.Equal("remediation_kept", await EditOutcomeAsync(role, heldId, "'host'", "moved-host", null, owner, ct));
                Assert.Equal(host + "|env:PM_INDEX_CHECK_PASSWORD|check_login",
                    await TextAsync(owner, $"SELECT host || '|' || hypothetical_index_encrypted_password || '|' || hypothetical_index_username FROM config_monitored_servers WHERE server_id = {heldId}", ct));

                /* A change that is not to how the row is reached still saves, and leaves the pair alone. */
                Assert.Equal("saved", await EditOutcomeAsync(role, heldId, "'encrypted_password'", null, "a-new-protected-blob", owner, ct));
                Assert.Equal("env:PM_INDEX_CHECK_PASSWORD",
                    await TextAsync(owner, $"SELECT hypothetical_index_encrypted_password FROM config_monitored_servers WHERE server_id = {heldId}", ct));

                Assert.Equal("saved", await EditOutcomeAsync(role, plainId, "'host','encrypted_password'", "moved-host", "a-new-protected-blob", owner, ct));
                Assert.Equal("moved-host", await TextAsync(owner, $"SELECT host FROM config_monitored_servers WHERE server_id = {plainId}", ct));
            }

            await RefusedAsync(() => ExecAsync(mcp, "UPDATE config_monitored_servers SET host = 'moved-host' WHERE server_id = 8203", ct), RemediationState, RemediationSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET host = 'moved-host', encrypted_password = 'typed-anything' WHERE server_id = 8203", ct), RemediationState, RemediationSentence);
            await RefusedAsync(() => ExecAsync(admin, "UPDATE config_monitored_servers SET hypothetical_index_username = 'another-login' WHERE server_id = 8203", ct), RemediationState, RemediationSentence);
            Assert.Equal("hyp-mcp|check_login",
                await TextAsync(owner, "SELECT host || '|' || hypothetical_index_username FROM config_monitored_servers WHERE server_id = 8203", ct));
            await ExecAsync(admin, "UPDATE config_monitored_servers SET hypothetical_index_username = 'another-login' WHERE server_id = 8204", ct);
            Assert.Equal("another-login", await TextAsync(owner, "SELECT hypothetical_index_username FROM config_monitored_servers WHERE server_id = 8204", ct));
        });
    }

    /// <summary>
    /// A self-managed store whose script was not re-run keeps the edit function from before the hypothetical-index pair existed,
    /// which looks only at the remediation password: through it, a change of host on a row that holds a pair saves (#6170).
    /// The service's start replaces both overloads, and the same call is then refused and writes nothing.
    /// </summary>
    [Fact]
    public async Task AStoreHoldingTheOlderEditFunction_GetsItReplacedByTheServicesStart_AndAHostChangeOnAHeldPairIsRefused()
    {
        await RunScenarioAsync(selfManagedText: false, async (ownerString, owner, ct) =>
        {
            const string pairCheck = " OR COALESCE((to_jsonb(s) ->> 'hypothetical_index_encrypted_password'), '') <> ''";
            var older = DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config");
            Assert.Contains(pairCheck, older, StringComparison.Ordinal);
            await ExecAsync(owner, older.Replace(pairCheck, "", StringComparison.Ordinal), ct);

            await InsertHypotheticalServerAsync(owner, 8301, "held-before", "check_login", "env:PM_INDEX_CHECK_PASSWORD", ct);
            await InsertHypotheticalServerAsync(owner, 8302, "held-after", "check_login", "env:PM_INDEX_CHECK_PASSWORD", ct);
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);

            /* The older function lets the host change through on a held pair: the store really is in the state the start must fix. */
            Assert.Equal("saved", await EditOutcomeAsync(viewer, 8301, "'host','encrypted_password'", "moved-host", "typed-anything", owner, ct));
            Assert.Equal("moved-host", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 8301", ct));

            var log = new CapturingTestLogger();
            await using (var ownerSource = NpgsqlDataSource.Create(ownerString))
            {
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, log, ct), log.Joined);
            }

            Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Equal("remediation_kept", await EditOutcomeAsync(viewer, 8302, "'host','encrypted_password'", "moved-host", "typed-anything", owner, ct));
            Assert.Equal("held-after", await TextAsync(owner, "SELECT host FROM config_monitored_servers WHERE server_id = 8302", ct));
            Assert.Equal("env:PM_INDEX_CHECK_PASSWORD",
                await TextAsync(owner, "SELECT hypothetical_index_encrypted_password FROM config_monitored_servers WHERE server_id = 8302", ct));
        });
    }

    /// <summary>
    /// A self-managed store's rules are created on the service's start, as the role that owns its tables, when the script was
    /// not re-run after an upgrade; running it again changes nothing, and a login that may not create them gets one warning
    /// that names the script and fails nothing. Rules already in place from another role raise no warning; a function that
    /// differs raises one.
    /// </summary>
    [Fact]
    public async Task ASelfManagedStoreWithNoRules_GetsThemFromTheServicesStart_AndALoginThatMayNotCreateThemGetsOneWarning()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);
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

            /* Rules that are already in place, made by a role this login is not, are left alone and warn nothing: the login
               could not replace the function, and nothing needs replacing. A function that differs in its setting or in its
               body gets one warning each. */
            await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config TO {login}", ct);
            await using (var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString))
            await using (var restrictedSource = NpgsqlDataSource.Create(restricted))
            {
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, new CapturingTestLogger(), ct));
                Assert.Equal("1", await TextAsync(owner, triggerCount, ct));

                var inPlace = new CapturingTestLogger();
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(restrictedSource, inPlace, ct), inPlace.Joined);
                Assert.Equal(0, inPlace.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

                await ExecAsync(owner, "ALTER FUNCTION config.monitored_server_password_rules() SET search_path = public", ct);
                var otherSetting = new CapturingTestLogger();
                Assert.False(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(restrictedSource, otherSetting, ct), otherSetting.Joined);
                Assert.Equal(1, otherSetting.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, new CapturingTestLogger(), ct));
                await ExecAsync(owner, "CREATE OR REPLACE FUNCTION config.monitored_server_password_rules() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $x$ BEGIN RETURN NEW; END; $x$", ct);
                var otherBody = new CapturingTestLogger();
                Assert.False(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(restrictedSource, otherBody, ct), otherBody.Joined);
                Assert.Equal(1, otherBody.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
                Assert.Contains("provision-roles.sql", otherBody.Joined, StringComparison.Ordinal);

                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, new CapturingTestLogger(), ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup, $"DROP OWNED BY {login}; DROP ROLE IF EXISTS {login}", cleanupCt));
        }
    }

    /// <summary>
    /// #6569: a self-managed store upgraded by replacing the binaries keeps the journal's owner-row function from before the three rows of an
    /// agent action (action_called, action_done, action_failed) were the owner's alone, so the mcp and admin logins could still write them. The
    /// service's start replaces the function under the same lock as the password rules: an mcp-grant login's insert of action_called is accepted
    /// before and refused after, a second start changes nothing, and a login that is not the function's owner gets one warning that names the script.
    /// </summary>
    [Fact]
    public async Task AStoreHoldingTheOlderJournalFunction_GetsTheOwnerOnlyEventListFromTheServicesStart_AndALoginThatMayNotReplaceItGetsOneWarning()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var writer = "journal_writer_" + suffix;
        var restrictedLogin = "journal_probe_" + suffix;
        const string functionText = "SELECT prosrc FROM pg_proc WHERE oid = 'config.investigation_journal_owner_rows()'::regprocedure";
        const string functionVersion = "SELECT xmin::text FROM pg_proc WHERE oid = 'config.investigation_journal_owner_rows()'::regprocedure";
        const string actionCalled = "INSERT INTO config.investigation_journal (event, actor, reason) VALUES ('action_called', 'agent:probe', 'probe')";

        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);

            /* The older function: the same text with the three rows of an agent action out of the owner-only list. */
            var current = DarlingManagedRoles.BuildJournalEventRulesSql("config");
            var olderList = string.Join(", ", DarlingManagedRoles.JournalOwnerEventList.Split(", ")
                .Where(item => !AgentActionEvents.OwnerOnly.Contains(item.Trim('\''))));
            Assert.NotEqual(DarlingManagedRoles.JournalOwnerEventList, olderList);
            var older = current.Replace(DarlingManagedRoles.JournalOwnerEventList, olderList, StringComparison.Ordinal);
            Assert.NotEqual(current, older);
            await ExecAsync(owner, older, ct);

            await ExecAsync(owner,
                $"CREATE ROLE {writer} LOGIN PASSWORD 'journal-writer-1'; GRANT USAGE ON SCHEMA config TO {writer}; "
                + $"GRANT SELECT, INSERT ({DarlingManagedRoles.JournalWriterColumns}) ON config.investigation_journal TO {writer};", ct);
            await ExecAsync(owner, $"CREATE ROLE {restrictedLogin} LOGIN PASSWORD 'rules-probe-1'; GRANT USAGE ON SCHEMA config TO {restrictedLogin}", ct);
            await using var asWriter = new NpgsqlConnection(
                new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = writer, Password = "journal-writer-1" }.ConnectionString);
            await asWriter.OpenAsync(ct);

            /* The store really is in the state the start must fix: the older function lets the login write the owner's row. */
            await ExecAsync(asWriter, actionCalled, ct);
            Assert.Equal("1", await TextAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_called'", ct));

            var restricted = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = restrictedLogin, Password = "rules-probe-1" }.ConnectionString;
            var oldText = await TextAsync(owner, functionText, ct);
            var warned = new CapturingTestLogger();
            await using (var restrictedSource = NpgsqlDataSource.Create(restricted))
            {
                Assert.False(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(restrictedSource, warned, ct), warned.Joined);
            }

            Assert.Equal(1, warned.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Contains("provision-roles.sql", warned.Joined, StringComparison.Ordinal);
            Assert.Equal(oldText, await TextAsync(owner, functionText, ct));

            var log = new CapturingTestLogger();
            string version;
            await using (var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, log, ct), log.Joined);
                Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
                Assert.Contains(string.Join(", ", AgentActionEvents.OwnerOnly.Select(e => "'" + e + "'")), await TextAsync(owner, functionText, ct), StringComparison.Ordinal);
                version = (await TextAsync(owner, functionVersion, ct))!;

                /* A second start changes nothing: the function is not rewritten. */
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, log, ct), log.Joined);
                Assert.Equal(version, await TextAsync(owner, functionVersion, ct));
            }

            var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(asWriter, actionCalled, ct));
            Assert.Equal("42501", refused.SqlState);
            Assert.Equal("1", await TextAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_called'", ct));

            /* Rules already in place, made by a role the login is not, are left alone and warn nothing. */
            var inPlace = new CapturingTestLogger();
            await using (var restrictedSource = NpgsqlDataSource.Create(restricted))
            {
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(restrictedSource, inPlace, ct), inPlace.Joined);
            }

            Assert.Equal(0, inPlace.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup, $"DROP OWNED BY {writer}, {restrictedLogin}; DROP ROLE IF EXISTS {writer}; DROP ROLE IF EXISTS {restrictedLogin}", cleanupCt));
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
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);
        await using (var probe = new NpgsqlConnection(scratch.ConnectionString))
        {
            await probe.OpenAsync(ct);
            await PgMigrations.MigrateAsync(probe, ct);
            Assert.SkipWhen(
                await TextAsync(probe, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
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
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName), ProvisioningTestSecrets.Web);
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
                    $"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    /// <summary>The 18-argument edit (#6254): only the columns named in <paramref name="columns"/> are written, the replica among them.</summary>
    private static async Task<string?> ReplicaEditOutcomeAsync(
        NpgsqlConnection role, int id, string columns, string? replica, string? secret, NpgsqlConnection ownerConnection, CancellationToken ct)
    {
        var token = await TextAsync(ownerConnection, $"SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config_monitored_servers WHERE server_id = {id}", ct);
        string Lit(string? value) => value is null ? "NULL" : "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        return await TextAsync(role,
            $"SELECT outcome FROM config.edit_monitored_server({id}, '{token}'::timestamp, ARRAY[{columns}]::text[], NULL, NULL, NULL, NULL, NULL, NULL, NULL, {Lit(secret)}, NULL, NULL, NULL, NULL, NULL, NULL, {Lit(replica)})", ct);
    }

    /// <summary>
    /// #6254: the Query Store replica is a host the stored password is sent to, so an edit that changes only it never keeps the
    /// stored secret. Through the viewer's own role, on a row holding a sealed password: setting a replica, changing it and
    /// clearing it with no new password answer <c>password_needed</c> and write nothing; with a new password they save; the same
    /// value written again, and a change that names a replica it already has, move nothing and need no password.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnEditThatChangesOnlyTheQueryStoreReplica_NeedsANewPassword_ThroughTheViewerRole(bool selfManagedText)
    {
        await RunScenarioAsync(selfManagedText, async (ownerString, owner, ct) =>
        {
            await InsertServerAsync(owner, 8401, "replica-row", ProtectedBlob, null, ct);
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);
            const string Replica = "SELECT query_store_replica FROM config_monitored_servers WHERE server_id = 8401";
            const string Secret = "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 8401";

            /* Setting one: refused, nothing written. */
            Assert.Equal("password_needed", await ReplicaEditOutcomeAsync(viewer, 8401, "'query_store_replica'", "replica-a", null, owner, ct));
            Assert.Equal("", await TextAsync(owner, Replica, ct));
            Assert.Equal(ProtectedBlob, await TextAsync(owner, Secret, ct));

            /* With a new password it saves, and the new password is the one stored. */
            Assert.Equal("saved", await ReplicaEditOutcomeAsync(viewer, 8401, "'query_store_replica','encrypted_password'", " replica-a ", "a-new-blob", owner, ct));
            Assert.Equal("replica-a", await TextAsync(owner, Replica, ct));
            Assert.Equal("a-new-blob", await TextAsync(owner, Secret, ct));

            /* Changing it, and clearing it, are the same: a new password or nothing. */
            Assert.Equal("password_needed", await ReplicaEditOutcomeAsync(viewer, 8401, "'query_store_replica'", "replica-b", null, owner, ct));
            Assert.Equal("password_needed", await ReplicaEditOutcomeAsync(viewer, 8401, "'query_store_replica'", "  ", null, owner, ct));
            Assert.Equal("replica-a", await TextAsync(owner, Replica, ct));
            Assert.Equal("a-new-blob", await TextAsync(owner, Secret, ct));

            /* The value it already has, spaced differently, is no change. */
            Assert.Equal("saved", await ReplicaEditOutcomeAsync(viewer, 8401, "'query_store_replica'", "replica-a  ", null, owner, ct));
            Assert.Equal("a-new-blob", await TextAsync(owner, Secret, ct));

            /* A blank clears it, with a password. */
            Assert.Equal("saved", await ReplicaEditOutcomeAsync(viewer, 8401, "'query_store_replica','encrypted_password'", "", "cleared-blob", owner, ct));
            Assert.Equal("", await TextAsync(owner, Replica, ct));
            Assert.Equal("cleared-blob", await TextAsync(owner, Secret, ct));
        });
    }

    /// <summary>
    /// R9-18 (#6254): a store that holds an edit function from before the replica existed, one that does not count a replica
    /// change as a change of how the row connects, is given the current function by the service's start. The older function lets a
    /// replica-only edit through with the stored password kept (the state a store upgraded without the start would be in); once the
    /// start has run, the same edit answers <c>password_needed</c> and writes nothing. The start is the self-managed path's; a
    /// managed store's provisioning batch carries the same function text.
    /// </summary>
    [Fact]
    public async Task AStoreHoldingTheEditFunctionWithoutTheReplica_GetsTheCurrentOneFromTheServicesStart()
    {
        await RunScenarioAsync(selfManagedText: false, async (ownerString, owner, ct) =>
        {
            const string replicaTerm = "\n      OR v_query_store_replica IS DISTINCT FROM v_old_query_store_replica;";
            var current = DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config").Replace("\r\n", "\n", StringComparison.Ordinal);
            Assert.Contains(replicaTerm, current, StringComparison.Ordinal);
            await ExecAsync(owner, current.Replace(replicaTerm, ";", StringComparison.Ordinal), ct);

            await InsertServerAsync(owner, 8402, "replica-before", ProtectedBlob, null, ct);
            await InsertServerAsync(owner, 8403, "replica-after", ProtectedBlob, null, ct);
            await using var viewer = await ConnectAsync(ownerString, "web", ProvisioningTestSecrets.WebPassword, ct);

            Assert.Equal("saved", await ReplicaEditOutcomeAsync(viewer, 8402, "'query_store_replica'", "replica-a", null, owner, ct));
            Assert.Equal("replica-a", await TextAsync(owner, "SELECT query_store_replica FROM config_monitored_servers WHERE server_id = 8402", ct));
            Assert.Equal(ProtectedBlob, await TextAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 8402", ct));

            var log = new CapturingTestLogger();
            await using (var ownerSource = NpgsqlDataSource.Create(ownerString))
            {
                Assert.True(await DarlingManagedRoles.EnsureServerPasswordRulesAsync(ownerSource, log, ct), log.Joined);
            }

            Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Equal("password_needed", await ReplicaEditOutcomeAsync(viewer, 8403, "'query_store_replica'", "replica-a", null, owner, ct));
            Assert.Equal("", await TextAsync(owner, "SELECT query_store_replica FROM config_monitored_servers WHERE server_id = 8403", ct));
        });
    }
}
