/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The least-privilege role provisioning DDL (V8 security hardening, #1262). Ungated: the
/// generated statements' shape — DO-guarded idempotent CREATE ROLE, the ALTER ROLE password
/// re-assert, the collect/config grants, the config-only writes, the ALTER DEFAULT PRIVILEGES that
/// auto-grant new tables, the public hardening + the admin/viewer CONNECT re-grant — plus the
/// alphanumeric-password safety guard. The live round-trip (roles actually created, admin writes /
/// viewer denied, default-privileges proof) is in the gated <see cref="DarlingSecuritySplitLiveTests"/>.
/// </summary>
public sealed class DarlingManagedRolesTests
{
    /// <summary>The EXECUTE grant of the edit function (#5240), as the managed batch and <c>Darling/tools/provision-roles.sql</c>
    /// both carry it. The edit's one write goes through that function; viewer holds no UPDATE on the table.</summary>
    private static readonly string EditGrant =
        "GRANT EXECUTE ON FUNCTION config.edit_monitored_server(" + DarlingManagedRoles.EditMonitoredServerSignature + ") TO web, mcp;";

    /// <summary>The REVOKE (#5240): revoking the table privilege also revokes every column privilege, so each run leaves viewer
    /// with no UPDATE on the table and strips one granted by an earlier build or by hand.</summary>
    private const string EditRevoke = "REVOKE UPDATE ON config.config_monitored_servers FROM viewer, web;";

    /// <summary>
    /// The census of viewer grants on <c>config.config_monitored_servers</c> (#4843, #5240): every uncommented GRANT that names
    /// viewer on it, except the column-level SELECT carve, which is a read and not a write. The skip is exactly that shape, a
    /// SELECT list in parentheses straight onto ON: a bare <c>(?!SELECT )</c> skipped every statement that merely STARTED with
    /// SELECT, so a table-level UPDATE behind a column SELECT, or a table-level SELECT that undoes the
    /// <c>encrypted_password</c> carve, passed the census unseen.
    /// </summary>
    private static string[] GrantsOnMonitoredServers(string sql, string role) =>
        Regex.Matches(
                Regex.Replace(sql, @"(?m)^\s*--.*$", ""),
                @"GRANT (?!SELECT \([^)]*\) ON )[^;]*? ON config\.config_monitored_servers TO [^;]*\b" + Regex.Escape(role) + @"\b[^;]*;")
            .Select(m => m.Value).ToArray();

    private static string[] ViewerGrantsOnMonitoredServers(string sql) => GrantsOnMonitoredServers(sql, "viewer");

    /// <summary>
    /// The census must SEE each statement that is not a pure column-level SELECT. Each line is appended to the real batch as one
    /// more viewer grant, and the census must then list it. Against the old <c>(?!SELECT )</c> skip the first two cases were
    /// invisible: both start with SELECT.
    /// </summary>
    [Theory]
    [InlineData("GRANT SELECT (name), UPDATE ON config.config_monitored_servers TO viewer;")]
    [InlineData("GRANT SELECT ON config.config_monitored_servers TO viewer;")]
    [InlineData("GRANT SELECT, UPDATE (name) ON config.config_monitored_servers TO viewer;")]
    [InlineData("GRANT UPDATE ON config.config_monitored_servers TO viewer;")]
    [InlineData("GRANT DELETE ON config.config_monitored_servers TO admin, viewer;")]
    public void TheMonitoredServersCensus_ListsAnyViewerGrantThatIsNotThePureColumnSelectCarve(string extraGrant)
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var shipped = ViewerGrantsOnMonitoredServers(sql);

        Assert.Equal(shipped.Append(extraGrant).ToArray(), ViewerGrantsOnMonitoredServers(sql + "\n" + extraGrant));
    }

    /// <summary>The other half: the column-level SELECT carve itself stays out of the census, whatever columns it lists, so
    /// narrowing the census did not turn the carve (a read) into a counted write.</summary>
    [Fact]
    public void TheMonitoredServersCensus_SkipsThePureColumnLevelSelectCarve()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        Assert.Equal(
            ViewerGrantsOnMonitoredServers(sql),
            ViewerGrantsOnMonitoredServers(sql + "\nGRANT SELECT (server_id, name, host) ON config.config_monitored_servers TO viewer;"));
    }

    private static string NormalizeSql(string sql) =>
        Regex.Replace(Regex.Replace(sql, @"(?m)^\s*--.*$", ""), @"\s+", " ").Trim();

    /// <summary>
    /// A store that is not managed is told to RE-RUN <c>Darling/tools/provision-roles.sql</c>, so that script is what it gets,
    /// and nothing tied it to the managed batch's: the script carries the SAME edit function the batch does (text compared
    /// with comments and spacing set aside), the same EXECUTE grant, the REVOKE of any viewer UPDATE, and never a viewer
    /// grant on the table beyond INSERT.
    /// </summary>
    [Fact]
    public void TheByoScript_CarriesTheSameEditFunction_AsTheManagedBatch()
    {
        var byo = Regex.Replace(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"), @"(?m)^\s*--.*$", "");

        Assert.Contains(NormalizeSql(DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config")), NormalizeSql(byo), StringComparison.Ordinal);
        Assert.Contains(EditGrant, byo, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT ON config.config_monitored_servers TO web;", byo, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"GRANT[^;]*\bUPDATE\b[^;]*ON config\.config_monitored_servers TO [^;]*viewer", byo);
        Assert.Single(Regex.Matches(byo, @"REVOKE UPDATE ON config\.config_monitored_servers FROM [^;]*viewer[^;]*;"));
        Assert.Contains(EditRevoke, byo, StringComparison.Ordinal);
    }

    /// <summary>
    /// A bring-your-own store gets the web login from the script, so the script must give <c>web</c> every write and EXECUTE the
    /// managed batch gives it, give <c>viewer</c> none, and take from <c>viewer</c> each write an earlier version of the script
    /// granted it. The batch revokes in one statement per kind; the script revokes per table, so one table a store has not
    /// migrated to yet cannot skip the others. The record_custom_alert_resolution function is the one the script does not create.
    /// </summary>
    [Fact]
    public void TheByoScript_GivesTheWebRoleTheBatchsWrites_AndViewerNone()
    {
        var byo = NormalizeSql(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"));
        var batch = NormalizeSql(DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp));
        const string writePrivilege = @"GRANT (?:INSERT|UPDATE|DELETE|EXECUTE)";

        var webWrites = Regex.Matches(batch, writePrivilege + @"[^;]*? TO [^;]*\bweb\b[^;]*;")
            .Select(match => match.Value)
            .Where(grant => !grant.Contains("record_custom_alert_resolution", StringComparison.Ordinal))
            // The transaction advisory lock grant is the one statement the script cannot carry: the functions belong to a superuser, so security.md's doc block has it.
            .Where(grant => !grant.Contains("pg_catalog.pg_advisory_xact_lock", StringComparison.Ordinal) && !grant.Contains("pg_catalog.pg_try_advisory_xact_lock", StringComparison.Ordinal))
            .Distinct()
            .ToList();
        Assert.True(webWrites.Count >= 20, $"expected the batch to give web its writes, found {webWrites.Count} statements");
        foreach (var grant in webWrites)
        {
            Assert.Contains(grant, byo, StringComparison.Ordinal);
        }

        Assert.DoesNotMatch(writePrivilege + @"[^;]*? TO [^;]*\bviewer\b", batch);
        Assert.DoesNotMatch(writePrivilege + @"[^;]*? TO [^;]*\bviewer\b", byo);

        var revokedTables = Regex.Matches(batch, @"REVOKE (?:INSERT, UPDATE, DELETE|UPDATE) ON ((?:config\.\w+(?:, )?)+) FROM viewer;")
            .SelectMany(match => match.Groups[1].Value.Split(", "))
            .Distinct()
            .ToList();
        Assert.Contains("config.config_mute_rules", revokedTables);
        Assert.Contains("config.config_alert_log", revokedTables);
        foreach (var table in revokedTables)
        {
            Assert.Matches(@"REVOKE [A-Z, ]+ ON " + Regex.Escape(table) + " FROM viewer;", byo);
        }

        var revokedFunctions = Regex.Matches(batch, @"REVOKE EXECUTE ON FUNCTION [^;]*? FROM viewer;")
            .Select(match => match.Value)
            .Where(revoke => !revoke.Contains("record_custom_alert_resolution", StringComparison.Ordinal))
            .ToList();
        Assert.True(revokedFunctions.Count >= 8, $"expected the batch to revoke the moved functions from viewer, found {revokedFunctions.Count}");
        foreach (var revoke in revokedFunctions)
        {
            Assert.Contains(revoke, byo, StringComparison.Ordinal);
        }

        /* The role itself, its password placeholder and its reads. */
        Assert.Contains("CREATE ROLE web LOGIN NOSUPERUSER;", byo, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE web LOGIN PASSWORD 'CHANGE_ME_WEB_PASSWORD';", byo, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config TO admin, viewer, web;", byo, StringComparison.Ordinal);
        Assert.Contains("GRANT CONNECT ON DATABASE darling TO admin, viewer, web;", byo, StringComparison.Ordinal);
    }

    /// <summary>
    /// The store's password rules (a trigger on <c>config_monitored_servers</c> and its function) are provisioning, not a
    /// migration, so a managed store gets them from the batch on every start and a self-managed store from the script: the
    /// same text in both, created idempotently, and nothing in the migrations that would move the schema version.
    /// </summary>
    [Fact]
    public void TheByoScript_CarriesTheSamePasswordRules_AsTheManagedBatch()
    {
        var byo = Regex.Replace(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"), @"(?m)^\s*--.*$", "");
        var rules = DarlingManagedRoles.BuildServerPasswordRulesSql("config");
        var batch = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        Assert.Contains(NormalizeSql(rules), NormalizeSql(byo), StringComparison.Ordinal);
        Assert.Contains(rules, batch, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE FUNCTION config.monitored_server_password_rules()", rules, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE TRIGGER trg_monitored_server_password_rules", rules, StringComparison.Ordinal);
        Assert.Contains("BEFORE INSERT OR UPDATE ON config.config_monitored_servers", rules, StringComparison.Ordinal);
        Assert.DoesNotContain("monitored_server_password_rules", PgMigrations.Scripts.Aggregate("", (all, script) => all + script.Sql), StringComparison.Ordinal);

        /* The edit function refuses a reference in the same two spellings the service reads. */
        var function = DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config");
        Assert.Contains("left(p_secret, 4) = 'env:' OR left(p_secret, 5) = 'file:'", function, StringComparison.Ordinal);
        Assert.Contains("'reference_refused'", function, StringComparison.Ordinal);

        /* The rules read the catalog by its schema and keep the temporary schema last; the two definer functions pin it the
           same way. Each string is in the batch's text and, by the comparison above, in the script's. */
        Assert.Contains("FROM pg_catalog.pg_class AS c", rules, StringComparison.Ordinal);
        Assert.Contains("SET search_path = pg_catalog, pg_temp", rules, StringComparison.Ordinal);
        Assert.Contains("SET search_path = pg_catalog, pg_temp", function, StringComparison.Ordinal);
        Assert.Contains("SET search_path = pg_catalog, pg_temp", DarlingManagedRoles.BuildCustomAlertResolveFunctionSql("config"), StringComparison.Ordinal);
        Assert.DoesNotContain("FROM pg_class", rules, StringComparison.Ordinal);

        /* The edit function names its table by schema in both statements, so the pinned path needs nothing from config. */
        Assert.Contains("FROM config.config_monitored_servers AS s", function, StringComparison.Ordinal);
        Assert.Contains("UPDATE config.config_monitored_servers AS s", function, StringComparison.Ordinal);

        /* A change to how a row is reached while it holds a remediation login has its own outcome and its own state. */
        Assert.Contains("'remediation_kept'", function, StringComparison.Ordinal);
        Assert.Contains("USING ERRCODE = 'PW003'", rules, StringComparison.Ordinal);
        Assert.Contains("This server has a remediation or hypothetical-index login stored. Change how it is reached on the service host, in the configuration file or with --add-server.", rules, StringComparison.Ordinal);
    }

    /// <summary>
    /// The rules' three refusal sentences are literal in the managed batch and in the script, and each is the one in
    /// <see cref="ServerConnectionRule"/> that the web, MCP and viewer answers use: every message the rules raise is one of
    /// the three, and each of the three is raised, plus the AWS role rule's (#5452).
    /// </summary>
    [Fact]
    public void TheRulesRefusalSentences_AreTheSharedConstants_InTheManagedBatchAndTheScript()
    {
        var expected = new[]
        {
            ServerConnectionRule.PasswordNeededOnMoveText,
            ServerConnectionRule.ReferenceRefusedText,
            ServerConnectionRule.RemediationKeptText,
            AwsRoleSettings.RoleChangeNeedsExternalIdMessage,
        };
        var byo = Regex.Replace(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"), @"(?m)^\s*--.*$", "");
        var rules = DarlingManagedRoles.BuildServerPasswordRulesSql("config");
        var pattern = new Regex(@"RAISE EXCEPTION '%', '([^']*)'");

        foreach (var text in new[] { rules, byo })
        {
            var raised = pattern.Matches(text).Select(match => match.Groups[1].Value).ToList();
            Assert.All(raised, sentence => Assert.Contains(sentence, expected));
            Assert.Equal(expected.OrderBy(s => s, StringComparer.Ordinal), raised.Distinct().OrderBy(s => s, StringComparer.Ordinal));
        }
    }

    [Fact]
    public void BuildProvisioningSql_CreatesRolesIdempotently_LoginNoSuperuser()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* DO-guarded CREATE ROLE (no IF NOT EXISTS on CREATE ROLE) + a password re-assert (the default, every
           role), each carrying the role's SCRAM-SHA-256 verifier rather than the password (#3910). */
        Assert.Contains("FROM pg_roles WHERE rolname = 'admin'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_roles WHERE rolname = 'viewer'", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE ROLE admin LOGIN NOSUPERUSER PASSWORD '{ProvisioningTestSecrets.Admin}';", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE ROLE viewer LOGIN NOSUPERUSER PASSWORD '{ProvisioningTestSecrets.Viewer}';", sql, StringComparison.Ordinal);
        /* #3914 review F5, a deliberate change to this pin: the re-assert switches off every attribute that widens a
           role, not SUPERUSER alone, so an ADOPTED role (already marked) cannot keep CREATEROLE, CREATEDB,
           REPLICATION or BYPASSRLS that someone gave it since. The CREATE lines need nothing: those are CREATE ROLE's
           defaults. */
        Assert.Contains($"ALTER ROLE admin  LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS PASSWORD '{ProvisioningTestSecrets.Admin}';", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER ROLE viewer LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS PASSWORD '{ProvisioningTestSecrets.Viewer}';", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3914 review F5: every role membership in which admin, viewer or mcp is the MEMBER is revoked, every start —
    /// a pg_read_all_data would undo the secret-column carve, and membership in the owner is all of the owner's
    /// rights. Measured on the bundled PostgreSQL 18: a superuser's plain <c>REVOKE role FROM member</c> removes only
    /// a grant recorded as the bootstrap superuser's and leaves another grantor's with a WARNING, and RESTRICT fails
    /// on a membership the member used to grant onward — so the statement names the recorded grantor and cascades.
    /// Revoking ALL of them is right only because the batch grants none, which is pinned here too: every GRANT in it
    /// is a privilege ON something.
    /// </summary>
    [Fact]
    public void BuildProvisioningSql_RevokesEveryMembershipOfTheThreeRoles_AndGrantsNone()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        var section = sql[sql.IndexOf("-- 11. Role memberships (#3914)", StringComparison.Ordinal)..];
        Assert.Contains("FROM pg_catalog.pg_auth_members AS a", section, StringComparison.Ordinal);
        Assert.Contains("JOIN pg_catalog.pg_roles AS m ON m.oid = a.member", section, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor", section, StringComparison.Ordinal);
        Assert.Contains("WHERE m.rolname IN ('admin', 'viewer', 'web', 'mcp')", section, StringComparison.Ordinal);
        Assert.Contains("EXECUTE format('REVOKE %I FROM %I', membership.granted, membership.member)", section, StringComparison.Ordinal);
        Assert.Contains("format(' GRANTED BY %I', membership.grantor)", section, StringComparison.Ordinal);
        Assert.Contains("|| ' CASCADE';", section, StringComparison.Ordinal);

        /* It comes after every grant, so nothing later in the batch can hand a membership back. */
        Assert.DoesNotContain("GRANT ", section[section.IndexOf("DO $do$", StringComparison.Ordinal)..], StringComparison.Ordinal);

        /* And the batch grants no membership: each GRANT statement names what it is ON. */
        var statements = System.Text.RegularExpressions.Regex.Matches(sql, @"(?m)^\s*GRANT\s[^;]*;");
        Assert.NotEmpty(statements);
        Assert.All(statements, statement => Assert.Matches(@"\sON\s", statement.Value));
    }

    [Fact]
    public void BuildProvisioningSql_StampsMarker_AndFailsLoudOnUnmarkedCollision()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        Assert.Equal("darling-managed", DarlingManagedRoles.RoleMarker);

        /* Fresh roles are STAMPED with the marker comment. */
        Assert.Contains("COMMENT ON ROLE admin IS 'darling-managed';", sql, StringComparison.Ordinal);
        Assert.Contains("COMMENT ON ROLE viewer IS 'darling-managed';", sql, StringComparison.Ordinal);

        /* An existing same-named role is trusted only if it carries the marker (read via
           shobj_description); otherwise provisioning RAISEs rather than repurposing it. */
        Assert.Contains("shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'admin'), 'pg_authid') IS DISTINCT FROM 'darling-managed'", sql, StringComparison.Ordinal);
        Assert.Contains("shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'viewer'), 'pg_authid') IS DISTINCT FROM 'darling-managed'", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE EXCEPTION 'Role \"admin\" already exists and was not created by Darling", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE EXCEPTION 'Role \"viewer\" already exists and was not created by Darling", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_GrantsReadBothSchemas_WritesConfigForAdminOnly()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        Assert.Contains("GRANT USAGE ON SCHEMA collect, config TO admin, viewer, web;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer, web;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer, web;", sql, StringComparison.Ordinal);

        /* config writes are admin-only — viewer never gets INSERT/UPDATE/DELETE. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin, viewer", sql, StringComparison.Ordinal);

        /* No BLANKET (schema-wide) collect write to anyone but the owner. mcp gets a single-table INSERT on
           collect.analysis_findings (section 6, asserted separately), never a schema-wide collect write. */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA collect", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_GrantsViewerTheNarrowCustomViewsWrite_WithoutWideningTheSchemaGrant()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* #1563: the first viewer write — an EXPLICIT single-table grant on config.custom_views (the web
           dashboard's user-authored view store; editing is loopback-gated server-side). The viewer write set
           has since grown by the same single-table shape (custom_alert_rules #3285, database_state_expected
           #1986, config_mute_rules #3450 below) and by nothing wider. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.custom_views TO web;", sql, StringComparison.Ordinal);

        /* #3450: the web mute-rule endpoints run as viewer, so it holds the SAME single-table mute write the
           mcp role has — plus the COLUMN-level config_service beacon grant the SECURITY-INVOKER bump trigger
           needs (trg_bump_mute_rules UPDATEs config_service.config_version AS the writing role), and never a
           table-wide config_service UPDATE that would reach paused / capture_plans / mcp_port. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO web;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT UPDATE (config_version, updated_at) ON config.config_service TO web;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT UPDATE ON config.config_service TO web", sql, StringComparison.Ordinal);

        /* #5085: the web server-tag endpoints run as viewer: the same two single-table writes mcp holds, and no
           default-privileges shortcut. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.server_tags TO web;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.server_tag_map TO web;", sql, StringComparison.Ordinal);

        /* #4843: the web add-server route runs the add_servers core as viewer: INSERT on config_monitored_servers and nothing
           else on it: no UPDATE (#5240: the edit's write is the config.edit_monitored_server function, granted EXECUTE below),
           no DELETE. The census is the whole set of grants web holds on the table, and viewer holds none. The credential column stays SELECT-carved. */
        Assert.Contains("GRANT INSERT ON config.config_monitored_servers TO web;", sql, StringComparison.Ordinal);
        Assert.Empty(ViewerGrantsOnMonitoredServers(sql));
        Assert.Equal(new[] { "GRANT INSERT ON config.config_monitored_servers TO web;" }, GrantsOnMonitoredServers(sql, "web"));

        /* The REVOKE takes away any UPDATE an earlier build granted viewer; it is not a GRANT, so the census never sees it. */
        Assert.Single(Regex.Matches(Regex.Replace(sql, @"(?m)^\s*--.*$", ""), @"REVOKE UPDATE ON config\.config_monitored_servers FROM [^;]*viewer[^;]*;"));
        Assert.Contains(EditRevoke, sql, StringComparison.Ordinal);

        /* The edit function (#5240): created, REVOKEd from PUBLIC, then EXECUTE to viewer and mcp only. */
        Assert.Contains("CREATE OR REPLACE FUNCTION config.edit_monitored_server(", sql, StringComparison.Ordinal);
        Assert.Contains(DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config"), sql, StringComparison.Ordinal);
        Assert.Contains(EditGrant, sql, StringComparison.Ordinal);
        var functionRevokeAt = sql.IndexOf("REVOKE ALL ON FUNCTION config.edit_monitored_server(", StringComparison.Ordinal);
        Assert.True(functionRevokeAt >= 0 && functionRevokeAt < sql.IndexOf(EditGrant, StringComparison.Ordinal), "the PUBLIC revoke must precede the EXECUTE grant.");

        /* It must NOT widen the schema-wide config write to viewer (that grant stays admin-only, pinned here). */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin, viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO viewer", sql, StringComparison.Ordinal);

        /* NO ALTER DEFAULT PRIVILEGES grants viewer a write — ADP has no per-table form, so an ADP write would
           broaden viewer to ALL of config. The default-privileges write grant names admin only. */
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON TABLES TO admin, viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON TABLES TO viewer", sql, StringComparison.Ordinal);

        /* custom_views carries no secret columns, so it is NOT part of the viewer secret-column carve. */
        Assert.DoesNotContain("REVOKE SELECT ON config.custom_views", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_DefaultPrivileges_AutoGrantNewTables()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* New collector tables (created bare into collect via search_path) auto-inherit SELECT. */
        Assert.Contains("ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON TABLES TO admin, viewer, web;", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA config", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON TABLES TO admin;", sql, StringComparison.Ordinal);

        /* Fail-closed: a future serial/identity config column needs sequence USAGE for admin's INSERT. */
        Assert.Contains("GRANT USAGE, SELECT ON SEQUENCES TO admin;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_CarvesViewerSecretColumns_FailClosed()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* The blanket config SELECT to viewer is still present (covers the non-secret config tables), but
           the three credential-bearing tables are then carved to column-level for viewer. */
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer, web;", sql, StringComparison.Ordinal);

        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
        {
            /* viewer's table-wide SELECT is dropped, then re-granted on ONLY the non-secret columns. */
            Assert.Contains($"REVOKE SELECT ON config.{acl.Table} FROM viewer;", sql, StringComparison.Ordinal);
            Assert.Contains($"GRANT SELECT ({string.Join(", ", acl.NonSecretColumns)}) ON config.{acl.Table} TO viewer;",
                sql, StringComparison.Ordinal);

            /* The carve is viewer-only — admin is never column-restricted (it writes these tables). */
            Assert.DoesNotContain($"REVOKE SELECT ON config.{acl.Table} FROM admin", sql, StringComparison.Ordinal);
        }

        /* No secret column name appears anywhere in a GRANT ... TO viewer (belt over the per-table check). */
        var viewerGrantLines = sql.Split('\n')
            .Where(line => line.Contains("TO viewer", StringComparison.Ordinal) && line.Contains("GRANT SELECT (", StringComparison.Ordinal));
        foreach (var secret in DarlingManagedRoles.ViewerRestrictedConfigTables.SelectMany(a => a.SecretColumns))
        {
            foreach (var line in viewerGrantLines)
            {
                /* A whole name, not a substring: aws_external_id_set (readable) contains aws_external_id (not). */
                Assert.DoesNotMatch($"(?<![A-Za-z0-9_]){Regex.Escape(secret)}(?![A-Za-z0-9_])", line);
            }
        }
    }

    [Fact]
    public void ViewerRestrictedConfigTables_SecretAndNonSecretColumns_AreDisjointAndNonEmpty()
    {
        var expectedSecrets = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            /* V113 (#2138 phase 1): the remediation credential's blob is the same kind of thing as the
               monitoring one beside it — a DPAPI secret — and it authenticates a WRITE, so if anything on
               this table is secret it is. */
            ["config_monitored_servers"] = new[] { "encrypted_password", "remediation_encrypted_password", "aws_external_id", "hypothetical_index_encrypted_password" },
            ["config_command"] = new[] { "args_json" },
            /* generic_url is a bearer secret like the sibling webhook URLs, and generic_headers holds the
               Authorization token itself (#1506 / V26). pagerduty_routing_key is the Events API v2 integration
               key — a bearer secret like the webhook URLs (V40). */
            ["config_notification"] = new[]
            {
                "smtp_encrypted_password", "smtp_username", "teams_url", "slack_url",
                "generic_url", "generic_headers", "pagerduty_routing_key", "signing_key", "previous_signing_key",
            },
            /* V131 (#3598): the sparse routes table mirrors the parent's destination columns under the same
               names, so the four webhook-class destinations are the same bearer secrets they are one table
               up; smtp_recipients and the GENERATED configured_channels presence column are not. */
            ["config_notification_routes"] = new[] { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key", "signing_key", "previous_signing_key" },
        };

        /* Exactly the four known secret-bearing tables, with the expected secret columns. */
        Assert.Equal(
            expectedSecrets.Keys.OrderBy(k => k, StringComparer.Ordinal),
            DarlingManagedRoles.ViewerRestrictedConfigTables.Select(a => a.Table).OrderBy(k => k, StringComparer.Ordinal));

        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
        {
            Assert.NotEmpty(acl.NonSecretColumns);
            Assert.NotEmpty(acl.SecretColumns);

            /* A column is never in both sets — a granted column can't also be a denied one. */
            Assert.Empty(acl.NonSecretColumns.Intersect(acl.SecretColumns, StringComparer.Ordinal));

            Assert.Equal(
                expectedSecrets[acl.Table].OrderBy(c => c, StringComparer.Ordinal),
                acl.SecretColumns.OrderBy(c => c, StringComparer.Ordinal));
        }
    }

    [Fact]
    public void BuildViewerColumnAclSql_UsesTheGivenSchemaAndRole()
    {
        /* The same generator the live test applies to its throwaway roles — pin that it honors the passed
           schema + role names (so the test and the real provisioning can never drift). */
        var sql = DarlingManagedRoles.BuildViewerColumnAclSql("config", "sec_viewer_test");

        Assert.Contains("REVOKE SELECT ON config.config_monitored_servers FROM sec_viewer_test;", sql, StringComparison.Ordinal);
        Assert.Contains("ON config.config_monitored_servers TO sec_viewer_test;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("encrypted_password", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("args_json", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("teams_url", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_HardensPublic_ButKeepsAdminViewerConnect()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        Assert.Contains("REVOKE CREATE ON SCHEMA public FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains("REVOKE ALL ON DATABASE darling FROM PUBLIC;", sql, StringComparison.Ordinal);

        /* REVOKE ALL drops PUBLIC's implicit CONNECT, so admin/viewer must be re-granted it, or the
           Viewer could no longer connect — the correctness fix over the design's literal DDL. */
        Assert.Contains("GRANT CONNECT ON DATABASE darling TO admin, viewer, web;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_CreatedAndGrantedReadPlusTwoInserts_NoWriteAdp()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* The mcp role is created + stamped + re-asserted, guarded exactly like admin/viewer. */
        Assert.Contains("FROM pg_roles WHERE rolname = 'mcp'", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE ROLE mcp LOGIN NOSUPERUSER PASSWORD '{ProvisioningTestSecrets.Mcp}';", sql, StringComparison.Ordinal);
        Assert.Contains("COMMENT ON ROLE mcp IS 'darling-managed';", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER ROLE mcp    LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS PASSWORD '{ProvisioningTestSecrets.Mcp}';", sql, StringComparison.Ordinal);
        Assert.Contains("RAISE EXCEPTION 'Role \"mcp\" already exists and was not created by Darling", sql, StringComparison.Ordinal);

        /* viewer's read surface, granted as SEPARATE 'TO mcp' statements — NOT appended to the pinned
           'TO admin, viewer' grant lines (which stay verbatim). */
        Assert.Contains("GRANT USAGE ON SCHEMA collect, config TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA collect TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA config  TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT CONNECT ON DATABASE darling TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("TO admin, viewer, mcp", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer, web;", sql, StringComparison.Ordinal);

        /* Exactly the two narrow ANALYSIS writes (Round 4 #1: INSERT-only). The mcp role's ONE other write —
           INSERT/UPDATE/DELETE on config.custom_views (#1599, the custom-view tools) — is a single-table grant
           pinned separately by BuildProvisioningSql_McpRole_GrantsCustomViewsWrite_LikeViewer; it does not widen
           either schema-wide claim below. */
        Assert.Contains("GRANT INSERT ON collect.analysis_findings TO mcp;", sql, StringComparison.Ordinal);
        /* analysis_muted: INSERT for mute_analysis_finding and DELETE for unmute_analysis_finding (#6383). */
        Assert.Contains("GRANT INSERT, DELETE ON config.analysis_muted TO mcp;", sql, StringComparison.Ordinal);

        /* No schema-wide write for mcp. */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA collect TO mcp", sql, StringComparison.Ordinal);

        /* No default WRITE names mcp — ADP has no per-table form, so an ADP INSERT would broaden mcp to ALL of
           collect; the narrow grants are explicit single-table statements. The ONE default mcp does get (#3914)
           is SELECT on collect: the continuous aggregates are created after this batch in the same start, and
           without it every one was unreadable to the MCP tools until the next restart. Collect holds no secrets;
           config gets no mcp default, because its secret columns are carved table by table. */
        Assert.Single(Regex.Matches(sql, "ON TABLES TO mcp"));
        Assert.Single(Regex.Matches(sql, @"ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect\s+GRANT SELECT ON TABLES TO mcp;"));
        Assert.DoesNotMatch(@"IN SCHEMA config\s+GRANT [A-Z, ]+ ON TABLES TO mcp", sql);
        Assert.DoesNotMatch(@"(?:INSERT|UPDATE|DELETE)[A-Z, ]* ON TABLES TO mcp", sql);
        Assert.DoesNotContain("ON SEQUENCES TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #5633 item 13, as ruled: <c>collect.analysis_collection_caveats</c> is the SCHEDULED pass's state, written
    /// only by the worker as the service role. The mcp role holds no write on it: no statement in the managed
    /// batch or in the BYO <c>provision-roles.sql</c> names the table at all, and no <c>GRANT ALL</c> reaches mcp
    /// (which would carry INSERT/UPDATE/DELETE/TRIGGER/REFERENCES onto it, and onto every other table the grant
    /// names). The live census (McpCollectionCaveatGrantLiveTests) proves the same on a real role; this one runs
    /// without a database.
    /// </summary>
    [Fact]
    public void McpRole_HoldsNoWriteOnTheCaveatTable_InTheManagedBatchOrTheByoScript()
    {
        var managed = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var byo = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");

        foreach (var (name, script) in new[] { ("managed", managed), ("byo", byo) })
        {
            var code = Regex.Replace(script, @"(?m)--.*$", "");
            Assert.True(!code.Contains("analysis_collection_caveats", StringComparison.Ordinal), name + ": a statement names the caveat table");
            Assert.True(!Regex.IsMatch(code, GrantAllToMcp), name + ": GRANT ALL reaches mcp");
            Assert.True(!Regex.IsMatch(code, McpWriteOnWholeCollectSchema), name + ": a collect-wide non-SELECT grant reaches mcp");
        }

        /* The patterns catch the shapes the earlier single-keyword regex missed. */
        Assert.Matches(GrantAllToMcp, "GRANT ALL ON collect.analysis_collection_caveats TO mcp;");
        Assert.Matches(GrantAllToMcp, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA collect TO admin, mcp;");
        Assert.Matches(GrantAllToMcp, "ALTER DEFAULT PRIVILEGES IN SCHEMA collect GRANT ALL ON TABLES TO mcp;");
        Assert.Matches(McpWriteOnWholeCollectSchema, "GRANT TRIGGER ON ALL TABLES IN SCHEMA collect TO mcp;");
        Assert.DoesNotMatch(McpWriteOnWholeCollectSchema, "GRANT SELECT ON ALL TABLES IN SCHEMA collect TO mcp;");
    }

    private const string GrantAllToMcp = @"(?is)\bGRANT\s+ALL\b[^;]*\bTO\b[^;]*\bmcp\b";
    private const string McpWriteOnWholeCollectSchema = @"(?is)\bGRANT\s+(?!SELECT\s+ON\b)[^;]*\bON\s+ALL\s+TABLES\s+IN\s+SCHEMA\s+collect\b[^;]*\bTO\b[^;]*\bmcp\b";

    [Fact]
    public void BuildProvisioningSql_McpRole_GrantsCustomViewsWrite_LikeViewer()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* #1599: the mcp role gets the SAME single-table write on config.custom_views the viewer role has, so the
           MCP custom-view tools can create/update/delete views. An EXPLICIT single-table statement (its own
           'TO mcp' line, separate from viewer's), mirroring the mcp analysis-write grants. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.custom_views TO mcp;", sql, StringComparison.Ordinal);

        /* Both write identities are pinned side by side and neither is folded into the other's line. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.custom_views TO web;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config.custom_views TO web, mcp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config.custom_views TO mcp, viewer", sql, StringComparison.Ordinal);

        /* The write stays NARROW: it must NOT widen mcp to a schema-wide config write, and NO ALTER DEFAULT
           PRIVILEGES names mcp (that would broaden it to all of config). */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }

        /* custom_views carries no secret columns, so it is NOT part of the mcp secret-column carve. */
        Assert.DoesNotContain("REVOKE SELECT ON config.custom_views", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_InvestigationTables_RevokeEverything_ThenGrantEachWriterOnlyWhatItWrites()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var flat = Regex.Replace(sql, @"\s+", " ");

        /* #5632: the blanket config writes gave admin INSERT/UPDATE/DELETE on all six tables. The REVOKE ALL comes after
           the last blanket grant and before the password key revoke, which stays the last grant-side statement. */
        const string Six = "config.investigation_requests, config.investigation_request_alerts, config.investigation_reports, config.investigation_journal, config.investigation_notes, config.agent_environment_profiles";
        var revoke = flat.IndexOf($"REVOKE ALL ON {Six} FROM PUBLIC, admin, viewer, web, mcp CASCADE;", StringComparison.Ordinal);
        Assert.True(revoke > flat.LastIndexOf("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;", StringComparison.Ordinal));
        Assert.True(revoke < flat.IndexOf("REVOKE ALL ON config.password_key, ", StringComparison.Ordinal));
        Assert.Contains($"GRANT SELECT ON {Six} TO admin, viewer, web;", flat, StringComparison.Ordinal);
        Assert.Contains($"GRANT SELECT ON {Six} TO mcp;", flat, StringComparison.Ordinal);

        /* mcp: the claim and report columns, and a note by the columns an agent's proposal sets. Since #6400 mcp also holds the person-only
           grants below (state on a note, the close columns); the tool policy (NeverAgentTools) keeps an agent from those tools, not this role. */
        Assert.Contains("GRANT UPDATE (state, claimed_by, claim_id, claimed_at, lease_until, claim_count, brief_format, brief, reported_at) ON config.investigation_requests TO mcp;", flat, StringComparison.Ordinal);
        /* #6008: follow_up_at (V193) has its own grant, behind a column check, so a store below V193 commits the rest. */
        Assert.Contains("AND attname = 'follow_up_at' AND NOT attisdropped) THEN EXECUTE $v193u$ GRANT UPDATE (follow_up_at) ON config.investigation_requests TO mcp; $v193u$;", flat, StringComparison.Ordinal);
        /* #6009: follow_up_condition, the same way. */
        Assert.Contains("AND attname = 'follow_up_condition' AND NOT attisdropped) THEN EXECUTE $v193c$ GRANT UPDATE (follow_up_condition) ON config.investigation_requests TO mcp; $v193c$;", flat, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT ON config.investigation_reports TO mcp;", flat, StringComparison.Ordinal);
        /* #6003 review round 2: the journal grant names its columns and leaves out journal_id and event_id. */
        Assert.Contains($"GRANT INSERT ({DarlingManagedRoles.JournalWriterColumns}) ON config.investigation_journal TO mcp;", flat, StringComparison.Ordinal);
        Assert.Contains($"GRANT INSERT ({DarlingManagedRoles.JournalWriterColumns}) ON config.investigation_journal TO admin;", flat, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT INSERT ON config.investigation_journal", flat, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT (server_id, story_path_hash, dedup_key, metric_name, note_text, author, source_request_id) ON config.investigation_notes TO mcp;", flat, StringComparison.Ordinal);

        /* admin (the WPF viewer): close a request, write the profiles, write and decide notes, journal it. */
        Assert.Contains("GRANT UPDATE (state, closed_at, closed_by, closed_reason) ON config.investigation_requests TO admin;", flat, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, DELETE ON config.investigation_notes TO admin;", flat, StringComparison.Ordinal);
        Assert.Contains("GRANT UPDATE (state, decided_at, decided_by, expires_at) ON config.investigation_notes TO admin;", flat, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.agent_environment_profiles TO admin;", flat, StringComparison.Ordinal);

        /* #5918: admin (the Viewer) opens a practice request and copies its case's alert rows; nobody else links alerts but the owner,
           and viewer writes nothing here. */
        Assert.Contains("GRANT INSERT ON config.investigation_request_alerts TO admin;", flat, StringComparison.Ordinal);
        Assert.Contains("ON config.investigation_requests TO admin;", flat, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"GRANT [A-Z ,()a-z_]*(INSERT|UPDATE|DELETE)[^;]* ON config\.investigation_request_alerts TO (?!admin;|mcp;)", flat);

        /* #6400: mcp (start_practice_request) copies a case's alert rows too, by the ten columns the copy sets, and never updates or deletes a link. */
        Assert.Contains("GRANT INSERT (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json, severity) ON config.investigation_request_alerts TO mcp;", flat, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"GRANT [A-Z ,()a-z_]*(UPDATE|DELETE)[^;]* ON config\.investigation_request_alerts TO mcp;", flat);
        Assert.DoesNotMatch(@"GRANT (INSERT|UPDATE|DELETE)[^;]* ON config\.(investigation|agent_environment)[a-z_]* TO [a-z_, ]*viewer", flat);
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_GrantsAlertTuningWrites_NarrowlyWithBeaconColumns()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* The MCP alert-tuning write tools: full CRUD on the mute rules, and UPDATE-only on the SINGLETON
           alert-settings row (never INSERT/DELETE — the row is a fixed singleton the service seeds). */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT UPDATE ON config.config_alert_settings TO mcp;", sql, StringComparison.Ordinal);
        /* The fleet server-tag write tools (#5085): the admin gate is exactly these two single-table grants. */
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.server_tags TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.server_tag_map TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON config.config_alert_settings", sql, StringComparison.Ordinal);

        /* The beacon caveat: a config_alert_settings write fires the SECURITY INVOKER bump trigger, which UPDATEs
           config_service — so mcp needs a COLUMN-level UPDATE on JUST the two beacon columns (never paused /
           mcp_enabled / mcp_port, and never a table-wide config_service UPDATE). */
        Assert.Contains("GRANT UPDATE (config_version, updated_at) ON config.config_service TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT UPDATE ON config.config_service TO mcp", sql, StringComparison.Ordinal);

        /* Still NARROW: no schema-wide config write for mcp, and NO ALTER DEFAULT PRIVILEGES names mcp (either
           would broaden it to all of config). */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_GrantsMonitoredServersWrite_Narrowly()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* The MCP server-onboarding write tools (add_servers / remove_server): full CRUD on the single
           config_monitored_servers table — an EXPLICIT single-table statement, its own 'TO mcp' line. */
        Assert.Contains("GRANT INSERT, DELETE ON config.config_monitored_servers TO mcp;", sql, StringComparison.Ordinal);

        /* The UPDATE is column by column (#5452): every column but the AWS role and external ID, which change through the edit function. */
        Assert.Contains($"GRANT UPDATE ({DarlingManagedRoles.McpMonitoredServerUpdateColumns})", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT INSERT, UPDATE, DELETE ON config.config_monitored_servers", sql, StringComparison.Ordinal);

        /* Still NARROW: no schema-wide config write for mcp, and NO ALTER DEFAULT PRIVILEGES names mcp (either
           would broaden it to all of config). No NEW config_service grant — section 8's beacon column-grant
           already covers the monitored-servers bump trigger. */
        Assert.DoesNotContain("INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mcp", sql, StringComparison.Ordinal);
        foreach (var adpLine in sql.Split('\n').Where(l => l.Contains("ALTER DEFAULT PRIVILEGES", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("mcp", adpLine, StringComparison.Ordinal);
        }

        /* The credential column stays SELECT-carved from mcp (section 6) — mcp WRITEs a password blob but never
           READs one back — so config_monitored_servers still appears in the mcp REVOKE/GRANT-column carve. */
        Assert.Contains("REVOKE SELECT ON config.config_monitored_servers FROM mcp;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildProvisioningSql_McpRole_CarvesSecretColumns_LikeViewer()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* The mcp role gets the SAME fail-closed secret-column carve as viewer (reusing
           BuildViewerColumnAclSql(config, "mcp")), so a network-reachable token can never read the
           credential columns. */
        foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
        {
            Assert.Contains($"REVOKE SELECT ON config.{acl.Table} FROM mcp;", sql, StringComparison.Ordinal);
            var columns = DarlingManagedRoles.McpWithheldColumns.TryGetValue(acl.Table, out var withheld)
                ? acl.NonSecretColumns.Where(c => !withheld.Contains(c, StringComparer.Ordinal))
                : acl.NonSecretColumns;
            Assert.Contains($"GRANT SELECT ({string.Join(", ", columns)}) ON config.{acl.Table} TO mcp;",
                sql, StringComparison.Ordinal);
        }

        /* #6230: no mcp column grant names result_json, the table is revoked from mcp before the re-grant, and the read function is
           granted to mcp alone. */
        Assert.DoesNotContain(sql.Split('\n').Where(l => l.Contains("TO mcp", StringComparison.Ordinal) && l.Contains("GRANT SELECT (", StringComparison.Ordinal)),
            l => l.Contains("result_json", StringComparison.Ordinal));
        Assert.Contains("GRANT EXECUTE ON FUNCTION config.read_mcp_command_result(bigint) TO mcp;", sql, StringComparison.Ordinal);
        Assert.Contains("REVOKE ALL ON FUNCTION config.read_mcp_command_result(bigint) FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("read_mcp_command_result(bigint) TO viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("read_mcp_command_result(bigint) TO admin", sql, StringComparison.Ordinal);

        /* No secret column name appears in any GRANT SELECT (...) TO mcp line (belt over the per-table check). */
        var mcpGrantLines = sql.Split('\n')
            .Where(line => line.Contains("TO mcp", StringComparison.Ordinal) && line.Contains("GRANT SELECT (", StringComparison.Ordinal));
        foreach (var secret in DarlingManagedRoles.ViewerRestrictedConfigTables.SelectMany(a => a.SecretColumns))
        {
            foreach (var line in mcpGrantLines)
            {
                /* A whole name, not a substring: aws_external_id_set (readable) contains aws_external_id (not). */
                Assert.DoesNotMatch($"(?<![A-Za-z0-9_]){Regex.Escape(secret)}(?![A-Za-z0-9_])", line);
            }
        }
    }

    /// <summary>
    /// #3910: the batch refuses anything that is not a SCRAM-SHA-256 verifier, a plain password first of all.
    /// The batch is recorded wherever statement text is (an ERROR's STATEMENT line in the store's own log,
    /// which the store-log sweep keeps and the viewer role reads), so the builder is where "never the password"
    /// is enforced rather than hoped for. A shape that could close the literal is refused with the rest.
    /// </summary>
    [Theory]
    [InlineData("AdminPassword01")]
    [InlineData("with space")]
    [InlineData("quote'here")]
    [InlineData("")]
    [InlineData("md5aaaabbbbccccddddeeeeffff00001111")]
    [InlineData("SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk'--:c2VydmVy")]
    [InlineData("SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk")]
    public void BuildProvisioningSql_RefusesAnythingButAScramVerifier(string bad)
    {
        Assert.Throws<ArgumentException>(() => DarlingManagedRoles.BuildProvisioningSql(bad, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp));
        Assert.Throws<ArgumentException>(() => DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, bad, ProvisioningTestSecrets.Mcp));
        Assert.Throws<ArgumentException>(() => DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, bad));
    }

    /// <summary>The real path's shape: verifiers of three generated passwords go in, and no password comes out
    /// anywhere in the batch.</summary>
    [Fact]
    public void BuildProvisioningSql_CarriesVerifiersOfTheGeneratedPasswords_AndNeverThePasswords()
    {
        var admin = DarlingManagedPostgres.GeneratePassword();
        var viewer = DarlingManagedPostgres.GeneratePassword();
        var mcp = DarlingManagedPostgres.GeneratePassword();

        var sql = DarlingManagedRoles.BuildProvisioningSql(
            ScramSha256Verifier.Create(admin), ScramSha256Verifier.Create(viewer), ScramSha256Verifier.Create(mcp));

        Assert.DoesNotContain(admin, sql, StringComparison.Ordinal);
        Assert.DoesNotContain(viewer, sql, StringComparison.Ordinal);
        Assert.DoesNotContain(mcp, sql, StringComparison.Ordinal);
        /* Four roles, each a CREATE and an ALTER with a verifier (the web role's is a throwaway one when none is passed). */
        Assert.Equal(8, System.Text.RegularExpressions.Regex.Matches(sql, @"PASSWORD 'SCRAM-SHA-256\$4096:").Count);
    }

    /// <summary>
    /// #3910's steady state: a role whose stored verifier already accepts its credential file re-asserts its
    /// attributes and NOT its password, so a converged start sends no PASSWORD clause at all. The CREATE branch
    /// keeps its verifier either way, for a role that does not exist yet.
    /// </summary>
    [Fact]
    public void BuildProvisioningSql_ReassertsThePasswordOnlyForTheRolesItIsTold()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 60, PasswordReassert.Viewer);

        Assert.Contains("ALTER ROLE admin  LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS;", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER ROLE viewer LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS PASSWORD '{ProvisioningTestSecrets.Viewer}';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS;", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE ROLE admin LOGIN NOSUPERUSER PASSWORD '{ProvisioningTestSecrets.Admin}';", sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE ROLE mcp LOGIN NOSUPERUSER PASSWORD '{ProvisioningTestSecrets.Mcp}';", sql, StringComparison.Ordinal);

        /* No PASSWORD clause anywhere after the CREATE branch: the #3914 attribute list moved the clause off the end
           of "LOGIN NOSUPERUSER", so the check is on the clause itself rather than on what used to precede it. */
        var none = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 60, PasswordReassert.None);
        Assert.DoesNotContain(" PASSWORD '", none[none.IndexOf("-- 1b.", StringComparison.Ordinal)..], StringComparison.Ordinal);
    }

    /// <summary>
    /// Which roles need their password re-asserted (#3910): a missing role, a stored MD5 hash (an older store),
    /// a verifier of a different password (a regenerated credential file) and a refused read all re-assert; a
    /// stored verifier that accepts the file's password does not.
    /// </summary>
    [Fact]
    public void PlanPasswordReassert_ReassertsOnlyWhatTheStoreDoesNotAlreadyAccept()
    {
        var stored = new System.Collections.Generic.Dictionary<string, string?>
        {
            ["admin"] = ProvisioningTestSecrets.Admin,
            ["viewer"] = ScramSha256Verifier.Create("SomeOtherPassword"),
            ["mcp"] = "md5aaaabbbbccccddddeeeeffff00001111",
            ["web"] = ProvisioningTestSecrets.Web,
        };

        Assert.Equal(
            PasswordReassert.Viewer | PasswordReassert.Mcp,
            DarlingManagedRoles.PlanPasswordReassert(stored, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, ProvisioningTestSecrets.WebPassword));

        /* The web role is planned like the others: a stored verifier that does not accept its file is re-asserted. */
        stored["web"] = ScramSha256Verifier.Create("SomeOtherWebPassword");
        Assert.Equal(
            PasswordReassert.Viewer | PasswordReassert.Mcp | PasswordReassert.Web,
            DarlingManagedRoles.PlanPasswordReassert(stored, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, ProvisioningTestSecrets.WebPassword));

        Assert.Equal(
            PasswordReassert.All,
            DarlingManagedRoles.PlanPasswordReassert(new System.Collections.Generic.Dictionary<string, string?>(), "a", "b", "c", "d"));

        var converged = new System.Collections.Generic.Dictionary<string, string?>
        {
            ["admin"] = ProvisioningTestSecrets.Admin,
            ["viewer"] = ScramSha256Verifier.Create(ProvisioningTestSecrets.ViewerPassword),
            ["mcp"] = ProvisioningTestSecrets.Mcp,
            ["web"] = ProvisioningTestSecrets.Web,
            ["viewer_unrelated"] = null,
        };
        Assert.Equal(
            PasswordReassert.None,
            DarlingManagedRoles.PlanPasswordReassert(converged, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, ProvisioningTestSecrets.WebPassword));
    }

    [Fact]
    public void BuildProvisioningSql_CreatesResolveDefinerFunction_ScopedAndGrantedToViewerAndMcp()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        /* #3334: the SECURITY DEFINER resolve-on-delete write function, so viewer/mcp can record the recovery
           row config_alert_log otherwise refuses them WITHOUT a blanket INSERT grant that would let them forge
           history. Definer-safety is the whole point, so each safety property is pinned individually. */
        Assert.Contains("CREATE OR REPLACE FUNCTION config.record_custom_alert_resolution(", sql, StringComparison.Ordinal);
        Assert.Contains("SECURITY DEFINER", sql, StringComparison.Ordinal);

        /* The pinned search_path (pg_catalog, the temporary schema last) and the table named by its
           schema keep a caller's temporary objects and search path from redirecting the config_alert_log write or now(). */
        Assert.Contains("SET search_path = pg_catalog, pg_temp", sql, StringComparison.Ordinal);

        /* A fresh function is EXECUTE-able by PUBLIC by default, so the REVOKE is mandatory and must precede the
           narrow grant; EXECUTE is the ONLY privilege the least-privilege roles get. */
        Assert.Contains("REVOKE ALL ON FUNCTION config.record_custom_alert_resolution(integer, text, text, text) FROM PUBLIC;", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT EXECUTE ON FUNCTION config.record_custom_alert_resolution(integer, text, text, text) TO web, mcp;", sql, StringComparison.Ordinal);
        var revokeAt = sql.IndexOf("REVOKE ALL ON FUNCTION config.record_custom_alert_resolution", StringComparison.Ordinal);
        var grantAt = sql.IndexOf("GRANT EXECUTE ON FUNCTION config.record_custom_alert_resolution", StringComparison.Ordinal);
        Assert.True(revokeAt >= 0 && grantAt > revokeAt, "the PUBLIC revoke must precede the EXECUTE grant.");

        /* The body writes ONE config_alert_log row, shape-LOCKED to a no-channel resolution (alert_sent false,
           notification_type 'none', zeroed values, unmuted) — so a grantee can page nothing and forge no fire. */
        Assert.Contains("INSERT INTO config.config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("false, 'none', NULL, false,", sql, StringComparison.Ordinal);

        /* NOT the rejected blanket grant: viewer/mcp never get a table-level write on the history table. The one
           privilege on it that the web login holds is the single-column dismiss grant (#4843), so every statement naming
           the table TO viewer or web must be exactly that one. */
        Assert.Equal(
            new[] { "GRANT UPDATE (dismissed) ON config.config_alert_log TO web;" },
            System.Text.RegularExpressions.Regex.Matches(sql, @"GRANT [^;]*? ON config\.config_alert_log TO [^;]*\b(?:viewer|web)\b[^;]*;")
                .Select(m => m.Value).ToArray());
        /* What mcp may do on the history table is proved by behaviour, as the mcp role:
           McpAlertAdminLiveTests.DismissAndRestore_RunAsTheMcpRole_SetOnlyTheDismissedColumn. */
        Assert.DoesNotContain("ON config_alert_log TO viewer", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON config_alert_log TO mcp", sql, StringComparison.Ordinal);

        /* The shared builder emits the same CREATE + REVOKE the gated live proof test creates the function from. */
        Assert.Contains(DarlingManagedRoles.BuildCustomAlertResolveFunctionSql("config"), sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3899: the two read identities log a statement past the slow-statement line, a third of their 60 s default
    /// ceiling (#4442) CAPPED at 5000 ms, WITHOUT its bind parameters (mcp also writes the alert settings, whose
    /// values include secrets), and the config writer (admin) is not among them. The BYO script sets the same
    /// line on its one read identity, so the two paths agree at the default, and it turns utility tracking off
    /// for its own session BEFORE its first password-bearing statement, so its own role DDL never reaches
    /// pg_stat_statements.
    /// </summary>
    [Fact]
    public void Provisioning_LogsSlowStatementsOnTheReadRolesOnly_WithoutParameters()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 60);

        Assert.Equal(5000, DarlingManagedRoles.SlowStatementThresholdMs(60));
        Assert.Equal(5000, DarlingManagedRoles.SlowStatementThresholdMs(15));
        Assert.Equal(1666, DarlingManagedRoles.SlowStatementThresholdMs(5));
        Assert.Equal(5000, DarlingManagedRoles.SlowStatementThresholdMs(600));
        Assert.Contains("ALTER ROLE viewer SET log_min_duration_statement = '5000ms';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    SET log_min_duration_statement = '5000ms';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE viewer SET log_parameter_max_length = 0;", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    SET log_parameter_max_length = 0;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER ROLE admin  SET log_min_duration_statement", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER ROLE admin SET log_min_duration_statement", sql, StringComparison.Ordinal);

        var byo = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");
        Assert.Contains("ALTER ROLE viewer SET log_min_duration_statement = '5000ms';", byo, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE viewer SET log_parameter_max_length = 0;", byo, StringComparison.Ordinal);

        var utilityOff = byo.IndexOf("\nSET pg_stat_statements.track_utility = off;", StringComparison.Ordinal);
        Assert.True(utilityOff >= 0, "provision-roles.sql no longer turns utility tracking off for its own session");
        Assert.True(utilityOff < byo.IndexOf("ALTER ROLE admin  LOGIN PASSWORD", StringComparison.Ordinal),
            "provision-roles.sql sends a password before it turns utility tracking off");

        /* The role-collision guard creates the roles without a password, so no CREATE ROLE statement carries one: a
           password rides only on the ALTER ROLE lines, which come after the guard's own utility-tracking check. */
        Assert.DoesNotContain("CREATE ROLE admin LOGIN NOSUPERUSER PASSWORD", byo, StringComparison.Ordinal);
        var trackingCheck = byo.IndexOf("current_setting('pg_stat_statements.track_utility', true) = 'on'", StringComparison.Ordinal);
        Assert.True(trackingCheck > utilityOff && trackingCheck < byo.IndexOf("CREATE ROLE admin LOGIN NOSUPERUSER;", StringComparison.Ordinal),
            "the guard must test utility tracking before it creates a role");

        /* #3914: the script creates mcp too, so its password statements and its slow-statement lines are held to
           the same order and the same settings. */
        var mcpCreate = byo.IndexOf("CREATE ROLE mcp LOGIN NOSUPERUSER;", StringComparison.Ordinal);
        var mcpAlter = byo.IndexOf("ALTER ROLE mcp    LOGIN PASSWORD", StringComparison.Ordinal);
        Assert.True(mcpCreate > utilityOff && mcpAlter > utilityOff,
            "provision-roles.sql sends the mcp password before it turns utility tracking off");
        Assert.Contains("ALTER ROLE mcp    SET log_min_duration_statement = '5000ms';", byo, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    SET log_parameter_max_length = 0;", byo, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    SET statement_timeout = '60s';", byo, StringComparison.Ordinal);

        /* The BYO remedy for the preload names the multi-literal ALTER SYSTEM form, never the one-literal list
           that stores a single library name (#3904's review). */
        Assert.Contains("ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_stat_statements';", byo, StringComparison.Ordinal);
        Assert.DoesNotContain("'timescaledb,pg_stat_statements'", byo, StringComparison.Ordinal);
    }

    /* ---------------------------------------------------------------------------------------------------
       A role's credential file is written only after the batch that sets its password has committed.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>A data source with nothing listening behind it: opening a connection fails at once, so provisioning
    /// fails after the credential files have been read and before any batch could be sent.</summary>
    private static NpgsqlDataSource UnreachableStore() => NpgsqlDataSource.Create(
        new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            Username = "darling",
            Database = "darling",
            Timeout = 3,
            Pooling = false,
        }.ConnectionString);

    /// <summary>A provisioning attempt that fails leaves no credential file for a role that had none: a file
    /// written first would hand the web and MCP hosts a password the store never received.</summary>
    [Fact]
    public async Task AFailedAttempt_WritesNoCredentialFile_ForRolesThatHadNone()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The credential files are DPAPI blobs, which are Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-role-files-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            await using var store = UnreachableStore();

            await Assert.ThrowsAnyAsync<Exception>(() => DarlingManagedRoles.EnsureProvisionedAsync(
                store, dataDirectory, NullLogger.Instance, TestContext.Current.CancellationToken));

            Assert.Empty(Directory.GetFileSystemEntries(root.FullName));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>A credential file an untrusted account owns is removed before the batch, so it is gone after a
    /// batch that fails, and nothing takes its place: the hosts wait for a file rather than read the untrusted one.</summary>
    [Fact]
    public async Task AnUntrustedCredentialFile_IsRemovedBeforeTheBatch_AndStaysGoneWhenTheBatchFails()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The credential files are DPAPI blobs, which are Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-role-files-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var viewerPath = DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);
            File.WriteAllText(viewerPath, DarlingSecrets.Protect("WrittenByAnotherAccount1"));
            var log = new CapturingTestLogger();
            await using var store = UnreachableStore();

            await Assert.ThrowsAnyAsync<Exception>(() => DarlingManagedRoles.ProvisionManagedRolesAsync(
                store, dataDirectory, log, isTrustedOwner: _ => false, TestContext.Current.CancellationToken));

            Assert.False(File.Exists(viewerPath), "the untrusted credential file survived a failed attempt");
            Assert.Empty(Directory.GetFileSystemEntries(root.FullName));
            Assert.Contains("not owned by a trusted principal", log.Joined, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>A trusted file is what it always was: read for its password and kept, whatever the batch does.</summary>
    [Fact]
    public async Task ATrustedCredentialFile_IsUsedAndKept_WhenTheBatchFails()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The credential files are DPAPI blobs, which are Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-role-files-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var paths = new[]
            {
                DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory),
                DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory),
                DarlingManagedPostgres.McpCredentialPathFor(dataDirectory),
            };
            var before = new Dictionary<string, byte[]>();
            foreach (var path in paths)
            {
                File.WriteAllText(path, DarlingSecrets.Protect("Existing-" + Path.GetFileName(path)));
                before[path] = File.ReadAllBytes(path);
            }

            var read = DarlingManagedRoles.ReadOrGenerateRoleCredential(
                paths[1], DarlingManagedPostgres.ViewerRoleName, allowInteractiveRead: true, NullLogger.Instance, _ => true);
            Assert.False(read.Generated);
            Assert.Equal("Existing-" + Path.GetFileName(paths[1]), read.Password);

            await using var store = UnreachableStore();
            await Assert.ThrowsAnyAsync<Exception>(() => DarlingManagedRoles.ProvisionManagedRolesAsync(
                store, dataDirectory, NullLogger.Instance, isTrustedOwner: _ => true, TestContext.Current.CancellationToken));

            Assert.Equal(paths.Length, Directory.GetFileSystemEntries(root.FullName).Length);
            foreach (var path in paths)
            {
                Assert.Equal(before[path], File.ReadAllBytes(path));
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>What a batch that succeeded leaves behind: a file for each generated password and for no other,
    /// created with the hardened ACL (no ordinary user can read it, and the mcp file has no entry beyond SYSTEM,
    /// Administrators and the service account), decrypting to the password its role now holds, with no temporary
    /// file left beside it.</summary>
    [Fact]
    public void GeneratedCredentials_AreWrittenHardened_AndOnlyTheGeneratedOnes()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The credential files are DPAPI blobs, which are Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-role-files-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var admin = new DarlingManagedRoles.RoleCredential(
                DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory), DarlingManagedPostgres.AdminRoleName,
                "AdminPassword1", allowInteractiveRead: true, generated: true);
            var viewer = new DarlingManagedRoles.RoleCredential(
                DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory), DarlingManagedPostgres.ViewerRoleName,
                "ViewerPassword1", allowInteractiveRead: true, generated: false);
            var mcp = new DarlingManagedRoles.RoleCredential(
                DarlingManagedPostgres.McpCredentialPathFor(dataDirectory), DarlingManagedPostgres.McpRoleName,
                "McpPassword1", allowInteractiveRead: false, generated: true);

            DarlingManagedRoles.PersistGeneratedRoleCredentials(new[] { admin, viewer, mcp }, NullLogger.Instance);

            Assert.Equal("AdminPassword1", DarlingSecrets.Unprotect(File.ReadAllText(admin.FilePath).Trim()));
            Assert.Equal("McpPassword1", DarlingSecrets.Unprotect(File.ReadAllText(mcp.FilePath).Trim()));
            Assert.False(File.Exists(viewer.FilePath), "a credential read from an existing file was written again");
            Assert.Equal(
                new[] { "pg-admin-credential.dpapi", "pg-mcp-credential.dpapi" },
                Directory.GetFiles(root.FullName).Select(f => Path.GetFileName(f)!).OrderBy(f => f, StringComparer.Ordinal).ToArray());
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(admin.FilePath));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(mcp.FilePath));
            Assert.Null(DarlingFileSecurity.AccessBeyondTrusted(mcp.FilePath));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>One file that cannot be written does not keep the others off the disk, and the caller gets one
    /// exception that names the role and never a password.</summary>
    [Fact]
    public void AGeneratedCredentialThatCannotBeWritten_DoesNotKeepTheOthersOffTheDisk_AndIsReportedOnce()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The credential files are DPAPI blobs, which are Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-role-files-");
        try
        {
            var admin = new DarlingManagedRoles.RoleCredential(
                Path.Combine(root.FullName, "no-such-folder", "pg-admin-credential.dpapi"), DarlingManagedPostgres.AdminRoleName,
                "AdminPassword1", allowInteractiveRead: true, generated: true);
            var mcp = new DarlingManagedRoles.RoleCredential(
                Path.Combine(root.FullName, "pg-mcp-credential.dpapi"), DarlingManagedPostgres.McpRoleName,
                "McpPassword1", allowInteractiveRead: false, generated: true);

            var thrown = Assert.Throws<IOException>(() =>
                DarlingManagedRoles.PersistGeneratedRoleCredentials(new[] { admin, mcp }, NullLogger.Instance));

            Assert.Contains("admin", thrown.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("mcp (", thrown.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("AdminPassword1", thrown.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("McpPassword1", thrown.Message, StringComparison.Ordinal);
            Assert.True(File.Exists(mcp.FilePath), "the credential that could be written was not");
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The order that makes the rest true: credentials are read (an untrusted file removed) before the
    /// batch is sent, and written only after it returns, with the result the core handed back returned unchanged.</summary>
    [Fact]
    public void ProvisioningReadsBeforeTheBatch_AndWritesOnlyAfterIt()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs"));

        var start = code.IndexOf("internal static async Task<int> ProvisionManagedRolesAsync(", StringComparison.Ordinal);
        Assert.True(start > 0, "the managed provisioning core could not be located, so this pin proves nothing");
        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', start)));

        var read = body.LastIndexOf("ReadOrGenerateRoleCredential(", StringComparison.Ordinal);
        var batch = body.IndexOf("await ProvisionRolesAsync(", StringComparison.Ordinal);
        var write = body.IndexOf("PersistGeneratedRoleCredentials(", StringComparison.Ordinal);
        Assert.True(read > 0 && batch > read, "a credential must be read (or generated) before the batch is sent");
        Assert.True(write > batch, "a credential file must be written only after the batch has returned");
        Assert.Equal(4, CountOf(body, "ReadOrGenerateRoleCredential("));
        Assert.Equal(1, CountOf(body, "PersistGeneratedRoleCredentials("));

        /* The read step writes nothing: a new file is only ever created by the persist step. */
        var readStep = code.IndexOf("internal static RoleCredential ReadOrGenerateRoleCredential(", StringComparison.Ordinal);
        Assert.True(readStep > 0, "the read step could not be located");
        var readBody = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', readStep)));
        Assert.DoesNotContain("CreateHardenedFile", readBody, StringComparison.Ordinal);
        Assert.DoesNotContain("File.WriteAllText", readBody, StringComparison.Ordinal);
        Assert.Contains("TryDelete(credentialPath, logger)", readBody, StringComparison.Ordinal);
    }

    /// <summary>The worker remembers a start-path provisioning failure and retries it from the hourly store-maintenance
    /// tick: outside the TimescaleDB gate (roles do not depend on the extension), as a sibling after the if/else that
    /// gate opens, and inside the tick's own method, so it runs at most once per tick.</summary>
    [Fact]
    public void TheWorker_RetriesAFailedProvisioning_OncePerHourlyTick_OutsideTheTimescaleGate()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        const string retryCall = "await RetryRoleProvisioningAsync(stoppingToken);";
        const string lastConvergence = "await ConvergeStoreObjectsAsync(stoppingToken, timescaleAvailable: false);";
        Assert.Equal(1, CountOf(code, retryCall));

        var tick = code.IndexOf("private async Task RunStoreMaintenanceTickAsync(", StringComparison.Ordinal);
        var convergence = code.IndexOf(lastConvergence, tick, StringComparison.Ordinal);
        var retry = code.IndexOf(retryCall, StringComparison.Ordinal);
        Assert.True(tick > 0 && convergence > tick && retry > convergence,
            "the retry must sit in the tick's own method, after both of its convergence arms");
        Assert.Equal("}", code[(convergence + lastConvergence.Length)..retry].Trim());

        var startCall = code.IndexOf("await DarlingManagedRoles.EnsureProvisionedAsync(postgres, dataDirectory, _logger, stoppingToken);", StringComparison.Ordinal);
        var remember = code.IndexOf("_roleProvisioningRetryDirectory = provisioningDirectory;", StringComparison.Ordinal);
        Assert.True(startCall > 0 && remember > startCall, "the start path must remember a failed provisioning after the attempt");
        Assert.Equal(1, CountOf(code, "_roleProvisioningRetryDirectory = provisioningDirectory;"));
    }

    /// <summary>The retry records what the roles were given exactly as the start path does, clears itself only
    /// after an attempt returned, runs under its own budget, and logs a failed attempt once.</summary>
    [Fact]
    public void TheRetry_ClearsOnlyAfterSuccess_RecordsTheAppliedTimeout_AndLogsEachFailureOnce()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var start = code.IndexOf("private async Task RetryRoleProvisioningAsync(CancellationToken cancellationToken)", StringComparison.Ordinal);
        Assert.True(start > 0, "the retry could not be located, so this pin proves nothing");
        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', start)));

        var attempt = body.IndexOf("DarlingManagedRoles.EnsureProvisionedAsync(_postgres, dataDirectory, _logger, budget.Token)", StringComparison.Ordinal);
        var clear = body.IndexOf("_roleProvisioningRetryDirectory = null;", StringComparison.Ordinal);
        Assert.True(attempt > 0 && clear > attempt, "the retry must clear itself only after the attempt returned");
        Assert.Equal(1, CountOf(body, "_roleProvisioningRetryDirectory = null;"));
        var applied = body.IndexOf("_appliedComposeStatementTimeoutSeconds =", StringComparison.Ordinal);
        Assert.True(applied > 0 && applied < attempt, "the retry must record what the roles were given, as the start path does");
        Assert.Contains("budget.CancelAfter(s_roleProvisioningRetryBudget)", body, StringComparison.Ordinal);

        /* One success line and one warning per failing arm (the two arms are exclusive), never an error line. */
        Assert.Equal(1, CountOf(body, "_logger.LogInformation("));
        Assert.Equal(2, CountOf(body, "_logger.LogWarning("));
        Assert.Equal(0, CountOf(body, "_logger.LogError("));
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
