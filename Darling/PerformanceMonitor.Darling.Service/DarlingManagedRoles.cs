/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Least-privilege role provisioning for the managed store (V8 security hardening, #1262) — the
/// conf-append discipline of <see cref="DarlingManagedPostgres"/> applied to roles and credentials.
/// The service connects as the bootstrap superuser <c>darling</c> (it does DDL: migrations,
/// hypertable conversion, retention), and it provisions three least-privilege LOGIN roles that
/// connect instead of the superuser:
/// <list type="bullet">
/// <item><b><c>admin</c></b> — SELECT on both schemas + INSERT/UPDATE/DELETE on <c>config</c> only.
/// The Viewer's default identity: it owns the alert-dismiss, mute-rule, and analysis-mute writes but
/// can never DROP, alter schema, touch <c>collect</c> data, or create objects.</item>
/// <item><b><c>viewer</c></b> — SELECT on both schemas, plus a NARROW, enumerated set of writes: the web
/// dashboard's write surfaces run as this role, so it holds INSERT/UPDATE/DELETE on
/// <c>config.custom_views</c> (#1563, the user-authored view definitions), <c>config.custom_alert_rules</c>
/// (#3285, the user-authored alert rules), <c>config.database_state_expected</c> (#1986, the Viewer's
/// per-database override editor), <c>config.server_tags</c> and <c>config.server_tag_map</c> (#5085, the
/// server-tag endpoints) and <c>config.config_mute_rules</c> (#3450, the dedicated mute-rule
/// endpoints — plus the two <c>config_service</c> beacon columns its bump trigger writes as the caller), and
/// the single <c>dismissed</c> column of <c>config.config_alert_log</c> (#4843, the web Alert History dismiss),
/// and INSERT (never DELETE, and no UPDATE of any kind) on <c>config.config_monitored_servers</c> (#4843, the web
/// add-server route, which runs the <c>add_servers</c> core; #5240, the web edit route, which holds no UPDATE on the
/// table and edits through <c>config.edit_monitored_server</c>, a function the batch grants EXECUTE on to viewer and
/// mcp; the <c>encrypted_password</c> column stays SELECT-carved, so the web host can write a password blob and
/// never read one back).
/// Every table is non-secret-keyed; over the web, editing is gated server-side by the host's auth + the seat model
/// (an OIDC viewer seat is refused every write) — these grants are only the floor beneath that gate. A
/// locked-down deployment points the Viewer at this role, and its WPF surfaces still read as "look but
/// don't touch": the read-only probe discriminates on a privilege this role never gets
/// (<c>ViewerDataService.ReadOnlyProbeSql</c>).</item>
/// <item><b><c>mcp</c></b> — the (optionally network-exposed) MCP host's store identity
/// (darling-network-endpoints, D3-role): the SAME read surface as <c>viewer</c> (SELECT on
/// <c>collect</c> + <c>config</c>-minus-the-secret-columns) PLUS a NARROW, enumerated set of writes —
/// INSERT on <c>collect.analysis_findings</c> and INSERT/DELETE on <c>config.analysis_muted</c> (what <c>analyze_server</c>
/// persists + the mute and unmute tools need, #6383), INSERT/UPDATE/DELETE on <c>config.custom_views</c> (the
/// custom-view tools, #1599), INSERT/UPDATE/DELETE on <c>config.database_state_expected</c> (the database-state expectation tool, #6397), the alert-tuning writes (INSERT/UPDATE/DELETE on
/// <c>config.config_mute_rules</c> + UPDATE on the singleton <c>config.config_alert_settings</c> + UPDATE on
/// the single non-secret <c>email_cooldown_minutes</c> column of <c>config.config_notification</c> (its secret columns are never
/// granted to <c>mcp</c>: the default channels are written only through the SECURITY DEFINER <c>set_notification_channels</c>, #6392,
/// served only under <c>mcp.allowWrites</c> with a token), plus the
/// two beacon columns of <c>config.config_service</c> so the settings write's self-bump trigger can fire),
/// and the server-onboarding writes (INSERT/UPDATE/DELETE on <c>config.config_monitored_servers</c> for the
/// <c>add_servers</c>/<c>remove_server</c> tools — a single non-secret-KEY table; INSERT/UPDATE/DELETE on
/// <c>config.server_tags</c> and <c>config.server_tag_map</c> (the fleet server-tag tools, #5085); the credential column stays
/// SELECT-carved, so <c>mcp</c> can WRITE a password blob but never READ one back).
/// Deliberately NOT <c>admin</c>: a token-holder reachable over the network must never get the
/// <c>config_command</c> service-credential pivot or the secret columns. Every write grant is an EXPLICIT
/// single-table (or single-column) statement with NO <c>ALTER DEFAULT PRIVILEGES</c> (ADP has no per-table
/// form -> it would broaden <c>mcp</c> to all of a schema); a dropped/recreated table re-grants because
/// provisioning re-runs every start.</item>
/// </list>
///
/// <para>On every managed startup (after migration, before TimescaleDB conversion), for each role:
/// read its DPAPI-LocalMachine credential file beside the data directory, or GENERATE one if missing
/// (self-heal — a superuser can always <c>ALTER ROLE … PASSWORD</c>, so a deleted file just
/// regenerates, a nicer property than the owner's unrecoverable password). Then run the idempotent
/// provisioning DDL with the passwords injected: <c>DO</c>-guarded <c>CREATE ROLE</c>, an
/// <c>ALTER ROLE … PASSWORD</c> re-assert so role and file never drift, and the
/// <c>GRANT</c>/<c>ALTER DEFAULT PRIVILEGES</c> that make new collector tables auto-inherit SELECT.
/// Every statement is idempotent, so re-running each start converges — no version stamp, existence
/// checks drive it exactly as <see cref="DarlingManagedPostgres.EnsureConfAppended"/> uses its conf
/// marker.</para>
///
/// <para>Managed provisioning is Windows-only (the DPAPI credential files), like every DPAPI surface here —
/// carried on the provisioning members rather than the type, because the compose
/// <c>statement_timeout</c> surface (<see cref="ShouldReassertComposeStatementTimeout"/> and friends)
/// is platform-neutral SQL that the cross-platform control-plane reload calls (#2918), and a member
/// cannot widen a type-level platform annotation. Managed mode provisions all three roles
/// (admin/viewer/mcp). So does the Linux compose distribution's own store (#3914,
/// <see cref="EnsureComposeStoreProvisionedAsync"/>): the same batch, run as the store container's bootstrap
/// superuser, with the passwords kept in plain owner-only files instead of DPAPI blobs. Any other
/// bring-your-own store provisions the roles out-of-band with <c>Darling/tools/provision-roles.sql</c>, and
/// its web dashboard and MCP server connect with <c>postgres.webConnectionString</c> /
/// <c>postgres.mcpConnectionString</c> (<see cref="DarlingStoreLogins"/>).</para>
/// </summary>
public static class DarlingManagedRoles
{
    /// <summary>
    /// The comment stamped on every Darling-created login role (<c>COMMENT ON ROLE … IS</c>, read back
    /// via <c>shobj_description(oid, 'pg_authid')</c>). Because the role names are the bare, un-prefixed
    /// <c>admin</c>/<c>viewer</c> (Erik's decision — no <c>darling_</c> namespace), provisioning must not
    /// silently repurpose a same-named role someone else created: an existing role WITHOUT this marker
    /// makes provisioning fail loud rather than reset its password/privileges.
    /// </summary>
    public const string RoleMarker = "darling-managed";

    /// <summary>
    /// The marker comment on a login role that belongs to an external alert source (#5654). The batch re-asserts the
    /// grants of a registered source role only when the role carries this marker, so a registry row that names some
    /// other role (a typo, or the owner) changes nothing about that role.
    /// </summary>
    public const string AlertSourceRoleMarker = "darling-alert-source";

    /// <summary>
    /// The NOLOGIN group role every registered external alert source login is a member of (#5654). It holds no privilege on
    /// anything; it exists so one pg_hba line (<c>hostssl darling +darling_alert_sources …</c>, written when
    /// <c>postgres.network.role</c> lists <c>sources</c>) admits every source login and nothing else. The membership is granted
    /// WITH INHERIT FALSE, SET FALSE on PostgreSQL 16 and later, and the source logins are NOINHERIT, so belonging to it gives a
    /// login nothing but the pg_hba match.
    /// </summary>
    public const string AlertSourceGroupRoleName = "darling_alert_sources";

    /// <summary>
    /// The marker comment on <see cref="AlertSourceGroupRoleName"/> (#5654). The same rule as <see cref="RoleMarker"/>: a role of
    /// that name without this marker makes provisioning fail rather than be taken over.
    /// </summary>
    public const string AlertSourceGroupRoleMarker = "darling-alert-source-group";

    /// <summary>The number of connections one external alert source login may hold at once (#5654), set on every start. A login cannot raise its own limit.</summary>
    public const int AlertSourceConnectionLimit = 5;

    /// <summary>The argument types of <c>config.submit_external_alert</c> (#5654), in order. The one place the signature is written, so the grant and the revoke cannot drift from the function.</summary>
    internal const string ExternalAlertSubmitSignature = "text, text, text, text, text, text, double precision";

    /// <summary>
    /// The marker on the roles the service provisions on the compose distribution's own store (#3914) — a
    /// second value rather than <see cref="RoleMarker"/> because <c>tools/provision-roles.sql</c> stamps that one
    /// too, and the service re-keys the roles it owns from its own credential files on every start. Sharing the
    /// marker would let it take over roles an operator created with the script, reset their passwords and break
    /// whatever logs in with them. With its own marker the service manages only roles it created on that store,
    /// and leaves any other same-named role alone (<see cref="RefuseComposeStore"/>).
    /// </summary>
    public const string ComposeStoreRoleMarker = "darling-compose";

    /// <summary>
    /// The <c>config</c> tables that carry SECRET columns the read-only <c>viewer</c> role must never read,
    /// each paired with the EXACT non-secret columns it may (#1262 Medium credential-column follow-up). The
    /// <c>admin</c> role — which writes and therefore owns these secrets, and which the Settings window
    /// connects as — keeps its full table-wide SELECT; only <c>viewer</c> is column-restricted.
    /// <list type="bullet">
    /// <item><c>config_monitored_servers.encrypted_password</c> — a DPAPI-LocalMachine password blob.</item>
    /// <item><c>config_command.args_json</c> — carries the inline test_connect credential blob (also nulled
    /// on terminal state; see <see cref="DarlingCommandExecutor.ReportCommandSql"/>).</item>
    /// <item><c>config_notification</c> — the SMTP password blob + username, and the Teams/Slack webhook
    /// URLs (a webhook URL is a bearer secret).</item>
    /// </list>
    ///
    /// <para><b>Fail-CLOSED by construction:</b> <see cref="BuildViewerColumnAclSql"/> grants <c>viewer</c>
    /// column-level SELECT on ONLY the enumerated <see cref="ViewerSecretTableAcl.NonSecretColumns"/>, so a
    /// column added to one of these tables in a future migration is INVISIBLE to <c>viewer</c> until it is
    /// deliberately added here — the opposite of GRANT-ALL-then-REVOKE-each-secret, which would silently
    /// expose a new secret column. The live <c>DarlingSecuritySplitLiveTests</c> asserts the union of the
    /// two column sets equals the table's actual columns, so a migration that adds a column without updating
    /// this list FAILS the build's live gate. A NEW secret-bearing <c>config</c> table added later must also
    /// be added here (its <c>ALTER DEFAULT PRIVILEGES</c> table-grant to <c>viewer</c> would otherwise
    /// expose every column).</para>
    /// </summary>
    public static readonly IReadOnlyList<ViewerSecretTableAcl> ViewerRestrictedConfigTables = new[]
    {
        new ViewerSecretTableAcl(
            "config_monitored_servers",
            NonSecretColumns: new[]
            {
                "server_id", "name", "host", "database", "auth", "username", "encrypt_mode",
                "trust_server_certificate", "read_only_intent", "multi_subnet_failover",
                "excluded_databases", "monthly_cost_usd", "capture_plans", "is_enabled",
                "created_at", "modified_at", "alert_delivery_mode_override",
                /* V68. Non-secret: which engine a target is, and its port, are exactly as sensitive as its
                   host — which is already readable. The fail-closed design is why they have to be named at
                   all: an unclassified column stays invisible to `viewer` rather than being exposed by
                   default, so the live security gate fails until someone decides which side it is on. */
                "engine", "port",
                /* V107 (#2138): whether the force-plan bot may write to this server. Non-secret — an
                   arm/disarm STATE, exactly as sensitive as is_enabled beside it, and the viewer has to
                   be able to SHOW which servers are armed for the opt-in to be auditable at all. The
                   fail-closed gate is why it must be named here: unclassified stays invisible to
                   `viewer` and the live security test fails until someone decides which side it is on. */
                "plan_force_bot_enabled",
                /* V113 (#2138 phase 1): the remediation credential's LOGIN NAME. Non-secret on the same
                   reasoning as `username` two lines up — a login name is not a credential, and it is the
                   only column that can answer "which identity would a remediation run as", which an
                   operator has to be able to audit without holding the secret. It is also how the viewer
                   learns a server is armed at all: the phase-1 surface exists when this is non-null, so a
                   `viewer` seat that could not read it would see no surface on an armed server. */
                "remediation_username",
                /* V203 (#6170): the hypothetical-index login's LOGIN NAME. Non-secret on the same reasoning as `remediation_username`. */
                "hypothetical_index_username",
                /* V213 (#6254): the Query Store replica HOST. Non-secret on the same reasoning as `host` above, which is already readable. */
                "query_store_replica",
                /* V169 (#5452): the AWS role Darling assumes for an RDS or Aurora target, and whether an external
                   ID is stored with it. Non-secret: an account id is an identifier, not a credential, and the
                   viewer has to be able to SHOW which role a target uses. The flag lets it show that an
                   external ID is set without ever being able to read the ID. */
                "aws_role_arn", "aws_external_id_set",
            },
            /* remediation_encrypted_password is the same kind of thing as encrypted_password beside it: a
               DPAPI blob whose whole purpose is to authenticate a WRITE to a monitored server, so if
               anything in this table is secret it is. Named explicitly rather than left unclassified
               because unclassified is only invisible until someone "fixes" the failing security gate by
               adding the column to whichever list is nearer. */
            /* aws_external_id (V169, #5452) is write-only for these roles. AWS does not treat an external ID as
               a secret, but a role that trusts a whole account plus an external ID is only as closed as that ID
               is private, so it is named here rather than left unclassified. */
            SecretColumns: new[] { "encrypted_password", "remediation_encrypted_password", "aws_external_id", "hypothetical_index_encrypted_password" }),

        new ViewerSecretTableAcl(
            "config_command",
            NonSecretColumns: new[]
            {
                "command_id", "created_at", "requested_by", "command_type", "target_server_id",
                "status", "claimed_at", "completed_at", "result_status", "result_json", "service_instance",
            },
            SecretColumns: new[] { "args_json" }),

        new ViewerSecretTableAcl(
            "config_notification",
            NonSecretColumns: new[]
            {
                "id", "smtp_host", "smtp_port", "smtp_use_ssl", "smtp_from_address", "smtp_recipients",
                "email_cooldown_minutes", "teams_proxy", "slack_proxy", "modified_at",
                "generic_body_template", "generic_proxy", "pagerduty_use_eu_region", "pagerduty_proxy",
                /* V166. Non-secret: which lifecycle a PagerDuty recovery rides — the shipped info-trigger or
                   the opt-in close — is behaviour, exactly as sensitive as pagerduty_use_eu_region beside
                   it. The fail-closed design is why it must be named at all: unclassified stays invisible
                   to the read roles and the live security gate fails until someone decides which side it
                   is on. */
                "pagerduty_auto_resolve",
                /* V187 (#5825): whether each channel sends an alert's resolution. Behaviour, not a credential, the
                   same as pagerduty_auto_resolve above. */
                "slack_send_resolutions", "teams_send_resolutions", "generic_send_resolutions", "email_send_resolutions",
                /* V193 (#5997): when the previous webhook signing key stops being sent. A time, not the key. */
                "previous_signing_key_until",
            },
            /* generic_headers carries the Authorization bearer token itself, and generic_url is a bearer
               secret like the sibling webhook URLs (#1506 / V26). pagerduty_routing_key is the Events API v2
               integration key — a bearer secret like the webhook URLs. */
            SecretColumns: new[]
            {
                "smtp_encrypted_password", "smtp_username", "teams_url", "slack_url",
                "generic_url", "generic_headers", "pagerduty_routing_key",
                /* V193 (#5997): the sealed webhook signing key and the previous one kept for the 7-day overlap. Whoever holds
                   a key can forge a signed send, so a read role never sees either. */
                "signing_key", "previous_signing_key",
            }),

        /* V131 (#3598): the sparse notification-routes table mirrors the parent row's destination columns
           under the SAME names, so the same classification applies verbatim — a webhook URL or a PagerDuty
           routing key is a bearer secret wherever it sits. configured_channels is the GENERATED presence
           column that exists precisely so a role denied the URLs can still say which channels a route sets
           (column-level SELECT is per column; the expression runs at write time). smtp_recipients is
           non-secret here as it is on the parent. */
        new ViewerSecretTableAcl(
            "config_notification_routes",
            NonSecretColumns: new[]
            {
                "route_id", "metric_match", "smtp_recipients", "configured_channels", "enabled", "modified_at",
                "scope_server_id", "scope_tag_id", "agent_level", "agent_max_per_hour", "agent_max_per_day", "agent_lease_minutes",
                "agent_expiry_minutes",
                /* V187 (#5825): the route's override of the channels' "send resolutions" flags. Non-secret. */
                "send_resolutions",
                /* V193 (#5997): the fallback route (D8) and when the previous signing key stops being sent. Non-secret. */
                "fallback_route_id", "previous_signing_key_until",
                /* V229 (#6928): the route's severity filter (both, critical or warning). Non-secret. */
                "severities",
            },
            SecretColumns: new[] { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key", "signing_key", "previous_signing_key" }),
    };

    /// <summary>
    /// The fail-closed viewer column-ACL block for one <c>config</c> schema + <c>viewer</c> role: for each
    /// <see cref="ViewerRestrictedConfigTables"/> entry, REVOKE the table-wide SELECT (undoing the blanket
    /// <c>GRANT SELECT ON ALL TABLES</c> and the V17 <c>ALTER DEFAULT PRIVILEGES</c> grant that already
    /// landed on it) and re-GRANT SELECT on ONLY the non-secret columns. Shared verbatim by
    /// <see cref="BuildProvisioningSql"/> and the live security test so the two can never drift. The table
    /// and column names are compile-time constants from this file (never user input), so interpolation is
    /// safe — the same reasoning the password-injection guard relies on.
    /// </summary>
    public static string BuildViewerColumnAclSql(string configSchema, string viewerRole) =>
        BuildColumnAclSql(configSchema, viewerRole, withheld: null);

    /// <summary>
    /// Columns the <c>mcp</c> role is denied although <c>viewer</c> reads them (#6230): <c>config_command.result_json</c> holds
    /// what the service wrote back for every command type, including the Viewer-only ones (an actual plan, the live active
    /// queries), so the <c>mcp</c> login reads a command's result only through
    /// <c>read_mcp_command_result</c> (<see cref="BuildReadMcpCommandResultFunctionSql"/>), which answers only for the types
    /// the MCP tools queue. <c>args_json</c> is already a secret column for every read role.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string[]> McpWithheldColumns =
        new Dictionary<string, string[]>(StringComparer.Ordinal) { ["config_command"] = ["result_json"] };

    /// <summary>
    /// The same fail-closed column-ACL block as <see cref="BuildViewerColumnAclSql"/> for the <c>mcp</c> role, minus the
    /// <see cref="McpWithheldColumns"/>. REVOKE comes first, so a store that granted <c>mcp</c> the withheld column before
    /// loses it on the next run.
    /// </summary>
    public static string BuildMcpColumnAclSql(string configSchema, string mcpRole) =>
        BuildColumnAclSql(configSchema, mcpRole, McpWithheldColumns);

    private static string BuildColumnAclSql(string configSchema, string role, IReadOnlyDictionary<string, string[]>? withheld) =>
        string.Join("\n", ViewerRestrictedConfigTables.Select(acl =>
        {
            var columns = withheld is not null && withheld.TryGetValue(acl.Table, out var drop)
                ? acl.NonSecretColumns.Where(c => !drop.Contains(c, StringComparer.Ordinal)).ToList()
                : acl.NonSecretColumns.ToList();
            return $"REVOKE SELECT ON {configSchema}.{acl.Table} FROM {role};\n" +
                   $"GRANT SELECT ({string.Join(", ", columns)}) ON {configSchema}.{acl.Table} TO {role};";
        }));

    /// <summary>
    /// Ensures the <c>admin</c>/<c>viewer</c>/<c>mcp</c> roles, their DPAPI credentials, and the
    /// collect/config grants exist and match — idempotent and self-healing. Opens one connection from the
    /// owner-<c>darling</c> data source (ALTER DEFAULT PRIVILEGES FOR ROLE darling only governs objects
    /// darling creates, which is all of them). MUST run AFTER migration: the <c>mcp</c> role's per-table
    /// INSERT grants name <c>collect.analysis_findings</c> / <c>config.analysis_muted</c> by qualified
    /// name, which the one-shot batch requires to already exist (they do — the worker migrates before it
    /// calls this). Throws on a hard failure; the caller degrades (the Viewer/MCP cannot connect as their
    /// roles until a later attempt succeeds) but keeps collecting.
    ///
    /// <para><b>A credential file is written only after the batch that sets its password has committed.</b> A
    /// trusted file is read as it stands; a missing one, or one an untrusted account owns (removed here, before
    /// anything is sent), gets a password generated in memory. The web and MCP hosts connect with whatever
    /// these files hold, so a file written first and a batch that then failed would leave them holding a
    /// password the store does not have. A failed attempt therefore leaves no new file and no untrusted one, and
    /// the hosts keep waiting for the file rather than logging in with a wrong password. The caller retries a
    /// failed attempt: nothing was recorded anywhere, so the retry generates its passwords again.</para>
    /// </summary>
    /// <returns>
    /// The compose <c>statement_timeout</c> in seconds that was actually WRITTEN onto the roles — which is
    /// not necessarily what the store holds a moment later. This runs BEFORE
    /// <c>StoreConfigProvider.SeedIfEmptyAsync</c>, so on a brand-new store there is no <c>config_service</c>
    /// row to read and the roles get the 15 s default, while the seed then inserts <c>darling.json</c>'s
    /// value. #2918's reload gate compares against what the roles were given, so it must be seeded from this
    /// return value and NOT from the post-seed store view — doing the latter would record a value the roles
    /// never received, and since the gate only fires on a difference, that first-run mismatch would never be
    /// corrected.
    /// </returns>
    [SupportedOSPlatform("windows")]
    public static Task<int> EnsureProvisionedAsync(
        NpgsqlDataSource dataSource, string dataDirectory, ILogger logger, CancellationToken cancellationToken = default) =>
        ProvisionManagedRolesAsync(dataSource, dataDirectory, logger, DarlingFileSecurity.IsTrustedOwner, cancellationToken);

    /// <summary>
    /// <see cref="EnsureProvisionedAsync"/> with the owner check on an existing credential file passed in, so a
    /// test can present a file an untrusted account owns (a real one cannot be made without elevation). The
    /// public entry point always passes <see cref="DarlingFileSecurity.IsTrustedOwner"/>.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static async Task<int> ProvisionManagedRolesAsync(
        NpgsqlDataSource dataSource, string dataDirectory, ILogger logger, Func<string, bool> isTrustedOwner,
        CancellationToken cancellationToken)
    {
        if (dataSource is null)
        {
            throw new ArgumentNullException(nameof(dataSource));
        }

        if (string.IsNullOrWhiteSpace(dataDirectory))
        {
            throw new ArgumentException("Data directory is required.", nameof(dataDirectory));
        }

        /* admin/viewer credentials are read by the operator's Viewer -> readable by the operator group. */
        var admin = ReadOrGenerateRoleCredential(
            DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory), DarlingManagedPostgres.AdminRoleName,
            allowInteractiveRead: true, logger, isTrustedOwner);
        var viewer = ReadOrGenerateRoleCredential(
            DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory), DarlingManagedPostgres.ViewerRoleName,
            allowInteractiveRead: true, logger, isTrustedOwner);
        /* The mcp credential is consumed only by the in-service MCP host, never an interactive Viewer, so it
           is hardened NON-interactive (mirrors the superuser posture, not admin/viewer's) — Round 4 #4. */
        var mcp = ReadOrGenerateRoleCredential(
            DarlingManagedPostgres.McpCredentialPathFor(dataDirectory), DarlingManagedPostgres.McpRoleName,
            allowInteractiveRead: false, logger, isTrustedOwner);

        /* The web host's own login: read by the in-service web host only, never an interactive Viewer, so it is hardened
           non-interactive like the mcp one. The same reader/generator as every other role's credential. */
        var web = ReadOrGenerateRoleCredential(
            DarlingManagedPostgres.WebCredentialPathFor(dataDirectory), DarlingManagedPostgres.WebRoleName,
            allowInteractiveRead: false, logger, isTrustedOwner);

        var applied = await ProvisionRolesAsync(
            dataSource, admin.Password, viewer.Password, mcp.Password, web.Password, ProvisioningTarget.Managed, logger, cancellationToken);

        /* Only now: each generated password is one its role has accepted, so a file on disk is always a login the
           store honours. A throw above leaves every generated password unwritten. */
        PersistGeneratedRoleCredentials(new[] { admin, viewer, mcp, web }, logger);
        return applied;
    }

    /// <summary>
    /// Provisions the <c>admin</c>/<c>viewer</c>/<c>mcp</c> roles on the Linux compose distribution's own store
    /// (#3914), the way <see cref="EnsureProvisionedAsync"/> does on the managed one, so the web dashboard and the
    /// MCP server connect as <c>viewer</c> and <c>mcp</c> there instead of as the owner. Nothing to configure: the
    /// caller runs it for a service in a container on a non-managed store, and this decides from the store itself.
    ///
    /// <para><b>Which store counts as the service's own.</b> One whose login is the cluster's BOOTSTRAP superuser
    /// (oid 10, the role <c>initdb</c> created) on a cluster that holds no database but the store's own,
    /// <c>postgres</c> and the templates. The compose file's store image runs <c>initdb --username
    /// $POSTGRES_USER</c> with the service's login and creates the one database, so that is exactly the compose
    /// store; a superuser granted on a cluster somebody else initialized never is, and neither is an operator's
    /// multi-database cluster the service reaches as its bootstrap superuser. Anything else is refused with a
    /// reason and nothing is written, so the surfaces keep the bring-your-own rules (<see cref="DarlingStoreLogins"/>).
    /// A same-named role without <see cref="ComposeStoreRoleMarker"/> is refused the same way: re-keying it would
    /// break whoever uses it.</para>
    ///
    /// <para><b>The credentials</b> are the <c>file:</c> secret shape (#1804): one plaintext password per file in
    /// <paramref name="credentialDirectory"/> (<see cref="ComposeStoreCredentialFileName"/>), created owner-only.
    /// Read when present and trusted, generated otherwise, and written only AFTER the batch has committed, so a
    /// file on disk is always a password its role accepts. A lost file (a container recreated without the
    /// credentials volume) regenerates and is re-asserted on the next start, which the marker makes safe. The
    /// directory is set owner-only again before any file is read, and one that cannot be trusted
    /// (<see cref="PrepareComposeCredentialDirectory"/>) has none of its files read: every role gets a new
    /// password this start.</para>
    ///
    /// <para>The same batch as managed mode, with three differences carried by <see cref="ProvisioningTarget"/>:
    /// the owner and database are the login's own names (the compose file can rename them), the marker is
    /// <see cref="ComposeStoreRoleMarker"/>, and the database-level <c>REVOKE ALL … FROM PUBLIC</c> is left out,
    /// because on this store other roles are the operator's (people read the store directly) and that statement
    /// would take their <c>CONNECT</c> away.</para>
    /// </summary>
    /// <returns>Provisioned with the applied compose statement_timeout and the viewer/mcp passwords, or refused
    /// with the reason. Throws on a hard failure; the caller degrades to the bring-your-own rules.</returns>
    public static async Task<ComposeStoreProvisioning> EnsureComposeStoreProvisionedAsync(
        NpgsqlDataSource dataSource, string credentialDirectory, ILogger logger, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        if (string.IsNullOrWhiteSpace(credentialDirectory))
        {
            throw new ArgumentException("Credential directory is required.", nameof(credentialDirectory));
        }

        ComposeStoreFacts facts;
        await using (var connection = await dataSource.OpenConnectionAsync(cancellationToken))
        {
            facts = await ReadComposeStoreFactsAsync(connection, cancellationToken);
        }

        if (RefuseComposeStore(facts) is { } refusal)
        {
            return ComposeStoreProvisioning.Refused(refusal);
        }

        /* One warning for the directory, not one per file it holds. */
        var directory = PrepareComposeCredentialDirectory(credentialDirectory, create: true, logger);
        if (directory.Distrust is { } distrust)
        {
            logger.LogWarning(
                "The compose store's credentials directory {Directory} is not trusted ({Reason}), so no credential file in it is read this start: every role gets a new password, re-asserted on the role{Written}.",
                credentialDirectory, distrust,
                directory.MayWrite ? ", and the new files replace the old" : ", and none is written there this start");
        }

        var readFiles = directory.Distrust is null;
        var admin = ReadOrGenerateComposeCredential(credentialDirectory, DarlingManagedPostgres.AdminRoleName, readFiles, logger);
        var viewer = ReadOrGenerateComposeCredential(credentialDirectory, DarlingManagedPostgres.ViewerRoleName, readFiles, logger);
        var mcp = ReadOrGenerateComposeCredential(credentialDirectory, DarlingManagedPostgres.McpRoleName, readFiles, logger);
        var web = ReadOrGenerateComposeCredential(credentialDirectory, DarlingManagedPostgres.WebRoleName, readFiles, logger);

        var applied = await ProvisionRolesAsync(
            dataSource, admin.Password, viewer.Password, mcp.Password, web.Password,
            ProvisioningTarget.ComposeStore(facts.Login, facts.Database), logger, cancellationToken);

        foreach (var credential in new[] { admin, viewer, mcp, web })
        {
            if (credential.Generated && directory.MayWrite)
            {
                PersistComposeCredential(credentialDirectory, credential, logger);
            }
        }

        return ComposeStoreProvisioning.Succeeded(applied, viewer.Password, mcp.Password, web.Password);
    }

    /// <summary>
    /// The advisory-lock key every role-provisioning write takes (#5560): "DARLROLE" in ASCII. Distinct from the
    /// migration lock's key on purpose, so a long migration never makes a provisioning start wait for it.
    /// </summary>
    internal const long ProvisioningLockKey = 0x4441524C_524F4C45;

    /// <summary>
    /// How long a provisioning write waits for <see cref="ProvisioningLockKey"/> before it stops waiting and runs
    /// without it (#5560). A real sibling's batch takes well under this, so the wait only runs out when something
    /// else is holding the key, and the write must not stall behind that.
    /// </summary>
    internal const int ProvisioningLockWaitSeconds = 12;

    /// <summary>How often the wait for <see cref="ProvisioningLockKey"/> asks again.</summary>
    internal const int ProvisioningLockPollMilliseconds = 1000;

    /// <summary>
    /// How many times a write that fails with a concurrent-write code (<see cref="IsConcurrentProvisioningConflict"/>) is run again, each time in a fresh
    /// transaction (#5560). Three runs in all.
    /// </summary>
    internal const int ProvisioningConcurrentUpdateRetries = 2;

    /// <summary>The savepoint a read inside the locked transaction takes, so a read that fails and is answered with a
    /// default does not leave the transaction aborted for the batch that follows it.</summary>
    private const string ReadSavepoint = "darling_provisioning_read";

    /// <summary>
    /// Runs a role-provisioning batch in ONE transaction that first takes <see cref="ProvisioningLockKey"/>
    /// (#5560), for a caller that has the command already built. The callback overload below has the whole contract.
    /// </summary>
    /// <remarks>
    /// Takes the CALLER's command rather than SQL text, so each call site keeps spelling its own
    /// <c>CommandTimeout</c> (the startup-deadline census reads it there). The wait for the key has its own budget
    /// (<see cref="ProvisioningLockWaitSeconds"/>); the batch keeps the call site's <c>CommandTimeout</c>.
    /// </remarks>
    internal static Task ExecuteSerializedAsync(
        NpgsqlCommand command, ILogger logger, CancellationToken cancellationToken, TimeSpan? lockWait = null)
    {
        ArgumentNullException.ThrowIfNull(command);
        var connection = command.Connection ?? throw new ArgumentException("The command has no connection.", nameof(command));
        return ExecuteSerializedCoreAsync(
            connection,
            (transaction, _) =>
            {
                command.Transaction = transaction;
                return Task.FromResult<NpgsqlCommand?>(command);
            },
            disposeCommand: false, logger, lockWait, cancellationToken);
    }

    /// <summary>
    /// Runs a role-provisioning write in ONE transaction that first takes <see cref="ProvisioningLockKey"/>, and
    /// lets the caller read and plan INSIDE that transaction (#5560). Roles, database privileges and default
    /// privileges are rows in shared catalogs, and PostgreSQL does not lock the row a GRANT, DROP OWNED or ALTER
    /// ROLE rewrites: two sessions rewriting the same row at once make the second fail with XX000 "tuple
    /// concurrently updated" once the first commits. The service adopts a postmaster that is already running, so
    /// two services can start against one cluster at once, each running the same batch against the same
    /// <c>CONNECT</c> ACL and the same roles. The batch is idempotent, so the loser has only to wait for the
    /// winner, and what it read before the wait would be stale after it: <paramref name="prepare"/> runs after the
    /// lock is taken (READ COMMITTED gives each of its reads a snapshot taken then) and returns the command to run,
    /// or <see langword="null"/> when there is nothing to run. A transaction-scoped lock is released by the commit
    /// (or the rollback, or the connection dropping), so it can never outlive the pooled connection it was taken on.
    ///
    /// <para><b>The lock never fails or stalls the write.</b> Any login with a session in the store's database can
    /// take an advisory lock, so the key is polled with <c>pg_try_advisory_xact_lock</c> about once a second for
    /// <see cref="ProvisioningLockWaitSeconds"/> rather than waited on without end. When that budget runs out one
    /// warning names the key and the session holding it (pid, user, application name), and the write runs WITHOUT
    /// the lock, in a fresh transaction, in the same order (<paramref name="prepare"/>, then the command). A write
    /// that fails with a concurrent-write code (<see cref="IsConcurrentProvisioningConflict"/>) is run again, up to <see cref="ProvisioningConcurrentUpdateRetries"/> more times, each in a
    /// fresh transaction, <paramref name="prepare"/> included. Npgsql closes the connection on an XX-class error, so a
    /// retry opens it again first (a new session from the same pool). That is safe because every write here is
    /// idempotent (the live tests run the real batch twice).</para>
    ///
    /// <para><b>What the lock covers.</b> An advisory lock belongs to the database the session is connected to, so
    /// it serializes only the sessions connected to the STORE's database, although the catalog rows it protects
    /// (<c>pg_authid</c>, <c>pg_db_role_setting</c>, <c>pg_database</c>) are cluster-wide. That is enough because
    /// every writer connects to that one database: the managed store always uses the fixed database
    /// <c>darling</c>, and the compose path refuses a cluster that holds any other non-template database
    /// (<c>RefuseComposeStore</c>), so a second database with a provisioning writer cannot exist. If either rule is
    /// ever relaxed, the lock stops serializing and only the retry is left;
    /// <c>ProvisioningSerializationTests</c> pins both rules.</para>
    /// </summary>
    /// <param name="connection">An open connection with no transaction on it.</param>
    /// <param name="prepare">Runs inside the transaction, after the lock, and returns the command to run (the helper
    /// disposes it) or <see langword="null"/>. It runs again on a retry, so it must not keep state between runs.
    /// A read in it that fails and is answered with a default must take the transaction's read savepoint first and
    /// roll back to it, or the transaction stays aborted. A callback whose read failed and was not rolled back to the
    /// read savepoint must throw, never return null, because a null return commits.</param>
    /// <param name="lockWait">The wait for the key; defaults to <see cref="ProvisioningLockWaitSeconds"/>.</param>
    internal static Task ExecuteSerializedAsync(
        NpgsqlConnection connection, Func<NpgsqlTransaction, CancellationToken, Task<NpgsqlCommand?>> prepare,
        ILogger logger, CancellationToken cancellationToken, TimeSpan? lockWait = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(prepare);
        return ExecuteSerializedCoreAsync(connection, prepare, disposeCommand: true, logger, lockWait, cancellationToken);
    }

    private static async Task ExecuteSerializedCoreAsync(
        NpgsqlConnection connection, Func<NpgsqlTransaction, CancellationToken, Task<NpgsqlCommand?>> prepare,
        bool disposeCommand, ILogger logger, TimeSpan? lockWait, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(logger);
        var wait = lockWait ?? TimeSpan.FromSeconds(ProvisioningLockWaitSeconds);
        var lockGivenUp = false;

        for (var retries = 0; ; retries++)
        {
            try
            {
                if (!lockGivenUp)
                {
                    if (await RunUnderLockAsync(connection, prepare, disposeCommand, wait, logger, cancellationToken))
                    {
                        return;
                    }

                    lockGivenUp = true;
                }

                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                await RunInTransactionAsync(transaction, prepare, disposeCommand, cancellationToken);
                return;
            }
            catch (PostgresException ex) when (IsConcurrentProvisioningConflict(ex) && retries < ProvisioningConcurrentUpdateRetries)
            {
                logger.LogInformation(
                    "Role provisioning write failed with {SqlState} ({Message}), which a concurrent catalog rewrite causes; running it again in a new transaction (retry {Retry} of {Retries})",
                    ex.SqlState, ex.MessageText, retries + 1, ProvisioningConcurrentUpdateRetries);

                /* Npgsql breaks the connection on an XX-class error (it closes it, and the server rolls the transaction
                   back), so the retry needs it opened again: the same object draws a new session from the pool. */
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    await connection.OpenAsync(cancellationToken);
                }
            }
        }
    }

    /// <summary>Whether a failed provisioning write is the kind two sessions running the batch together cause, so a
    /// rerun in a fresh transaction is right. Each is safe to rerun because the batch is idempotent and the failed
    /// transaction rolled back, so the rerun sees the row the other session made: XX000 (a catalog row updated
    /// under it), 23505 (both inserted the same catalog key; the rerun finds the row), 42710 (the role was created
    /// between the existence check and the create; the rerun skips it) and 40P01 (the two took the same rows in
    /// different orders; the other one finished). Any other code (a missing right, a timeout) is not a conflict
    /// and reaches the caller on the first failure. Matched on the code, never the message, which
    /// <c>lc_messages</c> translates.</summary>
    internal static bool IsConcurrentProvisioningConflict(PostgresException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return ex.SqlState is PostgresErrorCodes.InternalError
            or PostgresErrorCodes.UniqueViolation
            or PostgresErrorCodes.DuplicateObject
            or PostgresErrorCodes.DeadlockDetected;
    }

    /// <summary>One run with the key taken. False when the wait for the key ran out (after the one warning), in which
    /// case nothing ran.</summary>
    private static async Task<bool> RunUnderLockAsync(
        NpgsqlConnection connection, Func<NpgsqlTransaction, CancellationToken, Task<NpgsqlCommand?>> prepare,
        bool disposeCommand, TimeSpan wait, ILogger logger, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            await using (var poll = new NpgsqlCommand(
                $"SELECT pg_try_advisory_xact_lock({ProvisioningLockKey})", connection, transaction) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds })
            {
                if ((bool)(await poll.ExecuteScalarAsync(cancellationToken))!)
                {
                    break;
                }
            }

            var remaining = wait - clock.Elapsed;
            if (remaining <= TimeSpan.Zero)
            {
                await WarnLockHeldAsync(connection, transaction, wait, logger, cancellationToken);
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }

            var pause = TimeSpan.FromMilliseconds(ProvisioningLockPollMilliseconds);
            await Task.Delay(remaining < pause ? remaining : pause, cancellationToken);
        }

        await RunInTransactionAsync(transaction, prepare, disposeCommand, cancellationToken);
        return true;
    }

    private static async Task RunInTransactionAsync(
        NpgsqlTransaction transaction, Func<NpgsqlTransaction, CancellationToken, Task<NpgsqlCommand?>> prepare,
        bool disposeCommand, CancellationToken cancellationToken)
    {
        var command = await prepare(transaction, cancellationToken);
        try
        {
            if (command is not null)
            {
                command.Transaction = transaction;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            if (disposeCommand && command is not null)
            {
                await command.DisposeAsync();
            }
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>The one warning for a wait that ran out: the key and the session that holds it, read from
    /// <c>pg_locks</c> joined to <c>pg_stat_activity</c> in the current database. A login that is not a superuser,
    /// not a member of <c>pg_read_all_stats</c> and not in the holder's role sees "many columns" of the holder's row
    /// as null (the query text, state and client fields among them), but the pid, the session user and the
    /// application name stay visible to everyone, so the "(not visible)" text below shows only for a row that has
    /// no name at all (a background worker, for example). See the PostgreSQL manual, "The Cumulative Statistics
    /// System", security note on the dynamic statistics views
    /// (https://www.postgresql.org/docs/current/monitoring-stats.html).</summary>
    private static async Task WarnLockHeldAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, TimeSpan wait, ILogger logger, CancellationToken cancellationToken)
    {
        var holder = "no holder found (it may have just let go)";
        try
        {
            await using var command = new NpgsqlCommand(
                $@"SELECT a.pid, a.usename, a.application_name
FROM pg_catalog.pg_locks AS l
JOIN pg_catalog.pg_stat_activity AS a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
  AND l.classid::bigint = {ProvisioningLockKey >> 32} AND l.objid::bigint = {ProvisioningLockKey & 0xFFFFFFFFL}
  AND l.database = (SELECT d.oid FROM pg_catalog.pg_database AS d WHERE d.datname = current_database())
LIMIT 1", connection, transaction) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                holder = string.Format(
                    CultureInfo.InvariantCulture, "pid {0}, user '{1}', application '{2}'",
                    reader.GetInt32(0), reader.IsDBNull(1) ? "(not visible)" : reader.GetString(1), reader.IsDBNull(2) ? "(not visible)" : reader.GetString(2));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            holder = "the holder could not be read (" + ex.Message + ")";
        }

        logger.LogWarning(
            "Role provisioning could not take its advisory lock 0x{Key} within {Seconds}s; it is held by {Holder}. Running the provisioning write without it. Releasing the lock from that session (or ending the session) restores the serialization.",
            ProvisioningLockKey.ToString("X16", CultureInfo.InvariantCulture), (int)wait.TotalSeconds, holder);
    }

    /// <summary>Takes the read savepoint before a read inside the locked transaction (nothing without one).</summary>
    private static Task TakeReadSavepointAsync(NpgsqlTransaction? transaction, CancellationToken cancellationToken) =>
        transaction is null ? Task.CompletedTask : transaction.SaveAsync(ReadSavepoint, cancellationToken);

    /// <summary>Rolls the transaction back to the read savepoint after a read that failed and was answered with a
    /// default, so the batch after it does not meet "current transaction is aborted". Never throws: a connection that is
    /// gone fails the batch with its own error.</summary>
    private static async Task RecoverFromReadAsync(NpgsqlTransaction? transaction)
    {
        if (transaction is null)
        {
            return;
        }

        try
        {
            await transaction.RollbackAsync(ReadSavepoint, CancellationToken.None);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The connection is unusable; the next command on it reports that. */
        }
    }

    /// <summary>
    /// The rest of provisioning once each role's password is in hand — shared by
    /// <see cref="EnsureProvisionedAsync"/> and <see cref="EnsureComposeStoreProvisionedAsync"/> (#3914), which
    /// differ only in where the passwords live and in <paramref name="target"/>. Returns the compose
    /// statement_timeout it wrote onto the roles (see <see cref="EnsureProvisionedAsync"/>).
    /// </summary>
    private static async Task<int> ProvisionRolesAsync(
        NpgsqlDataSource dataSource, string adminPassword, string viewerPassword, string mcpPassword, string webPassword,
        ProvisioningTarget target, ILogger logger, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);

        /* #5560: the two reads, the plan and the batch all run inside the provisioning lock's transaction, so a sibling
           service's batch cannot rewrite the same catalog rows at once and what this one planned from is read after
           the wait rather than before it. The callback runs again on a retry, so it only assigns these. */
        int? composeTimeoutSeconds = null;
        var reassert = PasswordReassert.None;
        await ExecuteSerializedAsync(connection, async (transaction, token) =>
        {
            /* #2357: read the live knob rather than a constant. Ordering is what makes this safe -- migrations
               run before provisioning at startup, so the column exists by now -- and because this DDL is re-run
               on every managed start, a changed value reaches an existing install on its next restart without
               any new machinery. A store whose config row is not seeded yet answers with the default. */
            composeTimeoutSeconds = await ReadComposeStatementTimeoutAsync(connection, transaction, logger, token);

            /* #3910: the batch carries SCRAM-SHA-256 VERIFIERS, never a password, so no surface that records
               statement text (the server log's STATEMENT lines, log_statement, auto_explain, pg_stat_statements
               with utility tracking on) can capture a credential. And a role whose stored verifier already accepts
               its credential file's password is not re-asserted at all: a steady-state start sends no PASSWORD
               clause. The CREATE branch still carries a fresh verifier, for a role that does not exist yet. */
            var stored = await ReadStoredRoleSecretsAsync(connection, transaction, logger, token);
            reassert = PlanPasswordReassert(stored, adminPassword, viewerPassword, mcpPassword, webPassword);

            return new NpgsqlCommand(
                BuildProvisioningSql(
                    ScramSha256Verifier.Create(adminPassword),
                    ScramSha256Verifier.Create(viewerPassword),
                    ScramSha256Verifier.Create(mcpPassword),
                    composeTimeoutSeconds,
                    reassert,
                    target,
                    ScramSha256Verifier.Create(webPassword)),
                connection, transaction) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        }, logger, cancellationToken);

        logger.LogInformation(
            "Role passwords: {Reasserted} (sent as SCRAM-SHA-256 verifiers, never as the password)",
            reassert == PasswordReassert.None
                ? "unchanged, the stored verifiers already accept every credential file"
                : "re-asserted for " + DescribeReassert(reassert));

        logger.LogInformation(
            "Least-privilege roles ready (admin: read both schemas + write config; viewer: read-only, the login for remote and locked-down Viewer seats; web: viewer's reads + the narrow web-surface writes (custom_views, custom_alert_rules, database_state_expected, config_mute_rules + the reload beacon + the config_alert_log dismissed column + server_tags, server_tag_map + INSERT on config_monitored_servers and the edit function); mcp: viewer's reads + INSERT on analysis_findings/analysis_muted + write config.custom_views + tune alerting (config_mute_rules, config_alert_settings, config_notification.email_cooldown_minutes, config_service reload beacon) + onboard servers (config_monitored_servers) + tag servers (server_tags, server_tag_map)) — the Viewer, the web dashboard and the MCP host no longer connect as the superuser");

        /* CLAMPED, not raw: the batch above wrote the clamped form, so returning the raw read would hand the
           caller a baseline that differs from what the roles actually carry (a stored 0 provisions '15s').

           And when the read FAILED, the batch above deliberately wrote nothing, so there is no applied value
           to report. ComposeStatementTimeoutUnknown is the negative "not yet known" sentinel
           ShouldReassertComposeStatementTimeout already documents: it can never equal a clamped store value,
           so the next control-plane reload re-asserts rather than concluding the roles are already correct. */
        return composeTimeoutSeconds is int read
            ? StoreConfigProvider.ClampComposeStatementTimeoutSeconds(read)
            : ComposeStatementTimeoutUnknown;
    }

    /// <summary>
    /// The <c>statement_timeout</c> backstop on the two composed-query identities, as SQL, with the
    /// slow-statement line derived from it (#3899, <see cref="SlowStatementThresholdMs"/>). The SINGLE
    /// renderer for those statements — <see cref="BuildProvisioningSql"/> embeds it at startup and
    /// <see cref="ReassertComposeStatementTimeoutAsync"/> runs it alone on a control-plane reload (#2918),
    /// so the two paths cannot disagree about the ceiling, and a reload that moves the ceiling moves the line
    /// with it.
    ///
    /// <para>Clamped rather than trusted, because this is public and both callers reach it with an
    /// operator-supplied number: 0 or negative would remove the backstop entirely, which is the one outcome
    /// the whole design leans on not happening. A LIMIT bounds OUTPUT; a group-by scans and sorts before it,
    /// so something has to bound WORK.</para>
    ///
    /// <para>The clamp is <see cref="StoreConfigProvider.ClampComposeStatementTimeoutSeconds"/>, not a local
    /// copy of the formula. Re-deriving it here would be the same "two things must agree or the ceiling
    /// silently disagrees" hazard this single-renderer design exists to remove — applied to the SQL text but
    /// not to the bounds feeding it, which is not a coherent place to stop.</para>
    ///
    /// <para>The <c>temp_file_limit</c> backstop (#4605, <see cref="ComposeLimits.TempFileLimit"/>) that caps
    /// the on-disk spill a runaway read may write is NOT rendered here: it does not derive from the compose
    /// timeout, so an unreadable timeout must not drop it. It has its own renderer,
    /// <see cref="BuildComposeTempFileLimitSql"/>.</para>
    /// </summary>
    public static string BuildComposeStatementTimeoutSql(int composeStatementTimeoutSeconds)
    {
        const string viewer = DarlingManagedPostgres.ViewerRoleName;
        const string web = DarlingManagedPostgres.WebRoleName;
        const string mcp = DarlingManagedPostgres.McpRoleName;
        var statementTimeout =
            $"{StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeStatementTimeoutSeconds)}s";
        var slowStatement = $"{SlowStatementThresholdMs(composeStatementTimeoutSeconds).ToString(CultureInfo.InvariantCulture)}ms";

        return $@"ALTER ROLE {viewer} SET statement_timeout = '{statementTimeout}';
ALTER ROLE {web}    SET statement_timeout = '{statementTimeout}';
ALTER ROLE {mcp}    SET statement_timeout = '{statementTimeout}';
ALTER ROLE {viewer} SET log_min_duration_statement = '{slowStatement}';
ALTER ROLE {web}    SET log_min_duration_statement = '{slowStatement}';
ALTER ROLE {mcp}    SET log_min_duration_statement = '{slowStatement}';";
    }

    /// <summary>
    /// The <c>temp_file_limit</c> backstop on the two composed-query identities (#4605,
    /// <see cref="ComposeLimits.TempFileLimit"/>), as SQL: the cap on the on-disk spill a runaway read may
    /// write before the store cancels it with SQLSTATE 53400. A CONSTANT, unlike
    /// <see cref="BuildComposeStatementTimeoutSql"/>'s lines: it does not derive from the operator's compose
    /// timeout, so it has its own renderer and is emitted regardless of whether that timeout could be read —
    /// <see cref="BuildProvisioningSql"/> embeds it unconditionally at startup and
    /// <see cref="ReassertComposeStatementTimeoutAsync"/> runs it alongside the timeout lines on a
    /// control-plane reload (#2918), so the two paths cannot disagree about the ceiling.
    /// </summary>
    public static string BuildComposeTempFileLimitSql()
    {
        const string viewer = DarlingManagedPostgres.ViewerRoleName;
        const string web = DarlingManagedPostgres.WebRoleName;
        const string mcp = DarlingManagedPostgres.McpRoleName;

        return $@"ALTER ROLE {viewer} SET temp_file_limit = '{ComposeLimits.TempFileLimit}';
ALTER ROLE {web}    SET temp_file_limit = '{ComposeLimits.TempFileLimit}';
ALTER ROLE {mcp}    SET temp_file_limit = '{ComposeLimits.TempFileLimit}';";
    }

    /// <summary>
    /// The slow-statement line on the two read identities (#3899), in milliseconds: a statement from
    /// <c>viewer</c> or <c>mcp</c> that runs this long is written to the store's own log, where the store-log
    /// sweep retains it under <c>slow_statement</c>. A THIRD of the same clamped <c>statement_timeout</c>,
    /// CAPPED at 5000 ms (#4442): a read that is drifting toward the kill is named while it still completes,
    /// but the cap keeps the unlogged band from widening every time the ceiling is raised — the shipped
    /// default's own tail (5 s-60 s) is exactly what #4442 raised the ceiling to measure, and an uncapped
    /// third would push its own logging line to 20000 ms and stop logging it. 5 s gives 1666 ms (uncapped, the
    /// floor); 15 s gives 5000 ms (the cap, exactly); 60 s and above all give 5000 ms (the cap, clamped).
    /// Derived rather than fixed because the ceiling is an operator knob that goes down to 5 s, where a fixed
    /// 5 s line could never fire (the statement is cancelled at the line, and a cancel logs no duration).
    /// </summary>
    public static int SlowStatementThresholdMs(int composeStatementTimeoutSeconds) =>
        Math.Min(5000, StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeStatementTimeoutSeconds) * 1000 / 3);

    /// <summary>
    /// <c>log_parameter_max_length = 0</c> on the <c>viewer</c> and <c>mcp</c> roles, as SQL (#3899): the
    /// privacy half of their slow-statement logging, not tuning. The line would otherwise carry every bind
    /// parameter, and <c>mcp</c> also writes the alert settings, whose values include webhook URLs and SMTP
    /// credentials. The statement text names the query, and that is what the line is for (the store-log sweep
    /// masks its literals too, <c>StoreLogClassifier.SlowStatementClass</c>). <c>admin</c> gets no line: it is
    /// the config writer, and its reads still rank in <c>get_store_query_stats</c>, whose text is normalized.
    /// Fixed, unlike the threshold, so it lives in the startup batch alone; superuser-only, which the
    /// provisioning owner is. NOT a versioned migration, the section-1c reason.
    /// </summary>
    public static string BuildSlowStatementParameterSql()
    {
        const string viewer = DarlingManagedPostgres.ViewerRoleName;
        const string web = DarlingManagedPostgres.WebRoleName;
        const string mcp = DarlingManagedPostgres.McpRoleName;

        return $@"ALTER ROLE {viewer} SET log_parameter_max_length = 0;
ALTER ROLE {web}    SET log_parameter_max_length = 0;
ALTER ROLE {mcp}    SET log_parameter_max_length = 0;";
    }

    /// <summary>
    /// The <c>appliedSeconds</c> value meaning "provisioning did not write a <c>statement_timeout</c> this
    /// start, so the roles carry whatever they already had". Negative by construction so it can never equal
    /// a value that came through <see cref="StoreConfigProvider.ClampComposeStatementTimeoutSeconds"/>,
    /// which is what makes <see cref="ShouldReassertComposeStatementTimeout"/> converge on the next reload
    /// instead of concluding the roles are already correct.
    /// </summary>
    internal const int ComposeStatementTimeoutUnknown = -1;

    /// <summary>
    /// Whether a control-plane reload should re-assert the compose <c>statement_timeout</c> onto the roles
    /// (#2918). Pure, and deliberately separated from the reload that calls it: the decision is the whole
    /// design (when to pay a catalog write) and it is untestable inside a hosted worker loop.
    /// </summary>
    /// <param name="storeSeconds">The value the store view just reported.</param>
    /// <param name="appliedSeconds">
    /// The value last successfully WRITTEN onto the roles, or a negative sentinel for "not yet known".
    /// Compared unclamped on purpose — both sides come from the same clamped read, so a difference here is a
    /// real operator change rather than a rounding artifact.
    /// </param>
    /// <param name="managedStore">
    /// Managed mode. Any other store provisions these roles out-of-band via
    /// <c>tools/provision-roles.sql</c> and names them itself, so <c>ALTER ROLE viewer</c> would be guessing
    /// at an identity we do not own — unless <paramref name="composeStoreProvisioned"/> says otherwise.
    /// </param>
    /// <param name="isWindows">
    /// Mirrors managed provisioning's own gate. Not because the SQL needs Windows — it does not — but
    /// because provisioning is where these roles get CREATED, so off-Windows they may not exist at all and
    /// this would fail every reload.
    /// </param>
    /// <param name="composeStoreProvisioned">
    /// This process provisioned the roles on the compose distribution's own store at startup (#3914,
    /// <see cref="EnsureComposeStoreProvisionedAsync"/>): they exist and are the service's, so the reload keeps
    /// them current exactly as on a managed store. Set only on success, so a refused or failed provisioning
    /// never has the reload writing to roles the service does not own.
    /// </param>
    public static bool ShouldReassertComposeStatementTimeout(
        int storeSeconds, int appliedSeconds, bool managedStore, bool isWindows, bool composeStoreProvisioned = false) =>
        ((managedStore && isWindows) || composeStoreProvisioned) && storeSeconds != appliedSeconds;

    /// <summary>
    /// Re-asserts the compose <c>statement_timeout</c> on the viewer/mcp roles from a control-plane reload
    /// (#2918), so an operator's change to <c>config_service.compose_statement_timeout_seconds</c> reaches
    /// the live roles without a service restart — the behaviour every other <c>config_service</c> knob
    /// already had.
    ///
    /// <para><b>Why this is not the whole provisioning batch.</b> That batch also re-asserts all three role
    /// passwords from the credential files and re-grants every ACL. Running it on each <c>config_version</c>
    /// bump would do a large amount of unrelated work on a write that touched one integer, so this is the
    /// two statements and nothing else.</para>
    ///
    /// <para><b>A role SET only takes on the NEXT session for that role</b>, which is what makes this cheap
    /// and safe: it is a catalog write, it cannot disturb a query already running under the old ceiling, and
    /// an already-connected viewer keeps its old value until it reconnects. Lowering the ceiling therefore
    /// bounds the NEXT runaway, not the one in flight — killing that is still the operator's job.</para>
    ///
    /// <para>Non-throwing: a failure here must never kill a reload that has already applied the rest of the
    /// store view, the same posture startup provisioning takes (it degrades, collection continues).</para>
    /// </summary>
    public static async Task<bool> ReassertComposeStatementTimeoutAsync(
        NpgsqlDataSource dataSource, int composeStatementTimeoutSeconds, ILogger logger,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);

        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            /* #5560: the same lock as provisioning, so a reload cannot rewrite a role row a starting sibling is
               rewriting. The wait is the serial-loop deadline, not the full provisioning wait, because this runs at
               the top of a sweep: a key something else is holding delays the reload by that wait, then the lines run
               without it, so the bound is that wait plus the batch at its own CommandTimeout (and a rerun if it
               hits a concurrent-write failure). */
            await using var command = new NpgsqlCommand(
                BuildComposeStatementTimeoutSql(composeStatementTimeoutSeconds) + "\n" + BuildComposeTempFileLimitSql(),
                connection) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
            await ExecuteSerializedAsync(
                command, logger, cancellationToken, lockWait: TimeSpan.FromSeconds(ServiceCommandDeadlines.SerialLoopSeconds));

            logger.LogInformation(
                "Compose statement_timeout re-asserted on the viewer/mcp roles at {Seconds}s, with slow-statement logging at {SlowMs} ms and a temp_file_limit of {TempFileLimit} — takes effect on each role's next session (an already-connected viewer keeps the old ceiling until it reconnects)",
                StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeStatementTimeoutSeconds),
                SlowStatementThresholdMs(composeStatementTimeoutSeconds),
                ComposeLimits.TempFileLimit);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "Could not re-assert the compose statement_timeout/temp_file_limit on the viewer/mcp roles ({Message}) — the live ceilings are whatever the last successful provisioning set, and the next service start will converge them.",
                ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Reads a TRUSTED existing role credential, or generates a fresh password IN MEMORY (self-heal): nothing
    /// new is written here (<see cref="PersistGeneratedRoleCredentials"/> writes it once the batch has
    /// committed). Same 32-char alnum <see cref="DarlingManagedPostgres.GeneratePassword"/> and
    /// DPAPI-LocalMachine posture as the owner credential; unlike the owner's, a role password can always be
    /// re-asserted (<c>ALTER ROLE … PASSWORD</c>), so an untrusted-owned file is removed
    /// here, before the batch, and replaced by a generated password rather than trusted. A trusted file has its
    /// ACL restored on every start whatever the batch does next.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static RoleCredential ReadOrGenerateRoleCredential(
        string credentialPath, string roleName, bool allowInteractiveRead, ILogger logger, Func<string, bool> isTrustedOwner)
    {
        if (File.Exists(credentialPath) && isTrustedOwner(credentialPath))
        {
            var existing = DarlingSecrets.Unprotect(File.ReadAllText(credentialPath).Trim());

            /* Re-harden every start (self-healing): admin/viewer are additionally readable by the operator group
               (whose members' Viewer reads them); mcp is NOT (SYSTEM + Administrators + service account only —
               the in-service MCP host reads it, never an interactive user). */
            TryHardenRoleCredential(credentialPath, allowInteractiveRead, logger);
            return new RoleCredential(credentialPath, roleName, existing, allowInteractiveRead, generated: false);
        }

        if (File.Exists(credentialPath))
        {
            /* A role credential owned by an arbitrary local user would feed the
               caller's ALTER ROLE … PASSWORD re-assert a password that account chose. Discard it, and do it
               now: if the batch then fails, the hosts must find no file, not this one. */
            logger.LogWarning(
                "The managed '{Role}' credential {File} is not owned by a trusted principal — discarding and regenerating it.",
                roleName, Path.GetFileName(credentialPath));
            TryDelete(credentialPath, logger);
        }

        return new RoleCredential(
            credentialPath, roleName, DarlingManagedPostgres.GeneratePassword(), allowInteractiveRead, generated: true);
    }

    /// <summary>
    /// Writes each credential that <see cref="ReadOrGenerateRoleCredential"/> generated, after the batch that
    /// gave its role that password has committed. Every file is attempted, so one that cannot be written does
    /// not keep the others off the disk; when any failed, one exception names them all, so the caller logs the
    /// failure once and retries. A role whose file is missing gets a new password on that retry, which the
    /// stored-secret check re-asserts.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static void PersistGeneratedRoleCredentials(IReadOnlyList<RoleCredential> credentials, ILogger logger)
    {
        List<string>? failures = null;
        foreach (var credential in credentials)
        {
            if (!credential.Generated)
            {
                continue;
            }

            try
            {
                PersistRoleCredential(credential);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                (failures ??= new List<string>()).Add($"{credential.Role} ({Path.GetFileName(credential.FilePath)}: {ex.Message})");
                continue;
            }

            logger.LogInformation(
                "Generated the managed '{Role}' role credential ({File})", credential.Role, Path.GetFileName(credential.FilePath));

            /* Verified after the write, as for every role credential: attempting a harden is not evidence. */
            TryHardenRoleCredential(credential.FilePath, credential.AllowInteractiveRead, logger);
        }

        if (failures is not null)
        {
            throw new IOException(
                "The role passwords were set, but a credential file could not be written, so the web and MCP hosts "
                + "cannot use that role until a later attempt writes it: " + string.Join("; ", failures));
        }
    }

    /// <summary>
    /// Writes one role credential the way <see cref="PersistComposeCredential"/> does: created with its ACL
    /// already applied (<see cref="DarlingFileSecurity.CreateHardenedFile"/>, so the folder's inherited entries
    /// never reach it, not even between a create and a harden), put on the disk, and only then renamed over the
    /// real name. A host polling for the file therefore sees either no file or the whole one, and a failed
    /// write leaves nothing at the real name that a later attempt would trust and fail to read.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static void PersistRoleCredential(RoleCredential credential)
    {
        var temporary = credential.FilePath + ".tmp";
        var moved = false;
        try
        {
            /* A leftover from an attempt that died mid-write: the create refuses an existing file. */
            File.Delete(temporary);
            using (var stream = DarlingFileSecurity.CreateHardenedFile(temporary, credential.AllowInteractiveRead))
            using (var writer = new StreamWriter(stream, new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: false)))
            {
                writer.Write(DarlingSecrets.Protect(credential.Password));
                FlushToDisk(writer, stream);
            }

            File.Move(temporary, credential.FilePath, overwrite: true);
            moved = true;
        }
        finally
        {
            if (!moved)
            {
                try
                {
                    File.Delete(temporary);
                }
                catch (Exception cleanup) when (cleanup is IOException or UnauthorizedAccessException)
                {
                    /* Best effort: the next attempt deletes it first. */
                }
            }
        }
    }

    /// <summary>One managed role's credential for this start: the password, where its file is (or will be), and
    /// whether the password was generated here and still has to be written. A class, not a record, so no generated
    /// <c>ToString</c> can put the password in a log line.</summary>
    internal sealed class RoleCredential
    {
        public RoleCredential(string filePath, string role, string password, bool allowInteractiveRead, bool generated)
        {
            FilePath = filePath;
            Role = role;
            Password = password;
            AllowInteractiveRead = allowInteractiveRead;
            Generated = generated;
        }

        public string FilePath { get; }

        public string Role { get; }

        public string Password { get; }

        public bool AllowInteractiveRead { get; }

        public bool Generated { get; }
    }

    /// <summary>Best-effort restrictive ACL on a role credential; a failure is logged loud, not fatal — and the
    /// RESULT is verified afterwards, because attempting a harden is not evidence the secret is protected.</summary>
    [SupportedOSPlatform("windows")]
    private static void TryHardenRoleCredential(string path, bool allowInteractiveRead, ILogger logger)
    {
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead);
        }
        /* Filtered like every other catch in this file. HardenFile takes no CancellationToken so a
           cancellation cannot reach here today; the filter is what keeps the rule uniform, and a
           shutdown must never be reported to an operator as an ACL failure they should go fix. */
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "Could not restrict the ACL on {Path}{Detail} ({Message}). If the owner is not this service, the " +
                "re-ACL can never succeed — it needs ownership or FullControl — so restarting will not clear this; " +
                "grant the service account FullControl or make it the owner.",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path), ex.Message);
        }

        if (DarlingFileSecurity.IsReadableByOrdinaryUsers(path))
        {
            logger.LogCritical(
                "{Path} is READABLE by ordinary local users{Detail}. It holds this role's password as a " +
                "machine-scoped DPAPI blob, which any local process can decrypt — so read access to this file IS " +
                "the login. Remove the inherited read access.",
                path, DarlingFileSecurity.DescribeOwnerAndExposure(path));
        }
    }

    private static void TryDelete(string path, ILogger logger)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogError(
                "Could not delete the untrusted credential file {Path} ({Message}) — remove it by hand so a fresh one can be generated.",
                path, ex.Message);
        }
    }

    /// <summary>
    /// The store's compose <c>statement_timeout</c> in seconds (#2357), or 15 when it cannot be read.
    ///
    /// <para><b>Two outcomes that used to look identical, and only one of them justifies a default.</b>
    /// "The store has no opinion" — no <c>config_service</c> row yet, because this runs BEFORE
    /// <c>StoreConfigProvider.SeedIfEmptyAsync</c>, or a store predating the column — genuinely means the
    /// shipped 15 s is the answer. "I could not hear the store" does not: the operator's value is sitting in
    /// a column we failed to read, and provisioning the role with 15 anyway OVERWRITES the last known-good
    /// horizon with a number nobody chose. So a failed read returns <c>null</c> and the caller leaves the
    /// roles' <c>statement_timeout</c> alone.</para>
    ///
    /// <para><b>Why that mattered more than it looks.</b> #2931 made
    /// <c>McpCommandDeadlines.ResolveComposedQuerySecondsAsync</c> read this SAME column live, per run. A
    /// silently-defaulted role therefore desyncs the two halves of one backstop: an operator who set 120
    /// gets a client-side deadline of 120, a server-side ceiling of 15, every composed query dying at 15 s,
    /// and the configured 120 visible in the UI throughout. Nothing corrected it either — #2918's reload
    /// gate does fire on the difference, but only on a <c>config_version</c> bump, and a value set before
    /// the restart bumps nothing.</para>
    ///
    /// <para>Cancellation now propagates rather than being swallowed: a shutdown arriving mid-start must not
    /// be reported as "the store said 15". That is the <c>ex is not OperationCanceledException</c> filter
    /// this file's own callers already use.</para>
    /// </summary>
    internal static async Task<int?> ReadComposeStatementTimeoutAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await TakeReadSavepointAsync(transaction, cancellationToken);
            await using var command = new NpgsqlCommand(
                "SELECT compose_statement_timeout_seconds FROM config.config_service WHERE id = 1", connection, transaction) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
            var value = await command.ExecuteScalarAsync(cancellationToken);

            /* No row, or a NULL column: the store has not been seeded yet. The shipped default is the
               answer, and the seed that runs a moment later inserts darling.json's value. */
            return value is int seconds ? seconds : McpCommandDeadlines.ComposedQueryFallbackSeconds;
        }
        catch (PostgresException ex)
            when (ex.SqlState is PostgresErrorCodes.UndefinedColumn or PostgresErrorCodes.UndefinedTable)
        {
            /* A store older than the column or the table. Also "no opinion", and expected on a first start
               against a pre-#2357 store, so it is not a warning. */
            await RecoverFromReadAsync(transaction);
            logger.LogDebug(
                "config_service.compose_statement_timeout_seconds is not present on this store ({SqlState}) — provisioning the roles with the shipped {Seconds}s default",
                ex.SqlState, McpCommandDeadlines.ComposedQueryFallbackSeconds);

            return McpCommandDeadlines.ComposedQueryFallbackSeconds;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await RecoverFromReadAsync(transaction);
            logger.LogWarning(
                "Could not read config_service.compose_statement_timeout_seconds ({Message}) — leaving the viewer/mcp roles' statement_timeout at whatever the last successful provisioning set, rather than overwriting it with a default the operator did not choose. #2931's client-side composed-query deadline reads the same column, so writing a guess here would desync the two halves of that backstop.",
                ex.Message);

            return null;
        }
    }

    /// <summary>
    /// The idempotent, self-healing provisioning DDL with the role secrets injected. Each secret is a
    /// SCRAM-SHA-256 VERIFIER (<see cref="ScramSha256Verifier"/>, #3910), never a password: PostgreSQL stores a
    /// verifier as-is, so the password never appears in a statement any log or statistics view could record.
    /// Anything that is not a verifier is REFUSED here, which is what keeps a future caller from passing the
    /// password again; the verifier's shape admits no quote, so string-building the <c>PASSWORD '…'</c>
    /// literals is escaping-safe. <paramref name="reassert"/> names the roles whose 1b <c>ALTER ROLE</c>
    /// carries the verifier; the rest re-assert their attributes only. Public + shape-pinnable so a test can
    /// assert it without a live Postgres. The <c>mcp</c>-role INSERT grants reference
    /// <c>collect.analysis_findings</c> / <c>config.analysis_muted</c> by qualified name, so the one-shot
    /// batch requires those tables to already exist — safe because provisioning runs AFTER migration (see
    /// <see cref="EnsureProvisionedAsync"/>); a dropped/recreated table re-grants on the next start.
    /// </summary>
    /// <param name="composeStatementTimeoutSeconds">
    /// The per-session <c>statement_timeout</c> for the viewer and mcp roles (#2357). Defaults to the 15 the
    /// constant used to hard-code, so a caller that does not care gets today's behaviour exactly.
    /// <para><c>null</c> means "the store's value could not be read", and OMITS the two <c>ALTER ROLE</c>
    /// statements so the roles keep the horizon the last successful provisioning gave them. Writing a
    /// default there would overwrite an operator's configured ceiling with a number nobody chose, and
    /// silently disagree with the client-side deadline #2931 reads from the same column. It is a distinct
    /// value rather than a sentinel integer because every integer in range is a legitimate ceiling.</para>
    /// </param>
    /// <param name="target">
    /// Which store the batch is for (#3914): null or <see cref="ProvisioningTarget.Managed"/> renders the
    /// managed batch exactly; <see cref="ProvisioningTarget.ComposeStore"/> names the compose store's own owner
    /// and database, stamps <see cref="ComposeStoreRoleMarker"/>, and leaves the database-level PUBLIC revoke
    /// out.
    /// </param>
    public static string BuildProvisioningSql(
        string adminVerifier, string viewerVerifier, string mcpVerifier,
        int? composeStatementTimeoutSeconds = McpCommandDeadlines.ComposedQueryFallbackSeconds,
        PasswordReassert reassert = PasswordReassert.All,
        ProvisioningTarget? target = null,
        string? webVerifier = null)
    {
        /* The web login (a fourth role, the web host's own identity). A caller that does not pass one (an older call
           shape) still gets a role that exists, with a verifier nobody holds the password for, so its grants apply. */
        webVerifier ??= ScramSha256Verifier.Create(DarlingManagedPostgres.GeneratePassword());
        RequireVerifier(webVerifier, nameof(webVerifier));
        RequireVerifier(adminVerifier, nameof(adminVerifier));
        RequireVerifier(viewerVerifier, nameof(viewerVerifier));
        RequireVerifier(mcpVerifier, nameof(mcpVerifier));

        string PasswordClause(PasswordReassert role, string verifier) =>
            (reassert & role) != 0 ? $" PASSWORD '{verifier}'" : "";

        target ??= ProvisioningTarget.Managed;
        var owner = target.OwnerIdentifier;          // darling (owner/superuser) on a managed store
        var database = target.DatabaseIdentifier;    // darling on a managed store
        const string admin = DarlingManagedPostgres.AdminRoleName;
        const string viewer = DarlingManagedPostgres.ViewerRoleName;
        const string web = DarlingManagedPostgres.WebRoleName;
        const string mcp = DarlingManagedPostgres.McpRoleName;
        const string collect = PgSchemaGenerator.CollectSchema;
        const string config = PgSchemaGenerator.ConfigSchema;
        var marker = target.RoleMarker;

        /* #3914: the managed store is loopback-only and nothing but Darling's own logins connects to it, so it
           takes CONNECT away from PUBLIC, on postgres and template1 as well, because every login's grants stop at the database it connects to. The compose store is one people read directly with roles of their own,
           and the same statement would silently take their CONNECT away, so there it is a comment instead. */
        var publicDatabaseRevoke = target.RevokePublicDatabaseAccess
            ? $"REVOKE ALL ON DATABASE {database} FROM PUBLIC;{Environment.NewLine}"
              + "-- The maintenance databases too: the large object revoke below is per database, so a login that could connect to postgres or template1 would"
              + $"{Environment.NewLine}-- still reach large objects and TEMP there. Only the owner (a superuser, which needs no CONNECT grant) opens them."
              + $"{Environment.NewLine}REVOKE CONNECT ON DATABASE postgres, template1 FROM PUBLIC;"
            : "--     LEFT AS-IS on the compose store: its other roles are the operator's, and this would revoke their CONNECT.";
        /* #2357: was ComposeLimits.StatementTimeout, a bare "15s". Rendered by the SHARED builder rather
           than inline, because #2918 made the reload path re-assert the same two statements: two renderers
           for one pair of ALTER ROLEs is a drift waiting to happen, and the drift would be invisible (both
           sides run, the roles just disagree about the ceiling depending on which path touched them last). */
        /* An unreadable value renders as a COMMENT rather than as a statement, so the batch stays one
           round trip and the omission is visible to anyone reading the SQL that ran. */
        var composeTimeoutStatements = composeStatementTimeoutSeconds is int composeSeconds
            ? BuildComposeStatementTimeoutSql(composeSeconds)
            : "--     LEFT AS-IS this start: config_service.compose_statement_timeout_seconds could not be read.";
        /* #4610: temp_file_limit is a CONSTANT, not derived from composeStatementTimeoutSeconds, so it is
           rendered unconditionally — an unreadable compose timeout must not drop the spill guard too. */
        var composeTempFileLimitStatements = BuildComposeTempFileLimitSql();

        /* The fail-closed viewer column-ACL carve for the secret-bearing config tables (see
           ViewerRestrictedConfigTables). Runs AFTER the blanket config GRANT below, so it strips
           viewer's table-wide SELECT on those tables and re-grants only the non-secret columns. */
        var viewerColumnAcl = BuildViewerColumnAclSql(config, viewer);

        /* The SAME fail-closed carve for the mcp role — it gets viewer's read surface, so it must be
           denied the identical secret columns. Reusing BuildViewerColumnAclSql(config, mcp) means the
           mcp carve can never drift from viewer's (Round 4 #5 guards this with a live denial test). */
        var mcpColumnAcl = BuildMcpColumnAclSql(config, mcp);
        var webColumnAcl = BuildViewerColumnAclSql(config, web);

        return $@"
/* Least-privilege roles for the Darling security split (#1262). Idempotent + self-healing:
   re-run every start, converging role state to the service's credential files. */

-- 1. Roles (CREATE ROLE has no IF NOT EXISTS -> guard with a DO block). The names are bare
--    admin/viewer, so a fresh role is STAMPED with a marker comment and an existing SAME-NAMED role
--    is trusted only if it carries that marker; an unmarked collision fails loud (never repurposed).
DO $do$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{admin}') THEN
      CREATE ROLE {admin} LOGIN NOSUPERUSER PASSWORD '{adminVerifier}';
      COMMENT ON ROLE {admin} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{admin}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{admin}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;

   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{viewer}') THEN
      CREATE ROLE {viewer} LOGIN NOSUPERUSER PASSWORD '{viewerVerifier}';
      COMMENT ON ROLE {viewer} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{viewer}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{viewer}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;

   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{web}') THEN
      CREATE ROLE {web} LOGIN NOSUPERUSER PASSWORD '{webVerifier}';
      COMMENT ON ROLE {web} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{web}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{web}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;

   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{mcp}') THEN
      CREATE ROLE {mcp} LOGIN NOSUPERUSER PASSWORD '{mcpVerifier}';
      COMMENT ON ROLE {mcp} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{mcp}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{mcp}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
END $do$;

-- 1b. Re-assert attributes every start, and the password (as a SCRAM-SHA-256 verifier, #3910) only for a
--     role whose stored verifier does not already accept its credential file: the file is the source of
--     truth. Only reached when the guard above passed (fresh + marked, or already Darling-marked). Every
--     attribute that widens a role is switched off by name (#3914), not only SUPERUSER: a role this batch
--     ADOPTS (it already carries the marker) keeps whatever was granted to it since, and CREATEROLE, CREATEDB,
--     REPLICATION or BYPASSRLS on viewer or mcp would outrank every grant below. Section 11 does the same for
--     role memberships.
ALTER ROLE {admin}  LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS{PasswordClause(PasswordReassert.Admin, adminVerifier)};
ALTER ROLE {viewer} LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS{PasswordClause(PasswordReassert.Viewer, viewerVerifier)};
ALTER ROLE {web}    LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS{PasswordClause(PasswordReassert.Web, webVerifier)};
ALTER ROLE {mcp}    LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS{PasswordClause(PasswordReassert.Mcp, mcpVerifier)};

-- 1c. statement_timeout backstop on the composed-query identities (Custom Views v2, #1563). viewer is the web
--     dashboard's DB identity and mcp the optional network MCP identity; both serve the network-reachable
--     compose surface over a raw, no-rollup store, so a runaway aggregation can NEVER pin the store beyond this
--     (a LIMIT bounds output, not work). A role SET applies to every future session and re-asserts each start.
--     admin (the Settings writer, small config writes) is deliberately NOT bounded. NOT a versioned migration:
--     a role statement_timeout has no probeable schema footprint, so tying it to StorageVersion would break the
--     viewer's connect-time version gate. The same renderer sets the slow-statement line (#3899) at a third of
--     the ceiling: a viewer or mcp statement past it is written to the store's own log, where the store-log
--     sweep keeps it, so a slow read is named instead of guessed at.
{composeTimeoutStatements}

-- 1c-ii. temp_file_limit backstop on the same two roles (#4605, #4610): a CONSTANT ceiling on the on-disk
--        spill a runaway read may write before the store cancels it with SQLSTATE 53400. Rendered
--        unconditionally, unlike 1c above: it does not derive from the compose timeout, so an unreadable
--        timeout must not drop this guard too.
{composeTempFileLimitStatements}

-- 1d. No bind parameters on those slow-statement lines (#3899): the mcp role also writes alert settings, whose
--     values include secrets. admin gets no line. Same non-migration reason as 1c.
{BuildSlowStatementParameterSql()}

-- 2. Schema usage + SELECT everywhere (ALL TABLES covers tables AND views). collect holds no secrets,
--    so admin+viewer read all of it. config: admin (the writer, and the Settings window's identity) reads
--    every column; viewer reads all config tables too — MINUS the secret columns carved below.
GRANT USAGE ON SCHEMA {collect}, {config} TO {admin}, {viewer}, {web};
GRANT SELECT ON ALL TABLES IN SCHEMA {collect} TO {admin}, {viewer}, {web};
GRANT SELECT ON ALL TABLES IN SCHEMA {config}  TO {admin}, {viewer}, {web};

-- 2b. Credential-column fail-closed ACLs (#1262 Medium follow-up). The read-only viewer must NOT read the
--     secret columns in config_monitored_servers / config_command / config_notification (a DPAPI password
--     blob, the test_connect credential args, the SMTP password + username, the Teams/Slack webhook URLs).
--     Instead of GRANT-ALL-then-REVOKE-each-secret (fail-OPEN — a future secret column leaks until someone
--     remembers to revoke it), DROP viewer's table-wide SELECT on each and re-grant ONLY the non-secret
--     columns (fail-CLOSED — any column added later is invisible to viewer until explicitly listed in
--     DarlingManagedRoles.ViewerRestrictedConfigTables). admin keeps its table-wide SELECT (granted above),
--     so the Settings window is unaffected. This also undoes the table-level grant the V17 ALTER DEFAULT
--     PRIVILEGES already landed on these tables when they were created.
{viewerColumnAcl}
{webColumnAcl}

-- 3. config writes -- admin gets the whole schema. (mcp gets ONLY a narrow config.analysis_muted INSERT and DELETE
--    in section 6, never the config_command/monitored_servers/notification pivot tables; viewer: none.)
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {config} TO {admin};

-- 4. Default privileges so NEW tables/views auto-inherit (no per-table-grant foot-gun).
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {collect}
   GRANT SELECT ON TABLES TO {admin}, {viewer}, {web};
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {config}
   GRANT SELECT ON TABLES TO {admin}, {viewer}, {web};
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {config}
   GRANT INSERT, UPDATE, DELETE ON TABLES TO {admin};
-- Fail-closed: today no config table has a sequence (ids are app-generated / text), but a future
-- serial/identity column would give admin INSERT with no sequence USAGE -> the write breaks. Grant it now.
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {config}
   GRANT USAGE, SELECT ON SEQUENCES TO {admin};

-- 5. Public hardening: no world-writable public schema, no anonymous connect. The REVOKE ALL drops
--    PUBLIC's implicit CONNECT, so admin/viewer are re-granted CONNECT explicitly (darling is
--    superuser + owner and never needs it).
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
{publicDatabaseRevoke}
GRANT CONNECT ON DATABASE {database} TO {admin}, {viewer}, {web};

-- 5b. Built-in functions every role can run by default, taken from PUBLIC (this batch runs as a superuser, which owns them).
--     Large objects: lo_create, lo_creat and lo_from_bytea store data in the store database whatever a role's table grants are,
--     and the service uses none. Session advisory locks: a session lock on the migration key, held open, makes the next upgrade
--     wait out its budget and stop, so the session forms go to the logins that take one (admin and mcp through the agent-change
--     undo; the superuser needs no grant). The transaction forms (a transaction left open holds the same key just as long) go to
--     the logins whose own code takes one: admin, web and mcp. The store's own functions take them as the owner, and viewer takes none.
--     The unlock forms are not touched (a session can only release its own locks).
REVOKE EXECUTE ON FUNCTION pg_catalog.lo_create(oid), pg_catalog.lo_creat(integer), pg_catalog.lo_from_bytea(oid, bytea) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.pg_advisory_lock(bigint), pg_catalog.pg_advisory_lock(integer, integer),
   pg_catalog.pg_try_advisory_lock(bigint), pg_catalog.pg_try_advisory_lock(integer, integer),
   pg_catalog.pg_advisory_lock_shared(bigint), pg_catalog.pg_advisory_lock_shared(integer, integer),
   pg_catalog.pg_try_advisory_lock_shared(bigint), pg_catalog.pg_try_advisory_lock_shared(integer, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_advisory_lock(bigint), pg_catalog.pg_advisory_lock(integer, integer),
   pg_catalog.pg_try_advisory_lock(bigint), pg_catalog.pg_try_advisory_lock(integer, integer) TO {admin}, {mcp};
REVOKE EXECUTE ON FUNCTION pg_catalog.pg_advisory_xact_lock(bigint), pg_catalog.pg_advisory_xact_lock(integer, integer),
   pg_catalog.pg_try_advisory_xact_lock(bigint), pg_catalog.pg_try_advisory_xact_lock(integer, integer),
   pg_catalog.pg_advisory_xact_lock_shared(bigint), pg_catalog.pg_advisory_xact_lock_shared(integer, integer),
   pg_catalog.pg_try_advisory_xact_lock_shared(bigint), pg_catalog.pg_try_advisory_xact_lock_shared(integer, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_advisory_xact_lock(bigint), pg_catalog.pg_advisory_xact_lock(integer, integer),
   pg_catalog.pg_try_advisory_xact_lock(bigint), pg_catalog.pg_try_advisory_xact_lock(integer, integer),
   pg_catalog.pg_advisory_xact_lock_shared(bigint), pg_catalog.pg_advisory_xact_lock_shared(integer, integer),
   pg_catalog.pg_try_advisory_xact_lock_shared(bigint), pg_catalog.pg_try_advisory_xact_lock_shared(integer, integer) TO {admin}, {web}, {mcp};

-- 6. The mcp role (darling-network-endpoints, D3-role): the ONLY credential reachable (via the bearer
--    token) from the optional network MCP surface, so it is deliberately NOT admin. It gets viewer's exact
--    READ surface (SELECT on collect + config-minus-secret-columns) as SEPARATE 'TO mcp' statements -- the
--    'TO admin, viewer' grant lines above are pinned verbatim by tests, so mcp is never appended to them --
--    PLUS the narrow analysis writes HERE: what analyze_server persists (collect.analysis_findings, INSERT only)
--    and what the mute and unmute tools write (config.analysis_muted: INSERT for mute_analysis_finding, DELETE for
--    unmute_analysis_finding, #6383) -- no UPDATE on either analysis target and no DELETE on analysis_findings.
--    The mcp role ALSO gets the narrow
--    config.custom_views write in section 7 (for the MCP custom-view tools) -- likewise a single non-secret
--    config table, never the config_command pivot or the carved secret columns. EXPLICIT single-table WRITE grants
--    with NO ALTER DEFAULT PRIVILEGES: ADP has no per-table
--    form, so an ADP INSERT would broaden mcp to ALL of collect -- provisioning re-runs every start, so a
--    recreated table re-grants (self-heal) without ADP. The two INSERT targets must already exist here, so
--    provisioning runs AFTER migration (EnsureProvisionedAsync).
GRANT USAGE ON SCHEMA {collect}, {config} TO {mcp};
GRANT SELECT ON ALL TABLES IN SCHEMA {collect} TO {mcp};
GRANT SELECT ON ALL TABLES IN SCHEMA {config}  TO {mcp};
-- #3914: and SELECT on the collect relations created AFTER this batch, which the two grants above cannot
-- reach until the next start. The continuous aggregates are the case: the TimescaleDB step creates them
-- later in the SAME start, so on a fresh store every one was unreadable to the MCP tools for the whole first
-- process lifetime (a container can run for months on one). Read-only, and collect holds no secrets, so this
-- is viewer's own collect default; mcp still gets no default WRITE anywhere and no default read on config,
-- whose secret columns are carved table by table.
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {collect}
   GRANT SELECT ON TABLES TO {mcp};
{mcpColumnAcl}
GRANT INSERT ON {collect}.analysis_findings TO {mcp};
GRANT INSERT, DELETE ON {config}.analysis_muted TO {mcp};
-- NOT granted (Round 4 #9): collect.analysis_state. MCP analyze_server calls AnalyzeAsync directly and never
-- writes the analysis_state marker -- only the worker's RunAnalysisPassAsync wrapper does, as the owner
-- (the Analysis project cannot reference the Service-project observability writer), so mcp needs no grant on it.
GRANT CONNECT ON DATABASE {database} TO {mcp};

-- 7. Custom views (#1563): the SINGLE exception to viewer-writes-nothing, and the mcp role's ONLY write outside
--    the two section-6 analysis INSERTs. Both the web dashboard (as the viewer role) and the optional network MCP
--    surface (as the mcp role) CRUD rows in exactly this one config table (non-secret dashboard/notebook JSON --
--    no ViewerRestrictedConfigTables carve). The web composer and the MCP custom-view tools share ONE store
--    (CustomViewStore) + ONE validator (DarlingWebEndpoints.ValidateDefinition), so the two write identities need
--    the identical narrow grant. Editing is any AUTHENTICATED seat -- the surfaces' normal networked mode, gated
--    server-side by the host's token+CIDR auth (web token/cookie; MCP bearer token) NOT loopback-only; this DB
--    grant is only the narrow floor beneath that gate. EXPLICIT single-table statements (one per identity) with
--    NO ALTER DEFAULT PRIVILEGES, mirroring the narrow analysis-write grants above (ADP has no per-table form ->
--    an ADP write would broaden the role to ALL of config); provisioning re-runs every start, so a recreated
--    table re-grants (self-heal). The target must already exist here, so provisioning runs AFTER migration (V31
--    created config.custom_views). id is GENERATED ALWAYS AS IDENTITY (like config_command), so the INSERT needs
--    no sequence USAGE grant.
GRANT INSERT, UPDATE, DELETE ON {config}.custom_views TO {web};
GRANT INSERT, UPDATE, DELETE ON {config}.custom_views TO {mcp};
-- Custom alerting (#3285): the web editor (viewer) and the MCP rule tools (mcp) CRUD config.custom_alert_rules,
-- the same narrow single-table floor as custom_views (non-secret rule JSON -- no ViewerRestrictedConfigTables
-- carve, no config_command pivot). Created by V116, so provisioning runs after migration. NOTE: the sibling
-- config.custom_alert_state is written by the CustomAlertEvaluator on the OWNER pool only, so it deliberately
-- gets NO viewer/mcp grant here (add a viewer SELECT only when the editor surfaces a rule's firing status).
GRANT INSERT, UPDATE, DELETE ON {config}.custom_alert_rules TO {web};
GRANT INSERT, UPDATE, DELETE ON {config}.custom_alert_rules TO {mcp};
-- The Viewer's per-database database-state override editor (#1986) writes config.database_state_expected:
-- the same narrow single-table floor as custom_views. Created by V49, so provisioning runs after migration.
-- The mcp role gets the same three (#6397): set_database_state_expected upserts (INSERT, UPDATE) and clears (DELETE) one row, and its
-- read, get_database_state_expectations, already runs on mcp's SELECT on all of config. Nothing wider, and no sibling table.
GRANT INSERT, UPDATE, DELETE ON {config}.database_state_expected TO {web};
GRANT INSERT, UPDATE, DELETE ON {config}.database_state_expected TO {mcp};

-- 8. Alert tuning (the MCP alert-tuning write tools): the mcp role's alert-config writes, mirroring section 7's
--    custom_views grant model (EXPLICIT single-table statements, NO ALTER DEFAULT PRIVILEGES). update_alert_settings
--    / create_mute_rule / update_mute_rule / delete_mute_rule / set_mute_rule_enabled let a token-holder tune the SAME alert engine the Viewer's Settings
--    window drives: INSERT/UPDATE/DELETE on config_mute_rules (the mute rules the delivery paths honor) and UPDATE
--    on the SINGLETON config_alert_settings row (id=1 -- UPDATE only, never INSERT/DELETE: the row is a fixed
--    singleton the service seeds). Still NARROW -- never the config_command service-credential pivot, a
--    schema-wide config write, or any SECRET column (the default channels' secrets are written only through the SECURITY
--    DEFINER set_notification_channels function, #6392, served only under mcp.allowWrites with a token): the one
--    config_notification column write is a single column
--    (see #3314 below), and the monitored-servers credential column stays SELECT-carved.
--    The beacon caveat: a config_alert_settings write fires the existing statement-level bump trigger
--    (trg_bump_alert_settings -> config_bump_version), which UPDATEs config_service.config_version AS THE CURRENT
--    ROLE (the trigger function is SECURITY INVOKER). So mcp ALSO needs UPDATE on JUST the two beacon columns of
--    config_service, or every update_alert_settings write would fail 42501 in production -- and the superuser-run
--    gated-live tests would never catch it (they connect as the owner). A COLUMN-level grant lets mcp bump the
--    reload beacon but NOT flip paused / mcp_enabled / mcp_port (the five flags update_service_flags writes, capture_plans among them, are a separate column grant below). The targets exist here because
--    provisioning runs AFTER migration; a recreated table re-grants on the next start (self-heal).
--    EVERY mcp-writable beacon-triggered table rests on that one grant, not the alert-settings path alone:
--    config_mute_rules carries trg_bump_mute_rules (V117 / #3315) and config_monitored_servers carries
--    trg_bump_monitored_servers (section 9), so create_mute_rule / update_mute_rule / delete_mute_rule / set_mute_rule_enabled and add_servers /
--    remove_server bump the beacon as mcp too. Narrowing the grant to update_alert_settings 42501s all of
--    them -- one column UPDATE serves every trigger. Re-derive the set from which mcp-writable tables carry a
--    bump trigger rather than from a count recorded here, which a later rung ages out without changing it.
GRANT INSERT, UPDATE, DELETE ON {config}.config_mute_rules TO {mcp};
GRANT UPDATE ON {config}.config_alert_settings TO {mcp};
GRANT UPDATE (config_version, updated_at) ON {config}.config_service TO {mcp};
-- #6230: pause_collection / resume_collection queue a pause or resume command through {config}.request_mcp_command, so mcp holds no
--     write on the paused column. An earlier build granted it; this revoke takes it back from an upgraded store.
REVOKE UPDATE (paused) ON {config}.config_service FROM {mcp};
-- #6393: update_service_flags runs as mcp and sets exactly these five service flags (plan capture, Query Store backfill, text budget, max concurrent
--    sweeps, fetch top N): a column list, so mcp still cannot write mcp_enabled, mcp_port, web_enabled, web_port or paused (turning MCP off from MCP
--    would lock the admin out; those stay Viewer-only). The write fires the config_service self-bump trigger (BEFORE UPDATE, it assigns NEW.config_version and NEW.updated_at), so it needs no further grant.
GRANT UPDATE (capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n) ON {config}.config_service TO {mcp};
-- #5085: the fleet server-tag write tools (create/update/delete/assign/unassign_server_tag) run as mcp. The admin
--    gate is these two single-table grants and nothing else: a role without them gets 42501 on a tag write.
--    Both tables are non-secret and server_tags.id is GENERATED ALWAYS AS IDENTITY (so no sequence grant). V181 (#5750)
--    put a statement-level reload-beacon trigger on each, so a tag write bumps config_service; the config_service column
--    grant above covers that. Provisioning re-runs every start, so
--    no migration rung carries this.
GRANT INSERT, UPDATE, DELETE ON {config}.server_tags TO {mcp};
GRANT INSERT, UPDATE, DELETE ON {config}.server_tag_map TO {mcp};
-- #5977: the query-tag tables (V192). mcp writes the three tables a person edits: the tags, the hand assignments and the
--    rules. The map and the sightings are the service's (the owner writes them), so mcp holds only DELETE on the map, which
--    remove_server runs to clear a removed server's map rows (a server_id is a hash, so orphaned rows would come back when the
--    server is re-added). mcp holds no write on any collect table, the sightings included: like every other collect table,
--    a removed server's sightings are left to the owner's retention pass (DarlingRetention). The identity
--    columns need no sequence grant. The rules_version bump on a write is a SECURITY
--    DEFINER trigger function (V192), so no grant on config.query_tag_state is needed or given. Provisioning re-runs every
--    start, so no migration rung carries this.
GRANT INSERT, UPDATE, DELETE ON {config}.query_tags TO {mcp};
GRANT INSERT, UPDATE, DELETE ON {config}.query_tag_assignments TO {mcp};
GRANT INSERT, UPDATE, DELETE ON {config}.query_tag_rules TO {mcp};
GRANT DELETE ON {config}.query_tag_map TO {mcp};
-- #6227: the collector schedule tools (disable_collector, enable_collector, set_collector_frequency, set_collector_run_at,
--    set_collector_retention, and #6396's set_collector_databases and reset_collector_schedules) run as mcp. They write the two non-secret schedule tables the Viewer and the --enable-collector /
--    --set-collector-run-at verbs write: the sparse override rows (enabled, frequency, retention, databases) and the run times (V160).
--    Both carry a statement-level reload-beacon trigger (INVOKER) that UPDATEs config_service.config_version as the writer, which the
--    two-column config_service grant above covers; the tables are SELECT-able by mcp already. The tools are served only with
--    mcp.allowWrites and a token, and are never offered to an investigation agent. Provisioning re-runs every start, so no migration
--    rung carries this.
--    The grants are by column, the narrowest the tools' statements need: the override rows take an INSERT of the six columns the tools
--    set and an UPDATE of the four values they change (#6396 added the database scope, for set_collector_databases; the DELETE is #6454's,
--    below, and also serves reset_collector_schedules, as the run-time DELETE does). The run times take their one value column and a DELETE (set_collector_run_at
--    deletes the row for default). The table-level grants an earlier provisioning gave are revoked first, so a re-run narrows them.
REVOKE INSERT, UPDATE, DELETE ON {config}.config_collector_schedules FROM {mcp};
GRANT INSERT (server_id, collector_name, enabled, frequency_minutes, retention_days, databases) ON {config}.config_collector_schedules TO {mcp};
GRANT UPDATE (enabled, frequency_minutes, retention_days, databases) ON {config}.config_collector_schedules TO {mcp};
-- #6454: remove_server clears a removed server's override rows (a set-and-cleared row stayed behind all-null, and a set one would come
--    back when the server is re-added), so mcp holds DELETE on the override table. remove_server runs it by server_id, and
--    reset_collector_schedules (#6396) by server_id, or at the fleet scope with server_id IS NULL.
GRANT DELETE ON {config}.config_collector_schedules TO {mcp};
REVOKE INSERT, UPDATE, DELETE ON {config}.config_collector_run_times FROM {mcp};
GRANT INSERT (server_id, collector_name, run_at_minute) ON {config}.config_collector_run_times TO {mcp};
GRANT UPDATE (run_at_minute) ON {config}.config_collector_run_times TO {mcp};
GRANT DELETE ON {config}.config_collector_run_times TO {mcp};
-- #3450: the web dashboard's dedicated mute-rule endpoints (POST/PATCH/PUT/DELETE under /api/mute-rules) run
--    as the least-privilege viewer role -- the web host's ONLY store identity -- so viewer gets the SAME
--    single-table config_mute_rules write mcp holds above, the shape of the custom_views/custom_alert_rules
--    pairs in section 7. The SEAT model, not this grant, decides who may call the endpoints (an OIDC viewer
--    seat is refused every unsafe method by the host's write gate); this is only the narrow floor beneath that
--    gate. The section-8 beacon caveat applies verbatim: config_mute_rules carries trg_bump_mute_rules ->
--    config_bump_version (SECURITY INVOKER), which UPDATEs config_service.config_version AS viewer, so viewer
--    needs the same two-column config_service grant mcp has -- and no more (paused / capture_plans / mcp_port
--    stay out of reach; the column grant serves the trigger, never a service flag). One consequence is owned
--    where it bites: the WPF Viewer's read-only probe used to ask has_table_privilege on exactly this table's
--    INSERT, which this grant would have flipped to ''writable'' for a connectAs = ''viewer'' seat whose
--    alert-dismiss writes still 42501 -- the probe now discriminates on config_alert_log UPDATE
--    (ViewerDataService.ReadOnlyProbeSql), a write only admin/owner hold, so the locked-down Viewer's
--    read-only UX is unchanged by this grant.
GRANT INSERT, UPDATE, DELETE ON {config}.config_mute_rules TO {web};
GRANT UPDATE (config_version, updated_at) ON {config}.config_service TO {web};
-- #4843: the web dashboard's Alert History dismiss route (POST /api/alert-history/dismiss) runs as viewer, the web
--    host's ONLY store identity, and its write is `UPDATE config_alert_log SET dismissed = TRUE` (AlertDismissStore,
--    the one copy the Viewer's Dismiss Selected / Dismiss All share). The grant is COLUMN-level on exactly that one
--    column: viewer can flip the dismissed flag and cannot rewrite what an alert said (alert_time, server_id,
--    metric_name, the values, send_error, muted, detail_text, context_json) or reach INSERT/DELETE. config_alert_log
--    carries no trigger, so nothing else is written as the invoking role. The seat model, not this grant, decides who
--    may call the route (a read-only OIDC seat is refused every unsafe method by the host's write gate). The WPF
--    read-only probe is unaffected: has_table_privilege(..., 'UPDATE') answers for the TABLE-level privilege only
--    (the column-level form is has_column_privilege), so it stays false for viewer and the locked-down Viewer's
--    dismiss buttons stay hidden. mcp gets the same one-column grant just below (#6231).
GRANT UPDATE (dismissed) ON {config}.config_alert_log TO {web};
-- #6231: the MCP dismiss_alert_history / restore_alert_history tools run as mcp and set the SAME one column through
--    AlertDismissStore, so mcp gets the same column-level grant (revoke first, so a table-level UPDATE from an older
--    provisioning run cannot linger). It names no other column and no INSERT/DELETE: mcp can flip the dismissed
--    flag and cannot rewrite what an alert said. The store records only that flag, not who set it.
REVOKE UPDATE ON {config}.config_alert_log FROM {mcp};
GRANT UPDATE (dismissed) ON {config}.config_alert_log TO {mcp};
-- #5085: the web dashboard's server-tag endpoints (POST/PATCH/DELETE under /api/server-tags) run as viewer, so it
--    gets the SAME two single-table writes mcp holds above. The seat model decides who may call them (a read-only
--    seat is refused every unsafe method); these grants are only the floor beneath that gate. Both tables are
--    non-secret. V181 (#5750) put a reload-beacon trigger on each, covered by the viewer's config_service column grant. The WPF Viewer's
--    read-only probe is unchanged: it discriminates on config_alert_log UPDATE (ViewerDataService.ReadOnlyProbeSql),
--    which these grants do not touch, so a read-only desktop seat still reads as read-only and its tag editors
--    stay disabled. A server removal also clears that server's server_tag_map rows, which needs the DELETE here.
GRANT INSERT, UPDATE, DELETE ON {config}.server_tags TO {web};
GRANT INSERT, UPDATE, DELETE ON {config}.server_tag_map TO {web};
-- #5977: server removal clears that server's query-tag rows (assignments, rules, map), so viewer holds DELETE on
--    those three. #5986: the web page that manages query tags (POST/PATCH/DELETE under /api/query-tags and
--    /api/query-tag-rules) runs the SAME cores the MCP tools run, as viewer, so viewer also gets the three writes mcp holds
--    above: INSERT, UPDATE and DELETE on the tags, the hand assignments and the rules. The seat model decides who may call
--    the routes (a read-only seat is refused every unsafe method); these grants are only the floor beneath that gate. The
--    map stays DELETE only (the service's map builder writes it), and nobody but the owner writes the sightings or the
--    state row: the rules_version bump is a SECURITY DEFINER trigger function, so no grant on config.query_tag_state. Admin
--    (the desktop Viewer's default identity) already holds the config writes through the blanket grant above.
GRANT INSERT, UPDATE, DELETE ON {config}.query_tags TO {web};
GRANT INSERT, UPDATE, DELETE ON {config}.query_tag_assignments TO {web};
GRANT INSERT, UPDATE, DELETE ON {config}.query_tag_rules TO {web};
GRANT DELETE ON {config}.query_tag_map TO {web};
-- #4843: the web dashboard's add-server route (POST /api/servers) runs the SAME add_servers core the MCP tool runs, as
--    viewer, so viewer gets the INSERT that core issues on config_monitored_servers; no DELETE, because no web route
--    removes a server yet. Viewer holds no UPDATE on the table: see the #5240 note right below.
--    The credential column stays out of reach in both directions that matter here. encrypted_password is still
--    SELECT-carved from viewer (section 6), so viewer can WRITE a password blob and never READ one back; and the
--    cores need no carved read to write: the INSERT names no RETURNING, its ON CONFLICT (server_id) DO NOTHING arbiter
--    reads only server_id, and the dedupe and by-id lookups SELECT the five non-secret identity columns viewer
--    already holds. The BEACON is covered by the two-column config_service grant above: the write fires
--    trg_bump_monitored_servers -> config_bump_version (SECURITY INVOKER), which UPDATEs config_service.config_version
--    AS viewer. The seat model, not this grant, decides who may call the route (a read-only seat is refused every
--    unsafe method); the grant is only the floor beneath that gate. The WPF Viewer's read-only probe is unchanged: it
--    discriminates on config_alert_log UPDATE (ViewerDataService.ReadOnlyProbeSql), which this grant does not touch.
GRANT INSERT ON {config}.config_monitored_servers TO {web};
-- #5240: the web dashboard's edit route (PATCH /api/servers/{{id}}) and the MCP edit_server tool change a monitored
--    server through {config}.edit_monitored_server, a SECURITY DEFINER function granted to viewer and mcp in section 10.
--    Viewer holds NO UPDATE on this table, column-level or otherwise: the function checks the optimistic token, works
--    out for itself whether host or port moves, and refuses a move that keeps the stored secret on a SQL or
--    service-principal row, so a session holding the viewer login cannot send a stored secret to a host it picks.
--    REVOKE, not merely an absent grant: a store that ran a build with a column-level UPDATE keeps it until this
--    statement takes it away, and revoking the table privilege also revokes every column privilege on it, so each run
--    leaves viewer with no UPDATE at all (the same shape as the section-6 read carve). A plain REVOKE removes only the
--    privileges recorded as granted by the role that issues it (a superuser or the owner counts as the owner); this
--    batch grants and revokes as that same owner. The write fires trg_bump_monitored_servers inside the function, as
--    the owner. Idempotent, applied on every managed startup: not a migration rung.
REVOKE UPDATE ON {config}.config_monitored_servers FROM {viewer}, {web};
-- #3314: the DELIVERY cooldown -- the sole throttle on a Slack/Teams/PagerDuty/webhook post -- is the one
-- alert-engine knob stored on config_notification rather than config_alert_settings, so update_alert_settings
-- spans two tables and needs a write here. This DOES widen mcp into a table holding bearer secrets (the SMTP
-- password blob, the Teams/Slack/generic webhook URLs, the PagerDuty routing key), so the grant is
-- COLUMN-level on exactly that one column -- the same shape as the config_service beacon grant above and for
-- the same reason. MEASURED, not assumed: as mcp, the baseline UPDATE raises 42501; with this grant it
-- succeeds; with the column SELECT revoked and this grant kept it STILL succeeds (so UPDATE is the privilege
-- doing the work, not an ambient SELECT); and a write to smtp_encrypted_password, slack_url or even the
-- non-secret sibling smtp_host stays 42501. A missing SELECT and a missing UPDATE both raise the identical
-- 42501 permission-denied-for-table-config_notification message, so only isolating the grants separates them.
-- The READ side needs nothing: email_cooldown_minutes is already in the section-6 non-secret column carve.
-- The BEACON is already covered: config_notification carries trg_bump_notification -> config_bump_version
-- (SECURITY INVOKER), which UPDATEs config_service.config_version AS mcp, and the column grant above serves it.
GRANT UPDATE (email_cooldown_minutes) ON {config}.config_notification TO {mcp};
-- #5825 (V187): update_alert_settings writes delivery.send_resolutions, the four per-channel resolution flags on the
-- same row. Behaviour toggles beside the cooldown, never a destination or a credential.
GRANT UPDATE (slack_send_resolutions, teams_send_resolutions, generic_send_resolutions, email_send_resolutions) ON {config}.config_notification TO {mcp};
-- #3598 (V131): notification routes. The parent row's posture is that a network token-holder never READS a
-- destination and never WRITES one either (the one config_notification write above is the cooldown column),
-- and a route IS a destination -- pointing a family's Slack at a URL of the caller's choosing would let mcp
-- redirect alert content (query text, server names) anywhere. So mcp gets exactly the two writes that move no
-- destination: DELETE (alerts fall back to the parent, which the operator configured) and UPDATE on the
-- enabled flag plus its modified_at stamp (set_notification_route_enabled / delete_notification_route), the
-- shape of set_mute_rule_enabled / delete_mute_rule. No INSERT, no destination column. (#6231: create and edit of a route's
-- non-destination fields go through the two functions in section 10, never through a grant here, and no MCP tool ever sets a
-- destination.) The READ side is the
-- section-6 carve: route_id, metric_match, enabled, smtp_recipients and the GENERATED configured_channels
-- presence column, never the URLs. The BEACON is covered: trg_bump_notification_routes -> config_bump_version
-- (SECURITY INVOKER) UPDATEs config_service.config_version AS mcp, which the two-column grant above serves.
GRANT UPDATE (enabled, modified_at), DELETE ON {config}.config_notification_routes TO {mcp};
-- #6036 (V193): set_notification_route_fallback writes the fallback route id, a pointer to another route (never a
-- destination). Guarded on the column, so a store below V193 still commits the rest; delete_notification_route clears
-- the references to the deleted route with the same grant.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('{config}.config_notification_routes')
                AND attname = 'fallback_route_id' AND NOT attisdropped) THEN
      EXECUTE $v193f$
GRANT UPDATE (fallback_route_id) ON {config}.config_notification_routes TO {mcp};
$v193f$;
   END IF;
END $$;

-- 9. Server onboarding (the MCP server-admin write tools): the mcp role's monitored-server writes, mirroring
--    sections 7/8's model (an EXPLICIT single-table statement, NO ALTER DEFAULT PRIVILEGES). add_servers /
--    remove_server let a token-holder add or remove monitored servers in the SAME central store the Viewer's
--    Add / Manage-Servers dialogs write: INSERT/UPDATE/DELETE on config_monitored_servers. Still NARROW -- a
--    single non-secret-KEY table (the encrypted_password column is SELECT-carved from mcp by the section-6
--    secret-column ACL above, so mcp can WRITE a credential blob but never READ one back), never the
--    config_command service-credential pivot or a schema-wide config write. The BEACON is already covered: a
--    config_monitored_servers write fires trg_bump_monitored_servers -> config_bump_version (SECURITY INVOKER),
--    which UPDATEs config_service.config_version AS mcp, and section 8 already granted mcp
--    UPDATE (config_version, updated_at) ON config_service -- so no additional config_service grant is needed here.
GRANT INSERT, DELETE ON {config}.config_monitored_servers TO {mcp};
REVOKE UPDATE ON {config}.config_monitored_servers FROM {mcp};
-- The role and external ID of a server are set only through the edit function (and an INSERT): mcp's UPDATE
-- names every other column of the table and leaves out aws_role_arn, aws_external_id and the generated
-- aws_external_id_set. A column added to the table later is added to this list.
GRANT UPDATE ({McpMonitoredServerUpdateColumns})
   ON {config}.config_monitored_servers TO {mcp};

-- 10. Custom-alert resolve-on-delete privileged write (#3334). The recovery/resolution row a caller-initiated
--     rule DELETE writes (CustomAlertEvaluator.WriteTeardownResolutionAsync) lands in config.config_alert_log,
--     which ONLY admin/owner may INSERT (section 3's schema-wide grant). But that delete runs as the
--     least-privilege caller -- mcp (the delete_custom_alert_rule tool) or viewer (DELETE /api/alerts) -- so a
--     direct INSERT is permission-denied, the write is failure-isolated, and the resolution row #3305 intends
--     was SILENTLY dropped, leaving a deleted firing rule showing ""open"" in history forever. Rather than a
--     blanket INSERT grant on the history table to viewer/mcp (which would let those roles fabricate ARBITRARY
--     history rows, including fake fires), a SECURITY DEFINER function confines the privileged write to EXACTLY
--     a no-channel resolution row: every delivery-shape column is HARDCODED (alert_sent false, notification_type
--     'none', current/threshold 0, muted false, no send_error/context) -- the same zeroed/unmuted resolution
--     shape BuildResolutionRecord + PgAlertHistoryStore.RecordAlertAsync write, but pinned to a NO-CHANNEL row
--     rather than a natural clear's 'tray' (a teardown surfaces no operator notification) -- so a caller can
--     page nothing and fabricate no fire; only server_id/name and the already-sanitized title/detail vary.
--     Definer-safe: owned by the store owner (the creating provisioning role, {owner}), an explicit pinned
--     search_path with pg_temp last and the table named by its schema, so no temporary object can redirect the config_alert_log write or now(), and a fully
--     parameterized INSERT with NO dynamic SQL. Created + REVOKEd-from-PUBLIC by the shared builder below;
--     EXECUTE is the only privilege the least-privilege roles get, and admin/owner keep their direct INSERT and
--     never call it. NOT a versioned migration: CREATE OR REPLACE is idempotent and owner-run every start and
--     has no probeable schema footprint (the section-1c role-SET rationale), so a V-number would drag in the
--     probe rung + ladder fixture + version-pin tests for no gain.
{BuildCustomAlertResolveFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.record_custom_alert_resolution(integer, text, text, text) TO {web}, {mcp};
-- #6334: the known-pattern entries' writes. The web editor (viewer) and the Viewer (admin) write config.config_known_patterns only
--     through these three functions, and the MCP create_known_pattern / update_known_pattern / delete_known_pattern tools (#6398) call the same
--     functions as mcp; no role holds a direct INSERT/UPDATE/DELETE on the table. Owned by the store owner, pinned search_path, no dynamic SQL; created and
--     REVOKEd from PUBLIC by the shared builder. The reads need no grant: the schema-wide SELECT covers the table. The beacon trigger runs as
--     the function's owner, so no config_service grant is needed either. NOT a versioned migration, for the same reason as the edit function.
{BuildKnownPatternFunctionsSql(config)}
GRANT EXECUTE ON FUNCTION {config}.insert_known_pattern({InsertKnownPatternSignature}) TO {admin}, {web};
GRANT EXECUTE ON FUNCTION {config}.insert_known_pattern({InsertKnownPatternSignature}) TO {mcp};
GRANT EXECUTE ON FUNCTION {config}.update_known_pattern({UpdateKnownPatternSignature}) TO {admin}, {web};
GRANT EXECUTE ON FUNCTION {config}.update_known_pattern({UpdateKnownPatternSignature}) TO {mcp};
GRANT EXECUTE ON FUNCTION {config}.delete_known_pattern(bigint) TO {admin}, {web};
GRANT EXECUTE ON FUNCTION {config}.delete_known_pattern(bigint) TO {mcp};
-- #5240: the edit write. The web edit route (viewer) and the MCP edit_server tool (mcp) call
--     {config}.edit_monitored_server for their one UPDATE of a config_monitored_servers row. It locks the row, checks
--     the expected modified_at, and refuses a change of host or port that keeps the stored secret on a SQL or
--     service-principal row (the caller must send a new secret with it). Owned by the store owner, pinned
--     search_path, one fixed UPDATE with no dynamic SQL; created and REVOKEd from PUBLIC by the shared builder
--     below, and EXECUTE is the only privilege viewer and mcp get for this write. NOT a versioned migration:
--     CREATE OR REPLACE is idempotent and owner-run every start. A self-managed store gets the same function from
--     tools/provision-roles.sql, and is told to re-run it when the function is missing.
{BuildEditMonitoredServerFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerSignature}) TO {web}, {mcp};
-- #6254: the 17-argument edit function stays as a permanent wrapper over the 18-argument one, for a caller built before the Query
--     Store replica column. It passes NULL for the replica and strips the column name from p_columns, so a caller of the older
--     signature can neither change nor clear a replica. Same grants, same owner.
{BuildEditMonitoredServerSeventeenWrapperSql(config)}
GRANT EXECUTE ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerSeventeenSignature}) TO {web}, {mcp};
-- #5452: the 15-argument edit function stays as a permanent wrapper over the 17-argument one, for a caller that
--     was built before the AWS role columns. It passes NULL for both and strips the two AWS column names from
--     p_columns, so a caller of the old signature can neither change nor clear a role. Same grants, same owner.
{BuildEditMonitoredServerLegacyWrapperSql(config)}
GRANT EXECUTE ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerLegacySignature}) TO {web}, {mcp};
-- #5941: the run-collectors-now request. The web route (viewer) and the MCP run_collectors_now tool (mcp) queue a
--     snapshot_now command through {config}.request_snapshot_now, the only way those two roles write a command row.
--     The command type is fixed in the body; the caller supplies a server id and an optional list of collector names
--     and nothing else, so it cannot reach the service-credential pivot that test_connect's args_json is. Owned by
--     the store owner, pinned search_path, no dynamic SQL; created and REVOKEd from PUBLIC by the shared builder.
--     NOT a versioned migration, for the same reason as the edit function above.
{BuildRequestSnapshotNowFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.request_snapshot_now(integer, text, text) TO {web}, {mcp};
-- #6386: the web's Run live now button on the Active Queries panel. The web route (viewer) queues a fetch_active_queries command
--     through {config}.request_active_queries_live, the only way viewer writes that command row. The command type is fixed in
--     the body and the caller supplies a server id and nothing else, so it cannot reach the service-credential pivot that
--     test_connect's args_json is. Owned by the store owner, pinned search_path, no dynamic SQL; created and REVOKEd from
--     PUBLIC by the builder. NOT a versioned migration, for the same reason as request_snapshot_now above.
{BuildRequestActiveQueriesLiveFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.request_active_queries_live(integer, text) TO {web};
-- #6389: the web's Generate now button on the Recommendations tab. The web route (viewer) queues an analyze_now command
--     through {config}.request_analyze_now, the only way viewer writes that command row. The command type is fixed in the body
--     and the caller supplies a server id and a requested_by label (the label is not checked: the web host and a locked-down
--     Viewer share the viewer login, and the web's log line is the record of who asked). Owned by the store owner, pinned search_path, no dynamic SQL; created
--     and REVOKEd from PUBLIC by the builder. NOT a versioned migration, for the same reason as request_snapshot_now above.
{BuildRequestAnalyzeNowFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.request_analyze_now(integer, text) TO {web};
-- #6231: the MCP route tools. mcp holds no INSERT on {config}.config_notification_routes and no SELECT or UPDATE on its
--     destination columns, and gets none here: create_notification_route and update_notification_route reach the table only
--     through these functions. create leaves every channel empty and update writes the match, scope, enabled flag, resolution
--     override and fallback (the match or scope only on a route with no explicit agent setting); neither takes a destination.
--     set_notification_route_destinations is the one function that sets a route's own destinations: the service seals each
--     webhook value for the route's own row first and passes it in, and the function stores only an empty value or a sealed one
--     (the address list is plain text). All three re-read the row FOR UPDATE and refuse an agent route inside the transaction,
--     unless the actor the service passes is an operator-class token (#6542 part 2). Granted to mcp only.
{BuildNotificationRouteFunctionsSql(config)}
GRANT EXECUTE ON FUNCTION {config}.create_notification_route(text, boolean, integer, integer, boolean, integer, text) TO {mcp};
GRANT EXECUTE ON FUNCTION {config}.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer, text) TO {mcp};
GRANT EXECUTE ON FUNCTION {config}.set_notification_route_destinations(integer, text, text[], text, text, text, text, text) TO {mcp};
GRANT EXECUTE ON FUNCTION {config}.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean, jsonb) TO {mcp};
-- #6394: the MCP generate_webhook_signing_key and remove_webhook_signing_key tools. mcp holds no UPDATE on a signing key column of {config}.config_notification
--     or {config}.config_notification_routes (they are secret columns) and no SELECT on them, and gets none here. The service reads the saved (sealed) URL
--     and proxy a new key is sealed for through its own store connection, so there is no function that returns them to mcp (an earlier build of this
--     change made read_webhook_signing_target; it is dropped here). set_webhook_signing_key stores a key the service sealed (only the shape of one:
--     sealed:v1:, the 16 character key id, base64) and moves the current key to the previous slot with the 7 day window the Viewer uses, and
--     clear_webhook_signing_key clears the key, the previous key and the window. Neither returns a key. Granted to mcp only.
{BuildWebhookSigningKeyFunctionsSql(config)}
GRANT EXECUTE ON FUNCTION {config}.set_webhook_signing_key(integer, text, text, text) TO {mcp};
GRANT EXECUTE ON FUNCTION {config}.clear_webhook_signing_key(integer) TO {mcp};
-- #6392: the MCP update_notification_channels tool. mcp holds no UPDATE on any secret column of {config}.config_notification (the SMTP
--     user name and password, the Teams, Slack and generic webhook URLs, the generic headers, the PagerDuty routing key) and no SELECT
--     on them, and gets none here: set_notification_channels is the one way it writes the default channels. The service seals each
--     secret first (the SMTP password for its host, port, SSL flag and user name; a webhook value for its slot, proxy and, for the
--     headers, the generic URL), and the function stores only an empty value or a sealed one, reads the row FOR UPDATE and makes the
--     Viewer's refusals (a change to what a saved secret is tied to needs the secret again, a signing key blocks a URL or proxy change).
--     It returns only whether each secret named in the call was saved before. The tool is served only under mcp.allowWrites with a token.
--     Granted to mcp only.
{BuildNotificationChannelsFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.set_notification_channels(text[], text, integer, boolean, text, text, text, text, text, text, text, text, text, text, text, text, text, boolean, text, boolean) TO {mcp};
-- #6230: the MCP incident tools (analyze now, purge now, test a connection, pause, resume) queue their command through
--     {config}.request_mcp_command, the only way the mcp role writes a command row. It names five command types and refuses any
--     other, builds a connection test from the stored server row (no credential or connection string is an argument), and
--     is granted to mcp only. Owned by the store owner, pinned search_path, no dynamic SQL. NOT a versioned migration.
{BuildRequestMcpCommandFunctionSql(config)}
REVOKE ALL ON FUNCTION {config}.request_mcp_command(text, integer, integer, text) FROM {mcp};
GRANT EXECUTE ON FUNCTION {config}.request_mcp_command(text, integer, integer, text) TO {mcp};
-- #6230: mcp holds no SELECT on config_command.result_json; it reads a command's status and result through
--     {config}.read_mcp_command_result, which answers only for the command types the MCP tools queue. Owned by the store
--     owner, pinned search_path, no dynamic SQL, granted to mcp only. NOT a versioned migration.
{BuildReadMcpCommandResultFunctionSql(config)}
REVOKE ALL ON FUNCTION {config}.read_mcp_command_result(bigint) FROM {mcp};
GRANT EXECUTE ON FUNCTION {config}.read_mcp_command_result(bigint) TO {mcp};
-- The store's own password rules for every role but the owner (see BuildServerPasswordRulesSql): a trigger on
--     config_monitored_servers that refuses a password reference (env: or file:) and a move that keeps the stored
--     password. Created here, not in a migration: CREATE OR REPLACE is idempotent and owner-run every start. A
--     self-managed store gets the same function and trigger from tools/provision-roles.sql.
{BuildServerPasswordRulesSql(config)}

-- 10b. External alert submit (#5654): another service raises an alert through Darling's channels, routes and mute rules
--     by calling {config}.submit_external_alert, as its OWN login (one login per source, created by the add-alert-source
--     verb). The function is the only way in: a source login holds no table privilege anywhere. It looks the caller
--     up by session_user in {config}.external_alert_source, so the source name comes from the login the connection
--     was opened as and there is no argument that names it; session_user is used rather than current_user because
--     inside a SECURITY DEFINER function current_user is the owner. A non-superuser cannot change session_user, and
--     SET ROLE moves only current_user, so a source cannot record an alert under another source's name. The
--     limits sit in the function: severity list, key/title/detail/server-name caps, a per-source rate per minute
--     (DL429) and a per-source cap on rows waiting for delivery (DL429), a fixed 15 minute expiry. Same pattern as
--     the section 10 function: owned by the store owner, pinned search_path with pg_temp last, every table named by
--     its schema, fully parameterized with NO dynamic SQL, created and REVOKEd from PUBLIC by the shared builder,
--     rebuilt on every start. NOT a versioned migration, for the section 10 reason. EXECUTE goes to the registered
--     source logins only (section 10c); the three Darling roles hold none.
{BuildExternalAlertSubmitFunctionSql(config)}

-- 10c. External alert tables and source logins (#5654). The three tables (V180) are written only by the store owner and
--     by the section 10b function. The blanket grants above gave admin INSERT/UPDATE/DELETE on every config table (section
--     3 and its default privileges), and with that admin could insert a pending row under any source, so the same
--     REVOKE ALL ... CASCADE as 10a takes every privilege from PUBLIC and the three roles, and SELECT is granted back
--     to the three roles (an operator reads the queue and the registry). It runs after every blanket grant above, and before 10a so that the password key tables' revoke stays the last
--     grant-side statement of the batch.
--     Then, for each registered source login that carries the '{AlertSourceRoleMarker}' marker: every attribute that widens
--     a role off, NOINHERIT, every role membership revoked, then membership in the '{AlertSourceGroupRoleName}' group granted
--     back (WITH INHERIT FALSE, SET FALSE from PostgreSQL 16), and exactly three grants -- CONNECT on this database, USAGE on
--     the {config} schema, EXECUTE on the 10b function. A registry row naming a role with no marker, or no role at all, is
--     skipped with a WARNING; a skipped role that exists loses EXECUTE on the 10b function.
--     The group is a NOLOGIN role marked '{AlertSourceGroupRoleMarker}' that holds nothing (an unmarked role of that name stops
--     the batch, the section 1 rule): it exists so the pg_hba line written for postgres.network.role ""sources"" names one
--     role. Every member that is not a registered, marked source login is revoked from it.
REVOKE ALL ON {config}.external_alert_source, {config}.external_alert_pending, {config}.external_alert_state
   FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
GRANT SELECT ON {config}.external_alert_source, {config}.external_alert_pending, {config}.external_alert_state
   TO {admin}, {viewer}, {web};
GRANT SELECT ON {config}.external_alert_source, {config}.external_alert_pending, {config}.external_alert_state
   TO {mcp};
REVOKE ALL ON FUNCTION {config}.submit_external_alert({ExternalAlertSubmitSignature}) FROM {admin}, {viewer}, {web}, {mcp};
REVOKE EXECUTE ON FUNCTION {config}.config_bump_version(), {config}.config_service_bump() FROM PUBLIC;
{BuildExternalAlertSourceRolesSql(config)}

-- 10d. Investigation tables (V184, #5632). The blanket config grants above gave admin INSERT/UPDATE/DELETE on all
--     six, so REVOKE ALL ... CASCADE takes every privilege from PUBLIC and the three roles, and SELECT goes back to
--     all three (the web dashboard and the MCP list read them). Then each writer gets only what it writes. The
--     service opens, joins, sweeps and journals as the owner. mcp (the claim and submit tools) updates only the
--     claim and report columns of a request, inserts a report and a journal row, and proposes a note. The agent
--     tools (claim, submit) leave the note's state column to its default, pending, but the database does not hold mcp to
--     that: since #6400 mcp also holds INSERT on state, because the person-only investigation tools (write, approve and reject a
--     note, close a request, save a profile) run as mcp. What keeps an agent from them is the tool policy (NeverAgentTools), not this role. admin (the WPF viewer) closes a
--     request, writes the environment profiles, writes, decides or deletes a note, and journals what it did.
--     Nothing but the owner writes the alert links (but for the Viewer's practice copy, section 10e). An identity column needs no sequence grant. One table per write
--     grant, so provision-roles.sql's mirror (its step 3i) is compared grant for grant. It runs after every blanket
--     grant above, and before 10a.
REVOKE ALL ON {config}.investigation_requests, {config}.investigation_request_alerts, {config}.investigation_reports,
   {config}.investigation_journal, {config}.investigation_notes, {config}.agent_environment_profiles
   FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
GRANT SELECT ON {config}.investigation_requests, {config}.investigation_request_alerts, {config}.investigation_reports,
   {config}.investigation_journal, {config}.investigation_notes, {config}.agent_environment_profiles
   TO {admin}, {viewer}, {web};
GRANT SELECT ON {config}.investigation_requests, {config}.investigation_request_alerts, {config}.investigation_reports,
   {config}.investigation_journal, {config}.investigation_notes, {config}.agent_environment_profiles
   TO {mcp};
GRANT UPDATE (state, claimed_by, claim_id, claimed_at, lease_until, claim_count, brief_format, brief, reported_at)
   ON {config}.investigation_requests TO {mcp};
-- #6008 (V193): submit_investigation_report writes follow_up_at onto the report's own request. Guarded on the column,
-- as the #6036 fallback grant is, so a store below V193 still commits the rest; provision-roles.sql step 3i mirrors it.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('{config}.investigation_requests')
                AND attname = 'follow_up_at' AND NOT attisdropped) THEN
      EXECUTE $v193u$
GRANT UPDATE (follow_up_at) ON {config}.investigation_requests TO {mcp};
$v193u$;
   END IF;
END $$;
-- #6009 (V193): the deploy condition beside it, guarded the same way; provision-roles.sql step 3i mirrors it.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('{config}.investigation_requests')
                AND attname = 'follow_up_condition' AND NOT attisdropped) THEN
      EXECUTE $v193c$
GRANT UPDATE (follow_up_condition) ON {config}.investigation_requests TO {mcp};
$v193c$;
   END IF;
END $$;
GRANT INSERT ON {config}.investigation_reports TO {mcp};
GRANT INSERT ({JournalWriterColumns}) ON {config}.investigation_journal TO {mcp};
GRANT INSERT (server_id, story_path_hash, dedup_key, metric_name, note_text, author, source_request_id)
   ON {config}.investigation_notes TO {mcp};
GRANT UPDATE (state, closed_at, closed_by, closed_reason) ON {config}.investigation_requests TO {admin};
GRANT INSERT ({JournalWriterColumns}) ON {config}.investigation_journal TO {admin};
GRANT INSERT, DELETE ON {config}.investigation_notes TO {admin};
GRANT UPDATE (state, decided_at, decided_by, expires_at) ON {config}.investigation_notes TO {admin};
-- #6400: the four investigation admin tools (write_investigation_note, approve_investigation_note, reject_investigation_note,
--     close_investigation_request) run the same store code the web routes and the Viewer run, as mcp. They need the columns admin's
--     statements set and nothing more: close stamps the three close columns beside the state mcp already updates; a written note is
--     inserted approved (state, the two clock columns, expires_at and the decider) beside the columns an agent's proposal sets; a
--     decision updates the three columns that set the state and the decider (admin also updates expires_at, which no
--     mcp statement does: a note's expiry is set when it is inserted). The journal INSERT is mcp's already. No DELETE on notes, no INSERT on a
--     request, no profile write: the tools do none of them. Provisioning re-runs every start, so no migration rung carries this.
GRANT UPDATE (closed_at, closed_by, closed_reason) ON {config}.investigation_requests TO {mcp};
GRANT INSERT (state, created_at, expires_at, decided_at, decided_by) ON {config}.investigation_notes TO {mcp};
GRANT UPDATE (state, decided_at, decided_by) ON {config}.investigation_notes TO {mcp};
GRANT INSERT, UPDATE, DELETE ON {config}.agent_environment_profiles TO {admin};
-- #6400: save_environment_profile (the web's PUT /api/environment-profiles) runs InvestigationRequestStore.SaveProfileAsync as mcp. The
--     statements set the five profile columns on an insert (the profile_id identity takes its default), update three of them on a
--     conflict and a blank save deletes the scope's profile; the scope lock (SELECT ... FOR UPDATE) rides on the UPDATE grant. The
--     journal INSERT is mcp's already. Provisioning re-runs every start, so no migration rung carries this.
GRANT INSERT (scope_server_id, scope_tag_id, profile_text, modified_at, modified_by) ON {config}.agent_environment_profiles TO {mcp};
GRANT UPDATE (profile_text, modified_at, modified_by) ON {config}.agent_environment_profiles TO {mcp};
GRANT DELETE ON {config}.agent_environment_profiles TO {mcp};
-- #6003 review: the journal is the event log the service signs and delivers, and mcp and admin hold INSERT on it, so a
--     trigger keeps the event rows, their delivery and fallback marks and the time order to the owner (see
--     BuildJournalEventRulesSql). Created here, not in a migration, as the password rules are; a self-managed store gets
--     the same function and trigger from tools/provision-roles.sql step 3i. The INSERT grants above name their columns and
--     leave out journal_id and event_id: a role that could set them (OVERRIDING SYSTEM VALUE) could fill the identity's
--     next million values and make every service insert fail with a duplicate key. The same trigger idea guards the alert
--     links (BuildRequestAlertLinkRulesSql): admin's INSERT there is for a practice request's copy only.
{BuildJournalEventRulesSql(config)}
{BuildRequestAlertLinkRulesSql(config)}

-- 10e. Level 2 tables (V191, #5909): the threshold overrides an agent sets and the feedback on a report. The default
--     privileges already give admin INSERT/UPDATE/DELETE and viewer SELECT on a table made after them, but the grants are
--     spelled out here, after a REVOKE ALL ... CASCADE (which also takes TRIGGER and REFERENCES), so the result does not
--     depend on when the table was made. admin (the desktop Viewer) writes both. viewer reads both and writes neither.
--     mcp (the agent's change tools) writes the overrides only, and reads the feedback. The overrides carry V191's reload
--     beacon trigger, which UPDATEs config_service.config_version as the caller, and mcp already holds the two-column
--     UPDATE grant on config_service that it needs (the mute rules use it too), so no grant moves there. An identity
--     column needs no sequence grant. The mcp write grant on the overrides is table-level on purpose, unlike 10d's column
--     list on the notes: a note's state column carries authority (a person decides it), while an override IS the agent's
--     action, taking effect when the engine reloads, so the agent sets, changes and withdraws overrides, including one a
--     person made. set_by names who set the row and is not a guard; the Level 2 loader's rules (#5910) decide which keys
--     and values the agent may write, and the table has no authority column to protect. It runs after every blanket grant
--     above, and before 10a.
REVOKE ALL ON {config}.config_threshold_overrides, {config}.investigation_feedback
   FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
GRANT SELECT ON {config}.config_threshold_overrides, {config}.investigation_feedback TO {admin}, {viewer}, {web};
GRANT SELECT ON {config}.config_threshold_overrides, {config}.investigation_feedback TO {mcp};
GRANT INSERT, UPDATE, DELETE ON {config}.config_threshold_overrides TO {admin};
GRANT INSERT, UPDATE, DELETE ON {config}.investigation_feedback TO {admin};
GRANT INSERT, UPDATE, DELETE ON {config}.config_threshold_overrides TO {mcp};
-- #6401: set_agent_actions_enabled (the MCP twin of the web's PUT /api/agent-changes/switch) updates the service row's switch and who set it.
-- UPDATE only, column-level on exactly those columns, so mcp still cannot write paused, capture_plans or any other flag, and cannot create the row:
-- the service creates it when it copies darling.json into the store, and a switch written before then must not make the store look set up.
-- agent_actions_enabled arrives with V191, the version that makes the block above run.
GRANT UPDATE (agent_actions_enabled, updated_by) ON {config}.config_service TO {mcp};
-- #5918: the Viewer opens a practice request (V191's practice_of) and copies its case's alert rows, so these two grants are here, after 10d's revoke.
GRANT INSERT (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
   window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, practice_of) ON {config}.investigation_requests TO {admin};
GRANT INSERT ON {config}.investigation_request_alerts TO {admin};
-- #6400: start_practice_request and record_investigation_feedback run InvestigationPracticeStore.OpenAsync and
--     InvestigationFeedbackStore.RecordAsync as mcp, so mcp holds what those two statements set and nothing more: the practice's request row
--     and its copied alert rows (the journal INSERT is mcp's already, and trg_investigation_request_alerts_owner_rows still refuses a
--     link to any request but a practice one), and a feedback row (the insert's six columns, and on a conflict the author's earlier
--     row is replaced: the five text columns and created_at). No DELETE on feedback, no UPDATE of a request. Provisioning re-runs
--     every start, so no migration rung carries this.
GRANT INSERT (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
   window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, practice_of) ON {config}.investigation_requests TO {mcp};
GRANT INSERT (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json, severity)
   ON {config}.investigation_request_alerts TO {mcp};
GRANT INSERT (request_id, verdict, corrected_verdict, missed_step, comment, author) ON {config}.investigation_feedback TO {mcp};
GRANT UPDATE (verdict, corrected_verdict, missed_step, comment, author, created_at) ON {config}.investigation_feedback TO {mcp};

-- 10e2. The per-server action switches (V196, #5632 step 5). Every role reads them and NO role but the store owner (the service)
--     writes them: the default privileges would give admin INSERT/UPDATE/DELETE on a table made after them, so REVOKE ALL ... CASCADE
--     takes every privilege from PUBLIC and the three roles, and SELECT goes back to all three. No MCP tool, web route or darling.json
--     key can turn a switch on. How a person turns one on is a later grant, added here and in provision-roles.sql step 3j2. It runs
--     after every blanket grant above, and before 10a.
REVOKE ALL ON {config}.config_server_actions FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
GRANT SELECT ON {config}.config_server_actions TO {admin}, {viewer}, {web};
GRANT SELECT ON {config}.config_server_actions TO {mcp};

-- 10f. Phase C tables (V193, #5997): the evidence of a request, the daily deadlock signature counts and the signature
--     merges. Only the store owner (the service) writes them. The default privileges would give admin INSERT/UPDATE/DELETE
--     on a table made after them, so REVOKE ALL ... CASCADE (which also takes TRIGGER and REFERENCES) takes every privilege
--     from PUBLIC and the three roles, and SELECT goes back to all three: the web dashboard and the MCP tools read them.
--     A later slice that lets a role write one adds that single grant here and in provision-roles.sql step 3l. The new
--     columns on existing tables need no grant here: the investigation tables' grants are per table or name only the
--     columns a role writes, and the notification tables' read carve is ViewerRestrictedConfigTables, which keeps the
--     signing keys from viewer and mcp.
--     It runs after every blanket grant above, and before 10a.
REVOKE ALL ON {config}.investigation_evidence, {config}.investigation_signature_days, {config}.investigation_signature_merges
   FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
GRANT SELECT ON {config}.investigation_evidence, {config}.investigation_signature_days, {config}.investigation_signature_merges
   TO {admin}, {viewer}, {web};
GRANT SELECT ON {config}.investigation_evidence, {config}.investigation_signature_days, {config}.investigation_signature_merges
   TO {mcp};
-- #6006: the one write a role gets here. The gated merge tool runs as mcp and adds a merge row, naming only the columns an
-- agent supplies (merge_id and merged_at are the table's own); a person's undo runs as admin and stamps undone_at and undone_by.
GRANT INSERT (server_id, signature, merged_into, reason, merged_by) ON {config}.investigation_signature_merges TO {mcp};
GRANT UPDATE (undone_at, undone_by) ON {config}.investigation_signature_merges TO {admin};
-- #6401: undo_agent_change undoes a signature merge the way the web's undo does, by stamping the row; so mcp gets the same two columns.
GRANT UPDATE (undone_at, undone_by) ON {config}.investigation_signature_merges TO {mcp};

-- 10a. The password key tables (V165, #5366; V167 adds the legacy pin candidate table): written only by the store owner (the service, its command line and the
--     migration runner); a trigger on each table refuses every other writer. This is the grant side of the same rule,
--     and it runs after EVERY blanket grant above (the schema-wide SELECT and write grants and the mcp column ACL), so
--     none of them can put a privilege back. REVOKE ALL ... CASCADE takes every privilege on the five tables from
--     PUBLIC and the three roles, TRIGGER and REFERENCES included, so no role can add a new trigger or foreign key. A
--     trigger or foreign key already there (made while a role held the privilege) is dropped by the loops below, with
--     a WARNING, because it would outlast the REVOKE. Then SELECT on the key and service-state tables is granted back
--     to {admin} only (the desktop Viewer reads them as admin), and the DO block checks that nothing else is left; the
--     pin tables are read by the owner only. Provisioning runs after the migrations, so the tables exist here.
REVOKE ALL ON {config}.password_key, {config}.password_key_service,
   {config}.legacy_secret_pin, {config}.legacy_secret_pin_marker FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
REVOKE ALL ON {config}.legacy_secret_pin_candidate FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
-- V198 (#6066): the pseudonym key table; no role but the store owner reads it, so nothing is granted back.
REVOKE ALL ON {config}.pseudonym_key FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
GRANT SELECT ON {config}.password_key, {config}.password_key_service TO {admin};
DO $do$
DECLARE
   stray record;
BEGIN
   FOR stray IN
      SELECT t.tgname, t.tgrelid::pg_catalog.regclass::pg_catalog.text AS tablename
      FROM pg_catalog.pg_trigger AS t
      WHERE NOT t.tgisinternal
        AND t.tgrelid IN (pg_catalog.to_regclass('{config}.password_key'), pg_catalog.to_regclass('{config}.password_key_service'), pg_catalog.to_regclass('{config}.legacy_secret_pin'), pg_catalog.to_regclass('{config}.legacy_secret_pin_marker'), pg_catalog.to_regclass('{config}.legacy_secret_pin_candidate'), pg_catalog.to_regclass('{config}.pseudonym_key'))
        AND t.tgname NOT IN ('trg_password_key_owner_only_row', 'trg_password_key_owner_only_truncate', 'trg_password_key_service_owner_only_row', 'trg_password_key_service_owner_only_truncate', 'trg_legacy_secret_pin_owner_only_row', 'trg_legacy_secret_pin_owner_only_truncate', 'trg_legacy_secret_pin_marker_owner_only_row', 'trg_legacy_secret_pin_marker_owner_only_truncate', 'trg_legacy_secret_pin_candidate_owner_only_row', 'trg_legacy_secret_pin_candidate_owner_only_truncate', 'trg_pseudonym_key_owner_only_row', 'trg_pseudonym_key_owner_only_truncate')
   LOOP
      RAISE WARNING 'Dropped trigger % from % because only the store owner may write the password key tables.', stray.tgname, stray.tablename;
      EXECUTE pg_catalog.format('DROP TRIGGER %I ON %s', stray.tgname, stray.tablename);
   END LOOP;

   FOR stray IN
      SELECT k.conname, k.conrelid::pg_catalog.regclass::pg_catalog.text AS tablename
      FROM pg_catalog.pg_constraint AS k
      WHERE k.contype = 'f'
        AND k.confrelid IN (pg_catalog.to_regclass('{config}.password_key'), pg_catalog.to_regclass('{config}.password_key_service'), pg_catalog.to_regclass('{config}.legacy_secret_pin'), pg_catalog.to_regclass('{config}.legacy_secret_pin_marker'), pg_catalog.to_regclass('{config}.legacy_secret_pin_candidate'), pg_catalog.to_regclass('{config}.pseudonym_key'))
   LOOP
      RAISE WARNING 'Dropped foreign key % on % because it references a password key table.', stray.conname, stray.tablename;
      EXECUTE pg_catalog.format('ALTER TABLE %s DROP CONSTRAINT %I', stray.tablename, stray.conname);
   END LOOP;

   IF EXISTS (
      SELECT 1
      FROM pg_catalog.pg_class AS c
      CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS a
      LEFT JOIN pg_catalog.pg_roles AS r ON r.oid = a.grantee
      WHERE c.oid IN (pg_catalog.to_regclass('{config}.password_key'), pg_catalog.to_regclass('{config}.password_key_service'), pg_catalog.to_regclass('{config}.legacy_secret_pin'), pg_catalog.to_regclass('{config}.legacy_secret_pin_marker'), pg_catalog.to_regclass('{config}.legacy_secret_pin_candidate'), pg_catalog.to_regclass('{config}.pseudonym_key'))
        AND (a.grantee = 0 OR r.rolname IN ('{admin}', '{viewer}', '{web}', '{mcp}'))
        AND NOT (COALESCE(r.rolname = '{admin}', false) AND a.privilege_type = 'SELECT'
                 AND c.oid IN (pg_catalog.to_regclass('{config}.password_key'), pg_catalog.to_regclass('{config}.password_key_service')))
   ) THEN
      RAISE EXCEPTION 'A role other than the store owner still holds a privilege it should not on a password key table.';
   END IF;
END
$do$;

-- 10g. The web login's writes, taken from viewer. The web host used to connect as viewer and viewer held every write the
--     dashboard's pages make; those grants now go to the {web} role (above), and viewer is SELECT plus the secret-column
--     carve. A store that ran an earlier build keeps the old grants on viewer until this statement removes them, so every
--     start revokes them and the first start after an upgrade converges. REVOKE removes only privileges granted by the role
--     that issues it, and this batch granted them as that same owner. A table-level REVOKE of UPDATE also removes any
--     column-level UPDATE, so config_service and config_alert_log leave viewer with no write at all.
REVOKE INSERT, UPDATE, DELETE ON {config}.custom_views, {config}.custom_alert_rules, {config}.database_state_expected,
   {config}.config_mute_rules, {config}.server_tags, {config}.server_tag_map, {config}.query_tags,
   {config}.query_tag_assignments, {config}.query_tag_rules, {config}.query_tag_map, {config}.config_monitored_servers FROM {viewer};
REVOKE UPDATE ON {config}.config_service, {config}.config_alert_log FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.record_custom_alert_resolution(integer, text, text, text) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.insert_known_pattern({InsertKnownPatternSignature}) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.update_known_pattern({UpdateKnownPatternSignature}) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.delete_known_pattern(bigint) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerSignature}) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerSeventeenSignature}) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerLegacySignature}) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.request_snapshot_now(integer, text, text) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.request_active_queries_live(integer, text) FROM {viewer};
REVOKE EXECUTE ON FUNCTION {config}.request_analyze_now(integer, text) FROM {viewer};

-- 11. Role memberships (#3914): the four roles hold none. Nothing above grants one, so every membership in which
--     admin, viewer or mcp is the MEMBER was given by someone else, and each outranks the grants above -- a
--     pg_read_all_data undoes the secret-column carve, and membership in the owner is every privilege the owner
--     has. A role this batch adopts would otherwise keep them, so every one is revoked, every start.
--     GRANTED BY the recorded grantor: since PostgreSQL 16 a superuser's plain REVOKE removes only the grants
--     recorded as the bootstrap superuser's, and leaves one made by any other role in place with a WARNING.
--     CASCADE: a grant the member itself made with an ADMIN OPTION depends on that membership, and RESTRICT would
--     fail the whole batch on it every start. A grantor that no longer exists (possible before 16, which ignores
--     the grantor on REVOKE) is left out of the statement. A row an earlier CASCADE already removed draws a
--     WARNING, never an error.
DO $do$
DECLARE
   membership record;
BEGIN
   FOR membership IN
      SELECT g.rolname AS granted, m.rolname AS member, b.rolname AS grantor
      FROM pg_catalog.pg_auth_members AS a
      JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
      JOIN pg_catalog.pg_roles AS m ON m.oid = a.member
      LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor
      WHERE m.rolname IN ('{admin}', '{viewer}', '{web}', '{mcp}')
   LOOP
      EXECUTE format('REVOKE %I FROM %I', membership.granted, membership.member)
         || CASE WHEN membership.grantor IS NULL THEN '' ELSE format(' GRANTED BY %I', membership.grantor) END
         || ' CASCADE';
   END LOOP;
END $do$;
";
    }

    /// <summary>
    /// The <c>SECURITY DEFINER</c> function (#5654) a registered external alert source calls to queue a fire or a clear:
    /// the <c>CREATE OR REPLACE FUNCTION</c> plus the <c>REVOKE ALL ... FROM PUBLIC</c> (a new function is EXECUTE-able
    /// by PUBLIC by default); the caller adds the narrow <c>GRANT EXECUTE</c> (section 10c). Shared so the live tests run
    /// the identical function. Built after <see cref="BuildCustomAlertResolveFunctionSql"/>:
    /// <list type="bullet">
    /// <item>owned by whoever runs it (the store owner, which holds the INSERT and the row lock the body needs);</item>
    /// <item><c>SET search_path = pg_catalog, pg_temp</c>, pg_temp last, and every table named by its schema, so no
    /// temporary object a caller creates can stand in for a table or for <c>clock_timestamp()</c>;</item>
    /// <item>no dynamic SQL: the only values that reach a statement are the seven parameters, bound as parameters;</item>
    /// <item>the source is <c>session_user</c> looked up in the registry (a non-superuser cannot change it), never an
    /// argument; a login with no enabled registry row gets SQLSTATE <c>DL403</c>;</item>
    /// <item>a bad kind, severity, key or an over-long text gets <c>DL400</c>; a source over its per-minute rate or its
    /// waiting-row cap gets <c>DL429</c> with a HINT. A value that is NaN or infinite gets <c>DL400</c>. The registry row is
    /// locked <c>FOR NO KEY UPDATE</c>, so the count and the insert are atomic for one source while the service's writes to
    /// the state table, whose foreign key takes only a key share on that row, are not held up by an open transaction.
    /// The rate window, <c>submitted_at</c> and <c>expires_at</c> read <c>clock_timestamp()</c>, the time of the call. A clear without a severity is stored as 'warning'; a fire needs one;</item>
    /// <item>carriage returns and line feeds in the title, detail and server name become spaces, so a text cannot forge
    /// lines in the mute pre-fill that quotes it.</item>
    /// </list>
    /// The caller never supplies the context JSON, the source, the expiry or the status: Darling builds those.
    /// </summary>
    internal static string BuildExternalAlertSubmitFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.submit_external_alert(
   p_kind text,
   p_alert_key text,
   p_severity text,
   p_title text,
   p_detail text,
   p_server_name text,
   p_value double precision)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_source_id integer;
   v_max_per_minute integer;
   v_max_pending integer;
   v_severity text;
   v_pending_id bigint;
BEGIN
   SELECT s.source_id, s.max_per_minute, s.max_pending
   INTO v_source_id, v_max_per_minute, v_max_pending
   FROM {config}.external_alert_source AS s
   WHERE s.role_name = session_user::text
     AND s.enabled
   FOR NO KEY UPDATE OF s;

   IF NOT FOUND THEN
      RAISE EXCEPTION 'This login is not a registered external alert source.' USING ERRCODE = 'DL403';
   END IF;

   IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'submit_external_alert needs the READ COMMITTED isolation level.' USING ERRCODE = 'DL400',
         HINT = 'Run the call outside a REPEATABLE READ or SERIALIZABLE transaction.';
   END IF;

   v_severity := CASE WHEN p_severity IS NULL AND p_kind = 'clear' THEN 'warning' ELSE p_severity END;

   IF p_kind IS NULL OR p_kind NOT IN ('fire', 'clear') THEN
      RAISE EXCEPTION 'kind must be fire or clear.' USING ERRCODE = 'DL400';
   END IF;
   IF v_severity IS NULL OR v_severity NOT IN ('warning', 'critical') THEN
      RAISE EXCEPTION 'severity must be warning or critical.' USING ERRCODE = 'DL400';
   END IF;
   IF p_alert_key IS NULL OR char_length(p_alert_key) NOT BETWEEN 1 AND 128 OR p_alert_key !~ '^[A-Za-z0-9._:-]+$' THEN
      RAISE EXCEPTION 'alert_key must be 1 to 128 characters from A-Z a-z 0-9 . _ : -.' USING ERRCODE = 'DL400';
   END IF;
   IF char_length(p_title) > 200 THEN
      RAISE EXCEPTION 'title is longer than 200 characters.' USING ERRCODE = 'DL400';
   END IF;
   IF char_length(p_detail) > 2000 THEN
      RAISE EXCEPTION 'detail is longer than 2000 characters.' USING ERRCODE = 'DL400';
   END IF;
   IF char_length(p_server_name) > 128 THEN
      RAISE EXCEPTION 'server_name is longer than 128 characters.' USING ERRCODE = 'DL400';
   END IF;
   IF p_value IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN
      RAISE EXCEPTION 'value must be a finite number.' USING ERRCODE = 'DL400';
   END IF;

   IF (SELECT count(*) FROM {config}.external_alert_pending AS p
       WHERE p.source_id = v_source_id AND p.submitted_at > clock_timestamp() - interval '1 minute') >= v_max_per_minute THEN
      RAISE EXCEPTION 'This source has reached its limit of % alerts per minute.', v_max_per_minute
         USING ERRCODE = 'DL429', HINT = 'Send fewer alerts, or ask the Darling operator to raise max_per_minute for this source.';
   END IF;
   IF (SELECT count(*) FROM {config}.external_alert_pending AS p
       WHERE p.source_id = v_source_id AND p.status IN ('pending', 'claimed')) >= v_max_pending THEN
      RAISE EXCEPTION 'This source already has % alerts waiting for delivery.', v_max_pending
         USING ERRCODE = 'DL429', HINT = 'Darling has not delivered the earlier alerts yet. Retry later.';
   END IF;

   INSERT INTO {config}.external_alert_pending
      (source_id, kind, alert_key, severity, title, detail, server_name, value, submitted_at, expires_at)
   VALUES
      (v_source_id, p_kind, p_alert_key, v_severity,
       regexp_replace(p_title, '[\r\n]+', ' ', 'g'),
       regexp_replace(p_detail, '[\r\n]+', ' ', 'g'),
       regexp_replace(p_server_name, '[\r\n]+', ' ', 'g'),
       p_value, clock_timestamp(), clock_timestamp() + interval '15 minutes')
   RETURNING pending_id INTO v_pending_id;

   RETURN v_pending_id;
END;
$fn$;
REVOKE ALL ON FUNCTION {config}.submit_external_alert({ExternalAlertSubmitSignature}) FROM PUBLIC;";

    /// <summary>
    /// The block (#5654, section 10c) that re-asserts the grants of every registered external alert source login, on
    /// every start. A login is touched only when the registry names it AND the role exists AND it carries
    /// <see cref="AlertSourceRoleMarker"/>. Per role: LOGIN, every widening attribute off, NOINHERIT, CONNECTION LIMIT 5, every
    /// membership revoked (GRANTED BY the recorded grantor, CASCADE: section 11's rule), then CONNECT on the current
    /// database, USAGE on the config schema and EXECUTE on the submit function, nothing else (the function is also revoked from admin, viewer and mcp, and a table or sequence privilege
    /// found on the login in the config or collect schema is revoked first). Kept out of the statements the batch
    /// replays for a role (a test replays every GRANT and REVOKE that names viewer on a store that has no such function).
    /// <para>The same block owns <see cref="AlertSourceGroupRoleName"/>: it creates the NOLOGIN group with
    /// <see cref="AlertSourceGroupRoleMarker"/> (an unmarked role of that name raises, as section 1 does for admin, viewer and mcp),
    /// takes from it every membership and every privilege on the database, the config and collect schemas, their tables and
    /// sequences and the submit function, revokes it from every member that is not a registered, marked source login, and grants
    /// it to each source login after that login's memberships are revoked: WITH INHERIT FALSE, SET FALSE from PostgreSQL 16, a
    /// plain GRANT before (the group holds nothing, so the membership carries nothing). <see cref="ExternalAlertSourceAdmin"/>
    /// runs this block in the transaction that creates a login, so a new source is a member when the verb returns.</para>
    /// </summary>
    internal static string BuildExternalAlertSourceRolesSql(string config) => $@"
DO $do$
DECLARE
   name_grantor boolean := (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user)
                           OR pg_catalog.current_setting('server_version_num')::int >= 160000;
   source_login record;
   membership record;
   attribute text;
   own_role text;
   group_role record;
BEGIN
   FOR own_role IN
      SELECT rolname FROM pg_catalog.pg_roles
      WHERE rolname IN ('{DarlingManagedPostgres.AdminRoleName}', '{DarlingManagedPostgres.ViewerRoleName}', '{DarlingManagedPostgres.WebRoleName}', '{DarlingManagedPostgres.McpRoleName}')
   LOOP
      EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION {config}.submit_external_alert({ExternalAlertSubmitSignature}) FROM %I', own_role);
   END LOOP;

   IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{AlertSourceGroupRoleName}') THEN
      CREATE ROLE {AlertSourceGroupRoleName} NOLOGIN NOINHERIT;
      COMMENT ON ROLE {AlertSourceGroupRoleName} IS '{AlertSourceGroupRoleMarker}';
   ELSIF pg_catalog.shobj_description((SELECT oid FROM pg_catalog.pg_roles WHERE rolname = '{AlertSourceGroupRoleName}'), 'pg_authid') IS DISTINCT FROM '{AlertSourceGroupRoleMarker}' THEN
      RAISE EXCEPTION 'The group role ""{AlertSourceGroupRoleName}"" already exists and was not created by Darling (missing the ''{AlertSourceGroupRoleMarker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated role.';
   END IF;

   /* Only the attributes that are on are named: a role without SUPERUSER may not name SUPERUSER, REPLICATION or BYPASSRLS
      in ALTER ROLE even to switch them off, so a CREATEROLE owner running the add verb sets only what it may. */
   SELECT oid, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls INTO group_role
   FROM pg_catalog.pg_roles WHERE rolname = '{AlertSourceGroupRoleName}';
   ALTER ROLE {AlertSourceGroupRoleName} NOLOGIN NOINHERIT;
   FOREACH attribute IN ARRAY pg_catalog.array_remove(ARRAY[
         CASE WHEN group_role.rolsuper THEN 'SUPERUSER' END,
         CASE WHEN group_role.rolcreaterole THEN 'CREATEROLE' END,
         CASE WHEN group_role.rolcreatedb THEN 'CREATEDB' END,
         CASE WHEN group_role.rolreplication THEN 'REPLICATION' END,
         CASE WHEN group_role.rolbypassrls THEN 'BYPASSRLS' END], NULL)
   LOOP
      EXECUTE pg_catalog.format('ALTER ROLE %I NO%s', '{AlertSourceGroupRoleName}', attribute);
   END LOOP;

   FOR membership IN
      SELECT g.rolname AS granted, b.rolname AS grantor
      FROM pg_catalog.pg_auth_members AS a
      JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
      LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor
      WHERE a.member = group_role.oid
   LOOP
      EXECUTE pg_catalog.format('REVOKE %I FROM %I', membership.granted, '{AlertSourceGroupRoleName}')
         || CASE WHEN name_grantor AND membership.grantor IS NOT NULL THEN pg_catalog.format(' GRANTED BY %I', membership.grantor) ELSE '' END
         || ' CASCADE';
   END LOOP;

   EXECUTE pg_catalog.format('REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE %I FROM {AlertSourceGroupRoleName}', pg_catalog.current_database());
   REVOKE ALL ON SCHEMA {config}, {PgSchemaGenerator.CollectSchema} FROM {AlertSourceGroupRoleName};
   EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA %I, %I FROM %I', '{config}', '{PgSchemaGenerator.CollectSchema}', '{AlertSourceGroupRoleName}');
   EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA %I, %I FROM %I', '{config}', '{PgSchemaGenerator.CollectSchema}', '{AlertSourceGroupRoleName}');
   REVOKE ALL ON FUNCTION {config}.submit_external_alert({ExternalAlertSubmitSignature}) FROM {AlertSourceGroupRoleName};

   /* Every grant of the group to a role that is not a registered, marked source login, and every grant of a source login to
      any role, found by walking down from the group through the source logins (pg_hba's +group matches a member at any
      depth). A grant this role may not revoke (PostgreSQL 16 records a non-superuser creator of the group, or of a login, as
      its member, granted by the bootstrap superuser) is left and named in a WARNING; the pg_hba step and the verbs read the
      same closure (AlertSourceGroupExtrasSql) and name it too. */
   FOR membership IN
{AlertSourceGroupSweepSql(config, "group_role.oid")}
   LOOP
      BEGIN
         EXECUTE pg_catalog.format('REVOKE %I FROM %I', membership.granted, membership.member_name)
            || CASE WHEN name_grantor AND membership.grantor IS NOT NULL THEN pg_catalog.format(' GRANTED BY %I', membership.grantor) ELSE '' END
            || ' CASCADE';
      EXCEPTION WHEN insufficient_privilege THEN
         RAISE WARNING 'Role ""%"" is still a member of ""%"": revoking that needs a superuser, or ADMIN OPTION on it and the rights of the role that granted it.', membership.member_name, membership.granted;
      END;
   END LOOP;

   FOR source_login IN
      SELECT s.role_name, r.oid AS role_oid, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolreplication, r.rolbypassrls,
             (pg_catalog.shobj_description(r.oid, 'pg_authid') IS NOT DISTINCT FROM '{AlertSourceRoleMarker}') AS marked,
             (r.rolname IN ('{DarlingManagedPostgres.AdminRoleName}', '{DarlingManagedPostgres.ViewerRoleName}', '{DarlingManagedPostgres.WebRoleName}', '{DarlingManagedPostgres.McpRoleName}')
              OR r.rolname = current_user
              OR r.rolsuper
              OR r.oid = (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database())) AS reserved
      FROM {config}.external_alert_source AS s
      LEFT JOIN pg_catalog.pg_roles AS r ON r.rolname = s.role_name
   LOOP
      IF source_login.reserved IS TRUE THEN
         RAISE WARNING 'External alert source login ""%"" was skipped: it is one of Darling''s own roles, the role running this step, a superuser or the database owner, and its limit, inheritance and memberships are left as they are.', source_login.role_name;
         CONTINUE;
      END IF;

      IF source_login.role_oid IS NULL OR NOT source_login.marked THEN
         RAISE WARNING 'External alert source login ""%"" was skipped: it does not exist or does not carry the ''{AlertSourceRoleMarker}'' marker comment.', source_login.role_name;
         IF source_login.role_oid IS NOT NULL THEN
            EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION {config}.submit_external_alert({ExternalAlertSubmitSignature}) FROM %I', source_login.role_name);
         END IF;
         CONTINUE;
      END IF;

      EXECUTE pg_catalog.format('ALTER ROLE %I LOGIN NOINHERIT CONNECTION LIMIT {AlertSourceConnectionLimit}', source_login.role_name);
      FOREACH attribute IN ARRAY pg_catalog.array_remove(ARRAY[
            CASE WHEN source_login.rolsuper THEN 'SUPERUSER' END,
            CASE WHEN source_login.rolcreaterole THEN 'CREATEROLE' END,
            CASE WHEN source_login.rolcreatedb THEN 'CREATEDB' END,
            CASE WHEN source_login.rolreplication THEN 'REPLICATION' END,
            CASE WHEN source_login.rolbypassrls THEN 'BYPASSRLS' END], NULL)
      LOOP
         EXECUTE pg_catalog.format('ALTER ROLE %I NO%s', source_login.role_name, attribute);
      END LOOP;

      FOR membership IN
         SELECT g.rolname AS granted, b.rolname AS grantor
         FROM pg_catalog.pg_auth_members AS a
         JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
         LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor
         WHERE a.member = source_login.role_oid
      LOOP
         EXECUTE pg_catalog.format('REVOKE %I FROM %I', membership.granted, source_login.role_name)
            || CASE WHEN name_grantor AND membership.grantor IS NOT NULL THEN pg_catalog.format(' GRANTED BY %I', membership.grantor) ELSE '' END
            || ' CASCADE';
      END LOOP;

      EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA %I, %I FROM %I', '{config}', '{PgSchemaGenerator.CollectSchema}', source_login.role_name);
      EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA %I, %I FROM %I', '{config}', '{PgSchemaGenerator.CollectSchema}', source_login.role_name);
      EXECUTE pg_catalog.format('GRANT CONNECT ON DATABASE %I TO %I', pg_catalog.current_database(), source_login.role_name);
      EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA {config} TO %I', source_login.role_name);
      EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION {config}.submit_external_alert({ExternalAlertSubmitSignature}) TO %I', source_login.role_name);
      IF pg_catalog.current_setting('server_version_num')::int >= 160000 THEN
         EXECUTE pg_catalog.format('GRANT %I TO %I WITH INHERIT FALSE, SET FALSE', '{AlertSourceGroupRoleName}', source_login.role_name);
      ELSE
         EXECUTE pg_catalog.format('GRANT %I TO %I', '{AlertSourceGroupRoleName}', source_login.role_name);
      END IF;
   END LOOP;
END $do$;";

    /// <summary>
    /// The query (#5654) the group sweep loops over: every grant to remove so that the members of
    /// <see cref="AlertSourceGroupRoleName"/>, at any depth, are only the registered, marked source logins. A <c>WITH RECURSIVE</c>
    /// walk starts at the group and goes on only through such a login. It returns each grant of the group to a role that is not
    /// one, and each grant of one of those logins to any role (a source login has no members). One row per
    /// <c>pg_auth_members</c> row: granted, member_name, grantor. <paramref name="groupOid"/> is a PL/pgSQL expression.
    /// <c>tools/provision-roles.sql</c> carries the same query.
    /// </summary>
    internal static string AlertSourceGroupSweepSql(string config, string groupOid) => $@"      WITH RECURSIVE valid AS (
         SELECT r.oid
         FROM {config}.external_alert_source AS s
         JOIN pg_catalog.pg_roles AS r ON r.rolname = s.role_name
         WHERE pg_catalog.shobj_description(r.oid, 'pg_authid') IS NOT DISTINCT FROM '{AlertSourceRoleMarker}'
           AND r.rolname NOT IN ('{DarlingManagedPostgres.AdminRoleName}', '{DarlingManagedPostgres.ViewerRoleName}', '{DarlingManagedPostgres.WebRoleName}', '{DarlingManagedPostgres.McpRoleName}')
           AND r.rolname <> current_user
           AND NOT r.rolsuper
           AND r.oid <> (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database())
      ), walk (roleid, member) AS (
         SELECT a.roleid, a.member FROM pg_catalog.pg_auth_members AS a WHERE a.roleid = {groupOid}
         UNION
         SELECT a.roleid, a.member
         FROM pg_catalog.pg_auth_members AS a
         JOIN walk AS w ON a.roleid = w.member
         WHERE w.member IN (SELECT v.oid FROM valid AS v)
      )
      SELECT DISTINCT g.rolname AS granted, m.rolname AS member_name, b.rolname AS grantor
      FROM walk AS w
      JOIN pg_catalog.pg_auth_members AS a ON a.roleid = w.roleid AND a.member = w.member
      JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
      JOIN pg_catalog.pg_roles AS m ON m.oid = a.member
      LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor
      WHERE (w.roleid = {groupOid} AND w.member NOT IN (SELECT v.oid FROM valid AS v))
         OR w.roleid IN (SELECT v.oid FROM valid AS v)";

    /// <summary>
    /// Every role that a pg_hba line naming <c>+darling_alert_sources</c> admits and that is not a registered, marked source
    /// login (#5654): each member of the group at any depth (pg_hba follows membership through every role, whatever its INHERIT
    /// and SET options), plus the group itself when it can log in. Read after the provisioning step, this is what that step
    /// could not revoke, for example the PostgreSQL 16 grant of the group, or of a source login, to the non-superuser role that
    /// created it. One row: a text array sorted by name, empty when the group does not exist. <paramref name="registryExists"/>
    /// false (a store not yet upgraded) counts every member.
    /// </summary>
    internal static string AlertSourceGroupExtrasSql(string config, bool registryExists)
    {
        var isSource = registryExists
            ? $@"EXISTS (SELECT 1 FROM {config}.external_alert_source AS s WHERE s.role_name = r.rolname)
              AND pg_catalog.shobj_description(r.oid, 'pg_authid') IS NOT DISTINCT FROM '{AlertSourceRoleMarker}'
              AND r.rolname NOT IN ('{DarlingManagedPostgres.AdminRoleName}', '{DarlingManagedPostgres.ViewerRoleName}', '{DarlingManagedPostgres.WebRoleName}', '{DarlingManagedPostgres.McpRoleName}')
              AND NOT r.rolsuper
              AND r.oid <> (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database())"
            : "false";
        return $@"
WITH RECURSIVE closure (oid) AS (
   SELECT a.member
   FROM pg_catalog.pg_auth_members AS a
   JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
   WHERE g.rolname = '{AlertSourceGroupRoleName}'
   UNION
   SELECT a.member FROM pg_catalog.pg_auth_members AS a JOIN closure AS c ON a.roleid = c.oid
)
SELECT COALESCE(pg_catalog.array_agg(x.rolname ORDER BY x.rolname COLLATE ""C""), ARRAY[]::text[])
FROM (
   SELECT r.rolname::text AS rolname
   FROM closure AS c
   JOIN pg_catalog.pg_roles AS r ON r.oid = c.oid
   WHERE NOT ({isSource})
   UNION
   SELECT rolname::text FROM pg_catalog.pg_roles WHERE rolname = '{AlertSourceGroupRoleName}' AND rolcanlogin
) AS x";
    }

    /// <summary>
    /// Reads <see cref="AlertSourceGroupExtrasSql"/> on <paramref name="connection"/> (#5654): the roles besides the registered,
    /// marked source logins that a <c>+darling_alert_sources</c> pg_hba line admits. The managed pg_hba step writes that line
    /// only when this is empty, and the add and rotate verbs name each role it returns.
    /// </summary>
    internal static async Task<IReadOnlyList<string>> ReadAlertSourceGroupExtrasAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Its own short transaction with search_path set to pg_catalog, pg_temp, as the verbs' own transactions are: the add and
           rotate verbs run this as the store owner right after they commit, and no function another role made in a schema on
           the session's path (public on a store whose PUBLIC still holds CREATE there) can stand in for a catalog one. */
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var path = new NpgsqlCommand("SET LOCAL search_path = pg_catalog, pg_temp", connection, transaction))
        {
            await path.ExecuteNonQueryAsync(ct);
        }

        bool registryExists;
        await using (var probe = new NpgsqlCommand(AlertSourceRegistryProbeSql, connection, transaction))
        {
            registryExists = (bool)(await probe.ExecuteScalarAsync(ct))!;
        }

        string[] extras;
        await using (var command = new NpgsqlCommand(AlertSourceGroupExtrasSql(PgSchemaGenerator.ConfigSchema, registryExists), connection, transaction))
        {
            extras = (string[])(await command.ExecuteScalarAsync(ct))!;
        }

        await transaction.CommitAsync(ct);
        return extras;
    }

    /// <summary>Whether the store holds the external alert source registry yet (#5654), read before <see cref="AlertSourceGroupExtrasSql"/>.</summary>
    internal static readonly string AlertSourceRegistryProbeSql =
        $"SELECT pg_catalog.to_regclass('{PgSchemaGenerator.ConfigSchema}.external_alert_source') IS NOT NULL";

    /// <summary>The argument types of <c>config.insert_known_pattern</c> (#6334), in order. The one place the signature is spelled for the GRANT.</summary>
    internal const string InsertKnownPatternSignature = "integer, text, time, text, integer, text, text, timestamp";

    /// <summary>The argument types of <c>config.update_known_pattern</c> (#6334), in order.</summary>
    internal const string UpdateKnownPatternSignature = "bigint, integer, text, time, text, integer, text, timestamp, text";

    /// <summary>
    /// The <c>SECURITY DEFINER</c> functions (#6334) that are the writes of the known-pattern entries
    /// (<c>config.config_known_patterns</c>): <c>insert_known_pattern</c>, <c>update_known_pattern</c> and <c>delete_known_pattern</c>. The
    /// web editor (viewer), the Viewer (admin) and the MCP write tools (mcp, #6398) call them; the table itself carries NO direct write grant for any reader role.
    /// Returns the <c>CREATE OR REPLACE FUNCTION</c> statements plus the <c>REVOKE ALL ... FROM PUBLIC</c> of each (a new function is
    /// EXECUTE-able by PUBLIC); the caller adds the <c>GRANT EXECUTE</c>. Because the body runs as the owner, the table's
    /// <c>config_version</c> trigger bumps the beacon as the owner, so the caller needs no grant on <c>config_service</c>. Pinned
    /// <c>search_path</c>, fixed statements, no dynamic SQL; the table's CHECKs hold every column to what a reader can load whatever the caller sends, and
    /// both writes refuse a server_id that names no registered server (DL404). <c>update_known_pattern</c> alone stamps updated_by and
    /// updated_at_utc. Same builder for the managed batch, the script's text and the live tests, so none drifts.
    /// </summary>
    internal static string BuildKnownPatternFunctionsSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.insert_known_pattern(
   p_server_id integer,
   p_metric_name text,
   p_time_of_day time,
   p_time_zone text,
   p_window_minutes integer,
   p_note text,
   p_created_by text,
   p_expires_at_utc timestamp)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_id bigint;
   v_count bigint;
BEGIN
   IF p_server_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   SELECT count(*) INTO v_count FROM {config}.config_known_patterns;
   IF v_count >= {PerformanceMonitor.Common.KnownPatterns.KnownPatternMatcher.MaxEntries} THEN
      RAISE EXCEPTION 'Known patterns are capped at {PerformanceMonitor.Common.KnownPatterns.KnownPatternMatcher.MaxEntries} entries and % exist. Delete entries you no longer need to make room.', v_count USING ERRCODE = 'DL409';
   END IF;
   INSERT INTO {config}.config_known_patterns
      (server_id, metric_name, time_of_day, time_zone, window_minutes, note, created_by, expires_at_utc)
   VALUES
      (p_server_id, p_metric_name, p_time_of_day, p_time_zone, p_window_minutes, p_note, p_created_by, p_expires_at_utc)
   RETURNING id INTO v_id;
   RETURN v_id;
END;
$fn$;
REVOKE ALL ON FUNCTION {config}.insert_known_pattern({InsertKnownPatternSignature}) FROM PUBLIC;
DROP FUNCTION IF EXISTS {config}.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp);
CREATE OR REPLACE FUNCTION {config}.update_known_pattern(
   p_id bigint,
   p_server_id integer,
   p_metric_name text,
   p_time_of_day time,
   p_time_zone text,
   p_window_minutes integer,
   p_note text,
   p_expires_at_utc timestamp,
   p_updated_by text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_rows bigint;
BEGIN
   IF p_server_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   UPDATE {config}.config_known_patterns
      SET server_id = p_server_id, metric_name = p_metric_name, time_of_day = p_time_of_day, time_zone = p_time_zone,
          window_minutes = p_window_minutes, note = p_note, expires_at_utc = p_expires_at_utc,
          updated_by = p_updated_by, updated_at_utc = pg_catalog.now() AT TIME ZONE 'UTC'
    WHERE id = p_id;
   GET DIAGNOSTICS v_rows = ROW_COUNT;
   RETURN v_rows > 0;
END;
$fn$;
REVOKE ALL ON FUNCTION {config}.update_known_pattern({UpdateKnownPatternSignature}) FROM PUBLIC;
CREATE OR REPLACE FUNCTION {config}.delete_known_pattern(p_id bigint)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_rows bigint;
BEGIN
   DELETE FROM {config}.config_known_patterns WHERE id = p_id;
   GET DIAGNOSTICS v_rows = ROW_COUNT;
   RETURN v_rows > 0;
END;
$fn$;
REVOKE ALL ON FUNCTION {config}.delete_known_pattern(bigint) FROM PUBLIC;";

    /// <summary>
    /// The <c>SECURITY DEFINER</c> function (#3334) that lets the least-privilege viewer/mcp roles write the ONE
    /// resolution row a caller-initiated custom-alert rule delete records in <c>config_alert_log</c> -- a table
    /// they may not INSERT directly -- WITHOUT a blanket INSERT grant that would let them fabricate arbitrary
    /// history. Returns the <c>CREATE OR REPLACE FUNCTION</c> plus the <c>REVOKE ALL ... FROM PUBLIC</c> (a
    /// freshly created function is EXECUTE-able by PUBLIC by default, so revoking is mandatory); the caller adds
    /// the narrow <c>GRANT EXECUTE</c>. Shared so the gated live proof test creates the IDENTICAL function rather
    /// than a drifting hand-copy. Definer-safe by construction: owned by whoever runs it (the provisioning owner,
    /// which holds the config_alert_log INSERT the body needs), an explicit <c>SET search_path = pg_catalog,
    /// pg_temp</c> so neither the schema-qualified table nor <c>now()</c> can be redirected by a caller's
    /// search_path, and a fully parameterized INSERT that hardcodes the resolution shape (never a fire) with no
    /// dynamic SQL. The 12-column list matches <c>PgAlertHistoryStore.RecordAlertAsync</c>'s write for a
    /// <c>BuildResolutionRecord</c> and so do the fixed values, EXCEPT this is pinned to a no-channel row
    /// (<c>alert_sent</c> false, <c>notification_type</c> 'none') rather than a natural clear's 'tray': a
    /// teardown surfaces no operator notification, and a grantee must never write a row claiming a delivery.
    /// </summary>
    internal static string BuildCustomAlertResolveFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.record_custom_alert_resolution(
   p_server_id integer,
   p_server_name text,
   p_metric_name text,
   p_detail_text text)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
   INSERT INTO {config}.config_alert_log
      (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
       alert_sent, notification_type, send_error, muted, detail_text, context_json)
   VALUES
      ((now() AT TIME ZONE 'UTC'), p_server_id, p_server_name, p_metric_name, 0, 0,
       false, 'none', NULL, false, p_detail_text, NULL);
$fn$;
REVOKE ALL ON FUNCTION {config}.record_custom_alert_resolution(integer, text, text, text) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER function (#5941) the web route and the MCP tool queue a <c>snapshot_now</c> command through:
    /// a fixed command type, a server id and an optional list of collector names, nothing else. Created and REVOKEd from
    /// PUBLIC here; the caller adds the narrow GRANT EXECUTE.
    /// </summary>
    internal static string BuildRequestSnapshotNowFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.request_snapshot_now(
   p_server_id integer,
   p_args_json text,
   p_requested_by text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_args jsonb;
   v_id bigint;
   v_last_status text;
   v_last_at timestamp;
BEGIN
   -- The one-request-at-a-time rule below reads the newest row of its kind with a fresh snapshot per statement. A transaction that
   -- keeps one snapshot (REPEATABLE READ and up) could miss a row another session just queued, so it is refused.
   IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'This request must run at the READ COMMITTED isolation level.' USING ERRCODE = 'DL400';
   END IF;
   IF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   IF p_args_json IS NOT NULL THEN
      v_args := p_args_json::jsonb;
      IF jsonb_typeof(v_args) IS DISTINCT FROM 'object'
         OR EXISTS (SELECT 1 FROM jsonb_object_keys(v_args) AS k(key) WHERE k.key <> 'collectors')
         OR jsonb_typeof(v_args -> 'collectors') IS DISTINCT FROM 'array'
         OR jsonb_array_length(v_args -> 'collectors') > 200
         OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_args -> 'collectors') AS e(value)
                    WHERE jsonb_typeof(e.value) <> 'string' OR length(e.value #>> '{{}}') > 100) THEN
         RAISE EXCEPTION 'The only argument is a list of collector names.' USING ERRCODE = 'DL400';
      END IF;
   END IF;
   -- The same guard the service applies (#5941 review): one run at a time per server, and not within 60 seconds of the last
   -- request for it, so a holder of the viewer or mcp login cannot queue rows without limit. One lock per server makes the
   -- check and the insert one step.
   -- Never blocks (provisioning rule): a request that holds the lock for this server is a concurrent request, which is the
   -- same conflict as a pending row, so this refuses with the same DL409 instead of waiting.
   IF NOT pg_try_advisory_xact_lock(5941, p_server_id) THEN
      RAISE EXCEPTION 'A collector run for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM {config}.config_command AS c
   WHERE c.command_type = 'snapshot_now' AND c.target_server_id = p_server_id
   ORDER BY c.command_id DESC
   LIMIT 1;
   IF v_last_status IN ('pending', 'in_progress') THEN
      RAISE EXCEPTION 'A collector run for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   IF v_last_at IS NOT NULL AND v_last_at > (now() AT TIME ZONE 'UTC') - interval '60 seconds' THEN
      RAISE EXCEPTION 'A collector run was requested for this server less than 60 seconds ago.'
         USING ERRCODE = 'DL429',
               HINT = to_char(v_last_at + interval '60 seconds', 'YYYY-MM-DD') || 'T' || to_char(v_last_at + interval '60 seconds', 'HH24:MI:SS') || 'Z';
   END IF;
   INSERT INTO {config}.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES (left(COALESCE(p_requested_by, 'web'), 200), 'snapshot_now', p_server_id, v_args, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.request_snapshot_now(integer, text, text) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER function (#6386) the web route queues a <c>fetch_active_queries</c> command through: a fixed command type
    /// and a server id, nothing else (the command takes no arguments). One request per server at a time and not within 10 seconds of
    /// the last, so a holder of the viewer login cannot fill the queue. Created and REVOKEd from PUBLIC here; the caller adds the
    /// narrow GRANT EXECUTE.
    /// </summary>
    internal static string BuildRequestActiveQueriesLiveFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.request_active_queries_live(
   p_server_id integer,
   p_requested_by text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_id bigint;
   v_last_status text;
   v_last_at timestamp;
BEGIN
   -- The one-request-at-a-time rule below reads the newest row of its kind with a fresh snapshot per statement. A transaction that
   -- keeps one snapshot (REPEATABLE READ and up) could miss a row another session just queued, so it is refused.
   IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'This request must run at the READ COMMITTED isolation level.' USING ERRCODE = 'DL400';
   END IF;
   IF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   -- Never blocks (provisioning rule): a request that holds the lock for this server is a concurrent request, which is the same
   -- conflict as a pending row, so this refuses with DL409 instead of waiting.
   IF NOT pg_try_advisory_xact_lock(6386, p_server_id) THEN
      RAISE EXCEPTION 'A live active-queries read for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM {config}.config_command AS c
   WHERE c.command_type = 'fetch_active_queries' AND c.target_server_id = p_server_id
   ORDER BY c.command_id DESC
   LIMIT 1;
   IF v_last_status IN ('pending', 'in_progress') THEN
      RAISE EXCEPTION 'A live active-queries read for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   IF v_last_at IS NOT NULL AND v_last_at > (now() AT TIME ZONE 'UTC') - interval '10 seconds' THEN
      RAISE EXCEPTION 'A live active-queries read was requested for this server less than 10 seconds ago.'
         USING ERRCODE = 'DL429',
               HINT = to_char(v_last_at + interval '10 seconds', 'YYYY-MM-DD') || 'T' || to_char(v_last_at + interval '10 seconds', 'HH24:MI:SS') || 'Z';
   END IF;
   -- Finished results do not pile up: a result carries the plan XML with no size cap and the web never deletes the row it reads, so each
   -- request first removes this server's finished live reads that finished more than an hour ago. Only terminal rows
   -- (succeeded or failed) with a completed_at are touched, so a pending or running row and every other command type stay. The Viewer
   -- reads and deletes its own row within seconds, far inside the hour.
   DELETE FROM {config}.config_command
   WHERE command_type = 'fetch_active_queries' AND target_server_id = p_server_id
     AND status IN ('succeeded', 'failed')
     AND completed_at < (now() AT TIME ZONE 'UTC') - interval '1 hour';
   INSERT INTO {config}.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES (left(COALESCE(p_requested_by, 'web'), 200), 'fetch_active_queries', p_server_id, NULL, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.request_active_queries_live(integer, text) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER function (#6389) the web route queues an <c>analyze_now</c> command through: a fixed command type, a
    /// server id and a requested_by label (the label is not checked: the web host and a locked-down Viewer share the viewer login,
    /// and the web's log line is the record of who asked). One request per server at a time and not within 15 seconds of the last,
    /// under the same advisory lock key the MCP analyze_now takes, so the viewer login cannot fill the queue. Created and REVOKEd
    /// from PUBLIC here; the caller adds the narrow GRANT EXECUTE.
    /// </summary>
    internal static string BuildRequestAnalyzeNowFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.request_analyze_now(
   p_server_id integer,
   p_requested_by text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_id bigint;
   v_last_status text;
   v_last_at timestamp;
BEGIN
   -- The one-request-at-a-time rule below reads the newest row of its kind with a fresh snapshot per statement. A transaction that
   -- keeps one snapshot (REPEATABLE READ and up) could miss a row another session just queued, so it is refused.
   IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'This request must run at the READ COMMITTED isolation level.' USING ERRCODE = 'DL400';
   END IF;
   IF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   -- Never blocks (provisioning rule): a request that holds the lock for this server is a concurrent request, which is the same
   -- conflict as a pending row, so this refuses with DL409 instead of waiting. The key is the one config.request_mcp_command takes
   -- for an analyze_now on this server, so a web click and an MCP call for the same server share one guard.
   IF NOT pg_try_advisory_xact_lock(6230, hashtext('analyze_now:' || p_server_id::text)) THEN
      RAISE EXCEPTION 'An analysis run for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM {config}.config_command AS c
   WHERE c.command_type = 'analyze_now' AND c.target_server_id = p_server_id
   ORDER BY c.command_id DESC
   LIMIT 1;
   IF v_last_status IN ('pending', 'in_progress') THEN
      RAISE EXCEPTION 'An analysis run for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   IF v_last_at IS NOT NULL AND v_last_at > (now() AT TIME ZONE 'UTC') - interval '15 seconds' THEN
      RAISE EXCEPTION 'An analysis run was requested for this server less than 15 seconds ago.'
         USING ERRCODE = 'DL429',
               HINT = to_char(v_last_at + interval '15 seconds', 'YYYY-MM-DD') || 'T' || to_char(v_last_at + interval '15 seconds', 'HH24:MI:SS') || 'Z';
   END IF;
   INSERT INTO {config}.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES (left(COALESCE(p_requested_by, 'web'), 200), 'analyze_now', p_server_id, NULL, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.request_analyze_now(integer, text) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER functions (#6231) the MCP <c>create_notification_route</c> and <c>update_notification_route</c>
    /// tools write a notification route through; <c>set_notification_route_destinations</c> is the one that sets a route's own
    /// destinations (the service seals the webhook values for the route's row and the function stores only sealed text or empty). The <c>mcp</c> login holds no INSERT on the table and no SELECT or UPDATE on its
    /// destination columns, and these functions are how it still creates and edits a route: create inserts a route with every
    /// destination empty (it has no destinations of its own, so each channel goes where it went before: the next matching route,
    /// else the Settings channels), and update writes the match, scope, enabled flag, resolution override and fallback, never
    /// a destination, and changes the match or scope only on a route with no agent setting. Update reads the row again under its lock and refuses an
    /// agent route inside the transaction, unless the actor the service passes is an operator-class token (#6542 part 2: MCP is full admin). Created and REVOKEd from PUBLIC here; the caller adds the GRANT EXECUTE to mcp only.
    /// </summary>
    internal static string BuildNotificationRouteFunctionsSql(string config) => $@"
-- #6928 (V229): p_severities joins the argument list, so the six-argument function is dropped and stays uncallable. NULL is both.
DROP FUNCTION IF EXISTS {config}.create_notification_route(text, boolean, integer, integer, boolean, integer);
CREATE OR REPLACE FUNCTION {config}.create_notification_route(
   p_metric_match text,
   p_enabled boolean,
   p_scope_server_id integer,
   p_scope_tag_id integer,
   p_send_resolutions boolean,
   p_fallback_route_id integer,
   p_severities text DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_match text := btrim(COALESCE(p_metric_match, ''));
   v_severities text := lower(btrim(COALESCE(p_severities, 'both')));
   v_id integer;
BEGIN
   IF v_severities NOT IN ('both', 'critical', 'warning') THEN
      RAISE EXCEPTION 'severities is critical, warning or both.' USING ERRCODE = 'DL400';
   END IF;
   IF v_match = '' THEN
      RAISE EXCEPTION 'Choose a family or enter an exact metric name.' USING ERRCODE = 'DL400';
   END IF;
   IF p_scope_server_id IS NOT NULL AND p_scope_tag_id IS NOT NULL THEN
      RAISE EXCEPTION 'A route is scoped to one server or one tag, not both.' USING ERRCODE = 'DL400';
   END IF;
   IF v_match = '*' AND p_scope_server_id IS NULL AND p_scope_tag_id IS NULL THEN
      RAISE EXCEPTION 'Every metric (*) needs a server or a tag scope. Pick one, or choose a family or an exact metric.' USING ERRCODE = 'DL400';
   END IF;
   IF p_scope_server_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_scope_server_id) THEN
      RAISE EXCEPTION 'No monitored server has that server_id.' USING ERRCODE = 'DL404';
   END IF;
   IF p_scope_tag_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {config}.server_tags WHERE id = p_scope_tag_id) THEN
      RAISE EXCEPTION 'No server tag has that id.' USING ERRCODE = 'DL404';
   END IF;
   IF p_fallback_route_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE route_id = p_fallback_route_id) THEN
      RAISE EXCEPTION 'The fallback route does not exist.' USING ERRCODE = 'DL404';
   END IF;
   -- Every destination column keeps its empty default, so the route has no destinations of its own: each channel
   -- goes where it went before (the next matching route, else the Settings channels), and no caller of this function can name a URL, a key or an address list.
   -- The agent columns keep their defaults too: this function never makes an agent route.
   INSERT INTO {config}.config_notification_routes
      (metric_match, enabled, scope_server_id, scope_tag_id, send_resolutions, fallback_route_id, severities, modified_at)
   VALUES
      (v_match, COALESCE(p_enabled, true), p_scope_server_id, p_scope_tag_id, p_send_resolutions, p_fallback_route_id, v_severities,
       (now() AT TIME ZONE 'UTC'))
   RETURNING route_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.create_notification_route(text, boolean, integer, integer, boolean, integer, text) FROM PUBLIC;
-- #6542 part 2: the actor is an argument now and the before row comes back, so the old signature is dropped and stays uncallable.
DROP FUNCTION IF EXISTS {config}.update_notification_route(integer, text[], text, boolean, integer, integer, boolean, integer);
-- #6928 (V229): p_severities joins the argument list, so the nine-argument function is dropped and stays uncallable. It is read only when
-- p_columns names severities.
DROP FUNCTION IF EXISTS {config}.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer);
CREATE OR REPLACE FUNCTION {config}.update_notification_route(
   p_route_id integer,
   p_actor text,
   p_columns text[],
   p_metric_match text,
   p_enabled boolean,
   p_scope_server_id integer,
   p_scope_tag_id integer,
   p_send_resolutions boolean,
   p_fallback_route_id integer,
   p_severities text DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_row {config}.config_notification_routes%ROWTYPE;
   -- #6542 part 2: an operator-class MCP token (MCP is full admin) changes a route with an agent setting too. Every token connects as the
   -- same mcp role, so this is the label the service passes, and the service's own tool gate is what holds an agent token away.
   v_operator boolean := COALESCE(p_actor ~ '^operator:[A-Za-z0-9._: -]{{1,64}}$', false);
   v_match text;
   v_enabled boolean;
   v_server integer;
   v_tag integer;
   v_resolutions boolean;
   v_fallback integer;
   v_severities text;
BEGIN
   IF p_columns IS NULL OR cardinality(p_columns) = 0
      OR EXISTS (SELECT 1 FROM unnest(p_columns) AS c(name)
                 WHERE c.name IS NULL OR c.name NOT IN ('metric_match', 'enabled', 'scope', 'send_resolutions', 'fallback_route_id', 'severities')) THEN
      RAISE EXCEPTION 'The columns to change are metric_match, enabled, scope, send_resolutions, fallback_route_id and severities.' USING ERRCODE = 'DL400';
   END IF;
   -- The row is read again here, under its lock, so the agent-route refusal and every check below are on what is stored now
   -- and not on what the caller read a moment ago.
   SELECT * INTO v_row FROM {config}.config_notification_routes WHERE route_id = p_route_id FOR UPDATE;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'No notification route has that route_id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_row.agent_level IS NOT NULL AND v_row.agent_level >= 1 AND NOT v_operator THEN
      RAISE EXCEPTION 'That route is an agent route: it is changed in the Viewer, not over MCP.' USING ERRCODE = 'DL403';
   END IF;
   -- What a route matches and which servers it covers decide which servers an agent route covers, so a route with an
   -- explicit agent setting (Off included) keeps both in the Viewer. A route's own destinations do not stop a change
   -- to what it matches: the destinations are set over MCP too. (#6231)
   IF ('metric_match' = ANY (p_columns) OR 'scope' = ANY (p_columns) OR 'severities' = ANY (p_columns)) AND v_row.agent_level IS NOT NULL AND NOT v_operator THEN
      RAISE EXCEPTION 'That route has an agent setting (Off), so what it matches and which servers it covers are changed in the Viewer or on the web dashboard, not over MCP. Turning it on, send_resolutions and fallback_route_id can still be changed here.' USING ERRCODE = 'DL403';
   END IF;
   -- An Off route keeps servers away from a broader agent route, so turning it off widens agent coverage: that is done in the Viewer
   -- or on the web dashboard. Turning it on stays here. (#6281)
   IF 'enabled' = ANY (p_columns) AND v_row.agent_level IS NOT NULL AND COALESCE(p_enabled, v_row.enabled) = false AND NOT v_operator THEN
      RAISE EXCEPTION 'That route has an agent setting (Off), so turning it off is done in the Viewer or on the web dashboard, not over MCP. Turning it on can still be done here.' USING ERRCODE = 'DL403';
   END IF;
   v_match := CASE WHEN 'metric_match' = ANY (p_columns) THEN btrim(COALESCE(p_metric_match, '')) ELSE v_row.metric_match END;
   v_enabled := CASE WHEN 'enabled' = ANY (p_columns) THEN COALESCE(p_enabled, v_row.enabled) ELSE v_row.enabled END;
   v_server := CASE WHEN 'scope' = ANY (p_columns) THEN p_scope_server_id ELSE v_row.scope_server_id END;
   v_tag := CASE WHEN 'scope' = ANY (p_columns) THEN p_scope_tag_id ELSE v_row.scope_tag_id END;
   v_resolutions := CASE WHEN 'send_resolutions' = ANY (p_columns) THEN p_send_resolutions ELSE v_row.send_resolutions END;
   v_fallback := CASE WHEN 'fallback_route_id' = ANY (p_columns) THEN p_fallback_route_id ELSE v_row.fallback_route_id END;
   v_severities := CASE WHEN 'severities' = ANY (p_columns) THEN lower(btrim(COALESCE(p_severities, ''))) ELSE v_row.severities END;
   IF v_severities NOT IN ('both', 'critical', 'warning') THEN
      RAISE EXCEPTION 'severities is critical, warning or both.' USING ERRCODE = 'DL400';
   END IF;
   IF v_match = '' THEN
      RAISE EXCEPTION 'Choose a family or enter an exact metric name.' USING ERRCODE = 'DL400';
   END IF;
   IF v_server IS NOT NULL AND v_tag IS NOT NULL THEN
      RAISE EXCEPTION 'A route is scoped to one server or one tag, not both.' USING ERRCODE = 'DL400';
   END IF;
   IF v_match = '*' AND v_server IS NULL AND v_tag IS NULL THEN
      RAISE EXCEPTION 'Every metric (*) needs a server or a tag scope. Pick one, or choose a family or an exact metric.' USING ERRCODE = 'DL400';
   END IF;
   IF v_server IS NOT NULL AND v_server IS DISTINCT FROM v_row.scope_server_id
      AND NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = v_server) THEN
      RAISE EXCEPTION 'No monitored server has that server_id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_tag IS NOT NULL AND v_tag IS DISTINCT FROM v_row.scope_tag_id
      AND NOT EXISTS (SELECT 1 FROM {config}.server_tags WHERE id = v_tag) THEN
      RAISE EXCEPTION 'No server tag has that id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_fallback IS NOT NULL AND v_fallback = p_route_id THEN
      RAISE EXCEPTION 'A route cannot be its own fallback route.' USING ERRCODE = 'DL400';
   END IF;
   IF v_fallback IS NOT NULL AND v_fallback IS DISTINCT FROM v_row.fallback_route_id
      AND NOT EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE route_id = v_fallback) THEN
      RAISE EXCEPTION 'The fallback route does not exist.' USING ERRCODE = 'DL404';
   END IF;
   -- The one UPDATE names no destination column: the sealed webhook values, the key, the signing keys and the address list
   -- stay exactly as stored.
   UPDATE {config}.config_notification_routes
      SET metric_match = v_match,
          enabled = v_enabled,
          scope_server_id = v_server,
          scope_tag_id = v_tag,
          send_resolutions = v_resolutions,
          fallback_route_id = v_fallback,
          severities = v_severities,
          modified_at = (now() AT TIME ZONE 'UTC')
    WHERE route_id = p_route_id;
   -- The row as it was before this change, in the journal's words: set_notification_route_agent journals it as the before of a
   -- route_agent_changed row in the same transaction, so a match or scope change made with the agent fields is named in the row.
   RETURN jsonb_build_object(
      'metric_match', btrim(v_row.metric_match), 'scope_server_id', v_row.scope_server_id, 'scope_tag_id', v_row.scope_tag_id,
      'agent_level', v_row.agent_level, 'agent_max_per_hour', v_row.agent_max_per_hour, 'agent_max_per_day', v_row.agent_max_per_day,
      'agent_lease_minutes', v_row.agent_lease_minutes, 'agent_expiry_minutes', v_row.agent_expiry_minutes, 'enabled', v_row.enabled,
      'severities', v_row.severities);
END
$fn$;
REVOKE ALL ON FUNCTION {config}.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer, text) FROM PUBLIC;
-- #6542: a named operator token sets a route's agent level, its four limits and its fallback over MCP through this function, and
-- the change is journaled as route_agent_changed under the token's name (operator:<name>), in the same transaction. Levels 0 to 3 are
-- set here (MCP is full admin). A level 3 server action still needs that server's switch (config_server_actions) and its remediation login.
-- p_before is the row update_notification_route returned in the same transaction, so the journal row names a match, scope or enabled change
-- made in the same call. The old signature is dropped and stays uncallable.
DROP FUNCTION IF EXISTS {config}.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean);
CREATE OR REPLACE FUNCTION {config}.set_notification_route_agent(
   p_route_id integer,
   p_actor text,
   p_columns text[],
   p_agent_level integer,
   p_agent_max_per_hour integer,
   p_agent_max_per_day integer,
   p_agent_lease_minutes integer,
   p_agent_expiry_minutes integer,
   p_fallback_route_id integer,
   p_created boolean,
   p_before jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_row {config}.config_notification_routes%ROWTYPE;
   v_level integer;
   v_hour integer;
   v_day integer;
   v_lease integer;
   v_expiry integer;
   v_fallback integer;
   v_changed text[] := ARRAY[]::text[];
   v_before jsonb;
   v_after jsonb;
BEGIN
   IF p_actor IS NULL OR p_actor !~ '^operator:[A-Za-z0-9._: -]{{1,64}}$' THEN
      RAISE EXCEPTION 'The actor is operator: and the operator token''s name, or the single token''s identity.' USING ERRCODE = 'DL400';
   END IF;
   -- No column is allowed only with p_before: a match, scope or enabled change to a route with an agent setting is journaled on its own.
   IF p_columns IS NULL OR (cardinality(p_columns) = 0 AND p_before IS NULL)
      OR EXISTS (SELECT 1 FROM unnest(p_columns) AS c(name)
                 WHERE c.name IS NULL OR c.name NOT IN ('agent_level', 'agent_max_per_hour', 'agent_max_per_day', 'agent_lease_minutes', 'agent_expiry_minutes', 'fallback_route_id')) THEN
      RAISE EXCEPTION 'The columns to change are agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes and fallback_route_id.' USING ERRCODE = 'DL400';
   END IF;
   SELECT * INTO v_row FROM {config}.config_notification_routes WHERE route_id = p_route_id FOR UPDATE;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'No notification route has that route_id.' USING ERRCODE = 'DL404';
   END IF;
   v_level := CASE WHEN 'agent_level' = ANY (p_columns) THEN p_agent_level ELSE v_row.agent_level END;
   v_hour := CASE WHEN 'agent_max_per_hour' = ANY (p_columns) THEN p_agent_max_per_hour ELSE v_row.agent_max_per_hour END;
   v_day := CASE WHEN 'agent_max_per_day' = ANY (p_columns) THEN p_agent_max_per_day ELSE v_row.agent_max_per_day END;
   v_lease := CASE WHEN 'agent_lease_minutes' = ANY (p_columns) THEN p_agent_lease_minutes ELSE v_row.agent_lease_minutes END;
   v_expiry := CASE WHEN 'agent_expiry_minutes' = ANY (p_columns) THEN p_agent_expiry_minutes ELSE v_row.agent_expiry_minutes END;
   v_fallback := CASE WHEN 'fallback_route_id' = ANY (p_columns) THEN p_fallback_route_id ELSE v_row.fallback_route_id END;
   IF v_level IS NOT NULL AND v_level NOT IN (0, 1, 2, 3) THEN
      RAISE EXCEPTION 'The agent level is Off (0), Investigate (1), Act (2) or Act on servers (3).' USING ERRCODE = 'DL400';
   END IF;
   IF v_hour IS NULL OR v_hour NOT BETWEEN 1 AND 60 THEN
      RAISE EXCEPTION 'Requests per hour must be between 1 and 60.' USING ERRCODE = 'DL400';
   END IF;
   IF v_day IS NULL OR v_day NOT BETWEEN 1 AND 500 THEN
      RAISE EXCEPTION 'Requests per day must be between 1 and 500.' USING ERRCODE = 'DL400';
   END IF;
   IF v_lease IS NULL OR v_lease NOT BETWEEN 5 AND 120 THEN
      RAISE EXCEPTION 'Lease minutes must be between 5 and 120.' USING ERRCODE = 'DL400';
   END IF;
   IF v_expiry IS NULL OR v_expiry NOT BETWEEN 15 AND 1440 THEN
      RAISE EXCEPTION 'Expiry minutes must be between 15 and 1440.' USING ERRCODE = 'DL400';
   END IF;
   IF v_fallback IS NOT NULL AND v_fallback = p_route_id THEN
      RAISE EXCEPTION 'A route cannot be its own fallback route.' USING ERRCODE = 'DL400';
   END IF;
   IF v_fallback IS NOT NULL AND v_fallback IS DISTINCT FROM v_row.fallback_route_id
      AND NOT EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE route_id = v_fallback) THEN
      RAISE EXCEPTION 'The fallback route does not exist.' USING ERRCODE = 'DL404';
   END IF;
   -- The names and their order are the Viewer's (NotificationRouteWriter.AgentChange).
   IF p_before IS NOT NULL THEN
      IF (p_before ->> 'metric_match') IS DISTINCT FROM btrim(v_row.metric_match) THEN v_changed := array_append(v_changed, 'metric_match'); END IF;
      IF (p_before ->> 'scope_server_id')::integer IS DISTINCT FROM v_row.scope_server_id THEN v_changed := array_append(v_changed, 'scope_server_id'); END IF;
      IF (p_before ->> 'scope_tag_id')::integer IS DISTINCT FROM v_row.scope_tag_id THEN v_changed := array_append(v_changed, 'scope_tag_id'); END IF;
      -- #6928: the severity filter moves alerts between agent routes, so it is named like the match and the scope. Only a route with an agent setting.
      IF (p_before ->> 'severities') IS DISTINCT FROM v_row.severities AND ((p_before ->> 'agent_level') IS NOT NULL OR v_level IS NOT NULL) THEN
         v_changed := array_append(v_changed, 'severities');
      END IF;
   END IF;
   IF v_level IS DISTINCT FROM v_row.agent_level THEN v_changed := array_append(v_changed, 'agent_level'); END IF;
   IF v_hour IS DISTINCT FROM v_row.agent_max_per_hour THEN v_changed := array_append(v_changed, 'agent_max_per_hour'); END IF;
   IF v_day IS DISTINCT FROM v_row.agent_max_per_day THEN v_changed := array_append(v_changed, 'agent_max_per_day'); END IF;
   IF v_lease IS DISTINCT FROM v_row.agent_lease_minutes THEN v_changed := array_append(v_changed, 'agent_lease_minutes'); END IF;
   IF v_expiry IS DISTINCT FROM v_row.agent_expiry_minutes THEN v_changed := array_append(v_changed, 'agent_expiry_minutes'); END IF;
   IF p_before IS NOT NULL AND ((p_before ->> 'agent_level') IS NOT NULL OR v_level IS NOT NULL)
      AND (p_before ->> 'enabled')::boolean IS DISTINCT FROM v_row.enabled THEN
      v_changed := array_append(v_changed, 'enabled');
   END IF;
   UPDATE {config}.config_notification_routes
      SET agent_level = v_level::smallint,
          agent_max_per_hour = v_hour,
          agent_max_per_day = v_day,
          agent_lease_minutes = v_lease,
          agent_expiry_minutes = v_expiry,
          fallback_route_id = v_fallback,
          modified_at = (now() AT TIME ZONE 'UTC')
    WHERE route_id = p_route_id;
   -- The journal row the Viewer's save writes (NotificationRouteWriter.AgentChange): a new route journals only when it sets a level,
   -- an edit when a level or a limit changed. A fallback alone changes no agent coverage, so it is not journaled, as in the Viewer.
   IF (COALESCE(p_created, false) AND v_level IS NOT NULL) OR (NOT COALESCE(p_created, false) AND cardinality(v_changed) > 0) THEN
      v_before := CASE WHEN COALESCE(p_created, false) THEN NULL WHEN p_before IS NOT NULL THEN p_before ELSE jsonb_build_object(
         'metric_match', btrim(v_row.metric_match), 'scope_server_id', v_row.scope_server_id, 'scope_tag_id', v_row.scope_tag_id,
         'agent_level', v_row.agent_level, 'agent_max_per_hour', v_row.agent_max_per_hour, 'agent_max_per_day', v_row.agent_max_per_day,
         'agent_lease_minutes', v_row.agent_lease_minutes, 'agent_expiry_minutes', v_row.agent_expiry_minutes, 'enabled', v_row.enabled,
         'severities', v_row.severities) END;
      v_after := jsonb_build_object(
         'metric_match', btrim(v_row.metric_match), 'scope_server_id', v_row.scope_server_id, 'scope_tag_id', v_row.scope_tag_id,
         'agent_level', v_level, 'agent_max_per_hour', v_hour, 'agent_max_per_day', v_day,
         'agent_lease_minutes', v_lease, 'agent_expiry_minutes', v_expiry, 'enabled', v_row.enabled,
         'severities', v_row.severities);
      INSERT INTO {config}.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail)
      VALUES (now(), NULL, p_route_id, v_row.scope_server_id, 'route_agent_changed', p_actor,
              CASE WHEN COALESCE(p_created, false) THEN 'created route' ELSE 'changed ' || array_to_string(v_changed, ', ') END,
              jsonb_build_object('before', v_before, 'after', v_after));
   END IF;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean, jsonb) FROM PUBLIC;
-- The return type changed (a previous value is now only a flag for whether one was saved), which CREATE OR REPLACE cannot do on its own.
-- #6542 part 2: the actor is an argument too, so the old signature is dropped and stays uncallable.
DROP FUNCTION IF EXISTS {config}.set_notification_route_destinations(integer, text[], text, text, text, text, text);
CREATE OR REPLACE FUNCTION {config}.set_notification_route_destinations(
   p_route_id integer,
   p_actor text,
   p_columns text[],
   p_teams_url text,
   p_slack_url text,
   p_generic_url text,
   p_pagerduty_routing_key text,
   p_smtp_recipients text)
RETURNS TABLE (prev_teams_url_set boolean, prev_slack_url_set boolean, prev_generic_url_set boolean, prev_pagerduty_routing_key_set boolean, prev_smtp_recipients_set boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_row {config}.config_notification_routes%ROWTYPE;
   v_teams text;
   v_slack text;
   v_generic text;
   v_key text;
   v_recipients text;
BEGIN
   IF p_columns IS NULL OR cardinality(p_columns) = 0
      OR EXISTS (SELECT 1 FROM unnest(p_columns) AS c(name)
                 WHERE c.name IS NULL OR c.name NOT IN ('teams_url', 'slack_url', 'generic_url', 'pagerduty_routing_key', 'smtp_recipients')) THEN
      RAISE EXCEPTION 'The destinations to change are teams_url, slack_url, generic_url, pagerduty_routing_key and smtp_recipients.' USING ERRCODE = 'DL400';
   END IF;
   -- The row is read under its lock, so every check below is on what is stored now. The agent-route rule of
   -- update_notification_route holds here too: an agent route is changed in the Viewer, or by an operator-class MCP token (#6542 part 2).
   SELECT * INTO v_row FROM {config}.config_notification_routes WHERE route_id = p_route_id FOR UPDATE;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'No notification route has that route_id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_row.agent_level IS NOT NULL AND v_row.agent_level >= 1 AND NOT COALESCE(p_actor ~ '^operator:[A-Za-z0-9._: -]{{1,64}}$', false) THEN
      RAISE EXCEPTION 'That route is an agent route: it is changed in the Viewer, not over MCP.' USING ERRCODE = 'DL403';
   END IF;
   v_teams := CASE WHEN 'teams_url' = ANY (p_columns) THEN COALESCE(p_teams_url, '') ELSE v_row.teams_url END;
   v_slack := CASE WHEN 'slack_url' = ANY (p_columns) THEN COALESCE(p_slack_url, '') ELSE v_row.slack_url END;
   v_generic := CASE WHEN 'generic_url' = ANY (p_columns) THEN COALESCE(p_generic_url, '') ELSE v_row.generic_url END;
   v_key := CASE WHEN 'pagerduty_routing_key' = ANY (p_columns) THEN COALESCE(p_pagerduty_routing_key, '') ELSE v_row.pagerduty_routing_key END;
   v_recipients := CASE WHEN 'smtp_recipients' = ANY (p_columns) THEN btrim(COALESCE(p_smtp_recipients, '')) ELSE v_row.smtp_recipients END;
   -- A webhook value is sealed by the Darling service before it gets here, for this route's own row. This function stores
   -- only an empty value or a sealed one, so no caller of it can store a destination as text. A changed value must be exactly
   -- the sealed shape (sealed:v1:, the 16 character key id, base64) and no longer than 16384 characters: a half-formed value
   -- turns the channel off for the route, and an oversized one is loaded on every configuration reload.
   IF EXISTS (SELECT 1
                FROM (VALUES (v_teams, v_row.teams_url), (v_slack, v_row.slack_url), (v_generic, v_row.generic_url), (v_key, v_row.pagerduty_routing_key))
                     AS w(new_value, old_value)
               WHERE w.new_value <> w.old_value AND w.new_value <> ''
                 AND (length(w.new_value) > 16384
                      OR w.new_value !~ '^sealed:v1:[0-9a-f]+:[A-Za-z0-9+/]+=?=?$'
                      OR length(split_part(w.new_value, ':', 3)) <> 16)) THEN
      RAISE EXCEPTION 'A webhook value is stored sealed by the Darling service, never as text.' USING ERRCODE = 'DL400';
   END IF;
   IF length(v_recipients) > 4000 THEN
      RAISE EXCEPTION 'The email recipients are longer than can be stored.' USING ERRCODE = 'DL400';
   END IF;
   -- A changed recipient list is a comma separated list of at most 50 plain addresses: no control character, angle bracket or
   -- double quote (so no display name or group), the rule the Viewer and the MCP tools apply before they get here.
   IF v_recipients <> v_row.smtp_recipients
      AND (v_recipients ~ '[[:cntrl:]<>]' OR position(chr(34) IN v_recipients) > 0
           OR (SELECT count(*) FROM unnest(string_to_array(v_recipients, ',')) AS e(item) WHERE btrim(e.item) <> '') > 50) THEN
      RAISE EXCEPTION 'The email recipients are a comma separated list of at most 50 plain addresses such as name@example.com.' USING ERRCODE = 'DL400';
   END IF;
   -- A signing key is sealed for the route's stored generic URL text (#5998): a change to that text needs the key removed first.
   IF v_row.signing_key <> '' AND v_generic IS DISTINCT FROM v_row.generic_url THEN
      RAISE EXCEPTION 'A signing key is saved for this route''s generic webhook, and it is tied to the generic webhook URL. Remove the key in the Viewer first, then change the URL.' USING ERRCODE = 'DL403';
   END IF;
   UPDATE {config}.config_notification_routes
      SET teams_url = v_teams,
          slack_url = v_slack,
          generic_url = v_generic,
          pagerduty_routing_key = v_key,
          smtp_recipients = v_recipients,
          modified_at = (now() AT TIME ZONE 'UTC')
    WHERE route_id = p_route_id;
   -- What was stored before comes back only as a flag for whether one was saved, and only for the columns this call names: the function runs
   -- with the owner's rights, so it never hands a stored value (sealed text, or a value saved before sealing) to its caller.
   RETURN QUERY SELECT
      CASE WHEN 'teams_url' = ANY (p_columns) THEN v_row.teams_url <> '' END,
      CASE WHEN 'slack_url' = ANY (p_columns) THEN v_row.slack_url <> '' END,
      CASE WHEN 'generic_url' = ANY (p_columns) THEN v_row.generic_url <> '' END,
      CASE WHEN 'pagerduty_routing_key' = ANY (p_columns) THEN v_row.pagerduty_routing_key <> '' END,
      CASE WHEN 'smtp_recipients' = ANY (p_columns) THEN v_row.smtp_recipients <> '' END;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.set_notification_route_destinations(integer, text, text[], text, text, text, text, text) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER functions (#6394) the MCP webhook signing key tools write through: <c>set_webhook_signing_key</c> and
    /// <c>clear_webhook_signing_key</c>. The saved URL and proxy a key is sealed for are read by the service on its own store connection, not
    /// through a function the <c>mcp</c> role could call; <c>read_webhook_signing_target</c>, which an earlier build of this change created,
    /// is dropped. A <c>NULL</c> route id is the settings row. The rotation
    /// window is the 7 days of <c>WebhookSigning.RotationOverlapDays</c>, as in the Viewer.
    /// </summary>
    internal static string BuildWebhookSigningKeyFunctionsSql(string config) => $@"
DROP FUNCTION IF EXISTS {config}.read_webhook_signing_target(integer);
CREATE OR REPLACE FUNCTION {config}.set_webhook_signing_key(p_route_id integer, p_sealed_key text, p_stored_url text, p_proxy text)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_url text;
   v_until timestamptz;
BEGIN
   IF p_sealed_key IS NULL OR length(p_sealed_key) > 16384
      OR p_sealed_key !~ '^sealed:v1:[0-9a-f]+:[A-Za-z0-9+/]+=?=?$'
      OR length(split_part(p_sealed_key, ':', 3)) <> 16 THEN
      RAISE EXCEPTION 'A signing key is stored sealed by the Darling service, never as text.' USING ERRCODE = 'DL400';
   END IF;
   IF p_route_id IS NULL THEN
      SELECT n.generic_url INTO v_url FROM {config}.config_notification AS n WHERE n.id = 1;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'The notification settings row does not exist yet.' USING ERRCODE = 'DL404';
      END IF;
   ELSE
      SELECT r.generic_url INTO v_url FROM {config}.config_notification_routes AS r WHERE r.route_id = p_route_id;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'No notification route with route_id %.', p_route_id USING ERRCODE = 'DL404';
      END IF;
   END IF;
   IF v_url !~ '^sealed:' THEN
      RAISE EXCEPTION 'There is no saved generic webhook URL to tie a key to. Save the generic webhook URL first, then generate the key.' USING ERRCODE = 'DL400';
   END IF;
   -- The Viewer's own writes (ViewerDataService.SigningKeys): the current key moves to the previous slot with a window of 7 days when there
   -- was one, and the write is guarded on the URL and proxy the key was sealed for still being what is stored. The route form reads the settings
   -- row's proxy FOR SHARE, so it waits for a Settings save that holds that row.
   IF p_route_id IS NULL THEN
      UPDATE {config}.config_notification
         SET previous_signing_key = signing_key,
             previous_signing_key_until = CASE WHEN signing_key <> '' THEN now() + make_interval(days => {PerformanceMonitor.Notifications.WebhookSigning.RotationOverlapDays}) END,
             signing_key = p_sealed_key
       WHERE id = 1 AND generic_url = p_stored_url AND generic_proxy = p_proxy
      RETURNING previous_signing_key_until INTO v_until;
   ELSE
      UPDATE {config}.config_notification_routes
         SET previous_signing_key = signing_key,
             previous_signing_key_until = CASE WHEN signing_key <> '' THEN now() + make_interval(days => {PerformanceMonitor.Notifications.WebhookSigning.RotationOverlapDays}) END,
             signing_key = p_sealed_key
       WHERE route_id = p_route_id AND generic_url = p_stored_url
         AND COALESCE((SELECT n.generic_proxy FROM {config}.config_notification AS n WHERE n.id = 1 FOR SHARE), '') = p_proxy
      RETURNING previous_signing_key_until INTO v_until;
   END IF;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'The generic webhook URL or proxy changed while the key was made. Nothing was written; try again.' USING ERRCODE = 'DL403';
   END IF;
   RETURN v_until;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.set_webhook_signing_key(integer, text, text, text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION {config}.clear_webhook_signing_key(p_route_id integer)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_url text;
   v_had boolean;
BEGIN
   IF p_route_id IS NULL THEN
      SELECT n.generic_url, n.signing_key <> '' INTO v_url, v_had FROM {config}.config_notification AS n WHERE n.id = 1 FOR UPDATE;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'The notification settings row does not exist yet.' USING ERRCODE = 'DL404';
      END IF;
   ELSE
      SELECT r.generic_url, r.signing_key <> '' INTO v_url, v_had FROM {config}.config_notification_routes AS r WHERE r.route_id = p_route_id FOR UPDATE;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'No notification route with route_id %.', p_route_id USING ERRCODE = 'DL404';
      END IF;
   END IF;
   -- A destination with no generic webhook has no key to remove (a key that is somehow still stored is cleared all the same).
   IF v_url = '' AND NOT v_had THEN
      RAISE EXCEPTION 'There is no generic webhook on this destination, so there is no signing key to remove.' USING ERRCODE = 'DL400';
   END IF;
   -- The Viewer's own clear: the key, the previous key and the window.
   IF p_route_id IS NULL THEN
      UPDATE {config}.config_notification SET signing_key = '', previous_signing_key = '', previous_signing_key_until = NULL WHERE id = 1;
   ELSE
      UPDATE {config}.config_notification_routes SET signing_key = '', previous_signing_key = '', previous_signing_key_until = NULL WHERE route_id = p_route_id;
   END IF;
   RETURN v_had;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.clear_webhook_signing_key(integer) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER function (#6392) the MCP <c>update_notification_channels</c> tool writes the default notification channels through: <c>set_notification_channels</c>. The <c>mcp</c> login holds no UPDATE on a secret column of <c>config_notification</c>; the service seals each secret and this function stores only sealed text or empty, makes the Viewer's refusals on the stored row, and returns only whether each secret was saved. Created and REVOKEd from PUBLIC here; the caller adds the GRANT EXECUTE to mcp only.
    /// </summary>
    internal static string BuildNotificationChannelsFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.set_notification_channels(
   p_columns text[],
   p_smtp_host text,
   p_smtp_port integer,
   p_smtp_use_ssl boolean,
   p_smtp_username text,
   p_smtp_password text,
   p_smtp_from_address text,
   p_smtp_recipients text,
   p_teams_url text,
   p_teams_proxy text,
   p_slack_url text,
   p_slack_proxy text,
   p_generic_url text,
   p_generic_headers text,
   p_generic_body_template text,
   p_generic_proxy text,
   p_pagerduty_routing_key text,
   p_pagerduty_use_eu_region boolean,
   p_pagerduty_proxy text,
   p_pagerduty_auto_resolve boolean)
RETURNS TABLE (prev_smtp_username_set boolean, prev_smtp_password_set boolean, prev_teams_url_set boolean, prev_slack_url_set boolean,
               prev_generic_url_set boolean, prev_generic_headers_set boolean, prev_pagerduty_routing_key_set boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_row {config}.config_notification%ROWTYPE;
   v_host text;
   v_port integer;
   v_ssl boolean;
   v_user text;
   v_password text;
   v_from text;
   v_recipients text;
   v_teams text;
   v_teams_proxy text;
   v_slack text;
   v_slack_proxy text;
   v_generic text;
   v_headers text;
   v_body text;
   v_generic_proxy text;
   v_key text;
   v_eu boolean;
   v_pd_proxy text;
   v_auto boolean;
BEGIN
   IF p_columns IS NULL OR cardinality(p_columns) = 0
      OR EXISTS (SELECT 1 FROM unnest(p_columns) AS c(name)
                 WHERE c.name IS NULL OR c.name NOT IN (
                    'smtp_host', 'smtp_port', 'smtp_use_ssl', 'smtp_username', 'smtp_encrypted_password', 'smtp_from_address', 'smtp_recipients',
                    'teams_url', 'teams_proxy', 'slack_url', 'slack_proxy', 'generic_url', 'generic_headers', 'generic_body_template',
                    'generic_proxy', 'pagerduty_routing_key', 'pagerduty_use_eu_region', 'pagerduty_proxy', 'pagerduty_auto_resolve')) THEN
      RAISE EXCEPTION 'The notification channel columns to change are the SMTP, Teams, Slack, generic webhook and PagerDuty settings.' USING ERRCODE = 'DL400';
   END IF;
   -- The row is read under its lock, so every check below is on what is stored now.
   SELECT * INTO v_row FROM {config}.config_notification WHERE id = 1 FOR UPDATE;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'The notification settings row does not exist yet.' USING ERRCODE = 'DL404';
   END IF;
   v_host := CASE WHEN 'smtp_host' = ANY (p_columns) THEN COALESCE(p_smtp_host, '') ELSE v_row.smtp_host END;
   v_port := CASE WHEN 'smtp_port' = ANY (p_columns) THEN COALESCE(p_smtp_port, v_row.smtp_port) ELSE v_row.smtp_port END;
   v_ssl := CASE WHEN 'smtp_use_ssl' = ANY (p_columns) THEN COALESCE(p_smtp_use_ssl, v_row.smtp_use_ssl) ELSE v_row.smtp_use_ssl END;
   v_user := CASE WHEN 'smtp_username' = ANY (p_columns) THEN COALESCE(p_smtp_username, '') ELSE COALESCE(v_row.smtp_username, '') END;
   v_password := CASE WHEN 'smtp_encrypted_password' = ANY (p_columns) THEN COALESCE(p_smtp_password, '') ELSE COALESCE(v_row.smtp_encrypted_password, '') END;
   v_from := CASE WHEN 'smtp_from_address' = ANY (p_columns) THEN COALESCE(p_smtp_from_address, '') ELSE v_row.smtp_from_address END;
   v_recipients := CASE WHEN 'smtp_recipients' = ANY (p_columns) THEN btrim(COALESCE(p_smtp_recipients, '')) ELSE v_row.smtp_recipients END;
   v_teams := CASE WHEN 'teams_url' = ANY (p_columns) THEN COALESCE(p_teams_url, '') ELSE v_row.teams_url END;
   v_teams_proxy := CASE WHEN 'teams_proxy' = ANY (p_columns) THEN COALESCE(p_teams_proxy, '') ELSE v_row.teams_proxy END;
   v_slack := CASE WHEN 'slack_url' = ANY (p_columns) THEN COALESCE(p_slack_url, '') ELSE v_row.slack_url END;
   v_slack_proxy := CASE WHEN 'slack_proxy' = ANY (p_columns) THEN COALESCE(p_slack_proxy, '') ELSE v_row.slack_proxy END;
   v_generic := CASE WHEN 'generic_url' = ANY (p_columns) THEN COALESCE(p_generic_url, '') ELSE v_row.generic_url END;
   v_headers := CASE WHEN 'generic_headers' = ANY (p_columns) THEN COALESCE(p_generic_headers, '') ELSE v_row.generic_headers END;
   v_body := CASE WHEN 'generic_body_template' = ANY (p_columns) THEN COALESCE(p_generic_body_template, '') ELSE v_row.generic_body_template END;
   v_generic_proxy := CASE WHEN 'generic_proxy' = ANY (p_columns) THEN COALESCE(p_generic_proxy, '') ELSE v_row.generic_proxy END;
   v_key := CASE WHEN 'pagerduty_routing_key' = ANY (p_columns) THEN COALESCE(p_pagerduty_routing_key, '') ELSE v_row.pagerduty_routing_key END;
   v_eu := CASE WHEN 'pagerduty_use_eu_region' = ANY (p_columns) THEN COALESCE(p_pagerduty_use_eu_region, v_row.pagerduty_use_eu_region) ELSE v_row.pagerduty_use_eu_region END;
   v_pd_proxy := CASE WHEN 'pagerduty_proxy' = ANY (p_columns) THEN COALESCE(p_pagerduty_proxy, '') ELSE v_row.pagerduty_proxy END;
   v_auto := CASE WHEN 'pagerduty_auto_resolve' = ANY (p_columns) THEN COALESCE(p_pagerduty_auto_resolve, v_row.pagerduty_auto_resolve) ELSE v_row.pagerduty_auto_resolve END;
   IF v_port < 1 OR v_port > 65535 THEN
      RAISE EXCEPTION 'The SMTP port is between 1 and 65535.' USING ERRCODE = 'DL400';
   END IF;
   IF length(v_host) > 1000 OR length(v_user) > 1000 OR length(v_from) > 1000 OR length(v_teams_proxy) > 2000 OR length(v_slack_proxy) > 2000
      OR length(v_generic_proxy) > 2000 OR length(v_pd_proxy) > 2000 OR length(v_body) > 16384 THEN
      RAISE EXCEPTION 'A notification channel setting is longer than can be stored.' USING ERRCODE = 'DL400';
   END IF;
   -- A secret is sealed by the Darling service before it gets here. This function stores only an empty value or a sealed one (the shape
   -- of sealed:v1:, the 16 character key id, base64, at most 16384 characters), so no caller of it can store a secret as text.
   IF EXISTS (SELECT 1
                FROM (VALUES (v_password, COALESCE(v_row.smtp_encrypted_password, '')), (v_teams, v_row.teams_url), (v_slack, v_row.slack_url),
                             (v_generic, v_row.generic_url), (v_headers, v_row.generic_headers), (v_key, v_row.pagerduty_routing_key))
                     AS w(new_value, old_value)
               WHERE w.new_value <> w.old_value AND w.new_value <> ''
                 AND (length(w.new_value) > 16384
                      OR w.new_value !~ '^sealed:v1:[0-9a-f]+:[A-Za-z0-9+/]+=?=?$'
                      OR length(split_part(w.new_value, ':', 3)) <> 16)) THEN
      RAISE EXCEPTION 'A secret is stored sealed by the Darling service, never as text.' USING ERRCODE = 'DL400';
   END IF;
   IF length(v_recipients) > 4000 THEN
      RAISE EXCEPTION 'The email recipients are longer than can be stored.' USING ERRCODE = 'DL400';
   END IF;
   IF v_recipients <> v_row.smtp_recipients
      AND (v_recipients ~ '[[:cntrl:]<>]' OR position(chr(34) IN v_recipients) > 0
           OR (SELECT count(*) FROM unnest(string_to_array(v_recipients, ',')) AS e(item) WHERE btrim(e.item) <> '') > 50) THEN
      RAISE EXCEPTION 'The email recipients are a comma separated list of at most 50 plain addresses such as name@example.com.' USING ERRCODE = 'DL400';
   END IF;
   -- A saved secret opens only for what it was sealed for (#5366), and this function cannot show a secret to the caller, so the same
   -- refusals the Viewer's Settings window makes are made here, on the stored row: a change to what a saved value is tied to needs the
   -- value given again in the same call.
   IF COALESCE(v_row.smtp_encrypted_password, '') <> '' AND NOT ('smtp_encrypted_password' = ANY (p_columns))
      AND (v_host IS DISTINCT FROM v_row.smtp_host OR v_port IS DISTINCT FROM v_row.smtp_port OR v_ssl IS DISTINCT FROM v_row.smtp_use_ssl
           OR v_user IS DISTINCT FROM COALESCE(v_row.smtp_username, '')) THEN
      RAISE EXCEPTION 'Changing how mail is sent needs the SMTP password again.' USING ERRCODE = 'DL403';
   END IF;
   IF v_teams_proxy IS DISTINCT FROM v_row.teams_proxy AND v_row.teams_url <> '' AND NOT ('teams_url' = ANY (p_columns)) THEN
      RAISE EXCEPTION 'Enter the Teams webhook URL again: its proxy changed.' USING ERRCODE = 'DL403';
   END IF;
   IF v_slack_proxy IS DISTINCT FROM v_row.slack_proxy AND v_row.slack_url <> '' AND NOT ('slack_url' = ANY (p_columns)) THEN
      RAISE EXCEPTION 'Enter the Slack webhook URL again: its proxy changed.' USING ERRCODE = 'DL403';
   END IF;
   IF v_pd_proxy IS DISTINCT FROM v_row.pagerduty_proxy AND v_row.pagerduty_routing_key <> '' AND NOT ('pagerduty_routing_key' = ANY (p_columns)) THEN
      RAISE EXCEPTION 'Enter the PagerDuty routing key again: its proxy changed.' USING ERRCODE = 'DL403';
   END IF;
   IF v_generic_proxy IS DISTINCT FROM v_row.generic_proxy
      AND ((v_row.generic_url <> '' AND NOT ('generic_url' = ANY (p_columns))) OR (v_row.generic_headers <> '' AND NOT ('generic_headers' = ANY (p_columns)))) THEN
      RAISE EXCEPTION 'Enter the generic webhook URL and headers again: the proxy changed.' USING ERRCODE = 'DL403';
   END IF;
   -- The headers are sealed for the generic URL text as stored, so the two change together.
   IF 'generic_url' = ANY (p_columns) AND v_generic <> v_row.generic_url AND v_row.generic_headers <> '' AND NOT ('generic_headers' = ANY (p_columns)) THEN
      RAISE EXCEPTION 'Enter the generic webhook headers again: the generic webhook URL changed.' USING ERRCODE = 'DL403';
   END IF;
   IF 'generic_headers' = ANY (p_columns) AND v_headers <> '' AND v_headers <> v_row.generic_headers AND NOT ('generic_url' = ANY (p_columns)) THEN
      RAISE EXCEPTION 'Type the generic webhook URL again to change its headers.' USING ERRCODE = 'DL403';
   END IF;
   -- A signing key is sealed for the generic URL text and proxy (#5998): a change to either needs the key removed first.
   IF (v_generic <> v_row.generic_url AND v_row.signing_key <> '')
      OR (v_generic_proxy IS DISTINCT FROM v_row.generic_proxy
          AND (v_row.signing_key <> '' OR EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE signing_key <> ''))) THEN
      RAISE EXCEPTION 'A signing key is saved for this destination, and it is tied to the generic webhook URL and proxy. Remove the key first, save the change, then generate a new key.' USING ERRCODE = 'DL403';
   END IF;
   -- A route's own destination value is sealed for its channel's proxy on this row, so a proxy change would turn that route's channel off on
   -- the next reload (and leave a value saved before sealing to go out through the new proxy): it is refused while any route holds one (#6392).
   IF (v_teams_proxy IS DISTINCT FROM v_row.teams_proxy AND EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE teams_url <> ''))
      OR (v_slack_proxy IS DISTINCT FROM v_row.slack_proxy AND EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE slack_url <> ''))
      OR (v_generic_proxy IS DISTINCT FROM v_row.generic_proxy AND EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE generic_url <> ''))
      OR (v_pd_proxy IS DISTINCT FROM v_row.pagerduty_proxy AND EXISTS (SELECT 1 FROM {config}.config_notification_routes WHERE pagerduty_routing_key <> '')) THEN
      RAISE EXCEPTION 'A notification route holds its own value for a channel whose proxy is changing, and that value is tied to the proxy. Clear the value on each route first, change the proxy, then enter the value on the route again.' USING ERRCODE = 'DL403';
   END IF;
   UPDATE {config}.config_notification
      SET smtp_host = v_host,
          smtp_port = v_port,
          smtp_use_ssl = v_ssl,
          smtp_username = NULLIF(v_user, ''),
          smtp_encrypted_password = NULLIF(v_password, ''),
          smtp_from_address = v_from,
          smtp_recipients = v_recipients,
          teams_url = v_teams,
          teams_proxy = v_teams_proxy,
          slack_url = v_slack,
          slack_proxy = v_slack_proxy,
          generic_url = v_generic,
          generic_headers = v_headers,
          generic_body_template = v_body,
          generic_proxy = v_generic_proxy,
          pagerduty_routing_key = v_key,
          pagerduty_use_eu_region = v_eu,
          pagerduty_proxy = v_pd_proxy,
          pagerduty_auto_resolve = v_auto,
          modified_at = (now() AT TIME ZONE 'UTC')
    WHERE id = 1;
   -- What was stored before comes back only as a flag for whether a secret was saved, and only for the columns this call names: the
   -- function runs with the owner's rights, so it never hands a stored secret (sealed text, or a value saved before sealing) to its caller.
   RETURN QUERY SELECT
      CASE WHEN 'smtp_username' = ANY (p_columns) THEN COALESCE(v_row.smtp_username, '') <> '' END,
      CASE WHEN 'smtp_encrypted_password' = ANY (p_columns) THEN COALESCE(v_row.smtp_encrypted_password, '') <> '' END,
      CASE WHEN 'teams_url' = ANY (p_columns) THEN v_row.teams_url <> '' END,
      CASE WHEN 'slack_url' = ANY (p_columns) THEN v_row.slack_url <> '' END,
      CASE WHEN 'generic_url' = ANY (p_columns) THEN v_row.generic_url <> '' END,
      CASE WHEN 'generic_headers' = ANY (p_columns) THEN v_row.generic_headers <> '' END,
      CASE WHEN 'pagerduty_routing_key' = ANY (p_columns) THEN v_row.pagerduty_routing_key <> '' END;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.set_notification_channels(text[], text, integer, boolean, text, text, text, text, text, text, text, text, text, text, text, text, text, boolean, text, boolean) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER read function (#6230) behind <c>get_command_result</c> and the collector-run state read: one command's
    /// status and result, for the command types the MCP tools queue only (<c>analyze_now</c>, <c>purge_now</c>, <c>test_connect</c>,
    /// <c>pause</c>, <c>resume</c> and <c>snapshot_now</c>). Any other type answers no row. The <c>mcp</c> role holds no column
    /// SELECT on <c>result_json</c> (<see cref="McpWithheldColumns"/>), so this is its only way to that column. Granted to
    /// <c>mcp</c> only.
    /// </summary>
    internal static string BuildReadMcpCommandResultFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.read_mcp_command_result(p_command_id bigint)
RETURNS TABLE (
   command_type text,
   target_server_id integer,
   status text,
   requested_by text,
   created_at timestamp,
   claimed_at timestamp,
   completed_at timestamp,
   result_status text,
   result_json jsonb)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
   SELECT c.command_type, c.target_server_id, c.status, c.requested_by, c.created_at, c.claimed_at, c.completed_at,
          c.result_status, c.result_json
   FROM {config}.config_command AS c
   WHERE c.command_id = p_command_id
     AND c.command_type IN ('analyze_now', 'purge_now', 'test_connect', 'pause', 'resume', 'snapshot_now');
$fn$;
REVOKE ALL ON FUNCTION {config}.read_mcp_command_result(bigint) FROM PUBLIC;";

    /// <summary>
    /// The SECURITY DEFINER function (#6230) the MCP incident tools queue a command through: <c>analyze_now</c>, <c>purge_now</c>
    /// (with an optional retention), <c>test_connect</c> (built from the stored server row of an enabled server, so no credential or connection string
    /// is ever an argument), <c>pause</c> and <c>resume</c> (the two the Viewer queues). Any other command type is refused in the body. The row is always
    /// stamped <c>mcp</c>, a transaction above READ COMMITTED is refused, and the lock is a try-lock that answers busy. Granted to the <c>mcp</c> role only.
    /// </summary>
    internal static string BuildRequestMcpCommandFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.request_mcp_command(
   p_command_type text,
   p_server_id integer,
   p_retention_days integer,
   p_requested_by text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_type text := lower(btrim(COALESCE(p_command_type, '')));
   v_args jsonb;
   v_id bigint;
   v_last_status text;
   v_last_at timestamp;
BEGIN
   -- The only command types the mcp role may queue (#6230): the two the Viewer's pause and resume buttons queue, plus analyze_now,
   -- purge_now and test_connect. Everything else the command plane runs (disable_server, reload, the plan and execute commands) stays
   -- out of this function on purpose: it is the one way mcp writes a command row.
   IF v_type NOT IN ('pause', 'resume', 'analyze_now', 'purge_now', 'test_connect') THEN
      RAISE EXCEPTION 'That command type cannot be queued through this function.' USING ERRCODE = 'DL400';
   END IF;
   -- The one-request-at-a-time rule below reads the newest row of its kind with a fresh snapshot per statement. A transaction that
   -- keeps one snapshot (REPEATABLE READ and up) could miss a row another session just queued, so it is refused.
   IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'This request must run at the READ COMMITTED isolation level.' USING ERRCODE = 'DL400';
   END IF;
   IF p_retention_days IS NOT NULL AND (v_type <> 'purge_now' OR p_retention_days < 1 OR p_retention_days > 3650) THEN
      RAISE EXCEPTION 'retention_days is only for purge_now and must be 1 to 3650.' USING ERRCODE = 'DL400';
   END IF;
   IF v_type IN ('purge_now', 'pause', 'resume') THEN
      IF p_server_id IS NOT NULL THEN
         RAISE EXCEPTION 'This command takes no server.' USING ERRCODE = 'DL400';
      END IF;
   ELSIF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   -- A connection test dials the server with its stored credentials, so it runs only against a server the service is monitoring.
   IF v_type = 'test_connect' AND NOT EXISTS (SELECT 1 FROM {config}.config_monitored_servers WHERE server_id = p_server_id AND is_enabled) THEN
      RAISE EXCEPTION 'A connection test runs only against an enabled server.' USING ERRCODE = 'DL400';
   END IF;
   IF v_type = 'purge_now' AND p_retention_days IS NOT NULL THEN
      v_args := jsonb_build_object('retention_days', p_retention_days);
   END IF;
   -- A connection test is built HERE from the stored row. The caller supplies a server id and nothing else, so there is no
   -- argument that carries a credential, a host or a connection string, and the password is the stored one (the service
   -- tests it only against the server that holds it).
   IF v_type = 'test_connect' THEN
      SELECT jsonb_strip_nulls(jsonb_build_object(
                'name', s.name, 'engine', s.engine, 'host', s.host, 'port', s.port, 'database', s.database,
                'auth', s.auth, 'username', s.username, 'encryptedPassword', s.encrypted_password,
                'readOnlyIntent', s.read_only_intent, 'trustServerCertificate', s.trust_server_certificate,
                'encryptMode', s.encrypt_mode, 'multiSubnetFailover', s.multi_subnet_failover,
                'queryStoreReplica', NULLIF(btrim(to_jsonb(s) ->> 'query_store_replica'), '')))
        INTO v_args
      FROM {config}.config_monitored_servers AS s
      WHERE s.server_id = p_server_id;
   END IF;
   -- One request of a kind (per server where it has one) at a time, and not within 15 seconds of the last, so a holder of the
   -- mcp login cannot fill the queue. The lock is a try-lock: a request that cannot take it at once answers busy and never waits.
   IF NOT pg_try_advisory_xact_lock(6230, hashtext(v_type || ':' || COALESCE(p_server_id::text, ''))) THEN
      RAISE EXCEPTION 'Another request of this kind is being queued right now (busy); try again shortly.' USING ERRCODE = 'DL423';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM {config}.config_command AS c
   WHERE c.command_type = v_type AND c.target_server_id IS NOT DISTINCT FROM p_server_id
   ORDER BY c.command_id DESC
   LIMIT 1;
   IF v_last_status IN ('pending', 'in_progress') THEN
      RAISE EXCEPTION 'The same request is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   IF v_last_at IS NOT NULL AND v_last_at > (now() AT TIME ZONE 'UTC') - interval '15 seconds' THEN
      RAISE EXCEPTION 'The same request was made less than 15 seconds ago.' USING ERRCODE = 'DL429';
   END IF;
   -- The caller's p_requested_by is ignored (the parameter stays so an older caller still binds): every row this function queues is
   -- stamped 'mcp', so a row cannot carry another actor's name.
   INSERT INTO {config}.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES ('mcp', v_type, p_server_id, v_args, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION {config}.request_mcp_command(text, integer, integer, text) FROM PUBLIC;";

    // A new column on config.config_monitored_servers is added to this list, EXCEPT a column that holds a secret or that decides where
    // the service connects with credentials it holds: those change only through config.edit_monitored_server (which checks the
    // stored password binding and, over MCP, the agent-route guard) and go into the exclusion list of ServerEditLiveTests (the
    // NOT IN (...) beside the column-set check) instead. query_store_replica (#6254) is one: never add it here, a direct UPDATE
    // would skip the edit core.
    /// <summary>The columns of <c>config.config_monitored_servers</c> the <c>mcp</c> login may UPDATE directly (#5452): every column except the AWS role, the external ID, the generated external-ID flag and <c>query_store_replica</c> (#6254), which change only through <c>config.edit_monitored_server</c>. <c>tools/provision-roles.sql</c> carries the same list.</summary>
    internal const string McpMonitoredServerUpdateColumns =
        "server_id, name, host, database, auth, username, encrypted_password, encrypt_mode, trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans, is_enabled, created_at, modified_at, alert_delivery_mode_override, engine, port, plan_force_bot_enabled, remediation_username, remediation_encrypted_password";

    /// <summary>The argument types of <c>config.edit_monitored_server</c> (#5240, 18 since #6254), in order. The one place the signature is
    /// spelled for the <c>GRANT EXECUTE</c>, so a changed parameter list cannot leave the grant naming another function.</summary>
    internal const string EditMonitoredServerSignature = "integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text, text";

    /// <summary>The argument types of the 17-argument <c>config.edit_monitored_server</c> that callers built before the Query
    /// Store replica column (#6254) still call: a permanent wrapper over <see cref="EditMonitoredServerSignature"/>'s function
    /// (<see cref="BuildEditMonitoredServerSeventeenWrapperSql"/>).</summary>
    internal const string EditMonitoredServerSeventeenSignature = "integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text";

    /// <summary>The argument types of the 15-argument <c>config.edit_monitored_server</c> that callers built before the AWS
    /// role columns (#5452) still call: a permanent wrapper over <see cref="EditMonitoredServerSignature"/>'s function
    /// (<see cref="BuildEditMonitoredServerLegacyWrapperSql"/>).</summary>
    internal const string EditMonitoredServerLegacySignature = "integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric";

    /// <summary>
    /// The <c>SECURITY DEFINER</c> function (#5240) that is the one write of a monitored-server edit, for the web edit route
    /// (viewer) and the MCP <c>edit_server</c> tool (mcp). Returns the <c>CREATE OR REPLACE FUNCTION</c> plus the
    /// <c>REVOKE ALL ... FROM PUBLIC</c>; the caller adds the <c>GRANT EXECUTE</c>. It locks the row, compares
    /// <c>modified_at</c> to the token the edit read (exact to the microsecond), works out a change of host or port
    /// itself from the stored row, answers <c>remediation_kept</c> instead of writing when the row holds a remediation login,
    /// and answers <c>password_needed</c> instead of writing when that change, or a switch
    /// of authentication mode, would keep the stored secret on a SQL or service-principal row. A row whose
    /// authentication stores no secret never keeps one. A missing value for a required column, or a value the table's
    /// checks refuse, answers <c>invalid_value</c> instead of writing. Same builder for the managed batch, the script's text and the
    /// live tests, so none drifts. Pinned <c>search_path</c>, fixed statements, no dynamic SQL.
    /// </summary>
    internal static string BuildEditMonitoredServerFunctionSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.edit_monitored_server(
   p_server_id integer,
   p_expected_modified_at timestamp,
   p_columns text[],
   p_name text,
   p_host text,
   p_port integer,
   p_database text,
   p_read_only_intent boolean,
   p_auth text,
   p_username text,
   p_secret text,
   p_encrypt_mode text,
   p_trust_server_certificate boolean,
   p_multi_subnet_failover boolean,
   p_monthly_cost_usd numeric,
   p_aws_role_arn text,
   p_aws_external_id text,
   p_query_store_replica text)
RETURNS TABLE (outcome text, new_modified_at timestamp)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_old_modified_at timestamp;
   v_old_host text;
   v_old_port integer;
   v_old_auth text;
   v_old_database text;
   v_old_read_only_intent boolean;
   v_old_username text;
   v_old_encrypt_mode text;
   v_old_trust_server_certificate boolean;
   v_old_multi_subnet_failover boolean;
   v_old_query_store_replica text;
   v_query_store_replica text;
   v_has_replica boolean;
   v_host text;
   v_port integer;
   v_auth text;
   v_database text;
   v_read_only_intent boolean;
   v_username text;
   v_encrypt_mode text;
   v_trust_server_certificate boolean;
   v_multi_subnet_failover boolean;
   v_secret_auth boolean;
   v_new_secret boolean;
   v_secret_set boolean;
   v_secret text;
   v_remediation_held boolean;
   v_connection_changed boolean;
   v_old_role text;
   v_old_ext text;
   v_role text;
   v_ext text;
BEGIN
   p_columns := COALESCE(p_columns, ARRAY[]::text[]);

   SELECT s.modified_at, s.host, s.port, s.auth, s.database, s.read_only_intent, s.username, s.encrypt_mode,
          s.trust_server_certificate, s.multi_subnet_failover,
          (COALESCE(s.remediation_encrypted_password, '') <> '' OR COALESCE((to_jsonb(s) ->> 'hypothetical_index_encrypted_password'), '') <> ''),
          s.aws_role_arn, s.aws_external_id, NULLIF(btrim(to_jsonb(s) ->> 'query_store_replica'), '')
   INTO v_old_modified_at, v_old_host, v_old_port, v_old_auth, v_old_database, v_old_read_only_intent,
        v_old_username, v_old_encrypt_mode, v_old_trust_server_certificate, v_old_multi_subnet_failover,
        v_remediation_held, v_old_role, v_old_ext, v_old_query_store_replica
   FROM {config}.config_monitored_servers AS s
   WHERE s.server_id = p_server_id
   FOR UPDATE OF s;

   IF NOT FOUND THEN
      RETURN QUERY SELECT 'not_found'::text, NULL::timestamp;
      RETURN;
   END IF;

   -- The token is compared as it was read, to the microsecond.
   IF v_old_modified_at IS DISTINCT FROM p_expected_modified_at THEN
      RETURN QUERY SELECT 'conflict'::text, v_old_modified_at;
      RETURN;
   END IF;

   v_host := CASE WHEN 'host' = ANY (p_columns) THEN btrim(p_host) ELSE v_old_host END;
   v_port := CASE WHEN 'port' = ANY (p_columns) THEN p_port ELSE v_old_port END;
   v_auth := CASE WHEN 'auth' = ANY (p_columns) THEN p_auth ELSE v_old_auth END;
   v_database := CASE WHEN 'database' = ANY (p_columns) THEN p_database ELSE v_old_database END;
   v_read_only_intent := CASE WHEN 'read_only_intent' = ANY (p_columns) THEN p_read_only_intent ELSE v_old_read_only_intent END;
   v_username := CASE WHEN 'username' = ANY (p_columns) THEN p_username ELSE v_old_username END;
   v_encrypt_mode := CASE WHEN 'encrypt_mode' = ANY (p_columns) THEN p_encrypt_mode ELSE v_old_encrypt_mode END;
   v_trust_server_certificate := CASE WHEN 'trust_server_certificate' = ANY (p_columns) THEN p_trust_server_certificate ELSE v_old_trust_server_certificate END;
   v_multi_subnet_failover := CASE WHEN 'multi_subnet_failover' = ANY (p_columns) THEN p_multi_subnet_failover ELSE v_old_multi_subnet_failover END;
   -- The Query Store replica (#6254): trimmed, blank is none. Left out of p_columns, the stored one is kept. It is a host the stored
   -- password would be sent to, so a change of it is a change of how the row connects, below.
   v_query_store_replica := CASE WHEN 'query_store_replica' = ANY (p_columns) THEN NULLIF(btrim(p_query_store_replica), '') ELSE v_old_query_store_replica END;
   -- The AWS role (#5452). Left out of p_columns, the stored role and external ID are kept. A role named in p_columns
   -- with a NULL value clears it, and a cleared role takes its external ID with it.
   v_role := CASE WHEN 'aws_role_arn' = ANY (p_columns) THEN p_aws_role_arn ELSE v_old_role END;
   v_ext := CASE WHEN v_role IS NULL THEN NULL
                 WHEN 'aws_external_id' = ANY (p_columns) THEN p_aws_external_id
                 ELSE v_old_ext END;
   v_secret_auth := lower(v_auth) IN ('sql', 'serviceprincipal');
   v_new_secret := 'encrypted_password' = ANY (p_columns) AND COALESCE(p_secret, '') <> '';

   -- A NULL for a column the table requires is refused with a plain outcome: the edit sends nothing the table would
   -- have to turn away.
   IF ('name' = ANY (p_columns) AND p_name IS NULL)
      OR v_host IS NULL OR v_port IS NULL OR v_auth IS NULL OR v_encrypt_mode IS NULL
      OR v_read_only_intent IS NULL OR v_trust_server_certificate IS NULL OR v_multi_subnet_failover IS NULL
      OR ('monthly_cost_usd' = ANY (p_columns) AND p_monthly_cost_usd IS NULL) THEN
      RETURN QUERY SELECT 'invalid_value'::text, NULL::timestamp;
      RETURN;
   END IF;

   -- The store takes the password itself from these roles: a secret that starts with env: or file: (a reference,
   -- compared as the service reads one, case-sensitive and at the start of the text) is refused. References are set
   -- in the configuration file.
   IF v_new_secret AND (left(p_secret, 4) = 'env:' OR left(p_secret, 5) = 'file:') THEN
      RETURN QUERY SELECT 'reference_refused'::text, NULL::timestamp;
      RETURN;
   END IF;

   -- A new role would take the stored external ID with it unseen; the caller sends the ID again or clears it.
   IF v_role IS NOT NULL AND v_role IS DISTINCT FROM v_old_role AND v_old_ext IS NOT NULL
      AND NOT ('aws_external_id' = ANY (p_columns)) THEN
      RETURN QUERY SELECT 'external_id_needed'::text, NULL::timestamp;
      RETURN;
   END IF;

   -- An external ID with no role is refused (the table's check would refuse it too).
   IF v_role IS NULL AND 'aws_external_id' = ANY (p_columns) AND p_aws_external_id IS NOT NULL THEN
      RETURN QUERY SELECT 'external_id_needs_role'::text, NULL::timestamp;
      RETURN;
   END IF;

   -- Any change to how the row connects (host, port, database, read-only intent, authentication mode, username,
   -- encryption, certificate trust, multi-subnet failover, Query Store replica) never keeps the stored secret on a row that has one:
   -- the same set the route refuses (#5240). The caller's word is not taken: the change is worked out from the row.
   v_connection_changed := v_host IS DISTINCT FROM v_old_host
      OR v_port IS DISTINCT FROM v_old_port
      OR v_database IS DISTINCT FROM v_old_database
      OR v_read_only_intent IS DISTINCT FROM v_old_read_only_intent
      OR lower(v_auth) IS DISTINCT FROM lower(v_old_auth)
      OR v_username IS DISTINCT FROM v_old_username
      OR lower(v_encrypt_mode) IS DISTINCT FROM lower(v_old_encrypt_mode)
      OR v_trust_server_certificate IS DISTINCT FROM v_old_trust_server_certificate
      OR v_multi_subnet_failover IS DISTINCT FROM v_old_multi_subnet_failover
      OR v_query_store_replica IS DISTINCT FROM v_old_query_store_replica;

   -- A row that holds a remediation secret, or a hypothetical-index login secret (#6170), is not moved from here,
   -- whatever else the call sends: that secret is set and changed on the service host, and a new main password
   -- does not replace it.
   IF v_connection_changed AND v_remediation_held THEN
      RETURN QUERY SELECT 'remediation_kept'::text, NULL::timestamp;
      RETURN;
   END IF;

   IF v_secret_auth AND NOT v_new_secret AND v_connection_changed THEN
      RETURN QUERY SELECT 'password_needed'::text, NULL::timestamp;
      RETURN;
   END IF;

   -- A row whose authentication stores no secret never keeps one.
   IF NOT v_secret_auth THEN
      v_secret_set := true;
      v_secret := NULL;
   ELSIF 'encrypted_password' = ANY (p_columns) THEN
      v_secret_set := true;
      v_secret := p_secret;
   ELSE
      v_secret_set := false;
      v_secret := NULL;
   END IF;

   v_has_replica := EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = '{config}.config_monitored_servers'::regclass
                           AND attname = 'query_store_replica' AND NOT attisdropped);

   -- A value the table's own checks refuse (a required column, a format, a duplicate) answers invalid_value too:
   -- the table's error text is not passed on.
   BEGIN
      -- A store below V213 has no query_store_replica column (a script run before the service migrated it): the write leaves it out there.
      IF v_has_replica THEN
         UPDATE {config}.config_monitored_servers AS s
         SET name = CASE WHEN 'name' = ANY (p_columns) THEN p_name ELSE s.name END,
             host = v_host,
             port = v_port,
             database = v_database,
             read_only_intent = v_read_only_intent,
             auth = v_auth,
             username = v_username,
             encrypted_password = CASE WHEN v_secret_set THEN v_secret ELSE s.encrypted_password END,
             encrypt_mode = v_encrypt_mode,
             trust_server_certificate = v_trust_server_certificate,
             multi_subnet_failover = v_multi_subnet_failover,
             query_store_replica = v_query_store_replica,
             monthly_cost_usd = CASE WHEN 'monthly_cost_usd' = ANY (p_columns) THEN p_monthly_cost_usd ELSE s.monthly_cost_usd END,
             aws_role_arn = v_role,
             aws_external_id = v_ext,
             modified_at = (now() AT TIME ZONE 'UTC')
         WHERE s.server_id = p_server_id
         RETURNING s.modified_at INTO new_modified_at;
      ELSE
         UPDATE {config}.config_monitored_servers AS s
         SET name = CASE WHEN 'name' = ANY (p_columns) THEN p_name ELSE s.name END,
             host = v_host,
             port = v_port,
             database = v_database,
             read_only_intent = v_read_only_intent,
             auth = v_auth,
             username = v_username,
             encrypted_password = CASE WHEN v_secret_set THEN v_secret ELSE s.encrypted_password END,
             encrypt_mode = v_encrypt_mode,
             trust_server_certificate = v_trust_server_certificate,
             multi_subnet_failover = v_multi_subnet_failover,
             monthly_cost_usd = CASE WHEN 'monthly_cost_usd' = ANY (p_columns) THEN p_monthly_cost_usd ELSE s.monthly_cost_usd END,
             aws_role_arn = v_role,
             aws_external_id = v_ext,
             modified_at = (now() AT TIME ZONE 'UTC')
         WHERE s.server_id = p_server_id
         RETURNING s.modified_at INTO new_modified_at;
      END IF;
   EXCEPTION WHEN integrity_constraint_violation THEN
      RETURN QUERY SELECT 'invalid_value'::text, NULL::timestamp;
      RETURN;
   END;

   -- A registration moved to another address no longer vouches for the machine it reached before (#6340 round 2, N2): the machine
   -- identity the last connect stored (collect.servers.machine_identity, V212) belongs to the old address, so it is cleared here and the
   -- next connect writes the new one. A store that predates V212 has no such column; the edit is not held up by that.
   IF v_host IS DISTINCT FROM v_old_host OR v_port IS DISTINCT FROM v_old_port
      OR v_database IS DISTINCT FROM v_old_database OR v_read_only_intent IS DISTINCT FROM v_old_read_only_intent THEN
      BEGIN
         UPDATE {PgSchemaGenerator.CollectSchema}.servers SET machine_identity = NULL WHERE server_id = p_server_id;
      EXCEPTION WHEN undefined_column OR undefined_table THEN
         NULL;
      END;
   END IF;

   outcome := 'saved';
   RETURN NEXT;
END;
$fn$;
REVOKE ALL ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerSignature}) FROM PUBLIC;";

    /// <summary>
    /// The 17-argument <c>config.edit_monitored_server</c> (#5452), kept for good as a wrapper over the 18-argument function
    /// (#6254), for a caller built before the Query Store replica column existed. It passes NULL for the replica and removes
    /// <c>query_store_replica</c> from <c>p_columns</c> first, so a caller of this signature cannot change or clear a replica,
    /// however it builds its list. <c>SECURITY DEFINER</c> with a pinned <c>search_path</c>, revoked from PUBLIC; the caller adds
    /// the <c>GRANT EXECUTE</c>. Must run after <see cref="BuildEditMonitoredServerFunctionSql"/>.
    /// </summary>
    internal static string BuildEditMonitoredServerSeventeenWrapperSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.edit_monitored_server(
   p_server_id integer,
   p_expected_modified_at timestamp,
   p_columns text[],
   p_name text,
   p_host text,
   p_port integer,
   p_database text,
   p_read_only_intent boolean,
   p_auth text,
   p_username text,
   p_secret text,
   p_encrypt_mode text,
   p_trust_server_certificate boolean,
   p_multi_subnet_failover boolean,
   p_monthly_cost_usd numeric,
   p_aws_role_arn text,
   p_aws_external_id text)
RETURNS TABLE (outcome text, new_modified_at timestamp)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
   SELECT e.outcome, e.new_modified_at
   FROM {config}.edit_monitored_server(
      p_server_id, p_expected_modified_at,
      array_remove(COALESCE(p_columns, ARRAY[]::text[]), 'query_store_replica'),
      p_name, p_host, p_port, p_database, p_read_only_intent, p_auth, p_username, p_secret, p_encrypt_mode,
      p_trust_server_certificate, p_multi_subnet_failover, p_monthly_cost_usd, p_aws_role_arn, p_aws_external_id, NULL::text) AS e;
$fn$;
REVOKE ALL ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerSeventeenSignature}) FROM PUBLIC;";

    /// <summary>
    /// The 15-argument <c>config.edit_monitored_server</c> (#5452), kept for good as a wrapper over the 17-argument
    /// wrapper (which reaches the 18-argument function), for a caller built before the AWS role columns existed (the web route, the MCP tool and the service's
    /// own call all name the shorter list until they are moved). It passes NULL for the role and the external ID, and
    /// removes both AWS column names from <c>p_columns</c> first, so a caller of this signature cannot change or clear a
    /// role, however it builds its list. <c>SECURITY DEFINER</c> with a pinned <c>search_path</c>, revoked from PUBLIC;
    /// the caller adds the <c>GRANT EXECUTE</c>. Must run after <see cref="BuildEditMonitoredServerFunctionSql"/>.
    /// </summary>
    internal static string BuildEditMonitoredServerLegacyWrapperSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.edit_monitored_server(
   p_server_id integer,
   p_expected_modified_at timestamp,
   p_columns text[],
   p_name text,
   p_host text,
   p_port integer,
   p_database text,
   p_read_only_intent boolean,
   p_auth text,
   p_username text,
   p_secret text,
   p_encrypt_mode text,
   p_trust_server_certificate boolean,
   p_multi_subnet_failover boolean,
   p_monthly_cost_usd numeric)
RETURNS TABLE (outcome text, new_modified_at timestamp)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
   SELECT e.outcome, e.new_modified_at
   FROM {config}.edit_monitored_server(
      p_server_id, p_expected_modified_at,
      array_remove(array_remove(COALESCE(p_columns, ARRAY[]::text[]), 'aws_role_arn'), 'aws_external_id'),
      p_name, p_host, p_port, p_database, p_read_only_intent, p_auth, p_username, p_secret, p_encrypt_mode,
      p_trust_server_certificate, p_multi_subnet_failover, p_monthly_cost_usd, NULL::text, NULL::text) AS e;
$fn$;
REVOKE ALL ON FUNCTION {config}.edit_monitored_server({EditMonitoredServerLegacySignature}) FROM PUBLIC;";

    /// <summary>
    /// #6003 review: the journal rows only the store owner writes. <c>config.investigation_journal</c> is the request event
    /// log: the service signs each event row with the agent route's key and posts it, and the MCP long-poll orders its cursor
    /// by <c>event_time</c>. <c>mcp</c> (the claim and submit tools) and <c>admin</c> (the desktop Viewer) hold INSERT on it
    /// (the columns of <see cref="JournalWriterColumns"/>) to journal their own actions, so this <c>BEFORE INSERT</c> trigger
    /// refuses a row from any role but the owner that
    /// (a) is an event (<see cref="InvestigationEvents.Kinds"/>), a delivery or fallback mark, a server action switch (#6269) or one of
    /// the three rows of an agent action (<see cref="AgentActionEvents.OwnerOnly"/>, #6569: the service journals them itself, on the
    /// owner connection, and the Agent changes list offers Undo on the <c>action_called</c> row), but for the one
    /// <c>opened</c> row of a practice request (the Viewer opens those, #5918), whose reason is exactly
    /// <c>practice of &lt;the case's request id&gt;</c>; or
    /// (b) carries a once key other than the prefixes the tools and the Viewer write (<see cref="JournalWriterOnceKeyPrefixes"/>),
    /// so no role can pre-empt a key the service uses (<c>delivered:</c>, <c>fallback:</c>, <c>resolved:</c>, <c>expired:</c>,
    /// <c>evidence_ready:</c>, <c>lease_expired:</c>, <c>limit_reached:</c>). A practice <c>opened</c> row gets the once key
    /// <c>practice_opened:&lt;request id&gt;</c> from the trigger, so a second one for the request fails on the unique index even
    /// when two inserts race. A row dated ahead of the store's clock is clamped to it (a role's own clock may run fast, and a
    /// future date would move every long-poll cursor past the real events after it). The owner (the table's owner or a
    /// superuser) is not held to any of this. The same text is in <c>tools/provision-roles.sql</c> for a self-managed store.
    /// </summary>
    internal static string BuildJournalEventRulesSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.investigation_journal_owner_rows()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $rules$
DECLARE
   v_practice boolean := false;
BEGIN
   IF pg_has_role(current_user, (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = TG_RELID), 'USAGE') THEN
      RETURN NEW;
   END IF;

   IF NEW.event IN ({JournalOwnerEventList}) THEN
      IF NEW.event = 'opened'
         AND EXISTS (SELECT 1 FROM {config}.investigation_requests AS r
                     WHERE r.request_id = NEW.request_id AND r.practice_of IS NOT NULL
                       AND NEW.reason = 'practice of ' || r.practice_of::text)
         AND NOT EXISTS (SELECT 1 FROM {config}.investigation_journal AS j
                         WHERE j.request_id = NEW.request_id AND j.event = 'opened') THEN
         v_practice := true;
      ELSE
         RAISE EXCEPTION 'Only the Darling service journals the request event %.', NEW.event USING ERRCODE = '42501';
      END IF;
   END IF;

   IF NEW.once_key IS NOT NULL AND split_part(NEW.once_key, ':', 1) NOT IN ({JournalWriterOnceKeyList}) THEN
      RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'Only the Darling service writes the once keys of request events and their delivery marks.';
   END IF;

   IF v_practice THEN
      NEW.once_key := 'practice_opened:' || NEW.request_id::text;
   END IF;

   IF NEW.event_time > now() THEN
      NEW.event_time := now();
   END IF;

   RETURN NEW;
END;
$rules$;
REVOKE ALL ON FUNCTION {config}.investigation_journal_owner_rows() FROM PUBLIC;
CREATE OR REPLACE TRIGGER trg_investigation_journal_owner_rows
   BEFORE INSERT ON {config}.investigation_journal
   FOR EACH ROW EXECUTE FUNCTION {config}.investigation_journal_owner_rows();";

    /// <summary>
    /// The columns of <c>config.investigation_journal</c> that <c>mcp</c> and <c>admin</c> may set on an insert: every column the
    /// claim and submit tools and the Viewer's journal writers name (the change tools' rows are the owner's, #6569). Not <c>journal_id</c> (the identity: a role that
    /// set it could fill the next million values and make every service insert fail) and not <c>event_id</c> (the unique
    /// webhook id): both take their defaults.
    /// </summary>
    internal const string JournalWriterColumns = "event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key";

    /// <summary>
    /// The once-key prefixes (the text before the first colon) a non-owner journal writer uses: the claim row, the refusals
    /// (<c>refused</c> for claim, submit and the note tools, <c>agent_refused</c> for a Level 2 change), the journal cap row,
    /// and a report's link rows. Every other prefix is refused (<see cref="BuildJournalEventRulesSql"/>); the trigger itself
    /// sets <c>practice_opened</c> for a practice request's one opened row.
    /// </summary>
    internal static readonly string[] JournalWriterOnceKeyPrefixes = ["claimed", "refused", "agent_refused", "journal_capped", "link"];

    private static string JournalWriterOnceKeyList => string.Join(", ", JournalWriterOnceKeyPrefixes.Select(k => "'" + k + "'"));

    /// <summary>
    /// The alert links of a request (<c>config.investigation_request_alerts</c>) say which alerts a request waits on to resolve.
    /// Only the service writes them, but for the Viewer's copy for a practice request (#5918): <c>admin</c> holds INSERT for
    /// that, so a link to a live request (which would keep it from ever resolving) is refused here, for any role but the owner.
    /// The same text is in <c>tools/provision-roles.sql</c> for a self-managed store.
    /// </summary>
    internal static string BuildRequestAlertLinkRulesSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.investigation_request_alerts_owner_rows()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $rules$
BEGIN
   IF pg_has_role(current_user, (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = TG_RELID), 'USAGE') THEN
      RETURN NEW;
   END IF;

   IF NOT EXISTS (SELECT 1 FROM {config}.investigation_requests AS r
                  WHERE r.request_id = NEW.request_id AND r.practice_of IS NOT NULL) THEN
      RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'Only the Darling service links an alert to a request, and only a practice request takes a link from anyone else.';
   END IF;

   RETURN NEW;
END;
$rules$;
REVOKE ALL ON FUNCTION {config}.investigation_request_alerts_owner_rows() FROM PUBLIC;
CREATE OR REPLACE TRIGGER trg_investigation_request_alerts_owner_rows
   BEFORE INSERT ON {config}.investigation_request_alerts
   FOR EACH ROW EXECUTE FUNCTION {config}.investigation_request_alerts_owner_rows();";

    /// <summary>The journal events only the owner writes (<see cref="BuildJournalEventRulesSql"/>), as a SQL list. <c>server_action_switched</c>
    /// is the owner's alone (#6269): only the switch writer, on the owner connection, journals it, so no other login can write who changed a switch.
    /// So are the three rows of an agent action (<see cref="AgentActionEvents.OwnerOnly"/>, #6569): the Agent changes list offers Undo on an
    /// <c>action_called</c> row, so only the service, which runs the action and checks the request, journals it and its result.</summary>
    internal static string JournalOwnerEventList => string.Join(", ",
        InvestigationEvents.Kinds.Append(InvestigationEvents.Delivered).Append(InvestigationEvents.FallbackSent).Append(ServerActionSwitches.JournalEvent)
            .Concat(AgentActionEvents.OwnerOnly)
            .Select(k => "'" + k + "'"));

    /// <summary>
    /// The store's own password rules for every role but the owner: a <c>BEFORE INSERT OR UPDATE</c> trigger on
    /// <c>config_monitored_servers</c> and its function. A password that starts with <c>env:</c> or <c>file:</c> (a
    /// reference, read as <see cref="DarlingSecretSource.IsReference"/> reads it) is refused on insert and on change, and an
    /// update that changes how the row connects while it keeps a stored password is refused, and one that changes it on a
    /// row holding a remediation login is refused with its own state (<c>PW003</c>). The owner (the table's
    /// owner or a superuser: the service, <c>--add-server</c>, the configuration-file seed and the body of
    /// <c>edit_monitored_server</c>) is not held to either rule, and no existing row is touched. The same text is
    /// in <c>tools/provision-roles.sql</c> for a self-managed store.
    /// </summary>
    internal static string BuildServerPasswordRulesSql(string config) => $@"
CREATE OR REPLACE FUNCTION {config}.monitored_server_password_rules()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $rules$
DECLARE
   v_reference_main boolean;
   v_reference_remediation boolean;
   v_reference_hypothetical boolean;
   v_held boolean;
   v_moved boolean;
BEGIN
   -- The store owner (the role that owns the table, and a superuser) writes what it writes: the service, the
   -- --add-server verb, the configuration-file seed, and the body of edit_monitored_server, which runs as the
   -- function's owner. Every other role is held to the two rules below.
   IF pg_has_role(current_user, (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = TG_RELID), 'USAGE') THEN
      RETURN NEW;
   END IF;

   v_reference_main := COALESCE(left(NEW.encrypted_password, 4) = 'env:' OR left(NEW.encrypted_password, 5) = 'file:', false);
   v_reference_remediation := COALESCE(left(NEW.remediation_encrypted_password, 4) = 'env:' OR left(NEW.remediation_encrypted_password, 5) = 'file:', false);
   -- #6170: the hypothetical-index login's password column is held to the same rule as the remediation one.
   -- It is read through to_jsonb, not by name, so this function also runs on a store below V203 that does not have the columns
   -- yet (a script run before the service migrated it): the value is then NULL and these checks do nothing.
   v_reference_hypothetical := COALESCE(left((to_jsonb(NEW) ->> 'hypothetical_index_encrypted_password'), 4) = 'env:' OR left((to_jsonb(NEW) ->> 'hypothetical_index_encrypted_password'), 5) = 'file:', false);

   IF TG_OP = 'INSERT' THEN
      -- A new row never holds a reference. The one pass: an upsert's proposed row for a server whose stored value is
      -- already that exact reference (the row is locked, so it cannot go away before the write), which then meets the
      -- UPDATE rules below. A role that cannot read or lock the row (viewer, mcp) gets the refusal.
      IF v_reference_main OR v_reference_remediation OR v_reference_hypothetical THEN
         v_held := false;
         BEGIN
            PERFORM 1
            FROM {config}.config_monitored_servers AS s
            WHERE s.server_id = NEW.server_id
              AND (NOT v_reference_main OR s.encrypted_password = NEW.encrypted_password)
              AND (NOT v_reference_remediation OR s.remediation_encrypted_password = NEW.remediation_encrypted_password)
              AND (NOT v_reference_hypothetical OR (to_jsonb(s) ->> 'hypothetical_index_encrypted_password') = (to_jsonb(NEW) ->> 'hypothetical_index_encrypted_password'))
            FOR UPDATE OF s;
            v_held := FOUND;
         EXCEPTION WHEN insufficient_privilege THEN
            v_held := false;
         END;

         IF NOT v_held THEN
            RAISE EXCEPTION '%', 'Enter the password itself. References (env: or file:) can only be set in the configuration file.' USING ERRCODE = 'PW001';
         END IF;
      END IF;

      RETURN NEW;
   END IF;

   -- An update never sets a reference.
   IF (v_reference_main AND NEW.encrypted_password IS DISTINCT FROM OLD.encrypted_password)
      OR (v_reference_remediation AND NEW.remediation_encrypted_password IS DISTINCT FROM OLD.remediation_encrypted_password)
      OR (v_reference_hypothetical AND (to_jsonb(NEW) ->> 'hypothetical_index_encrypted_password') IS DISTINCT FROM (to_jsonb(OLD) ->> 'hypothetical_index_encrypted_password')) THEN
      RAISE EXCEPTION '%', 'Enter the password itself. References (env: or file:) can only be set in the configuration file.' USING ERRCODE = 'PW001';
   END IF;

   -- An update that changes how the row connects (the set edit_monitored_server counts, plus the engine) never keeps
   -- a stored secret that is still there: the main password is typed again with the change, and a remediation
   -- password is changed on the service host.
   v_moved := NEW.host IS DISTINCT FROM OLD.host
      OR NEW.port IS DISTINCT FROM OLD.port
      OR NEW.engine IS DISTINCT FROM OLD.engine
      OR NEW.database IS DISTINCT FROM OLD.database
      OR NEW.read_only_intent IS DISTINCT FROM OLD.read_only_intent
      OR lower(NEW.auth) IS DISTINCT FROM lower(OLD.auth)
      OR NEW.username IS DISTINCT FROM OLD.username
      OR lower(NEW.encrypt_mode) IS DISTINCT FROM lower(OLD.encrypt_mode)
      OR NEW.trust_server_certificate IS DISTINCT FROM OLD.trust_server_certificate
      OR NEW.multi_subnet_failover IS DISTINCT FROM OLD.multi_subnet_failover
      -- The Query Store replica (#6254) is a host the stored password would be sent to. Read through to_jsonb, blank as none, so this
      -- function also runs on a store below V213 that does not have the column yet.
      OR COALESCE(NULLIF(btrim(to_jsonb(NEW) ->> 'query_store_replica'), ''), '') IS DISTINCT FROM COALESCE(NULLIF(btrim(to_jsonb(OLD) ->> 'query_store_replica'), ''), '');

   -- A remediation secret that is kept goes with the login name it was stored for: that name is as much a part of how
   -- the row is reached as the host is, so it is not changed here either. Not part of v_moved, so it never raises PW002.
   -- The hypothetical-index login's secret (#6170) goes with its login name in the same way.
   IF (COALESCE(OLD.remediation_encrypted_password, '') <> ''
       AND NEW.remediation_encrypted_password IS NOT DISTINCT FROM OLD.remediation_encrypted_password
       AND (v_moved OR NEW.remediation_username IS DISTINCT FROM OLD.remediation_username))
      OR (COALESCE((to_jsonb(OLD) ->> 'hypothetical_index_encrypted_password'), '') <> ''
       AND (to_jsonb(NEW) ->> 'hypothetical_index_encrypted_password') IS NOT DISTINCT FROM (to_jsonb(OLD) ->> 'hypothetical_index_encrypted_password')
       AND (v_moved OR (to_jsonb(NEW) ->> 'hypothetical_index_username') IS DISTINCT FROM (to_jsonb(OLD) ->> 'hypothetical_index_username'))) THEN
      RAISE EXCEPTION '%', 'This server has a remediation or hypothetical-index login stored. Change how it is reached on the service host, in the configuration file or with --add-server.' USING ERRCODE = 'PW003';
   END IF;

   IF v_moved
      AND COALESCE(OLD.encrypted_password, '') <> ''
      AND NEW.encrypted_password IS NOT DISTINCT FROM OLD.encrypted_password THEN
      RAISE EXCEPTION '%', 'Changing the host, port or any other connection setting of this server needs its password sent with the change, because a saved password is only ever sent to the server it was saved for. Send the password together with the new settings.' USING ERRCODE = 'PW002';
   END IF;

   -- A new AWS role (#5452) never inherits an external ID stored for the old one, unseen: the update names the ID
   -- again, or clears it. The owner is not held to this; edit_monitored_server answers it first.
   IF NEW.aws_role_arn IS NOT NULL
      AND NEW.aws_role_arn IS DISTINCT FROM OLD.aws_role_arn
      AND OLD.aws_external_id IS NOT NULL
      AND NEW.aws_external_id IS NOT DISTINCT FROM OLD.aws_external_id THEN
      RAISE EXCEPTION '%', 'Changing the AWS role needs the external ID with it: send the external ID again, or clear it.' USING ERRCODE = 'PW004';
   END IF;

   RETURN NEW;
END;
$rules$;
REVOKE ALL ON FUNCTION {config}.monitored_server_password_rules() FROM PUBLIC;
CREATE OR REPLACE TRIGGER trg_monitored_server_password_rules
   BEFORE INSERT OR UPDATE ON {config}.config_monitored_servers
   FOR EACH ROW EXECUTE FUNCTION {config}.monitored_server_password_rules();";

    /// <summary>
    /// A self-managed store (not the managed one, and not the compose store the service provisions) has its password rules
    /// from <c>tools/provision-roles.sql</c> alone, so a store upgraded without re-running the script has none. This runs
    /// <see cref="BuildServerPasswordRulesSql"/> on the service's own connection, which owns the tables, on every start. It
    /// never throws and never fails the start: when the connection may not create the function or the trigger, one warning
    /// names <c>provision-roles.sql</c>. Rules that are already in place are left alone: when the trigger exists, is
    /// enabled and its function has the built text and search path (a function another role created from the script
    /// included, which this connection could not replace), nothing is run and nothing is warned, and no lock is taken on
    /// the table. The check and the command run under the provisioning advisory lock (#5560,
    /// <see cref="ProvisioningLockKey"/>), so two services starting against one store do not both replace the function
    /// at once. The two <c>edit_monitored_server</c> overloads are part of the rules (#6170): a store that holds them from an
    /// older script keeps a version that does not know the hypothetical-index pair, so they are replaced the same way. The journal's owner-row
    /// function (<see cref="BuildJournalEventRulesSql"/>, #6569) is checked and replaced in the same lock and the same transaction, so a store
    /// upgraded by replacing the binaries refuses the owner-only journal events from the mcp and admin logins at the next start. Returns whether the rules are in place.
    /// </summary>
    public static async Task<bool> EnsureServerPasswordRulesAsync(
        NpgsqlDataSource dataSource, ILogger logger, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);

        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);

            /* #5560: the check and the CREATE OR REPLACE run under the provisioning lock, like the provisioning batch.
               Two services on one self-managed store would otherwise both find the function stale and both replace
               the same pg_proc row at once, and the second would fail "tuple concurrently updated". The check runs
               inside the lock, so the one that waited finds the function the other wrote and runs nothing. */
            var alreadyInPlace = false;
            await ExecuteSerializedAsync(connection, async (transaction, token) =>
            {
                alreadyInPlace = await ServerPasswordRulesAreInPlaceAsync(connection, transaction, token);
                /* #6170: the edit function is part of the same rules (it answers the host change before the trigger is reached),
                   and a store whose script was not re-run keeps the version that knows only the remediation column. */
                alreadyInPlace &= await EditMonitoredServerFunctionsAreCurrentAsync(connection, transaction, token);
                /* #6569: the journal's owner-only event list is part of the same start check. A store upgraded by replacing the binaries
                   keeps the older function, which still accepts action_called, action_done and action_failed from mcp and admin. */
                var journal = await JournalEventRulesAreInPlaceAsync(connection, transaction, token);
                alreadyInPlace &= journal is not false;
                return alreadyInPlace
                    ? null
                    : new NpgsqlCommand(BuildServerPasswordRulesSql("config") + BuildEditMonitoredServerFunctionsSql("config") + (journal is null ? "" : BuildJournalEventRulesSql("config")), connection, transaction)
                    {
                        CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds,
                    };
            }, logger, cancellationToken);

            logger.LogInformation(
                alreadyInPlace
                    ? "The store's password and journal rules are already in place"
                    : "The store's password and journal rules are in place");
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(
                "Could not create the store's password or journal rules ({Message}). Run provision-roles.sql against the store as the role that owns its tables.",
                ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Whether the trigger is on <c>config_monitored_servers</c>, enabled, a row-level BEFORE INSERT OR UPDATE one, and its
    /// function has the text <see cref="BuildServerPasswordRulesSql"/> builds (full-line comments and spacing set aside) and
    /// the pinned search path. A read that fails answers false, so the caller falls back to creating them.
    /// </summary>
    internal static async Task<bool> ServerPasswordRulesAreInPlaceAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        try
        {
            await TakeReadSavepointAsync(transaction, cancellationToken);
            await using var read = new NpgsqlCommand(@"
SELECT p.prosrc, p.proconfig
FROM pg_catalog.pg_trigger AS t
JOIN pg_catalog.pg_proc AS p ON p.oid = t.tgfoid
WHERE t.tgrelid = 'config.config_monitored_servers'::regclass
  AND t.tgname = 'trg_monitored_server_password_rules'
  AND t.tgenabled = 'O'
  AND t.tgtype = 23
  AND p.proname = 'monitored_server_password_rules'
  AND p.pronamespace = 'config'::regnamespace", connection, transaction)
            {
                CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds,
            };
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }

            var stored = reader.GetString(0);
            var settings = reader.IsDBNull(1) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(1);
            if (settings.Length != 1 || !string.Equals(settings[0], "search_path=pg_catalog, pg_temp", StringComparison.Ordinal))
            {
                return false;
            }

            var built = BuildServerPasswordRulesSql("config");
            const string quote = "$rules$";
            var open = built.IndexOf(quote, StringComparison.Ordinal);
            var close = built.IndexOf(quote, open + quote.Length, StringComparison.Ordinal);
            if (open < 0 || close < 0)
            {
                return false;
            }

            return string.Equals(
                NormalizeRulesText(stored),
                NormalizeRulesText(built.Substring(open + quote.Length, close - open - quote.Length)),
                StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await RecoverFromReadAsync(transaction);
            return false;
        }
    }

    /// <summary>
    /// Whether the journal's owner-row trigger (<see cref="BuildJournalEventRulesSql"/>) is on <c>config.investigation_journal</c>, enabled,
    /// a row-level BEFORE INSERT one, and its function has the text this build writes (full-line comments and spacing set aside) and the pinned
    /// search path (#6569). A store below V191, which has no journal or no <c>practice_of</c> column yet, answers null: the function reads that
    /// column, so there is nothing to replace until the service has migrated the store (the script skips it the same way). A read that fails
    /// answers false, so the caller falls back to replacing the function.
    /// </summary>
    internal static async Task<bool?> JournalEventRulesAreInPlaceAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        try
        {
            await TakeReadSavepointAsync(transaction, cancellationToken);
            await using (var applicable = new NpgsqlCommand(@"
SELECT pg_catalog.to_regclass('config.investigation_journal') IS NOT NULL
   AND pg_catalog.to_regclass('config.investigation_request_alerts') IS NOT NULL
   AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
               WHERE attrelid = pg_catalog.to_regclass('config.investigation_requests')
                 AND attname = 'practice_of' AND NOT attisdropped)", connection, transaction)
            {
                CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds,
            })
            {
                if (!(bool)(await applicable.ExecuteScalarAsync(cancellationToken) ?? false))
                {
                    return null;
                }
            }

            await using var read = new NpgsqlCommand(@"
SELECT p.prosrc, p.proconfig
FROM pg_catalog.pg_trigger AS t
JOIN pg_catalog.pg_proc AS p ON p.oid = t.tgfoid
WHERE t.tgrelid = 'config.investigation_journal'::regclass
  AND t.tgname = 'trg_investigation_journal_owner_rows'
  AND t.tgenabled = 'O'
  AND t.tgtype = 7
  AND p.proname = 'investigation_journal_owner_rows'
  AND p.pronamespace = 'config'::regnamespace", connection, transaction)
            {
                CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds,
            };
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }

            var stored = reader.GetString(0);
            var settings = reader.IsDBNull(1) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(1);
            if (settings.Length != 1 || !string.Equals(settings[0], "search_path=pg_catalog, pg_temp", StringComparison.Ordinal))
            {
                return false;
            }

            var built = BuildJournalEventRulesSql("config");
            const string quote = "$rules$";
            var open = built.IndexOf(quote, StringComparison.Ordinal);
            var close = built.IndexOf(quote, open + quote.Length, StringComparison.Ordinal);
            if (open < 0 || close < 0)
            {
                return false;
            }

            return string.Equals(
                NormalizeRulesText(stored),
                NormalizeRulesText(built.Substring(open + quote.Length, close - open - quote.Length)),
                StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await RecoverFromReadAsync(transaction);
            return false;
        }
    }

    /// <summary>
    /// All three <c>config.edit_monitored_server</c> overloads (#5240, #5452, #6254), the 18-argument function first and then the wrappers
    /// over it: what a self-managed store replaces on every start (<see cref="EnsureServerPasswordRulesAsync"/>), because
    /// <c>CREATE OR REPLACE</c> keeps the grants the script made. The same text is in <c>tools/provision-roles.sql</c>.
    /// </summary>
    internal static string BuildEditMonitoredServerFunctionsSql(string config) =>
        BuildEditMonitoredServerFunctionSql(config) + BuildEditMonitoredServerSeventeenWrapperSql(config) + BuildEditMonitoredServerLegacyWrapperSql(config);

    /// <summary>
    /// Whether the <c>config.edit_monitored_server</c> overloads of a store are the ones this build writes (#6170): body text
    /// (full-line comments and spacing set aside) and the pinned search path, for both. A store that holds neither overload
    /// has no edit function to replace (the script was never run there) and answers true. A store that holds one, or an
    /// older body, answers false. A read that fails answers false, so the caller falls back to replacing them.
    /// </summary>
    internal static async Task<bool> EditMonitoredServerFunctionsAreCurrentAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        try
        {
            await TakeReadSavepointAsync(transaction, cancellationToken);
            var present = 0;
            foreach (var (signature, builder) in new (string, Func<string, string>)[]
            {
                (EditMonitoredServerSignature, BuildEditMonitoredServerFunctionSql),
                (EditMonitoredServerSeventeenSignature, BuildEditMonitoredServerSeventeenWrapperSql),
                (EditMonitoredServerLegacySignature, BuildEditMonitoredServerLegacyWrapperSql),
            })
            {
                await using var read = new NpgsqlCommand(
                    "SELECT p.prosrc, p.proconfig FROM pg_catalog.pg_proc AS p WHERE p.oid = pg_catalog.to_regprocedure(@signature)", connection, transaction)
                {
                    CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds,
                };
                read.Parameters.AddWithValue("signature", "config.edit_monitored_server(" + signature + ")");
                await using var reader = await read.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                {
                    continue;
                }

                present++;
                var stored = reader.GetString(0);
                var settings = reader.IsDBNull(1) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(1);
                if (settings.Length != 1 || !string.Equals(settings[0], "search_path=pg_catalog, pg_temp", StringComparison.Ordinal))
                {
                    return false;
                }

                var built = builder("config");
                const string quote = "$fn$";
                var open = built.IndexOf(quote, StringComparison.Ordinal);
                var close = built.IndexOf(quote, open + quote.Length, StringComparison.Ordinal);
                if (open < 0 || close < 0
                    || !string.Equals(
                        NormalizeRulesText(stored),
                        NormalizeRulesText(built.Substring(open + quote.Length, close - open - quote.Length)),
                        StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return present is 0 or 3;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await RecoverFromReadAsync(transaction);
            return false;
        }
    }

    /// <summary>The function text with full-line comments and runs of white space set aside, the comparison the script's
    /// copy of the rules is held to as well.</summary>
    private static string NormalizeRulesText(string sql) =>
        Regex.Replace(Regex.Replace(sql, @"(?m)^\s*--.*$", ""), @"\s+", " ").Trim();

    /// <summary>
    /// Fails closed unless <paramref name="secret"/> is a SCRAM-SHA-256 verifier (#3910). This refuses a plain
    /// password on purpose: the batch is recorded wherever statement text is (an ERROR's STATEMENT line in the
    /// store's own log, which the store-log sweep keeps and the viewer role reads), so it must only ever carry
    /// what PostgreSQL stores, not what a client logs in with. The verifier's shape admits no quote, which is
    /// what makes the <c>PASSWORD '…'</c> interpolation escaping-safe.
    /// </summary>
    private static void RequireVerifier(string secret, string parameterName)
    {
        if (!ScramSha256Verifier.IsVerifier(secret))
        {
            throw new ArgumentException(
                "Role secrets must be SCRAM-SHA-256 verifiers (ScramSha256Verifier.Create), never the password: the provisioning batch can be recorded by the store's own log.",
                parameterName);
        }
    }

    /// <summary>
    /// Each managed role's stored secret (<c>pg_authid.rolpassword</c>), keyed by role; a role that does not
    /// exist yet is absent. Needs a superuser, which the managed owner is. A read this role is refused
    /// returns nothing, so every role is re-asserted: the safe direction, and the pre-#3910 behaviour.
    /// </summary>
    internal static async Task<Dictionary<string, string?>> ReadStoredRoleSecretsAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, ILogger logger, CancellationToken cancellationToken)
    {
        var stored = new Dictionary<string, string?>(StringComparer.Ordinal);
        try
        {
            await TakeReadSavepointAsync(transaction, cancellationToken);
            await using var command = new NpgsqlCommand(
                "SELECT rolname::text, rolpassword FROM pg_catalog.pg_authid WHERE rolname = ANY($1)", connection, transaction) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
            command.Parameters.AddWithValue(new[] { DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.WebRoleName, DarlingManagedPostgres.McpRoleName });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                stored[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            await RecoverFromReadAsync(transaction);
            logger.LogDebug("Could not read the managed roles' stored verifiers ({Message}); re-asserting every role's password.", ex.Message);
            stored.Clear();
        }

        return stored;
    }

    /// <summary>
    /// Which roles need their password re-asserted (#3910): those whose stored secret is missing, is not a
    /// SCRAM-SHA-256 verifier (an MD5 hash from an older store), or does not accept the credential file's
    /// password (the file was regenerated). Pure, so the decision is pinned without a server.
    /// </summary>
    internal static PasswordReassert PlanPasswordReassert(
        IReadOnlyDictionary<string, string?> stored, string adminPassword, string viewerPassword, string mcpPassword, string webPassword)
    {
        ArgumentNullException.ThrowIfNull(stored);

        var reassert = PasswordReassert.None;
        if (!ScramSha256Verifier.Verifies(stored.GetValueOrDefault(DarlingManagedPostgres.AdminRoleName), adminPassword))
        {
            reassert |= PasswordReassert.Admin;
        }

        if (!ScramSha256Verifier.Verifies(stored.GetValueOrDefault(DarlingManagedPostgres.ViewerRoleName), viewerPassword))
        {
            reassert |= PasswordReassert.Viewer;
        }

        if (!ScramSha256Verifier.Verifies(stored.GetValueOrDefault(DarlingManagedPostgres.McpRoleName), mcpPassword))
        {
            reassert |= PasswordReassert.Mcp;
        }

        if (!ScramSha256Verifier.Verifies(stored.GetValueOrDefault(DarlingManagedPostgres.WebRoleName), webPassword))
        {
            reassert |= PasswordReassert.Web;
        }

        return reassert;
    }

    private static string DescribeReassert(PasswordReassert reassert)
    {
        var roles = new List<string>(3);
        if ((reassert & PasswordReassert.Admin) != 0)
        {
            roles.Add(DarlingManagedPostgres.AdminRoleName);
        }

        if ((reassert & PasswordReassert.Viewer) != 0)
        {
            roles.Add(DarlingManagedPostgres.ViewerRoleName);
        }

        if ((reassert & PasswordReassert.Mcp) != 0)
        {
            roles.Add(DarlingManagedPostgres.McpRoleName);
        }

        if ((reassert & PasswordReassert.Web) != 0)
        {
            roles.Add(DarlingManagedPostgres.WebRoleName);
        }

        return string.Join(", ", roles);
    }

    /// <summary>
    /// Where the compose distribution's store keeps the role passwords the service generates (#3914). A fixed
    /// path, not a setting: the Dockerfile creates it owner-only and the compose file mounts the
    /// <c>darling-credentials</c> volume on it, so a recreated container keeps its passwords. Without the volume
    /// the files regenerate and are re-asserted, which costs one password re-assert per recreation and nothing
    /// else. The Windows branch exists for the live tests, which run on the bundled Windows runtime.
    /// </summary>
    public static string ComposeStoreCredentialDirectory => OperatingSystem.IsWindows()
        ? Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
            "PerformanceMonitorDarling", "compose-credentials")
        : "/var/lib/darling/credentials";

    /// <summary>The compose store's credential file for <paramref name="role"/>: the managed file's name without
    /// the <c>.dpapi</c> extension, because it holds the password itself (#3914).</summary>
    public static string ComposeStoreCredentialFileName(string role) => $"pg-{role}-credential";

    /// <summary>
    /// What the store says about the login the service connects with, about the three role names, and about the
    /// cluster's other databases, read in one statement before anything is written (#3914). oid 10 is
    /// <c>BOOTSTRAP_SUPERUSERID</c>, the role <c>initdb</c> creates, fixed in every PostgreSQL version. The other
    /// databases are every one but the store's own, <c>postgres</c> and the templates (<c>datistemplate</c>, and
    /// <c>template0</c>/<c>template1</c> by name in case either lost the flag): the compose store's cluster holds
    /// nothing else, and roles are cluster-wide.
    /// </summary>
    internal const string ComposeStoreFactsSql = @"
SELECT
    u.rolname::text,
    pg_catalog.current_database()::text,
    (u.oid = 10 AND u.rolsuper),
    c.rolname::text,
    pg_catalog.shobj_description(c.oid, 'pg_authid'),
    (
        SELECT
            pg_catalog.array_agg(d.datname::text ORDER BY d.datname)
        FROM pg_catalog.pg_database AS d
        WHERE d.datname NOT IN (pg_catalog.current_database(), 'postgres', 'template0', 'template1')
        AND   NOT d.datistemplate
    )
FROM pg_catalog.pg_roles AS u
LEFT JOIN pg_catalog.pg_roles AS c
    ON c.rolname = ANY ($1)
WHERE u.rolname = current_user";

    private static async Task<ComposeStoreFacts> ReadComposeStoreFactsAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ComposeStoreFactsSql, connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        command.Parameters.AddWithValue(new[] { DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.WebRoleName, DarlingManagedPostgres.McpRoleName });

        string? login = null;
        string? database = null;
        var bootstrapSuperuser = false;
        var markers = new Dictionary<string, string?>(StringComparer.Ordinal);
        IReadOnlyList<string> otherDatabases = Array.Empty<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            login = reader.GetString(0);
            database = reader.GetString(1);
            bootstrapSuperuser = reader.GetBoolean(2);
            if (!reader.IsDBNull(3))
            {
                markers[reader.GetString(3)] = reader.IsDBNull(4) ? null : reader.GetString(4);
            }

            /* array_agg over no rows is NULL, not an empty array. */
            otherDatabases = reader.IsDBNull(5) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(5);
        }

        if (login is null || database is null)
        {
            throw new InvalidOperationException("The store did not report the login this service connects as.");
        }

        return new ComposeStoreFacts(login, database, bootstrapSuperuser, markers, otherDatabases);
    }

    /// <summary>
    /// Why the service must NOT provision roles on this store, or null when it may (#3914). Pure, so every refusal
    /// is pinned without a server. The reason reaches the web and MCP hosts' startup warning, so it is written for
    /// the operator reading it there.
    /// </summary>
    internal static string? RefuseComposeStore(ComposeStoreFacts facts)
    {
        ArgumentNullException.ThrowIfNull(facts);

        if (!facts.BootstrapSuperuser)
        {
            return $"The service logs in to this store as '{facts.Login}', which is not the store's bootstrap superuser (the role initdb created, POSTGRES_USER in the compose file), so the store is not treated as the service's own and no roles were created on it.";
        }

        /* The bootstrap superuser alone does not make a cluster the compose store: a container that logs in to an
           operator's own cluster as its bootstrap superuser (postgres, say) passes that test too. The compose store's
           image creates one database and the service owns the cluster, so any other database means someone else's
           cluster, and roles are cluster-wide: provisioning there would put admin/viewer/mcp on everything it
           serves. */
        if (facts.OtherDatabases.Count > 0)
        {
            return $"This cluster also holds {DescribeOtherDatabases(facts.OtherDatabases)} that the service does not own, so it is not treated as the service's own and no roles were created on it.";
        }

        /* The two names reach the batch as quoted identifiers; a control character is legal inside one but has
           no business in a store the service provisions, and refusing it keeps every name the batch carries
           printable. */
        if (facts.Login.Any(char.IsControl) || facts.Database.Any(char.IsControl))
        {
            return "The store's login or database name contains a control character, so the service did not provision roles on it.";
        }

        var foreign = facts.ExistingRoleMarkers
            .Where(role => !string.Equals(role.Value, ComposeStoreRoleMarker, StringComparison.Ordinal))
            .OrderBy(role => role.Key, StringComparer.Ordinal)
            .Select(role => $"'{role.Key}' ({DescribeForeignRoleMarker(role.Value)})")
            .ToList();
        if (foreign.Count > 0)
        {
            var one = foreign.Count == 1;
            return $"This store already has {(one ? "a role" : "roles")} {string.Join(" and ", foreign)} that the service did not create on it, so it leaves {(one ? "it" : "them")} alone rather than re-key {(one ? "its password" : "their passwords")}. Drop or rename {(one ? "it" : "them")} to have the service provision its own.";
        }

        return null;
    }

    /// <summary>True when a refusal from <see cref="RefuseComposeStore"/> names <paramref name="role"/> as a role it found that the service did not create.</summary>
    internal static bool RefusalNamesForeignRole(string? refusal, string role) =>
        refusal is not null && refusal.Contains($"'{role}' (", StringComparison.Ordinal);

    private static string DescribeForeignRoleMarker(string? marker) => marker switch
    {
        RoleMarker => "created by tools/provision-roles.sql",
        null => "not created by Darling",
        _ => $"comment '{marker}'",
    };

    /// <summary>How many of the cluster's other databases a refusal names before it counts the rest.</summary>
    internal const int OtherDatabasesNamed = 3;

    /// <summary>"the database 'a'", "the databases 'a' and 'b'", "the databases 'a', 'b', 'c' and 2 more". A
    /// control character in a name is shown as '?', so a name cannot forge a line in the log it is written to.</summary>
    private static string DescribeOtherDatabases(IReadOnlyList<string> names)
    {
        var shown = names
            .Take(OtherDatabasesNamed)
            .Select(name => "'" + new string(name.Select(c => char.IsControl(c) ? '?' : c).ToArray()) + "'")
            .ToList();
        var more = names.Count - shown.Count;
        var list = more > 0
            ? $"{string.Join(", ", shown)} and {more.ToString(CultureInfo.InvariantCulture)} more"
            : shown.Count == 1
                ? shown[0]
                : $"{string.Join(", ", shown.Take(shown.Count - 1))} and {shown[^1]}";

        return (names.Count == 1 ? "the database " : "the databases ") + list;
    }

    /// <summary>
    /// The compose store's credential for <paramref name="role"/>: the trusted file's password, or a freshly
    /// generated one that <see cref="PersistComposeCredential"/> writes once the batch has committed (#3914).
    /// An untrusted file is discarded rather than read — the Unix equivalent of <see cref="ReadOrGenerateRoleCredential"/>'s
    /// pre-plant check (<see cref="UntrustedComposeCredentialReason"/>): a password someone else could have written
    /// would be re-asserted on the role, and a new password costs only its re-assert. With
    /// <paramref name="readFile"/> false (a directory that cannot be trusted) the file is not looked at.
    /// </summary>
    private static ComposeCredential ReadOrGenerateComposeCredential(string directory, string role, bool readFile, ILogger logger)
    {
        if (readFile)
        {
            var path = Path.Combine(directory, ComposeStoreCredentialFileName(role));
            if (ReadTrustedComposeCredentialFile(path, out var untrusted) is { } password)
            {
                return new ComposeCredential(role, password, generated: false);
            }

            if (untrusted is not null)
            {
                logger.LogWarning(
                    "The compose store's '{Role}' credential {File} is not trusted ({Reason}) — discarding it; a new password is generated and re-asserted on the role.",
                    role, path, untrusted);
                TryDelete(path, logger);
            }
        }

        return new ComposeCredential(role, DarlingManagedPostgres.GeneratePassword(), generated: true);
    }

    /// <summary>
    /// The password a web or MCP host connects with when this start did NOT provision the compose store's roles
    /// (#3914): refused, failed, or a collector that stood down before it got there. The roles still hold what an
    /// earlier start gave them. A credential file is written only after the batch that set its password committed,
    /// for a role carrying <see cref="ComposeStoreRoleMarker"/>, so a trusted file is a login its role accepted when
    /// it was written, and the surface stays on its least-privilege role instead of the owner. Trusted by the
    /// worker's own checks: the directory set owner-only again first (<see cref="PrepareComposeCredentialDirectory"/>),
    /// then the file (<see cref="UntrustedComposeCredentialReason"/>). Never generates or writes anything: replacing a
    /// distrusted file is the next provisioning's job, and only the worker's batch ever sets a role's password from a
    /// file, so the worst a planted file can do here is a login the store refuses. The one removal is the directory
    /// check's own: a directory found open to other users' writes loses every file it holds right there, whichever
    /// caller finds it (#4004 review), because this read sets it owner-only and the next start would trust them.
    /// </summary>
    /// <returns>The password, or why there is none this surface can use.</returns>
    internal static EarlierComposeCredential ReadEarlierComposeCredential(string directory, string role, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);

        var file = ComposeStoreCredentialFileName(role);
        try
        {
            if (!Directory.Exists(directory) && new DirectoryInfo(directory).LinkTarget is null)
            {
                return EarlierComposeCredential.None($"{directory} does not exist");
            }

            var trust = PrepareComposeCredentialDirectory(directory, create: false, logger);
            if (trust.Distrust is { } distrust)
            {
                return EarlierComposeCredential.None($"the credentials directory {directory} is not trusted ({distrust})");
            }

            var path = Path.Combine(directory, file);
            if (ReadTrustedComposeCredentialFile(path, out var untrusted) is { } password)
            {
                return EarlierComposeCredential.Trusted(password);
            }

            return EarlierComposeCredential.None(untrusted is null
                ? $"{path} does not exist"
                : $"{path} is not trusted ({untrusted})");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return EarlierComposeCredential.None($"{file} could not be read ({ex.Message})");
        }
    }

    /// <summary>
    /// A trusted credential file's password (#3914), or null: <paramref name="untrusted"/> then says why the file
    /// is not trusted, or is null when there is no file at all. Opens nothing it has not judged first, and deletes
    /// nothing.
    /// </summary>
    private static string? ReadTrustedComposeCredentialFile(string path, out string? untrusted)
    {
        /* LinkTarget before Exists: File.Exists follows a link, so a dangling one reads as "no file"; and a
           directory at the path is not a FileInfo that exists. */
        var info = new FileInfo(path);
        if (info.LinkTarget is null && !info.Exists && !Directory.Exists(path))
        {
            untrusted = null;
            return null;
        }

        untrusted = UntrustedComposeCredentialReason(info);
        if (untrusted is not null)
        {
            return null;
        }

        var password = File.ReadAllText(path).Trim();
        if (password.Length > 0)
        {
            return password;
        }

        untrusted = "it holds no password";
        return null;
    }

    /// <summary>
    /// Why a compose-store credential file cannot be trusted, or null (#3914). It must be a regular file (not a
    /// symbolic link, a directory, a device, a pipe or a socket) that no one else can reach: no group or other bits
    /// on Unix; on Windows, owned by a trusted principal (<see cref="DarlingFileSecurity.IsTrustedOwner"/>) and not
    /// readable by ordinary users (<see cref="DarlingFileSecurity.IsReadableByOrdinaryUsers"/>). A plaintext password
    /// others could read may already be theirs, so such a file is distrusted and regenerated, not re-hardened.
    ///
    /// <para><b>What this cannot see.</b> Managed .NET cannot read a Unix file's owner, and this adds no native
    /// interop to get it, so a file another user created with mode 0600 passes the mode check. What keeps anyone
    /// else from creating one is the directory: the shipped <c>darling-credentials</c> named volume is seeded
    /// root:root 0700 by the Dockerfile, and the service sets the directory owner-only again every start and
    /// distrusts it when it cannot (<see cref="PrepareComposeCredentialDirectory"/>). A bind mount whose host
    /// directory another host user owns or can write to is outside what either check can see.</para>
    /// </summary>
    internal static string? UntrustedComposeCredentialReason(FileInfo info)
    {
        if (info.LinkTarget is not null)
        {
            return "it is a symbolic link";
        }

        if (Directory.Exists(info.FullName))
        {
            return "it is a directory";
        }

        /* A device, a pipe and a socket all stat at size 0, so the size turns them away before anything opens
           them: reading a pipe that has no writer would block the start forever. */
        if (info.Length == 0)
        {
            return "it is empty, or not a regular file";
        }

        if (OperatingSystem.IsWindows())
        {
            if (!DarlingFileSecurity.IsTrustedOwner(info.FullName))
            {
                return "it is not owned by SYSTEM, Administrators or the service account";
            }

            return DarlingFileSecurity.IsReadableByOrdinaryUsers(info.FullName)
                ? "ordinary local users can read it"
                : null;
        }

        var mode = File.GetUnixFileMode(info.FullName);
        return (mode & GroupOrOtherAccess) == 0 ? null : $"its mode {Octal(mode)} gives other users access";
    }

    /// <summary>
    /// Sets the compose store's credentials directory owner-only again before any file in it is read, and says what
    /// the service may do with it this start (#3914). A symbolic link is never followed: nothing in it is read or
    /// written. Unix: 0700 is re-applied on every call and the result read back
    /// (<see cref="JudgeComposeCredentialDirectory"/>), and a directory this call found open to other users' writes
    /// has every file the service reads from it removed before the call returns
    /// (<see cref="DiscardWhatOthersCouldHavePut"/>, #4004 review), under a lock every caller in the process shares
    /// (<see cref="ComposeCredentialDirectoryGuard"/>). Windows
    /// (the live tests' runtime): a directory the service creates is hardened as it is created, and each file's
    /// owner and readability carry the rest. <paramref name="create"/> makes a missing directory; only the worker
    /// passes it. Never throws: a directory that cannot be created or checked (a read-only mount, say) is one
    /// nothing is read from or written to, and the roles are still provisioned with this start's passwords, as
    /// they were when only the write could fail.
    /// </summary>
    internal static ComposeCredentialDirectoryTrust PrepareComposeCredentialDirectory(string directory, bool create, ILogger logger)
    {
        try
        {
            var info = new DirectoryInfo(directory);
            if (info.LinkTarget is not null)
            {
                return new ComposeCredentialDirectoryTrust("it is a symbolic link", MayWrite: false);
            }

            if (!info.Exists && !create)
            {
                return new ComposeCredentialDirectoryTrust(null, MayWrite: false);
            }

            var guard = ComposeCredentialDirectoryGuard.Current;
            if (guard.UnixModes is { } modes)
            {
                /* #4004 review, round 2: the look, the chmod and the discard below are one step for every caller in
                   this process (role provisioning, a host's earlier-credential read, the log-hash key's load), so no
                   caller can find the directory owner-only between the chmod and the discard and read a file that
                   was about to go. */
                lock (guard.Gate)
                {
                    if (!info.Exists)
                    {
                        modes.CreateOwnerOnly(directory);
                    }

                    var before = modes.Get(directory);
                    try
                    {
                        modes.Set(directory, OwnerOnlyDirectory);
                    }
                    catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                    {
                        /* Neither its owner nor root: a rootless container on someone else's bind mount. The mode
                           read back below decides. */
                        logger.LogDebug("Could not set {Directory} to owner-only: {Message}", directory, ex.Message);
                    }

                    var verdict = JudgeComposeCredentialDirectory(before, modes.Get(directory));
                    if (verdict.Distrust is not { } wasOpen || !verdict.MayWrite)
                    {
                        /* A directory this process found open and could not empty stays refused to every later caller
                           this start, though it now reads owner-only, so "nothing in it is used this start" holds for
                           all of them. The next start trusts it, so what was left must not pass for a credential then:
                           a password key that could not be moved aside leaves an empty file at its quarantine name (so
                           its load refuses on every start), and a directory at a credential's name is read by nothing. */
                        return verdict.Distrust is null && guard.LeftBehind(directory) is { } leftBehind
                            ? new ComposeCredentialDirectoryTrust(leftBehind, MayWrite: false)
                            : verdict;
                    }

                    return DiscardWhatOthersCouldHavePut(directory, wasOpen, guard, logger);
                }
            }

            if (OperatingSystem.IsWindows())
            {
                if (!info.Exists)
                {
                    Directory.CreateDirectory(directory);
                    try
                    {
                        DarlingFileSecurity.HardenDirectory(directory, allowInteractiveTraverse: false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        return new ComposeCredentialDirectoryTrust(
                            $"it could not be restricted to SYSTEM, Administrators and the service account ({ex.Message})", MayWrite: false);
                    }
                }

                return new ComposeCredentialDirectoryTrust(null, MayWrite: true);
            }

            /* Unreachable: every platform but Windows has Unix modes. */
            return new ComposeCredentialDirectoryTrust("this platform has no way to check it", MayWrite: false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new ComposeCredentialDirectoryTrust($"it could not be created or checked ({ex.Message})", MayWrite: false);
        }
    }

    /// <summary>
    /// The Unix credentials-directory verdict, pure so each arm is pinned without a Linux box (#3914).
    /// <paramref name="before"/> is the mode the directory was found with, <paramref name="after"/> its mode once the
    /// service set it to 0700.
    /// <list type="bullet">
    /// <item><description>Still reachable by other users afterwards (a filesystem that ignores Unix modes): none of
    /// its files is read, and none is written there either, because a new one would be as reachable as the
    /// old.</description></item>
    /// <item><description>Writable by other users BEFORE: anyone could have put a file in it until now, and a planted
    /// 0600 file passes the file check (<see cref="UntrustedComposeCredentialReason"/> cannot see its owner), so none
    /// of its files is read as it stands: <see cref="PrepareComposeCredentialDirectory"/> removes every one of them
    /// at once (#4004 review), and new ones are written, since only the service can reach the directory
    /// now.</description></item>
    /// </list>
    /// </summary>
    internal static ComposeCredentialDirectoryTrust JudgeComposeCredentialDirectory(UnixFileMode before, UnixFileMode after)
    {
        if ((after & GroupOrOtherAccess) != 0)
        {
            return new ComposeCredentialDirectoryTrust(
                $"its mode is still {Octal(after)} after the service set it to owner-only, which a filesystem that ignores Unix modes does",
                MayWrite: false);
        }

        if ((before & (UnixFileMode.GroupWrite | UnixFileMode.OtherWrite)) != 0)
        {
            return new ComposeCredentialDirectoryTrust(
                $"its mode was {Octal(before)}, which let other users put files in it until the service set it to owner-only this start",
                MayWrite: true);
        }

        return new ComposeCredentialDirectoryTrust(null, MayWrite: true);
    }

    /// <summary>
    /// Every file the service reads from a credentials directory, and the temporary file each is written through
    /// (#4004, round 2): the three role passwords and the log-hash key, under both platforms' names. The
    /// password key's own two names are not here, because an open directory keeps that file, renamed
    /// (<see cref="PasswordKeyNames"/>); only the temporary files it is written through are.
    /// </summary>
    internal static IReadOnlyList<string> CredentialDirectoryFileNames { get; } = BuildCredentialDirectoryFileNames();

    private static string[] BuildCredentialDirectoryFileNames()
    {
        var names = new List<string>
        {
            ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName),
            ComposeStoreCredentialFileName(DarlingManagedPostgres.ViewerRoleName),
            ComposeStoreCredentialFileName(DarlingManagedPostgres.McpRoleName),
            ComposeStoreCredentialFileName(DarlingManagedPostgres.WebRoleName),
            DarlingLogHashKeyFile.UnixFileName,
            DarlingLogHashKeyFile.WindowsFileName,
        };
        return names.SelectMany(name => new[] { name, name + ".tmp" })
            .Concat(PasswordKeyNames.Select(name => name + ".tmp"))
            .ToArray();
    }

    /// <summary>The password key's file name on each platform (#5366). The directory check does not remove these when it
    /// finds the directory open: it renames what is at them to the quarantine name
    /// (<see cref="DarlingPasswordKeyFile.QuarantineSuffix"/>), so the key's load can say so, on this start and every
    /// later one, and the caller can check the key against what it published.</summary>
    internal static IReadOnlyList<string> PasswordKeyNames =>
        [DarlingPasswordKeyFile.UnixFileName, DarlingPasswordKeyFile.WindowsFileName];

    /// <summary>
    /// The directory was open to other users until this call set it owner-only (#4004, round 2), so any file
    /// in it could have been planted. Every entry at a name the service reads, except the password key, is removed NOW,
    /// before anything reads it, so the discard does not depend on this start living long enough to reach whichever
    /// step would have replaced that file: the chmod is permanent, and the next start, finding the directory
    /// owner-only, trusts what is left. Removed, the directory holds only what the service writes from here on, so it
    /// is trusted. The password key is the one file that cannot be regenerated, so it is renamed, not removed
    /// (<see cref="SetPasswordKeyAside"/>): the live name is empty before the directory lock is released, the kept file
    /// stays on disk under its quarantine name, and every later start finds it there until the key's owner accepts or
    /// retires it (<see cref="DarlingPasswordKeyFile.Accept"/>, <see cref="DarlingPasswordKeyFile.Retire"/>).
    ///
    /// <para><b>Every name is tried, and the directory stays owner-only whatever is left</b> (#4004 review, round 3).
    /// Round 2 stopped at the first entry it could not remove and set the directory back to the mode it was found
    /// with, so a directory planted at the first name shielded 0600 files planted at the later ones: an operator who
    /// did what the refusal said (set the directory 0700, remove the entry it named) got a next start that trusted the
    /// rest. Now a file or a link at any name is removed, and so is an empty directory. Only a directory with
    /// something in it can be left, a tree that is someone else's and is never removed recursively: in a directory
    /// this call has just set owner-only (the chmod took, so the service owns it or is root), unlinking a file or a
    /// link fails only for someone who can set an immutable attribute or mount over a file, which is more than
    /// writing to the directory gives. Every reader refuses a directory at a credential's name, so what is left is
    /// never read as one, and the next start, finding the directory owner-only, trusts only what the service writes
    /// there. Reopening it instead would let anyone put files in it again until an operator closed it. When anything
    /// is left, nothing in the directory is read or written this start, by this caller or any later one in the process
    /// (<see cref="ComposeCredentialDirectoryGuard.LeftBehind"/>), and the reason names every path to remove.</para>
    ///
    /// <para>A key removed here is recorded on <paramref name="guard"/>, so the key's load can say the key it
    /// generates replaced one (<see cref="ComposeCredentialDirectoryGuard.TakeDiscardedKey"/>).</para>
    /// </summary>
    private static ComposeCredentialDirectoryTrust DiscardWhatOthersCouldHavePut(
        string directory, string wasOpen, ComposeCredentialDirectoryGuard guard, ILogger logger)
    {
        var removed = new List<string>();
        var left = new List<string>();

        /* #5366: the password key is kept, not removed: whatever is at its name is renamed (never overwritten) to a name
           that says it was found while the directory was open, so the record is on disk and survives a restart. The live
           name is empty before the lock is released, and the key's load reads the kept file under its usual checks. */
        foreach (var name in PasswordKeyNames)
        {
            SetPasswordKeyAside(directory, name, guard, left, logger);
        }

        /* The password key is written through a temporary file with a name of its own each time (#5366): the ones a
           crashed write left are removed with the rest. */
        var names = new List<string>(CredentialDirectoryFileNames);
        foreach (var keyName in PasswordKeyNames)
        {
            try
            {
                names.AddRange(Directory.EnumerateFileSystemEntries(directory, keyName + DarlingServiceKeyFile.UniqueTemporaryInfix + "*")
                    .Select(Path.GetFileName)!);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                left.Add($"{Path.Combine(directory, keyName + DarlingServiceKeyFile.UniqueTemporaryInfix)}* (they could not be listed: {ex.Message})");
            }
        }

        foreach (var name in names)
        {
            var path = Path.Combine(directory, name);
            var info = new FileInfo(path);
            var isDirectory = info.LinkTarget is null && Directory.Exists(path);
            if (info.LinkTarget is null && !info.Exists && !isDirectory)
            {
                continue;
            }

            try
            {
                /* A symbolic link is removed itself, never followed. A directory only when it is empty: the delete is
                   not recursive, so nothing in someone else's tree is touched. */
                if (isDirectory)
                {
                    Directory.Delete(path, recursive: false);
                }
                else
                {
                    File.Delete(path);
                }

                removed.Add(name);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                left.Add($"{path} ({ex.Message})");
            }
        }

        if (removed.Contains(DarlingLogHashKeyFile.FileName, StringComparer.Ordinal))
        {
            guard.RecordDiscardedKey(directory, wasOpen);
        }

        if (removed.Count > 0)
        {
            logger.LogError(
                "The credentials directory {Directory} was open to other users until now ({Reason}), so its files could have been planted: {Files} discarded. Every role password removed is generated again and re-asserted on its role, and a removed log-hash key is generated again, which gives every log event stored from now on a new identity. Keep the directory owner-only (on Kubernetes, an fsGroup re-applied on every mount opens it again) so this does not repeat, and run one service per credentials volume (on Kubernetes, one replica with strategy: Recreate): a second one starting beside this one could read the directory between this start's chmod and this removal.",
                directory, wasOpen, string.Join(", ", removed));
        }

        if (left.Count > 0)
        {
            var reason = $"{wasOpen}, and {(left.Count == 1 ? "this entry at a credential's name" : "these entries at credentials' names")} could not be removed, so nothing in it is used this start: "
                + $"{string.Join("; ", left)}. Every other credential file there is removed and the directory stays owner-only, so remove {(left.Count == 1 ? "it" : "each of them")} and restart";
            logger.LogError("The credentials directory {Directory} is not trusted ({Reason}).", directory, reason);
            guard.RecordLeftBehind(directory, reason);
            return new ComposeCredentialDirectoryTrust(reason, MayWrite: false);
        }

        guard.RecordLeftBehind(directory, null);
        return new ComposeCredentialDirectoryTrust(null, MayWrite: true);
    }

    /// <summary>
    /// Renames whatever is at <paramref name="name"/> in <paramref name="directory"/> to the password key's quarantine name
    /// (<see cref="DarlingPasswordKeyFile.QuarantineSuffix"/>), with no overwrite (#5366). A file already at the
    /// quarantine name is never replaced and never trusted: this one goes to the next free
    /// <c>name.discarded-N</c> instead. Nothing is deleted. A directory or a link at the name is moved as it is, never
    /// followed. On Unix a regular file that is kept is set to 0600 afterwards (a mount's group setting can add group
    /// bits back); when that fails the load reports the file untrusted. An entry that cannot be moved is added to
    /// <paramref name="left"/>, which refuses the directory this start.
    /// </summary>
    private static void SetPasswordKeyAside(string directory, string name, ComposeCredentialDirectoryGuard guard, List<string> left, ILogger logger)
    {
        var path = Path.Combine(directory, name);
        var quarantine = path + DarlingPasswordKeyFile.QuarantineSuffix;
        var target = quarantine;
        var moveStarted = false;
        try
        {
            if (!DarlingServiceKeyFile.AnythingAt(path))
            {
                return;
            }

            for (var attempt = 1; DarlingServiceKeyFile.AnythingAt(target) && attempt < 1000; attempt++)
            {
                target = path + ".discarded-" + attempt.ToString(CultureInfo.InvariantCulture);
            }

            var regularFile = new FileInfo(path).LinkTarget is null && File.Exists(path);
            moveStarted = true;
            if (!regularFile && Directory.Exists(path) && new FileInfo(path).LinkTarget is null)
            {
                Directory.Move(path, target);
            }
            else
            {
                File.Move(path, target, overwrite: false);
            }

            moveStarted = false;
            if (regularFile && guard.UnixModes is { } modes)
            {
                SetKeptKeyOwnerOnly(modes, target, logger);
            }

            logger.LogWarning(
                "The password key at {Path} was found while the credentials directory {Directory} was open to other users, and is kept as {Kept} (#5366). It is not deleted, and the key's load checks it against the store before it is used.",
                path, directory, target);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            left.Add($"{path} (it could not be set aside as {Path.GetFileName(target)}: {ex.Message})");

            /* A move that failed leaves the key at its live name in a directory that is owner-only from now on, which the
               next start would trust and load with nothing to say it was found open (#5366). An empty file at the
               quarantine name makes every later load refuse (two names, or an empty key), however many starts follow. When
               the quarantine name was taken already, that file is the record and nothing is added. */
            if (moveStarted && target == quarantine)
            {
                try
                {
                    var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
                    if (!OperatingSystem.IsWindows())
                    {
                        options.UnixCreateMode = OwnerOnlyFile;
                    }

                    using var marker = new FileStream(quarantine, options);
                }
                catch (Exception markerEx) when (markerEx is IOException or UnauthorizedAccessException)
                {
                    logger.LogError("Could not leave a marker at {Path} for the password key that could not be set aside: {Message}", quarantine, markerEx.Message);
                }
            }
        }
    }

    /// <summary>
    /// Sets a kept password key to 0600 (#5366; a mount's group setting can add group bits back), but only a file that is
    /// the service's own with one name: as root, a chmod would otherwise change the mode of whatever file someone
    /// linked in while the directory was open. A file whose owner cannot be told is left as it is; the load then
    /// reports it untrusted by its mode.
    /// </summary>
    private static void SetKeptKeyOwnerOnly(IUnixDirectoryModes modes, string target, ILogger logger)
    {
        try
        {
            if (DarlingServiceKeyFile.UnexpectedOwnerReason(target) is { } notOurs)
            {
                logger.LogDebug("Left {File} as it is: {Reason}", target, notOurs);
                return;
            }

            modes.Set(target, OwnerOnlyFile);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogDebug("Could not set {File} to owner-only: {Message}", target, ex.Message);
        }
    }

    private const UnixFileMode GroupOrOtherAccess =
        UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute
        | UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

    internal const UnixFileMode OwnerOnlyDirectory = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;

    internal const UnixFileMode OwnerOnlyFile = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    /// <summary>A mode the way an operator types it, <c>0700</c>, rather than the enum's flag names.</summary>
    internal static string Octal(UnixFileMode mode) => "0" + Convert.ToString((int)mode, 8);

    /// <summary>
    /// Writes a generated compose-store credential (#3914), owner-only from the moment it exists: created 0600 on
    /// Unix (the <c>UnixCreateMode</c> applies at creation, so there is no window at the umask's mode), and created
    /// with its ACL already applied on Windows (<see cref="DarlingFileSecurity.CreateHardenedFile"/>, so the folder's
    /// inherited access never reaches it), then renamed over the old file so a reader never sees half of one. On
    /// Windows the result is checked before it replaces anything, the managed files' verify-don't-assume rule: a file
    /// ordinary users can still read after one more harden is not put in place. Best-effort: the role already has
    /// this start's password, so a failure costs a re-assert on the next start and is logged. The directory is the
    /// caller's (<see cref="PrepareComposeCredentialDirectory"/>).
    /// </summary>
    private static void PersistComposeCredential(string directory, ComposeCredential credential, ILogger logger)
    {
        var path = Path.Combine(directory, ComposeStoreCredentialFileName(credential.Role));
        var temporary = path + ".tmp";
        try
        {
            File.Delete(temporary);
            if (OperatingSystem.IsWindows())
            {
                using (var stream = DarlingFileSecurity.CreateHardenedFile(temporary, allowInteractiveRead: false))
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(credential.Password);
                    FlushToDisk(writer, stream);
                }

                if (DarlingFileSecurity.IsReadableByOrdinaryUsers(temporary))
                {
                    DarlingFileSecurity.HardenFile(temporary, allowInteractiveRead: false);
                    if (DarlingFileSecurity.IsReadableByOrdinaryUsers(temporary))
                    {
                        throw new InvalidOperationException(
                            "ordinary local users can read it even after it was created owner-only and hardened again"
                            + DarlingFileSecurity.DescribeOwnerAndExposure(temporary));
                    }
                }
            }
            else
            {
                using var stream = new FileStream(temporary, new FileStreamOptions
                {
                    Mode = FileMode.CreateNew,
                    Access = FileAccess.Write,
                    UnixCreateMode = OwnerOnlyFile,
                });
                using var writer = new StreamWriter(stream);
                writer.Write(credential.Password);
                FlushToDisk(writer, stream);
            }

            File.Move(temporary, path, overwrite: true);
            logger.LogInformation(
                "Generated the compose store's '{Role}' role credential ({File})", credential.Role, path);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "Could not write the compose store's '{Role}' credential {File} ({Message}). The role has this start's password; the next start generates a new one and re-asserts it.",
                credential.Role, path, ex.Message);
            try
            {
                File.Delete(temporary);
            }
            catch (Exception cleanup) when (cleanup is IOException or UnauthorizedAccessException)
            {
                logger.LogDebug("Could not remove {File}: {Message}", temporary, cleanup.Message);
            }
        }
    }

    /// <summary>
    /// Puts what <paramref name="writer"/> wrote on the disk before its file is renamed into place (#4004 review). A
    /// rename can reach the disk before the data it names does, so a power loss could otherwise leave a zero-length
    /// file where a good one was: a key refused at every start, or a role credential a host can no longer use.
    /// </summary>
    internal static void FlushToDisk(StreamWriter writer, FileStream stream)
    {
        writer.Flush();
        stream.Flush(flushToDisk: true);
    }

    /// <summary>One role's compose-store password and whether it was generated this start (#3914). A class, not
    /// a record, so no generated <c>ToString</c> can put the password in a log line.</summary>
    private sealed class ComposeCredential
    {
        public ComposeCredential(string role, string password, bool generated)
        {
            Role = role;
            Password = password;
            Generated = generated;
        }

        public string Role { get; }

        public string Password { get; }

        public bool Generated { get; }
    }
}

/// <summary>
/// The managed roles whose provisioning <c>ALTER ROLE</c> carries a password verifier this start (#3910).
/// </summary>
[Flags]
public enum PasswordReassert
{
    /// <summary>No role: every stored verifier already accepts its credential file.</summary>
    None = 0,

    /// <summary>The <c>admin</c> role.</summary>
    Admin = 1,

    /// <summary>The <c>viewer</c> role.</summary>
    Viewer = 2,

    /// <summary>The <c>mcp</c> role.</summary>
    Mcp = 4,

    /// <summary>The <c>web</c> role.</summary>
    Web = 8,

    /// <summary>Every role: the default, and what a store without stored verifiers gets.</summary>
    All = Admin | Viewer | Mcp | Web,
}

/// <summary>
/// One secret-bearing <c>config</c> table's viewer ACL split: the <paramref name="NonSecretColumns"/> the
/// read-only <c>viewer</c> role is granted column-level SELECT on, and the <paramref name="SecretColumns"/>
/// it is denied (credential blobs / bearer secrets). The two sets must PARTITION the table's real columns —
/// the live security test asserts their union equals the table's actual column set, so a migration that adds
/// a column without classifying it here fails that gate. See
/// <see cref="DarlingManagedRoles.ViewerRestrictedConfigTables"/>.
/// </summary>
public sealed record ViewerSecretTableAcl(
    string Table, IReadOnlyList<string> NonSecretColumns, IReadOnlyList<string> SecretColumns);

/// <summary>
/// Which store a provisioning batch is written for (#3914): the managed store, or the compose distribution's own.
/// A class with two factories rather than a record, so no caller can put an arbitrary marker into the batch's
/// string literals or an unquoted name into its identifiers.
/// </summary>
public sealed class ProvisioningTarget
{
    private ProvisioningTarget(string ownerIdentifier, string databaseIdentifier, string roleMarker, bool revokePublicDatabaseAccess)
    {
        OwnerIdentifier = ownerIdentifier;
        DatabaseIdentifier = databaseIdentifier;
        RoleMarker = roleMarker;
        RevokePublicDatabaseAccess = revokePublicDatabaseAccess;
    }

    /// <summary>The managed store: owner and database <c>darling</c>, bare, exactly as the batch always named
    /// them, and <see cref="DarlingManagedRoles.RoleMarker"/>.</summary>
    public static ProvisioningTarget Managed { get; } = new(
        DarlingManagedPostgres.UserName, DarlingManagedPostgres.DatabaseName, DarlingManagedRoles.RoleMarker, revokePublicDatabaseAccess: true);

    /// <summary>The compose store, named by the login and database the service is connected to, quoted, since
    /// the compose file can rename both.</summary>
    public static ProvisioningTarget ComposeStore(string login, string database) => new(
        QuoteIdentifier(login), QuoteIdentifier(database), DarlingManagedRoles.ComposeStoreRoleMarker, revokePublicDatabaseAccess: false);

    /// <summary>The role that creates the store's tables, as SQL: what <c>ALTER DEFAULT PRIVILEGES FOR ROLE</c> names.</summary>
    public string OwnerIdentifier { get; }

    /// <summary>The store's database, as SQL.</summary>
    public string DatabaseIdentifier { get; }

    /// <summary>The comment stamped on the roles this batch creates, and required on the ones it adopts.</summary>
    public string RoleMarker { get; }

    /// <summary>Whether the batch revokes PUBLIC's database-level access (<c>CONNECT</c>/<c>TEMPORARY</c>).</summary>
    public bool RevokePublicDatabaseAccess { get; }

    internal static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}

/// <summary>The login and role facts <see cref="DarlingManagedRoles.RefuseComposeStore"/> decides on (#3914).</summary>
/// <param name="Login">The role the service connects as.</param>
/// <param name="Database">The database it is connected to.</param>
/// <param name="BootstrapSuperuser">Whether that role is the cluster's bootstrap superuser (oid 10).</param>
/// <param name="ExistingRoleMarkers">Each of admin/viewer/mcp that already exists, with its role comment.</param>
/// <param name="OtherDatabases">The cluster's databases other than the store's own, <c>postgres</c> and the
/// templates, by name.</param>
internal sealed record ComposeStoreFacts(
    string Login, string Database, bool BootstrapSuperuser, IReadOnlyDictionary<string, string?> ExistingRoleMarkers,
    IReadOnlyList<string> OtherDatabases);

/// <summary>
/// What <see cref="DarlingManagedRoles.EnsureComposeStoreProvisionedAsync"/> did (#3914). A class, not a record, so
/// no generated <c>ToString</c> can print the passwords into a log line.
/// </summary>
public sealed class ComposeStoreProvisioning
{
    private ComposeStoreProvisioning(bool provisioned, int appliedSeconds, string? viewerPassword, string? mcpPassword, string? webPassword, string? refusalReason)
    {
        Provisioned = provisioned;
        AppliedComposeStatementTimeoutSeconds = appliedSeconds;
        ViewerPassword = viewerPassword;
        McpPassword = mcpPassword;
        WebPassword = webPassword;
        RefusalReason = refusalReason;
    }

    /// <summary>The roles exist, carry the passwords below, and hold the managed grants.</summary>
    public bool Provisioned { get; }

    /// <summary>The compose statement_timeout written onto the roles, for the #2918 reload baseline.</summary>
    public int AppliedComposeStatementTimeoutSeconds { get; }

    /// <summary>The <c>viewer</c> role's password; null when refused.</summary>
    public string? ViewerPassword { get; }

    /// <summary>The <c>mcp</c> role's password; null when refused.</summary>
    public string? McpPassword { get; }

    /// <summary>The <c>web</c> role's password; null when refused.</summary>
    public string? WebPassword { get; }

    /// <summary>Why nothing was provisioned; null when provisioned.</summary>
    public string? RefusalReason { get; }

    internal static ComposeStoreProvisioning Succeeded(int appliedSeconds, string viewerPassword, string mcpPassword, string webPassword) =>
        new(true, appliedSeconds, viewerPassword, mcpPassword, webPassword, null);

    internal static ComposeStoreProvisioning Refused(string reason) =>
        new(false, DarlingManagedRoles.ComposeStatementTimeoutUnknown, null, null, null, reason);
}

/// <summary>
/// What the service holds from an earlier start for one of the compose store's roles (#3914,
/// <see cref="DarlingManagedRoles.ReadEarlierComposeCredential"/>): a trusted credential file's password, or why
/// there is none. A class, not a record, so no generated <c>ToString</c> can print the password.
/// </summary>
internal sealed class EarlierComposeCredential
{
    private EarlierComposeCredential(string? password, string? missingReason)
    {
        Password = password;
        MissingReason = missingReason;
    }

    /// <summary>The role's password; null when there is none to use.</summary>
    internal string? Password { get; }

    /// <summary>Why there is none, for the owner-fallback warning; null when there is.</summary>
    internal string? MissingReason { get; }

    internal static EarlierComposeCredential Trusted(string password) =>
        new(password ?? throw new ArgumentNullException(nameof(password)), null);

    internal static EarlierComposeCredential None(string reason) =>
        new(null, reason ?? throw new ArgumentNullException(nameof(reason)));
}

/// <summary>What the service may do with the compose store's credentials directory this start (#3914).</summary>
/// <param name="Distrust">Why none of its files is read this start; null when they are.</param>
/// <param name="MayWrite">Whether a credential generated this start is written there.</param>
internal sealed record ComposeCredentialDirectoryTrust(string? Distrust, bool MayWrite);

/// <summary>
/// How this process looks at its credentials directories (#4004 review): the Unix mode calls, and the one lock every
/// caller of <see cref="DarlingManagedRoles.PrepareComposeCredentialDirectory"/> takes (role provisioning, a host's
/// <see cref="DarlingManagedRoles.ReadEarlierComposeCredential"/>, <see cref="DarlingLogHashKeyFile.Load"/>), so the
/// look, the chmod and the discard of whatever an open directory held are one step.
///
/// <para>Round 1 kept the mode each directory had at the process's FIRST look and judged every later caller by it. That
/// record lived only in memory while the chmod was permanent, so a start that closed the directory and ended before
/// the step that replaced a planted file (a stand-down, a crash) left the file for the next start to trust, and a
/// directory opened again after the first look was still judged by it. The discard now happens at the look itself,
/// so the verdict is on disk, and nothing needs remembering: only the lock is kept, because without it a second
/// caller could find the directory owner-only between the first caller's chmod and its discard, and read a planted
/// file before it went.</para>
///
/// <para><b>One process per volume.</b> The lock is the process's: the shipped compose runs the worker, MCP and web in
/// one process, so it covers every caller there. Two services on one credentials volume (a Kubernetes rolling update,
/// a second replica) are not covered, and one could find the directory owner-only between the other's chmod and its
/// discard. That is documented as unsupported (one replica, <c>strategy: Recreate</c>) rather than locked across
/// processes (#4004 review, round 3): the only place both could lock is a file in this directory, which is the one
/// place the check exists to distrust. While the directory is open, whoever else can write to it can put a symbolic
/// link at the lock's name, which the service, running as root, would open through (.NET cannot open without
/// following one), or hold the lock forever.</para>
///
/// A test stands in its own Unix mode calls (<see cref="BeginForTest"/>).
/// </summary>
internal sealed class ComposeCredentialDirectoryGuard
{
    private static readonly ComposeCredentialDirectoryGuard Process = new(PlatformModes());

    private static readonly AsyncLocal<ComposeCredentialDirectoryGuard?> TestGuard = new();

    private static PlatformUnixDirectoryModes? PlatformModes()
    {
        if (OperatingSystem.IsWindows())
        {
            return null;
        }

        return new PlatformUnixDirectoryModes();
    }

    private ComposeCredentialDirectoryGuard(IUnixDirectoryModes? unixModes) => UnixModes = unixModes;

    /// <summary>The process's, or the calling test's.</summary>
    internal static ComposeCredentialDirectoryGuard Current => TestGuard.Value ?? Process;

    /// <summary>The Unix mode calls; null on Windows, which judges by ACL instead.</summary>
    internal IUnixDirectoryModes? UnixModes { get; }

    /// <summary>Held across the look, the chmod and the discard.</summary>
    internal Lock Gate { get; } = new();

    /* Both keyed by the directory's full path with trailing separators trimmed, so every caller's spelling of it meets. */
    private readonly ConcurrentDictionary<string, string> _discardedKeys = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, string> _leftBehind = new(StringComparer.Ordinal);

    /// <summary>
    /// Records why the discard left entries in <paramref name="directory"/> this start, or clears that with null
    /// (#4004 review, round 3), so every later caller in this process gets the same refusal.
    /// </summary>
    internal void RecordLeftBehind(string directory, string? reason)
    {
        if (reason is null)
        {
            _leftBehind.TryRemove(Key(directory), out _);
        }
        else
        {
            _leftBehind[Key(directory)] = reason;
        }
    }

    /// <summary>Why the discard left entries in <paramref name="directory"/> in this process; null when it did not.</summary>
    internal string? LeftBehind(string directory) => _leftBehind.TryGetValue(Key(directory), out var reason) ? reason : null;

    /// <summary>
    /// Records that the discard removed the log-hash key from <paramref name="directory"/> because
    /// <paramref name="reason"/> (#4004 review, round 3): the look that removes it can be any caller's, and only the
    /// key's own load knows whether it then generates the replacement.
    /// </summary>
    internal void RecordDiscardedKey(string directory, string reason) => _discardedKeys[Key(directory)] = reason;

    /// <summary>
    /// Why the discard removed the log-hash key from <paramref name="directory"/> in this process, once: null when it
    /// did not, or when this was already taken.
    /// </summary>
    internal string? TakeDiscardedKey(string directory) => _discardedKeys.TryRemove(Key(directory), out var reason) ? reason : null;

    private static string Key(string directory) => Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));

    /// <summary>
    /// Stands in <paramref name="unixModes"/> for the calling test's flow only, so the Unix verdict and the discard are
    /// pinned on the Windows box the suite runs on. Disposing it ends that; beginning another with the same stand-in,
    /// which keeps the mode the service set, is the service's next start.
    /// </summary>
    internal static IDisposable BeginForTest(IUnixDirectoryModes unixModes)
    {
        ArgumentNullException.ThrowIfNull(unixModes);
        var previous = TestGuard.Value;
        TestGuard.Value = new ComposeCredentialDirectoryGuard(unixModes);
        return new EndTest(previous);
    }

    private sealed class EndTest(ComposeCredentialDirectoryGuard? previous) : IDisposable
    {
        public void Dispose() => TestGuard.Value = previous;
    }
}

/// <summary>
/// The Unix mode calls behind the credentials-directory verdict (#4004 review): create a directory owner-only, read its
/// mode, set it. A seam, so a test can stand in a directory reported as 0777
/// (<see cref="ComposeCredentialDirectoryGuard.BeginForTest"/>).
/// </summary>
internal interface IUnixDirectoryModes
{
    void CreateOwnerOnly(string directory);

    UnixFileMode Get(string directory);

    void Set(string directory, UnixFileMode mode);

    /// <summary>The owner's user id and the link count of the file at <paramref name="path"/> (#5366), or null when they
    /// cannot be read here (Windows, a platform whose stat layout is not known, a stand-in that does not model them).</summary>
    UnixFileOwner? OwnerOf(string path) => null;

    /// <summary>The user id this process runs as, for comparing with <see cref="UnixFileOwner.UserId"/>.</summary>
    uint EffectiveUserId => 0;
}

/// <summary>Who owns a file and how many names it has (#5366).</summary>
internal readonly record struct UnixFileOwner(uint UserId, ulong Links);

/// <summary>The platform's own <see cref="IUnixDirectoryModes"/>, on every platform but Windows.</summary>
[UnsupportedOSPlatform("windows")]
internal sealed class PlatformUnixDirectoryModes : IUnixDirectoryModes
{
    public void CreateOwnerOnly(string directory) => Directory.CreateDirectory(directory, DarlingManagedRoles.OwnerOnlyDirectory);

    public UnixFileMode Get(string directory) => File.GetUnixFileMode(directory);

    public void Set(string directory, UnixFileMode mode) => File.SetUnixFileMode(directory, mode);

    public UnixFileOwner? OwnerOf(string path) => FileIdentity.UnixOwnerOf(path);

    public uint EffectiveUserId => FileIdentity.EffectiveUserId();
}
