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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
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
/// per-database override editor) and <c>config.config_mute_rules</c> (#3450, the dedicated mute-rule
/// endpoints — plus the two <c>config_service</c> beacon columns its bump trigger writes as the caller).
/// All non-secret tables; over the web, editing is gated server-side by the host's auth + the seat model
/// (an OIDC viewer seat is refused every write) — these grants are only the floor beneath that gate. A
/// locked-down deployment points the Viewer at this role, and its WPF surfaces still read as "look but
/// don't touch": the read-only probe discriminates on a privilege this role never gets
/// (<c>ViewerDataService.ReadOnlyProbeSql</c>).</item>
/// <item><b><c>mcp</c></b> — the (optionally network-exposed) MCP host's store identity
/// (darling-network-endpoints, D3-role): the SAME read surface as <c>viewer</c> (SELECT on
/// <c>collect</c> + <c>config</c>-minus-the-secret-columns) PLUS a NARROW, enumerated set of writes —
/// INSERT on <c>collect.analysis_findings</c> and <c>config.analysis_muted</c> (what <c>analyze_server</c>
/// persists + the <c>mute</c> tool need), INSERT/UPDATE/DELETE on <c>config.custom_views</c> (the
/// custom-view tools, #1599), the alert-tuning writes (INSERT/UPDATE/DELETE on
/// <c>config.config_mute_rules</c> + UPDATE on the singleton <c>config.config_alert_settings</c> + UPDATE on
/// the single non-secret <c>email_cooldown_minutes</c> column of <c>config.config_notification</c>, plus the
/// two beacon columns of <c>config.config_service</c> so the settings write's self-bump trigger can fire),
/// and the server-onboarding writes (INSERT/UPDATE/DELETE on <c>config.config_monitored_servers</c> for the
/// <c>add_servers</c>/<c>remove_server</c> tools — a single non-secret-KEY table; the credential column stays
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
            },
            /* remediation_encrypted_password is the same kind of thing as encrypted_password beside it: a
               DPAPI blob whose whole purpose is to authenticate a WRITE to a monitored server, so if
               anything in this table is secret it is. Named explicitly rather than left unclassified
               because unclassified is only invisible until someone "fixes" the failing security gate by
               adding the column to whichever list is nearer. */
            SecretColumns: new[] { "encrypted_password", "remediation_encrypted_password" }),

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
            },
            /* generic_headers carries the Authorization bearer token itself, and generic_url is a bearer
               secret like the sibling webhook URLs (#1506 / V26). pagerduty_routing_key is the Events API v2
               integration key — a bearer secret like the webhook URLs. */
            SecretColumns: new[]
            {
                "smtp_encrypted_password", "smtp_username", "teams_url", "slack_url",
                "generic_url", "generic_headers", "pagerduty_routing_key",
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
            },
            SecretColumns: new[] { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key" }),
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
        string.Join("\n", ViewerRestrictedConfigTables.Select(acl =>
            $"REVOKE SELECT ON {configSchema}.{acl.Table} FROM {viewerRole};\n" +
            $"GRANT SELECT ({string.Join(", ", acl.NonSecretColumns)}) ON {configSchema}.{acl.Table} TO {viewerRole};"));

    /// <summary>
    /// Ensures the <c>admin</c>/<c>viewer</c>/<c>mcp</c> roles, their DPAPI credentials, and the
    /// collect/config grants exist and match — idempotent and self-healing. Opens one connection from the
    /// owner-<c>darling</c> data source (ALTER DEFAULT PRIVILEGES FOR ROLE darling only governs objects
    /// darling creates, which is all of them). MUST run AFTER migration: the <c>mcp</c> role's per-table
    /// INSERT grants name <c>collect.analysis_findings</c> / <c>config.analysis_muted</c> by qualified
    /// name, which the one-shot batch requires to already exist (they do — the worker migrates before it
    /// calls this). Throws on a hard failure; the caller degrades (the Viewer/MCP cannot connect as their
    /// roles until a later start succeeds) but keeps collecting.
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
    public static async Task<int> EnsureProvisionedAsync(
        NpgsqlDataSource dataSource, string dataDirectory, ILogger logger, CancellationToken cancellationToken = default)
    {
        if (dataSource is null)
        {
            throw new ArgumentNullException(nameof(dataSource));
        }

        if (string.IsNullOrWhiteSpace(dataDirectory))
        {
            throw new ArgumentException("Data directory is required.", nameof(dataDirectory));
        }

        /* admin/viewer credentials are read by the interactive Viewer -> INTERACTIVE-readable ACL. */
        var adminPassword = EnsureRoleCredential(
            DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory), DarlingManagedPostgres.AdminRoleName,
            allowInteractiveRead: true, logger);
        var viewerPassword = EnsureRoleCredential(
            DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory), DarlingManagedPostgres.ViewerRoleName,
            allowInteractiveRead: true, logger);
        /* The mcp credential is consumed only by the in-service MCP host, never an interactive Viewer, so it
           is hardened NON-interactive (mirrors the superuser posture, not admin/viewer's) — Round 4 #4. */
        var mcpPassword = EnsureRoleCredential(
            DarlingManagedPostgres.McpCredentialPathFor(dataDirectory), DarlingManagedPostgres.McpRoleName,
            allowInteractiveRead: false, logger);

        return await ProvisionRolesAsync(
            dataSource, adminPassword, viewerPassword, mcpPassword, ProvisioningTarget.Managed, logger, cancellationToken);
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

        var applied = await ProvisionRolesAsync(
            dataSource, admin.Password, viewer.Password, mcp.Password,
            ProvisioningTarget.ComposeStore(facts.Login, facts.Database), logger, cancellationToken);

        foreach (var credential in new[] { admin, viewer, mcp })
        {
            if (credential.Generated && directory.MayWrite)
            {
                PersistComposeCredential(credentialDirectory, credential, logger);
            }
        }

        return ComposeStoreProvisioning.Succeeded(applied, viewer.Password, mcp.Password);
    }

    /// <summary>
    /// The rest of provisioning once each role's password is in hand — shared by
    /// <see cref="EnsureProvisionedAsync"/> and <see cref="EnsureComposeStoreProvisionedAsync"/> (#3914), which
    /// differ only in where the passwords live and in <paramref name="target"/>. Returns the compose
    /// statement_timeout it wrote onto the roles (see <see cref="EnsureProvisionedAsync"/>).
    /// </summary>
    private static async Task<int> ProvisionRolesAsync(
        NpgsqlDataSource dataSource, string adminPassword, string viewerPassword, string mcpPassword,
        ProvisioningTarget target, ILogger logger, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        /* #2357: read the live knob rather than a constant. Ordering is what makes this safe -- migrations
           run before provisioning at startup, so the column exists by now -- and because this DDL is re-run
           on every managed start, a changed value reaches an existing install on its next restart without
           any new machinery. A store whose config row is not seeded yet answers with the default. */
        var composeTimeoutSeconds = await ReadComposeStatementTimeoutAsync(connection, logger, cancellationToken);

        /* #3910: the batch carries SCRAM-SHA-256 VERIFIERS, never a password, so no surface that records
           statement text (the server log's STATEMENT lines, log_statement, auto_explain, pg_stat_statements
           with utility tracking on) can capture a credential. And a role whose stored verifier already accepts
           its credential file's password is not re-asserted at all: a steady-state start sends no PASSWORD
           clause. The CREATE branch still carries a fresh verifier, for a role that does not exist yet. */
        var stored = await ReadStoredRoleSecretsAsync(connection, logger, cancellationToken);
        var reassert = PlanPasswordReassert(stored, adminPassword, viewerPassword, mcpPassword);

        await using var command = new NpgsqlCommand(
            BuildProvisioningSql(
                ScramSha256Verifier.Create(adminPassword),
                ScramSha256Verifier.Create(viewerPassword),
                ScramSha256Verifier.Create(mcpPassword),
                composeTimeoutSeconds,
                reassert,
                target),
            connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken);

        logger.LogInformation(
            "Role passwords: {Reasserted} (sent as SCRAM-SHA-256 verifiers, never as the password)",
            reassert == PasswordReassert.None
                ? "unchanged, the stored verifiers already accept every credential file"
                : "re-asserted for " + DescribeReassert(reassert));

        logger.LogInformation(
            "Least-privilege roles ready (admin: read both schemas + write config; viewer: read-only + the narrow web-surface writes (custom_views, custom_alert_rules, database_state_expected, config_mute_rules + the reload beacon); mcp: viewer's reads + INSERT on analysis_findings/analysis_muted + write config.custom_views + tune alerting (config_mute_rules, config_alert_settings, config_notification.email_cooldown_minutes, config_service reload beacon) + onboard servers (config_monitored_servers)) — the Viewer, the web dashboard and the MCP host no longer connect as the superuser");

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
    /// </summary>
    public static string BuildComposeStatementTimeoutSql(int composeStatementTimeoutSeconds)
    {
        const string viewer = DarlingManagedPostgres.ViewerRoleName;
        const string mcp = DarlingManagedPostgres.McpRoleName;
        var statementTimeout =
            $"{StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeStatementTimeoutSeconds)}s";
        var slowStatement = $"{SlowStatementThresholdMs(composeStatementTimeoutSeconds).ToString(CultureInfo.InvariantCulture)}ms";

        return $@"ALTER ROLE {viewer} SET statement_timeout = '{statementTimeout}';
ALTER ROLE {mcp}    SET statement_timeout = '{statementTimeout}';
ALTER ROLE {viewer} SET log_min_duration_statement = '{slowStatement}';
ALTER ROLE {mcp}    SET log_min_duration_statement = '{slowStatement}';";
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
        const string mcp = DarlingManagedPostgres.McpRoleName;

        return $@"ALTER ROLE {viewer} SET log_parameter_max_length = 0;
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
            await using var command = new NpgsqlCommand(
                BuildComposeStatementTimeoutSql(composeStatementTimeoutSeconds), connection) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken);

            logger.LogInformation(
                "Compose statement_timeout re-asserted on the viewer/mcp roles at {Seconds}s, with slow-statement logging at {SlowMs} ms — takes effect on each role's next session (an already-connected viewer keeps the old ceiling until it reconnects)",
                StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeStatementTimeoutSeconds),
                SlowStatementThresholdMs(composeStatementTimeoutSeconds));
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "Could not re-assert the compose statement_timeout on the viewer/mcp roles ({Message}) — the live ceiling is whatever the last successful provisioning set, and the next service start will converge it.",
                ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Reads a TRUSTED existing role credential, or generates + DPAPI-persists a fresh one (self-heal),
    /// then restricts its ACL. Same 32-char alnum <see cref="DarlingManagedPostgres.GeneratePassword"/>
    /// and DPAPI-LocalMachine posture as the owner credential; unlike the owner's, a role password can
    /// always be re-asserted (<c>ALTER ROLE … PASSWORD</c>), so an untrusted-owned (possibly pre-planted)
    /// file is discarded and regenerated rather than trusted.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static string EnsureRoleCredential(string credentialPath, string roleName, bool allowInteractiveRead, ILogger logger)
    {
        string password;
        if (File.Exists(credentialPath) && DarlingFileSecurity.IsTrustedOwner(credentialPath))
        {
            password = DarlingSecrets.Unprotect(File.ReadAllText(credentialPath).Trim());
        }
        else
        {
            if (File.Exists(credentialPath))
            {
                /* Pre-plant defense: a role credential owned by an arbitrary local user would feed the
                   caller's ALTER ROLE … PASSWORD re-assert a password the attacker chose. Discard it. */
                logger.LogWarning(
                    "The managed '{Role}' credential {File} is not owned by a trusted principal — discarding and regenerating it (possible pre-plant).",
                    roleName, Path.GetFileName(credentialPath));
                TryDelete(credentialPath, logger);
            }

            password = DarlingManagedPostgres.GeneratePassword();
            File.WriteAllText(credentialPath, DarlingSecrets.Protect(password));
            logger.LogInformation(
                "Generated the managed '{Role}' role credential ({File})", roleName, Path.GetFileName(credentialPath));
        }

        /* Re-harden every start (self-healing): admin/viewer are additionally readable by the interactive
           operator (whose Viewer reads them); mcp is NOT (SYSTEM + Administrators + service account only —
           the in-service MCP host reads it, never an interactive user). */
        TryHardenRoleCredential(credentialPath, allowInteractiveRead, logger);
        return password;
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
    private static async Task<int?> ReadComposeStatementTimeoutAsync(
        NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(
                "SELECT compose_statement_timeout_seconds FROM config.config_service WHERE id = 1", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
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
            logger.LogDebug(
                "config_service.compose_statement_timeout_seconds is not present on this store ({SqlState}) — provisioning the roles with the shipped {Seconds}s default",
                ex.SqlState, McpCommandDeadlines.ComposedQueryFallbackSeconds);

            return McpCommandDeadlines.ComposedQueryFallbackSeconds;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
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
        ProvisioningTarget? target = null)
    {
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
        const string mcp = DarlingManagedPostgres.McpRoleName;
        const string collect = PgSchemaGenerator.CollectSchema;
        const string config = PgSchemaGenerator.ConfigSchema;
        var marker = target.RoleMarker;

        /* #3914: the managed store is loopback-only and nothing but Darling's own logins connects to it, so it
           takes CONNECT away from PUBLIC. The compose store is one people read directly with roles of their own,
           and the same statement would silently take their CONNECT away, so there it is a comment instead. */
        var publicDatabaseRevoke = target.RevokePublicDatabaseAccess
            ? $"REVOKE ALL ON DATABASE {database} FROM PUBLIC;"
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

        /* The fail-closed viewer column-ACL carve for the secret-bearing config tables (see
           ViewerRestrictedConfigTables). Runs AFTER the blanket config GRANT below, so it strips
           viewer's table-wide SELECT on those tables and re-grants only the non-secret columns. */
        var viewerColumnAcl = BuildViewerColumnAclSql(config, viewer);

        /* The SAME fail-closed carve for the mcp role — it gets viewer's read surface, so it must be
           denied the identical secret columns. Reusing BuildViewerColumnAclSql(config, mcp) means the
           mcp carve can never drift from viewer's (Round 4 #5 guards this with a live denial test). */
        var mcpColumnAcl = BuildViewerColumnAclSql(config, mcp);

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

-- 1d. No bind parameters on those slow-statement lines (#3899): the mcp role also writes alert settings, whose
--     values include secrets. admin gets no line. Same non-migration reason as 1c.
{BuildSlowStatementParameterSql()}

-- 2. Schema usage + SELECT everywhere (ALL TABLES covers tables AND views). collect holds no secrets,
--    so admin+viewer read all of it. config: admin (the writer, and the Settings window's identity) reads
--    every column; viewer reads all config tables too — MINUS the secret columns carved below.
GRANT USAGE ON SCHEMA {collect}, {config} TO {admin}, {viewer};
GRANT SELECT ON ALL TABLES IN SCHEMA {collect} TO {admin}, {viewer};
GRANT SELECT ON ALL TABLES IN SCHEMA {config}  TO {admin}, {viewer};

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

-- 3. config writes -- admin gets the whole schema. (mcp gets ONLY a narrow config.analysis_muted INSERT
--    in section 6, never the config_command/monitored_servers/notification pivot tables; viewer: none.)
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {config} TO {admin};

-- 4. Default privileges so NEW tables/views auto-inherit (no per-table-grant foot-gun).
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {collect}
   GRANT SELECT ON TABLES TO {admin}, {viewer};
ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {config}
   GRANT SELECT ON TABLES TO {admin}, {viewer};
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
GRANT CONNECT ON DATABASE {database} TO {admin}, {viewer};

-- 6. The mcp role (darling-network-endpoints, D3-role): the ONLY credential reachable (via the bearer
--    token) from the optional network MCP surface, so it is deliberately NOT admin. It gets viewer's exact
--    READ surface (SELECT on collect + config-minus-secret-columns) as SEPARATE 'TO mcp' statements -- the
--    'TO admin, viewer' grant lines above are pinned verbatim by tests, so mcp is never appended to them --
--    PLUS exactly two narrow analysis INSERTs HERE: what analyze_server persists (collect.analysis_findings)
--    and what the mute tool writes (config.analysis_muted) -- no UPDATE/DELETE on EITHER analysis target (no
--    MCP unmute tool exists; DELETE/unmute is viewer/admin only). The mcp role ALSO gets the narrow
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
GRANT INSERT ON {config}.analysis_muted TO {mcp};
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
GRANT INSERT, UPDATE, DELETE ON {config}.custom_views TO {viewer};
GRANT INSERT, UPDATE, DELETE ON {config}.custom_views TO {mcp};
-- Custom alerting (#3285): the web editor (viewer) and the MCP rule tools (mcp) CRUD config.custom_alert_rules,
-- the same narrow single-table floor as custom_views (non-secret rule JSON -- no ViewerRestrictedConfigTables
-- carve, no config_command pivot). Created by V116, so provisioning runs after migration. NOTE: the sibling
-- config.custom_alert_state is written by the CustomAlertEvaluator on the OWNER pool only, so it deliberately
-- gets NO viewer/mcp grant here (add a viewer SELECT only when the editor surfaces a rule's firing status).
GRANT INSERT, UPDATE, DELETE ON {config}.custom_alert_rules TO {viewer};
GRANT INSERT, UPDATE, DELETE ON {config}.custom_alert_rules TO {mcp};
-- The Viewer's per-database database-state override editor (#1986) writes config.database_state_expected:
-- the same narrow single-table floor as custom_views. Created by V49, so provisioning runs after migration.
GRANT INSERT, UPDATE, DELETE ON {config}.database_state_expected TO {viewer};

-- 8. Alert tuning (the MCP alert-tuning write tools): the mcp role's alert-config writes, mirroring section 7's
--    custom_views grant model (EXPLICIT single-table statements, NO ALTER DEFAULT PRIVILEGES). update_alert_settings
--    / create_mute_rule / update_mute_rule / delete_mute_rule / set_mute_rule_enabled let a token-holder tune the SAME alert engine the Viewer's Settings
--    window drives: INSERT/UPDATE/DELETE on config_mute_rules (the mute rules the delivery paths honor) and UPDATE
--    on the SINGLETON config_alert_settings row (id=1 -- UPDATE only, never INSERT/DELETE: the row is a fixed
--    singleton the service seeds). Still NARROW -- never the config_command service-credential pivot, a
--    schema-wide config write, or any SECRET column: the one config_notification write is a single column
--    (see #3314 below), and the monitored-servers credential column stays SELECT-carved.
--    The beacon caveat: a config_alert_settings write fires the existing statement-level bump trigger
--    (trg_bump_alert_settings -> config_bump_version), which UPDATEs config_service.config_version AS THE CURRENT
--    ROLE (the trigger function is SECURITY INVOKER). So mcp ALSO needs UPDATE on JUST the two beacon columns of
--    config_service, or every update_alert_settings write would fail 42501 in production -- and the superuser-run
--    gated-live tests would never catch it (they connect as the owner). A COLUMN-level grant lets mcp bump the
--    reload beacon but NOT flip paused / capture_plans / mcp_enabled / mcp_port. The targets exist here because
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
GRANT INSERT, UPDATE, DELETE ON {config}.config_mute_rules TO {viewer};
GRANT UPDATE (config_version, updated_at) ON {config}.config_service TO {viewer};
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
-- #3598 (V131): notification routes. The parent row's posture is that a network token-holder never READS a
-- destination and never WRITES one either (the one config_notification write above is the cooldown column),
-- and a route IS a destination -- pointing a family's Slack at a URL of the caller's choosing would let mcp
-- redirect alert content (query text, server names) anywhere. So mcp gets exactly the two writes that move no
-- destination: DELETE (alerts fall back to the parent, which the operator configured) and UPDATE on the
-- enabled flag plus its modified_at stamp (set_notification_route_enabled / delete_notification_route), the
-- shape of set_mute_rule_enabled / delete_mute_rule. No INSERT, no destination column. The READ side is the
-- section-6 carve: route_id, metric_match, enabled, smtp_recipients and the GENERATED configured_channels
-- presence column, never the URLs. The BEACON is covered: trg_bump_notification_routes -> config_bump_version
-- (SECURITY INVOKER) UPDATEs config_service.config_version AS mcp, which the two-column grant above serves.
GRANT UPDATE (enabled, modified_at), DELETE ON {config}.config_notification_routes TO {mcp};

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
GRANT INSERT, UPDATE, DELETE ON {config}.config_monitored_servers TO {mcp};

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
--     search_path so no injected path can redirect the unqualified config_alert_log or now(), and a fully
--     parameterized INSERT with NO dynamic SQL. Created + REVOKEd-from-PUBLIC by the shared builder below;
--     EXECUTE is the only privilege the least-privilege roles get, and admin/owner keep their direct INSERT and
--     never call it. NOT a versioned migration: CREATE OR REPLACE is idempotent and owner-run every start and
--     has no probeable schema footprint (the section-1c role-SET rationale), so a V-number would drag in the
--     probe rung + ladder fixture + version-pin tests for no gain.
{BuildCustomAlertResolveFunctionSql(config)}
GRANT EXECUTE ON FUNCTION {config}.record_custom_alert_resolution(integer, text, text, text) TO {viewer}, {mcp};

-- 11. Role memberships (#3914): the three roles hold none. Nothing above grants one, so every membership in which
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
      WHERE m.rolname IN ('{admin}', '{viewer}', '{mcp}')
   LOOP
      EXECUTE format('REVOKE %I FROM %I', membership.granted, membership.member)
         || CASE WHEN membership.grantor IS NULL THEN '' ELSE format(' GRANTED BY %I', membership.grantor) END
         || ' CASCADE';
   END LOOP;
END $do$;
";
    }

    /// <summary>
    /// The <c>SECURITY DEFINER</c> function (#3334) that lets the least-privilege viewer/mcp roles write the ONE
    /// resolution row a caller-initiated custom-alert rule delete records in <c>config_alert_log</c> -- a table
    /// they may not INSERT directly -- WITHOUT a blanket INSERT grant that would let them fabricate arbitrary
    /// history. Returns the <c>CREATE OR REPLACE FUNCTION</c> plus the <c>REVOKE ALL ... FROM PUBLIC</c> (a
    /// freshly created function is EXECUTE-able by PUBLIC by default, so revoking is mandatory); the caller adds
    /// the narrow <c>GRANT EXECUTE</c>. Shared so the gated live proof test creates the IDENTICAL function rather
    /// than a drifting hand-copy. Definer-safe by construction: owned by whoever runs it (the provisioning owner,
    /// which holds the config_alert_log INSERT the body needs), an explicit <c>SET search_path = {config},
    /// pg_catalog</c> so neither the unqualified table nor <c>now()</c> can be redirected by a caller's
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
SET search_path = {config}, pg_catalog
AS $fn$
   INSERT INTO config_alert_log
      (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
       alert_sent, notification_type, send_error, muted, detail_text, context_json)
   VALUES
      ((now() AT TIME ZONE 'UTC'), p_server_id, p_server_name, p_metric_name, 0, 0,
       false, 'none', NULL, false, p_detail_text, NULL);
$fn$;
REVOKE ALL ON FUNCTION {config}.record_custom_alert_resolution(integer, text, text, text) FROM PUBLIC;";

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
    private static async Task<Dictionary<string, string?>> ReadStoredRoleSecretsAsync(
        NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        var stored = new Dictionary<string, string?>(StringComparer.Ordinal);
        try
        {
            await using var command = new NpgsqlCommand(
                "SELECT rolname::text, rolpassword FROM pg_catalog.pg_authid WHERE rolname = ANY($1)", connection) { CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };
            command.Parameters.AddWithValue(new[] { DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                stored[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
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
        IReadOnlyDictionary<string, string?> stored, string adminPassword, string viewerPassword, string mcpPassword)
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
        command.Parameters.AddWithValue(new[] { DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName });

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
    /// An untrusted file is discarded rather than read — the Unix equivalent of <see cref="EnsureRoleCredential"/>'s
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
                        /* #4004 review, round 3: a directory this process found open and could not empty stays refused
                           to every later caller this start, though it now reads owner-only, so "nothing in it is used this
                           start" holds for all of them. The next start trusts it: only a directory can be left, and no
                           reader takes one for a credential. */
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
    /// (#4004 review, round 2): the three role passwords and the log-hash key, under both platforms' names.
    /// </summary>
    internal static IReadOnlyList<string> CredentialDirectoryFileNames { get; } = BuildCredentialDirectoryFileNames();

    private static string[] BuildCredentialDirectoryFileNames()
    {
        var names = new List<string>
        {
            ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName),
            ComposeStoreCredentialFileName(DarlingManagedPostgres.ViewerRoleName),
            ComposeStoreCredentialFileName(DarlingManagedPostgres.McpRoleName),
            DarlingLogHashKeyFile.UnixFileName,
            DarlingLogHashKeyFile.WindowsFileName,
        };
        return names.SelectMany(name => new[] { name, name + ".tmp" }).ToArray();
    }

    /// <summary>
    /// The directory was open to other users until this call set it owner-only (#4004 review, round 2), so any file
    /// in it could have been planted, and the file check cannot tell (it cannot see a Unix owner). Every entry at a
    /// name the service reads is removed NOW, before anything reads it, so the discard does not depend on this start
    /// living long enough to reach whichever step would have replaced that file: the chmod is permanent, and the
    /// next start, finding the directory owner-only, trusts what is left. Removed, the directory holds only what the
    /// service writes from here on, so it is trusted.
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
        foreach (var name in CredentialDirectoryFileNames)
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
                "The credentials directory {Directory} was open to other users until now ({Reason}), so its files could have been planted: {Files} discarded (#4004). Every role password removed is generated again and re-asserted on its role, and a removed log-hash key is generated again, which gives every log event stored from now on a new identity. Keep the directory owner-only (on Kubernetes, an fsGroup re-applied on every mount opens it again) so this does not repeat, and run one service per credentials volume (on Kubernetes, one replica with strategy: Recreate): a second one starting beside this one could read the directory between this start's chmod and this removal.",
                directory, wasOpen, string.Join(", ", removed));
        }

        if (left.Count > 0)
        {
            var reason = $"{wasOpen}, and {(left.Count == 1 ? "this entry at a credential's name" : "these entries at credentials' names")} could not be removed, so nothing in it is used this start: "
                + $"{string.Join("; ", left)}. Every other credential file there is removed and the directory stays owner-only, so remove {(left.Count == 1 ? "it" : "each of them")} and restart";
            logger.LogError("The credentials directory {Directory} is not trusted ({Reason}) (#4004).", directory, reason);
            guard.RecordLeftBehind(directory, reason);
            return new ComposeCredentialDirectoryTrust(reason, MayWrite: false);
        }

        guard.RecordLeftBehind(directory, null);
        return new ComposeCredentialDirectoryTrust(null, MayWrite: true);
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

    /// <summary>Every role: the default, and what a store without stored verifiers gets.</summary>
    All = Admin | Viewer | Mcp,
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
    private ComposeStoreProvisioning(bool provisioned, int appliedSeconds, string? viewerPassword, string? mcpPassword, string? refusalReason)
    {
        Provisioned = provisioned;
        AppliedComposeStatementTimeoutSeconds = appliedSeconds;
        ViewerPassword = viewerPassword;
        McpPassword = mcpPassword;
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

    /// <summary>Why nothing was provisioned; null when provisioned.</summary>
    public string? RefusalReason { get; }

    internal static ComposeStoreProvisioning Succeeded(int appliedSeconds, string viewerPassword, string mcpPassword) =>
        new(true, appliedSeconds, viewerPassword, mcpPassword, null);

    internal static ComposeStoreProvisioning Refused(string reason) =>
        new(false, DarlingManagedRoles.ComposeStatementTimeoutUnknown, null, null, reason);
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
}

/// <summary>The platform's own <see cref="IUnixDirectoryModes"/>, on every platform but Windows.</summary>
[UnsupportedOSPlatform("windows")]
internal sealed class PlatformUnixDirectoryModes : IUnixDirectoryModes
{
    public void CreateOwnerOnly(string directory) => Directory.CreateDirectory(directory, DarlingManagedRoles.OwnerOnlyDirectory);

    public UnixFileMode Get(string directory) => File.GetUnixFileMode(directory);

    public void Set(string directory, UnixFileMode mode) => File.SetUnixFileMode(directory, mode);
}
