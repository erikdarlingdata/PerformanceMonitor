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
/// <item><b><c>viewer</c></b> — SELECT on both schemas, and (the single write exception, #1563)
/// INSERT/UPDATE/DELETE on ONLY <c>config.custom_views</c> (the web dashboard's user-authored view
/// definitions — non-secret JSON; editing is any AUTHENTICATED seat, the web surface's normal networked
/// mode gated server-side by the host's token+CIDR auth — NOT loopback-only). No other writes anywhere. A
/// locked-down deployment points the Viewer at this ("look but don't touch" — plus its own saved custom
/// views).</item>
/// <item><b><c>mcp</c></b> — the (optionally network-exposed) MCP host's store identity
/// (darling-network-endpoints, D3-role): the SAME read surface as <c>viewer</c> (SELECT on
/// <c>collect</c> + <c>config</c>-minus-the-secret-columns) PLUS a NARROW, enumerated set of writes —
/// INSERT on <c>collect.analysis_findings</c> and <c>config.analysis_muted</c> (what <c>analyze_server</c>
/// persists + the <c>mute</c> tool need), INSERT/UPDATE/DELETE on <c>config.custom_views</c> (the
/// custom-view tools, #1599), the alert-tuning writes (INSERT/UPDATE/DELETE on
/// <c>config.config_mute_rules</c> + UPDATE on the singleton <c>config.config_alert_settings</c>, plus the
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
/// <para>Windows-only (the DPAPI credential files), like every DPAPI surface here. Managed mode provisions
/// all three roles (admin/viewer/mcp); bring-your-own Postgres provisions admin + viewer out-of-band via
/// <c>Darling/tools/provision-roles.sql</c> and correctly has NO <c>mcp</c> role — the network MCP endpoint
/// is managed-mode-only, so a BYO operator's own PostgreSQL governs any MCP-role exposure it wants.</para>
/// </summary>
[SupportedOSPlatform("windows")]
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
            },
            SecretColumns: new[] { "encrypted_password" }),

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

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        /* #2357: read the live knob rather than a constant. Ordering is what makes this safe -- migrations
           run before provisioning at startup, so the column exists by now -- and because this DDL is re-run
           on every managed start, a changed value reaches an existing install on its next restart without
           any new machinery. A store whose config row is not seeded yet answers with the default. */
        var composeTimeoutSeconds = await ReadComposeStatementTimeoutAsync(connection, cancellationToken);

        await using var command = new NpgsqlCommand(
            BuildProvisioningSql(adminPassword, viewerPassword, mcpPassword, composeTimeoutSeconds), connection);
        await command.ExecuteNonQueryAsync(cancellationToken);

        logger.LogInformation(
            "Least-privilege roles ready (admin: read both schemas + write config; viewer: read-only + write config.custom_views; mcp: viewer's reads + INSERT on analysis_findings/analysis_muted + write config.custom_views + tune alerting (config_mute_rules, config_alert_settings, config_service reload beacon) + onboard servers (config_monitored_servers)) — the Viewer and MCP host no longer connect as the superuser");

        /* CLAMPED, not raw: the batch above wrote the clamped form, so returning the raw read would hand the
           caller a baseline that differs from what the roles actually carry (a stored 0 provisions '15s'). */
        return StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeTimeoutSeconds);
    }

    /// <summary>
    /// The <c>statement_timeout</c> backstop on the two composed-query identities, as SQL. The SINGLE
    /// renderer for those statements — <see cref="BuildProvisioningSql"/> embeds it at startup and
    /// <see cref="ReassertComposeStatementTimeoutAsync"/> runs it alone on a control-plane reload (#2918),
    /// so the two paths cannot disagree about the ceiling.
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

        return $@"ALTER ROLE {viewer} SET statement_timeout = '{statementTimeout}';
ALTER ROLE {mcp}    SET statement_timeout = '{statementTimeout}';";
    }

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
    /// Managed mode only. A BYO store provisions these roles out-of-band via
    /// <c>tools/provision-roles.sql</c> and names them itself, so <c>ALTER ROLE viewer</c> would be guessing
    /// at an identity we do not own.
    /// </param>
    /// <param name="isWindows">
    /// Mirrors startup provisioning's own gate. Not because the SQL needs Windows — it does not — but
    /// because provisioning is where these roles get CREATED, so off-Windows they may not exist at all and
    /// this would fail every reload.
    /// </param>
    public static bool ShouldReassertComposeStatementTimeout(
        int storeSeconds, int appliedSeconds, bool managedStore, bool isWindows) =>
        managedStore && isWindows && storeSeconds != appliedSeconds;

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
                BuildComposeStatementTimeoutSql(composeStatementTimeoutSeconds), connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            logger.LogInformation(
                "Compose statement_timeout re-asserted on the viewer/mcp roles at {Seconds}s — takes effect on each role's next session (an already-connected viewer keeps the old ceiling until it reconnects)",
                StoreConfigProvider.ClampComposeStatementTimeoutSeconds(composeStatementTimeoutSeconds));
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
    private static void TryHardenRoleCredential(string path, bool allowInteractiveRead, ILogger logger)
    {
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead);
        }
        catch (Exception ex)
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
    /// <para>Defensive on purpose. This runs during startup provisioning, before the config row is
    /// necessarily seeded and on stores that may predate the column, and a role-provisioning step that threw
    /// over a tuning knob would stop the service from starting over something that has a perfectly good
    /// default.</para>
    /// </summary>
    private static async Task<int> ReadComposeStatementTimeoutAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(
                "SELECT compose_statement_timeout_seconds FROM config.config_service WHERE id = 1", connection);
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return value is int seconds ? seconds : 15;
        }
        catch (Exception) 
        {
            return 15;
        }
    }

    /// <summary>
    /// The idempotent, self-healing provisioning DDL with the role passwords injected. Passwords are
    /// alnum-only (<see cref="DarlingManagedPostgres.GeneratePassword"/>), verified here before the
    /// interpolation, so string-building the <c>PASSWORD '…'</c> literals is escaping-safe — the same
    /// reasoning <see cref="DarlingManagedPostgres"/> relies on for <c>--pwfile</c>. Public + shape-pinnable
    /// so a test can assert it without a live Postgres. The <c>mcp</c>-role INSERT grants reference
    /// <c>collect.analysis_findings</c> / <c>config.analysis_muted</c> by qualified name, so the one-shot
    /// batch requires those tables to already exist — safe because provisioning runs AFTER migration (see
    /// <see cref="EnsureProvisionedAsync"/>); a dropped/recreated table re-grants on the next start.
    /// </summary>
    /// <param name="composeStatementTimeoutSeconds">
    /// The per-session <c>statement_timeout</c> for the viewer and mcp roles (#2357). Defaults to the 15 the
    /// constant used to hard-code, so a caller that does not care gets today's behaviour exactly.
    /// </param>
    public static string BuildProvisioningSql(
        string adminPassword, string viewerPassword, string mcpPassword, int composeStatementTimeoutSeconds = 15)
    {
        RequireAlphanumeric(adminPassword, nameof(adminPassword));
        RequireAlphanumeric(viewerPassword, nameof(viewerPassword));
        RequireAlphanumeric(mcpPassword, nameof(mcpPassword));

        const string owner = DarlingManagedPostgres.UserName;      // darling (owner/superuser)
        const string database = DarlingManagedPostgres.DatabaseName; // darling
        const string admin = DarlingManagedPostgres.AdminRoleName;
        const string viewer = DarlingManagedPostgres.ViewerRoleName;
        const string mcp = DarlingManagedPostgres.McpRoleName;
        const string collect = PgSchemaGenerator.CollectSchema;
        const string config = PgSchemaGenerator.ConfigSchema;
        const string marker = RoleMarker;
        /* #2357: was ComposeLimits.StatementTimeout, a bare "15s". Rendered by the SHARED builder rather
           than inline, because #2918 made the reload path re-assert the same two statements: two renderers
           for one pair of ALTER ROLEs is a drift waiting to happen, and the drift would be invisible (both
           sides run, the roles just disagree about the ceiling depending on which path touched them last). */
        var composeTimeoutStatements = BuildComposeStatementTimeoutSql(composeStatementTimeoutSeconds);

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
   re-run every managed start, converging role state to the DPAPI credential files. */

-- 1. Roles (CREATE ROLE has no IF NOT EXISTS -> guard with a DO block). The names are bare
--    admin/viewer, so a fresh role is STAMPED with a marker comment and an existing SAME-NAMED role
--    is trusted only if it carries that marker; an unmarked collision fails loud (never repurposed).
DO $do$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{admin}') THEN
      CREATE ROLE {admin} LOGIN NOSUPERUSER PASSWORD '{adminPassword}';
      COMMENT ON ROLE {admin} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{admin}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{admin}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;

   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{viewer}') THEN
      CREATE ROLE {viewer} LOGIN NOSUPERUSER PASSWORD '{viewerPassword}';
      COMMENT ON ROLE {viewer} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{viewer}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{viewer}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;

   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{mcp}') THEN
      CREATE ROLE {mcp} LOGIN NOSUPERUSER PASSWORD '{mcpPassword}';
      COMMENT ON ROLE {mcp} IS '{marker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{mcp}'), 'pg_authid') IS DISTINCT FROM '{marker}' THEN
      RAISE EXCEPTION 'Role ""{mcp}"" already exists and was not created by Darling (missing the ''{marker}'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
END $do$;

-- 1b. Re-assert password + attributes every start (the credential file is the source of truth).
--     Only reached when the guard above passed (fresh + marked, or already Darling-marked).
ALTER ROLE {admin}  LOGIN NOSUPERUSER PASSWORD '{adminPassword}';
ALTER ROLE {viewer} LOGIN NOSUPERUSER PASSWORD '{viewerPassword}';
ALTER ROLE {mcp}    LOGIN NOSUPERUSER PASSWORD '{mcpPassword}';

-- 1c. statement_timeout backstop on the composed-query identities (Custom Views v2, #1563). viewer is the web
--     dashboard's DB identity and mcp the optional network MCP identity; both serve the network-reachable
--     compose surface over a raw, no-rollup store, so a runaway aggregation can NEVER pin the store beyond this
--     (a LIMIT bounds output, not work). A role SET applies to every future session and re-asserts each start.
--     admin (the Settings writer, small config writes) is deliberately NOT bounded. NOT a versioned migration:
--     a role statement_timeout has no probeable schema footprint, so tying it to StorageVersion would break the
--     viewer's connect-time version gate.
{composeTimeoutStatements}

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
REVOKE ALL ON DATABASE {database} FROM PUBLIC;
GRANT CONNECT ON DATABASE {database} TO {admin}, {viewer};

-- 6. The mcp role (darling-network-endpoints, D3-role): the ONLY credential reachable (via the bearer
--    token) from the optional network MCP surface, so it is deliberately NOT admin. It gets viewer's exact
--    READ surface (SELECT on collect + config-minus-secret-columns) as SEPARATE 'TO mcp' statements -- the
--    'TO admin, viewer' grant lines above are pinned verbatim by tests, so mcp is never appended to them --
--    PLUS exactly two narrow analysis INSERTs HERE: what analyze_server persists (collect.analysis_findings)
--    and what the mute tool writes (config.analysis_muted) -- no UPDATE/DELETE on EITHER analysis target (no
--    MCP unmute tool exists; DELETE/unmute is viewer/admin only). The mcp role ALSO gets the narrow
--    config.custom_views write in section 7 (for the MCP custom-view tools) -- likewise a single non-secret
--    config table, never the config_command pivot or the carved secret columns. EXPLICIT single-table grants
--    with NO ALTER DEFAULT PRIVILEGES: ADP has no per-table
--    form, so an ADP INSERT would broaden mcp to ALL of collect -- provisioning re-runs every start, so a
--    recreated table re-grants (self-heal) without ADP. The two INSERT targets must already exist here, so
--    provisioning runs AFTER migration (EnsureProvisionedAsync).
GRANT USAGE ON SCHEMA {collect}, {config} TO {mcp};
GRANT SELECT ON ALL TABLES IN SCHEMA {collect} TO {mcp};
GRANT SELECT ON ALL TABLES IN SCHEMA {config}  TO {mcp};
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
-- The Viewer's per-database database-state override editor (#1986) writes config.database_state_expected:
-- the same narrow single-table floor as custom_views. Created by V49, so provisioning runs after migration.
GRANT INSERT, UPDATE, DELETE ON {config}.database_state_expected TO {viewer};

-- 8. Alert tuning (the MCP alert-tuning write tools): the mcp role's alert-config writes, mirroring section 7's
--    custom_views grant model (EXPLICIT single-table statements, NO ALTER DEFAULT PRIVILEGES). update_alert_settings
--    / create_mute_rule / delete_mute_rule let a token-holder tune the SAME alert engine the Viewer's Settings
--    window drives: INSERT/UPDATE/DELETE on config_mute_rules (the mute rules the delivery paths honor) and UPDATE
--    on the SINGLETON config_alert_settings row (id=1 -- UPDATE only, never INSERT/DELETE: the row is a fixed
--    singleton the service seeds). Still NARROW -- never the config_command service-credential pivot, the
--    monitored-servers/notification secret tables, or a schema-wide config write.
--    The beacon caveat: a config_alert_settings write fires the existing statement-level bump trigger
--    (trg_bump_alert_settings -> config_bump_version), which UPDATEs config_service.config_version AS THE CURRENT
--    ROLE (the trigger function is SECURITY INVOKER). So mcp ALSO needs UPDATE on JUST the two beacon columns of
--    config_service, or every update_alert_settings write would fail 42501 in production -- and the superuser-run
--    gated-live tests would never catch it (they connect as the owner). A COLUMN-level grant lets mcp bump the
--    reload beacon but NOT flip paused / capture_plans / mcp_enabled / mcp_port. The targets exist here because
--    provisioning runs AFTER migration; a recreated table re-grants on the next start (self-heal).
GRANT INSERT, UPDATE, DELETE ON {config}.config_mute_rules TO {mcp};
GRANT UPDATE ON {config}.config_alert_settings TO {mcp};
GRANT UPDATE (config_version, updated_at) ON {config}.config_service TO {mcp};

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
";
    }

    /// <summary>
    /// The generated passwords are alnum by construction; this fails closed if that ever changes,
    /// because the passwords are string-interpolated into DDL literals (belt: <c>quote_literal</c> if
    /// the alphabet is ever widened).
    /// </summary>
    private static void RequireAlphanumeric(string password, string parameterName)
    {
        if (string.IsNullOrEmpty(password))
        {
            throw new ArgumentException("Role password must not be empty.", parameterName);
        }

        foreach (var c in password)
        {
            if (!char.IsLetterOrDigit(c) || c > 127)
            {
                throw new ArgumentException(
                    "Role password must be ASCII alphanumeric (it is interpolated into DDL); use DarlingManagedPostgres.GeneratePassword.",
                    parameterName);
            }
        }
    }
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
