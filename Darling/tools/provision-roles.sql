-- ============================================================================================
-- Darling security hardening (#1262) — least-privilege role provisioning for BRING-YOUR-OWN
-- PostgreSQL (postgres.managed = false).
--
-- In managed mode the service provisions these roles automatically on every start (see
-- DarlingManagedRoles) and generates their DPAPI credentials. The Linux compose distribution does the
-- same on the store its compose file creates (#3914): there the service logs in as the store's
-- bootstrap superuser and provisions all three roles itself, so do NOT run this script against the
-- compose store. On any other store YOU run this script once, as the database OWNER (the role that
-- owns the Darling store — the one your postgres.connectionString connects as for collection). It
-- creates three least-privilege login roles:
--
--   admin   -- reads both schemas + writes the operator-config tables (mute rules, alert
--              dismissals, analysis mutes). The Viewer's default identity (darling.json
--              postgres.connectAs = "admin").
--   viewer  -- reads both schemas; its writes are the narrow, enumerated web-surface set: INSERT/UPDATE/DELETE
--              on config.custom_views (the user-authored view definitions, #1563), on
--              config.custom_alert_rules (the user-authored alert rules, #3285), on
--              config.database_state_expected (the per-database override editor, #1986), and on
--              config.config_mute_rules (the web dashboard's dedicated mute-rule endpoints, #3450 -- plus the
--              two config_service beacon columns their bump trigger writes as the caller). All non-secret
--              tables; over the web every write is gated server-side by the host's auth + seat model -- these
--              grants are only the floor beneath that gate. All other write actions degrade gracefully. The
--              web dashboard's identity, and a locked-down Viewer's (postgres.connectAs = "viewer").
--   mcp     -- the MCP server's identity: viewer's reads (the same secret-column carve), plus the MCP tools'
--              narrow writes -- analyze_server's findings, finding mutes, custom views, custom alert rules,
--              alert tuning (mute rules, the alert settings row, the delivery cooldown column, a notification
--              route's enabled flag or its removal) and server onboarding (config_monitored_servers, whose
--              credential column it can write but never read). Never admin: a network token-holder must not
--              reach the config_command service-credential pivot or any secret column.
--
-- WHO CONNECTS AS WHAT (#3914). The service collects as the owner. The web dashboard and the MCP server
-- connect with postgres.webConnectionString and postgres.mcpConnectionString in darling.json: point
-- them at the viewer and mcp roles this script creates, for example
--
--     "webConnectionString": "Host=db.example.com;Port=5432;Username=viewer;Password=<viewer password>;Database=darling"
--
-- or as "file:/path" / "env:NAME" references, like connectionString, to keep the password out of
-- darling.json. Left unset, that surface connects as the OWNER, and nothing this script sets up applies
-- to it: not the secret-column carve (steps 2b and 2c), not the narrow write grants (steps 3b-3f), not
-- the statement_timeout backstop (step 1a). The service says so in a warning at startup.
--
-- PREREQUISITE: the V8 migration (which the service applies on startup) must already have created
-- the collect and config schemas and moved the tables into them. Run this AFTER the service has
-- started at least once against your store. Re-running after a later schema upgrade is safe and
-- idempotent: the GRANT ... ON ALL TABLES statements below re-cover every table that now exists
-- (including the V17 control-plane tables config_monitored_servers / config_alert_settings /
-- config_notification / config_collector_schedules / config_service / config_command), and the
-- ALTER DEFAULT PRIVILEGES already auto-grant any table the owner creates after this runs (config
-- tables to admin and viewer; collect tables to all three). The writes that are NOT covered by
-- re-running blindly are the single-table grants in steps 3b-3f: each names a table (or, for the
-- beacon columns, a trigger dependency) a specific migration creates -- custom_views is V31,
-- database_state_expected is V49, custom_alert_rules is V116, the mute-rule reload-beacon trigger is
-- V117 and config_notification_routes is V131 -- so re-run this script after upgrading past each.
--
-- BEFORE RUNNING:
--   1. Replace CHANGE_ME_ADMIN_PASSWORD, CHANGE_ME_VIEWER_PASSWORD and CHANGE_ME_MCP_PASSWORD with strong
--      passwords. Better, keep the passwords out of every statement (#3910): replace each literal with a
--      SCRAM-SHA-256 verifier, which PostgreSQL stores as-is, or set the passwords afterwards with psql's
--      \password admin, \password viewer and \password mcp, which compute the verifier locally and send
--      only that. A password literal is kept verbatim by anything that records statement text:
--      log_statement, a failed statement's STATEMENT line in the server log, auto_explain, and
--      pg_stat_statements with utility tracking on.
--   2. If your database is not named "darling", change it in the REVOKE/GRANT ... ON DATABASE
--      lines and in the ALTER DATABASE note at the bottom.
--   3. If the owner role is not "darling", change it in the ALTER DEFAULT PRIVILEGES FOR ROLE
--      lines (it must be the role that CREATEs the tables — your collection connection's role).
--
--   psql -h <host> -U <owner> -d darling -f provision-roles.sql
--
-- NAME-COLLISION SAFETY: the roles are the bare, un-prefixed names "admin", "viewer" and "mcp". If your
-- cluster ALREADY has a role by any of those names that this script did not create, it will NOT be
-- silently repurposed: fresh roles are stamped with a marker comment ('darling-managed'), and an existing
-- same-named role without that marker makes this script FAIL LOUD (rename or drop the other role first,
-- or use a dedicated cluster/database for the Darling store). That includes the roles the service
-- provisions on the compose store, which carry 'darling-compose' instead.
-- ============================================================================================

-- 0. Keep this script's own role DDL out of pg_stat_statements (#3899). Step 1 sends each password as an
--    ALTER ROLE ... PASSWORD literal, and while pg_stat_statements is loaded with utility tracking on (the
--    module's default), it records that statement VERBATIM, password included, where any superuser or
--    pg_read_all_stats member (a monitoring agent's pg_monitor login, say) can read it. This covers this
--    session only; step 1b says how to turn it off for good. Harmless when the module is not loaded, and
--    superuser-only when it is, like the rest of this script.
SET pg_stat_statements.track_utility = off;

-- 1. Roles (CREATE ROLE has no IF NOT EXISTS -> guard with a DO block). Idempotent: re-running
--    this script re-asserts the password below, so it doubles as a password rotation. A fresh role
--    is stamped 'darling-managed'; an unmarked same-named role fails loud (never repurposed).
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
      CREATE ROLE admin LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_ADMIN_PASSWORD';
      COMMENT ON ROLE admin IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'admin'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "admin" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'viewer') THEN
      CREATE ROLE viewer LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_VIEWER_PASSWORD';
      COMMENT ON ROLE viewer IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'viewer'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "viewer" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp') THEN
      CREATE ROLE mcp LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_MCP_PASSWORD';
      COMMENT ON ROLE mcp IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'mcp'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "mcp" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
END $$;

ALTER ROLE admin  LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_ADMIN_PASSWORD';
ALTER ROLE viewer LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_VIEWER_PASSWORD';
ALTER ROLE mcp    LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_MCP_PASSWORD';

-- 1a. statement_timeout backstop on viewer and mcp (Custom Views v2, #1563): the web dashboard's compose surface
--     and the MCP custom-view tools run network-reachable aggregations over a raw, no-rollup store, so this caps
--     a runaway query at the database (a LIMIT bounds output, not work). Keep this value in step with
--     ComposeLimits.StatementTimeout in the service. It bounds the web dashboard and the MCP server only when
--     postgres.webConnectionString / postgres.mcpConnectionString name these roles (#3914); a surface left on
--     the owner login has no backstop at all.
ALTER ROLE viewer SET statement_timeout = '60s';
ALTER ROLE mcp    SET statement_timeout = '60s';

-- 1b. Slow-statement logging on viewer and mcp (#3899): a statement from either that runs past a third of its
--     statement_timeout, capped at 5s so raising the ceiling never widens the unlogged band (#4442), is written
--     to the server log with its text, so a slow read can be named instead
--     of guessed at, and the service's store-log sweep keeps it (literals masked). Bind parameters are not
--     logged: mcp also writes the alert settings, whose values include secrets. Managed mode derives the same
--     line from its own ceiling (a third of it): keep this in step if you change 1a. Both settings are
--     superuser-only.
--
--     WHO this covers: the viewer role is the web dashboard's identity when postgres.webConnectionString
--     names it, and a Darling Viewer seat's (postgres.connectAs = "viewer"); the mcp role is the MCP server's
--     when postgres.mcpConnectionString names it. A surface whose setting is unset connects as the owner, so
--     to log ITS slow statements set the same two lines on the owner role (ALTER ROLE darling SET ...),
--     which also logs the collectors' slow statements.
--
--     Per-statement timings (get_store_query_stats), as a superuser:
--       a. In postgresql.conf, add pg_stat_statements to shared_preload_libraries (keep every library
--          already there, timescaledb included) AND set pg_stat_statements.track_utility = off, then restart.
--          track_utility = off is a security setting, not tuning: with it on, the module records every
--          utility statement verbatim, this script's ALTER ROLE ... PASSWORD literals included (step 0 covers
--          only this script's session). With ALTER SYSTEM instead, pass ONE quoted literal per library:
--          ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_stat_statements';
--          a single literal holding the whole list is stored as one library name, and the server will not
--          start.
--       b. CREATE EXTENSION pg_stat_statements;  in this database.
--       c. Optional, and a trade-off: if the owner role is not a superuser, other roles' statement text reads
--          <insufficient privilege> in the service's reader. GRANT pg_read_all_stats TO darling; shows it, but
--          also lets that login read every session's query text and every database's statements on the
--          cluster, and a web dashboard or MCP server left on the owner login (no webConnectionString /
--          mcpConnectionString) connects as it. On a cluster shared with other applications, leave it out.
--     The service builds its reader functions at its next start, and then within the hour.
ALTER ROLE viewer SET log_min_duration_statement = '5000ms';
ALTER ROLE mcp    SET log_min_duration_statement = '5000ms';
ALTER ROLE viewer SET log_parameter_max_length = 0;
ALTER ROLE mcp    SET log_parameter_max_length = 0;

-- 2. Schema usage + SELECT on everything that exists now (ALL TABLES covers tables AND views). collect
--    holds no secrets, so admin+viewer read all of it. config: admin (the writer, and the Settings
--    window's identity) reads every column; viewer reads all config tables too -- MINUS the secret columns
--    carved in step 2b. mcp gets viewer's read surface in its own statements, carved the same way.
GRANT USAGE ON SCHEMA collect, config TO admin, viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer;
GRANT USAGE ON SCHEMA collect, config TO mcp;
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO mcp;
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO mcp;

-- 2b. Credential-column fail-closed ACLs (#1262). The read-only viewer must NOT read the secret columns of
--     the three credential-bearing config tables: config_monitored_servers.encrypted_password (a DPAPI
--     password blob), config_command.args_json (the inline test_connect credential blob), and
--     config_notification's SMTP password/username + the Teams/Slack/generic webhook URLs and the generic
--     channel's headers (a webhook URL is a bearer secret, and generic_headers carries the Authorization
--     token itself). Instead of GRANT-ALL-then-REVOKE-each-secret (fail-OPEN -- a future secret column leaks until
--     someone revokes it), DROP viewer's table-wide SELECT on each and re-grant ONLY the non-secret columns
--     (fail-CLOSED -- a column added later is invisible to viewer until you add it below). admin keeps its
--     table-wide SELECT from step 2. Re-running this whole script re-asserts the carve idempotently. If a
--     later schema upgrade ADDS a column to any of these three tables, add it to the matching GRANT list
--     below (or, if it is itself secret, deliberately leave it out).
--
--     SOURCE OF TRUTH: DarlingManagedRoles.ViewerRestrictedConfigTables (the C# list managed mode builds its
--     identical carve from). These GRANT lists are a HAND MIRROR of it, and hand mirrors drift -- they
--     did, silently, for three releases (see #1639). The ungated ProvisionRolesAclDriftTests parses THIS FILE
--     and asserts set-equality against that C# list, for viewer and for mcp, so adding a column to one side
--     without the other now fails the build. Keep the columns in the C# list's order so the two read as the
--     same list.
REVOKE SELECT ON config.config_monitored_servers FROM viewer;
GRANT SELECT (server_id, name, host, database, auth, username, encrypt_mode, trust_server_certificate,
              read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans,
              is_enabled, created_at, modified_at, alert_delivery_mode_override,
              -- V68: engine + port. Non-secret, exactly like host.
              engine, port,
              -- V107 (#2138): the force-plan bot's per-server arm state. Non-secret, exactly like is_enabled.
              plan_force_bot_enabled,
              -- V113 (#2138 phase 1): the remediation credential's login name. Non-secret, exactly like
              -- username; remediation_encrypted_password is deliberately NOT granted.
              remediation_username)
    ON config.config_monitored_servers TO viewer;
REVOKE SELECT ON config.config_command FROM viewer;
GRANT SELECT (command_id, created_at, requested_by, command_type, target_server_id, status, claimed_at,
              completed_at, result_status, result_json, service_instance)
    ON config.config_command TO viewer;
REVOKE SELECT ON config.config_notification FROM viewer;
GRANT SELECT (id, smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients,
              email_cooldown_minutes, teams_proxy, slack_proxy, modified_at,
              generic_body_template, generic_proxy, pagerduty_use_eu_region, pagerduty_proxy)
    ON config.config_notification TO viewer;
-- V131 (#3598): the sparse notification-routes table mirrors the parent row's destination columns under the
-- same names, so the same carve applies: the webhook URLs and the PagerDuty routing key are bearer secrets and
-- are deliberately NOT granted. configured_channels is a GENERATED presence column (which channels a route
-- sets, never their values) that exists so this carve can still answer "what does route 3 configure".
-- Created by V131, so re-run this script AFTER the service has migrated your store to V131.
REVOKE SELECT ON config.config_notification_routes FROM viewer;
GRANT SELECT (route_id, metric_match, smtp_recipients, configured_channels, enabled, modified_at)
    ON config.config_notification_routes TO viewer;

-- 2c. The SAME carve for mcp (#3914): it reads exactly what viewer reads, so it is denied exactly the same
--     secret columns -- the column lists are identical to 2b's, and the drift test holds both to the C# list.
REVOKE SELECT ON config.config_monitored_servers FROM mcp;
GRANT SELECT (server_id, name, host, database, auth, username, encrypt_mode, trust_server_certificate,
              read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans,
              is_enabled, created_at, modified_at, alert_delivery_mode_override,
              engine, port,
              plan_force_bot_enabled,
              remediation_username)
    ON config.config_monitored_servers TO mcp;
REVOKE SELECT ON config.config_command FROM mcp;
GRANT SELECT (command_id, created_at, requested_by, command_type, target_server_id, status, claimed_at,
              completed_at, result_status, result_json, service_instance)
    ON config.config_command TO mcp;
REVOKE SELECT ON config.config_notification FROM mcp;
GRANT SELECT (id, smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients,
              email_cooldown_minutes, teams_proxy, slack_proxy, modified_at,
              generic_body_template, generic_proxy, pagerduty_use_eu_region, pagerduty_proxy)
    ON config.config_notification TO mcp;
REVOKE SELECT ON config.config_notification_routes FROM mcp;
GRANT SELECT (route_id, metric_match, smtp_recipients, configured_channels, enabled, modified_at)
    ON config.config_notification_routes TO mcp;

-- 3. config writes -- admin gets the whole schema.
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;

-- 3b. Custom views (#1563): the web dashboard's custom-view composer writes exactly this one config table
--     (non-secret dashboard JSON) as viewer, and the MCP custom-view tools as mcp. Editing is any
--     authenticated seat -- the web surface's normal networked mode, gated server-side by the host's
--     token+CIDR auth (not loopback-only); this narrow grant is only the floor beneath that gate. EXPLICIT
--     single-table statements, NO ALTER DEFAULT PRIVILEGES (ADP has no per-table
--     form -> it would broaden the role to ALL of config). config.custom_views is created by the V31 migration,
--     so re-run this script AFTER the service has migrated your store to V31 (else this line errors: no table).
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO viewer;
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO mcp;

-- 3c. Database-state expected states (#1986): the viewer's per-database override editor writes this one
--     config table (non-secret; the expected/(ignore) state per database). SELECT is already covered by
--     the blanket config grant in step 2; this is the write floor for the editor, gated server-side by the
--     same token+CIDR auth as the custom-view composer. EXPLICIT single-table statement (no ALTER DEFAULT
--     PRIVILEGES, which would broaden viewer to ALL of config). config.database_state_expected is created by
--     the V49 migration, so re-run this script AFTER the service has migrated your store to V49.
GRANT INSERT, UPDATE, DELETE ON config.database_state_expected TO viewer;

-- 3d. Mute rules (#3450): the web dashboard's dedicated mute-rule endpoints (create / update / set-enabled /
--     delete under /api/mute-rules) run as viewer, and the MCP mute-rule tools as mcp, so both get the same
--     single-table write shape as 3b. The table exists from V3, but its reload-beacon trigger
--     (trg_bump_mute_rules, V117) is SECURITY INVOKER and UPDATEs config_service.config_version AS the caller
--     on every mute-rule write -- so the COLUMN-level config_service grants below are load-bearing, not
--     optional: without them every mute-rule write fails 42501 at the trigger. Column-level on exactly the
--     two beacon columns, so neither role can flip a service flag like paused. Every other mcp-writable table
--     with a bump trigger (config_alert_settings, config_notification, config_notification_routes,
--     config_monitored_servers) rides the same mcp grant. Re-run this script after upgrading to V117+.
--     (The WPF Viewer's read-only probe discriminates on config_alert_log UPDATE -- a write viewer never
--     gets -- so a connectAs = "viewer" Viewer stays read-only in its UI despite this grant.)
GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO viewer;
GRANT UPDATE (config_version, updated_at) ON config.config_service TO viewer;
GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO mcp;
GRANT UPDATE (config_version, updated_at) ON config.config_service TO mcp;

-- 3e. Custom alert rules (#3285): the web dashboard's rule editor (/api/alerts, as viewer) and the MCP rule
--     tools (as mcp) create, edit and delete config.custom_alert_rules -- non-secret rule JSON, the same
--     single-table floor as 3b. Created by V116, so re-run this script after upgrading past it.
GRANT INSERT, UPDATE, DELETE ON config.custom_alert_rules TO viewer;
GRANT INSERT, UPDATE, DELETE ON config.custom_alert_rules TO mcp;

-- 3f. The rest of the MCP tools' writes, mirroring managed provisioning's grants exactly:
--     analyze_server persists its findings and mute_analysis_finding records a mute (INSERT only: there is no
--     MCP unmute); update_alert_settings writes the singleton alert-settings row and the ONE non-secret
--     column of config_notification it spans, email_cooldown_minutes (COLUMN-level: the row holds the SMTP
--     password and the webhook URLs, which mcp can neither read nor write); set_notification_route_enabled
--     and delete_notification_route take a route out of force or remove it (never INSERT, never a
--     destination column: a route IS a destination); add_servers / remove_server edit the monitored-server
--     registry (whose credential column stays carved, so mcp can WRITE a password blob and never READ one).
GRANT INSERT ON collect.analysis_findings TO mcp;
GRANT INSERT ON config.analysis_muted TO mcp;
GRANT UPDATE ON config.config_alert_settings TO mcp;
GRANT UPDATE (email_cooldown_minutes) ON config.config_notification TO mcp;
GRANT UPDATE (enabled, modified_at), DELETE ON config.config_notification_routes TO mcp;
GRANT INSERT, UPDATE, DELETE ON config.config_monitored_servers TO mcp;

-- 4. Default privileges so NEW tables/views (future collectors, created bare into collect via
--    search_path) auto-inherit SELECT. FOR ROLE <owner> must name the role that creates them.
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect
   GRANT SELECT ON TABLES TO admin, viewer;
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA config
   GRANT SELECT ON TABLES TO admin, viewer;
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA config
   GRANT INSERT, UPDATE, DELETE ON TABLES TO admin;
-- Sequence USAGE for admin: config_command (V17) is GENERATED ALWAYS AS IDENTITY, which needs NO
-- sequence USAGE to INSERT (unlike serial) -- but keep this fail-closed grant so a future serial
-- config column would still work without a follow-up migration.
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA config
   GRANT USAGE, SELECT ON SEQUENCES TO admin;
-- The MCP role's default is SELECT on collect only (#3914): collect holds no secrets, and the continuous
-- aggregates the service builds after this script ran are collect views, so without it the MCP tools lose
-- them until the next re-run. No default read on config, whose secret columns are carved table by table,
-- and no default write anywhere.
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect
   GRANT SELECT ON TABLES TO mcp;

-- 5. Public hardening: no world-writable public schema, no anonymous connect. REVOKE ALL drops
--    PUBLIC's implicit CONNECT, so the three roles are re-granted CONNECT explicitly. (Any other login
--    that reads this store directly needs its own GRANT CONNECT ON DATABASE darling after this.)
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE darling FROM PUBLIC;
GRANT CONNECT ON DATABASE darling TO admin, viewer;
GRANT CONNECT ON DATABASE darling TO mcp;

-- 6. Search path. The service best-effort runs this on every start, but if your collection login
--    lacks the database-owner privilege it needs, run it once yourself as the owner so every
--    connection (the Viewer, psql, pg_dump) resolves the bare table names to collect/config:
--
--       ALTER DATABASE darling SET search_path = collect, config, public;
--
-- 7. Not mirrored from managed mode, deliberately: the config.record_custom_alert_resolution function
--    (#3334) the managed batch creates for viewer and mcp. It exists for the custom-alert evaluator, which
--    runs on a managed store only; with no evaluator there is never a firing rule for a rule delete to
--    resolve.
