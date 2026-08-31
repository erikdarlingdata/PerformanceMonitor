-- ============================================================================================
-- Darling security hardening (#1262) — least-privilege role provisioning for BRING-YOUR-OWN
-- PostgreSQL (postgres.managed = false).
--
-- In managed mode the service provisions these roles automatically on every start (see
-- DarlingManagedRoles) and generates their DPAPI credentials. In BYO mode YOU run this script
-- once, as the database OWNER (the role that owns the Darling store — the one your
-- postgres.connectionString connects as for collection). It creates the two least-privilege
-- login roles the Viewer connects as instead of the owner:
--
--   admin   -- reads both schemas + writes the operator-config tables (mute rules, alert
--              dismissals, analysis mutes). The Viewer's default identity (darling.json
--              postgres.connectAs = "admin").
--   viewer  -- reads both schemas; the ONLY write it gets is INSERT/UPDATE/DELETE on config.custom_views
--              (the web dashboard's user-authored view definitions, #1563 -- non-secret JSON; edited by any
--              authenticated seat, gated server-side by the host's token+CIDR auth -- not loopback-only). All
--              other write actions degrade gracefully. Point a locked-down Viewer at this with
--              postgres.connectAs = "viewer".
--
-- PREREQUISITE: the V8 migration (which the service applies on startup) must already have created
-- the collect and config schemas and moved the tables into them. Run this AFTER the service has
-- started at least once against your store. Re-running after a later schema upgrade is safe and
-- idempotent: the GRANT ... ON ALL TABLES statements below re-cover every table that now exists
-- (including the V17 control-plane tables config_monitored_servers / config_alert_settings /
-- config_notification / config_collector_schedules / config_service / config_command), and the
-- ALTER DEFAULT PRIVILEGES already auto-grant any config table the owner creates after this runs. The one
-- write that is NOT covered by re-running blindly is the V31 config.custom_views viewer grant in step 3b:
-- that table only exists once the service has migrated your store to V31, so re-run this script after upgrading.
--
-- BEFORE RUNNING:
--   1. Replace CHANGE_ME_ADMIN_PASSWORD and CHANGE_ME_VIEWER_PASSWORD with strong passwords.
--   2. If your database is not named "darling", change it in the REVOKE/GRANT ... ON DATABASE
--      lines and in the ALTER DATABASE note at the bottom.
--   3. If the owner role is not "darling", change it in the ALTER DEFAULT PRIVILEGES FOR ROLE
--      lines (it must be the role that CREATEs the tables — your collection connection's role).
--
--   psql -h <host> -U <owner> -d darling -f provision-roles.sql
--
-- NAME-COLLISION SAFETY: the roles are the bare, un-prefixed names "admin" and "viewer". If your
-- cluster ALREADY has a role by either name that this script did not create, it will NOT be silently
-- repurposed: fresh roles are stamped with a marker comment ('darling-managed'), and an existing
-- same-named role without that marker makes this script FAIL LOUD (rename or drop the other role
-- first, or use a dedicated cluster/database for the Darling store).
-- ============================================================================================

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
END $$;

ALTER ROLE admin  LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_ADMIN_PASSWORD';
ALTER ROLE viewer LOGIN NOSUPERUSER PASSWORD 'CHANGE_ME_VIEWER_PASSWORD';

-- 1a. statement_timeout backstop on viewer (Custom Views v2, #1563): the web dashboard's compose surface
--     connects as viewer and runs network-reachable aggregations over a raw, no-rollup store, so this caps a
--     runaway query at the database (a LIMIT bounds output, not work). Keep this value in step with
--     ComposeLimits.StatementTimeout in the service. (Managed mode also sets this on the network 'mcp' role,
--     which a BYO deployment does not create -- your own PostgreSQL governs any MCP-role exposure it wants.)
ALTER ROLE viewer SET statement_timeout = '15s';

-- 2. Schema usage + SELECT on everything that exists now (ALL TABLES covers tables AND views). collect
--    holds no secrets, so admin+viewer read all of it. config: admin (the writer, and the Settings
--    window's identity) reads every column; viewer reads all config tables too -- MINUS the secret columns
--    carved in step 2b.
GRANT USAGE ON SCHEMA collect, config TO admin, viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer;

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
--     identical carve from). These three GRANT lists are a HAND MIRROR of it, and hand mirrors drift -- they
--     did, silently, for three releases (see #1639). The ungated ProvisionRolesAclDriftTests parses THIS FILE
--     and asserts set-equality against that C# list, so adding a column to one side without the other now
--     fails the build. Keep the columns in the C# list's order so the two read as the same list.
REVOKE SELECT ON config.config_monitored_servers FROM viewer;
GRANT SELECT (server_id, name, host, database, auth, username, encrypt_mode, trust_server_certificate,
              read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans,
              is_enabled, created_at, modified_at, alert_delivery_mode_override,
              -- V68: engine + port. Non-secret, exactly like host.
              engine, port,
              -- V107 (#2138): the force-plan bot's per-server arm state. Non-secret, exactly like is_enabled.
              plan_force_bot_enabled)
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

-- 3. config writes -- admin only.
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;

-- 3b. Custom views (#1563): the SINGLE viewer write. The web dashboard connects as the least-privilege
--     viewer role and its custom-view composer writes exactly this one config table (non-secret dashboard
--     JSON). Editing is any authenticated seat -- the web surface's normal networked mode, gated server-side
--     by the host's token+CIDR auth (not loopback-only); this narrow grant is only the floor beneath that
--     gate. EXPLICIT single-table statement, NO ALTER DEFAULT PRIVILEGES (ADP has no per-table
--     form -> it would broaden viewer to ALL of config). config.custom_views is created by the V31 migration,
--     so re-run this script AFTER the service has migrated your store to V31 (else this line errors: no table).
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO viewer;

-- 3c. Database-state expected states (#1986): the viewer's per-database override editor writes this one
--     config table (non-secret; the expected/(ignore) state per database). SELECT is already covered by
--     the blanket config grant in step 3; this is the write floor for the editor, gated server-side by the
--     same token+CIDR auth as the custom-view composer. EXPLICIT single-table statement (no ALTER DEFAULT
--     PRIVILEGES, which would broaden viewer to ALL of config). config.database_state_expected is created by
--     the V49 migration, so re-run this script AFTER the service has migrated your store to V49.
GRANT INSERT, UPDATE, DELETE ON config.database_state_expected TO viewer;

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

-- 5. Public hardening: no world-writable public schema, no anonymous connect. REVOKE ALL drops
--    PUBLIC's implicit CONNECT, so admin/viewer are re-granted CONNECT explicitly.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE darling FROM PUBLIC;
GRANT CONNECT ON DATABASE darling TO admin, viewer;

-- 6. Search path. The service best-effort runs this on every start, but if your collection login
--    lacks the database-owner privilege it needs, run it once yourself as the owner so every
--    connection (the Viewer, psql, pg_dump) resolves the bare table names to collect/config:
--
--       ALTER DATABASE darling SET search_path = collect, config, public;
