-- ============================================================================================
-- Darling security hardening (#1262) — least-privilege role provisioning for BRING-YOUR-OWN
-- PostgreSQL (postgres.managed = false).
--
-- In managed mode the service provisions these roles automatically on every start (see
-- DarlingManagedRoles) and generates their DPAPI credentials. The Linux compose distribution does the
-- same on the store its compose file creates (#3914): there the service logs in as the store's
-- bootstrap superuser and provisions all four roles itself, so do NOT run this script against the
-- compose store. On any other store YOU run this script once, as the database OWNER (the role that
-- owns the Darling store — the one your postgres.connectionString connects as for collection). It
-- creates four least-privilege login roles:
--
--   admin   -- reads both schemas + writes the operator-config tables (mute rules, alert
--              dismissals, analysis mutes). The Viewer's default identity (darling.json
--              postgres.connectAs = "admin").
--   viewer  -- reads both schemas, minus the secret columns carved in step 2b, and writes nothing. It is the login for remote
--              and locked-down Viewer seats (postgres.connectAs = "viewer"), which show "Seat: read-only". An earlier version of
--              this script also gave it the web dashboard's writes; running the script again takes them away from it.
--   web     -- the web dashboard host's login (postgres.webConnectionString): viewer's reads, plus the narrow, enumerated web-surface writes: INSERT/UPDATE/DELETE
--              on config.custom_views (the user-authored view definitions, #1563), on
--              config.custom_alert_rules (the user-authored alert rules, #3285), on
--              config.database_state_expected (the per-database override editor, #1986), and on
--              config.config_mute_rules (the web dashboard's dedicated mute-rule endpoints, #3450 -- plus the
--              two config_service beacon columns their bump trigger writes as the caller), and the single
--              dismissed column of config.config_alert_log (the web Alert History dismiss, #4843), and INSERT
--              on config.config_monitored_servers and no UPDATE on it: the web add-server route (#4843) inserts, and
--              the web edit-server route (#5240) edits through config.edit_monitored_server, a function it may
--              execute (its credential column stays SELECT-carved), and the web Run collectors now (#5941), Run live now
--              (#6386) and Generate now (#6389) buttons queue their command rows through config.request_snapshot_now,
--              config.request_active_queries_live and config.request_analyze_now, functions it may execute (mcp may execute request_snapshot_now too). Every table is non-secret-keyed; over the web every write is gated server-side by the host's auth + seat model -- these
--              grants are only the floor beneath that gate. All other write actions degrade gracefully. The
--              web dashboard's identity only. It never gets a network (pg_hba) line, and it is not a seat login.
--   mcp     -- the MCP server's identity: viewer's reads (the same secret-column carve), plus the MCP tools'
--              narrow writes -- analyze_server's findings, finding mutes, custom views, custom alert rules, database state expectations (#6397),
--              alert tuning (mute rules, the alert settings row, the delivery cooldown column, a notification
--              route's enabled flag or its removal) and server onboarding (config_monitored_servers, whose
--              credential column it can write but never read). Never admin: a network token-holder must not
--              reach the config_command service-credential pivot or any secret column.
--
-- WHO CONNECTS AS WHAT (#3914). The service collects as the owner. The web dashboard and the MCP server
-- connect with postgres.webConnectionString and postgres.mcpConnectionString in darling.json: point
-- them at the web and mcp roles this script creates, for example
--
--     "webConnectionString": "Host=db.example.com;Port=5432;Username=web;Password=<web password>;Database=darling"
--
-- or as "file:/path" / "env:NAME" references, like connectionString, to keep the password out of
-- darling.json. Left unset, that surface connects as the OWNER, and nothing this script sets up applies
-- to it: not the secret-column carve (steps 2b and 2c), not the narrow write grants (steps 3b-3f), not
-- the statement_timeout backstop (step 1a). The service says so in a warning at startup.
--
-- MOVING AN EXISTING STORE TO THE web ROLE. A store this script set up before the web role existed has no web role, and
-- its viewer role still holds the web dashboard's writes. Until you act, the web dashboard keeps working on the login its
-- webConnectionString names. To move it: (1) replace every CHANGE_ME_WEB_PASSWORD below with the web password you choose (and the other three placeholders, as before: the script stops, before it sets any password, while any of the four is unchanged), (2) run
-- this script again, which creates web and takes the writes away from viewer, and (3) set postgres.webConnectionString to the
-- web login and restart the service. Do steps 2 and 3 together: between them a web dashboard still on viewer reads fine
-- and its writes are refused. Until step 3, the service warns at startup that the web connection string names viewer.
--
-- PREREQUISITE: the V8 migration (which the service applies on startup) must already have created
-- the collect and config schemas and moved the tables into them. Run this AFTER the service has
-- started at least once against your store. Re-running after a later schema upgrade is safe and
-- idempotent: the GRANT ... ON ALL TABLES statements below re-cover every table that now exists
-- (including the V17 control-plane tables config_monitored_servers / config_alert_settings /
-- config_notification / config_collector_schedules / config_service / config_command), and the
-- ALTER DEFAULT PRIVILEGES already auto-grant any table the owner creates after this runs (config
-- tables to admin, viewer and web; collect tables to all four). The writes that are NOT covered by
-- re-running blindly are the single-table grants in steps 3b-3f: each names a table (or, for the
-- beacon columns, a trigger dependency) a specific migration creates -- custom_views is V31,
-- database_state_expected is V49, custom_alert_rules is V116, the mute-rule reload-beacon trigger is
-- V117, the query-tag tables (step 3k, #5977) are V192, config_notification_routes is V131 (its scope_server_id and scope_tag_id columns are V181, #5750: run this script only after the store has migrated to V181, since the route column grants name them; the agent_* route columns are V184, #5632, and are granted only once they exist) and config_collector_run_times is V160 and the password key tables (step 3g) are V165 and the Level 2 tables (step 3j: config_threshold_overrides and investigation_feedback, #5909) are V191 and the Phase C tables (step 3l: investigation_evidence, investigation_signature_days and investigation_signature_merges, #5997) are V193, as are the signing key and fallback route columns the read carve above names -- so re-run this script after upgrading past each.
--
-- BEFORE RUNNING:
--   1. Replace CHANGE_ME_ADMIN_PASSWORD, CHANGE_ME_VIEWER_PASSWORD, CHANGE_ME_WEB_PASSWORD and CHANGE_ME_MCP_PASSWORD with strong
--      passwords, every occurrence of each (a find-and-replace does it; each is in a check and in its ALTER ROLE line). The
--      script stops with an error, before any password is set, when any of the four is unchanged: a routine written before
--      the web role existed replaces only the admin, viewer and mcp placeholders, and the web one makes it stop instead of
--      setting the web password to the placeholder text. To set the passwords with \password afterwards, replace each
--      placeholder with a long random value for this run first, which \password then replaces (run \password for all four roles). Better, keep the passwords out of every statement (#3910): replace each literal with a
--      SCRAM-SHA-256 verifier, which PostgreSQL stores as-is, or set the passwords afterwards with psql's
--      \password admin, \password viewer, \password web and \password mcp, which compute the verifier locally and send
--      only that. A password literal is kept verbatim by anything that records statement text:
--      log_statement set to ddl, mod or all, log_min_duration_statement set to 0 or more, a failed statement's
--      STATEMENT line in the server log, auto_explain, pg_stat_statements with utility tracking on, and
--      pg_stat_monitor with utility tracking on (pg_stat_monitor.pgsm_track_utility, on by default). pgaudit's role
--      class writes ALTER ROLE statements to the log too, with the statement text while pgaudit.log_statement is on
--      (its default). Step 0 turns off the first four, the utility tracking of both modules, and
--      pgaudit.log_statement, for this session when the role running the script is allowed to. Only a superuser can
--      change a pgaudit setting, so for any other role the pgaudit entries of this run keep their statement text:
--      keep the passwords out of the statements as above.
--   2. If your database is not named "darling", change it in the REVOKE/GRANT ... ON DATABASE
--      lines and in the ALTER DATABASE note at the bottom.
--   3. If the owner role is not "darling", change it in the ALTER DEFAULT PRIVILEGES FOR ROLE
--      lines (it must be the role that CREATEs the tables — your collection connection's role).
--
--   Needs psql 10 or later (step 1 says why):
--   psql -X -h <host> -U <owner> -d darling -f provision-roles.sql
--
--   -X keeps your own psqlrc out of the run: an \set AUTOCOMMIT off in it would leave steps 3 to 5 (the write
--   grants, the default privileges and the public hardening) uncommitted while psql still exits 0, and an
--   ON_ERROR_STOP in it would change where the run stops. The roles, the passwords, the read grants, the
--   secret-column carve and the password key table revoke (step 2d) commit at the script's own COMMIT.
--
-- NAME-COLLISION SAFETY: the roles are the bare, un-prefixed names "admin", "viewer", "web" and "mcp". If your
-- cluster ALREADY has a role by any of those names that this script did not create, it will NOT be
-- silently repurposed: fresh roles are stamped with a marker comment ('darling-managed'), and an existing
-- same-named role without that marker makes this script FAIL LOUD (rename or drop the other role first,
-- or use a dedicated cluster/database for the Darling store). That includes the roles the service
-- provisions on the compose store, which carry 'darling-compose' instead.
--
-- The marker is a public string, so it does not show who made a role. From PostgreSQL 16 a role with CREATEROLE
-- holds ADMIN OPTION on every role it creates, which lets it reset that role's password and grant it onward. So
-- a marked role is refused too (the same stop as above) while a role that is not a superuser, not the database
-- owner and not the role running this script holds ADMIN OPTION on it. To go on, run the script as that login (it
-- is then the role running it, so the check leaves it out), or, as a superuser, revoke that option (a login that
-- created the role cannot revoke the option itself) or drop the role. A plain membership (GRANT viewer TO
-- some_analyst) is not refused.
--
-- AN OWNER WHO IS NOT A SUPERUSER can run this script, with these limits:
--   * It needs CREATEROLE. From PostgreSQL 16 it also needs ADMIN OPTION on each of the four roles to set a
--     password or take back an attribute, and it holds that option only on a role it created itself. A role a
--     superuser made needs GRANT <role> TO <owner> WITH ADMIN OPTION from a superuser first. The guard in step 1
--     checks this before any password is sent, and stops the run when the owner cannot change one of the four
--     roles that already exist: it has no CREATEROLE, the role is a superuser, from PostgreSQL 16 it lacks ADMIN
--     OPTION on the role, or before 16 the role has REPLICATION (only a superuser can change such a role). The
--     stop names the role and the remedy: a superuser runs GRANT <role> TO <owner> WITH ADMIN OPTION, or runs
--     this script. The guard also stops the run when one of the four roles is a member of a role that would
--     outrank the grants below (the owner, a superuser or BYPASSRLS role, pg_read_all_data, pg_write_all_data,
--     pg_read_server_files, pg_write_server_files or pg_execute_server_program) and the owner cannot revoke that
--     membership: from PostgreSQL 16 revoking needs ADMIN OPTION on the role the membership is in, and no role
--     holds that option on itself. The stop names each membership. A password line that still fails (a
--     password-check module can reject a password) is written to the server log with its password, unless step 0
--     could turn off log_min_error_statement for the session.
--   * pg_stat_statements: while the module is loaded with utility tracking on, step 0 fails for this owner, and
--     the guard in step 1 then stops the run before any password is sent. Either GRANT SET ON PARAMETER
--     pg_stat_statements.track_utility TO <owner> (PostgreSQL 15 and later), so step 0 can turn it off for this
--     session, or turn it off for the cluster (step 1b).
--   * The superuser-only lines (temp_file_limit, log_min_duration_statement, log_parameter_max_length) fail, and
--     psql carries on. SUPERUSER is not switched off, nor is REPLICATION, BYPASSRLS or CREATEDB unless the owner
--     holds that attribute itself (PostgreSQL 16 and later; before 16 only a superuser can change REPLICATION or
--     BYPASSRLS), and a role membership the owner is not allowed to revoke stays. Step 1 prints a WARNING for
--     each of those, so read psql's output.
--   * Step 0 cannot turn off log_statement, log_min_duration_statement or log_min_error_statement for this session
--     unless the owner was granted SET on them (PostgreSQL 15 and later). Where log_statement is ddl, mod or all,
--     log_min_duration_statement is 0 or more, log_transaction_sample_rate is above 0, log_min_duration_sample is 0 or
--     more with log_statement_sample_rate above 0, or pgaudit.log covers the role class while pgaudit.log_statement is
--     on, the run's password lines are written to the server log: the script prints a WARNING after step 0 when it
--     sees any of them, and you should keep the passwords out of the lines, as in BEFORE RUNNING above.
-- ============================================================================================

-- 0. Keep this script's own role DDL out of pg_stat_statements (#3899). Step 1 sends each password as an
--    ALTER ROLE ... PASSWORD literal, and while pg_stat_statements is loaded with utility tracking on (the
--    module's default), it records that statement VERBATIM, password included, where any superuser or
--    pg_read_all_stats member (a monitoring agent's pg_monitor login, say) can read it. This covers this
--    session only; step 1b says how to turn it off for good. Harmless when the module is not loaded. When it is
--    loaded, this SET needs a superuser, or an owner that was granted SET on the parameter (PostgreSQL 15 and
--    later): for any other owner it fails, and the first statement of the guard in step 1 then stops the run
--    before a password is sent.
SET pg_stat_statements.track_utility = off;

--    pg_stat_monitor tracks utility statements through pg_stat_monitor.pgsm_track_utility (on by default), so it gets
--    the same switch and the same stop in the guard. The setting can be changed by any role.
SET pg_stat_monitor.pgsm_track_utility = off;

--    The same for the server log. log_statement set to ddl, mod or all writes each ALTER ROLE ... PASSWORD line to
--    the log as sent, and log_min_duration_statement (or log_min_duration_sample) at 0 or more writes it with its
--    duration, so these turn that logging off for this session (this run's own role and grant statements are then
--    not logged either). A statement that fails is written to the log with its text whenever its error is at or
--    above log_min_error_statement (error by default), so that is raised to panic, which no statement reaches. The
--    debug_print_parse, debug_print_rewritten and debug_print_plan settings write a statement's tree, literals
--    included, so they are switched off too, and so is pgaudit.log_statement, which adds the statement text to
--    pgaudit's entries. Every one of them except the debug_print ones is superuser-only, so an owner that was not granted SET on it
--    (PostgreSQL 15 and later) is refused, quietly, and the setting stays as it is: the README says what that owner
--    does then. A setting this server does not have (the two sampling ones need PostgreSQL 13 and 12) is skipped.
DO $$
DECLARE
   log_setting record;
BEGIN
   FOR log_setting IN
      SELECT name, value
      FROM (VALUES ('log_statement', 'none'), ('log_min_duration_statement', '-1'),
                   ('log_min_duration_sample', '-1'), ('log_transaction_sample_rate', '0'),
                   ('log_min_error_statement', 'panic'), ('debug_print_parse', 'off'),
                   ('debug_print_rewritten', 'off'), ('debug_print_plan', 'off'),
                   ('pgaudit.log_statement', 'off')) AS log_settings (name, value)
   LOOP
      BEGIN
         PERFORM set_config(log_setting.name, log_setting.value, false);
      EXCEPTION WHEN insufficient_privilege OR undefined_object THEN
         NULL;
      END;
   END LOOP;
END $$;

--    What step 0 could not turn off is said here, before any password is sent: the role running the script was not
--    allowed to change the setting, so the run's password lines are written to the server log. The settings are
--    log_statement at ddl, mod or all, log_min_duration_statement at 0 or more, log_transaction_sample_rate above 0
--    (every statement of a sampled transaction is logged), log_min_duration_sample at 0 or more with
--    log_statement_sample_rate above 0, and pgaudit.log covering the role class (role or all, and not -role) while
--    pgaudit.log_statement is on. This is a WARNING and the run goes on, because the passwords may already be
--    SCRAM-SHA-256 verifiers.
DO $$
DECLARE
   logging_on text;
BEGIN
   SELECT string_agg(name || ' = ' || setting
                     || CASE name
                           WHEN 'log_min_duration_sample' THEN ' with log_statement_sample_rate = '
                              || (SELECT sample.setting FROM pg_settings AS sample WHERE sample.name = 'log_statement_sample_rate')
                           WHEN 'pgaudit.log' THEN ' with pgaudit.log_statement = on'
                           ELSE ''
                        END, ', ' ORDER BY name) INTO logging_on
   FROM pg_settings
   WHERE CASE name
            WHEN 'log_statement' THEN setting IN ('ddl', 'mod', 'all')
            WHEN 'log_min_duration_statement' THEN setting::bigint >= 0
            WHEN 'log_transaction_sample_rate' THEN setting::numeric > 0
            WHEN 'log_min_duration_sample' THEN setting::bigint >= 0
               AND (SELECT sample.setting::numeric FROM pg_settings AS sample WHERE sample.name = 'log_statement_sample_rate') > 0
            WHEN 'pgaudit.log' THEN (SELECT statement.setting FROM pg_settings AS statement WHERE statement.name = 'pgaudit.log_statement') = 'on'
               AND ((string_to_array(replace(lower(setting), ' ', ''), ',') && ARRAY['role', 'all']
                     AND NOT ('-role' = ANY (string_to_array(replace(lower(setting), ' ', ''), ',')))
                    ) OR (string_to_array(replace(lower(setting), ' ', ''), ',') && ARRAY['function', 'all']
                     AND NOT ('-function' = ANY (string_to_array(replace(lower(setting), ' ', ''), ',')))))
            ELSE false
         END;
   IF logging_on IS NOT NULL THEN
      RAISE WARNING 'The server log records the statements of this run (%). The ALTER ROLE ... PASSWORD lines and the placeholder checks (DO blocks, which pgaudit logs under its function class) are written to it with their passwords, because step 0 could not turn that logging off for this role. Put a SCRAM-SHA-256 verifier in each ALTER ROLE line in place of the literal, or set the passwords afterwards with the psql password command, or run this script as a superuser.', logging_on;
   END IF;
END $$;

-- 1. Roles (CREATE ROLE has no IF NOT EXISTS -> guard with a DO block). Idempotent: re-running
--    this script re-asserts the password below, so it doubles as a password rotation. A fresh role
--    is stamped 'darling-managed'; an unmarked same-named role fails loud (never repurposed).
--    The guard carries no password. A statement that fails is written to the server log with its whole text,
--    and the guard is the statement that fails when it stops the run, so it creates each role without one and the
--    ALTER ROLE lines after it set them.
-- Stop on an error for this guard alone (#4746): a script-wide stop would end a non-superuser owner before any grant.
-- The caller's own ON_ERROR_STOP (psql -v ON_ERROR_STOP=1, or psqlrc) is saved first and put back after the guard,
-- not forced off. From psql 10 an unset ON_ERROR_STOP reads as off, so the saved value is always on or off. In psql 9.6
-- an unset variable stays the literal text :ON_ERROR_STOP and reads as on, so the whole run would stop at its first
-- error and an owner who is not a superuser would get no grants: this script needs psql 10 or later.
\set darling_saved_stop :ON_ERROR_STOP
\set ON_ERROR_STOP on
DO $$
DECLARE
   other_holder record;
   unchangeable text;
   remedy text;
BEGIN
   -- First, before any role is touched: with utility tracking still on for this session, pg_stat_statements would
   -- record the password statements below verbatim, so stop. NULL (the module is not loaded) passes.
   IF current_setting('pg_stat_statements.track_utility', true) = 'on' THEN
      RAISE EXCEPTION 'pg_stat_statements.track_utility is on for this session. pg_stat_statements records the password statements of this script verbatim, and any pg_read_all_stats member can read them. The script created and changed nothing. Do one of these three things, then run the script again. Run GRANT SET ON PARAMETER pg_stat_statements.track_utility TO <owner> as a superuser (PostgreSQL 15 and later). Then step 0 can turn it off for this session. Or set pg_stat_statements.track_utility = off in postgresql.conf and reload, which turns it off for the whole cluster (step 1b). Or run this script as a superuser.';
   END IF;
   -- The same stop for pg_stat_monitor, which tracks utility statements through pgsm_track_utility. Any role can switch it
   -- off for its session and step 0 does, so this is reached only when that setting is on anyway (a parameter reset, say).
   IF current_setting('pg_stat_monitor.pgsm_track_utility', true) IN ('on', 'yes', 'true', '1') THEN
      RAISE EXCEPTION 'pg_stat_monitor.pgsm_track_utility is on for this session. pg_stat_monitor tracks the password statements of this script. The script created and changed nothing. Set pg_stat_monitor.pgsm_track_utility = off in postgresql.conf and reload, which turns it off for the whole cluster, then run the script again.';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
      CREATE ROLE admin LOGIN NOSUPERUSER;
      COMMENT ON ROLE admin IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'admin'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "admin" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'viewer') THEN
      CREATE ROLE viewer LOGIN NOSUPERUSER;
      COMMENT ON ROLE viewer IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'viewer'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "viewer" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp') THEN
      CREATE ROLE mcp LOGIN NOSUPERUSER;
      COMMENT ON ROLE mcp IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'mcp'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "mcp" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'web') THEN
      CREATE ROLE web LOGIN NOSUPERUSER;
      COMMENT ON ROLE web IS 'darling-managed';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = 'web'), 'pg_authid') IS DISTINCT FROM 'darling-managed' THEN
      RAISE EXCEPTION 'Role "web" already exists and was not created by Darling (missing the ''darling-managed'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated login.';
   END IF;
   -- The marker does not show who made a role, so a marked role is also refused while a role that is not a
   -- superuser, not the role running this script and not the database owner holds ADMIN OPTION on it: that role
   -- could reset its password and grant it onward. (A role this run created has no holder but the one running it.)
   -- Plain memberships do not count, and neither does the owner of a first run that a superuser then rotates.
   FOR other_holder IN
      SELECT granted.rolname AS role_name, string_agg(quote_ident(holder.rolname), ', ' ORDER BY holder.rolname) AS holders
      FROM pg_auth_members AS membership
      JOIN pg_roles AS granted ON granted.oid = membership.roleid
      JOIN pg_roles AS holder ON holder.oid = membership.member
      WHERE granted.rolname IN ('admin', 'viewer', 'web', 'mcp')
        AND membership.admin_option
        AND NOT holder.rolsuper
        AND holder.rolname <> current_user
        AND holder.oid <> (SELECT datdba FROM pg_database WHERE datname = current_database())
      GROUP BY granted.rolname
      ORDER BY granted.rolname
   LOOP
      RAISE EXCEPTION 'Role "%" carries the ''darling-managed'' marker, but another login holds ADMIN OPTION on it: %. That login is not a superuser, not the database owner and not the role running this script. It can reset the password of the role or grant the role to other roles, and the marker does not show who made the role. To go on, run this script as that login, which this check leaves out. If more than one login is listed, run the script as one of them after the others lose the option. Or, as a superuser, revoke that ADMIN OPTION (a login that created the role cannot revoke it itself), or drop the role.', other_holder.role_name, other_holder.holders;
   END LOOP;
   -- A role that is not a superuser must also be able to change the four roles. If it cannot, each ALTER ROLE ... PASSWORD
   -- below fails, the server writes the failed statement to its log WITH the password, and psql exits 0 unless the caller
   -- set ON_ERROR_STOP. So a run by such a role stops here, before any password is sent. The ALTER ROLE lines need:
   -- the CREATEROLE attribute; roles that are not superusers; from PostgreSQL 16, ADMIN OPTION on each role (pg_has_role
   -- tests the same thing ALTER ROLE does, and a role this run just created passes because its creator holds the option);
   -- before 16, roles without REPLICATION. Every role exists at this point, and a superuser is never stopped here.
   IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN
      IF NOT (SELECT rolcreaterole FROM pg_roles WHERE rolname = current_user) THEN
         RAISE EXCEPTION 'The role running this script, %, is not a superuser and does not have CREATEROLE. It cannot change the passwords of the roles admin, viewer, web and mcp. The script created and changed nothing. It stopped here so that a failed ALTER ROLE line does not write a password to the server log. Run this script again as a superuser, or as a role that has CREATEROLE.', quote_ident(current_user);
      END IF;
      SELECT string_agg(quote_ident(rolname), ', ' ORDER BY rolname) INTO unchangeable
      FROM pg_roles
      WHERE rolname IN ('admin', 'viewer', 'web', 'mcp') AND rolsuper;
      IF unchangeable IS NOT NULL THEN
         RAISE EXCEPTION 'These roles are superusers: %. The role running this script, %, is not a superuser, so it cannot change them. The script created and changed nothing. It stopped here so that a failed ALTER ROLE line does not write a password to the server log. Run this script again as a superuser.', unchangeable, quote_ident(current_user);
      END IF;
      IF current_setting('server_version_num')::int >= 160000 THEN
         SELECT string_agg(quote_ident(rolname), ', ' ORDER BY rolname), string_agg(format('GRANT %I TO %I WITH ADMIN OPTION;', rolname, current_user), ' ' ORDER BY rolname)
         INTO unchangeable, remedy
         FROM pg_roles
         WHERE rolname IN ('admin', 'viewer', 'web', 'mcp') AND NOT pg_has_role(current_user, oid, 'MEMBER WITH ADMIN OPTION');
         IF unchangeable IS NOT NULL THEN
            RAISE EXCEPTION 'The role running this script, %, does not hold ADMIN OPTION on these roles: %. From PostgreSQL 16 a role needs that option to change the password of another role. The script created and changed nothing. It stopped here so that a failed ALTER ROLE line does not write a password to the server log. Run this script as a superuser. Or, as a superuser, run these statements and then run this script again: %', quote_ident(current_user), unchangeable, remedy;
         END IF;
      ELSE
         SELECT string_agg(quote_ident(rolname), ', ' ORDER BY rolname) INTO unchangeable
         FROM pg_roles
         WHERE rolname IN ('admin', 'viewer', 'web', 'mcp') AND rolreplication;
         IF unchangeable IS NOT NULL THEN
            RAISE EXCEPTION 'These roles have the REPLICATION attribute: %. Before PostgreSQL 16 only a superuser can change a role that has it, and the role running this script, %, is not a superuser. The script created and changed nothing. It stopped here so that a failed ALTER ROLE line does not write a password to the server log. Run this script again as a superuser.', unchangeable, quote_ident(current_user);
         END IF;
      END IF;
      -- A membership of one of the four roles that outranks every grant below (a role that is, or is a member of, the
      -- owner, a superuser or BYPASSRLS role, or a predefined role that reads or writes every table or the server's files
      -- or programs) is revoked after the passwords, and the block that revokes it can only warn when the role running
      -- this script lacks the right to. So the run stops here, before a password is sent, as it does when it cannot change
      -- the roles at all. From PostgreSQL 16 revoking needs ADMIN OPTION on the role the membership is in, and no role
      -- holds that on itself; it also removes only the row recorded with the grantor it names (the block that revokes
      -- names the recorded one), so the role running this script must be able to act as that grantor, which it can
      -- when it is, or inherits the privileges of, that role. Before 16 CREATEROLE is enough, except for a superuser role.
      SELECT string_agg(format('%I in %I', member.rolname, granted.rolname), ', ' ORDER BY member.rolname, granted.rolname)
      INTO unchangeable
      FROM pg_auth_members AS membership
      JOIN pg_roles AS granted ON granted.oid = membership.roleid
      JOIN pg_roles AS member ON member.oid = membership.member
      WHERE member.rolname IN ('admin', 'viewer', 'web', 'mcp')
        AND EXISTS (
           SELECT 1
           FROM pg_roles AS voiding
           WHERE (voiding.rolsuper
                  OR voiding.rolbypassrls
                  OR voiding.oid IN ((SELECT oid FROM pg_roles WHERE rolname = current_user), (SELECT datdba FROM pg_database WHERE datname = current_database()))
                  OR voiding.rolname IN ('pg_read_all_data', 'pg_write_all_data', 'pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program'))
             AND pg_has_role(granted.oid, voiding.oid, 'MEMBER'))
        AND NOT CASE WHEN current_setting('server_version_num')::int >= 160000
                     THEN pg_has_role(current_user, granted.oid, 'MEMBER WITH ADMIN OPTION')
                          AND pg_has_role(current_user, membership.grantor, 'USAGE')
                     ELSE NOT granted.rolsuper END;
      IF unchangeable IS NOT NULL THEN
         RAISE EXCEPTION 'These roles are members of roles whose privileges outrank the grants below: %. The role running this script, %, cannot revoke those memberships. The script created and changed nothing. It stopped here before a password is sent, so that no role keeps privileges the carve does not hold back. Run this script again as a superuser, or have a superuser revoke those memberships first (from PostgreSQL 16 with GRANTED BY the grantor that recorded each one).', unchangeable, quote_ident(current_user);
      END IF;
   END IF;
END $$;

-- The placeholder check (before any password is set). Each of the four password placeholders must be replaced, or the
-- run stops here: left alone, a placeholder would become the role's real password. A routine written before the web
-- role existed replaces only the three earlier placeholders and would otherwise set web to the placeholder text. Each
-- role has its own statement, so a stop writes only the placeholder that was left, never another role's password, to
-- the server log. The stop is the one the guard above runs with. The guard has already created any missing role without
-- a password, which cannot be logged into with one; the ALTER ROLE lines below set the passwords, and none has run.
-- The four checks use their own dollar-quote tag, so a password that contains two dollar signs in a row cannot end the check's body early.
-- admin: the placeholder is on the left, so a find-and-replace of it fills in your password here too. The right side is
-- built from two pieces, so that replace never reaches it.
DO $darling_placeholder_check$
BEGIN
   IF 'CHANGE_ME_ADMIN_PASSWORD' = 'CHANGE_ME_' || 'ADMIN_PASSWORD' THEN
      RAISE EXCEPTION 'The password for role admin is still the CHANGE_ME placeholder. The script set no password. Replace the admin placeholder on every line of this script that has it (the check above the ALTER ROLE lines and the ALTER ROLE admin line), then run it again.';
   END IF;
END $darling_placeholder_check$;
-- viewer: the placeholder is on the left, so a find-and-replace of it fills in your password here too. The right side is
-- built from two pieces, so that replace never reaches it.
DO $darling_placeholder_check$
BEGIN
   IF 'CHANGE_ME_VIEWER_PASSWORD' = 'CHANGE_ME_' || 'VIEWER_PASSWORD' THEN
      RAISE EXCEPTION 'The password for role viewer is still the CHANGE_ME placeholder. The script set no password. Replace the viewer placeholder on every line of this script that has it (the check above the ALTER ROLE lines and the ALTER ROLE viewer line), then run it again.';
   END IF;
END $darling_placeholder_check$;
-- web: the placeholder is on the left, so a find-and-replace of it fills in your password here too. The right side is
-- built from two pieces, so that replace never reaches it.
DO $darling_placeholder_check$
BEGIN
   IF 'CHANGE_ME_WEB_PASSWORD' = 'CHANGE_ME_' || 'WEB_PASSWORD' THEN
      RAISE EXCEPTION 'The password for role web is still the CHANGE_ME placeholder. The script set no password. Replace the web placeholder on every line of this script that has it (the check above the ALTER ROLE lines and the ALTER ROLE web line), then run it again.';
   END IF;
END $darling_placeholder_check$;
-- mcp: the placeholder is on the left, so a find-and-replace of it fills in your password here too. The right side is
-- built from two pieces, so that replace never reaches it.
DO $darling_placeholder_check$
BEGIN
   IF 'CHANGE_ME_MCP_PASSWORD' = 'CHANGE_ME_' || 'MCP_PASSWORD' THEN
      RAISE EXCEPTION 'The password for role mcp is still the CHANGE_ME placeholder. The script set no password. Replace the mcp placeholder on every line of this script that has it (the check above the ALTER ROLE lines and the ALTER ROLE mcp line), then run it again.';
   END IF;
END $darling_placeholder_check$;
\set ON_ERROR_STOP :darling_saved_stop

-- The passwords. Each ALTER names LOGIN and the password only: NOSUPERUSER here would fail for an owner that is
-- not a superuser (it needs one for any change to that attribute), and a run that fails these three lines rotates
-- nothing while psql still exits 0. The roles were created NOSUPERUSER; the next block takes the attributes back.
ALTER ROLE admin  LOGIN PASSWORD 'CHANGE_ME_ADMIN_PASSWORD';
ALTER ROLE viewer LOGIN PASSWORD 'CHANGE_ME_VIEWER_PASSWORD';
ALTER ROLE web    LOGIN PASSWORD 'CHANGE_ME_WEB_PASSWORD';
ALTER ROLE mcp    LOGIN PASSWORD 'CHANGE_ME_MCP_PASSWORD';

-- Attributes and memberships (managed mode's step 1b and section 11). A role this script ADOPTS (it already
-- carries the marker) keeps whatever was granted to it since: CREATEROLE, CREATEDB, REPLICATION or BYPASSRLS on
-- viewer, web or mcp would outrank every grant below, and so would a membership (pg_read_all_data undoes the
-- secret-column carve, and a membership in the owner is every privilege the owner has). So every one is taken
-- back, every run.
--   As a superuser these are managed mode's statements exactly: every attribute off by name, and every membership
--   in which one of the roles is the MEMBER revoked, GRANTED BY the recorded grantor (since PostgreSQL 16 a plain
--   REVOKE removes only the grants the bootstrap superuser made), with CASCADE (a grant the member itself made
--   with an ADMIN OPTION depends on the membership).
--   Otherwise it goes as far as the owner's privileges allow. From PostgreSQL 16, REPLICATION, BYPASSRLS and
--   CREATEDB need the owner to hold that attribute itself, and a membership can be revoked only with ADMIN OPTION on
--   the role it is in. Before 16, REPLICATION and BYPASSRLS need a superuser. SUPERUSER always needs a superuser,
--   and a role that has SUPERUSER, or before 16 REPLICATION, has already stopped the run at the guard. What remains
--   draws a WARNING, and the run carries on. Any other error (a lock or statement timeout, a concurrent change to a
--   role) stops the run: the attributes and the memberships are two blocks, so a failure in one never undoes the
--   other, and the stop is on for both, so psql does not carry on past a role that kept an attribute or a membership.
\set ON_ERROR_STOP on
DO $$
DECLARE
   as_superuser boolean := (SELECT rolsuper FROM pg_roles WHERE rolname = current_user);
   item record;
   attribute text;
BEGIN
   FOR item IN
      SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
      FROM pg_roles
      WHERE rolname IN ('admin', 'viewer', 'web', 'mcp')
      ORDER BY rolname
   LOOP
      IF as_superuser THEN
         EXECUTE format('ALTER ROLE %I NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS', item.rolname);
      ELSE
         FOREACH attribute IN ARRAY array_remove(ARRAY[
               CASE WHEN item.rolsuper THEN 'SUPERUSER' END,
               CASE WHEN item.rolcreaterole THEN 'CREATEROLE' END,
               CASE WHEN item.rolcreatedb THEN 'CREATEDB' END,
               CASE WHEN item.rolreplication THEN 'REPLICATION' END,
               CASE WHEN item.rolbypassrls THEN 'BYPASSRLS' END], NULL)
         LOOP
            BEGIN
               EXECUTE format('ALTER ROLE %I NO%s', item.rolname, attribute);
            EXCEPTION WHEN insufficient_privilege THEN
               RAISE WARNING 'Role "%" still has the % attribute: switching it off needs a superuser, or, from PostgreSQL 16, an owner that holds % itself.', item.rolname, attribute, attribute;
            END;
         END LOOP;
      END IF;
   END LOOP;
END $$;

DO $$
DECLARE
   as_superuser boolean := (SELECT rolsuper FROM pg_roles WHERE rolname = current_user);
   item record;
   remaining text;
BEGIN
   FOR item IN
      SELECT granted.rolname AS granted, member.rolname AS member, grantor.rolname AS grantor
      FROM pg_auth_members AS membership
      JOIN pg_roles AS granted ON granted.oid = membership.roleid
      JOIN pg_roles AS member ON member.oid = membership.member
      LEFT JOIN pg_roles AS grantor ON grantor.oid = membership.grantor
      WHERE member.rolname IN ('admin', 'viewer', 'web', 'mcp')
   LOOP
      BEGIN
         EXECUTE format('REVOKE %I FROM %I', item.granted, item.member)
            || CASE WHEN (as_superuser OR current_setting('server_version_num')::int >= 160000) AND item.grantor IS NOT NULL
                    THEN format(' GRANTED BY %I', item.grantor) ELSE '' END
            || ' CASCADE';
      EXCEPTION WHEN insufficient_privilege THEN
         RAISE WARNING 'Role "%" is still a member of "%": revoking that needs a superuser, or ADMIN OPTION on "%" and the right to act as "%", the role that granted it.', item.member, item.granted, item.granted, item.grantor;
      END;
   END LOOP;
   -- Nothing that outranks the grants below may be left. From PostgreSQL 16 a revoke removes only the row recorded
   -- with the grantor it names and warns, without an error, when it finds none, so the memberships are read again
   -- here and a voiding one that is still there stops the run (exit 3 under the stop above) before any grant.
   SELECT string_agg(format('%I in %I', member.rolname, granted.rolname), ', ' ORDER BY member.rolname, granted.rolname)
   INTO remaining
   FROM pg_auth_members AS membership
   JOIN pg_roles AS granted ON granted.oid = membership.roleid
   JOIN pg_roles AS member ON member.oid = membership.member
   WHERE member.rolname IN ('admin', 'viewer', 'web', 'mcp')
     AND EXISTS (
        SELECT 1
        FROM pg_roles AS voiding
        WHERE (voiding.rolsuper
               OR voiding.rolbypassrls
               OR voiding.oid IN ((SELECT oid FROM pg_roles WHERE rolname = current_user), (SELECT datdba FROM pg_database WHERE datname = current_database()))
               OR voiding.rolname IN ('pg_read_all_data', 'pg_write_all_data', 'pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program'))
          AND pg_has_role(granted.oid, voiding.oid, 'MEMBER'));
   IF remaining IS NOT NULL THEN
      RAISE EXCEPTION 'These roles are still members of roles whose privileges outrank the grants below: %. The run revoked every membership it could, and these remain, so it stopped before any grant. The passwords above were already set. Revoke them as a superuser (REVOKE <role> FROM <member> GRANTED BY <grantor>, as recorded in pg_auth_members), then run this script again.', remaining;
   END IF;
END $$;
\set ON_ERROR_STOP :darling_saved_stop

-- 1a. statement_timeout backstop on viewer and mcp (Custom Views v2, #1563): the web dashboard's compose surface
--     and the MCP custom-view tools run network-reachable aggregations over a raw, no-rollup store, so this caps
--     a runaway query at the database (a LIMIT bounds output, not work). Keep this value in step with
--     ComposeLimits.StatementTimeout in the service. It bounds the web dashboard and the MCP server only when
--     postgres.webConnectionString / postgres.mcpConnectionString name these roles (#3914); a surface left on
--     the owner login has no backstop at all.
ALTER ROLE viewer SET statement_timeout = '60s';
ALTER ROLE web    SET statement_timeout = '60s';
ALTER ROLE mcp    SET statement_timeout = '60s';

--     temp_file_limit backstop on viewer and mcp (#4605): the on-disk-spill half of the same DoS control --
--     a runaway aggregation that would spill past this many bytes of on-disk temp files is cancelled by the
--     store itself (SQLSTATE 53400) rather than writing gigabytes to the store's own volume and starving the
--     collector's writes. Keep this value in step with ComposeLimits.TempFileLimit in the service.
--     Superuser-only (PGC_SUSET), like log_min_duration_statement below -- if the OWNER role running this
--     script is not a superuser and has not been GRANTed SET on this parameter, these two statements fail and
--     psql carries on with the rest of the script (only the role-collision guard in step 1 stops the run), so viewer
--     and mcp are left without this limit; grant the owner SET on it and run the script again (it is idempotent),
--     or run these two lines as a superuser.
ALTER ROLE viewer SET temp_file_limit = '1GB';
ALTER ROLE web    SET temp_file_limit = '1GB';
ALTER ROLE mcp    SET temp_file_limit = '1GB';

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
ALTER ROLE web    SET log_min_duration_statement = '5000ms';
ALTER ROLE mcp    SET log_min_duration_statement = '5000ms';
ALTER ROLE viewer SET log_parameter_max_length = 0;
ALTER ROLE web    SET log_parameter_max_length = 0;
ALTER ROLE mcp    SET log_parameter_max_length = 0;

-- 2. Schema usage + SELECT on everything that exists now (ALL TABLES covers tables AND views). collect
--    holds no secrets, so admin+viewer read all of it. config: admin (the writer, and the Settings
--    window's identity) reads every column; viewer reads all config tables too -- MINUS the secret columns
--    carved in step 2b. mcp gets viewer's read surface in its own statements, carved the same way.
--    Steps 2 to 2d are ONE transaction (managed mode sends the same statements as one batch, which is one
--    implicit transaction): the blanket config grants below give viewer, web and mcp table-wide SELECT on the secret
--    columns, and on the password key tables (step 2d), until the carve and the revoke take it back, and committed on
--    their own they would leave that window open on every run, and for good if a statement in them failed. Now they
--    commit together or not at all, so an error anywhere in them leaves the read roles with no read access, never a
--    blanket grant. psql rolls the
--    transaction back at the COMMIT after an error, with ON_ERROR_STOP off, and at exit with it on.
BEGIN;
GRANT USAGE ON SCHEMA collect, config TO admin, viewer, web;
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer, web;
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO admin, viewer, web;
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
              remediation_username,
              -- V169 (#5452): the AWS role a target uses, and whether an external ID is stored with it.
              -- Non-secret; the external ID itself (aws_external_id) is deliberately NOT granted.
              aws_role_arn, aws_external_id_set)
    ON config.config_monitored_servers TO viewer;
REVOKE SELECT ON config.config_command FROM viewer;
GRANT SELECT (command_id, created_at, requested_by, command_type, target_server_id, status, claimed_at,
              completed_at, result_status, result_json, service_instance)
    ON config.config_command TO viewer;
REVOKE SELECT ON config.config_notification FROM viewer;
GRANT SELECT (id, smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients,
              email_cooldown_minutes, teams_proxy, slack_proxy, modified_at,
              generic_body_template, generic_proxy, pagerduty_use_eu_region, pagerduty_proxy,
              -- V166: pagerduty_auto_resolve. Non-secret (a behaviour toggle, like the EU-region flag
              -- beside it), so it stays granted.
              pagerduty_auto_resolve)
    ON config.config_notification TO viewer;
-- V131 (#3598): the sparse notification-routes table mirrors the parent row's destination columns under the
-- same names, so the same carve applies: the webhook URLs and the PagerDuty routing key are bearer secrets and
-- are deliberately NOT granted. configured_channels is a GENERATED presence column (which channels a route
-- sets, never their values) that exists so this carve can still answer "what does route 3 configure".
-- Created by V131, so re-run this script AFTER the service has migrated your store to V131. On a store below V131
-- the pair below is skipped (the table does not exist yet), so the rest of the carve still commits; unguarded, its
-- error would abort the transaction and roll back the grants above with it. It runs as EXECUTE of a dollar-quoted
-- string, so the statements are sent only when the table exists. If they fail on a store that has the table, the
-- whole transaction rolls back and the read roles keep no access. The five agent_* route columns are V184 (#5632):
-- their grant runs only when agent_level exists, so on a V181-V183 store the carve still commits without them.
DO $$
BEGIN
   IF to_regclass('config.config_notification_routes') IS NOT NULL THEN
      EXECUTE $v131$
REVOKE SELECT ON config.config_notification_routes FROM viewer;
GRANT SELECT (route_id, metric_match, smtp_recipients, configured_channels, enabled, modified_at, scope_server_id, scope_tag_id)
    ON config.config_notification_routes TO viewer;
$v131$;
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'agent_level' AND NOT attisdropped) THEN
         EXECUTE $v184$
GRANT SELECT (agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes)
    ON config.config_notification_routes TO viewer;
$v184$;
      END IF;
      -- V187 (#5825): a route's override of the channels' "send resolutions" flags. Non-secret. Granted here, inside
      -- the routes table's guard and on its own column, so a store without the table or below V187 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'send_resolutions' AND NOT attisdropped) THEN
         EXECUTE $v187$
GRANT SELECT (send_resolutions) ON config.config_notification_routes TO viewer;
$v187$;
      END IF;
      -- V229 (#6928): a route's severity filter. Non-secret. Granted on its own column inside the routes table's guard, so a
      -- store below V229 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'severities' AND NOT attisdropped) THEN
         EXECUTE $v229$
GRANT SELECT (severities) ON config.config_notification_routes TO viewer;
$v229$;
      END IF;
      -- V233 (#6979): a route's report notice setting. Non-secret. Granted on its own column inside the routes table's guard, so a
      -- store below V233 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'report_notice' AND NOT attisdropped) THEN
         EXECUTE $v233$
GRANT SELECT (report_notice) ON config.config_notification_routes TO viewer;
$v233$;
      END IF;
   END IF;
END $$;
-- V187 (#5825): the four per-channel "send resolutions" flags. Non-secret (behaviour toggles, like
-- pagerduty_auto_resolve), so they are granted. Guarded on the column, so a store below V187 still commits the rest
-- of the carve. This block names config_notification only: a grant on the routes table here, outside that table's
-- guard, failed on a store without it and rolled the whole carve back (#5855 review).
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 'config.config_notification'::regclass
                AND attname = 'slack_send_resolutions' AND NOT attisdropped) THEN
      EXECUTE $v187$
GRANT SELECT (slack_send_resolutions, teams_send_resolutions, generic_send_resolutions, email_send_resolutions)
    ON config.config_notification TO viewer;
$v187$;
   END IF;
END $$;
-- V193 (#5997): when the previous webhook signing key stops being sent, on the settings row, and on a route that and the
-- fallback route. Non-secret. The signing keys (signing_key, previous_signing_key) are deliberately NOT granted: whoever
-- holds one can forge a signed send. Guarded on the column, so a store below V193 (or without the routes table) still
-- commits the rest of the carve.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification')
                AND attname = 'previous_signing_key_until' AND NOT attisdropped) THEN
      EXECUTE $v193$
GRANT SELECT (previous_signing_key_until) ON config.config_notification TO viewer;
$v193$;
   END IF;
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification_routes')
                AND attname = 'fallback_route_id' AND NOT attisdropped) THEN
      EXECUTE $v193r$
GRANT SELECT (fallback_route_id, previous_signing_key_until) ON config.config_notification_routes TO viewer;
$v193r$;
   END IF;
END $$;
-- V203 (#6170): the hypothetical-index login's NAME, on the registry. Non-secret, like remediation_username; the password
-- column (hypothetical_index_encrypted_password) is deliberately NOT granted. Guarded on the column, so a store below V203
-- still commits the rest of the carve instead of failing on a column it does not have yet.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_monitored_servers')
                AND attname = 'hypothetical_index_username' AND NOT attisdropped) THEN
      EXECUTE $v203$
GRANT SELECT (hypothetical_index_username) ON config.config_monitored_servers TO viewer;
$v203$;
   END IF;
END $$;
-- V213 (#6254): the Query Store replica HOST, on the registry. Non-secret, like host. Guarded on the column, so a store below V213
-- still commits the rest of the carve instead of failing on a column it does not have yet.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_monitored_servers')
                AND attname = 'query_store_replica' AND NOT attisdropped) THEN
      EXECUTE $v211r$
GRANT SELECT (query_store_replica) ON config.config_monitored_servers TO viewer;
$v211r$;
   END IF;
END $$;

-- 2b-2. The SAME carve for web: it reads exactly what viewer reads (the web host connects as it, not as viewer), so the column
--     lists are the 2b lists with the role swapped, and a column added to one is added to the other.
REVOKE SELECT ON config.config_monitored_servers FROM web;
GRANT SELECT (server_id, name, host, database, auth, username, encrypt_mode, trust_server_certificate,
              read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans,
              is_enabled, created_at, modified_at, alert_delivery_mode_override,
              -- V68: engine + port. Non-secret, exactly like host.
              engine, port,
              -- V107 (#2138): the force-plan bot's per-server arm state. Non-secret, exactly like is_enabled.
              plan_force_bot_enabled,
              -- V113 (#2138 phase 1): the remediation credential's login name. Non-secret, exactly like
              -- username; remediation_encrypted_password is deliberately NOT granted.
              remediation_username,
              -- V169 (#5452): the AWS role a target uses, and whether an external ID is stored with it.
              -- Non-secret; the external ID itself (aws_external_id) is deliberately NOT granted.
              aws_role_arn, aws_external_id_set)
    ON config.config_monitored_servers TO web;
REVOKE SELECT ON config.config_command FROM web;
GRANT SELECT (command_id, created_at, requested_by, command_type, target_server_id, status, claimed_at,
              completed_at, result_status, result_json, service_instance)
    ON config.config_command TO web;
REVOKE SELECT ON config.config_notification FROM web;
GRANT SELECT (id, smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients,
              email_cooldown_minutes, teams_proxy, slack_proxy, modified_at,
              generic_body_template, generic_proxy, pagerduty_use_eu_region, pagerduty_proxy,
              -- V166: pagerduty_auto_resolve. Non-secret (a behaviour toggle, like the EU-region flag
              -- beside it), so it stays granted.
              pagerduty_auto_resolve)
    ON config.config_notification TO web;
-- V131 (#3598): the sparse notification-routes table mirrors the parent row's destination columns under the
-- same names, so the same carve applies: the webhook URLs and the PagerDuty routing key are bearer secrets and
-- are deliberately NOT granted. configured_channels is a GENERATED presence column (which channels a route
-- sets, never their values) that exists so this carve can still answer "what does route 3 configure".
-- Created by V131, so re-run this script AFTER the service has migrated your store to V131. On a store below V131
-- the pair below is skipped (the table does not exist yet), so the rest of the carve still commits; unguarded, its
-- error would abort the transaction and roll back the grants above with it. It runs as EXECUTE of a dollar-quoted
-- string, so the statements are sent only when the table exists. If they fail on a store that has the table, the
-- whole transaction rolls back and the read roles keep no access. The five agent_* route columns are V184 (#5632):
-- their grant runs only when agent_level exists, so on a V181-V183 store the carve still commits without them.
DO $$
BEGIN
   IF to_regclass('config.config_notification_routes') IS NOT NULL THEN
      EXECUTE $v131$
REVOKE SELECT ON config.config_notification_routes FROM web;
GRANT SELECT (route_id, metric_match, smtp_recipients, configured_channels, enabled, modified_at, scope_server_id, scope_tag_id)
    ON config.config_notification_routes TO web;
$v131$;
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'agent_level' AND NOT attisdropped) THEN
         EXECUTE $v184$
GRANT SELECT (agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes)
    ON config.config_notification_routes TO web;
$v184$;
      END IF;
      -- V187 (#5825): a route's override of the channels' "send resolutions" flags. Non-secret. Granted here, inside
      -- the routes table's guard and on its own column, so a store without the table or below V187 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'send_resolutions' AND NOT attisdropped) THEN
         EXECUTE $v187$
GRANT SELECT (send_resolutions) ON config.config_notification_routes TO web;
$v187$;
      END IF;
      -- V229 (#6928): a route's severity filter. Non-secret. Granted on its own column inside the routes table's guard, so a
      -- store below V229 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'severities' AND NOT attisdropped) THEN
         EXECUTE $v229$
GRANT SELECT (severities) ON config.config_notification_routes TO web;
$v229$;
      END IF;
      -- V233 (#6979): a route's report notice setting. Non-secret. Granted on its own column inside the routes table's guard, so a
      -- store below V233 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'report_notice' AND NOT attisdropped) THEN
         EXECUTE $v233$
GRANT SELECT (report_notice) ON config.config_notification_routes TO web;
$v233$;
      END IF;
   END IF;
END $$;
-- V187 (#5825): the four per-channel "send resolutions" flags. Non-secret (behaviour toggles, like
-- pagerduty_auto_resolve), so they are granted. Guarded on the column, so a store below V187 still commits the rest
-- of the carve. This block names config_notification only: a grant on the routes table here, outside that table's
-- guard, failed on a store without it and rolled the whole carve back.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 'config.config_notification'::regclass
                AND attname = 'slack_send_resolutions' AND NOT attisdropped) THEN
      EXECUTE $v187$
GRANT SELECT (slack_send_resolutions, teams_send_resolutions, generic_send_resolutions, email_send_resolutions)
    ON config.config_notification TO web;
$v187$;
   END IF;
END $$;
-- V193 (#5997): when the previous webhook signing key stops being sent, on the settings row, and on a route that and the
-- fallback route. Non-secret. The signing keys (signing_key, previous_signing_key) are deliberately NOT granted: whoever
-- holds one can sign a send as that route. Guarded on the column, so a store below V193 (or without the routes table) still
-- commits the rest of the carve.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification')
                AND attname = 'previous_signing_key_until' AND NOT attisdropped) THEN
      EXECUTE $v193$
GRANT SELECT (previous_signing_key_until) ON config.config_notification TO web;
$v193$;
   END IF;
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification_routes')
                AND attname = 'fallback_route_id' AND NOT attisdropped) THEN
      EXECUTE $v193r$
GRANT SELECT (fallback_route_id, previous_signing_key_until) ON config.config_notification_routes TO web;
$v193r$;
   END IF;
END $$;
-- V203 (#6170): the hypothetical-index login's NAME, on the registry. Non-secret, like remediation_username; the password
-- column (hypothetical_index_encrypted_password) is deliberately NOT granted. Guarded on the column, so a store below V203
-- still commits the rest of the carve instead of failing on a column it does not have yet.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_monitored_servers')
                AND attname = 'hypothetical_index_username' AND NOT attisdropped) THEN
      EXECUTE $v203$
GRANT SELECT (hypothetical_index_username) ON config.config_monitored_servers TO web;
$v203$;
   END IF;
END $$;
-- V213 (#6254): the Query Store replica HOST, on the registry. Non-secret, like host. Guarded on the column, so a store below V213
-- still commits the rest of the carve instead of failing on a column it does not have yet.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_monitored_servers')
                AND attname = 'query_store_replica' AND NOT attisdropped) THEN
      EXECUTE $v211r$
GRANT SELECT (query_store_replica) ON config.config_monitored_servers TO web;
$v211r$;
   END IF;
END $$;

-- 2c. The SAME carve for mcp (#3914): it reads exactly what viewer reads, so it is denied exactly the same
--     secret columns -- the column lists are identical to 2b's, and the drift test holds both to the C# list.
REVOKE SELECT ON config.config_monitored_servers FROM mcp;
GRANT SELECT (server_id, name, host, database, auth, username, encrypt_mode, trust_server_certificate,
              read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans,
              is_enabled, created_at, modified_at, alert_delivery_mode_override,
              engine, port,
              plan_force_bot_enabled,
              remediation_username,
              aws_role_arn, aws_external_id_set)
    ON config.config_monitored_servers TO mcp;
REVOKE SELECT ON config.config_command FROM mcp;
-- (#6230) result_json is withheld from mcp: it holds the result of every command type, including the Viewer-only ones.
--     mcp reads a command's result through config.read_mcp_command_result (created below), which answers only for the
--     command types the MCP tools queue. The REVOKE above removes a result_json grant an earlier run made.
GRANT SELECT (command_id, created_at, requested_by, command_type, target_server_id, status, claimed_at,
              completed_at, result_status, service_instance)
    ON config.config_command TO mcp;
REVOKE SELECT ON config.config_notification FROM mcp;
GRANT SELECT (id, smtp_host, smtp_port, smtp_use_ssl, smtp_from_address, smtp_recipients,
              email_cooldown_minutes, teams_proxy, slack_proxy, modified_at,
              generic_body_template, generic_proxy, pagerduty_use_eu_region, pagerduty_proxy,
              -- V166: pagerduty_auto_resolve. Non-secret (a behaviour toggle, like the EU-region flag
              -- beside it), so it stays granted.
              pagerduty_auto_resolve)
    ON config.config_notification TO mcp;
-- V131 again, guarded the same way (see 2b).
DO $$
BEGIN
   IF to_regclass('config.config_notification_routes') IS NOT NULL THEN
      EXECUTE $v131$
REVOKE SELECT ON config.config_notification_routes FROM mcp;
GRANT SELECT (route_id, metric_match, smtp_recipients, configured_channels, enabled, modified_at, scope_server_id, scope_tag_id)
    ON config.config_notification_routes TO mcp;
$v131$;
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'agent_level' AND NOT attisdropped) THEN
         EXECUTE $v184$
GRANT SELECT (agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes)
    ON config.config_notification_routes TO mcp;
$v184$;
      END IF;
      -- V187 (#5825): a route's override of the channels' "send resolutions" flags. Non-secret. Granted here, inside
      -- the routes table's guard and on its own column, so a store without the table or below V187 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'send_resolutions' AND NOT attisdropped) THEN
         EXECUTE $v187$
GRANT SELECT (send_resolutions) ON config.config_notification_routes TO mcp;
$v187$;
      END IF;
      -- V229 (#6928): a route's severity filter. Non-secret. Granted on its own column inside the routes table's guard, so a
      -- store below V229 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'severities' AND NOT attisdropped) THEN
         EXECUTE $v229$
GRANT SELECT (severities) ON config.config_notification_routes TO mcp;
$v229$;
      END IF;
      -- V233 (#6979): a route's report notice setting. Non-secret. Granted on its own column inside the routes table's guard, so a
      -- store below V233 still commits.
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = 'config.config_notification_routes'::regclass
                   AND attname = 'report_notice' AND NOT attisdropped) THEN
         EXECUTE $v233$
GRANT SELECT (report_notice) ON config.config_notification_routes TO mcp;
$v233$;
      END IF;
   END IF;
END $$;
-- V187 (#5825): the four per-channel "send resolutions" flags. Non-secret (behaviour toggles, like
-- pagerduty_auto_resolve), so they are granted. Guarded on the column, so a store below V187 still commits the rest
-- of the carve. This block names config_notification only: a grant on the routes table here, outside that table's
-- guard, failed on a store without it and rolled the whole carve back (#5855 review).
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 'config.config_notification'::regclass
                AND attname = 'slack_send_resolutions' AND NOT attisdropped) THEN
      EXECUTE $v187$
GRANT SELECT (slack_send_resolutions, teams_send_resolutions, generic_send_resolutions, email_send_resolutions)
    ON config.config_notification TO mcp;
$v187$;
   END IF;
END $$;
-- V193 (#5997): when the previous webhook signing key stops being sent, on the settings row, and on a route that and the
-- fallback route. Non-secret. The signing keys (signing_key, previous_signing_key) are deliberately NOT granted: whoever
-- holds one can forge a signed send. Guarded on the column, so a store below V193 (or without the routes table) still
-- commits the rest of the carve.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification')
                AND attname = 'previous_signing_key_until' AND NOT attisdropped) THEN
      EXECUTE $v193$
GRANT SELECT (previous_signing_key_until) ON config.config_notification TO mcp;
$v193$;
   END IF;
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification_routes')
                AND attname = 'fallback_route_id' AND NOT attisdropped) THEN
      EXECUTE $v193r$
GRANT SELECT (fallback_route_id, previous_signing_key_until) ON config.config_notification_routes TO mcp;
$v193r$;
   END IF;
END $$;
-- V203 (#6170): the hypothetical-index login's NAME, on the registry. Non-secret, like remediation_username; the password
-- column (hypothetical_index_encrypted_password) is deliberately NOT granted. Guarded on the column, so a store below V203
-- still commits the rest of the carve instead of failing on a column it does not have yet.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_monitored_servers')
                AND attname = 'hypothetical_index_username' AND NOT attisdropped) THEN
      EXECUTE $v203$
GRANT SELECT (hypothetical_index_username) ON config.config_monitored_servers TO mcp;
$v203$;
   END IF;
END $$;
-- V213 (#6254): the Query Store replica HOST, on the registry. Non-secret, like host. Guarded on the column, so a store below V213
-- still commits the rest of the carve instead of failing on a column it does not have yet.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_monitored_servers')
                AND attname = 'query_store_replica' AND NOT attisdropped) THEN
      EXECUTE $v211r$
GRANT SELECT (query_store_replica) ON config.config_monitored_servers TO mcp;
$v211r$;
   END IF;
END $$;
-- The collector run times (V160, #4938): get_collection_health reads them as mcp. The blanket config SELECT above
-- already covers the table when it exists at that point; naming it makes the grant explicit and idempotent, mirroring
-- how the schedule tables are read, so a store that reaches V160 after an earlier provisioning is covered by the re-run.
-- Guarded like the V131 pair above: on a store below V160 the table does not exist, and an unguarded GRANT would abort
-- the transaction and roll back every grant before it.
DO $$
BEGIN
   IF to_regclass('config.config_collector_run_times') IS NOT NULL THEN
      EXECUTE $v160$
GRANT SELECT ON config.config_collector_run_times TO mcp;
$v160$;
   END IF;
END $$;

-- 2d. The password key tables (V165 and V167, #5366), still inside the transaction. The blanket config grants above gave
--     SELECT on them to admin, viewer, web and mcp, and the COMMIT below makes whatever is granted by then durable, so the
--     revoke is in effect at the COMMIT: every privilege on each of these tables that exists is taken from PUBLIC and
--     the four roles, and SELECT on the key and service-state tables goes back to admin alone, the same result as
--     managed mode's batch. Step 3g repeats it after the grants of step 3, which give admin write access, and also
--     removes triggers and foreign keys, so a stop anywhere between here and 3g never leaves viewer, web or mcp able to read
--     them. The tables are looked up one at a time: a store below V165 has none, a store at V165 or V166 has no
--     candidate table.
DO $do$
DECLARE
    key_table text;
BEGIN
    FOREACH key_table IN ARRAY ARRAY['password_key', 'password_key_service', 'legacy_secret_pin', 'legacy_secret_pin_marker', 'legacy_secret_pin_candidate', 'pseudonym_key']
    LOOP
        IF pg_catalog.to_regclass(pg_catalog.format('config.%I', key_table)) IS NOT NULL THEN
            EXECUTE pg_catalog.format('REVOKE ALL ON config.%I FROM PUBLIC, admin, viewer, web, mcp CASCADE', key_table);
            IF key_table IN ('password_key', 'password_key_service') THEN
                EXECUTE pg_catalog.format('GRANT SELECT ON config.%I TO admin', key_table);
            END IF;
        END IF;
    END LOOP;
END
$do$;
COMMIT;

-- 3. config writes -- admin gets the whole schema.
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin;

-- 3b. Custom views (#1563): the web dashboard's custom-view composer writes exactly this one config table
--     (non-secret dashboard JSON) as web, and the MCP custom-view tools as mcp. Editing is any
--     authenticated seat -- the web surface's normal networked mode, gated server-side by the host's
--     token+CIDR auth (not loopback-only); this narrow grant is only the floor beneath that gate. EXPLICIT
--     single-table statements, NO ALTER DEFAULT PRIVILEGES (ADP has no per-table
--     form -> it would broaden the role to ALL of config). config.custom_views is created by the V31 migration,
--     so re-run this script AFTER the service has migrated your store to V31 (else this line errors: no table).
REVOKE INSERT, UPDATE, DELETE ON config.custom_views FROM viewer;
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO web;
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO mcp;

-- 3c. Database-state expected states (#1986): the web dashboard's per-database override editor writes this one
--     config table (non-secret; the expected/(ignore) state per database). SELECT is already covered by
--     the blanket config grant in step 2; this is the write floor for the editor, gated server-side by the
--     same token+CIDR auth as the custom-view composer. EXPLICIT single-table statement (no ALTER DEFAULT
--     PRIVILEGES, which would broaden viewer to ALL of config). config.database_state_expected is created by
--     the V49 migration, so re-run this script AFTER the service has migrated your store to V49.
REVOKE INSERT, UPDATE, DELETE ON config.database_state_expected FROM viewer;
GRANT INSERT, UPDATE, DELETE ON config.database_state_expected TO web;

-- 3d. Mute rules (#3450): the web dashboard's dedicated mute-rule endpoints (create / update / set-enabled /
--     delete under /api/mute-rules) run as web, and the MCP mute-rule tools as mcp, so both get the same
--     single-table write shape as 3b. The table exists from V3, but its reload-beacon trigger
--     (trg_bump_mute_rules, V117) is SECURITY INVOKER and UPDATEs config_service.config_version AS the caller
--     on every mute-rule write -- so the COLUMN-level config_service grants below are load-bearing, not
--     optional: without them every mute-rule write fails 42501 at the trigger. Column-level on exactly the
--     two beacon columns, so neither role can flip a service flag like paused. Every other mcp-writable table
--     with a bump trigger (config_alert_settings, config_notification, config_notification_routes,
--     config_monitored_servers) rides the same mcp grant. Re-run this script after upgrading to V117+.
--     (The WPF Viewer's read-only probe discriminates on config_alert_log UPDATE -- a write viewer never
--     gets -- so a connectAs = "viewer" Viewer stays read-only in its UI despite this grant.)
REVOKE INSERT, UPDATE, DELETE ON config.config_mute_rules FROM viewer;
GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO web;
REVOKE UPDATE ON config.config_service FROM viewer;
GRANT UPDATE (config_version, updated_at) ON config.config_service TO web;
GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO mcp;
GRANT UPDATE (config_version, updated_at) ON config.config_service TO mcp;
-- #6230: pause_collection / resume_collection queue a pause or resume command through config.request_mcp_command, so mcp holds no
--     write on the paused column. An earlier build granted it; this revoke takes it back from an upgraded store.
REVOKE UPDATE (paused) ON config.config_service FROM mcp;
-- #6393: update_service_flags runs as mcp and sets exactly these five service flags (plan capture, Query Store backfill, text budget, max concurrent
--    sweeps, fetch top N): a column list, so mcp still cannot write mcp_enabled, mcp_port, web_enabled, web_port or paused (turning MCP off from MCP
--    would lock the admin out; those stay Viewer-only). The write fires the config_service self-bump trigger (BEFORE UPDATE, it assigns NEW.config_version and NEW.updated_at), so it needs no further grant.
--    Skipped, with a warning, on a store the service has not upgraded to the fetch-top-N column yet (V209); re-run this script after it has.
DO $do$
BEGIN
   IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_service' AND column_name = 'query_store_fetch_top_n') THEN
      GRANT UPDATE (capture_plans, query_store_backfill_enabled, query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n) ON config.config_service TO mcp;
   ELSE
      RAISE WARNING 'config.config_service has no query_store_fetch_top_n column yet: skipped the mcp service-flag column grant. Start the service once to migrate the store, then re-run this script.';
   END IF;
END
$do$;
-- #5085: the fleet server-tag write tools run as mcp; these two single-table grants are the admin gate. Both
--     tables are non-secret, and server_tags.id is an identity column (no sequence grant). V181 (#5750) put a
--     statement-level reload-beacon trigger on each, so a tag write bumps config_service; the config_service column grant above covers it.
GRANT INSERT, UPDATE, DELETE ON config.server_tags TO mcp;
GRANT INSERT, UPDATE, DELETE ON config.server_tag_map TO mcp;
-- #6227: the collector schedule tools (disable_collector, enable_collector, set_collector_frequency, set_collector_run_at,
--     set_collector_retention, and #6396's set_collector_databases and reset_collector_schedules) run as mcp and write the two non-secret schedule tables. Both carry a statement-level
--     reload-beacon trigger that UPDATEs config_service.config_version as the caller; the column grant above covers it.
--     The run-time table (V160) is skipped, with a warning, on a store the service has not upgraded that far.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.config_collector_schedules') IS NOT NULL THEN
      REVOKE INSERT, UPDATE, DELETE ON config.config_collector_schedules FROM mcp;
      -- #6396: the database scope (V125) joins both lists, for set_collector_databases.
      GRANT INSERT (server_id, collector_name, enabled, frequency_minutes, retention_days, databases) ON config.config_collector_schedules TO mcp;
      GRANT UPDATE (enabled, frequency_minutes, retention_days, databases) ON config.config_collector_schedules TO mcp;
      -- #6454: remove_server clears a removed server's override rows, so mcp holds DELETE on this table (managed provisioning grants it too).
      --    reset_collector_schedules (#6396) runs the same DELETE, by server_id or, at the fleet scope, with server_id IS NULL.
      GRANT DELETE ON config.config_collector_schedules TO mcp;
   ELSE
      RAISE WARNING 'config.config_collector_schedules was not found, so its mcp grant was skipped. Run this script again after the service has upgraded the store.';
   END IF;

   IF pg_catalog.to_regclass('config.config_collector_run_times') IS NOT NULL THEN
      REVOKE INSERT, UPDATE, DELETE ON config.config_collector_run_times FROM mcp;
      GRANT INSERT (server_id, collector_name, run_at_minute) ON config.config_collector_run_times TO mcp;
      GRANT UPDATE (run_at_minute) ON config.config_collector_run_times TO mcp;
      GRANT DELETE ON config.config_collector_run_times TO mcp;
   ELSE
      RAISE WARNING 'config.config_collector_run_times was not found, so its mcp grant was skipped. Run this script again after the service has upgraded the store.';
   END IF;
END $do$;

-- 3d-2. Alert History dismiss (#4843): the web dashboard's POST /api/alert-history/dismiss runs as web and
--     writes `UPDATE config_alert_log SET dismissed = TRUE`. Column-level on exactly that one column, so web
--     can flip the dismissed flag and cannot rewrite what an alert said or reach INSERT/DELETE. config_alert_log
--     carries no trigger. The WPF read-only probe (has_table_privilege ... 'UPDATE') answers for the table-level
--     privilege only, so it stays false for web and viewer. mcp gets the same one-column grant just below (#6231).
REVOKE UPDATE ON config.config_alert_log FROM viewer;
GRANT UPDATE (dismissed) ON config.config_alert_log TO web;
-- #6231: the MCP dismiss_alert_history / restore_alert_history tools run as mcp and set the same one column, so mcp
--     gets the same column-level grant (revoke first, so no table-level UPDATE lingers). No other column, no INSERT/DELETE.
REVOKE UPDATE ON config.config_alert_log FROM mcp;
GRANT UPDATE (dismissed) ON config.config_alert_log TO mcp;

-- #5085: the web dashboard's /api/server-tags endpoints run as web, so web gets the same two single-table
--     writes (the seat model decides who may call them; these grants are only the floor). The WPF Viewer's read-only
--     probe still discriminates on config_alert_log UPDATE, so a connectAs = "viewer" Viewer stays read-only.
REVOKE INSERT, UPDATE, DELETE ON config.server_tags FROM viewer;
GRANT INSERT, UPDATE, DELETE ON config.server_tags TO web;
REVOKE INSERT, UPDATE, DELETE ON config.server_tag_map FROM viewer;
GRANT INSERT, UPDATE, DELETE ON config.server_tag_map TO web;

-- #4843: the web dashboard's add-server route runs the add_servers core as web, so web gets INSERT on
--     config_monitored_servers (never DELETE: no web route removes a server). The
--     credential column stays SELECT-carved from viewer, so it can write a password blob and never read one back;
--     the write's bump trigger is served by the two config_service beacon columns granted above.
REVOKE INSERT ON config.config_monitored_servers FROM viewer;
GRANT INSERT ON config.config_monitored_servers TO web;

-- #5240: the web dashboard's edit route (PATCH /api/servers/{id}) and the MCP edit_server tool change a monitored
--     server through config.edit_monitored_server, created right below. Viewer holds NO UPDATE on the table: the
--     function checks the optimistic token, works out for itself whether host or port moves, and refuses a move that
--     keeps the stored secret on a SQL or service-principal row. RE-RUN THIS SCRIPT AFTER UPGRADING to the release that
--     adds the edit route (the precedent is the V117 note above): a store whose roles predate the function answers
--     every web edit with an error that says to re-run it. The functions end up owned by the owner of config.config_command (step 8 at the end), but the REVOKEs below only undo grants made by the role that runs them, so run it
--     as the store owner, the role that owns config.config_monitored_servers.
--     REVOKE first, like the SELECT carve above: it takes away any UPDATE an earlier release granted viewer here, and
--     revoking the table privilege also revokes every column privilege on it. A plain REVOKE removes only the privileges
--     recorded as granted by the role that issues it (a superuser or the owner counts as the owner), so run this script
--     as the same owner every time, as above.
REVOKE UPDATE ON config.config_monitored_servers FROM viewer, web;

CREATE OR REPLACE FUNCTION config.edit_monitored_server(
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
   FROM config.config_monitored_servers AS s
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

   v_has_replica := EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'config.config_monitored_servers'::regclass
                           AND attname = 'query_store_replica' AND NOT attisdropped);

   -- A value the table's own checks refuse (a required column, a format, a duplicate) answers invalid_value too:
   -- the table's error text is not passed on.
   BEGIN
      -- A store below V213 has no query_store_replica column (a script run before the service migrated it): the write leaves it out there.
      IF v_has_replica THEN
         UPDATE config.config_monitored_servers AS s
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
         UPDATE config.config_monitored_servers AS s
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
         UPDATE collect.servers SET machine_identity = NULL WHERE server_id = p_server_id;
      EXCEPTION WHEN undefined_column OR undefined_table THEN
         NULL;
      END;
   END IF;

   outcome := 'saved';
   RETURN NEXT;
END;
$fn$;
REVOKE ALL ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text, text) FROM viewer;
GRANT EXECUTE ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text, text) TO web, mcp;

-- #6254: the 17-argument edit function stays as a permanent wrapper over the 18-argument one, for a caller built before the Query
--     Store replica column. It passes NULL for the replica and strips the column name from p_columns, so a caller of the older
--     signature can neither change nor clear a replica. Same grants, same owner.
CREATE OR REPLACE FUNCTION config.edit_monitored_server(
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
   FROM config.edit_monitored_server(
      p_server_id, p_expected_modified_at,
      array_remove(COALESCE(p_columns, ARRAY[]::text[]), 'query_store_replica'),
      p_name, p_host, p_port, p_database, p_read_only_intent, p_auth, p_username, p_secret, p_encrypt_mode,
      p_trust_server_certificate, p_multi_subnet_failover, p_monthly_cost_usd, p_aws_role_arn, p_aws_external_id, NULL::text) AS e;
$fn$;
REVOKE ALL ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text) FROM viewer;
GRANT EXECUTE ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text) TO web, mcp;

-- #5452: the 15-argument edit function stays as a permanent wrapper over the 17-argument one, for a caller that
--     was built before the AWS role columns. It passes NULL for both and strips the two AWS column names from
--     p_columns, so a caller of the old signature can neither change nor clear a role. Same grants, same owner.
CREATE OR REPLACE FUNCTION config.edit_monitored_server(
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
   FROM config.edit_monitored_server(
      p_server_id, p_expected_modified_at,
      array_remove(array_remove(COALESCE(p_columns, ARRAY[]::text[]), 'aws_role_arn'), 'aws_external_id'),
      p_name, p_host, p_port, p_database, p_read_only_intent, p_auth, p_username, p_secret, p_encrypt_mode,
      p_trust_server_certificate, p_multi_subnet_failover, p_monthly_cost_usd, NULL::text, NULL::text) AS e;
$fn$;
REVOKE ALL ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric) FROM viewer;
GRANT EXECUTE ON FUNCTION config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric) TO web, mcp;

-- #6334: the known-pattern entries' writes. The web editor (web) and the Viewer (admin) write config.config_known_patterns only
--     through these three functions, and the MCP create_known_pattern / update_known_pattern / delete_known_pattern tools (#6398) call the same
--     functions as mcp; no role holds a direct INSERT/UPDATE/DELETE on the table. Same functions, same text, as managed provisioning rebuilds on every
--     start. The reads need no grant: the schema-wide SELECT covers the table. The table exists from V214, so re-run this script after upgrading. All three bodies are plpgsql (late-bound) so the script also runs on a store that has not reached V214 yet; a LANGUAGE sql body would fail the CREATE there.
CREATE OR REPLACE FUNCTION config.insert_known_pattern(
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
   IF p_server_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   SELECT count(*) INTO v_count FROM config.config_known_patterns;
   IF v_count >= 500 THEN
      RAISE EXCEPTION 'Known patterns are capped at 500 entries and % exist. Delete entries you no longer need to make room.', v_count USING ERRCODE = 'DL409';
   END IF;
   INSERT INTO config.config_known_patterns
      (server_id, metric_name, time_of_day, time_zone, window_minutes, note, created_by, expires_at_utc)
   VALUES
      (p_server_id, p_metric_name, p_time_of_day, p_time_zone, p_window_minutes, p_note, p_created_by, p_expires_at_utc)
   RETURNING id INTO v_id;
   RETURN v_id;
END;
$fn$;
REVOKE ALL ON FUNCTION config.insert_known_pattern(integer, text, time, text, integer, text, text, timestamp) FROM PUBLIC;
DROP FUNCTION IF EXISTS config.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp);
CREATE OR REPLACE FUNCTION config.update_known_pattern(
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
   IF p_server_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   UPDATE config.config_known_patterns
      SET server_id = p_server_id, metric_name = p_metric_name, time_of_day = p_time_of_day, time_zone = p_time_zone,
          window_minutes = p_window_minutes, note = p_note, expires_at_utc = p_expires_at_utc,
          updated_by = p_updated_by, updated_at_utc = pg_catalog.now() AT TIME ZONE 'UTC'
    WHERE id = p_id;
   GET DIAGNOSTICS v_rows = ROW_COUNT;
   RETURN v_rows > 0;
END;
$fn$;
REVOKE ALL ON FUNCTION config.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp, text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION config.delete_known_pattern(p_id bigint)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_rows bigint;
BEGIN
   DELETE FROM config.config_known_patterns WHERE id = p_id;
   GET DIAGNOSTICS v_rows = ROW_COUNT;
   RETURN v_rows > 0;
END;
$fn$;
REVOKE ALL ON FUNCTION config.delete_known_pattern(bigint) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.insert_known_pattern(integer, text, time, text, integer, text, text, timestamp) FROM viewer;
GRANT EXECUTE ON FUNCTION config.insert_known_pattern(integer, text, time, text, integer, text, text, timestamp) TO admin, web;
GRANT EXECUTE ON FUNCTION config.insert_known_pattern(integer, text, time, text, integer, text, text, timestamp) TO mcp;
REVOKE EXECUTE ON FUNCTION config.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp, text) FROM viewer;
GRANT EXECUTE ON FUNCTION config.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp, text) TO admin, web;
GRANT EXECUTE ON FUNCTION config.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp, text) TO mcp;
REVOKE EXECUTE ON FUNCTION config.delete_known_pattern(bigint) FROM viewer;
GRANT EXECUTE ON FUNCTION config.delete_known_pattern(bigint) TO admin, web;
GRANT EXECUTE ON FUNCTION config.delete_known_pattern(bigint) TO mcp;

-- #5941: the run-collectors-now request. The web route (web) and the MCP run_collectors_now tool (mcp) queue a
--     snapshot_now command through config.request_snapshot_now, the only way those two roles write a command row.
--     The command type is fixed in the body; the caller supplies a server id and an optional list of collector names
--     and nothing else. Same function, same text, as managed provisioning rebuilds on every start.
CREATE OR REPLACE FUNCTION config.request_snapshot_now(
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
   IF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   IF p_args_json IS NOT NULL THEN
      v_args := p_args_json::jsonb;
      IF jsonb_typeof(v_args) IS DISTINCT FROM 'object'
         OR EXISTS (SELECT 1 FROM jsonb_object_keys(v_args) AS k(key) WHERE k.key <> 'collectors')
         OR jsonb_typeof(v_args -> 'collectors') IS DISTINCT FROM 'array'
         OR jsonb_array_length(v_args -> 'collectors') > 200
         OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_args -> 'collectors') AS e(value)
                    WHERE jsonb_typeof(e.value) <> 'string' OR length(e.value #>> '{}') > 100) THEN
         RAISE EXCEPTION 'The only argument is a list of collector names.' USING ERRCODE = 'DL400';
      END IF;
   END IF;
   -- The same guard the service applies (#5941 review): one run at a time per server, and not within 60 seconds of the last
   -- request for it, so a holder of the web or mcp login cannot queue rows without limit. One lock per server makes the
   -- check and the insert one step.
   -- Never blocks (provisioning rule): a request that holds the lock for this server is a concurrent request, which is the
   -- same conflict as a pending row, so this refuses with the same DL409 instead of waiting.
   IF NOT pg_try_advisory_xact_lock(5941, p_server_id) THEN
      RAISE EXCEPTION 'A collector run for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM config.config_command AS c
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
   INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES (left(COALESCE(p_requested_by, 'web'), 200), 'snapshot_now', p_server_id, v_args, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION config.request_snapshot_now(integer, text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.request_snapshot_now(integer, text, text) FROM viewer;
GRANT EXECUTE ON FUNCTION config.request_snapshot_now(integer, text, text) TO web, mcp;

-- #6386: the web Run live now button on the Active Queries panel. The web route (web) queues a fetch_active_queries command
--     through config.request_active_queries_live, the only way web writes that command row. The command type is fixed in the
--     body and the caller supplies a server id and nothing else. Same function, same text, as managed provisioning rebuilds on every start.
CREATE OR REPLACE FUNCTION config.request_active_queries_live(
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
   IF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   -- Never blocks (provisioning rule): a request that holds the lock for this server is a concurrent request, which is the same
   -- conflict as a pending row, so this refuses with DL409 instead of waiting.
   IF NOT pg_try_advisory_xact_lock(6386, p_server_id) THEN
      RAISE EXCEPTION 'A live active-queries read for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM config.config_command AS c
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
   DELETE FROM config.config_command
   WHERE command_type = 'fetch_active_queries' AND target_server_id = p_server_id
     AND status IN ('succeeded', 'failed')
     AND completed_at < (now() AT TIME ZONE 'UTC') - interval '1 hour';
   INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES (left(COALESCE(p_requested_by, 'web'), 200), 'fetch_active_queries', p_server_id, NULL, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION config.request_active_queries_live(integer, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.request_active_queries_live(integer, text) FROM viewer;
GRANT EXECUTE ON FUNCTION config.request_active_queries_live(integer, text) TO web;
-- #6389: the web's Generate now button on the Recommendations tab. The web route (web) queues an analyze_now command through
--     config.request_analyze_now, the only way web writes that command row. The command type is fixed in the body and the
--     caller supplies a server id and a requested_by label (the label is not checked, and the web's log line is the record of who asked). One request per server at a time, not within 15 seconds of
--     the last, under the same advisory lock key the MCP analyze_now takes. Same function, same text, as managed provisioning
--     rebuilds on every start.
CREATE OR REPLACE FUNCTION config.request_analyze_now(
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
   IF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   -- Never blocks (provisioning rule): a request that holds the lock for this server is a concurrent request, which is the same
   -- conflict as a pending row, so this refuses with DL409 instead of waiting. The key is the one config.request_mcp_command takes
   -- for an analyze_now on this server, so a web click and an MCP call for the same server share one guard.
   IF NOT pg_try_advisory_xact_lock(6230, hashtext('analyze_now:' || p_server_id::text)) THEN
      RAISE EXCEPTION 'An analysis run for this server is already queued or running.' USING ERRCODE = 'DL409';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM config.config_command AS c
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
   INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES (left(COALESCE(p_requested_by, 'web'), 200), 'analyze_now', p_server_id, NULL, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION config.request_analyze_now(integer, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION config.request_analyze_now(integer, text) FROM viewer;
GRANT EXECUTE ON FUNCTION config.request_analyze_now(integer, text) TO web;
-- #6231: the MCP route tools. mcp holds no INSERT on config.config_notification_routes and no SELECT or UPDATE on its
--     destination columns, and gets none here: create_notification_route and update_notification_route reach the table only
--     through these functions. create leaves every channel empty and update writes the match, scope, enabled flag, resolution
--     override and fallback (the match or scope only on a route with no explicit agent setting); neither takes a destination.
--     set_notification_route_destinations is the one function that sets a route's own destinations: the service seals each
--     webhook value for the route's own row first and passes it in, and the function stores only an empty value or a sealed one
--     (the address list is plain text). All three re-read the row FOR UPDATE and refuse an agent route inside the transaction,
--     unless the actor the service passes is an operator-class token (#6542 part 2). Granted to mcp only.
BEGIN;
-- #6928 (V229): p_severities joins the argument list, so the six-argument function is dropped and stays uncallable. NULL is both.
DROP FUNCTION IF EXISTS config.create_notification_route(text, boolean, integer, integer, boolean, integer);
-- #6979 (V233): p_report_notice joins the argument list, so the seven-argument function is dropped and stays uncallable. NULL is off.
DROP FUNCTION IF EXISTS config.create_notification_route(text, boolean, integer, integer, boolean, integer, text);
CREATE OR REPLACE FUNCTION config.create_notification_route(
   p_metric_match text,
   p_enabled boolean,
   p_scope_server_id integer,
   p_scope_tag_id integer,
   p_send_resolutions boolean,
   p_fallback_route_id integer,
   p_severities text DEFAULT NULL,
   p_report_notice text DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_match text := btrim(COALESCE(p_metric_match, ''));
   v_severities text := lower(btrim(COALESCE(p_severities, 'both')));
   v_report_notice text := lower(btrim(COALESCE(p_report_notice, 'off')));
   v_id integer;
BEGIN
   IF v_severities NOT IN ('both', 'critical', 'warning') THEN
      RAISE EXCEPTION 'severities is critical, warning or both.' USING ERRCODE = 'DL400';
   END IF;
   IF v_report_notice NOT IN ('off', 'needs_person_or_change', 'all') THEN
      RAISE EXCEPTION 'report_notice is off, needs_person_or_change or all.' USING ERRCODE = 'DL400';
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
      AND NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_scope_server_id) THEN
      RAISE EXCEPTION 'No monitored server has that server_id.' USING ERRCODE = 'DL404';
   END IF;
   IF p_scope_tag_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM config.server_tags WHERE id = p_scope_tag_id) THEN
      RAISE EXCEPTION 'No server tag has that id.' USING ERRCODE = 'DL404';
   END IF;
   IF p_fallback_route_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM config.config_notification_routes WHERE route_id = p_fallback_route_id) THEN
      RAISE EXCEPTION 'The fallback route does not exist.' USING ERRCODE = 'DL404';
   END IF;
   -- Every destination column keeps its empty default, so the route has no destinations of its own: each channel
   -- goes where it went before (the next matching route, else the Settings channels), and no caller of this function can name a URL, a key or an address list.
   -- The agent columns keep their defaults too: this function never makes an agent route.
   INSERT INTO config.config_notification_routes
      (metric_match, enabled, scope_server_id, scope_tag_id, send_resolutions, fallback_route_id, severities, report_notice, modified_at)
   VALUES
      (v_match, COALESCE(p_enabled, true), p_scope_server_id, p_scope_tag_id, p_send_resolutions, p_fallback_route_id, v_severities, v_report_notice,
       (now() AT TIME ZONE 'UTC'))
   RETURNING route_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION config.create_notification_route(text, boolean, integer, integer, boolean, integer, text, text) FROM PUBLIC;
-- #6542 part 2: the actor is an argument now and the before row comes back, so the old signature is dropped and stays uncallable.
DROP FUNCTION IF EXISTS config.update_notification_route(integer, text[], text, boolean, integer, integer, boolean, integer);
-- #6928 (V229): p_severities joins the argument list, so the nine-argument function is dropped and stays uncallable. It is read only when
-- p_columns names severities.
DROP FUNCTION IF EXISTS config.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer);
-- #6979 (V233): p_report_notice joins the argument list, so the ten-argument function is dropped and stays uncallable. It is read only when
-- p_columns names report_notice.
DROP FUNCTION IF EXISTS config.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer, text);
CREATE OR REPLACE FUNCTION config.update_notification_route(
   p_route_id integer,
   p_actor text,
   p_columns text[],
   p_metric_match text,
   p_enabled boolean,
   p_scope_server_id integer,
   p_scope_tag_id integer,
   p_send_resolutions boolean,
   p_fallback_route_id integer,
   p_severities text DEFAULT NULL,
   p_report_notice text DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
   v_row config.config_notification_routes%ROWTYPE;
   -- #6542 part 2: an operator-class MCP token (MCP is full admin) changes a route with an agent setting too. Every token connects as the
   -- same mcp role, so this is the label the service passes, and the service's own tool gate is what holds an agent token away.
   v_operator boolean := COALESCE(p_actor ~ '^operator:[A-Za-z0-9._: -]{1,64}$', false);
   v_match text;
   v_enabled boolean;
   v_server integer;
   v_tag integer;
   v_resolutions boolean;
   v_fallback integer;
   v_severities text;
   v_report_notice text;
BEGIN
   IF p_columns IS NULL OR cardinality(p_columns) = 0
      OR EXISTS (SELECT 1 FROM unnest(p_columns) AS c(name)
                 WHERE c.name IS NULL OR c.name NOT IN ('metric_match', 'enabled', 'scope', 'send_resolutions', 'fallback_route_id', 'severities', 'report_notice')) THEN
      RAISE EXCEPTION 'The columns to change are metric_match, enabled, scope, send_resolutions, fallback_route_id, severities and report_notice.' USING ERRCODE = 'DL400';
   END IF;
   -- The row is read again here, under its lock, so the agent-route refusal and every check below are on what is stored now
   -- and not on what the caller read a moment ago.
   SELECT * INTO v_row FROM config.config_notification_routes WHERE route_id = p_route_id FOR UPDATE;
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
   v_report_notice := CASE WHEN 'report_notice' = ANY (p_columns) THEN lower(btrim(COALESCE(p_report_notice, ''))) ELSE v_row.report_notice END;
   IF v_report_notice NOT IN ('off', 'needs_person_or_change', 'all') THEN
      RAISE EXCEPTION 'report_notice is off, needs_person_or_change or all.' USING ERRCODE = 'DL400';
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
      AND NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = v_server) THEN
      RAISE EXCEPTION 'No monitored server has that server_id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_tag IS NOT NULL AND v_tag IS DISTINCT FROM v_row.scope_tag_id
      AND NOT EXISTS (SELECT 1 FROM config.server_tags WHERE id = v_tag) THEN
      RAISE EXCEPTION 'No server tag has that id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_fallback IS NOT NULL AND v_fallback = p_route_id THEN
      RAISE EXCEPTION 'A route cannot be its own fallback route.' USING ERRCODE = 'DL400';
   END IF;
   IF v_fallback IS NOT NULL AND v_fallback IS DISTINCT FROM v_row.fallback_route_id
      AND NOT EXISTS (SELECT 1 FROM config.config_notification_routes WHERE route_id = v_fallback) THEN
      RAISE EXCEPTION 'The fallback route does not exist.' USING ERRCODE = 'DL404';
   END IF;
   -- The one UPDATE names no destination column: the sealed webhook values, the key, the signing keys and the address list
   -- stay exactly as stored.
   UPDATE config.config_notification_routes
      SET metric_match = v_match,
          enabled = v_enabled,
          scope_server_id = v_server,
          scope_tag_id = v_tag,
          send_resolutions = v_resolutions,
          fallback_route_id = v_fallback,
          severities = v_severities,
          report_notice = v_report_notice,
          modified_at = (now() AT TIME ZONE 'UTC')
    WHERE route_id = p_route_id;
   -- The row as it was before this change, in the journal's words: set_notification_route_agent journals it as the before of a
   -- route_agent_changed row in the same transaction, so a match or scope change made with the agent fields is named in the row.
   RETURN jsonb_build_object(
      'metric_match', btrim(v_row.metric_match), 'scope_server_id', v_row.scope_server_id, 'scope_tag_id', v_row.scope_tag_id,
      'agent_level', v_row.agent_level, 'agent_max_per_hour', v_row.agent_max_per_hour, 'agent_max_per_day', v_row.agent_max_per_day,
      'agent_lease_minutes', v_row.agent_lease_minutes, 'agent_expiry_minutes', v_row.agent_expiry_minutes, 'enabled', v_row.enabled,
      'severities', v_row.severities, 'report_notice', v_row.report_notice);
END
$fn$;
REVOKE ALL ON FUNCTION config.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer, text, text) FROM PUBLIC;
-- #6542: a named operator token sets a route's agent level, its four limits and its fallback over MCP through this function, and
-- the change is journaled as route_agent_changed under the token's name (operator:<name>), in the same transaction. Levels 0 to 3 are
-- set here (MCP is full admin). A level 3 server action still needs that server's switch (config_server_actions) and its remediation login.
-- p_before is the row update_notification_route returned in the same transaction, so the journal row names a match, scope or enabled change
-- made in the same call. The old signature is dropped and stays uncallable.
DROP FUNCTION IF EXISTS config.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean);
CREATE OR REPLACE FUNCTION config.set_notification_route_agent(
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
   v_row config.config_notification_routes%ROWTYPE;
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
   IF p_actor IS NULL OR p_actor !~ '^operator:[A-Za-z0-9._: -]{1,64}$' THEN
      RAISE EXCEPTION 'The actor is operator: and the operator token''s name, or the single token''s identity.' USING ERRCODE = 'DL400';
   END IF;
   -- No column is allowed only with p_before: a match, scope or enabled change to a route with an agent setting is journaled on its own.
   IF p_columns IS NULL OR (cardinality(p_columns) = 0 AND p_before IS NULL)
      OR EXISTS (SELECT 1 FROM unnest(p_columns) AS c(name)
                 WHERE c.name IS NULL OR c.name NOT IN ('agent_level', 'agent_max_per_hour', 'agent_max_per_day', 'agent_lease_minutes', 'agent_expiry_minutes', 'fallback_route_id')) THEN
      RAISE EXCEPTION 'The columns to change are agent_level, agent_max_per_hour, agent_max_per_day, agent_lease_minutes, agent_expiry_minutes and fallback_route_id.' USING ERRCODE = 'DL400';
   END IF;
   SELECT * INTO v_row FROM config.config_notification_routes WHERE route_id = p_route_id FOR UPDATE;
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
      AND NOT EXISTS (SELECT 1 FROM config.config_notification_routes WHERE route_id = v_fallback) THEN
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
      -- #6979: the report notice setting decides whether the route's requests post a notice. Only a route with an agent setting.
      IF (p_before ->> 'report_notice') IS DISTINCT FROM v_row.report_notice AND ((p_before ->> 'agent_level') IS NOT NULL OR v_level IS NOT NULL) THEN
         v_changed := array_append(v_changed, 'report_notice');
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
   UPDATE config.config_notification_routes
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
         'severities', v_row.severities, 'report_notice', v_row.report_notice) END;
      v_after := jsonb_build_object(
         'metric_match', btrim(v_row.metric_match), 'scope_server_id', v_row.scope_server_id, 'scope_tag_id', v_row.scope_tag_id,
         'agent_level', v_level, 'agent_max_per_hour', v_hour, 'agent_max_per_day', v_day,
         'agent_lease_minutes', v_lease, 'agent_expiry_minutes', v_expiry, 'enabled', v_row.enabled,
         'severities', v_row.severities, 'report_notice', v_row.report_notice);
      INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail)
      VALUES (now(), NULL, p_route_id, v_row.scope_server_id, 'route_agent_changed', p_actor,
              CASE WHEN COALESCE(p_created, false) THEN 'created route' ELSE 'changed ' || array_to_string(v_changed, ', ') END,
              jsonb_build_object('before', v_before, 'after', v_after));
   END IF;
END
$fn$;
REVOKE ALL ON FUNCTION config.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean, jsonb) FROM PUBLIC;
-- The return type changed (a previous value is now only a flag for whether one was saved), which CREATE OR REPLACE cannot do on its own.
-- #6542 part 2: the actor is an argument too, so the old signature is dropped and stays uncallable.
DROP FUNCTION IF EXISTS config.set_notification_route_destinations(integer, text[], text, text, text, text, text);
CREATE OR REPLACE FUNCTION config.set_notification_route_destinations(
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
   v_row config.config_notification_routes%ROWTYPE;
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
   SELECT * INTO v_row FROM config.config_notification_routes WHERE route_id = p_route_id FOR UPDATE;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'No notification route has that route_id.' USING ERRCODE = 'DL404';
   END IF;
   IF v_row.agent_level IS NOT NULL AND v_row.agent_level >= 1 AND NOT COALESCE(p_actor ~ '^operator:[A-Za-z0-9._: -]{1,64}$', false) THEN
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
   UPDATE config.config_notification_routes
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
REVOKE ALL ON FUNCTION config.set_notification_route_destinations(integer, text, text[], text, text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION config.create_notification_route(text, boolean, integer, integer, boolean, integer, text, text) TO mcp;
GRANT EXECUTE ON FUNCTION config.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer, text, text) TO mcp;
GRANT EXECUTE ON FUNCTION config.set_notification_route_destinations(integer, text, text[], text, text, text, text, text) TO mcp;
GRANT EXECUTE ON FUNCTION config.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean, jsonb) TO mcp;
-- #6394: the MCP generate_webhook_signing_key and remove_webhook_signing_key tools. mcp holds no UPDATE on a signing key column of config.config_notification
--     or config.config_notification_routes (they are secret columns) and no SELECT on them, and gets none here. The service reads the saved (sealed) URL
--     and proxy a new key is sealed for through its own store connection, so there is no function that returns them to mcp (an earlier build of this
--     change made read_webhook_signing_target; it is dropped here). set_webhook_signing_key stores a key the service sealed (only the shape of one:
--     sealed:v1:, the 16 character key id, base64) and moves the current key to the previous slot with the 7 day window the Viewer uses, and
--     clear_webhook_signing_key clears the key, the previous key and the window. Neither returns a key. Granted to mcp only.
DROP FUNCTION IF EXISTS config.read_webhook_signing_target(integer);
CREATE OR REPLACE FUNCTION config.set_webhook_signing_key(p_route_id integer, p_sealed_key text, p_stored_url text, p_proxy text)
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
      SELECT n.generic_url INTO v_url FROM config.config_notification AS n WHERE n.id = 1;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'The notification settings row does not exist yet.' USING ERRCODE = 'DL404';
      END IF;
   ELSE
      SELECT r.generic_url INTO v_url FROM config.config_notification_routes AS r WHERE r.route_id = p_route_id;
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
      UPDATE config.config_notification
         SET previous_signing_key = signing_key,
             previous_signing_key_until = CASE WHEN signing_key <> '' THEN now() + make_interval(days => 7) END,
             signing_key = p_sealed_key
       WHERE id = 1 AND generic_url = p_stored_url AND generic_proxy = p_proxy
      RETURNING previous_signing_key_until INTO v_until;
   ELSE
      UPDATE config.config_notification_routes
         SET previous_signing_key = signing_key,
             previous_signing_key_until = CASE WHEN signing_key <> '' THEN now() + make_interval(days => 7) END,
             signing_key = p_sealed_key
       WHERE route_id = p_route_id AND generic_url = p_stored_url
         AND COALESCE((SELECT n.generic_proxy FROM config.config_notification AS n WHERE n.id = 1 FOR SHARE), '') = p_proxy
      RETURNING previous_signing_key_until INTO v_until;
   END IF;
   IF NOT FOUND THEN
      RAISE EXCEPTION 'The generic webhook URL or proxy changed while the key was made. Nothing was written; try again.' USING ERRCODE = 'DL403';
   END IF;
   RETURN v_until;
END
$fn$;
REVOKE ALL ON FUNCTION config.set_webhook_signing_key(integer, text, text, text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION config.clear_webhook_signing_key(p_route_id integer)
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
      SELECT n.generic_url, n.signing_key <> '' INTO v_url, v_had FROM config.config_notification AS n WHERE n.id = 1 FOR UPDATE;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'The notification settings row does not exist yet.' USING ERRCODE = 'DL404';
      END IF;
   ELSE
      SELECT r.generic_url, r.signing_key <> '' INTO v_url, v_had FROM config.config_notification_routes AS r WHERE r.route_id = p_route_id FOR UPDATE;
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
      UPDATE config.config_notification SET signing_key = '', previous_signing_key = '', previous_signing_key_until = NULL WHERE id = 1;
   ELSE
      UPDATE config.config_notification_routes SET signing_key = '', previous_signing_key = '', previous_signing_key_until = NULL WHERE route_id = p_route_id;
   END IF;
   RETURN v_had;
END
$fn$;
REVOKE ALL ON FUNCTION config.clear_webhook_signing_key(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION config.set_webhook_signing_key(integer, text, text, text) TO mcp;
GRANT EXECUTE ON FUNCTION config.clear_webhook_signing_key(integer) TO mcp;
-- #6392: the MCP update_notification_channels tool. mcp holds no UPDATE on any secret column of config.config_notification (the SMTP
--     user name and password, the Teams, Slack and generic webhook URLs, the generic headers, the PagerDuty routing key) and no SELECT
--     on them, and gets none here: set_notification_channels is the one way it writes the default channels. The service seals each
--     secret first (the SMTP password for its host, port, SSL flag and user name; a webhook value for its slot, proxy and, for the
--     headers, the generic URL), and the function stores only an empty value or a sealed one, reads the row FOR UPDATE and makes the
--     Viewer's refusals (a change to what a saved secret is tied to needs the secret again, a signing key blocks a URL or proxy change).
--     It returns only whether each secret named in the call was saved before. The tool is served only under mcp.allowWrites with a token.
--     Granted to mcp only.
CREATE OR REPLACE FUNCTION config.set_notification_channels(
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
   v_row config.config_notification%ROWTYPE;
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
   SELECT * INTO v_row FROM config.config_notification WHERE id = 1 FOR UPDATE;
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
          AND (v_row.signing_key <> '' OR EXISTS (SELECT 1 FROM config.config_notification_routes WHERE signing_key <> ''))) THEN
      RAISE EXCEPTION 'A signing key is saved for this destination, and it is tied to the generic webhook URL and proxy. Remove the key first, save the change, then generate a new key.' USING ERRCODE = 'DL403';
   END IF;
   -- A route's own destination value is sealed for its channel's proxy on this row, so a proxy change would turn that route's channel off on
   -- the next reload (and leave a value saved before sealing to go out through the new proxy): it is refused while any route holds one (#6392).
   IF (v_teams_proxy IS DISTINCT FROM v_row.teams_proxy AND EXISTS (SELECT 1 FROM config.config_notification_routes WHERE teams_url <> ''))
      OR (v_slack_proxy IS DISTINCT FROM v_row.slack_proxy AND EXISTS (SELECT 1 FROM config.config_notification_routes WHERE slack_url <> ''))
      OR (v_generic_proxy IS DISTINCT FROM v_row.generic_proxy AND EXISTS (SELECT 1 FROM config.config_notification_routes WHERE generic_url <> ''))
      OR (v_pd_proxy IS DISTINCT FROM v_row.pagerduty_proxy AND EXISTS (SELECT 1 FROM config.config_notification_routes WHERE pagerduty_routing_key <> '')) THEN
      RAISE EXCEPTION 'A notification route holds its own value for a channel whose proxy is changing, and that value is tied to the proxy. Clear the value on each route first, change the proxy, then enter the value on the route again.' USING ERRCODE = 'DL403';
   END IF;
   UPDATE config.config_notification
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
REVOKE ALL ON FUNCTION config.set_notification_channels(text[], text, integer, boolean, text, text, text, text, text, text, text, text, text, text, text, text, text, boolean, text, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION config.set_notification_channels(text[], text, integer, boolean, text, text, text, text, text, text, text, text, text, text, text, text, text, boolean, text, boolean) TO mcp;
COMMIT;

-- #6230: the MCP incident tools queue analyze_now, purge_now, test_connect, pause and resume through this function, the only way the
--     mcp role writes a command row. Same function, same text, as managed provisioning rebuilds on every start.
CREATE OR REPLACE FUNCTION config.request_mcp_command(
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
   ELSIF p_server_id IS NULL OR NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id) THEN
      RAISE EXCEPTION 'No monitored server with that id.' USING ERRCODE = 'DL404';
   END IF;
   -- A connection test dials the server with its stored credentials, so it runs only against a server the service is monitoring.
   IF v_type = 'test_connect' AND NOT EXISTS (SELECT 1 FROM config.config_monitored_servers WHERE server_id = p_server_id AND is_enabled) THEN
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
      FROM config.config_monitored_servers AS s
      WHERE s.server_id = p_server_id;
   END IF;
   -- One request of a kind (per server where it has one) at a time, and not within 15 seconds of the last, so a holder of the
   -- mcp login cannot fill the queue. The lock is a try-lock: a request that cannot take it at once answers busy and never waits.
   IF NOT pg_try_advisory_xact_lock(6230, hashtext(v_type || ':' || COALESCE(p_server_id::text, ''))) THEN
      RAISE EXCEPTION 'Another request of this kind is being queued right now (busy); try again shortly.' USING ERRCODE = 'DL423';
   END IF;
   SELECT c.status, c.created_at INTO v_last_status, v_last_at
   FROM config.config_command AS c
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
   INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at)
   VALUES ('mcp', v_type, p_server_id, v_args, 'pending', (now() AT TIME ZONE 'UTC'))
   RETURNING command_id INTO v_id;
   RETURN v_id;
END
$fn$;
REVOKE ALL ON FUNCTION config.request_mcp_command(text, integer, integer, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION config.request_mcp_command(text, integer, integer, text) FROM mcp;
GRANT EXECUTE ON FUNCTION config.request_mcp_command(text, integer, integer, text) TO mcp;

-- #6230: mcp holds no SELECT on config_command.result_json; it reads a command's status and result through
--     config.read_mcp_command_result, which answers only for the command types the MCP tools queue. Owned by the store
--     owner, pinned search_path, no dynamic SQL, granted to mcp only. NOT a versioned migration.
CREATE OR REPLACE FUNCTION config.read_mcp_command_result(p_command_id bigint)
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
   FROM config.config_command AS c
   WHERE c.command_id = p_command_id
     AND c.command_type IN ('analyze_now', 'purge_now', 'test_connect', 'pause', 'resume', 'snapshot_now');
$fn$;
REVOKE ALL ON FUNCTION config.read_mcp_command_result(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION config.read_mcp_command_result(bigint) FROM mcp;
GRANT EXECUTE ON FUNCTION config.read_mcp_command_result(bigint) TO mcp;

-- The store's own password rules for every role but the owner: a BEFORE INSERT OR UPDATE trigger on
--     config_monitored_servers. A new or changed password that starts with env: or file: (a reference) is refused:
--     references are set in the configuration file, or by --add-server on the service host, as the owner. An update
--     that changes how a server is reached (host, port, engine, database, read-only intent, authentication mode,
--     username, encryption, certificate trust, multi-subnet failover) while keeping the stored password is refused:
--     the password is typed again with the change, and a row that holds a remediation login is not moved by these roles
--     at all (PW003). The owner (the role that owns the table, or a superuser) is not
--     held to either rule, and neither is a row already in the table: only INSERT and UPDATE are checked. The
--     trigger and its function are created here, not in a migration, so re-running this script after an upgrade
--     keeps them current.
CREATE OR REPLACE FUNCTION config.monitored_server_password_rules()
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
            FROM config.config_monitored_servers AS s
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
REVOKE ALL ON FUNCTION config.monitored_server_password_rules() FROM PUBLIC;
CREATE OR REPLACE TRIGGER trg_monitored_server_password_rules
   BEFORE INSERT OR UPDATE ON config.config_monitored_servers
   FOR EACH ROW EXECUTE FUNCTION config.monitored_server_password_rules();

-- 3e. Custom alert rules (#3285): the web dashboard's rule editor (/api/alerts, as web) and the MCP rule
--     tools (as mcp) create, edit and delete config.custom_alert_rules -- non-secret rule JSON, the same
--     single-table floor as 3b. Created by V116, so re-run this script after upgrading past it.
REVOKE INSERT, UPDATE, DELETE ON config.custom_alert_rules FROM viewer;
GRANT INSERT, UPDATE, DELETE ON config.custom_alert_rules TO web;
GRANT INSERT, UPDATE, DELETE ON config.custom_alert_rules TO mcp;

-- 3e2. Database state expectations (#6397): the MCP set_database_state_expected tool upserts (INSERT, UPDATE) and clears
--      (DELETE) one config.database_state_expected row, the same narrow single-table floor web holds in 3c. Its read,
--      get_database_state_expectations, runs on mcp's SELECT on all of config (step 2). The table is V49. BRING-YOUR-OWN
--      STORES: an existing store must RE-RUN this script to add the grant; until then the tool answers a permission error.
GRANT INSERT, UPDATE, DELETE ON config.database_state_expected TO mcp;

-- 3f. The rest of the MCP tools' writes, mirroring managed provisioning's grants exactly:
--     analyze_server persists its findings and mute_analysis_finding records a mute (INSERT, and DELETE for
--     unmute_analysis_finding, #6383); update_alert_settings writes the singleton alert-settings row and the ONE non-secret
--     column of config_notification it spans, email_cooldown_minutes (COLUMN-level: the row holds the SMTP
--     password and the webhook URLs, which mcp can neither read nor write; it sets them only through the SECURITY DEFINER
--     set_notification_channels function, #6392, served only under mcp.allowWrites with a token); set_notification_route_enabled
--     and delete_notification_route take a route out of force or remove it (never INSERT, never a
--     destination column: a route IS a destination); add_servers / remove_server edit the monitored-server
--     registry (whose credential column stays carved, so mcp can WRITE a password blob and never READ one).
GRANT INSERT ON collect.analysis_findings TO mcp;
GRANT INSERT, DELETE ON config.analysis_muted TO mcp;
GRANT UPDATE ON config.config_alert_settings TO mcp;
GRANT UPDATE (email_cooldown_minutes) ON config.config_notification TO mcp;
-- #5825 (V187): update_alert_settings writes delivery.send_resolutions, the four per-channel resolution flags.
-- Guarded on the column, so a store below V187 still commits the rest.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 'config.config_notification'::regclass
                AND attname = 'slack_send_resolutions' AND NOT attisdropped) THEN
      EXECUTE $v187$
GRANT UPDATE (slack_send_resolutions, teams_send_resolutions, generic_send_resolutions, email_send_resolutions)
    ON config.config_notification TO mcp;
$v187$;
   END IF;
END $$;
GRANT UPDATE (enabled, modified_at), DELETE ON config.config_notification_routes TO mcp;
-- #6036 (V193): set_notification_route_fallback writes the fallback route id, a pointer to another route (never a
-- destination). Guarded on the column, so a store below V193 still commits the rest.
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = pg_catalog.to_regclass('config.config_notification_routes')
                AND attname = 'fallback_route_id' AND NOT attisdropped) THEN
      EXECUTE $v193f$
GRANT UPDATE (fallback_route_id) ON config.config_notification_routes TO mcp;
$v193f$;
   END IF;
END $$;
GRANT INSERT, DELETE ON config.config_monitored_servers TO mcp;
REVOKE UPDATE ON config.config_monitored_servers FROM mcp;
-- The role and external ID of a server are set only through the edit function (and an INSERT): mcp's UPDATE
-- names every other column of the table and leaves out aws_role_arn, aws_external_id and the generated
-- aws_external_id_set, and query_store_replica (#6254), a host the service connects to with its own credentials:
-- it changes only through the edit function, never add it here. A column added to the table later is added to this list
-- unless it is one of those.
GRANT UPDATE (server_id, name, host, database, auth, username, encrypted_password, encrypt_mode, trust_server_certificate, read_only_intent, multi_subnet_failover, excluded_databases, monthly_cost_usd, capture_plans, is_enabled, created_at, modified_at, alert_delivery_mode_override, engine, port, plan_force_bot_enabled, remediation_username, remediation_encrypted_password)
   ON config.config_monitored_servers TO mcp;

-- 3h. External alert submit (#5654): another service raises an alert through Darling's channels, routes and mute rules by
--     calling config.submit_external_alert as its OWN login (one login per source). This is the same function managed
--     provisioning rebuilds on every start (DarlingManagedRoles section 10b), the same text, and the same grants (section
--     10c). It looks the caller up by session_user in config.external_alert_source, so no argument names the source and
--     no source can record an alert under another source's name. EXECUTE goes to the registered source logins only.
--     The three tables (V180) are written only by the owner and by this function: the REVOKE below takes the
--     INSERT/UPDATE/DELETE that step 3 and the default privileges gave admin. A source login is touched only when the registry names it
--     and it carries the 'darling-alert-source' marker comment; each gets LOGIN, every widening attribute off, NOINHERIT,
--     no role membership but darling_alert_sources (WITH INHERIT FALSE, SET FALSE from PostgreSQL 16), CONNECT on this
--     database, USAGE on config and EXECUTE on the function, and nothing else. darling_alert_sources is a NOLOGIN group
--     marked 'darling-alert-source-group' that holds nothing; it exists so the managed pg_hba line for
--     postgres.network.role "sources" names one role. An unmarked role of that name stops the script (the step 1 rule), and
--     every member that is not a registered, marked source login is revoked from it. On a
--     store below V180 the block warns and skips the tables and the logins (the function itself is created either way);
--     re-run the script once the service has migrated the store to V180, and again after each add-alert-source.
CREATE OR REPLACE FUNCTION config.submit_external_alert(
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
   FROM config.external_alert_source AS s
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

   IF (SELECT count(*) FROM config.external_alert_pending AS p
       WHERE p.source_id = v_source_id AND p.submitted_at > clock_timestamp() - interval '1 minute') >= v_max_per_minute THEN
      RAISE EXCEPTION 'This source has reached its limit of % alerts per minute.', v_max_per_minute
         USING ERRCODE = 'DL429', HINT = 'Send fewer alerts, or ask the Darling operator to raise max_per_minute for this source.';
   END IF;
   IF (SELECT count(*) FROM config.external_alert_pending AS p
       WHERE p.source_id = v_source_id AND p.status IN ('pending', 'claimed')) >= v_max_pending THEN
      RAISE EXCEPTION 'This source already has % alerts waiting for delivery.', v_max_pending
         USING ERRCODE = 'DL429', HINT = 'Darling has not delivered the earlier alerts yet. Retry later.';
   END IF;

   INSERT INTO config.external_alert_pending
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
REVOKE ALL ON FUNCTION config.submit_external_alert(text, text, text, text, text, text, double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION config.submit_external_alert(text, text, text, text, text, text, double precision) FROM admin, viewer, web, mcp;
-- The two trigger functions need no EXECUTE to fire, so PUBLIC (and with it a source login) gets none.
REVOKE EXECUTE ON FUNCTION config.config_bump_version(), config.config_service_bump() FROM PUBLIC;
-- pg_stat_statements gives PUBLIC its views and functions; Darling reads them through its own SECURITY DEFINER functions.
-- pg_read_all_stats (part of pg_monitor) keeps them, so a monitoring login that watches this database as a PostgreSQL target
-- still reads them, and so does each store owner that is not a superuser (the owner of config.config_monitored_servers and
-- of the reader functions), which those functions run as. Only the owner of the extension or a superuser may revoke them:
-- for any other role this block changes nothing. When PUBLIC still holds them afterwards (the extension belongs to a
-- superuser and this script runs as the store owner), it raises a WARNING naming the owner of the extension and the
-- statements that owner runs; the service logs the same text.
DO $do$
DECLARE
   v_pass integer;
   v_may_change boolean;
   v_statements text;
   v_warning text;
BEGIN
   -- The first pass revokes when this role may; the second reads the grants as they are now.
   FOR v_pass IN 1..2 LOOP
      SELECT q.may_change, q.sql_text, q.warning INTO v_may_change, v_statements, v_warning
      FROM (
      WITH ext AS
      (
          SELECT
              n.nspname,
              e.extowner,
              e.extnamespace,
              (
                  SELECT c.oid
                  FROM pg_catalog.pg_class AS c
                  WHERE c.relnamespace = e.extnamespace
                  AND   c.relname OPERATOR(pg_catalog.=) 'pg_stat_statements'
              ) AS view_oid,
              (
                  SELECT c.oid
                  FROM pg_catalog.pg_class AS c
                  WHERE c.relnamespace = e.extnamespace
                  AND   c.relname OPERATOR(pg_catalog.=) 'pg_stat_statements_info'
              ) AS info_oid
          FROM pg_catalog.pg_extension AS e
          JOIN pg_catalog.pg_namespace AS n
            ON n.oid = e.extnamespace
          WHERE e.extname = 'pg_stat_statements'
      ),
      facts AS
      (
          SELECT
              ext.*,
              (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = ext.view_oid) AS view_owner,
              (
                  SELECT pg_catalog.string_agg(pg_catalog.quote_ident(r.rolname), ', ' ORDER BY r.rolname)
                  FROM pg_catalog.pg_roles AS r
                  WHERE NOT r.rolsuper
                  AND   r.oid IS DISTINCT FROM (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = ext.view_oid)
                  AND   r.oid IN
                        (
                            SELECT c.relowner
                            FROM pg_catalog.pg_class AS c
                            JOIN pg_catalog.pg_namespace AS cn
                              ON cn.oid = c.relnamespace
                            WHERE cn.nspname OPERATOR(pg_catalog.=) 'config'
                            AND   c.relname OPERATOR(pg_catalog.=) 'config_monitored_servers'
                            UNION ALL
                            SELECT p.proowner
                            FROM pg_catalog.pg_proc AS p
                            JOIN pg_catalog.pg_namespace AS pn
                              ON pn.oid = p.pronamespace
                            WHERE pn.nspname OPERATOR(pg_catalog.=) 'config'
                            AND   p.proname IN ('store_statement_stats', 'store_statement_stats_info')
                        )
              ) AS owners,
              (
                  EXISTS
                  (
                      SELECT 1
                      FROM pg_catalog.pg_class AS c
                      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))) AS a
                      WHERE c.oid IN (ext.view_oid, ext.info_oid)
                      AND   a.grantee = 0
                      AND   a.privilege_type = 'SELECT'
                  )
                  OR EXISTS
                  (
                      SELECT 1
                      FROM pg_catalog.pg_proc AS p
                      CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
                      WHERE p.pronamespace = ext.extnamespace
                      AND   p.proname IN ('pg_stat_statements', 'pg_stat_statements_info')
                      AND   a.grantee = 0
                      AND   a.privilege_type = 'EXECUTE'
                  )
              ) AS public_reads
          FROM ext
      ),
      built AS
      (
          SELECT
              f.*,
              'pg_read_all_stats' || COALESCE(', ' || f.owners, '') AS grantees
          FROM facts AS f
      ),
      statements AS
      (
          SELECT
              b.*,
              pg_catalog.format('REVOKE SELECT ON %1$I.pg_stat_statements FROM PUBLIC; REVOKE EXECUTE ON FUNCTION %1$I.pg_stat_statements(boolean) FROM PUBLIC; GRANT SELECT ON %1$I.pg_stat_statements TO %2$s; GRANT EXECUTE ON FUNCTION %1$I.pg_stat_statements(boolean) TO %2$s;', b.nspname, b.grantees)
              || CASE
                     WHEN b.info_oid IS NULL THEN ''
                     ELSE pg_catalog.format(' REVOKE SELECT ON %1$I.pg_stat_statements_info FROM PUBLIC; REVOKE EXECUTE ON FUNCTION %1$I.pg_stat_statements_info() FROM PUBLIC; GRANT SELECT ON %1$I.pg_stat_statements_info TO %2$s; GRANT EXECUTE ON FUNCTION %1$I.pg_stat_statements_info() TO %2$s;', b.nspname, b.grantees)
                 END AS sql_text
          FROM built AS b
      )
      SELECT
          COALESCE((SELECT r.rolsuper FROM pg_catalog.pg_roles AS r WHERE r.rolname OPERATOR(pg_catalog.=) current_user), false)
              OR (s.view_owner IS NOT NULL AND pg_catalog.pg_has_role(current_user, s.view_owner, 'USAGE')) AS may_change,
          s.sql_text,
          CASE
              WHEN s.public_reads THEN pg_catalog.format(
                  'Statement statistics: PUBLIC can still read %s.pg_stat_statements, so every login that may connect to this database reads the statement statistics of every role, the external alert source logins included. Only the owner of the extension, %s, or a superuser can revoke that. Have %s run, in this database: %s',
                  pg_catalog.quote_ident(s.nspname),
                  pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(s.extowner)),
                  pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(s.extowner)),
                  s.sql_text)
          END AS warning
      FROM statements AS s
      WHERE s.view_oid IS NOT NULL
      ) AS q;
      IF v_pass = 1 AND v_may_change AND v_statements IS NOT NULL THEN
         EXECUTE v_statements;
      END IF;
   END LOOP;
   IF v_warning IS NOT NULL THEN
      RAISE WARNING '%', v_warning;
   END IF;
END $do$;

DO $do$
DECLARE
   as_superuser boolean := (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user);
   source_login record;
   membership record;
   attribute text;
   group_oid oid;
   reset_failed boolean;
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'darling_alert_sources') THEN
      CREATE ROLE darling_alert_sources NOLOGIN NOINHERIT;
      COMMENT ON ROLE darling_alert_sources IS 'darling-alert-source-group';
   ELSIF pg_catalog.shobj_description((SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'darling_alert_sources'), 'pg_authid') IS DISTINCT FROM 'darling-alert-source-group' THEN
      RAISE EXCEPTION 'The group role "darling_alert_sources" already exists and was not created by Darling (missing the ''darling-alert-source-group'' marker comment). Rename or drop it before provisioning so Darling does not repurpose an unrelated role.';
   END IF;

   SELECT oid INTO group_oid FROM pg_catalog.pg_roles WHERE rolname = 'darling_alert_sources';
   -- NOLOGIN and NOINHERIT alone, then only the attributes that are on: a role without SUPERUSER may not name SUPERUSER,
   -- REPLICATION or BYPASSRLS in ALTER ROLE even to switch them off, so naming them all would always fail for it.
   BEGIN
      ALTER ROLE darling_alert_sources NOLOGIN NOINHERIT;
   EXCEPTION WHEN insufficient_privilege THEN
      RAISE WARNING 'Role "darling_alert_sources" could not be set to NOLOGIN NOINHERIT: this needs a superuser, or CREATEROLE and, from PostgreSQL 16, ADMIN OPTION on the role.';
   END;
   FOREACH attribute IN ARRAY (
         SELECT pg_catalog.array_remove(ARRAY[
            CASE WHEN r.rolsuper THEN 'SUPERUSER' END,
            CASE WHEN r.rolcreaterole THEN 'CREATEROLE' END,
            CASE WHEN r.rolcreatedb THEN 'CREATEDB' END,
            CASE WHEN r.rolreplication THEN 'REPLICATION' END,
            CASE WHEN r.rolbypassrls THEN 'BYPASSRLS' END], NULL)
         FROM pg_catalog.pg_roles AS r WHERE r.oid = group_oid)
   LOOP
      BEGIN
         EXECUTE pg_catalog.format('ALTER ROLE darling_alert_sources NO%s', attribute);
      EXCEPTION WHEN insufficient_privilege THEN
         RAISE WARNING 'Role "darling_alert_sources" still has the % attribute: switching it off needs a superuser, or, from PostgreSQL 16, an owner that holds % itself.', attribute, attribute;
      END;
   END LOOP;
   IF (SELECT r.rolcanlogin FROM pg_catalog.pg_roles AS r WHERE r.oid = group_oid) THEN
      RAISE WARNING 'Role "darling_alert_sources" can still log in, and a pg_hba line for +darling_alert_sources admits it. Have a superuser run: ALTER ROLE darling_alert_sources NOLOGIN;';
   END IF;

   FOR membership IN
      SELECT g.rolname AS granted, b.rolname AS grantor
      FROM pg_catalog.pg_auth_members AS a
      JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
      LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor
      WHERE a.member = group_oid
   LOOP
      BEGIN
         EXECUTE pg_catalog.format('REVOKE %I FROM darling_alert_sources', membership.granted)
            || CASE WHEN (as_superuser OR pg_catalog.current_setting('server_version_num')::int >= 160000) AND membership.grantor IS NOT NULL
                    THEN pg_catalog.format(' GRANTED BY %I', membership.grantor) ELSE '' END
            || ' CASCADE';
      EXCEPTION WHEN insufficient_privilege THEN
         RAISE WARNING 'Role "darling_alert_sources" is still a member of "%": revoking that needs a superuser, or ADMIN OPTION on it.', membership.granted;
      END;
   END LOOP;

   EXECUTE pg_catalog.format('REVOKE ALL ON DATABASE %I FROM darling_alert_sources', pg_catalog.current_database());
   REVOKE ALL ON SCHEMA config, collect FROM darling_alert_sources;
   REVOKE ALL ON ALL TABLES IN SCHEMA config, collect FROM darling_alert_sources;
   REVOKE ALL ON ALL SEQUENCES IN SCHEMA config, collect FROM darling_alert_sources;
   REVOKE ALL ON FUNCTION config.submit_external_alert(text, text, text, text, text, text, double precision) FROM darling_alert_sources;

   IF pg_catalog.to_regclass('config.external_alert_source') IS NULL
      OR pg_catalog.to_regclass('config.external_alert_pending') IS NULL
      OR pg_catalog.to_regclass('config.external_alert_state') IS NULL THEN
      RAISE WARNING 'The external alert tables were not found, so their grants and the source logins were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   REVOKE ALL ON config.external_alert_source, config.external_alert_pending, config.external_alert_state
      FROM PUBLIC, admin, viewer, web, mcp CASCADE;
   GRANT SELECT ON config.external_alert_source, config.external_alert_pending, config.external_alert_state
      TO admin, viewer, web;
   GRANT SELECT ON config.external_alert_source, config.external_alert_pending, config.external_alert_state
      TO mcp;

   -- Every grant of the group to a role that is not a registered, marked source login, and every grant of a source login to
   -- any role, found by walking down from the group through the source logins: pg_hba's +darling_alert_sources matches a
   -- member at any depth, whatever its INHERIT and SET options. A grant this role may not revoke (from PostgreSQL 16, a role
   -- that is not a superuser is recorded as a member of each role it creates, granted by the bootstrap superuser) is named
   -- in a WARNING here, and again at the end of this block with the pg_hba line that keeps it out.
   FOR membership IN
      WITH RECURSIVE valid AS (
         SELECT r.oid
         FROM config.external_alert_source AS s
         JOIN pg_catalog.pg_roles AS r ON r.rolname = s.role_name
         WHERE pg_catalog.shobj_description(r.oid, 'pg_authid') IS NOT DISTINCT FROM 'darling-alert-source'
           AND r.rolname NOT IN ('admin', 'viewer', 'web', 'mcp')
           AND r.rolname <> current_user
           AND NOT r.rolsuper
           AND r.oid <> (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database())
      ), walk (roleid, member) AS (
         SELECT a.roleid, a.member FROM pg_catalog.pg_auth_members AS a WHERE a.roleid = group_oid
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
      WHERE (w.roleid = group_oid AND w.member NOT IN (SELECT v.oid FROM valid AS v))
         OR w.roleid IN (SELECT v.oid FROM valid AS v)
   LOOP
      BEGIN
         EXECUTE pg_catalog.format('REVOKE %I FROM %I', membership.granted, membership.member_name)
            || CASE WHEN (as_superuser OR pg_catalog.current_setting('server_version_num')::int >= 160000) AND membership.grantor IS NOT NULL
                    THEN pg_catalog.format(' GRANTED BY %I', membership.grantor) ELSE '' END
            || ' CASCADE';
      EXCEPTION WHEN insufficient_privilege THEN
         RAISE WARNING 'Role "%" is still a member of "%": revoking that needs a superuser, or ADMIN OPTION on it and the rights of the role that granted it.', membership.member_name, membership.granted;
      END;
   END LOOP;

   FOR source_login IN
      SELECT s.role_name, r.oid AS role_oid, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolreplication, r.rolbypassrls,
             (pg_catalog.shobj_description(r.oid, 'pg_authid') IS NOT DISTINCT FROM 'darling-alert-source') AS marked,
             (r.rolname IN ('admin', 'viewer', 'web', 'mcp')
              OR r.rolname = current_user
              OR r.rolsuper
              OR r.oid = (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database())) AS reserved
      FROM config.external_alert_source AS s
      LEFT JOIN pg_catalog.pg_roles AS r ON r.rolname = s.role_name
   LOOP
      IF source_login.reserved IS TRUE THEN
         RAISE WARNING 'External alert source login "%" was skipped: it is one of Darling''s own roles, the role running this step, a superuser or the database owner, and its limit, inheritance and memberships are left as they are.', source_login.role_name;
         CONTINUE;
      END IF;

      IF source_login.role_oid IS NULL OR NOT source_login.marked THEN
         RAISE WARNING 'External alert source login "%" was skipped: it does not exist or does not carry the ''darling-alert-source'' marker comment.', source_login.role_name;
         IF source_login.role_oid IS NOT NULL THEN
            EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION config.submit_external_alert(%s) FROM %I',
               'text, text, text, text, text, text, double precision', source_login.role_name);
         END IF;
         CONTINUE;
      END IF;

      -- A login whose reset below does not complete (an attribute or a membership this role may not take back) gets no
      -- grant and no membership in the group, as the service's add verb does: it is not admitted until a superuser runs
      -- this script, or the add verb's statements, for it.
      reset_failed := false;
      BEGIN
         EXECUTE pg_catalog.format('ALTER ROLE %I LOGIN NOINHERIT CONNECTION LIMIT 5', source_login.role_name);
      EXCEPTION WHEN insufficient_privilege THEN
         reset_failed := true;
         RAISE WARNING 'Login "%" could not be set to LOGIN NOINHERIT CONNECTION LIMIT 5: this needs CREATEROLE, and from PostgreSQL 16 ADMIN OPTION on the role.', source_login.role_name;
      END;
      FOREACH attribute IN ARRAY pg_catalog.array_remove(ARRAY[
            CASE WHEN source_login.rolsuper THEN 'SUPERUSER' END,
            CASE WHEN source_login.rolcreaterole THEN 'CREATEROLE' END,
            CASE WHEN source_login.rolcreatedb THEN 'CREATEDB' END,
            CASE WHEN source_login.rolreplication THEN 'REPLICATION' END,
            CASE WHEN source_login.rolbypassrls THEN 'BYPASSRLS' END], NULL)
      LOOP
         BEGIN
            EXECUTE pg_catalog.format('ALTER ROLE %I NO%s', source_login.role_name, attribute);
         EXCEPTION WHEN insufficient_privilege THEN
            reset_failed := true;
            RAISE WARNING 'Login "%" still has the % attribute: switching it off needs a superuser, or, from PostgreSQL 16, an owner that holds % itself.', source_login.role_name, attribute, attribute;
         END;
      END LOOP;

      FOR membership IN
         SELECT g.rolname AS granted, b.rolname AS grantor
         FROM pg_catalog.pg_auth_members AS a
         JOIN pg_catalog.pg_roles AS g ON g.oid = a.roleid
         LEFT JOIN pg_catalog.pg_roles AS b ON b.oid = a.grantor
         WHERE a.member = source_login.role_oid
      LOOP
         BEGIN
            EXECUTE pg_catalog.format('REVOKE %I FROM %I', membership.granted, source_login.role_name)
               || CASE WHEN (as_superuser OR pg_catalog.current_setting('server_version_num')::int >= 160000) AND membership.grantor IS NOT NULL
                       THEN pg_catalog.format(' GRANTED BY %I', membership.grantor) ELSE '' END
               || ' CASCADE';
         EXCEPTION WHEN insufficient_privilege THEN
            reset_failed := true;
            RAISE WARNING 'Login "%" is still a member of "%": revoking that needs a superuser, or ADMIN OPTION on it.', source_login.role_name, membership.granted;
         END;
      END LOOP;

      EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA config, collect FROM %I', source_login.role_name);
      EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA config, collect FROM %I', source_login.role_name);
      IF reset_failed THEN
         EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION config.submit_external_alert(%s) FROM %I',
            'text, text, text, text, text, text, double precision', source_login.role_name);
         BEGIN
            EXECUTE pg_catalog.format('REVOKE darling_alert_sources FROM %I', source_login.role_name);
         EXCEPTION WHEN insufficient_privilege THEN
            NULL;
         END;
         RAISE WARNING 'External alert source login "%" is not admitted: this script could not finish its reset (see the warnings above), so it gets no grant and no membership in "darling_alert_sources". A superuser runs this script again, or the add verb''s statements, to admit it.', source_login.role_name;
         CONTINUE;
      END IF;
      EXECUTE pg_catalog.format('GRANT CONNECT ON DATABASE %I TO %I', pg_catalog.current_database(), source_login.role_name);
      EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA config TO %I', source_login.role_name);
      EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION config.submit_external_alert(%s) TO %I',
         'text, text, text, text, text, text, double precision', source_login.role_name);
      BEGIN
         IF pg_catalog.current_setting('server_version_num')::int >= 160000 THEN
            EXECUTE pg_catalog.format('GRANT darling_alert_sources TO %I WITH INHERIT FALSE, SET FALSE', source_login.role_name);
         ELSE
            EXECUTE pg_catalog.format('GRANT darling_alert_sources TO %I', source_login.role_name);
         END IF;
      EXCEPTION WHEN insufficient_privilege THEN
         RAISE WARNING 'Login "%" could not be made a member of "darling_alert_sources": this needs a superuser, or ADMIN OPTION on the group.', source_login.role_name;
      END;
   END LOOP;

   -- What a pg_hba line for +darling_alert_sources admits besides the source logins: every member of the group at any depth
   -- (through any role) that is not a registered, marked source login, after the revokes above. The role running this script
   -- is one of them when it is not a superuser and created the group or a source login on PostgreSQL 16 or later.
   FOR membership IN
      WITH RECURSIVE closure (oid) AS (
         SELECT a.member FROM pg_catalog.pg_auth_members AS a WHERE a.roleid = group_oid
         UNION
         SELECT a.member FROM pg_catalog.pg_auth_members AS a JOIN closure AS c ON a.roleid = c.oid
      )
      SELECT r.rolname AS member_name
      FROM closure AS c
      JOIN pg_catalog.pg_roles AS r ON r.oid = c.oid
      WHERE NOT (EXISTS (SELECT 1 FROM config.external_alert_source AS s WHERE s.role_name = r.rolname)
                 AND pg_catalog.shobj_description(r.oid, 'pg_authid') IS NOT DISTINCT FROM 'darling-alert-source'
                 AND r.rolname NOT IN ('admin', 'viewer', 'web', 'mcp')
                 AND NOT r.rolsuper
                 AND r.oid <> (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database()))
      ORDER BY r.rolname
   LOOP
      RAISE WARNING 'Role "%" is a member of "darling_alert_sources" (directly or through a source login)%, so a pg_hba line for +darling_alert_sources admits it too. Put this line ABOVE that one: hostssl % % <range> reject',
         membership.member_name,
         CASE WHEN membership.member_name = current_user THEN ', and it is the role running this script' ELSE '' END,
         -- pg_hba quoting: a name is bare only when it is plain lower-case letters, digits and underscores and not a
         -- pg_hba keyword, so a role named all is that one role and never every role (the service quotes the same way).
         CASE WHEN pg_catalog.current_database() ~ '^[a-z0-9_]+$'
                   AND pg_catalog.current_database() NOT IN ('all', 'sameuser', 'samerole', 'samegroup', 'replication')
              THEN pg_catalog.current_database()::text
              ELSE '"' || pg_catalog.replace(pg_catalog.current_database(), '"', '""') || '"' END,
         CASE WHEN membership.member_name ~ '^[a-z0-9_]+$'
                   AND membership.member_name NOT IN ('all', 'sameuser', 'samerole', 'samegroup', 'replication')
              THEN membership.member_name::text
              ELSE '"' || pg_catalog.replace(membership.member_name, '"', '""') || '"' END;
   END LOOP;
END $do$;

-- 3i. Investigation tables (V184, #5632), mirroring managed provisioning's section 10d grant for grant. Step 3 and the
--     default privileges gave admin INSERT/UPDATE/DELETE on all six, so REVOKE ALL ... CASCADE takes every privilege
--     from PUBLIC and the four roles, and SELECT goes back to all four. mcp (the claim and submit tools) updates only
--     the claim and report columns of a request, inserts a report and a journal row, and proposes a note. The agent
--     tools (claim, submit) leave the note's state column to its default, pending, but the database does not hold mcp to
--     that: since #6400 mcp also holds INSERT on state, because the person-only investigation tools (write, approve and reject a
--     note, close a request, save a profile) run as mcp. What keeps an agent from them is the tool policy (NeverAgentTools), not this role. admin (the desktop Viewer) closes a
--     request, writes the environment profiles, writes, decides or deletes a note, and journals what it did. The
--     service writes the rest as the owner. On a store below V184 the block warns and skips, as the agent route column
--     grants in steps 2b and 2c do, so the whole script runs clean on a V181-V183 store; re-run it once the service has
--     migrated the store to V184.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.investigation_requests') IS NULL
      OR pg_catalog.to_regclass('config.investigation_request_alerts') IS NULL
      OR pg_catalog.to_regclass('config.investigation_reports') IS NULL
      OR pg_catalog.to_regclass('config.investigation_journal') IS NULL
      OR pg_catalog.to_regclass('config.investigation_notes') IS NULL
      OR pg_catalog.to_regclass('config.agent_environment_profiles') IS NULL THEN
      RAISE WARNING 'The investigation tables were not found, so their grants were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   REVOKE ALL ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports,
      config.investigation_journal, config.investigation_notes, config.agent_environment_profiles
      FROM PUBLIC, admin, viewer, web, mcp CASCADE;
   GRANT SELECT ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports,
      config.investigation_journal, config.investigation_notes, config.agent_environment_profiles
      TO admin, viewer, web;
   GRANT SELECT ON config.investigation_requests, config.investigation_request_alerts, config.investigation_reports,
      config.investigation_journal, config.investigation_notes, config.agent_environment_profiles
      TO mcp;
   GRANT UPDATE (state, claimed_by, claim_id, claimed_at, lease_until, claim_count, brief_format, brief, reported_at)
      ON config.investigation_requests TO mcp;
   -- #6008 (V193): submit_investigation_report writes follow_up_at onto the report's own request. Guarded on the column, so a
   -- store at V184 to V192 still takes the rest of this block; re-run the script once the service has migrated it to V193.
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 'config.investigation_requests'::regclass
                AND attname = 'follow_up_at' AND NOT attisdropped) THEN
      EXECUTE $v193u$
GRANT UPDATE (follow_up_at) ON config.investigation_requests TO mcp;
$v193u$;
   END IF;
   -- #6009 (V193): the deploy condition beside it, guarded the same way.
   IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 'config.investigation_requests'::regclass
                AND attname = 'follow_up_condition' AND NOT attisdropped) THEN
      EXECUTE $v193c$
GRANT UPDATE (follow_up_condition) ON config.investigation_requests TO mcp;
$v193c$;
   END IF;
   GRANT INSERT ON config.investigation_reports TO mcp;
   GRANT INSERT (event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key)
      ON config.investigation_journal TO mcp;
   GRANT INSERT (server_id, story_path_hash, dedup_key, metric_name, note_text, author, source_request_id)
      ON config.investigation_notes TO mcp;
   GRANT UPDATE (state, closed_at, closed_by, closed_reason) ON config.investigation_requests TO admin;
   GRANT INSERT (event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key)
      ON config.investigation_journal TO admin;
   GRANT INSERT, DELETE ON config.investigation_notes TO admin;
   GRANT UPDATE (state, decided_at, decided_by, expires_at) ON config.investigation_notes TO admin;
   -- #6400: the investigation admin tools (write, approve, reject a note; close a request) run as mcp: the columns admin's
   --     statements set and nothing more. BRING-YOUR-OWN STORES: re-run this script to add them.
   GRANT UPDATE (closed_at, closed_by, closed_reason) ON config.investigation_requests TO mcp;
   GRANT INSERT (state, created_at, expires_at, decided_at, decided_by) ON config.investigation_notes TO mcp;
   GRANT UPDATE (state, decided_at, decided_by) ON config.investigation_notes TO mcp;
   GRANT INSERT, UPDATE, DELETE ON config.agent_environment_profiles TO admin;
   -- #6400: save_environment_profile runs InvestigationRequestStore.SaveProfileAsync as mcp: the columns the statements set, and
   --     the delete a blank save does. BRING-YOUR-OWN STORES: re-run this script to add them.
   GRANT INSERT (scope_server_id, scope_tag_id, profile_text, modified_at, modified_by) ON config.agent_environment_profiles TO mcp;
   GRANT UPDATE (profile_text, modified_at, modified_by) ON config.agent_environment_profiles TO mcp;
   GRANT DELETE ON config.agent_environment_profiles TO mcp;
END $do$;

-- 3i (continued). #6003 review: the journal is the request event log the service signs and delivers, and mcp and admin
--     hold INSERT on it (the nine columns above: not journal_id, the identity, and not event_id, the webhook id, so a role
--     cannot fill the next million values and make every service insert fail), so this trigger (managed provisioning's
--     BuildJournalEventRulesSql, text for text) refuses a row from any role but the owner that is an event, a delivery
--     or fallback mark, a server action switch (#6269) or one of the three rows an agent action writes, action_called,
--     action_done and action_failed (#6569: the service writes them itself, on the owner login, and a person's Undo
--     acts on the action_called row) (but for a practice request's one opened row, which the Viewer writes with the
--     reason "practice of <case id>"), or that carries a once key other than the prefixes the tools and the Viewer write. A practice opened
--     row gets the once key practice_opened:<request id> from the trigger, so a second one fails on the unique index. A row
--     dated ahead of the store's clock is clamped to it. The alert links get the twin rule (BuildRequestAlertLinkRulesSql):
--     only a practice request takes a link from a role but the owner. Both read V191's practice_of, so on a store below
--     V191 the block warns and skips; re-run the script once the service has migrated the store.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.investigation_journal') IS NULL
      OR pg_catalog.to_regclass('config.investigation_request_alerts') IS NULL
      OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                     WHERE attrelid = pg_catalog.to_regclass('config.investigation_requests')
                       AND attname = 'practice_of' AND NOT attisdropped) THEN
      RAISE WARNING 'The investigation journal rules were skipped: the store is below V191. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   EXECUTE $ddl$
CREATE OR REPLACE FUNCTION config.investigation_journal_owner_rows()
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

   IF NEW.event IN ('opened', 'joined', 'resolved', 'expired', 'claims_exhausted', 'evidence_ready', 'follow_up', 'deadlock_digest', 'event_delivered', 'fallback_sent', 'server_action_switched', 'action_called', 'action_done', 'action_failed', 'person_added', 'person_renamed', 'person_enabled', 'person_disabled', 'person_identity_added', 'person_identity_removed', 'person_grant_added', 'person_grant_removed') THEN
      IF NEW.event = 'opened'
         AND EXISTS (SELECT 1 FROM config.investigation_requests AS r
                     WHERE r.request_id = NEW.request_id AND r.practice_of IS NOT NULL
                       AND NEW.reason = 'practice of ' || r.practice_of::text)
         AND NOT EXISTS (SELECT 1 FROM config.investigation_journal AS j
                         WHERE j.request_id = NEW.request_id AND j.event = 'opened') THEN
         v_practice := true;
      ELSE
         RAISE EXCEPTION 'Only the Darling service journals the request event %.', NEW.event USING ERRCODE = '42501';
      END IF;
   END IF;

   IF NEW.once_key IS NOT NULL AND split_part(NEW.once_key, ':', 1) NOT IN ('claimed', 'refused', 'agent_refused', 'journal_capped', 'link') THEN
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
$rules$
$ddl$;
   REVOKE ALL ON FUNCTION config.investigation_journal_owner_rows() FROM PUBLIC;
   CREATE OR REPLACE TRIGGER trg_investigation_journal_owner_rows
      BEFORE INSERT ON config.investigation_journal
      FOR EACH ROW EXECUTE FUNCTION config.investigation_journal_owner_rows();

   EXECUTE $ddl$
CREATE OR REPLACE FUNCTION config.investigation_request_alerts_owner_rows()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $rules$
BEGIN
   IF pg_has_role(current_user, (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = TG_RELID), 'USAGE') THEN
      RETURN NEW;
   END IF;

   IF NOT EXISTS (SELECT 1 FROM config.investigation_requests AS r
                  WHERE r.request_id = NEW.request_id AND r.practice_of IS NOT NULL) THEN
      RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'Only the Darling service links an alert to a request, and only a practice request takes a link from anyone else.';
   END IF;

   RETURN NEW;
END;
$rules$
$ddl$;
   REVOKE ALL ON FUNCTION config.investigation_request_alerts_owner_rows() FROM PUBLIC;
   CREATE OR REPLACE TRIGGER trg_investigation_request_alerts_owner_rows
      BEFORE INSERT ON config.investigation_request_alerts
      FOR EACH ROW EXECUTE FUNCTION config.investigation_request_alerts_owner_rows();
END $do$;

-- 3j. Level 2 tables (V191, #5909), mirroring managed provisioning's section 10e grant for grant. The default privileges
--     give admin INSERT/UPDATE/DELETE and viewer SELECT on a table made after them, but REVOKE ALL ... CASCADE (which also
--     takes TRIGGER and REFERENCES) and the explicit grants below make the result independent of when the table was made.
--     admin (the desktop Viewer) writes the threshold overrides and the report feedback. viewer reads both and writes
--     neither. mcp (the agent's change tools) writes the overrides only and reads the feedback; the overrides' reload
--     beacon trigger UPDATEs config_service.config_version as the caller, which mcp's two-column grant in step 3d covers.
--     The mcp write grant on the overrides is table-level on purpose (an override IS the agent's action, and the table has
--     no authority column to protect, unlike the notes' state column); see managed section 10e.
--     The two tables are created by V191: on a store below it the block warns and skips, so the rest of the script still
--     runs; re-run the script once the service has migrated the store to V191.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.config_threshold_overrides') IS NULL
      OR pg_catalog.to_regclass('config.investigation_feedback') IS NULL THEN
      RAISE WARNING 'The Level 2 tables were not found, so their grants were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   REVOKE ALL ON config.config_threshold_overrides, config.investigation_feedback FROM PUBLIC, admin, viewer, web, mcp CASCADE;
   GRANT SELECT ON config.config_threshold_overrides, config.investigation_feedback TO admin, viewer, web;
   GRANT SELECT ON config.config_threshold_overrides, config.investigation_feedback TO mcp;
   GRANT INSERT, UPDATE, DELETE ON config.config_threshold_overrides TO admin;
   GRANT INSERT, UPDATE, DELETE ON config.investigation_feedback TO admin;
   GRANT INSERT, UPDATE, DELETE ON config.config_threshold_overrides TO mcp;
   -- #6401: set_agent_actions_enabled (the MCP twin of the web's PUT /api/agent-changes/switch) updates the service row's switch and who set it.
   -- UPDATE only, column-level on exactly those columns, so mcp still cannot write paused, capture_plans or any other flag, and cannot create the row:
   -- the service creates it when it copies darling.json into the store, and a switch written before then must not make the store look set up.
   -- agent_actions_enabled arrives with V191, the version that makes the block above run.
   GRANT UPDATE (agent_actions_enabled, updated_by) ON config.config_service TO mcp;
   -- #5918 (V191's practice_of column, so here and not in 3i): the Viewer opens a practice request (a replay of a judged case) and copies the case's alert rows.
   GRANT INSERT (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
      window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, practice_of) ON config.investigation_requests TO admin;
   GRANT INSERT ON config.investigation_request_alerts TO admin;
   -- #6400: start_practice_request and record_investigation_feedback run the practice and feedback stores as mcp: the columns their
   --     statements set and nothing more. BRING-YOUR-OWN STORES: re-run this script to add them.
   GRANT INSERT (request_id, route_id, server_id, server_name, family, first_metric, story_path_hash, first_alert_at, last_alert_at,
      window_until, opened_at, expires_at, lease_minutes, autonomy_level, alert_count, practice_of) ON config.investigation_requests TO mcp;
   GRANT INSERT (request_id, alert_time, server_id, metric_name, dedup_key, current_value, threshold_value, muted, detail_text, context_json, severity)
      ON config.investigation_request_alerts TO mcp;
   GRANT INSERT (request_id, verdict, corrected_verdict, missed_step, comment, author) ON config.investigation_feedback TO mcp;
   GRANT UPDATE (verdict, corrected_verdict, missed_step, comment, author, created_at) ON config.investigation_feedback TO mcp;
END $do$;

-- 3j2. The per-server action switches (V196, #5632 step 5), mirroring managed provisioning's section 10e2. Every role reads them and
--     NO role but the store owner (the service) writes them: REVOKE ALL ... CASCADE takes every privilege (the default privileges
--     would give admin INSERT/UPDATE/DELETE on a table made after them), and SELECT goes back to all four. No MCP tool, web route or
--     darling.json key can turn a switch on. The table is created by V196: on a store below it the block warns and skips, so the
--     rest of the script still runs; re-run the script once the service has migrated the store to V196.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.config_server_actions') IS NULL THEN
      RAISE WARNING 'The per-server action switch table was not found, so its grants were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   REVOKE ALL ON config.config_server_actions FROM PUBLIC, admin, viewer, web, mcp CASCADE;
   GRANT SELECT ON config.config_server_actions TO admin, viewer, web;
   GRANT SELECT ON config.config_server_actions TO mcp;
END $do$;

-- 3j3. The people directory (V237, #7107), mirroring managed provisioning's section 10e3: the people, their identities and their grants.
--     Every role reads them and NO role but the store owner (the service, which journals each change) writes them: REVOKE ALL ... CASCADE
--     takes every privilege (the default privileges would give admin INSERT/UPDATE/DELETE on a table made after them), and SELECT goes
--     back to all four. The tables are created by V237: on a store below it the block warns and skips, so the rest of the script still
--     runs; re-run the script once the service has migrated the store to V237.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.person_grant') IS NULL THEN
      RAISE WARNING 'The people directory tables were not found, so their grants were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   REVOKE ALL ON config.person, config.person_identity, config.person_grant FROM PUBLIC, admin, viewer, web, mcp CASCADE;
   GRANT SELECT ON config.person, config.person_identity, config.person_grant TO admin, viewer, web;
   GRANT SELECT ON config.person, config.person_identity, config.person_grant TO mcp;
END $do$;

-- 3k. Query-tag tables (V192, #5977), mirroring managed provisioning's grants for them. mcp writes the three tables a person
--     edits: the tags, the hand assignments and the rules. The map and the sightings are the service's (the owner writes
--     them), so mcp holds only DELETE on the map, which remove_server runs to clear a removed server's map rows (a server_id is
--     a hash, so orphaned rows would come back when the server is re-added). web gets the same three writes (#5986: the web page that manages
--     query tags runs the MCP cores as web; the read-only seat gate decides who may call it) and DELETE on the map for server
--     removal, and nothing else on the query-tag tables. admin (the desktop Viewer's default identity) already holds the config writes
--     through the blanket grant of step 3. No role holds a write on the sightings in collect: like every other collect table,
--     a removed server's sightings are left to the owner's retention pass. The identity
--     columns need no sequence grant, and the rules_version bump on a write is a SECURITY DEFINER trigger function, so no
--     role is granted anything on config.query_tag_state. The tables are created by V192: on a store below it the block warns
--     and skips, so the rest of the script still runs; re-run the script once the service has migrated the store to V192.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.query_tags') IS NULL
      OR pg_catalog.to_regclass('config.query_tag_assignments') IS NULL
      OR pg_catalog.to_regclass('config.query_tag_rules') IS NULL
      OR pg_catalog.to_regclass('config.query_tag_map') IS NULL
      OR pg_catalog.to_regclass('collect.query_tag_sightings') IS NULL THEN
      RAISE WARNING 'The query-tag tables were not found, so their grants were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   GRANT INSERT, UPDATE, DELETE ON config.query_tags TO mcp;
   GRANT INSERT, UPDATE, DELETE ON config.query_tag_assignments TO mcp;
   GRANT INSERT, UPDATE, DELETE ON config.query_tag_rules TO mcp;
   GRANT DELETE ON config.query_tag_map TO mcp;
   REVOKE INSERT, UPDATE, DELETE ON config.query_tags FROM viewer;
   GRANT INSERT, UPDATE, DELETE ON config.query_tags TO web;
   REVOKE INSERT, UPDATE, DELETE ON config.query_tag_assignments FROM viewer;
   GRANT INSERT, UPDATE, DELETE ON config.query_tag_assignments TO web;
   REVOKE INSERT, UPDATE, DELETE ON config.query_tag_rules FROM viewer;
   GRANT INSERT, UPDATE, DELETE ON config.query_tag_rules TO web;
   REVOKE DELETE ON config.query_tag_map FROM viewer;
   GRANT DELETE ON config.query_tag_map TO web;
END $do$;

-- 3l. Phase C tables (V193, #5997), mirroring managed provisioning's section 10f grant for grant. Only the store owner (the
--     service) writes them. REVOKE ALL ... CASCADE (which also takes TRIGGER and REFERENCES) takes every privilege from PUBLIC
--     and the four roles, and SELECT goes back to all four: the web dashboard and the MCP tools read them. The merge table's two
--     grants (#6006) are the first such writes. A later slice that lets a role write one adds that single grant here and in
--     managed section 10f.
--     The three tables are created by V193: on a store below it the block warns and skips, so the rest of the script still
--     runs; re-run the script once the service has migrated the store to V193.
DO $do$
BEGIN
   IF pg_catalog.to_regclass('config.investigation_evidence') IS NULL
      OR pg_catalog.to_regclass('config.investigation_signature_days') IS NULL
      OR pg_catalog.to_regclass('config.investigation_signature_merges') IS NULL THEN
      RAISE WARNING 'The Phase C tables were not found, so their grants were skipped. Run this script again after the service has upgraded the store.';
      RETURN;
   END IF;

   REVOKE ALL ON config.investigation_evidence, config.investigation_signature_days, config.investigation_signature_merges FROM PUBLIC, admin, viewer, web, mcp CASCADE;
   GRANT SELECT ON config.investigation_evidence, config.investigation_signature_days, config.investigation_signature_merges TO admin, viewer, web;
   GRANT SELECT ON config.investigation_evidence, config.investigation_signature_days, config.investigation_signature_merges TO mcp;
   -- #6006: the one write a role gets here. The gated merge tool runs as mcp and adds a merge row, naming only the columns an
   -- agent supplies (merge_id and merged_at are the table's own); a person's undo runs as admin and stamps undone_at and undone_by.
   GRANT INSERT (server_id, signature, merged_into, reason, merged_by) ON config.investigation_signature_merges TO mcp;
   GRANT UPDATE (undone_at, undone_by) ON config.investigation_signature_merges TO admin;
   -- #6401: undo_agent_change undoes a signature merge the way the web's undo does, by stamping the row; so mcp gets the same two columns.
   GRANT UPDATE (undone_at, undone_by) ON config.investigation_signature_merges TO mcp;
END $do$;

-- 3g. The password key tables (V165, #5366): written only by the store owner (the service, its command line and the
--     migration runner); a trigger on each table refuses every other writer. This is the grant side of the same rule,
--     and it comes after EVERY grant above, so none of them can put a privilege back. REVOKE ALL ... CASCADE takes every
--     privilege on the four tables from PUBLIC and the four roles, TRIGGER and REFERENCES included, so no role can add
--     a new trigger or foreign key. A trigger or foreign key already there (made while a role held the privilege) is
--     dropped by the loops below, with a WARNING, because it would outlast the REVOKE. Then SELECT on the key and
--     service-state tables is granted back to admin only (the desktop Viewer reads them as admin), and the DO block
--     checks that nothing else is left; the pin tables are read by the owner only. The four tables are created by V165 and the
--     legacy pin candidate table by V167: on a store below V165 the block warns and skips, so the rest of the script
--     (the PUBLIC revoke below included) still runs; re-run the script once the service has migrated the store to V167.
--     A store at V165 or V166 gets the four-table revoke now, and the candidate table's own revoke once V167 has made it.
--     Step 2d has already taken the read grants away before the COMMIT; what follows is the part that has to come last.
DO $do$
DECLARE
    stray record;
BEGIN
    IF pg_catalog.to_regclass('config.password_key') IS NOT NULL
       AND pg_catalog.to_regclass('config.password_key_service') IS NOT NULL
       AND pg_catalog.to_regclass('config.legacy_secret_pin') IS NOT NULL
       AND pg_catalog.to_regclass('config.legacy_secret_pin_marker') IS NOT NULL THEN
        REVOKE ALL ON config.password_key, config.password_key_service,
           config.legacy_secret_pin, config.legacy_secret_pin_marker FROM PUBLIC, admin, viewer, web, mcp CASCADE;
        GRANT SELECT ON config.password_key, config.password_key_service TO admin;
        IF pg_catalog.to_regclass('config.legacy_secret_pin_candidate') IS NOT NULL THEN
            REVOKE ALL ON config.legacy_secret_pin_candidate FROM PUBLIC, admin, viewer, web, mcp CASCADE;
        END IF;
        -- V198 (#6066): the pseudonym key table. No role but the store owner reads it, so nothing is granted back.
        IF pg_catalog.to_regclass('config.pseudonym_key') IS NOT NULL THEN
            REVOKE ALL ON config.pseudonym_key FROM PUBLIC, admin, viewer, web, mcp CASCADE;
        END IF;

        FOR stray IN
           SELECT t.tgname, t.tgrelid::pg_catalog.regclass::pg_catalog.text AS tablename
           FROM pg_catalog.pg_trigger AS t
           WHERE NOT t.tgisinternal
             AND t.tgrelid IN (pg_catalog.to_regclass('config.password_key'), pg_catalog.to_regclass('config.password_key_service'), pg_catalog.to_regclass('config.legacy_secret_pin'), pg_catalog.to_regclass('config.legacy_secret_pin_marker'), pg_catalog.to_regclass('config.legacy_secret_pin_candidate'), pg_catalog.to_regclass('config.pseudonym_key'))
             AND t.tgname NOT IN ('trg_password_key_owner_only_row', 'trg_password_key_owner_only_truncate', 'trg_password_key_service_owner_only_row', 'trg_password_key_service_owner_only_truncate', 'trg_legacy_secret_pin_owner_only_row', 'trg_legacy_secret_pin_owner_only_truncate', 'trg_legacy_secret_pin_marker_owner_only_row', 'trg_legacy_secret_pin_marker_owner_only_truncate', 'trg_legacy_secret_pin_candidate_owner_only_row', 'trg_legacy_secret_pin_candidate_owner_only_truncate', 'trg_pseudonym_key_owner_only_row', 'trg_pseudonym_key_owner_only_truncate')
        LOOP
           RAISE WARNING 'Dropped trigger % from % because only the store owner may write the password key tables.', stray.tgname, stray.tablename;
           EXECUTE pg_catalog.format('DROP TRIGGER %I ON %s', stray.tgname, stray.tablename);
        END LOOP;

        FOR stray IN
           SELECT k.conname, k.conrelid::pg_catalog.regclass::pg_catalog.text AS tablename
           FROM pg_catalog.pg_constraint AS k
           WHERE k.contype = 'f'
             AND k.confrelid IN (pg_catalog.to_regclass('config.password_key'), pg_catalog.to_regclass('config.password_key_service'), pg_catalog.to_regclass('config.legacy_secret_pin'), pg_catalog.to_regclass('config.legacy_secret_pin_marker'), pg_catalog.to_regclass('config.legacy_secret_pin_candidate'), pg_catalog.to_regclass('config.pseudonym_key'))
        LOOP
           RAISE WARNING 'Dropped foreign key % on % because it references a password key table.', stray.conname, stray.tablename;
           EXECUTE pg_catalog.format('ALTER TABLE %s DROP CONSTRAINT %I', stray.tablename, stray.conname);
        END LOOP;

        IF EXISTS (
           SELECT 1
           FROM pg_catalog.pg_class AS c
           CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS a
           LEFT JOIN pg_catalog.pg_roles AS r ON r.oid = a.grantee
           WHERE c.oid IN (pg_catalog.to_regclass('config.password_key'), pg_catalog.to_regclass('config.password_key_service'), pg_catalog.to_regclass('config.legacy_secret_pin'), pg_catalog.to_regclass('config.legacy_secret_pin_marker'), pg_catalog.to_regclass('config.legacy_secret_pin_candidate'), pg_catalog.to_regclass('config.pseudonym_key'))
             AND (a.grantee = 0 OR r.rolname IN ('admin', 'viewer', 'web', 'mcp'))
             AND NOT (COALESCE(r.rolname = 'admin', false) AND a.privilege_type = 'SELECT'
                      AND c.oid IN (pg_catalog.to_regclass('config.password_key'), pg_catalog.to_regclass('config.password_key_service')))
        ) THEN
           RAISE EXCEPTION 'A role other than the store owner still holds a privilege it should not on a password key table.';
        END IF;
    ELSE
        RAISE WARNING 'The password key tables were not found, so their grants were skipped. Run this script again after the service has upgraded the store.';
    END IF;
END
$do$;

-- 4. Default privileges so NEW tables/views (future collectors, created bare into collect via
--    search_path) auto-inherit SELECT. FOR ROLE <owner> must name the role that creates them.
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect
   GRANT SELECT ON TABLES TO admin, viewer, web;
ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA config
   GRANT SELECT ON TABLES TO admin, viewer, web;
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
--    PUBLIC's implicit CONNECT, so the four roles are re-granted CONNECT explicitly. (Any other login
--    that reads this store directly needs its own GRANT CONNECT ON DATABASE darling after this.)
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE darling FROM PUBLIC;
GRANT CONNECT ON DATABASE darling TO admin, viewer, web;
GRANT CONNECT ON DATABASE darling TO mcp;

-- 6. Search path. The service best-effort runs this on every start, but if your collection login
--    lacks the database-owner privilege it needs, run it once yourself as the owner so every
--    connection (the Viewer, psql, pg_dump) resolves the bare table names to collect/config:
--
--       ALTER DATABASE darling SET search_path = collect, config, public;
--
-- 7. Not mirrored from managed mode, deliberately: the config.record_custom_alert_resolution function
--    (#3334) the managed batch creates for web and mcp. It exists for the custom-alert evaluator, which
--    runs on a managed store only; with no evaluator there is never a firing rule for a rule delete to
--    resolve.

-- 8. Function owner. The queue functions run as the owner of config.config_command, whoever runs this script: each definer-rights
--    function this script creates in schema config is created by the role running the script, which is a superuser on some
--    stores, so the last step hands each one to the table owner. A function already owned by that role is left alone.
DO $do$
DECLARE
   v_owner text;
   v_sig text;
BEGIN
   SELECT pg_catalog.pg_get_userbyid(c.relowner) INTO v_owner
   FROM pg_catalog.pg_class AS c
   WHERE c.oid = pg_catalog.to_regclass('config.config_command');
   IF v_owner IS NULL THEN
      RAISE WARNING 'The function owners were not changed: config.config_command does not exist yet.';
      RETURN;
   END IF;
   FOREACH v_sig IN ARRAY ARRAY[
   'config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text, text)',
   'config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric, text, text)',
   'config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric)',
   'config.insert_known_pattern(integer, text, time, text, integer, text, text, timestamp)',
   'config.update_known_pattern(bigint, integer, text, time, text, integer, text, timestamp, text)',
   'config.delete_known_pattern(bigint)',
   'config.request_snapshot_now(integer, text, text)',
   'config.request_active_queries_live(integer, text)',
   'config.request_analyze_now(integer, text)',
   'config.create_notification_route(text, boolean, integer, integer, boolean, integer, text, text)',
   'config.update_notification_route(integer, text, text[], text, boolean, integer, integer, boolean, integer, text, text)',
   'config.set_notification_route_agent(integer, text, text[], integer, integer, integer, integer, integer, integer, boolean, jsonb)',
   'config.set_notification_route_destinations(integer, text, text[], text, text, text, text, text)',
   'config.request_mcp_command(text, integer, integer, text)',
   'config.read_mcp_command_result(bigint)',
   'config.submit_external_alert(text, text, text, text, text, text, double precision)'
   ] LOOP
      IF (SELECT p.proowner FROM pg_catalog.pg_proc AS p WHERE p.oid = pg_catalog.to_regprocedure(v_sig))
         IS DISTINCT FROM (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = pg_catalog.to_regclass('config.config_command')) THEN
         EXECUTE pg_catalog.format('ALTER FUNCTION %s OWNER TO %I', v_sig, v_owner);
      END IF;
   END LOOP;
END
$do$;
