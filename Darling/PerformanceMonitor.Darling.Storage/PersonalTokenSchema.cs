/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// V272 (#8737): the store of personal MCP tokens, <c>config.mcp_personal_token</c>. One row per token: the sha256 of its secret (never the
/// secret), the owner key and subject it acts as, a name, a role, and its lifetime and revocation. Only the store owner reads or writes it.
/// </summary>
public static class PersonalTokenSchema
{
    /// <summary>The migration creating the table.</summary>
    public const int Version = 272;

    /// <summary>
    /// The table, its index and the owner-only lockdown, applied atomically by the migration runner. Every statement is written literally, with
    /// no dynamic SQL. The trigger function follows <c>config.password_key_owner_only()</c> (V165, which guards <c>config.pseudonym_key</c>):
    /// it tests <c>pg_has_role(session_user, &lt;table owner&gt;, 'USAGE')</c>, so a later GRANT made by mistake still cannot write. It has its
    /// own function, because the shared one raises <c>PW010</c> with a password-key message and these rows raise <c>42501</c>.
    /// </summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS config.mcp_personal_token
(
    token_id text NOT NULL,
    secret_sha256 bytea NOT NULL,
    owner_key text NOT NULL,
    subject text NOT NULL,
    token_name text NOT NULL,
    role text NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    last_used_at timestamptz,
    revoked_at timestamptz,
    revoked_by text,
    revoke_reason text,
    CONSTRAINT pk_mcp_personal_token PRIMARY KEY (token_id),
    CONSTRAINT ck_mcp_personal_token_id CHECK (token_id ~ '^[0-9a-f]{16}$'),
    CONSTRAINT ck_mcp_personal_token_hash CHECK (octet_length(secret_sha256) = 32),
    CONSTRAINT ck_mcp_personal_token_role CHECK (role IN ('operator', 'reader')),
    CONSTRAINT ck_mcp_personal_token_expiry CHECK (expires_at > created_at)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS mcp_personal_token_owner_key ON config.mcp_personal_token (owner_key);

REVOKE ALL ON config.mcp_personal_token FROM PUBLIC;

DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'admin') THEN
        REVOKE ALL ON config.mcp_personal_token FROM admin;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'viewer') THEN
        REVOKE ALL ON config.mcp_personal_token FROM viewer;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'web') THEN
        REVOKE ALL ON config.mcp_personal_token FROM web;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'mcp') THEN
        REVOKE ALL ON config.mcp_personal_token FROM mcp;
    END IF;
END
$do$;

CREATE OR REPLACE FUNCTION config.mcp_personal_token_owner_only() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $fn$
BEGIN
    IF pg_catalog.pg_has_role(
               session_user,
               (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = TG_RELID),
               'USAGE') IS NOT TRUE THEN
        RAISE EXCEPTION 'Only the store owner can change the personal MCP tokens.' USING ERRCODE = '42501';
    END IF;

    IF TG_LEVEL = 'STATEMENT' THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$fn$;

REVOKE ALL ON FUNCTION config.mcp_personal_token_owner_only() FROM PUBLIC;

DROP TRIGGER IF EXISTS trg_mcp_personal_token_owner_only_row ON config.mcp_personal_token;
CREATE TRIGGER trg_mcp_personal_token_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.mcp_personal_token
    FOR EACH ROW EXECUTE FUNCTION config.mcp_personal_token_owner_only();
DROP TRIGGER IF EXISTS trg_mcp_personal_token_owner_only_truncate ON config.mcp_personal_token;
CREATE TRIGGER trg_mcp_personal_token_owner_only_truncate
    BEFORE TRUNCATE ON config.mcp_personal_token
    FOR EACH STATEMENT EXECUTE FUNCTION config.mcp_personal_token_owner_only();
ALTER TABLE config.mcp_personal_token ENABLE ALWAYS TRIGGER trg_mcp_personal_token_owner_only_row;
ALTER TABLE config.mcp_personal_token ENABLE ALWAYS TRIGGER trg_mcp_personal_token_owner_only_truncate;
""";

    /// <summary>
    /// The managed-roles re-revoke: takes every privilege on the table from PUBLIC and the four roles, and grants nothing back. It runs after every
    /// blanket grant, so none of them can put a privilege back. Guarded, so a store below V272 is left alone.
    /// </summary>
    public static string ReassertPermissionsSql(string config, string admin, string viewer, string web, string mcp) => $"""
DO $personal_token$
BEGIN
    IF pg_catalog.to_regclass('{config}.mcp_personal_token') IS NOT NULL THEN
        REVOKE ALL ON {config}.mcp_personal_token FROM PUBLIC, {admin}, {viewer}, {web}, {mcp} CASCADE;
    END IF;
END $personal_token$;
""";
}
