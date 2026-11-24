/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// V275 (#8737): the store of personal MCP tokens, <c>config.mcp_personal_token</c>. One row per token: the sha256 of its secret (never the
/// secret), the owner key and subject it acts as, a name, a role, and its lifetime and revocation. Only the store owner reads or writes it.
/// </summary>
public static class PersonalTokenSchema
{
    /// <summary>The migration creating the table.</summary>
    public const int Version = 275;

    /// <summary>
    /// The table, its index and the owner-only lockdown, applied atomically by the migration runner. Every statement is written literally, with
    /// no dynamic SQL except the revoke block, which finds the roles holding a grant at run time (<see cref="RevokeFromEveryRoleSql"/>). The trigger function follows <c>config.password_key_owner_only()</c> (V165, which guards <c>config.pseudonym_key</c>):
    /// it tests <c>pg_has_role(session_user, &lt;table owner&gt;, 'USAGE')</c>, so a later GRANT made by mistake still cannot write. It has its
    /// own function, because the shared one raises <c>PW010</c> with a password-key message and these rows raise <c>42501</c>.
    /// </summary>
    public static readonly string CreateSql = CreateSqlHead + RevokeFromEveryRoleSql("config", "admin", "viewer", "web", "mcp") + CreateSqlTail;

    private const string CreateSqlHead = """
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

""";

    private const string CreateSqlTail = """

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
    /// The managed-roles re-revoke: takes every privilege on the table, and on each of its columns, from PUBLIC, the four roles and every other role
    /// that holds a grant on it, and grants nothing back. It runs after every blanket grant, so none of them can put a privilege back. Guarded, so a
    /// store below V275 is left alone.
    /// </summary>
    public static string ReassertPermissionsSql(string config, string admin, string viewer, string web, string mcp) =>
        RevokeFromEveryRoleSql(config, admin, viewer, web, mcp);

    /// <summary>Doubles single quotes, for a value written into a SQL string literal.</summary>
    private static string Lit(string value) => value.Replace("'", "''", System.StringComparison.Ordinal);

    /// <summary>
    /// Closes the table to every role but its owner, in the pattern of <c>config.harden_web_access_groups()</c>: custom default privileges can grant a
    /// new <c>config</c> table to roles beyond the product's four logins, so the roles to strip are PUBLIC, the four named ones and whatever
    /// <c>aclexplode</c> finds on the table or its columns. Used by the rung and by the managed-roles batch (and mirrored in
    /// <c>Darling/tools/provision-roles.sql</c>). Guarded on the table existing.
    /// </summary>
    internal static string RevokeFromEveryRoleSql(string config, string admin, string viewer, string web, string mcp) => $"""
DO $personal_token$
DECLARE
    r text;
    c text;
BEGIN
    IF pg_catalog.to_regclass('{config}.mcp_personal_token') IS NOT NULL THEN
        FOR r IN
            WITH grantees AS (
                SELECT a.grantee FROM pg_catalog.pg_class AS o CROSS JOIN LATERAL pg_catalog.aclexplode(o.relacl) AS a
                WHERE o.oid = pg_catalog.to_regclass('{config}.mcp_personal_token')
                UNION
                SELECT a.grantee FROM pg_catalog.pg_attribute AS t CROSS JOIN LATERAL pg_catalog.aclexplode(t.attacl) AS a
                WHERE t.attrelid = pg_catalog.to_regclass('{config}.mcp_personal_token')
            )
            SELECT 'PUBLIC'
            UNION
            SELECT pg_catalog.quote_ident(rolname) FROM pg_catalog.pg_roles
            WHERE oid <> (SELECT relowner FROM pg_catalog.pg_class WHERE oid = pg_catalog.to_regclass('{config}.mcp_personal_token'))
              AND (rolname IN ('{Lit(admin)}', '{Lit(viewer)}', '{Lit(web)}', '{Lit(mcp)}') OR oid IN (SELECT grantee FROM grantees))
        LOOP
            EXECUTE pg_catalog.format('REVOKE ALL ON {config}.mcp_personal_token FROM %s CASCADE', r);
            FOR c IN SELECT attname FROM pg_catalog.pg_attribute
                     WHERE attrelid = pg_catalog.to_regclass('{config}.mcp_personal_token') AND attnum > 0 AND NOT attisdropped LOOP
                EXECUTE pg_catalog.format('REVOKE ALL (%I) ON {config}.mcp_personal_token FROM %s CASCADE', c, r);
            END LOOP;
        END LOOP;
    END IF;
END $personal_token$;
""";
}
