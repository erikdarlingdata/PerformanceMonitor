/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Protected storage for Viewer-managed reader credentials. No migration changes existing tokens.</summary>
public static class WebAccessGroupSchema
{
    /// <summary>The migration creating the managed team credential control plane.</summary>
    public const int Version = 258;

    /// <summary>DDL and protected mutation functions, applied atomically by the migration runner.</summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS config.web_access_groups (
    group_id uuid PRIMARY KEY CHECK (group_id <> '00000000-0000-0000-0000-000000000000'),
    name text NOT NULL UNIQUE CHECK (name ~ '^[A-Za-z0-9._:-]{1,64}$'),
    scope_kind text NOT NULL CHECK (scope_kind IN ('all_servers', 'tags')),
    enabled boolean NOT NULL,
    token_generation uuid NOT NULL CHECK (token_generation <> '00000000-0000-0000-0000-000000000000'),
    sealed_token text NOT NULL CHECK (sealed_token LIKE 'sealed:v1:%'),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    activation_state text NOT NULL DEFAULT 'pending' CHECK (activation_state IN ('pending','active','refused')),
    refusal_code text,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);
CREATE TABLE IF NOT EXISTS config.web_access_group_tags (
    group_id uuid NOT NULL REFERENCES config.web_access_groups(group_id) ON DELETE CASCADE,
    tag_id integer NOT NULL REFERENCES config.server_tags(id),
    PRIMARY KEY (group_id, tag_id)
);
CREATE INDEX IF NOT EXISTS ix_web_access_group_tags_tag ON config.web_access_group_tags(tag_id);
CREATE TABLE IF NOT EXISTS config.web_access_group_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id uuid NOT NULL,
    event text NOT NULL CHECK (event IN ('created','updated','rotated','deleted','activated','refused','tag_deleted')),
    event_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    database_actor text NOT NULL DEFAULT session_user,
    actor text NOT NULL,
    before_state jsonb,
    after_state jsonb
);
CREATE INDEX IF NOT EXISTS ix_web_access_group_events_group ON config.web_access_group_events(group_id,event_id);

-- Defense in depth against a blanket/default grant putting DML back on these tables.
-- SECURITY DEFINER management functions run as the table owner; ordinary callers do not.
CREATE OR REPLACE FUNCTION config.web_access_owner_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
    IF current_user <> pg_catalog.pg_get_userbyid(
        (SELECT relowner FROM pg_catalog.pg_class WHERE oid = TG_RELID)) THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='Web access groups are changed only through their protected functions.';
    END IF;
    RETURN NULL;
END $fn$;
DO $ddl$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['web_access_groups','web_access_group_tags','web_access_group_events'] LOOP
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS web_access_owner_guard ON config.%I',t);
        EXECUTE pg_catalog.format('CREATE TRIGGER web_access_owner_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON config.%I FOR EACH STATEMENT EXECUTE FUNCTION config.web_access_owner_guard()',t);
        EXECUTE pg_catalog.format('ALTER TABLE config.%I ENABLE ALWAYS TRIGGER web_access_owner_guard',t);
    END LOOP;
END $ddl$;

-- Safe snapshots used only by owner functions to journal metadata, never the credential.
CREATE OR REPLACE FUNCTION config.web_access_snapshot(p_id uuid) RETURNS jsonb
LANGUAGE sql SET search_path = pg_catalog, pg_temp AS $fn$
    SELECT pg_catalog.jsonb_build_object(
        'name', g.name, 'scope_kind', g.scope_kind, 'enabled', g.enabled,
        'revision', g.revision, 'activation_state', g.activation_state, 'token_generation', g.token_generation,
        'tag_ids', COALESCE((SELECT pg_catalog.jsonb_agg(t.tag_id ORDER BY t.tag_id)
            FROM config.web_access_group_tags t WHERE t.group_id = g.group_id), '[]'::jsonb))
    FROM config.web_access_groups g WHERE g.group_id = p_id
$fn$;

CREATE OR REPLACE FUNCTION config.manage_web_access_group(
    p_action text, p_id uuid, p_revision bigint, p_name text, p_scope text,
    p_tags integer[], p_generation uuid, p_sealed text, p_enabled boolean, p_actor text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
    g config.web_access_groups%ROWTYPE;
    prior jsonb;
    new_revision bigint;
BEGIN
    IF p_action IS NULL OR p_action NOT IN ('create','update','rotate','delete')
        OR p_id IS NULL OR p_id = '00000000-0000-0000-0000-000000000000'
        OR p_actor IS NULL OR length(btrim(p_actor)) = 0 OR length(p_actor) > 256 THEN
        RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='Invalid web access group operation.';
    END IF;
    IF p_action IN ('create','update') THEN
        IF p_scope IS NULL OR p_scope NOT IN ('all_servers','tags') OR p_enabled IS NULL
            OR p_tags IS NULL OR cardinality(p_tags) > 1000
            OR EXISTS (SELECT 1 FROM unnest(p_tags) x WHERE x IS NULL OR x <= 0)
            OR (p_scope='all_servers' AND cardinality(p_tags) <> 0)
            OR (p_action='create' AND p_scope='tags' AND cardinality(p_tags)=0) THEN
            RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='Choose All servers or one or more existing server tags.';
        END IF;
        PERFORM id FROM config.server_tags WHERE id=ANY(p_tags) ORDER BY id FOR KEY SHARE;
        IF EXISTS (SELECT 1 FROM unnest(p_tags) x WHERE NOT EXISTS
            (SELECT 1 FROM config.server_tags t WHERE t.id=x)) THEN
            RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='A selected server tag no longer exists.';
        END IF;
    END IF;
    -- Tag locks precede the group administration lock, matching tag deletion's
    -- row lock then cleanup trigger. Never wait for a tag while holding this lock.
    PERFORM pg_catalog.pg_advisory_xact_lock(7988, 1);
    IF p_action <> 'create' THEN
        SELECT * INTO g FROM config.web_access_groups WHERE group_id=p_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE='WG404', MESSAGE='Web access group not found.';
        END IF;
        IF p_revision IS NULL OR p_revision <> g.revision THEN
            RAISE EXCEPTION USING ERRCODE='WG409', MESSAGE='Web access group changed; reload and retry.';
        END IF;
        prior := config.web_access_snapshot(p_id);
    END IF;
    IF p_action IN ('create','rotate') AND
        (p_generation IS NULL OR p_generation='00000000-0000-0000-0000-000000000000'
         OR p_sealed IS NULL OR p_sealed NOT LIKE 'sealed:v1:%' OR length(p_sealed)>20000) THEN
        RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='A new generation and sealed token are required.';
    END IF;
    IF p_action='create' THEN
        IF p_name IS NULL OR p_name !~ '^[A-Za-z0-9._:-]{1,64}$'
            OR lower(p_name) IN ('web','mcp','cli','viewer','invalid','unclaimed') THEN
            RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='Invalid or reserved web token name.';
        END IF;
        -- The journal is also an identity tombstone. Recreating a deleted ID could revive an old cookie.
        IF EXISTS (SELECT 1 FROM config.web_access_groups WHERE group_id=p_id OR name=p_name)
            OR EXISTS (SELECT 1 FROM config.web_access_group_events WHERE group_id=p_id) THEN
            RAISE EXCEPTION USING ERRCODE='WG409', MESSAGE='Web access group identity or name already exists.';
        END IF;
        INSERT INTO config.web_access_groups(group_id,name,scope_kind,enabled,token_generation,sealed_token)
            VALUES(p_id,p_name,p_scope,p_enabled,p_generation,p_sealed) RETURNING revision INTO new_revision;
    ELSIF p_action='update' THEN
        UPDATE config.web_access_groups SET scope_kind=p_scope,enabled=p_enabled,
            revision=revision+1,updated_at=now() AT TIME ZONE 'UTC'
            WHERE group_id=p_id RETURNING revision INTO new_revision;
        DELETE FROM config.web_access_group_tags WHERE group_id=p_id;
    ELSIF p_action='rotate' THEN
        -- Never cycle back to a previous generation, even if the caller supplies a newly sealed value.
        IF p_generation=g.token_generation OR EXISTS (SELECT 1 FROM config.web_access_group_events
            WHERE group_id=p_id AND after_state->>'token_generation'=p_generation::text) THEN
            RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='Rotation requires an unused token generation.';
        END IF;
        UPDATE config.web_access_groups SET token_generation=p_generation,sealed_token=p_sealed,
            activation_state='pending',refusal_code=NULL,revision=revision+1,updated_at=now() AT TIME ZONE 'UTC'
            WHERE group_id=p_id RETURNING revision INTO new_revision;
    ELSE
        DELETE FROM config.web_access_groups WHERE group_id=p_id;
        new_revision := g.revision+1;
    END IF;
    IF p_action IN ('create','update') THEN
        INSERT INTO config.web_access_group_tags(group_id,tag_id)
            SELECT p_id, x FROM (SELECT DISTINCT unnest(p_tags) AS x) tags;
    END IF;
    INSERT INTO config.web_access_group_events(group_id,event,actor,before_state,after_state)
        VALUES(p_id,CASE p_action WHEN 'create' THEN 'created' WHEN 'update' THEN 'updated'
            WHEN 'rotate' THEN 'rotated' ELSE 'deleted' END,p_actor,prior,config.web_access_snapshot(p_id));
    UPDATE config.config_service SET config_version=config_version+1 WHERE id=1;
    RETURN new_revision;
END $fn$;

-- Only the service owner can activate, after opening/checking the sealed credential.
-- Revision + generation prevent a stale loader from resurrecting deleted/rotated groups.
CREATE OR REPLACE FUNCTION config.activate_web_access_group(
    p_id uuid, p_revision bigint, p_generation uuid, p_refusal text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE prior jsonb;
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(7988,1);
    IF p_refusal IS NOT NULL AND p_refusal NOT IN
        ('invalid_token','unreadable_token','duplicate_token','duplicate_name','key_unavailable') THEN
        RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='Invalid token activation status.';
    END IF;
    PERFORM 1 FROM config.web_access_groups WHERE group_id=p_id AND revision=p_revision
        AND token_generation=p_generation
        AND (activation_state='pending' OR (p_refusal IS NOT NULL AND activation_state='active')) FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    prior := config.web_access_snapshot(p_id);
    UPDATE config.web_access_groups SET activation_state=CASE WHEN p_refusal IS NULL THEN 'active' ELSE 'refused' END,
        refusal_code=p_refusal,revision=revision+1,updated_at=now() AT TIME ZONE 'UTC' WHERE group_id=p_id;
    INSERT INTO config.web_access_group_events(group_id,event,actor,before_state,after_state)
        VALUES(p_id,CASE WHEN p_refusal IS NULL THEN 'activated' ELSE 'refused' END,
            'service',prior,config.web_access_snapshot(p_id));
    UPDATE config.config_service SET config_version=config_version+1 WHERE id=1;
    RETURN true;
END $fn$;

-- A tag deletion must remove grants even when issued by the tag editor's narrower role.
-- The trigger runs as owner, so callers never need DML on the protected grant tables.
-- A tag no group grants is the common case: no lock is taken, no journal row is written and the
-- service's configuration version is left alone. A grant inserted after the check holds FOR KEY SHARE
-- on the tag row, so the row delete behind this trigger fails on the foreign key rather than leaving
-- a grant behind, and the next attempt sees it.
CREATE OR REPLACE FUNCTION config.web_access_tag_deleted() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE g uuid; prior jsonb; touched boolean := false;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM config.web_access_group_tags WHERE tag_id=OLD.id) THEN
        RETURN OLD;
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(7988,1);
    FOR g IN SELECT group_id FROM config.web_access_group_tags WHERE tag_id=OLD.id ORDER BY group_id LOOP
        prior := config.web_access_snapshot(g);
        DELETE FROM config.web_access_group_tags WHERE group_id=g AND tag_id=OLD.id;
        UPDATE config.web_access_groups SET revision=revision+1,updated_at=now() AT TIME ZONE 'UTC' WHERE group_id=g;
        INSERT INTO config.web_access_group_events(group_id,event,actor,before_state,after_state)
            VALUES(g,'tag_deleted','server tag deletion',prior,config.web_access_snapshot(g));
        touched := true;
    END LOOP;
    IF touched THEN
        UPDATE config.config_service SET config_version=config_version+1 WHERE id=1;
    END IF;
    RETURN OLD;
END $fn$;
DROP TRIGGER IF EXISTS web_access_tag_deleted ON config.server_tags;
CREATE TRIGGER web_access_tag_deleted BEFORE DELETE ON config.server_tags
    FOR EACH ROW EXECUTE FUNCTION config.web_access_tag_deleted();
ALTER TABLE config.server_tags ENABLE ALWAYS TRIGGER web_access_tag_deleted;

-- Replayed after provisioning's blanket/default grants, and during the migration.
-- PUBLIC's default function EXECUTE must be removed in this same transaction.
CREATE OR REPLACE FUNCTION config.harden_web_access_groups() RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE r text; t text; c text; f record; seq text;
BEGIN
    -- Custom default privileges can grant a new table/function to roles beyond the
    -- product's four logins. Strip those grants on THESE objects only, then allowlist
    -- metadata reads and admin mutation functions. Never alter other config objects.
    FOR r IN
        WITH objects AS (
            SELECT oid,relacl FROM pg_catalog.pg_class WHERE oid IN (
                'config.web_access_groups'::regclass,'config.web_access_group_tags'::regclass,
                'config.web_access_group_events'::regclass,
                pg_catalog.pg_get_serial_sequence('config.web_access_group_events','event_id')::regclass)
        ), grantees AS (
            SELECT a.grantee FROM objects o CROSS JOIN LATERAL pg_catalog.aclexplode(o.relacl) a
            UNION
            SELECT a.grantee FROM pg_catalog.pg_attribute c JOIN objects o ON c.attrelid=o.oid
                CROSS JOIN LATERAL pg_catalog.aclexplode(c.attacl) a
            UNION
            SELECT a.grantee FROM pg_catalog.pg_proc p CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
                WHERE p.pronamespace='config'::regnamespace AND p.proname IN
                ('web_access_owner_guard','web_access_snapshot','manage_web_access_group',
                 'activate_web_access_group','web_access_tag_deleted','harden_web_access_groups')
        )
        SELECT 'PUBLIC'
        UNION
        SELECT quote_ident(rolname) FROM pg_catalog.pg_roles
        WHERE oid<>(SELECT relowner FROM pg_catalog.pg_class WHERE oid='config.web_access_groups'::regclass)
          AND (rolname IN ('admin','viewer','web','mcp') OR oid IN (SELECT grantee FROM grantees))
    LOOP
        FOREACH t IN ARRAY ARRAY['web_access_groups','web_access_group_tags','web_access_group_events'] LOOP
            EXECUTE format('REVOKE ALL ON config.%I FROM %s CASCADE',t,r);
            FOR c IN SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid=format('config.%I',t)::regclass AND attnum>0 AND NOT attisdropped LOOP
                EXECUTE format('REVOKE ALL (%I) ON config.%I FROM %s CASCADE',c,t,r);
            END LOOP;
        END LOOP;
        seq := pg_catalog.pg_get_serial_sequence('config.web_access_group_events','event_id');
        EXECUTE format('REVOKE ALL ON SEQUENCE %s FROM %s CASCADE',seq,r);
        FOR f IN SELECT oid::regprocedure AS sig FROM pg_catalog.pg_proc
            WHERE pronamespace='config'::regnamespace AND proname IN
            ('web_access_owner_guard','web_access_snapshot','manage_web_access_group',
             'activate_web_access_group','web_access_tag_deleted','harden_web_access_groups') LOOP
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s CASCADE',f.sig,r);
        END LOOP;
    END LOOP;
    FOR r IN SELECT quote_ident(rolname) FROM pg_catalog.pg_roles WHERE rolname IN ('admin','web') LOOP
        EXECUTE format('GRANT SELECT (group_id,name,scope_kind,enabled,token_generation,revision,activation_state,refusal_code,created_at,updated_at) ON config.web_access_groups TO %s',r);
        EXECUTE format('GRANT SELECT ON config.web_access_group_tags TO %s',r);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='admin') THEN
        GRANT SELECT ON config.web_access_group_events TO admin;
        GRANT EXECUTE ON FUNCTION config.manage_web_access_group(text,uuid,bigint,text,text,integer[],uuid,text,boolean,text) TO admin;
    END IF;
END $fn$;
SELECT config.harden_web_access_groups();
""";

    /// <summary>Reassert the credential carve AFTER every blanket/default role grant.</summary>
    public const string ReassertPermissionsSql = """
DO $groups$
BEGIN
    IF pg_catalog.to_regprocedure('config.harden_web_access_groups()') IS NOT NULL THEN
        PERFORM config.harden_web_access_groups();
        IF pg_catalog.to_regprocedure('config.harden_web_access_advanced()') IS NOT NULL THEN PERFORM config.harden_web_access_advanced(); END IF;
    END IF;
END $groups$;
""";
}
