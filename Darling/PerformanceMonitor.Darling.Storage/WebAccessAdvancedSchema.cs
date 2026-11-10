/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The advanced web access switch: one row that says whether Viewer-managed group tokens may sign in at all, and the session
/// epoch every group cookie is bound to. Turning the switch either way takes a new epoch, so every group session ends with it.</summary>
public static class WebAccessAdvancedSchema
{
    /// <summary>The migration creating the switch. It follows <see cref="WebAccessGroupSchema.Version"/>; the only groups table it
    /// touches is the journal, where a flip writes its row.</summary>
    public const int Version = 259;

    /// <summary>DDL for the switch, its owner guard, the protected setter and the hardening function. Every statement is re-runnable, so a
    /// half-applied store finishes on the next attempt.</summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS config.web_access_advanced (
    id integer PRIMARY KEY CHECK (id=1),
    enabled boolean NOT NULL DEFAULT false,
    session_epoch uuid NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO config.web_access_advanced(id) VALUES(1) ON CONFLICT (id) DO NOTHING;
DROP TRIGGER IF EXISTS web_access_owner_guard ON config.web_access_advanced;
CREATE TRIGGER web_access_owner_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON config.web_access_advanced
FOR EACH STATEMENT EXECUTE FUNCTION config.web_access_owner_guard();
ALTER TABLE config.web_access_advanced ENABLE ALWAYS TRIGGER web_access_owner_guard;
-- A flip rolls the session epoch, which ends every group session, and writes a journal row under the switch's own
-- sentinel group id (the all-ones uuid, which no group has), with the database role that made it and when. Setting the
-- switch to its current value changes nothing.
CREATE OR REPLACE FUNCTION config.set_web_access_advanced(p_enabled boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE prior boolean;
BEGIN
    SELECT enabled INTO prior FROM config.web_access_advanced WHERE id=1 FOR UPDATE;
    IF NOT FOUND OR prior IS NOT DISTINCT FROM p_enabled THEN RETURN; END IF;
    UPDATE config.web_access_advanced SET enabled=p_enabled,session_epoch=gen_random_uuid() WHERE id=1;
    INSERT INTO config.web_access_group_events(group_id,event,actor,before_state,after_state)
        VALUES('ffffffff-ffff-ffff-ffff-ffffffffffff',
            CASE WHEN p_enabled THEN 'advanced_enabled' ELSE 'advanced_disabled' END,
            session_user,jsonb_build_object('enabled',prior),jsonb_build_object('enabled',p_enabled));
END $fn$;
-- Replayed after provisioning's blanket/default grants, and during the migration. The loop covers PUBLIC, the
-- product's four logins and any role that holds a grant on these objects, and nothing else on the cluster.
CREATE OR REPLACE FUNCTION config.harden_web_access_advanced() RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE r text; c text;
BEGIN
    FOR r IN
        WITH grantees AS (
            SELECT a.grantee FROM pg_catalog.pg_class o CROSS JOIN LATERAL pg_catalog.aclexplode(o.relacl) a
                WHERE o.oid='config.web_access_advanced'::regclass
            UNION
            SELECT a.grantee FROM pg_catalog.pg_attribute c CROSS JOIN LATERAL pg_catalog.aclexplode(c.attacl) a
                WHERE c.attrelid='config.web_access_advanced'::regclass
            UNION
            SELECT a.grantee FROM pg_catalog.pg_proc p CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
                WHERE p.pronamespace='config'::regnamespace AND p.proname IN ('set_web_access_advanced','harden_web_access_advanced')
        )
        SELECT 'PUBLIC'
        UNION
        SELECT quote_ident(rolname) FROM pg_catalog.pg_roles
        WHERE oid<>(SELECT relowner FROM pg_catalog.pg_class WHERE oid='config.web_access_advanced'::regclass)
          AND (rolname IN ('admin','viewer','web','mcp') OR oid IN (SELECT grantee FROM grantees))
    LOOP
        EXECUTE format('REVOKE ALL ON config.web_access_advanced FROM %s CASCADE',r);
        FOREACH c IN ARRAY ARRAY['id','enabled','session_epoch'] LOOP
            EXECUTE format('REVOKE ALL (%I) ON config.web_access_advanced FROM %s CASCADE',c,r);
        END LOOP;
        EXECUTE format('REVOKE ALL ON FUNCTION config.set_web_access_advanced(boolean),config.harden_web_access_advanced() FROM %s CASCADE',r);
    END LOOP;
    FOR r IN SELECT quote_ident(rolname) FROM pg_catalog.pg_roles WHERE rolname IN ('admin','web') LOOP
        EXECUTE format('GRANT SELECT ON config.web_access_advanced TO %s',r);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='admin') THEN
        GRANT EXECUTE ON FUNCTION config.set_web_access_advanced(boolean) TO admin;
    END IF;
END $fn$;
SELECT config.harden_web_access_advanced();
""";
}
