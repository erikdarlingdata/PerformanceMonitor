/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The advanced web access setter that names its caller and decides the confirm under its own lock (#9662). The MCP tool writes on the
/// service's store owner connection, so the journal row of a flip named the store owner and not the MCP caller. And the tool read the
/// switch before it wrote, so a switch turned off in between was turned back on with no confirm.
/// </summary>
public static class WebAccessAdvancedActor
{
    /// <summary>The migration adding the three argument setter. It follows <see cref="InvestigationChangeWindows.Version"/>.</summary>
    public const int Version = 319;

    /// <summary>The setter's three argument form, and the hardening function re-created so it covers every setter in the schema. The
    /// one argument form from <see cref="WebAccessAdvancedSchema"/> stays as it was, so the Viewer and an older build keep working and the
    /// V262 probe still finds it. Every statement is re-runnable.</summary>
    public const string CreateSql = """
-- Returns the value the switch held under the row lock, so the caller builds its answer from what was true when it wrote. Turning the switch
-- on from off takes p_confirmed: without it nothing changes and the prior value comes back. Setting the switch to its current value changes
-- nothing. p_actor names the journal row's actor, and session_user does when p_actor is NULL. The database role stays in database_actor.
CREATE OR REPLACE FUNCTION config.set_web_access_advanced(p_enabled boolean, p_actor text, p_confirmed boolean) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE prior boolean;
BEGIN
    IF p_enabled IS NULL OR p_confirmed IS NULL OR (p_actor IS NOT NULL AND (length(btrim(p_actor)) = 0 OR length(p_actor) > 256 OR p_actor ~ '[[:cntrl:]\u202A-\u202E\u2066-\u2069]')) THEN
        RAISE EXCEPTION USING ERRCODE='WG400', MESSAGE='Invalid advanced web access operation.';
    END IF;
    SELECT enabled INTO prior FROM config.web_access_advanced WHERE id=1 FOR UPDATE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    IF prior IS NOT DISTINCT FROM p_enabled OR (p_enabled AND NOT p_confirmed) THEN RETURN prior; END IF;
    UPDATE config.web_access_advanced SET enabled=p_enabled,session_epoch=gen_random_uuid() WHERE id=1;
    INSERT INTO config.web_access_group_events(group_id,event,actor,before_state,after_state)
        VALUES('ffffffff-ffff-ffff-ffff-ffffffffffff',
            CASE WHEN p_enabled THEN 'advanced_enabled' ELSE 'advanced_disabled' END,
            coalesce(p_actor,session_user),jsonb_build_object('enabled',prior),jsonb_build_object('enabled',p_enabled));
    RETURN prior;
END $fn$;
-- The one argument form is the Viewer's and the web path's, and stays what it was: it confirms nothing extra and names session_user.
CREATE OR REPLACE FUNCTION config.set_web_access_advanced(p_enabled boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
BEGIN
    PERFORM config.set_web_access_advanced(p_enabled, NULL::text, true);
END $fn$;
-- Replayed after provisioning's blanket/default grants, and during the migration. It walks the setters in the schema instead of naming one
-- signature, so every form is revoked from everyone. Admin gets the one argument form only: it is SECURITY DEFINER and calls the three
-- argument form as the store owner, so only the store owner can name a different actor.
CREATE OR REPLACE FUNCTION config.harden_web_access_advanced() RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE r text; c text; f text;
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
        FOR f IN SELECT p.oid::pg_catalog.regprocedure::text FROM pg_catalog.pg_proc p
                 WHERE p.pronamespace='config'::regnamespace AND p.proname IN ('set_web_access_advanced','harden_web_access_advanced') LOOP
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s CASCADE',f,r);
        END LOOP;
    END LOOP;
    FOR r IN SELECT quote_ident(rolname) FROM pg_catalog.pg_roles WHERE rolname IN ('admin','web') LOOP
        EXECUTE format('GRANT SELECT ON config.web_access_advanced TO %s',r);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='admin') THEN
        FOR f IN SELECT p.oid::pg_catalog.regprocedure::text FROM pg_catalog.pg_proc p
                 WHERE p.pronamespace='config'::regnamespace AND p.proname='set_web_access_advanced' AND p.pronargs=1 LOOP
            EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO admin',f);
        END LOOP;
    END IF;
END $fn$;
SELECT config.harden_web_access_advanced();
""";
}
