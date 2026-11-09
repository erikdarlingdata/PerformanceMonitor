/* Copyright (c) 2026 Erik Darling, Darling Data LLC
 * Licensed under the MIT License. See LICENSE in the project root. */
namespace PerformanceMonitor.Darling.Storage;

/// <summary>Administrator-only recovery envelopes for repeatable Viewer token handover.</summary>
public static class WebAccessExportSchema
{
    public const string CreateSql = """
CREATE TABLE config.web_access_exports (
    group_id uuid PRIMARY KEY REFERENCES config.web_access_groups ON DELETE CASCADE,
    generation uuid NOT NULL,
    envelope text,
    description text NOT NULL DEFAULT '',
    dashboard_address text NOT NULL DEFAULT ''
);
CREATE TABLE config.web_access_export_keys (
    group_id uuid PRIMARY KEY REFERENCES config.web_access_groups ON DELETE CASCADE,
    generation uuid NOT NULL,
    recovery_key text NOT NULL
);
CREATE TRIGGER web_access_owner_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON config.web_access_exports
FOR EACH STATEMENT EXECUTE FUNCTION config.web_access_owner_guard();
CREATE TRIGGER web_access_owner_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON config.web_access_export_keys
FOR EACH STATEMENT EXECUTE FUNCTION config.web_access_owner_guard();
CREATE OR REPLACE FUNCTION config.web_access_export(p_action text,p_id uuid,p_generation uuid,p_envelope text,p_description text,p_address text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE g config.web_access_groups%ROWTYPE; result text;
BEGIN
    PERFORM pg_advisory_xact_lock(7988,1);
    SELECT * INTO g FROM config.web_access_groups WHERE group_id=p_id FOR UPDATE;
    IF NOT FOUND OR g.token_generation<>p_generation THEN
        RAISE EXCEPTION USING ERRCODE='WG409',MESSAGE='The group token changed. Refresh and retry.';
    END IF;
    IF p_action='store' THEN
        IF p_envelope IS NULL OR length(p_envelope)>20000 OR cardinality(string_to_array(p_envelope,'.'))<>4 THEN RAISE EXCEPTION 'Invalid recovery envelope'; END IF;
        INSERT INTO config.web_access_export_keys VALUES(p_id,p_generation,split_part(p_envelope,'.',1))
        ON CONFLICT(group_id) DO UPDATE SET generation=excluded.generation,recovery_key=excluded.recovery_key;
        INSERT INTO config.web_access_exports(group_id,generation,envelope)
        VALUES(p_id,p_generation,substring(p_envelope from position('.' in p_envelope)+1))
        ON CONFLICT(group_id) DO UPDATE SET generation=excluded.generation,envelope=excluded.envelope;
    ELSIF p_action='metadata' THEN
        INSERT INTO config.web_access_exports(group_id,generation,description,dashboard_address)
        VALUES(p_id,p_generation,COALESCE(p_description,''),COALESCE(p_address,''))
        ON CONFLICT(group_id) DO UPDATE SET description=excluded.description,dashboard_address=excluded.dashboard_address;
    ELSIF p_action='read' THEN
        SELECT json_build_object('description',description,'address',dashboard_address)::text INTO result
        FROM config.web_access_exports WHERE group_id=p_id AND generation=p_generation;
    ELSIF p_action='export' THEN
        IF NOT g.enabled OR g.activation_state<>'active' THEN
            RAISE EXCEPTION USING ERRCODE='WG400',MESSAGE='Only an active enabled token can be exported.';
        END IF;
        SELECT k.recovery_key || '.' || e.envelope INTO result FROM config.web_access_exports e
        JOIN config.web_access_export_keys k USING(group_id,generation)
        WHERE e.group_id=p_id AND e.generation=p_generation;
        IF result IS NOT NULL THEN
            INSERT INTO config.web_access_group_events(group_id,event,actor) VALUES(p_id,'exported','viewer');
        END IF;
    ELSE RAISE EXCEPTION 'Invalid export operation';
    END IF;
    RETURN result;
END $fn$;
CREATE OR REPLACE FUNCTION config.manage_web_access_group_recoverable(
    p_action text,p_id uuid,p_revision bigint,p_name text,p_scope text,p_tags integer[],
    p_generation uuid,p_sealed text,p_enabled boolean,p_envelope text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE result bigint;
BEGIN
    IF p_action NOT IN ('create','rotate') OR p_action IS NULL THEN RAISE EXCEPTION 'Invalid recovery operation'; END IF;
    result := config.manage_web_access_group(p_action,p_id,p_revision,p_name,p_scope,p_tags,p_generation,p_sealed,p_enabled,'viewer');
    PERFORM config.web_access_export('store',p_id,p_generation,p_envelope,NULL,NULL);
    RETURN result;
END $fn$;
ALTER TABLE config.web_access_group_events DROP CONSTRAINT web_access_group_events_event_check;
ALTER TABLE config.web_access_group_events ADD CHECK(event IN ('created','updated','rotated','deleted','activated','refused','tag_deleted','exported'));
CREATE OR REPLACE FUNCTION config.harden_web_access_exports() RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE r text; t text; c text;
BEGIN
    FOR r IN SELECT 'PUBLIC' UNION SELECT quote_ident(rolname) FROM pg_roles
        WHERE oid<>(SELECT relowner FROM pg_class WHERE oid='config.web_access_exports'::regclass) LOOP
        FOREACH t IN ARRAY ARRAY['web_access_exports','web_access_export_keys'] LOOP
            EXECUTE format('REVOKE ALL ON config.%I FROM %s CASCADE',t,r);
            FOR c IN SELECT attname FROM pg_attribute
                WHERE attrelid=format('config.%I',t)::regclass AND attnum>0 AND NOT attisdropped LOOP
                EXECUTE format('REVOKE ALL (%I) ON config.%I FROM %s CASCADE',c,t,r);
            END LOOP;
        END LOOP;
        EXECUTE format('REVOKE ALL ON FUNCTION config.web_access_export(text,uuid,uuid,text,text,text) FROM %s CASCADE',r);
        EXECUTE format('REVOKE ALL ON FUNCTION config.manage_web_access_group_recoverable(text,uuid,bigint,text,text,integer[],uuid,text,boolean,text) FROM %s CASCADE',r);
        EXECUTE format('REVOKE ALL ON FUNCTION config.harden_web_access_exports() FROM %s CASCADE',r);
    END LOOP;
    IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='admin') THEN
        GRANT EXECUTE ON FUNCTION config.web_access_export(text,uuid,uuid,text,text,text) TO admin;
        GRANT EXECUTE ON FUNCTION config.manage_web_access_group_recoverable(text,uuid,bigint,text,text,integer[],uuid,text,boolean,text) TO admin;
    END IF;
END $fn$;
SELECT config.harden_web_access_exports();
""";
}
