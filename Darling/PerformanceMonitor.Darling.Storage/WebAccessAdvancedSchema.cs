/* Copyright (c) 2026 Erik Darling, Darling Data LLC
 * Licensed under the MIT License. See LICENSE in the project root. */
namespace PerformanceMonitor.Darling.Storage;

public static class WebAccessAdvancedSchema
{
    public const int Version = 260;
    public const string CreateSql = """
CREATE TABLE config.web_access_advanced (
    id integer PRIMARY KEY CHECK (id=1),
    enabled boolean NOT NULL DEFAULT false,
    session_epoch uuid NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO config.web_access_advanced(id) VALUES(1);
CREATE TRIGGER web_access_owner_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON config.web_access_advanced
FOR EACH STATEMENT EXECUTE FUNCTION config.web_access_owner_guard();
ALTER TABLE config.web_access_advanced ENABLE ALWAYS TRIGGER web_access_owner_guard;
CREATE FUNCTION config.set_web_access_advanced(p_enabled boolean) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fn$
    UPDATE config.web_access_advanced SET enabled=p_enabled,session_epoch=gen_random_uuid()
    WHERE id=1 AND enabled IS DISTINCT FROM p_enabled;
$fn$;
CREATE FUNCTION config.harden_web_access_advanced() RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $fn$
DECLARE r text; c text;
BEGIN
    FOR r IN SELECT 'PUBLIC' UNION SELECT quote_ident(rolname) FROM pg_roles
        WHERE oid<>(SELECT relowner FROM pg_class WHERE oid='config.web_access_advanced'::regclass) LOOP
        EXECUTE format('REVOKE ALL ON config.web_access_advanced FROM %s CASCADE',r);
        FOREACH c IN ARRAY ARRAY['id','enabled','session_epoch'] LOOP
            EXECUTE format('REVOKE ALL (%I) ON config.web_access_advanced FROM %s CASCADE',c,r);
        END LOOP;
        EXECUTE format('REVOKE ALL ON FUNCTION config.set_web_access_advanced(boolean),config.harden_web_access_advanced() FROM %s CASCADE',r);
    END LOOP;
    FOR r IN SELECT quote_ident(rolname) FROM pg_roles WHERE rolname IN ('admin','web') LOOP
        EXECUTE format('GRANT SELECT ON config.web_access_advanced TO %s',r);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='admin') THEN
        GRANT EXECUTE ON FUNCTION config.set_web_access_advanced(boolean) TO admin;
    END IF;
END $fn$;
SELECT config.harden_web_access_advanced();
""";
}
