/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store tables for query tags (#5977, wave 1 of #5971): the tag set, hand assignments by <c>query_hash</c> and by object,
/// rules, the per-pass sightings of query keys and caller fields, the materialised tag map the reads join, and the one-row
/// version state. This is the only migration of #5971; every later slice uses these tables. A query tag is never a server tag:
/// <c>config.query_tags</c> has the shape of <c>config.server_tags</c> and is its own table.
/// </summary>
public static class QueryTagSchema
{
    /// <summary>
    /// The most characters <c>collect.query_tag_sightings.text_sample</c> holds. The collector hook (#5980) trims to it and the
    /// stores share it; the column's CHECK holds the same number, so a longer sample is refused rather than stored.
    /// </summary>
    public const int TextSampleMaxLength = 4000;

    /// <summary>
    /// The statements that clear one server's rows from the per-server query-tag tables, each taking <c>$1</c> = server_id.
    /// Run at both server-removal paths in the removal's own transaction. The reason is <see cref="ServerTagStore.ClearForServerSql"/>'s:
    /// <c>server_id</c> is a deterministic hash of host, database and read-only intent, so a removed-then-re-added server would
    /// otherwise get its old assignments, rules, sightings and map rows back. Only that server's rows go: an assignment or rule
    /// whose <c>server_id</c> is NULL applies to every server and stays. The tags themselves are fleet-wide and stay.
    /// </summary>
    public static readonly string[] ClearForServerSqls =
    [
        "DELETE FROM config.query_tag_assignments WHERE server_id = $1",
        "DELETE FROM config.query_tag_rules WHERE server_id = $1",
        "DELETE FROM config.query_tag_map WHERE server_id = $1",
        "DELETE FROM collect.query_tag_sightings WHERE server_id = $1",
    ];

    /// <summary>The DDL of the rung. Every statement is idempotent, so a second run changes nothing.</summary>
    public static readonly string CreateSql = string.Create(CultureInfo.InvariantCulture, $"""
CREATE TABLE IF NOT EXISTS config.query_tags (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    parent_id integer REFERENCES config.query_tags(id) ON DELETE CASCADE,
    sort_order integer NOT NULL DEFAULT 0,
    colour text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_query_tags_name CHECK (btrim(name) <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_tags_parent_name
    ON config.query_tags (COALESCE(parent_id, 0), lower(name));

CREATE TABLE IF NOT EXISTS config.query_tag_assignments (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag_id integer NOT NULL REFERENCES config.query_tags(id) ON DELETE CASCADE,
    kind text NOT NULL,
    server_id integer,
    query_hash text,
    database_name text,
    schema_name text,
    object_name text,
    query_id bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    CONSTRAINT ck_query_tag_assignments_kind CHECK (kind IN ('hash', 'object')),
    CONSTRAINT ck_query_tag_assignments_key CHECK (
        (kind = 'hash'
            AND query_hash IS NOT NULL AND btrim(query_hash) <> ''
            AND database_name IS NULL AND schema_name IS NULL AND object_name IS NULL)
        OR
        (kind = 'object'
            AND query_hash IS NULL
            AND database_name IS NOT NULL AND btrim(database_name) <> ''
            AND schema_name IS NOT NULL AND btrim(schema_name) <> ''
            AND object_name IS NOT NULL AND btrim(object_name) <> ''))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_tag_assignments_all_servers
    ON config.query_tag_assignments (kind, COALESCE(query_hash, ''), COALESCE(database_name, ''), COALESCE(schema_name, ''), COALESCE(object_name, ''))
    WHERE server_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_tag_assignments_server
    ON config.query_tag_assignments (server_id, kind, COALESCE(query_hash, ''), COALESCE(database_name, ''), COALESCE(schema_name, ''), COALESCE(object_name, ''))
    WHERE server_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_query_tag_assignments_tag
    ON config.query_tag_assignments (tag_id);
CREATE INDEX IF NOT EXISTS idx_query_tag_assignments_server
    ON config.query_tag_assignments (server_id) WHERE server_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_query_tag_assignments_hash
    ON config.query_tag_assignments (query_hash) WHERE query_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_query_tag_assignments_object
    ON config.query_tag_assignments (object_name) WHERE object_name IS NOT NULL;

CREATE TABLE IF NOT EXISTS config.query_tag_rules (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag_id integer REFERENCES config.query_tags(id) ON DELETE CASCADE,
    priority integer NOT NULL DEFAULT 100,
    enabled boolean NOT NULL DEFAULT true,
    server_id integer,
    match_field text NOT NULL,
    pattern text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    CONSTRAINT ck_query_tag_rules_field CHECK (match_field IN ('application', 'login', 'host', 'database', 'object', 'text', 'text_tag')),
    CONSTRAINT ck_query_tag_rules_tag CHECK (match_field = 'text_tag' OR tag_id IS NOT NULL),
    CONSTRAINT ck_query_tag_rules_pattern CHECK (btrim(pattern) <> '')
);

CREATE INDEX IF NOT EXISTS idx_query_tag_rules_tag
    ON config.query_tag_rules (tag_id) WHERE tag_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_query_tag_rules_server
    ON config.query_tag_rules (server_id) WHERE server_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_query_tag_rules_priority
    ON config.query_tag_rules (enabled, priority, id);

CREATE TABLE IF NOT EXISTS collect.query_tag_sightings (
    server_id integer NOT NULL,
    key_kind text NOT NULL,
    key text NOT NULL,
    database_name text,
    object_name text,
    application_name text,
    login_name text,
    host_name text,
    text_sample text,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    seen_count bigint NOT NULL DEFAULT 1,
    CONSTRAINT ck_query_tag_sightings_kind CHECK (key_kind IN ('hash', 'object', 'caller')),
    CONSTRAINT ck_query_tag_sightings_sample CHECK (text_sample IS NULL OR char_length(text_sample) <= {TextSampleMaxLength}),
    CONSTRAINT ck_query_tag_sightings_count CHECK (seen_count >= 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_tag_sightings_key
    ON collect.query_tag_sightings (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''));
CREATE INDEX IF NOT EXISTS idx_query_tag_sightings_key
    ON collect.query_tag_sightings (key_kind, key);
CREATE INDEX IF NOT EXISTS idx_query_tag_sightings_object
    ON collect.query_tag_sightings (object_name) WHERE object_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_query_tag_sightings_last_seen
    ON collect.query_tag_sightings (last_seen);

CREATE TABLE IF NOT EXISTS config.query_tag_map (
    server_id integer NOT NULL,
    key_kind text NOT NULL,
    key text NOT NULL,
    tag_id integer NOT NULL REFERENCES config.query_tags(id) ON DELETE CASCADE,
    source text NOT NULL,
    rule_id integer,
    matched_field text,
    matched_value text,
    built_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (server_id, key_kind, key),
    CONSTRAINT ck_query_tag_map_kind CHECK (key_kind IN ('hash', 'object', 'caller')),
    CONSTRAINT ck_query_tag_map_source CHECK (source IN ('hand_hash', 'hand_object', 'rule'))
);

CREATE INDEX IF NOT EXISTS idx_query_tag_map_tag
    ON config.query_tag_map (tag_id);
CREATE INDEX IF NOT EXISTS idx_query_tag_map_key
    ON config.query_tag_map (key_kind, key);

CREATE TABLE IF NOT EXISTS config.query_tag_state (
    id smallint PRIMARY KEY DEFAULT 1,
    rules_version bigint NOT NULL DEFAULT 1,
    built_version bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_query_tag_state_one_row CHECK (id = 1)
);

INSERT INTO config.query_tag_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE FUNCTION config.query_tag_bump_rules_version() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, config
AS $fn$
BEGIN
    UPDATE config.query_tag_state SET rules_version = rules_version + 1, updated_at = now() WHERE id = 1;
    RETURN NULL;
END
$fn$;

REVOKE ALL ON FUNCTION config.query_tag_bump_rules_version() FROM PUBLIC;

DROP TRIGGER IF EXISTS trg_bump_query_tags ON config.query_tags;
CREATE TRIGGER trg_bump_query_tags
    AFTER INSERT OR UPDATE OR DELETE ON config.query_tags
    FOR EACH STATEMENT EXECUTE FUNCTION config.query_tag_bump_rules_version();

DROP TRIGGER IF EXISTS trg_bump_query_tag_assignments ON config.query_tag_assignments;
CREATE TRIGGER trg_bump_query_tag_assignments
    AFTER INSERT OR UPDATE OR DELETE ON config.query_tag_assignments
    FOR EACH STATEMENT EXECUTE FUNCTION config.query_tag_bump_rules_version();

DROP TRIGGER IF EXISTS trg_bump_query_tag_rules ON config.query_tag_rules;
CREATE TRIGGER trg_bump_query_tag_rules
    AFTER INSERT OR UPDATE OR DELETE ON config.query_tag_rules
    FOR EACH STATEMENT EXECUTE FUNCTION config.query_tag_bump_rules_version();

COMMENT ON COLUMN config.query_tag_assignments.server_id IS 'NULL means the assignment applies to every server. A server id applies to that server only.';
COMMENT ON COLUMN config.query_tag_assignments.query_id IS 'A display key only (a Query Store query_id as last seen). Never matched on: the hash or the object parts are the key.';
COMMENT ON COLUMN config.query_tag_rules.server_id IS 'NULL means the rule applies to every server. A server id applies to that server only.';
COMMENT ON COLUMN config.query_tag_rules.tag_id IS 'NULL only for the text_tag field, a rule that reads a tag name out of the statement text instead of naming one.';
COMMENT ON COLUMN collect.query_tag_sightings.text_sample IS 'At most {TextSampleMaxLength} characters (QueryTagSchema.TextSampleMaxLength); the CHECK refuses a longer one.';
COMMENT ON COLUMN collect.query_tag_sightings.application_name IS 'A caller field. NULL reads as an empty string in the unique key, so a sighting without it is one row, not many.';
COMMENT ON COLUMN config.query_tag_map.rule_id IS 'The rule that produced the row. No foreign key: a deleted rule leaves its map rows until the next rebuild, which the version bump triggers.';
COMMENT ON COLUMN config.query_tag_state.built_version IS 'The rules_version the map was last built from. The map is stale while it is below rules_version.';
""");
}
