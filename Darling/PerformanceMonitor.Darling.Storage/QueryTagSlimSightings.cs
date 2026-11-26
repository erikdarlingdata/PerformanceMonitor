/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// V320 (#9705): <c>collect.query_tag_sightings_slim</c>, the short form of a query-tag sighting that outlives the sighting itself.
/// <c>collect.query_tag_sightings</c> is kept as long as the raw rows of the collectors that write it (30 days at the shipped
/// defaults), and the tag reads also join tiers that keep data longer (the hourly IO tiers, 90 days). A key whose sighting aged
/// out lost its map row at the next rebuild, so its older usage read as untagged. The retention purge moves each sighting it
/// deletes here, in the statement that deletes it, and the map builder reads both tables, so a rule on the database, object,
/// application, login or host tags a key for as long as any usage row of it is kept.
///
/// <para>The row has no <c>text_sample</c>, which is up to <see cref="QueryTagSchema.TextSampleMaxLength"/> characters and most of a
/// sighting's width. A <c>text</c> or <c>text_tag</c> rule therefore reads only the sightings table, and tags a key seen inside
/// the sightings window. The statement text tables are no source for an older key: they are pruned on the raw rows' horizon
/// too.</para>
///
/// <para>The primary key is the sightings table's identity (<c>ux_query_tag_sightings_key_db</c>): server, kind, key, the three
/// caller fields and the database, so one hash seen in two databases keeps a row for each and a database rule is tested
/// against both. Those four fields are NOT NULL with an empty string for "none", so they can be the primary key. The sightings
/// table keeps NULL and reads it as an empty string in its unique index, which is the same key. The readers turn the empty
/// string back into NULL.</para>
/// </summary>
public static class QueryTagSlimSightings
{
    /// <summary>The store schema version that has the table.</summary>
    public const int Version = 320;

    public const string Table = "collect.query_tag_sightings_slim";

    /// <summary>The DDL of the rung. One plain table created empty: no data moves. Every statement is idempotent.</summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS collect.query_tag_sightings_slim (
    server_id integer NOT NULL,
    key_kind text NOT NULL,
    key text NOT NULL,
    application_name text NOT NULL DEFAULT '',
    login_name text NOT NULL DEFAULT '',
    host_name text NOT NULL DEFAULT '',
    database_name text NOT NULL DEFAULT '',
    object_name text,
    last_seen timestamptz NOT NULL,
    CONSTRAINT pk_query_tag_sightings_slim PRIMARY KEY (server_id, key_kind, key, application_name, login_name, host_name, database_name),
    CONSTRAINT ck_query_tag_sightings_slim_kind CHECK (key_kind IN ('hash', 'object', 'caller'))
);

CREATE INDEX IF NOT EXISTS idx_query_tag_sightings_slim_last_seen
    ON collect.query_tag_sightings_slim (last_seen);

COMMENT ON TABLE collect.query_tag_sightings_slim IS 'A query-tag sighting past the sightings window, without its text sample. Written by the retention purge as it deletes the sighting, read by the tag map rebuild.';
COMMENT ON COLUMN collect.query_tag_sightings_slim.application_name IS 'A caller field. An empty string means none, so the column can be part of the primary key.';
COMMENT ON COLUMN collect.query_tag_sightings_slim.database_name IS 'An empty string means none, so the column can be part of the primary key.';
""";

    /// <summary>
    /// Wraps a sightings delete so the rows it deletes land in the slim table in the same statement: a sighting is never gone
    /// without its slim row, and a failed statement leaves both tables as they were. <paramref name="deleteSightingsSql"/> is a
    /// <c>DELETE FROM collect.query_tag_sightings ...</c> with no RETURNING. The statement's row count is the rows deleted: the
    /// sightings unique index and the slim primary key are the same key, so each deleted row inserts or updates exactly one,
    /// and no two rows of one slice meet the same slim row (PostgreSQL refuses a second update of one row in a statement, which
    /// would fail the slice on every pass). A key that was moved, sighted again and aged out again meets its own slim row and
    /// keeps the newer <c>last_seen</c>.
    /// </summary>
    public static string MoveExpiredSql(string deleteSightingsSql) => $"""
WITH moved AS (
    {deleteSightingsSql}
    RETURNING server_id, key_kind, key, application_name, login_name, host_name, database_name, object_name, last_seen
)
INSERT INTO collect.query_tag_sightings_slim AS t
    (server_id, key_kind, key, application_name, login_name, host_name, database_name, object_name, last_seen)
SELECT server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''),
       COALESCE(database_name, ''), object_name, last_seen
FROM moved
ON CONFLICT ON CONSTRAINT pk_query_tag_sightings_slim DO UPDATE
SET last_seen = GREATEST(t.last_seen, EXCLUDED.last_seen),
    object_name = COALESCE(EXCLUDED.object_name, t.object_name)
""";
}
