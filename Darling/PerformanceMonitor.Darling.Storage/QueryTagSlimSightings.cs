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
/// defaults), and the tag reads also join tiers that keep data longer (the hourly IO tiers, 90 days). A key whose sightings all
/// aged out lost its map row at the next rebuild, so its older usage read as untagged. The retention purge appends each sighting
/// it deletes here, in the statement that deletes it, and the map rebuild resolves a key from these rows when the key has no
/// sighting left, so a rule on the database, object, application, login or host tags a key for as long as any usage row of it
/// is kept. A key that has a sighting is resolved from its sightings alone.
///
/// <para>The row has no <c>text_sample</c>, which is up to <see cref="QueryTagSchema.TextSampleMaxLength"/> characters and most of a
/// sighting's width. A <c>text</c> or <c>text_tag</c> rule therefore reads only the sightings table, and tags a key seen inside
/// the sightings window. The statement text tables are no source for an older key: they are pruned on the raw rows' horizon
/// too.</para>
///
/// <para>The table is append only and has no key: the move is a plain INSERT, so a slice writes its rows at the end of the heap
/// and at the right edge of the one index, in <c>last_seen</c> order. A unique key would make every slice read and write the
/// leaf pages of a key-ordered index all over the table. A caller and database that aged out, came back and aged out again
/// therefore has more than one row. The rebuild resolves a key from all of its rows, so an equal row changes nothing, and each
/// row leaves on its own <c>last_seen</c>.</para>
/// </summary>
public static class QueryTagSlimSightings
{
    /// <summary>The store schema version that has the table.</summary>
    public const int Version = 320;

    public const string Table = "collect.query_tag_sightings_slim";

    /// <summary>
    /// The DDL of the rung. One plain table created empty: no data moves. Every statement is idempotent. The one index is on
    /// <c>last_seen</c>, for the purge of this table. The rebuild reads the heap in block ranges and needs none.
    /// </summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS collect.query_tag_sightings_slim (
    server_id integer NOT NULL,
    key_kind text NOT NULL,
    key text NOT NULL,
    application_name text,
    login_name text,
    host_name text,
    database_name text,
    object_name text,
    last_seen timestamptz NOT NULL,
    CONSTRAINT ck_query_tag_sightings_slim_kind CHECK (key_kind IN ('hash', 'object', 'caller'))
);

CREATE INDEX IF NOT EXISTS idx_query_tag_sightings_slim_last_seen
    ON collect.query_tag_sightings_slim (last_seen);

COMMENT ON TABLE collect.query_tag_sightings_slim IS 'A query-tag sighting past the sightings window, without its text sample. Appended by the retention purge as it deletes the sighting, read by the tag map rebuild for a key with no sighting left. No key: a caller that aged out twice has two rows.';
""";

    /// <summary>
    /// The hours of <c>last_seen</c> one slice of the move covers: a day, the slice every other time-sliced purge takes.
    /// </summary>
    public const int MoveSliceHours = 24;

    /// <summary>
    /// Finds the first slice of a drain: the oldest expired <c>last_seen</c> and that time plus <see cref="MoveSliceHours"/>, both
    /// computed by the store. <c>$1</c> is the cutoff. The start is NULL when no expired sighting is left.
    /// </summary>
    public static readonly string FirstSliceProbeSql = SliceProbeSql(string.Empty);

    /// <summary>
    /// The same probe for every later slice of a drain, bounded from below by <c>$2</c>, the end of the slice before it. Nothing
    /// vacuums the table between two slices, so a probe from the bottom of the index would visit the dead entry of every row
    /// the slices before deleted.
    /// </summary>
    public static readonly string NextSliceProbeSql = SliceProbeSql(" AND last_seen >= $2");

    private static string SliceProbeSql(string lowerBound) =>
        $"SELECT s.slice_start, s.slice_start + INTERVAL '{MoveSliceHours} hours' FROM (SELECT min(last_seen) AS slice_start FROM collect.query_tag_sightings WHERE last_seen < $1{lowerBound}) AS s";

    /// <summary>
    /// Deletes one slice of expired sightings and appends their slim rows in the same statement: a sighting is never gone
    /// without its slim row, and a failed statement leaves both tables as they were. <c>$1</c> is the cutoff, <c>$2</c> and
    /// <c>$3</c> the slice's start and end as a probe returned them. They are plain parameters, not subqueries, so the planner
    /// sees the real range: a slice's rows lie all over the heap, and with the range known it reads them through a bitmap,
    /// in heap order with read-ahead, where a range it cannot see is planned as an index scan with one random read a row.
    /// The statement's row count is the rows deleted, which is the rows inserted.
    /// </summary>
    public const string MoveSliceSql = """
WITH moved AS (
    DELETE FROM collect.query_tag_sightings
    WHERE last_seen < $1 AND last_seen >= $2 AND last_seen < $3
    RETURNING server_id, key_kind, key, application_name, login_name, host_name, database_name, object_name, last_seen
)
INSERT INTO collect.query_tag_sightings_slim
    (server_id, key_kind, key, application_name, login_name, host_name, database_name, object_name, last_seen)
SELECT server_id, key_kind, key, application_name, login_name, host_name, database_name, object_name, last_seen
FROM moved
""";
}
