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
/// is kept. A rebuild resolves a key that has a sighting from its sightings alone (<see cref="QueryTagMapBuilder"/> has what an
/// extend does in between).
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
    /// The hours of <c>last_seen</c> one slice of the move covers (#9705): a day, the slice every other time-sliced purge takes.
    /// Measured on a lab store, one cold run a length, on a day of 402,668 expired rows that lie all over the heap: a 24 hour
    /// slice moves in 5.8 s through the bitmap read <see cref="MovePlanSettingSql"/> gives (12 hours 5.3 s, 6 hours 4.3 s)
    /// and in 54.6 s through the index scan the planner picks without it (12 hours 51.5 s, 6 hours 43.8 s). So a shorter
    /// slice saves under 30 percent of a slice's time, doubles or quadruples the probes and the pacing waits of a drain, and
    /// a day is under the purge's 100 s target on either plan.
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
    /// The plan setting of the move (#9705), run as its own command inside the transaction that holds only it and
    /// <see cref="MoveSliceSql"/>: <c>SET LOCAL</c> ends with that transaction, committed or rolled back, so no probe and no
    /// other statement of the purge is planned under it. With index scans off the delete finds its slice through a bitmap of
    /// the <c>last_seen</c> index and reads the heap in block order with read-ahead. Left alone, the planner picks the index
    /// scan even with the slice's real bounds as parameters, because <c>last_seen</c> follows heap position closely over the
    /// whole table (a correlation of 0.98 on a lab store) while the rows of one expired day lie all over the heap, so it
    /// prices one random read a row as if the rows were neighbours. It prices the bitmap plan 3.7 times its own pick on
    /// either kind of day, so the setting forces the bitmap and no cost is left to choose it. Measured on that store, the
    /// median of three cold runs of a day's slice: a day of 402,668 rows that lie all over the heap moves in 5.74 s by the
    /// bitmap against 55.11 s by the index scan, 9.6 times faster. A day of 402,467 rows that lie in heap order moves in
    /// 4.91 s by the bitmap against 3.81 s by the index scan, 1.29 times slower. The setting is on for every slice: it gains
    /// 49.4 s on a day of the first kind and loses 1.10 s on a day of the second, and 27 of that store's 28 days are of the
    /// first kind.
    /// </summary>
    public const string MovePlanSettingSql = "SET LOCAL enable_indexscan = off";

    /// <summary>
    /// Deletes one slice of expired sightings and appends their slim rows in the same statement: a sighting is never gone
    /// without its slim row, and a failed statement leaves both tables as they were. <c>$1</c> is the cutoff, <c>$2</c> and
    /// <c>$3</c> the slice's start and end as a probe returned them, as plain parameters. The range alone does not choose the
    /// plan (#9705): the planner takes the index scan for it, so the statement runs after <see cref="MovePlanSettingSql"/>
    /// in a transaction of its own and reads the slice through a bitmap. Lab medians of three cold runs: a day of 402,668
    /// rows that lie all over the heap moves in 5.74 s against 55.11 s by the index scan (9.6 times faster), and a day of
    /// 402,467 rows in heap order in 4.91 s against 3.81 s (1.29 times slower). The statement's row count is the rows
    /// deleted, which is the rows inserted.
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
