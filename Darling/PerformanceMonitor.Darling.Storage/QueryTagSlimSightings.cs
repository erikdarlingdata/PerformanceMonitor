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
    /// slice moves in 5.8 s through the bitmap read the scan setting of <see cref="MoveSliceSql"/> gives (12 hours 5.3 s, 6 hours 4.3 s)
    /// and in 54.6 s through the index scan the planner picks without it (12 hours 51.5 s, 6 hours 43.8 s). So a shorter
    /// slice saves under 30 percent of a slice's time, doubles or quadruples the probes and the pacing waits of a drain, and
    /// a day is under the purge's 100 s target on either plan. A drain with a backlog also caps each slice at
    /// <see cref="MoveSliceRowCap"/> rows.
    /// </summary>
    public const int MoveSliceHours = 24;

    /// <summary>
    /// The most rows one slice moves while a drain has a backlog (#9956). A move holds the row lock of every sighting it deletes
    /// until it commits, and a sighting upsert that touches one of those keys waits for it, under
    /// <see cref="QueryTagSightingWriter.CommandTimeoutSeconds"/> (30 s). Over a field-size backlog (a lab store with 1 GB of
    /// shared_buffers on a shared disk) the move ran 3,900 to 7,000 rows a second: a day of 1,134,777 rows held its locks 160.9 s
    /// and one of 499,994 rows 113.6 s. At the slowest of those rates this cap is 6.4 s, and an estimated 11 to 15 s on a disk of
    /// 3,000 IOPS (1.7 to 2.4 times the lab's time). The WAL a row writes (about 5.3 KB with full-page images) is the same in any slice, so a smaller slice
    /// does not slow the drain, whose pace is the WAL pacer's.
    /// </summary>
    public const int MoveSliceRowCap = 25_000;

    /// <summary>
    /// How far below the cutoff the oldest expired sighting must be for a drain to count as a backlog (#9956): two slices of
    /// <see cref="MoveSliceHours"/>. A store whose purge has kept up has about a day to move, and a pass that runs hours late or
    /// a missed day stays in the one-day slices. A drain whose first slice starts further back caps every slice it takes at
    /// <see cref="MoveSliceRowCap"/> rows, its last days included.
    /// </summary>
    public const int BacklogHours = 2 * MoveSliceHours;

    /// <summary>
    /// Finds the first slice of a drain: the oldest expired <c>last_seen</c>, the slice's end and whether the drain has a backlog,
    /// all computed by the store. <c>$1</c> is the cutoff. The start is NULL when no expired sighting is left. The probe has no
    /// lower bound, so it reads the <c>last_seen</c> index from its first entry. The purge runs it only when no move left a
    /// bound it can trust (DarlingRetention.ReadSightingsDrainStartAsync).
    /// </summary>
    public static readonly string FirstSliceProbeSql = SliceProbeSql(string.Empty, string.Empty);

    /// <summary>
    /// The same probe from a lower bound: <c>$2</c> is the point the slice before it moved through, or the point the last slice
    /// of an earlier pass moved through, and <c>$3</c> is true when the drain already counts as a backlog. Nothing vacuums the table between two
    /// slices or between two passes a few hours apart, and a probe from the bottom of the index would visit the dead entry of
    /// every row the slices before deleted: after a pass that moved 1.23 million rows, the next pass's probe from the bottom
    /// ran past the purge's 300 s command timeout.
    /// </summary>
    public static readonly string NextSliceProbeSql = SliceProbeSql(" AND last_seen >= $2", "$3 OR ");

    /* A slice of a backlog ends at the last_seen of the first row past the cap, so every expired row below its end is in it and
       the end stays a bound for the next probe. A run of rows that share the start and are more than the cap moves whole: the
       end is never the start itself, or the slice would move nothing and end the drain. A slice that is not a backlog's, or a
       backlog day under the cap, is the whole day. */
    private static string SliceProbeSql(string lowerBound, string backlogSoFar) => $"""
SELECT
    s.slice_start,
    CASE WHEN s.backlog
         THEN LEAST(
             s.slice_start + INTERVAL '{MoveSliceHours} hours',
             GREATEST(
                 s.slice_start + INTERVAL '1 microsecond',
                 COALESCE(
                     (SELECT c.last_seen FROM collect.query_tag_sightings AS c
                      WHERE c.last_seen >= s.slice_start AND c.last_seen < $1
                      ORDER BY c.last_seen OFFSET {MoveSliceRowCap} LIMIT 1),
                     s.slice_start + INTERVAL '{MoveSliceHours} hours')))
         ELSE s.slice_start + INTERVAL '{MoveSliceHours} hours' END,
    s.backlog
FROM (
    SELECT m.slice_start, ({backlogSoFar}m.slice_start < $1 - INTERVAL '{BacklogHours} hours') AS backlog
    FROM (SELECT min(last_seen) AS slice_start FROM collect.query_tag_sightings WHERE last_seen < $1{lowerBound}) AS m
) AS s
""";

    /// <summary>The <c>collect.collector_state</c> owner of the move's stored bound (#9956), under the fleet's server id.</summary>
    public const string MoveStateOwner = "retention";

    /// <summary>The state key of the point the last slice moved through: its end, or the cutoff when the end lies past it.</summary>
    public const string MovedThroughKey = "query_tag_sightings_moved_through";

    /// <summary>The state key of the sightings table's vacuum count, read after that move committed.</summary>
    public const string VacuumsAtMoveKey = "query_tag_sightings_vacuums_at_move";

    /// <summary>
    /// Stores the point a slice that moved rows moved through (#9956), <c>$2</c>, and the sightings table's count of finished
    /// vacuums, under server <c>$1</c>. That point is the slice's end clamped to the pass's cutoff: a slice that ends past the
    /// cutoff leaves the rows between the two, which have not expired, and a bound at its end would skip them once they do. The
    /// purge runs it after the move commits, so a stored point is never ahead of the rows moved, and a write that fails leaves an
    /// older point, which only costs the next probe the dead entries in between.
    /// </summary>
    public const string RecordMovedThroughSql = $"""
INSERT INTO collect.collector_state AS s (server_id, collector_name, state_key, state_value, updated_at)
SELECT $1, '{MoveStateOwner}', k.state_key, k.state_value, now()
FROM (VALUES
    ('{MovedThroughKey}', $2::timestamptz::text),
    ('{VacuumsAtMoveKey}', (SELECT (t.vacuum_count + t.autovacuum_count)::text FROM pg_stat_user_tables AS t WHERE t.relid = 'collect.query_tag_sightings'::regclass))
) AS k(state_key, state_value)
WHERE k.state_value IS NOT NULL
ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at
""";

    /// <summary>
    /// The lower bound of a pass's first probe (#9956): the stored point the last slice moved through, while fewer than two
    /// vacuums of the sightings table have finished since that move. No row when there is no stored point, or two vacuums have
    /// finished (the first may have started before the move committed and kept its dead tuples, the second started after it), or
    /// the count is below the stored one (the statistics were reset). Then the probe starts from the bottom of the index: the dead entries
    /// of the moves are gone, and it finds any sighting with a <c>last_seen</c> below the old bound. The sighting writer never
    /// writes one (it stamps the store's <c>now()</c>, or that less <see cref="QueryTagSightingWriter.ReSightingBackdate"/>), so
    /// such a row comes only from outside the service, such as a restore. <c>$1</c> is the server id the bound is stored under.
    /// </summary>
    public const string DrainStartBoundSql = $"""
SELECT b.state_value::timestamptz
FROM collect.collector_state AS b
JOIN collect.collector_state AS v
  ON v.server_id = b.server_id AND v.collector_name = b.collector_name AND v.state_key = '{VacuumsAtMoveKey}'
CROSS JOIN (SELECT t.vacuum_count + t.autovacuum_count AS n FROM pg_stat_user_tables AS t WHERE t.relid = 'collect.query_tag_sightings'::regclass) AS c
WHERE b.server_id = $1 AND b.collector_name = '{MoveStateOwner}' AND b.state_key = '{MovedThroughKey}'
  AND c.n >= v.state_value::bigint AND c.n < v.state_value::bigint + 2
""";

    /// <summary>
    /// Deletes one slice of expired sightings and appends their slim rows in the same statement: a sighting is never gone
    /// without its slim row, and a failed statement leaves both tables as they were. <c>$1</c> is the cutoff, <c>$2</c> and
    /// <c>$3</c> the slice's start and end as a probe returned them, as plain parameters. The range alone does not choose the
    /// plan (#9705): the planner takes the index scan for it, so the purge runs the statement in a transaction of its own
    /// after <c>SET LOCAL enable_indexscan = off</c> (the scan setting of #9796, DarlingRetention.WithSliceDeleteScanSettingAsync),
    /// which ends with that transaction, committed or rolled back, so no probe and no other statement of the purge is planned
    /// under it. With index scans off the DELETE finds its slice through a bitmap of the <c>last_seen</c> index and reads the
    /// heap in block order with read-ahead. The planner prices that bitmap plan 3.7 times its own pick on either kind of day,
    /// so the setting forces the bitmap and no cost is left to choose it. Lab medians of three cold runs: a day of 402,668
    /// rows that lie all over the heap moves in 5.74 s against 55.11 s by the index scan (9.6 times faster), and a day of
    /// 402,467 rows in heap order in 4.91 s against 3.81 s (1.29 times slower). The setting is on for every slice: it gains
    /// 49.4 s on a day of the first kind and loses 1.10 s on a day of the second, and 27 of that store's 28 days are of the
    /// first kind. The statement's row count is the rows deleted, which is the rows inserted.
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
