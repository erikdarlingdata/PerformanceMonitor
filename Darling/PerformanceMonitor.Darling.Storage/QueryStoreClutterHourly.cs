/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly Query Store clutter rollups (#9173): <see cref="QueryStoreClosedDayRollup"/>'s two clutter rollups of raw
/// <c>collect.query_store_stats</c> at hourly grain, so a 24-hour read, which almost never holds a whole UTC day, is served from whole
/// hours and reads raw only for its edges. <c>query_store_clutter_plan_hourly</c> holds one row per (server, hour, database, query, plan)
/// with the plan's first and last <c>collection_time</c> in the hour; <c>query_store_clutter_collections_hourly</c> one row per (server,
/// hour, database) with the number of distinct <c>collection_time</c> values. A collection belongs to exactly one hour, so the hourly
/// counts add up to the window's count, exactly as the daily ones do. <c>query_store_clutter_hour_built</c> says which (server, hour)
/// both tables hold.
///
/// <para><b>The job.</b> The closed-day tick (<see cref="QueryStoreClosedDayRollup.RunTickAsync(NpgsqlDataSource, DateTime, int, int?, ILogger, CancellationToken)"/>)
/// runs it. An hour is closed once it has ended and <see cref="Grace"/> has passed: a batch collected inside the hour has committed by
/// then. A closed hour is built in one transaction that takes the hour's built row first, clears the hour and fills it again, the way
/// a day is built. A tick builds the newest hours first.</para>
///
/// <para><b>Late rows.</b> A raw batch whose <c>collection_time</c> falls in an hour that is already closed marks the hour for a rebuild
/// in the batch's own transaction (<see cref="TryMarkLateHourAsync"/>); the reader leaves a marked hour to raw until the next tick builds it
/// again. The mark and the build take the same built row, so a row written while its hour is being built is either read by that build or marks
/// the hour after it.</para>
///
/// <para><b>Heartbeat.</b> The hour has its own heartbeat, <c>hour_ticked_at</c> on <c>query_store_closed_day_tick</c>. A build that does not know
/// these tables (V285 to V287) keeps the day heartbeat fresh but marks no hour, so the hours are served only while the hour heartbeat is
/// fresh. The first tick after a gap marks the built hours that hold a row collected since the last hour heartbeat.</para>
///
/// <para><b>Retention.</b> Raw's, as <see cref="RawHeldDaysAsync"/> says: where raw <c>query_store_stats</c> is a TimescaleDB hypertable only the gated raw
/// purge drops it, at <see cref="TimescaleSupport.RawRetentionSpan"/>, and a custom retention never applies to it, so that span is the horizon (#9690).
/// On a plain raw table it is the collector's retention. A tick removes the hours before the first whole hour inside the horizon from all three
/// tables, oldest first, <see cref="PruneSliceHours"/> hours per transaction under the statement timeout and the retention purge's WAL pacer, and
/// stops at <see cref="MaxPruneDuration"/>: a backlog is removed over as many ticks as it takes. The hours it plans are bounded by the same horizon.</para>
///
/// <para><b>Raw floor (#9383).</b> No hour before the first whole hour at or after a server's oldest raw row is built, the way a day is not (#9287): raw
/// holds nothing there, so a build would stamp an empty hour that the route then serves as "no plans". A tick also removes the stamp of such an
/// hour when it holds no clutter row, so the span reads as not collected.</para>
/// </summary>
public static class QueryStoreClutterHourly
{
    /// <summary>The migration that creates the three tables and the hour heartbeat column.</summary>
    public const int Version = 289;

    /// <summary>An hour is closed once it has ended and this has passed: a batch collected inside the hour has committed by then. It is the
    /// margin the day heartbeat uses for the same reason (<see cref="QueryStoreClosedDayRollup.HeartbeatMargin"/>).</summary>
    public static readonly TimeSpan Grace = QueryStoreClosedDayRollup.HeartbeatMargin;

    /// <summary>The most (server, hour) builds one tick plans: high enough that <see cref="MaxTickDuration"/> is the bound that holds.</summary>
    public const int MaxBuildsPerTick = 1000;

    /// <summary>The hourly builds stop starting once this much of the tick has passed, so a long backlog of hours leaves the daily builds
    /// their time (the tick's whole budget is <see cref="QueryStoreClosedDayRollup.MaxTickDuration"/>).</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromSeconds(150);

    private const int CommandTimeoutSeconds = QueryStoreClosedDayRollup.BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{QueryStoreClosedDayRollup.BuildStatementTimeoutSeconds}s'";

    /// <summary>
    /// #9173: the hourly clutter rollups and the hour heartbeat column. Created empty, and idempotent.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_clutter_plan_hourly
(
    server_id integer NOT NULL,
    hour_utc timestamp NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_clutter_plan_hourly
ON collect.query_store_clutter_plan_hourly (server_id, hour_utc, database_name, query_id, plan_id)
NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS collect.query_store_clutter_collections_hourly
(
    server_id integer NOT NULL,
    hour_utc timestamp NOT NULL,
    database_name text,
    collections bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_clutter_collections_hourly
ON collect.query_store_clutter_collections_hourly (server_id, hour_utc, database_name)
NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS collect.query_store_clutter_hour_built
(
    server_id integer NOT NULL,
    hour_utc timestamp NOT NULL,
    built_at timestamp NOT NULL,
    rebuild_needed boolean NOT NULL DEFAULT false,
    CONSTRAINT pk_query_store_clutter_hour_built PRIMARY KEY (server_id, hour_utc)
);

ALTER TABLE collect.query_store_closed_day_tick ADD COLUMN IF NOT EXISTS hour_ticked_at timestamp;";

    /// <summary>Takes the hour's built row before anything is read, so a writer marking the same hour waits for this build or is read by it.
    /// $1 server_id, $2 hour, $3 built_at.</summary>
    public const string ClaimHourSql = @"
INSERT INTO collect.query_store_clutter_hour_built AS b (server_id, hour_utc, built_at, rebuild_needed)
VALUES ($1, $2, $3, false)
ON CONFLICT (server_id, hour_utc) DO UPDATE
SET built_at = EXCLUDED.built_at,
    rebuild_needed = false;";

    /// <summary>Clears one (server, hour) of the plan table. $1 server_id, $2 hour.</summary>
    public const string DeletePlanHourSql = @"
DELETE FROM collect.query_store_clutter_plan_hourly
WHERE server_id = $1
  AND hour_utc = $2;";

    /// <summary>Clears one (server, hour) of the collections table. $1 server_id, $2 hour.</summary>
    public const string DeleteCollectionsHourSql = @"
DELETE FROM collect.query_store_clutter_collections_hourly
WHERE server_id = $1
  AND hour_utc = $2;";

    /// <summary>Fills one (server, hour) of the plan table from raw <c>query_store_stats</c>: each plan's first and last <c>collection_time</c>
    /// in the hour. $1 server_id, $2 hour.</summary>
    public const string BuildPlanHourSql = @"
INSERT INTO collect.query_store_clutter_plan_hourly (server_id, hour_utc, database_name, query_id, plan_id, first_seen, last_seen)
SELECT s.server_id, $2, s.database_name, s.query_id, s.plan_id, MIN(s.collection_time), MAX(s.collection_time)
FROM collect.query_store_stats AS s
WHERE s.server_id = $1
  AND s.collection_time >= $2::timestamp
  AND s.collection_time < $2::timestamp + interval '1 hour'
GROUP BY s.server_id, s.database_name, s.query_id, s.plan_id;";

    /// <summary>Fills one (server, hour) of the collections table: the distinct <c>collection_time</c> values per database in the hour.
    /// $1 server_id, $2 hour.</summary>
    public const string BuildCollectionsHourSql = @"
INSERT INTO collect.query_store_clutter_collections_hourly (server_id, hour_utc, database_name, collections)
SELECT c.server_id, $2, c.database_name, COUNT(*)
FROM
(
    SELECT DISTINCT s.server_id, s.database_name, s.collection_time
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
      AND s.collection_time >= $2::timestamp
      AND s.collection_time < $2::timestamp + interval '1 hour'
) AS c
GROUP BY c.server_id, c.database_name;";

    /// <summary>
    /// Marks an hour as needing a rebuild. $1 server_id, $2 hour, $3 now. An hour with no built row gets one already marked, so a build of
    /// that hour running at the same moment cannot record it as built without this row: the insert waits on the build's own row and then marks it.
    /// </summary>
    public const string MarkLateHourSql = @"
INSERT INTO collect.query_store_clutter_hour_built AS b (server_id, hour_utc, built_at, rebuild_needed)
VALUES ($1, $2, $3, true)
ON CONFLICT (server_id, hour_utc) DO UPDATE
SET rebuild_needed = true
WHERE NOT b.rebuild_needed;";

    /// <summary>
    /// #9383: the first whole hour at or after the server's oldest raw row, as a SQL expression over <c>fl.t</c> (that row's
    /// <c>collection_time</c>). Raw is purged in whole chunks and a store may have begun collecting only recently, so an hour that starts before
    /// the oldest row is empty or partial in raw: it is never built, and a stamp it already has (and no rows) is removed.
    /// </summary>
    private const string FirstWholeHourSql = "date_trunc('hour', fl.t - interval '1 microsecond') + interval '1 hour'";

    /// <summary>#9383: each server's oldest raw row, one read of the raw table's <c>(server_id, collection_time)</c> index that stops at the first
    /// entry. A server with no raw row has no row here, so it plans no hour.</summary>
    private const string OldestRawRowSql = @"CROSS JOIN LATERAL
(
    SELECT f.collection_time AS t
    FROM collect.query_store_stats AS f
    WHERE f.server_id = s.server_id
    ORDER BY f.collection_time
    LIMIT 1
) AS fl";

    /// <summary>
    /// The hours due now: one row per (server, hour) with no built row or a marked one, newest hour first. $1 now, $2 raw retention in days,
    /// $3 <see cref="Grace"/>, $4 the cap. Hours run from the first whole hour inside raw's retention, or the first whole hour at or after the
    /// server's oldest raw row when that is later (#9383), through the newest closed hour, for the enabled servers.
    /// </summary>
    public const string PlanSql = @"
WITH span AS
(
    SELECT
        date_trunc('hour', $1::timestamp - make_interval(days => $2)) + interval '1 hour' AS first_hour,
        date_trunc('hour', $1::timestamp - $3::interval - interval '1 hour') AS last_hour
)
SELECT s.server_id, d.hour_utc
FROM collect.servers AS s
CROSS JOIN span
" + OldestRawRowSql + @"
CROSS JOIN LATERAL generate_series(GREATEST(span.first_hour, " + FirstWholeHourSql + @"), span.last_hour, interval '1 hour') AS d(hour_utc)
LEFT JOIN collect.query_store_clutter_hour_built AS b
  ON b.server_id = s.server_id AND b.hour_utc = d.hour_utc
WHERE s.is_enabled
  AND (b.server_id IS NULL OR b.rebuild_needed)
ORDER BY d.hour_utc DESC, s.server_id
LIMIT $4;";

    /// <summary>
    /// #9383: removes the built stamp of an hour that starts before its server's oldest raw row and holds no clutter row in either hourly table
    /// (an earlier build stamped it empty, before raw held it), so the clutter route reports the span as not collected instead of "no plans". An
    /// hour with rows keeps its stamp. A server with no raw row is left alone. Only a stamp built when raw could no longer hold its hour is removed
    /// (built at or after the hour plus the days raw keeps): a stamp built while raw held the hour is an hour the collector ran in that stored
    /// nothing, and it stays, so the hour reads "no plans" and not "not collected". $1 the days raw keeps (<see cref="RawHeldDaysAsync"/>).
    /// </summary>
    public const string UnstampBeforeFloorSql = @"
DELETE FROM collect.query_store_clutter_hour_built AS b
USING
(
    SELECT s.server_id, " + FirstWholeHourSql + @" AS first_hour
    FROM collect.servers AS s
    " + OldestRawRowSql + @"
) AS f
WHERE b.server_id = f.server_id
  AND b.hour_utc < f.first_hour
  AND b.built_at >= b.hour_utc + make_interval(days => $1)
  AND NOT EXISTS (SELECT 1 FROM collect.query_store_clutter_plan_hourly AS p WHERE p.server_id = b.server_id AND p.hour_utc = b.hour_utc)
  AND NOT EXISTS (SELECT 1 FROM collect.query_store_clutter_collections_hourly AS c WHERE c.server_id = b.server_id AND c.hour_utc = b.hour_utc);";

    /// <summary>The first whole hour raw's retention still holds whole, in the three tables' own terms. $1 now, $2 raw retention in days.</summary>
    private const string EdgeSql = "date_trunc('hour', $1::timestamp - make_interval(days => $2)) + interval '1 hour'";

    /// <summary>The hours one prune transaction removes from the three tables: one hour, so each transaction deletes a small slice (on the largest
    /// store about 430k rows and a few seconds, where six hours was about 2.8M rows and 400 MB of WAL in one statement) inside
    /// <see cref="QueryStoreClosedDayRollup.BuildStatementTimeoutSeconds"/>. A first prune over the weeks between the collector's retention and a
    /// hypertable raw's four days takes a hundred or more of them, spread over as many ticks as <see cref="MaxPruneDuration"/> needs (#9690).</summary>
    public const int PruneSliceHours = 1;

    /// <summary>No prune slice starts once this much of the tick has passed: half of <see cref="MaxTickDuration"/>, so a long prune leaves the
    /// hourly builds their time. The hours left wait for the next tick.</summary>
    public static readonly TimeSpan MaxPruneDuration = MaxTickDuration / 2;

    /// <summary>
    /// The next slice of hours to prune, or no row when no hour is older than the edge. $1 now, $2 the horizon in days, $3 the hours per slice.
    /// <c>lo</c> is the oldest stamped hour (an hour holds rows only while it is stamped) and <c>hi</c> is the lower of the edge (the first whole
    /// hour inside the horizon) and <c>lo</c> plus the slice. <c>servers</c> is every server the stamps hold: both rollups' unique indexes lead
    /// with <c>server_id</c>, so a delete that names them reads a range of the index and does not scan the table.
    /// </summary>
    public const string NextPruneSliceSql = @"
SELECT o.lo,
       LEAST(c.at, o.lo + make_interval(hours => $3)) AS hi,
       (SELECT array_agg(DISTINCT b.server_id) FROM collect.query_store_clutter_hour_built AS b) AS servers
FROM (SELECT " + EdgeSql + @" AS at) AS c
CROSS JOIN (SELECT min(b.hour_utc) AS lo FROM collect.query_store_clutter_hour_built AS b) AS o
WHERE o.lo < c.at;";

    /// <summary>Removes one slice of hours from the three tables, the stamps last. $1 server ids, $2 first hour, $3 the hour the slice stops before.</summary>
    public static readonly string[] PruneSliceSql =
    [
        "DELETE FROM collect.query_store_clutter_plan_hourly WHERE server_id = ANY($1::integer[]) AND hour_utc >= $2::timestamp AND hour_utc < $3::timestamp;",
        "DELETE FROM collect.query_store_clutter_collections_hourly WHERE server_id = ANY($1::integer[]) AND hour_utc >= $2::timestamp AND hour_utc < $3::timestamp;",
        "DELETE FROM collect.query_store_clutter_hour_built WHERE server_id = ANY($1::integer[]) AND hour_utc >= $2::timestamp AND hour_utc < $3::timestamp;",
    ];

    /// <summary>The enabled servers, each with its hour heartbeat (null before its first tick). Every enabled server is planned for hours.</summary>
    public const string HeartbeatServersSql = @"
SELECT sv.server_id, t.hour_ticked_at
FROM collect.servers AS sv
LEFT JOIN collect.query_store_closed_day_tick AS t ON t.server_id = sv.server_id
WHERE sv.is_enabled;";

    /// <summary>
    /// Marks every built hour that holds a row collected after the server's last hour heartbeat as needing a rebuild, and returns how many it
    /// marked: the mark a build that does not know the hourly tables never writes. The raw table has an index on (server, collection_time), so
    /// the scan reads only the rows since the heartbeat. #9384: it also marks the stamped clutter day (<see cref="QueryStoreClutterDayFold"/>) of
    /// each hour it marks. The service migrates the store before any tick, so the stamp table is always there and this does not probe for it.
    /// $1 server_id, $2 the server's last hour heartbeat.
    /// </summary>
    public const string MarkChangedHoursSql = @"
WITH late AS
(
    SELECT DISTINCT date_trunc('hour', s.collection_time) AS hour_utc
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
      AND s.collection_time > $2
),
marked AS
(
    UPDATE collect.query_store_clutter_hour_built AS b
    SET rebuild_needed = true
    FROM late AS l
    WHERE b.server_id = $1
      AND b.hour_utc = l.hour_utc
      AND NOT b.rebuild_needed
    RETURNING b.hour_utc
),
marked_days AS
(
    UPDATE collect.query_store_clutter_day_built AS d
    SET rebuild_needed = true
    FROM (SELECT DISTINCT m.hour_utc::date AS day_utc FROM marked AS m) AS md
    WHERE d.server_id = $1
      AND d.day_utc = md.day_utc
      AND NOT d.rebuild_needed
    RETURNING 1
)
SELECT COUNT(*) FROM marked;";

    /// <summary>Records the hour heartbeat; it never moves back. A server with no heartbeat row gets one. $1 server_id, $2 the heartbeat.</summary>
    public const string RecordHeartbeatSql = @"
INSERT INTO collect.query_store_closed_day_tick AS t (server_id, ticked_at, hour_ticked_at)
VALUES ($1, $2, $2)
ON CONFLICT (server_id) DO UPDATE
SET hour_ticked_at = EXCLUDED.hour_ticked_at
WHERE t.hour_ticked_at IS NULL OR t.hour_ticked_at < EXCLUDED.hour_ticked_at;";

    /// <summary>Whether the server's hour heartbeat is missing or older than $2. $1 server_id.</summary>
    public const string StaleHeartbeatSql = @"
SELECT NOT EXISTS
(
    SELECT 1
    FROM collect.query_store_closed_day_tick AS t
    WHERE t.server_id = $1
      AND t.hour_ticked_at >= (now() AT TIME ZONE 'UTC') - $2
);";

    /// <summary>The built, unmarked hours of one server in <c>[$2, $3)</c>, oldest first. $1 server_id.</summary>
    public const string ServedHoursSql = @"
SELECT b.hour_utc
FROM collect.query_store_clutter_hour_built AS b
WHERE b.server_id = $1
  AND b.hour_utc >= $2
  AND b.hour_utc < $3
  AND NOT b.rebuild_needed
ORDER BY b.hour_utc;";

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateTime Hour);

    /// <summary>What the hourly step of one tick did. <paramref name="PruneFailure"/> is the one-line text of the prune slice that failed (its hours and the error), or null when the prune did not fail: the step's outcome carries it, because a prune that fails every tick leaves the rollup growing past its horizon.</summary>
    public readonly record struct TickResult(int Built, int Failed, long Removed, int Deferred, string? PruneFailure = null);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The start of the hour <paramref name="value"/> falls in.</summary>
    public static DateTime FloorHour(DateTime value) => new(value.Ticks - value.Ticks % TimeSpan.TicksPerHour, DateTimeKind.Unspecified);

    /// <summary><paramref name="value"/> when it is on the hour, otherwise the next hour.</summary>
    public static DateTime CeilHour(DateTime value)
    {
        var floor = FloorHour(value);
        return floor == Unspecified(value) ? floor : floor.AddHours(1);
    }

    /// <summary>Whether the hour starting at <paramref name="hour"/> can already be built at <paramref name="nowUtc"/>: it has ended and
    /// <see cref="Grace"/> has passed.</summary>
    public static bool IsClosed(DateTime hour, DateTime nowUtc) => hour.AddHours(1) + Grace <= nowUtc;

    /// <summary>
    /// Marks the hour of a raw <c>query_store_stats</c> batch's <c>collection_time</c> for a rebuild when it is already closed, on the batch's
    /// connection and in its transaction, behind the caller's savepoint. Returns whether it marked. A live batch is stamped now, so its hour is
    /// open and nothing is sent.
    /// </summary>
    public static async Task<bool> TryMarkLateHourAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime collectionTime, DateTime nowUtc, int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        var hour = FloorHour(collectionTime);
        if (!IsClosed(hour, nowUtc))
        {
            return false;
        }

        await using var command = new NpgsqlCommand(MarkLateHourSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = hour });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        await command.ExecuteNonQueryAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// The built, unmarked hours of one server in <c>[first, afterLast)</c> (both on the hour), or null when the server's hour heartbeat is not
    /// fresh: a build that does not know these tables marks no hour, so the hours are not known to be current. Run it on the read's own
    /// connection, on a store at <see cref="Version"/> or above.
    /// </summary>
    public static async Task<HashSet<DateTime>?> ResolveServedHoursAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime first, DateTime afterLast, int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using (var stale = new NpgsqlCommand(StaleHeartbeatSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            stale.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            stale.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryStoreClosedDayRollup.HeartbeatFreshFor });
            if ((bool)(await stale.ExecuteScalarAsync(cancellationToken))!)
            {
                return null;
            }
        }

        var hours = new HashSet<DateTime>();
        if (afterLast <= first)
        {
            return hours;
        }

        await using var command = new NpgsqlCommand(ServedHoursSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(first) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(afterLast) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            hours.Add(Unspecified(reader.GetDateTime(0)));
        }

        return hours;
    }

    /// <summary>The start of the unbroken run of <paramref name="hours"/> that ends at <paramref name="from"/> (exclusive), no earlier than
    /// <paramref name="lowest"/>; <paramref name="from"/> itself when the hour before it is not in the set.</summary>
    public static DateTime ExtendBack(HashSet<DateTime> hours, DateTime from, DateTime lowest)
    {
        ArgumentNullException.ThrowIfNull(hours);
        var start = from;
        while (start.AddHours(-1) >= lowest && hours.Contains(start.AddHours(-1)))
        {
            start = start.AddHours(-1);
        }

        return start;
    }

    /// <summary>The exclusive end of the unbroken run of <paramref name="hours"/> that starts at <paramref name="from"/>, no later than
    /// <paramref name="highest"/>; <paramref name="from"/> itself when its hour is not in the set.</summary>
    public static DateTime ExtendForward(HashSet<DateTime> hours, DateTime from, DateTime highest)
    {
        ArgumentNullException.ThrowIfNull(hours);
        var end = from;
        while (end < highest && hours.Contains(end))
        {
            end = end.AddHours(1);
        }

        return end;
    }

    /// <summary>The longest unbroken run of <paramref name="hours"/> as <c>[Start, End)</c>, the newest on a tie, or null for none.</summary>
    public static (DateTime Start, DateTime End)? LongestRun(HashSet<DateTime> hours)
    {
        ArgumentNullException.ThrowIfNull(hours);
        var sorted = hours.OrderBy(h => h).ToList();
        if (sorted.Count == 0)
        {
            return null;
        }

        int bestStart = 0, bestLength = 1, runStart = 0;
        for (var i = 1; i <= sorted.Count; i++)
        {
            if (i < sorted.Count && sorted[i] == sorted[i - 1].AddHours(1))
            {
                continue;
            }

            if (i - runStart >= bestLength)
            {
                bestStart = runStart;
                bestLength = i - runStart;
            }

            runStart = i;
        }

        return (sorted[bestStart], sorted[bestStart + bestLength - 1].AddHours(1));
    }

    /// <summary>
    /// The first step of the hourly work for every enabled server: moves the hour heartbeat to <paramref name="nowUtc"/> less
    /// <see cref="QueryStoreClosedDayRollup.HeartbeatMargin"/>. When the last hour heartbeat is older than
    /// <see cref="QueryStoreClosedDayRollup.HeartbeatFreshFor"/> (a build that does not know the hourly tables ran in between, or ticks stopped),
    /// it first marks the built hours that hold a row collected since, in the same transaction. A server whose step fails keeps its old heartbeat.
    /// Returns how many hours it marked.
    /// </summary>
    public static async Task<int> HeartbeatAsync(NpgsqlConnection connection, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);

        var servers = new List<(int ServerId, DateTime? Previous)>();
        await using (var list = new NpgsqlCommand(HeartbeatServersSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await list.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add((reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1)));
            }
        }

        var marked = 0;
        var beat = Unspecified(nowUtc - QueryStoreClosedDayRollup.HeartbeatMargin);
        foreach (var (serverId, previous) in servers)
        {
            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                if (previous is { } last && last < nowUtc - QueryStoreClosedDayRollup.HeartbeatFreshFor)
                {
                    await using var mark = new NpgsqlCommand(MarkChangedHoursSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
                    mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                    mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(last) });
                    marked += Convert.ToInt32(await mark.ExecuteScalarAsync(cancellationToken));
                }

                await using (var record = new NpgsqlCommand(RecordHeartbeatSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
                {
                    record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                    record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = beat });
                    await record.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Query Store hourly clutter rollups: the hour heartbeat of server {ServerId} failed and is retried next tick: {Message}", serverId, ex.Message);
            }
        }

        return marked;
    }

    /// <summary>The builds due at <paramref name="nowUtc"/>, newest hour first.</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, int rawRetentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rawRetentionDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = Grace });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetInt32(0), Unspecified(reader.GetDateTime(1))));
        }

        return builds;
    }

    /// <summary>One slice of hours to prune: <c>[From, To)</c> for the servers the stamps hold.</summary>
    public readonly record struct PruneSlice(DateTime From, DateTime To, int[] ServerIds);

    /// <summary>The next slice of hours before the first whole hour inside <paramref name="horizonDays"/>, or null when no hour is older than that.</summary>
    public static async Task<PruneSlice?> NextPruneSliceAsync(
        NpgsqlConnection connection, DateTime nowUtc, int horizonDays, int sliceHours, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(NextPruneSliceSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = horizonDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = sliceHours });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new PruneSlice(reader.GetDateTime(0), reader.GetDateTime(1), reader.GetFieldValue<int[]>(2))
            : null;
    }

    /// <summary>
    /// #9690: removes the hours before the first whole hour inside <paramref name="horizonDays"/> (<see cref="RawHeldDaysAsync"/>'s answer) from the
    /// three tables, oldest first, <paramref name="sliceHours"/> hours per transaction, and returns the rollup rows removed. A slice's rows and stamps go in
    /// one transaction, so a stamp never names an hour whose rows are gone. No slice starts once <paramref name="elapsed"/> reaches
    /// <see cref="MaxPruneDuration"/>: the hours left wait for the next tick. After each commit the WAL the slice wrote goes to the pacer
    /// <paramref name="pacerFactory"/> builds (null runs unpaced), which is the retention purge's. A slice that fails (it rolled back, so its hours
    /// are untouched) is logged with its hours and ends this prune, and the rows the earlier slices removed are still returned: the next tick starts at
    /// the same slice, and a failure there must not stop the hourly builds that follow the prune. <paramref name="onSliceFailed"/> receives the failed
    /// slice's hours and error as one line, so the caller can report it in the step's outcome.
    /// </summary>
    public static async Task<long> PruneAsync(
        NpgsqlConnection connection, DateTime nowUtc, int horizonDays, int sliceHours, Func<TimeSpan> elapsed,
        Func<CancellationToken, Task<IBatchWalPacer?>>? pacerFactory, ILogger logger, CancellationToken cancellationToken,
        Action<string>? onSliceFailed = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(elapsed);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThan(sliceHours, 1);

        long removed = 0;
        IBatchWalPacer? pacer = null;
        var pacerBuilt = false;
        while (elapsed() < MaxPruneDuration && await NextPruneSliceAsync(connection, nowUtc, horizonDays, sliceHours, cancellationToken) is { } slice)
        {
            if (!pacerBuilt && pacerFactory is not null)
            {
                pacer = await pacerFactory(cancellationToken);
                pacerBuilt = true;
            }

            var walBefore = pacer is null ? null : await pacer.ReadWalPositionAsync(connection, cancellationToken);
            try
            {
                removed += await PruneSliceAsync(connection, slice, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Query Store hourly clutter rollups: pruning hours {From:yyyy-MM-dd HH} to {To:yyyy-MM-dd HH} failed, the prune stops for this tick and the builds go on: {Message}",
                    slice.From, slice.To, ex.Message);
                onSliceFailed?.Invoke(FormattableString.Invariant($"pruning hours {slice.From:yyyy-MM-dd HH} to {slice.To:yyyy-MM-dd HH} failed: {ex.Message.ReplaceLineEndings(" ")}"));
                break;
            }

            if (pacer is not null)
            {
                await pacer.AfterBatchAsync(connection, walBefore, cancellationToken);
            }
        }

        return removed;
    }

    private static async Task<long> PruneSliceAsync(NpgsqlConnection connection, PruneSlice slice, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        long removed = 0;
        for (var i = 0; i < PruneSliceSql.Length; i++)
        {
            await using var command = new NpgsqlCommand(PruneSliceSql[i], connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = slice.ServerIds });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(slice.From) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(slice.To) });
            var rows = await command.ExecuteNonQueryAsync(cancellationToken);
            if (i < 2)
            {
                removed += rows;
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return removed;
    }

    /// <summary>
    /// The days raw <c>query_store_stats</c> keeps on this store, or null when that is not known. A hypertable raw table is dropped only by the gated
    /// raw purge, at <see cref="TimescaleSupport.RawRetentionSpan"/>, and a custom retention does not apply to it. A plain raw table is cleared by the
    /// purge's time-sliced DELETE at <paramref name="rawRetentionDays"/>. The hypertable answer is the one the retention sweep uses to leave the raw
    /// tables to the gated purge (<see cref="TimescaleSupport.ReadRawHypertablesAsync"/>).
    /// </summary>
    public static async Task<int?> RawHeldDaysAsync(NpgsqlConnection connection, int? rawRetentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var hypertables = await TimescaleSupport.ReadRawHypertablesAsync(connection, CommandTimeoutSeconds, cancellationToken);
        return hypertables.Contains("query_store_stats", StringComparer.Ordinal) ? (int)TimescaleSupport.RawRetentionSpan.TotalDays : rawRetentionDays;
    }

    /// <summary>#9383: removes the empty built stamps of hours before each server's oldest raw row that were built when raw could no longer hold
    /// their hour (<see cref="UnstampBeforeFloorSql"/>); returns how many. <paramref name="rawHeldDays"/> is <see cref="RawHeldDaysAsync"/>'s answer.</summary>
    public static async Task<long> UnstampBeforeFloorAsync(NpgsqlConnection connection, int rawHeldDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(UnstampBeforeFloorSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rawHeldDays });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Builds one (server, hour) in one transaction: the built row is taken first, the hour is cleared and filled again.</summary>
    public static async Task BuildHourAsync(
        NpgsqlConnection connection, int serverId, DateTime hour, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var sql in new[] { ClaimHourSql, DeletePlanHourSql, DeleteCollectionsHourSql, BuildPlanHourSql, BuildCollectionsHourSql })
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(hour) });
            if (ReferenceEquals(sql, ClaimHourSql))
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            }

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// The hourly step of one tick: the hour heartbeat, the horizon raw is held to (<see cref="RawHeldDaysAsync"/>, read once and used by the prune,
    /// the unstamp and the plan alike), the sliced prune (<see cref="PruneAsync"/>), then the planned builds, newest hour first, each in its own
    /// transaction. A build that fails is logged and the loop goes on; the next tick plans it again. No prune slice starts once
    /// <see cref="MaxPruneDuration"/> has passed on <paramref name="elapsed"/>, and no build starts once <see cref="MaxTickDuration"/> has.
    /// </summary>
    public static async Task<TickResult> RunAsync(
        NpgsqlConnection connection, DateTime nowUtc, int rawRetentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken,
        Func<CancellationToken, Task<IBatchWalPacer?>>? pacerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        await HeartbeatAsync(connection, nowUtc, logger, cancellationToken);
        var horizonDays = await RawHeldDaysAsync(connection, rawRetentionDays, cancellationToken) ?? rawRetentionDays;
        string? pruneFailure = null;
        var removed = await PruneAsync(connection, nowUtc, horizonDays, PruneSliceHours, elapsed, pacerFactory, logger, cancellationToken, failure => pruneFailure = failure);
        removed += await UnstampBeforeFloorAsync(connection, horizonDays, cancellationToken);
        var plan = await PlanBuildsAsync(connection, nowUtc, horizonDays, cancellationToken);

        int built = 0, failed = 0, started = 0;
        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                await BuildHourAsync(connection, item.ServerId, item.Hour, nowUtc, cancellationToken);
                built++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Query Store hourly clutter rollups: building server {ServerId} hour {Hour:yyyy-MM-dd HH} failed and is retried next tick: {Message}",
                    item.ServerId, item.Hour, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        logger.LogInformation("Query Store hourly clutter rollups: built {Built}, failed {Failed}, deferred {Deferred}, removed {Removed} row(s)", built, failed, deferred, removed);
        return new TickResult(built, failed, removed, deferred, pruneFailure);
    }
}
