/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6800 part 2, step 3b: <c>collect.query_store_hash_daily</c>, the whole-UTC-day sum of <c>collect.query_store_hash_hourly</c> for the
/// fleet Query Store tag roll-up's 7 and 30 day windows (a 7 day read is about 26 million hourly rows on a store with 42 servers, about 10
/// times fewer day rows). The DDL is part of V221 (<see cref="QueryStoreComposeStamp.HashHourlyCreateSql"/>). This class builds the days,
/// removes them, and answers which days a reader may use.
///
/// <para><b>The validity rule.</b> A (server, day) row set may be read only when, in the reader's own snapshot, all three hold:
/// (1) all 24 hours of the day are in <c>query_store_compose_stamp_hours</c>; (2) every pair of that server in the day has
/// <c>hash_built_seq = late_seq</c>; (3) the server's pairs of the day still have the count and the sum of <c>hash_built_seq</c> that the
/// day was built from (<c>query_store_hash_daily_built.pair_count</c>, <c>seq_sum</c>; no ledger row reads as 0 and 0). Part 3 is read from
/// ledger state only, never from a time. It cannot miss a change because: a pair's <c>hash_built_seq</c> only ever grows (an hour build
/// writes the <c>late_seq</c> it read, and <c>late_seq</c> only grows; builds of one hour are serialized by the hour lock) and pairs are not
/// removed while their hour is in the hours table; the hourly rows of (server, hour) change only when that hour is rebuilt, and a rebuild
/// that changes them follows a late write, which raised <c>late_seq</c> above the <c>hash_built_seq</c> the day saw, so the rebuild
/// writes a larger <c>hash_built_seq</c>; a late write that creates a pair adds one to the count. With every pair valid (part 2) and no
/// pair smaller than at the build, equal count and equal sum mean no pair grew and none was added, so no hourly row of the server in the
/// day was rewritten. A pair that is stale makes part 2 fail at once, before any rebuild.</para>
///
/// <para>The day build reads the ledger fingerprint and the hourly rows in ONE statement (one snapshot), so an hour build that commits
/// meanwhile is either in both or in neither. The hour builds and the day build take different locks, so a day built from a snapshot
/// that an hour build then overtakes is recorded with the older fingerprint and reads as invalid until the next tick rebuilds it.</para>
///
/// <para>Granularity: a build is one UTC day for all servers, one transaction with its ledger rows. A rebuild re-sums the day's hourly rows
/// for every server even when one server changed (the hour builds already rebuild every server of an hour), which keeps the unit of work,
/// the lock, the cap and the failure record per day. A server whose pairs are not all valid gets no rows and no ledger row in the build,
/// so it reads as invalid until its hours are rebuilt and the day is rebuilt.</para>
/// </summary>
public static class QueryStoreHashDaily
{
    public const string Table = "collect.query_store_hash_daily";
    public const string BuiltTable = "collect.query_store_hash_daily_built";
    public const string FailuresTable = "collect.query_store_hash_daily_failures";

    /// <summary>
    /// The days one tick builds at most, newest first, after the tick's hours. A day sums about 3.8 million hourly rows on a store with 42
    /// servers, so the cap keeps the backfill after an upgrade (up to ten days, each buildable once its hours are) from taking a tick the
    /// hours need. Days are also held to the tick's <see cref="QueryStoreComposeStamp.MaxTickDuration"/>.
    /// </summary>
    public const int MaxBuildsPerTick = 2;

    /// <summary>The hours a UTC day has, all of which must be in the hours table before the day is built or read.</summary>
    public const int HoursPerDay = 24;

    /// <summary>The advisory lock of a day build: key one is the issue, key two the number of days since 1970-01-01 (the hour lock's shape).</summary>
    public const string DayLockSql = "SELECT pg_advisory_xact_lock(6800221, $1::integer);";

    /// <summary>The hours of the day in the hours table: $1 the day start.</summary>
    public const string HoursInDaySql = @"
SELECT count(*)
FROM collect.query_store_compose_stamp_hours AS h
WHERE h.hour >= $1 AND h.hour < $1 + interval '1 day';";

    /// <summary>Clears the day's rows before they are rebuilt: $1 the day. Served by the (day, server_id) index.</summary>
    public const string DeleteDaySql = @"
DELETE FROM collect.query_store_hash_daily
WHERE day = $1;";

    /// <summary>Clears the day's ledger rows before they are rebuilt: $1 the day.</summary>
    public const string DeleteDayBuiltSql = @"
DELETE FROM collect.query_store_hash_daily_built
WHERE day = $1;";

    /// <summary>
    /// Builds one day: $1 the day start, $2 built_at. ONE statement, so the fingerprint (<c>fp</c>: per server, the pair count and the sum
    /// of <c>hash_built_seq</c> over the day's 24 hours, kept only for a server whose pairs ALL have <c>hash_built_seq = late_seq</c>) and
    /// the hourly rows summed come from one snapshot. The rows are the day's 24 hourly rows grouped by <c>server_id, query_hash</c>: sums
    /// stay exact (<c>numeric</c>), <c>mem_max</c> is the MAX, <c>wide_rows</c> the sum. Returns the day rows and the ledger rows written.
    /// </summary>
    public const string BuildDaySql = @"
WITH fp AS
(
    SELECT b.server_id, count(*)::integer AS pair_count, sum(b.hash_built_seq)::bigint AS seq_sum
    FROM collect.query_store_compose_stamp_built AS b
    WHERE b.hour >= $1 AND b.hour < $1 + interval '1 day'
    GROUP BY b.server_id
    HAVING count(*) FILTER (WHERE b.hash_built_seq IS DISTINCT FROM b.late_seq) = 0
),
rows_in AS
(
    INSERT INTO collect.query_store_hash_daily (day, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum)
    SELECT $1, sum(h.wide_rows)::bigint, max(h.mem_max), h.server_id, h.query_hash,
           sum(h.ec_sum), sum(h.cpu_wsum), sum(h.dur_wsum), sum(h.reads_wsum), sum(h.writes_wsum)
    FROM collect.query_store_hash_hourly AS h
    JOIN fp ON fp.server_id = h.server_id
    WHERE h.hour >= $1 AND h.hour < $1 + interval '1 day'
    GROUP BY h.server_id, h.query_hash
    RETURNING 1
),
led AS
(
    INSERT INTO collect.query_store_hash_daily_built (day, server_id, pair_count, seq_sum, built_at)
    SELECT $1, fp.server_id, fp.pair_count, fp.seq_sum, $2 FROM fp
    RETURNING 1
)
SELECT (SELECT count(*) FROM rows_in), (SELECT count(*) FROM led);";

    /// <summary>
    /// The days to build: $1 the cap, $2 the store clock a test passes (NULL in the service: the store's own), $3 the statement-timeout
    /// SQLSTATE. A day is due when all 24 hours are in the hours table and some server whose pairs of the day are all valid has no ledger
    /// row for it or a ledger fingerprint that differs from its pairs now. Newest day first, at most the cap. A day with a failure record
    /// is planned only once its <c>retry_at</c> is due, as <see cref="QueryStoreComposeStamp.PlanSql"/> does for an hour. Columns: day, the
    /// consecutive failures (0 for none), and whether the last failure was a statement timeout.
    /// </summary>
    public const string PlanSql = @"
WITH clock AS
(
    SELECT coalesce($2::timestamp, now() AT TIME ZONE 'UTC') AS at
),
full_days AS
(
    SELECT date_trunc('day', h.hour) AS day
    FROM collect.query_store_compose_stamp_hours AS h
    GROUP BY date_trunc('day', h.hour)
    HAVING count(*) = 24
),
cur AS
(
    SELECT d.day, b.server_id, count(*)::integer AS pair_count, sum(b.hash_built_seq)::bigint AS seq_sum,
           count(*) FILTER (WHERE b.hash_built_seq IS DISTINCT FROM b.late_seq) AS invalid
    FROM full_days AS d
    JOIN collect.query_store_compose_stamp_built AS b ON b.hour >= d.day AND b.hour < d.day + interval '1 day'
    GROUP BY d.day, b.server_id
),
due AS
(
    SELECT DISTINCT c.day
    FROM cur AS c
    LEFT JOIN collect.query_store_hash_daily_built AS l ON l.day = c.day AND l.server_id = c.server_id
    WHERE c.invalid = 0
      AND (l.server_id IS NULL OR l.pair_count <> c.pair_count OR l.seq_sum <> c.seq_sum)
)
SELECT u.day, coalesce(f.failures, 0), coalesce(f.last_sqlstate = $3::text, false)
FROM due AS u
LEFT JOIN collect.query_store_hash_daily_failures AS f ON f.day = u.day
CROSS JOIN clock
WHERE f.day IS NULL
   OR f.retry_at <= clock.at
ORDER BY u.day DESC
LIMIT $1;";

    /// <summary>
    /// The (day, server) pairs a reader may take from the day table: $1 the servers (integer[]), $2 and $3 the window [a, b). Whole UTC
    /// days inside the window only (<c>a &lt;= day</c> and <c>day + 1 day &lt;= b</c>). Run it in the SAME snapshot as the read of the
    /// day rows (one repeatable-read transaction, or one statement), because validity is a fact about the ledger at a moment. A server
    /// with no pair in a valid day is returned: the day is complete and empty for it.
    /// </summary>
    public const string ValidDaysSql = @"
WITH full_days AS
(
    SELECT g.day
    FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), interval '1 day') AS g (day)
    WHERE g.day >= $2::timestamp
      AND g.day + interval '1 day' <= $3::timestamp
      AND (SELECT count(*) FROM collect.query_store_compose_stamp_hours AS h WHERE h.hour >= g.day AND h.hour < g.day + interval '1 day') = 24
)
SELECT d.day, s.server_id
FROM full_days AS d
CROSS JOIN (SELECT DISTINCT u.server_id FROM unnest($1::integer[]) AS u (server_id)) AS s
CROSS JOIN LATERAL
(
    SELECT count(*)::integer AS pair_count, coalesce(sum(b.hash_built_seq), 0)::bigint AS seq_sum,
           count(*) FILTER (WHERE b.hash_built_seq IS DISTINCT FROM b.late_seq) AS invalid
    FROM collect.query_store_compose_stamp_built AS b
    WHERE b.server_id = s.server_id AND b.hour >= d.day AND b.hour < d.day + interval '1 day'
) AS cur
LEFT JOIN collect.query_store_hash_daily_built AS l ON l.day = d.day AND l.server_id = s.server_id
WHERE cur.invalid = 0
  AND cur.pair_count = coalesce(l.pair_count, 0)
  AND cur.seq_sum = coalesce(l.seq_sum, 0)
ORDER BY d.day, s.server_id;";

    /// <summary>Records a failed day build and returns its consecutive failures, next attempt and whether that is a backoff. The parameters and the rule are those of <see cref="QueryStoreComposeStamp.RecordFailureSql"/>, keyed by day.</summary>
    public const string RecordFailureSql = @"
INSERT INTO collect.query_store_hash_daily_failures AS f
    (day, failures, last_sqlstate, last_reason, last_timeout_seconds, first_failed_at, last_failed_at, retry_at)
SELECT $1, 1, $2::text, $3::text, $4::integer, c.at, c.at,
       c.at + CASE WHEN 1 >= $6::integer OR ($2::text = $9::text AND $4::integer >= $7::integer) THEN $8::interval ELSE interval '0' END
FROM (SELECT coalesce($5::timestamp, now() AT TIME ZONE 'UTC') AS at) AS c
ON CONFLICT (day) DO UPDATE
SET failures = f.failures + 1,
    last_sqlstate = EXCLUDED.last_sqlstate,
    last_reason = EXCLUDED.last_reason,
    last_timeout_seconds = EXCLUDED.last_timeout_seconds,
    last_failed_at = EXCLUDED.last_failed_at,
    retry_at = EXCLUDED.last_failed_at
        + CASE WHEN f.failures + 1 >= $6::integer OR (EXCLUDED.last_sqlstate = $9::text AND EXCLUDED.last_timeout_seconds >= $7::integer)
               THEN $8::interval ELSE interval '0' END
RETURNING f.failures, f.retry_at, f.retry_at > f.last_failed_at AS backed_off;";

    /// <summary>A successful day build clears the day's failure record: $1 the day. In the build's own transaction.</summary>
    public const string ClearFailureSql = @"
DELETE FROM collect.query_store_hash_daily_failures
WHERE day = $1;";

    /// <summary>
    /// The cleanup, on the wide table's horizon: $1 the floor (<see cref="QueryStoreComposeStamp.FloorHour"/>). A day goes only when its whole
    /// day is below the floor (<c>day + 1 day &lt;= floor</c>, written so the (day, server_id) index serves it): a day that straddles the
    /// floor has lost hours from the hours table, so no reader takes it, and it goes with the next floor.
    /// </summary>
    public const string GcDaySql = @"
DELETE FROM collect.query_store_hash_daily
WHERE day <= $1 - interval '1 day';";

    public const string GcDayBuiltSql = @"
DELETE FROM collect.query_store_hash_daily_built
WHERE day <= $1 - interval '1 day';";

    public const string GcDayFailuresSql = @"
DELETE FROM collect.query_store_hash_daily_failures
WHERE day <= $1 - interval '1 day';";

    /// <summary>One planned day build: <paramref name="Failures"/> its consecutive failed builds, <paramref name="Extended"/> true when the last was a statement timeout.</summary>
    public readonly record struct Build(DateTime Day, int Failures, bool Extended);

    /// <summary>What a day build wrote: the day rows and the ledger rows (one per server whose pairs were all valid).</summary>
    public readonly record struct DayBuilt(long Rows, long Servers);

    /// <summary>What a tick's days did.</summary>
    public readonly record struct TickResult(int Built, int Failed);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Whole days since 1970-01-01 of <paramref name="day"/>: the day lock's second key.</summary>
    public static int DayNumber(DateTime day) =>
        checked((int)((DateTime.SpecifyKind(day, DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerDay));

    /// <summary>The days to build, newest first, at most <see cref="MaxBuildsPerTick"/> (<see cref="PlanSql"/>).</summary>
    public static async Task<IReadOnlyList<Build>> PlanAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken, DateTime? storeNow = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = QueryStoreComposeStamp.BuildStatementTimeoutSeconds + 30 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = storeNow is { } at ? Unspecified(at) : DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = QueryStoreComposeStamp.StatementTimeoutSqlState });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetDateTime(0), reader.GetInt32(1), reader.GetBoolean(2)));
        }

        return builds;
    }

    /// <summary>
    /// The (day, server) pairs a reader may take from the day table in [<paramref name="from"/>, <paramref name="to"/>)
    /// (<see cref="ValidDaysSql"/>), on <paramref name="connection"/> and its current transaction.
    /// </summary>
    public static async Task<IReadOnlyList<(DateTime Day, int ServerId)>> ReadValidDaysAsync(
        NpgsqlConnection connection, IReadOnlyCollection<int> servers, DateTime from, DateTime to, CancellationToken cancellationToken,
        int commandTimeoutSeconds = QueryStoreComposeStamp.BuildStatementTimeoutSeconds)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(servers);

        var days = new List<(DateTime, int)>();
        var ids = System.Linq.Enumerable.ToArray(servers);
        await using var command = new NpgsqlCommand(ValidDaysSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ids });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(from) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(to) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            days.Add((reader.GetDateTime(0), reader.GetInt32(1)));
        }

        return days;
    }

    /// <summary>
    /// Builds one day in one transaction: lock the day, check that its 24 hours are in the hours table, clear the day's rows and ledger
    /// rows, write both from <see cref="BuildDaySql"/>, clear the day's failure record, commit. Returns null (nothing changed) when an
    /// hour of the day is not in the hours table. <paramref name="statementTimeoutSeconds"/> is the transaction's <c>statement_timeout</c>.
    /// </summary>
    public static async Task<DayBuilt?> BuildDayAsync(
        NpgsqlConnection connection, DateTime day, DateTime nowUtc, CancellationToken cancellationToken,
        int statementTimeoutSeconds = QueryStoreComposeStamp.BuildStatementTimeoutSeconds, Func<Task>? beforeCommit = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentOutOfRangeException.ThrowIfLessThan(statementTimeoutSeconds, 1);

        var dayValue = Unspecified(day);
        var commandTimeout = Math.Max(QueryStoreComposeStamp.BuildStatementTimeoutSeconds + 30, statementTimeoutSeconds + 30);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        NpgsqlCommand NewCommand(string sql) => new(sql, connection, transaction) { CommandTimeout = commandTimeout };

        await using (var timeout = NewCommand(QueryStoreComposeStamp.StatementTimeoutSql(statementTimeoutSeconds)))
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var advisory = NewCommand(DayLockSql))
        {
            advisory.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DayNumber(dayValue) });
            await advisory.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var hours = NewCommand(HoursInDaySql))
        {
            hours.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = dayValue });
            if (Convert.ToInt64(await hours.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture) != HoursPerDay)
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        foreach (var sql in new[] { DeleteDaySql, DeleteDayBuiltSql })
        {
            await using var delete = NewCommand(sql);
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = dayValue });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        DayBuilt built;
        await using (var build = NewCommand(BuildDaySql))
        {
            build.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = dayValue });
            build.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            await using var reader = await build.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            built = new DayBuilt(reader.GetInt64(0), reader.GetInt64(1));
        }

        await using (var clear = NewCommand(ClearFailureSql))
        {
            clear.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = dayValue });
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Test seam: runs with the build's rows written and nothing committed. */
        if (beforeCommit is not null)
        {
            await beforeCommit();
        }

        await transaction.CommitAsync(cancellationToken);
        return built;
    }

    /// <summary>Records a failed day build (<see cref="RecordFailureSql"/>) on <paramref name="connection"/>, outside any transaction.</summary>
    public static async Task<QueryStoreComposeStamp.FailureOutcome> RecordFailureAsync(
        NpgsqlConnection connection, DateTime day, string? sqlState, string reason, int timeoutSeconds, DateTime? storeNow, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(RecordFailureSql, connection) { CommandTimeout = QueryStoreComposeStamp.BuildStatementTimeoutSeconds + 30 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(day) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)sqlState ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = reason ?? string.Empty });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = timeoutSeconds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = storeNow is { } at ? Unspecified(at) : DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = QueryStoreComposeStamp.FailuresBeforeBackoff });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = QueryStoreComposeStamp.ExtendedBuildStatementTimeoutSeconds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryStoreComposeStamp.BackoffInterval });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = QueryStoreComposeStamp.StatementTimeoutSqlState });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new QueryStoreComposeStamp.FailureOutcome(reader.GetInt32(0), reader.GetDateTime(1), reader.GetBoolean(2));
    }

    /// <summary>
    /// The cleanup's day step, inside the caller's transaction (<see cref="QueryStoreComposeStamp.GcAsync"/>): the day rows, ledger rows and
    /// failure records of every day whose whole day is below <paramref name="floor"/> (<see cref="GcDaySql"/>).
    /// </summary>
    public static async Task GcAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime floor, CancellationToken cancellationToken)
    {
        foreach (var sql in new[] { GcDaySql, GcDayBuiltSql, GcDayFailuresSql })
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = QueryStoreComposeStamp.BuildStatementTimeoutSeconds + 30 };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(floor) });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// The days step of the hourly tick, after the hours: plan, then build each planned day in its own transaction, newest first. A build
    /// that fails is logged, recorded (<see cref="RecordFailureSql"/>, the hours' back-off and extended timeout) and the loop goes on. No
    /// build starts once <paramref name="elapsed"/> reaches <see cref="QueryStoreComposeStamp.MaxTickDuration"/>.
    /// </summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlConnection connection, DateTime nowUtc, ILogger logger, Func<TimeSpan> elapsed, DateTime? storeNow, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, failed = 0;
        foreach (var item in await PlanAsync(connection, cancellationToken, storeNow))
        {
            if (elapsed() >= QueryStoreComposeStamp.MaxTickDuration)
            {
                break;
            }

            var timeoutSeconds = item.Extended
                ? QueryStoreComposeStamp.ExtendedBuildStatementTimeoutSeconds
                : QueryStoreComposeStamp.BuildStatementTimeoutSeconds;
            try
            {
                if (await BuildDayAsync(connection, item.Day, nowUtc, cancellationToken, timeoutSeconds) is not null)
                {
                    built++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                var sqlState = (ex as PostgresException)?.SqlState;
                QueryStoreComposeStamp.FailureOutcome? outcome = null;
                try
                {
                    outcome = await RecordFailureAsync(connection, item.Day, sqlState, ex.Message, timeoutSeconds, storeNow, cancellationToken);
                }
                catch (Exception recordEx) when (recordEx is not OperationCanceledException)
                {
                    /* A broken connection fails the record too; the day has no new record and is planned again next tick. */
                    logger.LogDebug("Query Store hash-daily: recording the failure of day {Day:yyyy-MM-dd} failed: {Message}", item.Day, recordEx.Message);
                }

                logger.LogWarning(
                    "Query Store hash-daily: building day {Day:yyyy-MM-dd} failed ({Failures} time(s) in a row), the day stays unbuilt{Backoff}: {Message}",
                    item.Day, outcome?.Failures ?? item.Failures + 1, outcome is { BackedOff: true } ? "; the next attempt is a day away" : "; the next tick retries", ex.Message);
            }
        }

        return new TickResult(built, failed);
    }
}
