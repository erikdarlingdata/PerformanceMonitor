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
/// #9384: a Query Store clutter day built from its 24 hourly clutter rows (<see cref="QueryStoreClutterHourly"/>) soon after the day ends,
/// instead of from raw at <see cref="QueryStoreClosedDayRollup.CloseAfter"/>. The clutter daily rows share the closed-day built row with the
/// Query Store grid tables, so a clutter day waited for the grid's close (more than a day after midnight) and the plan-churn read served the
/// newest day and a half from the hourly table. The clutter rows are placed by <c>collection_time</c>, which is stamped at collection, and an
/// hour closes at its end plus <see cref="QueryStoreClutterHourly.Grace"/>, so the day can close as soon as all of its hours have.
///
/// <para><b>The stamp.</b> <c>query_store_clutter_day_built</c> says which (server, day) this fold built. It is separate from the grid's
/// <c>built_at</c>, so the clutter day closes before the grid day does. The grid's own build at <see cref="QueryStoreClosedDayRollup.CloseAfter"/>
/// still builds a clutter day (from raw, or keeps the one it has when raw is purged, #9287) and stamps it on the grid row as before. When it does,
/// it removes this stamp: the grid row is then the one that says the day is served.</para>
///
/// <para><b>The fold.</b> A (server, day) is planned when all 24 of its hours are built and unmarked, and it has no stamp, or a marked one. The
/// fold takes the stamp first, checks the hours again under it, clears the day and fills it from the hours: the earliest <c>first_seen</c> and
/// the latest <c>last_seen</c> per plan, and the sum of the collections. A collection belongs to exactly one hour, so the sums are the day's.</para>
///
/// <para><b>Late rows.</b> A raw batch whose hour is already closed marks the hour (<see cref="QueryStoreClutterHourly.TryMarkLateHourAsync"/>)
/// and, in the same transaction, the hour's clutter day (<see cref="MarkLateDaySql"/>). The mark and the fold take the same stamp row, so a row
/// written while its day is folded is either read by the fold or marks the day after it. A marked day is read from the hours again until the next
/// tick rebuilds it.</para>
///
/// <para><b>Retention.</b> The tick removes a stamped day's clutter rows and stamp once the day is before the first whole day inside the day
/// rollups' retention, the edge <see cref="QueryStoreClosedDayRollup.GcAsync"/> uses for the days it built.</para>
/// </summary>
public static class QueryStoreClutterDayFold
{
    /// <summary>The migration that creates the stamp table.</summary>
    public const int Version = 303;

    /// <summary>The most (server, day) folds one tick plans.</summary>
    public const int MaxFoldsPerTick = 200;

    private const int CommandTimeoutSeconds = QueryStoreClosedDayRollup.BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{QueryStoreClosedDayRollup.BuildStatementTimeoutSeconds}s'";

    /// <summary>
    /// #9384: the clutter-day stamp. One row per (server, day) the hourly fold built, or a late row marked for a rebuild. Created empty, and
    /// idempotent: no data moves.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_clutter_day_built
(
    server_id integer NOT NULL,
    day_utc date NOT NULL,
    built_at timestamp NOT NULL,
    rebuild_needed boolean NOT NULL DEFAULT false,
    CONSTRAINT pk_query_store_clutter_day_built PRIMARY KEY (server_id, day_utc)
);";

    /// <summary>Takes the day's stamp before anything is read, so a writer marking the same day waits for this fold or is read by it.
    /// $1 server_id, $2 day, $3 built_at.</summary>
    public const string ClaimDaySql = @"
INSERT INTO collect.query_store_clutter_day_built AS b (server_id, day_utc, built_at, rebuild_needed)
VALUES ($1, $2, $3, false)
ON CONFLICT (server_id, day_utc) DO UPDATE
SET built_at = EXCLUDED.built_at,
    rebuild_needed = false;";

    /// <summary>How many of the day's 24 hours are built and unmarked, read under the claim. $1 server_id, $2 day.</summary>
    public const string CountBuiltHoursSql = @"
SELECT COUNT(*)
FROM collect.query_store_clutter_hour_built AS h
WHERE h.server_id = $1
  AND h.hour_utc >= $2::timestamp
  AND h.hour_utc < $2::timestamp + interval '1 day'
  AND NOT h.rebuild_needed;";

    /// <summary>Fills one (server, day) of the clutter plan table from the day's hours: each plan's earliest first sighting and latest last
    /// sighting. $1 server_id, $2 day.</summary>
    public const string FoldPlanDaySql = @"
INSERT INTO collect.query_store_clutter_plan_daily (server_id, day_utc, database_name, query_id, plan_id, first_seen, last_seen)
SELECT h.server_id, $2, h.database_name, h.query_id, h.plan_id, MIN(h.first_seen), MAX(h.last_seen)
FROM collect.query_store_clutter_plan_hourly AS h
WHERE h.server_id = $1
  AND h.hour_utc >= $2::timestamp
  AND h.hour_utc < $2::timestamp + interval '1 day'
GROUP BY h.server_id, h.database_name, h.query_id, h.plan_id;";

    /// <summary>Fills one (server, day) of the clutter collections table: the sum of the hours' distinct collection counts per database. $1
    /// server_id, $2 day.</summary>
    public const string FoldCollectionsDaySql = @"
INSERT INTO collect.query_store_clutter_collections_daily (server_id, day_utc, database_name, collections)
SELECT h.server_id, $2, h.database_name, SUM(h.collections)
FROM collect.query_store_clutter_collections_hourly AS h
WHERE h.server_id = $1
  AND h.hour_utc >= $2::timestamp
  AND h.hour_utc < $2::timestamp + interval '1 day'
GROUP BY h.server_id, h.database_name;";

    /// <summary>
    /// Marks a day as needing a rebuild. $1 server_id, $2 day, $3 now. A day with no stamp gets one already marked, so a fold of that day running
    /// at the same moment cannot record it without this row: the insert waits on the fold's own row and then marks it.
    /// </summary>
    public const string MarkLateDaySql = @"
INSERT INTO collect.query_store_clutter_day_built AS b (server_id, day_utc, built_at, rebuild_needed)
VALUES ($1, $2, $3, true)
ON CONFLICT (server_id, day_utc) DO UPDATE
SET rebuild_needed = true
WHERE NOT b.rebuild_needed;";

    /// <summary>
    /// The days due now: one row per (server, day) whose 24 hours are all built and unmarked, that has ended and <see cref="QueryStoreClutterHourly.Grace"/>
    /// has passed, and that has no stamp, a marked one, or one older than the newest of its hours (an older build rebuilt an hour and does not mark
    /// the day), newest day first. A day the grid already built from raw with its clutter rows current is left to that stamp (a day with a marked
    /// stamp is folded again either way). $1 now, $2 the day rollups' retention in days, $3 the grace, $4 the cap.
    /// </summary>
    public static readonly string PlanSql = $@"
WITH full_days AS
(
    SELECT h.server_id, h.hour_utc::date AS day_utc, MAX(h.built_at) AS newest_hour_built_at
    FROM collect.query_store_clutter_hour_built AS h
    WHERE NOT h.rebuild_needed
      AND h.hour_utc >= ({QueryStoreClosedDayRollup.FirstWholeDaySql("$1", "$2")})::timestamp
      AND h.hour_utc < (($1::timestamp - $3::interval)::date)::timestamp
    GROUP BY h.server_id, h.hour_utc::date
    HAVING COUNT(*) = 24
)
SELECT f.server_id, f.day_utc
FROM full_days AS f
JOIN collect.servers AS s ON s.server_id = f.server_id AND s.is_enabled
LEFT JOIN collect.query_store_clutter_day_built AS c ON c.server_id = f.server_id AND c.day_utc = f.day_utc
LEFT JOIN collect.query_store_closed_day_built AS b ON b.server_id = f.server_id AND b.day_utc = f.day_utc
WHERE c.rebuild_needed
   OR c.built_at < f.newest_hour_built_at
   OR (c.server_id IS NULL
       AND NOT coalesce(NOT b.rebuild_needed AND NOT b.removed AND b.clutter_built_at = b.built_at, false))
ORDER BY f.day_utc DESC, f.server_id
LIMIT $4;";

    /// <summary>Removes the clutter rows and stamps of the days before the first whole day inside retention that this fold stamped. Read per
    /// stamped (server, day), so each delete is an index lookup on the clutter tables. $1 now, $2 the day rollups' retention in days.</summary>
    public static readonly string[] PruneSql =
    [
        $@"DELETE FROM collect.query_store_clutter_plan_daily AS d
USING collect.query_store_clutter_day_built AS b
WHERE b.day_utc < ({QueryStoreClosedDayRollup.FirstWholeDaySql("$1", "$2")})
  AND d.server_id = b.server_id AND d.day_utc = b.day_utc;",
        $@"DELETE FROM collect.query_store_clutter_collections_daily AS d
USING collect.query_store_clutter_day_built AS b
WHERE b.day_utc < ({QueryStoreClosedDayRollup.FirstWholeDaySql("$1", "$2")})
  AND d.server_id = b.server_id AND d.day_utc = b.day_utc;",
        $@"DELETE FROM collect.query_store_clutter_day_built AS b
WHERE b.day_utc < ({QueryStoreClosedDayRollup.FirstWholeDaySql("$1", "$2")});",
    ];

    /// <summary>One planned fold.</summary>
    public readonly record struct Fold(int ServerId, DateOnly Day);

    /// <summary>What the clutter-day step of one tick did.</summary>
    public readonly record struct TickResult(int Built, int Failed, long Removed, int Deferred);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>
    /// Marks the clutter day of a raw batch's <c>collection_time</c> for a rebuild when its hour is closed (<see cref="QueryStoreClutterHourly.IsClosed"/>),
    /// on the batch's connection and in its transaction, behind the caller's savepoint. The caller has found the stamp table. Returns whether it marked.
    /// </summary>
    public static async Task<bool> TryMarkLateDayAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime collectionTime, DateTime nowUtc, int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        if (!QueryStoreClutterHourly.IsClosed(QueryStoreClutterHourly.FloorHour(collectionTime), nowUtc))
        {
            return false;
        }

        await using var command = new NpgsqlCommand(MarkLateDaySql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = DateOnly.FromDateTime(collectionTime) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        await command.ExecuteNonQueryAsync(cancellationToken);
        return true;
    }

    /// <summary>The folds due at <paramref name="nowUtc"/>, newest day first.</summary>
    public static async Task<IReadOnlyList<Fold>> PlanAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var plan = new List<Fold>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryStoreClutterHourly.Grace });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxFoldsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            plan.Add(new Fold(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
        }

        return plan;
    }

    /// <summary>Removes the days before the first whole day inside retention that this fold stamped, in one transaction; returns the rollup rows
    /// and stamps removed.</summary>
    public static async Task<long> PruneAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        long removed = 0;
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        foreach (var sql in PruneSql)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
            removed += await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return removed;
    }

    /// <summary>
    /// Folds one (server, day) in one transaction: the stamp is taken first, the day's 24 hours are counted again under it, then the day is cleared
    /// and filled from the hours. Returns false, with nothing changed, when an hour was marked or removed since the plan.
    /// </summary>
    public static async Task<bool> FoldDayAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await ExecuteAsync(ClaimDaySql, connection, transaction, serverId, day, nowUtc, cancellationToken);

        await using (var count = new NpgsqlCommand(CountBuiltHoursSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            count.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            count.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            if (Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken)) != 24)
            {
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        foreach (var sql in new[]
        {
            QueryStoreClosedDayRollup.DeleteClutterPlanDaySql, QueryStoreClosedDayRollup.DeleteClutterCollectionsDaySql, FoldPlanDaySql, FoldCollectionsDaySql,
        })
        {
            await ExecuteAsync(sql, connection, transaction, serverId, day, null, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    private static async Task ExecuteAsync(
        string sql, NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateOnly day, DateTime? builtAt, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
        if (builtAt is { } at)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(at) });
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// The clutter-day step of one tick, after the hourly step has built and rebuilt the hours: the prune, then the planned folds, newest day first,
    /// each in its own transaction. A fold that fails is logged and the loop goes on, and the next tick plans it again. No fold starts once
    /// <see cref="QueryStoreClosedDayRollup.MaxTickDuration"/> has passed on <paramref name="elapsed"/>.
    /// </summary>
    public static async Task<TickResult> RunAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        var removed = await PruneAsync(connection, nowUtc, retentionDays, cancellationToken);
        var plan = await PlanAsync(connection, nowUtc, retentionDays, cancellationToken);

        int built = 0, failed = 0, started = 0;
        foreach (var item in plan)
        {
            if (elapsed() >= QueryStoreClosedDayRollup.MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                if (await FoldDayAsync(connection, item.ServerId, item.Day, nowUtc, cancellationToken))
                {
                    built++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Query Store clutter days from hours: folding server {ServerId} day {Day:yyyy-MM-dd} failed and is retried next tick: {Message}",
                    item.ServerId, item.Day, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        logger.LogInformation("Query Store clutter days from hours: built {Built}, failed {Failed}, deferred {Deferred}, removed {Removed} row(s)", built, failed, deferred, removed);
        return new TickResult(built, failed, removed, deferred);
    }
}
