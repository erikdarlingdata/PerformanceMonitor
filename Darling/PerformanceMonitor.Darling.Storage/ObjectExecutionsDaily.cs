/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-day execution count of each procedure, function and trigger Query Store saw (V268, #8784): one row per (server, day, database,
/// module) with the day's executions and its newest execution time, so <c>get_object_usage</c> can say whether a module ran at all across a
/// year without keeping a year of Query Store rows.
///
/// <para><b>Why a table of its own.</b> The raw Query Store rows, the wide table and its daily summary (<c>query_store_top_daily</c>) are all
/// kept as long as <c>query_store</c>'s retention, 30 days by default, and raising that keeps the plan text and every interval for as long. A
/// decommissioning review needs a year, so this table keeps only what that question reads, about one row per module per day, for as long as
/// <c>index_object_stats</c> is kept (<see cref="RetentionDaysSql"/>): one retention setting then covers the table and the module evidence
/// together.</para>
///
/// <para><b>Where a day comes from.</b> A day is the sum of <c>query_store_top_daily</c>'s rows for that (server, day), grouped by database and
/// module, with <c>Adhoc</c>, <c>Unknown</c> and NULL module names left out (they are not objects). The summary has already kept one row per
/// Query Store interval, so a re-collected open interval is counted once. It is approximate in the way that summary is (a day is built after it
/// ends, and rebuilt once more a day later): a reader that uses this table says so.</para>
///
/// <para><b>Which days a database was looked at.</b> A module that never ran has no row here, so "no row" has to be told apart from "Query
/// Store held nothing for this database that day". <see cref="DatabaseDaysTable"/> holds one row per (server, database, day) on which the
/// summary had ANY row for the database, a statement outside a module included. Query Store is per database, so this is per database too: a
/// database with Query Store off has no rows there while its neighbours on the same server do.</para>
///
/// <para><b>How it is kept.</b> <see cref="RunTickAsync"/> runs after <c>QueryStoreTopDaily.RunTickAsync</c> on the same housekeeping tick. It
/// copies every (server, day) whose summary was built since this table last copied it (<c>query_store_top_daily_built.built_at</c> newer
/// than <c>source_built_at</c> here), newest first and at most <see cref="MaxBuildsPerTick"/> a tick, each in its own transaction with its
/// built row. The first ticks after the upgrade copy the summary's existing days, so the table starts with the month of history the store
/// already holds rather than empty. Days older than the retention edge are deleted on the same tick. The summary's own cleanup never touches
/// this table.</para>
///
/// <para><b>A shorter retention waits before it deletes.</b> The table follows <c>index_object_stats</c>'s retention setting, and what it
/// holds beyond the Query Store summary's own 30 days cannot be built again. Deleting to the setting as read on each tick would let a
/// setting lowered for an hour (to reclaim space, or by mistake) take a year of evidence with it for good. So a longer retention applies at
/// once, and a shorter one only after it has stood unchanged for <see cref="RetentionLoweringGraceDays"/> days
/// (<see cref="NextRetention"/>, kept in <see cref="RetentionTable"/>): put the setting back inside that time and nothing is lost.</para>
///
/// <para><b>A copy only ever adds.</b> This table outlives its source by design, so the source can be rebuilt smaller than what was already
/// copied: a day near the summary's own horizon is rebuilt from a partly purged wide table, and a re-enabled server has its days built
/// again. A copy therefore never deletes: it inserts the modules it finds and, for one already here, keeps the larger count and the newer
/// execution time. A module the summary no longer holds keeps the row it had. The other direction (a count that should have gone down) does
/// not happen to a day that has ended, and would only make a module look more used, which is the safe way to be wrong here.</para>
/// </summary>
public static class ObjectExecutionsDaily
{
    /// <summary>The summary table.</summary>
    public const string Table = "collect.object_executions_daily";

    /// <summary>The per-(server, day) record of which summary build each day was copied from.</summary>
    public const string BuiltTable = "collect.object_executions_daily_built";

    /// <summary>The (server, database, day) rows on which the Query Store summary held anything for the database.</summary>
    public const string DatabaseDaysTable = "collect.object_executions_daily_databases";

    /// <summary>The one-row record of the retention the table's cleanup is applying, and of a shorter one waiting to apply.</summary>
    public const string RetentionTable = "collect.object_executions_daily_retention";

    /// <summary>How many days a shorter retention has to stand before the cleanup deletes to it. Long enough to notice and undo a setting
    /// lowered by mistake or for a short while; short enough that a real change takes effect within the week.</summary>
    public const int RetentionLoweringGraceDays = 7;

    /// <summary>The most (server, day) copies one tick runs. A copy reads one day of one server's summary through its unique index.</summary>
    public const int MaxBuildsPerTick = 120;

    /// <summary>Command timeout of each statement.</summary>
    public const int CommandTimeoutSeconds = 120;

    /// <summary>
    /// The four tables as the V268 rung runs them. Idempotent: a re-run keeps the rows. The rung embeds this text, so editing it after it ships
    /// edits a shipped rung: add a new rung instead. Plain tables on every store. The primary key serves the copy's upsert and the read's range
    /// per server and database; <c>idx_object_executions_daily_day</c> serves the retention delete. The database-days table is a row per
    /// database per day, small enough that its retention delete needs no index of its own. Needs no GRANT: the <c>collect</c> schema's blanket
    /// SELECT covers a table a migration introduces.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.object_executions_daily
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    module_name text NOT NULL,
    day date NOT NULL,
    execution_count numeric NOT NULL,
    last_execution_time timestamp,
    CONSTRAINT pk_object_executions_daily PRIMARY KEY (server_id, database_name, module_name, day)
);

CREATE INDEX IF NOT EXISTS idx_object_executions_daily_day ON collect.object_executions_daily (day);

CREATE TABLE IF NOT EXISTS collect.object_executions_daily_built
(
    server_id integer NOT NULL,
    day date NOT NULL,
    source_built_at timestamp NOT NULL,
    built_at timestamp NOT NULL,
    CONSTRAINT pk_object_executions_daily_built PRIMARY KEY (server_id, day)
);

CREATE TABLE IF NOT EXISTS collect.object_executions_daily_databases
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    day date NOT NULL,
    execution_count numeric NOT NULL,
    CONSTRAINT pk_object_executions_daily_databases PRIMARY KEY (server_id, database_name, day)
);

CREATE TABLE IF NOT EXISTS collect.object_executions_daily_retention
(
    only_row boolean NOT NULL DEFAULT true,
    applied_days integer NOT NULL,
    pending_days integer,
    pending_since timestamp,
    CONSTRAINT pk_object_executions_daily_retention PRIMARY KEY (only_row),
    CONSTRAINT ck_object_executions_daily_retention_only_row CHECK (only_row)
);";

    /// <summary>
    /// How many days the table keeps: the longest retention <c>index_object_stats</c> has anywhere (the fleet value, or any per-server
    /// override that is longer), so a server whose object stats are kept for 400 days keeps 400 days of module evidence too. Keeping the
    /// longest for every server costs a few rows per module per day, and keeps the table's own cleanup one statement.
    /// </summary>
    internal static readonly string RetentionDaysSql =
        $"SELECT GREATEST({DarlingRetentionHorizons.FleetRetentionDaysSql("index_object_stats")}, COALESCE((SELECT max(LEAST(o.retention_days, {CollectorScheduleDefaults.MaxRetentionDays.ToString(CultureInfo.InvariantCulture)})) FROM config.config_collector_schedules AS o WHERE lower(o.collector_name) = 'index_object_stats' AND o.retention_days >= 1), 0))";

    /// <summary>The retention the cleanup is applying and any shorter one waiting. No row before the first tick.</summary>
    internal const string ReadRetentionStateSql = @"
SELECT applied_days, pending_days, pending_since
FROM collect.object_executions_daily_retention;";

    /// <summary>Stores the retention state. $1 applied_days, $2 pending_days, $3 pending_since.</summary>
    internal const string UpsertRetentionStateSql = @"
INSERT INTO collect.object_executions_daily_retention (only_row, applied_days, pending_days, pending_since)
VALUES (true, $1, $2, $3)
ON CONFLICT (only_row) DO UPDATE
SET applied_days = EXCLUDED.applied_days,
    pending_days = EXCLUDED.pending_days,
    pending_since = EXCLUDED.pending_since;";

    /// <summary>
    /// The (server, day) copies due, newest first: every built summary day that this table has not copied, or copied from an older build.
    /// $1 the retention edge (a date: older days are not copied, as the same tick deletes them), $2 the cap.
    /// </summary>
    internal const string PlanSql = @"
SELECT b.server_id, b.day, b.built_at
FROM collect.query_store_top_daily_built AS b
LEFT JOIN collect.object_executions_daily_built AS o
  ON  o.server_id = b.server_id
  AND o.day = b.day
WHERE b.day >= $1
AND   (o.server_id IS NULL OR o.source_built_at < b.built_at)
ORDER BY
    b.day DESC,
    b.server_id
LIMIT $2;";

    /// <summary>
    /// Copies one (server, day) from the daily Query Store summary, one row per database and module. $1 server_id, $2 day. The summary's
    /// <c>module_name</c> is <c>schema.name</c> for a module, <c>Adhoc</c> for a statement outside one and <c>Unknown</c> where the module was
    /// dropped before collection; only the first names an object. A module already copied keeps the larger count and the newer execution
    /// time (PostgreSQL's GREATEST skips a NULL), and nothing is deleted, so a rebuild from a purged source cannot take evidence away.
    /// Returns the rows written.
    /// </summary>
    internal const string CopyDaySql = @"
INSERT INTO collect.object_executions_daily (server_id, database_name, module_name, day, execution_count, last_execution_time)
SELECT
    t.server_id,
    t.database_name,
    t.module_name,
    t.day,
    COALESCE(sum(t.execution_count_sum), 0),
    max(t.last_execution_time_max)
FROM collect.query_store_top_daily AS t
WHERE t.server_id = $1
AND   t.day = $2
AND   t.database_name IS NOT NULL
AND   t.module_name IS NOT NULL
AND   t.module_name NOT IN ('Adhoc', 'Unknown')
GROUP BY
    t.server_id,
    t.database_name,
    t.module_name,
    t.day
ON CONFLICT (server_id, database_name, module_name, day) DO UPDATE
SET execution_count = GREATEST(collect.object_executions_daily.execution_count, EXCLUDED.execution_count),
    last_execution_time = GREATEST(collect.object_executions_daily.last_execution_time, EXCLUDED.last_execution_time);";

    /// <summary>
    /// Records, for one (server, day), each database the summary held anything for, with the day's executions of every statement (a
    /// statement outside a module included). $1 server_id, $2 day. Kept the same way as <see cref="CopyDaySql"/>: the larger count stays and
    /// nothing is deleted.
    /// </summary>
    internal const string CopyDatabaseDaysSql = @"
INSERT INTO collect.object_executions_daily_databases (server_id, database_name, day, execution_count)
SELECT
    t.server_id,
    t.database_name,
    t.day,
    COALESCE(sum(t.execution_count_sum), 0)
FROM collect.query_store_top_daily AS t
WHERE t.server_id = $1
AND   t.day = $2
AND   t.database_name IS NOT NULL
GROUP BY
    t.server_id,
    t.database_name,
    t.day
ON CONFLICT (server_id, database_name, day) DO UPDATE
SET execution_count = GREATEST(collect.object_executions_daily_databases.execution_count, EXCLUDED.execution_count);";

    /// <summary>Records the copy. $1 server_id, $2 day, $3 the summary build it was copied from, $4 now.</summary>
    internal const string UpsertBuiltSql = @"
INSERT INTO collect.object_executions_daily_built (server_id, day, source_built_at, built_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, day) DO UPDATE
SET source_built_at = EXCLUDED.source_built_at,
    built_at = EXCLUDED.built_at;";

    /// <summary>The retention delete of the summary: days before $1 (a date), through <c>idx_object_executions_daily_day</c>. One statement per
    /// command: a positional parameter is not parsed client-side, and PostgreSQL refuses two statements in one prepared command.</summary>
    internal const string RetentionDeleteSql = @"
DELETE FROM collect.object_executions_daily
WHERE day < $1;";

    /// <summary>The retention delete of the build record: days before $1 (a date).</summary>
    internal const string RetentionDeleteBuiltSql = @"
DELETE FROM collect.object_executions_daily_built
WHERE day < $1;";

    /// <summary>The retention delete of the database days: days before $1 (a date).</summary>
    internal const string RetentionDeleteDatabaseDaysSql = @"
DELETE FROM collect.object_executions_daily_databases
WHERE day < $1;";

    /// <summary>One planned copy.</summary>
    public readonly record struct Copy(int ServerId, DateOnly Day, DateTime SourceBuiltAt);

    /// <summary>The retention the cleanup applies (<paramref name="AppliedDays"/>) and a shorter configured one that is waiting out the
    /// grace period, with when it was first seen.</summary>
    public readonly record struct RetentionState(int AppliedDays, int? PendingDays, DateTime? PendingSince);

    /// <summary>
    /// The retention state after a tick that read <paramref name="configuredDays"/> from the settings. A value at or above the one applied
    /// applies at once and clears anything waiting. A lower one starts waiting, starts again when it changes, and applies once it has stood
    /// for <see cref="RetentionLoweringGraceDays"/> days. With nothing stored (the first tick) the configured value applies as it is: there
    /// is no longer retention on record to protect.
    /// </summary>
    internal static RetentionState NextRetention(RetentionState? stored, int configuredDays, DateTime nowUtc)
    {
        if (stored is not { } state || configuredDays >= state.AppliedDays)
        {
            return new RetentionState(configuredDays, null, null);
        }

        if (state.PendingDays != configuredDays || state.PendingSince is not { } since)
        {
            return new RetentionState(state.AppliedDays, configuredDays, nowUtc);
        }

        return nowUtc - since >= TimeSpan.FromDays(RetentionLoweringGraceDays)
            ? new RetentionState(configuredDays, null, null)
            : state;
    }

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int Copied, int Failed, int Deferred);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The retention edge for <paramref name="nowUtc"/>: the first day kept.</summary>
    internal static DateOnly RetentionEdge(DateTime nowUtc, int retentionDays) =>
        DateOnly.FromDateTime(nowUtc.Date.AddDays(-retentionDays));

    /// <summary>
    /// One tick: read the retention (a shorter one only once it has stood for the grace period), delete the expired days, plan, then copy
    /// each planned day in its own transaction. A copy that fails is
    /// logged (naming the server and day) and the loop goes on; the next tick plans it again.
    /// </summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);

        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.ObjectExecutionsDaily, cancellationToken);

        int configuredDays;
        await using (var retention = new NpgsqlCommand(RetentionDaysSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            configuredDays = Convert.ToInt32(await retention.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        var retentionState = await ApplyRetentionAsync(connection, configuredDays, nowUtc, logger, cancellationToken);
        var edge = RetentionEdge(nowUtc, retentionState.AppliedDays);
        foreach (var sql in new[] { RetentionDeleteSql, RetentionDeleteBuiltSql, RetentionDeleteDatabaseDaysSql })
        {
            await using var delete = new NpgsqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = edge });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        var plan = new List<Copy>();
        await using (var planCommand = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            planCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = edge });
            planCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick + 1 });
            await using var reader = await planCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                plan.Add(new Copy(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1), reader.GetDateTime(2)));
            }
        }

        var deferred = plan.Count > MaxBuildsPerTick ? 1 : 0;
        int copied = 0, failed = 0;
        for (var i = 0; i < plan.Count && i < MaxBuildsPerTick; i++)
        {
            var item = plan[i];
            try
            {
                await CopyDayAsync(connection, item, nowUtc, cancellationToken);
                copied++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(
                    "Object executions summary: copying server {ServerId} day {Day:yyyy-MM-dd} failed (SqlState {SqlState}); the next tick retries: {Message}",
                    item.ServerId, item.Day, (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
            }
        }

        return new TickResult(copied, failed, deferred);
    }

    /// <summary>Reads the stored retention state, moves it on for this tick's configured value and stores it when it changed. Returns the
    /// state the tick's cleanup applies.</summary>
    internal static async Task<RetentionState> ApplyRetentionAsync(
        NpgsqlConnection connection, int configuredDays, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);

        RetentionState? stored = null;
        await using (var read = new NpgsqlCommand(ReadRetentionStateSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                stored = new RetentionState(
                    reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetDateTime(2));
            }
        }

        var next = NextRetention(stored, configuredDays, nowUtc);
        if (stored == next)
        {
            return next;
        }

        await using (var write = new NpgsqlCommand(UpsertRetentionStateSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            write.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = next.AppliedDays });
            write.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)next.PendingDays ?? DBNull.Value });
            write.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Timestamp,
                Value = next.PendingSince is { } since ? Unspecified(since) : DBNull.Value,
            });
            await write.ExecuteNonQueryAsync(cancellationToken);
        }

        if (next.PendingDays is { } waiting && stored?.PendingDays != waiting)
        {
            logger.LogInformation(
                "Object executions summary: index_object_stats retention is now {Configured} days, shorter than the {Applied} days this summary keeps. "
                + "Nothing is deleted yet: the shorter retention applies after it has stood for {Grace} days, because the days beyond the Query Store "
                + "summary cannot be rebuilt. Set the retention back before then to keep them.",
                waiting, next.AppliedDays, RetentionLoweringGraceDays);
        }
        else if (stored is { PendingDays: not null } && next.PendingDays is null && next.AppliedDays < stored.Value.AppliedDays)
        {
            logger.LogInformation(
                "Object executions summary: the shorter retention of {Applied} days has stood for {Grace} days and now applies.",
                next.AppliedDays, RetentionLoweringGraceDays);
        }

        return next;
    }

    /// <summary>Copies one (server, day) in one transaction: upsert its modules and its database days from the summary, then record the build
    /// they came from. Returns the module rows written.</summary>
    internal static async Task<int> CopyDayAsync(NpgsqlConnection connection, Copy item, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        int written;
        await using (var insert = new NpgsqlCommand(CopyDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            written = await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var databases = new NpgsqlCommand(CopyDatabaseDaysSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            databases.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            databases.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            await databases.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var built = new NpgsqlCommand(UpsertBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(item.SourceBuiltAt) });
            built.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            await built.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return written;
    }
}
