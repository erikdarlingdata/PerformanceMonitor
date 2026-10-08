/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The builder of <see cref="PgIoStatsHourly"/> (#5495): the hourly store-maintenance tick's eighth tenant. It follows
/// V168's <see cref="PlanRegressionDaily"/> builder: one transaction per (server, hour) under a per-server advisory lock, a
/// <c>statement_timeout</c> on the build, a per-tick bound on wall-clock time (<see cref="TickBudget"/>; that bound is what keeps the
/// first fill after the upgrade from holding the tick), and a failed build that is logged and retried by the next tick while readers
/// stay on raw.
///
/// <para><b>Order and watermark.</b> Hours are built oldest first from <c>first_hour</c>, each seeded from the newest earlier
/// rollup row, and <c>built_through</c> (exclusive) moves to the end of each hour in the same transaction as its rows. An hour is
/// closed once <see cref="CloseMarginMinutes"/> have passed its end. The first fill starts at the first whole hour after the server's
/// oldest raw row, no earlier than <see cref="FillDays"/> back. Every tick also rebuilds the last <see cref="RebuildHours"/> built
/// hours, so a late row inside that span is picked up. A later one leaves that hour's raw and rollup counts different, and the count
/// guard sends a read back to the raw statement when the window spans that hour or starts in the hour after it (that hour's last row
/// seeds the first rollup hour's boundary difference); a window that ends before the hour or starts after the next one does not need
/// it. Until the hour ages out of retention the rollup row is never rebuilt, unlike V168's, which rebuilds on a late-row trigger.</para>
///
/// <para><b>First fill cost and its bound.</b> One hour costs 8 to 13 ms on the large store (90 combinations, one-minute cadence, about
/// 5,400 raw rows), so a 31-day fill is 744 builds, about 6 to 10 s per server. Two bounds apply to a tick, both by time and neither by a
/// count of builds: <see cref="TickBudget"/> (2 minutes: no new build starts after it, and the next tick continues from the watermark)
/// and <see cref="MaxTickDuration"/> (10 minutes: the hard cap, which the budget can never exceed). A build that has started always
/// finishes or times out (<see cref="BuildStatementTimeoutSeconds"/>). Nothing runs in the migration: V170 only creates empty tables.</para>
/// </summary>
public static class PgIoStatsHourlyBuilder
{
    /// <summary>How far back the first fill reaches: raw's 30 day default retention plus a day.</summary>
    public const int FillDays = 31;

    /// <summary>An hour is closed this long after its end, so the collector's last cycle of the hour has committed.</summary>
    public const int CloseMarginMinutes = 5;

    /// <summary>The built hours before the watermark that every tick rebuilds, to catch late rows.</summary>
    public const int RebuildHours = 2;

    /// <summary>
    /// The time bound of one tick: no new build starts once a tick has run this long, whatever is left (the next tick continues from the
    /// watermark). A fixed count of builds would have stretched the first fill over days on a large store, where a build takes
    /// milliseconds; a time budget lets a fast store finish its fill in one tick and a slow one stop early.
    /// </summary>
    public static readonly TimeSpan TickBudget = TimeSpan.FromMinutes(2);

    /// <summary>The hard cap: no new build starts after this, even if <see cref="TickBudget"/> is raised past it.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(10);

    private static readonly TimeSpan StartCutoff = TickBudget < MaxTickDuration ? TickBudget : MaxTickDuration;

    /// <summary>The server-side statement_timeout of each hour's build transaction.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>The enabled servers with raw rows: `$1` the closed-hour ceiling. Returns server_id, the oldest raw time, the state (first_hour, built_through; NULL when none).</summary>
    public const string ServersSql = """
SELECT sv.server_id, (SELECT min(t.collection_time) FROM collect.pg_io_stats AS t WHERE t.server_id = sv.server_id),
       st.first_hour, st.built_through
FROM collect.servers AS sv
LEFT JOIN collect.pg_io_stats_hourly_state AS st ON st.server_id = sv.server_id
WHERE sv.is_enabled
ORDER BY sv.server_id;
""";

    /// <summary>One builder per server at a time: `$1` server_id. False means another builder holds the lock.</summary>
    public const string LockSql = "SELECT pg_try_advisory_xact_lock(hashtextextended('pg_io_stats_hourly:' || $1::integer::text, 0));";

    public const string DeleteHourSql = "DELETE FROM collect.pg_io_stats_hourly WHERE server_id = $1::integer AND hour_start = $2::timestamp;";

    /// <summary>Moves the watermark: `$1` server_id, `$2` first_hour (kept when the row exists), `$3` built_through.</summary>
    public const string MarkBuiltSql = """
INSERT INTO collect.pg_io_stats_hourly_state AS s (server_id, first_hour, built_through)
VALUES ($1::integer, $2::timestamp, $3::timestamp)
ON CONFLICT (server_id) DO UPDATE SET built_through = EXCLUDED.built_through;
""";

    /// <summary>
    /// Removes the rollup and state rows of servers that are not enabled. Driven from the small state table (one row per server),
    /// as V168's cleanup is driven from its built table: the rollup rows go by <c>server_id</c>, the leading column of the unique
    /// index, instead of a scan of the whole rollup every hourly tick. The state row is written in the same transaction as a
    /// server's first rollup row, so no rollup row exists without one.
    /// </summary>
    public const string GcSql = """
DELETE FROM collect.pg_io_stats_hourly WHERE server_id IN (SELECT st.server_id FROM collect.pg_io_stats_hourly_state AS st WHERE st.server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled));
DELETE FROM collect.pg_io_stats_hourly_state WHERE server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled);
""";

    /// <summary>The retention prune (`$1` the cutoff, naive UTC): rollup rows older than the raw table's retention.</summary>
    public const string PruneSql = "DELETE FROM collect.pg_io_stats_hourly WHERE hour_start < date_trunc('hour', $1::timestamp);";

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int Built, int Failed, int Skipped, int Deferred);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static DateTime FloorHour(DateTime value) => new(value.Year, value.Month, value.Day, value.Hour, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// Builds one (server, hour) in its own transaction and moves the watermark to the hour's end (the new watermark never moves
    /// back past an hour already built: a rebuild of an earlier hour keeps the later watermark). Returns the rows inserted, or null when the lock was not free.
    /// </summary>
    public static async Task<long?> BuildHourAsync(
        NpgsqlConnection connection, int serverId, DateTime hourStart, DateTime firstHour, DateTime builtThrough, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var @lock = new NpgsqlCommand(LockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            @lock.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            if (!(bool)(await @lock.ExecuteScalarAsync(cancellationToken))!)
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        await using (var delete = new NpgsqlCommand(DeleteHourSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(hourStart) });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        long inserted;
        await using (var build = new NpgsqlCommand(PgIoStatsHourly.BuildHourSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            build.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            build.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(hourStart) });
            inserted = Convert.ToInt64(await build.ExecuteScalarAsync(cancellationToken));
        }

        var through = hourStart.AddHours(1) > builtThrough ? hourStart.AddHours(1) : builtThrough;
        await using (var mark = new NpgsqlCommand(MarkBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(firstHour) });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(through) });
            await mark.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return inserted;
    }

    /// <summary>One tick: drop disabled servers' rows, then for each enabled server build the closed hours from the watermark (less <see cref="RebuildHours"/>) in order.</summary>
    public static Task<TickResult> RunTickAsync(NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/> with the elapsed time supplied (a test seam).</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, failed = 0, skipped = 0, deferred = 0;
        var ceiling = FloorHour(nowUtc.AddMinutes(-CloseMarginMinutes));

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using (var gc = new NpgsqlCommand(GcSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            await gc.ExecuteNonQueryAsync(cancellationToken);
        }

        var servers = new List<(int ServerId, DateTime? OldestRaw, DateTime? FirstHour, DateTime? BuiltThrough)>();
        await using (var list = new NpgsqlCommand(ServersSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await list.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add((reader.GetInt32(0),
                    reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
            }
        }

        foreach (var (serverId, oldestRaw, firstHourState, builtThroughState) in servers)
        {
            if (oldestRaw is null)
            {
                continue;
            }

            DateTime firstHour, from;
            var through = builtThroughState ?? DateTime.MinValue;
            if (firstHourState is null || builtThroughState is null)
            {
                /* First fill: the first whole hour after the oldest raw row, no earlier than FillDays back. */
                var floor = FloorHour(nowUtc.AddDays(-FillDays));
                var start = FloorHour(oldestRaw.Value).AddHours(1);
                firstHour = start > floor ? start : floor.AddHours(1);
                from = firstHour;
            }
            else
            {
                firstHour = firstHourState.Value;
                from = builtThroughState.Value.AddHours(-RebuildHours);
                if (from < firstHour)
                {
                    from = firstHour;
                }
            }

            for (var hour = from; hour < ceiling; hour = hour.AddHours(1))
            {
                if (elapsed() >= StartCutoff)
                {
                    deferred++;
                    continue;
                }

                try
                {
                    if (await BuildHourAsync(connection, serverId, hour, firstHour, through, cancellationToken) is null)
                    {
                        skipped++;
                        break;
                    }

                    built++;
                    var end = hour.AddHours(1);
                    through = end > through ? end : through;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failed++;
                    logger.LogWarning(ex, "PostgreSQL I/O hourly rollup: building server {ServerId} hour {Hour:yyyy-MM-dd HH:mm} failed and is retried next tick (long reads stay on raw rows): {Message}",
                        serverId, hour, ex.Message);
                    break;
                }
            }
        }

        logger.LogInformation("PostgreSQL I/O hourly rollup: built {Built} hour(s), failed {Failed}, skipped {Skipped}, deferred {Deferred}", built, failed, skipped, deferred);
        return new TickResult(built, failed, skipped, deferred);
    }
}
