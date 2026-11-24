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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly host-object rollup of <c>collect.query_stats</c> (#9164): one row per (server, UTC hour, database, query hash, host object)
/// carrying every aggregate that the top-queries statement (<c>DarlingDataReader.TopQueriesSql</c>) builds in its two passes, so a Past
/// day read can rank whole hours from this table and read raw only for the edge hours. This class is the table, the job that keeps it and
/// the coverage check a read asks first. No read uses it yet.
///
/// <para><b>What a row holds.</b> The statement's own aggregates over the same rows under the same filters. Its ranking pass and its
/// wide-row pass both read the interval-honest rows (<c>sample_interval_seconds IS DISTINCT FROM 0</c>), so the sums, the minimums and
/// maximums, the handles and <c>raw_rows</c> are taken over those rows only. The columns that are observations, not measures, are taken
/// over every row of the hour: <c>last_execution_time</c>, the per-second CPU peak (a zero interval gives NULL there anyway) and
/// <c>latest_text_key</c>, the byte key the statement builds for its newest text-bearing row. One <c>max_dop</c> column serves the
/// parallelism floor of the ranking pass and the figure of the wide row, because both read the same interval-honest rows. An hour whose
/// rows for a key are all zero-interval keeps its row, with NULL sums, because those rows can hold the newest text. <c>digests</c> is the
/// hour's distinct text digests, so a read counts the distinct texts of a range exactly by taking the distinct union.</para>
///
/// <para><b>The job.</b> An hour is closed once it has ended and <see cref="Grace"/> has passed. A closed hour is built in one transaction
/// at repeatable-read isolation: its <c>query_stats_host_hourly_built</c> row records <c>raw_rows</c> (the interval-honest raw rows the
/// build counted) and <c>ledger_rows</c> (what <c>query_stats_hour_ledger</c> held for the hour), the hour is cleared and filled again,
/// and the next tick builds it once more (<see cref="SettleAfter"/>). After that an hour is built again only when the ledger no longer
/// equals the <c>ledger_rows</c> of its last build, which is what a late row does, since the collector adds its batch to the ledger in
/// the same transaction as the row. One transaction builds one hour for every server that is due in it, so the scan of the hour is shared
/// instead of repeated for each server. A tick builds the newest hours first, at most <see cref="MaxHoursPerTick"/> of them and not past
/// <see cref="MaxTickDuration"/>, and the next tick takes the rest.</para>
///
/// <para><b>Coverage.</b> <see cref="IncompleteHoursAsync"/> says which hours of a range the table does not hold whole for a server: the
/// hours before the ledger's <c>counted_since</c>, the hours with no build, and the hours whose build counted a different number of raw
/// rows than the ledger holds now. A read that finds none takes those hours from this table, and takes any other hour from raw.</para>
///
/// <para><b>Retention.</b> Raw's: a tick removes the hours before the first whole hour inside raw's retention, from both tables, and
/// builds none of them.</para>
/// </summary>
public static class QueryStatsHostHourly
{
    /// <summary>The migration that creates the two tables.</summary>
    public const int Version = 287;

    /// <summary>The rollup table.</summary>
    public const string Table = "collect.query_stats_host_hourly";

    /// <summary>The table that says which (server, hour) the rollup holds and what each build counted.</summary>
    public const string BuiltTable = "collect.query_stats_host_hourly_built";

    /// <summary>An hour is closed once it has ended and this has passed: a batch that was collected inside the hour has committed by then.</summary>
    public static readonly TimeSpan Grace = TimeSpan.FromMinutes(10);

    /// <summary>An hour built before its end plus <see cref="Grace"/> plus one tick (an hour) is built once more, whatever the ledger says.</summary>
    public static readonly TimeSpan SettleAfter = Grace + TimeSpan.FromHours(1);

    /// <summary>The most hours one tick plans: a server-hour is cheap, so <see cref="MaxTickDuration"/> is the bound that usually holds.</summary>
    public const int MaxHoursPerTick = 36;

    /// <summary>The wall-clock budget of one tick: no new hour starts once this much time has passed since the tick began.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    /// <summary>The server-side <c>statement_timeout</c> each hour's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>The rows the statement's two passes read for their measures.</summary>
    private const string Honest = "sample_interval_seconds IS DISTINCT FROM 0";

    private static string Sum(string column) => $"CAST(SUM({column}) FILTER (WHERE {Honest}) AS bigint)";

    private static string Min(string column) => $"MIN({column}) FILTER (WHERE {Honest})";

    private static string Max(string column) => $"MAX({column}) FILTER (WHERE {Honest})";

    /// <summary>
    /// The value columns in one order for the table, the build and the readers: name, type, and the aggregate the build computes over the
    /// hour's raw rows of one key. Each is the aggregate the top-queries statement's wide-row pass computes under the same filter.
    /// </summary>
    public static readonly (string Name, string Type, string Aggregate)[] ValueColumns =
    [
        ("raw_rows", "bigint", $"COUNT(*) FILTER (WHERE {Honest})"),
        ("total_executions", "bigint", Sum("delta_execution_count")),
        ("total_cpu_us", "bigint", Sum("delta_worker_time")),
        ("total_elapsed_us", "bigint", Sum("delta_elapsed_time")),
        ("total_reads", "bigint", Sum("delta_logical_reads")),
        ("total_writes", "bigint", Sum("delta_logical_writes")),
        ("total_physical_reads", "bigint", Sum("delta_physical_reads")),
        ("total_rows", "bigint", Sum("delta_rows")),
        ("total_spills", "bigint", Sum("delta_spills")),
        ("min_dop", "bigint", Min("min_dop")),
        ("max_dop", "bigint", Max("max_dop")),
        ("min_worker_time", "bigint", Min("min_worker_time")),
        ("max_worker_time", "bigint", Max("max_worker_time")),
        ("min_elapsed_time", "bigint", Min("min_elapsed_time")),
        ("max_elapsed_time", "bigint", Max("max_elapsed_time")),
        ("query_plan_hash", "text", Max("query_plan_hash")),
        ("sql_handle", "text", Max("sql_handle")),
        ("plan_handle", "text", Max("plan_handle")),
        ("last_execution_time", "timestamp", "MAX(last_execution_time)"),
        ("creation_time", "timestamp", Max("creation_time")),
        ("min_physical_reads", "bigint", Min("min_physical_reads")),
        ("max_physical_reads", "bigint", Max("max_physical_reads")),
        ("min_rows", "bigint", Min("min_rows")),
        ("max_rows", "bigint", Max("max_rows")),
        ("min_grant_kb", "bigint", Min("min_grant_kb")),
        ("max_grant_kb", "bigint", Max("max_grant_kb")),
        ("min_used_grant_kb", "bigint", Min("min_used_grant_kb")),
        ("max_used_grant_kb", "bigint", Max("max_used_grant_kb")),
        ("min_ideal_grant_kb", "bigint", Min("min_ideal_grant_kb")),
        ("max_ideal_grant_kb", "bigint", Max("max_ideal_grant_kb")),
        ("min_spills", "bigint", Min("min_spills")),
        ("max_spills", "bigint", Max("max_spills")),
        ("min_reserved_threads", "bigint", Min("min_reserved_threads")),
        ("max_reserved_threads", "bigint", Max("max_reserved_threads")),
        ("min_used_threads", "bigint", Min("min_used_threads")),
        ("max_used_threads", "bigint", Max("max_used_threads")),
        ("total_clr_time", "bigint", Max("total_clr_time")),
        ("plan_generation_num", "bigint", Max("plan_generation_num")),
        ("worker_time_per_second", "double precision", "MAX(CAST(delta_worker_time AS double precision) / NULLIF(sample_interval_seconds, 0) / 1000.0)"),
        ("digests", "bytea[]", $"array_agg(DISTINCT query_text_digest) FILTER (WHERE {Honest} AND query_text_digest IS NOT NULL)"),
        ("latest_text_key", "bytea",
            @"MAX(timestamp_send(collection_time) || int8send(collection_id)
        || CASE WHEN query_text IS NOT NULL THEN '\x00'::bytea || convert_to(query_text, 'UTF8') ELSE '\x01'::bytea || query_text_digest END)
        FILTER (WHERE query_text IS NOT NULL OR query_text_digest IS NOT NULL)"),
    ];

    private static string Lines(IEnumerable<string> parts) => string.Join(",\n    ", parts);

    /// <summary>V287: the two tables, created empty. The service's tick fills them; no data moves in the migration.</summary>
    public static readonly string CreateSql = $@"
/* #9164: the hourly rollup of collect.query_stats the top-queries read ranks whole hours from. One row per (server, UTC hour, database,
   query hash, host object); NULL key columns are stored as they are. */
CREATE TABLE IF NOT EXISTS collect.query_stats_host_hourly
(
    server_id integer NOT NULL,
    bucket timestamp NOT NULL,
    database_name text,
    query_hash text,
    host_object_name text,
    {Lines(ValueColumns.Select(c => $"{c.Name} {c.Type}"))}
);

/* NULLS NOT DISTINCT: ad-hoc rows carry no host object and some carry no hash or database, and two NULLs are one group. Leads with
   (server_id, bucket), so it also serves a range of hours and the delete of one hour. */
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_stats_host_hourly
ON collect.query_stats_host_hourly (server_id, bucket, database_name, query_hash, host_object_name)
NULLS NOT DISTINCT;

/* The wide row's pass reads the winners' hours by hash. */
CREATE INDEX IF NOT EXISTS idx_query_stats_host_hourly_hash
ON collect.query_stats_host_hourly (server_id, query_hash, bucket);

/* Which (server, hour) the rollup holds. raw_rows: the interval-honest raw rows the build counted. ledger_rows: what
   collect.query_stats_hour_ledger held for the hour in the same snapshot. The rollup is whole for the hour when raw_rows equals the
   ledger now; the tick builds the hour again when the ledger is no longer ledger_rows. */
CREATE TABLE IF NOT EXISTS collect.query_stats_host_hourly_built
(
    server_id integer NOT NULL,
    bucket timestamp NOT NULL,
    built_at timestamp NOT NULL,
    raw_rows bigint NOT NULL,
    ledger_rows bigint NOT NULL,
    CONSTRAINT pk_query_stats_host_hourly_built PRIMARY KEY (server_id, bucket)
);";

    /// <summary>Clears one hour of the rollup for a set of servers. $1 server ids, $2 hour.</summary>
    public const string DeleteHourSql = @"
DELETE FROM collect.query_stats_host_hourly
WHERE server_id = ANY($1::integer[])
  AND bucket = $2::timestamp;";

    /// <summary>
    /// Fills one hour of the rollup for a set of servers from raw, in one scan of the hour. $1 server ids, $2 hour. The rows of the hour
    /// are those whose <c>collection_time</c> is in it, the hour the ledger counts them under.
    /// </summary>
    public static readonly string BuildHourSql = $@"
INSERT INTO collect.query_stats_host_hourly (server_id, bucket, database_name, query_hash, host_object_name, {string.Join(", ", ValueColumns.Select(c => c.Name))})
SELECT
    server_id, $2::timestamp, database_name, query_hash, host_object_name,
    {Lines(ValueColumns.Select(c => c.Aggregate))}
FROM collect.query_stats
WHERE server_id = ANY($1::integer[])
  AND collection_time >= $2::timestamp
  AND collection_time < $2::timestamp + interval '1 hour'
GROUP BY server_id, database_name, query_hash, host_object_name;";

    /// <summary>
    /// Records the build of one hour for a set of servers, in the build's own snapshot: the raw rows it counted and what the ledger held. A
    /// server with no raw rows in the hour gets a row of zero, which is a built hour with nothing in it. $1 server ids, $2 hour, $3 built_at.
    /// </summary>
    public const string RecordBuiltSql = @"
INSERT INTO collect.query_stats_host_hourly_built AS b (server_id, bucket, built_at, raw_rows, ledger_rows)
SELECT
    s.server_id, $2::timestamp, $3::timestamp,
    COALESCE((SELECT SUM(h.raw_rows) FROM collect.query_stats_host_hourly AS h WHERE h.server_id = s.server_id AND h.bucket = $2::timestamp), 0),
    COALESCE((SELECT SUM(l.n) FROM collect.query_stats_hour_ledger AS l WHERE l.server_id = s.server_id AND l.bucket = $2::timestamp), 0)
FROM unnest($1::integer[]) AS s(server_id)
ON CONFLICT (server_id, bucket) DO UPDATE
SET built_at = EXCLUDED.built_at,
    raw_rows = EXCLUDED.raw_rows,
    ledger_rows = EXCLUDED.ledger_rows;";

    /// <summary>
    /// The hours due now, newest first, each with the servers that are due in it. $1 now, $2 raw retention in days, $3 <see cref="Grace"/>,
    /// $4 <see cref="SettleAfter"/>, $5 the cap on hours. An hour runs from the first whole hour inside raw's retention (or the ledger's
    /// <c>counted_since</c>, when that is later) to the newest closed hour. A (server, hour) is due when it has no built row, when the ledger
    /// is no longer what its last build held, or when it was built before <see cref="SettleAfter"/> had passed.
    /// </summary>
    public const string PlanSql = @"
WITH span AS
(
    SELECT
        GREATEST(date_trunc('hour', $1::timestamp - make_interval(days => $2)) + interval '1 hour',
                 (SELECT t.counted_since FROM collect.query_stats_hour_ledger_state AS t WHERE t.id = 1)) AS first_hour,
        date_trunc('hour', $1::timestamp - $3::interval - interval '1 hour') AS last_hour
)
SELECT d.bucket, array_agg(s.server_id ORDER BY s.server_id)
FROM collect.servers AS s
CROSS JOIN span
CROSS JOIN LATERAL generate_series(span.first_hour, span.last_hour, interval '1 hour') AS d(bucket)
LEFT JOIN collect.query_stats_host_hourly_built AS b
  ON b.server_id = s.server_id AND b.bucket = d.bucket
WHERE s.is_enabled
  AND (b.server_id IS NULL
       OR b.built_at < d.bucket + interval '1 hour' + $4::interval
       OR b.ledger_rows <> COALESCE((SELECT SUM(l.n) FROM collect.query_stats_hour_ledger AS l WHERE l.server_id = s.server_id AND l.bucket = d.bucket), 0))
GROUP BY d.bucket
ORDER BY d.bucket DESC
LIMIT $5;";

    /// <summary>Removes the rollup's hours before the first one inside raw's retention. $1 now, $2 raw retention in days.</summary>
    public const string PruneSql = @"
DELETE FROM collect.query_stats_host_hourly
WHERE bucket < date_trunc('hour', $1::timestamp - make_interval(days => $2)) + interval '1 hour';";

    /// <summary>Removes the built rows of the same hours. $1 now, $2 raw retention in days.</summary>
    public const string PruneBuiltSql = @"
DELETE FROM collect.query_stats_host_hourly_built
WHERE bucket < date_trunc('hour', $1::timestamp - make_interval(days => $2)) + interval '1 hour';";

    /// <summary>
    /// The whole hours of <c>[$2, $3)</c> that the rollup does not hold for server $1: before the ledger's <c>counted_since</c>, with no
    /// build, or built from a different number of raw rows than the ledger holds now (a late row). $2 and $3 are whole hours.
    /// </summary>
    public const string IncompleteHoursSql = @"
SELECT h.bucket, b.server_id IS NOT NULL AS built
FROM generate_series($2::timestamp, $3::timestamp - interval '1 hour', interval '1 hour') AS h(bucket)
LEFT JOIN collect.query_stats_host_hourly_built AS b
  ON b.server_id = $1 AND b.bucket = h.bucket
WHERE NOT (
    h.bucket >= COALESCE((SELECT t.counted_since FROM collect.query_stats_hour_ledger_state AS t WHERE t.id = 1), 'infinity'::timestamp)
    AND b.server_id IS NOT NULL
    AND b.raw_rows = COALESCE((SELECT SUM(l.n) FROM collect.query_stats_hour_ledger AS l WHERE l.server_id = $1 AND l.bucket = h.bucket), 0))
ORDER BY h.bucket;";

    /// <summary>The fewest whole hours worth reading from the rollup. A shorter mid saves little.</summary>
    public static readonly TimeSpan MinMid = TimeSpan.FromHours(2);

    /// <summary>
    /// The whole hours <c>[MidStart, MidEnd)</c> of a raw top-queries window <c>(startUtc, endUtc]</c> that the rollup may serve, or null to
    /// read raw alone. The read is (start, end] on <c>collection_time</c>, so an hour bucket <c>[h, h + 1h)</c> is whole only when
    /// <c>h &gt; start</c> and <c>h + 1h &lt;= end</c>. The mid starts at the first hour boundary after <paramref name="startUtc"/> and ends at
    /// the last hour boundary at or before <paramref name="endUtc"/> and before the newest hour the tick may have closed
    /// (<paramref name="nowUtc"/> less <see cref="Grace"/>). The tick builds a closed hour only on its first hourly run after that, so a reader
    /// shrinks the mid again to the last built hour (<see cref="UsableMidEndAsync"/>). A mid shorter than <see cref="MinMid"/> saves little.
    /// </summary>
    public static (DateTime MidStart, DateTime MidEnd)? PlanMid(DateTime startUtc, DateTime endUtc, DateTime nowUtc)
    {
        var midStart = new DateTime(startUtc.Year, startUtc.Month, startUtc.Day, startUtc.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(1);
        var closed = nowUtc - Grace;
        var limit = endUtc < closed ? endUtc : closed;
        var midEnd = new DateTime(limit.Year, limit.Month, limit.Day, limit.Hour, 0, 0, DateTimeKind.Unspecified);
        return midEnd - midStart < MinMid ? null : (midStart, midEnd);
    }

    /// <summary>True when the store has the rollup, its build ledger and the hour ledger the coverage check reads (a store below V287 has no rollup).</summary>
    public const string TablesExistSql =
        "SELECT to_regclass('collect.query_stats_host_hourly') IS NOT NULL AND to_regclass('collect.query_stats_host_hourly_built') IS NOT NULL " +
        "AND to_regclass('collect.query_stats_hour_ledger') IS NOT NULL AND to_regclass('collect.query_stats_hour_ledger_state') IS NOT NULL";

    /// <summary>
    /// Where the rollup's mid ends for a read: null when the store has no rollup, when an hour of <c>[midStart, midEnd)</c> is not whole
    /// anywhere but at the end, or when the end that is left is under <see cref="MinMid"/> (<see cref="BuiltThroughAsync"/>). Run it on the
    /// read's own repeatable-read transaction and run the read's statement on the same one, so a build or a late row between the two cannot
    /// change what this answered.
    /// </summary>
    public static async Task<DateTime?> UsableMidEndAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime midStart, DateTime midEnd, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using (var probe = new NpgsqlCommand(TablesExistSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            if (!(bool)(await probe.ExecuteScalarAsync(cancellationToken))!)
            {
                return null;
            }
        }

        var through = await BuiltThroughAsync(connection, transaction, serverId, midStart, midEnd, commandTimeoutSeconds, cancellationToken);
        return through is DateTime end && end - midStart >= MinMid ? end : null;
    }

    /// <summary>One planned hour and the servers due in it.</summary>
    public readonly record struct Build(DateTime Bucket, int[] ServerIds);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int HoursBuilt, int ServerHoursBuilt, int Failed, int Deferred, long RowsRemoved);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>
    /// The whole hours of <c>[from, to)</c> (both on the hour, naive UTC) that the rollup does not hold whole for <paramref name="serverId"/>,
    /// oldest first. Run it on the read's own connection and transaction, on a store at <see cref="Version"/> or above, and take every
    /// hour of the range that is not listed from the rollup and every listed hour from raw.
    /// </summary>
    public static async Task<IReadOnlyList<DateTime>> IncompleteHoursAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime from, DateTime to, int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        (await ReadIncompleteHoursAsync(connection, transaction, serverId, from, to, commandTimeoutSeconds, cancellationToken)).Select(h => h.Bucket).ToList();

    /// <summary>
    /// #9164: where the whole hours of <c>[from, to)</c> the rollup holds stop, for a read whose window ends now. The tick builds an hour on its
    /// first hourly run after <see cref="Grace"/>, so the newest closed hours of a window that ends now have no build yet. Those hours, at the
    /// END of the range and with no build row at all, are not a defect: the read takes them from raw, as it takes its edges. Returns the end of
    /// the usable range (<paramref name="to"/> when every hour is whole), or null when any other hour is not whole: a hole, a stale hour or an
    /// hour before the ledger's start anywhere but in that unbuilt tail. A hole in the middle sends the whole read to raw even when the tail is unbuilt.
    /// </summary>
    public static async Task<DateTime?> BuiltThroughAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime from, DateTime to, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var incomplete = await ReadIncompleteHoursAsync(connection, transaction, serverId, from, to, commandTimeoutSeconds, cancellationToken);
        var through = to;
        for (var i = incomplete.Count - 1; i >= 0 && incomplete[i].Bucket == through.AddHours(-1) && !incomplete[i].Built; i--)
        {
            through = incomplete[i].Bucket;
            incomplete.RemoveAt(i);
        }

        return incomplete.Count == 0 ? through : null;
    }

    private static async Task<List<(DateTime Bucket, bool Built)>> ReadIncompleteHoursAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime from, DateTime to, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var hours = new List<(DateTime Bucket, bool Built)>();
        await using var command = new NpgsqlCommand(IncompleteHoursSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(from) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(to) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            hours.Add((reader.GetDateTime(0), reader.GetBoolean(1)));
        }

        return hours;
    }

    /// <summary>Whether the rollup holds every whole hour of <c>[from, to)</c> for <paramref name="serverId"/>; see <see cref="IncompleteHoursAsync"/>.</summary>
    public static async Task<bool> IsCompleteAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime from, DateTime to, int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        (await IncompleteHoursAsync(connection, transaction, serverId, from, to, commandTimeoutSeconds, cancellationToken)).Count == 0;

    /// <summary>The hours due at <paramref name="nowUtc"/>, newest first, at most <see cref="MaxHoursPerTick"/>.</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = Grace });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = SettleAfter });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxHoursPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetDateTime(0), reader.GetFieldValue<int[]>(1)));
        }

        return builds;
    }

    /// <summary>Removes the hours before the first one inside raw's retention from both tables, in one transaction; returns the rollup rows removed.</summary>
    public static async Task<long> PruneAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        long removed;
        await using (var rollup = PruneCommand(connection, transaction, PruneSql, nowUtc, retentionDays))
        {
            removed = await rollup.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var built = PruneCommand(connection, transaction, PruneBuiltSql, nowUtc, retentionDays))
        {
            await built.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return removed;
    }

    private static NpgsqlCommand PruneCommand(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, DateTime nowUtc, int retentionDays)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
        return command;
    }

    /// <summary>
    /// Builds one hour for the servers due in it, in one repeatable-read transaction so the raw rows the build reads and the ledger it
    /// records are one snapshot: the hour is cleared, filled again and recorded together, and a reader sees the old hour or the new one.
    /// </summary>
    public static async Task BuildHourAsync(
        NpgsqlConnection connection, int[] serverIds, DateTime bucket, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(serverIds);

        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await RunHourStatementAsync(connection, transaction, DeleteHourSql, serverIds, bucket, null, cancellationToken);
        await RunHourStatementAsync(connection, transaction, BuildHourSql, serverIds, bucket, null, cancellationToken);
        await RunHourStatementAsync(connection, transaction, RecordBuiltSql, serverIds, bucket, nowUtc, cancellationToken);

        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task RunHourStatementAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, int[] serverIds, DateTime bucket, DateTime? builtAt, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(bucket) });
        if (builtAt is { } stamp)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(stamp) });
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// One tick: remove the hours raw's retention took, plan, then build each planned hour in its own transaction, newest first. An hour
    /// that fails is logged and the loop goes on; the next tick plans it again. Once <see cref="MaxTickDuration"/> has passed no further
    /// hour starts, and the planned hours left are reported as deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, retentionDays, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, int, ILogger, CancellationToken)"/> with the elapsed time since the
    /// tick began supplied by <paramref name="elapsed"/>, read before each hour starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int hoursBuilt = 0, serverHours = 0, failed = 0, started = 0;

        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.QueryStatsHostHourly, cancellationToken);
        var removed = await PruneAsync(connection, nowUtc, retentionDays, cancellationToken);
        var plan = await PlanBuildsAsync(connection, nowUtc, retentionDays, cancellationToken);

        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                await BuildHourAsync(connection, item.ServerIds, item.Bucket, nowUtc, cancellationToken);
                hoursBuilt++;
                serverHours += item.ServerIds.Length;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Query stats host hourly rollup: building hour {Hour:yyyy-MM-dd HH}:00 for {Servers} server(s) failed and is retried next tick: {Message}",
                    item.Bucket, item.ServerIds.Length, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        logger.LogInformation("Query stats host hourly rollup: built {Hours} hour(s) for {ServerHours} server-hour(s), failed {Failed}, deferred {Deferred}, removed {Removed} row(s), {ElapsedMs} ms",
            hoursBuilt, serverHours, failed, deferred, removed, (long)elapsed().TotalMilliseconds);
        return new TickResult(hoursBuilt, serverHours, failed, deferred, removed);
    }
}
