/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One retained job-history row (a step, or the step_id 0 job outcome) as the store holds it, with its times as
/// naive UTC: <c>run_datetime</c> is the monitored server's local wall clock, so the reader converts it with the
/// server's <see cref="ServerClock"/> (#4766). The WPF viewer and the MCP tool both map from this record.
/// </summary>
public sealed record DarlingJobHistoryRow(
    int ServerId,
    string ServerName,
    long InstanceId,
    string JobId,
    string JobName,
    bool JobEnabled,
    string? CategoryName,
    int StepId,
    string? StepName,
    int RunStatus,
    string? RunStatusDesc,
    DateTime? RunDateTimeUtc,
    long RunDurationSeconds,
    int RetriesAttempted,
    string? Message,
    DateTime? LastSuccessfulRunUtc,
    bool IsLongRunning);

/// <summary>
/// Optional filters on the job-history read, applied inside the per-server top-N so the row limit counts only
/// matching rows. The MCP tool passes what the caller asked for, and the viewer's Job History tab passes its Status and Category
/// choices (#5703), so a filter is applied before the row cap rather than to the rows the cap kept.
/// Job name and category match exactly, ignoring case. <paramref name="UntilUtc"/> (naive UTC) is an optional end
/// instant: the SQL bounds the newest runs by it (widened by an hour, like the start) and the job stats exactly,
/// and <see cref="DarlingJobHistoryReader.GetAsync"/> then cuts the runs exactly in C#.
/// </summary>
public sealed record JobHistoryFilter(string? JobName = null, int? RunStatus = null, string? Category = null, DateTime? UntilUtc = null, int? MinDurationSeconds = null)
{
    private int UntilParam(int firstParam) => firstParam + (JobName is null ? 0 : 1) + (RunStatus is null ? 0 : 1) + (Category is null ? 0 : 1) + (MinDurationSeconds is null ? 0 : 1);

    /// <summary>The <c>AND</c> clauses for the set filters, numbering their parameters from <paramref name="firstParam"/>.</summary>
    public string Clauses(int firstParam)
    {
        var next = firstParam;
        var sql = string.Empty;
        if (JobName is not null)
        {
            sql += $"\n        AND   lower(jh.job_name) = lower(${next++})";
        }

        if (RunStatus is not null)
        {
            sql += $"\n        AND   jh.run_status = ${next++}";
        }

        if (Category is not null)
        {
            sql += $"\n        AND   lower(jh.category_name) = lower(${next++})";
        }

        if (MinDurationSeconds is not null)
        {
            sql += $"\n        AND   jh.run_duration_seconds >= ${next++}";
        }

        if (UntilUtc is not null)
        {
            sql += $"\n        AND   jh.run_datetime <= ${next} + make_interval(mins => so.offset_minutes)";
        }

        return sql;
    }

    /// <summary>
    /// The job-stats clause for the end instant, the same exact bound as <see cref="Clauses"/>, so a stat never names a run after it
    /// and a run exactly at the end instant is both shown and counted. Both convert with the stored server offset, so across a DST
    /// change they can be off by up to an hour; the exact C# cut in <see cref="ApplyWindow"/> settles the rows shown.
    /// </summary>
    internal string StatsClause(int firstParam) =>
        UntilUtc is null ? string.Empty : $"\n        AND   jh.run_datetime <= ${UntilParam(firstParam)} + make_interval(mins => so.offset_minutes)";

    internal IEnumerable<NpgsqlParameter> Parameters()
    {
        if (JobName is not null)
        {
            yield return new NpgsqlParameter<string> { TypedValue = JobName };
        }

        if (RunStatus is not null)
        {
            yield return new NpgsqlParameter<int> { TypedValue = RunStatus.Value };
        }

        if (Category is not null)
        {
            yield return new NpgsqlParameter<string> { TypedValue = Category };
        }

        if (MinDurationSeconds is not null)
        {
            yield return new NpgsqlParameter<int> { TypedValue = MinDurationSeconds.Value };
        }

        if (UntilUtc is not null)
        {
            yield return new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(UntilUtc.Value, DateTimeKind.Unspecified) };
        }
    }
}

/// <summary>
/// The store-side job-history read shared by the WPF Job History tab and the <c>get_job_history</c> tool: the SQL,
/// the per-row server-clock conversion and the exact window cut. See <see cref="BuildJobHistorySql"/> for the
/// statement and <see cref="ApplyWindow"/> for the cut.
/// </summary>
public static class DarlingJobHistoryReader
{
    /// <summary>The job-history statement's text: <see cref="BuildJobHistorySql"/> with no filter, fleet-wide.</summary>
    public static readonly string FleetSql = BuildJobHistorySql(scopedToServer: false);

    /// <summary>The job-history statement's text scoped to one server.</summary>
    public static readonly string ServerSql = BuildJobHistorySql(scopedToServer: true);

    /// <summary>
    /// Reads the newest <paramref name="limit"/> runs at or after <paramref name="sinceUtc"/> (naive UTC), for one
    /// server or, with no <paramref name="serverId"/>, all of them, newest first. The window is exact after the
    /// server-local to UTC conversion; the SQL pre-filters widened by an hour.
    /// </summary>
    public static async Task<List<DarlingJobHistoryRow>> GetAsync(
        NpgsqlDataSource dataSource, DateTime sinceUtc, int? serverId, int limit, int commandTimeoutSeconds,
        JobHistoryFilter? filter = null, CancellationToken cancellationToken = default)
    {
        var sql = BuildJobHistorySql(serverId.HasValue, filter);
        var clocks = await DarlingServerClocksReader.GetAsync(dataSource, serverId, commandTimeoutSeconds, cancellationToken);

        var rows = new List<DarlingJobHistoryRow>();

        await using var command = dataSource.CreateCommand(sql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified) });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        if (filter is not null)
        {
            foreach (var p in filter.Parameters())
            {
                command.Parameters.Add(p);
            }
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRow(reader, clocks));
        }

        return ApplyWindow(rows, sinceUtc, limit, filter?.UntilUtc);
    }

    /// <summary>
    /// Every distinct job category with a run in the window (#5703): the Job History tab's Category drop-down choices. A read of its
    /// own, with no row cap and no Status or Category filter, because the grid's read keeps the newest runs of the whole fleet and a
    /// busy server fills that cap, so the categories of a quieter server would otherwise reach the drop-down only after a Status
    /// filter surfaced them. The window is the grid's, widened by the hour the grid's pre-filter widens by (<see cref="BuildCategoriesSql"/>);
    /// a category whose only run sits in that margin can be offered, which costs an empty grid and never a missing choice. With
    /// <paramref name="serverId"/> the read is that server's, so the drop-down is not offered a category only another server has (a pick
    /// that would give an empty grid); without it the read is fleet-wide.
    /// </summary>
    /// <param name="dataSource">The store.</param>
    /// <param name="sinceUtc">The window's start (naive UTC), the grid's.</param>
    /// <param name="untilUtc">The window's end (naive UTC) for a range that ended in the past, else null.</param>
    /// <param name="commandTimeoutSeconds">The read's deadline.</param>
    /// <param name="serverId">The one server to read, or null for every server.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public static async Task<List<string>> GetCategoriesAsync(
        NpgsqlDataSource dataSource, DateTime sinceUtc, DateTime? untilUtc, int commandTimeoutSeconds, int? serverId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var categories = new List<string>();
        await using var command = dataSource.CreateCommand(BuildCategoriesSql(untilUtc.HasValue, serverId.HasValue));
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        if (untilUtc is { } until)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(until, DateTimeKind.Unspecified) });
        }

        if (serverId is { } server)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = server });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            categories.Add(reader.GetString(0));
        }

        return categories;
    }

    /// <summary>
    /// <see cref="GetCategoriesAsync"/>'s SQL text. $1 is the window start (naive UTC) and $2 the <see cref="EventWindowFloor"/> for it
    /// (<c>job_history</c> is partitioned on <c>collection_time</c>, so the floor lets the planner exclude chunks, as in
    /// <see cref="BuildJobHistorySql"/>); with <paramref name="bounded"/>, $3 is the window end. <c>run_datetime</c> is the server's
    /// wall clock, so each server's latest collected UTC offset (0 when none) moves the bound onto it, widened by an hour as the grid's
    /// pre-filter is. With <paramref name="forServer"/>, the next parameter after those (the window end when bounded, else $3) is the
    /// server's id.
    /// </summary>
    public static string BuildCategoriesSql(bool bounded, bool forServer = false)
    {
        var serverSql = forServer ? $"\nAND   jh.server_id = ${(bounded ? 4 : 3)}" : string.Empty;

        var untilSql = bounded
            ? "\nAND   jh.run_datetime <= $3 + make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) + interval '1 hour'"
            : string.Empty;

        return $@"
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
)
SELECT DISTINCT jh.category_name
FROM job_history AS jh
LEFT JOIN svr ON svr.server_id = jh.server_id
WHERE jh.collection_time >= $2
AND   jh.run_datetime >= $1 + make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) - interval '1 hour'{untilSql}{serverSql}
AND   jh.category_name IS NOT NULL
AND   jh.category_name <> ''
ORDER BY jh.category_name";
    }

    /// <summary>
    /// The exact window, after the server-local to UTC conversion: keeps the runs at or after
    /// <paramref name="sinceUtc"/> (the SQL pre-filter is widened by an hour) and, when given, at or before
    /// <paramref name="untilUtc"/>, orders them newest first by their real
    /// UTC time, and keeps the newest <paramref name="limit"/> (#4766).
    /// </summary>
    public static List<DarlingJobHistoryRow> ApplyWindow(List<DarlingJobHistoryRow> rows, DateTime sinceUtc, int limit, DateTime? untilUtc = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var since = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified);
        var until = untilUtc is { } u ? DateTime.SpecifyKind(u, DateTimeKind.Unspecified) : (DateTime?)null;
        var kept = new List<DarlingJobHistoryRow>(rows.Count);
        foreach (var row in rows)
        {
            if (row.RunDateTimeUtc is { } runUtc && runUtc >= since && (until is null || runUtc <= until))
            {
                kept.Add(row);
            }
        }

        kept.Sort(static (a, b) =>
        {
            var byTime = Nullable.Compare(b.RunDateTimeUtc, a.RunDateTimeUtc);
            return byTime != 0 ? byTime : b.InstanceId.CompareTo(a.InstanceId);
        });

        if (limit >= 0 && kept.Count > limit)
        {
            kept.RemoveRange(limit, kept.Count - limit);
        }

        return kept;
    }

    /// <summary>
    /// Builds <see cref="GetAsync"/>'s SQL text, split out so Darling.Tests can pin both parameter
    /// shapes (fleet-wide and single-server) without a live Postgres. $1 window start (naive UTC); when
    /// <paramref name="scopedToServer"/>, $2 server_id and the floor moves to $3, the limit to $4 — otherwise
    /// the floor is $2 and the limit $3. The floor is the <see cref="EventWindowFloor"/> for $1:
    /// <c>job_history</c> is a hypertable partitioned on <c>collection_time</c>, which the de-skewed
    /// <c>run_datetime</c> window alone gives the planner nothing to exclude a chunk on (#4229). The
    /// de-skewed run time is always ≤ <c>collection_time</c> (store UTC at collection, and a job's history row
    /// is collected after the run it reports), so the floor cannot drop a qualifying row.
    /// <para>
    /// <b>Per-server top-N over V150's index (#4477), replacing a fleet-wide scan.</b> The old text scanned
    /// every <c>job_history</c> row in the two-day window across the whole fleet, sorted the lot by
    /// <c>run_datetime_utc DESC, instance_id DESC</c>, and took the top <c>limit</c> — a full scan of every
    /// server's history to find the newest rows, with no index able to serve <c>run_datetime_utc</c> because
    /// it is computed (<c>run_datetime</c> minus a per-server offset), not a stored column. Each monitored
    /// server's UTC offset is constant for the life of the read (one <c>server_properties</c> lookup), so
    /// "newest <c>limit</c> rows fleet-wide by <c>run_datetime_utc DESC, instance_id DESC</c>" is exactly a
    /// merge of each server's own newest <c>limit</c> rows by RAW <c>run_datetime DESC, instance_id DESC</c> —
    /// an order <see cref="PgMigrations"/>' V150 index <c>idx_job_history_server_run (server_id, run_datetime
    /// DESC, instance_id DESC)</c> serves directly, with no sort. <c>server_offsets</c> enumerates every
    /// registered server (mirroring the old <c>reg</c> LEFT JOIN, but now the LATERAL's driving table) with
    /// its offset (0 when none collected yet, exactly like the old COALESCE); <c>top_by_server</c> is one
    /// <c>CROSS JOIN LATERAL</c> per server, each an index-only top-<c>limit</c> descent; <c>base</c> merges
    /// those (at most <c>servers × limit</c>, never more) and re-applies the SAME final ORDER BY/LIMIT to pick
    /// the fleet-wide top <c>limit</c> — identical rows, identical order, identical tie-break to the old text.
    /// <c>active_servers</c> drives off <c>job_history</c> itself (<c>DISTINCT server_id</c> for rows with
    /// <c>collection_time &gt;= {floorParam}</c>, the same superset floor the per-server LATERAL already
    /// filters on) rather than the <c>servers</c> registry — an earlier draft drove off <c>servers</c>
    /// directly, which would have silently dropped every row for a <c>server_id</c> the registry no longer
    /// carries (a decommissioned server whose retained history the old <c>LEFT JOIN reg</c> still showed,
    /// under the raw collected name). <c>server_offsets</c> LEFT JOINs <c>servers</c> onto that driving set for
    /// the display-name fallback (NULL when unregistered, exactly like the old COALESCE), so a
    /// <c>job_history</c> row survives regardless of whether its server is still registered.
    /// </para>
    /// <para>
    /// <b>job_stats stays a bounded per-server window, not a second fleet-wide scan.</b> The average/max a row
    /// needs only ever depends on its OWN job's step_id-0 SUCCESS rows on its OWN server, so <c>job_stats</c>
    /// is a <c>CROSS JOIN LATERAL</c> per server actually present in <c>base</c> (not all registered servers —
    /// a server with no rows in the final top-<c>limit</c> needs no stats at all), using the same index's
    /// <c>server_id</c>/<c>run_datetime</c> range to bound the scan to that one server's window, filtered to
    /// <c>step_id = 0 AND run_status = 1</c> and to just the job_ids <c>base</c> actually kept for that server
    /// (a job never shown needs no average computed for it), <c>GROUP BY job_id</c>. Values are identical to
    /// the old text's: same 24-hour-window predicate, same step_id/run_status filter, same GROUP BY grain —
    /// only the scan is now bounded to one server's rows via the index instead of every server's success rows
    /// combined. <c>job_stats</c> LEFT JOINs onto <c>base</c> so a job with no successful step-0 row in the
    /// window still surfaces its other rows with a NULL avg/last-success, exactly as before.
    /// </para>
    /// <para>
    /// <b>One aggregate per (server, job) over V220's partial index (#6798).</b> <c>job_stats</c> now drives off
    /// the distinct (server_id, job_id) pairs <c>base</c> kept and aggregates each pair on its own, so the job is
    /// an equality the planner seeks on. V220's <c>idx_job_history_job_success (server_id, job_id, run_datetime
    /// DESC) INCLUDE (run_duration_seconds, collection_time) WHERE step_id = 0 AND run_status = 1</c> serves that
    /// seek index-only: it holds only the successful job outcomes, and the INCLUDE columns cover the average and
    /// the <c>collection_time</c> floor, so a cold read no longer visits the heap. Before, the per-server
    /// aggregate read the server's whole run-time range through <c>idx_job_history_server_run</c> and dropped
    /// the step rows, the failures and the other jobs after reading them from scattered heap pages. An
    /// aggregate with no GROUP BY returns one row even when nothing matches. The <c>WHERE
    /// js.last_success_run_local IS NOT NULL</c> drops that row so the CTE's own row set matches the old GROUP BY, which never
    /// produced one. The output does not need it: the one LEFT JOIN reads both columns NULL-tolerantly, so a pair with no
    /// success lands on NULL and false either way. <c>MAX(run_datetime)</c> is the test, not the average, because the window
    /// predicate already requires a non-null <c>run_datetime</c> while <c>run_duration_seconds</c> is nullable:
    /// a job whose successful runs all lack a duration keeps its last success and a NULL average, as before. The
    /// values, the window predicates and the LEFT JOIN are unchanged, so the rows are identical.
    /// </para>
    /// </summary>
    public static string BuildJobHistorySql(bool scopedToServer, JobHistoryFilter? filter = null)
    {
        var serverFilter = scopedToServer ? "AND   jh.server_id = $2" : string.Empty;
        var floorParam = scopedToServer ? "$3" : "$2";
        var limitParam = scopedToServer ? "$4" : "$3";
        var filterSql = filter is null ? string.Empty : filter.Clauses(scopedToServer ? 5 : 4);
        var statsUntilSql = filter is null ? string.Empty : filter.StatsClause(scopedToServer ? 5 : 4);

        return $@"
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
active_servers AS (
    SELECT DISTINCT jh.server_id
    FROM job_history AS jh
    WHERE jh.collection_time >= {floorParam}
    {serverFilter}
),
server_offsets AS (
    SELECT
        a.server_id,
        reg.display_name,
        COALESCE(svr.utc_offset_minutes, 0) AS offset_minutes
    FROM active_servers AS a
    LEFT JOIN svr ON svr.server_id = a.server_id
    LEFT JOIN servers AS reg ON reg.server_id = a.server_id
),
top_by_server AS (
    SELECT
        so.server_id,
        COALESCE(so.display_name, top.server_name) AS server_name,
        top.instance_id,
        top.job_id,
        top.job_name,
        top.job_enabled,
        top.category_name,
        top.step_id,
        top.step_name,
        top.run_status,
        top.run_status_desc,
        top.run_datetime AS run_datetime_local,
        top.run_datetime - make_interval(mins => so.offset_minutes) AS approx_run_utc,
        top.run_duration_seconds,
        top.retries_attempted,
        top.message
    FROM server_offsets AS so
    CROSS JOIN LATERAL (
        SELECT
            jh.server_name,
            jh.instance_id,
            jh.job_id,
            jh.job_name,
            jh.job_enabled,
            jh.category_name,
            jh.step_id,
            jh.step_name,
            jh.run_status,
            jh.run_status_desc,
            jh.run_datetime,
            jh.run_duration_seconds,
            jh.retries_attempted,
            jh.message
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.collection_time >= {floorParam}
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes) - interval '1 hour'{filterSql}
        ORDER BY jh.run_datetime DESC, jh.instance_id DESC
        LIMIT {limitParam}
    ) AS top
),
base AS (
    SELECT *
    FROM top_by_server
    ORDER BY approx_run_utc DESC, instance_id DESC
    LIMIT {limitParam}
),
job_stats AS (
    SELECT
        b.server_id,
        b.job_id,
        js.avg_success_duration,
        js.last_success_run_local
    FROM (SELECT DISTINCT server_id, job_id FROM base) AS b
    JOIN server_offsets AS so ON so.server_id = b.server_id
    CROSS JOIN LATERAL (
        SELECT
            AVG(jh.run_duration_seconds) AS avg_success_duration,
            MAX(jh.run_datetime) AS last_success_run_local
        FROM job_history AS jh
        WHERE jh.server_id = b.server_id
        AND   jh.job_id = b.job_id
        AND   jh.step_id = 0
        AND   jh.run_status = 1
        AND   jh.collection_time >= {floorParam}
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes){statsUntilSql}
    ) AS js
    WHERE js.last_success_run_local IS NOT NULL
)
SELECT
    base.server_id,
    base.server_name,
    base.instance_id,
    base.job_id,
    base.job_name,
    base.job_enabled,
    base.category_name,
    base.step_id,
    base.step_name,
    base.run_status,
    base.run_status_desc,
    base.run_datetime_local,
    base.run_duration_seconds,
    base.retries_attempted,
    base.message,
    job_stats.last_success_run_local,
    CASE
        WHEN base.step_id = 0
        AND  job_stats.avg_success_duration IS NOT NULL
        AND  job_stats.avg_success_duration > 0
        AND  base.run_duration_seconds > job_stats.avg_success_duration * 2
        AND  base.run_duration_seconds > 60
        THEN true
        ELSE false
    END AS is_long_running
FROM base
LEFT JOIN job_stats
    ON  job_stats.server_id = base.server_id
    AND job_stats.job_id = base.job_id
ORDER BY base.approx_run_utc DESC, base.instance_id DESC";
    }

    /// <summary>Maps one row of <see cref="BuildJobHistorySql"/>'s result set. The run time and the last
    /// successful run arrive as the server's own wall clock; they leave as naive UTC, converted with that
    /// server's clock from <paramref name="clocks"/> (UTC when it has none).</summary>
    public static DarlingJobHistoryRow ReadRow(DbDataReader reader, IReadOnlyDictionary<int, ServerClock> clocks)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var serverId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
        var clock = DarlingServerClocksReader.ClockFor(clocks, serverId);

        return new DarlingJobHistoryRow(
            serverId,
            reader.IsDBNull(1) ? "" : reader.GetString(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
            reader.IsDBNull(3) ? "" : reader.GetString(3),
            reader.IsDBNull(4) ? "" : reader.GetString(4),
            !reader.IsDBNull(5) && reader.GetBoolean(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : clock.ToUtc(reader.GetDateTime(11)),
            reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
            reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
            reader.IsDBNull(14) ? null : reader.GetString(14),
            reader.IsDBNull(15) ? null : clock.ToUtc(reader.GetDateTime(15)),
            !reader.IsDBNull(16) && reader.GetBoolean(16));
    }
}
