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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One retained job-history row for the Job History tab (issue #1433) — a single collected
/// <c>sysjobhistory</c> row (a step or the step_id 0 job-outcome). The Darling twin of Lite's
/// <c>JobHistoryRow</c>: the same duration / step / retry display helpers and the same failure /
/// long-runtime / retry flags for the grid's color-coding. run_datetime is the monitored server's LOCAL
/// wall clock, so <see cref="ViewerDataService.GetJobHistoryAsync"/> de-skews it to naive-UTC in SQL
/// (subtracting <c>server_properties.utc_offset_minutes</c>) before it reaches this row — exactly like the
/// Default Trace reader — so <see cref="RunTimeLocal"/> renders through the same
/// <see cref="ViewerTimeHelper.ForDisplay"/> as every other viewer grid and sorts consistently with them.
/// </summary>
public sealed class ViewerJobHistoryRow
{
    public int ServerId { get; init; }

    /// <summary>The operator's display alias when one is registered, the raw collected name otherwise
    /// (#2126 — the Server column and filter combo show the same names every other tab does).</summary>
    public string ServerName { get; init; } = "";

    public long InstanceId { get; init; }
    public string JobId { get; init; } = "";
    public string JobName { get; init; } = "";
    public bool JobEnabled { get; init; }
    public string? CategoryName { get; init; }
    public int StepId { get; init; }
    public string? StepName { get; init; }
    public int RunStatus { get; init; }
    public string? RunStatusDesc { get; init; }

    /// <summary>run_datetime as naive UTC: the stored server-local time converted with the server's
    /// <see cref="ServerClock"/> (its time zone where known, else its offset), so a run on either side of a
    /// daylight-saving change lands at its real UTC time (#4766).</summary>
    public DateTime? RunDateTimeUtc { get; init; }

    public long RunDurationSeconds { get; init; }
    public int RetriesAttempted { get; init; }
    public string? Message { get; init; }
    public DateTime? LastSuccessfulRunUtc { get; init; }
    public bool IsLongRunning { get; init; }

    /// <summary>Stored naive-UTC; shown in the viewer machine's local time (the viewer convention).</summary>
    public string RunTimeLocal => RunDateTimeUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "";

    public string LastSuccessfulRunLocal => LastSuccessfulRunUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "Never";

    public string DurationFormatted => FormatDuration(RunDurationSeconds);

    public string StepDisplay => StepId == 0 ? "(Job outcome)" : $"{StepId}: {StepName}";

    public string RetriesDisplay => RetriesAttempted > 0 ? RetriesAttempted.ToString(System.Globalization.CultureInfo.InvariantCulture) : "";

    public string JobEnabledDisplay => JobEnabled ? "Yes" : "No";

    public bool IsFailed => RunStatus == 0;
    public bool IsSucceeded => RunStatus == 1;
    public bool IsRetry => RunStatus == 2;
    public bool IsCanceled => RunStatus == 3;

    private static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Retained SQL Agent job-run history for the fleet Job History tab. Reads the BASE
    /// <c>job_history</c> table (no <c>v_*</c> view — a collector added after V14 has none; the bare name
    /// resolves through the store's <c>search_path</c>, like default_trace_events / server_properties),
    /// windowing on the run time so "last N hours/days" means jobs that RAN in that window (a first-run
    /// backfill of a year of history does not flood a short window the way a collection_time filter would).
    /// <para>
    /// run_datetime is the monitored server's LOCAL wall clock, so it is converted to naive-UTC in C# with
    /// the server's <see cref="ServerClock"/> (its time zone id where SQL Server reports one, else the
    /// collected <c>server_properties.utc_offset_minutes</c>, else UTC) and windowed against the naive-UTC
    /// bound ($1), so the returned timestamps share the viewer's UTC frame and render/sort consistently on
    /// the tab. The SQL cannot do that conversion (PostgreSQL <c>AT TIME ZONE</c> does not resolve Windows
    /// zone ids, and one subtracted offset is an hour off on the far side of a daylight-saving change), so it
    /// pre-filters with the latest offset, widened by an hour, and <see cref="ApplyJobHistoryWindow"/> filters
    /// exactly after the conversion (#4766).
    /// Long-runtime is computed reader-side via a per-job window function (a step_id 0 outcome exceeding 2x
    /// its job's average successful-outcome duration, floored at 60s), and each row carries its job's last
    /// successful outcome run. With no <paramref name="serverId"/> it aggregates ALL servers (the tab
    /// default); with one it scopes to that server (the Server filter combo). server_name resolves through
    /// the <c>servers</c> registry to the operator's display alias when one exists (#2126), so the tab
    /// speaks the same names as the rest of the viewer.
    /// </para>
    /// </summary>
    public async Task<List<ViewerJobHistoryRow>> GetJobHistoryAsync(
        DateTime sinceUtc, int? serverId = null, int limit = 2000, CancellationToken cancellationToken = default)
    {
        var sql = BuildJobHistorySql(serverId.HasValue);
        var clocks = await GetServerClocksAsync(serverId, cancellationToken);

        var rows = new List<ViewerJobHistoryRow>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified) });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadJobHistoryRow(reader, clocks));
        }

        return ApplyJobHistoryWindow(rows, sinceUtc, limit);
    }

    /// <summary>
    /// The exact window, after the server-local to UTC conversion: keeps the runs at or after
    /// <paramref name="sinceUtc"/> (the SQL pre-filter is widened by an hour, so a run just before the
    /// window can still be in the set), orders them newest first by their real UTC time (the SQL orders by
    /// the latest offset, which is an hour off across a daylight-saving change), and keeps the newest
    /// <paramref name="limit"/> (#4766).
    /// </summary>
    internal static List<ViewerJobHistoryRow> ApplyJobHistoryWindow(List<ViewerJobHistoryRow> rows, DateTime sinceUtc, int limit)
    {
        var since = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified);
        var kept = new List<ViewerJobHistoryRow>(rows.Count);
        foreach (var row in rows)
        {
            if (row.RunDateTimeUtc is { } runUtc && runUtc >= since)
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
    /// Each server's clock from its newest <c>server_properties</c> row that has an offset (the time zone id
    /// alongside it where the store has the V134 column, else the offset alone), keyed by server id. A server
    /// with no row is absent, and its stored times are read as UTC — what the old SQL's <c>COALESCE(..., 0)</c>
    /// did. With <paramref name="serverId"/> it reads that one server (#4766).
    /// </summary>
    internal async Task<Dictionary<int, ServerClock>> GetServerClocksAsync(int? serverId, CancellationToken cancellationToken)
    {
        try
        {
            return await ReadServerClocksAsync(BuildServerClocksSql(serverId.HasValue, withZone: true), serverId, withZone: true, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            /* A store below V134 has no time_zone_id: read the offset alone, as this read did before. */
            return await ReadServerClocksAsync(BuildServerClocksSql(serverId.HasValue, withZone: false), serverId, withZone: false, cancellationToken);
        }
    }

    internal static string BuildServerClocksSql(bool scopedToServer, bool withZone)
    {
        var zone = withZone ? ",\n    time_zone_id" : "";
        var scope = scopedToServer ? "AND   server_id = $1\n" : "";
        return $@"
SELECT DISTINCT ON (server_id)
    server_id,
    utc_offset_minutes{zone}
FROM server_properties
WHERE utc_offset_minutes IS NOT NULL
{scope}ORDER BY server_id, collection_time DESC";
    }

    private async Task<Dictionary<int, ServerClock>> ReadServerClocksAsync(
        string sql, int? serverId, bool withZone, CancellationToken cancellationToken)
    {
        var clocks = new Dictionary<int, ServerClock>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            clocks[reader.GetInt32(0)] = ServerClock.Resolve(
                withZone && !reader.IsDBNull(2) ? reader.GetString(2) : null,
                reader.IsDBNull(1) ? null : reader.GetInt32(1));
        }

        return clocks;
    }

    /// <summary>
    /// Builds <see cref="GetJobHistoryAsync"/>'s SQL text, split out so Darling.Tests can pin both parameter
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
    /// </summary>
    internal static string BuildJobHistorySql(bool scopedToServer)
    {
        var serverFilter = scopedToServer ? "AND   jh.server_id = $2" : string.Empty;
        var floorParam = scopedToServer ? "$3" : "$2";
        var limitParam = scopedToServer ? "$4" : "$3";

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
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes) - interval '1 hour'
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
        so.server_id,
        js.job_id,
        js.avg_success_duration,
        js.last_success_run_local
    FROM (SELECT DISTINCT server_id FROM base) AS b
    JOIN server_offsets AS so ON so.server_id = b.server_id
    CROSS JOIN LATERAL (
        SELECT
            jh.job_id,
            AVG(jh.run_duration_seconds) AS avg_success_duration,
            MAX(jh.run_datetime) AS last_success_run_local
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.step_id = 0
        AND   jh.run_status = 1
        AND   jh.collection_time >= {floorParam}
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes)
        AND   jh.job_id IN (SELECT job_id FROM base WHERE base.server_id = so.server_id)
        GROUP BY jh.job_id
    ) AS js
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
    internal static ViewerJobHistoryRow ReadJobHistoryRow(DbDataReader reader, IReadOnlyDictionary<int, ServerClock> clocks)
    {
        var serverId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
        var clock = clocks.TryGetValue(serverId, out var known) ? known : ServerClock.Utc;

        return new()
        {
            ServerId = serverId,
            ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
            InstanceId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
            JobId = reader.IsDBNull(3) ? "" : reader.GetString(3),
            JobName = reader.IsDBNull(4) ? "" : reader.GetString(4),
            JobEnabled = !reader.IsDBNull(5) && reader.GetBoolean(5),
            CategoryName = reader.IsDBNull(6) ? null : reader.GetString(6),
            StepId = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
            StepName = reader.IsDBNull(8) ? null : reader.GetString(8),
            RunStatus = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
            RunStatusDesc = reader.IsDBNull(10) ? null : reader.GetString(10),
            RunDateTimeUtc = reader.IsDBNull(11) ? null : clock.ToUtc(reader.GetDateTime(11)),
            RunDurationSeconds = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
            RetriesAttempted = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
            Message = reader.IsDBNull(14) ? null : reader.GetString(14),
            LastSuccessfulRunUtc = reader.IsDBNull(15) ? null : clock.ToUtc(reader.GetDateTime(15)),
            IsLongRunning = !reader.IsDBNull(16) && reader.GetBoolean(16),
        };
    }

    /// <summary>
    /// The latest SQL Agent status snapshot per server (issue #1433 Phase 2) — Running/Stopped, startup
    /// type, and next scheduled run — read from the base <c>agent_status</c> table (newest row per server).
    /// <c>next_scheduled_run</c> is the server's local wall clock (from msdb), so it is de-skewed to
    /// naive-UTC in SQL (like the job run times) and rendered in the viewer's local time. With no
    /// <paramref name="serverId"/> it returns one row per server (the fleet header roll-up); with one it
    /// scopes to that server. The tab header consumes this; the "Agent Not Running" self-alert reads the same
    /// <c>agent_status</c> data service-side.
    /// </summary>
    public async Task<List<ViewerAgentStatusRow>> GetAgentStatusAsync(int? serverId = null, CancellationToken cancellationToken = default)
    {
        var serverFilter = serverId.HasValue ? "WHERE a.server_id = $1" : string.Empty;

        var sql = $@"
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
latest AS (
    SELECT
        a.server_id,
        COALESCE(reg.display_name, a.server_name) AS server_name,
        a.agent_running,
        a.agent_status_desc,
        a.agent_startup_desc,
        a.next_scheduled_run - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) AS next_scheduled_run_utc,
        ROW_NUMBER() OVER (PARTITION BY a.server_id ORDER BY a.collection_time DESC) AS rn
    FROM agent_status AS a
    LEFT JOIN svr ON svr.server_id = a.server_id
    LEFT JOIN servers AS reg ON reg.server_id = a.server_id
    {serverFilter}
)
SELECT
    server_id,
    server_name,
    agent_running,
    agent_status_desc,
    agent_startup_desc,
    next_scheduled_run_utc
FROM latest
WHERE rn = 1
ORDER BY server_name";

        var rows = new List<ViewerAgentStatusRow>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerAgentStatusRow
            {
                ServerId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                AgentRunning = !reader.IsDBNull(2) && reader.GetBoolean(2),
                AgentStatusDesc = reader.IsDBNull(3) ? null : reader.GetString(3),
                AgentStartupDesc = reader.IsDBNull(4) ? null : reader.GetString(4),
                NextScheduledRunUtc = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            });
        }

        return rows;
    }
}

/// <summary>
/// The latest SQL Agent status snapshot for one server (issue #1433 Phase 2) — the Darling twin of Lite's
/// <c>AgentStatusRow</c>. Drives the Job History tab header and the "Agent Not Running" self-alert.
/// next_scheduled_run is de-skewed to naive-UTC in SQL and rendered in the viewer's local time.
/// </summary>
public sealed class ViewerAgentStatusRow
{
    public int ServerId { get; init; }
    public string ServerName { get; init; } = "";
    public bool AgentRunning { get; init; }
    public string? AgentStatusDesc { get; init; }
    public string? AgentStartupDesc { get; init; }
    public DateTime? NextScheduledRunUtc { get; init; }

    public string StatusDisplay => AgentRunning ? "Running" : (AgentStatusDesc ?? "Stopped");

    public string NextScheduledRunLocal => NextScheduledRunUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "None scheduled";
}
