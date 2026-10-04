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
/// wall clock, so <see cref="ViewerDataService.GetJobHistoryAsync"/> converts it to naive-UTC in C# with the
/// server's <see cref="ServerClock"/> before it reaches this row — exactly like the Default Trace reader
/// (#4766) — so <see cref="RunTimeLocal"/> sorts consistently with every other viewer grid and renders through
/// <see cref="ViewerTimeHelper.ForDisplay"/>, as the plain wall time: a stored server-local time cannot say which pass
/// of the repeated autumn hour it was, so it never takes the offset suffix a real instant does.
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

    /// <summary>The viewer row for a store row (<see cref="DarlingJobHistoryRow"/>); the times are already naive UTC.</summary>
    public static ViewerJobHistoryRow From(DarlingJobHistoryRow dto)
    {
        ArgumentNullException.ThrowIfNull(dto);
        return new()
        {
            ServerId = dto.ServerId,
            ServerName = dto.ServerName,
            InstanceId = dto.InstanceId,
            JobId = dto.JobId,
            JobName = dto.JobName,
            JobEnabled = dto.JobEnabled,
            CategoryName = dto.CategoryName,
            StepId = dto.StepId,
            StepName = dto.StepName,
            RunStatus = dto.RunStatus,
            RunStatusDesc = dto.RunStatusDesc,
            RunDateTimeUtc = dto.RunDateTimeUtc,
            RunDurationSeconds = dto.RunDurationSeconds,
            RetriesAttempted = dto.RetriesAttempted,
            Message = dto.Message,
            LastSuccessfulRunUtc = dto.LastSuccessfulRunUtc,
            IsLongRunning = dto.IsLongRunning,
        };
    }

    /// <summary>
    /// <see cref="RunDateTimeUtc"/> in the current display mode, as the plain wall time and never with the repeated-hour
    /// UTC offset that <see cref="ViewerTimeHelper.FormatForDisplay(DateTime, string)"/> adds (#4766). That offset is only
    /// true for a real instant, and this one was converted from a STORED server-local time: a wall time that happens
    /// twice on the autumn change day reads as its first occurrence (<see cref="ServerClock.ToUtc"/>), so a step that
    /// ran at 01:30 the second time (-05:00) would print "-04:00", the other pass's offset. The bare text is what
    /// the row's own wall clock said.
    /// </summary>
    public string RunTimeLocal => RunDateTimeUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "";

    /// <summary><see cref="LastSuccessfulRunUtc"/> as the plain wall time, for the reason <see cref="RunTimeLocal"/> gives.</summary>
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
        var dtos = await DarlingJobHistoryReader.GetAsync(
            _dataSource, sinceUtc, serverId, limit, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken: cancellationToken);

        var rows = new List<ViewerJobHistoryRow>(dtos.Count);
        foreach (var dto in dtos)
        {
            rows.Add(ViewerJobHistoryRow.From(dto));
        }

        return rows;
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
    /// Where the job history's coverage starts for the window, through the shared probe (<see cref="DataWindowFloor"/>) over the
    /// <c>job_history</c> collector table (#4966): the later of the server's first collection and the table's retention edge, or
    /// its first row in the window if that is earlier. Job history is an event surface: the first collection copies the server's
    /// msdb history, so a run's own time (the time the read windows on) can sit long before the collection that stored it, and
    /// the caller names the earlier of this and the earliest run it shows (<see cref="ViewerEventDataStart.Of"/>). With a
    /// <paramref name="serverId"/> the answer is that server's; with none it is the earliest coverage among the servers that
    /// hold a row, or logged a run, in the window (the fleet scope of <see cref="DataWindowFloor.GetAsync"/>), the form the tab's
    /// All Servers view reads. A window no longer than <see cref="DurationTrendRouting.TruncationSlack"/> can never get a coverage
    /// note, so it starts no query for either form (<see cref="DataWindowFloor.GetForServerAsync"/> says the same for one server;
    /// the fleet form has no such guard of its own). Null when no server in scope counts and when the window lies wholly before the
    /// coverage.
    /// </summary>
    /// <param name="serverId">The server the tab shows, or null for every server.</param>
    /// <param name="startUtc">The window's start, the one the read takes (<see cref="GetJobHistoryAsync"/>).</param>
    /// <param name="endUtc">The window's end.</param>
    public async Task<DateTime?> GetJobHistoryDataStartAsync(
        int? serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        if (endUtc - startUtc <= DurationTrendRouting.TruncationSlack)
        {
            return null;
        }

        /* async, so a failure on the way to the query is a faulted task the tab's note step logs, never a throw out of the
           tab's load that costs the grid its rows. */
        var source = DataWindowFloor.Source.ForCollectorTable("job_history");
        return serverId is int id
            ? await DataWindowFloor.GetForServerAsync(
                _dataSource, source, id, startUtc, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken)
            : await DataWindowFloor.GetAsync(
                _dataSource, [source], null, startUtc, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
    }

    /// <summary>
    /// Each server's clock from its newest <c>server_properties</c> row that has an offset (the time zone id
    /// alongside it where the store has the V134 column, else the offset alone), keyed by server id. A server
    /// with no row is absent, and its stored times are read as UTC — what the old SQL's <c>COALESCE(..., 0)</c>
    /// did. With <paramref name="serverId"/> it reads that one server (#4766).
    /// </summary>
    internal Task<Dictionary<int, ServerClock>> GetServerClocksAsync(int? serverId, CancellationToken cancellationToken) =>
        DarlingServerClocksReader.GetAsync(_dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    internal static string BuildServerClocksSql(bool scopedToServer, bool withZone) =>
        DarlingServerClocksReader.BuildSql(scopedToServer, withZone);

    /// <summary>
    /// <paramref name="serverId"/>'s entry in <paramref name="clocks"/>, else the UTC clock: a server with no
    /// collected offset has its stored times read as UTC, as the old SQL's <c>COALESCE(..., 0)</c> did (#4766).
    /// </summary>
    internal static ServerClock ClockFor(IReadOnlyDictionary<int, ServerClock> clocks, int serverId)
    {
        return DarlingServerClocksReader.ClockFor(clocks, serverId);
    }

    /// <summary>The job-history statement, which lives in <see cref="DarlingJobHistoryReader"/> beside its reader. $1 window start
    /// (naive UTC); when <paramref name="scopedToServer"/>, $2 server_id, then the floor and the limit.</summary>
    internal static string BuildJobHistorySql(bool scopedToServer) =>
        DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer);

    /// <summary>Maps one row of <see cref="BuildJobHistorySql"/>'s result set. The run time and the last
    /// successful run arrive as the server's own wall clock; they leave as naive UTC, converted with that
    /// server's clock from <paramref name="clocks"/> (UTC when it has none).</summary>
    internal static ViewerJobHistoryRow ReadJobHistoryRow(DbDataReader reader, IReadOnlyDictionary<int, ServerClock> clocks) =>
        ViewerJobHistoryRow.From(DarlingJobHistoryReader.ReadRow(reader, clocks));

    /// <summary>
    /// The latest SQL Agent status snapshot per server (issue #1433 Phase 2) — Running/Stopped, startup
    /// type, and next scheduled run — read from the base <c>agent_status</c> table (newest row per server).
    /// <c>next_scheduled_run</c> is the server's local wall clock (from msdb), so the SQL returns it raw and
    /// it is converted to naive-UTC in C# with that server's <see cref="ServerClock"/> (like the job run
    /// times, so a next run on the far side of a daylight-saving change lands at its real UTC time), then
    /// rendered in the viewer's local time. With no <paramref name="serverId"/> it returns one row per server
    /// (the fleet header roll-up); with one it scopes to that server. The tab header consumes this; the
    /// "Agent Not Running" self-alert reads the same <c>agent_status</c> data service-side.
    /// </summary>
    public async Task<List<ViewerAgentStatusRow>> GetAgentStatusAsync(int? serverId = null, CancellationToken cancellationToken = default)
    {
        var sql = BuildAgentStatusSql(serverId.HasValue);
        var clocks = await GetServerClocksAsync(serverId, cancellationToken);

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await ReadAgentStatusRowsAsync(reader, clocks, cancellationToken);
    }

    /// <summary>
    /// Builds <see cref="GetAgentStatusAsync"/>'s SQL text, split out so Darling.Tests can pin both shapes
    /// (fleet-wide and single-server) without a live Postgres. When <paramref name="scopedToServer"/>, $1 is
    /// the server_id. The next run leaves as the server's own wall clock (<c>next_scheduled_run_local</c>);
    /// <see cref="ReadAgentStatusRowsAsync"/> converts it (#4766).
    /// </summary>
    internal static string BuildAgentStatusSql(bool scopedToServer)
    {
        var serverFilter = scopedToServer ? "WHERE a.server_id = $1" : string.Empty;

        return $@"
WITH latest AS (
    SELECT
        a.server_id,
        COALESCE(reg.display_name, a.server_name) AS server_name,
        a.agent_running,
        a.agent_status_desc,
        a.agent_startup_desc,
        a.next_scheduled_run AS next_scheduled_run_local,
        ROW_NUMBER() OVER (PARTITION BY a.server_id ORDER BY a.collection_time DESC) AS rn
    FROM agent_status AS a
    LEFT JOIN servers AS reg ON reg.server_id = a.server_id
    {serverFilter}
)
SELECT
    server_id,
    server_name,
    agent_running,
    agent_status_desc,
    agent_startup_desc,
    next_scheduled_run_local
FROM latest
WHERE rn = 1
ORDER BY server_name";
    }

    /// <summary>
    /// Maps <see cref="BuildAgentStatusSql"/>'s result set. The next scheduled run arrives as the server's
    /// own wall clock and leaves as naive UTC, converted with that row's server clock from
    /// <paramref name="clocks"/> (UTC when it has none).
    /// </summary>
    internal static async Task<List<ViewerAgentStatusRow>> ReadAgentStatusRowsAsync(
        DbDataReader reader, IReadOnlyDictionary<int, ServerClock> clocks, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerAgentStatusRow>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);

            rows.Add(new ViewerAgentStatusRow
            {
                ServerId = serverId,
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                AgentRunning = !reader.IsDBNull(2) && reader.GetBoolean(2),
                AgentStatusDesc = reader.IsDBNull(3) ? null : reader.GetString(3),
                AgentStartupDesc = reader.IsDBNull(4) ? null : reader.GetString(4),
                NextScheduledRunUtc = reader.IsDBNull(5) ? null : ClockFor(clocks, serverId).ToUtc(reader.GetDateTime(5)),
            });
        }

        return rows;
    }
}

/// <summary>
/// The latest SQL Agent status snapshot for one server (issue #1433 Phase 2) — the Darling twin of Lite's
/// <c>AgentStatusRow</c>. Drives the Job History tab header and the "Agent Not Running" self-alert.
/// next_scheduled_run is converted to naive-UTC in C# with the server's clock and rendered in the viewer's
/// local time.
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

    /// <summary>
    /// <see cref="NextScheduledRunUtc"/> as the plain wall time (#4766). It was converted from the server's stored wall
    /// clock, which cannot say which pass of a repeated autumn hour it was, so it never takes the UTC offset that
    /// <see cref="ViewerTimeHelper.FormatForDisplay(DateTime, string)"/> adds for a real instant.
    /// </summary>
    public string NextScheduledRunLocal => NextScheduledRunUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "None scheduled";
}
