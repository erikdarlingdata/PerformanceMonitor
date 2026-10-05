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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side read for the running-jobs MCP tool (<see cref="DarlingMcpJobTools"/>) — the SAME
/// <c>running_jobs</c> data Lite's <c>get_running_jobs</c> and the viewer's <c>ViewerDataService.RunningJobs</c>
/// read, adapted here so the MCP host never references the WPF viewer project. A STORED read of the latest
/// snapshot only (the <c>collection_time = MAX(...)</c> self-subquery), on the <c>v_running_jobs</c> passthrough
/// view. Every derived column (avg / p95 / percent-of-average / is-running-long) is collector-side, so the read
/// is a plain projection; the human-readable duration strings are formatted in the tool. Every SQL string is a
/// public const so Darling.Tests can pin the dialect + columns without a live Postgres.
///
/// <para><b>Server-local storage, UTC at the read boundary (load-bearing):</b> <c>running_jobs.start_time</c>
/// is the msdb Agent's LOCAL wall clock — <c>RunningJobsCollector</c> ships <c>ja.start_execution_date</c>
/// verbatim and computes <c>current_duration_seconds</c> against <c>GETDATE()</c> on the next line, so the
/// collector's own arithmetic is local-vs-local and the STORED frame has to stay local. The SQL returns it as
/// stored and <see cref="MapRunningJobRow"/> converts it to naive UTC with the server's <see cref="ServerClock"/>
/// (<see cref="DarlingServerClockReader"/>) — the same conversion <c>DarlingDefaultTraceReader</c> and
/// <c>ViewerDataService.SystemEvents</c> use: the server's time zone where SQL Server reports one, else the newest
/// collected <c>server_properties.utc_offset_minutes</c> (V16). A server with no offset yet collected reads as
/// UTC (treat local == UTC).</para>
///
/// <para><b>Why the returned value is converted and not merely labelled.</b> The tool emits
/// <c>collection_time</c> in the same payload and that is naive UTC, so a server-local <c>start_time</c>
/// beside it makes a job that started seconds ago read as having started by the server's whole offset earlier
/// — 4 hours on the production fleet, which is a long-running-job alert's exact signature. A suffix or a note
/// would leave the wrong value in the response for a reader to line up against UTC surfaces anyway. The
/// payload field name is unchanged; only the frame is.</para>
/// </summary>
internal static class DarlingJobReader
{
    /// <summary>One currently-running SQL Agent job with its historical duration comparison. The start time is
    /// naive UTC: the read converts the stored msdb-local value, so it shares the frame of
    /// <c>CollectionTime</c> on the same row.</summary>
    public sealed record RunningJobRow(
        DateTime CollectionTime, string JobName, string JobId, bool JobEnabled, DateTime StartTimeUtc,
        long CurrentDurationSeconds, long AvgDurationSeconds, long P95DurationSeconds, long SuccessfulRunCount,
        bool IsRunningLong, decimal? PercentOfAverage);

    /// <summary>
    /// The latest running-jobs snapshot for one server — Lite's <c>GetRunningJobsAsync</c> / the viewer's
    /// <c>RunningJobsSql</c>: the newest collection only, longest-running first. The columns are exactly those
    /// the alert engine's <c>DarlingAlertReadAdapter.AnomalousJobsSql</c> reads plus the display fields, with
    /// <c>start_time</c> unconverted (the Agent's local clock; <see cref="MapRunningJobRow"/> converts it to naive
    /// UTC). The snapshot self-subquery stays on the naive-UTC <c>collection_time</c>, so which snapshot counts as
    /// latest does not depend on the clock, and the ordering stays on the collector-computed duration rather
    /// than on either clock. $1 server_id.
    ///
    /// <para>The conversion follows the server's time zone, so a job that started before a daylight saving
    /// change lands at its real UTC time rather than an hour off (#4793). Before that it subtracted the ONE
    /// newest collected offset, which was right only for a job that started after the last change.</para>
    /// </summary>
    public const string RunningJobsSql = """
        SELECT
            collection_time,
            job_name,
            job_id,
            job_enabled,
            start_time,
            current_duration_seconds,
            avg_duration_seconds,
            p95_duration_seconds,
            successful_run_count,
            is_running_long,
            percent_of_average
        FROM v_running_jobs
        WHERE server_id = $1
        AND   collection_time = (
            SELECT MAX(collection_time)
            FROM v_running_jobs
            WHERE server_id = $1
        )
        ORDER BY current_duration_seconds DESC
        """;

    public static async Task<List<RunningJobRow>> GetRunningJobsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<RunningJobRow>();
        var clock = await DarlingServerClockReader.ReadAsync(postgres, serverId, cancellationToken);
        await using var command = postgres.CreateCommand(RunningJobsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(MapRunningJobRow(reader, clock));
        }

        return rows;
    }

    /// <summary>Maps one row of <see cref="RunningJobsSql"/> (11 columns, in the SELECT's order).</summary>
    internal static RunningJobRow MapRunningJobRow(DbDataReader reader, ServerClock clock) =>
        new(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? "" : reader.GetString(1),
            reader.IsDBNull(2) ? "" : reader.GetString(2),
            !reader.IsDBNull(3) && reader.GetBoolean(3),
            clock.ToUtc(reader.GetDateTime(4)),
            reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
            reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
            reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
            reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
            !reader.IsDBNull(9) && reader.GetBoolean(9),
            reader.IsDBNull(10) ? null : reader.GetDecimal(10));

    /// <summary>
    /// The newest <c>agent_status</c> row of every enabled server that has one, or of the one server asked for ($1;
    /// NULL for every server). A per-server <c>LATERAL</c> reads one row off the (server_id, collection_time) index, so
    /// the cost is one index probe per server rather than a walk of the table's retained history. A server with no row
    /// (a PostgreSQL target, or an Agent collector that has not run) is simply absent. <c>next_scheduled_run</c> is the
    /// server's own wall clock; <see cref="ReadLatestAgentStatesAsync"/> converts it to UTC.
    /// </summary>
    public const string LatestAgentStatusSql = """
        SELECT
            s.server_id,
            COALESCE(s.display_name, s.server_name),
            a.agent_running,
            a.agent_status_desc,
            a.next_scheduled_run,
            a.collection_time
        FROM servers AS s
        CROSS JOIN LATERAL
        (
            SELECT x.agent_running, x.agent_status_desc, x.next_scheduled_run, x.collection_time
            FROM agent_status AS x
            WHERE x.server_id = s.server_id
            ORDER BY x.collection_time DESC
            LIMIT 1
        ) AS a
        WHERE s.is_enabled
        AND   ($1::int IS NULL OR s.server_id = $1)
        ORDER BY COALESCE(s.display_name, s.server_name), s.server_id
        """;

    /// <summary>The description served for an Agent whose newest snapshot is older than the staleness window.</summary>
    public const string AgentUnknownDescription = "unknown (no recent status)";

    /// <summary>The description served for a server whose newest snapshot found no SQL Agent service at all (the
    /// collector stores <c>agent_running = false</c> with NULL descriptions then: Express, an Agent-off container). That
    /// is not a stopped Agent, and the page draws it as a neutral line, so the page quotes this text.</summary>
    public const string NoAgentServiceDescription = "no SQL Agent service found";

    /// <summary>The live staleness window of the Agent Not Running self-alert: the <c>collection_stale_minutes</c> setting
    /// the alert engine reads (<c>update_alert_settings</c> / <c>get_alert_settings</c> report the same column), so the
    /// page and the alert judge a snapshot's age by one number.</summary>
    public const string CollectionStaleMinutesSql = "SELECT collection_stale_minutes FROM config_alert_settings WHERE id = 1";

    /// <summary>The setting's bounds, as <c>DarlingAlertSettings.CollectionStaleMinutes</c> clamps it.</summary>
    internal const int StaleMinutesMin = 5;
    internal const int StaleMinutesMax = 1440;

    /// <summary>The window for a stored setting: the clamp the engine applies, or the shipped default when the store holds none.</summary>
    internal static TimeSpan StaleWindowFor(int? storedMinutes)
    {
        if (storedMinutes is null) return DarlingSelfAlertEvaluator.StaleWindow;
        return TimeSpan.FromMinutes(Math.Clamp(storedMinutes.Value, StaleMinutesMin, StaleMinutesMax));
    }

    /// <summary>The window the Agent Not Running alert judges a snapshot against right now. A store that has not seeded the
    /// settings row (or predates the column) answers with the shipped default, which is what the engine runs on then too.</summary>
    public static async Task<TimeSpan> ReadStaleWindowAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = postgres.CreateCommand(CollectionStaleMinutesSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return StaleWindowFor(value is null or DBNull ? null : Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture));
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UndefinedColumn or PostgresErrorCodes.UndefinedTable)
        {
            return DarlingSelfAlertEvaluator.StaleWindow;
        }
    }

    /// <summary>One server's SQL Agent state as <c>get_job_history</c> reports it. <paramref name="AgentRunning"/> is null
    /// when the state is not known: no recent snapshot, no Agent service, or a snapshot that did not say.</summary>
    public sealed record AgentState(
        string Server, bool? AgentRunning, string? AgentStatusDesc, DateTime? NextRunUtc, DateTime CapturedAtUtc);

    /// <summary>
    /// Turns a stored snapshot into the state served. A snapshot is judged only while it is fresh, against the alert's live
    /// window (<paramref name="staleWindow"/>, from <see cref="ReadStaleWindowAsync"/>) and with the comparison the "Agent
    /// Not Running" self-alert uses: an older one says nothing about the Agent now, so it reads as unknown with no next run,
    /// never as the last value seen. A fresh row of <c>agent_running = false</c> with no description is the collector's
    /// "no Agent service row" answer, which reads as <see cref="NoAgentServiceDescription"/> with no running flag: that
    /// server has no Agent to stop, and the alert stays silent for it on purpose.
    /// </summary>
    internal static AgentState ResolveAgentState(
        string server, bool? running, string? statusDesc, DateTime? nextRunUtc, DateTime capturedAtUtc, DateTime nowUtc, TimeSpan staleWindow)
    {
        if (nowUtc - capturedAtUtc >= staleWindow)
            return new AgentState(server, null, AgentUnknownDescription, null, capturedAtUtc);
        if (running == false && statusDesc is null)
            return new AgentState(server, null, NoAgentServiceDescription, null, capturedAtUtc);
        return new AgentState(server, running, statusDesc, nextRunUtc, capturedAtUtc);
    }

    /// <summary>A fault a test arms for the current async flow, thrown ahead of the Agent read to prove the runs survive it.</summary>
    internal static readonly System.Threading.AsyncLocal<Exception?> AgentReadFaultForTests = new();

    /// <summary>The latest Agent state of one server (<paramref name="serverId"/>) or of every enabled server that has a
    /// snapshot, ordered by name. <paramref name="nowUtc"/> is the instant the snapshots are judged against.</summary>
    public static async Task<List<AgentState>> ReadLatestAgentStatesAsync(
        NpgsqlDataSource postgres, int? serverId, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        if (AgentReadFaultForTests.Value is { } fault) throw fault;
        var staleWindow = await ReadStaleWindowAsync(postgres, cancellationToken);
        var clocks = await DarlingServerClocksReader.GetAsync(postgres, serverId, McpCommandDeadlines.ReadSeconds, cancellationToken);
        var states = new List<AgentState>();
        await using var command = postgres.CreateCommand(LatestAgentStatusSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = (object?)serverId ?? DBNull.Value });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.GetInt32(0);
            states.Add(ResolveAgentState(
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : DarlingServerClocksReader.ClockFor(clocks, id).ToUtc(reader.GetDateTime(4)),
                DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc),
                nowUtc,
                staleWindow));
        }

        return states;
    }

    /// <summary>Lite/Dashboard's job-duration display formatting (Xs / Xm Ys / Xh Ym).</summary>
    public static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}
