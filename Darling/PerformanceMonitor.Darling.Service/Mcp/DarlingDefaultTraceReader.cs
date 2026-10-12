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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side read for the <c>get_default_trace_events</c> MCP tool — the stored Default Trace events the
/// shared collector captured from the monitored server's built-in trace (<c>sys.fn_trace_gettable</c>).
/// A STORED read (no live monitored-server hit) windowed on <c>event_time</c> (the trace StartTime — the
/// event's real time). Reads the BASE <c>default_trace_events</c> table directly (like <c>server_properties</c>,
/// it has no <c>v_*</c> passthrough view — no Lite analysis SQL ports through it); the bare name resolves
/// through the store's <c>search_path = collect, config, public</c>. The SQL is a public const so
/// Darling.Tests can pin the dialect + columns without a live Postgres.
///
/// <para><b>Server-local storage, UTC at the read boundary (load-bearing):</b> unlike the XE collectors
/// (deadlock / system_health), whose <c>event_time</c> is the UTC XE <c>@timestamp</c>, the Default Trace
/// <c>StartTime</c> — and thus this table's <c>event_time</c> — is the monitored server's LOCAL wall-clock
/// time (the .trc files store local time). Storing it raw keeps the collector's dedup watermark bulletproof
/// (local StartTime vs a local watermark, no conversion), which is why the STORED frame stays local and
/// <c>CollectorTimestampFrameTests</c> pins it that way. Each of this column's three readers then converts to
/// naive UTC by the collected <c>server_properties</c> clock (V16 offset, V134 time zone id) — this read in C#
/// through the server's <see cref="ServerClock"/> (<see cref="DarlingServerClockReader"/>), the viewer's
/// <c>ViewerDataService.DefaultTraceEventsByWindowSql</c> the same way (#4766, #4793), and Lite's in C# on the
/// loaded row with one offset. The zone is what keeps an event from before a daylight saving change from
/// coming back an hour off. A server with no offset yet collected reads as UTC (treat local == UTC), and the
/// single-row COALESCE CTE that pre-filters the window guarantees the cross join never drops the events.</para>
///
/// <para><b>Why the returned value is converted and not merely labelled.</b> The surfaces a caller
/// actually correlates these events against are naive UTC — <c>collection_log.collection_time</c>,
/// <c>list_servers.last_collection</c>, the XE <c>event_time</c> columns, and this tool's own
/// <c>as_of</c> argument — so a server-local <c>event_time</c> beside them reads as UTC by default and is
/// wrong by the offset in the direction that INVERTS causality: an event at 04:28 UTC on a server at UTC-4
/// renders as 00:28 and appears to precede the 04:28 collector error it actually coincided with. A suffix
/// or a note would leave that value in the response for a reader to line up against those surfaces anyway.
/// Converting is also what the two sibling readers of this very column already do — the viewer's
/// <c>DefaultTraceEventRow.EventTimeUtc</c>, and Lite's <c>get_default_trace_events</c>, which de-skews to
/// <c>DefaultTraceEventRow.EventTimeUtc</c> — so a second convention here would leave one column read three
/// ways.</para>
/// </summary>
internal static class DarlingDefaultTraceReader
{
    /// <summary>One stored Default Trace event row (the fields the MCP tool surfaces + gates on). The event
    /// time is naive UTC: the read converts the stored server-local StartTime, so it shares the frame of every
    /// other timestamp the MCP surface returns.</summary>
    public sealed record DefaultTraceEventRow(
        DateTime? EventTimeUtc,
        string? EventName,
        int? EventClass,
        int? Spid,
        string? DatabaseName,
        string? LoginName,
        string? HostName,
        string? ApplicationName,
        string? ObjectName,
        string? Filename,
        string? TextData,
        int? ErrorNumber,
        int? Severity,
        long? DurationUs,
        long? IntegerData);

    /// <summary>
    /// Stored Default Trace events for the window, newest first. Windows on <c>event_time</c> (the trace
    /// StartTime), NOT collection_time, so "last 24 hours" means events that HAPPENED in the last 24 hours.
    /// The stored <c>event_time</c> is server-LOCAL and comes back RAW as <c>event_time_local</c>:
    /// <see cref="ReadEventRowsAsync"/> converts each row to naive UTC with the server's
    /// <see cref="ServerClock"/> and applies the exact window to the converted time, so the caller's window, the
    /// caller's <c>as_of</c> and every timestamp in the response are one frame. $1 server_id, $2/$3 window
    /// (naive UTC). Reads the base tables (no v_* views).
    ///
    /// <para>The SQL window is only a PRE-FILTER. It still subtracts the newest collected offset (single-row
    /// COALESCE CTE — 0 when none is collected yet, and the cross join keeps every event), but widens each bound
    /// by an hour, because the offset in force when an event happened can differ from the newest one by an hour
    /// across a daylight saving change (#4793). The read has no LIMIT, so the extra hour drops nothing: the rows
    /// inside the widened window but outside the real one are dropped in C# after the exact conversion. It costs
    /// no index either way: <c>PgSchemaGenerator.CreateIndex</c> gives this table <c>(server_id,
    /// collection_time)</c>, so there is no <c>event_time</c> index for an expression to forfeit.</para>
    ///
    /// <para>$4 is the <see cref="EventWindowFloor"/> for $2, bound against <c>collection_time</c> directly
    /// rather than the converted expression — <c>default_trace_events</c> is a hypertable partitioned on
    /// <c>collection_time</c>, which this event-time window alone gives the planner nothing to exclude a chunk
    /// on (#4229). The real UTC event time is always ≤ <c>collection_time</c> (store UTC at collection), so the
    /// floor cannot drop a qualifying row.</para>
    ///
    /// <para>#5245: $5 is the chosen databases as ONE <c>text[]</c> (SQL NULL for every database, so the statement text
    /// never changes with the selection): <c>database_name = ANY($5)</c>, a residual filter inside the window. An event
    /// with no database name (a server-level event) is not in any chosen database, as on the desktop. Like the
    /// window, it is applied in SQL before the tool's page limit, so the page is the top N of the chosen databases.</para>
    /// </summary>
    public const string EventsByWindowSql = """
        WITH svr AS (
            SELECT COALESCE((
                SELECT sp.utc_offset_minutes
                FROM server_properties AS sp
                WHERE sp.server_id = $1
                AND   sp.utc_offset_minutes IS NOT NULL
                ORDER BY sp.collection_time DESC
                LIMIT 1), 0) AS offset_minutes
        )
        SELECT
            dte.event_time AS event_time_local,
            dte.event_name,
            dte.event_class,
            dte.spid,
            dte.database_name,
            dte.login_name,
            dte.host_name,
            dte.application_name,
            dte.object_name,
            dte.filename,
            dte.text_data,
            dte.error_number,
            dte.severity,
            dte.duration_us,
            dte.integer_data
        FROM default_trace_events AS dte, svr
        WHERE dte.server_id = $1
        AND   dte.event_time - make_interval(mins => svr.offset_minutes) >= $2 - interval '1 hour'
        AND   dte.event_time - make_interval(mins => svr.offset_minutes) <= $3 + interval '1 hour'
        AND   dte.collection_time >= $4
        AND   ($5::text[] IS NULL OR dte.database_name = ANY($5))
        ORDER BY event_time_local DESC
        """;

    /// <summary>Reads the stored Default Trace event rows over the window (newest first), for every database.</summary>
    public static Task<List<DefaultTraceEventRow>> ReadEventsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        ReadEventsAsync(postgres, serverId, startUtc, endUtc, DatabaseFilter.All, cancellationToken);

    /// <summary>
    /// The same read over a SET of databases (#5245): <see cref="DatabaseFilter.All"/> is every database, and the
    /// names are bound once as the <c>text[]</c> <see cref="EventsByWindowSql"/> reads as <c>$5</c>.
    /// </summary>
    public static async Task<List<DefaultTraceEventRow>> ReadEventsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        var clock = await DarlingServerClockReader.ReadAsync(postgres, serverId, cancellationToken);

        await using var command = postgres.CreateCommand(EventsByWindowSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(startUtc));
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await ReadEventRowsAsync(reader, clock, startUtc, endUtc, cancellationToken);
    }

    /// <summary>
    /// Maps <see cref="EventsByWindowSql"/>'s result set. The event time arrives as the server's own wall clock
    /// and leaves as naive UTC, converted with <paramref name="clock"/>; the SQL window is only a pre-filter an
    /// hour wider on each side, so the rows outside [<paramref name="startUtc"/>, <paramref name="endUtc"/>]
    /// once converted are dropped here, and an event with no time cannot be inside a window. The rows come back
    /// newest first by that UTC time; a STABLE sort, so events at the same instant keep the reader's order
    /// (#4793).
    /// </summary>
    internal static async Task<List<DefaultTraceEventRow>> ReadEventRowsAsync(
        DbDataReader reader, ServerClock clock, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var rows = new List<DefaultTraceEventRow>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var eventTimeUtc = reader.IsDBNull(0) ? (DateTime?)null : clock.ToUtc(reader.GetDateTime(0));
            if (eventTimeUtc is not { } utc || utc < startUtc || utc > endUtc)
            {
                continue;
            }

            rows.Add(new DefaultTraceEventRow(
                eventTimeUtc,
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? null : reader.GetInt32(11),
                reader.IsDBNull(12) ? null : reader.GetInt32(12),
                reader.IsDBNull(13) ? null : reader.GetInt64(13),
                reader.IsDBNull(14) ? null : reader.GetInt64(14)));
        }

        /* OrderByDescending is a stable sort (List.Sort is not), so ties keep the reader's order. */
        return rows.OrderByDescending(static r => r.EventTimeUtc).ToList();
    }
}
