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
using Npgsql;

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
/// <c>CollectorTimestampFrameTests</c> pins it that way. Every read then de-skews to naive UTC by the
/// collected <c>server_properties.utc_offset_minutes</c> (V16) — the same expression, on the same column,
/// that <c>ViewerDataService.DefaultTraceEventsByWindowSql</c> uses for the System Events tab. A server with
/// no offset yet collected falls back to 0 (treat local == UTC) and the single-row COALESCE CTE guarantees
/// the cross join never drops the events.</para>
///
/// <para><b>Why the returned value is converted and not merely labelled.</b> Every other timestamp an MCP
/// caller can reach is naive UTC — <c>collection_time</c>, <c>list_servers.last_collection</c>, the XE
/// <c>event_time</c> columns, and this tool's own <c>as_of</c> argument — so a server-local
/// <c>event_time</c> beside them reads as UTC by default and is wrong by the offset in the direction that
/// INVERTS causality: an event at 04:28 UTC on a UTC-4 server renders as 00:28 and appears to precede the
/// 04:28 collector error it actually coincided with. A suffix or a note would leave that value in the
/// response for a reader to line up against UTC surfaces anyway. Converting is also what the two sibling
/// readers of this very column already do — the viewer's <c>event_time_utc</c> above, and Lite's
/// <c>get_default_trace_events</c>, which de-skews to <c>DefaultTraceEventRow.EventTimeUtc</c> — so a second
/// convention here would be the only local-frame timestamp on any read surface in the tree.</para>
/// </summary>
internal static class DarlingDefaultTraceReader
{
    /// <summary>One stored Default Trace event row (the fields the MCP tool surfaces + gates on). The event
    /// time is naive UTC: the read de-skews the stored server-local StartTime, so it shares the frame of every
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
    /// The stored <c>event_time</c> is server-LOCAL, so it is de-skewed to naive UTC by the collected
    /// <c>utc_offset_minutes</c> (single-row COALESCE CTE — 0 when none is collected yet, and the cross join
    /// keeps every event) and BOTH returned and windowed as <c>event_time_utc</c>. Returning and bounding on
    /// the same expression is the point: the caller's window, the caller's <c>as_of</c>, and every timestamp
    /// in the response are then one frame. $1 server_id, $2/$3 window (naive UTC). Reads the base tables
    /// (no v_* views).
    ///
    /// <para>The de-skew is spelled on the COLUMN rather than added to the bounds. The two forms select the
    /// same rows — one collected offset applies to both sides, so <c>event_time &gt;= $2 + off</c> and
    /// <c>event_time - off &gt;= $2</c> are algebraically identical — but only this one leaves a UTC value to
    /// return, and it is byte-comparable with the viewer's read of the same column. It costs no index either
    /// way: <c>PgSchemaGenerator.CreateIndex</c> gives this table <c>(server_id, collection_time)</c>, so
    /// there is no <c>event_time</c> index for an expression to forfeit.</para>
    ///
    /// <para>One collected offset covers the whole window, so a window straddling a DST transition de-skews
    /// both sides by the post-transition offset and is off by an hour on the far side. That is the same
    /// single-snapshot approximation the viewer's read and #2992's <c>creation_time</c> de-skew make, and it
    /// is stated here rather than implied.</para>
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
            dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc,
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
        AND   dte.event_time - make_interval(mins => svr.offset_minutes) >= $2
        AND   dte.event_time - make_interval(mins => svr.offset_minutes) <= $3
        ORDER BY event_time_utc DESC
        """;

    /// <summary>Reads the stored Default Trace event rows over the window (newest first).</summary>
    public static async Task<List<DefaultTraceEventRow>> ReadEventsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<DefaultTraceEventRow>();

        await using var command = postgres.CreateCommand(EventsByWindowSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DefaultTraceEventRow(
                reader.IsDBNull(0) ? null : reader.GetDateTime(0),
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

        return rows;
    }
}
