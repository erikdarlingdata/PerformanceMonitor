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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the system_health parse-on-read MCP tools (<see cref="DarlingMcpHealthParserTools"/>)
/// — the SAME stored <c>system_health_events</c> the viewer's System Events tab reads
/// (<c>ViewerDataService.SystemEvents</c>), adapted here so the MCP host never references the WPF viewer
/// project. Each read is a STORED read (no live monitored-server hit): the raw <c>event_xml</c> for one XE
/// event type over the window is fetched from <c>v_system_health_events</c>, then the shared
/// <see cref="SystemHealthParser"/> (PerformanceMonitor.Common — Stage 2a, NOT re-implemented here) shreds
/// each blob into the category record, and <see cref="SystemHealthSignificance"/> (Stage 2b) keeps
/// only the SIGNIFICANT rows. This is exactly the viewer's pipeline; the raw table + the shared parser are
/// the whole thing (no persisted parsed tables).
///
/// <para>
/// The reads window on <c>event_time</c> (the XE <c>@timestamp</c> — the event's real time, which for the
/// ring-buffer categories can lag when it was collected), NOT <c>collection_time</c>, so "last 24 hours"
/// means events that happened in the last 24 hours — the viewer's deliberate choice (the sibling Dashboard
/// windows on collection_time). Bounds bind naive-UTC (Kind=Unspecified → the store's
/// <c>timestamp without time zone</c> columns). Severe-error <c>database_id</c> is resolved to a name from
/// the collected size-stats history (<see cref="DatabaseNameHistory"/>: the name the id carried at the error's time), since the
/// DB-free shred left it null. Every SQL string is a public const so Darling.Tests can pin the dialect +
/// columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingSystemHealthReader
{
    /// <summary>
    /// Raw event_xml for one XE event type over the tab window, newest first — the viewer's
    /// <c>SystemHealthEventsByTypeSql</c>. Windows on <c>event_time</c> (the event's real time), and on the
    /// category's own <c>event_type</c> ($4). $1 server_id, $2/$3 window (naive UTC), $4 event_type. $5 is the
    /// <see cref="EventWindowFloor"/> for $2 — <c>v_system_health_events</c> is a hypertable partitioned on
    /// <c>collection_time</c>, which this event-time window alone gives the planner nothing to exclude a chunk
    /// on (#4229); the floor lets it skip every chunk older than the window, without being able to drop a row
    /// (an event is collected after it happens).
    /// </summary>
    public const string SystemHealthEventsByTypeSql = """
        SELECT
            event_xml
        FROM v_system_health_events
        WHERE server_id = $1
        AND   event_time >= $2
        AND   event_time <= $3
        AND   event_type = $4
        AND   event_xml IS NOT NULL
        AND   collection_time >= $5
        ORDER BY event_time DESC
        """;

    /// <summary>Reads the raw event_xml blobs for one XE event type over the window (newest first).</summary>
    public static async Task<List<string>> ReadEventXmlAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string eventType, CancellationToken cancellationToken = default)
    {
        var xmls = new List<string>();
        await using var command = postgres.CreateCommand(SystemHealthEventsByTypeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        DarlingMcpReadParameters.AddText(command, eventType);
        DarlingMcpReadParameters.AddTimestamp(command, EventWindowFloor.For(startUtc));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            xmls.Add(reader.GetString(0));
        }

        return xmls;
    }

    /// <summary>
    /// The newest <c>collection_time</c> at which the system_health collector stored ANY event for this
    /// server — the source witness every one of the nine parse-on-read tools publishes (#3541 A12, contract
    /// rule 5: zero is a measurement).
    /// <para>Eight of the nine tools answered a dead <c>system_health</c> session, or a collector that had
    /// never run, with the same <c>empty</c> a healthy quiet window earns, and "no severe errors" from a
    /// server nothing was ever read from is a clean bill of health nobody issued. The witness is the
    /// events view itself, NOT <c>collection_log</c>: the log records a SUCCESS for a run that read a dead
    /// session and stored nothing, which is exactly the shape being mis-reported, whereas a stored event is
    /// proof the session was alive and the collector reached it. Windowless and type-less on purpose — it
    /// answers "has this server's ring buffer ever been read into the store", which is the question a
    /// category with no rows in the window needs answered first; the type-scoped question is
    /// <see cref="LastCaptureOfTypeSql"/>. Reads the SAME view the tools read, for the #2484 reason: a
    /// probe on another relation could report a source as observed for rows the read itself can never
    /// see.</para>
    /// <para>Cheap by shape: <c>MAX(collection_time)</c> under <c>server_id = $1</c> is a backward walk of the
    /// <c>(server_id, collection_time)</c> index that stops at the first row, so it rides on the data path
    /// of every call and not only on the empty one. Anchored by construction — it names no clock; it is a
    /// fact about the store, and a caller anchored in the past receives the store's newest capture,
    /// which may be later than its window and is labelled as the collector's, not the window's.</para>
    /// <para>$1 server_id.</para>
    /// </summary>
    public const string LastCaptureSql = """
        SELECT MAX(collection_time)
        FROM v_system_health_events
        WHERE server_id = $1
        AND   event_xml IS NOT NULL
        """;

    /// <summary>
    /// The newest <c>collection_time</c> at which an event of ONE type was stored for this server — the
    /// type-scoped half of the witness, run only when a window came back with no events of that type.
    /// <para>Separates "this category has fired before, the window is quiet" (widen) from "this category
    /// has never fired here while the session IS being read" — which for a rare category (a memory-node
    /// OOM, a severe error) is the healthy measurement, not a blind spot. Same view as the read, same
    /// <c>event_xml IS NOT NULL</c> guard, same backward index walk with a type filter — it stops at the
    /// first match for a type that exists and walks the server's rows for one that never did, the cost the
    /// #2484 <c>HasAnyEventOfTypeSql</c> probe this replaces already paid on the same path (that probe
    /// answered only yes/no; this one also says WHEN, which is what the message needs).</para>
    /// <para>$1 server_id, $2 event_type.</para>
    /// </summary>
    public const string LastCaptureOfTypeSql = """
        SELECT MAX(collection_time)
        FROM v_system_health_events
        WHERE server_id = $1
        AND   event_type = $2
        AND   event_xml IS NOT NULL
        """;

    /// <summary>
    /// #6441: the note the collector's NEWEST run left, when that run succeeded (NULL otherwise). A truncated ring
    /// buffer is recorded only there (<see cref="RingBufferTruncationNote"/>). One backward step down
    /// <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>.
    /// <para>$1 server_id, $2 collector_name.</para>
    /// </summary>
    public const string LatestRunNoteSql = """
        SELECT CASE WHEN status = 'SUCCESS' THEN error_message END
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// #6827: the candidate truncated ring-buffer runs of the collector that read one read's window [$3, $4] (inclusive,
    /// naive UTC), oldest first: each run's collection_time and note. A window is blind if ANY run that read it was
    /// truncated, however clean the run at its end, and a clean past window is not made blind by today's run. Those runs
    /// are the ones inside the window PLUS the first SUCCESS run after $4: a truncated run hides events, and the hidden
    /// ones can fall in the window's last minutes, so the run just after the window end has to be asked too. A live read
    /// has no run after its end, so it asks the runs inside the window only. Only SUCCESS runs count, as in
    /// <see cref="LatestRunNoteSql"/>.
    /// <para>The regex is only a pre-filter, built from <see cref="RingBufferTruncationNote.TruncatedLabel"/>: a note
    /// carrying the label with a value above zero somewhere. It is looser than
    /// <see cref="RingBufferTruncationNote.TryParse"/> (a substring, not a whole token), so
    /// <see cref="RingBufferTruncationNote.TruncatedStretch.FromRuns"/> decides every returned row by TryParse.</para>
    /// Rides the same <c>idx_collection_log_watermark</c> range.
    /// <para>$1 server_id, $2 collector_name, $3 window start, $4 window end.</para>
    /// </summary>
    public const string TruncatedRunsInWindowSql = $$"""
        SELECT collection_time,
               error_message
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        AND   collection_time >= $3
        AND   collection_time <= COALESCE(
              (
                  SELECT MIN(a.collection_time)
                  FROM v_collection_log AS a
                  WHERE a.server_id = $1
                  AND   a.collector_name = $2
                  AND   a.collection_time > $4
                  AND   a.status = 'SUCCESS'
              ), $4)
        AND   status = 'SUCCESS'
        AND   error_message ~ '{{RingBufferTruncationNote.TruncatedLabel}}=0*[1-9]'
        ORDER BY collection_time
        """;

    /// <summary>Runs <see cref="TruncatedRunsInWindowSql"/>: null when no run that read the window logged a truncated ring buffer.</summary>
    public static async Task<RingBufferTruncationNote.TruncatedStretch?> GetTruncatedRunsInWindowAsync(
        NpgsqlDataSource postgres, int serverId, string collectorName, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(TruncatedRunsInWindowSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, collectorName);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var runs = new List<(DateTime CollectionTime, string? Note)>();
        while (await reader.ReadAsync(cancellationToken))
            runs.Add((reader.GetDateTime(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        return RingBufferTruncationNote.TruncatedStretch.FromRuns(runs);
    }

    /// <summary>Runs <see cref="LatestRunNoteSql"/>: null when the collector never ran or its newest run left no note.</summary>
    public static async Task<string?> GetLatestRunNoteAsync(
        NpgsqlDataSource postgres, int serverId, string collectorName, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(LatestRunNoteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, collectorName);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>Runs <see cref="LastCaptureSql"/>: null when no system_health event of any type has ever
    /// been stored for the server.</summary>
    public static Task<DateTime?> GetLastCaptureAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => ReadNullableTimestampAsync(postgres, LastCaptureSql, serverId, eventType: null, cancellationToken);

    /// <summary>Runs <see cref="LastCaptureOfTypeSql"/>: null when no event of <paramref name="eventType"/>
    /// has ever been stored for the server.</summary>
    public static Task<DateTime?> GetLastCaptureOfTypeAsync(
        NpgsqlDataSource postgres, int serverId, string eventType, CancellationToken cancellationToken = default)
        => ReadNullableTimestampAsync(postgres, LastCaptureOfTypeSql, serverId, eventType, cancellationToken);

    private static async Task<DateTime?> ReadNullableTimestampAsync(
        NpgsqlDataSource postgres, string sql, int serverId, string? eventType, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        if (eventType is not null)
            DarlingMcpReadParameters.AddText(command, eventType);
        /* MAX over zero rows is one row holding SQL NULL, which Npgsql surfaces as DBNull — the aggregate
           never returns no rows, so the null check is on the value rather than on the row. */
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime stamp ? stamp : null;
    }
}
