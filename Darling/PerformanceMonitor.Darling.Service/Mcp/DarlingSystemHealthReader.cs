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
/// the collected size-stats mapping (the viewer's <see cref="ResolveDatabaseName"/> derivation), since the
/// DB-free shred left it null. Every SQL string is a public const so Darling.Tests can pin the dialect +
/// columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingSystemHealthReader
{
    /// <summary>
    /// Raw event_xml for one XE event type over the tab window, newest first — the viewer's
    /// <c>SystemHealthEventsByTypeSql</c>. Windows on <c>event_time</c> (the event's real time), and on the
    /// category's own <c>event_type</c> ($4). $1 server_id, $2/$3 window (naive UTC), $4 event_type.
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
        ORDER BY event_time DESC
        """;

    /// <summary>
    /// The server's latest database_id↔database_name mapping — the viewer's <c>DatabaseNameMapSql</c>.
    /// <c>DISTINCT ON (database_id)</c> keeps the most-recently-collected name per id (handles a dropped-and-
    /// recreated id). <c>database_size_stats</c> is the source because it is the only collected table carrying
    /// BOTH database_id and database_name for every online DB. Feeds the Severe Errors DB resolution.
    /// $1 server_id.
    /// </summary>
    public const string DatabaseNameMapSql = """
        SELECT DISTINCT ON (database_id)
            database_id,
            database_name
        FROM v_database_size_stats
        WHERE server_id = $1
        ORDER BY database_id, collection_time DESC
        """;

    /// <summary>Reads the raw event_xml blobs for one XE event type over the window (newest first).</summary>
    public static async Task<List<string>> ReadEventXmlAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string eventType, CancellationToken cancellationToken = default)
    {
        var xmls = new List<string>();
        await using var command = postgres.CreateCommand(SystemHealthEventsByTypeSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        DarlingMcpReadParameters.AddText(command, eventType);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            xmls.Add(reader.GetString(0));
        }

        return xmls;
    }

    /// <summary>
    /// Whether this server has EVER recorded a system_health event of one type, ignoring any window.
    /// <para>Lets an empty parse-on-read result say WHICH kind of nothing it found. Zero significant rows
    /// is true both of a healthy window and of a server whose system_health events were never collected,
    /// and the two want opposite responses -- widen the window, versus go find out why nothing is being
    /// captured. Probes <c>v_system_health_events</c>, the SAME source
    /// <see cref="SystemHealthEventsByTypeSql"/> reads, so it cannot report a server as captured for rows
    /// the read itself can never see. Scoped to the event_type because that is the granularity the caller
    /// asked about: a server capturing sp_server_diagnostics but no wait_info has not been sampled for
    /// waits, whatever its other categories hold. LIMIT 1, so it stops at the first row.
    /// $1 server_id, $2 event_type.</para>
    /// </summary>
    public const string HasAnyEventOfTypeSql = """
        SELECT 1
        FROM v_system_health_events
        WHERE server_id = $1
        AND   event_type = $2
        AND   event_xml IS NOT NULL
        LIMIT 1
        """;

    /// <summary>Runs <see cref="HasAnyEventOfTypeSql"/>.</summary>
    public static async Task<bool> HasAnyEventOfTypeAsync(
        NpgsqlDataSource postgres, int serverId, string eventType, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HasAnyEventOfTypeSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, eventType);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>Loads the server's latest database_id → database_name map for Severe Errors DB resolution.</summary>
    public static async Task<Dictionary<int, string>> GetDatabaseNameMapAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var map = new Dictionary<int, string>();
        await using var command = postgres.CreateCommand(DatabaseNameMapSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1))
                continue;
            map[reader.GetInt32(0)] = reader.GetString(1);
        }

        return map;
    }

    /// <summary>
    /// Resolves a severe-error <c>database_id</c> to a display name using the collected mapping — the viewer's
    /// <c>ResolveDatabaseName</c>. A null or 0 id means "no database context" (error_reported often carries
    /// database_id 0; <c>DB_NAME(0)</c> is NULL server-side too) → empty. A real id absent from the map (a
    /// database dropped before the latest size-stats snapshot, or one never captured) is surfaced as its raw
    /// id rather than silently blanked.
    /// </summary>
    public static string ResolveDatabaseName(int? databaseId, IReadOnlyDictionary<int, string> databaseNameMap)
    {
        if (databaseId is not { } id || id == 0)
            return string.Empty;
        if (databaseNameMap.TryGetValue(id, out var name))
            return name;
        return $"database_id {id}";
    }
}
