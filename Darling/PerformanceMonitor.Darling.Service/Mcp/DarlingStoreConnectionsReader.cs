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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The reads behind <c>get_store_connections</c> (#9437), over what <see cref="PerformanceMonitor.Darling.Storage.StoreConnectionSamples"/>
/// records once a minute. Two statements: the minutes (one slot row each, newest first, filters and <c>LIMIT</c> in SQL) and the group rows for
/// the minutes that came back.
/// </summary>
internal static class DarlingStoreConnectionsReader
{
    /// <summary>The ways a minute's group rows can be folded. <c>none</c> keeps the full grain (role, application, state, wait type).</summary>
    public static readonly IReadOnlyList<string> GroupByChoices = new[] { "none", "role", "application", "state" };

    /// <summary>
    /// The minutes. $1 window start, $2 window end (naive UTC, end inclusive), $3 role (or NULL), $4 application name (or NULL), $5 the fetch
    /// size (the caller's limit plus one). With a filter, a minute is kept only when it has a group row that matches both filters, so the
    /// filter decides which minutes the limit counts. The slot row is always the whole minute's, never filtered.
    /// </summary>
    public const string MinutesSql = @"
SELECT
    s.sample_time,
    s.max_connections,
    s.superuser_reserved_connections,
    s.reserved_connections,
    s.backends_total,
    s.slots_available,
    s.host_cpu_percent,
    s.host_available_memory_bytes,
    s.host_disk_queue
FROM collect.store_connection_slots AS s
WHERE s.sample_time >= $1
AND   s.sample_time <= $2
AND   ($3::text IS NULL AND $4::text IS NULL
       OR EXISTS (SELECT 1
                  FROM collect.store_connection_samples AS g
                  WHERE g.sample_time = s.sample_time
                  AND   ($3::text IS NULL OR g.role_name = $3)
                  AND   ($4::text IS NULL OR g.application_name = $4)))
ORDER BY s.sample_time DESC
LIMIT $5";

    /// <summary>
    /// Whether any minute at all lies in the window, with no filter. Tells a window nobody sampled from a filter that matched nothing.
    /// $1 window start, $2 window end.
    /// </summary>
    public const string AnyMinuteSql = @"
SELECT EXISTS (SELECT 1 FROM collect.store_connection_slots AS s WHERE s.sample_time >= $1 AND s.sample_time <= $2)";

    /// <summary>
    /// The group rows. $1 first minute, $2 last minute (both inclusive, taken from the page), $3 role (or NULL), $4 application name (or NULL).
    /// The select list and the grouping come from <see cref="GroupSql"/>, never from a caller's text.
    /// </summary>
    private const string GroupsSqlTemplate = @"
SELECT
    g.sample_time,
    @@COLS@@
    sum(g.backends)::integer AS backends
FROM collect.store_connection_samples AS g
WHERE g.sample_time >= $1
AND   g.sample_time <= $2
AND   ($3::text IS NULL OR g.role_name = $3)
AND   ($4::text IS NULL OR g.application_name = $4)
GROUP BY g.sample_time@@GROUP@@
ORDER BY g.sample_time DESC, sum(g.backends) DESC@@GROUP@@";

    private static string Build(string columns, string grouping) => GroupsSqlTemplate.Replace("@@COLS@@", columns).Replace("@@GROUP@@", grouping);

    /// <summary>The group statement for one <c>group_by</c> choice.</summary>
    internal static string GroupSql(string groupBy) => groupBy switch
    {
        "role" => Build("g.role_name, NULL::text AS application_name, NULL::text AS state, NULL::text AS wait_event_type,", ", g.role_name"),
        "application" => Build("NULL::text AS role_name, g.application_name, NULL::text AS state, NULL::text AS wait_event_type,", ", g.application_name"),
        "state" => Build("NULL::text AS role_name, NULL::text AS application_name, g.state, NULL::text AS wait_event_type,", ", g.state"),
        _ => Build("g.role_name, g.application_name, g.state, g.wait_event_type,", ", g.role_name, g.application_name, g.state, g.wait_event_type"),
    };

    /// <summary>One minute's slot row. The host figures are null when the host reader had none.</summary>
    public sealed record Minute(
        DateTime SampleTime,
        int MaxConnections,
        int SuperuserReserved,
        int Reserved,
        int BackendsTotal,
        int SlotsAvailable,
        double? HostCpuPercent,
        long? HostAvailableMemoryBytes,
        double? HostDiskQueue);

    /// <summary>One group row of a minute. A column the chosen <c>group_by</c> folded away is null.</summary>
    public sealed record Group(DateTime SampleTime, string? Role, string? Application, string? State, string? WaitEventType, int Backends);

    /// <summary>Reads up to <paramref name="fetch"/> minutes, newest first. Pass the caller's limit plus one.</summary>
    public static async Task<IReadOnlyList<Minute>> GetMinutesAsync(
        NpgsqlDataSource postgres, DateTime start, DateTime end, string? role, string? application, int fetch, CancellationToken cancellationToken)
    {
        var minutes = new List<Minute>();
        await using var command = postgres.CreateCommand(MinutesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, start, end);
        AddFilters(command, role, application);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = fetch });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            minutes.Add(new Minute(
                reader.GetDateTime(0),
                reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4), reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetDouble(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetDouble(8)));
        }

        return minutes;
    }

    /// <summary>True when the window holds any sampled minute, whatever the filters.</summary>
    public static async Task<bool> AnyMinuteAsync(NpgsqlDataSource postgres, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(AnyMinuteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, start, end);
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>Reads the group rows of the minutes from <paramref name="first"/> to <paramref name="last"/>, newest first.</summary>
    public static async Task<IReadOnlyList<Group>> GetGroupsAsync(
        NpgsqlDataSource postgres, DateTime first, DateTime last, string? role, string? application, string groupBy, CancellationToken cancellationToken)
    {
        var groups = new List<Group>();
        await using var command = postgres.CreateCommand(GroupSql(groupBy));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, first, last);
        AddFilters(command, role, application);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            groups.Add(new Group(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetInt32(5)));
        }

        return groups;
    }

    private static void AddWindow(NpgsqlCommand command, DateTime start, DateTime end)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
    }

    private static void AddFilters(NpgsqlCommand command, string? role, string? application)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = role is null ? DBNull.Value : role });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = application is null ? DBNull.Value : application });
    }
}
