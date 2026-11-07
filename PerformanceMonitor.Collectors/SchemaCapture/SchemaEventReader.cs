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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: one event of the schema-changes session. <see cref="Sequence"/> is the session's <c>event_sequence</c>.
/// <see cref="ObjectId"/> is the object the event names (for an index event, the table), <see cref="RelatedObjectId"/> the parent
/// table of a trigger event. There is no statement text: the session records none.
/// </summary>
public sealed record SchemaEvent(
    string EventName, long Sequence, DateTime TimeUtc, string DatabaseName, int? ObjectId, int? ObjectType, int? IndexId,
    string? ObjectName, string? Login, string? App, string? Host, int? RelatedObjectId);

/// <summary>
/// #7643: what one read of the ring buffer returned. <see cref="SessionCreateTimeUtc"/> is null when the session does not run,
/// <see cref="TotalEventsProcessed"/> is the target's own count of every event it took, and <see cref="XmlTruncated"/> says SQL Server
/// cut the buffer's XML, keeping the oldest events.
/// </summary>
public sealed record SchemaEventBatch(
    DateTime? SessionCreateTimeUtc, long TotalEventsProcessed, bool XmlTruncated, IReadOnlyList<SchemaEvent> Events);

/// <summary>#7643: turns the two result sets of <see cref="SchemaEventQuery"/> into a <see cref="SchemaEventBatch"/>.</summary>
public static class SchemaEventReader
{
    public static async Task<SchemaEventBatch> ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(reader);

        DateTime? createTime = null;
        long total = 0;
        var truncated = false;
        if (await reader.ReadAsync(ct))
        {
            var createOrdinal = reader.GetOrdinal("create_time");
            createTime = reader.IsDBNull(createOrdinal)
                ? null
                : DateTime.SpecifyKind(reader.GetDateTime(createOrdinal), DateTimeKind.Utc);
            total = Long(reader, "total_events_processed") ?? 0;
            truncated = reader.GetBoolean(reader.GetOrdinal("ring_truncated"));
        }

        var events = new List<SchemaEvent>();
        if (await reader.NextResultAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                events.Add(new SchemaEvent(
                    reader.GetString(reader.GetOrdinal("event_name")),
                    Long(reader, "event_sequence") ?? 0,
                    DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("event_time_utc")), DateTimeKind.Utc),
                    Str(reader, "database_name") ?? string.Empty,
                    Int(reader, "object_id"),
                    Int(reader, "object_type"),
                    Int(reader, "index_id"),
                    Str(reader, "object_name"),
                    Str(reader, "login"),
                    Str(reader, "client_app"),
                    Str(reader, "client_host"),
                    Int(reader, "related_object_id")));
            }
        }

        return new SchemaEventBatch(createTime, total, truncated, events);
    }

    private static string? Str(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static int? Int(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static long? Long(DbDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }
}
