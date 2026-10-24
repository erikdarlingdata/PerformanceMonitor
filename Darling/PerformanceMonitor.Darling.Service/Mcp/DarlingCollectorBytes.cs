/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6629: the network bytes each collector received and sent, per day, for <c>get_collection_health</c> and
/// <c>get_collector_cost</c>.
///
/// <para><b>Where the figures come from.</b> <c>collection_log.bytes_received</c> / <c>bytes_sent</c>, one pair per run (V219), read
/// the way <c>CollectionHealthStatements.ComposedSql</c> reads the same log: closed hours the rollup holds
/// (<c>collection_log_hourly.bytes_*_sum</c>), plus the raw log for the partial first hour and for everything after the newest rolled
/// hour. A read of the raw log over a week would sort tens of thousands of rows per call. The rollup keeps about 7 days, so a longer
/// window is cut to 7 here and the reply says so.</para>
///
/// <para><b>Per day.</b> The window's sum divided by the window's length in days, so a collector that runs every 15 minutes and one
/// that runs hourly are comparable. A collector whose every row in the window is NULL (a PostgreSQL target, a build before V219, a
/// connection that never opened) is "not measured", never 0.</para>
/// </summary>
internal static class DarlingCollectorBytes
{
    /// <summary>The longest window the hourly rollup can answer.</summary>
    internal const int MaxWindowDays = 7;

    internal const string Measured = "measured";

    internal const string NotMeasured = "not measured";

    internal const int TopCount = 5;

    /// <summary>One collector's sums over the window; null when no run in the window measured it.</summary>
    internal sealed record Row(string Collector, long? Received, long? Sent);

    /// <summary>
    /// $1 server_id (NULL for every enabled server), $2 window start (naive UTC). One row per collector.
    /// </summary>
    internal static readonly string Sql = $@"
WITH w AS
(
    SELECT $2::timestamp AS since,
           CASE WHEN date_trunc('hour', $2::timestamp) = $2::timestamp
                THEN $2::timestamp
                ELSE date_trunc('hour', $2::timestamp) + interval '1 hour'
           END AS first_hour
),
rolled AS MATERIALIZED
(
    SELECT h.collector_name, h.hour_start, h.bytes_received_sum AS received, h.bytes_sent_sum AS sent
    FROM collect.collection_log_hourly AS h
    CROSS JOIN w
    WHERE h.hour_start >= w.first_hour
    AND   (($1::integer IS NULL AND h.server_id IN (SELECT c.server_id FROM config.config_monitored_servers AS c WHERE c.is_enabled))
           OR h.server_id = $1::integer)
),
raw AS
(
    SELECT l.collector_name, l.bytes_received AS received, l.bytes_sent AS sent
    FROM collect.collection_log AS l
    CROSS JOIN w
    WHERE l.collection_time >= w.since
    AND   (l.collection_time < w.first_hour
           OR l.collection_time >= COALESCE((SELECT MAX(r.hour_start) FROM rolled AS r) + interval '1 hour', w.first_hour))
    AND   l.bytes_received IS NOT NULL
    AND   l.{CollectionCycleRow.NotCycleRowSql}
    AND   (($1::integer IS NULL AND l.server_id IN (SELECT c.server_id FROM config.config_monitored_servers AS c WHERE c.is_enabled))
           OR l.server_id = $1::integer)
)
SELECT p.collector_name, SUM(p.received)::bigint, SUM(p.sent)::bigint
FROM
(
    SELECT collector_name, received, sent FROM rolled
    UNION ALL
    SELECT collector_name, received, sent FROM raw
) AS p
GROUP BY p.collector_name;";

    /// <summary>The window the bytes are read over: the caller's, cut to what the rollup keeps.</summary>
    internal static double WindowDays(double requestedDays) => Math.Clamp(requestedDays, 1.0 / 24.0, MaxWindowDays);

    internal static async Task<List<Row>> ReadAsync(
        NpgsqlDataSource postgres, int? serverId, DateTime nowUtc, double windowDays, CancellationToken cancellationToken)
    {
        var since = DateTime.SpecifyKind(nowUtc.AddDays(-windowDays), DateTimeKind.Unspecified);
        await using var command = postgres.CreateCommand(Sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId.HasValue ? serverId.Value : DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = since });

        var rows = new List<Row>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2)));
        }

        return rows;
    }

    /// <summary>A sum scaled to one day, or null when nothing was measured.</summary>
    internal static long? PerDay(long? sum, double windowDays) =>
        sum.HasValue ? (long)Math.Round(sum.Value / windowDays) : null;

    /// <summary>
    /// Adds the bytes to a reply whose <c>collectors</c> array names each collector under <paramref name="nameKey"/>: per collector
    /// <c>bytes_received_per_day</c>, <c>bytes_sent_per_day</c> (null when not measured) and <c>bytes_status</c>, and a top-level
    /// <c>network_bytes</c> block with the window and the top collectors by bytes received per day. A reply with no
    /// <c>collectors</c> array is returned as it is.
    /// </summary>
    internal static string Merge(string json, IReadOnlyList<Row> rows, double windowDays, string nameKey, bool explicitStatus)
    {
        var root = JsonNode.Parse(json)!;
        if (root is not JsonObject top || top["collectors"] is not JsonArray collectors)
        {
            return json;
        }

        var byName = rows.ToDictionary(r => r.Collector, StringComparer.Ordinal);
        foreach (var node in collectors)
        {
            if (node is JsonObject row && row[nameKey] is JsonValue name && name.TryGetValue<string>(out var collector))
            {
                AddFields(row, byName.GetValueOrDefault(collector), windowDays, explicitStatus);
            }
        }

        var unmeasured = collectors.Count(n => n is JsonObject o && o[nameKey] is JsonValue v && v.TryGetValue<string>(out var c)
            && !(byName.TryGetValue(c, out var r) && r.Received.HasValue));
        top["network_bytes"] = Block(rows, windowDays, unmeasured);
        return root.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The same fields for a reply about ONE collector, at the top level, with the window they cover.</summary>
    internal static string MergeSingle(string json, IReadOnlyList<Row> rows, string collector, double windowDays)
    {
        if (JsonNode.Parse(json) is not JsonObject top)
        {
            return json;
        }

        AddFields(top, rows.FirstOrDefault(r => string.Equals(r.Collector, collector, StringComparison.Ordinal)), windowDays, explicitStatus: true);
        top["bytes_window_days"] = Math.Round(windowDays, 2);
        return top.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// Adds <c>bytes_received</c> and <c>bytes_sent</c> to the <c>runs</c> of a <c>get_collection_log</c> page, in page order. A run
    /// that measured nothing gets null for both, never 0.
    /// </summary>
    internal static string WithRunBytes(string json, IEnumerable<(long? Received, long? Sent)> bytes)
    {
        var list = bytes.ToList();
        if (JsonNode.Parse(json) is not JsonObject top || top["runs"] is not JsonArray runs)
        {
            return json;
        }

        for (var i = 0; i < runs.Count && i < list.Count; i++)
        {
            if (runs[i] is JsonObject run)
            {
                run["bytes_received"] = list[i].Received;
                run["bytes_sent"] = list[i].Sent;
            }
        }

        return top.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// A measured collector carries its per-day figures. An unmeasured one carries nothing when <paramref name="explicitStatus"/> is false
    /// (<c>get_collection_health</c>, whose reply is held to a size budget and counts them in <c>network_bytes.collectors_not_measured</c>),
    /// and null figures with <c>bytes_status</c> "not measured" when it is true. Never 0.
    /// </summary>
    private static void AddFields(JsonObject target, Row? row, double windowDays, bool explicitStatus)
    {
        var received = PerDay(row?.Received, windowDays);
        if (received.HasValue)
        {
            target["bytes_received_per_day"] = received;
            target["bytes_sent_per_day"] = PerDay(row!.Sent, windowDays);
            if (explicitStatus)
            {
                target["bytes_status"] = Measured;
            }
        }
        else if (explicitStatus)
        {
            target["bytes_received_per_day"] = null;
            target["bytes_sent_per_day"] = null;
            target["bytes_status"] = NotMeasured;
        }
    }

    private static JsonObject Block(IReadOnlyList<Row> rows, double windowDays, int unmeasured)
    {
        var topCollectors = new JsonArray();
        foreach (var r in rows
            .Where(r => r.Received.HasValue)
            .OrderByDescending(r => r.Received!.Value)
            .ThenBy(r => r.Collector, StringComparer.Ordinal)
            .Take(TopCount))
        {
            topCollectors.Add(new JsonObject
            {
                ["collector"] = r.Collector,
                ["bytes_received_per_day"] = PerDay(r.Received, windowDays),
                ["bytes_sent_per_day"] = PerDay(r.Sent, windowDays),
            });
        }

        return new JsonObject
        {
            ["window_days"] = Math.Round(windowDays, 2),
            ["unit"] = "bytes per day; a collector with no figure is not measured, never 0",
            ["collectors_not_measured"] = unmeasured,
            ["top_collectors_by_bytes_received_per_day"] = topCollectors,
        };
    }
}
