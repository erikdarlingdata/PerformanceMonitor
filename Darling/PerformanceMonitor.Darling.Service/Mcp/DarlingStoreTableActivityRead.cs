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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9413: the per-table activity section of <c>get_store_metrics</c> (<c>table_activity = true</c>). It reads
/// <c>collect.store_table_activity</c> (<see cref="StoreTableActivity"/>), the hourly differences of each store table's counters with
/// chunks and compressed chunks rolled up to their hypertable. The window is the <c>hours_back</c> hours ending at <c>as_of</c> (default now), with
/// the same parse and UTC rules as <c>get_store_log</c>. A table with no row in an hour had no activity in it (rows are written only for active tables), and
/// <c>last_capture_at</c> says whether captures are running at all.
/// </summary>
internal static class DarlingStoreTableActivityRead
{
    /// <summary>Default window in hours.</summary>
    public const int DefaultHours = 24;

    /// <summary>The longest window: the shared hours ceiling (7 days of hourly rows).</summary>
    public const int MaxHours = McpHelpers.MaxHoursBack;

    /// <summary>The rankings, each over the window's summed differences.</summary>
    internal static readonly string[] RankKeys = { "rows_written", "blocks_read", "blocks_hit" };

    private static string RankExpression(string rank) => rank switch
    {
        "blocks_read" => "sum(heap_blks_read) + sum(idx_blks_read)",
        "blocks_hit" => "sum(heap_blks_hit) + sum(idx_blks_hit)",
        _ => "sum(tup_inserted) + sum(tup_updated) + sum(tup_deleted)",
    };

    private const string Columns = @"table_schema, table_name, table_kind,
    tup_inserted, tup_updated, tup_hot_updated, tup_deleted,
    heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
    seq_scans, idx_scans, vacuums, analyzes, relations_scanned, relations_active";

    private static string RankedSql(string rank) => @"
SELECT table_schema, table_name, min(table_kind),
    sum(tup_inserted)::bigint, sum(tup_updated)::bigint, sum(tup_hot_updated)::bigint, sum(tup_deleted)::bigint,
    sum(heap_blks_read)::bigint, sum(heap_blks_hit)::bigint, sum(idx_blks_read)::bigint, sum(idx_blks_hit)::bigint,
    sum(seq_scans)::bigint, sum(idx_scans)::bigint, sum(vacuums)::bigint, sum(analyzes)::bigint,
    max(relations_scanned), max(relations_active), count(*)::integer,
    count(*) OVER ()
FROM collect.store_table_activity
WHERE metric_time > $1 AND metric_time <= $2
GROUP BY table_schema, table_name
ORDER BY " + RankExpression(rank) + @" DESC, table_schema, table_name
LIMIT $3";

    private const string SeriesSql = @"
SELECT metric_time, interval_seconds, " + Columns + @"
FROM collect.store_table_activity
WHERE metric_time > $1 AND metric_time <= $2
  AND (table_name = $3 OR table_schema || '.' || table_name = $3)
ORDER BY metric_time DESC, table_schema, table_name";

    private static string Time(DateTime naiveUtc) =>
        DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>
    /// The section. <paramref name="asOf"/> is the window's end as the caller wrote it (null or empty is now), validated by the shared window check.
    /// <paramref name="responseByteBudget"/> null keeps every row.
    /// </summary>
    internal static async Task<string> GetAsync(
        NpgsqlDataSource postgres, int hoursBack, string? asOf, string? rankBy, string? objectName, int limit, int? responseByteBudget,
        CancellationToken cancellationToken)
    {
        var windowError = DarlingReadWindow.Validate(hoursBack, asOf, MaxHours, out var windowEnd);
        if (windowError != null)
        {
            return windowError;
        }

        var rankError = McpHelpers.ValidateChoice(rankBy, RankKeys, "rank_by");
        if (rankError != null)
        {
            return rankError;
        }

        var rank = string.IsNullOrWhiteSpace(rankBy)
            ? RankKeys[0]
            : RankKeys.First(k => string.Equals(k, rankBy.Trim(), StringComparison.OrdinalIgnoreCase));
        var name = string.IsNullOrWhiteSpace(objectName) ? null : objectName.Trim();
        var end = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified);
        var start = end.AddHours(-hoursBack);

        try
        {
            DateTime? lastCapture;
            await using (var stamp = postgres.CreateCommand(StoreTableActivity.LastCaptureSql))
            {
                stamp.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                var value = await stamp.ExecuteScalarAsync(cancellationToken);
                lastCapture = value is DateTime t ? t : null;
            }

            if (lastCapture is null)
            {
                return McpHelpers.Status(
                    "unavailable",
                    "No table activity capture has completed on this store yet. The service captures once an hour with the store self-metrics, and the first capture only "
                    + "records the counters to compare with, so the first activity rows land with the second capture, about an hour later.");
            }

            var rows = new List<Dictionary<string, object?>>();
            var matched = 0;
            if (name is null)
            {
                await using var command = postgres.CreateCommand(RankedSql(rank));
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, start);
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, end);
                command.Parameters.AddWithValue(limit);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    long L(int i) => reader.GetInt64(i);
                    var written = L(3) + L(4) + L(6);
                    rows.Add(new Dictionary<string, object?>
                    {
                        ["table_schema"] = reader.GetString(0),
                        ["table_name"] = reader.GetString(1),
                        ["table_kind"] = reader.GetString(2),
                        ["rows_written"] = written,
                        ["tuples_inserted"] = L(3),
                        ["tuples_updated"] = L(4),
                        ["tuples_hot_updated"] = L(5),
                        ["tuples_deleted"] = L(6),
                        ["blocks_read"] = L(7) + L(9),
                        ["blocks_hit"] = L(8) + L(10),
                        ["heap_blocks_read"] = L(7),
                        ["heap_blocks_hit"] = L(8),
                        ["index_blocks_read"] = L(9),
                        ["index_blocks_hit"] = L(10),
                        ["sequential_scans"] = L(11),
                        ["index_scans"] = L(12),
                        ["vacuums"] = L(13),
                        ["analyzes"] = L(14),
                        ["most_relations_scanned_in_an_hour"] = reader.GetInt32(15),
                        ["most_relations_active_in_an_hour"] = reader.GetInt32(16),
                        ["hours_active"] = reader.GetInt32(17),
                    });
                    matched = (int)reader.GetInt64(18);
                }
            }
            else
            {
                await using var command = postgres.CreateCommand(SeriesSql);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, start);
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, end);
                command.Parameters.AddWithValue(name);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    long L(int i) => reader.GetInt64(i);
                    rows.Add(new Dictionary<string, object?>
                    {
                        ["hour_ending"] = Time(reader.GetDateTime(0)),
                        ["interval_seconds"] = reader.GetInt32(1),
                        ["table_schema"] = reader.GetString(2),
                        ["table_name"] = reader.GetString(3),
                        ["table_kind"] = reader.GetString(4),
                        ["rows_written"] = L(5) + L(6) + L(8),
                        ["tuples_inserted"] = L(5),
                        ["tuples_updated"] = L(6),
                        ["tuples_hot_updated"] = L(7),
                        ["tuples_deleted"] = L(8),
                        ["blocks_read"] = L(9) + L(11),
                        ["blocks_hit"] = L(10) + L(12),
                        ["heap_blocks_read"] = L(9),
                        ["heap_blocks_hit"] = L(10),
                        ["index_blocks_read"] = L(11),
                        ["index_blocks_hit"] = L(12),
                        ["sequential_scans"] = L(13),
                        ["index_scans"] = L(14),
                        ["vacuums"] = L(15),
                        ["analyzes"] = L(16),
                        ["relations_scanned"] = reader.GetInt32(17),
                        ["relations_active"] = reader.GetInt32(18),
                    });
                }

                matched = rows.Count;
            }

            if (rows.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    (name is null ? "No store table had activity" : $"'{name}' had no activity")
                    + $" {McpHelpers.InTheLast(CountText.Plural(hoursBack, "hour"))}. The newest capture was at {Time(lastCapture.Value)}, "
                    + "so captures are running. A table has a row only in an hour it was active.");
            }

            string Reply(int kept) => JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["window_hours"] = hoursBack,
                ["window_start"] = Time(start),
                ["window_end"] = Time(end),
                ["last_capture_at"] = Time(lastCapture.Value),
                ["rank_by"] = name is null ? rank : null,
                ["object_name"] = name,
                [name is null ? "tables_with_activity" : "hours_returned"] = matched,
                [name is null ? "tables" : "series"] = kept < rows.Count ? rows.GetRange(0, kept) : rows,
                ["note"] = "Differences of each store table's counters between captures, with chunks, compressed chunks and partitions rolled up to the table. "
                    + "A table has a row only in an hour it was active. rows_written is inserted plus updated plus deleted. blocks_read and blocks_hit are heap plus index blocks. "
                    + "A counter that went down between two captures (a statistics reset) is counted from the reset, so the hour is a lower bound.",
                ["budget_truncated"] = kept < rows.Count,
            }, McpHelpers.JsonOptions);

            return responseByteBudget is int budget
                ? McpResponseBudget.FitPrefix(rows.Count, Reply, budget, minimum: 0).Json
                : Reply(rows.Count);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return McpHelpers.Status(
                "unavailable",
                "This store has no table activity tables yet: it is below schema version " + StoreTableActivity.Version + ". The service applies the migration when it starts.");
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status(
                "precondition",
                "The role this read uses cannot read collect.store_table_activity. Grant it: GRANT SELECT ON collect.store_table_activity, collect.store_table_activity_baseline TO <the read role>;");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_metrics", ex);
        }
    }
}
