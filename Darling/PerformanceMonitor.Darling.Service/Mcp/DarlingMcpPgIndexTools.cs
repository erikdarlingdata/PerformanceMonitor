/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for MEASURED index bloat and for column distribution statistics — the two PostgreSQL
/// reads that answer "why is this plan shaped like that", paired with the <c>pg_index_bloat</c> and
/// <c>pg_column_stats</c> collectors (#2629).
///
/// <para>
/// <c>get_pg_index_bloat</c> is the measured counterpart of <c>get_pg_table_bloat</c>, and the difference
/// is the whole point: table bloat is ESTIMATED from statistics and is suppressed when those statistics
/// cannot be trusted, while this reads <c>pgstatindex</c>, which walks the index. That costs real I/O, so
/// the collector measures only the largest indexes per cycle and LABELS the rest rather than dropping
/// them — a row carrying <c>skipped_reason</c> is a real index that was not measured, not a healthy one.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgIndexTools
{
    [McpServerTool(Name = "get_pg_index_bloat"), Description("Gets MEASURED PostgreSQL index bloat from the pgstattuple extension: average leaf density, leaf fragmentation, empty and deleted pages, and how many bytes a REINDEX could plausibly reclaim. This is measured by walking the index, not estimated - contrast get_pg_table_bloat, which estimates from statistics. Low avg_leaf_density is the bloat signal: a freshly built btree is around 90%, and an index that has churned heavily falls well below that. Because measuring costs real I/O the collector measures only the largest indexes each cycle and LABELS the others, so a row with a skipped_reason is an index that was NOT measured rather than one that is healthy - never read a missing measurement as a clean bill of health.")]
    public static async Task<string> GetPgIndexBloat(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_index_bloat")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_index_bloat")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No index bloat measurements for {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). This collector runs DAILY, so a window shorter than a day can be empty "
                        + "on a perfectly healthy server — widen it before concluding anything.");
            }

            var truncated = rows.Count >= limit;
            var measured = rows.Count(r => r.SkippedReason is null);

            var indexes = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                index_name = r.IndexName,
                index_bytes = r.IndexBytes,
                index_mb = Math.Round(r.IndexBytes / 1024.0 / 1024.0, 1),
                /* Null on a skipped row, and deliberately not zero: zero density would read as a
                   catastrophically bloated index, which is the opposite of "we did not look". */
                avg_leaf_density = r.AvgLeafDensity,
                leaf_fragmentation = r.LeafFragmentation,
                tree_level = r.TreeLevel,
                empty_pages = r.EmptyPages,
                deleted_pages = r.DeletedPages,
                estimated_reclaimable_bytes = r.EstimatedReclaimableBytes,
                estimated_reclaimable_mb = r.EstimatedReclaimableBytes is { } bytes
                    ? Math.Round(bytes / 1024.0 / 1024.0, 1)
                    : (double?)null,
                /* Present means NOT MEASURED. Named rather than boolean so the row says WHY. */
                skipped_reason = r.SkippedReason,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                index_count = rows.Count,
                truncated,
                /* Over the returned rows, and withheld when they are only a page of them: "12 of 25
                   measured" reads as a statement about the server's indexes and would not be one. */
                measured_count = truncated ? (int?)null : measured,
                note = "avg_leaf_density is the bloat signal — a freshly built btree sits near 90%. Rows "
                     + "carrying a skipped_reason were NOT measured (measuring walks the index, so the "
                     + "collector bounds how many it does per cycle); a null measurement on those rows is "
                     + "absence of data, never a clean result."
                     + (measured < rows.Count
                         ? $" {rows.Count - measured} of the {rows.Count} row(s) RETURNED are labelled rather than measured."
                         : string.Empty)
                     + (truncated
                         ? " TRUNCATED at the row limit: there are more indexes than this. Raise the limit "
                           + "before concluding anything about the server as a whole."
                         : string.Empty),
                indexes,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL index bloat failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_column_stats"), Description("Gets PostgreSQL per-column distribution statistics from pg_stats: n_distinct, null fraction, average width, physical correlation, and the frequency of the single most common value. These are the numbers the PLANNER uses, so they explain plan shapes that otherwise look arbitrary. n_distinct is negative when PostgreSQL expresses it as a RATIO of table rows (-1 means every value is unique) and positive when it is an absolute count - do not compare the two without checking the sign. correlation near 1 or -1 means the column's physical order matches its logical order, which is what makes an index range scan cheap; near 0 makes the same scan expensive. A high top_value_frequency is the classic cause of a plan that is right for the common value and wrong for every other one. Only columns on tables above a size floor are collected, and only where the monitoring login can see the statistics.")]
    public static async Task<string> GetPgColumnStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (7 days) - this collector runs daily.")] int hours_back = 168,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgColumnStatsReader.GetPgColumnStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_column_stats")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_column_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No column statistics for {resolved.ServerName} in the last {hours_back} hour(s). "
                        + "This collector runs DAILY and only reads tables above a size floor, so a small "
                        + "database can be legitimately empty here. pg_stats is also filtered by "
                        + "privilege: a monitoring login without SELECT on a table sees no rows for it, "
                        + "and that looks identical to a table with no statistics.");
            }

            var columns = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                column_name = r.ColumnName,
                /* Passed through with its sign intact. Normalising it to an absolute count would need the
                   row count at the time the sample was taken, which is not stored and would be a guess. */
                n_distinct = r.NDistinct,
                null_frac = r.NullFrac,
                avg_width = r.AvgWidth,
                correlation = r.Correlation,
                top_value_frequency = r.TopValueFrequency,
                /* Null means NO most-common-value list at all, which is itself informative: a perfectly
                   uniform column has none. Zero would claim the list exists and is empty. */
                common_value_count = r.CommonValueCount,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                column_count = rows.Count,
                note = "n_distinct is a RATIO of table rows when negative and an absolute count when "
                     + "positive — check the sign before comparing two columns. correlation near ±1 is "
                     + "what makes an index range scan cheap. A null common_value_count means the column "
                     + "has no most-common-value list at all, not that the list is empty.",
                columns,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL column stats failed: {ex.Message}");
        }
    }
}
