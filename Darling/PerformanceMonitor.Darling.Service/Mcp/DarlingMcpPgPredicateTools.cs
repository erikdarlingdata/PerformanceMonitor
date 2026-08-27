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
/// The MCP surface for predicate selectivity, paired with the <c>pg_qualstats</c>-backed
/// <c>pg_predicate_stats</c> collector (#2629).
///
/// <para>
/// The evidence behind an index recommendation, which is the thing an index recommendation usually lacks.
/// It records which COLUMNS were actually filtered on, with which operators, how many rows each predicate
/// examined and how many it threw away — so "this column should be indexed" stops being a guess about the
/// workload and becomes a count taken from it.
/// </para>
///
/// <para>
/// <b>It is sampled.</b> <c>pg_qualstats.sample_rate</c> decides what fraction of executions are recorded,
/// and the default is 1%. The row counts are therefore counts of what was SAMPLED; the sample rate travels
/// with every row so a caller can scale them, and deliberately is not applied here — multiplying by 100
/// and presenting the product as a measurement would launder an estimate into a fact.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgPredicateTools
{
    [McpServerTool(Name = "get_pg_predicate_stats"), Description("Gets PostgreSQL predicate selectivity from the pg_qualstats extension: which columns queries actually filter on, with which operator, how many rows each predicate evaluated and how many it filtered out. This is the evidence behind an index recommendation - a predicate that evaluates many rows and filters nearly all of them away is a column doing work an index could do instead. filtered_pct is that ratio. worst_estimate_error_ratio compares what the planner expected against what it got, so a large value marks a predicate the planner is misjudging, which is a statistics or correlated-column problem rather than an indexing one. IMPORTANT: these are SAMPLED counts - pg_qualstats records only a fraction of executions, given per row as sample_rate (commonly 0.01), and the counts are NOT scaled up here. queryid joins get_pg_top_queries.")]
    public static async Task<string> GetPgPredicateStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
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
            var rows = await DarlingPgPredicateStatsReader.GetPgPredicateStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_predicate_stats")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_predicate_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No predicate statistics for {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). pg_qualstats SAMPLES executions — at the default 1% rate a low-traffic "
                        + "database can genuinely record nothing — and it needs shared_preload_libraries "
                        + "plus a restart to be active at all.");
            }

            /* One rate for the server in the ordinary case; distinct() rather than First() because a rate
               changed mid-window would otherwise be reported as whichever row sorted first. */
            var rates = rows.Select(r => r.SampleRate).Distinct().OrderBy(r => r).ToArray();

            var predicates = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                schema_name = r.SchemaName,
                table_name = r.TableName,
                column_name = r.ColumnName,
                @operator = r.Operator,
                queryid = r.QueryId.ToString(CultureInfo.InvariantCulture),
                sampled_executions = r.SampleCount,
                rows_evaluated = r.RowsEvaluated,
                rows_filtered = r.RowsFiltered,
                /* Null when nothing was evaluated — a percentage of zero rows is not zero percent. */
                filtered_pct = r.FilteredPct,
                worst_estimate_error_ratio = Math.Round(r.WorstEstimateErrorRatio, 2),
                /* Per row, because it can change under the operator's hand mid-window and the counts
                   beside it are only interpretable against the rate that produced them. */
                sample_rate = r.SampleRate,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                predicate_count = rows.Count,
                sample_rates = rates,
                note = "Counts are SAMPLED and are not scaled up here: multiply by 1/sample_rate to "
                     + "estimate the true volume, and treat the product as an estimate. A predicate with "
                     + "a high filtered_pct over many rows is an indexing candidate; a high "
                     + "worst_estimate_error_ratio is a planner-estimate problem instead, which an index "
                     + "will not fix."
                     + (rates.Length > 1
                         ? " The sample rate CHANGED inside this window, so rows are not directly comparable."
                         : string.Empty),
                predicates,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL predicate stats failed: {ex.Message}");
        }
    }
}
