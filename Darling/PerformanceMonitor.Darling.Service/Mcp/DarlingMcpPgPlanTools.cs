/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// Captured PostgreSQL execution plans (#2567).
///
/// <para><b>It returns the plan, not a pointer to one.</b> #2538 is explicit about this and it is the whole
/// point of the tool: an agent consuming the MCP has no viewer to follow a reference into, so a read that
/// answers with an id has not answered.</para>
///
/// <para><b>Nothing here needs to redact anything, and nothing here should try.</b> The plan JSON is
/// stripped at collection (#2566) — query text dropped, literals replaced — so there is no un-redacted copy
/// in the store for this read to leak. Re-deriving that logic here would create a second place for it to
/// drift out of agreement with the first.</para>
///
/// <para><b>The empty answers are the work.</b> "No plan" has three unrelated causes with three unrelated
/// remedies, and collapsing them into one sentence is how a missing grant reads as a healthy query. They are
/// separated here using facts the store already holds rather than prose that guesses between them, which is
/// what #2557 replaced on the Query Store side.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgPlanTools
{
    [McpServerTool(Name = "get_pg_plans"), Description("Gets execution plans captured from PostgreSQL by auto_explain, grouped by plan shape and ranked by total time. Returns the plan JSON ITSELF, not a reference to it. The plan is REDACTED at collection and that is not a limitation to work around: auto_explain emits the statement text and its filter literals verbatim, so the query text is dropped entirely and every literal inside the plan tree is replaced with a placeholder before the plan is ever stored — node types, relation names, costs, row estimates and the tree shape all survive intact, which is what a plan is read for. Join query_id to get_pg_top_queries for the statement's normalized text, its call count and its timing. queryid is returned as a STRING because it is a signed 64-bit value spread over the whole int8 range: most ids exceed what a JSON number survives and a numeric wire form is silently rounded by any parser decoding numbers as IEEE-754 doubles, after which it matches nothing. When there are no plans this tool distinguishes three genuinely different causes rather than reporting one vague absence: capture is not configured on the server (with the specific missing precondition), capture is configured but this window's statements never crossed the duration threshold, or plans existed and aged out of retention. This is PostgreSQL-only and separate from get_plan_xml, which covers SQL Server.")]
    public static async Task<string> GetPgPlans(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum plan shapes to return. Default 10.")] int limit = 10,
        [Description("Only return plans for this queryid, as a string. Optional.")] string? query_id = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        /* Parsed from a string for the same reason it is returned as one: an agent that round-trips the id
           through a JSON number has already lost it, and accepting a number here would make that silent. */
        long? wantedQueryId = null;

        if (!string.IsNullOrWhiteSpace(query_id))
        {
            if (!long.TryParse(query_id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            {
                return McpHelpers.Status(
                    "error",
                    $"query_id '{query_id}' is not a 64-bit integer. PostgreSQL queryids are signed int8 "
                    + "values and must be passed as their exact decimal text — if this one arrived through a "
                    + "JSON number it has already been rounded and no longer matches anything.");
            }

            wantedQueryId = parsed;
        }

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);

            var rows = await DarlingPgPlanCaptureReader.GetPgPlanCaptureAsync(
                postgres, resolved.ServerId, start, now, wantedQueryId is null ? limit : limit * 10);

            if (wantedQueryId is not null)
            {
                rows = rows.Where(r => r.QueryId == wantedQueryId.Value).Take(limit).ToList();
            }

            if (rows.Count == 0)
            {
                return await NoPlansStatusAsync(postgres, resolved.ServerId, resolved.ServerName, wantedQueryId);
            }

            return BuildPlansJson(resolved.ServerName, hours_back, rows, limit);
        }
        catch (Exception ex)
        {
            var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                postgres, resolved.ServerId, resolved.ServerName, "pg_plan_capture");
            if (gated != null)
            {
                return gated;
            }

            return McpHelpers.Status("error", $"Reading PostgreSQL plans failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Separates the three reasons there is no plan, using what the store already knows.
    ///
    /// <para>The order matters and is not arbitrary. A server that cannot capture at all makes every other
    /// explanation irrelevant, so the readiness facets are asked FIRST — and they are the only branch that
    /// yields an actionable remedy. Only once capture is known to be possible does "this query never crossed
    /// the threshold" become a true statement rather than a guess.</para>
    /// </summary>
    private static async Task<string> NoPlansStatusAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, long? wantedQueryId)
    {
        var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
            postgres, serverId, serverName, "pg_plan_capture");
        if (gated != null)
        {
            return gated;
        }

        var precondition = await DarlingRuntimePrecondition.StatusAsync(
            postgres, serverId, serverName, "pg_plan_capture");
        if (precondition != null)
        {
            return precondition;
        }

        /* The readiness collector (#2564) already measured every precondition and stored the remedy beside
           it, so this names the specific missing step instead of listing everything that could be wrong. */
        var unmet = await UnsatisfiedFacetsAsync(postgres, serverId);

        if (unmet.Count > 0)
        {
            return McpHelpers.Status(
                "precondition",
                "This server cannot capture execution plans yet, so an empty result here says nothing about "
                + "the query. Unsatisfied precondition(s), from pg_plan_capture_readiness: "
                + string.Join(" | ", unmet)
                + " — get_pg_plan_capture_readiness has the full detail and the remedy for each.");
        }

        var subject = wantedQueryId is null ? "any statement" : "this statement";

        return McpHelpers.Status(
            "empty",
            $"Capture is configured on this server and no plan was captured for {subject} in this window. "
            + "Two things produce that and they are different: the statement never ran longer than "
            + "auto_explain.log_min_duration, which is the healthy answer and means it is not the query to "
            + "look at; or a plan was captured earlier and has aged out, which plan_content_retention_days "
            + "governs — widen hours_back to tell those apart, because a plan that exists further back will "
            + "reappear and one that never existed will not.");
    }

    /// <summary>
    /// The unsatisfied readiness facets, newest reading per facet. Read directly rather than through the
    /// readiness tool so this stays a fact lookup rather than one MCP tool narrating another's prose.
    /// </summary>
    private static async Task<List<string>> UnsatisfiedFacetsAsync(NpgsqlDataSource postgres, int serverId)
    {
        var unmet = new List<string>();

        const string sql = """
            SELECT facet, observed
            FROM (
                SELECT DISTINCT ON (facet) facet, is_satisfied, observed, collection_time
                FROM pg_plan_capture_readiness
                WHERE server_id = $1
                ORDER BY facet, collection_time DESC
            ) AS latest
            WHERE is_satisfied IS NOT TRUE
            ORDER BY facet
            """;

        try
        {
            await using var command = postgres.CreateCommand(sql);
            command.Parameters.AddWithValue(serverId);

            await using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var facet = reader.IsDBNull(0) ? "(unnamed)" : reader.GetString(0);
                var observed = reader.IsDBNull(1) ? "(not reported)" : reader.GetString(1);
                unmet.Add($"{facet} = {observed}");
            }
        }
        catch (PostgresException)
        {
            /* An older store without the readiness table. Silence is correct: the caller falls through to
               the generic answer, which is less specific but not wrong. */
        }

        return unmet;
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store (#2548) — the
    /// same reason <c>BuildTopQueriesJson</c> is separate.
    /// </summary>
    internal static string BuildPlansJson(
        string serverName,
        int hoursBack,
        IReadOnlyList<DarlingPgPlanCaptureReader.PgPlanCaptureRow> rows,
        int limit)
    {
        var result = rows.Take(limit).Select(r => new
        {
            /* #2548: a STRING. queryid is a signed int8 spread over the whole 64-bit range, so most values
               are past 2^53 and any parser decoding JSON numbers as doubles rounds one — and queryid is an
               equality join key, which rounding loses outright rather than approximates. */
            queryid = r.QueryId.ToString(CultureInfo.InvariantCulture),
            plan_hash = r.PlanHash,
            top_node_type = r.TopNodeType,
            node_count = r.NodeCount,
            /* CAPTURES, not executions. The collector reads an overlapping tail of the server log, so one
               execution can be seen twice; get_pg_top_queries.calls is the authority on how often a
               statement actually ran. Named so the difference is visible on the wire. */
            captures = r.Captures,
            total_duration_ms = Math.Round(r.TotalDurationMs, 3),
            max_duration_ms = Math.Round(r.MaxDurationMs, 3),
            avg_duration_ms = Math.Round(r.AvgDurationMs, 3),
            last_seen = r.LastSeen,
            /* The plan itself, already redacted at collection. Emitted as parsed JSON rather than as a
               string so a consumer can walk the tree without a second parse. */
            plan = ParsePlan(r.PlanJson),
        }).ToList();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            plan_shapes = result.Count,
            note = "Plans are REDACTED at collection: statement text is dropped and literals inside the "
                 + "plan are replaced with placeholders. Node types, relation names, costs, row estimates "
                 + "and tree shape are intact. Join queryid to get_pg_top_queries for the statement text "
                 + "and its call counts.",
            plans = result,
        }, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// Emits the stored plan as JSON when it parses and as a raw string when it does not, rather than
    /// dropping it. A plan that survived collection but will not re-parse is worth showing to whoever has to
    /// explain why — and an exception here would take out the whole response for one bad row.
    /// </summary>
    private static object? ParsePlan(string? planJson)
    {
        if (string.IsNullOrWhiteSpace(planJson))
        {
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize<JsonElement>(planJson);
        }
        catch (JsonException)
        {
            return planJson;
        }
    }
}
