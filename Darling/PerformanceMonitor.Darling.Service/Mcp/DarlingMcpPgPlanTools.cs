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
/// Captured PostgreSQL execution plans (#2567), and whether the target can capture them at all (#3070).
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
///
/// <para><b>Readiness is a first-class read, not a footnote on the empty answer</b> (#3070). The facets carry
/// a per-facet <c>detail</c> — the operator-facing remedy for that specific step — and until #3070 its only
/// reader was the WPF tab, so on a Linux host and to every agent the remedy did not exist.
/// <c>UnsatisfiedFacetsAsync</c> below cannot substitute for it: by construction it shows only the facets
/// that are UNSATISFIED and only their observed values, so it can never answer "what is the state of this
/// server's plan capture", which is the question somebody has when the plans are missing.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgPlanTools
{
    [McpServerTool(Name = "get_pg_plans"), Description("Gets PostgreSQL execution plans captured by auto_explain, grouped by plan shape and ranked by total time. Returns the plan JSON ITSELF, not a reference. Redacted at collection: no statement text or literals. queryid is a STRING: a 64-bit id a JSON number silently rounds. Window ends at as_of. Empty separates three causes: capture not configured, no statement crossed the duration threshold, or plans aged out of retention - read get_pg_plan_capture_readiness first. PostgreSQL-only; get_plan_xml covers SQL Server. <<GUIDE>> Gets execution plans captured from PostgreSQL by auto_explain, grouped by plan shape and ranked by total time. Returns the plan JSON ITSELF, not a reference to it. The plan is REDACTED at collection and that is not a limitation to work around: auto_explain emits the statement text and its filter literals verbatim, so the query text is dropped entirely and every literal inside the plan tree is replaced with a placeholder before the plan is ever stored — node types, relation names, costs, row estimates and the tree shape all survive intact, which is what a plan is read for. Join query_id to get_pg_top_queries for the statement's normalized text, its call count and its timing. queryid is returned as a STRING because it is a signed 64-bit value spread over the whole int8 range: most ids exceed what a JSON number survives and a numeric wire form is silently rounded by any parser decoding numbers as IEEE-754 doubles, after which it matches nothing. When there are no plans this tool distinguishes three genuinely different causes rather than reporting one vague absence: capture is not configured on the server (with the specific missing precondition), capture is configured but this window's statements never crossed the duration threshold, or plans existed and aged out of retention. This is PostgreSQL-only and separate from get_plan_xml, which covers SQL Server. This is what bounds the page - read truncated to know whether the window held more shapes than were returned; it is observed by fetching one row past this cap, never inferred from a full page. The filter is applied in the store over EVERY capture in the window, not over the top-duration page, so a statement ranked far below the busiest shapes is still found - and an empty answer with this set genuinely means no plan for it was captured in the window.")]
    public static async Task<string> GetPgPlans(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum plan shapes to return. Default 10. See the tool's reading guide.")] int limit = 10,
        [Description("Only return plans for this queryid, as a string. Optional. See the tool's reading guide.")] string? query_id = null,
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
                return McpHelpers.Refusal(
                    "query_id",
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

            /* The queryid pin travels INTO the SQL (#3533). It used to be applied here, over a fetched
               top-duration page, which made every plan ranked below the page unfindable and then reported
               the miss as "not captured" — the predicate has to run where the rows are. */
            /* #3653 (the #3541 A3 class, found beside the readiness site below): limit + 1 as the fetch, so
               BuildPlansJson can say whether the window held more shapes than the page. It used to Take(limit)
               over a read already capped at `limit` and publish nothing about the cut, so a page of ten shapes
               read as "this server's plans" whether the window held ten or ten thousand. */
            var rows = await DarlingPgPlanCaptureReader.GetPgPlanCaptureAsync(
                postgres, resolved.ServerId, start, now, limit + 1, wantedQueryId);

            if (rows.Count == 0)
            {
                return await NoPlansStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, wantedQueryId, hours_back);
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

            return McpHelpers.FormatError("get_pg_plans", ex);
        }
    }

    [McpServerTool(Name = "get_pg_plan_capture_readiness"), Description("Gets whether a PostgreSQL target can capture execution plans at all, facet by facet: is_satisfied, the observed value, and the operator remedy (detail), in CAUSAL order - fix them in that order. Runs HOURLY; a window under an hour can legitimately find nothing. An EMPTY answer here is NOT the healthy case: the collector writes one row per facet on every run, so empty means nothing was collected, not that every facet passed. Read this before trusting an empty get_pg_plans or any target log read. Never claims a plan was captured; get_pg_plans is that read. PostgreSQL-only. <<GUIDE>> Gets whether a PostgreSQL target can capture execution plans at all, facet by facet, with the specific remedy for every step that is not in place. Read this FIRST when get_pg_plans is empty, and read it before concluding anything from an empty target-side log read: the facets are the preconditions, not a summary of them, and each one carries the operator-facing consequence and fix for its own state rather than a shared 'plans unavailable'. Every facet is reported whether or not it is satisfied, because the state somebody needs is the whole picture and a list of only the failures cannot show that capture is configured and working. The facets are returned in CAUSAL order - could this server run auto_explain, is it loaded, is it capturing, in what format, and can what it captures be attributed to a statement - so they read as the sequence they have to be fixed in rather than alphabetically. Two states are the ones people misread and each is its own facet with its own remedy: auto_explain loaded with log_min_duration = -1 is loaded and capturing NOTHING, which is indistinguishable from not loaded from the outside; and a log_line_prefix with no %Q means auto_explain captured the plan but nothing can join it to the statement it came from, because auto_explain puts no query id in the plan itself. On Aurora and RDS the remedy text says which changes need a custom cluster parameter group and a writer reboot rather than a SET, which is the misconception worth heading off. One facet reaches beyond plan capture: message_locale reports whether the target writes its log messages in English, which every target-side log read matches - including get_pg_deadlocks, where zero rows is the healthy state, so an unmet locale facet is the difference between a quiet server and a read that cannot see anything. This is PostgreSQL-only. It reports readiness and never claims a plan was captured; get_pg_plans is the read for the plans themselves. This is what bounds the page - read truncated to know whether a facet sat past it, in which case unsatisfied_facets is withheld; it is observed by fetching one row past this cap, so asking for exactly the number of facets the collector emits reads as complete.")]
    public static async Task<string> GetPgPlanCaptureReadiness(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for each facet's most recent reading. Default 24 - this collector runs hourly, so a window under an hour can legitimately find nothing.")] int hours_back = 24,
        [Description("Maximum facet rows to return. Default 25 - the collector emits one row per facet, so the default is well clear of the whole set. See the tool's reading guide.")] int limit = 25,
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
            /* The reader shared with the WPF tab (#2530), which is the whole shape of this tool: the SQL,
               the latest-per-facet reduction and the causal ordering already exist and are already proven
               against a live store. A second copy of that query here is how the two surfaces start
               disagreeing about what a server's readiness is. */
            /* #3653 (the #3541 A3 class): limit + 1 as the fetch, so BuildReadinessJson can OBSERVE whether a
               facet sat past the cap rather than infer it from a full page. This tool is where that
               inference bit hardest: the collector emits exactly six facets, so a caller asking for
               `limit = 6` - the whole set - used to be told the result was truncated and have its
               unsatisfied_facets WITHHELD, for a page that was complete. */
            var rows = await DarlingPgPlanCaptureReadinessReader.GetPgPlanCaptureReadinessAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit + 1);

            if (rows.Count == 0)
            {
                return await NoReadinessStatusAsync(postgres, resolved.ServerId, resolved.ServerName, hours_back);
            }

            return BuildReadinessJson(resolved.ServerName, hours_back, rows, limit);
        }
        catch (Exception ex)
        {
            var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                postgres, resolved.ServerId, resolved.ServerName, "pg_plan_capture_readiness");
            if (gated != null)
            {
                return gated;
            }

            return McpHelpers.FormatError("get_pg_plan_capture_readiness", ex);
        }
    }

    /// <summary>
    /// Why there is no readiness state, which is a different question from why there are no plans.
    ///
    /// <para>The facets are configuration, so an empty answer here is never "the server was healthy and had
    /// nothing to report" — this collector stores a row per facet on every run regardless of what it finds.
    /// It is the engine gate, a run that could not complete, or a window shorter than the hourly cadence, in
    /// that order.</para>
    /// </summary>
    private static async Task<string> NoReadinessStatusAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int hoursBack)
    {
        var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
            postgres, serverId, serverName, "pg_plan_capture_readiness");
        if (gated != null)
        {
            return gated;
        }

        var precondition = await DarlingRuntimePrecondition.StatusAsync(
            postgres, serverId, serverName, "pg_plan_capture_readiness");
        if (precondition != null)
        {
            return precondition;
        }

        return McpHelpers.Status(
            "empty",
            $"No plan-capture readiness state for {serverName} in the last {hoursBack} hour(s). This is NOT "
            + "the healthy case for this read: the collector writes one row per facet on every run whatever "
            + "it finds, so an absence means nothing was collected rather than that nothing was wrong. It "
            + "runs HOURLY, so widen hours_back before concluding anything — a window shorter than the "
            + "cadence is legitimately empty on a server that is collecting normally.");
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store — the same reason
    /// <see cref="BuildPlansJson"/> is separate.
    /// </summary>
    /// <param name="fetched">The rows the reader returned for a request of <paramref name="limit"/><c> + 1</c>:
    /// up to one more than the caller asked for, the extra one being the truncation signal. The page this
    /// emits is the first <paramref name="limit"/> of them.</param>
    internal static string BuildReadinessJson(
        string serverName,
        int hoursBack,
        IReadOnlyList<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow> fetched,
        int limit)
    {
        /* #2629's lesson, applied to a read whose row count is small enough to make it look unnecessary: a
           summary taken over a CAPPED result describes the page and reads as a fact about the server. The
           collector emits a handful of facets and the default limit clears them all, but limit is a caller's
           parameter — so the unsatisfied summary is WITHHELD rather than computed over whatever arrived.

           And WHETHER it was capped is observed, not inferred (#3653, the #3541 A3 class): the tool fetches
           limit + 1, and a page of exactly `limit` facets with nothing behind it is complete - the old
           `rows.Count >= limit` called that page truncated and withheld a summary that was whole. */
        var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

        var unsatisfied = rows.Where(r => !r.IsSatisfied).Select(r => r.Facet).ToArray();

        var facets = rows.Select(r => new
        {
            facet = r.Facet,
            is_satisfied = r.IsSatisfied,
            /* Verbatim, so a reader can see the value the server actually answered rather than trust this
               tool's interpretation of it. */
            observed = r.Observed,
            /* The point of the tool. detail is the per-facet consequence and remedy, written against what
               the collector saw, and until #3070 nothing outside the Windows Viewer could read it. */
            detail = r.Detail,
            last_observed = r.CaptureTime,
        }).ToList();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            facet_count = rows.Count,
            truncated,
            /* Deliberately NOT a single ready/not-ready verdict. The facets do not reduce to one: an unmet
               plan_attribution still captures plans and merely orphans them, and message_locale is about
               every target-side log read rather than about capture. A boolean would have to pick one meaning
               and would be wrong under the other, which is the exact collapse the collector splits its rows
               to avoid. The unsatisfied facets are NAMED instead. */
            unsatisfied_facets = truncated ? null : unsatisfied,
            note = "Every facet is reported, satisfied or not, in CAUSAL order rather than alphabetically: "
                 + "fix them in the order they are listed. detail carries the remedy for the state that "
                 + "facet is actually in — including, on Aurora/RDS, whether the change needs a custom "
                 + "cluster parameter group and a writer reboot rather than a SET. This is readiness only "
                 + "and never a claim that a plan was captured; get_pg_plans is that read."
                 + (truncated
                     ? " TRUNCATED at the row limit, so unsatisfied_facets is WITHHELD: naming them from a "
                       + "capped result would describe this page rather than the server. Raise the limit."
                     : string.Empty),
            facets,
        }, McpHelpers.JsonOptions);
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
        NpgsqlDataSource postgres, int serverId, string serverName, long? wantedQueryId, int hoursBack)
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
                + " — the pg_plan_capture_readiness collector stores the full detail and the remedy "
                + "beside each observation, and get_pg_plan_capture_readiness returns them: every facet, "
                + "satisfied or not, in the order they have to be fixed in.");
        }

        return McpHelpers.Status("empty", NoPlanCapturedMessage(wantedQueryId, hoursBack));
    }

    /// <summary>
    /// The empty answer once capture is known to be configured, split by whether a queryid was asked for.
    ///
    /// <para>The queryid arm changed with #3533 and its honesty depends on the reader: the filter now runs
    /// in the SQL over every capture in the window, so "no plan for this statement was captured" is a fact
    /// rather than a statement about a fetched page. The text this replaced said the query was "not the
    /// query to look at" — over a top-N page that was an invented verdict, and even over the whole window
    /// it collapses three different causes into the most reassuring one.</para>
    /// </summary>
    internal static string NoPlanCapturedMessage(long? wantedQueryId, int hoursBack) => wantedQueryId is null
        ? $"Capture is configured on this server and nothing was captured in the last {hoursBack} hour(s) — "
          + "this read has no filter, so that is a fact about every statement, not about a page of them. "
          + "Two things produce it and they are different: no statement ran longer than "
          + "auto_explain.log_min_duration, which is the healthy answer on a server whose statements are "
          + "all fast; or plans were captured earlier and have aged out, which plan_content_retention_days "
          + "governs — widen hours_back to tell those apart, because a plan that exists further back will "
          + "reappear and one that never existed will not."
        : $"Every capture in the last {hoursBack} hour(s) was searched for this query_id — the whole "
          + "window, in the store, not a top-N page — and none matches, so no plan for this statement was "
          + "captured in this window. That has three different causes with three different remedies: the "
          + "statement did not run in this window at all, which get_pg_top_queries can confirm from its "
          + "call counts; it ran but never crossed auto_explain.log_min_duration, so auto_explain never "
          + "wrote a plan — the healthy reading for a statement that is fast HERE, not a verdict about it "
          + "at other times; or capture was not working when it ran — get_pg_plan_capture_readiness "
          + "reports every precondition with the remedy beside it, and its facets are the latest reading "
          + "rather than the window's history, so a server that is ready now can still have missed an "
          + "earlier run. A plan captured before this window has aged out under "
          + "plan_content_retention_days; widen hours_back to reach further back.";

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
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
    /// <param name="fetched">The rows the reader returned for a request of <paramref name="limit"/><c> + 1</c>;
    /// the page is the first <paramref name="limit"/> of them and the extra row, when present, is the
    /// truncation signal (#3653). Before that the parameter was the page already cut at <c>limit</c> and the
    /// <c>Take(limit)</c> here was a no-op that said nothing about what lay past it.</param>
    internal static string BuildPlansJson(
        string serverName,
        int hoursBack,
        IReadOnlyList<DarlingPgPlanCaptureReader.PgPlanCaptureRow> fetched,
        int limit)
    {
        var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

        var result = rows.Select(r => new
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
            /* Observed off the limit + 1 fetch, never inferred from a full page (#3653): true means the window
               held at least one more plan shape, ranked below these by total duration, than this page shows. */
            truncated,
            note = "Plans are REDACTED at collection: statement text is dropped and literals inside the "
                 + "plan are replaced with placeholders. Node types, relation names, costs, row estimates "
                 + "and tree shape are intact. Join queryid to get_pg_top_queries for the statement text "
                 + "and its call counts."
                 + (truncated
                     ? $" TRUNCATED at the row limit of {limit}: the window holds more plan shapes than this, "
                       + "ranked below these by total duration. Raise limit, or pass query_id to pin one "
                       + "statement's plans wherever they rank."
                     : string.Empty),
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
