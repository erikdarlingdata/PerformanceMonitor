/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for PostgreSQL wait statistics, paired with the <c>pg_wait_stats</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgWaitTools
{
    [McpServerTool(Name = "get_pg_wait_stats"), Description("Gets the top PostgreSQL wait events aggregated over a time period, for Amazon Aurora PostgreSQL targets. Waits reveal what the database spends time waiting on: IO events point at storage or cache misses, Lock events at blocking between sessions, LWLock at internal contention, and LSN is Aurora's storage-durability wait. Background-worker and client-idle waits are already excluded by the collector, so every row here is real work. Note this is a separate tool from get_wait_stats, which covers SQL Server: PostgreSQL has a two-level type/event taxonomy, no signal-wait concept, and reports in microseconds, so the two cannot share one result shape. THE PAGE IS BOUNDED BY limit: wait_events_returned is how many events you got, truncated says the window held more, and the rows are the heaviest so the ones past the cap are lighter. SHARES ARE OF THE WINDOW, NOT OF THE PAGE: pct_of_total_wait's denominator is total_wait_time_ms, the WHOLE window's wait time across every event, computed in the same statement as the rows - so a three-row page does not sum to 100%, and the gap between returned_wait_time_ms (what the page adds up to) and total_wait_time_ms is the waiting the cap left out; returned_pct_of_total is that ratio stated once.")]
    public static async Task<string> GetPgWaitStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum wait events to return, heaviest total wait first. Default 20. This is what bounds the page - read truncated to know whether the window held more; the shares stay of the whole window whatever this is set to.")] int limit = 20,
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
            var now = windowEnd;
            /* #3541 A7: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal - and the window total rides on the same statement, so the shares below have a
               denominator the cap cannot shrink. */
            var page = await DarlingPgWaitReader.GetPgWaitStatsPageAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1);

            /* An empty result is genuinely ambiguous here in a way it is not for SQL Server: it means
               either no data in the window, or that this server is not a PostgreSQL target at all. Say
               so, rather than letting a caller read "no waits" as "no waiting". */
            if (page.Rows.Count == 0)
            {
                /* Both halves of the old ambiguity are answerable when the store knows the engine (#2532):
                   a SQL Server target gets the dialect answer, a stock PostgreSQL one gets the Aurora-only
                   answer, and both say the gap is permanent instead of inviting a hunt. So the sentence
                   below is reached in exactly two states — an Aurora target with no rows in the window, or
                   a row whose engine_kind is NULL — and it names those rather than repeating the two the
                   capability answer has already ruled out. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No PostgreSQL wait data for this server and window. On Aurora, the pg_wait_stats "
                        + "collector may not have completed a cycle yet. Otherwise the store has not "
                        + "recorded this server's engine yet, and a target it cannot classify may not be a "
                        + "PostgreSQL one at all — check list_servers.");
            }

            return BuildWaitStatsJson(resolved.ServerName, hours_back, page, limit);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL wait stats failed: {ex.Message}");
        }
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store — the same reason
    /// the statement tool's <c>BuildTopQueriesJson</c> is separate.
    ///
    /// <para><b>The denominator is the window's, not the page's</b> (#3541 A7). <paramref name="page"/> carries
    /// <c>WindowTotalWaitTimeMs</c> from the same statement as its rows, and every <c>pct_of_total_wait</c>
    /// divides by THAT. The previous shape divided by the sum of the rows fetched, so at <c>limit = 3</c> the
    /// three shares summed to 100% and read as "these three are everything". The page's own sum still
    /// travels as <c>returned_wait_time_ms</c>, and the ratio of the two is <c>returned_pct_of_total</c>.</para>
    ///
    /// <para><paramref name="page"/> holds up to <c>limit + 1</c> rows; the extra one is the truncation signal
    /// and is cut before the projection.</para>
    /// </summary>
    internal static string BuildWaitStatsJson(
        string serverName,
        int hoursBack,
        DarlingPgWaitReader.PgWaitStatsPage page,
        int limit)
    {
        var truncated = page.Rows.Count > limit;
        var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows;

        var windowTotalMs = page.WindowTotalWaitTimeMs;
        var returnedMs = rows.Sum(r => r.TotalWaitTimeMs);

        var result = rows.Select(r => new
        {
            wait_type = r.WaitType,
            wait_event = r.WaitEvent,
            total_wait_time_ms = Math.Round(r.TotalWaitTimeMs, 1),
            waits = r.TotalWaits,
            avg_wait_time_ms = Math.Round(r.AvgWaitTimeMs, 3),
            /* Share of the WINDOW's total wait time - never of the page's. The absolute figure alone does
               not say whether an event is the story or a rounding error; a share of the page could not say
               it either, because a page always sums to itself. */
            pct_of_total_wait = windowTotalMs > 0 ? Math.Round(r.TotalWaitTimeMs / windowTotalMs * 100, 1) : 0,
        })
        .ToList();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            /* #3541 A3 dialect: the page described as a page. No time bounds — the rows are per-event
               aggregates over the whole window, so there is no page reach to report, only a cap. */
            wait_events_returned = result.Count,
            truncated,
            order = "total_wait_time_ms_desc",
            /* The WINDOW's wait time, across every event that accrued any — the denominator of every
               pct_of_total_wait above. NOT the sum of the rows; that is returned_wait_time_ms. */
            total_wait_time_ms = Math.Round(windowTotalMs, 1),
            returned_wait_time_ms = Math.Round(returnedMs, 1),
            returned_pct_of_total = windowTotalMs > 0 ? Math.Round(returnedMs / windowTotalMs * 100, 1) : 0,
            note = "total_wait_time_ms is the WHOLE window's wait time across every event, computed in the "
                 + "same statement as the rows; each row's pct_of_total_wait divides by it, so the shares on "
                 + "a page do not sum to 100 unless the page is the whole window (truncated = false). "
                 + "returned_wait_time_ms is what the rows returned add up to.",
            waits = result,
        }, McpHelpers.JsonOptions);
    }
}
