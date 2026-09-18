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
/// The MCP surface for sampled PostgreSQL waits, paired with the <c>pg_wait_sampling</c> collector.
///
/// <para>
/// The stock-PostgreSQL answer to the question <c>get_pg_wait_stats</c> answers on Aurora, and #2629's
/// first entry because #2625 shipped a message that <b>points here</b>: a permanent-gap sentence now tells
/// a non-Aurora operator that <c>pg_wait_sampling</c> covers what Aurora's wait instrumentation cannot.
/// Until this tool existed that pointer led to a Windows-only WPF panel — so on a Linux host, and for any
/// agent anywhere, "why is this slow" had an answer that could not be read.
/// </para>
///
/// <para>
/// <b>It is a sampler, and the shape of the answer has to say so.</b> Aurora's
/// <c>aurora_stat_system_waits()</c> measures accumulated wait TIME; this extension counts how many
/// periodic samples caught a backend in each state. Multiplying samples by the profile period estimates
/// milliseconds and the reader does, but it is an estimate whose error grows as the event gets rarer, so
/// the sample count travels beside it rather than being converted away.
/// </para>
///
/// <para>
/// <b>CPU is a row here, not a missing one.</b> The collector maps a NULL event type to <c>CPU</c>/
/// <c>Running</c>, so a backend that was not waiting is counted rather than dropped — which makes the
/// share column answer "waiting or working?" before it answers "waiting on what?". A profile that is
/// mostly CPU and a profile that is mostly IO call for opposite next steps, and a wait-only view cannot
/// tell them apart.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgWaitSamplingTools
{
    [McpServerTool(Name = "get_pg_wait_sampling"), Description("Gets sampled PostgreSQL wait events attributed to query shapes, from the pg_wait_sampling extension. This is the stock-PostgreSQL counterpart of get_pg_wait_stats, which reads an Aurora-only source: use this tool on any self-hosted or non-Aurora PostgreSQL target. A sampling profiler periodically records what each backend is doing, so results are sample COUNTS, and estimated_wait_ms is samples multiplied by the sampling period rather than a measured duration - treat a rare event's estimate as approximate. Rows with event_type CPU mean the backend was running, not waiting, so the profile answers 'waiting or working' as well as 'waiting on what'. queryid joins get_pg_top_queries; queryid 0 is work belonging to no statement, such as a background worker. The profile is cluster-wide and carries no database attribution by design. THE PAGE IS BOUNDED BY limit: waits_returned is how many rows you got, truncated says the window held more, and the rows are the most-sampled so the ones past the cap are rarer. SHARES ARE OF THE WINDOW, NOT OF THE PAGE: pct_of_samples' denominator is total_samples, the WHOLE window's differenced sample count across every (event type, event, query) series, computed in the same statement as the rows - so a three-row page does not sum to 100%, and the gap between returned_samples (what the page adds up to) and total_samples is the activity the cap left out; returned_pct_of_total is that ratio stated once.")]
    public static async Task<string> GetPgWaitSampling(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, most samples first. Default 20. This is what bounds the page - read truncated to know whether the window held more; the shares stay of the whole window whatever this is set to.")] int limit = 20,
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
            /* #3541 A7: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal - and the window's sample count rides on the same statement, so the shares below
               have a denominator the cap cannot shrink. */
            var page = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit + 1);

            if (page.Rows.Count == 0)
            {
                /* Three distinct empty states, and only the last of them is "nothing happened". The
                   capability answer rules out a wrong-engine target; the precondition answer names an
                   extension that is not installed — which is the LIKELY one here, because
                   pg_wait_sampling needs shared_preload_libraries and a server restart, so an operator
                   who has not done that gets told what to do rather than shown a blank. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_sampling")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_wait_sampling")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No sampled waits for {resolved.ServerName} in the last {hours_back} hour(s). The "
                        + "figures here are per-interval deltas, so a single collection has nothing to "
                        + "difference against and the window fills on the second one. On a genuinely idle "
                        + "server this is the healthy state: the profiler samples backends, and an idle "
                        + "server has none to sample.");
            }

            return BuildWaitSamplingJson(resolved.ServerName, hours_back, page, limit);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL sampled waits failed: {ex.Message}");
        }
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store.
    ///
    /// <para><b>The denominator is the window's, not the page's</b> (#3541 A7). <paramref name="page"/> carries
    /// <c>WindowTotalSamples</c> from the same statement as its rows, and every <c>pct_of_samples</c> divides by
    /// THAT. The previous shape divided by the sum of the rows fetched, so at <c>limit = 3</c> the three shares
    /// summed to 100% and "waiting or working?" was answered over three series rather than over the
    /// profile. The page's own sum still travels as <c>returned_samples</c>; the ratio of the two is
    /// <c>returned_pct_of_total</c>.</para>
    ///
    /// <para>The denominator is SAMPLES, not the estimate. Every row's estimate is the same count times the
    /// same period, so the two shares are arithmetically identical — and taking it from the count keeps
    /// the percentage anchored to what was actually observed.</para>
    /// </summary>
    internal static string BuildWaitSamplingJson(
        string serverName,
        int hoursBack,
        DarlingPgWaitSamplingReader.PgWaitSamplingPage page,
        int limit)
    {
        var truncated = page.Rows.Count > limit;
        var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows;

        var windowTotalSamples = page.WindowTotalSamples;
        var returnedSamples = rows.Sum(r => r.SampleCount);
        var resetInWindow = rows.Any(r => r.CounterReset);

        var waits = rows.Select(r => new
        {
            event_type = r.EventType,
            wait_event = r.Event,
            /* String, like every other queryid on this surface: it is a signed 64-bit value and a
               JSON number would round it in any double-decoding parser, silently producing an id
               that joins to nothing. */
            queryid = r.QueryId.ToString(CultureInfo.InvariantCulture),
            samples = r.SampleCount,
            estimated_wait_ms = r.EstimatedWaitMs,
            backends = r.BackendCount,
            /* Of the WINDOW's samples, never of the page's — see the remarks. */
            pct_of_samples = windowTotalSamples > 0
                ? Math.Round((double)r.SampleCount / windowTotalSamples * 100, 1)
                : 0,
            /* Per row, because a reset is per series: one query's profile can be reset while another
               accumulated normally, and a single window-level flag would libel both. */
            counter_reset = r.CounterReset,
        })
        .ToList();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            /* #3541 A3 dialect: the page described as a page. No time bounds — each row is one series
               differenced across the whole window, so there is no page reach to report, only a cap. */
            waits_returned = waits.Count,
            truncated,
            order = "samples_desc",
            /* The WINDOW's samples, across every series — the denominator of every pct_of_samples above.
               NOT the sum of the rows; that is returned_samples. */
            total_samples = windowTotalSamples,
            returned_samples = returnedSamples,
            returned_pct_of_total = windowTotalSamples > 0
                ? Math.Round((double)returnedSamples / windowTotalSamples * 100, 1)
                : 0,
            note = "Sample COUNTS from a periodic profiler, not measured durations. estimated_wait_ms "
                 + "is samples multiplied by the sampling period and is approximate — most so for rare "
                 + "events. event_type CPU means the backend was running rather than waiting. "
                 + "total_samples is the WHOLE window's sample count across every series, computed in the "
                 + "same statement as the rows; each row's pct_of_samples divides by it, so the shares on a "
                 + "page do not sum to 100 unless the page is the whole window (truncated = false). "
                 + "returned_samples is what the rows returned add up to."
                 + (resetInWindow
                     ? " At least one series was RESET inside this window, so its figures cover only "
                       + "the time since the reset."
                     : string.Empty),
            waits,
        }, McpHelpers.JsonOptions);
    }
}
