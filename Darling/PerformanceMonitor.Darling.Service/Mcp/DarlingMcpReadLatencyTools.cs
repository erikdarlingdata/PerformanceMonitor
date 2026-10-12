/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read-latency MCP surface (#4442 scope 2) — the tool measuring its OWN read speed, so "which of our
/// reads is really slow, and how often does it time out" is a query over stored history instead of a guess
/// from the last slow-query log line. Reads the hourly histogram <see cref="ReadLatencyAccumulator"/> flushes
/// into <c>collect.read_latency</c>: per (surface, route), the summed count/total/max and the element-wise
/// summed bucket histogram over the window, from which <see cref="ReadLatencyPercentiles"/> answers
/// p50/p95/p99 as bucket UPPER-BOUND estimates. Store-scoped by nature, so it takes no <c>server_name</c>.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpReadLatencyTools
{
    public const int DefaultHours = 24;
    public const int DefaultLimit = 25;

    /// <summary>
    /// The caveat every row of this surface carries: percentiles here are the SMALLEST bucket upper bound
    /// whose cumulative count reaches the target, never an interpolation — the same honesty
    /// <see cref="ReadLatencyPercentiles.FromBuckets"/> documents. A percentile flagged <c>"≥ &lt;bound&gt;"</c>
    /// means the true value only proven to exceed the last finite bucket, with no bound above it to report.
    /// </summary>
    internal const string BucketEstimateNote =
        "p50/p95/p99 are bucket UPPER-BOUND estimates from a fixed log-scale histogram, never an interpolation "
        + "— the true duration could be anywhere at or below the reported bound. A value flagged with \"≥\" means "
        + "the true duration only proven to exceed the last finite bucket bound, with no bound above it to report.";

    [McpServerTool(Name = "get_read_latency"), Description(
        "Gets the monitoring tool's OWN read-latency history — which of the web dashboard's or MCP server's own "
        + "reads is really slow, and how often it times out; no server_name. Sums the stored hourly histograms "
        + "over the window and answers p50/p95/p99 as bucket UPPER-BOUND estimates (never an interpolation), "
        + "plus count, mean, max and timeouts, per (surface, route). Sorted p95 desc. Optional surface "
        + "(web/compose/mcp) and route filter. Empty means no read recorded in the window, not a failure. "
        + "<<GUIDE>> "
        + "Gets the monitoring tool's OWN read-latency history — how long the web dashboard's `/api/read/*` "
        + "reads, the composed-panel runner and every MCP tool call take. This is the tool measuring itself, NOT a monitored SQL Server or PostgreSQL target. The service "
        + "records every finished read into an in-process histogram keyed by (surface, route, outcome) and "
        + "flushes an hourly aggregate: run count, total and max duration in ms, and a fixed log-scale bucket "
        + "histogram (about 24 bounds, 10 ms to 120 s, plus an overflow bucket for anything past 120 s). This "
        + "tool sums those hourly rows over the window per (surface, route) — run_count and total_ms by "
        + "addition, max_ms by MAX, and the bucket histogram ELEMENT-WISE (bucket 1 across every hour summed "
        + "together, bucket 2 across every hour summed together, and so on) — and computes p50/p95/p99 from "
        + "the SUMMED histogram. Every percentile here is a BUCKET UPPER BOUND, never an interpolated exact "
        + "value: the histogram only knows which bucket a duration landed in, so the honest answer is \"at most "
        + "this many ms\", flagged with \"≥ <bound>\" on the rare row whose true value only proven to exceed "
        + "the last finite bucket. timeouts is the run_count of samples whose outcome was a caught statement "
        + "timeout (57014) for that route — a route that answers fast most of the time but times out on a heavy "
        + "case shows both figures side by side. Sorted by p95 descending, then by count, then surface and route, so the worst tail "
        + "leads. fallbacks is the run_count of reads whose interval-table path faulted, or whose hour ledger did not yet cover the window, "
        + "and were answered from raw (outcome fallback_raw); gate_failures is the run_count of reads whose "
        + "source decision itself faulted, so they read raw without one (outcome gate_failed). Both are counted "
        + "inside count and the percentiles, and a fallback that then timed out counts as a timeout, not a "
        + "fallback. Pass surface to scope to one of web/compose/mcp, or route to scope to one read/panel name; "
        + "both are optional and compose (AND) when both are given. Empty means no read was recorded for the "
        + "window/filter, which on a fresh store, or one below schema V148, is the expected answer — not a "
        + "sign anything is broken. The page is capped at limit rows or the shared ~32 KB response budget, whichever cuts first: "
        + "truncated says either cut applied, reads_returned is the rows sent and reads_total the rows the window "
        + "held, so reads_total minus reads_returned were left out, always from the end of the order above. Raise "
        + "limit (up to the budget) or narrow with surface or route to see them.")]
    public static async Task<string> GetReadLatency(
        NpgsqlDataSource postgres,
        [Description("Hours of history to summarize. Default 24; max 168 (7 days).")] int hours = DefaultHours,
        [Description("Optional: scope to one surface (web, compose, or mcp).")] string? surface = null,
        [Description("Optional: scope to one route (read/panel name).")] string? route = null,
        [Description("Maximum rows to return. Default 25; max 1000.")] int limit = DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        var hoursError = McpHelpers.ValidateHoursBack(hours);
        if (hoursError != null)
        {
            return hoursError;
        }

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null)
        {
            return limitError;
        }

        var since = DateTime.UtcNow.AddHours(-hours);

        try
        {
            var rows = await DarlingReadLatencyReader.GetSummaryAsync(postgres, since, surface, route, cancellationToken);
            if (rows.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No read recorded in the last {hours} hour(s)" +
                    (string.IsNullOrWhiteSpace(surface) && string.IsNullOrWhiteSpace(route) ? "" : " for that filter") +
                    ". The service records an hourly histogram per (surface, route, outcome); a store before " +
                    "schema V148, or a quiet window, has no row.");
            }

            var projected = rows.Select(r =>
            {
                var p50 = ReadLatencyPercentiles.FromBuckets(r.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.50);
                var p95 = ReadLatencyPercentiles.FromBuckets(r.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.95);
                var p99 = ReadLatencyPercentiles.FromBuckets(r.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.99);
                return new
                {
                    Row = r,
                    P50 = p50,
                    P95 = p95,
                    P99 = p99,
                };
            })
            /* p95 desc, then count desc — the worst tail leads, and among equal tails the busiest route
               leads. A null p95 (no samples; cannot happen alongside a non-zero run_count, since a drained
               bucket always carries at least one count) sorts last. */
            .OrderByDescending(x => x.P95?.UpperBoundMs ?? -1)
            .ThenByDescending(x => x.Row.RunCount)
            .ThenBy(x => x.Row.Surface, StringComparer.Ordinal)
            .ThenBy(x => x.Row.Route, StringComparer.Ordinal)
            .ToArray();

            var shaped = projected.Select(x => (object)new
            {
                surface = x.Row.Surface,
                route = x.Row.Route,
                count = x.Row.RunCount,
                mean_ms = x.Row.MeanMs,
                max_ms = x.Row.MaxMs,
                p50_ms = x.P50?.UpperBoundMs,
                p50_is_at_least = x.P50?.IsAtLeast ?? false,
                p95_ms = x.P95?.UpperBoundMs,
                p95_is_at_least = x.P95?.IsAtLeast ?? false,
                p99_ms = x.P99?.UpperBoundMs,
                p99_is_at_least = x.P99?.IsAtLeast ?? false,
                timeouts = x.Row.Timeouts,
                fallbacks = x.Row.Fallbacks,
                gate_failures = x.Row.GateFailures,
            }).ToArray();

            return BuildResponse(hours, surface, route, shaped, limit, McpResponseBudget.DefaultBytes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_read_latency", ex);
        }
    }

    /// <summary>
    /// Serializes the page: the first <paramref name="limit"/> rows, further cut to fit <paramref name="budgetBytes"/>.
    /// Rows are taken greedily in order using each row's own bytes plus a comma, then the REAL final response is
    /// serialized and, while it is over budget with more than one row, the last row is dropped and it is
    /// serialized again — the envelope's own digits and the true/false spelling depend on the final page.
    /// </summary>
    internal static string BuildResponse(int hours, string? surface, string? route, IReadOnlyList<object> shaped, int limit, int budgetBytes)
    {
        string Envelope(IReadOnlyList<object> page, bool truncated) => JsonSerializer.Serialize(new
        {
            hours,
            surface,
            route,
            note = BucketEstimateNote,
            truncated,
            reads_returned = page.Count,
            reads_total = shaped.Count,
            reads = page,
        });

        var page = new List<object>();
        var running = Encoding.UTF8.GetByteCount(Envelope(Array.Empty<object>(), true));
        foreach (var row in shaped.Take(limit))
        {
            var added = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(row)) + (page.Count == 0 ? 0 : 1);
            if (page.Count > 0 && running + added > budgetBytes)
            {
                break;
            }

            running += added;
            page.Add(row);
        }

        var result = Envelope(page, page.Count < shaped.Count);
        while (Encoding.UTF8.GetByteCount(result) > budgetBytes && page.Count > 1)
        {
            page.RemoveAt(page.Count - 1);
            result = Envelope(page, true);
        }

        return result;
    }
}
