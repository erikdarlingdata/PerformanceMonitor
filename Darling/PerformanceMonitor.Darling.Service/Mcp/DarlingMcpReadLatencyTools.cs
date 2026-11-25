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
using PerformanceMonitor.Darling.Service.FrontDoor;

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
    /// means the true value is only proven to exceed the last finite bucket, with no bound above it to report.
    /// </summary>
    internal const string BucketEstimateNote =
        "p50/p95/p99 are bucket UPPER-BOUND estimates from a fixed log-scale histogram, never an interpolation"
        + ": the true duration could be anywhere at or below the reported bound, and no percentile is reported above max_ms (a bound past the slowest read seen is capped to max_ms). A value flagged with \"≥\" means "
        + "the true duration is only proven to exceed the last finite bucket bound, with no bound above it to report.";

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_read_latency"), Description(
        "Gets the monitoring tool's OWN read-latency history (web and MCP reads); no server_name. "
        + "Sums hourly histograms into p50/p95/p99 bucket upper bounds, count, mean, max and timeouts per "
        + "(surface, route, window band: 1h, 24h, 7d, 7d+ or none). "
        + "max_in_flight is the most other reads running when one started. "
        + "wait_total_ms is the summed wait for a store connection. "
        + "distinct_callers is the most different callers in one hour; no personal data. "
        + "Sorted p95 desc. Optional surface (web/compose/mcp) and route filter. Empty means no read in the window, not a failure. "
        + "split_at: before/after start. "
        + "<<GUIDE>> "
        + "Gets the monitoring tool's OWN read-latency history: how long the web dashboard's `/api/read/*` "
        + "reads, the composed-panel runner and every MCP tool call take. This is the tool measuring itself, NOT a monitored SQL Server or PostgreSQL target. The service "
        + "records every finished read into an in-process histogram keyed by (surface, route, outcome, window band) and "
        + "flushes an hourly aggregate: run count, total and max duration in ms, and a fixed log-scale bucket "
        + "histogram (about 24 bounds, 10 ms to 120 s, plus an overflow bucket for anything past 120 s). This "
        + "tool sums those hourly rows over the window per (surface, route, window_band): run_count and total_ms by "
        + "addition, max_ms by MAX, and the bucket histogram ELEMENT-WISE (bucket 1 across every hour summed "
        + "together, bucket 2 across every hour summed together, and so on), and computes p50/p95/p99 from "
        + "the SUMMED histogram. Every percentile here is a BUCKET UPPER BOUND, never an interpolated exact "
        + "value: the histogram only knows which bucket a duration landed in, so the honest answer is \"at most "
        + "this many ms\", flagged with \"≥ <bound>\" on the rare row whose true value is only proven to exceed "
        + "the last finite bucket; a bound above max_ms is capped to max_ms, the slowest read seen. timeouts is the run_count of samples whose outcome was a caught statement "
        + "timeout (57014) for that route: a route that answers fast most of the time but times out on a heavy "
        + "case shows both figures side by side. Sorted by p95 descending, then by count, then surface and route, so the worst tail "
        + "leads. fallbacks is the run_count of reads whose interval-table path faulted, or whose hour ledger did not yet cover the window, "
        + "and were answered from raw (outcome fallback_raw); gate_failures is the run_count of reads whose "
        + "source decision itself faulted, so they read raw without one (outcome gate_failed). Both are counted "
        + "inside count and the percentiles, and a fallback that then timed out counts as a timeout, not a "
        + "fallback. window_band is the width of the window the read asked for: 1h (up to 1 hour), 24h (over 1 hour up to 24), "
        + "7d (over 24 hours up to 7 days), 7d+ (longer), or none (the read asked for no window). "
        + "max_in_flight is the most other reads already running on the service when one of these reads started. "
        + "wait_total_ms is the summed part of these reads' time spent waiting for a store connection rather than running; it never includes "
        + "time spent waiting for a long-read slot. "
        + "The service runs only a few long reads at once, and the rows that are not a read running are counted on their own, "
        + "outside count, mean_ms, max_ms, the percentiles, max_in_flight and wait_total_ms: queued is the reads that waited for a long-read "
        + "slot, queue_wait_ms their summed wait and queue_wait_p95_ms the 95th percentile of that wait (a bucket upper bound like the others, "
        + "null when none waited); rejected is the reads answered busy (HTTP 429 on the web) because the slots and their queue were full; "
        + "cache_hits is the reads answered from the service's short-lived read cache; cancelled is the reads whose caller left before "
        + "they finished. A cache miss is a read that ran, so it is in count. A route whose only rows are these shows count 0. "
        + "When the host admits long reads, admission is this service process's live state since it started (never stored): limit, "
        + "running, queued, and per route the reads admitted, queued, rejected, answered from the cache and missed by it. "
        + "Older rows read as band none with 0 for both. "
        + "reply_bytes_total is the summed size in bytes of the replies the reads that ran sent (an MCP read's text, a web read's answer text, counted as UTF-8 once "
        + "the read finished), null when none of those reads was measured, and mean_reply_bytes is that sum over the reads that were measured, null when none was. "
        + "A read whose reply size was never taken (a composed panel, a tool call that threw, a web read answered with the service's fixed failure body or a plain sentence) is left out of both, "
        + "as are the hours stored before this column existed, so an hour that mixes measured and unmeasured reads averages its measured reads only. "
        + "mean_reply_bytes covers the reads that ran: the size of a read answered from the cache (cache_hits) is stored but is left out of both the sum and the average. "
        + "distinct_callers is the most different callers (MCP clients by name and version, web sessions by a salted hash that "
        + "holds no personal data) that read the route in any one hour of the window, 0 on older rows. At most 1,000 are counted per hour; 1,000 means at least that many. Pass surface to scope to one of web/compose/mcp, or route to scope to one read/panel name; "
        + "both are optional and compose (AND) when both are given. Empty means no read was recorded for the "
        + "window/filter, which on a fresh store, is the expected answer, not a "
        + "sign anything is broken. The page is capped at limit rows or the shared ~32 KB response budget, whichever cuts first: "
        + "truncated says either cut applied, reads_returned is the rows sent and reads_total the rows the window "
        + "held, so reads_total minus reads_returned were left out, always from the end of the order above. Raise "
        + "limit (up to the budget) or narrow with surface or route to see them. "
        + "split_at (an ISO-8601 UTC instant such as 2026-11-24T14:30:00Z, inside the window and not in the future) adds a split object to every row: "
        + "before and after, each with count, median_ms (a bucket upper bound like the percentiles, with median_is_at_least) and max_ms, "
        + "so a fix can be re-timed after a new build without comparing by hand. Each hourly bucket records the start instant of the service process "
        + "that wrote it, and a bucket counts as after when that process started at or after split_at, so the hour a new build started is divided "
        + "exactly, because a restart inside an hour writes its own bucket. Every answer lists process_starts: the distinct service start instants in the window, newest first, at most 20, each one a build or restart "
        + "that wrote buckets. Copy one of those values into split_at to judge the build that started then; a start taken from a log line or a clock is seconds off and "
        + "would put that build's own buckets before the split. "
        + "Buckets stored before the start was recorded have no start: they are placed by their hour, as after when the hour begins at or after split_at, "
        + "so the hour that holds split_at counts as before, and ambiguous_count says how many reads that is (they may belong to either side). "
        + "An unparsable split_at, one before the window starts or one in the future is refused, never moved. The split covers the same window, surface and route filters, "
        + "and rows, as the rest of the answer. A row with no read on a side shows count 0 and null median_ms for it.")]
    public static async Task<string> GetReadLatency(
        NpgsqlDataSource postgres,
        [Description("Hours of history to summarize. Default 24; max 168 (7 days).")] int hours = DefaultHours,
        [Description("Optional: scope to one surface (web, compose, or mcp).")] string? surface = null,
        [Description("Optional: scope to one route (read/panel name).")] string? route = null,
        [Description("Maximum rows to return. Default 25; max 1000.")] int limit = DefaultLimit,
        [Description("Optional: an ISO-8601 UTC instant (a service start) to split each row's count, median and max into before and after. Inside the window, not in the future.")] string? split_at = null,
        ReadAdmission? admission = null,
        CancellationToken cancellationToken = default)
    {
        var hoursError = DarlingReadWindow.ValidateHoursBack(hours);
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

        var splitProblem = ResolveSplitAt(split_at, since, out var splitAtUtc);
        if (splitProblem != null)
        {
            return McpHelpers.Refusal("split_at", splitProblem);
        }

        try
        {
            var rows = await DarlingReadLatencyReader.GetSummaryAsync(postgres, since, surface, route, splitAtUtc, cancellationToken);
            var processStarts = (await DarlingReadLatencyReader.GetProcessStartsAsync(postgres, since, cancellationToken)).Select(FormatInstant).ToArray();
            if (rows.Count == 0)
            {
                var emptyMessage = $"No read recorded {McpHelpers.InTheLast(CountText.Plural(hours, "hour"))}" +
                    (string.IsNullOrWhiteSpace(surface) && string.IsNullOrWhiteSpace(route) ? "" : " for that filter") +
                    ". The service records an hourly histogram per (surface, route, outcome, window band); a fresh store, " +
                    "or a quiet window, has no row.";

                /* #5506 part 3: rows flush hourly, so a quiet store window can still have long reads running or queued right
                   now; the empty answer carries the live admission object too, under hints. */
                var emptyHints = new Dictionary<string, object?> { ["process_starts"] = processStarts };
                if (AdmissionObject(admission) is { } emptyAdmission)
                {
                    emptyHints["admission"] = emptyAdmission;
                }

                return McpHelpers.Status("empty", emptyMessage, emptyHints);
            }

            var projected = rows.Select(r =>
            {
                var p50 = ReadLatencyPercentiles.FromBuckets(r.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.50, r.MaxMs);
                var p95 = ReadLatencyPercentiles.FromBuckets(r.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.95, r.MaxMs);
                var p99 = ReadLatencyPercentiles.FromBuckets(r.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.99, r.MaxMs);
                var queueP95 = r.QueueWaitBucketCounts is { } queueBuckets
                    ? ReadLatencyPercentiles.FromBuckets(queueBuckets, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.95, r.QueueWaitMaxMs)
                    : null;
                return new
                {
                    Row = r,
                    Split = r.Split,
                    P50 = p50,
                    P95 = p95,
                    P99 = p99,
                    QueueP95 = queueP95,
                };
            })
            /* p95 desc, then count desc — the worst tail leads, and among equal tails the busiest route
               leads. A null p95 (no samples; cannot happen alongside a non-zero run_count, since a drained
               bucket always carries at least one count) sorts last. */
            .OrderByDescending(x => x.P95?.UpperBoundMs ?? -1)
            .ThenByDescending(x => x.Row.RunCount)
            .ThenBy(x => x.Row.Surface, StringComparer.Ordinal)
            .ThenBy(x => x.Row.Route, StringComparer.Ordinal)
            .ThenBy(x => x.Row.WindowBand, StringComparer.Ordinal)
            .ToArray();

            var shaped = projected.Select(x => WithSplit(new
            {
                surface = x.Row.Surface,
                route = x.Row.Route,
                window_band = x.Row.WindowBand,
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
                max_in_flight = x.Row.MaxInFlight,
                wait_total_ms = x.Row.WaitTotalMs,
                distinct_callers = x.Row.DistinctCallers,
                queued = x.Row.Queued,
                queue_wait_ms = x.Row.QueueWaitMs,
                queue_wait_p95_ms = x.QueueP95?.UpperBoundMs,
                rejected = x.Row.Rejected,
                cache_hits = x.Row.CacheHits,
                cancelled = x.Row.Cancelled,
                reply_bytes_total = x.Row.ReplyBytesTotal,
                mean_reply_bytes = x.Row.MeanReplyBytes,
            }, x.Split)).ToArray();

            return BuildResponse(hours, surface, route, shaped, limit, McpResponseBudget.DefaultBytes, AdmissionObject(admission), splitAtUtc, processStarts);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_read_latency", ex);
        }
    }

    /// <summary>An instant spelled the way <c>split_at</c> reads it back, so a value copied from <c>process_starts</c> splits exactly there.</summary>
    private static string FormatInstant(DateTime utc)
        => utc.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF'Z'", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>The spellings <c>split_at</c> accepts, the same four as <c>as_of</c> (see <see cref="McpHelpers.ResolveAsOf"/>): an
    /// allowlist, so <c>01/02/2026</c> is refused instead of read as a month the caller did not mean.</summary>
    private static readonly string[] SplitAtFormats =
    {
        "yyyy-MM-dd",
        "yyyy-MM-ddTHH:mmK",
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
    };

    /// <summary>Resolves the optional <c>split_at</c> (#9434). Null in, null out. A value that does not parse, one before the window
    /// starts (<paramref name="windowStartUtc"/>) and one past now plus the <c>as_of</c> clock allowance are all refused: the reason
    /// comes back as the sentence (the tool wraps it in <see cref="McpHelpers.Refusal"/>), because a split moved to the nearest edge
    /// would answer a question nobody asked.</summary>
    internal static string? ResolveSplitAt(string? splitAt, DateTime windowStartUtc, out DateTime? splitAtUtc)
    {
        splitAtUtc = null;
        if (string.IsNullOrWhiteSpace(splitAt))
        {
            return null;
        }

        if (!DateTime.TryParseExact(
                splitAt.Trim(),
                SplitAtFormats,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal,
                out var parsed))
        {
            return ($"Invalid split_at value '{splitAt}'. Expected an ISO-8601 UTC instant: '2026-11-24T14:30:00Z', " +
                   "'2026-11-24T14:30:00' (read as UTC), '2026-11-24T16:30:00+02:00', or '2026-11-24' for midnight UTC.");
        }

        var utc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        if (utc > DateTime.UtcNow + McpHelpers.AsOfFutureTolerance)
        {
            return ($"split_at value '{splitAt}' is in the future. A split needs reads on both sides, so name a service start that has already happened (UTC).");
        }

        if (utc < windowStartUtc)
        {
            return ($"split_at value '{splitAt}' is before the start of the window ({windowStartUtc:yyyy-MM-dd HH:mm} UTC), so every read would fall after it. Raise hours (up to 168) to reach it, or name a later instant.");
        }

        splitAtUtc = utc;
        return null;
    }

    /// <summary>Adds the <c>split</c> block to a shaped row when the read asked for one (#9434). A read without split_at returns
    /// the row exactly as before, with no <c>split</c> member.</summary>
    private static object WithSplit(object row, DarlingReadLatencyReader.ReadLatencySplit? split)
    {
        if (split is null)
        {
            return row;
        }

        var node = JsonSerializer.SerializeToNode(row)!.AsObject();
        node["split"] = JsonSerializer.SerializeToNode(SplitObject(split));
        return node;
    }

    /// <summary>One row's split block (#9434): per side the reads that ran, their median as a bucket upper bound, and the slowest.</summary>
    private static object SplitObject(DarlingReadLatencyReader.ReadLatencySplit split)
    {
        object Side(DarlingReadLatencyReader.ReadLatencySide side)
        {
            var median = ReadLatencyPercentiles.FromBuckets(side.BucketCounts, ReadLatencyAccumulator.BucketUpperBoundsMs, 0.50, side.MaxMs);
            return new
            {
                count = side.Count,
                median_ms = median?.UpperBoundMs,
                median_is_at_least = median?.IsAtLeast ?? false,
                max_ms = side.MaxMs,
            };
        }

        return new
        {
            before = Side(split.Before),
            after = Side(split.After),
            ambiguous_count = split.AmbiguousCount,
        };
    }

    /// <summary>#5506 part 3: the live admission counts of THIS process since it started (never stored, so they reset on a
    /// restart and do not cover another service on the same store): the limit, the long reads running and waiting now,
    /// and per route the long reads admitted, queued and refused. Null when the host has no admission.</summary>
    internal static object? AdmissionObject(ReadAdmission? admission)
    {
        if (admission is null)
        {
            return null;
        }

        var stats = admission.Stats();
        return new
        {
            scope = "this service process, since it started",
            limit = stats.Limit,
            running = stats.Running,
            queued = stats.Queued,
            routes = stats.Routes.Select(r => new
            {
                surface = r.Surface,
                route = r.Route,
                admitted = r.Admitted,
                queued = r.Queued,
                rejected = r.Rejected,
                cache_hits = r.CacheHits,
                cache_misses = r.CacheMisses,
            }).ToArray(),
        };
    }

    /// <summary>
    /// Serializes the page: the first <paramref name="limit"/> rows, further cut to fit <paramref name="budgetBytes"/>.
    /// Rows are taken greedily in order using each row's own bytes plus a comma, then the REAL final response is
    /// serialized and, while it is over budget with more than one row, the last row is dropped and it is
    /// serialized again — the envelope's own digits and the true/false spelling depend on the final page.
    /// </summary>
    internal static string BuildResponse(int hours, string? surface, string? route, IReadOnlyList<object> shaped, int limit, int budgetBytes, object? admission = null, DateTime? splitAt = null, IReadOnlyList<string>? processStarts = null)
    {
        /* Insertion order is the member order of the answer. split_at appears only when the read asked for a split. process_starts is on every
           answer, so a caller can copy one of them into split_at (#9434). */
        string Envelope(IReadOnlyList<object> page, bool truncated)
        {
            var answer = new Dictionary<string, object?>
            {
                ["hours"] = hours,
                ["surface"] = surface,
                ["route"] = route,
            };
            if (splitAt is { } at)
            {
                answer["split_at"] = FormatInstant(at);
            }

            if (processStarts is not null)
            {
                answer["process_starts"] = processStarts;
            }

            answer["note"] = BucketEstimateNote;
            if (admission is not null)
            {
                answer["admission"] = admission;
            }

            answer["truncated"] = truncated;
            answer["reads_returned"] = page.Count;
            answer["reads_total"] = shaped.Count;
            answer["reads"] = page;
            return JsonSerializer.Serialize(answer);
        }

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
