// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

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
/// Time-series reads for PostgreSQL (#2663). The service had fourteen trend reads and none worked on a
/// PostgreSQL target, so every PostgreSQL answer described one window and none of them answered "is this
/// getting worse".
///
/// <para>Every subject here is OPTIONAL and every read says which subject it chose. That is what makes them
/// usable as web panels with no drill-down plumbing, and it is what stops an automatic pick from being
/// mistaken for an answer about the event, query, I/O path or database the caller had in mind.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgTrendTools
{
    [McpServerTool(Name = "get_pg_wait_trend"), Description("Gets a time series for ONE PostgreSQL wait event: how much the server waited on it in each collection interval, normalised per second. Use get_pg_wait_sampling first to find which events dominate, then this to see whether one is growing. Summed across the queries that waited, because this answers a question about the SERVER - per-query attribution for a single window is what get_pg_wait_sampling already gives. The figures are estimates from a sampling profiler (samples x profile period), not measured durations, so treat the SHAPE as the finding rather than the absolute number. An interval where pg_wait_sampling's profile was reset - by pg_wait_sampling_reset_profile() or a server restart - is flagged, and reports everything since the reset rather than a misleadingly quiet interval.")]
    public static async Task<string> GetPgWaitTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The exact wait event name, e.g. DataFileRead, WALWrite. Omit to follow whichever event dominates the window.")] string? wait_event = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var start = windowEnd.AddHours(-hours_back);

            /* With no event named, follow the one that actually dominates this server rather than a name
               somebody guessed. Which one was chosen is reported, so the answer is never about a different
               event than the reader thinks. */
            var chosen = string.IsNullOrWhiteSpace(wait_event)
                ? await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, resolved.ServerId, start, windowEnd)
                : wait_event.Trim();

            if (string.IsNullOrWhiteSpace(chosen))
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_sampling")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No wait event was sampled on {resolved.ServerName} in the last {hours_back} "
                        + "hour(s), so there is nothing to follow. pg_wait_sampling needs the extension "
                        + "loaded; get_pg_extensions reports whether it is.");
            }

            var points = await DarlingPgTrendReader.GetWaitTrendAsync(
                postgres, resolved.ServerId, chosen, start, windowEnd);

            if (points.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_sampling")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No samples for wait event '{chosen}' on {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). A trend needs at least TWO snapshots to difference, so a "
                        + "window holding one collection is legitimately empty here even when the event is "
                        + "being sampled. get_pg_wait_sampling lists the events this server does record.");
            }

            var resets = points.Count(p => p.CounterReset);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                wait_event = chosen,
                /* Said plainly when it was not asked for, so a caller never reads this as an answer about
                   the event they had in mind. */
                wait_event_source = string.IsNullOrWhiteSpace(wait_event)
                    ? "chosen automatically: the most-sampled event in this window that is an actual WAIT. "
                      + "The CPU class - pg_wait_sampling's 'Running', meaning the backend was not waiting - "
                      + "is excluded from the choice because it dominates any healthy server's profile and "
                      + "would answer the opposite of the question. Name it explicitly to follow it."
                    : "as requested",
                hours_back,
                status = "wait_trend",
                point_count = points.Count,
                counter_reset_count = resets,
                note = "estimated_wait_ms_per_second is samples x the profiler's period, over the interval's "
                     + "length - an estimate from a sampling profiler, not a measured duration, so the shape "
                     + "over time is the finding rather than the absolute value. Per SECOND because "
                     + "collection intervals are not uniform: a restart or a slow cycle stretches one, and a "
                     + "per-interval total would render that as a spike."
                     + (resets > 0
                         ? $"  {resets} interval(s) span a profile RESET - pg_wait_sampling_reset_profile() "
                           + "or a server restart - and report everything since the reset rather than a "
                           + "quiet interval, which is what clamping the difference at zero would have shown."
                         : string.Empty),
                points = points.Select(p => new
                {
                    collection_time = p.CollectionTimeUtc,
                    sample_count = p.SampleCount,
                    estimated_wait_ms_per_second = Math.Round(p.EstimatedWaitMsPerSecond, 3),
                    backend_count = p.BackendCount,
                    counter_reset = p.CounterReset,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL wait trend failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_query_duration_trend"), Description("Gets a time series for ONE PostgreSQL statement by queryid: what a single execution cost in each collection interval, how many times it ran, and its call rate. This is the regression read - a query whose mean execution time steps up and stays up has changed plan or lost an index, and the step is visible here where a single-window average hides it. Use get_pg_top_queries first to get a queryid. mean_exec_ms is null rather than zero for an interval where the statement did not run, because a mean over no calls is absent rather than fast.")]
    public static async Task<string> GetPgQueryDurationTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The queryid from get_pg_top_queries; PostgreSQL query ids can be negative. Omit to follow the statement that spent the most time in the window.")] string? queryid = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        long parsedQueryId;
        var requested = !string.IsNullOrWhiteSpace(queryid);

        /* Taken as TEXT and parsed here: a PostgreSQL query id is a signed 64-bit hash that routinely
           exceeds what a JSON number survives intact, and a client that rounds one silently asks about a
           statement that does not exist. */
        if (requested && !long.TryParse(queryid!.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out parsedQueryId))
        {
            return McpHelpers.Status(
                "error",
                $"queryid '{queryid}' is not a 64-bit integer. PostgreSQL query ids are signed and often "
                + "negative; pass the value from get_pg_top_queries exactly as it appears.");
        }

        try
        {
            var start = windowEnd.AddHours(-hours_back);

            if (!requested)
            {
                var top = await DarlingPgTrendReader.GetTopQueryIdAsync(postgres, resolved.ServerId, start, windowEnd);

                if (top is null)
                {
                    return await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats")
                        ?? McpHelpers.Status(
                            "empty",
                            $"No statement recorded execution time on {resolved.ServerName} in the last "
                            + $"{hours_back} hour(s), so there is nothing to follow.");
                }

                parsedQueryId = top.Value;
            }
            else
            {
                parsedQueryId = long.Parse(queryid!.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture);
            }

            var points = await DarlingPgTrendReader.GetQueryDurationTrendAsync(
                postgres, resolved.ServerId, parsedQueryId, start, windowEnd);

            if (points.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No samples for queryid {parsedQueryId} on {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). pg_stat_statements evicts statements under memory "
                        + "pressure, so a queryid that was there yesterday can be gone rather than idle - "
                        + "get_pg_top_queries shows what the server currently tracks.");
            }

            var ran = points.Where(p => p.Calls > 0).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                queryid = parsedQueryId.ToString(CultureInfo.InvariantCulture),
                queryid_source = requested
                    ? "as requested"
                    : "chosen automatically: the statement with the most execution time in this window",
                hours_back,
                status = "query_duration_trend",
                point_count = points.Count,
                intervals_with_calls = ran.Count,
                /* The two ends of the series that actually ran, so a step is visible without reading every
                   point - and null when nothing ran, rather than a shape invented from no executions. */
                first_mean_exec_ms = ran.Count > 0 ? Math.Round(ran[0].MeanExecMs, 3) : (double?)null,
                last_mean_exec_ms = ran.Count > 0 ? Math.Round(ran[^1].MeanExecMs, 3) : (double?)null,
                note = "mean_exec_ms is the interval's total execution time over its calls - what ONE "
                     + "execution cost then. It is null for an interval with no calls, because a mean over "
                     + "no executions is absent rather than zero. A step that persists is a plan or index "
                     + "change; a spike that recovers is usually contention, which get_pg_wait_trend and "
                     + "get_pg_blocking speak to.",
                points = points.Select(p => new
                {
                    collection_time = p.CollectionTimeUtc,
                    calls = p.Calls,
                    total_exec_ms = Math.Round(p.TotalExecMs, 3),
                    mean_exec_ms = p.Calls > 0 ? Math.Round(p.MeanExecMs, 3) : (double?)null,
                    calls_per_second = Math.Round(p.CallsPerSecond, 4),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL query duration trend failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_io_trend"), Description("Gets a time series for ONE PostgreSQL (backend_type, context) pair from pg_stat_io: read, write and extend rates per second, the buffer-cache hit ratio for each interval, and per-operation latency where the server measures it. Use get_pg_io_stats first to see which combinations are busy, then this to see whether one is growing or slowing. The subject is a PAIR because a hit ratio summed across contexts is meaningless - bulkread is a sequential scan deliberately bypassing the buffer pool with a ring buffer, so averaging its misses with the normal context's understates both and they have opposite remedies. Object types are summed together, which cannot distort the ratio because WAL rows report no buffer hits. Reports whether the server tracks I/O TIMING at all: track_io_timing is OFF by default in PostgreSQL, and its zero read_time would otherwise divide out to a latency of 0.000 ms that reads as an impossibly fast disk rather than an unmeasured one. Also reports whether byte volumes are MEASURED (PostgreSQL 18's read_bytes/write_bytes) or ESTIMATED as operations x block size (pre-18) - the two are different quantities and 18 moves several blocks per operation, so the older estimate undercounts there. Write counters are null rather than zero on Amazon Aurora, where backends do not write data files. An interval spanning a pg_stat_reset_shared('io') or a restart is flagged and reports everything since the reset, rather than the quiet interval that clamping the difference at zero would show. Requires PostgreSQL 16 or later; valid on a standby.")]
    public static async Task<string> GetPgIoTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Which backend did the I/O, e.g. 'client backend', 'autovacuum worker', 'checkpointer'. Naming this alone follows the busiest CONTEXT for that backend; omit both to follow whichever pair moved the most I/O.")] string? backend_type = null,
        [Description("Why the I/O happened: normal, bulkread, bulkwrite, vacuum, index, walreplay. Naming this alone follows the busiest BACKEND in that context; omit both to follow whichever pair moved the most I/O.")] string? context = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var start = windowEnd.AddHours(-hours_back);

            var askedBackend = string.IsNullOrWhiteSpace(backend_type) ? null : backend_type.Trim();
            var askedContext = string.IsNullOrWhiteSpace(context) ? null : context.Trim();
            var requested = askedBackend is not null && askedContext is not null;
            string chosenBackend;
            string chosenContext;

            if (requested)
            {
                chosenBackend = askedBackend!;
                chosenContext = askedContext!;
            }
            else
            {
                /* HALF a subject is a real request, not an error and not an excuse to ignore the half that
                   was given: "the busiest context for the autovacuum worker" is a question somebody asks.
                   Whichever half was named CONSTRAINS the choice, and subject_source says which half was
                   chosen for them - the alternative was accepting a backend type and quietly answering
                   about a different one. */
                var dominant = await DarlingPgTrendReader.GetDominantIoSubjectAsync(
                    postgres, resolved.ServerId, start, windowEnd, askedBackend, askedContext);

                if (dominant is null)
                {
                    return await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_io_stats")
                        ?? McpHelpers.Status(
                            "empty",
                            (askedBackend ?? askedContext) is null
                                ? $"No backend type recorded any I/O or buffer activity on "
                                  + $"{resolved.ServerName} in the last {hours_back} hour(s), so there is "
                                  + "nothing to follow. Buffer HITS count here, not only physical reads and "
                                  + "writes, so this is a genuinely idle window rather than a fully cached "
                                  + "one. pg_stat_io also needs PostgreSQL 16 or later - on an older major "
                                  + "the collector never runs, which get_collection_health reports."
                                : $"Nothing matching {(askedBackend is not null ? $"backend type '{askedBackend}'" : $"context '{askedContext}'")} "
                                  + $"recorded any I/O or buffer activity on {resolved.ServerName} in the "
                                  + $"last {hours_back} hour(s). get_pg_io_stats lists the combinations "
                                  + "this server reports; omit both parameters to follow whichever pair is "
                                  + "busiest.");
                }

                chosenBackend = dominant.Value.BackendType;
                chosenContext = dominant.Value.Context;
            }

            var points = await DarlingPgTrendReader.GetIoTrendAsync(
                postgres, resolved.ServerId, chosenBackend, chosenContext, start, windowEnd);

            if (points.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_io_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No I/O samples for '{chosenBackend}' in the '{chosenContext}' context on "
                        + $"{resolved.ServerName} in the last {hours_back} hour(s). A trend needs at least "
                        + "TWO snapshots to difference, so a window holding one collection is legitimately "
                        + "empty here even while the combination is being collected. get_pg_io_stats lists "
                        + "the combinations this server actually reports.");
            }

            /* Asked of the server's OWN configuration rather than inferred from the zeros, because the two
               readings a zero latency permits - "the disk is instant" and "nobody is timing it" - are not
               distinguishable in the counters and only one of them is ever true. */
            var timingSetting = await DarlingPgTrendReader.GetIoTimingTrackedAsync(
                postgres, resolved.ServerId, windowEnd);
            var timingObserved = points.Any(p => p.ReadTimeMs > 0 || p.WriteTimeMs > 0);
            var timingTracked = timingSetting ?? timingObserved;

            var writesTracked = points.Any(p => p.WriteCountersTracked);
            var bytesMeasured = points.Any(p => p.BytesMeasured);
            var bytesEstimated = !bytesMeasured && points.Any(p => p.BytesEstimable);
            var resets = points.Count(p => p.CounterReset);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                backend_type = chosenBackend,
                context = chosenContext,
                /* Said plainly when it was not asked for, so a caller never reads this as an answer about
                   the combination they had in mind - and said per HALF, because half a subject can be
                   named and the other half then chosen inside it. */
                subject_source = requested
                    ? "as requested"
                    : (askedBackend is not null
                        ? "backend_type as requested; context chosen automatically within it: "
                        : askedContext is not null
                            ? "context as requested; backend_type chosen automatically within it: "
                            : "chosen automatically: ")
                      + "the pair that moved the most read, write and extend operations in this window. "
                      + "Ranked on OPERATIONS rather than on read time - which is what the single-window "
                      + "read ranks by - because track_io_timing is off by default, and over a store full "
                      + "of zeros that ranking degenerates into picking a name alphabetically. Name both "
                      + "parameters to pin the pair exactly.",
                context_meaning = DarlingPgIoReader.ContextMeaning(chosenContext),
                hours_back,
                status = "io_trend",
                point_count = points.Count,
                counter_reset_count = resets,
                total_reads = points.Sum(p => p.Reads),
                total_writes = writesTracked ? points.Sum(p => p.Writes) : (long?)null,
                total_extends = points.Sum(p => p.Extends),
                total_hits = points.Sum(p => p.Hits),
                total_read_bytes = bytesMeasured || bytesEstimated ? points.Sum(p => p.ReadBytes) : (decimal?)null,
                total_write_bytes = (bytesMeasured || bytesEstimated) && writesTracked
                    ? points.Sum(p => p.WriteBytes)
                    : (decimal?)null,
                peak_reads_per_second = Math.Round(points.Max(p => p.ReadsPerSecond), 3),
                /* Named at the top rather than only per point: a caller has to know which of these are
                   measured before it draws any conclusion from a number below. */
                write_counters_tracked = writesTracked,
                io_timing_tracked = timingTracked,
                io_timing_source = timingSetting is null
                    ? "inferred from the data - this server's configuration has not been collected, so "
                      + "track_io_timing is unknown and the answer here is simply whether any non-zero I/O "
                      + "time appears in the window"
                    : "the target's own track_io_timing, as collected into pg_server_config",
                bytes_source = bytesMeasured
                    ? "measured"
                    : (bytesEstimated ? "estimated_from_block_size" : "unavailable"),
                note = "Every figure is the difference between CONSECUTIVE snapshots, normalised per SECOND "
                     + "by the interval's own length - collection cadence is not uniform, and a per-interval "
                     + "total would render a slow sweep as a spike in the data rather than in the server. "
                     + "cache_hit_pct is scoped to this pair, which is the only scope where it means "
                     + "anything."
                     + (writesTracked
                         ? string.Empty
                         : "  This server tracks NO write counters - the signature of Amazon Aurora, where "
                           + "backends do not write data files and the storage layer does. The write fields "
                           + "are null rather than zero, because absent here means unmeasured.")
                     + (resets > 0
                         ? $"  {resets} interval(s) span a statistics RESET - pg_stat_reset_shared('io') or "
                           + "a restart - and report everything since the reset rather than the quiet "
                           + "interval that clamping the difference at zero would have shown."
                         : string.Empty),
                timing_note = timingTracked
                    ? "avg_read_ms and avg_write_ms are measured per-operation latencies: this server has "
                      + "track_io_timing on."
                    : "avg_read_ms and avg_write_ms are NULL throughout because this server does not "
                      + "measure I/O time. track_io_timing is off by DEFAULT in PostgreSQL, so this is the "
                      + "ordinary configuration rather than a fault - but it means the volume figures below "
                      + "are the only I/O evidence here, and nothing in this store can say whether the "
                      + "storage is slow. Turning it on costs a clock read per operation; measure that on "
                      + "the platform before enabling it fleet-wide.",
                bytes_note = bytesMeasured
                    ? "Byte rates are MEASURED, from PostgreSQL 18's read_bytes / write_bytes. They are not "
                      + "comparable with the figures a pre-18 server reports, which are operations x block "
                      + "size: 18 reads several blocks per operation, so the older estimate undercounts."
                    : (bytesEstimated
                        ? "Byte rates are ESTIMATED as operations x op_bytes, which is exact below "
                          + "PostgreSQL 18 because one operation moves one block. 18 removed op_bytes and "
                          + "measures the bytes directly instead."
                        : "This server reports no byte figures at all - neither op_bytes nor the measured "
                          + "columns PostgreSQL 18 replaced it with - so the byte rates are null. The "
                          + "operation counts and the hit ratio are unaffected."),
                points = points.Select(p => new
                {
                    collection_time = p.CollectionTimeUtc,
                    interval_seconds = Math.Round(p.IntervalSeconds, 1),
                    reads_per_second = Math.Round(p.ReadsPerSecond, 3),
                    writes_per_second = p.WriteCountersTracked ? Math.Round(p.WritesPerSecond, 3) : (double?)null,
                    extends_per_second = Math.Round(p.ExtendsPerSecond, 3),
                    /* The rate beside the ratio, because they answer different questions: cache_hit_pct
                       says what share the pool absorbed, hits_per_second says how much work there was to
                       absorb. A pair can hold 100% while doing almost nothing. */
                    hits_per_second = Math.Round(p.HitsPerSecond, 3),
                    /* Ring-buffer REUSE is a different thing and is not folded in here: a bulk operation
                       recycling its own buffers is not pressure on the pool, and conflating the two is the
                       standard misreading of pg_stat_io. */
                    evictions_per_second = p.IntervalSeconds > 0
                        ? Math.Round(p.Evictions / p.IntervalSeconds, 3)
                        : 0,
                    cache_hit_pct = p.CacheHitPct is { } hit ? Math.Round(hit, 2) : (double?)null,
                    /* Nulled when the server does not time I/O, rather than passing the 0.000 the
                       arithmetic produces: that value is a statement about the configuration, not about
                       the disk, and printed as a latency it is the most reassuring wrong number here. */
                    avg_read_ms = timingTracked && p.AvgReadMs is { } read ? Math.Round(read, 3) : (double?)null,
                    avg_write_ms = timingTracked && p.WriteCountersTracked && p.AvgWriteMs is { } write
                        ? Math.Round(write, 3)
                        : (double?)null,
                    read_bytes_per_second = bytesMeasured || bytesEstimated
                        ? Math.Round(p.ReadBytesPerSecond, 1)
                        : (double?)null,
                    write_bytes_per_second = (bytesMeasured || bytesEstimated) && p.WriteCountersTracked
                        ? Math.Round(p.WriteBytesPerSecond, 1)
                        : (double?)null,
                    counter_reset = p.CounterReset,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_io_trend", ex);
        }
    }

    [McpServerTool(Name = "get_pg_database_trend"), Description("Gets a time series for ONE PostgreSQL database from pg_stat_database: temp-file spills, the buffer-cache hit ratio, deadlocks and the rollback share, interval by interval. This is where the cache hit ratio becomes usable at all - pg_stat_database's counters are cumulative since the last reset, so the ratio computed from them raw is a lifetime average that barely moves, and a database that fell off a cliff an hour ago still reports 99% because of the weeks behind it. Differenced per interval, the cliff is visible. Temp files are the same story: 'this database spilled 40 GB this week' does not say whether it was one bad afternoon or a steady leak, and the two have different fixes. Deadlocks are reported as a COUNT per interval rather than a rate, because they are discrete server-recorded events. Omit database to follow the biggest temp-file spiller. PostgreSQL's shared-relations row - the cluster-wide catalog, which has a NULL database name - is followable by passing '(shared relations)' but is never chosen automatically. An interval spanning a pg_stat_reset or a crash restart is flagged and reports everything since the reset, rather than the quiet interval that clamping the difference at zero would show. Works on every PostgreSQL major and on a standby, where sorts spill exactly the way they do on a writer.")]
    public static async Task<string> GetPgDatabaseTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("The database to follow. Pass '(shared relations)' for PostgreSQL's cluster-wide catalog row. Omit to follow the biggest temp-file spiller in the window.")] string? database = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var requested = !string.IsNullOrWhiteSpace(database);
            string? chosen;

            if (requested)
            {
                var trimmed = database!.Trim();

                /* The label the single-window read prints for PostgreSQL's NULL datname row is accepted back
                   as input. Without this the shared-relations series would be visible in one read and
                   unaskable in the other, which is the kind of gap that makes a reader assume the data is
                   missing rather than the parameter is. */
                chosen = string.Equals(trimmed, DarlingMcpPgDatabaseTools.SharedRelationsLabel, StringComparison.OrdinalIgnoreCase)
                    ? null
                    : trimmed;
            }
            else
            {
                chosen = await DarlingPgTrendReader.GetTopDatabaseAsync(
                    postgres, resolved.ServerId, start, windowEnd);

                if (chosen is null)
                {
                    return await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_database_stats")
                        ?? McpHelpers.Status(
                            "empty",
                            $"No database recorded block accesses or temp files on {resolved.ServerName} in "
                            + $"the last {hours_back} hour(s), so there is nothing to follow.");
                }
            }

            var points = await DarlingPgTrendReader.GetDatabaseTrendAsync(
                postgres, resolved.ServerId, chosen, start, windowEnd);

            var label = chosen ?? DarlingMcpPgDatabaseTools.SharedRelationsLabel;

            if (points.Count == 0)
            {
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_database_stats");
                if (gated != null)
                {
                    return gated;
                }

                /* The denominator, from the SAME relation the trend walks: pg_stat_database is a periodic
                   surface, so any stored sample proves somebody looked, and TWO are needed before a
                   cumulative counter can be differenced at all. A server whose first cycle has just run is
                   in exactly that state, and telling its operator the database was quiet would be a
                   confident wrong answer for as long as it takes the second cycle to land. */
                var (samplesInWindow, everCollected) = await DarlingPgDatabaseReader.GetCoverageAsync(
                    postgres, resolved.ServerId, start, windowEnd);

                return McpHelpers.Status(
                    "empty",
                    $"No differenced intervals for database '{label}' on {resolved.ServerName} in the last "
                    + $"{hours_back} hour(s). "
                    + (samplesInWindow >= 2
                        ? "The collector has samples in this window, so this database is either not one of "
                          + "the ones it reports or its name is spelled differently - get_pg_database_stats "
                          + "lists what the server actually reports."
                        : samplesInWindow == 1
                            ? "Only ONE snapshot exists in this window and a cumulative counter needs two "
                              + "before it can be differenced, so this is too early rather than quiet - the "
                              + "next collection cycle fills it."
                            : everCollected
                                ? "No snapshot at all landed in this window, though this server has been "
                                  + "collected before. get_collection_health says whether the sweep is "
                                  + "running."
                                : "This server has never had pg_database_stats collected."));
            }

            var totalTempFiles = points.Sum(p => p.TempFiles);
            var totalTempBytes = points.Sum(p => p.TempBytes);
            var totalHits = points.Sum(p => p.BlksHit);
            var totalReads = points.Sum(p => p.BlksRead);
            var totalAccesses = totalHits + totalReads;
            var totalCommits = points.Sum(p => p.XactCommit);
            var totalRollbacks = points.Sum(p => p.XactRollback);
            var spilled = points.Where(p => p.TempFiles > 0).ToList();
            var rated = points.Where(p => p.CacheHitPct.HasValue).ToList();
            var resets = points.Count(p => p.CounterReset);

            double? windowHitPct = totalAccesses > 0
                ? Math.Round((double)totalHits / totalAccesses * 100, 2)
                : null;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                database = label,
                is_shared_relations = chosen is null,
                database_source = requested
                    ? "as requested"
                    : "chosen automatically: the biggest temp-file spiller in this window, or the busiest by "
                      + "block access when nothing spilled. PostgreSQL's shared-relations row is never "
                      + "chosen automatically - it is the cluster-wide catalog and never the database in "
                      + "trouble - but it is followable by passing '(shared relations)'.",
                hours_back,
                status = "database_trend",
                point_count = points.Count,
                counter_reset_count = resets,
                intervals_with_temp_files = spilled.Count,
                total_temp_files = totalTempFiles,
                total_temp_bytes = totalTempBytes,
                total_deadlocks = points.Sum(p => p.Deadlocks),
                /* The RANGE, not just the average. The average over the window is what the single-window
                   read already gives; the reason to difference per interval is that a database can average
                   99% and still have spent twenty minutes at 40%, and only the floor says so. */
                cache_hit_pct_window = windowHitPct,
                worst_interval_cache_hit_pct = rated.Count > 0
                    ? Math.Round(rated.Min(p => p.CacheHitPct!.Value), 2)
                    : (double?)null,
                worst_interval_at = rated.Count > 0
                    ? rated.OrderBy(p => p.CacheHitPct!.Value).First().CollectionTimeUtc
                    : (DateTime?)null,
                peak_temp_bytes_per_second = Math.Round(points.Max(p => p.TempBytesPerSecond), 1),
                /* The same prose the single-window read serves, from the same method: the scale of a spill
                   decides whether work_mem is the answer or a plan is, and two copies of that judgement is
                   how it stops being one judgement. */
                spill_finding = DarlingMcpPgDatabaseTools.SpillFinding(totalTempFiles, totalTempBytes),
                cache_finding = DarlingMcpPgDatabaseTools.CacheHitFinding(windowHitPct),
                xact_commit = totalCommits,
                xact_rollback = totalRollbacks,
                rollback_finding = DarlingMcpPgDatabaseTools.RollbackFinding(totalCommits, totalRollbacks),
                note = "Every figure is the difference between CONSECUTIVE snapshots, normalised per SECOND "
                     + "by the interval's own length where it is a rate. cache_hit_pct and rollback_pct are "
                     + "NULL rather than zero for an interval with no block accesses or no completed "
                     + "transactions, because a ratio over nothing is absent rather than bad. deadlocks is a "
                     + "COUNT for the interval, not a rate: they are discrete events, and per-second would "
                     + "render every real one as four leading zeros."
                     + (resets > 0
                         ? $"  {resets} interval(s) span a statistics RESET - pg_stat_reset(), or a crash "
                           + "restart discarding the statistics - and report everything since the reset "
                           + "rather than the quiet interval that clamping at zero would have shown."
                         : string.Empty),
                points = points.Select(p => new
                {
                    collection_time = p.CollectionTimeUtc,
                    transactions_per_second = Math.Round(p.TransactionsPerSecond, 3),
                    rollback_pct = p.RollbackPct is { } rb ? Math.Round(rb, 2) : (double?)null,
                    cache_hit_pct = p.CacheHitPct is { } hit ? Math.Round(hit, 2) : (double?)null,
                    temp_files = p.TempFiles,
                    temp_bytes = p.TempBytes,
                    temp_bytes_per_second = Math.Round(p.TempBytesPerSecond, 1),
                    deadlocks = p.Deadlocks,
                    counter_reset = p.CounterReset,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_database_trend", ex);
        }
    }
}
