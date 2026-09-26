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
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for I/O attribution, paired with the <c>pg_io_stats</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgIoTools
{
    /// <summary>
    /// Explains what a <c>context</c> value means, because it is the dimension with no SQL Server
    /// counterpart and the one that changes what you do about a number.
    /// <para>#2530 moved the prose to <see cref="DarlingPgIoReader.ContextMeaning"/>, beside the query that
    /// produces the value it explains, so the MCP surface and the WPF viewer's I/O tab print one copy of it
    /// rather than two that drift. Kept as a delegating member because this file's tests name it.</para>
    /// </summary>
    internal static string ContextMeaning(string? context) => DarlingPgIoReader.ContextMeaning(context);

    [McpServerTool(Name = "get_pg_io_stats"), Description("Gets PostgreSQL I/O attributed to WHO, WHAT and WHY - backend_type/object/context from pg_stat_io, differenced across the window. track_io_timing is OFF by default: when untracked every *_time_ms and avg_read_ms are null, never a false 0.000; busiest_basis names the fallback ranking. Write counters are always null on Aurora, never a true zero. THE PAGE IS BOUNDED BY limit: combinations_returned/truncated. SHARES ARE OF THE WINDOW, NOT OF THE PAGE: pct_of_total_reads/pct_of_total_read_time divide by total_reads/total_read_time_ms; returned_reads states the page's own share. <<GUIDE>> Gets PostgreSQL I/O attributed to WHO did it, to WHAT, and WHY - the (backend_type, object, context) breakdown from pg_stat_io, differenced across the requested window. Richer than SQL Server's file-level dm_io_virtual_file_stats: instead of 'this file is busy' you get 'autovacuum workers are reading relations in the vacuum context', which names the cause. The context dimension is the one with no SQL Server equivalent and the one that changes the remedy - it separates ordinary buffer-pool misses (where more shared_buffers or a better index helps) from sequential scans that deliberately bypass the pool via a ring buffer (where it will not help at all), from vacuum's ring buffer, from a standby applying WAL. Reports whether the server tracks I/O TIMING at all: track_io_timing is OFF by default in PostgreSQL, and its zero read_time would otherwise divide out to a latency of 0.000 ms that reads as an impossibly fast disk rather than an unmeasured one - the time fields are null when untracked, and busiest_basis says what the ranking actually used. Also reports whether write counters are TRACKED at all, because on Amazon Aurora they are always null - backends there do not write data files, the storage layer does - and a zero would otherwise read as 'no writes happened'. Requires PostgreSQL 16 or later; valid on a standby. THE PAGE IS BOUNDED BY limit: combinations_returned is how many combinations you got, truncated says the window held more, and the rows are the busiest so the ones past the cap did less. SHARES ARE OF THE WINDOW, NOT OF THE PAGE: pct_of_total_reads' denominator is total_reads and pct_of_total_read_time's is total_read_time_ms, each the WHOLE window's figure across every combination that moved, computed in the same statement as the rows - so a three-row page does not sum to 100%, and the gap between returned_reads / returned_read_time_ms (what the page adds up to) and the totals is the I/O the cap left out; returned_pct_of_total_reads and returned_pct_of_total_read_time are those ratios stated once.")]
    public static async Task<string> GetPgIoStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum (backend_type, object, context) combinations to return, busiest first. Default 20. Bounds the page - read truncated for more; the shares stay of the whole window whatever this is set to.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            /* #3541 A7: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal - and the window's reads and read time ride on the same statement, so the shares
               below have denominators the cap cannot shrink. */
            var page = await DarlingPgIoReader.GetPgIoPageAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, cancellationToken);

            if (page.Rows.Count == 0)
            {
                /* Ask the engine BEFORE offering the idle-server reading (#2532). "No combination recorded
                   activity" is a statement about a PostgreSQL instance; said about a SQL Server target it
                   is not a weak answer but a false one, and it is the one an agent asking by name gets. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_io_stats", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = "no_io_activity",
                    finding = "No (backend_type, object, context) combination recorded read, write, extend or "
                            + "hit activity in this window. On a busy server that more likely means the "
                            + "collector has not run yet than that the server is idle — pg_stat_io needs "
                            + "PostgreSQL 16 or later, so check the target's major version.",
                }, McpHelpers.JsonOptions);
            }

            /* Asked of the server's OWN configuration rather than inferred from the zeros, exactly as the
               trend sibling asks it (#3536): the two readings a zero latency permits — "the disk is
               instant" and "nobody is timing it" — are not distinguishable in the counters, and
               track_io_timing is OFF by default, so the zeros are the ordinary case rather than a fault. */
            var timingSetting = await DarlingPgTrendReader.GetIoTimingTrackedAsync(
                postgres, resolved.ServerId, windowEnd, cancellationToken);

            return BuildIoJson(resolved.ServerName, hours_back, page, limit, timingSetting);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_io_stats", ex);
        }
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store — the same
    /// reason the plan tools' <c>BuildPlansJson</c> is separate.
    ///
    /// <para><c>timingSetting</c> is the target's own <c>track_io_timing</c> from <c>pg_server_config</c>,
    /// or null when the configuration has not been collected — in which case whether any non-zero time
    /// appears in the window is the only evidence available and is used, stated as inference.</para>
    ///
    /// <para><b>The denominators are the window's, not the page's</b> (#3541 A7). <paramref name="page"/>
    /// carries <c>WindowTotalReads</c> and <c>WindowTotalReadTimeMs</c> from the same statement as its rows, and
    /// <c>pct_of_total_reads</c> / <c>pct_of_total_read_time</c> divide by THOSE. The previous shape divided by
    /// the sums of the rows fetched, so at <c>limit = 3</c> the three shares summed to 100% and read as "these
    /// three combinations are all the I/O". The page's own sums still travel as <c>returned_reads</c> /
    /// <c>returned_read_time_ms</c>, and the ratios of page to window are stated once each.</para>
    ///
    /// <para><paramref name="page"/> holds up to <c>limit + 1</c> rows; the extra one is the truncation signal
    /// and is cut before the projection. The timing inference below runs over the CUT rows: the sentinel is
    /// not on the page, so it must not decide anything the page reports.</para>
    /// </summary>
    internal static string BuildIoJson(
        string serverName,
        int hoursBack,
        DarlingPgIoReader.PgIoPage page,
        int limit,
        bool? timingSetting)
    {
        var truncated = page.Rows.Count > limit;
        var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows;

        var timingObserved = rows.Any(r => r.ReadTimeMs > 0 || r.WriteTimeMs > 0);
        var timingTracked = timingSetting ?? timingObserved;

        var totalReads = page.WindowTotalReads;
        var totalReadTime = page.WindowTotalReadTimeMs;
        var returnedReads = rows.Sum(r => r.Reads);
        var returnedReadTime = rows.Sum(r => r.ReadTimeMs);

        var combinations = rows.Select(r =>
        {
            var accesses = r.Reads + r.Hits;
            return new
            {
                backend_type = r.BackendType,
                object_type = r.ObjectType,
                context = r.Context,
                context_meaning = ContextMeaning(r.Context),
                reads = r.Reads,
                /* Every time figure is null when the server does not measure I/O time (#3536), rather
                   than the 0.0 the arithmetic produces: that zero is a fact about the configuration, and
                   printed as a time it is the most reassuring wrong number here. */
                read_time_ms = timingTracked ? Math.Round(r.ReadTimeMs, 1) : (double?)null,
                /* Per-read latency is the figure that separates "a lot of I/O" from "slow I/O", and
                   they have completely different remedies. */
                avg_read_ms = timingTracked && r.Reads > 0 ? Math.Round(r.ReadTimeMs / r.Reads, 3) : (double?)null,
                hits = r.Hits,
                /* A hit ratio scoped to this combination, which is the only scope where it means
                   anything: a server-wide ratio averages bulkread's deliberate misses together with
                   normal-context misses and understates both. */
                hit_pct = accesses > 0 ? Math.Round((double)r.Hits / accesses * 100, 1) : (double?)null,
                /* Of the WINDOW's totals, never of the page's — see the remarks. */
                pct_of_total_reads = totalReads > 0 ? Math.Round((double)r.Reads / totalReads * 100, 1) : 0,
                pct_of_total_read_time = timingTracked
                    ? (totalReadTime > 0 ? Math.Round(r.ReadTimeMs / totalReadTime * 100, 1) : 0)
                    : (double?)null,
                extends = r.Extends,
                extend_time_ms = timingTracked ? Math.Round(r.ExtendTimeMs, 1) : (double?)null,
                evictions = r.Evictions,
                /* Ring-buffer reuse, NOT eviction pressure. Conflating the two is the standard
                   misreading of this view: reuses are a bulk operation recycling its OWN buffers. */
                reuses = r.Reuses,
                writes = r.WriteCountersTracked ? r.Writes : (long?)null,
                write_time_ms = timingTracked && r.WriteCountersTracked ? Math.Round(r.WriteTimeMs, 1) : (double?)null,
                write_counters_tracked = r.WriteCountersTracked,
                /* The block size an operation moves. Gone from 18, where a read is no longer one
                   block, so it is null there and read_bytes below is measured instead of derived. */
                block_bytes = r.OpBytes > 0 ? r.OpBytes : (long?)null,
                /* One name for the volume answer, and bytes_source says how it was arrived at. From 18
                   these are measured totals; below 18 they are reads x block size. Never both, and
                   never silently swapped: the two are different quantities, and on 18 the old estimate
                   would UNDERCOUNT because a vectored read covers several blocks. */
                read_bytes = r.ByteCountersTracked
                    ? r.ReadBytes
                    : (r.OpBytes > 0 ? r.Reads * r.OpBytes : (decimal?)null),
                write_bytes = r.ByteCountersTracked
                    ? r.WriteBytes
                    : (r.OpBytes > 0 && r.WriteCountersTracked ? r.Writes * r.OpBytes : (decimal?)null),
                extend_bytes = r.ByteCountersTracked ? r.ExtendBytes : (decimal?)null,
                bytes_source = r.ByteCountersTracked
                    ? "measured"
                    : (r.OpBytes > 0 ? "estimated_from_block_size" : "unavailable"),
                stats_reset = r.StatsReset,
            };
        })
        .ToList();

        var anyWritesTracked = rows.Any(r => r.WriteCountersTracked);
        /* #2655: PostgreSQL 18 replaced op_bytes with measured byte totals. Said once at the top for
           the same reason the write flag is: a caller has to know which quantity it is reading before
           it compares two servers, and the two are not comparable. */
        var bytesMeasured = rows.Any(r => r.ByteCountersTracked);
        var bytesEstimated = !bytesMeasured && rows.Any(r => r.OpBytes > 0);

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            status = "io_activity",
            /* #3541 A3 dialect: the page described as a page. combinations_returned is the page's count, in
               the `<noun>s_returned` spelling every other paged tool uses (queries_returned,
               wait_events_returned, waits_returned); truncated beside it says whether the window held more.
               #3613 kept the older `combination_count` only because the web I/O tile read it by key - a
               bare count beside window totals reads as the window's, and the tile now reads this key
               (#3653). No time bounds: each row is one combination differenced across the whole window, so
               there is no page reach to report, only a cap. */
            combinations_returned = combinations.Count,
            truncated,
            order = "read_time_ms_desc_then_reads_desc",
            /* The WINDOW's reads and read time, across every combination that moved — the denominators of
               every pct_of_total_reads / pct_of_total_read_time above. NOT the sums of the rows; those are
               returned_reads / returned_read_time_ms. The read-time total is null under untracked timing
               for the same reason every other time figure is: it is a sum of stored zeros, not a measurement. */
            total_reads = totalReads,
            total_read_time_ms = timingTracked ? Math.Round(totalReadTime, 1) : (double?)null,
            returned_reads = returnedReads,
            returned_read_time_ms = timingTracked ? Math.Round(returnedReadTime, 1) : (double?)null,
            returned_pct_of_total_reads = totalReads > 0 ? Math.Round((double)returnedReads / totalReads * 100, 1) : 0,
            returned_pct_of_total_read_time = timingTracked
                ? (totalReadTime > 0 ? Math.Round(returnedReadTime / totalReadTime * 100, 1) : 0)
                : (double?)null,
            /* The key survives untracked timing for the web tile's sake; busiest_basis beside it says
               what the ranking actually used. The reader orders by read time and then by read count, so
               over a store of zero times the count IS the ordering rather than a tiebreak. */
            busiest_by_read_time = $"{rows[0].BackendType}/{rows[0].ObjectType}/{rows[0].Context}",
            busiest_basis = timingTracked
                ? "total read time (read_time_ms), then read count"
                : "read count (reads) — this server does not measure I/O time, so every read-time figure "
                  + "is zero in the store and cannot rank anything; the ordering falls back to the counter "
                  + "that exists",
            /* Said once at the top rather than repeated per row: on Aurora this is false everywhere,
               and a caller needs to know the write side is unmeasured before it concludes anything
               from the absence of writes. */
            write_counters_tracked_anywhere = anyWritesTracked,
            io_timing_tracked = timingTracked,
            io_timing_source = timingSetting is null
                ? "inferred from the data - this server's configuration has not been collected, so "
                  + "track_io_timing is unknown and the answer here is simply whether any non-zero I/O "
                  + "time appears in the window"
                : "the target's own track_io_timing, as collected into pg_server_config",
            bytes_source = bytesMeasured
                ? "measured"
                : (bytesEstimated ? "estimated_from_block_size" : "unavailable"),
            note = (anyWritesTracked
                ? "All counters are windowed differences, clamped per interval so a stats reset cannot "
                + "produce a negative figure."
                : "All counters are windowed differences. This server tracks NO write counters — the "
                + "signature of Amazon Aurora, where backends do not write data files and the storage "
                + "layer does. Absent writes here mean unmeasured, not zero.")
                + " total_reads and total_read_time_ms are the WHOLE window's figures across every "
                + "combination that moved, computed in the same statement as the rows; each row's "
                + "pct_of_total_reads and pct_of_total_read_time divide by them, so the shares on a page do "
                + "not sum to 100 unless the page is the whole window (truncated = false). returned_reads "
                + "and returned_read_time_ms are what the rows returned add up to.",
            timing_note = timingTracked
                ? "read_time_ms, avg_read_ms, write_time_ms and extend_time_ms are measured I/O times: "
                  + "this server has track_io_timing on."
                : "read_time_ms, avg_read_ms, write_time_ms, extend_time_ms and the read-time shares are "
                  + "NULL throughout because this server does not measure I/O time. track_io_timing is off "
                  + "by DEFAULT in PostgreSQL, so this is the ordinary configuration rather than a fault - "
                  + "but it means the operation counts are the only I/O evidence here, and nothing in this "
                  + "store can say whether the storage is slow. Turning it on costs a clock read per "
                  + "operation; measure that on the platform before enabling it fleet-wide.",
            bytes_note = bytesMeasured
                ? "Byte totals are MEASURED, from PostgreSQL 18's read_bytes/write_bytes/extend_bytes. "
                  + "They are not comparable with the figures a pre-18 server reports, which are "
                  + "reads x block size - 18 reads several blocks per operation, so the older estimate "
                  + "undercounts."
                : (bytesEstimated
                    ? "Byte totals are ESTIMATED as count x block_bytes, which is exact below "
                      + "PostgreSQL 18 because one operation moves one block. PostgreSQL 18 measures "
                      + "them directly instead."
                    : "This server reports no byte figures at all: op_bytes is absent and the measured "
                      + "columns PostgreSQL 18 replaced it with are not being collected. The counts and "
                      + "times above are unaffected."),
            combinations,
        }, McpHelpers.JsonOptions);
    }
}
