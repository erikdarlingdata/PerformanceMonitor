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

    [McpServerTool(Name = "get_pg_io_stats"), Description("Gets PostgreSQL I/O attributed to WHO did it, to WHAT, and WHY - the (backend_type, object, context) breakdown from pg_stat_io, differenced across the requested window. Richer than SQL Server's file-level dm_io_virtual_file_stats: instead of 'this file is busy' you get 'autovacuum workers are reading relations in the vacuum context', which names the cause. The context dimension is the one with no SQL Server equivalent and the one that changes the remedy - it separates ordinary buffer-pool misses (where more shared_buffers or a better index helps) from sequential scans that deliberately bypass the pool via a ring buffer (where it will not help at all), from vacuum's ring buffer, from a standby applying WAL. Reports whether write counters are TRACKED at all, because on Amazon Aurora they are always null - backends there do not write data files, the storage layer does - and a zero would otherwise read as 'no writes happened'. Requires PostgreSQL 16 or later; valid on a standby.")]
    public static async Task<string> GetPgIoStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum (backend_type, object, context) combinations to return, busiest first. Default 20.")] int limit = 20,
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
            var rows = await DarlingPgIoReader.GetPgIoAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit);

            if (rows.Count == 0)
            {
                /* Ask the engine BEFORE offering the idle-server reading (#2532). "No combination recorded
                   activity" is a statement about a PostgreSQL instance; said about a SQL Server target it
                   is not a weak answer but a false one, and it is the one an agent asking by name gets. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_io_stats");
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

            var totalReads = rows.Sum(r => r.Reads);
            var totalReadTime = rows.Sum(r => r.ReadTimeMs);

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
                    read_time_ms = Math.Round(r.ReadTimeMs, 1),
                    /* Per-read latency is the figure that separates "a lot of I/O" from "slow I/O", and
                       they have completely different remedies. */
                    avg_read_ms = r.Reads > 0 ? Math.Round(r.ReadTimeMs / r.Reads, 3) : (double?)null,
                    hits = r.Hits,
                    /* A hit ratio scoped to this combination, which is the only scope where it means
                       anything: a server-wide ratio averages bulkread's deliberate misses together with
                       normal-context misses and understates both. */
                    hit_pct = accesses > 0 ? Math.Round((double)r.Hits / accesses * 100, 1) : (double?)null,
                    pct_of_total_reads = totalReads > 0 ? Math.Round((double)r.Reads / totalReads * 100, 1) : 0,
                    pct_of_total_read_time = totalReadTime > 0 ? Math.Round(r.ReadTimeMs / totalReadTime * 100, 1) : 0,
                    extends = r.Extends,
                    extend_time_ms = Math.Round(r.ExtendTimeMs, 1),
                    evictions = r.Evictions,
                    /* Ring-buffer reuse, NOT eviction pressure. Conflating the two is the standard
                       misreading of this view: reuses are a bulk operation recycling its OWN buffers. */
                    reuses = r.Reuses,
                    writes = r.WriteCountersTracked ? r.Writes : (long?)null,
                    write_time_ms = r.WriteCountersTracked ? Math.Round(r.WriteTimeMs, 1) : (double?)null,
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
                server = resolved.ServerName,
                hours_back,
                status = "io_activity",
                combination_count = combinations.Count,
                total_reads = totalReads,
                total_read_time_ms = Math.Round(totalReadTime, 1),
                busiest_by_read_time = $"{rows[0].BackendType}/{rows[0].ObjectType}/{rows[0].Context}",
                /* Said once at the top rather than repeated per row: on Aurora this is false everywhere,
                   and a caller needs to know the write side is unmeasured before it concludes anything
                   from the absence of writes. */
                write_counters_tracked_anywhere = anyWritesTracked,
                bytes_source = bytesMeasured
                    ? "measured"
                    : (bytesEstimated ? "estimated_from_block_size" : "unavailable"),
                note = anyWritesTracked
                    ? "All counters are windowed differences, clamped per interval so a stats reset cannot "
                    + "produce a negative figure."
                    : "All counters are windowed differences. This server tracks NO write counters — the "
                    + "signature of Amazon Aurora, where backends do not write data files and the storage "
                    + "layer does. Absent writes here mean unmeasured, not zero.",
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
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL I/O stats failed: {ex.Message}");
        }
    }
}
