/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over the store's OWN server-log census (#3021) — the second self-monitoring surface beside
/// <c>get_store_metrics</c>, and the one that answers what the store COMPLAINED about rather than how big it
/// got.
///
/// <para>A NEW tool rather than a facet of <c>get_collection_health</c>, deliberately: collection health is
/// about the monitored servers' collectors, and the store's own runtime is a different subject with a
/// different denominator. Folding it in would put a store-side fact under a per-server heading, which is the
/// shape that makes a reader mis-attribute it.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreLogTools
{
    /// <summary>Default rows of retained-message detail. Small on purpose: the census above it is the answer,
    /// and the detail is what you read after the census points at something.</summary>
    public const int DefaultRetainedLimit = 50;

    [McpServerTool(Name = "get_store_log"), Description(
        "Gets what the monitoring store's OWN PostgreSQL server log recorded — not a monitored server's. The service reads its own store's log hourly, classifies every entry, and stores a per-class census rather than the lines, because a production day of that log holds roughly 1,100 'canceling statement due to user request' entries: the store's rendering of a client-side CommandTimeout cancel, which is the ordinary consequence of having timeouts and not a fault. So each class comes back with its window total, its per-hour median and max over the same window (computed over EVERY captured hour, including the ones the class did not appear in), and whether its text is retained. Classes whose text is NOT retained are the expected floor, named and counted so the exclusion is a number you can see rather than a filter you cannot: user-request cancels, administrator terminations of background workers and connections, and the Windows shared-memory-reservation retry that PostgreSQL retries itself. Classes whose text IS retained are the ones worth reading one at a time: crash recovery, data-integrity and disk complaints, background-worker slot exhaustion (the silent stopper for compression and continuous-aggregate jobs), the store deadlocking against itself, connections lost mid-statement (the signature of a refresh convoy), the store's own statement and lock timeouts, panics, and — this is the safety net — anything at WARNING or worse that no rule recognises, which keeps its text so a shape nobody anticipated is visible rather than filtered. Every answer carries the CAPTURE DENOMINATOR: how many hourly captures landed against how many the window expected, how many bytes were read, how many captures discarded a resume marker because the weekday log ring truncated the file, and how many bytes are still unread. That denominator is what separates 'the store said nothing' from 'nobody read the log'. NO health band is applied and none is intended: a quiet store with a large cancel floor is healthy and must keep reading that way. Takes no server_name — the store is the subject.")]
    public static async Task<string> GetStoreLog(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 168.")] int hours_back = 24,
        [Description("Maximum retained-message rows to return. Default 50.")] int limit = DefaultRetainedLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            /* The denominator FIRST, because it is what makes an empty answer honest — the get_pg_blocking
               ordering, for its reason. */
            var captures = await DarlingStoreLogReader.GetCaptureSummaryAsync(
                postgres, windowStart, windowEnd, hours_back);

            if (captures.Captures == 0)
            {
                /* Two different empties, and they are not interchangeable. Zero captures means nobody read
                   the log in this window; it is NOT "the store had nothing to say", and #1852's whole point
                   is that those must not look alike. The store-log sweep rides the hourly self-metrics tick,
                   so the first capture lands within an hour of the service starting on a store at V111 or
                   later. */
                return McpHelpers.Status(
                    "not_collected",
                    $"No store-log captures in the last {hours_back} hour(s), so this window makes no claim "
                    + "about what the store's log holds. The service reads its own store's log on the hourly "
                    + "self-metrics tick; the first capture lands within an hour of starting on a store at "
                    + "schema V111 or later. A store the operator brought themselves keeps whatever logging "
                    + "configuration its owner gave it, and this read is empty if the server has no logging "
                    + "collector writing files for pg_ls_logdir() to list.");
            }

            var classes = await DarlingStoreLogReader.GetClassCensusAsync(postgres, windowStart, windowEnd);
            var retained = await DarlingStoreLogReader.GetRetainedEventsAsync(
                postgres, windowStart, windowEnd, limit);

            var report = new DarlingStoreLogReader.StoreLogReport
            {
                WindowHours = hours_back,
                AsOf = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                Captures = captures,
                Classes = classes,
                ClassesNotSeen = DarlingStoreLogReader.ComputeAbsentClasses(classes),
                RetainedEvents = retained,
            };

            return JsonSerializer.Serialize(report, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_log", ex);
        }
    }
}
