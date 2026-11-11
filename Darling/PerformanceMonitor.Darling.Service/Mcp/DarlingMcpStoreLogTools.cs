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
using System.Threading;
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
        "Gets the monitoring STORE's OWN PostgreSQL log, not a monitored server's; no server_name. Returns a "
        + "per-class CENSUS (window total, per-hour median/max over every captured hour), not raw lines. "
        + "not_collected (captures=0) means nobody read the log, not that nothing happened. NOT-retained "
        + "classes are named+counted as the expected floor (cancels, admin terminations, memory retries); NO "
        + "health band: a large cancel floor is healthy. Every answer carries the capture denominator "
        + "(captures vs expected, bytes read/unread) separating a quiet store from an unread log. <<GUIDE>> "
        + "Gets what the monitoring store's OWN PostgreSQL server log recorded, not a monitored server's. The service reads its own store's log hourly, classifies every entry, and stores a per-class census rather than the lines, because a production day of that log holds roughly 1,100 'canceling statement due to user request' entries: the store's rendering of a client-side CommandTimeout cancel, which is the ordinary consequence of having timeouts and not a fault. So each class comes back with its window total, its per-hour median and max over the same window (computed over EVERY captured hour, including the ones the class did not appear in), and whether its text is retained. Classes whose text is NOT retained are the expected floor, named and counted so the exclusion is a number you can see rather than a filter you cannot: user-request cancels, administrator terminations of background workers and connections, and the Windows shared-memory-reservation retry that PostgreSQL retries itself. Classes whose text IS retained are the ones worth reading one at a time: crash recovery, data-integrity and disk complaints, background-worker slot exhaustion (the silent stopper for compression and continuous-aggregate jobs), the store deadlocking against itself, connections lost mid-statement (the signature of a refresh convoy), the store's own statement and lock timeouts, statements that ran past the store's slow-statement line (the statement kept with its literals masked, one row per statement however often it ran), auto_explain plans (kept as redacted JSON, one row per plan, where runs with the same plan and estimates share a row, with the statement's query id when the plan carries one; a plan that cannot be read whole keeps only its duration line), panics, and (this is the safety net) anything at WARNING or worse that no rule recognises, which keeps its text so a shape nobody anticipated is visible rather than filtered. Messages are shown as PostgreSQL wrote them; SQL text is normalized with literals replaced by ?. Messages that differ only in a quoted value or a number count as one, shown with one sample. Every answer carries the CAPTURE DENOMINATOR: how many hourly captures landed against how many the window expected, how many bytes were read, how many captures discarded a resume marker because the weekday log ring truncated the file, and how many bytes are still unread. That denominator is what separates 'the store said nothing' from 'nobody read the log'. NO health band is applied and none is intended: a quiet store with a large cancel floor is healthy and must keep reading that way. Takes no server_name: the store is the subject. A statement cancelled by statement_timeout never finishes, so auto_explain logs no plan for it. "
        + "Each retained row's sample_line is a 400-character preview: sample_line_truncated says it was cut and sample_line_length gives the stored entry's whole length, and full_text = true returns it whole. "
        + "The census is never cut. The retained rows are held to the 32,768-byte response budget, newest capture first, whole rows only: retained_events_returned is how many the answer holds, budget_truncated = true says rows were left off the end for the budget, and budget_note says how many of how many fit. The rows left off are older ones: oldest_returned_last_capture_at is the oldest last_capture_at among the rows returned. The retained rows keep one order for a window: newest capture, then most occurrences, then class, severity and message. offset skips that many of them, so it pages inside one capture, which no as_of divides: one capture keeps up to 20 distinct messages a class. budget_note names the way on: the offset that reads on from the last row returned, with as_of held at the answer's own as_of so the window and its order stay put, and as_of at oldest_returned_last_capture_at, or earlier than it when every row returned is from one capture, for the older captures. An offset past the last retained message answers empty.")]
    public static Task<string> GetStoreLog(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 168.")] int hours_back = 24,
        [Description("Maximum retained-message rows to return. Default 50.")] int limit = DefaultRetainedLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Return each row's whole sample_line instead of a 400-character preview. Default false.")] bool full_text = false,
        [Description("Retained messages to skip before the page, in the answer's order (a page start). Default 0; budget_note names the offset that reads on when budget_truncated is true.")] int offset = 0,
        CancellationToken cancellationToken = default) =>
        GetStoreLog(postgres, hours_back, limit, as_of, full_text, offset, McpResponseBudget.DefaultBytes, cancellationToken);

    /// <summary>Characters of <c>sample_line</c> a default row carries (#8568). A logged plan ran past 6,500
    /// characters and a logged statement past 4,600, each several times the rest of its row.</summary>
    internal const int SampleLinePreviewLength = 400;

    /// <summary>
    /// get_store_log under an explicit <paramref name="responseByteBudget"/> (#8568): the MCP tool passes
    /// <see cref="McpResponseBudget.DefaultBytes"/> and keeps the newest retained rows that fit it, the web viewer's
    /// <c>/api/read</c> mirror and the diagnostics bundle pass null with <paramref name="full_text"/> and no offset,
    /// and keep every row whole.
    /// </summary>
    internal static async Task<string> GetStoreLog(
        NpgsqlDataSource postgres, int hours_back, int limit, string? as_of, bool full_text, int offset, int? responseByteBudget,
        CancellationToken cancellationToken)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        if (offset < 0 || offset > DarlingMcpPgServerStateTools.MaxPageOffset)
        {
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be 0-{DarlingMcpPgServerStateTools.MaxPageOffset}.");
        }

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            /* The denominator FIRST, because it is what makes an empty answer honest — the get_pg_blocking
               ordering, for its reason. */
            var captures = await DarlingStoreLogReader.GetCaptureSummaryAsync(
                postgres, windowStart, windowEnd, hours_back, cancellationToken);

            if (captures.Captures == 0)
            {
                /* Two different empties, and they are not interchangeable. Zero captures means nobody read
                   the log in this window; it is NOT "the store had nothing to say", and #1852's whole point
                   is that those must not look alike. The store-log sweep rides the hourly self-metrics tick,
                   so the first capture lands within an hour of the service starting on a store at V111 or
                   later. */
                return McpHelpers.Status(
                    "not_collected",
                    $"No store-log captures in the last {CountText.Plural(hours_back, "hour")}, so this window makes no claim "
                    + "about what the store's log holds. The service reads its own store's log on the hourly "
                    + "self-metrics tick; the first capture lands within an hour of starting on a store at "
                    + "schema V111 or later. Two conditions make this permanently empty on a store the "
                    + "operator brought themselves rather than one the service provisioned: the server may "
                    + "have no logging collector writing files for pg_ls_logdir() to list, and the role the "
                    + "service connects as needs pg_read_server_files plus EXECUTE on pg_read_binary_file "
                    + "(the managed store's own role is the cluster's bootstrap superuser and has both). "
                    + "The service log carries a warning naming whichever it was.");
            }

            var classes = await DarlingStoreLogReader.GetClassCensusAsync(postgres, windowStart, windowEnd, cancellationToken);
            var (retained, skipped) = await DarlingStoreLogReader.GetRetainedEventsAsync(
                postgres, windowStart, windowEnd, limit, offset, full_text ? null : SampleLinePreviewLength, cancellationToken);
            if (skipped > 0 && retained.Count == 0)
            {
                return McpHelpers.Status("empty", $"offset {offset} is past the {CountText.Plural(skipped, "retained message")} this window holds.");
            }

            var absent = DarlingStoreLogReader.ComputeAbsentClasses(classes);

            /* #8568: the census is the answer and is never cut; the retained rows are the detail under it, so they are
               what gives way, newest first, whole rows only. */
            string Reply(int kept) => JsonSerializer.Serialize(new DarlingStoreLogReader.StoreLogReport
            {
                WindowHours = hours_back,
                AsOf = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                Captures = captures,
                Classes = classes,
                ClassesNotSeen = absent,
                RetainedEvents = kept < retained.Count ? retained.GetRange(0, kept) : retained,
                RetainedEventsSkipped = skipped,
                BudgetTruncated = kept < retained.Count,
                BudgetNote = kept < retained.Count
                    ? McpResponseBudget.PageCutNote(retained.Count, kept, "newest capture first", "retained messages",
                        DarlingStoreLogReader.RetainedCutRemedy(retained, kept, full_text, skipped))
                    : null,
            }, McpHelpers.JsonOptions);

            return responseByteBudget is int budget
                ? McpResponseBudget.FitPrefix(retained.Count, Reply, budget, minimum: 0).Json
                : Reply(retained.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_log", ex);
        }
    }
}
