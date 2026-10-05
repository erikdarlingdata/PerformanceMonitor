/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The slow-read MCP surface (#5097): the reads the monitoring tool itself answered slowly or badly, with the
/// arguments they were asked with, the source that answered, and each statement's time. Reads
/// <c>collect.slow_reads</c>, which <see cref="SlowReadLog"/> fills. Store-scoped; <c>server_name</c> only filters.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpSlowReadTools
{
    public const int DefaultHours = 24;
    public const int DefaultLimit = 20;
    public const int MaxLimit = 100;

    /// <summary>How many statements a row shows without <c>full_detail</c>.</summary>
    internal const int DefaultStatements = 5;

    private static readonly string[] s_surfaces = { "web", "compose", "mcp" };

    [McpServerTool(Name = "get_slow_reads"), Description(
        "Gets the monitoring tool's OWN slow or failed reads — each with the arguments it was asked with, the source "
        + "that answered, and its slowest statements. Not a monitored SQL Server or PostgreSQL target. Newest first. "
        + "Optional server_name, surface (web/compose/mcp) and route filter. Empty means none was recorded, not a failure. "
        + "<<GUIDE>> "
        + "Gets the reads the monitoring tool itself answered slowly or badly. The service records one row per read on any "
        + "surface (web dashboard, composed panel, MCP tool) when the read took 5 seconds or more, or ended in a timeout, "
        + "error or limit. A cancelled read or a fallback is recorded only when it was also slow; their counts live in "
        + "get_read_latency. Each row carries surface, route, outcome, total_ms, the server it named (server_name, null for "
        + "a store-wide read), the arguments as sent (the server replaced by its id, a window lifted into window_start and "
        + "window_end, 4,096 bytes at most), the source that answered (raw, interval_table, hourly_edges) with its reason "
        + "where the read noted one, error_class (a type name or SQLSTATE, never message text), and statements: the "
        + "slowest 5 by ms, with statements_omitted saying how many more were left out; full_detail=true returns all stored "
        + "statements in the order they ran (50 at most, statements_truncated says more ran). A statement's ms runs until "
        + "its reader CLOSES, so it includes row streaming and the caller's time between rows, not only server time; rows "
        + "is filled only by the reads that count them and is null elsewhere. A statement label is the product's own SQL "
        + "text with a short hash; parameter values are never stored. summary counts reads per (surface, route, outcome) "
        + "over the window and the filters, so it covers more than the page. reads is newest first, then newest id; the "
        + "page is capped at limit rows or the ~32 KB response budget, whichever cuts first, and truncated says either "
        + "cut applied. Every instant is UTC from the store's own clock, so there are no *_server_local fields. "
        + "Empty means no read was recorded for the window and filters, which on a fresh store or one below schema V162 "
        + "is the expected answer.")]
    public static async Task<string> GetSlowReads(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 168.")] int hours = DefaultHours,
        [Description("Optional: scope to one monitored server.")] string? server_name = null,
        [Description("Optional: web, compose or mcp.")] string? surface = null,
        [Description("Optional: one route (tool or read name).")] string? route = null,
        [Description("Rows to return. Default 20; max 100.")] int limit = DefaultLimit,
        [Description("True returns every stored statement, in run order.")] bool full_detail = false,
        CancellationToken cancellationToken = default)
    {
        var hoursError = McpHelpers.ValidateHoursBack(hours);
        if (hoursError != null)
        {
            return hoursError;
        }

        if (limit <= 0 || limit > MaxLimit)
        {
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be 1-{MaxLimit}.");
        }

        var surfaceError = McpHelpers.ValidateChoice(surface, s_surfaces, "surface");
        if (surfaceError != null)
        {
            return surfaceError;
        }

        try
        {
            int? serverId = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error != null)
                {
                    return error;
                }

                serverId = resolved.ServerId;
            }

            var since = DateTime.UtcNow.AddHours(-hours);
            var page = await DarlingSlowReadReader.GetAsync(postgres, since, serverId, surface, route, limit, cancellationToken);
            if (page.Summary.Count == 0 && page.Reads.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No slow read recorded in the last {hours} hour(s)" +
                    (serverId is null && string.IsNullOrWhiteSpace(surface) && string.IsNullOrWhiteSpace(route) ? "" : " for that filter") +
                    ". The service records a read that took 5 seconds or more, or ended in a timeout, error or limit; " +
                    "a store before schema V162, or a quiet window, has none.");
            }

            return BuildResponse(hours, server_name, surface, route, page, limit, full_detail, McpResponseBudget.DefaultBytes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_slow_reads", ex);
        }
    }

    /// <summary>The statements a row shows: the slowest <see cref="DefaultStatements"/> by ms desc then ordinal asc, or all in run order.</summary>
    internal static (List<object> Shown, int Omitted) ShapeStatements(JsonArray statements, bool fullDetail)
    {
        var parsed = statements
            .OfType<JsonObject>()
            .Select(s => (
                Ordinal: s["ordinal"]?.GetValue<int>() ?? 0,
                Label: s["label"]?.GetValue<string>() ?? string.Empty,
                Ms: Math.Round(s["ms"]?.GetValue<double>() ?? 0, 1, MidpointRounding.AwayFromZero),
                Rows: s["rows"]?.GetValue<long?>()))
            .ToList();

        var chosen = fullDetail
            ? parsed.OrderBy(s => s.Ordinal).ToList()
            : parsed.OrderByDescending(s => s.Ms).ThenBy(s => s.Ordinal).Take(DefaultStatements).ToList();

        return (chosen.Select(s => (object)new { ordinal = s.Ordinal, label = s.Label, ms = s.Ms, rows = s.Rows }).ToList(),
            parsed.Count - chosen.Count);
    }

    /// <summary>A one-line summary of a row's three slowest statements, built here so the browser computes nothing.</summary>
    internal static string StatementSummary(JsonArray statements)
    {
        var top = statements
            .OfType<JsonObject>()
            .Select(s => (
                Ordinal: s["ordinal"]?.GetValue<int>() ?? 0,
                Label: s["label"]?.GetValue<string>() ?? string.Empty,
                Ms: Math.Round(s["ms"]?.GetValue<double>() ?? 0, 1, MidpointRounding.AwayFromZero),
                Rows: s["rows"]?.GetValue<long?>()))
            .OrderByDescending(s => s.Ms)
            .ThenBy(s => s.Ordinal)
            .Take(3);

        return string.Join("; ", top.Select(s =>
        {
            var label = s.Label.Length > 40 ? s.Label[..40] : s.Label;
            var rows = s.Rows is { } r ? $", {r.ToString("N0", CultureInfo.InvariantCulture)} rows" : string.Empty;
            return $"#{s.Ordinal} {label} {s.Ms.ToString("N0", CultureInfo.InvariantCulture)} ms{rows}";
        }));
    }

    internal static object ShapeRow(DarlingSlowReadReader.SlowReadRow r, bool fullDetail)
    {
        var (shown, omitted) = ShapeStatements(r.Statements, fullDetail);
        return new
        {
            read_time = McpHelpers.FormatEffectiveStart(r.ReadTime),
            surface = r.Surface,
            route = r.Route,
            outcome = r.Outcome,
            total_ms = r.TotalMs,
            server_name = r.ServerName,
            window_start = McpHelpers.FormatEffectiveStart(r.WindowStart),
            window_end = McpHelpers.FormatEffectiveStart(r.WindowEnd),
            arguments = r.Arguments,
            arguments_truncated = r.ArgumentsTruncated,
            source = r.Source,
            source_reason = r.SourceReason,
            error_class = r.ErrorClass,
            statement_count = r.StatementCount,
            statements_truncated = r.StatementsTruncated,
            statements_omitted = omitted,
            statement_summary = StatementSummary(r.Statements),
            statements = shown,
        };
    }

    /// <summary>
    /// Serializes the page: the first <paramref name="limit"/> rows, further cut to fit <paramref name="budgetBytes"/>
    /// the way <see cref="DarlingMcpReadLatencyTools.BuildResponse"/> does it: rows are taken greedily, then the REAL
    /// envelope is serialized and the last row dropped while it is over budget with more than one row left.
    /// </summary>
    internal static string BuildResponse(
        int hours, string? serverName, string? surface, string? route, DarlingSlowReadReader.Page page, int limit, bool fullDetail, int budgetBytes)
    {
        var summary = page.Summary
            .OrderByDescending(s => s.Count)
            .ThenBy(s => s.Surface, StringComparer.Ordinal)
            .ThenBy(s => s.Route, StringComparer.Ordinal)
            .ThenBy(s => s.Outcome, StringComparer.Ordinal)
            .Select(s => (object)new { surface = s.Surface, route = s.Route, outcome = s.Outcome, count = s.Count })
            .ToArray();
        var total = page.Summary.Sum(s => s.Count);

        var shaped = page.Reads
            .OrderByDescending(r => r.ReadTime)
            .ThenByDescending(r => r.SlowReadId)
            .Take(limit)
            .Select(r => ShapeRow(r, fullDetail))
            .ToList();

        string Envelope(IReadOnlyList<object> rows, bool truncated) => JsonSerializer.Serialize(new
        {
            hours,
            server_name = serverName,
            surface,
            route,
            truncated,
            reads_returned = rows.Count,
            reads_total = total,
            summary,
            reads = rows,
        });

        var kept = new List<object>();
        var running = Encoding.UTF8.GetByteCount(Envelope(Array.Empty<object>(), true));
        foreach (var row in shaped)
        {
            var added = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(row)) + (kept.Count == 0 ? 0 : 1);
            if (kept.Count > 0 && running + added > budgetBytes)
            {
                break;
            }

            running += added;
            kept.Add(row);
        }

        var result = Envelope(kept, kept.Count < total);
        while (Encoding.UTF8.GetByteCount(result) > budgetBytes && kept.Count > 1)
        {
            kept.RemoveAt(kept.Count - 1);
            result = Envelope(kept, true);
        }

        return result;
    }
}
