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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_change_windows</c> (#9485): the planned change windows and what each held. A request that opens inside a window, or in the 30 minutes
/// after it, is created held (<see cref="InvestigationChangeWindows"/>). This read gives one person the whole window in one pass: its servers,
/// span, reason and state, how many requests it held and released, and the counts by family and by server. With <c>window_id</c> it also lists
/// the requests the window held. It reads the store only and writes nothing.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpChangeWindowTools
{
    internal const int DefaultHoursBack = 168;
    internal const int MaxHoursBack = 8760;
    internal const int DefaultLimit = 20;
    internal const int MaxLimit = 100;

    /// <summary>This tool's entries on the web read surface and in Custom Views validation, kept here and not in the shared lists (#9477).</summary>
    internal sealed class Registration : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            "get_change_windows",
            (c, pg, an) => GetChangeWindows(pg, Number(c, "window_id"), (int?)Number(c, "hours_back") ?? DefaultHoursBack,
                (int?)Number(c, "limit") ?? DefaultLimit, (int?)Number(c, "offset") ?? 0, c.RequestAborted),
            new(
                DarlingWebEndpoints.CatAlerts,
                "Planned change windows and what each held: servers, span, tail end, reason, state (upcoming, open, in_tail, closed), held and released counts, and counts by family and by server. One window by id also lists the requests it held.",
                new[]
                {
                    DarlingWebEndpoints.PInt("window_id"), DarlingWebEndpoints.PInt("hours_back", DefaultHoursBack),
                    DarlingWebEndpoints.PLimit(DefaultLimit), DarlingWebEndpoints.PInt("offset", 0),
                }));

        /// <summary>A whole-number query value, or null when absent or not a whole number (the tool then defaults).</summary>
        private static long? Number(HttpContext context, string key) =>
            long.TryParse(context.Request.Query[key].ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : null;
    }

    private static InvestigationRequestStore Store(NpgsqlDataSource postgres) =>
        new(postgres, ServiceCommandDeadlines.SerialLoopSeconds) { ReadServerIds = DarlingServerResolver.WebAllowedServerIds };

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_change_windows"), Description(
        "Lists the planned change windows and what each held. A request that opens inside a window, or within 30 minutes after its end, is "
        + "created held: it is listable and claimable but does not wake wait_for_investigation_requests. A late alert that joins a held request "
        + "after the tail releases the hold. Writes nothing. status ok or empty. <<GUIDE>> "
        + "Without window_id: the windows whose tail ended within hours_back hours (1-8760, default 168) or that have not ended, newest end first, "
        + "each with its summary only. With window_id: that one window, and the requests it held, oldest first. "
        + "Each window: window_id, servers ('all' or the server names), starts_at, ends_at, tail_ends_at (ends_at plus 30 minutes), reason, "
        + "declared_by, declared_at, cleared_at (null unless cleared early), state (upcoming, open, in_tail or closed), held (every request the "
        + "window held), released (the ones a late join let go), still_held, by_family and by_server (counts of held), summarized (true once the "
        + "close summary was journaled). Each held request: request_id, server_name, family, opened_at, state (as list_investigation_requests "
        + "computes it) and released_at (null while still held). limit (1-100, default 20) caps the windows, or the held requests with window_id. "
        + "truncated says rows were left out, and next_offset is the offset of the next page (pass it as offset).")]
    public static Task<string> GetChangeWindows(
        NpgsqlDataSource postgres,
        [Description("One window's id. Omit for the windows in the span.")] long? window_id = null,
        [Description("1-8760. Default 168. Ignored with window_id.")] int hours_back = DefaultHoursBack,
        [Description("1-100. Default 20.")] int limit = DefaultLimit,
        [Description("Rows to skip. Default 0.")] int offset = 0,
        CancellationToken cancellationToken = default) =>
        McpHelpers.ValidateHoursBack(hours_back, MaxHoursBack) is { } badWindow
            ? Task.FromResult(badWindow)
            : GetChangeWindowsAsync(Store(postgres), window_id, hours_back, limit, offset, cancellationToken);

    internal static async Task<string> GetChangeWindowsAsync(
        InvestigationRequestStore store, long? windowId, int hoursBack, int limit, int offset, CancellationToken cancellationToken)
    {
        if (windowId is < 1)
            return McpHelpers.Refusal("window_id", $"Invalid window_id value '{windowId}'. Must be a positive integer.");
        if (McpHelpers.ValidateHoursBack(hoursBack, MaxHoursBack) is { } badWindow)
            return badWindow;
        if (limit < 1 || limit > MaxLimit)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be an integer from 1 to {MaxLimit}.");
        if (offset < 0)
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be zero or a positive integer.");

        try
        {
            var read = await store.ReadChangeWindowsAsync(windowId, hoursBack, limit, offset, cancellationToken);
            var windows = read.Windows.Select(w => new
            {
                window_id = w.Window.WindowId,
                servers = w.Servers is null ? (object)"all" : w.Servers,
                starts_at = Utc(w.Window.StartsAt),
                ends_at = Utc(w.Window.EndsAt),
                tail_ends_at = Utc(w.TailEndsAt),
                reason = w.Window.Reason,
                declared_by = w.Window.DeclaredBy,
                declared_at = Utc(w.Window.DeclaredAt),
                cleared_at = w.Window.ClearedAt is DateTime c ? Utc(c) : (DateTime?)null,
                state = w.State,
                held = w.Held,
                released = w.Released,
                still_held = w.Held - w.Released,
                by_family = w.ByFamily,
                by_server = w.ByServer,
                summarized = w.Window.SummarizedAt is not null,
            }).ToList();

            var empty = windows.Count == 0;
            return JsonSerializer.Serialize(new
            {
                status = empty ? "empty" : "ok",
                message = empty
                    ? windowId is null ? $"No change window ended within the last {hoursBack} hours or is still to come." : $"No change window {windowId} is on record."
                    : null,
                windows_returned = windows.Count,
                truncated = read.Truncated,
                next_offset = read.NextOffset,
                windows,
                held_returned = read.HeldRequests?.Count,
                held_requests = read.HeldRequests?.Select(r => new
                {
                    request_id = r.RequestId,
                    server_name = r.ServerName,
                    family = r.Family,
                    opened_at = Utc(r.OpenedAt),
                    state = r.State,
                    released_at = r.ReleasedAt is DateTime released ? Utc(released) : (DateTime?)null,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_change_windows", ex);
        }
    }

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
