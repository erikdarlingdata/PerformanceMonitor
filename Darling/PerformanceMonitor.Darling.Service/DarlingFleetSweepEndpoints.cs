/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The fleet sweep web feed (#3466, lane 3 of 4): the read-only <c>/api/sweeps</c> routes the
/// dashboard's Fleet Sweeps page reads — the sweep timeline at a caller-configurable span, one
/// sweep's full document, and the watch-item worklist. DEDICATED read endpoints like
/// <c>/api/fleet</c> and <c>/api/ag</c>, not <c>/api/read/{tool}</c> mirrors: the sweep has no read
/// tool yet (lane 4's <c>get_sweep_reports</c> arrives against the SAME
/// <see cref="FleetSweepPresentation"/> builders these routes serve), so
/// <see cref="DarlingWebEndpoints.ExcludedToolNames"/> is untouched and the tool-catalog parity pin
/// has nothing of this surface's to count.
///
/// <para><b>Who may call these.</b> Every route is a GET, so both seats reach all of them — the
/// group-level write gate (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/>) grants a read-only
/// seat every safe method, and nothing here consults <c>CanEdit</c>, because a sweep report is
/// exactly the surface the viewer seat exists to grant. The auth middleware (token→cookie + CIDR;
/// loopback tokenless) runs before these routes like the rest of the surface, so no-seat is a 401
/// before any handler here runs. The reads run on the host's least-privilege VIEWER pool, whose
/// blanket <c>collect</c> SELECT already covers the four sweep tables (the lane-1 rung took no ACL
/// decision, verified there).</para>
///
/// <para><b>Span discipline.</b> The timeline takes <c>hours</c> (default 1 — the owner's ruling:
/// viewing spans are user-configurable, DEFAULT HOURLY) and the optional <c>as_of</c> end anchor, and
/// validates BOTH through <see cref="McpHelpers.ValidateWindow"/> — the same authority every windowed
/// MCP read applies, so the two surfaces cannot disagree about what a bad span or a future anchor
/// means, and the refusal prose is the one vocabulary (#2506: both bounds are the caller's; the
/// store statement never says <c>now()</c>). An out-of-range span is REFUSED, not clamped: the
/// timeline is a statement about a window, and silently answering a different window is the
/// misreading the <c>as_of</c> discipline exists to remove.</para>
///
/// <para><b>The cadence knob is read elsewhere, deliberately.</b> The page displays the effective
/// cadence from <c>/api/read/get_alert_settings</c>'s <c>fleet_sweep</c> group — the same read every
/// settings surface makes — and nothing here serves or accepts it: the Settings window and
/// <c>update_alert_settings</c> own the writes (lane 2's V124), and the web feed only reads.</para>
/// </summary>
internal static class DarlingFleetSweepEndpoints
{
    /// <summary>The default timeline span: one hour — the owner's ruling ("configurable spans of
    /// time though, default hourly"), which at the shipped hourly cadence lands the page on the
    /// newest sweep with the span control as the way into history.</summary>
    internal const int DefaultSpanHours = 1;

    /// <summary>
    /// The watch-item states a caller may ask for by name — the state machine's own four words,
    /// derived from its constants so this list and the machine cannot disagree. Absent
    /// <c>?state=</c> means the DEFAULT VIEW: open UNION carried, in one store read
    /// (<see cref="FleetSweepStore.GetOpenAndCarriedWatchItemsSql"/>), because the open state lasts
    /// exactly one sweep by design and "what is open right now" honestly means both.
    /// </summary>
    internal static readonly IReadOnlyList<string> KnownWatchStates = new[]
    {
        FleetSweepWatchStateMachine.Pending,
        FleetSweepWatchStateMachine.Open,
        FleetSweepWatchStateMachine.Carried,
        FleetSweepWatchStateMachine.Closed,
    };

    /// <summary>
    /// Binds and validates the timeline's span knobs — pure over the two raw strings so the refusal
    /// table pins without an HttpContext. An UNREADABLE <c>hours</c> is refused rather than
    /// defaulted (the #3287 filter discipline: a caller who asked for a span they mistyped must not
    /// receive a complete-looking answer to a different one), a readable value goes through
    /// <see cref="McpHelpers.ValidateWindow"/>'s range check, and the anchor is parsed by the same
    /// authority every MCP read uses. Returns null and the resolved window on success, else the
    /// refusal message.
    /// </summary>
    internal static string? ValidateSpan(string? rawHours, string? asOf, out int hours, out DateTime endUtc)
    {
        hours = DefaultSpanHours;
        endUtc = DateTime.UtcNow;

        if (rawHours is not null
            && !int.TryParse(rawHours, NumberStyles.Integer, CultureInfo.InvariantCulture, out hours))
        {
            return $"Invalid hours value '{rawHours}'. Expected a whole number of hours (1-{McpHelpers.MaxHoursBack}).";
        }

        return McpHelpers.ValidateWindow(hours, asOf, out endUtc);
    }

    /// <summary>
    /// Validates the watch-item <c>?state=</c> filter: null (absent) selects the open-union-carried
    /// default view; one of the machine's four states selects that state; anything else is refused
    /// naming the legal values, because an unknown state matches nothing and an empty answer to a
    /// typo is indistinguishable from a clean worklist.
    /// </summary>
    internal static string? ValidateWatchState(string? state)
    {
        if (state is null || KnownWatchStates.Contains(state, StringComparer.Ordinal))
        {
            return null;
        }

        return $"Unknown state '{state}'. Legal values: {string.Join(", ", KnownWatchStates)}; omit for open + carried.";
    }

    /// <summary>
    /// The worklist's names map (#3482), gated on the worklist actually naming a server. The read
    /// scans the retained verdict history in one statement
    /// (<see cref="FleetSweepStore.GetSweepServerNamesAsync"/> — see its SQL's doc for why the newest
    /// sweep's verdicts cannot serve: a carried item can outlive its server's presence in the fleet),
    /// so a worklist that is empty or all fleet-scope skips it — on a healthy fleet that is the
    /// COMMON case, polled every 60 seconds by the page's refresh. One read for the whole batch,
    /// log-and-degrade: a failed read costs the names (the client falls back to the bare id, exactly
    /// the pre-#3482 rendering), never the worklist. Shared with <c>get_sweep_reports</c> the same
    /// way <see cref="ValidateWatchState"/> is, so the two surfaces cannot drift on when — or
    /// whether — names are joined.
    /// </summary>
    internal static async Task<Dictionary<int, string>> ReadWatchItemNamesAsync(
        NpgsqlDataSource postgres,
        IReadOnlyList<FleetSweepWatchItem> items,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        return items.Any(item => item.ServerId != FleetSweepStore.FleetScopeServerId)
            ? await FleetSweepStore.GetSweepServerNamesAsync(postgres, logger, cancellationToken)
            : new Dictionary<int, string>();
    }

    /// <summary>
    /// Maps the sweep feed's routes. Called once from <see cref="DarlingWebEndpoints.MapAll"/>, after
    /// the auth middleware like every other route. Handlers answer through the surface's standard
    /// shapes: data as JSON, refusals as 400 <c>{"error"}</c>, absence as 404, and a store fault as a
    /// 500 <c>{"error"}</c> — the page's own error strip is the degraded rendering, so nothing here
    /// swallows a fault into an answer that reads healthier than the store.
    ///
    /// <para><b>The logger seat is the HOST SERVICE's logger, never <c>app.Logger</c>.</b> The web
    /// host clears the dashboard app's logging providers (the framework-noise decision, stated at its
    /// ClearProviders site), which makes <c>app.Logger</c> a logger with nowhere to write — these
    /// routes' log-and-degrade store reads used to log through it, so every web-path degradation line
    /// vanished. <paramref name="logger"/> is the service's real logger, threaded from
    /// <see cref="Mcp.DarlingWebHostService"/> through <see cref="DarlingWebEndpoints.MapAll"/> —
    /// the web path's copy of the MCP host's <c>AddSingleton&lt;ILogger&gt;(_logger)</c> fix (#3473
    /// review), by parameter rather than DI because these routes are mapped directly.</para>
    /// </summary>
    internal static void Map(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        /* The timeline: runs inside the caller's span, newest first, each with its document embedded.
           Both bounds are computed HERE from the caller's knobs and handed to the store (#2506). */
        app.MapGet("/api/sweeps", async (HttpContext context) =>
        {
            var error = ValidateSpan(
                Query(context, "hours") ?? Query(context, "hours_back"),
                Query(context, "as_of"),
                out var hours, out var endUtc);
            if (error is not null)
            {
                return SweepError(error, StatusCodes.Status400BadRequest);
            }

            var startUtc = endUtc.AddHours(-hours);
            var runs = await FleetSweepStore.GetSweepsBySpanAsync(
                postgres, startUtc, endUtc, logger, context.RequestAborted);

            return JsonResult(FleetSweepPresentation.BuildTimelineNode(runs, startUtc, endUtc));
        });

        /* The newest sweep in full — the page's landing document. 404 with the honest sentence when
           the store holds no sweep yet (a fresh install, or the engine disabled), which the page
           renders as its empty state rather than an error. */
        app.MapGet("/api/sweeps/latest", async (HttpContext context) =>
        {
            try
            {
                var run = await FleetSweepStore.GetLatestSweepAsync(postgres, context.RequestAborted);
                return run is null
                    ? SweepError("No sweep has been recorded yet.", StatusCodes.Status404NotFound)
                    : JsonResult(await BuildDetailAsync(logger, postgres, run, context));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The latest-sweep read throws on a store fault (the engine-seam posture); here the
                   loud shape is a 500 the page renders red — never a 404 that reads as "no sweeps". */
                return SweepError($"Error reading the latest sweep: {ex.Message}", StatusCodes.Status500InternalServerError);
            }
        });

        /* One sweep in full, by id — the timeline click-through. 404 means genuinely absent (pruned
           by retention, or never recorded); a store fault is the 500 arm, per the store read's doc. */
        app.MapGet("/api/sweeps/{id:long}", async (HttpContext context, long id) =>
        {
            try
            {
                var run = await FleetSweepStore.GetSweepAsync(postgres, id, context.RequestAborted);
                return run is null
                    ? SweepError("Sweep not found.", StatusCodes.Status404NotFound)
                    : JsonResult(await BuildDetailAsync(logger, postgres, run, context));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return SweepError($"Error reading sweep {id}: {ex.Message}", StatusCodes.Status500InternalServerError);
            }
        });

        /* The watch-item worklist. Default = open + carried in ONE store read; ?state= narrows to a
           named state (closed is how the history is asked for); an unknown state is refused. */
        app.MapGet("/api/sweeps/watch-items", async (HttpContext context) =>
        {
            var state = Query(context, "state");
            var error = ValidateWatchState(state);
            if (error is not null)
            {
                return SweepError(error, StatusCodes.Status400BadRequest);
            }

            var items = state is null
                ? await FleetSweepStore.GetOpenAndCarriedWatchItemsAsync(postgres, logger, context.RequestAborted)
                : await FleetSweepStore.GetWatchItemsByStateAsync(postgres, state, logger, context.RequestAborted);

            var names = await ReadWatchItemNamesAsync(postgres, items, logger, context.RequestAborted);
            return JsonResult(FleetSweepPresentation.BuildWatchItemsNode(items, names));
        });
    }

    /// <summary>One sweep's full document: the run plus its verdicts and — under master-off — the
    /// would-have-paged ledger, through the shared builders. The child reads are the store's
    /// presentation reads (log-and-degrade into the service log via <paramref name="logger"/> —
    /// the seat <see cref="Map"/>'s doc explains), so a child fault costs its section, not the page.</summary>
    private static async Task<JsonObject> BuildDetailAsync(
        ILogger logger, NpgsqlDataSource postgres, FleetSweepRun run, HttpContext context)
    {
        var verdicts = await FleetSweepStore.GetServerVerdictsAsync(
            postgres, run.SweepId, logger, context.RequestAborted);

        var ledger = run.AlertsEnabled
            ? new List<FleetSweepWouldHavePagedEntry>()
            : await FleetSweepStore.GetWouldHavePagedAsync(postgres, run.SweepId, logger, context.RequestAborted);

        return FleetSweepPresentation.BuildSweepDetailNode(run, verdicts, ledger);
    }

    /// <summary>The first non-empty value for a query key, or null — the read surface's binding rule.</summary>
    private static string? Query(HttpContext context, string key)
    {
        var value = context.Request.Query[key].ToString();
        return string.IsNullOrEmpty(value) ? null : value;
    }

    /// <summary>JSON written verbatim, bypassing any serializer naming policy — the surface's rule.</summary>
    private static IResult JsonResult(JsonNode node) =>
        Results.Text(node.ToJsonString(), "application/json");

    private static IResult SweepError(string message, int statusCode) =>
        Results.Text(new JsonObject { ["error"] = message }.ToJsonString(), "application/json", statusCode: statusCode);
}
