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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The fleet sweep MCP tool (#3466 lane 4) — <c>get_sweep_reports</c>, the read the spec's non-goals
/// section designed the whole feature around: the product publishes evidence at cadence and an
/// operator's agent layers judgment on top, and this tool is the interface between the two. It serves
/// the SAME <see cref="FleetSweepPresentation"/> builders the web feed's <c>/api/sweeps</c> routes
/// serve, over the SAME <see cref="FleetSweepStore"/> presentation reads, so an agent and a browser
/// cannot disagree about what a sweep looks like — the zero-drift rule <c>DarlingMcpFleetTools</c>
/// and <c>/api/fleet</c> established, applied to the sweep rows.
///
/// <para><b>The span discipline is the shared one.</b> The timeline half validates its window through
/// <see cref="McpHelpers.ValidateWindow"/> — the authority every windowed read on both SKUs applies —
/// and REFUSES an out-of-range span rather than clamping it. The watch-state filter goes through
/// <see cref="DarlingFleetSweepEndpoints.ValidateWatchState"/>, the web feed's own validator, so the
/// two surfaces refuse in one vocabulary.</para>
///
/// <para><b><c>sweep_id</c> is TEXT, deliberately (#2548's rule).</b> Sweep ids are tick-scale
/// integers, far past the 2^53 boundary where a JSON number silently loses low-order digits in any
/// double-based client — an id that made that round trip matches nothing, and the store would answer
/// the honest-looking "no such sweep". The tool takes the id as a string and rejects one it cannot
/// parse EXACTLY, rather than silently querying a rounded neighbor. Since #3487 the shared builders
/// EMIT every sweep id as a string too — the output side had been handing consumers a value already
/// through the double, which is how the web drill-down fetched sweeps that were "never recorded".</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFleetSweepTools
{
    [McpServerTool(Name = "get_sweep_reports"), Description(
        "Reads the scheduled Fleet Sweep Report: the timeline for the window, the newest sweep in full, and the " +
        "watch worklist (default: open+carried only; pass watch_state for pending/closed); pass sweep_id (a " +
        "STRING) for one sweep. MUTE SEMANTICS: alerts_enabled false means DELIVERY WAS OFF that window; such a " +
        "sweep carries would_have_paged (present-and-empty means nothing would have paged; ABSENT on an " +
        "alerts-on sweep). QUIET IS NOT CLEAN: instruments_alive false means the sweep could not prove its data " +
        "sources; read instrument_liveness before trusting a quiet window. Stored read. <<GUIDE>> " +
        "Reads the scheduled Fleet Sweep Reports - the stateful whole-fleet summaries the service persists at " +
        "the configured cadence (get_alert_settings' fleet_sweep group holds the knobs; default hourly). One " +
        "call returns the sweep timeline for the window (each sweep's full document embedded), the newest " +
        "sweep in full (per-server verdicts with band transitions pre-computed, plus its would-have-paged " +
        "ledger when one was taken), and the watch-item worklist with its hysteresis position; pass sweep_id " +
        "(as a STRING - the ids are too large for a JSON number to round-trip) to fetch one sweep in full " +
        "instead. Every sweep id the payload carries (sweep_id, previous_sweep_id, the watch items' " +
        "*_sweep_id anchors) is spelled as a JSON string for the same reason - pass one back verbatim. " +
        "Two honesty rules ride every payload, and both carry field-level meaning an agent must read. " +
        "MUTE SEMANTICS: alerts_enabled on a sweep states the alert master switch AS THE SWEEP RAN - false " +
        "means DELIVERY WAS OFF: nothing paged while that sweep's window elapsed, however bad the window was. " +
        "Such a sweep carries the would_have_paged ledger (what the sweep's own scoring banded Critical while " +
        "the fleet was muted, with evidence) - present-and-empty means 'muted, and nothing would have paged', " +
        "a statement, where the key is ABSENT on an alerts-on sweep because no such check was made. " +
        "QUIET IS NOT CLEAN: every sweep proves its instruments before reporting a quiet hour, and " +
        "instruments_alive: false means the sweep could NOT prove its own data sources - a dead reader, a " +
        "silent fleet outside the post-restart settle window, or a frozen alert-pass counter beside a " +
        "delivering path. Read the instrument_liveness block before believing any quiet card on that sweep: " +
        "an empty window there is unreadable, not healthy. Sweep content reaches the alert channels through " +
        "at most one daily rollup (INFO, master-gated); this tool and the web feed are the full-cadence record. " +
        "Watch items carry entry/exit hysteresis (the bars ride the payload beside the counters); the default " +
        "worklist view is open plus carried, because open lasts exactly one sweep by design - ask for a named " +
        "state (pending, open, carried, closed) with watch_state. A stored read over the monitoring store; no " +
        "monitored server is touched.")]
    public static async Task<string> GetSweepReports(
        NpgsqlDataSource postgres,
        /* DI-resolved like postgres (no [Description], so it never reaches the advertised schema): the
           MCP host registers ITS logger — the service host's, with real providers — so a child-read
           fault on this path leaves a trace instead of degrading silently, which was the one
           observation the #3473 review recorded against this tool. The /api/read mirror's shared
           handler delegate still has no logger seat; its dispatch BUILDER captures the web host's
           service logger for this entry (the same instance — the wiring is beside the entry in
           DarlingWebEndpoints.BuildReadDispatch), so both hosts' paths trace, and only a dispatch
           built without a host behind it still passes null here. */
        ILogger? logger,
        [Description("Hours of sweep timeline. Default 1 (the delivery ruling's default viewing span).")] int hours_back = 1,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("One sweep's id, AS A STRING (from the timeline's sweep_id). When set, returns that " +
            "sweep in full and nothing else; unparseable or unknown ids are refused/reported, never rounded.")] string? sweep_id = null,
        [Description("Watch-item state filter: pending, open, carried, or closed. Omit for the default view " +
            "(open + carried - what is standing right now).")] string? watch_state = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (sweep_id is not null)
            {
                /* #2548: exact parse or refusal — a rounded id would query a neighbor that does not exist
                   and the miss would read as retention. */
                if (!long.TryParse(sweep_id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id))
                {
                    return McpHelpers.Refusal("sweep_id", $"Invalid sweep_id value '{sweep_id}'. Expected a whole number as a string, " +
                        "exactly as the timeline's sweep_id field spells it.");
                }

                var run = await FleetSweepStore.GetSweepAsync(postgres, id, cancellationToken);
                return run is null
                    ? McpHelpers.Status(
                        "empty",
                        $"No sweep with id {id} exists - pruned by retention, or never recorded. The timeline " +
                        "(call this tool without sweep_id) shows what the store holds.")
                    : (await BuildDetailAsync(postgres, run, cancellationToken)).ToJsonString();
            }

            var windowError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (windowError != null) return windowError;

            var stateError = DarlingFleetSweepEndpoints.ValidateWatchState(watch_state, parameterName: "watch_state");
            if (stateError != null) return stateError;

            var windowStart = windowEnd.AddHours(-hours_back);
            var runs = await FleetSweepStore.GetSweepsBySpanAsync(
                postgres, windowStart, windowEnd, cancellationToken);

            /* The latest sweep is the landing document — read OUTSIDE the caller's window on purpose, the
               web feed's own landing shape: an agent asking about a quiet historical hour still learns what
               the newest sweep says now, and the timeline answers the window it asked about. */
            var latest = await FleetSweepStore.GetLatestSweepAsync(postgres, cancellationToken);
            if (latest is null)
            {
                return McpHelpers.Status(
                    "empty",
                    "No sweep has been recorded yet. The fleet sweep writes one per cadence tick once " +
                    "fleet_sweep.enabled is on - get_alert_settings shows the knobs, and a fresh install's " +
                    "first sweep lands within one interval of startup.");
            }

            var watchItems = watch_state is null
                ? await FleetSweepStore.GetOpenAndCarriedWatchItemsAsync(postgres, cancellationToken)
                : await FleetSweepStore.GetWatchItemsByStateAsync(postgres, watch_state, cancellationToken);

            /* The worklist's names, through the web feed's own gate-and-read (#3482) — the
               ValidateWatchState sharing pattern, so the two surfaces cannot drift on when names are
               joined, and a failed read costs the names, never the worklist. */
            var watchItemNames = await DarlingFleetSweepEndpoints.ReadWatchItemNamesAsync(
                postgres, watchItems, logger, cancellationToken);

            var result = FleetSweepPresentation.BuildTimelineNode(runs, windowStart, windowEnd);
            result["latest"] = await BuildDetailAsync(postgres, latest, cancellationToken);
            result["watch_state"] = watch_state ?? "open + carried (default)";
            result["watch_items"] = FleetSweepPresentation.BuildWatchItemsNode(watchItems, watchItemNames);
            return result.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #4315: the child presentation reads above now throw instead of log-and-degrade, so this
               is the ONE place a sweep-report fault is traced for the MCP path — the #3473 review's
               reason this tool carries its own logger seat unlike get_fleet_overview/get_ag_health.
               McpHelpers.FormatError still answers the tool's usual error envelope; this only adds the
               service-log line the removed internal catches used to write.

               Reached through /api/read/get_sweep_reports, a fault logs TWICE, on purpose: this line
               carries the exception and its stack, and DarlingWebEndpoints' ToHttpResult ->
               ServerErrorResult line (the same one every other /api/read/* tool's fault writes) carries
               the route and the elapsed time. Two lines, two different pieces of the same fault — not a
               duplicate to collapse. */
            logger?.LogError(ex, "get_sweep_reports failed: {Message}", ex.Message);
            return McpHelpers.FormatError("get_sweep_reports", ex);
        }
    }

    /// <summary>One sweep's full document — the web feed's <c>BuildDetailAsync</c> twin over the same
    /// builders and the same presentation reads. THROWS on a store fault (#4315): a verdicts or
    /// would-have-paged fault now fails the whole answer rather than costing only its section, caught
    /// by <see cref="GetSweepReports"/>'s outer try, which logs and answers the tool's usual error
    /// envelope. The ledger is read only for a master-off sweep, because the engine writes none
    /// otherwise and the builder OMITS the key on an alerts-on sweep by contract.</summary>
    private static async Task<JsonObject> BuildDetailAsync(NpgsqlDataSource postgres, FleetSweepRun run, CancellationToken cancellationToken = default)
    {
        var verdicts = await FleetSweepStore.GetServerVerdictsAsync(
            postgres, run.SweepId, cancellationToken);

        var ledger = run.AlertsEnabled
            ? new List<FleetSweepWouldHavePagedEntry>()
            : await FleetSweepStore.GetWouldHavePagedAsync(postgres, run.SweepId, cancellationToken);

        return FleetSweepPresentation.BuildSweepDetailNode(run, verdicts, ledger);
    }
}
