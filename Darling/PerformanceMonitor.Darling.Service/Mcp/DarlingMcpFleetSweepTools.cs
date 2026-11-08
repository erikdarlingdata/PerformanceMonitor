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
        "call returns the sweep timeline for the window (each sweep's full document embedded - the same entry the " +
        "web feed and the Viewer show), the newest sweep in full (per-server verdicts with band transitions " +
        "pre-computed, plus its would-have-paged ledger when one was taken), and the watch-item worklist with " +
        "its hysteresis position; pass sweep_id (as a STRING - the ids are too large for a JSON number to " +
        "round-trip) to fetch one sweep in full instead. SIZE: the newest sweep's document ships once, under latest; " +
        "its timeline entry keeps the row counts (report.fleet.bands, report.watch, report.would_have_paged) and says " +
        "report_under: latest. The reply is cut to the shared 32 KB response budget in two steps - the timeline loses " +
        "its oldest sweeps first (the newest entry always rides), then the worklist loses items by standing (carried " +
        "before open, oldest first, so the open items and the newest survive; the items kept ride in the worklist's " +
        "usual order). truncated is true only when something was cut; sweeps_returned and watch_items_returned say how " +
        "many of each count ride, and budget_note names what was cut. latest and the sweep_id read are never cut, so a " +
        "reply can still be over the budget at its floor: then over_budget is true and budget_note says OVER BUDGET " +
        "with the never-cut pieces' sizes, rather than claiming a fit. Narrow hours_back or move as_of to see the " +
        "older sweeps; pass watch_state for one state's worklist. Every sweep id the payload carries (sweep_id, previous_sweep_id, the watch items' " +
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

            var windowError = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
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

            /* #8081: the timeline entries are the web feed's and the Viewer's own (BuildRunNode, compared field
               for field by ViewerFleetSweepsReadTests), each older entry with its document embedded; the newest
               entry's document rides under latest (#8318, below). The cut fields are seated here, beside sweeps,
               and written by the fit. */
            var result = FleetSweepPresentation.BuildTimelineNode(runs, windowStart, windowEnd);
            var entries = ((JsonArray)result["sweeps"]!).ToList();
            var latestNode = await BuildDetailAsync(postgres, latest, cancellationToken);
            var latestBytes = Encoding.UTF8.GetByteCount(latestNode.ToJsonString());

            /* #8318: the newest sweep's document ships once. latest carries it in full, with its verdicts, so the
               timeline entry for the same sweep keeps its header facts and a slim report holding exactly the fields
               the sweep rows draw from (fleet.bands, watch, and would_have_paged when the sweep ran muted), equal to
               latest's, and names latest with report_under. Older entries keep their documents. The Viewer's and the
               web feed's entry shape (BuildRunNode) is unchanged; ViewerFleetSweepsReadTests compares this reply's
               newest entry with its document taken from latest, field for field. */
            var latestId = latestNode["sweep_id"]?.GetValue<string>();
            var newestEntryBytes = 0;
            foreach (var entry in entries.OfType<JsonObject>())
            {
                if (latestId is null || entry["sweep_id"]?.GetValue<string>() != latestId)
                {
                    continue;
                }

                if (entry["report"] is JsonObject full)
                {
                    var slim = new JsonObject();
                    if (full["fleet"] is JsonObject fleet && fleet["bands"] is JsonNode bands)
                    {
                        slim["fleet"] = new JsonObject { ["bands"] = bands.DeepClone() };
                    }

                    if (full["watch"] is JsonNode watch)
                    {
                        slim["watch"] = watch.DeepClone();
                    }

                    if (full["would_have_paged"] is JsonNode wouldHavePaged)
                    {
                        slim["would_have_paged"] = wouldHavePaged.DeepClone();
                    }

                    entry["report"] = slim;
                }

                entry["report_under"] = "latest";
                newestEntryBytes = Encoding.UTF8.GetByteCount(entry.ToJsonString());
            }

            /* The worklist node is the Viewer's and the web feed's own (BuildWatchItemsNode; the zero-drift rule
               ViewerFleetSweepsReadTests pins covers this reply's watch_items too), so its items keep the
               store's order. #8318: a budget cut keeps items by STANDING rather than by position - open before
               carried, newest first within a state, the store's order as the tie - so the items that survive
               are the ones standing now and the newest of them, and they still ride in the shared order. A
               watch_state read holds one state, so there the cut keeps the newest. itemNodes and watchItems
               are index-aligned: the builder writes one node per item in order. */
            var worklist = FleetSweepPresentation.BuildWatchItemsNode(watchItems, watchItemNames);
            var itemNodes = ((JsonArray)worklist["items"]!).ToList();
            var itemKeepOrder = Enumerable.Range(0, itemNodes.Count)
                .OrderBy(i => WatchStateCutRank(watchItems[i].State))
                .ThenByDescending(i => watchItems[i].LastSeenAtUtc)
                .ThenBy(i => i)
                .ToList();

            result["sweeps_returned"] = null;
            result["watch_items_returned"] = null;
            result["truncated"] = null;
            result["over_budget"] = null;
            result["budget_note"] = null;
            result["latest"] = latestNode;
            result["watch_state"] = watch_state ?? "open + carried (default)";
            result["watch_items"] = worklist;

            /* The whole reply for the first sweepsKept entries and the first itemsKept worklist items, flags
               and note included, so the fit measures the bytes the caller gets. The nodes are cloned per fit
               because a JsonNode has one parent. */
            string Serialize(int sweepsKept, int itemsKept, bool overAtFloor)
            {
                var sweepsCut = sweepsKept < entries.Count;
                var itemsCut = itemsKept < itemNodes.Count;
                result["sweeps"] = new JsonArray(entries.Take(sweepsKept).Select(e => e?.DeepClone()).ToArray());
                worklist["items"] = new JsonArray(itemKeepOrder.Take(itemsKept).OrderBy(i => i).Select(i => itemNodes[i]?.DeepClone()).ToArray());
                result["sweeps_returned"] = sweepsKept;
                result["watch_items_returned"] = itemsKept;
                result["truncated"] = sweepsCut || itemsCut;
                result["over_budget"] = overAtFloor;
                result["budget_note"] = BudgetNote(entries.Count, sweepsKept, itemNodes.Count, itemsKept, overAtFloor, latestBytes, newestEntryBytes);
                return result.ToJsonString();
            }

            /* #8318: two cuts to the shared response budget, in order. The timeline loses its OLDEST sweeps
               first (the store's order is newest first), and the newest entry always rides (small, since its
               document is under latest, which is answered whole - the single-item rule every budget read keeps). When the
               timeline at its floor still leaves the reply over the budget, the worklist loses items by
               standing (itemKeepOrder). count and watch_items.count still count the whole window and the whole
               worklist. latest is never cut, so a reply can be over the budget at the floor, and then the note
               says so rather than claiming a fit. */
            var sweepsFit = McpResponseBudget.FitPrefix(entries.Count, kept => Serialize(kept, itemNodes.Count, overAtFloor: false));
            if (FitsBudget(sweepsFit.Json))
            {
                return sweepsFit.Json;
            }

            var itemsFit = McpResponseBudget.FitPrefix(itemNodes.Count, kept => Serialize(sweepsFit.Count, kept, overAtFloor: false));
            if (FitsBudget(itemsFit.Json))
            {
                return itemsFit.Json;
            }

            return Serialize(sweepsFit.Count, itemsFit.Count, overAtFloor: true);
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

    private static bool FitsBudget(string json) =>
        Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes;

    /// <summary>#8318: the worklist's cut order by state. Open items are what is standing right now and go
    /// first, carried next; pending and closed only arrive through a <c>watch_state</c> read, where the list
    /// holds one state and this rank is a tie.</summary>
    private static int WatchStateCutRank(string state) => state switch
    {
        FleetSweepWatchStateMachine.Open => 0,
        FleetSweepWatchStateMachine.Carried => 1,
        FleetSweepWatchStateMachine.Pending => 2,
        FleetSweepWatchStateMachine.Closed => 3,
        _ => 4,
    };

    /// <summary>#8318: the <c>budget_note</c> for a reply, null when nothing was cut and the reply is under the
    /// budget. It names what was cut (sweeps, watch items, or both) and the reads that reach the rest, and when
    /// the reply at its floor is still over the budget it says OVER BUDGET and names the pieces that are never
    /// cut with their sizes (latest, and the newest sweep's timeline entry when the window holds it), never that
    /// the reply fit.</summary>
    private static string? BudgetNote(int sweepsInWindow, int sweepsKept, int itemsStanding, int itemsKept, bool overAtFloor, int latestBytes, int newestEntryBytes)
    {
        var sweepsCut = sweepsKept < sweepsInWindow;
        var itemsCut = itemsKept < itemsStanding;
        if (overAtFloor)
        {
            var newestEntry = newestEntryBytes > 0
                ? string.Create(CultureInfo.InvariantCulture, $", and the newest sweep's timeline entry is {newestEntryBytes:#,0} bytes")
                : string.Empty;
            return string.Create(CultureInfo.InvariantCulture,
                $"OVER BUDGET: the reply is over the {McpResponseBudget.DefaultBytes:#,0}-byte response budget at its floor ({sweepsKept:#,0} of {sweepsInWindow:#,0} sweeps in the window and {itemsKept:#,0} of {itemsStanding:#,0} watch items). The pieces that are never cut leave no room under it: latest, the newest sweep's full document with its verdicts, is {latestBytes:#,0} bytes{newestEntry}. Pass sweep_id for one sweep, or watch_state for one state's worklist.");
        }

        if (sweepsCut && itemsCut)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"TRUNCATED: {sweepsInWindow:#,0} sweeps fell in the window and {itemsStanding:#,0} watch items stand; only the newest {sweepsKept:#,0} sweeps and the {itemsKept:#,0} watch items standing first (open before carried, newest first) fit the {McpResponseBudget.DefaultBytes:#,0}-byte response budget beside latest. Narrow hours_back, or move as_of, to see the older sweeps; pass watch_state for one state's worklist; latest and the sweep_id read carry a sweep's full document.");
        }

        if (sweepsCut)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"TRUNCATED: {sweepsInWindow:#,0} sweeps fell in the window; only the newest {sweepsKept:#,0} fit the {McpResponseBudget.DefaultBytes:#,0}-byte response budget beside latest and the {itemsStanding:#,0}-item worklist. Narrow hours_back, or move as_of, to see the older sweeps; latest and the sweep_id read carry a sweep's full document.");
        }

        if (itemsCut)
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"TRUNCATED: {itemsStanding:#,0} watch items stand; only the {itemsKept:#,0} standing first (open before carried, newest first) fit the {McpResponseBudget.DefaultBytes:#,0}-byte response budget beside latest and the timeline. Pass watch_state for one state's worklist to see the rest.");
        }

        return null;
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
