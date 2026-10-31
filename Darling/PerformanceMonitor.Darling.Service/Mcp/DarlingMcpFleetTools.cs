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
using System.Linq;
using System.Text.Json;
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
/// The fleet-overview MCP tool (#1562) — the signature cross-server roll-up Darling's single central store
/// enables and neither the Dashboard nor Lite can do (each monitors one server per database). It reads through
/// the SHARED <see cref="DarlingFleetReader"/> — the SAME reader (and the SAME <c>ServerHealthClassifier</c>
/// banding) that powers the web dashboard's <c>/api/fleet</c> and the WPF viewer's Overview — so an MCP client,
/// a browser, and the desktop app all see one consistently-banded fleet. A STORED read (no live
/// monitored-server hit).
///
/// <para>This is ADDITIVE alongside get_server_summary (the Lite-parity one-shot per-server check, unchanged):
/// get_fleet_overview returns EVERY enabled server's pre-banded card plus the fleet rollup (band counts, the
/// cross-server blocking / deadlock totals, and the worst-first "needs attention" ranking) in one call.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFleetTools
{
    [McpServerTool(Name = "get_fleet_overview"), Description(
        "One pre-banded card per server (CPU, memory, blocking, deadlocks, threads, collector health) plus a " +
        "fleet rollup (band counts, cross-server totals, worst-first ranking). Blocking/deadlock counts window " +
        "hours_back (default 1) ending now; collector-health fields use a separate scan, not hours_back. " +
        "cpu_source explains a missing total_cpu_percent: NotCollected is a real gap (get_collection_health); " +
        "NoSourceForEngine (non-Aurora PostgreSQL) is structural, no grant or upgrade fixes it. PostgreSQL " +
        "memory_mb, buffer_pool_mb and threads are always null (no DMV equivalent), not a gap. <<GUIDE>> " +
        "Gets the whole fleet's health at a glance: one pre-banded card per monitored server (CPU, memory, " +
        "blocking, deadlocks, worker threads, and collector health, each with a Healthy/Warning/Critical band), " +
        "plus a rollup — counts by band, cross-server blocking and deadlock totals, and a worst-first 'needs " +
        "attention' ranking. Use this first to decide which server to drill into, then call the per-server tools. " +
        "This cross-server view is unique to the central store. " +
        "cpu_source names which collector produced each card's total_cpu_percent, so a missing number is never " +
        "ambiguous: RingBuffer (SQL Server, and the only arm that also fills cpu_percent / " +
        "other_process_cpu_percent with the per-process split), PerformanceInsights (an Aurora PostgreSQL " +
        "target's instance CPU from the AWS API, total only — no per-process breakdown exists), NotCollected " +
        "(a source that applies here produced no current reading — check get_collection_health), or " +
        "NoSourceForEngine (a PostgreSQL target not known to be Aurora, INCLUDING plain RDS for PostgreSQL: " +
        "PostgreSQL exposes no instance-CPU counter and the Performance Insights ingest is gated to Aurora, " +
        "so that null is structural and no grant or upgrade changes it). A PostgreSQL target's memory_mb, " +
        "buffer_pool_mb and the whole threads block are null for " +
        "the same structural reason and no band is claimed for them — those metrics are SQL Server DMV " +
        "readings with no PostgreSQL equivalent collected; get_pg_buffer_usage, get_pg_kernel_stats and " +
        "get_pg_session_states are the reads that answer the nearest PostgreSQL questions. A PostgreSQL " +
        "target's deadlock_count IS measured: it is the server's own pg_stat_database.deadlocks counter, " +
        "differenced per database over the window (a statistics reset clamps to zero, never subtracts) and " +
        "summed, banded through the same deadlock_warn_per_hour / deadlock_critical_per_hour tiers as SQL " +
        "Server's graph count; deadlock_source reads PostgresTarget for it, which means COUNTED " +
        "from that counter (deadlock_coverage.postgres_servers is a sub-count of servers_read, not a gap), " +
        "and get_pg_deadlocks has the parsed deadlock reports themselves. Its blocking_severity stays " +
        "Unknown on purpose: PostgreSQL blocking is a once-a-minute SAMPLE of pg_stat_activity, and the " +
        "blocking band's count tiers were measured in engine-recorded reports per hour, so a sighting count " +
        "through them would band on a denominator they were never measured against — the PostgreSQL " +
        "Blocking alert speaks for that condition until a sampled-shape band is measured. " +
        "collection_health_age_seconds says how old the COLLECTOR-HEALTH half of the payload is: the per-card " +
        "healthy/failed/collector_count, collector_severity, deadlock_collector_band and deadlock_source, and the " +
        "fleet's servers_with_collection_failures and deadlock_coverage, come from one 7-day scan of the collection " +
        "log that does not depend on hours_back and is shared by every overview call of the same minute on this " +
        "host — 0 means this call ran it, anything up to 59 means a scan that many seconds before generated_at " +
        "did. Everything else on the payload was read for this call. Racing several calls with different " +
        "hours_back values buys nothing on that half and costs the window-bound reads N times over: call once " +
        "with the widest window and derive, or sequence the calls. " +
        "detail defaults to \"summary\" (#4198): the rollup, band counts and worst_servers, WITHOUT the " +
        "per-server cards array — #4198 measured that array at 67-81 KB on a fleet this size, large enough " +
        "that Claude Code refused it inline outright. Pass detail=\"cards\" for the full per-server detail " +
        "this tool returned before #4198, once you already know which server needs it; cards_included says " +
        "which shape you got, and a summary response's cards_note repeats the lever. worst_only and band " +
        "narrow a detail=\"cards\" call back toward a manageable size instead of shipping every card: " +
        "worst_only keeps only the cards already named in worst_servers, and band keeps only cards at one " +
        "FleetHealthBand (healthy, warning, critical, offline). Both are ignored under detail=\"summary\", " +
        "which never includes cards to filter.")]
    public static async Task<string> GetFleetOverview(
        NpgsqlDataSource postgres,
        [Description("Hours of blocking/deadlock history the per-server cards and fleet totals window over. Default 1.")] int hours_back = 1,
        [Description("\"summary\" (default): rollup, band counts, worst_servers, no cards array. \"cards\": adds every server's full card, this tool's shape before #4198. See tool guide.")] string detail = "summary",
        [Description("Cards-only: keep only cards already in worst_servers (the needs-attention list). No effect under detail=\"summary\". Default false.")] bool worst_only = false,
        [Description("Cards-only filter: keep only cards at this FleetHealthBand — \"healthy\", \"warning\", \"critical\", or \"offline\" (case-insensitive). Ignored under detail=\"summary\". Omit for every band.")] string? band = null,
        MonitoredServerRegistryState? registryState = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var validation = DarlingReadWindow.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        var wantsCards = string.Equals(detail, "cards", StringComparison.OrdinalIgnoreCase);
        if (!wantsCards && !string.Equals(detail, "summary", StringComparison.OrdinalIgnoreCase))
        {
            return McpHelpers.Refusal("detail", $"detail must be \"summary\" or \"cards\" (got \"{detail}\").");
        }

        FleetHealthBand? bandFilter = null;
        if (!string.IsNullOrWhiteSpace(band))
        {
            if (!Enum.TryParse<FleetHealthBand>(band, ignoreCase: true, out var parsed))
            {
                return McpHelpers.Refusal(
                    "band", $"band must be one of healthy, warning, critical, offline (got \"{band}\").");
            }
            bandFilter = parsed;
        }

        try
        {
            var now = DateTime.UtcNow;
            var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-hours_back), now, now,
                /* The live registry, injected like the analysis service's: an Azure master's card skips the events of
                   databases monitored as their own targets. Null (the web mirror, tests) keeps the unscoped counts. */
                separatelyMonitored: registryState is null
                    ? null
                    : (serverId, ct) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(serverId, registryState.Read(), postgres, ct),
                cancellationToken: cancellationToken,
                logger: logger);

            if (result.TotalServers == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No servers are registered yet. The service registers each monitored server on its first successful connection.");
            }

            /* Both branches serialize the SAME result (so a rollup field added later reaches both shapes
               with no second edit site) and then add cards_included explicitly — true/false, never left
               for a client to infer from whether "cards" is present or empty, which is indistinguishable
               from "no servers have cards". */
            var node = JsonSerializer.SerializeToNode(result, DarlingFleetReader.JsonOptions)!.AsObject();

            /* #7010: the window notice, one fleet-wide and one per truncated card (summary mode strips the cards, and only the top level stays). */
            var windowNotice = await ReadWindowNoticeAsync(
                postgres, result.Cards.Select(c => c.ServerName).ToList(), now.AddHours(-hours_back), now, logger, cancellationToken);
            windowNotice?.AttachTop(node);

            if (wantsCards)
            {
                if (worst_only || bandFilter != null)
                {
                    /* Filtered at the TYPED level (result.Cards / result.WorstServers), not by inspecting
                       the JsonObject's own "band"/"server_id" string keys — the typed properties cannot
                       drift from what was just serialized, and a card's Band is already the same enum
                       bandFilter was parsed into, so the comparison needs no second string parse. */
                    var worstIds = worst_only ? new HashSet<int>() : null;
                    if (worstIds != null)
                    {
                        foreach (var w in result.WorstServers) worstIds.Add(w.ServerId);
                    }

                    var filteredCards = new JsonArray();
                    foreach (var card in result.Cards)
                    {
                        if (worstIds != null && !worstIds.Contains(card.ServerId)) continue;
                        if (bandFilter != null && card.Band != bandFilter.Value) continue;
                        filteredCards.Add(JsonSerializer.SerializeToNode(card, DarlingFleetReader.JsonOptions));
                    }
                    node["cards"] = filteredCards;
                }

                windowNotice?.AttachCards(node["cards"] as JsonArray);
                node["cards_included"] = true;
                return node.ToJsonString(DarlingFleetReader.JsonOptions);
            }

            /* #4198 summary mode: strip the heavy cards array — measured at 67-81 KB on a fleet this size,
               large enough that Claude Code refused it inline outright. worst_only/band are cards-only
               filters (see the Description) so they are silently no-ops here rather than refused: a caller
               that always passes band="critical" alongside its own detail choice should not have to drop
               it again to get the cheap summary shape. */
            node.Remove("cards");
            node["cards_included"] = false;
            node["cards_note"] =
                "Per-server cards were left out of this summary to keep the response small. Call again with "
                + "detail=\"cards\" for the full per-server detail.";
            return node.ToJsonString(DarlingFleetReader.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_fleet_overview", ex);
        }
    }

    /// <summary>The relations the cards' blocking and deadlock counts read (#7010): the XE blocked process reports, the DMV blocking snapshots
    /// that stand in for them, the deadlock graphs, and a PostgreSQL target's own deadlock counter table. One per-server probe each.</summary>
    private static readonly string[] s_cardCountRelations = ["blocked_process_reports", "dmv_blocking_snapshots", "deadlocks", "pg_database_stats"];

    private const int MaxTruncatedServersNamed = 20;

    /// <summary>
    /// The window notice of the fleet overview (#7010): the cards' blocking and deadlock counts are the windowed part, so each card's floor is
    /// the LATEST of its server's floors over <see cref="s_cardCountRelations"/> (a card is only as complete as its least-covered count), and the
    /// top level carries the latest of the cards' floors, so the fleet reads truncated when any card does. Each relation is read ONCE for the
    /// whole fleet (<see cref="DataWindowFloor.GetPerServerAsync"/>), never once per server. A server that has no floor (it never collected a
    /// relation in the window) adds nothing, so a never-collected server's card stays bare. A window no longer than the truncation slack is
    /// not probed (covered whatever the probe would read), and a probe that fails costs the notice, never the answer. Null: no notice at all.
    /// </summary>
    private static async Task<FleetWindowNotice?> ReadWindowNoticeAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string> serverNames, DateTime windowStart, DateTime windowEnd, ILogger? logger,
        CancellationToken cancellationToken)
    {
        var floors = new Dictionary<string, DateTime?>(StringComparer.Ordinal);
        if (windowEnd - windowStart > DurationTrendRouting.TruncationSlack)
        {
            try
            {
                foreach (var relation in s_cardCountRelations)
                {
                    var perServer = await DataWindowFloor.GetPerServerAsync(
                        postgres, [DataWindowFloor.Source.ForCollectorTable(relation)], windowStart, windowEnd,
                        StorageCommandDeadlines.McpReadSeconds, cancellationToken);
                    foreach (var (name, floor) in perServer)
                    {
                        floors[name] = floors.TryGetValue(name, out var seen) ? DarlingMcpWindowNotice.Later(seen, floor) : floor;
                    }
                }
            }
            catch (Exception ex) when (!(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                logger?.LogWarning(ex, "The coverage probe of the fleet overview's blocking and deadlock tables failed; the answer goes without its window-floor notice.");
                return null;
            }
        }

        DateTime? latest = null;
        var cards = new Dictionary<string, McpWindowNotice>(StringComparer.Ordinal);
        var truncated = new List<string>();
        foreach (var name in serverNames.Distinct(StringComparer.Ordinal))
        {
            if (!floors.TryGetValue(name, out var floor) || floor is null)
            {
                continue;
            }

            latest = DarlingMcpWindowNotice.Later(latest, floor);
            if (RawWindowFloor.IsTruncated(floor, windowStart))
            {
                truncated.Add(name);
                cards[name] = DarlingMcpWindowNotice.Build(
                    floor, windowStart, "blocking and deadlock data",
                    "The blocking and deadlock counts on this card cover the window only from effective_start, so they under-count the window that was asked for.");
            }
        }

        truncated.Sort(StringComparer.Ordinal);
        var top = DarlingMcpWindowNotice.Build(
            latest, windowStart, "blocking and deadlock data",
            "The fleet blocking and deadlock totals sum the cards' counts, so they under-count the window that was asked for; window_truncated_servers names the servers whose cards are affected.");
        return new FleetWindowNotice(top, cards, truncated);
    }

    /// <summary>The notices <see cref="ReadWindowNoticeAsync"/> built: the top-level one, each truncated card's, and the truncated servers' names.</summary>
    private sealed record FleetWindowNotice(McpWindowNotice Top, Dictionary<string, McpWindowNotice> Cards, List<string> TruncatedServers)
    {
        /// <summary>The three window-floor keys, and <c>window_truncated_servers</c> (the first 20 names, sorted) with <c>window_truncated_servers_more</c>
        /// (the count of the rest) only when there are more than 20.</summary>
        public void AttachTop(JsonObject node)
        {
            node["effective_start"] = Top.EffectiveStart;
            node["window_truncated"] = Top.WindowTruncated;
            node["truncation_note"] = Top.TruncationNote;
            var named = new JsonArray();
            foreach (var name in TruncatedServers.Take(MaxTruncatedServersNamed))
            {
                named.Add(name);
            }

            node["window_truncated_servers"] = named;
            if (TruncatedServers.Count > MaxTruncatedServersNamed)
            {
                node["window_truncated_servers_more"] = TruncatedServers.Count - MaxTruncatedServersNamed;
            }
        }

        /// <summary>The three keys on each card whose server is truncated; every other card stays as it was.</summary>
        public void AttachCards(JsonArray? cards)
        {
            if (cards is null)
            {
                return;
            }

            foreach (var card in cards.OfType<JsonObject>())
            {
                if (card["server_name"]?.GetValue<string>() is { } name && Cards.TryGetValue(name, out var notice))
                {
                    card["effective_start"] = notice.EffectiveStart;
                    card["window_truncated"] = notice.WindowTruncated;
                    card["truncation_note"] = notice.TruncationNote;
                }
            }
        }
    }
}
