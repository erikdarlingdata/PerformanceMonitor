/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service-side routing of <c>get_resource_use_by_tag</c>'s plan-cache and procedures sections (#5983): which tier answers, and how far it reached.
/// It stays in the service because it rides the service's own rollup availability cache and top-ranking route table; the reader that runs the
/// statements moved to <c>PerformanceMonitor.Darling.Storage</c> so the viewer shares it (#5988).
/// </summary>
internal static class DarlingQueryTagRollupRouting
{
    /// <summary>
    /// How one stats section (plan cache or procedures) is served and what it says about how far it reached. <paramref name="Hourly"/>: the
    /// io rollup serves it, cut at <paramref name="Ceiling"/>. <paramref name="Disclosure"/> is merged into the section: on the hourly
    /// branch <c>window_end</c> (the served end), <c>window_edges_note</c> and the four edge fields (<c>served_start</c>,
    /// <c>start_dropped_from</c>, <c>materialized_to</c>, <c>end_hour_counted_whole</c>, #6831). The Query Store section carries only
    /// <c>served_start</c> and <c>start_dropped_from</c>: its read ends exactly at the window's end, so it has no ceiling or whole last hour to
    /// report. On the raw branch <c>effective_start</c>, <c>window_truncated</c>
    /// and <c>truncation_note</c>, the notice <c>get_top_queries_by_cpu</c> carries.
    /// </summary>
    internal sealed record StatsRoute(bool Hourly, DateTime? Ceiling, Dictionary<string, object?> Disclosure);

    /// <summary>
    /// Routes the plan-cache and procedures sections independently (their rollups, floors and tiers are independent) and builds each
    /// one's reach disclosure (#5983). The hourly rollup serves a section only when the retention tier left raw and the io route reaches
    /// the window's start; <c>tier Hourly</c> with <c>route Raw</c> falls to raw over a window raw may not hold, and says so. The tier is
    /// judged against the wall clock (retention is wall-clock age), the window's own end is the anchor.
    /// <para>#6794: a read for more than one server also takes the io rollup inside raw retention when the route reaches the window and
    /// the rollup serves most of it (<see cref="QueryTagFleetRoute.ReadsHourlyInsideRaw"/>, the rule the Viewer calls too). Raw there means
    /// every server's raw rows for the whole window (about 12 million plan-cache rows for 24 hours on a 42-server store), while the rollup
    /// holds one row per key per hour. The section says so as every hourly read does (<c>tier</c>, <c>window_end</c>, <c>window_edges_note</c>).
    /// A short window (1 or 2 hours, where the rollup's ceiling sits at or below the window start) and a one-server read keep raw.</para>
    /// </summary>
    internal static async Task<(StatsRoute Plan, StatsRoute Procedures)> ChooseStatsRoutesAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime windowStart, DateTime windowEnd, CancellationToken cancellationToken)
    {
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
        var now = DateTime.UtcNow;

        async Task<StatsRoute> RouteAsync(
            HourlyRoute route, RetentionTier tier, string ioView, string rawSource, RawWindowFloor.Table floorTable)
        {
            var ceiling = coverage.HourlyEndCeiling(ioView, windowStart);
            var hourly = route == HourlyRoute.Io
                && (tier != RetentionTier.Raw || QueryTagFleetRoute.ReadsHourlyInsideRaw(
                    serverIds.Length, windowStart, windowEnd, ceiling, coverage.FloorOf(ioView)));
            if (hourly)
            {
                var (_, servedEnd) = HourlyWindowEdges.ServedSpan(windowStart, null, windowEnd, ceiling);
                var edges = HourlyWindowEdges.Note(windowStart, null, windowEnd, ceiling);
                var fields = HourlyWindowEdges.Describe(windowStart, null, windowEnd, ceiling);
                return new StatsRoute(true, ceiling, new Dictionary<string, object?>
                {
                    ["tier"] = "hourly",
                    ["window_end"] = servedEnd is DateTime end ? McpHelpers.FormatEffectiveStart(end) : null,
                    ["window_edges_note"] = string.IsNullOrEmpty(edges) ? null : edges,
                    /* #6831: the same edges as fields, so the page says them in its own clock and words instead of reading the note. */
                    ["served_start"] = McpHelpers.FormatEffectiveStart(fields.ServedStart),
                    ["start_dropped_from"] = fields.StartDroppedFrom is DateTime dropped ? McpHelpers.FormatEffectiveStart(dropped) : null,
                    ["materialized_to"] = fields.MaterializedTo is DateTime built ? McpHelpers.FormatEffectiveStart(built) : null,
                    ["end_hour_counted_whole"] = fields.EndCountedWhole,
                });
            }

            /* The raw tier: the reach is what raw holds per server (the probe get_top_queries_by_cpu runs), and the section carries the
               latest floor among the servers in scope, because from there every server has coverage. */
            DateTime? latest = null;
            var truncated = false;
            foreach (var serverId in serverIds)
            {
                DateTime? floor;
                try
                {
                    floor = await RawWindowFloor.GetAsync(
                        postgres, floorTable, serverId, windowStart, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    /* The raw table is not in this store yet: no floor to report, and the section read says "unavailable" (#5983). */
                    floor = null;
                }

                var effective = RawWindowFloor.EffectiveStart(floor, windowStart);
                if (latest is null || effective > latest)
                {
                    latest = effective;
                }

                truncated |= RawWindowFloor.IsTruncated(floor, windowStart);
            }

            var notice = ComposeStoreAvailability.BuildRetentionNotice(rawSource, ComposeRoute.Raw, windowStart, now, rollups, coverage);
            return new StatsRoute(false, null, new Dictionary<string, object?>
            {
                ["tier"] = "raw",
                ["effective_start"] = McpHelpers.FormatEffectiveStart(latest ?? windowStart),
                ["window_truncated"] = truncated,
                ["truncation_note"] = truncated ? notice : null,
            });
        }

        var plan = await RouteAsync(
            DarlingDataReader.ChooseQueriesHourlyRoute(rollups, coverage, TopRanking.Reads, windowStart),
            RetentionTierRouter.Resolve(now, windowStart, rollups.QueryGrainHourly, dailyAvailable: false,
                coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView)),
            TimescaleSupport.QueryStatsIoHourlyView, "query_stats", RawWindowFloor.Table.QueryStats);
        var procedures = await RouteAsync(
            DarlingDataReader.ChooseProceduresHourlyRoute(rollups, coverage, TopRanking.Reads, windowStart),
            RetentionTierRouter.Resolve(now, windowStart, rollups.ProcedureGrainHourly, dailyAvailable: false,
                coverage.For(TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView)),
            TimescaleSupport.ProcedureStatsIoHourlyView, "procedure_stats", RawWindowFloor.Table.ProcedureStats);
        return (plan, procedures);
    }
}
