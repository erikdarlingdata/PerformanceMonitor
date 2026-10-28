/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #5988 part B: what the "Resource use by tag" view reads. The grouped facts per section come from the shared
/// <see cref="DarlingQueryTagRollupReader"/> (the code <c>get_resource_use_by_tag</c> runs), so the viewer writes no roll-up SQL of its own.
/// <see cref="Unavailable"/> names each section that could not be served and why; <see cref="Refused"/> names the servers the Query Store gate refused.
/// <see cref="Notes"/> carries a section's reach disclosure (a window the raw tier no longer holds in full).
/// </summary>
public sealed record QueryTagRollupData(
    IReadOnlyList<QueryTagNode> Tags,
    IReadOnlyDictionary<string, List<QueryTagRollupFact>> Sections,
    List<QueryTagDeadlockFact> Deadlocks,
    bool ScanTruncated,
    IReadOnlyDictionary<string, string> Unavailable,
    IReadOnlyDictionary<int, string> Refused,
    IReadOnlyDictionary<string, string> Notes);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Reads every section's facts for <paramref name="serverIds"/> over [<paramref name="startUtc"/>, <paramref name="endUtc"/>]. Each section is read
    /// on its own and never added to another. Plan cache and procedures read the hourly io rollup when it reaches the window's start and the
    /// retention tier left raw (the rule the grids use, <see cref="IoHourlyCoversWindow"/>), else raw; a raw read that starts after the window does
    /// says so in <c>Notes</c>. Query Store reads the interval table only, through the same gate the MCP tool applies.
    /// <para>#6794: a read for more than one server also takes the hourly io rollup inside raw retention when it serves most of the
    /// window, by the rule the MCP tool calls (<see cref="QueryTagFleetRoute.ReadsHourlyInsideRaw"/>), because raw there is every server's
    /// raw rows for the whole window. An hourly-served section says so in <c>Notes</c>, with the edges the hour buckets move.</para>
    /// </summary>
    public async Task<QueryTagRollupData> GetQueryTagRollupDataAsync(
        IReadOnlyList<int> serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var ids = serverIds.ToArray();
        var seconds = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        var tags = await DarlingQueryTagRollupReader.ReadTagsAsync(_dataSource, cancellationToken, seconds);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var now = DateTime.UtcNow;
        var sections = new Dictionary<string, List<QueryTagRollupFact>>();
        var unavailable = new Dictionary<string, string>();
        var notes = new Dictionary<string, string>();
        var at = QueryTagUsageNotes.Stamper(ids.Length);

        async Task StatsAsync(
            QueryTagRollupSection section, string hourlySql, string rawSql, string ioView,
            bool grainHourly, string hourlyView, string dailyView, RawWindowFloor.Table floorTable)
        {
            var tier = RetentionTierRouter.Resolve(now, startUtc, grainHourly, dailyAvailable: false, coverage.For(hourlyView, dailyView));
            var measured = coverage.HourlyEndCeiling(ioView, startUtc);
            var hourly = IoHourlyCoversWindow(rollups, coverage, ioView, startUtc)
                && (tier != RetentionTier.Raw || QueryTagFleetRoute.ReadsHourlyInsideRaw(ids.Length, startUtc, endUtc, measured, coverage.FloorOf(ioView)));
            var ceiling = hourly ? measured : null;
            try
            {
                sections[section.Name] = await DarlingQueryTagRollupReader.ReadFactsAsync(
                    _dataSource, hourly ? hourlySql : rawSql, section, ids, startUtc, endUtc, ceiling, cancellationToken, seconds);
                if (hourly)
                {
                    notes[section.Name] = QueryTagUsageNotes.Hourly(HourlyWindowEdges.Describe(startUtc, null, endUtc, ceiling), at);
                }
                else
                {
                    foreach (var id in ids)
                    {
                        var floor = await RawWindowFloor.GetAsync(_dataSource, floorTable, id, startUtc, endUtc, seconds, cancellationToken);
                        if (RawWindowFloor.IsTruncated(floor, startUtc))
                        {
                            notes[section.Name] = QueryTagUsageNotes.RawFloor(QueryTagSourceWords.For(section.Name), RawWindowFloor.EffectiveStart(floor, startUtc), at);
                            break;
                        }
                    }
                }
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                sections[section.Name] = [];
                unavailable[section.Name] = QueryTagUsageNotes.Missing(section.Name);
            }
        }

        await StatsAsync(QueryTagRollupSection.PlanCache, QueryTagRollupSql.PlanCacheHourlySql, QueryTagRollupSql.PlanCacheRawSql,
            TimescaleSupport.QueryStatsIoHourlyView, rollups.QueryGrainHourly,
            TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView, RawWindowFloor.Table.QueryStats);

        var qs = await DarlingQueryTagRollupReader.ReadQueryStoreAsync(_dataSource, ids, startUtc, endUtc, null, cancellationToken, seconds);
        if (qs.TableMissing)
        {
            sections[QueryTagRollupSection.QueryStore.Name] = [];
            unavailable[QueryTagRollupSection.QueryStore.Name] = QueryTagUsageNotes.Missing(QueryTagRollupSection.QueryStore.Name);
        }
        else
        {
            sections[QueryTagRollupSection.QueryStore.Name] = qs.Facts;
            if (qs.UsedHashHourly && !string.IsNullOrEmpty(qs.WindowEdgesNote))
            {
                var first = qs.RoutedStarts is { Count: > 0 } routed ? routed.Values.Max() : startUtc;
                notes[QueryTagRollupSection.QueryStore.Name] = QueryTagUsageNotes.QueryStore(qs.UsedHashDaily, first, qs.StartDroppedFrom, endUtc, at);
            }

            if (qs.BacklogHoursWaiting > 0)
            {
                /* #6865: the fleet read leaves out the hours that wait for the hourly builder; this is the newest span that needs none. */
                var served = qs.RoutedStarts is { Count: > 0 } span ? span.Values.Max() : startUtc;
                notes[QueryTagRollupSection.QueryStore.Name] = QueryTagUsageNotes.QueryStoreBacklog(
                    served, qs.BacklogHoursLeftOut, qs.BacklogHoursWaiting, at, notes.GetValueOrDefault(QueryTagRollupSection.QueryStore.Name));
            }

            if (qs.Allowed.Count == 0)
            {
                unavailable[QueryTagRollupSection.QueryStore.Name] = "Query Store is read from the composed interval table only, and it is not readable here: "
                    + (qs.Refused.Count == 0 ? "no server was read" : string.Join("; ", qs.Refused.Values.Distinct())) + ".";
            }
        }

        await StatsAsync(QueryTagRollupSection.Procedures, QueryTagRollupSql.ProceduresHourlySql, QueryTagRollupSql.ProceduresRawSql,
            TimescaleSupport.ProcedureStatsIoHourlyView, rollups.ProcedureGrainHourly,
            TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView, RawWindowFloor.Table.ProcedureStats);

        async Task AdditiveAsync(QueryTagRollupSection section, string sql)
        {
            try
            {
                sections[section.Name] = await DarlingQueryTagRollupReader.ReadFactsAsync(
                    _dataSource, sql, section, ids, startUtc, endUtc, null, cancellationToken, seconds);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                sections[section.Name] = [];
                unavailable[section.Name] = QueryTagUsageNotes.Missing(section.Name);
            }
        }

        await AdditiveAsync(QueryTagRollupSection.BlockingReports, QueryTagRollupSql.BlockingReportsSql);
        await AdditiveAsync(QueryTagRollupSection.BlockingSnapshots, QueryTagRollupSql.BlockingSnapshotsSql);

        var deadlocks = new List<QueryTagDeadlockFact>();
        var scanTruncated = false;
        try
        {
            (deadlocks, scanTruncated) = await DarlingQueryTagRollupReader.ReadDeadlocksAsync(_dataSource, ids, startUtc, endUtc, null, cancellationToken, seconds);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            unavailable[QueryTagRollupSection.Deadlocks.Name] = QueryTagUsageNotes.Missing(QueryTagRollupSection.Deadlocks.Name);
        }

        return new QueryTagRollupData(tags, sections, deadlocks, scanTruncated, unavailable, qs.Refused, notes);
    }
}
