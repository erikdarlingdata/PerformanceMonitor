/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4229: the category pin for every Viewer/MCP read left out of #3895's <c>EventWindowFloor</c> — a read
/// windowed on an EVENT's own timestamp (<c>event_time</c>, <c>sample_time</c>, the de-skewed
/// <c>run_datetime</c>), over a table partitioned on <c>collection_time</c>, with no <c>collection_time</c>
/// lower bound to let TimescaleDB exclude a chunk. Same family as
/// <see cref="FleetReadsAreBoundedByTheFleetTests.EveryEventTableScan_CarriesThePartitionFloor"/>, extended to
/// the tab-level reads that test covers only the fleet overview: enumerated by name here, since these span
/// four different classes (<see cref="ViewerDataService"/>'s partials, <see cref="DarlingBlockingTrendReader"/>,
/// <see cref="DarlingSystemHealthReader"/>, <see cref="DarlingDefaultTraceReader"/>,
/// <see cref="DarlingDataReader"/>) rather than one reflection sweep of a single reader's public constants.
///
/// <para><b>The check.</b> Every statement's own event-time scan(s) each carry a literal
/// <c>collection_time &gt;= $N</c> predicate — counted, not just detected, because the two-CTE (XE-preferred /
/// DMV-fallback) reads have TWO scans and a floor on only one leaves the other still opening every chunk.
/// <see cref="TheRule_RefusesTheShapesThisIssueReplaced"/> proves the count is not vacuously satisfied: run
/// against each statement's own shipped text from before #4229 (kept verbatim), every one of them counts zero.</para>
/// </summary>
public sealed class EventWindowedReadsCarryTheFloorTests
{
    private static readonly Regex s_floorPredicate = new(@"collection_time\s*>=\s*\$\d", RegexOptions.Compiled);

    private static int FloorCount(string sql) => s_floorPredicate.Matches(sql).Count;

    public static TheoryData<string, string, int> Statements => new()
    {
        { nameof(ViewerDataService) + "." + nameof(ViewerDataService.BlockingTrendSql), ViewerDataService.BlockingTrendSql, 2 },
        { nameof(DarlingBlockingTrendReader) + "." + nameof(DarlingBlockingTrendReader.BlockingTrendSql), DarlingBlockingTrendReader.BlockingTrendSql, 2 },
        { nameof(ViewerDataService) + "." + nameof(ViewerDataService.SystemHealthEventsByTypeSql), ViewerDataService.SystemHealthEventsByTypeSql, 1 },
        { nameof(DarlingSystemHealthReader) + "." + nameof(DarlingSystemHealthReader.SystemHealthEventsByTypeSql), DarlingSystemHealthReader.SystemHealthEventsByTypeSql, 1 },
        { nameof(ViewerDataService) + "." + nameof(ViewerDataService.DefaultTraceEventsByWindowSql), ViewerDataService.DefaultTraceEventsByWindowSql, 1 },
        { nameof(DarlingDefaultTraceReader) + ".EventsByWindowSql", DarlingDefaultTraceReader.EventsByWindowSql, 1 },
        { nameof(ViewerDataService) + "." + nameof(ViewerDataService.BlockingDurationStatsSql), ViewerDataService.BlockingDurationStatsSql, 2 },
        { nameof(DarlingDataReader) + "." + nameof(DarlingDataReader.BlockingDurationStatsSql), DarlingDataReader.BlockingDurationStatsSql, 2 },
        { nameof(ViewerDataService) + "." + nameof(ViewerDataService.BlockingPairRowsSql), ViewerDataService.BlockingPairRowsSql, 1 },
        { nameof(ViewerDataService) + "." + nameof(ViewerDataService.MemoryPressureEventsSql), ViewerDataService.MemoryPressureEventsSql, 1 },
        /* 2, not 1: #4229's own fix split the single job_history scan into two — job_stats (the per-job
           GROUP BY) and base (the newest-2000 selection) — so this is the "two-CTE reads have TWO scans"
           case the class summary calls out, and a floor on only one would leave the other opening every
           chunk again. */
        { nameof(ViewerDataService) + ".BuildJobHistorySql(false)", ViewerDataService.BuildJobHistorySql(scopedToServer: false), 2 },
        { nameof(ViewerDataService) + ".BuildJobHistorySql(true)", ViewerDataService.BuildJobHistorySql(scopedToServer: true), 2 },
    };

    [Theory]
    [MemberData(nameof(Statements))]
    public void EveryEventWindowedRead_CarriesTheCollectionTimeFloor_OnEveryScan(string name, string sql, int expectedFloors)
    {
        Assert.True(expectedFloors >= 1, $"{name}: the theory case itself claims no floor is needed, which proves nothing");
        Assert.Equal(expectedFloors, FloorCount(sql));
    }

    /// <summary>
    /// The System Events and Default Trace readers are consumed identically by the viewer and the MCP/web
    /// twin (#4229's issue table): same table, same predicate, same columns. Pinned as exact text so the two
    /// cannot drift back apart the way #3895 found the fleet card and <c>get_server_summary</c> had.
    /// </summary>
    [Fact]
    public void SystemHealthEventsByTypeSql_TheViewerAndTheMcpTwin_AreTextuallyIdentical()
    {
        Assert.Equal(ViewerDataService.SystemHealthEventsByTypeSql, DarlingSystemHealthReader.SystemHealthEventsByTypeSql);
    }

    /// <summary>
    /// The rule's positive controls: each statement's own shipped text from immediately before #4229, kept
    /// verbatim, which the rule must refuse. Proves <see cref="EveryEventWindowedRead_CarriesTheCollectionTimeFloor_OnEveryScan"/>
    /// is not vacuously true on the tree this issue was filed against.
    /// </summary>
    [Theory]
    [InlineData("""
        WITH bpr AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY DATE_TRUNC('minute', event_time)
        ),
        dmv AS (
            SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY DATE_TRUNC('minute', event_time)
        )
        SELECT bucket, incident_count FROM bpr
        UNION ALL
        SELECT bucket, incident_count FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
        ORDER BY bucket
        """)]
    [InlineData("""
        SELECT
            event_xml
        FROM v_system_health_events
        WHERE server_id = $1
        AND   event_time >= $2
        AND   event_time <= $3
        AND   event_type = $4
        AND   event_xml IS NOT NULL
        ORDER BY event_time DESC
        """)]
    [InlineData("""
        SELECT
            sample_time,
            memory_notification,
            memory_indicators_process,
            memory_indicators_system
        FROM v_memory_pressure_events
        WHERE server_id = $1
        AND   sample_time >= $2
        AND   sample_time <= $3
        ORDER BY sample_time
        """)]
    public void TheRule_RefusesTheShapesThisIssueReplaced(string shippedBeforeTheFix)
    {
        Assert.Equal(0, FloorCount(shippedBeforeTheFix));
    }
}
