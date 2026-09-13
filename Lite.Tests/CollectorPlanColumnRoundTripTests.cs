/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for the #1262 plan columns on procedure_stats, blocked_process_reports
/// and deadlocks. The collector-definition tests pin PayloadColumns/WritePayload against a recording
/// writer, but nothing else proves the Lite DuckDB table's physical column layout stays aligned with
/// WritePayload — the DuckDB appender writes BY POSITION, so a table whose column count/order drifts
/// from (4 prefix + WritePayload) fails at EndRow(). This exercises the exact append path
/// RemoteCollectorService.RunCollectorDefinitionAsync uses (CreateAppender → prefix → WritePayload →
/// EndRow) against a freshly initialized schema, then reads the trailing plan column back. It also
/// guards the append itself: an un-migrated legacy DB (fewer columns) would throw here.
/// </summary>
public sealed class CollectorPlanColumnRoundTripTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public CollectorPlanColumnRoundTripTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ProcedureStats_AppendWithPlan_RoundTripsTrailingColumn()
    {
        await AssertRoundTripsAsync(
            ProcedureStatsCollector.Instance,
            new ProcedureStatsCollector.Row(
                "DB", "dbo", "usp_X", "PROCEDURE",
                new DateTime(2026, 7, 4, 1, 0, 0, DateTimeKind.Utc), new DateTime(2026, 7, 4, 2, 0, 0, DateTimeKind.Utc),
                1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L,
                "0xSH", "0xPH", "<ShowPlanXML>proc</ShowPlanXML>", 4096L),
            ("query_plan_xml", "<ShowPlanXML>proc</ShowPlanXML>"),
            ("query_plan_xml_bytes", 4096L));
    }

    /// <summary>
    /// #3392: query_stats' own trailing pair. query_plan_xml_bytes is the newest payload column on the
    /// widest collector in the product, and the appender writes BY POSITION — so a DuckDB table missing it
    /// fails EndRow() for the whole batch, not just the column. Reading the size back as a BIGINT also pins
    /// the storage type: a VARCHAR column would accept the append and hand back a string.
    /// </summary>
    [Fact]
    public async Task QueryStats_AppendWithPlan_RoundTripsTrailingPlanColumns()
    {
        var row = new QueryStatsCollector.Row
        {
            DatabaseName = "DB",
            QueryHash = "0xQH",
            QueryPlanXml = "<ShowPlanXML>query</ShowPlanXML>",
            QueryPlanXmlBytes = 8_388_608L,
            SqlHandle = "0xSH",
            PlanHandle = "0xPH",
        };

        await AssertRoundTripsAsync(QueryStatsCollector.Instance, row,
            ("query_plan_xml", "<ShowPlanXML>query</ShowPlanXML>"),
            ("query_plan_xml_bytes", 8_388_608L));
    }

    [Fact]
    public async Task BlockedProcessReports_AppendWithPlans_RoundTripsBothTrailingColumns()
    {
        var row = new BlockedProcessReportCollector.Row
        {
            EventTime = new DateTime(2026, 7, 4, 3, 0, 0, DateTimeKind.Utc),
            BlockedQueryPlanXml = "<ShowPlanXML>blocked</ShowPlanXML>",
            BlockingQueryPlanXml = "<ShowPlanXML>blocking</ShowPlanXML>",
        };

        await AssertRoundTripsAsync(BlockedProcessReportCollector.Instance, row,
            ("blocked_query_plan_xml", "<ShowPlanXML>blocked</ShowPlanXML>"),
            ("blocking_query_plan_xml", "<ShowPlanXML>blocking</ShowPlanXML>"));
    }

    [Fact]
    public async Task Deadlocks_AppendWithVictimPlan_RoundTripsTrailingColumn()
    {
        var row = new DeadlocksCollector.Row
        {
            DeadlockTime = new DateTime(2026, 7, 4, 4, 0, 0, DateTimeKind.Utc),
            VictimProcessId = "pv",
            VictimQueryPlanXml = "<ShowPlanXML>victim</ShowPlanXML>",
        };

        await AssertRoundTripsAsync(DeadlocksCollector.Instance, row,
            ("victim_query_plan_xml", "<ShowPlanXML>victim</ShowPlanXML>"));
    }

    /// <summary>
    /// Appends one row through the exact runner path (prefix columns + WritePayload + EndRow) against a
    /// freshly initialized DuckDB, then asserts each named column read back its expected plan text. EndRow
    /// throwing here would mean the Schema.cs table column count/order drifted from WritePayload.
    /// </summary>
    private async Task AssertRoundTripsAsync<TRow>(
        ICollectorDefinition<TRow> definition, TRow row, params (string Column, object? Expected)[] columns)
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using (var appender = connection.CreateAppender(definition.TargetTable))
        {
            var appenderRow = appender.CreateRow();
            if (definition.IncludesCollectionId)
            {
                appenderRow.AppendValue(1L);
            }
            appenderRow.AppendValue(context.CollectionTime).AppendValue(42).AppendValue("s");

            var writer = new AppenderCollectorRowWriter { CurrentRow = appenderRow };
            definition.WritePayload(row, writer, context);
            appenderRow.EndRow();   /* throws if the appended column count != table column count */
        }

        foreach (var (column, expected) in columns)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT {column} FROM {definition.TargetTable} LIMIT 1";
            var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
            Assert.Equal(expected, value);
        }
    }
}
