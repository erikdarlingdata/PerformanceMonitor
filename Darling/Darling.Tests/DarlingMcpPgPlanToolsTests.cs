/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Wire shape for <c>get_pg_plans</c> (#2567), asserted against the real projection rather than a
/// re-implementation of it — a guard that rebuilt the shape would keep passing while the shipped one drifted.
/// </summary>
public class DarlingMcpPgPlanToolsTests
{
    /// <summary>A queryid past 2^53, which is where a JSON number stops being able to hold one exactly.</summary>
    private const long BigQueryId = -8126435036642491494;

    private static List<DarlingPgPlanCaptureReader.PgPlanCaptureRow> Rows() => new()
    {
        new DarlingPgPlanCaptureReader.PgPlanCaptureRow(
            QueryId: BigQueryId,
            PlanHash: "ABC123",
            TopNodeType: "Seq Scan",
            NodeCount: 3,
            Captures: 4,
            TotalDurationMs: 120.5,
            MaxDurationMs: 42.25,
            AvgDurationMs: 30.125,
            PlanJson: """{"Plan":{"Node Type":"Seq Scan","Relation Name":"orders","Filter":"(id > ?)"}}""",
            LastSeen: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
    };

    /// <summary>
    /// <b>queryid is a STRING on the wire.</b> It is a signed int8 spread over the whole 64-bit range, so
    /// most ids are past 2^53 and any consumer decoding JSON numbers as IEEE-754 doubles rounds one. It is
    /// also an equality join key, so a rounded value does not approximate the answer — it matches nothing.
    /// </summary>
    [Fact]
    public void QueryId_IsSerializedAsAString_AndSurvivesExactly()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0];
        var queryId = plan.GetProperty("queryid");

        Assert.Equal(JsonValueKind.String, queryId.ValueKind);
        Assert.Equal("-8126435036642491494", queryId.GetString());

        /* The point of the string, demonstrated: the same value through a double loses its identity. */
        Assert.NotEqual(BigQueryId, (long)(double)BigQueryId);
    }

    /// <summary>
    /// The plan is returned as parsed JSON, not as an opaque string and not as an id to fetch later. #2538
    /// is explicit: an agent has no viewer to follow a reference into, so a pointer is not an answer.
    /// </summary>
    [Fact]
    public void ThePlanItself_IsReturnedAsNavigableJson()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0].GetProperty("plan");

        Assert.Equal(JsonValueKind.Object, plan.ValueKind);
        Assert.Equal("Seq Scan", plan.GetProperty("Plan").GetProperty("Node Type").GetString());
        Assert.Equal("orders", plan.GetProperty("Plan").GetProperty("Relation Name").GetString());
    }

    /// <summary>
    /// <c>captures</c> is named for what it counts. The collector reads an overlapping tail of the server
    /// log, so one execution can be seen twice — calling it <c>executions</c> would invite a reader to
    /// divide by it.
    /// </summary>
    [Fact]
    public void CaptureCount_IsNotPresentedAsAnExecutionCount()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0];

        Assert.Equal(4, plan.GetProperty("captures").GetInt64());
        Assert.False(plan.TryGetProperty("executions", out _));
        Assert.False(plan.TryGetProperty("calls", out _));
    }

    /// <summary>
    /// The response says the plans are redacted. A consumer that does not know this could reasonably read a
    /// placeholder-bearing filter as the literal the query used.
    /// </summary>
    [Fact]
    public void TheResponseSaysThePlansAreRedacted()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var note = doc.RootElement.GetProperty("note").GetString();

        Assert.Contains("REDACTED", note, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("get_pg_top_queries", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// A plan that will not re-parse is still shown, as its raw text. Dropping it would hide the one row
    /// somebody needs to explain, and throwing would take out the whole response for it.
    /// </summary>
    [Fact]
    public void AnUnparseablePlan_IsShownRatherThanDropped()
    {
        var rows = new List<DarlingPgPlanCaptureReader.PgPlanCaptureRow>
        {
            Rows()[0] with { PlanJson = "{not valid json" },
        };

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, rows, 10));

        var plan = doc.RootElement.GetProperty("plans")[0].GetProperty("plan");

        Assert.Equal(JsonValueKind.String, plan.ValueKind);
        Assert.Equal("{not valid json", plan.GetString());
    }
}
