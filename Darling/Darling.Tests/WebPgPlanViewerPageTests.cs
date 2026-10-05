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
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source and payload pins for the web PostgreSQL plan viewer (#5229). The page reads the row's own
/// <c>plan</c> from <c>get_pg_plans</c>; <see cref="PgPlanViewerBehaviourTests"/> runs the module.
/// </summary>
public sealed class WebPgPlanViewerPageTests
{
    private static string Js(params string[] rel) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" }.Concat(rel).ToArray()).ReplaceLineEndings("\n");

    private static List<DarlingPgPlanCaptureReader.PgPlanCaptureRow> Rows(string planJson) => new()
    {
        new DarlingPgPlanCaptureReader.PgPlanCaptureRow(
            QueryId: -8126435036642491494,
            PlanHash: "ABC123",
            TopNodeType: "Seq Scan",
            NodeCount: 3,
            Captures: 4,
            TotalDurationMs: 120.5,
            MaxDurationMs: 42.25,
            AvgDurationMs: 30.125,
            PlanJson: planJson,
            LastSeen: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
    };

    [Fact]
    public void TheViewer_DrawsOnlyText_AndMakesNoRead()
    {
        var viewer = Js("pages", "pg-plan-viewer.js");
        Assert.DoesNotContain("innerHTML", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("insertAdjacentHTML", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("DOMParser", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("readTool", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("fetch(", viewer, StringComparison.Ordinal);
        Assert.Contains("el(\"pre\", { class: \"code plan-json\", text: pretty })", viewer, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCapturedPlansGrid_CarriesThePlanColumn_OnceWithItsImport()
    {
        var tabs = Js("pages", "server-tabs.js");
        const string column = "[...PG_PLAN_COLUMNS, pgPlanColumn(server)]";
        Assert.Equal(1, tabs.Split(column).Length - 1);
        Assert.Contains("import { pgPlanColumn } from \"./pg-plan-viewer.js\";", tabs, StringComparison.Ordinal);
    }

    [Fact]
    public void GetPgPlans_EmitsTheThreeKeysThePageReads()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows("""{"Plan":{"Node Type":"Seq Scan"}}"""), 10));
        var row = doc.RootElement.GetProperty("plans")[0];
        Assert.Equal(JsonValueKind.Object, row.GetProperty("plan").ValueKind);
        Assert.Equal(JsonValueKind.String, row.GetProperty("queryid").ValueKind);
        Assert.True(row.TryGetProperty("plan_hash", out _));
    }

    [Fact]
    public void APlanThatDoesNotParse_ReachesThePageAsAString()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows("{\"Plan\": {\"Node Ty"), 10));
        Assert.Equal(JsonValueKind.String, doc.RootElement.GetProperty("plans")[0].GetProperty("plan").ValueKind);
    }

    [Fact]
    public void NoSeparatePlanJsonRead_WasAdded()
    {
        var reads = DarlingWebEndpoints.CatalogDescriptors.Keys
            .Where(k => k.StartsWith("get_pg_plan", StringComparison.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[] { "get_pg_plan_capture_readiness", "get_pg_plans" }, reads);
    }
}
