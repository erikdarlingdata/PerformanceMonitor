/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Xunit;
using PerformanceMonitor.Darling.Service;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the web stored-plan viewer (#4843): the one read it makes, the params it sends, and the three
/// grids that carry its Plan column. <see cref="PlanViewerBehaviourTests"/> runs the module.
/// </summary>
public sealed class WebPlanViewerPageTests
{
    private static string Js(params string[] rel) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" }.Concat(rel).ToArray()).ReplaceLineEndings("\n");

    [Fact]
    public void TheViewer_ReadsGetPlanXml_WithItsRequiredAndOptionalParams()
    {
        var viewer = Js("pages", "plan-viewer.js");
        Assert.Contains("const PLAN_READ = \"get_plan_xml\"", viewer, StringComparison.Ordinal);
        Assert.Contains("readTool(PLAN_READ, params)", viewer, StringComparison.Ordinal);
        Assert.Contains("{ server, query_hash: hash }", viewer, StringComparison.Ordinal);
        Assert.Contains("params.database_name = database", viewer, StringComparison.Ordinal);

        Assert.True(DarlingWebEndpoints.CatalogDescriptors.TryGetValue("get_plan_xml", out var d), "get_plan_xml is not on the read surface");
        var names = d!.Params.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("query_hash", names);
        Assert.Contains("server", names);
        Assert.Contains("database_name", names);
        Assert.True(d.Params.Single(p => p.Name == "query_hash").Required);
        Assert.False(d.Params.Single(p => p.Name == "database_name").Required);
    }

    [Fact]
    public void TheViewer_NeverDrawsTheXmlAsMarkup()
    {
        var viewer = Js("pages", "plan-viewer.js");
        Assert.DoesNotContain("innerHTML", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("insertAdjacentHTML", viewer, StringComparison.Ordinal);
        Assert.Contains("el(\"pre\", { class: \"code plan-xml\", text: pretty })", viewer, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pages/server-tabs.js", "[...TOP_QUERY_COLUMNS, planColumn(server)]", 2)]
    [InlineData("pages/finops/high-impact.js", "[...COLUMNS, planColumn(server)]", 1)]
    [InlineData("pages/finops/optimization.js", "[...QUERY_COLUMNS, planColumn(server)]", 1)]
    public void EachGridWithAQueryHash_AddsThePlanColumn(string file, string columns, int count)
    {
        var src = Js(file.Split('/'));
        Assert.Contains("plan-viewer.js\"", src, StringComparison.Ordinal);
        Assert.Equal(count, src.Split(columns).Length - 1);
    }

    [Fact]
    public void ThePostgresTwin_HasNoPlanColumn_BecauseGetPlanXmlIsSqlServerOnly()
    {
        var tabs = Js("pages", "server-tabs.js");
        var pg = tabs.Substring(tabs.IndexOf("const PG_TOP_QUERY_COLUMNS", StringComparison.Ordinal));
        pg = pg.Substring(0, pg.IndexOf("];", StringComparison.Ordinal));
        Assert.DoesNotContain("planColumn", pg, StringComparison.Ordinal);
    }
}
