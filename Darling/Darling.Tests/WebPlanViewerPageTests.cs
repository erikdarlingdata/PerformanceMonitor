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
        Assert.Contains("read: \"get_plan_xml\"", viewer, StringComparison.Ordinal);
        Assert.Contains("readTool(spec.read, { server, ...spec.params(source) })", viewer, StringComparison.Ordinal);
        Assert.Contains("params: (s) => ({ query_hash: s.query_hash, database_name: s.database_name || null })", viewer, StringComparison.Ordinal);

        Assert.True(DarlingWebEndpoints.CatalogDescriptors.TryGetValue("get_plan_xml", out var d), "get_plan_xml is not on the read surface");
        var names = d!.Params.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("query_hash", names);
        Assert.Contains("server", names);
        Assert.Contains("database_name", names);
        Assert.True(d.Params.Single(p => p.Name == "query_hash").Required);
        Assert.False(d.Params.Single(p => p.Name == "database_name").Required);
    }

    /// <summary>Each plan source kind (#5228) reads a tool that is on the web read surface, and sends every parameter
    /// that tool requires. The kinds and their params are parsed from the shipped SOURCES map.</summary>
    [Theory]
    [InlineData("query_hash", "get_plan_xml", new[] { "query_hash" })]
    [InlineData("active_snapshot", "get_active_query_plan_xml", new[] { "collection_time", "session_id" })]
    [InlineData("query_store", "get_query_store_plan_xml", new[] { "database_name", "query_id" })]
    [InlineData("procedure", "get_procedure_plan_xml", new[] { "sql_handle" })]
    [InlineData("blocking", "get_blocking_plan_xml", new[] { "event_time", "blocked_spid", "blocking_spid" })]
    [InlineData("deadlock_victim", "get_deadlock_plan_xml", new[] { "collection_time", "deadlock_time" })]
    public void EachPlanSourceKind_ReadsACatalogTool_AndSendsItsRequiredParams(string kind, string tool, string[] required)
    {
        var viewer = Js("pages", "plan-viewer.js");
        var start = viewer.IndexOf("  " + kind + ": {", StringComparison.Ordinal);
        Assert.True(start >= 0, kind + " is not in SOURCES");
        var end = viewer.IndexOf("\n  },", start, StringComparison.Ordinal);
        var block = viewer.Substring(start, end - start);
        Assert.Contains("read: \"" + tool + "\"", block, StringComparison.Ordinal);

        Assert.True(DarlingWebEndpoints.CatalogDescriptors.TryGetValue(tool, out var d), tool + " is not on the read surface");
        var catalogRequired = d!.Params.Where(p => p.Required).Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(required.OrderBy(n => n, StringComparer.Ordinal).ToArray(), catalogRequired);

        var paramsStart = block.IndexOf("params:", StringComparison.Ordinal);
        var paramsBlock = block.Substring(paramsStart, block.IndexOf("key:", paramsStart, StringComparison.Ordinal) - paramsStart);
        foreach (var name in d.Params.Select(p => p.Name).Where(n => n != "server"))
        {
            if (kind == "active_snapshot" && name == "live") Assert.Contains("live:", paramsBlock, StringComparison.Ordinal);
            else if (d.Params.Single(p => p.Name == name).Required) Assert.Contains(name, paramsBlock, StringComparison.Ordinal);
        }

        Assert.False(d.Params.Any(p => p.Name is "hours" or "as_of"), tool + " is a point read: it takes no window");
    }

    [Fact]
    public void TheTimestamp_GoesToTheRead_AsTheRowHoldsIt_NeverThroughADate()
    {
        var viewer = Js("pages", "plan-viewer.js");
        Assert.DoesNotContain("new Date", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("Date.parse", viewer, StringComparison.Ordinal);
        Assert.Contains("collection_time: s.collection_time,", viewer, StringComparison.Ordinal);
        // The blocked-process-report and deadlock reads (#5236) match their stamps for equality as well.
        Assert.Contains("event_time: s.event_time,", viewer, StringComparison.Ordinal);
        Assert.Contains("deadlock_time: s.deadlock_time,", viewer, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ACTIVE_COLUMNS", "[...ACTIVE_COLUMNS, ...activePlanColumns(server)]")]
    [InlineData("QUERY_STORE_COLUMNS", "[...QUERY_STORE_COLUMNS, queryStorePlanColumn(server)]")]
    [InlineData("TOP_PROC_COLUMNS", "[...TOP_PROC_COLUMNS, procedurePlanColumn(server)]")]
    [InlineData("BLOCKING_COLUMNS", "[...BLOCKING_COLUMNS, ...blockingPlanColumns(server)]")]
    [InlineData("DEADLOCK_COLUMNS", "[...DEADLOCK_COLUMNS, deadlockPlanColumn(server)]")]
    public void TheRowGrids_AddTheirPlanColumns_WhereTheyAreBuilt(string list, string composed)
    {
        var tabs = Js("pages", "server-tabs.js");
        Assert.Contains(composed, tabs, StringComparison.Ordinal);
        // The bare list is no longer handed to a grid; the composed form is, as many times as the list had call sites.
        var bareCalls = tabs.Split("        " + list + ",\n").Length - 1;
        Assert.Equal(0, bareCalls);
    }

    [Fact]
    public void TheServerTabsImport_NamesTheFactories()
    {
        var tabs = Js("pages", "server-tabs.js");
        var line = tabs.Split('\n').Single(l => l.EndsWith("from \"./plan-viewer.js\";", StringComparison.Ordinal));
        foreach (var name in new[] { "activePlanColumns", "blockingPlanColumns", "deadlockPlanColumn", "planColumn", "procedurePlanColumn", "queryStorePlanColumn" })
            Assert.Contains(name, line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFactories_KeyEachColumnUniquely_OnTheFieldThatIsPresentWhenAButtonShows()
    {
        var viewer = Js("pages", "plan-viewer.js");
        Assert.Contains("key: \"has_query_plan\"", viewer, StringComparison.Ordinal);
        Assert.Contains("key: \"has_live_query_plan\"", viewer, StringComparison.Ordinal);
        Assert.Contains("key: \"sql_handle\"", viewer, StringComparison.Ordinal);
        // The blocking and deadlock columns (#5236) each key on their own presence flag, one flag per button.
        Assert.Contains("key: \"has_blocked_plan\"", viewer, StringComparison.Ordinal);
        Assert.Contains("key: \"has_blocking_plan\"", viewer, StringComparison.Ordinal);
        Assert.Contains("key: \"has_victim_plan\"", viewer, StringComparison.Ordinal);
        // The original stored-plan column keeps query_hash; no new column reuses it.
        Assert.Equal(1, viewer.Split("key: \"query_hash\",\n    label: \"Plan\"").Length - 1);
    }

    /// <summary>The blocking and deadlock buttons (#5236) draw only where their presence flag is exactly true. The server
    /// sends the flag as true or leaves it off, so a looser test (truthy, not false) would draw a button for a value the
    /// server never sends, and that button could only answer "unavailable".</summary>
    [Theory]
    [InlineData("has_blocked_plan", "Blocked plan")]
    [InlineData("has_blocking_plan", "Blocking plan")]
    [InlineData("has_victim_plan", "Victim plan")]
    public void TheBlockingAndDeadlockButtons_DrawOnlyWhereTheirFlagIsExactlyTrue(string flag, string label)
    {
        var viewer = Js("pages", "plan-viewer.js");
        var start = viewer.IndexOf("key: \"" + flag + "\"", StringComparison.Ordinal);
        Assert.True(start >= 0, flag + " is not a column key");
        var column = viewer.Substring(start, viewer.IndexOf("copy: false", start, StringComparison.Ordinal) - start);
        Assert.Contains("label: \"" + label + "\"", column, StringComparison.Ordinal);
        Assert.Contains("row." + flag + " === true", column, StringComparison.Ordinal);
        // The flag is read once, by that strict comparison, and nowhere else in the column.
        Assert.Equal(1, column.Split("row." + flag).Length - 1);
        Assert.Contains("hideWhenEmpty: true", column, StringComparison.Ordinal);
        Assert.Contains("sortable: false", column, StringComparison.Ordinal);
        Assert.Contains("filter: false", column, StringComparison.Ordinal);
        Assert.Contains("csv: false", column, StringComparison.Ordinal);
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
