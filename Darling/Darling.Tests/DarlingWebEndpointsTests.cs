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
using System.Reflection;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web dashboard's read surface (#1562). The parity pin is the load-bearing one: it reflects the WHOLE
/// <c>[McpServerTool]</c> catalog off the service assembly and asserts the <c>/api/read/*</c> dispatch table
/// equals that catalog MINUS exactly the documented exclusions — so a tool added later cannot be silently
/// missed from the web surface (it fails this test until it is wired or explicitly excluded). The response-kind
/// and query-parse pins cover the thin glue around the reused tool methods.
/// </summary>
public sealed class DarlingWebEndpointsTests
{
    /// <summary>Every <c>[McpServerTool(Name = ...)]</c> across every <c>[McpServerToolType]</c> class in the
    /// service assembly — the full tool catalog, discovered the same way the MCP host registers them.</summary>
    private static HashSet<string> ReflectToolCatalog()
    {
        var assembly = typeof(DarlingMcpTools).Assembly;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in assembly.GetTypes())
        {
            if (type.GetCustomAttribute<McpServerToolTypeAttribute>() is null)
            {
                continue;
            }

            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            {
                var attr = method.GetCustomAttribute<McpServerToolAttribute>();
                if (attr?.Name is { Length: > 0 } name)
                {
                    names.Add(name);
                }
            }
        }

        return names;
    }

    [Fact]
    public void ReadEndpoints_AreExactlyTheReadToolCatalog_MinusTheDocumentedExclusions()
    {
        var catalog = ReflectToolCatalog();
        var endpoints = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);

        var expected = catalog.Except(DarlingWebEndpoints.ExcludedToolNames).ToHashSet(StringComparer.Ordinal);

        /* Symmetric-difference messages so a miss names the exact tool. */
        var missing = expected.Except(endpoints).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        var extra = endpoints.Except(expected).OrderBy(n => n, StringComparer.Ordinal).ToArray();

        Assert.True(missing.Length == 0, "read-only tools with no /api/read endpoint: " + string.Join(", ", missing));
        Assert.True(extra.Length == 0, "/api/read endpoints with no matching read-only tool: " + string.Join(", ", extra));
    }

    [Fact]
    public void ExcludedToolNames_AreAllRealToolsInTheCatalog()
    {
        /* Guards against a typo in the exclusion list silently dropping a real tool from the parity check. */
        var catalog = ReflectToolCatalog();
        foreach (var excluded in DarlingWebEndpoints.ExcludedToolNames)
        {
            Assert.Contains(excluded, catalog);
        }
    }

    [Fact]
    public void ExcludedToolNames_AreTheNonReadSurfaceTools()
    {
        /* The six original non-read tools (analyze_server, the mute write, the four analyze_*_plan), the eight
           Custom Views tools (#1599 + describe_custom_view_catalog) served by /api/views + /api/compose/run +
           /api/catalog, the eight custom-alert-rule tools (#3285 — create/update/delete write, get/list/validate
           read against the compose catalog, test_custom_alert_rule (#3299) evaluate-now, and
           list_custom_alert_templates (#3285 Component 7) starter templates, none a
           /api/read/{tool} mirror), the three alert-tuning WRITE tools, and the two server-onboarding WRITE tools
           (add_servers / remove_server) — all with no /api/read/{tool} 1:1 mirror, like mute_analysis_finding. */
        Assert.Equal(
            new[]
            {
                "add_servers", "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan",
                "analyze_server", "create_custom_alert_rule", "create_custom_view", "create_mute_rule", "delete_custom_alert_rule",
                "delete_custom_view", "delete_mute_rule", "describe_custom_view_catalog", "get_custom_alert_rule", "get_custom_view",
                "list_custom_alert_rules", "list_custom_alert_templates", "list_custom_views", "mute_analysis_finding", "remove_server", "run_custom_view_panel",
                "test_custom_alert_rule", "update_alert_settings", "update_custom_alert_rule", "update_custom_view", "validate_custom_alert_rule", "validate_custom_view",
            },
            DarlingWebEndpoints.ExcludedToolNames.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void ReadEndpoints_IncludeTheNewFleetOverviewTool()
    {
        /* get_fleet_overview is a read tool born with this feature — it must be in both the catalog and the
           web surface (its own /api/read entry, alongside the richer pre-banded /api/fleet DTO endpoint). */
        Assert.Contains("get_fleet_overview", ReflectToolCatalog());
        Assert.Contains("get_fleet_overview", DarlingWebEndpoints.BuildReadDispatch().Keys);
    }

    [Fact]
    public void ReadEndpoints_IncludeTheAgHealthTool()
    {
        /* get_ag_health (#991) is a read tool, so it must be in both the catalog and the web surface — its own
           /api/read entry alongside the richer pre-banded /api/ag DTO endpoint the topology page reads. */
        Assert.Contains("get_ag_health", ReflectToolCatalog());
        Assert.Contains("get_ag_health", DarlingWebEndpoints.BuildReadDispatch().Keys);
    }

    /* ── response-kind mapping (the '{'-sniff, error -> 500, status-envelope -> 200) ── */

    [Theory]
    [InlineData("{\"cpu_percent\":42}")]
    [InlineData("  {\"cpu_percent\":42}")]                 // leading whitespace still sniffs as JSON
    [InlineData("[]")]
    [InlineData("[{\"a\":1}]")]
    [InlineData("{\"status\":\"empty\",\"message\":\"nothing\"}")] // the miss envelope passes through as 200
    public void ClassifyToolResponse_JsonPassesThrough(string result) =>
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.JsonPassthrough, DarlingWebEndpoints.ClassifyToolResponse(result));

    [Fact]
    public void ClassifyToolResponse_ErrorDuring_IsServerError() =>
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ServerError,
            DarlingWebEndpoints.ClassifyToolResponse("Error during get_wait_stats: connection reset"));

    [Theory]
    [InlineData("Could not resolve server. Available servers:\nSQL2022")]
    [InlineData("Invalid hours_back value '9999'. Must be a positive integer (1-168).")]
    [InlineData("Missing required parameter 'wait_type'.")]
    [InlineData("baseline_hours_back must be greater than hours_back.")]
    [InlineData("")]
    public void ClassifyToolResponse_OtherBareStrings_AreClientErrors(string result) =>
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ClientError, DarlingWebEndpoints.ClassifyToolResponse(result));

    /* ── query-string parse helpers (invariant, default-on-miss) ── */

    [Theory]
    [InlineData("50", 50)]
    [InlineData(null, 10)]
    [InlineData("", 10)]
    [InlineData("not-a-number", 10)]
    public void ParseInt_DefaultsOnMissOrGarbage(string? raw, int expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.ParseInt(raw, 10));

    [Theory]
    [InlineData("true", false, true)]
    [InlineData("false", true, false)]
    [InlineData(null, true, true)]
    [InlineData("garbage", false, false)]
    public void ParseBool_DefaultsOnMissOrGarbage(string? raw, bool def, bool expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.ParseBool(raw, def));

    [Theory]
    [InlineData("0.5", 0.5)]
    [InlineData(null, 0.0)]
    [InlineData("bad", 0.0)]
    public void ParseDouble_DefaultsOnMissOrGarbage(string? raw, double expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.ParseDouble(raw, 0.0));

    /// <summary>
    /// #3287's optional numeric FILTER binding, which deliberately does NOT behave like
    /// <see cref="DarlingWebEndpoints.ParseDouble"/> beside it.
    ///
    /// <para>Every other optional knob on this dispatch falls back to its default on a value it cannot read,
    /// so <c>?hours=abc</c> quietly means 24. For a filter that same fallback means the filter does not
    /// apply and the caller receives a complete-looking UNFILTERED page — the silently-dropped-parameter
    /// failure <c>min_duration_ms</c> was added to remove, reintroduced one layer down. So an unreadable
    /// filter is REFUSED: absent binds null, a number binds the number, and garbage returns false and reaches
    /// the caller as a message.</para>
    ///
    /// <para>Zero and a negative both BIND rather than being rejected here. This layer only decides whether a
    /// value was readable; the tool owns the range refusal, so the web and MCP surfaces cannot disagree about
    /// what a bad floor means — the rule <c>AsOf</c> already follows for the same reason.</para>
    /// </summary>
    [Theory]
    [InlineData(null, true, null)]        // absent: no filter, and that is not an error
    [InlineData("5000", true, 5000.0)]    // a plain number
    [InlineData("0", true, 0.0)]          // zero is a real floor, not an absent one
    [InlineData("-1", true, -1.0)]        // readable; the TOOL refuses the range, not this layer
    [InlineData("2.5", true, 2.5)]
    [InlineData("abc", false, null)]      // unreadable: refused, NOT defaulted to "no filter"
    [InlineData("", true, null)]          // an empty value is an absent one (First() returns null)
    public void TryParseOptionalDouble_RefusesGarbageRatherThanDroppingTheFilter(string? raw, bool expectedOk, double? expectedValue)
    {
        /* First() maps an empty query value to null, so the empty case arrives here as null. */
        var ok = DarlingWebEndpoints.TryParseOptionalDouble(string.IsNullOrEmpty(raw) ? null : raw, out var value);

        Assert.Equal(expectedOk, ok);
        Assert.Equal(expectedValue, value);
    }

    /// <summary>
    /// And the sibling this is NOT: <c>ParseDouble</c> really does swallow the same garbage, so the theory
    /// above is pinning a difference rather than restating shared behaviour.
    /// </summary>
    [Fact]
    public void TheFilterBinding_DiffersFromTheDefaultingOne_OnGarbage()
    {
        Assert.Equal(0.0, DarlingWebEndpoints.ParseDouble("abc", 0.0));
        Assert.False(DarlingWebEndpoints.TryParseOptionalDouble("abc", out _));
    }

    /* ── row-count clamp: the abuse bound on ?limit= / ?top= (security review M3) ── */

    [Theory]
    [InlineData(20, 20)]        // ordinary request unchanged
    [InlineData(1000, 1000)]    // exactly the ceiling
    [InlineData(50000, 1000)]   // an unbounded ask is clamped to the ceiling
    [InlineData(0, 1)]          // zero/negative floor to 1
    [InlineData(-5, 1)]
    public void ClampRows_BoundsCallerSuppliedRowCounts(int requested, int expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.ClampRows(requested));

    /* ── custom-alert-rule wire-shape builders (#3285): the ONE shape shared by the /api/alerts responses AND
       the MCP alert tools (get/create/update/list), so the two surfaces cannot drift. ── */

    [Fact]
    public void BuildFullRuleNode_CarriesEnabled_AndEmbedsTheDefinitionAsAnObject()
    {
        var created = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var updated = new DateTime(2026, 1, 2, 3, 5, 6, DateTimeKind.Utc);
        var rule = new CustomAlertRule(
            Id: 7, Name: "PG dead tuples", DefinitionJson: "{\"predicate\":{\"op\":\"gt\",\"warnThreshold\":1000}}",
            Description: "desc", Enabled: false, Version: 3, CreatedAt: created, UpdatedAt: updated, UpdatedBy: "web");

        var node = DarlingWebEndpoints.BuildFullRuleNode(rule);

        foreach (var key in new[] { "id", "name", "description", "definition", "enabled", "version", "created_at", "updated_at", "updated_by" })
        {
            Assert.True(node.ContainsKey(key), "missing key: " + key);
        }

        Assert.Equal(7L, (long)node["id"]!);
        Assert.Equal("PG dead tuples", (string)node["name"]!);
        Assert.False((bool)node["enabled"]!);          // 'enabled' round-trips (the view shape carries no such field)
        Assert.Equal(3, (int)node["version"]!);

        // The definition is an embedded JSON object (NOT an escaped string), so a client reads its fields directly.
        var definition = Assert.IsType<JsonObject>(node["definition"]);
        Assert.Equal("gt", (string)definition["predicate"]!["op"]!);
    }

    [Fact]
    public void BuildRuleSummariesNode_IsABareArray_WithEnabled_AndNoDefinitionBody()
    {
        var summaries = new List<CustomAlertRuleSummary>
        {
            new(Id: 11, Name: "blocking", Description: null, Enabled: true, Version: 2,
                UpdatedAt: new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), UpdatedBy: "mcp", LastFired: null),
        };

        var array = DarlingWebEndpoints.BuildRuleSummariesNode(summaries);

        var only = Assert.IsType<JsonObject>(Assert.Single(array));
        Assert.Equal(11L, (long)only["id"]!);
        Assert.Equal("blocking", (string)only["name"]!);
        Assert.True((bool)only["enabled"]!);
        Assert.False(only.ContainsKey("definition"));  // the list projection never carries the definition body
    }

    [Fact]
    public void BuildRuleSummariesNode_CarriesLastFired_OrNullWhenNeverFired()
    {
        var firedAt = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc);
        var summaries = new List<CustomAlertRuleSummary>
        {
            new(Id: 1, Name: "fired", Description: null, Enabled: true, Version: 1,
                UpdatedAt: firedAt, UpdatedBy: null, LastFired: firedAt),
            new(Id: 2, Name: "never", Description: null, Enabled: true, Version: 1,
                UpdatedAt: firedAt, UpdatedBy: null, LastFired: null),
        };

        var array = DarlingWebEndpoints.BuildRuleSummariesNode(summaries);

        var fired = Assert.IsType<JsonObject>(array[0]);
        Assert.True(fired.ContainsKey("last_fired"));
        Assert.Equal(firedAt, fired["last_fired"]!.GetValue<DateTime>());

        // The key is ALWAYS present (a stable wire shape) — a never-fired rule carries JSON null, not an absent key.
        var never = Assert.IsType<JsonObject>(array[1]);
        Assert.True(never.ContainsKey("last_fired"));
        Assert.Null(never["last_fired"]);
    }
}
