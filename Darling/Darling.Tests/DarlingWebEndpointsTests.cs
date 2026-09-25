/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
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
           /api/read/{tool} mirror), the five alert-tuning WRITE tools — of which the four mute-rule verbs are
           served by their OWN dedicated /api/mute-rules endpoints since #3450, the Custom Views disposition,
           so they STAY excluded from the generic mirror for the same reason those do, while
           update_alert_settings remains a write with no web surface at all — and the two server-onboarding
           WRITE tools (add_servers / remove_server). All with no /api/read/{tool} 1:1 mirror, like
           mute_analysis_finding. get_tool_guide (#3898) reads no data: it serves the MCP tools' reading guides from
           the MCP host's registration-time catalog, so there is nothing for a web read to mirror. */
        Assert.Equal(
            new[]
            {
                "add_servers", "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan",
                "analyze_server", "create_custom_alert_rule", "create_custom_view", "create_mute_rule", "delete_custom_alert_rule",
                "delete_custom_view", "delete_mute_rule", "delete_notification_route", "describe_custom_view_catalog", "get_custom_alert_rule", "get_custom_view", "get_tool_guide",
                "list_custom_alert_rules", "list_custom_alert_templates", "list_custom_views", "mute_analysis_finding", "remove_server", "run_custom_view_panel",
                "set_mute_rule_enabled", "set_notification_route_enabled", "test_custom_alert_rule", "update_alert_settings", "update_custom_alert_rule", "update_custom_view", "update_mute_rule", "validate_custom_alert_rule", "validate_custom_view",
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

    [Fact]
    public void ReadEndpoints_ActiveQueries_KeepsTheTwoThousandCharacterWebPreview()
    {
        /* #4198 lane W2: the MCP default fell to a 500-char query_text preview (QueryTextPreviewLength), but
           the web viewer isn't that budget's caller — its /api/read row calls the internal budget-taking
           overload with an explicit 2000, the pre-#4198 McpHelpers.Truncate budget every caller got, so the
           Active Queries tab doesn't shrink under it. A regression here (dropping the overload, or the literal
           2000) silently starves that tab's query text down to 500 characters. Source-text pin rather than a
           live call: no rig in this lane. */
        var source = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Contains(
            "[\"get_active_queries\"] = (c, pg, an) => DarlingMcpSessionTools.GetActiveQueries(pg, Server(c), Hours(c, 1), Str(c, \"database_name\"), QueryBool(c, \"blocking_only\", false), Rows(c, \"limit\", 50), 2000, AsOf(c)),",
            source, StringComparison.Ordinal);
    }

    /* ── response-kind mapping (the error envelope -> 500, the invalid envelope -> 400 as the body, the '{'-sniff -> 200, miss envelope -> 200) ── */

    [Theory]
    [InlineData("{\"cpu_percent\":42}")]
    [InlineData("  {\"cpu_percent\":42}")]                 // leading whitespace still sniffs as JSON
    [InlineData("[]")]
    [InlineData("[{\"a\":1}]")]
    [InlineData("{\"status\":\"empty\",\"message\":\"nothing\"}")] // the miss envelope passes through as 200
    [InlineData("{\"status\":\"precondition\",\"message\":\"Query Store is off\",\"hints\":{\"statement\":\"ALTER DATABASE\"}}")]
    [InlineData("{\"status_counts\":{\"error\":2}}")]      // a data key that merely begins with "status" is data
    public void ClassifyToolResponse_JsonPassesThrough(string result) =>
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.JsonPassthrough, DarlingWebEndpoints.ClassifyToolResponse(result));

    /// <summary>
    /// The tools' caught exception is the <c>{"status":"error", ...}</c> envelope since #3653 Q11 (a WIRE CHANGE
    /// for the 214 tools that answered with the bare sentence), and it is a 500 — tested against the REAL
    /// producer, not a hand-written literal, so a change to <c>McpHelpers.FormatError</c>'s serialization
    /// that the recognizer did not follow fails here rather than as every failure quietly becoming a 200.
    /// The PostgreSQL tools already answered with this envelope before the ruling, and the '{'-sniff was
    /// passing their failures through as 200 — the ordering this pins is the fix for that too.
    ///
    /// <para>The third case is the ruling #3719 asked for, made (#3739): a hand-built <c>Status("error", …)</c>
    /// around a refusal sentence is STILL read as a fault here — the classifier cannot know better — which is
    /// exactly why the tree may no longer build one: nine PostgreSQL <c>limit</c> / <c>family</c> /
    /// <c>min_severity</c> refusals wore this word and answered 500 for a typo between #3719 and #3739. They
    /// now return <c>McpHelpers.Refusal</c>'s <c>invalid</c> envelope (the next fact), and
    /// <c>McpPayloadContractCensusTests.TheFailureWord_HasOneProducer_OnBothSkus</c> holds that <c>FormatError</c>
    /// is the only thing that builds the failure word.</para>
    /// </summary>
    [Fact]
    public void ClassifyToolResponse_TheErrorEnvelope_IsServerError()
    {
        var wire = McpHelpers.FormatError("get_wait_stats", new InvalidOperationException("connection reset"));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ServerError, DarlingWebEndpoints.ClassifyToolResponse(wire));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ServerError, DarlingWebEndpoints.ClassifyToolResponse("  " + wire));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ServerError,
            DarlingWebEndpoints.ClassifyToolResponse(McpHelpers.Status("error", "Invalid limit value '0'. Must be a positive integer (1-1000).")));
    }

    /// <summary>
    /// The REFUSAL is the <c>{"status":"invalid", ...}</c> envelope and it is a 400 (#3739) — tested against the
    /// REAL producers, not a hand-written literal: <c>McpHelpers.Refusal</c> itself, every shared validator past
    /// its bound, both resolvers' miss, the web dispatch's own missing-parameter arm, and the write tools'
    /// <c>Outcome("invalid", …)</c> bytes, which the same recognizer must fire on so the read and write surfaces
    /// read one word by one rule. It is tested BEFORE the <c>{</c>-sniff (which would answer 200 over it — what
    /// the nine PostgreSQL refusals got before #3719) and is NOT the error envelope (which would answer 500 —
    /// what they got after it). Neither code was the 400 a client-correctable refusal deserves; this is it.
    /// </summary>
    [Fact]
    public void ClassifyToolResponse_TheInvalidEnvelope_IsARefusal()
    {
        var wire = McpHelpers.Refusal("limit", "Invalid limit value '0'. Must be a positive integer (1-1000).");
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(wire));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse("  " + wire));

        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(McpHelpers.ValidateHoursBack(9999)!));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(McpHelpers.ValidateTop(0)!));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(McpHelpers.ValidateWindow(4, "not-a-time", out _)!));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(McpHelpers.ParseSummaryDate("01/02/2026", out _)!));

        var (_, miss) = DarlingServerResolver.ResolveOrError(
            new[] { new DarlingServerResolver.RegisteredServer(1, "SQL2022", null) }, "no-such-server", DarlingPeerDirectory.Snapshot.Empty);
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(miss!));

        /* The write tools' own builder — same word, same bytes, same code. */
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal,
            DarlingWebEndpoints.ClassifyToolResponse("{\"status\":\"invalid\",\"message\":\"rule_id is required.\"}"));

        /* And the neighbours it must not fire on: a data key that begins with the word, a miss envelope. */
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.JsonPassthrough,
            DarlingWebEndpoints.ClassifyToolResponse("{\"status\":\"invalid_count\",\"message\":\"x\"}"));
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.JsonPassthrough,
            DarlingWebEndpoints.ClassifyToolResponse("{\"invalid\":true}"));
    }

    /// <summary>The pre-#3653 bare sentence is still a 500: an un-migrated producer must not fall through to
    /// the client-correctable 400 arm, which would tell a caller to fix a request that was fine.</summary>
    [Fact]
    public void ClassifyToolResponse_ErrorDuring_IsServerError() =>
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ServerError,
            DarlingWebEndpoints.ClassifyToolResponse("Error during get_wait_stats: connection reset"));

    /// <summary>The bare-string arm survives as the floor under everything the producers no longer emit: the
    /// four sentences here WERE the wire shape of these refusals until #3739 (they are the envelope's <c>message</c>
    /// now, and classify as <c>Refusal</c> above); what still reaches this arm is the <c>list_servers</c> miss on
    /// an empty registry. The resolver's registry-read fault used to reach it too; since #4283 it is a server error
    /// (the fact below). Kept at 400 so a producer nobody shaped is still refused rather than passed through as
    /// data.</summary>
    [Theory]
    [InlineData("Could not resolve server. Available servers:\nSQL2022")]
    [InlineData("Invalid hours_back value '9999'. Must be a positive integer (1-168).")]
    [InlineData("Missing required parameter 'wait_type'.")]
    [InlineData("baseline_hours_back must be greater than hours_back.")]
    [InlineData("No servers are registered yet. The service registers each monitored server on its first successful connection.")]
    [InlineData("")]
    public void ClassifyToolResponse_OtherBareStrings_AreClientErrors(string result) =>
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.ClientError, DarlingWebEndpoints.ClassifyToolResponse(result));

    /// <summary>#4283: the resolver's registry-read fault is a store fault, not a refusal, so it answers 500 with the
    /// fixed body rather than 400 with the sentence (which carries the store's own error text).</summary>
    [Fact]
    public void ClassifyToolResponse_TheResolversRegistryReadFault_IsAServerError() =>
        Assert.Equal(
            DarlingWebEndpoints.ToolResponseKind.ServerError,
            DarlingWebEndpoints.ClassifyToolResponse(DarlingServerResolver.RegistryReadFaultPrefix + "connection refused"));

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
    /// The integer twin (#3897), for <c>bucket_minutes</c>: absent means "let the read size the points", so an
    /// unreadable width falling back to that would chart a different width than the one asked for. A fraction is
    /// unreadable here, not rounded; zero and a negative bind, and the tool's range refusal answers them.
    /// </summary>
    [Theory]
    [InlineData(null, true, null)]
    [InlineData("15", true, 15)]
    [InlineData("0", true, 0)]
    [InlineData("-5", true, -5)]
    [InlineData("1.5", false, null)]
    [InlineData("abc", false, null)]
    [InlineData("99999999999", false, null)]
    public void TryParseOptionalInt_RefusesGarbageRatherThanChoosingAWidth(string? raw, bool expectedOk, int? expectedValue)
    {
        var ok = DarlingWebEndpoints.TryParseOptionalInt(raw, out var value);

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

    /* ── the mute-rule envelope → HTTP status mapping (#3450): the dedicated /api/mute-rules write routes pass
       the MCP verb's own {status, ...} envelope through verbatim and add ONLY the HTTP status, so a web client
       and an MCP client read one truth. This table is that addition, whole. ── */

    [Theory]
    [InlineData("{\"status\":\"created\",\"mute_rule\":{}}", 201, 201)]      // create's success rides the route's own code
    [InlineData("{\"status\":\"updated\",\"mute_rule\":{}}", 200, 200)]
    [InlineData("{\"status\":\"unchanged\",\"mute_rule\":{}}", 200, 200)]    // retry-safe "already so" shares the success code; the envelope carries the distinction
    [InlineData("{\"status\":\"deleted\",\"rule_id\":\"x\"}", 200, 200)]
    [InlineData("{\"status\":\"invalid\",\"message\":\"bad field\"}", 201, 400)]   // a refusal outranks whatever success the route hoped for
    [InlineData("{\"status\":\"invalid\",\"message\":\"Invalid limit value '0'.\",\"hints\":{\"parameter\":\"limit\"}}", 200, 400)] // McpHelpers.Refusal's bytes (#3739): the same word, the same 400, by the same rule as the read surface
    [InlineData("{\"status\":\"not_found\",\"message\":\"no rule\"}", 200, 404)]
    [InlineData("{ \"status\" : \"invalid\", \"message\": \"spaced\" }", 200, 400)]       // the parsed switch's belt-and-braces: an envelope serialized some other way still reads as invalid
    public void MuteRuleEnvelopeStatus_MapsTheVerbEnvelopeOntoHttp(string envelope, int successStatus, int expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.MuteRuleEnvelopeStatus(envelope, successStatus));

    /// <summary>The refusal a shared producer builds reaches the write surface's status mapping through the
    /// same classifier arm the read surface uses (#3739): one recognizer, one word, one code — executed
    /// against the real builder rather than a literal.</summary>
    [Fact]
    public void MuteRuleEnvelopeStatus_TheSharedRefusal_IsAClientError() =>
        Assert.Equal(400, DarlingWebEndpoints.MuteRuleEnvelopeStatus(McpHelpers.Refusal("rule_id", "rule_id is required."), 201));

    [Fact]
    public void MuteRuleEnvelopeStatus_TheCoresCaughtException_IsAServerError()
    {
        /* The cores swallow their own exceptions into McpHelpers.FormatError — the {"status":"error"} envelope
           since #3653 Q11 — the same shape the read surface maps to 500, classified by the same
           ClassifyToolResponse BEFORE the verb-status switch, so "error" is never read as a verb outcome and
           handed the route's success code. The bare sentence the cores produced before the ruling still
           maps the same way. */
        Assert.Equal(500, DarlingWebEndpoints.MuteRuleEnvelopeStatus(
            McpHelpers.FormatError("update_mute_rule", new InvalidOperationException("connection reset")), 200));
        Assert.Equal(500, DarlingWebEndpoints.MuteRuleEnvelopeStatus(
            McpHelpers.FormatError("create_mute_rule", new InvalidOperationException("connection reset")), 201));
        Assert.Equal(500, DarlingWebEndpoints.MuteRuleEnvelopeStatus("Error during update_mute_rule: connection reset", 200));
    }

    [Fact]
    public void MuteRuleEnvelopeStatus_ABareString_IsAClientError() =>
        /* Not a shape the cores produce; mapped like the read surface's client-correctable arm rather than
           claiming success over a body that is not an envelope. */
        Assert.Equal(400, DarlingWebEndpoints.MuteRuleEnvelopeStatus("rule_id is required.", 200));

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

    /// <summary>
    /// #4198: <c>get_analysis_findings</c> grew a default <c>limit</c> (18 chains) and a default text preview
    /// (<c>full_text: false</c>), sized for a chat caller watching its own token budget. The web viewer's two
    /// "Analysis Findings" tables never asked for that budget and render no field the preview cuts — a
    /// regression here would silently drop rows past the 18th from the table with no error, which a JSON-shape
    /// test cannot catch because the response is still well-formed, just short. No rig: this reads the
    /// dispatch-row SOURCE rather than invoking it, because invoking it needs a live Postgres connection.
    /// </summary>
    [Fact]
    public void GetAnalysisFindingsRow_PassesTheOldViewerDefaults_EveryChainFullText()
    {
        var source = ReadSource(WebEndpointsSourcePath);

        var line = source
            .Split('\n')
            .FirstOrDefault(l => l.Contains("[\"get_analysis_findings\"] = (c, pg, an) =>", StringComparison.Ordinal));

        Assert.True(line is not null,
            "#4198: the /api/read dispatch row for get_analysis_findings has moved or been renamed; update this pin's search text.");

        Assert.Contains("Rows(c, \"limit\", MaxRowLimit)", line, StringComparison.Ordinal);
        Assert.Contains("QueryBool(c, \"full_text\", true)", line, StringComparison.Ordinal);

        /* #4316 M2: GetAnalysisFindings grew a trailing ILogger? logger parameter so a web-path force-plan
           read failure logs instead of riding along unlabelled in the payload note. This row builds the
           service off the DI-resolved postgres/an directly, not through a logger-less helper, so dropping the
           argument here is the whole regression — there is no second call site downstream to catch it. */
        Assert.Contains("logger: logger", line, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4316 M2: <c>MapAll</c> used to build the web host's <see cref="DarlingAnalysisService"/> with no
    /// logger, so both fact collectors on the web path (audit_config, get_analysis_facts) logged nothing on
    /// failure and the collection-failure note's "the log has the full error" was false there. No rig: this
    /// reads the construction site's SOURCE rather than invoking it.
    /// </summary>
    [Fact]
    public void MapAll_BuildsTheAnalysisServiceWithTheHostsLogger()
    {
        var source = ReadSource(WebEndpointsSourcePath);

        var line = source
            .Split('\n')
            .FirstOrDefault(l => l.Contains("new DarlingAnalysisService(", StringComparison.Ordinal));

        Assert.True(line is not null,
            "#4316: the web host's DarlingAnalysisService construction has moved or been renamed; update this pin's search text.");

        Assert.Contains("logger: logger", line, StringComparison.Ordinal);
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#4198 scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private const string WebEndpointsSourcePath =
        "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
