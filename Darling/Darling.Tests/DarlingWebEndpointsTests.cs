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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web dashboard's read surface (#1562). The parity pin is the load-bearing one: it reflects the WHOLE
/// <c>[McpServerTool]</c> catalog off the service assembly and asserts the <c>/api/read/*</c> dispatch table
/// equals that catalog MINUS exactly the documented exclusions — so a tool added later cannot be silently
/// missed from the web surface (it fails this test until it is wired or explicitly excluded). The response-kind
/// and query-parse pins cover the thin glue around the reused tool methods.
/// </summary>
[Trait("Stage", "Guard")]
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
    public void EveryReadTool_HasAReadEndpoint_UnlessDocumentedAsExcluded()
    {
        var catalog = ReflectToolCatalog();
        var endpoints = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);

        var expected = catalog.Except(DarlingWebEndpoints.ExcludedToolNames).ToHashSet(StringComparer.Ordinal);

        /* The web page's own keyed reads (#5241) have no tool behind them, by design: exactly this set is added. */
        expected.UnionWith(DarlingWebEndpoints.WebOnlyReadNames);

        /* One direction (#5950): a read-only tool with no /api/read endpoint fails by name. An endpoint whose
           tool has since been renamed or removed is not asserted here. */
        var missing = expected.Except(endpoints).OrderBy(n => n, StringComparer.Ordinal).ToArray();

        Assert.True(missing.Length == 0, "read-only tools with no /api/read endpoint: " + string.Join(", ", missing));
    }

    /// <summary>#4782: the test-only extra dispatch entry belongs to the async flow that set it. It is not a
    /// process-wide switch: <c>ReadLatencyWebRecordingTests</c> sets one route while other test classes, outside
    /// its collection, run at the same time and call <see cref="DarlingWebEndpoints.BuildReadDispatch"/>. With a
    /// plain static, any of them could see that route, and <c>DarlingCustomViewsTests</c> failed on it (it
    /// compares the dispatch keys with the Custom Views catalog). The same flow still sees the route (the
    /// recording test builds its server there), and a flow that does not inherit the caller's context does not.</summary>
    [Fact]
    public async Task TheTestOnlyExtraDispatchEntry_IsSeenBySameFlow_AndNotByAFlowThatDoesNotInheritIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var routeName = "__test_flow_local_" + Guid.NewGuid().ToString("N");

        try
        {
            DarlingWebEndpoints.TestOnlyExtraDispatchEntry = (routeName, (_, _, _) => Task.FromResult("{}"));

            Assert.True(
                DarlingWebEndpoints.BuildReadDispatch().ContainsKey(routeName),
                "the flow that set the entry must see it in its own BuildReadDispatch (the recording test builds its server there)");

            var seenByOtherFlow = await ReadDispatchContainsInAFlowThatDoesNotInherit(routeName, ct);
            Assert.False(
                seenByOtherFlow,
                "a flow that does not inherit the setter's context must not see the entry: a test class running at the same time would otherwise get an extra read route");
        }
        finally
        {
            DarlingWebEndpoints.TestOnlyExtraDispatchEntry = null;
        }

        Assert.False(DarlingWebEndpoints.BuildReadDispatch().ContainsKey(routeName), "the entry is cleared once the fact is done");
    }

    /// <summary>Starts the dispatch build on a task whose <see cref="ExecutionContext"/> is NOT the caller's --
    /// the same position a test class running beside another one is in. Kept in its own synchronous method so
    /// the <see cref="ExecutionContext.SuppressFlow"/> scope opens and closes on one thread, which the API
    /// requires (an <c>await</c> inside the <c>using</c> would break that).</summary>
    private static Task<bool> ReadDispatchContainsInAFlowThatDoesNotInherit(string routeName, CancellationToken ct)
    {
        using (ExecutionContext.SuppressFlow())
        {
            return Task.Run(() => DarlingWebEndpoints.BuildReadDispatch().ContainsKey(routeName), ct);
        }
    }

    [Fact]
    public void WebOnlyReads_AreNotMcpTools_AndAreServedAndCataloged()
    {
        var catalog = ReflectToolCatalog();
        foreach (var name in DarlingWebEndpoints.WebOnlyReadNames)
        {
            Assert.DoesNotContain(name, catalog);
            Assert.Contains(name, DarlingWebEndpoints.BuildReadDispatch().Keys);
            Assert.Contains(name, DarlingWebEndpoints.CatalogDescriptors.Keys);
        }
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

    /// <summary>
    /// #5235: the Active Queries page sends <c>?wait_type=</c> to <c>/api/read/get_active_queries</c>, so the web read
    /// has to declare it and pass it on. The catalog row lists it as optional text between blocking_only and limit (the
    /// tool's own order, so the catalog picker and the MCP signature agree), and the dispatch row hands the raw query
    /// value to the tool, which trims it and treats a blank as none. Without the catalog row the picker never offers
    /// the parameter; without the dispatch argument the page's filter is silently ignored and shows every wait.
    /// </summary>
    [Fact]
    public void ReadEndpoints_ActiveQueries_DeclaresWaitType()
    {
        var read = DarlingWebEndpoints.CatalogDescriptors["get_active_queries"];
        Assert.Equal(
            new[] { "server", "hours", "database_name", "blocking_only", "wait_type", "limit", "as_of", "tag", "collection_time", "snapshots_only" },
            read.Params.Select(p => p.Name).ToArray());
        var waitType = Assert.Single(read.Params, p => p.Name == "wait_type");
        Assert.Equal("text", waitType.Type);
        Assert.False(waitType.Required);
        Assert.Null(waitType.Default);

        var source = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Contains("Str(c, \"wait_type\"), Rows(c, \"limit\", 50), 2000, AsOf(c)", source, StringComparison.Ordinal);
    }

    /* ── response-kind mapping (the error envelope -> 500, the invalid envelope -> 400 as the body, the '{'-sniff -> 200, miss envelope -> 200) ── */

    /// <summary>A stored plan arrives from the tool as bare XML; the web route answers it as 200 JSON (the
    /// classifier would otherwise file a leading "&lt;" as a 400), with the truncation flag decided from the tool's
    /// own cut marker. Envelopes pass through unchanged.</summary>
    [Fact]
    public void WrapPlanXml_AWholePlan_IsJsonThatClassifiesAsPassthrough()
    {
        const string xml = "<ShowPlanXML><x a=\"1\"/></ShowPlanXML>";
        var wrapped = DarlingWebEndpoints.WrapPlanXml(xml, "0xABC", "db1");
        Assert.Equal(DarlingWebEndpoints.ToolResponseKind.JsonPassthrough, DarlingWebEndpoints.ClassifyToolResponse(wrapped));
        using var doc = System.Text.Json.JsonDocument.Parse(wrapped);
        Assert.Equal(xml, doc.RootElement.GetProperty("plan_xml").GetString());
        Assert.Equal("0xABC", doc.RootElement.GetProperty("query_hash").GetString());
        Assert.Equal("db1", doc.RootElement.GetProperty("database_name").GetString());
        Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public void WrapPlanXml_ACutPlan_IsFlaggedTruncated_FromTheRealTruncateMarker()
    {
        var cut = McpHelpers.Truncate("<ShowPlanXML>" + new string('x', 100) + "</ShowPlanXML>", 20)!;
        using var doc = System.Text.Json.JsonDocument.Parse(DarlingWebEndpoints.WrapPlanXml(cut, "H", null));
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
        Assert.DoesNotContain("(truncated)", doc.RootElement.GetProperty("plan_xml").GetString());
        Assert.Equal(System.Text.Json.JsonValueKind.Null, doc.RootElement.GetProperty("database_name").ValueKind);
    }

    [Theory]
    [InlineData("{\"status\":\"unavailable\",\"message\":\"No stored plan found.\"}")]
    [InlineData("{\"status\":\"not_collected\",\"message\":\"Not collected.\"}")]
    public void WrapPlanXml_AnEnvelope_PassesThroughUnchanged(string envelope) =>
        Assert.Equal(envelope, DarlingWebEndpoints.WrapPlanXml(envelope, "H", null));

    /// <summary>#5228: the identity overload echoes the caller's key in the order given, then plan_xml and
    /// truncated; the original (query_hash, database_name) overload is that same wrap with those two keys.</summary>
    [Fact]
    public void WrapPlanXml_TheIdentityOverload_EchoesTheKeyInOrder_AndMatchesTheHashOverload()
    {
        const string xml = "<ShowPlanXML><x a=\"1\"/></ShowPlanXML>";
        var identity = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["collection_time"] = "2026-03-04T05:06:07.1234560",
            ["session_id"] = 51,
            ["request_id"] = 0,
            ["live"] = false,
        };
        using var doc = System.Text.Json.JsonDocument.Parse(DarlingWebEndpoints.WrapPlanXml(xml, identity));
        var names = doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Equal(new[] { "collection_time", "session_id", "request_id", "live", "plan_xml", "truncated" }, names);
        Assert.Equal(51, doc.RootElement.GetProperty("session_id").GetInt32());
        Assert.Equal(xml, doc.RootElement.GetProperty("plan_xml").GetString());

        var byHash = DarlingWebEndpoints.WrapPlanXml(xml, "0xABC", "db1");
        var byIdentity = DarlingWebEndpoints.WrapPlanXml(xml, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["query_hash"] = "0xABC",
            ["database_name"] = "db1",
        });
        Assert.Equal(byHash, byIdentity);
    }

    /// <summary>#5228: the three row plan reads refuse a missing or unparseable key BEFORE touching the store
    /// (the store argument is never used on these paths), with the dispatch's own <c>invalid</c> envelope.</summary>
    [Theory]
    [InlineData("get_active_query_plan_xml", "session_id=1", "collection_time")]
    [InlineData("get_active_query_plan_xml", "collection_time=not-a-time&session_id=1", "collection_time")]
    [InlineData("get_active_query_plan_xml", "collection_time=2026-03-04T05:06:07.1234560Z", "session_id")]
    [InlineData("get_active_query_plan_xml", "collection_time=2026-03-04T05:06:07.1234560Z&session_id=abc", "session_id")]
    [InlineData("get_active_query_plan_xml", "collection_time=2026-03-04T05:06:07.1234560Z&session_id=1&request_id=x", "request_id")]
    [InlineData("get_query_store_plan_xml", "query_id=1", "database_name")]
    [InlineData("get_query_store_plan_xml", "database_name=db1", "query_id")]
    [InlineData("get_query_store_plan_xml", "database_name=db1&query_id=abc", "query_id")]
    [InlineData("get_query_store_plan_xml", "database_name=db1&query_id=1&plan_id=abc", "plan_id")]
    [InlineData("get_query_store_query_history", "query_id=1", "database_name")]
    [InlineData("get_query_store_query_history", "database_name=db1", "query_id")]
    [InlineData("get_query_store_query_history", "database_name=db1&query_id=abc", "query_id")]
    [InlineData("get_active_query_plan_xml", "collection_time=2026-03-04&session_id=1", "collection_time")]
    [InlineData("get_active_query_plan_xml", "collection_time=3/4/2026&session_id=1", "collection_time")]
    [InlineData("get_active_query_plan_xml", "collection_time=2026-03-04T05:06:07.1234560Z&session_id=1&live=yes", "live")]
    [InlineData("get_procedure_plan_xml", "sql_handle=abc", "sql_handle")]
    [InlineData("get_procedure_plan_xml", "sql_handle=0x", "sql_handle")]
    [InlineData("get_procedure_plan_xml", "sql_handle=0x03%27%3BDROP", "sql_handle")]
    [InlineData("get_procedure_plan_xml", "", "sql_handle")]
    [InlineData("get_query_repro_script", "", "kind")]
    [InlineData("get_query_repro_script", "kind=procedure", "kind")]
    [InlineData("get_query_repro_script", "kind=bogus", "kind")]
    [InlineData("get_query_repro_script", "kind=query_hash", "query_hash")]
    [InlineData("get_query_repro_script", "kind=query_store&query_id=1", "database_name")]
    [InlineData("get_query_repro_script", "kind=query_store&database_name=d", "query_id")]
    [InlineData("get_query_repro_script", "kind=query_store&database_name=d&query_id=x", "query_id")]
    [InlineData("get_query_repro_script", "kind=query_store&database_name=d&query_id=1&plan_id=x", "plan_id")]
    [InlineData("get_query_repro_script", "kind=active_snapshot&session_id=1", "collection_time")]
    [InlineData("get_query_repro_script", "kind=active_snapshot&collection_time=2026-03-04T05:06:07.1234560Z", "session_id")]
    [InlineData("get_query_repro_script", "kind=active_snapshot&collection_time=bad&session_id=1", "collection_time")]
    [InlineData("get_query_repro_script", "kind=active_snapshot&collection_time=2026-03-04T05:06:07.1234560Z&session_id=1&request_id=x", "request_id")]
    [InlineData("get_blocking_plan_xml", "blocked_spid=1&blocking_spid=2", "event_time")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04T05:06:07.1234560Z&blocking_spid=2", "blocked_spid")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04T05:06:07.1234560Z&blocked_spid=1", "blocking_spid")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04T05:06:07.1234560Z&blocked_spid=abc&blocking_spid=2", "blocked_spid")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04T05:06:07.1234560Z&blocked_spid=1&blocking_spid=2&blocked_ecid=x", "blocked_ecid")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04T05:06:07.1234560Z&blocked_spid=1&blocking_spid=2&blocking_ecid=x", "blocking_ecid")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04&blocked_spid=1&blocking_spid=2", "event_time")]
    [InlineData("get_blocking_plan_xml", "event_time=not-a-time&blocked_spid=1&blocking_spid=2", "event_time")]
    [InlineData("get_blocking_plan_xml", "event_time=2026-03-04T05:06:07.1234560Z&blocked_spid=1&blocking_spid=2&side=sideways", "side")]
    [InlineData("get_deadlock_plan_xml", "deadlock_time=2026-03-04T05:06:07.1234560Z", "collection_time")]
    [InlineData("get_deadlock_plan_xml", "collection_time=2026-03-04T05:06:07.1234560Z", "deadlock_time")]
    [InlineData("get_deadlock_plan_xml", "collection_time=2026-03-04&deadlock_time=2026-03-04T05:06:07.1234560Z", "collection_time")]
    [InlineData("get_deadlock_plan_xml", "collection_time=2026-03-04T05:06:07.1234560Z&deadlock_time=3/4/2026", "deadlock_time")]
    public async Task TheRowPlanReads_RefuseAMissingOrUnparseableKey_BeforeTheStore(string tool, string query, string key)
    {
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
        context.Request.QueryString = new Microsoft.AspNetCore.Http.QueryString("?" + query);
        var result = await DarlingWebEndpoints.BuildReadDispatch()[tool](context, null!, null!);
        using var doc = System.Text.Json.JsonDocument.Parse(result);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains(key, result, StringComparison.Ordinal);
    }

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
            new[] { new RegisteredServer(1, "SQL2022", null) }, "no-such-server", DarlingPeerDirectory.Snapshot.Empty);
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
    /// now, and classify as <c>Refusal</c> above); <c>list_servers</c> on an empty registry used to reach it and now answers
    /// the <c>empty</c> envelope (#6277). The resolver's registry-read fault used to reach it too; since #4283 it is a server error
    /// (the fact below). Kept at 400 so a producer nobody shaped is still refused rather than passed through as
    /// data.</summary>
    [Theory]
    [InlineData("Could not resolve server. Available servers:\nSQL2022")]
    [InlineData("Invalid hours_back value '9999'. Must be a positive integer (1-168).")]
    [InlineData("Missing required parameter 'wait_type'.")]
    [InlineData("baseline_hours_back must be greater than hours_back.")]
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

    /// <summary>
    /// #5644 W22: a present, non-integer integer knob is refused (the 400 envelope), not read as the default, so
    /// <c>?hours=abc</c> and <c>?hours=1.5</c> no longer answer 200 for a different window than the one asked for.
    /// Absent and empty knobs still take the default; a whole number (even out of range) is the tool's to judge.
    /// </summary>
    [Theory]
    [InlineData("hours", "abc")]
    [InlineData("hours", "1.5")]
    [InlineData("hours", "1e2")]
    [InlineData("hours_back", "24h")]
    [InlineData("limit", "ten")]
    [InlineData("top", "5.0")]
    [InlineData("days_back", "x")]
    [InlineData("bucket_minutes", "five")]
    [InlineData("worst_count", "-")]
    public void NonIntegerParamRefusal_RefusesAPresentNonInteger(string key, string raw)
    {
        var refusal = DarlingWebEndpoints.NonIntegerParamRefusal(k => k == key ? raw : null);

        Assert.NotNull(refusal);
        Assert.True(McpHelpers.IsRefusalEnvelope(refusal));
        var envelope = JsonNode.Parse(refusal)!;
        Assert.Equal("invalid", (string?)envelope["status"]);
        Assert.Equal(key, (string?)envelope["hints"]!["parameter"]);
        Assert.Equal($"Invalid {key} value '{raw}'. Must be a whole number.", (string?)envelope["message"]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("24")]
    [InlineData("-5")]      // readable: the tool owns the range refusal ("Must be a positive integer")
    [InlineData("500")]
    [InlineData("+7")]
    public void NonIntegerParamRefusal_LeavesAbsentEmptyAndWholeNumbersToTheTool(string? raw)
    {
        foreach (var key in DarlingWebEndpoints.IntegerQueryKeys)
        {
            Assert.Null(DarlingWebEndpoints.NonIntegerParamRefusal(k => k == key ? raw : null));
        }
    }

    /* #5644 W22: the tools refuse an out-of-range window as hours_back; a web caller that sent hours is told about hours. */
    private static string HoursBackRefusal(int value) =>
        McpHelpers.ValidateHoursBack(value)!;

    [Theory]
    [InlineData(-5)]
    [InlineData(500)]
    public void WindowRefusal_NamesTheHoursParameterTheWebCallerSent(int value)
    {
        var refusal = HoursBackRefusal(value);
        Assert.Contains("hours_back", refusal);

        var named = DarlingWebEndpoints.NameWindowParameterTheCallerSent(refusal, k => k == "hours" ? value.ToString() : null);

        var doc = System.Text.Json.JsonDocument.Parse(named).RootElement;
        Assert.Equal("invalid", doc.GetProperty("status").GetString());
        Assert.Equal("hours", doc.GetProperty("hints").GetProperty("parameter").GetString());
        var message = doc.GetProperty("message").GetString()!;
        Assert.DoesNotContain("hours_back", message);
        Assert.Contains($"hours value '{value}'", message);
    }

    [Fact]
    public void WindowRefusal_ForAnMcpStyleRequest_OrAnotherParameter_IsLeftAlone()
    {
        var refusal = HoursBackRefusal(-5);

        /* The caller sent hours_back (alone or beside hours): the tool's own wording is right. */
        Assert.Equal(refusal, DarlingWebEndpoints.NameWindowParameterTheCallerSent(refusal, k => k == "hours_back" ? "-5" : null));
        Assert.Equal(refusal, DarlingWebEndpoints.NameWindowParameterTheCallerSent(refusal, k => k is "hours" or "hours_back" ? "-5" : null));
        /* No window parameter sent, and a result that is data rather than a refusal. */
        Assert.Equal(refusal, DarlingWebEndpoints.NameWindowParameterTheCallerSent(refusal, _ => null));
        const string Data = "{\"status\":\"ok\",\"hints\":{\"parameter\":\"hours_back\"}}";
        Assert.Equal(Data, DarlingWebEndpoints.NameWindowParameterTheCallerSent(Data, k => k == "hours" ? "1" : null));
        /* A refusal for a different parameter keeps its wording, even when it mentions hours_back. */
        var other = McpHelpers.Refusal("baseline_hours_back", "baseline_hours_back must be greater than hours_back.");
        Assert.Equal(other, DarlingWebEndpoints.NameWindowParameterTheCallerSent(other, k => k == "hours" ? "4" : null));
    }

    [Fact]
    public void NonIntegerParamRefusal_ThroughTheQueryString_ReadsTheFirstNonEmptyValue()
    {
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
        context.Request.QueryString = new Microsoft.AspNetCore.Http.QueryString("?server_name=SQL1&hours=abc");
        Assert.NotNull(DarlingWebEndpoints.NonIntegerParamRefusal(k => DarlingWebEndpoints.First(context, k)));

        context.Request.QueryString = new Microsoft.AspNetCore.Http.QueryString("?hours=&hours=12");
        Assert.Null(DarlingWebEndpoints.NonIntegerParamRefusal(k => DarlingWebEndpoints.First(context, k)));
    }

    /// <summary>
    /// #5644 W22 source pin: the /api/read dispatch loop and /api/fleet both call the check BEFORE any tool or reader
    /// runs, and every key the dispatch reads through QueryInt/Rows/Hours is in the list.
    /// </summary>
    [Fact]
    public void NonIntegerParamRefusal_RunsBeforeEveryReadAndCoversEveryIntegerKnob()
    {
        var source = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs");

        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(source, @"NonIntegerParamRefusal\(context\) is \{ \}").Count);
        Assert.True(source.IndexOf("NonIntegerParamRefusal(context) is { } nonIntegerRefusal", StringComparison.Ordinal)
            < source.IndexOf("result = await handler(context, postgres, analysis);", StringComparison.Ordinal));

        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(
            source, @"(?:QueryInt\((?:c|context), ""|Rows\(c, "")([a-z_]+)"""))
        {
            keys.Add(m.Groups[1].Value);
        }

        keys.Add("hours");
        keys.Add("hours_back");
        Assert.NotEmpty(keys);
        Assert.All(keys, k => Assert.Contains(k, DarlingWebEndpoints.IntegerQueryKeys));
    }

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
    [InlineData("{\"status\":\"already_exists\",\"rule_id\":\"x\",\"mute_rule\":{}}", 201, 409)]   // #4734: a create that repeats a rule in force is a conflict, and the body keeps the envelope with the existing id
    [InlineData("{ \"status\" : \"invalid\", \"message\": \"spaced\" }", 200, 400)]       // the parsed switch's belt-and-braces: an envelope serialized some other way still reads as invalid
    public void MuteRuleEnvelopeStatus_MapsTheVerbEnvelopeOntoHttp(string envelope, int successStatus, int expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.MuteRuleEnvelopeStatus(envelope, successStatus));

    /* ── the server-tag envelope → HTTP status mapping (#5085): the mute-rule mapping plus conflict and
       confirm_required as 409, the body untouched. ── */

    [Theory]
    [InlineData("{\"status\":\"created\",\"tag\":{}}", 201, 201)]
    [InlineData("{\"status\":\"updated\",\"tag\":{}}", 201, 201)]
    [InlineData("{\"status\":\"updated\",\"tag\":{}}", 200, 200)]
    [InlineData("{\"status\":\"unchanged\",\"tag\":{}}", 200, 200)]
    [InlineData("{\"status\":\"deleted\",\"tag_id\":7}", 200, 200)]
    [InlineData("{\"status\":\"assigned\",\"tag_id\":7}", 200, 200)]
    [InlineData("{\"status\":\"unassigned\",\"tag_id\":7}", 200, 200)]
    [InlineData("{\"status\":\"some_future_status\"}", 200, 500)]   // N2: an unmapped status is a 500, never a silent 2xx
    [InlineData("{\"status\":\"some_future_status\"}", 201, 500)]
    [InlineData("{\"status\":\"invalid\",\"refusal\":\"bad_name\",\"message\":\"x\"}", 201, 400)]
    [InlineData("{\"status\":\"not_found\",\"message\":\"x\"}", 200, 404)]
    [InlineData("{\"status\":\"conflict\",\"refusal\":\"duplicate_name\",\"message\":\"x\"}", 201, 409)]
    [InlineData("{\"status\":\"confirm_required\",\"refusal\":\"rules_affected\",\"affected_rules\":[{\"rule_id\":1}]}", 200, 409)]
    public void ServerTagEnvelopeStatus_MapsTheVerbEnvelopeOntoHttp(string envelope, int successStatus, int expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.ServerTagEnvelopeStatus(envelope, successStatus));

    [Fact]
    public void ServerTagEnvelopeStatus_TheCoresCaughtException_IsAServerError() =>
        Assert.Equal(500, DarlingWebEndpoints.ServerTagEnvelopeStatus(
            McpHelpers.FormatError("create_server_tag", new InvalidOperationException("connection reset")), 201));

    [Fact]
    public async Task ServerTagToolResult_ConfirmRequired_KeepsTheEnvelopeAsTheBodyUnder409()
    {
        var envelope = "{\"status\":\"confirm_required\",\"refusal\":\"rules_affected\",\"affected_rules\":[{\"rule_id\":1}]}";
        var result = DarlingWebEndpoints.ServerTagToolResult(envelope, "/api/server-tags/{id}", Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, 5, System.Threading.CancellationToken.None);
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext { RequestServices = new Microsoft.Extensions.DependencyInjection.ServiceCollection().AddLogging().BuildServiceProvider() };
        context.Response.Body = new System.IO.MemoryStream();
        await result.ExecuteAsync(context);
        Assert.Equal(409, context.Response.StatusCode);
        context.Response.Body.Position = 0;
        Assert.Contains("affected_rules", await new System.IO.StreamReader(context.Response.Body).ReadToEndAsync(), StringComparison.Ordinal);
    }

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
    /// #5644 (review of W22): /api/fleet refuses an out-of-range window or a negative worst_count with the
    /// {"status":"invalid"} envelope, naming the spelling the caller sent, and still answers the in-range requests
    /// (the bare route, hours=24, the edges 1 and 168, worst_count=0) with no refusal. It used to clamp
    /// (hours=500 answered 200 over 168 hours, worst_count=-3 over zero).
    /// </summary>
    [Theory]
    [InlineData("hours", 500, "hours")]
    [InlineData("hours", 0, "hours")]
    [InlineData("hours", -5, "hours")]
    [InlineData("hours", 169, "hours")]
    [InlineData("hours_back", 500, "hours_back")]
    [InlineData("hours_back", 0, "hours_back")]
    public void FleetParamRefusal_RefusesAnOutOfRangeWindow_NamingTheParameterSent(string key, int value, string expectedParameter)
    {
        var refusal = DarlingWebEndpoints.FleetParamRefusal(value, 3, k => k == key ? value.ToString() : null);

        Assert.NotNull(refusal);
        Assert.True(McpHelpers.IsRefusalEnvelope(refusal));
        var envelope = JsonNode.Parse(refusal)!;
        Assert.Equal("invalid", (string?)envelope["status"]);
        Assert.Equal(expectedParameter, (string?)envelope["hints"]!["parameter"]);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-3)]
    public void FleetParamRefusal_RefusesANegativeWorstCount(int worstCount)
    {
        var refusal = DarlingWebEndpoints.FleetParamRefusal(24, worstCount, k => k == "worst_count" ? worstCount.ToString() : null);

        Assert.NotNull(refusal);
        var envelope = JsonNode.Parse(refusal)!;
        Assert.Equal("invalid", (string?)envelope["status"]);
        Assert.Equal("worst_count", (string?)envelope["hints"]!["parameter"]);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 5)]
    [InlineData(24, 5)]
    [InlineData(168, 5)]
    public void FleetParamRefusal_AnswersInRangeRequests(int hours, int worstCount)
    {
        Assert.Null(DarlingWebEndpoints.FleetParamRefusal(hours, worstCount, k => k == "hours" ? hours.ToString() : null));
    }

    /// <summary>The /api/fleet route runs that refusal before it reads, and no longer clamps the window or the count.</summary>
    [Fact]
    public void FleetRoute_RefusesBeforeReading_AndDoesNotClamp()
    {
        var source = ReadSource(WebEndpointsSourcePath);
        var start = source.IndexOf("app.MapGet(\"/api/fleet\"", StringComparison.Ordinal);
        Assert.True(start >= 0, "the /api/fleet route has moved; update this pin.");
        var end = source.IndexOf("app.MapGet(", start + 10, StringComparison.Ordinal);
        var route = source[start..end];

        Assert.Contains("FleetParamRefusal(", route, StringComparison.Ordinal);
        Assert.DoesNotContain("Math.Clamp", route, StringComparison.Ordinal);
        Assert.DoesNotContain("Math.Max", route, StringComparison.Ordinal);
        Assert.True(
            route.IndexOf("FleetParamRefusal(", StringComparison.Ordinal) < route.IndexOf("GetFleetOverviewAsync", StringComparison.Ordinal),
            "the refusal must come before the read");
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#4198 scan target not found: {path}");

        return RepoFile.ReadRepoFile(relative);
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

    /* ---- #5245: a single-value query key sent more than once is refused, never read as the joined "a,b" ---- */

    private static DefaultHttpContext Ask(string query)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        return context;
    }

    [Theory]
    [InlineData("", "", true)]
    [InlineData("?confirm=true", "true", true)]
    [InlineData("?confirm=", "", true)]
    [InlineData("?confirm=true&confirm=true", "", false)]
    [InlineData("?confirm=&confirm=true", "", false)]
    public void TrySingleQueryValue_ReturnsTheOneValue_AndRefusesARepeatedKey(string query, string expected, bool accepted)
    {
        var context = Ask(query);

        Assert.Equal(accepted, DarlingWebEndpoints.TrySingleQueryValue(context.Request.Query, "confirm", out var value));
        Assert.Equal(expected, value);
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("?confirm=true", true)]
    public void ParseConfirm_SingleValue_BehavesAsBefore(string query, bool expectedConfirm)
    {
        var (confirm, refusal) = DarlingWebEndpoints.ParseConfirm(Ask(query).Request.Query);

        Assert.Equal(expectedConfirm, confirm);
        Assert.Null(refusal);
    }

    [Theory]
    [InlineData("?confirm=1", "confirm must be true, or omitted.")]
    [InlineData("?confirm=TRUE", "confirm must be true, or omitted.")]
    [InlineData("?confirm=true&confirm=true", "confirm must be given once, as true, or omitted.")]
    [InlineData("?confirm=true&confirm=1", "confirm must be given once, as true, or omitted.")]
    public void ParseConfirm_RefusesAWrongValue_AndARepeatedKey(string query, string expectedRefusal)
    {
        var (confirm, refusal) = DarlingWebEndpoints.ParseConfirm(Ask(query).Request.Query);

        Assert.False(confirm);
        Assert.Equal(expectedRefusal, refusal);
    }
}
