/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Ungated (no-live-store) contract for the #1599 Custom Views MCP tools: the tool surface is EXACTLY the eight
/// names (seven management tools + describe_custom_view_catalog; all static, on a [McpServerToolType] class,
/// returning Task&lt;string&gt;), the advertised tools/list schema is Gemini-clean (#1074) with the expected
/// required-param set, and validate_custom_view /
/// create_custom_view / update_custom_view run the SAME DarlingWebEndpoints.ValidateDefinition authority BEFORE
/// any persistence — an invalid definition never reaches the store. The live CRUD + run round-trip is gated below.
/// </summary>
public sealed class DarlingMcpCustomViewToolsSurfaceTests
{
    /// <summary>A dead data source (unroutable port) — proves the validate-before-persist tools bail on a bad
    /// definition WITHOUT ever opening a connection (the call returns before touching the store).</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string GoodDashboard =
        "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}";

    private const string GoodNotebook =
        "{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\",\"text\":\"# notes\"}," +
        "{\"type\":\"panel\",\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}";

    private const string BadDefinition =
        "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}]}";

    private static readonly string[] ExpectedToolSurface =
    {
        "create_custom_view",
        "delete_custom_view",
        "describe_custom_view_catalog",
        "get_custom_view",
        "list_custom_views",
        "run_custom_view_panel",
        "update_custom_view",
        "validate_custom_view",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpCustomViewTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IsExactlyTheEightCustomViewTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ExpectedToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpCustomViewTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static System.Collections.Generic.Dictionary<string, ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpCustomViewTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllEightTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(8, tools.Count);
        var violations = tools.Values.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
    }

    [Theory]
    [InlineData("describe_custom_view_catalog", "")]
    [InlineData("list_custom_views", "")]
    [InlineData("get_custom_view", "view_id")]
    [InlineData("validate_custom_view", "definition")]
    [InlineData("create_custom_view", "name,definition")]
    [InlineData("update_custom_view", "view_id,name,definition,version")]
    [InlineData("delete_custom_view", "view_id")]
    [InlineData("run_custom_view_panel", "spec")]
    public void AdvertisedSchema_RequiredParams_MatchTheContract(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Length == 0 ? Array.Empty<string>() : expectedCsv.Split(',');
        var required = DarlingMcpSchemaAssert.RequiredOf(BuildToolSchemas()[toolName].InputSchema)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    [Fact]
    public async Task ValidateCustomView_ValidDashboard_ReturnsValidTrue()
    {
        var result = await DarlingMcpCustomViewTools.ValidateCustomView(GoodDashboard);
        using var doc = JsonDocument.Parse(result);
        Assert.True(doc.RootElement.GetProperty("valid").GetBoolean(), result);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("error").ValueKind);
    }

    [Fact]
    public async Task ValidateCustomView_ValidNotebook_ReturnsValidTrue()
    {
        var result = await DarlingMcpCustomViewTools.ValidateCustomView(GoodNotebook);
        using var doc = JsonDocument.Parse(result);
        Assert.True(doc.RootElement.GetProperty("valid").GetBoolean(), result);
    }

    [Fact]
    public async Task ValidateCustomView_BadMeasure_ReturnsValidFalse_WithError()
    {
        var result = await DarlingMcpCustomViewTools.ValidateCustomView(BadDefinition);
        using var doc = JsonDocument.Parse(result);
        Assert.False(doc.RootElement.GetProperty("valid").GetBoolean(), result);
        Assert.Contains("unknown measure", doc.RootElement.GetProperty("error").GetString()!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DescribeCustomViewCatalog_DefaultIsCompact_GroupedBySource()
    {
        /* The catalog tool exists so an LLM composes a valid panel WITHOUT reading source or guessing names — it
           must surface every vocabulary a panel draws from, plus a known measure with its composable fields.
           #4198: the full catalog is 98 KB, so the DEFAULT call groups measures by source and keeps only the
           fields a panel spec actually names (key/displayName/kind/unitFamily/validAggregates); source=/
           full_detail= reach the rest. Byte-budget coverage of this default lives in
           DarlingMcpCustomViewCatalogSizeTests (#4198 exempt from McpReadToolBudgetLiveTests — no server/store
           argument to seed). */
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog();
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        foreach (var section in new[] { "sources", "annotationSources", "unitFamilies", "aggregates", "timeBuckets", "filterOps", "viz" })
        {
            Assert.True(root.TryGetProperty(section, out _), $"compact catalog is missing the '{section}' vocabulary");
        }

        Assert.True(root.GetProperty("compact").GetBoolean());

        /* A known measure is discoverable, grouped under its source, with the fields a panel binds from it. */
        var waitStats = root.GetProperty("sources").EnumerateArray().Single(s => s.GetProperty("source").GetString() == "wait_stats");
        var waitTime = waitStats.GetProperty("measures").EnumerateArray().Single(m => m.GetProperty("key").GetString() == "wait_time_ms");
        Assert.Equal("scalar", waitTime.GetProperty("kind").GetString());
        Assert.Contains("sum", waitTime.GetProperty("validAggregates").EnumerateArray().Select(a => a.GetString()));

        /* The scalar vocabularies the panel's aggregate + viz fields draw from. */
        Assert.Contains("sum", root.GetProperty("aggregates").EnumerateArray().Select(a => a.GetString()));
        Assert.Contains("bar", root.GetProperty("viz").EnumerateArray().Select(v => v.GetString()));
    }

    [Fact]
    public async Task DescribeCustomViewCatalog_FullDetail_ReturnsTodaysOriginalShape()
    {
        /* full_detail=true is the #4198 escape hatch: the exact flat shape (and every field) this tool always
           returned, byte-for-byte what BuildComposeCatalogNode / the web /api/catalog compose section serve. */
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(full_detail: true);
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        foreach (var section in new[] { "measures", "dimensions", "annotationSources", "universalDimensions", "unitFamilies", "aggregates", "timeBuckets", "filterOps", "viz" })
        {
            Assert.True(root.TryGetProperty(section, out _), $"full_detail catalog is missing the '{section}' vocabulary");
        }

        Assert.False(root.TryGetProperty("compact", out _), "full_detail must not carry the compact-mode marker");

        var waitTime = root.GetProperty("measures").EnumerateArray()
            .Single(m => m.GetProperty("key").GetString() == "wait_time_ms");
        Assert.Equal("wait_stats", waitTime.GetProperty("source").GetString());
        Assert.Contains("sum", waitTime.GetProperty("validAggregates").EnumerateArray().Select(a => a.GetString()));
        Assert.True(waitTime.TryGetProperty("appliesTo", out _), "full_detail must keep appliesTo per measure");
        Assert.True(waitTime.TryGetProperty("allowedDimensions", out _), "full_detail must keep allowedDimensions per measure");
    }

    [Fact]
    public async Task DescribeCustomViewCatalog_Source_DrillsIntoOneSource_FullDetail()
    {
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(source: "wait_stats");
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        Assert.Equal("wait_stats", root.GetProperty("source").GetString());
        var measures = root.GetProperty("measures").EnumerateArray().ToList();
        Assert.NotEmpty(measures);
        Assert.All(measures, m => Assert.Equal("wait_stats", m.GetProperty("source").GetString()));

        var waitTime = measures.Single(m => m.GetProperty("key").GetString() == "wait_time_ms");
        Assert.True(waitTime.TryGetProperty("appliesTo", out _), "source drill-down must keep appliesTo per measure");
        Assert.True(waitTime.TryGetProperty("allowedDimensions", out _), "source drill-down must keep allowedDimensions per measure");

        var dimensions = root.GetProperty("dimensions").EnumerateArray().ToList();
        Assert.NotEmpty(dimensions);
        Assert.All(dimensions, d => Assert.Equal("wait_stats", d.GetProperty("source").GetString()));

        /* The small shared vocabularies still ride along, same as every other mode. */
        Assert.Contains("sum", root.GetProperty("aggregates").EnumerateArray().Select(a => a.GetString()));
    }

    [Fact]
    public async Task DescribeCustomViewCatalog_UnknownSource_ReturnsEmptyWithNote_NotAnError()
    {
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(source: "no_such_source");
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        Assert.Empty(root.GetProperty("measures").EnumerateArray());
        Assert.Empty(root.GetProperty("dimensions").EnumerateArray());
        Assert.True(root.TryGetProperty("note", out var note), "an unmatched source should explain how to find the real names");
        Assert.Contains("no_such_source", note.GetString());
    }

    [Fact]
    public async Task DescribeCustomViewCatalog_CompactDefault_IsLosslessAndUnderBudget()
    {
        /* Every measure key the compact default advertises must be reachable, at full detail, either by drilling
           into its source or by full_detail=true — the #4198 rule that a cut must never hide what an author
           needs. Every source name the compact default lists must itself be a working source= filter. */
        var compactResult = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog();
        Assert.True(System.Text.Encoding.UTF8.GetByteCount(compactResult) < McpResponseBudget.DefaultBytes,
            $"default describe_custom_view_catalog call is {System.Text.Encoding.UTF8.GetByteCount(compactResult):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget");

        using var compactDoc = JsonDocument.Parse(compactResult);
        var compactKeys = compactDoc.RootElement.GetProperty("sources").EnumerateArray()
            .SelectMany(s => s.GetProperty("measures").EnumerateArray().Select(m => m.GetProperty("key").GetString()!))
            .ToHashSet();

        var fullResult = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(full_detail: true);
        using var fullDoc = JsonDocument.Parse(fullResult);
        var fullKeys = fullDoc.RootElement.GetProperty("measures").EnumerateArray().Select(m => m.GetProperty("key").GetString()!).ToHashSet();

        Assert.Equal(fullKeys, compactKeys);

        foreach (var sourceElement in compactDoc.RootElement.GetProperty("sources").EnumerateArray())
        {
            var sourceName = sourceElement.GetProperty("source").GetString()!;
            var drillResult = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog(source: sourceName);
            using var drillDoc = JsonDocument.Parse(drillResult);
            Assert.NotEmpty(drillDoc.RootElement.GetProperty("measures").EnumerateArray());
        }
    }

    [Fact]
    public async Task CreateCustomView_InvalidDefinition_ReturnsInvalid_WithoutTouchingTheStore()
    {
        /* Validation runs BEFORE persistence, so a bad definition returns 'invalid' without ever opening a
           connection (the dead store would throw if it were reached). */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomViewTools.CreateCustomView(dead, "cv-should-not-persist", BadDefinition);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
        Assert.Contains("unknown measure", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UpdateCustomView_InvalidDefinition_ReturnsInvalid_WithoutTouchingTheStore()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomViewTools.UpdateCustomView(dead, 1, "cv-should-not-persist", BadDefinition, 1);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    /* ---------------- #3541 A14: one write vocabulary for an optional text field ---------------- */

    /// <summary>
    /// The rule itself, as a truth table: omitted (null) keeps the current value — including a current null —
    /// an empty or whitespace-only string clears, and text replaces. The helper is the alert-rule tool's, and
    /// the census below pins that the view tool calls the same one.
    /// </summary>
    [Theory]
    [InlineData(null, "kept", "kept")]
    [InlineData(null, null, null)]
    [InlineData("", "kept", null)]
    [InlineData("   ", "kept", null)]
    [InlineData("new", "kept", "new")]
    [InlineData("new", null, "new")]
    public void ResolveOptionalText_OmittedKeeps_EmptyClears_TextReplaces(string? sent, string? current, string? expected)
    {
        Assert.Equal(expected, DarlingMcpCustomAlertTools.ResolveOptionalText(sent, current));
    }

    /// <summary>
    /// Both update tools route their optional description through the ONE helper — the cross-tool census the
    /// contract asks for. Before this, the two tools disagreed (the view tool wrote an omitted description as
    /// NULL; the rule tool kept it), and a reader of either description had no way to know which rule the other
    /// followed. Read off the stripped source so a comment naming the helper cannot satisfy it; each tool's
    /// UpdateAsync call must pass the helper's result where its description argument goes.
    /// </summary>
    [Fact]
    public void BothUpdateTools_RouteDescriptionThroughTheSharedVocabulary()
    {
        var viewSource = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpCustomViewTools.cs"));
        var ruleSource = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpCustomAlertTools.cs"));

        /* The view tool: reads the row first (there is no "current" to keep without it), then passes the
           helper's result — never the bare parameter — to the store. */
        var viewUpdate = viewSource[viewSource.IndexOf("UpdateCustomView(", StringComparison.Ordinal)..];
        viewUpdate = viewUpdate[..viewUpdate.IndexOf("DeleteCustomView(", StringComparison.Ordinal)];
        Assert.Contains("store.GetAsync(view_id)", viewUpdate, StringComparison.Ordinal);
        Assert.Contains("DarlingMcpCustomAlertTools.ResolveOptionalText(description, currentOk.View.Description)", viewUpdate, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"UpdateAsync\(\s*view_id,\s*name,\s*description,", viewUpdate);

        /* The rule tool: the same helper over its own current row. */
        var ruleUpdate = ruleSource[ruleSource.IndexOf("UpdateCustomAlertRule(", StringComparison.Ordinal)..];
        ruleUpdate = ruleUpdate[..ruleUpdate.IndexOf("DeleteCustomAlertRule(", StringComparison.Ordinal)];
        Assert.Contains("ResolveOptionalText(description, row.Description)", ruleUpdate, StringComparison.Ordinal);
        Assert.DoesNotContain("description ?? row.Description", ruleUpdate, StringComparison.Ordinal);

        /* And the helper is declared exactly once, in the rule tool (the newer contract's home). */
        Assert.Single(Regex.Matches(ruleSource, @"internal static string\? ResolveOptionalText\("));
        Assert.Empty(Regex.Matches(viewSource, @"static string\? ResolveOptionalText\("));
    }

    /// <summary>The two descriptions and their two `description` parameter descriptions tell the SAME story, in
    /// the words a caller will search for. A vocabulary shared in code and not in prose is shared with nobody.</summary>
    [Fact]
    public void BothUpdateTools_DescribeTheSameDescriptionVocabulary()
    {
        static (string Tool, string Param) Prose<T>(string toolName)
        {
            var method = typeof(T)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName);
            var tool = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
            var param = method.GetParameters().Single(p => p.Name == "description").GetCustomAttribute<DescriptionAttribute>()!.Description;
            return (tool, param);
        }

        var view = Prose<DarlingMcpCustomViewTools>("update_custom_view");
        var rule = Prose<DarlingMcpCustomAlertTools>("update_custom_alert_rule");

        foreach (var (tool, param) in new[] { view, rule })
        {
            Assert.Contains("write vocabulary", tool, StringComparison.Ordinal);
            Assert.Contains("Omit to keep the current description; send an empty string \"\" to clear it.", param, StringComparison.Ordinal);
        }

        /* Each names the other, so a reader of one is pointed at the shared rule. */
        Assert.Contains("update_custom_alert_rule", view.Tool, StringComparison.Ordinal);
        Assert.Contains("update_custom_view", rule.Tool, StringComparison.Ordinal);

        /* The release-old denial is gone: the rule tool no longer says the description cannot be cleared. */
        Assert.DoesNotContain("cannot clear it", rule.Param, StringComparison.Ordinal);
        Assert.DoesNotContain("full replacement of name/description/definition", view.Tool, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunCustomViewPanel_BadJson_ReturnsInvalid_WithoutTouchingTheStore()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomViewTools.RunCustomViewPanel(dead, "{ not json");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Fact]
    public async Task RunCustomViewPanel_UnknownMeasure_ReturnsInvalid_WithoutTouchingTheStore()
    {
        /* The panel is validated + compiled before any query, so a bad measure returns 'invalid' without a
           connection — the compile authority (ComposeSpec.TryParsePanel) rejects it first. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomViewTools.RunCustomViewPanel(
            dead, "{\"panel\":{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Fact]
    public async Task RunCustomViewPanel_DoublyNestedSpec_NamesTheNesting_NotTheCatalog()
    {
        /* #2733's run-path half: the run spec DOES nest under 'panel', so the natural over-correction is
           nesting twice — which has no 'source' and used to fail with the misdirecting "unknown source ''".
           The run path stays lenient about unknown keys (stored pre-strictness views must keep running), but
           this one mis-shape is named. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomViewTools.RunCustomViewPanel(
            dead,
            "{\"panel\":{\"panel\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\"}}}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
        Assert.Contains("doubly nested", result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("unknown source", result, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the Custom Views MCP tools against a real PostgreSQL — proves the
/// tool logic CRUDs config.custom_views end-to-end (create → get → list → update → stale-conflict → dup-conflict
/// → delete → not-found) and that run_custom_view_panel compiles + executes a composed panel against the real
/// collector schema. Own-scoped per the shared-store doctrine (GUID-suffixed name + a finally cleanup). Mirrors
/// DarlingCustomViewsLiveTests, but through the tool surface rather than CustomViewStore directly.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpCustomViewToolsLivePostgresTests
{
    private const string GoodDashboard =
        "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}";

    private const string GoodDashboardV2 =
        "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}";

    private const string RunSpec =
        "{\"panel\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}}";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task CustomViewTools_CrudAndRunRoundTrip_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-view MCP tools live test.");

        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(cs))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        /* Explicit search_path so the store's bare custom_views resolves to config.custom_views regardless of the
           store's database default (mirrors DarlingCustomViewsLiveTests). */
        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(cs)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        await using var postgres = NpgsqlDataSource.Create(dataSourceConnectionString);

        var name = "cv_mcp_live_" + Guid.NewGuid().ToString("N");
        var seedServerName = "cv_mcp_seed_" + Guid.NewGuid().ToString("N");
        var seedServerId = ServerIdHelper.GetDeterministicHashCode(seedServerName);
        var bodySucceeded = false;
        try
        {
            /* validate (dry-run) — valid + invalid, no persistence. */
            Assert.True(JsonDocument.Parse(await DarlingMcpCustomViewTools.ValidateCustomView(GoodDashboard))
                .RootElement.GetProperty("valid").GetBoolean());
            Assert.False(JsonDocument.Parse(await DarlingMcpCustomViewTools.ValidateCustomView(
                "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}]}"))
                .RootElement.GetProperty("valid").GetBoolean());

            /* create with an INVALID definition — rejected by ValidateDefinition BEFORE any persistence (the LIVE
               path, not just the dead-store ungated test): the tool returns 'invalid' AND no row is stored. */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpCustomViewTools.CreateCustomView(
                postgres, name + "_invalid",
                "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}]}")));
            using (var afterInvalid = JsonDocument.Parse(await DarlingMcpCustomViewTools.ListCustomViews(postgres)))
            {
                Assert.False(afterInvalid.RootElement.EnumerateArray()
                    .Any(v => v.GetProperty("name").GetString() == name + "_invalid"),
                    "an invalid definition must not persist a row.");
            }

            /* create — returns the stored view at version 1, MCP-stamped. */
            long id;
            using (var created = JsonDocument.Parse(
                await DarlingMcpCustomViewTools.CreateCustomView(postgres, name, GoodDashboard, "made over MCP")))
            {
                Assert.Equal(name, created.RootElement.GetProperty("name").GetString());
                Assert.Equal(1, created.RootElement.GetProperty("version").GetInt32());
                Assert.Equal("mcp", created.RootElement.GetProperty("updated_by").GetString());
                id = created.RootElement.GetProperty("id").GetInt64();
            }

            /* get — full view with the embedded definition. */
            using (var fetched = JsonDocument.Parse(await DarlingMcpCustomViewTools.GetCustomView(postgres, id)))
            {
                Assert.Equal(id, fetched.RootElement.GetProperty("id").GetInt64());
                Assert.Equal(name, fetched.RootElement.GetProperty("name").GetString());
                Assert.True(fetched.RootElement.TryGetProperty("definition", out _));
            }

            /* list — our view appears as a dashboard summary. */
            using (var list = JsonDocument.Parse(await DarlingMcpCustomViewTools.ListCustomViews(postgres)))
            {
                var mine = list.RootElement.EnumerateArray().Single(v => v.GetProperty("id").GetInt64() == id);
                Assert.Equal(name, mine.GetProperty("name").GetString());
                Assert.Equal("dashboard", mine.GetProperty("kind").GetString());
            }

            /* update at the correct version — bumps to 2. */
            using (var updated = JsonDocument.Parse(
                await DarlingMcpCustomViewTools.UpdateCustomView(postgres, id, name, GoodDashboardV2, 1, "edited over MCP")))
            {
                Assert.Equal(2, updated.RootElement.GetProperty("version").GetInt32());
                Assert.Equal("edited over MCP", updated.RootElement.GetProperty("description").GetString());
            }

            /* #3541 A14: an OMITTED description is unchanged — this exact call used to write NULL. Version 3. */
            using (var kept = JsonDocument.Parse(
                await DarlingMcpCustomViewTools.UpdateCustomView(postgres, id, name, GoodDashboardV2, 2)))
            {
                Assert.Equal(3, kept.RootElement.GetProperty("version").GetInt32());
                Assert.Equal("edited over MCP", kept.RootElement.GetProperty("description").GetString());
            }

            /* ... and an EMPTY string is the explicit clear. Version 4. */
            using (var cleared = JsonDocument.Parse(
                await DarlingMcpCustomViewTools.UpdateCustomView(postgres, id, name, GoodDashboardV2, 3, "")))
            {
                Assert.Equal(4, cleared.RootElement.GetProperty("version").GetInt32());
                Assert.Equal(JsonValueKind.Null, cleared.RootElement.GetProperty("description").ValueKind);
            }

            /* stale update (still presenting version 1) — conflict, not a silent clobber. */
            Assert.Equal("conflict", DarlingMcpTestData.StatusOf(
                await DarlingMcpCustomViewTools.UpdateCustomView(postgres, id, name, GoodDashboardV2, 1)));

            /* duplicate-name create — conflict. */
            Assert.Equal("conflict", DarlingMcpTestData.StatusOf(
                await DarlingMcpCustomViewTools.CreateCustomView(postgres, name, GoodDashboard)));

            /* Seed collect.wait_stats so run_custom_view_panel returns REAL rows — the panel sums
               delta_wait_time_ms into hourly buckets over the default 24h window (two rows, one and two hours back).
               The seed connection carries the explicit collect/config search_path (like the tool data source) so the
               helper's bare `servers` / `wait_stats` resolve regardless of pool state — a raw `cs` connection can
               get a stale pre-migration session whose search_path is just `public` (only surfaces in a full run). */
            await using (var seed = new NpgsqlConnection(dataSourceConnectionString))
            {
                await seed.OpenAsync(ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, seedServerId, seedServerName, ct);
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    1L, DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-2)), seedServerId, seedServerName, "CXPACKET", 10L, 40000L);
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    2L, DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-1)), seedServerId, seedServerName, "PAGEIOLATCH_SH", 5L, 60000L);
            }

            /* run_custom_view_panel — compiles + executes against the real store and returns NON-EMPTY rows for the
               seeded window, proving the composed panel's DATA flows end-to-end (not just that the SQL runs). */
            using (var run = JsonDocument.Parse(await DarlingMcpCustomViewTools.RunCustomViewPanel(postgres, RunSpec)))
            {
                Assert.True(run.RootElement.TryGetProperty("sql", out var sql));
                Assert.Contains("collect.", sql.GetString()!, StringComparison.Ordinal);
                var rows = run.RootElement.GetProperty("rows");
                Assert.Equal(JsonValueKind.Array, rows.ValueKind);
                Assert.True(rows.GetArrayLength() >= 1,
                    "expected the seeded wait_stats rows to produce at least one bucket: " + run.RootElement.GetRawText());
                Assert.Equal(JsonValueKind.Array, run.RootElement.GetProperty("annotations").ValueKind);
            }

            /* run with a bad measure — invalid, no crash. */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpCustomViewTools.RunCustomViewPanel(
                postgres, "{\"panel\":{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}}")));

            /* delete — deleted; then get + delete-again — not_found. */
            Assert.Equal("deleted", DarlingMcpTestData.StatusOf(await DarlingMcpCustomViewTools.DeleteCustomView(postgres, id)));
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpCustomViewTools.GetCustomView(postgres, id)));
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpCustomViewTools.DeleteCustomView(postgres, id)));

            bodySucceeded = true;
        }
        finally
        {
            /* Separate single-statement commands: Npgsql cannot run a multi-command batch as one parameterized
               (prepared) statement (42601). Own-scoped by GUID name + seed server id, so nothing else is touched.

               The hand-rolled cleanup connection is gone (#1902): LiveStoreCleanup already supplies one, and
               with it the two things this did not have — a token that survives a cancelled run, and silence
               while the body's own exception is in flight. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt,
                    "DELETE FROM config.custom_views WHERE name = $1 OR name = $2", name, name + "_invalid");
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.wait_stats WHERE server_id = $1", seedServerId);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.servers WHERE server_id = $1", seedServerId);
            });
        }
    }
}
