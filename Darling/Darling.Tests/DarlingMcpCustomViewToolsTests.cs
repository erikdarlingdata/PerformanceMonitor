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
    public async Task DescribeCustomViewCatalog_ReturnsTheComposeVocabulary()
    {
        /* The catalog tool exists so an LLM composes a valid panel WITHOUT reading source or guessing names — it
           must surface every vocabulary a panel draws from, plus a known measure with its composable fields. */
        var result = await DarlingMcpCustomViewTools.DescribeCustomViewCatalog();
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        foreach (var section in new[] { "measures", "dimensions", "unitFamilies", "aggregates", "timeBuckets", "filterOps", "viz" })
        {
            Assert.True(root.TryGetProperty(section, out _), $"catalog is missing the '{section}' vocabulary");
        }

        /* A known measure is discoverable with the fields a panel binds from it (source + valid aggregates). */
        var waitTime = root.GetProperty("measures").EnumerateArray()
            .Single(m => m.GetProperty("key").GetString() == "wait_time_ms");
        Assert.Equal("wait_stats", waitTime.GetProperty("source").GetString());
        Assert.Contains("sum", waitTime.GetProperty("validAggregates").EnumerateArray().Select(a => a.GetString()));

        /* The scalar vocabularies the panel's aggregate + viz fields draw from. */
        Assert.Contains("sum", root.GetProperty("aggregates").EnumerateArray().Select(a => a.GetString()));
        Assert.Contains("bar", root.GetProperty("viz").EnumerateArray().Select(v => v.GetString()));
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
