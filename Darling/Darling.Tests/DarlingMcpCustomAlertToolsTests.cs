/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Ungated (no-live-store) contract for the #3285 custom-alert-rule MCP tools: the tool surface is EXACTLY the
/// seven management tools (the six CRUD/validate tools plus #3299's test_custom_alert_rule evaluate-now; all
/// static, on a [McpServerToolType] class, returning Task&lt;string&gt;), the advertised tools/list schema is
/// Gemini-clean (#1074) with the expected required-param set, and validate / create / update run the SAME
/// CustomAlertRuleDefinition.TryParse authority (the one the evaluator uses) BEFORE any persistence - an invalid
/// definition never reaches the store. The live CRUD round-trip is gated below.
/// </summary>
public sealed class DarlingMcpCustomAlertToolsSurfaceTests
{
    /// <summary>A dead data source (unroutable port) - proves the validate-before-persist tools bail on a bad
    /// definition (or a no-op update) WITHOUT ever opening a connection (the call returns before touching the
    /// store).</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string GoodRule =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}";

    private const string BadMeasureRule =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private const string MissingPredicateRule =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}}";

    private static readonly string[] ExpectedToolSurface =
    {
        "create_custom_alert_rule",
        "delete_custom_alert_rule",
        "get_custom_alert_rule",
        "list_custom_alert_rules",
        "test_custom_alert_rule",
        "update_custom_alert_rule",
        "validate_custom_alert_rule",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpCustomAlertTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IsExactlyTheSevenCustomAlertTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ExpectedToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpCustomAlertTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static System.Collections.Generic.Dictionary<string, ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpCustomAlertTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllSevenTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(7, tools.Count);
        var violations = tools.Values.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
    }

    [Theory]
    [InlineData("list_custom_alert_rules", "")]
    [InlineData("get_custom_alert_rule", "rule_id")]
    [InlineData("validate_custom_alert_rule", "definition")]
    [InlineData("create_custom_alert_rule", "name,definition")]
    [InlineData("update_custom_alert_rule", "rule_id,version")]
    [InlineData("delete_custom_alert_rule", "rule_id")]
    /* #3299: both inputs are optional (the tool enforces "exactly one" at runtime, not via required-schema). */
    [InlineData("test_custom_alert_rule", "")]
    public void AdvertisedSchema_RequiredParams_MatchTheContract(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Length == 0 ? Array.Empty<string>() : expectedCsv.Split(',');
        var required = DarlingMcpSchemaAssert.RequiredOf(BuildToolSchemas()[toolName].InputSchema)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    [Fact]
    public async Task ValidateCustomAlertRule_ValidRule_ReturnsValidTrue()
    {
        var result = await DarlingMcpCustomAlertTools.ValidateCustomAlertRule(GoodRule);
        using var doc = JsonDocument.Parse(result);
        Assert.True(doc.RootElement.GetProperty("valid").GetBoolean(), result);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("error").ValueKind);
    }

    [Fact]
    public async Task ValidateCustomAlertRule_BadMeasure_ReturnsValidFalse_WithError()
    {
        var result = await DarlingMcpCustomAlertTools.ValidateCustomAlertRule(BadMeasureRule);
        using var doc = JsonDocument.Parse(result);
        Assert.False(doc.RootElement.GetProperty("valid").GetBoolean(), result);
        Assert.Contains("measure", doc.RootElement.GetProperty("error").GetString()!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ValidateCustomAlertRule_MissingPredicate_ReturnsValidFalse()
    {
        /* The predicate is the rule-specific half TryParse owns beyond the compose panel; a metric alone is not
           a rule. */
        var result = await DarlingMcpCustomAlertTools.ValidateCustomAlertRule(MissingPredicateRule);
        using var doc = JsonDocument.Parse(result);
        Assert.False(doc.RootElement.GetProperty("valid").GetBoolean(), result);
        Assert.Contains("predicate", doc.RootElement.GetProperty("error").GetString()!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateCustomAlertRule_InvalidDefinition_ReturnsInvalid_WithoutTouchingTheStore()
    {
        /* Validation runs BEFORE persistence, so a bad definition returns 'invalid' without ever opening a
           connection (the dead store would throw if it were reached). */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomAlertTools.CreateCustomAlertRule(dead, "car-should-not-persist", BadMeasureRule);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
        Assert.Contains("measure", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UpdateCustomAlertRule_InvalidDefinition_ReturnsInvalid_WithoutTouchingTheStore()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(dead, 1, 1, definition: BadMeasureRule);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Fact]
    public async Task UpdateCustomAlertRule_NoFields_ReturnsInvalid_WithoutTouchingTheStore()
    {
        /* A partial update with nothing to change is a no-op request; it is refused before any store hit rather
           than pointlessly bumping the version. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(dead, 1, 1);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the custom-alert-rule MCP tools against a real PostgreSQL - proves
/// the tool logic CRUDs config.custom_alert_rules end-to-end (create -> get -> list -> partial-update -> definition
/// update -> stale-conflict -> dup-conflict -> delete -> not-found) through the SAME store the evaluator reads.
/// Own-scoped per the shared-store doctrine (GUID-suffixed name + a finally cleanup). Mirrors
/// CustomAlertStateStoreLiveTests / DarlingMcpCustomViewToolsLivePostgresTests, but through the tool surface.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpCustomAlertToolsLivePostgresTests
{
    private const string GoodRule =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}";

    private const string GoodRuleV2 =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"avg\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":500,\"criticalThreshold\":2000}}";

    private const string BadMeasureRule =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task CustomAlertRuleTools_CrudRoundTrip_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-alert-rule MCP tools live test.");

        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(cs))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        /* Explicit search_path so the store's bare custom_alert_rules resolves to config.custom_alert_rules
           regardless of the store's database default (mirrors CustomAlertStateStoreLiveTests). */
        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(cs)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        await using var postgres = NpgsqlDataSource.Create(dataSourceConnectionString);

        var name = "car_mcp_live_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            /* validate (dry-run) - valid + invalid, no persistence. */
            Assert.True(JsonDocument.Parse(await DarlingMcpCustomAlertTools.ValidateCustomAlertRule(GoodRule))
                .RootElement.GetProperty("valid").GetBoolean());
            Assert.False(JsonDocument.Parse(await DarlingMcpCustomAlertTools.ValidateCustomAlertRule(BadMeasureRule))
                .RootElement.GetProperty("valid").GetBoolean());

            /* create with an INVALID definition - rejected by TryParse BEFORE any persistence (the LIVE path,
               not just the dead-store ungated test): the tool returns 'invalid' AND no row is stored. */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(
                await DarlingMcpCustomAlertTools.CreateCustomAlertRule(postgres, name + "_invalid", BadMeasureRule)));
            using (var afterInvalid = JsonDocument.Parse(await DarlingMcpCustomAlertTools.ListCustomAlertRules(postgres)))
            {
                Assert.False(afterInvalid.RootElement.EnumerateArray()
                    .Any(r => r.GetProperty("name").GetString() == name + "_invalid"),
                    "an invalid definition must not persist a row.");
            }

            /* create - returns the stored rule at version 1, enabled, MCP-stamped. */
            long id;
            using (var created = JsonDocument.Parse(
                await DarlingMcpCustomAlertTools.CreateCustomAlertRule(postgres, name, GoodRule, "made over MCP")))
            {
                Assert.Equal(name, created.RootElement.GetProperty("name").GetString());
                Assert.Equal(1, created.RootElement.GetProperty("version").GetInt32());
                Assert.True(created.RootElement.GetProperty("enabled").GetBoolean());
                Assert.Equal("mcp", created.RootElement.GetProperty("updated_by").GetString());
                id = created.RootElement.GetProperty("id").GetInt64();
            }

            /* get - full rule with the embedded definition. */
            using (var fetched = JsonDocument.Parse(await DarlingMcpCustomAlertTools.GetCustomAlertRule(postgres, id)))
            {
                Assert.Equal(id, fetched.RootElement.GetProperty("id").GetInt64());
                Assert.Equal(name, fetched.RootElement.GetProperty("name").GetString());
                Assert.True(fetched.RootElement.TryGetProperty("definition", out _));
            }

            /* list - our rule appears, enabled. */
            using (var list = JsonDocument.Parse(await DarlingMcpCustomAlertTools.ListCustomAlertRules(postgres)))
            {
                var mine = list.RootElement.EnumerateArray().Single(r => r.GetProperty("id").GetInt64() == id);
                Assert.Equal(name, mine.GetProperty("name").GetString());
                Assert.True(mine.GetProperty("enabled").GetBoolean());
            }

            /* PARTIAL update at version 1 - flip enabled only. name + definition are omitted, so they must be
               preserved, and the version bumps to 2. */
            using (var paused = JsonDocument.Parse(
                await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(postgres, id, 1, enabled: false)))
            {
                Assert.Equal(2, paused.RootElement.GetProperty("version").GetInt32());
                Assert.False(paused.RootElement.GetProperty("enabled").GetBoolean());
                Assert.Equal(name, paused.RootElement.GetProperty("name").GetString());
            }

            /* PARTIAL update at version 2 - swap the definition only. enabled (false) must be preserved, version 3. */
            using (var redefined = JsonDocument.Parse(
                await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(postgres, id, 2, definition: GoodRuleV2)))
            {
                Assert.Equal(3, redefined.RootElement.GetProperty("version").GetInt32());
                Assert.False(redefined.RootElement.GetProperty("enabled").GetBoolean());
            }

            /* stale update (still presenting version 1) - conflict, not a silent clobber. */
            Assert.Equal("conflict", DarlingMcpTestData.StatusOf(
                await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(postgres, id, 1, enabled: true)));

            /* duplicate-name create - conflict. */
            Assert.Equal("conflict", DarlingMcpTestData.StatusOf(
                await DarlingMcpCustomAlertTools.CreateCustomAlertRule(postgres, name, GoodRule)));

            /* update with an INVALID definition on a live row - invalid, and the row is untouched. */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(
                await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(postgres, id, 3, definition: BadMeasureRule)));

            /* delete - deleted; then get + delete-again - not_found. */
            Assert.Equal("deleted", DarlingMcpTestData.StatusOf(await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(postgres, id)));
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpCustomAlertTools.GetCustomAlertRule(postgres, id)));
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(postgres, id)));

            bodySucceeded = true;
        }
        finally
        {
            /* Own-scoped by GUID name. LiveStoreCleanup supplies a cleanup connection with a token that survives
               a cancelled run and stays silent while the body's own exception is in flight. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt,
                    "DELETE FROM config.custom_alert_rules WHERE name = $1 OR name = $2", name, name + "_invalid");
            });
        }
    }
}
