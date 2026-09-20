/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the plan-analysis MCP slice — the five plan tools (analyze_query_plan,
/// analyze_procedure_plan, analyze_query_store_plan, analyze_plan_xml, get_plan_xml) over the
/// Postgres store, the same names the Dashboard and Lite expose. Ungated: the tool surface is
/// EXACTLY the five names (all static, the McpSchemaCompat requirement, on a [McpServerToolType]
/// class); each tool's MCP parameter contract matches the Dashboard's required params (with the
/// store's finer keys — database_name / plan_id — as OPTIONAL refinements); the three stored-plan
/// reads are Postgres-dialect, positional-param, and read the collector columns the schema
/// generator actually emits (query_stats.query_plan_xml, procedure_stats.query_plan_xml +
/// sql_handle, query_store_stats.query_plan_text); analyze_plan_xml's no-fetch path is
/// resilient (empty → the bare message, malformed → no throw); and the four analyze_* descriptions
/// name missing_indexes[].create_statement and impact_basis for what they are rather than #3696's
/// suppression sentence (#3805). Gated on DARLING_TEST_PG: register
/// a server the way the worker does, plant stored plans, call the tool METHODS directly (not over
/// the wire) and assert each fetch + analysis round-trips, the optional keys refine, and an absent
/// key returns the #1224 "unavailable" envelope.
/// </summary>
public sealed class DarlingMcpPlanToolsSurfaceAndSqlTests
{
    /* ---------------- ungated: tool-surface pin ---------------- */

    /// <summary>The five plan-analysis tool names, ordinal-sorted — the same names the Dashboard's
    /// and Lite's McpPlanTools register, so MCP clients see one consistent product.</summary>
    private static readonly string[] SharedPlanToolSurface =
    {
        "analyze_plan_xml",
        "analyze_procedure_plan",
        "analyze_query_plan",
        "analyze_query_store_plan",
        "get_plan_xml"
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpPlanTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheFivePlanTools()
    {
        var toolMethods = ToolMethods();

        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(SharedPlanToolSurface, names);

        /* Discoverable as a tool type, every tool static (WithGeminiCompatibleTools REJECTS instance
           methods), and every tool returning the string envelope both apps' tools return (Task<string>
           for the fetching tools, string for the synchronous analyze_plan_xml). */
        Assert.NotNull(typeof(DarlingMcpPlanTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.True(
            m.ReturnType == typeof(Task<string>) || m.ReturnType == typeof(string),
            $"{m.Name} must return string or Task<string>"));
    }

    /* ---------------- ungated: per-tool MCP parameter contract ---------------- */

    /// <summary>The MCP input parameters of a tool are those decorated with [Description]; the
    /// injected DI services (NpgsqlDataSource) carry no [Description] and are not part of the schema.</summary>
    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m =>
            m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Fact]
    public void ParamContract_AnalyzeQueryPlan_QueryHashRequired_DatabaseAndServerOptional()
    {
        var p = McpParams("analyze_query_plan");
        Assert.Equal(new[] { "query_hash", "server_name", "database_name" }, p.Select(x => x.Name).ToArray());
        Assert.False(Optional(p, "query_hash"));      /* required — the Dashboard's key */
        Assert.True(Optional(p, "server_name"));      /* optional — sole-server auto-select */
        Assert.True(Optional(p, "database_name"));    /* optional refinement over the Dashboard's contract */
    }

    [Fact]
    public void ParamContract_AnalyzeProcedurePlan_SqlHandleRequired_ServerOptional()
    {
        var p = McpParams("analyze_procedure_plan");
        Assert.Equal(new[] { "sql_handle", "server_name" }, p.Select(x => x.Name).ToArray());
        Assert.False(Optional(p, "sql_handle"));
        Assert.True(Optional(p, "server_name"));
    }

    [Fact]
    public void ParamContract_AnalyzeQueryStorePlan_DatabaseAndQueryIdRequired_PlanIdAndServerOptional()
    {
        var p = McpParams("analyze_query_store_plan");
        Assert.Equal(new[] { "database_name", "query_id", "server_name", "plan_id" }, p.Select(x => x.Name).ToArray());
        Assert.False(Optional(p, "database_name"));   /* required — the Dashboard's key */
        Assert.False(Optional(p, "query_id"));        /* required — the Dashboard's key */
        Assert.True(Optional(p, "server_name"));
        Assert.True(Optional(p, "plan_id"));          /* optional refinement over the Dashboard's contract */
    }

    [Fact]
    public void ParamContract_AnalyzePlanXml_TakesOnlyRawXml_NoDbFetch()
    {
        var p = McpParams("analyze_plan_xml");
        Assert.Equal(new[] { "plan_xml" }, p.Select(x => x.Name).ToArray());
        Assert.False(Optional(p, "plan_xml"));
    }

    [Fact]
    public void ParamContract_GetPlanXml_QueryHashRequired_DatabaseAndServerOptional()
    {
        var p = McpParams("get_plan_xml");
        Assert.Equal(new[] { "query_hash", "server_name", "database_name" }, p.Select(x => x.Name).ToArray());
        Assert.False(Optional(p, "query_hash"));
        Assert.True(Optional(p, "server_name"));
        Assert.True(Optional(p, "database_name"));
    }

    private static bool Optional((string Name, bool Optional)[] p, string name) =>
        p.Single(x => x.Name == name).Optional;

    /* ---------------- ungated: the four analyze_* descriptions say what the payload carries ---------------- */

    /// <summary>
    /// #3805 — the four Darling <c>analyze_*_plan</c> descriptions (every plan tool but <c>get_plan_xml</c>, which
    /// returns raw XML and enumerates no payload) name <c>missing_indexes[].create_statement</c> for what it is: the
    /// optimizer's suggested CREATE INDEX for that one statement, CORROBORATION for a statement already measured slow
    /// and never a diagnosis, beside the labelled <c>impact_basis</c> and the fixed per-row <c>caveat</c> whose three
    /// clauses the description restates (per-statement estimate; per-table commitment with write cost and regression
    /// risk for other plans; test it). #3696 had them say "no CREATE INDEX text — the suggestion is a hint, not a
    /// design" on a rule that was never made — suppression; the maintainer's reframing is honesty, "corroboration,
    /// with caveats", and the suppression sentence must not come back. Lite's three are pinned in <c>Lite.Tests</c>
    /// (<c>McpDescriptionTruthPinTests</c>) with the same fragments, so the two SKUs describe one field in one voice.
    /// </summary>
    [Fact]
    public void AnalyzeDescriptions_NameCreateStatementAndImpactBasis_NotTheSuppression()
    {
        var analyzeTools = ToolMethods()
            .Where(m => (m.GetCustomAttribute<McpServerToolAttribute>()!.Name ?? "").StartsWith("analyze_", StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(4, analyzeTools.Length);

        Assert.All(analyzeTools, m =>
        {
            var description = m.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.Contains("labelled impact_basis", description, StringComparison.Ordinal);
            Assert.Contains("create_statement — the optimizer's suggested CREATE INDEX for this statement", description, StringComparison.Ordinal);
            Assert.Contains("corroboration for a statement already measured slow, never a diagnosis", description, StringComparison.Ordinal);
            Assert.Contains("every row carries the fixed caveat", description, StringComparison.Ordinal);
            Assert.Contains("regression risk for other plans", description, StringComparison.Ordinal);
            Assert.DoesNotContain("no CREATE INDEX text", description, StringComparison.Ordinal);
            Assert.DoesNotContain("a hint, not a design", description, StringComparison.Ordinal);
        });

        /* The instructions' family paragraph names the field the same way and no longer carries #3696's
           "no CREATE INDEX text". */
        var instructions = DarlingMcpInstructions.Text;
        Assert.Contains("`create_statement` — the optimizer's suggested CREATE INDEX for that one statement, corroboration for a statement already measured slow and never a diagnosis", instructions, StringComparison.Ordinal);
        Assert.Contains("every row carries the fixed `caveat`", instructions, StringComparison.Ordinal);
        Assert.DoesNotContain("no CREATE INDEX text", instructions, StringComparison.Ordinal);
        Assert.DoesNotContain("a hint, not a design", instructions, StringComparison.Ordinal);
    }

    /* ---------------- ungated: stored-plan read SQL pins ---------------- */

    [Fact]
    public void QueryStatsPlanSql_ReadsStoredXml_KeyedByHash_OptionalDatabase_LatestNonNull()
    {
        var sql = DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql;
        /* Both content forms (#2069): plans written since V54 are gzip bytes with the text NULL. */
        Assert.Contains("SELECT query_plan_xml, query_plan_gz", sql, StringComparison.Ordinal);
        /* The RESOLVING view, not the base table (#1767): query_stats.query_plan_xml is NULL on every row
           written since the migration. Reading the base table would return NULL rather than error. */
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $2", sql, StringComparison.Ordinal);                    /* the Dashboard's key */
        Assert.Contains("$3::text IS NULL OR database_name = $3", sql, StringComparison.Ordinal); /* optional database refinement */
        /* The presence guard must accept EITHER form — on the text column alone it discards every
           post-V54 plan silently (zero rows, no error), the #1767 bare-column regression re-run. */
        Assert.Contains("(query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);      /* most-recent plan for the key */
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedurePlanSql_ResolvesThePlanDimensionItself_GuardingOnTheCoalescedExpression()
    {
        var sql = DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql;

        /* No v_procedure_stats exists to resolve the #1767 plan dimension, so this read joins query_plan_dim
           itself — the same shape as the viewer's ProcedureStatsPlanXmlSql twin. */
        Assert.Contains("SELECT COALESCE(ps.query_plan_xml, qpd.query_plan_xml), qpd.query_plan_gz", sql, StringComparison.Ordinal);
        Assert.Contains("FROM procedure_stats AS ps", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_procedure_stats", sql, StringComparison.Ordinal);            /* that view has never existed */
        Assert.Contains("LEFT JOIN query_plan_dim AS qpd", sql, StringComparison.Ordinal);
        Assert.Contains("ON qpd.digest = ps.query_plan_digest", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE ps.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ps.sql_handle = $2", sql, StringComparison.Ordinal);                 /* the Dashboard's key */

        /* THE regression to guard: the presence filter must ride the COALESCED expression. On the bare
           inline column it discards every row written since #1767 before the join can resolve it — zero
           rows, no error, reads exactly like "no plan captured". */
        Assert.Contains("AND   (COALESCE(ps.query_plan_xml, qpd.query_plan_xml) IS NOT NULL OR qpd.query_plan_gz IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AND   ps.query_plan_xml IS NOT NULL", sql, StringComparison.Ordinal);

        Assert.Contains("ORDER BY ps.collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStorePlanSql_ReadsStoredText_KeyedByDatabaseQueryId_OptionalPlanId_LatestNonNull()
    {
        var sql = DarlingStoredPlanReader.QueryStorePlanTextSql;
        Assert.Contains("SELECT query_plan_text", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);                 /* the Dashboard's key */
        Assert.Contains("query_id = $3", sql, StringComparison.Ordinal);                      /* the Dashboard's key */
        Assert.Contains("$4::bigint IS NULL OR plan_id = $4", sql, StringComparison.Ordinal); /* optional plan_id refinement */
        Assert.Contains("query_plan_text IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql))]
    [InlineData(nameof(DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql))]
    [InlineData(nameof(DarlingStoredPlanReader.QueryStorePlanTextSql))]
    public void PlanReads_ArePostgresDialect_PositionalParams_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql) => DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql,
            nameof(DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql) => DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql,
            _ => DarlingStoredPlanReader.QueryStorePlanTextSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);   /* no T-SQL binary CONVERT — the store keeps hex strings as text */
        Assert.DoesNotContain("top (", lower);      /* LIMIT, not TOP */
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanColumns_ExistInTheGeneratedCollectorTables()
    {
        /* The reads depend on these collector columns; pin that the schema generator emits them. */
        Assert.Equal("query_stats", QueryStatsCollector.Instance.TargetTable);
        Assert.Contains("query_plan_xml", PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance), StringComparison.Ordinal);

        Assert.Equal("procedure_stats", ProcedureStatsCollector.Instance.TargetTable);
        var procTable = PgSchemaGenerator.CreateTable(ProcedureStatsCollector.Instance);
        Assert.Contains("query_plan_xml", procTable, StringComparison.Ordinal);
        Assert.Contains("sql_handle", procTable, StringComparison.Ordinal);

        Assert.Equal("query_store_stats", QueryStoreCollector.Instance.TargetTable);
        Assert.Contains("query_plan_text", PgSchemaGenerator.CreateTable(QueryStoreCollector.Instance), StringComparison.Ordinal);
    }

    /* ---------------- ungated: analyze_plan_xml no-fetch path ---------------- */

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void AnalyzePlanXml_EmptyOrWhitespace_IsRefused(string? input)
    {
        /* The refusal is McpHelpers.Refusal's `invalid` envelope since #3739 (it was this bare sentence); the
           sentence is unchanged inside it and the parameter is named. */
        var refusal = DarlingMcpPlanTools.AnalyzePlanXml(input!);
        Assert.Equal(McpHelpers.Refusal("plan_xml", "No plan XML provided."), refusal);
        Assert.Equal("No plan XML provided.", McpHelpers.ErrorMessageOf(refusal));
    }

    [Theory]
    [InlineData("<not-a-plan/>")]
    [InlineData("<ShowPlanXML>truncated...")]
    [InlineData("{\"json\":true}")]
    public void AnalyzePlanXml_Malformed_DoesNotThrow_ReturnsNonEmptyString(string input)
    {
        /* The tool's try/catch degrades a parse failure to a message string — it never throws
           (mirrors the shared PlanAdvisoryAggregator resilience contract). */
        string? result = null;
        var ex = Record.Exception(() => result = DarlingMcpPlanTools.AnalyzePlanXml(input));
        Assert.Null(ex);
        Assert.False(string.IsNullOrEmpty(result));
    }

    [Fact]
    public void AnalyzePlanXml_WellFormedShowPlan_ReturnsXmlSourcedEnvelope()
    {
        /* The shared McpPlanAnalysisFormatter runs and serializes the envelope (server null, source
           "xml"); this also proves the sample plan parses cleanly for the gated fetch tests below. */
        var json = DarlingMcpPlanTools.AnalyzePlanXml(DarlingPlanFixtures.SamplePlanXml);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("xml", root.GetProperty("source").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("server").ValueKind);
        Assert.True(root.TryGetProperty("statement_count", out _));
        Assert.True(root.TryGetProperty("total_warnings", out _));
        Assert.True(root.TryGetProperty("total_missing_indexes", out _));
    }

    /* ---------------- ungated: advertised MCP schema (the #1074 Gemini contract) ---------------- */

    /// <summary>
    /// Builds the exact protocol tools the host advertises in tools/list, registering the plan-tool
    /// class through the SAME WithGeminiCompatibleTools path Program wiring uses. NpgsqlDataSource (the
    /// only DI-injected tool parameter) is registered as a no-op singleton so it is excluded from the
    /// schema; the test reads schemas only and never invokes a tool.
    /// </summary>
    private static List<ModelContextProtocol.Protocol.Tool> BuildPlanToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpPlanTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    private static List<string> SchemaViolations(string toolName, JsonElement schema)
    {
        var violations = new List<string>();
        Walk(schema, toolName);
        return violations;

        void Walk(JsonElement element, string path)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        /* Gemini/Antigravity drops the whole tool set on a union "type" array or a
                           "default" keyword (issue #1074); assert neither survives the transform. */
                        if (property.NameEquals("type") && property.Value.ValueKind == JsonValueKind.Array)
                            violations.Add($"{path}: union type {property.Value.GetRawText()}");
                        if (property.NameEquals("default"))
                            violations.Add($"{path}: default = {property.Value.GetRawText()}");
                        Walk(property.Value, $"{path}.{property.Name}");
                    }
                    break;
                case JsonValueKind.Array:
                    var i = 0;
                    foreach (var item in element.EnumerateArray())
                        Walk(item, $"{path}[{i++}]");
                    break;
            }
        }
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllFivePlanTools()
    {
        var tools = BuildPlanToolSchemas();
        Assert.Equal(5, tools.Count);

        var violations = tools.SelectMany(t => SchemaViolations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0,
            "Gemini-incompatible schema keywords leaked into tools/list:\n" + string.Join("\n", violations));
    }

    [Fact]
    public void AdvertisedSchema_RequiredParams_MatchTheDashboardContract()
    {
        var tools = BuildPlanToolSchemas().ToDictionary(t => t.Name, t => t.InputSchema);

        /* The required set is the Dashboard's key params; the store's finer keys (database_name /
           plan_id) are optional refinements, so they must NOT appear in required. */
        Assert.Equal(new[] { "query_hash" }, RequiredOf(tools["analyze_query_plan"]));
        Assert.Equal(new[] { "sql_handle" }, RequiredOf(tools["analyze_procedure_plan"]));
        Assert.Equal(new[] { "database_name", "query_id" }, RequiredOf(tools["analyze_query_store_plan"]));
        Assert.Equal(new[] { "plan_xml" }, RequiredOf(tools["analyze_plan_xml"]));
        Assert.Equal(new[] { "query_hash" }, RequiredOf(tools["get_plan_xml"]));
    }

    [Fact]
    public void AdvertisedSchema_NullableLongPlanId_CollapsesToInteger_NotAUnion()
    {
        /* plan_id is the product's first nullable value-type MCP parameter (long?). Prove the Gemini
           transform collapses its ["integer","null"] union to a single "integer" type. */
        var schema = BuildPlanToolSchemas().Single(t => t.Name == "analyze_query_store_plan").InputSchema;
        var planId = schema.GetProperty("properties").GetProperty("plan_id");
        Assert.Equal(JsonValueKind.String, planId.GetProperty("type").ValueKind);
        Assert.Equal("integer", planId.GetProperty("type").GetString());
        Assert.DoesNotContain("plan_id", RequiredOf(schema));
    }

    private static string[] RequiredOf(JsonElement schema) =>
        schema.TryGetProperty("required", out var req) && req.ValueKind == JsonValueKind.Array
            ? req.EnumerateArray().Select(e => e.GetString()!).ToArray()
            : Array.Empty<string>();
}

/// <summary>Shared, well-formed ShowPlanXML fixtures (the same minimal shape the viewer's plan tests
/// round-trip through the store). Kept in one place so the ungated parse test and the gated fetch
/// tests use identical text.</summary>
internal static class DarlingPlanFixtures
{
    public const string SamplePlanXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" />" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    public const string NewerPlanXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 2\" StatementId=\"1\" StatementType=\"SELECT\" />" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the five plan tools. Registers a sentinel server the
/// worker's way, plants stored plans in query_stats / procedure_stats / query_store_stats (with and
/// without a plan, across two databases), then calls the tool methods and asserts: the raw fetch
/// (get_plan_xml) returns the exact stored XML with newest-wins ordering; the optional database_name /
/// plan_id keys refine the fetch; each analyze_* tool returns its source-tagged analysis envelope; an
/// absent key returns the "unavailable" miss envelope; and an unknown server name returns the listing
/// error. Shares the serialized "live-postgres" collection and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPlanToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-plan-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string QueryHash = "0xE2EQUERYHASH01";
    private const string SqlHandle = "0xE2ESQLHANDLE01";
    private const string Db1 = "StackOverflow";
    private const string Db2 = "AdventureWorks";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task PlanTools_FetchAndAnalyzeStoredPlans_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-tools test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection);

            var older = TruncateToSeconds(DateTime.UtcNow).AddHours(-3);
            var newer = older.AddHours(1);

            /* Same query_hash in two databases: Db1's plan is older, Db2's is newer — the coarse
               (hash-only) read returns Db2's newest plan, the database-scoped read returns Db1's. */
            await InsertQueryStatsAsync(connection, older, Db1, QueryHash, DarlingPlanFixtures.SamplePlanXml);
            await InsertQueryStatsAsync(connection, newer, Db2, QueryHash, DarlingPlanFixtures.NewerPlanXml);

            /* A procedure plan keyed by sql_handle. */
            await InsertProcedureStatsAsync(connection, newer, Db1, "dbo", "usp_Widget", SqlHandle, DarlingPlanFixtures.SamplePlanXml);

            /* Query Store: query_id 42 has a plan on plan_id 7 and NO plan on plan_id 9. */
            await InsertQueryStoreAsync(connection, newer, Db1, queryId: 42, planId: 7, planText: DarlingPlanFixtures.SamplePlanXml);
            await InsertQueryStoreAsync(connection, newer, Db1, queryId: 42, planId: 9, planText: null);

            /* ---- get_plan_xml: raw stored XML, newest-wins, with the optional database filter. */
            var rawNewest = await DarlingMcpPlanTools.GetPlanXml(postgres, QueryHash, ServerName);
            Assert.Equal(DarlingPlanFixtures.NewerPlanXml, rawNewest);          /* Db2 newest across the hash */

            var rawDb1 = await DarlingMcpPlanTools.GetPlanXml(postgres, QueryHash, ServerName, Db1);
            Assert.Equal(DarlingPlanFixtures.SamplePlanXml, rawDb1);            /* database_name pins Db1 */

            var rawMiss = await DarlingMcpPlanTools.GetPlanXml(postgres, "0xNOSUCHHASH", ServerName);
            Assert.Equal("unavailable", StatusOf(rawMiss));

            /* ---- analyze_query_plan: the source-tagged analysis envelope. */
            var q = await DarlingMcpPlanTools.AnalyzeQueryPlan(postgres, QueryHash, ServerName);
            AssertAnalysisEnvelope(q, "query_stats");

            var qDb1 = await DarlingMcpPlanTools.AnalyzeQueryPlan(postgres, QueryHash, ServerName, Db1);
            AssertAnalysisEnvelope(qDb1, "query_stats");

            var qMiss = await DarlingMcpPlanTools.AnalyzeQueryPlan(postgres, "0xNOSUCHHASH", ServerName);
            Assert.Equal("unavailable", StatusOf(qMiss));

            /* ---- analyze_procedure_plan: by sql_handle. */
            var p = await DarlingMcpPlanTools.AnalyzeProcedurePlan(postgres, SqlHandle, ServerName);
            AssertAnalysisEnvelope(p, "procedure_stats");

            var pMiss = await DarlingMcpPlanTools.AnalyzeProcedurePlan(postgres, "0xNOSUCHHANDLE", ServerName);
            Assert.Equal("unavailable", StatusOf(pMiss));

            /* ---- analyze_query_store_plan: by (db, query_id), with the optional plan_id. */
            var qs = await DarlingMcpPlanTools.AnalyzeQueryStorePlan(postgres, Db1, 42, ServerName);
            AssertAnalysisEnvelope(qs, "query_store");

            var qsPlan7 = await DarlingMcpPlanTools.AnalyzeQueryStorePlan(postgres, Db1, 42, ServerName, 7);
            AssertAnalysisEnvelope(qsPlan7, "query_store");

            /* plan_id 9 exists but its plan text is NULL -> unavailable; an absent plan_id likewise. */
            var qsPlan9 = await DarlingMcpPlanTools.AnalyzeQueryStorePlan(postgres, Db1, 42, ServerName, 9);
            Assert.Equal("unavailable", StatusOf(qsPlan9));
            var qsPlan999 = await DarlingMcpPlanTools.AnalyzeQueryStorePlan(postgres, Db1, 42, ServerName, 999);
            Assert.Equal("unavailable", StatusOf(qsPlan999));

            /* ---- server resolution flows through the tool: an unknown name returns the listing error. */
            var unknown = await DarlingMcpPlanTools.AnalyzeQueryPlan(postgres, QueryHash, "darling-mcp-no-such-server");
            Assert.StartsWith("Could not resolve server.", McpHelpers.ErrorMessageOf(unknown), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static void AssertAnalysisEnvelope(string json, string expectedSource)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(expectedSource, root.GetProperty("source").GetString());
        Assert.Equal(ServerName, root.GetProperty("server").GetString());
        Assert.True(root.TryGetProperty("statement_count", out _));
    }

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET
    server_name = EXCLUDED.server_name,
    display_name = EXCLUDED.display_name,
    is_enabled = TRUE,
    modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /* Payload columns are nullable (PgSchemaGenerator emits only the framework columns NOT NULL), so
       each insert supplies just the identity + plan columns the reads key on. */

    private static async Task InsertQueryStatsAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string databaseName, string queryHash, string? planXml)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(queryHash);
        command.Parameters.AddWithValue((object?)planXml ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertProcedureStatsAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string databaseName, string schemaName,
        string objectName, string sqlHandle, string? planXml)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(schemaName);
        command.Parameters.AddWithValue(objectName);
        command.Parameters.AddWithValue(sqlHandle);
        command.Parameters.AddWithValue((object?)planXml ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertQueryStoreAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string databaseName,
        long queryId, long planId, string? planText)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_plan_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue((object?)planText ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
