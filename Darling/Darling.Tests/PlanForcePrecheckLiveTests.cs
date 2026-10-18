/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 step A2: get_plan_force_precheck over seeded store rows. Each case plants one query's Query Store rows, its text and its
/// stored plans, reads the tool, and asserts the verdict and the stable refusal codes. as_of is fixed so no clock decides the window.
/// </summary>
[Collection("live-postgres")]
public sealed class PlanForcePrecheckLiveTests
{
    private const string ServerName = "darling-force-precheck";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "ForceDb";

    private static readonly DateTime Anchor = DateTime.SpecifyKind(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-6), DateTimeKind.Unspecified);
    private static readonly DateTime AsOf = Anchor.AddHours(1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Plan(string? value = null, string extra = "", string stmtAttrs = "", string? queryPlanAttrs = null) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\"" + stmtAttrs + ">" + extra
        + "<QueryPlan CachedPlanSize=\"16\"" + queryPlanAttrs + ">"
        + (value is null ? "" : "<ParameterList><ColumnReference Column=\"@p\" ParameterDataType=\"int\" ParameterCompiledValue=\"" + value + "\" ParameterRuntimeValue=\"" + value + "\"/></ParameterList>")
        + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    private sealed record Seed(long QueryId, string Text, (long PlanId, long Rows, long Execs, int LastExecMinutesBeforeAnchor, string Xml, bool Forced)[] Plans);

    private static async Task<JsonDocument> RunAsync(Seed seed, Func<NpgsqlConnection, CancellationToken, Task>? extra = null, long? planId = null, long? compare = null)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pre-check test.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var ok = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await Exec(connection, ct, "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1,$2,$3,$4,$5)", ServerId, Db, seed.QueryId, seed.Text, Anchor);
            foreach (var p in seed.Plans)
            {
                var last = Anchor.AddMinutes(-p.LastExecMinutesBeforeAnchor);
                await Exec(connection, ct, @"INSERT INTO query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
 interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_rowcount, is_forced_plan, force_failure_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,'Regular',$7,$8,$8,$9,$10,5000,7000,$11,$12,0)",
                    CollectionIdGenerator.Next(), Anchor, ServerId, ServerName, Db, seed.QueryId, p.PlanId, last.AddHours(-1), last, p.Execs, p.Rows, p.Forced);
                var stored = p.Xml + "<!-- plan " + p.PlanId.ToString(CultureInfo.InvariantCulture) + " -->";
                var digest = PayloadDimensions.Digest(stored);
                await using (var dim = new NpgsqlCommand("INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", connection))
                {
                    dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
                    dim.Parameters.AddWithValue(stored);
                    dim.Parameters.AddWithValue(NpgsqlDbType.Timestamp, Anchor);
                    await dim.ExecuteNonQueryAsync(ct);
                }

                await using var map = new NpgsqlCommand("INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1,$2,$3,$4,$5,$6)", connection);
                map.Parameters.AddWithValue(ServerId);
                map.Parameters.AddWithValue(Db);
                map.Parameters.AddWithValue(p.PlanId);
                map.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
                map.Parameters.AddWithValue("0x5632");
                map.Parameters.AddWithValue(NpgsqlDbType.Timestamp, Anchor);
                await map.ExecuteNonQueryAsync(ct);
            }

            if (extra is not null) await extra(connection, ct);
            var json = await DarlingMcpPlanForcePrecheckTools.GetPlanForcePrecheck(
                postgres, Db, seed.QueryId, planId ?? seed.Plans[0].PlanId, ServerName, compare, 24, AsOf.ToString("o", CultureInfo.InvariantCulture), cancellationToken: ct);
            var doc = JsonDocument.Parse(json);
            ok = true;
            return doc;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, ok, async (c, t) => await DeleteAsync(c, t));
        }
    }

    private static Task Exec(NpgsqlConnection c, CancellationToken ct, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, ct, sql, values.Select(v => v is DateTime d ? DateTime.SpecifyKind(d, DateTimeKind.Unspecified) : v).ToArray());

    private static string[] Codes(JsonDocument d) =>
        d.RootElement.GetProperty("refusals").EnumerateArray().Select(r => r.GetProperty("code").GetString()!).ToArray();

    private static string[] UnknownChecks(JsonDocument d) =>
        d.RootElement.GetProperty("unknown").EnumerateArray().Select(r => r.GetProperty("check").GetString()!).ToArray();

    /* A clean query: two plans of about the same output, both well run, both recent, no hint, no temp table. The candidate is the cheap, older plan. */
    private static Seed Clean(long q = 7001, string text = "SELECT * FROM dbo.widgets WHERE id = @p") =>
        new(q, text, [(1, 100, 60, 30, Plan("1"), false), (2, 120, 60, 5, Plan("1"), false)]);

    [Fact]
    public async Task ACleanCandidate_HasNoRefusal_ButTheVerdictNeverSaysSafe()
    {
        using var doc = await RunAsync(Clean());
        var codes = Codes(doc);
        /* The store holds no force state, so the gate's state_unavailable is the one refusal; every check this read owns is quiet. */
        Assert.DoesNotContain(codes, c => c is "recompile_hint" or "temp_table" or "not_run_recently" or "too_few_executions" or "psp_variant" or "output_differs" or "query_store_read_only");
        Assert.Contains("query_store_read_only", UnknownChecks(doc));
        Assert.DoesNotContain("safe", doc.RootElement.GetProperty("verdict").GetString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Pattern1_AStatementWithOptionRecompile_IsRefused()
    {
        using var doc = await RunAsync(Clean(7002, "SELECT * FROM dbo.widgets WHERE id = @p OPTION (RECOMPILE)"));
        Assert.Contains("recompile_hint", Codes(doc));
        Assert.Equal("refuse", doc.RootElement.GetProperty("verdict").GetString());
    }

    [Fact]
    public async Task Pattern2_PlansReturning26000AndFiftyRowsPerCall_AreRefusedAsOutputDiffers()
    {
        var seed = new Seed(7003, "SELECT 1", [(1, 50, 60, 30, Plan("1"), false), (2, 26_000, 60, 5, Plan("1"), false)]);
        using var doc = await RunAsync(seed);
        Assert.Contains("output_differs", Codes(doc));
    }

    [Fact]
    public async Task ABlockerFromTheExistingGate_AppearsUnderItsOwnCode()
    {
        var seed = new Seed(7004, "SELECT 1", [(1, 100, 60, 30, Plan("1"), true), (2, 100, 60, 5, Plan("1"), false)]);
        using var doc = await RunAsync(seed);
        Assert.Contains("already_forced", Codes(doc));
    }

    [Fact]
    public async Task ARunStoppedQuery_AndAThinPlan_AreRefused()
    {
        var seed = new Seed(7005, "SELECT 1", [(1, 100, 3, 300, Plan("1"), false), (2, 100, 60, 240, Plan("1"), false)]);
        using var doc = await RunAsync(seed);
        var codes = Codes(doc);
        Assert.Contains("not_run_recently", codes);
        Assert.Contains("too_few_executions", codes);
    }

    [Fact]
    public async Task DifferentCompiledValues_AreShown_NeverRefused()
    {
        var seed = new Seed(7006, "SELECT 1", [(1, 100, 60, 30, Plan("111"), false), (2, 100, 60, 5, Plan("222"), false)]);
        using var doc = await RunAsync(seed);
        Assert.DoesNotContain(Codes(doc), c => c is "output_differs" or "recompile_hint" or "psp_variant");
        var raw = doc.RootElement.GetRawText();
        Assert.Contains("\"compiled_value\":\"111\"", raw, StringComparison.Ordinal);
        Assert.Contains("\"compiled_value\":\"222\"", raw, StringComparison.Ordinal);
        Assert.Equal(PerformanceMonitor.Common.McpDataDetailMasker.ReplacedValue,
            System.Text.Json.Nodes.JsonNode.Parse(McpDataDetailMasker.MaskJson(raw, McpDataDetail.MaskLiterals))!["shown"]!["plans"]![0]!["compiled_values"]![0]!["compiled_value"]!.GetValue<string>());
        var meta = McpDataDetailMasker.MaskJson(raw, McpDataDetail.MetadataOnly);
        Assert.DoesNotContain("111", meta, StringComparison.Ordinal);
        Assert.DoesNotContain("222", meta, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATempTable_AndAPspVariant_AreRefused_AndATableVariableIsNot()
    {
        var temp = Plan("1", "<RelOp NodeId=\"0\"><Object Database=\"[ForceDb]\" Schema=\"[dbo]\" Table=\"[#orders]\"/></RelOp>");
        var tableVar = Plan("1", "<RelOp NodeId=\"0\"><Object Database=\"[ForceDb]\" Schema=\"[dbo]\" Table=\"[@orders]\"/></RelOp>");
        using (var d1 = await RunAsync(new Seed(7007, "SELECT 1", [(1, 100, 60, 30, temp, false), (2, 100, 60, 5, Plan("1"), false)])))
            Assert.Contains("temp_table", Codes(d1));
        using (var d2 = await RunAsync(new Seed(7008, "SELECT 1", [(1, 100, 60, 30, tableVar, false), (2, 100, 60, 5, Plan("1"), false)])))
            Assert.DoesNotContain("temp_table", Codes(d2));
        using var d3 = await RunAsync(new Seed(7009, "SELECT 1", [(1, 100, 60, 30, Plan("1", queryPlanAttrs: " QueryVariantID=\"2\""), false), (2, 100, 60, 5, Plan("1"), false)]));
        Assert.Contains("psp_variant", Codes(d3));
        Assert.Contains("psp_variant_siblings", UnknownChecks(d3));
    }

    [Fact]
    public async Task AReadOnlyQueryStore_IsRefused_AndNoHealthRowIsUnknown()
    {
        using var ro = await RunAsync(Clean(7010), async (c, ct) => await Exec(c, ct,
            @"INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason)
              VALUES ($1,$2,$3,$4,$5,'READ_ONLY','READ_WRITE',65536)", CollectionIdGenerator.Next(), Anchor, ServerId, ServerName, Db));
        Assert.Contains("query_store_read_only", Codes(ro));
        using var none = await RunAsync(Clean(7011));
        Assert.DoesNotContain("query_store_read_only", Codes(none));
        Assert.Contains("query_store_read_only", UnknownChecks(none));
    }

    private static async Task DeleteAsync(NpgsqlConnection c, CancellationToken ct)
    {
        foreach (var table in new[] { "query_store_stats", "collect.query_store_text", "collect.query_store_health", "collect.query_store_plan_map" })
            await Exec(c, ct, "DELETE FROM " + table + " WHERE server_id = $1", ServerId);
        await Exec(c, ct, "DELETE FROM query_plan_dim WHERE last_seen = $1 AND query_plan_xml LIKE '%<!-- plan %'", Anchor);
        await Exec(c, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
