/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582: the refusal of a composed Query Store panel whose wide-table read cannot finish inside the compose statement
/// timeout. The pure half (the estimate arithmetic and the message) first; the live half after it runs the refusal through
/// every path that runs a composed panel: the shared runner (behind the web endpoint) and the MCP tool
/// <c>run_custom_view_panel</c>.
/// </summary>
public sealed class QueryStoreWideReadGuardTests
{
    private static readonly DateTime Start = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheLimit_IsTheObservedColdRate_At90PercentOfTheStatementTimeout()
    {
        /* 8,457,483 rows in 48.3 s, against a 60 s timeout. */
        const double rowsPerSecond = 8_457_483d / 48.3d;
        var derived = rowsPerSecond * 60d * 0.9d;
        Assert.InRange(ComposeLimits.MaxQueryStoreWideRows, derived * 0.99d, derived * 1.01d);
        Assert.Equal("60s", ComposeLimits.StatementTimeout);
        Assert.True(ComposeLimits.MaxQueryStoreWideRows > 8_457_483, "the observed read that finished in 48 s must not be refused");
    }

    [Fact]
    public void OverTheLimit_RefusesWithTheCountServersAndWindow()
    {
        var message = QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(12_000_000, 43), Start, Start.AddDays(1));

        Assert.Equal(
            "This panel needs about 12 million Query Store rows (43 servers, 1 day), over the limit of 9.4 million. Choose fewer servers or a shorter window.",
            message);
    }

    [Fact]
    public void TheEstimate_ScalesWithTheCountedRange_NotTheWindowTheCallerAsked()
    {
        var estimate = new QueryStoreWideReadGuard.Estimate(4_000_000, 10);

        Assert.Null(QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(2)));
        Assert.Equal(
            "This panel needs about 12 million Query Store rows (10 servers, 3 days), over the limit of 9.4 million. Choose fewer servers or a shorter window.",
            QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(3)));

        /* The same window with the counted range cut to the newest day: under the limit, so no refusal. */
        Assert.Null(QueryStoreWideReadGuard.Refusal(estimate, Start.AddDays(2), Start.AddDays(3)));
    }

    [Fact]
    public void AtOrUnderTheLimit_NeverRefuses()
    {
        var atLimit = new QueryStoreWideReadGuard.Estimate(ComposeLimits.MaxQueryStoreWideRows, 5);
        Assert.Null(QueryStoreWideReadGuard.Refusal(atLimit, Start, Start.AddDays(1)));
        Assert.NotNull(QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(ComposeLimits.MaxQueryStoreWideRows + 1, 5), Start, Start.AddDays(1)));

        /* The panel that ran in 48 s on the large store. */
        Assert.Null(QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(8_457_483, 43), Start, Start.AddDays(1)));
    }

    [Fact]
    public void NoEstimate_OrNoServerWithABuiltDay_NeverRefuses()
    {
        Assert.Null(QueryStoreWideReadGuard.Refusal(null, Start, Start.AddDays(30)));
        Assert.Null(QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(0, 0), Start, Start.AddDays(30)));
        Assert.Null(QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(long.MaxValue, 0), Start, Start.AddDays(30)));
    }

    [Theory]
    [InlineData(1, "1 server")]
    [InlineData(2, "2 servers")]
    public void TheServerCount_AgreesInNumber(int servers, string expected)
    {
        var message = QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(ComposeLimits.MaxQueryStoreWideRows * 2, servers), Start, Start.AddDays(1));
        Assert.Contains("(" + expected + ", ", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(12, "12 hours")]
    [InlineData(36, "36 hours")]
    [InlineData(24, "1 day")]
    [InlineData(72, "3 days")]
    public void TheWindow_IsWholeDaysOrHours(int hours, string expected)
    {
        var message = QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(ComposeLimits.MaxQueryStoreWideRows * 10, 3), Start, Start.AddHours(hours));
        Assert.Contains(", " + expected + ")", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEstimateSql_TakesEachServersMostRecentBuiltDay_OfTheServersInScope_DisabledOnesIncluded()
    {
        var sql = QueryStoreWideReadGuard.PerServerRowsSql;
        Assert.Contains("collect.query_store_top_daily_built", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY d.day DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        /* The compiled wide read joins collect.servers with no is_enabled predicate, so the estimate counts a disabled server too. */
        Assert.DoesNotContain("is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("s.server_name = ANY($1)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ARankedTimeSeriesPanelThatScansTwice_GetsHalfTheLimit_AndTheMessageNamesIt()
    {
        Assert.Equal(ComposeLimits.MaxQueryStoreWideRows, QueryStoreWideReadGuard.LimitFor(false));
        Assert.Equal(ComposeLimits.MaxQueryStoreWideRows / 2, QueryStoreWideReadGuard.LimitFor(true));
        var estimate = new QueryStoreWideReadGuard.Estimate(6_000_000, 43);
        Assert.Null(QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(1), QueryStoreWideReadGuard.LimitFor(false)));
        Assert.Equal(
            "This panel needs about 6 million Query Store rows (43 servers, 1 day), over the limit of 4.7 million. Choose fewer servers or a shorter window.",
            QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(1), QueryStoreWideReadGuard.LimitFor(true)));
    }

    [Fact]
    public void TheMessage_ReadsPlainly()
    {
        /* The plain-English checker's rules, pinned on the message the user reads: short sentences, no code words. */
        var message = QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(12_000_000, 43), Start, Start.AddDays(1))!;
        foreach (var sentence in message.Split(". ", StringSplitOptions.RemoveEmptyEntries))
        {
            Assert.True(sentence.Split(' ').Length <= 20, "keep each sentence short: " + sentence);
        }

        Assert.DoesNotContain("wide", message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("collect.", message, StringComparison.Ordinal);
    }
}

/// <summary>
/// #5582, live: the estimate reads the right rows, and the refusal answers on every path that runs a composed panel
/// (the shared runner behind <c>/api/compose/run</c>, and the MCP tool <c>run_custom_view_panel</c>). Seeds the same
/// wide-eligible store <see cref="ComposeQueryStoreWideStartLiveTests"/> builds, then writes the #5094 daily summary's
/// built rows the estimate reads.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreWideReadRefusalLiveTests
{
    private const int ServerId = -5582002;
    private const string ServerName = "qsiw-refusal";
    private const string OtherServerName = "qsiw-refusal-other";
    private const int OtherServerId = -5582003;

    private static readonly DateTime S = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-6), DateTimeKind.Unspecified);

    private const string PanelJson =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    [Fact]
    public async Task Estimate_ReadsTheNewestBuiltDayOfEachEnabledServerInScope()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 estimate live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, OtherServerId, OtherServerName, ct);
        var noBuiltDayId = ServerId - 10;
        await DarlingMcpTestData.RegisterServerAsync(connection, noBuiltDayId, "qsiw-refusal-nobuilt", ct);
        var disabledId = ServerId - 11;
        await DarlingMcpTestData.RegisterServerAsync(connection, disabledId, "qsiw-refusal-disabled", ct);
        await ExecAsync(connection, $"UPDATE collect.servers SET is_enabled = FALSE WHERE server_id = {disabledId}", ct);

        /* Two built days for the first server: only the newest counts. A server with no built day adds nothing. A disabled
           server with one counts, as the compiled wide read takes its retained rows too. */
        await InsertBuiltAsync(connection, ServerId, S.Date.AddDays(1), 1_000, ct);
        await InsertBuiltAsync(connection, ServerId, S.Date.AddDays(3), 700, ct);
        await InsertBuiltAsync(connection, OtherServerId, S.Date.AddDays(2), 50, ct);
        await InsertBuiltAsync(connection, disabledId, S.Date.AddDays(2), 9_999_999, ct);

        var fleet = await QueryStoreWideReadGuard.EstimateAsync(postgres, null, null, ct);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(750 + 9_999_999, 3), fleet);

        var scoped = await QueryStoreWideReadGuard.EstimateAsync(postgres, new[] { ServerName }, null, ct);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(700, 1), scoped);

        var noBuilt = await QueryStoreWideReadGuard.EstimateAsync(postgres, new[] { "qsiw-refusal-nobuilt" }, null, ct);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(0, 0), noBuilt);
        Assert.Null(QueryStoreWideReadGuard.Refusal(noBuilt, S, S.AddDays(30)));

    }

    [Fact]
    public async Task EveryPathThatRunsAComposedPanel_RefusesAnOversizedRead_AndRunsASmallOne()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 refusal live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, S, ct);
        await ExecAsync(connection,
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + $"VALUES ({ServerId}, '{ServerName}', '{ServerName}', TRUE, 16, now(), now()) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, S.AddDays(-60), S.AddDays(4), null, null, ct);
        await ExecAsync(connection,
            $"UPDATE collect.query_store_interval_wide_coverage SET filled_since = TIMESTAMP '{S.AddHours(12):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        var end = (DateTime)(await ScalarAsync(connection,
            $"SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = {ServerId}", ct))!;
        await ExecAsync(connection, $"SELECT drop_chunks('collect.query_store_stats', older_than => TIMESTAMP '{S.AddDays(1):yyyy-MM-dd HH:mm:ss}')", ct);

        var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(postgres, null, S, end, end, ct);
        Assert.True(resolution.Eligible, "the seeded store must route the panel to the wide table, or this test proves nothing");

        /* The window end carries the microseconds applied_through has: a whole-second end can fall before it, and the gate
           then reads raw (the literal-end rule). */
        var spec = new JsonObject
        {
            ["panel"] = JsonNode.Parse(PanelJson),
            ["windowStart"] = S.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture),
            ["windowEnd"] = end.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", System.Globalization.CultureInfo.InvariantCulture),
        };

        /* No built day: the guard fails open and the panel runs, on both paths. */
        await AssertRunsAsync(postgres, spec, ct);

        /* A small built day: still runs. */
        await InsertBuiltAsync(connection, ServerId, S.Date.AddDays(2), 1_000, ct);
        await AssertRunsAsync(postgres, spec, ct);

        /* An oversized built day: refused with the message, on the shared runner and on the MCP tool. */
        await ExecAsync(connection, $"UPDATE collect.query_store_top_daily_built SET source_rows = {ComposeLimits.MaxQueryStoreWideRows * 4} WHERE server_id = {ServerId}", ct);

        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, (JsonObject)spec.DeepClone(), ct);
        Assert.True(outcome.Payload is null, "the oversized read must be refused; the panel ran: " + outcome.Payload?["sql"]);
        Assert.False(outcome.IsServerError);
        Assert.NotNull(outcome.Error);
        Assert.StartsWith("This panel needs about ", outcome.Error, StringComparison.Ordinal);
        Assert.Contains("Query Store rows (1 server, ", outcome.Error, StringComparison.Ordinal);
        Assert.EndsWith("Choose fewer servers or a shorter window.", outcome.Error, StringComparison.Ordinal);

        var tool = await DarlingMcpCustomViewTools.RunCustomViewPanel(postgres, spec.ToJsonString());
        Assert.Contains("\"status\":\"invalid\"", tool.Replace(" ", string.Empty), StringComparison.Ordinal);
        Assert.Contains("This panel needs about ", tool, StringComparison.Ordinal);
        Assert.Contains("Choose fewer servers or a shorter window.", tool, StringComparison.Ordinal);

        /* A scope that leaves out the oversized server is not refused. */
        var scoped = (JsonObject)spec.DeepClone();
        scoped["server"] = "no-such-server-5582";
        var scopedOutcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, scoped, ct);
        Assert.True(scopedOutcome.Error is null || !scopedOutcome.Error.StartsWith("This panel needs about ", StringComparison.Ordinal), "a scope with no built day must not be refused");

    }

    private static async Task AssertRunsAsync(NpgsqlDataSource postgres, JsonObject spec, CancellationToken ct)
    {
        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, (JsonObject)spec.DeepClone(), ct);
        Assert.True(outcome.Payload is not null, "the panel must run: " + outcome.Error);

        var tool = await DarlingMcpCustomViewTools.RunCustomViewPanel(postgres, spec.ToJsonString());
        Assert.DoesNotContain("This panel needs about", tool, StringComparison.Ordinal);
    }

    private static Task InsertBuiltAsync(NpgsqlConnection connection, int serverId, DateTime day, long sourceRows, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
            + $"VALUES ({serverId}, DATE '{day:yyyy-MM-dd}', 2, now() AT TIME ZONE 'UTC', {sourceRows})", ct);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
