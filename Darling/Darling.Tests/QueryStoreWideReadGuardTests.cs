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
[Trait("Stage", "Guard")]
public sealed class QueryStoreWideReadGuardTests
{
    private static readonly DateTime Start = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheLimits_AreTheFieldColdRate_At75PercentOfTheStatementTimeout()
    {
        /* S4: 8,457,483 rows cold in 55.2 s, 6.53 us per row. The budget is 45 s, 75% of the 60 s timeout (three cold runs differed by 14%). */
        const double coldPerRow = 55.215d / 8_457_483d;
        const double budget = 60d * 0.75d;
        Assert.Equal("60s", ComposeLimits.StatementTimeout);

        var oneScan = budget / coldPerRow;
        Assert.InRange(ComposeLimits.MaxQueryStoreWideRows, oneScan * 0.98d, oneScan);
        Assert.Equal(6_800_000L, ComposeLimits.MaxQueryStoreWideRows);

        /* Two scans: 6.53 cold first scan + 0.13 hash aggregate + 1.54 warm second scan = 8.19 us per row. */
        var twoScans = budget / ((6.53d + 0.13d + 1.54d) / 1_000_000d);
        Assert.InRange(ComposeLimits.MaxQueryStoreWideRowsTwoScans, twoScans * 0.98d, twoScans);
        Assert.Equal(5_400_000L, ComposeLimits.MaxQueryStoreWideRowsTwoScans);

        /* The old limit, 9.4 million, takes 61.4 s cold at the field rate: over the timeout. */
        Assert.True(9_400_000d * coldPerRow > 60d, "the old limit is over the timeout at the field rate");
        Assert.True(ComposeLimits.MaxQueryStoreWideRows * coldPerRow < budget);
    }

    [Fact]
    public void TheWeightAndTheBaseRowBound_AreThePinnedFigures()
    {
        Assert.Equal(0.3d, ComposeLimits.StampRowWeight);
        Assert.Equal(1_000_000L, ComposeLimits.MaxSingleScanBaseRows);

        /* 436 bytes of temp per base row against the 1 GB limit: the bound is 43% of it, and 3 million rows would pass it. */
        Assert.True(ComposeLimits.MaxSingleScanBaseRows * 436L < 1_000_000_000L * 0.5d);
        Assert.True(100L * 30_000L * 436L > 1_000_000_000L);
    }

    [Fact]
    public void OverTheLimit_RefusesWithTheCountServersAndWindow()
    {
        var message = QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(12_000_000, 43), Start, Start.AddDays(1));

        Assert.Equal(
            "This panel needs about 12 million Query Store rows (43 servers, 1 day), over the limit of 6.8 million. Choose fewer servers or a shorter window.",
            message);
    }

    [Fact]
    public void TheEstimate_ScalesWithTheCountedRange_NotTheWindowTheCallerAsked()
    {
        var estimate = new QueryStoreWideReadGuard.Estimate(3_000_000, 10);

        Assert.Null(QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(2)));
        Assert.Equal(
            "This panel needs about 9 million Query Store rows (10 servers, 3 days), over the limit of 6.8 million. Choose fewer servers or a shorter window.",
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

        /* The fleet day that took 55 s cold on the large store is refused by the one-scan limit (before the stamp covers it). */
        Assert.NotNull(QueryStoreWideReadGuard.Refusal(new QueryStoreWideReadGuard.Estimate(8_457_483, 43), Start, Start.AddDays(1)));
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
    public void ARankedTimeSeriesPanelThatScansTwice_GetsTheTwoScanLimit_AndTheMessageNamesIt()
    {
        Assert.Equal(ComposeLimits.MaxQueryStoreWideRows, QueryStoreWideReadGuard.LimitFor(false));
        Assert.Equal(ComposeLimits.MaxQueryStoreWideRowsTwoScans, QueryStoreWideReadGuard.LimitFor(true));
        var estimate = new QueryStoreWideReadGuard.Estimate(6_000_000, 43);
        Assert.Null(QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(1), QueryStoreWideReadGuard.LimitFor(false)));
        Assert.Equal(
            "This panel needs about 6 million Query Store rows (43 servers, 1 day), over the limit of 5.4 million. Choose fewer servers or a shorter window.",
            QueryStoreWideReadGuard.Refusal(estimate, Start, Start.AddDays(1), QueryStoreWideReadGuard.LimitFor(true)));
    }

    /* A fleet day after the build: 8.46 million wide rows in the day, the newest 2.5 hours not yet stamped. The first 21.5 hours
       count at 0.3 and the tail at 1.0. */
    private static readonly QueryStoreWideReadGuard.Estimate FleetDay = new(8_457_483, 43);
    private static readonly DateTime StampedThrough = Start.AddHours(21.5d);

    [Fact]
    public void AFleetDayWithATail_CountsTheStampedHoursAtTheWeight_AndPassesBothLimits()
    {
        var rows = FleetDay.RowsOver(Start, Start.AddDays(1), StampedThrough);
        /* 2.5/24 x 8.46 M = 0.88 M wide + 0.3 x 7.57 M = 3.15 M, as the design derived it. */
        Assert.InRange(rows, 3_100_000L, 3_200_000L);
        Assert.Null(QueryStoreWideReadGuard.Refusal(FleetDay, Start, Start.AddDays(1), QueryStoreWideReadGuard.LimitFor(false), StampedThrough));
        Assert.Null(QueryStoreWideReadGuard.Refusal(FleetDay, Start, Start.AddDays(1), QueryStoreWideReadGuard.LimitFor(true), StampedThrough));
    }

    [Fact]
    public void TwoStampedDays_PassOneScanAndFailTwo_AndThreeDaysFailBoth()
    {
        /* Two days with the same 2.5 h tail: 5.7 million equivalent rows. */
        var twoDays = Start.AddDays(2);
        var throughTwo = twoDays.AddHours(-2.5d);
        Assert.InRange(FleetDay.RowsOver(Start, twoDays, throughTwo), 5_600_000L, 5_800_000L);
        Assert.Null(QueryStoreWideReadGuard.Refusal(FleetDay, Start, twoDays, QueryStoreWideReadGuard.LimitFor(false), throughTwo));
        Assert.NotNull(QueryStoreWideReadGuard.Refusal(FleetDay, Start, twoDays, QueryStoreWideReadGuard.LimitFor(true), throughTwo));

        /* Three days: 8.2 million, refused by both. */
        var threeDays = Start.AddDays(3);
        var throughThree = threeDays.AddHours(-2.5d);
        Assert.InRange(FleetDay.RowsOver(Start, threeDays, throughThree), 8_100_000L, 8_300_000L);
        Assert.NotNull(QueryStoreWideReadGuard.Refusal(FleetDay, Start, threeDays, QueryStoreWideReadGuard.LimitFor(false), throughThree));
        Assert.NotNull(QueryStoreWideReadGuard.Refusal(FleetDay, Start, threeDays, QueryStoreWideReadGuard.LimitFor(true), throughThree));
    }

    [Fact]
    public void TheStampThrough_IsClamped_AndNoStampThroughIsTodaysFigure()
    {
        var end = Start.AddDays(1);
        var unweighted = FleetDay.RowsOver(Start, end);
        Assert.Equal(unweighted, FleetDay.RowsOver(Start, end, null));
        Assert.Equal(unweighted, FleetDay.RowsOver(Start, end, Start));                    /* nothing stamped */
        Assert.Equal(unweighted, FleetDay.RowsOver(Start, end, Start.AddHours(-5)));       /* before the range */
        var allStamped = FleetDay.RowsOver(Start, end, end);
        Assert.Equal(FleetDay.RowsOver(Start, end, end.AddDays(3)), allStamped);           /* past the range */
        Assert.InRange(allStamped, (long)(unweighted * 0.3d) - 1, (long)(unweighted * 0.3d) + 2);
    }

    [Fact]
    public void AWeightedRefusal_NeverCallsTheFigureRows_AndNamesTheServersTheWindowAndWhatToChange()
    {
        var through = Start.AddDays(3).AddHours(-2.5d);
        var message = QueryStoreWideReadGuard.Refusal(FleetDay, Start, Start.AddDays(3), QueryStoreWideReadGuard.LimitFor(false), through)!;
        Assert.Equal(
            "This panel reads too much Query Store history to finish inside the time limit (43 servers, 3 days). Choose fewer servers or a shorter window.",
            message);
        Assert.DoesNotContain("rows", message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("million", message, StringComparison.OrdinalIgnoreCase);
        foreach (var sentence in message.Split(". ", StringSplitOptions.RemoveEmptyEntries))
        {
            Assert.True(sentence.Split(' ').Length <= 20, "keep each sentence short: " + sentence);
        }
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
