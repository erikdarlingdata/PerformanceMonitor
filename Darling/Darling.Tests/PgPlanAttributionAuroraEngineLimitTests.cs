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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the store test mints its own scratch database through ScratchPostgres. */

/// <summary>
/// #8912: on Aurora the plan capture readiness check for <c>plan_attribution</c> asked the operator to add <c>%Q</c> to <c>log_line_prefix</c>, a change
/// Aurora's parameter groups do not allow (the parameter is not modifiable, or accepts only fixed values, none with <c>%Q</c>). On an Aurora target the
/// check now reports an engine limit, and every surface that shows readiness gives the same answer: the readiness read, the plan read's message, the
/// Viewer panel and its grid. On vanilla PostgreSQL the check still asks for the prefix change.
///
/// <para>The collector statement runs on a real PostgreSQL with the settings it judges swapped for literals, as the sibling readiness classes do, because
/// the facets cannot otherwise be reached on a server that has not preloaded auto_explain. It only reads settings and catalogs. The classes skip without
/// <c>DARLING_TEST_PG</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgPlanAttributionAuroraEngineLimitTests
{
    private const string AuroraPg15Prefix = "%t:%r:%u@%d:[%p]:";
    private const string AuroraPg17OtherPrefix = "%m:%r:%u@%d:[%p]:%l:%e:%s:%v:%x:%c:%q%a:";
    private const string ServerName = "aurora-plan-attribution-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime Now = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Sql(bool aurora, string linePrefix, string logDestination = "stderr", string preload = "pg_stat_statements,auto_explain")
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = new DateTime(2026, 11, 17, 1, 0, 0, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = aurora },
        };

        var text = PgPlanCaptureReadinessCollector.Instance.BuildQuery(context).Text;
        var swaps = new (string From, string To)[]
        {
            ("current_setting('shared_preload_libraries', true)", "'" + preload + "'::text"),
            ("current_setting('auto_explain.log_min_duration', true)", "'10'::text"),
            ("current_setting('auto_explain.log_format', true)", "'json'::text"),
            ("current_setting('log_line_prefix', true)", "'" + linePrefix + "'::text"),
            ("current_setting('log_destination', true)", "'" + logDestination + "'::text"),
            ("current_setting('lc_messages', true)", "'C'::text"),
        };

        foreach (var (from, to) in swaps)
        {
            Assert.Contains(from, text, StringComparison.Ordinal);
            text = text.Replace(from, to, StringComparison.Ordinal);
        }

        return text;
    }

    private static async Task<List<(string Facet, bool Satisfied, string Observed, string Detail)>> RunCollectorSqlAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<(string, bool, string, string)>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetBoolean(1), reader.GetString(2), reader.GetString(3)));
        }

        return rows;
    }

    private static async Task<(bool Satisfied, string Observed, string Detail)> PlanAttributionAsync(bool aurora, string prefix, string logDestination = "stderr")
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a throwaway PostgreSQL to run the live readiness test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        var rows = await RunCollectorSqlAsync(connection, Sql(aurora, prefix, logDestination), ct);
        var row = rows.Single(r => r.Facet == "plan_attribution");
        return (row.Satisfied, row.Observed, row.Detail);
    }

    [Theory]
    [InlineData(AuroraPg15Prefix)]
    [InlineData(AuroraPg17OtherPrefix)]
    [InlineData("%m [%p] %q%u@%d %Q ")]
    public async Task OnAnAuroraTarget_PlanAttributionIsAnEngineLimit_AndNeverAsksForALogLinePrefixChange(string prefix)
    {
        var (satisfied, observed, detail) = await PlanAttributionAsync(aurora: true, prefix);

        Assert.False(satisfied);
        Assert.Equal(prefix, observed);
        Assert.StartsWith(PgPlanCaptureReadinessCollector.AuroraPlanAttributionLimitPrefix, detail, StringComparison.Ordinal);
        Assert.Contains("Nothing to change in log_line_prefix", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("Add %Q", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("Put %Q", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("needs no restart", detail, StringComparison.Ordinal);
        Assert.True(DarlingPgPlanCaptureReadinessReader.IsEngineLimitRow("plan_attribution", satisfied, detail));
    }

    [Theory]
    [InlineData(AuroraPg15Prefix)]
    [InlineData("%m [%p] ")]
    public async Task OnVanillaPostgres_AMissingQueryIdStaysAPrecondition_WithTodaysRemedy(string prefix)
    {
        var (satisfied, _, detail) = await PlanAttributionAsync(aurora: false, prefix);

        Assert.False(satisfied);
        Assert.StartsWith("log_line_prefix does NOT carry %Q.", detail, StringComparison.Ordinal);
        Assert.Contains("Add %Q directly after [%p]", detail, StringComparison.Ordinal);
        Assert.Contains("needs no restart", detail, StringComparison.Ordinal);
        Assert.False(DarlingPgPlanCaptureReadinessReader.IsEngineLimitRow("plan_attribution", satisfied, detail));
    }

    /// <summary>The engine limit is for the prefix the operator cannot change. An Aurora target that logs to csvlog on PostgreSQL 14 or later takes the
    /// query id from the record's own column, so the check is met there and nothing about an engine limit is said.</summary>
    [Fact]
    public async Task AnAuroraTargetThatLogsToCsvlog_IsStillMet_AndSaysNothingAboutAnEngineLimit()
    {
        var (satisfied, _, detail) = await PlanAttributionAsync(aurora: true, AuroraPg15Prefix, logDestination: "csvlog");

        Assert.True(satisfied);
        Assert.DoesNotContain("Engine limit", detail, StringComparison.Ordinal);
        Assert.False(DarlingPgPlanCaptureReadinessReader.IsEngineLimitRow("plan_attribution", satisfied, detail));
    }

    /// <summary>The evidence behind the sentence "the stderr plan reader stores no plan at all from a prefix like this one": the same plan block, written
    /// under each Aurora prefix, yields nothing, and under a prefix with <c>%Q</c> yields the plan.</summary>
    [Theory]
    [InlineData("2026-11-17 10:00:00 UTC:[local]:app@db:[81]:")]
    [InlineData("2026-11-17 10:00:00.123 UTC:[local]:app@db:[81]:1:00000:2026-11-17 10:00:00 UTC:5/3:0:psql:")]
    public void UnderAnAuroraPrefix_TheStderrPlanReaderStoresNoPlan_ButUnderAQueryIdPrefixItDoes(string linePrefixText)
    {
        const string block = "LOG:  duration: 12.3 ms  plan:\n\t{\n\t  \"Query Text\": \"select 1\",\n\t  \"Plan\": {\n\t    \"Node Type\": \"Result\"\n\t  }\n\t}\n";

        Assert.Empty(PgPlanLogParser.Extract(linePrefixText + block, AuroraPg15Prefix));
        Assert.Empty(PgPlanLogParser.Extract(linePrefixText + block, AuroraPg17OtherPrefix));

        var withQueryId = PgPlanLogParser.Extract("2026-11-17 10:00:00 UTC:[local]:app@db:[81]:424242 " + block, "%t:%r:%u@%d:[%p]:%Q ");
        Assert.Equal(424242, Assert.Single(withQueryId).QueryId);
    }

    private static DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow Row(string facet, bool satisfied, string detail) =>
        new(facet, satisfied, "observed", detail, Now);

    private static readonly string EngineLimitDetail = PgPlanCaptureReadinessCollector.AuroraPlanAttributionLimitPrefix + "Nothing to change in log_line_prefix.";

    [Fact]
    public void TheReadinessJson_NamesTheEngineLimitApart_AndLeavesItOutOfWhatStillNeedsSetup()
    {
        var rows = new List<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow>
        {
            Row("library_loaded", true, "loaded"),
            Row("capture_threshold", false, "threshold"),
            Row("plan_attribution", false, EngineLimitDetail),
        };

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, rows, 25));
        var root = doc.RootElement;

        Assert.Equal(new[] { "capture_threshold" }, root.GetProperty("unsatisfied_facets").EnumerateArray().Select(f => f.GetString()).ToArray());
        Assert.Equal(new[] { "plan_attribution" }, root.GetProperty("engine_limited_facets").EnumerateArray().Select(f => f.GetString()).ToArray());

        var byFacet = root.GetProperty("facets").EnumerateArray().ToDictionary(f => f.GetProperty("facet").GetString()!);
        Assert.True(byFacet["plan_attribution"].GetProperty("engine_limit").GetBoolean());
        Assert.False(byFacet["plan_attribution"].GetProperty("is_satisfied").GetBoolean());
        Assert.False(byFacet["capture_threshold"].GetProperty("engine_limit").GetBoolean());
        Assert.Equal(EngineLimitDetail, byFacet["plan_attribution"].GetProperty("detail").GetString());
    }

    [Fact]
    public void AnUnmetPlanAttributionWithoutTheEngineLimitOpening_StaysInTheUnsatisfiedList()
    {
        var rows = new List<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow>
        {
            Row("plan_attribution", false, "log_line_prefix does NOT carry %Q. Add %Q ..."),
        };

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, rows, 25));
        Assert.Equal(new[] { "plan_attribution" }, doc.RootElement.GetProperty("unsatisfied_facets").EnumerateArray().Select(f => f.GetString()).ToArray());
        Assert.Empty(doc.RootElement.GetProperty("engine_limited_facets").EnumerateArray());
    }

    [Fact]
    public void ThePlanReadsPreconditionMessage_CountsTheEngineLimitOut_AndSaysItOnce()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.PlanCaptureNotReadyStatus(new[] { "capture_threshold = -1" }, engineLimited: true));
        var root = doc.RootElement;

        Assert.Equal("precondition", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("1 unsatisfied precondition", message, StringComparison.Ordinal);
        Assert.Contains("engine limit on Aurora PostgreSQL", message, StringComparison.Ordinal);
        Assert.Equal(1, root.GetProperty("hints").GetProperty("unsatisfied_count").GetInt32());
        Assert.True(root.GetProperty("hints").GetProperty("engine_limited").GetBoolean());

        using var plain = JsonDocument.Parse(DarlingMcpPgPlanTools.PlanCaptureNotReadyStatus(new[] { "capture_threshold = -1" }));
        Assert.DoesNotContain("engine limit", plain.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>The whole chain on a scratch store: the collector's statement on an Aurora target, its rows stored, then the readiness read and the plan
    /// read. Every other facet is met, so the engine limit is the only thing left, and it must not read as a setup step.</summary>
    [Fact]
    public async Task AuroraTarget_ReadinessAndPlanReads_ReportTheEngineLimit_AndVanillaStillReportsAPrecondition()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live store test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Aurora: the one unmet facet is the engine limit. */
            await StoreAsync(connection, await RunCollectorSqlAsync(connection, Sql(aurora: true, AuroraPg15Prefix), ct), ct);

            using (var readiness = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlanCaptureReadiness(postgres, ServerName, 24, 25)))
            {
                var root = readiness.RootElement;
                Assert.Empty(root.GetProperty("unsatisfied_facets").EnumerateArray());
                Assert.Equal(new[] { "plan_attribution" }, root.GetProperty("engine_limited_facets").EnumerateArray().Select(f => f.GetString()).ToArray());
            }

            using (var plans = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(postgres, ServerName, 24, 5)))
            {
                var root = plans.RootElement;
                Assert.Equal("not_collected", root.GetProperty("status").GetString());
                Assert.Contains("engine limit on Aurora PostgreSQL", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
                Assert.Equal("plan_attribution_engine_limit", root.GetProperty("hints").GetProperty("reason").GetString());
            }

            /* Aurora with another facet unmet too: a precondition for that one, counted without the engine limit. */
            await ClearAsync(connection, ct);
            await StoreAsync(connection, await RunCollectorSqlAsync(connection, Sql(aurora: true, AuroraPg15Prefix, preload: "pg_stat_statements"), ct), ct);

            using (var plans = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(postgres, ServerName, 24, 5)))
            {
                var root = plans.RootElement;
                Assert.Equal("precondition", root.GetProperty("status").GetString());
                var hints = root.GetProperty("hints");
                Assert.Equal("plan_capture_not_ready", hints.GetProperty("reason").GetString());
                Assert.True(hints.GetProperty("engine_limited").GetBoolean());
                Assert.DoesNotContain("plan_attribution =", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
            }

            /* Vanilla PostgreSQL with the same prefix: today's precondition, the prefix counted as one thing left to set. */
            await ClearAsync(connection, ct);
            await StoreAsync(connection, await RunCollectorSqlAsync(connection, Sql(aurora: false, AuroraPg15Prefix), ct), ct);

            using (var readiness = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlanCaptureReadiness(postgres, ServerName, 24, 25)))
            {
                var root = readiness.RootElement;
                Assert.Equal(new[] { "plan_attribution" }, root.GetProperty("unsatisfied_facets").EnumerateArray().Select(f => f.GetString()).ToArray());
                Assert.Empty(root.GetProperty("engine_limited_facets").EnumerateArray());
            }

            using (var plans = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(postgres, ServerName, 24, 5)))
            {
                var root = plans.RootElement;
                Assert.Equal("precondition", root.GetProperty("status").GetString());
                Assert.Equal(1, root.GetProperty("hints").GetProperty("unsatisfied_count").GetInt32());
                Assert.Contains("plan_attribution =", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
                Assert.DoesNotContain("engine limit", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ClearAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_plan_capture_readiness WHERE server_id = $1", ServerId);

    private static async Task StoreAsync(
        NpgsqlConnection connection, List<(string Facet, bool Satisfied, string Observed, string Detail)> rows, CancellationToken ct)
    {
        foreach (var row in rows)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_plan_capture_readiness
    (collection_id, collection_time, server_id, server_name, facet, is_satisfied, observed, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2)), ServerId, ServerName, row.Facet, row.Satisfied, row.Observed, row.Detail);
        }
    }

    [Fact]
    public void TheViewerNote_CountsTheEngineLimitOut_AndNamesItOnItsOwn()
    {
        var facets = new (string, bool)[] { ("library_loaded", true), ("plan_attribution", false) };
        var limited = new HashSet<string>(StringComparer.Ordinal) { "plan_attribution" };

        var onlyTheLimit = ViewerServerTab.PlanCaptureReadinessNote(facets, limited);
        Assert.StartsWith("Plans cannot be captured on this server.", onlyTheLimit.Text, StringComparison.Ordinal);
        Assert.Contains("Aurora engine limit", onlyTheLimit.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("Not met:", onlyTheLimit.Text, StringComparison.Ordinal);

        var both = ViewerServerTab.PlanCaptureReadinessNote(new (string, bool)[] { ("capture_threshold", false), ("plan_attribution", false) }, limited);
        Assert.Contains("Not met: a plan capture threshold.", both.Text, StringComparison.Ordinal);
        Assert.Contains("Aurora engine limit", both.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("query ids in the log.", both.Text.Split("Aurora engine limit")[0], StringComparison.Ordinal);

        var vanilla = ViewerServerTab.PlanCaptureReadinessNote(facets);
        Assert.Equal("Plan capture is not ready on this server. Not met: query ids in the log.", vanilla.Text);
    }

    [Fact]
    public void TheViewerGridRow_SaysEngineLimit_NotNo()
    {
        var limit = PgDisplay.PlanCapture(Row("plan_attribution", false, EngineLimitDetail));
        var vanilla = PgDisplay.PlanCapture(Row("plan_attribution", false, "log_line_prefix does NOT carry %Q."));
        var met = PgDisplay.PlanCapture(Row("plan_attribution", true, "ok"));

        Assert.Equal("engine limit", limit.Satisfied);
        Assert.Equal("no", vanilla.Satisfied);
        Assert.Equal("yes", met.Satisfied);
    }
}
