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
using System.Threading;
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
/// Pins get_query_store_regressions (#2484) — the viewer's Query Store Regressions tab, the only tab in the
/// per-server page that was entirely unreachable rather than merely reduced.
///
/// <para>The SQL pins matter more here than on most reads. The regression percent is computed from AVERAGES
/// over CUMULATIVE per-interval snapshots, and the baseline arm is UNBOUNDED while the recent arm is a short
/// window — so the two arms have systematically different re-collection density per interval, and losing the
/// dedup moves the numbers and the 25% gate in a direction that has nothing to do with the query. The gate,
/// the ranking and the severity bands are pinned for the same reason: they are the Dashboard TVF's, and a
/// browser that disagreed with the desktop viewer about which queries regressed would be worse than one that
/// showed nothing.</para>
/// </summary>
public sealed class DarlingQueryStoreRegressionsSurfaceAndSqlTests
{
    private static readonly string[] ToolSurface =
    {
        "get_query_store_regressions",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpQueryStoreRegressionTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IsExactlyTheRegressionRead()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpQueryStoreRegressionTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_AllOptional_MatchesLite()
    {
        var method = ToolMethods().Single();
        var p = method.GetParameters()
            .Where(x => x.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(x => (x.Name!, x.HasDefaultValue))
            .ToArray();

        Assert.Equal(new[] { "server_name", "hours_back", "database_name", "limit", "full_text", "as_of" }, p.Select(x => x.Item1).ToArray());
        Assert.All(p, x => Assert.True(x.Item2, $"{x.Item1} must be optional"));
    }

    /// <summary>
    /// The dedup is correctness, not performance. Losing it makes the averages an avg-of-avgs weighted by
    /// how often each interval happened to be re-collected — and the baseline arm is unbounded while the
    /// recent arm is a short window, so the weighting differs systematically between the two sides being
    /// compared. That manufactures and hides regressions on its own.
    /// </summary>
    [Fact]
    public void RegressionsSql_DedupsBothArms_OnTheFullIntervalIdentity()
    {
        var sql = DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql;

        /* Both arms, not one: deduping only the recent side compares a deduped number to an inflated one. */
        Assert.Equal(2, CountOf(sql, "ROW_NUMBER() OVER"));
        Assert.Equal(2, CountOf(sql, "PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role"));
        Assert.Equal(2, CountOf(sql, "WHERE rn = 1"));
        Assert.Equal(2, CountOf(sql, "ORDER BY collection_time DESC, execution_count DESC"));
    }

    /// <summary>
    /// The baseline is everything BEFORE the window and the recent side is the window — the two must not
    /// overlap and must not leave a gap, or a row is either counted on both sides of its own comparison or
    /// dropped from it.
    /// </summary>
    [Fact]
    public void RegressionsSql_SplitsBaselineFromRecent_OnTheSameBoundary()
    {
        var sql = DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql;
        /* #4217/#4310: the baseline has a FIXED lower bound too, not "everything before the window" — this
           is the assertion that fails against the pre-#4217 shape (the grid's twin,
           ViewerQueryStoreRegressionsTests.RegressionsSql_SplitsBaselineBeforeWindow_RecentInWindow_OverTheBaseTable,
           already carries it for the viewer's copy; this reader's own copy was uncovered). */
        Assert.Contains("collection_time >= $6", sql, StringComparison.Ordinal);  /* baseline: window start minus BaselineLookbackDays */
        Assert.Contains("collection_time < $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_store_stats", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4310: the grid keeps its own copy of <c>BaselineLookbackDays</c> (it cannot reference this project)
    /// and pins it equal to 7 in <c>ViewerQueryStoreRegressionsTests.BaselineLookbackDays_IsSevenDays_SameAsTheMcpReaderAndLite</c>.
    /// This reader's OWN constant had no such pin — nothing stopped the two copies drifting apart, since
    /// #4217's fixed-baseline lower bound above depends on this constant's value at the call site rather
    /// than in the SQL text.
    /// </summary>
    [Fact]
    public void BaselineLookbackDays_IsSevenDays_SameAsTheViewerAndLite()
    {
        Assert.Equal(7, DarlingQueryStoreRegressionReader.BaselineLookbackDays);
    }

    /// <summary>
    /// The Dashboard TVF's gate, ranking and severity bands, kept verbatim. A browser that disagreed with
    /// the desktop viewer about WHICH queries regressed would be worse than one that showed nothing.
    /// </summary>
    [Fact]
    public void RegressionsSql_KeepsTheTvfGate_RankingAndSeverityBands()
    {
        var sql = DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql;

        /* CPU-only > 25% is the TVF's single-metric gate; duration drives the severity band. */
        Assert.Contains("WHERE (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) > 25", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY additional_duration_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("(r.avg_duration_ms - b.avg_duration_ms) * r.exec_count AS additional_duration_ms", sql, StringComparison.Ordinal);

        foreach (var band in new[] { "'CRITICAL'", "'HIGH'", "'MEDIUM'", "'LOW'" })
            Assert.Contains(band, sql, StringComparison.Ordinal);

        /* Every percent divides through NULLIF, so a zero baseline yields NULL rather than a division error. */
        Assert.Equal(7, CountOf(sql, "NULLIF("));

        /* The cap is the one deliberate change from the viewer, and it is a bound parameter rather than the
           viewer's hardcoded 50 — the gate and the ranking above are what keep the two surfaces agreeing. */
        Assert.Contains("LIMIT $5", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The coverage probe reads the SAME table the regression read reads, and answers BOTH questions in one
    /// round trip. Splitting it would let the two answers describe different instants.
    /// </summary>
    [Fact]
    public void CoverageSql_ProbesTheSameTable_ForBaselineAndRecent()
    {
        var sql = DarlingQueryStoreRegressionReader.RegressionCoverageSql;
        Assert.Equal(2, CountOf(sql, "FROM query_store_stats"));
        Assert.DoesNotContain("v_query_store_stats", sql, StringComparison.Ordinal);
        /* #4310: the coverage probe's own baseline arm must use the SAME fixed lower bound as the read it
           probes for — an unbounded coverage probe over a bounded read would answer "has_baseline" for
           history the read itself can no longer see, and report the no-baseline branch (#4195/#4217) as
           false whenever it is in fact true. */
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);  /* baseline: window start minus BaselineLookbackDays */
        Assert.Contains("collection_time < $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(sql, "EXISTS ("));
    }

    /// <summary>
    /// #5015: the coverage probe takes the read's database filter ($5), in both of its questions, so a filter that matched
    /// nothing can be told from one that matched and found nothing wrong.
    /// </summary>
    [Fact]
    public void CoverageSql_TakesTheReadsDatabaseFilter_InBothQuestions()
    {
        var sql = DarlingQueryStoreRegressionReader.RegressionCoverageSql;
        Assert.Equal(2, CountOf(sql, "$5::text[] IS NULL OR database_name = ANY($5)"));
    }

    [Theory]
    [InlineData(nameof(DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql))]
    [InlineData(nameof(DarlingQueryStoreRegressionReader.RegressionCoverageSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = (string)typeof(DarlingQueryStoreRegressionReader).GetField(sqlName)!.GetValue(null)!;
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("decompress(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTable()
    {
        var store = PgSchemaGenerator.CreateTable(QueryStoreCollector.Instance);
        Assert.Equal("query_store_stats", QueryStoreCollector.Instance.TargetTable);
        foreach (var col in new[]
                 {
                     "database_name", "query_id", "plan_id", "execution_count", "avg_duration_us",
                     "avg_cpu_time_us", "avg_logical_io_reads", "runtime_stats_interval_id",
                     "first_execution_time", "execution_type_desc", "last_execution_time", "query_text",
                 })
        {
            Assert.Contains(col, store, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpQueryStoreRegressionTools>();
        using var provider = services.BuildServiceProvider();
        var tools = provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();

        Assert.Equal(ToolSurface.Length, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(tools[0].InputSchema));
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var i = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (i >= 0)
        {
            count++;
            i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal);
        }
        return count;
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for get_query_store_regressions.
///
/// <para>The four empty branches carry the weight. Only ONE of them is good news, and the other three look
/// identical to it in a bare empty array — the worst being a server whose entire collected history sits
/// inside the requested window, which has no BEFORE and therefore cannot show a regression however badly it
/// regressed. Answering "no regressions" there is a confident wrong answer rather than a missing one.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingQueryStoreRegressionsLiveTests
{
    private const int ServerId = -949556;
    private const string ServerName = "query-store-regressions";
    private const string Db = "AppDb";
    private const string OtherDb = "OtherDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task FourKindsOfNothing_AndARealRegression_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live regressions test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* ── 1. nothing ever collected ── */
            var never = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName));
            Assert.Equal("unavailable", never.GetProperty("status").GetString());
            var neverText = never.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);
            Assert.Contains("Query Store may be OFF", neverText, StringComparison.Ordinal);

            /*
                ── 2. a baseline exists but nothing landed in the window ──
                Seeded FIRST, and the ordering is load-bearing: once a row exists 30 minutes ago no window
                a caller can legally ask for excludes it, so this branch is unreachable after the recent
                row is planted. The four states are walked by moving hours_back over one fixed set of rows
                rather than by deleting rows between assertions.
            */
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);

            var noRecent = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 2));
            Assert.Equal("empty", noRecent.GetProperty("status").GetString());
            var noRecentText = noRecent.GetProperty("message").GetString()!;
            Assert.Contains("Widen hours_back", noRecentText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", noRecentText, StringComparison.Ordinal);

            /* ── 3. both sides collected, nothing regressed: the ONE good-news answer ── */
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1);

            var clear = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));
            Assert.Equal("empty", clear.GetProperty("status").GetString());
            var clearText = clear.GetProperty("message").GetString()!;
            /* #4966: the baseline reaches 40 hours back, past the 24-hour window's start but not seven days, so the window is cut and the all-clear gives way. */
            AssertCutAllClear(clear, clearText);
            Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);
            Assert.DoesNotContain("Widen", clearText, StringComparison.Ordinal);

            /*
                ── 4. recent rows but NO baseline ──
                The branch this read exists to get right, reached from the SAME rows by widening the window
                until every one of them falls inside it. There is then nothing to compare against, and no
                regression is detectable however bad one is.
            */
            var noBaseline = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 48));
            Assert.Equal("unavailable", noBaseline.GetProperty("status").GetString());
            var noBaselineText = noBaseline.GetProperty("message").GetString()!;
            Assert.Contains("no baseline", noBaselineText, StringComparison.Ordinal);
            Assert.Contains("NOT a clean bill of health", noBaselineText, StringComparison.Ordinal);

            /* #4195: the baseline is now a FIXED lookback (BaselineLookbackDays), not "everything before
               hours_back" — so neither widening nor shortening hours_back changes how far back the baseline
               reaches, and the old "Shorten hours_back" advice (correct for the unbounded baseline, where a
               shorter recent window left more history in the baseline arm) no longer applies and was removed. */
            Assert.Contains("baseline lookback", noBaselineText, StringComparison.Ordinal);
            Assert.DoesNotContain("Shorten hours_back", noBaselineText, StringComparison.Ordinal);
            Assert.DoesNotContain("Widen", noBaselineText, StringComparison.Ordinal);
            Assert.NotEqual(neverText, noBaselineText);

            /* All four sentences are different sentences, not one sentence four times. */
            var messages = new[] { neverText, noBaselineText, noRecentText, clearText };
            Assert.Equal(4, messages.Distinct(StringComparer.Ordinal).Count());

            /*
                ── a real regression ──
                Query 7 doubles its CPU and duration between the baseline and the window. The baseline
                interval is ALSO re-collected with a higher cumulative count, which is what the dedup has to
                survive: un-deduped, the baseline average is dragged by counting one interval twice.
            */
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 50, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3, queryId: 7);
            await SeedAsync(connection, ct, baseNow.AddHours(-39), 90, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3, queryId: 7);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 200, avgDurationUs: 4000, avgCpuUs: 4000, intervalId: 4, queryId: 7);

            var hit = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));
            Assert.Equal(ServerName, hit.GetProperty("server").GetString());
            Assert.Equal(1, hit.GetProperty("regression_count").GetInt32());
            Assert.False(hit.GetProperty("truncated").GetBoolean());

            var row = hit.GetProperty("regressions")[0];
            Assert.Equal(7, row.GetProperty("query_id").GetInt64());
            Assert.Equal(Db, row.GetProperty("database_name").GetString());

            /* 1 ms -> 4 ms is +300%, which is the CRITICAL band (> 100%). */
            Assert.Equal("CRITICAL", row.GetProperty("severity").GetString());
            Assert.Equal(1.0, row.GetProperty("baseline_duration_ms").GetDouble(), 6);
            Assert.Equal(4.0, row.GetProperty("recent_duration_ms").GetDouble(), 6);
            Assert.Equal(300.0, row.GetProperty("duration_regression_percent").GetDouble(), 6);

            /*
                The ranking key: 3 ms per execution across the 200 recent executions. If the dedup had
                failed, the baseline exec_count would be 140 (50 + 90) rather than 90 and the baseline
                averages would be an avg-of-avgs over two snapshots of one interval.
            */
            Assert.Equal(600.0, row.GetProperty("additional_duration_ms").GetDouble(), 6);
            Assert.Equal(90, row.GetProperty("baseline_exec_count").GetInt64());
            Assert.Equal(200, row.GetProperty("recent_exec_count").GetInt64());

            /* ── the cap REFUSES out of range rather than quietly rewriting it ── */
            var refused = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, null, 5000);
            Assert.Contains("exceeds maximum of 1000", refused, StringComparison.Ordinal);

            /* ── truncation is OBSERVED, not inferred from the row count ── */
            var exact = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, null, 1));
            Assert.Equal(1, exact.GetProperty("regression_count").GetInt32());
            Assert.False(exact.GetProperty("truncated").GetBoolean());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Runs <paramref name="body"/> against a registered, empty server and removes what it seeded. The seeded
    /// instants are offsets from the current second, never a fixed date: the data-start probe reads the clock for the
    /// purge edge, so a fixed past anchor would fall wholly outside the store's coverage.</summary>
    private async Task WithServerAsync(Func<NpgsqlConnection, NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live regressions test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The truncated-baseline sentence Lite adds to its note, which this tool copies.</summary>
    private const string BaselineTail = "Here the window starts at baseline_start, so the baseline holds only the part from effective_start to baseline_end.";

    /// <summary>
    /// #4966: the store's first Query Store row comes 40 hours back, inside the 7-day baseline, so the baseline holds only part
    /// of what <c>baseline_start</c> names. The answer says so: truncated, <c>effective_start</c> is that first row (exact: the
    /// table is raw, so its coverage starts at its oldest row), the note names the table and ends with the baseline sentence.
    /// </summary>
    [Fact]
    public async Task ARegression_WhoseBaselineStartsInsideTheRange_SaysWhereTheDataStarts_AgainstDevPostgres()
    {
        await WithServerAsync(async (connection, postgres, baseNow, ct) =>
        {
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 90, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3, queryId: 7);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 200, avgDurationUs: 4000, avgCpuUs: 4000, intervalId: 4, queryId: 7);

            var hit = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));

            Assert.Equal(1, hit.GetProperty("regression_count").GetInt32());
            Assert.True(hit.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(baseNow.AddHours(-40)), hit.GetProperty("effective_start").GetString());
            Assert.NotEqual(hit.GetProperty("baseline_start").GetString(), hit.GetProperty("effective_start").GetString());
            var note = hit.GetProperty("truncation_note").GetString()!;
            Assert.Contains("raw query_store_stats retains", note, StringComparison.Ordinal);
            Assert.EndsWith(BaselineTail, note, StringComparison.Ordinal);
            Assert.False(hit.TryGetProperty("effective_hours_back", out _));

            var names = hit.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });
    }

    /// <summary>
    /// #4966: a quiet start. The store holds a row from before the baseline begins (nine days back, for a baseline that
    /// starts eight days back), so the baseline is covered whenever the rows inside it begin: false, no note, and
    /// <c>effective_start</c> is exactly <c>baseline_start</c>.
    /// </summary>
    [Fact]
    public async Task ARegression_WhoseTableReachesBeforeTheBaseline_SaysNothing_AgainstDevPostgres()
    {
        await WithServerAsync(async (connection, postgres, baseNow, ct) =>
        {
            await SeedAsync(connection, ct, baseNow.AddDays(-9), 10, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2, queryId: 9);
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 90, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3, queryId: 7);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 200, avgDurationUs: 4000, avgCpuUs: 4000, intervalId: 4, queryId: 7);

            var hit = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));

            Assert.Equal(1, hit.GetProperty("regression_count").GetInt32());
            Assert.False(hit.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, hit.GetProperty("truncation_note").ValueKind);
            Assert.Equal(hit.GetProperty("baseline_start").GetString(), hit.GetProperty("effective_start").GetString());
        });
    }

    /// <summary>
    /// #4966: a failed data-start probe costs the notice, never the answer. The regression rows come back with the three keys
    /// absent, and an empty answer carries no hints.
    /// </summary>
    [Fact]
    public async Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres()
    {
        await WithServerAsync(async (connection, postgres, baseNow, ct) =>
        {
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 90, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3, queryId: 7);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 200, avgDurationUs: 4000, avgCpuUs: 4000, intervalId: 4, queryId: 7);

            var bodySucceeded = false;
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");
            try
            {
                var hit = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));

                Assert.False(hit.TryGetProperty("status", out _));
                Assert.False(hit.TryGetProperty("error", out _));
                Assert.Equal(1, hit.GetProperty("regression_count").GetInt32());
                Assert.False(hit.TryGetProperty("effective_start", out _));
                Assert.False(hit.TryGetProperty("window_truncated", out _));
                Assert.False(hit.TryGetProperty("truncation_note", out _));

                /* Take the recent row away: the recent side is missing, so the answer is empty. */
                await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1 AND collection_time > $2", ServerId, DarlingMcpTestData.Naive(baseNow.AddHours(-2)));

                var empty = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 2));
                Assert.Equal("empty", empty.GetProperty("status").GetString());
                Assert.False(empty.TryGetProperty("hints", out _));
                bodySucceeded = true;
            }
            finally
            {
                await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
                {
                    DarlingMcpWindowNotice.TestOnlyProbe = null;
                    return Task.CompletedTask;
                });
            }
        });
    }

    /// <summary>
    /// #4966: every <c>empty</c> answer carries the three keys under <c>hints</c>, the all-clear included (a clean comparison
    /// over a baseline the store only partly holds is not a true negative), and every <c>unavailable</c> answer carries none.
    /// </summary>
    [Fact]
    public async Task TheEmptyAnswers_CarryTheDataStartHints_AndTheUnavailableOnesStayBare_AgainstDevPostgres()
    {
        await WithServerAsync(async (connection, postgres, baseNow, ct) =>
        {
            var never = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName));
            Assert.Equal("unavailable", never.GetProperty("status").GetString());
            Assert.False(never.TryGetProperty("hints", out _));

            await SeedAsync(connection, ct, baseNow.AddHours(-40), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);

            /* Nothing in the last two hours: the recent side is missing. The baseline still starts 40 hours back, inside it. */
            var noRecent = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 2));
            Assert.Equal("empty", noRecent.GetProperty("status").GetString());
            var noRecentHints = noRecent.GetProperty("hints");
            Assert.True(noRecentHints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(baseNow.AddHours(-40)), noRecentHints.GetProperty("effective_start").GetString());
            Assert.EndsWith(BaselineTail, noRecentHints.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);

            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1);

            var clear = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));
            Assert.Equal("empty", clear.GetProperty("status").GetString());
            AssertCutAllClear(clear, clear.GetProperty("message").GetString()!);
            var clearHints = clear.GetProperty("hints");
            Assert.True(clearHints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(baseNow.AddHours(-40)), clearHints.GetProperty("effective_start").GetString());

            /* No baseline: the whole history sits inside the window. The unavailable answer stays bare. */
            var noBaseline = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 48));
            Assert.Equal("unavailable", noBaseline.GetProperty("status").GetString());
            Assert.False(noBaseline.TryGetProperty("hints", out _));
        });
    }

    /// <summary>
    /// #5015: a database_name filter that matches nothing in the baseline or the window says the filter matched nothing, not
    /// "this IS the all-clear" (the server-wide probe saw both sides collected). A filter that matches, and finds nothing
    /// wrong, is still the all-clear.
    /// </summary>
    [Fact]
    public async Task AFilteredMiss_SaysTheFilterMatchedNothing_NotTheAllClear_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live regressions test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1);

            var miss = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, "NoSuchDb"));
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            var missText = miss.GetProperty("message").GetString()!;
            Assert.Contains("database_name 'NoSuchDb'", missText, StringComparison.Ordinal);
            Assert.Contains("the filter matched nothing and this is NOT the all-clear", missText, StringComparison.Ordinal);
            Assert.DoesNotContain("this IS the all-clear", missText, StringComparison.Ordinal);

            var match = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, Db));
            /* #4966: this fixture's baseline is cut (40 hours of the seven days), so the matching filter's nothing is the cut sentence. */
            AssertCutAllClear(match, match.GetProperty("message").GetString()!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5015: a filter whose database has captures in the baseline and none in the window is a missing recent side FOR THAT
    /// DATABASE, not the all-clear: the server-wide probe saw both sides (another database wrote the window), and no query of
    /// the filtered database was compared. It words the server-wide "nothing collected IN the last N hour(s)" answer,
    /// naming the database.
    /// </summary>
    [Fact]
    public async Task AFilteredDatabase_WithBaselineCapturesOnly_SaysItsWindowIsMissing_NotTheAllClear_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live regressions test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1, queryId: 2, database: OtherDb);

            var noRecent = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, Db));
            Assert.Equal("empty", noRecent.GetProperty("status").GetString());
            var noRecentText = noRecent.GetProperty("message").GetString()!;
            Assert.Contains("database_name 'AppDb'", noRecentText, StringComparison.Ordinal);
            Assert.Contains("nothing of it collected IN the last 24 hour(s)", noRecentText, StringComparison.Ordinal);
            Assert.Contains("NOT the all-clear", noRecentText, StringComparison.Ordinal);
            Assert.DoesNotContain("this IS the all-clear", noRecentText, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5015: a filter whose database has captures in the window and none in the baseline has nothing to compare against FOR
    /// THAT DATABASE: <c>unavailable</c>, worded like the server-wide no-baseline answer and naming the database, not the
    /// all-clear.
    /// </summary>
    [Fact]
    public async Task AFilteredDatabase_WithWindowCapturesOnly_SaysItHasNoBaseline_NotTheAllClear_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live regressions test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, ct, baseNow.AddHours(-40), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2, queryId: 2, database: OtherDb);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 100, avgDurationUs: 3000, avgCpuUs: 3000, intervalId: 1);

            var noBaseline = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, Db));
            Assert.Equal("unavailable", noBaseline.GetProperty("status").GetString());
            var noBaselineText = noBaseline.GetProperty("message").GetString()!;
            Assert.Contains("database_name 'AppDb'", noBaselineText, StringComparison.Ordinal);
            Assert.Contains("no baseline", noBaselineText, StringComparison.Ordinal);
            Assert.Contains("NOT a clean bill of health", noBaselineText, StringComparison.Ordinal);
            Assert.DoesNotContain("this IS the all-clear", noBaselineText, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #4966: the same all-clear over a baseline the store covers in full (a capture older than the seven-day lookback) keeps
    /// its "this IS the all-clear" sentence, and its hints say the window is not cut.
    /// </summary>
    [Fact]
    public async Task TheAllClear_OverACoveredBaseline_KeepsItsSentence_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live regressions test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, ct, baseNow.AddDays(-9), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 3);
            await SeedAsync(connection, ct, baseNow.AddDays(-3), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1);

            var clear = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24));
            Assert.Equal("empty", clear.GetProperty("status").GetString());
            Assert.False(clear.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
            Assert.Contains("this IS the all-clear", clear.GetProperty("message").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #4966: the all-clear over a cut baseline keeps its factual first sentence (the 25% threshold included) and swaps only the
    /// claim for the cut sentence its hints select: the one that points at <c>effective_start</c> when the notice names one.
    /// </summary>
    private static void AssertCutAllClear(JsonElement answer, string message)
    {
        Assert.True(answer.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
        Assert.StartsWith("No query on ", message, StringComparison.Ordinal);
        Assert.Contains("more than 25% worse than its baseline", message, StringComparison.Ordinal);
        /* Every fixture here has a baseline that starts 40 hours back, so the notice names a start: the CutWindowNothingMessage variant. */
        Assert.NotEqual(System.Text.Json.JsonValueKind.Null, answer.GetProperty("hints").GetProperty("effective_start").ValueKind);
        Assert.EndsWith(". " + McpHelpers.CutWindowNothingMessage, message, StringComparison.Ordinal);
        Assert.DoesNotContain("this IS the all-clear", message, StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    /// <summary>One Query Store interval of a query in <paramref name="database"/> (<see cref="Db"/> unless a test needs a second database to filter on).</summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long executions,
        long avgDurationUs, long avgCpuUs, long intervalId, long queryId = 1, string database = Db) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            database, queryId, 9L, "Regular", executions, avgDurationUs, avgCpuUs, 100L, intervalId,
            "SELECT * FROM dbo.Widgets", DarlingMcpTestData.Naive(collectionTime));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
