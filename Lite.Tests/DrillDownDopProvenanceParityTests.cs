/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of the cross-SKU text guard for #3648. The <c>top_cpu_queries</c> and <c>bad_actor_query</c> reads are
/// one text, <c>PerformanceMonitor.Common.DrillDownSql</c> (#8470), that both apps run: Darling's
/// <c>DrillDownSqlTests</c> holds its constants to it, and this class watches what Lite's two drill-down methods hand
/// DuckDB when they run and holds that to it, so a method that runs a copy of its own, however close, fails here. It also
/// owns Lite's own census: the text carries the per-plan provenance shape rather than the old hash-folded
/// <c>MAX(max_dop)</c>, and a row with no DOP reading comes out of both running readers as null, never 0.
///
/// <para><b>Why not read Darling's SQL from here.</b> The <c>lite</c> filter does not reach
/// <c>Darling/PerformanceMonitor.Darling.Analysis</c>, and <c>CrossAppGuardCiGateTests</c> fails any Lite
/// guard whose source read PR CI cannot run (#2839): a guard that compares the two apps cannot live behind a
/// filter that fires for only one of them. Both apps hold their own text to the Common constant instead.</para>
/// </summary>
public sealed class DrillDownDopProvenanceParityTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 84700;
    private const string ServerName = "DrillDownSqlSrv";
    private const string Hash = "0x8470HASH";

    private static readonly DateTime WindowEnd = new(2026, 8, 18, 14, 30, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);

    private readonly DuckDbInitializer _duckDb;

    public DrillDownDopProvenanceParityTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static string CommonText(string name) => name switch
    {
        nameof(DrillDownSql.TopCpuQueries) => DrillDownSql.TopCpuQueries,
        nameof(DrillDownSql.BadActorDetail) => DrillDownSql.BadActorDetail,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "not a DrillDownSql constant"),
    };

    [Theory]
    [InlineData("CPU_SQL_PERCENT", "top_cpu_queries", nameof(DrillDownSql.TopCpuQueries))]
    [InlineData("BAD_ACTOR_" + Hash, "bad_actor_query", nameof(DrillDownSql.BadActorDetail))]
    public async Task TheDrillDown_RunsTheCommonText_AsTheArchivePlanRewritesIt(string factKey, string drillDown, string constant)
    {
        /* What the method hands DuckDB, taken at the moment it runs. The archive plan may swap v_query_stats for the
           hot table first, so the expectation is the Common constant through the same plan over the same window; with
           nothing archived that is the constant itself. Anything else the method runs, an inline copy differing by one
           character included, fails here. */
        string? executed = null;
        var collector = new DrillDownCollector(_duckDb)
        {
            StatementObserver = (key, sql) =>
            {
                if (key == drillDown) executed = sql;
            },
        };

        var finding = new AnalysisFinding
        {
            RootFactKey = factKey,
            StoryPath = factKey,
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = [factKey],
            /* Past the 0.5 display gate — below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await collector.EnrichFindingsAsync([finding], Context());

        Assert.True(executed is not null, $"{drillDown} did not run");
        var plan = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", WindowStart, CancellationToken.None);
        Assert.Equal(plan.Apply(CommonText(constant)), executed);
    }

    [Theory]
    [InlineData(nameof(DrillDownSql.TopCpuQueries))]
    [InlineData(nameof(DrillDownSql.BadActorDetail))]
    public void TheDrillDownSql_CarriesThePerPlanProvenanceShape_NotTheHashFoldedMaximum(string constant)
    {
        var sql = CommonText(constant);

        /* The headline is the NEWEST plan's reading, picked by a newest-first ranking with the tie-breakers
           spelled out (compile time, then CPU spent) and their null placement made explicit — DuckDB and
           Postgres default DESC null placement differently, and an implicit default here would make the two
           SKUs pick different rows on the same data. */
        Assert.Matches(
            new Regex(@"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY database_name, query_hash\s+ORDER BY collection_time DESC, creation_time DESC NULLS LAST, delta_worker_time DESC NULLS LAST\s*\)\s+AS newest_rn", RegexOptions.Singleline),
            sql);
        Assert.Contains("MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop", sql, StringComparison.Ordinal);

        /* The history: how many plans the group spans, the cross-plan maximum, and when it was last seen. */
        Assert.Contains("COUNT(DISTINCT query_plan_hash) AS plan_count", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop_any_plan) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN max_dop = max_dop_any_plan THEN collection_time END) AS max_dop_any_plan_last_seen", sql, StringComparison.Ordinal);

        /* And the lie itself is gone: no bare hash-folded MAX(max_dop) projected as max_dop. */
        Assert.DoesNotContain("MAX(max_dop) AS max_dop", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("CPU_SQL_PERCENT", "top_cpu_queries")]
    [InlineData("BAD_ACTOR_" + Hash, "bad_actor_query")]
    public async Task AnUnknownDop_ArrivesAsNull_NotZero_FromBothDrillDowns(string factKey, string drillDown)
    {
        /* The DMV never reports 0 — a serial plan is 1 — so a 0 on a DOP field is "no reading" rendered as a
           degree of parallelism. One row inside the window with no max_dop: each reader projects null on every
           DOP field, and the note composed from those four values is null too, nothing invented from nothing. */
        await SeedRowWithoutDopAsync();

        var finding = NewFinding(factKey);
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue(drillDown, out var raw), $"{drillDown} was not collected");
        var section = JsonSerializer.SerializeToElement(raw);
        var row = section.ValueKind == JsonValueKind.Array ? Assert.Single(section.EnumerateArray()) : section;

        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop_any_plan").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("max_dop_any_plan_last_seen").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("dop_note").ValueKind);
    }

    private static AnalysisFinding NewFinding(string factKey) => new()
    {
        RootFactKey = factKey,
        StoryPath = factKey,
        /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
        PathKeys = [factKey],
        /* Past the 0.5 display gate — below it the expensive drill-downs are skipped wholesale. */
        Severity = 1.0,
    };

    /// <summary>One query_stats row for the server, inside the window, spending CPU, with no max_dop reading.</summary>
    private async Task SeedRowWithoutDopAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, max_dop, min_dop, delta_execution_count, delta_worker_time, delta_spills, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, 10, 400000, 0, $9)";
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddMinutes(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "DrillDownSqlDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Hash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0x8470PLAN" });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT /* no DOP reading */ 1" });
        await cmd.ExecuteNonQueryAsync();
    }

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };
}
