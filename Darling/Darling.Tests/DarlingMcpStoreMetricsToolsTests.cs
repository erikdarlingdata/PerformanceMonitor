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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the store self-metrics MCP slice (#2068). The surface is exactly <c>get_store_metrics</c> (static,
/// [McpServerToolType], Task&lt;string&gt;) with the one windowing knob — no <c>server_name</c>, because
/// the monitoring store itself is the subject. Both reads are Postgres-dialect over
/// <c>collect.store_metrics</c>: the latest snapshot per object and the settled LAST-sample-per-day series.
/// The derivable number the issue called out — the per-server daily ingest rate — is a pure computation,
/// pinned here without a store: deltas between settled days, divided by THAT day's enabled-server count,
/// null (never zero or infinity) when the denominator is missing, and negative deltas preserved because
/// retention drops and compression passes genuinely shrink the store.
/// </summary>
public sealed class DarlingMcpStoreMetricsToolsTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpStoreMetricsTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyGetStoreMetrics()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .ToArray();

        Assert.Equal(new[] { "get_store_metrics" }, names);
        Assert.NotNull(typeof(DarlingMcpStoreMetricsTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_DaysBackOnly_Default30_CeilingIsTheSweepsRetention()
    {
        var method = ToolMethods().Single();
        var mcpParams = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name, p.HasDefaultValue, p.DefaultValue))
            .ToArray();

        /* Deliberately NO server_name: the store is the subject, not a monitored server. */
        Assert.Equal(new[] { "days_back" }, mcpParams.Select(p => p.Name).ToArray());
        Assert.Equal(30, mcpParams.Single().DefaultValue);

        /* The window ceiling is the series' own retention — past it there is nothing to read, and the two
           numbers drifting apart would let a caller ask for days the sweep deliberately deleted. */
        Assert.Equal(StoreSelfMetrics.RetentionDays, DarlingMcpStoreMetricsTools.MaxDaysBack);
    }

    [Fact]
    public void StoreMetricsLatestSql_NewestRowPerObject()
    {
        var sql = DarlingStoreMetricsReader.StoreMetricsLatestSql;

        /* DISTINCT ON with a newest-first tiebreak — one settled row per (kind, name). */
        Assert.Contains("SELECT DISTINCT ON (object_kind, object_name)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.store_metrics", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY object_kind, object_name, metric_time DESC", sql, StringComparison.Ordinal);

        /* The forecasting columns ride along. */
        Assert.Contains("compressed_before_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("compressed_after_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("enabled_server_count", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StoreMetricsDailySql_LastSamplePerObjectPerDay_Windowed()
    {
        var sql = DarlingStoreMetricsReader.StoreMetricsDailySql;

        /* The settled-point pin: the LAST sample of each object per day, not 24 near-duplicates — the
           grain a growth question wants. */
        Assert.Contains("SELECT DISTINCT ON (object_kind, object_name, date_trunc('day', metric_time))", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE metric_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains(
            "ORDER BY object_kind, object_name, date_trunc('day', metric_time), metric_time DESC",
            sql,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingStoreMetricsReader.StoreMetricsLatestSql))]
    [InlineData(nameof(DarlingStoreMetricsReader.StoreMetricsDailySql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms_NoBareNow(string sqlName)
    {
        var sql = (string)typeof(DarlingStoreMetricsReader).GetField(sqlName, BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("[", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The daily series is a LAST-SNAPSHOT selection, so the description has to say so (#3119). An
    /// operator asking a maximum question of it — this job's longest run that day, whether it entered its
    /// warning band — gets a confident wrong answer, because the day's peak is dropped rather than
    /// smoothed, and nothing else on the surface says so.
    ///
    /// <para><b>Asserted TOGETHER with the shipped SQL rather than on its own.</b> The caveat is true only
    /// while the read is a <c>DISTINCT ON</c> with a newest-first tiebreak; a read that became
    /// max-preserving would make the sentence wrong in the other direction. Requiring both means either
    /// half moving lands here and the pairing gets re-decided, instead of the sentence outliving the query
    /// it describes.</para>
    ///
    /// <para><b>One ordered match rather than three substrings, and deliberately strict.</b> A description
    /// that said "last snapshot" about some other field and "maximum" about a third would satisfy three
    /// independent <c>Contains</c> calls while telling a reader nothing about the daily series. Strictness
    /// here fails toward re-deciding the sentence: a rewording that still carries the claim goes red and
    /// gets re-approved, where a looser match would let a rewording that DROPPED it pass.</para>
    /// </summary>
    [Fact]
    public void TheDescription_SaysADailyPointIsALastSnapshotAndNotAMaximum()
    {
        var description = ToolMethods().Single().GetCustomAttribute<DescriptionAttribute>()?.Description;
        Assert.NotNull(description);

        /* The selection the caveat is about. Both halves: DISTINCT ON alone would keep an arbitrary row,
           and it is the newest-first tiebreak that makes the kept row the day's LAST. */
        var sql = DarlingStoreMetricsReader.StoreMetricsDailySql;
        Assert.Contains(
            "DISTINCT ON (object_kind, object_name, date_trunc('day', metric_time))",
            sql,
            StringComparison.Ordinal);
        Assert.Contains("metric_time DESC", sql, StringComparison.Ordinal);

        /* The claim, in one match: the daily point, what it IS, and what it is not. */
        Assert.Matches(@"daily point is that day's LAST snapshot[^.]*maximum", description!);

        /* And the route that answers what this series cannot, so the caveat leaves a reader somewhere to
           go rather than only telling them to distrust the number in front of them. */
        Assert.Contains("timescaledb_information.job_history", description!, StringComparison.Ordinal);
    }

    [Fact]
    public void GetStoreMetrics_IsInTheServerInstructions()
    {
        /* The instructions body is how an agent discovers the tool exists — the get_ag_health precedent. */
        Assert.Contains("get_store_metrics", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    /* ---------------- the pure per-server ingest computation ---------------- */

    private static DarlingStoreMetricsReader.StoreMetricDailyPoint StoreDay(
        int day, long? totalBytes, int? servers) => new(
            "store", "darling", new DateTime(2026, 8, day, 0, 0, 0, DateTimeKind.Unspecified),
            totalBytes, null, null, null, null, servers);

    [Fact]
    public void ComputeDailyGrowth_DeltasBetweenSettledDays_DividedByThatDaysServerCount()
    {
        var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 100_000_000_000, 52),
            StoreDay(2, 102_300_000_000, 52),
            StoreDay(3, 104_600_000_000, 50),
        });

        /* Two deltas from three days — the first day has no predecessor and yields no point. */
        Assert.Equal(2, growth.Count);

        Assert.Equal(new DateTime(2026, 8, 2), growth[0].Day);
        Assert.Equal(2_300_000_000, growth[0].DeltaBytes);
        Assert.Equal(2_300_000_000 / 52.0, growth[0].PerServerBytes);

        /* The denominator is the day BEING MEASURED's server count, not the baseline day's. */
        Assert.Equal(2_300_000_000 / 50.0, growth[1].PerServerBytes);
    }

    [Fact]
    public void ComputeDailyGrowth_MissingDenominator_IsNull_NeverZeroOrInfinity()
    {
        var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 100, 0),
            StoreDay(2, 150, 0),
            StoreDay(3, 175, null),
        });

        Assert.Equal(2, growth.Count);
        Assert.All(growth, g => Assert.Null(g.PerServerBytes));
        /* The byte delta itself still reports — only the rate needs the denominator. */
        Assert.Equal(50, growth[0].DeltaBytes);
    }

    [Fact]
    public void ComputeDailyGrowth_UnrecordedTotals_AreSkipped_And_NegativeDeltasSurvive()
    {
        var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 200, 10),
            StoreDay(2, null, 10),
            StoreDay(3, 180, 10),
        });

        /* Day 2 recorded no total: neither its delta nor day 3's (whose predecessor is the hole) is
           invented. Nothing else survives — and nothing throws. */
        Assert.Empty(growth);

        /* Shrinkage is real (retention drops, compression passes) and must not be hidden: a forecast
           extrapolating only the positive days would overstate growth. */
        var shrinking = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 200, 10),
            StoreDay(2, 180, 10),
        });
        Assert.Equal(-20, shrinking.Single().DeltaBytes);
        Assert.Equal(-2.0, shrinking.Single().PerServerBytes);
    }

    [Fact]
    public void ComputeDailyGrowth_EmptyAndSingleDay_YieldNothing()
    {
        Assert.Empty(DarlingStoreMetricsReader.ComputeDailyGrowth(Array.Empty<DarlingStoreMetricsReader.StoreMetricDailyPoint>()));
        Assert.Empty(DarlingStoreMetricsReader.ComputeDailyGrowth(new[] { StoreDay(1, 100, 5) }));
    }
}
