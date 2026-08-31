/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The ungated (no-live-store) contract for the Custom Views v2 backend (#1563): the hand-authored measure
/// catalog pinned to the real collector columns (drift = Erik's #1 pain), the compose spec validator (the
/// write-time + compile authority), the SQL compiler's safety invariants (catalog-only identifiers,
/// schema-qualified collect.*, every value bound, archetype-gated aggs, window-bounded #1568 join, DoS ceiling),
/// the ValidateDefinition v1/v2 dispatch, the /api/catalog v2 body, and the statement_timeout provisioning +
/// loopback-comment scrub. The live compile-and-run round-trip is exercised against a real store elsewhere; this
/// pins the pure, no-DB contract.
/// </summary>
public sealed class DarlingComposeTests
{
    /* ─────────────────────────── catalog pins (drift-proofing) ─────────────────────────── */

    private static Dictionary<string, HashSet<string>> PayloadColumnsByTable() =>
        CollectorCatalog.All.ToDictionary(
            c => c.TargetTable,
            c => c.PayloadColumns.Select(p => p.Name).ToHashSet(StringComparer.Ordinal),
            StringComparer.Ordinal);

    [Fact]
    public void EveryMeasureSource_IsARealCollectorTable()
    {
        var tables = CollectorCatalog.All.Select(c => c.TargetTable).ToHashSet(StringComparer.Ordinal);
        foreach (var measure in MeasureCatalog.Measures)
        {
            Assert.True(tables.Contains(measure.SourceTable),
                $"measure '{measure.Key}' names source '{measure.SourceTable}', which is not a collector table.");
        }
    }

    [Fact]
    public void NoMeasureSource_IsAConfigTable()
    {
        /* The structural config-exclusion guarantee (design §3): a composed query can only ever name a collect
           collector table, so it can never read a config control-plane table (hostnames, SMTP recipients, …).
           Every collector TargetTable is a collect-schema table; none begins with the config_ prefix. */
        foreach (var measure in MeasureCatalog.Measures)
        {
            Assert.False(measure.SourceTable.StartsWith("config", StringComparison.Ordinal),
                $"measure '{measure.Key}' resolves to a config table '{measure.SourceTable}'.");
        }
    }

    [Fact]
    public void EveryScalarMeasureColumn_IsARealPayloadColumn()
    {
        var payload = PayloadColumnsByTable();
        foreach (var measure in MeasureCatalog.Measures.Where(m => m.Kind == MeasureKind.Scalar))
        {
            var columns = payload[measure.SourceTable];
            Assert.True(measure.Column is not null && columns.Contains(measure.Column),
                $"measure '{measure.Key}' column '{measure.Column}' is not a payload column of '{measure.SourceTable}'.");

            /* A cumulative counter's delta column (what the aggregate actually operates on) must be real too. */
            if (measure.Archetype == MeasureArchetype.Cumulative)
            {
                Assert.True(measure.DeltaColumn is not null && columns.Contains(measure.DeltaColumn),
                    $"cumulative measure '{measure.Key}' delta column '{measure.DeltaColumn}' is not a payload column of '{measure.SourceTable}'.");
            }
        }
    }

    [Fact]
    public void EveryRatioMeasure_ReferencesRealSameSourceScalars()
    {
        /* Sum/Avg ratios divide two scalar measures; Weighted/WeightedSum ratios instead reference RAW source
           columns (WeightedValueColumn + WeightColumn), pinned separately by WeightedRatio_Columns_AreRealPayloadColumns. */
        foreach (var ratio in MeasureCatalog.Measures.Where(m => m.Kind == MeasureKind.Ratio && m.RatioMode is MeasureRatioMode.Sum or MeasureRatioMode.Avg))
        {
            var numerator = MeasureCatalog.Measure(ratio.NumeratorKey);
            var denominator = MeasureCatalog.Measure(ratio.DenominatorKey);
            Assert.True(numerator is not null, $"ratio '{ratio.Key}' numerator '{ratio.NumeratorKey}' is not a measure.");
            Assert.True(denominator is not null, $"ratio '{ratio.Key}' denominator '{ratio.DenominatorKey}' is not a measure.");
            Assert.Equal(MeasureKind.Scalar, numerator!.Kind);
            Assert.Equal(MeasureKind.Scalar, denominator!.Kind);
            Assert.Equal(ratio.SourceTable, numerator.SourceTable);
            Assert.Equal(ratio.SourceTable, denominator.SourceTable);
        }
    }

    [Fact]
    public void WeightedRatio_Columns_AreRealPayloadColumns()
    {
        var payload = PayloadColumnsByTable();
        var weighted = MeasureCatalog.Measures
            .Where(m => m.Kind == MeasureKind.Ratio && m.RatioMode is MeasureRatioMode.Weighted or MeasureRatioMode.WeightedSum).ToList();
        Assert.NotEmpty(weighted);
        foreach (var ratio in weighted)
        {
            var columns = payload[ratio.SourceTable];
            /* A Weighted/WeightedSum ratio's two operands are raw columns of its own source (the pre-aggregated
               average and its execution weight), so both must be real payload columns — the compiler emits them
               directly. */
            Assert.True(ratio.WeightedValueColumn is not null && columns.Contains(ratio.WeightedValueColumn),
                $"weighted ratio '{ratio.Key}' value column '{ratio.WeightedValueColumn}' is not a payload column of '{ratio.SourceTable}'.");
            Assert.True(ratio.WeightColumn is not null && columns.Contains(ratio.WeightColumn),
                $"weighted ratio '{ratio.Key}' weight column '{ratio.WeightColumn}' is not a payload column of '{ratio.SourceTable}'.");
            /* Weighted ratios do NOT use the numerator/denominator measure operands. */
            Assert.Null(ratio.NumeratorKey);
            Assert.Null(ratio.DenominatorKey);
        }
    }

    [Fact]
    public void EveryDimensionColumn_IsARealPayloadColumn_ExceptTheModuleJoin()
    {
        var payload = PayloadColumnsByTable();
        foreach (var dimension in MeasureCatalog.Dimensions)
        {
            /* The #1568 object_name is stitched from procedure_stats, not a column of the fact source. */
            var table = dimension.ViaModuleJoin ? "procedure_stats" : dimension.SourceTable;
            Assert.True(payload[table].Contains(dimension.Column),
                $"dimension '{dimension.SourceTable}.{dimension.Name}' column '{dimension.Column}' is not a payload column of '{table}'.");
        }
    }

    [Fact]
    public void EveryMeasureAllowedDimension_IsADeclaredDimensionOfItsSource()
    {
        foreach (var measure in MeasureCatalog.Measures)
        {
            foreach (var dimensionName in measure.AllowedDimensions)
            {
                Assert.True(MeasureCatalog.Dimension(measure.SourceTable, dimensionName) is not null,
                    $"measure '{measure.Key}' allows dimension '{dimensionName}', which is not declared for source '{measure.SourceTable}'.");
            }
        }
    }

    [Fact]
    public void Gauges_AreNeverSummable()
    {
        /* The grain-trap guard: a gauge has no aggregation delta column and never offers SUM. */
        foreach (var gauge in MeasureCatalog.Measures.Where(m => m.Archetype == MeasureArchetype.Gauge))
        {
            Assert.Null(gauge.AggregationColumn);
            Assert.DoesNotContain(ComposeAggregate.Sum, gauge.ValidAggs);
        }
    }

    [Fact]
    public void PercentileCont_IsOfferedOnlyByPerEventMeasures()
    {
        foreach (var measure in MeasureCatalog.Measures)
        {
            if (measure.ValidAggs.Contains(ComposeAggregate.PercentileCont))
            {
                Assert.Equal(MeasureArchetype.PerEvent, measure.Archetype);
            }
        }
    }

    [Fact]
    public void TheSlice_ProvesEveryArchetype()
    {
        var archetypes = MeasureCatalog.Measures.Where(m => m.Kind == MeasureKind.Scalar).Select(m => m.Archetype).ToHashSet();
        Assert.Contains(MeasureArchetype.Cumulative, archetypes);
        Assert.Contains(MeasureArchetype.Delta, archetypes);
        Assert.Contains(MeasureArchetype.Gauge, archetypes);
        Assert.Contains(MeasureArchetype.PerEvent, archetypes);
        Assert.Contains(MeasureCatalog.Measures, m => m.Kind == MeasureKind.Ratio);
    }

    /* ─────────────────────────── the compose spec validator ─────────────────────────── */

    private static JsonObject PanelJson(string json) => (JsonObject)JsonNode.Parse(json)!;

    private static PanelPlan ValidPlan(string json, params string[] declaredVariables)
    {
        var (plan, error) = ComposeSpec.TryParsePanel(PanelJson(json), declaredVariables);
        Assert.True(error is null, error);
        Assert.NotNull(plan);
        return plan!;
    }

    private static string RejectReason(string json, params string[] declaredVariables)
    {
        var (plan, error) = ComposeSpec.TryParsePanel(PanelJson(json), declaredVariables);
        Assert.Null(plan);
        Assert.NotNull(error);
        return error!;
    }

    [Fact]
    public void TryParsePanel_AcceptsAWellFormedScalarTimeSeries()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"minute\",\"viz\":\"line\",\"groupBy\":[\"wait_type\"]}");
        Assert.Equal(PanelMode.TimeSeries, plan.Mode);
        Assert.Equal(ComposeAggregate.Sum, plan.Aggregate);
        Assert.Single(plan.GroupBy);
    }

    [Fact]
    public void TryParsePanel_AcceptsARankedBar()
    {
        var plan = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}");
        Assert.Equal(PanelMode.Ranked, plan.Mode);
        Assert.Equal(10, plan.TopN);
    }

    [Fact]
    public void TryParsePanel_AcceptsARatio()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"ratio\":\"signal_wait_pct\",\"timeBucket\":\"minute\",\"viz\":\"line\"}");
        Assert.Equal(MeasureKind.Ratio, plan.Measure.Kind);
        Assert.Equal("percent", plan.Unit);
    }

    [Fact]
    public void TryParsePanel_AcceptsAModuleNameLikeFilter_OnAVariable()
    {
        var plan = ValidPlan(
            "{\"source\":\"procedure_stats\",\"measure\":\"proc_elapsed_us\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"dbo.usp_Payment%\"},{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"$db\"}]}",
            "db");
        Assert.Equal(2, plan.Filters.Count);
        Assert.True(plan.Filters[1].Value.IsVariable);
    }

    [Theory]
    [InlineData("{\"source\":\"nope\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\"}", "unknown source")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}", "unknown measure")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"viz\":\"table\"}", "is not on source")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"signal_wait_pct\",\"aggregate\":\"sum\",\"viz\":\"table\"}", "reference it as 'ratio'")]
    [InlineData("{\"source\":\"wait_stats\",\"ratio\":\"wait_time_ms\",\"viz\":\"table\"}", "reference it as 'measure'")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"viz\":\"table\"}", "missing 'aggregate'")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"nope\",\"viz\":\"table\"}", "unknown aggregate")]
    /* SUM on a gauge — the grain-trap the archetype gate blocks. */
    [InlineData("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"sum\",\"viz\":\"table\"}", "not valid for measure")]
    /* Same trap on an AG backlog gauge (#991): summing queue depth over a window is meaningless. */
    [InlineData("{\"source\":\"ag_database_replica_states\",\"measure\":\"ag_redo_queue\",\"aggregate\":\"sum\",\"viz\":\"table\"}", "not valid for measure")]
    /* percentile on a non-per-event measure. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"percentile_cont\",\"viz\":\"table\"}", "not valid for measure")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"unit\":\"gb\",\"viz\":\"table\"}", "not valid for measure")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"nope\"}", "unknown or missing viz")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"nope\",\"viz\":\"line\"}", "unknown timeBucket")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"minute\",\"topN\":5,\"viz\":\"line\"}", "cannot set both")]
    /* like on a non-likeable dimension (query_hash). */
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"viz\":\"table\",\"filters\":[{\"dimension\":\"query_hash\",\"op\":\"like\",\"value\":\"x\"}]}", "not allowed on dimension")]
    /* a dimension the measure does not allow. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\",\"groupBy\":[\"database_name\"]}", "not allowed for measure")]
    public void TryParsePanel_RejectsOffCatalog_NamingTheReason(string json, string expectedFragment)
    {
        var reason = RejectReason(json);
        Assert.Contains(expectedFragment, reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParsePanel_RejectsAnUndeclaredVariable()
    {
        var reason = RejectReason(
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\",\"filters\":[{\"dimension\":\"wait_type\",\"op\":\"eq\",\"value\":\"$undeclared\"}]}");
        Assert.Contains("undeclared variable", reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParsePanel_RejectsTooManyFilters()
    {
        var filters = string.Join(",", Enumerable.Repeat("{\"dimension\":\"wait_type\",\"op\":\"eq\",\"value\":\"x\"}", ComposeLimits.MaxFilters + 1));
        var reason = RejectReason("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\",\"filters\":[" + filters + "]}");
        Assert.Contains("maximum", reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ParseVariables_RejectsUnknownDimension_AndDuplicates()
    {
        Assert.NotNull(ComposeSpec.ParseVariables(JsonNode.Parse("[{\"name\":\"x\",\"dimension\":\"nope\"}]")).Error);
        Assert.NotNull(ComposeSpec.ParseVariables(JsonNode.Parse("[{\"name\":\"x\",\"dimension\":\"server\"},{\"name\":\"x\",\"dimension\":\"database_name\"}]")).Error);
        Assert.Null(ComposeSpec.ParseVariables(JsonNode.Parse("[{\"name\":\"srv\",\"dimension\":\"server\"}]")).Error);
    }

    [Theory]
    [InlineData("{\"hours\":24}", true)]
    [InlineData("{\"hours\":0}", false)]
    [InlineData("{\"hours\":100000}", false)]
    public void ParseRange_BoundsHours(string json, bool valid)
    {
        var (_, error) = ComposeSpec.ParseRange(JsonNode.Parse(json));
        Assert.Equal(valid, error is null);
    }

    /* ─────────────────────────── the SQL compiler (safety invariants) ─────────────────────────── */

    private static readonly DateTime WindowStart = new(2026, 7, 18, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = new(2026, 7, 18, 6, 0, 0, DateTimeKind.Utc);

    private static string Compile(PanelPlan plan, IReadOnlyList<string>? servers = null, IReadOnlyDictionary<string, string?>? variables = null)
    {
        var (compiled, error) = ComposeCompiler.Compile(
            plan, new ComposeRunContext(servers, WindowStart, WindowEnd, variables ?? ComposeRunContext.NoVariables, RollupAvailability.All, WindowEnd, RollupCoverage.Unknown));
        Assert.True(error is null, error);
        Assert.NotNull(compiled);
        return compiled!.Sql;
    }

    [Fact]
    public void Compile_QualifiesCollect_AndNeverConfig()
    {
        var sql = Compile(ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("collect.wait_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_BindsTheWindowAndServerScope_NoRawValues()
    {
        var sql = Compile(ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"), new[] { "PROD-01" });
        Assert.Contains("$1", sql, StringComparison.Ordinal);   /* window start */
        Assert.Contains("$2", sql, StringComparison.Ordinal);   /* window end */
        Assert.Contains("server_name = ANY($3)", sql, StringComparison.Ordinal);
        /* The server name is a bound parameter, never interpolated. */
        Assert.DoesNotContain("PROD-01", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_BindsFilterValues_NeverInterpolatesThem()
    {
        var sql = Compile(ValidPlan(
            "{\"source\":\"procedure_stats\",\"measure\":\"proc_elapsed_us\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"dbo.usp_Payment%\"}]}"));
        Assert.Contains("LIKE $", sql, StringComparison.Ordinal);
        /* The LIKE pattern is a bound parameter, never in the SQL text. */
        Assert.DoesNotContain("usp_Payment", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_EqFilter_IsABoundArray()
    {
        var sql = Compile(ValidPlan(
            "{\"source\":\"file_io_stats\",\"measure\":\"file_read_bytes\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":[\"Sales\",\"HR\"]}]}"));
        Assert.Contains("= ANY($", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("Sales", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_Ratio_IsSumOverNullifSum()
    {
        var sql = Compile(ValidPlan("{\"source\":\"wait_stats\",\"ratio\":\"signal_wait_pct\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("NULLIF(SUM(", sql, StringComparison.Ordinal);
        Assert.Contains("delta_signal_wait_time_ms", sql, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_Percentile_OnlyRendersForPerEvent()
    {
        var sql = Compile(ValidPlan("{\"source\":\"long_query_completions\",\"measure\":\"lqc_duration_us\",\"aggregate\":\"percentile_cont\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("percentile_cont(0.95)", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(ComposeTimeBucket.None)]
    [InlineData(ComposeTimeBucket.Minute)]
    [InlineData(ComposeTimeBucket.Hour)]
    [InlineData(ComposeTimeBucket.Day)]
    public void ResolveBucket_LeavesNonAutoUnchanged(ComposeTimeBucket bucket)
    {
        Assert.Equal(bucket, MeasureCatalog.ResolveBucket(bucket, 3600d));
    }

    [Fact]
    public void ResolveBucket_Auto_PicksGrainFromWindow()
    {
        Assert.Equal(ComposeTimeBucket.Minute, MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, 2d * 86_400d));       // 2 days -> minute
        Assert.Equal(ComposeTimeBucket.Hour, MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, 2d * 86_400d + 1d));   // just over 2 days -> hour
        Assert.Equal(ComposeTimeBucket.Hour, MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, 60d * 86_400d));       // 60 days -> hour
        Assert.Equal(ComposeTimeBucket.Day, MeasureCatalog.ResolveBucket(ComposeTimeBucket.Auto, 60d * 86_400d + 1d));   // over 60 days -> day
    }

    [Theory]
    [InlineData(6, "date_trunc('minute'")]      // 6h window -> minute grain
    [InlineData(24 * 10, "date_trunc('hour'")]  // 10 days -> hour grain
    [InlineData(24 * 90, "date_trunc('day'")]   // 90 days -> day grain
    public void Compile_AutoBucket_ResolvesGrainFromWindow(int windowHours, string expectedTrunc)
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"auto\",\"viz\":\"line\"}");
        var end = WindowEnd;
        var start = end.AddHours(-windowHours);
        var (compiled, error) = ComposeCompiler.Compile(plan, new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, RollupCoverage.Unknown));
        Assert.True(error is null, error);
        Assert.Contains(expectedTrunc, compiled!.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_UnitConversion_ScalesMicrosecondsToMilliseconds()
    {
        /* proc_elapsed_us: native µs, default ms — the aggregate is divided by the 1000 family factor. */
        var sql = Compile(ValidPlan("{\"source\":\"procedure_stats\",\"measure\":\"proc_elapsed_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("/ 1000.0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_ModuleJoin_IsEmittedAndWindowBounded_ForTheDerivedObject()
    {
        var sql = Compile(ValidPlan(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"object_name\"],\"viz\":\"bar\"}"), new[] { "PROD-01" });
        Assert.Contains("procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER()", sql, StringComparison.Ordinal);
        /* Per-server attribution — a handle reused across servers attributes per server, not globally. */
        Assert.Contains("PARTITION BY server_name, sql_handle", sql, StringComparison.Ordinal);
        /* The #1568 stitch is bounded by the SAME window ($1/$2) and scoped to the same server set ($3). */
        Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("server_name = ANY($3)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_NoModuleJoin_WhenNoDerivedObjectUsed()
    {
        var sql = Compile(ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}"));
        Assert.DoesNotContain("ROW_NUMBER()", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_RankedShape_OrdersByValueDescWithBoundLimit()
    {
        var sql = Compile(ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}"));
        Assert.Contains("ORDER BY value DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_RejectsTooFineABucketOverTooWideAWindow()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"minute\",\"viz\":\"line\"}");
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var end = start.AddDays(60); /* 60 days of minutes >> MaxBuckets */
        var (compiled, error) = ComposeCompiler.Compile(plan, new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, RollupCoverage.Unknown));
        Assert.Null(compiled);
        Assert.Contains("points", error!, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────── CAGG read-routing (age-based source selection) ─────────────── */

    private static (ComposeCompiled? Compiled, string? Error) CompileAged(string planJson, int daysOld, string[]? servers = null)
    {
        var plan = ValidPlan(planJson);
        var end = WindowEnd;                 /* EndUtc serves as "now" in the compiler */
        var start = end.AddDays(-daysOld);
        /* NowUtc = end here on purpose: these pins predate #1606 and reason about age relative to the window end. */
        return ComposeCompiler.Compile(plan, new ComposeRunContext(servers, start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, RollupCoverage.Unknown));
    }

    [Fact]
    public void Compile_OldWindow_RoutesQueryStatsToHourlyCagg_RemappingColumns()
    {
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 10);
        Assert.True(error is null, error);
        var sql = compiled!.Sql;
        Assert.Contains("FROM collect.query_stats_hourly AS f", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(f.worker_time_sum) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('hour', f.bucket)", sql, StringComparison.Ordinal);  /* the CAGG time column */
        Assert.DoesNotContain("delta_worker_time", sql, StringComparison.Ordinal);       /* not the raw column */
    }

    [Fact]
    public void Compile_VeryOldWindow_RoutesToDailyCagg_AndCoarsensDisplayGrainToDay()
    {
        /* hour requested, but 120 days old -> daily CAGG, and the grain clamps up to day (can't render finer). */
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 120);
        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_stats_daily AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('day', f.bucket)", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_OldWindow_AvgRemapsToSumOverSampleCount()
    {
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 10);
        Assert.True(error is null, error);
        Assert.Contains("CAST(SUM(f.worker_time_sum) AS double precision) / NULLIF(SUM(f.sample_count), 0)", compiled!.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_OldWindow_SumRatio_RemapsBothOperands()
    {
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_stats\",\"ratio\":\"query_avg_elapsed_us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 10);
        Assert.True(error is null, error);
        Assert.Contains("SUM(f.elapsed_time_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0)", compiled!.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_OldWindow_QueryStore_WeightedRatio_RoutesToCagg_RemapsWeightedMean()
    {
        /* A 10-day window routes QS to the hourly CAGG; the weighted mean remaps to the reshaped weighted-sum over
           execution_count_sum (exact, not an avg-of-avgs). Since #1849 that is the CORRECTED hourly — same
           column names, so the compiled expression is byte-identical and only the relation changed. A >21d
           window would route to the corrected daily. */
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 10);
        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_store_stats_corrected_hourly AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(f.duration_us_weighted_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0)", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_OldWindow_QueryStore_WeightedSum_RoutesToCagg_ReadsTheWeightedSumColumnAlone()
    {
        /* #2732: the total remaps to the reshaped CAGG's pre-multiplied product-sum DIRECTLY — the corrected
           rollups materialize SUM(avg * execution_count) as cpu_us_weighted_sum, so the total re-aggregates
           additively with no denominator and no NULLIF. */
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 10);
        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_store_stats_corrected_hourly AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(f.cpu_us_weighted_sum) AS double precision)", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NULLIF", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("avg_cpu_time_us", compiled.Sql, StringComparison.Ordinal); /* not the raw column */
    }

    [Fact]
    public void Compile_VeryOldWindow_QueryStore_RoutesToDailyCagg()
    {
        /* A 120-day QS window routes to the corrected daily (same weighted-sum columns as the hourly). This
           helper measures no coverage, and #1869's day-grain daily wins only on positive evidence that it
           holds the window — so with none, the corrected daily keeps it. */
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"timeBucket\":\"day\",\"viz\":\"line\"}", daysOld: 120);
        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_store_stats_corrected_daily AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(f.duration_us_weighted_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0)", compiled.Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1869 end to end through the COMPILER, not just the router: once the day-grain daily has measurably
    /// materialized the window, the compiled SQL names it — and names nothing else differently. The projection
    /// is asserted byte-for-byte against the corrected daily's, because the whole design rests on the two
    /// carrying identical column names so ComposeCaggValueMapper and every composed panel are untouched.
    /// </summary>
    [Fact]
    public void Compile_VeryOldWindow_QueryStore_DayGrainDailyCovered_RoutesThereWithTheSameProjection()
    {
        var plan = ValidPlan("{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"timeBucket\":\"day\",\"viz\":\"line\"}");
        var end = WindowEnd;
        var start = end.AddDays(-40);

        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStoreStatsCorrectedDailyView] = end.AddDays(-60),
                [TimescaleSupport.QueryStoreStatsDayGrainDailyView] = end.AddDays(-60),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal));

        var (compiled, error) = ComposeCompiler.Compile(
            plan, new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, RollupAvailability.All, end, coverage));

        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_store_stats_daygrain_daily AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(f.duration_us_weighted_sum) AS double precision) / NULLIF(SUM(f.execution_count_sum), 0)", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('day', f.bucket)", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_OldWindow_NonCaggTable_StaysRaw()
    {
        var (compiled, _) = CompileAged(
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 40);
        Assert.Contains("FROM collect.wait_stats AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("_hourly", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("_daily", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_OldWindow_ObjectName_RoutesToCagg_JoinsModuleMap()
    {
        /* object_name on query_stats now routes to the CAGG (120d -> daily) and joins the retained module_map for
           attribution, instead of the window-bounded procedure_stats #1568 CTE. */
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"object_name\"],\"viz\":\"bar\"}",
            daysOld: 120, servers: new[] { "PROD-01" });
        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_stats_daily AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN collect.module_map AS m ON m.sql_handle = f.sql_handle AND m.server_name = f.server_name", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("m.object_name", compiled.Sql, StringComparison.Ordinal);        /* attribution from the map */
        Assert.DoesNotContain("ROW_NUMBER()", compiled.Sql, StringComparison.Ordinal);   /* not the raw #1568 CTE */
    }

    [Fact]
    public void Compile_RecentWindow_QueryStats_StaysRaw()
    {
        /* The 6-hour helper window is well inside the raw horizon -> raw columns, byte-for-byte as before. */
        var sql = Compile(ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("FROM collect.query_stats AS f", sql, StringComparison.Ordinal);
        Assert.Contains("delta_worker_time", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("worker_time_sum", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_ResolvesAVariableToABoundValue()
    {
        var plan = ValidPlan(
            "{\"source\":\"file_io_stats\",\"measure\":\"file_read_bytes\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"$db\"}]}",
            "db");
        var sql = Compile(plan, variables: new Dictionary<string, string?>(StringComparer.Ordinal) { ["db"] = "Sales" });
        Assert.Contains("= ANY($", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("Sales", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── ValidateDefinition: v1/v2 dispatch ─────────────────────────── */

    [Fact]
    public void ValidateDefinition_AcceptsAComposedPanel()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Fact]
    public void ValidateDefinition_AcceptsAMixedV1AndV2Definition()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"variables\":[{\"name\":\"db\",\"dimension\":\"database_name\"}]," +
            "\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\"}," +
            "{\"source\":\"procedure_stats\",\"measure\":\"proc_elapsed_us\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"$db\"}]}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Fact]
    public void ValidateDefinition_RejectsABadComposedPanel_NamingThePanel()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}]}");
        Assert.False(result.IsValid);
        Assert.Contains("panel 0", result.Error!, StringComparison.Ordinal);
        Assert.Contains("unknown measure", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsAComposedPanelWithAnUndeclaredVariable()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\"," +
            "\"filters\":[{\"dimension\":\"wait_type\",\"op\":\"eq\",\"value\":\"$nope\"}]}]}");
        Assert.False(result.IsValid);
        Assert.Contains("undeclared variable", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsBadViewVariables()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"variables\":[{\"name\":\"x\",\"dimension\":\"nope\"}],\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\"}]}");
        Assert.False(result.IsValid);
        Assert.Contains("dimension", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── /api/catalog v2 ─────────────────────────── */

    [Fact]
    public void Catalog_CarriesTheComposeSection()
    {
        var catalog = DarlingWebEndpoints.BuildCatalogNode();

        /* v1 keys stay intact. */
        Assert.NotNull(catalog["reads"]);
        Assert.NotNull(catalog["viz"]);

        var compose = Assert.IsType<JsonObject>(catalog["compose"]);
        var measures = Assert.IsType<JsonArray>(compose["measures"]);
        Assert.Equal(MeasureCatalog.Measures.Count, measures.Count);
        Assert.NotNull(compose["dimensions"]);
        Assert.NotNull(compose["unitFamilies"]);
        Assert.NotNull(compose["aggregates"]);
        Assert.NotNull(compose["timeBuckets"]);
        Assert.NotNull(compose["filterOps"]);
    }

    /* ─────────────────────────── DoS backstop + loopback scrub (provisioning) ─────────────────────────── */

    [Fact]
    public void Provisioning_SetsStatementTimeout_OnViewerAndMcp_NotAdmin()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");
        Assert.Contains($"ALTER ROLE viewer SET statement_timeout = '{ComposeLimits.StatementTimeout}';", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER ROLE mcp    SET statement_timeout = '{ComposeLimits.StatementTimeout}';", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER ROLE admin  SET statement_timeout", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Provisioning_HasNoStaleLoopbackComment()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("AdminPassword01", "ViewerPassword02", "McpPassword03");
        Assert.DoesNotContain("LOOPBACK-ONLY", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── fleet / multi-server (§2b + Flow B) ─────────────────────────── */

    [Fact]
    public void Compile_Fleet_HasNoServerPredicate_WhenNoServerScope()
    {
        var sql = Compile(ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"bar\"}"));
        Assert.DoesNotContain("server_name = ANY", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_MultiServer_BindsServerNameArray_NeverInterpolated()
    {
        var sql = Compile(
            ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"bar\"}"),
            new[] { "PROD-01", "PROD-02" });
        Assert.Contains("server_name = ANY($", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("PROD-01", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("PROD-02", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_GroupByServer_SelectsServerName_OnAServerGrainMeasure()
    {
        /* Flow B's grain-trap alternative: "top servers by CPU" — group a server-grain gauge by the universal
           server dimension across the fleet. */
        var sql = Compile(ValidPlan("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"topN\":5,\"groupBy\":[\"server\"],\"viz\":\"bar\"}"));
        Assert.Contains("server_name AS server", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TryParsePanel_AllowsServer_AsFilterAndGroupBy_OnEveryMeasure()
    {
        /* server is universal even on a measure with NO declared dimensions (cpu is server-grain). */
        var plan = ValidPlan(
            "{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"topN\":5,\"groupBy\":[\"server\"],\"viz\":\"bar\"," +
            "\"filters\":[{\"dimension\":\"server\",\"op\":\"eq\",\"value\":[\"A\",\"B\"]}]}");
        Assert.Single(plan.GroupBy);
        Assert.Single(plan.Filters);
    }

    /* ─────────────────────────── viz ↔ mode coherence (§4) ─────────────────────────── */

    [Theory]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"pie\"}", "not a time series")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"line\"}", "ranked")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"stacked\"}", "needs a group-by")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"line\"}", "single value")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"viz\":\"bar\"}", "needs a group-by")]
    /* D2: stacked-bar behaves exactly like stacked — a time-series viz that needs a group-by, rejected for ranked/scalar. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"stacked-bar\"}", "needs a group-by")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"stacked-bar\"}", "ranked")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"stacked-bar\"}", "single value")]
    public void TryParsePanel_RejectsIncoherentVizForMode(string json, string expectedFragment)
    {
        Assert.Contains(expectedFragment, RejectReason(json), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"area\"}")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"groupBy\":[\"wait_type\"],\"viz\":\"stacked\"}")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"pie\"}")]
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"stat\"}")]
    /* D2: stacked-bar accepted for a grouped time series, exactly like stacked. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"groupBy\":[\"wait_type\"],\"viz\":\"stacked-bar\"}")]
    public void TryParsePanel_AcceptsCoherentVizForMode(string json)
    {
        var (plan, error) = ComposeSpec.TryParsePanel(PanelJson(json), Array.Empty<string>());
        Assert.True(error is null, error);
        Assert.NotNull(plan);
    }

    /* ─────────────────────────── acceptance flows compile end-to-end ─────────────────────────── */

    [Fact]
    public void FlowA_AvgProcedureElapsed_LikePattern_OnServer_ValidatesAndCompiles()
    {
        /* Acceptance Flow A: "avg procedure elapsed LIKE 'dbo.usp_Payment%' on $server, 24h → line." The panel
           validates against the catalog + declared $server, then compiles to a schema-qualified, fully-bound,
           time-bucketed, server-scoped query. */
        var panel =
            "{\"source\":\"procedure_stats\",\"measure\":\"proc_elapsed_us\",\"aggregate\":\"avg\"," +
            "\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"dbo.usp_Payment%\"}]}";

        var (plan, error) = ComposeSpec.TryParsePanel(PanelJson(panel), new[] { "srv" });
        Assert.True(error is null, error);

        var sql = Compile(plan!, new[] { "PROD-01" }); /* $server resolved to one server */
        Assert.Contains("collect.procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('hour'", sql, StringComparison.Ordinal);
        Assert.Contains("LIKE $", sql, StringComparison.Ordinal);
        Assert.Contains("server_name = ANY($", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("usp_Payment", sql, StringComparison.Ordinal); /* the pattern is bound, not interpolated */
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FlowB_TopDatabasesByCpu_AcrossTheFleet_ValidatesAndCompiles()
    {
        /* Acceptance Flow B: "top-10 databases by CPU across the fleet → bar." No server scope = whole fleet
           (no server predicate), grouped by database, ranked, bounded by a LIMIT. */
        var panel =
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\"," +
            "\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"bar\"}";

        var plan = ValidPlan(panel);
        var sql = Compile(plan); /* no server scope => whole fleet */
        Assert.Contains("collect.query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("f.database_name", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY value DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_name = ANY", sql, StringComparison.Ordinal); /* fleet-wide, no server filter */
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── B3 full catalog fill ─────────────────────────── */

    [Fact]
    public void B3_Catalog_CoversTheExpectedSources()
    {
        /* The B3 fill: every remaining collector table with composable measures is in the catalog (config
           snapshots + query_store are deliberately excluded — see the source comments). */
        var sources = MeasureCatalog.Measures.Select(m => m.SourceTable).ToHashSet(StringComparer.Ordinal);
        foreach (var expected in new[]
        {
            "latch_stats", "spinlock_stats", "cpu_scheduler_stats", "plan_cache_stats", "memory_stats",
            "memory_clerks", "memory_grant_stats", "tempdb_stats", "database_size_stats", "index_object_stats",
            "session_stats", "session_summary_stats", "waiting_tasks", "query_snapshots",
            "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks", "system_health_events",
            "default_trace_events", "running_jobs", "job_history", "perfmon_stats", "query_store_stats",
            "ag_database_replica_states",
        })
        {
            Assert.True(sources.Contains(expected), $"catalog is missing a measure for '{expected}'.");
        }
    }

    [Fact]
    public void B3_CountOnlyEventMeasure_CompilesToCountStar()
    {
        var sql = Compile(ValidPlan("{\"source\":\"deadlocks\",\"measure\":\"deadlock_count\",\"aggregate\":\"count\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("collect.deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void B3_ServerGrainGauge_CompilesAndGroupsByServer()
    {
        /* A server-grain gauge (memory) grouped by the universal server dimension = "top servers by memory". */
        var sql = Compile(ValidPlan("{\"source\":\"memory_stats\",\"measure\":\"mem_total_server_mb\",\"aggregate\":\"avg\",\"topN\":5,\"groupBy\":[\"server\"],\"viz\":\"bar\"}"));
        Assert.Contains("collect.memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(", sql, StringComparison.Ordinal);
        Assert.Contains("server_name AS server", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void B3_PerEventMeasure_SupportsCountAndPercentile()
    {
        /* blocked_process_reports: COUNT of reports, and a true p95 block duration (per-event rows exist). */
        var count = Compile(ValidPlan("{\"source\":\"blocked_process_reports\",\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"count\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("COUNT(*)", count, StringComparison.Ordinal);

        var p95 = Compile(ValidPlan("{\"source\":\"blocked_process_reports\",\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"percentile_cont\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("percentile_cont(0.95)", p95, StringComparison.Ordinal);
    }

    [Fact]
    public void B3_AvgPerExecutionRatio_CompilesToWeightedSumOverSum()
    {
        /* "avg procedure duration per execution" — the execution-WEIGHTED ratio SUM(elapsed)/SUM(execs), NOT an
           avg-of-avgs. Proves the bread-and-butter same-source ratio (design §2c). */
        var sql = Compile(ValidPlan("{\"source\":\"procedure_stats\",\"ratio\":\"proc_avg_elapsed_us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));
        Assert.Contains("collect.procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("NULLIF(SUM(", sql, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time", sql, StringComparison.Ordinal);
        Assert.Contains("delta_execution_count", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────── #1687: the row cap keeps the NEWEST slice, and says so ─────────────────────── */

    [Fact]
    public void TimeSeries_RowCap_KeepsTheNewestBuckets_ViaADescLimitedSubquery()
    {
        /* The bug: "ORDER BY bucket LIMIT 10000" returns the EARLIEST 10,000 rows, so a grouped
           minute-grain 24h panel rendered its first ~87 minutes as if that were the window. The cap now
           applies DESC inside a subquery, and the survivors are re-sorted ascending for the renderer. */
        var sql = Compile(ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"minute\",\"groupBy\":[\"wait_type\"],\"viz\":\"stacked\"}"));

        Assert.Contains("SELECT * FROM (", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY bucket DESC\nLIMIT " + ComposeLimits.HardRowCap + "\n) AS capped\nORDER BY bucket", sql, StringComparison.Ordinal);

        /* The inner ORDER BY must be the DESC one — an ascending inner sort would re-introduce the bug
           while still looking wrapped. */
        Assert.DoesNotContain("ORDER BY bucket\nLIMIT", sql, StringComparison.Ordinal);

        /* The cap survives as the LAST thing applied inside the subquery, and the outer sort is what the
           renderer consumes. */
        Assert.EndsWith("ORDER BY bucket", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TimeSeries_RowCap_WrapperOpensAfterTheCte_SoTheWithStaysTopLevel()
    {
        /* A module-join panel emits "WITH m AS (...)" first. The wrapper must open AFTER it: swallowing
           the CTE into the subquery would be a different (and in some engines invalid) statement, and
           the join would lose its source. */
        var sql = Compile(ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"groupBy\":[\"object_name\"],\"viz\":\"line\"}"));

        Assert.StartsWith("WITH ", sql, StringComparison.Ordinal);
        Assert.True(
            sql.IndexOf("WITH ", StringComparison.Ordinal) < sql.IndexOf("SELECT * FROM (", StringComparison.Ordinal),
            "the row-cap wrapper must open after the CTE, not before it");
    }

    [Fact]
    public void RankedAndScalar_AreUntouchedByTheRowCapWrapper()
    {
        /* Ranked's LIMIT is the caller's own topN — reaching it is the request being honored, not
           truncation — and Scalar is one row. Neither gets the wrapper. */
        var ranked = Compile(ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"bar\"}"));
        Assert.DoesNotContain("SELECT * FROM (", ranked, StringComparison.Ordinal);
        Assert.DoesNotContain(") AS capped", ranked, StringComparison.Ordinal);
        Assert.Contains("ORDER BY value DESC", ranked, StringComparison.Ordinal);

        var scalar = Compile(ValidPlan("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"viz\":\"stat\"}"));
        Assert.DoesNotContain("SELECT * FROM (", scalar, StringComparison.Ordinal);
        Assert.EndsWith("LIMIT 1", scalar, StringComparison.Ordinal);
    }

    [Fact]
    public void RowCapNotice_FiresAtExactlyTheCap_AndNotBelowIt()
    {
        /* Exactly-at-cap is the only detectable signal: the compiler cannot know buckets x groups ahead
           of time, because group cardinality is a property of the DATA, not the spec. */
        Assert.Null(ComposeStoreAvailability.BuildRowCapNotice(PanelMode.TimeSeries, ComposeLimits.HardRowCap - 1));
        Assert.Null(ComposeStoreAvailability.BuildRowCapNotice(PanelMode.TimeSeries, 0));

        var notice = ComposeStoreAvailability.BuildRowCapNotice(PanelMode.TimeSeries, ComposeLimits.HardRowCap);
        Assert.NotNull(notice);
        Assert.Contains("row cap reached", notice, StringComparison.Ordinal);
        Assert.Contains("most recent", notice, StringComparison.Ordinal);

        /* Names both escape hatches, because "it was truncated" without "here is what to do" is only
           half the fix. */
        Assert.Contains("Coarsen the time bucket", notice, StringComparison.Ordinal);
        Assert.Contains("narrow the group-by", notice, StringComparison.Ordinal);
    }

    [Fact]
    public void CombineNotices_ShowsEveryNoticeThatApplies_NotJustTheFirst()
    {
        /* Retention truncates the OLD end of a window and the row cap now truncates the old end too (by
           keeping the newest buckets) — a long grouped panel on a retention-active store hits both. The
           panel in the most trouble is exactly the one that must not have half its explanation dropped. */
        Assert.Null(ComposeStoreAvailability.CombineNotices(null, null));
        Assert.Equal("only one", ComposeStoreAvailability.CombineNotices(null, "only one"));
        Assert.Equal("only one", ComposeStoreAvailability.CombineNotices("only one", null));
        Assert.Equal("first second", ComposeStoreAvailability.CombineNotices("first", "second"));

        /* An empty or whitespace notice is not a notice — it must not produce a stray separator or an
           empty strip on the page. */
        Assert.Null(ComposeStoreAvailability.CombineNotices("", "   "));
        Assert.Equal("real", ComposeStoreAvailability.CombineNotices("", "real"));
    }

    [Fact]
    public void RowCapNotice_IsTimeSeriesOnly()
    {
        /* A Ranked panel returning exactly topN rows is the request being satisfied. Warning there would
           be a false alarm on the most ordinary result there is. */
        Assert.Null(ComposeStoreAvailability.BuildRowCapNotice(PanelMode.Ranked, ComposeLimits.HardRowCap));
        Assert.Null(ComposeStoreAvailability.BuildRowCapNotice(PanelMode.Scalar, ComposeLimits.HardRowCap));
    }

    /* ─────────────────────────── #991 Availability Group measures ─────────────────────────── */

    [Fact]
    public void Ag_Measures_AreAllGaugesOnTheDatabaseGrainTable()
    {
        /* The queues, rates and lag are recomputed from the CURRENT backlog on every read, so every one is
           a Gauge — non-summable, avg/min/max only. A Cumulative or Delta archetype here would let the
           composer SUM a backlog over a window and report a number that means nothing. */
        var agMeasures = MeasureCatalog.Measures
            .Where(m => string.Equals(m.SourceTable, "ag_database_replica_states", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(7, agMeasures.Count);

        foreach (var measure in agMeasures)
        {
            Assert.Equal(MeasureKind.Scalar, measure.Kind);
            Assert.Equal(MeasureArchetype.Gauge, measure.Archetype);
            Assert.Null(measure.AggregationColumn);
            Assert.Null(measure.DeltaColumn);
            Assert.Equal(new[] { ComposeAggregate.Avg, ComposeAggregate.Min, ComposeAggregate.Max }, measure.ValidAggs);
            Assert.DoesNotContain(ComposeAggregate.Sum, measure.ValidAggs);
            Assert.Equal("Availability Groups", measure.Category);
        }

        Assert.Equal(
            new[] { "ag_est_redo_drain_min", "ag_est_send_drain_min", "ag_log_send_queue", "ag_log_send_rate", "ag_redo_queue", "ag_redo_rate", "ag_secondary_lag" },
            agMeasures.Select(m => m.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void Ag_Measures_CarryTheDmvsNativeUnits()
    {
        /* Straight from the DMV: the queues are KB, the rates KB/second, the lag seconds. Getting a unit
           wrong here silently mis-scales every chart built on it. The two rates ride the bytes family
           because the catalog has no per-second family — the display name carries the "per second". */
        foreach (var key in new[] { "ag_log_send_queue", "ag_redo_queue", "ag_log_send_rate", "ag_redo_rate" })
        {
            var measure = MeasureCatalog.Measure(key)!;
            Assert.Equal("kb", measure.NativeUnit);
            Assert.Equal(MeasureCatalog.FamilyBytes, measure.UnitFamily);
        }

        Assert.Contains("per second", MeasureCatalog.Measure("ag_log_send_rate")!.DisplayName, StringComparison.Ordinal);
        Assert.Contains("per second", MeasureCatalog.Measure("ag_redo_rate")!.DisplayName, StringComparison.Ordinal);

        var lag = MeasureCatalog.Measure("ag_secondary_lag")!;
        Assert.Equal("s", lag.NativeUnit);
        Assert.Equal(MeasureCatalog.FamilyDuration, lag.UnitFamily);

        /* Backlog and lag default to the worst reading in the bucket; the rates to the sustained average. */
        Assert.Equal(ComposeAggregate.Max, MeasureCatalog.Measure("ag_log_send_queue")!.DefaultTimeAgg);
        Assert.Equal(ComposeAggregate.Max, MeasureCatalog.Measure("ag_redo_queue")!.DefaultTimeAgg);
        Assert.Equal(ComposeAggregate.Max, lag.DefaultTimeAgg);
        Assert.Equal(ComposeAggregate.Avg, MeasureCatalog.Measure("ag_log_send_rate")!.DefaultTimeAgg);
        Assert.Equal(ComposeAggregate.Avg, MeasureCatalog.Measure("ag_redo_rate")!.DefaultTimeAgg);
    }

    [Fact]
    public void Ag_Dimensions_AreTheGrainColumnsPlusTheSuspensionState()
    {
        foreach (var name in new[] { "ag_name", "database_name", "replica_server_name", "synchronization_state_desc", "suspend_reason_desc" })
        {
            var dimension = MeasureCatalog.Dimension("ag_database_replica_states", name);
            Assert.NotNull(dimension);
            Assert.Equal(name, dimension!.Column);
            Assert.True(dimension.Likeable);
        }

        /* Every AG measure allows all five, so any of them can slice or group a panel. */
        foreach (var measure in MeasureCatalog.Measures.Where(m => m.SourceTable == "ag_database_replica_states"))
        {
            Assert.Equal(
                new[] { "ag_name", "database_name", "replica_server_name", "synchronization_state_desc", "suspend_reason_desc" },
                measure.AllowedDimensions);
        }

        /* The universal server dimension resolves for this source too (fleet-wide AG views). */
        Assert.NotNull(MeasureCatalog.Dimension("ag_database_replica_states", MeasureCatalog.ServerDimensionName));

        /* The replica-grain table is all state strings — no numeric column worth aggregating — so it
           deliberately contributes no measures and therefore no dimensions. */
        Assert.False(MeasureCatalog.IsKnownSource("ag_replica_states"));
    }

    [Fact]
    public void Ag_LagPanel_CanExcludeSuspendedReplicas()
    {
        /* The reason synchronization_state_desc is a dimension at all: secondary_lag_seconds reads 0 while
           data movement is suspended, so an unfiltered lag panel shows a suspended replica as perfectly
           healthy. Filtering it out has to actually compile, or the trap is documented but unavoidable. */
        var sql = Compile(ValidPlan("{\"source\":\"ag_database_replica_states\",\"measure\":\"ag_secondary_lag\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"filters\":[{\"dimension\":\"synchronization_state_desc\",\"op\":\"neq\",\"value\":\"NOT SYNCHRONIZING\"}]}"));

        Assert.Contains("collect.ag_database_replica_states", sql, StringComparison.Ordinal);
        Assert.Contains("synchronization_state_desc", sql, StringComparison.Ordinal);

        /* The filter value is a bound parameter, never inlined. */
        Assert.DoesNotContain("NOT SYNCHRONIZING", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Ag_LagMeasure_CompilesAndGroupsByReplica()
    {
        /* The headline AG panel: worst secondary lag per replica over time. */
        var sql = Compile(ValidPlan("{\"source\":\"ag_database_replica_states\",\"measure\":\"ag_secondary_lag\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"groupBy\":[\"replica_server_name\"],\"viz\":\"line\"}"));

        Assert.Contains("collect.ag_database_replica_states", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(", sql, StringComparison.Ordinal);
        Assert.Contains("replica_server_name", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── D1: gauge-operand (Avg) ratios (#1563 follow-ups) ─────────────────────────── */

    [Fact]
    public void AvgRatio_Operands_AreBothGaugesOnTheSameSource()
    {
        var avgRatios = MeasureCatalog.Measures
            .Where(m => m.Kind == MeasureKind.Ratio && m.RatioMode == MeasureRatioMode.Avg).ToList();
        Assert.NotEmpty(avgRatios);
        foreach (var ratio in avgRatios)
        {
            var numerator = MeasureCatalog.Measure(ratio.NumeratorKey)!;
            var denominator = MeasureCatalog.Measure(ratio.DenominatorKey)!;
            /* an Avg ratio divides two point-in-time gauges (their AggregationColumn is null — never summable). */
            Assert.Equal(MeasureArchetype.Gauge, numerator.Archetype);
            Assert.Equal(MeasureArchetype.Gauge, denominator.Archetype);
            Assert.Equal(ratio.SourceTable, numerator.SourceTable);
            Assert.Equal(ratio.SourceTable, denominator.SourceTable);
        }
    }

    [Fact]
    public void SumRatio_Operands_HaveANonNullAggregationColumn()
    {
        var sumRatios = MeasureCatalog.Measures
            .Where(m => m.Kind == MeasureKind.Ratio && m.RatioMode == MeasureRatioMode.Sum).ToList();
        Assert.NotEmpty(sumRatios);
        foreach (var ratio in sumRatios)
        {
            var numerator = MeasureCatalog.Measure(ratio.NumeratorKey)!;
            var denominator = MeasureCatalog.Measure(ratio.DenominatorKey)!;
            /* a Sum ratio sums a cumulative/delta column — both operands must have a summable column. */
            Assert.NotNull(numerator.AggregationColumn);
            Assert.NotNull(denominator.AggregationColumn);
        }
    }

    [Theory]
    [InlineData("{\"source\":\"memory_grant_stats\",\"ratio\":\"grant_used_pct\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", "used_memory_mb", "granted_memory_mb")]
    [InlineData("{\"source\":\"plan_cache_stats\",\"ratio\":\"single_use_plan_pct\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", "single_use_plans", "total_plans")]
    public void Compile_GaugeRatio_IsAvgOverNullifAvg_ScaledToPercent(string json, string numeratorColumn, string denominatorColumn)
    {
        var sql = Compile(ValidPlan(json));
        Assert.Contains($"AVG(f.{numeratorColumn})", sql, StringComparison.Ordinal);
        Assert.Contains($"NULLIF(AVG(f.{denominatorColumn}), 0)", sql, StringComparison.Ordinal);
        /* fraction family: native 'ratio' displayed as 'percent' => × 100. */
        Assert.Contains("* 100.0", sql, StringComparison.Ordinal);
        /* a gauge ratio never uses the Sum form (gauges are never summable). */
        Assert.DoesNotContain("SUM(", sql, StringComparison.Ordinal);
    }

    /* ─────────────── L1: Query Store execution-weighted average ratios (Weighted mode) ─────────────── */

    [Theory]
    [InlineData("qs_avg_duration_us", "avg_duration_us")]
    [InlineData("qs_avg_cpu_us", "avg_cpu_time_us")]
    public void Compile_WeightedRatio_IsProductSumOverNullifSumOfWeight(string ratioKey, string valueColumn)
    {
        /* The execution-weighted average of a pre-aggregated per-interval average:
           SUM(value * execution_count) / NULLIF(SUM(execution_count), 0) — not an avg-of-avgs (design §2). */
        var sql = Compile(ValidPlan($"{{\"source\":\"query_store_stats\",\"ratio\":\"{ratioKey}\",\"timeBucket\":\"hour\",\"viz\":\"line\"}}"));
        Assert.Contains($"SUM(f.{valueColumn} * f.execution_count)", sql, StringComparison.Ordinal);
        Assert.Contains("NULLIF(SUM(f.execution_count), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
        /* native µs displayed as the default ms — the same /1000 family conversion the other duration ratios use. */
        Assert.Contains("/ 1000.0", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("qs_total_duration_us", "avg_duration_us")]
    [InlineData("qs_total_cpu_us", "avg_cpu_time_us")]
    public void Compile_WeightedSum_IsProductSum_WithNoDenominator(string measureKey, string valueColumn)
    {
        /* #2732: the window TOTAL — the Weighted ratios' numerator alone, SUM(avg * execution_count), since
           avg * execution_count is each interval's total consumption. No NULLIF anywhere: there is no division. */
        var sql = Compile(ValidPlan($"{{\"source\":\"query_store_stats\",\"ratio\":\"{measureKey}\",\"timeBucket\":\"hour\",\"viz\":\"line\"}}"));
        Assert.Contains($"CAST(SUM(f.{valueColumn} * f.execution_count) AS double precision)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NULLIF", sql, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
        /* native µs displayed as the default s — a window total across a store runs to seconds-to-hours. */
        Assert.Contains("/ 1000000.0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_QueryStoreRawRoute_DedupsPerIntervalBeforeAggregating()
    {
        /* #1841. query_store_stats rows are CUMULATIVE per-interval snapshots re-collected every cycle,
           so the raw route must aggregate the LATEST snapshot per interval, not every stored row. Tier 2
           made runtime_stats_interval_id the real interval identity, with first_execution_time kept
           beside it as the tier-1 proxy for rows collected before it existed; server_id is in the
           partition because a composed panel spans the fleet. */
        /* Scoped to specific servers on purpose — that is what puts server_name in the outer WHERE, which
           is the whole reason it has to be in the partition too. */
        var sql = Compile(
            ValidPlan("{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"),
            servers: ["srv-a", "srv-b"]);

        /* The execution_count tie-break is the #1907 residual and is pinned as part of the ORDER BY rather
           than left to a looser Contains: collection_time alone was not a total order on rows collected
           before that fix, where Query Store's flushed and in-memory slices of one interval were stored as
           two rows sharing this whole partition AND collection_time. It cannot fire on rows collected since
           — the collector combines the slices — so it exists for what is already stored (#1912). */
        Assert.Contains(
            "PARTITION BY server_id, server_name, database_name, query_id, plan_id, runtime_stats_interval_id, "
            + "first_execution_time, execution_type_desc, replica_role ORDER BY collection_time DESC, execution_count DESC",
            sql, StringComparison.Ordinal);
        Assert.Contains("AS qs_rn", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE qs_rn = 1", sql, StringComparison.Ordinal);

        /* server_name must be IN the partition, not only in the outer WHERE: Postgres pushes a qual through
           a window-function subquery only when the qual's columns appear in every PARTITION BY, so without
           it a fleet store would rank every server's rows before narrowing to the panel's scope. */
        var partition = sql.IndexOf("PARTITION BY", StringComparison.Ordinal);
        var scope = sql.IndexOf("f.server_name = ANY(", StringComparison.Ordinal);
        Assert.True(scope > partition, "the server scope stays in the outer WHERE, so the partition must carry server_name");

        /* The dedup window is pushed INSIDE the derived table, so "latest" means latest within the
           panel's window rather than latest overall — and it prunes chunks before the window function. */
        var relation = sql.IndexOf("collect.query_store_stats ", StringComparison.Ordinal);
        var innerWindow = sql.IndexOf("WHERE collection_time >= $1 AND collection_time <= $2", StringComparison.Ordinal);
        Assert.True(relation >= 0 && innerWindow > relation,
            "the dedup subquery must carry the window predicate itself");

        /* Still only catalog identifiers and the two already-bound window parameters. */
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_QueryStoreCaggRoute_IsNotWrappedInTheRawDedup()
    {
        /* The read-time dedup belongs to the RAW route only: a CAGG cannot contain a window function at all.
           Since #1849 the rollup is not un-deduped either — the dedup moved INTO the rollup chain, where
           query_store_stats_interval_hourly collapses each interval with last(x, collection_time) before the
           composer-grain view sums it. So this route is correct without the wrapper, not merely un-wrapped. */
        var (compiled, error) = CompileAged(
            "{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}", daysOld: 10);
        Assert.True(error is null, error);

        Assert.Contains("FROM collect.query_store_stats_corrected_hourly AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qs_rn", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_NonQueryStoreRawRoute_ReadsTheTableDirectly()
    {
        /* The #1841 wrapper is scoped to query_store_stats — every other source keeps its bare
           "FROM collect.<table> AS f", so this stays a one-table exception rather than a compiler-wide
           behavior change. */
        var sql = Compile(ValidPlan(
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}"));

        Assert.Contains("FROM collect.wait_stats AS f", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qs_rn", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WeightedQsRatios_ValidateAsRatios_AndRejectMeasureReference()
    {
        foreach (var key in new[] { "qs_avg_duration_us", "qs_avg_cpu_us" })
        {
            var plan = ValidPlan($"{{\"source\":\"query_store_stats\",\"ratio\":\"{key}\",\"timeBucket\":\"hour\",\"viz\":\"line\"}}");
            Assert.Equal(MeasureKind.Ratio, plan.Measure.Kind);
            Assert.Equal(MeasureRatioMode.Weighted, plan.Measure.RatioMode);
            Assert.Equal("ms", plan.Unit);

            /* A ratio referenced via 'measure' (with an aggregate) is rejected — it must be referenced as 'ratio'. */
            var reason = RejectReason($"{{\"source\":\"query_store_stats\",\"measure\":\"{key}\",\"aggregate\":\"sum\",\"viz\":\"table\"}}");
            Assert.Contains("reference it as 'ratio'", reason, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void WeightedSumQsTotals_ValidateAsRatios_AndRejectMeasureReference()
    {
        /* #2732: the totals ride the Ratio kind (their aggregation is the definition, like a ratio's), so the
           spec/wire seam needs no new special case — referenced as 'ratio', aggregate fixed, default unit 's'. */
        foreach (var key in new[] { "qs_total_duration_us", "qs_total_cpu_us" })
        {
            var plan = ValidPlan($"{{\"source\":\"query_store_stats\",\"ratio\":\"{key}\",\"timeBucket\":\"hour\",\"viz\":\"line\"}}");
            Assert.Equal(MeasureKind.Ratio, plan.Measure.Kind);
            Assert.Equal(MeasureRatioMode.WeightedSum, plan.Measure.RatioMode);
            Assert.Equal("s", plan.Unit);

            var reason = RejectReason($"{{\"source\":\"query_store_stats\",\"measure\":\"{key}\",\"aggregate\":\"sum\",\"viz\":\"table\"}}");
            Assert.Contains("reference it as 'ratio'", reason, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void WeightedSumQsTotals_SupportRankedMode_TopQueryHashByTotalCpu()
    {
        /* The panel the issue exists for: "top query_hash by total CPU" — ranked mode, ORDER BY value DESC.
           A per-execution average would rank a one-shot 30s query over one that burned an hour at 200ms/call. */
        var sql = Compile(ValidPlan(
            "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}"));
        Assert.Contains("CAST(SUM(f.avg_cpu_time_us * f.execution_count) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY value DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $", sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── D3: per-panel thresholds (render-only) ─────────────────────────── */

    [Fact]
    public void TryParsePanel_AcceptsThresholds_AndCarriesThemOnThePlan()
    {
        var plan = ValidPlan("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"thresholds\":[80,90]}");
        Assert.Equal(new[] { 80.0, 90.0 }, plan.Thresholds);
    }

    [Fact]
    public void TryParsePanel_DefaultsThresholdsToEmpty_WhenAbsent()
    {
        var plan = ValidPlan("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"}");
        Assert.Empty(plan.Thresholds);
    }

    [Theory]
    [InlineData("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"thresholds\":[1,2,3,4,5]}", "maximum")]
    [InlineData("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"thresholds\":[\"abc\"]}", "finite number")]
    [InlineData("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"thresholds\":42}", "must be an array")]
    public void TryParsePanel_RejectsBadThresholds(string json, string expectedFragment)
    {
        Assert.Contains(expectedFragment, RejectReason(json), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParsePanel_RejectsNonFiniteThreshold()
    {
        /* NaN/Infinity can't appear in JSON text, so build the node directly to prove the finite guard. */
        var panel = PanelJson("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"}");
        panel["thresholds"] = new JsonArray(JsonValue.Create(80.0), JsonValue.Create(double.NaN));
        var (plan, error) = ComposeSpec.TryParsePanel(panel, Array.Empty<string>());
        Assert.Null(plan);
        Assert.Contains("finite number", error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Compile_NeverEmitsThresholds_TheyAreRenderOnly()
    {
        var sql = Compile(ValidPlan("{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"thresholds\":[80,90]}"));
        Assert.DoesNotContain("80", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("90", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateDefinition_AcceptsAComposedPanelWithThresholds()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"thresholds\":[80,90]}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    /* ─────────────────────────── D4: per-measure availability (appliesTo) ─────────────────────────── */

    [Fact]
    public void Catalog_Measures_CarryAppliesTo_ReflectingTheCollectorGate()
    {
        var compose = Assert.IsType<JsonObject>(DarlingWebEndpoints.BuildCatalogNode()["compose"]);
        var measures = Assert.IsType<JsonArray>(compose["measures"]);

        JsonObject AppliesToFor(string measureKey)
        {
            var measure = measures.Cast<JsonObject>().Single(m => m!["key"]!.GetValue<string>() == measureKey);
            return Assert.IsType<JsonObject>(measure["appliesTo"]);
        }

        /* An all-targets collector (wait_stats) is universally available and needs no msdb. */
        var wait = AppliesToFor("wait_time_ms");
        Assert.True(wait["onPrem"]!.GetValue<bool>());
        Assert.True(wait["azureSqlDb"]!.GetValue<bool>());
        Assert.True(wait["azureMi"]!.GetValue<bool>());
        Assert.True(wait["awsRds"]!.GetValue<bool>());
        Assert.False(wait["needsMsdb"]!.GetValue<bool>());

        /* A SQL-Agent collector (running_jobs) needs msdb and is gated off Azure SQL DB + AWS RDS.

           needsMsdb is still TRUE here after #2559, and that is the point: the dispatch gate stopped reading
           msdb access, but the DATA still comes from msdb.dbo.sysjobs, so a composer building a job panel
           still has to know. What changed is the consequence of not having the grant - a PERMISSIONS row
           instead of no dispatch - so the flag is now sourced from the named SQL-Agent set rather than
           inferred by probing the gate against an msdb-less target, which no longer discriminates. */
        var job = AppliesToFor("runningjob_current_duration_s");
        Assert.True(job["onPrem"]!.GetValue<bool>());
        Assert.False(job["azureSqlDb"]!.GetValue<bool>());
        Assert.True(job["azureMi"]!.GetValue<bool>());
        Assert.False(job["awsRds"]!.GetValue<bool>());
        Assert.True(job["needsMsdb"]!.GetValue<bool>());
    }

    /// <summary>
    /// The named SQL-Agent set behind <c>needsMsdb</c> is exactly the collectors that read msdb, asserted
    /// against each one's actual query text rather than against a second copy of the list.
    ///
    /// <para>Before #2559 the flag was derived by probing the gate, so it could not drift. It is a named set
    /// now, which can — so this holds both halves it is able to hold: every collector claiming the flag
    /// really does reference msdb, and no other measure claims it. What it cannot catch is a NEW collector
    /// that reads msdb and never gets added, and saying so here is better than implying coverage that is
    /// not there.</para>
    /// </summary>
    [Fact]
    public void NeedsMsdb_IsClaimedByExactlyTheCollectorsWhoseQueriesReadMsdb()
    {
        var compose = Assert.IsType<JsonObject>(DarlingWebEndpoints.BuildCatalogNode()["compose"]);
        var measures = Assert.IsType<JsonArray>(compose["measures"]);

        var msdbBacked = new[] { "agent_status", "job_history", "running_jobs" };

        var claiming = measures.Cast<JsonObject>()
            .Where(m => m!["appliesTo"]?["needsMsdb"]?.GetValue<bool>() == true)
            .Select(m => m!["source"]!.GetValue<string>())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToArray();

        /* Compared against the msdb tables that actually HAVE a measure, not against the raw set. The first
           draft asserted the raw set and went red on agent_status, which exposes a real fact rather than a
           typo: agent_status is a collector nothing in the compose catalog builds a measure from, so it can
           never appear here. Asserting the raw set would have forced a fake expectation to make it pass. */
        var sources = measures.Cast<JsonObject>()
            .Select(m => m!["source"]!.GetValue<string>())
            .ToHashSet(StringComparer.Ordinal);
        var expected = msdbBacked.Where(sources.Contains).OrderBy(t => t, StringComparer.Ordinal).ToArray();

        Assert.NotEmpty(expected);
        Assert.Equal(expected, claiming);

        /* And each one genuinely reads msdb — so a collector rewritten off it stops matching, rather than
           the badge quietly outliving the dependency it describes. */
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = new DateTime(2026, 8, 25, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        };
        Assert.Contains("msdb", RunningJobsCollector.Instance.BuildQuery(context).Text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("msdb", JobHistoryCollector.Instance.BuildQuery(context).Text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("msdb", AgentStatusCollector.Instance.BuildQuery(context).Text, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── D5: event annotations (catalog pins) ─────────────────────────── */

    [Fact]
    public void EveryAnnotationSource_IsARealCollectorTable()
    {
        var tables = CollectorCatalog.All.Select(c => c.TargetTable).ToHashSet(StringComparer.Ordinal);
        foreach (var source in MeasureCatalog.AnnotationSources)
        {
            Assert.True(tables.Contains(source.SourceTable),
                $"annotation source '{source.Key}' names table '{source.SourceTable}', which is not a collector table.");
        }
    }

    [Fact]
    public void EveryAnnotationSourceColumns_AreRealPayloadColumns()
    {
        /* Same drift-proofing as the measure column pins: an annotation's event-time + label columns must be
           real payload columns of its collector, so the compiler emits only real, schema-qualified identifiers. */
        var payload = PayloadColumnsByTable();
        foreach (var source in MeasureCatalog.AnnotationSources)
        {
            var columns = payload[source.SourceTable];
            Assert.True(columns.Contains(source.TimeColumn),
                $"annotation source '{source.Key}' time column '{source.TimeColumn}' is not a payload column of '{source.SourceTable}'.");
            Assert.True(columns.Contains(source.LabelColumn),
                $"annotation source '{source.Key}' label column '{source.LabelColumn}' is not a payload column of '{source.SourceTable}'.");
        }
    }

    [Fact]
    public void NoAnnotationSource_IsAConfigTable()
    {
        /* The same structural config-exclusion guarantee the measures carry: an annotation query can only ever
           name a collect collector table, never a config control-plane table. */
        foreach (var source in MeasureCatalog.AnnotationSources)
        {
            Assert.False(source.SourceTable.StartsWith("config", StringComparison.Ordinal),
                $"annotation source '{source.Key}' resolves to a config table '{source.SourceTable}'.");
        }
    }

    /* ─────────────────────────── D5: the spec validator ─────────────────────────── */

    [Fact]
    public void TryParsePanel_AcceptsAnnotations_OnATimeSeries_AndCarriesThemOnThePlan()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\",\"blocked_process_reports\"]}");
        Assert.Equal(new[] { "deadlocks", "blocked_process_reports" }, plan.Annotations.Select(a => a.Key));
    }

    [Fact]
    public void TryParsePanel_DefaultsAnnotationsToEmpty_WhenAbsent()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}");
        Assert.Empty(plan.Annotations);
    }

    [Theory]
    /* an unknown annotation-source key. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"nope\"]}", "unknown source")]
    /* annotations on a ranked panel — a marker overlay needs a time axis. */
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"bar\",\"annotations\":[\"deadlocks\"]}", "time-series")]
    /* annotations on a scalar panel — likewise no time axis. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"stat\",\"annotations\":[\"deadlocks\"]}", "time-series")]
    /* the same source listed twice. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\",\"deadlocks\"]}", "more than once")]
    /* not an array. */
    [InlineData("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":\"deadlocks\"}", "must be an array")]
    public void TryParsePanel_RejectsBadAnnotations_NamingTheReason(string json, string expectedFragment)
    {
        Assert.Contains(expectedFragment, RejectReason(json), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParsePanel_RejectsTooManyAnnotations()
    {
        /* All five distinct sources exceeds the cap of four. */
        var reason = RejectReason("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"annotations\":[\"deadlocks\",\"blocked_process_reports\",\"long_query_completions\",\"default_trace_events\",\"system_health_events\"]}");
        Assert.Contains("maximum", reason, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── D5: the annotation compiler (safety invariants) ─────────────────────────── */

    private static IReadOnlyList<(string Source, ComposeCompiled Compiled)> CompileAnnotations(
        PanelPlan plan, IReadOnlyList<string>? servers = null) =>
        ComposeCompiler.CompileAnnotations(plan, new ComposeRunContext(servers, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.All, WindowEnd, RollupCoverage.Unknown));

    [Fact]
    public void CompileAnnotations_ReturnsEmpty_WhenNoneRequested()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}");
        Assert.Empty(CompileAnnotations(plan));
    }

    [Fact]
    public void CompileAnnotations_EmitsSchemaQualifiedCollect_WindowAndServerBound_Capped()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\"]}");
        var (source, one) = Assert.Single(CompileAnnotations(plan, new[] { "PROD-01" }));
        Assert.Equal("deadlocks", source);

        var sql = one.Sql;
        Assert.Contains("collect.deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("f.deadlock_time AS ts", sql, StringComparison.Ordinal);
        Assert.Contains("f.database_name AS label", sql, StringComparison.Ordinal);
        /* window + server scope are the SAME bound parameters the measure query uses ($1/$2 window, $3 server[]). */
        Assert.Contains("f.deadlock_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("f.deadlock_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("server_name = ANY($3)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY ts", sql, StringComparison.Ordinal);
        Assert.Contains($"LIMIT {ComposeLimits.MaxAnnotationEvents}", sql, StringComparison.Ordinal);
        /* catalog-only: never a config table, and the server name is bound, never interpolated. */
        Assert.DoesNotContain("config.", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("PROD-01", sql, StringComparison.Ordinal);
        Assert.Equal(3, one.Parameters.Count); /* every value bound: $1 start, $2 end, $3 server[] */
    }

    [Fact]
    public void CompileAnnotations_HasNoServerPredicate_ForTheWholeFleet()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\"]}");
        var (_, one) = Assert.Single(CompileAnnotations(plan));
        Assert.DoesNotContain("server_name = ANY", one.Sql, StringComparison.Ordinal);
        Assert.Equal(2, one.Parameters.Count); /* $1 start, $2 end — no server predicate on the fleet */
    }

    [Fact]
    public void CompileAnnotations_CompilesOneQueryPerSource_InOrder()
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\",\"system_health_events\"]}");
        Assert.Equal(new[] { "deadlocks", "system_health_events" }, CompileAnnotations(plan).Select(c => c.Source));
    }

    [Theory]
    [InlineData("deadlocks", "collect.deadlocks", "f.deadlock_time AS ts", "f.database_name AS label")]
    [InlineData("blocked_process_reports", "collect.blocked_process_reports", "f.event_time AS ts", "f.contentious_object AS label")]
    [InlineData("long_query_completions", "collect.long_query_completions", "f.event_time AS ts", "f.object_name AS label")]
    [InlineData("default_trace_events", "collect.default_trace_events", "f.event_time AS ts", "f.event_name AS label")]
    [InlineData("system_health_events", "collect.system_health_events", "f.event_time AS ts", "f.event_type AS label")]
    public void CompileAnnotations_EachSource_SelectsItsCatalogTimeAndLabelColumns(string key, string table, string tsExpr, string labelExpr)
    {
        var plan = ValidPlan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"" + key + "\"]}");
        var (_, one) = Assert.Single(CompileAnnotations(plan));
        Assert.Contains(table, one.Sql, StringComparison.Ordinal);
        Assert.Contains(tsExpr, one.Sql, StringComparison.Ordinal);
        Assert.Contains(labelExpr, one.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.", one.Sql, StringComparison.Ordinal);
    }

    /* ─────────────────────────── D5: /api/catalog + ValidateDefinition ─────────────────────────── */

    [Fact]
    public void Catalog_CarriesAnnotationSources()
    {
        var compose = Assert.IsType<JsonObject>(DarlingWebEndpoints.BuildCatalogNode()["compose"]);
        var sources = Assert.IsType<JsonArray>(compose["annotationSources"]);
        Assert.Equal(MeasureCatalog.AnnotationSources.Count, sources.Count);

        var deadlocks = sources.Cast<JsonObject>().Single(s => s!["key"]!.GetValue<string>() == "deadlocks");
        Assert.False(string.IsNullOrEmpty(deadlocks["displayName"]!.GetValue<string>()));
        Assert.False(string.IsNullOrEmpty(deadlocks["category"]!.GetValue<string>()));
        /* appliesTo parity with measures (D4) so the composer can grey a source a target can't collect. */
        Assert.NotNull(Assert.IsType<JsonObject>(deadlocks["appliesTo"])["onPrem"]);
    }

    [Fact]
    public void ValidateDefinition_AcceptsAComposedPanelWithAnnotations()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\",\"system_health_events\"]}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Fact]
    public void ValidateDefinition_RejectsAComposedPanelWithABadAnnotation()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"nope\"]}]}");
        Assert.False(result.IsValid);
        Assert.Contains("unknown source", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── D7: notebook mode (kind dispatch) ─────────────────────────── */

    [Fact]
    public void ValidateDefinition_AcceptsANotebook_WithMarkdownAndPanelCells()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\",\"cells\":[" +
            "{\"type\":\"markdown\",\"text\":\"# Wait analysis\\nSome prose about the panel below.\"}," +
            "{\"type\":\"panel\",\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Fact]
    public void ValidateDefinition_AcceptsAMarkdownOnlyNotebook()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\",\"text\":\"just prose, no panels\"}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Fact]
    public void ValidateDefinition_AcceptsANotebook_WithVariablesRange_AndAVariableFilterPanelCell()
    {
        /* A notebook's view-level variables + range validate through the SAME authorities a dashboard uses, and a
           panel cell's $var filter resolves against those declared variables. */
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\"," +
            "\"variables\":[{\"name\":\"db\",\"dimension\":\"database_name\"}]," +
            "\"range\":{\"hours\":24}," +
            "\"cells\":[" +
            "{\"type\":\"markdown\",\"text\":\"scoped to $db\"}," +
            "{\"type\":\"panel\",\"source\":\"procedure_stats\",\"measure\":\"proc_elapsed_us\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"$db\"}]}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Theory]
    /* cells missing entirely / not an array. */
    [InlineData("{\"kind\":\"notebook\"}", "must be an array")]
    [InlineData("{\"kind\":\"notebook\",\"cells\":\"nope\"}", "must be an array")]
    /* an empty cells array. */
    [InlineData("{\"kind\":\"notebook\",\"cells\":[]}", "at least one")]
    /* a cell that is not an object. */
    [InlineData("{\"kind\":\"notebook\",\"cells\":[\"nope\"]}", "must be an object")]
    /* an unknown cell type. */
    [InlineData("{\"kind\":\"notebook\",\"cells\":[{\"type\":\"chart\",\"text\":\"x\"}]}", "unknown type")]
    /* a cell with no type at all. */
    [InlineData("{\"kind\":\"notebook\",\"cells\":[{\"text\":\"x\"}]}", "unknown type")]
    /* a markdown cell missing 'text'. */
    [InlineData("{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\"}]}", "string 'text'")]
    /* a markdown cell whose 'text' is present but not a string. */
    [InlineData("{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\",\"text\":42}]}", "string 'text'")]
    public void ValidateDefinition_RejectsBadNotebook_NamingTheReason(string json, string expectedFragment)
    {
        var result = DarlingWebEndpoints.ValidateDefinition(json);
        Assert.False(result.IsValid);
        Assert.Contains(expectedFragment, result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsANotebook_WithAnInvalidPanelCell_NamingTheCell()
    {
        /* A panel cell routes through the EXACT ComposeSpec.TryParsePanel authority a dashboard's composed panel
           uses, so an off-catalog measure is caught and the offending cell is named by index. */
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\",\"cells\":[" +
            "{\"type\":\"markdown\",\"text\":\"intro\"}," +
            "{\"type\":\"panel\",\"source\":\"wait_stats\",\"measure\":\"nope\",\"aggregate\":\"sum\",\"viz\":\"table\"}]}");
        Assert.False(result.IsValid);
        Assert.Contains("cell 1", result.Error!, StringComparison.Ordinal);
        Assert.Contains("unknown measure", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsANotebookPanelCell_WithAnUndeclaredVariable()
    {
        /* The panel cell's $var must reference a variable the notebook declared — the same allowlist a dashboard
           composed panel enforces. */
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\",\"cells\":[" +
            "{\"type\":\"panel\",\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"viz\":\"table\"," +
            "\"filters\":[{\"dimension\":\"wait_type\",\"op\":\"eq\",\"value\":\"$nope\"}]}]}");
        Assert.False(result.IsValid);
        Assert.Contains("undeclared variable", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsANotebook_WithBadViewVariables()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\",\"variables\":[{\"name\":\"x\",\"dimension\":\"nope\"}]," +
            "\"cells\":[{\"type\":\"markdown\",\"text\":\"x\"}]}");
        Assert.False(result.IsValid);
        Assert.Contains("dimension", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsANotebook_WithBadRange()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"notebook\",\"range\":{\"hours\":0}," +
            "\"cells\":[{\"type\":\"markdown\",\"text\":\"x\"}]}");
        Assert.False(result.IsValid);
        Assert.Contains("range.hours", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsANotebook_WithOversizeMarkdownText()
    {
        /* One markdown cell over the per-cell byte cap, while the whole doc stays under the 128 KB definition cap
           — so this pins the per-cell bound specifically, not the doc bound. */
        var bigText = new string('a', DarlingWebEndpoints.MaxMarkdownCellBytes + 1);
        var notebook = new JsonObject
        {
            ["kind"] = "notebook",
            ["cells"] = new JsonArray(new JsonObject { ["type"] = "markdown", ["text"] = bigText }),
        };
        var result = DarlingWebEndpoints.ValidateDefinition(notebook.ToJsonString());
        Assert.False(result.IsValid);
        Assert.Contains("maximum size", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsANotebook_WithTooManyCells()
    {
        var cells = new JsonArray();
        for (var i = 0; i < DarlingWebEndpoints.MaxNotebookCells + 1; i++)
        {
            cells.Add(new JsonObject { ["type"] = "markdown", ["text"] = "x" });
        }

        var notebook = new JsonObject { ["kind"] = "notebook", ["cells"] = cells };
        var result = DarlingWebEndpoints.ValidateDefinition(notebook.ToJsonString());
        Assert.False(result.IsValid);
        Assert.Contains("maximum", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_StillAcceptsADashboard_AfterTheKindDispatch()
    {
        /* Regression: the D7 kind dispatch leaves the dashboard path untouched — a plain {panels} dashboard (no
           kind) and an explicit "kind":"dashboard" both validate through the same unchanged panels path. */
        var noKind = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}");
        Assert.True(noKind.IsValid, noKind.Error);

        var explicitDashboard = DarlingWebEndpoints.ValidateDefinition(
            "{\"kind\":\"dashboard\",\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}");
        Assert.True(explicitDashboard.IsValid, explicitDashboard.Error);
    }

    [Fact]
    public void ValidateDefinition_RejectsACellsDoc_SentWithoutTheNotebookKind()
    {
        /* Without "kind":"notebook", a cells-only doc is just a dashboard missing its panels — rejected, so a
           notebook can never be silently mistaken for (or stored as) an empty dashboard. */
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"cells\":[{\"type\":\"markdown\",\"text\":\"x\"}]}");
        Assert.False(result.IsValid);
        Assert.Contains("panels", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── D7: list-summary kind (badge/route) ─────────────────────────── */

    [Theory]
    [InlineData("notebook", "notebook")]
    [InlineData("dashboard", "dashboard")]
    /* absent kind (a legacy/plain dashboard) defaults to a concrete "dashboard" on the wire. */
    [InlineData(null, "dashboard")]
    public void BuildSummariesNode_EmitsAConcreteKind_DefaultingDashboard(string? storedKind, string expectedWireKind)
    {
        var summary = new CustomViewSummary(
            Id: 7, Name: "v", Description: null, Version: 1, UpdatedAt: DateTime.UnixEpoch, UpdatedBy: "web", Kind: storedKind);

        var node = DarlingWebEndpoints.BuildSummariesNode(new[] { summary });

        var one = Assert.IsType<JsonObject>(Assert.Single(node));
        Assert.Equal(expectedWireKind, one["kind"]!.GetValue<string>());
    }

    /* ─────────────────────────── D7: seed-template drift guard ─────────────────────────── */

    [Fact]
    public void ValidateDefinition_AcceptsEverySeedNotebookTemplate()
    {
        /* Drift guard for the frontend's NOTEBOOK_TEMPLATES (wwwroot/js/notebook.js): each template's PANEL cells
           are mirrored here verbatim (the markdown prose is abbreviated — ValidateDefinition only length-checks a
           cell's text), so if a future MeasureCatalog change renames/removes a measure, dimension, or annotation
           source a seed template relies on, THIS fails loudly instead of the template silently 400ing in the UI.
           If a template's panels are added to or restructured, mirror the change here. */
        var templates = new (string Name, string Definition)[]
        {
            ("blocking-rca",
                "{\"kind\":\"notebook\",\"cells\":[" +
                "{\"type\":\"markdown\",\"text\":\"# Blocking-chain RCA\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 1. Blocking over time\"}," +
                "{\"type\":\"panel\",\"source\":\"blocked_process_reports\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Blocked-process reports over time\",\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"count\",\"unit\":\"count\",\"annotations\":[\"deadlocks\"]}," +
                "{\"type\":\"markdown\",\"text\":\"## 2. Most-contended objects\"}," +
                "{\"type\":\"panel\",\"source\":\"blocked_process_reports\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Most-blocked objects\",\"groupBy\":[\"contentious_object\"],\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"count\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 3. Blocking by database\"}," +
                "{\"type\":\"panel\",\"source\":\"blocked_process_reports\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Blocking by database\",\"groupBy\":[\"database_name\"],\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"count\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 4. Lock modes involved\"}," +
                "{\"type\":\"panel\",\"source\":\"blocked_process_reports\",\"viz\":\"pie\",\"topN\":8,\"title\":\"Lock modes\",\"groupBy\":[\"lock_mode\"],\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"count\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"Next steps\"}]}"),

            ("deadlock-postmortem",
                "{\"kind\":\"notebook\",\"cells\":[" +
                "{\"type\":\"markdown\",\"text\":\"# Deadlock post-mortem\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Deadlocks over time\"}," +
                "{\"type\":\"panel\",\"source\":\"deadlocks\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Deadlocks over time\",\"measure\":\"deadlock_count\",\"aggregate\":\"count\",\"unit\":\"count\",\"annotations\":[\"blocked_process_reports\"]}," +
                "{\"type\":\"markdown\",\"text\":\"## Deadlocks by database\"}," +
                "{\"type\":\"panel\",\"source\":\"deadlocks\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Deadlocks by database\",\"groupBy\":[\"database_name\"],\"measure\":\"deadlock_count\",\"aggregate\":\"count\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Correlated blocking\"}," +
                "{\"type\":\"panel\",\"source\":\"blocked_process_reports\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Blocked-process reports (deadlock markers)\",\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"count\",\"unit\":\"count\",\"annotations\":[\"deadlocks\"]}," +
                "{\"type\":\"markdown\",\"text\":\"Reading the deadlock graph\"}]}"),

            ("what-changed",
                "{\"kind\":\"notebook\",\"cells\":[" +
                "{\"type\":\"markdown\",\"text\":\"# What changed at 2am?\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Default-trace events over time\"}," +
                "{\"type\":\"panel\",\"source\":\"default_trace_events\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Default-trace events over time\",\"measure\":\"deftrace_duration_us\",\"aggregate\":\"count\",\"unit\":\"count\",\"annotations\":[\"system_health_events\",\"deadlocks\"]}," +
                "{\"type\":\"markdown\",\"text\":\"## Events by type\"}," +
                "{\"type\":\"panel\",\"source\":\"default_trace_events\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Default-trace events by type\",\"groupBy\":[\"event_name\"],\"measure\":\"deftrace_duration_us\",\"aggregate\":\"count\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Events by database\"}," +
                "{\"type\":\"panel\",\"source\":\"default_trace_events\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Default-trace events by database\",\"groupBy\":[\"database_name\"],\"measure\":\"deftrace_duration_us\",\"aggregate\":\"count\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Correlate with CPU\"}," +
                "{\"type\":\"panel\",\"source\":\"cpu_utilization_stats\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"SQL Server CPU % (event markers)\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"unit\":\"percent\",\"annotations\":[\"default_trace_events\",\"system_health_events\"]}," +
                "{\"type\":\"markdown\",\"text\":\"Where to look next\"}]}"),

            ("memory-oom",
                "{\"kind\":\"notebook\",\"cells\":[" +
                "{\"type\":\"markdown\",\"text\":\"# Memory / OOM walkthrough\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Server memory over time\"}," +
                "{\"type\":\"panel\",\"source\":\"memory_stats\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Total server memory\",\"measure\":\"mem_total_server_mb\",\"aggregate\":\"avg\",\"unit\":\"mb\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Query-memory grants\"}," +
                "{\"type\":\"panel\",\"source\":\"memory_grant_stats\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Granted query memory\",\"measure\":\"grant_granted_mb\",\"aggregate\":\"avg\",\"unit\":\"mb\"}," +
                "{\"type\":\"panel\",\"source\":\"memory_grant_stats\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Query-memory grant used %\",\"ratio\":\"grant_used_pct\",\"unit\":\"percent\",\"thresholds\":[90]}," +
                "{\"type\":\"markdown\",\"text\":\"## Grant pressure\"}," +
                "{\"type\":\"panel\",\"source\":\"memory_grant_stats\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Memory-grant waiters (peak)\",\"measure\":\"grant_waiters\",\"aggregate\":\"max\",\"unit\":\"count\"}," +
                "{\"type\":\"panel\",\"source\":\"memory_grant_stats\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Memory-grant timeouts\",\"measure\":\"grant_timeouts\",\"aggregate\":\"sum\",\"unit\":\"count\"}," +
                "{\"type\":\"markdown\",\"text\":\"## Top memory clerks\"}," +
                "{\"type\":\"panel\",\"source\":\"memory_clerks\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Top memory clerks\",\"groupBy\":[\"clerk_type\"],\"measure\":\"clerk_memory_mb\",\"aggregate\":\"max\",\"unit\":\"mb\"}," +
                "{\"type\":\"markdown\",\"text\":\"RESOURCE_SEMAPHORE notes\"}]}"),

            /* #991: the AG Health seed. Exercises three shapes the other four seeds do not — a dual-axis
               overlay (ungrouped line + second measure), a stacked time series, and a scalar stat tile —
               so this entry is also the drift guard for those panel modes staying valid. */
            ("ag-health",
                "{\"kind\":\"notebook\",\"cells\":[" +
                "{\"type\":\"markdown\",\"text\":\"# Availability Group health\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 1. How far behind is each secondary?\"}," +
                "{\"type\":\"panel\",\"source\":\"ag_database_replica_states\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Secondary lag by replica\",\"measure\":\"ag_secondary_lag\",\"aggregate\":\"max\",\"unit\":\"s\",\"groupBy\":[\"replica_server_name\"]}," +
                "{\"type\":\"markdown\",\"text\":\"Reading the lag panel\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 2. Send vs redo: which side is the bottleneck?\"}," +
                "{\"type\":\"panel\",\"source\":\"ag_database_replica_states\",\"viz\":\"line\",\"timeBucket\":\"hour\",\"title\":\"Log send rate vs redo rate\",\"measure\":\"ag_log_send_rate\",\"aggregate\":\"avg\",\"unit\":\"kb\",\"overlay\":{\"measure\":\"ag_redo_rate\",\"aggregate\":\"avg\",\"unit\":\"kb\"}}," +
                "{\"type\":\"markdown\",\"text\":\"Reading the rate panel\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 3. Where is the redo backlog?\"}," +
                "{\"type\":\"panel\",\"source\":\"ag_database_replica_states\",\"viz\":\"stacked\",\"timeBucket\":\"hour\",\"title\":\"Redo queue by database\",\"measure\":\"ag_redo_queue\",\"aggregate\":\"max\",\"unit\":\"kb\",\"groupBy\":[\"database_name\"]}," +
                "{\"type\":\"markdown\",\"text\":\"## 4. How long until the redo queue clears?\"}," +
                "{\"type\":\"panel\",\"source\":\"ag_database_replica_states\",\"viz\":\"stat\",\"title\":\"Estimated redo drain (worst, minutes)\",\"measure\":\"ag_est_redo_drain_min\",\"aggregate\":\"max\",\"unit\":\"min\"}," +
                "{\"type\":\"markdown\",\"text\":\"Blank means no drain rate\"}," +
                "{\"type\":\"markdown\",\"text\":\"## 5. Which databases are backing up on the send side?\"}," +
                "{\"type\":\"panel\",\"source\":\"ag_database_replica_states\",\"viz\":\"bar\",\"topN\":10,\"title\":\"Log send queue by database\",\"groupBy\":[\"database_name\"],\"measure\":\"ag_log_send_queue\",\"aggregate\":\"max\",\"unit\":\"kb\"}," +
                "{\"type\":\"markdown\",\"text\":\"Next steps\"}]}"),
        };

        foreach (var (name, definition) in templates)
        {
            var result = DarlingWebEndpoints.ValidateDefinition(definition);
            Assert.True(result.IsValid, $"seed template '{name}' failed validation: {result.Error}");
        }

        /* The hole a hand-written mirror always has: it protects the templates it happens to list. Read the
           REAL key list out of notebook.js and require the mirror to cover exactly it, so a sixth template
           added without a mirror entry fails here instead of silently going unvalidated until it 400s in
           someone's browser. Source-scanned rather than executed — Darling.Tests has no JS runtime, and the
           same read-the-source idiom already backs the cross-app preset pin. */
        var declaredKeys = NotebookTemplateKeys();
        Assert.NotEmpty(declaredKeys);
        Assert.Equal(declaredKeys, templates.Select(t => t.Name).OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    /// <summary>Every <c>key:</c> declared in <c>NOTEBOOK_TEMPLATES</c> (wwwroot/js/notebook.js), sorted.
    /// <c>key:</c> appears nowhere else in that file, so a plain line scan is unambiguous.</summary>
    private static string[] NotebookTemplateKeys([CallerFilePath] string thisFile = "")
    {
        var testDir = Path.GetDirectoryName(thisFile)!;
        var notebookJs = Path.GetFullPath(Path.Combine(
            testDir, "..", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "notebook.js"));

        Assert.True(File.Exists(notebookJs), $"notebook.js not found at {notebookJs} (did the frontend move?)");

        return Regex.Matches(File.ReadAllText(notebookJs), @"^\s*key:\s*""([^""]+)""", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
    }

    /* ─────────────── #1665: availability-gated routing + the partial-window notice ─────────────── */

    /// <summary>
    /// The compile-level 42P01 repro: a store with NO rollups (plain PostgreSQL, or a probe that failed)
    /// must compile an old window against the raw table — never against a rollup relation that does not
    /// exist there. Raw is complete on that store shape, so this is the correct route, not a degraded one.
    /// </summary>
    [Fact]
    public void Compile_OldWindow_NoRollupsInStore_CompilesRaw()
    {
        var plan = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}");
        var end = WindowEnd;
        var (compiled, error) = ComposeCompiler.Compile(
            plan, new ComposeRunContext(null, end.AddDays(-10), end, ComposeRunContext.NoVariables, RollupAvailability.None, end, RollupCoverage.Unknown));
        Assert.True(error is null, error);
        Assert.Contains("FROM collect.query_stats AS f", compiled!.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("_hourly", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("_daily", compiled.Sql, StringComparison.Ordinal);
        Assert.Equal(ComposeSourceTier.Raw, compiled.Route.Tier);
    }

    /// <summary>The compiled result carries the route it took — the runner's input for the notice.</summary>
    [Fact]
    public void Compile_CarriesTheRouteItTook()
    {
        var (compiled, error) = CompileAged("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}", 10);
        Assert.True(error is null, error);
        Assert.Equal(ComposeSourceTier.Hourly, compiled!.Route.Tier);
    }

    /// <summary>
    /// The "partial window, and says so" notice (#1665): fires only when the route's tier cannot RETAIN the
    /// window's start on a retention-active store. Plain PG (no rollups) is silent — raw is complete there —
    /// and the daily tier is silent always (kept indefinitely).
    /// </summary>
    [Fact]
    public void RetentionNotice_FiresOnlyWhenTheTierCannotRetainTheWindow()
    {
        var now = new DateTime(2026, 7, 23, 12, 0, 0, DateTimeKind.Utc);
        var rawRoute = ComposeRoute.Raw;
        var hourlyRoute = new ComposeRoute(ComposeSourceTier.Hourly, "query_stats_hourly");
        var dailyRoute = new ComposeRoute(ComposeSourceTier.Daily, "query_stats_daily");

        /* Plain PG: silent even for a 40-day raw window — nothing ever dropped raw. */
        Assert.Null(ComposeStoreAvailability.BuildRetentionNotice("query_stats", rawRoute, now.AddDays(-40), now, RollupAvailability.None, RollupCoverage.Unknown));

        /* Retention-active store, raw route, window fits raw's ~4 days: silent. */
        Assert.Null(ComposeStoreAvailability.BuildRetentionNotice("query_stats", rawRoute, now.AddDays(-1), now, RollupAvailability.All, RollupCoverage.Unknown));

        /* Retention-active store, raw route, 10-day window: partial — says so, with the tier's horizon. */
        var rawNotice = ComposeStoreAvailability.BuildRetentionNotice("query_stats", rawRoute, now.AddDays(-10), now, RollupAvailability.All, RollupCoverage.Unknown);
        Assert.NotNull(rawNotice);
        Assert.Contains("partial window", rawNotice, StringComparison.Ordinal);
        Assert.Contains("4 days", rawNotice, StringComparison.Ordinal);
        Assert.Contains("10 days back", rawNotice, StringComparison.Ordinal);

        /* Hourly route PAST its horizon (daily view missing on this store): partial. The window and the
           expected text are both derived from HourlyRetentionSpan rather than written out, because #1937 moved
           that horizon from 21 days to 90 and a hardcoded window silently stopped exercising this branch —
           40 days used to be past the horizon and is now comfortably inside it. */
        var partial = RollupAvailability.All with { QueryGrainDaily = false };
        var pastHourly = now - TimescaleSupport.HourlyRetentionSpan - TimeSpan.FromDays(10);
        var hourlyNotice = ComposeStoreAvailability.BuildRetentionNotice("query_stats", hourlyRoute, pastHourly, now, partial, RollupCoverage.Unknown);
        Assert.NotNull(hourlyNotice);
        Assert.Contains(
            $"{TimescaleSupport.HourlyRetentionSpan.TotalDays:0} days",
            hourlyNotice,
            StringComparison.Ordinal);

        /* Hourly route, window INSIDE the horizon: silent. */
        Assert.Null(ComposeStoreAvailability.BuildRetentionNotice("query_stats", hourlyRoute, now.AddDays(-10), now, RollupAvailability.All, RollupCoverage.Unknown));

        /* Daily route: kept indefinitely — silent no matter the window. */
        Assert.Null(ComposeStoreAvailability.BuildRetentionNotice("query_stats", dailyRoute, now.AddDays(-400), now, RollupAvailability.All, RollupCoverage.Unknown));

        /* A NON-TIERED table (no CAGG pair) reaches Raw via the no-CAGG early return and lives on the
           30-day collector purge, not the 4-day tier — a 7-day wait_stats window is complete on raw, so
           a notice would be a false alarm even on a fully-built TimescaleDB store. */
        Assert.Null(ComposeStoreAvailability.BuildRetentionNotice("wait_stats", rawRoute, now.AddDays(-7), now, RollupAvailability.All, RollupCoverage.Unknown));
    }


    /* ─────────────── #1606 reporting layer: overlay (second measure), scatter, absolute windows ─────────────── */

    /// <summary>The overlay validation matrix: same-source only, coherent with viz/mode/groupBy, the same
    /// aggregate/unit rulebook as the primary, and scatter REQUIRES one.</summary>
    [Theory]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"overlay\":{\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\"}}", "must share the panel's source")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"scatter\"}", "needs an 'overlay'")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"groupBy\":[\"database_name\"],\"viz\":\"line\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\"}}", "cannot also group")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"groupBy\":[\"database_name\"],\"viz\":\"stacked\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\"}}", "cannot carry an overlay")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"bar\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\"}}", "cannot carry an overlay")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"scatter\",\"overlay\":{\"measure\":\"query_avg_elapsed_us\",\"aggregate\":\"sum\"}}", "aggregation is fixed")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"scatter\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"percentile_cont\"}}", "not valid for measure")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"scatter\",\"overlay\":{\"measure\":\"nope\"}}", "unknown measure")]
    [InlineData("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"scatter\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\"}}", "not a time series")]
    public void TryParsePanel_RejectsIncoherentOverlays(string json, string expectedFragment)
    {
        Assert.Contains(expectedFragment, RejectReason(json), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParsePanel_AcceptsAScatter_AndADualAxisLine()
    {
        var (scatter, scatterError) = ComposeSpec.TryParsePanel(PanelJson("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":25,\"groupBy\":[\"query_hash\"],\"viz\":\"scatter\",\"overlay\":{\"measure\":\"query_executions\",\"aggregate\":\"sum\"}}"), Array.Empty<string>());
        Assert.True(scatterError is null, scatterError);
        Assert.NotNull(scatter!.Overlay);
        Assert.Equal("query_executions", scatter.Overlay!.Measure.Key);

        /* A ratio overlay takes its fixed aggregation and needs no 'aggregate'. */
        var (dual, dualError) = ComposeSpec.TryParsePanel(PanelJson("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"overlay\":{\"measure\":\"query_avg_elapsed_us\"}}"), Array.Empty<string>());
        Assert.True(dualError is null, dualError);
        Assert.NotNull(dual!.Overlay);
        Assert.Equal(MeasureKind.Ratio, dual.Overlay!.Measure.Kind);
    }

    /// <summary>The overlay compiles as ONE more select expression over the same fact rows — never a join,
    /// never a parameter (the param-order pin: identical $n count with and without the overlay).</summary>
    [Fact]
    public void Compile_Overlay_EmitsValue2_AndBindsNoExtraParameters()
    {
        var with = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\"}}");
        var without = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}");
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.All, WindowEnd, RollupCoverage.Unknown);

        var (compiledWith, e1) = ComposeCompiler.Compile(with, context);
        var (compiledWithout, e2) = ComposeCompiler.Compile(without, context);
        Assert.True(e1 is null, e1);
        Assert.True(e2 is null, e2);

        Assert.Contains("AS value2", compiledWith!.Sql, StringComparison.Ordinal);
        Assert.Contains("SUM(f.delta_elapsed_time)", compiledWith.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("value2", compiledWithout!.Sql, StringComparison.Ordinal);
        Assert.Equal(compiledWithout.Parameters.Count, compiledWith.Parameters.Count);
    }

    [Fact]
    public void Compile_Scatter_RanksByPrimary_AndCarriesValue2()
    {
        var plan = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":25,\"groupBy\":[\"query_hash\"],\"viz\":\"scatter\",\"overlay\":{\"measure\":\"query_executions\",\"aggregate\":\"sum\"}}");
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.All, WindowEnd, RollupCoverage.Unknown);
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        Assert.Contains("AS value2", compiled!.Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY value DESC", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $", compiled.Sql, StringComparison.Ordinal);
    }

    /// <summary>The CAGG gate with an overlay (#1606): one route serves both value expressions, and a
    /// remappable pair rides the rollup with BOTH expressions remapped to the pre-aggregated columns.</summary>
    [Fact]
    public void Compile_OverlayCaggGate_RemappablePairRidesTheRollup()
    {
        var context = new ComposeRunContext(
            null, WindowEnd.AddDays(-10), WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.All, WindowEnd, RollupCoverage.Unknown);

        var both = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"overlay\":{\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\"}}");
        var (compiledBoth, e1) = ComposeCompiler.Compile(both, context);
        Assert.True(e1 is null, e1);
        Assert.Contains("query_stats_hourly", compiledBoth!.Sql, StringComparison.Ordinal);
        Assert.Contains("AS value2", compiledBoth.Sql, StringComparison.Ordinal);
        Assert.Contains("_sum", compiledBoth.Sql, StringComparison.Ordinal);
    }

    /// <summary>#1606 NowUtc decoupling: a purely-historical absolute window (30→25 days ago) must route by
    /// age from NOW — the daily rollup — never by age from the window's end (which would claim recency the
    /// retention tiers no longer have). This is the pin that keeps zoomed/historical windows honest.</summary>
    [Fact]
    public void Compile_HistoricalAbsoluteWindow_RoutesByAgeFromNow()
    {
        var now = WindowEnd;
        var plan = ValidPlan("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}");
        var context = new ComposeRunContext(
            null, now.AddDays(-120), now.AddDays(-115), ComposeRunContext.NoVariables, RollupAvailability.All, now, RollupCoverage.Unknown);
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        Assert.Contains("query_stats_daily", compiled!.Sql, StringComparison.Ordinal);
    }

    /// <summary>The absolute-window request validation (#1606): both-or-neither, parseable ISO, start
    /// strictly before end, span under the 90-day ceiling. All reject BEFORE any store round trip, so no
    /// live database is needed.</summary>
    [Theory]
    [InlineData("{\"windowStart\":\"2026-07-01T00:00:00Z\"}", "together")]
    [InlineData("{\"windowStart\":\"not-a-date\",\"windowEnd\":\"2026-07-02T00:00:00Z\"}", "ISO-8601")]
    [InlineData("{\"windowStart\":\"2026-07-02T00:00:00Z\",\"windowEnd\":\"2026-07-01T00:00:00Z\"}", "earlier than")]
    [InlineData("{\"windowStart\":\"2025-01-01T00:00:00Z\",\"windowEnd\":\"2026-07-01T00:00:00Z\"}", "ceiling")]
    public async Task RunComposedPanel_RejectsBadAbsoluteWindows(string windowJson, string expectedFragment)
    {
        var body = (JsonObject)JsonNode.Parse("{\"panel\":{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}}")!;
        foreach (var kv in (JsonObject)JsonNode.Parse(windowJson)!)
        {
            body[kv.Key] = kv.Value!.DeepClone();
        }

        await using var postgres = NpgsqlDataSource.Create("Host=localhost;Port=1;Database=never_reached;Username=x");
        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, CancellationToken.None);
        Assert.NotNull(outcome.Error);
        Assert.Contains(expectedFragment, outcome.Error!, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// The ONE live compose round-trip, split out of <see cref="DarlingComposeTests"/> (#1776). It connects to the
/// shared <c>DARLING_TEST_PG</c> store and runs <c>PgMigrations.MigrateAsync</c> against it, so it must serialize
/// with the rest of the live collection. Its 126 siblings are pure and must NOT: dragging a 127-test class into
/// the serialized collection to protect one test would cost the suite far more than the race it prevents, and the
/// established shape here is exactly this pair (more than forty files already split
/// <c>...SurfaceAndSqlTests</c> from <c>...LivePostgresTests</c> for the same reason).
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingComposeLivePostgresTests
{
    /// <summary>
    /// The live #1665 repro, end-to-end through the ONE shared runner: a >3-day window against a plain
    /// PostgreSQL store (the darling-pg CI service container — no TimescaleDB, so no rollups exist). Before
    /// the availability gate this compiled <c>collect.query_stats_hourly</c> and failed 42P01 at run time;
    /// now it routes raw, runs clean, and carries NO notice (raw is complete on this store shape).
    /// </summary>
    [Fact]
    public async Task RunComposedPanel_OldWindow_AgainstPlainPostgres_RunsCleanOnRaw()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live compose-routing test.");

        var ct = TestContext.Current.CancellationToken;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(
                "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}"),
            ["hours"] = 240, /* 10 days — far past the 3-day raw route horizon */
        };

        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, ct);

        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        Assert.NotNull(outcome.Payload);
        var sql = (string)outcome.Payload!["sql"]!;
        Assert.Contains("collect.query_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("_hourly", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("_daily", sql, StringComparison.Ordinal);
        Assert.Null(outcome.Payload["notice"]);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) execution of the composed Query Store panel against real Postgres. The rest of
/// the compose suite is string-only, so nothing else ever RUNS the compiler's output — and the #1841 raw
/// route is a hand-assembled derived table, exactly the shape where a string pin passes while Postgres
/// rejects the statement (or silently resolves a column the dedup forgot to project).
/// </summary>
[Collection("live-postgres")]
public sealed class ComposeQueryStoreLivePostgresTests
{
    private const int ServerId = -970820;
    private const string ServerName = "compose-qs-dedup";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ComposedQueryStorePanel_RunsOnPostgres_AndCountsEachIntervalOnce()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live compose Query Store test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, TestContext.Current.CancellationToken);

        var end = new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        var bucket = end.AddHours(-2);
        var firstExecA = bucket.AddMinutes(1);
        var firstExecB = bucket.AddMinutes(2);

        var bodySucceeded = false;
        try
        {
            /* Interval A re-collected three times with a FLAT count of 1, interval B twice while it grew
               10 -> 40. True total executions = 1 + 40 = 41; un-deduped = 3 + 50 = 53. */
            foreach (var minute in new[] { 5, 10, 15 })
            {
                await InsertAsync(connection, bucket.AddMinutes(minute), queryId: 1, planId: 11, firstExecA, execCount: 1);
            }

            await InsertAsync(connection, bucket.AddMinutes(5), queryId: 2, planId: 22, firstExecB, execCount: 10);
            await InsertAsync(connection, bucket.AddMinutes(10), queryId: 2, planId: 22, firstExecB, execCount: 40);

            var (plan, parseError) = ComposeSpec.TryParsePanel(
                (JsonObject)JsonNode.Parse("{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}")!,
                []);
            Assert.True(parseError is null, parseError);

            var (compiled, compileError) = ComposeCompiler.Compile(
                plan!,
                new ComposeRunContext([ServerName], end.AddHours(-3), end, ComposeRunContext.NoVariables,
                    RollupAvailability.All, end, RollupCoverage.Unknown));
            Assert.True(compileError is null, compileError);

            /* A 3-hour window ending now stays inside the raw tier, so this is the deduped raw route
               under test and not the CAGG (whose sums #1841 tier 2 still owes a fix). */
            Assert.False(compiled!.Route.IsCagg, "the window must stay on the raw route for this test to mean anything");

            await using var command = new NpgsqlCommand(compiled.Sql, connection);
            foreach (var parameter in compiled.Parameters)
            {
                command.Parameters.Add(parameter);
            }

            var total = 0.0;
            await using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
            {
                while (await reader.ReadAsync(TestContext.Current.CancellationToken))
                {
                    /* Time-series shape: bucket, then value. */
                    total += reader.GetDouble(reader.GetOrdinal("value"));
                }
            }

            Assert.Equal(41.0, total, 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ComposedQueryStoreTotalCpuPanel_RunsOnPostgres_AndSumsTheProductOverDedupedIntervals()
    {
        /* #2732: the WeightedSum raw route — SUM(avg_cpu_time_us * execution_count) — must ride the same #1841
           dedup wrapper as everything else on this source. Every row carries avg_cpu_time_us = 500, so the
           deduped total is 500 * 41 = 20500 µs; an un-deduped product-sum would read 500 * 53 = 26500. */
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live compose Query Store test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, TestContext.Current.CancellationToken);

        var end = new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        var bucket = end.AddHours(-2);
        var firstExecA = bucket.AddMinutes(1);
        var firstExecB = bucket.AddMinutes(2);

        var bodySucceeded = false;
        try
        {
            /* The dedup test's fixture: interval A re-collected three times flat at 1, interval B twice while
               it grew 10 -> 40. Deduped executions = 41. */
            foreach (var minute in new[] { 5, 10, 15 })
            {
                await InsertAsync(connection, bucket.AddMinutes(minute), queryId: 1, planId: 11, firstExecA, execCount: 1);
            }

            await InsertAsync(connection, bucket.AddMinutes(5), queryId: 2, planId: 22, firstExecB, execCount: 10);
            await InsertAsync(connection, bucket.AddMinutes(10), queryId: 2, planId: 22, firstExecB, execCount: 40);

            /* unit 'us' (not the 's' default) so the expected value stays integral. */
            var (plan, parseError) = ComposeSpec.TryParsePanel(
                (JsonObject)JsonNode.Parse("{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"unit\":\"us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}")!,
                []);
            Assert.True(parseError is null, parseError);

            var (compiled, compileError) = ComposeCompiler.Compile(
                plan!,
                new ComposeRunContext([ServerName], end.AddHours(-3), end, ComposeRunContext.NoVariables,
                    RollupAvailability.All, end, RollupCoverage.Unknown));
            Assert.True(compileError is null, compileError);
            Assert.False(compiled!.Route.IsCagg, "the window must stay on the raw route for this test to mean anything");

            await using var command = new NpgsqlCommand(compiled.Sql, connection);
            foreach (var parameter in compiled.Parameters)
            {
                command.Parameters.Add(parameter);
            }

            var total = 0.0;
            await using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
            {
                while (await reader.ReadAsync(TestContext.Current.CancellationToken))
                {
                    total += reader.GetDouble(reader.GetOrdinal("value"));
                }
            }

            Assert.Equal(20500.0, total, 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("DELETE FROM collect.query_store_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, long queryId, long planId, DateTime firstExecutionTimeUtc, long execCount)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_hash, execution_count, avg_duration_us, avg_cpu_time_us)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("ComposeDb");
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue("Regular");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(firstExecutionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("0xCOMPOSEHASH");
        command.Parameters.AddWithValue(execCount);
        command.Parameters.AddWithValue(1000L);
        command.Parameters.AddWithValue(500L);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
