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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8574: what a <c>compare_analysis</c> caller reads on a PostgreSQL target whose rows are banded on the baseline.
/// Darling's reply builder serializes <c>ComparisonRow.ToPayload()</c> and <c>ComparisonResult.SummaryPayload()</c>
/// from the shared <see cref="ComparisonBanding"/>, so these run the real scorer and the shared banding and read the
/// same payload the tool returns, with no store. The Lite side of the same rules is in
/// <c>ComparisonBandingTests</c>.
/// </summary>
public sealed class CompareAnalysisBandedReadingTests
{
    /// <summary>
    /// The issue's row: the shown fraction FELL while peak sessions rose, and the row said worse with no session reading
    /// on it. The row now carries both session readings with their metric and unit, and its verdict agrees with them.
    /// </summary>
    [Fact]
    public void ASaturationFractionThatFell_WhilePeakSessionsRose_CarriesBothSessionReadings_AndReadsWorse()
    {
        var (baseline, comparison) = Scored([Saturation(peak: 90, usable: 100)], [Saturation(peak: 95, usable: 110)]);
        Assert.True(comparison[0].Value < baseline[0].Value);

        var result = ComparisonBanding.Compare(baseline, comparison, Dispersion(median: 80), coverageCaveat: false);
        var row = Row(result, PgTargetFactKeys.ConnectionSaturation);

        Assert.True(row.GetProperty("value_delta").GetDouble() < 0);
        Assert.Equal(ComparisonBanding.BandSourceBaseline, row.GetProperty("band_source").GetString());
        Assert.Equal(90.0, row.GetProperty("baseline_banded_value").GetDouble());
        Assert.Equal(95.0, row.GetProperty("comparison_banded_value").GetDouble());
        Assert.Equal("peak_total_sessions", row.GetProperty("banded_metric").GetString());
        Assert.Equal("sessions", row.GetProperty("banded_unit").GetString());
        Assert.Equal(ComparisonBanding.StatusWorse, row.GetProperty("status").GetString());
    }

    /// <summary>
    /// A graded fact that is severity 0 in both windows has no verdict however many sigma it moved: it is not counted in
    /// the summary's worse or families_worse, and delta_sigma and beyond_anomaly_cutoff stay on the row. PG_TPS is
    /// context the scorer never grades, so its baseline verdict stands.
    /// </summary>
    [Fact]
    public void ANegligibleGradedMove_GetsNoVerdictAndIsNotCounted_WhileAnUngradedTpsSurgeKeepsItsVerdict()
    {
        var (baseline, comparison) = Scored(
            [Saturation(peak: 20, usable: 100), Tps(10)],
            [Saturation(peak: 60, usable: 100), Tps(60)]);
        Assert.Equal(0.0, baseline[0].Severity);
        Assert.Equal(0.0, comparison[0].Severity);

        var dispersion = Dispersion(median: 20);
        dispersion[MetricNames.PgTps] = Bucket(median: 10, mad: 0.6745);
        var result = ComparisonBanding.Compare(baseline, comparison, dispersion, coverageCaveat: false);

        var saturation = Row(result, PgTargetFactKeys.ConnectionSaturation);
        Assert.Equal(ComparisonBanding.StatusStable, saturation.GetProperty("status").GetString());
        Assert.Equal(20.0, saturation.GetProperty("delta_sigma").GetDouble());
        Assert.True(saturation.GetProperty("beyond_anomaly_cutoff").GetBoolean());

        var tps = Row(result, PgTargetFactKeys.Tps);
        Assert.Equal(ComparisonBanding.StatusWorse, tps.GetProperty("status").GetString());

        using var summaryDoc = JsonDocument.Parse(JsonSerializer.Serialize(result.SummaryPayload()));
        var summary = summaryDoc.RootElement;
        Assert.Equal(1, summary.GetProperty("worse").GetInt32());      // PG_TPS alone
        Assert.Equal(1, summary.GetProperty("stable").GetInt32());     // the saturation fact
        Assert.Equal(1, summary.GetProperty("families_worse").GetInt32());
    }

    private static JsonElement Row(ComparisonResult result, string key)
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Rows.Select(r => r.ToPayload()).ToList()));
        return doc.RootElement.EnumerateArray().Single(r => r.GetProperty("key").GetString() == key).Clone();
    }

    private static (List<Fact>, List<Fact>) Scored(List<Fact> baseline, List<Fact> comparison)
    {
        var scorer = new FactScorer();
        scorer.ScoreAll(baseline);
        scorer.ScoreAll(comparison);
        return (baseline, comparison);
    }

    /// <summary>The collector's shape: value = peak over usable connections, the peak count beside it, the ratio the scorer grades.</summary>
    private static Fact Saturation(double peak, double usable) => new()
    {
        Source = PgTargetSources.SessionsSource, Key = PgTargetFactKeys.ConnectionSaturation, Value = peak / usable,
        Metadata = new Dictionary<string, double>
        {
            ["peak_total_sessions"] = peak, ["usable_connections"] = usable, ["saturation_ratio"] = peak / usable
        }
    };

    private static Fact Tps(double tps) => new() { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.Tps, Value = tps };

    private static Dictionary<string, BaselineBucket> Dispersion(double median) =>
        new() { [MetricNames.PgSessionCount] = Bucket(median, mad: 0) };

    private static BaselineBucket Bucket(double median, double mad) => new()
    {
        HourOfDay = 9, DayOfWeek = 2, Tier = BaselineTier.Full,
        Mean = median, StdDev = Math.Max(mad, 1), Median = median, Mad = mad, SampleCount = 20, DistinctDays = 5
    };
}
