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
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8322: <c>CONFIG_PG_SHARED_BUFFERS</c> ("shared_buffers is 128 MB, the initdb default ... nobody sized it") rooted on an
/// Aurora PostgreSQL target, where the engine sizes <c>shared_buffers</c> from the instance's capacity and moves it as the
/// capacity moves. The initdb story assumes a self-managed server. The collector stamps the knob <c>not_applicable</c> on
/// Aurora, the same stamp <c>max_wal_size</c> takes there, the scorer reads the stamp as base 0 so no advisory roots and no
/// co-fire can lift it, and the buffer-pressure card's remediation says the service owns the value and points at the
/// capacity figures instead of telling the operator to raise it.
/// </summary>
public sealed class PgSharedBuffersManagedByServiceTests
{
    [Fact]
    public void TheKnobAtTheInitdbDefault_ScoresZero_WhenStampedNotApplicable()
    {
        var aurora = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        aurora.Metadata["not_applicable"] = 1;
        aurora.Metadata["not_applicable_on_aurora"] = 1;

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(aurora));
    }

    [Theory]
    [InlineData(128, PgTargetScorer.ConfigAdvisoryBase)]
    [InlineData(64, PgTargetScorer.ConfigAdvisoryBase)]
    [InlineData(256, 0.0)]
    public void TheKnobWithoutTheStamp_KeepsTheInitdbCheck(double mb, double expected)
    {
        Assert.Equal(expected, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigSharedBuffers, mb)));
    }

    /// <summary>The stamp also keeps the knob from corroborating the composite: FactScorer skips amplifiers at base 0.</summary>
    [Fact]
    public void TheStampedKnob_DoesNotCorroborateABufferPressureFinding()
    {
        var knob = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        knob.Metadata["not_applicable"] = 1;
        var pressure = Pressure(missShare: 0.3);
        new FactScorer().ScoreAll([knob, pressure]);

        Assert.Equal(0.0, knob.BaseSeverity);
        Assert.Equal(0.0, knob.Severity);
        Assert.Equal(pressure.BaseSeverity, pressure.Severity, precision: 6);

        var plain = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        var plainPressure = Pressure(missShare: 0.3);
        new FactScorer().ScoreAll([plain, plainPressure]);

        Assert.True(plain.Severity > plain.BaseSeverity, "a self-managed server's default knob is still lifted by the co-fire");
        Assert.True(plainPressure.Severity > plainPressure.BaseSeverity, "and still corroborates the composite");
    }

    [Fact]
    public void TheBufferPressureRemediation_OnAurora_NamesTheServiceAndTheCapacityFigures_NotInitdb()
    {
        var knob = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        knob.Metadata["not_applicable"] = 1;
        knob.Metadata["not_applicable_on_aurora"] = 1;
        var pressure = Pressure(missShare: 0.3, aurora: true);
        new FactScorer().ScoreAll([knob, pressure]);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.BufferCachePressure, Lookup(pressure, knob))!;

        Assert.DoesNotContain("Raise it toward a quarter", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("initdb", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("128 MB", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("capacity", advice.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("get_pg_cpu_utilization", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBufferPressureRemediation_OnASelfManagedServer_StillSaysRaiseIt()
    {
        var knob = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        var pressure = Pressure(missShare: 0.3);
        new FactScorer().ScoreAll([knob, pressure]);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.BufferCachePressure, Lookup(pressure, knob))!;

        Assert.Contains("Raise it toward a quarter", advice.Remediation, StringComparison.Ordinal);
    }

    private static Fact Config(string key, double value) =>
        new() { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };

    /// <summary>
    /// A composite with one arm graded above its concerning line: the hit-ratio arm on a self-managed server, the
    /// eviction arm on Aurora, where the hit-ratio arm is suppressed.
    /// </summary>
    private static Fact Pressure(double missShare, bool aurora = false) =>
        new()
        {
            Source = PgTargetSources.BufferSource,
            Key = PgTargetFactKeys.BufferCachePressure,
            Value = missShare,
            ServerId = 1,
            Metadata =
            {
                ["hit_ratio_suppressed"] = aurora ? 1 : 0,
                ["block_requests_per_sec"] = 500,
                ["miss_share"] = missShare,
                ["evictions_tracked"] = aurora ? 1 : 0,
                ["cache_turnovers_per_hour"] = 5,
                ["bgwriter_tracked"] = 0,
            },
        };

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts) =>
        facts.ToDictionary(f => f.Key, f => f, StringComparer.Ordinal);
}
