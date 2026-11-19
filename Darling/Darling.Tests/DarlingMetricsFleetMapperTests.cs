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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/* #9013: fleet cards become gauge samples; a missing reading is absent, never 0, and the server label is unique. */
public sealed class DarlingMetricsFleetMapperTests
{
    private static IReadOnlyDictionary<int, string> Labels(params (int Id, string Name)[] servers) =>
        servers.ToDictionary(s => s.Id, s => s.Name);

    private static double ValueOf(IReadOnlyList<MetricSample> samples, string name) =>
        samples.Single(s => s.Name == name).Value;

    [Theory]
    [InlineData(FleetHealthBand.Healthy, 0)]
    [InlineData(FleetHealthBand.Warning, 1)]
    [InlineData(FleetHealthBand.Critical, 2)]
    [InlineData(FleetHealthBand.Offline, 3)]
    public void Band_IsTheEnumOrdinal(FleetHealthBand band, int expected)
    {
        var card = new FleetServerCard { ServerId = 1, DisplayName = "a", Band = band };

        var (samples, _) = DarlingMetricsFleetMapper.Map(new[] { card }, Labels((1, "a")));

        Assert.Equal(expected, ValueOf(samples, "darling_server_health_band"));
    }

    [Fact]
    public void NullCardFields_AreAbsent_NeverZero()
    {
        var card = new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "a",
            BlockingCount = 4,
            FailedCollectorCount = 2,
            CollectorCount = 30,
            DeadlockMeasured = false,
            LastCollectionTime = new DateTime(2026, 11, 19, 10, 0, 0, DateTimeKind.Utc),
        };

        var (samples, stamps) = DarlingMetricsFleetMapper.Map(new[] { card }, Labels((1, "a")));

        Assert.Equal(
            new[]
            {
                "darling_server_health_band",
                "darling_server_blocking_events",
                "darling_server_failing_collectors",
                "darling_server_collectors",
            }.OrderBy(n => n),
            samples.Select(s => s.Name).OrderBy(n => n));
        Assert.Single(stamps);
    }

    [Fact]
    public void NoCollectionYet_EmitsTheBandOnly()
    {
        var card = new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "a",
            Band = FleetHealthBand.Warning,
            TotalCpuPercent = 50,
            BlockingCount = 0,
            FailedCollectorCount = 0,
            CollectorCount = 0,
            DeadlockMeasured = true,
            LastCollectionTime = null,
        };

        var (samples, stamps) = DarlingMetricsFleetMapper.Map(new[] { card }, Labels((1, "a")));

        var sample = Assert.Single(samples);
        Assert.Equal("darling_server_health_band", sample.Name);
        Assert.Equal((int)FleetHealthBand.Warning, sample.Value);
        Assert.Empty(stamps);
    }

    [Fact]
    public void AllReadings_MapToTheirMetrics_AndTheStamp()
    {
        var when = new DateTime(2026, 11, 19, 10, 0, 0, DateTimeKind.Utc);
        var card = new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "a",
            TotalCpuPercent = 12.5,
            MemoryMb = 1000,
            BufferPoolMb = 800,
            BlockingCount = 3,
            DeadlockCount = 2,
            DeadlockMeasured = true,
            CurrentWorkers = 40,
            TotalThreads = 512,
            LastCollectionTime = when,
            FailedCollectorCount = 1,
            CollectorCount = 20,
        };

        var (samples, stamps) = DarlingMetricsFleetMapper.Map(new[] { card }, Labels((1, "a")));

        Assert.Equal(12.5, ValueOf(samples, "darling_server_cpu_percent"));
        Assert.Equal(1000, ValueOf(samples, "darling_server_memory_mb"));
        Assert.Equal(800, ValueOf(samples, "darling_server_buffer_pool_mb"));
        Assert.Equal(3, ValueOf(samples, "darling_server_blocking_events"));
        Assert.Equal(2, ValueOf(samples, "darling_server_deadlocks"));
        Assert.Equal(40, ValueOf(samples, "darling_server_workers_current"));
        Assert.Equal(512, ValueOf(samples, "darling_server_workers_max"));
        Assert.Equal(1, ValueOf(samples, "darling_server_failing_collectors"));
        Assert.Equal(20, ValueOf(samples, "darling_server_collectors"));
        var stamp = Assert.Single(stamps);
        Assert.Equal("a", stamp.Server);
        Assert.Equal(when, stamp.LastCollectionUtc);
    }

    [Fact]
    public void Deadlocks_AbsentWhenNotMeasured()
    {
        var card = new FleetServerCard
        {
            ServerId = 1, DisplayName = "a", DeadlockCount = 5, DeadlockMeasured = false, LastCollectionTime = DateTime.UtcNow,
        };

        var (samples, _) = DarlingMetricsFleetMapper.Map(new[] { card }, Labels((1, "a")));

        /* The card has a collection, so its counts are written: the deadlock series alone is held back. */
        Assert.Contains(samples, s => s.Name == "darling_server_blocking_events");
        Assert.DoesNotContain(samples, s => s.Name == "darling_server_deadlocks");
    }

    [Fact]
    public void DuplicateDisplayNames_GetTheIdSuffix()
    {
        var labels = DarlingMetricsFleetMapper.AssignServerLabels(
            new[] { (7, "prod"), (9, "prod"), (3, "dev") }, out var collisions);

        Assert.Equal("prod #7", labels[7]);
        Assert.Equal("prod #9", labels[9]);
        Assert.Equal("dev", labels[3]);
        var set = Assert.Single(collisions);
        Assert.Equal(new[] { 7, 9 }, set.OrderBy(i => i));
    }

    [Fact]
    public void SuffixedLabel_CollidingWithARealName_IsStillUnique()
    {
        var labels = DarlingMetricsFleetMapper.AssignServerLabels(
            new[] { (7, "prod"), (9, "prod"), (3, "prod #7") }, out var collisions);

        Assert.Equal(3, labels.Values.Distinct(StringComparer.Ordinal).Count());
        Assert.NotEmpty(collisions);
        Assert.Contains(collisions, set => set.Contains(7) && set.Contains(9));
    }

    [Fact]
    public void CaseDistinctNames_StaySeparate()
    {
        var labels = DarlingMetricsFleetMapper.AssignServerLabels(
            new[] { (1, "Prod"), (2, "prod") }, out var collisions);

        Assert.Equal("Prod", labels[1]);
        Assert.Equal("prod", labels[2]);
        Assert.Empty(collisions);
    }

    [Fact]
    public void EverySample_HasOnlyTheServerLabel()
    {
        var card = new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "a",
            TotalCpuPercent = 1,
            MemoryMb = 1,
            BufferPoolMb = 1,
            DeadlockMeasured = true,
            CurrentWorkers = 1,
            TotalThreads = 1,
            LastCollectionTime = DateTime.UtcNow,
        };

        var (samples, stamps) = DarlingMetricsFleetMapper.Map(new[] { card }, Labels((1, "a #1")));

        Assert.NotEmpty(samples);
        Assert.All(samples, s =>
        {
            var label = Assert.Single(s.Labels);
            Assert.Equal(DarlingMetricCatalog.ServerLabel, label.Key);
            Assert.Equal("a #1", label.Value);
            Assert.True(DarlingMetricCatalog.Metrics.ContainsKey(s.Name));
        });
        Assert.Equal("a #1", Assert.Single(stamps).Server);
    }
}
