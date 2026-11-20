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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/* #9013: the Prometheus text writer for the opt-in /metrics port. Behaviour only: what a scraper would read. */
public class PrometheusTextTests
{
    private static readonly DateTime Now = new(2026, 11, 19, 12, 0, 0, DateTimeKind.Utc);
    private static readonly Regex LineShape = new(@"^(\w+)(\{(.*)\})? (\S+)$");

    private static IReadOnlyList<KeyValuePair<string, string>> Server(string name) =>
        new[] { new KeyValuePair<string, string>("server", name) };

    private static MetricsScrape Scrape(MetricsSnapshot snapshot, bool serveServers = true) =>
        new(snapshot, serveServers, true, 1.5);

    private static string[] Lines(string body) =>
        body.Split('\n', StringSplitOptions.RemoveEmptyEntries);

    private static string[] SampleLines(string body) =>
        Lines(body).Where(l => !l.StartsWith('#')).ToArray();

    [Fact]
    public void NoSnapshot_WritesMetaOnly()
    {
        var body = PrometheusText.Write(new MetricsScrape(null, false, false, null), Now);

        Assert.Contains("darling_metrics_last_refresh_success 0", Lines(body));
        Assert.DoesNotContain(SampleLines(body), l => l.StartsWith("darling_metrics_snapshot_age_seconds"));
        Assert.DoesNotContain(SampleLines(body), l => l.StartsWith("darling_metrics_last_refresh_duration_seconds"));
        Assert.DoesNotContain(Lines(body), l => l.Contains("darling_server_"));
    }

    [Fact]
    public void Snapshot_WritesSamples_WithScrapeTimeAges()
    {
        var snapshot = new MetricsSnapshot(
            Now.AddSeconds(-30),
            new[] { new MetricSample("darling_cpu_sql_percent", Server("a"), 12.5) },
            new[] { new ServerCollectionStamp("a", Now.AddSeconds(-90)) });

        var body = PrometheusText.Write(Scrape(snapshot), Now);

        Assert.Contains("darling_metrics_snapshot_age_seconds 30", Lines(body));
        Assert.Contains("darling_metrics_last_refresh_success 1", Lines(body));
        Assert.Contains("darling_metrics_last_refresh_duration_seconds 1.5", Lines(body));
        Assert.Contains("darling_server_last_collection_age_seconds{server=\"a\"} 90", Lines(body));
        Assert.Contains("darling_cpu_sql_percent{server=\"a\"} 12.5", Lines(body));
    }

    /// <summary>#9013: a wall clock that stepped back (or a collection stamped ahead of this host's clock) never reads as a negative
    /// age: the snapshot age and each collection age are clamped at 0.</summary>
    [Fact]
    public void AgesInTheFuture_AreClampedAtZero()
    {
        var snapshot = new MetricsSnapshot(
            Now.AddSeconds(30),
            new[] { new MetricSample("darling_cpu_sql_percent", Server("a"), 12.5) },
            new[] { new ServerCollectionStamp("a", Now.AddSeconds(45)) });

        var body = PrometheusText.Write(Scrape(snapshot), Now);

        Assert.Contains("darling_metrics_snapshot_age_seconds 0", Lines(body));
        Assert.Contains("darling_server_last_collection_age_seconds{server=\"a\"} 0", Lines(body));
    }

    [Fact]
    public void StaleSnapshot_DropsServerSeries_KeepsMeta()
    {
        var snapshot = new MetricsSnapshot(
            Now.AddMinutes(-6),
            new[] { new MetricSample("darling_cpu_sql_percent", Server("a"), 12.5) },
            new[] { new ServerCollectionStamp("a", Now.AddSeconds(-90)) });

        var body = PrometheusText.Write(Scrape(snapshot, serveServers: false), Now);

        Assert.DoesNotContain(Lines(body), l => l.Contains("darling_server_"));
        Assert.DoesNotContain(Lines(body), l => l.Contains("darling_cpu_"));
        Assert.Contains("darling_metrics_snapshot_age_seconds 360", Lines(body));
        Assert.Contains("darling_metrics_last_refresh_success 1", Lines(body));
    }

    [Fact]
    public void HelpAndType_AppearOncePerName_BeforeItsSamples()
    {
        var snapshot = new MetricsSnapshot(
            Now,
            new[]
            {
                new MetricSample("darling_cpu_sql_percent", Server("a"), 1),
                new MetricSample("darling_cpu_sql_percent", Server("b"), 2),
            },
            Array.Empty<ServerCollectionStamp>());

        var lines = Lines(PrometheusText.Write(Scrape(snapshot), Now));

        var help = Array.IndexOf(lines, lines.Single(l => l.StartsWith("# HELP darling_cpu_sql_percent ")));
        var type = Array.IndexOf(lines, "# TYPE darling_cpu_sql_percent gauge");
        var first = Array.IndexOf(lines, "darling_cpu_sql_percent{server=\"a\"} 1");
        var second = Array.IndexOf(lines, "darling_cpu_sql_percent{server=\"b\"} 2");

        Assert.Equal(1, lines.Count(l => l.StartsWith("# TYPE darling_cpu_sql_percent ")));
        Assert.True(help < type && type < first && first < second, "HELP, TYPE, then the samples");
    }

    [Fact]
    public void LabelValues_AreEscaped_AndTheBodyParses()
    {
        var snapshot = new MetricsSnapshot(
            Now,
            new[] { new MetricSample("darling_cpu_sql_percent", Server("a\"b\\c\nd{e}"), 1) },
            new[] { new ServerCollectionStamp("a\"b\\c\nd{e}", Now.AddSeconds(-5)) });

        var body = PrometheusText.Write(Scrape(snapshot), Now);

        Assert.Contains("darling_cpu_sql_percent{server=\"a\\\"b\\\\c\\nd{e}\"} 1", Lines(body));
        foreach (var line in SampleLines(body))
        {
            Assert.Matches(LineShape, line);
        }
    }

    [Fact]
    public void NaN_IsNeverWritten()
    {
        var snapshot = new MetricsSnapshot(
            Now,
            new[]
            {
                new MetricSample("darling_cpu_sql_percent", Server("nan"), double.NaN),
                new MetricSample("darling_cpu_sql_percent", Server("inf"), double.PositiveInfinity),
                new MetricSample("darling_cpu_sql_percent", Server("ninf"), double.NegativeInfinity),
                new MetricSample("darling_cpu_sql_percent", Server("ok"), 3),
            },
            Array.Empty<ServerCollectionStamp>());

        var body = PrometheusText.Write(Scrape(snapshot), Now);

        Assert.DoesNotContain("NaN", body);
        Assert.DoesNotContain("Inf", body);
        Assert.Contains("darling_cpu_sql_percent{server=\"ok\"} 3", Lines(body));
        Assert.Single(SampleLines(body), l => l.StartsWith("darling_cpu_sql_percent"));
    }

    [Fact]
    public void EveryCatalogLabel_IsServerOrAFixedLabel()
    {
        var allowed = new HashSet<string> { "server", "category", "file_type", "scope", "kind", "state" };

        foreach (var (name, (_, labels)) in DarlingMetricCatalog.Metrics)
        {
            Assert.All(labels, label => Assert.True(allowed.Contains(label), $"{name} has label {label}"));
        }
        Assert.All(DarlingMetricCatalog.FixedLabelValues.Keys, k => Assert.Contains(k, allowed));
    }
}
