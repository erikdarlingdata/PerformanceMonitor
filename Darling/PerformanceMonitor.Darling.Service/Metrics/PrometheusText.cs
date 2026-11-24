/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Darling.Service.Metrics;

/// <summary>
/// Writes a <see cref="MetricsScrape"/> as Prometheus text exposition 0.0.4: gauges only, no sample timestamps.
/// Ages are taken from <c>nowUtc</c> at scrape time so a cached snapshot never reports a frozen age.
/// </summary>
internal static class PrometheusText
{
    public const string ContentType = "text/plain; version=0.0.4; charset=utf-8";

    private const string SnapshotAge = "darling_metrics_snapshot_age_seconds";
    private const string RefreshSuccess = "darling_metrics_last_refresh_success";
    private const string RefreshDuration = "darling_metrics_last_refresh_duration_seconds";
    private const string CollectionAge = "darling_server_last_collection_age_seconds";

    public static string Write(MetricsScrape scrape, DateTime nowUtc)
    {
        var byName = new Dictionary<string, List<MetricSample>>();
        void Add(MetricSample sample)
        {
            if (!DarlingMetricCatalog.Metrics.ContainsKey(sample.Name))
            {
                Debug.Fail("Metric not in the catalog: " + sample.Name);
                return;
            }
            /* #9013: a non-finite value has no valid text form for a scraper that rejects the whole body. */
            if (!double.IsFinite(sample.Value))
            {
                return;
            }
            if (!byName.TryGetValue(sample.Name, out var list))
            {
                list = new List<MetricSample>();
                byName[sample.Name] = list;
            }
            list.Add(sample);
        }

        var none = Array.Empty<KeyValuePair<string, string>>();
        if (scrape.Snapshot != null)
        {
            /* #9013: clamped at 0, so a wall clock that stepped back since the snapshot was built never reads as a negative age. */
            Add(new MetricSample(SnapshotAge, none, Math.Max(0d, (nowUtc - scrape.Snapshot.BuiltAtUtc).TotalSeconds)));
        }
        Add(new MetricSample(RefreshSuccess, none, scrape.LastRefreshSucceeded ? 1 : 0));
        if (scrape.LastRefreshSeconds is double seconds)
        {
            Add(new MetricSample(RefreshDuration, none, seconds));
        }

        if (scrape.Snapshot != null && scrape.ServeServers)
        {
            foreach (var sample in scrape.Snapshot.Samples)
            {
                Add(sample);
            }
            foreach (var stamp in scrape.Snapshot.LastCollections)
            {
                var label = new[] { new KeyValuePair<string, string>(DarlingMetricCatalog.ServerLabel, stamp.Server) };
                /* #9013: clamped at 0 like the snapshot age, so a clock step or a collection stamped ahead never reads as negative. */
                Add(new MetricSample(CollectionAge, label, Math.Max(0d, (nowUtc - stamp.LastCollectionUtc).TotalSeconds)));
            }
        }

        var sb = new StringBuilder();
        foreach (var (name, (help, _)) in DarlingMetricCatalog.Metrics)
        {
            if (!byName.TryGetValue(name, out var samples) || samples.Count == 0)
            {
                continue;
            }
            sb.Append("# HELP ").Append(name).Append(' ').Append(help).Append('\n');
            sb.Append("# TYPE ").Append(name).Append(" gauge\n");
            foreach (var sample in samples)
            {
                sb.Append(name);
                if (sample.Labels.Count > 0)
                {
                    sb.Append('{');
                    for (var i = 0; i < sample.Labels.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(',');
                        }
                        sb.Append(sample.Labels[i].Key).Append("=\"").Append(Escape(sample.Labels[i].Value)).Append('"');
                    }
                    sb.Append('}');
                }
                sb.Append(' ').Append(sample.Value.ToString("R", CultureInfo.InvariantCulture)).Append('\n');
            }
        }
        return sb.ToString();
    }

    private static string Escape(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal)
             .Replace("\"", "\\\"", StringComparison.Ordinal)
             .Replace("\n", "\\n", StringComparison.Ordinal);
}
