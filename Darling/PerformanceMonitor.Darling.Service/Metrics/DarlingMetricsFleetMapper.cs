/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.Metrics;

/* #9013: turns the web dashboard's fleet cards into the fleet metric samples. A reading the card does not have is left
   out, never written as 0: a gauge of 0 would read as a measurement. */
internal static class DarlingMetricsFleetMapper
{
    /// <summary>The unique <c>server</c> label per server id. A name used by one server is kept; a name shared by several
    /// becomes "name #id" on each, and each shared set is returned so the caller can log it once per refresh.</summary>
    internal static IReadOnlyDictionary<int, string> AssignServerLabels(
        IEnumerable<(int ServerId, string DisplayName)> servers,
        out IReadOnlyList<IReadOnlyList<int>> collisions)
    {
        var list = servers.ToList();
        var labels = new Dictionary<int, string>(list.Count);
        var sets = new List<IReadOnlyList<int>>();

        foreach (var group in list.GroupBy(s => s.DisplayName, StringComparer.Ordinal))
        {
            var ids = group.Select(s => s.ServerId).Distinct().OrderBy(id => id).ToList();
            if (ids.Count == 1)
            {
                labels[ids[0]] = group.Key;
                continue;
            }

            sets.Add(ids);
            foreach (var id in ids)
            {
                labels[id] = group.Key + " #" + id.ToString(CultureInfo.InvariantCulture);
            }
        }

        collisions = sets;
        return labels;
    }

    /// <summary>The fleet samples and the per-server newest-collection stamps. The age series is not a sample: the writer
    /// takes it from the stamps at scrape time.</summary>
    internal static (IReadOnlyList<MetricSample> Samples, IReadOnlyList<ServerCollectionStamp> Stamps) Map(
        IReadOnlyList<FleetServerCard> cards,
        IReadOnlyDictionary<int, string> labels)
    {
        var samples = new List<MetricSample>(cards.Count * 10);
        var stamps = new List<ServerCollectionStamp>(cards.Count);

        foreach (var card in cards)
        {
            var server = labels.TryGetValue(card.ServerId, out var label) ? label : card.DisplayName;
            var serverLabels = new[] { new KeyValuePair<string, string>(DarlingMetricCatalog.ServerLabel, server) };

            void Add(string name, double? value)
            {
                if (value.HasValue)
                {
                    samples.Add(new MetricSample(name, serverLabels, value.Value));
                }
            }

            Add("darling_server_health_band", (int)card.Band);
            Add("darling_server_cpu_percent", card.TotalCpuPercent);
            Add("darling_server_memory_mb", card.MemoryMb);
            Add("darling_server_buffer_pool_mb", card.BufferPoolMb);
            Add("darling_server_blocking_events", card.BlockingCount);
            Add("darling_server_deadlocks", card.DeadlockMeasured ? card.DeadlockCount : null);
            Add("darling_server_workers_current", card.CurrentWorkers);
            Add("darling_server_workers_max", card.TotalThreads);
            Add("darling_server_failing_collectors", card.FailedCollectorCount);
            Add("darling_server_collectors", card.CollectorCount);

            if (card.LastCollectionTime.HasValue)
            {
                stamps.Add(new ServerCollectionStamp(server, card.LastCollectionTime.Value));
            }
        }

        return (samples, stamps);
    }
}
