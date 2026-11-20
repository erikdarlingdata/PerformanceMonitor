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
        var labels = new Dictionary<int, string>();
        foreach (var (serverId, displayName) in servers)
        {
            labels[serverId] = displayName;
        }

        /* A suffix can itself meet another server's real name ("prod #7" next to two "prod"), so repeat until no value is
           shared. It ends because ids are unique: each round makes every member of a shared value longer by its own id. */
        var sets = new List<IReadOnlyList<int>>();
        while (true)
        {
            var shared = labels
                .GroupBy(kv => kv.Value, StringComparer.Ordinal)
                .Where(g => g.Count() > 1)
                .Select(g => g.Select(kv => kv.Key).OrderBy(id => id).ToList())
                .ToList();
            if (shared.Count == 0)
            {
                break;
            }

            foreach (var ids in shared)
            {
                sets.Add(ids);
                foreach (var id in ids)
                {
                    labels[id] = labels[id] + " #" + id.ToString(CultureInfo.InvariantCulture);
                }
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

            /* #9013: a server with no data yet gets its band and no value series, never zeros, so it never looks like an idle server. The
               card's counts are plain ints, so before a first collection they would read as measured zeros. */
            if (!card.LastCollectionTime.HasValue)
            {
                continue;
            }

            Add("darling_server_cpu_percent", card.TotalCpuPercent);
            Add("darling_server_memory_mb", card.MemoryMb);
            Add("darling_server_buffer_pool_mb", card.BufferPoolMb);
            Add("darling_server_blocking_events", card.BlockingCount);
            Add("darling_server_deadlocks", card.DeadlockMeasured ? card.DeadlockCount : null);
            Add("darling_server_workers_current", card.CurrentWorkers);
            Add("darling_server_workers_max", card.TotalThreads);
            Add("darling_server_failing_collectors", card.FailedCollectorCount);
            Add("darling_server_collectors", card.CollectorCount);

            stamps.Add(new ServerCollectionStamp(server, card.LastCollectionTime.Value));
        }

        return (samples, stamps);
    }
}
