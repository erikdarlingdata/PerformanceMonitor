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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Metrics;

/* #9013: the in-memory model behind the opt-in /metrics port. One snapshot is built per refresh and read by every
   scrape; nothing here touches the store. */

/// <summary>One gauge sample. <see cref="Labels"/> holds only label names from <see cref="DarlingMetricCatalog"/>.</summary>
internal sealed record MetricSample(string Name, IReadOnlyList<KeyValuePair<string, string>> Labels, double Value);

/// <summary>The newest collection time of one server, kept apart so the age is taken at scrape time.</summary>
internal sealed record ServerCollectionStamp(string Server, DateTime LastCollectionUtc);

/// <summary>Everything one refresh read, stamped with when it was built.</summary>
internal sealed record MetricsSnapshot(
    DateTime BuiltAtUtc,
    IReadOnlyList<MetricSample> Samples,
    IReadOnlyList<ServerCollectionStamp> LastCollections);

/// <summary>What a scrape sees: the snapshot (null before the first), whether per-server series are still served
/// (false once the snapshot is 5 minutes old or older), and the last refresh's outcome.</summary>
internal sealed record MetricsScrape(
    MetricsSnapshot? Snapshot,
    bool ServeServers,
    bool LastRefreshSucceeded,
    double? LastRefreshSeconds);

/// <summary>The metric names, help texts and label sets the port may ever write.</summary>
internal static class DarlingMetricCatalog
{
    public const string ServerLabel = "server";

    private static readonly string[] Server = { ServerLabel };
    private static readonly string[] None = Array.Empty<string>();
    private static readonly string[] DiskLabels = { ServerLabel, "file_type", "scope" };

    /// <summary>Label names and the only values each may take: no text from a monitored server reaches a label.</summary>
    public static readonly IReadOnlyDictionary<string, string[]> FixedLabelValues = new Dictionary<string, string[]>
    {
        /* #9013: read from WaitCategorySql.Categories, which lists what the shared wait-category CASE can return, not copied
           from it, so a new category cannot reach a scrape without reaching this set. */
        ["category"] = WaitCategorySql.Categories.Select(c => c.ToLowerInvariant()).ToArray(),
        ["file_type"] = new[] { "data", "log" },
        ["scope"] = new[] { "user", "tempdb" },
        ["kind"] = new[] { "user_objects", "internal_objects", "version_store" },
        ["state"] = new[] { "running", "sleeping", "background", "dormant" },
    };

    /// <summary>In writer order, the order <see cref="Build"/> adds them: the meta series (darling_metrics_*), the fleet set
    /// (darling_server_*, from the fleet read), then the server set (the readings from the server-set read).</summary>
    public static readonly IReadOnlyDictionary<string, (string Help, string[] Labels)> Metrics = Build();

    private static OrderedCatalog Build()
    {
        var catalog = new OrderedCatalog();

        catalog.Add("darling_metrics_snapshot_age_seconds", "Seconds since the metrics snapshot was built.", None);
        catalog.Add("darling_metrics_last_refresh_success", "1 if the last metrics refresh succeeded, else 0.", None);
        catalog.Add("darling_metrics_last_refresh_duration_seconds", "Seconds the last metrics refresh took.", None);

        catalog.Add("darling_server_health_band", "Fleet health band: 0 healthy, 1 warning, 2 critical, 3 offline.", Server);
        catalog.Add("darling_server_cpu_percent", "Total CPU utilisation of the server, percent (0-100).", Server);
        catalog.Add("darling_server_memory_mb", "Server memory in use, megabytes.", Server);
        catalog.Add("darling_server_buffer_pool_mb", "Buffer pool size, megabytes.", Server);
        catalog.Add("darling_server_blocking_events", "Blocking events in the last hour.", Server);
        catalog.Add("darling_server_deadlocks", "Deadlocks in the last hour.", Server);
        catalog.Add("darling_server_workers_current", "Current worker threads.", Server);
        catalog.Add("darling_server_workers_max", "Maximum worker threads.", Server);
        catalog.Add("darling_server_last_collection_age_seconds", "Seconds since the server's newest collection.", Server);
        catalog.Add("darling_server_failing_collectors", "Collectors currently failing for the server.", Server);
        catalog.Add("darling_server_collectors", "Collectors configured for the server.", Server);

        catalog.Add("darling_cpu_sql_percent", "SQL Server process CPU, percent (0-100).", Server);
        catalog.Add("darling_cpu_other_percent", "Other processes' CPU on the host, percent (0-100).", Server);
        catalog.Add("darling_scheduler_runnable_tasks", "Runnable tasks across the schedulers.", Server);
        catalog.Add("darling_batch_requests_per_second", "Batch requests per second.", Server);
        catalog.Add("darling_compilations_per_second", "SQL compilations per second.", Server);
        catalog.Add("darling_recompilations_per_second", "SQL recompilations per second.", Server);
        catalog.Add("darling_transactions_per_second", "Transactions per second.", Server);
        catalog.Add("darling_lock_waits_per_second", "Lock waits per second.", Server);
        catalog.Add("darling_processes_blocked", "Processes currently blocked.", Server);
        catalog.Add("darling_sessions", "Sessions by state.", new[] { ServerLabel, "state" });
        catalog.Add("darling_memory_total_server_mb", "Total server memory, megabytes.", Server);
        catalog.Add("darling_memory_target_server_mb", "Target server memory, megabytes.", Server);
        catalog.Add("darling_memory_plan_cache_mb", "Plan cache size, megabytes.", Server);
        catalog.Add("darling_memory_os_available_mb", "Physical memory available to the OS, megabytes.", Server);
        catalog.Add("darling_memory_grants_pending", "Queries waiting for a memory grant.", Server);
        catalog.Add("darling_memory_granted_mb", "Memory granted to queries, megabytes.", Server);
        catalog.Add("darling_lazy_writes_per_second", "Lazy writes per second.", Server);
        catalog.Add("darling_page_reads_per_second", "Page reads per second.", Server);
        catalog.Add("darling_page_writes_per_second", "Page writes per second.", Server);
        catalog.Add("darling_free_list_stalls_per_second", "Free list stalls per second.", Server);
        catalog.Add("darling_disk_read_latency_ms", "Average read latency, milliseconds.", DiskLabels);
        catalog.Add("darling_disk_write_latency_ms", "Average write latency, milliseconds.", DiskLabels);
        catalog.Add("darling_disk_read_mb_per_second", "Read throughput, megabytes per second.", DiskLabels);
        catalog.Add("darling_disk_write_mb_per_second", "Write throughput, megabytes per second.", DiskLabels);
        catalog.Add("darling_disk_reads_per_second", "Reads per second.", DiskLabels);
        catalog.Add("darling_disk_writes_per_second", "Writes per second.", DiskLabels);
        catalog.Add("darling_log_flushes_per_second", "Log flushes per second.", Server);
        catalog.Add("darling_log_bytes_flushed_per_second", "Log bytes flushed per second.", Server);
        catalog.Add("darling_disk_volume_min_free_percent", "Lowest free space across the server's volumes, percent (0-100).", Server);
        catalog.Add("darling_wait_ms_per_second", "Wait time by category, milliseconds per second.", new[] { ServerLabel, "category" });
        catalog.Add("darling_signal_wait_percent", "Signal wait share of all waits, percent (0-100).", Server);
        catalog.Add("darling_tempdb_used_mb", "tempdb space used by kind, megabytes.", new[] { ServerLabel, "kind" });
        catalog.Add("darling_tempdb_unallocated_mb", "tempdb unallocated space, megabytes.", Server);
        catalog.Add("darling_tempdb_log_used_percent", "tempdb log space used, percent (0-100).", Server);

        return catalog;
    }

    /// <summary>A read-only view that enumerates in insertion order (a plain Dictionary does not promise it).</summary>
    private sealed class OrderedCatalog : IReadOnlyDictionary<string, (string Help, string[] Labels)>
    {
        private readonly Dictionary<string, (string Help, string[] Labels)> _byName = new();
        private readonly List<string> _order = new();

        public void Add(string name, string help, string[] labels)
        {
            _byName.Add(name, (help, labels));
            _order.Add(name);
        }

        public (string Help, string[] Labels) this[string key] => _byName[key];
        public IEnumerable<string> Keys => _order;
        public IEnumerable<(string Help, string[] Labels)> Values
        {
            get
            {
                foreach (var k in _order)
                {
                    yield return _byName[k];
                }
            }
        }
        public int Count => _order.Count;
        public bool ContainsKey(string key) => _byName.ContainsKey(key);
        public bool TryGetValue(string key, out (string Help, string[] Labels) value) => _byName.TryGetValue(key, out value);
        public IEnumerator<KeyValuePair<string, (string Help, string[] Labels)>> GetEnumerator()
        {
            foreach (var k in _order)
            {
                yield return new(k, _byName[k]);
            }
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
