/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The lightweight store reads that drive the shell chrome ported from Lite's MainWindow: the sidebar
/// status dot (one <c>v_collection_log</c> freshness query for every server at once) and the status bar's
/// database-size field (<c>pg_database_size</c>). Both are single round-trips so they can run on the
/// refresh timers without weighing anything down; the SQL lives in public constants so tests can pin the
/// load-bearing clauses without a live Postgres.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Newest collection time per server across all collectors, in one pass — the sidebar dots and the
    /// status bar's collection field derive freshness from this (the same <c>MAX(collection_time)</c> the
    /// Overview cards use per server, so a dot and its card agree). Timestamps are the store's naive UTC.
    /// Excludes <c>server_id = 0</c>, the fleet-level retention run-record sentinel
    /// (<c>DarlingObservability.FleetServerId</c>) — it is not a real server, so it must not appear as a
    /// phantom key a future key-iterating consumer could render as "server 0".
    /// </summary>
    public const string ServerFreshnessSql = @"
SELECT server_id, MAX(collection_time)
FROM v_collection_log
WHERE server_id <> 0
GROUP BY server_id";

    /// <summary>The store's on-disk size in bytes (status-bar Database field). No parameters.</summary>
    public const string StoreSizeSql = "SELECT pg_database_size(current_database())";

    /// <summary>
    /// Newest run of ANY status per (server, collector) — the raw side of the per-server cadence-aware
    /// stale threshold (#3236), the same aggregate the service's <c>DarlingFleetReader</c> reads so a
    /// viewer card and a fleet card cannot band the same server's freshness differently. Bounded to 48
    /// hours (TimescaleDB chunk exclusion; a collector quiet longer than that vouches for nothing anyway)
    /// and excluding the <c>server_id = 0</c> retention sentinel like <see cref="ServerFreshnessSql"/>.
    /// $1 window start (naive UTC).
    /// </summary>
    public const string CollectorCadenceSamplesSql = @"
SELECT server_id, collector_name, MAX(collection_time) AS last_run_time
FROM v_collection_log
WHERE server_id <> 0
AND   collection_time >= $1
GROUP BY server_id, collector_name";

    /// <summary>
    /// Reads MAX(collection_time) for every server in a single query, keyed by server_id. A server with no
    /// collection rows simply isn't in the dictionary (the caller treats a miss as "no collection" → Offline).
    /// </summary>
    public async Task<Dictionary<int, DateTime>> GetServerFreshnessAsync(CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<int, DateTime>();

        await using var command = _dataSource.CreateCommand(ServerFreshnessSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(1))
            {
                result[reader.GetInt32(0)] = reader.GetDateTime(1);
            }
        }

        return result;
    }

    /// <summary>The store database's size in bytes, or null when it can't be read.</summary>
    public async Task<long?> GetStoreSizeBytesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(StoreSizeSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null || result == DBNull.Value ? null : Convert.ToInt64(result);
    }

    /// <summary>
    /// Per-server <see cref="CollectorCadenceSample"/> lists for the cadence-aware stale threshold (#3236):
    /// each enabled scheduled collector's newest run beside the cadence it actually runs on for that server.
    /// Cadence resolution goes through the viewer's own <see cref="CollectorScheduleOverlay"/> — the SAME
    /// per-column layering (per-server override &gt; fleet override &gt; code default) the service's
    /// <c>StoreConfigProvider.ResolveSchedule</c> applies, already pinned against it by the schedule-editor
    /// tests — so the viewer bands on the schedule the sweep genuinely runs, not on the shipped defaults.
    /// A collector the overlay does not know (a store written by a newer build) or that resolves disabled /
    /// on-load contributes nothing, which falls toward the flat floor.
    /// </summary>
    public async Task<Dictionary<int, List<CollectorCadenceSample>>> GetCollectorCadenceSamplesAsync(CancellationToken cancellationToken = default)
    {
        var overrides = await GetCollectorSchedulesAsync(cancellationToken);

        /* (server, collector, newest run) first, then one effective-schedule overlay per server — the
           overlay builds the full per-collector schedule, so resolving it once per server rather than once
           per row keeps this a cheap pass even on a wide fleet. */
        var lastRuns = new Dictionary<int, List<(string CollectorName, DateTime LastRunUtc)>>();
        await using (var command = _dataSource.CreateCommand(CollectorCadenceSamplesSql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<DateTime>
            {
                TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-48), DateTimeKind.Unspecified),
            });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.IsDBNull(2))
                {
                    continue;
                }

                var serverId = reader.GetInt32(0);
                if (!lastRuns.TryGetValue(serverId, out var list))
                {
                    list = new List<(string, DateTime)>();
                    lastRuns[serverId] = list;
                }

                list.Add((reader.GetString(1), reader.GetDateTime(2)));
            }
        }

        var samples = new Dictionary<int, List<CollectorCadenceSample>>(lastRuns.Count);
        foreach (var (serverId, runs) in lastRuns)
        {
            var effective = CollectorScheduleOverlay.BuildEffectiveSchedule(overrides, serverId);
            var byName = new Dictionary<string, CollectorScheduleEditItem>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in effective)
            {
                byName[item.Name] = item;
            }

            var serverSamples = new List<CollectorCadenceSample>(runs.Count);
            foreach (var (collectorName, lastRunUtc) in runs)
            {
                if (byName.TryGetValue(collectorName, out var schedule)
                    && schedule.Enabled
                    && schedule.FrequencyMinutes > 0)
                {
                    serverSamples.Add(new CollectorCadenceSample(lastRunUtc, schedule.FrequencyMinutes));
                }
            }

            if (serverSamples.Count > 0)
            {
                samples[serverId] = serverSamples;
            }
        }

        return samples;
    }
}
