/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store self-metrics MCP surface (#2068) — "how fast is the monitoring store growing and what's
/// driving it" as a query instead of an expedition. Reads the series the hourly <c>StoreSelfMetrics</c>
/// sweep persists: the latest size/compression snapshot per object (each hypertable, each payload
/// dimension table, the whole store) plus the daily series for the window, with the whole-store daily
/// growth and the derived per-server ingest rate — the number onboarding N servers multiplies. Store-level
/// by nature, so unlike almost every other read tool it takes no <c>server_name</c>: the store is the
/// server.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreMetricsTools
{
    /// <summary>The window ceiling — the sweep's own retention (<see cref="StoreSelfMetrics.RetentionDays"/>),
    /// past which there is nothing to read.</summary>
    public const int MaxDaysBack = StoreSelfMetrics.RetentionDays;

    [McpServerTool(Name = "get_store_metrics"), Description(
        "Gets the monitoring store's OWN size and growth metrics — not a monitored SQL Server's. The service records an hourly self-metrics snapshot: per-hypertable total size, pre/post-compression bytes and chunk count; the query-text and query-plan payload dimension tables' total size (the store's dominant payloads) and row counts; the whole store's size with the enabled-server count; and one row per TimescaleDB background job (CAGG refresh, compression, retention) with its last run duration, schedule interval, duration-vs-cadence percent, and run/failure totals — the jobs whose runtimes scale with fleet size. Returns the latest snapshot per object plus a daily series over the window, with the whole-store daily growth in bytes and the derived per-server ingest rate (daily growth / enabled servers). Also reports, read LIVE from the catalog rather than from the recorded series, every retention policy the rollup-coverage gate is holding PAUSED, with the tier's actual data span and how many times its configured drop_after horizon it is really holding — a held policy records zero failures and a normal-looking last run, so it is invisible in the stored job telemetry and is a common cause of unexplained store growth. Use for capacity forecasting: what is driving store growth, how fast, what adding N servers would multiply, which background job is closest to outgrowing its own cadence, and whether retention is actually running.")]
    public static async Task<string> GetStoreMetrics(
        NpgsqlDataSource postgres,
        [Description("Days of daily-series history. Default 30; max 400 (the series' own retention).")] int days_back = 30)
    {
        if (days_back <= 0 || days_back > MaxDaysBack)
        {
            return $"Invalid days_back value '{days_back}'. Must be a positive integer (1-{MaxDaysBack}).";
        }

        try
        {
            var latest = await DarlingStoreMetricsReader.GetLatestAsync(postgres);
            if (latest.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No store self-metrics recorded yet. The service records a snapshot hourly (the first lands " +
                    "within an hour of starting on a store at schema V53 or later).");
            }

            var daily = await DarlingStoreMetricsReader.GetDailyAsync(
                postgres, DateTime.UtcNow.AddDays(-days_back));

            var storeDaily = daily
                .Where(p => p.ObjectKind == "store")
                .OrderBy(p => p.Day)
                .ToList();
            var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(storeDaily);

            var storeLatest = latest.FirstOrDefault(r => r.ObjectKind == "store");

            /* #2813: retention holds are read LIVE from the catalog, not from the series, because the
               series does not carry them. StoreSelfMetrics records total_runs and total_failures but not
               j.scheduled, so a policy the coverage gate has PAUSED reports zero failures and a plausible
               last run — indistinguishable from a healthy job in every stored column. On the production
               store that shape hid five held policies for 16 days while the tier grew to 4.5x its horizon.
               One catalog round trip on an already-open connection answers it for the current moment;
               persisting it into the series is a migration rung's worth of work and the better long-term
               answer, tracked separately. */
            List<RetentionHoldReading> holds;
            await using (var connection = await postgres.OpenConnectionAsync())
            {
                holds = (await TimescaleSupport.ReadRetentionHoldReadingsAsync(connection, logger: null))
                    .ToList();
            }

            var heldPolicies = holds
                .Where(h => !h.Armed)
                .OrderByDescending(h => h.OverHorizonRatio ?? 0)
                .ToList();

            return JsonSerializer.Serialize(new
            {
                as_of = latest.Max(r => r.MetricTime).ToString("o"),
                days_back,
                store = storeLatest is null ? null : new
                {
                    name = storeLatest.ObjectName,
                    total_bytes = storeLatest.TotalBytes,
                    enabled_server_count = storeLatest.EnabledServerCount,
                    daily_growth = growth.Select(g => new
                    {
                        day = g.Day.ToString("yyyy-MM-dd"),
                        delta_bytes = g.DeltaBytes,
                        per_server_bytes = g.PerServerBytes is { } rate ? Math.Round(rate) : (double?)null,
                    }),
                },
                /* #2813. Present on EVERY response, including when nothing is held — an absent block and
                   "nothing is held" must not look alike, which is the entire failure this reports on. */
                retention = new
                {
                    policy_count = holds.Count,
                    held_count = heldPolicies.Count,
                    note = holds.Count == 0
                        ? "No retention policies found (a plain-PostgreSQL store, or TimescaleDB is unavailable)."
                        : heldPolicies.Count == 0
                            ? "Every retention policy is armed."
                            : "HELD policies are PAUSED by the rollup-coverage gate so retention cannot drop history a "
                              + "rollup has never materialized. They arm themselves once the consumer catches up; the "
                              + "missing step is a backfill (--backfill-rollups). Arming one by hand drops the only copy "
                              + "of that history. over_horizon_ratio is how many times its configured depth the tier is "
                              + "actually holding — the cost of the hold.",
                    held = heldPolicies.Select(h => new
                    {
                        hypertable = h.HypertableName,
                        job_id = h.JobId,
                        drop_after = h.DropAfter,
                        chunk_count = h.ChunkCount,
                        actual_span_days = h.SpanSeconds is { } sec ? Math.Round(sec / 86400.0, 1) : (double?)null,
                        over_horizon_ratio = h.OverHorizonRatio is { } r ? Math.Round(r, 2) : (double?)null,
                    }),
                },
                objects = latest
                    .Where(r => r.ObjectKind != "store")
                    .OrderByDescending(r => r.TotalBytes ?? 0)
                    .Select(r => new
                    {
                        object_kind = r.ObjectKind,
                        object_name = r.ObjectName,
                        metric_time = r.MetricTime.ToString("o"),
                        total_bytes = r.TotalBytes,
                        compressed_before_bytes = r.CompressedBeforeBytes,
                        compressed_after_bytes = r.CompressedAfterBytes,
                        /* How many times smaller compression made what it compressed — before/after, the
                           way the operator already talks about it (6-36x measured on the motivating store). */
                        compression_ratio = r.CompressedBeforeBytes is > 0 && r.CompressedAfterBytes is > 0
                            ? Math.Round(r.CompressedBeforeBytes.Value / (double)r.CompressedAfterBytes.Value, 1)
                            : (double?)null,
                        chunk_count = r.ChunkCount,
                        row_count = r.RowCount,
                        /* #2136 background_job rows only (NULL elsewhere): last run duration, the job's own
                           cadence, and how much of that cadence the run consumed — the ceiling-proximity
                           number an onboarding wave moves first. */
                        last_run_duration_ms = r.LastRunDurationMs,
                        schedule_interval_ms = r.ScheduleIntervalMs,
                        duration_vs_cadence_percent = r.LastRunDurationMs is > 0 && r.ScheduleIntervalMs is > 0
                            ? Math.Round(100.0 * r.LastRunDurationMs.Value / r.ScheduleIntervalMs.Value, 1)
                            : (double?)null,
                        total_runs = r.TotalRuns,
                        total_failures = r.TotalFailures,
                    }),
                daily = daily
                    .Where(p => p.ObjectKind != "store")
                    .GroupBy(p => (p.ObjectKind, p.ObjectName))
                    .OrderBy(g => g.Key.ObjectKind, StringComparer.Ordinal)
                    .ThenBy(g => g.Key.ObjectName, StringComparer.Ordinal)
                    .Select(g => new
                    {
                        object_kind = g.Key.ObjectKind,
                        object_name = g.Key.ObjectName,
                        points = g.OrderBy(p => p.Day).Select(p => new
                        {
                            day = p.Day.ToString("yyyy-MM-dd"),
                            total_bytes = p.TotalBytes,
                            compressed_before_bytes = p.CompressedBeforeBytes,
                            compressed_after_bytes = p.CompressedAfterBytes,
                            chunk_count = p.ChunkCount,
                            row_count = p.RowCount,
                            last_run_duration_ms = p.LastRunDurationMs,
                            schedule_interval_ms = p.ScheduleIntervalMs,
                            total_runs = p.TotalRuns,
                            total_failures = p.TotalFailures,
                        }),
                    }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_store_metrics", ex);
        }
    }
}
