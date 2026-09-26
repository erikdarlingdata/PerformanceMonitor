/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for instance-level PostgreSQL/Aurora CPU (#2719/#2629), paired with the
/// <c>pg_cpu_utilization</c> collector. Mirrors <c>DarlingMcpDataTools.GetCpuUtilization</c>'s shape (a
/// bucketed time series, #4193) rather than <c>get_pg_kernel_stats</c>'s per-query ranking — the two
/// collectors answer different questions, and this one is a gauge over time like SQL Server's own CPU read,
/// not a ranked list.
///
/// <para><b>It publishes the capacity a CPU percentage is a fraction OF</b> (#3281). Performance Insights'
/// figure is capacity-relative — to the capacity CURRENTLY ALLOCATED — and on Aurora Serverless v2 that
/// allocation is re-sized continuously, so it is not the "OS-level utilization against a fixed ceiling"
/// its shape suggests. A caller reading this series without the ACU columns beside it concludes a routine
/// scale-up was a saturation incident, which is what both in-product consumers of the metric did.</para>
///
/// <para><b>#4193: on the TrendBuckets contract</b> (#3897) the rest of the trend family already carries.
/// Until now this read returned one row per raw 1-minute Performance Insights sample for the whole window —
/// 618 KB for a day, 4.1 MB for a week, once the V136 host-memory columns doubled its row width. Now the
/// caller's window is bucketed to a point budget the same way <c>get_cpu_utilization</c> is: CPU and ACU are
/// averaged with each bucket's peak kept beside it (so a saturation minute survives a wide bucket), and the
/// memory pair that says whether the host was under PRESSURE (min free bytes, max active bytes) is the
/// bucket's worst sample rather than an average, so a brief spike is not smoothed away.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgCpuUtilizationTools
{
    [McpServerTool(Name = "get_pg_cpu_utilization"), Description("Gets instance-level CPU for a PostgreSQL/Aurora target from AWS Performance Insights, in time buckets (bucket_minutes) to as_of. Aurora only; RDS and self-hosted are not_collected. cpu_percent is percent of capacity CURRENTLY ALLOCATED, not a fixed ceiling - on Serverless v2 that moves, so 100% is often a scale-up, not saturation. acu_utilization_percent is percent of the CONFIGURED ceiling, the saturation figure; null ACU means no sample, never headroom. Host-memory bytes (since V136) are null when unmeasured; memory_samples_in_bucket is the count. <<GUIDE>> Gets instance-level CPU utilization over time for a PostgreSQL/Aurora target, from AWS Performance Insights, bucketed to a point budget the same way get_cpu_utilization is (#4193, the #3897 TrendBuckets contract) rather than one row per raw 1-minute sample. Aurora only - a plain RDS or self-hosted target has no route here and this is not_collected for it; Performance Insights could reach plain RDS too, but no monitored target is that shape yet. cpu_percent is os.cpuUtilization.total.avg, which is percent of the capacity CURRENTLY ALLOCATED: on Aurora Serverless v2 that allocation moves, so 100% is routinely a scale-up rather than saturation. acu_utilization_percent (os.general.acuUtilization.avg) is percent of the CONFIGURED ACU ceiling and is the saturation figure - band and alert on that one. Both are reported per bucket as the AVERAGE of the bucket's samples, with peak_cpu_percent/peak_acu_utilization_percent holding the single busiest sample, so a saturation minute is never averaged away by a wide bucket; a null ACU figure means no capacity sample in that bucket, never headroom. Since V136 the same row carries the HOST'S MEMORY from Performance Insights' os.memory.* counters, in bytes: memory_total_bytes, memory_cached_bytes, memory_buffers_bytes and configured_memory_bytes are the bucket's average; memory_free_bytes and memory_active_bytes are its WORST sample (minimum free, maximum active) rather than an average, so a brief pressure spike survives a wide bucket - the figures the PG_HOST_MEMORY_PRESSURE and CONFIG_PG_MEMORY_OVERCOMMIT facts are measured against (get_analysis_facts source=pg_memory). Every memory figure is null on a pre-V136 row or an endpoint without os.memory.*, and null means not measured, never zero memory; memory_samples_in_bucket says how many samples carried it.")]
    public static Task<string> GetPgCpuUtilization(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetPgCpuUtilization(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.PgCpuMaxPoints), cancellationToken);

    /// <summary>
    /// get_pg_cpu_utilization under an explicit <paramref name="budget"/> (#4193): the MCP tool passes its own,
    /// the web viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>.
    /// </summary>
    internal static async Task<string> GetPgCpuUtilization(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, int? bucket_minutes, TrendBudget budget,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
        if (bucketError != null) return bucketError;

        try
        {
            var points = await DarlingPgCpuUtilizationReader.GetBucketedHistoryAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, bucketMinutes, cancellationToken);

            if (points.Count == 0)
            {
                /* Not-collected first: a self-hosted target has no route at all (see
                   PgCpuUtilizationCollector's doc comment), so that is the likelier and more actionable
                   answer than a bare "no data in window". */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_cpu_utilization", cancellationToken)
                    ?? McpHelpers.Status(
                        "empty",
                        $"No CPU utilization data for {resolved.ServerName} in the last {hours_back} hour(s).");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                bucket = TrendBuckets.Word(bucketMinutes),
                bucket_minutes = bucketMinutes,
                aggregate_note = TrendBuckets.LevelNote(bucketMinutes, bucket_minutes is not null, budget.AutoPoints, firstAtWindowStart: false),
                note = "cpu_percent is os.cpuUtilization.total.avg from AWS Performance Insights - percent "
                     + "of the capacity CURRENTLY ALLOCATED, not of a fixed ceiling. On Aurora Serverless "
                     + "v2 the allocation is re-sized continuously, so a 1-vCPU instance reads 100% "
                     + "whenever one core is busy for a minute, which is the routine trigger for scaling "
                     + "up. acu_utilization_percent (os.general.acuUtilization.avg) is percent of the "
                     + "CONFIGURED ACU ceiling and is the saturation figure - 100% of it means the ceiling "
                     + "really is reached. A null acu_utilization_percent means no capacity sample in that "
                     + "bucket, never headroom. memory_free_bytes/memory_active_bytes are each bucket's "
                     + "WORST sample (minimum free, maximum active), not an average, so a pressure spike "
                     + "is never smoothed away by a wide bucket; the other memory columns are averaged.",
                samples = points.Select(p => new
                {
                    sample_time = p.BucketStartUtc.ToString("o"),
                    cpu_percent = Math.Round(p.CpuPercent, 1),
                    peak_cpu_percent = p.PeakCpuPercent is { } peakCpu ? Math.Round(peakCpu, 1) : (double?)null,
                    acu_utilization_percent = p.AcuUtilizationPercent is { } acu ? Math.Round(acu, 1) : (double?)null,
                    peak_acu_utilization_percent = p.PeakAcuUtilizationPercent is { } peakAcu ? Math.Round(peakAcu, 1) : (double?)null,
                    serverless_capacity_acu = p.ServerlessCapacityAcu is { } cap ? Math.Round(cap, 1) : (double?)null,
                    max_configured_acu = p.MaxConfiguredAcu is { } maxAcu ? Math.Round(maxAcu, 1) : (double?)null,
                    samples_in_bucket = p.Samples,
                    capacity_samples_in_bucket = p.CapacitySamples,
                    memory_total_bytes = p.Memory?.TotalBytes,
                    memory_free_bytes = p.Memory?.FreeBytes,
                    memory_cached_bytes = p.Memory?.CachedBytes,
                    memory_buffers_bytes = p.Memory?.BuffersBytes,
                    memory_active_bytes = p.Memory?.ActiveBytes,
                    configured_memory_bytes = p.Memory?.ConfiguredBytes,
                    memory_samples_in_bucket = p.MemorySamples,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_cpu_utilization", ex);
        }
    }
}
