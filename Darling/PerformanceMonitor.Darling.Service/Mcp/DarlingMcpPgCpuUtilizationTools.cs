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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for instance-level PostgreSQL/Aurora CPU (#2719/#2629), paired with the
/// <c>pg_cpu_utilization</c> collector. Mirrors <c>DarlingMcpDataTools.GetCpuUtilization</c>'s shape (a
/// 1-minute-bucketed time series) rather than <c>get_pg_kernel_stats</c>'s per-query ranking — the two
/// collectors answer different questions, and this one is a gauge over time like SQL Server's own CPU read,
/// not a ranked list.
///
/// <para><b>It publishes the capacity a CPU percentage is a fraction OF</b> (#3281). Performance Insights'
/// figure is capacity-relative — to the capacity CURRENTLY ALLOCATED — and on Aurora Serverless v2 that
/// allocation is re-sized continuously, so it is not the "OS-level utilization against a fixed ceiling"
/// its shape suggests. A caller reading this series without the ACU columns beside it concludes a routine
/// scale-up was a saturation incident, which is what both in-product consumers of the metric did.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgCpuUtilizationTools
{
    [McpServerTool(Name = "get_pg_cpu_utilization"), Description("Gets instance-level CPU for a PostgreSQL/Aurora target from AWS Performance Insights, 1-minute buckets to as_of. Aurora only; RDS and self-hosted are not_collected. cpu_percent is percent of the capacity CURRENTLY ALLOCATED, not a fixed ceiling - on Serverless v2 that moves, so 100% is often a scale-up, not saturation. acu_utilization_percent is percent of the CONFIGURED ceiling and is the saturation figure; null ACU means no sample, never headroom. Host-memory bytes (since V136) are null when unmeasured, never 0; memory_samples_in_bucket is the count. <<GUIDE>> Gets instance-level CPU utilization over time for a PostgreSQL/Aurora target, from AWS Performance Insights, downsampled to 1-minute averages. Aurora only - a plain RDS or self-hosted target has no route here and this is not_collected for it; Performance Insights could reach plain RDS too, but no monitored target is that shape yet. cpu_percent is os.cpuUtilization.total.avg, which is percent of the capacity CURRENTLY ALLOCATED: on Aurora Serverless v2 that allocation moves, so 100% is routinely a scale-up rather than saturation. acu_utilization_percent is percent of the CONFIGURED ACU ceiling and is the saturation figure - band and alert on that one. Both are reported per bucket, with the allocated and configured ACU beside them; a null ACU figure means no capacity sample, not headroom. Since V136 the same row carries the HOST'S MEMORY from Performance Insights' os.memory.* counters, and each bucket reports it in bytes: memory_total_bytes, memory_free_bytes, memory_cached_bytes, memory_buffers_bytes, memory_active_bytes and configured_memory_bytes (the instance class's figure) - the figures the PG_HOST_MEMORY_PRESSURE and CONFIG_PG_MEMORY_OVERCOMMIT facts are measured against (get_analysis_facts source=pg_memory). Every memory figure is null on a pre-V136 row or an endpoint without os.memory.*, and null means not measured, never zero memory; memory_samples_in_bucket says how many samples carried it.")]
    public static async Task<string> GetPgCpuUtilization(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgCpuUtilizationReader.GetHistoryAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd);

            if (rows.Count == 0)
            {
                /* Not-collected first: a self-hosted target has no route at all (see
                   PgCpuUtilizationCollector's doc comment), so that is the likelier and more actionable
                   answer than a bare "no data in window". */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_cpu_utilization")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No CPU utilization data for {resolved.ServerName} in the last {hours_back} hour(s).");
            }

            var bucketed = rows
                .GroupBy(r => new DateTime(r.SampleTimeUtc.Year, r.SampleTimeUtc.Month, r.SampleTimeUtc.Day,
                    r.SampleTimeUtc.Hour, r.SampleTimeUtc.Minute, 0, r.SampleTimeUtc.Kind))
                .OrderBy(g => g.Key)
                .Select(g => new
                {
                    sample_time = g.Key.ToString("o"),
                    cpu_percent = Math.Round(g.Average(r => r.CpuPercent), 1),
                    /* Averaged over the samples that HAVE a capacity reading, and null when none of them
                       does (#3281). Coalescing the missing ones to 0 first would drag a real figure down
                       toward "plenty of headroom" in exactly the window where the capacity was not
                       measured — the one direction that must not be invented.

                       Averaged rather than taken last, on the same terms as cpu_percent above, because a
                       bucket is one minute and Performance Insights' period is 60 seconds: the normal
                       bucket holds a single sample, so the mean IS that sample, and a bucket that holds
                       more is a re-read window where a mean is the honest summary. That applies to the
                       ceiling too — it is a setting someone can change mid-window, and a mean says so by
                       landing between the two values instead of silently picking one. */
                    acu_utilization_percent = Rounded(g.Select(r => r.AcuUtilizationPercent)),
                    serverless_capacity_acu = Rounded(g.Select(r => r.ServerlessCapacityAcu)),
                    max_configured_acu = Rounded(g.Select(r => r.MaxConfiguredAcu)),
                    samples_in_bucket = g.Count(),
                    /* acu_utilization_percent's own denominator, reported because that average is over a
                       SUBSET of samples_in_bucket — without it a null or a low figure cannot be told from
                       a thin one. It counts that column specifically rather than all three: they arrive on
                       one Performance Insights call at one period, so they are present together in
                       practice, and the figure a reader bands on is the one whose coverage matters. */
                    capacity_samples_in_bucket = g.Count(r => r.AcuUtilizationPercent.HasValue),
                    /* The V136 host-memory columns (#3809; the third between-waves batch of #3691), in bytes,
                       averaged over the samples that carried them on the same terms as the capacity trio: a
                       bucket is normally one sample, so the mean IS the sample, and a null must stay null - a
                       0 here reads as a host with no memory. Integer bytes, not a double mean, because the
                       payload is read as sizes and 8589934592.3 bytes is not a size. */
                    memory_total_bytes = MeanBytes(g.Select(r => r.Memory?.TotalBytes)),
                    memory_free_bytes = MeanBytes(g.Select(r => r.Memory?.FreeBytes)),
                    memory_cached_bytes = MeanBytes(g.Select(r => r.Memory?.CachedBytes)),
                    memory_buffers_bytes = MeanBytes(g.Select(r => r.Memory?.BuffersBytes)),
                    memory_active_bytes = MeanBytes(g.Select(r => r.Memory?.ActiveBytes)),
                    configured_memory_bytes = MeanBytes(g.Select(r => r.Memory?.ConfiguredBytes)),
                    /* memory_total_bytes' own denominator, for the same reason capacity_samples_in_bucket
                       exists: a null or a thin memory figure cannot otherwise be told from a bucket with no
                       memory samples at all. Counts the total specifically - the six arrive together on one
                       Performance Insights call, and the total is the one every share is measured against. */
                    memory_samples_in_bucket = g.Count(r => r.Memory?.TotalBytes is not null),
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                note = "cpu_percent is os.cpuUtilization.total.avg from AWS Performance Insights - percent "
                     + "of the capacity CURRENTLY ALLOCATED, not of a fixed ceiling. On Aurora Serverless "
                     + "v2 the allocation is re-sized continuously, so a 1-vCPU instance reads 100% "
                     + "whenever one core is busy for a minute, which is the routine trigger for scaling "
                     + "up. acu_utilization_percent (os.general.acuUtilization.avg) is percent of the "
                     + "CONFIGURED ACU ceiling and is the saturation figure - 100% of it means the ceiling "
                     + "really is reached. A null acu_utilization_percent means no capacity sample in that "
                     + "bucket, never headroom.",
                samples = bucketed,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_cpu_utilization", ex);
        }
    }

    /// <summary>The mean of the values that were actually sampled, to one decimal, or null when none was
    /// (#3281). Separate from the <c>cpu_percent</c> average above because that column is never null in
    /// this read — the store query already filters it — while all three capacity columns can be, and
    /// coalescing a missing one to 0 would report measured headroom that nobody measured.</summary>
    private static double? Rounded(System.Collections.Generic.IEnumerable<double?> values)
    {
        var present = values.Where(v => v.HasValue).Select(v => v!.Value).ToList();
        return present.Count == 0 ? null : Math.Round(present.Average(), 1);
    }

    /// <summary>The byte-column twin of <see cref="Rounded"/>: the mean of the memory values that were sampled,
    /// rounded to whole bytes, or null when none was. Null stays null for the same reason - zero bytes of
    /// host memory is a measurement nobody made.</summary>
    private static long? MeanBytes(System.Collections.Generic.IEnumerable<long?> values)
    {
        var present = values.Where(v => v.HasValue).Select(v => v!.Value).ToList();
        return present.Count == 0 ? null : (long)Math.Round(present.Average());
    }
}
