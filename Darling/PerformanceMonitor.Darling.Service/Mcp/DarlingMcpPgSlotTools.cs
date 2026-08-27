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
/// The MCP surface for replication slot health, paired with the <c>pg_replication_slots</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgSlotTools
{
    /// <summary>
    /// Severity from slot state, not from the retained figure alone. The size of the hole matters far
    /// less than whether it is still being dug: a slot holding 45 GB steadily is a consumer keeping pace,
    /// while one that grew from 2 GB to 45 GB in an hour is a volume filling in front of you.
    /// </summary>
    internal static string Classify(string? walStatus, bool isActive, bool retainedWalGrowing) =>
        walStatus switch
        {
            /* The slot is already unusable — its consumer cannot resume and needs recreating. */
            "lost" => "critical_slot_lost",
            /* Required WAL has been removed; the consumer is about to find that out. */
            "unreserved" => "critical_wal_already_removed",
            /* WAL is being retained BECAUSE of this slot. Inactive and still growing is the disk bomb. */
            "extended" when !isActive && retainedWalGrowing => "critical_orphan_filling_disk",
            "extended" => "warning_retaining_wal",
            _ when !isActive && retainedWalGrowing => "warning_inactive_and_growing",
            _ when !isActive => "info_inactive",
            _ => "ok",
        };

    [McpServerTool(Name = "get_pg_replication_slots"), Description("Gets PostgreSQL replication slot health, including whether any slot is retaining WAL without bound. An abandoned slot is one of the few PostgreSQL conditions that can take a server down by itself, and it does so two independent ways: it retains every WAL segment its consumer has not confirmed - unbounded by default, so it will fill the volume and stop the server - and it simultaneously pins the vacuum horizon so nothing gets reclaimed cluster-wide. Reports whether retained WAL is still GROWING across the window, which is the difference between a consumer that is merely behind and a volume filling in front of you. Common orphan sources are a removed CDC task, a finished blue/green deployment, a decommissioned Debezium consumer, or a failed major-version upgrade. Works on any PostgreSQL target.")]
    public static async Task<string> GetPgReplicationSlots(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze, used for the WAL growth comparison. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingPgSlotReader.GetPgSlotsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            /* No slots is the common, healthy case on most servers — say so rather than returning an
               "unavailable" envelope that reads like a collection problem.
               The scope caveat is not hedging. Replication slots live on the WRITER, so an empty result
               read from a replica says nothing about its cluster, and a caller that treats "no slots" as
               a cluster-wide all-clear would be drawing the one conclusion this result cannot support. */
            if (rows.Count == 0)
            {
                /* The engine question comes first (#2532): "this instance has no replication slots" is a
                   real finding on a PostgreSQL target and a false one on a SQL Server target, where the
                   collector has never run and never will. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_replication_slots");
                if (gated != null)
                {
                    return gated;
                }

                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = "no_slots",
                    finding = "This instance has no replication slots in the window, so it is not itself "
                            + "retaining WAL or pinning vacuum through one.",
                    scope = "Per-instance. Slots live on the writer, so if this target is a replica, check "
                          + "the cluster's writer before concluding the cluster has no abandoned slot.",
                }, McpHelpers.JsonOptions);
            }

            var slots = rows.Select(r =>
            {
                var growthBytes = r.RetainedWalBytes >= 0 && r.FirstRetainedWalBytes >= 0
                    ? r.RetainedWalBytes - r.FirstRetainedWalBytes
                    : 0;
                var hours = Math.Max((r.MeasuredAt - r.FirstSeenAt).TotalHours, 0);
                var growing = growthBytes > 0;

                return new
                {
                    slot_name = r.SlotName,
                    severity = Classify(r.WalStatus, r.IsActive, growing),
                    slot_type = r.SlotType,
                    plugin = r.Plugin,
                    database_name = r.DatabaseName,
                    is_active = r.IsActive,
                    wal_status = r.WalStatus,
                    retained_wal_bytes = r.RetainedWalBytes,
                    retained_wal_gb = r.RetainedWalBytes >= 0
                        ? Math.Round(r.RetainedWalBytes / 1024.0 / 1024.0 / 1024.0, 2)
                        : (double?)null,
                    /* Growth is the actionable half. Rate is only reported when the window actually spans
                       time, so a single-sample window cannot produce a fabricated per-hour figure. */
                    retained_wal_growth_bytes = growthBytes,
                    retained_wal_growth_gb_per_hour = hours >= 0.05
                        ? Math.Round(growthBytes / 1024.0 / 1024.0 / 1024.0 / hours, 3)
                        : (double?)null,
                    /* -1 is the collector's not-applicable sentinel: safe_wal_size is NULL whenever
                       max_slot_wal_keep_size is -1, which is the default, so on a stock server there is
                       no configured ceiling at all. */
                    has_configured_wal_ceiling = r.SafeWalSizeBytes >= 0,
                    safe_wal_size_bytes = r.SafeWalSizeBytes >= 0 ? r.SafeWalSizeBytes : (long?)null,
                    /* The second failure mode, from the same slot. */
                    xmin_age = r.XminAge >= 0 ? r.XminAge : (long?)null,
                    catalog_xmin_age = r.CatalogXminAge >= 0 ? r.CatalogXminAge : (long?)null,
                    inactive_since = r.InactiveSince,
                    invalidation_reason = r.InvalidationReason,
                    conflicting = r.Conflicting,
                };
            })
            .OrderByDescending(s => s.retained_wal_bytes)
            .ToList();

            var worst = slots[0];

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "slots_present",
                slot_count = slots.Count,
                inactive_count = slots.Count(s => !s.is_active),
                worst_slot = worst.slot_name,
                worst_severity = worst.severity,
                total_retained_wal_gb = Math.Round(
                    slots.Where(s => s.retained_wal_bytes > 0).Sum(s => s.retained_wal_bytes) / 1024.0 / 1024.0 / 1024.0, 2),
                slots,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL replication slots failed: {ex.Message}");
        }
    }
}
