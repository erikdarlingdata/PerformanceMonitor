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
    /// <para>Growth is nullable because it is not always measurable — the collector's -1 sentinel on
    /// either endpoint, or a one-sample window. Unknown never escalates to the growing verdict, and never
    /// reads as the flat one either: "flat" is a measured claim, and it is the claim that separates a
    /// consumer between polls from a volume filling (#3535).</para>
    /// </summary>
    internal static string Classify(string? walStatus, bool isActive, bool? retainedWalGrowing) =>
        walStatus switch
        {
            /* The slot is already unusable — its consumer cannot resume and needs recreating. */
            "lost" => "critical_slot_lost",
            /* Required WAL has been removed; the consumer is about to find that out. */
            "unreserved" => "critical_wal_already_removed",
            /* WAL is being retained BECAUSE of this slot. Inactive and still growing is the disk bomb. */
            "extended" when !isActive && retainedWalGrowing == true => "critical_orphan_filling_disk",
            "extended" when !isActive && retainedWalGrowing is null => "warning_retaining_wal_growth_unknown",
            "extended" => "warning_retaining_wal",
            _ when !isActive && retainedWalGrowing == true => "warning_inactive_and_growing",
            _ when !isActive && retainedWalGrowing is null => "info_inactive_growth_unknown",
            _ when !isActive => "info_inactive",
            _ => "ok",
        };

    /// <summary>
    /// Rank by how bad the label is, so the headline slot is the worst-CLASSIFIED one rather than the
    /// fattest one (the get_pg_wraparound_risk pattern). Picking worst_slot by retained size contradicted
    /// this type's own design note: an active 45 GB keeping-pace slot ("ok") outranked an inactive 2→8 GB
    /// grower ("critical_orphan_filling_disk") — the one slot the caller needed to see first (#3535).
    /// Size still breaks ties within a label.
    /// </summary>
    internal static int Rank(string severity) => severity switch
    {
        "critical_slot_lost" => 8,
        "critical_wal_already_removed" => 7,
        "critical_orphan_filling_disk" => 6,
        "warning_inactive_and_growing" => 5,
        /* Above the measured-flat warning: this is the orphan shape with its discriminator unmeasured,
           which deserves the look before a slot known to be holding steady. */
        "warning_retaining_wal_growth_unknown" => 4,
        "warning_retaining_wal" => 3,
        "info_inactive_growth_unknown" => 2,
        "info_inactive" => 1,
        _ => 0,
    };

    [McpServerTool(Name = "get_pg_replication_slots"), Description("Replication slot health: whether retained WAL is unbounded and GROWING (fills disk, pins vacuum). Window ends at as_of. not_collected: engine is not PostgreSQL. no_slots: none sampled in hours_back, none exist or none collected; per-instance, a replica's no_slots doesn't clear the writer. slots_present: slots in window; read worst_severity. retained_wal_bytes/_gb, safe_wal_size_bytes, xmin ages: null on the collector's -1 sentinel; no ceiling by default isn't a gap. retained_wal_growth_bytes is 0 when flat (measured), null when unmeasurable. worst_slot ranks by severity, not size.<<GUIDE>>Gets PostgreSQL replication slot health, including whether any slot is retaining WAL without bound. An abandoned slot is one of the few PostgreSQL conditions that can take a server down by itself, and it does so two independent ways: it retains every WAL segment its consumer has not confirmed - unbounded by default, so it will fill the volume and stop the server - and it simultaneously pins the vacuum horizon so nothing gets reclaimed cluster-wide. Reports whether retained WAL is still GROWING across the window, which is the difference between a consumer that is merely behind and a volume filling in front of you. Common orphan sources are a removed CDC task, a finished blue/green deployment, a decommissioned Debezium consumer, or a failed major-version upgrade. Works on any PostgreSQL target.")]
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
                /* Growth exists only when BOTH endpoints were measured across a window that spans time.
                   The -1 sentinel used to become a measured 0 here — "no growth" — feeding growing=false
                   into Classify, so an unmeasurable slot read as stable (#3535); and a one-sample window
                   is the same fabrication one step milder, "flat" from a single point. Null, both. */
                long? growthBytes =
                    r.MeasuredAt != r.FirstSeenAt && r.RetainedWalBytes >= 0 && r.FirstRetainedWalBytes >= 0
                        ? r.RetainedWalBytes - r.FirstRetainedWalBytes
                        : null;
                var hours = Math.Max((r.MeasuredAt - r.FirstSeenAt).TotalHours, 0);
                bool? growing = growthBytes is null ? null : growthBytes > 0;

                return new
                {
                    slot_name = r.SlotName,
                    severity = Classify(r.WalStatus, r.IsActive, growing),
                    slot_type = r.SlotType,
                    plugin = r.Plugin,
                    database_name = r.DatabaseName,
                    is_active = r.IsActive,
                    wal_status = r.WalStatus,
                    /* -1 is the collector's sentinel, not a size — null it here as the _gb twin below
                       always has, rather than serializing a figure no slot can hold. */
                    retained_wal_bytes = r.RetainedWalBytes >= 0 ? r.RetainedWalBytes : (long?)null,
                    retained_wal_gb = r.RetainedWalBytes >= 0
                        ? Math.Round(r.RetainedWalBytes / 1024.0 / 1024.0 / 1024.0, 2)
                        : (double?)null,
                    /* Growth is the actionable half. Rate is only reported when the window actually spans
                       time, so a single-sample window cannot produce a fabricated per-hour figure. */
                    retained_wal_growth_bytes = growthBytes,
                    retained_wal_growth_gb_per_hour = growthBytes is not null && hours >= 0.05
                        ? Math.Round(growthBytes.Value / 1024.0 / 1024.0 / 1024.0 / hours, 3)
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
            /* Worst-CLASSIFIED first; size only breaks ties. Ordering by size alone put the fattest slot
               in worst_slot regardless of verdict (#3535). Unknown sizes sort after measured ones within
               a label. */
            .OrderByDescending(s => Rank(s.severity))
            .ThenByDescending(s => s.retained_wal_bytes ?? -1)
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
                    slots.Where(s => s.retained_wal_bytes > 0).Sum(s => s.retained_wal_bytes ?? 0) / 1024.0 / 1024.0 / 1024.0, 2),
                slots,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_replication_slots", ex);
        }
    }
}
