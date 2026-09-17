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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One editable collector row in the viewer's Collector Schedule editor — the effective (override-resolved)
/// schedule for a collector, mutated in place by the grid and the presets. Plain mutable class (matching
/// Lite's <c>CollectorSchedule</c>) so the WPF DataGrid two-way-binds its cells; the editor re-binds the grid
/// after a preset apply. <see cref="FrequencyMinutes"/> 0 = collect once on server load only.
/// </summary>
public sealed class CollectorScheduleEditItem
{
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public bool Enabled { get; set; } = true;
    public int FrequencyMinutes { get; set; }
    public int RetentionDays { get; set; }

    /// <summary>
    /// The V125 per-collector database scope (#3477) as the grid edits it: comma-separated names,
    /// the same entry format the Settings window's excluded-databases box uses, so the two
    /// instruments agree on how a name is typed as well as how it is matched. Blank = no scope =
    /// every database the server enumerates. <see cref="CollectorScheduleOverlay"/> owns the
    /// parse/format round trip; presets never touch it (they change frequencies only).
    /// </summary>
    public string DatabasesText { get; set; } = "";
}

/// <summary>
/// The viewer's port of Lite's collection-cadence presets (<c>ScheduleManager.s_presets</c> +
/// <c>DetectPreset</c>/<c>ApplyPreset</c>) — the SINGLE source of preset logic for the Darling schedule
/// editor. The three tables (Aggressive / Balanced / Low-Impact) are duplicated byte-for-byte from Lite's
/// (the projects don't share a schedule library); a Darling.Tests pin asserts they cannot drift from Lite's
/// intervals. A preset changes FREQUENCIES ONLY — enabled and retention are untouched — exactly as Lite's.
/// </summary>
public static class CollectorSchedulePresets
{
    public const string Custom = "Custom";

    /// <summary>The preset names offered in the editor combo (plus the <see cref="Custom"/> sentinel).</summary>
    public static readonly string[] Names = { "Aggressive", "Balanced", "Low-Impact" };

    /// <summary>Per-preset collector→frequency (minutes) tables — duplicated from Lite's ScheduleManager.s_presets.</summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, int>> Presets =
        new Dictionary<string, IReadOnlyDictionary<string, int>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Aggressive"] = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["wait_stats"] = 1, ["latch_stats"] = 1, ["spinlock_stats"] = 1,
                ["cpu_scheduler_stats"] = 1, ["plan_cache_stats"] = 2,
                ["query_stats"] = 1, ["procedure_stats"] = 1,
                ["query_store"] = 2, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
                ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 2,
                ["memory_pressure_events"] = 5,
                ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 2,
                ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
                ["dmv_blocking_snapshot"] = 1,
                ["blocked_process_report"] = 1, ["running_jobs"] = 2,
                ["session_summary_stats"] = 2, ["system_health_events"] = 2,
                ["default_trace_events"] = 2, ["job_history"] = 2, ["agent_status"] = 2,
                ["ag_replica_states"] = 1, ["ag_database_replica_states"] = 1,
                /* plan_correction tracks query_store across the presets: same per-database enumeration
                   shape, same default tier, so an operator backing one off wants the other to follow. */
                ["plan_correction"] = 2,
                ["database_states"] = 1,
            },
            ["Balanced"] = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["wait_stats"] = 1, ["latch_stats"] = 1, ["spinlock_stats"] = 1,
                ["cpu_scheduler_stats"] = 1, ["plan_cache_stats"] = 5,
                ["query_stats"] = 1, ["procedure_stats"] = 1,
                ["query_store"] = 5, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
                ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 5,
                ["memory_pressure_events"] = 5,
                /* deadlocks follows its new 5-minute default tier (#1963) - Balanced mirrors the defaults. */
                ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 5,
                ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
                ["dmv_blocking_snapshot"] = 1,
                ["blocked_process_report"] = 1, ["running_jobs"] = 5,
                ["session_summary_stats"] = 5, ["system_health_events"] = 5,
                ["default_trace_events"] = 5, ["job_history"] = 5, ["agent_status"] = 5,
                ["ag_replica_states"] = 1, ["ag_database_replica_states"] = 1,
                ["plan_correction"] = 5,
                ["database_states"] = 1,
            },
            ["Low-Impact"] = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["wait_stats"] = 5, ["latch_stats"] = 5, ["spinlock_stats"] = 5,
                ["cpu_scheduler_stats"] = 5, ["plan_cache_stats"] = 15,
                ["query_stats"] = 10, ["procedure_stats"] = 10,
                ["query_store"] = 30, ["query_snapshots"] = 5, ["cpu_utilization"] = 5,
                ["file_io_stats"] = 10, ["memory_stats"] = 10, ["memory_clerks"] = 30,
                ["memory_pressure_events"] = 15,
                ["tempdb_stats"] = 5, ["perfmon_stats"] = 5, ["deadlocks"] = 15,
                ["memory_grant_stats"] = 5, ["waiting_tasks"] = 5,
                ["dmv_blocking_snapshot"] = 5,
                ["blocked_process_report"] = 5, ["running_jobs"] = 30,
                ["session_summary_stats"] = 15, ["system_health_events"] = 15,
                ["default_trace_events"] = 15, ["job_history"] = 15, ["agent_status"] = 15,
                ["ag_replica_states"] = 5, ["ag_database_replica_states"] = 5,
                ["plan_correction"] = 30,
                ["database_states"] = 5,
            },
        };

    /// <summary>
    /// The full editable schedule seeded from the shared <see cref="CollectorScheduleDefaults"/> (every
    /// collector, at its code-default frequency/retention and code-default ENABLED state) — the baseline the editor overlays store
    /// overrides onto. Ordered by collector name for a stable grid.
    /// </summary>
    public static List<CollectorScheduleEditItem> BuildDefaultSchedule() =>
        CollectorScheduleDefaults.All
            .OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase)
            .Select(kv => new CollectorScheduleEditItem
            {
                Name = kv.Key,
                /* #2064: the collector's OWN shipped enabled state, not a blanket true. Hardcoding
                   true made the editor show a default-OFF collector (long_query_completions) as
                   CHECKED at default scope — it read as "already enabled" while the store held
                   nothing and the feature was off, which is exactly how #2061 was reported. */
                Enabled = kv.Value.DefaultEnabled,
                FrequencyMinutes = kv.Value.FrequencyMinutes,
                RetentionDays = kv.Value.RetentionDays,
            })
            .ToList();

    /// <summary>Detects which preset a schedule's frequencies match, or <see cref="Custom"/> (mirrors
    /// Lite's <c>ScheduleManager.DetectPreset</c> — frequency-only comparison).</summary>
    public static string DetectPreset(IReadOnlyList<CollectorScheduleEditItem> schedules)
    {
        ArgumentNullException.ThrowIfNull(schedules);

        foreach (var (presetName, intervals) in Presets)
        {
            var matches = true;
            foreach (var (collector, freq) in intervals)
            {
                var schedule = schedules.FirstOrDefault(s => s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
                if (schedule is not null && schedule.FrequencyMinutes != freq)
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                return presetName;
            }
        }

        return Custom;
    }

    /// <summary>Applies a named preset's frequencies to a schedule list IN PLACE (enabled + retention
    /// untouched); an unknown name is a no-op (mirrors Lite's <c>ScheduleManager.ApplyPreset</c>).</summary>
    public static void ApplyPreset(IReadOnlyList<CollectorScheduleEditItem> schedules, string presetName)
    {
        ArgumentNullException.ThrowIfNull(schedules);

        if (!Presets.TryGetValue(presetName, out var intervals))
        {
            return;
        }

        foreach (var (collector, freq) in intervals)
        {
            var schedule = schedules.FirstOrDefault(s => s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
            if (schedule is not null)
            {
                schedule.FrequencyMinutes = freq;
            }
        }
    }
}
