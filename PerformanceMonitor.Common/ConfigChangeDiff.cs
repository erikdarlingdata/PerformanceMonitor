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

namespace PerformanceMonitor.Common;

/// <summary>
/// The ONE shared source for the Configuration Changes histories — server-config / database-config /
/// trace-flag DRIFT over time, COMPUTED by diffing the store's APPEND-ONLY config SNAPSHOT tables in pure C#
/// (there is no persisted change table — the Dashboard's <c>report.*_changes</c> views LAG over a config
/// history the headless editions do not materialize). Authored once here in <c>PerformanceMonitor.Common</c>
/// (pure, dependency-free, unit-testable) precisely because three consumers need it and none may reference
/// another: the Darling <b>viewer</b>'s Configuration Changes tab, the headless <b>MCP host</b>'s
/// <c>get_*_config_changes</c> tools, and <b>Lite</b>'s Configuration Changes tab. This was previously a
/// hand-mirrored twin (the viewer's <c>Diff*</c> and the service's <c>Compute*</c>); a Lite port would have
/// made it a third copy, so — exactly like <see cref="SystemHealthSignificance"/> — the algorithm is hoisted
/// and every side delegates. The diff runs in C# (not SQL) so it is unit-testable without a live store and so
/// trace-flag enable/disable (a flag row appearing / disappearing between snapshots) is handled by set-diff.
///
/// <para>
/// WINDOWING — every diff takes BOTH window edges (<paramref name="startUtc"/>, <paramref name="endUtc"/>) and
/// applies them to the computed <c>change_time</c>, NOT to the snapshot read. Callers deliberately keep the
/// snapshot immediately BEFORE the window as the diff baseline (the reads upper-bound at the window end but
/// never lower-bound), so a change landing on the first in-window snapshot is still detected — mirroring the
/// Dashboard, whose <c>report.*_changes</c> views LAG over the full history and then filter
/// <c>change_time &gt;= @from AND change_time &lt;= @to</c>. The headless MCP tools, which read the full
/// unbounded history and only lower-bound, pass <see cref="DateTime.MaxValue"/> as the upper edge (a no-op)
/// so this both-edges algorithm reproduces their prior lower-bound-only behavior exactly.
/// </para>
///
/// <para>
/// SHAPE CAVEATS (surfaced by the consumers, never silently dropped): the config collectors run ON CONNECT
/// only (<c>FrequencyMinutes = 0</c>), so change granularity equals the connect/restart cadence and a fresh /
/// stably-connected server may hold a single snapshot — then there is no change yet (the diff needs &gt;= 2
/// snapshots). The change records carry ONLY collected values plus PURE-DERIVED columns (requires_restart,
/// change_description, scope, status text) that the Dashboard grids show; the Dashboard enrichment the headless
/// editions do not collect is OMITTED (not fabricated): server-config <c>description</c> and database-config
/// <c>setting_type</c> (the store is WIDE — <c>setting_name</c> is the literal column name). <c>requires_restart</c>
/// is NOT one of the gaps: it is derivable from collected columns (<c>is_dynamic = false AND
/// value_configured != value_in_use</c>, the Dashboard view's own definition), so it is kept. Timezone /
/// display formatting of <c>change_time</c> stays per-app (each app renders through its own time helper).
/// </para>
/// </summary>
public static class ConfigChangeDiff
{
    /* ─────────────────────────── snapshot input records (the diff's raw input) ─────────────────────────── */

    /// <summary>One sys.configurations snapshot capture for a single configuration_name.</summary>
    public sealed record ServerConfigSnapshot(
        DateTime CaptureTime, string ConfigurationName, long? ValueConfigured, long? ValueInUse, bool? IsDynamic, bool? IsAdvanced);

    /// <summary>One sys.databases snapshot capture for a single database — the WIDE row's 27 settings CAST to
    /// text (in <see cref="DatabaseConfigChangeSettingNames"/> order) so the diff walks them positionally.</summary>
    public sealed record DatabaseConfigSnapshot(
        DateTime CaptureTime, string DatabaseName, IReadOnlyList<string?> Values);

    /// <summary>One trace-flag snapshot row (a row exists only while a flag is enabled at that capture).</summary>
    public sealed record TraceFlagSnapshot(
        DateTime CaptureTime, int TraceFlag, bool? Status, bool? IsGlobal, bool? IsSession);

    /* ─────────────────────────── change output records (raw data + PURE-DERIVED display props) ─────────────────────────── */

    /// <summary>One server-configuration change. <see cref="RequiresRestart"/> and <see cref="ChangeDescription"/>
    /// are DERIVED from the collected values, matching the Dashboard's <c>report.server_configuration_changes</c>
    /// view definitions. Timezone rendering of <see cref="ChangeTime"/> is per-app (not here).</summary>
    public sealed record ServerConfigChange(
        DateTime ChangeTime, string ConfigurationName, long? OldValueConfigured, long? NewValueConfigured,
        long? OldValueInUse, long? NewValueInUse, bool? IsDynamic, bool? IsAdvanced)
    {
        public string DynamicDisplay => IsDynamic == true ? "Yes" : "No";
        public string AdvancedDisplay => IsAdvanced == true ? "Yes" : "No";

        /* requires_restart = a non-dynamic setting whose configured value differs from the in-use value (the
           Dashboard view's exact definition: is_dynamic = 0 AND value_configured != value_in_use). Derivable
           from collected columns, so it is kept — NOT a collection gap. */
        public bool RequiresRestart => IsDynamic == false && NewValueConfigured != NewValueInUse;
        public string RequiresRestartDisplay => RequiresRestart ? "Yes" : "No";

        /* Derived narrative matching report.server_configuration_changes.change_description wording. */
        public string ChangeDescription =>
            OldValueConfigured != NewValueConfigured
                ? $"Configured value changed from {OldValueConfigured} to {NewValueConfigured}"
                : OldValueInUse != NewValueInUse
                    ? $"In-use value changed from {OldValueInUse} to {NewValueInUse}"
                    : "Value unchanged";
    }

    /// <summary>One database-configuration change. The Dashboard's <c>setting_type</c> column is OMITTED (the
    /// WIDE store has no setting_type — <see cref="SettingName"/> is the literal column name);
    /// <see cref="ChangeDescription"/> is DERIVED, matching the Dashboard view.</summary>
    public sealed record DatabaseConfigChange(
        DateTime ChangeTime, string DatabaseName, string SettingName, string? OldValue, string? NewValue)
    {
        /* Derived narrative matching report.database_configuration_changes.change_description wording. */
        public string ChangeDescription =>
            OldValue is null && NewValue is not null ? $"Set to: {NewValue}"
            : OldValue is not null && NewValue is null ? $"Cleared (was: {OldValue})"
            : $"Changed from {OldValue} to {NewValue}";
    }

    /// <summary>One trace-flag change. <see cref="Scope"/> and <see cref="ChangeDescription"/> are DERIVED,
    /// matching the Dashboard's <c>report.trace_flag_changes</c> view. <see cref="ChangeType"/> is the set-diff
    /// outcome (enabled / disabled / modified).</summary>
    public sealed record TraceFlagChange(
        DateTime ChangeTime, int TraceFlag, bool? PreviousStatus, bool? NewStatus, bool? IsGlobal, bool? IsSession, string ChangeType)
    {
        public string PreviousStatusDisplay => StatusText(PreviousStatus);
        public string NewStatusDisplay => StatusText(NewStatus);
        public string GlobalDisplay => IsGlobal == true ? "Yes" : "No";
        public string SessionDisplay => IsSession == true ? "Yes" : "No";

        /* Matches report.trace_flag_changes.scope (GLOBAL / SESSION / UNKNOWN). */
        public string Scope => IsGlobal == true ? "GLOBAL" : IsSession == true ? "SESSION" : "UNKNOWN";

        /* Derived narrative matching report.trace_flag_changes.change_description wording. */
        public string ChangeDescription => ChangeType switch
        {
            "enabled" => $"Trace flag {TraceFlag} ENABLED",
            "disabled" => $"Trace flag {TraceFlag} DISABLED",
            "modified" => $"Trace flag {TraceFlag} scope changed",
            _ => "Status unchanged",
        };

        private static string StatusText(bool? status) => status is null ? "" : status.Value ? "ON" : "OFF";
    }

    /// <summary>The 27 database-config setting names, in the collector's exact column order — the WIDE store
    /// row is UNPIVOTed to a change row per changed setting, with this literal column name as the setting_name.
    /// Load-bearing: every consumer's snapshot read SELECTs the 27 setting columns in THIS order and hands them
    /// as <see cref="DatabaseConfigSnapshot.Values"/>, which the diff walks positionally.</summary>
    public static readonly IReadOnlyList<string> DatabaseConfigChangeSettingNames = new[]
    {
        "state_desc", "compatibility_level", "collation_name", "recovery_model", "is_read_only",
        "is_auto_close_on", "is_auto_shrink_on", "is_auto_create_stats_on", "is_auto_update_stats_on",
        "is_auto_update_stats_async_on", "is_read_committed_snapshot_on", "snapshot_isolation_state",
        "is_parameterization_forced", "is_query_store_on", "is_encrypted", "is_trustworthy_on",
        "is_db_chaining_on", "is_broker_enabled", "is_cdc_enabled", "is_mixed_page_allocation_on",
        "log_reuse_wait_desc", "page_verify_option", "target_recovery_time_seconds", "delayed_durability",
        "is_accelerated_database_recovery_on", "is_memory_optimized_enabled", "is_optimized_locking_on",
    };

    /* ─────────────────────────── C# snapshot diffs (pure, unit-testable, no live store) ─────────────────────────── */

    /// <summary>
    /// Diffs the sys.configurations snapshots into a change per (configuration_name) whose configured or in-use
    /// value moved between two consecutive captures (every config row is present in every snapshot, so a
    /// per-name walk is the whole story). Emits changes whose newer capture falls in [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], newest first.
    /// </summary>
    public static List<ServerConfigChange> DiffServerConfigChanges(
        IReadOnlyList<ServerConfigSnapshot> snapshots, DateTime startUtc, DateTime endUtc)
    {
        var changes = new List<ServerConfigChange>();
        foreach (var group in snapshots.GroupBy(s => s.ConfigurationName))
        {
            ServerConfigSnapshot? prev = null;
            foreach (var cur in group.OrderBy(s => s.CaptureTime))
            {
                if (prev is not null
                    && (prev.ValueConfigured != cur.ValueConfigured || prev.ValueInUse != cur.ValueInUse)
                    && cur.CaptureTime >= startUtc && cur.CaptureTime <= endUtc)
                {
                    changes.Add(new ServerConfigChange(
                        cur.CaptureTime, cur.ConfigurationName, prev.ValueConfigured, cur.ValueConfigured,
                        prev.ValueInUse, cur.ValueInUse, cur.IsDynamic, cur.IsAdvanced));
                }

                prev = cur;
            }
        }

        return changes.OrderByDescending(c => c.ChangeTime).ThenBy(c => c.ConfigurationName, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// Diffs the sys.databases snapshots into a change per (database, setting) whose value moved between two
    /// consecutive captures — the WIDE row is UNPIVOTed against <see cref="DatabaseConfigChangeSettingNames"/> so
    /// the setting_name is the literal column name. Emits changes in [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], newest first.
    /// </summary>
    public static List<DatabaseConfigChange> DiffDatabaseConfigChanges(
        IReadOnlyList<DatabaseConfigSnapshot> snapshots, DateTime startUtc, DateTime endUtc)
    {
        var changes = new List<DatabaseConfigChange>();
        foreach (var dbGroup in snapshots.GroupBy(s => s.DatabaseName))
        {
            DatabaseConfigSnapshot? prev = null;
            foreach (var cur in dbGroup.OrderBy(s => s.CaptureTime))
            {
                if (prev is not null && cur.CaptureTime >= startUtc && cur.CaptureTime <= endUtc)
                {
                    for (var i = 0; i < DatabaseConfigChangeSettingNames.Count; i++)
                    {
                        var oldVal = i < prev.Values.Count ? prev.Values[i] : null;
                        var newVal = i < cur.Values.Count ? cur.Values[i] : null;
                        if (!string.Equals(oldVal, newVal, StringComparison.Ordinal))
                        {
                            /* A column the engine did not report before (NULL) and reports now is a first read,
                               not a change. Only this column: Azure SQL Database starts reporting it after an
                               upgrade of the collector. */
                            if (oldVal is null
                                && string.Equals(DatabaseConfigChangeSettingNames[i], "is_optimized_locking_on", StringComparison.Ordinal))
                                continue;

                            changes.Add(new DatabaseConfigChange(
                                cur.CaptureTime, cur.DatabaseName, DatabaseConfigChangeSettingNames[i], oldVal, newVal));
                        }
                    }
                }

                prev = cur;
            }
        }

        return changes
            .OrderByDescending(c => c.ChangeTime)
            .ThenBy(c => c.DatabaseName, StringComparer.Ordinal)
            .ThenBy(c => c.SettingName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// Set-diffs consecutive full trace-flag snapshots: a flag present in the newer capture but not the older =
    /// <c>enabled</c>; present in the older but not the newer = <c>disabled</c>; present in both with a changed
    /// status/scope = <c>modified</c>. Emits changes in [<paramref name="startUtc"/>, <paramref name="endUtc"/>],
    /// newest first.
    /// </summary>
    public static List<TraceFlagChange> DiffTraceFlagChanges(
        IReadOnlyList<TraceFlagSnapshot> snapshots, DateTime startUtc, DateTime endUtc)
    {
        var changes = new List<TraceFlagChange>();
        var byCapture = snapshots.GroupBy(s => s.CaptureTime).OrderBy(g => g.Key).ToList();

        for (var i = 1; i < byCapture.Count; i++)
        {
            var changeTime = byCapture[i].Key;
            if (changeTime < startUtc || changeTime > endUtc)
            {
                continue;
            }

            var prevSnap = byCapture[i - 1].GroupBy(s => s.TraceFlag).ToDictionary(g => g.Key, g => g.First());
            var curSnap = byCapture[i].GroupBy(s => s.TraceFlag).ToDictionary(g => g.Key, g => g.First());

            foreach (var (flag, c) in curSnap)
            {
                if (!prevSnap.TryGetValue(flag, out var p))
                {
                    changes.Add(new TraceFlagChange(changeTime, flag, null, c.Status, c.IsGlobal, c.IsSession, "enabled"));
                }
                else if (p.Status != c.Status || p.IsGlobal != c.IsGlobal || p.IsSession != c.IsSession)
                {
                    changes.Add(new TraceFlagChange(changeTime, flag, p.Status, c.Status, c.IsGlobal, c.IsSession, "modified"));
                }
            }

            foreach (var (flag, p) in prevSnap)
            {
                if (!curSnap.ContainsKey(flag))
                {
                    changes.Add(new TraceFlagChange(changeTime, flag, p.Status, null, p.IsGlobal, p.IsSession, "disabled"));
                }
            }
        }

        return changes.OrderByDescending(c => c.ChangeTime).ThenBy(c => c.TraceFlag).ToList();
    }
}
