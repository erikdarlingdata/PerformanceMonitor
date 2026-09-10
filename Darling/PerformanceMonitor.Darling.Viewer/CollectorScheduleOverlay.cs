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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Pure store↔editor resolution for the Collector Schedule editor — builds the EFFECTIVE editable schedule
/// the grid shows (the same per-column layering the service's
/// <c>StoreConfigProvider.ResolveSchedule</c> applies: per-server override &gt; fleet override &gt; the shared
/// <see cref="CollectorScheduleDefaults"/> code default) and turns an edited schedule back into the sparse
/// override rows to persist. No WPF or I/O, so Darling.Tests exercise the round-trip without a live store.
/// </summary>
public static class CollectorScheduleOverlay
{
    /// <summary>
    /// The effective editable schedule for a scope: start from the code defaults, apply the fleet overrides
    /// (<c>server_id</c> NULL), then — when <paramref name="serverId"/> is a server — apply that server's
    /// overrides on top (each override's non-null column wins; a NULL column falls through). Editing the fleet
    /// scope (<paramref name="serverId"/> null) shows code-default-over-fleet; editing a server shows the full
    /// effective schedule it currently collects on.
    /// </summary>
    public static List<CollectorScheduleEditItem> BuildEffectiveSchedule(
        IReadOnlyList<CollectorScheduleRow> allOverrides, int? serverId)
    {
        ArgumentNullException.ThrowIfNull(allOverrides);

        var schedule = CollectorSchedulePresets.BuildDefaultSchedule();

        ApplyScope(schedule, allOverrides.Where(o => o.ServerId is null));
        if (serverId is int sid)
        {
            ApplyScope(schedule, allOverrides.Where(o => o.ServerId == sid));
        }

        return schedule;
    }

    private static void ApplyScope(List<CollectorScheduleEditItem> schedule, IEnumerable<CollectorScheduleRow> scopeRows)
    {
        foreach (var row in scopeRows)
        {
            var item = schedule.FirstOrDefault(s => s.Name.Equals(row.CollectorName, StringComparison.OrdinalIgnoreCase));
            if (item is null)
            {
                continue; /* An override for a collector this build no longer defines — ignore it. */
            }

            if (row.FrequencyMinutes is int freq)
            {
                item.FrequencyMinutes = freq;
            }

            if (row.RetentionDays is int retention)
            {
                item.RetentionDays = retention;
            }

            item.Enabled = row.Enabled;
        }
    }

    /// <summary>True when the store holds any override row for this server (the editor's "custom vs. use
    /// default" initial state).</summary>
    /// <summary>
    /// The fastest cadence, in minutes, that any collector ENABLED and SCHEDULED for this server runs on —
    /// the input <see cref="ServerHealthClassifier.EffectiveStaleThreshold"/> reduces to the server's stale
    /// cutoff (#3236). Built on <see cref="BuildEffectiveSchedule"/> so the viewer's freshness band reads the
    /// SAME effective schedule its own schedule editor shows, rather than a second resolution of the same
    /// override rows.
    ///
    /// <para>Excludes collectors for the other engine (a PostgreSQL target must not inherit the SQL Server
    /// collectors' one-minute cadences), disabled collectors (a cadence nothing runs on — this is why the
    /// Low-Impact preset answers 5 and not 1, since <c>long_query_completions</c> keeps a one-minute default
    /// there but ships opt-in, #1496), and on-load collectors, which run on connects rather than the loop
    /// this band watches. Returns 0 when nothing qualifies, which maps to the flat floor.</para>
    /// </summary>
    public static int FastestEnabledCadenceMinutes(
        IReadOnlyList<CollectorScheduleRow> allOverrides, int serverId, bool isPostgres)
    {
        var target = new CollectorTargetInfo
        {
            Engine = isPostgres ? CollectorTargetEngine.PostgreSql : CollectorTargetEngine.SqlServer,
        };

        var fastest = 0;
        foreach (var item in BuildEffectiveSchedule(allOverrides, serverId))
        {
            if (!item.Enabled || item.FrequencyMinutes <= 0)
            {
                continue;
            }

            if (!CollectorCatalog.EngineMatches(item.Name, target))
            {
                continue;
            }

            if (fastest == 0 || item.FrequencyMinutes < fastest)
            {
                fastest = item.FrequencyMinutes;
            }
        }

        return fastest;
    }

    public static bool ServerHasOverride(IReadOnlyList<CollectorScheduleRow> allOverrides, int serverId)
    {
        ArgumentNullException.ThrowIfNull(allOverrides);
        return allOverrides.Any(o => o.ServerId == serverId);
    }

    /// <summary>
    /// The SPARSE fleet override rows to persist: one explicit row per collector that differs from its code
    /// default (frequency, retention, or disabled) — collectors matching the default emit no row, so the fleet
    /// scope stays sparse and un-set collectors fall through to the code default.
    /// </summary>
    public static List<CollectorScheduleRow> ToFleetOverrideRows(IReadOnlyList<CollectorScheduleEditItem> edited)
    {
        ArgumentNullException.ThrowIfNull(edited);

        var rows = new List<CollectorScheduleRow>();
        foreach (var item in edited)
        {
            if (!CollectorScheduleDefaults.All.TryGetValue(item.Name, out var def))
            {
                continue; /* Not a known collector — never persist an override for it. */
            }

            /* #2064: compare Enabled to the collector's DEFAULT, not to bare true. The old test
               skipped the row whenever the item was enabled at default frequency/retention — so
               ENABLING a default-OFF collector (long_query_completions) at fleet scope wrote
               NOTHING and silently never took effect, while the same edit at SERVER scope (which
               writes every row unconditionally) did. That asymmetry is the #2061 report. */
            if (item.FrequencyMinutes == def.FrequencyMinutes
                && item.RetentionDays == def.RetentionDays
                && item.Enabled == def.DefaultEnabled)
            {
                continue; /* Matches the code default — no override row (keeps the table sparse). */
            }

            rows.Add(new CollectorScheduleRow(null, item.Name, item.FrequencyMinutes, item.RetentionDays, item.Enabled));
        }

        return rows;
    }

    /// <summary>
    /// The full PER-SERVER override rows to persist: one explicit row per known collector (a full snapshot,
    /// matching Lite's per-server schedule model) so the server collects on EXACTLY the shown schedule
    /// regardless of the fleet layer — WYSIWYG. Used only when the server is "custom" (not using defaults).
    /// </summary>
    public static List<CollectorScheduleRow> ToServerOverrideRows(IReadOnlyList<CollectorScheduleEditItem> edited, int serverId)
    {
        ArgumentNullException.ThrowIfNull(edited);

        return edited
            .Where(item => CollectorScheduleDefaults.All.ContainsKey(item.Name))
            .Select(item => new CollectorScheduleRow(serverId, item.Name, item.FrequencyMinutes, item.RetentionDays, item.Enabled))
            .ToList();
    }
}
