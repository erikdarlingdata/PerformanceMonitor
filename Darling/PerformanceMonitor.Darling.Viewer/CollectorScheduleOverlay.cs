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

            /* V125 (#3477): the scope layers the way its column resolves — NULL falls through (the
               fleet value already applied stays), a non-null value (INCLUDING empty, the explicit
               "no scope") overwrites. Same reading as StoreConfigProvider.ResolveDatabaseScope. */
            if (row.Databases is not null)
            {
                item.DatabasesText = FormatDatabases(row.Databases);
            }

            item.Enabled = row.Enabled;
        }
    }

    /// <summary>Comma-joined for the grid — the Settings window's excluded-databases format.</summary>
    public static string FormatDatabases(IReadOnlyList<string> databases) => string.Join(", ", databases);

    /// <summary>Parses the grid's comma-separated scope exactly the way the Settings window parses
    /// excluded databases (split on comma, trim, drop blanks) — one entry discipline for the two
    /// instruments that must agree on what a name is.</summary>
    public static List<string> ParseDatabases(string? text) =>
        (text ?? "")
            .Split(',')
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();

    /// <summary>True when the store holds any override row for this server (the editor's "custom vs. use
    /// default" initial state).</summary>
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
            var databases = ParseDatabases(item.DatabasesText);

            /* #3477: a non-empty scope is an override in its own right — a collector at default
               cadence scoped to one database must still write its fleet row, or the scope silently
               never takes effect (the #2064/#2061 skipped-row failure, one column over). */
            if (item.FrequencyMinutes == def.FrequencyMinutes
                && item.RetentionDays == def.RetentionDays
                && item.Enabled == def.DefaultEnabled
                && databases.Count == 0)
            {
                continue; /* Matches the code default — no override row (keeps the table sparse). */
            }

            /* A blank scope on a fleet row writes NULL, not an empty array: at fleet level there is
               no lower layer to opt back out of, and NULL is this table's "column not overridden". */
            rows.Add(new CollectorScheduleRow(
                null, item.Name, item.FrequencyMinutes, item.RetentionDays, item.Enabled,
                databases.Count > 0 ? databases : null));
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
            /* #3477: WYSIWYG holds for the scope column too — a blank box writes the EXPLICIT empty
               array (not NULL), so a customizing server collects exactly the shown scope and a
               fleet-level scope cannot bleed through a server whose grid shows none. The empty/NULL
               distinction is the resolver's documented contract. */
            .Select(item => new CollectorScheduleRow(
                serverId, item.Name, item.FrequencyMinutes, item.RetentionDays, item.Enabled,
                ParseDatabases(item.DatabasesText)))
            .ToList();
    }
}
