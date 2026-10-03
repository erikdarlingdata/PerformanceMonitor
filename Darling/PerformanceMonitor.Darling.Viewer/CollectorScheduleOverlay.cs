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
    /// <summary>What the Run at cell shows for a scope with no run-time row of its own: the scope falls through to the
    /// fleet row, or to no fixed time (#4938).</summary>
    public const string UseDefaultRunAtText = "Use default";

    /// <summary>What a server row's Run at cell shows for -1: no fixed time on this server, which stops a fleet-wide
    /// time (#4938). On the fleet row it is read as clearing the time, because the fleet has nothing to stop.</summary>
    public const string NoRunAtText = "None";

    /// <summary>The stored value for "no fixed time on this server", the V160 table's -1 (a server row only).</summary>
    private const int NoFixedRunTime = -1;

    /// <summary>
    /// The text the Run at cell holds for a stored value: none (and a value the V160 CHECK would refuse, which the
    /// service also reads as not set) is <see cref="UseDefaultRunAtText"/>, -1 is <see cref="NoRunAtText"/>, and a
    /// minute after midnight is <c>HH:MM</c> through the shared <see cref="CollectorRunTime.Format"/>.
    /// </summary>
    public static string FormatRunAt(int? runAtMinute) => runAtMinute switch
    {
        NoFixedRunTime => NoRunAtText,
        >= 0 and < 1440 => CollectorRunTime.Format(runAtMinute.Value),
        _ => UseDefaultRunAtText,
    };

    /// <summary>
    /// Reads what the Run at cell holds (#4938): blank, "Use default" or "default" is null (no value at this level),
    /// "None" is -1 (no fixed time), and a 24-hour <c>HH:MM</c> is its minutes after midnight, parsed by the shared
    /// <see cref="CollectorRunTime.TryParse"/>. Anything else is refused with the shared
    /// <see cref="CollectorRunTime.InvalidRunAtMessage"/> and a null minute. Surrounding spaces and letter case are
    /// ignored. Pure.
    /// </summary>
    public static bool TryParseRunAt(string? text, out int? minuteOfDay, out string error)
    {
        minuteOfDay = null;
        error = "";

        var trimmed = (text ?? "").Trim();
        if (trimmed.Length == 0
            || trimmed.Equals(UseDefaultRunAtText, StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("default", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (trimmed.Equals(NoRunAtText, StringComparison.OrdinalIgnoreCase))
        {
            minuteOfDay = NoFixedRunTime;
            return true;
        }

        if (CollectorRunTime.TryParse(trimmed, out var minute))
        {
            minuteOfDay = minute;
            return true;
        }

        error = CollectorRunTime.InvalidRunAtMessage;
        return false;
    }

    /// <summary>
    /// The effective editable schedule for a scope: start from the code defaults, apply the fleet overrides
    /// (<c>server_id</c> NULL), then — when <paramref name="serverId"/> is a server — apply that server's
    /// overrides on top (each override's non-null column wins; a NULL column falls through). Editing the fleet
    /// scope (<paramref name="serverId"/> null) shows code-default-over-fleet; editing a server shows the full
    /// effective schedule it currently collects on.
    ///
    /// <para>The Run at cell (#4938) comes from the run-time table (<paramref name="runTimes"/>), not from the schedule
    /// rows, and is the one column that does NOT layer: it holds the edited scope's OWN stored value, so a server with no
    /// run-time row shows "Use default" and not the fleet's time. If the cell took the fleet's value, a save would write
    /// it into every server's run-time rows and "Use default" could never survive a save. A run time needs no schedule
    /// row: a collector at its default cadence with a run time of its own has a run-time row and no schedule row.</para>
    /// </summary>
    public static List<CollectorScheduleEditItem> BuildEffectiveSchedule(
        IReadOnlyList<CollectorScheduleRow> allOverrides, IReadOnlyList<CollectorRunTimeRow> runTimes, int? serverId)
    {
        ArgumentNullException.ThrowIfNull(allOverrides);
        ArgumentNullException.ThrowIfNull(runTimes);

        var schedule = CollectorSchedulePresets.BuildDefaultSchedule();

        ApplyScope(schedule, allOverrides.Where(o => o.ServerId is null));
        if (serverId is int sid)
        {
            ApplyScope(schedule, allOverrides.Where(o => o.ServerId == sid));
        }

        ApplyRunTimes(schedule, runTimes.Where(r => r.ServerId == serverId));

        return schedule;
    }

    /// <summary>The schedule rows alone, with no run times: every Run at cell reads "Use default". The shape released viewers
    /// have, kept so a caller that has no run-time rows (the schedule overlay's own tests, a store below V160) need not
    /// invent an empty list. The editor window passes the run times it read, through the overload above.</summary>
    public static List<CollectorScheduleEditItem> BuildEffectiveSchedule(
        IReadOnlyList<CollectorScheduleRow> allOverrides, int? serverId) =>
        BuildEffectiveSchedule(allOverrides, Array.Empty<CollectorRunTimeRow>(), serverId);

    /// <summary>Puts the edited scope's own run times on the grid items (#4938): a collector with a row in the scope shows
    /// it (<see cref="FormatRunAt"/>), and every other collector keeps "Use default". A row for a collector this build no
    /// longer defines is ignored, the way a schedule row for one is.</summary>
    private static void ApplyRunTimes(List<CollectorScheduleEditItem> schedule, IEnumerable<CollectorRunTimeRow> scopeRunTimes)
    {
        foreach (var runTime in scopeRunTimes)
        {
            var item = schedule.FirstOrDefault(s => s.Name.Equals(runTime.CollectorName, StringComparison.OrdinalIgnoreCase));
            if (item is not null)
            {
                item.RunAtText = FormatRunAt(runTime.RunAtMinute);
            }
        }
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

    /// <summary>
    /// Enforces what the store and the collection pipeline can honor before the write, so a bad value
    /// surfaces as a friendly message rather than a raw Postgres error or silent bad data: the V17 CHECK
    /// constraints (frequency &gt;= 0, retention &gt;= 1) plus the delta gap-policy cadence cap (#3532) —
    /// a delta-family collector past <see cref="CollectorDeltaCalculator.MaxDeltaFrequencyMinutes"/> would
    /// exceed <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> every cycle and record permanent
    /// zeros (the service's <c>StoreConfigProvider.ResolveSchedule</c> refuses such a row too, by falling
    /// through to the default). Pure, so Darling.Tests exercise it without a Window.
    /// </summary>
    public static bool ValidateSchedule(IReadOnlyList<CollectorScheduleEditItem> edited, out string error)
    {
        ArgumentNullException.ThrowIfNull(edited);

        foreach (var item in edited)
        {
            if (item.FrequencyMinutes < 0)
            {
                error = $"'{item.Name}': frequency (minutes) can't be negative. Use 0 to collect once on server load.";
                return false;
            }

            if (CollectorDeltaCalculator.DeltaFrequencyError(item.Name, item.FrequencyMinutes) is string frequencyError)
            {
                error = frequencyError;
                return false;
            }

            if (item.RetentionDays < 1)
            {
                error = $"'{item.Name}': retention (days) must be at least 1.";
                return false;
            }

            /* #4938: the run time. The text is the shared one, so the viewer, the CLI and the service's warning
               say one thing. "None" and "Use default" are never refused, because clearing is the remedy the
               whole-day refusal names; a time is refused only when the collector's interval is not whole days
               (an on-load collector re-runs daily, so it is judged as a daily one). */
            if (!TryParseRunAt(item.RunAtText, out var runAt, out var runAtError))
            {
                error = $"'{item.Name}': {runAtError}";
                return false;
            }

            if (runAt is >= 0)
            {
                var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(item.FrequencyMinutes);
                if (!CollectorRunTime.AllowsRunAt(interval))
                {
                    error = CollectorRunTime.IntervalRefusalMessage(item.Name, interval);
                    return false;
                }
            }
        }

        error = "";
        return true;
    }

    /// <summary>True when the store holds any schedule row for this server (the released rule, with no run-time rows to count).</summary>
    public static bool ServerHasOverride(IReadOnlyList<CollectorScheduleRow> allOverrides, int serverId) =>
        ServerHasOverride(allOverrides, Array.Empty<CollectorRunTimeRow>(), serverId);

    /// <summary>True when the store holds any override row for this server, a schedule row or a run-time row (the editor's
    /// "custom vs. use default" initial state). A server that has only a run time of its own (set from the command line, say,
    /// which writes no schedule row) is custom, so the editor shows that run time instead of the fleet's (#4938).</summary>
    public static bool ServerHasOverride(
        IReadOnlyList<CollectorScheduleRow> allOverrides, IReadOnlyList<CollectorRunTimeRow> runTimes, int serverId)
    {
        ArgumentNullException.ThrowIfNull(allOverrides);
        ArgumentNullException.ThrowIfNull(runTimes);
        return allOverrides.Any(o => o.ServerId == serverId) || runTimes.Any(r => r.ServerId == serverId);
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
            /* #4938: a run time is not an override of THIS table. It has its own table (ToRunTimeChanges), so a
               collector left at its default cadence with a fleet run time writes no schedule row. */
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
            /* #4938: the run time does not ride these rows (ToRunTimeChanges writes it to its own table), so this Save
               carries no run time and leaves every one as it was. */
            .Select(item => new CollectorScheduleRow(
                serverId, item.Name, item.FrequencyMinutes, item.RetentionDays, item.Enabled,
                ParseDatabases(item.DatabasesText)))
            .ToList();
    }

    /// <summary>
    /// The run-time table changes that make the store match the edited grid for one scope (#4938), each of them a statement
    /// of its own, written in the Save's transaction (<see cref="ViewerDataService.SaveCollectorScheduleAsync"/>): only the collectors whose time differs from
    /// what <paramref name="runTimes"/> holds for the scope, so a Save that did not touch a run time writes none and does not
    /// reload the service for it.
    ///
    /// <para>A cell that is "Use default" means no row at this level (a delete, when there is one); a time is an upsert; "None"
    /// is -1 on a server row, which stops a fleet-wide time for that server only, and on the fleet scope it is read as no row
    /// because the fleet has nothing to stop (the CHECK refuses -1 there). A server scope that "uses the default schedule"
    /// (<paramref name="usesDefault"/>) deletes every run-time row the server has, so the server follows the fleet again; a cell
    /// that does not parse is left to <see cref="ValidateSchedule"/> and changes nothing. A collector this build does not
    /// define is never written. A stored row is deleted by the name it is stored under, so a row an older writer left in a
    /// different letter case is removed too. Pure.</para>
    /// </summary>
    public static List<CollectorRunTimeChange> ToRunTimeChanges(
        IReadOnlyList<CollectorScheduleEditItem> edited, IReadOnlyList<CollectorRunTimeRow> runTimes, int? serverId, bool usesDefault)
    {
        ArgumentNullException.ThrowIfNull(edited);
        ArgumentNullException.ThrowIfNull(runTimes);

        var changes = new List<CollectorRunTimeChange>();
        var stored = runTimes.Where(r => r.ServerId == serverId).ToList();

        foreach (var item in edited)
        {
            if (!CollectorScheduleDefaults.All.ContainsKey(item.Name))
            {
                continue; /* Not a known collector — never persist a run time for it. */
            }

            int? wanted;
            if (usesDefault)
            {
                wanted = null;
            }
            else if (!TryParseRunAt(item.RunAtText, out wanted, out _))
            {
                continue;
            }
            else if (serverId is null && wanted == NoFixedRunTime)
            {
                wanted = null;
            }

            var current = stored.Where(r => r.CollectorName.Equals(item.Name, StringComparison.OrdinalIgnoreCase)).ToList();
            if (wanted is int minute)
            {
                if (current.Count == 0 || current.Any(r => r.RunAtMinute != minute))
                {
                    changes.Add(new CollectorRunTimeChange(serverId, item.Name, minute));
                }
            }
            else
            {
                foreach (var row in current)
                {
                    changes.Add(new CollectorRunTimeChange(serverId, row.CollectorName, null));
                }
            }
        }

        return changes;
    }
}
