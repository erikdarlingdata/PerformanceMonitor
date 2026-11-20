/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The lower bound on <c>collection_time</c> for a collector's newest reading to still count as current: now minus the
/// larger of a fixed floor and a multiple of the interval the collector is scheduled at on that server, so a collector
/// an operator scheduled slower than the floor assumes is not read as stopped between samples (#7196, #9013).
///
/// <para>The interval is the one the scheduler runs by: the server's own row in <c>config_collector_schedules</c>, else
/// the fleet-wide row, else the shipped default. The table is sparse, so the two lookups are keyed scalar subqueries
/// against a handful of rows rather than a join that could multiply rows. A row of 0 means on-load only, which the
/// scheduler re-runs every <see cref="CollectorScheduleDefaults.OnLoadRecaptureMinutes"/>
/// (<see cref="CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes"/>), so 0 is bounded by that interval and
/// not read as the floor. A server row of 0 wins over a fleet row, as it does in the scheduler.</para>
///
/// <para>A collector that is switched off collects nothing more, so no newer reading is coming and the multiple would
/// only keep its last reading current for hours after it stopped (#9139). When the collector is disabled the cutoff is
/// the floor alone. <c>enabled</c> resolves as <c>StoreConfigProvider.ResolveSchedule</c> resolves it, column by column:
/// the server's row, else the fleet row, else <see cref="CollectorScheduleDefaults.Entry.DefaultEnabled"/>. A NULL
/// <c>enabled</c> on the server row falls through to the fleet row, so a server row that sets only the interval does
/// not re-enable a collector the fleet row switched off.</para>
/// </summary>
public static class CollectorFreshnessSql
{
    /// <summary>
    /// A SQL expression for the cutoff instant (naive UTC, a <c>timestamp</c>).
    /// </summary>
    /// <param name="collectorName">The collector the reading comes from, a name in <see cref="CollectorScheduleDefaults.All"/>.</param>
    /// <param name="intervalMultiplier">How many of the collector's intervals a reading may be old, at least 1.</param>
    /// <param name="serverIdExpression">The server's id as SQL: <c>s.server_id</c> or a parameter.</param>
    /// <param name="nowParameter">The parameter holding now as naive UTC, for example <c>$2</c>.</param>
    /// <param name="floorMinutesSql">The floor in whole minutes as SQL: an integer parameter or literal.</param>
    public static string Cutoff(
        string collectorName, int intervalMultiplier, string serverIdExpression, string nowParameter, string floorMinutesSql)
    {
        if (!CollectorScheduleDefaults.All.ContainsKey(collectorName))
        {
            throw new ArgumentException($"'{collectorName}' is not a collector the scheduler knows.", nameof(collectorName));
        }

        if (intervalMultiplier < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(intervalMultiplier), intervalMultiplier, "The multiplier must be at least 1.");
        }

        var multiplier = intervalMultiplier.ToString(CultureInfo.InvariantCulture);
        var defaultEnabled = CollectorScheduleDefaults.All[collectorName].DefaultEnabled ? "TRUE" : "FALSE";
        return $@"({nowParameter} - (CASE WHEN COALESCE(
        (SELECT enabled FROM config.config_collector_schedules
         WHERE collector_name = '{collectorName}' AND server_id = {serverIdExpression}),
        (SELECT enabled FROM config.config_collector_schedules
         WHERE collector_name = '{collectorName}' AND server_id IS NULL),
        {defaultEnabled})
        THEN GREATEST({floorMinutesSql}, {multiplier} * COALESCE(NULLIF(COALESCE(
        (SELECT frequency_minutes FROM config.config_collector_schedules
         WHERE collector_name = '{collectorName}' AND server_id = {serverIdExpression}),
        (SELECT frequency_minutes FROM config.config_collector_schedules
         WHERE collector_name = '{collectorName}' AND server_id IS NULL),
        {CollectorScheduleDefaults.ResolveFrequencyMinutes(collectorName, null, null)}), 0),
        {CollectorScheduleDefaults.OnLoadRecaptureMinutes}))
        ELSE {floorMinutesSql} END) * INTERVAL '1 minute')";
    }
}
