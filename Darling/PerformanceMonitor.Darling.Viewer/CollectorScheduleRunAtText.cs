/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The line under the Collector Schedules grid for the selected row's Run at cell (#4938): what the time means on the
/// monitored server's clock and in UTC, and when the next run is. It is built from the shared rules in
/// <see cref="CollectorRunTime"/> (the slot, the next due time, the whole-day check and every refusal text) and not
/// from a second copy of them, so what the editor says is what the service does. Pure: the window passes the clock
/// it read, the server's engine and the current time in, so Darling.Tests exercise it without a window or a store.
///
/// <para>A fleet row shows the spread it will run across ("between 02:00 and 03:00"), because each server's own
/// minute is a function of its id and no one slot belongs to the fleet. A server row shows its exact minute, the UTC
/// instant it converts to and the next run. A server's clock that is not known yet (it has not collected
/// <c>server_properties</c>) reads the time as UTC, and the line says so.</para>
/// </summary>
public static class CollectorScheduleRunAtText
{
    private const int MinutesPerDay = 1440;

    /// <summary>
    /// Describes one row's run time.
    /// </summary>
    /// <param name="collector">The collector's name, for the refusal text.</param>
    /// <param name="runAtText">What the Run at cell holds (see <see cref="CollectorScheduleOverlay.TryParseRunAt"/>).</param>
    /// <param name="frequencyMinutes">The row's own interval, as edited.</param>
    /// <param name="serverId">The server the row belongs to, or null for the fleet row.</param>
    /// <param name="fleetRunAtMinute">The fleet row's run time for this collector, which a server row set to "Use default" falls through to; null for none.</param>
    /// <param name="clock">The server's clock from its newest <c>server_properties</c> row, or null when it has none yet.</param>
    /// <param name="azureSqlDatabase">The server is an Azure SQL Database, which always reports UTC.</param>
    /// <param name="nowUtc">The current instant, UTC.</param>
    public static string Describe(
        string collector, string runAtText, int frequencyMinutes, int? serverId, int? fleetRunAtMinute,
        ServerClock? clock, bool azureSqlDatabase, DateTime nowUtc)
    {
        if (!CollectorScheduleOverlay.TryParseRunAt(runAtText, out var typed, out var invalid))
        {
            return invalid;
        }

        var fleet = fleetRunAtMinute is >= 0 ? fleetRunAtMinute : null;

        /* Which time applies. The fleet row has no level below it to fall through to, and "None" there clears the
           column, so both read as no time. On a server row "None" stops the fleet's time, and "Use default" takes it. */
        int? effective;
        var inherited = false;
        if (serverId is null)
        {
            effective = typed is >= 0 ? typed : null;
        }
        else if (typed is -1)
        {
            return fleet is int fleetMinute
                ? $"No fixed run time on this server: it collects on its own cadence, and the fleet-wide run time ({CollectorRunTime.Format(fleetMinute)}) does not apply to it."
                : "No fixed run time on this server: it collects on its own cadence.";
        }
        else if (typed is null)
        {
            effective = fleet;
            inherited = fleet is not null;
        }
        else
        {
            effective = typed;
        }

        if (effective is not int minute)
        {
            return "No fixed run time. The collector runs on its own cadence.";
        }

        /* The whole-day rule, judged on the interval the collector really recurs at (an on-load collector re-runs daily). */
        var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(frequencyMinutes);
        if (!CollectorRunTime.AllowsRunAt(interval))
        {
            return CollectorRunTime.IntervalRefusalMessage(collector, interval);
        }

        var days = interval / MinutesPerDay;
        var every = days > 1 ? string.Create(CultureInfo.InvariantCulture, $" It runs every {days} days.") : "";

        if (serverId is not int id)
        {
            var end = (minute + CollectorRunTime.SpreadSeconds / 60) % MinutesPerDay;
            return $"Runs between {CollectorRunTime.Format(minute)} and {CollectorRunTime.Format(end)} server time: each server gets its own minute in that hour, so a fleet does not all start at once.{every}";
        }

        Func<DateTime, DateTime> toUtc = clock is null ? CollectorRunTime.LocalIsUtc : clock.ToUtc;
        var nowLocal = clock is null ? nowUtc : clock.ToServerLocal(nowUtc);
        var slot = CollectorRunTime.SlotUtc(DateOnly.FromDateTime(nowLocal), minute, id, toUtc);
        var slotLocal = clock is null ? slot : clock.ToServerLocal(slot);

        var text = new StringBuilder();
        if (inherited)
        {
            text.Append(string.Create(CultureInfo.InvariantCulture, $"Uses the fleet-wide run time ({CollectorRunTime.Format(minute)}). "));
        }

        text.Append(string.Create(CultureInfo.InvariantCulture,
            $"{slotLocal:HH:mm} server time ({slot:HH:mm} UTC {DayWord(slot, nowUtc)})."));

        if (azureSqlDatabase)
        {
            text.Append(" Azure SQL Database always reports UTC.");
        }
        else if (clock is null)
        {
            text.Append(" This server's clock is not known yet, so the time is read as UTC until its first server_properties row arrives.");
        }

        if (days > 1)
        {
            /* The date of a run that is not daily depends on the last run, which the viewer does not read. */
            text.Append(every);
            return text.ToString();
        }

        var next = CollectorRunTime.NextDue(nowUtc, null, minute, interval, id, toUtc);
        if (next <= nowUtc)
        {
            /* Today's window is open now. A run still due today starts in it, and the viewer cannot tell whether one
               ran, so it names the slot after this window. */
            var following = CollectorRunTime.NextDue(nowUtc, nowUtc, minute, interval, id, toUtc);
            text.Append(string.Create(CultureInfo.InvariantCulture,
                $" Next run: {following:yyyy-MM-dd HH:mm} UTC (today's window is open now, and a run still due today starts in it)."));
        }
        else
        {
            text.Append(string.Create(CultureInfo.InvariantCulture, $" Next run: {next:yyyy-MM-dd HH:mm} UTC."));
        }

        return text.ToString();
    }

    /// <summary>The UTC day a slot falls on, against the UTC day of now: a zone ahead of UTC puts its early-morning slot on the day before.</summary>
    private static string DayWord(DateTime slotUtc, DateTime nowUtc) =>
        (slotUtc.Date - nowUtc.Date).Days switch
        {
            0 => "today",
            -1 => "yesterday",
            1 => "tomorrow",
            _ => slotUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        };
}
