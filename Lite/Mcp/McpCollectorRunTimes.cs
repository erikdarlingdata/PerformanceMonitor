/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #4938: what <c>get_collection_health</c> shows for a collector that has a run time. <see cref="RunAt"/> is the run
/// time as 24-hour <c>HH:MM</c> on the monitored server's clock. <see cref="NextRunUtc"/> is when the collector is next
/// due, UTC, or null when the collector is disabled (it has a run time, but nothing is scheduled).
/// <see cref="SkippedDayNote"/> is set only when a day was skipped: the collector has not run for longer than its stale
/// line allows, and its next slot is still ahead (see <see cref="McpCollectorRunTimes.SkippedDayNoteFor"/>). Darling's
/// twin is the same record, so the two tools publish the same three fields.
/// </summary>
internal sealed record CollectorRunTimeReading(string RunAt, DateTime? NextRunUtc, string? SkippedDayNote = null)
{
    /// <summary>The next due time as the round-trip text every other stamp on the health payload uses, or null.</summary>
    public string? NextRunUtcText => NextRunUtc?.ToString("o");
}

/// <summary>
/// #4938: reads which collectors on one server have a run time and when each is next due, for the health tool. The
/// schedule is the one the sweep reads (<see cref="ScheduleManager.GetRunTimeSettingsForStorageServer"/>), and the due
/// time comes from <see cref="CollectorRunTime.NextDue"/>, the shared rule the sweep asks, so the answer here is the one
/// the sweep acts on: the run time plus the server's fixed spread (under 60 minutes, by the server id), on the server's
/// own clock, which is the newest server_properties row's (UTC while the store holds none).
///
/// <para><b>A run counts the way the start-up read counts it.</b> The last run is the health row's newest
/// collection_log row of ANY status, so a failed attempt is the day's run here as it is in the sweep. A run older than
/// the collector's floor, its interval plus a day (<see cref="ScheduleManager.LastRunFloor"/>), reads as never run, the
/// same floor the sweep applies to each collector it seeds, so the two name the same next run. The health read covers
/// seven days, which holds a floor for an interval up to six days; a run older than seven days is not seen here.</para>
///
/// <para><b>Nothing here is banded.</b> The health band is read from the shipped cadence and never sees any of this.</para>
///
/// <para>Registered in the host's services (<see cref="McpHostService.RegisterCollectorRunTimes"/>), so the tool takes it
/// as a service parameter and the schedule reaches the tool without a static.</para>
/// </summary>
public sealed class McpCollectorRunTimes
{
    private static readonly IReadOnlyDictionary<string, CollectorRunTimeReading> s_none =
        new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);

    private readonly ScheduleManager? _schedules;
    private readonly ServerManager _servers;

    /// <param name="schedules">The collector schedules, or null when the host has none: no collector has a run time then.</param>
    /// <param name="servers">The server list the storage id is looked up in.</param>
    public McpCollectorRunTimes(ScheduleManager? schedules, ServerManager servers)
    {
        _schedules = schedules;
        _servers = servers;
    }

    /// <summary>
    /// The run time and next due time of every collector in <paramref name="rows"/> that has a run time on this server.
    /// The server's clock is read only when a run time exists, so a server with none costs no store read.
    /// </summary>
    internal async Task<IReadOnlyDictionary<string, CollectorRunTimeReading>> ReadAsync(
        LocalDataService dataService, int storageServerId, IReadOnlyCollection<CollectorHealthRow> rows, DateTime nowUtc)
    {
        if (_schedules is null)
        {
            return s_none;
        }

        var settings = _schedules.GetRunTimeSettingsForStorageServer(_servers, storageServerId, rows.Select(r => r.CollectorName));
        if (settings.Count == 0)
        {
            return s_none;
        }

        var clock = await dataService.GetServerClockAsync(storageServerId);
        return Compute(storageServerId, settings, rows, clock, nowUtc);
    }

    /// <summary>
    /// Pure: the readings for the collectors in <paramref name="settings"/>. <paramref name="rows"/> are the health rows,
    /// which carry each collector's last run and its shipped cadence (the note's inputs); a setting with no row has never
    /// run in the window and carries no note. <paramref name="clock"/> is the server's clock, or null while the store
    /// holds none, which reads the run time as UTC.
    /// </summary>
    internal static IReadOnlyDictionary<string, CollectorRunTimeReading> Compute(
        int storageServerId,
        IReadOnlyList<ScheduleManager.RunTimeSetting> settings,
        IEnumerable<CollectorHealthRow> rows,
        ServerClock? clock,
        DateTime nowUtc)
    {
        Func<DateTime, DateTime> localToUtc = clock is null ? CollectorRunTime.LocalIsUtc : clock.ToUtc;
        var byCollector = rows.ToDictionary(r => r.CollectorName, StringComparer.OrdinalIgnoreCase);
        var result = new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);
        foreach (var setting in settings)
        {
            DateTime? next = null;
            string? note = null;
            if (setting.Enabled)
            {
                byCollector.TryGetValue(setting.Collector, out var row);

                /* The health row's stamp is naive UTC. A run older than the collector's floor (its interval plus a day) reads
                   as never run, the floor the sweep's start-up read applies, so the tool names the sweep's next run. */
                DateTime? last = ScheduleManager.LastRunWithinFloor(
                    row?.LastRunTime is { } ran ? DateTime.SpecifyKind(ran, DateTimeKind.Utc) : null, nowUtc, setting.IntervalMinutes);
                next = CollectorRunTime.NextDue(nowUtc, last, setting.RunAtMinute, setting.IntervalMinutes, storageServerId, localToUtc);
                if (row is not null)
                {
                    note = SkippedDayNoteFor(row, setting.RunAtMinute, setting.IntervalMinutes, next.Value, nowUtc);
                }
            }

            result[setting.Collector] = new CollectorRunTimeReading(CollectorRunTime.Format(setting.RunAtMinute), next, note);
        }

        return result;
    }

    /// <summary>
    /// The note a row carries when a day was skipped (#4938). Lite collects only while it is open, and a run time skips a
    /// day that could not start inside its 60-minute grace and never replays it, so the gap to the next slot can be longer
    /// than the shipped cadence allows, and the collector crosses its stale line before that slot arrives. The band is
    /// right to say so, because a day WAS missed; the note says why the band reads that way and when the next run is.
    ///
    /// <para>It needs all of: a last run (any status, as the sweep counts it) older than the stale line the band reads
    /// from the shipped cadence; older than the longest gap between two on-time runs, one interval plus the grace and an
    /// hour for a daylight-saving change, so a collector whose shipped cadence is shorter than its run-time interval does
    /// not read as skipped on an ordinary day; and a next run that is still ahead. A collector that is due now will run
    /// in a moment, so it has no note. The note is never a band input: <see cref="CollectorHealthRow.HealthStatus"/> does
    /// not see it. Darling's twin applies the same rule and carries the same text up to the sentence about why.</para>
    ///
    /// <para>The note names the slot as <c>HH:MM</c> on the monitored server's clock, the form the row's <c>run_at</c> uses.</para>
    /// </summary>
    internal static string? SkippedDayNoteFor(
        CollectorHealthRow row, int runAtMinute, int intervalMinutes, DateTime nextRunUtc, DateTime nowUtc)
    {
        if (row.LastRunTime is not DateTime lastRun || nextRunUtc <= nowUtc)
        {
            return null;
        }

        var hoursSinceLastRun = (nowUtc - DateTime.SpecifyKind(lastRun, DateTimeKind.Utc)).TotalHours;
        var staleLineHours = CollectorHealthClassifier.StaleThresholdHours(row.FrequencyMinutes);
        var longestOnTimeGapHours = (intervalMinutes + CollectorRunTime.GraceMinutes + 60) / 60.0;
        if (hoursSinceLastRun <= staleLineHours || hoursSinceLastRun <= longestOnTimeGapHours)
        {
            return null;
        }

        return string.Create(CultureInfo.InvariantCulture,
            $"Skipped day: no run for {hoursSinceLastRun:0.#} hours, past this collector's {staleLineHours:0.#}-hour stale line, and its next run is not due until {nextRunUtc:u}. Lite collects only while it is open: with a run time, a day is skipped, not replayed, when Lite could not start its {CollectorRunTime.Format(runAtMinute)} slot (the monitored server's clock) within {CollectorRunTime.GraceMinutes} minutes of it.");
    }
}
