/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #4938: what <c>get_collection_health</c> shows for a collector that has a run time. <see cref="RunAt"/> is the run
/// time as 24-hour <c>HH:MM</c> on the monitored server's clock. <see cref="NextRunUtc"/> is when the collector is next
/// due, UTC, or null when the collector is disabled (it has a run time, but nothing is scheduled). Darling's twin is
/// the same record, so the two tools publish the same two fields.
/// </summary>
internal sealed record CollectorRunTimeReading(string RunAt, DateTime? NextRunUtc)
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
/// collection_log row of ANY status, so a failed attempt is the day's run here as it is in the sweep. The health read
/// covers seven days; a collector whose last run is older reads as never run, which gives the same next slot for every
/// interval up to seven days.</para>
///
/// <para><b>Nothing here is banded.</b> The health band is read from the shipped cadence and never sees any of this.</para>
///
/// <para>Registered in the host's services, so the tool takes it as a service parameter and the schedule reaches the
/// tool without a static.</para>
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
        var lastRuns = rows.ToDictionary(r => r.CollectorName, r => r.LastRunTime, StringComparer.OrdinalIgnoreCase);
        return Compute(storageServerId, settings, lastRuns, clock, nowUtc);
    }

    /// <summary>
    /// Pure: the readings for the collectors in <paramref name="settings"/>. <paramref name="clock"/> is the server's
    /// clock, or null while the store holds none, which reads the run time as UTC.
    /// </summary>
    internal static IReadOnlyDictionary<string, CollectorRunTimeReading> Compute(
        int storageServerId,
        IReadOnlyList<ScheduleManager.RunTimeSetting> settings,
        IReadOnlyDictionary<string, DateTime?> lastRuns,
        ServerClock? clock,
        DateTime nowUtc)
    {
        Func<DateTime, DateTime> localToUtc = clock is null ? CollectorRunTime.LocalIsUtc : clock.ToUtc;
        var result = new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);
        foreach (var setting in settings)
        {
            DateTime? next = null;
            if (setting.Enabled)
            {
                /* The health row's stamp is naive UTC. */
                DateTime? last = lastRuns.TryGetValue(setting.Collector, out var stamp) && stamp is { } ran
                    ? DateTime.SpecifyKind(ran, DateTimeKind.Utc)
                    : null;
                next = CollectorRunTime.NextDue(nowUtc, last, setting.RunAtMinute, setting.IntervalMinutes, storageServerId, localToUtc);
            }

            result[setting.Collector] = new CollectorRunTimeReading(CollectorRunTime.Format(setting.RunAtMinute), next);
        }

        return result;
    }
}
