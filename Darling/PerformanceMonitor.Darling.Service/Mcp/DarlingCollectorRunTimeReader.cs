/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #4938: what <c>get_collection_health</c> shows for a collector that has a run time. <see cref="RunAt"/> is the
/// run time as 24-hour <c>HH:MM</c> on the monitored server's clock. <see cref="NextRunUtc"/> is when the collector
/// is next due, UTC, or null when the collector is disabled (it has a run time but nothing is scheduled).
/// </summary>
internal sealed record CollectorRunTimeReading(string RunAt, DateTime? NextRunUtc)
{
    /// <summary>The next due time as the round-trip text every other stamp on the health payload uses, or null.</summary>
    public string? NextRunUtcText => NextRunUtc?.ToString("o");
}

/// <summary>
/// #4938: reads which collectors on one server have a run time and when each is next due, for the health tool. It
/// resolves the schedule the way the service does (<see cref="StoreConfigProvider.ResolveSchedule"/>: the server's row,
/// then the fleet row, then the default, with a run time refused on an interval that is not a whole number of days),
/// and it asks <see cref="CollectorRunTime.NextDue"/> for the due time, so the answer here is the one the worker acts on.
///
/// <para><b>A run counts the way the worker's connect-time read counts it.</b> The worker seeds each collector from its
/// newest <c>collection_log</c> row of ANY status (<see cref="DarlingWorker.ReadCollectorWatermarksAsync"/>), looking back
/// no further than <see cref="DarlingWorker.WatermarkFloorLookback"/>. The health row's <c>LastRunTime</c> is that same
/// newest row of any status, so it is used here, with the same floor: an older run reads as never run, exactly as it does
/// for the worker.</para>
///
/// <para><b>Nothing here is memoized or banded.</b> The health rows are held for a minute; the schedule is read fresh on
/// every call, so a run time that was just set shows at once. The health band is read from the shipped cadence and never
/// sees any of this.</para>
/// </summary>
internal static class DarlingCollectorRunTimeReader
{
    /// <summary>The fleet row and this server's row, the only two layers <see cref="StoreConfigProvider.ResolveSchedule"/>
    /// reads for one server. $1 server_id. The table is small (one row per override), so this is one cheap read.</summary>
    public const string ScheduleSql = """
        SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, run_at_minute
        FROM config.config_collector_schedules
        WHERE server_id = $1 OR server_id IS NULL
        """;

    private static readonly IReadOnlyDictionary<string, CollectorRunTimeReading> s_none =
        new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The run time and next due time of every collector in <paramref name="rows"/> that has a run time on this server.
    /// A server with no run time anywhere costs the one schedule read: the clock is read only when a run time exists.
    /// </summary>
    public static async Task<IReadOnlyDictionary<string, CollectorRunTimeReading>> ReadAsync(
        NpgsqlDataSource postgres, int serverId, IReadOnlyCollection<CollectorHealth> rows, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        var overrides = new List<ScheduleOverride>();
        await using (var command = postgres.CreateCommand(ScheduleSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                overrides.Add(new ScheduleOverride(
                    reader.IsDBNull(0) ? null : reader.GetInt32(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    reader.GetBoolean(4),
                    Databases: null,
                    RunAtMinute: reader.IsDBNull(5) ? null : reader.GetInt16(5)));
            }
        }

        if (!overrides.Exists(o => o.RunAtMinute is >= 0))
        {
            return s_none;
        }

        var clock = await DarlingServerClockReader.ReadAsync(postgres, serverId, cancellationToken);
        var collectors = new List<(string Collector, DateTime? LastRun)>(rows.Count);
        foreach (var row in rows)
        {
            collectors.Add((row.CollectorName, row.LastRunTime));
        }

        return Compute(serverId, overrides, collectors, clock.ToUtc, nowUtc);
    }

    /// <summary>
    /// Pure: the readings for the collectors that have a run time. A collector the catalog does not know, or one with no
    /// run time once the layers and the whole-day rule are applied, has no entry. <paramref name="localToUtc"/> is the
    /// server clock's conversion, or <see cref="CollectorRunTime.LocalIsUtc"/> while no clock is known.
    /// </summary>
    internal static IReadOnlyDictionary<string, CollectorRunTimeReading> Compute(
        int serverId, IReadOnlyList<ScheduleOverride> overrides, IEnumerable<(string Collector, DateTime? LastRun)> collectors,
        Func<DateTime, DateTime> localToUtc, DateTime nowUtc)
    {
        var result = new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);
        foreach (var (collector, lastRun) in collectors)
        {
            if (!CollectorScheduleDefaults.All.ContainsKey(collector))
            {
                continue;
            }

            var schedule = StoreConfigProvider.ResolveSchedule(collector, serverId, overrides);
            if (schedule.RunAtMinute is not int minute)
            {
                continue;
            }

            DateTime? next = null;
            if (schedule.Enabled)
            {
                /* The worker's floor: a last run older than it counts as never run. The health row's stamp is naive UTC. */
                DateTime? last = lastRun is DateTime stamp && stamp >= nowUtc - DarlingWorker.WatermarkFloorLookback
                    ? DateTime.SpecifyKind(stamp, DateTimeKind.Utc)
                    : null;
                next = CollectorRunTime.NextDue(
                    nowUtc, last, minute, CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes),
                    serverId, localToUtc);
            }

            result[collector] = new CollectorRunTimeReading(CollectorRunTime.Format(minute), next);
        }

        return result;
    }
}
