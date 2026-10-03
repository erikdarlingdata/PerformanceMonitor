/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #4938: what <c>get_collection_health</c> shows for a collector that has a run time. <see cref="RunAt"/> is the
/// run time as 24-hour <c>HH:MM</c> on the monitored server's clock. <see cref="NextRunUtc"/> is when the collector
/// is next due, UTC, or null when the collector is disabled (it has a run time but nothing is scheduled).
/// <see cref="SkippedDayNote"/> is set only when a day was skipped: the collector has not run for longer than its
/// stale line allows, and its next slot is still ahead (see <see cref="DarlingCollectorRunTimeReader.SkippedDayNoteFor"/>).
/// </summary>
internal sealed record CollectorRunTimeReading(string RunAt, DateTime? NextRunUtc, string? SkippedDayNote = null)
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
        SELECT server_id, collector_name, frequency_minutes, retention_days, enabled
        FROM config.config_collector_schedules
        WHERE server_id = $1 OR server_id IS NULL
        """;

    /// <summary>The same two layers of the run times, from their own table (<c>config.config_collector_run_times</c>), which the
    /// service reads the same way (<see cref="StoreConfigProvider.RunTimesSelectSql"/>): a run time is not a column of the
    /// schedule rows, because the viewer's schedule Save deletes and re-inserts those. $1 server_id. Schema-qualified, so a 42P01
    /// from it can only mean this table is missing.</summary>
    public const string RunTimeSql = """
        SELECT server_id, collector_name, run_at_minute
        FROM config.config_collector_run_times
        WHERE server_id = $1 OR server_id IS NULL
        """;

    private static readonly IReadOnlyDictionary<string, CollectorRunTimeReading> s_none =
        new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The run time and next due time of every collector in <paramref name="rows"/> that has a run time on this server.
    /// A server with no run time anywhere costs the two small reads: the clock is read only when a run time exists. The run
    /// times are layered onto the schedule rows by <see cref="StoreConfigProvider.MergeRunTimes"/>, the service's own merge. A
    /// store below V160 has no run-time table, which is no run times, so the 42P01 is answered with none.
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
                    reader.GetBoolean(4)));
            }
        }

        var runTimes = await ReadRunTimesAsync(postgres, serverId, cancellationToken);
        var layered = StoreConfigProvider.MergeRunTimes(overrides, runTimes);

        if (!layered.Any(o => o.RunAtMinute is >= 0))
        {
            return s_none;
        }

        var clock = await DarlingServerClockReader.ReadAsync(postgres, serverId, cancellationToken);
        return Compute(serverId, layered, rows, clock.ToUtc, nowUtc);
    }

    /// <summary>The fleet's and this server's run-time rows. A store below V160 has no such table, and that is "no run times"
    /// (every collector had none before the table existed), so the 42P01 is answered with an empty list. A role that may not
    /// read the table (42501, an mcp role provisioned before the grant existed) is answered the same way, with one warning,
    /// so the health tool never fails as a whole for a missing optional layer; any other failure propagates. The state code,
    /// not the message text, is matched: lc_messages is not always English.</summary>
    private static Task<IReadOnlyList<RunTimeOverride>> ReadRunTimesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken) =>
        ReadRunTimesAsync(
            async () =>
            {
                var runTimes = new List<RunTimeOverride>();
                await using var command = postgres.CreateCommand(RunTimeSql);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                DarlingMcpReadParameters.AddInt(command, serverId);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    runTimes.Add(new RunTimeOverride(
                        reader.IsDBNull(0) ? null : reader.GetInt32(0),
                        reader.GetString(1),
                        reader.GetInt16(2)));
                }

                return runTimes;
            },
            message => System.Diagnostics.Trace.TraceWarning(message));

    /// <summary>The state-code handling of <see cref="ReadRunTimesAsync(NpgsqlDataSource,int,CancellationToken)"/>, with the read
    /// and the warning passed in so the two refusals are testable without a store.</summary>
    internal static async Task<IReadOnlyList<RunTimeOverride>> ReadRunTimesAsync(
        Func<Task<List<RunTimeOverride>>> read, Action<string> warn)
    {
        try
        {
            return await read();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return Array.Empty<RunTimeOverride>();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            warn("get_collection_health: the MCP role cannot read config.config_collector_run_times (42501), so collector run times are not shown. "
                + "Re-run provision-roles.sql to grant it SELECT.");
            return Array.Empty<RunTimeOverride>();
        }
    }

    /// <summary>
    /// Pure: the readings for the collectors that have a run time. A collector the catalog does not know, or one with no
    /// run time once the layers and the whole-day rule are applied, has no entry. <paramref name="localToUtc"/> is the
    /// server clock's conversion, or <see cref="CollectorRunTime.LocalIsUtc"/> while no clock is known.
    /// </summary>
    internal static IReadOnlyDictionary<string, CollectorRunTimeReading> Compute(
        int serverId, IReadOnlyList<ScheduleOverride> overrides, IEnumerable<CollectorHealth> rows,
        Func<DateTime, DateTime> localToUtc, DateTime nowUtc)
    {
        var result = new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows)
        {
            var collector = row.CollectorName;
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
            string? note = null;
            if (schedule.Enabled)
            {
                var interval = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes);

                /* The worker's floor: a last run older than it counts as never run. The health row's stamp is naive UTC. */
                DateTime? last = row.LastRunTime is DateTime stamp && stamp >= nowUtc - DarlingWorker.WatermarkFloorLookback
                    ? DateTime.SpecifyKind(stamp, DateTimeKind.Utc)
                    : null;
                next = CollectorRunTime.NextDue(nowUtc, last, minute, interval, serverId, localToUtc);
                note = SkippedDayNoteFor(row, interval, next.Value, nowUtc);
            }

            result[collector] = new CollectorRunTimeReading(CollectorRunTime.Format(minute), next, note);
        }

        return result;
    }

    /// <summary>
    /// The note a row carries when a day was skipped (#4938). A run time skips a day that could not start inside its
    /// 60-minute grace and never replays it, so the gap to the next slot can be longer than the shipped cadence allows,
    /// and the collector crosses its stale line before that slot arrives. The band is right to say so, because a day WAS
    /// missed; the note says why the band reads that way and when the next run is.
    ///
    /// <para>It needs all of: a last run (any status, as the worker counts it) older than the stale line the band reads
    /// from the shipped cadence; older than the longest gap between two on-time runs, one interval plus the grace and an
    /// hour for a daylight-saving change, so a collector whose shipped cadence is shorter than its run-time interval does
    /// not read as skipped on an ordinary day; and a next run that is still ahead. A collector that is due now will run
    /// in a moment, so it has no note. The note is never a band input: <see cref="CollectorHealth.HealthStatus"/> does
    /// not see it.</para>
    /// </summary>
    internal static string? SkippedDayNoteFor(CollectorHealth row, int intervalMinutes, DateTime nextRunUtc, DateTime nowUtc)
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
            $"Skipped day: no run for {hoursSinceLastRun:0.#} hours, past this collector's {staleLineHours:0.#}-hour stale line, and its next run is not due until {nextRunUtc:u}. With a run time, a day that could not start within {CollectorRunTime.GraceMinutes} minutes of its slot is skipped, not replayed.");
    }
}
