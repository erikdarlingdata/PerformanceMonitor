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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The store side of the Instance Restarted and Memory Dumps alerts (#8884).
///
/// <para><b>Restart.</b> Nothing new is collected. <see cref="ServerEpoch"/> already persists, for each identity carrier
/// (<c>wait_stats</c> and <c>cpu_utilization</c>, both every minute), the instance's start time and <c>@@SERVERNAME</c> as the pair
/// <see cref="ServerEpoch.IdentityStateKey"/> beside the pair it replaced (<see cref="ServerEpoch.IdentityPreviousStateKey"/>), in
/// <c>collector_state</c>. <see cref="JudgeRestart"/> reads that pair: a restart is a start time that moved LATER while the name did not move.
/// A name that moved is a failover or a rename, and the Availability Group alerts cover failover.</para>
///
/// <para><b>Memory dumps.</b> <c>cpu_utilization</c> reads <c>sys.dm_server_memory_dumps</c> in the round trip it already makes and hands
/// <see cref="ObserveMemoryDumps"/> the total, the newest creation time (UTC) and the count of the last ten minutes. The stamp is persisted as
/// <see cref="MemoryDumpsStateKey"/>, and each time it changes the stamp it replaced goes to <see cref="MemoryDumpsPreviousStateKey"/>, so the pair always describes the latest change, the way the identity pair
/// does. <see cref="JudgeMemoryDumps"/> reads that pair. No store column and no migration: both keys are rows of the existing table.</para>
///
/// <para>The judges are pure and take the rows both stores return for <see cref="StateSql"/>, so the two read adapters (Lite over DuckDB, Darling
/// over PostgreSQL) execute SQL and nothing else, the <see cref="BaselineDiscontinuities.Compose"/> idiom.</para>
/// </summary>
public static class InstanceEvents
{
    /// <summary>The persisted memory dump stamp (<see cref="SerializeDumps"/> form), written by <c>cpu_utilization</c>.</summary>
    public const string MemoryDumpsStateKey = "memory_dumps";

    /// <summary>The stamp the current one replaced, written only on a rise.</summary>
    public const string MemoryDumpsPreviousStateKey = "memory_dumps_previous";

    /// <summary>The run-note label (<c>memory_dumps_new=2</c>) a collector run writes when the newest dump moved later: how many dumps arrived since the
    /// previous stamp, at least one. A count, per <c>CollectorMeasurement</c>'s grammar.</summary>
    public const string MemoryDumpsNewMeasurement = "memory_dumps_new";

    /// <summary>The window, in minutes, that <see cref="MemoryDumpStamp.RecentCount"/> counts dumps in.</summary>
    public const int RecentWindowMinutes = 10;

    private const char Separator = '|';

    /// <summary>
    /// The identity pairs and the memory dump stamps one server's collectors have persisted, with the instant each was written. <c>$1</c> server_id. No
    /// collector filter, for the reason <see cref="BaselineDiscontinuities.IdentityPairSql"/> gives. Built from the key constants so a renamed key
    /// reds the read at compile time.
    /// </summary>
    public const string StateSql =
        "SELECT collector_name, state_key, state_value, updated_at\n"
        + "FROM collector_state\n"
        + "WHERE server_id = $1\n"
        + "AND   state_key IN ('" + ServerEpoch.IdentityStateKey + "', '" + ServerEpoch.IdentityPreviousStateKey + "', '"
        + MemoryDumpsStateKey + "', '" + MemoryDumpsPreviousStateKey + "')";

    /// <summary>One reading of <c>sys.dm_server_memory_dumps</c>: the total, the newest creation time in UTC (null with no dump), and how many dumps were created in
    /// the last <see cref="RecentWindowMinutes"/> minutes.</summary>
    public readonly record struct MemoryDumpStamp(DateTime? NewestUtc, long Count, long RecentCount);

    /// <summary>A restart the store holds: the start time before and after, and when the carrier persisted the change (UTC, the store's frame).
    /// The two start times are the SERVER's local clock, as <c>sqlserver_start_time</c> reports it.</summary>
    public readonly record struct RestartObservation(DateTime PreviousStartTime, DateTime NewStartTime, DateTime ObservedAtUtc);

    /// <summary>A rise in the newest memory dump the store holds: the stamp now and the one it replaced, and when it was persisted (UTC).</summary>
    public readonly record struct MemoryDumpObservation(MemoryDumpStamp Current, MemoryDumpStamp Previous, DateTime ObservedAtUtc);

    /// <summary>The stored form of a stamp: <c>newest|count|recent</c>, the newest time in round-trip form (empty with no dump).</summary>
    public static string SerializeDumps(MemoryDumpStamp stamp) =>
        (stamp.NewestUtc.HasValue ? stamp.NewestUtc.Value.ToString("O", CultureInfo.InvariantCulture) : string.Empty)
        + Separator + stamp.Count.ToString(CultureInfo.InvariantCulture)
        + Separator + stamp.RecentCount.ToString(CultureInfo.InvariantCulture);

    /// <summary>Reads a <see cref="SerializeDumps"/> form back. False on a null, empty or malformed value, which a caller reads as "no prior stamp".</summary>
    public static bool TryParseDumps(string? text, out MemoryDumpStamp stamp)
    {
        stamp = default;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var parts = text.Split(Separator);
        if (parts.Length != 3
            || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var count)
            || !long.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var recent))
        {
            return false;
        }

        DateTime? newest = null;
        if (parts[0].Length > 0)
        {
            if (!DateTime.TryParseExact(parts[0], "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
            {
                return false;
            }

            newest = parsed;
        }

        stamp = new MemoryDumpStamp(newest, count, recent);
        return true;
    }

    /// <summary>
    /// The memory dump observation for a SQL Server target. <paramref name="current"/> is null when the read failed or the engine has no dump view
    /// (Azure SQL Database, no permission): nothing is compared and nothing is persisted, so a failed pass can neither fire nor erase the stored stamp.
    /// A newest creation time that moved later than the stored one is a rise: the run's note gets <see cref="MemoryDumpsNewMeasurement"/>. The first
    /// observation (no stored stamp) is only recorded. The stamp is staged whenever its newest time or total changed, and ONLY then, so a steady pass
    /// writes no state row and the last-ten-minutes figure ageing out does not refresh the row. Whenever the stamp is staged over a stored one, the
    /// replaced stamp is staged with it under <see cref="MemoryDumpsPreviousStateKey"/>, a rise or not, so the pair always describes the latest
    /// change: after a drop (a file removed, or the list cleared) the pair holds the stamp before the drop and the one after it, which
    /// <see cref="JudgeMemoryDumps"/> never reads as a rise. Returns true on a rise.
    /// </summary>
    public static bool ObserveMemoryDumps(CollectorContext context, MemoryDumpStamp? current)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (current is null)
        {
            return false;
        }

        var now = current.Value;
        context.State.TryGetValue(MemoryDumpsStateKey, out var storedText);
        var hasPrior = TryParseDumps(storedText, out var prior);

        var rose = hasPrior && IsRise(prior, now);
        if (rose)
        {
            context.Measure(MemoryDumpsNewMeasurement, Math.Max(1, now.Count - prior.Count));
        }

        if (!hasPrior || prior.NewestUtc != now.NewestUtc || prior.Count != now.Count)
        {
            if (hasPrior)
            {
                context.PendingState[MemoryDumpsPreviousStateKey] = SerializeDumps(prior);
            }

            context.PendingState[MemoryDumpsStateKey] = SerializeDumps(now);
        }

        return rose;
    }

    private static bool IsRise(MemoryDumpStamp previous, MemoryDumpStamp current) =>
        current.NewestUtc.HasValue && (!previous.NewestUtc.HasValue || current.NewestUtc.Value > previous.NewestUtc.Value);

    private static BaselineDiscontinuities.StateRow? Find(IEnumerable<BaselineDiscontinuities.StateRow> rows, string key)
    {
        foreach (var row in rows)
        {
            if (string.Equals(row.StateKey, key, StringComparison.Ordinal))
            {
                return row;
            }
        }

        return null;
    }

    /// <summary>
    /// The newest restart the persisted identity pairs hold, or null. A carrier's pair is a restart when both start times are known, the current one is
    /// LATER, and the server name did not move (unchanged, or unknown on either side). A name that moved is a failover or a rename and is not
    /// a restart. A carrier with only a current pair (the first observation) holds no previous and never counts. Of several carriers the pair persisted last
    /// wins. The CALLER decides how old is too old, and whether it fired already.
    /// </summary>
    public static RestartObservation? JudgeRestart(IReadOnlyList<BaselineDiscontinuities.StateRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        RestartObservation? best = null;
        foreach (var carrier in rows.GroupBy(r => r.CollectorName, StringComparer.Ordinal))
        {
            var current = Find(carrier, ServerEpoch.IdentityStateKey);
            var previous = Find(carrier, ServerEpoch.IdentityPreviousStateKey);
            if (current is null || previous is null
                || !ServerEpoch.TryParse(current.Value.StateValue, out var now)
                || !ServerEpoch.TryParse(previous.Value.StateValue, out var before)
                || !now.StartTime.HasValue || !before.StartTime.HasValue
                || now.StartTime.Value <= before.StartTime.Value)
            {
                continue;
            }

            var nameMoved = now.Name is not null && before.Name is not null
                && !string.Equals(now.Name, before.Name, StringComparison.OrdinalIgnoreCase);
            if (nameMoved)
            {
                continue;
            }

            var candidate = new RestartObservation(before.StartTime.Value, now.StartTime.Value, current.Value.UpdatedAt);
            if (best is null || candidate.ObservedAtUtc > best.Value.ObservedAtUtc)
            {
                best = candidate;
            }
        }

        return best;
    }

    /// <summary>
    /// The newest rise the persisted memory dump stamps hold, or null: the current stamp has a newest time and the previous stamp (the one the current replaced)
    /// has none or an earlier one. A server with no previous stamp, a stamp that failed to parse, or a current stamp with no dump never counts. The CALLER decides
    /// how old is too old, and whether that newest dump fired already.
    /// </summary>
    public static MemoryDumpObservation? JudgeMemoryDumps(IReadOnlyList<BaselineDiscontinuities.StateRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        MemoryDumpObservation? best = null;
        foreach (var carrier in rows.GroupBy(r => r.CollectorName, StringComparer.Ordinal))
        {
            var current = Find(carrier, MemoryDumpsStateKey);
            var previous = Find(carrier, MemoryDumpsPreviousStateKey);
            if (current is null || previous is null
                || !TryParseDumps(current.Value.StateValue, out var now)
                || !TryParseDumps(previous.Value.StateValue, out var before)
                || !IsRise(before, now))
            {
                continue;
            }

            var candidate = new MemoryDumpObservation(now, before, current.Value.UpdatedAt);
            if (best is null || candidate.ObservedAtUtc > best.Value.ObservedAtUtc)
            {
                best = candidate;
            }
        }

        return best;
    }
}
