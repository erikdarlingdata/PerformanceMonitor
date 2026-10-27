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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6800 part 2, step 2: the route of the fleet Query Store tag roll-up. A fleet read (two or more allowed servers) takes the hours the
/// hourly builder has done from <c>collect.query_store_hash_hourly</c> and everything else from <c>collect.query_store_interval_wide</c>, so
/// the answer stays what the single wide statement (<see cref="QueryTagRollupSql.QueryStoreSql"/>) returns, while the whole hours cost a
/// narrow table's rows instead of the wide table's.
///
/// <para>The source of a (server, hour) is decided in C# from two small reads in the read's own snapshot: the window's built hours and
/// the pairs whose table rows are no longer valid. Each server's read STARTS ON THE HOUR: its read start snapped up to the next whole hour
/// (itself when already on the hour, #6800 part 2, step 3a), so the partial first hour is not read and the section says so
/// (<see cref="HourlyWindowEdges.StartNote"/>). A table hour is a WHOLE hour inside <c>[snapped start, window end]</c> that the builder has
/// done, for a pair whose <c>hash_built_seq</c> equals its <c>late_seq</c>. Everything else is a wide range: every unbuilt hour (the open
/// hour and the newest two or three included), every stale pair and the tail. Wide hours that touch are merged into one range. Step 3c adds the daily table: a server's valid whole UTC days
/// inside its range (<see cref="QueryStoreHashDaily.ValidDaysSql"/>, read on this same snapshot) go to <see cref="Plan.DayPairs"/> first, and
/// only the hours outside a taken day are table hours or wide ranges. This class only decides which instants each source answers.</para>
/// </summary>
public static class QueryTagRollupHashRoute
{
    /// <summary>The store schema version that has <c>collect.query_store_hash_hourly</c> (V221).</summary>
    public const int MinSchemaVersion = 221;

    /// <summary>How the section's source is named when the route read without a day: both tables.</summary>
    public const string RoutedSourceName = "collect.query_store_hash_hourly + collect.query_store_interval_wide";

    /// <summary>How the section's source is named when the route also read whole days from the day table.</summary>
    public const string RoutedSourceNameWithDays = "collect.query_store_hash_daily + " + RoutedSourceName;

    /// <summary>The table's existence, read on the same snapshot as the gate. A store below 221, or a Viewer on an older store, has none and reads today's statement.</summary>
    internal const string ProbeSql = "SELECT to_regclass('collect.query_store_hash_hourly') IS NOT NULL";

    /// <summary>
    /// The window's validity facts in one result: a row with a NULL <c>server_id</c> is an hour the builder has done, a row with a server is a
    /// pair of the allowed servers whose table rows are not valid (<c>hash_built_seq</c> NULL or different from <c>late_seq</c>). $1 first hour,
    /// $2 end of the last whole hour (exclusive), $3 the allowed server ids.
    /// </summary>
    internal const string LedgerSql = """
        SELECT h.hour, NULL::integer AS server_id
        FROM collect.query_store_compose_stamp_hours AS h
        WHERE h.hour >= $1
        AND   h.hour < $2
        UNION ALL
        SELECT b.hour, b.server_id
        FROM collect.query_store_compose_stamp_built AS b
        WHERE b.hour >= $1
        AND   b.hour < $2
        AND   b.server_id = ANY($3)
        AND   b.hash_built_seq IS DISTINCT FROM b.late_seq
        """;

    /// <summary>Which source answers what: the (day, server) pairs read from the day table, the (hour, server) pairs read from the hourly table and the (server, from, to) ranges read from the wide table, each range <c>[from, to)</c> with the statement's window-end bound applied on top.
    /// <paramref name="Starts"/> is each server's snapped start (the first instant it reads) and <paramref name="RequestedStart"/> the latest read start before snapping, the start the section's note names.</summary>
    public sealed record Plan(
        List<(DateTime Hour, int ServerId)> TablePairs,
        List<(int ServerId, DateTime From, DateTime To)> WideRanges,
        Dictionary<int, DateTime> Starts,
        DateTime RequestedStart,
        List<(DateTime Day, int ServerId)> DayPairs)
    {
        /// <summary>Whether any whole day is read from the day table (the section's source and tier then say so).</summary>
        public bool UsesDays => DayPairs.Count > 0;

        /// <summary>The section's start-edge note: null when no server's start moved.</summary>
        public string? EdgesNote => HourlyWindowEdges.StartNote(RequestedStart, CeilHour(RequestedStart));
    }

    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private static DateTime FloorHour(DateTime value) => new(value.Ticks - (value.Ticks % Hour.Ticks), DateTimeKind.Unspecified);

    internal static DateTime CeilHour(DateTime value) => value.Ticks % Hour.Ticks == 0 ? FloorHour(value) : FloorHour(value).Add(Hour);

    private static DateTime FloorDay(DateTime value) => new(value.Ticks - (value.Ticks % TimeSpan.TicksPerDay), DateTimeKind.Unspecified);

    private static DateTime CeilDay(DateTime value) => value.Ticks % TimeSpan.TicksPerDay == 0 ? FloorDay(value) : FloorDay(value).AddDays(1);

    /// <summary>
    /// Splits each server's span <c>[snapped start, window end]</c> into table hours and wide ranges, the snapped start being the read start
    /// rounded up to a whole hour. Pure: no connection. A whole hour is <c>[H, H + 1 h)</c> with <c>H</c> at or after the snapped start and
    /// <c>H + 1 h</c> at or before the window end, so every row the table holds for it passes both of the wide statement's bounds. Each instant
    /// of the span is in exactly one source. The last range runs through the window end's hour (its upper bound is the next hour start, the
    /// statement's own window-end bound cuts it), which also keeps a window end that falls exactly on an hour start: a row stamped at that
    /// instant is in no whole hour. A snapped start past the window end (a read start inside the window's last hour) reads nothing.
    /// <para>Step 3c: <paramref name="validDays"/> are the (day, server) pairs whose day rows may be read. A server takes each valid whole UTC
    /// day <c>[D, D + 1 day)</c> with <c>D</c> at or after its snapped start and <c>D + 1 day</c> at or before the window's whole-hour end, first.
    /// The hours of a taken day are then neither table hours nor wide hours, so each instant is still in exactly one source.</para>
    /// </summary>
    public static Plan Split(
        IReadOnlyDictionary<int, DateTime> readStarts, DateTime windowEnd, ISet<DateTime> builtHours, ISet<(DateTime Hour, int ServerId)> stalePairs,
        ISet<(DateTime Day, int ServerId)>? validDays = null)
    {
        var dayPairs = new List<(DateTime Day, int ServerId)>();
        var tablePairs = new List<(DateTime Hour, int ServerId)>();
        var wideRanges = new List<(int ServerId, DateTime From, DateTime To)>();
        var starts = new Dictionary<int, DateTime>();
        var wholeEnd = FloorHour(windowEnd);
        foreach (var (serverId, readStart) in readStarts.OrderBy(r => r.Key))
        {
            var start = CeilHour(readStart);
            starts[serverId] = start;
            var takenDays = new HashSet<DateTime>();
            if (validDays is { Count: > 0 })
            {
                for (var day = CeilDay(start); day.AddDays(1) <= wholeEnd; day = day.AddDays(1))
                {
                    if (validDays.Contains((day, serverId)))
                    {
                        takenDays.Add(day);
                        dayPairs.Add((day, serverId));
                    }
                }
            }

            DateTime? run = null;
            for (var hour = start; hour < wholeEnd; hour = hour.Add(Hour))
            {
                var inDay = takenDays.Contains(FloorDay(hour));
                if (inDay || (builtHours.Contains(hour) && !stalePairs.Contains((hour, serverId))))
                {
                    if (run is not null && run.Value < hour)
                    {
                        wideRanges.Add((serverId, run.Value, hour));
                    }

                    run = null;
                    if (!inDay)
                    {
                        tablePairs.Add((hour, serverId));
                    }
                }
                else
                {
                    run ??= hour;
                }
            }

            var tail = run ?? (start > wholeEnd ? start : wholeEnd);
            if (tail < wholeEnd.Add(Hour))
            {
                wideRanges.Add((serverId, tail, wholeEnd.Add(Hour)));
            }
        }

        return new Plan(tablePairs, wideRanges, starts, readStarts.Count == 0 ? windowEnd : DateTime.SpecifyKind(readStarts.Values.Max(), DateTimeKind.Unspecified), dayPairs);
    }

    /// <summary>
    /// Decides whether the fleet read is routed and, if so, how: null means today's statement (one allowed server, a store below
    /// <see cref="MinSchemaVersion"/> or without the table, or no hour the table can answer yet, for example right after the upgrade before the
    /// builder has rebuilt any hour). Runs on the read's own connection and transaction, so the facts are the snapshot's. That includes the
    /// valid days: a day rebuild deletes an invalid server's day rows, so a validity answer from one snapshot and the read from another
    /// could return no rows for that server (#6800 step 3c). Do not move the day read out of this transaction.
    /// </summary>
    public static async Task<Plan?> TryPlanAsync(
        NpgsqlConnection connection, IReadOnlyDictionary<int, DateTime> allowed, DateTime windowEnd, int readSeconds, CancellationToken cancellationToken)
    {
        if (allowed.Count < 2 || StorageVersion.SchemaVersion < MinSchemaVersion)
        {
            return null;
        }

        await using (var probe = new NpgsqlCommand(ProbeSql, connection) { CommandTimeout = readSeconds })
        {
            if (!(bool)(await probe.ExecuteScalarAsync(cancellationToken))!)
            {
                return null;
            }
        }

        var first = CeilHour(allowed.Values.Min());
        var wholeEnd = FloorHour(windowEnd);
        var built = new HashSet<DateTime>();
        var stale = new HashSet<(DateTime Hour, int ServerId)>();
        await using (var ledger = new NpgsqlCommand(LedgerSql, connection) { CommandTimeout = readSeconds })
        {
            ledger.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = first });
            ledger.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = wholeEnd });
            ledger.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = allowed.Keys.ToArray() });
            await using var reader = await ledger.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var hour = reader.GetDateTime(0);
                if (reader.IsDBNull(1))
                {
                    built.Add(hour);
                }
                else
                {
                    stale.Add((hour, reader.GetInt32(1)));
                }
            }
        }

        /* A server's valid days are asked for once, over the earliest snapped start. Split keeps, per server, the days inside its own range. */
        HashSet<(DateTime Day, int ServerId)>? validDays = null;
        if (CeilDay(first).AddDays(1) <= wholeEnd)
        {
            var days = await QueryStoreHashDaily.ReadValidDaysAsync(connection, allowed.Keys.ToArray(), first, wholeEnd, cancellationToken, readSeconds);
            validDays = days.ToHashSet();
        }

        var plan = Split(allowed, windowEnd, built, stale, validDays);
        return plan.TablePairs.Count == 0 && plan.DayPairs.Count == 0 ? null : plan;
    }

    /// <summary>
    /// The routed statement's command (<see cref="QueryTagRollupSql.QueryStoreRoutedSqlFor"/>): the table pairs as parallel arrays, the
    /// window end, the day pairs when the plan has any (two more arrays, $4 and $5), and one (servers, from, to) triple per DISTINCT wide range, each range with the servers that share it. The statement has
    /// one arm for each, so the planner sees every range's own bounds (#6800 part 2, step 2b).
    /// </summary>
    public static NpgsqlCommand Command(NpgsqlConnection connection, Plan plan, DateTime windowEnd, int readSeconds)
    {
        var groups = plan.WideRanges
            .GroupBy(r => (r.From, r.To))
            .OrderBy(g => g.Key.From)
            .ThenBy(g => g.Key.To)
            .Select(g => (g.Key.From, g.Key.To, Servers: g.Select(r => r.ServerId).OrderBy(s => s).ToArray()))
            .ToList();
        var command = new NpgsqlCommand(QueryTagRollupSql.QueryStoreRoutedSqlFor(groups.Count, plan.UsesDays), connection) { CommandTimeout = readSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = plan.TablePairs.Select(p => p.Hour).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = plan.TablePairs.Select(p => p.ServerId).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified) });
        if (plan.UsesDays)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = plan.DayPairs.Select(p => p.Day).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = plan.DayPairs.Select(p => p.ServerId).ToArray() });
        }

        foreach (var group in groups)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = group.Servers });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(group.From, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(group.To, DateTimeKind.Unspecified) });
        }

        return command;
    }
}
