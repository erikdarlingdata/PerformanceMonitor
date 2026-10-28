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
/// the answer is what the single wide statement (<see cref="QueryTagRollupSql.QueryStoreSql"/>) returns, while the whole hours cost a
/// narrow table's rows instead of the wide table's, with ONE exception: a row whose <c>first_execution_time</c> is more than 12 hours
/// after the window end (after its range's end, for a routed wide arm, #6847). The wide statement and the routed wide arms bound that
/// column (<c>first_execution_time &lt;= window end + FirstExecUpperSlack</c>, #5523, the arm's range end when that is earlier) to keep
/// their <c>(server_id, first_execution_time)</c> range finite, so they drop such a row from a window it would count in a wider one. The hour and day tables carry no <c>first_execution_time</c>, so they keep the row every time.
/// That takes a monitored server whose clock runs more than 12 hours fast, and the wide statement's own answer for the row already
/// depends on where the window ends, so the bound is deliberately not added to the builds.
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
    /// $2 end of the last whole hour (exclusive), $3 the allowed server ids. <c>late_write</c> (#6865) is true for a pair whose rollup rows are
    /// stale too (<c>built_seq</c> differs from <c>late_seq</c>: a late writer touched the hour), false for a pair stale only for the hash table.
    /// <c>failing</c> (#6865) marks a third kind of row: a NULL <c>server_id</c> with <c>failing</c> true is an hour with a record in
    /// <c>collect.query_store_compose_stamp_failures</c>, an hour the builder does not build in its backlog pass (in back-off or being retried).
    /// </summary>
    internal const string LedgerSql = """
        SELECT h.hour, NULL::integer AS server_id, false AS late_write, false AS failing
        FROM collect.query_store_compose_stamp_hours AS h
        WHERE h.hour >= $1
        AND   h.hour < $2
        UNION ALL
        SELECT b.hour, b.server_id, b.built_seq IS DISTINCT FROM b.late_seq AS late_write, false AS failing
        FROM collect.query_store_compose_stamp_built AS b
        WHERE b.hour >= $1
        AND   b.hour < $2
        AND   b.server_id = ANY($3)
        AND   b.hash_built_seq IS DISTINCT FROM b.late_seq
        UNION ALL
        SELECT f.hour, NULL::integer AS server_id, false AS late_write, true AS failing
        FROM collect.query_store_compose_stamp_failures AS f
        WHERE f.hour >= $1
        AND   f.hour < $2
        """;

    /// <summary>Which source answers what: the (day, server) pairs read from the day table, the (hour, server) pairs read from the hourly table and the (server, from, to) ranges read from the wide table, each range <c>[from, to)</c> with the statement's window-end bound applied on top.
    /// <paramref name="Starts"/> is each server's snapped start (the first instant it reads) and <paramref name="RequestedStart"/> the latest read start before snapping, the start the section's note names.</summary>
    public sealed record Plan(
        List<(DateTime Hour, int ServerId)> TablePairs,
        List<(int ServerId, DateTime From, DateTime To)> WideRanges,
        Dictionary<int, DateTime> Starts,
        DateTime RequestedStart,
        List<(DateTime Day, int ServerId)> DayPairs,
        int BacklogHoursWaiting = 0,
        int BacklogHoursLeftOut = 0)
    {
        /// <summary>Whether any whole day is read from the day table (the section's source and tier then say so).</summary>
        public bool UsesDays => DayPairs.Count > 0;

        /// <summary>
        /// Whether the plan reads any hour or day from the hash tables. False when the route answers only because backlog hours waiting for the builder
        /// were left out (#6865): the wide table's tail served it alone, so the section must not name the hash tables as a source.
        /// </summary>
        public bool ReadsHashTables => TablePairs.Count > 0 || DayPairs.Count > 0;

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
    /// #6865: the span a read answers when whole hours the builder has done wait for the hash table. A BACKLOG hour is a built hour whose stale
    /// pairs are stale only for the hash table (<c>built_seq</c> equals <c>late_seq</c>, <c>hash_built_seq</c> NULL or different), which is every
    /// hour built before V221 until the builder reaches it. Reading such an hour from the wide table costs the wide table's rows for the hour (on a
    /// large store a 7-day read spent minutes of disk on them), so the read does not: it starts after the newest backlog hour. The hours after it
    /// are read exactly as without a backlog (table hours, day rows, the tail, and real late-write stale pairs from the wide table).
    /// Pure. <paramref name="backlogHours"/> are the backlog hours of the ledger. Returns each server's start moved to the end of the newest backlog hour
    /// when that is later than its own start (the starts are the snapped ones), and how many distinct backlog hours lie in the span the read would
    /// otherwise have covered (<c>HoursWaiting</c>), and how many whole hours the read leaves out, from the first whole hour of the REQUESTED window
    /// (<paramref name="windowStart"/>, not the earliest read start: the interval table's floor may already have cut the starts) to where the figures
    /// start (<c>HoursLeftOut</c>: the latest server's start after the move, which is the cut unless a server's own start is already past it,
    /// so the count and the start the notes name are the same instant; it holds the backlog hours plus every other hour before that start, among them
    /// the hours the table's floor cut);
    /// an empty dictionary and 0, 0 when no backlog hour is in the span.
    /// </summary>
    public static (Dictionary<int, DateTime> Starts, int HoursWaiting, int HoursLeftOut) SpanPastBacklog(
        IReadOnlyDictionary<int, DateTime> readStarts, DateTime windowStart, DateTime windowEnd, ISet<DateTime> backlogHours)
    {
        ArgumentNullException.ThrowIfNull(readStarts);
        ArgumentNullException.ThrowIfNull(backlogHours);
        var moved = new Dictionary<int, DateTime>();
        if (readStarts.Count == 0)
        {
            return (moved, 0, 0);
        }

        var wholeEnd = FloorHour(windowEnd);
        var first = CeilHour(readStarts.Values.Min());
        var inSpan = backlogHours.Where(h => h >= first && h < wholeEnd).ToList();
        if (inSpan.Count == 0)
        {
            return (moved, 0, 0);
        }

        var cut = inSpan.Max().Add(Hour);
        foreach (var (serverId, readStart) in readStarts)
        {
            moved[serverId] = CeilHour(readStart) < cut ? cut : readStart;
        }

        /* The count runs to the instant every surface names as the start of the figures: the latest server's snapped start, which is the cut
           unless a server's own start (a server added later) is already past it. */
        var figuresStart = moved.Values.Select(CeilHour).Max();
        return (moved, inSpan.Count, (int)((figuresStart - CeilHour(windowStart)).TotalHours));
    }

    /// <summary>
    /// #6865: the one sentence the MCP section, the Viewer and (as a copy in query-tag-usage.js) the web page give when the read leaves out the first
    /// hours of the window. <paramref name="hoursLeftOut"/> is the size of the gap, counted from the window's first whole hour, and
    /// <paramref name="hoursWaiting"/> the backlog hours inside it that are the reason. The two are different numbers, and the text never gives the second as the first.
    /// </summary>
    public static string BacklogNote(string startText, int hoursLeftOut, int hoursWaiting) =>
        $"The figures start at {startText}, which leaves out the first {hoursLeftOut} {(hoursLeftOut == 1 ? "hour" : "hours")} of the window, because "
        + $"{hoursWaiting} {(hoursWaiting == 1 ? "hour" : "hours")} in that part {(hoursWaiting == 1 ? "is" : "are")} still being built into the hourly Query Store table. "
        + "The full window answers once they are built.";

    /// <summary>
    /// Decides whether the fleet read is routed and, if so, how: null means today's statement (one allowed server, a store below
    /// <see cref="MinSchemaVersion"/> or without the table, or no hour the table can answer yet, for example right after the upgrade before the
    /// builder has rebuilt any hour). Runs on the read's own connection and transaction, so the facts are the snapshot's. That includes the
    /// valid days: a day rebuild deletes an invalid server's day rows, so a validity answer from one snapshot and the read from another
    /// could return no rows for that server (#6800 step 3c). Do not move the day read out of this transaction.
    /// </summary>
    public static async Task<Plan?> TryPlanAsync(
        NpgsqlConnection connection, IReadOnlyDictionary<int, DateTime> allowed, DateTime windowStart, DateTime windowEnd, int readSeconds, CancellationToken cancellationToken)
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
        var lateWriteHours = new HashSet<DateTime>();
        var failingHours = new HashSet<DateTime>();
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
                    if (reader.GetBoolean(3))
                    {
                        failingHours.Add(hour);
                    }
                    else
                    {
                        built.Add(hour);
                    }
                }
                else
                {
                    stale.Add((hour, reader.GetInt32(1)));
                    if (reader.GetBoolean(2))
                    {
                        lateWriteHours.Add(hour);
                    }
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

        /* #6865: a built hour whose pairs are stale only for the hash table (and no pair of it is a late write) is backlog: the read starts after the
           newest one instead of reading it from the wide table. An hour with a failure record is not: the builder's backlog pass skips it (it is in
           back-off, or retried by the normal selection), so it may not build for a day or more, and an hour that stays stale must not pin the cut
           and take every older hour with it. Such an hour is read wide, as before, unless a backlog hour newer than it puts it before the cut. */
        var backlog = stale.Select(p => p.Hour).Where(h => built.Contains(h) && !lateWriteHours.Contains(h) && !failingHours.Contains(h)).ToHashSet();
        var (moved, waiting, leftOut) = SpanPastBacklog(allowed, windowStart, windowEnd, backlog);
        var plan = Split(waiting == 0 ? allowed : moved, windowEnd, built, stale, validDays) with { BacklogHoursWaiting = waiting, BacklogHoursLeftOut = leftOut };
        /* With hours left out the route answers even with no table hour in the span: today's statement would read the whole window wide. */
        return waiting == 0 && plan.TablePairs.Count == 0 && plan.DayPairs.Count == 0 ? null : plan;
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
