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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800 part 2, step 2: the fleet Query Store tag roll-up reads built hours from <c>collect.query_store_hash_hourly</c> and the rest from
/// the wide table (<see cref="QueryTagRollupHashRoute"/>), against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). The answer must equal
/// the single wide statement's (<see cref="QueryTagRollupSql.QueryStoreSql"/>) value for value over the window the route actually read, which
/// starts on the hour (the read start rounded up, #6800 part 2, step 3a), for a seed whose first executions are all within 12 hours of the
/// window end: a row first executed later than that is dropped by the wide statement's <c>first_execution_time</c> bound and kept by the
/// tables, which is the one difference (see <see cref="QueryTagRollupHashRoute"/>). The seed is the shared 30-hour one (three
/// servers, 28 hours built, the newest two hours unbuilt), read through a window with an unaligned start, with the three measures the
/// stamp does not carry added, three tags mapped on two servers, and a NULL-hash group on every server.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryTagRollupHashRouteLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the V221 hash-hourly route facts.";

    private static readonly int[] Servers = [1, 2, 3];

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) => ComposeStampLiveSupport.ExecAsync(connection, sql, ct);

    private static string At(DateTime value) => ComposeStampLiveSupport.At(value);

    private sealed record Arranged(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime HourNow)
    {
        /// <summary>An unaligned window start: 17 minutes into the first hour, so the partial first hour is a wide range.</summary>
        public DateTime Start => HourNow.AddHours(-24).AddMinutes(17);

        /// <summary>The window end, past the coverage claim's applied_through (30 minutes into the open hour), as the gate wants.</summary>
        public DateTime End => HourNow.AddMinutes(40);
    }

    /// <summary>
    /// The shared seed anchored at <see cref="LiveClock.AnchoredEnd"/> (#5608), measures added, tags mapped, hours built, and a table-floor row
    /// 72 hours back per server (a plain store's gate wants the table's own floor a purge margin below the window).
    /// </summary>
    private static async Task<Arranged> ArrangeAsync(CancellationToken ct, bool collectionTimeOrder = false)
    {
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false, at: LiveClock.AnchoredEnd());
        await ExecAsync(connection, @"
UPDATE collect.query_store_interval_wide
SET avg_logical_io_reads = CASE WHEN query_id % 4 = 0 THEN NULL ELSE ((query_id * 7 + plan_id) % 1000) + 1 END,
    avg_logical_io_writes = CASE WHEN query_id % 3 = 0 THEN NULL ELSE query_id * 5 + (plan_id % 7) END,
    max_query_max_used_memory = CASE WHEN query_id % 5 = 0 THEN NULL ELSE (runtime_stats_interval_id % 5000) + 1 END", ct);
        await ExecAsync(connection, @"
WITH t AS (INSERT INTO config.query_tags (name) VALUES ('tag-a'), ('tag-b') RETURNING id, name)
INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
SELECT m.server_id, 'hash', m.key, t.id, 'hand_hash'
FROM t
JOIN (VALUES (1, 'hash1', 'tag-a'), (1, 'hash2', 'tag-b'), (2, 'hash1', 'tag-a'), (2, 'hash3', 'tag-b')) AS m (server_id, key, name) ON m.name = t.name", ct);
        if (collectionTimeOrder)
        {
            /* The store appends rows in the order they are collected, so its heap runs in collection_time order, which is what the BRIN
               index on collection_time is for. The seed is inserted in whatever order its joins produce, which would make that index useless
               in a test that reads a plan. A btree on the column lets CLUSTER lay the heap out in order, and goes again so the plan has
               the indexes the store has. */
            await ExecAsync(connection, "CREATE INDEX ix_zz_cluster_collection_time ON collect.query_store_interval_wide (collection_time)", ct);
            await ExecAsync(connection, "CLUSTER collect.query_store_interval_wide USING ix_zz_cluster_collection_time", ct);
            await ExecAsync(connection, "DROP INDEX collect.ix_zz_cluster_collection_time", ct);
        }

        Assert.Equal(28, await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)));
        foreach (var server in Servers)
        {
            await ComposeStampLiveSupport.InsertRowAsync(connection, server, hourNow.AddHours(-72), 900_000 + server, ct);
        }

        return new Arranged(scratch, connection, source, hourNow);
    }

    private static Task CleanupAsync(Arranged a, bool bodySucceeded) => ComposeStampLiveSupport.CleanupAsync(a.Scratch, a.Connection, a.Source, bodySucceeded);

    private static async Task<DarlingQueryTagRollupReader.QueryStoreRead> FleetAsync(Arranged a, int[] servers, DateTime start, DateTime end, CancellationToken ct)
    {
        var read = await DarlingQueryTagRollupReader.ReadQueryStoreAsync(a.Source, servers, start, end, null, ct);
        Assert.True(servers.Length == read.Allowed.Count, "the gate refused the window " + start.ToString("o", CultureInfo.InvariantCulture) + " to " + end.ToString("o", CultureInfo.InvariantCulture) + ": " + string.Join("; ", read.Refused.Values.Distinct()));
        return read;
    }

    /// <summary>The starts a read actually began at: the rounded-up starts when the route read, the gate's own when today's statement did (#6800 part 2, step 3a).</summary>
    private static Dictionary<int, DateTime> Snapped(DarlingQueryTagRollupReader.QueryStoreRead read) => read.RoutedStarts ?? read.Allowed;

    /// <summary>The single wide statement, the way the reader builds it, for the starts the gate gave.</summary>
    private static async Task<List<QueryTagRollupFact>> TodaysAsync(Arranged a, Dictionary<int, DateTime> allowed, DateTime end, CancellationToken ct)
    {
        var ordered = allowed.OrderBy(x => x.Key).ToArray();
        await using var command = new NpgsqlCommand(QueryTagRollupSql.QueryStoreSql, a.Connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ordered.Select(x => x.Key).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = ordered.Select(x => DateTime.SpecifyKind(x.Value, DateTimeKind.Unspecified)).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(ordered.Min(x => x.Value), DateTimeKind.Unspecified) });
        var facts = new List<QueryTagRollupFact>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var ordinals = QueryTagRollupSection.QueryStore.Measures.Select(m => reader.GetOrdinal(m.Name)).ToArray();
        while (await reader.ReadAsync(ct))
        {
            var values = ordinals.Select(o => reader.IsDBNull(o) ? 0 : Convert.ToDouble(reader.GetValue(o), CultureInfo.InvariantCulture)).ToArray();
            facts.Add(new QueryTagRollupFact(reader.GetInt32(reader.GetOrdinal("server_id")), reader.IsDBNull(reader.GetOrdinal("tag_id")) ? null : reader.GetInt32(reader.GetOrdinal("tag_id")), values));
        }

        return facts;
    }

    /// <summary>The facts as text, sorted, every value in the round-trip format: equal text is equal value for value.</summary>
    private static string Canon(IEnumerable<QueryTagRollupFact> facts) => string.Join(
        "\n", facts.OrderBy(f => f.ServerId).ThenBy(f => f.TagId ?? int.MinValue)
            .Select(f => f.ServerId.ToString(CultureInfo.InvariantCulture) + "|" + (f.TagId?.ToString(CultureInfo.InvariantCulture) ?? "-") + "|"
                + string.Join(",", f.Values.Select(v => v.ToString("R", CultureInfo.InvariantCulture)))));

    [Fact]
    public async Task TheFleetRead_AnswersBuiltHoursFromTheTable_ADeletedWideHourDoesNotMoveIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var before = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(before.UsedHashHourly, "a three-server read over built hours must take the route");
            Assert.All(before.Allowed.Values, v => Assert.Equal(a.Start, v));
            Assert.All(before.RoutedStarts!.Values, v => Assert.Equal(a.HourNow.AddHours(-23), v));
            var beforeText = Canon(before.Facts);
            Assert.Equal(Canon(await TodaysAsync(a, Snapped(before), a.End, ct)), beforeText);
            Assert.Contains("|-|", beforeText.Replace("\n", "||"));

            /* There is no DELETE trigger on the wide table, so the pairs of the hour stay valid and the table still holds its rows. */
            var hour = a.HourNow.AddHours(-10);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddHours(1))}' AND collection_time > TIMESTAMP '{At(a.HourNow.AddHours(-72).AddMinutes(1))}'", ct);

            var after = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(after.UsedHashHourly);
            Assert.NotEqual(beforeText, Canon(await TodaysAsync(a, Snapped(after), a.End, ct)));
            Assert.Equal(beforeText, Canon(after.Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheFleetRead_EqualsTheWideStatementValueForValue_AcrossMovedIntervalsAnUnalignedStartTheTailANullHashAndStalePairs()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Intervals that move between hours after the build (each move makes two pairs stale), one late row into a built hour of a mapped
               hash, and rows at the window end's own instant and just past it (the wide statement keeps the first, drops the second). */
            var from = a.HourNow.AddHours(-12);
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time + interval '1 hour', avg_logical_io_reads = 4000
WHERE server_id = 2 AND query_id IN (1, 2) AND collection_time >= TIMESTAMP '{At(from)}' AND collection_time < TIMESTAMP '{At(from.AddHours(1))}'", ct);
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time - interval '1 hour'
WHERE server_id = 1 AND query_id = 3 AND plan_id = 31 AND collection_time >= TIMESTAMP '{At(a.HourNow.AddHours(-20))}' AND collection_time < TIMESTAMP '{At(a.HourNow.AddHours(-20).AddMinutes(30))}'", ct);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 1, a.HourNow.AddHours(-6).AddMinutes(7), 700_001, ct);
            await ExecAsync(a.Connection, "UPDATE collect.query_store_interval_wide SET query_hash = 'hash1', avg_cpu_time_us = 123456789 WHERE query_hash = 'hashX' AND server_id = 1", ct);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, a.HourNow, 700_002, ct);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, a.HourNow.AddMinutes(10), 700_003, ct);
            var stale = await ComposeStampLiveSupport.CountAsync(a.Connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hash_built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT hour FROM collect.query_store_compose_stamp_hours)", ct);
            Assert.True(stale >= 5, "the moves and the late row must leave stale pairs in built hours, got " + stale);

            /* Each window: the unaligned start and the end 40 minutes into the open hour, an aligned start with the end exactly on an hour start
               (a row is stamped at that instant), and a 24-hour window that ends mid-hour 5 hours back. */
            foreach (var (start, end) in new[] { (a.Start, a.End), (a.HourNow.AddHours(-24), a.HourNow), (a.HourNow.AddHours(-29).AddMinutes(-13), a.HourNow.AddHours(-4).AddMinutes(-33)) })
            {
                /* The gate refuses a window that ends before the claim's applied_through, so the claim is moved back for the two earlier windows. */
                if (end < a.End)
                {
                    await ExecAsync(a.Connection, $"UPDATE collect.query_store_interval_wide_coverage SET applied_through = TIMESTAMP '{At(a.HourNow.AddHours(-6))}'", ct);
                }

                var read = await FleetAsync(a, Servers, start, end, ct);
                Assert.True(read.UsedHashHourly, "start " + start + " end " + end);
                Assert.NotEmpty(read.Facts);
                Assert.Equal(Canon(await TodaysAsync(a, Snapped(read), end, ct)), Canon(read.Facts));
                Assert.All(Snapped(read).Values, v => Assert.Equal(start.Ticks % TimeSpan.TicksPerHour == 0 ? start : new DateTime(start.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour).AddHours(1), v));
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6847: each routed wide arm bounds <c>first_execution_time</c> by its OWN range's end (or the window end when that is earlier) plus
    /// the 12-hour slack, not by the window end. A row collected inside a range holds a first execution at most the slack after the
    /// range's end, so every row the slack was written to keep is kept: one a minute short of the range end plus the slack in a range in
    /// the middle of the window, and one a minute short of the window end plus the slack in the tail, whose range ends an hour past the
    /// window end. Both answer as the single wide statement does. A tail row a minute past the window end plus the slack is dropped by
    /// both (the tail range's end is later than the window's, so the bound takes the window end). A middle row a minute past ITS range's
    /// end plus the slack is the one difference: it was collected before the range ended and executed more than 12 hours after it, which is
    /// the clock-skew case the slack already leaves out, and the statement for the whole window would still count it.
    /// </summary>
    [Fact]
    public async Task TheWideArms_BoundFirstExecutionByTheirOwnRangeEnd_KeepingEveryRowTheSlackIsWrittenFor()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var slack = QueryStoreIntervalWide.FirstExecUpperSlack;
            async Task InsertAsync(int serverId, DateTime collected, DateTime firstExecution, long interval) => await ExecAsync(a.Connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{At(collected)}', {serverId}, 'dbX', 98, 98, 'Regular', TIMESTAMP '{At(firstExecution)}', TIMESTAMP '{At(collected)}',
        'modX', 'hashE', 7, 1000, 500, 2000, 900, {interval})", ct);

            /* Server 2's rows of the hour 12 hours back move into the next hour: that leaves the two hours' pairs stale, so they are wide
               ranges in the middle of the window, [hour - 12 h, hour - 10 h). The tail is [hour - 2 h, hour + 1 h) for every server. */
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time + interval '1 hour'
WHERE server_id = 2 AND query_id IN (1, 2) AND collection_time >= TIMESTAMP '{At(a.HourNow.AddHours(-12))}' AND collection_time < TIMESTAMP '{At(a.HourNow.AddHours(-11))}'", ct);
            var middleEnd = a.HourNow.AddHours(-10);
            var windowEnd = a.End;
            await InsertAsync(2, a.HourNow.AddHours(-11).AddMinutes(10), middleEnd + slack - TimeSpan.FromMinutes(1), 710_001);
            await InsertAsync(3, a.HourNow.AddMinutes(20), windowEnd + slack - TimeSpan.FromMinutes(1), 710_002);
            await InsertAsync(3, a.HourNow.AddMinutes(25), windowEnd + slack + TimeSpan.FromMinutes(1), 710_003);

            var executionsAt = QueryTagRollupSection.QueryStore.Measures.Select(m => m.Name).ToList().IndexOf("executions");
            Assert.True(executionsAt >= 0);
            async Task<(string Routed, string Wide, double RoutedExecutions, double WideExecutions)> ReadBothAsync()
            {
                var read = await FleetAsync(a, Servers, a.Start, windowEnd, ct);
                Assert.True(read.UsedHashHourly);
                var wide = await TodaysAsync(a, Snapped(read), windowEnd, ct);
                return (Canon(read.Facts), Canon(wide), read.Facts.Sum(f => f.Values[executionsAt]), wide.Sum(f => f.Values[executionsAt]));
            }

            var kept = await ReadBothAsync();
            Assert.Equal(kept.Wide, kept.Routed);
            /* The two rows a minute short of their bounds are in the answer (seven executions each), the tail row past it is not. */
            await ExecAsync(a.Connection, "DELETE FROM collect.query_store_interval_wide WHERE runtime_stats_interval_id IN (710001, 710002)", ct);
            var without = await ReadBothAsync();
            Assert.Equal(14, kept.RoutedExecutions - without.RoutedExecutions);

            await InsertAsync(2, a.HourNow.AddHours(-11).AddMinutes(10), middleEnd + slack + TimeSpan.FromMinutes(1), 710_004);
            var over = await ReadBothAsync();
            Assert.Equal(7, over.WideExecutions - over.RoutedExecutions);
            Assert.Equal(without.RoutedExecutions, over.RoutedExecutions);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    [Fact]
    public async Task ALateWriteToABuiltHour_ShowsInTheFleetReadAtOnce_AndStaysCorrectAfterTheNextBuild()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var before = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(before.UsedHashHourly);
            var beforeText = Canon(before.Facts);

            var hour = a.HourNow.AddHours(-8);
            await ExecAsync(a.Connection, $"UPDATE collect.query_store_interval_wide SET avg_cpu_time_us = 999999999 WHERE server_id = 1 AND query_id = 2 AND plan_id = 21 AND collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddMinutes(10))}'", ct);
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(a.Connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{At(hour)}' AND server_id = 1 AND hash_built_seq IS DISTINCT FROM late_seq", ct));

            var during = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(during.UsedHashHourly);
            Assert.NotEqual(beforeText, Canon(during.Facts));
            Assert.Equal(Canon(await TodaysAsync(a, Snapped(during), a.End, ct)), Canon(during.Facts));

            Assert.Equal(1, await ComposeStampLiveSupport.BuildAllAsync(a.Source, ct, ComposeStampLiveSupport.ClockWithin(a.HourNow)));
            var rebuilt = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.Equal(Canon(during.Facts), Canon(rebuilt.Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    [Fact]
    public async Task OneServer_AStoreWithoutTheTable_AndAStoreWhereNoHourIsValid_KeepTheWideStatement()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var one = await FleetAsync(a, [1], a.Start, a.End, ct);
            Assert.False(one.UsedHashHourly, "one allowed server reads today's statement");
            Assert.Equal(Canon(await TodaysAsync(a, one.Allowed, a.End, ct)), Canon(one.Facts));
            Assert.Null(one.WindowEdgesNote);

            var fleet = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(fleet.UsedHashHourly);
            Assert.NotNull(fleet.RoutedStarts);
            var fleetText = Canon(fleet.Facts);
            /* The fallback reads from the exact start, the route from the next hour: the fallback is today's statement over the exact start, with no note. */
            var exactText = Canon(await TodaysAsync(a, fleet.Allowed, a.End, ct));
            Assert.NotEqual(fleetText, exactText);

            /* Right after the upgrade every built hour's pair has hash_built_seq NULL: no table hour, so no route. */
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            var stale = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.False(stale.UsedHashHourly);
            Assert.Null(stale.RoutedStarts);
            Assert.Null(stale.WindowEdgesNote);
            Assert.Equal(exactText, Canon(stale.Facts));

            /* A store below 221 has no table. */
            await ExecAsync(a.Connection, "DROP TABLE collect.query_store_hash_hourly", ct);
            var below = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.False(below.UsedHashHourly);
            Assert.Null(below.WindowEdgesNote);
            Assert.Equal(exactText, Canon(below.Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6800 part 2, step 3a: the routed read starts on the hour and says so. An unaligned start gives the hourly tier's start sentence (the
    /// data from the start to the next hour is not included), an aligned start gives no note, and the fallback (one server, no valid hour, no
    /// table) gives none either and keeps the exact start. Through the tool, the Query Store section carries <c>tier</c>
    /// <c>hourly+tail</c>, the snapped <c>effective_start</c> and the note, and a fallback section carries none of the three.
    /// </summary>
    [Fact]
    public async Task TheRoutedRead_StartsOnTheHourAndSaysSo_AnAlignedStartAndTheFallbackSayNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var snappedStart = a.HourNow.AddHours(-23);
            var expectedNote = HourlyWindowEdges.StartNote(a.Start, snappedStart);
            Assert.StartsWith("hourly buckets start on the hour: no bucket before ", expectedNote, StringComparison.Ordinal);
            Assert.Contains("is not included", expectedNote, StringComparison.Ordinal);

            var unaligned = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(unaligned.UsedHashHourly);
            Assert.Equal(expectedNote, unaligned.WindowEdgesNote);

            var aligned = await FleetAsync(a, Servers, a.HourNow.AddHours(-24), a.End, ct);
            Assert.True(aligned.UsedHashHourly);
            Assert.Null(aligned.WindowEdgesNote);
            Assert.All(aligned.RoutedStarts!.Values, v => Assert.Equal(a.HourNow.AddHours(-24), v));

            /* The tool's window is 24 hours back from as_of, which is a.End, so its start is 40 minutes past the hour. */
            var toolStart = a.End.AddHours(-24);
            var section = QueryStoreSection(await ToolAsync(a, ct));
            Assert.Equal("hourly+tail", section.GetProperty("tier").GetString());
            Assert.Equal(HourlyWindowEdges.StartNote(toolStart, snappedStart), section.GetProperty("window_edges_note").GetString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(snappedStart), section.GetProperty("effective_start").GetString());

            /* The Viewer's section reads through the same reader and shows the same sentence in its note. */
            await using var viewer = new ViewerDataService(a.Scratch.ConnectionString);
            var viewed = await viewer.GetQueryTagRollupDataAsync(Servers, toolStart, a.End, ct);
            Assert.Contains(HourlyWindowEdges.StartNote(toolStart, snappedStart)!, viewed.Notes[QueryTagRollupSection.QueryStore.Name], StringComparison.Ordinal);

            /* The fallback: today's statement, the exact start, no note and no tier. */
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            Assert.False((await viewer.GetQueryTagRollupDataAsync(Servers, toolStart, a.End, ct)).Notes.ContainsKey(QueryTagRollupSection.QueryStore.Name));
            var fallback = QueryStoreSection(await ToolAsync(a, ct));
            Assert.False(fallback.TryGetProperty("tier", out _));
            Assert.False(fallback.TryGetProperty("window_edges_note", out _));
            Assert.Equal(McpHelpers.FormatEffectiveStart(toolStart), fallback.GetProperty("effective_start").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    private static async Task<JsonDocument> ToolAsync(Arranged a, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(
            a.Source, null, 24, null, 25, a.End.ToString("o", CultureInfo.InvariantCulture) + "Z", null, ct));

    private static JsonElement QueryStoreSection(JsonDocument doc) =>
        doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == QueryTagRollupSection.QueryStore.Name);

    /// <summary>
    /// The plan of the routed statement, found by what its nodes do: the table arm reaches <c>query_store_hash_hourly</c> only through an
    /// index scan on <c>(hour, server_id)</c>, never a sequential scan of the table. Sequential scans are turned off for the read (the seed
    /// is a few thousand rows, where any plan scans), so the plan shows the paths the statement allows. The seed and the read end at
    /// <see cref="LiveClock.AnchoredEnd"/>. The wide arm's plan is not asserted: on a small seed the planner's choice between the
    /// <c>(server_id, first_execution_time)</c> index and the <c>collection_time</c> index varies from run to run (PR notes).
    /// </summary>
    [Fact]
    public async Task TheRoutedStatementsPlan_ProbesTheTablesHourServerIndexForTheListedPairs()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(a.Connection, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_server_first_exec ON collect.query_store_interval_wide (server_id, first_execution_time)", ct);
            await ExecAsync(a.Connection, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_collection_time_brin ON collect.query_store_interval_wide USING brin (collection_time) WITH (pages_per_range = 4)", ct);
            await ExecAsync(a.Connection, "ANALYZE collect.query_store_interval_wide; ANALYZE collect.query_store_hash_hourly", ct);

            var read = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(read.UsedHashHourly);
            await using var transaction = await a.Connection.BeginTransactionAsync(ct);
            await ExecAsync(a.Connection, "SET LOCAL enable_seqscan = off", ct);
            var window = new[] { (1, a.Start), (2, a.Start), (3, a.Start) }.ToDictionary(x => x.Item1, x => x.Item2);
            var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, window, a.End, 120, ct);
            Assert.NotNull(plan);
            await using var explain = QueryTagRollupHashRoute.Command(a.Connection, plan, a.End, 120);
            explain.Transaction = transaction;
            explain.CommandText = "EXPLAIN (ANALYZE, FORMAT JSON) " + explain.CommandText;
            var json = Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
            using var document = JsonDocument.Parse(json);
            var nodes = new List<JsonElement>();
            void Walk(JsonElement node)
            {
                nodes.Add(node);
                if (node.TryGetProperty("Plans", out var children))
                {
                    foreach (var child in children.EnumerateArray())
                    {
                        Walk(child);
                    }
                }
            }

            Walk(document.RootElement[0].GetProperty("Plan"));
            string Text(JsonElement n, string name) => n.TryGetProperty(name, out var v) ? v.GetString() ?? string.Empty : string.Empty;

            var tableNodes = nodes.Where(n => Text(n, "Relation Name") == "query_store_hash_hourly").ToList();
            Assert.NotEmpty(tableNodes);
            Assert.All(tableNodes, n => Assert.Contains("Index", Text(n, "Node Type"), StringComparison.Ordinal));
            Assert.Contains(nodes, n => Text(n, "Index Name") == "ix_query_store_hash_hourly_hour_server");

            await transaction.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6800 part 2, step 2b: the wide side of the routed statement. A window of 24 hours whose wide ranges are the partial first hour and
    /// the unbuilt tail (about 2 of the 24 hours) must scan about those rows of <c>collect.query_store_interval_wide</c>, not the window's.
    /// Found by what the nodes do: every scan of the wide table or one of its partitions, that is, the nodes that read rows from the table.
    /// A Bitmap Index Scan is not counted (#6847): it puts index entries into a bitmap and reads no table row, and when PostgreSQL ANDs two
    /// bitmaps (<c>collection_time</c> with <c>(server_id, first_execution_time)</c>, which it does on some versions and some statistics)
    /// the entries it returns are not rows read. Per node the rows it examined are Actual Rows plus Rows Removed by Filter plus Rows Removed
    /// by Index Recheck, times Actual Loops, so a plan that reads the whole window from the table shows it however it gets there. The heap is laid out in collection_time order, as the store's is, so the BRIN
    /// index on collection_time can do what it is for, and sequential scans are turned off (the seed is a few thousand rows, where any
    /// plan scans), so the plan shows the paths the statement allows.
    /// </summary>
    [Fact]
    public async Task TheRoutedStatementsWideArms_ExamineAboutTheirRangesRows_NotTheWindows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct, collectionTimeOrder: true);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(a.Connection, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_server_first_exec ON collect.query_store_interval_wide (server_id, first_execution_time)", ct);
            await ExecAsync(a.Connection, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_collection_time_brin ON collect.query_store_interval_wide USING brin (collection_time) WITH (pages_per_range = 4)", ct);
            await ExecAsync(a.Connection, "ANALYZE collect.query_store_interval_wide; ANALYZE collect.query_store_hash_hourly", ct);

            var windowRows = await ComposeStampLiveSupport.CountAsync(
                a.Connection, $"SELECT count(*) FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(a.Start)}' AND collection_time <= TIMESTAMP '{At(a.End)}'", ct);
            Assert.True(windowRows > 10_000, "the window must hold many wide rows, got " + windowRows);

            var window = Servers.ToDictionary(s => s, _ => a.Start);
            await using var transaction = await a.Connection.BeginTransactionAsync(ct);
            await ExecAsync(a.Connection, "SET LOCAL enable_seqscan = off", ct);
            var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, window, a.End, 120, ct);
            Assert.NotNull(plan);
            var wideHours = plan.WideRanges.Select(r => (r.To - r.From).TotalHours).Max();
            Assert.InRange(wideHours, 0.5, 3.5);
            await using var explain = QueryTagRollupHashRoute.Command(a.Connection, plan, a.End, 120);
            explain.Transaction = transaction;
            explain.CommandText = "EXPLAIN (ANALYZE, FORMAT JSON) " + explain.CommandText;
            var json = Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
            using var document = JsonDocument.Parse(json);
            var nodes = new List<JsonElement>();
            void Walk(JsonElement node)
            {
                nodes.Add(node);
                if (node.TryGetProperty("Plans", out var children))
                {
                    foreach (var child in children.EnumerateArray())
                    {
                        Walk(child);
                    }
                }
            }

            Walk(document.RootElement[0].GetProperty("Plan"));
            string Text(JsonElement n, string name) => n.TryGetProperty(name, out var v) ? v.GetString() ?? string.Empty : string.Empty;
            double Num(JsonElement n, string name) => n.TryGetProperty(name, out var v) ? v.GetDouble() : 0;

            var wideScans = nodes.Where(n =>
                Text(n, "Relation Name").StartsWith("query_store_interval_wide", StringComparison.Ordinal)
                && Text(n, "Node Type") != "Bitmap Index Scan").ToList();
            Assert.NotEmpty(wideScans);
            var examined = wideScans.Sum(n => (Num(n, "Actual Rows") + Num(n, "Rows Removed by Filter") + Num(n, "Rows Removed by Index Recheck")) * Num(n, "Actual Loops"));
            Assert.True(examined > 0, "the wide arms must have read rows");
            Assert.True(
                examined < windowRows / 2.0,
                "the wide arms examined " + examined.ToString("0", CultureInfo.InvariantCulture) + " rows of a window that holds " + windowRows + "; nodes: "
                + string.Join(" | ", wideScans.Select(n => Text(n, "Node Type") + " " + Text(n, "Relation Name") + Text(n, "Index Name") + " rows " + Num(n, "Actual Rows") + " loops " + Num(n, "Actual Loops"))));

            await transaction.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// The batched Query Store commands bind their servers and each server's own start (or stale pairs) as parallel arrays, and nothing
    /// else would show a mis-pairing: when every server resolves to the same start, reversing one array changes no figure. Here the three
    /// servers read from three different starts and each has stale hours of its own. Both batched commands are reached through the
    /// reader's read after the gate (<see cref="DarlingQueryTagRollupReader.ReadAllowedAsync"/>, which <c>ReadQueryStoreAsync</c> calls),
    /// the routed one and today's (reached by taking every built hour away), and each server's figures must equal that same server's
    /// one-server read from the same start over the same window.
    /// </summary>
    [Fact]
    public async Task TheBatchedCommands_PairEachServerWithItsOwnStartAndItsOwnStalePairs()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Stale pairs of each server's own: server 1 moves an interval between hours 20 and 21 back, server 2 two intervals at hour 12
               (and changes a measure), server 3 gets one late row into hour 6 back. Every stale hour is after the server's own start. */
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time - interval '1 hour', avg_cpu_time_us = 77777777
WHERE server_id = 1 AND query_id = 3 AND plan_id = 31 AND collection_time >= TIMESTAMP '{At(a.HourNow.AddHours(-20))}' AND collection_time < TIMESTAMP '{At(a.HourNow.AddHours(-20).AddMinutes(30))}'", ct);
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time + interval '1 hour', avg_logical_io_reads = 4000
WHERE server_id = 2 AND query_id IN (1, 2) AND collection_time >= TIMESTAMP '{At(a.HourNow.AddHours(-12))}' AND collection_time < TIMESTAMP '{At(a.HourNow.AddHours(-11))}'", ct);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, a.HourNow.AddHours(-6).AddMinutes(7), 700_001, ct);

            var starts = new Dictionary<int, DateTime>
            {
                [1] = a.Start,
                [2] = a.HourNow.AddHours(-16).AddMinutes(41),
                [3] = a.HourNow.AddHours(-9).AddMinutes(13),
            };

            await AssertEachServerEqualsItsOwnReadAsync(a, starts, expectRouted: true, ct);

            /* No built hour left: the same three servers go through today's batched statement. */
            var routed = await FleetFromAsync(a, starts, ct);
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await AssertEachServerEqualsItsOwnReadAsync(a, starts, expectRouted: false, ct);
            var snappedStarts = starts.ToDictionary(s => s.Key, s => QueryTagRollupHashRoute.CeilHour(s.Value));
            Assert.Equal(Canon(routed.Facts), Canon((await FleetFromAsync(a, snappedStarts, ct)).Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>The reader's read after the gate, on its own read-only snapshot, for the servers and starts given.</summary>
    private static async Task<DarlingQueryTagRollupReader.QueryStoreRead> FleetFromAsync(Arranged a, Dictionary<int, DateTime> starts, CancellationToken ct)
    {
        await using var transaction = await a.Connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        var read = await DarlingQueryTagRollupReader.ReadAllowedAsync(a.Connection, new Dictionary<int, DateTime>(starts), [], a.End, 120, ct);
        await transaction.CommitAsync(ct);
        return read;
    }

    private static async Task AssertEachServerEqualsItsOwnReadAsync(Arranged a, Dictionary<int, DateTime> starts, bool expectRouted, CancellationToken ct)
    {
        var fleet = await FleetFromAsync(a, starts, ct);
        Assert.Equal(expectRouted, fleet.UsedHashHourly);
        Assert.Equal(3, starts.Values.Distinct().Count());
        foreach (var (server, start) in starts)
        {
            /* The routed fleet read starts each server on the hour, so the one-server read it is compared with starts there too. */
            var own = await FleetFromAsync(a, new Dictionary<int, DateTime> { [server] = expectRouted ? QueryTagRollupHashRoute.CeilHour(start) : start }, ct);
            Assert.False(own.UsedHashHourly, "a one-server read uses today's statement");
            Assert.NotEmpty(own.Facts);
            Assert.Equal(Canon(own.Facts), Canon(fleet.Facts.Where(f => f.ServerId == server)));
        }
    }
}
