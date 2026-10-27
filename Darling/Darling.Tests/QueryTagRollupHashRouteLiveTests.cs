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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800 part 2, step 2: the fleet Query Store tag roll-up reads built hours from <c>collect.query_store_hash_hourly</c> and the rest from
/// the wide table (<see cref="QueryTagRollupHashRoute"/>), against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). The answer must equal
/// the single wide statement's (<see cref="QueryTagRollupSql.QueryStoreSql"/>) value for value. The seed is the shared 30-hour one (three
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
    private static async Task<Arranged> ArrangeAsync(CancellationToken ct)
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
            var beforeText = Canon(before.Facts);
            Assert.Equal(Canon(await TodaysAsync(a, before.Allowed, a.End, ct)), beforeText);
            Assert.Contains("|-|", beforeText.Replace("\n", "||"));

            /* There is no DELETE trigger on the wide table, so the pairs of the hour stay valid and the table still holds its rows. */
            var hour = a.HourNow.AddHours(-10);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddHours(1))}' AND collection_time > TIMESTAMP '{At(a.HourNow.AddHours(-72).AddMinutes(1))}'", ct);

            var after = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(after.UsedHashHourly);
            Assert.NotEqual(beforeText, Canon(await TodaysAsync(a, after.Allowed, a.End, ct)));
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
                Assert.Equal(Canon(await TodaysAsync(a, read.Allowed, end, ct)), Canon(read.Facts));
            }

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
            Assert.Equal(Canon(await TodaysAsync(a, during.Allowed, a.End, ct)), Canon(during.Facts));

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

            var fleet = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(fleet.UsedHashHourly);
            var fleetText = Canon(fleet.Facts);

            /* Right after the upgrade every built hour's pair has hash_built_seq NULL: no table hour, so no route. */
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            var stale = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.False(stale.UsedHashHourly);
            Assert.Equal(fleetText, Canon(stale.Facts));

            /* A store below 221 has no table. */
            await ExecAsync(a.Connection, "DROP TABLE collect.query_store_hash_hourly", ct);
            var below = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.False(below.UsedHashHourly);
            Assert.Equal(fleetText, Canon(below.Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

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
}
