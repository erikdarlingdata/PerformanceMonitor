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
   live collection. It does join "viewer-time-statics" (#6831): one test renders the Viewer's note, which reads ViewerTimeHelper's process-wide
   statics, and compares it to the same note rendered a moment later in the test, so another class setting the display mode in between
   (ViewerTimeHelperTests and others run in parallel) made the equality fail at random. The collection runs with parallelization disabled. */
[Collection("viewer-time-statics")]
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
    private static async Task<Arranged> ArrangeAsync(CancellationToken ct, bool collectionTimeOrder = false, Func<DateTime, string>? preBuildSql = null)
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

        if (preBuildSql is not null)
        {
            await ExecAsync(connection, preBuildSql(hourNow), ct);
        }

        Assert.Equal(ComposeStampLiveSupport.BuiltHours, await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)));
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
    /// #7240 step 5: each arm joins the tag map and groups by <c>(server_id, tag_id)</c>, and the outer query adds the arms: SUM for the
    /// sums, MAX for the memory peak. The seed maps hash1 and hash2 of server 1 to two tags, hash1 and hash3 of server 2 to the same two,
    /// and leaves server 3 and every other hash untagged. Four facts, the first three against today's wide statement:
    /// 1) server 1 has one row for each of its two tags, with different figures. 2) Each server has exactly one untagged row, and server 3
    /// has nothing else. 3) A hash that is in the hourly arm (built hours) and in a wide arm (the open hour, the unaligned first hour) adds
    /// across them. 4) The memory peak is the larger of the arms' peaks, whichever arm holds it: server 1's tag-a peak sits in the wide
    /// arm (9,000,000 against 8,000,000 in the hourly arm), server 2's tag-b peak sits in the hourly arm (6,000,000 against 3,000,000 in
    /// the wide arm). A SUM in place of the MAX gives 17,000,000 and 9,000,000.
    /// </summary>
    [Fact]
    public async Task TheFleetRead_AddsTheArmsPerServerAndTag_SumsAddAndTheMemoryPeakIsTheLargerArm()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct, preBuildSql: hourNow => $@"
UPDATE collect.query_store_interval_wide SET max_query_max_used_memory = 8000000
WHERE server_id = 1 AND query_hash = 'hash1' AND collection_time >= TIMESTAMP '{At(hourNow.AddHours(-5))}' AND collection_time < TIMESTAMP '{At(hourNow.AddHours(-4))}';
UPDATE collect.query_store_interval_wide SET max_query_max_used_memory = 6000000
WHERE server_id = 2 AND query_hash = 'hash3' AND collection_time >= TIMESTAMP '{At(hourNow.AddHours(-5))}' AND collection_time < TIMESTAMP '{At(hourNow.AddHours(-4))}'");
        var bodySucceeded = false;
        try
        {
            /* The open hour is no built hour, so these two rows are read by a wide arm, under the hashes the seed maps. */
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 1, a.HourNow.AddMinutes(5), 710_001, ct);
            await ExecAsync(a.Connection, "UPDATE collect.query_store_interval_wide SET query_hash = 'hash1', max_query_max_used_memory = 9000000 WHERE runtime_stats_interval_id = 710001", ct);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 2, a.HourNow.AddMinutes(5), 710_002, ct);
            await ExecAsync(a.Connection, "UPDATE collect.query_store_interval_wide SET query_hash = 'hash3', max_query_max_used_memory = 3000000 WHERE runtime_stats_interval_id = 710002", ct);
            Assert.Equal(2, await ComposeStampLiveSupport.CountAsync(a.Connection, "SELECT count(*) FROM collect.query_store_hash_hourly WHERE mem_max IN (8000000, 6000000)", ct));

            var read = await FleetAsync(a, Servers, a.Start, a.End, ct);
            Assert.True(read.UsedHashHourly);

            var peak = Array.FindIndex(QueryTagRollupSection.QueryStore.Measures.ToArray(), m => m.Name == "peak_memory_pages");
            var tagA = (int)await ComposeStampLiveSupport.CountAsync(a.Connection, "SELECT id FROM config.query_tags WHERE name = 'tag-a'", ct);
            var tagB = (int)await ComposeStampLiveSupport.CountAsync(a.Connection, "SELECT id FROM config.query_tags WHERE name = 'tag-b'", ct);
            Assert.Equal(read.Facts.Count, read.Facts.Select(f => (f.ServerId, f.TagId)).Distinct().Count());

            /* 1) Server 1's two tags are two rows with figures of their own. */
            var oneA = Assert.Single(read.Facts, f => f.ServerId == 1 && f.TagId == tagA);
            var oneB = Assert.Single(read.Facts, f => f.ServerId == 1 && f.TagId == tagB);
            Assert.NotEqual(string.Join(",", oneA.Values), string.Join(",", oneB.Values));

            /* 2) One untagged row for every server, and server 3 has only that one. */
            foreach (var server in Servers)
            {
                var untagged = Assert.Single(read.Facts, f => f.ServerId == server && f.TagId is null);
                Assert.Contains(untagged.Values, v => v > 0);
            }

            Assert.Single(read.Facts, f => f.ServerId == 3);

            /* 3 and 4) The arms add, and the peak is the larger arm's peak, not their sum. */
            Assert.Equal(9_000_000d, oneA.Values[peak]);
            var twoB = Assert.Single(read.Facts, f => f.ServerId == 2 && f.TagId == tagB);
            Assert.Equal(6_000_000d, twoB.Values[peak]);

            /* Every figure of every row, against the wide statement. */
            Assert.Equal(Canon(await TodaysAsync(a, Snapped(read), a.End, ct)), Canon(read.Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #7240 step 5: the hourly arm reads many hashes and sends out a row per server and tag. Step 4 grouped it by
    /// <c>(server_id, query_hash)</c> first, so the hash aggregate held one group per hash, and on the 210-server load-test store that
    /// wrote 1.34 GB of temp files, which the 1 GB <c>temp_file_limit</c> of the mcp and viewer roles refuses. Here the table holds about
    /// a million rows over 36,000 distinct (server, hash) pairs and the statement runs under a 1 MB <c>work_mem</c>: the step 4 shape
    /// (the control, written out below) must write temp blocks, so the setting is tight enough to show a spill, and the routed
    /// statement must write none.
    /// </summary>
    [Fact]
    public async Task TheRoutedStatement_WritesNoTempBlocks_WhereTheGroupByHashShapeSpills()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await SeedBulkHourlyAsync(a, ct);

            var window = Servers.ToDictionary(s => s, _ => a.Start);
            var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, window, window.Values.Min(), a.End, 120, ct);
            Assert.NotNull(plan);

            async Task<long> TempBlocksWrittenAsync(NpgsqlCommand command, NpgsqlTransaction transaction)
            {
                command.Transaction = transaction;
                command.CommandText = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + command.CommandText;
                var json = Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
                using var document = JsonDocument.Parse(json);
                var root = document.RootElement[0].GetProperty("Plan");
                return root.GetProperty("Temp Written Blocks").GetInt64();
            }

            await using var transaction = await a.Connection.BeginTransactionAsync(ct);
            await ExecAsync(a.Connection, "SET LOCAL work_mem = '1MB'; SET LOCAL max_parallel_workers_per_gather = 0", ct);

            /* The control: step 4's hourly arm, grouped by hash over the same rows. */
            await using var control = new NpgsqlCommand(@"
SELECT t.server_id, t.query_hash, SUM(t.ec_sum), SUM(t.cpu_wsum), SUM(t.dur_wsum), SUM(t.reads_wsum), SUM(t.writes_wsum), MAX(t.mem_max)
FROM collect.query_store_hash_hourly AS t
WHERE t.server_id = ANY($1)
GROUP BY t.server_id, t.query_hash", a.Connection);
            control.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = Servers });
            var controlBlocks = await TempBlocksWrittenAsync(control, transaction);
            Assert.True(controlBlocks > 0, "the control must spill under this work_mem, else the test shows nothing, got " + controlBlocks);

            await using var routed = QueryTagRollupHashRoute.Command(a.Connection, plan, a.End, 120);
            var routedBlocks = await TempBlocksWrittenAsync(routed, transaction);
            Assert.Equal(0, routedBlocks);
            await transaction.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// Rows added to the table directly leave the ledger's pairs as they are, so every built hour stays valid and is read from the table:
    /// about a million rows over 36,000 distinct (server, hash) pairs.
    /// </summary>
    private static async Task SeedBulkHourlyAsync(Arranged a, CancellationToken ct) => await ExecAsync(a.Connection, $@"
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum)
SELECT TIMESTAMP '{At(a.HourNow.AddHours(-24))}' + h * interval '1 hour', 2, (q % 5000) + 1, s, 'bulk' || q, q % 7 + 1, q * 3, q * 5, q * 7, q
FROM generate_series(0, 27) AS h CROSS JOIN generate_series(1, 3) AS s CROSS JOIN generate_series(1, 12000) AS q;
ANALYZE collect.query_store_hash_hourly", ct);

    /// <summary>
    /// #7365: the statement keeps its parallel plan and still writes no temp blocks, whether the workers start or not. On the load-test store
    /// the flat statement (no GROUP BY in the arms, one at the top) failed on the 1 GB <c>temp_file_limit</c> of the mcp and viewer roles when
    /// no worker could start: the planner hid <c>(server_id, tag_id)</c> behind the <c>UNION ALL</c>, guessed 40,000 groups, and sorted every
    /// row of the hourly arm. Here the same million-row seed runs under a 1 MB <c>work_mem</c> and a 4 MB <c>temp_file_limit</c> (the seed's
    /// scale of the role's limit: a sort of the arm writes about 70 MB and the statement would fail) with 4 workers planned. First with the
    /// workers free to start: the plan must have a Gather with workers planned and write no temp block. Then with <c>max_parallel_workers</c> at
    /// 0, which leaves every Gather to the leader alone: the same plan, no worker launched, no temp block and no error.
    /// </summary>
    [Fact]
    public async Task TheRoutedStatement_KeepsItsParallelPlan_AndWritesNoTempBlocks_WithFourWorkersAndWithTheLeaderAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await SeedBulkHourlyAsync(a, ct);
            var window = Servers.ToDictionary(s => s, _ => a.Start);
            var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, window, window.Values.Min(), a.End, 120, ct);
            Assert.NotNull(plan);

            /* Every Gather in the plan: (workers planned, workers launched). */
            static void CollectGathers(JsonElement node, List<(long Planned, long Launched)> into)
            {
                if (node.TryGetProperty("Workers Planned", out var planned))
                {
                    into.Add((planned.GetInt64(), node.TryGetProperty("Workers Launched", out var launched) ? launched.GetInt64() : -1));
                }

                if (node.TryGetProperty("Plans", out var children))
                {
                    foreach (var child in children.EnumerateArray())
                    {
                        CollectGathers(child, into);
                    }
                }
            }

            async Task<(long TempBlocks, List<(long Planned, long Launched)> Gathers)> RunAsync(string label, int maxParallelWorkers)
            {
                await using var transaction = await a.Connection.BeginTransactionAsync(ct);
                await ExecAsync(a.Connection, $"SET LOCAL work_mem = '1MB'; SET LOCAL temp_file_limit = '4MB'; SET LOCAL max_parallel_workers_per_gather = 4; SET LOCAL max_parallel_workers = {maxParallelWorkers}", ct);
                await using var routed = QueryTagRollupHashRoute.Command(a.Connection, plan, a.End, 120);
                routed.Transaction = transaction;
                routed.CommandText = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + routed.CommandText;
                string json;
                try
                {
                    json = Convert.ToString(await routed.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ConfigurationLimitExceeded)
                {
                    throw new Xunit.Sdk.XunitException(label + ": the statement hit the temp_file_limit: " + ex.MessageText);
                }

                using var document = JsonDocument.Parse(json);
                var root = document.RootElement[0].GetProperty("Plan");
                var gathers = new List<(long Planned, long Launched)>();
                CollectGathers(root, gathers);
                await transaction.RollbackAsync(ct);
                return (root.GetProperty("Temp Written Blocks").GetInt64(), gathers);
            }

            var withWorkers = await RunAsync("four workers", 8);
            Assert.True(withWorkers.Gathers.Any(g => g.Planned > 0), "four workers: the plan has no Gather with workers planned");
            Assert.Equal(0, withWorkers.TempBlocks);

            var leaderAlone = await RunAsync("leader alone", 0);
            Assert.True(leaderAlone.Gathers.Any(g => g.Planned > 0), "leader alone: the plan has no Gather with workers planned");
            Assert.All(leaderAlone.Gathers, g => Assert.Equal(0, g.Launched));
            Assert.Equal(0, leaderAlone.TempBlocks);
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

            /* Every built hour's pair is a late write (#6865: hours stale only for the hash table are left out instead, so this is the way to leave no table hour): no table hour, so no route. */
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET late_seq = late_seq + 1", ct);
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
            /* #6831: the dropped start and the first instant read, as fields for the page's own words. */
            Assert.Equal(McpHelpers.FormatEffectiveStart(snappedStart), section.GetProperty("served_start").GetString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(toolStart), section.GetProperty("start_dropped_from").GetString());

            /* The Viewer's section reads through the same reader and says the same edge in its own clock and words (#6831): no ISO stamp. */
            await using var viewer = new ViewerDataService(a.Scratch.ConnectionString);
            var viewed = await viewer.GetQueryTagRollupDataAsync(Servers, toolStart, a.End, ct);
            Assert.Equal(QueryTagUsageNotes.QueryStore(false, snappedStart, toolStart, a.End, QueryTagUsageNotes.Stamper(Servers.Length)),
                viewed.Notes[QueryTagRollupSection.QueryStore.Name]);

            /* The fallback: today's statement, the exact start, no note and no tier. */
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET late_seq = late_seq + 1", ct);
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
            var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, window, window.Values.Min(), a.End, 120, ct);
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
            /* #7240 step 4: the arm reads a range of hours, so the heap is reached through the index as an index scan or as a bitmap heap scan over a bitmap index scan of it. */
            Assert.All(tableNodes, n => Assert.NotEqual("Seq Scan", Text(n, "Node Type")));
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
    /// Found by what the nodes do: every scan of the wide table or one of its partitions, plus every bitmap index scan of one of their
    /// indexes. Per node the rows it examined are Actual Rows plus Rows Removed by Filter plus Rows Removed by Index Recheck, times Actual
    /// Loops. A node on the <c>(server_id, first_execution_time)</c> index is bounded by what its own index condition admits (#6847): each
    /// arm's first-execution range is <c>[range start - 26 h, earlier of range end and window end + 12 h]</c>, which for a small range is
    /// about the seed's window, and PostgreSQL may AND that index into the scan of <c>collection_time</c> on some versions and statistics.
    /// So those nodes are held to the rows the arms' first-execution ranges hold (counted with the statement's bounds), and fail on an
    /// index walk past them, and every other node is held to the window's half as before. The heap is laid out in collection_time order, as the store's is, so the BRIN
    /// index on collection_time can do what it is for, and sequential scans are turned off (the seed is a few thousand rows, where any
    /// plan scans), so the plan shows the paths the statement allows.
    /// </summary>
    [Fact]
    public async Task TheRoutedStatementsWideArms_ExamineAboutTheirRangesRows_AndTheirIndexRangesStayInTheirBounds()
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
            var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, window, window.Values.Min(), a.End, 120, ct);
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

            /* A node reads the table through either index family. Those on (server_id, first_execution_time) return every entry their own
               condition admits, which the arm's first-execution range (floor 26 h before its start, #4605, cap 12 h after its end, #5523,
               #6847) makes about as wide as the seed's window for a range that is itself small, and that cost is what the floor is written
               for. So those nodes are bounded by the rows that range holds, counted here with the statement's own bounds, and every other node
               (the scans of collection_time, and the heap) by the rows of the arm's own range as before. A node that walks past its own
               bounds fails the first, and a plan that reads the window through collection_time fails the second. */
            var wideNodes = nodes.Where(n =>
                Text(n, "Relation Name").StartsWith("query_store_interval_wide", StringComparison.Ordinal)
                || (Text(n, "Node Type") == "Bitmap Index Scan" && Text(n, "Index Name").Contains("query_store_interval_wide", StringComparison.Ordinal))).ToList();
            Assert.NotEmpty(wideNodes);
            double Examined(JsonElement n) => (Num(n, "Actual Rows") + Num(n, "Rows Removed by Filter") + Num(n, "Rows Removed by Index Recheck")) * Num(n, "Actual Loops");
            // Only a Bitmap Index Scan returns index entries without reading the table. A plain Index Scan on
            // that index reads table rows, so it stays under the half-the-window bound with the other nodes that read the table.
            bool OnFirstExecIndex(JsonElement n) => Text(n, "Node Type") == "Bitmap Index Scan" && Text(n, "Index Name").Contains("first_exec", StringComparison.Ordinal);
            string Describe(JsonElement n) => Text(n, "Node Type") + " " + Text(n, "Relation Name") + Text(n, "Index Name") + " rows " + Num(n, "Actual Rows") + " loops " + Num(n, "Actual Loops");

            var rangeNodes = wideNodes.Where(n => !OnFirstExecIndex(n)).ToList();
            var firstExecNodes = wideNodes.Where(OnFirstExecIndex).ToList();
            var examined = rangeNodes.Sum(Examined);
            Assert.True(examined > 0, "the wide arms must have read rows");
            Assert.True(
                examined < windowRows / 2.0,
                "the wide arms examined " + examined.ToString("0", CultureInfo.InvariantCulture) + " rows of a window that holds " + windowRows + "; nodes: "
                + string.Join(" | ", rangeNodes.Select(Describe)));

            /* The rows each arm's first-execution range holds, for the servers that share the arm, summed over the arms (the arms' index
               scans sit under one plan, and an arm's nodes cannot return more than the rows of its own bounds). */
            long firstExecRows = 0;
            foreach (var group in plan.WideRanges.GroupBy(r => (r.From, r.To)))
            {
                var servers = string.Join(",", group.Select(r => r.ServerId).Distinct().OrderBy(s => s));
                firstExecRows += await ComposeStampLiveSupport.CountAsync(
                    a.Connection,
                    $@"SELECT count(*) FROM collect.query_store_interval_wide
WHERE server_id IN ({servers})
AND first_execution_time >= TIMESTAMP '{At(group.Key.From)}' - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
AND first_execution_time <= LEAST(TIMESTAMP '{At(group.Key.To)}', TIMESTAMP '{At(a.End)}') + {QueryStoreIntervalWide.FirstExecUpperSlackSql}",
                    ct);
            }

            var firstExecExamined = firstExecNodes.Sum(Examined);
            Assert.True(
                firstExecExamined <= firstExecRows,
                "the first-execution indexes returned " + firstExecExamined.ToString("0", CultureInfo.InvariantCulture) + " entries for arms whose first-execution ranges hold "
                + firstExecRows + " rows; nodes: " + string.Join(" | ", firstExecNodes.Select(Describe)));

            await transaction.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6865: hours the builder has done that are stale only for the hash table (the state of every hour right after V221) are not read from
    /// the wide table. The read answers the newest span after them, says how many hours it left out, and its figures equal the wide statement
    /// over that span. A window with no such hour reads in full. Real late-write stale hours are still read from the wide table.
    /// </summary>
    [Fact]
    public async Task AReadWithBacklogHours_AnswersTheNewestSpanAfterThem_AndSaysHowManyHoursWait()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var starts = Servers.ToDictionary(s => s, _ => a.Start);

            /* No backlog hour: the full window. */
            var full = await FleetFromAsync(a, starts, ct);
            Assert.True(full.UsedHashHourly);
            Assert.Equal(0, full.BacklogHoursWaiting);
            Assert.All(full.RoutedStarts!.Values, start => Assert.Equal(QueryTagRollupHashRoute.CeilHour(a.Start), start));

            /* The state mid-backlog: the hours 13 and more back have no valid hash rows (the post-V221 state), the newer ones do. */
            var backlogBelow = a.HourNow.AddHours(-12);
            await ExecAsync(a.Connection, $"UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL WHERE hour < TIMESTAMP '{At(backlogBelow)}'", ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_hourly WHERE hour < TIMESTAMP '{At(backlogBelow)}'", ct);

            var cut = await FleetFromAsync(a, starts, ct);
            Assert.True(cut.UsedHashHourly);
            /* Hours 23 back to 13 back wait: 11 of them, and the read leaves out the same 11 whole hours (from the first whole hour of the window to the cut). */
            Assert.Equal(11, cut.BacklogHoursWaiting);
            Assert.Equal(11, cut.BacklogHoursLeftOut);
            Assert.Equal(3, cut.RoutedStarts!.Count);
            Assert.All(cut.RoutedStarts!.Values, start => Assert.Equal(backlogBelow, start));
            Assert.NotEmpty(cut.Facts);
            var span = Servers.ToDictionary(s => s, _ => backlogBelow);
            Assert.Equal(Canon(await TodaysAsync(a, span, a.End, ct)), Canon(cut.Facts));

            /* A real late write into a built hour inside the span (the rollup rows go stale too) is still read from the wide table, not left out. */
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, a.HourNow.AddHours(-6).AddMinutes(7), 700_001, ct);
            var late = await FleetFromAsync(a, starts, ct);
            Assert.Equal(11, late.BacklogHoursWaiting);
            Assert.All(late.RoutedStarts!.Values, start => Assert.Equal(backlogBelow, start));
            Assert.Equal(Canon(await TodaysAsync(a, span, a.End, ct)), Canon(late.Facts));

            /* An hour inside the gap whose hash rows are valid again is not backlog: the backlog count drops to 10, but the read still starts at the cut,
               so it still leaves out 11 hours. The two numbers are not the same number. */
            await ExecAsync(a.Connection, $"UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = late_seq WHERE hour = TIMESTAMP '{At(a.HourNow.AddHours(-18))}'", ct);
            var gap = await FleetFromAsync(a, starts, ct);
            Assert.Equal(10, gap.BacklogHoursWaiting);
            Assert.Equal(11, gap.BacklogHoursLeftOut);
            Assert.All(gap.RoutedStarts!.Values, start => Assert.Equal(backlogBelow, start));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6865: an hour the builder will not build soon (a record in <c>collect.query_store_compose_stamp_failures</c>, here in daily back-off) is not
    /// backlog, so it does not pin the cut. A 168-hour fleet read keeps every older hour and reads the stuck hour from the wide table, exactly as before the
    /// backlog cut existed. With the record gone the same hour is backlog and the read does cut there: that is what the record changes.
    /// The 168-hour window is read through the reader with starts of its own (the seed holds 28 built hours, the older ones are plain wide hours),
    /// and the tool is read for the seed's own 24-hour window.
    /// </summary>
    [Fact]
    public async Task AnHourInBackoff_DoesNotPinTheCut_SoTheOlderHoursStayInTheRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var stuck = a.HourNow.AddHours(-20);
            await ExecAsync(a.Connection, $"UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL WHERE hour = TIMESTAMP '{At(stuck)}'", ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_hourly WHERE hour = TIMESTAMP '{At(stuck)}'", ct);
            await ExecAsync(a.Connection, $@"
INSERT INTO collect.query_store_compose_stamp_failures (hour, failures, last_sqlstate, last_reason, last_timeout_seconds, first_failed_at, last_failed_at, retry_at)
VALUES (TIMESTAMP '{At(stuck)}', 6, '57014', 'canceling statement due to statement timeout', 900,
        (now() AT TIME ZONE 'UTC') - interval '3 days', (now() AT TIME ZONE 'UTC') - interval '1 hour', (now() AT TIME ZONE 'UTC') + interval '23 hours')", ct);

            var windowStart = a.HourNow.AddHours(-168).AddMinutes(17);
            var starts = Servers.ToDictionary(s => s, _ => windowStart);
            var first = QueryTagRollupHashRoute.CeilHour(windowStart);

            /* In back-off: the hour is read wide, nothing is cut, and the figures are the wide statement over the whole 168 hours. */
            var kept = await FleetFromAsync(a, starts, ct);
            Assert.True(kept.UsedHashHourly);
            Assert.Equal(0, kept.BacklogHoursWaiting);
            Assert.Equal(0, kept.BacklogHoursLeftOut);
            Assert.All(kept.RoutedStarts!.Values, start => Assert.Equal(first, start));
            var keptText = Canon(kept.Facts);
            Assert.Equal(Canon(await TodaysAsync(a, Snapped(kept), a.End, ct)), keptText);

            using (var doc = await ToolAsync(a, ct))
            {
                var section = QueryStoreSection(doc);
                Assert.False(section.TryGetProperty("backlog_note", out _));
                Assert.False(section.TryGetProperty("hours_left_out", out _));
                Assert.False(section.TryGetProperty("backlog_hours_waiting", out _));
            }

            /* The record gone: the same hour is backlog, and the read starts after it, leaving out the 148 hours from the first whole hour to the cut. */
            await ExecAsync(a.Connection, "DELETE FROM collect.query_store_compose_stamp_failures", ct);
            var cut = await FleetFromAsync(a, starts, ct);
            Assert.Equal(1, cut.BacklogHoursWaiting);
            Assert.Equal((int)(stuck.AddHours(1) - first).TotalHours, cut.BacklogHoursLeftOut);
            Assert.All(cut.RoutedStarts!.Values, start => Assert.Equal(stuck.AddHours(1), start));
            Assert.NotEqual(keptText, Canon(cut.Facts));

            using (var doc = await ToolAsync(a, ct))
            {
                var section = QueryStoreSection(doc);
                Assert.Equal(1, section.GetProperty("backlog_hours_waiting").GetInt32());
                var windowFirst = QueryTagRollupHashRoute.CeilHour(DateTime.Parse(doc.RootElement.GetProperty("window_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
                Assert.Equal((int)(stuck.AddHours(1) - windowFirst).TotalHours, section.GetProperty("hours_left_out").GetInt32());
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>An open writer of the wide table: an uncommitted insert on its own session, <paramref name="trackActivities"/> off for a session whose start time nobody can read.</summary>
    private static async Task<(NpgsqlConnection Connection, NpgsqlTransaction Transaction)> OpenWriterAsync(Arranged a, bool trackActivities, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(a.Scratch.ConnectionString) { ApplicationName = "writer_6865" };
        if (!trackActivities)
        {
            builder.Options = "-c track_activities=off";
        }

        var writer = new NpgsqlConnection(builder.ConnectionString);
        await writer.OpenAsync(ct);
        var transaction = await writer.BeginTransactionAsync(ct);
        var at = At(a.HourNow.AddMinutes(-10));
        await using var insert = new NpgsqlCommand($@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{at}', 1, 'dbX', 99, 99, 'Regular', TIMESTAMP '{at}', TIMESTAMP '{at}', 'modX', 'hashX', 7, 1000, 500, 2000, 900, 9300002)", writer, transaction);
        await insert.ExecuteNonQueryAsync(ct);
        return (writer, transaction);
    }

    /// <summary>A built hour that waits only for the hash table: the upgrade backlog.</summary>
    private static async Task MakeBacklogAsync(Arranged a, DateTime hour, CancellationToken ct)
    {
        await ExecAsync(a.Connection, $"UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL WHERE hour = TIMESTAMP '{At(hour)}'", ct);
        await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_hourly WHERE hour = TIMESTAMP '{At(hour)}'", ct);
    }

    private static async Task CloseWriterAsync(NpgsqlConnection? writer, NpgsqlTransaction? open, CancellationToken ct)
    {
        if (open is not null)
        {
            await open.RollbackAsync(ct);
        }

        if (writer is not null)
        {
            await writer.DisposeAsync();
        }
    }

    /// <summary>
    /// #6865, #6945: a writer whose start the reading role cannot read (<c>track_activities</c> off, or another role's session without
    /// <c>pg_read_all_stats</c>) holds no hour back from the READ. The planner still counts it as open from before every hour (it runs as the
    /// owner, and a start it cannot read cannot be shown to be late enough), but the read only decides where the answer starts: the backlog hour
    /// stays backlog and the read is cut after it, the same as with a writer whose start is readable and recent, and the same as with no writer.
    /// </summary>
    [Fact]
    public async Task AWriterWhoseStartCannotBeRead_DoesNotHoldBackTheBacklogCut_ANormalWriterAndNoWriterGiveTheSameCut()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        NpgsqlConnection? writer = null;
        NpgsqlTransaction? open = null;
        try
        {
            var stuck = a.HourNow.AddHours(-20);
            await MakeBacklogAsync(a, stuck, ct);
            var windowStart = a.HourNow.AddHours(-168).AddMinutes(17);
            var starts = Servers.ToDictionary(s => s, _ => windowStart);

            foreach (var trackActivities in new[] { true, false })
            {
                (writer, open) = await OpenWriterAsync(a, trackActivities, ct);
                var read = await FleetFromAsync(a, starts, ct);
                Assert.Equal(1, read.BacklogHoursWaiting);
                Assert.All(read.RoutedStarts!.Values, start => Assert.Equal(stuck.AddHours(1), start));
                await CloseWriterAsync(writer, open, ct);
                writer = null;
                open = null;
            }

            var none = await FleetFromAsync(a, starts, ct);
            Assert.Equal(1, none.BacklogHoursWaiting);
            Assert.All(none.RoutedStarts!.Values, start => Assert.Equal(stuck.AddHours(1), start));
            bodySucceeded = true;
        }
        finally
        {
            await CloseWriterAsync(writer, open, ct);
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6945: the roles the tag rollup reads run as (<c>mcp</c>, <c>viewer</c>) are not members of <c>pg_read_all_stats</c>, so the store owner's
    /// open writer shows them no <c>xact_start</c>. The ledger used to read that as a writer open from before every hour: every built hour came
    /// back blocked, the backlog was empty and the read fell back to the wide table. Run as each role with the owner's writer open, no built
    /// hour is blocked (a writer under an hour old can touch none of them) and the backlog hour is recognised.
    /// </summary>
    [Fact]
    public async Task TheOwnersOpenWriter_IsHiddenFromTheMcpAndViewerRoles_AndBlocksNoBuiltHourOnTheirLedger()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        NpgsqlConnection? writer = null;
        NpgsqlTransaction? open = null;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var roles = new[] { "mcp_6945_" + suffix, "viewer_6945_" + suffix };
        try
        {
            var stuck = a.HourNow.AddHours(-20);
            await MakeBacklogAsync(a, stuck, ct);
            var windowStart = a.HourNow.AddHours(-168).AddMinutes(17);
            var starts = Servers.ToDictionary(s => s, _ => windowStart);
            foreach (var role in roles)
            {
                await ExecAsync(a.Connection, $"CREATE ROLE {role} LOGIN", ct);
                await ExecAsync(a.Connection, $"GRANT USAGE ON SCHEMA collect TO {role}", ct);
                await ExecAsync(a.Connection, $"GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {role}", ct);
            }

            (writer, open) = await OpenWriterAsync(a, trackActivities: true, ct);
            foreach (var role in roles)
            {
                var builder = new NpgsqlConnectionStringBuilder(a.Scratch.ConnectionString) { Username = role, Password = null, Pooling = false };
                await using var reader = new NpgsqlConnection(builder.ConnectionString);
                await reader.OpenAsync(ct);

                /* The premise: the owner's session is there, and this role gets no start for it. */
                await using (var seen = new NpgsqlCommand("SELECT count(*) FILTER (WHERE xact_start IS NULL), count(*) FROM pg_stat_activity WHERE application_name = 'writer_6865'", reader))
                await using (var counts = await seen.ExecuteReaderAsync(ct))
                {
                    Assert.True(await counts.ReadAsync(ct));
                    Assert.Equal(1L, counts.GetInt64(1));
                    Assert.Equal(1L, counts.GetInt64(0));
                }

                var built = 0;
                var blocked = new List<DateTime>();
                await using (var ledger = new NpgsqlCommand(QueryTagRollupHashRoute.LedgerSql, reader))
                {
                    ledger.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = QueryTagRollupHashRoute.CeilHour(windowStart) });
                    ledger.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = a.HourNow });
                    ledger.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = Servers });
                    await using var rows = await ledger.ExecuteReaderAsync(ct);
                    while (await rows.ReadAsync(ct))
                    {
                        if (rows.IsDBNull(1) && !rows.GetBoolean(3))
                        {
                            built++;
                            if (rows.GetBoolean(4))
                            {
                                blocked.Add(rows.GetDateTime(0));
                            }
                        }
                    }
                }

                Assert.True(built > 20, role + " saw " + built + " built hours");
                Assert.Empty(blocked);

                var plan = await QueryTagRollupHashRoute.TryPlanAsync(reader, starts, windowStart, a.End, 60, ct);
                Assert.NotNull(plan);
                Assert.Equal(1, plan.BacklogHoursWaiting);
                Assert.All(plan.Starts.Values, start => Assert.Equal(stuck.AddHours(1), start));
            }

            bodySucceeded = true;
        }
        finally
        {
            await CloseWriterAsync(writer, open, ct);
            await CleanupAsync(a, bodySucceeded);
            foreach (var role in roles)
            {
                await using var drop = new NpgsqlConnection(ComposeStampLiveSupport.BaseConnectionString);
                try
                {
                    await drop.OpenAsync(CancellationToken.None);
                    await using var command = new NpgsqlCommand($"DROP ROLE IF EXISTS {role}", drop);
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (PostgresException)
                {
                    /* The scratch database is already dropped, so its grants are gone with it; a role left behind carries a fresh suffix. */
                }
            }
        }
    }

    /// <summary>
    /// #6865: a server added later starts after the cut. Every surface names the latest server's start as where the figures start, so the hours left
    /// out are counted to that same instant, not to the cut. Servers 1 and 2 read from the window start, server 3 from 8 hours back, and the
    /// backlog (hours 13 and more back) ends before server 3 starts.
    /// </summary>
    [Fact]
    public async Task AServerStartingAfterTheCut_HasTheHoursLeftOutCountedToTheStartTheFiguresNameAsTheirOwn()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var backlogBelow = a.HourNow.AddHours(-12);
            await ExecAsync(a.Connection, $"UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL WHERE hour < TIMESTAMP '{At(backlogBelow)}'", ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_hourly WHERE hour < TIMESTAMP '{At(backlogBelow)}'", ct);
            var later = a.HourNow.AddHours(-8).AddMinutes(17);
            var starts = new Dictionary<int, DateTime> { [1] = a.Start, [2] = a.Start, [3] = later };

            var read = await FleetFromAsync(a, starts, ct);
            Assert.Equal(11, read.BacklogHoursWaiting);
            var figuresStart = read.RoutedStarts!.Values.Max();
            Assert.Equal(QueryTagRollupHashRoute.CeilHour(later), figuresStart);
            Assert.True(figuresStart > backlogBelow, "the later server must start after the cut");
            Assert.Equal(backlogBelow, read.RoutedStarts![1]);
            Assert.Equal(figuresStart, read.RoutedStarts![3]);
            /* From the first whole hour (23 back) to the later server's start (7 back), 16 hours, not the 11 to the cut. */
            Assert.Equal((int)(figuresStart - QueryTagRollupHashRoute.CeilHour(a.Start)).TotalHours, read.BacklogHoursLeftOut);
            Assert.Equal(16, read.BacklogHoursLeftOut);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6865: the hours left out are counted from the window's own first whole hour, which is what the sentence ("the first N hours of the window") and the
    /// tool description say, not from the earliest start the gate allows. The interval table's floor has already cut every start 10 hours into the
    /// window (the gate gives each server a read start 10 hours after the window's), and the backlog (hour 13 back and older) ends one hour into what
    /// is read. One backlog hour waits, the figures start at the cut 12 hours back, and the read leaves out the 11 hours from the window's first whole
    /// hour, 10 of them before the table's floor. Counted from the earliest read start it would have said 1.
    /// </summary>
    [Fact]
    public async Task WhenTheTablesFloorAlreadyCutTheStart_TheHoursLeftOutAreCountedFromTheWindowsStart()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var backlogBelow = a.HourNow.AddHours(-12);
            await ExecAsync(a.Connection, $"UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL WHERE hour < TIMESTAMP '{At(backlogBelow)}'", ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_hourly WHERE hour < TIMESTAMP '{At(backlogBelow)}'", ct);
            var floorCut = a.Start.AddHours(10);
            var starts = Servers.ToDictionary(s => s, _ => floorCut);

            var read = await FleetFromAsync(a, starts, ct, windowStart: a.Start);
            Assert.Equal(1, read.BacklogHoursWaiting);
            Assert.All(read.RoutedStarts!.Values, start => Assert.Equal(backlogBelow, start));
            var windowFirst = QueryTagRollupHashRoute.CeilHour(a.Start);
            Assert.Equal(11, read.BacklogHoursLeftOut);
            Assert.Equal((int)(backlogBelow - windowFirst).TotalHours, read.BacklogHoursLeftOut);
            var span = Servers.ToDictionary(s => s, _ => backlogBelow);
            Assert.Equal(Canon(await TodaysAsync(a, span, a.End, ct)), Canon(read.Facts));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// #6865: the newest built hour of the window has a real late write and every older built hour is backlog. The cut is the end of the newest BACKLOG
    /// hour, which is the start of the late-written hour, so the late-written hour is read wide with the tail. Nothing after the cut is a valid table
    /// hour (every hash row is gone), so the wide table's tail serves the whole answer. The route still answers (today's statement would read the whole
    /// window wide), but the section must not name the hash tables as a source: <c>UsedHashHourly</c>, the section's <c>tier</c> and its <c>source</c>
    /// say only what served it, and the figures are the wide statement over the tail.
    /// </summary>
    [Fact]
    public Task AReadWhoseNewestBuiltHourIsALateWrite_AndTheRestBacklog_IsServedByTheTailAlone_AndNamesNoHashTable() => ServedByTheTailAloneAsync(everyBuiltHourIsBacklog: false);

    /// <summary>
    /// #6865: the state right after V221, where no built hour has a late write, so literally every built hour of the window is backlog. The cut is the
    /// newest built hour plus one (one hour later than in the late-write sibling above), the hours after it are the builder's own lag and are not built,
    /// so the plan holds no table pair and no day pair, only the wide tail. The section must say so: no tier, the wide table as the only source, and
    /// the backlog count equals the number of built hours of the window.
    /// </summary>
    [Fact]
    public Task RightAfterTheUpgrade_WhereEveryBuiltHourIsBacklog_TheSectionNamesTheWideTableAlone() => ServedByTheTailAloneAsync(everyBuiltHourIsBacklog: true);

    private static async Task ServedByTheTailAloneAsync(bool everyBuiltHourIsBacklog)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The seed builds its hours after its writes, so none of its built hours is late-written. The sibling that is not "every hour" gets a genuine
               late write into the newest built hour, so its pairs differ and the hour is not backlog. */
            DateTime? lateHour = null;
            if (!everyBuiltHourIsBacklog)
            {
                await using var newestBuilt = new NpgsqlCommand($"SELECT max(hour) FROM collect.query_store_compose_stamp_hours WHERE hour < TIMESTAMP '{At(a.HourNow)}'", a.Connection);
                lateHour = DateTime.SpecifyKind((DateTime)(await newestBuilt.ExecuteScalarAsync(ct))!, DateTimeKind.Unspecified);
                await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, lateHour.Value.AddMinutes(7), 700_002, ct);
            }

            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await ExecAsync(a.Connection, "DELETE FROM collect.query_store_hash_hourly", ct);
            var starts = Servers.ToDictionary(s => s, _ => a.Start);

            var read = await FleetFromAsync(a, starts, ct);
            Assert.False(read.UsedHashHourly);
            Assert.False(read.UsedHashDaily);
            var first = QueryTagRollupHashRoute.CeilHour(a.Start);
            /* The backlog hours are the built hours of the window whose stale pairs are stale only for the hash table (built_seq equals late_seq), without
               the late-written hour: that one is read wide as before. The cut is the end of the newest backlog hour. */
            var backlogWhere = $"FROM collect.query_store_compose_stamp_built WHERE hour >= TIMESTAMP '{At(first)}' AND hour < TIMESTAMP '{At(a.HourNow)}' AND built_seq IS NOT DISTINCT FROM late_seq"
                + (lateHour is null ? "" : $" AND hour <> TIMESTAMP '{At(lateHour.Value)}'");
            var backlogHours = await ComposeStampLiveSupport.CountAsync(a.Connection, $"SELECT count(DISTINCT hour) {backlogWhere}", ct);
            await using var newest = new NpgsqlCommand($"SELECT max(hour) {backlogWhere}", a.Connection);
            var cut = DateTime.SpecifyKind((DateTime)(await newest.ExecuteScalarAsync(ct))!, DateTimeKind.Unspecified).AddHours(1);
            Assert.True(backlogHours > 0);
            if (lateHour is not null)
            {
                Assert.Equal(lateHour.Value, cut);
            }

            if (everyBuiltHourIsBacklog)
            {
                var builtHours = await ComposeStampLiveSupport.CountAsync(a.Connection,
                    $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour >= TIMESTAMP '{At(first)}' AND hour < TIMESTAMP '{At(a.HourNow)}'", ct);
                Assert.Equal(builtHours, backlogHours);
            }

            Assert.Equal(backlogHours, read.BacklogHoursWaiting);
            Assert.Equal((int)(cut - first).TotalHours, read.BacklogHoursLeftOut);
            Assert.All(read.RoutedStarts!.Values, start => Assert.Equal(cut, start));
            var tail = Servers.ToDictionary(s => s, _ => cut);
            var expected = await TodaysAsync(a, tail, a.End, ct);
            Assert.NotEmpty(expected);
            Assert.Equal(Canon(expected), Canon(read.Facts));

            /* The section the MCP tool writes names the wide table only, and has no tier. */
            using var doc = JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(
                a.Source, null, 24, null, 25, a.End.ToString("o", CultureInfo.InvariantCulture), null, ct));
            var section = doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == "query_store");
            Assert.Equal("collect.query_store_interval_wide", section.GetProperty("source").GetString());
            Assert.False(section.TryGetProperty("tier", out _));
            Assert.DoesNotContain("query_store_hash", section.GetRawText(), StringComparison.Ordinal);
            Assert.True(section.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(backlogHours, section.GetProperty("backlog_hours_waiting").GetInt32());
            /* The sentence, the count and the start name one instant: the count is the whole hours from the window's first whole hour to effective_start. */
            var effectiveStart = section.GetProperty("effective_start").GetString()!;
            Assert.Equal(McpHelpers.FormatEffectiveStart(cut), effectiveStart);
            var leftOut = section.GetProperty("hours_left_out").GetInt32();
            Assert.Equal((int)(cut - QueryTagRollupHashRoute.CeilHour(a.End.AddHours(-24))).TotalHours, leftOut);
            Assert.Equal(QueryTagRollupHashRoute.BacklogNote(effectiveStart, leftOut, (int)backlogHours), section.GetProperty("backlog_note").GetString());
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
            await ExecAsync(a.Connection, "UPDATE collect.query_store_compose_stamp_built SET late_seq = late_seq + 1", ct);
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
    private static async Task<DarlingQueryTagRollupReader.QueryStoreRead> FleetFromAsync(Arranged a, Dictionary<int, DateTime> starts, CancellationToken ct, DateTime? windowStart = null)
    {
        await using var transaction = await a.Connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        var read = await DarlingQueryTagRollupReader.ReadAllowedAsync(a.Connection, new Dictionary<int, DateTime>(starts), [], windowStart ?? starts.Values.Min(), a.End, 120, ct);
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

    /// <summary>
    /// #7240 step 4: the routed statement's hourly arm is one range on <c>hour</c> less a list of gaps, grouped by <c>(server_id, query_hash)</c>
    /// before the tag join. It must return, value for value, what the statement it replaced returned: the <c>unnest</c> of every
    /// <c>(hour, server)</c> pair, ungrouped. The old text is kept below as the reference and run with the old parameters on the same data.
    /// Plan one has servers with three different starts, stale pairs of each (moved intervals and a late row), and a built hour taken
    /// out of the middle of the window (every server's pair is then read wide). Plan two has the day table: two servers take a day from
    /// <c>query_store_hash_daily</c> (rows that differ from the hours' sums, so an hourly arm that read the day's hours too would show), the
    /// third reads the same day's hours, one of which is stale. The hourly table also holds a NULL <c>query_hash</c>, NULL measures, hashes mapped to
    /// two tags and hashes mapped to none, and a repeated row for one (hour, server, hash).
    /// </summary>
    [Fact]
    public async Task TheHourlyArmWithARangeAndGaps_ReturnsWhatTheListOfEveryPairReturned()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time - interval '1 hour', avg_cpu_time_us = 77777777
WHERE server_id = 1 AND query_id = 3 AND plan_id = 31 AND collection_time >= TIMESTAMP '{At(a.HourNow.AddHours(-20))}' AND collection_time < TIMESTAMP '{At(a.HourNow.AddHours(-20).AddMinutes(30))}'", ct);
            await ExecAsync(a.Connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time + interval '1 hour', avg_logical_io_reads = 4000
WHERE server_id = 2 AND query_id IN (1, 2) AND collection_time >= TIMESTAMP '{At(a.HourNow.AddHours(-12))}' AND collection_time < TIMESTAMP '{At(a.HourNow.AddHours(-11))}'", ct);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, a.HourNow.AddHours(-6).AddMinutes(7), 700_001, ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(a.HourNow.AddHours(-14))}'", ct);

            var starts = new Dictionary<int, DateTime>
            {
                [1] = a.Start,
                [2] = a.HourNow.AddHours(-16).AddMinutes(41),
                [3] = a.HourNow.AddHours(-9).AddMinutes(13),
            };
            var planOne = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, starts, starts.Values.Min(), a.End, 120, ct);
            Assert.NotNull(planOne);
            Assert.Equal(3, planOne.TablePairs.Select(p => p.ServerId).Distinct().Count());
            Assert.DoesNotContain(planOne.TablePairs, p => p.Hour == a.HourNow.AddHours(-14));
            Assert.DoesNotContain(planOne.TablePairs, p => p.Hour == a.HourNow.AddHours(-12) && p.ServerId == 2);
            Assert.Contains(planOne.TablePairs, p => p.Hour == a.HourNow.AddHours(-12) && p.ServerId == 1);
            var hourlyOne = QueryTagRollupHashRoute.HourlyReadFor(planOne, a.End);
            Assert.True(hourlyOne.GapServers.Length >= 5, "the gaps must cover the unbuilt hour, the stale pairs and the later starts, got " + hourlyOne.GapServers.Length);
            await AssertSameAsTheListOfPairsAsync(a, planOne, ct);
            await AssertSameAsThePerArmGroupedStatementAsync(a, planOne, ct);

            /* Plan two: a day from the day table. */
            var day = new DateTime(a.HourNow.AddHours(-48).Date.Ticks, DateTimeKind.Unspecified);
            await ExecAsync(a.Connection, $@"
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum)
SELECT h, 2, CASE WHEN k.hash = 'hash3' THEN NULL ELSE extract(hour FROM h)::int * 10 + s END, s, k.hash,
       extract(hour FROM h)::int + s + 1, (extract(hour FROM h)::int + 1) * 100 + s, (extract(hour FROM h)::int + 1) * 1000 + s,
       CASE WHEN extract(hour FROM h)::int % 7 = 0 THEN NULL ELSE extract(hour FROM h)::int * 13 + s END, extract(hour FROM h)::int * 3 + s
FROM generate_series(TIMESTAMP '{At(day)}', TIMESTAMP '{At(day.AddHours(23))}', interval '1 hour') AS h
CROSS JOIN (VALUES (1), (2), (3)) AS srv (s)
CROSS JOIN (VALUES ('hash1'), ('hash2'), ('hash3'), ('hashX'), (NULL)) AS k (hash);
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum)
SELECT hour, wide_rows, mem_max + 5, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum
FROM collect.query_store_hash_hourly WHERE server_id = 1 AND query_hash = 'hash1' AND hour >= TIMESTAMP '{At(day.AddHours(6))}' AND hour < TIMESTAMP '{At(day.AddHours(9))}';
INSERT INTO collect.query_store_hash_daily (day, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum)
SELECT TIMESTAMP '{At(day)}', 48, 999, srv.s, k.hash, 5000 + srv.s, 600000 + srv.s, 7000000 + srv.s, 80000 + srv.s, 9000 + srv.s
FROM (VALUES (1), (2)) AS srv (s)
CROSS JOIN (VALUES ('hash1'), ('hash2'), ('hash3'), ('hashX'), (NULL)) AS k (hash)", ct);
            var builtHours = new HashSet<DateTime>();
            for (var hour = day; hour < a.HourNow.AddHours(1); hour = hour.AddHours(1))
            {
                builtHours.Add(hour);
            }

            var stale = new HashSet<(DateTime Hour, int ServerId)> { (day.AddHours(5), 3), (a.HourNow.AddHours(-3), 2) };
            var validDays = new HashSet<(DateTime Day, int ServerId)> { (day, 1), (day, 2) };
            var planTwo = QueryTagRollupHashRoute.Split(new Dictionary<int, DateTime> { [1] = day, [2] = day, [3] = day }, a.HourNow, builtHours, stale, validDays);
            Assert.Equal(2, planTwo.DayPairs.Count);
            Assert.Contains(planTwo.TablePairs, p => p.Hour == day.AddHours(4) && p.ServerId == 3);
            Assert.DoesNotContain(planTwo.TablePairs, p => p.Hour == day.AddHours(5) && p.ServerId == 3);
            Assert.DoesNotContain(planTwo.TablePairs, p => p.Hour == day.AddHours(4) && p.ServerId == 1);
            await AssertSameAsTheListOfPairsAsync(a, planTwo, ct);
            await AssertSameAsThePerArmGroupedStatementAsync(a, planTwo, ct);

            /* Plan three (#7365): two servers take the day from the day table, so the hourly arm has two ranges, the hours before the day and the
               hours after it, and the day's own hourly rows (still in the table, seeded above for both servers) lie between the first range's
               start and the last range's end but in no range. The plain bounds alone would read them; the range filter must not. */
            var aroundTheDay = new Dictionary<int, DateTime> { [1] = day.AddHours(-3), [2] = day.AddHours(-3) };
            var allHours = new HashSet<DateTime>();
            for (var hour = day.AddHours(-3); hour < a.HourNow.AddHours(1); hour = hour.AddHours(1))
            {
                allHours.Add(hour);
            }

            var planThree = QueryTagRollupHashRoute.Split(
                aroundTheDay, a.HourNow, allHours, new HashSet<(DateTime Hour, int ServerId)>(), new HashSet<(DateTime Day, int ServerId)> { (day, 1), (day, 2) });
            Assert.Equal(2, planThree.DayPairs.Count);
            var hourlyThree = QueryTagRollupHashRoute.HourlyReadFor(planThree, a.End);
            Assert.True(hourlyThree.RangeFrom.Length >= 2, "the day table's day must split the hourly arm into two ranges, got " + hourlyThree.RangeFrom.Length);
            var between = await ScalarAsync(a.Connection, $@"
SELECT count(*) FROM collect.query_store_hash_hourly
WHERE server_id IN (1, 2) AND hour >= TIMESTAMP '{At(day)}' AND hour < TIMESTAMP '{At(day.AddHours(24))}'", ct);
            Assert.True(between > 0, "the day's hourly rows must sit between the ranges for the test to mean anything");
            await AssertSameAsThePerArmGroupedStatementAsync(a, planThree, ct);
            await AssertSameAsTheListOfPairsAsync(a, planThree, ct);

            /* Plan four: no hour is read from the table, so the hourly arm has no ranges and its bounds are NULL. */
            var planFour = QueryTagRollupHashRoute.Split(
                new Dictionary<int, DateTime> { [1] = a.Start, [2] = a.Start }, a.HourNow, new HashSet<DateTime>(), new HashSet<(DateTime Hour, int ServerId)>());
            Assert.Empty(planFour.TablePairs);
            await AssertSameAsThePerArmGroupedStatementAsync(a, planFour, ct);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    private static async Task AssertSameAsTheListOfPairsAsync(Arranged a, QueryTagRollupHashRoute.Plan plan, CancellationToken ct)
    {
        var groups = plan.WideRanges
            .GroupBy(r => (r.From, r.To))
            .OrderBy(g => g.Key.From)
            .ThenBy(g => g.Key.To)
            .Select(g => (g.Key.From, g.Key.To, Servers: g.Select(r => r.ServerId).OrderBy(s => s).ToArray()))
            .ToList();
        await using var old = new NpgsqlCommand(ListOfPairsSqlFor(groups.Count, plan.UsesDays), a.Connection) { CommandTimeout = 120 };
        old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = plan.TablePairs.Select(p => p.Hour).ToArray() });
        old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = plan.TablePairs.Select(p => p.ServerId).ToArray() });
        old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(a.End, DateTimeKind.Unspecified) });
        if (plan.UsesDays)
        {
            old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = plan.DayPairs.Select(p => p.Day).ToArray() });
            old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = plan.DayPairs.Select(p => p.ServerId).ToArray() });
        }

        foreach (var group in groups)
        {
            old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = group.Servers });
            old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(group.From, DateTimeKind.Unspecified) });
            old.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(group.To, DateTimeKind.Unspecified) });
        }

        await using var current = QueryTagRollupHashRoute.Command(a.Connection, plan, a.End, 120);
        var expected = await ReadFactsAsync(old, ct);
        var actual = await ReadFactsAsync(current, ct);
        Assert.True(expected.Count >= 6, "the reference read must return several (server, tag) rows, got " + expected.Count);
        Assert.Contains(expected, f => f.TagId is null);
        Assert.True(expected.Select(f => f.TagId).Distinct().Count() >= 3, "the reference must hold two tags and the untagged rows");
        Assert.Equal(Canon(expected), Canon(actual));
    }

    private static async Task<List<QueryTagRollupFact>> ReadFactsAsync(NpgsqlCommand command, CancellationToken ct)
    {
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

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The routed statement (#7365: one hourly arm for each range, plain bounds) against the one before it (#7358), where the hourly arm was
    /// driven by <c>unnest</c> of the ranges. The reference takes the routed command's parameters in the old numbering.
    /// </summary>
    private static async Task AssertSameAsThePerArmGroupedStatementAsync(Arranged a, QueryTagRollupHashRoute.Plan plan, CancellationToken ct)
    {
        var groups = plan.WideRanges.Select(r => (r.From, r.To)).Distinct().Count();
        await using var flat = QueryTagRollupHashRoute.Command(a.Connection, plan, a.End, 120);
        await using var reference = new NpgsqlCommand(PerArmGroupedSqlFor(groups, plan.UsesDays), a.Connection) { CommandTimeout = 120 };
        /* The reference keeps the old numbering: the servers, then the ranges as two arrays, then the gaps, the window end and the rest. The
           routed command has the servers and gaps first and the ranges as pairs at the end (#7365). */
        var hourly = QueryTagRollupHashRoute.HourlyReadFor(plan, a.End);
        var routedCount = flat.Parameters.Count - (2 * Math.Max(1, hourly.RangeFrom.Length));
        NpgsqlParameter Copy(int index) => new() { NpgsqlDbType = flat.Parameters[index].NpgsqlDbType, Value = flat.Parameters[index].Value };
        reference.Parameters.Add(Copy(0));
        reference.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = hourly.RangeFrom });
        reference.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = hourly.RangeTo });
        for (var i = 1; i < routedCount; i++)
        {
            reference.Parameters.Add(Copy(i));
        }

        var expected = await ReadFactsAsync(reference, ct);
        var actual = await ReadFactsAsync(flat, ct);
        Assert.NotEmpty(expected);

        /* All six measures (cpu, duration, reads, writes, executions, peak memory), compared exactly: the arms add exact numeric sums, so the
           flat statement's totals are not just within a tolerance of the grouped one's, they are the same doubles. */
        Assert.Equal(6, QueryTagRollupSection.QueryStore.Measures.Count);
        Assert.Equal(Canon(expected), Canon(actual));
    }

    /// <summary>The routed statement as #7358 (70084d98a) left it: each arm grouped by (server, tag), the hourly arm driven by <c>unnest</c> of the ranges.</summary>
    private static string PerArmGroupedSqlFor(int wideRanges, bool days)
    {
        var first = days ? 10 : 8;
        var text = new System.Text.StringBuilder();
        text.Append(@"
WITH partials AS
(
    SELECT t.server_id, m.tag_id,
           SUM(t.ec_sum) AS ec_sum, SUM(t.cpu_wsum) AS cpu_wsum, SUM(t.dur_wsum) AS dur_wsum,
           SUM(t.reads_wsum) AS reads_wsum, SUM(t.writes_wsum) AS writes_wsum, MAX(t.mem_max) AS mem_max
    FROM unnest($2::timestamp[], $3::timestamp[]) AS r(range_from, range_to)
    JOIN collect.query_store_hash_hourly AS t
           ON t.hour >= r.range_from
          AND t.hour < r.range_to
    LEFT JOIN config.query_tag_map AS m
           ON m.server_id = t.server_id AND m.key_kind = 'hash' AND m.key = t.query_hash
    WHERE t.server_id = ANY($1::integer[])
    AND   NOT EXISTS
          (
              SELECT 1
              FROM unnest($4::integer[], $5::timestamp[], $6::timestamp[]) AS g(server_id, gap_from, gap_to)
              WHERE g.server_id = t.server_id
              AND   t.hour >= g.gap_from
              AND   t.hour < g.gap_to
          )
    GROUP BY t.server_id, m.tag_id
");
        if (days)
        {
            text.Append(@"
    UNION ALL
    SELECT t.server_id, m.tag_id,
           SUM(t.ec_sum), SUM(t.cpu_wsum), SUM(t.dur_wsum), SUM(t.reads_wsum), SUM(t.writes_wsum), MAX(t.mem_max)
    FROM unnest($8::timestamp[], $9::integer[]) AS p(day, server_id)
    JOIN collect.query_store_hash_daily AS t
           ON t.day = p.day
          AND t.server_id = p.server_id
    LEFT JOIN config.query_tag_map AS m
           ON m.server_id = t.server_id AND m.key_kind = 'hash' AND m.key = t.query_hash
    GROUP BY t.server_id, m.tag_id
");
        }

        for (var arm = 0; arm < wideRanges; arm++)
        {
            var servers = "$" + (first + (3 * arm)).ToString(CultureInfo.InvariantCulture);
            var from = "$" + (first + 1 + (3 * arm)).ToString(CultureInfo.InvariantCulture);
            var to = "$" + (first + 2 + (3 * arm)).ToString(CultureInfo.InvariantCulture);
            text.Append($@"
    UNION ALL
    SELECT q.server_id, m.tag_id,
           SUM(q.execution_count), SUM(q.avg_cpu_time_us * q.execution_count), SUM(q.avg_duration_us * q.execution_count),
           SUM(q.avg_logical_io_reads * q.execution_count), SUM(q.avg_logical_io_writes * q.execution_count),
           MAX(q.max_query_max_used_memory)
    FROM collect.query_store_interval_wide AS q
    LEFT JOIN config.query_tag_map AS m
           ON m.server_id = q.server_id AND m.key_kind = 'hash' AND m.key = q.query_hash
    WHERE q.server_id = ANY({servers}::integer[])
    AND   q.collection_time >= {from}::timestamp
    AND   q.collection_time < {to}::timestamp
    AND   q.collection_time <= $7
    AND   q.first_execution_time >= {from}::timestamp - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
    AND   q.first_execution_time <= LEAST({to}::timestamp, $7) + {QueryStoreIntervalWide.FirstExecUpperSlackSql}
    GROUP BY q.server_id, m.tag_id
");
        }

        text.Append(@"
)
SELECT p.server_id, p.tag_id,
       CAST(COALESCE(SUM(p.cpu_wsum), 0) AS double precision) AS cpu_ms,
       CAST(COALESCE(SUM(p.dur_wsum), 0) AS double precision) AS duration_ms,
       CAST(COALESCE(SUM(p.reads_wsum), 0) AS double precision) AS logical_reads,
       CAST(COALESCE(SUM(p.writes_wsum), 0) AS double precision) AS logical_writes,
       CAST(COALESCE(SUM(p.ec_sum), 0) AS double precision) AS executions,
       CAST(COALESCE(MAX(p.mem_max), 0) AS double precision) AS peak_memory_pages
FROM partials AS p
GROUP BY p.server_id, p.tag_id");
        return text.ToString();
    }

    /// <summary>
    /// The routed statement as it was before #7240 step 4, kept as the reference: $1 and $2 the table pairs as parallel arrays, $3 the window
    /// end, the day pairs at $4 and $5 when <paramref name="days"/>, then three parameters per wide range. The hourly arm is the
    /// <c>unnest</c> of every pair joined to the table, with no grouping before the <c>UNION ALL</c>.
    /// </summary>
    private static string ListOfPairsSqlFor(int wideRanges, bool days)
    {
        var first = days ? 6 : 4;
        var text = new System.Text.StringBuilder();
        text.Append(@"
WITH partials AS
(
    SELECT t.server_id, t.query_hash, t.ec_sum, t.cpu_wsum, t.dur_wsum, t.reads_wsum, t.writes_wsum, t.mem_max
    FROM unnest($1::timestamp[], $2::integer[]) AS p(hour, server_id)
    JOIN collect.query_store_hash_hourly AS t
           ON t.hour = p.hour
          AND t.server_id = p.server_id
");
        if (days)
        {
            text.Append(@"
    UNION ALL
    SELECT t.server_id, t.query_hash, t.ec_sum, t.cpu_wsum, t.dur_wsum, t.reads_wsum, t.writes_wsum, t.mem_max
    FROM unnest($4::timestamp[], $5::integer[]) AS p(day, server_id)
    JOIN collect.query_store_hash_daily AS t
           ON t.day = p.day
          AND t.server_id = p.server_id
");
        }

        for (var arm = 0; arm < wideRanges; arm++)
        {
            var servers = "$" + (first + (3 * arm)).ToString(CultureInfo.InvariantCulture);
            var from = "$" + (first + 1 + (3 * arm)).ToString(CultureInfo.InvariantCulture);
            var to = "$" + (first + 2 + (3 * arm)).ToString(CultureInfo.InvariantCulture);
            text.Append($@"
    UNION ALL
    SELECT q.server_id, q.query_hash,
           SUM(q.execution_count) AS ec_sum,
           SUM(q.avg_cpu_time_us * q.execution_count) AS cpu_wsum,
           SUM(q.avg_duration_us * q.execution_count) AS dur_wsum,
           SUM(q.avg_logical_io_reads * q.execution_count) AS reads_wsum,
           SUM(q.avg_logical_io_writes * q.execution_count) AS writes_wsum,
           MAX(q.max_query_max_used_memory) AS mem_max
    FROM collect.query_store_interval_wide AS q
    WHERE q.server_id = ANY({servers}::integer[])
    AND   q.collection_time >= {from}::timestamp
    AND   q.collection_time < {to}::timestamp
    AND   q.collection_time <= $3
    AND   q.first_execution_time >= {from}::timestamp - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
    AND   q.first_execution_time <= LEAST({to}::timestamp, $3) + {QueryStoreIntervalWide.FirstExecUpperSlackSql}
    GROUP BY q.server_id, q.query_hash
");
        }

        text.Append(@"
)
SELECT p.server_id, m.tag_id,
       CAST(COALESCE(SUM(p.cpu_wsum), 0) AS double precision) AS cpu_ms,
       CAST(COALESCE(SUM(p.dur_wsum), 0) AS double precision) AS duration_ms,
       CAST(COALESCE(SUM(p.reads_wsum), 0) AS double precision) AS logical_reads,
       CAST(COALESCE(SUM(p.writes_wsum), 0) AS double precision) AS logical_writes,
       CAST(COALESCE(SUM(p.ec_sum), 0) AS double precision) AS executions,
       CAST(COALESCE(MAX(p.mem_max), 0) AS double precision) AS peak_memory_pages
FROM partials AS p
LEFT JOIN config.query_tag_map AS m
       ON m.server_id = p.server_id AND m.key_kind = 'hash' AND m.key = p.query_hash
GROUP BY p.server_id, m.tag_id");
        return text.ToString();
    }
}
