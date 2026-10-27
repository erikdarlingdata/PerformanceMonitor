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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800 part 2, step 3c: the fleet Query Store tag roll-up also takes whole UTC days from <c>collect.query_store_hash_daily</c>
/// (<see cref="QueryTagRollupHashRoute"/>), against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). The answer must equal the single wide
/// statement's (<see cref="QueryTagRollupSql.QueryStoreSql"/>) value for value over the window the route read, for a seed whose first
/// executions are all within 12 hours of the window end: a row first executed later than that is dropped by the wide statement's
/// <c>first_execution_time</c> bound and kept by the tables, the one difference (see <see cref="QueryTagRollupHashRoute"/>). The seed is nine days of
/// wide rows for four servers (a stamp every 20 minutes, six queries, two plans, a NULL hash, products past a bigint), every hour and every
/// whole day built, the anchored end (<see cref="LiveClock.AnchoredEnd"/>) 22:00 yesterday, so the newest day is open.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryTagRollupHashRouteDailyLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the V221 day route facts.";

    private const int RetentionDays = 30;

    private static readonly int[] Servers = [1, 2, 3, 4];

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) => ComposeStampLiveSupport.ExecAsync(connection, sql, ct);

    private static string At(DateTime value) => ComposeStampLiveSupport.At(value);

    private sealed record Arranged(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime HourNow)
    {
        /// <summary>The newest UTC midnight before the anchored end (yesterday 00:00): the day of the open hours.</summary>
        public DateTime Newest => HourNow.Date;

        /// <summary>The window end, past the coverage claim's applied_through (30 minutes into the open hour), as the gate wants.</summary>
        public DateTime End => HourNow.AddMinutes(40);

        /// <summary>An unaligned start <paramref name="days"/> days back.</summary>
        public DateTime StartDaysBack(int days) => HourNow.AddDays(-days).AddMinutes(17);
    }

    private static async Task BuildEverythingAsync(NpgsqlDataSource source, DateTime hourNow, CancellationToken ct)
    {
        var at = ComposeStampLiveSupport.ClockWithin(hourNow);
        for (var guard = 0; guard < 400; guard++)
        {
            var tick = await QueryStoreComposeStamp.RunTickAsync(
                source, at, RetentionDays, NullLogger.Instance, () => TimeSpan.Zero, ct, new QueryStoreComposeStamp.TickOptions(StoreNow: at));
            Assert.Equal(0, tick.Failed + tick.DaysFailed);
            if (tick.Built == 0 && tick.DaysBuilt == 0)
            {
                return;
            }
        }

        throw new InvalidOperationException("the builders never ran out of work");
    }

    private static async Task<Arranged> ArrangeAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        try
        {
            return await ArrangeIntoAsync(scratch, ct);
        }
        catch
        {
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task<Arranged> ArrangeIntoAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var hourNow = LiveClock.AnchoredEnd();
        foreach (var server in Servers)
        {
            await ComposeStampLiveSupport.EnrollAsync(connection, server, hourNow, ct);
        }

        await ExecAsync(connection, $"UPDATE collect.query_store_interval_wide_coverage SET filled_since = TIMESTAMP '{At(hourNow.AddDays(-12))}'", ct);
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, replica_role,
 runtime_stats_interval_id, interval_start_time_utc, avg_logical_io_reads, avg_logical_io_writes, max_query_max_used_memory)
SELECT t.ct, s.server_id, 'db' || (q % 2), q, q * 10 + p,
       CASE WHEN (h + m + q + p) % 11 = 0 THEN 'Aborted' WHEN (h + m + q + p) % 13 = 0 THEN 'Exception' ELSE 'Regular' END,
       t.ct - interval '3 minutes', t.ct,
       CASE WHEN q IN (3, 6) THEN NULL ELSE 'mod' || (q % 3) END,
       CASE WHEN q = 5 THEN NULL ELSE 'hash' || q END,
       CASE WHEN (h + m + p) % 17 = 0 AND q <> 1 THEN NULL WHEN q = 1 THEN 2000000 ELSE ((h + m + q + p) * 100 + 1)::bigint END,
       CASE WHEN (h + m + q) % 7 = 0 AND q <> 1 THEN NULL WHEN q = 1 THEN 3000000000000::bigint ELSE ((h * 1000 + m * 37 + q * 13 + p) * 1000)::bigint END,
       CASE WHEN (h + q) % 5 = 0 AND q <> 1 THEN NULL ELSE ((h + m + q) * 523 + p)::bigint END,
       ((h + 1) * 997 + m + q)::bigint, CASE WHEN (m + q) % 4 = 0 THEN NULL ELSE ((h + 2) * 311 + m * q)::bigint END,
       NULL,
       ((((h * 3 + m) * 4 + s.k) * 6 + q) * 3 + p), t.ct - interval '10 minutes',
       CASE WHEN q % 4 = 0 THEN NULL ELSE ((q * 7 + (q * 10 + p)) % 1000) + 1 END,
       CASE WHEN q % 3 = 0 THEN NULL ELSE q * 5 + ((q * 10 + p) % 7) END,
       CASE WHEN q % 5 = 0 THEN NULL ELSE (((((h * 3 + m) * 4 + s.k) * 6 + q) * 3 + p) % 5000) + 1 END
FROM generate_series(0, {(9 * 24) - 1}) AS h
CROSS JOIN generate_series(0, 2) AS m
CROSS JOIN (VALUES (1, 0), (2, 1), (3, 2), (4, 3)) AS s (server_id, k)
CROSS JOIN generate_series(1, 6) AS q
CROSS JOIN generate_series(1, 2) AS p
CROSS JOIN LATERAL (SELECT TIMESTAMP '{At(hourNow)}' - interval '9 days' + h * interval '1 hour' + m * interval '20 minutes' AS ct) AS t", ct);
        await ExecAsync(connection, @"
WITH t AS (INSERT INTO config.query_tags (name) VALUES ('tag-a'), ('tag-b') RETURNING id, name)
INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
SELECT m.server_id, 'hash', m.key, t.id, 'hand_hash'
FROM t
JOIN (VALUES (1, 'hash1', 'tag-a'), (1, 'hash2', 'tag-b'), (2, 'hash1', 'tag-a'), (3, 'hash3', 'tag-b'), (4, 'hash4', 'tag-a')) AS m (server_id, key, name) ON m.name = t.name", ct);
        var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        await BuildEverythingAsync(source, hourNow, ct);

        /* The table's own floor, a purge margin below the oldest window (a plain store's gate wants it). */
        foreach (var server in Servers)
        {
            await ComposeStampLiveSupport.InsertRowAsync(connection, server, hourNow.AddDays(-10), 900_000 + server, ct);
        }

        return new Arranged(scratch, connection, source, hourNow);
    }

    private static Task CleanupAsync(Arranged a, bool bodySucceeded) => ComposeStampLiveSupport.CleanupAsync(a.Scratch, a.Connection, a.Source, bodySucceeded);

    private static async Task<DarlingQueryTagRollupReader.QueryStoreRead> FleetAsync(Arranged a, DateTime start, DateTime end, CancellationToken ct)
    {
        var read = await DarlingQueryTagRollupReader.ReadQueryStoreAsync(a.Source, Servers, start, end, null, ct);
        Assert.True(Servers.Length == read.Allowed.Count, "the gate refused the window " + start.ToString("o", CultureInfo.InvariantCulture) + ": " + string.Join("; ", read.Refused.Values.Distinct()));
        return read;
    }

    private static Dictionary<int, DateTime> Snapped(DarlingQueryTagRollupReader.QueryStoreRead read) => read.RoutedStarts ?? read.Allowed;

    /// <summary>The single wide statement, the way the reader builds it, for the starts the read began at.</summary>
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

    /// <summary>The routed read equals today's statement over the window it read, and the plan the same snapshot logic gives is returned for inspection.</summary>
    private static async Task<QueryTagRollupHashRoute.Plan> AssertRoutedEqualsTodaysAsync(Arranged a, DateTime start, DateTime end, string what, CancellationToken ct)
    {
        var read = await FleetAsync(a, start, end, ct);
        Assert.True(read.UsedHashHourly, what);
        Assert.NotEmpty(read.Facts);
        Assert.Equal(Canon(await TodaysAsync(a, Snapped(read), end, ct)), Canon(read.Facts));
        var plan = await QueryTagRollupHashRoute.TryPlanAsync(a.Connection, Servers.ToDictionary(s => s, _ => start), end, 120, ct);
        Assert.NotNull(plan);
        Assert.Equal(plan.UsesDays, read.UsedHashDaily);
        return plan;
    }

    private static string DaysOf(QueryTagRollupHashRoute.Plan plan, int server, DateTime newest) =>
        string.Join(",", plan.DayPairs.Where(p => p.ServerId == server).OrderBy(p => p.Day).Select(p => (int)(p.Day - newest).TotalDays));

    /// <summary>
    /// For a 2 and a 7 day window the routed answer equals today's statement value for value, with every whole day valid, then with a day
    /// that was never built, a day invalidated by a late write for one server, and a window that holds the open newest day whole.
    /// </summary>
    [Fact]
    public async Task TheFleetRead_WithDaysEqualsTheWideStatementValueForValue_AcrossValidInvalidAndIncompleteDays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Every day D-8 to D-1 is built (D is the newest, open day). A 2 day window holds one whole day, a 7 day window six. */
            var two = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(2), a.End, "2 days, every day valid", ct);
            Assert.Equal(4, two.DayPairs.Count);
            Assert.All(Servers, s => Assert.Equal("-1", DaysOf(two, s, a.Newest)));
            var seven = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(7), a.End, "7 days, every day valid", ct);
            Assert.Equal(4 * 6, seven.DayPairs.Count);
            Assert.All(Servers, s => Assert.Equal("-6,-5,-4,-3,-2,-1", DaysOf(seven, s, a.Newest)));

            /* A day that was never built (no day rows, no ledger rows): every server reads that day from its hours. */
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_daily WHERE day = TIMESTAMP '{At(a.Newest.AddDays(-5))}'", ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_daily_built WHERE day = TIMESTAMP '{At(a.Newest.AddDays(-5))}'", ct);
            seven = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(7), a.End, "7 days, one day never built", ct);
            Assert.All(Servers, s => Assert.Equal("-6,-4,-3,-2,-1", DaysOf(seven, s, a.Newest)));
            Assert.Equal(24 * 4, seven.TablePairs.Count(p => p.Hour >= a.Newest.AddDays(-5) && p.Hour < a.Newest.AddDays(-4)));

            /* A late write into one hour of day D-3 for server 2: that server's day is invalid at once, before any rebuild. */
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 2, a.Newest.AddDays(-3).AddHours(10).AddMinutes(7), 700_001, ct);
            seven = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(7), a.End, "7 days, a late write for one server", ct);
            Assert.Equal("-6,-4,-3,-2,-1", DaysOf(seven, 1, a.Newest));
            Assert.Equal("-6,-4,-2,-1", DaysOf(seven, 2, a.Newest));
            Assert.Equal("-6,-4,-3,-2,-1", DaysOf(seven, 3, a.Newest));
            Assert.Equal("-6,-4,-3,-2,-1", DaysOf(seven, 4, a.Newest));
            two = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(2), a.End, "2 days, a late write in another day", ct);
            Assert.Equal(4, two.DayPairs.Count);

            /* The newest day is whole inside a window that ends after midnight, but its last hours are not built: it is not read as a day. */
            var late = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(7), a.HourNow.AddHours(2).AddMinutes(20), "7 days, the open newest day inside the window", ct);
            Assert.DoesNotContain(late.DayPairs, p => p.Day >= a.Newest);
            Assert.NotEmpty(late.DayPairs);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// A day invalid for one server is read from that server's hours and wide ranges only: the other servers keep the day, and the answer is
    /// today's. The stale hour is a wide range for the server, its other 23 hours are table hours, and no hour of a taken day is read twice.
    /// </summary>
    [Fact]
    public async Task ADayInvalidForOneServer_IsReadFromHoursAndWideForThatServerOnly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var day = a.Newest.AddDays(-2);
            var staleHour = day.AddHours(5);
            await ComposeStampLiveSupport.InsertRowAsync(a.Connection, 3, staleHour.AddMinutes(11), 700_002, ct);
            var plan = await AssertRoutedEqualsTodaysAsync(a, a.StartDaysBack(4), a.End, "4 days, a late write for server 3", ct);

            Assert.DoesNotContain(plan.DayPairs, p => p.ServerId == 3 && p.Day == day);
            Assert.All(new[] { 1, 2, 4 }, s => Assert.Contains(plan.DayPairs, p => p.ServerId == s && p.Day == day));
            Assert.Equal(23, plan.TablePairs.Count(p => p.ServerId == 3 && p.Hour >= day && p.Hour < day.AddDays(1)));
            Assert.DoesNotContain(plan.TablePairs, p => p.ServerId == 3 && p.Hour == staleHour);
            Assert.Contains(plan.WideRanges, r => r.ServerId == 3 && r.From == staleHour && r.To == staleHour.AddHours(1));
            Assert.All(new[] { 1, 2, 4 }, s => Assert.DoesNotContain(plan.TablePairs, p => p.ServerId == s && p.Hour >= day && p.Hour < day.AddDays(1)));
            Assert.All(new[] { 1, 2, 4 }, s => Assert.DoesNotContain(plan.WideRanges, r => r.ServerId == s && r.From >= day && r.From < day.AddDays(1)));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    /// <summary>
    /// The answer for a valid day comes from the day table: deleting that day's wide rows and hourly rows (no DELETE trigger, so every
    /// pair and the ledger stay valid) leaves the routed read unchanged and moves today's statement. Through the tool the section names the day
    /// table and says <c>daily+hourly+tail</c>, and the Viewer's note names the daily and hourly tables.
    /// </summary>
    [Fact]
    public async Task AValidDaysAnswerComesFromTheDayTable_AndTheSectionAndTheViewerSayTheyReadDays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var a = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var start = a.StartDaysBack(7);
            var before = await FleetAsync(a, start, a.End, ct);
            Assert.True(before.UsedHashDaily);
            var beforeText = Canon(before.Facts);

            var day = a.Newest.AddDays(-4);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(day)}' AND collection_time < TIMESTAMP '{At(day.AddDays(1))}'", ct);
            await ExecAsync(a.Connection, $"DELETE FROM collect.query_store_hash_hourly WHERE hour >= TIMESTAMP '{At(day)}' AND hour < TIMESTAMP '{At(day.AddDays(1))}'", ct);
            var after = await FleetAsync(a, start, a.End, ct);
            Assert.True(after.UsedHashDaily);
            Assert.Equal(beforeText, Canon(after.Facts));
            Assert.NotEqual(beforeText, Canon(await TodaysAsync(a, Snapped(after), a.End, ct)));

            var toolStart = a.End.AddHours(-168);
            using var doc = JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(
                a.Source, null, 168, null, 25, a.End.ToString("o", CultureInfo.InvariantCulture) + "Z", null, ct));
            var section = doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == QueryTagRollupSection.QueryStore.Name);
            Assert.Equal("daily+hourly+tail", section.GetProperty("tier").GetString());
            Assert.Equal(QueryTagRollupHashRoute.RoutedSourceNameWithDays, section.GetProperty("source").GetString());
            Assert.Contains("collect.query_store_hash_daily", section.GetProperty("source").GetString(), StringComparison.Ordinal);

            await using var viewer = new ViewerDataService(a.Scratch.ConnectionString);
            var viewed = await viewer.GetQueryTagRollupDataAsync(Servers, toolStart, a.End, ct);
            Assert.StartsWith("Read from the daily and hourly Query Store tables, with the raw intervals for the edges, from ", viewed.Notes[QueryTagRollupSection.QueryStore.Name], StringComparison.Ordinal);

            /* A window without a whole day reads hours only, and says so in the older words. */
            var hourly = QueryStoreSectionOf(JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(
                a.Source, null, 24, null, 25, a.End.ToString("o", CultureInfo.InvariantCulture) + "Z", null, ct)));
            Assert.Equal("hourly+tail", hourly.GetProperty("tier").GetString());
            Assert.Equal(QueryTagRollupHashRoute.RoutedSourceName, hourly.GetProperty("source").GetString());
            var viewedHourly = await viewer.GetQueryTagRollupDataAsync(Servers, a.End.AddHours(-24), a.End, ct);
            Assert.StartsWith("Read from the hourly Query Store table, with the raw intervals for the edges, from ", viewedHourly.Notes[QueryTagRollupSection.QueryStore.Name], StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(a, bodySucceeded);
        }
    }

    private static JsonElement QueryStoreSectionOf(JsonDocument doc) =>
        doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == QueryTagRollupSection.QueryStore.Name);
}
