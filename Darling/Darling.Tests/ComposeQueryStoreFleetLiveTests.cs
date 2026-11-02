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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7146 (part 3 of #5506): the fleet rollup of the compose stamp, V239. Fleet use is proved as behaviour: the fleet rows are multiplied in
/// the store, so a read that takes any of them gives a different answer than the oracle's, and a read that does not take them still gives
/// the oracle's. Each case runs on the 28-hour seed with every hour built (the builder fills the fleet table in the stamp's transaction).
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class ComposeQueryStoreFleetLiveTests
{
    private const string HashPanel = "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":4,\"groupBy\":[\"query_hash\"],\"timeBucket\":\"hour\",\"viz\":\"line\"}";
    private const string TotalPanel = "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"viz\":\"stat\"}";
    private const string FilteredPanel = "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":4,\"groupBy\":[\"query_hash\"],\"timeBucket\":\"hour\",\"viz\":\"line\","
        + "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"like\",\"value\":\"%\"}]}";
    private const string ModulePanel = "{\"source\":\"query_store_stats\",\"measure\":\"qs_max_duration_us\",\"aggregate\":\"max\",\"topN\":5,\"groupBy\":[\"module_name\"],\"viz\":\"bar\"}";

    private static string Hour(DateTime h) => ComposeStampLiveSupport.At(h);

    /* Multiplies the fleet rows of [from, to) so a read that takes them differs from the oracle. */
    private static Task CorruptFleetAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct) => ComposeStampLiveSupport.ExecAsync(connection,
        $"UPDATE collect.query_store_compose_fleet SET ec_sum = ec_sum * 3 + 7, cpu_wsum = cpu_wsum * 3 + 7, maxdur_max = maxdur_max * 3 + 7, wide_rows = wide_rows * 3 " +
        $"WHERE collection_time >= TIMESTAMP '{Hour(from)}' AND collection_time < TIMESTAMP '{Hour(to)}'", ct);

    private static async Task<QueryStoreRankedHarness.Comparison> CompareAsync(
        NpgsqlConnection connection, string json, DateTime start, DateTime end, DateTime through, bool fleetReady, IReadOnlyList<string>? scope, CancellationToken ct)
    {
        var candidate = QueryStoreRankedHarness.WideContext(start, end, scope) with { QueryStoreStampThrough = through, QueryStoreFleetReady = fleetReady };
        var oracle = QueryStoreRankedHarness.WideContext(start, end, scope);
        return await QueryStoreRankedHarness.CompareAsync(connection, json, candidate, QueryStoreRankedHarness.Product, oracle, QueryStoreRankedHarness.Oracle, 0d, ct);
    }

    [Fact]
    public async Task TheFleetPathGivesTheStampPathsAnswer_AndOnlyAnUnfilteredQueryHashPanelWithNoServerScopeTakesTheFleetRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = hourNow.AddHours(-2);
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_fleet", ct) > 0);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE fleet_built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT hour FROM collect.query_store_compose_stamp_hours)", ct));

            /* 1. The same answer: the fleet path, the stamp path (fleet not ready) and the oracle agree, with values, order and members. */
            foreach (var json in new[] { HashPanel, TotalPanel })
            {
                var viaFleet = await CompareAsync(connection, json, start, end, through, true, null, ct);
                Assert.True(viaFleet.Equal, $"fleet | {json}: {viaFleet.Difference}");
                Assert.True(viaFleet.OracleRows > 0, json);
                var viaStamp = await CompareAsync(connection, json, start, end, through, false, null, ct);
                Assert.True(viaStamp.Equal, $"stamp | {json}: {viaStamp.Difference}");
            }

            /* A removed server only flips is_enabled (DarlingObservability.DisableOrphanedServersSql): its registry, stamp, ledger and fleet rows stay, so
               the fleet path still counts its history and still gives the oracle's answer. */
            await ComposeStampLiveSupport.ExecAsync(connection, "UPDATE collect.servers SET is_enabled = FALSE WHERE server_id = 3", ct);
            var disabled = await CompareAsync(connection, HashPanel, start, end, through, true, null, ct);
            Assert.True(disabled.Equal, $"fleet, one server disabled: {disabled.Difference}");

            /* 2. Fleet use is behaviour: with every fleet row multiplied, the fleet path differs from the oracle and nothing else does. */
            await CorruptFleetAsync(connection, hourNow.AddHours(-30), hourNow.AddHours(1), ct);
            Assert.False((await CompareAsync(connection, HashPanel, start, end, through, true, null, ct)).Equal);
            Assert.False((await CompareAsync(connection, TotalPanel, start, end, through, true, null, ct)).Equal);
            Assert.True((await CompareAsync(connection, HashPanel, start, end, through, false, null, ct)).Equal);          /* store below V239 */
            Assert.True((await CompareAsync(connection, HashPanel, start, end, through, true, new[] { "srv1" }, ct)).Equal);  /* a server scope */
            Assert.True((await CompareAsync(connection, FilteredPanel, start, end, through, true, null, ct)).Equal);        /* a filter */
            Assert.True((await CompareAsync(connection, ModulePanel, start, end, through, true, null, ct)).Equal);          /* a group the fleet table lacks */

            /* A server on the raw route ($4 not empty) reads no fleet rows either. */
            var split = QueryStoreRankedHarness.WideContext(start, end) with { QueryStoreStampThrough = through, QueryStoreFleetReady = true, QueryStoreRawServerIds = new[] { 3 } };
            var splitOracle = QueryStoreRankedHarness.WideContext(start, end) with { QueryStoreRawServerIds = new[] { 3 } };
            var splitResult = await QueryStoreRankedHarness.CompareAsync(connection, HashPanel, split, QueryStoreRankedHarness.Product, splitOracle, QueryStoreRankedHarness.Oracle, 0d, ct);
            Assert.True(splitResult.Equal, splitResult.Difference);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AnHourWithOneStalePair_ReadsTheStampPathForThatHourOnly_AndTheAnswerStillMatches_AndALateRebuildLeavesTheFleetRowsAFreshAggregate()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = hourNow.AddHours(-2);   /* the lookup's answer from before the write: the stale arm covers the race */
            var staleHour = hourNow.AddHours(-10);
            var otherHour = hourNow.AddHours(-6);

            /* A replayed row lands in a built hour: its pair turns stale, the fleet rows of the hour are now short of the wide table. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 1, staleHour.AddMinutes(55), 9_000_001, ct);
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT hour FROM collect.query_store_compose_stamp_hours)", ct));
            Assert.True((await CompareAsync(connection, HashPanel, start, end, through, true, null, ct)).Equal);
            Assert.True((await CompareAsync(connection, TotalPanel, start, end, through, true, null, ct)).Equal);

            /* The stale hour's fleet rows are not read (multiplied, the answer holds). Another hour's are (multiplied, the answer breaks). */
            await CorruptFleetAsync(connection, staleHour, staleHour.AddHours(1), ct);
            Assert.True((await CompareAsync(connection, HashPanel, start, end, through, true, null, ct)).Equal);
            Assert.True((await CompareAsync(connection, TotalPanel, start, end, through, true, null, ct)).Equal);
            await CorruptFleetAsync(connection, otherHour, otherHour.AddHours(1), ct);
            Assert.False((await CompareAsync(connection, TotalPanel, start, end, through, true, null, ct)).Equal);

            /* A late rebuild of the hour (and the corrupted one, by hand-set staleness) leaves the fleet rows equal to a fresh aggregate of the stamp. */
            await ComposeStampLiveSupport.ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET late_seq = late_seq + 1 WHERE hour = TIMESTAMP '{Hour(otherHour)}' AND server_id = 2", ct);
            var rebuilt = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(0, rebuilt.Failed);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE (built_seq IS DISTINCT FROM late_seq OR fleet_built_seq IS DISTINCT FROM late_seq) AND hour IN (SELECT hour FROM collect.query_store_compose_stamp_hours)", ct));
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, @"
SELECT count(*) FROM
(
    (SELECT collection_time, query_hash, wide_rows, ec_sum, cpu_wsum, dur_wsum, maxdur_max, maxdur_min, ec_min, ec_max, ec_count, maxcpu_sum FROM collect.query_store_compose_fleet
     EXCEPT
     SELECT s.collection_time, s.query_hash, sum(s.wide_rows)::bigint, sum(s.ec_sum), sum(s.cpu_wsum), sum(s.dur_wsum), max(s.maxdur_max), min(s.maxdur_min), min(s.ec_min), max(s.ec_max), sum(s.ec_count)::bigint, sum(s.maxcpu_sum)
     FROM collect.query_store_compose_stamp AS s GROUP BY s.collection_time, s.query_hash)
    UNION ALL
    (SELECT s.collection_time, s.query_hash, sum(s.wide_rows)::bigint, sum(s.ec_sum), sum(s.cpu_wsum), sum(s.dur_wsum), max(s.maxdur_max), min(s.maxdur_min), min(s.ec_min), max(s.ec_max), sum(s.ec_count)::bigint, sum(s.maxcpu_sum)
     FROM collect.query_store_compose_stamp AS s GROUP BY s.collection_time, s.query_hash
     EXCEPT
     SELECT collection_time, query_hash, wide_rows, ec_sum, cpu_wsum, dur_wsum, maxdur_max, maxdur_min, ec_min, ec_max, ec_count, maxcpu_sum FROM collect.query_store_compose_fleet)
) AS d", ct));
            Assert.True((await CompareAsync(connection, HashPanel, start, end, through, true, null, ct)).Equal);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AnHourBuiltBeforeV239_HasNoFleetRowsAndIsNeverReadFromTheFleetTable_UntilTheTickBuildsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = hourNow.AddHours(-2);
            var old = hourNow.AddHours(-12);

            /* The shape the upgrade leaves: the pairs of the hour have a stamp build and no fleet build. */
            await ComposeStampLiveSupport.ExecAsync(connection, $"DELETE FROM collect.query_store_compose_fleet WHERE collection_time >= TIMESTAMP '{Hour(old)}' AND collection_time < TIMESTAMP '{Hour(old.AddHours(1))}'", ct);
            await ComposeStampLiveSupport.ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET fleet_built_seq = NULL WHERE hour = TIMESTAMP '{Hour(old)}'", ct);
            Assert.True((await CompareAsync(connection, HashPanel, start, end, through, true, null, ct)).Equal);
            Assert.True((await CompareAsync(connection, TotalPanel, start, end, through, true, null, ct)).Equal);

            var rebuilt = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(0, rebuilt.Failed);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE fleet_built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT hour FROM collect.query_store_compose_stamp_hours)", ct));
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_fleet WHERE collection_time >= TIMESTAMP '{Hour(old)}' AND collection_time < TIMESTAMP '{Hour(old.AddHours(1))}'", ct) > 0);
            Assert.True((await CompareAsync(connection, TotalPanel, start, end, through, true, null, ct)).Equal);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /* #7146: the shape an older build's GcAsync leaves after retiring an hour: the ledger and stamp rows are gone and the fleet rows stay
       (that build does not know the fleet table). The stamp path reads nothing of such an hour, so the fleet path must not read its fleet
       rows either, before and after the next cleanup. The fleet rows are multiplied so a read that takes any of them shows. */
    [Fact]
    public async Task AnHourWithNoLedgerRowsAtAll_IsNotReadFromItsLeftoverFleetRows_TheSameAsTheStampPath()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = hourNow.AddHours(-2);
            var old = hourNow.AddHours(-12);
            var fleetRows = $"SELECT count(*) FROM collect.query_store_compose_fleet WHERE collection_time >= TIMESTAMP '{Hour(old)}' AND collection_time < TIMESTAMP '{Hour(old.AddHours(1))}'";

            await ComposeStampLiveSupport.ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{Hour(old)}'", ct);
            await ComposeStampLiveSupport.ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp WHERE collection_time >= TIMESTAMP '{Hour(old)}' AND collection_time < TIMESTAMP '{Hour(old.AddHours(1))}'", ct);
            await CorruptFleetAsync(connection, old, old.AddHours(1), ct);
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, fleetRows, ct) > 0);

            foreach (var json in new[] { HashPanel, TotalPanel })
            {
                var candidate = QueryStoreRankedHarness.WideContext(start, end) with { QueryStoreStampThrough = through, QueryStoreFleetReady = true };
                var stampOnly = QueryStoreRankedHarness.WideContext(start, end) with { QueryStoreStampThrough = through, QueryStoreFleetReady = false };
                var viaFleet = await QueryStoreRankedHarness.CompareAsync(connection, json, candidate, QueryStoreRankedHarness.Product, stampOnly, QueryStoreRankedHarness.Product, 0d, ct);
                Assert.True(viaFleet.Equal, $"fleet against stamp | {json}: {viaFleet.Difference}");
            }
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /* The stamp's partials combined per (collection_time, query_hash), compared to the fleet rows of the hour in both directions (the rows
       are counted, so a missing, an extra and a wrong row each show). */
    private static Task<long> FleetDiffersFromStampAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct)
    {
        var range = $"collection_time >= TIMESTAMP '{Hour(hour)}' AND collection_time < TIMESTAMP '{Hour(hour.AddHours(1))}'";
        const string Stamp = "SELECT collection_time, query_hash, sum(wide_rows)::bigint AS w, sum(ec_sum) AS e, sum(cpu_wsum) AS c, max(maxdur_max) AS m";
        const string Fleet = "SELECT collection_time, query_hash, wide_rows, ec_sum, cpu_wsum, maxdur_max";
        var stamp = $"({Stamp} FROM collect.query_store_compose_stamp WHERE {range} GROUP BY collection_time, query_hash)";
        var fleet = $"({Fleet} FROM collect.query_store_compose_fleet WHERE {range})";
        return ComposeStampLiveSupport.CountAsync(connection,
            $"SELECT (SELECT count(*) FROM ({stamp} EXCEPT {fleet}) a) + (SELECT count(*) FROM ({fleet} EXCEPT {stamp}) b)", ct);
    }

    [Fact]
    public async Task AnHourBuiltBeforeV239_GetsItsFleetRowsFromItsStampRowsAlone_WithoutReadingTheWideTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var old = hourNow.AddHours(-12);
            var range = $"collection_time >= TIMESTAMP '{Hour(old)}' AND collection_time < TIMESTAMP '{Hour(old.AddHours(1))}'";
            var stampRows = await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp WHERE {range}", ct);
            Assert.True(stampRows > 0);

            /* The upgrade-shaped hour: the stamp and the hash-hourly rows are built and valid, the fleet rows do not exist. The hour's wide rows are then
               REMOVED (a delete fires no late-writer trigger), so a build that reads the wide table would empty the stamp rows and the fleet rows. */
            await ComposeStampLiveSupport.ExecAsync(connection, $"DELETE FROM collect.query_store_compose_fleet WHERE {range}", ct);
            await ComposeStampLiveSupport.ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET fleet_built_seq = NULL WHERE hour = TIMESTAMP '{Hour(old)}'", ct);
            await ComposeStampLiveSupport.ExecAsync(connection, $"DELETE FROM collect.query_store_interval_wide WHERE {range}", ct);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide WHERE {range}", ct));

            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(1, tick.Built);

            Assert.Equal(stampRows, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp WHERE {range}", ct));
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_fleet WHERE {range}", ct) > 0);
            Assert.Equal(0, await FleetDiffersFromStampAsync(connection, old, ct));
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection,
                $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{Hour(old)}' AND (fleet_built_seq IS DISTINCT FROM late_seq OR built_seq IS DISTINCT FROM late_seq)", ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AWriterThatBumpedAPairBeforeTheFleetOnlyBuild_LeavesThatPairStale_AndTheFullBuildTakesTheHourNext()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var old = hourNow.AddHours(-12);
            var hour = $"hour = TIMESTAMP '{Hour(old)}'";
            await ComposeStampLiveSupport.ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET fleet_built_seq = NULL WHERE {hour}", ct);

            /* The plan saw an hour stale only for the fleet table; a late write to server 1 lands before the build runs. */
            await ComposeStampLiveSupport.ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET late_seq = late_seq + 1 WHERE {hour} AND server_id = 1", ct);
            Assert.NotNull(await QueryStoreComposeStamp.BuildFleetHourAsync(connection, old, ct));

            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE {hour} AND fleet_built_seq IS DISTINCT FROM late_seq", ct));
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE {hour} AND server_id = 1 AND fleet_built_seq IS DISTINCT FROM late_seq", ct));

            /* The full build, which the plan now chooses for the hour, brings every pair level again. */
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection,
                $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE {hour} AND (fleet_built_seq IS DISTINCT FROM late_seq OR built_seq IS DISTINCT FROM late_seq)", ct));
            Assert.Equal(0, await FleetDiffersFromStampAsync(connection, old, ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task GroupsTiedAtTheNthValue_GiveTheSameMembersOnTheFleetPathTheStampPathAndTheWideTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            /* One hour of rows of our own: five hashes tied at 10 executions, inserted in reverse order, and one hash at 99. A top 3 keeps hashF and
               then two of the five: the key column breaks the tie, so it is hashA and hashB on every path. */
            var tied = hourNow.AddHours(-6).AddMinutes(5);
            await ComposeStampLiveSupport.ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide", ct);
            var id = 1;
            foreach (var (hash, executions) in new[] { ("hashE", 10), ("hashD", 10), ("hashC", 10), ("hashB", 10), ("hashA", 10), ("hashF", 99) })
            {
                await ComposeStampLiveSupport.ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{Hour(tied)}', 1, 'db', {id}, {id}, 'Regular', TIMESTAMP '{Hour(tied.AddMinutes(-3))}', TIMESTAMP '{Hour(tied)}', 'mod', '{hash}', {executions}, 1000, 500, 2000, 900, {id})", ct);
                id++;
            }

            Assert.True(await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)) > 0);
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_fleet WHERE query_hash = 'hashA'", ct) > 0);

            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = hourNow.AddHours(-2);
            const string Ranked = "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":3,\"groupBy\":[\"query_hash\"],\"viz\":\"bar\"}";
            const string Series = "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":3,\"groupBy\":[\"query_hash\"],\"timeBucket\":\"hour\",\"viz\":\"line\"}";

            var expected = new[] { "hashF", "hashA", "hashB" };
            foreach (var fleetReady in new[] { true, false })
            {
                var context = QueryStoreRankedHarness.WideContext(start, end) with { QueryStoreStampThrough = through, QueryStoreFleetReady = fleetReady };
                Assert.Equal(expected, await RankedMembersAsync(connection, Ranked, context, ct));
                var series = await CompareAsync(connection, Series, start, end, through, fleetReady, null, ct);
                Assert.True(series.Equal, series.Difference);
                Assert.False(series.MemberSetsDiffered);
            }

            /* The wide table (the oracle's route) gives the same members. */
            Assert.Equal(expected, await RankedMembersAsync(connection, Ranked, QueryStoreRankedHarness.WideContext(start, end), ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /* The groups of a ranked (bar) panel, in the order the statement returns them. */
    private static async Task<string[]> RankedMembersAsync(NpgsqlConnection connection, string json, ComposeRunContext context, CancellationToken ct)
    {
        var plan = QueryStoreRankedHarness.Parse(json);
        var (compiled, error) = ComposeCompiler.CompileCore(plan, context, singleScanRankedTimeSeries: true);
        Assert.True(error is null, error);
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var members = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            members.Add(reader.GetString(0));
        }

        return members.ToArray();
    }

    [Fact]
    public async Task TheRungV239_CreatesTheTableAndTheLedgerColumnFromV238_AndARerunChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = await QueryStoreIntervalRetentionTestKit.OpenStoreAsync(scratch, ct);

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 238, ct);
        await ComposeStampLiveSupport.ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 239", ct);
        Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'collect' AND table_name = 'query_store_compose_fleet'", ct));

        Assert.Equal(StorageVersion.SchemaVersion - 238, await PgMigrations.MigrateAsync(connection, ct));
        Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'collect' AND table_name = 'query_store_compose_fleet'", ct));
        Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_store_compose_stamp_built' AND column_name = 'fleet_built_seq'", ct));

        var shape = "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name IN ('query_store_compose_fleet', 'query_store_compose_stamp_built')";
        var before = await ComposeStampLiveSupport.CountAsync(connection, shape, ct);
        await ComposeStampLiveSupport.ExecAsync(connection, QueryStoreComposeStamp.FleetCreateSql, ct);
        Assert.Equal(before, await ComposeStampLiveSupport.CountAsync(connection, shape, ct));
        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
    }
}
