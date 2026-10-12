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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3, lane 4: a late write into a built hour never changes an answer. Two writes, each on the lane 1 seed with all 28 hours
/// built: a replayed row inserted into a built hour, and a row moved out of a built hour by an UPDATE of its collection_time. For each:
/// the trigger marks the pair, so the runner's StampThrough lookup stops at that hour; the statement's stale arm gives the oracle's
/// answer when the pair turns stale AFTER the lookup (simulated by handing the compiler the StampThrough from before the write); and
/// a rebuild makes the hour current, so the lookup passes it again and the rollup answers it.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class ComposeQueryStoreStampInvalidationLiveTests
{
    private static readonly string[] Panels =
    {
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"viz\":\"stat\"}",
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"min\",\"timeBucket\":\"hour\",\"viz\":\"line\"}",
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_max_duration_us\",\"aggregate\":\"max\",\"topN\":5,\"groupBy\":[\"module_name\"],\"viz\":\"bar\"}",
        "{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"topN\":4,\"groupBy\":[\"module_name\"],\"timeBucket\":\"hour\",\"includeOther\":true,\"viz\":\"line\"}",
        "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":4,\"groupBy\":[\"query_hash\"],\"timeBucket\":\"hour\",\"viz\":\"line\"}",
    };

    private static async Task AssertSameAsOracleAsync(
        NpgsqlConnection connection, DateTime start, DateTime end, DateTime? stampThrough, IReadOnlyList<string>? scope, string label, CancellationToken ct)
    {
        var withStamp = QueryStoreRankedHarness.WideContext(start, end, scope) with { QueryStoreStampThrough = stampThrough };
        var without = QueryStoreRankedHarness.WideContext(start, end, scope);
        foreach (var json in Panels)
        {
            var result = await QueryStoreRankedHarness.CompareAsync(
                connection, json, withStamp, QueryStoreRankedHarness.Product, without, QueryStoreRankedHarness.Oracle, 0d, ct);
            Assert.True(result.Equal, $"{label} | {json}: {result.Difference}");
            Assert.True(result.OracleRows > 0, $"{label} | {json}: no rows");
            if (stampThrough is not null)
            {
                Assert.Contains("query_store_compose_stamp", result.CandidateSql, StringComparison.Ordinal);
            }
        }
    }

    private static string Pair(int server, DateTime hour) => server.ToString(CultureInfo.InvariantCulture) + "@" + hour.ToString("yyyy-MM-dd HH", CultureInfo.InvariantCulture);

    private static Task<string> StaleListAsync(NpgsqlConnection connection, CancellationToken ct) => ComposeStampLiveSupport.TextAsync(connection, @"
SELECT COALESCE(string_agg(b.server_id || '@' || to_char(b.hour, 'YYYY-MM-DD HH24'), ',' ORDER BY b.server_id, b.hour), '')
FROM collect.query_store_compose_stamp_built AS b
JOIN collect.query_store_compose_stamp_hours AS h ON h.hour = b.hour
WHERE b.built_seq IS DISTINCT FROM b.late_seq", ct);

    [Fact]
    public async Task AReplayedRow_AndARowMovedOutOfABuiltHour_StopTheLookupAtThatHour_TheStaleArmGivesTheOraclesAnswer_AndARebuildMakesTheHourCurrent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var baseline = hourNow.AddHours(-2);
            Assert.Equal(baseline, await ComposeStampLiveSupport.StampThroughAsync(connection, start, end, ct));
            Assert.Equal(string.Empty, await StaleListAsync(connection, ct));
            await AssertSameAsOracleAsync(connection, start, end, baseline, null, "before any late write", ct);

            /* 1. A replayed row into a built hour (server 1, 10 hours ago). */
            var replayHour = hourNow.AddHours(-10);
            await ComposeStampLiveSupport.InsertRowAsync(connection, 1, replayHour.AddMinutes(55), 9_000_001, ct);
            Assert.Equal(Pair(1, replayHour), await StaleListAsync(connection, ct));
            Assert.NotEqual(
                await ComposeStampLiveSupport.CountAsync(connection, $"SELECT sum(execution_count) FROM collect.query_store_interval_wide WHERE server_id = 1 AND collection_time >= TIMESTAMP '{ComposeStampLiveSupport.At(replayHour)}' AND collection_time < TIMESTAMP '{ComposeStampLiveSupport.At(replayHour.AddHours(1))}'", ct),
                await ComposeStampLiveSupport.CountAsync(connection, $"SELECT sum(ec_sum) FROM collect.query_store_compose_stamp WHERE server_id = 1 AND collection_time >= TIMESTAMP '{ComposeStampLiveSupport.At(replayHour)}' AND collection_time < TIMESTAMP '{ComposeStampLiveSupport.At(replayHour.AddHours(1))}'", ct));

            /* The lookup stops at the stale hour, for the fleet and for a scope that includes server 1, and passes it for a scope
               of the other two servers (the stale pair is not theirs). */
            Assert.Equal(replayHour, await ComposeStampLiveSupport.StampThroughAsync(connection, start, end, ct));
            Assert.Equal(baseline, await StampThroughForAsync(connection, new[] { 2, 3 }, start, end, ct));
            Assert.Equal(replayHour, await StampThroughForAsync(connection, new[] { 1 }, start, end, ct));

            /* The pair turned stale after the lookup: the statement still has the baseline StampThrough, and its stale arm answers. */
            await AssertSameAsOracleAsync(connection, start, end, baseline, null, "replayed row, stale arm", ct);
            await AssertSameAsOracleAsync(connection, start, end, baseline, new[] { "srv1" }, "replayed row, stale arm, server 1", ct);
            await AssertSameAsOracleAsync(connection, start, end, replayHour, null, "replayed row, runner's StampThrough", ct);

            /* The rebuild makes it current. */
            var rebuilt = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, ComposeStampLiveSupport.RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, rebuilt.Failed);
            Assert.Equal(1, rebuilt.BuiltStale);
            Assert.Equal(string.Empty, await StaleListAsync(connection, ct));
            Assert.Equal(baseline, await ComposeStampLiveSupport.StampThroughAsync(connection, start, end, ct));
            await AssertSameAsOracleAsync(connection, start, end, baseline, null, "replayed row, after the rebuild", ct);

            /* 2. A row moved out of a built hour into the current hour (the open interval's refresh): the OLD hour is marked. */
            var moveHour = hourNow.AddHours(-15);
            await ComposeStampLiveSupport.ExecAsync(connection, $@"
UPDATE collect.query_store_interval_wide SET collection_time = TIMESTAMP '{ComposeStampLiveSupport.At(hourNow.AddMinutes(5))}'
WHERE ctid = (SELECT ctid FROM collect.query_store_interval_wide WHERE server_id = 2 AND collection_time >= TIMESTAMP '{ComposeStampLiveSupport.At(moveHour)}'
                AND collection_time < TIMESTAMP '{ComposeStampLiveSupport.At(moveHour.AddHours(1))}' ORDER BY collection_time, query_id, plan_id LIMIT 1)", ct);
            Assert.Equal(Pair(2, moveHour), await StaleListAsync(connection, ct));
            Assert.Equal(moveHour, await ComposeStampLiveSupport.StampThroughAsync(connection, start, end, ct));
            await AssertSameAsOracleAsync(connection, start, end, baseline, null, "moved-out row, stale arm", ct);
            await AssertSameAsOracleAsync(connection, start, end, baseline, new[] { "srv2" }, "moved-out row, stale arm, server 2", ct);

            rebuilt = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, ComposeStampLiveSupport.RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, rebuilt.Failed);
            Assert.Equal(1, rebuilt.BuiltStale);
            Assert.Equal(string.Empty, await StaleListAsync(connection, ct));
            Assert.Equal(baseline, await ComposeStampLiveSupport.StampThroughAsync(connection, start, end, ct));
            await AssertSameAsOracleAsync(connection, start, end, baseline, null, "moved-out row, after the rebuild", ct);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    private static Task<DateTime?> StampThroughForAsync(NpgsqlConnection connection, int[] serverIds, DateTime start, DateTime end, CancellationToken ct) =>
        DarlingWebEndpoints.ResolveQueryStoreStampThroughAsync(connection, serverIds, start, end, QueryStoreComposeStamp.RungVersion, ct);
}
