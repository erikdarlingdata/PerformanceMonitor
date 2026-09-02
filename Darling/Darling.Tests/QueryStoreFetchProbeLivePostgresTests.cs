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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2312 touch-and-probe against a REAL store — the one statement the whole activity-driven fetch
/// runs on, whose risk lives entirely in how PostgreSQL evaluates the data-modifying CTEs, the hourly
/// guard, the hash comparisons and the LEFT JOIN together; no source pin can speak to any of it. Also the
/// writer's NULL-digest content-less marker, which V77's nullable column exists for: it must land, read as
/// RESOLVED, and never re-enter the fetch list.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreFetchProbeLivePostgresTests
{
    private const string ServerName = "darling-fetch-probe-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "ProbeDb";

    /* #2776: the store-write path now takes an explicit command timeout instead of inheriting Npgsql's
       30s default. These fixtures write a handful of rows, so the value is immaterial to what they assert —
       it is here only because the parameter is required, which is deliberate: making it required is what
       forced every call site (including this one) to be found by the compiler rather than by a timeout in
       production. */
    private const int TestTimeoutSeconds = 30;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TouchAndProbe_AnswersMissingStaleAndMarker_AndRefreshesLiveness()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live fetch-probe test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var landedAt = DateTime.UtcNow.AddHours(-3);

            /* Plan 1: real content with a hash. Plan 2: the engine had nothing to give — the writer must
               land the NULL-digest marker rather than skipping the row. */
            var landed = await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db,
                new[]
                {
                    new FetchedPlan(1, "<plan one/>", "0xAAAA"),
                    new FetchedPlan(2, PlanXml: null, PlanHash: "0xBBBB"),
                },
                landedAt, TestTimeoutSeconds, ct);
            Assert.Equal(new long[] { 1, 2 }, landed);

            using (var marker = new NpgsqlCommand(
                "SELECT digest IS NULL FROM collect.query_store_plan_map WHERE server_id = $1 AND database_name = $2 AND plan_id = 2", connection))
            {
                marker.Parameters.AddWithValue(ServerId);
                marker.Parameters.AddWithValue(Db);
                Assert.Equal(true, await marker.ExecuteScalarAsync(ct));
            }

            /* The cycle references four plans: 1 current, 2 the marker, 3 never seen, and 1-with-a-new-hash
               is exercised by a second batch below. The probe must return them all, in order. */
            var now = DateTime.UtcNow;
            var verdicts = await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                connection, ServerId, Db,
                new[] { (1L, (string?)"0xAAAA"), (2L, (string?)"0xBBBB"), (3L, (string?)"0xCCCC") },
                now, TestTimeoutSeconds, ct);

            Assert.Equal(3, verdicts.Count);
            Assert.Equal(new FetchProbeVerdict(1, Resolved: true, HashStale: false), verdicts[0]);
            /* The marker resolves — that is its entire job. */
            Assert.Equal(new FetchProbeVerdict(2, Resolved: true, HashStale: false), verdicts[1]);
            Assert.Equal(new FetchProbeVerdict(3, Resolved: false, HashStale: false), verdicts[2]);

            /* Liveness: the touch advanced last_seen past the 3-hour-old landing stamp (the rows were
               older than the hourly guard, so the update fired) — on the map AND on the dimension row the
               real digest points at. */
            using (var freshness = new NpgsqlCommand(@"
SELECT
    (SELECT COUNT(*) FROM collect.query_store_plan_map
     WHERE server_id = $1 AND database_name = $2 AND last_seen > $3),
    (SELECT COUNT(*) FROM collect.query_plan_dim d
     JOIN collect.query_store_plan_map m ON m.digest = d.digest
     WHERE m.server_id = $1 AND m.database_name = $2 AND d.last_seen > $3)", connection))
            {
                freshness.Parameters.AddWithValue(ServerId);
                freshness.Parameters.AddWithValue(Db);
                freshness.Parameters.AddWithValue(QueryStorePlanMap.Naive(landedAt.AddMinutes(1)));
                await using var reader = await freshness.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(2L, reader.GetInt64(0));   /* both referenced map rows touched */
                Assert.Equal(1L, reader.GetInt64(1));   /* the one real dim row touched */
            }

            /* An in-place rewrite: same plan_id, different live hash. Stale, and still resolved — the
               caller refetches on the OR of the two. */
            var stale = await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                connection, ServerId, Db, new[] { (1L, (string?)"0xDEAD") }, now.AddHours(2), TestTimeoutSeconds, ct);
            Assert.Equal(new FetchProbeVerdict(1, Resolved: true, HashStale: true), stale.Single());

            /* Text side: land one row WITHOUT a hash (the legacy shape), then probe with a live hash —
               NULL adopts rather than reading stale, and a second probe with a DIFFERENT hash is the
               reset detector firing. */
            await QueryStoreTextWriter.WriteAsync(
                connection, ServerId, Db,
                new[] { new FetchedQueryText(10, "SELECT 1", QueryHash: null) }, landedAt, TestTimeoutSeconds, ct);

            var adopt = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db, new[] { (10L, (string?)"0x1111"), (11L, (string?)"0x2222") }, now, TestTimeoutSeconds, ct);
            Assert.Equal(new FetchProbeVerdict(10, Resolved: true, HashStale: false), adopt[0]);
            Assert.Equal(new FetchProbeVerdict(11, Resolved: false, HashStale: false), adopt[1]);

            var renumbered = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db, new[] { (10L, (string?)"0x9999") }, now.AddHours(2), TestTimeoutSeconds, ct);
            Assert.Equal(new FetchProbeVerdict(10, Resolved: true, HashStale: true), renumbered.Single());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql =
            $"DELETE FROM collect.query_store_plan_map WHERE server_id = {ServerId};" +
            $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId};" +
            $"DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
