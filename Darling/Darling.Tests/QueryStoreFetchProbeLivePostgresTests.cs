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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2312 touch-and-probe against a REAL store — the one statement the whole activity-driven fetch
/// runs on, whose risk lives entirely in how PostgreSQL evaluates the data-modifying CTEs, the liveness
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

    /* #4981: every plan content this class lands in the shared digest-keyed dimension. The cleanup removes them by
       digest, so a dimension row an older run left behind (its map rows long gone) cannot skew this run either. */
    private const string PlanOne = "<plan one/>";
    private const string PlanRestart = "<plan restart/>";
    private const string PlanOnlyOurs = "<plan only-ours/>";
    private const string PlanShared = "<plan shared/>";

    /* #4981: plan content this class does NOT write in its probe tests, and so must never delete. Only the cleanup test
       below lands them, to prove the cleanup stays inside the four contents above even when a map row points at one of
       these and a fact table references it. They are deliberately not in the cleanup's known list. */
    private const string PlanOutsideOurs = "<plan outside-ours/>";
    private const string PlanOutsideOther = "<plan outside-other/>";

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
            /* Older than the touch guard, DERIVED from it rather than typed: the liveness assertions below
               are the guard firing, so a fixture with a hard-coded age silently stops testing the touch the
               moment the width moves — it would land inside the guard, no UPDATE would run, and the freshness
               counts would read 0/0. One hour past the width is enough; the guard is a strict inequality. */
            var landedAt = DateTime.UtcNow.AddHours(-(QueryStoreLivenessTouchGuard.GuardHours + 1));

            /* Plan 1: real content with a hash. Plan 2: the engine had nothing to give — the writer must
               land the NULL-digest marker rather than skipping the row. */
            var landed = await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db,
                new[]
                {
                    new FetchedPlan(1, PlanOne, "0xAAAA"),
                    new FetchedPlan(2, PlanXml: null, PlanHash: "0xBBBB"),
                },
                landedAt, TestTimeoutSeconds, cancellationToken: ct);
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

            /* Liveness: the touch advanced last_seen past the landing stamp (the rows were older than the
               guard, so the update fired) — on the map AND on the dimension row the real digest points
               at. */
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
                new[] { new FetchedQueryText(10, "SELECT 1", QueryHash: null) }, landedAt, TestTimeoutSeconds, cancellationToken: ct);

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

    /// <summary>
    /// #4250: the touch guard's freshness gate is the <c>last_seen</c> stored in the map, dimension and
    /// text rows themselves — nothing about it lives in the writer's memory, because this design has none;
    /// <see cref="QueryStoreFetchProbe"/> is static and every call is connection-scoped. So a process
    /// restart — the nearest thing a live rig can rehearse is a brand-new connection, since there is no
    /// other per-process state to reset — must not re-touch rows the guard already stamped fresh: probing
    /// the SAME references again from a DIFFERENT connection, still inside the guard window, updates zero
    /// rows anywhere.
    /// </summary>
    [Fact]
    public async Task Restart_ANewConnectionRetouchesNothingFresh_TheGuardReadsStoredLastSeen()
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
            /* Older than the guard, so the FIRST touch below actually fires (same reasoning as the sibling
               test above: a hard-coded age would silently stop exercising the touch once the width moves). */
            var landedAt = DateTime.UtcNow.AddHours(-(QueryStoreLivenessTouchGuard.GuardHours + 1));

            await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db,
                new[] { new FetchedPlan(201, PlanRestart, "0xR1") },
                landedAt, TestTimeoutSeconds, cancellationToken: ct);
            await QueryStoreTextWriter.WriteAsync(
                connection, ServerId, Db,
                new[] { new FetchedQueryText(202, "SELECT 'restart'", "0xR2") },
                landedAt, TestTimeoutSeconds, cancellationToken: ct);

            async Task<(long Map, long Dim, long Text)> FreshnessCountsAsync(DateTime stamp)
            {
                using var freshness = new NpgsqlCommand(@"
SELECT
    (SELECT COUNT(*) FROM collect.query_store_plan_map
     WHERE server_id = $1 AND database_name = $2 AND last_seen = $3),
    (SELECT COUNT(*) FROM collect.query_plan_dim d
     JOIN collect.query_store_plan_map m ON m.digest = d.digest
     WHERE m.server_id = $1 AND m.database_name = $2 AND d.last_seen = $3),
    (SELECT COUNT(*) FROM collect.query_store_text
     WHERE server_id = $1 AND database_name = $2 AND last_seen = $3)", connection);
                freshness.Parameters.AddWithValue(ServerId);
                freshness.Parameters.AddWithValue(Db);
                freshness.Parameters.AddWithValue(stamp);
                await using var reader = await freshness.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);
                return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
            }

            // The first touch, from the original connection: the rows landed older than the guard, so
            // this fires and stamps all three relations with firstTouch.
            var firstTouch = DateTime.UtcNow;
            await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                connection, ServerId, Db, new[] { (201L, (string?)"0xR1") }, firstTouch, TestTimeoutSeconds, ct);
            await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                connection, ServerId, Db, new[] { (202L, (string?)"0xR2") }, firstTouch, TestTimeoutSeconds, ct);

            var firstTouchStamp = QueryStorePlanMap.Naive(firstTouch);
            Assert.Equal((1L, 1L, 1L), await FreshnessCountsAsync(firstTouchStamp));

            // "Restart": a brand-new connection. Same references, five minutes later — still well inside
            // the guard window, so nothing here should qualify for a re-touch.
            using var freshConnection = new NpgsqlConnection(cs);
            await freshConnection.OpenAsync(ct);
            var secondTouch = firstTouch.AddMinutes(5);

            var planVerdicts = await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                freshConnection, ServerId, Db, new[] { (201L, (string?)"0xR1") }, secondTouch, TestTimeoutSeconds, ct);
            var textVerdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                freshConnection, ServerId, Db, new[] { (202L, (string?)"0xR2") }, secondTouch, TestTimeoutSeconds, ct);

            // Zero rows updated: every row still carries the FIRST touch's stamp, none carry the second.
            Assert.Equal((1L, 1L, 1L), await FreshnessCountsAsync(firstTouchStamp));
            Assert.Equal((0L, 0L, 0L), await FreshnessCountsAsync(QueryStorePlanMap.Naive(secondTouch)));

            // The fresh connection's verdicts still resolve correctly — the restart cost nothing but the
            // write it correctly skipped.
            Assert.Equal(new FetchProbeVerdict(201, Resolved: true, HashStale: false), planVerdicts.Single());
            Assert.Equal(new FetchProbeVerdict(202, Resolved: true, HashStale: false), textVerdicts.Single());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #4981: the cleanup every test here ends with leaves the shared store as it found it, and takes only what this
    /// class wrote. The plan content these tests land goes to the digest-keyed dimension, which the map-row delete
    /// never reached, so each run left dimension rows behind. The cleanup deletes by the content's digest and by
    /// nothing else, because the dimension keeps no reference count and a lookup through the map rows would also take
    /// a row that <c>query_stats</c> or <c>procedure_stats</c> still references.
    ///
    /// <para>This lands two of the class's own plans (one a second server's map row also points at) and two plans it
    /// does not write, each referenced by a <c>query_stats</c> row; one of those is also pointed at by a map row of the
    /// server being cleaned, the other by a map row of the second server. After each cleanup the class's own plans are
    /// gone, and both others are still there.</para>
    /// </summary>
    [Fact]
    public async Task TheCleanup_RemovesTheDimensionRowsItLanded_AndLeavesARowAFactTableStillReferences()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live fetch-probe test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var otherServerId = ServerIdHelper.GetDeterministicHashCode(ServerName + "-other");

        /* The second server's rows, removed at the start as well as the end so a run that died mid-way cannot break
           the next one. Its dimension rows go the way this server's do, by the four known digests alone: following
           its map rows to the dimension would take a row that another table still references. otherServerId is an
           int computed here, never user input: safe to inline. The digest delete takes a parameter, so it is its own
           command, and the map delete is a second one (a parameterised command may not carry more than one
           statement). */
        async Task DeleteOtherServerRowsAsync(NpgsqlConnection target, CancellationToken token)
        {
            await DeleteKnownPlanDimensionRowsAsync(target, token);
            using var other = new NpgsqlCommand($"DELETE FROM collect.query_store_plan_map WHERE server_id = {otherServerId}", target);
            await other.ExecuteNonQueryAsync(token);
        }

        var outsideOurs = PayloadDimensions.Digest(PlanOutsideOurs);
        var outsideOther = PayloadDimensions.Digest(PlanOutsideOther);

        /* What the two outside plans leave behind: the fact rows that reference them, and their own dimension rows
           (the cleanup under test must not remove those, so this test does). Removed at the start as well as the end,
           for the same reason as the second server's rows. */
        async Task DeleteOutsideRowsAsync(NpgsqlConnection target, CancellationToken token)
        {
            await DarlingMcpTestData.ExecAsync(target, token,
                "DELETE FROM collect.query_stats WHERE server_id = $1", ServerId);
            using var outside = new NpgsqlCommand("DELETE FROM collect.query_plan_dim WHERE digest = ANY($1)", target);
            outside.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bytea, new[] { outsideOurs, outsideOther });
            await outside.ExecuteNonQueryAsync(token);
        }

        await DeleteOtherServerRowsAsync(connection, ct);
        await DeleteOutsideRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var landedAt = DateTime.UtcNow.AddHours(-(QueryStoreLivenessTouchGuard.GuardHours + 1));
            await QueryStorePlanWriter.WriteAsync(
                connection, ServerId, Db,
                new[]
                {
                    new FetchedPlan(301, PlanOnlyOurs, "0xC1"),
                    new FetchedPlan(302, PlanShared, "0xC2"),
                    new FetchedPlan(303, PlanOutsideOurs, "0xC3"),
                },
                landedAt, TestTimeoutSeconds, cancellationToken: ct);
            await QueryStorePlanWriter.WriteAsync(
                connection, otherServerId, Db,
                new[] { new FetchedPlan(401, PlanOutsideOther, "0xC4") },
                landedAt, TestTimeoutSeconds, cancellationToken: ct);

            /* A fact row referencing each outside plan: the references the old map-only check could not see. */
            foreach (var referenced in new[] { outsideOurs, outsideOther })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_digest, delta_execution_count, delta_elapsed_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    CollectionIdGenerator.Next(), QueryStorePlanMap.Naive(DateTime.UtcNow), ServerId, ServerName, Db, "0xFACT", referenced, 1L, 1000L);
            }

            async Task<byte[]> DigestOfAsync(long planId)
            {
                using var read = new NpgsqlCommand(
                    "SELECT digest FROM collect.query_store_plan_map WHERE server_id = $1 AND database_name = $2 AND plan_id = $3", connection);
                read.Parameters.AddWithValue(ServerId);
                read.Parameters.AddWithValue(Db);
                read.Parameters.AddWithValue(planId);
                return (byte[])(await read.ExecuteScalarAsync(ct))!;
            }

            async Task<long> DimRowsAsync(byte[] digest)
            {
                using var count = new NpgsqlCommand("SELECT COUNT(*) FROM collect.query_plan_dim WHERE digest = $1", connection);
                count.Parameters.AddWithValue(digest);
                return (long)(await count.ExecuteScalarAsync(ct))!;
            }

            var ours = await DigestOfAsync(301);
            var shared = await DigestOfAsync(302);

            /* The cleanup's by-content delete is only as good as this: the stored digest is the content's digest. */
            Assert.Equal(PayloadDimensions.Digest(PlanOnlyOurs), ours);
            Assert.Equal(PayloadDimensions.Digest(PlanShared), shared);
            Assert.Equal(1L, await DimRowsAsync(ours));
            Assert.Equal(1L, await DimRowsAsync(shared));
            Assert.Equal(1L, await DimRowsAsync(outsideOurs));
            Assert.Equal(1L, await DimRowsAsync(outsideOther));

            using (var other = new NpgsqlCommand(
                "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1, $2, 1, $3, '0xC2', $4)", connection))
            {
                other.Parameters.AddWithValue(otherServerId);
                other.Parameters.AddWithValue(Db);
                other.Parameters.AddWithValue(shared);
                other.Parameters.AddWithValue(QueryStorePlanMap.Naive(landedAt));
                await other.ExecuteNonQueryAsync(ct);
            }

            await DeleteRowsAsync(connection, ct);

            /* Content it did not write stays, though a map row of the server being cleaned pointed at the first. */
            Assert.Equal(1L, await DimRowsAsync(outsideOurs));
            Assert.Equal(1L, await DimRowsAsync(outsideOther));

            /* The class's own content goes by its digest, though a second server's map row pointed at one of the two. */
            Assert.Equal(0L, await DimRowsAsync(ours));
            Assert.Equal(0L, await DimRowsAsync(shared));

            /* The second server's cleanup is held to the same line: its map row points at the second outside plan. */
            await DeleteOtherServerRowsAsync(connection, ct);

            Assert.Equal(1L, await DimRowsAsync(outsideOurs));
            Assert.Equal(1L, await DimRowsAsync(outsideOther));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteOtherServerRowsAsync(cleanup, cleanupCt);
                await DeleteRowsAsync(cleanup, cleanupCt);
                await DeleteOutsideRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>The digests of the four plan contents this class writes: the whole of what its cleanup may delete from the dimension.</summary>
    private static readonly byte[][] KnownDigests =
        new[] { PlanOne, PlanRestart, PlanOnlyOurs, PlanShared }.Select(PayloadDimensions.Digest).ToArray();

    /// <summary>
    /// #4981: removes the dimension rows of the four plan contents this class writes, by digest and by nothing else.
    /// The dimension is keyed by content and no server id scopes it, so deleting the map rows alone left one row per
    /// plan behind in the shared store on every run (eight of them failed a later class). The delete does not follow
    /// the map rows to find them, and does not check which tables still reference a row before it goes: the dimension
    /// keeps no reference count, <c>query_stats</c> and <c>procedure_stats</c> reference it as well as the map, and a
    /// check of only some of those would remove a row a table the check did not name still uses. Four literal contents
    /// no other class writes need no such check.
    /// </summary>
    private static async Task DeleteKnownPlanDimensionRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var dimension = new NpgsqlCommand("DELETE FROM collect.query_plan_dim WHERE digest = ANY($1)", connection);
        dimension.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bytea, KnownDigests);
        await dimension.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// #4981: removes every row this class plants: the four plan contents in the shared dimension (see
    /// <see cref="DeleteKnownPlanDimensionRowsAsync"/>), then this server's map, text and registry rows.
    /// </summary>
    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DeleteKnownPlanDimensionRowsAsync(connection, ct);

        var sql =
            $"DELETE FROM collect.query_store_plan_map WHERE server_id = {ServerId};" +
            $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId};" +
            $"DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
