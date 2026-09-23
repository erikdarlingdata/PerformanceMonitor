/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3976: <c>list_servers</c> (<see cref="DarlingDataReader.ServerListSql"/>) and the two per-server summary
/// reads (<see cref="ViewerDataService.ServerSummaryLastCollectionSql"/>,
/// <see cref="DarlingHealthReader.ServerSummaryLastCollectionSql"/>) now take a server's newest collection
/// through the same per-server <c>ORDER BY collection_time DESC LIMIT 1</c> shape
/// <see cref="ViewerDataService.ServerFreshnessSql"/> already proved (#3895) — an ordered descent that stops
/// at the newest chunk with a row, rather than a <c>MAX(collection_time)</c> a bound cannot help: any bound
/// wide enough to keep the answer identical is the retention horizon itself, and every retained chunk falls
/// inside it (the issue's own "why a time bound doesn't help" argument, which is why this fix changes the
/// PLAN's SHAPE rather than adding a WHERE clause).
///
/// <para>Behavioral parity — a dark-past-retention server still reading Offline, a just-registered one still
/// "Awaiting first collection" — is <c>DarkPastRetentionReadsOfflineTests</c>'s and
/// <c>DarkPastRetentionReadsOfflineLivePostgresTests</c>'s job, both re-run unchanged by this PR. This file
/// proves the two things those cannot: the shipped answer matches the pre-#3976 oracle for a collecting
/// server, and the shipped PLAN is the ordered, early-exiting descent the fix is about (not a magnitude
/// comparison against the oracle - collection_log is the store's shared hypertable across every suite's live
/// tests, and PostgreSQL's own MIN/MAX rewrite can reach for the same index on a small rig, both of which
/// make a synthetic seed an unreliable place to reproduce the issue's field measurement).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ServerListAndSummaryPlanShapeTests
{
    private const int CollectingServerId = -397_601;
    private static readonly int[] s_serverIds = { CollectingServerId };

    /// <summary>The pre-#3976 shape, kept here as the row-correctness oracle for all three reads (they all
    /// answered this same MAX per server before this fix).</summary>
    private const string OldSummaryLastCollectionSql = "SELECT MAX(collection_time) FROM v_collection_log WHERE server_id = $1";

    [Fact]
    public async Task TheShippedReads_TouchFarFewerChunks_AndReturnTheSameNewestCollection_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #3976 plan-shape test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct);
        }

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            var now = TruncateToSeconds(DateTime.UtcNow);
            await ExecAsync(connection, ct,
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 3, 16, 'sqlserver', $3, $3)",
                CollectingServerId, "server3976-a", now.AddDays(-9));

            /* A week of daily history at production-like density (a real store's collectors write hundreds of
               rows a day per server), so the planner's cost model has the same incentive it has in the field
               to prefer the index descent over a seq scan of a near-empty chunk. Eight daily chunks, 200
               rows/day, plus a row a minute ago — the newest, and the one every read must return. */
            for (var day = 8; day >= 1; day--)
            {
                await InsertCollectionLogBatchAsync(connection, ct, CollectingServerId, now.AddDays(-day), 200);
            }

            var newest = now.AddMinutes(-1);
            await InsertCollectionLogAsync(connection, ct, CollectingServerId, newest);

            using (var analyze = new NpgsqlCommand("ANALYZE collect.collection_log", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            /* list_servers: the shipped LATERAL read's ANSWER against the old correlated-MAX oracle, and the
               shipped read's PLAN SHAPE on its own. Not a shape-vs-shape comparison: collection_log is the
               store's shared hypertable (other suites' chunks, mostly empty past their own cleanup, sit
               beside this test's seeded week) and PostgreSQL's own MIN/MAX rewrite can also reach for the same
               index on a small rig, both of which make a synthetic seed an unreliable place to reproduce the
               issue's field measurement (planning cost on a store with hundreds of chunks). What IS reliable
               is that the SHIPPED plan is an ordered per-chunk descent with a LIMIT that stops at the first
               chunk holding a row - "never executed" on the rest is the direct evidence a MAX() over the whole
               table cannot produce, whatever the surrounding noise; the field numbers are in the issue and the
               PR body. */
            var oracleRow = await ScalarAsync(connection, OldSummaryLastCollectionSql, CollectingServerId, ct);
            var rows = await DarlingDataReader.GetServerListAsync(NpgsqlDataSource.Create(connectionString!), ct);
            var row = Assert.Single(rows, r => r.ServerId == CollectingServerId);
            Assert.Equal(DarlingMcpTestData.Naive(newest), row.LastCollection);
            Assert.Equal(oracleRow, row.LastCollection);

            if (timescaleEnabled)
            {
                /* Parameterized to this sentinel alone, rather than EXPLAINing the full ServerListSql: that
                   statement has no server scope (it lists the whole fleet by design), so its plan shape on a
                   shared rig depends on how many OTHER enabled servers other suites left behind - which
                   varies run to run and is not this test's to control. This is ServerListSql's own LATERAL
                   body (asserted below, ignoring formatting), parameterized, so it proves the identical
                   per-server mechanism without that noise. */
                const string LateralProbe = "SELECT cl.collection_time FROM v_collection_log cl WHERE cl.server_id = $1 ORDER BY cl.collection_time DESC LIMIT 1";
                var flattenedServerListSql = Regex.Replace(DarlingDataReader.ServerListSql, @"\s+", " ");
                Assert.Contains(
                    LateralProbe.Replace("cl.server_id = $1", "cl.server_id = s.server_id", StringComparison.Ordinal),
                    flattenedServerListSql,
                    StringComparison.Ordinal);
                var shipped = await ExplainAsync(connection, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + LateralProbe, [CollectingServerId], ct);
                Assert.True(IsOrderedEarlyExitDescent(shipped), $"expected an ordered per-chunk descent that stops early:\n{shipped}");
            }

            /* The two per-server summary reads: same oracle-vs-shipped comparison, single scalar answer. */
            foreach (var (label, shippedSql) in new[]
            {
                ("viewer summary", ViewerDataService.ServerSummaryLastCollectionSql),
                ("mcp health summary", DarlingHealthReader.ServerSummaryLastCollectionSql),
            })
            {
                var shippedScalar = await ScalarAsync(connection, shippedSql, CollectingServerId, ct);
                var oracleScalar = await ScalarAsync(connection, OldSummaryLastCollectionSql, CollectingServerId, ct);
                Assert.Equal(oracleScalar, shippedScalar);
                Assert.Equal(DarlingMcpTestData.Naive(newest), shippedScalar);

                if (timescaleEnabled)
                {
                    var shippedPlan = await ExplainAsync(connection, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + shippedSql, [CollectingServerId], ct);
                    Assert.True(IsOrderedEarlyExitDescent(shippedPlan), $"{label}: expected an ordered per-chunk descent that stops early:\n{shippedPlan}");
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>An ordered per-chunk descent (<c>Order: ... collection_time DESC</c>) under a <c>Limit</c>
    /// that stops before opening every chunk — either the classic <c>ChunkAppend</c> shape (later children
    /// marked "(never executed)") or TimescaleDB's <c>DeferredChunkAppend</c>, which decides how many chunks
    /// to touch at runtime and reports it directly ("Chunks Visited: N"). Both are the early-exit descent this
    /// fix is about; neither is the old correlated MAX's plan, which opens every chunk unconditionally.</summary>
    private static bool IsOrderedEarlyExitDescent(string plan) =>
        Regex.IsMatch(plan, @"Order: \S*collection_time DESC")
        && plan.Contains("Limit", StringComparison.Ordinal)
        && (plan.Contains("never executed", StringComparison.Ordinal)
            /* DeferredChunkAppend reports exactly how many chunks it opened for THIS server's descent,
               independent of how many chunks the shared hypertable holds overall - one or two here (the
               newest chunk with a row, plus at most the boundary chunk the descent started from) is the
               early-exit signature; anywhere near the seeded week's eight-plus chunks would not be. */
            || Regex.IsMatch(plan, @"Chunks Visited: [12]\b"));

    private static async Task<DateTime?> ScalarAsync(NpgsqlConnection connection, string sql, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(serverId);
        var result = await command.ExecuteScalarAsync(ct);
        return result is null || result == DBNull.Value ? null : Convert.ToDateTime(result);
    }

    private static Task InsertCollectionLogAsync(NpgsqlConnection connection, CancellationToken ct, int id, DateTime at) =>
        ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, 'server3976', 'wait_stats', $3, 'SUCCESS')",
            CollectionIdGenerator.Next(), id, at);

    /// <summary>Production-like density: <paramref name="rowCount"/> rows spread across the day ending at
    /// <paramref name="dayEnd"/>, one set-based INSERT with a unique log_id per row.</summary>
    private static Task InsertCollectionLogBatchAsync(NpgsqlConnection connection, CancellationToken ct, int id, DateTime dayEnd, int rowCount) =>
        ExecAsync(connection, ct, """
            INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
            SELECT 9_397_601_000_000 + (extract(epoch from $2::timestamp)::bigint * 1000) + row_number() OVER (),
                   $1, 'server3976', 'wait_stats', t, 'SUCCESS'
            FROM generate_series($2::timestamp - interval '1 day', $2::timestamp, interval '1 day' / $3) AS t
            """, id, dayEnd, rowCount);

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] args)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, object[] args, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        var plan = new StringBuilder();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", Array.ConvertAll(s_serverIds, id => id.ToString(CultureInfo.InvariantCulture)));
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id IN ({ids}); DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
