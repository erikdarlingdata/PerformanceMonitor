/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3597: the capture-down self-alert read asks "what did this collector's LATEST run log?" once per
/// collector as a chunk-orderable <c>LIMIT 1</c>, not as a window function over the server's whole
/// <c>collection_log</c> history.
///
/// <para>This is the read #3496 named and deliberately left — <c>ROW_NUMBER() OVER (PARTITION BY
/// collector_name ORDER BY log_id DESC)</c> cannot early-stop by reordering alone. <c>collection_log</c> is a
/// hypertable partitioned on <c>collection_time</c> with no index on <c>log_id</c>, so the window's only legal
/// plan decompressed EVERY chunk in the server's retention horizon, Merge-Appended them and numbered ~100 K
/// rows to keep two: 9,573 buffers over 61 chunks on a rig with 60 days of one server's log, every alert
/// pass, every 30 seconds, per server — the heaviest read on the pass by two to three orders of magnitude,
/// and one of the four sites the issue saw die at the 10 s deadline while the interval-hourly refresh
/// starved the store. Per collector, <c>ORDER BY collection_time DESC LIMIT 1</c> lets ChunkAppend walk the
/// chunks newest-first and stop at the first row: 14 buffers, the newest chunk only, 120 of 122 chunk scans
/// never executed. The property is horizon-independent, so a longer retention cannot regress it.</para>
///
/// <para>The regression is QUIET, exactly as #3496's was: a revert to the window returns the same two rows on
/// any store small enough for a test and only shows up as deadline breaches once a store's retention has
/// filled. So the shape is pinned at the source, and the gated arm asks the planner.</para>
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn
   cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class CaptureDownChunkOrderTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -735971;
    private const string TestServerName = "capture-down-chunk-order-e2e";

    [Fact]
    public void EachCollector_IsAskedOnce_OrderedByThePartitionColumn_LimitOne()
    {
        var sql = DarlingSelfAlertEvaluator.MissingCaptureSessionsSql;

        /* One arm per capture collector, UNION ALL between them, each stopping at its newest row. */
        Assert.Equal(2, Regex.Matches(sql, @"ORDER BY cl\.collection_time DESC\s+LIMIT 1").Count);
        Assert.Single(Regex.Matches(sql, @"\bUNION ALL\b"));
        Assert.Contains("cl.collector_name = 'deadlocks'", sql, StringComparison.Ordinal);
        Assert.Contains("cl.collector_name = 'blocked_process_report'", sql, StringComparison.Ordinal);

        /* The verdict is still on the LATEST run's status, applied after each arm has picked its row. */
        Assert.Contains("WHERE x.status = 'SESSION_MISSING'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The negative half: no window function, and no ordering by <c>log_id</c> in any spelling — the two
    /// shapes that force every chunk to execute. Matched as shapes rather than the literals that shipped, so a
    /// re-spelling cannot slip past the pin the way the original slipped past review.
    /// </summary>
    [Fact]
    public void NoWindowFunction_AndNoOrderingByLogId()
    {
        var sql = DarlingSelfAlertEvaluator.MissingCaptureSessionsSql;

        Assert.DoesNotMatch(new Regex(@"\bOVER\s*\(", RegexOptions.IgnoreCase), sql);
        Assert.DoesNotMatch(new Regex(@"ROW_NUMBER", RegexOptions.IgnoreCase), sql);
        Assert.DoesNotMatch(new Regex(@"log_id", RegexOptions.IgnoreCase), sql);
    }

    /// <summary>
    /// The evidence no string pin can give: that the planner stops at the newest chunk. Builds the store the
    /// way the service does (ladder, then <c>collection_log</c>'s hypertable conversion where TimescaleDB is
    /// present), seeds one server's capture-collector rows across eight days — eight 1-day chunks — and
    /// EXPLAINs the shipped statement with its real bound parameter: no <c>WindowAgg</c>, and at most one
    /// chunk executed per arm, every other chunk scan reported <c>never executed</c>. Then the read answers
    /// through the same path, and it is the NEWEST run that decides: a <c>SESSION_MISSING</c> three days ago
    /// followed by a success is not a missing session; a success three days ago followed by
    /// <c>SESSION_MISSING</c> is.
    /// </summary>
    [Fact]
    public async Task TheShippedRead_ExecutesOnlyTheNewestChunk_AndTheNewestRunDecides_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live capture-down access-path test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* #1922: probe on its own connection. The service's own runtime conversion for collection_log, which sits
           outside the collector catalog and so outside ConvertToHypertablesAsync. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        }

        var bodySucceeded = false;
        try
        {
            await DeleteTestRowsAsync(connection, ct);

            /* Eight days of one-minute blocked_process_report and five-minute deadlocks rows, plus a filler
               collector so the newest chunk holds rows the arms must skip past. All Kind-Unspecified: naive-UTC
               storage, see DarlingObservability.LogCollectionAsync. */
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await SeedHistoryAsync(connection, utcNow, ct);

            /* The two verdict rows. deadlocks: SESSION_MISSING three days ago, then a SUCCESS a minute ago — NOT
               missing. blocked_process_report: SUCCESS all along, then SESSION_MISSING a minute ago — missing. */
            await InsertAsync(connection, 9_100_000_001, "deadlocks", utcNow.AddDays(-3).AddSeconds(7), "SESSION_MISSING", ct);
            await InsertAsync(connection, 9_100_000_002, "deadlocks", utcNow.AddMinutes(-1), "SUCCESS", ct);
            await InsertAsync(connection, 9_100_000_003, "blocked_process_report", utcNow.AddMinutes(-1), "SESSION_MISSING", ct);

            using (var analyze = new NpgsqlCommand("ANALYZE collect.collection_log", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            var plan = await ExplainShippedReadAsync(connection, ct);

            Assert.DoesNotContain("WindowAgg", plan, StringComparison.Ordinal);

            if (timescaleEnabled)
            {
                /* Every chunk scan the plan carries, split into executed and never-executed. ChunkAppend orders
                   the chunks newest-first for ORDER BY collection_time DESC, so each arm's LIMIT 1 is satisfied
                   by the newest chunk and the rest never start. From TimescaleDB 2.30 the same read plans as
                   DeferredChunkAppend (on by default), which lists only the chunks it visited and counts them,
                   so there the proof is one visited chunk per arm (#3908, measured on 2.30.1). */
                var lines = plan.Split('\n');
                var chunkScans = lines.Where(l => Regex.IsMatch(l, @"Scan .* on _hyper_\d+_\d+_chunk")).ToList();
                var visited = lines
                    .Select(l => Regex.Match(l, @"Chunks Visited: (\d+)"))
                    .Where(m => m.Success)
                    .Select(m => int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture))
                    .ToList();
                if (visited.Count > 0)
                {
                    Assert.True(visited.Count == 2 && visited.All(v => v == 1),
                        "expected each arm's deferred chunk append to visit exactly the newest chunk:\n" + plan);
                }
                else
                {
                    Assert.True(chunkScans.Count >= 2 * 8,
                        "expected the plan to carry at least eight chunk scans per arm (eight seeded days):\n" + plan);
                }

                var executed = chunkScans.Where(l => !l.Contains("never executed", StringComparison.Ordinal)).ToList();
                Assert.True(executed.Count <= 2,
                    "more than one chunk executed per arm — the read is walking history again:\n" + plan);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            /* #3854: an instance method now — the read goes out through the shared retry seam, whose
               counter and pause live on the evaluator. The read itself is unchanged, which is what this
               chunk-order pin is about. */
            var h = new DarlingSelfAlertTests.Harness();
            var evaluator = new DarlingSelfAlertEvaluator(h.Settings, h.Deliverer, h.History, _ => false);
            var missing = await evaluator.ReadMissingCaptureSessionsAsync(postgres, TestServerId, ct);
            Assert.Equal(new[] { "Blocking" }, missing);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// One multi-row INSERT per collector over the eight days, so the seed is three statements rather than
    /// fifteen thousand. log_id is derived from the row's instant so id order and time order agree, as
    /// CollectionIdGenerator's do.
    /// </summary>
    private static async Task SeedHistoryAsync(NpgsqlConnection connection, DateTime utcNow, CancellationToken ct)
    {
        var start = utcNow.AddDays(-8);
        foreach (var (collector, stepMinutes) in new[] { ("blocked_process_report", 1), ("deadlocks", 5), ("wait_stats", 1) })
        {
            using var insert = new NpgsqlCommand(
                "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) " +
                "SELECT 9_000_000_000 + (EXTRACT(EPOCH FROM t)::bigint * 10) + $5, $1, $2, $3, t, 20, 'SUCCESS', 0 " +
                "FROM generate_series($4::timestamp, $4::timestamp + interval '8 days' - interval '2 minutes', ($6::text || ' minutes')::interval) AS t", connection);
            insert.Parameters.AddWithValue(TestServerId);
            insert.Parameters.AddWithValue(TestServerName);
            insert.Parameters.AddWithValue(collector);
            insert.Parameters.AddWithValue(start);
            insert.Parameters.AddWithValue((long)stepMinutes);
            insert.Parameters.AddWithValue(stepMinutes.ToString(System.Globalization.CultureInfo.InvariantCulture));
            await insert.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, long logId, string collector, DateTime when, string status, CancellationToken ct)
    {
        using var insert = new NpgsqlCommand(
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) " +
            "VALUES ($1, $2, $3, $4, $5, 20, $6, 0)", connection);
        insert.Parameters.AddWithValue(logId);
        insert.Parameters.AddWithValue(TestServerId);
        insert.Parameters.AddWithValue(TestServerName);
        insert.Parameters.AddWithValue(collector);
        insert.Parameters.AddWithValue(when);
        insert.Parameters.AddWithValue(status);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainShippedReadAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var explain = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + DarlingSelfAlertEvaluator.MissingCaptureSessionsSql, connection);
        explain.Parameters.AddWithValue(TestServerId);
        var plan = new StringBuilder();
        using var reader = await explain.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM collect.collection_log WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(TestServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
