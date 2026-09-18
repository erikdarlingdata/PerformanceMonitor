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
/// #3573: the alerting pass's forced-plan-failures read and the covering index that gives it an access path
/// the planner will actually take.
///
/// <para><b>What went wrong is not that an index was missing.</b> V1 generates
/// <c>idx_query_store_stats_time (server_id, collection_time)</c>, the exact composite the read's predicate
/// wants, and the production catalog carried it on the hypertable and on the chunk in the failing plan. The
/// planner priced it out: <c>server_id</c> has near-zero physical correlation (43 servers interleaved by
/// collection pass), so the cost model charged one random page per tuple for the composite's heap fetches
/// and preferred streaming the entire fleet's two-hour slice through the perfectly-correlated time index
/// and filtering 95% of it away. Forced under <c>random_page_cost = 1.1</c> the same statement took the
/// composite and touched 5,063 buffers instead of 57,307 \u2014 the composite was right all along. A second plain
/// composite would have been priced, and ignored, identically.</para>
///
/// <para><b>Covering is the fix because it deletes the term the cost model got wrong.</b> With every column
/// the read touches in the key or INCLUDE, the plan is an Index Only Scan with no heap component to misprice,
/// at any <c>random_page_cost</c> and at any share of the fleet the busiest server grows into. That makes the
/// INCLUDE list load-bearing in a way most index definitions are not: a column added to the read and not to
/// the index does not fail anything \u2014 it silently hands the read back to the fleet-wide plan. The ungated pins
/// below hold the two against each other from source; the gated one asks the planner.</para>
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn
   cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class ForcePlanFailuresAccessPathTests
{
    /// <summary>Distinctive fake ids \u2014 a real server_id is a storage-name hash, never these. The target is
    /// one of six so its rows are ~17% of the seeded chunk, a share at which a seq scan is not competitive.</summary>
    private const int TargetServerId = -735730;
    private static readonly int[] OtherServerIds = { -735731, -735732, -735733, -735734, -735735 };
    private const string TestServerName = "force-plan-access-path-e2e";

    /// <summary>
    /// Every <c>qs.&lt;column&gt;</c> the shipped statement references, read from the statement itself. The
    /// alias is fixed by the SQL, so this is the complete set of columns the scan must produce.
    /// </summary>
    private static IReadOnlyCollection<string> ColumnsTheReadReferences()
    {
        return Regex.Matches(DarlingAlertReadAdapter.ForcePlanFailuresSql, @"\bqs\.([a-z_]+)")
            .Select(m => m.Groups[1].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(c => c, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// The read's column references and the index's column list are the SAME set, both ways. A column the
    /// read touches that the index lacks degrades the Index Only Scan to the heap plan it replaced, silently;
    /// a column the index carries that the read no longer touches is dead weight on every insert into the
    /// largest table in the store (INCLUDE disables deduplication, so each is real bytes per row).
    /// </summary>
    [Fact]
    public void TheCoveringIndex_CarriesExactlyTheColumnsTheReadReferences()
    {
        var referenced = ColumnsTheReadReferences();
        var carried = PgTableTuning.ForcePlanFailuresIndexColumns.OrderBy(c => c, StringComparer.Ordinal).ToList();

        Assert.Equal(carried, referenced);

        /* The predicate columns are the KEY, in predicate order: the equality column first so one server's
           rows are one contiguous index range, the range column second. Everything else is INCLUDE. */
        Assert.Equal("server_id", PgTableTuning.ForcePlanFailuresIndexColumns[0]);
        Assert.Equal("collection_time", PgTableTuning.ForcePlanFailuresIndexColumns[1]);
    }

    /// <summary>
    /// The statement text is BUILT from the same column list the pin above holds, so the two cannot drift: the
    /// literal SQL carries the key as <c>(server_id, collection_time DESC)</c> and the remaining columns, in
    /// order, as INCLUDE. Also pins the decisions the rig measured (see <see cref="PgTableTuning"/>): idempotent
    /// <c>IF NOT EXISTS</c>; NOT <c>CONCURRENTLY</c>, which hypertables refuse; NOT
    /// <c>timescaledb.transaction_per_chunk</c>, whose mid-build cancel leaves an invalid parent index that the
    /// idempotent re-run then skips forever; and <c>collect.</c>-qualified like every neighbour.
    /// </summary>
    [Fact]
    public void TheStatement_IsBuiltFromTheColumnList_AndTakesNeitherMeasuredTrap()
    {
        var statements = PgTableTuning.Statements
            .Where(s => s.Contains(PgTableTuning.ForcePlanFailuresIndexName, StringComparison.Ordinal))
            .ToList();
        var statement = Assert.Single(statements);

        var columns = PgTableTuning.ForcePlanFailuresIndexColumns;
        var expected =
            "CREATE INDEX IF NOT EXISTS " + PgTableTuning.ForcePlanFailuresIndexName
            + " ON collect.query_store_stats (" + columns[0] + ", " + columns[1] + " DESC)"
            + " INCLUDE (" + string.Join(", ", columns.Skip(2)) + ")";
        Assert.Equal(expected, statement);

        Assert.DoesNotContain("CONCURRENTLY", statement, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("transaction_per_chunk", statement, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(" WHERE ", statement, StringComparison.Ordinal); /* not partial \u2014 see the statement's remarks */
    }

    /// <summary>
    /// #3579: the observation stamp is the LAST column of the shipped read and is <c>n.collection_time</c> — the
    /// newer sighting's collector clock — not a new <c>qs.</c> reference. Last, because the reader binds ordinals
    /// 0–6 to the seven pre-#3579 columns and an inserted column would silently shift every one of them onto
    /// its neighbour's type (a string read as a bigint fails; a bigint read as a bigint from the wrong column
    /// does not). Not a <c>qs.</c> reference, because the covering pin above re-derives the index list from
    /// exactly those references and a new one would demand a new INCLUDE column on the largest table in the
    /// store; <c>collection_time</c> is already in the key.
    /// </summary>
    [Fact]
    public void TheObservationStamp_IsTheLastColumn_AndIsTheNewerSightingsCollectionTime()
    {
        var sql = DarlingAlertReadAdapter.ForcePlanFailuresSql;
        var selectList = sql[sql.LastIndexOf("SELECT", StringComparison.Ordinal)..sql.IndexOf("FROM ranked AS n", StringComparison.Ordinal)];
        var columns = selectList.Replace("SELECT", "", StringComparison.Ordinal)
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal(8, columns.Length);
        Assert.Equal("n.failures AS total_failures", columns[6]);
        Assert.Equal("n.collection_time AS observed_at", columns[7]);

        /* The set of scan columns did not grow — the same nine the index carried before #3579. */
        Assert.Equal(PgTableTuning.ForcePlanFailuresIndexColumns.Count, ColumnsTheReadReferences().Count);
    }

    /// <summary>
    /// The evidence no string pin can give: that the planner TAKES the index for the shipped statement. The
    /// production failure was a plan choice, not a missing object \u2014 the right composite was in the catalog and
    /// the plan walked past it \u2014 so a test that only checked <c>pg_indexes</c> would have passed on the broken
    /// store. This builds the store the way the service does (ladder, then the hypertable conversion where
    /// TimescaleDB is present, then <see cref="PgTableTuning.ApplyAsync"/>), seeds six servers' rows in the
    /// collector's per-pass contiguous batches, and EXPLAINs the exact shipped SQL with its real bound
    /// parameters: the plan must be an Index Only Scan on the covering index and must NOT be the time-index
    /// scan filtering on <c>server_id</c> that the issue's plan showed.
    /// </summary>
    [Fact]
    public async Task TheShippedRead_PlansAsAnIndexOnlyScanOnTheCoveringIndex_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live access-path test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The service's own order: hypertables first (where the extension exists), so the index is created on
           a hypertable and propagates to chunks, then the tuning list. On a plain-PostgreSQL store the index is
           an ordinary btree and the plan assertion below holds the same way. #1922: probe on its own connection. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        await PgTableTuning.ApplyAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteTestRowsAsync(connection, ct);

            /* The index exists on the hypertable with the shipped definition. */
            using (var indexDef = new NpgsqlCommand(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'query_store_stats' AND indexname = $1", connection))
            {
                indexDef.Parameters.AddWithValue(PgTableTuning.ForcePlanFailuresIndexName);
                var def = await indexDef.ExecuteScalarAsync(ct) as string;
                Assert.NotNull(def);
                Assert.Contains("(server_id, collection_time DESC)", def, StringComparison.Ordinal);
                Assert.Contains("INCLUDE (" + string.Join(", ", PgTableTuning.ForcePlanFailuresIndexColumns.Skip(2)) + ")", def, StringComparison.Ordinal);
            }

            /* Eight passes fifteen minutes apart, all inside the read's two-hour window; each pass writes the six
               servers in turn, each server's batch contiguous \u2014 the write pattern that makes server_id's
               correlation near zero, which is the condition the production plan was chosen under. All
               Kind-Unspecified: naive-UTC storage, see PgCollectorRowWriter. */
            /* Floored to whole microseconds: PostgreSQL timestamp is microsecond-resolution and .NET ticks are
               100 ns, so a raw UtcNow does not survive the round trip and the #3579 stamp assertion below
               (tick-equality against what was seeded) would fail on any clock that is not itself
               microsecond-aligned — Windows' is not; the first CI run proved it by three ticks. */
            var rawNow = DateTime.UtcNow;
            var utcNow = DateTime.SpecifyKind(new DateTime(rawNow.Ticks - (rawNow.Ticks % 10)), DateTimeKind.Unspecified);
            for (var pass = 7; pass >= 0; pass--)
            {
                var collectionTime = utcNow.AddMinutes(-2 - pass * 15);
                foreach (var serverId in OtherServerIds.Take(3).Append(TargetServerId).Concat(OtherServerIds.Skip(3)))
                {
                    /* The target's forced plan climbs one failure per pass (pass 7 = 0 ... pass 0 = 7). */
                    var forcedFailures = serverId == TargetServerId ? 7L - pass : (long?)null;
                    await SeedPassAsync(connection, serverId, collectionTime, rows: 400, forcedFailures, ct);
                }
            }

            /* The planner needs the visibility map current (the product keeps it so with the insert-autovacuum
               override; a test cannot wait for autovacuum) and statistics for the rows just written. */
            using (var vacuum = new NpgsqlCommand("VACUUM ANALYZE collect.query_store_stats", connection))
            {
                await vacuum.ExecuteNonQueryAsync(ct);
            }

            var plan = await ExplainShippedReadAsync(connection, utcNow - DarlingAlertReadAdapter.ForcePlanFailureWindow, ct);

            /* The chunk copy of a hypertable index is named <chunk>_<index>, truncated to 63 characters, so match
               on the name's stable prefix rather than its whole. */
            Assert.Contains("Index Only Scan", plan, StringComparison.Ordinal);
            Assert.Contains("idx_query_store_stats_server_time", plan, StringComparison.Ordinal);
            Assert.DoesNotContain("collection_time_idx", plan, StringComparison.Ordinal);
            /* The issue's signature: server_id applied as a post-scan Filter (any alias, any parenthesisation)
               rather than inside the Index Cond. */
            Assert.False(Regex.IsMatch(plan, @"Filter: \(+(qs(_\d+)?\.)?server_id"),
                "server_id is being applied as a Filter after the scan — the fleet-wide plan is back:\n" + plan);

            /* And the read still answers correctly through the new path: the target's forced plan's counter
               rose between its two newest sightings (pass 1 -> pass 0), and nothing else did. */
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var adapter = new DarlingAlertReadAdapter(postgres);
            var failures = await adapter.GetForcePlanFailuresAsync(TargetServerId.ToString(CultureInfo.InvariantCulture), ct);
            var failure = Assert.Single(failures);
            Assert.Equal("ForcedDb", failure.DatabaseName);
            Assert.Equal(1L, failure.QueryId);
            Assert.Equal(10L, failure.PlanId);
            Assert.Equal(1L, failure.FailureDelta);
            Assert.Equal(7L, failure.TotalFailures);
            /* #3579: the observation's identity is the NEWEST sighting's collection_time (pass 0, two minutes
               ago), read back through the real Npgsql path and stamped Utc. Ticks-equal to what was seeded:
               the store holds naive UTC and the adapter only names the Kind, never shifts the value. */
            Assert.Equal((DateTime?)utcNow.AddMinutes(-2), failure.ObservedAtUtc);
            Assert.Equal(DateTimeKind.Utc, failure.ObservedAtUtc!.Value.Kind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// One server's batch for one pass: <paramref name="rows"/> ordinary rows plus, when
    /// <paramref name="forcedFailures"/> is given, one forced plan carrying that <c>force_failure_count</c>, so
    /// the caller can make the newest two sightings differ by exactly one. Written as a single multi-row INSERT
    /// so the batch lands as one contiguous run of heap pages, the way the collector's COPY does.
    /// </summary>
    private static async Task SeedPassAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, int rows, long? forcedFailures, CancellationToken ct)
    {
        var sql = new StringBuilder(
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, " +
            "execution_count, avg_duration_us, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason) " +
            "SELECT $1, $2, $3, $4, 'db_' || (g % 4), 1000 + g, (1000 + g) * 10, 10 + g, 500 + g, 'NONE', FALSE, 0, NULL " +
            "FROM generate_series(1, $5) AS g");
        using (var insert = new NpgsqlCommand(sql.ToString(), connection))
        {
            insert.Parameters.AddWithValue(1L);
            insert.Parameters.AddWithValue(collectionTime);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(TestServerName);
            insert.Parameters.AddWithValue(rows);
            await insert.ExecuteNonQueryAsync(ct);
        }

        if (forcedFailures is null)
        {
            return;
        }

        using var forced = new NpgsqlCommand(
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, " +
            "execution_count, avg_duration_us, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason) " +
            "VALUES ($1, $2, $3, $4, 'ForcedDb', 1, 10, 5, 900, 'MANUAL', TRUE, $5, 'GENERAL_FAILURE')", connection);
        forced.Parameters.AddWithValue(1L);
        forced.Parameters.AddWithValue(collectionTime);
        forced.Parameters.AddWithValue(serverId);
        forced.Parameters.AddWithValue(TestServerName);
        forced.Parameters.AddWithValue(forcedFailures.Value);
        await forced.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainShippedReadAsync(NpgsqlConnection connection, DateTime windowStart, CancellationToken ct)
    {
        using var explain = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + DarlingAlertReadAdapter.ForcePlanFailuresSql, connection);
        explain.Parameters.AddWithValue(TargetServerId);
        explain.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));
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
        var ids = string.Join(", ", OtherServerIds.Append(TargetServerId).Select(id => id.ToString(CultureInfo.InvariantCulture)));
        using var cleanup = new NpgsqlCommand($"DELETE FROM collect.query_store_stats WHERE server_id IN ({ids})", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
