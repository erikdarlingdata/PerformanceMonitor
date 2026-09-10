/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1912 end-to-end against a REAL TimescaleDB: collapsing the pre-#1907 split slices in STORED raw rows, then
/// re-materializing, makes the corrected rollups report the interval's true totals instead of one slice.
///
/// <para>This cannot be a unit test. Every load-bearing fact is the engine's: that a forced refresh actually
/// recomputes changed source rows, that DML against a hypertable's chunks is even possible where the collapse
/// needs it, and — the one that shapes the whole verb — that a refresh whose range lies entirely within
/// DROPPED chunks DESTROYS the materialization there rather than skipping it. A C# test can assert we emit a
/// string; only a live store says what the string then does.</para>
///
/// <para><b>#1776 own-store</b> — mints its own scratch database rather than sharing the live fixture, because
/// it creates continuous aggregates the shared fixture must never inherit.</para>
/// </summary>
public sealed class QueryStoreSliceRepairLiveTests
{
    [Fact]
    public void SliceStatementTimeout_IsGenerousButBounded()
    {
        /* #2105 field failure: Npgsql's default 30s killed the stage aggregation on a store fresh
           off a large catch-up, surfacing as "Exception while reading from stream" with no mention
           of a timeout. Bounded on purpose - the slice transaction holds chunk locks the live
           service's compression jobs also want, so infinite (the VACUUM precedent) is wrong here. */
        Assert.Equal(900, QueryStoreSliceRepair.SliceStatementTimeoutSeconds);
    }

    [Fact]
    public void TheCollapseStagesEveryPayloadColumn_BecauseTheWIDTHIsWhatKeepsThePlanSafe()
    {
        /* #2876. Lite's collapse (#2771) OOM'd pushing query_plan_text through a per-group aggregate and was
           rewritten to narrow the staged projection. The obvious reading is that Darling should follow; the
           measurement says the opposite, and this pin exists so the "obvious" change cannot be made quietly.

           Measured on PostgreSQL 18 at 31,426 and 51,426 split groups carrying 3,033 MB of decompressed plan
           text: the SHIPPED statement's ~50 aggregate transition states per group make GroupAggregate win on
           cost, which spills through an external merge sort (125 MB temp, negligible resident). A cut-down
           three-column form of the same query instead plans as an UNSPILLED HashAggregate at 1.6 GB.

           So the breadth is load-bearing: narrowing this projection toward Lite's shape moves it toward the
           very plan that is dangerous. Asserted over the collector's own PayloadColumns rather than a literal
           count, so a column added to the collector has to appear here too. */
        var sql = QueryStoreSliceRepair.BuildCollapseSql();
        var staged = sql[..sql.IndexOf("DELETE FROM", StringComparison.Ordinal)];

        var payload = QueryStoreCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.True(payload.Length >= 40, $"expected a wide payload, saw {payload.Length}");

        /* Matched on a WORD BOUNDARY rather than an assumed delimiter. Working copies are CRLF
           (.gitattributes eol=crlf) so the raw string literal carries \r\n, and a suffix check for "\n"
           silently misses the final column — which is exactly how the first draft of this pin failed. */
        var missing = payload.Where(name => !StagesColumn(staged, name)).ToArray();

        Assert.True(
            missing.Length == 0,
            "the staged projection dropped payload column(s), which moves the plan toward the unspilled "
                + "HashAggregate #2876 measured at 1.6 GB: " + string.Join(", ", missing));
    }

    /// <summary>
    /// Whether the staged projection names <paramref name="column"/> — as an aggregate alias or carried
    /// through as a key — requiring a non-identifier character after it so <c>min_dop</c> cannot be
    /// satisfied by <c>min_dop_something</c>.
    /// </summary>
    private static bool StagesColumn(string staged, string column)
    {
        foreach (var prefix in new[] { " AS ", "s." })
        {
            var needle = prefix + column;
            var at = -1;

            while ((at = staged.IndexOf(needle, at + 1, StringComparison.Ordinal)) >= 0)
            {
                var end = at + needle.Length;

                if (end >= staged.Length)
                {
                    return true;
                }

                var next = staged[end];

                if (!char.IsLetterOrDigit(next) && next != '_')
                {
                    return true;
                }
            }
        }

        return false;
    }

    private const int TestServerId = -919120;

    /// <summary>
    /// The live SQL 2022 repro's arithmetic, as the pre-#1907 collector would have stored it: one interval
    /// arriving as a FLUSHED slice of 100 executions and an IN-MEMORY slice of 25, at ONE collection_time.
    /// The interval's truth is 125, cross-checked in #1907 against sys.dm_exec_procedure_stats.
    /// </summary>
    private const long FlushedCount = 100;
    private const long MemoryCount = 25;
    private const long TrueCount = FlushedCount + MemoryCount;
    private const long FlushedAvgUs = 1778;
    private const long MemoryAvgUs = 2245;

    /// <summary>(1778*100 + 2245*25) / 125 — the count-weighted mean, which is NOT (1778+2245)/2 = 2011.</summary>
    private const long TrueWeightedAvgUs = ((FlushedAvgUs * FlushedCount) + (MemoryAvgUs * MemoryCount)) / TrueCount;

    [Fact]
    public async Task CollapsingStoredSlices_ThenRefreshing_MakesTheRollupsReportTheIntervalsTruth()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1912 slice-repair test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var hour = new DateTime(2026, 6, 10, 14, 0, 0, DateTimeKind.Unspecified);

        /* One interval, collected across three cycles, EACH cycle storing the two tied slices — the pre-#1907
           shape exactly. Cumulative growth across cycles is the #1841 re-collection the dedup already
           handles; the split WITHIN each cycle is what #1912 repairs. */
        foreach (var (minute, flushed, memory) in new[] { (5, 40L, 10L), (10, 70L, 18L), (15, FlushedCount, MemoryCount) })
        {
            await SeedSliceAsync(connection, hour.AddMinutes(minute), intervalId: 8001, queryId: 71, planId: 91,
                intervalStart: hour, executionCount: flushed, avgDurationUs: FlushedAvgUs, ct: ct);
            await SeedSliceAsync(connection, hour.AddMinutes(minute), intervalId: 8001, queryId: 71, planId: 91,
                intervalStart: hour, executionCount: memory, avgDurationUs: MemoryAvgUs, ct: ct);
        }

        /* A correctly-collected interval alongside it: ONE row per cycle, as the collector emits since #1907.
           It must come through the repair untouched — the signature cannot match it, and proving that is what
           makes the verb safe to run on a store that is mostly healthy. */
        await SeedSliceAsync(connection, hour.AddMinutes(20), intervalId: 8002, queryId: 72, planId: 92,
            intervalStart: hour, executionCount: 55, avgDurationUs: 500, ct: ct);

        await EnsureAggregatesAsync(connection, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, hour, hour.AddHours(1), force: true, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, hour, hour.AddHours(1), force: true, ct);

        /* ── 1. BEFORE: the rollup carries one SLICE, never the interval's truth. Which slice last() picks is
               not asserted — it has no tie-break, and that it CANNOT reach 125 is the defect. ── */
        var before = await ReadIntervalCountAsync(connection, queryId: 71, ct);
        Assert.Contains(before, new long[] { FlushedCount, MemoryCount });
        Assert.NotEqual(TrueCount, before);

        /* ── 2. SURVEY: the pre-fix signature finds exactly the three split cycles and nothing else. ── */
        var survey = await QueryStoreSliceRepair.SurveyAsync(connection, ct);
        Assert.Equal(3, survey.SplitGroups);
        Assert.Equal(6, survey.SplitRows);
        Assert.Equal(3, survey.RowsRemoved);
        Assert.Equal(hour.AddMinutes(5), survey.OldestUtc);
        Assert.Equal(hour.AddMinutes(15), survey.NewestUtc);

        /* ── 3. COLLAPSE, over the range the survey measured — never a nominal window, so the refresh below
               can never reach under raw's own extent. ── */
        var removed = await QueryStoreSliceRepair.CollapseSliceAsync(
            connection, survey.OldestUtc!.Value, survey.NewestUtc!.Value.AddSeconds(1), ct);
        Assert.Equal(3, removed);

        /* The healthy interval is untouched: still exactly one row, still 55. */
        Assert.Equal(1, await RawRowCountAsync(connection, queryId: 72, ct));
        Assert.Equal(55, await RawExecutionCountAsync(connection, queryId: 72, ct));

        /* And the repaired interval is now ONE row per cycle carrying the combined values. */
        Assert.Equal(3, await RawRowCountAsync(connection, queryId: 71, ct));

        /* ── 4. IDEMPOTENT: the signature cannot match post-collapse rows, so a re-run is a no-op. That is
               what makes the verb safe to re-run, which an operator will do. ── */
        var second = await QueryStoreSliceRepair.SurveyAsync(connection, ct);
        Assert.False(second.HasWork);
        Assert.Equal(0, await QueryStoreSliceRepair.CollapseSliceAsync(
            connection, hour, hour.AddHours(1), ct));

        /* ── 5. RE-MATERIALIZE and the rollup now reports the interval's truth, weighted mean included. ── */
        await RefreshAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, hour, hour.AddHours(1), force: true, ct);

        var after = await ReadIntervalCountAsync(connection, queryId: 71, ct);
        Assert.Equal(TrueCount, after);

        var avg = await ReadIntervalAvgAsync(connection, queryId: 71, ct);
        Assert.Equal(TrueWeightedAvgUs, avg);
        Assert.NotEqual((FlushedAvgUs + MemoryAvgUs) / 2, avg);
    }

    /// <summary>
    /// The safety property the whole verb is clamped around, asserted rather than trusted: a refresh whose
    /// range lies entirely within DROPPED raw chunks DESTROYS the materialization there — with force and
    /// without. Measured on PG 18.4 + TimescaleDB 2.28.1 while designing #1912.
    ///
    /// <para>Pinned here because it is an ENGINE behavior no code of ours controls, it is not documented, and
    /// the consequence of forgetting it is blanking the 21-day hourly and the indefinitely-kept daily below
    /// raw's floor — the one thing #1759/#1793 forbid. If a future TimescaleDB makes this safe, this test
    /// fails and the clamp can be revisited deliberately instead of by accident.</para>
    /// </summary>
    [Fact]
    public async Task ARefreshEntirelyBelowRawsFloor_DestroysMaterializedHistory_WhichIsWhyTheRepairIsClamped()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1912 refresh-clamp test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* #1940: this test builds a RAW scratch hypertable with no migrations, so the scratch database has
           no collect/config schemas. A DARLING_TEST_PG carrying SearchPath=collect,config (the local-rig
           recipe's CI-parity pin) rides into the scratch connection string, leaving NO writable schema on
           the path - CREATE TABLE dies 3F000 and the extension's functions would not resolve either. The
           session path is set explicitly so the test is self-sufficient under BOTH connection-string
           flavors; the sibling tests in this class migrate first, which creates the pinned schemas, and are
           unaffected. */
        await ExecAsync(connection, "SET search_path = public", ct);

        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS timescaledb", ct);
        await ExecAsync(connection, "CREATE TABLE r (t timestamp NOT NULL, v bigint NOT NULL)", ct);
        await ExecAsync(connection, "SELECT create_hypertable('r', 't', chunk_time_interval => INTERVAL '1 day')", ct);
        await ExecAsync(connection,
            "INSERT INTO r SELECT g, 100 FROM generate_series(TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-10 23:00', INTERVAL '1 hour') g", ct);
        await ExecAsync(connection,
            "CREATE MATERIALIZED VIEW h WITH (timescaledb.continuous) AS SELECT time_bucket(INTERVAL '1 hour', t) AS bucket, sum(v) AS total FROM r GROUP BY 1 WITH NO DATA", ct);

        await CallAsync(connection, "CALL refresh_continuous_aggregate('h', NULL, NULL)", ct);
        Assert.Equal(240, await ScalarAsync(connection, "SELECT count(*) FROM h", ct));

        /* Retention drops the first six days from RAW; the rollup legitimately keeps them. */
        await ExecAsync(connection, "SELECT drop_chunks('r', older_than => TIMESTAMP '2026-01-07')", ct);
        Assert.Equal(240, await ScalarAsync(connection, "SELECT count(*) FROM h", ct));

        /* A refresh aimed only at the dropped region wipes it. */
        await CallAsync(connection,
            "CALL refresh_continuous_aggregate('h', TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-07', force => TRUE)", ct);

        Assert.Equal(0, await ScalarAsync(connection, "SELECT count(*) FROM h WHERE bucket < TIMESTAMP '2026-01-07'", ct));
        Assert.Equal(96, await ScalarAsync(connection, "SELECT count(*) FROM h WHERE bucket >= TIMESTAMP '2026-01-07'", ct));
    }

    /// <summary>
    /// The operator verb end to end: a dry run that reports real numbers and changes NOTHING, then a real run
    /// that repairs, then a third run that finds nothing left. Driven through
    /// <c>DarlingCliCommands.CollapseLegacySlicesAsync</c> with a bring-your-own darling.json pointed at the
    /// scratch store, so the wiring an operator actually invokes is what gets exercised.
    /// </summary>
    [Fact]
    public async Task CollapseVerb_DryRunsWithoutChanging_ThenRepairs_ThenFindsNothingLeft()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1912 verb test.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "--collapse-legacy-slices is Windows-only (DPAPI store credential in managed mode).");

        var ct = TestContext.Current.CancellationToken;
        var hour = new DateTime(2026, 6, 11, 9, 0, 0, DateTimeKind.Unspecified);

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(setup, null, ct));
            await TimescaleSupport.ConvertToHypertablesAsync(setup, null, ct);

            await SeedSliceAsync(setup, hour.AddMinutes(5), 8101, 81, 101, hour, FlushedCount, FlushedAvgUs, ct);
            await SeedSliceAsync(setup, hour.AddMinutes(5), 8101, 81, 101, hour, MemoryCount, MemoryAvgUs, ct);

            await EnsureAggregatesAsync(setup, ct);
        }

        var configPath = Path.Combine(Path.GetTempPath(), $"darling-1912-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(
            configPath,
            $$"""{"postgres":{"managed":false,"connectionString":{{System.Text.Json.JsonSerializer.Serialize(scratch.ConnectionString)}}},"servers":[]}""",
            ct);

        try
        {
            /* ── DRY RUN: real numbers, zero writes. A dry run that quietly repaired would be the worst bug a
                  verb like this could have — it exists so an operator can look before committing. ── */
            var dryOut = new StringWriter();
            Assert.Equal(0, await DarlingCliCommands.CollapseLegacySlicesAsync(configPath, dryRun: true, dryOut, new StringWriter(), ct));

            var dryText = dryOut.ToString();
            Assert.Contains("Split intervals found : 1", dryText, StringComparison.Ordinal);
            Assert.Contains("DRY RUN — nothing was changed.", dryText, StringComparison.Ordinal);
            Assert.DoesNotContain("[OK]", dryText, StringComparison.Ordinal);

            await using (var check = new NpgsqlConnection(scratch.ConnectionString))
            {
                await check.OpenAsync(ct);
                Assert.Equal(2, await RawRowCountAsync(check, 81, ct));
            }

            /* ── REAL RUN. ── */
            var runOut = new StringWriter();
            Assert.Equal(0, await DarlingCliCommands.CollapseLegacySlicesAsync(configPath, dryRun: false, runOut, new StringWriter(), ct));

            var runText = runOut.ToString();
            Assert.Contains("Collapsed. Rows removed: 1", runText, StringComparison.Ordinal);
            Assert.Contains("DONE", runText, StringComparison.Ordinal);
            /* Per-slice progress: the real run announces each slice with its removal count and span
               percent — a big backlog is no longer a silent console between the banner and DONE. The
               removed figure inside the [OK] line is the same deleted-minus-reinserted derivation the
               summary total uses, so the two cannot disagree. */
            Assert.Contains("[OK]", runText, StringComparison.Ordinal);
            Assert.Contains("1 removed (100% of span, 1 total)", runText, StringComparison.Ordinal);

            await using (var check = new NpgsqlConnection(scratch.ConnectionString))
            {
                await check.OpenAsync(ct);
                Assert.Equal(1, await RawRowCountAsync(check, 81, ct));
                Assert.Equal(TrueCount, await RawExecutionCountAsync(check, 81, ct));
                Assert.Equal(TrueCount, await ReadIntervalCountAsync(check, 81, ct));
            }

            /* ── THIRD RUN: nothing left, and it says so rather than reporting a repair of zero rows. ── */
            var againOut = new StringWriter();
            Assert.Equal(0, await DarlingCliCommands.CollapseLegacySlicesAsync(configPath, dryRun: false, againOut, new StringWriter(), ct));
            Assert.Contains("Nothing to repair", againOut.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(configPath))
            {
                File.Delete(configPath);
            }
        }
    }

    /// <summary>
    /// The SECOND #2105 field failure, pinned the way DarlingRetentionTests pins the first of this class
    /// (#1564): the collapse's DELETE touches COMPRESSED chunks — a store old enough to need this repair has
    /// had its compression policy running the whole time — and TimescaleDB rails DML decompression at 100k
    /// tuples per transaction by default, so the field run died at <c>53400: tuple decompression limit
    /// exceeded</c> four minutes in. The fix is the <c>SET LOCAL ... = 0</c> lift at the top of the slice
    /// transaction, and this test is its tripwire: the session arms the rail at ONE tuple before calling the
    /// collapse, so the transaction-local lift is the only thing standing between the DELETE and the exact
    /// field error. Drop the lift and this fails with the operator's 53400 instead of a silent coverage hole.
    /// Compression is applied SYNCHRONOUSLY (the retention test's pattern — no background-job race), and the
    /// compressed shape is PROVEN before the repair runs, not assumed.
    /// </summary>
    [Fact]
    public async Task CollapsingRowsInsideACompressedChunk_SurvivesTheDecompressionRail_TheSecondFieldFailure()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #2105 compressed-chunk collapse test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var hour = new DateTime(2026, 6, 12, 8, 0, 0, DateTimeKind.Unspecified);
        await SeedSliceAsync(connection, hour.AddMinutes(5), intervalId: 8201, queryId: 91, planId: 111,
            intervalStart: hour, executionCount: FlushedCount, avgDurationUs: FlushedAvgUs, ct: ct);
        await SeedSliceAsync(connection, hour.AddMinutes(5), intervalId: 8201, queryId: 91, planId: 111,
            intervalStart: hour, executionCount: MemoryCount, avgDurationUs: MemoryAvgUs, ct: ct);

        /* Compression enablement lives in ApplyCompressionPolicyAsync (a separate service-start step this
           test deliberately skips — a background policy racing the assertions is pure interference), so
           enable it directly, exactly like the retention test's compressed-chunk pin; then compress the
           seeded chunk synchronously and PROVE the compressed shape is what the collapse runs against. */
        await ExecAsync(connection,
            "ALTER TABLE collect.query_store_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
        await ExecAsync(connection,
            "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('collect.query_store_stats') c", ct);
        Assert.True(await ScalarAsync(connection, @"
SELECT count(*)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'query_store_stats'
  AND is_compressed", ct) >= 1,
            "expected the seeded query_store_stats chunk to be compressed — the fixture is not exercising the compressed-chunk shape");

        /* Arm the rail at ONE tuple for this session. The DELETE must decompress the seeded rows' batch
           (two tuples at minimum), so only the transaction-local SET LOCAL lift lets the collapse commit. */
        await ExecAsync(connection, "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 1", ct);

        var removed = await QueryStoreSliceRepair.CollapseSliceAsync(connection, hour, hour.AddHours(1), ct);
        Assert.Equal(1, removed);

        Assert.Equal(1, await RawRowCountAsync(connection, queryId: 91, ct));
        Assert.Equal(TrueCount, await RawExecutionCountAsync(connection, queryId: 91, ct));
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    private static async Task SeedSliceAsync(
        NpgsqlConnection connection, DateTime collectionTime, long intervalId, long queryId, long planId,
        DateTime intervalStart, long executionCount, long avgDurationUs, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
VALUES
    ((extract(epoch FROM $1)::bigint * 100000) + $2 + $7, $1, $3, 'SQL01', 'AdventureWorks', 'dbo.GetOrders', '0xABCD',
     $4, $5, 'Regular', 'Primary', $2, $6, $6, $1, $7, $8, 50, 998, 4708)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(intervalId);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(intervalStart);
        command.Parameters.AddWithValue(executionCount);
        command.Parameters.AddWithValue(avgDurationUs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task EnsureAggregatesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Strip the refresh policies the sweep attaches: TimescaleDB runs a new policy's first check
           IMMEDIATELY (#1564/#1567), and a background refresh racing these exact-value assertions is pure
           interference. Same reason QueryStoreCorrectedRollupLiveTests strips them. */
        await ExecAsync(connection, @"
DO $$
DECLARE j record;
BEGIN
    FOR j IN SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate'
    LOOP
        PERFORM delete_job(j.job_id);
    END LOOP;
END $$;", ct);
    }

    private static async Task RefreshAsync(
        NpgsqlConnection connection, string view, DateTime from, DateTime to, bool force, CancellationToken ct)
    {
        var sql = $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp{(force ? ", force => TRUE" : "")})";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(from);
        command.Parameters.AddWithValue(to);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ReadIntervalCountAsync(NpgsqlConnection connection, long queryId, CancellationToken ct)
        => await ScalarAsync(connection,
            $"SELECT coalesce(sum(execution_count), 0) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView} WHERE query_id = {queryId}", ct);

    /// <summary>
    /// L1 carries the interval's own <c>avg_duration_us</c> through <c>last(...)</c> — the weighted SUMS live
    /// one level up in the corrected hourly, which rebuilds them from these values. So the number to assert
    /// here is the average the collapse wrote into raw, read back through the rollup unchanged.
    /// </summary>
    private static async Task<long> ReadIntervalAvgAsync(NpgsqlConnection connection, long queryId, CancellationToken ct)
        => await ScalarAsync(connection,
            $"SELECT coalesce(max(avg_duration_us), 0) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView} WHERE query_id = {queryId}", ct);

    private static async Task<long> RawRowCountAsync(NpgsqlConnection connection, long queryId, CancellationToken ct)
        => await ScalarAsync(connection, $"SELECT count(*) FROM collect.query_store_stats WHERE query_id = {queryId}", ct);

    private static async Task<long> RawExecutionCountAsync(NpgsqlConnection connection, long queryId, CancellationToken ct)
        => await ScalarAsync(connection, $"SELECT coalesce(sum(execution_count), 0) FROM collect.query_store_stats WHERE query_id = {queryId}", ct);

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? 0L : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task CallAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
        => ExecAsync(connection, sql, ct);
}
