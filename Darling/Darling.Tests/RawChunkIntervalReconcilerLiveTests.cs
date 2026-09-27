/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4211's live half: <see cref="RawChunkIntervalReconciler.ReconcileAsync"/> against a real TimescaleDB
/// hypertable it compresses itself, and the bring-your-own budget read off a real <c>pg_settings</c>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Both live facts here build their own
   hypertable through ScratchPostgres and never touch the shared database's collector tables, so they cannot
   race the live collection and serializing them would be pure slowdown. Leave this out; this comment is here
   so the next sweep does not "fix" it. */
public sealed class RawChunkIntervalReconcilerLiveTests
{
    /// <summary>
    /// One hypertable, started at the ceiling rung (24h), with two closed chunks compressed synchronously (no
    /// background-job race — the #1564 precedent every live test here follows). A budget of 1 byte guarantees
    /// the narrowing pass fires no matter what the compressed bytes actually are, which keeps the test from
    /// depending on TimescaleDB's compression ratio for this run's data.
    ///
    /// <para><b>Phase 1</b> proves the apply + history write: one rung narrower in
    /// <c>timescaledb_information.dimensions</c>, exactly one <c>raw_chunk_interval_rung_history</c> row, one
    /// <c>raw_chunk_interval_reconcile_runs</c> row.</para>
    ///
    /// <para><b>Phase 2</b> (same UTC day) proves <see cref="RawChunkIntervalPlanner.MinimumDaysBetweenMoves"/>
    /// holds ACROSS calls because <c>IntervalLastChangedUtc</c> is read back from phase 1's history row, not
    /// held in memory: interval and history row count are both unchanged, but a second run row is written. This
    /// is the exact assertion that would fail if the gathering query stopped reading
    /// <c>raw_chunk_interval_rung_history</c> for the last-changed stamp — proven once by hand: with that read
    /// swapped for a literal <c>null</c>, this phase narrows a second time (12h -> 6h) and the row-count assert
    /// fails; restoring the read passes it again.</para>
    ///
    /// <para><b>Phase 3</b> re-runs the V144 migration SQL directly (bypassing the version gate that would
    /// otherwise skip it) and proves it is a no-op: same interval, same two row counts.</para>
    /// </summary>
    [Fact]
    public async Task EndToEnd_NarrowsOneRungOverBudget_ThenHoldsForOneDayFromHistory_AndMigrationReapplyIsANoOp_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live raw chunk-interval reconcile test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB is not available in the DARLING_TEST_PG store — the reconcile needs it.");

        await ExecAsync(connection, "CREATE TABLE collect.rci_test (ts timestamp NOT NULL, val text NOT NULL)", ct);
        await ExecAsync(connection,
            "SELECT create_hypertable('collect.rci_test', by_range('ts', INTERVAL '24 hours'), if_not_exists => true, migrate_data => true)", ct);
        await ExecAsync(connection, "ALTER TABLE collect.rci_test SET (timescaledb.compress, timescaledb.compress_orderby = 'ts')", ct);

        var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await PlantChunkAsync(connection, utcNow.AddDays(-3), ct);
        await PlantChunkAsync(connection, utcNow.AddDays(-2), ct);
        await ExecAsync(connection, "SELECT compress_chunk(c) FROM show_chunks('collect.rci_test') c", ct);

        Assert.Equal(24, await CurrentIntervalHoursAsync(connection, ct));

        /* ---- phase 1: narrows one rung, applies, records ---- */
        var changed1 = await RawChunkIntervalReconciler.ReconcileAsync(connection, budgetBytes: 1.0, DateTime.UtcNow, logger: null, ct);
        Assert.Equal(1, changed1);
        Assert.Equal(12, await CurrentIntervalHoursAsync(connection, ct));

        await using (var history = new NpgsqlCommand(
            "SELECT table_name, from_interval_hours, to_interval_hours FROM collect.raw_chunk_interval_rung_history", connection))
        await using (var reader = await history.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct), "expected one rung history row after phase 1");
            Assert.Equal("rci_test", reader.GetString(0));
            Assert.Equal(24, reader.GetInt32(1));
            Assert.Equal(12, reader.GetInt32(2));
            Assert.False(await reader.ReadAsync(ct), "expected exactly one rung history row after phase 1");
        }

        Assert.Equal(1L, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.raw_chunk_interval_reconcile_runs", ct));

        /* ---- phase 2: same day, held — proves IntervalLastChangedUtc came from the history table ---- */
        var changed2 = await RawChunkIntervalReconciler.ReconcileAsync(connection, budgetBytes: 1.0, DateTime.UtcNow, logger: null, ct);
        Assert.Equal(0, changed2);
        Assert.Equal(12, await CurrentIntervalHoursAsync(connection, ct));
        Assert.Equal(1L, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.raw_chunk_interval_rung_history", ct));
        Assert.Equal(2L, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.raw_chunk_interval_reconcile_runs", ct));

        /* ---- phase 3: the migration SQL itself is idempotent, run raw a second time ---- */
        string? v144Sql = null;
        foreach (var migration in PgMigrations.Scripts)
        {
            if (migration.Version == 144)
            {
                v144Sql = migration.Sql;
                break;
            }
        }
        Assert.NotNull(v144Sql);
        await ExecAsync(connection, v144Sql!, ct);
        Assert.Equal(12, await CurrentIntervalHoursAsync(connection, ct));
        Assert.Equal(1L, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.raw_chunk_interval_rung_history", ct));
        Assert.Equal(2L, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.raw_chunk_interval_reconcile_runs", ct));
    }

    /// <summary>Plants one row a day before <paramref name="ts"/> and one AT <paramref name="ts"/>, so the chunk
    /// this falls in has a real, closed (not "today's still-open") range once the next day's chunk exists.</summary>
    private static async Task PlantChunkAsync(NpgsqlConnection connection, DateTime ts, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand("INSERT INTO collect.rci_test (ts, val) VALUES ($1, $2)", connection);
        insert.Parameters.AddWithValue(ts);
        insert.Parameters.AddWithValue(new string('x', 4096));
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> CurrentIntervalHoursAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT (EXTRACT(EPOCH FROM time_interval) / 3600.0)::integer
FROM timescaledb_information.dimensions
WHERE hypertable_schema = 'collect' AND hypertable_name = 'rci_test' AND dimension_type = 'Time'", connection);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The bring-your-own path (#4211 ruling decision 2): <c>max(shared_buffers, effective_cache_size /
    /// 3)</c> read live off <c>pg_settings</c> through <c>pg_size_bytes(current_setting(...))</c>, proven against
    /// an independent read of the SAME two GUCs rather than a hard-coded expectation, so this does not depend on
    /// the rig's particular defaults.</summary>
    [Fact]
    public async Task BringYourOwnBudgetBytes_ReadFromPgSettings_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live bring-your-own budget test.");

        var ct = TestContext.Current.CancellationToken;

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var config = new DarlingConfig();
        config.Postgres.Managed = false;

        var budget = await DarlingWorker.ResolveRawChunkIntervalBudgetBytesAsync(postgres, config, ct);
        Assert.NotNull(budget);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var independent = new NpgsqlCommand(
            "SELECT pg_size_bytes(current_setting('shared_buffers')), pg_size_bytes(current_setting('effective_cache_size'))", connection);
        await using var reader = await independent.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var expected = RawChunkIntervalPlanner.BringYourOwnBudgetBytes(reader.GetInt64(0), reader.GetInt64(1));

        Assert.Equal(expected, budget!.Value);
    }

    /// <summary>
    /// The production shape from #4457: 72 raw hypertables, 1,210 chunks total, the largest at 73 — a store
    /// well past the OLD store-wide 1,000-chunk cap, but where no single table is anywhere near it. One
    /// compressed, RATED table carries real ingest bytes; the other 71 are uncompressed filler, cheap to seed
    /// with <c>generate_series</c>, that exist only so <c>timescaledb_information.hypertables.num_chunks</c>
    /// sums past 1,000 on the store — they never enter <see cref="RawChunkIntervalReconciler.TableInputsSql"/>'s
    /// JOIN (no compressed chunk), so they cannot move and cannot affect the rated table's own decision. The
    /// budget is set so the rated table alone sits at 2.55x it, matching the ratio a real store's
    /// <c>query_stats</c> table sat at when this cap was store-wide. This proves the fix at the product's own
    /// call path — <see cref="RawChunkIntervalReconciler.ReconcileAsync"/> end to end, not the planner
    /// directly — and is also the RUNTIME RED against dev (#4457): on dev's store-wide cap, this store's total
    /// chunk count alone holds the rated table at 24h regardless of its own forecast.
    /// </summary>
    [Fact]
    public async Task ProductionShape_72Hypertables1210ChunksLargest73_NarrowsTheRatedTableOnItsOwnForecast_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live raw chunk-interval reconcile test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB is not available in the DARLING_TEST_PG store — the reconcile needs it.");

        /* 28 tables at 36-73 chunks (sum 1,069) + 43 tables at 3-4 chunks (sum 136) = 71 filler tables, 1,205
           chunks, largest 73 — plus the rated table's 5 gives 72 tables / 1,210 chunks / max 73, per the brief's
           shape. Uncompressed, so they never enter TableInputsSql's JOIN. */
        int[] fillerChunkCounts =
        {
            73, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 36, 36, 36,
            4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        };
        Assert.Equal(71, fillerChunkCounts.Length);
        Assert.Equal(1_205, fillerChunkCounts.Sum());
        Assert.Equal(73, fillerChunkCounts.Max());

        for (var i = 0; i < fillerChunkCounts.Length; i++)
        {
            await CreateFillerHypertableAsync(connection, $"filler_{i}", fillerChunkCounts[i], ct);
        }

        await ExecAsync(connection, "CREATE TABLE collect.rated_test (ts timestamp NOT NULL, val text NOT NULL)", ct);
        await ExecAsync(connection,
            "SELECT create_hypertable('collect.rated_test', by_range('ts', INTERVAL '24 hours'), if_not_exists => true, migrate_data => true)", ct);
        await ExecAsync(connection, "ALTER TABLE collect.rated_test SET (timescaledb.compress, timescaledb.compress_orderby = 'ts')", ct);

        var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        for (var day = 1; day <= 5; day++)
        {
            await PlantRatedChunkAsync(connection, utcNow.AddDays(-day), ct);
        }
        await ExecAsync(connection, "SELECT compress_chunk(c) FROM show_chunks('collect.rated_test') c", ct);

        Assert.Equal((72, 1_210, 73), await HypertableCensusAsync(connection, ct));
        Assert.Equal(24, await CurrentIntervalHoursAsync(connection, "rated_test", ct));

        /* The rated table's own ingest rate, read the same way the reconciler does — chunk_compression_stats()
           bytes over the compressed chunks' own wall-clock span — so the budget below is set from the SAME
           figure ReconcileAsync will compute, not a hand guess at it. */
        double ratePerHour;
        await using (var rateCommand = new NpgsqlCommand(@"
SELECT SUM(ccs.before_compression_total_bytes)::double precision
           / (SUM(EXTRACT(EPOCH FROM (c.range_end - c.range_start))) / 3600.0)
FROM chunk_compression_stats('collect.rated_test'::regclass) ccs
JOIN timescaledb_information.chunks c
  ON c.hypertable_schema = 'collect' AND c.hypertable_name = 'rated_test'
 AND c.chunk_schema = ccs.chunk_schema AND c.chunk_name = ccs.chunk_name
WHERE ccs.before_compression_total_bytes IS NOT NULL", connection))
        {
            ratePerHour = (double)(await rateCommand.ExecuteScalarAsync(ct))!;
        }

        var openBytesAt24Hours = ratePerHour * 24;
        var budgetBytes = openBytesAt24Hours / 2.55;

        var logger = new CapturingTestLogger();
        var changed = await RawChunkIntervalReconciler.ReconcileAsync(connection, budgetBytes, DateTime.UtcNow, logger, ct);

        Assert.Equal(1, changed);
        Assert.Equal(12, await CurrentIntervalHoursAsync(connection, "rated_test", ct));
        Assert.Equal(1L, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.raw_chunk_interval_rung_history WHERE table_name = 'rated_test'", ct));

        var summary = logger.Lines.FirstOrDefault(l => l.Contains("moved", StringComparison.Ordinal) && l.Contains("evaluated", StringComparison.Ordinal));
        Assert.NotNull(summary);
        Assert.Contains("1 moved", summary, StringComparison.Ordinal);

        /* ---- second arm: a huge budget, no table over its own forecast cap, so nothing moves and the
           summary line still logs at Information with the ratio under 1 ---- */
        var loggerNoChange = new CapturingTestLogger();
        var changedNoChange = await RawChunkIntervalReconciler.ReconcileAsync(
            connection, budgetBytes: openBytesAt24Hours * 1_000, DateTime.UtcNow, loggerNoChange, ct);

        Assert.Equal(0, changedNoChange);
        var summaryNoChange = loggerNoChange.Lines.FirstOrDefault(l => l.Contains("evaluated", StringComparison.Ordinal));
        Assert.NotNull(summaryNoChange);
        Assert.Contains("0 moved", summaryNoChange, StringComparison.Ordinal);
        Assert.Contains("0.", summaryNoChange, StringComparison.Ordinal); /* ratio under 1, e.g. "0.00x budget" */
    }

    /// <summary>Plants one row a day before <paramref name="ts"/> for <c>collect.rated_test</c>, the same
    /// closed-chunk pattern as <see cref="PlantChunkAsync"/> uses for <c>rci_test</c>.</summary>
    private static async Task PlantRatedChunkAsync(NpgsqlConnection connection, DateTime ts, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand("INSERT INTO collect.rated_test (ts, val) VALUES ($1, $2)", connection);
        insert.Parameters.AddWithValue(ts);
        insert.Parameters.AddWithValue(new string('x', 4096));
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One uncompressed filler hypertable at the 6h rung with <paramref name="chunkCount"/> chunks,
    /// seeded with one <c>generate_series</c> insert — never compressed, so it never enters
    /// <see cref="RawChunkIntervalReconciler.TableInputsSql"/>'s JOIN and cannot move or affect any other
    /// table's decision; it exists only to push the store's total chunk count (and hypertable count) up to the
    /// production shape.</summary>
    private static async Task CreateFillerHypertableAsync(NpgsqlConnection connection, string tableName, int chunkCount, CancellationToken ct)
    {
        await ExecAsync(connection, $"CREATE TABLE collect.{tableName} (ts timestamp NOT NULL, val text NOT NULL)", ct);
        await ExecAsync(connection,
            $"SELECT create_hypertable('collect.{tableName}', by_range('ts', INTERVAL '6 hours'), if_not_exists => true, migrate_data => true)", ct);
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO collect.{tableName} (ts, val) SELECT date_trunc('hour', now()) - (n * INTERVAL '6 hours'), 'x' FROM generate_series(1, $1) n", connection);
        insert.Parameters.AddWithValue(chunkCount);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Only this fact's own tables (the <c>filler_*</c> series and <c>rated_test</c>) —
    /// <c>PgMigrations.MigrateAsync</c> already creates its own real hypertables (e.g.
    /// <c>collect.collection_log</c>) in every scratch database, so an unfiltered count of
    /// <c>timescaledb_information.hypertables</c> would overcount by however many migrations create.</summary>
    private static async Task<(long Tables, long Chunks, long MaxChunks)> HypertableCensusAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT COUNT(*), COALESCE(SUM(num_chunks), 0), COALESCE(MAX(num_chunks), 0)
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'collect'
  AND (hypertable_name LIKE 'filler\_%' OR hypertable_name = 'rated_test')", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static async Task<int> CurrentIntervalHoursAsync(NpgsqlConnection connection, string hypertableName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT (EXTRACT(EPOCH FROM time_interval) / 3600.0)::integer
FROM timescaledb_information.dimensions
WHERE hypertable_schema = 'collect' AND hypertable_name = $1 AND dimension_type = 'Time'", connection);
        command.Parameters.AddWithValue(hypertableName);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The off switch (#4211): <see cref="DarlingConfig.RawChunkIntervalReconcileEnabled"/> defaults on,
    /// and the daily-purge tick's source gates the whole reconcile — budget resolution, the connection, and
    /// <see cref="RawChunkIntervalReconciler.ReconcileAsync"/> itself — behind it and <c>_timescaleAvailable</c>,
    /// so turning it off applies and records nothing. <see cref="DarlingWorker"/> cannot be constructed without a
    /// live fleet of dependencies, so this reads the source the same way the rung tests pin a viewer arm's
    /// position: not a live assertion, but a direct check that the call site named here still exists and still
    /// reads the switch before the call it guards.</summary>
    [Fact]
    public void OffSwitch_GatesTheWholeReconcileBeforeItRuns()
    {
        Assert.True(new DarlingConfig().RawChunkIntervalReconcileEnabled);

        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var guard = worker.IndexOf("if (_timescaleAvailable && config.RawChunkIntervalReconcileEnabled)", StringComparison.Ordinal);
        Assert.True(guard >= 0, "the daily-purge tick no longer gates the reconcile on the off switch");

        var call = worker.IndexOf("RawChunkIntervalReconciler.ReconcileAsync(", StringComparison.Ordinal);
        Assert.True(call >= 0, "the daily-purge tick no longer calls the reconciler");
        Assert.True(guard < call, "the off-switch check must precede the call it guards");

        var nextMethod = worker.IndexOf("private static async Task<double?> ResolveRawChunkIntervalBudgetBytesAsync", StringComparison.Ordinal);
        Assert.True(nextMethod < 0, "ResolveRawChunkIntervalBudgetBytesAsync must be internal (tested directly), not private");
    }
}
