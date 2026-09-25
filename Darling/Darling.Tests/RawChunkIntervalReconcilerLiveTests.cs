/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
