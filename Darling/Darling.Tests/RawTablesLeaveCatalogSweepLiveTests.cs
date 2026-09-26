/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4427: the daily catalog sweep (<see cref="DarlingRetention.PurgeAsync"/>) gates every collector table's
/// drop on <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> alone — a FLOOR-only comparison (each
/// rollup's oldest bucket against raw's oldest row). A hole INSIDE the covered span (an outage seam the
/// repair hasn't reached, a range the repair deferred, a failed repair) moves neither floor, so the sweep
/// reads Covered and drops the chunk the service-triggered, fully-gated purge (#4299/#4391,
/// <see cref="DarlingWorker.TriggerRawPurgeCoreAsync"/>) is holding for exactly that reason.
///
/// <para>These pins prove the three raw relations (<see cref="TimescaleSupport.RawRelations"/>) now leave
/// the sweep's drop path entirely on a TimescaleDB store, that plain-PostgreSQL's DELETE fallback is
/// unchanged, that <c>purge_now</c> runs the gated trigger and reports what it held, and that removing the
/// membership check (the mutation) makes the hole-survives pin fail.</para>
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawTablesLeaveCatalogSweepLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenTimescaleAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4427 pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);

        /* Max Auto Prepare=0: see RawPurgeTriggerLiveTests — RunRetentionPurgeJobSql is multi-statement and
           this rig's connection reuse can trip Npgsql's auto-prepare cache on it. */
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4427 pins need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await stop.ExecuteNonQueryAsync();
        }

        return (connection, scratch);
    }

    private static async Task ArmRawJobAsync(NpgsqlConnection connection, string relation)
    {
        await using var arm = new NpgsqlCommand(
            $"SELECT add_retention_policy('collect.{relation}', drop_after => interval '4 days') AS job_id", connection) { CommandTimeout = SetupTimeoutSeconds };
        await arm.ExecuteScalarAsync();
    }

    /// <summary>Ten days back through one day back, one row per hour — old enough to clear a 1-day custom
    /// retention, matching <see cref="RawPurgeTriggerLiveTests"/>'s seed shape.</summary>
    private static async Task SeedRawAsync(NpgsqlConnection connection)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    -1,
    now() - (n || ' hours')::interval,
    1,
    'probe-4427-sweep',
    'ProbeDb',
    decode(md5('sweep-4427-' || n), 'hex'),
    decode(md5('sweep-4427-h-' || n), 'hex'),
    1, 1, 1, 30
FROM generate_series(24, 240) AS n", connection) { CommandTimeout = SetupTimeoutSeconds };
        await seed.ExecuteNonQueryAsync();
    }

    private static async Task RefreshSuccessorsAsync(NpgsqlConnection connection, DateTime from, DateTime to)
    {
        async Task RefreshAsync(string view)
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = SetupTimeoutSeconds };
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync();
        }

        await RefreshAsync(TimescaleSupport.QueryStatsIntervalHourlyView);
        await RefreshAsync(TimescaleSupport.QueryStatsDbIntervalHourlyView);
        await RefreshAsync(TimescaleSupport.QueryStatsBaselineView);
    }

    /// <summary>Refreshes the FULL 10-day seeded span EXCEPT the interior window
    /// <c>[holeStart, holeEnd)</c> — a real hole inside the covered span, the exact shape the floor-only
    /// check cannot see (the oldest bucket on either side of the hole still moves, so the floor comparison
    /// still reads Covered).</summary>
    private static async Task RefreshSuccessorsWithInteriorHoleAsync(
        NpgsqlConnection connection, DateTime seedFrom, DateTime seedTo, DateTime holeStart, DateTime holeEnd)
    {
        await RefreshSuccessorsAsync(connection, seedFrom, holeStart);
        await RefreshSuccessorsAsync(connection, holeEnd, seedTo);
    }

    private static async Task<long> ChunkCountAsync(NpgsqlConnection connection)
    {
        await using var read = new NpgsqlCommand(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_stats'",
            connection) { CommandTimeout = SetupTimeoutSeconds };
        return (long)(await read.ExecuteScalarAsync())!;
    }

    private static async Task<long> HoleRowCountAsync(NpgsqlConnection connection, DateTime holeStart, DateTime holeEnd)
    {
        await using var read = new NpgsqlCommand(
            "SELECT count(*) FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", connection)
        { CommandTimeout = SetupTimeoutSeconds };
        read.Parameters.AddWithValue(DateTime.SpecifyKind(holeStart, DateTimeKind.Unspecified));
        read.Parameters.AddWithValue(DateTime.SpecifyKind(holeEnd, DateTimeKind.Unspecified));
        return (long)(await read.ExecuteScalarAsync())!;
    }

    /// <summary>
    /// Pin 1 (RED on dev at runtime): a hole inside the covered span reads Covered under the floor-only
    /// check, but the sweep must no longer act on that verdict for a raw table — the fix takes the three raw
    /// relations out of the drop path entirely, so the hole's rows and its chunk survive a 1-day-retention
    /// sweep regardless of what the floor check says.
    /// </summary>
    [Fact]
    public async Task InteriorHole_SurvivesSweep_RawTableLeavesDropPath()
    {
        var (connection, scratch) = await OpenTimescaleAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            var seedFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var seedTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            /* The interior hole: a day-wide window well inside the covered span, nowhere near either edge. */
            var holeStart = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-6), TimeSpan.FromHours(1));
            var holeEnd = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-5), TimeSpan.FromHours(1));

            await RefreshSuccessorsWithInteriorHoleAsync(connection, seedFrom, seedTo, holeStart, holeEnd);

            var holeRowsBefore = await HoleRowCountAsync(connection, holeStart, holeEnd);
            Assert.True(holeRowsBefore > 0, "the hole window must hold seeded rows, or this pin proves nothing");

            /* The floor-only check reads Covered: both edges of the 10-day span were refreshed, so the
               rollup's oldest bucket still reaches back to raw's oldest row. This is the exact blindness
               #4427 is about — the interior hole moves neither floor. */
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var floorSafe = await CallIsTieredDropSafeAsync(postgres, connection);
            Assert.True(floorSafe, "the floor-only check must read Covered despite the interior hole, or this pin proves nothing");

            var chunksBefore = await ChunkCountAsync(connection);
            Assert.True(chunksBefore > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            var logger = new CapturingTestLogger();
            var summary = await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: true, logger, default, retentionDaysFor: _ => 1);
            _ = summary;

            var chunksAfter = await ChunkCountAsync(connection);
            var holeRowsAfter = await HoleRowCountAsync(connection, holeStart, holeEnd);

            /* THE PIN: the hole's raw rows (and, by extension, its chunk) survive the sweep. RED on dev —
               dev's PurgeAsync drops the chunk because IsTieredDropSafeAsync/IsRawTierDropSafeAsync is the
               ONLY gate it runs for query_stats, and that gate cannot see this hole. */
            Assert.Equal(holeRowsBefore, holeRowsAfter);
            Assert.Equal(chunksBefore, chunksAfter);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Pin 2: plain-PostgreSQL mode keeps the DELETE fallback for the three raw tables unchanged — there
    /// are no rollups there, so the fallback is raw's only purge and must still fire.
    /// </summary>
    [Fact]
    public async Task PlainPostgres_StillDeletesExpiredRawRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plain-PostgreSQL pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* This rig's template1 (and so every scratch database created from it) may already carry the
           timescaledb extension row. The probe this pin exists to test decides the skip on pg_extension
           alone, so \"plain PostgreSQL\" here must mean the extension really is absent, or the probe would
           see it and skip the raw relation — the opposite of what this pin means to prove. */
        await using (var dropExtension = new NpgsqlCommand("DROP EXTENSION IF EXISTS timescaledb", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await dropExtension.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    -1,
    now() - (n || ' days')::interval,
    1,
    'probe-4427-plain',
    'ProbeDb',
    decode(md5('plain-4427-' || n), 'hex'),
    decode(md5('plain-4427-h-' || n), 'hex'),
    1, 1, 1, 30
FROM generate_series(2, 20) AS n", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            await using var countBefore = new NpgsqlCommand(
                "SELECT count(*) FROM collect.query_stats WHERE server_name = 'probe-4427-plain'", connection)
            { CommandTimeout = SetupTimeoutSeconds };
            var before = (long)(await countBefore.ExecuteScalarAsync(ct))!;
            Assert.True(before > 0, "the seed must have produced rows, or this pin proves nothing");

            await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, null, ct, retentionDaysFor: _ => 1);

            await using var countAfter = new NpgsqlCommand(
                "SELECT count(*) FROM collect.query_stats WHERE server_name = 'probe-4427-plain'", connection)
            { CommandTimeout = SetupTimeoutSeconds };
            var after = (long)(await countAfter.ExecuteScalarAsync(ct))!;

            Assert.True(after < before, $"plain-PostgreSQL mode must still DELETE expired raw rows (before={before}, after={after})");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var cleanupCommand = new NpgsqlCommand(
                    "DELETE FROM collect.query_stats WHERE server_name = 'probe-4427-plain'", cleanup);
                await cleanupCommand.ExecuteNonQueryAsync(cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Pin 3: <c>purge_now</c>'s raw-table step (<see cref="DarlingWorker.BuildRawTablePurgeNowReportAsync"/>)
    /// reports <c>hole</c> for a relation the trigger just held for an interior hole, and includes the
    /// custom-retention note when the requested horizon is shorter than the gated one.
    /// </summary>
    [Fact]
    public async Task PurgeNow_RawTableReport_ShowsHoleAndCustomRetentionNote()
    {
        var (connection, scratch) = await OpenTimescaleAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            var seedFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var seedTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            var holeStart = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-6), TimeSpan.FromHours(1));
            var holeEnd = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-5), TimeSpan.FromHours(1));

            await RefreshSuccessorsWithInteriorHoleAsync(connection, seedFrom, seedTo, holeStart, holeEnd);

            var periodic = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Periodic, default);
            Assert.True(periodic.Armed >= 1, "the seeded relation must read Covered under the floor-only check");

            await using (var stampCmd = new NpgsqlCommand(TimescaleSupport.PostmasterStartEpochMicrosecondsSql, connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                var epoch = (long)(await stampCmd.ExecuteScalarAsync())!;
                await using var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(Raw), connection) { CommandTimeout = SetupTimeoutSeconds };
                stamp.Parameters.AddWithValue(epoch);
                await stamp.ExecuteNonQueryAsync();
            }

            var passStartUtc = DateTime.UtcNow;
            await DarlingWorker.TriggerRawPurgeCoreAsync(connection, NullLogger.Instance, default);

            var report = await DarlingWorker.BuildRawTablePurgeNowReportAsync(connection, customRetentionDays: 1, passStartUtc, null, default);

            var rawEntry = Assert.Single(report, entry => (string)entry.GetType().GetProperty("relation")!.GetValue(entry)! == Raw);
            var outcome = (string)rawEntry.GetType().GetProperty("outcome")!.GetValue(rawEntry)!;
            var note = (string)rawEntry.GetType().GetProperty("note")!.GetValue(rawEntry)!;

            /* THE PIN: the trigger held on the interior hole, and the report says so plainly, plus the
               custom-retention (1 day, shorter than the 4-day gated horizon) note. */
            Assert.Equal("hole", outcome);
            Assert.Contains("hole", note, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("custom retention was not applied", note, StringComparison.OrdinalIgnoreCase);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>Drives the same predicate <see cref="DarlingRetention.PurgeAsync"/> uses internally, through
    /// the one live probe it actually runs (<see cref="TimescaleSupport.IsRawTierDropSafeAsync"/>) — the
    /// private <c>IsTieredDropSafeAsync</c> wrapper adds only the non-gated-table short-circuit, irrelevant
    /// here since <c>query_stats</c> is always gated.</summary>
    private static async Task<bool> CallIsTieredDropSafeAsync(NpgsqlDataSource postgres, NpgsqlConnection sharedConnection)
    {
        _ = postgres;
        return await TimescaleSupport.IsRawTierDropSafeAsync(sharedConnection, Raw, default);
    }

    /// <summary>
    /// Pin 4: a stale <c>timescaleAvailable</c> latch cannot reopen the drop path for the three raw
    /// relations. On a store WITH the extension present, the same interior-hole seed as
    /// <see cref="InteriorHole_SurvivesSweep_RawTableLeavesDropPath"/> must still hold the hole's rows even
    /// when the caller passes <c>timescaleAvailable: false</c> — the skip is decided on the live
    /// <c>pg_extension</c> probe (<see cref="DarlingRetention.ProbeRawSkipSafeAsync"/>), never on the
    /// worker's latch argument.
    /// </summary>
    [Fact]
    public async Task StaleTimescaleAvailableLatch_DoesNotReopenDropPath()
    {
        var (connection, scratch) = await OpenTimescaleAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);
            await SeedRawAsync(connection);

            var seedFrom = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-11), TimeSpan.FromHours(1));
            var seedTo = TimescaleSupport.AlignDown(DateTime.UtcNow, TimeSpan.FromHours(1));
            var holeStart = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-6), TimeSpan.FromHours(1));
            var holeEnd = TimescaleSupport.AlignDown(DateTime.UtcNow.AddDays(-5), TimeSpan.FromHours(1));

            await RefreshSuccessorsWithInteriorHoleAsync(connection, seedFrom, seedTo, holeStart, holeEnd);

            var holeRowsBefore = await HoleRowCountAsync(connection, holeStart, holeEnd);
            Assert.True(holeRowsBefore > 0, "the hole window must hold seeded rows, or this pin proves nothing");

            var chunksBefore = await ChunkCountAsync(connection);
            Assert.True(chunksBefore > 0, "the seed must have produced at least one raw chunk, or this pin proves nothing");

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* THE PIN: timescaleAvailable is false here, matching a stale worker latch, but the extension is
               present in this store — the probe must still skip the raw relations, so the hole's rows and
               chunk survive exactly as they do when timescaleAvailable is true. */
            var summary = await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, new CapturingTestLogger(), default, retentionDaysFor: _ => 1);
            _ = summary;

            var chunksAfter = await ChunkCountAsync(connection);
            var holeRowsAfter = await HoleRowCountAsync(connection, holeStart, holeEnd);

            Assert.Equal(holeRowsBefore, holeRowsAfter);
            Assert.Equal(chunksBefore, chunksAfter);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Pin 5: when the live probe itself cannot answer (a connection failure, not an absent extension), the
    /// probe fails toward keeping rows — it returns <c>true</c> — and logs exactly one Warning rather than
    /// throwing or logging once per raw table.
    /// </summary>
    [Fact]
    public async Task ProbeRawSkipSafe_ConnectionFailure_KeepsRowsAndLogsOnce()
    {
        /* Port 1 is never listening — a connection attempt there fails fast without touching any real
           server, live or otherwise. */
        await using var unreachable = NpgsqlDataSource.Create("Host=localhost;Port=1;Timeout=2");
        var logger = new CapturingTestLogger();

        var skipSafe = await DarlingRetention.ProbeRawSkipSafeAsync(unreachable, logger, default);

        Assert.True(skipSafe, "a probe that cannot answer must fail toward keeping rows (true), not toward deleting on a guess");
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>
    /// Pin 6: <c>purge_now</c>'s raw-table report never claims a PREVIOUS pass's stored outcome as this
    /// pass's own. A <c>ran</c> record written before <paramref name="passStartUtc"/> parameter (below) must
    /// report <c>gate_unknown</c> with a note saying no outcome was recorded for this pass, driven through
    /// the same server-side writer (<see cref="TimescaleSupport.RecordRawLastPurgeOutcomeAsync"/>) the
    /// trigger itself uses, so the store's shape is real.
    /// </summary>
    [Fact]
    public async Task PurgeNow_RawTableReport_StaleOutcome_ReportsGateUnknown()
    {
        var (connection, scratch) = await OpenTimescaleAsync();
        var bodySucceeded = false;
        try
        {
            await ArmRawJobAsync(connection, Raw);

            /* Write a 'ran' outcome for the raw relation right now, on the same connection the trigger
               would use — the real record shape, not a hand-built JSON string. */
            await TimescaleSupport.RecordRawLastPurgeOutcomeAsync(connection, Raw, "ran", null, 5, null, default);

            /* passStartUtc reads AFTER the record above was written, so the record is unambiguously from a
               pass that predates this one — the exact shape BuildRawTablePurgeNowReportAsync must catch. */
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            var passStartUtc = DateTime.UtcNow;

            var report = await DarlingWorker.BuildRawTablePurgeNowReportAsync(connection, customRetentionDays: null, passStartUtc, null, default);

            var rawEntry = Assert.Single(report, entry => (string)entry.GetType().GetProperty("relation")!.GetValue(entry)! == Raw);
            var outcome = (string)rawEntry.GetType().GetProperty("outcome")!.GetValue(rawEntry)!;
            var note = (string)rawEntry.GetType().GetProperty("note")!.GetValue(rawEntry)!;

            /* THE PIN: a real, freshly-written 'ran' record exists, but it predates passStartUtc, so it must
               not be reported as this pass's outcome. */
            Assert.Equal("gate_unknown", outcome);
            Assert.Contains("no outcome was recorded for this pass", note, StringComparison.OrdinalIgnoreCase);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });
            await connection.DisposeAsync();
        }
    }
}
