/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of #3399: an EMPTY sweep tick's run-record actually lands in <c>collection_log</c> and comes
/// back out of the read the acceptance criterion names.
///
/// <para><b>This is the only test that can catch the failure mode the shared INSERT has produced three
/// times.</b> <c>InsertCollectionLogSql</c> now has three writers, and a column list widened without every
/// binding block widened raises <c>08P01</c> at RUNTIME — inside a writer that is failure-isolated by design,
/// so the exception goes to a Debug log and the row silently never appears. No source-text or pure pin can
/// see that; the one next door counts bindings against placeholders, which is the guard, and this is the
/// proof the guard is describing the right thing.</para>
///
/// <para><b>The empty tick is what is asserted</b>, and deliberately so: a busy tick's row would be written
/// by the same statement and prove the same bind, but it would not demonstrate the thing #3399 is about — a
/// tick with nothing to fetch leaving a trace. The tally is left at its defaults, which is exactly what the
/// startup tick against a freshly-created backlog table produces.</para>
///
/// <para>Split out of <see cref="OversizedPlanSweepRunRecordTests"/> rather than serialized beside it, the
/// #1776 hygiene rule: that class is pure and has no business waiting on the shared store.</para>
///
/// <para>Writes under the FLEET SENTINEL, which is a shared row nothing else keys on, so the cleanup deletes
/// by collector name and a time floor taken before the write rather than by a fabricated test id — there is
/// no test-owned <c>server_id</c> to delete by here, and deleting every sentinel row would take the
/// retention job's audit trail with it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class OversizedPlanSweepRunRecordLivePostgresTests
{
    [Fact]
    public async Task AnEmptyTicksRunRecordLandsUnderTheSentinel_AndReadsBackThroughTheCollectionLog()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep run-record test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        /* Idempotent — an older store comes up to current, a current one no-ops. */
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        /* The floor is taken BEFORE the write and used by both the read and the cleanup, so neither can
           reach a row this test did not create. Naive UTC, matching what the writer stores. */
        var floor = DateTime.SpecifyKind(DateTime.UtcNow.AddSeconds(-1), DateTimeKind.Unspecified);

        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(
            new OversizedPlanBacklogSweep.SweepTally { ServersSwept = 2 });

        var wrote = false;
        try
        {
            await DarlingObservability.LogOversizedPlanSweepRunAsync(
                postgres, status, 0, 250, 100, message, null, TestContext.Current.CancellationToken);

            /* Read back through the SAME reader get_collection_log runs, scoped to the sentinel the
               sentinel-aware resolve hands it — so this exercises the client path rather than a
               hand-written SELECT that could agree with the write and with nothing else. */
            var rows = await DarlingDataReader.GetCollectionLogAsync(
                postgres,
                DarlingObservability.FleetServerId,
                floor,
                DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(1), DateTimeKind.Unspecified),
                50,
                DarlingObservability.OversizedPlanSweepCollectorName);

            var row = Assert.Single(rows);
            wrote = true;

            /* rows_collected = 0 is the whole point: the row exists, and it says the tick captured
               nothing. Before this the two were the same observation — none. */
            Assert.Equal(0, row.RowsCollected);
            Assert.Equal("SUCCESS", row.Status);
            Assert.Equal(message, row.ErrorMessage);
            Assert.Equal(250, row.DurationMs);
            Assert.Equal(100, row.SqlDurationMs);
            Assert.Equal(150, row.StoreDurationMs);

            /* And the V108/V109/V110 column families stay unclaimed, so nothing reads a maintenance pass
               as a collector run that measured an instant open or a fetch that cost nothing. */
            Assert.Null(row.SqlOpenMs);
            Assert.Null(row.SqlDrainMs);
            Assert.Null(row.DrainRowsRead);
            Assert.Null(row.PlanFetchIdsAttempted);
            Assert.Null(row.TextFetchIdsAttempted);
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(
                wrote,
                async () =>
                {
                    using var delete = new NpgsqlCommand(
                        "DELETE FROM collect.collection_log WHERE server_id = $1 AND collector_name = $2 AND collection_time >= $3",
                        connection);
                    delete.Parameters.AddWithValue(DarlingObservability.FleetServerId);
                    delete.Parameters.AddWithValue(DarlingObservability.OversizedPlanSweepCollectorName);
                    delete.Parameters.AddWithValue(floor);
                    await delete.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                });
        }
    }
}
