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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8157: the pileup finding says its statement was compared over the prior
/// <see cref="SameStatementPileupDetector.BaselineLookbackMinutes"/> minutes, so the window the reader hands the
/// detector has to reach that far back on a busy server too. The read is bounded by its time floor, the lookback
/// plus the staleness margin the worker adds, with a row cap well above this seed as a tripwire
/// (<see cref="SameStatementPileupDetector.WindowRowCap"/>; <see cref="PileupWindowCapLiveTests"/> covers the trip): a
/// working cap newest-first would fill on the busiest servers before the floor is reached and quietly shorten the
/// baseline the finding describes.
///
/// <para>The seed: 56 one-minute snapshots of 120 user sessions on one statement, 6,720 rows, anchored through
/// <see cref="LiveClock"/>. The floor is the worker's: lookback plus staleness margin behind the newest snapshot.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PileupBaselineReachLiveTests
{
    private const string ServerName = "darling-pileup-baseline-reach-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const int Snapshots = 56;
    private const int SessionsPerSnapshot = 120;

    [Fact]
    public async Task TheWindowRead_ReachesTheBaselineFloorTheFindingStates_OnABusyServer()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pileup baseline-reach test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var latest = LiveClock.AnchoredEnd();
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
SELECT $1 + m, $2 - (m * interval '1 minute'), $3, $4, 100 + s, 'AppDb', '00 00:00:00.100', 'SELECT payload FROM orders WHERE id = 1', 'running', 0, NULL, 0, 10, 100, 0, 0, 20, 0.0, 'Read Committed', 1, 0, 'app', 'APP01', 'AppServer', 1, 0
FROM generate_series(0, $5) AS m CROSS JOIN generate_series(0, $6) AS s",
                CollectionIdGenerator.Next(), latest, ServerId, ServerName, Snapshots - 1, SessionsPerSnapshot - 1);

            /* The worker's floor: the baseline lookback plus the staleness margin behind the newest snapshot. */
            var floor = latest.AddMinutes(-(SameStatementPileupDetector.BaselineLookbackMinutes + SameStatementPileupDetector.StaleSnapshotCutoffMinutes));
            var rows = await new PgPileupSnapshotReader(postgres).ReadWindowAsync(ServerId, floor, ct);

            Assert.NotEmpty(rows);
            var newestRead = rows.Max(r => r.CollectionTime);
            var oldestRead = rows.Min(r => r.CollectionTime);
            var baselineFloor = newestRead.AddMinutes(-SameStatementPileupDetector.BaselineLookbackMinutes);
            Assert.True(oldestRead <= baselineFloor,
                $"the read reaches back only to {oldestRead:HH:mm} on {rows.Count} rows, but the finding states a baseline over the prior {SameStatementPileupDetector.BaselineLookbackMinutes} minutes, back to {baselineFloor:HH:mm}");

            /* Every seeded row sits inside the floor, and the detector's baseline for this statement is the 45 snapshots
               before the newest one. */
            Assert.Equal(Snapshots * SessionsPerSnapshot, rows.Count);
            Assert.Equal(SameStatementPileupDetector.BaselineLookbackMinutes * SessionsPerSnapshot,
                rows.Count(r => r.CollectionTime >= baselineFloor && r.CollectionTime < newestRead));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
