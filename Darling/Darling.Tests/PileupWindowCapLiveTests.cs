/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8157, the tripwire: the pileup window read keeps a row cap against a runaway session count, and when the cap
/// fills before the read reaches the baseline floor the finding would state, the sweep evaluates nothing and says
/// so in the log, instead of handing the detector a baseline shorter than the one its finding describes. The floor is
/// strict: a cap that fills with its oldest row exactly on the floor instant has cut a snapshot the detector's baseline
/// keeps, so that read is skipped too. A cap that fills past the floor changes nothing: the rows are the baseline, and the
/// detector gets at most the cap of them. A window of exactly cap rows is whole, not cut, and stands however short it is.
///
/// <para>The cap is lowered through the reader's seam so the seed stays small: 56 one-minute snapshots of 120 user
/// sessions on one statement, 6,720 rows, anchored through <see cref="LiveClock"/>. The read binds one row more than the
/// cap, so a cap of 3,000 reads 3,001 rows, 25 snapshots and one row of the next, short of the 45-minute floor; 5,519
/// reads 5,520, 46 snapshots whose oldest sits exactly on the floor; 6,000 reads 6,001, 50 snapshots and a row past them,
/// and hands 6,000 on. A 24-minute window of 3,000 rows at a cap of 3,000 is exactly cap rows, and stands.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PileupWindowCapLiveTests
{
    private const string ServerName = "darling-pileup-window-cap-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const int Snapshots = 56;
    private const int SessionsPerSnapshot = 120;

    [Fact]
    public async Task ACapThatFillsShortOfTheBaselineFloor_SkipsTheSweepWithALogLine_AndOnePastTheFloorDoesNot()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pileup window-cap test.");

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

            var (_, floor) = await SeedAsync(connection, ct);

            /* The seam: a constructor that takes the row cap. Reached by reflection so this test builds against a reader
               without one and fails here, saying what is missing. */
            var seam = typeof(PgPileupSnapshotReader).GetConstructors(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(c => c.GetParameters().Length == 3 && c.GetParameters()[2].ParameterType == typeof(int));
            Assert.True(seam is not null,
                "PgPileupSnapshotReader has no row-cap seam (a third constructor parameter, int): the window read's cap cannot be lowered, so the trip case cannot be exercised (#8157)");

            /* A cap of 3,000 reads 3,001 rows, 25 snapshots and one row of the 26th: the cut lands 20 minutes short of the
               45-minute floor. */
            var shortLogger = new CapturingTestLogger();
            var shortReader = (PgPileupSnapshotReader)seam!.Invoke(new object?[] { postgres, shortLogger, 3_000 });
            var shortRows = await shortReader.ReadWindowAsync(ServerId, floor, ct);
            Assert.Empty(shortRows);
            Assert.Equal(1, shortLogger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("3000", shortLogger.Joined, StringComparison.Ordinal);
            Assert.Contains("skipped", shortLogger.Joined, StringComparison.Ordinal);

            /* A cap of 6,000 reads 6,001 rows, 50 snapshots and one row past them, with the floor four minutes behind: the rows
               stand, and the detector gets the cap of them. */
            var pastLogger = new CapturingTestLogger();
            var pastReader = (PgPileupSnapshotReader)seam.Invoke(new object?[] { postgres, pastLogger, 6_000 });
            var pastRows = await pastReader.ReadWindowAsync(ServerId, floor, ct);
            Assert.Equal(6_000, pastRows.Count);
            var newest = pastRows.Max(r => r.CollectionTime);
            Assert.True(pastRows.Min(r => r.CollectionTime) < newest.AddMinutes(-SameStatementPileupDetector.BaselineLookbackMinutes));
            Assert.Equal(0, pastLogger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ACapThatFillsWithTheOldestRowExactlyAtTheFloor_SkipsTheSweep_AndExactlyCapRowsShortOfTheFloorStand()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pileup window-cap boundary test.");

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
            var (latest, floor) = await SeedAsync(connection, ct);

            /* A cap of 5,519 reads 5,520 rows, 46 snapshots: the newest and the 45 behind it, so the read's oldest row sits on
               the floor instant itself. The detector's baseline keeps that instant, so the read has to reach past it to stand. */
            var logger = new CapturingTestLogger();
            var cap = (SameStatementPileupDetector.BaselineLookbackMinutes + 1) * SessionsPerSnapshot - 1;
            var rows = await new PgPileupSnapshotReader(postgres, logger, cap).ReadWindowAsync(ServerId, floor, ct);

            Assert.Empty(rows);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("skipped", logger.Joined, StringComparison.Ordinal);

            /* Exactly cap rows is a whole window, not a cut, however short: a 24-minute window (a server added minutes ago)
               holds 25 snapshots, 3,000 rows, and at a cap of 3,000 every one of them stands with nothing logged. */
            var wholeLogger = new CapturingTestLogger();
            var wholeRows = await new PgPileupSnapshotReader(postgres, wholeLogger, 3_000).ReadWindowAsync(ServerId, latest.AddMinutes(-24), ct);

            Assert.Equal(3_000, wholeRows.Count);
            Assert.Equal(0, wholeLogger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Seeds the 56 snapshots behind <see cref="LiveClock.AnchoredEnd"/> and returns the newest instant with the
    /// floor the sweep passes: the baseline lookback plus the staleness margin behind the newest snapshot.</summary>
    private static async Task<(DateTime Latest, DateTime Floor)> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var latest = LiveClock.AnchoredEnd();
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
SELECT $1 + m, $2 - (m * interval '1 minute'), $3, $4, 100 + s, 'AppDb', '00 00:00:00.100', 'SELECT payload FROM orders WHERE id = 1', 'running', 0, NULL, 0, 10, 100, 0, 0, 20, 0.0, 'Read Committed', 1, 0, 'app', 'APP01', 'AppServer', 1, 0
FROM generate_series(0, $5) AS m CROSS JOIN generate_series(0, $6) AS s",
            CollectionIdGenerator.Next(), latest, ServerId, ServerName, Snapshots - 1, SessionsPerSnapshot - 1);

        return (latest, latest.AddMinutes(-(SameStatementPileupDetector.BaselineLookbackMinutes + SameStatementPileupDetector.StaleSnapshotCutoffMinutes)));
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
