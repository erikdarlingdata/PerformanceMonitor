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
/// so in the log, instead of handing the detector a baseline shorter than the one its finding describes. A cap that
/// fills with the floor already reached changes nothing: the rows are the baseline.
///
/// <para>The cap is lowered through the reader's seam so the seed stays small: 56 one-minute snapshots of 120 user
/// sessions on one statement, 6,720 rows, anchored through <see cref="LiveClock"/>. A cap of 3,000 rows is 25
/// snapshots, short of the 45-minute floor; a cap of 6,000 is 50 snapshots, past it.</para>
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

            var latest = LiveClock.AnchoredEnd();
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
SELECT $1 + m, $2 - (m * interval '1 minute'), $3, $4, 100 + s, 'AppDb', '00 00:00:00.100', 'SELECT payload FROM orders WHERE id = 1', 'running', 0, NULL, 0, 10, 100, 0, 0, 20, 0.0, 'Read Committed', 1, 0, 'app', 'APP01', 'AppServer', 1, 0
FROM generate_series(0, $5) AS m CROSS JOIN generate_series(0, $6) AS s",
                CollectionIdGenerator.Next(), latest, ServerId, ServerName, Snapshots - 1, SessionsPerSnapshot - 1);

            var floor = latest.AddMinutes(-(SameStatementPileupDetector.BaselineLookbackMinutes + SameStatementPileupDetector.StaleSnapshotCutoffMinutes));

            /* The seam: a constructor that takes the row cap. Reached by reflection so this test builds against a reader
               without one and fails here, saying what is missing. */
            var seam = typeof(PgPileupSnapshotReader).GetConstructors(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(c => c.GetParameters().Length == 3 && c.GetParameters()[2].ParameterType == typeof(int));
            Assert.True(seam is not null,
                "PgPileupSnapshotReader has no row-cap seam (a third constructor parameter, int): the window read's cap cannot be lowered, so the trip case cannot be exercised (#8157)");

            /* 3,000 rows is 25 snapshots: the cap fills 20 minutes short of the 45-minute floor. */
            var shortLogger = new CapturingTestLogger();
            var shortReader = (PgPileupSnapshotReader)seam!.Invoke(new object?[] { postgres, shortLogger, 3_000 });
            var shortRows = await shortReader.ReadWindowAsync(ServerId, floor, ct);
            Assert.Empty(shortRows);
            Assert.Equal(1, shortLogger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("3000", shortLogger.Joined, StringComparison.Ordinal);
            Assert.Contains("skipped", shortLogger.Joined, StringComparison.Ordinal);

            /* 6,000 rows is 50 snapshots: the cap fills with the floor already four minutes behind, so the rows stand. */
            var pastLogger = new CapturingTestLogger();
            var pastReader = (PgPileupSnapshotReader)seam.Invoke(new object?[] { postgres, pastLogger, 6_000 });
            var pastRows = await pastReader.ReadWindowAsync(ServerId, floor, ct);
            Assert.Equal(6_000, pastRows.Count);
            var newest = pastRows.Max(r => r.CollectionTime);
            Assert.True(pastRows.Min(r => r.CollectionTime) <= newest.AddMinutes(-SameStatementPileupDetector.BaselineLookbackMinutes));
            Assert.Equal(0, pastLogger.CountAtLevel(LogLevel.Warning));

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
