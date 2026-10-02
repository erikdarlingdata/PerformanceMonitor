/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB pins for the Lite twin of #4197's backfill candidate scan
/// (<see cref="RemoteCollectorService.GetBackfillCandidateDatabasesAsync"/>) and floor read
/// (<see cref="RemoteCollectorService.GetMinCollectedTimeForDatabaseAsync"/>). DuckDB has no
/// chunks or compression, so unlike Darling's live twin there is no plan check here — only the
/// candidate set and the two floor facts, which are exactly what the bounded SQL text promises.
/// </summary>
public sealed class QueryStoreBackfillCandidateTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -4197;
    private const string ServerName = "backfill-candidate-test";

    private readonly DuckDbInitializer _duckDb;

    public QueryStoreBackfillCandidateTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>Exposes the runner's now-internal backfill reads; only <c>_duckDb</c> is exercised
    /// (matching the plan: neither method touches <c>_serverManager</c>/<c>_scheduleManager</c>).</summary>
    private sealed class BackfillHarness(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task<List<string>> GetCandidatesAsync(int serverId, DateTime floorLimit, IReadOnlyDictionary<string, string> state) =>
            GetBackfillCandidateDatabasesAsync(serverId, floorLimit, state, CancellationToken.None);

        public Task<DateTime?> GetFloorAsync(string databaseName, DateTime floorLimit) =>
            GetMinCollectedTimeForDatabaseAsync(
                ServerId, QueryStoreCollector.Instance.TargetTable, "last_execution_time", "database_name", databaseName, floorLimit, CancellationToken.None);
    }

    /// <summary>Minimal query_store_stats row — the column list TestDataSeeder's plan-regression
    /// scenario uses as its model. Only collection_time and last_execution_time drive the two reads
    /// under test; the rest are benign fixed values.</summary>
    private static async Task SeedRowAsync(
        DuckDbInitializer duckDb, long collectionId, string databaseName, DateTime collectionTime, DateTime lastExecutionTime)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)";
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastExecutionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastExecutionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xTESTHASH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 2000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xTESTPLANHASH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = false });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task CandidateRead_KeepsQuietDatabaseWithAnOpenHole_DropsFinishedDatabase_FindsNewDatabase()
    {
        /* Fixed anchor, not DateTime.UtcNow, so the test is deterministic regardless of wall clock.
           Utc throughout (seeded rows and the anchor alike) -- DuckDB.NET has no analogue of Npgsql's
           hard throw on a Kind mismatch, but a stray Local/Unspecified value here would silently shift
           what gets persisted by the machine's UTC offset instead of failing loudly, so every DateTime
           in this fixture stays the same Kind on purpose. */
        var floorLimit = new DateTime(2026, 6, 15, 9, 0, 0, DateTimeKind.Utc);

        // quiet_hole_db: one row several days before floorLimit, in a different day; still has an
        // open hole spanning across floorLimit, so it must survive even though its row is too old
        // for the bounded SELECT to find on its own.
        await SeedRowAsync(_duckDb, -1, "quiet_hole_db", floorLimit.AddDays(-5), floorLimit.AddDays(-5));

        // finished_db: one row in the same old window, no hole key -- fully drained, must not
        // resurface just because it once shipped rows.
        await SeedRowAsync(_duckDb, -2, "finished_db", floorLimit.AddDays(-5), floorLimit.AddDays(-5));

        // new_db: one row only, inside the window (collection_time > floorLimit), with an old
        // last_execution_time (as a backfilled row would carry) so the floor test below is unambiguous.
        var backfilledLastExecution = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc);
        await SeedRowAsync(_duckDb, -3, "new_db", floorLimit.AddHours(1), backfilledLastExecution);

        var state = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [QueryStoreBackfillState.HoleKeyPrefix + "quiet_hole_db"] =
                QueryStoreBackfillState.EncodeHole(floorLimit.AddDays(-30), floorLimit.AddDays(30)),
        };

        var harness = new BackfillHarness(_duckDb);

        var candidates = await harness.GetCandidatesAsync(ServerId, floorLimit, state);

        /* One assertion covering all three databases: GetBackfillCandidateDatabasesAsync swallows
           read exceptions internally and still returns the hole-merged list, so asserting only the
           hole-derived member could pass even with a dead store read. new_db's presence is what
           proves the bounded SELECT itself executed; finished_db's absence proves the bound excludes
           a database with nothing recent and no recorded hole. */
        Assert.Equal(["new_db", "quiet_hole_db"], candidates);
    }

    [Fact]
    public async Task FloorRead_ReturnsFloorLimit_WhenARowIsAtOrBeforeIt_AndTheTrueMinimum_WhenThereIsNot()
    {
        var floorLimit = new DateTime(2026, 6, 15, 9, 0, 0, DateTimeKind.Utc);

        // quiet_hole_db has a row with collection_time <= floorLimit, so the EXISTS check hits and
        // the floor is clamped at floorLimit itself.
        await SeedRowAsync(_duckDb, -1, "quiet_hole_db", floorLimit.AddDays(-5), floorLimit.AddDays(-5));

        // new_db has no row that old -- its only row is after floorLimit -- so the EXISTS check
        // misses and the read falls through to the real bounded MIN(last_execution_time), which is
        // the backfilled row's old last_execution_time, not floorLimit and not collection_time.
        var backfilledLastExecution = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc);
        await SeedRowAsync(_duckDb, -3, "new_db", floorLimit.AddHours(1), backfilledLastExecution);

        var harness = new BackfillHarness(_duckDb);

        /* Assert.Equal against the actual expected values (not just NotNull) so a caught exception
           -- which returns null -- fails loudly instead of a weaker assertion quietly passing on a
           dead read. */
        Assert.Equal(floorLimit, await harness.GetFloorAsync("quiet_hole_db", floorLimit));
        Assert.Equal(backfilledLastExecution, await harness.GetFloorAsync("new_db", floorLimit));
    }
}
