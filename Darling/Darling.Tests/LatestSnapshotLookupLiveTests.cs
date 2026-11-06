/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8013: the latest-snapshot reads of <c>session_stats</c>, <c>index_object_stats</c> and <c>running_jobs</c>, and the
/// query_store state prune's <c>database_states</c> snapshot, use the newest snapshot and nothing from an older one, for
/// the three server shapes a latest read has to get right:
/// a server collecting now (its newest stamp a few minutes ahead of this reader's clock, as a collector host's clock can
/// be), a server whose newest snapshot is twenty days old, and a server with no rows at all. The reads resolve the snapshot
/// before the row statement, so these pin that the resolved instant is the one the rows come from.
/// </summary>
[Collection("live-postgres")]
public sealed class LatestSnapshotLookupLiveTests
{
    private const string FreshName = "darling-8013-latest-fresh";
    private const string StaleName = "darling-8013-latest-stale20d";
    private const string EmptyName = "darling-8013-latest-empty";
    private const string Db = "SnapshotDb";

    private static readonly int FreshId = ServerIdHelper.GetDeterministicHashCode(FreshName);
    private static readonly int StaleId = ServerIdHelper.GetDeterministicHashCode(StaleName);
    private static readonly int EmptyId = ServerIdHelper.GetDeterministicHashCode(EmptyName);

    private static readonly (string Name, int Id)[] AllServers = [(FreshName, FreshId), (StaleName, StaleId), (EmptyName, EmptyId)];

    /// <summary>The stamps each seeded server gets, oldest first; the last is the one every read must return.</summary>
    private static DateTime[] FreshStamps(DateTime now) => [now.AddDays(-20), now.AddDays(-1), now.AddMinutes(5)];
    private static DateTime[] StaleStamps(DateTime now) => [now.AddDays(-25), now.AddDays(-20)];

    private static string Tag(DateTime stamp) => "snap-" + stamp.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    [Fact]
    public Task SessionStats_ReturnsOnlyTheNewestSnapshot_AndNothingForAServerWithNone() =>
        WithStoreAsync(async (connection, postgres, now, ct) =>
        {
            foreach (var (id, name, stamps) in Seeded(now))
                foreach (var stamp in stamps)
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_logical_reads)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                        CollectionIdGenerator.Next(), Naive(stamp), id, name, Tag(stamp), 12L, 3, 9, 0, 4200L, 99999L);

            foreach (var (id, _, stamps) in Seeded(now))
            {
                var newest = stamps[^1];
                var rows = await DarlingSessionStatsReader.GetLatestAsync(postgres, id, 30, ct);
                var row = Assert.Single(rows);
                Assert.Equal(Naive(newest), row.CollectionTime);
                Assert.Equal(Tag(newest), row.ProgramName);
            }

            Assert.Empty(await DarlingSessionStatsReader.GetLatestAsync(postgres, EmptyId, 30, ct));
        });

    [Fact]
    public Task IndexUsage_ItsMatchCount_ObjectLocking_AndItsDetail_ReturnOnlyTheNewestSnapshot() =>
        WithStoreAsync(async (connection, postgres, now, ct) =>
        {
            foreach (var (id, name, stamps) in Seeded(now))
                foreach (var stamp in stamps)
                    await SeedIndexAsync(connection, ct, id, name, stamp);

            foreach (var (id, _, stamps) in Seeded(now))
            {
                var newest = stamps[^1];

                var usage = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, id, 50, DatabaseFilter.All, ct);
                Assert.Equal(Tag(newest), Assert.Single(usage).IndexName);
                Assert.Equal(1L, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, id, DatabaseFilter.All, ct));

                var locking = await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, id, 50, ct);
                var lockingRow = Assert.Single(locking);
                Assert.Equal(Naive(newest), lockingRow.CollectionTime);
                Assert.Equal(Tag(newest), lockingRow.IndexName);

                var detail = await DarlingObjectStatsReader.GetIndexLockingDetailAsync(postgres, id, DatabaseFilter.One(Db), "dbo", "Orders", Tag(newest), ct);
                Assert.NotNull(detail);
                Assert.Equal(Naive(newest), detail!.CollectionTime);
                /* An index that exists only in an older snapshot is not in the newest capture, so the detail read has no row for it. */
                Assert.Null(await DarlingObjectStatsReader.GetIndexLockingDetailAsync(postgres, id, DatabaseFilter.One(Db), "dbo", "Orders", Tag(stamps[0]), ct));
            }

            Assert.Empty(await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, EmptyId, 50, DatabaseFilter.All, ct));
            Assert.Equal(0L, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, EmptyId, DatabaseFilter.All, ct));
            Assert.Empty(await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, EmptyId, 50, ct));
            Assert.Null(await DarlingObjectStatsReader.GetIndexLockingDetailAsync(postgres, EmptyId, DatabaseFilter.One(Db), "dbo", "Orders", "IX_none", ct));
        });

    [Fact]
    public Task StorageGrowthIndexDetail_ReturnsOnlyTheDatabasesNewestSnapshot() =>
        WithStoreAsync(async (connection, postgres, now, ct) =>
        {
            foreach (var (id, name, stamps) in Seeded(now))
                foreach (var stamp in stamps)
                    await SeedIndexAsync(connection, ct, id, name, stamp);

            foreach (var (id, _, stamps) in Seeded(now))
            {
                var newest = stamps[^1];
                var rows = await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(postgres, id, Db, "dbo", "Orders", 30, ct);
                var row = Assert.Single(rows);
                Assert.Equal(Tag(newest), row.IndexName);
                Assert.Equal(Naive(newest), DateTime.SpecifyKind(row.CollectionTime, DateTimeKind.Unspecified));
            }

            Assert.Empty(await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(postgres, EmptyId, Db, "dbo", "Orders", 30, ct));
        });

    [Fact]
    public Task AnomalousJobs_ReadsTheNewestSnapshotsJobs_AndIsStaleWithoutAFreshOne() =>
        WithStoreAsync(async (connection, postgres, now, ct) =>
        {
            /* The fresh server's newest stamp is a minute ago here: the freshness rule judges the stamp against this clock. */
            var freshStamps = new[] { now.AddDays(-20), now.AddDays(-1), now.AddMinutes(-1) };
            foreach (var (id, name, stamps) in new[] { (FreshId, FreshName, freshStamps), (StaleId, StaleName, StaleStamps(now)) })
                foreach (var stamp in stamps)
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, percent_of_average)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                        Naive(stamp), id, name, Tag(stamp), "job-" + Tag(stamp), Naive(stamp.AddHours(-1)), 3661L, 90L, 120L, 350.0m);

            var adapter = new DarlingAlertReadAdapter(postgres);

            var fresh = await adapter.GetAnomalousJobsAsync(FreshId.ToString(CultureInfo.InvariantCulture), 2, ct);
            Assert.True(fresh.SnapshotIsFresh);
            Assert.Equal(Tag(freshStamps[^1]), Assert.Single(fresh.Jobs).JobName);

            /* Twenty days old resolves, and the freshness rule then reports it stale rather than serving it as current. */
            Assert.False((await adapter.GetAnomalousJobsAsync(StaleId.ToString(CultureInfo.InvariantCulture), 2, ct)).SnapshotIsFresh);
            Assert.False((await adapter.GetAnomalousJobsAsync(EmptyId.ToString(CultureInfo.InvariantCulture), 2, ct)).SnapshotIsFresh);
        });

    [Fact]
    public Task QueryStoreStatePrune_JudgesDroppedAgainstTheNewestSnapshotOnly_AndDeletesNothingWithoutOne() =>
        WithStoreAsync(async (connection, postgres, now, ct) =>
        {
            const string Owner = QueryStoreOpenIntervalState.StateCollectorName;
            const string Prefix = QueryStoreOpenIntervalState.WatermarkKeyPrefix;

            /* Every older snapshot still names "Gone"; only the newest has dropped it. A prune reading any snapshot but
               the newest would keep Gone's key forever; one reading past the newest would also take Kept's. */
            foreach (var (id, name, stamps) in Seeded(now))
            {
                foreach (var stamp in stamps[..^1])
                    await SeedDatabaseStatesAsync(connection, ct, id, name, stamp, "Kept", "Gone");
                await SeedDatabaseStatesAsync(connection, ct, id, name, stamps[^1], "Kept");

                var writtenBeforeNewest = stamps[^1].AddMinutes(-10);
                await SeedStateAsync(connection, ct, id, Owner, Prefix + "Kept", writtenBeforeNewest);
                await SeedStateAsync(connection, ct, id, Owner, Prefix + "Gone", writtenBeforeNewest);
            }
            await SeedStateAsync(connection, ct, EmptyId, Owner, Prefix + "Unjudged", now.AddDays(-30));

            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
            foreach (var (name, id) in AllServers)
                Assert.True(await runner.PruneOrphanedQueryStoreDatabaseStateAsync(id, ct), name);

            foreach (var (id, _, _) in Seeded(now))
            {
                Assert.Equal(["Kept"], await StateKeysAsync(connection, ct, id, Owner, Prefix));
            }

            /* No database_states rows at all: nothing can be judged dropped, so the key stays. */
            Assert.Equal(["Unjudged"], await StateKeysAsync(connection, ct, EmptyId, Owner, Prefix));
        });

    private static async Task SeedDatabaseStatesAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime stamp, params string[] databases)
    {
        foreach (var database in databases)
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES (0, $1, $2, $3, $4, 5, 'ONLINE', false)",
                Naive(stamp), serverId, serverName, database);
    }

    private static Task SeedStateAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string owner, string key, DateTime updatedAt) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES ($1, $2, $3, 'v', $4)",
            serverId, owner, key, Naive(updatedAt));

    private static async Task<string[]> StateKeysAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string owner, string prefix)
    {
        using var cmd = new NpgsqlCommand(
            "SELECT substr(state_key, length($3) + 1) FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND starts_with(state_key, $3) ORDER BY 1", connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(owner);
        cmd.Parameters.AddWithValue(prefix);
        var keys = new System.Collections.Generic.List<string>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) keys.Add(reader.GetString(0));
        return [.. keys];
    }

    private static (int Id, string Name, DateTime[] Stamps)[] Seeded(DateTime now) =>
        [(FreshId, FreshName, FreshStamps(now)), (StaleId, StaleName, StaleStamps(now))];

    /// <summary>One index per snapshot, named for its stamp, with nonzero lock waits so the locking read's contention filter keeps it.</summary>
    private static Task SeedIndexAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime stamp) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
            CollectionIdGenerator.Next(), Naive(stamp), serverId, serverName, Db, "dbo", 100, "Orders", 2, Tag(stamp), "NONCLUSTERED",
            90m, 85m, 500_000L, 500L, 10L, 20L, 300L, 40L, 12000L, 5L, 800L, 3L, 60L, 30L);

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-snapshot tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var succeeded = false;
        try
        {
            await DeleteRowsAsync(connection, ct);
            foreach (var (name, id) in AllServers)
                await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);

            await body(connection, postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, succeeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = AllServers.Select(s => s.Id).ToArray();
        foreach (var table in new[] { "session_stats", "index_object_stats", "running_jobs", "database_states", "collector_state", "servers" })
        {
            using var cmd = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = ANY($1);", connection);
            cmd.Parameters.AddWithValue(ids);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
