/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
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

    /// <summary>
    /// <c>get_index_usage</c> reads the rows and then the match count, and both come from ONE capture: the tool resolves
    /// the newest snapshot once and binds it to both statements, so a capture that lands between the two reads changes
    /// neither. A count that resolved its own snapshot would count the new capture against the old capture's rows and
    /// report a truncation that never happened. The fixture plants a two-index capture a day old, inserts a five-index
    /// capture the moment the tool's row statement completes (an Npgsql activity listener on the store's database, the way
    /// <see cref="NpgsqlCommandCounter"/> counts), and reads the payload the tool built from the count that followed.
    /// </summary>
    [Fact]
    public Task IndexUsageTool_CountsTheCaptureItReturned_WhenANewerOneLandsBetweenTheReads() =>
        WithStoreAsync(async (connection, postgres, now, ct) =>
        {
            var older = now.AddDays(-1);
            var newer = now.AddMinutes(5);
            await SeedIndexAsync(connection, ct, FreshId, FreshName, older, 2, "IX_old_1");
            await SeedIndexAsync(connection, ct, FreshId, FreshName, older, 3, "IX_old_2");

            var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
            using var landing = new NpgsqlCommandCompletionHook(connection.Database, ["AS classification", "FROM v_index_object_stats"], () =>
            {
                using var writer = new NpgsqlConnection(cs);
                writer.Open();
                /* SET, never inherit, as LiveStoreCleanup does: Npgsql pools physical sessions, and one opened before the
                   store's first migration keeps the pre-ALTER search_path for its whole life, so on that session the bare
                   table name in the seed resolves to nothing (42P01) while on any other it is index_object_stats. */
                using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, writer))
                    setPath.ExecuteNonQuery();
                for (var i = 1; i <= 5; i++)
                    SeedIndex(writer, FreshId, FreshName, newer, 10 + i, "IX_new_" + i.ToString(CultureInfo.InvariantCulture));
            });

            var json = await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, FreshName, DatabaseFilter.All, 50, ct);
            Assert.True(landing.Error is null, $"the listener's seed threw inside the tool's read, so the payload is not evidence about the count: {landing.Error}");
            Assert.True(landing.Fired, "the newer capture was never planted: the listener did not see the row statement complete on this database, so the test proved nothing");

            using var payload = JsonDocument.Parse(json);
            var root = payload.RootElement;
            Assert.Equal(
                new[] { "IX_old_1", "IX_old_2" },
                root.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).Order().ToArray());
            Assert.Equal(2, root.GetProperty("returned_index_count").GetInt32());
            Assert.Equal(2L, root.GetProperty("matching_index_count").GetInt64());
            Assert.False(root.GetProperty("truncated").GetBoolean());

            /* The newer capture did land: a read after the tool returns is the five-index capture. */
            Assert.Equal(5L, await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, FreshId, DatabaseFilter.All, ct));
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

    /// <summary>
    /// The plan half of #8013, on a store with one chunk per retained day: every row statement, with the probed snapshot
    /// bound, is planned against the ONE chunk holding it, and each windowed probe against the few chunks inside its
    /// window, a number set by the window and not by retention. The correlated form the statements replaced
    /// (<c>collection_time = (SELECT MAX(collection_time) ...)</c>) is run as the control on the same store: the planner
    /// cannot see through the subquery at plan time, so it keeps every retained chunk in the plan. The control is what
    /// makes the one-chunk assertion a statement about the fix rather than about the store; if a planner ever excludes
    /// the chunks for the correlated form too, the control fails and says so.
    /// <para>Mints its own scratch database, as the other plan-shape tests do: another class's leftover chunks would change
    /// the chunk list the plan is read against. <c>EXPLAIN</c> without <c>ANALYZE</c>: the statement is not run, only
    /// planned, and the chunks are read off the plan's relation names.</para>
    /// </summary>
    [Fact]
    public async Task EveryRowStatement_PlansAgainstTheOneChunkHoldingTheSnapshot_NotEveryRetainedChunk()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-snapshot plan test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString, ct);
        Assert.SkipUnless(timescaleEnabled, "TimescaleDB is not available on this cluster; chunk exclusion needs it.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* No background policy job reshapes the chunks under the plan assertion. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, FreshId, FreshName, ct);

        /* One snapshot a day for RetainedDays days, the newest one now, so each of the four tables holds about one chunk per
           day (the product's chunk interval is one day). The newest snapshot is the one every read below must plan for. */
        const int RetainedDays = 30;
        /* LiveClock.Now(), not AnchoredEnd(): the answer holds at any hour. The snapshots are a day apart, so each lands in its
           own one-day chunk wherever the boundary falls, and every read below matches the newest stamp exactly (#5608). */
        var now = LiveClock.Now();
        for (var day = RetainedDays - 1; day >= 0; day--)
        {
            var stamp = now.AddDays(-day);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_logical_reads)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                CollectionIdGenerator.Next(), Naive(stamp), FreshId, FreshName, Tag(stamp), 12L, 3, 9, 0, 4200L, 99999L);
            await SeedIndexAsync(connection, ct, FreshId, FreshName, stamp);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, percent_of_average)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                Naive(stamp), FreshId, FreshName, Tag(stamp), "job-" + Tag(stamp), Naive(stamp.AddHours(-1)), 3661L, 90L, 120L, 350.0m);
            await SeedDatabaseStatesAsync(connection, ct, FreshId, FreshName, stamp, "Kept");
        }

        var chunks = await ChunksByHypertableAsync(connection, ct);
        foreach (var table in new[] { "session_stats", "index_object_stats", "running_jobs", "database_states" })
        {
            Assert.True(chunks.TryGetValue(table, out var set) && set.Count >= RetainedDays - 1,
                $"{table} has {(chunks.TryGetValue(table, out var s) ? s.Count : 0)} chunk(s); the seed did not produce one per day, so the plan would prove nothing");
        }

        var windowTwoDays = Naive(now.AddDays(-2));
        var windowThreeDays = Naive(now.AddDays(-3));

        /* The snapshots as the readers resolve them: every probe lands on the newest row. */
        var sessionSnapshot = await DarlingSessionStatsReader.GetLatestSnapshotTimeAsync(postgres, FreshId, 30, ct);
        var indexSnapshot = await DarlingObjectStatsReader.GetLatestIndexObjectStatsSnapshotTimeAsync(postgres, FreshId, ct);
        var objectSnapshot = await DarlingFinOpsStorageGrowthReader.GetLatestObjectIndexSnapshotAsync(postgres, FreshId, Db, 30, ct);
        var jobsSnapshot = await ScalarAsync(connection, ct, DarlingAlertReadAdapter.RunningJobsLatestSnapshotWindowedProbeSql, Int(FreshId), Stamp(windowTwoDays));
        var statesSnapshot = await ScalarAsync(connection, ct, DarlingCollectorRunner.NewestDatabaseStateWindowedProbeSql, Int(FreshId), Stamp(windowTwoDays));
        foreach (var snapshot in new object?[] { sessionSnapshot, indexSnapshot, objectSnapshot, jobsSnapshot, statesSnapshot })
        {
            Assert.Equal(Naive(now), Assert.IsType<DateTime>(snapshot));
        }

        var filterAll = DatabaseFilter.All;
        var filterOne = DatabaseFilter.One(Db);

        /* Each row statement with the parameters its reader binds (a fresh parameter per statement, since a parameter
           belongs to one command): one chunk, the one holding the snapshot. */
        var rowStatements = new (string Table, string Name, string Sql, Func<NpgsqlParameter[]> Parameters)[]
        {
            ("session_stats", "DarlingSessionStatsReader.LatestSql", DarlingSessionStatsReader.LatestSql,
                () => [Int(FreshId), Stamp(now)]),
            ("index_object_stats", "DarlingObjectStatsReader.IndexUsageSql", DarlingObjectStatsReader.IndexUsageSql,
                () => [Int(FreshId), filterAll.Parameter(), Int(50), Stamp(now)]),
            ("index_object_stats", "DarlingObjectStatsReader.IndexUsageMatchCountSql", DarlingObjectStatsReader.IndexUsageMatchCountSql,
                () => [Int(FreshId), filterAll.Parameter(), Stamp(now)]),
            ("index_object_stats", "DarlingObjectStatsReader.IndexLockingSql", DarlingObjectStatsReader.IndexLockingSql,
                () => [Int(FreshId), filterAll.Parameter(), Int(50), Stamp(now)]),
            ("index_object_stats", "DarlingObjectStatsReader.IndexLockingDetailSql", DarlingObjectStatsReader.IndexLockingDetailSql,
                () => [Int(FreshId), filterOne.Parameter(), Text("dbo"), Text("Orders"), Text(Tag(now)), Stamp(now)]),
            ("index_object_stats", "DarlingFinOpsStorageGrowthReader.ObjectIndexDetailSql", DarlingFinOpsStorageGrowthReader.ObjectIndexDetailSql,
                () => [Int(FreshId), Text(Db), Text("dbo"), Text("Orders"), Stamp(now)]),
            ("running_jobs", "DarlingAlertReadAdapter.AnomalousJobsSql", DarlingAlertReadAdapter.AnomalousJobsSql,
                () => [Int(FreshId), new NpgsqlParameter<decimal> { TypedValue = 200m }, Stamp(now)]),
            ("database_states", "DarlingCollectorRunner.PruneOrphanedDatabaseStateKeysSql", DarlingCollectorRunner.PruneOrphanedDatabaseStateKeysSql,
                () => [Int(FreshId), Text(QueryStoreOpenIntervalState.StateCollectorName), Text(QueryStoreOpenIntervalState.WatermarkKeyPrefix), Stamp(now)]),
        };

        foreach (var (table, name, sql, parameters) in rowStatements)
        {
            var planned = await PlannedChunksAsync(connection, ct, sql, chunks[table], parameters());
            Assert.True(planned.Count == 1,
                $"{name} is planned against {planned.Count} of {chunks[table].Count} {table} chunks; with the snapshot bound as a literal the planner should keep only the chunk holding it:\n{string.Join(", ", planned)}");
        }

        /* Each windowed probe: bounded below, so the chunks in the plan are the window's, a count that does not grow with
           retention. A window of N days over one-day chunks touches at most N + 1 chunks, plus one for the boundary. */
        var probes = new (string Table, string Name, string Sql, int WindowDays, Func<NpgsqlParameter[]> Parameters)[]
        {
            ("session_stats", "DarlingSessionStatsReader.LatestSnapshotWindowedProbeSql", DarlingSessionStatsReader.LatestSnapshotWindowedProbeSql, 2,
                () => [Int(FreshId), Stamp(windowTwoDays)]),
            ("index_object_stats", "DarlingObjectStatsReader.IndexObjectStatsLatestSnapshotWindowedProbeSql", DarlingObjectStatsReader.IndexObjectStatsLatestSnapshotWindowedProbeSql, 3,
                () => [Int(FreshId), Stamp(windowThreeDays)]),
            ("index_object_stats", "DarlingFinOpsStorageGrowthReader.ObjectIndexSnapshotWindowedProbeSql", DarlingFinOpsStorageGrowthReader.ObjectIndexSnapshotWindowedProbeSql, 3,
                () => [Int(FreshId), Text(Db), Stamp(windowThreeDays)]),
            ("running_jobs", "DarlingAlertReadAdapter.RunningJobsLatestSnapshotWindowedProbeSql", DarlingAlertReadAdapter.RunningJobsLatestSnapshotWindowedProbeSql, 2,
                () => [Int(FreshId), Stamp(windowTwoDays)]),
            ("database_states", "DarlingCollectorRunner.NewestDatabaseStateWindowedProbeSql", DarlingCollectorRunner.NewestDatabaseStateWindowedProbeSql, 2,
                () => [Int(FreshId), Stamp(windowTwoDays)]),
        };

        foreach (var (table, name, sql, windowDays, parameters) in probes)
        {
            var planned = await PlannedChunksAsync(connection, ct, sql, chunks[table], parameters());
            Assert.True(planned.Count >= 1 && planned.Count <= windowDays + 2,
                $"{name} is planned against {planned.Count} of {chunks[table].Count} {table} chunks; a {windowDays}-day window should keep at most {windowDays + 2}:\n{string.Join(", ", planned)}");
        }

        /* The control: the correlated form, on the same store, keeps every retained chunk in the plan. */
        var correlated = DarlingSessionStatsReader.LatestSql.Replace(
            "collection_time = $2",
            "collection_time = (SELECT MAX(collection_time) FROM v_session_stats WHERE server_id = $1)",
            StringComparison.Ordinal);
        Assert.NotEqual(DarlingSessionStatsReader.LatestSql, correlated);
        var control = await PlannedChunksAsync(connection, ct, correlated, chunks["session_stats"], Int(FreshId));
        Assert.True(control.Count == chunks["session_stats"].Count,
            $"the correlated control is planned against {control.Count} of {chunks["session_stats"].Count} session_stats chunks; the planner now excludes chunks for that form too, so the one-chunk assertion above no longer tells the two shapes apart");
    }

    private static NpgsqlParameter Int(int value) => new NpgsqlParameter<int> { TypedValue = value };
    private static NpgsqlParameter Text(string value) => new NpgsqlParameter<string> { TypedValue = value };
    private static NpgsqlParameter Stamp(DateTime value) => new NpgsqlParameter<DateTime> { TypedValue = Naive(value) };

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params NpgsqlParameter[] parameters)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>Every chunk of every hypertable in the store, by hypertable name.</summary>
    private static async Task<Dictionary<string, HashSet<string>>> ChunksByHypertableAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var chunks = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        using var command = new NpgsqlCommand("SELECT hypertable_name, chunk_name FROM timescaledb_information.chunks", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (!chunks.TryGetValue(reader.GetString(0), out var set))
            {
                chunks[reader.GetString(0)] = set = new HashSet<string>(StringComparer.Ordinal);
            }

            set.Add(reader.GetString(1));
        }

        return chunks;
    }

    /// <summary>The distinct chunks of <paramref name="tableChunks"/> that <c>EXPLAIN</c> (planned, not run) keeps in the
    /// statement's plan, read off every node's relation name. The parameters are bound the way the reader binds them, which
    /// is what lets the planner see the snapshot as a literal.</summary>
    private static async Task<SortedSet<string>> PlannedChunksAsync(
        NpgsqlConnection connection, CancellationToken ct, string sql, HashSet<string> tableChunks, params NpgsqlParameter[] parameters)
    {
        using var command = new NpgsqlCommand("EXPLAIN (COSTS OFF, FORMAT JSON) " + sql, connection);
        command.Parameters.AddRange(parameters);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);

        var planned = new SortedSet<string>(StringComparer.Ordinal);
        void Visit(JsonElement node)
        {
            if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() is string name && tableChunks.Contains(name))
            {
                planned.Add(name);
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Visit(child);
                }
            }
        }

        foreach (var statement in document.RootElement.EnumerateArray())
        {
            Visit(statement.GetProperty("Plan"));
        }

        return planned;
    }

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

    private const string SeedIndexSql =
        @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)";

    /// <summary>One index per snapshot, named for its stamp, with nonzero lock waits so the locking read's contention filter keeps it.</summary>
    private static Task SeedIndexAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime stamp) =>
        SeedIndexAsync(connection, ct, serverId, serverName, stamp, 2, Tag(stamp));

    private static Task SeedIndexAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime stamp, int indexId, string indexName) =>
        DarlingMcpTestData.ExecAsync(connection, ct, SeedIndexSql, SeedIndexValues(serverId, serverName, stamp, indexId, indexName));

    /// <summary>The synchronous twin, for a seed planted from inside an Npgsql activity callback.</summary>
    private static void SeedIndex(NpgsqlConnection connection, int serverId, string serverName, DateTime stamp, int indexId, string indexName)
    {
        using var command = new NpgsqlCommand(SeedIndexSql, connection);
        foreach (var value in SeedIndexValues(serverId, serverName, stamp, indexId, indexName))
            command.Parameters.AddWithValue(value);
        command.ExecuteNonQuery();
    }

    private static object[] SeedIndexValues(int serverId, string serverName, DateTime stamp, int indexId, string indexName) =>
        [CollectionIdGenerator.Next(), Naive(stamp), serverId, serverName, Db, "dbo", 100, "Orders", indexId, indexName, "NONCLUSTERED",
         90m, 85m, 500_000L, 500L, 10L, 20L, 300L, 40L, 12000L, 5L, 800L, 3L, 60L, 30L];

    /// <summary>
    /// Runs one action the first time a command on the named database whose text carries every fragment completes
    /// (Npgsql stops the command's activity when its reader closes), before the caller that ran the command gets control
    /// back. The same process-wide listener and value-matched tags as <see cref="NpgsqlCommandCounter"/>.
    /// <para>The action runs inside the command's reader close, so a throw from it would surface as the command's own
    /// failure, in the tool's error payload, with the fixture's fault reported as the product's. The hook keeps the
    /// throw in <see cref="Error"/> instead, for the test to assert on before it reads the payload.</para>
    /// </summary>
    private sealed class NpgsqlCommandCompletionHook : IDisposable
    {
        private readonly string _databaseName;
        private readonly string[] _fragments;
        private readonly Action _onCompleted;
        private readonly System.Diagnostics.ActivityListener _listener;
        private int _fired;
        private Exception? _error;

        public NpgsqlCommandCompletionHook(string databaseName, string[] fragments, Action onCompleted)
        {
            _databaseName = databaseName;
            _fragments = fragments;
            _onCompleted = onCompleted;
            _listener = new System.Diagnostics.ActivityListener
            {
                ShouldListenTo = source => source.Name == "Npgsql",
                Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) => System.Diagnostics.ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = OnStopped,
            };
            System.Diagnostics.ActivitySource.AddActivityListener(_listener);
        }

        public bool Fired => Volatile.Read(ref _fired) == 1;

        /// <summary>What the action threw, or null when it ran clean or has not run.</summary>
        public Exception? Error => Volatile.Read(ref _error);

        private void OnStopped(System.Diagnostics.Activity activity)
        {
            var onThisDatabase = false;
            var carriesTheText = false;
            foreach (var tag in activity.TagObjects)
            {
                if (tag.Value is not string value)
                    continue;
                if (string.Equals(value, _databaseName, StringComparison.Ordinal))
                    onThisDatabase = true;
                else if (_fragments.All(fragment => value.Contains(fragment, StringComparison.Ordinal)))
                    carriesTheText = true;
            }
            if (onThisDatabase && carriesTheText && Interlocked.Exchange(ref _fired, 1) == 0)
            {
                try
                {
                    _onCompleted();
                }
                catch (Exception ex)
                {
                    Volatile.Write(ref _error, ex);
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }

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
