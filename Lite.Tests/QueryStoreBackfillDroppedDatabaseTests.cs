/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5483, the Lite twin of Darling's dropped-database pins. A database dropped from the monitored server must
/// stop being a backfill candidate.
///
/// <para>The cause is not the <c>collector_state</c> keys: a failed slice writes no state, and the hourly orphan
/// prune retires a dropped database's <c>done:</c> and <c>hole:</c> keys (pinned below). The candidate list is read
/// from the COLLECTED ROWS in <c>query_store_stats</c> (plus any database a hole names), so a database that
/// shipped rows inside the horizon stayed a candidate until those rows aged out, and every tick tried it and
/// failed with "database does not exist". The fix reads the newest <c>database_states</c> snapshot: a candidate
/// absent from it, with no stored row at or after the snapshot, is gone.</para>
///
/// <para>The slice body is replaced through <see cref="RemoteCollectorService.SliceOverrideForTests"/> and throws
/// what the server throws for a missing database, so no SQL Server is needed; the tick loop, the DuckDB
/// candidate and stored-floor reads and the real orphan prune are the production ones.</para>
/// </summary>
public sealed class QueryStoreBackfillDroppedDatabaseTests : IClassFixture<SharedDuckDbFixture>
{
    private const string Kept = "aaa_kept_db";
    private const string Dropped = "bbb_dropped_db";
    private const string Created = "ccc_created_after_snapshot_db";
    private const string ServerLabel = "backfill-dropped-test";
    private const string MissingMessage = "Cannot open database requested by the login. The login failed. (database does not exist)";

    private readonly DuckDbInitializer _duckDb;

    public QueryStoreBackfillDroppedDatabaseTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ADroppedDatabase_StopsBeingTried_OnceTheNewestSnapshotLeavesItOut_AndItsStateIsPruned()
    {
        using var run = new Run(_duckDb);

        /* Two databases with a young stored history, so each has a pending tail slice. The dropped one also carries
           a hole key, written 20 minutes ago. */
        await run.SeedRowAsync(Kept, TimeSpan.FromHours(1));
        await run.SeedRowAsync(Dropped, TimeSpan.FromHours(1));
        await run.SeedHoleAsync(Dropped, TimeSpan.FromMinutes(20));

        /* The first snapshot names both: the drop has not happened yet, so the dropped database is still worked. */
        await run.SeedSnapshotAsync(TimeSpan.FromMinutes(30), Kept, Dropped);
        await run.TicksAsync(4);
        Assert.Contains(Dropped, run.Attempts);

        /* The drop: the newest snapshot (5 minutes ago, slower than the 5-minute backfill tick) no longer names it. */
        await run.SeedSnapshotAsync(TimeSpan.FromMinutes(5), Kept);
        run.Attempts.Clear();

        await run.Harness.PruneAsync(run.ServerId);
        Assert.Null(await run.HoleAsync(Dropped));

        await run.TicksAsync(6);

        Assert.DoesNotContain(Dropped, run.Attempts);
        Assert.Null(await run.HoleAsync(Dropped));
    }

    [Fact]
    public async Task ADatabaseWithRowsNewerThanTheSnapshot_IsStillTried()
    {
        using var run = new Run(_duckDb);

        /* Created after the newest snapshot: absent from it, but its rows are newer than it. Dropping it would be
           the very mistake the prune's own freshness guard exists to avoid. */
        await run.SeedRowAsync(Kept, TimeSpan.FromHours(1));
        await run.SeedRowAsync(Created, TimeSpan.FromMinutes(2));
        await run.SeedSnapshotAsync(TimeSpan.FromMinutes(10), Kept);

        await run.TicksAsync(4);

        Assert.Contains(Created, run.Attempts);
    }

    [Fact]
    public async Task WithNoSnapshotAtAll_NothingIsDropped()
    {
        using var run = new Run(_duckDb);

        /* No database_states row for this server (Azure SQL DB never collects one; so does a server that has not
           collected yet): the list is not known, so every candidate stays. */
        await run.SeedRowAsync(Kept, TimeSpan.FromHours(1));
        await run.SeedRowAsync(Dropped, TimeSpan.FromHours(1));

        await run.TicksAsync(4);

        Assert.Contains(Kept, run.Attempts);
        Assert.Contains(Dropped, run.Attempts);
    }

    private sealed class Harness(DuckDbInitializer duckDb, ServerManager servers, ScheduleManager schedules)
        : RemoteCollectorService(duckDb, servers, schedules, logger: null!)
    {
        public Task<bool> TickAsync(ServerConnection server) => RunQueryStoreBackfillSliceAsync(server, CancellationToken.None);

        public Task PruneAsync(int serverId) => PruneOrphanedQueryStoreDatabaseStateAsync(serverId, CancellationToken.None);

        public Task MarkDoneAsync(int serverId, string database) => SaveCollectorStateAsync(
            serverId,
            QueryStoreBackfillState.StateCollectorName,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [QueryStoreBackfillState.DoneKeyPrefix + database] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            },
            CancellationToken.None);
    }

    /// <summary>One backfill run against the shared DuckDB: a harness whose slice body completes for every
    /// database except <see cref="Dropped"/>, which throws what a missing database throws.</summary>
    private sealed class Run : IDisposable
    {
        private readonly DuckDbInitializer _duckDb;
        private readonly string _configDirectory = Path.Combine(Path.GetTempPath(), "qs-backfill-dropped-" + Guid.NewGuid().ToString("N"));
        private readonly ServerConnection _server = new() { ServerName = ServerLabel, DisplayName = ServerLabel };
        private long _id = 548300;

        public Run(DuckDbInitializer duckDb)
        {
            _duckDb = duckDb;
            Directory.CreateDirectory(_configDirectory);
            Harness = new Harness(duckDb, new ServerManager(_configDirectory), new ScheduleManager(_configDirectory));
            ServerId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(_server));
            Harness.SliceOverrideForTests = async (database, span) =>
            {
                Attempts.Add(database);
                if (string.Equals(database, Dropped, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(MissingMessage);
                }

                await Harness.MarkDoneAsync(ServerId, database);
            };
        }

        public Harness Harness { get; }

        public int ServerId { get; }

        public List<string> Attempts { get; } = [];

        public async Task TicksAsync(int ticks)
        {
            for (var tick = 0; tick < ticks; tick++)
            {
                try
                {
                    await Harness.TickAsync(_server);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("database does not exist", StringComparison.Ordinal))
                {
                    /* The collection loop's per-server catch logs a failed slice and carries on to the next tick. */
                }
            }
        }

        /// <summary>One stored row <paramref name="age"/> old whose last_execution_time is an hour older: inside the
        /// horizon, so the database is a candidate, and newer than the floor, so a tail slice is pending.</summary>
        public async Task SeedRowAsync(string databaseName, TimeSpan age)
        {
            var now = DateTime.UtcNow;
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)";
            var id = ++_id;
            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = now - age });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerLabel });
            cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
            cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
            cmd.Parameters.Add(new DuckDBParameter { Value = now - age - TimeSpan.FromHours(1) });
            cmd.Parameters.Add(new DuckDBParameter { Value = now - age - TimeSpan.FromHours(1) });
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

        /// <summary>A hole key for <paramref name="databaseName"/>, its updated_at set explicitly so it can sit
        /// before a snapshot (a state save stamps "now", which no past snapshot could judge).</summary>
        public async Task SeedHoleAsync(string databaseName, TimeSpan age)
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)";
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = QueryStoreBackfillState.StateCollectorName });
            cmd.Parameters.Add(new DuckDBParameter { Value = QueryStoreBackfillState.HoleKeyPrefix + databaseName });
            cmd.Parameters.Add(new DuckDBParameter
            {
                Value = QueryStoreBackfillState.EncodeHole(DateTime.UtcNow.AddDays(-2), DateTime.UtcNow.AddHours(-5)),
            });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow - age });
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task SeedSnapshotAsync(TimeSpan age, params string[] databases)
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            var dbId = 1;
            foreach (var database in databases)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES ($1, $2, $3, $4, $5, $6, 'ONLINE', false)";
                cmd.Parameters.Add(new DuckDBParameter { Value = ++_id });
                cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow - age });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerLabel });
                cmd.Parameters.Add(new DuckDBParameter { Value = database });
                cmd.Parameters.Add(new DuckDBParameter { Value = dbId++ });
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<string?> HoleAsync(string databaseName)
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = QueryStoreBackfillState.StateCollectorName });
            cmd.Parameters.Add(new DuckDBParameter { Value = QueryStoreBackfillState.HoleKeyPrefix + databaseName });
            return await cmd.ExecuteScalarAsync() as string;
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_configDirectory, recursive: true);
            }
            catch (IOException)
            {
                /* Best effort: a leftover temp directory is harmless. */
            }
        }
    }
}
