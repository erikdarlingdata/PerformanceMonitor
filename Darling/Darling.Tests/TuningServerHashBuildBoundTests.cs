/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9289: the start-path build of <c>idx_query_store_stats_server_hash_time</c> is bounded. A build that runs out of time is recorded
/// (when, the table's uncompressed heap pages, the elapsed time); the next start skips the build while the record is under 24 hours old
/// and the table is no smaller, and a successful build deletes the record. The timeout is forced without a sleep: the build's own
/// CommandTimeout is 1 s (the test seam) while another session holds a lock the CREATE INDEX queues behind, so it can never finish in time.
/// Run on a plain store and on a TimescaleDB hypertable (the pages read differs). Own scratch database per test.
/// </summary>
/* #1776 own-store: every live test here mints its own scratch database through ScratchPostgres. */
public sealed class TuningServerHashBuildBoundTests
{
    private const string Index = PgTableTuning.QueryStoreServerHashIndexName;
    private static readonly DateTime T0 = new(2030, 1, 1, 6, 0, 0, DateTimeKind.Utc);

    private sealed class FixedClock(DateTime utc) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => new(utc, TimeSpan.Zero);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ATimedOutBuildIsRecorded_AndTheNextStartSkipsItWithAWarning(bool hypertable)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(hypertable, ct);
        await store.TimeOutOnceAsync(ct);

        var record = await store.ReadRecordAsync(ct);
        Assert.NotNull(record);
        Assert.Equal(T0, record!.AttemptedAt);
        Assert.True(record.Pages > 0, "the seeded table has heap pages");
        Assert.True(record.ElapsedMs > 0);
        Assert.False(await store.IndexExistsAsync(ct));

        /* The lock is gone now, so a build would succeed: an index still absent after this start is the skip. */
        var log = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(store.Body, log, hourly: false, new FixedClock(T0.AddHours(1)), 300, ct);
        Assert.False(await store.IndexExistsAsync(ct), log.Joined);
        Assert.Single(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
            && l.Contains(Index, StringComparison.Ordinal)
            && l.Contains("is not built this start", StringComparison.Ordinal)
            && l.Contains(record.Pages + " uncompressed heap pages", StringComparison.Ordinal));
        Assert.Equal(record, await store.ReadRecordAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADayLater_TheNextStartBuildsIt_AndTheRecordIsGone(bool hypertable)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(hypertable, ct);
        await store.TimeOutOnceAsync(ct);

        var log = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(store.Body, log, hourly: false, new FixedClock(T0.AddHours(24).AddMinutes(1)), 300, ct);
        Assert.True(await store.IndexExistsAsync(ct), log.Joined);
        Assert.Null(await store.ReadRecordAsync(ct));
        Assert.DoesNotContain(log.Lines, l => l.Contains("is not built this start", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WithFewerPagesThanRecorded_TheNextStartBuildsIt_AndTheRecordIsGone(bool hypertable)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(hypertable, ct);
        await store.TimeOutOnceAsync(ct);
        await Store.ExecAsync(store.Body, "UPDATE collect.index_build_attempts SET heap_pages = heap_pages + 100000", ct);

        var log = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(store.Body, log, hourly: false, new FixedClock(T0.AddHours(1)), 300, ct);
        Assert.True(await store.IndexExistsAsync(ct), log.Joined);
        Assert.Null(await store.ReadRecordAsync(ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ASuccessfulBuildLeavesNoRecord(bool hypertable)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(hypertable, ct);
        await Store.ExecAsync(store.Body, "DROP INDEX collect." + Index, ct);

        var log = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(store.Body, log, hourly: false, new FixedClock(T0), 300, ct);
        Assert.True(await store.IndexExistsAsync(ct), log.Joined);
        Assert.Null(await store.ReadRecordAsync(ct));
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
    }

    private sealed record Record(DateTime AttemptedAt, long Pages, long ElapsedMs);

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, NpgsqlConnection body)
        {
            _scratch = scratch;
            Body = body;
        }

        public NpgsqlConnection Body { get; }

        public static async Task<Store> CreateAsync(bool hypertable, CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9289 live tests.");
            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            var body = new NpgsqlConnection(scratch.ConnectionString);
            try
            {
                await body.OpenAsync(ct);
                await PgMigrations.MigrateAsync(body, ct);
                if (hypertable)
                {
                    var enabled = await TimescaleSupport.TryEnableAsync(body, null, ct);
                    Assert.SkipWhen(!enabled, "The hypertable variant needs TimescaleDB.");
                    await TimescaleSupport.ConvertToHypertablesAsync(body, null, ct);
                    Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(body, null, ct));
                    await ExecAsync(body, "SELECT _timescaledb_functions.stop_background_workers()", ct);
                }

                await DarlingMcpTestData.RegisterServerAsync(body, -925001, "p9289t-1", ct);
                await TuningStartPasses.ConvergeAsync(body, ct);
                await ExecAsync(body, @"INSERT INTO collect.query_store_stats
                      (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
                       runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                    SELECT 925000000 + g, t.ts, -925001, 'p9289t-1', 'db', 'dbo.P', '0xh' || (g % 50), g, g, 'Regular', 'PRIMARY', g,
                           t.ts, t.ts, 1, 1000, 500, 900, 400, t.ts + interval '1 hour'
                    FROM generate_series(1, 20000) AS g
                    CROSS JOIN LATERAL (SELECT (now() AT TIME ZONE 'UTC') - g * interval '1 minute' AS ts) AS t", ct);
                foreach (var relation in await RelationsAsync(body, ct))
                {
                    await ExecAsync(body, "ANALYZE " + relation, ct);
                }

                return new Store(scratch, body);
            }
            catch
            {
                await body.DisposeAsync();
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>Drops the index, holds a lock the build queues behind, and runs a start whose build times out after 1 s.</summary>
        public async Task TimeOutOnceAsync(CancellationToken ct)
        {
            await ExecAsync(Body, "DROP INDEX collect." + Index, ct);
            await using (var holder = new NpgsqlConnection(_scratch.ConnectionString))
            {
                await holder.OpenAsync(ct);
                await ExecAsync(holder, "BEGIN; LOCK TABLE collect.query_store_stats IN SHARE UPDATE EXCLUSIVE MODE", ct);
                var log = new CapturingTestLogger();
                await PgTableTuning.ApplyAsync(Body, log, hourly: false, new FixedClock(T0), 1, ct);
                Assert.Contains(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("ran out of time", StringComparison.Ordinal));
                await ExecAsync(holder, "ROLLBACK", ct);
            }

            Assert.NotNull(await ReadRecordAsync(ct));
        }

        public async Task<bool> IndexExistsAsync(CancellationToken ct)
        {
            await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", Body);
            cmd.Parameters.AddWithValue(Index);
            return await cmd.ExecuteScalarAsync(ct) is not null;
        }

        /// <summary>The record row, or null when there is none or the table does not exist.</summary>
        public async Task<Record?> ReadRecordAsync(CancellationToken ct)
        {
            await using (var exists = new NpgsqlCommand("SELECT to_regclass('collect.index_build_attempts') IS NOT NULL", Body))
            {
                if (await exists.ExecuteScalarAsync(ct) is not true)
                {
                    return null;
                }
            }

            await using var cmd = new NpgsqlCommand("SELECT attempted_at, heap_pages, elapsed_ms FROM collect.index_build_attempts WHERE index_name = $1", Body);
            cmd.Parameters.AddWithValue(Index);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            return await reader.ReadAsync(ct)
                ? new Record(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc), reader.GetInt64(1), reader.GetInt64(2))
                : null;
        }

        public static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct)
        {
            await using var cmd = new NpgsqlCommand(sql, c);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private static async Task<List<string>> RelationsAsync(NpgsqlConnection c, CancellationToken ct)
        {
            var relations = new List<string> { "collect.query_store_stats" };
            await using var cmd = new NpgsqlCommand(
                "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'collect.query_store_stats'::regclass", c);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                relations.Add(reader.GetString(0));
            }

            return relations;
        }

        public async ValueTask DisposeAsync()
        {
            await Body.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
