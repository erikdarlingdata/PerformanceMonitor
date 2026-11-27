/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace Darling.Tests;

/// <summary>
/// #9374: the last measured relation sizes the service keeps in the store for the Viewer's first open. The rows written equal a live pass's map, the
/// presized reads fed the stored rows return what a read that sizes each relation itself returns, a dropped relation's row goes at the next write, a
/// write that changes one relation updates one row, a store behind the rung reads null, and the cadence reuses a fresh pass and runs exactly one when the
/// held one is stale.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class StoreRelationSizesLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipMessage = "Set DARLING_TEST_PG to a Postgres connection string to run the #9374 live tests.";

    /* Plain tables with an index and TOASTed text, a hypertable with chunks and an index, and a partitioned table. The TOAST rows are
       incompressible text, so the values leave the heap for the TOAST table. */
    private const string SeedSql = @"
CREATE SCHEMA zz9374;
DO $$
BEGIN
    FOR i IN 1..12 LOOP
        EXECUTE format('CREATE TABLE zz9374.t%s (id integer PRIMARY KEY, v text)', i);
        EXECUTE format('CREATE INDEX t%s_v ON zz9374.t%s (v)', i, i);
    END LOOP;
END $$;
CREATE TABLE zz9374.big (id integer PRIMARY KEY, payload text);
INSERT INTO zz9374.big SELECT g, (SELECT string_agg(md5(random()::text || g::text || s::text), '') FROM generate_series(1, 200) AS s) FROM generate_series(1, 40) AS g;
CREATE TABLE zz9374.ht (ts timestamptz NOT NULL, dev integer NOT NULL, v double precision);
SELECT create_hypertable('zz9374.ht', 'ts', chunk_time_interval => interval '1 day');
CREATE INDEX ht_dev ON zz9374.ht (dev, ts DESC);
INSERT INTO zz9374.ht SELECT now() - (d || ' days')::interval, d, 1 FROM generate_series(0, 6) AS d;
CREATE TABLE zz9374.pt (d date NOT NULL, v text) PARTITION BY RANGE (d);
CREATE TABLE zz9374.pt_1 PARTITION OF zz9374.pt FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE INDEX pt_v ON zz9374.pt (v);
INSERT INTO zz9374.t1 SELECT g, md5(g::text) FROM generate_series(1, 500) AS g;
ANALYZE;";

    private static async Task ExecAsync(NpgsqlDataSource store, string sql, CancellationToken ct)
    {
        await using var command = store.CreateCommand(sql);
        command.CommandTimeout = 300;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlDataSource store, string sql, CancellationToken ct)
    {
        await using var command = store.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static bool OwnTable(string schema, string table) =>
        schema == "collect" && table.StartsWith("store_relation_sizes_", StringComparison.Ordinal);

    private static async Task<HashSet<long>> OidsOfAsync(NpgsqlDataSource store, string relation, CancellationToken ct)
    {
        var oids = new HashSet<long>();
        await using var command = store.CreateCommand(
            $"SELECT '{relation}'::regclass::oid::bigint UNION SELECT indexrelid::oid::bigint FROM pg_catalog.pg_index WHERE indrelid = '{relation}'::regclass");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            oids.Add(reader.GetInt64(0));
        }

        return oids;
    }

    private static List<(long Oid, long Parent, long Main, long Other, long Toast)> Rows(RelationSizePass pass) =>
        Enumerable.Range(0, pass.Oids.Length)
            .Select(i => (pass.Oids[i], pass.Parents[i], pass.Main[i], pass.OtherForks[i], pass.Toast[i]))
            .OrderBy(r => r.Item1)
            .ToList();

    private static async Task<Dictionary<long, long>> RowVersionsAsync(NpgsqlDataSource store, CancellationToken ct)
    {
        var versions = new Dictionary<long, long>();
        await using var command = store.CreateCommand("SELECT rel_oid, xmin::text::bigint FROM collect.store_relation_sizes_latest");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            versions[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        return versions;
    }

    /// <summary>The relations both size passes on <paramref name="source"/> have stat'ed so far.</summary>
    private static long RelationsSizedOn(NpgsqlDataSource source) => MainRelationsSizedOn(source) + ExtraRelationsSizedOn(source);

    [Fact]
    public async Task AFailedExtrasPass_RecordsNoCombinedPass_AndWritesNothing_UntilTheExtrasFinish()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var rig = new Rig();

            // The main pass finishes (the indexes read) and the extras pass fails (the tables read): half a pass is not recorded or written.
            FailExtrasPassesOn(store, new InvalidOperationException("injected extras failure"));
            Assert.NotEmpty((await rig.Cache.GetIndexesAsync(store, cancellationToken: ct)).Rows);
            await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Cache.GetTablesAsync(store, fresh: true, cancellationToken: ct));
            await rig.Cache.WhenIdleAsync();
            Assert.True(MainRelationsSizedOn(store) > 0);
            Assert.Null(LastSizePassOn(store));
            Assert.Equal(0, rig.WriteCount);
            Assert.Null(await StoreRelationSizesStore.ReadLastSizesAsync(store, ct));

            // The extras then finish for the same relation list: the combined pass is recorded and written once.
            FailExtrasPassesOn(store, null);
            await rig.Cache.GetTablesAsync(store, fresh: true, cancellationToken: ct);
            await rig.Cache.WhenIdleAsync();
            var recorded = LastSizePassOn(store);
            Assert.NotNull(recorded);
            Assert.True(recorded.Pass.Oids.Length >= 60, $"pass sized {recorded.Pass.Oids.Length}");
            Assert.Equal(1, rig.WriteCount);
            Assert.NotNull(await StoreRelationSizesStore.ReadLastSizesAsync(store, ct));
            Assert.Empty(rig.Reported);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static MeasuredSizePass With(MeasuredSizePass source, Func<long, bool> keep, Func<long, long> mainBytes, DateTime at)
    {
        var p = source.Pass;
        var kept = Enumerable.Range(0, p.Oids.Length).Where(i => keep(p.Oids[i])).ToArray();
        return new MeasuredSizePass(
            new RelationSizePass(
                kept.Select(i => p.Oids[i]).ToArray(),
                kept.Select(i => p.Parents[i]).ToArray(),
                kept.Select(i => p.Main[i] + mainBytes(p.Oids[i])).ToArray(),
                kept.Select(i => p.OtherForks[i]).ToArray(),
                kept.Select(i => p.Toast[i]).ToArray()),
            at);
    }

    /// <summary>One cache over the real reads that writes each pass it takes to the store, with the failures it reported and the writes it made.</summary>
    private sealed class Rig
    {
        private int _writes;

        public Rig(Func<DateTime>? clock = null)
        {
            Cache = new DarlingStoreTableHealthCache(
                clock ?? (static () => DateTime.UtcNow), GetAsync, GetIndexesAsync, GetIndexCoverageAsync)
            {
                OnReadFailed = (part, ex) => Reported.Add(part + ": " + ex.Message),
            };
            Cache.OnSizesMeasured = async (source, measured, token) =>
            {
                await StoreRelationSizesStore.WriteAsync(source, measured, token);
                Interlocked.Increment(ref _writes);
            };
        }

        public DarlingStoreTableHealthCache Cache { get; }

        public List<string> Reported { get; } = [];

        public int WriteCount => Volatile.Read(ref _writes);
    }

    [Fact]
    public async Task TheStoredRows_EqualALivePassesMap_AndThePresizedReadsFedThemMatchTheLiveSizes()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);

            var rig = new Rig();
            await rig.Cache.GetTablesAsync(store, cancellationToken: ct);
            await rig.Cache.WhenIdleAsync();
            Assert.Empty(rig.Reported);

            var live = LastSizePassOn(store);
            Assert.NotNull(live);
            var persisted = await StoreRelationSizesStore.ReadLastSizesAsync(store, ct);
            Assert.NotNull(persisted);

            // The written rows are the live pass's map, relation for relation, and the stamp is the time the pass finished.
            Assert.True(live.Pass.Oids.Length >= 60, $"pass sized {live.Pass.Oids.Length}");
            Assert.Equal(Rows(live.Pass), Rows(persisted.Pass));
            Assert.True(Math.Abs((persisted.MeasuredAtUtc - live.MeasuredAtUtc).Ticks) < 20, "measured_at is the pass's time to the microsecond");
            Assert.Equal(DateTimeKind.Utc, persisted.MeasuredAtUtc.Kind);
            Assert.Contains(persisted.Pass.Toast, t => t > 0);

            // The presized reads fed the stored map return what a read that sizes each relation itself returns.
            var tables = await GetAsync(store, DateTime.UtcNow, persisted.Pass, ct);
            await using (var command = store.CreateCommand(BuildSql(timescale: true)))
            {
                command.CommandTimeout = 300;
                await using var reader = await command.ExecuteReaderAsync(ct);
                var compared = 0;
                var toastSeen = false;
                while (await reader.ReadAsync(ct))
                {
                    // The write itself grew these two tables after the pass sized them empty, so their live size is not the size the pass took.
                    if (OwnTable(reader.GetString(0), reader.GetString(1)))
                    {
                        compared++;
                        continue;
                    }

                    var row = tables.Single(t => t.SchemaName == reader.GetString(0) && t.TableName == reader.GetString(1));
                    Assert.Equal(reader.GetInt64(4), row.HeapBytes);
                    Assert.Equal(reader.GetInt64(5), row.IndexBytes);
                    Assert.Equal(reader.GetInt64(6), row.ToastBytes);
                    toastSeen |= row.ToastBytes > 0;
                    compared++;
                }

                Assert.Equal(tables.Count, compared);
                Assert.True(toastSeen, "the seed has a table with TOAST");
                Assert.Contains(tables, t => t.Kind == KindHypertable);
            }

            var indexes = await GetIndexesAsync(store, persisted.Pass, ct);
            await using (var command = store.CreateCommand(BuildIndexSql(timescale: true)))
            {
                command.CommandTimeout = 300;
                await using var reader = await command.ExecuteReaderAsync(ct);
                var compared = 0;
                while (await reader.ReadAsync(ct))
                {
                    if (OwnTable(reader.GetString(0), reader.GetString(1)))
                    {
                        compared++;
                        continue;
                    }

                    var row = indexes.Single(i => i.SchemaName == reader.GetString(0) && i.TableName == reader.GetString(1) && i.IndexName == reader.GetString(3));
                    Assert.Equal(reader.GetInt64(7), row.Bytes);
                    Assert.Equal(reader.GetInt64(8), row.HeapBytes);
                    compared++;
                }

                Assert.Equal(indexes.Count, compared);
                Assert.True(compared >= 10, $"compared {compared} index rows");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ADroppedRelationsRow_IsRemovedOnTheNextWrite_WhetherThePassDropsItOrStillHoldsIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var rig = new Rig();
            await rig.Cache.GetTablesAsync(store, cancellationToken: ct);
            await rig.Cache.WhenIdleAsync();
            var first = LastSizePassOn(store)!;

            var t2 = await OidsOfAsync(store, "zz9374.t2", ct);
            var t3 = await OidsOfAsync(store, "zz9374.t3", ct);
            var t4 = await OidsOfAsync(store, "zz9374.t4", ct);
            Assert.True(t2.Count >= 3, "a heap, its primary key and its index");
            var before = Rows((await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!.Pass).Select(r => r.Oid).ToHashSet();
            Assert.True(t2.All(before.Contains) && t3.All(before.Contains));

            // t2 is dropped and a later pass no longer holds it: the write removes the rows of the heap and of every index on it.
            await ExecAsync(store, "DROP TABLE zz9374.t2", ct);
            var second = With(first, oid => !t2.Contains(oid), _ => 0, DateTime.UtcNow);
            var write = await StoreRelationSizesStore.WriteAsync(store, second, ct);
            Assert.Equal(t2.Count, write.Deleted);
            var afterSecond = Rows((await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!.Pass).Select(r => r.Oid).ToHashSet();
            Assert.DoesNotContain(afterSecond, t2.Contains);

            // t3 is dropped after the pass that holds it was taken (a stale pass): the write still removes its rows.
            await ExecAsync(store, "DROP TABLE zz9374.t3", ct);
            var third = With(second, _ => true, _ => 0, DateTime.UtcNow);
            Assert.True(t3.All(third.Pass.Oids.Contains));
            var staleWrite = await StoreRelationSizesStore.WriteAsync(store, third, ct);
            Assert.Equal(t3.Count, staleWrite.Deleted);

            var after = Rows((await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!.Pass).Select(r => r.Oid).ToHashSet();
            Assert.DoesNotContain(after, t3.Contains);
            Assert.True(t4.All(after.Contains));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ASecondWriteOfTheSameSizes_LocksNoRow_AndLeavesTheRowsAsTheFirstWriteLeftThem()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var rig = new Rig();
            await rig.Cache.GetTablesAsync(store, cancellationToken: ct);
            await rig.Cache.WhenIdleAsync();
            var first = LastSizePassOn(store)!;
            var versionsBefore = await RowVersionsAsync(store, ct);
            Assert.True(versionsBefore.Count >= 60);

            // The same sizes again, an hour later (#9374): PostgreSQL stamps xmax on every row a DO UPDATE reaches, even when its WHERE is false.
            var write = await StoreRelationSizesStore.WriteAsync(store, With(first, _ => true, _ => 0, first.MeasuredAtUtc.AddHours(1)), ct);

            Assert.Equal(0, write.Upserted);
            Assert.Equal(0, await ScalarAsync(store, "SELECT count(*) FROM collect.store_relation_sizes_latest WHERE xmax::text::bigint <> 0", ct));
            Assert.Equal(versionsBefore, await RowVersionsAsync(store, ct));
            var persisted = (await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!;
            Assert.Equal(Rows(first.Pass), Rows(persisted.Pass));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ASecondWrite_WhereOneRelationChanged_UpdatesExactlyOneRow_AndRestampsTheTime()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var rig = new Rig();
            await rig.Cache.GetTablesAsync(store, cancellationToken: ct);
            await rig.Cache.WhenIdleAsync();
            var first = LastSizePassOn(store)!;
            var changed = await ScalarAsync(store, "SELECT 'zz9374.t5'::regclass::oid::bigint", ct);

            var versionsBefore = await RowVersionsAsync(store, ct);
            Assert.True(versionsBefore.Count >= 60);

            var later = first.MeasuredAtUtc.AddMinutes(5);
            var second = With(first, _ => true, oid => oid == changed ? 8192 : 0, later);
            var write = await StoreRelationSizesStore.WriteAsync(store, second, ct);

            // Every row of the pass is present and equal to it, one row was written, and only that row's version moved.
            Assert.Equal(1, write.Upserted);
            Assert.Equal(0, write.Deleted);
            var versionsAfter = await RowVersionsAsync(store, ct);
            Assert.Equal(versionsBefore.Keys.OrderBy(k => k), versionsAfter.Keys.OrderBy(k => k));
            var moved = versionsAfter.Where(kv => kv.Value != versionsBefore[kv.Key]).Select(kv => kv.Key).ToList();
            Assert.Equal([changed], moved);

            var persisted = (await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!;
            Assert.Equal(Rows(second.Pass), Rows(persisted.Pass));

            // The time is the one-row stamp, set on every write even though the rows did not move.
            Assert.True(Math.Abs((persisted.MeasuredAtUtc - later).Ticks) < 20);
            var third = With(second, _ => true, _ => 0, later.AddMinutes(5));
            var unchanged = await StoreRelationSizesStore.WriteAsync(store, third, ct);
            Assert.Equal(0, unchanged.Upserted);
            var restamped = (await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!;
            Assert.True(Math.Abs((restamped.MeasuredAtUtc - later.AddMinutes(5)).Ticks) < 20);
            Assert.Equal(versionsAfter, await RowVersionsAsync(store, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreBehindTheRung_AndAnEmptyTable_ReadNull()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        // No collect schema at all: the store is behind the rung.
        await using (var empty = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true))
        {
            var behind = false;
            try
            {
                await using var store = NpgsqlDataSource.Create(empty.ConnectionString);
                Assert.Null(await StoreRelationSizesStore.ReadLastSizesAsync(store, ct));
                behind = true;
            }
            finally
            {
                await LiveStoreCleanup.RunAsync(empty.ConnectionString, behind, static (_, _) => Task.CompletedTask);
            }
        }

        // A migrated store nothing has written to.
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            Assert.Null(await StoreRelationSizesStore.ReadLastSizesAsync(store, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheCadence_ReusesAFreshPass_AndRunsExactlyOnePassWhenTheHeldOneIsStale()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            // Two data sources on one store: each keeps its own size pass, so the stale step below runs a real pass the 15-second pass reuse cannot answer.
            await using var first = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var second = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(first, SeedSql, ct);

            var now = new DateTime(2026, 11, 23, 12, 0, 0, DateTimeKind.Utc);
            var rig = new Rig(() => now);
            var cadence = TimeSpan.FromMinutes(60);

            Assert.True(await rig.Cache.RunDueSizePassAsync(first, cadence, ct));
            await rig.Cache.WhenIdleAsync();
            var sized = RelationsSizedOn(first);
            Assert.True(sized >= 60);
            Assert.Equal(1, rig.WriteCount);
            Assert.NotNull(await StoreRelationSizesStore.ReadLastSizesAsync(first, ct));

            // A pass newer than the cadence is reused: no pass runs and nothing more is written.
            now = now.AddMinutes(59);
            Assert.False(await rig.Cache.RunDueSizePassAsync(first, cadence, ct));
            await rig.Cache.WhenIdleAsync();
            Assert.Equal(sized, RelationsSizedOn(first));
            Assert.Equal(1, rig.WriteCount);

            // Past the cadence, exactly one pass runs through the cache and is written.
            now = now.AddMinutes(2);
            Assert.True(await rig.Cache.RunDueSizePassAsync(second, cadence, ct));
            await rig.Cache.WhenIdleAsync();
            Assert.Equal(sized, RelationsSizedOn(second));
            Assert.Equal(sized, RelationsSizedOn(first));
            Assert.Equal(2, rig.WriteCount);

            // And that pass is held in turn: the next check inside the cadence runs none.
            Assert.False(await rig.Cache.RunDueSizePassAsync(second, cadence, ct));
            Assert.Equal(sized, RelationsSizedOn(second));
            Assert.Empty(rig.Reported);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheStampAndTheRows_AreReadInOneSnapshot_SoAWriteBetweenTheTwoReadsIsNotPairedWithTheOldTime()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, "CREATE SCHEMA zz9374; CREATE TABLE zz9374.a (id integer); CREATE TABLE zz9374.b (id integer);", ct);
            var a = await ScalarAsync(store, "SELECT 'zz9374.a'::regclass::oid::bigint", ct);
            var b = await ScalarAsync(store, "SELECT 'zz9374.b'::regclass::oid::bigint", ct);
            var firstAt = new DateTime(2026, 11, 20, 8, 0, 0, DateTimeKind.Utc);
            var secondAt = firstAt.AddMinutes(30);
            MeasuredSizePass Pass(long mainA, long mainB, DateTime at) =>
                new(new RelationSizePass([a, b], [a, b], [mainA, mainB], [0, 0], [0, 0]), at);
            var first = Pass(8192, 16384, firstAt);
            await StoreRelationSizesStore.WriteAsync(store, first, ct);

            // A write commits after the stamp is read and before the rows are: the rows and the time read together are still the first pass.
            var seen = await StoreRelationSizesStore.ReadLastSizesAsync(
                store,
                async token => await StoreRelationSizesStore.WriteAsync(store, Pass(24576, 32768, secondAt), token),
                ct);

            Assert.NotNull(seen);
            Assert.True(Math.Abs((seen.MeasuredAtUtc - firstAt).Ticks) < 20, "the time is the first pass's");
            Assert.Equal(Rows(first.Pass), Rows(seen.Pass));

            // The next read, with nothing in between, sees the second pass whole.
            var after = (await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!;
            Assert.True(Math.Abs((after.MeasuredAtUtc - secondAt).Ticks) < 20);
            Assert.Equal(Rows(Pass(24576, 32768, secondAt).Pass), Rows(after.Pass));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheViewersLastSizesRead_ReturnsTheTablesFromTheStoredPass_AndNullForAnEmptyTable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var asOf = DateTime.UtcNow;

            // Nothing written yet: no last sizes, so the Viewer reads live.
            Assert.Null(await PerformanceMonitor.Darling.Viewer.ViewerDataService.ReadLastSizesHealthAsync(
                store, StoreRelationSizesStore.ReadLastSizesAsync, asOf, ct));

            var rig = new Rig();
            await rig.Cache.GetTablesAsync(store, cancellationToken: ct);
            await rig.Cache.WhenIdleAsync();
            var persisted = (await StoreRelationSizesStore.ReadLastSizesAsync(store, ct))!;

            var last = await PerformanceMonitor.Darling.Viewer.ViewerDataService.ReadLastSizesHealthAsync(
                store, StoreRelationSizesStore.ReadLastSizesAsync, asOf, ct);

            Assert.NotNull(last);
            Assert.Equal(persisted.MeasuredAtUtc, last.MeasuredAtUtc);
            var table = Assert.Single(last.Tables.Rows, r => r.SchemaName == "zz9374" && r.TableName == "t1");
            Assert.True(table.HeapBytes > 0);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
