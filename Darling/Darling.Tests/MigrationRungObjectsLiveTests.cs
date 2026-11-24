/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What each migration rung created, changed or dropped (#9438), against a real server. Each fact stages a small ladder in a scratch store
/// (<c>emptyStore: true</c>), applies it through the real migration runner with a start recorder as the observer, flushes the recorder, and reads
/// the answer back through <c>get_service_starts</c>.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
[Collection("timing")]
public sealed class MigrationRungObjectsLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the migration rung object tests.";

    private static readonly DateTime T0 = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    /// <summary>Rung 1: the history tables, and a table, an index and a function for later rungs to change. It moves the version table into collect, as V8 does, so a later start reads its stamps there.</summary>
    private static PgMigrations.Migration Setup() => new(1, "setup",
        "CREATE SCHEMA IF NOT EXISTS collect; ALTER TABLE darling_schema_version SET SCHEMA collect; " + ServiceStartHistory.CreateSql + ServiceStartRungObjects.CreateSql +
        @" CREATE TABLE collect.probe_existing (id integer NOT NULL, old_note text, keep_me integer, with_default integer DEFAULT 1, CONSTRAINT ck_old CHECK (id > 0));
           CREATE INDEX ix_probe ON collect.probe_existing(id);
           CREATE VIEW collect.probe_view AS SELECT id FROM collect.probe_existing;
           CREATE FUNCTION collect.probe_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1';");

    private static ServiceStartRecorder NewRecorder(string host, DateTime started, Func<DateTime> clock) =>
        new(new ServiceLogTally(), Guid.NewGuid(), host, "3.14.0-test.1", "0123abc", started, clock);

    /// <summary>Applies the ladder as one start of the service would, then flushes the start. Returns the start's id.</summary>
    private static async Task<Guid> StartAsync(
        ScratchPostgres scratch, NpgsqlDataSource source, string host, DateTime started, IReadOnlyList<PgMigrations.Migration> ladder, CancellationToken ct)
    {
        using var recorder = NewRecorder(host, started, () => started.AddMinutes(1));
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.ApplyLadderForTestsAsync(connection, null, ladder, 60, ct, recorder);
        recorder.StoreReady();
        Assert.True(await recorder.FlushAsync(source, NullLogger.Instance, ct));
        return recorder.StartId;
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static List<(string Kind, string Action, string Object, string? Column)> Rows(JsonElement start) =>
        start.GetProperty("objects").EnumerateArray().Select(o => (
            o.GetProperty("kind").GetString()!, o.GetProperty("action").GetString()!, o.GetProperty("object").GetString()!,
            o.GetProperty("column").GetString())).ToList();

    /// <summary>
    /// The issue's case: a store with a version, then a rung that adds a table, adds a column to an existing table and drops a column. The read for
    /// that rung lists all three with their kinds and actions, and does not list the new table's own columns.
    /// </summary>
    [Fact]
    public async Task ARungThatAddsATable_AddsAColumn_AndDropsAColumn_ListsAllThree()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await StartAsync(scratch, source, "host-a", T0, [Setup()], ct);

            var rung2 = new PgMigrations.Migration(2, "change-things",
                @"CREATE TABLE collect.probe_added (id integer NOT NULL, note text);
                  ALTER TABLE collect.probe_existing ADD COLUMN added_col integer;
                  ALTER TABLE collect.probe_existing DROP COLUMN old_note;");
            var rung3 = new PgMigrations.Migration(3, "add-third", "CREATE TABLE collect.probe_third (id integer NOT NULL);");
            var second = await StartAsync(scratch, source, "host-a", T0.AddHours(1), [Setup(), rung2, rung3], ct);

            var root = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: 2, cancellationToken: ct));
            var start = Assert.Single(root.GetProperty("starts").EnumerateArray());
            Assert.Equal(second, start.GetProperty("start_id").GetGuid());
            Assert.Equal(3, start.GetProperty("objects_total").GetInt32());

            var rows = Rows(start);
            Assert.Contains(("table", "create", "probe_added", null), rows);
            Assert.Contains(("column", "create", "probe_existing", "added_col"), rows);
            Assert.Contains(("column", "drop", "probe_existing", "old_note"), rows);
            Assert.All(start.GetProperty("objects").EnumerateArray(), o => Assert.Equal("collect", o.GetProperty("schema").GetString()));

            /* A rung after it lists only its own change: the closing catalog of one rung is the opening catalog of the next. */
            var third = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: 3, cancellationToken: ct));
            var thirdRows = Rows(Assert.Single(third.GetProperty("starts").EnumerateArray()));
            Assert.Equal(new[] { ("table", "create", "probe_third", (string?)null) }, thirdRows.ToArray());

            /* The start's rung rows carry the count, and the rung 1 row of the first start says fresh_install. */
            var all = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, cancellationToken: ct)).GetProperty("starts").EnumerateArray().ToList();
            var newest = all[0].GetProperty("rungs").EnumerateArray().Single(r => r.GetProperty("version").GetInt32() == 2);
            Assert.Equal(3, newest.GetProperty("objects").GetInt32());
            var oldest = all[1].GetProperty("rungs").EnumerateArray().Single(r => r.GetProperty("version").GetInt32() == 1);
            Assert.Equal("fresh_install", oldest.GetProperty("objects").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A column type change, an index redefinition and a function body change each read as an alter.</summary>
    [Fact]
    public async Task AColumnTypeChange_AnIndexRedefinition_AndAFunctionBodyChange_ReadAsAlter()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await StartAsync(scratch, source, "host-a", T0, [Setup()], ct);

            var rung2 = new PgMigrations.Migration(2, "alter-things",
                @"ALTER TABLE collect.probe_existing ALTER COLUMN keep_me TYPE bigint;
                  DROP INDEX collect.ix_probe;
                  CREATE INDEX ix_probe ON collect.probe_existing(id, keep_me);
                  CREATE OR REPLACE FUNCTION collect.probe_fn() RETURNS integer LANGUAGE sql AS 'SELECT 2';");
            await StartAsync(scratch, source, "host-a", T0.AddHours(1), [Setup(), rung2], ct);

            var root = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: 2, cancellationToken: ct));
            var rows = Rows(Assert.Single(root.GetProperty("starts").EnumerateArray()));
            Assert.Equal(3, rows.Count);
            Assert.Contains(("column", "alter", "probe_existing", "keep_me"), rows);
            Assert.Contains(("index", "alter", "ix_probe", null), rows);
            Assert.Contains(("function", "alter", "probe_fn()", null), rows);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A rung that changes only a column's nullability, a column's default, a constraint and a view's body is listed: the column and the view as an
    /// alter, the new constraint as a create and the removed one as a drop. The rung changes no column type, index or function.
    /// </summary>
    [Fact]
    public async Task ANullabilityChange_ADefaultChange_ConstraintChanges_AndAViewBodyChange_AreListed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await StartAsync(scratch, source, "host-a", T0, [Setup()], ct);

            var rung2 = new PgMigrations.Migration(2, "shape-changes",
                @"ALTER TABLE collect.probe_existing ALTER COLUMN keep_me SET NOT NULL;
                  ALTER TABLE collect.probe_existing ALTER COLUMN with_default SET DEFAULT 2;
                  ALTER TABLE collect.probe_existing ADD CONSTRAINT ck_new CHECK (keep_me > 0);
                  ALTER TABLE collect.probe_existing DROP CONSTRAINT ck_old;
                  CREATE OR REPLACE VIEW collect.probe_view AS SELECT id FROM collect.probe_existing WHERE id > 5;");
            await StartAsync(scratch, source, "host-a", T0.AddHours(1), [Setup(), rung2], ct);

            var root = Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: 2, cancellationToken: ct));
            var rows = Rows(Assert.Single(root.GetProperty("starts").EnumerateArray()));
            Assert.Equal(5, rows.Count);
            Assert.Contains(("column", "alter", "probe_existing", "keep_me"), rows);
            Assert.Contains(("column", "alter", "probe_existing", "with_default"), rows);
            Assert.Contains(("constraint", "create", "probe_existing.ck_new", null), rows);
            Assert.Contains(("constraint", "drop", "probe_existing.ck_old", null), rows);
            Assert.Contains(("view", "alter", "probe_view", null), rows);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A ladder that begins on an empty store records no object rows, only one marker for the start, and the read for any of its rungs is empty. A
    /// retention purge takes a start's object rows with it.
    /// </summary>
    [Fact]
    public async Task AFreshLadder_WritesOneMarkerAndNoObjectRows_AndThePurgeTakesTheRowsWithTheirStart()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rung2 = new PgMigrations.Migration(2, "add-table", "CREATE TABLE collect.probe_added (id integer NOT NULL);");
            await StartAsync(scratch, source, "host-a", T0, [Setup(), rung2], ct);

            Assert.Equal(0, await CountAsync(source, "SELECT count(*) FROM collect.service_start_rung_objects WHERE rung_version > 0", ct));
            Assert.Equal(1, await CountAsync(source, "SELECT count(*) FROM collect.service_start_rung_objects WHERE rung_version = 0", ct));
            Assert.Equal("empty", Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: 2, cancellationToken: ct)).GetProperty("status").GetString());

            var record = Assert.Single(await ServiceStartHistory.ReadAsync(source, 10, serviceHost: null, ct));
            Assert.All(record.Rungs, r => Assert.True(r.FreshInstall));

            await ServiceStartHistory.PurgeAsync(source, T0.AddDays(ServiceStartHistory.RetentionDays + 2), ct);
            Assert.Equal(0, await CountAsync(source, "SELECT count(*) FROM collect.service_start_rung_objects", ct));
            Assert.Equal(0, await CountAsync(source, "SELECT count(*) FROM collect.service_start_rungs", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>An out-of-range rung or hours_back is refused, never clamped, and a filter with no rows answers empty.</summary>
    [Fact]
    public async Task ARungOrHoursBackOutOfRange_IsRefused_AndAFilterWithNoRowsIsEmpty()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await StartAsync(scratch, source, "host-a", T0, [Setup()], ct);

            foreach (var bad in new[] { 0, -1, StorageVersion.SchemaVersion + 1 })
            {
                Assert.Equal("invalid", Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: bad, cancellationToken: ct)).GetProperty("status").GetString());
            }

            foreach (var bad in new[] { 0, DarlingMcpServiceStartTools.MaxHoursBack + 1 })
            {
                Assert.Equal("invalid", Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, hours_back: bad, cancellationToken: ct)).GetProperty("status").GetString());
            }

            Assert.Equal("empty", Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, rung: 1, service_host: "host-z", cancellationToken: ct)).GetProperty("status").GetString());

            /* The start began long ago, so a recent window holds none of it. */
            Assert.Equal("empty", Parse(await DarlingMcpServiceStartTools.GetServiceStarts(source, hours_back: 1, cancellationToken: ct)).GetProperty("status").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The snapshot of a fully migrated store is taken in a small fraction of a second: it runs twice per rung, in the rung's own transaction. The bound is
    /// ten times the figure aimed for, so only a real slowdown fails it. The median is sent to the test output.
    /// </summary>
    [Fact]
    public async Task TheSnapshotOfAMigratedStore_IsTakenInWellUnderASecond()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            var times = new List<double>();
            MigrationCatalogSnapshot? snapshot = null;
            for (var i = 0; i < 5; i++)
            {
                var clock = Stopwatch.StartNew();
                snapshot = await MigrationCatalogSnapshot.TryTakeAsync(connection, transaction, null, 60, ct);
                times.Add(clock.Elapsed.TotalMilliseconds);
            }

            Assert.NotNull(snapshot);
            Assert.True(snapshot.Count > 300, $"The migrated store holds {snapshot.Count} snapshot objects.");
            var median = times.OrderBy(t => t).ElementAt(times.Count / 2);
            TestContext.Current.SendDiagnosticMessage($"rung catalog snapshot: {snapshot.Count} objects, median {median:F1} ms, max {times.Max():F1} ms");
            Assert.True(median < 1000, $"The catalog snapshot took {median:F1} ms.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9486: the snapshot is taken before a rung sets its own lock timeout, so it must never wait behind a lock another session holds on a view. Session A
    /// holds <c>ACCESS EXCLUSIVE</c> on a view and session B takes the snapshot under a short statement timeout. The snapshot finishes with no wait.
    /// </summary>
    [Fact]
    public async Task TheSnapshot_DoesNotWaitBehindAnExclusiveLockOnAView()
    {
        await SnapshotDoesNotWaitAsync(
            "CREATE VIEW collect.probe_locked_view AS SELECT 1 AS one;", "LOCK TABLE collect.probe_locked_view IN ACCESS EXCLUSIVE MODE");
    }

    /// <summary>#9486: the same for a table that has a column default: session A holds <c>ACCESS EXCLUSIVE</c> on it and the snapshot does not wait.</summary>
    [Fact]
    public async Task TheSnapshot_DoesNotWaitBehindAnExclusiveLockOnATableWithAColumnDefault()
    {
        await SnapshotDoesNotWaitAsync(
            "CREATE TABLE collect.probe_locked_table (id integer NOT NULL, note text DEFAULT 'x', stamp timestamptz DEFAULT now());",
            "LOCK TABLE collect.probe_locked_table IN ACCESS EXCLUSIVE MODE");
    }

    private static async Task SnapshotDoesNotWaitAsync(string setupSql, string lockSql)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
            {
                await setup.OpenAsync(ct);
                await using var create = new NpgsqlCommand(setupSql, setup);
                await create.ExecuteNonQueryAsync(ct);
            }

            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await using var holderTransaction = await holder.BeginTransactionAsync(ct);
            await using (var hold = new NpgsqlCommand(lockSql, holder, holderTransaction))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            await using var reader = new NpgsqlConnection(scratch.ConnectionString);
            await reader.OpenAsync(ct);
            await using var transaction = await reader.BeginTransactionAsync(ct);
            await using (var limit = new NpgsqlCommand("SET LOCAL statement_timeout = '4s'", reader, transaction))
            {
                await limit.ExecuteNonQueryAsync(ct);
            }

            var clock = Stopwatch.StartNew();
            var snapshot = await MigrationCatalogSnapshot.TryTakeAsync(reader, transaction, null, 60, ct);
            clock.Stop();

            Assert.NotNull(snapshot);
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(4), $"The snapshot took {clock.Elapsed.TotalMilliseconds:F0} ms behind a held exclusive lock.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9486: a snapshot also must not leave a lock behind, because the rung's transaction holds every lock the snapshot took until the rung ends. After a
    /// snapshot, the backend holds no relation lock on a relation in <c>collect</c> or <c>config</c>.
    /// </summary>
    [Fact]
    public async Task TheSnapshot_HoldsNoRelationLockOnTheStoresOwnTablesOrViews()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            var snapshot = await MigrationCatalogSnapshot.TryTakeAsync(connection, transaction, null, 60, ct);
            Assert.NotNull(snapshot);

            await using var locks = new NpgsqlCommand(
                @"SELECT count(*) FROM pg_locks AS l
                  JOIN pg_class AS c ON c.oid = l.relation
                  JOIN pg_namespace AS n ON n.oid = c.relnamespace
                  WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation' AND n.nspname IN ('collect', 'config')", connection, transaction);
            Assert.Equal(0L, (long)(await locks.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
