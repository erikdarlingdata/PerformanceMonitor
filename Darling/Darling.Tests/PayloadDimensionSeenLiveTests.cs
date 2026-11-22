/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #9304 (V298): the payload dimension tables keep their liveness stamp in <c>collect.{dim}_seen</c>, one narrow row per
/// digest. These run the shipped statements against a migrated store and read what each one wrote:
/// <list type="bullet">
/// <item>a refresh moves the stamp and leaves the wide row's <c>ctid</c> and <c>xmin</c> alone while the wide row's own
/// stamp is inside <see cref="PayloadDimensionSeen.WideRowRefreshHours"/>, and rewrites the wide row past it;</item>
/// <item>the GC goes oldest stamp first, removes the wide row and the stamp row together, and gives no place under its
/// cap to a stamp whose wide row is fresh;</item>
/// <item>a refresh that commits while the GC waits for the wide row keeps the row and its stamp;</item>
/// <item>the rung gives a store with dimension rows the same GC result it had before it, and can run again.</item>
/// </list>
/// What a statement wrote is read from the row's <c>ctid</c> and <c>xmin</c>, and the order of the race is set by
/// <c>pg_stat_activity</c>, never by a timing.
/// </summary>
public sealed class PayloadDimensionSeenLiveTests
{
    private const string Skip =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live payload dimension stamp tests (each mints its own scratch database).";

    private const int RungVersion = 298;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData(PayloadDimensions.QueryTextDimTable)]
    [InlineData(PayloadDimensions.QueryPlanDimTable)]
    public async Task ARefreshPastTheGuard_MovesTheStamp_AndLeavesTheWideRowAlone(string dimTable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        /* Past the stamp guard, inside the wide row's refresh interval. */
        var now = WholeSecondNow();
        var landedAt = now.AddHours(-(PayloadDimensions.LastSeenRefreshGuardHours + 1));
        Assert.True(PayloadDimensions.LastSeenRefreshGuardHours + 1 < PayloadDimensionSeen.WideRowRefreshHours);
        var digest = await InsertAsync(connection, dimTable, "refresh-a", landedAt, ct);

        /* The insert's trigger wrote the stamp row, at the wide row's stamp. */
        Assert.Equal(landedAt, await StampAsync(connection, dimTable, digest, ct));
        var before = await WideRowAsync(connection, dimTable, digest, ct);

        var absent = await TouchAsync(connection, dimTable, new[] { digest }, now, ct);

        Assert.Empty(absent);
        Assert.Equal(now, await StampAsync(connection, dimTable, digest, ct));
        Assert.Equal(before, await WideRowAsync(connection, dimTable, digest, ct));
    }

    [Theory]
    [InlineData(PayloadDimensions.QueryTextDimTable)]
    [InlineData(PayloadDimensions.QueryPlanDimTable)]
    public async Task ARefreshPastTheWideRowInterval_RewritesTheWideRowToo_AndAMissingStampGetsOne(string dimTable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var now = WholeSecondNow();
        var pastTheInterval = now.AddHours(-(PayloadDimensionSeen.WideRowRefreshHours + 1));
        var old = await InsertAsync(connection, dimTable, "interval-a", pastTheInterval, ct);
        var unstamped = await InsertAsync(connection, dimTable, "interval-b", now.AddHours(-1), ct);
        await ExecuteAsync(connection, $"DELETE FROM collect.{PayloadDimensionSeen.SeenName(dimTable)} WHERE digest = $1", ct, Bytea(unstamped));
        var oldBefore = await WideRowAsync(connection, dimTable, old, ct);
        var unstampedBefore = await WideRowAsync(connection, dimTable, unstamped, ct);

        var absent = await TouchAsync(connection, dimTable, new[] { old, unstamped, PayloadDimensions.Digest("never-stored") }, now, ct);

        Assert.Equal(new[] { Hex(PayloadDimensions.Digest("never-stored")) }, absent.Select(Hex).ToArray());

        /* The old digest: stamp and wide row both restamped, the wide row as a new version. */
        Assert.Equal(now, await StampAsync(connection, dimTable, old, ct));
        var oldAfter = await WideRowAsync(connection, dimTable, old, ct);
        Assert.NotEqual(oldBefore.Ctid, oldAfter.Ctid);
        Assert.Equal(now, oldAfter.LastSeen);

        /* The digest with no stamp row: it has one now, and its wide row (an hour old) was not written. */
        Assert.Equal(now, await StampAsync(connection, dimTable, unstamped, ct));
        Assert.Equal(unstampedBefore, await WideRowAsync(connection, dimTable, unstamped, ct));
    }

    [Theory]
    [InlineData(PayloadDimensions.QueryTextDimTable)]
    [InlineData(PayloadDimensions.QueryPlanDimTable)]
    public async Task TheGc_GoesOldestStampFirst_RemovesBothRows_AndSkipsAStampWhoseWideRowIsFresh(string dimTable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        var now = WholeSecondNow();
        var cutoff = now.AddHours(-PayloadDimensionSeen.OlderBuildCutoffHours);

        /* Four rows behind the cutoff, one hour apart, one row inside it, and one whose stamp is behind the cutoff while its
           wide row is fresh: the oldest stamp of all, which a store back from an older build can hold. */
        await InsertAsync(connection, dimTable, "gc-1", cutoff.AddHours(-5), ct);
        await InsertAsync(connection, dimTable, "gc-2", cutoff.AddHours(-4), ct);
        var third = await InsertAsync(connection, dimTable, "gc-3", cutoff.AddHours(-3), ct);
        var fourth = await InsertAsync(connection, dimTable, "gc-4", cutoff.AddHours(-2), ct);
        var live = await InsertAsync(connection, dimTable, "gc-live", now.AddHours(-1), ct);
        var freshWide = await InsertAsync(connection, dimTable, "gc-fresh-wide", now.AddHours(-1), ct);
        await ExecuteAsync(
            connection,
            $"UPDATE collect.{PayloadDimensionSeen.SeenName(dimTable)} SET last_seen = $1 WHERE digest = $2",
            ct, cutoff.AddHours(-9), Bytea(freshWide));

        /* A cap of two: the first batch retires the two oldest stamps that have an old wide row, and the fresh-wide stamp,
           older than both, takes no place under the cap. */
        var first = await GcAsync(connection, dimTable, cutoff, cap: 2, cursor: null, ct);
        Assert.Equal(new[] { cutoff.AddHours(-5), cutoff.AddHours(-4) }, first);
        Assert.Equal(new[] { third, fourth, live, freshWide }.Select(Hex).Order(), (await DigestsAsync(connection, dimTable, ct)).Select(Hex).Order());

        /* The next batch starts at the first one's newest stamp, as the retention pass's cursor does. */
        var next = await GcAsync(connection, dimTable, cutoff, cap: 2, cursor: first[^1], ct);
        Assert.Equal(new[] { cutoff.AddHours(-3), cutoff.AddHours(-2) }, next);

        var left = (await DigestsAsync(connection, dimTable, ct)).Select(Hex).Order().ToArray();
        Assert.Equal(new[] { live, freshWide }.Select(Hex).Order(), left);
        Assert.Equal(
            left,
            (await StampDigestsAsync(connection, dimTable, ct)).Select(Hex).Order().ToArray());
    }

    [Theory]
    [InlineData(PayloadDimensions.QueryTextDimTable)]
    [InlineData(PayloadDimensions.QueryPlanDimTable)]
    public async Task ARefreshThatCommitsWhileTheGcWaitsForTheWideRow_KeepsTheRowAndItsStamp(string dimTable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var writer = await OpenAsync(scratch, ct);
        await using var gc = await OpenAsync(scratch, ct);
        await using var watcher = await OpenAsync(scratch, ct);

        /* The smallest cutoff the GC is given, and both stamps one hour behind it: the youngest digest the GC may retire,
           which is the one nearest the wide row's refresh interval. */
        var now = WholeSecondNow();
        var cutoff = now.AddHours(-PayloadDimensionSeen.OlderBuildCutoffHours);
        var behindTheCutoff = cutoff.AddHours(-1);
        var revived = await InsertAsync(writer, dimTable, "race-1", behindTheCutoff, ct);
        await InsertAsync(writer, dimTable, "race-2", behindTheCutoff, ct);

        /* A GC that still waits after the refresh has committed fails with an error instead of holding the test host. */
        await ExecuteAsync(gc, "SET lock_timeout = '120s'", ct);

        bool waited;
        IReadOnlyList<DateTime> retired;
        await using (var transaction = await writer.BeginTransactionAsync(ct))
        {
            var absent = await TouchAsync(writer, dimTable, new[] { revived }, now, ct, transaction);
            Assert.Empty(absent);

            var batch = GcAsync(gc, dimTable, cutoff, cap: 1000, cursor: null, ct);
            waited = await WaitsOnALockHeldByAsync(watcher, gc.ProcessID, writer.ProcessID, batch, ct);
            await transaction.CommitAsync(ct);
            retired = await batch;
        }

        Assert.True(waited, "the GC finished, or ran for a minute, without waiting on a lock the refresh's transaction held");

        /* The expired digest only. The revived one carries the refresh's stamp on both rows. */
        Assert.Equal(new[] { behindTheCutoff }, retired);
        Assert.Equal(new[] { Hex(revived) }, (await DigestsAsync(writer, dimTable, ct)).Select(Hex).ToArray());
        Assert.Equal(new[] { Hex(revived) }, (await StampDigestsAsync(writer, dimTable, ct)).Select(Hex).ToArray());
        Assert.Equal(now, await StampAsync(writer, dimTable, revived, ct));
        Assert.Equal(now, (await WideRowAsync(writer, dimTable, revived, ct)).LastSeen);
    }

    [Fact]
    public async Task TheRung_FillsBothStampTables_ArmsTheTriggers_DropsTheWideIndexes_AndGivesTheSameGcResult()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);

        /* A store one rung behind: no stamp tables, the dimension's own last_seen indexes back. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        await ExecuteAsync(connection, "DELETE FROM darling_schema_version WHERE version >= $1", ct, RungVersion);
        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            Assert.False(await TableExistsAsync(connection, PayloadDimensionSeen.TableName(dimTable), ct));
            Assert.True(await IndexExistsAsync(connection, PayloadDimensionSeen.WideIndexName(dimTable), ct));
        }

        /* Dimension rows with a different stamp on every row: the first three behind the cutoff, the last two inside it. */
        var now = WholeSecondNow();
        var cutoff = now.AddHours(-PayloadDimensionSeen.OlderBuildCutoffHours);
        var expectedKept = new Dictionary<string, List<string>>();
        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            expectedKept[dimTable] = new List<string>();
            for (var i = 0; i < 5; i++)
            {
                var stamp = i < 3 ? cutoff.AddHours(-(i + 1)) : now.AddHours(-(i + 1));
                var digest = await InsertAsync(connection, dimTable, $"rung-{i}", stamp, ct);
                if (i >= 3)
                {
                    expectedKept[dimTable].Add(Hex(digest));
                }
            }
        }

        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));
        foreach (var dimTable in PayloadDimensions.DimTables)
        {
            var seen = PayloadDimensionSeen.TableName(dimTable);
            Assert.Equal(5L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {seen}", ct));
            Assert.Equal(
                5L,
                await ScalarAsync<long>(connection, $"SELECT count(*) FROM {seen} s JOIN collect.{dimTable} d ON d.digest = s.digest AND d.last_seen = s.last_seen", ct));
            Assert.False(await IndexExistsAsync(connection, PayloadDimensionSeen.WideIndexName(dimTable), ct));
            Assert.True(await IndexExistsAsync(connection, PayloadDimensionSeen.LastSeenIndexName(dimTable), ct));
            Assert.Equal(
                1L,
                await ScalarAsync<long>(
                    connection,
                    $"SELECT count(*) FROM pg_trigger WHERE tgrelid = 'collect.{dimTable}'::regclass AND tgname = '{PayloadDimensionSeen.InsertTriggerName(dimTable)}' AND tgenabled = 'O'",
                    ct));

            /* The trigger is armed: a dimension row inserted with no thought for the stamp table gets its stamp row. */
            var added = await InsertAsync(connection, dimTable, "rung-armed", now.AddHours(-2), ct);
            Assert.Equal(now.AddHours(-2), await StampAsync(connection, dimTable, added, ct));
            await ExecuteAsync(connection, $"DELETE FROM collect.{dimTable} WHERE digest = $1", ct, Bytea(added));

            /* The GC retires exactly the rows the dimension's own stamp put behind the cutoff, as the old statement did. */
            var gone = await GcAsync(connection, dimTable, cutoff, cap: 100, cursor: null, ct);
            Assert.Equal(3, gone.Count);
            Assert.Equal(
                expectedKept[dimTable].Order(),
                (await DigestsAsync(connection, dimTable, ct)).Select(Hex).Order().ToArray());
            Assert.Equal(
                expectedKept[dimTable].Order(),
                (await StampDigestsAsync(connection, dimTable, ct)).Select(Hex).Order().ToArray());
        }

        /* The rung again, on a store that has the tables, with one dimension row that has no stamp row: it fills that row
           and leaves the rest as they are. */
        var text = PayloadDimensions.QueryTextDimTable;
        var bare = await InsertAsync(connection, text, "rung-bare", now.AddHours(-3), ct);
        await ExecuteAsync(connection, $"DELETE FROM collect.{PayloadDimensionSeen.SeenName(text)} WHERE digest = $1", ct, Bytea(bare));
        var keptStamp = Bytea(PayloadDimensions.Digest("rung-3"));
        await ExecuteAsync(connection, $"UPDATE collect.{PayloadDimensionSeen.SeenName(text)} SET last_seen = $1 WHERE digest = $2", ct, now.AddHours(1), keptStamp);
        await ExecuteAsync(connection, "DELETE FROM darling_schema_version WHERE version >= $1", ct, RungVersion);

        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(now.AddHours(-3), await StampAsync(connection, text, bare, ct));
        Assert.Equal(now.AddHours(1), await StampAsync(connection, text, PayloadDimensions.Digest("rung-3"), ct));
        Assert.Equal(3L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {PayloadDimensionSeen.TableName(text)}", ct));
    }

    // ---- helpers ----

    private static DateTime WholeSecondNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
    }

    private static string Hex(byte[] digest) => Convert.ToHexString(digest);

    private static NpgsqlParameter Bytea(byte[] value) => new() { NpgsqlDbType = NpgsqlDbType.Bytea, Value = value };

    private static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>Inserts one dimension row with the given stamp the way an insert with no thought for the stamp table does, so
    /// the trigger gives it its stamp row. Returns its digest.</summary>
    private static async Task<byte[]> InsertAsync(NpgsqlConnection connection, string dimTable, string payload, DateTime lastSeen, CancellationToken ct)
    {
        var digest = PayloadDimensions.Digest(payload);
        await ExecuteAsync(
            connection,
            $"INSERT INTO collect.{dimTable} (digest, {PayloadDimensions.PayloadColumnOf(dimTable)}, last_seen) VALUES ($1, $2, $3)",
            ct, Bytea(digest), payload, lastSeen);
        return digest;
    }

    /// <summary>One run of the shipped refresh statement. Returns the digests it reports as having no dimension row.</summary>
    private static async Task<IReadOnlyList<byte[]>> TouchAsync(
        NpgsqlConnection connection, string dimTable, byte[][] digests, DateTime now, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(PayloadDimensions.TouchSql(dimTable), connection, transaction);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea, Value = digests });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now });
        var absent = new List<byte[]>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            absent.Add((byte[])reader.GetValue(0));
        }

        return absent;
    }

    /// <summary>One statement of the dimension GC as the retention pass runs it. Returns the stamps of the rows it deleted, oldest first.</summary>
    private static async Task<IReadOnlyList<DateTime>> GcAsync(
        NpgsqlConnection connection, string dimTable, DateTime cutoff, int cap, DateTime? cursor, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PayloadDimensions.CursoredDeleteSql(dimTable, cap, cursor is not null), connection) { CommandTimeout = 180 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = cutoff });
        if (cursor is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = cursor.Value });
        }

        var stamps = new List<DateTime>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            stamps.Add(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Unspecified));
        }

        stamps.Sort();
        return stamps;
    }

    private static async Task<DateTime?> StampAsync(NpgsqlConnection connection, string dimTable, byte[] digest, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT last_seen FROM {PayloadDimensionSeen.TableName(dimTable)} WHERE digest = $1", connection);
        command.Parameters.Add(Bytea(digest));
        var value = await command.ExecuteScalarAsync(ct);
        return value is DateTime stamp ? DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified) : null;
    }

    private sealed record WideRow(string Ctid, string Xmin, DateTime LastSeen);

    private static async Task<WideRow> WideRowAsync(NpgsqlConnection connection, string dimTable, byte[] digest, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT ctid::text, xmin::text, last_seen FROM collect.{dimTable} WHERE digest = $1", connection);
        command.Parameters.Add(Bytea(digest));
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the dimension row is missing");
        return new WideRow(reader.GetString(0), reader.GetString(1), DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Unspecified));
    }

    private static Task<List<byte[]>> DigestsAsync(NpgsqlConnection connection, string dimTable, CancellationToken ct) =>
        DigestListAsync(connection, $"SELECT digest FROM collect.{dimTable}", ct);

    private static Task<List<byte[]>> StampDigestsAsync(NpgsqlConnection connection, string dimTable, CancellationToken ct) =>
        DigestListAsync(connection, $"SELECT digest FROM {PayloadDimensionSeen.TableName(dimTable)}", ct);

    private static async Task<List<byte[]>> DigestListAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var digests = new List<byte[]>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            digests.Add((byte[])reader.GetValue(0));
        }

        return digests;
    }

    private static async Task<bool> TableExistsAsync(NpgsqlConnection connection, string qualifiedName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT to_regclass('{qualifiedName}') IS NOT NULL", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT to_regclass('collect.{indexName}') IS NOT NULL", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            if (parameter is NpgsqlParameter typed)
            {
                command.Parameters.Add(typed);
            }
            else if (parameter is DateTime stamp)
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
            }
            else
            {
                command.Parameters.AddWithValue(parameter);
            }
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (T)value;
    }

    /// <summary>
    /// Polls <c>pg_stat_activity</c> until the backend <paramref name="waiterPid"/> waits on a lock that the backend
    /// <paramref name="holderPid"/> holds. False when <paramref name="statement"/> finishes first or about a minute passes:
    /// either way the statement did not wait on that backend, and the caller fails on it.
    /// </summary>
    private static async Task<bool> WaitsOnALockHeldByAsync(
        NpgsqlConnection watcher, int waiterPid, int holderPid, Task statement, CancellationToken ct)
    {
        for (var i = 0; i < 600 && !statement.IsCompleted; i++)
        {
            await using var command = new NpgsqlCommand(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock' AND $2 = ANY (pg_blocking_pids(pid))",
                watcher);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = waiterPid });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = holderPid });
            if ((long)(await command.ExecuteScalarAsync(ct))! > 0)
            {
                return true;
            }

            await Task.Delay(100, ct);
        }

        return false;
    }
}
