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
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #9668: the retention pass's adopt step gives a row with no liveness row one in keyset slices of the key, under a time
/// budget per table, with its cursor carried from pass to pass in a <see cref="LivenessAdoptState"/>. These run whole
/// retention passes against a migrated store and read the stamp rows each pass left:
/// <list type="bullet">
/// <item>a dimension larger than one slice gets every missing stamp row over several passes, each pass stopping at its
/// budget and the next going on from that key;</item>
/// <item>a slice that runs into its command timeout halves the next slice and fails no table;</item>
/// <item>the cursor goes back to the first key after the end, and a later pass adopts a row that lost its stamp row behind
/// the cursor.</item>
/// </list>
/// The clock is the test's: it advances one second each time it is read, and a slice reads it twice, so every slice takes
/// one second and a budget is a count of slices. The timeout is forced by a lock the test holds and releases when the
/// state shows the slice shrunk, never by a timing.
/// </summary>
public sealed class LivenessAdoptSliceLiveTests
{
    private const string Skip =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live liveness adopt tests (each mints its own scratch database).";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData(PayloadDimensions.QueryTextDimTable)]
    [InlineData(PayloadDimensions.QueryPlanDimTable)]
    public async Task ADimensionLargerThanOneSlice_GetsEveryMissingStamp_OverSeveralPasses_EachStoppingAtItsBudget(string dimTable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        var seenTable = PayloadDimensionSeen.TableName(dimTable);
        var lastSeen = WholeSecondNow().AddHours(-1);

        /* Twenty rows the trigger stamps, every third of which then loses its stamp row, and three rows written with the
           trigger off, which never had one. */
        await InsertRowsAsync(connection, dimTable, first: 1, last: 20, lastSeen, ct);
        await ExecuteAsync(
            connection,
            $"DELETE FROM {seenTable} WHERE digest IN (SELECT sha256(('adopt-' || gs)::bytea) FROM generate_series(1, 20, 3) AS gs)",
            ct);
        await ExecuteAsync(connection, "SET session_replication_role = replica", ct);
        await InsertRowsAsync(connection, dimTable, first: 21, last: 23, lastSeen, ct);
        await ExecuteAsync(connection, "SET session_replication_role = DEFAULT", ct);

        var keys = await DigestsInKeyOrderAsync(connection, $"SELECT digest FROM collect.{dimTable} ORDER BY digest", ct);
        var missingAtStart = await MissingAsync(connection, dimTable, ct);
        Assert.Equal(23, keys.Count);
        Assert.Equal(10, missingAtStart.Count);

        /* Five keys a slice and two slices a pass: ten keys a pass, so three passes for twenty-three keys. */
        var state = new LivenessAdoptState(
            sliceKeys: 5, sliceKeysFloor: 1, budget: TimeSpan.FromSeconds(2), sliceTimeoutSeconds: 30, secondsClock: TickingClock());
        await using var postgres = DataSource(scratch);

        for (var pass = 1; pass <= 3; pass++)
        {
            var log = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, log, ct, retentionDaysFor: _ => 30, livenessAdopt: state);

            /* The keys this pass and the ones before it covered have their stamp rows, and no key past them has one yet. */
            var covered = keys.Take(pass * 10).Select(Hex).ToHashSet(StringComparer.Ordinal);
            var expected = missingAtStart.Select(Hex).Where(digest => !covered.Contains(digest)).ToArray();
            var missing = (await MissingAsync(connection, dimTable, ct)).Select(Hex).ToArray();
            Assert.True(expected.SequenceEqual(missing), $"pass {pass}: {missing.Length} row(s) without a stamp row, expected {expected.Length}\n{log.Joined}");

            /* A pass that stops on its budget is no failure and no warning, and it keeps its place. */
            Assert.True("SUCCESS" == await LastRunStatusAsync(connection, ct), log.Joined);
            Assert.DoesNotContain(log.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains(seenTable, StringComparison.Ordinal));
            Assert.Contains(log.Lines, line => line.StartsWith("Information:", StringComparison.Ordinal) && line.Contains(seenTable, StringComparison.Ordinal));
            Assert.Equal(pass < 3, state.CursorOf(seenTable) is not null);
        }

        /* Every row has its stamp row, stamped with the dimension row's own stamp, and no dimension row was retired. */
        Assert.Equal(23L, await CountAsync(connection, $"SELECT count(*) FROM collect.{dimTable}", ct));
        Assert.Equal(
            23L,
            await CountAsync(connection, $"SELECT count(*) FROM collect.{dimTable} AS d JOIN {seenTable} AS s ON s.digest = d.digest AND s.last_seen = d.last_seen", ct));
    }

    [Fact]
    public async Task ASliceThatTimesOut_HalvesTheNextSlice_AndFailsNoTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        const string dimTable = PayloadDimensions.QueryPlanDimTable;
        var seenTable = PayloadDimensionSeen.TableName(dimTable);

        await InsertRowsAsync(connection, dimTable, first: 1, last: 8, WholeSecondNow().AddHours(-1), ct);
        await ExecuteAsync(
            connection,
            $"DELETE FROM {seenTable} WHERE digest IN (SELECT sha256(('adopt-' || gs)::bytea) FROM generate_series(1, 8, 4) AS gs)",
            ct);
        Assert.Equal(2, (await MissingAsync(connection, dimTable, ct)).Count);

        /* The stamp table is locked against every statement, so the first slice's insert waits until its command timeout.
           The clock is read at the start of each slice: once the state shows the slice shrunk, the lock goes, and the
           slice after the timeout runs. */
        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        var held = await holder.BeginTransactionAsync(ct);
        await using (var lockCommand = new NpgsqlCommand($"LOCK TABLE {seenTable} IN ACCESS EXCLUSIVE MODE", holder, held))
        {
            await lockCommand.ExecuteNonQueryAsync(ct);
        }

        LivenessAdoptState? state = null;
        var released = false;
        var seconds = 0.0;
        double Clock()
        {
            if (!released && state!.SliceKeysOf(seenTable) < 8)
            {
                held.Rollback();
                released = true;
            }

            return seconds++;
        }

        state = new LivenessAdoptState(
            sliceKeys: 8, sliceKeysFloor: 2, budget: TimeSpan.FromSeconds(100), sliceTimeoutSeconds: 2, secondsClock: Clock);
        await using var postgres = DataSource(scratch);
        var log = new CapturingTestLogger();

        await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, log, ct, retentionDaysFor: _ => 30, livenessAdopt: state);

        /* The timeout shrank the slice, the pass went on at that size in the same pass, and every row has its stamp row. */
        Assert.True(released, log.Joined);
        Assert.Equal(4, state.SliceKeysOf(seenTable));
        Assert.True(0 == (await MissingAsync(connection, dimTable, ct)).Count, log.Joined);

        /* It is no failed table: the run record is a success and nothing about the stamp table is a warning. */
        Assert.True("SUCCESS" == await LastRunStatusAsync(connection, ct), log.Joined);
        Assert.DoesNotContain(log.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains(seenTable, StringComparison.Ordinal));
        Assert.Contains(
            log.Lines,
            line => line.StartsWith("Information:", StringComparison.Ordinal)
                && line.Contains(seenTable, StringComparison.Ordinal)
                && line.Contains("1 slice(s) timed out", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheCursorGoesBackToTheFirstKeyAfterTheEnd_AndALaterPassAdoptsARowThatLostItsStampBehindIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        const string dimTable = PayloadDimensions.QueryPlanDimTable;
        var seenTable = PayloadDimensionSeen.TableName(dimTable);

        await InsertRowsAsync(connection, dimTable, first: 1, last: 12, WholeSecondNow().AddHours(-1), ct);
        var keys = await DigestsInKeyOrderAsync(connection, $"SELECT digest FROM collect.{dimTable} ORDER BY digest", ct);
        Assert.Equal(12, keys.Count);

        /* Five keys a slice and one slice a pass. */
        var state = new LivenessAdoptState(
            sliceKeys: 5, sliceKeysFloor: 1, budget: TimeSpan.FromSeconds(1), sliceTimeoutSeconds: 30, secondsClock: TickingClock());
        await using var postgres = DataSource(scratch);

        async Task PassAsync() =>
            await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, logger: null, ct, retentionDaysFor: _ => 30, livenessAdopt: state);

        /* The first pass covers keys one to five. Then the second key, behind the cursor, and the eighth, ahead of it, lose
           their stamp rows. */
        await PassAsync();
        Assert.Equal(Hex(keys[4]), Hex((byte[])Assert.Single(state.CursorOf(seenTable)!)));
        var behind = keys[1];
        var ahead = keys[7];
        await ExecuteAsync(connection, $"DELETE FROM {seenTable} WHERE digest = $1 OR digest = $2", ct, Bytea(behind), Bytea(ahead));

        /* The second pass covers keys six to ten: the row ahead of the cursor gets its stamp row, the one behind does not. */
        await PassAsync();
        Assert.Equal(new[] { Hex(behind) }, (await MissingAsync(connection, dimTable, ct)).Select(Hex).ToArray());

        /* The third pass reaches the end of the keys and goes back to the first key. */
        await PassAsync();
        Assert.Null(state.CursorOf(seenTable));
        Assert.Equal(new[] { Hex(behind) }, (await MissingAsync(connection, dimTable, ct)).Select(Hex).ToArray());

        /* The fourth pass covers keys one to five again, and adopts the row. */
        await PassAsync();
        Assert.Empty(await MissingAsync(connection, dimTable, ct));
    }

    /// <summary>A clock that reads one second later each time. A slice reads it at its start and at its end, so it takes one second.</summary>
    private static Func<double> TickingClock()
    {
        var seconds = 0.0;
        return () => seconds++;
    }

    private static NpgsqlDataSource DataSource(ScratchPostgres scratch) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);

    /// <summary>Inserts dimension rows <paramref name="first"/> to <paramref name="last"/> in one statement, each with the digest of its own name.</summary>
    private static Task InsertRowsAsync(NpgsqlConnection connection, string dimTable, int first, int last, DateTime lastSeen, CancellationToken ct) =>
        ExecuteAsync(
            connection,
            $"INSERT INTO collect.{dimTable} (digest, {PayloadDimensions.PayloadColumnOf(dimTable)}, last_seen) "
            + $"SELECT sha256(('adopt-' || gs)::bytea), 'adopt-' || gs, $1 FROM generate_series({first}, {last}) AS gs",
            ct, lastSeen);

    /// <summary>The digests of the dimension rows that have no stamp row, in key order.</summary>
    private static Task<List<byte[]>> MissingAsync(NpgsqlConnection connection, string dimTable, CancellationToken ct) =>
        DigestsInKeyOrderAsync(
            connection,
            $"SELECT d.digest FROM collect.{dimTable} AS d WHERE NOT EXISTS (SELECT 1 FROM {PayloadDimensionSeen.TableName(dimTable)} AS s WHERE s.digest = d.digest) ORDER BY d.digest",
            ct);

    private static async Task<List<byte[]>> DigestsInKeyOrderAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
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

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The status of the newest run record a retention pass wrote.</summary>
    private static async Task<string> LastRunStatusAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT status FROM collect.collection_log WHERE collector_name = 'data_retention' ORDER BY collection_time DESC LIMIT 1",
            connection);
        var status = await command.ExecuteScalarAsync(ct);
        Assert.True(status is string, "the retention pass wrote no run record");
        return (string)status!;
    }

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
}
