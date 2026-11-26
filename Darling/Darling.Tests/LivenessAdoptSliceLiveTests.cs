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
/// the cursor;</item>
/// <item>a cursor kept in the store carries over to a new state, as after a restart, and one that does not fit starts the
/// table at its first key;</item>
/// <item>the Query Store text's three-column key is covered slice by slice across servers and databases, with its cursor
/// carried from state to state through the store.</item>
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

    [Fact]
    public async Task ACursorKeptInTheStore_CarriesOverToANewState_AndOneThatDoesNotFitStartsAtTheFirstKey()
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
        await using var postgres = DataSource(scratch);

        /* Five keys a slice and one slice a pass, each state as a service that has just started. */
        LivenessAdoptState NewState() => new(
            sliceKeys: 5, sliceKeysFloor: 1, budget: TimeSpan.FromSeconds(1), sliceTimeoutSeconds: 30, secondsClock: TickingClock(),
            keepsCursorInStore: true);

        /* The first pass covers keys one to five and writes its cursor to the store. */
        var first = NewState();
        await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, logger: null, ct, retentionDaysFor: _ => 30, livenessAdopt: first);
        Assert.Equal(Hex(keys[4]), Hex((byte[])Assert.Single(first.CursorOf(seenTable)!)));
        Assert.Equal(LivenessAdoptState.EncodeCursor(new object[] { keys[4] }), await StoredCursorAsync(connection, seenTable, ct));

        /* The second key, behind the cursor, and the eighth, ahead of it, lose their stamp rows. A new state reads the cursor
           back and covers keys six to ten: the row ahead gets its stamp row, the one behind does not. */
        var behind = keys[1];
        var ahead = keys[7];
        await ExecuteAsync(connection, $"DELETE FROM {seenTable} WHERE digest = $1 OR digest = $2", ct, Bytea(behind), Bytea(ahead));
        var second = NewState();
        await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, logger: null, ct, retentionDaysFor: _ => 30, livenessAdopt: second);
        Assert.Equal(new[] { Hex(behind) }, (await MissingAsync(connection, dimTable, ct)).Select(Hex).ToArray());
        Assert.Equal(Hex(keys[9]), Hex((byte[])Assert.Single(second.CursorOf(seenTable)!)));
        Assert.Equal(LivenessAdoptState.EncodeCursor(new object[] { keys[9] }), await StoredCursorAsync(connection, seenTable, ct));

        /* A stored cursor that is not a cursor starts the table at its first key and fails nothing: the row behind is adopted. */
        await ExecuteAsync(
            connection,
            "UPDATE collect.collector_state SET state_value = 'not a cursor' WHERE server_id = 0 AND collector_name = $1 AND state_key = $2",
            ct, LivenessAdoptState.CursorStateCollectorName, seenTable);
        var third = NewState();
        var log = new CapturingTestLogger();
        await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct, retentionDaysFor: _ => 30, livenessAdopt: third);
        Assert.True(0 == (await MissingAsync(connection, dimTable, ct)).Count, log.Joined);
        Assert.Equal(Hex(keys[4]), Hex((byte[])Assert.Single(third.CursorOf(seenTable)!)));
        Assert.True("SUCCESS" == await LastRunStatusAsync(connection, ct), log.Joined);
        Assert.DoesNotContain(log.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains(seenTable, StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheQueryStoreTextKey_IsCoveredSliceBySliceAcrossServersAndDatabases_WithItsCursorCarriedThroughTheStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        const string seenTable = QueryStoreTextSeen.TableName;
        var lastSeen = WholeSecondNow().AddHours(-1);

        /* Thirteen texts over two servers and two databases, with query ids past the int range. 'alpha' and 'Beta' sort one
           way by byte and the other way under most linguistic collations: the slice's order and its range both use the
           column's own collation, so the keys below are read in that order and the test holds under either. The trigger
           stamps every text, and then every text but the query id 2 ones loses its seen row. */
        await ExecuteAsync(
            connection,
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) "
            + "SELECT s, d, q, 'SELECT ' || q, $1 FROM unnest(ARRAY[3, 11]) AS s, unnest(ARRAY['alpha', 'Beta']) AS d, "
            + "unnest(ARRAY[1, 2, 3000000001]::bigint[]) AS q "
            + "UNION ALL SELECT 11, 'alpha', 4000000002, 'SELECT 4000000002', $1",
            ct, lastSeen);
        await ExecuteAsync(connection, $"DELETE FROM {seenTable} WHERE query_id <> 2", ct);

        var keys = await TextKeysAsync(connection, "SELECT t.server_id, t.database_name, t.query_id FROM collect.query_store_text AS t", ct);
        var missingAtStart = await TextKeysAsync(connection, MissingTextsSql, ct);
        Assert.Equal(13, keys.Count);
        Assert.Equal(9, missingAtStart.Count);
        await using var postgres = DataSource(scratch);

        /* Two keys a slice and three slices a pass, six keys a pass, each pass by a new state as a service that has just
           started: the first pass covers keys one to six, the second seven to twelve, and the third reaches the end. */
        for (var pass = 1; pass <= 3; pass++)
        {
            var state = new LivenessAdoptState(
                sliceKeys: 2, sliceKeysFloor: 1, budget: TimeSpan.FromSeconds(3), sliceTimeoutSeconds: 30, secondsClock: TickingClock(),
                keepsCursorInStore: true);
            var log = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, log, ct, retentionDaysFor: _ => 30, livenessAdopt: state);

            var covered = keys.Take(pass * 6).ToHashSet(StringComparer.Ordinal);
            var expected = missingAtStart.Where(key => !covered.Contains(key)).ToArray();
            var missing = (await TextKeysAsync(connection, MissingTextsSql, ct)).ToArray();
            Assert.True(expected.SequenceEqual(missing), $"pass {pass}: missing [{string.Join(", ", missing)}], expected [{string.Join(", ", expected)}]\n{log.Joined}");
            Assert.True("SUCCESS" == await LastRunStatusAsync(connection, ct), log.Joined);
            Assert.DoesNotContain(log.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains(seenTable, StringComparison.Ordinal));

            /* The cursor is the last key the pass covered, as an int, a text and a bigint in key order, and the store holds the
               same; the pass that reached the end holds none. */
            var cursor = state.CursorOf(seenTable);
            if (pass < 3)
            {
                Assert.NotNull(cursor);
                Assert.Equal(3, cursor.Count);
                Assert.IsType<int>(cursor[0]);
                Assert.IsType<string>(cursor[1]);
                Assert.IsType<long>(cursor[2]);
                Assert.Equal(keys[(pass * 6) - 1], TextKey((int)cursor[0], (string)cursor[1], (long)cursor[2]));
            }
            else
            {
                Assert.Null(cursor);
            }

            Assert.Equal(LivenessAdoptState.EncodeCursor(cursor), await StoredCursorAsync(connection, seenTable, ct));
        }

        /* Every text has its seen row, stamped with the text row's own stamp, and no text was retired. */
        Assert.Equal(13L, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_text", ct));
        Assert.Equal(
            13L,
            await CountAsync(
                connection,
                $"SELECT count(*) FROM collect.query_store_text AS t JOIN {seenTable} AS s "
                + "ON s.server_id = t.server_id AND s.database_name = t.database_name AND s.query_id = t.query_id AND s.last_seen = t.last_seen",
                ct));
    }

    [Fact]
    public void TheCursorTextForm_RoundTripsEveryKeyType_AndReadsWhatDoesNotFitAsNoCursor()
    {
        var digest = new byte[] { 0x00, 0x7F, 0x80, 0xFF };
        var digestText = LivenessAdoptState.EncodeCursor(new object[] { digest });
        Assert.Equal(Convert.ToHexString(digest), Convert.ToHexString((byte[])Assert.Single(LivenessAdoptState.DecodeCursor(digestText, 1)!)));

        /* The Query Store text key: an integer, a text with the form's own separator in it, and a bigint. */
        var textKey = LivenessAdoptState.DecodeCursor(LivenessAdoptState.EncodeCursor(new object[] { -7, "db:\"x\" \u00e9", long.MaxValue }), 3)!;
        Assert.Equal(new object[] { -7, "db:\"x\" \u00e9", long.MaxValue }, textKey);

        /* No cursor both ways. */
        Assert.Equal(string.Empty, LivenessAdoptState.EncodeCursor(null));
        Assert.Null(LivenessAdoptState.DecodeCursor(string.Empty, 1));
        Assert.Null(LivenessAdoptState.DecodeCursor(null, 1));

        /* A value type the form does not hold is not written. */
        Assert.Null(LivenessAdoptState.EncodeCursor(new object[] { Guid.Empty }));

        /* Text that is not the form, or a cursor of another key's width, is no cursor. */
        Assert.Null(LivenessAdoptState.DecodeCursor("not a cursor", 1));
        Assert.Null(LivenessAdoptState.DecodeCursor(digestText, 3));
        Assert.Null(LivenessAdoptState.DecodeCursor("[\"q:1\"]", 1));
        Assert.Null(LivenessAdoptState.DecodeCursor("[\"x:zz\"]", 1));
        Assert.Null(LivenessAdoptState.DecodeCursor("[\"i:12x\"]", 1));
        Assert.Null(LivenessAdoptState.DecodeCursor("[null]", 1));
    }

    /// <summary>The texts with no seen row. <see cref="TextKeysAsync"/> puts them in key order.</summary>
    private const string MissingTextsSql =
        "SELECT t.server_id, t.database_name, t.query_id FROM collect.query_store_text AS t "
        + "WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_text_seen AS s "
        + "WHERE s.server_id = t.server_id AND s.database_name = t.database_name AND s.query_id = t.query_id)";

    /// <summary>
    /// The text keys <paramref name="sql"/> selects (server id, database name, query id, aliased <c>t</c>), in the primary
    /// key's order under the column's own collation, which is the order the slices take.
    /// </summary>
    private static async Task<List<string>> TextKeysAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql + " ORDER BY t.server_id, t.database_name, t.query_id", connection);
        var keys = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add(TextKey(reader.GetInt32(0), reader.GetString(1), reader.GetInt64(2)));
        }

        return keys;
    }

    private static string TextKey(int serverId, string databaseName, long queryId) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{serverId}/{databaseName}/{queryId}");

    /// <summary>The adopt cursor the store holds for <paramref name="livenessTable"/>, or null when it holds none.</summary>
    private static async Task<string?> StoredCursorAsync(NpgsqlConnection connection, string livenessTable, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT state_value FROM collect.collector_state WHERE server_id = 0 AND collector_name = $1 AND state_key = $2", connection);
        command.Parameters.AddWithValue(LivenessAdoptState.CursorStateCollectorName);
        command.Parameters.AddWithValue(livenessTable);
        return await command.ExecuteScalarAsync(ct) as string;
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
