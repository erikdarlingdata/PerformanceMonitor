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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9492: the parameter-sensitivity fact and its drill-down take each plan's newest row by an aggregate (the largest
/// array whose leading elements are the row's time and id), not by numbering every row of the window. Both reads are run
/// on a live store over the same seeded rows, and each case below can only pass if the row the read keeps is the right
/// ONE row: the newest by time, the larger collection_id when two share that time, with every figure from that same row.
///
/// <para>The cases, by plan key: two rows at the newest time (the larger id wins, with its own worker, grant, spill,
/// creation time to the microsecond, execution count, inline-text flag and digest) with an older row that has a larger id
/// and larger figures; a newest row with NULL grant and spill fields over an older row that has values; an older row that
/// passes the filter under a newest row that does not; and NULL key parts, which form one group.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ParameterSensitivityNewestRowLiveTests
{
    /// <summary>Distinctive fake id: a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -949201;
    private const string TestServerName = "PspNewestSrv";
    private const int LiveTimeoutSeconds = 60;

    private const string TieDb = "Psp9492Db";
    private const string TieQueryHash = "0xQH_9492_TIE";
    private const string TiePlanHash = "0xPH_9492_TIE";
    private const string NullNewestQueryHash = "0xQH_9492_NULLNEWEST";
    private const string NewestQuietQueryHash = "0xQH_9492_NEWESTQUIET";
    private const string OlderQueryHash = "0xQH_9492_OLDER";
    private const string InlineTieQueryHash = "0xQH_9492_INLINETIE";

    private static readonly byte[] DigestTieLoser = [0x94, 0x92, 0x01];
    private static readonly byte[] DigestTieWinner = [0x94, 0x92, 0x02];
    private static readonly byte[] DigestOlderRow = [0x94, 0x92, 0x03];

    /// <summary>The compile time of the tied pair's winner, microseconds included, so the epoch round trip is tested to the last digit.</summary>
    private static DateTime WinnerCreation => Window().Start.AddDays(-2).AddHours(-3).AddTicks(1_234_560);

    private static DateTime OtherCreation => Window().Start.AddDays(-2).AddTicks(7_654_320);

    /// <summary>One seeded query_stats row.</summary>
    private sealed record Row(
        string? Db,
        string? QueryHash,
        string? PlanHash,
        DateTime Time,
        long Id,
        long ExecutionCount,
        long MinWorker,
        long MaxWorker,
        long? MinGrant,
        long? MaxGrant,
        long? MinSpills,
        long? MaxSpills,
        string? InlineText,
        byte[]? Digest,
        DateTime Creation);

    /// <summary>What a read handed back for one plan key.</summary>
    private sealed record Seen(
        double WorkerRatio,
        double? GrantRatio,
        int SpillDivergence,
        DateTime? CreationLocal,
        long? ExecutionCount,
        bool? HasInlineText,
        byte[]? Digest);

    [Fact]
    public async Task TheFact_KeepsEachPlansNewestRow_AndTheLargerIdOnATiedTime_AgainstDevPostgres()
    {
        var seen = await RunReadAsync(PgFactCollector.ParameterSensitivitySql, keyOrdinal: 8, isDrillDown: false);
        AssertNewestRows(seen, isDrillDown: false);
    }

    [Fact]
    public async Task TheDrillDown_KeepsEachPlansNewestRow_AndReadsItsTextFieldsFromThatRow_AgainstDevPostgres()
    {
        var seen = await RunReadAsync(PgDrillDownCollector.ParameterSensitiveSql, keyOrdinal: 0, isDrillDown: true);
        AssertNewestRows(seen, isDrillDown: true);
    }

    [Fact]
    public async Task TheDrillDownInlineText_OnATiedTime_ComesFromTheLargerCollectionId_AgainstDevPostgres()
    {
        /* Both rows of the key carry inline text and the texts differ. The key's ratio is 2, so the ranking does not report it:
           the inline-text read takes its keys as arrays, so it is asked for this plan directly. */
        var texts = await WithSeededStoreAsync(async (connection, windowStart, windowEnd, ct) =>
        {
            await using var cmd = new NpgsqlCommand(PgDrillDownCollector.ParameterSensitiveInlineTextSql, connection) { CommandTimeout = LiveTimeoutSeconds };
            cmd.Parameters.AddWithValue(TestServerId);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(new[] { TieDb });
            cmd.Parameters.AddWithValue(new[] { InlineTieQueryHash });
            cmd.Parameters.AddWithValue(new[] { TiePlanHash });

            var found = new List<string?>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                found.Add(reader.IsDBNull(3) ? null : reader.GetString(3));
            }

            return found;
        });

        Assert.Equal("inline text of the larger id", Assert.Single(texts));
    }

    private static void AssertNewestRows(Dictionary<string, Seen> seen, bool isDrillDown)
    {
        /* Two rows at the newest time: the larger collection_id wins and every figure is its own. The older row (the
           largest id of the three, and the largest figures) never wins on id: time decides first. */
        var tie = seen[Key(TieDb, TieQueryHash, TiePlanHash)];
        Assert.Equal(100.0, tie.WorkerRatio);
        Assert.Equal(10.0, tie.GrantRatio);
        Assert.Equal(1, tie.SpillDivergence);
        Assert.Equal(WinnerCreation, tie.CreationLocal);
        if (isDrillDown)
        {
            Assert.Equal(700L, tie.ExecutionCount);
            Assert.False(tie.HasInlineText);
            Assert.Equal(DigestTieWinner, tie.Digest);
        }

        /* A NULL grant and spill on the newest row come back NULL: the older row's values are not borrowed. */
        var nullNewest = seen[Key(TieDb, NullNewestQueryHash, TiePlanHash)];
        Assert.Equal(20.0, nullNewest.WorkerRatio);
        Assert.Null(nullNewest.GrantRatio);
        Assert.Equal(0, nullNewest.SpillDivergence);

        /* An older row with larger values is not chosen. */
        var older = seen[Key(TieDb, OlderQueryHash, TiePlanHash)];
        Assert.Equal(20.0, older.WorkerRatio);
        Assert.Equal(2.0, older.GrantRatio);

        /* The older row passes the filter and the newest does not: the plan is not reported. */
        Assert.False(seen.ContainsKey(Key(TieDb, NewestQuietQueryHash, TiePlanHash)));

        /* NULL key parts form one group, with the newest row's figures. */
        var nullKeys = seen.Where(pair => pair.Key == Key(null, null, null)).ToList();
        var nullKey = Assert.Single(nullKeys).Value;
        Assert.Equal(20.0, nullKey.WorkerRatio);

        Assert.Equal(4, seen.Count);
    }

    private static string Key(string? db, string? queryHash, string? planHash) =>
        string.Join('|', db ?? "<null>", queryHash ?? "<null>", planHash ?? "<null>");

    private static readonly DateTime WindowEnd = LiveClock.Now();

    private static (DateTime Start, DateTime End) Window() => (WindowEnd.AddHours(-4), WindowEnd);

    private static async Task<Dictionary<string, Seen>> RunReadAsync(string sql, int keyOrdinal, bool isDrillDown) =>
        await WithSeededStoreAsync(async (connection, windowStart, windowEnd, ct) =>
        {
            var seen = new Dictionary<string, Seen>();
            await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = LiveTimeoutSeconds };
            cmd.Parameters.AddWithValue(TestServerId);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified));

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var key = Key(
                    reader.IsDBNull(keyOrdinal) ? null : reader.GetString(keyOrdinal),
                    reader.IsDBNull(keyOrdinal + 1) ? null : reader.GetString(keyOrdinal + 1),
                    reader.IsDBNull(keyOrdinal + 2) ? null : reader.GetString(keyOrdinal + 2));

                /* The fact's columns: worker ratio 2, grant ratio 3, spill 4, creation time 5. The drill-down's: execution
                   count 3, worker ratio 6, grant ratio 7, spill 8, has inline text 9, creation time 10, digest 13. */
                var workerOrdinal = isDrillDown ? 6 : 2;
                var creationOrdinal = isDrillDown ? 10 : 5;
                seen.Add(key, new Seen(
                    reader.GetDouble(workerOrdinal),
                    reader.IsDBNull(workerOrdinal + 1) ? null : reader.GetDouble(workerOrdinal + 1),
                    Convert.ToInt32(reader.GetValue(workerOrdinal + 2), CultureInfo.InvariantCulture),
                    reader.IsDBNull(creationOrdinal) ? null : reader.GetDateTime(creationOrdinal),
                    isDrillDown ? reader.GetInt64(3) : null,
                    isDrillDown ? reader.GetBoolean(9) : null,
                    isDrillDown && !reader.IsDBNull(13) ? reader.GetFieldValue<byte[]>(13) : null));
            }

            return seen;
        });

    /// <summary>Migrates, seeds the window's rows, runs one read on its own connection, and cleans up.</summary>
    private static async Task<T> WithSeededStoreAsync<T>(Func<NpgsqlConnection, DateTime, DateTime, CancellationToken, Task<T>> read)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9492 newest-row test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var (windowStart, windowEnd) = Window();

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                foreach (var row in SeedRows(windowStart))
                {
                    await InsertAsync(connection, row, windowStart, ct);
                }
            }

            T result;
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                result = await read(connection, windowStart, windowEnd, ct);
            }

            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>
    /// The window's rows, in the order they are inserted: the tied pair goes in smaller id first, so a read that kept the
    /// first or the last of the tie by physical order could not pass both by luck.
    /// </summary>
    private static IEnumerable<Row> SeedRows(DateTime windowStart)
    {
        /* A fractional second, so the time the read hands back through epoch microseconds must round trip exactly. */
        var newest = windowStart.AddHours(2).AddTicks(6_543_210);
        var hourBefore = newest.AddHours(-1);
        var twoHoursBefore = newest.AddHours(-2);

        /* Two rows at the newest time. The winner (larger id) has ratio 100, grant ratio 10, a spill, no inline text and its own
           digest; the loser has ratio 20, grant ratio 2, no spill, inline text. The older row has the largest id and the largest
           figures: time, not id, is what picks the newest row. */
        yield return new Row(TieDb, TieQueryHash, TiePlanHash, newest, -94_920_050, 500, 20_000, 400_000, 1_024, 2_048, 0, 0, "inline loser", DigestTieLoser, OtherCreation);
        yield return new Row(TieDb, TieQueryHash, TiePlanHash, hourBefore, -94_920_030, 900, 20_000, 4_000_000, 1_024, 20_480, 0, 9, "inline older", DigestOlderRow, OtherCreation);
        yield return new Row(TieDb, TieQueryHash, TiePlanHash, newest, -94_920_040, 700, 25_000, 2_500_000, 1_024, 10_240, 0, 5, null, DigestTieWinner, WinnerCreation);

        /* A newest row with NULL grant and spill fields over an older row that has them. */
        yield return new Row(TieDb, NullNewestQueryHash, TiePlanHash, hourBefore, -94_920_060, 500, 20_000, 2_000_000, 1_024, 8_192, 0, 7, null, null, OtherCreation);
        yield return new Row(TieDb, NullNewestQueryHash, TiePlanHash, newest, -94_920_070, 500, 20_000, 400_000, null, null, null, null, null, null, OtherCreation);

        /* An older row with larger values under a newest row with smaller ones. */
        yield return new Row(TieDb, OlderQueryHash, TiePlanHash, twoHoursBefore, -94_920_080, 500, 20_000, 2_000_000, 1_024, 20_480, 0, 0, null, null, OtherCreation);
        yield return new Row(TieDb, OlderQueryHash, TiePlanHash, newest, -94_920_090, 500, 20_000, 400_000, 1_024, 2_048, 0, 0, null, null, OtherCreation);

        /* An older row that passes the filter under a newest row that does not (a ratio of 2). */
        yield return new Row(TieDb, NewestQuietQueryHash, TiePlanHash, hourBefore, -94_920_100, 500, 20_000, 2_000_000, 1_024, 2_048, 0, 0, null, null, OtherCreation);
        yield return new Row(TieDb, NewestQuietQueryHash, TiePlanHash, newest, -94_920_110, 500, 20_000, 40_000, 1_024, 2_048, 0, 0, null, null, OtherCreation);

        /* Two rows at the newest time that both carry inline text, the larger id inserted last. A ratio of 2, so the ranking does not
           report the key and the other reads are unchanged. */
        yield return new Row(TieDb, InlineTieQueryHash, TiePlanHash, newest, -94_920_150, 500, 20_000, 40_000, 1_024, 2_048, 0, 0, "inline text of the smaller id", null, OtherCreation);
        yield return new Row(TieDb, InlineTieQueryHash, TiePlanHash, newest, -94_920_140, 500, 20_000, 40_000, 1_024, 2_048, 0, 0, "inline text of the larger id", null, OtherCreation);

        /* NULL database, query hash and plan hash: one group, two rows, the newest wins. */
        yield return new Row(null, null, null, hourBefore, -94_920_120, 500, 20_000, 2_000_000, 1_024, 2_048, 0, 0, null, null, OtherCreation);
        yield return new Row(null, null, null, newest, -94_920_130, 500, 20_000, 400_000, 1_024, 2_048, 0, 0, null, null, OtherCreation);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, Row row, DateTime windowStart, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, query_text_digest, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 25)", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };

        cmd.Parameters.AddWithValue(row.Id);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(row.Time, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(TestServerName);
        cmd.Parameters.AddWithValue((object?)row.Db ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)row.QueryHash ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)row.PlanHash ?? DBNull.Value);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(row.Creation, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(row.ExecutionCount);
        cmd.Parameters.AddWithValue(row.MinWorker);
        cmd.Parameters.AddWithValue(row.MaxWorker);
        cmd.Parameters.AddWithValue((object?)row.MinGrant ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)row.MaxGrant ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)row.MinSpills ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)row.MaxSpills ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)row.InlineText ?? DBNull.Value);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea, Value = (object?)row.Digest ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        cmd.Parameters.AddWithValue(TestServerId);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
