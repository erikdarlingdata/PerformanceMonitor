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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates its own scratch database through ScratchPostgres and
   works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #9460 (after #9303): both trigram indexes clean their pending list every <see cref="QueryTextTrigramIndex.CleanInterval"/>. The interval
/// is the longest one whose pending list, at the busiest insert rate, stays under half of <c>gin_pending_list_limit</c>, so a collection
/// INSERT never meets a full list and never merges it inline.
///
/// <para><b>How the run is scaled.</b> The list's size depends on how many rows land between two cleans, not on the clock. The run
/// therefore counts virtual minutes: one virtual minute is <see cref="PeakRowsPerMinute"/> rows inserted back to back in
/// <see cref="BatchesPerMinute"/> statements, and a clean runs every <c>interval</c> virtual minutes. The rate and the interval stay the
/// real ones, only the waiting is dropped. A forced checkpoint every 5 virtual minutes stands in for <c>checkpoint_timeout</c>, so the
/// full-page images the cleans write are counted as a real store would write them.</para>
/// </summary>
public sealed class TrigramCleanIntervalLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the trigram clean interval live tests.";

    /// <summary>The busiest insert rate the interval is sized for: 60,000 new distinct texts an hour into each table.</summary>
    private const int PeakRowsPerMinute = 1000;

    private const int BatchesPerMinute = 10;

    /// <summary>Words per generated text: about 1.4 KB of statement text, about 450 distinct trigrams, the size of a Query Store statement.</summary>
    private const int WordsPerText = 190;

    /// <summary>Distinct words the texts draw from, with a skew toward the common ones: a statement repeats its keywords and names, so a
    /// 1.4 KB text carries about 450 distinct trigrams rather than the thousand a text of random words would.</summary>
    private const int TextVocabulary = 100;

    private const int VocabularyRows = 5000;

    /// <summary>One GIN pending-list page is 8 KB, and the index's limit is <see cref="QueryTextTrigramIndex.PendingListLimitKb"/> kB.</summary>
    private const int PendingPagesAtLimit = QueryTextTrigramIndex.PendingListLimitKb / 8;

    private const int PendingPagesAtHalfLimit = PendingPagesAtLimit / 2;

    private sealed record CleanSample(int Minute, long PendingPages, long PendingTuples, long WalBytes, long FullPageImages, double Milliseconds);

    private sealed record ScenarioResult(
        string Index, int IntervalMinutes, int VirtualMinutes, IReadOnlyList<CleanSample> Cleans, long PeakPendingPages, long PeakPendingTuples,
        double MaxInsertMilliseconds, int InlineMerges);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<(long Pages, long Tuples)> PendingAsync(NpgsqlConnection connection, string indexName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT pending_pages::bigint, pending_tuples::bigint FROM pgstatginindex('{indexName}')", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    /// <summary>The SELECT that makes <c>rows</c> distinct texts of about 1.4 KB each from a skewed vocabulary, so the common trigrams repeat
    /// across rows as a store's statements do.</summary>
    private const string TextsSql = @"
WITH v AS (SELECT array_agg(w ORDER BY id) AS a FROM public.zz_vocab)
SELECT g AS n,
       (SELECT string_agg(v.a[1 + floor(power(random(), 3) * {0} * (1 + x.i * 0))::int], ' ') FROM generate_series(1, {1}) AS x(i) WHERE x.i > 0 OR g > 0) || ' ' || g::text || '/' || {2}::text AS t
FROM v, generate_series({3}, {4}) AS g";

    private static string Texts(int first, int last, int salt, int vocabulary = TextVocabulary, int words = WordsPerText) =>
        string.Format(CultureInfo.InvariantCulture, TextsSql, vocabulary, words, salt, first, last);

    /// <summary>Seeds the session's random() from the first row number and the salt, so every run generates the same texts. Call it on the
    /// connection that runs the generating statement, right before that statement: the peak sits close to the bound, and a different draw of
    /// words moves it by a percent or two.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, int first, int salt, CancellationToken ct)
    {
        var seed = (((long)first * 31 + (long)salt * 7919) % 10_000 + 1) / 10_001.0;
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, "SELECT setseed({0:F6})", seed), ct);
    }

    private static async Task PrepareVocabularyAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public", ct);
        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pgstattuple WITH SCHEMA public", ct);
        await ExecAsync(connection,
            @"CREATE TABLE IF NOT EXISTS public.zz_vocab AS
              SELECT g AS id, (SELECT string_agg(chr(97 + get_byte(decode(md5(g::text), 'hex'), i) % 26), '' ORDER BY i)
                               FROM generate_series(0, 2 + g % 8) AS i) AS w
              FROM generate_series(1, " + VocabularyRows + ") AS g", ct);
    }

    private static async Task PrepareAsync(NpgsqlConnection connection, bool queryStore, int seedRows, CancellationToken ct)
    {
        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public", ct);
        await ExecAsync(connection, "CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public", ct);
        await PrepareVocabularyAsync(connection, ct);

        var table = queryStore ? QueryStoreTextTrigramIndex.TableName : QueryTextTrigramIndex.TableName;

        // The store's autovacuum also cleans a pending list. Off here, so the cleans under test are the only thing that empties it.
        await ExecAsync(connection, $"ALTER TABLE {table} SET (autovacuum_enabled = false)", ct);

        for (var first = 1; first <= seedRows; first += 10_000)
        {
            var last = Math.Min(first + 9_999, seedRows);
            await SeedAsync(connection, first, 0, ct);
            await ExecAsync(connection, InsertSql(queryStore, Texts(first, last, 0), 0), ct);
        }

        await ExecAsync(connection, queryStore
            ? QueryStoreTextTrigramIndex.CreateIndexSql("public", "public")
            : QueryTextTrigramIndex.CreateIndexSql("public"), ct);
        await ExecAsync(connection, $"VACUUM {table}", ct);
    }

    private static string InsertSql(bool queryStore, string textsSelect, int idOffset) => queryStore
        ? $@"INSERT INTO {QueryStoreTextTrigramIndex.TableName} (server_id, database_name, query_id, query_sql_text, last_seen)
             SELECT 1, 'db', n + {idOffset}, t, timezone('UTC', now()) FROM ({textsSelect}) AS s"
        : $@"INSERT INTO {QueryTextTrigramIndex.TableName} (digest, query_text, last_seen)
             SELECT sha256(convert_to(t, 'UTF8')), t, timezone('UTC', now()) FROM ({textsSelect}) AS s";

    private static async Task<ScenarioResult> RunScenarioAsync(
        string connectionString, string adminConnectionString, bool queryStore, int intervalMinutes, int virtualMinutes, int seedRows, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var admin = new NpgsqlConnection(adminConnectionString);
        await admin.OpenAsync(ct);

        await PrepareAsync(connection, queryStore, seedRows, ct);

        var indexName = queryStore ? QueryStoreTextTrigramIndex.IndexName : QueryTextTrigramIndex.IndexName;
        var cleanSql = QueryTextTrigramIndex.CleanPendingListSql(indexName);
        await ExecAsync(connection, cleanSql, ct);
        await ExecAsync(admin, "CHECKPOINT", ct);

        var cleans = new List<CleanSample>();
        long peakPages = 0, peakTuples = 0, previousPages = 0;
        double maxInsertMs = 0;
        var inlineMerges = 0;
        var rowsPerBatch = PeakRowsPerMinute / BatchesPerMinute;
        var nextRow = seedRows + 1;

        for (var minute = 1; minute <= virtualMinutes; minute++)
        {
            for (var batch = 0; batch < BatchesPerMinute; batch++)
            {
                await SeedAsync(connection, nextRow, minute, ct);
                var started = System.Diagnostics.Stopwatch.StartNew();
                await ExecAsync(connection, InsertSql(queryStore, Texts(nextRow, nextRow + rowsPerBatch - 1, minute), 0), ct);
                started.Stop();
                nextRow += rowsPerBatch;
                maxInsertMs = Math.Max(maxInsertMs, started.Elapsed.TotalMilliseconds);

                var (pages, tuples) = await PendingAsync(connection, indexName, ct);
                if (pages < previousPages)
                {
                    inlineMerges++;
                }

                previousPages = pages;
                peakPages = Math.Max(peakPages, pages);
                peakTuples = Math.Max(peakTuples, tuples);
            }

            if (minute % intervalMinutes == 0)
            {
                var (pagesBefore, tuplesBefore) = await PendingAsync(connection, indexName, ct);
                var lsnBefore = await ScalarAsync<string>(admin, "SELECT pg_current_wal_insert_lsn()::text", ct);
                var fpiBefore = await FullPageImagesAsync(connection, ct);
                var clock = System.Diagnostics.Stopwatch.StartNew();
                await ExecAsync(connection, cleanSql, ct);
                clock.Stop();
                var fpiAfter = await FullPageImagesAsync(connection, ct);
                var walBytes = await ScalarAsync<decimal>(admin, $"SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '{lsnBefore}'::pg_lsn)", ct);
                cleans.Add(new CleanSample(minute, pagesBefore, tuplesBefore, (long)walBytes, fpiAfter - fpiBefore, clock.Elapsed.TotalMilliseconds));
                previousPages = 0;
            }

            if (minute % 5 == 0)
            {
                await ExecAsync(admin, "CHECKPOINT", ct);
            }
        }

        return new ScenarioResult(
            queryStore ? "query_store_text" : "query_text_dim", intervalMinutes, virtualMinutes, cleans, peakPages, peakTuples, maxInsertMs, inlineMerges);
    }

    /// <summary>The session's own full-page-image count, flushed to the shared statistics first so the read sees the clean's images.</summary>
    private static async Task<long> FullPageImagesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        await ExecAsync(connection, "SELECT 1", ct);
        return await ScalarAsync<long>(connection, "SELECT wal_fpi FROM pg_stat_wal", ct);
    }

    private static string AdminConnectionString(string baseConnectionString, ScratchPostgres scratch)
    {
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString);
        var baseBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString);
        builder.Username = baseBuilder.Username;
        builder.Password = baseBuilder.Password;
        return builder.ConnectionString;
    }

    private static async Task<ScenarioResult> RunOnScratchAsync(
        bool queryStore, int intervalMinutes, int virtualMinutes, int seedRows, CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        ScenarioResult result;
        try
        {
            await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            result = await RunScenarioAsync(scratch.ConnectionString, AdminConnectionString(BaseConnectionString!, scratch), queryStore, intervalMinutes, virtualMinutes, seedRows, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }

        return result;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AtThePeakInsertRate_ThePendingListStaysUnderHalfTheLimit_BetweenCleans_AndNoInsertMergesItInline(bool queryStore)
    {
        var ct = TestContext.Current.CancellationToken;
        var interval = (int)(queryStore ? QueryStoreTextTrigramIndex.CleanInterval : QueryTextTrigramIndex.CleanInterval).TotalMinutes;

        var result = await RunOnScratchAsync(queryStore, interval, virtualMinutes: 3 * interval, seedRows: 3000, ct);

        var peakOut = Environment.GetEnvironmentVariable("DARLING_TRIGRAM_PEAK_OUT");
        if (!string.IsNullOrEmpty(peakOut))
        {
            await File.AppendAllTextAsync(peakOut, string.Format(CultureInfo.InvariantCulture,
                "classes: P|{0}|interval={1}min|peakPages={2}|peakTuples={3}|inlineMerges={4}{5}",
                result.Index, interval, result.PeakPendingPages, result.PeakPendingTuples, result.InlineMerges, Environment.NewLine), ct);
        }

        Assert.Equal(3, result.Cleans.Count);
        Assert.True(result.PeakPendingPages <= PendingPagesAtHalfLimit,
            $"{result.Index}: the pending list peaked at {result.PeakPendingPages} pages ({result.PeakPendingTuples} tuples) between cleans every {interval} minutes; half the limit is {PendingPagesAtHalfLimit} pages.");
        Assert.Equal(0, result.InlineMerges);
    }

    /// <summary>The measurement behind the interval, opt in: <c>DARLING_TRIGRAM_MEASURE_OUT</c> names the file the table is written to, and
    /// <c>DARLING_TRIGRAM_MEASURE_INTERVALS</c> the comma list of intervals in minutes. One virtual hour at the peak rate per interval and
    /// index, on a table seeded with an hour of rows.</summary>
    [Fact]
    public async Task Measure_TextShape_PerVocabularySize()
    {
        var output = Environment.GetEnvironmentVariable("DARLING_TRIGRAM_MEASURE_OUT");
        Assert.SkipWhen(string.IsNullOrEmpty(output), "Set DARLING_TRIGRAM_MEASURE_OUT to run the clean interval measurement.");
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PrepareVocabularyAsync(connection, ct);
        foreach (var (vocabulary, words) in new[] { (60, 190), (100, 190), (150, 190) })
        {
            await using var command = new NpgsqlCommand(
                $"SELECT avg(length(t)), avg(cardinality(show_trgm(t))) FROM ({Texts(1, 200, 0, vocabulary, words)}) AS s", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            await File.AppendAllTextAsync(output!, string.Format(CultureInfo.InvariantCulture,
                "classes: S|vocabulary={0}|words={1}|avgChars={2:F0}|avgDistinctTrigrams={3:F0}{4}", vocabulary, words, reader.GetDecimal(0), reader.GetDecimal(1), Environment.NewLine), ct);
        }

        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, true, static (_, _) => Task.CompletedTask);
    }

    [Fact]
    public async Task Measure_PendingListWalAndCleanTime_PerInterval()
    {
        var output = Environment.GetEnvironmentVariable("DARLING_TRIGRAM_MEASURE_OUT");
        Assert.SkipWhen(string.IsNullOrEmpty(output), "Set DARLING_TRIGRAM_MEASURE_OUT to run the clean interval measurement.");
        var ct = TestContext.Current.CancellationToken;
        var intervals = (Environment.GetEnvironmentVariable("DARLING_TRIGRAM_MEASURE_INTERVALS") ?? "2,5,10")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(int.Parse).ToArray();
        var virtualMinutes = int.Parse(Environment.GetEnvironmentVariable("DARLING_TRIGRAM_MEASURE_MINUTES") ?? "30", CultureInfo.InvariantCulture);
        var which = Environment.GetEnvironmentVariable("DARLING_TRIGRAM_MEASURE_INDEX") ?? "both";
        var seedRows = 20 * PeakRowsPerMinute;
        var scale = 60.0 / virtualMinutes;

        foreach (var interval in intervals)
        {
            foreach (var queryStore in new[] { false, true }.Where(q => which == "both" || (which == "qs") == q))
            {
                var r = await RunOnScratchAsync(queryStore, interval, virtualMinutes, seedRows, ct);
                var line = string.Format(CultureInfo.InvariantCulture,
                    "classes: M|{0}|interval={1}min|virtualMin={12}|peakPages={2}|peakTuples={3}|cleans={4}|walMBperHour={5:F1}|fpiPerHour={6}|cleanMsAvg={7:F0}|cleanMsMax={8:F0}|maxInsertMs={9:F0}|inlineMerges={10}|pagesBeforeEachClean={11}",
                    r.Index, r.IntervalMinutes, r.PeakPendingPages, r.PeakPendingTuples, r.Cleans.Count,
                    r.Cleans.Sum(c => c.WalBytes) / 1048576.0 * scale, r.Cleans.Sum(c => c.FullPageImages) * scale,
                    r.Cleans.Average(c => c.Milliseconds), r.Cleans.Max(c => c.Milliseconds), r.MaxInsertMilliseconds, r.InlineMerges,
                    string.Join(",", r.Cleans.Select(c => c.PendingPages)), virtualMinutes);
                await File.AppendAllTextAsync(output!, line + Environment.NewLine, ct);
            }
        }
    }
}
