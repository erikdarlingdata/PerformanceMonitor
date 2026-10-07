/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5244 (PR5, lane R2): get_file_io_stats and get_file_io_trend read a SET of databases. Each reader and tool gains an
/// overload that takes a <see cref="DatabaseFilter"/>; the public tool methods keep passing every database (the trend: the
/// one database_name it already takes) until the wiring lane. This class pins, over a seed of A, B and C plus a crowd of
/// small databases: [A, B] returns only A and B on every statement the reads use; the snapshot's capture does not move with
/// the filter; one chosen database keeps the per-file chart (a <c>file_name</c> on every line) while two or more draw
/// per-database lines within the set; the top-five-plus-"(other)" cap holds in both shapes and applies after the filter; and
/// the empty, unavailable and never-collected answers say something true for a list. Skips without <c>DARLING_TEST_PG</c>.
/// </summary>
// #1776 own-store: this fixture seeds its own server id and removes its rows; it does not use ScratchPostgres.
[Collection("live-postgres")]
public sealed class FileIoDatabaseFilterLiveTests
{
    internal const string ServerName = "darling-file-io-dbfilter-e2e";
    private const string EmptyServerName = "darling-file-io-dbfilter-none-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);
    internal const string DbA = "FioDbA";
    internal const string DbB = "FioDbB";
    internal const string DbC = "FioDbC";
    internal const string DbMany = "FioMany";
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live file I/O database-filter test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Crowd(int i) => "FioCrowd" + i.ToString("00", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public void Sql_IsTheListForm_OnEveryStatementTheReadsUse_WithNoNameSplicedIn()
    {
        Assert.Contains("($2::text[] IS NULL OR database_name = ANY($2))", DarlingDataReader.LatestFileIoStatsSql, StringComparison.Ordinal);
        Assert.Contains("($4::text[] IS NULL OR database_name = ANY($4))", DarlingTrendReader.FileIoSeriesSql, StringComparison.Ordinal);
        Assert.Contains("($4::text[] IS NULL OR database_name = ANY($4))", DarlingTrendReader.FileIoTrendSql, StringComparison.Ordinal);
        /* The per-file grain is "exactly one chosen database", on the ranking and on the bucket query alike. */
        Assert.DoesNotContain("$4::text IS NULL", DarlingTrendReader.FileIoSeriesSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4::text IS NULL", DarlingTrendReader.FileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("cardinality($4::text[]) = 1", DarlingTrendReader.FileIoSeriesSql, StringComparison.Ordinal);
        Assert.Contains("cardinality($4::text[]) = 1", DarlingTrendReader.FileIoTrendSql, StringComparison.Ordinal);
        /* The snapshot's anchor subquery is NOT filtered, so a filtered and an unfiltered call show the same capture. */
        var sql = DarlingDataReader.LatestFileIoStatsSql;
        var anchorSubquery = sql[sql.IndexOf("(SELECT MAX", StringComparison.Ordinal)..sql.IndexOf("($2::text[]", StringComparison.Ordinal)];
        Assert.DoesNotContain("$2", anchorSubquery, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Snapshot_TwoNames_ReturnOnlyThoseDatabases_OnTheSameCaptureAsEveryDatabase_AndAnEmptyAnswerSaysWhichKind()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, ct);

            /* The newest capture holds A and C only (B was in the older ones), so it is the same capture for every filter. */
            var all = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, ServerId, DatabaseFilter.All, ct);
            Assert.Equal(new[] { DbA, DbA, DbC }, all.Rows.Select(r => r.DatabaseName).Order().ToArray());
            var viaDefault = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, ServerId, cancellationToken: ct);
            Assert.Equal(all.Rows.Count, viaDefault.Rows.Count);

            var ab = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, ServerId, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { DbA, DbA }, ab.Rows.Select(r => r.DatabaseName).ToArray());
            Assert.Equal(all.CapturedAt, ab.CapturedAt);

            var bc = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, ServerId, DatabaseFilter.Of([DbB, DbC]), ct);
            Assert.Equal(new[] { DbC }, bc.Rows.Select(r => r.DatabaseName).ToArray());
            Assert.Equal(all.CapturedAt, bc.CapturedAt);

            /* B has rows in the store but none in the newest capture: the filtered snapshot is empty, the capture is not. */
            var b = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, ServerId, DatabaseFilter.One(DbB), ct);
            Assert.True(b.IsEmpty);
            Assert.NotNull(await DarlingDataReader.GetLatestFileIoCaptureAsync(postgres, ServerId, ct));
            Assert.Null(await DarlingDataReader.GetLatestFileIoCaptureAsync(postgres, EmptyServerId, ct));

            /* The tool overload: the page holds only the chosen databases and names them. */
            var page = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, ServerName, DatabaseFilter.Of([DbA, DbB]), ct)).RootElement;
            Assert.Equal("the chosen databases", page.GetProperty("database_name").GetString());
            Assert.Equal(all.CapturedAt!.Value.ToString("o"), page.GetProperty("captured_at").GetString());
            Assert.All(page.GetProperty("files").EnumerateArray(), f => Assert.Equal(DbA, f.GetProperty("database_name").GetString()));
            var one = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, ServerName, DatabaseFilter.One(DbC), ct)).RootElement;
            Assert.Equal(DbC, one.GetProperty("database_name").GetString());
            var every = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, ServerName)).RootElement;
            Assert.Equal(JsonValueKind.Null, every.GetProperty("database_name").ValueKind);
            Assert.Equal(3, every.GetProperty("files").GetArrayLength());

            /* Empty path: the newest capture exists and holds none of the chosen databases. A true negative, not "unavailable",
               and the sentence names the database (one) or the set (two or more), never the server. */
            var emptyOne = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, ServerName, DatabaseFilter.One(DbB), ct)).RootElement;
            Assert.Equal("empty", emptyOne.GetProperty("status").GetString());
            Assert.Contains("for the database " + DbB, emptyOne.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal(DbB, emptyOne.GetProperty("database_name").GetString());
            var emptyMany = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, ServerName, DatabaseFilter.Of([DbB, "NoSuchDb"]), ct)).RootElement;
            Assert.Equal("empty", emptyMany.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases", emptyMany.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("the chosen databases", emptyMany.GetProperty("database_name").GetString());

            /* Unavailable path: a server with no file I/O row at all stays "not collected" under a filter, and says which
               databases the call was limited to. */
            var never = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, EmptyServerName, DatabaseFilter.Of([DbA, DbB]), ct)).RootElement;
            Assert.NotEqual("empty", never.GetProperty("status").GetString());
            Assert.Equal("the chosen databases", never.GetProperty("database_name").GetString());
            var neverAll = JsonDocument.Parse(await DarlingMcpDataTools.GetFileIoStats(postgres, EmptyServerName)).RootElement;
            Assert.Equal(never.GetProperty("status").GetString(), neverAll.GetProperty("status").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Trend_OneName_KeepsPerFileLines_TwoNames_DrawPerDatabaseLinesWithinTheSet()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var start = end.AddHours(-1);

            /* Reader level. One database: each of its files is a series. Two: one series per (database, file type). */
            var oneSeries = await DarlingTrendReader.GetFileIoSeriesAsync(postgres, ServerId, start, end, DatabaseFilter.One(DbA), ct);
            Assert.Equal(new[] { "FioDbA.a1.mdf", "FioDbA.a2.ldf" }, oneSeries.Select(s => s.DatabaseName + "." + s.FileName).Order().ToArray());
            Assert.All(oneSeries, s => Assert.NotNull(s.FileName));

            var abSeries = await DarlingTrendReader.GetFileIoSeriesAsync(postgres, ServerId, start, end, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { (DbB, "ROWS"), (DbA, "ROWS"), (DbA, "LOG") }, abSeries.Select(s => (s.DatabaseName, s.FileType)).ToArray());
            Assert.All(abSeries, s => Assert.Null(s.FileName));
            Assert.Equal(new long[] { 1, 1, 1 }, abSeries.Select(s => s.Files).ToArray());

            var allSeries = await DarlingTrendReader.GetFileIoSeriesAsync(postgres, ServerId, start, end, DatabaseFilter.All, ct);
            Assert.Contains(allSeries, s => s.DatabaseName == DbC);
            Assert.All(allSeries, s => Assert.Null(s.FileName));

            var abPoints = await DarlingTrendReader.GetFileIoTrendAsync(postgres, ServerId, start, end, DatabaseFilter.Of([DbA, DbB]), 5, 10, ct);
            Assert.NotEmpty(abPoints);
            Assert.All(abPoints, p => Assert.Contains(p.DatabaseName, new[] { DbA, DbB }));
            Assert.All(abPoints, p => Assert.Null(p.FileName));
            var onePoints = await DarlingTrendReader.GetFileIoTrendAsync(postgres, ServerId, start, end, DatabaseFilter.One(DbA), 5, 10, ct);
            Assert.All(onePoints, p => Assert.Equal(DbA, p.DatabaseName));
            Assert.All(onePoints, p => Assert.NotNull(p.FileName));

            /* Tool level: the shape the answer declares follows the number of chosen databases, and the echo names them. */
            var asOf = end.ToString("o");
            var two = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.Of([DbA, DbB]), TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("database_file_type", two.GetProperty("series_grain").GetString());
            Assert.Equal("the chosen databases", two.GetProperty("database_name").GetString());
            Assert.Equal(3, two.GetProperty("series_active").GetInt32());
            Assert.All(two.GetProperty("trend").EnumerateArray(), p => Assert.Contains(p.GetProperty("database_name").GetString(), new[] { DbA, DbB }));
            Assert.All(two.GetProperty("trend").EnumerateArray(), p => Assert.False(p.TryGetProperty("file_name", out _)));

            var single = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.One(DbA), TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("file", single.GetProperty("series_grain").GetString());
            Assert.Equal(DbA, single.GetProperty("database_name").GetString());
            Assert.All(single.GetProperty("trend").EnumerateArray(), p => Assert.NotEqual(JsonValueKind.Null, p.GetProperty("file_name").ValueKind));

            /* The public tool still takes one name, and a whitespace-only name is every database; a name is kept exactly (#5244:
               the tool no longer trims), so a padded name matches nothing. */
            var publicBlank = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, asOf, null, "   ", ct)).RootElement;
            Assert.Equal("database_file_type", publicBlank.GetProperty("series_grain").GetString());
            Assert.Equal(JsonValueKind.Null, publicBlank.GetProperty("database_name").ValueKind);
            var publicOne = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, asOf, null, DbA, ct)).RootElement;
            Assert.Equal("file", publicOne.GetProperty("series_grain").GetString());
            var padded = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, asOf, null, " " + DbA, ct)).RootElement;
            Assert.Equal("empty", padded.GetProperty("status").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Trend_TheTopFivePlusOtherCap_HoldsInBothShapes_AndAppliesAfterTheFilter()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var asOf = end.ToString("o");

            /* One database with seven files: per-file lines, the five heaviest keep their own and the other two fold. */
            var perFile = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.One(DbMany), TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("file", perFile.GetProperty("series_grain").GetString());
            Assert.Equal(7, perFile.GetProperty("series_active").GetInt32());
            Assert.Equal(5, perFile.GetProperty("series_charted").GetInt32());
            Assert.Equal(2, perFile.GetProperty("series_folded").GetInt32());
            var perFileLegend = perFile.GetProperty("series").EnumerateArray().ToList();
            Assert.Equal(6, perFileLegend.Count);
            Assert.Equal(new[] { "m7.mdf", "m6.mdf", "m5.mdf", "m4.mdf", "m3.mdf", "(other)" }, perFileLegend.Select(s => s.GetProperty("file_name").GetString()).ToArray());
            Assert.Equal(2, perFileLegend[5].GetProperty("files").GetInt64());
            Assert.All(perFile.GetProperty("trend").EnumerateArray(), p => Assert.True(p.GetProperty("database_name").GetString() is DbMany or "(other)"));

            /* Seven chosen databases of a store that holds twelve: per-database lines, the cap applies to the set. A, B, C and
               the many-file database are outside it and appear nowhere. */
            var chosen = Enumerable.Range(1, 7).Select(Crowd).ToArray();
            var perDatabase = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.Of(chosen), TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("database_file_type", perDatabase.GetProperty("series_grain").GetString());
            Assert.Equal(7, perDatabase.GetProperty("series_active").GetInt32());
            Assert.Equal(5, perDatabase.GetProperty("series_charted").GetInt32());
            Assert.Equal(2, perDatabase.GetProperty("series_folded").GetInt32());
            var legend = perDatabase.GetProperty("series").EnumerateArray().ToList();
            Assert.Equal(new[] { Crowd(7), Crowd(6), Crowd(5), Crowd(4), Crowd(3), "(other)" }, legend.Select(s => s.GetProperty("database_name").GetString()).ToArray());
            Assert.All(perDatabase.GetProperty("trend").EnumerateArray(), p => Assert.Contains(p.GetProperty("database_name").GetString()!, chosen.Append("(other)")));

            /* The same twelve-database store read for every database folds differently: the cap is not a property of the data. */
            var every = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.All, TrendBudget.Chart, ct)).RootElement;
            Assert.True(every.GetProperty("series_active").GetInt32() > 7);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Trend_EmptyAndUnavailableAnswers_SaySomethingTrueForAList()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var asOf = end.ToString("o");

            /* A list that matches nothing: empty, about the set, never a verdict on the server's collection. */
            var many = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("empty", many.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases", many.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("the chosen databases", many.GetProperty("database_name").GetString());
            Assert.DoesNotContain("genuinely quiet", many.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* One name keeps its sentence, and now echoes the name. */
            var one = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, asOf, null, DatabaseFilter.One("NoSuchDb"), TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("empty", one.GetProperty("status").GetString());
            Assert.Contains("database 'NoSuchDb'", one.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("NoSuchDb", one.GetProperty("database_name").GetString());

            /* The unfiltered quiet-window sentence is unchanged: a window with no rows at all. */
            var quiet = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, ServerName, 1, end.AddDays(-30).ToString("o"), null, DatabaseFilter.All, TrendBudget.Chart, ct)).RootElement;
            Assert.Equal("empty", quiet.GetProperty("status").GetString());
            Assert.Contains("genuinely quiet", quiet.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal(JsonValueKind.Null, quiet.GetProperty("database_name").ValueKind);

            /* A server with no file I/O row ever: the never-collected answer is about the server, filtered or not, and carries
               the echo so a client reads which databases the call was limited to. */
            var never = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, EmptyServerName, 1, asOf, null, DatabaseFilter.Of([DbA, DbB]), TrendBudget.Chart, ct)).RootElement;
            Assert.NotEqual("empty", never.GetProperty("status").GetString());
            Assert.Equal("the chosen databases", never.GetProperty("database_name").GetString());
            var neverAll = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(
                postgres, EmptyServerName, 1, asOf, null, DatabaseFilter.All, TrendBudget.Chart, ct)).RootElement;
            Assert.Equal(never.GetProperty("status").GetString(), neverAll.GetProperty("status").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Seeds two servers (the second has no file I/O row) and returns the window end (now). Two older captures (30 and 20
    /// minutes back) hold every database: A (a1.mdf ROWS, a2.ldf LOG), B (b1.mdf ROWS, B the heaviest stall of the three), C
    /// (c1.mdf ROWS, the heaviest of all), seven one-file "crowd" databases (stall rising with the number) and one database
    /// (FioMany) with seven files m1..m7 (stall rising with the number). The newest capture (5 minutes back) holds A and C
    /// only, so B has rows in the window but none in the newest snapshot.
    /// </summary>
    internal static async Task<DateTime> SeedAsync(string cs, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, EmptyServerId, EmptyServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        foreach (var minutesBack in new[] { 30, 20 })
        {
            var at = DarlingMcpTestData.Naive(end.AddMinutes(-minutesBack));
            await SeedFileAsync(connection, ct, at, DbA, "a1.mdf", "ROWS", 100, 10, 1_000, 100);
            await SeedFileAsync(connection, ct, at, DbA, "a2.ldf", "LOG", 0, 50, 0, 500);
            await SeedFileAsync(connection, ct, at, DbB, "b1.mdf", "ROWS", 50, 10, 5_000, 100);
            await SeedFileAsync(connection, ct, at, DbC, "c1.mdf", "ROWS", 100, 10, 90_000, 100);
            for (var i = 1; i <= 7; i++)
            {
                await SeedFileAsync(connection, ct, at, Crowd(i), "crowd" + i + ".mdf", "ROWS", 10, 1, i * 100, 10);
                await SeedFileAsync(connection, ct, at, DbMany, "m" + i + ".mdf", "ROWS", 10, 1, i * 10, 1);
            }
        }

        var newest = DarlingMcpTestData.Naive(end.AddMinutes(-5));
        await SeedFileAsync(connection, ct, newest, DbA, "a1.mdf", "ROWS", 100, 10, 1_000, 100);
        await SeedFileAsync(connection, ct, newest, DbA, "a2.ldf", "LOG", 0, 50, 0, 500);
        await SeedFileAsync(connection, ct, newest, DbC, "c1.mdf", "ROWS", 100, 10, 90_000, 100);
        return end;
    }

    private static async Task SeedFileAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string database, string file, string fileType,
        long reads, long writes, long stallRead, long stallWrite) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, database, file, fileType,
            "D:\\data\\" + file, 1000m, reads, writes, reads * 8192, writes * 8192, stallRead, stallWrite, 60);

    internal static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM file_io_stats WHERE server_id IN ({ServerId}, {EmptyServerId}); DELETE FROM servers WHERE server_id IN ({ServerId}, {EmptyServerId});",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
