/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1855, Darling half: the Collection Health exemplar columns must show the message from the LATEST run
/// that left one, not <c>MAX()</c>'s lexicographically greatest string. Lite.Tests
/// (<c>CollectionHealthLatestNoteTests</c>) pins the same expectations against live DuckDB on the
/// byte-identical query shape; parity is the point, and the two stores disagreeing on identical data was
/// itself part of the defect (DuckDB sorts text on bytes, Postgres on the database collation).
///
/// <para>Run against a REAL store through BOTH Postgres readers — the Viewer's and the MCP service's — for
/// the reason #1855 exists: a source pin cannot tell a lexicographic MAX from a newest-first rank, since
/// both are valid SQL returning one string. Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCollectionHealthLatestNoteTests
{
    private const int ServerId = -949551;
    private const string ServerName = "health-latest-note";

    /// <summary>
    /// The fleet read rolls up EVERY enabled server and its rows carry no server_id, so the fleet assertions
    /// scope themselves by a collector name no real install has — the sentinel-server discipline
    /// <c>DarlingFleetReaderLivePostgresTests</c> uses, applied to the only key this projection exposes.
    /// </summary>
    private const string FleetProbeCollector = "note_rank_fleet_probe";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The real probe-failure note for a count, from the shared driver — the text the defect rides on.</summary>
    private static string ProbeNote(int count) =>
        string.Format(CultureInfo.InvariantCulture, EnumeratedCollectorDriver.ProbeFailureNoteFormat, count);

    [Fact]
    public async Task BothReaders_TakeTheNewestNoteAndError_NotTheLexicographicallyGreatest_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-health note test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* The exact shape of #1855: the LATER run reports 12, the earlier one 9. "12 item(s)" sorts
               below "9 item(s)", so a value-MAX hands back the OLDER run's count. */
            await SeedAsync(connection, ct, "query_store", MinutesAgo(30), "SUCCESS", ProbeNote(9));
            await SeedAsync(connection, ct, "query_store", MinutesAgo(20), "SUCCESS", ProbeNote(12));
            /* A later CLEAN run must not blank a note the window still holds — the "(2 of 3 runs)"
               qualifier describes exactly this sometimes-empty window. */
            await SeedAsync(connection, ct, "query_store", MinutesAgo(10), "SUCCESS", null);

            /* The sibling column, same defect: "zzz" is the greatest string, "aaa" is the newest failure.
               The instants are held in locals because MinutesAgo truncates to the second, so re-deriving
               one at assert time is a coin flip on whether the clock crossed a second boundary. */
            var newestFailure = MinutesAgo(20);
            await SeedAsync(connection, ct, "deadlocks", MinutesAgo(30), "ERROR", "zzz older failure");
            await SeedAsync(connection, ct, "deadlocks", newestFailure, "ERROR", "aaa newest failure");

            /* No failing run carried text, so the error rank falls through to the newest row of any class —
               a SUCCESS row holding a note. The status re-check on the rank-1 row is what stops that note
               from being reported as a failure; last_error_time still names the run that failed. */
            var fileIoFailedAt = MinutesAgo(30);
            await SeedAsync(connection, ct, "file_io", fileIoFailedAt, "ERROR", null);
            await SeedAsync(connection, ct, "file_io", MinutesAgo(20), "SUCCESS", EnumeratedCollectorDriver.EmptyEnumerationMessage);

            /* A message on a non-SUCCESS row is not a note however new it is: Darling's SESSION_MISSING is
               a real capture fault with its own self-alert, and it is lexicographically greater here. */
            await SeedAsync(connection, ct, "blocking", MinutesAgo(30), "SUCCESS", EnumeratedCollectorDriver.EmptyEnumerationMessage);
            await SeedAsync(connection, ct, "blocking", MinutesAgo(20), "SESSION_MISSING", "zzz session missing");

            /* #1851 made database_size_stats — a collector with no enumeration at all — the first
               NON-enumerating source of a note. The read must treat it identically: same newest-first rank
               over the same counted text, same qualifier, and still nowhere near last_error. A rank or a
               gate that had quietly assumed "notes come from enumerations" would pass every #1854 case
               above and fail here. */
            await SeedAsync(connection, ct, "database_size_stats", MinutesAgo(30), "SUCCESS", ProbeNote(12));
            await SeedAsync(connection, ct, "database_size_stats", MinutesAgo(20), "SUCCESS", ProbeNote(3));
            await SeedAsync(connection, ct, "database_size_stats", MinutesAgo(10), "SUCCESS", null);

            /* ── the Viewer's read (the Collection Health grid) ── */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var health = await viewer.GetCollectionHealthAsync(ServerId, ct);

                var queryStore = health.Single(h => h.CollectorName == "query_store");
                Assert.Equal(ProbeNote(12), queryStore.LastNote);
                Assert.Equal(3, queryStore.TotalRuns);
                Assert.Equal(2, queryStore.NoteCount);
                Assert.Equal(ProbeNote(12) + " (2 of 3 runs)", queryStore.NoteFormatted);

                var deadlocks = health.Single(h => h.CollectorName == "deadlocks");
                Assert.Equal("aaa newest failure", deadlocks.LastError);
                Assert.Equal(newestFailure.Ticks, deadlocks.LastErrorTime!.Value.Ticks);

                var fileIo = health.Single(h => h.CollectorName == "file_io");
                Assert.Null(fileIo.LastError);
                Assert.Equal(fileIoFailedAt.Ticks, fileIo.LastErrorTime!.Value.Ticks);
                Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, fileIo.LastNote);

                var blocking = health.Single(h => h.CollectorName == "blocking");
                Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, blocking.LastNote);
                Assert.Equal(1, blocking.NoteCount);

                var databaseSize = health.Single(h => h.CollectorName == "database_size_stats");
                Assert.Equal(ProbeNote(3), databaseSize.LastNote);
                Assert.Equal(ProbeNote(3) + " (2 of 3 runs)", databaseSize.NoteFormatted);
                Assert.Equal(2, databaseSize.NoteCount);
                Assert.Null(databaseSize.LastError);
            }

            /* ── the MCP service's read (get_collection_health, and the web dashboard's table behind it) ── */
            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var health = await DarlingDataReader.GetCollectionHealthAsync(
                    postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);

                var queryStore = health.Single(h => h.CollectorName == "query_store");
                Assert.Equal(ProbeNote(12), queryStore.LastNote);
                Assert.Equal(2, queryStore.NoteCount);

                Assert.Equal("aaa newest failure", health.Single(h => h.CollectorName == "deadlocks").LastError);
                Assert.Null(health.Single(h => h.CollectorName == "file_io").LastError);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task TheFleetRead_BandsWithoutExemplarText_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet collection-health test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* The fleet read is scoped to the CONFIG registry, not the collector-side servers table. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            await SeedAsync(connection, ct, FleetProbeCollector, MinutesAgo(30), "SUCCESS", ProbeNote(9));
            await SeedAsync(connection, ct, FleetProbeCollector, MinutesAgo(20), "SUCCESS", ProbeNote(12));

            await using var viewer = new ViewerDataService(cs!);
            var fleet = await viewer.GetFleetCollectionHealthAsync(ct);
            var row = fleet.Single(h => h.CollectorName == FleetProbeCollector);

            /* The fleet rollup answers "how many collectors, how many failing" for the status bar and
               nothing else, so it carries the BAND inputs and the cheap note COUNT but not the exemplar
               text: ranking a message per group turns a parallel hash aggregate into a serial sort of
               every row in the window. Blank, deliberately — never the pre-#1855 lexicographic MAX, which
               would have been the older run's "9 item(s)" on data where the truth is 12. */
            Assert.Null(row.LastNote);
            Assert.Null(row.LastError);
            Assert.Equal(2, row.NoteCount);
            Assert.Equal(2, row.TotalRuns);

            /* And the banding — the thing this read exists for — still works off the counts. */
            Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
            Assert.NotNull(row.LastSuccessTime);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>
    /// #2460, Darling half: the two duration statistics, against a REAL Postgres, on the population that
    /// motivated them. A source pin cannot tell PERCENTILE_DISC from AVG — both are valid SQL returning
    /// one number — and the whole finding is that one of those numbers describes no run that ever ran.
    ///
    /// <para>The fixture is prod-sql-use2-multi-49's query_store at 1/11.55 scale, same 83/17 shape: 83
    /// runs carrying the empty-enumeration note at the 36 ms prod-sql-use2-alpha-01 measurably pays for it,
    /// and 17 productive runs at the ~80,933 ms the store's own numbers force. Lite.Tests pins the
    /// IDENTICAL fixture and the identical expected values against live DuckDB, which is the parity that
    /// matters: the two engines were asked the same question and had to give the same answer, including
    /// on the NULL-duration row — an ordered-set aggregate ignoring nulls is engine behaviour neither
    /// query states, and a p95 that came back NULL would silently collapse to the mean.</para>
    /// </summary>
    [Fact]
    public async Task TheDurationStatistics_SplitABimodalCollector_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live duration-statistics test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            for (var i = 0; i < 83; i++)
            {
                await SeedDurationAsync(connection, ct, "query_store", MinutesAgo(600 - i), 36,
                    EnumeratedCollectorDriver.EmptyEnumerationMessage);
            }

            for (var i = 0; i < 17; i++)
            {
                await SeedDurationAsync(connection, ct, "query_store", MinutesAgo(400 - i), 80_933, null);
            }

            /* A failed run with no duration recorded. */
            await SeedDurationAsync(connection, ct, "query_store", MinutesAgo(300), null, null, "ERROR");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var health = await DarlingDataReader.GetCollectionHealthAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);
            var queryStore = health.Single(h => h.CollectorName == "query_store");

            Assert.Equal(101, queryStore.TotalRuns);
            Assert.Equal(83, queryStore.NoteCount);

            Assert.Equal(13_788.49, queryStore.AvgDurationMs, 2);
            Assert.Equal(80_933, queryStore.MaxDurationMs, 3);
            Assert.Equal(80_933, queryStore.P95DurationMs, 3);

            /* The finding, as an assertion: one number says this collector fits a body four times over,
               the other says one run of it does not fit at all, and both are honest about the same 101
               runs. Charged its mean, the aligned cycle called it 23% of a body; charged its heavy run,
               135% of one. */
            Assert.True(queryStore.AvgDurationMs < SweepPressureClassifier.SweepBudgetMs,
                $"the mean was {queryStore.AvgDurationMs} ms");
            Assert.True(queryStore.P95DurationMs > SweepPressureClassifier.SweepBudgetMs,
                $"a heavy run was {queryStore.P95DurationMs} ms");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ── helpers ── */

    /// <summary>Whole seconds so the assertions can compare ticks against what Postgres stored.</summary>
    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));


    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct,
        string collector, DateTime collectionTimeUtc, string status, string? message) =>
        SeedDurationAsync(connection, ct, collector, collectionTimeUtc, 100, message, status);

    /// <summary>#2460: the same seed with the run's own duration_ms spelled out, null included.</summary>
    private static async Task SeedDurationAsync(
        NpgsqlConnection connection, CancellationToken ct,
        string collector, DateTime collectionTimeUtc, int? durationMs, string? message, string status = "SUCCESS") =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), durationMs, status, message, 10, 80, 20);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
