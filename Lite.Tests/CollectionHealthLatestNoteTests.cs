/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1855, Lite half: the Collection Health grid's exemplar columns must show the message from the LATEST
/// run that left one, not <c>MAX()</c>'s lexicographically greatest string.
///
/// <para>#1854 shipped both columns as <c>MAX(CASE ... THEN error_message END)</c>, which is the greatest
/// STRING in the window rather than the most recent one. That was harmless while every message was a
/// stable failure-mode sentence, and #1837 broke the assumption by introducing the first note carrying a
/// NUMBER: text does not sort like a count, so "12 item(s)" sorts BELOW "9 item(s)" and a collector whose
/// probe-failure count varied cycle to cycle displayed an arbitrary earlier run's number.</para>
///
/// <para>These run against a REAL DuckDB through the real reader — a source pin cannot tell a lexicographic
/// MAX from a newest-first rank, because both are valid SQL that returns one string. Darling.Tests pins the
/// same expectations against live Postgres on the byte-identical query shape.</para>
/// </summary>
public sealed class CollectionHealthLatestNoteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4242;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthLatestNoteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>The real probe-failure note for a count, from the shared driver — the text the defect rides on.</summary>
    private static string ProbeNote(int count) =>
        string.Format(CultureInfo.InvariantCulture, EnumeratedCollectorDriver.ProbeFailureNoteFormat, count);

    [Fact]
    public async Task LastNote_IsTheNewestNote_NotTheLexicographicallyGreatestOne()
    {
        var service = new LocalDataService(_duckDb);

        /* The exact shape of #1855: the LATER run reports 12, the earlier one 9. "12 item(s)" sorts below
           "9 item(s)", so a value-MAX returns the OLDER run's count — a number the operator has no way to
           know is stale, on a column whose whole job is to say what the last cycle found. */
        await SeedAsync("query_store", MinutesAgo(30), "SUCCESS", ProbeNote(9));
        await SeedAsync("query_store", MinutesAgo(20), "SUCCESS", ProbeNote(12));

        var row = await ReadAsync(service, "query_store");

        Assert.Equal(ProbeNote(12), row.LastNote);
        Assert.Equal(2, row.NoteCount);
    }

    [Fact]
    public async Task LastNote_SurvivesALaterCleanRun()
    {
        var service = new LocalDataService(_duckDb);

        /* The note is the newest one the WINDOW holds, not the newest successful run's message. Ranking by
           time alone would blank the column the moment one cycle came back clean, and take the "(2 of 3
           runs)" qualifier down with it — the qualifier exists precisely to describe a window that is only
           SOMETIMES empty. */
        await SeedAsync("query_store", MinutesAgo(30), "SUCCESS", ProbeNote(9));
        await SeedAsync("query_store", MinutesAgo(20), "SUCCESS", ProbeNote(12));
        await SeedAsync("query_store", MinutesAgo(10), "SUCCESS", null);

        var row = await ReadAsync(service, "query_store");

        Assert.Equal(ProbeNote(12), row.LastNote);
        Assert.Equal(3, row.TotalRuns);
        Assert.Equal(2, row.NoteCount);
        Assert.Equal(ProbeNote(12) + " (2 of 3 runs)", row.NoteFormatted);
    }

    [Fact]
    public async Task LastError_IsTheNewestFailuresMessage_NotTheLexicographicallyGreatestOne()
    {
        var service = new LocalDataService(_duckDb);

        /* The sibling column has always had the same defect; it just never bit, because a failure mode's
           text is stable. Seeded so lexicographic order and time order DISAGREE: "zzz" is the greatest
           string, "aaa" is the newest failure. */
        /* Held in locals: MinutesAgo truncates to the second, so re-deriving the same instant at assert
           time is a coin flip on whether the clock crossed a second boundary in between. */
        var older = MinutesAgo(30);
        var newest = MinutesAgo(20);
        await SeedAsync("deadlocks", older, "ERROR", "zzz older failure");
        await SeedAsync("deadlocks", newest, "ERROR", "aaa newest failure");

        var row = await ReadAsync(service, "deadlocks");

        Assert.Equal("aaa newest failure", row.LastError);
        Assert.Equal(newest.Ticks, row.LastErrorTime!.Value.Ticks);
    }

    [Fact]
    public async Task ANoteNeverLeaksIntoLastError_EvenWhenNoFailureCarriedText()
    {
        var service = new LocalDataService(_duckDb);

        /* The status re-check on the rank-1 row, which is the whole reason it is there. With no failing run
           carrying text, the error rank falls through to the newest row of any class — here a SUCCESS row
           holding a note. Reading that as the last error would turn a quiet enumeration into a fake
           failure, the exact regression #1837's SUCCESS gating exists to prevent.

           last_error_time still names the failure, because "when did this last fail" is about the RUN. */
        var failedAt = MinutesAgo(30);
        await SeedAsync("file_io", failedAt, "ERROR", null);
        await SeedAsync("file_io", MinutesAgo(20), "SUCCESS", EnumeratedCollectorDriver.EmptyEnumerationMessage);

        var row = await ReadAsync(service, "file_io");

        Assert.Null(row.LastError);
        Assert.Equal(failedAt.Ticks, row.LastErrorTime!.Value.Ticks);
        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, row.LastNote);
    }

    [Fact]
    public async Task TheNoteStaysGatedOnSuccess_NotOnMessagePresence()
    {
        var service = new LocalDataService(_duckDb);

        /* #1854's gate, re-asserted through the rank: a message on a non-SUCCESS row is not a note however
           new it is. SESSION_MISSING is a real capture fault with its own alert, and SKIPPED is a no-op
           run; neither belongs in a column whose tooltip promises it is not an error. */
        await SeedAsync("blocking", MinutesAgo(30), "SUCCESS", EnumeratedCollectorDriver.EmptyEnumerationMessage);
        await SeedAsync("blocking", MinutesAgo(20), "SESSION_MISSING", "zzz session missing");
        await SeedAsync("blocking", MinutesAgo(10), "SKIPPED", "zzz skipped");

        var row = await ReadAsync(service, "blocking");

        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, row.LastNote);
        Assert.Equal(1, row.NoteCount);
        Assert.Null(row.LastError);
    }

    [Fact]
    public async Task APayloadCollectorsProbeNoteRanksAndReadsLikeAnEnumerationsNote()
    {
        var service = new LocalDataService(_duckDb);

        /* #1851 made database_size_stats — a collector with no enumeration at all — the first NON-enumerating
           source of a note. The read must treat it identically: same newest-first rank over the same
           counted text, same qualifier, and still nowhere near last_error, because the note rides a SUCCESS
           row and every band and count keys on status. A rank or a gate that had quietly assumed "notes come
           from enumerations" would pass every #1854 test and fail here. */
        await SeedAsync("database_size_stats", MinutesAgo(30), "SUCCESS", ProbeNote(12));
        await SeedAsync("database_size_stats", MinutesAgo(20), "SUCCESS", ProbeNote(3));
        await SeedAsync("database_size_stats", MinutesAgo(10), "SUCCESS", null);

        var row = await ReadAsync(service, "database_size_stats");

        Assert.Equal(ProbeNote(3), row.LastNote);
        Assert.Equal(ProbeNote(3) + " (2 of 3 runs)", row.NoteFormatted);
        Assert.Equal(2, row.NoteCount);
        Assert.Null(row.LastError);
    }

    [Fact]
    public async Task OneCollectorsNewestNoteDoesNotBecomeAnothers()
    {
        var service = new LocalDataService(_duckDb);

        /* The ranking is partitioned per collector, so the fleet-wide newest note must not win everywhere.
           Seeded so the OTHER collector's note is both newer and lexicographically greater — a rank that
           forgot to partition, and a MAX that never did, would both hand it to query_store. */
        await SeedAsync("query_store", MinutesAgo(30), "SUCCESS", ProbeNote(12));
        await SeedAsync("wait_stats", MinutesAgo(10), "SUCCESS", "zzz a different collector's note");

        var health = await service.GetCollectionHealthAsync(ServerId);

        Assert.Equal(ProbeNote(12), health.Single(h => h.CollectorName == "query_store").LastNote);
        Assert.Equal("zzz a different collector's note", health.Single(h => h.CollectorName == "wait_stats").LastNote);
    }

    /// <summary>
    /// #2460, Lite half: the two duration statistics, against a REAL DuckDB, on the population that
    /// motivated them. A source pin cannot tell PERCENTILE_DISC from AVG — both are valid SQL returning
    /// one number — and the whole finding is that one of those numbers describes no run that ever ran.
    ///
    /// <para>The fixture is prod-sql-use2-multi-49's query_store at 1/11.55 scale, same 83/17 shape: 83
    /// runs carrying the empty-enumeration note at the 36 ms prod-sql-use2-alpha-01 measurably pays for it,
    /// and 17 productive runs at the ~80,933 ms the store's own numbers force. The assertion that matters
    /// is the last pair: the MEAN sits comfortably inside a 60,000 ms sweep budget while a heavy run costs
    /// more than the whole budget by itself, which is the sentence the mean alone could never produce.
    /// Darling.Tests pins the identical fixture against live Postgres — two engines, one answer.</para>
    /// </summary>
    [Fact]
    public async Task TheDurationStatistics_SplitABimodalCollectorTheMeanBlendsAway()
    {
        var service = new LocalDataService(_duckDb);

        for (var i = 0; i < 83; i++)
        {
            await SeedDurationAsync("query_store", MinutesAgo(600 - i), 36,
                EnumeratedCollectorDriver.EmptyEnumerationMessage);
        }

        for (var i = 0; i < 17; i++)
        {
            await SeedDurationAsync("query_store", MinutesAgo(400 - i), 80_933, null);
        }

        /* A failed run with no duration recorded: the new aggregates must ignore it exactly as AVG
           already does, or a collector that errors occasionally reports a NULL p95 and silently falls
           back to its mean. Verified here rather than assumed, because the two engines had to agree. */
        await SeedDurationAsync("query_store", MinutesAgo(300), null, null, "ERROR");

        var row = await ReadAsync(service, "query_store");

        Assert.Equal(101, row.TotalRuns);
        Assert.Equal(83, row.NoteCount);

        Assert.Equal(13_788.49, row.AvgDurationMs, 2);
        Assert.Equal(80_933, row.MaxDurationMs, 3);
        Assert.Equal(80_933, row.P95DurationMs, 3);

        /* The finding, as an assertion: one number says this collector fits a body four times over, the
           other says one run of it does not fit at all, and both are honest about the same 101 runs. */
        Assert.True(row.AvgDurationMs < SweepPressureClassifier.SweepBudgetMs,
            $"the mean was {row.AvgDurationMs} ms");
        Assert.True(row.P95DurationMs > SweepPressureClassifier.SweepBudgetMs,
            $"a heavy run was {row.P95DurationMs} ms");
    }

    /* ── helpers ── */

    private async Task<CollectorHealthRow> ReadAsync(LocalDataService service, string collector) =>
        (await service.GetCollectionHealthAsync(ServerId)).Single(h => h.CollectorName == collector);

    /// <summary>Whole seconds so the assertions can compare ticks against what DuckDB stored.</summary>
    private static DateTime MinutesAgo(int minutes)
    {
        var t = DateTime.UtcNow.AddMinutes(-minutes);
        return new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private Task SeedAsync(string collector, DateTime collectionTimeUtc, string status, string? message) =>
        SeedDurationAsync(collector, collectionTimeUtc, 100, message, status);

    /// <summary>#2460: the same seed with the run's own duration_ms spelled out, null included.</summary>
    private async Task SeedDurationAsync(
        string collector, DateTime collectionTimeUtc, int? durationMs, string? message, string status = "SUCCESS")
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)durationMs ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)message ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }
}
