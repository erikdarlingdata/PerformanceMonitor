/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3636: the alerting pass's file-growth read (#2349) carries the observation's identity. The read's growth
/// figure is a fact about two COLLECTIONS — the newest sample and the oldest inside the window — and the
/// <c>database_size_stats</c> collector lands one per hour, so the row reads byte-identical on every ~30 s alert
/// pass in between. Without the newest sample's <c>collection_time</c> on the row, the engine's rise arm re-fired
/// on every 5-minute cooldown against the same observation: up to twelve cards per growth event. #3579 gave the
/// forced-plan read its <c>observed_at</c> for the identical mechanism at a 5-minute cadence; this is the same
/// column on the sibling read.
///
/// <para>The ungated pins hold the statement's shape — the stamp is LAST, so the fourteen ordinals the reader
/// already binds do not move, and it is the CTE's own <c>collection_time</c> rather than a new source column.
/// The gated one seeds two hourly collections on a live store and reads them back through the real Npgsql path:
/// the growth is the difference, the window is the measured span, and the stamp is the newest collection's
/// clock to the tick, Kind Utc — the engine compares one file's stamps for equality, so a stamp that came back
/// shifted or truncated would either never match (cooldown-repeat returns) or match a different collection.</para>
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn
   cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DatabaseFileGrowthReadTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -363636;
    private static readonly string TestServerKey = TestServerId.ToString(CultureInfo.InvariantCulture);
    private const string TestServerName = "file-growth-read-e2e";

    /* ---------------- ungated shape pins ---------------- */

    /// <summary>
    /// The observation stamp is the LAST column of the shipped read and is <c>c.collection_time</c> — the newest
    /// sample's collector clock, which <c>current_files</c> already selected for the window-width arithmetic and
    /// never projected. Last, because the reader binds ordinals 0–13 to the fourteen pre-#3636 columns and an
    /// inserted column would silently shift every one of them onto its neighbour's type. Pinned by splitting the
    /// final select list into its lines (one column per line; several carry <c>COALESCE(a, b)</c>, so a comma
    /// split would over-count) rather than by substring, so a column appended AFTER it would fail here too.
    /// </summary>
    [Fact]
    public void TheObservationStamp_IsTheLastColumn_AndIsTheNewestSamplesCollectionTime()
    {
        var sql = DarlingAlertReadAdapter.DatabaseFileGrowthSql;
        var selectStart = sql.LastIndexOf("SELECT", StringComparison.Ordinal);
        var fromStart = sql.IndexOf("FROM current_files c", StringComparison.Ordinal);
        Assert.True(selectStart >= 0 && fromStart > selectStart, "the final select list was not found");

        var columns = sql[(selectStart + "SELECT".Length)..fromStart]
            .Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal(15, columns.Length);
        Assert.Equal("c.max_size_mb,", columns[13]);
        Assert.Equal("c.collection_time AS observed_at", columns[14]);

        /* Not a new source column: the CTE selected collection_time before #3636 (the window width needs it). */
        var cte = sql[..sql.IndexOf("baseline AS", StringComparison.Ordinal)];
        Assert.Contains("file_type_desc, collection_time,", cte, StringComparison.Ordinal);
    }

    /// <summary>The read stays a raw-table, parameter-bound-clock statement like every other alert feed: no bare
    /// <c>now()</c> (timestamptz against the naive-UTC columns) and never Lite's <c>v_</c> view.</summary>
    [Fact]
    public void TheRead_TargetsTheRawTable_AndBindsItsClock()
    {
        var sql = DarlingAlertReadAdapter.DatabaseFileGrowthSql;
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("FROM v_", sql, StringComparison.Ordinal);
        Assert.Contains("FROM database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    /* ---------------- gated: the round trip ---------------- */

    /// <summary>
    /// Two hourly collections of one file plus a single-sample neighbour, read back through the real adapter.
    /// The stamp is the NEWEST collection's <c>collection_time</c>, tick-equal to what was seeded (the store
    /// holds naive UTC; the adapter names the Kind and never shifts the value), a re-read between collections
    /// returns the same stamp, and a third collection moves it.
    /// </summary>
    [Fact]
    public async Task TheStamp_IsTheNewestCollectionsClock_ToTheTick_AndMovesWithTheNextCollection()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live file-growth read test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(postgres);

        var bodySucceeded = false;
        try
        {
            /* Floored to whole microseconds (#3579's lesson): PostgreSQL timestamp is microsecond-resolution and
               .NET ticks are 100 ns, so a raw UtcNow does not survive the round trip and the tick-equality below
               would fail on any clock that is not itself microsecond-aligned. Kind Unspecified: naive-UTC storage. */
            var rawNow = DateTime.UtcNow;
            var utcNow = DateTime.SpecifyKind(new DateTime(rawNow.Ticks - (rawNow.Ticks % 10)), DateTimeKind.Unspecified);
            var collection0 = utcNow.AddMinutes(-70);
            var collection1 = utcNow.AddMinutes(-10);

            /* tempdev: 100 GB at the top of the hour, 120 GB an hour later. templog: one sample only. */
            await SeedFileAsync(connection, ct, 1L, collection0, "tempdb", "tempdev", 102_400m);
            await SeedFileAsync(connection, ct, 2L, collection1, "tempdb", "tempdev", 122_880m);
            await SeedFileAsync(connection, ct, 2L, collection1, "tempdb", "templog", 4_096m);

            var files = await adapter.GetDatabaseFileGrowthAsync(TestServerKey, lookbackMinutes: 120, ct);
            Assert.Equal(2, files.Count);

            var tempdev = Assert.Single(files, f => f.FileName == "tempdev");
            Assert.Equal(122_880d, tempdev.TotalSizeMb, precision: 3);
            Assert.Equal(20_480d, tempdev.GrowthMb, precision: 3);
            Assert.Equal(60d, tempdev.GrowthWindowMinutes, precision: 3);
            /* #3636: the observation's identity is the newest sample's collection_time, to the tick, Kind Utc. */
            Assert.Equal((DateTime?)collection1, tempdev.ObservedAtUtc);
            Assert.Equal(DateTimeKind.Utc, tempdev.ObservedAtUtc!.Value.Kind);

            /* The single-sample neighbour: no rise observed, and its own stamp all the same. */
            var templog = Assert.Single(files, f => f.FileName == "templog");
            Assert.Equal(0d, templog.GrowthMb, precision: 3);
            Assert.Equal(0d, templog.GrowthWindowMinutes, precision: 3);
            Assert.Equal((DateTime?)collection1, templog.ObservedAtUtc);

            /* Re-reading between collections is the same observation: same row, same stamp. This is the read
               the engine used to fire on twelve times; the stamp is what lets it recognise the repeat. */
            var reread = await adapter.GetDatabaseFileGrowthAsync(TestServerKey, lookbackMinutes: 120, ct);
            Assert.Equal(tempdev.ObservedAtUtc, Assert.Single(reread, f => f.FileName == "tempdev").ObservedAtUtc);

            /* The next collection lands: the stamp moves with it. */
            var collection2 = utcNow.AddMinutes(-2);
            await SeedFileAsync(connection, ct, 3L, collection2, "tempdb", "tempdev", 122_880m);
            var after = await adapter.GetDatabaseFileGrowthAsync(TestServerKey, lookbackMinutes: 120, ct);
            Assert.Equal((DateTime?)collection2, Assert.Single(after, f => f.FileName == "tempdev").ObservedAtUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task SeedFileAsync(
        NpgsqlConnection connection, CancellationToken ct,
        long collectionId, DateTime collectionTime, string databaseName, string fileName, decimal totalSizeMb)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb,
     volume_mount_point, volume_total_mb, volume_free_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb)
VALUES ($1, $2, $3, $4, $5, 2, $6, $7, $8, $9, $10, $11, 'D:\', 4096000, 3000000, 1024, false, NULL, -1)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(fileName == "tempdev" ? 1 : 2);
        command.Parameters.AddWithValue(fileName == "tempdev" ? "ROWS" : "LOG");
        command.Parameters.AddWithValue(fileName);
        command.Parameters.AddWithValue(@"D:\data\" + fileName);
        command.Parameters.AddWithValue(totalSizeMb);
        command.Parameters.AddWithValue(totalSizeMb * 0.8m);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM database_size_stats WHERE server_id = {TestServerId}", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
