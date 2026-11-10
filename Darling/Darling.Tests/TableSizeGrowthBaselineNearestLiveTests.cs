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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The table-size read resolves each nominal-window baseline as the snapshot nearest its cutoff within a day either side,
/// the rule the storage-growth read applies to database baselines, over fixed snapshot times and fixed cutoffs. A daily
/// collector that missed its run at the 7-day or the 30-day mark still yields a figure, from the snapshot a day inside the
/// mark; a snapshot a minute after the cutoff is the baseline as one a minute before is, so the figure does not turn on
/// which side of the cutoff a run's stamp fell; and a gap wider than a day on both sides of a cutoff yields no baseline,
/// with the history note saying no snapshot lies within a day, not that none is that old.
/// </summary>
[Collection("live-postgres")]
public sealed class TableSizeGrowthBaselineNearestLiveTests
{
    private const string ServerName = "darling-growth-nearest-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "Hist";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* The collector runs at 06:00 each day; the read happens at 14:00 on the latest snapshot's day. */
    private static readonly DateTime Latest = new(2026, 11, 9, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Read = new(2026, 11, 9, 14, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Cutoff7d = Read.AddDays(-7);
    private static readonly DateTime Cutoff30d = Read.AddDays(-30);

    [Fact]
    public async Task EachBaseline_IsTheSnapshotNearestItsCutoffWithinADay()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live growth-baseline test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* One missed run at each mark: the 7-day cutoff (2 November 14:00) has the 3 November snapshot 16 hours after it
               and the 1 November snapshot 32 hours before, so the nearer, six days before the latest, is the baseline; the
               same a day inside the 30-day mark. The newest snapshot at or before either cutoff would be a day outside it. */
            await SeedAsync(connection, ct, Enumerable.Range(0, 41).Where(age => age != 7 && age != 30).Select(age => (age, Latest.AddDays(-age))));
            var row = await ReadAsync(postgres, ct);
            Assert.Equal(Latest.AddDays(-6), row.Snapshot7dTime);
            Assert.Equal(Latest.AddDays(-29), row.Snapshot30dTime);
            Assert.True(row.Covers7d);
            Assert.True(row.Covers30d);
            Assert.Equal(18, row.Growth7dMb);
            Assert.Equal(87, row.Growth30dMb);
            Assert.Null(DarlingMcpObjectStatsTools.GrowthNote(row));
            Assert.Null(DarlingMcpObjectStatsTools.HistoryNote(row));

            /* The run at the 7-day mark started a minute late, a minute after the cutoff: it is still the 7-day baseline, as it
               is when it started a minute early. */
            foreach (var jitter in new[] { TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(-1) })
            {
                await DeleteRowsAsync(connection, ct, keepServer: true);
                await SeedAsync(connection, ct, Enumerable.Range(0, 41).Select(age => (age, age == 7 ? Cutoff7d + jitter : Latest.AddDays(-age))));
                row = await ReadAsync(postgres, ct);
                Assert.Equal(Cutoff7d + jitter, row.Snapshot7dTime);
                Assert.True(row.Covers7d);
                Assert.Equal(21, row.Growth7dMb);
                Assert.Equal(Latest.AddDays(-30), row.Snapshot30dTime);
                Assert.Equal(90, row.Growth30dMb);
            }

            /* Nothing within a day of the 7-day cutoff on either side (snapshots up to 4 days old, then from 10 days back):
               the 7-day baseline is null and the note says why. The 30-day baseline is 8 hours before its cutoff and stands. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            await SeedAsync(connection, ct, Enumerable.Range(0, 41).Where(age => age <= 4 || age >= 10).Select(age => (age, Latest.AddDays(-age))));
            row = await ReadAsync(postgres, ct);
            Assert.Null(row.Snapshot7dTime);
            Assert.Null(row.Growth7dMb);
            Assert.False(row.Covers7d);
            Assert.Equal(Latest.AddDays(-30), row.Snapshot30dTime);
            Assert.Equal(90, row.Growth30dMb);
            Assert.Equal(40, row.DaysOfData);
            var note = DarlingMcpObjectStatsTools.HistoryNote(row)!;
            Assert.Contains("40 days of index snapshots", note, StringComparison.Ordinal);
            Assert.Contains("no snapshot lies within a day of 7 days ago", note, StringComparison.Ordinal);
            Assert.Contains("growth_7d_mb is null", note, StringComparison.Ordinal);
            Assert.Contains("no snapshot lies within a day of 7 days ago", DarlingMcpObjectStatsTools.GrowthNote(row), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<TableSizeGrowthRow> ReadAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var rows = await DarlingTableSizeGrowthReader.GetAsync(
            postgres, ServerId, Cutoff7d, Cutoff30d, DarlingTableSizeGrowthReader.TopTables + 1, DatabaseFilter.All, commandTimeoutSeconds: 30, ct);
        return Assert.Single(rows);
    }

    /// <summary>TableA grows 3 MB a day: 100 MB at the run forty days before <see cref="Latest"/>, 220 MB at the latest. Each run's
    /// size follows its day, whatever minute its stamp fell on.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, IEnumerable<(int Age, DateTime Stamp)> runs)
    {
        foreach (var (age, t) in runs)
        {
            var reservedMb = 100m + 3m * (40 - age);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "dbo", 1, "TableA", 1, "PK_TableA", "CLUSTERED",
                reservedMb, reservedMb - 1, 1000L, 1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct, bool keepServer = false)
    {
        var sql = $"DELETE FROM index_object_stats WHERE server_id = {ServerId};";
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
