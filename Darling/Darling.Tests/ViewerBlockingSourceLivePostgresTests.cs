/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5244: the desktop viewer's blocking reads, live. The Blocking Severity read (<c>GetBlockingDurationStatsAsync</c>) used to take no
/// database filter, so the Blocking Stats tab drew every database's blocking under a chosen filter while the Trends tab's count
/// followed it. It now binds the same <c>text[]</c> the trend binds, on both arms, and both reads tag each point with the arm that
/// answered ("blocked-process-report" or "DMV snapshot") so the charts can name it. The mixed server holds an XE report for A, and DMV
/// rows for A and B, so the answer flips with the filter: [A] and [A, B] and no filter read the XE arm, [B] alone reads the DMV arm.
/// Gated on DARLING_TEST_PG, serialized in the "live-postgres" collection, and it uses negative sentinel server ids and cleans up in
/// finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerBlockingSourceLivePostgresTests
{
    private const int MixedServerId = -972201;
    private const int XeOnlyServerId = -972202;
    private const int DmvOnlyServerId = -972203;
    private const int EmptyServerId = -972204;
    private static readonly int[] ServerIds = { MixedServerId, XeOnlyServerId, DmvOnlyServerId, EmptyServerId };

    private const string Xe = "blocked-process-report";
    private const string Dmv = "DMV snapshot";

    [Fact]
    public async Task BlockingSeverityAndTrend_FollowTheDatabaseFilter_AndNameTheArmThatAnswered_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking source test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAllAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var start = TruncateToMinute(DateTime.UtcNow.AddMinutes(-20));
            var end = start.AddMinutes(30);
            var m0 = start.AddMinutes(1);
            var m1 = start.AddMinutes(3);
            var m2 = start.AddMinutes(5);

            /* The mixed server: an XE report for A (1,000 ms), and DMV rows for B (7,000 ms) and for A (5,000 ms). */
            await InsertBlockedProcessReportAsync(connection, MixedServerId, m0, "A", 1000, ct);
            await InsertDmvBlockingSnapshotAsync(connection, MixedServerId, m1, "B", 7000, ct);
            await InsertDmvBlockingSnapshotAsync(connection, MixedServerId, m2, "A", 5000, ct);
            /* An XE-only server (A and B) and a DMV-only server (A). The empty server has no rows. */
            await InsertBlockedProcessReportAsync(connection, XeOnlyServerId, m0, "A", 1000, ct);
            await InsertBlockedProcessReportAsync(connection, XeOnlyServerId, m1, "B", 2000, ct);
            await InsertDmvBlockingSnapshotAsync(connection, DmvOnlyServerId, m1, "A", 3000, ct);

            /* [A]: the XE arm holds A's report, so it answers and A's DMV rows stay out. */
            var a = new[] { "A" };
            var statsA = await viewer.GetBlockingDurationStatsAsync(MixedServerId, start, end, a, ct);
            var onlyA = Assert.Single(statsA);
            Assert.Equal(1000, onlyA.TotalDurationMs);
            Assert.Equal(Xe, onlyA.Source);
            var trendA = await viewer.GetBlockingTrendAsync(MixedServerId, start, end, a, ct);
            Assert.Equal((1, Xe), Assert.Single(trendA.Select(p => (p.Count, p.Source!))));

            /* [B]: the XE arm holds nothing for B, so the DMV arm answers with B's row ALONE. Before #5244 the severity read ignored
               the filter and drew the XE row for A here. */
            var b = new[] { "B" };
            var statsB = await viewer.GetBlockingDurationStatsAsync(MixedServerId, start, end, b, ct);
            var onlyB = Assert.Single(statsB);
            Assert.Equal(7000, onlyB.TotalDurationMs);
            Assert.Equal(1, onlyB.EventCount);
            Assert.Equal(Dmv, onlyB.Source);
            var trendB = await viewer.GetBlockingTrendAsync(MixedServerId, start, end, b, ct);
            Assert.Equal((1, Dmv), Assert.Single(trendB.Select(p => (p.Count, p.Source!))));

            /* [A, B]: the XE arm holds A's report, so it answers and B's DMV-only event drops out: the case [B] alone showed. */
            var ab = new[] { "A", "B" };
            var statsAb = await viewer.GetBlockingDurationStatsAsync(MixedServerId, start, end, ab, ct);
            var onlyXe = Assert.Single(statsAb);
            Assert.Equal(1000, onlyXe.TotalDurationMs);
            Assert.Equal(Xe, onlyXe.Source);

            /* No filter (null, and an empty list): the XE arm answers. */
            Assert.Equal(Xe, Assert.Single(await viewer.GetBlockingDurationStatsAsync(MixedServerId, start, end, null, ct)).Source);
            Assert.Equal(Xe, Assert.Single(await viewer.GetBlockingDurationStatsAsync(MixedServerId, start, end, Array.Empty<string>(), ct)).Source);

            /* A filter that matches nothing on either arm: no rows, so no source to name. */
            Assert.Empty(await viewer.GetBlockingDurationStatsAsync(MixedServerId, start, end, new[] { "C" }, ct));
            Assert.Empty(await viewer.GetBlockingTrendAsync(MixedServerId, start, end, new[] { "C" }, ct));

            /* XE-only, DMV-only and empty servers: the tag follows the data, on both reads. */
            var xeOnlyStats = await viewer.GetBlockingDurationStatsAsync(XeOnlyServerId, start, end, null, ct);
            Assert.Equal(2, xeOnlyStats.Count);
            Assert.All(xeOnlyStats, p => Assert.Equal(Xe, p.Source));
            Assert.All(await viewer.GetBlockingTrendAsync(XeOnlyServerId, start, end, null, ct), p => Assert.Equal(Xe, p.Source));
            var xeOnlyB = Assert.Single(await viewer.GetBlockingDurationStatsAsync(XeOnlyServerId, start, end, b, ct));
            Assert.Equal(2000, xeOnlyB.TotalDurationMs);

            Assert.Equal(Dmv, Assert.Single(await viewer.GetBlockingDurationStatsAsync(DmvOnlyServerId, start, end, null, ct)).Source);
            Assert.Equal(Dmv, Assert.Single(await viewer.GetBlockingTrendAsync(DmvOnlyServerId, start, end, null, ct)).Source);
            Assert.Empty(await viewer.GetBlockingDurationStatsAsync(DmvOnlyServerId, start, end, b, ct));

            Assert.Empty(await viewer.GetBlockingDurationStatsAsync(EmptyServerId, start, end, null, ct));
            Assert.Empty(await viewer.GetBlockingTrendAsync(EmptyServerId, start, end, null, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteAllAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertBlockedProcessReportAsync(
        NpgsqlConnection connection, int serverId, DateTime eventTimeUtc, string database, long waitTimeMs, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
     blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-blocking-source-e2e");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc.AddSeconds(5), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(55);
        command.Parameters.AddWithValue(60);
        command.Parameters.AddWithValue(waitTimeMs);
        command.Parameters.AddWithValue("X");
        command.Parameters.AddWithValue("SELECT 1");
        command.Parameters.AddWithValue("UPDATE t SET c = 1");
        command.Parameters.AddWithValue("<blocked/>");
        command.Parameters.AddWithValue("dbo.t");
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDmvBlockingSnapshotAsync(
        NpgsqlConnection connection, int serverId, DateTime eventTimeUtc, string database, long waitTimeMs, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-blocking-source-e2e");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTimeUtc.AddSeconds(5), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(155);
        command.Parameters.AddWithValue(160);
        command.Parameters.AddWithValue(waitTimeMs);
        command.Parameters.AddWithValue("S");
        command.Parameters.AddWithValue("SELECT 2");
        command.Parameters.AddWithValue("UPDATE t SET c = 2");
        command.Parameters.AddWithValue("dbo.t");
        await command.ExecuteNonQueryAsync(ct);
    }

    /* Both reads bucket by DATE_TRUNC('minute', event_time); anchoring on a minute boundary keeps added seconds from splitting a bucket. */
    private static DateTime TruncateToMinute(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task DeleteAllAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var ids = string.Join(", ", ServerIds);
        foreach (var table in new[] { "blocked_process_reports", "dmv_blocking_snapshots" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
