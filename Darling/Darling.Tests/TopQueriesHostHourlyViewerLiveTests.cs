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
using PerformanceMonitor.Darling.Viewer;
using Darling.Tests;
using Xunit;
using static PerformanceMonitor.Darling.Tests.TopQueriesHostHourlyLiveTests;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: each test mints a scratch database through TopQueriesHostHourlyLiveTests.WithStoreAsync (a ScratchPostgres), because it alters
   the rollup and deletes its ledger rows, which no other class sharing the store may see. */
/// <summary>
/// #9164 part B, live: the Viewer's Top Queries page over the seed and store of <see cref="TopQueriesHostHourlyLiveTests"/>. The page ranked
/// from the hourly host rollup is the raw page row for row, and a hole in the rollup sends the read to raw. The tests name the Viewer, which
/// the build without the Viewer leaves out (#9686), so they are a class of their own and the rest of that class runs on Linux.
/// </summary>
public sealed class TopQueriesHostHourlyViewerLiveTests
{
    [Fact]
    public Task TheViewersTopQueries_AreTheRawPageRowForRow_WhenRankedFromTheRollup() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* A restart row (interval 0) in a whole hour, after the build: raw's ranking sums it, the rollup's sums leave it out, so the
               statement reads such rows from raw. It outweighs every other statement, so it must lead the page. */
            var at = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(12).AddMinutes(30);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count,
                                                 delta_worker_time, delta_elapsed_time, sample_interval_seconds)
                VALUES (916430000001, $1, {ServerId}, '{ServerName}', 'dbV', 'V_RESTART', 1, 1, 9000000000, 0)
                """, at);

            var routed = await ViewerPageAsync(viewer, start, end, ct);
            Assert.Contains("V_RESTART", routed);
            Assert.Contains("H003", routed);

            /* The rollup must be what ranked it: twelve statements the rollup calls enormous crowd the real top five (H003 among them, ranked there by its restart rows) out of the candidates. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE collect.query_stats_host_hourly SET total_elapsed_us = 1000000000000 WHERE query_hash >= 'H029'");
            Assert.DoesNotContain("H003", await ViewerPageAsync(viewer, start, end, ct));

            /* With the build rows gone the read is the raw statement, and its page is the page the rollup ranked before it was altered. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built");
            Assert.Equal(await ViewerPageAsync(viewer, start, end, ct), routed);
        });

    [Fact]
    public Task TheViewersTopQueries_WithAHoleInTheRollup_ReadRawWithTheRawAnswer() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var whole = await ViewerPageAsync(viewer, start, end, ct);

            /* An hour with no build sends the read to raw: an altered rollup changes nothing, and the answer is the same page. */
            var hour = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0).AddHours(9);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_host_hourly_built WHERE server_id = $1 AND bucket = $2", ServerId, hour);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE collect.query_stats_host_hourly SET total_elapsed_us = 1000000000000 WHERE query_hash >= 'H029'");
            Assert.Equal(whole, await ViewerPageAsync(viewer, start, end, ct));
        });

    /// <summary>The Viewer's Top Queries page as JSON, every column of every row, in page order.</summary>
    private static async Task<string> ViewerPageAsync(ViewerDataService viewer, DateTime start, DateTime end, CancellationToken ct)
    {
        /* Every plain column of every row (the row also carries a tag-label member that is not data). */
        var columns = typeof(ViewerQueryStatsRow).GetProperties()
            .Where(p => (Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType) is { } t && (t.IsPrimitive || t == typeof(string) || t == typeof(DateTime) || t == typeof(decimal)))
            .ToArray();
        var rows = await viewer.GetTopQueriesByCpuAsync(ServerId, start, end, 5, null, null, ct);
        return JsonSerializer.Serialize(rows.Select(r => columns.ToDictionary(c => c.Name, c => c.GetValue(r))));
    }
}
