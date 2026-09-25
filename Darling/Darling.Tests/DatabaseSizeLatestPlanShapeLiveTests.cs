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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live #4245 regression: the two-step probe-then-bind shape (<c>GetLatestDatabaseSizeSnapshotAsync</c> /
/// <c>GetDatabaseSizeSnapshotAtOrBeforeAsync</c> in the viewer, <c>GetLatestSnapshotTimeAsync</c> in the MCP
/// reader) returns the SAME rows the pre-#4245 unbounded correlated <c>MAX(collection_time)</c> shape did,
/// for the four server shapes the fix's ruling distinguishes: an actively-collecting server (the windowed
/// probe hits), one whose newest snapshot is ~20 days old (misses the 2-day window, exercises the
/// fallback probe), one with zero rows (both probes return null, main read never runs), and one whose
/// newest snapshot is stamped 5 minutes into the future (the collector host's clock ran ahead of this
/// reader's — the old unbounded MAX always found it; the windowed-probe replacement must too, which is
/// exactly the "latest read takes no upper bound" ruling this PR's checkpoint commit added).
///
/// <para>Oracle: the pre-#4245 raw SQL (git 55b21e42^), not the current production constants — an
/// unqualified <c>MAX(collection_time)</c> has no failure mode to reproduce; it is the ground truth every
/// later shape must match, including the future-stamped snapshot.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseSizeLatestPlanShapeLiveTests
{
    private const string CurrentName = "darling-4245-latest-current";
    private const string StaleName = "darling-4245-latest-stale20d";
    private const string EmptyName = "darling-4245-latest-empty";
    private const string FutureName = "darling-4245-latest-future5m";

    private static readonly int CurrentId = ServerIdHelper.GetDeterministicHashCode(CurrentName);
    private static readonly int StaleId = ServerIdHelper.GetDeterministicHashCode(StaleName);
    private static readonly int EmptyId = ServerIdHelper.GetDeterministicHashCode(EmptyName);
    private static readonly int FutureId = ServerIdHelper.GetDeterministicHashCode(FutureName);

    private static readonly (string Name, int Id)[] AllServers =
    [
        (CurrentName, CurrentId), (StaleName, StaleId), (EmptyName, EmptyId), (FutureName, FutureId),
    ];

    [Fact]
    public async Task TwoStepReads_ReturnTheOldUnboundedReadsRows_ForEveryServerShape()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #4245 read-equality test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DeleteRowsAsync(connection, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            foreach (var (name, id) in AllServers)
                await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);

            await SeedAsync(connection, CurrentId, CurrentName, [now.AddDays(-3), now.AddDays(-1), now], ct);
            await SeedAsync(connection, StaleId, StaleName, [now.AddDays(-25), now.AddDays(-22), now.AddDays(-20)], ct);
            await SeedAsync(connection, FutureId, FutureName, [now.AddDays(-3), now.AddDays(-1), now.AddMinutes(5)], ct);
            // EmptyId: registered, zero database_size_stats rows.

            await using var viewer = new ViewerDataService(cs!);
            foreach (var (name, id) in AllServers)
            {
                var oldLatest = await ReadOldLatestAsync(connection, id, ct);
                var newLatest = FormatViewerLatest(await viewer.GetDatabaseSizeLatestAsync(id, ct));
                Assert.Equal(oldLatest, newLatest);

                var oldSummary = await ReadOldSummaryAsync(connection, id, ct);
                var newSummary = FormatSummary(await viewer.GetDatabaseSizeSummaryAsync(id, 10, ct));
                Assert.Equal(oldSummary, newSummary);

                var oldGrowth = await ReadOldGrowthAsync(connection, id, now, ct);
                var newGrowth = FormatGrowth(await viewer.GetStorageGrowthAsync(id, ct));
                Assert.Equal(oldGrowth, newGrowth);

                var oldMcp = await ReadOldMcpLatestAsync(connection, id, ct);
                var newMcp = await DarlingObjectStatsReader.GetLatestDatabaseSizesAsync(postgres, id, ct);
                Assert.Equal(oldMcp, newMcp);
            }

            /* Not vacuous: the current server actually has rows, the empty one actually has none, and the
               future server's resolved row really does carry a collection_time after "now" — the case a
               windowed probe or fallback bounded by DateTime.UtcNow would silently drop. */
            Assert.NotEmpty(await viewer.GetDatabaseSizeLatestAsync(CurrentId, ct));
            Assert.Empty(await viewer.GetDatabaseSizeLatestAsync(EmptyId, ct));
            var futureRows = await viewer.GetDatabaseSizeLatestAsync(FutureId, ct);
            Assert.NotEmpty(futureRows);
            var futureMcp = await DarlingObjectStatsReader.GetLatestDatabaseSizesAsync(postgres, FutureId, ct);
            Assert.All(futureMcp, r => Assert.True(r.CollectionTime > now, $"expected {r.CollectionTime:o} after {now:o}"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    // ---- old (pre-#4245, git 55b21e42^) oracle SQL, read via raw ADO ----

    private const string OldViewerLatestSql = @"
SELECT database_name, file_type_desc, file_name, total_size_mb, used_size_mb, volume_mount_point,
       volume_total_mb, volume_free_mb, recovery_model_desc, auto_growth_mb, is_percent_growth, growth_pct, vlf_count
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1)
ORDER BY total_size_mb DESC, database_name, file_type_desc, file_name";

    private const string OldSummarySql = @"
SELECT database_name, SUM(total_size_mb) AS total_mb, SUM(used_size_mb) AS used_mb
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1)
GROUP BY database_name
ORDER BY total_mb DESC
LIMIT $2";

    private const string OldGrowthSql = @"
WITH latest AS (
    SELECT database_name, SUM(total_size_mb) AS current_size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1)
    GROUP BY database_name
),
past_7d AS (
    SELECT database_name, SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1 AND collection_time <= $2)
    GROUP BY database_name
),
past_30d AS (
    SELECT database_name, SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1 AND collection_time <= $3)
    GROUP BY database_name
)
SELECT l.database_name, l.current_size_mb, p7.size_mb, p30.size_mb,
       l.current_size_mb - COALESCE(p7.size_mb, l.current_size_mb) AS growth_7d_mb,
       l.current_size_mb - COALESCE(p30.size_mb, l.current_size_mb) AS growth_30d_mb,
       CASE WHEN p30.size_mb IS NOT NULL THEN (l.current_size_mb - p30.size_mb) / 30.0
            WHEN p7.size_mb IS NOT NULL THEN (l.current_size_mb - p7.size_mb) / 7.0 ELSE 0 END AS daily_growth_rate_mb,
       CASE WHEN p30.size_mb IS NOT NULL AND p30.size_mb > 0
            THEN (l.current_size_mb - p30.size_mb) * 100.0 / p30.size_mb ELSE 0 END AS growth_pct_30d
FROM latest l
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name
ORDER BY growth_30d_mb DESC";

    private const string OldMcpLatestSql = """
        SELECT collection_time, database_name, file_name, file_type_desc,
               CAST(total_size_mb AS double precision) AS total_size_mb,
               CAST(used_size_mb AS double precision) AS used_size_mb,
               CAST(auto_growth_mb AS double precision) AS auto_growth_mb,
               CAST(max_size_mb AS double precision) AS max_size_mb,
               volume_mount_point,
               CAST(volume_total_mb AS double precision) AS volume_total_mb,
               CAST(volume_free_mb AS double precision) AS volume_free_mb
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1)
        ORDER BY database_name, file_type_desc, file_name
        """;

    private static async Task<List<string>> ReadOldLatestAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(OldViewerLatestSql, connection);
        cmd.Parameters.AddWithValue(serverId);
        var rows = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|",
                reader.GetString(0), reader.GetString(1), reader.GetString(2),
                reader.GetDecimal(3).ToString(CultureInfo.InvariantCulture),
                reader.IsDBNull(4) ? "" : reader.GetDecimal(4).ToString(CultureInfo.InvariantCulture),
                reader.IsDBNull(5) ? "" : reader.GetString(5),
                reader.IsDBNull(6) ? "" : reader.GetDecimal(6).ToString(CultureInfo.InvariantCulture),
                reader.IsDBNull(7) ? "" : reader.GetDecimal(7).ToString(CultureInfo.InvariantCulture)));
        }
        return rows;
    }

    private static List<string> FormatViewerLatest(List<DatabaseSizeRow> rows) => rows.Select(r => string.Join("|",
        r.DatabaseName, r.FileTypeDesc, r.FileName,
        r.TotalSizeMb.ToString(CultureInfo.InvariantCulture),
        r.UsedSizeMb?.ToString(CultureInfo.InvariantCulture) ?? "",
        r.VolumeMountPoint ?? "",
        r.VolumeTotalMb?.ToString(CultureInfo.InvariantCulture) ?? "",
        r.VolumeFreeMb?.ToString(CultureInfo.InvariantCulture) ?? "")).ToList();

    private static async Task<List<string>> ReadOldSummaryAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(OldSummarySql, connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(10);
        var rows = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", reader.GetString(0), reader.GetDecimal(1).ToString(CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? "" : reader.GetDecimal(2).ToString(CultureInfo.InvariantCulture)));
        }
        return rows;
    }

    private static List<string> FormatSummary(List<DatabaseSizeSummaryRow> rows) => rows.Select(r => string.Join("|",
        r.DatabaseName, r.TotalMb.ToString(CultureInfo.InvariantCulture), r.UsedMb?.ToString(CultureInfo.InvariantCulture) ?? "")).ToList();

    private static async Task<List<string>> ReadOldGrowthAsync(NpgsqlConnection connection, int serverId, DateTime now, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(OldGrowthSql, connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(Naive(now.AddDays(-7)));
        cmd.Parameters.AddWithValue(Naive(now.AddDays(-30)));
        var rows = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|",
                reader.GetString(0), reader.GetDecimal(1).ToString(CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? "" : reader.GetDecimal(2).ToString(CultureInfo.InvariantCulture),
                reader.IsDBNull(3) ? "" : reader.GetDecimal(3).ToString(CultureInfo.InvariantCulture),
                reader.GetDecimal(4).ToString(CultureInfo.InvariantCulture),
                reader.GetDecimal(5).ToString(CultureInfo.InvariantCulture),
                reader.GetDecimal(6).ToString(CultureInfo.InvariantCulture),
                reader.GetDecimal(7).ToString(CultureInfo.InvariantCulture)));
        }
        return rows;
    }

    private static List<string> FormatGrowth(List<StorageGrowthRow> rows) => rows.Select(r => string.Join("|",
        r.DatabaseName, r.CurrentSizeMb.ToString(CultureInfo.InvariantCulture),
        r.Size7dAgoMb?.ToString(CultureInfo.InvariantCulture) ?? "", r.Size30dAgoMb?.ToString(CultureInfo.InvariantCulture) ?? "",
        r.Growth7dMb.ToString(CultureInfo.InvariantCulture), r.Growth30dMb.ToString(CultureInfo.InvariantCulture),
        r.DailyGrowthRateMb.ToString(CultureInfo.InvariantCulture), r.GrowthPct30d.ToString(CultureInfo.InvariantCulture))).ToList();

    private static async Task<List<DarlingObjectStatsReader.DatabaseSizeRow>> ReadOldMcpLatestAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(OldMcpLatestSql, connection);
        cmd.Parameters.AddWithValue(serverId);
        var rows = new List<DarlingObjectStatsReader.DatabaseSizeRow>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new DarlingObjectStatsReader.DatabaseSizeRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDouble(5),
                reader.IsDBNull(6) ? null : reader.GetDouble(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.IsDBNull(10) ? null : reader.GetDouble(10)));
        }
        return rows;
    }

    // ---- seeding / cleanup ----

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task SeedAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime[] stamps, CancellationToken ct)
    {
        foreach (var stamp in stamps)
        {
            foreach (var (fileId, typeDesc, fileName) in new[] { (1, "ROWS", "data.mdf"), (2, "LOG", "log.ldf") })
            {
                using var cmd = new NpgsqlCommand(@"
INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb, max_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)", connection);
                cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
                cmd.Parameters.AddWithValue(Naive(stamp));
                cmd.Parameters.AddWithValue(serverId);
                cmd.Parameters.AddWithValue(serverName);
                cmd.Parameters.AddWithValue("PlanShapeDb");
                cmd.Parameters.AddWithValue(5);
                cmd.Parameters.AddWithValue(fileId);
                cmd.Parameters.AddWithValue(typeDesc);
                cmd.Parameters.AddWithValue(fileName);
                cmd.Parameters.AddWithValue("D:\\" + fileName);
                cmd.Parameters.AddWithValue(1000m);
                cmd.Parameters.AddWithValue(900m);
                cmd.Parameters.AddWithValue(-1m);
                cmd.Parameters.AddWithValue("D:\\");
                cmd.Parameters.AddWithValue(5000m);
                cmd.Parameters.AddWithValue(2500m);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = AllServers.Select(s => s.Id).ToArray();
        using (var cmd = new NpgsqlCommand("DELETE FROM database_size_stats WHERE server_id = ANY($1);", connection))
        {
            cmd.Parameters.AddWithValue(ids);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        using (var cmd = new NpgsqlCommand("DELETE FROM servers WHERE server_id = ANY($1);", connection))
        {
            cmd.Parameters.AddWithValue(ids);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
