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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Shared assertions on the two catch-all rows (#6971) for the live sweep tests, on both store shapes. The hourly sweep sizes
/// them from <c>pg_class.relpages</c> (<c>UnenumeratedEstimateInsertSql</c>, or the plain twin when there is no TimescaleDB)
/// and the sweep that is exact writes the file-sized row. A helper rather than a copy in each test so the timescale and plain
/// stores are held to the same checks.
/// </summary>
internal static class CatchAllEstimateChecks
{
    private const string SeedTable = "public.tick6971_toasted";

    internal sealed record CatchAllRow(long TotalBytes, int ChunkCount, bool? SizeEstimated);

    internal static async Task<Dictionary<string, CatchAllRow>> ReadAsync(NpgsqlConnection connection, DateTime metricTime, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($@"
SELECT object_kind, total_bytes, chunk_count, size_estimated
FROM collect.store_metrics
WHERE metric_time = $1
AND   object_kind IN ('{StoreSelfMetrics.OtherObjectKind}', '{StoreSelfMetrics.SystemObjectKind}')", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(metricTime, DateTimeKind.Unspecified));
        var rows = new Dictionary<string, CatchAllRow>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows[reader.GetString(0)] = new CatchAllRow(
                reader.GetInt64(1),
                reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetBoolean(3));
        }

        return rows;
    }

    /// <summary>
    /// The exact sweep wrote both catch-all rows unmarked. The hourly sweep after it wrote both rows marked
    /// <c>size_estimated = true</c> with the same relation count, and a size at or below the exact one (the estimate leaves
    /// out the free-space and visibility forks and counts the pages of the last vacuum).
    /// </summary>
    internal static async Task AssertHourlyRowsAgainstExactAsync(NpgsqlConnection connection, DateTime exactTime, DateTime hourlyTime, CancellationToken ct)
    {
        var exact = await ReadAsync(connection, exactTime, ct);
        var hourly = await ReadAsync(connection, hourlyTime, ct);
        var kinds = new[] { StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.SystemObjectKind }.OrderBy(k => k).ToList();
        Assert.Equal(kinds, exact.Keys.OrderBy(k => k).ToList());
        Assert.Equal(kinds, hourly.Keys.OrderBy(k => k).ToList());
        foreach (var kind in kinds)
        {
            Assert.NotEqual(true, exact[kind].SizeEstimated);
            Assert.True(hourly[kind].SizeEstimated, $"{kind}: the hourly catch-all row is not marked estimated");
            Assert.Equal(exact[kind].ChunkCount, hourly[kind].ChunkCount);
            Assert.True(hourly[kind].TotalBytes > 0, $"{kind}: the hourly catch-all estimate is empty");
            Assert.True(hourly[kind].TotalBytes <= exact[kind].TotalBytes,
                $"{kind}: the estimate {hourly[kind].TotalBytes} is above the exact {exact[kind].TotalBytes}");
        }
    }

    /// <summary>The exact sweep a day later writes both catch-all rows unmarked again.</summary>
    internal static async Task AssertDailyRowsAreExactAsync(NpgsqlConnection connection, DateTime dailyTime, CancellationToken ct)
    {
        var daily = await ReadAsync(connection, dailyTime, ct);
        Assert.Equal(2, daily.Count);
        Assert.All(daily, r => Assert.NotEqual(true, r.Value.SizeEstimated));
    }

    /// <summary>
    /// Sweeps an hour after <paramref name="previousHourlyTime"/> for a baseline, adds a relation to the catch-all that is
    /// mostly TOAST (large uncompressed values, plus a primary key), vacuums it so its page counts are current, and sweeps
    /// an hour after the baseline. The baseline is taken here, right before the seed, so nothing else moves between the two
    /// reads. The catch-all
    /// estimate must rise by exactly the main-fork bytes of the table, its TOAST table, and the indexes of both (the
    /// population <c>pg_total_relation_size</c> adds up), measured here by file size, independent of <c>relpages</c>. A
    /// statement that drops the TOAST arm, the TOAST-index arm or the index arm misses by a TOAST table of about 16 MB or a
    /// few hundred KB of index, so this fails; a plain band around the exact figure would not.
    /// </summary>
    internal static async Task AssertEstimateCountsTheToastedRelationAsync(
        NpgsqlConnection connection, bool timescale, DateTime previousHourlyTime, CancellationToken ct)
    {
        var baselineTime = previousHourlyTime.AddHours(1);
        /* Bring every relation's relpages current first, so autovacuum has nothing pending that could move another catch-all
           relation's relpages between the two sweeps and break the exact comparison below. */
        await ExecAsync(connection, "VACUUM (ANALYZE)", ct);
        await StoreSelfMetrics.SweepAsync(connection, timescale, baselineTime, null, null, null, ct);
        var before = await ReadAsync(connection, baselineTime, ct);

        await ExecAsync(connection, $"CREATE TABLE {SeedTable} (id integer PRIMARY KEY, payload text NOT NULL)", ct);
        await ExecAsync(connection, $"ALTER TABLE {SeedTable} ALTER COLUMN payload SET STORAGE EXTERNAL", ct);
        await ExecAsync(connection,
            $"INSERT INTO {SeedTable} SELECT g, repeat(md5(g::text), 256) FROM generate_series(1, 2000) AS g", ct);
        await ExecAsync(connection, $"VACUUM (ANALYZE) {SeedTable}", ct);
        /* A bulk load into an insert-only table leaves index page counts at their build-time values (VACUUM skips index cleanup when nothing was deleted), so rebuild the table's indexes and its TOAST index to make relpages current. */
        await ExecAsync(connection, $"REINDEX TABLE {SeedTable}", ct);

        long expectedDelta;
        long toastBytes;
        await using (var oracle = new NpgsqlCommand($@"
WITH rels AS (
    SELECT '{SeedTable}'::regclass::oid AS oid, 'heap' AS part
    UNION ALL
    SELECT reltoastrelid, 'toast' FROM pg_class WHERE oid = '{SeedTable}'::regclass AND reltoastrelid <> 0
    UNION ALL
    SELECT indexrelid, 'index' FROM pg_index WHERE indrelid = '{SeedTable}'::regclass
    UNION ALL
    SELECT i.indexrelid, 'toast_index' FROM pg_index i JOIN pg_class c ON c.reltoastrelid = i.indrelid WHERE c.oid = '{SeedTable}'::regclass
)
SELECT coalesce(sum(pg_relation_size(oid, 'main')), 0)::bigint,
       coalesce(sum(pg_relation_size(oid, 'main')) FILTER (WHERE part = 'toast'), 0)::bigint
FROM rels", connection))
        await using (var reader = await oracle.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct));
            expectedDelta = reader.GetInt64(0);
            toastBytes = reader.GetInt64(1);
        }

        Assert.True(toastBytes > 10_000_000, $"the seeded relation should be mostly TOAST, but its TOAST table is {toastBytes} bytes");

        var next = baselineTime.AddHours(1);
        await StoreSelfMetrics.SweepAsync(connection, timescale, next, null, null, null, ct);
        var after = await ReadAsync(connection, next, ct);

        var other = StoreSelfMetrics.OtherObjectKind;
        Assert.True(after[other].SizeEstimated, "the second hourly sweep writes the catch-all estimate");
        Assert.Equal(before[other].ChunkCount + 1, after[other].ChunkCount);
        var delta = after[other].TotalBytes - before[other].TotalBytes;
        Assert.True(delta == expectedDelta,
            $"the catch-all estimate rose by {delta} bytes for the seeded relation, expected {expectedDelta} "
            + $"(heap, {toastBytes} bytes of TOAST, and the indexes of both; timescale {timescale})");
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
