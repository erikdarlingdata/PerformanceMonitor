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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3934: <see cref="DarlingStoreMetricsReader.StoreMetricsLatestSql"/>'s skip-scan rewrite, against a real
/// store. <c>collect.store_metrics</c> is a plain table (never a hypertable), so the only question is whether
/// the recursive-CTE walk over <c>idx_store_metrics_kind_name_time</c> (<see cref="PgTableTuning.Statements"/>)
/// finds the SAME newest-per-object rows the old <c>DISTINCT ON</c> found, through index descents rather than
/// a sort of the whole table.
///
/// <para>Sentinel object names, cleaned up in <c>finally</c> — <c>collect.store_metrics</c> has no server_id
/// to scope by, so cleanup is by name prefix.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StoreMetricsLatestSkipScanLiveTests
{
    private const string NamePrefix = "skipscan3934-";

    /// <summary>The pre-#3934 shape, kept here as the row-correctness oracle.</summary>
    private const string OldStoreMetricsLatestSql = """
        SELECT DISTINCT ON (object_kind, object_name)
            object_kind,
            object_name,
            metric_time,
            total_bytes,
            compressed_before_bytes,
            compressed_after_bytes,
            chunk_count,
            row_count,
            enabled_server_count,
            last_run_duration_ms,
            schedule_interval_ms,
            total_runs,
            total_failures,
            toast_bytes,
            toast_live_bytes
        FROM collect.store_metrics
        WHERE object_name LIKE 'skipscan3934-%'
        ORDER BY object_kind, object_name, metric_time DESC
        """;

    [Fact]
    public async Task TheSkipScan_FindsTheSameNewestRowPerObject_AsDistinctOn_AndUsesTheIndex_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #3934 skip-scan live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            /* #3934's index, applied the way the service applies it (runtime tuning, not a migration). */
            await PgTableTuning.ApplyAsync(connection, logger: null, ct);

            var now = TruncateToSeconds(DateTime.UtcNow);

            /* Thirty objects across three kinds, each with a month of hourly history (720 rows/object,
               21,600 rows total) — enough for the newest-per-object tiebreak to matter (the DISTINCT ON's
               whole reason to exist) and for the planner to have a real population to choose a plan over,
               without a multi-million-row seed slowing the suite down. The full-retention scale (2.4M rows)
               is measured separately in the PR body; this proves the SHAPE, not the field magnitude. */
            var kinds = new[] { "hypertable", "continuous_aggregate", "dimension" };
            var objects = new List<(string Kind, string Name)>();
            foreach (var kind in kinds)
            {
                for (var i = 0; i < 10; i++)
                {
                    objects.Add((kind, $"{NamePrefix}{kind}-{i:D2}"));
                }
            }

            foreach (var (kind, name) in objects)
            {
                await SeedHistoryAsync(connection, ct, kind, name, now);
            }

            var objectNames = objects.Select(o => o.Name).ToList();

            using (var analyze = new NpgsqlCommand("ANALYZE collect.store_metrics", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            /* Row correctness: the skip-scan's 30 rows for these sentinels, against the DISTINCT ON oracle's,
               as sets of rendered rows (order-independent — the shipped read's own ORDER BY is asserted
               separately, on the reader's own ordinals). */
            var shippedRows = await ReadSentinelRowsAsync(connection, DarlingStoreMetricsReader.StoreMetricsLatestSql, ct);
            var oracleRows = await ReadSentinelRowsAsync(connection, OldStoreMetricsLatestSql, ct);
            Assert.Equal(30, oracleRows.Count);
            Assert.Equal(oracleRows.OrderBy(r => r, StringComparer.Ordinal), shippedRows.OrderBy(r => r, StringComparer.Ordinal));

            /* Through the reader itself: GetLatestAsync's positional reads land the same values. */
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var latest = await DarlingStoreMetricsReader.GetLatestAsync(postgres, ct);
            var sentinelLatest = latest.Where(r => r.ObjectName.StartsWith(NamePrefix, StringComparison.Ordinal)).ToList();
            Assert.Equal(30, sentinelLatest.Count);
            foreach (var name in objectNames)
            {
                var row = Assert.Single(sentinelLatest, r => r.ObjectName == name);
                Assert.Equal(TruncateToSeconds(now), row.MetricTime);
            }

            /* Plan shape: every touch of store_metrics is a scan of the new composite - the recursive walk's
               seed and its "next pair" step need only (kind, name), so those two are Index ONLY Scans; the
               per-object newest-row probe needs every payload column, none of which the index carries beside
               the key, so it is a plain Index Scan (still a direct descent to the right key, just with a
               heap fetch for the columns outside it) - and never a Seq Scan of the base table. The plan does
               carry a top Sort node (over the ~30 already-deduplicated OBJECT rows, for the final ORDER BY -
               EXPLAIN prints its key as the underlying store_metrics columns even though it sorts the CTE's
               output, not the base table) - cheap at any retention because it is a function of the object
               count, not the row count, and proven so by ANALYZE's actual row count below rather than by
               absence of the word "Sort", which the old DISTINCT ON plan (sorting the WHOLE table) would also
               have carried. */
            var plan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + DarlingStoreMetricsReader.StoreMetricsLatestSql, ct);
            Assert.Equal(3, CountOccurrences(plan, "idx_store_metrics_kind_name_time"));
            Assert.Equal(2, CountOccurrences(plan, "Index Only Scan using idx_store_metrics_kind_name_time"));
            Assert.Contains("Index Scan using idx_store_metrics_kind_name_time", plan, StringComparison.Ordinal);
            Assert.DoesNotContain("Seq Scan", plan, StringComparison.Ordinal);

            var analyzed = await ExplainAsync(connection, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + DarlingStoreMetricsReader.StoreMetricsLatestSql, ct);
            Assert.Matches(new Regex(@"Sort \(actual rows=30\.00 loops=1\)"), analyzed);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>A month of hourly rows for one object, the newest stamped distinctly so the tiebreak is
    /// observable, one set-based INSERT.</summary>
    private static async Task SeedHistoryAsync(NpgsqlConnection connection, CancellationToken ct, string kind, string name, DateTime now)
    {
        using var command = new NpgsqlCommand("""
            INSERT INTO collect.store_metrics
                (metric_time, object_name, object_kind, total_bytes, compressed_before_bytes,
                 compressed_after_bytes, chunk_count, row_count, enabled_server_count)
            SELECT t, $1, $2,
                   CASE WHEN t = $3 THEN 999999999 ELSE 1000 END,
                   1000, 1000, 1, 1000, 5
            FROM generate_series($3::timestamp - interval '30 days', $3::timestamp, interval '1 hour') AS t
            """, connection);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(kind);
        command.Parameters.AddWithValue(now);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<string>> ReadSentinelRowsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        var rows = new List<string>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "null"
                    : reader.GetValue(i) is DateTime instant ? instant.ToString("O", CultureInfo.InvariantCulture)
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            var name = cells[1];
            if (name.StartsWith(NamePrefix, StringComparison.Ordinal))
            {
                rows.Add(string.Join("|", cells));
            }
        }

        return rows;
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        int count = 0, i = 0;
        while ((i = haystack.IndexOf(needle, i, StringComparison.Ordinal)) >= 0)
        {
            count++;
            i += needle.Length;
        }

        return count;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        var plan = new StringBuilder();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM collect.store_metrics WHERE object_name LIKE 'skipscan3934-%'", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
