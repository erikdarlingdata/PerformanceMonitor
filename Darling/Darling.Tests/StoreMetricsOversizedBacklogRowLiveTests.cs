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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8817, second part: <c>collect.oversized_plan_backlog</c> is a plain table that held 15.6 GiB of over-cap plan XML on one store,
/// and the inventory could only name it in <c>largest_unenumerated</c> with its size now and no history. It now has its own
/// <c>table</c> row in every sweep, the <c>other</c> row stops counting it, and <c>get_store_metrics</c> answers its name from the
/// daily series.
/// </summary>
public sealed class StoreMetricsOversizedBacklogRowLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task SweepAtAsync(NpgsqlConnection connection, DateTime utcNow, CancellationToken ct)
    {
        var written = await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: true, utcNow, null, null, null, ct);
        Assert.True(written > 0, "the sweep wrote nothing");
    }

    private static async Task<List<(DateTime Time, long Bytes)>> RowsAsync(NpgsqlConnection connection, string kind, string name, CancellationToken ct)
    {
        var rows = new List<(DateTime, long)>();
        await using var command = new NpgsqlCommand(
            "SELECT metric_time, total_bytes FROM collect.store_metrics WHERE object_kind = $1 AND object_name = $2 ORDER BY metric_time", connection);
        command.Parameters.AddWithValue(kind);
        command.Parameters.AddWithValue(name);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.GetInt64(1)));
        }

        return rows;
    }

    [Fact]
    public async Task TheOversizedBacklog_HasItsOwnTableRow_TheOtherRowStopsCountingIt_AndTheNameAnswersFromTheSeries_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8817 test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* A sweep two days back with the backlog table empty, then the table grows by several MB and a sweep now:
           the second sweep's other row must not take the growth, and the table row must carry it. */
        var now = DateTime.UtcNow;
        await SweepAtAsync(connection, now.AddDays(-2), ct);

        await using (var grow = new NpgsqlCommand($@"
INSERT INTO {OversizedPlanBacklog.TableName}
    (server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset, observed_bytes, first_seen_at, last_seen_at, plan_xml)
SELECT 1, '{OversizedPlanBacklog.QueryStatsCollectorName}', g::text, g::text, 0, 0, 2000000, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC', repeat('y', 1000)
FROM generate_series(1, 8000) AS g", connection))
        {
            await grow.ExecuteNonQueryAsync(ct);
        }

        await SweepAtAsync(connection, now, ct);

        var tableRows = await RowsAsync(connection, StoreSelfMetrics.TableObjectKind, OversizedPlanBacklog.TableName, ct);
        Assert.True(tableRows.Count == 2, $"expected one table row per sweep for {OversizedPlanBacklog.TableName}, found {tableRows.Count}");
        var grew = tableRows[1].Bytes - tableRows[0].Bytes;
        Assert.True(grew > 5_000_000, $"the table row should carry the table's growth, it grew {grew} bytes");

        await using (var live = new NpgsqlCommand($"SELECT pg_total_relation_size('{OversizedPlanBacklog.TableName}')", connection))
        {
            Assert.Equal((long)(await live.ExecuteScalarAsync(ct))!, tableRows[1].Bytes);
        }

        /* The other row is the same census minus every named table, so the growth must not be in it. */
        var otherRows = await RowsAsync(connection, StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, ct);
        Assert.Equal(2, otherRows.Count);
        var otherGrew = otherRows[1].Bytes - otherRows[0].Bytes;
        Assert.True(otherGrew < 1_000_000,
            $"the other row grew {otherGrew} bytes while the backlog table grew {grew}: the backlog is still counted in it");

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The name answers from the series, with history, and not from the live size fallback. */
        using (var asked = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: OversizedPlanBacklog.TableName)))
        {
            var root = asked.RootElement;
            Assert.Equal("object", root.GetProperty("view").GetString());
            Assert.Equal(1, root.GetProperty("objects_matched").GetInt32());
            Assert.Equal(OversizedPlanBacklog.TableName, root.GetProperty("objects").EnumerateArray().Single().GetProperty("object_name").GetString());
            Assert.Equal(2, root.GetProperty("series").GetArrayLength());
        }

        /* And it is no longer one of the relations the summary lists as un-enumerated. */
        using var summary = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource));
        var unenumerated = summary.RootElement.GetProperty("inventory").GetProperty("largest_unenumerated").EnumerateArray()
            .Select(r => r.GetProperty("relation").GetString()).ToArray();
        Assert.DoesNotContain(OversizedPlanBacklog.TableName, unenumerated);
    }
}
