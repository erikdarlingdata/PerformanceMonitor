/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows, so the
   class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9796: every slice of a time-sliced retention purge after the first bounds its start probe from below by the end of the slice
/// before it. These run the retention sweep's DELETE path on a live store (<c>timescaleAvailable: false</c>, so every table takes the
/// time-sliced DELETE) and check what the bound must not change (a backlog with an empty day drains in one sweep, the command purge
/// passes a live command, every server of the per-server shape drains), what it does change (a row written below the bound waits for
/// the next sweep), and what it is for (the bounded probe reads none of the rows the slice before deleted).
/// </summary>
public sealed class RetentionPurgeSliceBoundLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* The rows the proof's first slice deletes. */
    private const int ProofRowsPerDay = 30_000;

    private const string AlertColumns = "(alert_time, server_id, server_name, metric_name, current_value, threshold_value)";

    [Fact]
    public async Task ABacklogWithAnEmptyDayInTheMiddle_DrainsCompletelyInOneSweep_AndNoTableFails()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention slice-bound live tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* The alert log is kept 90 days. Expired days 100, 99, 97 and 96 back (no row 98 days back), live rows 80 days and 1 day
               back. Every row sits a day or more from the horizon, so the sweep's own clock a few seconds later decides the same. */
            var seedTime = SeedTime();
            await ExecAsync(connection,
                $"INSERT INTO config_alert_log {AlertColumns} "
                + $"SELECT {seedTime} - make_interval(days => d, mins => n), 1, 'srv', 'age-' || d, 0, 0 "
                + "FROM unnest(ARRAY[100, 99, 97, 96, 80, 1]) AS d CROSS JOIN generate_series(1, 25) AS n", ct);

            var log = new CapturingTestLogger();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            Assert.Equal("age-1:25,age-80:25", await ScalarAsync(connection,
                "SELECT string_agg(metric_name || ':' || n, ',' ORDER BY metric_name) "
                + "FROM (SELECT metric_name, count(*) AS n FROM config_alert_log GROUP BY metric_name) AS x", ct));

            /* The sweep ran every table's purge against this store, each time-sliced one through its probe: none failed. */
            Assert.DoesNotContain("Retention purge failed", log.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheCommandPurge_DrainsPastLiveCommands_AndKeepsThem()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention slice-bound live tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* Commands are kept 30 days. A pending command older than every finished one, an in-progress one between two expired
               days, finished ones on four expired days with a gap, and a finished one inside the horizon. */
            var seedTime = SeedTime();
            await ExecAsync(connection, "DELETE FROM config.config_command", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_command (created_at, command_type, status) "
                + $"SELECT {seedTime} - make_interval(days => v.d), 'slice-bound-test', v.status "
                + "FROM (VALUES (46, 'pending'), (45, 'succeeded'), (44, 'failed'), (43, 'in_progress'), (42, 'succeeded'), (40, 'failed'), (1, 'succeeded')) AS v(d, status)", ct);

            var log = new CapturingTestLogger();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            Assert.Equal("1:succeeded,43:in_progress,46:pending", await ScalarAsync(connection,
                $"SELECT string_agg((({seedTime})::date - created_at::date) || ':' || status, ',' ORDER BY created_at DESC) FROM config.config_command", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ARowWrittenBelowTheBoundAfterASliceRan_IsLeftByThatSweep_AndTheNextSweepRemovesIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention slice-bound live tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var seedTime = SeedTime();
            await ExecAsync(connection,
                $"INSERT INTO config_alert_log {AlertColumns} "
                + $"SELECT {seedTime} - make_interval(days => d, mins => n), 1, 'srv', 'age-' || d, 0, 0 "
                + "FROM unnest(ARRAY[100, 99, 98, 1]) AS d CROSS JOIN generate_series(1, 10) AS n", ct);

            /* A writer that lands between two slices of one sweep: the DELETE that removes the oldest slice (100 days back) fires
               this, and the row it writes is as old as the rows that slice just removed. */
            await ExecAsync(connection,
                "CREATE FUNCTION write_late_row() RETURNS trigger LANGUAGE plpgsql AS $fn$ "
                + "BEGIN "
                + "IF EXISTS (SELECT 1 FROM gone WHERE metric_name = 'age-100') THEN "
                + $"INSERT INTO config_alert_log {AlertColumns} VALUES ({seedTime} - make_interval(days => 100, mins => 5), 1, 'srv', 'late', 0, 0); "
                + "END IF; "
                + "RETURN NULL; "
                + "END $fn$", ct);
            await ExecAsync(connection,
                "CREATE TRIGGER write_late_row AFTER DELETE ON config_alert_log REFERENCING OLD TABLE AS gone "
                + "FOR EACH STATEMENT EXECUTE FUNCTION write_late_row()", ct);

            var log = new CapturingTestLogger();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            /* The sweep went on to the newer expired days and left the late row, which is below every later slice's bound. */
            Assert.Equal("age-1:10,late:1", await AlertCountsAsync(connection, ct));

            /* The next sweep starts with no bound and removes it. */
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);
            Assert.Equal("age-1:10", await AlertCountsAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ThePerServerPurge_DrainsEveryServer_WithAndWithoutAnEmptyDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention slice-bound live tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* The attention table is a plain table here (nothing converted it), kept 30 days, so the sweep purges it with the
               per-server shape. Each server holds a different set of days, and every server has one row inside the horizon. */
            var seedTime = SeedTime();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var log = new CapturingTestLogger();

            /* Consecutive expired days: 45, 44 and 43 back, the oldest on the last server id only. */
            await SeedAttentionAsync(connection, seedTime, 1000, "(-9, 44), (-9, 43), (3, 43), (70001, 45), (70001, 44), (-9, 2), (3, 2), (70001, 2)", ct);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);
            Assert.Equal("-9:2,3:2,70001:2", await AttentionAgesAsync(connection, seedTime, ct));

            /* Expired days with empty days between them (40, 37 and 36 back), each on a different server. */
            await SeedAttentionAsync(connection, seedTime, 2000, "(3, 40), (-9, 37), (70001, 36), (3, 36)", ct);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);
            Assert.Equal("-9:2,3:2,70001:2", await AttentionAgesAsync(connection, seedTime, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The reason for the bound. One slice deletes 30,000 rows and nothing vacuums the table. The next slice's probe, bounded by that
    /// slice's end, reads none of them. The probe with no bound, run second because an index scan that visits dead rows marks their
    /// index entries dead for the scans after it, visits every one of them in the heap before it reaches a live row.
    /// </summary>
    [Fact]
    public async Task TheBoundedProbe_ReadsNoneOfTheRowsTheSliceBeforeDeleted_WhereTheUnboundedProbeReadsThemAll()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention slice-bound live tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* Three days of rows in time order. Autovacuum stays off the table, so the only vacuum is the one below, before the
               delete: it sets the visibility map, and the probes are then index-only scans whose heap fetches are the rows on pages
               a delete touched. */
            await ExecAsync(connection, "CREATE TABLE slice_bound_proof (id bigint NOT NULL, seen timestamp NOT NULL) WITH (autovacuum_enabled = false)", ct);
            await ExecAsync(connection,
                "INSERT INTO slice_bound_proof (id, seen) "
                + "SELECT d * 1000000 + n, timestamp '2026-03-01 00:00:00' + make_interval(days => d, secs => n) "
                + $"FROM generate_series(0, 2) AS d CROSS JOIN generate_series(1, {ProofRowsPerDay}) AS n ORDER BY d, n", ct);
            await ExecAsync(connection, "CREATE INDEX slice_bound_proof_seen ON slice_bound_proof (seen)", ct);
            await ExecAsync(connection, "VACUUM (ANALYZE) slice_bound_proof", ct);

            var purge = DarlingRetention.TimeSlicedDeleteSql("slice_bound_proof", "seen");
            var cutoff = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);

            var (deleted, sliceEnd) = await DarlingRetention.ExecuteTimeSliceAsync(connection, purge, cutoff, secondParameter: null, lowerBound: null, ct);
            Assert.Equal(ProofRowsPerDay, deleted);
            Assert.NotNull(sliceEnd);

            var bounded = await HeapFetchesAsync(connection, purge.NextProbeSql!, ct, cutoff, sliceEnd);
            var unbounded = await HeapFetchesAsync(connection, purge.FirstProbeSql!, ct, cutoff);

            Assert.True(bounded < 100, $"the bounded probe fetched {bounded} heap rows: it walked rows the slice before deleted");
            Assert.True(unbounded >= ProofRowsPerDay, $"the unbounded probe fetched {unbounded} heap rows, fewer than the {ProofRowsPerDay} the slice before deleted: this store no longer shows the cost the bound removes");

            /* Both probes name the same next slice, and the drain finishes from the bound. */
            Assert.Equal(2 * ProofRowsPerDay, await DrainFromAsync(connection, purge, cutoff, sliceEnd, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM slice_bound_proof", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* The rest of a drain whose first slice already ran: every slice starts from the end of the one before. */
    private static async Task<int> DrainFromAsync(NpgsqlConnection connection, DarlingRetention.PurgeSql purge, DateTime cutoff, object? lowerBound, CancellationToken ct)
        => await DarlingRetention.DrainBatchesAsync(
            async token =>
            {
                var (rows, sliceEnd) = await DarlingRetention.ExecuteTimeSliceAsync(connection, purge, cutoff, secondParameter: null, lowerBound, token);
                lowerBound = sliceEnd ?? lowerBound;
                return (rows, 1);
            },
            ct);

    /* The heap rows a statement's index-only scans fetched, summed over its plan. */
    private static async Task<long> HeapFetchesAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object?[] parameters)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        foreach (var parameter in parameters)
        {
            command.Parameters.AddWithValue(parameter!);
        }

        using var plan = JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!);
        var scans = 0;
        var fetches = SumHeapFetches(plan.RootElement, ref scans);
        Assert.True(scans > 0, "the probe ran no index-only scan, so it reports no heap fetches to compare");
        return fetches;
    }

    private static long SumHeapFetches(JsonElement element, ref int scans)
    {
        var total = 0L;
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (property.NameEquals("Heap Fetches"))
                {
                    scans++;
                    total += property.Value.GetInt64();
                }
                else
                {
                    total += SumHeapFetches(property.Value, ref scans);
                }
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
            {
                total += SumHeapFetches(item, ref scans);
            }
        }

        return total;
    }

    /* One clock, read once per test, seeds the rows and measures their ages, so a run that crosses midnight UTC reads the same days. */
    private static string SeedTime()
        => "TIMESTAMP '" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";

    private static async Task<string?> AlertCountsAsync(NpgsqlConnection connection, CancellationToken ct)
        => (string?)await ScalarAsync(connection,
            "SELECT string_agg(metric_name || ':' || n, ',' ORDER BY metric_name) "
            + "FROM (SELECT metric_name, count(*) AS n FROM config_alert_log GROUP BY metric_name) AS x", ct);

    private static async Task SeedAttentionAsync(NpgsqlConnection connection, string seedTime, int firstId, string serverDays, CancellationToken ct)
        => await ExecAsync(connection,
            "INSERT INTO collect.attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) "
            + $"SELECT {firstId} + row_number() OVER (), {seedTime} - make_interval(days => v.d), v.server_id, 'slice-bound-test', 'count', 1 "
            + $"FROM (VALUES {serverDays}) AS v(server_id, d)", ct);

    /* Each remaining row as server:age-in-days. */
    private static async Task<string?> AttentionAgesAsync(NpgsqlConnection connection, string seedTime, CancellationToken ct)
        => (string?)await ScalarAsync(connection,
            $"SELECT string_agg(server_id || ':' || (({seedTime})::date - collection_time::date), ',' ORDER BY server_id, collection_time) "
            + "FROM collect.attention_events", ct);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
