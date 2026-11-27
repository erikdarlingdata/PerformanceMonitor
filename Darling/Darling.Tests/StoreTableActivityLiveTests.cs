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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9413: the hourly per-table activity capture and its read. Two captures an hour apart give the hour's differences, a hypertable with compressed
/// chunks is one row (its chunks and compressed chunks roll up to it and none is a row of its own), the first capture writes no rows, a counter that
/// fell between captures (a statistics reset) is counted from the reset, and the read ranks the tables by rows written and by blocks read.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class StoreTableActivityLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipMessage = "Set DARLING_TEST_PG to a Postgres connection string to run the #9413 live tests.";

    private const string SeedSql = @"
SELECT _timescaledb_functions.stop_background_workers();
CREATE SCHEMA zz9413;
CREATE TABLE zz9413.pt (id integer PRIMARY KEY, v text);
CREATE TABLE zz9413.ht (ts timestamptz NOT NULL, dev integer NOT NULL, v double precision);
SELECT create_hypertable('zz9413.ht', 'ts', chunk_time_interval => interval '1 day');
ALTER TABLE zz9413.ht SET (timescaledb.compress, timescaledb.compress_segmentby = 'dev');
INSERT INTO zz9413.ht SELECT now() - (d || ' days')::interval, d, 1 FROM generate_series(0, 4) AS d;
SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('zz9413.ht', older_than => now() - interval '1 day') AS c;
ANALYZE;";

    /* The writes and the flush of their counters share one command: PostgreSQL 15+ sends a backend's counters to the shared statistics up to a second
       after it goes idle, and pg_stat_force_next_flush() makes this transaction's end send them. */
    private static async Task ExecAsync(NpgsqlDataSource store, string sql, CancellationToken ct)
    {
        await using var command = store.CreateCommand(sql + ";SELECT pg_stat_force_next_flush();");
        command.CommandTimeout = 300;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string AsOf(DateTime utc) => utc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    /// <summary>A whole hour three days back: far enough that an as_of at it is never in the future.</summary>
    private static DateTime HourThreeDaysBack()
    {
        var back = LiveClock.Now().AddDays(-3);
        return new DateTime(back.Year, back.Month, back.Day, back.Hour, 0, 0, DateTimeKind.Utc);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource store, string sql, CancellationToken ct)
    {
        await using var command = store.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<int> CaptureAsync(string connectionString, DateTime at, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return await StoreTableActivity.CaptureAsync(connection, timescaleAvailable: true, at, logger: null, commandTimeoutSeconds: 120, ct);
    }

    private static async Task<List<Dictionary<string, object>>> ActivityAsync(NpgsqlDataSource store, DateTime at, CancellationToken ct)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var command = store.CreateCommand("SELECT table_schema, table_name, table_kind, interval_seconds, relations_active, relations_scanned, tup_inserted, tup_updated, tup_deleted, heap_blks_read + heap_blks_hit + idx_blks_read + idx_blks_hit AS blocks FROM collect.store_table_activity WHERE metric_time = $1");
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Enumerable.Range(0, reader.FieldCount).ToDictionary(i => reader.GetName(i), i => reader.GetValue(i)));
        }

        return rows;
    }

    [Fact]
    public async Task TwoCaptures_GiveTheHoursDifferences_AndCompressedChunksRollUpToTheirHypertable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var t0 = new DateTime(2026, 11, 20, 10, 0, 0, DateTimeKind.Utc);
            var t1 = t0.AddHours(1);
            var t2 = t0.AddHours(2);

            // The first capture has nothing to compare with: it writes the baseline and no activity rows.
            Assert.Equal(0, await CaptureAsync(scratch.ConnectionString, t0, ct));
            Assert.Empty(await ActivityAsync(store, t0, ct));

            // The hour: 10 rows into the plain table, 5 into the hypertable, and a read of every chunk of the hypertable, compressed ones included.
            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'x' FROM generate_series(1, 10) AS g", ct);
            await ExecAsync(store, "INSERT INTO zz9413.ht SELECT now(), 9, 2 FROM generate_series(1, 5)", ct);
            await ExecAsync(store, "SELECT count(*) FROM zz9413.ht", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t1, ct) > 0);

            var hour = await ActivityAsync(store, t1, ct);
            var plain = Assert.Single(hour, r => (string)r["table_name"] == "pt");
            Assert.Equal("table", (string)plain["table_kind"]);
            Assert.Equal(10L, (long)plain["tup_inserted"]);
            Assert.Equal(3600, (int)plain["interval_seconds"]);

            var hyper = Assert.Single(hour, r => (string)r["table_name"] == "ht");
            Assert.Equal("hypertable", (string)hyper["table_kind"]);
            Assert.Equal(5L, (long)hyper["tup_inserted"]);
            Assert.True((long)hyper["blocks"] > 0);
            Assert.True((int)hyper["relations_scanned"] >= 3, $"relations scanned: {hyper["relations_scanned"]}");
            Assert.True((int)hyper["relations_active"] >= (int)hyper["relations_scanned"]);
            Assert.DoesNotContain(hour, r => (string)r["table_schema"] == "_timescaledb_internal");

            // The read ranks the hour by rows written (the plain table wrote 10, the hypertable 5) and by blocks read.
            using (var byWrites = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 2, AsOf(t1), "rows_written", null, 10, null, ct)))
            {
                var tables = byWrites.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()).ToList();
                Assert.True(tables.IndexOf("pt") >= 0 && tables.IndexOf("pt") < tables.IndexOf("ht"), string.Join(",", tables));
            }

            using (var byBlocks = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 2, AsOf(t1), "blocks_read", null, 10, null, ct)))
            {
                var first = byBlocks.RootElement.GetProperty("tables").EnumerateArray().First();
                Assert.True(first.GetProperty("blocks_read").GetInt64() >= 0);
                Assert.Equal("blocks_read", byBlocks.RootElement.GetProperty("rank_by").GetString());
            }

            using (var series = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 2, AsOf(t1), null, "zz9413.ht", 10, null, ct)))
            {
                var point = Assert.Single(series.RootElement.GetProperty("series").EnumerateArray());
                Assert.Equal(5L, point.GetProperty("tuples_inserted").GetInt64());
            }

            // A statistics reset between captures: the counters read below their baseline and count from the reset.
            await ExecAsync(store, "SELECT pg_stat_reset()", ct);
            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'y' FROM generate_series(100, 102) AS g", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t2, ct) > 0);
            var afterReset = Assert.Single(await ActivityAsync(store, t2, ct), r => (string)r["table_name"] == "pt");
            Assert.Equal(3L, (long)afterReset["tup_inserted"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A statistics reset between captures where the busiest table writes MORE rows than its old baseline held before the next capture. The counter
    /// is above its baseline, so only the stored reset time can say a reset ran: the hour's rows are the value counted since the reset (25), not the
    /// value minus the old baseline (15).
    /// </summary>
    [Fact]
    public async Task AResetFollowedByMoreWritesThanTheOldBaseline_CountsTheValueSinceTheReset()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var t0 = HourThreeDaysBack();
            var t1 = t0.AddHours(1);

            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'x' FROM generate_series(1, 10) AS g", ct);
            Assert.Equal(0, await CaptureAsync(scratch.ConnectionString, t0, ct));

            await ExecAsync(store, "SELECT pg_stat_reset()", ct);
            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'y' FROM generate_series(100, 124) AS g", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t1, ct) > 0);

            var afterReset = Assert.Single(await ActivityAsync(store, t1, ct), r => (string)r["table_name"] == "pt");
            Assert.Equal(25L, (long)afterReset["tup_inserted"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// One table's counters reset alone do not move the database's reset time, so the fall below the baseline stays the sign: the new value
    /// is the hour's difference.
    /// </summary>
    [Fact]
    public async Task OneTablesCountersResetAlone_StillCountFromZero_ByTheFallRule()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var t0 = HourThreeDaysBack();
            var t1 = t0.AddHours(1);

            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'x' FROM generate_series(1, 10) AS g", ct);
            Assert.Equal(0, await CaptureAsync(scratch.ConnectionString, t0, ct));

            await ExecAsync(store, "SELECT pg_stat_reset_single_table_counters('zz9413.pt'::regclass)", ct);
            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'y' FROM generate_series(100, 102) AS g", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t1, ct) > 0);

            var afterReset = Assert.Single(await ActivityAsync(store, t1, ct), r => (string)r["table_name"] == "pt");
            Assert.Equal(3L, (long)afterReset["tup_inserted"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The ranking window ends at as_of. Two captures an hour apart each have their own activity, and an as_of between them (one hour wide)
    /// ranks only the first hour, an as_of at the second capture only the second, and a two-hour window both. A bad as_of is refused, not read as now.
    /// </summary>
    [Fact]
    public async Task AsOf_RanksOnlyTheHourTheWindowCovers()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, "SELECT _timescaledb_functions.stop_background_workers(); CREATE SCHEMA zz9413; CREATE TABLE zz9413.first_hour (id integer PRIMARY KEY); CREATE TABLE zz9413.second_hour (id integer PRIMARY KEY)", ct);
            var t0 = HourThreeDaysBack();
            var t1 = t0.AddHours(1);
            var t2 = t0.AddHours(2);

            Assert.Equal(0, await CaptureAsync(scratch.ConnectionString, t0, ct));
            await ExecAsync(store, "INSERT INTO zz9413.first_hour SELECT g FROM generate_series(1, 10) AS g", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t1, ct) > 0);
            await ExecAsync(store, "INSERT INTO zz9413.second_hour SELECT g FROM generate_series(1, 7) AS g", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t2, ct) > 0);

            async Task<List<string?>> RankedAsync(int hours, string asOf)
            {
                using var json = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, hours, asOf, "rows_written", null, 100, null, ct));
                return json.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()).ToList();
            }

            // As of half an hour past the first capture, a one-hour window is (t0 + 30 min, t1 + 30 min]: it holds the first capture's rows only.
            var firstHour = await RankedAsync(1, AsOf(t1.AddMinutes(30)));
            Assert.Contains("first_hour", firstHour);
            Assert.DoesNotContain("second_hour", firstHour);

            // As of the second capture, a one-hour window is (t1, t2]: only the second hour.
            var secondHour = await RankedAsync(1, AsOf(t2));
            Assert.Contains("second_hour", secondHour);
            Assert.DoesNotContain("first_hour", secondHour);

            // Two hours back from the second capture holds both.
            var both = await RankedAsync(2, AsOf(t2));
            Assert.Contains("first_hour", both);
            Assert.Contains("second_hour", both);

            // The window the answer reports is the one that was read.
            using (var json = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 1, AsOf(t2), null, null, 100, null, ct)))
            {
                Assert.Equal(t2, json.RootElement.GetProperty("window_end").GetDateTime().ToUniversalTime());
                Assert.Equal(t1, json.RootElement.GetProperty("window_start").GetDateTime().ToUniversalTime());
            }

            // The tool carries the same anchor: a past as_of through get_store_metrics reads that past window and not the current one.
            using (var viaTool = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(
                store, table_activity: true, hours_back: 1, as_of: AsOf(t1.AddMinutes(30)), cancellationToken: ct)))
            {
                var names = viaTool.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()).ToList();
                Assert.Contains("first_hour", names);
                Assert.DoesNotContain("second_hour", names);
                Assert.Equal(t1.AddMinutes(30), viaTool.RootElement.GetProperty("window_end").GetDateTime().ToUniversalTime());
            }

            // A value that is not a time is refused and not read as now.
            using (var refused = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 1, "not a time", null, null, 100, null, ct)))
            {
                Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Retention drops hypertable chunks every day. A chunk dropped between two captures must not change the hypertable's difference: the
    /// difference is taken per relation, so a relation that is gone is not in the current counters and the rest still count their own writes.
    /// A roll-up of the raw counters that is subtracted from the last roll-up falls with the dropped chunk and reads as a different number.
    /// </summary>
    [Fact]
    public async Task ADroppedChunk_DoesNotChangeTheHypertablesDifference()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var t0 = HourThreeDaysBack();
            var t1 = t0.AddHours(1);

            Assert.Equal(0, await CaptureAsync(scratch.ConnectionString, t0, ct));

            // Retention: the oldest chunk (and its compressed chunk) goes, and 4 rows land in the newest chunk.
            var before = await CountAsync(store, "SELECT count(*) FROM show_chunks('zz9413.ht')", ct);
            await ExecAsync(store, "SELECT drop_chunks('zz9413.ht', older_than => now() - interval '3 days')", ct);
            Assert.True(await CountAsync(store, "SELECT count(*) FROM show_chunks('zz9413.ht')", ct) < before, "no chunk was dropped, so the test proves nothing");
            await ExecAsync(store, "INSERT INTO zz9413.ht SELECT now(), 9, 2 FROM generate_series(1, 4)", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t1, ct) > 0);

            var hyper = Assert.Single(await ActivityAsync(store, t1, ct), r => (string)r["table_name"] == "ht");
            Assert.Equal(4L, (long)hyper["tup_inserted"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A store below the activity migration has no tables to read: the answer says unavailable and names the version, never an empty list.</summary>
    [Fact]
    public async Task AStoreBelowTheMigration_AnswersUnavailable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            // The store as it was one rung earlier: the migration's two tables are not there and its version is not stamped.
            await ExecAsync(store, "DROP TABLE collect.store_table_activity; DROP TABLE collect.store_table_activity_baseline; DELETE FROM darling_schema_version WHERE version >= " + StoreTableActivity.Version, ct);

            using var json = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 24, null, null, null, 10, null, ct));
            Assert.Equal("unavailable", json.RootElement.GetProperty("status").GetString());
            Assert.Contains(StoreTableActivity.Version.ToString(CultureInfo.InvariantCulture), json.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>A role that can use the schema but not read the activity tables answers precondition with the grant, and the grant it names fixes it.</summary>
    [Fact]
    public async Task ARoleWithoutTheGrant_AnswersPrecondition_AndTheNamedGrantFixesIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var role = "zz9413_" + Guid.NewGuid().ToString("N")[..8];
        const string password = "zz9413-reader-pw";
        var bodySucceeded = false;
        try
        {
            await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{password}'; GRANT USAGE ON SCHEMA collect TO {role}", ct);
            var asReader = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString;
            await using var reader = NpgsqlDataSource.Create(asReader);

            using (var denied = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(reader, 24, null, null, null, 10, null, ct)))
            {
                Assert.Equal("precondition", denied.RootElement.GetProperty("status").GetString());
                Assert.Contains("GRANT SELECT ON collect.store_table_activity", denied.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            await ExecAsync(owner, $"GRANT SELECT ON collect.store_table_activity, collect.store_table_activity_baseline TO {role}", ct);
            using (var granted = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(reader, 24, null, null, null, 10, null, ct)))
            {
                // No capture has run on this store, so the read says so. It is no longer the grant that is missing.
                Assert.Equal("unavailable", granted.RootElement.GetProperty("status").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            // The role belongs to the cluster, not the scratch database, so it goes here and not with the database.
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (connection, token) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}", connection);
                await drop.ExecuteNonQueryAsync(token);
            });
        }
    }
}
