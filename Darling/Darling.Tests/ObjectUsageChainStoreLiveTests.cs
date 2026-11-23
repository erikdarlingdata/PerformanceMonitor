/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 part B: the object usage chain's three tables (V299). The rung on a fresh store and on one migrated from V298 gives the same
/// shape; the two per-snapshot tables are compressed hypertables where TimescaleDB is present; and the retention pass purges both at the
/// horizon it uses for <c>index_object_stats</c>, a per-collector override included, so no derived row outlives a raw row.
/// </summary>
public sealed class ObjectUsageChainStoreLiveTests
{
    private const int ServerId = -934901;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string ShapeSql = @"
SELECT (SELECT string_agg(c.relname || '.' || a.attname || ':' || format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull::text, ',' ORDER BY c.relname, a.attnum)
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'collect' AND c.relname IN ('object_usage_snapshots', 'object_usage_deltas', 'object_usage_runs') AND a.attnum > 0 AND NOT a.attisdropped)
       || '|' || (SELECT string_agg(conrelid::regclass::text || ':' || pg_get_constraintdef(oid), ',' ORDER BY conrelid::regclass::text) FROM pg_constraint
                  WHERE conrelid IN ('collect.object_usage_snapshots'::regclass, 'collect.object_usage_deltas'::regclass, 'collect.object_usage_runs'::regclass) AND contype = 'p')";

    [Fact]
    public async Task TheRung_GivesTheSameShapeOnAFreshStoreAndOnOneMigratedFromTheRungBefore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live object usage chain tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var fresh = (string)(await ScalarAsync(connection, ShapeSql, ct))!;
            Assert.Contains("object_usage_deltas.reads:bigint:true", fresh, StringComparison.Ordinal);
            Assert.Contains("object_usage_snapshots.node_key:text:true", fresh, StringComparison.Ordinal);
            Assert.Contains("object_usage_runs.last_time:timestamp without time zone:false", fresh, StringComparison.Ordinal);
            Assert.Contains("PRIMARY KEY (server_id, database_name, collection_time, object_id, index_id)", fresh, StringComparison.Ordinal);

            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, ObjectUsageChain.Version - 1, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + ObjectUsageChain.Version.ToString(CultureInfo.InvariantCulture), ct);
            Assert.Null(await ScalarAsync(connection, "SELECT to_regclass('collect.object_usage_snapshots')::text", ct));

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, (int)(await ScalarAsync(connection, "SELECT max(version) FROM darling_schema_version", ct))!);
            Assert.Equal(fresh, (string)(await ScalarAsync(connection, ShapeSql, ct))!);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task WhereTimescaleDbIsPresent_BothPerSnapshotTablesAreCompressedHypertables_AndTheRunsTableStaysPlain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG (with TimescaleDB installed) to run the live object usage chain tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for this test");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        var bodySucceeded = false;
        try
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureObjectUsageHypertablesAsync(connection, NullLogger.Instance, ct));
            Assert.True(await TimescaleSupport.EnsureObjectUsageHypertablesAsync(connection, NullLogger.Instance, ct), "a second pass changes nothing");

            const string state = @"
SELECT h.compression_enabled::text || '|' || coalesce((SELECT string_agg(cs.attname, ', ' ORDER BY cs.segmentby_column_index) FROM timescaledb_information.compression_settings cs
         WHERE cs.hypertable_schema = h.hypertable_schema AND cs.hypertable_name = h.hypertable_name AND cs.segmentby_column_index IS NOT NULL), '')
    || '|' || (SELECT d.time_interval::text FROM timescaledb_information.dimensions d WHERE d.hypertable_schema = h.hypertable_schema AND d.hypertable_name = h.hypertable_name)
    || '|' || coalesce((SELECT j.config->>'compress_after' FROM timescaledb_information.jobs j WHERE j.hypertable_schema = h.hypertable_schema AND j.hypertable_name = h.hypertable_name
                        AND j.proc_name IN ('policy_compression', 'policy_columnstore')), '')
FROM timescaledb_information.hypertables h WHERE h.hypertable_schema = 'collect' AND h.hypertable_name = $1";

            var raw = (string)(await ScalarAsync(connection, state, ct, "index_object_stats"))!;
            Assert.StartsWith("true|server_id|1 day|", raw, StringComparison.Ordinal);
            foreach (var table in ObjectUsageChain.PerSnapshotTables)
            {
                var derived = (string)(await ScalarAsync(connection, state, ct, table))!;
                Assert.Equal(raw.Replace("|server_id|", "|server_id, database_name|", StringComparison.Ordinal), derived);
            }

            Assert.Equal(0L, (long)(await ScalarAsync(connection, "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'object_usage_runs'", ct))!);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Rows older than the horizon of index_object_stats go from both derived tables and the raw table alike, younger ones stay, for the default horizon and for a per-collector override, on hypertables and on plain tables.</summary>
    [Theory]
    [InlineData(true, 0)]
    [InlineData(false, 0)]
    [InlineData(true, 10)]
    [InlineData(false, 10)]
    public async Task TheRetentionPass_PurgesTheChainAtIndexObjectStatsHorizon_AndNeverKeepsADerivedRowPastARawOne(bool timescale, int overrideDays)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG (with TimescaleDB installed) to run the live object usage chain tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        if (timescale)
        {
            Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for this test");
        }

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        if (timescale)
        {
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureObjectUsageHypertablesAsync(connection, NullLogger.Instance, ct));
        }

        var bodySucceeded = false;
        try
        {
            Func<string, int>? horizon = overrideDays == 0
                ? null
                : name => string.Equals(name, "index_object_stats", StringComparison.Ordinal) ? overrideDays : CollectorScheduleDefaults.All[name].RetentionDays;
            var days = DarlingRetention.ObjectUsageChainRetentionDays(horizon);
            Assert.Equal(overrideDays == 0 ? CollectorScheduleDefaults.All["index_object_stats"].RetentionDays : overrideDays, days);

            var ages = new[] { days + 15, days + 2, days - 1, 1, 0 };
            var seedTime = "TIMESTAMP '" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";
            var agesSql = "unnest(ARRAY[" + string.Join(",", ages.Select(a => a.ToString(CultureInfo.InvariantCulture))) + "]) AS d";
            await ExecAsync(connection, "INSERT INTO collect.object_usage_snapshots (server_id, database_name, collection_time, step_version, node_key, reasons, counted, index_resets, built_at) "
                + "SELECT " + ServerId + ", 'db', " + seedTime + " - make_interval(days => d), 1, '', 0, 0, 0, " + seedTime + " FROM " + agesSql, ct);
            await ExecAsync(connection, "INSERT INTO collect.object_usage_deltas (server_id, database_name, collection_time, object_id, index_id, reads, updates) "
                + "SELECT " + ServerId + ", 'db', " + seedTime + " - make_interval(days => d), 1, 1, 5, 5 FROM " + agesSql, ct);
            await ExecAsync(connection, "INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name) "
                + "SELECT 934900 + d, " + seedTime + " - make_interval(days => d), " + ServerId + ", 'chain-test' FROM " + agesSql, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: timescale, NullLogger.Instance, ct, horizon);

            var tables = new[] { "collect.index_object_stats", "collect.object_usage_snapshots", "collect.object_usage_deltas" };
            foreach (var table in tables)
            {
                Assert.Equal("0,1," + (days - 1).ToString(CultureInfo.InvariantCulture), (string)(await ScalarAsync(connection,
                    "SELECT string_agg(d::text, ',' ORDER BY d) FROM (SELECT (" + seedTime + ")::date - collection_time::date AS d FROM " + table + " WHERE server_id = " + ServerId + ") x", ct))!);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct, string? parameter = null)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (parameter is not null)
        {
            command.Parameters.AddWithValue(parameter);
        }

        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }
}
