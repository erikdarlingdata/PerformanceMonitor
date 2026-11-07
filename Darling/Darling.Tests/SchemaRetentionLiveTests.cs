/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643 on a live store: the retention of closed schema versions. A closed version (valid_to set) older than the collector's retention
/// goes with its child rows, a current version never goes however old, and the collector's fleet-wide retention override applies.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class SchemaRetentionLiveTests
{
    private static readonly DateTime s_now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the schema retention live tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* One version of dbo.<name> with one column of each current flag; validTo null is a current version. */
    private static async Task SeedVersionAsync(NpgsqlConnection c, string name, DateTime validFrom, DateTime? validTo, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
WITH v AS (
    INSERT INTO collect.schema_objects (server_id, database_name, object_id, schema_name, object_name, object_type, valid_from, valid_to,
                                        change_kind, detected_by, detected_on_server_id, definition_hash)
    VALUES (1, 'SalesDb', abs(hashtext($1)), 'dbo', $1, 'U', $2, $3, 'altered', 'reconcile', 1, '\x00')
    RETURNING version_id)
INSERT INTO collect.schema_columns (version_id, is_current, ordinal, name, type_name, is_nullable)
SELECT version_id, $3 IS NULL, 1, 'Id', 'int', false FROM v", c);
        insert.Parameters.Add(new NpgsqlParameter { Value = name });
        insert.Parameters.Add(new NpgsqlParameter { Value = validFrom, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        insert.Parameters.Add(new NpgsqlParameter { Value = (object?)validTo ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ObjectNamesAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT coalesce(string_agg(object_name, ',' ORDER BY object_name), '') FROM collect.schema_objects", c);
        return (string)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> ColumnRowsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM collect.schema_columns", c);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task Purge_RemovesClosedVersionsOlderThanRetention_WithTheirChildren()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedVersionAsync(c, "OldClosed", s_now.AddDays(-90), s_now.AddDays(-60), ct);
            await SeedVersionAsync(c, "RecentClosed", s_now.AddDays(-20), s_now.AddDays(-10), ct);
            await SeedVersionAsync(c, "Live", s_now.AddDays(-5), null, ct);
            Assert.Equal(3L, await ColumnRowsAsync(c, ct));

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            Assert.Equal("Live,RecentClosed", await ObjectNamesAsync(c, ct));
            Assert.Equal(2L, await ColumnRowsAsync(c, ct));
        });
    }

    [Fact]
    public async Task Purge_NeverRemovesCurrentVersions_HoweverOld()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedVersionAsync(c, "AncientLive", s_now.AddDays(-3000), null, ct);
            await SeedVersionAsync(c, "AncientClosed", s_now.AddDays(-3000), s_now.AddDays(-2900), ct);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            Assert.Equal("AncientLive", await ObjectNamesAsync(c, ct));
            Assert.Equal(1L, await ColumnRowsAsync(c, ct));
        });
    }

    [Fact]
    public async Task Purge_UsesTheCollectorsRetentionOverride()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedVersionAsync(c, "TwoDaysClosed", s_now.AddDays(-5), s_now.AddDays(-2), ct);
            await SeedVersionAsync(c, "HourClosed", s_now.AddDays(-5), s_now.AddHours(-1), ct);

            /* Default 30 days: both stay. */
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);
            Assert.Equal("HourClosed,TwoDaysClosed", await ObjectNamesAsync(c, ct));

            var overrides = new[] { new ScheduleOverride(null, "schema_objects", null, 1, true) };
            await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, null, ct,
                retentionDaysFor: name => StoreConfigProvider.ResolveFleetRetentionDays(name, overrides));

            Assert.Equal("HourClosed", await ObjectNamesAsync(c, ct));
            Assert.Equal(1L, await ColumnRowsAsync(c, ct));
        });
    }
}
