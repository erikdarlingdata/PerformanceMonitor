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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: the worker's read of a server's clock (<see cref="DarlingWorker.TryReadServerClockAsync"/>) against a migrated
/// store, for a SQL Server target (its newest <c>server_properties</c> row that has an offset) and a PostgreSQL target
/// (its newest server-wide <c>TimeZone</c> row in <c>pg_server_config</c>). A collector's run time is a time on that
/// clock, so a clock that reads as UTC while the server keeps another zone moves the collector's slot by the server's
/// offset. The read answers from the newest row however old that row is: a server whose snapshots stopped a week or
/// a season ago still has the clock it last reported, and only a server with no row at all reads as UTC.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class DarlingWorkerServerClockLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the worker's server clock read against a store (each fact mints its own scratch database).";

    private const string NewYork = "America/New_York";
    private const string Tokyo = "Asia/Tokyo";
    private const string Paris = "Europe/Paris";
    private const string Kolkata = "Asia/Kolkata";
    private const string Chicago = "America/Chicago";

    private static string IdOf(string zone) => ServerClock.Resolve(zone, null).AsTimeZone().Id;

    private static string IdOfOffset(int offsetMinutes) => ServerClock.Resolve(null, offsetMinutes).AsTimeZone().Id;

    /// <summary>A machine whose zone database cannot name an IANA zone resolves every one of them to UTC, and a test
    /// that compares against UTC proves nothing there.</summary>
    private static bool ResolvesIanaZones() =>
        IdOf(NewYork) != TimeZoneInfo.Utc.Id && IdOf(Tokyo) != TimeZoneInfo.Utc.Id
        && IdOf(Paris) != TimeZoneInfo.Utc.Id && IdOf(Kolkata) != TimeZoneInfo.Utc.Id && IdOf(Chicago) != TimeZoneInfo.Utc.Id;

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>One <c>TimeZone</c> row of a PostgreSQL target's config snapshot. A row with a <paramref name="databaseName"/>
    /// is a per-database override of the setting, which says nothing about the server's own clock.</summary>
    private static async Task SeedTimeZoneAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string setting, string source,
        CancellationToken ct, string? databaseName = null)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO pg_server_config
                (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
                 source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc, database_name, role_name)
            VALUES ($1, $2, $3, $4, 'TimeZone', $5, NULL, 'Client Connection Defaults / Locale and Formatting', 'user', 'string',
                    $6, 'GMT', $5, NULL, 0, FALSE, NULL, $7, NULL)
            """, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(Naive(collectionTimeUtc));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("pg" + serverId);
        command.Parameters.AddWithValue(setting);
        command.Parameters.AddWithValue(source);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)databaseName ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One <c>server_properties</c> row of a SQL Server target. A null offset is a snapshot that carries none.</summary>
    private static async Task SeedPropertiesAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, int? utcOffsetMinutes, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
            VALUES ($1, $2, $3, $4, $5)
            """, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(Naive(collectionTimeUtc));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("sql" + serverId);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)utcOffsetMinutes ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ClockIdAsync(NpgsqlDataSource postgres, int serverId, CollectorTargetEngine engine, CancellationToken ct)
    {
        var stamp = await DarlingWorker.TryReadServerClockAsync(postgres, serverId, engine, logger: null, ct);
        Assert.NotNull(stamp);
        return stamp.Id;
    }

    /// <summary>A PostgreSQL target keeps its clock after its config collector stops: a snapshot ten days old, and one a hundred
    /// days old, are still the server's clock, and the newest snapshot wins when there are several. Only a target with no
    /// server-wide <c>TimeZone</c> row reads as UTC.</summary>
    [Fact]
    public async Task APostgresClock_IsReadHoweverOldItsNewestSnapshotIs_AndTheNewestSnapshotWins()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        Assert.SkipUnless(ResolvesIanaZones(), "This machine cannot resolve IANA zone names, so a zone cannot be told from UTC here.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int tenDays = 480_201, hundredDays = 480_202, newestWins = 480_203, none = 480_204;
            var now = DateTime.UtcNow;

            /* The config collector ran last ten days ago: the clock is still the one it reported. */
            await SeedTimeZoneAsync(connection, tenDays, now.AddDays(-10), NewYork, "configuration file", ct);
            /* ...and a hundred days ago. */
            await SeedTimeZoneAsync(connection, hundredDays, now.AddDays(-100), Kolkata, "configuration file", ct);
            /* Two snapshots: the zone changed, and the newest one is the clock. */
            await SeedTimeZoneAsync(connection, newestWins, now.AddDays(-40), Tokyo, "configuration file", ct);
            await SeedTimeZoneAsync(connection, newestWins, now.AddHours(-2), Paris, "configuration file", ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            Assert.Equal(IdOf(NewYork), await ClockIdAsync(postgres, tenDays, CollectorTargetEngine.PostgreSql, ct));
            Assert.Equal(IdOf(Kolkata), await ClockIdAsync(postgres, hundredDays, CollectorTargetEngine.PostgreSql, ct));
            Assert.Equal(IdOf(Paris), await ClockIdAsync(postgres, newestWins, CollectorTargetEngine.PostgreSql, ct));
            Assert.Equal(DarlingWorker.ServerClockStamp.Utc.Id, await ClockIdAsync(postgres, none, CollectorTargetEngine.PostgreSql, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A per-database override of <c>TimeZone</c> and a session-scoped value are not the server's clock. They are
    /// left out, so a target that has only those reads as UTC, and a server-wide row beside them is the one used.</summary>
    [Fact]
    public async Task APostgresClock_LeavesOutAnOverrideRow_AndASessionScopedRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        Assert.SkipUnless(ResolvesIanaZones(), "This machine cannot resolve IANA zone names, so a zone cannot be told from UTC here.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int overrideOnly = 480_211, sessionOnly = 480_212, serverWideBesideOverride = 480_213;
            var now = DateTime.UtcNow;

            await SeedTimeZoneAsync(connection, overrideOnly, now.AddHours(-1), Chicago, "database", ct, databaseName: "tenant1");
            await SeedTimeZoneAsync(connection, sessionOnly, now.AddHours(-1), Tokyo, "client", ct);
            await SeedTimeZoneAsync(connection, serverWideBesideOverride, now.AddDays(-3), Paris, "configuration file", ct);
            await SeedTimeZoneAsync(connection, serverWideBesideOverride, now.AddHours(-1), Chicago, "database", ct, databaseName: "tenant1");

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            Assert.Equal(DarlingWorker.ServerClockStamp.Utc.Id, await ClockIdAsync(postgres, overrideOnly, CollectorTargetEngine.PostgreSql, ct));
            Assert.Equal(DarlingWorker.ServerClockStamp.Utc.Id, await ClockIdAsync(postgres, sessionOnly, CollectorTargetEngine.PostgreSql, ct));
            Assert.Equal(IdOf(Paris), await ClockIdAsync(postgres, serverWideBesideOverride, CollectorTargetEngine.PostgreSql, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A SQL Server target's clock is its newest <c>server_properties</c> row that has an offset, with no age limit:
    /// a row thirty days old is still the offset the server last reported, and a newer snapshot that carries no offset does
    /// not hide it.</summary>
    [Fact]
    public async Task ASqlServerClock_IsReadHoweverOldItsNewestRowIs_AndTheNewestRowWithAnOffsetWins()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);
        var bodySucceeded = false;
        try
        {
            const int thirtyDays = 480_221, newestWins = 480_222, newerWithoutOffset = 480_223, none = 480_224;
            var now = DateTime.UtcNow;

            await SeedPropertiesAsync(connection, thirtyDays, now.AddDays(-30), -300, ct);
            await SeedPropertiesAsync(connection, newestWins, now.AddDays(-30), -300, ct);
            await SeedPropertiesAsync(connection, newestWins, now.AddDays(-20), -240, ct);
            await SeedPropertiesAsync(connection, newerWithoutOffset, now.AddDays(-30), 330, ct);
            await SeedPropertiesAsync(connection, newerWithoutOffset, now.AddHours(-1), null, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            Assert.Equal(IdOfOffset(-300), await ClockIdAsync(postgres, thirtyDays, CollectorTargetEngine.SqlServer, ct));
            Assert.Equal(IdOfOffset(-240), await ClockIdAsync(postgres, newestWins, CollectorTargetEngine.SqlServer, ct));
            Assert.Equal(IdOfOffset(330), await ClockIdAsync(postgres, newerWithoutOffset, CollectorTargetEngine.SqlServer, ct));
            Assert.Equal(DarlingWorker.ServerClockStamp.Utc.Id, await ClockIdAsync(postgres, none, CollectorTargetEngine.SqlServer, ct));
            Assert.NotEqual(DarlingWorker.ServerClockStamp.Utc.Id, IdOfOffset(-300));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
