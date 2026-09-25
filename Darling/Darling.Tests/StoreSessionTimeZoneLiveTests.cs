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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4277 live proof: a STORE connection opened the way the product opens one
/// (<c>DarlingStoreConnection.PinSessionTimeZoneUtc</c>) reports session <c>timezone</c> UTC even when the
/// DATABASE's own default is <c>America/New_York</c> — proven on a #1776 own-store scratch database
/// (<c>ALTER DATABASE ... SET timezone</c>, not the shared rig conf, so it means something on a UTC CI
/// runner too, not only a rig deliberately set to New York).
///
/// <para>The second assertion exercises the one bare-<c>now()</c> store SQL predicate
/// <see cref="StoreSqlClockDisciplineTests"/> waives on ARITHMETIC margin, not on zone-correctness —
/// <see cref="DarlingModuleMap.RefreshSql"/>'s <c>collection_time >= now() - interval '2 days'</c>. Pinned to
/// that exact text by an <c>Assert.Contains</c> so a future change to the interval cannot silently stale this
/// proof. Two rows straddle the 48h boundary with a 2h margin either side (inside: 46h old, outside: 50h
/// old) rather than one row safely in the middle, because a naive column read under a WEST-of-UTC session
/// (America/New_York is UTC-4 in September) is interpreted as YOUNGER than its true UTC meaning — the window
/// only ever WIDENS, so it only ever ADMITS more rows. A single "inside" row stays admitted under either
/// session and cannot discriminate; only the "outside" row's wrong admission does — which is also why this
/// predicate is safely waived rather than a bug: RefreshSql's own remarks show its worst case (34-60h) still
/// covers what it needs regardless of offset, but that tolerance is a fact about ITS margin, not a licence
/// for a naive-to-now() comparison to ignore the session zone in general.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StoreSessionTimeZoneLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AStoreConnection_PinsToUtc_EvenWhenTheDatabaseDefaultIsAmericaNewYork()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(
            string.IsNullOrEmpty(baseCs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4277 store-timezone-pin test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: a database-level default this test sets must not leak into any other test that
           shares DARLING_TEST_PG's own database. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);

        await using (var admin = new NpgsqlConnection(baseCs))
        {
            await admin.OpenAsync(ct);
            await using var alter = new NpgsqlCommand(
                $"ALTER DATABASE \"{scratch.DatabaseName}\" SET timezone = 'America/New_York'", admin);
            await alter.ExecuteNonQueryAsync(ct);
        }

        /* ALTER DATABASE ... SET only takes effect for sessions opened AFTER it commits, so migrate through a
           fresh connection rather than the admin one above. */
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        /* The database's own default, UNPINNED — confirms the fixture set up a genuinely non-UTC store
           before asserting that the product's pin overrides it. */
        await using (var unpinned = new NpgsqlConnection(scratch.ConnectionString))
        {
            await unpinned.OpenAsync(ct);
            Assert.Equal("America/New_York", await ShowTimeZoneAsync(unpinned, ct));
        }

        /* The way the product opens a STORE connection: PinSessionTimeZoneUtc first, then the connection. */
        var pinnedConnectionString = DarlingStoreConnection.PinSessionTimeZoneUtc(scratch.ConnectionString);
        await using var connection = new NpgsqlConnection(pinnedConnectionString);
        await connection.OpenAsync(ct);

        Assert.Equal("UTC", await ShowTimeZoneAsync(connection, ct));

        Assert.Contains(
            "collection_time >= now() - interval '2 days'", DarlingModuleMap.RefreshSql, StringComparison.Ordinal);

        await using (var create = new NpgsqlCommand(
            "CREATE TEMP TABLE probe_4277 (collection_time timestamp NOT NULL)", connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        /* Kind=Unspecified so Npgsql binds `timestamp` (naive), never `timestamptz` — the same naive-UTC
           digits every collector row carries, not a value that shifts on its own before the comparison even
           runs. */
        var insideWindow = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-46), DateTimeKind.Unspecified);
        var outsideWindow = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-50), DateTimeKind.Unspecified);

        await using (var insert = new NpgsqlCommand(
            "INSERT INTO probe_4277 (collection_time) VALUES ($1), ($2)", connection))
        {
            insert.Parameters.AddWithValue(insideWindow);
            insert.Parameters.AddWithValue(outsideWindow);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var read = new NpgsqlCommand(
            "SELECT count(*) FILTER (WHERE collection_time = $1), count(*) FILTER (WHERE collection_time = $2) "
            + "FROM probe_4277 WHERE collection_time >= now() - interval '2 days'", connection);
        read.Parameters.AddWithValue(insideWindow);
        read.Parameters.AddWithValue(outsideWindow);
        await using var reader = await read.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        Assert.Equal(1L, reader.GetInt64(0)); // 46h old, inside the 48h window: returned, as #4277 asks.
        Assert.Equal(0L, reader.GetInt64(1)); // 50h old, past the 48h window: NOT returned — only true under UTC.
    }

    private static async Task<string> ShowTimeZoneAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SHOW timezone", connection);

        return (string)(await command.ExecuteScalarAsync(ct))!;
    }
}
