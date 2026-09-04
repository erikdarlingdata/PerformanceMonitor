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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of the V110 rung (#2860): it applies to a store built by the product's own applier, it is
/// IDEMPOTENT, and it lands its ten columns as nullable integers on BOTH the base table and the passthrough
/// view every read goes through.
///
/// <para><b>Split out of <see cref="CollectionLogFetchPhaseSumsStoreTests"/> rather than serialized alongside
/// it</b>, which is what #1776's hygiene rule asks for when a class is mostly pure with one live test: the
/// eleven pins next door are pure source-and-SQL assertions that have no business waiting on the shared store,
/// and <c>[Collection("live-postgres")]</c> would have serialized every one of them. The shape more than forty
/// files here already use.</para>
///
/// <para><b>Being IN the collection is load-bearing twice over.</b> It serializes against the sixty-odd other
/// live classes, which is the point of the rule - this class calls <c>MigrateAsync</c> on the shared database,
/// and a migration racing another class's reads is the moving flake #1776 was filed about. It also means
/// <see cref="LivePostgresStoreFixture"/> has ALREADY migrated the store before this runs, so the two calls
/// below are the store's second and third applications rather than its first - which is a stronger
/// idempotency claim than this test could make on its own, and it is worth knowing that the assertion is
/// reading a store the fixture established rather than one this class built.</para>
///
/// <para><b>The view assertion is the one that earns its keep.</b> Postgres FREEZES a view's <c>SELECT *</c>
/// column list at CREATE, so an upgraded store whose rung forgot the refresh has the columns on the table and
/// NOT on the view - working perfectly on a fresh store and invisibly broken on every existing one. V14 exists
/// because that already happened; V80, V108 and V109 each re-learned it on this very table. Asserting the
/// table alone would pass through exactly that mistake.</para>
///
/// <para>Creates nothing and drops nothing, so it leaves no residue for the fixture's teardown diff to find:
/// the ALTER is already applied and idempotent, and everything else here reads
/// <c>information_schema</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionLogFetchPhaseSumsLivePostgresTests
{
    [Fact]
    public async Task TheRungIsIdempotent_AndLandsOnBothTheTableAndTheView_AgainstLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V110 live migration test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        /* The product's own applier, not hand-run DDL - the whole point is that this is the path a real store
           takes. Idempotent by contract, so calling it on an already-current store is a no-op and calling it
           twice must stay one. */
        await PgMigrations.MigrateAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(StorageVersion.SchemaVersion, await ScalarIntAsync(
            connection, "SELECT COALESCE(MAX(version), 0) FROM collect.darling_schema_version", ct));

        /* Exactly one ladder row for this rung after repeated passes. A rung applied twice would show two. */
        Assert.Equal(1, await ScalarIntAsync(
            connection,
            "SELECT count(*) FROM collect.darling_schema_version WHERE version = "
            + CollectionLogFetchPhaseSumsStoreTests.RungVersion.ToString(CultureInfo.InvariantCulture),
            ct));

        foreach (var relation in new[] { "collection_log", "v_collection_log" })
        {
            var found = await ColumnTypesAsync(connection, relation, ct);

            foreach (var column in CollectionLogFetchPhaseSumsStoreTests.ExpectedColumns)
            {
                Assert.True(found.TryGetValue(column, out var type),
                    $"collect.{relation} is missing {column} after MigrateAsync. On the VIEW this is the "
                    + "frozen-SELECT-* failure: Postgres fixes a view's column list at CREATE, so a rung "
                    + "without the CREATE OR REPLACE refresh leaves an upgraded store's reads blind to "
                    + "columns that are present on the table.");

                Assert.Equal("integer", type.DataType);
                Assert.True(type.IsNullable,
                    $"{column} must be nullable: NULL is how a row says it performed no deferred fetch, "
                    + "which is a different fact from a fetch that cost nothing.");
            }
        }
    }

    private static async Task<int> ScalarIntAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<Dictionary<string, (string DataType, bool IsNullable)>> ColumnTypesAsync(
        NpgsqlConnection connection, string relation, CancellationToken ct)
    {
        var found = new Dictionary<string, (string, bool)>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'collect' AND table_name = $1
            """, connection);
        command.Parameters.Add(new NpgsqlParameter { Value = relation });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found[reader.GetString(0)] = (reader.GetString(1),
                string.Equals(reader.GetString(2), "YES", StringComparison.Ordinal));
        }

        return found;
    }
}
