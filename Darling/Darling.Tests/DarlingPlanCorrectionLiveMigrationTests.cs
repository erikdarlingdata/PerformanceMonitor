/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling service.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Proves V46 (#1952) against a real store on BOTH paths, because they build the table by different code
/// and only one of them is exercised by an ordinary migrate: on a FRESH store V1's generated schema
/// already created <c>collect.plan_correction</c> and V46 is a no-op, so nothing anywhere else ever runs
/// <c>V46Sql</c>. The upgrade case below is the only thing that does.
///
/// <para>A drift between the two is silent and severe. Both writers are POSITIONAL — the appender binds by
/// ordinal, not by name — so a hand-written literal that disagreed with the generator by one column or one
/// type would put every value in the wrong column on exactly the stores that upgraded, while fresh stores
/// stayed correct. <c>DarlingPlanCorrectionMigrationTests</c> compares the two as strings; this compares
/// what PostgreSQL actually built.</para>
///
/// <para>Serialized into the live-postgres collection and deliberately narrow: it drops and re-creates only
/// <c>plan_correction</c> and only its own <c>darling_schema_version</c> row, and leaves the store fully
/// migrated. No other test reads this table (it ships in this same change), so the window cannot affect
/// anything else in the collection.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPlanCorrectionLiveMigrationTests
{
    /// <summary>The columns a correct plan_correction has, derived from the catalog rather than restated.</summary>
    private static List<(string Name, string Type)> ExpectedColumns()
    {
        var columns = new List<(string, string)>
        {
            ("collection_id", "bigint"),
            ("collection_time", "timestamp without time zone"),
            ("server_id", "integer"),
            ("server_name", "text"),
        };

        foreach (var column in PlanCorrectionCollector.Instance.PayloadColumns)
        {
            columns.Add((column.Name, PgSchemaGenerator.TypeFor(column) switch
            {
                "timestamp" => "timestamp without time zone",
                var other => other,
            }));
        }

        return columns;
    }

    [Fact]
    public async Task FreshStore_AndUpgradedStore_BuildTheSamePlanCorrectionTable_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to verify the V46 plan_correction migration.");

        var cancellationToken = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        /* --- Path 1: the store as an ordinary migrate leaves it (fresh here, catalog-generated). --- */
        await PgMigrations.MigrateAsync(connection, cancellationToken);

        Assert.Equal(StorageVersion.SchemaVersion, await CurrentVersionAsync(connection, cancellationToken));
        var fromFreshSchema = await ReadColumnsAsync(connection, cancellationToken);
        Assert.Equal(ExpectedColumns(), fromFreshSchema);

        /* --- Path 2: wind this ONE table back to its pre-V46 state and let the migration rebuild it. --- */
        /* The stamp rows for 46 AND everything above it have to go, not just 46's: the applier reads
           MAX(version) and skips anything at or below it, so leaving 47 stamped would make the rewind a
           no-op that still asserted green. Only plan_correction is dropped — V47's own objects survive and
           its re-run is a no-op, which is the point of every migration being idempotent. */
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.plan_correction", cancellationToken);
        await ExecuteAsync(connection, "DELETE FROM collect.darling_schema_version WHERE version >= 46", cancellationToken);

        Assert.Empty(await ReadColumnsAsync(connection, cancellationToken));
        Assert.Equal(44, await CurrentVersionAsync(connection, cancellationToken));

        var applied = await PgMigrations.MigrateAsync(connection, cancellationToken);

        /* Exactly the scripts above 44 ran — V46, V47, V48 (#1984), V49 (#1986 database-state alert),
           V50 (#2008 2a server-tag colour), V51 (#2012 stage 2 query-stats host object), V52 (#2060
           persisted finding drill-down), V53 (#2068 store self-metrics), V54 (#2069 plan-dim gzip), and
           V55 (#2107 self-alert knobs). If the applier had stumbled over the permanent V45 gap it would
           either re-run everything above 1 or nothing at all, and both show up right here. Version-agnostic
           on purpose: the ladder-top pin lives in ScaffoldTests, and this count just tracks it. */
        Assert.Equal(PgMigrations.Scripts.Count(m => m.Version > 44), applied);
        Assert.Equal(StorageVersion.SchemaVersion, await CurrentVersionAsync(connection, cancellationToken));

        var fromMigration = await ReadColumnsAsync(connection, cancellationToken);
        Assert.Equal(ExpectedColumns(), fromMigration);

        /* The two paths agree column-for-column, which is the invariant the positional appender needs. */
        Assert.Equal(fromFreshSchema, fromMigration);

        /* And the retrieval index came with it — retention and every windowed read depend on it. */
        Assert.True(await IndexExistsAsync(connection, "idx_plan_correction_time", cancellationToken),
            "V46 created collect.plan_correction without idx_plan_correction_time");
    }

    private static async Task<List<(string Name, string Type)>> ReadColumnsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'collect' AND table_name = 'plan_correction'
ORDER BY ordinal_position";

        var columns = new List<(string, string)>();
        using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add((reader.GetString(0), reader.GetString(1)));
        }

        return columns;
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", connection);
        command.Parameters.AddWithValue(indexName);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) > 0;
    }

    private static async Task<int> CurrentVersionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand("SELECT COALESCE(MAX(version), 0) FROM collect.darling_schema_version", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
