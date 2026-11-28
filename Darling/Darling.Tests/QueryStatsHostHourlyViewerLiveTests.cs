/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.QueryStatsHostHourlyLiveTests;

namespace Darling.Tests;

/// <summary>
/// #9164: the hourly host rollup's migration applied twice leaves the same schema, and the Viewer reads the current schema version from that
/// store. The test names the Viewer, which the build without the Viewer leaves out (#9686), so it is a class of its own and the rest of
/// <see cref="QueryStatsHostHourlyLiveTests"/> runs on Linux. It uses that class's connection string and SQL helpers.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStatsHostHourlyViewerLiveTests
{
    /// <summary>The migration, run twice over a store that has no trace of it, leaves the same tables, columns and indexes, and no rows.</summary>
    [Fact]
    public async Task TheMigration_AppliedTwice_LeavesTheSameSchema()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        /* Found by its version, not as the newest rung, so a later rung does not break this test. */
        var rung = PgMigrations.Scripts.Single(s => s.Version == QueryStatsHostHourly.Version);

        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.query_stats_host_hourly; DROP TABLE IF EXISTS collect.query_stats_host_hourly_built;", ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        var once = await SchemaAsync(connection, ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        var twice = await SchemaAsync(connection, ct);

        Assert.NotEmpty(once);
        Assert.Equal(once, twice);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built", ct));

        /* V287's DDL makes the hash index V322 (#9863) drops, so a store that ran V287 again reads one rung low until V322 runs again. */
        await ExecuteAsync(connection, PgMigrations.Scripts.Single(s => s.Version == HostHourlyHashIndexDrop.Version).Sql, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));
    }
}
