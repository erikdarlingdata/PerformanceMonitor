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
using System.Reflection;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #3477 scope column's live round-trip, split from <see cref="CollectorDatabaseScopeRungTests"/>
/// per the LivePostgresCollectionHygieneTests preferred shape: this test writes the SHARED
/// DARLING_TEST_PG store directly (config_service, config_collector_schedules), so it serializes
/// against the other live classes — and the rung file's eleven pure pins have no business paying
/// that serialization for one live arm.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorDatabaseScopeLivePostgresTests
{
    /* ---- live (DARLING_TEST_PG): the column round-trips with NULL and empty distinct ------------------- */

    /// <summary>
    /// Against a real store: the ladder lands the column, the viewer's own upsert SQL round-trips a
    /// list, an explicit empty array and NULL as three DIFFERENT stored readings through the service's
    /// own select, and a scope write bumps the V17 reload beacon — the live half of the "a store write
    /// is honored on the next sweep" claim. Rolled back, so the shared dev store is never mutated.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task TheScopeColumn_RoundTripsLive_WithNullAndEmptyDistinct_AndBumpsTheBeacon()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the scope round-trip test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var tx = await connection.BeginTransactionAsync(ct);

        await using (var seed = new NpgsqlCommand(
            "INSERT INTO config.config_service (id, updated_at) VALUES (1, now() AT TIME ZONE 'UTC') ON CONFLICT (id) DO NOTHING",
            connection, tx))
        {
            await seed.ExecuteNonQueryAsync(ct);
        }

        long Beacon()
        {
            using var read = new NpgsqlCommand("SELECT config_version FROM config.config_service WHERE id = 1", connection, tx);
            return Convert.ToInt64(read.ExecuteScalar(), CultureInfo.InvariantCulture);
        }

        var before = Beacon();

        /* The viewer's own upsert SQL, exercised verbatim: a fleet row carrying a list, and a server
           row carrying the EXPLICIT empty array. Schema-qualified via search_path defaulting to the
           connection role's — the viewer runs these against config through its own search path, so
           set it the way the constants expect a bare table name to resolve. */
        await using (var path = new NpgsqlCommand("SET LOCAL search_path = config, public", connection, tx))
        {
            await path.ExecuteNonQueryAsync(ct);
        }

        await using (var fleet = new NpgsqlCommand(ViewerDataService.CollectorScheduleFleetUpsertSql, connection, tx))
        {
            fleet.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "index_object_stats" });
            fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            fleet.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = true });
            fleet.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = new[] { "RefDb" } });
            await fleet.ExecuteNonQueryAsync(ct);
        }

        await using (var server = new NpgsqlCommand(ViewerDataService.CollectorScheduleServerUpsertSql, connection, tx))
        {
            server.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 424242 });
            server.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "index_object_stats" });
            server.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            server.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
            server.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = true });
            server.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = Array.Empty<string>() });
            await server.ExecuteNonQueryAsync(ct);
        }

        Assert.True(Beacon() > before, "a scope write must bump config_version — the live-reload claim");

        /* Read back through the SERVICE's column order and prove all three readings are distinct. */
        var overrides = new List<ScheduleOverride>();
        await using (var read = new NpgsqlCommand(
            "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled, databases " +
            "FROM config.config_collector_schedules WHERE collector_name = 'index_object_stats' AND (server_id = 424242 OR server_id IS NULL)",
            connection, tx))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                overrides.Add(new ScheduleOverride(
                    reader.IsDBNull(0) ? null : reader.GetInt32(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    reader.GetBoolean(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<string[]>(5)));
            }
        }

        var fleetRow = overrides.Single(o => o.ServerId is null);
        var serverRow = overrides.Single(o => o.ServerId == 424242);
        Assert.Equal(new[] { "RefDb" }, fleetRow.Databases);
        Assert.NotNull(serverRow.Databases);
        Assert.Empty(serverRow.Databases!);

        /* And the resolver reads them the documented way: the explicit empty stops the fall-through
           on the written server, while every OTHER server inherits the fleet scope. */
        Assert.Empty(StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 424242, overrides));
        Assert.Equal(new[] { "RefDb" }, StoreConfigProvider.ResolveDatabaseScope("index_object_stats", 777, overrides));

        await tx.RollbackAsync(ct);
    }

    /// <summary>A minimal context with the required members satisfied — the DeadlocksPlanSpliceTests
    /// idiom. Everything the scope predicate reads is what the caller passes.</summary>
}
