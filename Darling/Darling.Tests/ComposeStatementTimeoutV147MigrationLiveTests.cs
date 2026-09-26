/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V147 rung (#4442) at runtime: a store at V146 with the old shipped default (15) moves to 60 on
/// upgrade and picks up the new column DEFAULT; a store an operator deliberately left at 45 is untouched;
/// and, through the product's own re-assertion path (<see cref="DarlingManagedRoles.BuildComposeStatementTimeoutSql"/>,
/// the same statement <see cref="DarlingManagedRoles.ReassertComposeStatementTimeoutAsync"/> runs on a
/// control-plane reload), <c>pg_db_role_setting</c> shows the new ceiling on both <c>viewer</c> and <c>mcp</c>
/// after the upgrade.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database
   through ScratchPostgres and never touches the shared store's tables, so it cannot race the live collection
   and serializing it would be pure slowdown. */
public sealed class ComposeStatementTimeoutV147MigrationLiveTests
{
    private const int RungVersion = 147;
    private const int PreviousVersion = 146;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AStoreAtTheOldShippedDefault_MovesTo60_AndTheColumnDefaultMoves()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V147 upgrade round-trip.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* Climb to the top, then roll back JUST this rung's two effects — the column default and the
           version stamp — so the store looks exactly like a V146 store that shipped the old 15 default and
           whose config_service row was never touched by an operator. */
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedConfigServiceRowAsync(scratch.ConnectionString, ct);
        await ExecAsync(connection, ct, "ALTER TABLE config.config_service ALTER COLUMN compose_statement_timeout_seconds SET DEFAULT 15");
        await ExecAsync(connection, ct, "UPDATE config.config_service SET compose_statement_timeout_seconds = 15");
        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion);

        using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
        {
            Assert.Equal(PreviousVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct)));
        }

        /* The upgrade: apply every rung above V146, which is exactly V147 on this build. */
        var applied = await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= RungVersion), applied);

        using (var value = new NpgsqlCommand("SELECT compose_statement_timeout_seconds FROM config.config_service", connection))
        {
            Assert.Equal(60, Convert.ToInt32(await value.ExecuteScalarAsync(ct)));
        }

        using (var columnDefault = new NpgsqlCommand(
            "SELECT column_default FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_service' AND column_name = 'compose_statement_timeout_seconds'",
            connection))
        {
            Assert.Equal("60", Convert.ToString(await columnDefault.ExecuteScalarAsync(ct)));
        }
    }

    [Fact]
    public async Task AStoreAnOperatorLeftAt45_StaysAt45()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V147 operator-value round-trip.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await PgMigrations.MigrateAsync(connection, ct);
        await SeedConfigServiceRowAsync(scratch.ConnectionString, ct);
        await ExecAsync(connection, ct, "ALTER TABLE config.config_service ALTER COLUMN compose_statement_timeout_seconds SET DEFAULT 15");
        /* The operator's own deliberate value — not the old shipped default, and not the new one either —
           which V147's WHERE clause must leave alone. */
        await ExecAsync(connection, ct, "UPDATE config.config_service SET compose_statement_timeout_seconds = 45");
        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion);

        var applied = await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= RungVersion), applied);

        using var value = new NpgsqlCommand("SELECT compose_statement_timeout_seconds FROM config.config_service", connection);
        Assert.Equal(45, Convert.ToInt32(await value.ExecuteScalarAsync(ct)));
    }

    /// <summary>
    /// The upgrade alone changes no role — the ceiling is applied through role provisioning DDL, deliberately
    /// not schema (see <see cref="ComposeStatementTimeoutStoreTests"/>'s class summary). What reaches a live
    /// role is the product's own re-assertion path, run here exactly as a control-plane reload runs it, and
    /// checked through the catalog (<c>pg_db_role_setting</c>) rather than a fresh session's <c>current_setting</c>
    /// — a role SET only takes effect on the NEXT session, so reading it back on the SAME connection that ran
    /// the ALTER ROLE would prove nothing about what a reconnecting viewer or mcp actually gets.
    /// </summary>
    [Fact]
    public async Task TheRoleReassertion_SetsBothRolesTo60sAnd5000ms_ThroughPgDbRoleSetting()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V147 role re-assertion round-trip.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await ExecAsync(connection, ct, "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'viewer') THEN CREATE ROLE viewer LOGIN; END IF; END $$");
            await ExecAsync(connection, ct, "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp') THEN CREATE ROLE mcp LOGIN; END IF; END $$");
            /* Old shipped value, as if provisioning last ran before this build existed — the state the
               re-assertion must move forward. */
            await ExecAsync(connection, ct, "ALTER ROLE viewer SET statement_timeout = '15s'");
            await ExecAsync(connection, ct, "ALTER ROLE mcp SET statement_timeout = '15s'");
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        var reasserted = await DarlingManagedRoles.ReassertComposeStatementTimeoutAsync(dataSource, 60, logger, ct);
        Assert.True(reasserted);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            foreach (var role in new[] { "viewer", "mcp" })
            {
                using var setting = new NpgsqlCommand(
                    @"SELECT s.setting FROM pg_db_role_setting d
                        JOIN pg_roles r ON r.oid = d.setrole
                        CROSS JOIN LATERAL unnest(d.setconfig) AS s(setting)
                       WHERE r.rolname = $1 AND s.setting LIKE 'statement_timeout=%'",
                    connection);
                setting.Parameters.AddWithValue(role);
                Assert.Equal("statement_timeout=60s", Convert.ToString(await setting.ExecuteScalarAsync(ct)));

                using var slow = new NpgsqlCommand(
                    @"SELECT s.setting FROM pg_db_role_setting d
                        JOIN pg_roles r ON r.oid = d.setrole
                        CROSS JOIN LATERAL unnest(d.setconfig) AS s(setting)
                       WHERE r.rolname = $1 AND s.setting LIKE 'log_min_duration_statement=%'",
                    connection);
                slow.Parameters.AddWithValue(role);
                Assert.Equal("log_min_duration_statement=5000ms", Convert.ToString(await slow.ExecuteScalarAsync(ct)));
            }
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Puts a config_service row on the scratch store through the product's own seeding path
    /// (<see cref="StoreConfigProvider.SeedIfEmptyAsync"/>) rather than a hand-written INSERT, so this pin
    /// does not need to track that row's other NOT NULL columns as the store evolves.</summary>
    private static async Task SeedConfigServiceRowAsync(string connectionString, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        var provider = new StoreConfigProvider(dataSource);
        await provider.SeedIfEmptyAsync(new DarlingConfig(), ct);
    }
}
