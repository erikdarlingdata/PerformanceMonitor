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
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgReadBinaryFileCapability"/> against a real server (#4046 part 1c). It's split from
/// <see cref="PgReadBinaryFileCapabilityTests"/> because it creates a role and grants it EXECUTE on a
/// pg_catalog function in the SHARED DARLING_TEST_PG database, so it serializes with the other live
/// classes. The fake-connection tests stay in the statics collection.
/// </summary>
[Collection("live-postgres")]
public sealed class PgReadBinaryFileCapabilityLiveTests
{
    /// <summary>
    /// The real-server case (#4046 part 1c): a dedicated role with no grant at all reads false, and after
    /// granting <c>pg_read_binary_file</c> and dropping its cached verdict (a real cycle would simply wait out
    /// the TTL) the same probe reads true. Same DARLING_TEST_PG gate and dedicated-role-create-and-drop shape as
    /// the suite's other <c>*_AgainstDevPostgres</c> live tests.
    /// </summary>
    [Fact]
    public async Task IsGrantedAsync_AgainstDevPostgres()
    {
        var connectionStringRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrWhiteSpace(connectionStringRoot))
        {
            return;
        }

        const string role = "pm_test_pgreadbinaryfile_role";

        await using var adminConnection = new NpgsqlConnection(connectionStringRoot);
        await adminConnection.OpenAsync();

        await DropProbeRoleAsync(adminConnection, role);

        await using (var create = adminConnection.CreateCommand())
        {
            create.CommandText = $"CREATE ROLE {role} LOGIN PASSWORD 'pm_test_pgreadbinaryfile' NOSUPERUSER";
            await create.ExecuteNonQueryAsync();
        }

        var bodySucceeded = false;
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionStringRoot)
            {
                Username = role,
                Password = "pm_test_pgreadbinaryfile",
                Pooling = false,
            };

            await using (var roleConnection = new NpgsqlConnection(builder.ConnectionString))
            {
                await roleConnection.OpenAsync();

                var ungranted = await PgReadBinaryFileCapability.IsGrantedAsync(
                    roleConnection, "dev-postgres-role-probe", CancellationToken.None);
                Assert.False(ungranted);
            }

            await using (var grant = adminConnection.CreateCommand())
            {
                grant.CommandText =
                    $"GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint) TO {role}";
                await grant.ExecuteNonQueryAsync();
            }

            /* Only this test's own key. Reset() clears every key, and the statics collection's tests run in
               parallel with this live collection. */
            PgReadBinaryFileCapability.Invalidate("dev-postgres-role-probe");

            await using (var roleConnection = new NpgsqlConnection(builder.ConnectionString))
            {
                await roleConnection.OpenAsync();

                var granted = await PgReadBinaryFileCapability.IsGrantedAsync(
                    roleConnection, "dev-postgres-role-probe", CancellationToken.None);
                Assert.True(granted);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionStringRoot, bodySucceeded, async (cleanup, _) =>
            {
                await DropProbeRoleAsync(cleanup, role);
            });
        }
    }

    /// <summary>
    /// #4051 round-2 review, M-1: the probe gives a grant answer on a SQL_ASCII database, not NULL. PostgreSQL
    /// checks SQL_ASCII text as UTF-8 on its way to a UTF8 client, so the text route there meets the same
    /// planted-byte failure, and the byte route is the fix. The shared test database is UTF8, so this test creates
    /// a SQL_ASCII database of its own and drops it again.
    /// </summary>
    [Fact]
    public async Task TheProbeGivesAGrantAnswerOnASqlAsciiDatabase_AgainstDevPostgres()
    {
        var connectionStringRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrWhiteSpace(connectionStringRoot))
        {
            return;
        }

        const string database = "pm_test_pgreadbinaryfile_sqlascii";
        const string targetKey = "dev-postgres-sqlascii-probe";

        await using var adminConnection = new NpgsqlConnection(connectionStringRoot);
        await adminConnection.OpenAsync();

        await DropScratchDatabaseAsync(adminConnection, database);

        await using (var create = adminConnection.CreateCommand())
        {
            create.CommandTimeout = 60;
            create.CommandText = $"CREATE DATABASE {database} ENCODING 'SQL_ASCII' LOCALE_PROVIDER libc "
                + "LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0";
            await create.ExecuteNonQueryAsync();
        }

        var bodySucceeded = false;
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionStringRoot) { Database = database, Pooling = false };

            await using (var connection = new NpgsqlConnection(builder.ConnectionString))
            {
                await connection.OpenAsync();

                bool expected;
                await using (var privilege = connection.CreateCommand())
                {
                    privilege.CommandTimeout = 10;
                    privilege.CommandText = "SELECT pg_catalog.has_function_privilege(current_user, "
                        + "'pg_catalog.pg_read_binary_file(text, bigint, bigint)', 'EXECUTE')";
                    expected = (bool)(await privilege.ExecuteScalarAsync())!;
                }

                PgReadBinaryFileCapability.Invalidate(targetKey);
                var granted = await PgReadBinaryFileCapability.IsGrantedAsync(connection, targetKey, CancellationToken.None);

                Assert.False(PgReadBinaryFileCapability.IsCachedAsUnsupportedEncoding(targetKey));
                Assert.Equal(expected, granted);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionStringRoot, bodySucceeded, async (cleanup, _) =>
            {
                await DropScratchDatabaseAsync(cleanup, database);
            });
        }
    }

    /* WITH (FORCE), because the probe's own connection may not have finished closing on the server. */
    private static async Task DropScratchDatabaseAsync(NpgsqlConnection adminConnection, string database)
    {
        await using var drop = adminConnection.CreateCommand();
        drop.CommandTimeout = 60;
        drop.CommandText = $"DROP DATABASE IF EXISTS {database} WITH (FORCE)";
        await drop.ExecuteNonQueryAsync();
    }

    /* A role that holds a privilege can't be dropped ("some objects depend on it"), and this one holds
       EXECUTE on pg_read_binary_file once the test grants it. DROP OWNED BY revokes it first. It's guarded,
       because DROP OWNED BY errors on a role that doesn't exist. */
    private static async Task DropProbeRoleAsync(NpgsqlConnection adminConnection, string role)
    {
        await using var drop = adminConnection.CreateCommand();
        drop.CommandText =
            $"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{role}') THEN "
            + $"EXECUTE 'DROP OWNED BY {role}'; EXECUTE 'DROP ROLE {role}'; END IF; END $$";
        await drop.ExecuteNonQueryAsync();
    }
}
