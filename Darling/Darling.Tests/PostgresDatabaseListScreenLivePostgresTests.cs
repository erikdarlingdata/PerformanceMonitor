/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2995 against a REAL catalog. The source pin in <c>TargetProviderTests</c> proves the screen is in the
/// string; it cannot prove the row is gone, and "the string contains rdsadmin" is also true of a query
/// that mentions it in a comment.
///
/// <para>So this creates a database actually named <c>rdsadmin</c>, runs the SHIPPED query — the one
/// <c>BuildDatabaseListPlan</c> hands the runner, not a retyped copy — against <c>pg_database</c>, and
/// asserts both halves of the fix in one read: <c>rdsadmin</c> is absent, and every other database on the
/// cluster is still there. The second assertion is the one that matters. The failure mode this fix could
/// introduce is over-exclusion, and the note it protects (#2623) is only worth protecting if a real
/// database still reaches the fan-out to fail in.</para>
///
/// <para><b>Why a real database rather than a fixture.</b> The row has to exist in <c>pg_database</c> to be
/// screened out of <c>pg_database</c>, and nothing but <c>CREATE DATABASE</c> puts it there. A
/// <c>VALUES</c> substitution would mean rewriting the query, at which point the thing under test is the
/// rewrite.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PostgresDatabaseListScreenLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// A stand-in for a customer database — the one that must survive the screen, because a fix that
    /// silences rdsadmin by narrowing the fan-out to nothing would pass an rdsadmin-absent assertion.
    /// </summary>
    private const string CustomerDatabase = "darling_screen_appdb";

    [Fact]
    public async Task TheShippedEnumerationScreensRdsadminAndKeepsEveryOtherDatabase()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(
            string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-list screen test.");

        var ct = TestContext.Current.CancellationToken;

        /* Create-if-absent, drop-only-if-created. Not defensive padding: if this suite is ever pointed at a
           real managed cluster, rdsadmin is already there and is the platform's, so an unconditional
           CREATE would fail the test on a duplicate and an unconditional DROP would be the single most
           destructive thing in the repository. Owning only what we made keeps both outcomes impossible. */
        var weCreatedRdsadmin = false;

        try
        {
            weCreatedRdsadmin = await CreateIfAbsentAsync(
                cs!, PostgresTargetProvider.ManagedMaintenanceDatabase, ct);
            await CreateIfAbsentAsync(cs!, CustomerDatabase, ct);

            var (enumerationConnectionString, query) =
                PostgresTargetProvider.Instance.BuildDatabaseListPlan(cs!, excludedDatabases: null);

            var enumerated = new List<string>();
            await using (var connection = new NpgsqlConnection(enumerationConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var command = new NpgsqlCommand(query.Text, connection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    enumerated.Add(reader.GetString(0));
                }
            }

            /* The screen fired. */
            Assert.DoesNotContain(PostgresTargetProvider.ManagedMaintenanceDatabase, enumerated);

            /* And took nothing else with it. `postgres` is a genuine collection target on a managed
               instance — pg_extension_availability returns rows in it — and is what a "screen the
               system-looking databases" fix would quietly eat. */
            Assert.Contains(CustomerDatabase, enumerated);
            Assert.Contains("postgres", enumerated);

            /* The two sibling screens still fire, so this change is additive rather than a rewrite of the
               WHERE clause: template1 is connectable and excluded only by datistemplate, template0 only by
               datallowconn. Both are permanent cluster fixtures, unlike anything a test creates.
               Deliberately NOT an assertion on the total count — the own-store classes create and drop
               scratch databases in parallel with this collection, so a count is a race, and a moving
               cross-class flake is the exact failure LivePostgresCollectionHygieneTests exists to stop. */
            Assert.DoesNotContain("template0", enumerated);
            Assert.DoesNotContain("template1", enumerated);
        }
        finally
        {
            await DropAsync(cs!, CustomerDatabase);

            if (weCreatedRdsadmin)
            {
                await DropAsync(cs!, PostgresTargetProvider.ManagedMaintenanceDatabase);
            }
        }
    }

    /// <summary>
    /// Returns true when this call created the database, false when it was already present. PostgreSQL has
    /// no <c>CREATE DATABASE IF NOT EXISTS</c>, so the check and the create are separate statements.
    /// </summary>
    private static async Task<bool> CreateIfAbsentAsync(
        string adminConnectionString, string databaseName, CancellationToken cancellationToken)
    {
        await using var admin = new NpgsqlConnection(adminConnectionString);
        await admin.OpenAsync(cancellationToken);

        await using (var exists = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = $1", admin))
        {
            exists.Parameters.AddWithValue(databaseName);
            if (await exists.ExecuteScalarAsync(cancellationToken) is not null)
            {
                return false;
            }
        }

        /* Both names are compile-time constants in this file, so quoting the identifier is sufficient —
           no caller can reach this with an arbitrary string. */
        await using var create = new NpgsqlCommand($"CREATE DATABASE \"{databaseName}\"", admin);
        await create.ExecuteNonQueryAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Best-effort teardown with <c>WITH (FORCE)</c>, matching <c>ScratchPostgres</c>: a throw from this
    /// finally would replace the body's in-flight exception and hide the real failure (#1794).
    /// </summary>
    private static async Task DropAsync(string adminConnectionString, string databaseName)
    {
        try
        {
            await using var admin = new NpgsqlConnection(adminConnectionString);
            await admin.OpenAsync();
            await using var drop = new NpgsqlCommand(
                $"DROP DATABASE IF EXISTS \"{databaseName}\" WITH (FORCE)", admin);
            await drop.ExecuteNonQueryAsync();
        }
        catch
        {
            /* A leaked database on a throwaway CI cluster is harmless, and failing a passing test in its
               cleanup would invert the signal. */
        }
    }
}
