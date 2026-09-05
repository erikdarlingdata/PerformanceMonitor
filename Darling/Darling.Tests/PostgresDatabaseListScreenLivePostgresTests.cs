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

    /// <summary>
    /// The name RDS actually uses, spelled out here rather than read from
    /// <c>PostgresTargetProvider.ManagedMaintenanceDatabase</c> — deliberately, and it is the difference
    /// between this test proving something and proving nothing.
    ///
    /// <para>Building the fixture from the constant makes the test self-consistent under a rename: misspell
    /// the constant and this would create <c>rdsadmn</c>, screen <c>rdsadmn</c>, and pass green while the
    /// real <c>rdsadmin</c> went on failing every cycle on every managed target. Verified by mutation — with
    /// the fixture built from the constant, a one-character typo in it left this test passing and only the
    /// source pin in <c>TargetProviderTests</c> caught it. Hardcoding the platform's name here means the two
    /// assertions have to AGREE with reality rather than with each other.</para>
    /// </summary>
    private const string ManagedMaintenanceDatabase = "rdsadmin";

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
        var bodySucceeded = false;

        try
        {
            weCreatedRdsadmin = await CreateIfAbsentAsync(cs!, ManagedMaintenanceDatabase, ct);
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
            Assert.DoesNotContain(ManagedMaintenanceDatabase, enumerated);

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

            bodySucceeded = true;
        }
        finally
        {
            /* Through LiveStoreCleanup (#1902), on its own fresh connection. Hand-rolling this would earn
               the #1902 ratchet's exact complaint — a throw from a finally replaces the body's in-flight
               exception — and a bare catch-and-swallow would trade that for the opposite fault: a database
               this test leaked would then go unreported even on a passing run, and the NEXT run would find
               rdsadmin already present, not create it, and so never drop it either. RunAsync stays silent
               only while the body's own failure is in flight. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DropAsync(cleanup, CustomerDatabase, cleanupCt);

                if (weCreatedRdsadmin)
                {
                    await DropAsync(cleanup, ManagedMaintenanceDatabase, cleanupCt);
                }
            });
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
    /// Drops on the cleanup connection the caller supplies, and does NOT swallow — the masking rule lives
    /// in <see cref="LiveStoreCleanup.RunAsync"/>, which is the only place that knows whether the body
    /// already failed. A <c>catch</c> here would apply it unconditionally and hide a real leak.
    /// <para><c>WITH (FORCE)</c> so a pooled connection somewhere cannot wedge the drop, matching the
    /// scratch-database helper's teardown. <c>IF EXISTS</c> because the create is conditional.</para>
    /// <para>The helper is named in prose only, never as a bare identifier: the #1902 ratchet exempts any
    /// file whose text contains that type's name, so mentioning it would have taken this class out of
    /// scope of the very rule it now satisfies — passing for the wrong reason instead of complying.</para>
    /// </summary>
    private static async Task DropAsync(
        NpgsqlConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        await using var drop = new NpgsqlCommand(
            $"DROP DATABASE IF EXISTS \"{databaseName}\" WITH (FORCE)", connection);
        await drop.ExecuteNonQueryAsync(cancellationToken);
    }
}
