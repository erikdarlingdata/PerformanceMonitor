/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch databases through ScratchPostgres, and its own login role. */

/// <summary>
/// #9691 and #9698: the store built by a login that owns its database and is not a superuser, which is what a bring-your-own store's
/// install asks for. Every continuous aggregate and the frozen rollup are built with the indexes a superuser store gets, and a login that
/// lacks what the service uses is told so once, with grants that work.
/// </summary>
public sealed class NonSuperuserStoreOwnerLiveTests
{
    private const string OwnerPassword = "NonSuperOwner9691Pw";

    /// <summary>
    /// The product's start path on a store owned by a plain login: the migrations, TimescaleDB, then every step of the convergence list.
    /// The aggregate step's per-view failures are warnings, never throws, so the count it returns and the warnings are the outcome.
    /// </summary>
    [Fact]
    public async Task Store_BuiltByNonSuperuserOwner_HasEveryAggregateAndTheIndexesOfASuperuserStore()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live non-superuser store owner test (it mints its own scratch databases and login).");

        var ct = TestContext.Current.CancellationToken;
        var owner = "byo_owner_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            await using (var reference = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true))
            await using (var owned = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true))
            {
                Assert.SkipUnless(reference.TimescaleEnabled && owned.TimescaleEnabled, "TimescaleDB is not available on this cluster, so there are no continuous aggregates to build.");

                var referenceLogger = new CapturingTestLogger();
                var referenceReady = await BuildStoreAsync(reference.ConnectionString, referenceLogger, ct);

                await CreateOwnerAsync(owned, owner, ct);
                var ownerLogger = new CapturingTestLogger();
                var ownerReady = await BuildStoreAsync(OwnerConnectionString(owned, owner), ownerLogger, ct);

                var declared = TimescaleSupport.DeclaredContinuousAggregateViews.Count;
                Assert.True(referenceReady == declared, $"the superuser store built {referenceReady} of {declared} aggregates: {referenceLogger.Joined}");
                Assert.True(ownerReady == declared, $"the plain owner's store built {ownerReady} of {declared} aggregates: {ownerLogger.Joined}");

                var warnings = ownerLogger.Lines.Where(line => line.StartsWith("Warning:", StringComparison.Ordinal)).ToList();
                Assert.True(warnings.Count == 0, "the plain owner's start logged warnings: " + string.Join(" | ", warnings));

                Assert.Equal(await MaterializationIndexesAsync(reference.ConnectionString, ct), await MaterializationIndexesAsync(owned.ConnectionString, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(baseConnectionString!, bodySucceeded, (cleanup, _) => ExecAsync(cleanup, $"DROP ROLE IF EXISTS {owner}", CancellationToken.None));
        }
    }

    /// <summary>
    /// A login that is not a superuser and holds none of the optional privileges gets one warning naming what it lacks, and the statements the
    /// warning names, run by a superuser in the store database, leave nothing to report. A superuser login logs nothing.
    /// </summary>
    [Fact]
    public async Task Readiness_NamesGrantsThatWork_AndIsQuietForASuperuser()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live store login readiness test (it mints its own scratch database and login).");

        var ct = TestContext.Current.CancellationToken;
        var owner = "byo_login_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            await using (var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true))
            {
                await CreateOwnerAsync(scratch, owner, ct);

                /* Nothing is built in this database, so with TimescaleDB every declared aggregate is missing and the login cannot create in the
                   internal schema: the aggregate item is part of the report. */
                await using var asOwner = new NpgsqlConnection(OwnerConnectionString(scratch, owner));
                await asOwner.OpenAsync(ct);
                var report = await StoreLoginReadiness.ReadLoginPrivilegesAsync(asOwner, scratch.TimescaleEnabled, ct);
                Assert.NotNull(report);
                Assert.Contains(owner, report!.Message, StringComparison.Ordinal);
                Assert.NotEmpty(report.Statements);

                var logger = new CapturingTestLogger();
                await StoreLoginReadiness.LogMissingPrivilegesAsync(asOwner, scratch.TimescaleEnabled, logger, ct);
                Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));

                await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
                await superuser.OpenAsync(ct);
                var superuserLogger = new CapturingTestLogger();
                await StoreLoginReadiness.LogMissingPrivilegesAsync(superuser, scratch.TimescaleEnabled, superuserLogger, ct);
                Assert.Equal(0, superuserLogger.CountAtLevel(LogLevel.Warning));

                foreach (var statement in report.Statements)
                {
                    await ExecAsync(superuser, statement, ct);
                }

                Assert.Null(await StoreLoginReadiness.ReadLoginPrivilegesAsync(asOwner, scratch.TimescaleEnabled, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(baseConnectionString!, bodySucceeded, (cleanup, _) => ExecAsync(cleanup, $"DROP ROLE IF EXISTS {owner}", CancellationToken.None));
        }
    }

    /// <summary>
    /// The capture lists the log with <c>pg_ls_logdir()</c> and builds each file's path from the <c>log_directory</c> setting, which a plain
    /// login can read only through <c>pg_read_all_settings</c>. A login granted EXECUTE on <c>pg_ls_logdir()</c> alone lists the folder and
    /// then fails at the setting, so the probe must not count it as able to list; <c>pg_monitor</c> covers both.
    /// </summary>
    [Fact]
    public async Task ALoginGrantedOnlyTheLogListing_IsNotCountedAsAbleToList_UntilItHoldsPgMonitor()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live store login readiness test (it mints its own scratch database and login).");

        var ct = TestContext.Current.CancellationToken;
        var owner = "byo_login_" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            await using (var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true))
            {
                await CreateOwnerAsync(scratch, owner, ct);
                await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
                await superuser.OpenAsync(ct);
                await ExecAsync(superuser, $"GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_logdir() TO {owner}", ct);

                await using var asOwner = new NpgsqlConnection(OwnerConnectionString(scratch, owner));
                await asOwner.OpenAsync(ct);
                Assert.False(await CanListLogAsync(asOwner, ct), "a login with EXECUTE on pg_ls_logdir() alone was counted as able to list the log");
                var denied = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(asOwner, "SELECT pg_catalog.current_setting('log_directory')", ct));
                Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, denied.SqlState);

                await ExecAsync(superuser, $"GRANT pg_monitor TO {owner}", ct);
                Assert.True(await CanListLogAsync(asOwner, ct), "a login holding pg_monitor was not counted as able to list the log");
                await ExecAsync(asOwner, "SELECT pg_catalog.current_setting('log_directory')", ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(baseConnectionString!, bodySucceeded, (cleanup, _) => ExecAsync(cleanup, $"DROP ROLE IF EXISTS {owner}", CancellationToken.None));
        }
    }

    /// <summary>The probe's log-listing column, read through the product's own statement.</summary>
    private static async Task<bool> CanListLogAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(StoreLoginReadiness.LoginPrivilegesSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return reader.GetBoolean(3);
    }

    /// <summary>The migrations, TimescaleDB and every convergence step on one connection string, the way the service's start path runs them. Returns the aggregate step's count.</summary>
    private static async Task<int> BuildStoreAsync(string connectionString, ILogger logger, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false, CommandTimeout = 600 };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, logger, null, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, logger, ct), "TimescaleDB could not be enabled on the scratch store.");

        var ready = -1;
        foreach (var step in DarlingWorker.StoreObjectConvergence)
        {
            var count = await step.EnsureAsync(connection, logger, ct);
            if (step.Name == "continuous aggregates")
            {
                ready = count;
            }
        }

        return ready;
    }

    /// <summary>Makes <paramref name="owner"/> a plain login that owns the scratch database, the way a bring-your-own store's login does.</summary>
    private static async Task CreateOwnerAsync(ScratchPostgres scratch, string owner, CancellationToken ct)
    {
        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);
        await ExecAsync(admin, $"CREATE ROLE {owner} LOGIN NOSUPERUSER PASSWORD '{OwnerPassword}'", ct);
        await ExecAsync(admin, $"ALTER DATABASE \"{scratch.DatabaseName}\" OWNER TO {owner}", ct);
    }

    private static string OwnerConnectionString(ScratchPostgres scratch, string owner) => new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
    {
        Username = owner,
        Password = OwnerPassword,
        Pooling = false,
    }.ConnectionString;

    /// <summary>Each aggregate's materialization indexes, as definitions with the generated hypertable number taken out so two stores compare.</summary>
    private static async Task<List<string>> MaterializationIndexesAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand(@"
SELECT ca.view_name || ' ' || regexp_replace(pg_get_indexdef(i.indexrelid), '_materialized_hypertable_[0-9]+', 'MAT', 'g')
FROM timescaledb_information.continuous_aggregates ca
JOIN pg_index i ON i.indrelid = format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name)::regclass
ORDER BY 1", connection);
        var definitions = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            definitions.Add(reader.GetString(0));
        }

        return definitions;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
