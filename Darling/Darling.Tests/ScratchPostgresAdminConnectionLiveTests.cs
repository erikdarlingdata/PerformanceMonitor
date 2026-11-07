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
using Npgsql;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints scratch databases through ScratchPostgres and reads pg_stat_activity only for the
   factory's own admin connections, so the class is deliberately NOT [Collection("live-postgres")]. */
/// <summary>
/// #8206: the factory's admin work (the <c>CREATE DATABASE</c> and the <c>DROP DATABASE</c>) runs on connections of its
/// own that it keeps and reuses, instead of a fresh socket per statement that a leg minting hundreds of scratch databases
/// pays for in ephemeral ports, and that no test's own connection string can receive (#5549).
/// </summary>
public sealed class ScratchPostgresAdminConnectionLiveTests
{
    private static async Task<HashSet<int>> AdminBackendsAsync(NpgsqlConnection observer, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pid FROM pg_catalog.pg_stat_activity WHERE application_name = $1 AND backend_type = 'client backend'", observer);
        command.Parameters.AddWithValue(ScratchPostgres.AdminApplicationName);
        var pids = new HashSet<int>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            pids.Add(reader.GetInt32(0));
        }

        return pids;
    }

    [Fact]
    public async Task TheFactorysAdminConnection_OutlivesTheCreateAndTheDrop_AndIsNotWhatATestsOwnConnectionStringOpens()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the scratch-factory admin connection test.");
        var ct = TestContext.Current.CancellationToken;

        await using var observer = new NpgsqlConnection(connectionString);
        await observer.OpenAsync(ct);

        var first = await ScratchPostgres.CreateAsync(connectionString!, ct, plainPostgres: true);
        var afterCreate = await AdminBackendsAsync(observer, ct);
        Assert.NotEmpty(afterCreate);

        await first.DisposeAsync();
        await using var second = await ScratchPostgres.CreateAsync(connectionString!, ct, plainPostgres: true);
        var afterDropAndCreate = await AdminBackendsAsync(observer, ct);
        Assert.Contains(afterCreate, pid => afterDropAndCreate.Contains(pid));

        /* A connection opened the way a test opens one, on DARLING_TEST_PG itself, is never one of the factory's. */
        await using var test = new NpgsqlConnection(connectionString);
        await test.OpenAsync(ct);
        await using var ownPid = new NpgsqlCommand("SELECT pg_catalog.pg_backend_pid()", test);
        Assert.DoesNotContain(Convert.ToInt32(await ownPid.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture), afterDropAndCreate);
    }
}
