/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PUBLIC-CONNECT guard against a real cluster (#5618): its catalog read sees a database's PUBLIC CONNECT granted,
/// then revoked, and the comparison names the database (the revoke is on this test's own scratch database, never on
/// the shared one); and the collection that needs the guard still carries it.
/// </summary>
[Collection("live-postgres")]
public sealed class PublicConnectCatalogReadLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void ThePgClusterRolesCollection_CarriesTheGuard()
    {
        /* The guard only guards what it is attached to. Taking it off the collection would leave the next test that
           revokes the privilege to be found the way #5618 was, by the victim's name in a nightly run. */
        Assert.Single(typeof(PgClusterRolesCollection).GetCustomAttributes<PublicConnectGuardAttribute>());
    }

    [Fact]
    public async Task TheCatalogRead_SeesAGrantAndARevokeOfPublicConnect()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5618 guard read.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the revoke below is on this test's own scratch database, never on the shared one. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);

        var before = PublicConnectGuardAttribute.PublicConnectOn(cs!, scratch.DatabaseName, "database_that_does_not_exist_5618");
        Assert.Equal([scratch.DatabaseName], before.Keys.ToArray());
        Assert.True(before[scratch.DatabaseName], "a new database grants CONNECT to PUBLIC");

        await using (var admin = new NpgsqlConnection(cs))
        {
            await admin.OpenAsync(ct);
            await using var revoke = new NpgsqlCommand($"REVOKE ALL ON DATABASE \"{scratch.DatabaseName}\" FROM PUBLIC", admin);
            await revoke.ExecuteNonQueryAsync(ct);
        }

        var after = PublicConnectGuardAttribute.PublicConnectOn(cs!, scratch.DatabaseName);
        Assert.False(after[scratch.DatabaseName]);
        Assert.Equal([scratch.DatabaseName], PublicConnectDelta.DatabasesThatLostPublicConnect(before, after));

        /* The default read names the connection's own database, as CI has it. */
        var shared = PublicConnectGuardAttribute.PublicConnectOn(cs!);
        Assert.Contains(new NpgsqlConnectionStringBuilder(cs).Database!, shared.Keys);
    }
}
