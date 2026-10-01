/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Several monitored databases on one Azure SQL Database server are separate servers. The registry files each one
/// under its storage name (<c>host:database</c>), so the bare host name is a partial match for every one of them.
/// A read tool given that name used to answer for whichever database sorted first. It now refuses the name and lists
/// the databases, and a database's own storage name selects exactly that one.
/// </summary>
public sealed class DarlingServerResolverSameHostTests
{
    private const string Host = "shared-host";

    private static DarlingServerResolver.RegisteredServer Db(string database, string? displayName = null)
    {
        var storageName = Host + ":" + database;
        return new DarlingServerResolver.RegisteredServer(
            ServerIdHelper.GetDeterministicHashCode(storageName), storageName, displayName ?? storageName);
    }

    private static ((int ServerId, string ServerName) resolved, string? error) Resolve(
        IReadOnlyList<DarlingServerResolver.RegisteredServer> servers, string? name) =>
        DarlingServerResolver.ResolveOrError(servers, name, DarlingPeerDirectory.Snapshot.Empty);

    [Fact]
    public void AReadToolGivenTheHostName_IsRefusedWithEveryDatabaseListed_NotAnsweredForTheFirst()
    {
        var servers = new[] { Db("a"), Db("b"), Db("c") };

        var (resolved, error) = Resolve(servers, Host);

        Assert.NotNull(error);
        Assert.Equal(default, resolved);
        Assert.True(McpHelpers.IsRefusalEnvelope(error), error);
        var message = McpHelpers.ErrorMessageOf(error!);
        Assert.Contains("matches 3 monitored servers", message, StringComparison.Ordinal);
        Assert.Contains(Host + ":a", message, StringComparison.Ordinal);
        Assert.Contains(Host + ":b", message, StringComparison.Ordinal);
        Assert.Contains(Host + ":c", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AReadToolGivenOneDatabasesStorageName_AnswersForThatDatabase()
    {
        var a = Db("a");
        var b = Db("b");
        var c = Db("c");
        var servers = new[] { a, b, c };

        foreach (var wanted in new[] { b, c, a, b })
        {
            var (resolved, error) = Resolve(servers, wanted.ServerName);

            Assert.Null(error);
            Assert.Equal(wanted.ServerId, resolved.ServerId);
            Assert.Equal(wanted.ServerName, resolved.ServerName);
        }
    }

    [Fact]
    public void TheRefusal_ListsEachDatabasesStorageName_AndItsDisplayNameWhenThatDiffers()
    {
        var servers = new[] { Db("a", "orders"), Db("b") };

        var (_, error) = Resolve(servers, Host);

        var message = McpHelpers.ErrorMessageOf(error!);
        Assert.Contains(Host + ":a (orders)", message, StringComparison.Ordinal);
        Assert.Contains(Host + ":b", message, StringComparison.Ordinal);
        Assert.DoesNotContain(Host + ":b (", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AReadToolGivenAName_ThatOneServerAnswersTo_StillAnswersForIt()
    {
        var orders = Db("a", "orders");
        var billing = Db("b", "billing");
        var elsewhere = new DarlingServerResolver.RegisteredServer(
            ServerIdHelper.GetDeterministicHashCode("another-host"), "another-host", "another-host");
        var servers = new[] { orders, billing, elsewhere };

        Assert.Equal(billing.ServerId, Resolve(servers, "BILLING").resolved.ServerId);
        Assert.Equal(elsewhere.ServerId, Resolve(servers, "another").resolved.ServerId);
        Assert.Equal(billing.ServerId, Resolve(servers, "  " + billing.ServerName + "  ").resolved.ServerId);

        /* No name at all: the only server there is, or nothing to choose from. */
        Assert.Equal(elsewhere.ServerId, Resolve(new[] { elsewhere }, null).resolved.ServerId);
        Assert.NotNull(Resolve(servers, null).error);
        Assert.NotNull(Resolve(servers, "no-such-server").error);
    }

    [Fact]
    public void AReadToolGivenTheHostName_AnswersForThePlainRegistration_WhenOneExistsBesideItsDatabases()
    {
        /* The plain registration's storage name IS the host, and its per-database siblings take the host as their
           display name, so the host matches all three. Its own storage name is the one exact key that is unique. */
        var plain = new DarlingServerResolver.RegisteredServer(ServerIdHelper.GetDeterministicHashCode(Host), Host, Host);
        var servers = new[] { Db("a", Host), plain, Db("b", Host) };

        var (resolved, error) = Resolve(servers, Host);

        Assert.Null(error);
        Assert.Equal(plain.ServerId, resolved.ServerId);
    }

    [Fact]
    public void AFragmentOfSeveralDatabaseNames_IsRefusedAsPartOfTheirNames()
    {
        var servers = new[] { Db("sales-1"), Db("sales-2"), Db("hr") };

        var (resolved, error) = Resolve(servers, "sales-");

        Assert.Equal(default, resolved);
        var message = McpHelpers.ErrorMessageOf(error!);
        Assert.Contains("matches 2 monitored servers (as part of their names)", message, StringComparison.Ordinal);
        Assert.DoesNotContain(Host + ":hr", message, StringComparison.Ordinal);
    }
}
