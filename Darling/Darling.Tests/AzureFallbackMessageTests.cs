/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5498: when master cannot be read and the registration names no database, the thrown text puts the firewall first.
/// Error 40615 means the Azure firewall did not allow the client's IP address (a server-level block causes it as
/// readily as a database-only rule), so the text names that and the firewall rule as the fix. Setting a database is
/// only the fix for the master-only case. The 3.10 Azure release test hit this with a server-level rule removed and
/// was told to set a database. The fallback behavior itself (throw when there is nothing to fall back to, use the
/// named database otherwise) is unchanged and pinned here beside the wording.
/// </summary>
public sealed class AzureFallbackMessageTests
{
    private static DarlingCollectorRunner Runner()
        => new(NpgsqlDataSource.Create("Host=localhost;Database=unused"), new CollectorDeltaCalculator(), NullLogger.Instance);

    private static ServerRuntime Server() => new()
    {
        Config = new MonitoredServer { Name = "AzureTest1007", Host = "example.database.windows.net" },
        ConnectionString = "Server=tcp:example.database.windows.net,1433;Initial Catalog=master;Encrypt=True",
        Target = new CollectorTargetInfo { IsAzureSqlDb = true },
        StorageName = "example.database.windows.net",
        ServerId = 5498,
    };

    private const string FirewallMessage =
        "Cannot open server 'example' requested by the login. Client with IP address '192.0.2.1' is not allowed to " +
        "access the server. To enable access, use the Azure Management Portal or run sp_set_firewall_rule on the " +
        "master database to create a firewall rule for this IP address or address range.";

    [Theory]
    [InlineData("master")]
    [InlineData("")]
    [InlineData(null)]
    public void Error40615_NamesTheFirewallAndTheClientIp_WithNoSetDatabaseAdvice(string? targetDb)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Runner().FallbackDatabaseList(
                Server(), targetDb, reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: 40615, serverMessage: FirewallMessage));

        var message = ex.Message;
        Assert.StartsWith("master DB inaccessible (SQL error 40615), and this connection has no target database", message, StringComparison.Ordinal);
        Assert.Contains("The Azure firewall did not allow this client's IP address (192.0.2.1)", message, StringComparison.Ordinal);
        Assert.Contains("dd a firewall rule", message, StringComparison.Ordinal);
        Assert.Contains("either a server-level rule or a database-level rule in master", message, StringComparison.Ordinal);
        Assert.DoesNotContain("set a database", message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("can open a user database", message, StringComparison.Ordinal);
        Assert.DoesNotContain("AzureTest1007", message, StringComparison.Ordinal);
    }

    [Fact]
    public void Error40615_WithoutAnIpInTheServerText_StillHasNoSetDatabaseAdvice()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Runner().FallbackDatabaseList(
                Server(), "master", reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: 40615, serverMessage: "Ein anderer Text ohne Adresse."));

        Assert.Contains("did not allow this client's IP address. Add a firewall rule", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("set a database", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(4060)]
    [InlineData(18456)]
    public void AKnownNonFirewallErrorNumber_HasSetDatabaseAdviceAndNoFirewallText(int errorNumber)
    {
        /* The decision is the error number, not the words: a reason that merely says 40615 gets neither the
           firewall-only message nor firewall advice when the number the caller read is something else. */
        var ex = Assert.Throws<InvalidOperationException>(
            () => Runner().FallbackDatabaseList(
                Server(), "master", reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: errorNumber, serverMessage: FirewallMessage));

        var message = ex.Message;
        Assert.DoesNotContain("firewall", message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("40615, the", message, StringComparison.Ordinal);
        Assert.DoesNotContain("192.0.2.1", message, StringComparison.Ordinal);
        Assert.EndsWith(
            "If this login can open a user database but not master, set a database for 'AzureTest1007' " +
            "so database-scoped collectors have something to read.",
            message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoErrorNumber_KeepsTheFirewallThenSetDatabaseMessage()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Runner().FallbackDatabaseList(
                Server(), "master", reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: null, serverMessage: FirewallMessage));

        Assert.Contains(
            "If the error is 40615, the Azure firewall did not allow this client's IP address: " +
            "add a firewall rule for it, either a server-level rule or a database-level rule in master. " +
            "If this login can open a user database but not master, set a database for 'AzureTest1007' " +
            "so database-scoped collectors have something to read.",
            ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("192.0.2.1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANamedDatabaseIsStillTheFallback()
    {
        var databases = Runner().FallbackDatabaseList(Server(), "AdventureWorks", reason: "master DB inaccessible (SQL error 40615)");

        Assert.Equal(new List<string> { "AdventureWorks" }, databases);
    }
}
