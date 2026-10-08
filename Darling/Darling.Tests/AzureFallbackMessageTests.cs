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

    [Theory]
    [InlineData("master")]
    [InlineData("")]
    [InlineData(null)]
    public void NoDatabaseToFallBackTo_NamesTheFirewallFirst(string? targetDb)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Runner().FallbackDatabaseList(Server(), targetDb, reason: "master DB inaccessible (SQL error 40615)"));

        var message = ex.Message;
        Assert.StartsWith("master DB inaccessible (SQL error 40615), and this connection has no target database", message, StringComparison.Ordinal);

        var firewall = message.IndexOf("firewall did not allow this client's IP address", StringComparison.Ordinal);
        var rule = message.IndexOf("add a firewall rule", StringComparison.Ordinal);
        var setDatabase = message.IndexOf("set a database for 'AzureTest1007'", StringComparison.Ordinal);

        Assert.True(firewall > 0, message);
        Assert.True(rule > firewall, message);
        Assert.Contains("at the server level or on the database", message, StringComparison.Ordinal);
        Assert.True(setDatabase > rule, "Setting a database comes after the firewall, as the master-only case's fix.");
        Assert.Contains("can open a user database but not master", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANamedDatabaseIsStillTheFallback()
    {
        var databases = Runner().FallbackDatabaseList(Server(), "AdventureWorks", reason: "master DB inaccessible (SQL error 40615)");

        Assert.Equal(new List<string> { "AdventureWorks" }, databases);
    }
}
