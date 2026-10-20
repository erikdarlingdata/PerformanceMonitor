/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6179: the name an MCP answer shows for a server. An Azure SQL Database entry with a database set has the storage
/// name <c>host:database</c>; the answer shows its display name, and falls back to the storage name when another
/// registration carries the same display name, so the <c>server</c> value passed back as <c>server_name</c> reaches one server.
/// </summary>
public class ServerAnswerNameTests
{
    private const string Host = "example-host";

    private static ServerConnection Db(string database, string displayName) =>
        new() { ServerName = Host, DatabaseName = database, DisplayName = displayName };

    [Fact]
    public void AnAzureDatabaseEntryAnswersWithItsDisplayName_NotItsHostAndDatabaseKey()
    {
        var servers = new List<ServerConnection> { Db("salesdb", "Sales"), Db("hrdb", "HR") };

        var (resolved, error) = ServerResolver.ResolveIn(servers, Host + ":salesdb");

        Assert.Null(error);
        Assert.Equal(Host + ":salesdb", resolved.ServerName);
        Assert.Equal("Sales", resolved.DisplayName);
    }

    [Fact]
    public void TwoEntriesSharingADisplayName_AnswerWithTheirKeys_AndEachKeyResolvesToOneServer()
    {
        var servers = new List<ServerConnection> { Db("salesdb", Host), Db("hrdb", Host) };

        var first = ServerResolver.ResolveIn(servers, Host + ":salesdb");
        var second = ServerResolver.ResolveIn(servers, Host + ":hrdb");

        Assert.Equal(Host + ":salesdb", first.resolved.DisplayName);
        Assert.Equal(Host + ":hrdb", second.resolved.DisplayName);

        // The answer's `server` value, passed back as server_name, picks the same single server.
        var backFirst = ServerResolver.ResolveIn(servers, first.resolved.DisplayName);
        var backSecond = ServerResolver.ResolveIn(servers, second.resolved.DisplayName);
        Assert.Null(backFirst.error);
        Assert.Null(backSecond.error);
        Assert.Equal(first.resolved.ServerId, backFirst.resolved.ServerId);
        Assert.Equal(second.resolved.ServerId, backSecond.resolved.ServerId);
        Assert.NotEqual(backFirst.resolved.ServerId, backSecond.resolved.ServerId);
    }
}
