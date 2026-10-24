/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #6179: the name an MCP answer shows for a server. An Azure SQL Database entry with a database set is keyed
/// <c>host:database</c>; the answer shows its display name, and falls back to the key when another registration
/// carries the same display name so the <c>server</c> field passed back as <c>server_name</c> reaches one server.
/// </summary>
public class DarlingServerAnswerNameTests
{
    [Fact]
    public void AnAzureDatabaseEntryAnswersWithItsDisplayName_NotItsHostAndDatabaseKey()
    {
        var servers = new[]
        {
            new RegisteredServer(1, "example-host:salesdb", "Sales"),
            new RegisteredServer(2, "example-host:hrdb", "HR"),
        };

        var (resolved, error) = DarlingServerResolver.ResolveOrErrorForAnswer(servers, "example-host:salesdb");

        Assert.Null(error);
        Assert.Equal("example-host:salesdb", resolved.ServerName);
        Assert.Equal("Sales", resolved.DisplayName);
    }

    [Fact]
    public void TwoEntriesSharingADisplayName_AnswerWithTheirKeys_AndEachKeyResolvesToOneServer()
    {
        var servers = new[]
        {
            new RegisteredServer(1, "example-host:salesdb", "example-host"),
            new RegisteredServer(2, "example-host:hrdb", "example-host"),
            new RegisteredServer(3, "other-host", "Other"),
        };

        var first = DarlingServerResolver.ResolveOrErrorForAnswer(servers, "example-host:salesdb");
        var second = DarlingServerResolver.ResolveOrErrorForAnswer(servers, "example-host:hrdb");

        Assert.Equal("example-host:salesdb", first.resolved.DisplayName);
        Assert.Equal("example-host:hrdb", second.resolved.DisplayName);

        // The answer's `server` value, passed back as server_name, picks the same single server.
        var backFirst = DarlingServerResolver.ResolveOrError(servers, first.resolved.DisplayName);
        var backSecond = DarlingServerResolver.ResolveOrError(servers, second.resolved.DisplayName);
        Assert.Null(backFirst.error);
        Assert.Null(backSecond.error);
        Assert.Equal(1, backFirst.resolved.ServerId);
        Assert.Equal(2, backSecond.resolved.ServerId);

        // A server whose display name is unique still answers with it.
        Assert.Equal("Other", DarlingServerResolver.ResolveOrErrorForAnswer(servers, "other-host").resolved.DisplayName);
    }

    [Fact]
    public void AnEntryWithNoDisplayName_AnswersWithItsKey()
    {
        var servers = new[] { new RegisteredServer(1, "example-host:salesdb", null) };

        Assert.Equal("example-host:salesdb", DarlingServerResolver.ResolveOrErrorForAnswer(servers, null).resolved.DisplayName);
    }

    [Fact]
    public void AFingerprintResolveCarriesTheSameAnswerName()
    {
        var servers = new[]
        {
            new RegisteredServer(1, "example-host:salesdb", "Sales"),
            new RegisteredServer(2, "example-host:hrdb", "Sales"),
        };

        var (resolved, error) = DarlingServerResolver.ResolveWithFingerprintName(servers, "example-host:hrdb");

        Assert.Null(error);
        Assert.Equal("example-host:hrdb", resolved.DisplayName);
    }
}
