/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7077: a server-name miss says which name the caller sent, or says a name is needed, and still opens with the words
/// the web route and the tools match on ("Could not resolve server.").
/// </summary>
public class ServerMissMessageTests
{
    private static RegisteredServer Registered(string name) => new(name.GetHashCode(StringComparison.Ordinal), name, name);

    [Fact]
    public void NameThatMatchesNothing_IsEchoedInTheRefusal()
    {
        var (_, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("box-a"), Registered("box-b") }, "  box-c ", DarlingPeerDirectory.Snapshot.Empty);

        var sentence = McpHelpers.ErrorMessageOf(error!);
        Assert.StartsWith("Could not resolve server.", sentence, StringComparison.Ordinal);
        Assert.Contains("'box-c'", sentence, StringComparison.Ordinal);
        Assert.Contains("Available servers:\nbox-a", sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void NoNameWithSeveralServers_SaysANameIsRequired()
    {
        var (_, error) = DarlingServerResolver.ResolveOrError(
            new[] { Registered("box-a"), Registered("box-b") }, null, DarlingPeerDirectory.Snapshot.Empty);

        var sentence = McpHelpers.ErrorMessageOf(error!);
        Assert.StartsWith("Could not resolve server.", sentence, StringComparison.Ordinal);
        Assert.Contains("server_name is required because 2 servers are monitored", sentence, StringComparison.Ordinal);
        Assert.Contains("box-b", sentence, StringComparison.Ordinal);
    }
}
