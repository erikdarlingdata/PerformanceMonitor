/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7077: a server-name miss says which name the caller sent, or says a name is needed, and still opens with the words
/// callers match on ("Could not resolve server."). Same sentences as Darling's resolver.
/// </summary>
public class ServerMissMessageTests
{
    private static ServerConnection Server(string name) => new() { ServerName = name, DisplayName = name };

    [Fact]
    public void NameThatMatchesNothing_IsEchoedInTheRefusal()
    {
        var (_, error) = ServerResolver.ResolveIn(new[] { Server("box-a"), Server("box-b") }, "  box-c ");

        var sentence = McpHelpers.ErrorMessageOf(error!);
        Assert.StartsWith("Could not resolve server.", sentence, StringComparison.Ordinal);
        Assert.Contains("'box-c'", sentence, StringComparison.Ordinal);
        Assert.Contains("Available servers:\nbox-a", sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void NoNameWithSeveralServers_SaysANameIsRequired()
    {
        var (_, error) = ServerResolver.ResolveIn(new[] { Server("box-a"), Server("box-b") }, null);

        var sentence = McpHelpers.ErrorMessageOf(error!);
        Assert.StartsWith("Could not resolve server.", sentence, StringComparison.Ordinal);
        Assert.Contains("server_name is required because 2 servers are monitored", sentence, StringComparison.Ordinal);
        Assert.Contains("box-b", sentence, StringComparison.Ordinal);
    }
}
