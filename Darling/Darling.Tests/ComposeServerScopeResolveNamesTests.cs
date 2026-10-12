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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 item 17: the pure half of <see cref="ComposeServerScope.ResolveNames"/>, the placement of a panel spec's server names through
/// the registry resolver every other server-scoped tool uses. Before it, a spec naming a server by its display name matched nothing
/// (the scope matched only the stored <c>server_name</c>) and the run answered 0 rows.
/// </summary>
public sealed class ComposeServerScopeResolveNamesTests
{
    private static readonly IReadOnlyList<DarlingServerResolver.RegisteredServer> Servers = new[]
    {
        new DarlingServerResolver.RegisteredServer(1, "sql2022-host", "Prod Orders"),
        new DarlingServerResolver.RegisteredServer(2, "sql2025-host", "Prod Billing"),
    };

    [Fact]
    public void StorageName_StaysAsWritten()
    {
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { "sql2022-host" }, Servers);
        Assert.Null(refusal);
        Assert.Equal(new[] { "sql2022-host" }, scope);
    }

    [Fact]
    public void DisplayName_AndOtherCase_ResolveToTheStoredServerName()
    {
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { "Prod Orders", "SQL2025-HOST" }, Servers);
        Assert.Null(refusal);
        Assert.Equal(new[] { "sql2022-host", "sql2025-host" }, scope);
    }

    [Fact]
    public void UnknownName_StaysAsWritten_ForTheRunsUnknownServerAnswer()
    {
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { "NO-SUCH-SERVER" }, Servers);
        Assert.Null(refusal);
        Assert.Equal(new[] { "NO-SUCH-SERVER" }, scope);
    }

    /// <summary>Review M1: a name with no EXACT match is a removed or disabled server's history name, never the resolver's partial
    /// hit. "sql2022" is contained in "sql2022-host" and "Prod" in both display names; neither may be remapped.</summary>
    [Theory]
    [InlineData("sql2022")]
    [InlineData("Prod")]
    [InlineData("Orders")]
    public void PartialName_IsNotRemapped_ToAnotherServer(string name)
    {
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { name }, Servers);
        Assert.Null(refusal);
        Assert.Equal(new[] { name }, scope);
    }

    /// <summary>The scenario from the review: "SQL01" was removed, "SQL01-DR" is the only enabled server.</summary>
    [Fact]
    public void RemovedServerName_ThatIsAPrefixOfAnEnabledServer_StaysAsWritten()
    {
        var enabled = new[] { new DarlingServerResolver.RegisteredServer(7, "SQL01-DR", null) };
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { "SQL01" }, enabled);
        Assert.Null(refusal);
        Assert.Equal(new[] { "SQL01" }, scope);
    }

    [Fact]
    public void SeveralServersSharingAnExactName_AreRefused_WithTheResolversSentence()
    {
        var shared = new[]
        {
            new DarlingServerResolver.RegisteredServer(1, "sql-a", "Prod"),
            new DarlingServerResolver.RegisteredServer(2, "sql-b", "prod"),
        };
        var (_, refusal) = ComposeServerScope.ResolveNames(new[] { "Prod" }, shared);
        Assert.NotNull(refusal);
        var message = McpHelpers.ErrorMessageOf(refusal!);
        Assert.Contains("matches 2 monitored servers", message, StringComparison.Ordinal);
        Assert.Contains("sql-a", message, StringComparison.Ordinal);
    }

    /// <summary>Item 17: a name that matches no registered or stored server stays as written and is reported as not found, naming it
    /// (the run's W13 answer, shared by the web panel run and the MCP tool), never zero rows with no message.</summary>
    [Fact]
    public void UnknownName_IsReportedNotFound_NamingIt()
    {
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { "NO-SUCH-SERVER" }, Servers);
        Assert.Null(refusal);
        var message = DarlingWebEndpoints.UnknownServerMessage(scope, new[] { "NO-SUCH-SERVER" }, new System.Text.Json.Nodes.JsonArray());
        Assert.NotNull(message);
        Assert.Contains("'NO-SUCH-SERVER'", message, StringComparison.Ordinal);
    }
}
