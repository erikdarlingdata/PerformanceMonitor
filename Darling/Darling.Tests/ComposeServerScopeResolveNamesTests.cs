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

    [Fact]
    public void AmbiguousName_IsRefused_WithTheResolversSentence()
    {
        var (_, refusal) = ComposeServerScope.ResolveNames(new[] { "Prod" }, Servers);
        Assert.NotNull(refusal);
        var message = McpHelpers.ErrorMessageOf(refusal!);
        Assert.Contains("matches 2 monitored servers", message, StringComparison.Ordinal);
        Assert.Contains("sql2022-host", message, StringComparison.Ordinal);
    }
}
