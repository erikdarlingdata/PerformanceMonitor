/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>The refusals of <c>get_store_query_history</c> (#5097), which are decided before any read.</summary>
public sealed class StoreQueryHistoryToolTests
{
    [Theory]
    [InlineData(0, 24, "top")]
    [InlineData(1001, 24, "top")]
    [InlineData(20, 0, "hours_back")]
    [InlineData(20, 2161, "hours_back")]
    public async Task ABadTopOrWindow_IsRefused_NamingTheParameter(int top, int hours, string parameter)
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Password=nothing;Timeout=1");

        var result = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(unreachable, top: top, hours_back: hours, cancellationToken: TestContext.Current.CancellationToken);

        Assert.StartsWith("{\"status\":\"invalid\"", result, StringComparison.Ordinal);
        Assert.Contains($"\"parameter\":\"{parameter}\"", result, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTextRead_KeepsTheSharedSensitiveStatementPredicate_OnTopOfTheReaderFunctionsOwnFilter()
    {
        /* #5097: the diagnostics bundle applied the shared predicate in its own statement-text read and now reads the text through
           this tool, so the tool's read applies it: a store whose reader function is older than the pattern still withholds. The
           predicate wraps the text the read chooses (max(query) per query id), so it is the shown text that is tested. */
        Assert.Contains(PgSensitiveStatementFilter.SqlPredicate("max(f.query)"), DarlingMcpStoreQueryHistoryTools.TextSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABadRole_IsRefused_ListingTheRoles()
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Password=nothing;Timeout=1");

        var result = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(unreachable, role: "darling", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Contains("\"parameter\":\"role\"", result, StringComparison.Ordinal);
        Assert.Contains("viewer", result, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("1.5")]
    [InlineData("99999999999999999999")]
    public async Task ANonNumericQueryId_IsRefused(string queryId)
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Password=nothing;Timeout=1");

        var result = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(unreachable, query_id: queryId, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Contains("\"parameter\":\"query_id\"", result, StringComparison.Ordinal);
    }
}
