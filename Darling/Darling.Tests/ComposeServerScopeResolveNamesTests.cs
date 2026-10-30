/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
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
/* #1776 own-store: the live test seeds its own scratch database (ScratchPostgres), so it shares no rows with another class. */
public sealed class ComposeServerScopeResolveNamesTests
{
    private static readonly IReadOnlyList<RegisteredServer> Servers = new[]
    {
        new RegisteredServer(1, "sql2022-host", "Prod Orders"),
        new RegisteredServer(2, "sql2025-host", "Prod Billing"),
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
        var enabled = new[] { new RegisteredServer(7, "SQL01-DR", null) };
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { "SQL01" }, enabled);
        Assert.Null(refusal);
        Assert.Equal(new[] { "SQL01" }, scope);
    }

    [Fact]
    public void SeveralServersSharingAnExactName_AreRefused_WithTheResolversSentence()
    {
        var shared = new[]
        {
            new RegisteredServer(1, "sql-a", "Prod"),
            new RegisteredServer(2, "sql-b", "prod"),
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
        var message = ComposePanelRunner.UnknownServerMessage(scope, new[] { "NO-SUCH-SERVER" }, new System.Text.Json.Nodes.JsonArray());
        Assert.NotNull(message);
        Assert.Contains("'NO-SUCH-SERVER'", message, StringComparison.Ordinal);
    }

    /// <summary>Review L1(b): the resolver trims, so a padded name resolves as the bare one does (a stored name, a display name).</summary>
    [Theory]
    [InlineData(" sql2022-host ", "sql2022-host")]
    [InlineData("  Prod Orders", "sql2022-host")]
    [InlineData("SQL2025-HOST\t", "sql2025-host")]
    public void PaddedName_ResolvesAsTheBareNameDoes(string padded, string expected)
    {
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { padded }, Servers);
        Assert.Null(refusal);
        Assert.Equal(new[] { expected }, scope);
    }

    /// <summary>Review L1(b): a padded name with no exact match stays as it was written.</summary>
    [Fact]
    public void PaddedUnknownName_StaysAsWritten()
    {
        var (scope, _) = ComposeServerScope.ResolveNames(new[] { " NO-SUCH " }, Servers);
        Assert.Equal(new[] { " NO-SUCH " }, scope);
    }

    /// <summary>Review L1(a): the stored name of a DISABLED registration (the list the scope now loads holds every server) beats an
    /// enabled server's display name, exactly and in another case.</summary>
    [Theory]
    [InlineData("SQL01")]
    [InlineData(" SQL01 ")]
    public void StoredNameOfAnyRegistration_BeatsAnotherServersDisplayName(string name)
    {
        var all = new[]
        {
            new RegisteredServer(1, "SQL01", "Old box"),
            new RegisteredServer(2, "NEWBOX", "SQL01"),
        };
        var (scope, refusal) = ComposeServerScope.ResolveNames(new[] { name }, all);
        Assert.Null(refusal);
        Assert.Equal(new[] { "SQL01" }, scope);

        var otherCase = new[]
        {
            new RegisteredServer(1, "sql01", "Old box"),
            new RegisteredServer(2, "NEWBOX", "SQL01"),
        };
        var (caseScope, caseRefusal) = ComposeServerScope.ResolveNames(new[] { name }, otherCase);
        Assert.Null(caseRefusal);
        Assert.Equal(new[] { "sql01" }, caseScope);
    }

    /// <summary>Review L1(a): the registry read behind the scope has no is_enabled filter, so a disabled registration is seen.</summary>
    [Fact]
    public void TheScopeRegistryRead_CoversEveryRegisteredServer()
    {
        Assert.DoesNotContain("is_enabled", ComposeServerScope.LoadAllServersSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FROM servers", ComposeServerScope.LoadAllServersSql, StringComparison.Ordinal);
    }

    /// <summary>Review L1(a), end to end: a DISABLED server "SQL01" with stored history is not taken by an enabled server whose display
    /// name is "SQL01"; a padded display name still resolves to its enabled server.</summary>
    [Fact]
    public async Task ResolveNamesAsync_SeesDisabledRegistrations()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live scope-name test.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: its own scratch database. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, 4101, "SQL01", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, 4102, "NEWBOX", ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET is_enabled = FALSE WHERE server_id = 4101");
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = 'SQL01' WHERE server_id = 4102");

        var (scope, refusal) = await ComposeServerScope.ResolveNamesAsync(dataSource, new List<string> { "SQL01", " NEWBOX " }, ct);
        Assert.Null(refusal);
        Assert.Equal(new[] { "SQL01", "NEWBOX" }, scope);
    }
}
