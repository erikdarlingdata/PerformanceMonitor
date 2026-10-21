/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6352: a server's host is never a named pipe, a UNC path or a SqlClient protocol prefix other than tcp:. The rule, and
/// the MCP and command-line add that apply it (the web add route runs the same core; the edit, route, Viewer and Lite
/// paths have their own tests next to their neighbours).
/// </summary>
public sealed class ServerHostSettingTests
{
    // A store that does not exist: a refused entry must be answered before any store access or probe.
    private const string DeadStore = "Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1;Command Timeout=1";

    private static readonly DarlingMcpServerAdminTools.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for a refused host");

    [Theory]
    [InlineData("sql01")]
    [InlineData("sql01.corp.example.test")]
    [InlineData("sql01,1433")]
    [InlineData("sql01\\INST1")]
    [InlineData("sql01\\INST1,1433")]
    [InlineData("ag-listener")]
    [InlineData("10.1.2.3")]
    [InlineData("10.1.2.3,1433")]
    [InlineData("[fe80::1]")]
    [InlineData("[fe80::1],1433")]
    [InlineData("2001:db8::1")]
    [InlineData("fe80::1")]
    [InlineData("::1")]
    [InlineData("abcd::1")]
    [InlineData("dead:beef::1")]
    [InlineData("tcp:sql01")]
    [InlineData("TCP:sql01,1433")]
    [InlineData("tcp:10.1.2.3,1433")]
    [InlineData("tcp:fe80::1")]
    [InlineData("  sql01  ")]
    [InlineData("")]
    [InlineData(null)]
    public void AnOrdinaryHost_IsAccepted(string? host) => Assert.Null(ServerHostSetting.Refusal(host));

    [Theory]
    [InlineData("np:sql01")]
    [InlineData("NP:sql01")]
    [InlineData("np:\\\\sql01\\pipe\\sql\\query")]
    [InlineData("lpc:sql01")]
    [InlineData("admin:sql01")]
    [InlineData("Admin:sql01\\INST1")]
    [InlineData("via:sql01")]
    [InlineData("  np:sql01")]
    [InlineData("anything:sql01")]
    [InlineData("tcp:np:sql01")]
    [InlineData("tcp:\\\\sql01\\pipe\\sql\\query")]
    [InlineData("\\\\sql01\\pipe\\sql\\query")]
    [InlineData("\\sql01")]
    [InlineData("//sql01/pipe/sql/query")]
    [InlineData("/sql01")]
    [InlineData("  \\\\sql01")]
    public void ANamedPipeOrAdminOrPathHost_IsRefused_NamingTheValueAndTheReason(string host)
    {
        var refusal = ServerHostSetting.Refusal(host);

        Assert.NotNull(refusal);
        Assert.Contains("The host '" + host.Trim() + "' is not accepted", refusal, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("np:sql01")]
    [InlineData("admin:sql01")]
    [InlineData("\\\\sql01\\pipe\\sql\\query")]
    [InlineData("//sql01/pipe/sql/query")]
    public async Task McpAddServers_RefusesTheHost_BeforeTheStoreAndTheProbe(string host)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var json = JsonSerializer.Serialize(new[] { new { host } });

        var result = await DarlingMcpServerAdminTools.AddServersAsync(dead, json, ThrowingProbe, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
        var row = doc.RootElement.GetProperty("results")[0];
        Assert.Equal("invalid", row.GetProperty("status").GetString());
        Assert.Contains("is not accepted", row.GetProperty("detail").GetString(), StringComparison.Ordinal);
    }
}
