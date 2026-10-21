/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #6285: the connection test of add_servers and edit_server reads the name the SQL Server reports for itself
/// (<c>SERVERPROPERTY('ServerName')</c>) on the same connection, and one server reached under its host name and under another
/// name reports one identity, which is what the add and edit tools compare. Read only: one SELECT per connection.
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has none. Meant for the sql2019 dev box.</para>
/// </summary>
public sealed class McpMachineIdentitySqlServerLiveTests
{
    private static MonitoredServer Config(string host)
    {
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        return new MonitoredServer
        {
            Name = "identity-6285",
            Host = host,
            Auth = string.IsNullOrEmpty(user) ? "integrated" : "sql",
            Username = user,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };
    }

    [Fact]
    public async Task OneServerReachedUnderTwoNames_ReportsOneIdentity()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live identity read.");
        var ct = TestContext.Current.CancellationToken;

        var byName = await DarlingServerConnector.ProbeAsync(Config(host!), null, ct);
        Assert.True(byName.Success, byName.Error);
        Assert.False(string.IsNullOrWhiteSpace(byName.MachineIdentity));

        /* Windows authentication cannot reach a server by its address (the SPN names the host), so an integrated run uses the
           full domain name; a SQL login uses the address. Either is a spelling the host text cannot tie to the short name. */
        var other = string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"))
            ? (await Dns.GetHostEntryAsync(host!, ct)).HostName
            : (await Dns.GetHostAddressesAsync(host!, ct)).FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork)?.ToString();
        Assert.SkipWhen(string.IsNullOrEmpty(other) || string.Equals(other, host, StringComparison.OrdinalIgnoreCase), "The host has no second name to reach it by.");
        var byAddress = await DarlingServerConnector.ProbeAsync(Config(other!), null, ct);
        Assert.True(byAddress.Success, byAddress.Error);

        /* Two spellings the host text cannot tie together, one identity from the server. */
        Assert.NotEqual(host, other, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(byName.MachineIdentity, byAddress.MachineIdentity);
        Assert.Equal(
            Core.MachineIdentityOf(byName.MachineIdentity, byName.EngineEdition),
            Core.MachineIdentityOf(byAddress.MachineIdentity, byAddress.EngineEdition));
    }
}
