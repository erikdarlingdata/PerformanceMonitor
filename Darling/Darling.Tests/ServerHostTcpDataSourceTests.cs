/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6352: with no protocol prefix and no port, SqlClient tries TCP and then falls back to a named pipe over SMB with the
/// process's Windows credentials. Every connection to a monitored SQL Server takes its Data Source from
/// <see cref="ServerHostSetting.TcpDataSource"/>, so it is TCP only. These tests build the connection strings; none
/// opens a connection.
/// </summary>
public sealed class ServerHostTcpDataSourceTests
{
    [Theory]
    [InlineData("evil.example", "tcp:evil.example")]
    [InlineData("sql01", "tcp:sql01")]
    [InlineData("192.0.2.1", "tcp:192.0.2.1")]
    [InlineData("2001:db8::1", "tcp:2001:db8::1")]
    [InlineData("sql01\\INST1", "tcp:sql01\\INST1")]
    [InlineData("sql01,1433", "tcp:sql01,1433")]
    [InlineData("sql01\\INST1,1433", "tcp:sql01\\INST1,1433")]
    [InlineData("myserver.database.windows.net", "tcp:myserver.database.windows.net")]
    [InlineData("  sql01  ", "tcp:sql01")]
    public void RemoteHost_GetsTheTcpPrefix(string host, string expected) =>
        Assert.Equal(expected, ServerHostSetting.TcpDataSource(host));

    [Theory]
    [InlineData("tcp:sql01")]
    [InlineData("TCP:sql01,1433")]
    [InlineData("np:sql01")]
    [InlineData("np:\\\\sql01\\pipe\\sql\\query")]
    [InlineData("lpc:sql01")]
    [InlineData("admin:sql01")]
    [InlineData("\\\\sql01\\pipe\\sql\\query")]
    [InlineData("tcp:np:sql01")]
    public void HostWithAnExistingProtocol_IsLeftAsTyped(string host) =>
        Assert.Equal(host, ServerHostSetting.TcpDataSource(host));

    [Theory]
    [InlineData("(localdb)\\MSSQLLocalDB")]
    [InlineData("(LocalDB)\\.\\pipe\\x")]
    [InlineData(".")]
    [InlineData(".\\SQLEXPRESS")]
    [InlineData(".,1433")]
    [InlineData("(local)")]
    [InlineData("(LOCAL)\\INST1")]
    [InlineData("localhost")]
    [InlineData("LocalHost\\SQLEXPRESS")]
    [InlineData("localhost,1433")]
    [InlineData("127.0.0.1")]
    [InlineData("127.0.0.1,14330")]
    [InlineData("::1")]
    public void LocalHost_IsLeftAsTyped(string host) =>
        Assert.Equal(host, ServerHostSetting.TcpDataSource(host));

    [Theory]
    [InlineData("(localdb)x", "tcp:(localdb)x")]
    [InlineData("(localdb) \\x", "tcp:(localdb) \\x")]
    [InlineData("(localdb)192.0.2.1", "tcp:(localdb)192.0.2.1")]
    [InlineData("(localdb)", "tcp:(localdb)")]
    [InlineData("localhost.attacker.example", "tcp:localhost.attacker.example")]
    [InlineData("127.0.0.1.attacker.example", "tcp:127.0.0.1.attacker.example")]
    [InlineData("127.0.0.10", "tcp:127.0.0.10")]
    [InlineData("(local)x", "tcp:(local)x")]
    [InlineData(".x", "tcp:.x")]
    [InlineData("x.", "tcp:x.")]
    [InlineData("xlocalhost", "tcp:xlocalhost")]
    [InlineData("localhost2\\INST1", "tcp:localhost2\\INST1")]
    public void LooksLikeALocalHost_ButIsNot_GetsTheTcpPrefix(string host, string expected) =>
        Assert.Equal(expected, ServerHostSetting.TcpDataSource(host));

    [Theory]
    [InlineData("tcp :sql01")]
    [InlineData("np :sql01")]
    [InlineData("TCP\t:sql01")]
    [InlineData("np\u00A0:sql01")]
    [InlineData("np\u3000:sql01")]
    public void ProtocolWordWithWhitespaceBeforeTheColon_IsLeftAsTyped(string host) =>
        Assert.Equal(host, ServerHostSetting.TcpDataSource(host));

    [Fact]
    public void ThisMachinesOwnName_IsLeftAsTyped_InAnyCaseWithInstanceOrPort()
    {
        var name = Environment.MachineName;
        Assert.Equal(name, ServerHostSetting.TcpDataSource(name));
        Assert.Equal(name.ToLowerInvariant(), ServerHostSetting.TcpDataSource(name.ToLowerInvariant()));
        Assert.Equal(name + "\\INST1", ServerHostSetting.TcpDataSource(name + "\\INST1"));
        Assert.Equal(name + ",1433", ServerHostSetting.TcpDataSource(name + ",1433"));
        /* Only this machine's own name: a longer name that merely starts with it is another host. */
        Assert.Equal("tcp:" + name + "2", ServerHostSetting.TcpDataSource(name + "2"));
        Assert.Equal("tcp:x" + name, ServerHostSetting.TcpDataSource("x" + name));
        Assert.Equal("tcp:" + name + ".attacker.example", ServerHostSetting.TcpDataSource(name + ".attacker.example"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void BlankHost_IsReturnedAsGiven(string host) =>
        Assert.Equal(host, ServerHostSetting.TcpDataSource(host));

    [Fact]
    public void Prefixing_IsIdempotent()
    {
        var once = ServerHostSetting.TcpDataSource("sql01\\INST1");
        Assert.Equal(once, ServerHostSetting.TcpDataSource(once));
    }

    private static MonitoredServer Server(string host) => new()
    {
        Name = "srv", Host = host, Database = "app", Auth = "sql", Username = "monitor", Password = "x",
        EncryptMode = "Mandatory", RemediationUsername = "svc_fix", RemediationEncryptedPassword = "env:X",
    };

    [Fact]
    public void DarlingService_CollectionConnection_CarriesTcpForARemoteHost()
    {
        var parsed = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(Server("evil.example"), "pw"));
        Assert.Equal("tcp:evil.example", parsed.DataSource);

        var local = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(Server("localhost"), "pw"));
        Assert.Equal("localhost", local.DataSource);
    }

    [Fact]
    public void DarlingService_RemediationConnectionString_CarriesTcpForARemoteHost()
    {
        var parsed = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildRemediationConnectionString(Server("evil.example"), "pw"));
        Assert.Equal("tcp:evil.example", parsed.DataSource);
    }

    private static RemediationLogin LoginFor(string user, string secret)
    {
        var name = "PM_TCP_CONN_" + Guid.NewGuid().ToString("N");
        try
        {
            Environment.SetEnvironmentVariable(name, secret);
            return RemediationLoginResolver.Resolve(user, "env:" + name).Login!;
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public void Remediation_Connection_CarriesTcpForARemoteHost()
    {
        var login = LoginFor("svc_fix", "pw");
        var remote = new SqlConnectionStringBuilder(RemediationConnection.BuildConnectionString(
            new RemediationServer("evil.example", "app", "Mandatory", false, false), login));
        Assert.Equal("tcp:evil.example", remote.DataSource);

        var local = new SqlConnectionStringBuilder(RemediationConnection.BuildConnectionString(
            new RemediationServer("(localdb)\\MSSQLLocalDB", "app", "Mandatory", false, false), login));
        Assert.Equal("(localdb)\\MSSQLLocalDB", local.DataSource);
    }

    /// <summary>
    /// The Viewer's Test Connection opens nothing itself: it sends the server as args_json and the service's probe builds
    /// the connection from the MonitoredServer that args_json deserializes into (<c>DarlingCommandExecutor</c>'s probe branch, then
    /// <c>DarlingServerConnector.ResolveConnectionString</c>, which ends in <c>MonitoredServerConnection</c>). So the
    /// Viewer's test connects over TCP only when that string does.
    /// </summary>
    [Fact]
    public void ViewerTestConnect_ArgsBuildAConnectionThatCarriesTcp()
    {
        var args = ViewerDataService.BuildTestConnectArgs(new TestConnectServer { Name = "srv", Host = "evil.example", Auth = "integrated" });
        var server = JsonSerializer.Deserialize<MonitoredServer>(args, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        Assert.NotNull(server);

        var parsed = new SqlConnectionStringBuilder(DarlingServerConnector.ResolveConnectionString(server!));
        Assert.Equal("tcp:evil.example", parsed.DataSource);
    }
}
