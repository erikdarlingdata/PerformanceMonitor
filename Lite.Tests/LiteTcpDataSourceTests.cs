/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6352: Lite's collection connection and the Add/Edit dialog's Test Connection both take their Data Source from
/// <c>ServerHostSetting.TcpDataSource</c>, so neither falls back to a named pipe over SMB with the process's Windows
/// credentials. These build the connection string; neither opens a connection.
/// </summary>
public sealed class LiteTcpDataSourceTests
{
    [Fact]
    public void CollectionConnection_CarriesTcpForARemoteHost_AndLeavesALocalOneAlone()
    {
        var remote = new ServerConnection { ServerName = "evil.example", AuthenticationType = AuthenticationTypes.SqlServer };
        Assert.Equal("tcp:evil.example", new SqlConnectionStringBuilder(remote.BuildConnectionString("u", "p")).DataSource);

        var local = new ServerConnection { ServerName = "(localdb)\\MSSQLLocalDB", AuthenticationType = AuthenticationTypes.SqlServer };
        Assert.Equal("(localdb)\\MSSQLLocalDB", new SqlConnectionStringBuilder(local.BuildConnectionString("u", "p")).DataSource);
    }

    [Fact]
    public void DialogTestConnection_CarriesTcpForARemoteHost_AndLeavesALocalOneAlone()
    {
        var remote = AddServerDialog.BuildBaseConnectionBuilder(
            "  sql01\\INST1,1433 ", "", false, SqlConnectionEncryptOption.Mandatory, false, false);
        Assert.Equal("tcp:sql01\\INST1,1433", remote.DataSource);
        Assert.Equal("master", remote.InitialCatalog);

        var local = AddServerDialog.BuildBaseConnectionBuilder(
            "localhost", "app", false, SqlConnectionEncryptOption.Mandatory, false, false);
        Assert.Equal("localhost", local.DataSource);
    }
}
