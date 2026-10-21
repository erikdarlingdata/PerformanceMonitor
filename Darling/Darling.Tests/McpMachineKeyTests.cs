/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using Xunit;
using Core = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// #6281: which registrations are "the same machine" for the add and edit rule. The host is compared with its spelling folded
/// away (case, a trailing dot, a tcp: prefix, host,port, host\MSSQLSERVER, an explicit default port); different instances,
/// ports and engines stay apart. A short name against its full domain name, or a name against its address, is not decided here (#6285).
/// </summary>
public sealed class McpMachineKeyTests
{
    private static Core.ParsedServerEntry Entry(string host, string? engine = null, int port = 0) =>
        new(0, host, host, new MonitoredServer { Name = host, Host = host, Engine = engine ?? "sqlserver", Port = port }, null);

    private static Core.RegisteredTarget Live(string host, string? engine = null, int port = 0) =>
        new(1, host, host, engine, port, host, false);

    private static Core.RegisteredTarget Removed(string storageKey) =>
        new(2, storageKey, null, null, 0, storageKey, true);

    [Theory]
    [InlineData("sqlprod01", "sqlprod01")]
    [InlineData("sqlprod01", "SQLPROD01")]
    [InlineData("sqlprod01", "sqlprod01.")]
    [InlineData("sqlprod01", "tcp:sqlprod01")]
    [InlineData("sqlprod01", "TCP:sqlprod01,1433")]
    [InlineData("sqlprod01", "sqlprod01,1433")]
    [InlineData("sqlprod01", "sqlprod01\\MSSQLSERVER")]
    [InlineData("sqlprod01", "sqlprod01.\\mssqlserver,1433")]
    [InlineData("sqlprod01,1444", "sqlprod01,1444")]
    [InlineData("sqlprod01\\REPORTS", "SQLPROD01\\reports")]
    public void TheseSpellings_AreOneMachine(string held, string entry)
    {
        Assert.True(Core.SameTarget(Live(held), Entry(entry)));
        Assert.True(Core.SameTarget(Live(entry), Entry(held)));
    }

    [Theory]
    [InlineData("sqlprod01", "sqlprod02")]
    [InlineData("sqlprod01", "sqlprod01,1444")]
    [InlineData("sqlprod01", "sqlprod01\\REPORTS")]
    [InlineData("sqlprod01\\REPORTS", "sqlprod01\\REPORTS,1433")]
    [InlineData("sqlprod01", "sqlprod01.example.test")]
    [InlineData("sqlprod01", "10.0.0.5")]
    [InlineData("sqlprod01", "xsqlprod01")]
    public void TheseAreOtherMachines(string held, string entry)
    {
        Assert.False(Core.SameTarget(Live(held), Entry(entry)));
        Assert.False(Core.SameTarget(Live(entry), Entry(held)));
    }

    [Fact]
    public void TheDefaultPort_IsOneMachine_AndAnotherPortIsNot()
    {
        Assert.True(Core.SameTarget(Live("sqlprod01"), Entry("sqlprod01", port: 1433)));
        Assert.True(Core.SameTarget(Live("sqlprod01", port: 1433), Entry("sqlprod01")));
        Assert.True(Core.SameTarget(Live("sqlprod01", port: 1444), Entry("sqlprod01,1444")));
        Assert.False(Core.SameTarget(Live("sqlprod01", port: 1444), Entry("sqlprod01")));
        Assert.False(Core.SameTarget(Live("sqlprod01"), Entry("sqlprod01", port: 1444)));
    }

    [Fact]
    public void APostgresInstance_IsNotTheSqlServerOnTheSameHost_AndItsDefaultPortFolds()
    {
        Assert.False(Core.SameTarget(Live("db01"), Entry("db01", "postgres")));
        Assert.False(Core.SameTarget(Live("db01", "postgres"), Entry("db01")));
        Assert.True(Core.SameTarget(Live("db01", "postgres"), Entry("DB01.", "postgres", 5432)));
        Assert.True(Core.SameTarget(Live("db01", "pg", 5433), Entry("db01", "PostgreSQL", 5433)));
        Assert.False(Core.SameTarget(Live("db01", "postgres", 5433), Entry("db01", "postgres")));
    }

    [Theory]
    [InlineData("sqlprod01:orders", "sqlprod01", 0, true)]
    [InlineData("sqlprod01:orders:RO", "SQLPROD01.", 1433, true)]
    [InlineData("sqlprod01:orders:1444", "sqlprod01,1444", 0, true)]
    [InlineData("sqlprod01:orders:1444", "sqlprod01", 0, false)]
    [InlineData("sqlprod01:1444", "sqlprod01", 1444, true)]
    [InlineData("sqlprod01:orders:pg:5432", "sqlprod01", 0, false)]
    [InlineData("tcp:sqlprod01:orders", "sqlprod01", 0, true)]
    [InlineData("sqlprod01x:orders", "sqlprod01", 0, false)]
    public void ARemovedRegistration_IsMatchedByItsStorageName(string storageKey, string host, int port, bool same) =>
        Assert.Equal(same, Core.SameTarget(Removed(storageKey), Entry(host, port: port)));

    [Fact]
    public void ARemovedPostgresRegistration_IsMatchedOnItsEngineAndPort()
    {
        Assert.True(Core.SameTarget(Removed("db01:orders:pg:5433"), Entry("db01", "postgres", 5433)));
        Assert.True(Core.SameTarget(Removed("db01:pg"), Entry("DB01", "postgres", 5432)));
        Assert.False(Core.SameTarget(Removed("db01:orders:pg:5433"), Entry("db01", "postgres")));
        Assert.False(Core.SameTarget(Removed("db01:orders:pg:5433"), Entry("db01", port: 5433)));
    }
}
