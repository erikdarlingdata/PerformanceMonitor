/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9436: the Viewer's store connections are named "PerformanceMonitorDarling-Viewer/&lt;build&gt;", so
/// <c>pg_stat_activity</c> says which build holds a backend. The build has no "+source hash", the whole name
/// fits PostgreSQL's 63 bytes by cutting only the build, and the monitor's own-session match still holds.
/// </summary>
public sealed class ViewerApplicationNameBuildTests
{
    private const string Prefix = "PerformanceMonitorDarling-Viewer/";

    [Fact]
    public void Compose_PutsTheBuildAfterTheBaseName_WithoutTheSourceHash()
    {
        Assert.Equal(Prefix + "3.13.0", ViewerSettings.ComposeApplicationName("3.13.0+0123abcd"));
        Assert.Equal(Prefix + "3.13.0-beta.2", ViewerSettings.ComposeApplicationName("3.13.0-beta.2"));
    }

    [Fact]
    public void Compose_WithoutABuild_IsTheBaseName()
    {
        Assert.Equal(ViewerSettings.ApplicationName, ViewerSettings.ComposeApplicationName(null));
        Assert.Equal(ViewerSettings.ApplicationName, ViewerSettings.ComposeApplicationName("+abc"));
    }

    [Fact]
    public void Compose_ALongBuild_IsCutAt63Bytes_AndOnlyTheBuildIsCut()
    {
        var name = ViewerSettings.ComposeApplicationName("1.2.3-" + new string('x', 200) + "+hash");

        Assert.Equal(63, Encoding.UTF8.GetByteCount(name));
        Assert.StartsWith(Prefix + "1.2.3-x", name, StringComparison.Ordinal);
    }

    [Fact]
    public void Compose_AMultiByteBuild_NeverSplitsACharacter_AndStaysWithin63Bytes()
    {
        var name = ViewerSettings.ComposeApplicationName(new string('é', 100));

        Assert.True(Encoding.UTF8.GetByteCount(name) <= 63);
        Assert.StartsWith(Prefix, name, StringComparison.Ordinal);
        Assert.DoesNotContain('�', name);
    }

    [Fact]
    public void BuiltName_CarriesTheRunningBuild_AndStillMatchesTheMonitorsOwnSessions()
    {
        var informational = typeof(ViewerSettings).Assembly
            .GetCustomAttributes<AssemblyInformationalVersionAttribute>().Single().InformationalVersion;
        var build = informational.Split('+')[0];
        var name = ViewerSettings.BuiltApplicationName;

        Assert.StartsWith(Prefix, name, StringComparison.Ordinal);
        Assert.True(Encoding.UTF8.GetByteCount(name) <= 63);
        Assert.DoesNotContain("+", name, StringComparison.Ordinal);
        Assert.StartsWith(name[Prefix.Length..], build, StringComparison.Ordinal);
        Assert.True(MonitorApplicationNames.MatchesSqlProgramNamePattern(name));
    }

    [Fact]
    public void ManagedConnectionString_CarriesTheBuiltName()
    {
        var connectionString = ViewerSettings.BuildManagedConnectionString(5641, "viewer", "pw");
        var applicationName = new NpgsqlConnectionStringBuilder(connectionString).ApplicationName;

        Assert.Equal(ViewerSettings.BuiltApplicationName, applicationName);
        Assert.StartsWith(Prefix, applicationName, StringComparison.Ordinal);
    }

    [Fact]
    public void BringYourOwnConnectionString_WithoutAName_GetsTheBuiltName()
    {
        var result = ViewerDataService.NameStoreConnection("Host=h;Database=d;Username=u");

        Assert.Equal(ViewerSettings.BuiltApplicationName, new NpgsqlConnectionStringBuilder(result).ApplicationName);
    }

    [Fact]
    public void BringYourOwnConnectionString_WithItsOwnName_KeepsIt()
    {
        var result = ViewerDataService.NameStoreConnection("Host=h;Database=d;Username=u;Application Name=OperatorsOwn");

        Assert.Equal("OperatorsOwn", new NpgsqlConnectionStringBuilder(result).ApplicationName);
    }
}
