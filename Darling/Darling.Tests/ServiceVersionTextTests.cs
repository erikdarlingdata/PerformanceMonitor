/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Manage Servers version lines (#5655): the service's version once above the grid, host name included, beside the viewer's
/// own; and the grid no longer carries a per-row "Installed Version" column that repeated the viewer's version.
/// </summary>
public sealed class ServiceVersionTextTests
{
    private static ServiceVersionRow Row(string host, string version) =>
        new(host, version, new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc), new DateTime(2026, 10, 1, 8, 0, 1, DateTimeKind.Utc));

    [Fact]
    public void OneHost_ShowsTheServiceVersionAndTheHostOnOneLine()
    {
        Assert.Equal("Service version: 3.10.0 (SQLHOST1)", ServiceVersionText.ServiceLines(new[] { Row("SQLHOST1", "3.10.0") }));
    }

    [Fact]
    public void TwoHosts_ShowOneLinePerHost_InHostOrder()
    {
        var text = ServiceVersionText.ServiceLines(new[] { Row("hostB", "3.11.0"), Row("HostA", "3.10.0") });

        Assert.Equal("Service versions:" + Environment.NewLine + "HostA: 3.10.0" + Environment.NewLine + "hostB: 3.11.0", text);
    }

    [Fact]
    public void NoRow_ReadsUnknown_OlderThan311()
    {
        Assert.Equal("Service version: unknown (older than 3.11)", ServiceVersionText.ServiceLines(Array.Empty<ServiceVersionRow>()));
        Assert.Equal("unknown (older than 3.11)", ServiceVersionText.Unknown);
        Assert.Equal("Service version: unknown (older than 3.11)", ServiceVersionText.ServiceLines(new List<ServiceVersionRow>()));
    }

    [Fact]
    public void TheViewerLine_NamesTheViewersOwnVersion()
    {
        Assert.Equal("Viewer version: 3.7.1", ServiceVersionText.ViewerLine("3.7.1"));
    }

    [Fact]
    public void TheGrid_HasNoInstalledVersionColumn_AndTheWindowShowsBothVersionsOnce()
    {
        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ManageServersWindow.xaml");
        var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ManageServersWindow.xaml.cs");

        Assert.DoesNotContain("Installed Version", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("InstalledVersion", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("InstalledVersion", code, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"ServiceVersionBlock\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"ViewerVersionBlock\"", xaml, StringComparison.Ordinal);
        Assert.Contains("ServiceVersionText.ServiceLines(await _dataService.GetServiceVersionsAsync())", code, StringComparison.Ordinal);
        Assert.Contains("ServiceVersionText.ViewerLine(appVersion)", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSelectedSql_ReadsOnlyThisTable_AndTheViewerNeverWritesIt()
    {
        Assert.Contains("FROM config.service_version", ServiceVersionTable.SelectSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.ServerStatus.cs");
        Assert.DoesNotContain("ServiceVersionTable.UpsertAsync", viewer, StringComparison.Ordinal);
    }
}
