/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins Lite's wiring of the Azure master duplicate skip: the target list comes from the server manager's
/// servers, carries each server's enabled flag and read-only intent, and identifies self by its server id.
/// </summary>
public sealed class AzureMasterLiteWiringTests
{
    [Fact]
    public void TheAlertPass_BuildsTheSkipListFromTheServerManager_ByServerId()
    {
        var text = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.AlertEngine.cs"));
        var start = text.IndexOf("private async void CheckPerformanceAlerts(", StringComparison.Ordinal);
        Assert.True(start >= 0, "CheckPerformanceAlerts declaration not found");
        var body = text[start..];

        Assert.Contains("badgeServer.Id.ToString(),", body, StringComparison.Ordinal);
        Assert.Contains("_serverManager.GetAllServers().Select(t => new AlertTargetIdentity(", body, StringComparison.Ordinal);
        Assert.Contains("t.Id.ToString(), t.ServerName, t.DatabaseName, t.IsEnabled, t.ReadOnlyIntent", body, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
