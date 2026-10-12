/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;

using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// F19 of the Lite click-through: the status bar read "Connected to SQL2025 - Data loaded" right after a server tab was
/// opened, then "Connected to SQL2022" every time the user switched to a tab, because the switch handler worded the
/// text on its own without the suffix. One helper words it now, for the open and for every switch.
/// </summary>
public class ServerTabStatusTextTests
{
    [Fact]
    public void ConnectedStatusText_KeepsTheLoadedSuffix_AndHasNoneBeforeTheFirstCollection()
    {
        Assert.Equal("Connected to SQL2022 - Data loaded", MainWindow.ConnectedStatusText("SQL2022", "Data loaded"));
        Assert.Equal("Connected to SQL2022 - Collection error: timeout", MainWindow.ConnectedStatusText("SQL2022", "Collection error: timeout"));
        Assert.Equal("Connected to SQL2022", MainWindow.ConnectedStatusText("SQL2022", null));
        Assert.Equal("Connected to SQL2022", MainWindow.ConnectedStatusText("SQL2022", ""));
    }

    [Fact]
    public void TheTabSwitchHandler_WordsTheStatusThroughTheSharedHelper()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs"));
        var start = source.IndexOf("private void ServerTabControl_SelectionChanged", StringComparison.Ordinal);
        Assert.True(start > 0, "the tab switch handler moved");
        var end = source.IndexOf("Refresh alerts tab when selected", start, StringComparison.Ordinal);
        var body = source.Substring(start, end - start);

        Assert.Contains("ConnectedStatusText(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"Connected to", body, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
