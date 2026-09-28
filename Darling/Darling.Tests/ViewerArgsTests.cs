/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Viewer's argument rules (#4638): the elevated upgrade relaunch passes
/// <c>--upgrade-takeover</c>, which startup must never read as the darling.json path, and the relaunch
/// must carry the first launch's arguments forward.
/// </summary>
public sealed class ViewerArgsTests
{
    [Fact]
    public void UpgradeTakeoverAlone_IsNotAPath()
        => Assert.Null(ViewerArgs.ExplicitConfigPath(new[] { "--upgrade-takeover" }));

    [Fact]
    public void UpgradeTakeover_ThenPositional_GivesThePositional()
        => Assert.Equal("C:\\x\\darling.json", ViewerArgs.ExplicitConfigPath(new[] { "--upgrade-takeover", "C:\\x\\darling.json" }));

    [Fact]
    public void ConfigPair_WithUpgradeTakeover_GivesTheConfigPath()
        => Assert.Equal("C:\\x\\darling.json", ViewerArgs.ExplicitConfigPath(new[] { "--config", "C:\\x\\darling.json", "--upgrade-takeover" }));

    [Fact]
    public void OpenServerPair_WithUpgradeTakeover_IsNotAPath()
        => Assert.Null(ViewerArgs.ExplicitConfigPath(new[] { "--open-server", "A", "--upgrade-takeover" }));

    [Fact]
    public void PositionalAlone_IsThePath()
        => Assert.Equal("C:\\x\\darling.json", ViewerArgs.ExplicitConfigPath(new[] { "C:\\x\\darling.json" }));

    [Fact]
    public void HeadlessSelfTest_DelegatesToTheSharedRule()
        => Assert.Null(HeadlessSelfTest.ExplicitConfigPath(new[] { "--test", "--upgrade-takeover" }));

    [Fact]
    public void MainWindow_UsesTheSharedParser_NotItsOwnLoop()
    {
        var src = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));
        var i = src.IndexOf("string? ExplicitConfigPathFromArgs()", System.StringComparison.Ordinal);
        Assert.True(i > 0);
        var body = src.Substring(i, 200);
        Assert.Contains("ViewerArgs.ExplicitConfigPath(", body);
        Assert.DoesNotContain("for (", body);
    }

    [Fact]
    public void QuoteWindowsArgs_FollowsTheCommandLineRules()
    {
        Assert.Equal("a b", HandoffArgs.QuoteWindowsArgs(new[] { "a", "b" }));
        Assert.Equal("\"C:\\my dir\\d.json\"", HandoffArgs.QuoteWindowsArgs(new[] { "C:\\my dir\\d.json" }));
        Assert.Equal("\"say \\\"hi\\\"\"", HandoffArgs.QuoteWindowsArgs(new[] { "say \"hi\"" }));
        Assert.Equal("\"C:\\my dir\\\\\"", HandoffArgs.QuoteWindowsArgs(new[] { "C:\\my dir\\" }));
        Assert.Equal("\"\"", HandoffArgs.QuoteWindowsArgs(new[] { "" }));
    }

    [Fact]
    public void RelaunchArguments_CarryOriginalArgs_AndTakeoverExactlyOnce()
    {
        Assert.Equal("--config \"C:\\my dir\\d.json\" --upgrade-takeover",
            HandoffArgs.BuildRelaunchArguments(new[] { "--config", "C:\\my dir\\d.json" }));
        Assert.Equal("--upgrade-takeover", HandoffArgs.BuildRelaunchArguments(System.Array.Empty<string>()));
        var again = HandoffArgs.BuildRelaunchArguments(new[] { "x.json", "--upgrade-takeover" });
        Assert.Equal("x.json --upgrade-takeover", again);
        Assert.Single(again.Split(' ').Where(a => a == "--upgrade-takeover"));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
