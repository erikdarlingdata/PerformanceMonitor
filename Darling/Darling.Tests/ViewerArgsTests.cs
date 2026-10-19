/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
        Assert.Equal("--upgrade-takeover --config \"C:\\my dir\\d.json\"",
            HandoffArgs.BuildRelaunchArguments(new[] { "--config", "C:\\my dir\\d.json" }));
        Assert.Equal("--upgrade-takeover", HandoffArgs.BuildRelaunchArguments(System.Array.Empty<string>()));
        var again = HandoffArgs.BuildRelaunchArguments(new[] { "x.json", "--upgrade-takeover" });
        Assert.Equal("--upgrade-takeover x.json", again);
        Assert.Single(again.Split(' '), a => a == "--upgrade-takeover");
    }

    [Fact]
    public void RelaunchArguments_DanglingConfig_KeepsTakeoverFirst()
        => Assert.Equal("--upgrade-takeover --config", HandoffArgs.BuildRelaunchArguments(new[] { "--config" }));

    [Fact]
    public void ExplicitConfigPath_DanglingConfig_IsNull()
    {
        Assert.Null(ViewerArgs.ExplicitConfigPath(new[] { "--config" }));
        Assert.Null(ViewerArgs.ExplicitConfigPath(new[] { "--config", "--upgrade-takeover" }));
        Assert.Null(ViewerArgs.ExplicitConfigPath(new[] { "--upgrade-takeover", "--config" }));
        Assert.Null(ViewerArgs.ExplicitConfigPath(new[] { "--config", "--open-server", "X" }));
        Assert.Equal("a.json", ViewerArgs.ExplicitConfigPath(new[] { "--config", "a.json", "--upgrade-takeover" }));
    }

    [Fact]
    public void OpenServerName_ReadsOnlyARealValue()
    {
        Assert.Null(ViewerArgs.OpenServerName(new[] { "--open-server" }));
        Assert.Null(ViewerArgs.OpenServerName(new[] { "--open-server", "--config", "a.json" }));
        Assert.Equal("S1", ViewerArgs.OpenServerName(new[] { "--open-server", "S1" }));
    }

    public static TheoryData<string> HardArguments => new(HardList);

    private static readonly string[] HardList =
    {
        "", "\t", "a\"b", "a\\\"b", "trailing\\", "\\\\host\\share\\", "line\none", "naïve Ünïcödé 日本",
        "a\" --config \\\\host\\share\\x.json", "C:\\my dir\\", "say \"hi\"", "plain",
    };

    [Theory]
    [MemberData(nameof(HardArguments))]
    public void QuoteWindowsArgs_RoundTripsThroughCommandLineToArgvW_OneArgument(string argument)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CommandLineToArgvW is Windows-only.");
        Assert.Equal(new[] { argument }, WindowsArgv(HandoffArgs.QuoteWindowsArgs(new[] { argument })));
    }

    [Fact]
    public void QuoteWindowsArgs_RoundTripsThroughCommandLineToArgvW_AllTogether()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CommandLineToArgvW is Windows-only.");
        Assert.Equal(HardList, WindowsArgv(HandoffArgs.QuoteWindowsArgs(HardList)));
    }

    [Fact]
    public void Relaunch_NeverChangesWhichConfigTheChildReads()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CommandLineToArgvW is Windows-only.");
        var inputs = new[]
        {
            System.Array.Empty<string>(),
            new[] { "--config" },
            new[] { "--config", "C:\\my dir\\d.json" },
            new[] { "a b.json" },
            new[] { "--config", "--open-server", "X" },
            new[] { "--open-server", "X", "c.json" },
        };
        foreach (var input in inputs)
        {
            var child = WindowsArgv(HandoffArgs.BuildRelaunchArguments(input));
            Assert.Equal(ViewerArgs.ExplicitConfigPath(input), ViewerArgs.ExplicitConfigPath(child));
        }
    }

    [System.Runtime.InteropServices.DllImport("shell32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CommandLineToArgvW(string lpCmdLine, out int pNumArgs);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr LocalFree(IntPtr hMem);

    /// <summary>Windows' own parse of <paramref name="arguments"/>, after a stand-in program name.
    /// argv[0] is dropped (it has its own rules, and an empty command line returns the exe path).</summary>
    private static string[] WindowsArgv(string arguments)
    {
        var argv = CommandLineToArgvW("x.exe " + arguments, out var count);
        Assert.NotEqual(IntPtr.Zero, argv);
        try
        {
            var result = new string[count - 1];
            for (var i = 1; i < count; i++)
            {
                result[i - 1] = System.Runtime.InteropServices.Marshal.PtrToStringUni(
                    System.Runtime.InteropServices.Marshal.ReadIntPtr(argv, i * IntPtr.Size))!;
            }

            return result;
        }
        finally
        {
            LocalFree(argv);
        }
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
