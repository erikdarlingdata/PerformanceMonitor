using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using FlaUI.Core;
using FlaUI.Core.AutomationElements;
using FlaUI.Core.Capturing;
using FlaUI.Core.Definitions;
using FlaUI.Core.Tools;
using FlaUI.UIA3;
using Xunit;

namespace UiSmokeWpf;

/// <summary>The Lite walk (#5745): seed a data folder, start the real exe, and check the named controls (see <see cref="WalkBase"/>).</summary>
public sealed class LiteWalkTests : WalkBase
{
    protected override string ServerName => LiteSeed.ServerName;

    protected override Task SeedAsync() => LiteSeed.SeedAsync();

    [Fact]
    public void Walk_OverviewWaitStatsAndQueries_ShowTheSeededServer() => Walk();

    protected override string FindExe() => FindLiteExe();

    private static string FindLiteExe()
    {
        var fromEnv = Environment.GetEnvironmentVariable("LITE_EXE");
        if (!string.IsNullOrEmpty(fromEnv) && File.Exists(fromEnv)) return fromEnv;

        var beside = Path.Combine(AppContext.BaseDirectory, "PerformanceMonitorLite.exe");
        if (File.Exists(beside)) return beside;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "Lite")))
        {
            dir = dir.Parent;
        }
        if (dir is not null)
        {
            foreach (var cfg in new[] { "Release", "Debug" })
            {
                var p = Path.Combine(dir.FullName, "Lite", "bin", cfg, "net10.0-windows", "PerformanceMonitorLite.exe");
                if (File.Exists(p)) return p;
            }
        }
        throw new FileNotFoundException("PerformanceMonitorLite.exe not found; set LITE_EXE");
    }
}
