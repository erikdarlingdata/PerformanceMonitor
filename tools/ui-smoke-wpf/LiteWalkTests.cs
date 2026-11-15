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

    /// <summary>The Store Health window (#8788): opened from the sidebar's More menu, shown once its read has finished, then
    /// closed. The theme is Dark (Lite's default), so the shots before and after it show the dimmed sub-tab headers (#8770)
    /// and the access keys in the tab headers (#8768).</summary>
    protected override void ExtraSteps(AutomationElement main)
    {
        Step("More menu", () =>
        {
            Find(main, "MoreButton").AsButton().Invoke();
            return true;
        });
        Step("Store Health menu item", () =>
        {
            var item = WaitFor(() => Automation.GetDesktop()
                .FindFirstDescendant(cf => cf.ByControlType(ControlType.MenuItem).And(cf.ByName("Store Health"))), "a MenuItem named Store Health");
            Shot("20-more-menu");
            item.AsMenuItem().Invoke();
            return true;
        });
        var window = Step("Store Health window", () => WaitFor(() => App.GetAllTopLevelWindows(Automation)
            .FirstOrDefault(w => w.Title == "Store Health"), "a top-level window titled Store Health"));
        Step("Store Health read", () => WaitFor(() =>
            window.FindFirstDescendant(cf => cf.ByAutomationId("StatusText")) is { } status && status.Name.StartsWith("Read at", StringComparison.Ordinal)
                ? status : null, "StatusText reading 'Read at'"));
        Step("Store Health sections", () =>
        {
            foreach (var id in new[] { "StoreSentenceText", "StoreFactsText", "ArchiveFactsText", "ArchiveSentenceText", "HistoryGrid", "RefreshButton" })
                Find(window, id);
            return true;
        });
        window.Focus();
        Shot("21-store-health-dark");
        Step("close Store Health", () =>
        {
            window.Close();
            return true;
        });
        Shot("22-after-store-health");
    }

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
