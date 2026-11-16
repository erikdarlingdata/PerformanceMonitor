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
        // The window is found through its own StoreSentenceText, from the desktop, then walked up to its Window: the
        // desktop-child lookup by title did not find it on the runner (the shot shows it open).
        var window = Step("Store Health window", () =>
        {
            try
            {
                var text = WaitFor(() => Automation.GetDesktop().FindFirstDescendant(cf => cf.ByAutomationId("StoreSentenceText")),
                    "StoreSentenceText on the desktop");
                var w = text;
                while (w is not null && w.ControlType != ControlType.Window) w = w.Parent;
                return (w ?? throw new Xunit.Sdk.XunitException("StoreSentenceText has no Window ancestor")).AsWindow();
            }
            catch
            {
                foreach (var c in Automation.GetDesktop().FindAllChildren())
                    Console.WriteLine("desktop child: " + c.ControlType + " '" + c.Name + "' " + c.Properties.ProcessId.ValueOrDefault);
                throw;
            }
        });
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

        MeasureEditDialogs(main);
    }

    /// <summary>The two auto-sized edit dialogs (#8804): each is opened from Settings, shot with its screen position logged
    /// ("DIALOG ..." lines), and closed. Both open with every field they can show.</summary>
    private void MeasureEditDialogs(AutomationElement main)
    {
        Safely("lite settings", () =>
        {
            var settings = OpenWindow("Settings", () => Find(main, "SettingsButton").AsButton().Invoke(), "SettingsTabs");

            Safely("lite route dialog", () =>
            {
                Step("Delivery tab", () => SelectTab(settings, "Delivery"));
                var routes = OpenWindow("Notification Routes window",
                    () => Find(settings, "ManageNotificationRoutesButton").AsButton().Invoke(), "RoutesGrid");
                var dialog = OpenWindow("route dialog", () => Find(routes, "AddRouteButton").AsButton().Invoke(), "ScopeCombo");
                ShotDialog("30-lite-route-dialog", dialog);
                CloseWindow("close route dialog", dialog, "ScopeCombo");
                CloseWindow("close Notification Routes window", routes, "RoutesGrid");
            });

            Safely("lite mute dialog", () =>
            {
                Step("Muting tab", () => SelectTab(settings, "Muting & triage"));
                var rules = OpenWindow("Mute Rules window", () => Find(settings, "ManageMuteRulesButton").AsButton().Invoke(), "RulesGrid");
                var dialog = OpenWindow("mute dialog", () => Find(rules, "AddRuleButton").AsButton().Invoke(), "ExpirationCombo");
                ShotDialog("31-lite-mute-dialog", dialog);
                CloseWindow("close mute dialog", dialog, "ExpirationCombo");
                CloseWindow("close Mute Rules window", rules, "RulesGrid");
            });

            CloseWindow("close Settings", settings, "SettingsTabs");
        });
        ThrowDialogFailures();
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
