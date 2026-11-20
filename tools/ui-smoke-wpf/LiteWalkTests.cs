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
        MeasureNewScreens(main);

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

    /// <summary>The screens the 11-15 nightly added to Lite, each shot at the runner's full window size. A step that fails is kept
    /// and the next one still runs, so one run shows every screen it can reach (<see cref="ThrowDialogFailures"/> fails the walk
    /// at the end of <see cref="MeasureEditDialogs"/>).
    /// <list type="bullet">
    /// <item>The tempdb tab (#8881): the chart's Log Used line and the card line naming the session that holds the log.</item>
    /// <item>The FinOps High Impact sub-tab (#9058).</item>
    /// <item>The Overview card and the sidebar dot of a server whose collection is off (#9085): both say Disabled.</item>
    /// </list></summary>
    private void MeasureNewScreens(AutomationElement main)
    {
        Safely("lite tempdb log", () =>
        {
            Step("tempdb tab", () => SelectTabInWindow(main, "tempdb"));
            // The chart and the holder line are drawn after the tab's read finishes; no control carries either as text.
            System.Threading.Thread.Sleep(5000);
            Shot("12-tempdb-log");
        });

        Safely("lite finops high impact", () =>
        {
            Step("FinOps tab", () => SelectMainTab(main, "FinOpsTab"));
            // The tab's own selector starts on the first server in its list, which is the disabled one: pick the seeded server.
            Step("FinOps server", () =>
            {
                var combo = Find(Find(main, "FinOpsContent"), "ServerSelector").AsComboBox();
                combo.Select(ServerName);
                return true;
            });
            Step("High Impact sub-tab", () => SelectTabInWindow(main, "High Impact"));
            Step("high impact rows", () => WaitForRows(main, "HighImpactDataGrid", ControlType.DataItem));
            Shot("13-finops-high-impact");
        });

        Safely("lite disabled server", () =>
        {
            Step("Overview tab again", () => SelectMainTab(main, "OverviewTab"));
            Step("disabled Overview card", () => WaitFor(() =>
                main.FindAllDescendants(cf => cf.ByAutomationId("OverviewServerCardName"))
                    .FirstOrDefault(c => c.Name == LiteSeed.DisabledServerName), "an OverviewServerCardName reading " + LiteSeed.DisabledServerName));
            System.Threading.Thread.Sleep(2000);
            Shot("14-overview-disabled-server");

            // The rail shows only dots. Open the pane the way a person would, so the Disabled server's row is named beside its dot.
            // No wait for the Offline server's connection check: its dot can still be grey here, and a fixed wait for it would add
            // a minute to every smoke run.
            Step("open the sidebar", () =>
            {
                var toggle = Find(main, "SidebarToggleButton");
                toggle.AsButton().Invoke();
                return true;
            });
            System.Threading.Thread.Sleep(2000);
            Shot("15-sidebar-open-disabled-server");
        });
    }

    /// <summary>Selects one of the main window's own tabs by its x:Name, which the server tab's same-named tabs do not share.
    /// Find waits for the name and throws when it never appears, so the step fails: selecting by header
    /// text instead could pick the server tab's tab of the same name.</summary>
    private static bool SelectMainTab(AutomationElement main, string automationId)
    {
        Find(main, automationId).AsTabItem().Select();
        return true;
    }

    /// <summary>The Alert rules tab (#8881, #8882, #8884): focuses the last of the four new switches so the page scrolls to show
    /// them, logs where each sits against the window, and takes the shot.</summary>
    private void ShotAlertSwitches(Window settings)
    {
        Step("Alert rules tab", () => SelectTab(settings, "Alert rules"));
        var ids = new[] { "AlertInstanceRestartedCheckBox", "AlertMemoryDumpsCheckBox", "AlertTempDbLogSpaceCheckBox", "AlertTempDbGrowthFitCheckBox" };
        var last = Find(settings, ids[^1]);
        last.Focus();
        System.Threading.Thread.Sleep(1000);
        var win = settings.BoundingRectangle;
        foreach (var id in ids)
        {
            var box = Find(settings, id);
            var r = box.BoundingRectangle;
            Console.WriteLine($"SWITCH {id}: offscreen={box.Properties.IsOffscreen.ValueOrDefault} top={r.Top} bottom={r.Bottom} "
                + $"window top={win.Top} bottom={win.Bottom} inside={r.Top >= win.Top && r.Bottom <= win.Bottom}");
        }
        ShotDialog("32-lite-alert-switches", settings);
    }

    /// <summary>The two auto-sized edit dialogs (#8804): each is opened from Settings, shot with its screen position logged
    /// ("DIALOG ..." lines), and closed. Both open with every field they can show.</summary>
    private void MeasureEditDialogs(AutomationElement main)
    {
        Safely("lite settings", () =>
        {
            var settings = OpenWindow("Settings", () => Find(main, "SettingsButton").AsButton().Invoke(), "SettingsTabs");

            Safely("lite alert switches", () => ShotAlertSwitches(settings));

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
