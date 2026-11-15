using System;
using System.IO;
using System.Threading.Tasks;
using FlaUI.Core.AutomationElements;
using FlaUI.Core.Definitions;
using Xunit;

namespace UiSmokeWpf;

/// <summary>
/// The Darling viewer walk (#5745 part 2): the same four checks as the Lite walk (<see cref="WalkBase"/>), on the real
/// viewer exe reading a throwaway PostgreSQL store that the workflow has migrated and seeded with
/// <c>tools/ui-smoke/seed.sql</c> (the SQL Server target <c>ui-sql</c>, with waits and queries). The viewer finds the
/// store through <c>DARLING_CONFIG</c>, which the workflow sets for this step.
/// </summary>
public sealed class ViewerWalkTests : WalkBase
{
    protected override string ServerName => "ui-sql";

    // Nothing to seed here: the workflow started the cluster, ran the service once to migrate it and applied seed.sql.
    protected override Task SeedAsync()
    {
        var cfg = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Assert.True(!string.IsNullOrEmpty(cfg) && File.Exists(cfg), "DARLING_CONFIG must name the smoke darling.json; the workflow sets it");
        return Task.CompletedTask;
    }

    [Fact]
    public void Walk_OverviewWaitStatsAndQueries_ShowTheSeededServer() => Walk();

    /// <summary>The four auto-sized edit dialogs (#8804): each is opened from its list window, shot with its screen position
    /// logged ("DIALOG ..." lines), and closed. The notification route dialog is reached through Settings.</summary>
    protected override void ExtraSteps(AutomationElement main)
    {
        // #8842: the two screens the dialogs below never reach. Each runs under Safely, so a screen that does not open fails the
        // walk at the end (with its own failed-* screenshot) and the dialog steps after it still run.
        Safely("viewer performance trends", () => PerformanceTrendsPastWeek(main));
        Safely("viewer store health", () => StoreHealth(main));

        Step("Alert History tab", () => SelectTabInWindow(main, "Alert History"));

        Safely("viewer mute dialog", () =>
        {
            var rules = OpenWindow("Mute Rules window", () => Find(main, "ManageMuteRulesButton").AsButton().Invoke(), "RulesGrid");
            var dialog = OpenWindow("mute dialog", () => Find(rules, "AddRuleButton").AsButton().Invoke(), "ExpirationCombo");
            ShotDialog("40-viewer-mute-dialog", dialog);
            CloseWindow("close mute dialog", dialog, "ExpirationCombo");
            CloseWindow("close Mute Rules window", rules, "RulesGrid");
        });

        Safely("viewer alert rule dialog", () =>
        {
            var rules = OpenWindow("Alert Rules window", () => Find(main, "AlertRulesButton").AsButton().Invoke(), "RulesGrid");
            var dialog = OpenWindow("alert rule dialog", () => Find(rules, "NewButton").AsButton().Invoke(), "MeasureCombo");
            // "These servers" shows the server list, the tallest choice.
            Step("scope: these servers", () =>
            {
                Find(dialog, "ScopeServers").AsRadioButton().IsChecked = true;
                return true;
            });
            ShotDialog("41-viewer-alert-rule-dialog", dialog);
            CloseWindow("close alert rule dialog", dialog, "MeasureCombo");
            CloseWindow("close Alert Rules window", rules, "RulesGrid");
        });

        Safely("viewer known pattern dialog", () =>
        {
            var patterns = OpenWindow("Known Patterns window", () => Find(main, "KnownPatternsButton").AsButton().Invoke(), "PatternsGrid");
            var dialog = OpenWindow("known pattern dialog", () => Find(patterns, "AddButton").AsButton().Invoke(), "ServerCombo");
            ShotDialog("42-viewer-known-pattern-dialog", dialog);
            CloseWindow("close known pattern dialog", dialog, "ServerCombo");
            CloseWindow("close Known Patterns window", patterns, "PatternsGrid");
        });

        Safely("viewer route dialog", () =>
        {
            var settings = OpenWindow("Settings", () => Find(main, "SettingsSidebarButton").AsButton().Invoke(), "SettingsTabs");
            Step("Delivery tab", () => SelectTabInWindow(settings, "Delivery"));
            var routes = OpenWindow("Notification Routes window",
                () => Find(settings, "ManageNotificationRoutesButton").AsButton().Invoke(), "RoutesGrid");
            var dialog = OpenWindow("route dialog", () => Find(routes, "AddRouteButton").AsButton().Invoke(), "ScopeCombo");
            ShotDialog("43-viewer-route-dialog", dialog);
            CloseWindow("close route dialog", dialog, "ScopeCombo");
            CloseWindow("close Notification Routes window", routes, "RoutesGrid");
            CloseWindow("close Settings", settings, "SettingsTabs");
        });
        ThrowDialogFailures();
    }

    /// <summary>The server's Performance Trends sub-tab at "Past week" (#8842): opens the time range picker, picks the preset,
    /// waits for the charts, and shoots the page and then its scrolled end, where the Executions chart sits when the four charts
    /// do not all fit.</summary>
    private void PerformanceTrendsPastWeek(AutomationElement main)
    {
        Step("Performance Trends sub-tab", () => SelectTab(main, "Performance Trends"));
        Step("open the time range picker", () =>
        {
            var picker = WaitFor(() => main.FindAllDescendants(cf => cf.ByControlType(ControlType.Button).And(cf.ByName("Time range")))
                .FirstOrDefault(b => !b.Properties.IsOffscreen.ValueOrDefault), "an on-screen Button named Time range");
            picker.AsButton().Invoke();
            return true;
        });
        // The popup is its own window, so the preset is looked up from the desktop.
        Step("pick Past week", () =>
        {
            var preset = WaitFor(() => Automation.GetDesktop().FindAllDescendants(cf => cf.ByControlType(ControlType.Button).And(cf.ByName("Past week")))
                .FirstOrDefault(), "a Button named Past week in the picker popup");
            preset.AsButton().Invoke();
            return true;
        });
        Step("the range reads Past week", () => WaitFor(() => main.FindAllDescendants(cf => cf.ByControlType(ControlType.Text).And(cf.ByName("Past week")))
            .FirstOrDefault(), "the picker button reading Past week"));
        // The charts are drawn images with nothing in the automation tree to wait on, so this is a fixed settle time for the
        // reads and the redraw. A chart that is still empty after it shows as empty in the shot.
        System.Threading.Thread.Sleep(10000);
        Shot("20-viewer-performance-trends-past-week");
        Step("scroll the charts to the end", () => ScrollTo(Find(main, "PerformanceTrendsScroll"), -1, 100));
        Shot("21-viewer-performance-trends-executions");
    }

    /// <summary>The Store Health main tab (#8842): waits for both grids and the "Last refreshed" stamp, presses Refresh once,
    /// waits again, and shoots the left of the tables grid, then its right end, where the time columns are.</summary>
    private void StoreHealth(AutomationElement main)
    {
        Step("Store Health tab", () => SelectTabInWindow(main, "Store Health"));
        Step("store health loaded", () => StoreHealthLoaded(main));
        Step("store health refresh", () =>
        {
            Find(main, "StoreHealthRefreshButton").AsButton().Invoke();
            return StoreHealthLoaded(main);
        });
        Shot("30-viewer-store-health");
        Step("scroll the tables grid right", () => ScrollTo(Find(main, "StoreHealthTablesGrid"), 100, -1));
        Shot("31-viewer-store-health-time-columns");
    }

    /// <summary>True once the tables and indexes grids have rows and the strip beside Refresh reads "Last refreshed ...", which
    /// the tab writes only after its tables, indexes and readiness reads have all finished.</summary>
    private static bool StoreHealthLoaded(AutomationElement main)
    {
        WaitForRows(main, "StoreHealthTablesGrid", ControlType.DataItem);
        WaitForRows(main, "StoreHealthIndexesGrid", ControlType.DataItem);
        var stamp = WaitFor(() =>
        {
            var t = main.FindFirstDescendant(cf => cf.ByAutomationId("StoreHealthLastRefreshed"));
            return t is not null && t.Name.StartsWith("Last refreshed", StringComparison.Ordinal) ? t : null;
        }, "the Store Health strip reading Last refreshed");
        Console.WriteLine("store health: " + stamp.Name);
        return true;
    }

    /// <summary>Scrolls through the element's UI Automation scroll pattern. Percentages are 0 to 100, and -1 leaves that
    /// direction alone. An element that cannot scroll is logged and left, since the shot then shows all of it.</summary>
    private static bool ScrollTo(AutomationElement element, double horizontalPercent, double verticalPercent)
    {
        var pattern = element.Patterns.Scroll.PatternOrDefault;
        if (pattern is null)
        {
            Console.WriteLine("scroll: " + element.AutomationId + " has no scroll pattern");
            return true;
        }

        pattern.SetScrollPercent(horizontalPercent, verticalPercent);
        System.Threading.Thread.Sleep(1000);
        return true;
    }

    protected override string FindExe()
    {
        var fromEnv = Environment.GetEnvironmentVariable("VIEWER_EXE");
        if (!string.IsNullOrEmpty(fromEnv) && File.Exists(fromEnv)) return fromEnv;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "Darling")))
        {
            dir = dir.Parent;
        }
        if (dir is not null)
        {
            foreach (var cfg in new[] { "Release", "Debug" })
            {
                var p = Path.Combine(dir.FullName, "Darling", "PerformanceMonitor.Darling.Viewer", "bin", cfg, "net10.0-windows",
                    "PerformanceMonitor.Darling.Viewer.exe");
                if (File.Exists(p)) return p;
            }
        }
        throw new FileNotFoundException("PerformanceMonitor.Darling.Viewer.exe not found; set VIEWER_EXE");
    }
}
