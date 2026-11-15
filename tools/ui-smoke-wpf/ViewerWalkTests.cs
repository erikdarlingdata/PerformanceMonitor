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
            var settings = OpenWindow("Settings", () => Find(main, "SettingsSidebarButton").AsButton().Invoke(), "ManageNotificationRoutesButton");
            Step("Delivery tab", () => SelectTabInWindow(settings, "Delivery"));
            var routes = OpenWindow("Notification Routes window",
                () => Find(settings, "ManageNotificationRoutesButton").AsButton().Invoke(), "RoutesGrid");
            var dialog = OpenWindow("route dialog", () => Find(routes, "AddRouteButton").AsButton().Invoke(), "ScopeCombo");
            ShotDialog("43-viewer-route-dialog", dialog);
            CloseWindow("close route dialog", dialog, "ScopeCombo");
            CloseWindow("close Notification Routes window", routes, "RoutesGrid");
            CloseWindow("close Settings", settings, "ManageNotificationRoutesButton");
        });
        ThrowDialogFailures();
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
