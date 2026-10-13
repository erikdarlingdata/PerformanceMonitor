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

/// <summary>
/// The Lite walk (#5745): seed the data folder, start the real exe, and check the named controls show the seeded data.
///
/// <para>Controls are found by AutomationId (ServerPicker, OverviewServerCard, WaitStatsTypeList, QueryStatsGrid; the
/// Darling viewer carries the same ids) and tabs by their header text. Every wait is a bounded retry; on any failure a
/// screenshot of the screen goes to the output folder (UI_SMOKE_OUT, default %TEMP%\ui-smoke-wpf) for the workflow to
/// upload. Nothing here compares pixels.</para>
/// </summary>
public sealed class LiteWalkTests : IAsyncLifetime
{
    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(90);

    private readonly string _out = Environment.GetEnvironmentVariable("UI_SMOKE_OUT")
        ?? Path.Combine(Path.GetTempPath(), "ui-smoke-wpf");

    private Application? _app;
    private UIA3Automation? _automation;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_out);
        await LiteSeed.SeedAsync();

        var exe = FindLiteExe();
        Console.WriteLine("starting " + exe);
        var psi = new ProcessStartInfo(exe) { WorkingDirectory = Path.GetDirectoryName(exe)!, UseShellExecute = false };
        _app = Application.Launch(psi);
        _automation = new UIA3Automation();
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            _app?.Close();
            if (_app is { HasExited: false }) _app.Kill();
        }
        catch (Exception ex)
        {
            Console.WriteLine("closing the app: " + ex.Message);
        }
        _automation?.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void Walk_OverviewWaitStatsAndQueries_ShowTheSeededServer()
    {
        var main = Step("main window", () =>
            Retry.WhileNull(() => _app!.GetMainWindow(_automation!), s_wait, TimeSpan.FromSeconds(1)).Result
            ?? throw new Xunit.Sdk.XunitException("no main window within " + s_wait));
        Shot("00-main-window");

        // The named server picker lists the seeded server.
        var picker = Step("server picker", () => Find(main, "ServerPicker"));
        var pickerItem = Step("server picker item", () => WaitFor(() =>
            picker.FindAllDescendants(cf => cf.ByControlType(ControlType.ListItem))
                .FirstOrDefault(i => i.Name == LiteSeed.ServerName), "a ServerPicker item named " + LiteSeed.ServerName));

        // Overview: the fleet card for the seeded server.
        Step("Overview tab", () => SelectTab(main, "Overview"));
        Step("Overview card", () => WaitFor(() =>
            main.FindAllDescendants(cf => cf.ByAutomationId("OverviewServerCard"))
                .FirstOrDefault(c => c.Name == LiteSeed.ServerName), "an OverviewServerCard named " + LiteSeed.ServerName));

        // Open the server (double-click the picker row), then walk its tabs.
        Step("open the server", () => { pickerItem.DoubleClick(); return true; });

        Step("Wait Stats tab", () => SelectTab(main, "Wait Stats"));
        Step("wait types", () => WaitForRows(main, "WaitStatsTypeList", ControlType.ListItem));

        Step("Queries tab", () => SelectTab(main, "Queries"));
        Step("Top Queries by Duration", () => SelectTab(main, "Top Queries by Duration"));
        Step("query stats rows", () => WaitForRows(main, "QueryStatsGrid", ControlType.DataItem));
        Shot("99-after-walk");
    }

    private T Step<T>(string name, Func<T> action)
    {
        try
        {
            var result = action();
            Console.WriteLine("ok: " + name);
            return result;
        }
        catch
        {
            Console.WriteLine("FAILED: " + name);
            Shot("failed-" + name.Replace(' ', '-'));
            throw;
        }
    }

    private void Shot(string name)
    {
        try
        {
            Capture.Screen().ToFile(Path.Combine(_out, name + ".png"));
        }
        catch (Exception ex)
        {
            Console.WriteLine("screenshot " + name + " failed: " + ex.Message);
        }
    }

    private static AutomationElement Find(AutomationElement root, string automationId) =>
        WaitFor(() => root.FindFirstDescendant(cf => cf.ByAutomationId(automationId)), "an element with AutomationId " + automationId);

    private static T WaitFor<T>(Func<T?> find, string what) where T : class
    {
        var r = Retry.WhileNull(find, s_wait, TimeSpan.FromMilliseconds(500));
        return r.Result ?? throw new Xunit.Sdk.XunitException("timed out after " + s_wait + " waiting for " + what);
    }

    private static bool SelectTab(AutomationElement root, string header)
    {
        // The last match: the server tab's own "Overview" comes after the main window's, and tab headers repeat.
        var tab = WaitFor(() => root.FindAllDescendants(cf => cf.ByControlType(ControlType.TabItem).And(cf.ByName(header)))
            .Where(t => !t.Properties.IsOffscreen.ValueOrDefault).LastOrDefault(), "a TabItem named " + header);
        tab.AsTabItem().Select();
        return true;
    }

    private static int WaitForRows(AutomationElement root, string automationId, ControlType rowType)
    {
        var count = 0;
        var r = Retry.WhileFalse(() =>
        {
            var host = root.FindFirstDescendant(cf => cf.ByAutomationId(automationId));
            count = host?.FindAllDescendants(cf => cf.ByControlType(rowType)).Length ?? 0;
            return count > 0;
        }, s_wait, TimeSpan.FromSeconds(1));
        Assert.True(r.Result, $"{automationId} showed 0 {rowType} rows after {s_wait}");
        Console.WriteLine($"{automationId}: {count} {rowType} rows");
        return count;
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
