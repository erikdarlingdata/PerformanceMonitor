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
using FlaUI.Core.Input;
using FlaUI.Core.Tools;
using FlaUI.Core.WindowsAPI;
using FlaUI.UIA3;
using Xunit;

namespace UiSmokeWpf;

/// <summary>
/// The walk both apps share (#5745): start the real exe, then check the four named controls show the seeded server.
///
/// <para>Controls are found by AutomationId (ServerPicker, OverviewServerCardName, WaitStatsTypeList, QueryStatsGrid; Lite
/// and the Darling viewer carry the same ids) and tabs by their header text. Every wait is a bounded retry; on any failure
/// a screenshot of the screen goes to the output folder (UI_SMOKE_OUT, default %TEMP%\ui-smoke-wpf) for the workflow to
/// upload. Nothing here compares pixels.</para>
///
/// <para>The Overview card is asserted through its server-name text, not the card: the viewer's card is a plain Border
/// with no automation peer, so both apps put <c>OverviewServerCardName</c> on the TextBlock that shows the display
/// name.</para>
/// </summary>
public abstract class WalkBase : IAsyncLifetime
{
    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(90);

    private readonly string _out = Environment.GetEnvironmentVariable("UI_SMOKE_OUT")
        ?? Path.Combine(Path.GetTempPath(), "ui-smoke-wpf");

    private Application? _app;
    private UIA3Automation? _automation;

    /// <summary>The display name the seed gives the server the walk looks for.</summary>
    protected abstract string ServerName { get; }

    /// <summary>Makes the data the app will show: a Lite data folder, or nothing when the workflow seeds the store.</summary>
    protected abstract Task SeedAsync();

    /// <summary>The exe to start.</summary>
    protected abstract string FindExe();

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_out);
        await SeedAsync();

        var exe = FindExe();
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

    /// <summary>Picker, Overview card, Wait Stats types, Top Queries rows: the same four checks in both apps.</summary>
    protected void Walk()
    {
        var main = Step("main window", () =>
            Retry.WhileNull(() => _app!.GetMainWindow(_automation!), s_wait, TimeSpan.FromSeconds(1), ignoreException: true).Result
            ?? throw new Xunit.Sdk.XunitException("no main window within " + s_wait));
        Shot("00-main-window");

        // The named server picker lists the seeded server.
        Step("server picker", () => Find(main, "ServerPicker"));
        Step("server picker item", () => WaitFor(() => FindPickerItem(main), "a ServerPicker item named " + ServerName));

        // Overview: the fleet card for the seeded server, read through its server-name text.
        Step("Overview tab", () => SelectTab(main, "Overview"));
        Step("Overview card", () => WaitFor(() =>
            main.FindAllDescendants(cf => cf.ByAutomationId("OverviewServerCardName"))
                .FirstOrDefault(c => c.Name == ServerName), "an OverviewServerCardName reading " + ServerName));

        // Open the server (select the picker row, press Enter on the list), then walk its tabs. The row and the list are
        // looked up again at the action: the viewer rebinds the picker's items on each refresh, so an element found
        // earlier can belong to a row that has been replaced (#5761). Enter is the keyboard path #5763 added; a mouse
        // double-click is unreliable to drive from automation.
        Step("open the server", () => OpenServer(main));

        Step("Wait Stats tab", () => SelectTab(main, "Wait Stats"));
        Step("wait types", () => WaitForRows(main, "WaitStatsTypeList", ControlType.ListItem));

        Step("Queries tab", () => SelectTab(main, "Queries"));
        Step("Top Queries by Duration", () => SelectTab(main, "Top Queries by Duration"));
        Step("query stats rows", () => WaitForRows(main, "QueryStatsGrid", ControlType.DataItem));
        Shot("99-after-walk");
    }

    private AutomationElement? FindPickerItem(AutomationElement main) =>
        main.FindFirstDescendant(cf => cf.ByAutomationId("ServerPicker"))
            ?.FindAllDescendants(cf => cf.ByControlType(ControlType.ListItem))
            .FirstOrDefault(i => i.Name == ServerName);

    /// <summary>Selects the seeded server's picker row, then presses Enter with the picker focused. Every attempt looks
    /// the list and the row up afresh, and presses Enter only once the row reports itself selected.</summary>
    private bool OpenServer(AutomationElement main)
    {
        var r = Retry.WhileFalse(() =>
        {
            var picker = main.FindFirstDescendant(cf => cf.ByAutomationId("ServerPicker"));
            var item = picker is null ? null : FindPickerItem(main);
            if (picker is null || item is null) return false;

            var selection = item.Patterns.SelectionItem.Pattern;
            selection.Select();
            if (!selection.IsSelected.ValueOrDefault) return false;

            picker.Focus();
            Keyboard.Type(VirtualKeyShort.RETURN);
            return true;
        }, s_wait, TimeSpan.FromMilliseconds(500), ignoreException: true);
        Assert.True(r.Result, "could not select the " + ServerName + " picker row and press Enter within " + s_wait);
        return true;
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
        var r = Retry.WhileNull(find, s_wait, TimeSpan.FromMilliseconds(500), ignoreException: true);
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
        }, s_wait, TimeSpan.FromSeconds(1), ignoreException: true);
        Assert.True(r.Result, $"{automationId} showed 0 {rowType} rows after {s_wait}");
        Console.WriteLine($"{automationId}: {count} {rowType} rows");
        return count;
    }
}
