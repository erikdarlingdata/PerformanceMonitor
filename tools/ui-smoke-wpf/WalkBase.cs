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

    /// <summary>How long the sidebar step gives the server picker to appear before it treats the sidebar as a rail. Both
    /// apps put the picker in the automation tree as soon as the main window has laid out, so a picker still missing after
    /// this is a collapsed one, not a slow one.</summary>
    private static readonly TimeSpan s_sidebarProbe = TimeSpan.FromSeconds(10);

    private readonly string _out = Environment.GetEnvironmentVariable("UI_SMOKE_OUT")
        ?? Path.Combine(Path.GetTempPath(), "ui-smoke-wpf");

    private Application? _app;
    private UIA3Automation? _automation;

    /// <summary>The running app, for a walk that looks at windows other than the main one.</summary>
    protected Application App => _app!;

    /// <summary>The UIA automation the walk reads the app through.</summary>
    protected UIA3Automation Automation => _automation!;

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

        // On a narrow screen both apps start with the sidebar as a rail (#8266 in the viewer, #8219 in Lite), and the viewer's
        // rail takes the server list out of the automation tree. A person opens the pane with the rail's toggle, so the walk
        // does the same before it looks for the picker (#8311).
        Step("sidebar", () => OpenSidebarIfRail(main));

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
        Shot("10-wait-stats-tabs");

        Step("Queries tab", () => SelectTab(main, "Queries"));
        Step("Top Queries by Duration", () => SelectTab(main, "Top Queries by Duration"));
        Step("query stats rows", () => WaitForRows(main, "QueryStatsGrid", ControlType.DataItem));
        Shot("11-queries-subtabs");

        ExtraSteps(main);
        Shot("99-after-walk");
    }

    /// <summary>Steps one app adds after the shared four checks. The default adds none.</summary>
    protected virtual void ExtraSteps(AutomationElement main)
    {
    }

    /// <summary>Presses the sidebar's toggle (<c>SidebarToggleButton</c>) when the server picker is not in the automation
    /// tree within <see cref="s_sidebarProbe"/>. A sidebar that already shows the picker is left alone, so an app whose
    /// rail keeps its list in the tree (Lite) walks as it does on a wide screen. The toggle fixes the pane open, so a later
    /// resize does not collapse it again. The picker step that follows still decides whether the list is there.</summary>
    private static bool OpenSidebarIfRail(AutomationElement main)
    {
        var picker = Retry.WhileNull(() => main.FindFirstDescendant(cf => cf.ByAutomationId("ServerPicker")),
            s_sidebarProbe, TimeSpan.FromMilliseconds(500), ignoreException: true).Result;
        if (picker is not null)
        {
            Console.WriteLine("sidebar: open, the picker is in the tree");
            return false;
        }

        var toggle = Find(main, "SidebarToggleButton");
        toggle.AsButton().Invoke();
        Console.WriteLine("sidebar: a rail with no picker in the tree; pressed its toggle");
        return true;
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

    protected T Step<T>(string name, Func<T> action)
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

    protected void Shot(string name)
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

    protected static AutomationElement Find(AutomationElement root, string automationId) =>
        WaitFor(() => root.FindFirstDescendant(cf => cf.ByAutomationId(automationId)), "an element with AutomationId " + automationId);

    protected static T WaitFor<T>(Func<T?> find, string what) where T : class
    {
        var r = Retry.WhileNull(find, s_wait, TimeSpan.FromMilliseconds(500), ignoreException: true);
        return r.Result ?? throw new Xunit.Sdk.XunitException("timed out after " + s_wait + " waiting for " + what);
    }

    protected static bool SelectTab(AutomationElement root, string header)
    {
        // The last match: the server tab's own "Overview" comes after the main window's, and tab headers repeat.
        var tab = WaitFor(() => root.FindAllDescendants(cf => cf.ByControlType(ControlType.TabItem).And(cf.ByName(header)))
            .Where(t => !t.Properties.IsOffscreen.ValueOrDefault).LastOrDefault(), "a TabItem named " + header);
        tab.AsTabItem().Select();
        return true;
    }

    protected static int WaitForRows(AutomationElement root, string automationId, ControlType rowType)
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

    private readonly List<string> _dialogFailures = new();

    /// <summary>Runs one dialog measurement. A failure is kept and the walk goes on to the next dialog, after Escape has closed
    /// whatever the failed one left open. <see cref="ThrowDialogFailures"/> then fails the walk with all of them.</summary>
    protected void Safely(string name, Action body)
    {
        try
        {
            body();
        }
        catch (Exception ex)
        {
            _dialogFailures.Add(name + ": " + ex.Message.ReplaceLineEndings(" "));
            Console.WriteLine("DIALOG FAILED: " + name + ": " + ex.Message.ReplaceLineEndings(" "));
            Shot("failed-" + name.Replace(' ', '-'));
            for (var i = 0; i < 3; i++)
            {
                Keyboard.Type(VirtualKeyShort.ESCAPE);
                System.Threading.Thread.Sleep(500);
            }
        }
    }

    protected void ThrowDialogFailures()
    {
        if (_dialogFailures.Count > 0)
            throw new Xunit.Sdk.XunitException("dialog steps failed: " + string.Join(" | ", _dialogFailures));
    }

    /// <summary>Runs <paramref name="open"/> (a click that opens a window), then returns the window that holds the control
    /// with <paramref name="markerId"/>, found from the desktop and walked up to its Window.</summary>
    protected Window OpenWindow(string step, Action open, string markerId) => Step(step, () =>
    {
        open();
        var marker = WaitFor(() => Automation.GetDesktop().FindFirstDescendant(cf => cf.ByAutomationId(markerId)),
            "an element with AutomationId " + markerId + " on the desktop");
        var w = marker;
        while (w is not null && w.ControlType != ControlType.Window) w = w.Parent;
        return (w ?? throw new Xunit.Sdk.XunitException(markerId + " has no Window ancestor")).AsWindow();
    });

    /// <summary>Logs where a window sits on the screen (the numbers #8804 needs) and takes the screenshot. The desktop's
    /// rectangle is the screen, and the taskbar's top edge is the bottom of the work area.</summary>
    protected void ShotDialog(string name, Window w)
    {
        try { w.Focus(); } catch { /* the shot below still shows it */ }
        var screen = Automation.GetDesktop().BoundingRectangle;
        var r = w.BoundingRectangle;
        var tray = Automation.GetDesktop().FindFirstChild(cf => cf.ByClassName("Shell_TrayWnd"));
        var workBottom = tray is not null && tray.BoundingRectangle.Top > 0 ? tray.BoundingRectangle.Top : screen.Bottom;
        Console.WriteLine($"DIALOG {name}: top={r.Top} bottom={r.Bottom} height={r.Height} width={r.Width} left={r.Left} right={r.Right} "
            + $"screen={screen.Width}x{screen.Height} workAreaBottom={workBottom} fitsWorkArea={r.Top >= 0 && r.Bottom <= workBottom}");
        Shot(name);
    }

    /// <summary>Selects the first tab with this header inside one window, on-screen or not. A strip that scrolls (the viewer's
    /// "more tabs" button) keeps the tabs that do not fit in the tree, so <see cref="SelectTab"/>'s on-screen filter would never
    /// find them.</summary>
    protected static bool SelectTabInWindow(AutomationElement root, string header)
    {
        var tab = Retry.WhileNull(() => root.FindAllDescendants(cf => cf.ByControlType(ControlType.TabItem))
            .FirstOrDefault(t => t.Name == header), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(500), ignoreException: true).Result;
        if (tab is null)
        {
            Console.WriteLine("tabs found: " + string.Join(" | ", root.FindAllDescendants(cf => cf.ByControlType(ControlType.TabItem)).Select(t => "'" + t.Name + "'")));
            throw new Xunit.Sdk.XunitException("no TabItem named " + header);
        }
        tab.AsTabItem().Select();
        return true;
    }

    /// <summary>Closes a window the walk opened and waits until its marker control is gone from the desktop.</summary>
    protected void CloseWindow(string step, Window w, string markerId) => Step(step, () =>
    {
        w.Close();
        var gone = Retry.WhileFalse(() => Automation.GetDesktop().FindFirstDescendant(cf => cf.ByAutomationId(markerId)) is null,
            TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(500), ignoreException: true);
        Assert.True(gone.Result, markerId + " still on the desktop after closing " + step);
        return true;
    });
}
