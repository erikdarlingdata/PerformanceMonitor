/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1878: the first Alt mnemonics to land in either app's MAIN WINDOW access-key scope.
///
/// <para><b>Why that is different from every mnemonic before it.</b> #1847/#1860 put keys on dialogs, and a
/// dialog is its own window — its own scope, so a key only has to be unique within that one dialog. A
/// <c>UserControl</c> hosted in a <c>TabItem</c> gets NO scope of its own: these three checkboxes share the
/// main window's scope with the shell around them. Before this change both apps carried exactly ZERO keys in
/// that scope (every underscore in the tab controls is inside a <c>ContextMenu</c>, which IS its own scope),
/// measured by <see cref="MainWindowScopeKeys"/> below and validated against dialogs that do have keys.</para>
///
/// <para><b>Duplicate keys across per-server tabs are safe, and that is measured rather than assumed.</b>
/// Lite opens one <c>ServerTab</c> instance per connected server and holds them all — so N servers means N
/// live <c>_Auto-refresh</c> checkboxes as OBJECTS. WPF cycles focus instead of activating when a scope holds
/// duplicate keys, which would turn Alt+A into "move focus" and be worse than no mnemonic. It does not happen
/// here: a <c>TabControl</c> hosts only the SELECTED tab's content, so only one of them is ever in the visual
/// tree, which <see cref="TabControl_RealizesOnlyTheSelectedTabsContent"/> pins. Confirmed once with a real
/// shown window as well, where the unselected tab's checkbox reported <c>IsLoaded=false</c> while the selected
/// one reported true — that unload is what unregisters the key. The windowed form is not shipped because it
/// needs a desktop session; the structural form needs none and answers the same question.</para>
///
/// <para>The same structure is why <c>All Databases</c> (the FinOps tab) and <c>Auto-refresh</c> (a server
/// tab) cannot collide either — they are siblings in one <c>TabControl</c>. They still get DIFFERENT letters:
/// leaning on "these two tabs are mutually exclusive" would make the letters correct only for as long as
/// nobody moves a control, and D is free.</para>
/// </summary>
[Trait("Reads", "Darling")]
public sealed class MainWindowAccessKeyTests
{
    /// <summary>
    /// The control types WPF actually parses an access key out of. Anything else (TextBlock, ComboBox,
    /// TextBox) carries an underscore as a literal character, so scanning it would invent collisions.
    /// </summary>
    private const string KeyedControls =
        "Button|CheckBox|RadioButton|Label|ToggleButton|GroupBox|Expander|TabItem|MenuItem|DataGridColumnHeader";

    private const string LiteWindow = "Lite/MainWindow.xaml";
    private const string LiteControls = "Lite/Controls";
    private const string DarlingWindow = "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml";
    private const string DarlingControls = "Darling/PerformanceMonitor.Darling.Viewer";

    /// <summary>
    /// The tab bodies hosted from CODE rather than declared in the window's markup, so the derivation below
    /// cannot see them: both apps build their per-server tabs at runtime (Lite's <c>MainWindow.xaml.cs</c>
    /// news up a <c>ServerTab</c> per connected server and adds it to <c>ServerTabControl</c>). Named
    /// explicitly because they are the whole reason this issue exists.
    /// </summary>
    private static readonly Dictionary<string, string> s_dynamicallyHostedTab = new(StringComparer.Ordinal)
    {
        ["Lite"] = "Lite/Controls/ServerTab.xaml",
        ["Darling"] = "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml",
    };

    /// <summary>
    /// Every XAML file in one app's main-window access-key scope: the window itself, plus each
    /// <c>UserControl</c> it hosts.
    ///
    /// <para><b>Derived from the markup, not hand-listed</b>, because the hand-list was wrong when this test
    /// was first written — it missed all four of Darling's hosted tabs AND Lite's own
    /// <c>AvailabilityGroupsTab</c>. A list of file names maintained by hand is a list that silently stops
    /// matching what the window hosts, and an audit that quietly stops looking is worse than no audit: it
    /// reports success over the gap. Converters and template selectors share the same XML namespace prefix
    /// and fall out for free, having no <c>.xaml</c> of their own.</para>
    /// </summary>
    private static List<string> ScopeFiles(string app)
    {
        var windowPath = app == "Lite" ? LiteWindow : DarlingWindow;
        var controlsDir = app == "Lite" ? LiteControls : DarlingControls;
        var prefix = app == "Lite" ? "controls" : "local";

        var files = new List<string> { windowPath };

        foreach (Match hosted in Regex.Matches(ParitySource.ReadFile(windowPath), $@"<{prefix}:(\w+)\b"))
        {
            var candidate = $"{controlsDir}/{hosted.Groups[1].Value}.xaml";
            if (File.Exists(Path.Combine(ParitySource.RepoRoot(), candidate.Replace('/', Path.DirectorySeparatorChar)))
                && !files.Contains(candidate, StringComparer.Ordinal))
            {
                files.Add(candidate);
            }
        }

        files.Add(s_dynamicallyHostedTab[app]);
        return files;
    }

    [Fact]
    public void TheThreeInTabCheckboxes_CarryTheirAgreedMnemonics()
    {
        Assert.Contains("Content=\"_Auto-refresh\"", ParitySource.ReadFile("Lite/Controls/ServerTab.xaml"), StringComparison.Ordinal);
        Assert.Contains("Content=\"_Auto-refresh\"", ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml"), StringComparison.Ordinal);

        /* D, not A: see the class remarks — the FinOps and server tabs cannot be on screen together, but the
           letters must not DEPEND on that. */
        Assert.Contains("Content=\"All _Databases\"", ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml"), StringComparison.Ordinal);
    }

    /// <summary>
    /// The label is byte-identical in both SKUs, so the mnemonic is free parity — and parity that is free is
    /// exactly the kind that drifts unnoticed, because nothing makes the two files agree.
    /// </summary>
    [Fact]
    public void BothApps_UseTheSameKeyForAutoRefresh()
    {
        var lite = AutoRefreshContent(ParitySource.ReadFile("Lite/Controls/ServerTab.xaml"));
        var darling = AutoRefreshContent(ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml"));

        Assert.Equal(lite, darling);
        Assert.Equal('A', AccessKeyOf(lite));
    }

    [Theory]
    [InlineData("Lite")]
    [InlineData("Darling")]
    public void NothingInTheMainWindowScope_CollidesWithTheTabContentKeys(string app)
    {
        var files = ScopeFiles(app);

        /* Self-validating: a namespace-prefix rename would otherwise empty the derivation and turn this
           audit green by looking at nothing. Both apps host at least the per-server tab plus FinOps. */
        Assert.True(files.Count >= 3, $"{app}: the scope derivation found only {files.Count} file(s) — it has stopped seeing the hosted tabs.");
        Assert.Contains(s_dynamicallyHostedTab[app], files, StringComparer.Ordinal);

        var chrome = MainWindowScopeKeys(ParitySource.ReadFile(files[0]));

        /* Within the always-visible shell: no duplicates. */
        AssertNoDuplicates($"{app} main-window chrome", chrome);

        foreach (var tabFile in files.Skip(1))
        {
            var tab = MainWindowScopeKeys(ParitySource.ReadFile(tabFile));
            var name = Path.GetFileName(tabFile);

            /* Within one tab's content: no duplicates. */
            AssertNoDuplicates($"{app} {name}", tab);

            /* And against the shell, which is on screen at the same time as every tab. */
            var shared = tab.Select(t => t.Key).Intersect(chrome.Select(c => c.Key)).ToList();
            Assert.True(shared.Count == 0,
                $"{app}: {name} shares access key(s) {string.Join(", ", shared)} with the main window's own " +
                "controls, which are visible at the same time — Alt would cycle focus instead of activating.");
        }

        /* Cross-TAB duplicates are deliberately NOT asserted: only the selected tab's content is in the
           visual tree, which TabControl_RealizesOnlyTheSelectedTabsContent measures. */
    }

    /// <summary>
    /// The measurement the cross-tab exemption above rests on: a <c>TabControl</c> whose tabs each hold a
    /// UserControl with the SAME access key realizes only the selected one, so the scope never sees two.
    ///
    /// <para>Deliberately no <c>Window.Show()</c> — this needs to run on any CI agent, and the structural
    /// fact does not require a desktop. Reachability from the TabControl is the thing that matters: an
    /// element WPF cannot walk to from the root is not in the window's visual tree and has no registered
    /// key.</para>
    /// </summary>
    [Fact]
    public void TabControl_RealizesOnlyTheSelectedTabsContent()
    {
        var counts = OnStaThread(() =>
        {
            static UserControl TabBody()
            {
                var panel = new StackPanel();
                panel.Children.Add(new CheckBox { Content = "_Auto-refresh" });
                return new UserControl { Content = panel };
            }

            var tabs = new TabControl();
            var first = new TabItem { Header = "Server A", Content = TabBody() };
            var second = new TabItem { Header = "Server B", Content = TabBody() };
            tabs.Items.Add(first);
            tabs.Items.Add(second);

            int Realized()
            {
                tabs.Measure(new Size(800, 600));
                tabs.Arrange(new Rect(0, 0, 800, 600));
                tabs.UpdateLayout();
                return Descendants(tabs).OfType<CheckBox>().Count();
            }

            var onFirst = Realized();
            tabs.SelectedItem = second;
            var onSecond = Realized();
            return (onFirst, onSecond);
        });

        Assert.Equal(1, counts.onFirst);
        Assert.Equal(1, counts.onSecond);
    }

    /* ── helpers ── */

    /// <summary>
    /// Access keys a XAML file contributes to its WINDOW's scope: every keyed control's Content/Header,
    /// minus anything inside a <c>ContextMenu</c>, <c>ToolTip</c> or <c>Popup</c>, each of which WPF gives
    /// its own scope. That exclusion is doing real work rather than being defensive — FinOpsTab.xaml alone
    /// carries 20 context-menu keys and zero window-scope ones.
    /// </summary>
    private static List<(char Key, string Label)> MainWindowScopeKeys(string xaml)
    {
        foreach (var scoped in new[] { "ContextMenu", "ToolTip", "Popup" })
        {
            xaml = Regex.Replace(xaml, $@"<{scoped}[\s>].*?</{scoped}>", " ", RegexOptions.Singleline);
            xaml = Regex.Replace(xaml, $@"<[A-Za-z.]*\.{scoped}>.*?</[A-Za-z.]*\.{scoped}>", " ", RegexOptions.Singleline);
        }

        var found = new List<(char, string)>();
        foreach (Match m in Regex.Matches(xaml, $@"<(?:{KeyedControls})\b[^>]*?(?:Content|Header)=""([^""]*)"""))
        {
            var label = m.Groups[1].Value;
            var key = AccessKeyOf(label);
            if (key != '\0')
            {
                found.Add((key, label));
            }
        }

        return found;
    }

    /// <summary>
    /// The access key WPF would take from a label, or <c>'\0'</c> for none. <c>__</c> is an ESCAPED literal
    /// underscore and marks nothing, so it is neutralized before the search rather than matched.
    /// </summary>
    private static char AccessKeyOf(string label)
    {
        var m = Regex.Match(label.Replace("__", "\0", StringComparison.Ordinal), "_(.)");
        return m.Success ? char.ToUpperInvariant(m.Groups[1].Value[0]) : '\0';
    }

    private static string AutoRefreshContent(string xaml)
    {
        var m = Regex.Match(xaml, @"<CheckBox\s+x:Name=""AutoRefreshCheckBox""[^>]*?Content=""([^""]*)""");
        Assert.True(m.Success, "AutoRefreshCheckBox no longer declares its Content inline — this guard's anchor moved.");
        return m.Groups[1].Value;
    }

    private static void AssertNoDuplicates(string what, List<(char Key, string Label)> keys)
    {
        var dupes = keys.GroupBy(k => k.Key)
            .Where(g => g.Count() > 1)
            .Select(g => $"{g.Key} ({string.Join(" / ", g.Select(x => x.Label))})")
            .ToList();

        Assert.True(dupes.Count == 0,
            $"{what} has duplicate access key(s), which makes Alt cycle focus instead of activating: {string.Join("; ", dupes)}");
    }

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        var count = VisualTreeHelper.GetChildrenCount(root);
        for (var i = 0; i < count; i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var descendant in Descendants(child))
            {
                yield return descendant;
            }
        }
    }

    /// <summary>WPF objects require STA; same shape as Dashboard.Tests' DataGridExport tests.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }

}
