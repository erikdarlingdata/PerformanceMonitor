/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The server tab toolbar's "..." overflow closes on Escape after a mouse click opened it. The click moves no keyboard focus into
/// the popup, so the key goes to whatever had focus before, and the toolbar alone never hears it: the window does, and closes the
/// overflow. A control open inside the overflow (the Compare combo's list) takes the first Escape for itself, and the overflow stays
/// until the next one. At 900x600 the filters toolbar overflows (Compare, Apply to All and Use by tag), so there is something to open.
/// The tab's credential resolver reaches the real Credential Manager, so the class runs in that collection (#5785).
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class ServerTabOverflowEscapeTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void TheFiltersToolbarOverflow_OpenedByAClick_ClosesOnEscape()
    {
        var (hadOverflow, openedByClick, openAfterEscape) = WithOpenOverflow((tab, window, toolbar) =>
        {
            /* Escape, as the keyboard delivers it: a key event routed from the focused element, the window when nothing holds focus. */
            Escape(Keyboard.FocusedElement as UIElement ?? window, window);
            return (toolbar.HasOverflowItems, true, toolbar.IsOverflowOpen);
        });

        Assert.True(hadOverflow, "at 900x600 the filters toolbar has no overflow, so there is nothing to open");
        Assert.True(openedByClick, "the click on the overflow toggle did not open the popup");
        Assert.False(openAfterEscape, "the overflow popup is still open after Escape");
    }

    [Fact]
    public void EscapeInsideAnOpenComboInTheOverflow_ClosesTheComboFirst_AndTheOverflowOnTheSecondPress()
    {
        var (comboInOverflow, comboOpenAfterFirst, overflowOpenAfterFirst, overflowOpenAfterSecond) = WithOpenOverflow((tab, window, toolbar) =>
        {
            var combo = (ComboBox)tab.FindName("CompareToCombo");
            /* The toolbar's item is the panel holding the "Compare:" label and the combo, so the overflow mark sits on that panel. */
            var inOverflow = false;
            for (DependencyObject? node = combo; node is not null && node is not ToolBar; node = System.Windows.Media.VisualTreeHelper.GetParent(node) ?? LogicalTreeHelper.GetParent(node))
            {
                if (ToolBar.GetIsOverflowItem(node)) { inOverflow = true; break; }
            }
            combo.IsDropDownOpen = true;
            window.UpdateLayout();

            /* The first Escape, with the combo's list open: the combo takes it and the overflow stays. */
            Escape(combo, window);
            var comboOpenAfterFirst = combo.IsDropDownOpen;
            var overflowOpenAfterFirst = toolbar.IsOverflowOpen;

            /* The second, with nothing open inside the overflow: the overflow closes. */
            Escape(combo, window);
            return (inOverflow, comboOpenAfterFirst, overflowOpenAfterFirst, toolbar.IsOverflowOpen);
        });

        Assert.True(comboInOverflow, "at 900x600 the Compare combo is not in the overflow");
        Assert.False(comboOpenAfterFirst, "the combo's list is still open after Escape");
        Assert.True(overflowOpenAfterFirst, "Escape inside the open combo closed the whole overflow");
        Assert.False(overflowOpenAfterSecond, "the overflow popup is still open after the second Escape");
    }

    /// <summary>Raises Escape on <paramref name="target"/> the way the keyboard delivers it: the preview first, then the key itself unless the preview was handled.</summary>
    private static void Escape(UIElement target, Window window)
    {
        var source = PresentationSource.FromVisual(window)!;
        var preview = new KeyEventArgs(Keyboard.PrimaryDevice, source, 0, Key.Escape) { RoutedEvent = Keyboard.PreviewKeyDownEvent };
        target.RaiseEvent(preview);
        if (!preview.Handled)
        {
            target.RaiseEvent(new KeyEventArgs(Keyboard.PrimaryDevice, source, 0, Key.Escape) { RoutedEvent = Keyboard.KeyDownEvent });
        }
    }

    /// <summary>The real server tab at 900x600 with the filters toolbar's overflow opened by a click, as its automation peer invokes it: the popup opens and focus stays where it was.</summary>
    private static T WithOpenOverflow<T>(Func<ServerTab, Window, ToolBar, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabOverflowEscape_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "overflow-escape-test", DisplayName = "overflow-escape-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    ClippedTextProbe.Settle(window);
                    var toolbar = (ToolBar)tab.FindName("ServerToolbarFilters");
                    var toggle = Assert.IsType<ToggleButton>(toolbar.Template.FindName("OverflowButton", toolbar));
                    Assert.True(toolbar.HasOverflowItems, "at 900x600 the filters toolbar has no overflow, so there is nothing to open");
                    ((IToggleProvider)UIElementAutomationPeer.CreatePeerForElement(toggle).GetPattern(PatternInterface.Toggle)!).Toggle();
                    window.UpdateLayout();
                    Assert.True(toolbar.IsOverflowOpen, "the click on the overflow toggle did not open the popup");
                    return body(tab, window, toolbar);
                }
                finally
                {
                    window.Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
