/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's server tab toolbar "..." overflow closes on Escape after a mouse click opened it. The click moves no keyboard focus
/// into the popup, so the key goes to whatever had focus before, and the toolbar alone never hears it: the window does, and closes
/// the overflow. At 900x600 the filters toolbar overflows, so there is something to open.
/// </summary>
public sealed class ViewerServerTabOverflowEscapeTests
{
    [Fact]
    public void TheFiltersToolbarOverflow_OpenedByAClick_ClosesOnEscape()
    {
        var (hadOverflow, openedByClick, openAfterEscape) = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "overflow-escape-test", "overflow-escape-test", true, null));
            typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
            var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var toolbar = (ToolBar)tab.FindName("ServerToolbarFilters");
                var toggle = Assert.IsType<ToggleButton>(toolbar.Template.FindName("OverflowButton", toolbar));
                var hadOverflow = toolbar.HasOverflowItems;

                /* A mouse click on the toggle, as its automation peer invokes it: the popup opens and focus stays where it was. */
                ((IToggleProvider)UIElementAutomationPeer.CreatePeerForElement(toggle).GetPattern(PatternInterface.Toggle)!).Toggle();
                window.UpdateLayout();
                var openedByClick = toolbar.IsOverflowOpen;

                /* Escape, as the keyboard delivers it: a key event routed from the focused element, the window when nothing holds focus. */
                var target = Keyboard.FocusedElement as UIElement ?? window;
                var source = PresentationSource.FromVisual(window)!;
                var preview = new KeyEventArgs(Keyboard.PrimaryDevice, source, 0, Key.Escape) { RoutedEvent = Keyboard.PreviewKeyDownEvent };
                target.RaiseEvent(preview);
                if (!preview.Handled)
                {
                    target.RaiseEvent(new KeyEventArgs(Keyboard.PrimaryDevice, source, 0, Key.Escape) { RoutedEvent = Keyboard.KeyDownEvent });
                }

                return (hadOverflow, openedByClick, toolbar.IsOverflowOpen);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(hadOverflow, "at 900x600 the filters toolbar has no overflow, so there is nothing to open");
        Assert.True(openedByClick, "the click on the overflow toggle did not open the popup");
        Assert.False(openAfterEscape, "the overflow popup is still open after Escape");
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var theme = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
        app.Resources.MergedDictionaries.Add(theme);
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Remove(theme);
        }
    }
}
