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
/// overflow. At 900x600 the filters toolbar overflows (Compare, Apply to All and Use by tag), so there is something to open.
/// The tab's credential resolver reaches the real Credential Manager, so the class runs in that collection (#5785).
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabOverflowEscapeTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void TheFiltersToolbarOverflow_OpenedByAClick_ClosesOnEscape()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabOverflowEscape_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (hadOverflow, openedByClick, openAfterEscape) = StaTestThread.Run(() => WithTheme(() =>
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
