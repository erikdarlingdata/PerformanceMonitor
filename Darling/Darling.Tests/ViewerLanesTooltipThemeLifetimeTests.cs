/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Overview lanes' crosshair tooltip follows the theme only while its control is loaded (#8306). The manager that owns the
/// tooltip is built once per server tab and never disposed, so a theme subscription it holds for its whole life would keep every closed
/// tab's lanes, plots and data reachable from the static theme event for as long as the app runs. These tests show a control that has been
/// unloaded is collectable, that a theme switch while it is unloaded leaves its tooltip alone, and that the tooltip catches up with the
/// theme when the control loads again.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerLanesTooltipThemeLifetimeTests
{
    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static T Themed<T>(string theme, Func<Application, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = ThemeUri;
            try
            {
                ThemeManager.Apply(theme);
                return body(app);
            }
            finally
            {
                ThemeManager.Apply(previous);
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }

                ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
            }
        });

    private static Window Host(FrameworkElement content)
    {
        var root = new Grid();
        root.Children.Add(content);
        return new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
    }

    private static Grid RootOf(Window window) => (Grid)window.Content;

    private static CorrelatedTimelineLanesControl Lanes()
    {
        var control = new CorrelatedTimelineLanesControl();
        control.Initialize(null!, 1);
        return control;
    }

    private static CorrelatedCrosshairManager ManagerOf(CorrelatedTimelineLanesControl control) =>
        (CorrelatedCrosshairManager)typeof(CorrelatedTimelineLanesControl).GetField("_crosshairManager", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(control)!;

    private static Border TooltipFrameOf(CorrelatedCrosshairManager manager) =>
        (Border)((Popup)typeof(CorrelatedCrosshairManager).GetField("_tooltip", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!).Child;

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static Color TooltipBackground(Application app)
    {
        var style = (Style)app.FindResource(typeof(ToolTip));
        return ColorOf(style.Setters.OfType<Setter>().First(s => s.Property == Control.BackgroundProperty).Value as Brush);
    }

    private static string Hex(Color c) => $"#{c.R:X2}{c.G:X2}{c.B:X2}";

    /// <summary>A theme switched while the lanes are unloaded is not painted onto their tooltip, and is painted when they load again.</summary>
    [Fact]
    public void TheLanesTooltip_IsLeftAlone_WhileUnloaded_AndCatchesUp_WhenLoadedAgain()
    {
        var (dark, light, whileUnloaded, afterReload) = Themed("Dark", app =>
        {
            var control = Lanes();
            var window = Host(control);
            StaTestThread.ShowOffScreen(window);
            ClippedTextProbe.Settle(window);
            try
            {
                var frame = TooltipFrameOf(ManagerOf(control));
                var dark = TooltipBackground(app);
                RootOf(window).Children.Remove(control);
                ClippedTextProbe.Settle(window);
                ThemeManager.Apply("Light");
                var light = TooltipBackground(app);
                var whileUnloaded = ColorOf(frame.Background);
                RootOf(window).Children.Add(control);
                ClippedTextProbe.Settle(window);
                var afterReload = ColorOf(frame.Background);
                return (dark, light, whileUnloaded, afterReload);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotEqual(dark, light);
        Assert.True(dark == whileUnloaded, $"the tooltip of unloaded lanes was repainted to {Hex(whileUnloaded)} by a theme switch: the lanes still listen to the theme while unloaded, so a closed tab stays reachable from the theme event (expected the Dark {Hex(dark)})");
        Assert.True(light == afterReload, $"the tooltip of lanes loaded again under Light paints {Hex(afterReload)}, not the Light tooltip's {Hex(light)}");
    }

    /// <summary>Once its control is unloaded and dropped, the lanes control and its crosshair manager are collectable: no static event holds them.</summary>
    [Fact]
    public void TheLanesControl_AndItsCrosshairManager_AreCollectable_OnceUnloaded()
    {
        var (control, manager) = Themed("Dark", app =>
        {
            var window = new Window { Content = new Grid(), Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            try
            {
                var weak = ShowThenUnload(window);
                Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                return weak;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.False(manager.TryGetTarget(out _), "the lanes' crosshair manager is still reachable after its control was unloaded and dropped: something static holds it, and through its lanes the five plots and the tab they sit in");
        Assert.False(control.TryGetTarget(out _), "the lanes control is still reachable after it was unloaded and dropped");
    }

    /// <summary>Builds, shows, unloads and drops a lanes control in its own frame, so no local of the test keeps it alive through the collection.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference<CorrelatedTimelineLanesControl> Control, WeakReference<CorrelatedCrosshairManager> Manager) ShowThenUnload(Window window)
    {
        var control = Lanes();
        RootOf(window).Children.Add(control);
        StaTestThread.ShowOffScreen(window);
        ClippedTextProbe.Settle(window);
        var manager = ManagerOf(control);
        RootOf(window).Children.Remove(control);
        ClippedTextProbe.Settle(window);
        return (new WeakReference<CorrelatedTimelineLanesControl>(control), new WeakReference<CorrelatedCrosshairManager>(manager));
    }
}
