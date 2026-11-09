/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Settings window's routine notices are the app's own message window, drawn from its theme, not a native message box in the
/// system theme. Apply Default to All with no server on a custom schedule is one of them, and the one a test can reach with no
/// server, no SMTP and no webhook: the window the click opens is read while it is up and closed from the dialog's own message loop.
/// </summary>
public sealed class SettingsNoticeInAppTests
{
    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    private static extern IntPtr FindWindow(string? className, string windowTitle);

    [DllImport("user32.dll")]
    private static extern bool PostMessage(IntPtr handle, uint message, IntPtr wParam, IntPtr lParam);

    private const uint WmClose = 0x0010;

    [Fact]
    public void ApplyDefaultWithNothingToChange_SaysSoInTheAppsOwnWindow_NotANativeBox()
    {
        var dir = Path.Combine(Path.GetTempPath(), "SettingsNoticeInApp_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            var seen = StaTestThread.Run(() => WithTheme(() =>
            {
                var settings = new SettingsWindow(new ScheduleManager(dir), new ServerManager(dir)) { PersistSelectedTab = _ => { } };
                StaTestThread.ShowOffScreen(settings);
                try
                {
                    string? seen = null;
                    /* Queued before the click and run by the dialog's own message loop, so whichever dialog the click opens is up by then;
                       it is closed from here so the click returns. */
                    settings.Dispatcher.BeginInvoke(DispatcherPriority.Background, () =>
                    {
                        var owned = settings.OwnedWindows.Cast<Window>().FirstOrDefault();
                        if (owned is not null)
                        {
                            var body = ClippedTextProbe.Descendants(owned).OfType<TextBlock>().Select(b => b.Text).FirstOrDefault(t => !string.IsNullOrWhiteSpace(t));
                            seen = $"in-app window '{owned.Title}': {body}";
                            owned.Close();
                            return;
                        }

                        var native = FindWindow("#32770", "Apply Default");
                        if (native == IntPtr.Zero)
                        {
                            native = FindWindow(null, "Apply Default");
                        }

                        if (native != IntPtr.Zero)
                        {
                            seen = "native message box 'Apply Default'";
                            PostMessage(native, WmClose, IntPtr.Zero, IntPtr.Zero);
                            return;
                        }

                        seen = "nothing was shown";
                    });

                    typeof(SettingsWindow).GetMethod("ApplyDefaultToAll_Click", BindingFlags.Instance | BindingFlags.NonPublic)!
                        .Invoke(settings, new object?[] { null, new RoutedEventArgs() });
                    return seen ?? "the probe did not run while the click was up";
                }
                finally
                {
                    /* Closing an unsaved window reverts the live theme preview through the app's theme loader, which the test host has no theme for. */
                    typeof(SettingsWindow).GetField("_saved", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(settings, true);
                    settings.Close();
                }
            }));

            Assert.Equal("in-app window 'Apply Default': All servers are already using the default schedule.", seen);
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
