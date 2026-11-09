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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Settings window's routine notice is the app's own message window, drawn from its theme, not a native message box in
/// the system theme. Its one such notice is the signing key's "save the URL first", reached by typing a webhook URL and asking for
/// a key before saving: the window the click opens is read while it is up and closed from the dialog's own message loop.
/// </summary>
public sealed class ViewerSettingsNoticeInAppTests
{
    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    private static extern IntPtr FindWindow(string? className, string windowTitle);

    [DllImport("user32.dll")]
    private static extern bool PostMessage(IntPtr handle, uint message, IntPtr wParam, IntPtr lParam);

    private const uint WmClose = 0x0010;

    [Fact]
    public void ASigningKeyAskedForBeforeTheUrlIsSaved_SaysSoInTheAppsOwnWindow_NotANativeBox()
    {
        var seen = StaTestThread.Run(() => WithTheme(() =>
        {
            var store = new ViewerAppSettingsStore(Path.Combine(Path.GetTempPath(), "notice-" + Guid.NewGuid().ToString("N") + ".json"));
            var settings = new SettingsWindow(new ViewerPreferences(), store, new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"));
            StaTestThread.ShowOffScreen(settings);
            try
            {
                ((TextBox)settings.FindName("GenericWebhookUrlBox")).Text = "https://hooks.example.invalid/darling";
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

                    var native = FindWindow("#32770", "Signing key");
                    if (native == IntPtr.Zero)
                    {
                        native = FindWindow(null, "Signing key");
                    }

                    if (native != IntPtr.Zero)
                    {
                        seen = "native message box 'Signing key'";
                        PostMessage(native, WmClose, IntPtr.Zero, IntPtr.Zero);
                        return;
                    }

                    seen = "nothing was shown";
                });

                typeof(SettingsWindow).GetMethod("GenericSigningKeyButton_Click", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .Invoke(settings, new object?[] { null, new RoutedEventArgs() });
                return seen ?? "the probe did not run while the click was up";
            }
            finally
            {
                /* Closing an unsaved window reverts the live theme preview through the app's theme loader, which the test host has no theme for. */
                typeof(SettingsWindow).GetField("_themeSaved", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(settings, true);
                settings.Close();
            }
        }));

        Assert.Equal($"in-app window 'Signing key': {ViewerWebhookSigningKeys.SaveUrlFirstText}", seen);
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
