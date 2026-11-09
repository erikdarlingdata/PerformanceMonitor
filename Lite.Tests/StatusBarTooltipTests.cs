/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8332, Lite's side: the status bar's message carries its whole text as its tooltip, and an empty bar (what a connect attempt or a
/// cleared message leaves) opens no tooltip at all. The real main window is built on the shared STA thread with the Dark theme and
/// hosted at 900x600. The Viewer's twin is in ViewerStatusBarLinesTests.
/// </summary>
public sealed class StatusBarTooltipTests
{
    [Fact]
    public void AnEmptyBar_OpensNoTooltip_AndAFilledOneCarriesItsText()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var w = new MainWindow();
            var root = (UIElement)w.Content;
            w.Content = null;
            var host = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(host);
            try
            {
                ClippedTextProbe.Settle(host);
                var status = (TextBlock)w.FindName("StatusText");
                var found = new List<string>();
                status.Text = string.Empty;
                ClippedTextProbe.Settle(host);
                if (status.ToolTip is not null)
                {
                    found.Add($"an empty bar has the tooltip \"{status.ToolTip}\"");
                }

                status.Text = "Connected to SQL2022 - Data loaded";
                ClippedTextProbe.Settle(host);
                if (!Equals(status.ToolTip, status.Text))
                {
                    found.Add("a filled bar's tooltip is " + (status.ToolTip is null ? "unset" : "\"" + status.ToolTip + "\""));
                }

                return found;
            }
            finally
            {
                host.Close();
            }
        }));

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var d in saved)
            {
                app.Resources.MergedDictionaries.Add(d);
            }
        }
    }
}
