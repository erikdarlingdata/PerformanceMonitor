/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The column filter popup's heading names the column the way its header does. Alert History's Server column is bound to
/// <c>ServerName</c>, and the filter button carries that property name; the popup that opens from it must read "Filter: Server",
/// not "Filter: ServerName". The real tab is rendered and the real button is clicked.
/// </summary>
public sealed class FilterPopupHeaderTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void TheAlertHistoryServerFilter_HeadsItsPopupWithTheColumnHeader()
    {
        var heading = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
        {
            var tab = new AlertsHistoryTab();
            var window = new Window { Content = tab, Width = 1000, Height = 600, Left = 0, Top = 0 };
            DialogWalkTestHost.Show(window);
            var button = ClippedTextProbe.Descendants(tab).OfType<Button>().Single(b => b.Tag is string tag && tag == "ServerName");
            button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

            var popup = (ColumnFilterPopup)typeof(AlertsHistoryTab).GetField("_filterPopupContent", Instance)!.GetValue(tab)!;
            var text = ((TextBlock)popup.FindName("HeaderText")).Text;
            ((Popup)typeof(AlertsHistoryTab).GetField("_filterPopup", Instance)!.GetValue(tab)!).IsOpen = false;
            window.Close();
            return text;
        }));

        Assert.Equal("Filter: Server", heading);
    }
}
