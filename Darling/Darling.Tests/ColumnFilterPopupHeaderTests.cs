/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows.Controls;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared column filter popup heads itself with the column's header text when it is given one, and with the bound property
/// name only when it is not. The header text comes from the panel the filter button sits in, beside its TextBlock.
/// </summary>
public sealed class ColumnFilterPopupHeaderTests
{
    [Fact]
    public void TheHeading_UsesTheColumnHeader_WhenGivenOne()
    {
        var (withHeader, withoutHeader) = StaTestThread.Run(() =>
        {
            var popup = new ColumnFilterPopup();
            popup.Initialize("ServerName", null, columnHeader: "Server");
            var first = ((TextBlock)popup.FindName("HeaderText")).Text;
            popup.Initialize("ServerName", null);
            return (first, ((TextBlock)popup.FindName("HeaderText")).Text);
        });

        Assert.Equal("Filter: Server", withHeader);
        Assert.Equal("Filter: ServerName", withoutHeader);
    }

    [Fact]
    public void TheColumnHeader_IsTheTextBesideTheFilterButton_OrNullWhenThereIsNone()
    {
        var (beside, alone) = StaTestThread.Run(() =>
        {
            var button = new Button { Tag = "ServerName" };
            var panel = new StackPanel { Orientation = Orientation.Horizontal, Children = { button, new TextBlock { Text = " Server " } } };
            var bare = new Button { Tag = "MetricName" };
            return (ColumnFilterPopup.ColumnHeaderFor(button), ColumnFilterPopup.ColumnHeaderFor(bare));
        });

        Assert.Equal("Server", beside);
        Assert.Null(alone);
    }
}
