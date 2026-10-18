/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Reflection;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5912 review round 1: <c>agent_level</c> 2 ("Act") is a live route level (the router resolves it, V191 stores it, an operator
/// sets it in SQL), so every reader in the viewer names it. Pinned by behaviour: the grid's Agent and Channels columns say
/// Act and not Off, the edit dialog constructs on a Level 2 row (it used to throw from <c>.First(...)</c> on the combo) and
/// keeps the level on save instead of saving the row as another level, and the save check accepts it.
/// </summary>
public sealed class ViewerNotificationRouteActTests
{
    private static NotificationRouteRow ActRow() => new()
    {
        RouteId = 5,
        MetricMatch = "Blocking",
        AgentLevel = NotificationRoute.AgentLevelAct,
        AgentMaxPerHour = 7,
        AgentMaxPerDay = 70,
    };

    [Theory]
    [InlineData(null, "Inherit", "agent off")]
    [InlineData(0, "Off", "agent off")]
    [InlineData(1, "Investigate (7/h, 70/day)", "agent")]
    [InlineData(2, "Act (7/h, 70/day)", "agent")]
    [InlineData(3, "Act on servers (7/h, 70/day)", "agent")]
    public void TheAgentAndChannelsColumns_NameEveryLevel_AndActIsNotOff(int? level, string agent, string channels)
    {
        var row = ActRow();
        row.AgentLevel = level;
        Assert.Equal(agent, row.AgentDisplay);
        if (level is not null)
            Assert.Equal(channels, row.ChannelsDisplay);
    }

    [Fact]
    public void TheSaveCheck_AcceptsAct_AndStillRefusesALevelOutsideTheRange()
    {
        var row = ActRow();
        Assert.Null(ViewerDataService.ValidateNotificationRoute(row));
        row.AgentLevel = 4;
        Assert.Equal("The agent channel is Inherit, Off, Investigate, Act or Act on servers.", ViewerDataService.ValidateNotificationRoute(row));
    }

    [Fact]
    public void ALevelTwoRow_OpensTheEditDialog_ShowsAct_AndKeepsTheLevelOnSave()
    {
        StaTestThread.Run(() =>
        {
            var dialog = new NotificationRouteEditDialog(null, ActRow());

            var combo = (ComboBox)dialog.FindName("AgentLevelCombo");
            var shown = Assert.IsType<ComboBoxItem>(combo.SelectedItem);
            Assert.Equal("Act (Level 2: investigate and change alert settings)", shown.Content);
            Assert.True(shown.IsEnabled, "Level 2 is offered since #5915: Act can be picked from the dialog");

            /* Untouched, the dialog reads the level back as 2: it does not save a Level 2 row as Inherit, Off or Investigate. */
            var error = (string?)typeof(NotificationRouteEditDialog)
                .GetMethod("ReadAgent", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(dialog, null);
            Assert.Null(error);
            Assert.Equal(NotificationRoute.AgentLevelAct, dialog.Route.AgentLevel);
            Assert.True(((TextBox)dialog.FindName("AgentMaxPerHourBox")).IsEnabled, "Act is limited like Investigate, so its limits stay editable");
        });
    }
}
