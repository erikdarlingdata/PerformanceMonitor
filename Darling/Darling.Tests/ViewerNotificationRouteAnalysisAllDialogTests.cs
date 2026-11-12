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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7999: the Viewer's edit dialog offers "Every analysis finding (Analysis: *)" at every scope, opens a saved
/// <c>Analysis: *</c> route on that item, and saves the canonical text from the item or from a typed spelling.
/// Drives the real dialog on an STA thread, as the report-notice tests do.
/// </summary>
public sealed class ViewerNotificationRouteAnalysisAllDialogTests
{
    private static void Save(NotificationRouteEditDialog dialog)
    {
        var save = typeof(NotificationRouteEditDialog).GetMethod("Save_Click", BindingFlags.Instance | BindingFlags.NonPublic)!;
        try
        {
            save.Invoke(dialog, new object?[] { null, new RoutedEventArgs() });
        }
        catch (TargetInvocationException ex) when (ex.InnerException is InvalidOperationException)
        {
            /* A dialog that was never shown cannot take a DialogResult: that is the last statement of a valid save. */
        }

        Assert.Equal(Visibility.Collapsed, ((TextBlock)dialog.FindName("ValidationText")).Visibility);
    }

    private static ComboBox Match(NotificationRouteEditDialog dialog) => (ComboBox)dialog.FindName("FamilyCombo");

    [Fact]
    public void TheDialog_OffersTheChoiceAtAllServers_OpensASavedRouteOnIt_AndSavesTheCanonicalText()
    {
        StaTestThread.Run(() =>
        {
            /* (a) a new route at All servers offers the item, and the Every metric item is not offered there. */
            var fresh = new NotificationRouteEditDialog(null, null);
            var labels = Match(fresh).Items.Cast<ComboBoxItem>().Select(i => (string)i.Content).ToArray();
            Assert.Contains(NotificationRouteKinds.AnalysisAllChoiceLabel, labels);
            Assert.DoesNotContain(ViewerNotificationRouteScope.WildcardChoiceLabel, labels);

            /* (b) a saved Analysis: * route opens with that item selected, and (c) saves the same text. */
            var stored = new NotificationRouteRow { RouteId = 5, MetricMatch = "Analysis: *", SmtpRecipients = "a@example.invalid" };
            var opened = new NotificationRouteEditDialog(null, stored);
            Assert.Equal(NotificationRoute.AnalysisAllMatch, ((ComboBoxItem)Match(opened).SelectedItem).Tag);
            Save(opened);
            Assert.Equal("Analysis: *", opened.Route.MetricMatch);

            /* (d) choosing Exact metric name and typing a spelling of it saves the canonical text. */
            var typed = new NotificationRouteEditDialog(null, new NotificationRouteRow { RouteId = 6, MetricMatch = "Blocking", SmtpRecipients = "a@example.invalid" });
            Match(typed).SelectedItem = Match(typed).Items.Cast<ComboBoxItem>().Single(i => i.Tag is null);
            ((TextBox)typed.FindName("ExactMetricBox")).Text = "analysis:  *";
            Save(typed);
            Assert.Equal("Analysis: *", typed.Route.MetricMatch);

            /* (e) the main path: a family route switched to the item saves Analysis: *, so (c) is not just a value carried through. */
            var switched = new NotificationRouteEditDialog(null, new NotificationRouteRow { RouteId = 7, MetricMatch = AlertFamily.Performance, SmtpRecipients = "a@example.invalid" });
            Match(switched).SelectedItem = Match(switched).Items.Cast<ComboBoxItem>().Single(i => Equals(i.Tag, NotificationRoute.AnalysisAllMatch));
            Save(switched);
            Assert.Equal("Analysis: *", switched.Route.MetricMatch);
        });
    }
}
