/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Darling.Tests;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7999: Lite's edit dialog offers "Every analysis finding (Analysis: *)" at every scope, opens a saved
/// <c>Analysis: *</c> route on that item, and saves the canonical text from the item or from a typed spelling.
/// Drives the real dialog on an STA thread and reads what <c>BuildRoute</c> would save.
/// </summary>
public sealed class NotificationRouteAnalysisAllDialogTests
{
    private static NotificationRouteEditDialog Dialog(NotificationRoute? existing = null) =>
        new(NotificationRouteUi.ScopeOptions(new[] { (7, "Alpha") }, new List<ServerTag>()), _ => Task.FromResult(true), existing);

    private static NotificationRoute Build(NotificationRouteEditDialog dialog) =>
        (NotificationRoute)typeof(NotificationRouteEditDialog).GetMethod("BuildRoute", BindingFlags.Instance | BindingFlags.NonPublic, Type.EmptyTypes)!
            .Invoke(dialog, null)!;

    /* The dialog's XAML needs the theme's styles; the same loader the other Lite window tests use. */
    private static void WithTheme(Action body)
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
            body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }

    private static ComboBox Match(NotificationRouteEditDialog dialog) => (ComboBox)dialog.FindName("MatchCombo");

    [Fact]
    public void TheDialog_OffersTheChoiceAtAllServers_OpensASavedRouteOnIt_AndSavesTheCanonicalText()
    {
        StaTestThread.Run(() => WithTheme(() =>
        {
            /* (a) a new route at All servers offers the item, and the Every metric item is not offered there. */
            var fresh = Dialog();
            var labels = Match(fresh).Items.Cast<ComboBoxItem>().Select(i => (string)i.Content).ToArray();
            Assert.Contains(NotificationRouteKinds.AnalysisAllChoiceLabel, labels);
            Assert.DoesNotContain("Every metric (*)", labels);

            /* (b) a saved Analysis: * route opens with that item selected, and (c) saves the same text. */
            var saved = new NotificationRoute(3, "Analysis: *", "", "https://hooks.example.test/x", "", "", "", true);
            var opened = Dialog(saved);
            Assert.Equal(NotificationRoute.AnalysisAllMatch, ((ComboBoxItem)Match(opened).SelectedItem).Tag);
            Assert.Equal("Analysis: *", Build(opened).MetricMatch);

            /* (d) choosing Exact metric name and typing a spelling of it saves the canonical text. */
            var typed = Dialog();
            Match(typed).SelectedItem = Match(typed).Items.Cast<ComboBoxItem>().Single(i => i.Tag is null);
            ((TextBox)typed.FindName("ExactMetricBox")).Text = "analysis:  *";
            Assert.Equal("Analysis: *", Build(typed).MetricMatch);
        }));
    }
}
