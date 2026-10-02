/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Add/Edit one notification route (#3598). The match combo is filled from <see cref="AlertFamily.All"/> plus
/// one "Exact metric name…" entry, so the family list on screen IS the closed taxonomy the resolver matches
/// against and cannot drift from it. Validation is the store's own
/// (<see cref="ViewerDataService.ValidateNotificationRoute"/>), so a route this dialog accepts is one the
/// store accepts.
/// </summary>
public partial class NotificationRouteEditDialog : Window
{
    private const string ExactMetricChoice = "Exact metric name…";

    /// <summary>The route as edited — the caller persists it.</summary>
    public NotificationRouteRow Route { get; private set; }

    public NotificationRouteEditDialog(NotificationRouteRow? existing = null)
    {
        InitializeComponent();

        foreach (var name in AlertFamily.All)
        {
            FamilyCombo.Items.Add(new ComboBoxItem { Content = name, Tag = name });
        }

        FamilyCombo.Items.Add(new ComboBoxItem { Content = ExactMetricChoice, Tag = null });

        if (existing is null)
        {
            Route = new NotificationRouteRow();
            FamilyCombo.SelectedIndex = AlertFamily.All.Count - 1; /* performance — the pages family, the likeliest first route */
            return;
        }

        Route = existing.Clone();
        Title = "Edit Notification Route";
        HeaderText.Text = "Edit Notification Route";

        var family = Route.Family;
        if (family is null)
        {
            FamilyCombo.SelectedIndex = FamilyCombo.Items.Count - 1;
            ExactMetricBox.Text = Route.MetricMatch;
        }
        else
        {
            FamilyCombo.SelectedIndex = FamilyIndex(family);
        }

        TeamsUrlBox.Text = Route.TeamsUrl;
        SlackUrlBox.Text = Route.SlackUrl;
        GenericUrlBox.Text = Route.GenericUrl;
        PagerDutyKeyBox.Text = Route.PagerDutyRoutingKey;
        SmtpRecipientsBox.Text = Route.SmtpRecipients;
        EnabledCheckBox.IsChecked = Route.Enabled;
    }

    private void FamilyCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        var isExact = SelectedFamily() is null;
        ExactMetricBox.IsEnabled = isExact;
        FamilyDescriptionText.Text = SelectedFamily() switch
        {
            AlertFamily.SelfMonitor => "Alerts about the monitoring tool itself: its store, collectors, jobs and certificates (Compression Job Stuck, Collection Stopped, Store Disk Pressure, Retention Held, …).",
            AlertFamily.Reports => "Scheduled prose — the Collector Cost Digest, the Fleet Sweep Rollup and the Analysis Singles Digest. Reports to read, not pages.",
            AlertFamily.AgentJobs => "SQL Server Agent: Failed Agent Job, Long-Running Job, Agent Not Running.",
            AlertFamily.Performance => "A monitored server's health: blocking, deadlocks, CPU, long-running queries, space, availability groups, connection loss, custom rules and analysis findings — and any alert the taxonomy does not name.",
            _ => "One alert by its exact metric_name (as shown in Alert History). Wins over the family route; a recovery (Server Restored, AG Replica Reconnected) routes as the alert it clears.",
        };
    }

    private static int FamilyIndex(string family)
    {
        for (var i = 0; i < AlertFamily.All.Count; i++)
        {
            if (string.Equals(AlertFamily.All[i], family, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return 0;
    }

    private string? SelectedFamily() => (FamilyCombo.SelectedItem as ComboBoxItem)?.Tag as string;

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        Route.MetricMatch = SelectedFamily() ?? ExactMetricBox.Text.Trim();
        Route.TeamsUrl = TeamsUrlBox.Text.Trim();
        Route.SlackUrl = SlackUrlBox.Text.Trim();
        Route.GenericUrl = GenericUrlBox.Text.Trim();
        Route.PagerDutyRoutingKey = PagerDutyKeyBox.Text.Trim();
        Route.SmtpRecipients = SmtpRecipientsBox.Text.Trim();
        Route.Enabled = EnabledCheckBox.IsChecked == true;

        var error = ViewerDataService.ValidateNotificationRoute(Route);
        if (error is not null)
        {
            MessageBox.Show(error, "Notification Route", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        /* An exact name that is really a family spelled by hand is normalised to the family, so the grid and
           the resolver agree about what kind of route this is. */
        if (AlertFamily.NormalizeFamily(Route.MetricMatch) is { } family)
        {
            Route.MetricMatch = family;
        }

        DialogResult = true;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;
}
