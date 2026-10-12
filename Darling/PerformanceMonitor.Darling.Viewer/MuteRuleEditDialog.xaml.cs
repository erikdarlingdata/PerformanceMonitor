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
/// The viewer's Add/Edit mute-rule editor — Lite's <c>MuteRuleDialog</c>
/// (Lite/Windows/MuteRuleDialog.xaml.cs) ported field-for-field, including the metric-driven
/// pattern-field show/hide and the "no filters = mute all" confirmation, dark-styled for the
/// viewer. On Save it composes the edited <see cref="MuteRule"/>; the caller persists it straight
/// to Postgres (config_mute_rules). New rules default to the operator's
/// <see cref="ViewerAppSettings.MuteRuleDefaultExpiration"/> preference (via <see cref="DefaultExpiration"/>),
/// mirroring Lite's <c>App.MuteRuleDefaultExpiration</c> / the Dashboard's <c>MuteRuleDialog.DefaultExpiration</c>.
/// </summary>
public partial class MuteRuleEditDialog : Window
{
    /// <summary>
    /// The default expiration selection for NEW rules ("1 hour" / "24 hours" / "7 days" / "Never"), seeded from
    /// <see cref="ViewerAppSettings.MuteRuleDefaultExpiration"/> by <see cref="MainWindow"/> at startup and after
    /// the Settings window saves — the viewer's static twin of the Dashboard's <c>MuteRuleDialog.DefaultExpiration</c>
    /// (the viewer loads its app settings per-window, so a process-wide static carries the preference to every dialog).
    /// </summary>
    public static string DefaultExpiration { get; set; } = "24 hours";

    public MuteRule Rule { get; private set; }

    public MuteRuleEditDialog(MuteRule? existingRule = null)
    {
        InitializeComponent();

        if (existingRule != null)
        {
            Title = "Edit Mute Rule";
            HeaderText.Text = "Edit Mute Rule";
            Rule = existingRule.Clone();
            PopulateFromRule(Rule);
        }
        else
        {
            Rule = new MuteRule();
            ApplyDefaultExpiration();
        }
    }

    /// <summary>Selects the operator's default-expiration preference for a new rule; unknown values fall back to
    /// "Never (permanent)" (SelectedIndex 3), matching Lite's / the Dashboard's switch.</summary>
    private void ApplyDefaultExpiration()
    {
        ExpirationCombo.SelectedIndex = DefaultExpiration switch
        {
            "1 hour" => 0,
            "24 hours" => 1,
            "7 days" => 2,
            _ => 3
        };
    }

    /// <summary>Creates a dialog pre-populated for muting from an alert context (Lite parity).</summary>
    public MuteRuleEditDialog(AlertMuteContext context) : this()
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (!string.IsNullOrEmpty(context.ServerName))
            ServerNameBox.Text = context.ServerName;
        if (!string.IsNullOrEmpty(context.MetricName))
            SelectMetric(context.MetricName);
        if (!string.IsNullOrEmpty(context.DatabaseName))
            DatabasePatternBox.Text = context.DatabaseName;
        if (context.SeedQueryTextPattern(PerformanceMonitor.Common.SensitiveStatements.Text) is { } seededQueryText)
            QueryTextPatternBox.Text = seededQueryText;
        if (!string.IsNullOrEmpty(context.WaitType))
            WaitTypePatternBox.Text = context.WaitType;
        if (!string.IsNullOrEmpty(context.JobName))
            JobNamePatternBox.Text = context.JobName;
    }

    private void PopulateFromRule(MuteRule rule)
    {
        ReasonBox.Text = rule.Reason ?? "";
        ServerNameBox.Text = rule.ServerName ?? "";
        DatabasePatternBox.Text = rule.DatabasePattern ?? "";
        QueryTextPatternBox.Text = rule.QueryTextPattern ?? "";
        WaitTypePatternBox.Text = rule.WaitTypePattern ?? "";
        JobNamePatternBox.Text = rule.JobNamePattern ?? "";

        if (!string.IsNullOrEmpty(rule.MetricName))
            SelectMetric(rule.MetricName);

        if (rule.ExpiresAtUtc == null)
        {
            ExpirationCombo.SelectedIndex = 3;
        }
        else
        {
            var remaining = rule.ExpiresAtUtc.Value - DateTime.UtcNow;
            if (remaining.TotalHours <= 1.5) ExpirationCombo.SelectedIndex = 0;
            else if (remaining.TotalHours <= 25) ExpirationCombo.SelectedIndex = 1;
            else ExpirationCombo.SelectedIndex = 2;
        }
    }

    private void SelectMetric(string metricName)
    {
        for (int i = 0; i < MetricCombo.Items.Count; i++)
        {
            if (MetricCombo.Items[i] is ComboBoxItem item &&
                string.Equals(item.Content?.ToString(), metricName, StringComparison.OrdinalIgnoreCase))
            {
                MetricCombo.SelectedIndex = i;
                return;
            }
        }
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        Rule.Reason = string.IsNullOrWhiteSpace(ReasonBox.Text) ? null : ReasonBox.Text.Trim();
        Rule.ServerName = string.IsNullOrWhiteSpace(ServerNameBox.Text) ? null : ServerNameBox.Text.Trim();

        Rule.DatabasePattern = DatabasePatternBox.Visibility == Visibility.Visible && !string.IsNullOrWhiteSpace(DatabasePatternBox.Text)
            ? DatabasePatternBox.Text.Trim() : null;
        Rule.QueryTextPattern = QueryTextPatternBox.Visibility == Visibility.Visible && !string.IsNullOrWhiteSpace(QueryTextPatternBox.Text)
            ? QueryTextPatternBox.Text.Trim() : null;
        Rule.WaitTypePattern = WaitTypePatternBox.Visibility == Visibility.Visible && !string.IsNullOrWhiteSpace(WaitTypePatternBox.Text)
            ? WaitTypePatternBox.Text.Trim() : null;
        Rule.JobNamePattern = JobNamePatternBox.Visibility == Visibility.Visible && !string.IsNullOrWhiteSpace(JobNamePatternBox.Text)
            ? JobNamePatternBox.Text.Trim() : null;

        if (MetricCombo.SelectedIndex > 0 && MetricCombo.SelectedItem is ComboBoxItem selected)
            Rule.MetricName = selected.Content?.ToString();
        else
            Rule.MetricName = null;

        Rule.ExpiresAtUtc = ExpirationCombo.SelectedIndex switch
        {
            0 => DateTime.UtcNow.AddHours(1),
            1 => DateTime.UtcNow.AddHours(24),
            2 => DateTime.UtcNow.AddDays(7),
            _ => null
        };

        if (Rule.ServerName == null && Rule.MetricName == null && Rule.DatabasePattern == null
            && Rule.QueryTextPattern == null && Rule.WaitTypePattern == null && Rule.JobNamePattern == null)
        {
            var result = MessageBox.Show(
                "This rule has no filters and will mute ALL alerts. Are you sure?",
                "Mute All Alerts", MessageBoxButton.YesNo, MessageBoxImage.Warning);
            if (result != MessageBoxResult.Yes) return;
        }

        DialogResult = true;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;

    private void MetricCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (DatabasePatternBox == null) return;

        var metric = (MetricCombo.SelectedItem as ComboBoxItem)?.Content?.ToString();

        /* #1839's "Blocking Wait Time" carries the same blocked-process context as "Blocking Detected",
           so it offers the same database/query-text pattern fields. */
        bool showDatabase = metric is null or "(any)" or "Blocking Detected" or "Blocking Wait Time" or "Deadlocks Detected" or "Long-Running Query";
        bool showWaitType = metric is null or "(any)" or "Poison Wait" or "Long-Running Query";
        bool showQueryText = metric is null or "(any)" or "Blocking Detected" or "Blocking Wait Time" or "Long-Running Query";
        bool showJobName = metric is null or "(any)" or "Long-Running Job";

        DatabaseLabel.Visibility = DatabasePatternBox.Visibility = showDatabase ? Visibility.Visible : Visibility.Collapsed;
        WaitTypeLabel.Visibility = WaitTypePatternBox.Visibility = showWaitType ? Visibility.Visible : Visibility.Collapsed;
        QueryTextLabel.Visibility = QueryTextPatternBox.Visibility = showQueryText ? Visibility.Visible : Visibility.Collapsed;
        JobNameLabel.Visibility = JobNamePatternBox.Visibility = showJobName ? Visibility.Visible : Visibility.Collapsed;

        PatternFieldsGrid.Visibility = (showDatabase || showWaitType || showQueryText || showJobName)
            ? Visibility.Visible : Visibility.Collapsed;
    }
}
