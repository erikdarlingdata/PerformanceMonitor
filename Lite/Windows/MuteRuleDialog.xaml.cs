/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Windows;

public partial class MuteRuleDialog : Window
{
    public MuteRule Rule { get; private set; }

    public MuteRuleDialog(MuteRule? existingRule = null)
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

    /// <summary>
    /// Creates a dialog pre-populated for muting from an alert context.
    /// </summary>
    public MuteRuleDialog(AlertMuteContext context) : this()
    {
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
        if (!string.IsNullOrEmpty(context.ClientAppName))
            ClientAppPatternBox.Text = context.ClientAppName;
    }

    private void PopulateFromRule(MuteRule rule)
    {
        ReasonBox.Text = rule.Reason ?? "";
        ServerNameBox.Text = rule.ServerName ?? "";
        DatabasePatternBox.Text = rule.DatabasePattern ?? "";
        QueryTextPatternBox.Text = rule.QueryTextPattern ?? "";
        WaitTypePatternBox.Text = rule.WaitTypePattern ?? "";
        JobNamePatternBox.Text = rule.JobNamePattern ?? "";
        ClientAppPatternBox.Text = rule.ClientAppPattern ?? "";

        if (!string.IsNullOrEmpty(rule.MetricName))
            SelectMetric(rule.MetricName);

        /* #7173: a rule that already expires keeps that moment unless the operator picks another: the first entry stands for the
           stored expiry, so an edit that never touches the combo cannot stretch a window or bring an expired rule back to life. */
        if (rule.ExpiresAtUtc is { } storedExpiry)
        {
            _keptExpiry = storedExpiry;
            ExpirationCombo.Items.Insert(0, new ComboBoxItem { Content = KeepExpiryLabel(storedExpiry, DateTime.UtcNow) });
            ExpirationCombo.SelectedIndex = 0;
        }
        else
        {
            ExpirationCombo.SelectedIndex = 3;
        }
    }

    private DateTime? _keptExpiry;

    /// <summary>The expiry the Expires After combo stands for (#7173). When the rule being edited already expires, entry 0 is "Keep"
    /// and the entries after it are the four presets; otherwise the entries are the four presets alone.</summary>
    internal static DateTime? ExpiryFromSelection(int selectedIndex, DateTime? keptExpiry, DateTime nowUtc)
    {
        if (keptExpiry.HasValue)
        {
            if (selectedIndex == 0)
                return keptExpiry;
            selectedIndex--;
        }
        return selectedIndex switch
        {
            0 => nowUtc.AddHours(1),
            1 => nowUtc.AddHours(24),
            2 => nowUtc.AddDays(7),
            _ => null
        };
    }

    /// <summary>The text of the "Keep" entry: the stored expiry in local time, marked when it has already passed.</summary>
    internal static string KeepExpiryLabel(DateTime expiresAtUtc, DateTime nowUtc) =>
        expiresAtUtc <= nowUtc
            ? $"Keep: {expiresAtUtc.ToLocalTime():g} (expired)"
            : $"Keep: {expiresAtUtc.ToLocalTime():g}";


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
        Rule.ClientAppPattern = ClientAppPatternBox.Visibility == Visibility.Visible && !string.IsNullOrWhiteSpace(ClientAppPatternBox.Text)
            ? ClientAppPatternBox.Text.Trim() : null;

        if (MetricCombo.SelectedIndex > 0 && MetricCombo.SelectedItem is ComboBoxItem selected)
            Rule.MetricName = selected.Content?.ToString();
        else
            Rule.MetricName = null;

        Rule.ExpiresAtUtc = ExpiryFromSelection(ExpirationCombo.SelectedIndex, _keptExpiry, DateTime.UtcNow);

        if (Rule.ServerName == null && Rule.MetricName == null && Rule.DatabasePattern == null
            && Rule.QueryTextPattern == null && Rule.WaitTypePattern == null && Rule.JobNamePattern == null
            && Rule.ClientAppPattern == null)
        {
            var result = MessageBox.Show(
                "This rule has no filters and will mute ALL alerts. Are you sure?",
                "Mute All Alerts", MessageBoxButton.YesNo, MessageBoxImage.Warning);
            if (result != MessageBoxResult.Yes) return;
        }

        DialogResult = true;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e)
    {
        DialogResult = false;
    }

    private void MetricCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (DatabasePatternBox == null) return;

        var metric = (MetricCombo.SelectedItem as ComboBoxItem)?.Content?.ToString();

        /* #1839's "Blocking Wait Time" carries the same blocked-process context as "Blocking Detected",
           so it offers the same database/query-text pattern fields. */
        bool showDatabase = metric is null or "(any)" or "Blocking Detected" or "Blocking Wait Time" or "Deadlocks Detected" or "Long-Running Query" or AlertFamily.ClientTimeoutsMetric;
        bool showWaitType = metric is null or "(any)" or "Poison Wait" or "Long-Running Query";
        bool showQueryText = metric is null or "(any)" or "Blocking Detected" or "Blocking Wait Time" or "Long-Running Query";
        bool showJobName = metric is null or "(any)" or "Long-Running Job";
        /* #5640: Client Timeouts offers Database AND Client Application, the two dimensions its alert carries, so the
           same "Mute this alert" click makes the same rule here, in the viewer and on the web page. */
        bool showClientApp = metric is null or "(any)" or AlertFamily.ClientTimeoutsMetric;

        DatabaseLabel.Visibility = DatabasePatternBox.Visibility = showDatabase ? Visibility.Visible : Visibility.Collapsed;
        WaitTypeLabel.Visibility = WaitTypePatternBox.Visibility = showWaitType ? Visibility.Visible : Visibility.Collapsed;
        QueryTextLabel.Visibility = QueryTextPatternBox.Visibility = showQueryText ? Visibility.Visible : Visibility.Collapsed;
        JobNameLabel.Visibility = JobNamePatternBox.Visibility = showJobName ? Visibility.Visible : Visibility.Collapsed;
        ClientAppLabel.Visibility = ClientAppPatternBox.Visibility = showClientApp ? Visibility.Visible : Visibility.Collapsed;

        PatternFieldsGrid.Visibility = (showDatabase || showWaitType || showQueryText || showJobName || showClientApp)
            ? Visibility.Visible : Visibility.Collapsed;
    }

    private void ApplyDefaultExpiration()
    {
        ExpirationCombo.SelectedIndex = App.MuteRuleDefaultExpiration switch
        {
            "1 hour" => 0,
            "24 hours" => 1,
            "7 days" => 2,
            _ => 3
        };
    }
}
