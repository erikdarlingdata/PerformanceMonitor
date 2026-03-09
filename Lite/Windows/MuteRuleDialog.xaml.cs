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
            PopulateFromRule(existingRule);
            Rule = existingRule;
        }
        else
        {
            Rule = new MuteRule();
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
        if (!string.IsNullOrEmpty(context.QueryText))
            QueryTextPatternBox.Text = context.QueryText.Length > 200
                ? context.QueryText.Substring(0, 200)
                : context.QueryText;
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
            ExpirationCombo.SelectedIndex = 3;
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
        Rule.DatabasePattern = string.IsNullOrWhiteSpace(DatabasePatternBox.Text) ? null : DatabasePatternBox.Text.Trim();
        Rule.QueryTextPattern = string.IsNullOrWhiteSpace(QueryTextPatternBox.Text) ? null : QueryTextPatternBox.Text.Trim();
        Rule.WaitTypePattern = string.IsNullOrWhiteSpace(WaitTypePatternBox.Text) ? null : WaitTypePatternBox.Text.Trim();
        Rule.JobNamePattern = string.IsNullOrWhiteSpace(JobNamePatternBox.Text) ? null : JobNamePatternBox.Text.Trim();

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

        DialogResult = true;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e)
    {
        DialogResult = false;
    }
}
