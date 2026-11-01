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

    /// <summary>The schedule fields, Monday first, one box per day. Each box's tag is its <see cref="DayOfWeek"/>.</summary>
    private readonly List<CheckBox> _dayBoxes = new();

    /// <summary>Whether the store keeps a recurring schedule (V231, #7040). False hides the fields, and the rule is saved with none.</summary>
    private readonly bool _scheduleSupported;

    public MuteRuleEditDialog(MuteRule? existingRule = null, bool scheduleSupported = true)
    {
        InitializeComponent();
        _scheduleSupported = scheduleSupported;
        BuildScheduleSection();

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
    public MuteRuleEditDialog(AlertMuteContext context, bool scheduleSupported = true) : this((MuteRule?)null, scheduleSupported)
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
        if (!string.IsNullOrEmpty(context.ClientAppName))
            ClientAppPatternBox.Text = context.ClientAppName;
    }

    private static readonly DayOfWeek[] DisplayDays =
        [DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday, DayOfWeek.Saturday, DayOfWeek.Sunday];

    private void BuildScheduleSection()
    {
        if (!_scheduleSupported)
        {
            ScheduleSection.Visibility = Visibility.Collapsed;
            return;
        }

        foreach (var day in DisplayDays)
        {
            var box = new CheckBox
            {
                Content = MuteRecurrence.DayNames(MuteRecurrence.Bit(day))[0],
                Tag = day,
                Margin = new Thickness(0, 0, 12, 0),
            };
            _dayBoxes.Add(box);
            DaysPanel.Children.Add(box);
        }

        /* The zone starts as this computer's, as an IANA id, the same default the service gives a schedule with no zone. */
        RecurZoneBox.Text = MuteRecurrence.HostZoneIana();
    }

    private void PopulateSchedule(MuteRule rule)
    {
        ActionBox.SelectedIndex = rule.IsDigestAction ? 1 : 0;

        if (!_scheduleSupported || !rule.RecurStartTime.HasValue || !rule.RecurEndTime.HasValue)
        {
            return;
        }

        RecurStartBox.Text = MuteRecurrence.FormatTime(rule.RecurStartTime.Value);
        RecurEndBox.Text = MuteRecurrence.FormatTime(rule.RecurEndTime.Value);
        if (!string.IsNullOrEmpty(rule.RecurTimeZone))
        {
            RecurZoneBox.Text = rule.RecurTimeZone;
        }

        /* A null mask is every day, which the form says as no day ticked. */
        if (rule.RecurDays.HasValue)
        {
            foreach (var box in _dayBoxes)
            {
                box.IsChecked = MuteRecurrence.IncludesDay(rule.RecurDays, (DayOfWeek)box.Tag);
            }
        }
    }

    /// <summary>
    /// Reads the schedule fields into <paramref name="rule"/> and runs the shared check. Returns the refusal to show, or null when the
    /// fields are valid. Blank times with no day ticked is a one-off rule, so the zone the box starts with never makes one look scheduled.
    /// </summary>
    internal static string? ApplySchedule(MuteRule rule, IEnumerable<DayOfWeek> checkedDays, string? startText, string? endText, string? zoneText)
    {
        var mask = 0;
        foreach (var day in checkedDays)
        {
            mask |= MuteRecurrence.Bit(day);
        }

        var hasStart = !string.IsNullOrWhiteSpace(startText);
        var hasEnd = !string.IsNullOrWhiteSpace(endText);
        TimeOnly start = default, end = default;
        if (hasStart && !MuteRecurrence.TryParseTime(startText, out start))
        {
            return "Enter the start time as HH:mm, for example 22:00.";
        }

        if (hasEnd && !MuteRecurrence.TryParseTime(endText, out end))
        {
            return "Enter the end time as HH:mm, for example 02:00.";
        }

        rule.RecurStartTime = hasStart ? start : null;
        rule.RecurEndTime = hasEnd ? end : null;
        /* No day ticked is every day. Days ticked with no times reach the check below, which refuses them. */
        rule.RecurDays = mask == 0 ? null : mask;
        /* A one-off rule has no zone, even though the box starts with the computer's. */
        rule.RecurTimeZone = (hasStart || hasEnd) && !string.IsNullOrWhiteSpace(zoneText) ? zoneText.Trim() : null;

        return rule.TryNormalizeRecurrence(out var error) ? null : error;
    }

    private void PopulateFromRule(MuteRule rule)
    {
        PopulateSchedule(rule);
        ReasonBox.Text = rule.Reason ?? "";
        ServerNameBox.Text = rule.ServerName ?? "";
        DatabasePatternBox.Text = rule.DatabasePattern ?? "";
        QueryTextPatternBox.Text = rule.QueryTextPattern ?? "";
        WaitTypePatternBox.Text = rule.WaitTypePattern ?? "";
        JobNamePatternBox.Text = rule.JobNamePattern ?? "";
        ClientAppPatternBox.Text = rule.ClientAppPattern ?? "";

        if (!string.IsNullOrEmpty(rule.MetricName))
            SelectMetric(rule.MetricName);

        /* #6231: a rule that already has a start keeps it unless the operator picks another: the first entry stands for the stored
           start, so an edit that never touches the combo cannot move or drop it. */
        if (rule.StartsAtUtc is { } storedStart)
        {
            _keptStart = storedStart;
            StartCombo.Items.Insert(0, new ComboBoxItem { Content = $"Keep: {storedStart.ToLocalTime():g}" });
            StartCombo.SelectedIndex = 0;
        }

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
        Rule.Action = ActionBox.SelectedIndex == 1 ? MuteRuleActions.Digest : MuteRuleActions.Mute;
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

        Rule.ExpiresAtUtc = ExpirationCombo.SelectedIndex switch
        {
            0 => DateTime.UtcNow.AddHours(1),
            1 => DateTime.UtcNow.AddHours(24),
            2 => DateTime.UtcNow.AddDays(7),
            _ => null
        };

        Rule.StartsAtUtc = StartFromSelection(StartCombo.SelectedIndex, _keptStart, DateTime.UtcNow);
        if (WindowRefusal(Rule.StartsAtUtc, Rule.ExpiresAtUtc) is { } windowProblem)
        {
            MessageBox.Show(windowProblem, "Mute Rule", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        if (_scheduleSupported)
        {
            var scheduleProblem = ApplySchedule(Rule,
                _dayBoxes.Where(b => b.IsChecked == true).Select(b => (DayOfWeek)b.Tag),
                RecurStartBox.Text, RecurEndBox.Text, RecurZoneBox.Text);
            if (scheduleProblem is not null)
            {
                MessageBox.Show(scheduleProblem, "Mute Rule", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
        }

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

    /// <summary>The start the Starts combo stands for (#6231). When the rule being edited already had a start, entry 0 is "Keep" and
    /// the entries after it are the four offsets; otherwise entry 0 is "Now" (no start) and the entries after it are the same four.</summary>
    internal static DateTime? StartFromSelection(int selectedIndex, DateTime? keptStart, DateTime nowUtc)
    {
        var offset = keptStart.HasValue ? selectedIndex - 1 : selectedIndex;
        if (keptStart.HasValue && selectedIndex == 0)
            return keptStart;
        return offset switch
        {
            1 => nowUtc.AddHours(1),
            2 => nowUtc.AddHours(4),
            3 => nowUtc.AddHours(12),
            4 => nowUtc.AddHours(24),
            _ => null
        };
    }

    /// <summary>The refusal for an end that is not after the start, or null.</summary>
    internal static string? WindowRefusal(DateTime? startsAtUtc, DateTime? expiresAtUtc) =>
        startsAtUtc.HasValue && expiresAtUtc.HasValue && expiresAtUtc.Value <= startsAtUtc.Value
            ? "The rule would end before it starts. Pick a later 'Expires After' or an earlier start."
            : null;

    private DateTime? _keptStart;

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;

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
           same "Mute this alert" click makes the same rule here, in Lite and on the web page. */
        bool showClientApp = metric is null or "(any)" or AlertFamily.ClientTimeoutsMetric;

        DatabaseLabel.Visibility = DatabasePatternBox.Visibility = showDatabase ? Visibility.Visible : Visibility.Collapsed;
        WaitTypeLabel.Visibility = WaitTypePatternBox.Visibility = showWaitType ? Visibility.Visible : Visibility.Collapsed;
        QueryTextLabel.Visibility = QueryTextPatternBox.Visibility = showQueryText ? Visibility.Visible : Visibility.Collapsed;
        JobNameLabel.Visibility = JobNamePatternBox.Visibility = showJobName ? Visibility.Visible : Visibility.Collapsed;
        ClientAppLabel.Visibility = ClientAppPatternBox.Visibility = showClientApp ? Visibility.Visible : Visibility.Collapsed;

        PatternFieldsGrid.Visibility = (showDatabase || showWaitType || showQueryText || showJobName || showClientApp)
            ? Visibility.Visible : Visibility.Collapsed;
    }
}
