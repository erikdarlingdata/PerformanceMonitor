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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// Add or edit one notification route (#5750). The dialog saves through the delegate it is given (the store's insert or
/// update) so the store's own validation, an <see cref="ArgumentException"/> for "both scopes" or "* without a scope",
/// shows inline and the dialog stays open. A saved destination is never put in a box: the box starts with a fixed mask,
/// and <see cref="NotificationRouteUi.ResolveSecret"/> turns the box back into keep, replace or remove.
/// </summary>
public partial class NotificationRouteEditDialog : Window
{
    private const string RestoreTable = "notification_routes";
    private const string ExactMetricChoice = "Exact metric name...";
    private const string WildcardChoice = "Every metric (*)";

    private readonly Func<NotificationRoute, Task<bool>> _save;
    private readonly NotificationRoute? _stored;
    private readonly List<RouteScopeOption> _scopeOptions;
    private readonly ComboBoxItem _wildcardItem = new() { Content = WildcardChoice, Tag = NotificationRoute.WildcardMatch };
    private readonly bool _built;

    internal NotificationRouteEditDialog(
        IReadOnlyList<RouteScopeOption> scopeOptions,
        Func<NotificationRoute, Task<bool>> save,
        NotificationRoute? existing = null)
    {
        InitializeComponent();
        _save = save;
        _stored = existing;
        _scopeOptions = scopeOptions.ToList();
        if (_scopeOptions.Count == 0)
        {
            _scopeOptions.Add(new RouteScopeOption(RouteScopeKind.AllServers, null, NotificationRouteUi.AllServersLabel));
        }

        foreach (var name in AlertFamily.All)
        {
            MatchCombo.Items.Add(new ComboBoxItem { Content = name, Tag = name });
        }

        MatchCombo.Items.Add(new ComboBoxItem { Content = ExactMetricChoice, Tag = null });

        var selectedScope = existing is null ? _scopeOptions[0] : NotificationRouteUi.OptionFor(existing, _scopeOptions);
        foreach (var option in _scopeOptions)
        {
            ScopeCombo.Items.Add(option);
        }

        _built = true;
        ScopeCombo.SelectedItem = selectedScope;

        if (existing is null)
        {
            MatchCombo.SelectedIndex = AlertFamily.All.ToList().IndexOf(AlertFamily.Performance);
            return;
        }

        Title = "Edit Notification Route";
        HeaderText.Text = "Edit Notification Route";

        if (existing.IsWildcard)
        {
            SelectWildcard();
        }
        else if (existing.Family is { } family)
        {
            MatchCombo.SelectedIndex = AlertFamily.All.ToList().IndexOf(family);
        }
        else
        {
            MatchCombo.SelectedIndex = MatchCombo.Items.IndexOf(MatchCombo.Items.OfType<ComboBoxItem>().First(i => i.Tag is null));
            ExactMetricBox.Text = existing.MetricMatch;
        }

        /* The saved destinations are never shown: a mask where one is saved, nothing where none is. */
        InitSecretBox(TeamsUrlBox, existing.TeamsUrl, existing.IsOffFor(NotificationRouter.TeamsChannel));
        InitSecretBox(SlackUrlBox, existing.SlackUrl, existing.IsOffFor(NotificationRouter.SlackChannel));
        InitSecretBox(GenericUrlBox, existing.GenericUrl, existing.IsOffFor(NotificationRouter.GenericChannel));
        InitSecretBox(PagerDutyKeyBox, existing.PagerDutyRoutingKey, existing.IsOffFor(NotificationRouter.PagerDutyChannel));
        SmtpRecipientsBox.Text = existing.SmtpRecipients;
        EnabledCheckBox.IsChecked = existing.Enabled;
    }

    private static void InitSecretBox(PasswordBox box, string stored, bool unreadable)
    {
        box.Password = NotificationRouteUi.SecretBoxInitialText(stored);
        if (!string.IsNullOrWhiteSpace(stored))
        {
            box.ToolTip = "A destination is saved. Leave the dots to keep it, type to replace it, or clear the box to remove it.";
        }
        else if (unreadable)
        {
            box.ToolTip = "A destination was saved but Windows Credential Manager could not return it, so this channel is off for the route. Type a new value to replace it; saving with the box empty removes it.";
        }
    }

    /// <summary>Typing over the mask replaces it: select it all when the box is entered.</summary>
    private void SecretBox_GotKeyboardFocus(object sender, System.Windows.Input.KeyboardFocusChangedEventArgs e)
    {
        if (sender is PasswordBox box && box.Password == NotificationRouteUi.KeepMask)
        {
            box.SelectAll();
        }
    }

    private RouteScopeOption? SelectedScope() => ScopeCombo.SelectedItem as RouteScopeOption;

    private string? SelectedMatchTag() => (MatchCombo.SelectedItem as ComboBoxItem)?.Tag as string;

    private void SelectWildcard()
    {
        if (!MatchCombo.Items.Contains(_wildcardItem))
        {
            MatchCombo.Items.Insert(MatchCombo.Items.Count - 1, _wildcardItem);
        }

        MatchCombo.SelectedItem = _wildcardItem;
    }

    /// <summary>The every-metric match is offered only while a server or tag is chosen (ruling 4). Switching back to All
    /// servers removes it, and moves a selection that was on it to the Performance family.</summary>
    private void ScopeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!_built)
        {
            return;
        }

        var offered = NotificationRouteUi.WildcardOffered(SelectedScope());
        var present = MatchCombo.Items.Contains(_wildcardItem);
        if (offered && !present)
        {
            MatchCombo.Items.Insert(MatchCombo.Items.Count - 1, _wildcardItem);
        }
        else if (!offered && present)
        {
            var wasSelected = ReferenceEquals(MatchCombo.SelectedItem, _wildcardItem);
            MatchCombo.Items.Remove(_wildcardItem);
            if (wasSelected)
            {
                MatchCombo.SelectedIndex = AlertFamily.All.ToList().IndexOf(AlertFamily.Performance);
            }
        }
    }

    private void MatchCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!_built)
        {
            return;
        }

        var tag = SelectedMatchTag();
        ExactMetricBox.IsEnabled = MatchCombo.SelectedItem is ComboBoxItem { Tag: null };
        MatchDescriptionText.Text = tag switch
        {
            AlertFamily.SelfMonitor => "Alerts about the monitoring tool itself.",
            AlertFamily.Reports => "Scheduled reports and digests.",
            AlertFamily.AgentJobs => "SQL Server Agent: failed or long-running jobs.",
            AlertFamily.Performance => "A monitored server's health: blocking, deadlocks, CPU, long-running queries, space, connection loss, and any alert no other family names.",
            AlertFamily.External => "Alerts sent in from outside the collectors.",
            NotificationRoute.WildcardMatch => "Every alert for the chosen server or tag. The lowest-priority match: an exact metric or a family route for the same scope wins over it.",
            _ => "One alert by its exact metric name. Wins over a family or 'Every metric'; a recovery (Server Restored) routes as the alert it clears."
        };
    }

    private NotificationRoute BuildRoute()
    {
        var (serverId, tagId) = NotificationRouteUi.ScopeIds(SelectedScope());
        var match = SelectedMatchTag() ?? ExactMetricBox.Text.Trim();

        /* An exact name that is really a family spelled by hand becomes the family, so the list and the router agree. */
        if (SelectedMatchTag() is null && AlertFamily.NormalizeFamily(match) is { } family)
        {
            match = family;
        }

        return new NotificationRoute(
            _stored?.RouteId ?? 0,
            match,
            NotificationRouteUi.ResolveSecret(TeamsUrlBox.Password, _stored?.TeamsUrl),
            NotificationRouteUi.ResolveSecret(SlackUrlBox.Password, _stored?.SlackUrl),
            NotificationRouteUi.ResolveSecret(GenericUrlBox.Password, _stored?.GenericUrl),
            NotificationRouteUi.ResolveSecret(PagerDutyKeyBox.Password, _stored?.PagerDutyRoutingKey),
            SmtpRecipientsBox.Text.Trim(),
            EnabledCheckBox.IsChecked == true)
        {
            ScopeServerId = serverId,
            ScopeTagId = tagId
        };
    }

    private async void Save_Click(object sender, RoutedEventArgs e)
    {
        ErrorText.Visibility = Visibility.Collapsed;

        /* A restore can start while the dialog is open: refuse here, before the write, and say why. */
        if (PendingRestoreNotice.Refuse(RestoreTable, this))
        {
            return;
        }

        SaveButton.IsEnabled = false;
        try
        {
            if (!await _save(BuildRoute()))
            {
                ShowInline("This route no longer exists. Close this dialog and reopen the list.");
                return;
            }

            DialogResult = true;
        }
        catch (ArgumentException ex)
        {
            ShowInline(NotificationRouteUi.InlineMessage(ex));
        }
        catch (PendingRestoreException ex)
        {
            ShowInline(ex.Message);
        }
        catch (Exception ex)
        {
            ShowInline($"Could not save the route: {ex.Message}");
        }
        finally
        {
            SaveButton.IsEnabled = true;
        }
    }

    private void ShowInline(string message)
    {
        ErrorText.Text = message;
        ErrorText.Visibility = Visibility.Visible;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;
}
