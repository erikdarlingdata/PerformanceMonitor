/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One line of the rule list: the store's summary joined with the rule's scope and how many servers it is firing on.</summary>
public sealed record AlertRuleRow(
    long Id, int Version, string Name, bool Enabled, string Status, string ScopeText, string ExpiryText, string StateText,
    string LastFiredText, string UpdatedBy, DateTime? ExpiresAtUtc = null)
{
    public string Summary => $"{Name} ({Status})";

    /// <summary>Builds the row text from the store's summary and the scope text; pure, so a test can check what the list says.</summary>
    public static AlertRuleRow From(CustomAlertRuleSummary rule, string scopeText, int firingServers, DateTime nowUtc)
    {
        var expired = rule.IsExpiredAt(nowUtc);
        var status = expired ? "Expired" : rule.Enabled ? "Active" : "Paused";
        var state = expired ? "Not evaluated" : !rule.Enabled ? "Paused" : firingServers > 0 ? $"Firing on {firingServers}" : "Quiet";
        return new AlertRuleRow(
            rule.Id, rule.Version, rule.Name, rule.Enabled, status, scopeText,
            rule.ExpiresAtUtc is { } at ? at.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) : "Never",
            state,
            rule.LastFired is { } fired ? fired.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) : "Never",
            rule.UpdatedBy ?? "", rule.ExpiresAtUtc);
    }
}

/// <summary>
/// The viewer's custom alert rule manager (#6390, S4): New, New from template, Edit, Pause/Resume and Delete over
/// <c>custom_alert_rules</c>, all through <see cref="ViewerDataService"/>'s calls into <see cref="CustomAlertRuleStore"/>. The write buttons
/// are on only when the store login itself holds INSERT, UPDATE and DELETE on the table (<see cref="ViewerDataService.CanWriteCustomAlertRulesAsync"/>);
/// otherwise they are off and a banner says why. A save carries the version the row was loaded at, and a stale one shows the store's own text.
/// </summary>
public partial class AlertRulesWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly ObservableCollection<AlertRuleRow> _rows = new();
    private IReadOnlyList<string> _serverNames = Array.Empty<string>();
    private IReadOnlyList<DarlingTag> _tags = Array.Empty<DarlingTag>();
    private bool _canWrite;

    public AlertRulesWindow(ViewerDataService dataService)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        RulesGrid.ItemsSource = _rows;
        SetWriteButtons(false);
        Loaded += async (_, _) =>
        {
            _canWrite = await _dataService.CanWriteCustomAlertRulesAsync();
            SetWriteButtons(_canWrite);
            await LoadRulesAsync();
        };
    }

    private void SetWriteButtons(bool canWrite)
    {
        foreach (var button in new[] { NewButton, TemplateButton, EditButton, PauseButton, DeleteButton })
        {
            button.IsEnabled = canWrite;
        }

        ReadOnlyBanner.Text = ViewerDataService.CustomAlertRulesReadOnlyMessage;
        ReadOnlyBanner.Visibility = canWrite ? Visibility.Collapsed : Visibility.Visible;
        ActionButtonsPanel.Visibility = canWrite ? Visibility.Visible : Visibility.Collapsed;
    }

    private async Task LoadRulesAsync()
    {
        try
        {
            _serverNames = (await _dataService.GetMonitoredServersAsync()).Select(s => s.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToList();
            _tags = await _dataService.GetServerTagsAsync();
            var summaries = await _dataService.ListCustomAlertRulesAsync();
            var firing = await _dataService.GetCustomAlertFiringCountsAsync();
            var now = DateTime.UtcNow;
            var rows = new List<AlertRuleRow>();
            foreach (var summary in summaries)
            {
                var scope = await ScopeTextAsync(summary.Id);
                rows.Add(AlertRuleRow.From(summary, scope, firing.TryGetValue(summary.Id, out var count) ? count : 0, now));
            }

            _rows.Clear();
            foreach (var row in rows)
            {
                _rows.Add(row);
            }

            NoRulesMessage.Visibility = _rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            ShowError("Could not read alert rules", ex);
        }
    }

    private async Task<string> ScopeTextAsync(long id)
    {
        var result = await _dataService.GetCustomAlertRuleAsync(id);
        if (result is not CustomAlertRuleResult.Ok { Rule: { } rule } || AlertRuleEditorModel.Load(rule.DefinitionJson) is not { } form)
        {
            return "";
        }

        return form.Scope switch
        {
            AlertRuleScopeKind.Servers => string.Join(", ", form.Servers),
            AlertRuleScopeKind.Tag => $"Tag: {_tags.FirstOrDefault(t => t.Id == form.TagId)?.Name ?? $"#{form.TagId} (not found)"}",
            _ => "All servers",
        };
    }

    private async void NewRule_Click(object sender, RoutedEventArgs e) => await AddAsync(null);

    private void NewFromTemplate_Click(object sender, RoutedEventArgs e)
    {
        var menu = new ContextMenu { PlacementTarget = TemplateButton };
        foreach (var template in CustomAlertTemplates.All)
        {
            var item = new MenuItem { Header = template.Name, ToolTip = template.Description };
            var chosen = template;
            item.Click += async (_, _) => await AddAsync(chosen);
            menu.Items.Add(item);
        }

        menu.IsOpen = true;
    }

    private async Task AddAsync(CustomAlertTemplate? template)
    {
        var dialog = new AlertRuleEditDialog(_serverNames, _tags, null, template) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            var result = await _dataService.CreateCustomAlertRuleAsync(
                dialog.RuleName, dialog.Description, dialog.DefinitionJson, dialog.RuleEnabled, dialog.ExpiresAtUtc);
            if (!Report(result, "Could not save the alert rule"))
            {
                return;
            }

            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not save the alert rule", ex);
        }
    }

    private async void EditRule_Click(object sender, RoutedEventArgs e)
    {
        if (RulesGrid.SelectedItem is not AlertRuleRow selected)
        {
            return;
        }

        try
        {
            /* The dialog saves under the version it loads here, not the one the list showed, so a rule changed since the list was drawn is
               edited from its current text; a change landing while the dialog is open is the stale version the store refuses. */
            var loaded = await _dataService.GetCustomAlertRuleAsync(selected.Id);
            if (loaded is not CustomAlertRuleResult.Ok { Rule: { } rule })
            {
                Report(loaded, "Could not open the alert rule");
                await LoadRulesAsync();
                return;
            }

            var dialog = new AlertRuleEditDialog(_serverNames, _tags, rule) { Owner = this };
            if (dialog.ShowDialog() != true)
            {
                return;
            }

            var result = await _dataService.UpdateCustomAlertRuleAsync(
                rule.Id, dialog.RuleName, dialog.Description, dialog.DefinitionJson, dialog.RuleEnabled, rule.Version, dialog.ExpiresAtUtc);
            Report(result, "Could not update the alert rule");
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not update the alert rule", ex);
        }
    }

    private async void PauseResume_Click(object sender, RoutedEventArgs e)
    {
        if (RulesGrid.SelectedItem is not AlertRuleRow selected)
        {
            return;
        }

        try
        {
            var result = await _dataService.SetCustomAlertRuleEnabledAsync(selected.Id, !selected.Enabled, selected.Version);
            Report(result, "Could not change the alert rule");
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not change the alert rule", ex);
        }
    }

    private async void DeleteRule_Click(object sender, RoutedEventArgs e)
    {
        if (RulesGrid.SelectedItem is not AlertRuleRow selected)
        {
            return;
        }

        var answer = MessageBox.Show(
            $"Delete this alert rule?\n\n{selected.Summary}\n\nAny incident it has open is closed first.",
            "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (answer != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            var result = await _dataService.DeleteCustomAlertRuleAsync(selected.Id);
            Report(result, "Could not delete the alert rule");
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not delete the alert rule", ex);
        }
    }

    /// <summary>Shows a refused store result's own text; false when it was refused.</summary>
    private bool Report(CustomAlertRuleResult result, string what)
    {
        var refusal = ViewerDataService.CustomAlertRuleRefusal(result);
        if (refusal is null)
        {
            return true;
        }

        MessageBox.Show($"{what}: {refusal}", "Alert Rules", MessageBoxButton.OK, MessageBoxImage.Warning);
        return false;
    }

    private void RulesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (_canWrite)
        {
            EditRule_Click(sender, e);
        }
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowError(string what, Exception ex) =>
        MessageBox.Show($"{what}: {ex.Message}", "Alert Rules", MessageBoxButton.OK, MessageBoxImage.Warning);
}
