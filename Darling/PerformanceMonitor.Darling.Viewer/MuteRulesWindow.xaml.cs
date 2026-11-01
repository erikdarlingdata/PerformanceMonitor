/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's mute-rule manager — Lite's <c>ManageMuteRulesWindow</c>
/// (Lite/Windows/ManageMuteRulesWindow.xaml.cs) ported: Add/Edit/Toggle/Delete/Purge-Expired over
/// the alert mute rules. The one structural difference from Lite is the persistence seam: Lite goes
/// through the in-memory <c>MuteRuleService</c> cache, while the viewer writes STRAIGHT to Postgres
/// via <see cref="ViewerDataService"/> (config_mute_rules) — the store is the coordination point,
/// and the running Darling service re-reads it on its next mute-rule load. Each write is awaited and
/// the list reloaded from the store, so the grid always reflects what was persisted.
/// </summary>
public partial class MuteRulesWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly ObservableCollection<MuteRule> _rules = new();

    /// <summary>
    /// False when the viewer is connected with the read-only <c>viewer</c> role — bound by the
    /// Enabled-column checkbox's <c>IsEnabled</c> so a read-only user can't toggle a rule. Set once at
    /// construction (the connection's role does not change while the window is open).
    /// </summary>
    public bool RulesAreEditable { get; }

    public MuteRulesWindow(ViewerDataService dataService)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        RulesAreEditable = !dataService.IsReadOnly;

        /* V8 security hardening: a read-only connection views rules but can't Add/Edit/Toggle/Delete/
           Purge them — replace the action buttons with an explaining banner. The reactive 42501 catch
           in the write paths is the backstop if grants change under the open window. */
        if (dataService.IsReadOnly)
        {
            ActionButtonsPanel.Visibility = Visibility.Collapsed;
            ReadOnlyBanner.Text = "Read-only connection: mute rules can be viewed but not changed here. " + ViewerReadOnlyException.HowToConnectAsAdmin;
            ReadOnlyBanner.Visibility = Visibility.Visible;
        }

        RulesGrid.ItemsSource = _rules;
        Loaded += async (_, _) => await LoadRulesAsync();
    }

    private async System.Threading.Tasks.Task LoadRulesAsync()
    {
        try
        {
            var rules = await _dataService.GetMuteRulesAsync();
            _rules.Clear();
            foreach (var rule in rules)
            {
                _rules.Add(rule);
            }

            NoRulesMessage.Visibility = _rules.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            ShowError("Could not read mute rules", ex);
        }
    }

    private async void AddRule_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new MuteRuleEditDialog((MuteRule?)null, await _dataService.SupportsMuteRecurrenceAsync()) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await _dataService.InsertMuteRuleAsync(dialog.Rule);
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not save the mute rule", ex);
        }
    }

    private async void EditRule_Click(object sender, RoutedEventArgs e)
    {
        if (RulesGrid.SelectedItem is not MuteRule selected)
        {
            return;
        }

        var dialog = new MuteRuleEditDialog(selected, await _dataService.SupportsMuteRecurrenceAsync()) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await _dataService.UpdateMuteRuleAsync(dialog.Rule);
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not update the mute rule", ex);
        }
    }

    private async void ToggleRule_Click(object sender, RoutedEventArgs e)
    {
        if (RulesGrid.SelectedItem is not MuteRule selected)
        {
            return;
        }

        try
        {
            await _dataService.SetMuteRuleEnabledAsync(selected.Id, !selected.Enabled);
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not toggle the mute rule", ex);
        }
    }

    private async void DeleteRule_Click(object sender, RoutedEventArgs e)
    {
        if (RulesGrid.SelectedItem is not MuteRule selected)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Delete this mute rule?\n\n{selected.Summary}",
            "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.DeleteMuteRuleAsync(selected.Id);
            await LoadRulesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not delete the mute rule", ex);
        }
    }

    private async void PurgeExpired_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var removed = await _dataService.PurgeExpiredMuteRulesAsync();
            if (removed > 0)
            {
                await LoadRulesAsync();
            }

            MessageBox.Show(
                removed > 0 ? $"Removed {removed} expired rule(s)." : "No expired rules to remove.",
                "Purge Complete", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            ShowError("Could not purge expired mute rules", ex);
        }
    }

    private async void EnabledCheckBox_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not CheckBox cb || cb.DataContext is not MuteRule rule)
        {
            return;
        }

        try
        {
            await _dataService.SetMuteRuleEnabledAsync(rule.Id, cb.IsChecked == true);
        }
        catch (Exception ex)
        {
            /* The two-way binding already wrote the attempted value into rule.Enabled, so reload
               from the store (unchanged by the failed write) to restore the grid to the truth. */
            ShowError("Could not change the enabled state", ex);
            await LoadRulesAsync();
        }
    }

    private void RulesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e) => EditRule_Click(sender, e);

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowError(string what, Exception ex) =>
        MessageBox.Show($"{what}: {ex.Message}", "Mute Rules", MessageBoxButton.OK, MessageBoxImage.Warning);
}

/// <summary>The Schedule column of the Manage Mute Rules list (#7040): the rule's repeating window, with a note when it is open now.
/// A one-off rule shows nothing.</summary>
public sealed class MuteScheduleConverter : System.Windows.Data.IValueConverter
{
    internal static string Display(MuteRule rule, DateTime nowUtc)
    {
        var text = rule.ScheduleText;
        if (text is null)
        {
            return "";
        }

        return rule.IsInWindowAt(nowUtc) ? text + " (in window now)" : text;
    }

    public object Convert(object? value, Type targetType, object? parameter, System.Globalization.CultureInfo culture) =>
        value is MuteRule rule ? Display(rule, DateTime.UtcNow) : "";

    public object ConvertBack(object? value, Type targetType, object? parameter, System.Globalization.CultureInfo culture) =>
        throw new NotSupportedException();
}
