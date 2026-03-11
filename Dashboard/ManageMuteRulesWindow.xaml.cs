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
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class ManageMuteRulesWindow : Window
    {
        private readonly MuteRuleService _muteRuleService;
        private readonly ObservableCollection<MuteRule> _rules;

        public ManageMuteRulesWindow(MuteRuleService muteRuleService)
        {
            InitializeComponent();
            _muteRuleService = muteRuleService;
            _rules = new ObservableCollection<MuteRule>(_muteRuleService.GetRules());
            RulesGrid.ItemsSource = _rules;
        }

        private void AddRule_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new MuteRuleDialog { Owner = this };
            if (dialog.ShowDialog() == true)
            {
                _muteRuleService.AddRule(dialog.Rule);
                _rules.Add(dialog.Rule);
            }
        }

        private void EditRule_Click(object sender, RoutedEventArgs e)
        {
            if (RulesGrid.SelectedItem is not MuteRule selected) return;
            var dialog = new MuteRuleDialog(selected) { Owner = this };
            if (dialog.ShowDialog() == true)
            {
                _muteRuleService.UpdateRule(dialog.Rule);
                RefreshList();
            }
        }

        private void ToggleRule_Click(object sender, RoutedEventArgs e)
        {
            if (RulesGrid.SelectedItem is not MuteRule selected) return;
            var index = RulesGrid.SelectedIndex;
            _muteRuleService.SetRuleEnabled(selected.Id, !selected.Enabled);
            RefreshList();
            if (index < _rules.Count) RulesGrid.SelectedIndex = index;
            RulesGrid.Focus();
        }

        private void DeleteRule_Click(object sender, RoutedEventArgs e)
        {
            if (RulesGrid.SelectedItem is not MuteRule selected) return;
            var index = RulesGrid.SelectedIndex;
            var result = MessageBox.Show(
                $"Delete this mute rule?\n\n{selected.Summary}",
                "Confirm Delete",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (result == MessageBoxResult.Yes)
            {
                _muteRuleService.RemoveRule(selected.Id);
                _rules.Remove(selected);
                if (_rules.Count > 0)
                    RulesGrid.SelectedIndex = Math.Min(index, _rules.Count - 1);
                RulesGrid.Focus();
            }
        }

        private void PurgeExpired_Click(object sender, RoutedEventArgs e)
        {
            int removed = _muteRuleService.PurgeExpiredRules();
            if (removed > 0)
            {
                RefreshList();
                MessageBox.Show($"Removed {removed} expired rule(s).", "Purge Complete",
                    MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show("No expired rules to remove.", "Purge Complete",
                    MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        private void EnabledCheckBox_Click(object sender, RoutedEventArgs e)
        {
            if (sender is CheckBox cb && cb.DataContext is MuteRule rule)
            {
                _muteRuleService.SetRuleEnabled(rule.Id, cb.IsChecked == true);
            }
        }

        private void RulesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            EditRule_Click(sender, e);
        }

        private void RefreshList()
        {
            _rules.Clear();
            foreach (var rule in _muteRuleService.GetRules())
                _rules.Add(rule);
        }

        private void Close_Click(object sender, RoutedEventArgs e) => Close();
    }
}
