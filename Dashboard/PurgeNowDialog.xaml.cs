/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace PerformanceMonitorDashboard
{
    public partial class PurgeNowDialog : Window
    {
        private readonly string _serverDisplayName;

        /// <summary>
        /// null = use configured retention.
        /// 0 = TRUNCATE every collect.* table.
        /// N > 0 = override every table's cutoff to N days.
        /// </summary>
        public int? RetentionDaysOverride { get; private set; }

        public PurgeNowDialog(string serverDisplayName)
        {
            InitializeComponent();
            _serverDisplayName = serverDisplayName;
            HeaderText.Text = $"Purge collected data on '{serverDisplayName}'.";
            UpdateWarningForMode("configured");
        }

        private void ModeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            /* Fires once during InitializeComponent before other named elements exist; bail until window is loaded. */
            if (CustomDaysPanel is null || WarningText is null) return;

            if (ModeComboBox.SelectedItem is not ComboBoxItem item) return;

            string tag = item.Tag as string ?? "configured";
            CustomDaysPanel.Visibility = tag == "custom" ? Visibility.Visible : Visibility.Collapsed;
            UpdateWarningForMode(tag);
        }

        private void UpdateWarningForMode(string tag)
        {
            WarningText.Text = tag switch
            {
                "configured" => "Deletes data older than the per-collector retention configured in config.collection_schedule. Cannot be undone.",
                "all"        => "WARNING: TRUNCATEs every collect.* table. Wipes ALL collected monitoring data on this server. Cannot be undone.",
                "custom"     => "Deletes data older than the specified number of days. Cannot be undone.",
                _            => $"Deletes data older than {tag} day(s). Cannot be undone."
            };
        }

        private void CustomDaysBox_PreviewTextInput(object sender, TextCompositionEventArgs e)
        {
            e.Handled = !Regex.IsMatch(e.Text, "^[0-9]+$");
        }

        private void Purge_Click(object sender, RoutedEventArgs e)
        {
            if (ModeComboBox.SelectedItem is not ComboBoxItem item) return;

            string tag = item.Tag as string ?? "configured";

            switch (tag)
            {
                case "configured":
                    RetentionDaysOverride = null;
                    break;

                case "1":
                case "3":
                case "7":
                    RetentionDaysOverride = int.Parse(tag);
                    break;

                case "custom":
                    if (!int.TryParse(CustomDaysBox.Text, out int days) || days < 1)
                    {
                        MessageBox.Show(this,
                            "Enter a whole number of days greater than 0.",
                            "Invalid Retention",
                            MessageBoxButton.OK,
                            MessageBoxImage.Warning);
                        return;
                    }
                    RetentionDaysOverride = days;
                    break;

                case "all":
                    var confirm = MessageBox.Show(this,
                        $"This will TRUNCATE every collect.* table on '{_serverDisplayName}', wiping all monitoring data.\n\nAre you absolutely sure?",
                        "Confirm Purge All",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Warning,
                        MessageBoxResult.No);
                    if (confirm != MessageBoxResult.Yes) return;
                    RetentionDaysOverride = 0;
                    break;
            }

            DialogResult = true;
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }
    }
}
