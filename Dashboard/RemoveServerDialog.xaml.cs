/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;

namespace PerformanceMonitorDashboard
{
    public partial class RemoveServerDialog : Window
    {
        public bool DropDatabase => DropDatabaseCheckBox.IsChecked == true;

        public RemoveServerDialog(string serverDisplayName)
        {
            InitializeComponent();
            WarningText.Text = $"Are you sure you want to remove server '{serverDisplayName}'?\n\nThis action cannot be undone.";
        }

        private void Remove_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }
    }
}
