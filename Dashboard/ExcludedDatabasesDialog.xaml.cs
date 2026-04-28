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
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Media;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class ExcludedDatabasesDialog : Window
    {
        private readonly ServerManager _serverManager;
        private readonly ServerConnection _server;
        private ObservableCollection<DatabaseExclusionItem> _items = new();

        public bool ExclusionsModified { get; private set; }

        public ExcludedDatabasesDialog(ServerManager serverManager, ServerConnection server)
        {
            InitializeComponent();
            _serverManager = serverManager;
            _server = server;
            HeaderText.Text = $"Excluded Databases — {server.DisplayNameWithIntent}";
            Loaded += async (_, _) => await LoadAsync();
        }

        private async System.Threading.Tasks.Task LoadAsync()
        {
            StatusText.Text = "Loading databases…";
            DatabasesItemsControl.ItemsSource = null;

            try
            {
                var liveDatabases = await _serverManager.GetUserDatabasesAsync(_server);
                var existingExclusions = await _serverManager.GetCollectorDatabaseExclusionsAsync(_server);

                var liveSet = new HashSet<string>(liveDatabases, StringComparer.OrdinalIgnoreCase);

                _items = new ObservableCollection<DatabaseExclusionItem>();

                /* Live databases: sortable, checkable */
                foreach (var name in liveDatabases.OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
                {
                    _items.Add(new DatabaseExclusionItem
                    {
                        Name = name,
                        DisplayName = name,
                        IsExcluded = existingExclusions.Contains(name, StringComparer.OrdinalIgnoreCase),
                        IsEnabled = true,
                        IsStale = false
                    });
                }

                /* Stale entries: in exclusion list but not present on the server. Show greyed, disabled, pre-checked. */
                foreach (var name in existingExclusions
                             .Where(n => !liveSet.Contains(n))
                             .OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
                {
                    _items.Add(new DatabaseExclusionItem
                    {
                        Name = name,
                        DisplayName = $"{name}  (missing)",
                        IsExcluded = true,
                        IsEnabled = false,
                        IsStale = true
                    });
                }

                DatabasesItemsControl.ItemsSource = _items;
                StatusText.Text = $"{liveDatabases.Count} database(s) on this server, {existingExclusions.Count} currently excluded.";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Failed to load: {ex.Message}";
                MessageBox.Show(this,
                    $"Could not read database list from '{_server.DisplayNameWithIntent}':\n\n{ex.Message}",
                    "Load Failed",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
        }

        private async void Save_Click(object sender, RoutedEventArgs e)
        {
            /* Collect every checked item (live + stale). Stale ones can't be unchecked, so they stay if they were excluded. */
            var checkedNames = _items
                .Where(i => i.IsExcluded)
                .Select(i => i.Name)
                .ToList();

            Cursor = System.Windows.Input.Cursors.Wait;
            try
            {
                await _serverManager.SaveCollectorDatabaseExclusionsAsync(_server, checkedNames);
                ExclusionsModified = true;
                DialogResult = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show(this,
                    $"Failed to save exclusions on '{_server.DisplayNameWithIntent}':\n\n{ex.Message}",
                    "Save Failed",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
            finally
            {
                Cursor = null;
            }
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }
    }

    public class DatabaseExclusionItem : INotifyPropertyChanged
    {
        public string Name { get; set; } = "";
        public string DisplayName { get; set; } = "";
        private bool _isExcluded;
        public bool IsExcluded
        {
            get => _isExcluded;
            set { _isExcluded = value; OnPropertyChanged(nameof(IsExcluded)); }
        }
        public bool IsEnabled { get; set; } = true;
        public bool IsStale { get; set; }

        public Brush ForegroundBrush => IsStale
            ? (Brush)Application.Current.FindResource("ForegroundMutedBrush")
            : (Brush)Application.Current.FindResource("ForegroundBrush");

        public event PropertyChangedEventHandler? PropertyChanged;
        private void OnPropertyChanged(string propertyName)
            => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
