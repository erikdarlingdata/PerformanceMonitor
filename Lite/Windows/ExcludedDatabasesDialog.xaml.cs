/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

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

    private async Task LoadAsync()
    {
        StatusText.Text = "Loading databases…";
        DatabasesItemsControl.ItemsSource = null;

        try
        {
            var liveDatabases = await GetUserDatabasesAsync();
            var existingExclusions = _server.ExcludedDatabases ?? new List<string>();

            var liveSet = new HashSet<string>(liveDatabases, StringComparer.OrdinalIgnoreCase);

            _items = new ObservableCollection<DatabaseExclusionItem>();

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

            /* Stale entries: in exclusion list but not present on the server. Greyed, disabled, pre-checked. */
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

    private async Task<List<string>> GetUserDatabasesAsync()
    {
        var connectionString = _server.GetConnectionString(_serverManager.CredentialService);
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master",
            ConnectTimeout = 10
        };

        using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        using var cmd = new SqlCommand(@"
SELECT d.name
FROM sys.databases AS d
WHERE d.database_id > 4
AND   d.state_desc = N'ONLINE'
AND   d.database_id < 32761 /*exclude contained AG system databases*/
ORDER BY d.name;", connection);
        cmd.CommandTimeout = 30;

        var names = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }
        return names;
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            _server.ExcludedDatabases = _items
                .Where(i => i.IsExcluded)
                .Select(i => i.Name)
                .ToList();

            _serverManager.UpdateServer(_server);
            ExclusionsModified = true;
            DialogResult = true;
        }
        catch (Exception ex)
        {
            MessageBox.Show(this,
                $"Failed to save exclusions:\n\n{ex.Message}",
                "Save Failed",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
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
