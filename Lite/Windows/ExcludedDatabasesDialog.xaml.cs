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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class ExcludedDatabasesDialog : Window
{
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private ObservableCollection<DatabaseExclusionItem> _items = new();

    /// <summary>Set when master could not be enumerated (an Azure server-firewall block, or a contained
    /// login without master rights) and the list fell back to just the connected database — the picker then
    /// shows an explanatory note instead of a raw error, mirroring the collectors' single-database
    /// fallback (#1631).</summary>
    private bool _masterUnreachableFellBackToConnectedDb;

    /// <summary>
    /// #7639: true when the dialog edits <see cref="ServerConnection.XeSessionDatabases"/> (the databases that get Extended
    /// Events sessions on Azure SQL Database) instead of the excluded databases. Same picker, same checkboxes: a checked
    /// database is on the list. An empty list means every database.
    /// </summary>
    private readonly bool _xeSessionsMode;

    public bool ExclusionsModified { get; private set; }

    public ExcludedDatabasesDialog(ServerManager serverManager, ServerConnection server, bool xeSessionsMode = false)
    {
        InitializeComponent();
        _serverManager = serverManager;
        _server = server;
        _xeSessionsMode = xeSessionsMode;
        if (xeSessionsMode)
        {
            Title = "Databases for Extended Events Sessions";
            HeaderText.Text = $"Databases for Extended Events Sessions — {server.DisplayNameWithIntent}";
            DescriptionText.Text = "Applies to Azure SQL Database only. Azure caps database-scoped Extended Events sessions per pool, and Lite makes up to four per database "
                + "(deadlock, blocked process, client timeout, long query completions). Check the databases that should get sessions. "
                + "Leave every box clear to give every database a session. Excluded Databases still apply and win. "
                + "Sessions that already exist in unchecked databases are left in place; drop them yourself if you no longer need them. The old long query session from before sessions were named per install is still removed everywhere.";
        }
        else
        {
            HeaderText.Text = $"Excluded Databases — {server.DisplayNameWithIntent}";
        }

        Loaded += async (_, _) => await LoadAsync();
    }

    private async Task LoadAsync()
    {
        StatusText.Text = "Loading databases…";
        DatabasesItemsControl.ItemsSource = null;
        _masterUnreachableFellBackToConnectedDb = false;

        try
        {
            var liveDatabases = await GetUserDatabasesAsync();
            var existingExclusions = (_xeSessionsMode ? _server.XeSessionDatabases : _server.ExcludedDatabases) ?? new List<string>();

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
            StatusText.Text = _masterUnreachableFellBackToConnectedDb
                ? "Server firewall blocks reading the full list from master (Azure database-level rule); showing only the connected database."
                : _xeSessionsMode
                    ? $"{liveDatabases.Count} database(s) on this server, {existingExclusions.Count} on the list (none = every database)."
                    : $"{liveDatabases.Count} database(s) on this server, {existingExclusions.Count} currently excluded.";
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
        var connectionString = _serverManager.CredentialResolver.GetConnectionString(_server);
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master",
            ConnectTimeout = 10
        };

        try
        {
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
        catch (SqlException ex) when (SqlErrorClassification.ShouldFallBackToSingleDatabase(ex.Number))
        {
            /* Azure database-level firewall (or a contained login without master rights): master cannot be
               enumerated, but the connected database is reachable. Fall back to it instead of failing the
               picker outright, mirroring the collectors' single-database fallback (#1631). The connected
               database is the connection string's own catalog. */
            _masterUnreachableFellBackToConnectedDb = true;
            var connectedDb = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
            return string.IsNullOrWhiteSpace(connectedDb)
                ? new List<string>()
                : new List<string> { connectedDb };
        }
    }

    /// <summary>
    /// Writes the checked databases to the server (the XE session list in XE-sessions mode, the excluded databases otherwise)
    /// and saves servers.json. This touches no credential, so it goes through <see cref="ServerManager.UpdateServerSettings"/>:
    /// <see cref="ServerManager.UpdateServer"/> with no username would delete an Entra MFA server's stored username (#7639).
    /// </summary>
    internal static void PersistSelection(ServerManager serverManager, ServerConnection server, List<string> checkedNames, bool xeSessionsMode)
    {
        if (xeSessionsMode)
        {
            server.XeSessionDatabases = checkedNames;
        }
        else
        {
            server.ExcludedDatabases = checkedNames;
        }

        serverManager.UpdateServerSettings(server);
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var checkedNames = _items
                .Where(i => i.IsExcluded)
                .Select(i => i.Name)
                .ToList();
            PersistSelection(_serverManager, _server, checkedNames, _xeSessionsMode);
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
