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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's Excluded Databases editor — a checkbox picker over the databases the Darling service has
/// COLLECTED for this server, adapted from Lite's dialog. Lite reads the server's LIVE database list (it
/// connects to the monitored server); the viewer never connects, but the store already holds the
/// collected databases (<c>v_database_config</c> / <c>v_database_size_stats</c>), so the picker offers
/// those instead of a free-text editor. A not-yet-collected database can still be added by hand (the
/// manual fallback). Persists the checked names to <c>config.config_monitored_servers.excluded_databases</c>
/// (Stage 3) so the Darling service honors them on its next reload — keyed by the shared <c>server_id</c>,
/// which is why the collected-database read needs no name→id lookup.
/// </summary>
public partial class ExcludedDatabasesDialog : Window
{
    private readonly ViewerDataService _dataService;
    private readonly MonitoredServerRow _row;
    private ObservableCollection<ExcludedDatabaseItem> _items = new();

    /// <summary>Set true when the exclusion list was changed and saved, so the caller refreshes.</summary>
    public bool ExclusionsModified { get; private set; }

    /// <param name="dataService">The store this reads the collected-database list from and writes the exclusions to.</param>
    /// <param name="row">The store row being edited (its <c>ExcludedDatabases</c> is updated in place on save).</param>
    /// <param name="displayName">The header label (the server's display name with its read-only-intent suffix).</param>
    /// <param name="xeSessionsMode">#7639: true to edit the databases that get Extended Events sessions on Azure SQL Database (V247) instead of the
    /// excluded databases. Same picker: a checked database is on the list, and an empty list means every database.</param>
    public ExcludedDatabasesDialog(ViewerDataService dataService, MonitoredServerRow row, string displayName, bool xeSessionsMode = false)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _row = row ?? throw new ArgumentNullException(nameof(row));
        _xeSessionsMode = xeSessionsMode;
        if (xeSessionsMode)
        {
            Title = "Databases for Extended Events Sessions";
            HeaderText.Text = $"Databases for Extended Events Sessions — {displayName}";
            DescriptionText.Text = "Applies to Azure SQL Database only. Azure caps database-scoped Extended Events sessions per pool, and Darling makes up to four per database "
                + "(deadlock, blocked process, client timeout, long query completions). Check the databases that should get sessions. Leave every box clear to give every "
                + "database a session. Excluded Databases still apply and win. Sessions that already exist in unchecked databases are left in place; drop them yourself if you no longer need them.";
        }
        else
        {
            HeaderText.Text = $"Excluded Databases — {displayName}";
        }

        Loaded += async (_, _) => await LoadAsync();
    }

    /// <summary>#7639: true when the dialog edits the Extended Events session databases instead of the excluded databases.</summary>
    private readonly bool _xeSessionsMode;

    private async Task LoadAsync()
    {
        var existing = _row.ExcludedDatabases ?? new List<string>();
        if (_xeSessionsMode)
        {
            try
            {
                existing = await _dataService.GetMonitoredServerXeSessionDatabasesAsync(_row.ServerId);
            }
            catch (Exception ex)
            {
                existing = new List<string>();
                ViewerLogger.Warn("ExcludedDatabasesDialog", $"Could not read the Extended Events session databases: {ex.Message}");
            }
        }

        StatusText.Text = "Loading collected databases…";

        List<string> collected = new();
        var collectedCount = 0;
        try
        {
            /* The config row already carries the shared server_id, so the collected-database read needs no
               name→id lookup (unlike the old registry path). */
            collected = await _dataService.GetCollectedDatabaseNamesAsync(_row.ServerId);
            collectedCount = collected.Count;
        }
        catch (Exception ex)
        {
            /* A store read failure must never block editing exclusions — fall back to manual-only. */
            ViewerLogger.Warn("ExcludedDatabasesDialog", $"Could not read collected databases: {ex.Message}");
        }

        _items = new ObservableCollection<ExcludedDatabaseItem>(BuildDatabaseItems(collected, existing));
        DatabasesItemsControl.ItemsSource = _items;

        StatusText.Text = collectedCount > 0
            ? $"{collectedCount} collected database(s), {existing.Count} currently excluded."
            : $"No collected databases for this server yet — add names to exclude. {existing.Count} currently excluded.";
    }

    /// <summary>
    /// Merges the store's collected user-database names with the entry's existing exclusions into the
    /// checkbox list: every collected database is a checkbox (checked when already excluded), and any
    /// existing exclusion the store has NOT collected is preserved as a checked "(not collected)" row so
    /// saving never silently drops it (the viewer can't verify liveness, so it stays enabled/removable).
    /// Pure so it is unit-testable without a store. Case-insensitive throughout.
    /// </summary>
    public static List<ExcludedDatabaseItem> BuildDatabaseItems(IEnumerable<string> collectedNames, IEnumerable<string> existingExclusions)
    {
        var excluded = new HashSet<string>(
            (existingExclusions ?? Enumerable.Empty<string>())
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .Select(n => n.Trim()),
            StringComparer.OrdinalIgnoreCase);

        var collected = (collectedNames ?? Enumerable.Empty<string>())
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Select(n => n.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var collectedSet = new HashSet<string>(collected, StringComparer.OrdinalIgnoreCase);

        var items = new List<ExcludedDatabaseItem>();
        foreach (var name in collected)
        {
            items.Add(new ExcludedDatabaseItem
            {
                Name = name,
                DisplayName = name,
                IsExcluded = excluded.Contains(name),
                IsCollected = true
            });
        }

        /* Existing exclusions the store hasn't collected (yet, or ever): keep them, checked, so a save
           preserves them. Enabled (not disabled like Lite's stale rows) — the viewer can't confirm the
           database is truly gone, and this is exactly the not-yet-collected manual case. */
        foreach (var name in excluded.Where(n => !collectedSet.Contains(n)).OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            items.Add(new ExcludedDatabaseItem
            {
                Name = name,
                DisplayName = $"{name}  (not collected)",
                IsExcluded = true,
                IsCollected = false
            });
        }

        return items;
    }

    private void ManualAddBox_KeyDown(object sender, KeyEventArgs e)
    {
        if (e.Key == Key.Enter)
        {
            AddManualDatabase();
            e.Handled = true;
        }
    }

    private void ManualAdd_Click(object sender, RoutedEventArgs e) => AddManualDatabase();

    private void AddManualDatabase()
    {
        var name = ManualAddBox.Text.Trim();
        if (name.Length == 0)
        {
            return;
        }

        var match = _items.FirstOrDefault(i => string.Equals(i.Name, name, StringComparison.OrdinalIgnoreCase));
        if (match is not null)
        {
            /* Already listed (collected or previously added) — just tick it. */
            match.IsExcluded = true;
            StatusText.Text = $"'{name}' is already in the list — marked excluded.";
        }
        else
        {
            _items.Insert(0, new ExcludedDatabaseItem
            {
                Name = name,
                DisplayName = $"{name}  (not collected)",
                IsExcluded = true,
                IsCollected = false
            });
            StatusText.Text = $"Added '{name}'.";
        }

        ManualAddBox.Clear();
        ManualAddBox.Focus();
    }

    private async void Save_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var names = _items
                .Where(i => i.IsExcluded)
                .Select(i => i.Name)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (_xeSessionsMode)
            {
                await _dataService.SetMonitoredServerXeSessionDatabasesAsync(_row.ServerId, names);
            }
            else
            {
                await _dataService.SetMonitoredServerExcludedDatabasesAsync(_row.ServerId, names);
                _row.ExcludedDatabases = names;
            }

            ExclusionsModified = true;
            DialogResult = true;
        }
        catch (ViewerReadOnlyException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (ViewerSchemaSkewException ex)
        {
            StatusText.Text = ex.Message;
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Failed to save: {ex.Message}";
            ViewerLogger.Error("ExcludedDatabasesDialog", "Failed to save exclusions", ex);
        }
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;
}

/// <summary>
/// One row of the Excluded Databases picker — a database name, its checked (excluded) state, and whether
/// the store has actually collected it. Mirrors Lite's <c>DatabaseExclusionItem</c>; the viewer keeps
/// not-collected rows enabled (Lite disabled its stale rows) because the viewer can't verify liveness.
/// </summary>
public sealed class ExcludedDatabaseItem : INotifyPropertyChanged
{
    public string Name { get; set; } = "";
    public string DisplayName { get; set; } = "";

    private bool _isExcluded;
    public bool IsExcluded
    {
        get => _isExcluded;
        set { _isExcluded = value; OnPropertyChanged(nameof(IsExcluded)); }
    }

    /// <summary>True when the store has collected this database; false for a manually-added / not-yet-collected name.</summary>
    public bool IsCollected { get; set; } = true;

    /// <summary>Always editable — the viewer can't confirm a database is truly gone, so nothing is disabled.</summary>
    public bool IsEnabled => true;

    public Brush ForegroundBrush => IsCollected
        ? (Brush)Application.Current.FindResource("ForegroundBrush")
        : (Brush)Application.Current.FindResource("ForegroundMutedBrush");

    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged(string propertyName)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}
