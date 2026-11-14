/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Store Health (#8787): the health of the monitoring store's own tables and indexes, read only. The Viewer twin of the web's Store
/// health page. Every read goes through <see cref="ViewerDataService"/>'s store health reads, which call the same Storage readers the web
/// page and the MCP tools call. The tables, the indexes and the maintenance check are three separate reads started together: the tables
/// grid and the header render as soon as the tables read returns (the indexes read takes about as long again), and the index sections
/// say they are loading until theirs does. Load failures surface on the host status bar via <see cref="StatusChanged"/> and in the
/// section that failed.
/// </summary>
public partial class StoreHealthTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();
    private bool _measuring;

    /// <summary>Each read's last outcome: true once its content is on screen, false once it failed. A read that a newer one superseded
    /// leaves its outcome to that newer read.</summary>
    private bool _tablesLoaded;
    private bool _indexesLoaded;
    private bool _readinessLoaded;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public StoreHealthTab()
    {
        InitializeComponent();
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>Reruns the tables, indexes and maintenance reads together (the shell calls this when the tab becomes visible, and Refresh calls it).</summary>
    public async Task RefreshAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        await Task.WhenAll(LoadTablesAsync(), LoadIndexesAsync(), LoadReadinessAsync());
        StampIfLoaded();
    }

    /// <summary>Raises "store health: refreshed …" when all three reads last finished. Each read reports its own failure, so the stamp never
    /// writes over one.</summary>
    private void StampIfLoaded()
    {
        if (_tablesLoaded && _indexesLoaded && _readinessLoaded)
        {
            LastRefreshedIndicator.Text = "Last refreshed " + ViewerTimeHelper.RefreshedStamp();
            StatusChanged?.Invoke($"store health: refreshed {ViewerTimeHelper.RefreshedStamp()}");
        }
    }

    private async Task LoadTablesAsync()
    {
        var gen = _loads.Claim(nameof(LoadTablesAsync));
        TablesMessage.Text = "Loading store tables…";
        TablesMessage.Visibility = Visibility.Visible;
        HeaderText.Text = "Loading store tables…";
        try
        {
            var read = await _dataService!.GetStoreTableHealthAsync();
            if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

            var selected = (TablesDataGrid.SelectedItem as StoreTableRow)?.Table;
            var rows = StoreHealthRows.Tables(read);
            TablesDataGrid.ItemsSource = rows;
            TablesDataGrid.SelectedItem = selected is null ? null : rows.Find(r => r.Table == selected);
            HeaderText.Text = StoreHealthRows.HeaderText(read);
            TablesMessage.Text = "The store has no tables to show.";
            TablesMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            _tablesLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

            _tablesLoaded = false;
            TablesDataGrid.ItemsSource = null;
            HeaderText.Text = "The store tables failed to load.";
            TablesMessage.Text = $"The store tables failed to load: {ex.Message}";
            TablesMessage.Visibility = Visibility.Visible;
            StatusChanged?.Invoke($"failed to load the store tables: {ex.Message}");
        }

        UpdateMeasureButton();
    }

    private async Task LoadIndexesAsync()
    {
        var gen = _loads.Claim(nameof(LoadIndexesAsync));
        IndexesMessage.Text = "Loading store indexes…";
        IndexesMessage.Visibility = Visibility.Visible;
        IndexHeaderText.Text = "Loading store indexes…";
        CoverageMessage.Text = "Loading partition index coverage…";
        CoverageMessage.Visibility = Visibility.Visible;
        try
        {
            var read = await _dataService!.GetStoreIndexHealthAsync();
            if (_loads.Superseded(nameof(LoadIndexesAsync), gen)) return;

            var indexRows = StoreHealthRows.Indexes(read.Indexes);
            IndexesDataGrid.ItemsSource = indexRows;
            IndexHeaderText.Text = StoreHealthRows.IndexHeaderText(read.Indexes);
            IndexesMessage.Text = "The store has no indexes to show.";
            IndexesMessage.Visibility = indexRows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

            var coverageRows = StoreHealthRows.CoverageRows(read.Coverage);
            CoverageDataGrid.ItemsSource = coverageRows;
            CoverageDataGrid.Visibility = coverageRows.Count == 0 ? Visibility.Collapsed : Visibility.Visible;
            CoverageMessage.Text = "No partitioned tables in the store have indexes to check.";
            CoverageMessage.Visibility = coverageRows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            _indexesLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadIndexesAsync), gen)) return;

            _indexesLoaded = false;
            IndexesDataGrid.ItemsSource = null;
            CoverageDataGrid.ItemsSource = null;
            CoverageDataGrid.Visibility = Visibility.Collapsed;
            IndexHeaderText.Text = "The store indexes failed to load.";
            IndexesMessage.Text = $"The store indexes failed to load: {ex.Message}";
            IndexesMessage.Visibility = Visibility.Visible;
            CoverageMessage.Text = "Partition index coverage failed to load.";
            CoverageMessage.Visibility = Visibility.Visible;
            StatusChanged?.Invoke($"failed to load the store indexes: {ex.Message}");
        }
    }

    private async Task LoadReadinessAsync()
    {
        var gen = _loads.Claim(nameof(LoadReadinessAsync));
        ReadinessTextBlock.Text = "Checking whether now is a safe time for maintenance…";
        try
        {
            var readiness = await _dataService!.GetStoreMaintenanceReadinessAsync();
            if (_loads.Superseded(nameof(LoadReadinessAsync), gen)) return;

            ShowReadiness(StoreHealthRows.ReadinessParts(readiness));
            _readinessLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadReadinessAsync), gen)) return;

            _readinessLoaded = false;
            ReadinessTextBlock.Text = $"The maintenance check failed: {ex.Message}";
            StatusChanged?.Invoke($"failed to run the store maintenance check: {ex.Message}");
        }
    }

    /// <summary>Fills the verdict block: the verdict lead in semibold, the sentences and reasons after it in normal weight.</summary>
    private void ShowReadiness((string Lead, string After) parts)
    {
        ReadinessTextBlock.Inlines.Clear();
        if (parts.Lead.Length > 0)
        {
            ReadinessTextBlock.Inlines.Add(new Run(parts.Lead) { FontWeight = FontWeights.SemiBold });
        }

        if (parts.After.Length > 0)
        {
            ReadinessTextBlock.Inlines.Add(new Run(parts.After));
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync();

    private void TablesDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) => UpdateMeasureButton();

    /// <summary>Measure is on for a plain table, and off while a measure runs (one table at a time).</summary>
    private void UpdateMeasureButton()
    {
        var target = (TablesDataGrid.SelectedItem as StoreTableRow)?.MeasureTarget;
        MeasureButton.IsEnabled = target is not null && !_measuring;
        if (!_measuring && TablesDataGrid.SelectedItem is StoreTableRow { MeasureTarget: null } other)
        {
            MeasureResultText.Text = $"{other.Table} is a {other.Kind}. Measure works on plain tables. {StoreHealthRows.MeasureHint}";
        }
    }

    private async void MeasureButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null || _measuring || (TablesDataGrid.SelectedItem as StoreTableRow)?.MeasureTarget is not { } target)
        {
            return;
        }

        _measuring = true;
        MeasureButton.IsEnabled = false;
        MeasureResultText.Text = $"Measuring {target}. This reads every page of the table.";
        var clock = Stopwatch.StartNew();
        try
        {
            var result = await _dataService.MeasureStoreTableAsync(target);
            MeasureResultText.Text = StoreHealthRows.MeasureText(target, result, clock.Elapsed);
        }
        catch (Exception ex)
        {
            MeasureResultText.Text = $"Measuring {target} failed: {ex.Message}";
            StatusChanged?.Invoke($"failed to measure {target}: {ex.Message}");
        }
        finally
        {
            _measuring = false;
            UpdateMeasureButton();
        }
    }
}
