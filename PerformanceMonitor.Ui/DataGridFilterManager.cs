/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Media;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Non-generic interface for looking up filter state from a shared dictionary.
/// </summary>
public interface IDataGridFilterManager
{
    Dictionary<string, ColumnFilterState> Filters { get; }
    void SetFilter(ColumnFilterState filterState);
    void UpdateFilterButtonStyles();
    void ClearFilters();

    /// <summary>
    /// #5565: the user's "Clear all filters" on the grid: <see cref="ClearFilters"/> plus the stored copy, so the
    /// filters do not come back after a restart.
    /// </summary>
    void ClearAllFilters();

    /// <summary>
    /// #5565: the values a column's filter popup lists, from every row the grid holds now (before any filter), or
    /// null when the column gets no list (not a text column, query/plan/XML/prose text, too long, or no rows yet).
    /// </summary>
    ColumnValueCatalog? GetValueCatalog(string columnName);

    /// <summary>The grid has at least one active filter.</summary>
    bool AnyFilterActive { get; }
}

/// <summary>
/// Manages column filter state, unfiltered data capture, and filter application
/// for a single DataGrid. Eliminates per-grid boilerplate code. Preserves the user's
/// sort order across refresh/filter cycles. Shared by Lite and Dashboard.
/// </summary>
public class DataGridFilterManager<T> : IDataGridFilterManager
{
    private readonly DataGrid _dataGrid;
    private readonly Dictionary<string, ColumnFilterState> _filters = new();
    private List<T>? _unfilteredData;

    public DataGridFilterManager(DataGrid dataGrid)
    {
        _dataGrid = dataGrid;
    }

    public Dictionary<string, ColumnFilterState> Filters => _filters;

    public bool AnyFilterActive => HasActiveFilters();

    /// <summary>The server scope the stored filters were last loaded for; null until the first scoped refresh.</summary>
    private string? _loadedScope;

    /// <summary>
    /// Called when new data arrives (refresh cycle). Captures unfiltered data,
    /// then re-applies any active filters. Preserves user sort order.
    /// </summary>
    public void UpdateData(List<T> newData)
    {
        _unfilteredData = newData;
        LoadStoredFilters();

        /* An empty grid says so (Lite click-through F15) instead of showing a bare header row. */
        EmptyState.Show(_dataGrid, newData.Count == 0);

        if (!HasActiveFilters())
        {
            SetItemsSourcePreservingSort(newData);
            return;
        }

        ApplyFilters();
    }

    /// <summary>
    /// Applies or removes a filter and re-filters the data.
    /// </summary>
    public void SetFilter(ColumnFilterState filterState)
    {
        LoadStoredFilters(); /* a filter set before the first scoped refresh must not wipe the stored ones */

        if (filterState.IsActive)
            _filters[filterState.ColumnName] = filterState;
        else
            _filters.Remove(filterState.ColumnName);

        ApplyFilters();
        UpdateFilterButtonStyles();
        SaveStoredFilters();
    }

    /// <summary>
    /// #2306: drops every filter and restores the unfiltered data (sort preserved, funnel icons dimmed).
    /// The caller that matters is a SERVER SWITCH on a cross-server surface: a DatabaseName filter set
    /// against server A silently zeroes server B's grid while count indicators — computed from the
    /// unfiltered list — stay full, and Refresh cannot clear it because <see cref="UpdateData"/>
    /// deliberately re-applies active filters. That re-apply is correct for refresh-on-the-same-server
    /// and is untouched; only an explicit context change goes through here.
    /// </summary>
    public void ClearFilters()
    {
        /* #5565: the stored copy is not touched (a context change is not the user clearing the filters), but the next
           refresh loads whatever is stored for the scope it then finds, so a server switch brings that server's own. */
        _loadedScope = null;

        if (_filters.Count == 0)
        {
            return;
        }

        _filters.Clear();

        if (_unfilteredData is not null)
        {
            SetItemsSourcePreservingSort(_unfilteredData);
        }

        UpdateFilterButtonStyles();
    }

    /// <summary>
    /// #5565: "Clear all filters" on the grid. Drops every filter like <see cref="ClearFilters"/> and removes the
    /// grid's stored copy too, so nothing comes back after a restart.
    /// </summary>
    public void ClearAllFilters()
    {
        var scope = CurrentScope();
        ClearFilters();
        _loadedScope = scope;
        SaveStoredFilters();
    }

    /// <summary>The server scope and grid name the stored filters are keyed by, or null for a grid kept for the session only.</summary>
    private string? CurrentScope()
    {
        if (ColumnFilterStore.Current is null || string.IsNullOrEmpty(_dataGrid.Name))
            return null;
        var scope = ColumnFilterScope.GetServer(_dataGrid);
        return string.IsNullOrEmpty(scope) ? null : scope;
    }

    /// <summary>
    /// #5565: on the first refresh under a server scope (and again after <see cref="ClearFilters"/> changed the
    /// context), brings back the filters stored for this server's grid. A stored filter whose column the grid no
    /// longer has is ignored; a filter already set this session wins over a stored one on the same column.
    /// </summary>
    private void LoadStoredFilters()
    {
        var scope = CurrentScope();
        if (scope is null || scope == _loadedScope)
            return;
        _loadedScope = scope;

        IReadOnlyList<ColumnFilterState> stored;
        try
        {
            stored = ColumnFilterStore.Current!.Load(scope, _dataGrid.Name);
        }
        catch (Exception)
        {
            return; // a store that cannot answer never blocks a grid
        }

        if (stored.Count == 0)
            return;

        var known = KnownColumns();
        foreach (var filter in stored)
        {
            if (known.Contains(filter.ColumnName) && !_filters.ContainsKey(filter.ColumnName))
                _filters[filter.ColumnName] = filter;
        }
        UpdateFilterButtonStyles();
    }

    private void SaveStoredFilters()
    {
        var scope = CurrentScope();
        if (scope is null)
            return;
        try
        {
            ColumnFilterStore.Current!.Save(scope, _dataGrid.Name, _filters.Values);
        }
        catch (Exception)
        {
            /* a store that cannot save never blocks a grid; the store reports its own file problems */
        }
    }

    /// <summary>The columns the grid has: each bound column's path and each filter button's Tag.</summary>
    private HashSet<string> KnownColumns()
    {
        var known = new HashSet<string>(StringComparer.Ordinal);
        foreach (var column in _dataGrid.Columns)
        {
            if (column is DataGridBoundColumn { Binding: Binding binding } && !string.IsNullOrEmpty(binding.Path?.Path))
                known.Add(binding.Path.Path);
            if (column.Header is StackPanel panel && panel.Children.OfType<Button>().FirstOrDefault()?.Tag is string tag)
                known.Add(tag);
        }
        return known;
    }

    /// <summary>
    /// #5565: the values the column's popup lists. Null when the column gets none: it is excluded by name (query,
    /// plan, XML, prose, display strings), is not a text property of the row type, sorts by another member than it
    /// shows (a time shown as text), has a value over the length limit, or there are no rows yet.
    /// </summary>
    public ColumnValueCatalog? GetValueCatalog(string columnName)
    {
        if (_unfilteredData is null || ColumnValueListColumns.IsExcluded(columnName))
            return null;

        var property = typeof(T).GetProperty(columnName);
        if (property is null || property.PropertyType != typeof(string) || property.GetIndexParameters().Length != 0)
            return null;

        foreach (var column in _dataGrid.Columns)
        {
            if (column is DataGridBoundColumn { Binding: Binding binding } && binding.Path?.Path == columnName &&
                !string.IsNullOrEmpty(column.SortMemberPath) && column.SortMemberPath != columnName)
                return null;
        }

        var catalog = ColumnValueCatalog.Build(_unfilteredData.Select(item => (string?)property.GetValue(item)));
        return catalog.IsListable ? catalog : null;
    }

    private bool HasActiveFilters()
    {
        return _filters.Count > 0 && _filters.Values.Any(f => f.IsActive);
    }

    private void ApplyFilters()
    {
        if (_unfilteredData == null) return;

        if (!HasActiveFilters())
        {
            SetItemsSourcePreservingSort(_unfilteredData);
            return;
        }

        var filteredData = _unfilteredData.Where(item =>
        {
            foreach (var filter in _filters.Values)
            {
                if (filter.IsActive && !ColumnFilterMatcher.MatchesFilter(item!, filter))
                    return false;
            }
            return true;
        }).ToList();

        SetItemsSourcePreservingSort(filteredData);
    }

    private void SetItemsSourcePreservingSort(System.Collections.IEnumerable? newSource)
    {
        var savedSorts = _dataGrid.Items.SortDescriptions.ToList();

        _dataGrid.ItemsSource = newSource;

        if (savedSorts.Count > 0)
        {
            foreach (var sort in savedSorts)
                _dataGrid.Items.SortDescriptions.Add(sort);

            foreach (var column in _dataGrid.Columns)
            {
                if (column is DataGridBoundColumn bc &&
                    bc.Binding is Binding b)
                {
                    /* A column that sorts by another member than the one it shows (a time column bound to its text and
                       sorted by its DateTime, #4766) carries that member in SortMemberPath, which is also what a header click
                       put in the saved sort. The binding path is only the column's sort member when none is set. */
                    var sortPath = string.IsNullOrEmpty(column.SortMemberPath) ? b.Path.Path : column.SortMemberPath;
                    var match = savedSorts.FirstOrDefault(s => s.PropertyName == sortPath);
                    column.SortDirection = match.PropertyName != null ? match.Direction : null;
                }
            }
        }
    }

    /// <summary>
    /// Updates filter icon colors (gold when active, dim when inactive).
    /// </summary>
    public void UpdateFilterButtonStyles()
    {
        foreach (var column in _dataGrid.Columns)
        {
            if (column.Header is StackPanel headerPanel)
            {
                var filterButton = headerPanel.Children.OfType<Button>().FirstOrDefault();
                if (filterButton != null && filterButton.Tag is string columnName)
                {
                    bool hasActive = _filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                    var textBlock = new TextBlock
                    {
                        Text = hasActive ? "\uE16E" : "\uE71C",
                        FontFamily = new FontFamily("Segoe MDL2 Assets"),
                        Foreground = hasActive
                            ? new SolidColorBrush(Color.FromRgb(0xFF, 0xD7, 0x00))
                            : (Application.Current?.TryFindResource("ForegroundDimBrush") as Brush ?? Brushes.Gray) /* no Application in a headless test: a plain grey */
                    };
                    filterButton.Content = textBlock;

                    filterButton.ToolTip = hasActive && filter != null
                        ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                        : "Click to filter";
                }
            }
        }
    }
}
