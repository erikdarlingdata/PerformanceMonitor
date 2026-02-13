/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Non-generic interface for looking up filter state from a shared dictionary.
/// </summary>
public interface IDataGridFilterManager
{
    Dictionary<string, ColumnFilterState> Filters { get; }
    void SetFilter(ColumnFilterState filterState);
    void UpdateFilterButtonStyles();
}

/// <summary>
/// Manages column filter state, unfiltered data capture, and filter application
/// for a single DataGrid. Eliminates per-grid boilerplate code.
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

    /// <summary>
    /// Called when new data arrives (refresh cycle). Captures unfiltered data,
    /// then re-applies any active filters.
    /// </summary>
    public void UpdateData(List<T> newData)
    {
        _unfilteredData = newData;

        if (!HasActiveFilters())
        {
            _dataGrid.ItemsSource = newData;
            return;
        }

        ApplyFilters();
    }

    /// <summary>
    /// Applies or removes a filter and re-filters the data.
    /// </summary>
    public void SetFilter(ColumnFilterState filterState)
    {
        if (filterState.IsActive)
            _filters[filterState.ColumnName] = filterState;
        else
            _filters.Remove(filterState.ColumnName);

        ApplyFilters();
        UpdateFilterButtonStyles();
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
            _dataGrid.ItemsSource = _unfilteredData;
            return;
        }

        var filteredData = _unfilteredData.Where(item =>
        {
            foreach (var filter in _filters.Values)
            {
                if (filter.IsActive && !DataGridFilterService.MatchesFilter(item!, filter))
                    return false;
            }
            return true;
        }).ToList();

        _dataGrid.ItemsSource = filteredData;
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
                        Text = "\uE71C",
                        FontFamily = new FontFamily("Segoe MDL2 Assets"),
                        Foreground = hasActive
                            ? new SolidColorBrush(Color.FromRgb(0xFF, 0xD7, 0x00))
                            : (Brush)Application.Current.FindResource("ForegroundDimBrush")
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
