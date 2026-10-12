/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Shared host/apply plumbing for the per-grid column-filter <see cref="ColumnFilterPopup"/>. Extracted from the
/// byte-identical quartet (EnsureFilterPopup + FilterButton_Click + FilterPopup_FilterApplied +
/// FilterPopup_FilterCleared) that lived in Lite's and Darling's ServerTab / FinOps partials. Each host owns its
/// grid -> filter-manager dictionary and hands it here; the thin XAML-wired <c>FilterButton_Click</c> forwards to
/// <see cref="HandleFilterButtonClick"/>. Internal — reached by both apps via Ui's InternalsVisibleTo.
/// </summary>
internal sealed class ColumnFilterPopupController
{
    private readonly IReadOnlyDictionary<DataGrid, IDataGridFilterManager> _managers;
    private Popup? _popup;
    private ColumnFilterPopup? _content;
    private DataGrid? _currentGrid;

    internal ColumnFilterPopupController(IReadOnlyDictionary<DataGrid, IDataGridFilterManager> managers)
        => _managers = managers;

    /// <summary>
    /// Opens the column-filter popup for the clicked filter button: walks up to the owning DataGrid, seeds the
    /// popup with any existing filter state for that column, and anchors it under the button.
    /// </summary>
    internal void HandleFilterButtonClick(object sender)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;

        /* Walk up visual tree to find the parent DataGrid */
        var dataGrid = DataGridHelpers.FindParentDataGridFromElement(button);
        if (dataGrid == null || !_managers.TryGetValue(dataGrid, out var manager)) return;

        _currentGrid = dataGrid;

        EnsureFilterPopup();

        /* Rewire events to the current grid */
        _content!.FilterApplied -= OnFilterApplied;
        _content.FilterCleared -= OnFilterCleared;
        _content.ClearAllRequested -= OnClearAllRequested;
        _content.FilterApplied += OnFilterApplied;
        _content.FilterCleared += OnFilterCleared;
        _content.ClearAllRequested += OnClearAllRequested;

        /* Initialize with existing filter state */
        manager.Filters.TryGetValue(columnName, out var existingFilter);
        _content.Initialize(columnName, existingFilter, manager.GetValueCatalog(columnName), manager.AnyFilterActive);

        _popup!.PlacementTarget = button;
        _popup.IsOpen = true;
    }

    private void EnsureFilterPopup()
    {
        if (_popup == null)
        {
            _content = new ColumnFilterPopup();
            _popup = new Popup
            {
                Child = _content,
                StaysOpen = false,
                Placement = PlacementMode.Bottom,
                AllowsTransparency = true
            };
        }
    }

    private void OnFilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_popup != null)
            _popup.IsOpen = false;

        if (_currentGrid != null && _managers.TryGetValue(_currentGrid, out var manager))
        {
            manager.SetFilter(e.FilterState);
        }
    }

    /// <summary>#5565: "Clear all filters" removes the grid's filters and its stored copy.</summary>
    private void OnClearAllRequested(object? sender, EventArgs e)
    {
        if (_popup != null)
            _popup.IsOpen = false;

        if (_currentGrid != null && _managers.TryGetValue(_currentGrid, out var manager))
        {
            manager.ClearAllFilters();
        }
    }

    private void OnFilterCleared(object? sender, EventArgs e)
    {
        if (_popup != null)
            _popup.IsOpen = false;
    }
}
