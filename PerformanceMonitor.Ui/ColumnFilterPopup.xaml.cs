/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

public partial class ColumnFilterPopup : UserControl
{
    private string _columnName = string.Empty;
    private bool _suppressEvents = false;
    private ColumnFilterState? _existing;
    private ColumnValueListModel? _list;
    private bool _storedValuesCleared;

    public event EventHandler<FilterAppliedEventArgs>? FilterApplied;
    public event EventHandler? FilterCleared;

    /// <summary>#5565: the user chose "Clear all filters" for the grid.</summary>
    public event EventHandler? ClearAllRequested;

    public ColumnFilterPopup()
    {
        InitializeComponent();
        PopulateOperatorComboBox();
    }

    private void PopulateOperatorComboBox()
    {
        OperatorComboBox.Items.Clear();

        foreach (FilterOperator op in Enum.GetValues<FilterOperator>())
        {
            OperatorComboBox.Items.Add(new ComboBoxItem
            {
                Content = ColumnFilterState.GetOperatorDisplayName(op),
                Tag = op
            });
        }

        OperatorComboBox.SelectedIndex = 0;
    }

    /// <summary>The value list the popup shows now, or null for a column with no list. Exposed for tests.</summary>
    internal ColumnValueListModel? ListModel => _list;

    /// <param name="columnName">The column's bound property name.</param>
    /// <param name="existingFilter">The column's filter now, if any.</param>
    /// <param name="valueCatalog">The column's values (#5565), or null when it gets no list.</param>
    /// <param name="gridHasFilters">The grid has an active filter, so "Clear all filters" is offered.</param>
    public void Initialize(string columnName, ColumnFilterState? existingFilter, ColumnValueCatalog? valueCatalog = null, bool gridHasFilters = false)
    {
        _suppressEvents = true;
        _columnName = columnName;
        _existing = existingFilter;
        _storedValuesCleared = false;
        HeaderText.Text = $"Filter: {columnName}";

        if (_list is not null)
            _list.TicksChanged -= OnTicksChanged;
        _list = valueCatalog is null ? null : new ColumnValueListModel(valueCatalog, existingFilter);
        SearchTextBox.Text = string.Empty;
        ValueListPanel.Visibility = _list is null ? Visibility.Collapsed : Visibility.Visible;
        OperatorLabel.Text = _list is null ? "Operator:" : "Text match - operator:";
        ShowStoredValues();
        ClearAllButton.Visibility = gridHasFilters ? Visibility.Visible : Visibility.Collapsed;
        if (_list is not null)
        {
            ValuesListBox.ItemsSource = _list.Entries;
            _list.TicksChanged += OnTicksChanged;
            OnTicksChanged(_list, EventArgs.Empty);
        }
        else
        {
            ValuesListBox.ItemsSource = null;
        }

        if (existingFilter != null && existingFilter.HasTextMatch)
        {
            for (int i = 0; i < OperatorComboBox.Items.Count; i++)
            {
                if (OperatorComboBox.Items[i] is ComboBoxItem item && item.Tag is FilterOperator op)
                {
                    if (op == existingFilter.Operator)
                    {
                        OperatorComboBox.SelectedIndex = i;
                        break;
                    }
                }
            }

            ValueTextBox.Text = existingFilter.Value;
        }
        else
        {
            OperatorComboBox.SelectedIndex = 0;
            ValueTextBox.Text = string.Empty;
        }

        UpdateValueVisibility();
        _suppressEvents = false;

        if (_list is not null)
        {
            SearchTextBox.Focus();
        }
        else
        {
            ValueTextBox.Focus();
            ValueTextBox.SelectAll();
        }
    }

    private void OnTicksChanged(object? sender, EventArgs e)
    {
        if (_list is null) return;
        SelectAllCheckBox.IsChecked = _list.SelectAllState;
        var note = _list.Note;
        NoteText.Text = note ?? string.Empty;
        NoteText.Visibility = note is null ? Visibility.Collapsed : Visibility.Visible;
    }

    private void SearchTextBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        if (_suppressEvents || _list is null) return;
        _list.Search = SearchTextBox.Text;
    }

    private void SelectAllCheckBox_Click(object sender, RoutedEventArgs e)
    {
        if (_list is null) return;
        _list.ToggleSelectAll();
        SelectAllCheckBox.IsChecked = _list.SelectAllState;
    }

    /// <summary>
    /// A column that gets no list now (its longest value grew past the limit, say) but holds a stored value filter
    /// says so, read-only, and offers to clear it: otherwise the filter would stay active with nothing to change it.
    /// </summary>
    private void ShowStoredValues()
    {
        var show = _list is null && !_storedValuesCleared && _existing is { HasValuePart: true };
        StoredValuesPanel.Visibility = show ? Visibility.Visible : Visibility.Collapsed;
        StoredValuesText.Text = show ? $"Value filter kept on this column: {_existing!.ValueDisplayText}. This column offers no list now, so it can only be cleared." : string.Empty;
    }

    private void ClearStoredValuesButton_Click(object sender, RoutedEventArgs e)
    {
        _storedValuesCleared = true;
        ShowStoredValues();
        // At once, as the web page's clear does: the popup closes on a click outside (StaysOpen = false) and on Escape,
        // and a clear that waited for Apply would be lost with it, leaving the value filter hiding rows.
        ApplyFilter();
    }

    private void ClearAllButton_Click(object sender, RoutedEventArgs e)
    {
        ClearAllRequested?.Invoke(this, EventArgs.Empty);
    }

    /// <summary>Esc closes the popup from anywhere in it; Enter applies unless a button has focus.</summary>
    private void Popup_PreviewKeyDown(object sender, KeyEventArgs e)
    {
        if (e.Key == Key.Escape)
        {
            FilterCleared?.Invoke(this, EventArgs.Empty);
            e.Handled = true;
        }
        else if (e.Key == Key.Enter && e.OriginalSource is not Button)
        {
            ApplyFilter();
            e.Handled = true;
        }
    }

    private void OperatorComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressEvents) return;
        UpdateValueVisibility();
    }

    private void UpdateValueVisibility()
    {
        var selectedOp = GetSelectedOperator();

        bool showValue = selectedOp != FilterOperator.IsEmpty &&
                        selectedOp != FilterOperator.IsNotEmpty;

        ValueLabel.Visibility = showValue ? Visibility.Visible : Visibility.Collapsed;
        ValueTextBox.Visibility = showValue ? Visibility.Visible : Visibility.Collapsed;
    }

    private FilterOperator GetSelectedOperator()
    {
        if (OperatorComboBox.SelectedItem is ComboBoxItem item && item.Tag is FilterOperator op)
        {
            return op;
        }
        return FilterOperator.Contains;
    }

    private void ApplyButton_Click(object sender, RoutedEventArgs e)
    {
        ApplyFilter();
    }

    private void ApplyFilter()
    {
        var filterState = new ColumnFilterState
        {
            ColumnName = _columnName,
            Operator = GetSelectedOperator(),
            Value = ValueTextBox.Text.Trim()
        };
        BuildValuePart(filterState);

        FilterApplied?.Invoke(this, new FilterAppliedEventArgs { FilterState = filterState });
    }

    /// <summary>
    /// The value part of the filter: the ticks when the column has a list and a tick was changed. Otherwise whatever
    /// the filter already held (#5565: the mode is chosen when the list changes, so a text-only Apply leaves a value
    /// filter that today's rows have drifted away from as it was; a column whose list is not offered now, with a value
    /// filter stored earlier, keeps it unless the reader cleared it).
    /// </summary>
    private void BuildValuePart(ColumnFilterState state)
    {
        if (_list is not null && _list.TicksDirty)
        {
            _list.ApplyTo(state);
        }
        else if (_existing is not null && !_storedValuesCleared)
        {
            state.ValueMode = _existing.ValueMode;
            state.Values = new HashSet<string>(_existing.Values, StringComparer.OrdinalIgnoreCase);
            state.ValueBlank = _existing.ValueBlank;
        }
    }

    private void ClearButton_Click(object sender, RoutedEventArgs e)
    {
        ValueTextBox.Text = string.Empty;
        OperatorComboBox.SelectedIndex = 0;

        var filterState = new ColumnFilterState
        {
            ColumnName = _columnName,
            Operator = FilterOperator.Contains,
            Value = string.Empty
        };

        FilterCleared?.Invoke(this, EventArgs.Empty);
        FilterApplied?.Invoke(this, new FilterAppliedEventArgs { FilterState = filterState });
    }
}

public class FilterAppliedEventArgs : EventArgs
{
    public ColumnFilterState FilterState { get; set; } = new ColumnFilterState();
}
