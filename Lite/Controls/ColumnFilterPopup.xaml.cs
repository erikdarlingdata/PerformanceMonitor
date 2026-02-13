/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Controls;

public partial class ColumnFilterPopup : UserControl
{
    private string _columnName = string.Empty;
    private bool _suppressEvents = false;

    public event EventHandler<FilterAppliedEventArgs>? FilterApplied;
    public event EventHandler? FilterCleared;

    public ColumnFilterPopup()
    {
        InitializeComponent();
        PopulateOperatorComboBox();
    }

    private void PopulateOperatorComboBox()
    {
        OperatorComboBox.Items.Clear();

        foreach (FilterOperator op in Enum.GetValues(typeof(FilterOperator)))
        {
            OperatorComboBox.Items.Add(new ComboBoxItem
            {
                Content = ColumnFilterState.GetOperatorDisplayName(op),
                Tag = op
            });
        }

        OperatorComboBox.SelectedIndex = 0;
    }

    public void Initialize(string columnName, ColumnFilterState? existingFilter)
    {
        _suppressEvents = true;
        _columnName = columnName;
        HeaderText.Text = $"Filter: {columnName}";

        if (existingFilter != null && existingFilter.IsActive)
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

        ValueTextBox.Focus();
        ValueTextBox.SelectAll();
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

    private void ValueTextBox_KeyDown(object sender, KeyEventArgs e)
    {
        if (e.Key == Key.Enter)
        {
            ApplyFilter();
            e.Handled = true;
        }
        else if (e.Key == Key.Escape)
        {
            FilterCleared?.Invoke(this, EventArgs.Empty);
            e.Handled = true;
        }
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

        FilterApplied?.Invoke(this, new FilterAppliedEventArgs { FilterState = filterState });
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
