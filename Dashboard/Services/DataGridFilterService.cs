/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Provides shared filtering functionality for DataGrid controls.
    /// </summary>
    public static class DataGridFilterService
    {
        /* Numeric types that should use NumericFilterHelper */
        private static readonly HashSet<Type> NumericTypes = new HashSet<Type>
        {
            typeof(int), typeof(int?),
            typeof(long), typeof(long?),
            typeof(short), typeof(short?),
            typeof(decimal), typeof(decimal?),
            typeof(double), typeof(double?),
            typeof(float), typeof(float?)
        };

        /* Date types that should use DateFilterHelper */
        private static readonly HashSet<Type> DateTypes = new HashSet<Type>
        {
            typeof(DateTime), typeof(DateTime?)
        };

        /* Cache PropertyInfo lookups to avoid repeated reflection */
        private static readonly ConcurrentDictionary<(Type, string), PropertyInfo?> s_propertyCache = new();

        /// <summary>
        /// Gets a cached PropertyInfo for the given type and property name.
        /// </summary>
        private static PropertyInfo? GetCachedProperty(Type type, string propertyName)
        {
            return s_propertyCache.GetOrAdd((type, propertyName), key => key.Item1.GetProperty(key.Item2));
        }

        /// <summary>
        /// Checks if a single property value matches a filter string using type-aware filtering.
        /// Use this in custom filter implementations that need additional logic (e.g., ComboBox filters).
        /// </summary>
        /// <param name="item">The data item to check</param>
        /// <param name="propertyName">The property name to filter on</param>
        /// <param name="filterText">The filter text (supports operators for numeric/date types)</param>
        /// <returns>True if the value matches the filter, false otherwise</returns>
        public static bool MatchesFilter(object item, string propertyName, string filterText)
        {
            if (item == null || string.IsNullOrWhiteSpace(filterText))
                return true;

            var property = GetCachedProperty(item.GetType(), propertyName);
            if (property == null)
                return true;

            var propertyType = property.PropertyType;
            var rawValue = property.GetValue(item);

            // Use appropriate filter helper based on property type
            if (NumericTypes.Contains(propertyType))
            {
                return NumericFilterHelper.MatchesFilter(rawValue, filterText);
            }
            else if (DateTypes.Contains(propertyType))
            {
                return DateFilterHelper.MatchesFilter(rawValue, filterText);
            }
            else
            {
                // String filtering: case-insensitive contains with comma-separated OR
                var value = rawValue?.ToString()?.ToLowerInvariant() ?? string.Empty;
                var filterLower = filterText.ToLowerInvariant();
                var filterTerms = filterLower.Split(',').Select(t => t.Trim()).Where(t => !string.IsNullOrEmpty(t));
                return filterTerms.Any(term => value.Contains(term, StringComparison.Ordinal));
            }
        }

        /// <summary>
        /// Checks if an item matches a ColumnFilterState using operator-based filtering.
        /// Used by the new popup-based column filter UI.
        /// </summary>
        /// <param name="item">The data item to check</param>
        /// <param name="filter">The filter state containing operator and value</param>
        /// <returns>True if the value matches the filter, false otherwise</returns>
        public static bool MatchesFilter(object item, ColumnFilterState filter)
        {
            if (item == null || filter == null || !filter.IsActive)
                return true;

            var property = item.GetType().GetProperty(filter.ColumnName);
            if (property == null)
                return true;

            var rawValue = property.GetValue(item);

            // Handle IsEmpty/IsNotEmpty operators first
            if (filter.Operator == FilterOperator.IsEmpty)
                return IsValueEmpty(rawValue);
            if (filter.Operator == FilterOperator.IsNotEmpty)
                return !IsValueEmpty(rawValue);

            var stringValue = rawValue?.ToString() ?? string.Empty;
            var filterValue = filter.Value ?? string.Empty;

            // Split comma-separated values for text operators (e.g., "db1, db2")
            var terms = filterValue.Split(',')
                .Select(t => t.Trim())
                .Where(t => !string.IsNullOrEmpty(t))
                .ToArray();

            if (terms.Length == 0)
                return true;

            // Text operators match ANY term, NotEquals excludes ALL terms
            return filter.Operator switch
            {
                FilterOperator.Contains => terms.Any(t => stringValue.Contains(t, StringComparison.OrdinalIgnoreCase)),
                FilterOperator.Equals => terms.Any(t => CompareValues(rawValue, t, (a, b) => a == b)),
                FilterOperator.NotEquals => terms.All(t => CompareValues(rawValue, t, (a, b) => a != b)),
                FilterOperator.GreaterThan => CompareNumeric(rawValue, filterValue, (a, b) => a > b),
                FilterOperator.GreaterThanOrEqual => CompareNumeric(rawValue, filterValue, (a, b) => a >= b),
                FilterOperator.LessThan => CompareNumeric(rawValue, filterValue, (a, b) => a < b),
                FilterOperator.LessThanOrEqual => CompareNumeric(rawValue, filterValue, (a, b) => a <= b),
                FilterOperator.StartsWith => terms.Any(t => stringValue.StartsWith(t, StringComparison.OrdinalIgnoreCase)),
                FilterOperator.EndsWith => terms.Any(t => stringValue.EndsWith(t, StringComparison.OrdinalIgnoreCase)),
                _ => true
            };
        }

        /// <summary>
        /// Checks if a value is considered empty (null, empty string, or whitespace).
        /// </summary>
        private static bool IsValueEmpty(object? value)
        {
            if (value == null)
                return true;
            if (value is string str)
                return string.IsNullOrWhiteSpace(str);
            return false;
        }

        /// <summary>
        /// Compares values with case-insensitive string comparison for non-numeric types.
        /// </summary>
        private static bool CompareValues(object? rawValue, string filterValue, Func<int, int, bool> comparison)
        {
            if (rawValue == null)
                return comparison(0, 1); // null is "less than" any value

            var stringValue = rawValue.ToString() ?? string.Empty;

            // Try numeric comparison first
            if (TryParseNumeric(stringValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
            {
                return comparison(numericValue.CompareTo(filterNumeric), 0);
            }

            // Fall back to string comparison
            return comparison(string.Compare(stringValue, filterValue, StringComparison.OrdinalIgnoreCase), 0);
        }

        /// <summary>
        /// Compares values numerically. Returns true if the value can't be parsed (non-numeric values pass through).
        /// </summary>
        private static bool CompareNumeric(object? rawValue, string filterValue, Func<decimal, decimal, bool> comparison)
        {
            if (rawValue == null)
                return false;

            var stringValue = rawValue.ToString() ?? string.Empty;

            // Try to parse both values as decimals
            if (TryParseNumeric(stringValue, out var numericValue) && TryParseNumeric(filterValue, out var filterNumeric))
            {
                return comparison(numericValue, filterNumeric);
            }

            // If we can't parse both as numbers, the filter doesn't match
            return false;
        }

        /// <summary>
        /// Attempts to parse a string as a decimal, handling various formats.
        /// </summary>
        private static bool TryParseNumeric(string value, out decimal result)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                result = 0;
                return false;
            }

            // Remove common formatting characters (commas, percent signs, currency symbols)
            var cleanValue = value.Trim()
                .Replace(",", "")
                .Replace("%", "")
                .Replace("$", "")
                .Replace(" ", "");

            return decimal.TryParse(cleanValue, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
        }

        /// <summary>
        /// Applies a column-based filter to a DataGrid.
        /// Filter TextBoxes in column headers (inside StackPanels) with Tag set to property names
        /// are used to filter the data. Multiple terms separated by commas are OR-combined,
        /// while multiple columns are AND-combined.
        /// Numeric columns support operators: >, <, >=, <=, and ranges (10-20 or 10..20).
        /// Date columns support operators, relative dates (today, yesterday), and ranges.
        /// </summary>
        /// <param name="dataGrid">The DataGrid to filter</param>
        /// <param name="filterTextBox">The TextBox that triggered the filter (can be null)</param>
        public static void ApplyFilter(DataGrid? dataGrid, TextBox? filterTextBox)
        {
            if (dataGrid?.ItemsSource == null || filterTextBox == null)
                return;

            var view = CollectionViewSource.GetDefaultView(dataGrid.ItemsSource);
            if (view == null)
                return;

            view.Filter = item =>
            {
                if (item == null)
                    return false;

                // Get all filter TextBoxes for this DataGrid
                var filterBoxes = new List<(string PropertyName, string FilterText)>();
                foreach (var column in dataGrid.Columns)
                {
                    if (column.Header is StackPanel stackPanel)
                    {
                        var textBox = stackPanel.Children.OfType<TextBox>().FirstOrDefault();
                        if (textBox != null && !string.IsNullOrWhiteSpace(textBox.Text) && textBox.Tag is string propName)
                        {
                            filterBoxes.Add((propName, textBox.Text));
                        }
                    }
                }

                // If no filters, show all items
                if (filterBoxes.Count == 0)
                    return true;

                // Check all filters (AND logic)
                foreach (var (propertyName, filterText) in filterBoxes)
                {
                    var property = GetCachedProperty(item.GetType(), propertyName);
                    if (property != null)
                    {
                        var propertyType = property.PropertyType;
                        var rawValue = property.GetValue(item);

                        // Use appropriate filter helper based on property type
                        if (NumericTypes.Contains(propertyType))
                        {
                            // Numeric filtering: supports >, <, >=, <=, ranges
                            if (!NumericFilterHelper.MatchesFilter(rawValue, filterText))
                                return false;
                        }
                        else if (DateTypes.Contains(propertyType))
                        {
                            // Date filtering: supports operators, relative dates, ranges
                            if (!DateFilterHelper.MatchesFilter(rawValue, filterText))
                                return false;
                        }
                        else
                        {
                            // String filtering: case-insensitive contains with comma-separated OR
                            var value = rawValue?.ToString()?.ToLowerInvariant() ?? string.Empty;
                            var filterLower = filterText.ToLowerInvariant();
                            var filterTerms = filterLower.Split(',').Select(t => t.Trim()).Where(t => !string.IsNullOrEmpty(t));
                            var matchesAnyTerm = filterTerms.Any(term => value.Contains(term, StringComparison.Ordinal));

                            if (!matchesAnyTerm)
                                return false;
                        }
                    }
                }

                return true;
            };
        }
    }
}
