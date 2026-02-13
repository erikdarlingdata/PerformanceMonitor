/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Models;

/// <summary>
/// Represents the filter state for a single DataGrid column.
/// </summary>
public class ColumnFilterState
{
    public string ColumnName { get; set; } = string.Empty;
    public FilterOperator Operator { get; set; } = FilterOperator.Contains;
    public string Value { get; set; } = string.Empty;

    public bool IsActive => !string.IsNullOrEmpty(Value) ||
                            Operator == FilterOperator.IsEmpty ||
                            Operator == FilterOperator.IsNotEmpty;

    public string DisplayText
    {
        get
        {
            if (!IsActive) return string.Empty;

            return Operator switch
            {
                FilterOperator.Contains => $"Contains '{Value}'",
                FilterOperator.Equals => $"= '{Value}'",
                FilterOperator.NotEquals => $"!= '{Value}'",
                FilterOperator.GreaterThan => $"> {Value}",
                FilterOperator.GreaterThanOrEqual => $">= {Value}",
                FilterOperator.LessThan => $"< {Value}",
                FilterOperator.LessThanOrEqual => $"<= {Value}",
                FilterOperator.StartsWith => $"Starts with '{Value}'",
                FilterOperator.EndsWith => $"Ends with '{Value}'",
                FilterOperator.IsEmpty => "Is Empty",
                FilterOperator.IsNotEmpty => "Is Not Empty",
                _ => Value
            };
        }
    }

    public static string GetOperatorDisplayName(FilterOperator op)
    {
        return op switch
        {
            FilterOperator.Contains => "Contains",
            FilterOperator.Equals => "Equals (=)",
            FilterOperator.NotEquals => "Not Equals (!=)",
            FilterOperator.GreaterThan => "Greater Than (>)",
            FilterOperator.GreaterThanOrEqual => "Greater or Equal (>=)",
            FilterOperator.LessThan => "Less Than (<)",
            FilterOperator.LessThanOrEqual => "Less or Equal (<=)",
            FilterOperator.StartsWith => "Starts With",
            FilterOperator.EndsWith => "Ends With",
            FilterOperator.IsEmpty => "Is Empty",
            FilterOperator.IsNotEmpty => "Is Not Empty",
            _ => op.ToString()
        };
    }
}
