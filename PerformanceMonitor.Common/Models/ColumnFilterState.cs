/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// How a column filter's value list (#5565) reads its values: not at all, show only the listed values, or hide the
/// listed values. <see cref="Hide"/> keeps a value that first appears after a refresh visible; <see cref="ShowOnly"/>
/// keeps it out.
/// </summary>
public enum ColumnValueMode
{
    None,
    ShowOnly,
    Hide
}

/// <summary>
/// Represents the filter state for a single DataGrid column: the text match (operator and typed value) and the
/// value-list part (#5565). A row passes the column when it passes both.
/// </summary>
public class ColumnFilterState
{
    public string ColumnName { get; set; } = string.Empty;
    public FilterOperator Operator { get; set; } = FilterOperator.Contains;
    public string Value { get; set; } = string.Empty;

    /// <summary>The value-list part. <see cref="ColumnValueMode.None"/> means the list does not filter.</summary>
    public ColumnValueMode ValueMode { get; set; } = ColumnValueMode.None;

    /// <summary>
    /// The values the value part names (shown under <see cref="ColumnValueMode.ShowOnly"/>, hidden under
    /// <see cref="ColumnValueMode.Hide"/>), compared ordinal ignore-case.
    /// </summary>
    public HashSet<string> Values { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Whether the (Blanks) entry is named by the value part: shown under ShowOnly, hidden under Hide. A flag of its
    /// own, so a real value spelled "(Blanks)" stays an ordinary value.
    /// </summary>
    public bool ValueBlank { get; set; }

    /// <summary>The text match (operator and typed value) filters.</summary>
    public bool HasTextMatch => !string.IsNullOrEmpty(Value) ||
                                Operator == FilterOperator.IsEmpty ||
                                Operator == FilterOperator.IsNotEmpty;

    /// <summary>The value list filters. ShowOnly of nothing counts: it shows no rows.</summary>
    public bool HasValuePart => ValueMode != ColumnValueMode.None;

    /// <summary>The value part is ShowOnly with nothing ticked, so no row can pass it.</summary>
    public bool ShowsNoValues => ValueMode == ColumnValueMode.ShowOnly && Values.Count == 0 && !ValueBlank;

    public bool IsActive => HasTextMatch || HasValuePart;

    /// <summary>"hides 3 values" / "shows 2 values" for the value part (the blank entry counts as a value); empty when there is none.</summary>
    public string ValueDisplayText
    {
        get
        {
            if (!HasValuePart) return string.Empty;
            var count = Values.Count + (ValueBlank ? 1 : 0);
            var noun = count == 1 ? "value" : "values";
            return ValueMode == ColumnValueMode.Hide ? $"hides {count} {noun}" : $"shows {count} {noun}";
        }
    }

    public string DisplayText
    {
        get
        {
            if (!IsActive) return string.Empty;

            var parts = new List<string>();
            if (HasValuePart)
                parts.Add(ValueDisplayText);
            if (HasTextMatch)
                parts.Add(TextDisplayText);
            return string.Join(", ", parts);
        }
    }

    private string TextDisplayText
    {
        get
        {
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
