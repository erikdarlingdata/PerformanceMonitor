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
/// Which columns get no value list (#5565), decided by the column, not by length alone: query text, statement text,
/// plan and XML columns, prose (messages, definitions, details), and the display strings that stand in for a number
/// or a time (a "...Text" or "...Formatted" cell) keep the text match, because a list of them is a wall of near-unique
/// strings. The column-name rules read the bound property's name, which every grid's filter button already carries.
/// </summary>
public static class ColumnValueListColumns
{
    private static readonly string[] s_suffixes =
    {
        "Text", "Formatted", "Display", "Xml", "Plan", "Message", "Definition", "Preview", "Sql", "Detail", "Details",
        "Description", "Statement", "Query", "Command", "Json", "Graph",
        /* A script is statement-class text (index cleanup DDL, a plan-correction call), an error is a collector's
           message (it can name a login, a host, a path or the query), and an "...Info" cell is free text (#5565). */
        "Script", "Error", "Info"
    };

    private static readonly string[] s_fragments = { "QueryText", "QueryPlan", "SqlText", "StatementText", "BatchText", "PlanXml", "TextData" };

    /// <summary>
    /// The name suffixes and fragments, for the test that holds the web page's copy of this rule
    /// (<c>NO_LIST_SUFFIXES</c> and <c>NO_LIST_FRAGMENTS</c> in grid-value-filter.js) to the same list.
    /// </summary>
    public static IReadOnlyList<string> NameSuffixes => s_suffixes;

    /// <summary>See <see cref="NameSuffixes"/>.</summary>
    public static IReadOnlyList<string> NameFragments => s_fragments;

    /// <summary>The column's name marks it as one that never gets a value list.</summary>
    public static bool IsExcluded(string propertyName)
    {
        if (string.IsNullOrEmpty(propertyName))
            return true;

        foreach (var fragment in s_fragments)
            if (propertyName.Contains(fragment, StringComparison.OrdinalIgnoreCase))
                return true;

        foreach (var suffix in s_suffixes)
            if (propertyName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                return true;

        return false;
    }
}
