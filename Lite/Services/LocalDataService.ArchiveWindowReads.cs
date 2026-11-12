/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorLite.Services;

public sealed partial class LocalDataService
{
    /// <summary>
    /// #7256: the SQL of a windowed read with its <c>v_{table}</c> view swapped for <paramref name="relation"/>, the name
    /// <see cref="ReadWindowSkippingEmptyArchiveAsync{T}"/> hands the read: the hot table when the archive holds nothing at
    /// or after the window start, and the view itself otherwise (which leaves the text as it is). The view names are
    /// distinct whole words in the read's text, so a plain replace is exact.
    /// </summary>
    internal static string SwapArchiveView(string sql, string table, string relation)
    {
        var view = "v_" + table;
        return relation == view ? sql : sql.Replace(view, relation, StringComparison.Ordinal);
    }
}
