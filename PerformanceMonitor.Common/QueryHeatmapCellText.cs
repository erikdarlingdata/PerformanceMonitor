/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The count wording of a query heatmap cell tooltip, shared by the Lite chart and the Darling Viewer chart.
    /// A cell counts distinct queries (#7881), so a cell holding one steady query is common and must read
    /// "1 query", the same as the web page's heatmapCellText.
    /// </summary>
    public static class QueryHeatmapCellText
    {
        public static string CountText(long count) =>
            string.Create(CultureInfo.CurrentCulture, $"{count:N0}") + (count == 1 ? " query" : " queries");
    }
}
