/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>The desktop heatmap tooltip count wording (#7881): "1 query", never "1 queries".</summary>
public class QueryHeatmapCellTextTests
{
    [Theory]
    [InlineData(1L, "1 query")]
    [InlineData(5L, "5 queries")]
    [InlineData(1234L, "1,234 queries")]
    public void CountText_UsesSingularOnlyForOne(long count, string expected)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            Assert.Equal(expected, QueryHeatmapCellText.CountText(count));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }
}
