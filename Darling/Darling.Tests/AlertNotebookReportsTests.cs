/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4223: the notebook endpoint declares the three report metrics ("Collector Cost Digest", "Fleet Sweep
/// Rollup", "Analysis Singles Digest") as having no notebook at all — not the mechanical fallback, not an
/// authored template. Keyed on <c>AlertFamily.Reports</c> so this can never drift from
/// <see cref="TriageLinkReportsTests"/>'s own reports carve-out.
/// </summary>
public class AlertNotebookReportsTests
{
    [Theory]
    [InlineData("Collector Cost Digest")]
    [InlineData("Fleet Sweep Rollup")]
    [InlineData("Analysis Singles Digest")]
    public void IsDeclaredNoNotebook_IsTrue_ForEveryReportMetric(string metricName)
    {
        Assert.True(AlertNotebookEndpoint.IsDeclaredNoNotebook(metricName));
    }

    [Fact]
    public void IsDeclaredNoNotebook_IsFalse_ForANonReportMetric()
    {
        Assert.False(AlertNotebookEndpoint.IsDeclaredNoNotebook("Blocking Detected"));
    }

    [Fact]
    public void IsDeclaredNoNotebook_IsFalse_ForNull()
    {
        Assert.False(AlertNotebookEndpoint.IsDeclaredNoNotebook(null));
    }

    [Theory]
    [InlineData("Collector Cost Digest")]
    [InlineData("Fleet Sweep Rollup")]
    [InlineData("Analysis Singles Digest")]
    public void AuthoredTemplate_IsNull_ForEveryReportMetric_TheyAreDeclaredNotAuthored(string metricName)
    {
        Assert.Null(AlertNotebookEndpoint.AuthoredTemplate(metricName));
    }
}
