/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of #3010. The defect was measured on a Darling fleet, but the SHAPE is Lite's too: the same
/// single retained <c>last_error</c> slot, read by a <c>get_collection_health</c> tool of the same name,
/// with the same absent timestamp. #3010 says so explicitly — "the same treatment belongs on the Lite side,
/// which has the same single-slot shape".
///
/// <para>Parity matters here for a specific reason rather than as a habit: a caller who learns to check
/// <c>denied_since_last_success</c> on one SKU must not silently find it missing on the other, because the
/// absence reads as "no denial" rather than as "this SKU cannot tell you".</para>
/// </summary>
public sealed class LastErrorCurrencyTests
{
    /// <summary>
    /// The row an operator reads: the fossil shape says not-current, the counterfactual says current, and
    /// the band is identical across both — this is a reporting fix, not a banding change.
    /// </summary>
    [Fact]
    public void TheFossilReadsAsNotCurrent_TheCounterfactualReadsAsCurrent_AndTheBandIsUnchanged()
    {
        CollectorHealthRow Row(DateTime denied, DateTime success) => new()
        {
            CollectorName = "deadlocks",
            TotalRuns = 99_841,
            SuccessCount = 83_956,
            ErrorCount = 0,
            PermissionDeniedCount = 15_885,
            LastError = "The user does not have permission to perform this action.",
            LastDeniedTime = denied,
            LastErrorTime = denied,
            LastSuccessTime = success,
            LastRunTime = DateTime.UtcNow.AddMinutes(-1),
        };

        var fossil = Row(DateTime.UtcNow.AddDays(-6), DateTime.UtcNow.AddMinutes(-1));
        var current = Row(DateTime.UtcNow.AddMinutes(-1), DateTime.UtcNow.AddMinutes(-30));

        Assert.False(fossil.DeniedSinceLastSuccess, "every denial in the window predates a later success");
        Assert.True(current.DeniedSinceLastSuccess);

        /* Opposite verdicts on currency, identical band: #3010 is a reporting defect and the banding chain
           is deliberately untouched. A 15.9% denial share would have banded both. */
        Assert.Equal(CollectorHealthClassifier.Healthy, fossil.HealthStatus);
        Assert.Equal(CollectorHealthClassifier.Healthy, current.HealthStatus);

        /* And the fossil stays readable. The point was never to hide it, it is to date it. */
        Assert.NotNull(fossil.LastError);
        Assert.True(fossil.LastSuccessTime > fossil.LastDeniedTime);
    }

    /// <summary>
    /// The read has to project the column, or <c>LastDeniedTime</c> defaults to null and the answer
    /// silently becomes "never denied" — reassuring, wrong, and compiling.
    /// </summary>
    [Fact]
    public void TheCollectionHealthRead_SelectsTheNewestDenialInstant()
    {
        var sql = LocalDataService.CollectionHealthSql;

        Assert.Contains("AS last_denied_time", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN status = 'PERMISSIONS' THEN collection_time END)", sql, StringComparison.Ordinal);

        /* Its own aggregate over PERMISSIONS alone, not a reuse of last_error_time, which is a MAX over
           ERROR and PERMISSIONS together. The sibling below is the count this read demonstrably carries. */
        Assert.Contains("AS permission_denied_count", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The WIRING pin, because no behavioural test reaches it: the three fields are emitted by an anonymous
    /// object inside an MCP tool method. A mutation removing one left the whole suite green on the Darling
    /// side, so both SKUs pin it by source.
    /// </summary>
    [Theory]
    [InlineData("last_error_at = r.LastErrorTime?.ToString(\"o\")")]
    [InlineData("last_denied_at = r.LastDeniedTime?.ToString(\"o\")")]
    [InlineData("denied_since_last_success = r.DeniedSinceLastSuccess")]
    public void TheHealthTool_DatesTheLastErrorSlot(string wiring)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHealthTools.cs"));

        /* The anchor the get_collection_health row is built from, and the POSITIVE CONTROL for the span
           assertion below — a check that passed by matching nothing cannot hide behind it. */
        Assert.Contains("last_error = r.LastError,", source, StringComparison.Ordinal);
        Assert.Contains(wiring, source, StringComparison.Ordinal);

        var start = source.IndexOf("last_error = r.LastError,", StringComparison.Ordinal);
        var end = source.IndexOf("last_note = r.LastNote,", StringComparison.Ordinal);
        Assert.True(end > start, "the get_collection_health row's field order moved - this pin needs re-anchoring");
        Assert.Contains(wiring, source[start..end], StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
