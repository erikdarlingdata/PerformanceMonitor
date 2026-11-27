/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9101. Lite's collection health row bands through the shared ladder, so a window of successes that stored
/// nothing, followed only by <c>EXTENSION_MISSING</c> runs, bands <c>EXTENSION_MISSING</c> here as it does in
/// Darling. Each test builds a run history and reduces it to the counts and newest instants the health read
/// selects.
/// </summary>
public sealed class ExtensionMissingAfterEmptySuccessBandTests
{
    private sealed record Run(DateTime At, string Status, int Rows);

    private static IEnumerable<Run> Hourly(DateTime now, int fromHoursAgo, int toHoursAgo, string status, int rows = 0)
    {
        for (var hoursAgo = fromHoursAgo; hoursAgo >= toHoursAgo; hoursAgo--)
        {
            yield return new Run(now.AddHours(-hoursAgo).AddMinutes(-3), status, rows);
        }
    }

    private static List<Run> EmptySuccessesThenExtensionMissing(DateTime now, int lastSuccessHoursAgo = 72, int successRows = 0) =>
        Hourly(now, lastSuccessHoursAgo + 72, lastSuccessHoursAgo, "SUCCESS", successRows)
            .Concat(Hourly(now, lastSuccessHoursAgo - 1, 0, CollectorRuntimePrecondition.ExtensionMissingStatus))
            .ToList();

    private static DateTime? Newest(IEnumerable<Run> runs) =>
        runs.Select(r => (DateTime?)r.At).DefaultIfEmpty(null).Max();

    private static string Band(IReadOnlyCollection<Run> runs) => new CollectorHealthRow
    {
        CollectorName = "pg_plan_capture",
        TotalRuns = runs.Count,
        SuccessCount = runs.Count(r => r.Status == "SUCCESS"),
        ErrorCount = runs.Count(r => r.Status == "ERROR"),
        ExtensionMissingCount = runs.Count(r => r.Status == CollectorRuntimePrecondition.ExtensionMissingStatus),
        LastSuccessTime = Newest(runs.Where(r => r.Status == "SUCCESS")),
        LastRunTime = Newest(runs),
        LastNonSkipTime = Newest(runs.Where(r => !CollectorRuntimePrecondition.IsNamedSkip(r.Status))),
        LastProductiveTime = Newest(runs.Where(r => r.Rows > 0)),
    }.HealthStatus;

    [Fact]
    public void EmptySuccessesOlderThanADay_FollowedOnlyByExtensionMissing_BandExtensionMissing()
    {
        Assert.Equal(CollectorHealthClassifier.ExtensionMissing, Band(EmptySuccessesThenExtensionMissing(DateTime.UtcNow)));
    }

    [Fact]
    public void ACollectorThatStoredRows_ThenLostItsExtension_KeepsItsBand()
    {
        var now = DateTime.UtcNow;

        Assert.Equal(CollectorHealthClassifier.Failing, Band(EmptySuccessesThenExtensionMissing(now, successRows: 5)));
        Assert.Equal(
            CollectorHealthClassifier.Warning,
            Band(EmptySuccessesThenExtensionMissing(now, lastSuccessHoursAgo: 3, successRows: 5)));

        /* The same two histories with nothing stored are the setup step, not a failure. */
        Assert.Equal(CollectorHealthClassifier.ExtensionMissing, Band(EmptySuccessesThenExtensionMissing(now)));
        Assert.Equal(
            CollectorHealthClassifier.ExtensionMissing,
            Band(EmptySuccessesThenExtensionMissing(now, lastSuccessHoursAgo: 3)));
    }

    [Fact]
    public void AnEmptySuccessAfterTheExtensionMissingRuns_IsStillASuccess()
    {
        var now = DateTime.UtcNow;
        var runs = EmptySuccessesThenExtensionMissing(now);

        Assert.Equal(
            CollectorHealthClassifier.Healthy,
            Band(runs.Append(new Run(now.AddMinutes(-1), "SUCCESS", 0)).ToList()));

        /* Without that newest success the window is the skip alone. */
        Assert.Equal(CollectorHealthClassifier.ExtensionMissing, Band(runs));
    }
}
