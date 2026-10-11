// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5646: a collector that recovered no longer reads WARNING on the 7-day failure rate. A minimum-runs floor,
/// a recovery rule (the newest 3 runs in a row succeeded) and the note that carries the last failure time.
/// </summary>
public sealed class CollectorHealthRecoveryTests
{
    private static string Band(long total, long errors, long trailing, double hoursSinceSuccess = 0.1,
        double hoursSinceRun = 0.1, long abandoned = 0, string? note = null) =>
        CollectorHealthClassifier.Classify(
            total, total - errors, errors, 0, 0, abandoned, hoursSinceSuccess, hoursSinceRun, 5, note, trailing);

    [Fact]
    public void TheFloorAndTheRecoveryConstants_ArePinned()
    {
        Assert.Equal(10, CollectorHealthClassifier.MinimumRunsForFailureRate);
        Assert.Equal(3, CollectorHealthClassifier.RecoveryTrailingSuccessRuns);
    }

    [Fact]
    public void Floor_NineRuns_NewestRunSucceeded_IsHealthy_AndTenRunsIsWarning()
    {
        /* 3 errors of 9 is 33%, and 3 of 10 is 30%: both over 20%. The newest run succeeded (1 in a row), so
           only the run count decides. */
        Assert.Equal(CollectorHealthClassifier.Healthy, Band(9, 3, 1));
        Assert.Equal(CollectorHealthClassifier.Warning, Band(10, 3, 1));
    }

    [Fact]
    public void Floor_FewRuns_NewestRunFailed_StaysWarning()
    {
        /* Not turned healthy by the floor: its newest run did not succeed. */
        Assert.Equal(CollectorHealthClassifier.Warning, Band(4, 2, 0));
        Assert.Equal(CollectorHealthClassifier.Warning, Band(2, 1, 0));
    }

    [Fact]
    public void Floor_EveryRunFailed_IsNeverHealthy()
    {
        /* No success ever: the success clock (999 hours) bands it FAILING before the rate is looked at. */
        Assert.Equal(CollectorHealthClassifier.Failing, Band(3, 3, 0, hoursSinceSuccess: 999));
    }

    [Fact]
    public void Recovery_ThreeSuccessesAfterFailures_IsHealthy_WithTheNoteAndTheLastFailureTime()
    {
        Assert.Equal(CollectorHealthClassifier.Healthy, Band(40, 12, 3));

        var failure = new DateTime(2026, 10, 9, 14, 30, 5, DateTimeKind.Unspecified);
        var note = CollectorHealthClassifier.FormatRecoveredNote(40, 12, 3, failure);
        Assert.NotNull(note);
        Assert.Contains("recovered", note, StringComparison.Ordinal);
        Assert.Contains("2026-10-09 14:30:05", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Recovery_TwoSuccessesAfterFailures_StaysWarning_AndCarriesNoNote()
    {
        Assert.Equal(CollectorHealthClassifier.Warning, Band(40, 12, 2));
        Assert.Null(CollectorHealthClassifier.FormatRecoveredNote(40, 12, 2, DateTime.UtcNow));
    }

    [Fact]
    public void Recovery_AlternatingCollector_NeverHasThreeInARow_StaysWarning()
    {
        /* 12 runs, 6 errors, the newest run a success: the streak is 1, never 3. */
        Assert.Equal(CollectorHealthClassifier.Warning, Band(12, 6, 1));
        Assert.Equal(CollectorHealthClassifier.Warning, Band(12, 6, 0));
    }

    [Fact]
    public void Recovery_NoNoteWhenNothingWasSuppressed()
    {
        Assert.Null(CollectorHealthClassifier.FormatRecoveredNote(40, 0, 3, null));
        Assert.Null(CollectorHealthClassifier.FormatRecoveredNote(40, 2, 3, DateTime.UtcNow));
        Assert.Null(CollectorHealthClassifier.FormatRecoveredNote(9, 3, 3, DateTime.UtcNow));
    }

    [Fact]
    public void Staleness_StillWins_OverTheRecovery()
    {
        Assert.Equal(CollectorHealthClassifier.Failing, Band(40, 12, 3, hoursSinceSuccess: 999, hoursSinceRun: 0.1));
        Assert.Equal(CollectorHealthClassifier.Stopped, Band(40, 12, 3, hoursSinceSuccess: 999, hoursSinceRun: 999));
    }

    [Fact]
    public void Recovery_DoesNotSilence_TheAbandonRate_OrThePartialDatabaseNote()
    {
        Assert.Equal(CollectorHealthClassifier.Warning, Band(40, 12, 3, abandoned: 5));
        Assert.Equal(
            CollectorHealthClassifier.Warning,
            Band(40, 12, 3, note: string.Format(System.Globalization.CultureInfo.InvariantCulture, PartialDatabaseFailureNote.Format, 5, 8, "a, b", "login failed")));
    }

    [Fact]
    public void ARowFromPlainAggregates_NoNewestRuns_KeepsTheOldBanding()
    {
        /* trailingSuccessRuns 0: a row with no newest-run streak (the newest run failed, or the fleet roll-up
           found no row that needs it). Eight runs, two errors was WARNING before and is again. */
        Assert.Equal(
            CollectorHealthClassifier.Warning,
            CollectorHealthClassifier.Classify(8, 6, 2, 0, 0, 0, 0.1, 0.1, 5, null, 0));
    }

    [Fact]
    public void FormatCollectionNote_PutsTheRecoveredSentenceBesideTheRunNote()
    {
        Assert.Equal("recovered: x", CollectorHealthClassifier.FormatCollectionNote(null, 0, 10, null, false, "recovered: x"));
        Assert.Equal(
            "recovered: x; n (all 10 runs)",
            CollectorHealthClassifier.FormatCollectionNote("n", 10, 10, null, false, "recovered: x"));
        Assert.Equal(string.Empty, CollectorHealthClassifier.FormatCollectionNote(null, 0, 10));
    }
}
