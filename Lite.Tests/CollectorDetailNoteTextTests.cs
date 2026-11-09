/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// get_collection_health's <c>collector_detail_note</c> is built once in Common for both products. Every count in it agrees with
/// its verb, and the sentence for the rows the reply lists as not collected (an engine that never runs them, Always On off, a
/// Query Store that is a readable secondary) names its own count: one row "says why", two or more rows "each" say why.
/// </summary>
public sealed class CollectorDetailNoteTextTests
{
    [Theory]
    [InlineData(1, "(the row says why)", "(each row says why)")]
    [InlineData(2, "(each row says why)", "(the row says why)")]
    public void TheNotCollectedRowsSentence_AgreesItsParentheticalWithTheCount(int notCollectedCount, string expected, string notExpected)
    {
        var note = CollectorDetailNote.Build(compactCount: 3, shownCount: 4, partialCount: 1, notCollectedCount);
        Assert.Contains($"{CountText.Plural(notCollectedCount, "more row")} below", note, StringComparison.Ordinal);
        Assert.Contains(expected, note, StringComparison.Ordinal);
        Assert.DoesNotContain(notExpected, note, StringComparison.Ordinal);
    }

    [Fact]
    public void OneCollectorThatNeedsALook_ReadsNeeds_AndNoNotCollectedRowsMeansNoSentenceAboutThem()
    {
        var note = CollectorDetailNote.Build(compactCount: 3, shownCount: 4, partialCount: 1, notCollectedCount: 0);
        Assert.Contains("1 needs a look", note, StringComparison.Ordinal);
        Assert.DoesNotContain("not collected on this server", note, StringComparison.Ordinal);
        Assert.DoesNotContain("says why", note, StringComparison.Ordinal);
    }
}
