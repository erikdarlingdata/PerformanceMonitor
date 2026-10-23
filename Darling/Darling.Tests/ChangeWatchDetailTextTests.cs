/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#6551: a Change Watch request's detail names the database and the read (plan cache or Query Store) of every query measure.</summary>
public sealed class ChangeWatchDetailTextTests
{
    [Fact]
    public void AQueryMeasure_InTheDetail_CarriesItsDatabaseAndTheReadItCameFrom()
    {
        var first = new DateTime(2026, 10, 22, 10, 0, 0, DateTimeKind.Utc);
        var watch = new ChangeWatchRow(
            Guid.NewGuid(), 1, "sql1", first, first, first.AddHours(1), "claimed", null, null, null, null,
            new List<ChangeWatchChange> { new("k", "plan_force", first, "plan 9 of query 5 in Sales: forced", "Sales", 5, 9) });
        var measures = new List<ChangeWatchMeasure>
        {
            new("query_avg_duration_ms:0xABC", "ms", 10, 40, null, true, "read from the plan cache (query_stats), query_hash 0xABC in database Sales; 20 -> 25 executions"),
            new("query_avg_duration_ms:Sales:5", "ms", 10, 40, null, true, "read from Query Store: query_id 5 in database Sales; 20 -> 25 executions"),
            new("query_avg_duration_ms:0xDEF", "ms", 10, 11, null, false, "not moved, so not named"),
        };

        var text = ChangeWatchEvaluator.DetailText(watch, measures);

        Assert.Contains("query_avg_duration_ms:0xABC before 10 after 40 week earlier n/a ms;", text, StringComparison.Ordinal);
        Assert.Contains("query_avg_duration_ms:0xABC: read from the plan cache (query_stats), query_hash 0xABC in database Sales;", text, StringComparison.Ordinal);
        Assert.Contains("query_avg_duration_ms:Sales:5: read from Query Store: query_id 5 in database Sales;", text, StringComparison.Ordinal);
        Assert.DoesNotContain("0xDEF", text, StringComparison.Ordinal);
    }
    /// <summary>#6551 review: the 990-character cut drops the end of the text, so the notes of five moved measures (each a long sentence) must not push a measure line out.</summary>
    [Fact]
    public void LongNotes_OnFiveMovedMeasures_NeverPushAMeasureOutOfTheDetail()
    {
        var first = new DateTime(2026, 10, 22, 10, 0, 0, DateTimeKind.Utc);
        var watch = new ChangeWatchRow(
            Guid.NewGuid(), 1, "sql1", first, first, first.AddHours(1), "claimed", null, null, null, null,
            new List<ChangeWatchChange> { new("k", "plan_force", first, "plan 9 of query 5 in Sales: forced", "Sales", 5, 9) });
        var longNote = "read from Query Store: query_id 5 in database Sales; " + new string('x', 150);
        var measures = new List<ChangeWatchMeasure>();
        for (var i = 1; i <= 5; i++)
        {
            measures.Add(new($"query_avg_duration_ms:Sales:{i}", "ms", 10, 40 + i, null, true, longNote));
        }

        var text = ChangeWatchEvaluator.DetailText(watch, measures);

        Assert.True(text.Length <= 990, text.Length.ToString());
        for (var i = 1; i <= 5; i++)
        {
            Assert.Contains($"query_avg_duration_ms:Sales:{i} before 10 after {40 + i} week earlier n/a ms;", text, StringComparison.Ordinal);
        }

        /* The notes are what the cut takes: the text ends in the cut marker, and the first note still made it in. */
        Assert.EndsWith("...", text, StringComparison.Ordinal);
        Assert.Contains("query_avg_duration_ms:Sales:1: read from Query Store", text, StringComparison.Ordinal);
    }
}
