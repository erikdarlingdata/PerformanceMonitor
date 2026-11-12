/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7724: the shared rule that decides what an empty read of an already-offline server answers. Darling's <c>EmptyAsync</c> and Lite's offline answer both
/// call it, so the window boundary is tested once, with no store.
/// </summary>
public sealed class OfflineEmptyAnswerBoundaryTests
{
    private const string Lead = "srv is offline: the store has had no successful collection from it since 2026-11-01 10:00 UTC, so nothing is arriving to read.";
    private const string OfflineAnswer = "{\"status\":\"unavailable\",\"message\":\"the offline answer\"}";

    private static readonly DateTime Newest = new(2026, 11, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = new(2026, 11, 5, 10, 0, 0, DateTimeKind.Utc);

    /// <summary>The newest collection is before the window start: nothing was collected in the window, so the answer is today's offline answer.</summary>
    [Fact]
    public void NewestCollectionBeforeTheWindowStart_KeepsTheOfflineAnswer()
    {
        var answer = McpHelpers.OfflineEmptyAnswer(Lead, Newest, WindowEnd, Newest.AddSeconds(1), OfflineAnswer);

        Assert.Equal(OfflineAnswer, answer);
    }

    /// <summary>A window with no start passed has no way to hold a collection, so it keeps today's answer too.</summary>
    [Fact]
    public void NoWindowStart_KeepsTheOfflineAnswer()
    {
        Assert.Equal(OfflineAnswer, McpHelpers.OfflineEmptyAnswer(Lead, Newest, WindowEnd, null, OfflineAnswer));
    }

    /// <summary>The newest collection is at the window start or after it: the window holds collection, so the answer is the collected-through one.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-72 * 3600)]
    public void NewestCollectionAtOrAfterTheWindowStart_SaysWhatWasCollectedThrough(int startOffsetSeconds)
    {
        var answer = McpHelpers.OfflineEmptyAnswer(Lead, Newest, WindowEnd, Newest.AddSeconds(startOffsetSeconds), OfflineAnswer);

        Assert.NotNull(answer);
        var root = JsonDocument.Parse(answer).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.Equal("2026-11-01T10:00:00.0000000Z", root.GetProperty("collected_through").GetString());
        var sentence = "Nothing was recorded through the last collection at 2026-11-01T10:00:00Z. Nothing after that is known.";
        Assert.Equal(sentence, root.GetProperty("collected_through_note").GetString());
        /* An MCP caller still learns the server is offline and why: the lead comes first, then the sentence. */
        Assert.Equal(Lead + " " + sentence, root.GetProperty("message").GetString());
    }

    /// <summary>An as_of window that ends before the newest collection (or at it) lies inside collected history: no offline answer, so the read's own answer stands.</summary>
    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public void WindowEndingAtOrBeforeTheNewestCollection_HasNoOfflineAnswer(int endOffsetSeconds)
    {
        var answer = McpHelpers.OfflineEmptyAnswer(Lead, Newest, Newest.AddSeconds(endOffsetSeconds), Newest.AddDays(-7), OfflineAnswer);

        Assert.Null(answer);
    }

    /// <summary>No collection at all: nothing to say it was collected through, so the offline answer stands.</summary>
    [Fact]
    public void NoCollectionAtAll_KeepsTheOfflineAnswer()
    {
        Assert.Equal(OfflineAnswer, McpHelpers.OfflineEmptyAnswer(Lead, null, WindowEnd, WindowEnd.AddDays(-7), OfflineAnswer));
    }

    /// <summary>With no lead the message is the sentence alone (a caller with no offline reason to give).</summary>
    [Fact]
    public void NoLead_MessageIsTheSentenceAlone()
    {
        var root = JsonDocument.Parse(McpHelpers.CollectedThroughStatus(Newest)).RootElement;

        Assert.Equal(root.GetProperty("collected_through_note").GetString(), root.GetProperty("message").GetString());
    }
}
