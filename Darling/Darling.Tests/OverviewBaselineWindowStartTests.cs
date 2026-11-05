/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7644: <c>get_overview_baselines</c> costs one step per local hour from <c>window_start</c> to now, so the start is bounded:
/// an old one (0001-01-01 is about 70 million steps) and a future one are refused with the <c>window_start</c> refusal, never
/// clamped, and the refusal comes before anything is read. The reads here pass a null data source: a read that got past the
/// check would throw on it, so a returned refusal proves no store read and no step was started.
/// </summary>
public sealed class OverviewBaselineWindowStartTests
{
    private static readonly DateTime Now = new(2026, 11, 4, 6, 30, 0, DateTimeKind.Utc);

    private static readonly TimeSpan Longest = TimeSpan.FromHours(DarlingWebOverviewBaselines.MaxWindowHours) + DarlingWebOverviewBaselines.WindowSlack;

    private static async Task<string> ReadAsync(string windowStart)
        => await DarlingWebOverviewBaselines.GetOverviewBaselines(null!, null, "any-server", windowStart, TestContext.Current.CancellationToken);

    private static void AssertRefusedOnWindowStart(string reply)
    {
        using var doc = JsonDocument.Parse(reply);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("window_start", doc.RootElement.GetProperty("parameter").GetString());
    }

    [Fact]
    public async Task AWindowStartOfYearOne_IsRefused_AndNoBaselineReadRuns()
        => AssertRefusedOnWindowStart(await ReadAsync("0001-01-01T00:00:00Z"));

    [Fact]
    public async Task AWindowStartPastTheLongestWindowPlusSlack_IsRefused_AndNoBaselineReadRuns()
        => AssertRefusedOnWindowStart(await ReadAsync(DateTime.UtcNow.AddDays(-400).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'")));

    [Fact]
    public async Task AFutureWindowStart_IsRefused_AndNotAnsweredAsNotEnoughHistory()
        => AssertRefusedOnWindowStart(await ReadAsync(DateTime.UtcNow.AddDays(2).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'")));

    [Fact]
    public void TheLongestWindowThePageCanAsk_StillAnswers()
    {
        /* The page's 30-day reach, asked at its own start, and the edge of the slack. */
        Assert.Null(DarlingWebOverviewBaselines.ValidateWindowStart(Now.AddHours(-DarlingWebOverviewBaselines.MaxWindowHours), Now));
        Assert.Null(DarlingWebOverviewBaselines.ValidateWindowStart(Now - Longest, Now));
        Assert.NotNull(DarlingWebOverviewBaselines.ValidateWindowStart(Now - Longest - TimeSpan.FromSeconds(1), Now));
    }

    [Fact]
    public void ANormalWindow_AndAClockAFewMinutesAhead_StillAnswer()
    {
        Assert.Null(DarlingWebOverviewBaselines.ValidateWindowStart(Now.AddHours(-24), Now));
        Assert.Null(DarlingWebOverviewBaselines.ValidateWindowStart(Now, Now));
        Assert.Null(DarlingWebOverviewBaselines.ValidateWindowStart(Now.AddMinutes(2), Now));
        Assert.NotNull(DarlingWebOverviewBaselines.ValidateWindowStart(Now.AddMinutes(6), Now));
    }
}
