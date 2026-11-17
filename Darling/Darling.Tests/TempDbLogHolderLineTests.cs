/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8881 second step: the one line the three tempdb screens show beside the chart, the "Log Used" series values, and the get_tempdb_trend
/// fields the web page draws them from. Plain behaviour: given a holder, the exact words; given none, no line.
/// </summary>
public sealed class TempDbLogHolderLineTests
{
    private static readonly DateTime AsOf = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);

    private static TempDbLogHolder Holder(
        int? session = 87, string? login = "app_user", string? program = "MyApp", TimeSpan? open = null, long? bytes = 3435973837, string? reason = "NOTHING") =>
        new(AsOf, session, login, program, open is null ? null : AsOf - open.Value, bytes, reason);

    [Fact]
    public void AHolder_ReadsAsOneSentence_WithSessionLoginProgramAgeAndLogUsed()
    {
        var line = TempDbLogHolderLine.Build(Holder(open: TimeSpan.FromMinutes(14)));

        Assert.Equal("Oldest tempdb log transaction: session 87, login app_user, program MyApp, open 14 minutes, 3.2 GB of log used.", line);
    }

    [Fact]
    public void ASessionlessHolder_IsSaidToHaveNoSession_NotDropped()
    {
        var line = TempDbLogHolderLine.Build(Holder(session: null, login: null, program: null, open: TimeSpan.FromMinutes(14)));

        Assert.Equal("Oldest open tempdb transaction has no session (a system or orphaned transaction), open 14 minutes, 3.2 GB of log used.", line);
    }

    [Fact]
    public void NoHolder_MeansNoLine()
    {
        Assert.Null(TempDbLogHolderLine.Build(null));
        Assert.Null(TempDbLogHolderLine.Build(Holder(session: null, login: null, program: null, bytes: null)));
    }

    [Theory]
    [InlineData("NOTHING", false)]
    [InlineData("nothing", false)]
    [InlineData("", false)]
    [InlineData("   ", false)]
    [InlineData(null, false)]
    [InlineData("ACTIVE_TRANSACTION", true)]
    public void TheHoldupReason_IsShownWhenItIsNotNothing(string? reason, bool shown)
    {
        var line = TempDbLogHolderLine.Build(Holder(open: TimeSpan.FromMinutes(3), reason: reason))!;

        Assert.Equal(shown, line.Contains("Log truncation is held up by", StringComparison.Ordinal));
        if (shown)
        {
            Assert.EndsWith(" Log truncation is held up by ACTIVE_TRANSACTION.", line, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void MissingParts_AreLeftOutOrSaidUnknown_NeverInvented()
    {
        var line = TempDbLogHolderLine.Build(Holder(login: null, program: " ", open: null, bytes: null));

        Assert.Equal("Oldest tempdb log transaction: session 87, login unknown, program unknown.", line);
    }

    [Theory]
    [InlineData(1, "open 1 second,")]
    [InlineData(45, "open 45 seconds,")]
    [InlineData(60, "open 1 minute,")]
    [InlineData(119, "open 1 minute,")]
    [InlineData(2 * 3600, "open 2 hours,")]
    [InlineData(2 * 3600 + 5 * 60, "open 2 hours 5 minutes,")]
    [InlineData(3 * 86400 + 4 * 3600 + 59, "open 3 days 4 hours,")]
    public void TheTransactionAge_ReadsInTheLargestUnitsThatApply(int seconds, string expected)
    {
        Assert.Contains(expected, TempDbLogHolderLine.Build(Holder(open: TimeSpan.FromSeconds(seconds))), StringComparison.Ordinal);
    }

    [Fact]
    public void ABeginTimeAfterTheCollection_ReadsAsZeroSeconds_NeverNegative()
    {
        var holder = new TempDbLogHolder(AsOf, 5, "a", "b", AsOf.AddSeconds(30), 1, null);

        Assert.Contains("open 0 seconds,", TempDbLogHolderLine.Build(holder), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0L, "under 1 MB of log used")]
    [InlineData(1048575L, "under 1 MB of log used")]
    [InlineData(1048576L, "1 MB of log used")]
    [InlineData(891289600L, "850 MB of log used")]
    [InlineData(1073741824L, "1 GB of log used")]
    [InlineData(3435973837L, "3.2 GB of log used")]
    public void TheLogUsed_ReadsInMbBelowAGigabyteAndGbFromIt(long bytes, string expected)
    {
        Assert.Contains(expected, TempDbLogHolderLine.Build(Holder(open: TimeSpan.FromMinutes(1), bytes: bytes)), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSeries_IsGapsWhereThereIsNoReading_NeverZero_AndAbsentWhenThereIsNoneAtAll()
    {
        var values = TempDbLogSeries.Values(new double?[] { null, null, 120, null, 480 });

        Assert.NotNull(values);
        Assert.True(double.IsNaN(values![0]));
        Assert.True(double.IsNaN(values[1]));
        Assert.Equal(120d, values[2]);
        Assert.True(double.IsNaN(values[3]));
        Assert.Equal(480d, values[4]);
        Assert.Equal(480d, TempDbLogSeries.Max(values));

        Assert.Null(TempDbLogSeries.Values(new double?[] { null, null }));
        Assert.Null(TempDbLogSeries.Values(Array.Empty<double?>()));
    }

    [Fact]
    public void TheTrendPayload_CarriesPeakLogUsedMbPerPoint_AndTheRestOfTheLogOncePerWindow()
    {
        var points = new List<TempDbBucketPoint>
        {
            new(AsOf.AddMinutes(-10), 1, 2, 3, 6, 100, 7, 4, 5, 51, 1.5, 70, 700.04),
            new(AsOf.AddMinutes(-5), 1, 2, 3, 6, 100, 7, 4, 5, 51, 1.5),
        };
        var holder = Holder(open: TimeSpan.FromMinutes(14), reason: "ACTIVE_TRANSACTION") with { LogTotalMb = 1000.04, LogReuseWaitDesc = "ACTIVE_TRANSACTION" };

        using var doc = JsonDocument.Parse(TrendPayloads.TempDbTrend("server-a", 24, points, 5, requested: false, autoBudget: 100, newestHolder: holder));
        var root = doc.RootElement;

        Assert.Equal(TempDbLogHolderLine.Build(holder), root.GetProperty("log_holder_note").GetString());
        Assert.Equal(1000.0, root.GetProperty("log_total_mb").GetDouble());
        Assert.Equal("ACTIVE_TRANSACTION", root.GetProperty("log_reuse_wait_desc").GetString());
        Assert.Equal("ACTIVE_TRANSACTION", root.GetProperty("log_truncation_holdup_reason").GetString());
        Assert.Equal(87, root.GetProperty("log_holder_session_id").GetInt32());
        Assert.Equal("app_user", root.GetProperty("log_holder_login_name").GetString());
        var trend = root.GetProperty("trend");
        Assert.Equal(700.0, trend[0].GetProperty("peak_log_used_mb").GetDouble());
        Assert.Equal(70.0, trend[0].GetProperty("peak_log_used_percent").GetDouble());
        /* The size, reuse wait and holder repeat on nearly every point, so they ride once on the window and never on a point. */
        Assert.False(trend[0].TryGetProperty("log_total_mb", out _));
        Assert.False(trend[0].TryGetProperty("log_reuse_wait_desc", out _));
        Assert.False(trend[0].TryGetProperty("log_truncation_holdup_reason", out _));
        Assert.False(trend[0].TryGetProperty("log_holder_session_id", out _));
        Assert.False(trend[0].TryGetProperty("log_holder_login_name", out _));
        /* A bucket with no reading leaves the field out, so a server with no log reading pays nothing for it against the response budget. */
        Assert.False(trend[1].TryGetProperty("peak_log_used_mb", out _));
        Assert.False(trend[1].TryGetProperty("peak_log_used_percent", out _));
    }

    [Fact]
    public void TheTrendPayload_HasANullNote_WhenTheNewestCollectionHasNoHolder()
    {
        var points = new List<TempDbBucketPoint> { new(AsOf.AddMinutes(-5), 1, 2, 3, 6, 100, 7, 4, 5, 51, 1.5) };

        using var noHolder = JsonDocument.Parse(TrendPayloads.TempDbTrend("server-a", 24, points, 5, requested: false, autoBudget: 100, newestHolder: Holder(session: null, login: null, program: null, bytes: null)));
        using var neverRead = JsonDocument.Parse(TrendPayloads.TempDbTrend("server-a", 24, points, 5, requested: false, autoBudget: 100));

        Assert.Equal(JsonValueKind.Null, noHolder.RootElement.GetProperty("log_holder_note").ValueKind);
        Assert.Equal(JsonValueKind.Null, neverRead.RootElement.GetProperty("log_holder_note").ValueKind);
        Assert.Equal(JsonValueKind.Null, neverRead.RootElement.GetProperty("log_holder_session_id").ValueKind);
        Assert.Equal(JsonValueKind.Null, neverRead.RootElement.GetProperty("log_total_mb").ValueKind);
    }
}
