/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>get_store_job_runs without a store (#9440): the refusals that come before any read, the version rule and the trim job's config.</summary>
public sealed class StoreJobRunsToolTests
{
    private static async Task<JsonElement> RefusedAsync(int hours = 24, int limit = 50, string? asOf = null)
    {
        /* A refused argument is answered before the store is touched, so no data source is needed. */
        var json = await DarlingMcpStoreJobRunsTools.GetStoreJobRuns(null!, hours, null, null, limit, asOf, null, TestContext.Current.CancellationToken);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(DarlingMcpStoreJobRunsTools.MaxHours + 1)]
    public async Task AnOutOfRangeWindow_IsRefused_NotClamped(int hours)
    {
        var answer = await RefusedAsync(hours: hours);

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Equal("hours_back", answer.GetProperty("hints").GetProperty("parameter").GetString());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1_000_000)]
    public async Task AnOutOfRangeLimit_IsRefused_NotClamped(int limit)
    {
        var answer = await RefusedAsync(limit: limit);

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Equal("limit", answer.GetProperty("hints").GetProperty("parameter").GetString());
    }

    [Fact]
    public async Task AnAsOfInTheFuture_IsRefused()
    {
        var answer = await RefusedAsync(asOf: "2999-01-01T00:00:00Z");

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Equal("as_of", answer.GetProperty("hints").GetProperty("parameter").GetString());
    }

    [Theory]
    [InlineData("2.14.2", true)]
    [InlineData("2.9.0", true)]
    [InlineData("2.15.0", false)]
    [InlineData("2.32.1", false)]
    [InlineData("3.0.0", false)]
    [InlineData("2.15.0-dev", false)]
    [InlineData("not a version", false)]
    public void OnlyATimescaleDbOlderThan215_IsBelowTheFirstVersionWithHistory(string version, bool below) =>
        Assert.Equal(below, DarlingStoreJobRunsReader.BelowFirstHistoryVersion(version));

    [Fact]
    public void TheTrimJobsConfig_IsReadForItsRetentionFigures()
    {
        var retention = DarlingStoreJobRunsReader.ParseRetention(
            "{\"drop_after\": \"1 month\", \"max_failures_per_job\": 1000, \"max_successes_per_job\": 250}");

        Assert.NotNull(retention);
        Assert.Equal("1 month", retention!.DropAfter);
        Assert.Equal(1000, retention.MaxFailuresPerJob);
        Assert.Equal(250, retention.MaxSuccessesPerJob);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("[1, 2]")]
    [InlineData("not json")]
    public void ATrimConfigThatIsNotAnObject_GivesNoRetention(string? config) =>
        Assert.Null(DarlingStoreJobRunsReader.ParseRetention(config));

    [Theory]
    [InlineData(false, null, "unfinished")]
    [InlineData(true, true, "succeeded")]
    [InlineData(true, false, "failed")]
    [InlineData(true, null, "failed")]
    public void AnOutcome_FollowsTheEndAndTheSuccessFlag(bool finished, bool? succeeded, string expected)
    {
        var started = System.DateTime.UtcNow;
        var run = new DarlingStoreJobRunsReader.Run(1, 1000, "_timescaledb_functions", "policy_compression", "t", started, finished ? started.AddSeconds(1) : null, succeeded, null, true, null);

        Assert.Equal(expected, DarlingMcpStoreJobRunsTools.Outcome(run));
    }

    /// <summary>An emoji whose two UTF-16 units straddle the cut point is cut whole, not between its units: a lone surrogate would be written
    /// as U+FFFD by the JSON writer (#9484). The pair sits across chars 499 and 500, so a cut at the bare index splits it.</summary>
    [Fact]
    public void AnErrorCutInsideASurrogatePair_KeepsTheWholePairOrDropsIt()
    {
        var max = DarlingStoreJobRunsReader.MaxErrorMessageChars;
        var text = new string('a', max - 1) + "\uD83D\uDE00" + new string('b', 50);
        Assert.True(char.IsHighSurrogate(text[max - 1]) && char.IsLowSurrogate(text[max]));

        var cut = DarlingStoreJobRunsReader.CutError(text)!;

        Assert.True(cut.Length <= max);
        Assert.False(cut.Length > 0 && char.IsHighSurrogate(cut[^1]), "the cut ends on a lone high surrogate");
        var written = JsonDocument.Parse(JsonSerializer.Serialize(new { error = cut })).RootElement.GetProperty("error").GetString();
        Assert.DoesNotContain('\uFFFD', written!);
        Assert.Equal(new string('a', max - 1), cut);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("short")]
    public void AnErrorWithinTheLimit_IsReturnedAsItIs(string? message) =>
        Assert.Equal(message, DarlingStoreJobRunsReader.CutError(message));
}
