/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The judged count in the <c>get_client_timeouts</c> answer (#7061): <c>count_judged</c> and <c>rate_per_minute_judged</c> must be what
/// <see cref="AttentionEvaluator.Evaluate"/> judges for the same reading, with the SSMS and VS Code names, the sampled events under the
/// 1 s duration floor and Azure Data Studio's metadata statements all left out. The Lite twin is <c>ClientTimeoutsJudgedCountTests</c>.
/// </summary>
public sealed class ClientTimeoutsJudgedCountTests
{
    [Fact]
    public void TheJudgedCountAndRate_MatchWhatTheEvaluatorJudges_AndTheTwoNewEventCountsAreRight()
    {
        /* 190 sampled events: 100 from etl (10 under the floor), 40 from azdata (3 under the floor, 12 metadata statements, the two disjoint),
           30 from dbatools (2 under the floor, 10 SMO metadata reads, the two disjoint, #8877) and 20 from SSMS IntelliSense (its 4 short ones are already left out by name, so they are not counted twice). */
        var reading = new AttentionReading(
            TotalCount: 1600, CoveredMinutes: 5, RunCount: 6, NewestRunTime: new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc),
            Groups: new[]
            {
                new AttentionGroup("sales", "etl", 100, ShortSamples: 10),
                new AttentionGroup("sales", "azdata", 40, ShortSamples: 3, AzdataMetadataSamples: 12),
                new AttentionGroup("sales", AttentionWindowSql.DbatoolsClientApp, 30, ShortSamples: 2, DbatoolsMetadataSamples: 10),
                new AttentionGroup("sales", "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense", 20, ShortSamples: 4),
            });

        var finding = AttentionEvaluator.Evaluate(reading, warningPerMinute: 10, criticalPerMinute: 20);
        Assert.NotNull(finding);

        var created = new DateTime(2026, 10, 1, 11, 59, 0, DateTimeKind.Utc);
        var samples = new[]
        {
            new ClientTimeoutSample(created, created, "sales", "etl", "ws-1", 51, 4_000, "fp-a", "SELECT 1", BelowDurationFloor: true),
            new ClientTimeoutSample(created, created, "sales", "azdata", "ws-2", 52, 90_000, "fp-b", "create table #sver", AzdataMetadata: true),
            new ClientTimeoutSample(created, created, "sales", AttentionWindowSql.DbatoolsClientApp, "ws-4", 54, 90_000, "fp-d", "SELECT name FROM sys.schemas WHERE name = @_msparam_0", DbatoolsMetadata: true),
            new ClientTimeoutSample(created, created, "sales", "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense", "ws-3", 53, 90_000, "fp-c", "SELECT 3"),
        };

        var payload = JsonSerializer.SerializeToElement(
            ClientTimeoutsRead.Payload(reading, AttentionWindow.Empty, samples, limit: 10, statementTextLength: 200));
        var excluded = payload.GetProperty("excluded_from_alert");

        Assert.Equal(Math.Round(finding.JudgedCount, 1, MidpointRounding.AwayFromZero), excluded.GetProperty("count_judged").GetDouble());
        Assert.Equal(finding.RatePerMinute, excluded.GetProperty("rate_per_minute_judged").GetDouble());
        /* Guard the expectation itself: (100 - 10) + (40 - 3 - 12) + (30 - 2 - 10) = 133 of 190 sampled events pass, so 1120 of 1600 in 5 minutes. */
        Assert.Equal(1120.0, excluded.GetProperty("count_judged").GetDouble());
        Assert.Equal(224.0, excluded.GetProperty("rate_per_minute_judged").GetDouble());
        Assert.Equal(20, excluded.GetProperty("sampled_events").GetInt64());
        Assert.Equal(15, excluded.GetProperty("below_duration_floor_events").GetInt64());
        Assert.Equal(12, excluded.GetProperty("azdata_metadata_events").GetInt64());
        Assert.Equal(10, excluded.GetProperty("dbatools_metadata_events").GetInt64());

        /* The per-event excluded flag is the name rule only: a short or azdata metadata event carries its own flag instead. */
        var events = payload.GetProperty("sampled_events");
        Assert.False(events[0].GetProperty("excluded").GetBoolean());
        Assert.True(events[0].GetProperty("below_duration_floor").GetBoolean());
        Assert.False(events[1].GetProperty("excluded").GetBoolean());
        Assert.True(events[1].GetProperty("azdata_metadata").GetBoolean());
        Assert.False(events[2].GetProperty("excluded").GetBoolean());
        Assert.True(events[2].GetProperty("dbatools_metadata").GetBoolean());
        Assert.False(events[1].GetProperty("dbatools_metadata").GetBoolean());
        Assert.True(events[3].GetProperty("excluded").GetBoolean());
    }

    [Fact]
    public void AMatchedUnderFloorSample_IsJudged_NotLeftOut_AndTheAnswerListsTheCall()
    {
        /* 100 sampled events from etl: 10 under the floor with no captured call, 6 under the floor that match one (judged), 84 over the floor. */
        var reading = new AttentionReading(
            TotalCount: 500, CoveredMinutes: 5, RunCount: 6, NewestRunTime: new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc),
            Groups: new[] { new AttentionGroup("sales", "etl", 100, ShortSamples: 10, MatchedSamples: 6) });

        var finding = AttentionEvaluator.Evaluate(reading, warningPerMinute: 10, criticalPerMinute: 20);
        Assert.NotNull(finding);
        Assert.Equal(6, finding.MatchedCalls);
        Assert.Equal(450.0, finding.JudgedCount);

        var created = new DateTime(2026, 10, 1, 11, 59, 0, DateTimeKind.Utc);
        var window = AttentionWindow.Empty with
        {
            MatchedCallList = new[] { new ClientTimeoutMatchedCall(created, created, "sales", "etl", "ws-1", 51, 30_000_000, "dbo.GetOrders: EXEC dbo.GetOrders") },
        };
        var payload = JsonSerializer.SerializeToElement(ClientTimeoutsRead.Payload(reading, window, [], limit: 10, statementTextLength: 200));
        var excluded = payload.GetProperty("excluded_from_alert");

        Assert.Equal(10, excluded.GetProperty("below_duration_floor_events").GetInt64());
        Assert.Equal(6, excluded.GetProperty("matched_calls").GetInt64());
        Assert.Equal(450.0, excluded.GetProperty("count_judged").GetDouble());
        var call = payload.GetProperty("matched_calls")[0];
        Assert.Equal(30.0, call.GetProperty("elapsed_seconds").GetDouble());
        Assert.Equal("sales", call.GetProperty("database_name").GetString());
    }
}
