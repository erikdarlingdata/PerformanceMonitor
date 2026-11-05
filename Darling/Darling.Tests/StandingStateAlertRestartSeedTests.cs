// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7068: a lasting-state self-alert is announced once, not again every time the service starts. The "already announced"
/// stamp is in memory, so a restart in the middle of a standing condition read it as a new entry (seven servers down for
/// 21 hours each fired Collection Stopped again at the next start). The first sweep of a process that finds the condition
/// now seeds the stamp from the open fire row in the alert history, closed by the alert's own resolution rows.
/// </summary>
public sealed class StandingStateAlertRestartSeedTests
{
    private const int ServerId = 424242;
    private const string Name = "STATE-SRV";
    private static readonly DateTime Start = new(2026, 11, 1, 0, 14, 0, DateTimeKind.Utc);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static string ServerKey(int id) => id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private sealed class Rig
    {
        public Rig()
        {
            Evaluator = new DarlingSelfAlertEvaluator(
                Settings, Deliverer, History, _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now);
        }

        public DarlingSelfAlertTests.FakeHistoryStore History { get; } = new();
        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new() { CooldownMinutes = 5 };
        public AlertEngineTests.RecordingDeliverer Deliverer { get; } = new();
        public DateTime Now { get; set; } = Start;
        public DarlingSelfAlertEvaluator Evaluator { get; }
        public int Fires => Deliverer.Outcomes.Count;

        /// <summary>The history holds this open fire for the given server key and metric, and nothing else.</summary>
        public void OpenFire(string serverKey, string metric, string? contextJson = null) =>
            History.OpenFireFor = (s, m) => s == serverKey && m == metric
                ? new AlertHistoryFire(Start.AddHours(-21), 0, contextJson)
                : null;
    }

    public enum Case
    {
        CollectionStopped,
        AgentNotRunning,
    }

    private static string MetricOf(Case c) => c == Case.CollectionStopped ? "Collection Stopped" : DarlingSelfAlertEvaluator.AgentDownMetric;

    private static string[] ResolutionsOf(Case c) => c == Case.CollectionStopped
        ? new[] { "Collection Resumed", "Server Restored" }
        : new[] { "Agent Restarted" };

    private static Task DownAsync(Rig rig, Case c, int serverId = ServerId) => c == Case.CollectionStopped
        ? rig.Evaluator.ApplyCollectionStoppedAsync(serverId, Name, true, "No successful collection in 21 hours", Ct)
        : rig.Evaluator.ApplyAgentNotRunningAsync(serverId, Name, false, true, Ct);

    private static Task UpAsync(Rig rig, Case c, int serverId = ServerId) => c == Case.CollectionStopped
        ? rig.Evaluator.ApplyCollectionStoppedAsync(serverId, Name, false, string.Empty, Ct)
        : rig.Evaluator.ApplyAgentNotRunningAsync(serverId, Name, true, true, Ct);

    [Theory]
    [InlineData(Case.CollectionStopped)]
    [InlineData(Case.AgentNotRunning)]
    public async Task AConditionStillStanding_AfterARestart_WithAnOpenFireInTheHistory_SendsNothing(Case c)
    {
        var rig = new Rig();
        rig.OpenFire(ServerKey(ServerId), MetricOf(c));

        await DownAsync(rig, c);
        await DownAsync(rig, c);

        Assert.Equal(0, rig.Fires);
        var asked = Assert.Single(rig.History.OpenFireQuestions);
        Assert.Equal(ServerKey(ServerId), asked.ServerKey);
        Assert.Equal(ResolutionsOf(c), asked.Titles);
    }

    [Theory]
    [InlineData(Case.CollectionStopped)]
    [InlineData(Case.AgentNotRunning)]
    public async Task AConditionWithNoOpenFire_ClearedBeforeTheRestart_IsAnnounced_OnceAcrossSweeps(Case c)
    {
        var rig = new Rig();

        await DownAsync(rig, c);
        await DownAsync(rig, c);

        Assert.Equal(1, rig.Fires);
    }

    [Theory]
    [InlineData(Case.CollectionStopped)]
    [InlineData(Case.AgentNotRunning)]
    public async Task AConditionSeenClearFirst_AndDownLater_IsAnnounced_WhateverTheHistoryHolds(Case c)
    {
        /* It cleared while the service was down, so nothing wrote the resolution row: the open row is not this condition's. */
        var rig = new Rig();
        rig.OpenFire(ServerKey(ServerId), MetricOf(c));

        await UpAsync(rig, c);
        rig.Now += TimeSpan.FromHours(1);
        await DownAsync(rig, c);

        Assert.Equal(1, rig.Fires);
    }

    [Theory]
    [InlineData(Case.CollectionStopped)]
    [InlineData(Case.AgentNotRunning)]
    public async Task AStandingConditionThatClears_AndComesBack_IsAnnouncedAgain(Case c)
    {
        var rig = new Rig();
        rig.OpenFire(ServerKey(ServerId), MetricOf(c));

        await DownAsync(rig, c);
        Assert.Equal(0, rig.Fires);
        await UpAsync(rig, c);
        rig.Now += TimeSpan.FromHours(1);
        await DownAsync(rig, c);

        Assert.Equal(1, rig.Fires);
    }

    [Theory]
    [InlineData(Case.CollectionStopped)]
    [InlineData(Case.AgentNotRunning)]
    public async Task ADifferentServers_Condition_IsAnnounced_WhenOnlyAnotherServerHasAnOpenFire(Case c)
    {
        var rig = new Rig();
        rig.OpenFire(ServerKey(ServerId), MetricOf(c));

        await DownAsync(rig, c, serverId: ServerId + 1);

        Assert.Equal(1, rig.Fires);
    }

    [Theory]
    [InlineData(Case.CollectionStopped)]
    [InlineData(Case.AgentNotRunning)]
    public async Task AFailedHistoryRead_FallsBackToAnnouncing_AndIsNotReadAgainEverySweep(Case c)
    {
        var rig = new Rig();
        rig.History.OpenFireThrows = true;

        await DownAsync(rig, c);
        await DownAsync(rig, c);

        Assert.Equal(1, rig.Fires);
        Assert.Single(rig.History.OpenFireQuestions);
    }

    /* ----- an alert with severity bands: Store Job Over Cadence, a Warning below the schedule interval and a Critical at it ----- */

    private const string CadenceKey = "storejob:7";

    private static Task CadenceSweepAsync(Rig rig, double percent) =>
        rig.Evaluator.ApplyStoreJobCadenceAsync(
            new[] { new StoreJobCadenceReading(7, "refresh", (long)(percent * 600), 60_000) }, Ct);

    private static string SeverityContext(AlertSeverityLevel level) =>
        AlertContextSerializer.Serialize(new AlertContext { SeverityOverride = level })!;

    [Theory]
    [InlineData(AlertSeverityLevel.Warning, 60.0)]
    [InlineData(AlertSeverityLevel.Critical, 120.0)]
    public async Task ABandedCondition_StandingAtTheSameLevel_AfterARestart_SendsNothing(AlertSeverityLevel level, double percent)
    {
        var rig = new Rig();
        rig.OpenFire(CadenceKey, "Store Job Over Cadence", SeverityContext(level));

        await CadenceSweepAsync(rig, percent);
        await CadenceSweepAsync(rig, percent);

        Assert.Equal(0, rig.Fires);
        Assert.Equal(new[] { "Store Job Cadence Recovered" }, Assert.Single(rig.History.OpenFireQuestions).Titles);
    }

    [Fact]
    public async Task ABandedCondition_ThatWorsens_AfterARestart_IsAnnounced_AsTheRise()
    {
        var rig = new Rig();
        rig.OpenFire(CadenceKey, "Store Job Over Cadence", SeverityContext(AlertSeverityLevel.Warning));

        await CadenceSweepAsync(rig, 60.0);
        Assert.Equal(0, rig.Fires);
        await CadenceSweepAsync(rig, 120.0);

        Assert.Equal(1, rig.Fires);
        Assert.Equal(AlertSeverityLevel.Critical, Assert.Single(rig.Deliverer.Outcomes).Severity);
    }

    [Fact]
    public async Task ABandedCondition_SeenHealthyFirst_AndBackLater_IsAnnounced()
    {
        var rig = new Rig();
        rig.OpenFire(CadenceKey, "Store Job Over Cadence", SeverityContext(AlertSeverityLevel.Warning));

        await CadenceSweepAsync(rig, 5.0);
        rig.Now += TimeSpan.FromHours(1);
        await CadenceSweepAsync(rig, 60.0);

        Assert.Equal(1, rig.Fires);
    }

    [Fact]
    public async Task ABandedCondition_OnADifferentJob_IsAnnounced()
    {
        var rig = new Rig();
        rig.OpenFire("storejob:8", "Store Job Over Cadence", SeverityContext(AlertSeverityLevel.Warning));

        await CadenceSweepAsync(rig, 60.0);

        Assert.Equal(1, rig.Fires);
    }
}
