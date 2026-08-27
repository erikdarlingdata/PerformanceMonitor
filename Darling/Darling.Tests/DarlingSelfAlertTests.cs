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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Stage 4 of the Darling control plane — the SERVICE's self-alerts
/// (<see cref="DarlingSelfAlertEvaluator"/>). The pure detection (<c>IsCollectionStopped</c>) and the
/// EDGE behavior (fire once on the transition, not every sweep; write the resolution history row on
/// recovery) are tested ungated against a recording deliverer + fake history store + a controllable
/// clock; the two collection_log reads run against a real Postgres gated on <c>DARLING_TEST_PG</c>
/// (skipped otherwise), mirroring <see cref="DarlingAlertingTests"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingSelfAlertTests
{
    private const int ServerId = 424242;
    private const string Key = "424242";
    private const string Name = "SELF-ALERT-SRV";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ---------------- fakes ---------------- */

    private sealed class FakeSettings : IAlertEngineSettings
    {
        public bool AlertsEnabled { get; set; } = true;
        public bool CpuEnabled { get; set; }
        public bool BlockingEnabled { get; set; } = true;
        public bool DeadlockEnabled { get; set; } = true;
        public bool PoisonWaitEnabled { get; set; }
        public bool LongRunningQueryEnabled { get; set; }
        public bool TempDbSpaceEnabled { get; set; }
        public bool LowDiskEnabled { get; set; }
        public bool LongRunningJobEnabled { get; set; }
        public bool FailedJobEnabled { get; set; }
        public bool PvsEnabled { get; set; }
        public bool DatabaseStateEnabled { get; set; }
        public bool ForcePlanFailureEnabled { get; set; } = true;
        public int CpuThresholdPercent { get; set; } = 80;
        public int BlockingCountThreshold { get; set; } = 1;
        public int BlockingWaitSecondsThreshold { get; set; }
        public int DeadlockCountThreshold { get; set; } = 1;
        public int PoisonWaitThresholdMs { get; set; } = 500;
        public int LongRunningQueryThresholdMinutes { get; set; } = 30;
        public int LongRunningQueryMaxResults { get; set; } = 5;
        public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
        public bool LongRunningQueryExcludeWaitFor { get; set; } = true;
        public bool LongRunningQueryExcludeBackups { get; set; } = true;
        public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;
        public bool LongRunningQueryExcludeCdc { get; set; } = true;
        public int TempDbSpaceThresholdPercent { get; set; } = 80;
        public int LowDiskThresholdPercent { get; set; } = 10;
        public int LowDiskThresholdGb { get; set; } = 5;
        /* #2107: the previously-hardcoded knobs, at their shipped defaults. */
        public int DiskCriticalFreePercent { get; set; } = 3;
        public int DiskCriticalFreeGb { get; set; } = 2;
        public int SelfDiskFreeWarnPercent { get; set; } = 10;
        public int CollectionStaleMinutes { get; set; } = 30;
        public int CollectionFailureThreshold { get; set; } = 10;
        public int PvsThresholdPercent { get; set; } = 40;
        public int PvsFloorGb { get; set; } = 1;

        /* #2349: OFF in the fakes so existing expectations are untouched. */
        public bool FileGrowthEnabled { get; set; }
        public int FileGrowthRiseMb { get; set; } = 10240;
        public int FileGrowthVolumePercent { get; set; } = 60;
        public int FileGrowthLookbackMinutes { get; set; } = 60;
        public int LongRunningJobMultiplier { get; set; } = 3;
        public int FailedJobLookbackMinutes { get; set; } = 60;
        public int CooldownMinutes { get; set; } = 5;
        public List<string> ExcludedDatabasesList { get; } = new();
        public IReadOnlyList<string> ExcludedDatabases => ExcludedDatabasesList;
        public CpuAlertMode CpuAlertMode { get; set; } = CpuAlertMode.TotalServer;
    }

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeHistoryStore : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }

    /// <summary>
    /// Minimal ILogger that records level + formatted message. #1681 pins that a self-alert FIRING reaches the
    /// service log, which for a long time only recoveries did.
    /// </summary>
    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel,
            Microsoft.Extensions.Logging.EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    /// <summary>One evaluator + fakes + a controllable clock per test.</summary>
    private sealed class Harness
    {
        public FakeSettings Settings { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public FakeHistoryStore History { get; } = new();
        public bool Muted { get; set; }

        /// <summary>When set, the mute check throws — simulates a broken mute rule's Matches() to prove the
        /// evaluation isolates it (a throw here must never propagate out and stop collection).</summary>
        public bool MuteThrows { get; set; }

        /// <summary>The V20 connection-change notify gate, read live by the evaluator's connect edge (default on).</summary>
        public bool NotifyConnectionChanges { get; set; } = true;

        /// <summary>#1659 opt-ins (V33), read live like the V20 gate. Defaults off = classic edge-only.</summary>
        public bool NotifyConnectionDownAtStartup { get; set; }
        public int ConnectionRefireMinutes { get; set; }

        /// <summary>#991 Availability Group knobs (V35), read live like the V20 gate. Defaults are the shipped
        /// V35 DDL defaults: family on, lag trigger at 300s, redo-queue trigger off.</summary>
        public bool NotifyAgHealth { get; set; } = true;
        public int AgLagAlertSeconds { get; set; } = 300;
        public long AgRedoQueueAlertKb { get; set; }

        /// <summary>#1696 (V37): AG disconnect re-fire minutes. Default 0 = off, the shipped behavior.</summary>
        public int AgDisconnectRefireMinutes { get; set; }

        /// <summary>#2136 (V57): the Store Job Over Cadence warning percent. Default is the shipped 25.</summary>
        public int StoreJobCadenceWarnPercent { get; set; } = 25;

        public DateTime Now { get; set; } = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        /// <summary>#1681: captures what the evaluator writes to the service log, so the firing/recovery pair
        /// can be asserted rather than assumed.</summary>
        public CapturingLogger Log { get; } = new();

        public DarlingSelfAlertEvaluator Build() => new(
            Settings, Deliverer, History,
            _ => MuteThrows ? throw new InvalidOperationException("mute check boom") : Muted,
            logger: Log, utcNow: () => Now,
            notifyConnectionChanges: () => NotifyConnectionChanges,
            notifyConnectionDownAtStartup: () => NotifyConnectionDownAtStartup,
            connectionRefireMinutes: () => ConnectionRefireMinutes,
            notifyAgHealth: () => NotifyAgHealth,
            agLagAlertSeconds: () => AgLagAlertSeconds,
            agRedoQueueAlertKb: () => AgRedoQueueAlertKb,
            agDisconnectRefireMinutes: () => AgDisconnectRefireMinutes,
            storeJobCadenceWarnPercent: () => StoreJobCadenceWarnPercent);
    }

    /* ---------------- #991 Availability Group fixtures ---------------- */

    private const string Ag = "AG1";
    private const string Replica = "NODE2";
    private const string Db = "Sales";

    private static AgReplicaReading ReplicaRow(
        string? role = "SECONDARY", string? connected = "CONNECTED", string replica = Replica, string ag = Ag,
        bool? isLocal = null) =>
        new(ag, replica, role, connected, isLocal);

    private static AgDatabaseReading DatabaseRow(
        long? lagSeconds = 0,
        long? redoKb = 0,
        bool? suspended = false,
        string? suspendReason = null,
        string database = Db,
        string replica = Replica,
        string ag = Ag) =>
        new(ag, database, replica, lagSeconds, redoKb, suspended, suspendReason);

    /* ---------------- collection-stopped detection (pure) ---------------- */

    [Fact]
    public void IsCollectionStopped_FreshSuccess_NotStopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* Last success two minutes ago, recent runs all succeeded — healthy. */
        Assert.False(DarlingSelfAlertEvaluator.IsCollectionStopped(now.AddMinutes(-2), 10, 10, now, out var reason));
        Assert.Equal("", reason);
    }

    [Fact]
    public void IsCollectionStopped_NoSuccessWithinStaleWindow_Stopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* Last success 45 minutes ago (>= the 30-minute window), recent runs mixed — stale => stopped. */
        Assert.True(DarlingSelfAlertEvaluator.IsCollectionStopped(now.AddMinutes(-45), 5, 3, now, out var reason));
        Assert.Contains("45 minutes", reason);
    }

    [Fact]
    public void IsCollectionStopped_ExactlyAtStaleWindow_Stopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* Boundary: elapsed == StaleWindow counts as stale (>=). */
        Assert.True(DarlingSelfAlertEvaluator.IsCollectionStopped(
            now - DarlingSelfAlertEvaluator.StaleWindow, 5, 4, now, out _));
    }

    [Fact]
    public void IsCollectionStopped_NeverRan_NotStopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* A freshly-added / never-connected server (no success ever, no runs) must NOT read as stopped —
           the connection-lost alert covers that, and a warming-up server is not "broken". */
        Assert.False(DarlingSelfAlertEvaluator.IsCollectionStopped(null, 0, 0, now, out _));
    }

    [Fact]
    public void IsCollectionStopped_ConsecutiveFailuresNoSuccessEver_Stopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* Connected but every one of the last N runs failed (never a success) => stopped fast, before the
           staleness backstop would trip. */
        Assert.True(DarlingSelfAlertEvaluator.IsCollectionStopped(
            null, DarlingSelfAlertEvaluator.ConsecutiveFailureThreshold, 0, now, out var reason));
        Assert.Contains("failed", reason);
    }

    [Fact]
    public void IsCollectionStopped_ConsecutiveFailuresButSomeSuccessInWindow_NotStopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* The last N runs include at least one success and the last success is recent — not stopped
           (a single failing collector among healthy ones must not trip the server-level alert). */
        Assert.False(DarlingSelfAlertEvaluator.IsCollectionStopped(
            now.AddMinutes(-1), DarlingSelfAlertEvaluator.ConsecutiveFailureThreshold, 2, now, out _));
    }

    [Fact]
    public void IsCollectionStopped_FewFailuresRecentSuccess_NotStopped()
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        /* Fewer than the consecutive threshold and a recent success — a brief hiccup, not stopped. */
        Assert.False(DarlingSelfAlertEvaluator.IsCollectionStopped(now.AddMinutes(-1), 3, 0, now, out _));
    }

    /* ---------------- collection-stopped edge ---------------- */

    [Fact]
    public async Task CollectionStopped_FiresOnce_ThenCooldownSuppresses_ThenReFires()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Collection Stopped", fired.MetricName);
        Assert.Equal("collecting", fired.ThresholdValue);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal(Key, fired.ServerKey);

        /* Still stopped one minute later — inside the 5-minute cooldown, no re-fire (the EDGE: once,
           not every sweep). */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* After the cooldown the standing condition re-fires. */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task CollectionStopped_Recovery_WritesOneResumedHistoryRow()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct);
        Assert.Empty(h.History.Records);

        /* Recovery: exactly one "Collection Resumed" audit row, no email/webhook (it went to the history
           store, not the deliverer). */
        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: false, "", Ct);
        var resumed = Assert.Single(h.History.Records);
        Assert.Equal("Collection Resumed", resumed.MetricName);
        Assert.True(resumed.AlertSent);
        Assert.Equal("tray", resumed.NotificationType);
        Assert.Single(h.Deliverer.Outcomes); /* only the original fire went to the deliverer */

        /* Still healthy on the next sweep — no duplicate resumed row (resolution is edge-triggered too). */
        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: false, "", Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task CollectionStopped_Muted_RecordedMutedNotSuppressed()
    {
        var h = new Harness { Muted = true };
        var e = h.Build();

        await e.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, "no recent collection", Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted); /* the deliverer skips channels but still records — same as the engine */
    }

    /* ---------------- capture-down edge ---------------- */

    [Fact]
    public async Task CaptureDown_FiresOnce_ThenRestoredHistoryRow()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking", "Deadlock" }, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Capture Down", fired.MetricName);
        Assert.Equal("Blocking and Deadlock", fired.CurrentValue);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);

        /* Still down inside the cooldown — no re-fire. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking", "Deadlock" }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Sessions back — one "Capture Restored" audit row. */
        await e.ApplyCaptureDownAsync(ServerId, Name, Array.Empty<string>(), Ct);
        var restored = Assert.Single(h.History.Records);
        Assert.Equal("Capture Restored", restored.MetricName);
    }

    [Fact]
    public async Task CaptureDown_BlockingAndDeadlockDisabled_DoesNotFire()
    {
        var h = new Harness();
        h.Settings.BlockingEnabled = false;
        h.Settings.DeadlockEnabled = false;
        var e = h.Build();

        await e.ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking" }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* ---------------- agent-not-running edge (#1433 Phase 2) ---------------- */

    [Fact]
    public async Task AgentNotRunning_FiresOnce_ThenCooldownSuppresses_ThenReFires()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Agent Not Running", fired.MetricName);
        Assert.Equal("Stopped", fired.CurrentValue);
        Assert.Equal("Running", fired.ThresholdValue);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal(Key, fired.ServerKey);

        /* Still stopped inside the cooldown — the EDGE: no re-fire. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* After the cooldown the standing condition re-fires. */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AgentNotRunning_Recovery_WritesOneRestartedHistoryRow()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        Assert.Empty(h.History.Records);

        /* Agent back up: exactly one "Agent Restarted" audit row, no email/webhook. */
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: true, agentEverSeenRunning: true, Ct);
        var restarted = Assert.Single(h.History.Records);
        Assert.Equal("Agent Restarted", restarted.MetricName);
        Assert.True(restarted.AlertSent);
        Assert.Equal("tray", restarted.NotificationType);
        Assert.Single(h.Deliverer.Outcomes);

        /* Still running on the next sweep — no duplicate resolution (edge-triggered). */
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: true, agentEverSeenRunning: true, Ct);
        Assert.Single(h.History.Records);
    }

    /* ---------------- agent capability gate: never-seen-running stays silent ---------------- */

    /// <summary>
    /// THE fixture-container shape, and the field-noise case this gate exists for: a target where Agent has never
    /// been observed running — a container built with Agent off, Express, a Linux-minimal image. Before the gate
    /// this fired a Critical alert on EVERY sweep forever (observed on the AG fixture: identical alerts every 5
    /// minutes from first contact). It must be silent, permanently, and never accumulate a standing down-state.
    /// </summary>
    [Fact]
    public async Task AgentNotRunning_NeverSeenRunning_StaysSilentForever()
    {
        var h = new Harness();
        var e = h.Build();

        for (var sweep = 0; sweep < 5; sweep++)
        {
            await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: false, Ct);
            h.Now = h.Now.AddMinutes(10); /* well past the cooldown, so silence is the gate and not the cooldown */
        }

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    /// <summary>
    /// A never-seen-running server whose Agent later starts must NOT emit "Agent Restarted": no alert ever fired,
    /// so there is nothing to resolve. Suppressing the fire while still recording a standing down-state would
    /// produce exactly that phantom recovery.
    /// </summary>
    [Fact]
    public async Task AgentNotRunning_NeverSeenRunning_ThenStarts_EmitsNoPhantomRecovery()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: false, Ct);
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: true, agentEverSeenRunning: false, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    /// <summary>
    /// Once Agent HAS been seen running, the server is a real Agent user and the alert behaves exactly as before —
    /// the gate must not silence the case the alert exists for. This is the transition a container makes the first
    /// time someone actually starts Agent on it.
    /// </summary>
    [Fact]
    public async Task AgentNotRunning_AfterAgentIsSeenRunning_AlertsNormallyOnceItStops()
    {
        var h = new Harness();
        var e = h.Build();

        /* Silent while it has never run... */
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: false, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* ...Agent runs (the caller memoizes this), then stops: now it is a real signal. */
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: true, agentEverSeenRunning: true, Ct);
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Agent Not Running", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
    }

    /// <summary>
    /// The gate never overrides the staleness rule: a null (no fresh reading) still refuses to judge, whichever
    /// side of the gate the server is on.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AgentNotRunning_NoFreshReading_NeverJudges_RegardlessOfGate(bool everSeenRunning)
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: null, agentEverSeenRunning: everSeenRunning, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AgentNotRunning_NoFreshReading_DoesNotFireOrClearStandingAlert()
    {
        var h = new Harness();
        var e = h.Build();

        /* Fire on a fresh stopped reading. */
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* No fresh reading (stale snapshot / never collected) — must NEITHER clear the standing alert (no
           resolution row) NOR fire. The collection-stopped alert owns staleness. */
        h.Now = h.Now.AddMinutes(10);
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: null, agentEverSeenRunning: true, Ct);
        Assert.Empty(h.History.Records);

        /* The active flag persisted through the null gap: a fresh stopped reading after the cooldown re-fires
           (it would have been cleared to a first-fire if null had wrongly reset the state). */
        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AgentNotRunning_AlertsDisabled_DoesNotFire()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        await e.ApplyAgentNotRunningAsync(ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* ---------------- connection lost / restored edge ---------------- */

    /* ---------------- #1659: already-down-at-first-sight + standing-outage re-fire ---------------- */

    [Fact]
    public async Task Connection_AlreadyDownAtFirstSight_OptIn_FiresOnTheFirstOutcome()
    {
        var h = new Harness { NotifyConnectionDownAtStartup = true };
        var e = h.Build();

        /* Unknown -> Offline with the opt-in: the outage is announced at first sight instead of being a
           silent baseline — the case where the service starts mid-outage and would otherwise never alert. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Server Unreachable", fired.MetricName);
        Assert.Contains("Already unreachable when monitoring started", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Connection_Refire_FiresAfterTheInterval_NotBefore_AndUnderTheSameMetricName()
    {
        var h = new Harness { ConnectionRefireMinutes = 10 };
        var e = h.Build();

        /* Establish online, then lose it: the classic edge fires once. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Inside the window: offline->offline stays quiet. */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Past the window: the standing outage re-announces — SAME metric name, so webhook automation
           keyed on "Server Unreachable" re-triggers; the detail says it is a re-fire. */
        h.Now = h.Now.AddMinutes(6);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("Server Unreachable", h.Deliverer.Outcomes[1].MetricName);
        Assert.Contains("Still unreachable", h.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);

        /* Restore clears the re-fire clock and fires the classic restore. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
        Assert.Equal("Server Restored", h.Deliverer.Outcomes[2].MetricName);
    }

    [Fact]
    public async Task Connection_Refire_ClockStampsOnDeliveryOnly_SoASuppressedDecisionDoesNotConsumeTheWindow()
    {
        var h = new Harness { ConnectionRefireMinutes = 10, NotifyConnectionChanges = false };
        var e = h.Build();

        /* Down while the notify toggle is OFF: state advances, nothing delivers, nothing stamps. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Toggle on mid-outage: the very next offline->offline poll is due immediately (no recorded down
           alert to measure the window from), so the outage is announced rather than silently aged. */
        h.NotifyConnectionChanges = true;
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Server Unreachable", fired.MetricName);
    }

    [Fact]
    public async Task Connection_FirstConnect_IsSilentBaseline()
    {
        var h = new Harness();
        var e = h.Build();

        /* Unknown -> Online on the first-ever connect: no "restored" (there was no prior loss),
           mirroring the Dashboard's skip-first-check. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Connection_DownAtStartup_StaysDown_NeverFires()
    {
        var h = new Harness();
        var e = h.Build();

        /* Unknown -> Offline (baseline) then Offline -> Offline: a server simply down at startup never
           pages; only a transition FROM a known-online state does. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "no route", Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Connection_Lost_FiresOnce_RepeatedFailedReconnectDoesNotReFire()
    {
        var h = new Harness();
        var e = h.Build();

        /* Establish an online baseline (silent). */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Online -> Offline: fire "Server Unreachable" ONCE. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "Login timeout expired", Ct);
        var lost = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Server Unreachable", lost.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, lost.Severity);
        Assert.Equal("Login timeout expired", lost.CurrentValue);

        /* Offline -> Offline (the 60s retry keeps failing): the EDGE — no re-fire. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "Login timeout expired", Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "Login timeout expired", Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Connection_Restored_FiresOnce_AfterALoss()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);   /* baseline */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "boom", Ct); /* lost */
        Assert.Single(h.Deliverer.Outcomes);

        /* Offline -> Online: fire "Server Restored" once (Severity null so the shared map renders it green). */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        var restored = h.Deliverer.Outcomes[1];
        Assert.Equal("Server Restored", restored.MetricName);
        Assert.Null(restored.Severity);

        /* Staying online does not re-fire. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task Connection_Lost_NotDelivered_WhenNotifyConnectionChangesOff()
    {
        /* V20: the connection-change notify toggle gates DELIVERY of the connect edge, independently of the
           per-alert enables. Off -> even a genuine online->offline transition delivers nothing. */
        var h = new Harness { NotifyConnectionChanges = false };
        var e = h.Build();

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);   /* baseline */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "boom", Ct); /* lost — muted by the toggle */

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Connection_NotifyOff_StillTracksState_SoLaterEnabledRestoreFires()
    {
        /* The gate suppresses delivery only, NOT the state machine (mirrors the master-switch posture): a loss
           missed while the toggle was off still advances the state, so flipping it back on and reconnecting
           fires "Server Restored" from the correct baseline rather than replaying nothing. */
        var h = new Harness { NotifyConnectionChanges = false };
        var e = h.Build();

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);   /* Online baseline */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "boom", Ct); /* Offline (state advanced, no delivery) */
        Assert.Empty(h.Deliverer.Outcomes);

        h.NotifyConnectionChanges = true; /* operator re-enables the toggle */

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);   /* Offline -> Online: restore fires */
        var restored = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Server Restored", restored.MetricName);
    }

    [Fact]
    public async Task Connection_Forget_ResetsToBaseline()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);  /* Online baseline */
        e.Forget(ServerId);

        /* After Forget the next offline is a fresh Unknown->Offline baseline again — no spurious "lost". */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "gone", Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Connection_ThrowingMuteCheck_IsIsolated_AndStateStillAdvances()
    {
        /* The connection edge fires straight from the un-guarded sweep loop (TryConnectAsync), whose OWN catch
           re-calls this with online:false — so a throwing mute-check here (a broken rule's Matches()) would
           propagate out and stop collection for the whole fleet. The delivery portion must isolate it, and
           because the state machine advances BEFORE the fire, the edge must still transition correctly. */
        var h = new Harness();
        var e = h.Build();

        /* Online baseline (Unknown->Online is silent; no mute check reached). */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Now the mute check throws: Online->Offline WOULD fire "Server Unreachable" -> mute throws.
           Must NOT propagate (would kill the loop), and nothing is delivered (throw precedes delivery). */
        h.MuteThrows = true;
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "boom", Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* The state STILL advanced to Offline despite the throwing fire: with the mute check healthy again,
           Offline->Online now fires "Server Restored" — it would NOT if the state were stuck at Online. */
        h.MuteThrows = false;
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        var restored = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Server Restored", restored.MetricName);
    }

    /* ---------------- store disk pressure (fleet-level, pure decision) ---------------- */

    private const long Gib = 1024L * 1024 * 1024;

    [Fact]
    public void IsDiskPressure_BelowThreshold_Pressure()
    {
        /* 5% free (< the 10% warn threshold) reads as pressure; the reason names the percentage. */
        Assert.True(DarlingSelfAlertEvaluator.IsDiskPressure(5 * Gib, 100 * Gib, out var reason, out var percentFree));
        Assert.Contains("5", reason);
        Assert.Contains("%", reason);

        /* #1881: the percentage is now HANDED BACK rather than only spelled into the prose, so the fire
           site can store it as a real numeric instead of leaving the history store to re-find it by
           scanning the sentence for digits. */
        Assert.Equal(5.0, percentFree, precision: 6);
    }

    [Fact]
    public void IsDiskPressure_ExactlyAtThreshold_NotPressure()
    {
        /* Boundary: exactly 10% free is NOT pressure (strictly-less-than the threshold). */
        Assert.False(DarlingSelfAlertEvaluator.IsDiskPressure(10 * Gib, 100 * Gib, out _, out var percentFree));

        /* Measured whenever the total is usable, firing or not — see IsDiskPressure's remarks for why
           this must not collapse to 0 on the not-pressure path. */
        Assert.Equal(10.0, percentFree, precision: 6);
    }

    [Fact]
    public void IsDiskPressure_JustBelowThreshold_Pressure()
    {
        /* 9.9% free trips it — the threshold is a real edge, not a wide band. */
        Assert.True(DarlingSelfAlertEvaluator.IsDiskPressure(99 * Gib, 1000 * Gib, out _, out var percentFree));
        Assert.Equal(9.9, percentFree, precision: 6);
    }

    [Fact]
    public void IsDiskPressure_PlentyFree_NotPressure()
    {
        Assert.False(DarlingSelfAlertEvaluator.IsDiskPressure(50 * Gib, 100 * Gib, out _, out _));
    }

    [Fact]
    public void IsDiskPressure_NonPositiveTotal_NotPressure()
    {
        /* An undeterminable total ("can't tell") never reads as pressure. */
        Assert.False(DarlingSelfAlertEvaluator.IsDiskPressure(0, 0, out _, out var percentFree));

        /* Nothing was measured, so nothing is reported. The caller no-ops on this path before it can
           reach a fire site, so the 0 is never stored. */
        Assert.Equal(0.0, percentFree, precision: 6);
    }

    [Fact]
    public void IsDiskPressure_FullVolume_ReportsZeroPercentFree()
    {
        /* #1881's reason for keeping "Store Disk Pressure" OUT of AlertMetricClassifier.IsStateOnly: a
           genuine 0 here means a FULL volume, the one reading that must never be rendered as an em dash.
           Pinned at the source so the invariant is visible where the number is produced. */
        Assert.True(DarlingSelfAlertEvaluator.IsDiskPressure(0, 100 * Gib, out _, out var percentFree));
        Assert.Equal(0.0, percentFree, precision: 6);
    }

    /* ---------------- store disk pressure edge ---------------- */

    [Fact]
    public async Task DiskPressure_FiresOnce_ThenStaysQuietAtUnchangedLevel_ReFiresOnlyOnWorsening()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, storeSizeBytes: 20 * Gib, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Store Disk Pressure", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("store", fired.ServerKey);   /* the fleet sentinel key, not a real server_id */

        /* Still low one minute later — inside the 5-minute cooldown, no re-fire (the EDGE: once, not every sweep). */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, null, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* #2101: the cooldown elapsing is NOT enough — a standing breach at an UNCHANGED level stays
           quiet (the field report: 7.3% free re-notified every 15 minutes for hours). */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, null, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Worsened less than the 1pp margin (5.0% → 4.5%) — jitter, still quiet. */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyDiskPressureAsync(45 * Gib, 1000 * Gib, null, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Genuinely worsened (5.0% → 3.5%, past the margin) — re-fires, and re-anchors the watermark. */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyDiskPressureAsync(35 * Gib, 1000 * Gib, null, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task DiskPressure_Recovery_ClearsTheWorseningWatermark_SoTheNextBreachIsFresh()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, null, Ct);   /* breach at 5% */
        Assert.Single(h.Deliverer.Outcomes);

        await e.ApplyDiskPressureAsync(50 * Gib, 100 * Gib, null, Ct);  /* recovered */

        /* A NEW breach at the same 5% level after recovery must fire — the watermark died with the
           old episode, or a volume that oscillates around the threshold would go permanently silent. */
        h.Now = h.Now.AddMinutes(6);
        await e.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, null, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task DiskPressure_Recovery_WritesOneResolvedHistoryRow()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, null, Ct);   /* pressure */
        Assert.Empty(h.History.Records);

        /* Free space recovered: exactly one "Store Disk Pressure Resolved" audit row, no email/webhook. */
        await e.ApplyDiskPressureAsync(50 * Gib, 100 * Gib, null, Ct);
        var resolved = Assert.Single(h.History.Records);
        Assert.Equal("Store Disk Pressure Resolved", resolved.MetricName);
        Assert.True(resolved.AlertSent);
        Assert.Equal("tray", resolved.NotificationType);
        Assert.Single(h.Deliverer.Outcomes);   /* only the original fire went to the deliverer */

        /* Still healthy on the next sweep — no duplicate resolved row (resolution is edge-triggered too). */
        await e.ApplyDiskPressureAsync(50 * Gib, 100 * Gib, null, Ct);
        Assert.Single(h.History.Records);
    }

    [Fact]
    public async Task DiskPressure_UndeterminableFreeSpace_DoesNotFire()
    {
        /* A remote BYO store whose volume the service cannot see (null free/total) never alarms — even though
           a store size is known, it is context only and never the trigger. */
        var h = new Harness();
        var e = h.Build();

        await e.ApplyDiskPressureAsync(null, null, storeSizeBytes: 999 * Gib, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task DiskPressure_AlertsDisabled_DoesNotFire()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        await e.ApplyDiskPressureAsync(1 * Gib, 100 * Gib, null, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task DiskPressure_ThrowingMuteCheck_IsIsolated_DoesNotPropagate()
    {
        /* MAJOR: the disk-pressure sweep-loop body has no catch-all of its own, and the pre-deliver mute check
           (_isAlertMuted -> a mute rule's Matches()) is NOT internally isolated. EvaluateDiskPressureAsync must
           swallow a throw there — otherwise a single broken mute rule stops collection for the whole fleet. */
        var h = new Harness { MuteThrows = true };
        var e = h.Build();

        /* Must NOT throw (would propagate out of the un-guarded worker loop). */
        await e.EvaluateDiskPressureAsync(5 * Gib, 100 * Gib, storeSizeBytes: 20 * Gib, Ct);

        /* The throw happened in the mute check, before delivery — nothing was delivered, and we're still alive. */
        Assert.Empty(h.Deliverer.Outcomes);

        /* The isolation lives in the Evaluate wrapper (the worker's entry point), not the sibling-style
           un-isolated Apply: a fresh evaluator's Apply lets the same throw propagate. */
        var e2 = new Harness { MuteThrows = true }.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => e2.ApplyDiskPressureAsync(5 * Gib, 100 * Gib, null, Ct));
    }

    /* ---------------- compression-job self-heal (#1581) ---------------- */

    /// <summary>Records the job_ids passed to the re-arm delegate and returns a configurable success.</summary>
    private sealed class RearmRecorder
    {
        public List<long> Calls { get; } = new();
        public bool Result { get; set; } = true;
        public Func<long, Task<bool>> Delegate => id =>
        {
            Calls.Add(id);
            return Task.FromResult(Result);
        };
    }

    private static IReadOnlyList<StuckCompressionJob> Stuck(params long[] jobIds)
    {
        var list = new List<StuckCompressionJob>();
        foreach (var id in jobIds)
        {
            list.Add(new StuckCompressionJob(id, "wait_stats", "next_start is -infinity — the scheduler will never run it again"));
        }

        return list;
    }

    [Fact]
    public async Task StoreUpgrade_Succeeded_FiresOnceNamingBothVersions()
    {
        var h = new Harness();
        var e = h.Build();

        await e.EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                Succeeded: true, FromMajor: 17, ToMajor: 18,
                FromTimescale: "2.17.2", ToTimescale: "2.28.1",
                FailedStep: null, FailureMessage: null, WithoutRollbackCopy: false),
            Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Store Runtime Upgrade", fired.MetricName);
        Assert.Equal("storeupgrade", fired.ServerKey);   /* fleet-level, never parses as a server_id */
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Contains("PostgreSQL 18", fired.ShortMessage, StringComparison.Ordinal);
        /* Both extension versions belong in the detail: "which TimescaleDB am I on now" is the first thing
           asked after an upgrade, and #1705 happened because nobody could answer it. */
        Assert.Contains("2.17.2", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("2.28.1", fired.DetailText, StringComparison.Ordinal);
        /* A copy-mode upgrade keeps a rollback copy, and the alert says so. */
        Assert.Contains("rollback copy", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StoreUpgrade_SucceededWithABookkeepingWarning_AlarmsInsteadOfReassuring()
    {
        var h = new Harness();
        var e = h.Build();

        /* A post-commit bookkeeping failure arrives as Succeeded WITH a message: the store IS upgraded and
           verified, but cleanup did not finish. The alert must say so — an operator receiving "upgraded, the
           rollback copy ages out automatically" while the service log says the retention marker could not be
           written is worse than either surface alone, because the alert is the one that reaches them. */
        await e.EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                Succeeded: true, FromMajor: 17, ToMajor: 18,
                FromTimescale: "2.28.1", ToTimescale: "2.28.1",
                FailedStep: null,
                FailureMessage: "the rollback copy's retention marker could not be written (There is not enough space on the disk.)",
                WithoutRollbackCopy: false),
            Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Store Runtime Upgrade", fired.MetricName);
        /* Escalated: this one needs somebody to go and look. */
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Contains("did NOT complete", fired.ShortMessage, StringComparison.Ordinal);
        /* The actual reason has to survive the hop from the orchestration through the worker's mapping. */
        Assert.Contains("not enough space on the disk", fired.DetailText, StringComparison.Ordinal);
        /* And the reassuring sentence must be REPLACED, not merely appended to — the retention marker is
           precisely what failed, so promising it ages out automatically would be actively wrong. */
        Assert.DoesNotContain("then deleted automatically", fired.DetailText, StringComparison.Ordinal);
        /* The corrected wording: the copy DOES age out, one start later, because the sweep reads a
           missing counter as 1. Telling an operator it never ages out would send them to delete a
           multi-gigabyte directory for no reason — the same class of false operator-facing claim this
           whole round was about, just erring toward extra work instead of false comfort. */
        Assert.Contains("ages out one start later than usual", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("will not age out on its own", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StoreUpgrade_HardLinkMode_SaysThereIsNoRollback()
    {
        var h = new Harness();
        var e = h.Build();

        await e.EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                Succeeded: true, FromMajor: 17, ToMajor: 18,
                FromTimescale: "2.28.1", ToTimescale: "2.28.1",
                FailedStep: null, FailureMessage: null, WithoutRollbackCopy: true),
            Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        /* The one thing an operator must know after a link-mode upgrade: the way back is a backup, and if
           they do not have one they need to find out NOW rather than when they need it. */
        Assert.Contains("NO rollback copy", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("restore from backup", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StoreUpgrade_Failed_FiresCriticalNamingTheStep_AndSaysTheStoreStillRuns()
    {
        var h = new Harness();
        var e = h.Build();

        await e.EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                Succeeded: false, FromMajor: 17, ToMajor: 18,
                FromTimescale: "2.28.1", ToTimescale: "2.28.1",
                FailedStep: "pg_upgrade-check", FailureMessage: "pg_upgrade --check failed (exit 1)",
                WithoutRollbackCopy: false),
            Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Store Runtime Upgrade", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        /* Naming the failed step is the difference between an actionable page and "something broke". */
        Assert.Contains("pg_upgrade-check", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("pg_upgrade-check", fired.DetailText, StringComparison.Ordinal);
        /* And the reassurance that matters at 3am: the monitor is still monitoring, and nothing was lost. */
        Assert.Contains("collecting normally", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("no data was lost", fired.DetailText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task StoreUpgrade_RespectsTheMasterAlertsSwitch()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        await e.EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                true, 17, 18, "2.28.1", "2.28.1", null, null, false),
            Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task CompressionJobs_FirstDetection_RearmsOnce_AndFiresCritical()
    {
        var h = new Harness();
        var e = h.Build();
        var rearm = new RearmRecorder();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);

        /* Re-armed exactly once. */
        Assert.Equal(1001L, Assert.Single(rearm.Calls));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Compression Job Stuck", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("compressjob:1001", fired.ServerKey);  /* prefixed so it never parses as a server_id */
        Assert.Contains("auto-re-armed", fired.ShortMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CompressionJobs_ReHangAfterSelfHeal_Escalates_AndStopsRearming()
    {
        var h = new Harness();
        var e = h.Build();
        var rearm = new RearmRecorder();

        /* Check 1: detect + re-arm + fire. */
        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);

        /* Check 2 (an hour later): STILL stuck = a re-hang. Escalate, and do NOT re-arm again. */
        h.Now = h.Now.AddHours(1);
        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);

        Assert.Single(rearm.Calls);  /* never re-armed a second time */
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        var escalated = h.Deliverer.Outcomes[1];
        Assert.Equal(AlertSeverityLevel.Critical, escalated.Severity);
        Assert.Contains("re-hung", escalated.ShortMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CompressionJobs_AfterEscalation_NeverRearms_ReFiresOnlyOnCooldown()
    {
        var h = new Harness();
        var e = h.Build();
        var rearm = new RearmRecorder();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);  /* detect + re-arm */
        h.Now = h.Now.AddHours(1);
        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);  /* escalate (fire) */
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* Inside the 5-minute cooldown after the escalation: no re-fire, no re-arm. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);
        Assert.Single(rearm.Calls);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* After the cooldown: re-fires (still no re-arm). */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);
        Assert.Single(rearm.Calls);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task CompressionJobs_RearmFailure_EscalatesImmediately_AndNeverRetriesRearm()
    {
        var h = new Harness();
        var e = h.Build();
        var rearm = new RearmRecorder { Result = false };  /* alter_job fails (e.g. permission) */

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);

        /* Tried once, failed -> escalated with an "auto-re-arm FAILED" alert. */
        Assert.Single(rearm.Calls);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Contains("FAILED", fired.ShortMessage, StringComparison.Ordinal);

        /* Next check: never retries the re-arm (already escalated). */
        h.Now = h.Now.AddHours(1);
        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);
        Assert.Single(rearm.Calls);
    }

    [Fact]
    public async Task CompressionJobs_Recovery_WritesOneResolutionRow_AndClearsState()
    {
        var h = new Harness();
        var e = h.Build();
        var rearm = new RearmRecorder();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);  /* stuck */
        Assert.Empty(h.History.Records);

        /* No longer stuck: exactly one "Compression Job Recovered" audit row (BuildResolutionRecord maps the
           resolution Title onto the history MetricName, mirroring the disk-pressure recovery). */
        await e.ApplyCompressionJobsStuckAsync(Stuck(), rearm.Delegate, Ct);
        var resolved = Assert.Single(h.History.Records);
        Assert.Equal("Compression Job Recovered", resolved.MetricName);
        Assert.True(resolved.AlertSent);
        Assert.Contains("running on schedule again", resolved.DetailText, StringComparison.Ordinal);

        /* Still healthy next check — no duplicate resolution (edge-triggered), and a re-stuck job would be a
           fresh first-detection again (state was cleared) -> a new re-arm. */
        await e.ApplyCompressionJobsStuckAsync(Stuck(), rearm.Delegate, Ct);
        Assert.Single(h.History.Records);

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);
        Assert.Equal(2, rearm.Calls.Count);  /* re-stuck after recovery -> re-armed fresh */
    }

    [Fact]
    public async Task CompressionJobs_AlertsDisabled_DoesNotFireOrRearm()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();
        var rearm = new RearmRecorder();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), rearm.Delegate, Ct);

        Assert.Empty(rearm.Calls);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task CompressionJobs_MultipleStuckJobs_EachRearmedOncePerCheck()
    {
        var h = new Harness();
        var e = h.Build();
        var rearm = new RearmRecorder();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001, 1002, 1003), rearm.Delegate, Ct);

        Assert.Equal(new[] { 1001L, 1002L, 1003L }, rearm.Calls);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task CompressionJobs_EvaluateWrapper_IsolatesAThrowingSeam_DoesNotPropagate()
    {
        /* The worker's compression sweep opens the connection OUTSIDE the evaluator; the Evaluate wrapper must
           still swallow a throw from the mute check OR the re-arm delegate so it can never propagate out of the
           un-guarded sweep loop and stop collection for the whole fleet. */
        var h = new Harness { MuteThrows = true };
        var e = h.Build();

        /* A throwing mute check (inside FireAsync, after a successful re-arm) is isolated. */
        await e.EvaluateCompressionJobsAsync(Stuck(1001), _ => Task.FromResult(true), Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* A throwing re-arm delegate is isolated too. */
        var e2 = new Harness().Build();
        await e2.EvaluateCompressionJobsAsync(Stuck(1002), _ => throw new InvalidOperationException("boom"), Ct);
    }

    /* ---------------- #991 Availability Groups: sync-behind decision (pure) ---------------- */

    private static AgSyncJudgement Judge(
        AgDatabaseReading reading, int lagSeconds, long redoKb) =>
        AgAlertPolicy.JudgeSync(reading, lagSeconds, redoKb, out _);

    [Fact]
    public void JudgeAgSync_LagAtOrOverThreshold_IsBehind()
    {
        Assert.Equal(
            AgSyncJudgement.Behind,
            AgAlertPolicy.JudgeSync(DatabaseRow(lagSeconds: 300), 300, 0, out var reason));
        Assert.Contains("300 seconds behind", reason, StringComparison.Ordinal);
        Assert.Equal(AgSyncJudgement.Behind, Judge(DatabaseRow(lagSeconds: 301), 300, 0));
        Assert.Equal(AgSyncJudgement.CaughtUp, Judge(DatabaseRow(lagSeconds: 299), 300, 0));
    }

    [Fact]
    public void JudgeAgSync_ZeroThreshold_DisablesThatTrigger()
    {
        /* Both off: a wildly lagging, hugely queued secondary is NOT MEASURABLE — not "caught up". The
           distinction matters because only a measured CaughtUp resolves a standing alert. */
        Assert.Equal(
            AgSyncJudgement.NotMeasurable,
            Judge(DatabaseRow(lagSeconds: 99999, redoKb: 99999999), 0, 0));

        /* Redo alone: the seconds trigger stays off, the KB trigger fires. */
        Assert.Equal(
            AgSyncJudgement.Behind,
            AgAlertPolicy.JudgeSync(DatabaseRow(lagSeconds: 99999, redoKb: 5000), 0, 4096, out var reason));
        Assert.Contains("redo queue", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void JudgeAgSync_SuspendedDatabase_MayFire_ButMayNeverResolve()
    {
        /* Measured against a live AG: secondary_lag_seconds ACCRUES while suspended, it does not read 0 the
           way MS Learn documents. A suspended secondary drifting past the threshold is the single most common
           way a secondary falls behind, so it MUST fire — an earlier rule that abstained on suspended rows
           silenced exactly that case. */
        Assert.Equal(
            AgSyncJudgement.Behind,
            Judge(DatabaseRow(lagSeconds: 9999, suspended: true), 300, 0));

        /* The other half of the asymmetry, and the reason this is not simply "judge suspended rows normally":
           a suspended reading UNDER the threshold is not evidence of recovery. This row is NOT hypothetical:
           sampled every 15s across a 60-second suspend on an IDLE group, secondary_lag_seconds read 0 at every
           sample while synchronization_state_desc was already NOT SYNCHRONIZING — on a quiet group it may
           never latch at all. Calling that CaughtUp would clear a standing alarm on a replica receiving
           nothing. (It also covers the documented flat-zero behavior, if any build really does that.) */
        Assert.Equal(
            AgSyncJudgement.NotMeasurable,
            Judge(DatabaseRow(lagSeconds: 0, suspended: true), 300, 0));

        /* The redo queue fires while suspended too — a frozen queue over the threshold is a real backlog. */
        Assert.Equal(
            AgSyncJudgement.Behind,
            Judge(DatabaseRow(lagSeconds: 0, redoKb: 8192, suspended: true), 300, 4096));

        /* ...but a frozen queue UNDER the threshold is stale data, not a recovery: redo_queue_size freezes at
           its last value while suspended (measured), so it cannot clear anything either. */
        Assert.Equal(
            AgSyncJudgement.NotMeasurable,
            Judge(DatabaseRow(lagSeconds: 0, redoKb: 8, suspended: true), 300, 4096));

        /* Once movement resumes, the same small readings are a real measurement and DO resolve. */
        Assert.Equal(
            AgSyncJudgement.CaughtUp,
            Judge(DatabaseRow(lagSeconds: 0, redoKb: 8, suspended: false), 300, 4096));
    }

    [Fact]
    public void JudgeAgSync_NullReadings_AreNotMeasurable_NotCaughtUp()
    {
        /* The PRIMARY's own row, and every row under WSFC quorum loss, reads NULL. Reporting that as CaughtUp
           would resolve every standing lag alert on the fleet the moment the cluster lost quorum. */
        Assert.Equal(
            AgSyncJudgement.NotMeasurable,
            Judge(DatabaseRow(lagSeconds: null, redoKb: null, suspended: null), 300, 4096));

        /* One usable arm is enough to judge, even when the other reads NULL. */
        Assert.Equal(
            AgSyncJudgement.CaughtUp,
            Judge(DatabaseRow(lagSeconds: 5, redoKb: null), 300, 4096));
    }

    /* ---------------- #991: failover edge ---------------- */

    [Fact]
    public async Task AgFailover_FirstSighting_IsSilentBaseline_ThenARoleChangeFires()
    {
        var h = new Harness();
        var e = h.Build();

        /* First sighting of this replica: baseline only. A service starting up must not page "failover"
           merely because it has never seen the role before. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "SECONDARY") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Same role again: steady state, still silent. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "SECONDARY") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* SECONDARY -> PRIMARY: a genuine change from a known state. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "PRIMARY") }, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Failover", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("PRIMARY", fired.CurrentValue);
        Assert.Equal("SECONDARY", fired.ThresholdValue);
        Assert.Contains("from SECONDARY to PRIMARY", fired.DetailText, StringComparison.Ordinal);

        /* And back: the reverse edge is just as much a failover. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "SECONDARY") }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AgFailover_NullRole_IsSkipped_NotTreatedAsAChange()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "PRIMARY") }, Ct);

        /* WSFC quorum loss nulls the AG catalog views wholesale. Treating NULL as a transition would spray a
           failover alert for every replica in the fleet at the exact moment the cluster is already in trouble. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: null) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* The remembered role survives the null, so recovering to the SAME role is still not a failover. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "PRIMARY") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AgFailover_TracksEachReplicaSeparately()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[]
        {
            ReplicaRow(role: "PRIMARY", replica: "NODE1"),
            ReplicaRow(role: "SECONDARY", replica: "NODE2"),
        }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* A failover swaps both — two independent edges, two alerts. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[]
        {
            ReplicaRow(role: "SECONDARY", replica: "NODE1"),
            ReplicaRow(role: "PRIMARY", replica: "NODE2"),
        }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal("AG Failover", o.MetricName));
    }

    /* ---------------- #991: replica disconnected / reconnected ---------------- */

    [Fact]
    public async Task AgReplicaDisconnected_FiresOnTheEdge_ThenReconnectResolvesIt()
    {
        var h = new Harness();
        var e = h.Build();

        /* Baseline CONNECTED. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* CONNECTED -> DISCONNECTED. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        var lost = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Replica Disconnected", lost.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, lost.Severity);

        /* Still disconnected: edge-triggered, so no re-fire. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Back to CONNECTED: the informational reconnect, severity null so the shared severity map renders
           it green/RESOLVED like "Server Restored". */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("AG Replica Reconnected", h.Deliverer.Outcomes[1].MetricName);
        Assert.Null(h.Deliverer.Outcomes[1].Severity);
    }

    [Fact]
    public async Task AgReplicaDisconnected_AlreadyDisconnectedAtFirstSighting_IsASilentBaseline()
    {
        var h = new Harness();
        var e = h.Build();

        /* The ConnectionAlertPolicy discipline: a replica that was already down when monitoring started is a
           baseline, not an edge — only a transition from a KNOWN state pages. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* ...and the recovery from that baseline still reports, so the operator learns the AG is whole. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);
        var reconnected = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Replica Reconnected", reconnected.MetricName);
    }

    [Fact]
    public async Task AgReplicaDisconnected_UnrecognizedState_IsTreatedAsConnected_NotAsAPage()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);

        /* Only an exact DISCONNECTED pages. A value we do not recognize must not wake somebody up at 3am for
           a state the product never learned to interpret. */
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "SOMETHING_NEW") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* ---------------- #991: sync fell behind ---------------- */

    [Fact]
    public async Task AgSyncFellBehind_FiresOnce_ReFiresOnlyOnCooldown_ThenRecovers()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Sync Fell Behind", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);

        /* Inside the 5-minute cooldown: a standing condition, so it stays quiet. */
        h.Now = h.Now.AddMinutes(2);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Past the cooldown: still behind, so re-fire. */
        h.Now = h.Now.AddMinutes(4);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* Caught up: one resolution row, no new delivery. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 1) }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal("AG Sync Recovered", resolution.MetricName);
        Assert.Contains(Db, resolution.DetailText, StringComparison.Ordinal);

        /* Recovered state is dropped, so falling behind again fires cleanly rather than being swallowed by
           the previous episode's cooldown stamp. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AgSyncFellBehind_TracksEachDatabaseIndependently()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[]
        {
            DatabaseRow(lagSeconds: 600, database: "Sales"),
            DatabaseRow(lagSeconds: 0, database: "Orders"),
        }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Orders now falls behind INSIDE Sales's cooldown window. Per-database keying is the whole point: a
           second database going bad must not hide behind the first one's cooldown. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[]
        {
            DatabaseRow(lagSeconds: 600, database: "Sales"),
            DatabaseRow(lagSeconds: 600, database: "Orders"),
        }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Contains("Orders", h.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);

        /* Sales recovers while Orders stays behind: exactly one resolution, and Orders keeps its state. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[]
        {
            DatabaseRow(lagSeconds: 0, database: "Sales"),
            DatabaseRow(lagSeconds: 600, database: "Orders"),
        }, Ct);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal("AG Sync Recovered", resolution.MetricName);
        Assert.Contains("Sales", resolution.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AgSyncFellBehind_LaggingDatabaseThatBecomesSuspended_IsNotAnnouncedAsCaughtUp()
    {
        var h = new Harness();
        var e = h.Build();

        /* Baseline healthy, then it falls behind. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 0) }, Ct);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Now data movement is SUSPENDED. secondary_lag_seconds reads 0 in that state, so a recovery sweep
           driven off "stopped breaching" would emit "AG Sync Recovered — has caught up with the primary" in
           the SAME sweep that reports the suspension. It got worse, not better: the only rows written here
           must be the suspend alert, and no resolution. */
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(lagSeconds: 0, suspended: true, suspendReason: "SUSPEND_FROM_USER") }, Ct);

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("AG Database Suspended", h.Deliverer.Outcomes[1].MetricName);
        Assert.DoesNotContain(h.History.Records, r => r.MetricName == "AG Sync Recovered");

        /* Resuming with real lag still standing keeps the alert live; it re-fires past the cooldown rather
           than starting a fresh episode from a phantom recovery. The resume itself is a history row (the
           evaluator's recovery shape), not a third delivery. */
        h.Now = h.Now.AddMinutes(6);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
        Assert.Equal("AG Sync Fell Behind", h.Deliverer.Outcomes[2].MetricName);
        Assert.Equal("AG Data Movement Resumed", Assert.Single(h.History.Records).MetricName);
    }

    [Fact]
    public async Task AgSyncFellBehind_SuspendedSecondaryDriftingPastTheThreshold_Fires()
    {
        var h = new Harness();
        var e = h.Build();

        /* Healthy baseline, then movement is suspended. The suspend edge fires on its own. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 0) }, Ct);
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(lagSeconds: 12, suspended: true, suspendReason: "SUSPEND_FROM_USER") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Database Suspended", h.Deliverer.Outcomes[0].MetricName);

        /* Time passes and the lag accrues past the threshold while still suspended. This is the case the
           product exists to catch, and the rule this test guards used to silence it: lag really does climb
           while suspended (measured on a live AG), so the sync alert has to fire on its own rather than
           trusting the suspend alert to have said everything. */
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(lagSeconds: 600, suspended: true, suspendReason: "SUSPEND_FROM_USER") }, Ct);

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("AG Sync Fell Behind", h.Deliverer.Outcomes[1].MetricName);
        Assert.Contains("600 seconds behind", h.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);

        /* Still suspended, still behind, inside the cooldown: no spam. */
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(lagSeconds: 700, suspended: true) }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* Resumed and genuinely caught up: NOW it resolves, off a measurement taken while movement was
           running. Both the resume and the sync recovery are history rows. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 0) }, Ct);
        Assert.Contains(h.History.Records, r => r.MetricName == "AG Sync Recovered");
        Assert.Contains(h.History.Records, r => r.MetricName == "AG Data Movement Resumed");
    }

    [Fact]
    public async Task AgSyncFellBehind_NullReadingsUnderQuorumLoss_DoNotResolveTheStandingAlert()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* WSFC quorum loss nulls the columns. That is no signal — the alert must stand, not be declared
           recovered at the worst possible moment. */
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(lagSeconds: null, redoKb: null, suspended: null) }, Ct);
        Assert.Empty(h.History.Records);

        /* A genuine measured recovery still resolves it. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 0) }, Ct);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal("AG Sync Recovered", resolution.MetricName);
    }

    [Fact]
    public async Task AgSyncFellBehind_BothTriggersOff_NeitherFiresNorResolves()
    {
        var h = new Harness { AgLagAlertSeconds = 0, AgRedoQueueAlertKb = 0 };
        var e = h.Build();

        /* Nothing to measure against: a hugely lagging secondary is simply not judged. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 99999, redoKb: 99999999) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task AgSyncFellBehind_OneAgIsJudgedByOneServer_SoAFullyMonitoredAgReportsItOnce()
    {
        var h = new Harness();
        var e = h.Build();

        const int nodeA = 100001;
        const int nodeB = 100002;

        /* #1696: both nodes are monitored and BOTH see the same AG database, because every replica is
           visible from every node. Only one may judge it, or a 3-node AG reports one problem three times. */
        await e.ApplyAgDatabaseHealthAsync(nodeA, "NODE-A", new[] { DatabaseRow(lagSeconds: 600) }, Ct);
        await e.ApplyAgDatabaseHealthAsync(nodeB, "NODE-B", new[] { DatabaseRow(lagSeconds: 600) }, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Sync Fell Behind", fired.MetricName);

        /* The recovery is announced once too, by the same authoritative node. */
        await e.ApplyAgDatabaseHealthAsync(nodeA, "NODE-A", new[] { DatabaseRow(lagSeconds: 0) }, Ct);
        await e.ApplyAgDatabaseHealthAsync(nodeB, "NODE-B", new[] { DatabaseRow(lagSeconds: 0) }, Ct);

        var resolution = Assert.Single(h.History.Records);
        Assert.Equal("AG Sync Recovered", resolution.MetricName);
    }

    /* ---------------- #991: database suspended ---------------- */

    [Fact]
    public async Task AgDatabaseSuspended_FiresOnTheEdgeWithTheReason_ThenResumeResolves()
    {
        var h = new Harness();
        var e = h.Build();

        /* Baseline healthy. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(suspended: false) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* false -> true, carrying suspend_reason_desc. */
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(suspended: true, suspendReason: "SUSPEND_FROM_USER") }, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Database Suspended", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("SUSPEND_FROM_USER", fired.CurrentValue);
        Assert.Contains("SET HADR RESUME", fired.DetailText, StringComparison.Ordinal);

        /* Still suspended: edge-triggered, no re-fire. */
        await e.ApplyAgDatabaseHealthAsync(
            ServerId, Name, new[] { DatabaseRow(suspended: true, suspendReason: "SUSPEND_FROM_USER") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Resumed: one resolution row, no second delivery. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(suspended: false) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal("AG Data Movement Resumed", resolution.MetricName);
    }

    [Fact]
    public async Task AgDatabaseSuspended_AlreadySuspendedAtFirstSighting_IsASilentBaseline_AndNullIsNoSignal()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(suspended: true) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* A NULL is_suspended (quorum loss) is no signal at all: it must neither fire nor clobber the
           remembered state, so the later genuine false->true edge still reads as an edge. */
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(suspended: null) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(suspended: false) }, Ct);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(suspended: true) }, Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Database Suspended", fired.MetricName);
        Assert.Equal("no reason reported", fired.CurrentValue);
    }

    /* ---------------- #1696: fleet de-dup + disconnect re-fire ---------------- */

    [Fact]
    public async Task AgFailover_FullyMonitoredThreeNodeAg_ReportsTheFailoverOnce()
    {
        var h = new Harness();
        var e = h.Build();

        /* Every replica is visible from EVERY node, so all three monitored servers report the same two
           replica rows. Before #1696 that meant one failover paged three times. */
        var beforeFailover = new[]
        {
            ReplicaRow(role: "PRIMARY", replica: "NODE1"),
            ReplicaRow(role: "SECONDARY", replica: "NODE2"),
        };
        var afterFailover = new[]
        {
            ReplicaRow(role: "SECONDARY", replica: "NODE1"),
            ReplicaRow(role: "PRIMARY", replica: "NODE2"),
        };

        foreach (var serverId in new[] { 100001, 100002, 100003 })
        {
            await e.ApplyAgReplicaHealthAsync(serverId, "NODE" + serverId, beforeFailover, Ct);
        }

        Assert.Empty(h.Deliverer.Outcomes);

        foreach (var serverId in new[] { 100001, 100002, 100003 })
        {
            await e.ApplyAgReplicaHealthAsync(serverId, "NODE" + serverId, afterFailover, Ct);
        }

        /* Two replicas changed role, so two alerts — NOT six. */
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal("AG Failover", o.MetricName));
    }

    [Fact]
    public async Task AgAuthority_PrimaryTakesOverFromASecondary_BecauseOnlyThePrimarySeesTheWholeGroup()
    {
        var h = new Harness();
        var e = h.Build();

        /* A monitored SECONDARY claims the AG first — its sys.dm_hadr_* is a one-row self-view. */
        await e.ApplyAgReplicaHealthAsync(
            200001, "SEC", new[] { ReplicaRow(role: "SECONDARY", replica: "NODE2", isLocal: true) }, Ct);

        /* The PRIMARY is then monitored. Its vantage is strictly better, so it takes over and its view of
           NODE1 is judged — which the secondary could never have supplied. */
        await e.ApplyAgReplicaHealthAsync(200002, "PRI", new[]
        {
            ReplicaRow(role: "PRIMARY", replica: "NODE1", isLocal: true),
            ReplicaRow(role: "SECONDARY", replica: "NODE2"),
        }, Ct);

        Assert.Empty(h.Deliverer.Outcomes);

        await e.ApplyAgReplicaHealthAsync(200002, "PRI", new[]
        {
            ReplicaRow(role: "SECONDARY", replica: "NODE1", isLocal: true),
            ReplicaRow(role: "SECONDARY", replica: "NODE2"),
        }, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Failover", fired.MetricName);
    }

    [Fact]
    public async Task AgReplicaDisconnected_RefireOff_IsAPureEdge()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Default is off, so a replica down for a week still announces exactly once. */
        h.Now = h.Now.AddHours(8);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AgReplicaDisconnected_Refire_ReAnnouncesUnderTheSameMetricName_ThenReconnectClearsTheClock()
    {
        var h = new Harness { AgDisconnectRefireMinutes = 10 };
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Inside the window: quiet. */
        h.Now = h.Now.AddMinutes(5);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Past it: re-announce under the SAME metric name, so webhook automation keyed on it re-triggers. */
        h.Now = h.Now.AddMinutes(6);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "DISCONNECTED") }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("AG Replica Disconnected", h.Deliverer.Outcomes[1].MetricName);
        Assert.Contains("STILL disconnected", h.Deliverer.Outcomes[1].ShortMessage, StringComparison.Ordinal);

        /* #2426, caught in review: the DETAIL has to say it too, and this is the assertion that pays for the
           branch. ShortMessage is the interactive toast body and reaches neither the history row nor the
           email — DarlingAlertDeliverer forwards DetailText — so pinning only the short message would leave
           an operator opening Alert Detail on the sixth re-announcement reading text byte-identical to the
           first notice, which is the problem the knob exists to end. Both halves asserted, in both
           directions: the re-fire says so and names the interval, and the opening EDGE does not, or every
           first notice would claim to be a repeat. */
        Assert.Contains("STILL DISCONNECTED", h.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);
        Assert.Contains("re-alerting every 10 min", h.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("STILL", h.Deliverer.Outcomes[0].DetailText, StringComparison.Ordinal);

        /* Reconnect clears the clock, so a later outage starts a fresh episode rather than re-firing late. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(connected: "CONNECTED") }, Ct);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
        Assert.Equal("AG Replica Reconnected", h.Deliverer.Outcomes[2].MetricName);
    }

    /* ---------------- #991: gating and forget ---------------- */

    [Fact]
    public async Task AgAlerts_MasterSwitchOff_FiresNothing_AndTheAlertsSwitchGatesThemToo()
    {
        var h = new Harness { NotifyAgHealth = false };
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "SECONDARY") }, Ct);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "PRIMARY", connected: "DISCONNECTED") }, Ct);
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 9999, suspended: true) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* The master alerts switch gates the family as well, independently of the AG toggle. */
        var h2 = new Harness();
        h2.Settings.AlertsEnabled = false;
        var e2 = h2.Build();
        await e2.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "SECONDARY") }, Ct);
        await e2.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "PRIMARY") }, Ct);
        Assert.Empty(h2.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AgAlerts_ThresholdsAreReadLive_SoAStoreEditTakesEffectOnTheNextSweep()
    {
        var h = new Harness { AgLagAlertSeconds = 300 };
        var e = h.Build();

        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 120) }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Operator tightens the threshold in the viewer; the service re-reads it by reference, no restart. */
        h.AgLagAlertSeconds = 60;
        await e.ApplyAgDatabaseHealthAsync(ServerId, Name, new[] { DatabaseRow(lagSeconds: 120) }, Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AgAlerts_Forget_ReleasesTheAgClaim_ButKeepsTheGroupsEdgeState()
    {
        var h = new Harness();
        var e = h.Build();

        const int nodeA = 100001;
        const int nodeB = 100002;

        /* NODE-A claims the AG and establishes a role baseline; NODE-B sees the same AG but defers. */
        await e.ApplyAgReplicaHealthAsync(nodeA, "NODE-A", new[] { ReplicaRow(role: "PRIMARY") }, Ct);
        await e.ApplyAgReplicaHealthAsync(nodeB, "NODE-B", new[] { ReplicaRow(role: "PRIMARY") }, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* NODE-A is removed from monitoring. Its CLAIM is released so a survivor can take over, but the
           AG's edge state is deliberately kept: the group still exists and NODE-B is still watching it.
           Dropping the state here would re-baseline a live group and swallow the next failover. */
        e.Forget(nodeA);

        /* NODE-B takes over and sees the role change against the state NODE-A left behind — so the failover
           is reported exactly once, by the new owner, with the correct previous role. */
        await e.ApplyAgReplicaHealthAsync(nodeB, "NODE-B", new[] { ReplicaRow(role: "SECONDARY") }, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("AG Failover", fired.MetricName);
        Assert.Equal("PRIMARY", fired.ThresholdValue);
        Assert.Equal("SECONDARY", fired.CurrentValue);
        Assert.Equal(nodeB.ToString(System.Globalization.CultureInfo.InvariantCulture), fired.ServerKey);
    }

    [Fact]
    public async Task AgAlerts_FireUnderTheRealServerKey_SoPerServerDeliveryAndHistoryStillCorrelate()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "SECONDARY") }, Ct);
        await e.ApplyAgReplicaHealthAsync(ServerId, Name, new[] { ReplicaRow(role: "PRIMARY") }, Ct);

        /* The AG grain lives in the alert TEXT; the serverKey stays the real server_id so the deliverer's
           per-server delivery-mode override (#1236) and the history correlation keep working. */
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(Key, fired.ServerKey);
        Assert.Equal(Name, fired.ServerName);
        Assert.Contains(Ag, fired.DetailText, StringComparison.Ordinal);
        Assert.Contains(Replica, fired.DetailText, StringComparison.Ordinal);
    }

    /* ---------------- master switch ---------------- */

    [Fact]
    public async Task AlertsDisabled_ConnectionEdge_TracksStateButDoesNotFire_AndReEnableResumes()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        /* Disabled: transitions are tracked (so re-enabling has a correct baseline) but nothing fires. */
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: false, error: "x", Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Re-enable, then recover: the tracked Offline baseline makes this a real Offline->Online, so
           "Server Restored" fires (state was not lost while disabled). */
        h.Settings.AlertsEnabled = true;
        await e.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        var restored = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Server Restored", restored.MetricName);
    }

    [Fact]
    public async Task AlertsDisabled_EvaluateStoreAlerts_ShortCircuitsBeforeAnyStoreRead()
    {
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        /* The master gate returns before touching the store, so a null data source is never dereferenced;
           if the gate were ever moved after the read this NREs (i.e. still fails, flagging the regression). */
        await e.EvaluateStoreAlertsAsync(null!, ServerId, Name, connected: true, Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    /* ---------------- resolution-record shape + engine wiring (finding #4) ---------------- */

    [Fact]
    public void BuildResolutionRecord_MirrorsDashboardClearedRowShape()
    {
        var record = DarlingSelfAlertEvaluator.BuildResolutionRecord(
            new AlertResolution(Key, Name, "High CPU", "CPU Resolved", $"{Name}: Total CPU back to 12%"));

        Assert.Equal(Key, record.ServerId);
        Assert.Equal(Name, record.ServerName);
        Assert.Equal("CPU Resolved", record.MetricName);           /* the "…Resolved/Cleared" title, Dashboard shape */
        Assert.Equal($"{Name}: Total CPU back to 12%", record.DetailText);
        Assert.True(record.AlertSent);
        Assert.Equal("tray", record.NotificationType);
        Assert.Null(record.SendError);
        Assert.False(record.Muted);
    }

    [Fact]
    public async Task EngineResolution_WritesResolvedHistoryRow_ThroughBuildResolutionRecord()
    {
        /* Replicates DarlingWorker.BuildAlertEngine's resolution wiring: the shared engine's resolution
           callback writes a resolved-flavored history row (finding #4 — previously it only logged). */
        var settings = new FakeSettings { CpuEnabled = true };
        var history = new FakeHistoryStore();
        var deliverer = new RecordingDeliverer();
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        var engine = new AlertEngine(
            settings, new StubReadAdapter(), new StubStateStore(), deliverer,
            isAlertMuted: _ => false,
            failedJobsFetcher: null,
            resolutionCallback: async (resolution, _) =>
                await history.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(resolution)),
            logger: null,
            utcNow: () => now);

        /* Fire: total CPU 90 >= 80. */
        await engine.EvaluateServerAsync(new AlertServerSnapshot(Key, Name, IsOnline: true, 90, 90, false, false), Ct);
        Assert.Single(deliverer.Outcomes);
        Assert.Empty(history.Records); /* no resolution yet */

        /* Clear: CPU back below threshold => the engine emits a resolution => a history row is written. */
        now = now.AddMinutes(1);
        await engine.EvaluateServerAsync(new AlertServerSnapshot(Key, Name, IsOnline: true, 10, 10, false, false), Ct);
        var resolved = Assert.Single(history.Records);
        Assert.Equal("CPU Resolved", resolved.MetricName);
        Assert.Equal("tray", resolved.NotificationType);
    }

    private sealed class StubReadAdapter : IAlertReadAdapter
    {
        public Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<BlockedProcessAlertRow>());
        public Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult<CurrentBlockingWaitResult?>(null);
        public Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DeadlockAlertRow>());
        public Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(string serverKey, double thresholdMs, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<PoisonWaitDelta>());
        public Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
            string serverKey, int thresholdMinutes, int maxResults,
            bool excludeSpServerDiagnostics, bool excludeWaitFor, bool excludeBackups, bool excludeMiscWaits, bool excludeCdc,
            IReadOnlyList<string> excludedDatabases, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<LongRunningQueryInfo>());
        public Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<VolumeFreeSpaceInfo>());

        /* #2349: empty on purpose. These tests exercise other alerts, and a fabricated file would
           make the file-growth gate fire inside an unrelated scenario. */
        public Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(
            string serverKey, int lookbackMinutes, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DatabaseFileGrowthInfo>());
        public Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult<TempDbSpaceInfo?>(null);
        public Task<List<PvsPressureInfo>> GetPvsPressureAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<PvsPressureInfo>());
        public Task<AnomalousJobsResult> GetAnomalousJobsAsync(string serverKey, int multiplier, CancellationToken cancellationToken = default) =>
            Task.FromResult(new AnomalousJobsResult(SnapshotIsFresh: true, new List<AnomalousJobInfo>()));
        public Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DatabaseStateInfo>());

        public Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<ForcePlanFailureInfo>());
    }

    private sealed class StubStateStore : IAlertStateStore
    {
        public Task<int?> LoadEdgeTriggerWatermarkAsync(string serverKey, string metricName) => Task.FromResult<int?>(null);
        public Task SaveEdgeTriggerWatermarkAsync(string serverKey, string metricName, int watermark) => Task.CompletedTask;
        public Task<DateTime?> LoadFailedJobWatermarkAsync(string serverKey) => Task.FromResult<DateTime?>(null);
        public Task SaveFailedJobWatermarkAsync(string serverKey, DateTime watermark) => Task.CompletedTask;
        public Task SaveDatabaseStateAlertedAsync(string serverKey, string databaseName, string effectiveState) => Task.CompletedTask;
        public Task ClearDatabaseStateAlertedAsync(string serverKey, string databaseName) => Task.CompletedTask;

        /* #2216: the self-alert paths carry no fingerprintable incidents, so the engine never accumulates
           against this stub — it exists to satisfy the seam. */
        public Task<IReadOnlyDictionary<string, IncidentOccurrenceState>> LoadIncidentOccurrencesAsync(string serverKey, string metricName) =>
            Task.FromResult<IReadOnlyDictionary<string, IncidentOccurrenceState>>(
                new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal));

        public Task SaveIncidentOccurrencesAsync(string serverKey, string metricName, IReadOnlyDictionary<string, IncidentOccurrenceState> states) => Task.CompletedTask;
    }

    /* ---------------- live collection_log reads (gated on DARLING_TEST_PG) ---------------- */

    private const int LiveServerId = -770077;

    [Fact]
    public async Task LiveStoreReads_ComputeCollectionStoppedAndCaptureDown()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live self-alert store reads.");

        var ct = Ct;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            /* One old SUCCESS (45 min ago), then 12 recent ERRORs — the last 10 runs are all failures and
               the last success is well past the staleness window. */
            long logId = 9_000_000;
            await InsertLogAsync(connection, ct, logId++, "wait_stats", utcNow.AddMinutes(-45), "SUCCESS");
            for (int i = 0; i < 12; i++)
            {
                await InsertLogAsync(connection, ct, logId++, "wait_stats", utcNow.AddMinutes(-2), "ERROR");
            }

            var (lastSuccess, recentRuns, recentSuccess) = await DarlingSelfAlertEvaluator.ReadCollectionSignalsAsync(
                postgres, LiveServerId, DarlingSelfAlertEvaluator.ConsecutiveFailureThreshold, ct);

            Assert.NotNull(lastSuccess);
            Assert.Equal(DarlingSelfAlertEvaluator.ConsecutiveFailureThreshold, recentRuns);
            Assert.Equal(0, recentSuccess);
            Assert.True(DarlingSelfAlertEvaluator.IsCollectionStopped(
                lastSuccess, recentRuns, recentSuccess, DateTime.UtcNow, out _));

            /* Full path: EvaluateStoreAlertsAsync must NOT fire collection-stopped until the server has been
               online this run (the restart-staleness guard), then must fire once it has. Real-time clock so
               the 45-minute-old success reads as stale against the seeded rows. */
            var h = new Harness { Now = DateTime.UtcNow };
            var evaluator = h.Build();

            await evaluator.EvaluateStoreAlertsAsync(postgres, LiveServerId, Name, connected: true, ct);
            Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == "Collection Stopped");

            await evaluator.ApplyConnectionOutcomeAsync(LiveServerId, Name, online: true, error: null, ct); /* arm */
            await evaluator.EvaluateStoreAlertsAsync(postgres, LiveServerId, Name, connected: true, ct);
            Assert.Contains(h.Deliverer.Outcomes, o => o.MetricName == "Collection Stopped");

            /* Capture-down: latest deadlocks run is SESSION_MISSING, latest blocked_process_report is fine. */
            await InsertLogAsync(connection, ct, logId++, "blocked_process_report", utcNow.AddMinutes(-1), "SUCCESS");
            await InsertLogAsync(connection, ct, logId++, "deadlocks", utcNow.AddMinutes(-1), "SESSION_MISSING");

            var missing = await DarlingSelfAlertEvaluator.ReadMissingCaptureSessionsAsync(postgres, LiveServerId, ct);
            Assert.Equal(new[] { "Deadlock" }, missing);

            await evaluator.EvaluateStoreAlertsAsync(postgres, LiveServerId, Name, connected: true, ct);
            Assert.Contains(h.Deliverer.Outcomes, o => o.MetricName == "Capture Down");

            bodySucceeded = true;
        }
        finally
        {
            /* Fresh connection + body-aware masking (#1794): this class showed the "Connection is not
               open" signature on a reused local store, and cleanup on the body's connection is exactly
               how that masks the real failure and strands the seeded rows. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteLiveRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>
    /// EXECUTES both Availability Group reads against a real Postgres (#991). This exists because the two
    /// grains were briefly read as two statements over a SINGLE command to save a round trip, and that is
    /// rejected by PostgreSQL: Npgsql only splits multi-statement text into a batch when it parses the SQL for
    /// NAMED placeholders, so with the positional ($1) parameters these reads use it sends one
    /// extended-protocol Parse and the server answers "cannot insert multiple commands into a prepared
    /// statement". Every AG path is failure-isolated, so that defect did not crash anything — it would have
    /// logged one error per server per sweep with the whole alert family silently dead. No unit test can catch
    /// that shape; only running the SQL can. Also pins the freshness signal, the newest-snapshot filter, and
    /// the NULL-identity drop.
    /// </summary>
    [Fact]
    public async Task LiveStoreReads_ExecuteBothAgQueries_AndReturnTheNewestSnapshot()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live AG store reads.");

        var ct = Ct;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLiveAgRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var older = utcNow.AddMinutes(-5);

            /* An older snapshot that must be filtered out by the MAX(collection_time) predicate, a current one
               that must come back, and a current row with a NULL identity column that must be dropped. */
            await InsertAgReplicaAsync(connection, ct, older, "AG1", "NODE1", "PRIMARY", "CONNECTED");
            await InsertAgReplicaAsync(connection, ct, utcNow, "AG1", "NODE1", "SECONDARY", "CONNECTED");
            await InsertAgReplicaAsync(connection, ct, utcNow, "AG1", "NODE2", "PRIMARY", "DISCONNECTED");
            await InsertAgReplicaAsync(connection, ct, utcNow, null, "NODE3", "SECONDARY", "CONNECTED");

            var (replicaTime, replicas) =
                await DarlingSelfAlertEvaluator.ReadLatestAgReplicaStatesAsync(postgres, LiveServerId, ct);

            Assert.NotNull(replicaTime);
            Assert.Equal(2, replicas.Count);                       /* the un-keyable NULL row is dropped */
            Assert.DoesNotContain(replicas, r => r.RoleDesc == "PRIMARY" && r.ReplicaServerName == "NODE1");
            Assert.Contains(replicas, r => r.ReplicaServerName == "NODE2" && r.ConnectedStateDesc == "DISCONNECTED");

            await InsertAgDatabaseAsync(connection, ct, older, "AG1", "Sales", "NODE2", 10, 10, false, null);
            await InsertAgDatabaseAsync(connection, ct, utcNow, "AG1", "Sales", "NODE2", 900, 4096, false, null);
            await InsertAgDatabaseAsync(connection, ct, utcNow, "AG1", "Orders", "NODE2", null, null, true, "SUSPEND_FROM_USER");

            var (databaseTime, databases) =
                await DarlingSelfAlertEvaluator.ReadLatestAgDatabaseReplicaStatesAsync(postgres, LiveServerId, ct);

            Assert.NotNull(databaseTime);
            Assert.Equal(2, databases.Count);
            var sales = Assert.Single(databases, d => d.DatabaseName == "Sales");
            Assert.Equal(900, sales.SecondaryLagSeconds);
            Assert.Equal(4096, sales.RedoQueueSizeKb);
            var orders = Assert.Single(databases, d => d.DatabaseName == "Orders");
            Assert.True(orders.IsSuspended);
            Assert.Null(orders.SecondaryLagSeconds);
            Assert.Equal("SUSPEND_FROM_USER", orders.SuspendReasonDesc);

            /* End to end through the sweep entry point, against rows that are genuinely fresh: the disconnect
               and the suspend are first sightings (silent baselines), and the 900-second lag is past the
               300-second default, so exactly one alert lands. */
            var h = new Harness { Now = DateTime.UtcNow };
            var evaluator = h.Build();
            await evaluator.EvaluateStoreAlertsAsync(postgres, LiveServerId, Name, connected: true, ct);

            Assert.Contains(h.Deliverer.Outcomes, o => o.MetricName == "AG Sync Fell Behind");
            Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == "AG Replica Disconnected");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteLiveAgRowsAsync(cleanup, cleanupCt);
                await DeleteLiveRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task InsertAgReplicaAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime time,
        string? agName, string replicaServerName, string roleDesc, string connectedStateDesc)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name,
    replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc,
    synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url)
VALUES (0, $1, $2, $3, $4, $5, $6, 'ONLINE', $7, 'ONLINE', 'HEALTHY', 'SYNCHRONOUS_COMMIT', 'AUTOMATIC', NULL)", connection);
        command.Parameters.AddWithValue(time);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(Name);
        command.Parameters.AddWithValue((object?)agName ?? DBNull.Value);
        command.Parameters.AddWithValue(replicaServerName);
        command.Parameters.AddWithValue(roleDesc);
        command.Parameters.AddWithValue(connectedStateDesc);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAgDatabaseAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime time,
        string agName, string databaseName, string replicaServerName,
        long? secondaryLagSeconds, long? redoQueueSize, bool isSuspended, string? suspendReason)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name,
    database_name, replica_server_name, is_local, synchronization_state_desc, last_hardened_lsn,
    last_commit_lsn, log_send_queue_size, redo_queue_size, log_send_rate, redo_rate, is_suspended,
    suspend_reason_desc, availability_mode_desc, secondary_lag_seconds)
VALUES (0, $1, $2, $3, $4, $5, $6, FALSE, 'SYNCHRONIZING', NULL, NULL, 0, $7, 0, 0, $8, $9,
    'SYNCHRONOUS_COMMIT', $10)", connection);
        command.Parameters.AddWithValue(time);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(Name);
        command.Parameters.AddWithValue(agName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(replicaServerName);
        command.Parameters.AddWithValue((object?)redoQueueSize ?? DBNull.Value);
        command.Parameters.AddWithValue(isSuspended);
        command.Parameters.AddWithValue((object?)suspendReason ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)secondaryLagSeconds ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One command per statement, for the same reason the reads under test are: multi-statement text
    /// is a trap worth not re-laying even in cleanup.</summary>
    private static async Task DeleteLiveAgRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var id = LiveServerId.ToString(CultureInfo.InvariantCulture);
        foreach (var table in new[] { "ag_replica_states", "ag_database_replica_states" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {id}", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task InsertLogAsync(
        NpgsqlConnection connection, CancellationToken ct, long logId, string collector, DateTime time, string status)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 0, $6, NULL, 0, 0, 0)", connection);
        command.Parameters.AddWithValue(logId);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(Name);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(time);
        command.Parameters.AddWithValue(status);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {LiveServerId.ToString(CultureInfo.InvariantCulture)};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---------------- #1681: firings are logged, not just recoveries ---------------- */

    /// <summary>
    /// RecordResolutionAsync has always logged at Information, so the service log showed "... Recovered" with
    /// nothing preceding it - a spontaneous recovery from a condition that never appeared, which is worse than
    /// logging neither half. Every self-alert fired silently through the same path: compression stuck, disk
    /// pressure, capture-down, agent-not-running, collection-health.
    /// </summary>
    [Fact]
    public async Task CompressionStuck_Firing_IsLoggedAtWarning()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), new RearmRecorder().Delegate, Ct);

        var warnings = h.Log.Entries
            .Where(x => x.Level == Microsoft.Extensions.Logging.LogLevel.Warning)
            .ToList();

        Assert.NotEmpty(warnings);
        Assert.Contains(warnings, x => x.Message.Contains("Compression Job Stuck", StringComparison.Ordinal));
    }

    /// <summary>
    /// A muted alert still logs, flagged. Muting suppresses the notification CHANNELS, not the operator's ability
    /// to find the event afterwards - suppressing both would make a muted condition invisible everywhere at once.
    /// </summary>
    [Fact]
    public async Task CompressionStuck_Firing_IsLoggedEvenWhenMuted()
    {
        var h = new Harness { Muted = true };
        var e = h.Build();

        await e.ApplyCompressionJobsStuckAsync(Stuck(1001), new RearmRecorder().Delegate, Ct);

        var warnings = h.Log.Entries
            .Where(x => x.Level == Microsoft.Extensions.Logging.LogLevel.Warning)
            .ToList();

        Assert.Contains(warnings, x => x.Message.Contains("[muted]", StringComparison.Ordinal));
    }

    /* ---------------- #2136 Store Job Over Cadence ---------------- */

    private static StoreJobCadenceReading CadenceJob(
        long id = 1028, long? durMs = 900_000, long schedMs = 3_600_000,
        string name = "policy_compression query_store_stats") =>
        new(id, name, durMs, schedMs);

    [Fact]
    public async Task JobOverCadence_WarningTier_FiresAtTheKnobPercent()
    {
        var h = new Harness();
        var e = h.Build();

        /* 900s of 3600s = exactly 25%, the shipped default — the boundary is inclusive. */
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob() }, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.JobCadenceMetric, fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("storejob:1028", fired.ServerKey);  /* prefixed so it never parses as a server_id */
        Assert.Contains("25% of its schedule interval", fired.ShortMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task JobOverCadence_UnderTheKnob_StaysSilent()
    {
        var h = new Harness();
        var e = h.Build();

        /* 249s of 3600s ≈ 7% — the production store's worst job today. Must not fire at the default 25. */
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob(durMs: 249_000) }, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task JobOverCadence_At100Percent_EscalatesToCritical()
    {
        var h = new Harness();
        var e = h.Build();

        /* 3700s of 3600s — the job outruns its own cadence; runs back up behind each other. */
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob(durMs: 3_700_000) }, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
    }

    [Fact]
    public async Task JobOverCadence_HonorsTheLiveKnob()
    {
        var h = new Harness { StoreJobCadenceWarnPercent = 50 };
        var e = h.Build();

        /* 30% breaches the default 25 but not the configured 50 — the seam is read live. */
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob(durMs: 1_080_000) }, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task JobOverCadence_NoScheduleOrNoRun_IsSkippedWithoutJudging()
    {
        var h = new Harness();
        var e = h.Build();

        /* A one-shot job (no interval) and a job with no completed run have no cadence to breach. */
        await e.ApplyStoreJobCadenceAsync(new[]
        {
            CadenceJob(id: 1, schedMs: 0),
            CadenceJob(id: 2, durMs: null),
        }, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task JobOverCadence_IsAStandingCondition_ReFiresOnlyOnCooldown_AndWritesOneRecoveryRow()
    {
        var h = new Harness();
        var e = h.Build();

        /* Breach: fires once. */
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob() }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Still breaching one minute later — inside the 5-minute cooldown, no re-fire. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob() }, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Still breaching past the cooldown — re-fires under the SAME metric name. */
        h.Now = h.Now.AddMinutes(10);
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob() }, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* A later run comes back under: exactly one recovery audit row, and a fresh breach fires again. */
        await e.ApplyStoreJobCadenceAsync(new[] { CadenceJob(durMs: 200_000) }, Ct);
        var recovered = Assert.Single(h.History.Records);
        Assert.Equal("Store Job Cadence Recovered", recovered.MetricName);
    }
}
