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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the two load-bearing seams of the viewer's headless in-app alert toasts — the "new since last-seen"
/// watermark and the per-condition cooldown gate in <see cref="AlertToastCoordinator"/> — without WPF,
/// Postgres, or a real clock. The tray rendering itself (Hardcodet balloons) is not unit-testable; this
/// covers all the decision logic that decides IF and WHICH polled alert rows become toasts.
/// </summary>
public sealed class ViewerAlertToastCoordinatorTests
{
    private static readonly DateTime T0 = new(2026, 7, 7, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(20);
    private static readonly TimeSpan Cooldown = TimeSpan.FromMinutes(5);

    private static ViewerAlertRow Row(
        DateTime alertTime, int serverId, string metric, bool muted = false, string? detail = null) => new()
    {
        AlertTime = alertTime,
        ServerId = serverId,
        ServerName = $"Server{serverId}",
        MetricName = metric,
        CurrentValue = 90,
        ThresholdValue = 80,
        AlertSent = true,
        NotificationType = "tray",
        Muted = muted,
        DetailText = detail,
    };

    [Fact]
    public void Ctor_RejectsNonPositiveRetention()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new AlertToastCoordinator(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AlertToastCoordinator(TimeSpan.FromMinutes(-1)));
    }

    [Fact]
    public void Prime_SuppressesRowsThatExistedAtStartup()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var existing = new[] { Row(T0, 1, "High CPU"), Row(T0.AddMinutes(1), 2, "Blocking Detected") };
        coordinator.Prime(existing);

        /* Re-polling the exact rows that were present at startup must produce no toasts. */
        var toasts = coordinator.SelectToasts(existing, T0.AddMinutes(2), Cooldown);

        Assert.Empty(toasts);
    }

    [Fact]
    public void SelectToasts_NewRowAfterPrime_IsToasted()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        coordinator.Prime(new[] { Row(T0, 1, "High CPU") });

        var fresh = Row(T0.AddMinutes(1), 2, "Deadlocks Detected");
        var toasts = coordinator.SelectToasts(
            new[] { Row(T0, 1, "High CPU"), fresh }, T0.AddMinutes(1), Cooldown);

        Assert.Single(toasts);
        Assert.Same(fresh, toasts[0]);
    }

    [Fact]
    public void SelectToasts_SameRowAcrossTwoPolls_ToastsExactlyOnce()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var row = Row(T0, 1, "High CPU");

        var first = coordinator.SelectToasts(new[] { row }, T0, Cooldown);
        var second = coordinator.SelectToasts(new[] { row }, T0.AddMinutes(1), Cooldown);

        Assert.Single(first);
        Assert.Empty(second);
    }

    [Fact]
    public void SelectToasts_MutedRow_IsNeverToasted()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var muted = Row(T0, 1, "High CPU", muted: true);

        var toasts = coordinator.SelectToasts(new[] { muted }, T0, Cooldown);

        Assert.Empty(toasts);
    }

    [Fact]
    public void SelectToasts_MutedRow_IsMarkedSeen_SoAnUnmuteInTheStoreDoesNotReplay()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var muted = Row(T0, 1, "High CPU", muted: true);
        coordinator.SelectToasts(new[] { muted }, T0, Cooldown);

        /* Same identity re-read later (even if it came back unmuted) stays suppressed — the row was
           already considered. */
        var unmutedSameIdentity = Row(T0, 1, "High CPU");
        var toasts = coordinator.SelectToasts(new[] { unmutedSameIdentity }, T0.AddMinutes(1), Cooldown);

        Assert.Empty(toasts);
    }

    [Fact]
    public void SelectToasts_TwoDistinctRowsOfSameCondition_WithinCooldown_ToastsOnlyTheFirst()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var earlier = Row(T0, 1, "High CPU");
        var later = Row(T0.AddMinutes(1), 1, "High CPU"); /* distinct row, same server+metric */

        var toasts = coordinator.SelectToasts(new[] { earlier, later }, T0.AddMinutes(1), Cooldown);

        Assert.Single(toasts);
        Assert.Same(earlier, toasts[0]); /* oldest-first: the earliest wins the cooldown slot */
    }

    [Fact]
    public void SelectToasts_ProcessesOldestFirst_RegardlessOfInputOrder()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var earlier = Row(T0, 1, "High CPU");
        var later = Row(T0.AddMinutes(1), 1, "High CPU");

        /* Newest-first input (the read's ORDER BY alert_time DESC) must still admit the OLDEST. */
        var toasts = coordinator.SelectToasts(new[] { later, earlier }, T0.AddMinutes(1), Cooldown);

        Assert.Single(toasts);
        Assert.Same(earlier, toasts[0]);
    }

    [Fact]
    public void SelectToasts_DifferentConditions_BothToastWithinCooldown()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var cpuOnA = Row(T0, 1, "High CPU");
        var cpuOnB = Row(T0, 2, "High CPU");             /* different server */
        var blockingOnA = Row(T0, 1, "Blocking Detected"); /* different metric */

        var toasts = coordinator.SelectToasts(new[] { cpuOnA, cpuOnB, blockingOnA }, T0, Cooldown);

        Assert.Equal(3, toasts.Count);
    }

    [Fact]
    public void SelectToasts_CooldownElapsed_ReToastsSameCondition()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var first = coordinator.SelectToasts(new[] { Row(T0, 1, "High CPU") }, T0, Cooldown);

        /* A distinct later row of the same condition, polled after the cooldown window, toasts again. */
        var later = Row(T0.AddMinutes(6), 1, "High CPU");
        var second = coordinator.SelectToasts(new[] { later }, T0.AddMinutes(6), Cooldown);

        Assert.Single(first);
        Assert.Single(second);
        Assert.Same(later, second[0]);
    }

    [Fact]
    public void SelectToasts_ZeroCooldown_AdmitsEveryNewRowOfAConditionInOnePoll()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var a = Row(T0, 1, "High CPU");
        var b = Row(T0.AddMinutes(1), 1, "High CPU");
        var c = Row(T0.AddMinutes(2), 1, "High CPU");

        var toasts = coordinator.SelectToasts(new[] { a, b, c }, T0.AddMinutes(2), TimeSpan.Zero);

        Assert.Equal(3, toasts.Count);
    }

    [Fact]
    public void SelectToasts_ResolvedRow_IsSeverityAgnostic_AndStillSelected()
    {
        /* The coordinator does not filter by severity — the MainWindow renders a resolved row as a green
           styled card. A "Restored"/"Cleared"/"Resolved" row (unmuted) must be admitted. */
        var coordinator = new AlertToastCoordinator(Retention);
        var resolved = Row(T0, 1, "Server Restored");

        var toasts = coordinator.SelectToasts(new[] { resolved }, T0, Cooldown);

        Assert.Single(toasts);
        Assert.True(toasts[0].IsResolved);
    }

    [Fact]
    public void SelectToasts_PrimedRowWithinRetention_StaysSuppressed()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        coordinator.Prime(new[] { Row(T0, 1, "High CPU") });

        /* Well inside the retention window: the primed row is still remembered, so it never toasts. */
        var toasts = coordinator.SelectToasts(new[] { Row(T0, 1, "High CPU") }, T0.AddMinutes(10), Cooldown);

        Assert.Empty(toasts);
    }

    [Fact]
    public void SelectToasts_SeenRowBeyondRetention_IsPruned_ThenReToasts()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        coordinator.Prime(new[] { Row(T0, 1, "High CPU") });

        /* A poll past the retention window prunes the stale seen key (an out-of-window row can never be
           re-read, so remembering it forever would leak). */
        coordinator.SelectToasts(Array.Empty<ViewerAlertRow>(), T0.AddMinutes(21), Cooldown);

        /* If that identity somehow re-appears it is treated as new again (bounded: retention exceeds the
           poll fetch window, so an in-window row is never pruned early). */
        var toasts = coordinator.SelectToasts(new[] { Row(T0, 1, "High CPU") }, T0.AddMinutes(21), Cooldown);

        Assert.Single(toasts);
    }

    /* ---------------- #3570: the viewer honors mute rules for its own channel ---------------- */

    /// <summary>
    /// The report, as a pin. The service re-fires "Agent Not Running" with <c>muted = false</c> — because it has
    /// not reloaded its cache yet, or its reload failed, or the beacon never reached it — and the viewer holds
    /// the rule the operator's Snooze just wrote. The toast must not appear: the tray is the viewer's channel and
    /// the viewer's rule set decides. Before #3570 only <c>row.Muted</c> was consulted and this toasted.
    /// </summary>
    [Fact]
    public void SelectToasts_RowCoveredByAViewerRule_IsNotToasted_EvenWhenTheServiceLeftItUnmuted()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var refire = Row(T0.AddMinutes(5), 1, "Agent Not Running", muted: false);
        var snooze = ViewerDataService.BuildTraySnoozeRule("Server1", "Agent Not Running", TimeSpan.FromHours(4), T0);

        var toasts = coordinator.SelectToasts(new[] { refire }, T0.AddMinutes(5), Cooldown, new[] { snooze });

        Assert.Empty(toasts);
    }

    /// <summary>Null and empty rule sets are the pre-#3570 behavior exactly: the service's flag alone decides.</summary>
    [Fact]
    public void SelectToasts_NoViewerRules_LeavesAnUnmutedRowToasting()
    {
        var row = Row(T0, 1, "Agent Not Running");

        Assert.Single(new AlertToastCoordinator(Retention).SelectToasts(new[] { row }, T0, Cooldown, muteRules: null));
        Assert.Single(new AlertToastCoordinator(Retention).SelectToasts(new[] { row }, T0, Cooldown, Array.Empty<MuteRule>()));
    }

    /// <summary>
    /// The viewer-side judgement is <see cref="MuteRule.MatchesAt"/> on the coordinator's injected clock: a rule
    /// whose expiry has passed suppresses nothing, judged at the SAME instant the rest of the decision is.
    /// </summary>
    [Fact]
    public void SelectToasts_ExpiredViewerRule_DoesNotSuppress()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var snooze = ViewerDataService.BuildTraySnoozeRule("Server1", "Agent Not Running", TimeSpan.FromMinutes(15), T0);

        /* Judged one second after the 15 m snooze lapsed. */
        var toasts = coordinator.SelectToasts(
            new[] { Row(T0.AddMinutes(16), 1, "Agent Not Running") }, T0.AddMinutes(15).AddSeconds(1), Cooldown, new[] { snooze });

        Assert.Single(toasts);
    }

    [Fact]
    public void SelectToasts_DisabledViewerRule_DoesNotSuppress()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var rule = ViewerDataService.BuildTraySnoozeRule("Server1", "Agent Not Running", TimeSpan.FromHours(4), T0);
        rule.Enabled = false;

        var toasts = coordinator.SelectToasts(new[] { Row(T0, 1, "Agent Not Running") }, T0, Cooldown, new[] { rule });

        Assert.Single(toasts);
    }

    /// <summary>
    /// Scope is the shared matcher's: server name case-insensitive, metric exact (by name), a rule for another
    /// server or another metric leaves this row alone. Pinned here so the tray can never be broader OR narrower
    /// than the channels the service mutes with the same rule.
    /// </summary>
    [Fact]
    public void SelectToasts_ViewerRuleScope_IsTheSharedMatchersScope()
    {
        var row = Row(T0, 1, "Agent Not Running");

        var otherServer = ViewerDataService.BuildTraySnoozeRule("Server2", "Agent Not Running", TimeSpan.FromHours(4), T0);
        Assert.Single(new AlertToastCoordinator(Retention).SelectToasts(new[] { row }, T0, Cooldown, new[] { otherServer }));

        var otherMetric = ViewerDataService.BuildTraySnoozeRule("Server1", "Failed Agent Job", TimeSpan.FromHours(4), T0);
        Assert.Single(new AlertToastCoordinator(Retention).SelectToasts(new[] { row }, T0, Cooldown, new[] { otherMetric }));

        var differentCase = ViewerDataService.BuildTraySnoozeRule("SERVER1", "agent not running", TimeSpan.FromHours(4), T0);
        Assert.Empty(new AlertToastCoordinator(Retention).SelectToasts(new[] { row }, T0, Cooldown, new[] { differentCase }));

        /* A whole-server silence (no metric) covers every metric on that server — the sidebar's one-click rule. */
        var silence = ViewerDataService.BuildServerSilenceRule("Server1");
        Assert.Empty(new AlertToastCoordinator(Retention).SelectToasts(new[] { row }, T0, Cooldown, new[] { silence }));
    }

    /// <summary>
    /// A pattern-scoped rule is judged over the dimensions <see cref="ViewerAlertRow.ToMuteContext"/> parses out
    /// of the row's detail text — the same pre-fill the "Mute This Alert" dialog reads — so a database-scoped
    /// mute the operator authored FROM a row covers that row's toast, and only rows about that database.
    /// </summary>
    [Fact]
    public void SelectToasts_PatternScopedViewerRule_IsJudgedOverTheRowsDetailText()
    {
        var rule = new MuteRule { MetricName = "Blocking Detected", DatabasePattern = "Sales" };

        var salesRow = Row(T0, 1, "Blocking Detected", detail: "Blocking detected\n  Database: SalesDb\n  Wait Type: LCK_M_S");
        Assert.Empty(new AlertToastCoordinator(Retention).SelectToasts(new[] { salesRow }, T0, Cooldown, new[] { rule }));

        var otherDbRow = Row(T0, 1, "Blocking Detected", detail: "Blocking detected\n  Database: Payroll");
        Assert.Single(new AlertToastCoordinator(Retention).SelectToasts(new[] { otherDbRow }, T0, Cooldown, new[] { rule }));

        /* No detail text at all: the pattern dimension is unknown, the rule cannot claim it, the row toasts. */
        var bareRow = Row(T0, 1, "Blocking Detected");
        Assert.Single(new AlertToastCoordinator(Retention).SelectToasts(new[] { bareRow }, T0, Cooldown, new[] { rule }));
    }

    /// <summary>
    /// A row the viewer's rule suppressed is marked seen exactly like a service-muted one, so when the snooze
    /// lapses the rows it covered do not replay as a storm — only rows that arrive AFTER expiry can toast.
    /// </summary>
    [Fact]
    public void SelectToasts_ViewerSuppressedRow_IsMarkedSeen_SoRuleExpiryDoesNotReplayIt()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var snooze = ViewerDataService.BuildTraySnoozeRule("Server1", "Agent Not Running", TimeSpan.FromMinutes(15), T0);
        var covered = Row(T0.AddMinutes(5), 1, "Agent Not Running");

        Assert.Empty(coordinator.SelectToasts(new[] { covered }, T0.AddMinutes(5), Cooldown, new[] { snooze }));

        /* The snooze has lapsed and the same row is re-read (the poll window overlaps): still nothing. */
        Assert.Empty(coordinator.SelectToasts(new[] { covered }, T0.AddMinutes(16), Cooldown, new[] { snooze }));

        /* A NEW row after expiry toasts — the condition is live again and the operator asked for 15 m, not forever. */
        Assert.Single(coordinator.SelectToasts(new[] { Row(T0.AddMinutes(17), 1, "Agent Not Running") }, T0.AddMinutes(17), Cooldown, new[] { snooze }));
    }

    /// <summary>
    /// The viewer's rule set and the service's flag are ORed: either alone suppresses, and a rule covering row A
    /// says nothing about row B on another server in the same poll.
    /// </summary>
    [Fact]
    public void SelectToasts_ViewerRulesAndServiceFlag_AreIndependentPerRow()
    {
        var coordinator = new AlertToastCoordinator(Retention);
        var snooze = ViewerDataService.BuildTraySnoozeRule("Server1", "Agent Not Running", TimeSpan.FromHours(4), T0);

        var coveredByRule = Row(T0, 1, "Agent Not Running");
        var mutedByService = Row(T0, 2, "High CPU", muted: true);
        var neither = Row(T0, 3, "Agent Not Running");

        var toasts = coordinator.SelectToasts(new[] { coveredByRule, mutedByService, neither }, T0, Cooldown, new[] { snooze });

        var only = Assert.Single(toasts);
        Assert.Equal(3, only.ServerId);
    }

    /// <summary>A null entry in the rule list is skipped rather than thrown on — the filter runs inside the refresh loop.</summary>
    [Fact]
    public void IsMutedByViewerRules_SkipsNullEntries()
    {
        var row = Row(T0, 1, "Agent Not Running");
        var rules = new MuteRule[] { null!, ViewerDataService.BuildTraySnoozeRule("Server1", "Agent Not Running", TimeSpan.FromHours(1), T0) };

        Assert.True(AlertToastCoordinator.IsMutedByViewerRules(row, rules, T0));
        Assert.False(AlertToastCoordinator.IsMutedByViewerRules(row, new MuteRule[] { null! }, T0));
    }
}
