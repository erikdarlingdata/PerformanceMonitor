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
/// Pins the one-click "Silence This Server" / "Unsilence" logic (the sidebar shortcut over the multi-step
/// Manage Mute Rules dialog): the whole-server silence rule it writes and the predicate "Unsilence" uses to find
/// the rule(s) to remove. Pure domain logic (no WPF, no live Postgres) — the same discipline as the SQL pins.
/// The rule is keyed on the server's DISPLAY name because that's what the alert engine's mute context and the
/// alert rows carry (so the rule actually suppresses the alerts, matching the existing "Mute This Server").
/// </summary>
public sealed class ViewerServerSilenceTests
{
    [Fact]
    public void BuildServerSilenceRule_ScopesToServer_WithNoNarrowingPatterns_AndNoExpiry()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(1, "Prod SQL 1");

        Assert.Equal("Prod SQL 1", rule.ServerName);
        Assert.True(rule.Enabled);
        Assert.Null(rule.ExpiresAtUtc); /* indefinite — stays until Unsilence, mirroring Lite's per-server silence */
        Assert.Equal(ViewerDataService.ServerSilenceReason, rule.Reason);

        /* Every other pattern is null so the rule matches EVERY alert for the server. */
        Assert.Null(rule.MetricName);
        Assert.Null(rule.DatabasePattern);
        Assert.Null(rule.QueryTextPattern);
        Assert.Null(rule.WaitTypePattern);
        Assert.Null(rule.JobNamePattern);
    }

    [Fact]
    public void BuildServerSilenceRule_MatchesEveryAlertForThatServer_ButNotOtherServers()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(1, "Prod SQL 1");

        Assert.True(rule.Matches(new AlertMuteContext { ServerName = "Prod SQL 1", MetricName = "High CPU" }));
        Assert.True(rule.Matches(new AlertMuteContext { ServerName = "Prod SQL 1", MetricName = "Deadlocks Detected" }));
        Assert.False(rule.Matches(new AlertMuteContext { ServerName = "Prod SQL 2", MetricName = "High CPU" }));
    }

    [Fact]
    public void IsWholeServerSilence_TrueForABlanketServerRule_CaseInsensitiveOnTheName()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(1, "Prod SQL 1");

        Assert.True(ViewerDataService.IsWholeServerSilence(rule, 1, "Prod SQL 1"));
        Assert.True(ViewerDataService.IsWholeServerSilence(rule, 1, "prod sql 1")); /* name compare is case-insensitive */
    }

    [Fact]
    public void IsWholeServerSilence_FalseForADifferentServer()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(1, "Prod SQL 1");

        Assert.False(ViewerDataService.IsWholeServerSilence(rule, 1, "Prod SQL 2"));
    }

    [Fact]
    public void IsWholeServerSilence_FalseForANarrowedRule_SoUnsilenceLeavesSpecificMutesAlone()
    {
        /* A rule the operator authored to mute only ONE metric on the server is NOT a whole-server silence, so
           "Unsilence" must not remove it. */
        var narrowed = new MuteRule { ServerName = "Prod SQL 1", MetricName = "High CPU" };

        Assert.False(ViewerDataService.IsWholeServerSilence(narrowed, 1, "Prod SQL 1"));
    }

    [Fact]
    public void IsWholeServerSilence_FalseForAMetricOnlyRule_WithNoServerScope()
    {
        /* A metric-only (all-servers) mute has no ServerName, so it's never a per-server silence. */
        var metricOnly = new MuteRule { MetricName = "High CPU" };

        Assert.False(ViewerDataService.IsWholeServerSilence(metricOnly, 1, "Prod SQL 1"));
    }

    private static readonly IReadOnlyList<(int ServerId, string DisplayName)> TwoSameNamed =
        new[] { (7, "host1"), (8, "host1"), (9, "other") };

    private static MuteRule Legacy(string name) => new()
    {
        Id = "legacy", ServerName = name, Reason = "old", Enabled = true,
        ExpiresAtUtc = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
    };

    [Fact]
    public void SameNamedServers_IdKeyedSilence_IsWholeServerSilenceForOnlyItsOwnServer()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(7, "host1");

        Assert.Equal(7, rule.ServerId);
        Assert.Equal("host1", rule.ServerName);
        Assert.True(ViewerDataService.IsWholeServerSilence(rule, 7, "host1"));
        Assert.False(ViewerDataService.IsWholeServerSilence(rule, 8, "host1"));
    }

    [Fact]
    public void LegacyNameKeyedRule_StillSilencesEverySameNamedServer()
    {
        var rule = Legacy("host1");

        Assert.True(ViewerDataService.IsWholeServerSilence(rule, 7, "host1"));
        Assert.True(ViewerDataService.IsWholeServerSilence(rule, 8, "HOST1"));
        Assert.False(ViewerDataService.IsWholeServerSilence(rule, 9, "other"));
    }

    [Fact]
    public void PlanUnsilence_LegacyRule_IsDeletedAndTheOtherSameNamedServerKeepsAnIdKeyedSilence()
    {
        var plan = ViewerDataService.PlanUnsilence(new[] { Legacy("host1") }, 7, "host1", TwoSameNamed);

        Assert.Equal(new[] { "legacy" }, plan.DeleteRuleIds);
        var created = Assert.Single(plan.CreateRules);
        Assert.Equal(8, created.ServerId);
        Assert.Equal("host1", created.ServerName);
        Assert.Equal("old", created.Reason);
        Assert.True(created.Enabled);
        Assert.Equal(new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc), created.ExpiresAtUtc);
        Assert.Null(created.MetricName);
    }

    [Fact]
    public void PlanUnsilence_ServerListWithoutThisServer_KeepsTheLegacyRule_SoNoOtherServerIsUnsilenced()
    {
        /* The window's server list is empty while it loads. Deleting a legacy rule then would create no
           replacement for the other server it covers, and that server would stop being silenced. */
        var rules = new[] { Legacy("host1") };

        var empty = ViewerDataService.PlanUnsilence(rules, 7, "host1", Array.Empty<(int, string)>());
        Assert.Empty(empty.DeleteRuleIds);
        Assert.Empty(empty.CreateRules);
        Assert.True(empty.ServerListIncomplete);

        var withoutIt = ViewerDataService.PlanUnsilence(rules, 7, "host1", new[] { (8, "host1") });
        Assert.Empty(withoutIt.DeleteRuleIds);
        Assert.True(withoutIt.ServerListIncomplete);

        var complete = ViewerDataService.PlanUnsilence(rules, 7, "host1", TwoSameNamed);
        Assert.False(complete.ServerListIncomplete);
        Assert.Equal(new[] { "legacy" }, complete.DeleteRuleIds);
    }

    [Fact]
    public void PlanUnsilence_IdKeyedRule_IsDeletedEvenWhileTheServerListLoads()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(7, "host1");
        rule.Id = "mine";

        var plan = ViewerDataService.PlanUnsilence(new[] { rule }, 7, "host1", Array.Empty<(int, string)>());

        Assert.Equal(new[] { "mine" }, plan.DeleteRuleIds);
        Assert.False(plan.ServerListIncomplete);
    }

    [Fact]
    public void PlanUnsilence_IdKeyedRuleOnly_DeletesItAndCreatesNothing()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(7, "host1");
        rule.Id = "mine";

        var plan = ViewerDataService.PlanUnsilence(new[] { rule }, 7, "host1", TwoSameNamed);

        Assert.Equal(new[] { "mine" }, plan.DeleteRuleIds);
        Assert.Empty(plan.CreateRules);
    }

    [Fact]
    public void PlanUnsilence_NarrowedRule_IsNeverTouched()
    {
        var narrowed = Legacy("host1");
        narrowed.MetricName = "Blocking";

        var plan = ViewerDataService.PlanUnsilence(new[] { narrowed }, 7, "host1", TwoSameNamed);

        Assert.Empty(plan.DeleteRuleIds);
        Assert.Empty(plan.CreateRules);
    }

    [Fact]
    public void PlanUnsilence_NamedServer_NoOthersToKeep()
    {
        var plan = ViewerDataService.PlanUnsilence(new[] { Legacy("other") }, 9, "other", TwoSameNamed);

        Assert.Single(plan.DeleteRuleIds);
        Assert.Empty(plan.CreateRules);
    }
}
