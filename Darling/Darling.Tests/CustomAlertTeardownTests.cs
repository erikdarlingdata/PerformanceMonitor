/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3305: the pure teardown-decision the state reconcile runs per persisted (rule, server) row. A DISABLED
/// rule's state is orphaned (the evaluator's enabled-only sweep skips it, so its open incident never resolves
/// on its own); an ENABLED rule's state for a server that has left its scope — or left monitoring entirely — is
/// stale. Both are torn down (force-resolved if firing, then deleted). The DB orchestration is covered live in
/// <c>CustomAlertTeardownLiveTests</c>; this pins the branch logic.
/// </summary>
public class CustomAlertTeardownTests
{
    private static CustomAlertRuleDefinition AllScope() => Parse(
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}");

    private static CustomAlertRuleDefinition ServersScope(string server) => Parse(
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000},\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + server + "\"]}}");

    private static CustomAlertRuleDefinition TagScope() => Parse(
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000},\"scope\":{\"mode\":\"tag\",\"tagId\":5}}");

    private static CustomAlertRuleDefinition Parse(string json)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(json);
        Assert.Null(error);
        Assert.NotNull(def);
        return def!;
    }

    [Fact]
    public void DisabledRule_IsAlwaysTornDown_EvenWithNoDefinition()
    {
        // A disabled rule is not in the enabled cache, so the reconcile passes a null definition; disabled wins.
        Assert.Equal(
            CustomAlertEvaluator.TeardownReasonDisabled,
            CustomAlertEvaluator.ClassifyStateTeardown(ruleEnabled: false, definition: null, tagServerIds: null, serverId: 1, serverStorageName: "PROD01"));
    }

    [Fact]
    public void EnabledRule_WithNoCachedDefinition_IsLeftAlone()
    {
        // Just-enabled or a stale cache: don't tear down on uncertainty.
        Assert.Null(CustomAlertEvaluator.ClassifyStateTeardown(ruleEnabled: true, definition: null, tagServerIds: null, serverId: 1, serverStorageName: "PROD01"));
    }

    [Fact]
    public void EnabledAllScopeRule_WithAMonitoredServer_IsKept()
    {
        Assert.Null(CustomAlertEvaluator.ClassifyStateTeardown(ruleEnabled: true, AllScope(), tagServerIds: null, serverId: 1, serverStorageName: "PROD01"));
    }

    [Fact]
    public void EnabledServersScopeRule_ServerInScope_IsKept()
    {
        Assert.Null(CustomAlertEvaluator.ClassifyStateTeardown(ruleEnabled: true, ServersScope("PROD01"), tagServerIds: null, serverId: 1, serverStorageName: "PROD01"));
    }

    [Fact]
    public void EnabledServersScopeRule_ServerOutOfScope_IsTornDown()
    {
        Assert.Equal(
            CustomAlertEvaluator.TeardownReasonOutOfScope,
            CustomAlertEvaluator.ClassifyStateTeardown(ruleEnabled: true, ServersScope("PROD01"), tagServerIds: null, serverId: 1, serverStorageName: "PROD02"));
    }

    [Fact]
    public void EnabledRule_ServerLeftMonitoring_IsTornDown_EvenForAllScope()
    {
        // A null storage name means the server_id is not in the monitored map any more — it was removed. Its
        // firing state can never resolve on its own (the evaluator never sweeps a gone server), so tear it down
        // even though an "all"-scoped rule would otherwise apply to it.
        Assert.Equal(
            CustomAlertEvaluator.TeardownReasonOutOfScope,
            CustomAlertEvaluator.ClassifyStateTeardown(ruleEnabled: true, AllScope(), tagServerIds: null, serverId: 1, serverStorageName: null));
    }

    [Fact]
    public void EnabledTagScopeRule_ServerInTag_IsKept()
    {
        // The server's id is in the tag's resolved member set -> in scope, keep the row.
        var members = new HashSet<int> { 101 };
        Assert.Null(CustomAlertEvaluator.ClassifyStateTeardown(
            ruleEnabled: true, TagScope(), members, serverId: 101, serverStorageName: "PROD01"));
    }

    [Fact]
    public void EnabledTagScopeRule_ServerLeftTag_IsTornDown()
    {
        // The server is still monitored, but its id is no longer in the tag's resolved set (it left the tag) —
        // out of scope, so its open incident is force-resolved (#3350), the same idiom as leaving a 'servers' scope.
        var members = new HashSet<int> { 202 };
        Assert.Equal(
            CustomAlertEvaluator.TeardownReasonOutOfScope,
            CustomAlertEvaluator.ClassifyStateTeardown(
                ruleEnabled: true, TagScope(), members, serverId: 101, serverStorageName: "PROD01"));
    }

    [Fact]
    public void EnabledTagScopeRule_EmptyTag_IsTornDown()
    {
        // A tag that resolves to no members (empty / deleted) matches no server, so any lingering state is stale.
        Assert.Equal(
            CustomAlertEvaluator.TeardownReasonOutOfScope,
            CustomAlertEvaluator.ClassifyStateTeardown(
                ruleEnabled: true, TagScope(), new HashSet<int>(), serverId: 101, serverStorageName: "PROD01"));
    }
}
