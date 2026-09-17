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
/// #3350: the tag-aware scope gate <see cref="CustomAlertEvaluator.RuleAppliesToServer"/> — the single "does this
/// rule evaluate this server" decision the sweep, the reconcile teardown, and the never-firing check all route
/// through. All/Servers is the pure name-based <see cref="CustomAlertRuleDefinition.AppliesTo"/>; Tag is
/// membership of the server-id set the evaluator resolves from <c>config.server_tag_map</c>. A tag that resolves
/// to no members (a null or empty set) matches no server, so the rule never fires. The live tag-membership READ
/// is exercised through the evaluate-now / evaluator live tests; this pins the pure gate.
/// </summary>
public class CustomAlertTagScopeTests
{
    private const string Metric = "\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}";

    private static CustomAlertRuleDefinition Parse(string? scope)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}" + (scope is null ? "" : "," + scope) + "}");
        Assert.Null(error);
        Assert.NotNull(def);
        return def!;
    }

    private static CustomAlertRuleDefinition AllScope() => Parse(null);

    private static CustomAlertRuleDefinition ServersScope(string server) =>
        Parse("\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + server + "\"]}");

    private static CustomAlertRuleDefinition TagScope(int tagId) =>
        Parse("\"scope\":{\"mode\":\"tag\",\"tagId\":" + tagId + "}");

    [Fact]
    public void AllScope_AppliesToAnyServer_IgnoringTheTagSet()
    {
        Assert.True(CustomAlertEvaluator.RuleAppliesToServer(
            AllScope(), tagServerIds: null, serverId: 42, storageName: "PROD01"));
    }

    [Fact]
    public void ServersScope_MatchesOnStorageName_CaseInsensitive_IgnoringServerId()
    {
        var def = ServersScope("PROD01");
        Assert.True(CustomAlertEvaluator.RuleAppliesToServer(def, tagServerIds: null, serverId: 999, storageName: "prod01"));
        Assert.False(CustomAlertEvaluator.RuleAppliesToServer(def, tagServerIds: null, serverId: 999, storageName: "PROD02"));
    }

    [Fact]
    public void TagScope_ServerInTheResolvedSet_Applies()
    {
        var members = new HashSet<int> { 101, 202 };
        Assert.True(CustomAlertEvaluator.RuleAppliesToServer(TagScope(5), members, serverId: 101, storageName: "PROD01"));
    }

    [Fact]
    public void TagScope_ServerNotInTheResolvedSet_DoesNotApply()
    {
        // The storage name would satisfy a 'servers' scope, but tag scope is decided by server-id membership only.
        var members = new HashSet<int> { 202 };
        Assert.False(CustomAlertEvaluator.RuleAppliesToServer(TagScope(5), members, serverId: 101, storageName: "PROD01"));
    }

    [Fact]
    public void TagScope_NullResolvedSet_NeverApplies()
    {
        // A tag not resolved this pass matches no server -> the rule never fires (no accidental fleet-wide fire).
        Assert.False(CustomAlertEvaluator.RuleAppliesToServer(TagScope(5), tagServerIds: null, serverId: 101, storageName: "PROD01"));
    }

    [Fact]
    public void TagScope_EmptyResolvedSet_NeverApplies()
    {
        // An empty / deleted / renamed-away tag resolves to no members -> matches nothing.
        Assert.False(CustomAlertEvaluator.RuleAppliesToServer(TagScope(5), new HashSet<int>(), serverId: 101, storageName: "PROD01"));
    }
}
