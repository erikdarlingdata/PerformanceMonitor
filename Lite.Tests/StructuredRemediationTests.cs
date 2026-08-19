/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the #2138 machine-first remediation projection: <see cref="FactRemediation.ForcePlanBlockers"/>
/// is THE policy gate a future auto-force feature consults — these tests are the "never auto-force a
/// flagged target" data contract — and <see cref="FactRemediation.BuildStructuredRemediation"/> is how
/// agents see it. The wire-shape pin serializes with the SAME options the MCP surfaces use, because the
/// snake_case field names exist only by attribute (McpHelpers.JsonOptions carries no naming policy) and
/// a renamed record property would silently break every agent consumer.
/// </summary>
public sealed class StructuredRemediationTests
{
    private static ForcePlanTarget Target(
        bool pspCoFired = false, string? replicaRole = null,
        long queryId = 123, long planId = 99) =>
        new(
            Database: "MyDb",
            QueryId: queryId,
            PlanId: planId,
            BestPlanHash: "0xBEST",
            LatestPlanHash: "0xLATEST",
            LatestCpuPerExecUs: 9000,
            BestCpuPerExecUs: 1200,
            RegressionFactor: 7.5,
            ReplicaRole: replicaRole,
            ParameterSensitivityCoFired: pspCoFired);

    private static RemediationAction Action(params ForcePlanTarget[] targets) =>
        new("PLAN_REGRESSION", "force", targets);

    [Fact]
    public void CleanPrimaryTarget_IsEligible_WithNoBlockers()
    {
        /* Null replica role is every standalone/non-AG/pre-2022 server — the 99% case must be
           actionable, or the verdict field is noise. */
        var target = Assert.Single(
            FactRemediation.BuildStructuredRemediation(Action(Target()))!.ForcePlanTargets);

        Assert.True(target.Eligible);
        Assert.Empty(target.Blockers);
        Assert.Equal(7.5, target.Evidence.RegressionFactor);
    }

    [Fact]
    public void PspCoFiredTarget_IsIneligible_AndNamesTheBlocker()
    {
        /* THE contract: a flagged target is never auto-forced. The blocker is a NAMED string, not a
           boolean soup — an agent (and the bot's audit log) says WHY. */
        var target = Assert.Single(
            FactRemediation.BuildStructuredRemediation(Action(Target(pspCoFired: true)))!.ForcePlanTargets);

        Assert.False(target.Eligible);
        Assert.Equal("parameter_sensitivity_cofired", Assert.Single(target.Blockers));
    }

    [Fact]
    public void SecondaryReplicaEvidence_IsIneligible_PrimaryRoleIsNot()
    {
        /* #1882 as data: the statement forces on the PRIMARY, so evidence from a non-primary replica
           blocks; the primary's own evidence does not. Case-insensitive like the disclosure. */
        var secondary = Assert.Single(
            FactRemediation.BuildStructuredRemediation(Action(Target(replicaRole: "Secondary")))!.ForcePlanTargets);
        Assert.False(secondary.Eligible);
        Assert.Equal("secondary_replica_evidence", Assert.Single(secondary.Blockers));

        var primary = Assert.Single(
            FactRemediation.BuildStructuredRemediation(Action(Target(replicaRole: "Primary")))!.ForcePlanTargets);
        Assert.True(primary.Eligible);
        Assert.Empty(primary.Blockers);
    }

    [Fact]
    public void BothGates_StackAsTwoNamedBlockers()
    {
        var target = Assert.Single(FactRemediation.BuildStructuredRemediation(
            Action(Target(pspCoFired: true, replicaRole: "Geo Secondary")))!.ForcePlanTargets);

        Assert.False(target.Eligible);
        Assert.Equal(
            new[] { "parameter_sensitivity_cofired", "secondary_replica_evidence" },
            target.Blockers.OrderBy(b => b, StringComparer.Ordinal));
    }

    [Fact]
    public void Artifacts_AreSplitAndRunnable_AndVerifyChecksTheForceStuck()
    {
        var target = Assert.Single(
            FactRemediation.BuildStructuredRemediation(Action(Target()))!.ForcePlanTargets);

        Assert.Contains("EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 99;", target.ForceSql, StringComparison.Ordinal);
        Assert.Contains("EXEC sys.sp_query_store_unforce_plan @query_id = 123, @plan_id = 99;", target.UnforceSql, StringComparison.Ordinal);

        /* The verify artifact asks BOTH post-force questions: did the force stick, and what has the
           per-interval cost looked like since. */
        Assert.Contains("force_failure_count", target.VerifySql, StringComparison.Ordinal);
        Assert.Contains("last_force_failure_reason_desc", target.VerifySql, StringComparison.Ordinal);
        Assert.Contains("sys.query_store_runtime_stats", target.VerifySql, StringComparison.Ordinal);
        Assert.Contains("WHERE qsp.query_id = 123", target.VerifySql, StringComparison.Ordinal);

        /* Every artifact carries its own USE — an agent pastes them independently. */
        Assert.StartsWith("USE [MyDb];", target.ForceSql, StringComparison.Ordinal);
        Assert.StartsWith("USE [MyDb];", target.UnforceSql, StringComparison.Ordinal);
        Assert.StartsWith("USE [MyDb];", target.VerifySql, StringComparison.Ordinal);
    }

    [Fact]
    public void NullAndNonForceActions_ProjectToNull()
    {
        Assert.Null(FactRemediation.BuildStructuredRemediation(null));
        Assert.Null(FactRemediation.BuildStructuredRemediation(
            new RemediationAction("PLAN_REGRESSION", "force", Array.Empty<ForcePlanTarget>())));
    }

    [Fact]
    public void WireShape_IsSnakeCase_UnderTheMcpSerializerOptions()
    {
        /* The MCP surfaces serialize with McpHelpers.JsonOptions, which has NO naming policy — the
           snake_case names exist only via JsonPropertyName. This pin is what makes a record-property
           rename a test failure instead of a silent agent-facing break. */
        var json = JsonSerializer.Serialize(
            FactRemediation.BuildStructuredRemediation(Action(Target(pspCoFired: true))),
            McpHelpers.JsonOptions);

        foreach (var field in new[]
        {
            "\"fact_key\"", "\"verb\"", "\"force_plan_targets\"",
            "\"query_id\"", "\"plan_id\"", "\"eligible\"", "\"blockers\"", "\"evidence\"",
            "\"regression_factor\"", "\"parameter_sensitivity_cofired\"",
            "\"force_sql\"", "\"unforce_sql\"", "\"verify_sql\"",
        })
        {
            Assert.Contains(field, json, StringComparison.Ordinal);
        }
    }
}
