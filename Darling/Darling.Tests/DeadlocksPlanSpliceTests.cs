/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2681: the deadlocks collector splices victim-plan capture into its query with plain string.Replace on
/// paired markers. AzureQueryText has TWO SELECT blocks (UNION ALL): the ring-buffer block (both markers)
/// and the telemetry-blob block (#2641), which projects the plan column but has NO plan-APPLY marker and
/// cannot resolve a plan (it is master-scoped, reading user databases' deadlocks whose plans live in a
/// cache a master connection cannot read). Sharing the plan-SELECT marker spliced <c>vqp.query_plan</c>
/// into that block with no <c>vqp</c> alias, and every Azure SQL DB target's deadlocks collection failed at
/// parse with 4104 "The multi-part identifier 'vqp.query_plan' could not be bound" (reproduced live). The
/// fix gives the telemetry block its own NULL-plan marker so the ordinal still matches under the shared
/// reader but no undefined alias is referenced.
/// </summary>
public class DeadlocksPlanSpliceTests
{
    private static string Sql(bool azure, bool capturePlanXml) =>
        DeadlocksCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
            CapturePlanXml = capturePlanXml,
        }).Text;

    private static int CountOf(string haystack, string needle)
    {
        int n = 0, i = 0;
        while ((i = haystack.IndexOf(needle, i, StringComparison.Ordinal)) >= 0) { n++; i += needle.Length; }
        return n;
    }

    /// <summary>
    /// The regression guard: with plans on, the two-block Azure query references <c>vqp.query_plan</c>
    /// EXACTLY once (the ring-buffer block). Before the fix it was twice — the second orphaned — which is
    /// the exact 4104 bind failure. The telemetry block instead projects the plan column as NULL, at the
    /// same ordinal, so the UNION ALL still lines up for the one shared reader.
    /// </summary>
    [Fact]
    public void Azure_WithPlans_ReferencesVqpExactlyOnce_AndNullsTheTelemetryBlock()
    {
        var sql = Sql(azure: true, capturePlanXml: true);

        Assert.Equal(1, CountOf(sql, "vqp.query_plan"));
        Assert.Contains("victim_query_plan_xml = vqp.query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("victim_query_plan_xml = CONVERT(nvarchar(max), NULL)", sql, StringComparison.Ordinal);
        // Both UNION blocks project the plan column (same ordinal); no marker survives the splice.
        Assert.Equal(2, CountOf(sql, "victim_query_plan_xml ="));
        Assert.DoesNotContain("/*DL_PLAN", sql, StringComparison.Ordinal);
    }

    /// <summary>Plans off: both fragments erase, so neither block has a plan column or a vqp reference.</summary>
    [Fact]
    public void Azure_WithoutPlans_HasNoPlanColumnOrVqp()
    {
        var sql = Sql(azure: true, capturePlanXml: false);

        Assert.DoesNotContain("victim_query_plan_xml", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("vqp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("/*DL_PLAN", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The on-prem / MI / RDS query has one SELECT block with both markers correctly paired, so it always
    /// referenced vqp exactly once and was never affected — pinned so a future edit keeps it that way.
    /// </summary>
    [Fact]
    public void ServerScoped_WithPlans_ReferencesVqpExactlyOnce()
    {
        var sql = Sql(azure: false, capturePlanXml: true);

        Assert.Equal(1, CountOf(sql, "vqp.query_plan"));
        Assert.Contains("victim_query_plan_xml = vqp.query_plan", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("/*DL_PLAN", sql, StringComparison.Ordinal);
    }
}
