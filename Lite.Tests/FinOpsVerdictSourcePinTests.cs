/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2246, Lite's half. The provisioning verdict used to be duplicated SIX times — Darling's point-in-time,
/// trend and inventory reads, and Lite's three — each testing
/// <c>total_server_memory_mb / target_server_memory_mb &gt; 0.95</c>, a ratio measured at median 1.0000 across
/// 42 production servers. Every server came out UNDER_PROVISIONED and OVER_PROVISIONED was unreachable.
///
/// <para><b>Why source pins rather than behavioural ones.</b> Darling exposes its reads as
/// <c>public const string</c>, so its tests assert against the shipped SQL directly. Lite builds the same
/// queries as inline <c>command.CommandText</c>, so there is nothing to reference — the reads are only
/// reachable through a live DuckDB store. These pins therefore read the source, the same technique
/// <see cref="ParitySource"/> exists for, and cover the two invariants a broken edit would trip: the pressure
/// inputs must be SELECTed, and the verdict must not be decided in SQL.</para>
///
/// <para>Without them the identical mistake fails fast on Darling and silently on Lite, which is the drift
/// this whole issue is about — the shared predicate removed the duplicated LOGIC, but the two apps still
/// carry their own copies of the SQL that feeds it.</para>
/// </summary>
public sealed class FinOpsVerdictSourcePinTests
{
    private const string Utilization = "Lite/Services/LocalDataService.FinOps.Utilization.cs";
    private const string Inventory = "Lite/Services/LocalDataService.FinOps.ServerProperties.cs";

    /// <summary>Both Lite reads must fetch the workspace-memory pressure signals the shared predicate
    /// consumes. A reader wired to ordinals the SELECT list no longer produces is the failure this catches
    /// earliest.</summary>
    [Theory]
    [InlineData(Utilization)]
    [InlineData(Inventory)]
    public void BothLiteReads_FetchThePressureInputs(string path)
    {
        var source = ParitySource.ReadFile(path);

        Assert.Contains("FROM v_memory_grant_stats", source, StringComparison.Ordinal);
        Assert.Contains("waiter_count", source, StringComparison.Ordinal);
        Assert.Contains("timeout_error_count_delta", source, StringComparison.Ordinal);
        Assert.Contains("forced_grant_count_delta", source, StringComparison.Ordinal);
        Assert.Contains("granted_memory_mb", source, StringComparison.Ordinal);
        Assert.Contains("max_workers_count", source, StringComparison.Ordinal);
    }

    /// <summary>Both must classify through the shared predicate rather than deciding for themselves. The
    /// inventory read in particular used to carry an inline SQL <c>CASE</c>, and it feeds the Server
    /// Inventory grid — the screen the field report was looking at.</summary>
    [Theory]
    [InlineData(Utilization)]
    [InlineData(Inventory)]
    public void BothLiteReads_ClassifyThroughTheSharedPredicate(string path)
    {
        var source = ParitySource.ReadFile(path);

        Assert.Contains("ProvisioningVerdict.Evaluate(", source, StringComparison.Ordinal);
    }

    /// <summary>The verdict must not be decided in SQL. These literals are how the inventory read used to do
    /// it, so their absence is the guard against a SQL-side verdict coming back — which would silently
    /// disagree with the drill-down for the same server.</summary>
    [Theory]
    [InlineData(Utilization)]
    [InlineData(Inventory)]
    public void NoLiteRead_DecidesTheVerdictInSql(string path)
    {
        var source = ParitySource.ReadFile(path);

        Assert.DoesNotContain("'OVER_PROVISIONED'", source, StringComparison.Ordinal);
        Assert.DoesNotContain("'UNDER_PROVISIONED'", source, StringComparison.Ordinal);
        Assert.DoesNotContain("'RIGHT_SIZED'", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The retired threshold itself, gone from every Lite FinOps read.
    ///
    /// <para>0.95 and 0.5 were the two ratio comparisons that produced the bug. Pinning the NUMBER rather
    /// than the expression is deliberate: the defect survived being copied six times precisely because each
    /// copy was spelled slightly differently, and a literal is what a copy carries unchanged.</para>
    /// </summary>
    [Theory]
    [InlineData(Utilization)]
    [InlineData(Inventory)]
    public void TheRetiredMemoryRatioThresholdIsGone(string path)
    {
        var source = ParitySource.ReadFile(path);

        Assert.DoesNotContain("> 0.95", source, StringComparison.Ordinal);
        Assert.DoesNotContain("0.95m", source, StringComparison.Ordinal);
        Assert.DoesNotContain("< 0.5 ", source, StringComparison.Ordinal);
        Assert.DoesNotContain("0.5m)", source, StringComparison.Ordinal);
    }
}
