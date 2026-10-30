/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#6893: Amazon RDS refuses every Extended Events session on its Web and Express editions.</summary>
public sealed class RdsXeEditionGateTests
{
    private static readonly string[] XeCollectors = { "deadlocks", "blocked_process_report", "attention", "long_query_completions" };

    [Theory]
    [InlineData("Web Edition (64-bit)", true)]
    [InlineData("Express Edition (64-bit)", true)]
    [InlineData("Express Edition with Advanced Services (64-bit)", true)]
    [InlineData("Standard Edition (64-bit)", false)]
    [InlineData("Enterprise Edition: Core-based Licensing (64-bit)", false)]
    [InlineData(null, false)]
    public void TheEditionNameDecidesWebOrExpress(string? edition, bool expected) =>
        Assert.Equal(expected, CollectorEngineCapability.IsWebOrExpressEditionName(edition));

    [Fact]
    public void TheFourXeCollectors_DoNotApplyOnRdsWebOrExpress_AndDoApplyEverywhereElse()
    {
        foreach (var name in XeCollectors)
        {
            var definition = CollectorCatalog.Find(name)!;
            Assert.False(CollectorCatalog.AppliesTo(definition, new CollectorTargetInfo { IsAwsRds = true, IsAwsRdsWebOrExpress = true }), name);
            Assert.True(CollectorCatalog.AppliesTo(definition, new CollectorTargetInfo { IsAwsRds = true }), name);
            Assert.True(CollectorCatalog.AppliesTo(definition, new CollectorTargetInfo { IsAwsRdsWebOrExpress = true }), name);
        }
    }

    [Fact]
    public void OnAnRdsWebOrExpressTarget_TheFourCapturesAreAPermanentGap_WithAReasonNamingTheEditionLimit()
    {
        foreach (var name in XeCollectors)
        {
            /* The registry keeps the real EngineEdition (2 for Web and Standard, 4 for Express): the gap comes from the stored RDS fact. */
            foreach (var edition in new[] { 2, 3, 4 })
            {
                Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(name, edition), $"{name} {edition}");
                Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", edition, null, name, AwsRdsKind.None));
                Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", edition, null, name, AwsRdsKind.Rds));
            }

            Assert.False(CollectorEngineCapability.IsCollectedOnAwsRds(name, null, AwsRdsKind.RdsWebOrExpress), name);
            Assert.True(CollectorEngineCapability.IsCollectedOnAwsRds(name, null, AwsRdsKind.Rds), name);
            var message = CollectorEngineCapability.NotCollectedMessage("srv", 2, null, name, AwsRdsKind.RdsWebOrExpress);
            Assert.NotNull(message);
            Assert.Contains("only on the Enterprise and Standard editions", message, System.StringComparison.Ordinal);
            Assert.Contains("Amazon RDS for SQL Server Web or Express edition", message, System.StringComparison.Ordinal);
            Assert.DoesNotContain("1002", message, System.StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AnRdsWebOrExpressTarget_IsGapForARdsOnlyGate_AndPlainRdsMakesNoXeClaim()
    {
        /* running_jobs is closed on every RDS target, so the Web or Express kind still answers it, in the RDS words. */
        var jobs = CollectorEngineCapability.NotCollectedMessage("srv", 2, null, "running_jobs", AwsRdsKind.RdsWebOrExpress);
        Assert.NotNull(jobs);
        Assert.Contains("Amazon RDS for SQL Server Web or Express edition", jobs, System.StringComparison.Ordinal);
        Assert.DoesNotContain("only on the Enterprise and Standard editions", jobs, System.StringComparison.Ordinal);

        Assert.Equal(AwsRdsKind.None, CollectorEngineCapability.AwsRdsKindOf(false, true));
        Assert.Equal(AwsRdsKind.Rds, CollectorEngineCapability.AwsRdsKindOf(true, false));
        Assert.Equal(AwsRdsKind.RdsWebOrExpress, CollectorEngineCapability.AwsRdsKindOf(true, true));
    }

    [Theory]
    [InlineData(2, "Web Edition (64-bit)", "Web")]
    [InlineData(4, "Express Edition (64-bit)", "Express")]
    [InlineData(2, "Standard Edition (64-bit)", "Standard")]
    [InlineData(2, null, "Standard")]
    public void TheAddServerLine_NamesWebAndExpressFromTheEditionText(int engineEdition, string? editionName, string expected) =>
        Assert.Equal(expected, DarlingServerConnector.DescribeEditionForProbe(engineEdition, editionName));
}
