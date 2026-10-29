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
    public void OnTheRdsWebOrExpressEdition_TheFourCapturesAreAPermanentGap_WithAReasonNamingTheEditionLimit()
    {
        foreach (var name in XeCollectors)
        {
            foreach (var edition in new[] { 2, 3, 4 })
            {
                Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(name, edition), $"{name} {edition}");
                Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", edition, null, name));
            }

            Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition(name, CollectorEngineCapability.AwsRdsWebOrExpressEngineEdition), name);
            var message = CollectorEngineCapability.NotCollectedMessage("srv", CollectorEngineCapability.AwsRdsWebOrExpressEngineEdition, null, name);
            Assert.NotNull(message);
            Assert.Contains("only on the Enterprise and Standard editions", message, System.StringComparison.Ordinal);
            Assert.Contains("Web or Express", message, System.StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheRegistryEdition_IsTheRdsWebOrExpressEditionOnlyForRds()
    {
        Assert.Equal(CollectorEngineCapability.AwsRdsWebOrExpressEngineEdition,
            CollectorEngineCapability.EffectiveEngineEdition(2, new CollectorTargetInfo { IsAwsRds = true, IsAwsRdsWebOrExpress = true }));
        Assert.Equal(2, CollectorEngineCapability.EffectiveEngineEdition(2, new CollectorTargetInfo { IsAwsRds = true }));
        Assert.Equal(2, CollectorEngineCapability.EffectiveEngineEdition(2, new CollectorTargetInfo { IsAwsRdsWebOrExpress = true }));
    }

    [Theory]
    [InlineData(2, "Web Edition (64-bit)", "Web")]
    [InlineData(4, "Express Edition (64-bit)", "Express")]
    [InlineData(2, "Standard Edition (64-bit)", "Standard")]
    [InlineData(2, null, "Standard")]
    public void TheAddServerLine_NamesWebAndExpressFromTheEditionText(int engineEdition, string? editionName, string expected) =>
        Assert.Equal(expected, DarlingServerConnector.DescribeEditionForProbe(engineEdition, editionName));
}
