/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.AlertEngineTests;

namespace Darling.Tests;

/// <summary>
/// #5916, no store: the keys an override may take, the bounds, the loader's resolution (a server's override moves that
/// server and leaves another alone; a database override applies to that database only), the engine moved by the loader,
/// and the two PostgreSQL count thresholds following it. The store paths are in <see cref="ThresholdOverrideLiveTests"/>.
/// </summary>
public sealed class ThresholdOverrideTests
{
    [Theory]
    [InlineData("no.such.key", "not_in_registry")]
    [InlineData("cpu.threshold", "not_in_registry")]
    [InlineData("self_alerts.collection_stale_minutes", "not_tunable")]
    [InlineData("self_alerts.disk_free_warn_percent", "not_tunable")]
    [InlineData("cpu.enabled", "not_tunable")]
    [InlineData("enabled", "not_tunable")]
    [InlineData("cooldown_minutes", "not_tunable")]
    [InlineData("delivery.cooldown_minutes", "not_tunable")]
    [InlineData("route.metric_match", "not_tunable")]
    [InlineData("", "key_required")]
    public void AKeyThatIsNotATunableThreshold_IsRefused_WithItsClass(string key, string reason)
    {
        var refusal = DarlingThresholdKeys.RefusalFor(key);
        Assert.NotNull(refusal);
        Assert.Equal(reason, refusal!.Value.Reason);
    }

    [Fact]
    public void EveryRegistryKey_AndBothPostgreSqlCountThresholds_AreTunable_AndNothingElseIs()
    {
        foreach (var key in Enum.GetValues<AlertThresholdKey>())
        {
            var entry = DarlingThresholdKeys.For(key);
            Assert.Null(DarlingThresholdKeys.RefusalFor(entry.Wire));
            Assert.Equal(AlertThresholdKeys.ScopeOf(key), entry.Scope);
        }

        Assert.Null(DarlingThresholdKeys.RefusalFor(DarlingThresholdKeys.PgBlockingCount));
        Assert.Null(DarlingThresholdKeys.RefusalFor(DarlingThresholdKeys.PgDeadlockCount));
        /* #6875: the Identity Capacity alert's two bars are Darling's own keys, tunable per server and per database like the engine ones. */
        Assert.Null(DarlingThresholdKeys.RefusalFor(DarlingThresholdKeys.IdentityCapacityWarning));
        Assert.Null(DarlingThresholdKeys.RefusalFor(DarlingThresholdKeys.IdentityCapacityCritical));
        Assert.Equal(Enum.GetValues<AlertThresholdKey>().Length + 4, DarlingThresholdKeys.Entries.Count);
    }

    [Theory]
    [InlineData("cpu.threshold_percent", 0)]
    [InlineData("cpu.threshold_percent", 101)]
    [InlineData("long_running_job.multiplier", 1)]
    [InlineData("long_running_job.multiplier", 21)]
    [InlineData("file_growth.lookback_minutes", 4)]
    [InlineData("blocking.pg_count_threshold", 0)]
    [InlineData("deadlocks.pg_count_threshold", -3)]
    public void AValueOutsideTheGlobalSettingsBounds_IsRefused(string key, int value)
    {
        Assert.True(DarlingThresholdKeys.TryGet(key, out var entry));
        Assert.Equal("out_of_bounds", DarlingThresholdKeys.ValidateValue(entry, value, null)!.Value.Reason);
    }

    [Fact]
    public void ADatabaseNameIsAcceptedOnlyOnADatabaseScopedKey()
    {
        Assert.True(DarlingThresholdKeys.TryGet("pvs.threshold_percent", out var pvs));
        Assert.Null(DarlingThresholdKeys.ValidateValue(pvs, 60, "shop"));
        Assert.True(DarlingThresholdKeys.TryGet("cpu.threshold_percent", out var cpu));
        Assert.Equal("database_scope", DarlingThresholdKeys.ValidateValue(cpu, 60, "shop")!.Value.Reason);
        Assert.True(DarlingThresholdKeys.TryGet(DarlingThresholdKeys.PgBlockingCount, out var pg));
        Assert.Equal("database_scope", DarlingThresholdKeys.ValidateValue(pg, 5, "shop")!.Value.Reason);
    }

    [Fact]
    public void AnOverride_MovesItsServer_AndLeavesTheOther_AndADatabaseOverrideAppliesToThatDatabaseOnly()
    {
        var overrides = new DarlingThresholdOverrides();
        overrides.Replace(new (int, string?, string, double)[]
        {
            (101, null, "cpu.threshold_percent", 95),
            (101, "shop", "pvs.threshold_percent", 70),
            (101, null, "file_growth.rise_mb", 500),
        });

        Assert.Equal(95, overrides.Resolve(AlertThresholdKey.CpuThresholdPercent, "101", null, 80));
        Assert.Equal(80, overrides.Resolve(AlertThresholdKey.CpuThresholdPercent, "202", null, 80));

        /* Database scope: the database that holds the row, and no other database, and not the server's other keys. */
        Assert.Equal(70, overrides.Resolve(AlertThresholdKey.PvsThresholdPercent, "101", "shop", 40));
        Assert.Equal(40, overrides.Resolve(AlertThresholdKey.PvsThresholdPercent, "101", "ledger", 40));
        Assert.Equal(40, overrides.Resolve(AlertThresholdKey.PvsThresholdPercent, "101", null, 40));
        Assert.Equal(40, overrides.Resolve(AlertThresholdKey.PvsThresholdPercent, "202", "shop", 40));

        /* A server-level override on a database-scoped key covers its databases that hold no row of their own. */
        Assert.Equal(500, overrides.Resolve(AlertThresholdKey.FileGrowthRiseMb, "101", "shop", 1024));
        Assert.Equal(500, overrides.Resolve(AlertThresholdKey.FileGrowthRiseMb, "101", null, 1024));

        /* A server key that is not a store id reads the global value. */
        Assert.Equal(80, overrides.Resolve(AlertThresholdKey.CpuThresholdPercent, "not-an-id", null, 80));
    }

    [Fact]
    public void ARowTheGlobalSettingWouldRefuse_IsNotApplied()
    {
        var overrides = new DarlingThresholdOverrides();
        overrides.Replace(new (int, string?, string, double)[]
        {
            (101, null, "cpu.threshold_percent", 500),
            (101, null, "cpu.threshold_percent_typo", 90),
            (101, null, "self_alerts.collection_stale_minutes", 10),
            (101, null, "blocking.count_threshold", 2.5),
            (101, "shop", "cpu.threshold_percent", 90),
        });

        Assert.Equal(0, overrides.Count);
        Assert.Equal(80, overrides.Resolve(AlertThresholdKey.CpuThresholdPercent, "101", null, 80));
    }

    /// <summary>The loader behind the engine: a blocking override on server 101 lifts that server's bar, server 202 keeps the global one.</summary>
    [Fact]
    public async Task TheLoader_MovesOneServersAlert_ThroughTheEngine_AndLeavesTheOther()
    {
        var overrides = new DarlingThresholdOverrides();
        overrides.Replace(new (int, string?, string, double)[] { (101, null, "blocking.count_threshold", 5) });

        var h = new Harness { Overrides = overrides };
        h.Settings.BlockingEnabled = true;
        h.Adapter.Blocking.Add(BlockingRow(55));
        h.Adapter.Blocking.Add(BlockingRow(56));
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot() with { ServerKey = "101", ServerName = "SRV-A" });
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected");

        await engine.EvaluateServerAsync(Harness.Snapshot() with { ServerKey = "202", ServerName = "SRV-B" });
        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected");
        Assert.Equal("202", fired.ServerKey);
    }

    /// <summary>
    /// The two PostgreSQL count checks read their bar through <see cref="DarlingThresholdOverrides.ResolveWire"/> and hand it
    /// to <see cref="RollingCountAlertGate"/>: the same count fires on the server whose bar was lowered and not on another.
    /// </summary>
    [Theory]
    [InlineData(DarlingThresholdKeys.PgDeadlockCount)]
    [InlineData(DarlingThresholdKeys.PgBlockingCount)]
    public void EachPostgreSqlCountThreshold_FollowsItsServersOverride(string wire)
    {
        var overrides = new DarlingThresholdOverrides();
        overrides.Replace(new (int, string?, string, double)[] { (7, null, wire, 3) });

        const int globalBar = 10;
        const int count = 5;
        bool Fires(string serverKey) => RollingCountAlertGate.Evaluate(
            count, overrides.ResolveWire(wire, serverKey, null, globalBar), watermark: 0, cooldownElapsed: true, suppressed: false).Fire;

        Assert.True(Fires("7"));
        Assert.False(Fires("8"));
    }

    [Fact]
    public void TheToolCannotLowerAnAgentsBarToNothing_AndLeavesOrdinaryValuesAlone()
    {
        Assert.True(DarlingThresholdKeys.TryGet("cpu.threshold_percent", out var cpu));
        Assert.Equal("threshold_disables_alert", DarlingMcpThresholdOverrideTools.AgentKindRule(cpu, 100)!.Reason);
        Assert.Null(DarlingMcpThresholdOverrideTools.AgentKindRule(cpu, 95));
        Assert.True(DarlingThresholdKeys.TryGet("blocking.count_threshold", out var blocking));
        Assert.Equal("threshold_disables_alert", DarlingMcpThresholdOverrideTools.AgentKindRule(blocking, int.MaxValue)!.Reason);
        Assert.Contains("set_alert_threshold_override", DarlingMcpWritePolicy.AgentActionTools);
        Assert.DoesNotContain("update_alert_settings", DarlingMcpWritePolicy.AgentActionTools);
        Assert.True(DarlingMcpWritePolicy.Effective.Off.Offers("set_alert_threshold_override") is false);
        Assert.True(new DarlingMcpWritePolicy.Effective(true, false).Offers("set_alert_threshold_override"));
        Assert.True(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true).Offers("set_alert_threshold_override"));
    }
}
