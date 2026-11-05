/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Viewer;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6932: a collector the monitored server refuses (NO_PERMISSIONS, EXTENSION_MISSING) used to add to the total and to no count, so a
/// card read "43 healthy, 0 failing, 2 not collected" of 46. Every banded row now lands in one bucket and the buckets add up to the total.
/// </summary>
public sealed class CollectorTallyBandsTests
{
    /// <summary>Every band the classifier names, read from its public string constants (a band is an upper-case word such as
    /// NO_PERMISSIONS; the class's other constants are prose or lower-case markers), so a band added later joins this list without
    /// anyone remembering to edit it. #6828 and #6932 each dropped a band from a count because this list was written by hand.</summary>
    private static readonly string[] AllBands = typeof(CollectorHealthClassifier)
        .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.FlattenHierarchy)
        .Where(f => f.IsLiteral && f.FieldType == typeof(string))
        .Select(f => (string)f.GetRawConstantValue()!)
        .Where(v => v.Length > 0 && v.All(c => char.IsAsciiLetterUpper(c) || c == '_'))
        .ToArray();

    [Fact]
    public void OneRowInEveryBand_FillsTheBuckets_AndTheyAddUpToTheTotal()
    {
        var tally = new CollectorHealthClassifier.BandTally();
        foreach (var band in AllBands)
        {
            tally = tally.Add("wait_stats", band, alwaysOnOff: false, engineGated: false);
        }

        Assert.Equal(AllBands.Length, tally.Total);
        Assert.Equal(tally.Total, tally.Healthy + tally.Warning + tally.Failing + tally.NotCollected + tally.NoPermission);

        /* HEALTHY; WARNING and STALE; FAILING alone (the card's severity grades this one); NO_PERMISSIONS and EXTENSION_MISSING;
           NEVER_RUN and STOPPED. */
        Assert.Equal(new CollectorHealthClassifier.BandTally(1, 2, 1, 2, 2, 8), tally);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void AGatedOrAlwaysOnOffRow_StillCountsInExactlyOneBucket(bool engineGated, bool alwaysOnOff)
    {
        var band = CollectorHealthClassifier.Healthy;
        var name = alwaysOnOff ? "ag_replica_states" : "wait_stats";
        var tally = new CollectorHealthClassifier.BandTally().Add(name, band, alwaysOnOff, engineGated);

        Assert.Equal(new CollectorHealthClassifier.BandTally(0, 0, 0, 1, 0, 1), tally);
    }

    [Fact]
    public void OneRefusedCollector_IsCountedOnTheFleetCard_WithoutMovingTheCardsSeverity()
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        var refused = new CollectorHealth
        {
            CollectorName = "agent_alerts",
            TotalRuns = 5,
            SuccessCount = 0,
            ErrorCount = 0,
            PermissionDeniedCount = 5,
            LastRunTime = last,
            LastError = "The SELECT permission was denied on the object 'sysnotifications'",
        };
        Assert.Equal(CollectorHealthClassifier.NoPermissions, refused.HealthStatus);

        var counts = default(DarlingFleetReader.CollectorCounts);
        for (var i = 0; i < 43; i++)
        {
            var healthy = new CollectorHealth { CollectorName = "collector_" + i, TotalRuns = 10, SuccessCount = 10, LastSuccessTime = last, LastRunTime = last };
            counts = DarlingFleetReader.AddCollector(counts, healthy, alwaysOnOff: false, engineGated: false);
        }

        counts = DarlingFleetReader.AddCollector(counts, refused, alwaysOnOff: false, engineGated: false);
        Assert.Equal(1, counts.NoPermission);
        Assert.Equal(44, counts.Total);
        Assert.Equal(counts.Total, counts.Healthy + counts.Warning + counts.Failing + counts.NotCollected + counts.NoPermission);

        var now = DateTime.UtcNow;
        var card = DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "sql-01", "sql-01", 3, MonitoredEngineKind.SqlServer, false),
            new DarlingFleetReader.CpuRow(20, 5),
            default,
            default,
            default,
            new DarlingFleetReader.ThreadsRow(512, 100, 0, 0),
            default,
            default,
            default,
            last,
            counts,
            null,
            now,
            TimeSpan.FromHours(1),
            DeadlockRateThresholds.Default,
            now.AddDays(-30));

        Assert.Equal(1, card.NoPermissionCollectorCount);
        Assert.Equal(43, card.HealthyCollectorCount);
        Assert.Equal(44, card.CollectorCount);
        Assert.Equal(HealthSeverity.Healthy, card.CollectorSeverity);

        using var json = JsonDocument.Parse(JsonSerializer.Serialize(card));
        Assert.Equal(1, json.RootElement.GetProperty("no_permission_collector_count").GetInt32());
        Assert.Equal(43, json.RootElement.GetProperty("healthy_collector_count").GetInt32());
    }

    [Fact]
    public void TheAgentAlertsRefusal_NamesBothGrants_AndAgreesInNumber()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "sql-01", "agent_alerts", CollectorRuntimePrecondition.DegradedStatus,
            "The SELECT permission was denied on the object 'sysnotifications'", DateTime.UtcNow.AddMinutes(-3));

        Assert.NotNull(message);
        Assert.Contains("grant SELECT on msdb.dbo.sysalerts and msdb.dbo.sysnotifications to the monitoring login", message, StringComparison.Ordinal);
        Assert.Contains("nothing from the SQL Agent alert definitions and counters is being stored", message, StringComparison.Ordinal);
        Assert.DoesNotContain("counters is not being stored", message, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryGrantTheRefusalNames_IsOneTheMonitoringGrantScriptGives()
    {
        foreach (var (collector, grants) in CollectorRuntimePrecondition.MsdbGrantsByCollector)
        {
            foreach (var (permission, obj) in grants)
            {
                Assert.True(
                    MonitoringGrantScript.All.Any(e => e.Scope == GrantScope.Msdb && e.Permission == permission && e.OnObject == obj),
                    $"{collector}: the message names {permission} on msdb.{obj}, which the grant script does not give");
            }
        }
    }

    [Fact]
    public void ACollectorNeverRun_InsideItsFirstIntervalAfterTheServiceStarted_SaysItIsNotDueYet_NotSwitchedOff()
    {
        var now = DateTime.UtcNow;
        var serverFirst = now.AddDays(-30);
        var serverLast = now.AddSeconds(-20);
        var serviceStarted = now.AddMinutes(-2);

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            "sql-01", "agent_alerts", CollectorRuntimePrecondition.AgentAlertsPossibleCauses, null, serverLast, serverFirst, serviceStarted);

        Assert.NotNull(message);
        Assert.Contains("has not had its first run", message, StringComparison.Ordinal);
        Assert.Contains("every 15 minutes", message, StringComparison.Ordinal);
        Assert.Contains("is due at about " + serviceStarted.AddMinutes(15).ToString("u", System.Globalization.CultureInfo.InvariantCulture), message, StringComparison.Ordinal);
        Assert.DoesNotContain("switched off", message, StringComparison.Ordinal);
        Assert.DoesNotContain("AWS RDS", message, StringComparison.Ordinal);
        Assert.Equal(
            CollectorRuntimePrecondition.NotYetDueStatusWord,
            CollectorRuntimePrecondition.GatedOffStatusWord("agent_alerts", null, serverLast, serverFirst, serviceStarted));
    }

    [Fact]
    public void ACollectorNeverRun_ALongTimeAfterTheServiceStarted_StillSaysItNeverRan()
    {
        var now = DateTime.UtcNow;

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            "sql-01", "agent_alerts", CollectorRuntimePrecondition.AgentAlertsPossibleCauses, null, now.AddSeconds(-20), now.AddDays(-30), now.AddHours(-3));

        Assert.NotNull(message);
        Assert.Contains("has never run", message, StringComparison.Ordinal);
    }

    private static Task<(string Text, System.Windows.Visibility Visibility)> ViewerNote(
        string collector, DateTime? serviceStarted, DateTime serverFirst, DateTime serverLast, Func<Task<DateTime?>>? serviceStartReader = null) =>
        ViewerServerTab.ReadEngineGapStateAsync(
            (_, _) => Task.FromResult<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)>((null, serverLast, serverFirst)),
            null, 7, "sql-01", 3, MonitoredEngineKind.SqlServer, collector, 0,
            (_, _) => Task.FromResult<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)>((null, null, null)),
            AwsRdsKind.None,
            () => serviceStartReader is null ? Task.FromResult(serviceStarted) : serviceStartReader());

    [Theory]
    [InlineData("agent_alerts")]
    [InlineData("running_jobs")]
    [InlineData("query_store_health")]
    public async Task TheViewerNote_ForACollectorNeverRun_InsideItsFirstIntervalAfterTheServiceStarted_SaysItIsNotDueYet(string collector)
    {
        var now = DateTime.UtcNow;
        var serviceStarted = now.AddMinutes(-2);

        var (text, visibility) = await ViewerNote(collector, serviceStarted, now.AddDays(-30), now.AddSeconds(-20));

        Assert.Equal(System.Windows.Visibility.Visible, visibility);
        Assert.Contains("has not had its first run", text, StringComparison.Ordinal);
        Assert.Contains("is due at about", text, StringComparison.Ordinal);
        Assert.DoesNotContain("switched off", text, StringComparison.Ordinal);
        Assert.DoesNotContain("AWS RDS", text, StringComparison.Ordinal);
        Assert.DoesNotContain("never run", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("agent_alerts")]
    [InlineData("running_jobs")]
    public async Task TheViewerNote_ForACollectorNeverRun_ALongTimeAfterTheServiceStarted_StillSaysItNeverRan(string collector)
    {
        var now = DateTime.UtcNow;

        var (text, _) = await ViewerNote(collector, now.AddHours(-3), now.AddDays(-30), now.AddSeconds(-20));

        Assert.DoesNotContain("has not had its first run", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheViewerNote_WhenTheServiceStartCannotBeRead_FallsBackToTheServersOwnTimes()
    {
        var now = DateTime.UtcNow;

        var (text, visibility) = await ViewerNote(
            "agent_alerts", null, now.AddDays(-30), now.AddSeconds(-20), () => throw new InvalidOperationException("config.service_version unreadable"));

        Assert.Equal(System.Windows.Visibility.Visible, visibility);
        Assert.DoesNotContain("has not had its first run", text, StringComparison.Ordinal);
    }
}
