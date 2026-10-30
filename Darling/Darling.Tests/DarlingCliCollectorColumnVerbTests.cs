/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>--set-collector-frequency</c> and <c>--set-collector-retention</c> (#6227): the verbs parse strictly, run the command plane's own
/// plan (so the bounds are the executor's) and list themselves in the help text.
/// </summary>
public sealed class DarlingCliCollectorColumnVerbTests
{
    [Theory]
    [InlineData("--set-collector-frequency", CollectorScheduleColumn.FrequencyMinutes)]
    [InlineData("--set-collector-retention", CollectorScheduleColumn.RetentionDays)]
    public void TheVerb_IsRecognised_AndParsesCollectorValueServerAndConfig(string verb, CollectorScheduleColumn column)
    {
        Assert.True(column == CollectorScheduleColumn.FrequencyMinutes
            ? DarlingCliCommands.IsSetCollectorFrequencyVerb(verb)
            : DarlingCliCommands.IsSetCollectorRetentionVerb(verb));

        Assert.True(DarlingCliCommands.TryParseCollectorColumnArgs(
            verb, column, new[] { "wait_stats", "10", "--server", "sched-alpha", "--config", "darling.json" },
            out var collector, out var value, out var server, out var config, out var error));
        Assert.Null(error);
        Assert.Equal("wait_stats", collector);
        Assert.Equal("10", value);
        Assert.Equal("sched-alpha", server);
        Assert.Equal("darling.json", config);

        Assert.False(DarlingCliCommands.TryParseCollectorColumnArgs(verb, column, new[] { "wait_stats" }, out _, out _, out _, out _, out var missing));
        Assert.Contains("needs a value", missing);
        Assert.False(DarlingCliCommands.TryParseCollectorColumnArgs(verb, column, new[] { "wait_stats", "10", "--bogus" }, out _, out _, out _, out _, out var unknown));
        Assert.Contains("Unknown option", unknown);
    }

    [Fact]
    public void TheFrequencyVerb_PlansTheExecutorsOwnStatement_AndRefusesAValueAboveTheCap()
    {
        var plan = DarlingCliCommands.PlanCollectorColumn(CollectorScheduleColumn.FrequencyMinutes, "wait_stats", "10", 7401);
        Assert.Equal(CommandKind.StoreWrite, plan.Kind);
        Assert.Equal(CollectorScheduleSql.SetServerColumn(CollectorScheduleColumn.FrequencyMinutes), plan.Sql);
        Assert.Equal(new object?[] { 7401, "wait_stats", 10 }, plan.Parameters);

        var tooBig = DarlingCliCommands.PlanCollectorColumn(CollectorScheduleColumn.FrequencyMinutes, "wait_stats", (CollectorScheduleDefaults.MaxFrequencyMinutes + 1).ToString(), null);
        Assert.NotEqual(CommandKind.StoreWrite, tooBig.Kind);

        var cleared = DarlingCliCommands.PlanCollectorColumn(CollectorScheduleColumn.FrequencyMinutes, "wait_stats", "default", null);
        Assert.Equal(CollectorScheduleSql.ClearFleetColumn(CollectorScheduleColumn.FrequencyMinutes), cleared.Sql);
    }

    [Fact]
    public void TheRetentionVerb_PlansTheExecutorsOwnStatement_AndRefusesAValueAboveTheCap()
    {
        var plan = DarlingCliCommands.PlanCollectorColumn(CollectorScheduleColumn.RetentionDays, "wait_stats", "45", null);
        Assert.Equal(CommandKind.StoreWrite, plan.Kind);
        Assert.Equal(CollectorScheduleSql.SetFleetColumn(CollectorScheduleColumn.RetentionDays), plan.Sql);

        var tooBig = DarlingCliCommands.PlanCollectorColumn(CollectorScheduleColumn.RetentionDays, "wait_stats", (CollectorScheduleDefaults.MaxRetentionDays + 1).ToString(), null);
        Assert.NotEqual(CommandKind.StoreWrite, tooBig.Kind);
        Assert.Contains("3650", tooBig.FailReason);

        Assert.NotEqual(CommandKind.StoreWrite, DarlingCliCommands.PlanCollectorColumn(CollectorScheduleColumn.RetentionDays, "wait_stats", "0", null).Kind);
    }

    /* The purge reads one retention per collector, so a retention set for one server would be stored and never used. The verb refuses it
       before it reads config or opens the store; default (clear) with a server stays allowed, to remove a value an older build stored. */
    [Fact]
    public async Task TheRetentionVerb_RefusesANumberForOneServer_BeforeTouchingConfigOrTheStore()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var code = await DarlingCliCommands.SetCollectorColumnAsync(
            CollectorScheduleColumn.RetentionDays, new[] { "wait_stats", "30", "--server", "sql01", "--config", "C:\\no\\such\\darling.json" },
            output, error, TestContext.Current.CancellationToken);

        Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, code);
        Assert.Equal(DarlingCliCommands.PerServerRetentionRefusalText, error.ToString().Trim());
        Assert.Contains("fleet-wide", error.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, false, "sql01", true)]
    [InlineData(true, true, "sql01", false)]
    [InlineData(true, false, null, false)]
    [InlineData(true, false, " ", false)]
    [InlineData(false, false, "sql01", false)]
    public void ARetentionNumberWithAServer_IsTheOnlyCaseRefused(bool isRetention, bool clears, string? server, bool refused) =>
        Assert.Equal(refused, DarlingCliCommands.PerServerRetentionRefusal(isRetention, clears, server) is not null);

    [Fact]
    public void TheHelpText_ListsBothVerbs()
    {
        var help = DarlingCliCommands.CollectorColumnUsageText(CollectorScheduleColumn.FrequencyMinutes)
            + DarlingCliCommands.CollectorColumnUsageText(CollectorScheduleColumn.RetentionDays);
        Assert.Contains("--set-collector-frequency", help);
        Assert.Contains("--set-collector-retention", help);
    }
}
