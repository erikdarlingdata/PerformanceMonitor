/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts alert's detail items (#5640) and the mute pre-fill they feed: the rate, the exact count, the minutes covered,
/// the top 5 statements with their text through the statement filter, the top databases and client applications, the "estimated from
/// N sampled events" line only when the mute proportion applied, and "Mute this alert" seeding the top client application and the
/// top database from the stored detail text (the C# parse both desktop dialogs use). Also pins the shared read's classification.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ClientTimeoutAlertDetailTests
{
    private static readonly DateTime T0 = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);

    private static AttentionWindow Window(int statements = 7) => new(
        Array.Empty<DateTime>(),
        Array.Empty<AttentionCountRow>(),
        Array.Empty<AttentionSampleGroup>(),
        Enumerable.Range(1, statements).Select(i => new AttentionTopItem($"fp-{i}", 100 - i, $"SELECT {i} FROM t")).ToList(),
        new[] { new AttentionTopItem("sales", 50), new AttentionTopItem("hr", 20) },
        new[] { new AttentionTopItem("etl-job", 60), new AttentionTopItem("web", 10) });

    private static AttentionEvaluator.Finding Finding(bool estimated) => new(
        AlertSeverityLevel.Warning, 12.5, 10, 150, estimated ? 75 : 150, 12, 5, 40, estimated, false, T0);

    [Fact]
    public void TheItems_CarryTheRateCountAndMinutesCovered()
    {
        var items = AlertContextBuilders.BuildClientTimeoutItems(Finding(false), Window());

        var reading = items[0];
        Assert.Equal("Client timeouts", reading.Heading);
        Assert.Contains(reading.Fields, f => f.Label == "Rate" && f.Value.StartsWith("12.5 per minute", StringComparison.Ordinal));
        Assert.Contains(reading.Fields, f => f.Label == "Count" && f.Value.StartsWith("150 exact", StringComparison.Ordinal));
        Assert.Contains(reading.Fields, f => f.Label == "Minutes Covered" && f.Value.StartsWith("12 ", StringComparison.Ordinal));
        Assert.DoesNotContain(reading.Fields, f => f.Label == "Estimate");
    }

    [Fact]
    public void TheEstimateLine_AppearsOnlyWhenTheMuteProportionApplied()
    {
        var estimated = AlertContextBuilders.BuildClientTimeoutItems(Finding(true), Window())[0];

        var line = Assert.Single(estimated.Fields, f => f.Label == "Estimate");
        Assert.Contains("estimated from 40 sampled events", line.Value, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDetailText_NamesTheTopClientHosts_NextToTheTopApplications()
    {
        /* #6553: who was timing out, in the alert itself. A window without hosts lists no host block. */
        var window = Window() with { TopHosts = new[] { new AttentionTopItem("WS-ERIK", 90), new AttentionTopItem("APP01", 12) } };
        var context = new AlertContext();
        foreach (var item in AlertContextBuilders.BuildClientTimeoutItems(Finding(false), window))
        {
            context.Details.Add(item);
        }

        var hosts = Assert.Single(context.Details, i => i.Heading.StartsWith("Top client hosts", StringComparison.Ordinal));
        Assert.Equal(new[] { "WS-ERIK", "APP01" }, hosts.Fields.Where(f => f.Label == "Client Host").Select(f => f.Value));
        Assert.Contains("Client Host: WS-ERIK", AlertContextBuilders.ContextToDetailText(context), StringComparison.Ordinal);
        Assert.DoesNotContain(AlertContextBuilders.BuildClientTimeoutItems(Finding(false), Window()),
            i => i.Heading.StartsWith("Top client hosts", StringComparison.Ordinal));
    }

    [Fact]
    public void OnlyTheTopFiveStatements_AreListed_EachWithItsSampleText()
    {
        var items = AlertContextBuilders.BuildClientTimeoutItems(Finding(false), Window(statements: 7));

        var statements = items.Where(i => i.Heading.StartsWith("Top statement ", StringComparison.Ordinal)).ToList();
        Assert.Equal(5, statements.Count);
        Assert.Contains(statements[0].Fields, f => f.Label == "Fingerprint" && f.Value == "fp-1");
        Assert.Contains(statements[0].Fields, f => f.Label == "Statement" && f.Value.Contains("SELECT 1", StringComparison.Ordinal));
    }

    [Fact]
    public void TheDetailText_SeedsTheTopClientApplicationAndTheTopDatabase_ForMuteThisAlert()
    {
        var context = new AlertContext();
        foreach (var item in AlertContextBuilders.BuildClientTimeoutItems(Finding(false), Window()))
        {
            context.Details.Add(item);
        }

        var detailText = AlertContextBuilders.ContextToDetailText(context);
        var mute = new AlertMuteContext { ServerName = "sql01", MetricName = AttentionEvaluator.MetricName };
        mute.PopulateFromDetailText(detailText, AttentionEvaluator.MetricName);

        Assert.Equal("etl-job", mute.ClientAppName);
        Assert.Equal("sales", mute.DatabaseName);
    }

    [Fact]
    public void ASpanWithSamples_ListsAreTheSpans_HeadingsStayPlain_AndThePrefillTakesTheirTopEntries()
    {
        var items = AlertContextBuilders.BuildClientTimeoutItems(Finding(false), Window());

        Assert.Contains(items, i => i.Heading == "Top databases (sampled events)");
        Assert.Contains(items, i => i.Heading == "Top client applications (sampled events)");
        Assert.DoesNotContain(items, i => i.Heading.Contains("in the last", StringComparison.Ordinal));

        /* Final round of #5714 (L3): the positive half of the L6 rule. A span list is plain-headed, so the prefill takes its top entries
           (the lookback fallback below takes neither). */
        var context = new AlertContext();
        foreach (var item in items)
        {
            context.Details.Add(item);
        }

        var mute = new AlertMuteContext { ServerName = "sql01", MetricName = AttentionEvaluator.MetricName };
        mute.PopulateFromDetailText(AlertContextBuilders.ContextToDetailText(context), AttentionEvaluator.MetricName);
        Assert.Equal("sales", mute.DatabaseName);
        Assert.Equal("etl-job", mute.ClientAppName);
    }

    [Fact]
    public void ASpanWithNoSamples_ShowsTheLookbackLists_SaysTheirRealSpan_AndPrefillsNothingFromThem()
    {
        var window = Window() with { TopsFromLookback = true };
        var context = new AlertContext();
        foreach (var item in AlertContextBuilders.BuildClientTimeoutItems(Finding(false), window))
        {
            context.Details.Add(item);
        }

        var headings = context.Details.Select(i => i.Heading).ToList();
        Assert.Contains("Top databases (sampled events in the last 60 minutes)", headings);
        Assert.Contains("Top client applications (sampled events in the last 60 minutes)", headings);
        Assert.Contains(headings, h => h.StartsWith("Top statement 1: ", StringComparison.Ordinal)
            && h.EndsWith(" in the last 60 minutes", StringComparison.Ordinal));

        var mute = new AlertMuteContext { ServerName = "sql01", MetricName = AttentionEvaluator.MetricName };
        mute.PopulateFromDetailText(AlertContextBuilders.ContextToDetailText(context), AttentionEvaluator.MetricName);
        Assert.Null(mute.ClientAppName);
        Assert.Null(mute.DatabaseName);
    }

    [Fact]
    public void TheWebPrefill_ParsesTheSameLabelsAsTheDesktopDialogs()
    {
        var script = System.IO.File.ReadAllText(RepoFile("Darling/PerformanceMonitor.Darling.Service/wwwroot/js/mute-context.js"));
        Assert.Contains("\"Client Application: \"", script, StringComparison.Ordinal);
        Assert.Contains("client_app_pattern: d.clientApp", script, StringComparison.Ordinal);
    }

    [Fact]
    public void ARangeWithNoRuns_IsNotObserved_AndAQuietRangeIsEmpty()
    {
        var none = new AttentionWindow(Array.Empty<DateTime>(), Array.Empty<AttentionCountRow>(), Array.Empty<AttentionSampleGroup>(),
            Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>());
        Assert.Equal(ClientTimeoutsKind.NotObserved, ClientTimeoutsRead.Classify(none, T0).Kind);

        var runs = Enumerable.Range(0, 4).Select(i => T0.AddMinutes(-i)).ToList();
        var quiet = none with { RunTimes = runs };
        Assert.Equal(ClientTimeoutsKind.Quiet, ClientTimeoutsRead.Classify(quiet, T0).Kind);

        var stale = none with { RunTimes = runs.Select(r => r.AddHours(-2)).ToList() };
        Assert.Equal(ClientTimeoutsKind.NotObserved, ClientTimeoutsRead.Classify(stale, T0).Kind);
    }

    private static string RepoFile(string relative)
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        Assert.NotNull(dir);
        return System.IO.Path.Combine(dir!.FullName, relative);
    }
}
