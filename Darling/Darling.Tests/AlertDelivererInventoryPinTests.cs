/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5320 (part of #4348): the statement filter sits at the delivery choke point. Both deliverers run
/// <c>AlertStatementFilter.Apply</c> at their entry, so every caller that hands an outcome to one (the engine's
/// <c>FireAsync</c>, the PostgreSQL families, the self alerts, the custom alert rules, and any caller added later) is
/// filtered with no list to keep; <c>AlertOutcome.StatementFiltered</c> stops an engine alert being judged twice. The
/// finding senders filter their own entry points. This pin lists every type that implements a deliverer or a finding
/// sender, requires the filter at each entry, and lists every file that reaches a notifier or writes an alert history
/// row, so a new path that skips the deliverer fails here until someone has put the filter on it.
/// </summary>
public sealed class AlertDelivererInventoryPinTests
{
    private static readonly string[] SourceRoots =
    [
        "Darling/PerformanceMonitor.Darling.Service",
        "Lite",
        "PerformanceMonitor.Alerting",
        "PerformanceMonitor.Notifications",
        "PerformanceMonitor.Common",
    ];

    /// <summary>Reviewed deliverers. Each re-derives its text from the context the engine already filtered.</summary>
    private static readonly string[] Deliverers = ["DarlingAlertDeliverer", "LiteAlertDeliverer"];

    /// <summary>Reviewed finding senders. Each starts by running <c>AlertStatementFilter.Apply</c> on the finding.</summary>
    private static readonly string[] FindingSenders = ["DarlingFindingAlertSender", "EmailAlertService"];

    /// <summary>
    /// Every file that reaches a notification channel or writes a fired alert's history row without a deliverer
    /// between. The two deliverers filter at entry; the finding senders filter their own entry points; the two
    /// Lite sends in <c>MainWindow.AlertEngine.cs</c> (connection alerts and Availability Group alerts) build an
    /// outcome and run the filter themselves, which the test below requires.
    /// </summary>
    private static readonly string[] NotifierPaths =
    [
        "Darling/PerformanceMonitor.Darling.Service/DarlingAlertDeliverer.cs",
        "Darling/PerformanceMonitor.Darling.Service/DarlingFindingAlertSender.cs",
        "Lite/MainWindow.AlertEngine.cs",
        "Lite/Services/EmailAlertService.cs",
        "Lite/Services/LiteAlertDeliverer.cs",
    ];

    private static IEnumerable<(string Relative, string Text)> Sources()
    {
        foreach (var root in SourceRoots)
        {
            var directory = PathTo(root.Split('/'));
            foreach (var file in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(Root, file).Replace('\\', '/');
                if (relative.Contains("/obj/", StringComparison.Ordinal) || relative.Contains("/bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return (relative, CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file)));
            }
        }
    }

    private static List<string> Implementers(string interfaceName)
    {
        var declaration = new Regex(
            @"\b(?:class|record|struct)\s+(?<name>\w+)\s*(?:<[^>]*>)?\s*(?:\([^)]*\))?\s*:\s*(?<bases>[^{;]*)[{;]",
            RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));
        var found = new List<string>();
        foreach (var (_, text) in Sources())
        {
            foreach (Match match in declaration.Matches(text))
            {
                if (Regex.IsMatch(match.Groups["bases"].Value, @"\b" + interfaceName + @"\b", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5)))
                {
                    found.Add(match.Groups["name"].Value);
                }
            }
        }

        return found.Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
    }

    [Fact]
    public void EveryAlertDelivererInBothAppsIsReviewed()
    {
        Assert.Equal(Deliverers.OrderBy(n => n, StringComparer.Ordinal), Implementers("IAlertDeliverer"));
    }

    [Fact]
    public void EveryFindingSenderInBothAppsIsReviewed()
    {
        Assert.Equal(FindingSenders.OrderBy(n => n, StringComparer.Ordinal), Implementers("IFindingAlertSender"));
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingAlertDeliverer.cs")]
    [InlineData("Lite/Services/LiteAlertDeliverer.cs")]
    public void EveryDelivererFiltersBeforeItReadsTheOutcome(string relative)
    {
        var text = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(relative.Split('/')));
        var start = text.IndexOf("Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome", StringComparison.Ordinal);
        Assert.True(start > 0, "DeliverAndReportAsync not found in " + relative);
        var body = CSharpSourceWalker.BraceBalanced(text, text.IndexOf('{', start));

        var filter = body.IndexOf("outcome = AlertStatementFilter.Apply(outcome);", StringComparison.Ordinal);
        Assert.True(filter >= 0, relative + " does not run the statement filter at its delivery entry");
        Assert.True(filter < body.IndexOf("try", StringComparison.Ordinal), "the filter must run before the delivery body");
        Assert.True(filter < body.IndexOf("outcome.", StringComparison.Ordinal), "the filter must run before any field of the outcome is read");
    }

    [Fact]
    public void NoPathToANotifierOrTheAlertHistorySkipsTheDeliverer()
    {
        var channel = new Regex(
            @"[.]\s*(?:TrySendAlertEmailAsync|TrySendAsync|RecordAlertAsync)\s*\(\s*(?!(?:DarlingSelfAlertEvaluator[.])?BuildResolutionRecord)",
            RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));
        var files = Sources()
            .Where(s => channel.IsMatch(s.Text))
            .Select(s => s.Relative)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(NotifierPaths.OrderBy(n => n, StringComparer.Ordinal), files);
    }

    [Fact]
    public void TheTwoLiteDirectSendsFilterTheirOwnText()
    {
        var text = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile("Lite", "MainWindow.AlertEngine.cs"));
        var sends = Regex.Matches(text, @"_emailAlertService[.]TrySendAlertEmailAsync\(", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5)).Count;
        var filters = Regex.Matches(text, @"AlertStatementFilter[.]Apply\(", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5)).Count;
        Assert.Equal(2, sends);
        Assert.Equal(sends, filters);
    }

    [Theory]
    [InlineData("Task<PerformanceMonitor.Notifications.AlertDelivery?> SendConnectionAlert(")]
    [InlineData("Task<PerformanceMonitor.Notifications.AlertDelivery?> SendAgAlert(")]
    public void EachLiteDirectSendFiltersItsOwnTextAndSendsTheFilteredCopy(string signature)
    {
        /* #5320: both sends are WPF window methods no unit test can run, so this source pin is what fails when one of
           them loses its filter. Checked per method (a count over the file is satisfied by a filter call in a third
           place): the filter runs before the send, and the send reads the filtered copy, not the raw text. */
        var text = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile("Lite", "MainWindow.AlertEngine.cs"));
        var start = text.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start > 0, signature + " not found");
        var body = CSharpSourceWalker.BraceBalanced(text, text.IndexOf('{', start));

        var filter = body.IndexOf("filtered = AlertStatementFilter.Apply(", StringComparison.Ordinal);
        var send = body.IndexOf("_emailAlertService.TrySendAlertEmailAsync(", StringComparison.Ordinal);
        Assert.True(filter >= 0, signature + " does not run the statement filter");
        Assert.True(send > filter, "the filter must run before the send in " + signature);

        var call = body[send..];
        Assert.Contains("filtered.ShortMessage", call, StringComparison.Ordinal);
        Assert.Contains("filtered.DetailText", call, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryResolutionRowIsBuiltThroughTheFilteredRecordBuilder()
    {
        var evaluator = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs"));
        var start = evaluator.IndexOf("AlertHistoryRecord BuildResolutionRecord(AlertResolution resolution)", StringComparison.Ordinal);
        Assert.True(start > 0, "BuildResolutionRecord not found");
        var end = evaluator.IndexOf(';', start);
        Assert.Contains("SensitiveStatements.Text(resolution.Message)", evaluator[start..end], StringComparison.Ordinal);
    }

    [Fact]
    public void FireAsyncFiltersBeforeItLogsOrDelivers()
    {
        var engine = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs"));
        var start = engine.IndexOf("Task<AlertDelivery?> FireAsync(AlertOutcome outcome", StringComparison.Ordinal);
        Assert.True(start > 0, "FireAsync not found");
        var body = CSharpSourceWalker.BraceBalanced(engine, engine.IndexOf('{', start));

        var filter = body.IndexOf("AlertStatementFilter.Apply(outcome)", StringComparison.Ordinal);
        Assert.True(filter >= 0, "FireAsync does not run the statement filter");
        Assert.True(filter < body.IndexOf("_logger", StringComparison.Ordinal), "the filter must run before the log line");
        Assert.True(filter < body.IndexOf("_deliverer.DeliverAndReportAsync", StringComparison.Ordinal), "the filter must run before delivery");
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingFindingAlertSender.cs")]
    [InlineData("Lite/Services/EmailAlertService.cs")]
    public void EachFindingSenderFiltersBothEntryPoints(string relative)
    {
        var text = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(relative.Split('/')));
        foreach (var method in new[] { "SendFindingAlertAsync(FindingAlert alert)", "SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named)" })
        {
            var start = text.IndexOf(method, StringComparison.Ordinal);
            Assert.True(start > 0, method + " not found in " + relative);
            var body = CSharpSourceWalker.BraceBalanced(text, text.IndexOf('{', start));
            Assert.Contains("AlertStatementFilter.Apply(", body, StringComparison.Ordinal);
        }
    }
}
