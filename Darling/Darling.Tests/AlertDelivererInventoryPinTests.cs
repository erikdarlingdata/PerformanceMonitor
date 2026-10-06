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
/// #5320 (part of #4348): the statement filter sits at two seams, <c>AlertEngine.FireAsync</c> (every engine alert)
/// and the finding senders (every analysis finding). This pin lists every type in the two apps that implements a
/// deliverer or a finding sender, and every call site that hands an outcome to a deliverer without going through
/// <c>FireAsync</c>. A new implementer or a new direct caller fails here until someone has decided whether its
/// text can carry a statement, and adds it to the list below with the answer.
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
    /// Every file that hands an outcome to a deliverer, with the reason it is reviewed. <c>AlertEngine</c> is the
    /// filtered seam. The others build their own outcomes: PostgreSQL alert families (their statement text is
    /// withheld by the PostgreSQL statement filter where it is read), the Darling self alerts (collector and
    /// store health prose, no captured statement), and custom alert rules (a rule's measure and dimension labels,
    /// which the catalog census keeps free of statement-text columns).
    /// </summary>
    private static readonly string[] OutcomeCallers =
    [
        "PerformanceMonitor.Alerting/AlertEngine.cs",
        "Darling/PerformanceMonitor.Darling.Service/CustomAlertEvaluator.cs",
        "Darling/PerformanceMonitor.Darling.Service/DarlingSelfAlertEvaluator.cs",
        "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs",
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

    [Fact]
    public void EveryDirectHandOffToADelivererIsReviewed()
    {
        var call = new Regex(@"[.]\s*(?:DeliverAndReportAsync|DeliverAsync)\s*\(", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));
        var callers = Sources()
            .Where(s => call.IsMatch(s.Text))
            .Select(s => s.Relative)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(OutcomeCallers.OrderBy(n => n, StringComparer.Ordinal), callers);
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
