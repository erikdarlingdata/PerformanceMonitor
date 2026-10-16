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
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>#5914: the link rules of an investigation report, the answer's links, and the brief's new keys.</summary>
public sealed class InvestigationReportLinksTests
{
    private static string One(string url, string kind = "jira") =>
        new JsonArray(new JsonObject { ["kind"] = kind, ["url"] = url, ["key"] = "OPS-1" }).ToJsonString();

    /// <summary>The rows of report-link-vectors.json, the one list shared with the Node harness (ReportLinksPageTests).</summary>
    private static IEnumerable<object[]> Vectors(bool refused)
    {
        var file = PathTo("Darling", "Darling.Tests", "report-link-vectors.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(file));
        foreach (var v in doc.RootElement.GetProperty("vectors").EnumerateArray())
        {
            var url = v.GetProperty("url").GetString()!;
            if (v.TryGetProperty("padTo", out var pad))
                url = url.PadRight(pad.GetInt32(), 'a');
            var reason = v.GetProperty("reason").GetString();
            if ((reason is not null) == refused)
                yield return new object[] { v.GetProperty("name").GetString()!, url, reason ?? "" };
        }
    }

    public static IEnumerable<object[]> RefusedVectors() => Vectors(refused: true);

    public static IEnumerable<object[]> AcceptedVectors() => Vectors(refused: false);

    [Theory]
    [MemberData(nameof(RefusedVectors))]
    public void ACredentialLeakingOrNonHttpsUrl_IsRefused_NamingTheRule(string name, string url, string reason)
    {
        var why = InvestigationReportLinks.UrlProblem(url);
        Assert.True(why?.Contains(reason, StringComparison.Ordinal) == true, name + ": expected a refusal naming '" + reason + "', got '" + why + "'.");
        var problem = InvestigationReportLinks.Validate(One(url), out var links);
        Assert.NotNull(problem);
        Assert.Contains("https", problem, StringComparison.Ordinal);
        Assert.Empty(links);
        Assert.Throws<ArgumentException>(() => InvestigationReportLinks.ThrowIfInvalid(new[] { new InvestigationReportLink("jira", url, null) }, "report"));
    }

    [Theory]
    [MemberData(nameof(AcceptedVectors))]
    public void AnHttpsUrl_Passes_WithAQueryAnAtInThePathAndTheLimitExactly(string name, string url, string reason)
    {
        Assert.True(reason.Length == 0, name);
        Assert.Null(InvestigationReportLinks.UrlProblem(url));
    }

    [Fact]
    public void TenLinksPass_AnEleventhIsRefused_AndABadKindIsRefused()
    {
        string Many(int n) => new JsonArray(Enumerable.Range(0, n)
            .Select(i => (JsonNode?)new JsonObject { ["kind"] = "jira", ["url"] = "https://jira.example.invalid/browse/OPS-" + i }).ToArray()).ToJsonString();
        Assert.Null(InvestigationReportLinks.Validate(Many(10), out var ten));
        Assert.Equal(10, ten.Count);
        Assert.Equal("11 links, over 10.", InvestigationReportLinks.Validate(Many(11), out _));
        Assert.Contains("kind 'ticket' is not jira, pull_request or bug_report",
            InvestigationReportLinks.Validate(One("https://jira.example.invalid/x", "ticket"), out _), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("[]")]
    [InlineData("null")]
    public void NoLinks_IsNoProblem_AndNoLink(string? links)
    {
        Assert.Null(InvestigationReportLinks.Validate(links, out var parsed));
        Assert.Empty(parsed);
    }

    [Fact]
    public void ARepeatedUrl_IsKeptOnce_TheFirstKindWins_AndShareOneOnceKey()
    {
        var json = "[{\"kind\":\"jira\",\"url\":\"https://jira.example.invalid/browse/OPS-1\"},{\"kind\":\"bug_report\",\"url\":\"https://jira.example.invalid/browse/OPS-1\"}]";
        Assert.Null(InvestigationReportLinks.Validate(json, out var links));
        Assert.Equal("jira", Assert.Single(links).Kind);
        var id = Guid.NewGuid();
        Assert.Equal(InvestigationReportLinks.OnceKey(id, "https://jira.example.invalid/browse/OPS-1"),
            InvestigationReportLinks.OnceKey(id, "https://jira.example.invalid/browse/OPS-1"));
    }

    private static InvestigationReportsRead Read(params InvestigationReportLink[] links)
    {
        var request = Guid.NewGuid();
        var report = new InvestigationReportRow(request, 1, "agent-a", new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc), "real",
            "rc", true, "s", "[{\"tool\":\"t\",\"arguments\":{},\"key_numbers\":\"1\"}]", links.Length == 0 ? null : links);
        var journal = links.Select(l => new InvestigationJournalRow(new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc), request, 7,
            InvestigationReportLinks.JournalEvent, "agent:agent-a", l.Kind, InvestigationReportLinks.DetailJson(l))).ToList();
        return new InvestigationReportsRead(Array.Empty<InvestigationRequestRow>(), new[] { report }, journal, null);
    }

    [Fact]
    public void TheAnswer_HasNoLinksKey_WhenTheReportHasNone()
    {
        var answer = InvestigationToolCore.ReportsAnswer(Read(), 24, McpDataDetail.Full);
        Assert.DoesNotContain("\"links\"", answer, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAnswersLinks_AreMaskedAsFreeText_InTheReportAndTheJournal()
    {
        const string Url = "https://jira.example.invalid/browse/OPS-1?q=SELECT%20*%20FROM%20t%20WHERE%20id%20=%20'secret'";
        var read = Read(new InvestigationReportLink("jira", Url, "OPS-1"));

        var full = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(read, 24, McpDataDetail.Full))!;
        Assert.Equal(Url, full["reports"]![0]!["links"]![0]!["url"]!.GetValue<string>());
        Assert.Equal(Url, full["journal"]![0]!["detail"]!["url"]!.GetValue<string>());

        var withheld = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(read, 24, McpDataDetail.MetadataOnly))!;
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly);
        Assert.Equal(masker.FreeText(Url), withheld["reports"]![0]!["links"]![0]!["url"]?.GetValue<string>());
        Assert.NotEqual(Url, withheld["journal"]![0]!["detail"]!["url"]?.GetValue<string>());
        Assert.Equal("jira", withheld["reports"]![0]!["links"]![0]!["kind"]!.GetValue<string>());
    }
}
