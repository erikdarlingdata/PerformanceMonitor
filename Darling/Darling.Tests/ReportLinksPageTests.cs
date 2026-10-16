/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5914: the Links column of the web Investigations panel, from the shipped <c>alerts.js</c> run under Node
/// (<c>report-links-harness.mjs</c>). The harness feeds the vectors of <c>report-link-vectors.json</c>, the SAME list
/// <see cref="InvestigationReportLinksTests"/> pins <c>InvestigationReportLinks.UrlProblem</c> with, through
/// <c>reportLinkProblem</c> and <c>reportLinksCell</c>. <c>reportLinkProblem</c> is the only gate between a stored
/// <c>file:</c>, UNC or <c>javascript:</c> URL and a live anchor, and it is a second implementation of the C# rule, so it
/// is pinned against one list rather than copied twice.
/// </summary>
public sealed class ReportLinksPageTests
{
    private static JsonElement? _result;

    private static JsonElement Result()
    {
        if (_result is { } cached)
            return cached;

        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "report-links-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "report-link-vectors.json"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the report-links harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the report-links harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            _result = doc.RootElement.Clone();
            return _result.Value;
        }
    }

    private static string? Expected(JsonElement vector) => vector.GetProperty("reason").GetString();

    private static JsonElement[] Vectors(bool refused)
    {
        using var doc = JsonDocument.Parse(ReadRepoFile("Darling", "Darling.Tests", "report-link-vectors.json"));
        var wanted = doc.RootElement.GetProperty("vectors").EnumerateArray()
            .Where(v => (Expected(v) is not null) == refused)
            .Select(v => v.GetProperty("name").GetString()!)
            .ToHashSet();
        return Result().GetProperty("vectors").EnumerateArray().Where(v => wanted.Contains(v.GetProperty("name").GetString()!)).ToArray();
    }

    [Fact]
    public void EveryRefusedVector_IsRefusedByThePage_NamingTheSameReasonAsTheCSharpRule()
    {
        using var doc = JsonDocument.Parse(ReadRepoFile("Darling", "Darling.Tests", "report-link-vectors.json"));
        var reasons = doc.RootElement.GetProperty("vectors").EnumerateArray()
            .Where(v => Expected(v) is not null)
            .ToDictionary(v => v.GetProperty("name").GetString()!, v => Expected(v)!);
        var refused = Vectors(refused: true);
        Assert.Equal(reasons.Count, refused.Length);
        Assert.NotEmpty(refused);
        foreach (var v in refused)
        {
            var name = v.GetProperty("name").GetString()!;
            var problem = v.GetProperty("problem").GetString();
            Assert.True(problem is not null && problem.Contains(reasons[name], StringComparison.Ordinal),
                name + ": expected a refusal naming '" + reasons[name] + "', the page said '" + problem + "'.");
        }
    }

    [Fact]
    public void EveryRefusedVector_RendersAsTextWithNoAnchor()
    {
        foreach (var v in Vectors(refused: true))
        {
            var name = v.GetProperty("name").GetString()!;
            Assert.True(v.GetProperty("anchors").GetArrayLength() == 0, name + " drew an anchor.");
            var text = v.GetProperty("text").GetString()!;
            Assert.Contains("shown as text", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryAcceptedVector_IsAnAnchor_ThatOpensInANewTabWithNoOpener()
    {
        var accepted = Vectors(refused: false);
        Assert.NotEmpty(accepted);
        foreach (var v in accepted)
        {
            var name = v.GetProperty("name").GetString()!;
            Assert.True(v.GetProperty("problem").ValueKind == JsonValueKind.Null, name + " was refused: " + v.GetProperty("problem"));
            var anchor = Assert.Single(v.GetProperty("anchors").EnumerateArray());
            var href = anchor.GetProperty("href").GetString()!;
            Assert.StartsWith("https", href, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("_blank", anchor.GetProperty("target").GetString());
            Assert.Equal("noopener noreferrer", anchor.GetProperty("rel").GetString());
            Assert.Equal(href, anchor.GetProperty("title").GetString());
            Assert.Equal("jira OPS-1", anchor.GetProperty("text").GetString());
        }
    }

    [Fact]
    public void ANonLinkList_DrawsAnEmptyCell_AMissingUrlIsText_AndTheNewestReportsLinksReachTheRow()
    {
        var r = Result();
        foreach (var name in new[] { "noLinks", "emptyList", "notAnArray", "nonObjectEntries" })
        {
            Assert.Equal(0, r.GetProperty(name).GetProperty("anchors").GetInt32());
            Assert.Equal("", r.GetProperty(name).GetProperty("text").GetString());
        }

        var missing = r.GetProperty("missingUrl");
        Assert.Equal(0, missing.GetProperty("anchors").GetInt32());
        Assert.Contains("shown as text: missing.", missing.GetProperty("text").GetString(), StringComparison.Ordinal);

        var rows = r.GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal("new", rows[0].GetProperty("verdict").GetString());
        var link = Assert.Single(rows[0].GetProperty("links").EnumerateArray());
        Assert.Equal("pull_request", link.GetProperty("kind").GetString());
        Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("links").ValueKind);
    }
}
