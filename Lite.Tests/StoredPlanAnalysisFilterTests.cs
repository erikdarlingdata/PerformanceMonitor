/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Darling.Tests;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348: the three stored-plan analysis tools (<c>analyze_query_plan</c>, <c>analyze_procedure_plan</c>,
/// <c>analyze_query_store_plan</c>) lift parameter values and statement text out of the plan into fields of their own,
/// so the statement filter has to judge the stored XML before the analysis reads it, as <c>analyze_plan_xml</c> does.
/// A plan the filter withholds whole is answered with one plain sentence, not a parse error.
/// </summary>
public sealed class StoredPlanAnalysisFilterTests
{
    [Theory]
    [InlineData("query_stats", "abc123")]
    [InlineData("procedure_stats", "0x0200abcd")]
    [InlineData("query_store", "Db:7")]
    public void TheAnalysisOfTheCanaryPlan_HoldsNoSecret_ForEachStoredPlanSource(string source, string identifier)
    {
        string result = McpPlanTools.AnalyzeFilteredPlan(
            StatementScrubCanary.CanaryPlan(), "srv", source, identifier, null, null, default);

        foreach (string needle in StatementScrubCanary.SecretNeedles)
            Assert.DoesNotContain(needle, result);
        JsonDocument.Parse(result).Dispose();
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    [InlineData("query_store")]
    public void ACleanPlansAnalysis_IsByteIdenticalToTheUnfilteredAnalysis(string source)
    {
        string plan = SamplePlanXml;

        string filtered = McpPlanTools.AnalyzeFilteredPlan(plan, "srv", source, "id", null, null, default);
        string direct = McpPlanAnalysisFormatter.BuildAnalysisResult(plan, "srv", source, "id", null, null, default);

        Assert.Equal(direct, filtered);
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    [InlineData("query_store")]
    [InlineData("xml")]
    public void APlanTheFilterWithholdsWhole_IsAnsweredWithOnePlainSentence_NotAParseError(string source)
    {
        // A document that does not parse is judged as decoded text and withheld whole when it names a sensitive
        // statement (the same outcome as a spent read budget).
        string plan = "<ShowPlanXML><StmtSimple StatementText=\"" + SecurityElement.Escape(StatementScrubCanary.CanaryStatement) + "\"";
        Assert.Equal(SensitiveStatements.PlaceholderText, SensitiveStatements.Xml(plan));

        string result = source == "xml"
            ? McpPlanTools.AnalyzePlanXml(plan)
            : McpPlanTools.AnalyzeFilteredPlan(plan, "srv", source, "id", null, null, default);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("This plan was withheld by the statement filter (#4348), so it was not analysed.",
            doc.RootElement.GetProperty("message").GetString());
        Assert.DoesNotContain("could not be read", result);
    }

    [Fact]
    public void EveryAnalysisToolHere_ReachesTheAnalyzerOnlyThroughTheFilteredHelper()
    {
        string source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Lite", "Mcp", "McpPlanTools.cs"));

        // One direct call to the formatter: the helper's own. The four tools call the helper.
        Assert.Single(Regex.Matches(source, @"McpPlanAnalysisFormatter\.BuildAnalysisResult\("));
        Assert.Equal(4, Regex.Matches(source, @"return AnalyzeFilteredPlan\(").Count);
        Assert.Contains("xml = SensitiveStatements.Xml(xml) ?? SensitiveStatements.PlaceholderText;", source);
    }

    private const string SamplePlanXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" />" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
