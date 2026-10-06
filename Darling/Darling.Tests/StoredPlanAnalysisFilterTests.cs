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
using System.Runtime.CompilerServices;
using System.Security;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

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
        string result = DarlingMcpPlanTools.AnalyzeFilteredPlan(
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
        string plan = DarlingPlanFixtures.SamplePlanXml;

        string filtered = DarlingMcpPlanTools.AnalyzeFilteredPlan(plan, "srv", source, "id", null, null, default);
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
            ? DarlingMcpPlanTools.AnalyzePlanXml(plan)
            : DarlingMcpPlanTools.AnalyzeFilteredPlan(plan, "srv", source, "id", null, null, default);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("This plan was withheld by the statement filter (#4348), so it was not analysed.",
            doc.RootElement.GetProperty("message").GetString());
        Assert.DoesNotContain("could not be read", result);
    }

    /// <summary>
    /// Every product <c>.cs</c> file under <paramref name="appRoot"/> (not <c>bin</c>, <c>obj</c> or
    /// <c>deprecated</c>) is read with comments and string contents blanked by <c>CSharpSourceWalker</c>. The formatter's <c>BuildAnalysisResult</c> may appear exactly
    /// once, qualified, in <paramref name="helperRelativePath"/>: a direct call in any other file, a call through
    /// <c>using static</c> or an alias, and a method group all name it again and so fail.
    /// </summary>
    private static List<string> FormatterCallProblems(string appRoot, string helperRelativePath)
    {
        var problems = new List<string>();
        string helper = Path.GetFullPath(Path.Combine(appRoot, helperRelativePath));
        int mentions = 0;
        foreach (string file in Directory.EnumerateFiles(appRoot, "*.cs", SearchOption.AllDirectories))
        {
            string relative = Path.GetRelativePath(appRoot, file);
            string[] parts = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (parts.Take(parts.Length - 1).Any(p => p is "bin" or "obj" or "deprecated")) continue;

            bool isHelper = string.Equals(Path.GetFullPath(file), helper, StringComparison.OrdinalIgnoreCase);
            // Read as code, not by line prefix (#3052): the walker blanks line, block and doc comments and string
            // contents wherever they sit, so a mention inside a block comment's continuation lines or after code on a
            // line is judged correctly, and code that follows a closing block comment on its line is still read.
            foreach (string line in CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file)).Split('\n'))
            {
                if (Regex.IsMatch(line, @"\busing\s+static\s+[\w.]*McpPlanAnalysisFormatter\b"))
                    problems.Add(relative + ": using static of the formatter");

                int here = Regex.Matches(line, @"\bBuildAnalysisResult\b").Count;
                if (here == 0) continue;
                mentions += here;
                bool qualified = Regex.IsMatch(line, @"\bMcpPlanAnalysisFormatter\s*\.\s*BuildAnalysisResult\s*\(");
                if (!isHelper || !qualified || here != 1)
                    problems.Add(relative + ": a call to BuildAnalysisResult outside the filtered helper");
            }
        }

        if (mentions != 1) problems.Add("expected exactly one call to BuildAnalysisResult in the helper, found " + mentions);
        return problems;
    }

    [Fact]
    public void EveryAnalysisToolHere_ReachesTheAnalyzerOnlyThroughTheFilteredHelper()
    {
        string appRoot = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");
        string helperRelativePath = Path.Combine("Mcp", "DarlingMcpPlanTools.cs");
        string source = File.ReadAllText(Path.Combine(appRoot, helperRelativePath));

        // One direct call to the formatter across the whole app: the helper's own. The four tools call the helper.
        Assert.Empty(FormatterCallProblems(appRoot, helperRelativePath));
        Assert.Equal(4, Regex.Matches(source, @"return AnalyzeFilteredPlan\(").Count);
        Assert.Contains("xml = SensitiveStatements.Xml(xml) ?? SensitiveStatements.PlaceholderText;", source);
    }

    [Fact]
    public void TheFormatterCallScan_FlagsADirectCallInAnotherFile_AUsingStaticCall_AndIgnoresBinObjAndDeprecated()
    {
        string root = Path.Combine(Path.GetTempPath(), "spaf-" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(root);
            string helperLine = "return McpPlanAnalysisFormatter.BuildAnalysisResult(xml);";
            File.WriteAllText(Path.Combine(root, "Helper.cs"),
                "// McpPlanAnalysisFormatter.BuildAnalysisResult( is named here\n" + helperLine);
            Assert.Empty(FormatterCallProblems(root, "Helper.cs"));

            // Comments are not code wherever they sit: a block comment's unprefixed continuation lines, a trailing
            // comment after code, and a string that spells the name are all ignored.
            File.WriteAllText(Path.Combine(root, "Prose.cs"),
                "/* McpPlanAnalysisFormatter.BuildAnalysisResult(xml) is the old shape,\n"
                + "   and this line was once read as code */\n"
                + "class P { int x = 1; // BuildAnalysisResult( here\n"
                + "string s = \"BuildAnalysisResult(\"; }");
            Assert.Empty(FormatterCallProblems(root, "Helper.cs"));
            // ...but code after a closing block comment on the same line is still code.
            File.WriteAllText(Path.Combine(root, "Prose.cs"), "/* note */ class P { object o = BuildAnalysisResult(x); }");
            Assert.NotEmpty(FormatterCallProblems(root, "Helper.cs"));
            File.Delete(Path.Combine(root, "Prose.cs"));

            foreach (string ignored in new[] { "bin", "obj", "deprecated" })
            {
                Directory.CreateDirectory(Path.Combine(root, ignored));
                File.WriteAllText(Path.Combine(root, ignored, "Skipped.cs"), helperLine);
            }
            Assert.Empty(FormatterCallProblems(root, "Helper.cs"));

            string other = Path.Combine(root, "Other.cs");
            File.WriteAllText(other, "class T { string M() { " + helperLine + " } }");
            Assert.NotEmpty(FormatterCallProblems(root, "Helper.cs"));

            File.WriteAllText(other,
                "using static PerformanceMonitor.PlanAnalysis.McpPlanAnalysisFormatter;\nclass T { string M() { return BuildAnalysisResult(xml); } }");
            Assert.True(FormatterCallProblems(root, "Helper.cs").Count >= 2);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
