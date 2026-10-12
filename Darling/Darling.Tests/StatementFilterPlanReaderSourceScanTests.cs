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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, Layer 1: the stored-plan reads are filtered at ONE seam, <c>DarlingStoredPlanReader</c>. Three pins keep it that way.
/// (1) Every public method of the reader is listed here, takes the optional <c>maxOutputChars</c> and answers through
/// <c>SensitiveStatements.Xml</c> (or through another listed method that does); a new public read fails until it is listed,
/// wrapped and planted in <c>StatementFilterPlanReadsLiveTests</c>. (2) No other file under <c>Mcp</c> names a plan column in
/// SQL it runs, so a tool cannot read a plan around the seam. (3) The scan itself is proven against planted sources.
/// </summary>
public sealed class StatementFilterPlanReaderSourceScanTests
{
    /// <summary>Each public read, and how many times its own body calls <c>SensitiveStatements.Xml</c> (a method that only
    /// delegates to another listed read has none).</summary>
    private static readonly Dictionary<string, int> PublicReads = new(StringComparer.Ordinal)
    {
        ["GetQueryStatsPlanXmlByHashAsync"] = 1,
        ["GetProcedurePlanXmlBySqlHandleAsync"] = 1,
        ["GetQueryStorePlanTextAsync"] = 0,
        ["ResolveQueryStorePlanAsync"] = 1,
        ["ReadQueryStorePlanByIdAsync"] = 1,
        ["GetQuerySnapshotPlanXmlAsync"] = 1,
        ["GetBlockingPlanXmlAsync"] = 1,
        ["GetDeadlockVictimPlanXmlAsync"] = 1,
    };

    [Fact]
    public void EveryPublicPlanRead_IsListed_TakesMaxOutputChars_AndIsPlantedInTheLiveTests()
    {
        string[] actual = typeof(DarlingStoredPlanReader)
            .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(m => m.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(PublicReads.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray(), actual);

        string live = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "Darling.Tests", "StatementFilterPlanReadsLiveTests.cs"));
        foreach (MethodInfo method in typeof(DarlingStoredPlanReader).GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly))
        {
            ParameterInfo? cut = method.GetParameters().SingleOrDefault(p => p.Name == "maxOutputChars");
            Assert.True(cut is not null && cut.HasDefaultValue && Equals(cut.DefaultValue, int.MaxValue),
                method.Name + " must take an optional maxOutputChars that defaults to int.MaxValue");
            ParameterInfo[] parameters = method.GetParameters();
            Assert.True(parameters[^1].ParameterType == typeof(System.Threading.CancellationToken) && parameters[^2] == cut,
                method.Name + ": maxOutputChars sits just before the CancellationToken (CA1068)");
            Assert.Contains("DarlingStoredPlanReader." + method.Name + "(", live, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryPublicPlanRead_AnswersThroughTheStatementFilter_AndNoRawTwinIsPublic()
    {
        string source = ReaderCode();
        foreach ((string name, int expected) in PublicReads)
        {
            string body = MemberSource(source, name);
            Assert.Equal(expected, Regex.Matches(body, @"\bSensitiveStatements\s*\.\s*Xml\s*\(").Count);
            if (expected == 0)
                Assert.Matches(@"\bResolveQueryStorePlanAsync\s*\(", body);
            else
                Assert.Contains("maxOutputChars", body, StringComparison.Ordinal);
        }

        // The unfiltered twins are private, so nothing outside the file can reach a raw plan.
        MatchCollection twins = Regex.Matches(source, @"^    (?<access>\w+) static [^\n]*?\b\w+RawAsync\(", RegexOptions.Multiline);
        Assert.Equal(PublicReads.Count - 1, twins.Count);
        foreach (Match twin in twins)
            Assert.Equal("private", twin.Groups["access"].Value);
    }

    /// <summary>The columns that hold a plan: the stored XML, its gzip form, Query Store's text, the snapshot columns, and every
    /// <c>*_plan_xml</c> (the blocked, blocking and victim plans). <c>query_plan_hash</c> is a hash, not a plan.</summary>
    private static readonly Regex PlanColumn = new(
        @"(?<![\w])(?:live_query_plan|query_plan_text|query_plan_gz|query_plan_xml|query_plan|\w*plan_xml)(?![\w])", RegexOptions.Compiled);

    /// <summary>The reads of a plan column that are not plan reads: <c>col IS [NOT] NULL</c> and <c>col &lt;&gt; ''</c>, and the
    /// <c>has_*</c> aliases they are printed under: the has-a-plan flags the lists print.</summary>
    private static readonly Regex PresenceOnly = new(
        @"(?<![\w])[\w.]*plan(?:_xml)?\s*(?:IS\s+(?:NOT\s+)?NULL|<>\s*'')|(?<![\w])AS\s+has_\w+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex LooksLikeSql = new(@"\bSELECT\b[\s\S]*\bFROM\b", RegexOptions.Compiled);

    /// <summary>The names of the plan columns that SQL string literals in <paramref name="source"/> read, with the presence-only
    /// idiom taken out first. Comments are not code, and a string that is not SQL (a tool description) is not a read.</summary>
    internal static List<string> PlanColumnReads(string source)
    {
        var found = new List<string>();
        foreach ((int _, string text) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            if (!LooksLikeSql.IsMatch(text)) continue;
            foreach (Match m in PlanColumn.Matches(PresenceOnly.Replace(text, " ")))
                found.Add(m.Value);
        }

        return found;
    }

    [Fact]
    public void NoFileUnderMcp_ReadsAPlanColumn_ExceptTheStoredPlanReader()
    {
        string mcp = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp");
        var offenders = new List<string>();
        foreach (string file in Directory.EnumerateFiles(mcp, "*.cs", SearchOption.AllDirectories))
        {
            if (string.Equals(Path.GetFileName(file), "DarlingStoredPlanReader.cs", StringComparison.Ordinal)) continue;
            foreach (string column in PlanColumnReads(File.ReadAllText(file)).Distinct())
                offenders.Add(Path.GetFileName(file) + ": " + column);
        }

        Assert.Empty(offenders);
        // The reader itself is the one place the scan must find them: a scan that finds none there reads nothing.
        Assert.NotEmpty(PlanColumnReads(File.ReadAllText(Path.Combine(mcp, "DarlingStoredPlanReader.cs"))));
    }

    [Fact]
    public void ThePlanColumnScan_FlagsEachPlanColumn_AndIgnoresHashesFlagsCommentsAndDescriptions()
    {
        foreach (string column in new[] { "query_plan_xml", "query_plan_text", "query_plan_gz", "query_plan", "live_query_plan", "victim_query_plan_xml", "blocked_query_plan_xml", "plan_xml" })
        {
            Assert.Equal(new[] { column }, PlanColumnReads("const string S = \"SELECT " + column + " FROM t\";"));
            Assert.Equal(new[] { column }, PlanColumnReads("const string S = \"\"\"\n    SELECT 1\n    FROM t WHERE x = (SELECT " + column + " FROM u)\n    \"\"\";"));
        }

        // Not reads of a plan: a plan HASH, a presence flag, a comment, and a tool description that is not SQL.
        Assert.Empty(PlanColumnReads("const string S = \"SELECT query_plan_hash FROM t\";"));
        Assert.Empty(PlanColumnReads("const string S = \"SELECT (blocked_query_plan_xml IS NOT NULL AND blocked_query_plan_xml <> '') AS f FROM t\";"));
        Assert.Empty(PlanColumnReads("// SELECT query_plan_xml FROM t\nconst string S = \"SELECT 1 FROM t\";"));
        Assert.Empty(PlanColumnReads("[Description(\"Returns the raw stored query_plan_xml, see get_plan_xml\")] void M() { }"));
        Assert.Empty(PlanColumnReads("const string S = \"SELECT (query_plan IS NOT NULL) AS has_query_plan, (b.plan_xml IS NOT NULL) AS has_plan_xml FROM t\";"));
        // A flag does not excuse a read beside it.
        Assert.Equal(new[] { "blocked_query_plan_xml" },
            PlanColumnReads("const string S = \"SELECT blocked_query_plan_xml, (blocking_query_plan_xml IS NOT NULL) AS f FROM t\";"));
    }

    // ── helpers ──

    private static string ReaderCode() => CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(
        Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingStoredPlanReader.cs")));

    /// <summary>The text of the member that declares <paramref name="name"/> as a method, from its declaration line to the next
    /// member at class level (four spaces of indent).</summary>
    private static string MemberSource(string code, string name)
    {
        Match start = Regex.Match(code, @"^    public static [^\n]*?\b" + Regex.Escape(name) + @"\(", RegexOptions.Multiline);
        Assert.True(start.Success, name + " has no public declaration");
        Match next = Regex.Match(code.Substring(start.Index + start.Length), @"^    (?:public|private|internal)\b", RegexOptions.Multiline);
        return next.Success ? code.Substring(start.Index, start.Length + next.Index) : code.Substring(start.Index);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
