/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Plan XML that a caller supplies (a file opened or dropped, pasted text, or the <c>plan_xml</c> argument of
/// <c>analyze_plan_xml</c>) is bounded before it reaches the parser. Measured on the shipped parser, one element
/// with N attributes costs about N squared: 100,000 attributes take 0.27 s and 1,000,000 take 27 s (11.9M
/// characters, inside the 16M cap), while the same attributes over many elements are linear. The count walks
/// the attributes of each tag in one pass, so a refused input costs milliseconds.
/// </summary>
[Collection("timing")]
public sealed class CallerPlanXmlTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static string Wrap(string inner) =>
        $"<ShowPlanXML xmlns=\"{Ns}\" Version=\"1.564\" Build=\"16.0.1000.6\">{inner}</ShowPlanXML>";

    /// <summary>One element carrying <paramref name="count"/> attributes with unique names.</summary>
    private static string ElementWithAttributes(int count, string value = "1")
    {
        var sb = new StringBuilder(count * 12 + 64);
        sb.Append("<BatchSequence ");
        for (var i = 0; i < count; i++) sb.Append('a').Append(i).Append("=\"").Append(value).Append("\" ");
        sb.Append("/>");
        return Wrap(sb.ToString());
    }

    private static string FixturePath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", fileName);

    private static string[] FixtureFiles() =>
        Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans"), "*.sqlplan");

    [Fact]
    public void Limit_LeavesTenTimesTheWidestElementInTheRealFixturePlans()
    {
        var widest = 0;
        foreach (var file in FixtureFiles())
        {
            var doc = XDocument.Parse(File.ReadAllText(file));
            widest = Math.Max(widest, doc.Descendants().Max(e => e.Attributes().Count()));
        }

        Assert.InRange(widest, 1, int.MaxValue);
        Assert.True(CallerPlanXml.MaxAttributesPerElement >= 10 * widest,
            $"the limit {CallerPlanXml.MaxAttributesPerElement} must stay at least 10x the widest real element ({widest} attributes).");
    }

    [Fact]
    public void SizeCap_IsStillSixteenMillionCharacters()
    {
        // Size alone is linear (1,000,000 one-attribute elements parse in 0.8 s), and people open very large
        // real plans, so the caller gate adds no size limit of its own.
        Assert.Equal(16 * 1024 * 1024, ShowPlanParser.MaxParseCharacters);
    }

    [Fact]
    public void ElementAtTheLimit_IsNotRefused_AndStillParses()
    {
        var xml = ElementWithAttributes(CallerPlanXml.MaxAttributesPerElement);

        Assert.Null(CallerPlanXml.RefusalReason(xml));
        Assert.Null(ShowPlanParser.Parse(xml).ParseError);
    }

    [Fact]
    public void ElementJustPastTheLimit_IsRefused_NamingTheLimit()
    {
        var xml = ElementWithAttributes(CallerPlanXml.MaxAttributesPerElement + 1);

        var reason = CallerPlanXml.RefusalReason(xml);

        Assert.NotNull(reason);
        Assert.Contains(CallerPlanXml.MaxAttributesPerElement.ToString("N0"), reason);
        Assert.Contains("attributes", reason);
    }

    [Fact]
    public void AMillionAttributeElement_IsRefusedInWellUnderASecond_NotParsedForHalfAMinute()
    {
        var xml = ElementWithAttributes(1_000_000);
        Assert.InRange(xml.Length, 1, ShowPlanParser.MaxParseCharacters);

        var sw = Stopwatch.StartNew();
        var reason = CallerPlanXml.RefusalReason(xml);
        sw.Stop();

        Assert.NotNull(reason);
        Assert.True(sw.ElapsedMilliseconds < 1000, $"refusal took {sw.ElapsedMilliseconds} ms; the parse it replaces takes about 27 s.");
    }

    [Fact]
    public void AGreaterThanSignInEachValue_DoesNotHideTheAttributes()
    {
        // A plain '<'..'>' count resets at the '>' inside the first value and sees one attribute; this counter
        // treats the value as quoted text, so the tag still counts every attribute.
        var sb = new StringBuilder("<BatchSequence ");
        for (var i = 0; i < CallerPlanXml.MaxAttributesPerElement + 1; i++) sb.Append('a').Append(i).Append("=\">\" ");
        sb.Append("/>");

        Assert.NotNull(CallerPlanXml.RefusalReason(Wrap(sb.ToString())));
    }

    [Fact]
    public void SingleQuotedValues_AreTrackedToo()
    {
        var sb = new StringBuilder("<BatchSequence ");
        for (var i = 0; i < CallerPlanXml.MaxAttributesPerElement + 1; i++) sb.Append('a').Append(i).Append("='\">' ");
        sb.Append("/>");

        Assert.NotNull(CallerPlanXml.RefusalReason(Wrap(sb.ToString())));
    }

    [Fact]
    public void EqualsSignsInsideOneValue_AreNotAttributes()
    {
        // A predicate such as [a]=(1) OR [a]=(2) ... holds an '=' per term; a real plan with a long IN list
        // must not be counted as thousands of attributes.
        var predicate = string.Join(" OR ", Enumerable.Range(0, CallerPlanXml.MaxAttributesPerElement * 5).Select(i => $"[a]=({i})"));
        var xml = Wrap($"<BatchSequence ScalarString=\"{predicate}\" />");

        Assert.Null(CallerPlanXml.RefusalReason(xml));
        Assert.Null(ShowPlanParser.Parse(xml).ParseError);
    }

    [Fact]
    public void AQuoteInAComment_DoesNotCarryIntoTheNextTag()
    {
        // The '<' of the next tag clears the quote, so an odd quote in a comment cannot hide a flood after it.
        var flood = ElementWithAttributes(CallerPlanXml.MaxAttributesPerElement + 1);
        var xml = "<!-- a \" quote -->" + flood;

        Assert.NotNull(CallerPlanXml.RefusalReason(xml));
    }

    [Fact]
    public void ManyLargeButNarrowElements_AreNotRefused_AndParse()
    {
        // 100,000 elements with 20 attributes each (about 7M characters): big, linear to parse, and nothing like a flood.
        var attrs = string.Join(" ", Enumerable.Range(0, 20).Select(i => $"a{i}=\"1\""));
        var sb = new StringBuilder("<BatchSequence>");
        for (var i = 0; i < 100_000; i++) sb.Append("<E ").Append(attrs).Append("/>");
        sb.Append("</BatchSequence>");
        var xml = Wrap(sb.ToString());

        var sw = Stopwatch.StartNew();
        var reason = CallerPlanXml.RefusalReason(xml);
        sw.Stop();

        Assert.Null(reason);
        Assert.True(sw.ElapsedMilliseconds < 1000, $"counting {xml.Length:N0} characters took {sw.ElapsedMilliseconds} ms.");
        Assert.Null(ShowPlanParser.Parse(xml).ParseError);
    }

    [Fact]
    public void ADoctype_IsRefused()
    {
        var xml = "<!DOCTYPE ShowPlanXML [<!ENTITY big \"xxxxxxxxxx\">]>" + Wrap("<BatchSequence a=\"&big;\"/>");

        var reason = CallerPlanXml.RefusalReason(xml);

        Assert.NotNull(reason);
        Assert.Contains("DOCTYPE", reason);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void NothingToCount_IsNotRefused(string? xml) => Assert.Null(CallerPlanXml.RefusalReason(xml));

    [Fact]
    public void EveryRealFixturePlan_IsNotRefused_AndStillParsesAndAnalyzes()
    {
        var files = FixtureFiles();
        Assert.NotEmpty(files);

        foreach (var file in files)
        {
            var xml = File.ReadAllText(file);
            Assert.True(CallerPlanXml.RefusalReason(xml) == null, $"{Path.GetFileName(file)} was refused.");
            Assert.Null(ShowPlanParser.Parse(xml).ParseError);

            using var doc = JsonDocument.Parse(DarlingMcpPlanTools.AnalyzePlanXml(xml));
            Assert.Equal("xml", doc.RootElement.GetProperty("source").GetString());
            Assert.True(doc.RootElement.GetProperty("statement_count").GetInt32() >= 1, $"{Path.GetFileName(file)} analyzed to no statements.");
        }
    }

    [Fact]
    public void AnalyzePlanXml_OverTheLimit_IsRefusedBeforeParsing()
    {
        var xml = ElementWithAttributes(1_000_000);
        var reason = CallerPlanXml.RefusalReason(xml);
        Assert.NotNull(reason);

        var sw = Stopwatch.StartNew();
        var result = DarlingMcpPlanTools.AnalyzePlanXml(xml);
        sw.Stop();

        Assert.Equal(McpHelpers.Refusal("plan_xml", reason!), result);
        Assert.Contains(CallerPlanXml.MaxAttributesPerElement.ToString("N0"), McpHelpers.ErrorMessageOf(result));
        Assert.True(sw.ElapsedMilliseconds < 1000, $"analyze_plan_xml took {sw.ElapsedMilliseconds} ms to refuse; parsing it takes about 27 s.");
    }

    [Fact]
    public void AnalyzePlanXml_AtTheLimit_IsNotRefused()
    {
        var result = DarlingMcpPlanTools.AnalyzePlanXml(ElementWithAttributes(CallerPlanXml.MaxAttributesPerElement));

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("xml", doc.RootElement.GetProperty("source").GetString());
    }

    [Fact]
    public void AnalyzePlanXml_Doctype_IsRefused()
    {
        var xml = "<!DOCTYPE ShowPlanXML [<!ENTITY big \"xxxxxxxxxx\">]>" + Wrap("<BatchSequence a=\"&big;\"/>");

        var result = DarlingMcpPlanTools.AnalyzePlanXml(xml);

        Assert.Equal(McpHelpers.Refusal("plan_xml", CallerPlanXml.RefusalReason(xml)!), result);
    }

    // ---- Where the gate is wired: source reads, the way this suite pins the viewer's WPF handlers ----

    private static string UiFile(string fileName, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.Ui", fileName)));

    private static string AnalysisFile(string fileName, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.PlanAnalysis", fileName)));

    private static int Count(string source, string token) =>
        (source.Length - source.Replace(token, "", StringComparison.Ordinal).Length) / token.Length;

    [Fact]
    public void EveryViewerReadOfCallerText_IsGated()
    {
        // Open, drop (two paths), the paste button and Ctrl+V in the controller: three file reads and two clipboard reads.
        var controller = UiFile("StandalonePlanViewerController.cs");
        var controllerReads = Count(controller, "File.ReadAllText(") + Count(controller, "ClipboardText.TryReadAsync(");
        Assert.Equal(5, controllerReads);
        Assert.Equal(controllerReads, Count(controller, "CallerPlanXmlPrompt.Refuse("));

        // Ctrl+V inside the plan viewer control.
        var interaction = UiFile("PlanViewerControl.Interaction.cs");
        Assert.Equal(1, Count(interaction, "ClipboardText.TryReadAsync("));
        Assert.Equal(1, Count(interaction, "CallerPlanXmlPrompt.Refuse("));
    }

    [Fact]
    public void StoredPlanPaths_AreNotGated()
    {
        // The collectors' plans reach the parser and the viewer through these, and they keep their own limits.
        Assert.DoesNotContain("CallerPlanXml", AnalysisFile("ShowPlanParser.cs"));
        Assert.DoesNotContain("CallerPlanXml", AnalysisFile("McpPlanAnalysisFormatter.cs"));
        Assert.DoesNotContain("CallerPlanXml", AnalysisFile("PlanAdvisoryAggregator.cs"));
        Assert.DoesNotContain("CallerPlanXml", AnalysisFile("PlanAnalysisPipeline.cs"));
        Assert.DoesNotContain("CallerPlanXml", UiFile("PlanViewerControl.xaml.cs"));

        // LoadPlanIntoSubTab is also how the Darling viewer opens a stored plan in a tab.
        var controller = UiFile("StandalonePlanViewerController.cs");
        var start = controller.IndexOf("public async Task LoadPlanIntoSubTab(", StringComparison.Ordinal);
        var end = controller.IndexOf("private string GetUniqueSubTabLabel(", StringComparison.Ordinal);
        Assert.InRange(start, 0, controller.Length);
        Assert.True(end > start, "the read of LoadPlanIntoSubTab is broken.");
        Assert.DoesNotContain("CallerPlanXml", controller.Substring(start, end - start));
    }
}
