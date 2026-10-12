/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's side of the caller-supplied plan XML limits (the count itself is pinned in Darling.Tests'
/// <c>CallerPlanXmlTests</c>): Lite's <c>analyze_plan_xml</c> refuses an element with too many attributes
/// before it parses, still analyzes a plan just under the limit, and Lite's Ctrl+V on the server tab checks
/// pasted text before it opens a plan tab.
/// </summary>
[Collection("timing")]
public sealed class CallerPlanXmlLiteTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private const string SmallPlan =
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"0.01\" StatementEstRows=\"1\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"100\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"1E-07\" " +
        "AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"1E-07\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList /><ConstantScan /></RelOp></QueryPlan></StmtSimple>";

    private static string PlanWithWideElement(int attributes)
    {
        var sb = new StringBuilder(attributes * 12 + 1024);
        sb.Append("<ShowPlanXML xmlns=\"").Append(Ns).Append("\" Version=\"1.564\" Build=\"16.0.1000.6\">");
        sb.Append("<BatchSequence><Batch><Statements>").Append(SmallPlan).Append("</Statements></Batch></BatchSequence>");
        sb.Append("<Extra ");
        for (var i = 0; i < attributes; i++) sb.Append('a').Append(i).Append("=\"1\" ");
        sb.Append("/></ShowPlanXML>");
        return sb.ToString();
    }

    [Fact]
    public void AnalyzePlanXml_OverTheLimit_IsRefusedBeforeParsing()
    {
        var xml = PlanWithWideElement(1_000_000);
        var reason = CallerPlanXml.RefusalReason(xml);
        Assert.NotNull(reason);

        var sw = Stopwatch.StartNew();
        var result = McpPlanTools.AnalyzePlanXml(xml);
        sw.Stop();

        Assert.Equal(McpHelpers.Refusal("plan_xml", reason!), result);
        Assert.Contains(CallerPlanXml.MaxAttributesPerElement.ToString("N0"), McpHelpers.ErrorMessageOf(result));
        Assert.True(sw.ElapsedMilliseconds < 1000, $"analyze_plan_xml took {sw.ElapsedMilliseconds} ms to refuse; parsing it takes about 27 s.");
    }

    [Fact]
    public void AnalyzePlanXml_AtTheLimit_StillAnalyzes()
    {
        var xml = PlanWithWideElement(CallerPlanXml.MaxAttributesPerElement);
        Assert.Null(CallerPlanXml.RefusalReason(xml));

        using var doc = JsonDocument.Parse(McpPlanTools.AnalyzePlanXml(xml));

        Assert.Equal("xml", doc.RootElement.GetProperty("source").GetString());
        Assert.True(doc.RootElement.GetProperty("statement_count").GetInt32() >= 1);
    }

    [Fact]
    public void ServerTabPaste_ChecksThePastedText_BeforeOpeningAPlanTab()
    {
        var source = File.ReadAllText(ServerTabCs());
        var read = source.IndexOf("ClipboardText.TryReadAsync(", StringComparison.Ordinal);
        var gate = source.IndexOf("CallerPlanXmlPrompt.Refuse(", StringComparison.Ordinal);
        var open = source.IndexOf("await OpenPlanTab(xml, \"Pasted Plan\")", StringComparison.Ordinal);

        Assert.True(read >= 0 && gate > read && open > gate,
            "ServerTab_KeyDown must read the clipboard, check the text with CallerPlanXmlPrompt.Refuse, then open the plan tab.");
        Assert.Equal(1, (source.Length - source.Replace("ClipboardText.TryReadAsync(", "", StringComparison.Ordinal).Length) / "ClipboardText.TryReadAsync(".Length);
    }

    private static string ServerTabCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", "ServerTab.xaml.cs"));
}
