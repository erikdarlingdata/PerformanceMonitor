/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8387: the plan viewer's parameter card annotations, the sniffed-runtime tooltip, the warning header's benefit
/// suffix, a wait row's category tooltip and the origin link's "also from" tooltip read with plain punctuation, by
/// #7536's rule: a single dash becomes a colon, or a comma when the line already holds a colon. The two
/// name-and-figure separators keep their dash, as PerformanceStudio's viewer prints them: the "Wait Stats — N ms
/// total" header and the missing index row's " — Impact: " lead.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class PlanSync8387PlainPunctuationTests
{
    private const string EmDash = "—";

    private static readonly IReadOnlyList<PlanParameter> NoParameters = new List<PlanParameter>();

    private static IReadOnlyList<PlanParameter> OneParameter(string? compiled, string? runtime) =>
        new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = compiled, RuntimeValue = runtime },
        };

    [Fact]
    public void LocalVariables_NoParameters_ReadsWithAColon()
    {
        var text = "SELECT * FROM t WHERE x = @rep";

        var annotation = Assert.Single(ParameterCard.Annotations(NoParameters, text, text, new[] { "@rep" }));

        Assert.Equal("Local variables detected (@rep): values not captured in plan XML", annotation.Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotation.Tone);
    }

    [Fact]
    public void OptimizeForUnknown_ReadsWithAColon()
    {
        var text = "SELECT * FROM t WHERE x = @p1 OPTION (OPTIMIZE FOR UNKNOWN)";

        var annotation = Assert.Single(ParameterCard.Annotations(OneParameter(null, "5"), text, text, Array.Empty<string>()));

        Assert.Equal("OPTIMIZE FOR UNKNOWN: optimizer used average density estimates instead of sniffed values", annotation.Text);
        Assert.Equal(ParameterCardAnnotationTone.Accent, annotation.Tone);
    }

    [Fact]
    public void Recompile_ReadsWithAColon()
    {
        var text = "SELECT * FROM t WHERE x = @p1 OPTION (RECOMPILE)";

        var annotation = Assert.Single(ParameterCard.Annotations(OneParameter(null, "5"), text, text, Array.Empty<string>()));

        Assert.Equal("OPTION(RECOMPILE): parameter values embedded as literals, not sniffed", annotation.Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotation.Tone);
    }

    [Fact]
    public void ValuesNotCaptured_ReadsWithAColon()
    {
        var text = "SELECT * FROM t WHERE x = @p1";

        var annotation = Assert.Single(ParameterCard.Annotations(OneParameter(null, "5"), text, text, Array.Empty<string>()));

        Assert.Equal("Parameter values not captured in the plan: no compiled values, so the values the optimizer sniffed are not known", annotation.Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotation.Tone);
    }

    [Fact]
    public void UnresolvedVariables_ReadsWithAComma_SinceTheLineAlreadyHoldsAColon()
    {
        var text = "SELECT * FROM t WHERE x = @p1 AND y = @extra";

        var annotation = Assert.Single(ParameterCard.Annotations(OneParameter("1", "1"), text, text, new[] { "@extra" }));

        Assert.Equal("Unresolved variables: @extra, not in parameter list", annotation.Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotation.Tone);
    }

    [Fact]
    public void EveryAnnotation_HoldsNoEmDash()
    {
        var cases = new (IReadOnlyList<PlanParameter> Parameters, string Text, IReadOnlyCollection<string> Unresolved)[]
        {
            (NoParameters, "SELECT * FROM t WHERE x = @rep", new[] { "@rep" }),
            (OneParameter(null, "5"), "SELECT * FROM t WHERE x = @p1 OPTION (OPTIMIZE FOR UNKNOWN)", Array.Empty<string>()),
            (OneParameter(null, "5"), "SELECT * FROM t WHERE x = @p1 OPTION (RECOMPILE)", Array.Empty<string>()),
            (OneParameter(null, "5"), "SELECT * FROM t WHERE x = @p1", Array.Empty<string>()),
            (OneParameter(null, "5"), "SELECT * FROM t WHERE x = @p1 AND y = @extra", new[] { "@extra" }),
        };

        var offenders = cases
            .SelectMany(c => ParameterCard.Annotations(c.Parameters, c.Text, c.Text, c.Unresolved))
            .Select(a => a.Text)
            .Where(t => t.Contains(EmDash, StringComparison.Ordinal))
            .ToList();

        Assert.Empty(offenders);
    }

    [Fact]
    public void OriginNavigation_SeveralOrigins_ReadsWithAComma()
    {
        var nav = PlanWarningDisplay.OriginNavigationText(new List<int> { 7, 9, 12 });

        Assert.NotNull(nav);
        Assert.Equal("Go to Node 7, also from Node 9, Node 12", nav!.Value.Tooltip);
        Assert.DoesNotContain(EmDash, nav.Value.Tooltip, StringComparison.Ordinal);
    }

    [Fact]
    public void WarningHeader_BenefitSuffix_ReadsWithAColon()
    {
        var scored = new PlanWarning { WarningType = "Key Lookup", MaxBenefitPercent = 40.0 };
        var legacyFromTheEngine = new PlanWarning
        {
            WarningType = "Local Variables",
            MaxBenefitPercent = 125.0,
            IsLegacy = true,
            Source = PlanWarningSource.SqlServer
        };

        Assert.Equal("⚠ Key Lookup: up to 40.0% benefit", PlanWarningDisplay.PlanWarningHeader(scored));
        Assert.Equal("⚠ Local Variables [SQL Server] [legacy]: up to 125% benefit", PlanWarningDisplay.PlanWarningHeader(legacyFromTheEngine));
        Assert.Equal("⚠ Key Lookup", PlanWarningDisplay.PlanWarningHeader(new PlanWarning { WarningType = "Key Lookup" }));
    }

    /// <summary>
    /// The rendered insight cards: a runtime value that differs from the compiled one carries the sniffing tooltip, a
    /// variable the parameter list does not hold is named below the Parameters table, and a wait row's tooltip names its
    /// category, each with plain punctuation; the Wait Stats header and the missing index row's Impact lead keep their
    /// name-and-figure dash.
    /// </summary>
    [Fact]
    public void RenderedInsightCards_ClausesReadWithPlainPunctuation_AndTheTwoSeparatorsKeepTheirDash()
    {
        StaTestThread.Run(async () =>
        {
            // The node visuals read the theme's brushes, so the Viewer's theme is merged for the body and removed after it.
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("BackgroundBrush"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }

            StaTestThread.Step("new PlanViewerControl()");
            var control = new PlanViewerControl();
            try
            {
                StaTestThread.Step("LoadPlan: one sniffed parameter and one variable outside the parameter list");
                await control.LoadPlan(SniffedParameterPlan(), "plan");

                StaTestThread.Step("reading the Parameters card");
                var content = (StackPanel)control.FindName("ParametersContent");
                var texts = TextBlocks(content).ToList();
                Assert.NotEmpty(texts);

                var tooltips = texts.Select(t => t.ToolTip as string).Where(t => t != null).Select(t => t!).ToList();
                // #8455: a sniffed cell's tooltip leads with its value, so the note is one line of it.
                Assert.Contains(tooltips, t => t.Contains("Runtime value differs from compiled: possible parameter sniffing", StringComparison.Ordinal));

                var lines = texts.Select(t => t.Text).ToList();
                Assert.Contains("Unresolved variables: @extra, not in parameter list", lines);

                var offenders = lines.Concat(tooltips).Where(t => t.Contains(EmDash, StringComparison.Ordinal)).ToList();
                Assert.Empty(offenders);

                StaTestThread.Step("reading the Wait Stats card");
                var waitTooltips = TextBlocks((DependencyObject)control.FindName("WaitStatsContent"))
                    .Select(t => t.ToolTip as string).Where(t => t != null).Select(t => t!).ToList();
                Assert.Contains($"CXPACKET: {ChartPalette.WaitCategory("CXPACKET")} wait", waitTooltips);
                Assert.DoesNotContain(waitTooltips, t => t.Contains(EmDash, StringComparison.Ordinal));

                // The two name-and-figure separators keep their dash.
                Assert.Equal("  Wait Stats — 300ms total", ((TextBlock)control.FindName("WaitStatsHeader")).Text);
                var impactLeads = TextBlocks((DependencyObject)control.FindName("MissingIndexContent")).Select(t => t.Text).ToList();
                Assert.Contains(" — Impact: ", impactLeads);
            }
            finally
            {
                StaTestThread.Step("Cleanup()");
                control.Cleanup();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
    }

    private static IEnumerable<TextBlock> TextBlocks(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is TextBlock tb)
                yield return tb;
            foreach (var inner in TextBlocks(child))
                yield return inner;
        }
    }

    /// <summary>A single-statement actual plan whose one parameter was compiled as 1 and ran as 2, whose text names a
    /// second variable the parameter list does not carry, with one missing index suggestion and one wait.</summary>
    private static string SniffedParameterPlan() =>
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT * FROM dbo.t WHERE id = @id AND other = @extra\" StatementId=\"1\" StatementType=\"SELECT\" " +
        "StatementSubTreeCost=\"1\" StatementEstRows=\"10\" StatementOptmLevel=\"FULL\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" DegreeOfParallelism=\"1\">" +
        "<MissingIndexes><MissingIndexGroup Impact=\"45.5\"><MissingIndex Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\">" +
        "<ColumnGroup Usage=\"EQUALITY\"><Column Name=\"[other]\" ColumnId=\"2\" /></ColumnGroup></MissingIndex></MissingIndexGroup></MissingIndexes>" +
        "<WaitStats><Wait WaitType=\"CXPACKET\" WaitTimeMs=\"300\" WaitCount=\"4\" /></WaitStats>" +
        "<QueryTimeStats CpuTime=\"10\" ElapsedTime=\"10\" />" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Seek\" LogicalOp=\"Clustered Index Seek\" " +
        "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList />" +
        "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"10\" ActualElapsedms=\"10\" ActualCPUms=\"10\" ActualLogicalReads=\"10\" /></RunTimeInformation>" +
        "<IndexScan Ordered=\"1\" ForcedIndex=\"0\" ForceSeek=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
        "</IndexScan>" +
        "</RelOp>" +
        "<ParameterList><ColumnReference Column=\"@id\" ParameterDataType=\"int\" ParameterCompiledValue=\"(1)\" ParameterRuntimeValue=\"(2)\" /></ParameterList>" +
        "</QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";
}
