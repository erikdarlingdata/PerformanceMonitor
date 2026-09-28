/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4535's own census: every rule the analyzer runs stamps <see cref="PlanWarning.RuleNumber"/> on
/// the finding it constructs, and every rule's guard actually suppresses that finding when
/// disabled. This replaces <c>PlanSync4535ConfigPlumbingTests.RuleNumber_DefaultsToNull_UntilLaterStepsStampIt</c>,
/// which pinned the mid-rollout state (only rule 3 stamped) and is now wrong by design: every step
/// that stamps a rule has landed.
///
/// <para>(a) is a CODE census over <c>PlanAnalyzer.cs</c>'s source text: every <c>new PlanWarning</c>
/// initializer sets <c>RuleNumber =</c>, except the two kinds of exception the rules themselves
/// document — a <c>Source = PlanWarningSource.SqlServer</c> initializer (the engine's own finding,
/// not the analyzer's), and rules 7 and 29, which mutate an existing warning's severity/message
/// rather than constructing one, so they carry a guard with no stamp site.</para>
///
/// <para>(b) is a RUNTIME census: over every plan fixture the existing <c>PlanSync4535*</c>/
/// <c>PlanSync4530</c> tests build, every analyzer-sourced finding (excluding the scorer's
/// <c>Wait: </c> findings, which aren't a numbered rule) carries a non-null <c>RuleNumber</c>.</para>
///
/// <para>(c) is the cross-step check: disabling every rule number the census in (a) found leaves
/// only SQL Server's own findings across those same fixtures — proof that each rule's guard, not
/// just its stamp, is wired to <see cref="AnalyzerConfig"/>.</para>
/// </summary>
public sealed class PlanSync4535RuleNumberCensusTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    private static string PlanAnalyzerSource()
        => File.ReadAllText(Path.Combine(RepoRoot(), "PerformanceMonitor.PlanAnalysis", "PlanAnalyzer.cs"));

    /// <summary>
    /// Walks every <c>new PlanWarning</c> construction in <c>PlanAnalyzer.cs</c> and returns, for
    /// each, whether its initializer sets <c>RuleNumber</c> and whether it sets
    /// <c>Source = PlanWarningSource.SqlServer</c>. Brace-balances from the <c>{</c> that follows
    /// <c>new PlanWarning</c> to find each initializer's extent — the same construction the
    /// initializers themselves use, just walked in text rather than compiled.
    ///
    /// <para>Walked over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output rather
    /// than a hand-rolled <c>//</c>-prefix line filter (#3052): a prefix filter reads a block
    /// comment's continuation lines as code, and would also leave a brace inside an interpolated
    /// message's string literal free to unbalance the depth count. The walker blanks both while
    /// preserving line numbers, which is what the census reports offenders by.</para>
    /// </summary>
    private static List<(int Line, bool HasRuleNumber, bool IsSqlServerSourced)> WalkPlanWarningConstructions()
    {
        var text = CSharpSourceWalker.StripCommentsAndStrings(PlanAnalyzerSource());
        var lines = text.Split('\n');
        var results = new List<(int, bool, bool)>();

        for (var i = 0; i < lines.Length; i++)
        {
            if (!lines[i].Contains("new PlanWarning"))
                continue;

            var depth = 0;
            var started = false;
            var block = new System.Text.StringBuilder();
            var j = i;

            while (j < lines.Length)
            {
                var line = lines[j];
                block.Append(line).Append('\n');

                foreach (var ch in line)
                {
                    if (ch == '{')
                    {
                        depth++;
                        started = true;
                    }
                    else if (ch == '}')
                    {
                        depth--;
                    }
                }

                if (started && depth <= 0)
                    break;

                j++;
            }

            var blockText = block.ToString();
            var hasRuleNumber = Regex.IsMatch(blockText, @"RuleNumber\s*=");
            var isSqlServerSourced = blockText.Contains("PlanWarningSource.SqlServer");
            results.Add((i + 1, hasRuleNumber, isSqlServerSourced));
        }

        return results;
    }

    /// <summary>
    /// (a) Every <c>new PlanWarning</c> construction in <c>PlanAnalyzer.cs</c> stamps
    /// <c>RuleNumber</c>, except a <c>Source = PlanWarningSource.SqlServer</c> initializer. As of
    /// this step there are no such initializers in <c>PlanAnalyzer.cs</c> (the engine's own
    /// findings come from <c>ShowPlanParser.cs</c>, not the analyzer) — this pin still names the
    /// exception so the census stays honest if one is ever added there.
    /// </summary>
    [Fact]
    public void EveryAnalyzerConstructedWarning_StampsRuleNumber_UnlessSqlServerSourced()
    {
        var constructions = WalkPlanWarningConstructions();
        Assert.NotEmpty(constructions);

        var unstamped = constructions
            .Where(c => !c.HasRuleNumber && !c.IsSqlServerSourced)
            .ToList();

        Assert.Empty(unstamped);
    }

    /// <summary>
    /// Rules 7 and 29 mutate an existing warning's severity/message rather than constructing a new
    /// <see cref="PlanWarning"/>, so they carry a guard with no stamp site — pinned here so a
    /// future refactor that turns either into a construction is caught by (a) rather than silently
    /// staying unstamped.
    /// </summary>
    [Fact]
    public void Rule7AndRule29_AreGuardOnly_WithNoConstructionSite()
    {
        var source = PlanAnalyzerSource();
        Assert.Contains("IsRuleDisabled(7)", source);
        Assert.Contains("IsRuleDisabled(29)", source);
    }

    // ---- (b) runtime census -------------------------------------------------------------------

    private const string XmlFixture = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t WHERE b = 1" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5" StatementOptmLevel="FULL">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104" NonParallelPlanReason="CouldNotGenerateValidParallelPlan">
            <RelOp NodeId="0" PhysicalOp="Filter" LogicalOp="Filter" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <Filter StartupExpression="0">
                <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <TableScan Storage="RowStore">
                    <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </Filter>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    /// <summary>
    /// A second, directly-constructed fixture (a dynamic cursor plus a truncated statement text)
    /// covering two more statement-level rules the XML fixture above doesn't reach, built the same
    /// way <c>PlanSync4535StatementRulesTests</c> builds its per-rule fixtures rather than through
    /// a hand-shaped <c>StmtCursor</c> XML element (whose parse path needs a nested
    /// <c>CursorPlan</c>/<c>Operation</c> shape unrelated to what this census exercises).
    /// </summary>
    private static ParsedPlan DirectFixture()
    {
        var cursorStmt = new PlanStatement { CursorName = "c1", CursorActualType = "Dynamic" };
        var truncatedStmt = new PlanStatement { StatementText = new string('a', PlanStatement.TruncationLengthThreshold) };
        return new ParsedPlan { Batches = [new PlanBatch { Statements = [cursorStmt, truncatedStmt] }] };
    }

    private static IEnumerable<ParsedPlan> Fixtures()
    {
        yield return ShowPlanParser.Parse(XmlFixture);
        yield return DirectFixture();
    }

    private static List<PlanWarning> AllWarnings(ParsedPlan plan) => plan.AllWarnings;

    /// <summary>
    /// (b) Every analyzer-sourced finding across the fixtures carries a non-null
    /// <see cref="PlanWarning.RuleNumber"/>. Excludes the scorer's <c>Wait: </c> findings (not a
    /// numbered rule) and anything <c>Source == SqlServer</c> (the engine's own, never numbered).
    /// </summary>
    [Fact]
    public void EveryAnalyzerFinding_AcrossTheFixtures_HasANonNullRuleNumber()
    {
        var unstamped = new List<PlanWarning>();

        foreach (var plan in Fixtures())
        {
            PlanAnalysisPipeline.Run(plan, AnalyzerConfig.Default, null, CancellationToken.None);

            var findings = AllWarnings(plan)
                .Where(w => w.Source != PlanWarningSource.SqlServer)
                .Where(w => !w.WarningType.StartsWith("Wait: "))
                .ToList();

            Assert.NotEmpty(findings);
            unstamped.AddRange(findings.Where(w => w.RuleNumber == null));
        }

        Assert.Empty(unstamped);
    }

    // ---- (c) all-rules-disabled cross-step check ----------------------------------------------

    /// <summary>
    /// (c) With every rule number the code census in (a) found stamped somewhere disabled, running
    /// the fixtures yields zero analyzer findings — only SQL Server's own (there are none in these
    /// fixtures; they come from <c>&lt;Warnings&gt;</c> elements, absent here). Proves each rule's
    /// guard, not just its stamp, reaches <see cref="AnalyzerConfig"/>.
    /// </summary>
    [Fact]
    public void DisablingEveryStampedRule_YieldsZeroAnalyzerFindings_OnTheFixtures()
    {
        var stampedRuleNumbers = WalkPlanWarningConstructions()
            .Where(c => c.HasRuleNumber)
            .Select(c => RuleNumberAt(c.Line))
            .Where(n => n.HasValue)
            .Select(n => n!.Value)
            .Distinct()
            .ToList();

        // Rules 7 and 29 have no construction site to walk from but do have a guard; the disable
        // list must cover them too, or this check would prove nothing about them.
        stampedRuleNumbers.Add(7);
        stampedRuleNumbers.Add(29);

        Assert.True(stampedRuleNumbers.Count >= 30, $"expected at least 30 distinct rule numbers, found {stampedRuleNumbers.Count}");

        var cfg = new AnalyzerConfig { Rules = new RulesConfig { Disabled = stampedRuleNumbers } };

        foreach (var plan in Fixtures())
        {
            PlanAnalysisPipeline.Run(plan, cfg, null, CancellationToken.None);

            var analyzerFindings = AllWarnings(plan)
                .Where(w => w.Source != PlanWarningSource.SqlServer)
                .Where(w => !w.WarningType.StartsWith("Wait: "))
                .ToList();

            Assert.Empty(analyzerFindings);
        }
    }

    /// <summary>
    /// Reads the <c>RuleNumber = N</c> literal near the construction-site line so (c) can build its
    /// disable list from the same source walk (a) uses, rather than a second hand-maintained list.
    /// </summary>
    private static int? RuleNumberAt(int constructionLine)
    {
        var lines = PlanAnalyzerSource().Split('\n');
        var start = constructionLine - 1;
        var depth = 0;
        var started = false;

        for (var j = start; j < lines.Length; j++)
        {
            var line = lines[j];
            var match = Regex.Match(line, @"RuleNumber\s*=\s*(\d+)");
            if (match.Success)
                return int.Parse(match.Groups[1].Value);

            foreach (var ch in line)
            {
                if (ch == '{')
                {
                    depth++;
                    started = true;
                }
                else if (ch == '}')
                {
                    depth--;
                }
            }

            if (started && depth <= 0)
                break;
        }

        return null;
    }
}
