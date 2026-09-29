/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4687 (erikdarlingdata/PerformanceStudio#608) - Rule 33 names the default guess an estimate matches.
/// The labels used to be wrong: 30% was called the equality guess (it is the inequality guess), and 10%
/// and 1% were called inequality guesses that no predicate produces. The numbers below were measured on
/// SQL Server 2025 against a 100,000-row heap with no statistics, using estimated plans
/// (SET SHOWPLAN_XML), CE 70 through FORCE_LEGACY_CARDINALITY_ESTIMATION and CE 120 to 170 through the
/// compatibility level.
///
/// Each row is what the optimizer wrote for EstimateRows on the scan, so every test feeds the rule a
/// value the engine produced, not one worked out by hand. The code-built plans follow the pattern in
/// <c>PlanSync4535NodeRulesATests</c>.
/// </summary>
public sealed class PlanSync4687CeGuessDetectionTests
{
    private const double HundredThousand = 100_000;

    /// <summary>
    /// Runs the whole analyzer over one estimated table scan and returns the text of its
    /// "Estimated Plan CE Guess" warning, or null when the rule said nothing.
    /// </summary>
    private static string? Label(int ceModelVersion, double estimateRows, double tableCardinality = HundredThousand)
    {
        var node = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            EstimateRows = estimateRows,
            TableCardinality = tableCardinality,
            EstimatedTotalSubtreeCost = 1.0,
            Predicate = "[t].[a]>(5)"
        };
        var stmt = new PlanStatement
        {
            RootNode = node,
            StatementSubTreeCost = 1.0,
            CardinalityEstimationModelVersion = ceModelVersion
        };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);

        return node.Warnings.FirstOrDefault(w => w.WarningType == "Estimated Plan CE Guess")?.Message;
    }

    // ---- what each measured guess is called ----------------------------------------------------

    [Theory]
    // a > 5, a < 5, a >= 5: 30% in every estimator
    [InlineData(170, 30000, "the 30% guess for an inequality such as > or <")]
    [InlineData(70, 30000, "the 30% guess for an inequality such as > or <")]
    [InlineData(0, 30000, "the 30% guess for an inequality such as > or <")]
    // a BETWEEN 5 AND 10, a >= 5 AND a <= 10, and LIKE (CE 120+): 9%
    [InlineData(170, 9000, "the 9% guess for BETWEEN or a two-sided range on a column with known values, or for LIKE (9.0%)")]
    [InlineData(120, 9000, "the 9% guess for BETWEEN or a two-sided range on a column with known values, or for LIKE (9.0%)")]
    // a > 5 AND b > 5 and BETWEEN on variables under CE 70: 30% * 30% = 9%
    [InlineData(70, 9000, "the 9% guess for BETWEEN, a two-sided range, or two inequalities on different columns (9.0%)")]
    // no estimator named: only what both estimators agree on
    [InlineData(0, 9000, "the 9% guess for BETWEEN or a two-sided range (9.0%)")]
    // a > 5 AND b > 5, and BETWEEN on variables, under CE 120+: 30% * sqrt(30%) = 16.43%
    [InlineData(170, 16431.7, "the 16.4% guess for two inequalities on different columns, or for a BETWEEN or range on variables or on an expression")]
    [InlineData(120, 16431.7, "the 16.4% guess for two inequalities on different columns")]
    [InlineData(0, 16431.7, "the 16.4% guess for two inequalities on different columns")]
    // a = b, and from CE 130 an equality on an expression such as ABS(a) = 5: 10%
    [InlineData(170, 10000, "the 10% guess for comparing one column with another, or for an equality on an expression such as a function of a column")]
    [InlineData(130, 10000, "the 10% guess for comparing one column with another, or for an equality on an expression")]
    [InlineData(0, 10000, "the 10% guess for comparing one column with another, or for an equality on an expression")]
    [InlineData(120, 10000, "the 10% guess for comparing one column with another (10.0%)")]
    [InlineData(70, 10000, "the 10% guess for comparing one column with another (10.0%)")]
    // a = b AND c = d under CE 70: 10% * 10% = 1%
    [InlineData(70, 1000, "the 1% guess that CE 70 gets from multiplying two 10% guesses")]
    [InlineData(0, 1000, "the 1% guess that CE 70 gets from multiplying two 10% guesses")]
    // a = 5 and a IS NULL with no statistics: CE 120+ rows^0.5 (316.228), CE 70 rows^0.75 (5623.41)
    [InlineData(170, 316.228, "the square root of the row count (0.3%)")]
    [InlineData(120, 316.228, "the square root of the row count (0.3%)")]
    [InlineData(0, 316.228, "the square root of the row count (0.3%)")]
    [InlineData(70, 5623.41, "the row count to the power 0.75 (5.6%)")]
    [InlineData(0, 5623.41, "the row count to the power 0.75 (5.6%)")]
    public void MeasuredGuess_IsNamedForWhatProducedIt(int ceModelVersion, double estimateRows, string expected)
    {
        var label = Label(ceModelVersion, estimateRows);

        Assert.NotNull(label);
        Assert.Contains(expected, label);
    }

    [Fact]
    public void ThirtyPercent_IsTheInequalityGuess_NotTheEqualityGuess()
    {
        var label = Label(170, 30000);

        Assert.NotNull(label);
        Assert.DoesNotContain("30% equality", label);
        Assert.Contains("inequality", label);
    }

    [Fact]
    public void EqualityGuess_MessageSaysItIsAnEqualityGuess()
    {
        var label = Label(170, 316.228);

        Assert.NotNull(label);
        Assert.Contains("equality guess", label);
    }

    // ---- estimates that are not a guess in that estimator ----------------------------------------

    [Theory]
    [InlineData(70, 16431.7)]   // CE 70 has no 16.43% guess: it gets 9% for the same predicate
    [InlineData(170, 1000)]     // CE 120+ has no 1% guess: a = b AND c = d is 3.16% there
    [InlineData(120, 1000)]
    [InlineData(170, 5623.41)]  // rows^0.75 is CE 70's equality guess only
    [InlineData(70, 316.228)]   // the square root is CE 120+'s equality guess only
    [InlineData(170, 25000)]    // ordinary estimates
    [InlineData(170, 50000)]
    [InlineData(70, 12345)]
    [InlineData(170, 322.6)]    // 2% above the equality guess, outside its 1% tolerance
    [InlineData(170, 309.9)]    // 2% below
    public void NotAGuess_GetsNoLabel(int ceModelVersion, double estimateRows)
    {
        Assert.Null(Label(ceModelVersion, estimateRows));
    }

    [Fact]
    public void EqualityGuess_ToleratesHalfAPercent()
    {
        Assert.NotNull(Label(170, 316.228 * 1.005));
        Assert.NotNull(Label(170, 316.228 * 0.995));
    }

    // ---- the equality guess follows the table's row count ----------------------------------------

    [Theory]
    [InlineData(400_000, 170, 632.456)]        // measured: sqrt(400,000)
    [InlineData(400_000, 70, 15905.4)]         // measured: 400,000^0.75
    [InlineData(1_000_000, 170, 1000)]
    [InlineData(1_000_000, 70, 31622.8)]
    // 100,000,000^0.75 is 1,000,000, exactly 1% of the table. It is still the equality guess.
    [InlineData(100_000_000, 70, 1_000_000)]
    public void EqualityGuess_ScalesWithTheTable(double tableRows, int ceModelVersion, double estimateRows)
    {
        var label = Label(ceModelVersion, estimateRows, tableRows);

        Assert.NotNull(label);
        Assert.Contains("equality guess", label);
        Assert.DoesNotContain("1% guess", label);
    }

    [Fact]
    public void EqualityGuess_IsNotLookedForBelowTheTableSizeFloor()
    {
        // The rule only looks at tables of 100,000 rows or more. The same estimate on a table just
        // under the floor gets no label: there the equality guess could sit on a fixed guess.
        Assert.Null(Label(170, System.Math.Sqrt(99_999), 99_999));
    }

    // ---- the estimator version reaches the rule from the plan XML --------------------------------

    private const string PlanTemplate = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.599" Build="17.0.4045.5"><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT * FROM dbo.t WHERE {SQL}" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="0.745875" StatementEstRows="{ROWS}" StatementOptmLevel="TRIVIAL" CardinalityEstimationModelVersion="{CE}"><QueryPlan CachedPlanSize="24" CompileTime="0" CompileCPU="0" CompileMemory="144"><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="{ROWS}" EstimatedRowsRead="100000" EstimateIO="0.635796" EstimateCPU="0.110078" AvgRowSize="56" EstimatedTotalSubtreeCost="0.745875" TableCardinality="100000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row"><OutputList><ColumnReference Database="[probe]" Schema="[dbo]" Table="[t]" Column="a" /></OutputList><TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore"><DefinedValues><DefinedValue><ColumnReference Database="[probe]" Schema="[dbo]" Table="[t]" Column="a" /></DefinedValue></DefinedValues><Object Database="[probe]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore" /><Predicate><ScalarOperator ScalarString="{PREDICATE}" /></Predicate></TableScan></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static IEnumerable<PlanNode> Nodes(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var descendant in Nodes(child))
                yield return descendant;
    }

    /// <summary>
    /// The shape of a real estimated plan for one of the probe queries, with the estimate, the
    /// estimator version and the predicate filled in from what SQL Server 2025 produced.
    /// </summary>
    private static string? LabelFromXml(string sql, string predicate, int ce, string estimateRows)
    {
        var xml = PlanTemplate
            .Replace("{SQL}", sql)
            .Replace("{PREDICATE}", predicate)
            .Replace("{CE}", ce.ToString())
            .Replace("{ROWS}", estimateRows);

        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);

        return PlanStatements.EnumerateAll(plan)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Nodes(s.RootNode!))
            .SelectMany(n => n.Warnings)
            .FirstOrDefault(w => w.WarningType == "Estimated Plan CE Guess")?.Message;
    }

    private const string TwoInequalitiesSql = "a &gt; 5 AND b &gt; 5";
    private const string TwoInequalitiesPredicate = "[t].[a]&gt;(5) AND [t].[b]&gt;(5)";

    [Fact]
    public void TwoInequalitiesOnDifferentColumns_Ce170_IsSixteenPointFourPercent()
    {
        // WHERE a > 5 AND b > 5 under CE 170: EstimateRows="16431.7"
        var label = LabelFromXml(TwoInequalitiesSql, TwoInequalitiesPredicate, 170, "16431.7");

        Assert.NotNull(label);
        Assert.Contains("the 16.4% guess for two inequalities on different columns", label);
    }

    [Fact]
    public void TwoInequalitiesOnDifferentColumns_Ce70_IsNinePercent()
    {
        // The same query under CE 70: EstimateRows="9000", and the label says why.
        var label = LabelFromXml(TwoInequalitiesSql, TwoInequalitiesPredicate, 70, "9000");

        Assert.NotNull(label);
        Assert.Contains("the 9% guess for BETWEEN, a two-sided range, or two inequalities on different columns", label);
    }

    [Fact]
    public void Equality_Ce170_IsTheSquareRootOfTheRowCount()
    {
        // WHERE a = 5 under CE 170: EstimateRows="316.228"
        var label = LabelFromXml("a = 5", "[t].[a]=(5)", 170, "316.228");

        Assert.NotNull(label);
        Assert.Contains("the square root of the row count", label);
    }

    [Fact]
    public void Equality_Ce70_IsTheRowCountToTheThreeQuarters()
    {
        // WHERE a = 5 under CE 70: EstimateRows="5623.41"
        var label = LabelFromXml("a = 5", "[t].[a]=(5)", 70, "5623.41");

        Assert.NotNull(label);
        Assert.Contains("the row count to the power 0.75", label);
    }
}
