/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4524 — five rules (3, 20, 26, 27, 28) search <see cref="PlanStatement.StatementText"/> for a
/// hint or a keyword, but they searched the raw text, so the same word inside a comment or a
/// string literal counted as a real hint. A <c>-- OPTION (RECOMPILE)</c> comment stopped rule 20
/// from warning about local variables it should have flagged; <c>NOT IN</c> inside a string
/// literal made rule 28 warn about a pattern that isn't in the query at all.
///
/// <para>Mirrors erikdarlingdata/PerformanceStudio@cc18844's <c>MaskCommentsAndLiterals</c> and its
/// own test cases (translated to this project's plan model).</para>
/// </summary>
public sealed class PlanSync4524Tests
{
    private static ParsedPlan Analyze(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static bool Has(PlanStatement stmt, string warningType) =>
        stmt.PlanWarnings.Any(w => w.WarningType == warningType);

    private static bool Has(PlanNode node, string warningType) =>
        node.Warnings.Any(w => w.WarningType == warningType);

    // ---- (a) the helper's own scan behavior, exercised through rule 27 ----------------------

    [Theory]
    [InlineData("SELECT a FROM dbo.t -- OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("SELECT a FROM dbo.t /* OPTION (OPTIMIZE FOR UNKNOWN) */")]
    [InlineData("SELECT a FROM dbo.t /* outer /* inner */ OPTIMIZE FOR UNKNOWN */")]
    [InlineData("SELECT 'OPTIMIZE FOR UNKNOWN' FROM dbo.t")]
    [InlineData("SELECT N'it''s OPTIMIZE FOR UNKNOWN' FROM dbo.t")]
    public void Rule27_HintTextInCommentOrLiteral_DoesNotWarn(string text)
    {
        var stmt = new PlanStatement { StatementText = text };
        Analyze(stmt);

        Assert.False(Has(stmt, "Optimize For Unknown"));
    }

    [Fact]
    public void Rule27_HintAfterAnUnterminatedComment_StillMasksToEndOfText()
    {
        // An unterminated /* comment blanks everything after it, so a real hint later in the
        // text does not fire — the same as PS's handling: nothing after an open block comment
        // is code, because there's no closing */ to end it.
        var stmt = new PlanStatement
        {
            StatementText = "SELECT a FROM dbo.t /* unterminated OPTIMIZE FOR UNKNOWN"
        };
        Analyze(stmt);

        Assert.False(Has(stmt, "Optimize For Unknown"));
    }

    [Theory]
    [InlineData("SELECT a FROM dbo.t WHERE b = @b OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("SELECT '--' AS x FROM dbo.t OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("SELECT [it's] FROM dbo.t OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("SELECT a FROM dbo.t /* note */ OPTION (OPTIMIZE FOR UNKNOWN)")]
    public void Rule27_RealHint_StillWarns(string text)
    {
        // A dash pair inside a string, a quote inside a bracketed identifier, and a closed
        // comment before the hint must not hide the hint that follows them.
        var stmt = new PlanStatement { StatementText = text };
        Analyze(stmt);

        Assert.True(Has(stmt, "Optimize For Unknown"));
    }

    // ---- (b) rule 20: RECOMPILE in a comment does not suppress the local-variable warning ----

    [Theory]
    [InlineData("SELECT a FROM dbo.t WHERE b = @b -- OPTION (RECOMPILE)", true)]
    [InlineData("SELECT a FROM dbo.t WHERE b = @b /* OPTION (RECOMPILE) */", true)]
    [InlineData("SELECT a FROM dbo.t WHERE b = @b OPTION (RECOMPILE)", false)]
    public void Rule20_RecompileMentionedOnlyInAComment_StillWarnsAboutLocalVariables(string text, bool expectWarning)
    {
        var stmt = new PlanStatement
        {
            StatementText = text,
            StatementSubTreeCost = 10,
            Parameters = [new PlanParameter { Name = "@b" }]
        };
        Analyze(stmt);

        Assert.Equal(expectWarning, Has(stmt, "Local Variables"));
    }

    // ---- (c) rule 28: NOT IN inside a string literal is not a real anti-join pattern ---------

    private static (PlanStatement Stmt, PlanNode Spool) NotInSpoolStatement(string statementText)
    {
        var spool = new PlanNode
        {
            PhysicalOp = "Row Count Spool",
            LogicalOp = "Lazy Spool",
            HasActualStats = true,
            ActualRewinds = 20000
        };
        var antiSemiJoin = new PlanNode
        {
            PhysicalOp = "Nested Loops",
            LogicalOp = "Left Anti Semi Join",
            Predicate = "[dbo].[u].[c] IS NULL",
            Children = { spool }
        };
        spool.Parent = antiSemiJoin;
        var stmt = new PlanStatement { StatementText = statementText, RootNode = antiSemiJoin };
        return (stmt, spool);
    }

    [Fact]
    public void Rule28_NotInInsideAStringLiteral_DoesNotWarn()
    {
        var (stmt, spool) = NotInSpoolStatement(
            "SELECT a FROM dbo.t WHERE b = 'NOT IN the mood' AND EXISTS (SELECT 1 FROM dbo.u)");
        Analyze(stmt);

        Assert.False(Has(spool, "NOT IN with Nullable Column"));
    }

    [Fact]
    public void Rule28_RealNotIn_StillWarns()
    {
        var (stmt, spool) = NotInSpoolStatement(
            "SELECT a FROM dbo.t WHERE b NOT IN (SELECT c FROM dbo.u)");
        Analyze(stmt);

        Assert.True(Has(spool, "NOT IN with Nullable Column"));
    }

    // ---- rule 3: MAXDOP 1 in a comment is not a query-level hint -----------------------------

    [Theory]
    [InlineData("SELECT a FROM dbo.t -- OPTION (MAXDOP 1)", false)]
    [InlineData("SELECT a FROM dbo.t OPTION (MAXDOP 1)", true)]
    public void Rule03_Maxdop1MentionedOnlyInAComment_IsNotAQueryHint(string text, bool expectWarning)
    {
        var stmt = new PlanStatement
        {
            NonParallelPlanReason = "MaxDOPSetToOne",
            StatementSubTreeCost = 10,
            StatementOptmLevel = "FULL",
            StatementText = text
        };
        Analyze(stmt);

        Assert.Equal(expectWarning, Has(stmt, "Serial Plan"));
    }

    // ---- rule 26: row goal cause ignores keywords inside comments ----------------------------

    [Fact]
    public void Rule26_RowGoalCauseIgnoresKeywordsInComments()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Index Scan",
            LogicalOp = "Index Scan",
            EstimateRows = 1,
            EstimateRowsWithoutRowGoal = 1000
        };
        var stmt = new PlanStatement
        {
            StatementText = "SELECT a FROM dbo.t WHERE EXISTS (SELECT 1 FROM dbo.u) -- was TOP (1)",
            RootNode = scan
        };
        Analyze(stmt);

        var warning = Assert.Single(scan.Warnings, w => w.WarningType == "Row Goal");
        Assert.Contains("due to EXISTS.", warning.Message);
    }
}
