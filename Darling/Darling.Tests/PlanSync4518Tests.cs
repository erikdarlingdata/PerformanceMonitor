/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4518 — the non-SARGable predicate check had four faults, all in the same family: a helper looked at
/// the wrong side of a comparison, or at more of the predicate than the one comparison it should have read.
///
/// <para>1. <c>CONVERT_IMPLICIT</c> was flagged whenever the predicate contained it, with no check of which
/// side it converts. Data type precedence decides which side SQL Server converts, and it converts the
/// LOWER-precedence side — comparing a column to a parameter of narrower precedence converts the PARAMETER,
/// leaving the column untouched and still seekable.</para>
///
/// <para>2 &amp; 3. <c>IsFunctionOnColumnSide</c> (used for both the function-call check and ISNULL/COALESCE)
/// split the WHOLE predicate at its first comparison operator. In a compound predicate that put every later
/// comparison, column and all, on the function's side, so a parameter-side function in the first conjunct
/// made a real column reference in a LATER conjunct look like it shared the function's side.</para>
///
/// <para>4. <c>LIKE</c> was not recognized as a comparison operator, so <c>[col] like upper([@p])</c> found no
/// operator at all, fell to the assume-the-worst default, and reported the <c>upper()</c> on the pattern as a
/// function on the column.</para>
///
/// <para>Each predicate here is a raw ScalarString shape, not a plan fixture, because the shapes that matter
/// (which side a function sits on, across an AND/OR) are decided entirely inside
/// <see cref="PlanAnalyzer.DetectNonSargablePattern"/> and the helpers it calls.</para>
/// </summary>
public sealed class PlanSync4518Tests
{
    // ---------------------------------------------------------------
    // Fault 1: CONVERT_IMPLICIT on the parameter side is not a finding
    // ---------------------------------------------------------------

    [Fact]
    public void ConvertImplicitOnParameter_NotFlagged()
    {
        Assert.Null(PlanAnalyzer.DetectNonSargablePattern(
            "[t].[a]=CONVERT_IMPLICIT(int,[@p1],0)"));
    }

    [Fact]
    public void ConvertImplicitOnColumn_StillFlagged()
    {
        Assert.Equal("Implicit conversion (CONVERT_IMPLICIT)", PlanAnalyzer.DetectNonSargablePattern(
            "CONVERT_IMPLICIT(varchar(10),[t].[a],0)=[@p1]"));
    }

    // ---------------------------------------------------------------
    // Fault 2: the function-on-column-side check is scoped to its own comparison
    // ---------------------------------------------------------------

    [Fact]
    public void ParameterSideFunctionInEarlierComparison_NotFlagged()
    {
        Assert.Null(PlanAnalyzer.DetectNonSargablePattern(
            "[t].[a]=[@p1] AND [t].[b]=upper([@p2])"));
    }

    [Fact]
    public void ColumnSideFunctionInLaterComparison_StillFlagged()
    {
        Assert.Equal("Function call (UPPER) on column", PlanAnalyzer.DetectNonSargablePattern(
            "[t].[a]=[@p1] AND upper([t].[b])=[@p2]"));
    }

    // ---------------------------------------------------------------
    // Fault 3: the same scoping applies to ISNULL/COALESCE
    // ---------------------------------------------------------------

    [Fact]
    public void ParameterSideIsnull_NotFlagged()
    {
        Assert.Null(PlanAnalyzer.DetectNonSargablePattern(
            "[t].[a] = isnull([@p],0)"));
    }

    [Fact]
    public void ColumnSideIsnull_StillFlagged()
    {
        Assert.Equal("ISNULL/COALESCE wrapping column", PlanAnalyzer.DetectNonSargablePattern(
            "isnull([t].[a],0) = [@p]"));
    }

    // ---------------------------------------------------------------
    // Fault 4: LIKE is a recognized comparison operator
    // ---------------------------------------------------------------

    [Fact]
    public void ParameterSideFunctionInLikePattern_NotFlagged()
    {
        Assert.Null(PlanAnalyzer.DetectNonSargablePattern(
            "[t].[c] like upper([@p])"));
    }

    // ---------------------------------------------------------------
    // Mutation target: dropping the same-depth AND/OR scoping must turn
    // ColumnSideFunctionInLaterComparison_StillFlagged red for the wrong reason and
    // ParameterSideFunctionInEarlierComparison_NotFlagged red outright.
    // ---------------------------------------------------------------
}
