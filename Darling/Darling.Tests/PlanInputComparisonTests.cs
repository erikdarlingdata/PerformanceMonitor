/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5630: <see cref="PlanInputComparison"/> decides whether two plans were compiled for the same inputs, so the
/// PLAN_REGRESSION detector does not call a parameter-sensitive or recompiling query a plan regression.
/// </summary>
public sealed class PlanInputComparisonTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static string Plan(string statement, params (string Column, string Value)[] parameters)
    {
        var list = string.Empty;
        foreach (var (column, value) in parameters)
        {
            list += "<ColumnReference Column=\"" + column + "\" ParameterDataType=\"int\" ParameterCompiledValue=\"" + value
                + "\" ParameterRuntimeValue=\"" + value + "\"/>";
        }

        return "<ShowPlanXML xmlns=\"" + Ns + "\" Version=\"1.564\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + statement + "\" StatementId=\"1\" StatementType=\"SELECT\">"
            + "<QueryPlan CachedPlanSize=\"16\">"
            + (list.Length == 0 ? string.Empty : "<ParameterList>" + list + "</ParameterList>")
            + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";
    }

    [Theory]
    [InlineData("SELECT 1 OPTION (RECOMPILE)")]
    [InlineData("select 1 option(recompile)")]
    [InlineData("SELECT 1\r\nOPTION\r\n(\r\n  MAXDOP 1 ,\r\n  RecompILE\r\n)")]
    [InlineData("SELECT 1 OPTION (OPTIMIZE FOR (@a = 1), RECOMPILE)")]
    [InlineData("SELECT 1 /* x */ OPTION (/* y */ RECOMPILE /* z */)")]
    public void ARecompileHintIsFoundInAnyCaseAndSpacing(string text)
    {
        Assert.True(PlanInputComparison.HasRecompileHint(text));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("SELECT 1 OPTION (MAXDOP 1)")]
    [InlineData("SELECT 1 /* OPTION (RECOMPILE) */")]
    [InlineData("SELECT 1 -- OPTION (RECOMPILE)")]
    [InlineData("SELECT 'OPTION (RECOMPILE)'")]
    [InlineData("SELECT recompile FROM dbo.t")]
    [InlineData("SELECT 1 OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("-- statement text withheld (#4348)")]
    public void AStatementWithoutTheHintIsNotFlagged(string? text)
    {
        Assert.False(PlanInputComparison.HasRecompileHint(text));
    }

    [Fact]
    public void PlansCompiledForDifferentValuesAreDifferent()
    {
        var a = Plan("SELECT 1", ("@location_id", "(1)"));
        var b = Plan("SELECT 1", ("@location_id", "(7)"));
        Assert.Equal(PlanInputVerdict.Different, PlanInputComparison.Compare(a, b));
        Assert.Equal(PlanInputVerdict.Different, PlanInputComparison.Compare(b, a));
    }

    [Fact]
    public void PlansCompiledForTheSameValuesAreSame()
    {
        var a = Plan("SELECT 1", ("@a", "(1)"), ("@b", "(2)"));
        var b = Plan("SELECT 1", ("@b", "(2)"), ("@a", "(1)"));
        Assert.Equal(PlanInputVerdict.Same, PlanInputComparison.Compare(a, b));
    }

    [Fact]
    public void TwoPlansWithoutParametersAreSame()
    {
        Assert.Equal(PlanInputVerdict.Same, PlanInputComparison.Compare(Plan("SELECT 1"), Plan("SELECT 2")));
    }

    [Fact]
    public void DifferentParameterSetsAreDifferent()
    {
        var a = Plan("SELECT 1", ("@a", "(1)"));
        var b = Plan("SELECT 1", ("@a", "(1)"), ("@b", "(1)"));
        Assert.Equal(PlanInputVerdict.Different, PlanInputComparison.Compare(a, b));
        Assert.Equal(PlanInputVerdict.Different, PlanInputComparison.Compare(a, Plan("SELECT 1")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not xml at all")]
    [InlineData("<ShowPlanXML><unclosed>")]
    [InlineData("<!DOCTYPE r [<!ENTITY e \"x\">]><r>&e;</r>")]
    public void AMissingOrUnreadablePlanIsUnknown(string? bad)
    {
        var good = Plan("SELECT 1", ("@a", "(1)"));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(bad, good));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(good, bad));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(bad, bad));
    }

    [Fact]
    public void ThePlaceholderIsUnknownAndTwoPlaceholdersAreNeverSame()
    {
        var marker = WithheldStatementMarker.Text;
        var good = Plan("SELECT 1", ("@a", "(1)"));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(marker, good));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(marker, marker));

        var withheldValue = Plan("SELECT 1", ("@a", marker));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(withheldValue, good));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(good, withheldValue));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(withheldValue, withheldValue));
    }

    [Fact]
    public void ThePlaceholderTextMatchesTheFilterMarker()
    {
        Assert.Equal(PerformanceMonitor.Common.SensitiveStatements.PlaceholderText, WithheldStatementMarker.Text);
    }

    [Fact]
    public void PlansStoredAfterTheRealFilterKeepOrdinaryValuesAndWithholdTheNamedOnes()
    {
        var ordinaryA = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan(StatementScrubCanary.PlainStatement, ("@c", "(1)")));
        var ordinaryB = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan(StatementScrubCanary.PlainStatement, ("@c", "(9)")));
        Assert.Equal(PlanInputVerdict.Different, PlanInputComparison.Compare(ordinaryA, ordinaryB));
        Assert.Equal(PlanInputVerdict.Same, PlanInputComparison.Compare(ordinaryA, ordinaryA));

        var withheldA = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan(StatementScrubCanary.CanaryStatement, ("@p", "N'one'")));
        var withheldB = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan(StatementScrubCanary.CanaryStatement, ("@p", "N'two'")));
        Assert.DoesNotContain("N'one'", withheldA, StringComparison.Ordinal);
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(withheldA, withheldB));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(withheldA, withheldA));
        Assert.Equal(PlanInputVerdict.Unknown, PlanInputComparison.Compare(withheldA, ordinaryA));
    }
}
