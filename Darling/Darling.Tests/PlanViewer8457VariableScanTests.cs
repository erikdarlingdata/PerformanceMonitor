/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8457: the Parameters card's variable scan reads code only. An <c>@name</c> inside a string literal (plain or
/// <c>N''</c>, with doubled quotes), a <c>--</c> or <c>/* */</c> comment, a <c>"quoted"</c> identifier or a
/// <c>[bracketed]</c> name is text, not a variable, so it is neither flagged as a local variable nor listed as
/// unresolved. The repro script's scan of the query text reads the same way.
/// </summary>
public sealed class PlanViewer8457VariableScanTests
{
    private static readonly IReadOnlyList<PlanParameter> NoParameters = new List<PlanParameter>();

    [Theory]
    [InlineData("SELECT * FROM dbo.t WHERE Email LIKE '%@example.com'")]
    [InlineData("SELECT * FROM dbo.t WHERE Note = N'ask @support'")]
    [InlineData("SELECT * FROM dbo.t WHERE Note = 'it''s @inside'")]
    [InlineData("SELECT 1 -- uses @StartDate from the caller")]
    [InlineData("SELECT 1 /* @StartDate comes from the caller */")]
    [InlineData("SELECT 1 /* outer /* @nested */ still @inside */")]
    [InlineData("SELECT [@x] FROM dbo.t")]
    [InlineData("SELECT \"@x\" FROM dbo.t")]
    [InlineData("SELECT '@unterminated")]
    public void TextInsideLiteralsCommentsAndDelimitedNames_IsNotAVariable(string text)
    {
        Assert.Empty(ParameterCard.FindUnresolvedVariables(text, NoParameters, null));
    }

    [Theory]
    [InlineData("SELECT * FROM dbo.t WHERE Email LIKE '%@example.com' AND Id = @id", "@id")]
    [InlineData("SELECT @id -- not @comment\r\n  FROM dbo.t", "@id")]
    [InlineData("SELECT /* @c */ @id FROM dbo.t", "@id")]
    [InlineData("SELECT 'it''s', @id FROM dbo.t", "@id")]
    [InlineData("SELECT N'@n', @id FROM dbo.t", "@id")]
    [InlineData("SELECT [@b], \"@q\", @id FROM dbo.t", "@id")]
    [InlineData("SELECT ']', [a]]b], @id", "@id")]
    public void TheCodeVariable_IsStillFound_BesideTextThatOnlyLooksLikeOne(string text, string expected)
    {
        Assert.Equal([expected], ParameterCard.FindUnresolvedVariables(text, NoParameters, null));
    }

    [Fact]
    public void ANameInCodeAndInAComment_IsListedOnce()
    {
        Assert.Equal(["@id"], ParameterCard.FindUnresolvedVariables("SELECT @id -- @id again", NoParameters, null));
    }

    [Fact]
    public void TheCard_ShowsNoLocalVariablesAnnotation_ForAnAddressInALiteral()
    {
        var text = "SELECT * FROM dbo.t WHERE Email LIKE '%@example.com'";
        var masked = PlanAnalyzer.MaskCommentsAndLiterals(text);
        var unresolved = ParameterCard.FindUnresolvedVariables(text, NoParameters, null);

        Assert.Empty(ParameterCard.Annotations(NoParameters, text, masked, unresolved));
    }

    [Fact]
    public void TheCard_ShowsNoUnresolvedVariablesAnnotation_ForANameInAComment()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@id", DataType = "int", CompiledValue = "1", RuntimeValue = "1" },
        };
        var text = "SELECT * FROM dbo.t WHERE Id = @id -- @StartDate comes from the caller";
        var masked = PlanAnalyzer.MaskCommentsAndLiterals(text);
        var unresolved = ParameterCard.FindUnresolvedVariables(text, parameters, null);

        Assert.Empty(ParameterCard.Annotations(parameters, text, masked, unresolved));
    }

    [Fact]
    public void TheReproScript_DoesNotWarnOfAVariable_ForAnAddressInALiteralOrANameInAComment()
    {
        var script = ReproScriptBuilder.BuildReproScript(
            "SELECT * FROM dbo.t WHERE Email LIKE '%@example.com' -- @StartDate comes from the caller",
            "db", planXml: null, isolationLevel: null);

        Assert.DoesNotContain("Variables in query without values", script, StringComparison.Ordinal);
    }

    [Fact]
    public void TheReproScript_StillWarnsOfAVariableInCode()
    {
        var script = ReproScriptBuilder.BuildReproScript(
            "SELECT * FROM dbo.t WHERE Email LIKE '%@example.com' AND Id = @id",
            "db", planXml: null, isolationLevel: null);

        Assert.Contains("Variables in query without values: @id.", script, StringComparison.Ordinal);
    }
}
