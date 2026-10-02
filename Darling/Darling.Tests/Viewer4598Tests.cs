/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for the Plan Insights strip's Parameters card (issue #4598): the row list and column flags
/// built from a statement's parameters, the sniffing/mismatch flags, the masked-text-driven
/// annotation for an all-compiled-null statement, and the unresolved-variable scan. Mirrors
/// PerformanceStudio dev's <c>PlanViewerControl.Parameters.cs</c> (erikdarlingdata/PerformanceStudio@85492a1).
/// </summary>
public class Viewer4598Tests
{
    [Fact]
    public void HeaderText_IsPlainWhenEmpty_AndCarriesCountOtherwise()
    {
        Assert.Equal("Parameters", ParameterCard.HeaderText(0));
        Assert.Equal("Parameters (1)", ParameterCard.HeaderText(1));
        Assert.Equal("Parameters (3)", ParameterCard.HeaderText(3));
    }

    [Fact]
    public void Columns_ShowsBothWhenBothPresent()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = "5", RuntimeValue = "5" },
        };

        var columns = ParameterCard.Columns(parameters);

        Assert.True(columns.ShowCompiled);
        Assert.True(columns.ShowRuntime);
        Assert.Equal("Compiled", columns.CompiledHeaderText);
    }

    [Fact]
    public void Columns_DropsRuntimeColumn_WhenNoParameterCarriesIt()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = "5", RuntimeValue = null },
        };

        var columns = ParameterCard.Columns(parameters);

        Assert.True(columns.ShowCompiled);
        Assert.False(columns.ShowRuntime);
        Assert.Equal("Compiled", columns.CompiledHeaderText);
    }

    [Fact]
    public void Columns_HeadsTheCompiledColumnValue_WhenNoParameterCarriesACompiledValue()
    {
        // OPTION(RECOMPILE)/OPTIMIZE FOR UNKNOWN: every CompiledValue is null, but the grid still
        // shows one value column, headed "Value" instead of "Compiled" (PerformanceStudio's fallback).
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = null },
        };

        var columns = ParameterCard.Columns(parameters);

        Assert.True(columns.ShowCompiled);
        Assert.False(columns.ShowRuntime);
        Assert.Equal("Value", columns.CompiledHeaderText);
    }

    [Fact]
    public void Row_FlagsSniffed_WhenRuntimeDiffersFromCompiled()
    {
        var parameter = new PlanParameter { Name = "@p1", DataType = "int", CompiledValue = "5", RuntimeValue = "500" };

        var row = ParameterCard.Row(parameter, allCompiledNull: false);

        Assert.True(row.Sniffed);
        Assert.Equal("5", row.CompiledText);
        Assert.Equal("500", row.RuntimeText);
        Assert.False(row.CompiledIsMissing);
    }

    [Fact]
    public void Row_DoesNotFlagSniffed_WhenRuntimeMatchesCompiled()
    {
        var parameter = new PlanParameter { Name = "@p1", DataType = "int", CompiledValue = "5", RuntimeValue = "5" };

        var row = ParameterCard.Row(parameter, allCompiledNull: false);

        Assert.False(row.Sniffed);
    }

    [Fact]
    public void Row_DoesNotFlagSniffed_WhenEitherValueIsMissing()
    {
        var noRuntime = ParameterCard.Row(
            new PlanParameter { Name = "@p1", DataType = "int", CompiledValue = "5", RuntimeValue = null },
            allCompiledNull: false);
        var noCompiled = ParameterCard.Row(
            new PlanParameter { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
            allCompiledNull: false);

        Assert.False(noRuntime.Sniffed);
        Assert.False(noCompiled.Sniffed);
    }

    [Fact]
    public void Row_CompiledTextIsQuestionMark_WhenThisParameterAloneIsMissingIt()
    {
        var row = ParameterCard.Row(
            new PlanParameter { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
            allCompiledNull: false);

        Assert.Equal("?", row.CompiledText);
        Assert.True(row.CompiledIsMissing);
    }

    [Fact]
    public void Row_CompiledTextIsBlank_NotQuestionMark_WhenEveryParameterIsMissingIt()
    {
        var row = ParameterCard.Row(
            new PlanParameter { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = null },
            allCompiledNull: true);

        Assert.Equal("", row.CompiledText);
        Assert.False(row.CompiledIsMissing);
    }

    [Fact]
    public void Annotations_ReturnsNone_WhenThereAreParametersAndNothingElseToSay()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = "5", RuntimeValue = "5" },
        };

        var annotations = ParameterCard.Annotations(parameters, "SELECT 1", "SELECT 1", []);

        Assert.Empty(annotations);
    }

    [Fact]
    public void Annotations_FlagsLocalVariables_WhenThereAreNoParametersAtAll()
    {
        var annotations = ParameterCard.Annotations(
            [], "DECLARE @x INT", "DECLARE @x INT", ["@x"]);

        Assert.Single(annotations);
        Assert.Contains("@x", annotations[0].Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotations[0].Tone);
    }

    [Fact]
    public void Annotations_ReturnsNone_WhenThereAreNoParametersAndNoLocalVariables()
    {
        var annotations = ParameterCard.Annotations([], "SELECT 1", "SELECT 1", []);

        Assert.Empty(annotations);
    }

    [Fact]
    public void Annotations_FlagsOptionRecompile_WhenEveryCompiledValueIsNullAndTheHintIsAbsent()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
        };
        var text = "SELECT * FROM t WHERE x = @p1 OPTION(RECOMPILE)";

        var annotations = ParameterCard.Annotations(parameters, text, text, []);

        Assert.Single(annotations);
        Assert.Contains("OPTION(RECOMPILE)", annotations[0].Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotations[0].Tone);
    }

    [Fact]
    public void Annotations_FlagsOptimizeForUnknown_WhenTheMaskedTextCarriesTheHint()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
        };
        var text = "SELECT * FROM t WHERE x = @p1 OPTION(OPTIMIZE FOR UNKNOWN)";

        var annotations = ParameterCard.Annotations(parameters, text, text, []);

        Assert.Single(annotations);
        Assert.Contains("OPTIMIZE FOR UNKNOWN", annotations[0].Text);
        Assert.Equal(ParameterCardAnnotationTone.Accent, annotations[0].Tone);
    }

    [Fact]
    public void Annotations_IgnoresOptimizeForUnknown_WhenItAppearsOnlyInsideAMaskedComment()
    {
        // #579/#4524: the phrase inside a comment is not a hint. The raw text contains the
        // literal substring "OPTIMIZE" (so the cheap Contains check passes), but the masked text
        // (comments blanked) does not match the full "OPTIMIZE FOR UNKNOWN" phrase, so this falls
        // through to the OPTION(RECOMPILE) branch instead.
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
        };
        var rawText = "SELECT * FROM t WHERE x = @p1 /* OPTIMIZE FOR UNKNOWN, someday */ OPTION(RECOMPILE)";
        var maskedText = PlanAnalyzer.MaskCommentsAndLiterals(rawText);

        var annotations = ParameterCard.Annotations(parameters, rawText, maskedText, []);

        Assert.Single(annotations);
        Assert.Contains("OPTION(RECOMPILE)", annotations[0].Text);
    }

    [Fact]
    public void Annotations_AppendsUnresolvedVariables_AfterTheAllCompiledNullAnnotation()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
        };
        var text = "SELECT * FROM t WHERE x = @p1 AND y = @local OPTION(RECOMPILE)";

        var annotations = ParameterCard.Annotations(parameters, text, text, ["@local"]);

        Assert.Equal(2, annotations.Count);
        Assert.Contains("OPTION(RECOMPILE)", annotations[0].Text);
        Assert.Contains("@local", annotations[1].Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotations[1].Tone);
    }

    [Fact]
    public void FindUnresolvedVariables_ExcludesExtractedParameters_AndTableVariables()
    {
        var parameters = new List<PlanParameter> { new() { Name = "@p1", DataType = "int" } };
        var root = new PlanNode
        {
            ObjectName = "@tv.PK__tv",
            Children = [],
        };
        var text = "SELECT @p1, @tv, @unresolved";

        var unresolved = ParameterCard.FindUnresolvedVariables(text, parameters, root);

        Assert.Equal(["@unresolved"], unresolved);
    }

    [Fact]
    public void FindUnresolvedVariables_MatchesPerformanceStudiosAtAtQuirk()
    {
        // The @\w+ regex (PerformanceStudio's own pattern) starts a match at the SECOND '@' of a
        // "@@" system variable, since '@' is not a \w character — so the captured token is
        // "@ROWCOUNT", not "@@ROWCOUNT", and the StartsWith("@@") skip below it never fires. This
        // pins the port's fidelity to PerformanceStudio's actual behaviour, not an idealised one.
        var unresolved = ParameterCard.FindUnresolvedVariables("SELECT @@ROWCOUNT", [], null);

        Assert.Equal(["@ROWCOUNT"], unresolved);
    }

    [Fact]
    public void FindUnresolvedVariables_ReturnsEachNameOnce()
    {
        var unresolved = ParameterCard.FindUnresolvedVariables(
            "SELECT @x, @x, @x", [], rootNode: null);

        Assert.Equal(["@x"], unresolved);
    }

    [Fact]
    public void FindUnresolvedVariables_ReturnsEmpty_ForEmptyQueryText()
    {
        Assert.Empty(ParameterCard.FindUnresolvedVariables("", [], null));
    }
}
