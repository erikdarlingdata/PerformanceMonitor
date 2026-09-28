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
using System.Text.RegularExpressions;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The Plan Insights strip's Parameters card: which columns to show, each parameter's row text and
/// whether its runtime value looks sniffed, and the annotation lines below the table. Pulled out of
/// the WPF/Avalonia code-behind (matches PerformanceStudio dev's <c>PlanViewerControl.Parameters.cs</c>,
/// erikdarlingdata/PerformanceStudio@85492a1) into a pure function this project's test suite can pin
/// directly, since the viewer control itself is Windows-only and can't run in a unit test on macOS.
/// </summary>
public static partial class ParameterCard
{
    /// <summary>The card's header text: "Parameters" when there are none, else "Parameters (N)".</summary>
    public static string HeaderText(int parameterCount) =>
        parameterCount == 0 ? "Parameters" : $"Parameters ({parameterCount})";

    /// <summary>
    /// Which value columns the card shows, given the statement's parameters. Name and Data Type are
    /// always shown; Compiled and/or Runtime appear only when at least one parameter carries that
    /// value, matching PerformanceStudio's grid layout (which drops a column entirely rather than
    /// showing it empty for every row). When neither compiled nor runtime is present, PerformanceStudio
    /// still shows one value column headed "Value" so every parameter has somewhere for a "?" — this
    /// case can't arise from PM's model (empty means no <see cref="PlanParameter"/> rows at all, which
    /// is the separate "no parameters" state below), but the flags are kept for parity if that changes.
    /// </summary>
    public static ParameterCardColumns Columns(IReadOnlyList<PlanParameter> parameters)
    {
        var hasCompiled = parameters.Any(p => p.CompiledValue != null);
        var hasRuntime = parameters.Any(p => p.RuntimeValue != null);
        return new ParameterCardColumns(
            ShowCompiled: hasCompiled || !hasRuntime,
            ShowRuntime: hasRuntime,
            CompiledHeaderText: hasCompiled ? "Compiled" : "Value");
    }

    /// <summary>
    /// One parameter's row: its compiled-value text (matching PerformanceStudio's "" when every
    /// parameter's compiled value is null, else "?" for a parameter missing just its own), its
    /// runtime-value text, and whether the runtime value looks sniffed (present, and different from a
    /// present compiled value — matching PerformanceStudio's <c>sniffed</c> flag exactly).
    /// </summary>
    public static ParameterCardRow Row(PlanParameter parameter, bool allCompiledNull)
    {
        var compiledText = parameter.CompiledValue ?? (allCompiledNull ? "" : "?");
        var compiledIsMissing = parameter.CompiledValue == null && !allCompiledNull;
        var runtimeText = parameter.RuntimeValue ?? "";
        var sniffed = parameter.RuntimeValue != null
            && parameter.CompiledValue != null
            && parameter.RuntimeValue != parameter.CompiledValue;

        return new ParameterCardRow(
            Name: parameter.Name,
            DataType: parameter.DataType,
            CompiledText: compiledText,
            CompiledIsMissing: compiledIsMissing,
            RuntimeText: runtimeText,
            Sniffed: sniffed);
    }

    /// <summary>
    /// The annotation lines the card shows below the table (or in place of the table when there are no
    /// parameters at all), in PerformanceStudio's order: the no-parameters/local-variables state OR the
    /// all-compiled-null cause (OPTIMIZE FOR UNKNOWN vs. OPTION(RECOMPILE)) followed by unresolved
    /// variables. <paramref name="maskedStatementText"/> must already have comments and string literals
    /// blanked (<see cref="PlanAnalyzer.MaskCommentsAndLiterals"/>, #4524's masked-text read) so a hint
    /// spelled inside a comment or a literal doesn't count as code.
    /// </summary>
    public static List<ParameterCardAnnotation> Annotations(
        IReadOnlyList<PlanParameter> parameters,
        string statementText,
        string maskedStatementText,
        IReadOnlyCollection<string> unresolvedVariables)
    {
        var annotations = new List<ParameterCardAnnotation>();

        if (parameters.Count == 0)
        {
            if (unresolvedVariables.Count > 0)
            {
                annotations.Add(new ParameterCardAnnotation(
                    $"Local variables detected ({string.Join(", ", unresolvedVariables)}) \u2014 values not captured in plan XML",
                    ParameterCardAnnotationTone.Warning));
            }
            return annotations;
        }

        var allCompiledNull = parameters.All(p => p.CompiledValue == null);
        if (allCompiledNull)
        {
            // #579 (mirrored via #4524's masked-text read): the phrase inside a string literal or a
            // comment is not a hint, so the OPTIMIZE FOR UNKNOWN check runs on the masked text, not
            // the raw statement text.
            var hasOptimizeForUnknown = statementText.Contains("OPTIMIZE", StringComparison.OrdinalIgnoreCase)
                && OptimizeForUnknownRegExp().IsMatch(maskedStatementText);

            annotations.Add(hasOptimizeForUnknown
                ? new ParameterCardAnnotation(
                    "OPTIMIZE FOR UNKNOWN \u2014 optimizer used average density estimates instead of sniffed values",
                    ParameterCardAnnotationTone.Accent)
                : new ParameterCardAnnotation(
                    "OPTION(RECOMPILE) \u2014 parameter values embedded as literals, not sniffed",
                    ParameterCardAnnotationTone.Warning));
        }

        if (unresolvedVariables.Count > 0)
        {
            annotations.Add(new ParameterCardAnnotation(
                $"Unresolved variables: {string.Join(", ", unresolvedVariables)} \u2014 not in parameter list",
                ParameterCardAnnotationTone.Warning));
        }

        return annotations;
    }

    /// <summary>
    /// <c>@variable</c> references in <paramref name="queryText"/> that are not in the statement's
    /// parameter list, not a <c>@@</c> system variable, and not a table variable name found in the plan
    /// tree — the same three exclusions as PerformanceStudio's <c>FindUnresolvedVariables</c>. Table
    /// variable names come from <paramref name="rootNode"/>'s subtree so a table variable read (which
    /// shows up in the query text as an ordinary <c>@name</c> reference) isn't misreported as an
    /// unresolved local variable.
    /// </summary>
    public static List<string> FindUnresolvedVariables(
        string queryText, IReadOnlyList<PlanParameter> parameters, PlanNode? rootNode = null)
    {
        var unresolved = new List<string>();
        if (string.IsNullOrEmpty(queryText))
            return unresolved;

        var extractedNames = new HashSet<string>(
            parameters.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);

        var tableVarNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (rootNode != null)
            CollectTableVariableNames(rootNode, tableVarNames);

        var matches = AtVariableRegExp().Matches(queryText);
        var seenVars = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (Match match in matches)
        {
            var varName = match.Value;
            if (seenVars.Contains(varName) || extractedNames.Contains(varName))
                continue;
            if (varName.StartsWith("@@", StringComparison.OrdinalIgnoreCase))
                continue;
            if (tableVarNames.Contains(varName))
                continue;

            seenVars.Add(varName);
            unresolved.Add(varName);
        }

        return unresolved;
    }

    private static void CollectTableVariableNames(PlanNode node, HashSet<string> names)
    {
        if (!string.IsNullOrEmpty(node.ObjectName) && node.ObjectName.StartsWith('@'))
        {
            // ObjectName is like "@t.c" — extract the table variable name "@t".
            var dotIdx = node.ObjectName.IndexOf('.');
            var tvName = dotIdx > 0 ? node.ObjectName[..dotIdx] : node.ObjectName;
            names.Add(tvName);
        }
        foreach (var child in node.Children)
            CollectTableVariableNames(child, names);
    }

    [GeneratedRegex(@"OPTIMIZE\s+FOR\s+UNKNOWN", RegexOptions.IgnoreCase)]
    private static partial Regex OptimizeForUnknownRegExp();

    [GeneratedRegex(@"@\w+", RegexOptions.IgnoreCase)]
    private static partial Regex AtVariableRegExp();
}

/// <summary>Which value columns the Parameters card shows, and the compiled column's header text.</summary>
public readonly record struct ParameterCardColumns(bool ShowCompiled, bool ShowRuntime, string CompiledHeaderText);

/// <summary>One parameter's display row in the Parameters card's grid.</summary>
public readonly record struct ParameterCardRow(
    string Name, string DataType, string CompiledText, bool CompiledIsMissing, string RuntimeText, bool Sniffed);

/// <summary>The tone (brush) of one Parameters card annotation line, mirroring PerformanceStudio's brush keys.</summary>
public enum ParameterCardAnnotationTone
{
    Warning,
    Accent,
}

/// <summary>One annotation line shown below the Parameters card's table.</summary>
public readonly record struct ParameterCardAnnotation(string Text, ParameterCardAnnotationTone Tone);
