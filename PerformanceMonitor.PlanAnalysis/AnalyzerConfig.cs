/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Per-rule analyzer configuration (#4535). Ported from erikdarlingdata/PerformanceStudio dev
/// (85492a1) <c>src/PlanViewer.Core/Models/AnalyzerConfig.cs:6-35</c>. A null <see cref="Rules"/>
/// (or a null property inside it) behaves the same as an empty one: no rule is disabled, and no
/// severity is overridden. This step ports the shape only; nothing in the analyzer reads it yet.
/// </summary>
public class AnalyzerConfig
{
    [JsonPropertyName("rules")]
    public RulesConfig? Rules { get; set; }

    public static AnalyzerConfig Default => new();

    public bool IsRuleDisabled(int ruleNumber)
    {
        return Rules?.Disabled?.Contains(ruleNumber) == true;
    }

    public string? GetSeverityOverride(int ruleNumber)
    {
        if (Rules?.SeverityOverrides != null &&
            Rules.SeverityOverrides.TryGetValue(ruleNumber, out var severity))
            return severity;
        return null;
    }
}

/// <summary>
/// Ported from erikdarlingdata/PerformanceStudio dev (85492a1)
/// <c>src/PlanViewer.Core/Models/AnalyzerConfig.cs:28-35</c>.
/// </summary>
public class RulesConfig
{
    [JsonPropertyName("disabled")]
    public List<int> Disabled { get; set; } = new();

    [JsonPropertyName("severity_overrides")]
    public Dictionary<int, string> SeverityOverrides { get; set; } = new();
}
