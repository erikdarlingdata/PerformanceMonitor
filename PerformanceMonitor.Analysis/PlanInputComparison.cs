/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace PerformanceMonitor.Analysis;

/// <summary>The decision for one PLAN_REGRESSION candidate.</summary>
public enum PlanInputOutcome
{
    /// <summary>From <see cref="PlanInputComparison.JudgeBeforePlans"/> only: nothing decided yet; read both plans and call <see cref="PlanInputComparison.JudgePlans"/>.</summary>
    NeedsPlans,
    /// <summary>Drop: the statement recompiles on every call.</summary>
    DropRecompile,
    /// <summary>Drop: the newer plan returns far more rows per call and the cost per row did not regress.</summary>
    DropOutputGrew,
    /// <summary>Drop: the compiled values differ and the statistics did not change, so the values alone changed the plan.</summary>
    DropValuesOnly,
    /// <summary>Keep: both plans were compiled for the same parameter values (or neither has any).</summary>
    KeepSameValues,
    /// <summary>Keep: the compiled values differ and the statistics or the estimator version changed between the compiles.</summary>
    KeepStatisticsChanged,
    /// <summary>Keep: the inputs could not be compared.</summary>
    KeepUnverified,
}

/// <summary>
/// #5630/#5631: the PLAN_REGRESSION detector compares a query's newest plan with its cheapest one. The cost can follow
/// the output (a newer plan that returns far more rows), the statement can recompile on every call
/// (<c>OPTION (RECOMPILE)</c>), or the cheap plan can have served only other parameter values; none of those is a plan
/// that got worse. This class judges a candidate in that order: <see cref="JudgeBeforePlans"/> needs no plan,
/// <see cref="JudgePlans"/> reads the two stored plans. Only the outcome leaves the judging: a compiled value is a customer
/// value, so the detector never logs, stores or returns one.
///
/// <para><b>The one stored copy (#6002).</b> <see cref="CompiledValues"/> is the single exception: the investigation evidence
/// capture reads a regression target's good and bad plans through it and freezes their compiled values in the
/// <c>regression_targets</c> evidence row, the one place a compiled value is stored. Each value sits under the
/// <c>compiled_value</c> key, which <c>mcp.dataDetail</c> classes as a parameter value, so the served evidence masks it on the
/// same path as statement text (replaced under <c>maskLiterals</c>, withheld under <c>metadataOnly</c>).</para>
/// </summary>
public static class PlanInputComparison
{
    /// <summary>The newer plan must return at least this many times the rows per call of the cheaper plan for the output rule to apply: one round order of magnitude.</summary>
    public const int OutputGrowthRatio = 10;

    /// <summary>The output rule drops a candidate only when its cost per row grew by less than this factor (the detector's own 2x).</summary>
    public const int PerRowFactor = 2;

    /// <summary>
    /// Rows per call are floored at this before they are compared. Below one row per call the output cannot explain the
    /// cost, and the stores keep the average as a whole number, so fractions are lost anyway.
    /// </summary>
    public const int RowFloor = 1;

    // Matches the whole word RECOMPILE inside an OPTION list.
    private static readonly Regex s_recompileWord = new(
        @"(?<![A-Za-z0-9_@#$])RECOMPILE(?![A-Za-z0-9_@#$])",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromSeconds(1));

    private static readonly Regex s_optionOpen = new(
        @"(?<![A-Za-z0-9_@#$])OPTION\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromSeconds(1));

    /// <summary>
    /// True when the statement carries an <c>OPTION ( ... RECOMPILE ... )</c> query hint, in any case and spacing.
    /// Comments and string literals are removed first, so a commented-out hint or a hint quoted in a literal does
    /// not count. The withheld-statement placeholder has no hint, so it returns false.
    /// </summary>
    public static bool HasRecompileHint(string? statementText)
    {
        if (string.IsNullOrWhiteSpace(statementText))
            return false;

        try
        {
            var cleaned = StripCommentsAndLiterals(statementText);
            var search = 0;
            while (search < cleaned.Length)
            {
                var open = s_optionOpen.Match(cleaned, search);
                if (!open.Success)
                    return false;

                // Walk to the matching close paren; OPTION ( OPTIMIZE FOR (@a = 1), RECOMPILE ) nests.
                var depth = 1;
                var i = open.Index + open.Length;
                var bodyStart = i;
                while (i < cleaned.Length && depth > 0)
                {
                    if (cleaned[i] == '(')
                        depth++;
                    else if (cleaned[i] == ')')
                        depth--;
                    i++;
                }

                var bodyEnd = depth == 0 ? i - 1 : i;
                if (s_recompileWord.IsMatch(cleaned.AsSpan(bodyStart, bodyEnd - bodyStart)))
                    return true;

                search = Math.Max(i, open.Index + open.Length);
            }
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }

        return false;
    }


    /// <summary>
    /// The rule steps that need no plan, in order: a statement with <c>OPTION (... RECOMPILE ...)</c> is
    /// <see cref="PlanInputOutcome.DropRecompile"/>; a newer plan that returns at least <see cref="OutputGrowthRatio"/>
    /// times the rows per call of the cheaper one, while its cost per row grew by less than <see cref="PerRowFactor"/>,
    /// is <see cref="PlanInputOutcome.DropOutputGrew"/> (the cost follows the output). Anything else, including missing
    /// rows, is <see cref="PlanInputOutcome.NeedsPlans"/>. Rows per call are floored at <see cref="RowFloor"/>.
    /// </summary>
    /// <param name="statementText">The statement text, or null / the withheld placeholder when it is unavailable.</param>
    /// <param name="factor">The detector's regression factor, as both collectors read it from <c>regression_factor</c>: the newer plan's CPU per call divided by the cheaper plan's for a CPU-detected row, and half the duration ratio for a duration-dominant row. The per-row test below is therefore a cost-per-row ratio only for a CPU-detected row.</param>
    /// <param name="rowsLatest">Rows per call of the newer plan, or null when unknown.</param>
    /// <param name="rowsBest">Rows per call of the cheaper plan, or null when unknown.</param>
    public static PlanInputOutcome JudgeBeforePlans(string? statementText, double factor, double? rowsLatest, double? rowsBest)
    {
        if (HasRecompileHint(statementText))
            return PlanInputOutcome.DropRecompile;

        if (rowsLatest is { } latestRows && rowsBest is { } bestRows
            && double.IsFinite(latestRows) && double.IsFinite(bestRows) && double.IsFinite(factor))
        {
            var rl = Math.Max(latestRows, RowFloor);
            var rb = Math.Max(bestRows, RowFloor);
            if (rl >= OutputGrowthRatio * rb && factor * rb / rl < PerRowFactor)
                return PlanInputOutcome.DropOutputGrew;
        }

        return PlanInputOutcome.NeedsPlans;
    }

    /// <summary>
    /// The plan-based rule steps, for a candidate <see cref="JudgeBeforePlans"/> did not decide. Never returns
    /// <see cref="PlanInputOutcome.NeedsPlans"/>. A missing, unreadable or withheld plan, differing parameter names, a
    /// compiled value on one side only, or (when the values differ) a missing <c>OptimizerStatsUsage</c> is
    /// <see cref="PlanInputOutcome.KeepUnverified"/>. The same compiled values on every parameter (two plans with no
    /// parameters, or no compiled value on both sides, match) is <see cref="PlanInputOutcome.KeepSameValues"/>.
    /// Different values with a different statistics set, a different statistics update time or a different
    /// cardinality estimator version is <see cref="PlanInputOutcome.KeepStatisticsChanged"/>; different values over
    /// the same statistics is <see cref="PlanInputOutcome.DropValuesOnly"/>. Only the outcome leaves this method: a
    /// compiled value is a customer value, so the detector never logs, stores or returns one (the class remarks name the
    /// one stored copy, #6002).
    /// </summary>
    public static PlanInputOutcome JudgePlans(string? latestPlanXml, string? bestPlanXml)
    {
        var latest = ReadInputs(latestPlanXml);
        var best = latest is null ? null : ReadInputs(bestPlanXml);
        if (latest is null || best is null)
            return PlanInputOutcome.KeepUnverified;

        if (latest.Parameters.Count != best.Parameters.Count)
            return PlanInputOutcome.KeepUnverified;

        var valuesDiffer = false;
        foreach (var (column, latestValue) in latest.Parameters)
        {
            if (!best.Parameters.TryGetValue(column, out var bestValue))
                return PlanInputOutcome.KeepUnverified;

            if (latestValue is null != bestValue is null)
                return PlanInputOutcome.KeepUnverified;

            if (latestValue is not null && !string.Equals(latestValue, bestValue, StringComparison.Ordinal))
                valuesDiffer = true;
        }

        if (!valuesDiffer)
            return PlanInputOutcome.KeepSameValues;

        if (latest.Statistics is null || best.Statistics is null)
            return PlanInputOutcome.KeepUnverified;

        if (!latest.Statistics.SetEquals(best.Statistics))
            return PlanInputOutcome.KeepStatisticsChanged;

        if (latest.CardinalityEstimationModelVersion is null != best.CardinalityEstimationModelVersion is null)
            return PlanInputOutcome.KeepUnverified;

        return string.Equals(latest.CardinalityEstimationModelVersion, best.CardinalityEstimationModelVersion, StringComparison.Ordinal)
            ? PlanInputOutcome.DropValuesOnly
            : PlanInputOutcome.KeepStatisticsChanged;
    }

    /// <summary>
    /// The parameter compiled values of one plan, in plan order (#6002): each parameter column with its compiled value (null
    /// when the parameter has none). Null when the plan cannot be read, under the same rules as <see cref="JudgePlans"/>
    /// (missing, unparseable, the withheld placeholder, a withheld compiled value, or one parameter listed twice with
    /// different values). The investigation evidence is the only caller: see the class remarks on the one stored copy.
    /// </summary>
    public static IReadOnlyList<KeyValuePair<string, string?>>? CompiledValues(string? planXml) =>
        ReadInputs(planXml) is { } inputs ? new List<KeyValuePair<string, string?>>(inputs.Parameters) : null;

    /// <summary>True for the three outcomes that drop the candidate.</summary>
    public static bool IsDrop(PlanInputOutcome outcome) =>
        outcome is PlanInputOutcome.DropRecompile or PlanInputOutcome.DropOutputGrew or PlanInputOutcome.DropValuesOnly;

    /// <summary>
    /// The <c>worst_input_basis</c> value for a kept candidate: 1 same compiled values, 2 statistics changed between
    /// the compiles, 0 not checked (an unverified keep, or any outcome that is not a keep).
    /// </summary>
    public static int InputBasis(PlanInputOutcome outcome) => outcome switch
    {
        PlanInputOutcome.KeepSameValues => 1,
        PlanInputOutcome.KeepStatisticsChanged => 2,
        _ => 0,
    };

    /// <summary>What <see cref="JudgePlans"/> reads from a plan: nothing else is looked at.</summary>
    private sealed class PlanInputs
    {
        /// <summary>Parameter column to its compiled value (null when the parameter has none).</summary>
        public Dictionary<string, string?> Parameters { get; } = new(StringComparer.Ordinal);

        /// <summary>The optimizer statistics the plan used, with their last-update strings; null when the plan has no <c>OptimizerStatsUsage</c>.</summary>
        public HashSet<(string Database, string Schema, string Table, string Statistics, string LastUpdate)>? Statistics { get; set; }

        public string? CardinalityEstimationModelVersion { get; set; }
    }

    /// <summary>
    /// Reads the parameter compiled values, the statistics used and the estimator version of a plan, or null when it
    /// cannot be judged: missing, unparseable, the withheld placeholder, a withheld compiled value, or one parameter
    /// listed twice with different values.
    /// </summary>
    private static PlanInputs? ReadInputs(string? planXml)
    {
        if (string.IsNullOrWhiteSpace(planXml))
            return null;

        if (WithheldStatementMarker.IsMarker(planXml))
            return null;

        var result = new PlanInputs();
        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            IgnoreWhitespace = true,
            IgnoreComments = true,
            ConformanceLevel = ConformanceLevel.Document,
        };

        try
        {
            using var text = new StringReader(planXml);
            using var reader = XmlReader.Create(text, settings);
            var parameterListDepth = 0;
            var sawElement = false;
            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element)
                {
                    sawElement = true;
                    switch (reader.LocalName)
                    {
                        case "ParameterList":
                            if (!reader.IsEmptyElement)
                                parameterListDepth++;
                            break;
                        case "ColumnReference" when parameterListDepth > 0:
                            var column = reader.GetAttribute("Column") ?? string.Empty;
                            var value = reader.GetAttribute("ParameterCompiledValue");
                            if (value is not null && WithheldStatementMarker.IsMarker(value))
                                return null;

                            if (result.Parameters.TryGetValue(column, out var seen)
                                && !string.Equals(seen, value, StringComparison.Ordinal))
                                return null;

                            result.Parameters[column] = value;
                            break;
                        case "OptimizerStatsUsage":
                            result.Statistics ??= [];
                            break;
                        case "StatisticsInfo":
                            result.Statistics ??= [];
                            result.Statistics.Add((
                                reader.GetAttribute("Database") ?? string.Empty,
                                reader.GetAttribute("Schema") ?? string.Empty,
                                reader.GetAttribute("Table") ?? string.Empty,
                                reader.GetAttribute("Statistics") ?? string.Empty,
                                reader.GetAttribute("LastUpdate") ?? string.Empty));
                            break;
                        case "StmtSimple":
                            result.CardinalityEstimationModelVersion ??= reader.GetAttribute("CardinalityEstimationModelVersion");
                            break;
                    }
                }
                else if (reader.NodeType == XmlNodeType.EndElement && reader.LocalName == "ParameterList" && parameterListDepth > 0)
                {
                    parameterListDepth--;
                }
            }

            if (!sawElement)
                return null;
        }
        catch (XmlException)
        {
            return null;
        }

        return result;
    }

    /// <summary>Replaces comments and the insides of string literals with spaces, keeping everything else.</summary>
    private static string StripCommentsAndLiterals(string sql)
    {
        var sb = new StringBuilder(sql.Length);
        var i = 0;
        while (i < sql.Length)
        {
            var c = sql[i];
            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n' && sql[i] != '\r')
                    i++;
                sb.Append(' ');
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                // Block comments nest in T-SQL.
                var depth = 1;
                i += 2;
                while (i < sql.Length && depth > 0)
                {
                    if (sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                    {
                        depth++;
                        i += 2;
                    }
                    else if (sql[i] == '*' && i + 1 < sql.Length && sql[i + 1] == '/')
                    {
                        depth--;
                        i += 2;
                    }
                    else
                    {
                        i++;
                    }
                }

                sb.Append(' ');
            }
            else if (c == '\'')
            {
                i++;
                while (i < sql.Length)
                {
                    if (sql[i] == '\'')
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    i++;
                }

                sb.Append("''");
            }
            else
            {
                sb.Append(c);
                i++;
            }
        }

        return sb.ToString();
    }
}
