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

/// <summary>What <see cref="PlanInputComparison.Compare"/> could tell about two plans' compile-time inputs.</summary>
public enum PlanInputVerdict
{
    /// <summary>Both plans were compiled for the same parameter values (or neither has parameters).</summary>
    Same,

    /// <summary>The plans were compiled for different parameter values, so the cost difference may be the data, not the plan.</summary>
    Different,

    /// <summary>The inputs could not be compared: a plan is missing, unreadable, or was stored with its values withheld.</summary>
    Unknown,
}

/// <summary>
/// #5630: the PLAN_REGRESSION detector compares a query's newest plan with its cheapest one. When the two plans were
/// compiled for different parameter values (a parameter-sensitive query), or the statement recompiles on every call
/// (<c>OPTION (RECOMPILE)</c>), the cheap plan only served small inputs and the comparison is not a regression.
/// This class answers those two questions. Only the verdict leaves it: a compiled value is a customer value, so
/// it is never logged, stored or returned.
/// </summary>
public static class PlanInputComparison
{
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
    /// Compares the compiled parameter values of two plans. Different when some parameter's compiled value differs
    /// or the parameter sets differ; Same when they match (two plans with no parameters match); Unknown when either
    /// plan is missing, empty or unreadable, or when either plan, or any compiled value in it, is the
    /// withheld-statement placeholder (two placeholders must never compare Same).
    /// </summary>
    public static PlanInputVerdict Compare(string? latestPlanXml, string? bestPlanXml)
    {
        var latest = ReadCompiledValues(latestPlanXml);
        if (latest is null)
            return PlanInputVerdict.Unknown;

        var best = ReadCompiledValues(bestPlanXml);
        if (best is null)
            return PlanInputVerdict.Unknown;

        return latest.SetEquals(best) ? PlanInputVerdict.Same : PlanInputVerdict.Different;
    }

    /// <summary>
    /// The set of (column, compiled value) pairs in the plan's <c>ParameterList</c> elements, or null when the plan
    /// cannot be judged (missing, unparseable, or carrying the placeholder).
    /// </summary>
    private static HashSet<(string Column, string? Value)>? ReadCompiledValues(string? planXml)
    {
        if (string.IsNullOrWhiteSpace(planXml))
            return null;

        // The filter withholds a whole document by storing the placeholder in its place.
        if (WithheldStatementMarker.IsMarker(planXml))
            return null;

        var result = new HashSet<(string Column, string? Value)>();
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
                    if (reader.LocalName == "ParameterList")
                    {
                        if (!reader.IsEmptyElement)
                            parameterListDepth++;
                    }
                    else if (parameterListDepth > 0 && reader.LocalName == "ColumnReference")
                    {
                        var column = reader.GetAttribute("Column") ?? string.Empty;
                        var value = reader.GetAttribute("ParameterCompiledValue");
                        if (value is not null && WithheldStatementMarker.IsMarker(value))
                            return null;

                        result.Add((column, value));
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
