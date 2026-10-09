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
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Determines whether a query MODIFIES data (INSERT / UPDATE / DELETE / MERGE / …) — the safety signal that
/// gates "Get Actual Plan" (which RE-EXECUTES the query, re-applying any writes). One shared implementation for
/// every edition (Lite, the deprecated Dashboard, the Darling viewer).
///
/// <para>The authoritative signal is the PLAN XML, not the query text: SQL Server's Showplan declares a
/// <c>StatementType</c> attribute on each statement element (<c>SELECT</c> / <c>INSERT</c> / <c>UPDATE</c> /
/// <c>DELETE</c> / <c>MERGE</c> / <c>SELECT INTO</c> / DDL). When the plan XML is missing or unparseable there
/// is a CONSERVATIVE text-based fallback that marks the result <see cref="QueryModificationInfo.IsUncertain"/> —
/// and an uncertain result ALWAYS reports <see cref="QueryModificationInfo.ModifiesData"/> = true (fail safe:
/// if we cannot prove a query is read-only, we treat it as modifying).</para>
/// </summary>
public static partial class QueryModificationDetector
{
    /// <summary>
    /// The statement-type prefixes that WRITE data or schema. Matched case-insensitively against the leading
    /// token so variants ride along (e.g. <c>INSERT</c> covers "INSERT EXEC", <c>UPDATE</c> covers
    /// "UPDATE STATISTICS"). <c>SELECT INTO</c> is handled explicitly (it starts with SELECT but creates+writes
    /// a table). Erring toward flagging is the safe direction for a re-execution gate.
    /// </summary>
    private static readonly string[] s_modifyingPrefixes =
    {
        "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "DROP", "ALTER", "CREATE", "GRANT", "REVOKE", "DENY",
    };

    /// <summary>The keywords the text fallback scans for (display only — an uncertain result is modifying regardless).</summary>
    private static readonly string[] s_textKeywords =
    {
        "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "DROP", "ALTER", "CREATE",
    };

    /// <summary>
    /// Determines whether the query modifies data. Prefers the plan XML's <c>StatementType</c> declarations;
    /// falls back (marked uncertain → modifying) to a conservative text scan when the plan is missing or cannot
    /// be parsed, or parses but declares no statement type.
    /// </summary>
    public static QueryModificationInfo Detect(string? planXml, string? queryText)
    {
        if (!string.IsNullOrWhiteSpace(planXml))
        {
            var types = TryReadStatementTypesFromPlan(planXml);
            if (types is { Count: > 0 })
            {
                var modifies = types.Any(IsModifyingStatementType);
                return new QueryModificationInfo(modifies, types, IsUncertain: false);
            }
        }

        /* No plan, unparseable plan, or a plan with no declared statement type: fall back to a best-effort
           text scan for DISPLAY, but treat the result as MODIFYING because we could not prove otherwise. */
        return new QueryModificationInfo(ModifiesData: true, ScanTextForStatementTypes(queryText), IsUncertain: true);
    }

    /// <summary>
    /// True when a Showplan <c>StatementType</c> value denotes a data- or schema-modifying statement. Public so
    /// callers/tests and <see cref="BuildConsentWarning"/> can filter a mixed statement list down to the writers.
    /// </summary>
    public static bool IsModifyingStatementType(string? statementType)
    {
        if (string.IsNullOrWhiteSpace(statementType))
        {
            return false;
        }

        var upper = statementType.Trim().ToUpperInvariant();
        if (upper == "SELECT INTO")
        {
            return true;
        }

        foreach (var prefix in s_modifyingPrefixes)
        {
            if (upper.StartsWith(prefix, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The distinct <c>StatementType</c> values declared anywhere in the plan (statement elements at any nesting
    /// — StmtSimple, and those inside StmtCond IF/ELSE blocks or StmtCursor). Returns null when the XML cannot be
    /// parsed (the caller's fail-safe trigger); an empty list when it parses but declares no statement type.
    /// </summary>
    private static List<string>? TryReadStatementTypesFromPlan(string planXml)
    {
        try
        {
            var doc = XDocument.Parse(planXml);

            /* StatementType is an unqualified attribute, so match it regardless of the element's namespace —
               robust to the showplan namespace and to any fragment/stripped-namespace plan XML. */
            return doc.Descendants()
                .Select(e => e.Attribute("StatementType")?.Value)
                .Where(v => !string.IsNullOrWhiteSpace(v))
                .Select(v => v!.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        catch (XmlException)
        {
            return null;
        }
    }

    /// <summary>
    /// A best-effort keyword scan of the query text — used ONLY for the uncertain fallback's DISPLAY (the
    /// modification verdict is already "modifying" in that case). Returns the distinct DML/DDL keywords present.
    /// </summary>
    private static IReadOnlyList<string> ScanTextForStatementTypes(string? queryText)
    {
        if (string.IsNullOrWhiteSpace(queryText))
        {
            return Array.Empty<string>();
        }

        var found = new List<string>();
        foreach (var keyword in s_textKeywords)
        {
            if (Regex.IsMatch(queryText, $@"\b{keyword}\b", RegexOptions.IgnoreCase))
            {
                found.Add(keyword);
            }
        }

        return found;
    }

    /// <summary>
    /// One sentence every "run this captured query to get its actual plan" confirmation shows: the text came
    /// from the monitored server, and any user of that server may have written it, so the operator reads it
    /// before allowing the run. Shared so Lite, the Darling viewer and the shared plan navigation word it alike.
    /// </summary>
    public const string CapturedQueryNotice =
        "This query text was captured from the monitored server, and any user of that server may have written it, so read it before you allow the run.";

    /// <summary>
    /// Builds the PROMINENT, distinct data-modification warning block for a consent dialog (empty string when
    /// the query does not modify data — the caller shows only its normal "this will execute" warning). Shared
    /// so Lite and the Darling viewer word the modification flag identically. Names the statement types and
    /// states plainly that re-executing RE-APPLIES those writes to the database.
    /// </summary>
    public static string BuildConsentWarning(QueryModificationInfo info, string? databaseName)
    {
        ArgumentNullException.ThrowIfNull(info);
        if (!info.ModifiesData)
        {
            return string.Empty;
        }

        var db = string.IsNullOrWhiteSpace(databaseName) ? "the target database" : $"[{databaseName}]";
        var sb = new StringBuilder();
        sb.AppendLine("⚠  DATA-MODIFICATION WARNING  ⚠");
        sb.AppendLine();

        if (info.IsUncertain)
        {
            sb.AppendLine(
                "The execution plan for this query could not be analyzed, so it is being treated as " +
                "DATA-MODIFYING to be safe.");
            sb.AppendLine();
            sb.AppendLine(
                $"Re-executing it may INSERT, UPDATE, DELETE, or MERGE data in {db}. Only continue if you are " +
                "certain the query is safe to run again.");
        }
        else
        {
            var writers = info.StatementTypes.Where(IsModifyingStatementType).ToList();
            var typeList = writers.Count > 0 ? string.Join(", ", writers) : string.Join(", ", info.StatementTypes);
            sb.AppendLine($"This query MODIFIES DATA. Statement type(s): {typeList}.");
            sb.AppendLine();
            sb.AppendLine(
                $"Getting the actual plan RE-EXECUTES the query, which will RE-APPLY those {typeList} changes to " +
                $"{db}. This is NOT a read-only operation.");
        }

        return sb.ToString();
    }
}

/// <summary>
/// The result of a data-modification check. <see cref="ModifiesData"/> is the gate signal; when
/// <see cref="IsUncertain"/> is true it is ALWAYS true (the plan could not be analyzed, so the query is treated
/// as modifying). <see cref="StatementTypes"/> is the distinct set of statement types found (from the plan when
/// certain, from a text scan when uncertain) for display in the consent dialog.
/// </summary>
public sealed record QueryModificationInfo(
    bool ModifiesData,
    IReadOnlyList<string> StatementTypes,
    bool IsUncertain);
