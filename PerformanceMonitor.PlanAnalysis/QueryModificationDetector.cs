/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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
                if (!modifies && (types.Any(IsExecuteStatementType) || TextRunsCodeThePlanDoesNotShow(queryText)))
                {
                    /* #5639: an EXEC/EXECUTE statement runs code the plan's StatementType does not describe, and
                       OPENQUERY / OPENROWSET / OPENDATASOURCE send text to another server. Either can write data
                       while the plan reads SELECT, so these get the "could not be analyzed" warning instead of a
                       clean pass. A plan statement of type EXECUTE (EXECUTE PROC) is that same case whatever the
                       text says: a call with no EXEC keyword (a procedure name that starts the batch, sp_executesql)
                       still runs code the plan does not show. Only those cases: what a function or procedure does is
                       not analyzed. */
                    return new QueryModificationInfo(ModifiesData: true, types, IsUncertain: true);
                }

                return new QueryModificationInfo(modifies, types, IsUncertain: false);
            }
        }

        /* No plan, unparseable plan, or a plan with no declared statement type: fall back to a best-effort
           text scan for DISPLAY, but treat the result as MODIFYING because we could not prove otherwise. */
        return new QueryModificationInfo(ModifiesData: true, ScanTextForStatementTypes(queryText), IsUncertain: true);
    }

    /// <summary>
    /// True when the query text has an EXEC / EXECUTE statement or uses OPENQUERY, OPENROWSET or OPENDATASOURCE.
    /// A match inside a comment or string literal counts too: the safe direction for a re-execution gate.
    /// </summary>
    private static bool TextRunsCodeThePlanDoesNotShow(string? queryText)
        => !string.IsNullOrWhiteSpace(queryText)
           && (HasExecKeyword(queryText)
               || PlanRegex.IsMatch(s_remoteRowset, queryText, "remote rowset functions in the query text", onTimeout: true));

    /// <summary>True when a Showplan <c>StatementType</c> is an EXECUTE one (<c>EXECUTE PROC</c>).</summary>
    private static bool IsExecuteStatementType(string? statementType)
        => statementType is not null && statementType.TrimStart().StartsWith("EXECUTE", StringComparison.OrdinalIgnoreCase);

    /* EXEC or EXECUTE as a word of its own: not part of a longer name (dm_exec_requests, execution_count), not a
       column reached through a dot or a variable or temp-table name. A closing bracket or quote right before it
       (`[a]EXEC dbo.p`) or a quote right after it (`EXEC"dbo"."p"`) is NOT a reason to skip it; only a match that
       is fully wrapped in a matching pair (`[exec]`, `"exec"`) is an identifier. */
    private static readonly Regex s_execStatement = new(
        @"(?<![\w@#$.])EXEC(?:UTE)?(?!\w)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled,
        PlanRegex.DefaultTimeout);

    /// <summary>True when the text has an EXEC / EXECUTE keyword that is not a bracketed or double-quoted identifier.</summary>
    private static bool HasExecKeyword(string queryText)
    {
        var statements = PlanRegex.Matches(s_execStatement, queryText, "EXEC keyword in the query text");
        foreach (Match match in statements)
        {
            var before = match.Index > 0 ? queryText[match.Index - 1] : '\0';
            var afterIndex = match.Index + match.Length;
            var after = afterIndex < queryText.Length ? queryText[afterIndex] : '\0';
            var wrapped = (before == '[' && after == ']') || (before == '"' && after == '"');
            if (!wrapped)
            {
                return true;
            }
        }

        // A pass cut short cannot rule EXEC out: the re-execution gate takes the safe direction.
        return statements.Incomplete;
    }

    private static readonly Regex s_remoteRowset = new(
        @"\b(?:OPENQUERY|OPENROWSET|OPENDATASOURCE)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled,
        PlanRegex.DefaultTimeout);

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
            // Display only: a keyword that cannot be checked in time is listed, the safe direction.
            if (PlanRegex.IsMatch(queryText, $@"\b{keyword}\b", RegexOptions.IgnoreCase, "statement keyword scan", onTimeout: true))
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
    /// The caption every "run this captured query to get its actual plan" confirmation uses, so the title says
    /// plainly when the run will change data (or when that could not be ruled out). Shared with
    /// <see cref="BuildConsentWarning"/> so Lite, the shared plan navigation and the Darling viewer word it alike.
    /// </summary>
    public static string BuildConsentTitle(QueryModificationInfo info)
    {
        ArgumentNullException.ThrowIfNull(info);
        return info.ModifiesData
            ? "Get Actual Plan (re-run) — DATA WILL BE MODIFIED"
            : "Get Actual Plan (re-run)";
    }

    /// <summary>The most query text a consent prompt shows.</summary>
    public const int ConsentQueryTextLimit = 1000;

    /// <summary>
    /// The server and database a "run this captured query" confirmation names, worded the same in Lite, the
    /// shared plan navigation and the Darling viewer: <c>server [X], database [Y]</c>. A blank database reads
    /// as the connection's default database; a blank server reads as unknown, so the prompt never hides that.
    /// </summary>
    public static string BuildConsentTarget(string? serverName, string? databaseName)
    {
        var server = string.IsNullOrWhiteSpace(serverName) ? "[unknown]" : $"[{serverName.Trim()}]";
        var database = string.IsNullOrWhiteSpace(databaseName) ? "the default database" : $"[{databaseName.Trim()}]";
        return $"server {server}, database {database}";
    }

    /// <summary>The most lines of query text a consent prompt shows (#5639), so a text of many lines cannot grow the box past the screen.</summary>
    public const int ConsentQueryTextLineLimit = 40;

    /// <summary>
    /// What stands in for a character the prompt will not draw as itself. U+00B7 (a middle dot) is visible in every
    /// font the message box uses and is not a character query text normally holds; "?" would read as SQL.
    /// </summary>
    private const char ConsentUnprintableMarker = (char)0x00B7;

    /// <summary>
    /// The query text a confirmation shows, so the operator reads what will run. This is a DISPLAY copy only: the
    /// text that runs, and the text the data-modification check reads, are the full original.
    /// The header gives the full length BEFORE the text ("first 1,000 of N characters" when it is cut), each shown
    /// line starts with "| " so a line of the text cannot pass for prompt wording, a run of blank lines shows as one,
    /// control characters (tab excepted) and Unicode format characters (bidi controls U+202A-U+202E and
    /// U+2066-U+2069, U+200E, U+200F, U+061C, zero-width characters) show as a middle dot, and at most
    /// <see cref="ConsentQueryTextLineLimit"/> lines show. Whatever is not shown, because of the character limit or
    /// the line limit, is counted in "(N more characters not shown)".
    /// </summary>
    public static string BuildConsentQueryText(string? queryText)
    {
        var text = queryText ?? string.Empty;
        var shown = text.Length <= ConsentQueryTextLimit ? text : text[..ConsentQueryTextLimit];
        if (shown.Length > 0 && shown.Length < text.Length && char.IsHighSurrogate(shown[^1]))
        {
            shown = shown[..^1];
        }

        var lines = new List<string>();
        var consumed = 0;
        var lastWasBlank = false;
        var position = 0;
        while (position < shown.Length)
        {
            var end = shown.IndexOfAny(['\r', '\n'], position);
            var lineEnd = end < 0 ? shown.Length : end;
            var next = lineEnd;
            if (end >= 0)
            {
                next = shown[end] == '\r' && end + 1 < shown.Length && shown[end + 1] == '\n' ? end + 2 : end + 1;
            }

            var line = MakeVisible(shown.Substring(position, lineEnd - position));
            var blank = string.IsNullOrWhiteSpace(line);
            if (!(blank && lastWasBlank))
            {
                if (lines.Count == ConsentQueryTextLineLimit)
                {
                    break;
                }

                lines.Add("| " + line);
            }

            lastWasBlank = blank;
            consumed = next;
            position = next;
        }

        var sb = new StringBuilder();
        sb.Append("Query text that will run (");
        if (shown.Length < text.Length)
        {
            sb.Append("first ").Append(ConsentQueryTextLimit.ToString("N0", CultureInfo.InvariantCulture)).Append(" of ");
        }

        sb.Append(text.Length.ToString("N0", CultureInfo.InvariantCulture)).AppendLine(" characters):");
        foreach (var line in lines)
        {
            sb.AppendLine(line);
        }

        if (consumed < text.Length)
        {
            sb.AppendLine($"({(text.Length - consumed).ToString("N0", CultureInfo.InvariantCulture)} more characters not shown)");
        }

        return sb.ToString();
    }

    /// <summary>One line of query text with each control character (tab excepted), format character (bidi controls, zero-width characters), line or paragraph separator and unpaired surrogate replaced by <see cref="ConsentUnprintableMarker"/>.</summary>
    private static string MakeVisible(string line)
    {
        var sb = new StringBuilder(line.Length);
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            var category = CharUnicodeInfo.GetUnicodeCategory(line, i);
            if (char.IsHighSurrogate(c) && i + 1 < line.Length && char.IsLowSurrogate(line[i + 1]))
            {
                if (category == UnicodeCategory.Format || category == UnicodeCategory.Control)
                {
                    sb.Append(ConsentUnprintableMarker);
                }
                else
                {
                    sb.Append(c).Append(line[i + 1]);
                }

                i++;
            }
            else if (c == '\t')
            {
                sb.Append(c);
            }
            else if (char.IsControl(c) || char.IsSurrogate(c) || category is UnicodeCategory.Format
                or UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator)
            {
                sb.Append(ConsentUnprintableMarker);
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// True when a query hash can be used to look up the stored plan that decides the data-modification check.
    /// An empty or all-zero hash (with or without a 0x prefix) stands for no particular query, so a plan found by
    /// it would say nothing about this text: the caller treats that as "no plan" and the warning shows.
    /// </summary>
    public static bool IsUsableQueryHash(string? queryHash)
    {
        if (string.IsNullOrWhiteSpace(queryHash))
        {
            return false;
        }

        var hash = queryHash.Trim();
        if (hash.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hash = hash[2..];
        }

        return hash.Length > 0 && hash.Any(c => c != '0');
    }

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
