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
using System.Xml.Linq;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Builds paste-ready T-SQL reproduction scripts from query text and plan XML.
/// Extracts parameters from plan XML ParameterList (same approach as sp_QueryReproBuilder).
/// </summary>
public static partial class ReproScriptBuilder
{
    /// <summary>
    /// The valid <c>SET TRANSACTION ISOLATION LEVEL</c> arguments (uppercased). A captured isolation level that
    /// is not one of these — the snapshot collector records <c>Unspecified</c> (level 0) and <c>???</c> (out of
    /// range) for sessions with no explicit level — is NOT valid T-SQL, so the repro skips the SET rather than
    /// emit a statement that fails to parse (the query then runs under the connection's default isolation).
    /// </summary>
    private static readonly HashSet<string> s_validIsolationLevels = new(StringComparer.Ordinal)
    {
        "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE", "SNAPSHOT",
    };

    /// <summary>
    /// Builds a complete reproduction script from available query data.
    /// </summary>
    public static string BuildReproScript(
        string queryText,
        string? databaseName,
        string? planXml,
        string? isolationLevel,
        string source = "Query",
        bool isAzureSqlDb = false,
        string productName = "SQL Server Performance Monitor")
    {
        var sb = new StringBuilder();
        var warnings = new List<string>();

        /* Extract parameters from plan XML if available */
        var parameters = new List<QueryParameter>();
        if (!string.IsNullOrEmpty(planXml))
        {
            parameters = ExtractParametersFromPlan(planXml);
        }
        else
        {
            warnings.Add("Plan XML not available — parameters could not be extracted");
        }

        /* Plan XML is untrusted input: names, types, and compiled values come straight
           off ParameterList attributes. Drop parameters whose name or type isn't a
           plausible T-SQL token before anything is interpolated — the name lands in
           the warning comment and the sp_executesql assignment list. */
        var safeParameters = parameters
            .Where(p => IsValidParameterName(p.Name) && IsValidDataType(p.DataType))
            .ToList();

        /* Check for temp tables and table variables in query text */
        var tempTableWarnings = DetectTempTablesAndTableVariables(queryText);
        warnings.AddRange(tempTableWarnings);

        /* Check for parameters with missing compiled values */
        var missingValueParams = safeParameters.Where(p => string.IsNullOrEmpty(p.CompiledValue)).ToList();
        if (missingValueParams.Count > 0)
        {
            warnings.Add($"Parameters with missing values (set to ?): {string.Join(", ", missingValueParams.Select(p => p.Name))}. Fill in values before executing.");
        }

        /* Values that aren't a single self-contained literal also become ?, so say why
           rather than leaving an unexplained placeholder. */
        var unsafeValueParams = safeParameters
            .Where(p => !string.IsNullOrEmpty(p.CompiledValue) && !IsSafeLiteral(p.CompiledValue))
            .ToList();
        if (unsafeValueParams.Count > 0)
        {
            warnings.Add($"Parameters whose compiled value was not a simple literal (set to ?): {string.Join(", ", unsafeValueParams.Select(p => p.Name))}. Fill in values before executing.");
        }

        /* Parameters dropped entirely because the plan's name or data type wasn't a
           plain T-SQL token — the script would be incomplete, so don't stay silent. */
        var droppedCount = parameters.Count - safeParameters.Count;
        if (droppedCount > 0)
        {
            warnings.Add($"{droppedCount} parameter(s) omitted — the plan's parameter name or data type was not a valid T-SQL identifier. Declare them manually before executing.");
        }

        /* Check for local variables: query has parameter prefix but plan has no/few parameters */
        var trimmedQuery = queryText.Trim();
        var cleanedQuery = StripParameterPrefix(trimmedQuery);
        if (trimmedQuery.StartsWith("(@", StringComparison.Ordinal) && parameters.Count == 0 && !string.IsNullOrEmpty(planXml))
        {
            warnings.Add("Query has parameter declarations but no parameters found in plan. This typically indicates local variables without sniffed values.");
        }

        /* Check for @variables in query text that aren't in the extracted parameters */
        var unresolvedVars = FindUnresolvedVariables(cleanedQuery, parameters);
        if (unresolvedVars.Count > 0)
        {
            warnings.Add($"Variables in query without values: {string.Join(", ", unresolvedVars)}. These may be local variables — fill in values before executing.");
        }

        /* Header comment. Every value in it goes through CommentSafe: the database name
           comes off plan XML, and a crafted one must not be able to end the comment early. */
        sb.AppendLine("/*");
        sb.AppendLine($"Reproduction script generated by {productName}");
        sb.AppendLine($"Source: {CommentSafe(source)}");
        if (!string.IsNullOrEmpty(databaseName))
        {
            sb.AppendLine($"Database: [{CommentSafe(databaseName)}]");
        }
        sb.AppendLine($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");

        if (warnings.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("Warnings:");
            foreach (var warning in warnings)
            {
                sb.AppendLine($" - {CommentSafe(warning)}");
            }
        }

        sb.AppendLine("*/");
        sb.AppendLine();

        /* USE database (skip for Azure SQL DB — USE is invalid there). QUOTENAME-escape the closing bracket so a
           database name is never a script-injection vector — the actual-plan command's database_name is
           payload-supplied, and while the executor also pins it as InitialCatalog, escaping here is defense in
           depth for every repro consumer. Line breaks in the name stay: a client that splits batches
           correctly never splits inside brackets. */
        if (!string.IsNullOrEmpty(databaseName) && !isAzureSqlDb)
        {
            sb.AppendLine($"USE [{databaseName.Replace("]", "]]")}];");
            sb.AppendLine();
        }

        /* SET options from plan XML (match the query's original execution context) */
        if (!string.IsNullOrEmpty(planXml))
        {
            var setOptions = ExtractSetOptionsFromPlan(planXml);
            if (setOptions.Count > 0)
            {
                foreach (var setOption in setOptions)
                {
                    sb.AppendLine(setOption);
                }
            }
        }

        if (!string.IsNullOrEmpty(isolationLevel))
        {
            var normalizedIsolation = isolationLevel.Trim().ToUpperInvariant();
            if (s_validIsolationLevels.Contains(normalizedIsolation))
            {
                sb.AppendLine($"SET TRANSACTION ISOLATION LEVEL {normalizedIsolation};");
            }
        }
        sb.AppendLine("SET NOCOUNT ON;");
        sb.AppendLine();

        /* Query body — wrap in sp_executesql if parameters found. Values that
           aren't a single self-contained literal are emitted as ? so a crafted
           plan can't splice statements into the generated batch. */
        if (safeParameters.Count > 0)
        {
            /* Build parameter declaration and value assignment */
            var paramDecl = string.Join(", ", safeParameters.Select(p => $"{p.Name} {p.DataType}"));
            var paramValues = safeParameters.Select(p =>
            {
                /* Use ? for missing or unsafe values so query can't accidentally run
                   with wrong data */
                var value = string.IsNullOrEmpty(p.CompiledValue) || !IsSafeLiteral(p.CompiledValue)
                    ? "?"
                    : p.CompiledValue;
                return $"    {p.Name} = {value}";
            });

            sb.AppendLine("EXECUTE sys.sp_executesql");
            sb.AppendLine($"    N'{EscapeSqlString(cleanedQuery)}',");
            sb.AppendLine($"    N'{EscapeSqlString(paramDecl)}',");
            sb.AppendLine(string.Join("," + Environment.NewLine, paramValues) + ";");
        }
        else if (!string.IsNullOrEmpty(planXml))
        {
            /* Plan was available but had no parameters — query is not parameterized */
            sb.AppendLine("/* No parameters found in plan cache */");
            sb.AppendLine(cleanedQuery);
            if (!cleanedQuery.EndsWith(';'))
            {
                sb.AppendLine(";");
            }
        }
        else
        {
            /* No plan available at all */
            sb.AppendLine("/* Parameters unknown — plan XML not available */");
            sb.AppendLine(cleanedQuery);
            if (!cleanedQuery.EndsWith(';'))
            {
                sb.AppendLine(";");
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Detects temp tables (#table) and table variables (@table) in query text.
    /// Returns warning messages for any detected objects.
    /// </summary>
    private static List<string> DetectTempTablesAndTableVariables(string queryText)
    {
        var warnings = new List<string>();

        /* Detect temp tables that ALREADY EXIST (not SELECT INTO which creates them):
           FROM #, JOIN #, INSERT #, UPDATE #, DELETE #, MERGE #
           Note: INTO # is excluded because SELECT INTO creates the temp table */
        var tempTablePatterns = new[]
        {
            @"\bFROM\s+#",
            @"\bJOIN\s+#",
            @"\bINSERT\s+(INTO\s+)?#",   /* INSERT # or INSERT INTO # */
            @"\bUPDATE\s+#",
            @"\bDELETE\s+(FROM\s+)?#",   /* DELETE # or DELETE FROM # */
            @"\bMERGE\s+(INTO\s+)?#"     /* MERGE # or MERGE INTO # */
        };

        foreach (var pattern in tempTablePatterns)
        {
            if (Regex.IsMatch(queryText, pattern, RegexOptions.IgnoreCase))
            {
                warnings.Add("Query references temp table(s) — reproduction may require creating the temp table first");
                break;
            }
        }

        /* Detect table variables: FROM @, JOIN @, INSERT @, UPDATE @, DELETE @, MERGE @
           But exclude regular @parameters by checking for table-like usage patterns
           Note: INTO @ is excluded because SELECT INTO @tablevar creates it */
        var tableVarPatterns = new[]
        {
            @"\bFROM\s+@\w+\b(?!\s*=)",      /* FROM @var but not FROM @var = */
            @"\bJOIN\s+@\w+\b",
            @"\bINSERT\s+(INTO\s+)?@\w+\b",  /* INSERT @var or INSERT INTO @var */
            @"\bUPDATE\s+@\w+\b(?!\s*=)",    /* UPDATE @var but not SET @var = */
            @"\bDELETE\s+(FROM\s+)?@\w+\b",  /* DELETE @var or DELETE FROM @var */
            @"\bMERGE\s+(INTO\s+)?@\w+\b"    /* MERGE @var or MERGE INTO @var */
        };

        foreach (var pattern in tableVarPatterns)
        {
            if (Regex.IsMatch(queryText, pattern, RegexOptions.IgnoreCase))
            {
                warnings.Add("Query may reference table variable(s) — reproduction may require declaring and populating the table variable first");
                break;
            }
        }

        return warnings;
    }

    /// <summary>
    /// Extracts parameter names, data types, and compiled values from plan XML.
    /// Parses the ParameterList/ColumnReference elements in the showplan XML namespace.
    /// </summary>
    public static List<QueryParameter> ExtractParametersFromPlan(string planXml)
    {
        var parameters = new List<QueryParameter>();

        try
        {
            var doc = XDocument.Parse(planXml);
            XNamespace ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

            /* Find all ColumnReference elements under ParameterList */
            var paramElements = doc.Descendants(ns + "ParameterList")
                .SelectMany(pl => pl.Elements(ns + "ColumnReference"));

            foreach (var elem in paramElements)
            {
                var name = elem.Attribute("Column")?.Value;
                var dataType = elem.Attribute("ParameterDataType")?.Value;
                var compiledValue = elem.Attribute("ParameterCompiledValue")?.Value;

                if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(dataType))
                {
                    continue;
                }

                /* Strip surrounding parentheses from compiled values: (42) → 42 */
                if (!string.IsNullOrEmpty(compiledValue))
                {
                    compiledValue = StripParentheses(compiledValue);
                    compiledValue = StripGuidWrapper(compiledValue);
                }

                parameters.Add(new QueryParameter
                {
                    Name = name,
                    DataType = dataType,
                    CompiledValue = compiledValue
                });
            }
        }
        catch
        {
            /* Plan XML parse failure — return empty list */
        }

        return parameters;
    }

    /// <summary>
    /// Extracts StatementSetOptions from plan XML and returns ordered SET statements.
    /// These match the execution context the query was originally compiled under.
    /// </summary>
    private static List<string> ExtractSetOptionsFromPlan(string planXml)
    {
        var setStatements = new List<string>();

        try
        {
            var doc = XDocument.Parse(planXml);
            XNamespace ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

            var setOptsEl = doc.Descendants(ns + "StatementSetOptions").FirstOrDefault();
            if (setOptsEl == null) return setStatements;

            /* Emit in conventional order matching SSMS/plan explorer output */
            var options = new (string Attribute, string SetName)[]
            {
                ("ANSI_NULLS", "ANSI_NULLS"),
                ("ANSI_PADDING", "ANSI_PADDING"),
                ("ANSI_WARNINGS", "ANSI_WARNINGS"),
                ("ARITHABORT", "ARITHABORT"),
                ("CONCAT_NULL_YIELDS_NULL", "CONCAT_NULL_YIELDS_NULL"),
                ("QUOTED_IDENTIFIER", "QUOTED_IDENTIFIER"),
                ("NUMERIC_ROUNDABORT", "NUMERIC_ROUNDABORT"),
            };

            foreach (var (attr, setName) in options)
            {
                var value = setOptsEl.Attribute(attr)?.Value;
                if (value != null)
                {
                    var onOff = value is "true" or "1" ? "ON" : "OFF";
                    setStatements.Add($"SET {setName} {onOff};");
                }
            }
        }
        catch
        {
            /* Plan XML parse failure — return empty list, SET options are best-effort */
        }

        return setStatements;
    }

    /// <summary>
    /// Strips the parameter declaration prefix from query text captured via sp_executesql.
    /// Query text like "(@p1 int, @p2 nvarchar(50))SELECT ..." becomes "SELECT ...".
    /// </summary>
    private static string StripParameterPrefix(string queryText)
    {
        /* No list (0), or a list that never closes (-1): the text stays as it is, as before. */
        var bodyStart = DeclarationListEnd(queryText);
        return bodyStart <= 0 ? queryText : queryText[bodyStart..].TrimStart();
    }

    /// <summary>
    /// Where the statement starts in text that opens with an <c>sp_executesql</c> declaration list,
    /// such as <c>(@p1 int, @p2 decimal(18,2))SELECT …</c>. Returns 0 when the text has no list,
    /// and -1 when the list never closes: a plan cuts statement text off at 4,000 characters, and
    /// the declarations for a long IN list can fill all of them. The list ends at the parenthesis
    /// that closes its first one, so the parentheses of a type are counted, not taken for the end.
    /// A statement cannot begin with <c>(@</c>, so that opening always means a list.
    /// </summary>
    private static int DeclarationListEnd(string text)
    {
        var i = 0;
        while (i < text.Length && char.IsWhiteSpace(text[i]))
            i++;

        if (i + 1 >= text.Length || text[i] != '(' || text[i + 1] != '@')
            return 0;

        var depth = 0;
        for (; i < text.Length; i++)
        {
            if (text[i] == '(')
                depth++;
            else if (text[i] == ')' && --depth == 0)
                return i + 1;
        }

        return -1; // the list never closes: the text was cut off inside it
    }

    /// <summary>
    /// Strips surrounding parentheses from a compiled value.
    /// Example: "(42)" → "42", "((1))" → "(1)"
    /// </summary>
    private static string StripParentheses(string value)
    {
        if (value.Length >= 2 && value[0] == '(' && value[^1] == ')')
        {
            return value[1..^1];
        }
        return value;
    }

    /// <summary>
    /// Strips GUID wrapper notation from compiled values.
    /// Example: "{guid'AB12CD34-...'}" → "'AB12CD34-...'"
    /// </summary>
    private static string StripGuidWrapper(string value)
    {
        if (value.StartsWith("{guid'", StringComparison.OrdinalIgnoreCase) && value.EndsWith("'}", StringComparison.Ordinal))
        {
            return "'" + value[6..^2] + "'";
        }
        return value;
    }

    /// <summary>
    /// Escapes single quotes in SQL strings for use inside N'...' literals.
    /// </summary>
    private static string EscapeSqlString(string value)
    {
        return value.Replace("'", "''");
    }

    /// <summary>
    /// Makes text safe inside the header's block comment. "*/" would close the comment and
    /// "/*" would open a nested one (T-SQL block comments nest), so both are split with a
    /// space. Line breaks become spaces too, so each value stays on one line of the header
    /// and cannot put GO on a line of its own there.
    /// </summary>
    private static string CommentSafe(string? text)
    {
        /* \p{Cc} covers CR, LF, tab and NEL; U+2028 and U+2029 are the Unicode line and
           paragraph separators, which some editors also treat as line breaks. */
        return Regex.Replace(text ?? "", @"[\p{Cc}\u2028\u2029]", " ")
            .Replace("*/", "* /")
            .Replace("/*", "/ *");
    }

    /// <summary>
    /// Validates a parameter name from plan XML as a plain @identifier.
    /// Anything else is dropped from the generated script.
    /// </summary>
    private static bool IsValidParameterName(string name)
    {
        return Regex.IsMatch(name, @"^@[\p{L}_@#$][\p{L}\p{Nd}_@#$]*$");
    }

    /// <summary>
    /// Validates a parameter data type from plan XML: type name with optional
    /// schema prefix, brackets, and (size/precision) suffix. No quotes or comment
    /// characters, so it can't disturb the sp_executesql declaration list.
    /// </summary>
    private static bool IsValidDataType(string dataType)
    {
        return Regex.IsMatch(dataType, @"^[\p{L}\p{Nd}_\[\]., ()]+$");
    }

    /// <summary>
    /// Accepts a compiled value only when it's a single self-contained literal:
    /// NULL, a number, a 0x binary value, or one complete N'...' string with every
    /// embedded quote doubled. Such values can't escape the @param = value slot.
    /// </summary>
    private static bool IsSafeLiteral(string value)
    {
        if (value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return true;

        /* Integer/decimal/float/money forms: -12, 3.14, 1.5E+3, $9.99 */
        if (Regex.IsMatch(value, @"^-?\$?\d+(\.\d+)?([eE][+-]?\d+)?$"))
            return true;

        /* Binary literal */
        if (Regex.IsMatch(value, @"^0x[0-9A-Fa-f]*$"))
            return true;

        /* One complete string literal — every embedded quote must be doubled */
        if (Regex.IsMatch(value, @"^N?'([^']|'')*'$"))
            return true;

        return false;
    }

    /// <summary>
    /// Finds @variables in query text that aren't in the extracted parameter list.
    /// These are likely local variables without sniffed values.
    /// </summary>
    private static List<string> FindUnresolvedVariables(string queryText, List<QueryParameter> parameters)
    {
        var unresolved = new List<string>();
        var extractedNames = new HashSet<string>(parameters.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);

        /* Find all @variable references in the query text */
        var matches = AtVariableRegExp().Matches(queryText);
        var seenVars = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (Match match in matches)
        {
            var varName = match.Value;

            /* Skip if already seen or if it's in the extracted parameters */
            if (seenVars.Contains(varName) || extractedNames.Contains(varName))
            {
                continue;
            }

            /* Skip common system variables */
            if (varName.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase) ||
                varName.Equals("@@ERROR", StringComparison.OrdinalIgnoreCase) ||
                varName.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase) ||
                varName.Equals("@@TRANCOUNT", StringComparison.OrdinalIgnoreCase) ||
                varName.Equals("@@FETCH_STATUS", StringComparison.OrdinalIgnoreCase) ||
                varName.StartsWith("@@", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            seenVars.Add(varName);
            unresolved.Add(varName);
        }

        return unresolved;
    }

    [GeneratedRegex(@"@\w+", RegexOptions.IgnoreCase)]
    private static partial Regex AtVariableRegExp();
}

/// <summary>
/// Represents a parameter extracted from a query plan's ParameterList.
/// </summary>
public class QueryParameter
{
    public string Name { get; set; } = "";
    public string DataType { get; set; } = "";
    public string? CompiledValue { get; set; }
}
