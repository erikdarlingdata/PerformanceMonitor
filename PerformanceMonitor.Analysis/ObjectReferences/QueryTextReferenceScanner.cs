/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * The local-name visitor is ported from SQLExtended's ModuleLocalNameScanner (MIT, Copyright (c) 2026 James Noonan).
 */

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace PerformanceMonitor.Analysis.ObjectReferences;

/// <summary>What a table source in a statement names: a table or view, or a table-valued function.</summary>
public enum ScannedReferenceKind
{
    /// <summary>A table or view (<c>NamedTableReference</c>): <c>FROM dbo.v</c>, an UPDATE, INSERT, DELETE or MERGE target.</summary>
    TableOrView,

    /// <summary>A function: a table-valued one as a table source (<c>SchemaObjectFunctionTableReference</c>, <c>FROM dbo.f(1)</c>), or a
    /// scalar one called in an expression (<c>FunctionCall</c> on a schema, <c>SELECT dbo.f(id)</c>).</summary>
    Function,
}

/// <summary>One object a statement names as a table source, as written: each part is null where the statement left it out.</summary>
/// <param name="Server">The linked server part, or null.</param>
/// <param name="Database">The database part, or null.</param>
/// <param name="Schema">The schema part, or null.</param>
/// <param name="Name">The object name.</param>
/// <param name="Kind">Whether it is named as a table or view, or as a table-valued function.</param>
public readonly record struct ScannedReference(string? Server, string? Database, string? Schema, string Name, ScannedReferenceKind Kind);

/// <summary>The outcome of scanning one statement.</summary>
/// <param name="Parsed">True where the statement parsed cleanly. False where it did not (or was too long to try), and the caller falls back to
/// a whole-word text search (<see cref="SqlWholeWordMatcher"/>).</param>
/// <param name="References">The objects named, each once. Empty when <paramref name="Parsed"/> is false.</param>
public sealed record QueryTextScan(bool Parsed, IReadOnlyList<ScannedReference> References)
{
    /// <summary>For an unparsed statement held in a <c>QueryTextScanCache</c>, its words (<see cref="SqlWholeWordMatcher.Words"/>), so the text
    /// search does not split the same text again each day it comes back (#8784 review). Null otherwise.</summary>
    public IReadOnlySet<string>? Words { get; init; }

    /// <summary>The scan of a statement that could not be parsed.</summary>
    public static QueryTextScan Unparsed { get; } = new(false, Array.Empty<ScannedReference>());
}

/// <summary>
/// Finds the objects a Query Store statement names as table sources (#8784 part 2), for the view usage read: a view that is not indexed is
/// expanded at compile time, so its use moves only its base tables' counters, and the statement text is the one place its name is left.
///
/// <para><b>Parse, don't search.</b> The statement is parsed with ScriptDom and every <c>NamedTableReference</c> and
/// <c>SchemaObjectFunctionTableReference</c> is collected, so a name inside a comment or a string literal never counts, and neither does a
/// column or a variable that happens to share the name.</para>
///
/// <para><b>Local names are left out.</b> A statement can define names of its own: common table expressions and the aliases of table
/// sources, derived tables and table variables. An unqualified reference to one of them (<c>FROM Orders</c> where <c>Orders</c> is a CTE, or
/// the target of <c>UPDATE o ... FROM dbo.Orders AS o</c>) is not a use of a view of that name, so it is dropped. A reference that carries
/// its own alias is kept even when another source uses its name as an alias, and a schema-qualified reference is never local. Temp tables
/// (<c>#t</c>) and table variables (<c>@t</c>) are dropped whatever they are called. Where a name could be either, it is kept: a reference
/// counted wrongly only makes a view look used.</para>
///
/// <para><b>The parameter list is stripped first.</b> Query Store stores a parameterised statement as <c>(@p1 int,@p2 nvarchar(50))SELECT
/// ...</c>, which is not valid T-SQL until the leading list is removed.</para>
///
/// <para><b>A bare condition gets an empty body.</b> Query Store keeps the condition of an <c>IF</c> or a <c>WHILE</c> as a statement of
/// its own, with no body; such a statement is parsed again with an empty <c>BEGIN RETURN; END</c> after it, which names nothing.</para>
///
/// <para><b>Quoted identifiers.</b> The statement is parsed with <c>QUOTED_IDENTIFIER ON</c> first (the default for every modern client),
/// and again with it OFF if that fails, since a statement written for OFF can use double quotes for strings.</para>
///
/// <para>A statement that still does not parse, or is longer than <see cref="MaxParseLength"/>, comes back with
/// <see cref="QueryTextScan.Parsed"/> false, and the caller searches its text instead. A partial parse tree is not used: names after the
/// error would be missed, and a missed name can make a used view look unused, the one answer this read must not give.</para>
/// </summary>
public static class QueryTextReferenceScanner
{
    /// <summary>Above this length a statement is not parsed: the text search is cheaper and no less safe.</summary>
    public const int MaxParseLength = 1_000_000;

    /// <summary>Scans one statement.</summary>
    public static QueryTextScan Scan(string? statementText)
    {
        if (string.IsNullOrWhiteSpace(statementText) || statementText.Length > MaxParseLength)
        {
            return QueryTextScan.Unparsed;
        }

        var body = StripParameterList(statementText);
        var scan = TryScan(body, quotedIdentifiers: true) ?? TryScan(body, quotedIdentifiers: false);
        if (scan is null && IsBareCondition(body))
        {
            /* Query Store keeps the condition of an IF or a WHILE as a statement of its own (IF EXISTS (SELECT ... FROM dbo.v)), with no body,
               which is not valid T-SQL alone. An empty body that names nothing makes it parse, and adds no name. */
            var completed = body + "\nBEGIN\n    RETURN;\nEND;";
            scan = TryScan(completed, quotedIdentifiers: true) ?? TryScan(completed, quotedIdentifiers: false);
        }

        return scan ?? QueryTextScan.Unparsed;
    }

    /// <summary>Whether <paramref name="body"/> starts with the keyword <c>IF</c> or <c>WHILE</c>, as a whole word.</summary>
    internal static bool IsBareCondition(string body)
    {
        var text = body.AsSpan().TrimStart();
        foreach (var keyword in (ReadOnlySpan<string>)["IF", "WHILE"])
        {
            if (text.StartsWith(keyword, StringComparison.OrdinalIgnoreCase)
                && (text.Length == keyword.Length || !SqlWholeWordMatcher.IsWordChar(text[keyword.Length])))
            {
                return true;
            }
        }

        return false;
    }

    private static QueryTextScan? TryScan(string body, bool quotedIdentifiers)
    {
        TSqlFragment? fragment;
        try
        {
            var parser = new TSql170Parser(quotedIdentifiers);
            using var reader = new StringReader(body);
            fragment = parser.Parse(reader, out var errors);
            if (errors is { Count: > 0 })
            {
                return null;
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return null;
        }

        if (fragment is null)
        {
            return null;
        }

        var visitor = new ReferenceVisitor();
        try
        {
            fragment.Accept(visitor);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            /* A pathological tree can overflow the visitor's recursion guard; the text search is the safe answer then. */
            return null;
        }

        var seen = new HashSet<ScannedReference>(ScannedReferenceComparer.Instance);
        var references = new List<ScannedReference>();
        foreach (var (r, hasAlias) in visitor.References)
        {
            if (r.Name.Length == 0 || r.Name[0] is '#' or '@')
            {
                continue;
            }

            /* An unqualified name that is a CTE of the statement is the CTE. One that is another table source's alias is that source (the
               target of UPDATE o ... FROM dbo.Orders AS o), unless this reference carries an alias of its own: FROM Orders AS Orders names
               Orders and only then defines the alias. */
            var unqualified = r.Schema is null && r.Database is null && r.Server is null;
            if (unqualified && (visitor.CteNames.Contains(r.Name) || (!hasAlias && visitor.AliasNames.Contains(r.Name))))
            {
                continue;
            }

            if (seen.Add(r))
            {
                references.Add(r);
            }
        }

        return new QueryTextScan(true, references);
    }

    /// <summary>
    /// Removes the parameter list Query Store puts in front of a parameterised statement: a leading <c>(</c> whose first token is a
    /// <c>@</c> name, up to its matching <c>)</c>. Brackets inside the list (<c>decimal(10,2)</c>, <c>nvarchar(max)</c>) and quoted text are
    /// skipped. Returns the text unchanged when it does not start with such a list or the list never closes.
    /// </summary>
    public static string StripParameterList(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        var i = 0;
        while (i < text.Length && char.IsWhiteSpace(text[i]))
        {
            i++;
        }

        if (i >= text.Length || text[i] != '(')
        {
            return text;
        }

        var j = i + 1;
        while (j < text.Length && char.IsWhiteSpace(text[j]))
        {
            j++;
        }

        if (j >= text.Length || text[j] != '@')
        {
            return text;
        }

        var depth = 0;
        for (var k = i; k < text.Length; k++)
        {
            var c = text[k];
            if (c is '\'' or '[' or '"')
            {
                var close = c == '[' ? ']' : c;
                k++;
                while (k < text.Length)
                {
                    if (text[k] == close)
                    {
                        /* A doubled closing character is an escaped one inside the quoted text. */
                        if (k + 1 < text.Length && text[k + 1] == close)
                        {
                            k += 2;
                            continue;
                        }

                        break;
                    }

                    k++;
                }

                continue;
            }

            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
                if (depth == 0)
                {
                    return text[(k + 1)..];
                }
            }
        }

        return text;
    }

    private sealed class ScannedReferenceComparer : IEqualityComparer<ScannedReference>
    {
        public static ScannedReferenceComparer Instance { get; } = new();

        /* Ignore case: one statement writing dbo.Orders and DBO.ORDERS names one object on every database where both parse, and the
           read that matches these to views ignores case anyway. */
        public bool Equals(ScannedReference x, ScannedReference y) =>
            x.Kind == y.Kind
            && string.Equals(x.Server, y.Server, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Database, y.Database, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode(ScannedReference obj) =>
            HashCode.Combine(obj.Kind, StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Name));
    }

    private sealed class ReferenceVisitor : TSqlFragmentVisitor
    {
        public List<(ScannedReference Reference, bool HasAlias)> References { get; } = new();

        public HashSet<string> CteNames { get; } = new(StringComparer.OrdinalIgnoreCase);

        public HashSet<string> AliasNames { get; } = new(StringComparer.OrdinalIgnoreCase);

        private void AddAlias(string? name)
        {
            if (!string.IsNullOrEmpty(name))
            {
                AliasNames.Add(name);
            }
        }

        private void AddReference(SchemaObjectName? name, ScannedReferenceKind kind, Identifier? alias)
        {
            var baseName = name?.BaseIdentifier?.Value;
            if (string.IsNullOrEmpty(baseName))
            {
                return;
            }

            References.Add((new ScannedReference(
                NullIfEmpty(name!.ServerIdentifier?.Value),
                NullIfEmpty(name.DatabaseIdentifier?.Value),
                NullIfEmpty(name.SchemaIdentifier?.Value),
                baseName,
                kind), !string.IsNullOrEmpty(alias?.Value)));
        }

        private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

        public override void Visit(NamedTableReference node)
        {
            AddReference(node.SchemaObject, ScannedReferenceKind.TableOrView, node.Alias);
            AddAlias(node.Alias?.Value);
        }

        public override void Visit(SchemaObjectFunctionTableReference node)
        {
            /* CROSS APPLY t.doc.nodes('/r') is an XML type method on a column, not a function of schema "doc". */
            if (node.SchemaObject?.SchemaIdentifier is not null && XmlTypeMethodNames.Contains(node.SchemaObject.BaseIdentifier?.Value ?? ""))
            {
                AddAlias(node.Alias?.Value);
                return;
            }

            AddReference(node.SchemaObject, ScannedReferenceKind.Function, node.Alias);
            AddAlias(node.Alias?.Value);
        }

        /* A scalar UDF call is always schema-qualified (dbo.f(x), or db.dbo.f(x)), so it is a FunctionCall whose call target is a one- or
           two-part identifier (#8784 review). On SQL Server 2019 and later an inlined scalar UDF gets no executions in Query Store, so the
           statement naming it is the only place the dependency walk can start from to reach a view the function reads. A method call on a
           column of a CLR or spatial type (t.geo.STDistance(...)) has the same shape: recorded too, it names nothing that exists and only
           costs a lookup. The XML type methods are left out, as the dependency filter leaves them out. */
        public override void Visit(FunctionCall node)
        {
            var name = node.FunctionName?.Value;
            if (string.IsNullOrEmpty(name)
                || node.CallTarget is not MultiPartIdentifierCallTarget { MultiPartIdentifier.Identifiers: { Count: 1 or 2 } parts }
                || XmlTypeMethodNames.Contains(name))
            {
                return;
            }

            References.Add((new ScannedReference(
                Server: null,
                Database: parts.Count == 2 ? NullIfEmpty(parts[0].Value) : null,
                Schema: NullIfEmpty(parts[^1].Value),
                Name: name,
                Kind: ScannedReferenceKind.Function), HasAlias: false));
        }

        private static readonly HashSet<string> XmlTypeMethodNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "value", "exist", "query", "nodes", "modify",
        };

        public override void Visit(QueryDerivedTable node) => AddAlias(node.Alias?.Value);

        public override void Visit(InlineDerivedTable node) => AddAlias(node.Alias?.Value);

        public override void Visit(VariableTableReference node) => AddAlias(node.Alias?.Value);

        public override void Visit(CommonTableExpression node)
        {
            var name = node.ExpressionName?.Value;
            if (!string.IsNullOrEmpty(name))
            {
                CteNames.Add(name);
            }
        }
    }
}
