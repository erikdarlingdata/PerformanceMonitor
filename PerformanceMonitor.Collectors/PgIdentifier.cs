/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// PostgreSQL identifier quoting for the relation names Darling prints into commands it invites an
/// operator to run — <c>pgstatindex</c> and <c>pgstattuple</c> today.
///
/// <para>It lives in this project because it is the only one both the Darling STORAGE readers and the
/// Darling SERVICE MCP tools already reference, and because this is where the PostgreSQL SQL it quotes
/// for is authored. Putting it in Common would have meant widening Storage's dependency graph.</para>
///
/// <para><b>Shared rather than copied, because it was copied and one copy was wrong.</b> The index and
/// table bloat surfaces each concatenated raw catalog text into a single-quoted literal; a PR review
/// caught the index one and the table one had shipped that way. One rule, one implementation.</para>
/// </summary>
public static class PgIdentifier
{
    /// <summary>
    /// A schema-qualified relation name, each part quoted as an identifier and the whole escaped for the
    /// single-quoted SQL string literal it will sit inside.
    ///
    /// <para><b>Both levels of escaping are load-bearing.</b> These names are handed to functions taking
    /// <c>regclass</c>, whose TEXT input parses like an identifier: an unquoted part is folded to lower
    /// case, so a camelCase or reserved-word relation resolves to a different object or to none. And a
    /// name created through a double-quoted <c>CREATE</c> may contain almost any character INCLUDING a
    /// single quote, which would close the literal and append whatever follows to a command an operator
    /// is being invited to paste into a privileged session.</para>
    ///
    /// <para>Quoting is UNCONDITIONAL rather than applied when a name looks like it needs it: deciding
    /// that means reimplementing PostgreSQL's folding rules and its keyword list, and being wrong in the
    /// direction that omits the quotes is the direction that silently resolves to the wrong object.</para>
    /// </summary>
    public static string Qualify(string? schemaName, string? relationName) =>
        (Quote(schemaName ?? "public") + "." + Quote(relationName))
            .Replace("'", "''", StringComparison.Ordinal);

    /// <summary><c>quote_ident</c>'s rule: wrap in double quotes, double any embedded double quote.</summary>
    public static string Quote(string? part) =>
        "\"" + (part ?? string.Empty).Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
