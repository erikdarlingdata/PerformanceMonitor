/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The shape of a SQL statement, for telling the text <c>pg_stat_statements</c> stored from the text the service sent (#7681).
///
/// <para><b>Why a tokenizer.</b> <c>pg_stat_statements</c> stores the statement as the first call wrote it, with every literal
/// replaced by a numbered placeholder. The service's C# source has the literals, or its own <c>$n</c> parameters, so the two
/// strings are never equal. Both sides go through the one tokenizer below and the token lists are compared, so there is no
/// second set of rules to drift.</para>
///
/// <para><b>What the tokens ignore</b> (each measured on PostgreSQL 18.6): whitespace and comments, including a comment inside
/// the statement, which the server keeps, and the <c>/*, ... */</c> it writes into a squashed constant list; the case of an
/// unquoted identifier or keyword (a quoted identifier keeps its case); a trailing semicolon, which the server cuts from the
/// text of a statement in a batch.</para>
///
/// <para><b>What every literal becomes.</b> One wildcard token: a number, a string (with <c>''</c> escapes, <c>E''</c>, <c>B''</c>,
/// <c>X''</c>, <c>N''</c>), a dollar-quoted string, a <c>$n</c> parameter, and <c>true</c>, <c>false</c> and <c>null</c> (PostgreSQL
/// turns all three into placeholders too, so they are the wildcard on both sides). A <c>-</c> before a wildcard is dropped: the
/// server folds a unary minus into its constant (<c>-2</c> is stored as <c>$2</c>) and keeps a binary one (<c>5 - -4</c> is
/// <c>$6 - $7</c>), and a source text cannot tell the two apart without a keyword list, so neither side keeps a minus there.
/// Two wildcards in a row are one wildcard (adjacent string literals are one literal). The one exception is the dollar-quoted
/// body of a <c>DO</c> statement, which the server does not normalize: each body is its own entry (measured on 18.6), so the
/// body stays in the shape.</para>
///
/// <para><b>What a constant list becomes.</b> PostgreSQL 18 squashes a list of constants to <c>($1 /*, ... */)</c>, and earlier
/// versions write every element out. Only the two lists it squashes are collapsed here, to one wildcard: the parentheses after
/// <c>IN</c> and the brackets after <c>ARRAY</c> (<c>= ANY (ARRAY[...])</c> is the same list). Everywhere else a comma stays a
/// comma, so <c>SELECT 1</c> and <c>SELECT 1, 2</c>, or <c>f($1)</c> and <c>f($1, $2)</c>, are different shapes, as they are
/// different entries in <c>pg_stat_statements</c> (measured on 18.6: a <c>VALUES</c> row list, a function's arguments and a
/// select list are all written out). A statement with a <c>;</c> in it is several statements, which the server stores one by one
/// (<see cref="Keys"/>).</para>
/// </summary>
public static class PgStatementShape
{
    /// <summary>The token every literal becomes. A lone dollar sign is not a SQL token, so no real token equals it.</summary>
    public const string Wildcard = "$";

    /// <summary>The shape as one string: the tokens joined with NUL, which neither an identifier nor a literal can contain.</summary>
    public static string Key(string sql) => string.Join('\0', Tokenize(sql));

    /// <summary>
    /// The shape of each statement in <paramref name="sql"/>, split at the <c>;</c> tokens (#7681). <c>pg_stat_statements</c> stores
    /// a statement of a multi-statement string on its own, so the shape of the whole string matches no stored text. A <c>;</c>
    /// inside a string literal or a dollar-quoted body is part of that one wildcard token, not a split point. An empty statement
    /// (a doubled or trailing semicolon) has no shape and is left out.
    /// </summary>
    public static List<string> Keys(string sql)
    {
        var keys = new List<string>();
        var statement = new List<string>();
        foreach (var token in Tokenize(sql))
        {
            if (token != ";")
            {
                statement.Add(token);
            }
            else if (statement.Count > 0)
            {
                keys.Add(string.Join('\0', statement));
                statement.Clear();
            }
        }

        if (statement.Count > 0)
        {
            keys.Add(string.Join('\0', statement));
        }

        return keys;
    }

    /// <summary>The statement's tokens, with literals as <see cref="Wildcard"/>.</summary>
    public static List<string> Tokenize(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        var tokens = new List<string>();
        /* One flag per open ( or [: whether it opens a list PostgreSQL 18 squashes (IN ( or ARRAY[). */
        var lists = new List<bool>();
        /* Where the current statement's tokens start, to tell a DO statement. */
        var statementStart = 0;
        var n = sql.Length;
        var i = 0;
        while (i < n)
        {
            var c = sql[i];
            if (char.IsWhiteSpace(c))
            {
                i++;
            }
            else if (c == '-' && i + 1 < n && sql[i + 1] == '-')
            {
                while (i < n && sql[i] != '\n')
                {
                    i++;
                }
            }
            else if (c == '/' && i + 1 < n && sql[i + 1] == '*')
            {
                i = SkipBlockComment(sql, i);
            }
            else if (c == '\'')
            {
                i = SkipString(sql, i, backslashEscapes: false);
                AddWildcard(tokens, lists);
            }
            else if (c == '"')
            {
                var start = i;
                i++;
                while (i < n)
                {
                    if (sql[i] == '"')
                    {
                        if (i + 1 < n && sql[i + 1] == '"')
                        {
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    i++;
                }

                tokens.Add(sql[start..i]);
            }
            else if (IsIdentifierStart(c))
            {
                var start = i;
                i++;
                while (i < n && IsIdentifierPart(sql[i]))
                {
                    i++;
                }

                /* A one-letter prefix right before a quote is part of the literal: E'..', B'..', X'..', N'..'. */
                if (i - start == 1 && i < n && sql[i] == '\'' && "eEbBxXnN".Contains(c, StringComparison.Ordinal))
                {
                    i = SkipString(sql, i, backslashEscapes: c is 'e' or 'E');
                    AddWildcard(tokens, lists);
                    continue;
                }

                var word = sql[start..i].ToLowerInvariant();
                if (word is "true" or "false" or "null")
                {
                    AddWildcard(tokens, lists);
                }
                else
                {
                    tokens.Add(word);
                }
            }
            else if (char.IsAsciiDigit(c) || (c == '.' && i + 1 < n && char.IsAsciiDigit(sql[i + 1])))
            {
                i++;
                while (i < n && (char.IsAsciiLetterOrDigit(sql[i]) || sql[i] == '.' || sql[i] == '_'))
                {
                    /* An exponent's sign belongs to the number: 1e-3. */
                    if ((sql[i] is 'e' or 'E') && i + 1 < n && sql[i + 1] is '+' or '-')
                    {
                        i++;
                    }

                    i++;
                }

                AddWildcard(tokens, lists);
            }
            else if (c == '$')
            {
                if (i + 1 < n && char.IsAsciiDigit(sql[i + 1]))
                {
                    i++;
                    while (i < n && char.IsAsciiDigit(sql[i]))
                    {
                        i++;
                    }

                    AddWildcard(tokens, lists);
                    continue;
                }

                var tagEnd = i + 1;
                if (tagEnd < n && IsIdentifierStart(sql[tagEnd]) && sql[tagEnd] != '$')
                {
                    while (tagEnd < n && IsIdentifierPart(sql[tagEnd]) && sql[tagEnd] != '$')
                    {
                        tagEnd++;
                    }
                }

                if (tagEnd < n && sql[tagEnd] == '$')
                {
                    var tag = sql.Substring(i, tagEnd - i + 1);
                    var close = sql.IndexOf(tag, tagEnd + 1, StringComparison.Ordinal);
                    var bodyEnd = close < 0 ? n : close + tag.Length;
                    if (tokens.Count > statementStart && tokens[statementStart] == "do")
                    {
                        /* The server does not normalize a DO block: it stores the body as written, and each body is its own
                           entry, so the body is part of the shape. */
                        tokens.Add(sql[i..bodyEnd]);
                    }
                    else
                    {
                        AddWildcard(tokens, lists);
                    }

                    i = bodyEnd;
                }
                else
                {
                    i++;
                }
            }
            else if (IsOperatorChar(c))
            {
                var start = i;
                while (i < n && IsOperatorChar(sql[i]))
                {
                    if ((sql[i] == '-' && i + 1 < n && sql[i + 1] == '-') || (sql[i] == '/' && i + 1 < n && sql[i + 1] == '*'))
                    {
                        break;
                    }

                    i++;
                }

                /* The lexer's own rule: a multi-character operator does not end in + or - unless it holds one of ~!@#^&|`?%,
                   so 2 *-3 is the operator * and then -3. */
                var length = i - start;
                while (length > 1 && sql[start + length - 1] is '+' or '-')
                {
                    var holdsSpecial = false;
                    for (var k = start + length - 2; k >= start; k--)
                    {
                        if ("~!@#^&|`?%".Contains(sql[k], StringComparison.Ordinal))
                        {
                            holdsSpecial = true;
                            break;
                        }
                    }

                    if (holdsSpecial)
                    {
                        break;
                    }

                    length--;
                }

                i = start + length;
                tokens.Add(sql.Substring(start, length));
            }
            else
            {
                if (c is '(' or '[')
                {
                    lists.Add(tokens.Count > 0 && (c == '(' ? tokens[^1] == "in" : tokens[^1] == "array"));
                }
                else if ((c is ')' or ']') && lists.Count > 0)
                {
                    lists.RemoveAt(lists.Count - 1);
                }

                tokens.Add(c.ToString());
                if (c == ';')
                {
                    statementStart = tokens.Count;
                }

                i++;
            }
        }

        while (tokens.Count > 0 && tokens[^1] == ";")
        {
            tokens.RemoveAt(tokens.Count - 1);
        }

        return tokens;
    }

    private static void AddWildcard(List<string> tokens, List<bool> lists)
    {
        while (tokens.Count > 0 && tokens[^1] == "-")
        {
            tokens.RemoveAt(tokens.Count - 1);
        }

        if (tokens.Count > 0 && tokens[^1] == Wildcard)
        {
            return;
        }

        /* A comma between two wildcards is dropped only inside a list PostgreSQL 18 squashes. */
        if (lists.Count > 0 && lists[^1] && tokens.Count > 1 && tokens[^1] == "," && tokens[^2] == Wildcard)
        {
            tokens.RemoveAt(tokens.Count - 1);
            return;
        }

        tokens.Add(Wildcard);
    }

    private static int SkipBlockComment(string sql, int i)
    {
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

        return i;
    }

    /// <summary>The index after the string literal that opens with a quote at <paramref name="i"/>.</summary>
    private static int SkipString(string sql, int i, bool backslashEscapes)
    {
        i++;
        while (i < sql.Length)
        {
            if (backslashEscapes && sql[i] == '\\')
            {
                i += 2;
                continue;
            }

            if (sql[i] == '\'')
            {
                if (i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return sql.Length;
    }

    private static bool IsIdentifierStart(char c) => char.IsAsciiLetter(c) || c == '_' || c > '\u007f';

    private static bool IsIdentifierPart(char c) => char.IsAsciiLetterOrDigit(c) || c == '_' || c == '$' || c > '\u007f';

    private static bool IsOperatorChar(char c) => "+-*/<>=~!@#%^&|`?".Contains(c, StringComparison.Ordinal);
}
