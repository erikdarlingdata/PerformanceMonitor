/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;

namespace Darling.Tests;

/// <summary>
/// <para>Decides, for every character of a C# file, whether it is CODE — so a regex meant for code cannot
/// match prose in a comment or text in a literal, and cannot MISS code that happens to be written inside an
/// interpolation. Five source-walking pins were each carrying their own copy of this (#2913); this is the one
/// implementation they now share, the same treatment <see cref="IlCallSiteScanner"/> gave the five copies of
/// the IL walk in #2898.</para>
///
/// <para><b>The gap that prompted this.</b> Every copy blanked the ENTIRE span of a string literal, holes
/// included, so <c>Log($"count={Foo()}")</c> had no <c>Foo()</c> in it as far as any scan built on the walk
/// was concerned. That produced no known false negative at the time — nothing on the fleet-timer fan-out
/// called through an interpolation — but the failure direction is the dangerous one. These pins assert that a
/// count is ZERO or that a path does not exist, and an edge the walk cannot see satisfies both vacuously. A
/// pass for the wrong reason is worth less than a failure.</para>
///
/// <para><b>Two of the gaps were not latent.</b> Raw string literals did not exist for the old walk at all:
/// it read the opening <c>"""</c> as an empty string followed by the start of another one, so the delimiter
/// itself desynchronised the parse. And a <c>char</c> literal was plain code, which makes
/// <c>value.Contains('"')</c> — a real line in <c>ViewerServerTab.ChartContextMenu.cs</c>, inside the
/// directory both viewer pins glob — open a string that runs to the next <c>"</c> anywhere in the file,
/// blanking whatever code lies between. Four of the five copies had no newline stop, so that span was
/// bounded only by the next quote; the fifth had grown one, which is the other half of the argument for
/// having one copy: the copies had already drifted, and only one of them had the hardening.</para>
///
/// <para><b>Hole DELIMITERS are not code, hole CONTENTS are.</b> The <c>{</c> and <c>}</c> around a hole,
/// the format specifier after a top-level <c>:</c>, and the <c>$</c>/<c>@</c>/<c>"</c> of the token itself
/// are all blanked; only the expression is kept. Keeping the braces would be worse than blanking the hole:
/// <see cref="StripCommentsAndStrings"/>'s output is brace-matched by callers to find method bodies, and a
/// literal <c>}</c> in a string would unbalance the match. Blanking the format specifier is what keeps
/// <c>$"{count:N0}"</c> from putting <c>N0</c> into the code stream, which is the false-positive direction —
/// harmless for today's regexes, and a false positive fails a green build on correct code.</para>
///
/// <para><b>Newlines survive everything</b>, because the pins report offenders by line number and compute
/// that line by counting <c>\n</c> in the stripped text up to the match.</para>
/// </summary>
internal static class CSharpSourceWalker
{
    /// <summary>Which flavour of literal a frame is scanning, which decides how it ENDS.</summary>
    private enum LiteralKind
    {
        /// <summary><c>"..."</c> — <c>\</c> escapes, and a newline ends it because a non-verbatim string
        /// cannot span lines. Stopping at the newline is what bounds the damage from a mis-read opener.</summary>
        Regular,

        /// <summary><c>@"..."</c> — no <c>\</c> escapes, <c>""</c> is an escaped quote, newlines allowed.</summary>
        Verbatim,

        /// <summary><c>"""..."""</c> — no escapes at all; ends at a quote run at least as long as the
        /// opening delimiter.</summary>
        Raw,
    }

    /// <summary>
    /// One open literal. <c>Dollars</c> is the count of <c>$</c> on the opener and is therefore how many
    /// consecutive braces open a hole; <c>QuoteRun</c> is the raw delimiter's length.
    /// </summary>
    private readonly record struct Frame(LiteralKind Kind, int Dollars, int QuoteRun);

    /// <summary>
    /// A <c>bool</c> per character of <paramref name="text"/>: true where that character is code. This is the
    /// single definition of "code" that both entry points below are built on, so a construct handled by one
    /// cannot be mishandled by the other.
    /// </summary>
    internal static bool[] CodeMask(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        var code = new bool[text.Length];
        ScanCode(text, 0, code, null);

        return code;
    }

    /// <summary>
    /// Blanks comments and literal TEXT while preserving newlines and interpolation holes, so a regex meant
    /// for code cannot match prose or a literal — and cannot miss a call written inside an interpolation.
    /// </summary>
    internal static string StripCommentsAndStrings(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        var code = CodeMask(text);
        var sb = new StringBuilder(text.Length);

        for (var i = 0; i < text.Length; i++)
        {
            sb.Append(code[i] ? text[i] : text[i] == '\n' ? '\n' : ' ');
        }

        return sb.ToString();
    }

    /// <summary>
    /// <para>The text from <paramref name="start"/> through the <paramref name="statements"/>'th <c>;</c>
    /// that sits at bracket depth zero or below, or to the end of <paramref name="text"/> if there are
    /// fewer. Only CODE characters are counted, so a semicolon or a bracket inside a comment or a literal
    /// cannot end the span or unbalance the depth.</para>
    ///
    /// <para>The span is cut from the ORIGINAL text rather than the stripped one, because the callers match
    /// a value-bearing regex over it and want to see what is actually written there. It is the WALK that is
    /// literal-aware, not the result.</para>
    ///
    /// <para><b>Why depth <c>&lt;= 0</c> and not <c>== 0</c>.</b> An untimed command inside a
    /// <c>using (...) { }</c> statement has the block's closing brace between it and the next command's
    /// deadline. A depth counter that cannot go negative treats that <c>}</c> as still depth-zero, keeps
    /// consuming past it, and reads the FOLLOWING command's deadline — calling the untimed site clean. That
    /// is a false negative in a guard, and it is the shape this group's own tooling once got wrong.</para>
    ///
    /// <para>Comments are skipped for the mirror-image reason, and it is not hypothetical: the two-statement
    /// window exists for the <c>CreateCommand</c> shape, whose deadline is the statement AFTER the
    /// construction, and this codebase's style actively encourages an explanatory comment in exactly that
    /// gap. A semicolon inside one would end the span early and report a correctly-timed site as an
    /// offender — a false positive, which is worse than a miss because it fails a green build and trains
    /// people to distrust the pin.</para>
    /// </summary>
    internal static string StatementSpanFrom(string text, int start, int statements)
    {
        ArgumentNullException.ThrowIfNull(text);

        var code = CodeMask(text);
        var depth = 0;
        var seen = 0;

        for (var i = start; i < text.Length; i++)
        {
            if (!code[i])
            {
                continue;
            }

            var c = text[i];

            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }
            else if (c == ';' && depth <= 0 && ++seen >= statements)
            {
                return text[start..(i + 1)];
            }
        }

        return text[start..];
    }

    /// <summary>
    /// Marks code from <paramref name="i"/> onwards. When <paramref name="hole"/> is supplied we are inside
    /// that literal's interpolation hole, and the scan RETURNS at the hole's closing brace run or at its
    /// top-level format-specifier colon without consuming either — the caller owns those delimiters.
    /// </summary>
    private static int ScanCode(string text, int i, bool[] code, Frame? hole)
    {
        var depth = 0;

        while (i < text.Length)
        {
            var c = text[i];

            if (hole is { } f && depth == 0)
            {
                if (c == '}' && Run(text, i, '}') >= f.Dollars)
                {
                    return i;
                }

                if (c == ':')
                {
                    /* `global::Foo` is the one legitimate top-level colon pair inside a hole; a bare `:` is
                       the format specifier, because a conditional expression in a hole has to be
                       parenthesised and so is never at depth zero here. */
                    if (i + 1 < text.Length && text[i + 1] == ':')
                    {
                        code[i] = true;
                        code[i + 1] = true;
                        i += 2;
                        continue;
                    }

                    return i;
                }
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                var nl = text.IndexOf('\n', i);
                i = nl < 0 ? text.Length : nl;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? text.Length : close + 2;
                continue;
            }

            /* A char literal is blanked rather than kept, and it has to be RECOGNISED whether or not it is
               kept: `'"'` is real code in the viewer project, and a walk that does not know what it is reads
               that quote as opening a string. */
            if (c == '\'')
            {
                i = SkipCharLiteral(text, i);
                continue;
            }

            if (TryReadOpener(text, i, out var frame, out var bodyStart))
            {
                /* A QuoteRun of zero is the empty string `""`, which has no body to walk. */
                i = frame.QuoteRun == 0 ? bodyStart : ScanLiteral(text, bodyStart, code, frame);
                continue;
            }

            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }

            code[i] = true;
            i++;
        }

        return i;
    }

    /// <summary>
    /// Walks a literal's body from <paramref name="i"/> — which is the first character AFTER the opening
    /// delimiter — marking any interpolation holes it contains as code. Returns the index just past the
    /// closing delimiter.
    /// </summary>
    private static int ScanLiteral(string text, int i, bool[] code, Frame frame)
    {
        while (i < text.Length)
        {
            var c = text[i];

            switch (frame.Kind)
            {
                case LiteralKind.Raw when c == '"':
                {
                    var run = Run(text, i, '"');

                    if (run >= frame.QuoteRun)
                    {
                        return i + run;
                    }

                    i += run;
                    continue;
                }

                case LiteralKind.Verbatim when c == '"':
                {
                    /* `""` is an escaped quote inside a verbatim string, not the end of it. */
                    if (i + 1 < text.Length && text[i + 1] == '"')
                    {
                        i += 2;
                        continue;
                    }

                    return i + 1;
                }

                case LiteralKind.Regular when c == '\\':
                    i += 2;
                    continue;

                case LiteralKind.Regular when c == '"':
                    return i + 1;

                /* A non-verbatim string cannot span lines, so a newline means the opener was mis-read or the
                   file is malformed. Ending here bounds the damage to one line instead of to the next quote
                   anywhere in the file. */
                case LiteralKind.Regular when c == '\n':
                    return i;
            }

            if (frame.Dollars > 0 && c == '{')
            {
                var run = Run(text, i, '{');

                if (frame.Dollars == 1)
                {
                    /* `{{` is a literal brace; an odd leftover opens a hole. */
                    i += run / 2 * 2;

                    if (run % 2 == 0)
                    {
                        continue;
                    }

                    i = ScanHole(text, i + 1, code, frame);
                    continue;
                }

                /* With more than one `$`, that many consecutive braces open a hole and a shorter run is
                   literal text — `{{typeParams}}` inside a `$$"""..."""` is one hole, not two braces. */
                if (run >= frame.Dollars)
                {
                    i = ScanHole(text, i + frame.Dollars, code, frame);
                    continue;
                }

                i += run;
                continue;
            }

            if (frame.Dollars > 0 && c == '}')
            {
                /* Only reachable for a brace run the literal did not open a hole with, i.e. escaped output
                   braces. Consumed as text. */
                i += Run(text, i, '}');
                continue;
            }

            i++;
        }

        return i;
    }

    /// <summary>
    /// Walks one interpolation hole whose expression starts at <paramref name="i"/>, and returns the index
    /// just past its closing brace run. The expression is code; the closing braces and any format specifier
    /// are not.
    /// </summary>
    private static int ScanHole(string text, int i, bool[] code, Frame frame)
    {
        i = ScanCode(text, i, code, frame);

        if (i < text.Length && text[i] == ':')
        {
            /* Everything from the colon to the closing brace run is format text, not code. */
            while (i < text.Length && !(text[i] == '}' && Run(text, i, '}') >= frame.Dollars))
            {
                i++;
            }
        }

        if (i < text.Length && text[i] == '}')
        {
            i += Math.Min(Run(text, i, '}'), frame.Dollars);
        }

        return i;
    }

    /// <summary>
    /// Reads a literal's opening delimiter at <paramref name="i"/>, if there is one: any run of <c>$</c>
    /// with at most one <c>@</c> interleaved, then a quote run. <paramref name="bodyStart"/> is the first
    /// character after the delimiter, and a <c>QuoteRun</c> of zero means the token was the empty string
    /// <c>""</c> and has no body to walk.
    /// </summary>
    private static bool TryReadOpener(string text, int i, out Frame frame, out int bodyStart)
    {
        frame = default;
        bodyStart = i;

        var j = i;
        var dollars = 0;
        var verbatim = false;

        while (j < text.Length && (text[j] == '$' || text[j] == '@'))
        {
            if (text[j] == '$')
            {
                dollars++;
            }
            else if (verbatim)
            {
                return false; /* `@@` is not a literal prefix. */
            }
            else
            {
                verbatim = true;
            }

            j++;
        }

        if (j >= text.Length || text[j] != '"')
        {
            /* A bare `@` here is an identifier escape (`@class`), which the caller marks as code. */
            return false;
        }

        if (verbatim)
        {
            /* A raw string literal cannot carry the `@` prefix, so a quote RUN here is not a delimiter:
               `@"""` is a verbatim string whose first content character is an escaped quote. Real code, in
               `Lite.Tests/CrossAppPresetValuePinTests.cs`: `@"\[""" + Regex.Escape(name) + @"""\]..."`.
               Reading that run as a three-quote raw delimiter swallows the wrong span and desynchronises
               the rest of the file, which is how this was found — the repo-wide balance sweep left two
               files unbalanced, both of them this shape. */
            frame = new Frame(LiteralKind.Verbatim, dollars, 1);
            bodyStart = j + 1;

            return true;
        }

        var quotes = Run(text, j, '"');

        if (quotes >= 3)
        {
            /* Three or more quotes open a raw string literal, whose delimiter is that whole run. Reading
               them as `""` plus the start of another string is what desynchronised the old walk. */
            frame = new Frame(LiteralKind.Raw, dollars, quotes);
            bodyStart = j + quotes;

            return true;
        }

        if (quotes == 2)
        {
            /* Exactly two is the empty string, not a raw delimiter. */
            frame = new Frame(LiteralKind.Regular, dollars, 0);
            bodyStart = j + 2;

            return true;
        }

        frame = new Frame(LiteralKind.Regular, dollars, 1);
        bodyStart = j + 1;

        return true;
    }

    /// <summary>Index just past a char literal opening at <paramref name="i"/>.</summary>
    private static int SkipCharLiteral(string text, int i)
    {
        var j = i + 1;

        while (j < text.Length)
        {
            if (text[j] == '\\')
            {
                j += 2;
                continue;
            }

            if (text[j] == '\'')
            {
                return j + 1;
            }

            /* Not a char literal after all — no valid one spans a line, so the file is malformed here.
               Consume just the quote so the scan cannot stall, and resume classifying at the next
               character. Unreachable for source that compiles. */
            if (text[j] == '\n')
            {
                return i + 1;
            }

            j++;
        }

        return j;
    }

    /// <summary>How many consecutive <paramref name="c"/> start at <paramref name="i"/>.</summary>
    private static int Run(string text, int i, char c)
    {
        var j = i;

        while (j < text.Length && text[j] == c)
        {
            j++;
        }

        return j - i;
    }
}
