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
    /// <para>The text from <paramref name="start"/> through the <paramref name="statements"/>'th
    /// statement-ending <c>;</c>, or through the end of the SCOPE that <paramref name="start"/> sits in,
    /// whichever comes first. Only CODE characters are counted, so a semicolon or a bracket inside a
    /// comment or a literal cannot end the span or unbalance the depth.</para>
    ///
    /// <para><b>The span is cut from whatever text it was handed, and every caller now hands it STRIPPED
    /// source.</b> This once said the opposite - that the original was passed because a caller wants to see
    /// what is actually written there - and that reasoning was wrong in the direction that matters. A caller
    /// matching a value-bearing regex over a raw span accepts the value SPELLED in a comment: a site whose
    /// deadline exists only in an explanatory note about the deadline reads as timed. Making the walk
    /// literal-aware and then handing it raw text throws the awareness away at the last step.</para>
    ///
    /// <para><b>A closing BRACE that was open before <paramref name="start"/> ends the span.</b> A statement
    /// count alone cannot bound it. An untimed command inside a <c>using (...) { }</c> whose body holds a
    /// single statement spends the two-statement window on that statement and on the statement AFTER the
    /// block, so the FOLLOWING command's deadline satisfies the scan and the untimed site reads as clean;
    /// the same leak runs out through the closing brace of any block a construction is the last statement of.
    /// Both are false negatives in a guard, and a deadline written where the command is already out of scope
    /// was never this command's deadline — so the scope, not a count, is what has to bound the span. A
    /// parenthesis or a bracket is NOT a scope: it delimits an expression, and an expression that opened
    /// before <paramref name="start"/> does not change which block the construction lives in.</para>
    ///
    /// <para><b>A statement header is followed into its embedded statement.</b> When the construction sits in
    /// a <c>using (var command = ...)</c> header, the deadline is either in that header's initializer or in
    /// the block the header governs — fourteen sites across the four projects these pins scan set it as the
    /// block's first statement — and never after the block, where the command no longer exists. So the span
    /// continues past the header's <c>)</c> into the embedded block and ends at its closing brace, counting
    /// that block's own statements. Stopping at the header instead would report all fourteen as offenders.
    /// When the header governs a BRACELESS single statement, that statement is the whole span for the same
    /// reason: the scope ends with it.</para>
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

        /* The depth the statements being counted live at: zero, or the embedded block's depth once a header
           has been followed into one. `body` is that block's depth, and -1 until there is one. */
        var floor = 0;
        var body = -1;
        var header = false;

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

                if (c == '{' && header && body < 0)
                {
                    body = depth;
                    floor = depth;
                }

                continue;
            }

            if (c is ')' or ']' or '}')
            {
                if (c == '}' && depth == body)
                {
                    return text[start..(i + 1)];
                }

                depth--;

                if (depth >= 0)
                {
                    continue;
                }

                if (c == '}')
                {
                    return text[start..i];
                }

                /* A parenthesis or bracket that was open before `start` has closed. Neither ends the block, so
                   the walk carries on at depth zero - but WHICH of the two it was decides how much further it
                   may go, and the answer is in what comes next. A statement HEADER is followed by the statement
                   it governs, so that statement is the rest of the span. An ARGUMENT LIST is followed by the
                   rest of its own expression, and the construction it held is still an ordinary part of the
                   statement it sits in, so the statement budget continues as normal. Conflating them costs a
                   verdict either way: treat a header like an argument list and the span runs out of the scope
                   the command lives in; treat an argument list like a header and a correctly-timed
                   `Wrap(connection.CreateCommand())` is cut off before the assignment on the next line. */
                depth = 0;
                header = header || StartsAStatement(text, code, i + 1);

                continue;
            }

            if (c != ';' || depth != floor)
            {
                continue;
            }

            if (header && body < 0)
            {
                /* A header whose embedded statement carries no braces - `using (...) await cmd.RunAsync();`.
                   This semicolon ends that one statement, and the scope ends with it, so there is no next
                   statement to look at. Without this the braced and braceless forms of the same `using` would
                   be judged differently, which is the generality the brace stop above does not have on its
                   own. */
                return text[start..(i + 1)];
            }

            if (++seen >= statements)
            {
                return text[start..(i + 1)];
            }
        }

        return text[start..];
    }

    /// <summary>
    /// <para>The construction expression starting at <paramref name="start"/> — its argument list, and the
    /// object or collection initializer attached to it, if there is one. Nothing else: this is the span that
    /// belongs to the SITE rather than to its statement or to its scope.</para>
    ///
    /// <para><b>Why the scope bound on <see cref="StatementSpanFrom"/> is not enough on its own.</b> A scan
    /// asking "was a deadline set here" over a span that reaches into the following statement — which the
    /// <c>CreateCommand</c> shape needs, its deadline being an assignment rather than an initializer — will
    /// accept the following construction's OWN initializer. So an untimed command directly followed by a
    /// timed one reads as timed, and so does an untimed <c>using (...)</c> header whose block opens with a
    /// timed sibling. That sibling is in the same scope and often in the very next statement, so neither a
    /// statement count nor a scope bound can exclude it.</para>
    ///
    /// <para>Splitting the question is what excludes it. A value in a construction's initializer belongs to
    /// THAT construction, so it is read from this span; a value assigned through a member access belongs to
    /// whatever it was assigned to, so it is read from the surrounding statement span. Truncating the
    /// statement span at the next construction instead is the tempting one-liner, and it is wrong: two
    /// constructions can share one deadline, as the arms of the conditional in
    /// <c>ViewerDataService.FinOps.Locking.cs</c> do, and truncating there reports the first arm as an
    /// offender.</para>
    /// </summary>
    internal static string ConstructionSpanFrom(string text, int start)
    {
        ArgumentNullException.ThrowIfNull(text);

        var code = CodeMask(text);
        var open = start;

        /* The argument list has to be ATTACHED to the reference at `start`, so the walk to it may only cross
           what a construction's head is made of: the `new` keyword, a qualified type or member name, and any
           generic argument list. A scan that just looked for the next `(` would run out of the statement
           entirely for a match that has no argument list - a bare `.CreateCommand` METHOD GROUP, which two of
           these pins match deliberately - and return the NEXT construction's span, initializer included, so a
           hand-off with no deadline of its own read as timed. */
        while (open < text.Length
               && (!code[open] || char.IsWhiteSpace(text[open]) || char.IsLetterOrDigit(text[open])
                   || text[open] is '_' or '.' or '<' or '>'))
        {
            open++;
        }

        if (open >= text.Length || text[open] != '(')
        {
            /* No argument list attached: the reference itself is the whole span, and it cannot carry a
               deadline. */
            return text[start..open];
        }

        var end = ClosingIndex(text, code, open, '(', ')');

        if (end < 0)
        {
            return text[start..];
        }

        var next = end + 1;

        while (next < text.Length && (!code[next] || char.IsWhiteSpace(text[next])))
        {
            next++;
        }

        if (next >= text.Length || text[next] != '{')
        {
            return text[start..(end + 1)];
        }

        var brace = ClosingIndex(text, code, next, '{', '}');

        return brace < 0 ? text[start..] : text[start..(brace + 1)];
    }

    /// <summary>
    /// Whether the next CODE character from <paramref name="i"/> begins a STATEMENT rather than continuing an
    /// expression. Only used to tell a statement header apart from an argument list once the closing
    /// parenthesis of one of them has been passed, where the two want different amounts of the text that
    /// follows. A statement can begin with a block, an identifier or a keyword; an expression continues with
    /// punctuation or an operator, and a bare <c>;</c> ends the statement the expression was part of.
    /// </summary>
    private static bool StartsAStatement(string text, bool[] code, int i)
    {
        while (i < text.Length && (!code[i] || char.IsWhiteSpace(text[i])))
        {
            i++;
        }

        return i < text.Length
               && (text[i] == '{' || text[i] == '_' || text[i] == '@' || char.IsLetter(text[i]));
    }

    /// <summary>
    /// The index of the <paramref name="close"/> matching the <paramref name="open"/> delimiter at
    /// <paramref name="from"/>, or -1 when they never balance. Only CODE characters count, so a delimiter in
    /// prose or in a literal cannot unbalance the match — the same contract <see cref="BraceBalanced"/> gets
    /// by being handed already-stripped text, reached the other way so this one holds whatever it is handed.
    /// </summary>
    private static int ClosingIndex(string text, bool[] code, int from, char open, char close)
    {
        var depth = 0;

        for (var i = from; i < text.Length; i++)
        {
            if (!code[i])
            {
                continue;
            }

            if (text[i] == open)
            {
                depth++;
            }
            else if (text[i] == close)
            {
                depth--;

                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    /// <summary>
    /// <para>The brace-balanced block starting at <paramref name="open"/>, that brace included — or the
    /// rest of <paramref name="text"/> when the braces never balance, so a caller gets a truncated body
    /// rather than an exception on malformed input.</para>
    ///
    /// <para>It lives here because it is only correct over the output of
    /// <see cref="StripCommentsAndStrings"/>: braces in prose and in literals are exactly what would
    /// unbalance it, which is why blanking a hole's DELIMITERS is part of that method's contract. Two
    /// viewer pins had grown their own copy by #2923 — behaviourally identical over 2,836,304 offsets of
    /// the viewer project but already textually apart, one having fused the decrement into its condition,
    /// which is how the five walk copies started too.</para>
    /// </summary>
    internal static string BraceBalanced(string text, int open)
    {
        ArgumentNullException.ThrowIfNull(text);

        var depth = 0;

        for (var i = open; i < text.Length; i++)
        {
            if (text[i] == '{')
            {
                depth++;
            }
            else if (text[i] == '}')
            {
                depth--;

                if (depth == 0)
                {
                    return text[open..(i + 1)];
                }
            }
        }

        return text[open..];
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
