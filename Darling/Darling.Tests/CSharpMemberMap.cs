/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// <para>Which C# MEMBER a character offset falls inside, for source-scanning pins that have to name the
/// member an offending literal is written in. Built on <see cref="CSharpSourceWalker"/>, so a modifier
/// inside a comment and a brace inside a literal are neither of them code (#2913, #3052).</para>
///
/// <para><b>Two pins were carrying byte-identical copies of a broken resolver, and one of them was making
/// a DECISION with it.</b> #3094 is about the copy in <c>TsqlConventionGuardTests</c>, where a wrong member
/// name only mislabelled an offender — costly, because that list is the guard's whole output when it reds,
/// but inert. <c>StoreSqlClockDisciplineTests</c> had the same regex and fed its answer into
/// <c>Waived.Contains(file + ":" + member)</c>, so a wrong name there could break a legitimate waiver into
/// a false red or — in the direction that costs — collide with a waived key and silently swallow a real
/// bare-clock finding. Fixing the message-only copy and leaving the deciding one is shipping the inert half
/// of a defect, which is why this is one authority and not two corrected copies. Same treatment
/// <see cref="CSharpSourceWalker"/> gave five hand-rolled maskers in #2913 and <c>RepoFile</c> gave 34 path
/// walks in #3090.</para>
///
/// <para><b>The access modifier at the start of a line is the whole discriminator</b>, and
/// <see cref="DeclarationHead"/> carries the argument for it. The predecessor asked for an identifier, a
/// parenthesised anything and an opening brace, which describes most C# statements: measured across the
/// T-SQL guard's corpus it labelled 48 of 160 literals with a bare keyword or nothing, and across the store
/// tree it answered <c>if</c>, <c>using</c>, <c>while</c>, <c>Select</c> and <c>NpgsqlCommand</c> at 21
/// more sites.</para>
///
/// <para><b>What it cannot see is stated on each member rather than discovered later</b>, and every such
/// shape resolves to <see cref="Unknown"/> — a label a reader escalates, not a plausible name a reader
/// trusts. A caller is expected to red on <see cref="Unknown"/> rather than absorb it.</para>
/// </summary>
internal static class CSharpMemberMap
{
    /// <summary>
    /// The start of a MEMBER declaration at type scope: an access modifier at the beginning of a line,
    /// followed by any run of the other modifiers.
    ///
    /// <para><b>The access modifier is the whole discriminator, and requiring it is what this regex is
    /// for.</b> Its predecessor took "the nearest name above the literal" from a pattern that also matched
    /// <c>if (…) {</c>, <c>catch (…) {</c>, <c>new SqlConnectionStringBuilder(…) {</c> and any local
    /// <c>string x =</c>. Measured on the tree it shipped against (#3094), 48 of the corpus's 160 T-SQL
    /// literals were labelled with a bare keyword or nothing at all, and several more with a local
    /// variable or the type being constructed — a figure about a resolver that no longer exists, which is
    /// why it is the one number written down here. A local cannot
    /// carry an access modifier and neither can a statement keyword, so anchoring on one excludes every
    /// shape that misattributed rather than blacklisting them one at a time.</para>
    ///
    /// <para><b>What it therefore cannot see, stated rather than discovered later.</b> A member with no
    /// access modifier (an <c>enum</c> or <c>interface</c> member, or a field written
    /// <c>static readonly …</c>) is not a declaration here, and neither is an attribute argument, which
    /// sits above its own member's declaration line. A literal in one of those resolves to
    /// <see cref="Unknown"/> — and <c>TsqlConventionGuardTests.EveryTsqlLiteralInTheCorpus_IsAttributedToADeclaredMember</c>
    /// reds on it by name. Widening the modifier list to admit a bare <c>const</c> or <c>static</c> would
    /// re-admit the local-variable case, so the resolution fails toward the worse label instead: an honest
    /// <c>&lt;unknown&gt;</c> a reader escalates, not a plausible name a reader trusts.</para>
    ///
    /// <para><b>That widening is guarded by this comment and by five fixture rows, and by nothing else.</b>
    /// A re-admitted local produces an identifier-shaped label, and
    /// <c>TsqlConventionGuardTests.EveryTsqlLiteralInTheCorpus_IsAttributedToADeclaredMember</c> can only see a label that
    /// is a keyword or nothing — so it would report a clean run across the rest of the corpus. Its doc
    /// comment states the same bound from the other side; if you are here to widen the list, read
    /// it.</para>
    ///
    /// <para><b>The trailing lookahead excludes an ACCESSOR</b>, which is the one place an access modifier
    /// appears INSIDE another member's body. <c>ArchiveService.IsArchiving</c> is the whole population and
    /// the reason the exclusion is here rather than in a comment: its <c>private set =&gt; …</c> matched as
    /// a declaration of its own, which put a declaration start inside the property that holds it and made
    /// <c>TsqlConventionGuardTests.TheMemberScan_ReadsEveryDeclarationWhole</c> report the property as over-extended. It is
    /// the only accessor in the scanned trees carrying an access modifier — which is why the exclusion is
    /// here rather than in a note saying it might one day be needed, and why no count of them is written
    /// down: that number changes on any commit and nothing would hold it true. An accessor is not a member
    /// a literal is attributed to; the property is.</para>
    /// </summary>
    internal static readonly Regex DeclarationHead = new(
        @"^[ \t]*(?:public|private|protected|internal)\b"
        + @"(?:[ \t]+(?:public|private|protected|internal|abstract|async|const|event|explicit|extern"
        + @"|implicit|new|override|partial|readonly|required|sealed|static|unsafe|virtual|volatile))*[ \t]+"
        + @"(?!(?:get|set|init|add|remove)\b[ \t]*(?:=>|\{|;))",
        RegexOptions.Multiline | RegexOptions.Compiled);

    /// <summary>What <see cref="EnclosingMember"/> reports when no member declaration contains the
    /// offset.</summary>
    internal const string Unknown = "<unknown>";

    /// <summary>
    /// Whether a declaration can hold a literal (a method, property, field, constructor) or only other
    /// declarations (a <c>class</c>, <c>record</c>, <c>enum</c>, …).
    ///
    /// <para>Types are in the declaration list rather than filtered out of it because a member's range is
    /// bounded by the NEXT declaration of either kind, and a nested type is the next thing after the member
    /// above it. They are also what <see cref="EnclosingType"/> answers from. They are excluded from MEMBER
    /// attribution and from
    /// <c>TsqlConventionGuardTests.TheMemberScan_ReadsEveryDeclarationWhole</c>: a type's body legitimately contains every
    /// declaration below it, which is the exact shape that assertion calls an over-run.</para>
    /// </summary>
    internal enum DeclarationKind
    {
        /// <summary>A method, property, field, event or constructor — something a literal can be inside.</summary>
        Member,

        /// <summary>A <c>class</c>, <c>struct</c>, <c>record</c>, <c>interface</c>, <c>enum</c> or
        /// <c>delegate</c>.</summary>
        Type,
    }

    /// <summary>
    /// One declaration as a half-open range over walked source, plus where the NEXT declaration starts.
    ///
    /// <para><c>NextStart</c> is carried because it is the one bound on <c>End</c> that does not come from
    /// the brace walk: it is a regex match offset, so it can DISAGREE with a brace count that ran past the
    /// method it was reading. #3089 found the same thing about the same shape — a re-check that reads the
    /// same text and repeats the same walk is a tautology, not a check.</para>
    ///
    /// <para><c>End</c> is <c>-1</c> when the brace walk never closed the body.</para>
    ///
    /// <para><c>ContentEnd</c> is the same boundary derived a SECOND way — by
    /// <see cref="StatementEnd"/>, which asks where the declaration's own <c>;</c> or body brace falls
    /// rather than stopping at the first braced group. It exists so <see cref="ShapeOf"/> has something to
    /// disagree with: <c>End</c> alone cannot report a range that stopped short, because a short range is
    /// closed, is under <c>NextStart</c>, and overlaps nothing. Two derivations that fail on different
    /// shapes, which is the only reason either is worth checking.</para>
    /// </summary>
    internal readonly record struct DeclaredRange(
        string Name,
        DeclarationKind Kind,
        int Start,
        int End,
        int NextStart,
        int ContentEnd);

    /// <summary>Every declaration in one file, beside the walked source they are offsets into.</summary>
    internal sealed record MemberMap(string Code, IReadOnlyList<DeclaredRange> Declarations);

    /// <summary>
    /// Every declaration in <paramref name="text"/>, read through <see cref="CSharpSourceWalker"/> so a
    /// modifier in a comment and a brace in a literal are neither of them code (#2913, #3052). The walk
    /// preserves every character position, so an offset into the walked text is the same offset into the
    /// file — which is what lets <see cref="EnclosingMember"/> take the literal offset the scan already has.
    /// </summary>
    internal static MemberMap Of(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var heads = DeclarationHead.Matches(code);
        var declarations = new List<DeclaredRange>(heads.Count);

        for (var i = 0; i < heads.Count; i++)
        {
            var head = heads[i];
            var after = head.Index + head.Length;
            var (name, kind) = DeclaredName(code, after);

            declarations.Add(new DeclaredRange(
                name,
                kind,
                head.Index,
                DeclarationEnd(code, after),
                i + 1 < heads.Count ? heads[i + 1].Index : code.Length,
                StatementEnd(code, after)));
        }

        return new MemberMap(code, declarations);
    }

    /// <summary>
    /// The declared name and kind, reading forward from just past the modifiers.
    ///
    /// <para>Tokenised rather than pattern-matched because the name's position depends on the shape: it is
    /// the identifier before the parameter list on a method, before <c>=&gt;</c> or <c>{</c> on a property,
    /// and before <c>=</c> on a field. A single regex for all three has to guess which <c>(</c> it is
    /// looking at, and <c>ScannedTrees</c> is the counter-example that decides it — a
    /// <c>(string, string[], string)[]</c> tuple type whose FIRST <c>(</c> is part of the type, so "the
    /// identifier before the first paren" reads <c>readonly</c>. The scan tells them apart by position
    /// rather than by shape: a type's parentheses come before any identifier has been read, a parameter
    /// list's come after one.</para>
    ///
    /// <para><b>Every branch here is load-bearing, which took measuring rather than reasoning.</b> An
    /// earlier draft also required the <c>(</c> to follow the name with only whitespace between, and moved
    /// the name's end onto the <c>&gt;</c> that closed a generic argument list so that a generic method
    /// would still qualify. Both were removed after dumping EVERY declaration in the scanned trees with
    /// and without them and diffing: identical, every name, every file. They were a pair that only existed
    /// to cancel each other out — and because each masked the other's removal, testing them one at a time
    /// reported both as benign. No mutation of either could red a test, which is the tell.</para>
    /// </summary>
    private static (string Name, DeclarationKind Kind) DeclaredName(string code, int from)
    {
        var typeKeyword = Regex.Match(
            code[from..Math.Min(from + 64, code.Length)],
            @"^(?:(?:record|partial|readonly|ref)[ \t]+)*(?<kw>class|struct|record|interface|enum|delegate)\b[ \t]+(?<name>[A-Za-z_][A-Za-z0-9_]*)");

        if (typeKeyword.Success)
        {
            return (typeKeyword.Groups["name"].Value, DeclarationKind.Type);
        }

        string? last = null;
        var paren = 0;
        var bracket = 0;
        var angle = 0;

        for (var i = from; i < code.Length; i++)
        {
            var c = code[i];

            if (char.IsLetter(c) || c == '_')
            {
                var j = i;

                while (j < code.Length && (char.IsLetterOrDigit(code[j]) || code[j] == '_'))
                {
                    j++;
                }

                if (paren == 0 && bracket == 0 && angle == 0)
                {
                    last = code[i..j];
                }

                i = j - 1;
                continue;
            }

            switch (c)
            {
                case '(':
                    /* A parameter list ends the name; a TYPE's parentheses do not, and the difference is
                       that a type's come before any identifier has been read. That is the whole reason
                       this is a scan and not a regex — ScannedTrees and DefinitionsWithoutSql in this file
                       are (string, string[], string)[] tuples whose first ( belongs to the type, so
                       "the identifier before the first paren" answers readonly.

                       It has to return here rather than fall through to = / { / ; : a generic member's
                       constraint clause sits between the parameter list and the body, and where T : class
                       is three more depth-zero identifiers. Constrained in
                       TheResolver_AttributesByScope_NotByTheNearestNameAbove is the pin. */
                    if (paren == 0 && bracket == 0 && angle == 0 && last is not null)
                    {
                        return (last, DeclarationKind.Member);
                    }

                    paren++;
                    break;

                case ')':
                    paren--;
                    break;

                case '[':
                    bracket++;
                    break;

                case ']':
                    bracket--;
                    break;

                /* Type arguments, tracked so a ( or a ; inside one cannot end the name — Func<(int, int)>
                   is the shape, and TheResolver_AttributesByScope_NotByTheNearestNameAbove reds without
                   this on the plainer Dictionary<,> form. Only opened directly after an identifier, so a
                   comparison operator in an initialiser cannot open one; a stray > cannot drive the depth
                   negative and strand every check below it. */
                case '<':
                    if (paren == 0 && bracket == 0 && angle == 0 && last is not null)
                    {
                        angle++;
                    }

                    break;

                case '>':
                    if (angle > 0)
                    {
                        angle--;
                    }

                    break;

                case '=':
                case '{':
                case ';':
                    if (paren == 0 && bracket == 0 && angle == 0)
                    {
                        return (last ?? Unknown, DeclarationKind.Member);
                    }

                    break;

                default:
                    break;
            }
        }

        return (last ?? Unknown, DeclarationKind.Member);
    }

    /// <summary>
    /// One past the declaration's last character: the <c>;</c> that ends a field or an expression-bodied
    /// member, or the <c>}</c> that closes a braced body. <c>-1</c> when a body opened and never closed.
    ///
    /// <para>Deliberately UNBOUNDED — it reads to the end of the file rather than stopping at the next
    /// declaration, so <see cref="DeclaredRange.NextStart"/> stays an independent bound that a runaway
    /// count can be caught by. Clamping here would make the two agree by construction.</para>
    ///
    /// <para><b>A braced group does not always end the declaration.</b> Returning at the first depth-0
    /// <c>{</c> is right only when that brace opens the member's BODY. Four shapes put a braced group in
    /// front of the body instead, and each one ends the range early:</para>
    /// <list type="bullet">
    /// <item>an accessor list followed by <c>= initialiser;</c> — <c>public string Q { get; set; } = "SELECT …";</c></item>
    /// <item>a property or type pattern — <c>utc is { } t ? … : …</c>, <c>ex is PostgresException { SqlState: "57014" }</c></item>
    /// <item>an object initialiser — <c>=&gt; new NpgsqlConnectionStringBuilder(cs) { Database = db }.ConnectionString</c></item>
    /// <item>a collection expression — <c>=&gt; new[] { A, B }.SelectMany(…).Where(…)</c></item>
    /// </list>
    /// <para>Only the first is handled here, by the <c>=</c> look-ahead below, because on an auto-property
    /// the initialiser is where a literal lives. The other three are REPORTED rather than avoided:
    /// <see cref="RangeShape.Truncated"/> compares this walk against
    /// <see cref="StatementEnd"/> and reds when they disagree. Reporting is the arm that generalises — all
    /// four shapes arrived as cases nobody had enumerated, so the fifth will too, and a detector covers a
    /// shape this method has never been taught.</para>
    ///
    /// <para><c>Auto</c> in <c>TsqlConventionGuardTests.TheResolver_AttributesByScope_NotByTheNearestNameAbove</c> pins the
    /// accessor-list case; found in review of #3097, where it resolved to <see cref="Unknown"/> in silence.
    /// <c>TsqlConventionGuardTests.TheMemberScan_ReadsEveryDeclarationWhole</c> pins the other three over
    /// the whole scanned tree, by set, against its <c>KnownTruncatedRanges</c>.</para>
    /// </summary>
    private static int DeclarationEnd(string code, int from)
    {
        var paren = 0;
        var bracket = 0;

        for (var i = from; i < code.Length; i++)
        {
            var c = code[i];

            if (c == '(')
            {
                paren++;
            }
            else if (c == ')')
            {
                paren--;
            }
            else if (c == '[')
            {
                bracket++;
            }
            else if (c == ']')
            {
                bracket--;
            }
            else if (paren != 0 || bracket != 0)
            {
                continue;
            }
            else if (c == ';')
            {
                return i + 1;
            }
            else if (c == '{')
            {
                var close = BraceGroupEnd(code, i);

                if (close < 0)
                {
                    return -1;
                }

                /* An accessor list can be followed by "= initialiser;", and on an auto-property that
                   initialiser is exactly where a literal lives:

                       public string QueryText { get; set; } = "SELECT …";

                   Returning at the accessor list's closing brace ends the range BEFORE the literal, so
                   the literal is contained by nothing and labelled <unknown> — and neither arm of
                   ShapeOf can see it, because a range that stops short of its own member is still well
                   under NextStart and still closed. That is a truncation with no detector, which is the
                   one shape this scan must not produce.

                   An '=' is the only thing that can legally follow the brace inside the same
                   declaration; anything else there belongs to the next one, so the brace ends this
                   range. */
                var after = close;

                while (after < code.Length && char.IsWhiteSpace(code[after]))
                {
                    after++;
                }

                if (after < code.Length && code[after] == '=')
                {
                    i = after;
                    continue;
                }

                return close;
            }
        }

        return -1;
    }

    /// <summary>
    /// One past the declaration's last character, derived by asking which braced group is the BODY rather
    /// than by stopping at the first one.
    ///
    /// <para>A member is expression-bodied when <c>=&gt;</c> is reached at depth 0 before any depth-0
    /// <c>{</c>. Every braced group after that arrow is INSIDE the body — a property pattern, an object
    /// initialiser, a collection expression — so this steps over it and keeps looking for the <c>;</c> that
    /// actually ends the declaration. With no arrow, the first depth-0 <c>{</c> IS the body and closes it.</para>
    ///
    /// <para>This is deliberately not a corrected copy of <see cref="DeclarationEnd"/>, and it does not
    /// replace it. It is a SECOND derivation of the same boundary, kept so the two can disagree; a checker
    /// built by re-running the walk it is checking agrees with itself by construction, which is the
    /// tautology <see cref="DeclaredRange.NextStart"/> already exists to avoid. Where the two disagree,
    /// <see cref="ShapeOf"/> reports <see cref="RangeShape.Truncated"/> and a human adjudicates — neither
    /// derivation is privileged as correct.</para>
    ///
    /// <para><b>Trailing <c>;</c> and whitespace are trimmed, which is what makes the disagreement mean
    /// something.</b> <c>=&gt; new T { … };</c> ends the range at the initialiser's brace and leaves the
    /// <c>;</c> outside it, which is harmless because no literal lives in a semicolon — and those members
    /// outnumber the ones where real content falls outside the range by more than ten to one, so reporting
    /// them would bury the set that matters. What is compared is therefore the end
    /// of the declaration's CONTENT, and the shape reds only when something a scan could look for is
    /// stranded. Those semicolon-only members are the same defect one character short of mattering: append
    /// <c>.Normalize()</c> after the initialiser and one joins the reported set with nothing else changing,
    /// which is why the arm is a derivation rather than a list.</para>
    /// </summary>
    private static int StatementEnd(string code, int from)
    {
        var paren = 0;
        var bracket = 0;
        var arrow = false;

        for (var i = from; i < code.Length; i++)
        {
            var c = code[i];

            if (c == '(')
            {
                paren++;
            }
            else if (c == ')')
            {
                paren--;
            }
            else if (c == '[')
            {
                bracket++;
            }
            else if (c == ']')
            {
                bracket--;
            }
            else if (paren != 0 || bracket != 0)
            {
                continue;
            }
            else if (c == '=' && i + 1 < code.Length && code[i + 1] == '>')
            {
                arrow = true;
                i++;
            }
            else if (c == ';')
            {
                return ContentBefore(code, from, i + 1);
            }
            else if (c == '{')
            {
                var close = BraceGroupEnd(code, i);

                if (close < 0)
                {
                    return -1;
                }

                if (!arrow)
                {
                    return close;
                }

                i = close - 1;
            }
        }

        return -1;
    }

    /// <summary>
    /// <paramref name="end"/> walked back over trailing <c>;</c> and whitespace, so what it points past is
    /// the declaration's last character of CONTENT. Never walks below <paramref name="from"/>.
    /// </summary>
    private static int ContentBefore(string code, int from, int end)
    {
        var i = end;

        while (i > from && (code[i - 1] == ';' || char.IsWhiteSpace(code[i - 1])))
        {
            i--;
        }

        return i;
    }

    /// <summary>One past the <c>}</c> that closes the brace group opening at <paramref name="open"/>, or
    /// <c>-1</c> if it never closes.</summary>
    private static int BraceGroupEnd(string code, int open)
    {
        var depth = 0;

        for (var j = open; j < code.Length; j++)
        {
            if (code[j] == '{')
            {
                depth++;
            }
            else if (code[j] == '}' && --depth == 0)
            {
                return j + 1;
            }
        }

        return -1;
    }

    /// <summary>
    /// The member a literal belongs to: the innermost member declaration whose range CONTAINS the offset.
    ///
    /// <para><b>Containment is not what fixed the measured sites, and saying so precisely matters.</b> The
    /// predecessor had no containment test at all — whatever matched last before the offset won, inside the
    /// literal's member or not — but what actually produced the 48 wrong labels was the pattern it matched
    /// WITH, and <see cref="DeclarationHead"/> is where that is fixed. Measured over the corpus: asking for
    /// the innermost CONTAINING member and asking for the nearest member declaration ABOVE the offset give
    /// the same answer at all 160 T-SQL sites. So containment is hardening, for the one direction the
    /// declaration regex cannot cover — a literal that is inside no member at all (an attribute argument,
    /// or anything below a body the brace walk lost) resolves to <see cref="Unknown"/> here instead of
    /// borrowing the name of whichever member happens to sit above it. It is pinned by
    /// <see cref="TheMemberScan_ReadsABodyWhoseLiteralHoldsABrace_AndARawCountDoesNot"/>, which is an
    /// arranged case rather than a site on the tree, and that is the honest status of it.</para>
    /// </summary>
    internal static string EnclosingMember(MemberMap map, int offset) =>
        Enclosing(map, offset, DeclarationKind.Member);

    /// <summary>
    /// The TYPE a declaration belongs to: the innermost type declaration whose range CONTAINS the
    /// offset. What <see cref="EnclosingMember"/> answers about members, for the enclosing type — which
    /// a scan attributing a FIELD needs, because a file declares more than one type and the field
    /// belongs to whichever body it sits in rather than to the file it is written in.
    /// </summary>
    internal static string EnclosingType(MemberMap map, int offset) =>
        Enclosing(map, offset, DeclarationKind.Type);

    /// <summary>
    /// The innermost declaration of <paramref name="kind"/> whose range CONTAINS
    /// <paramref name="offset"/>, or <see cref="Unknown"/>.
    ///
    /// <para>ONE implementation for both kinds, which is the whole point of it being here. The two
    /// entry points differ by a <see cref="DeclarationKind"/> and nothing else, and a second copy of
    /// this scan would be free to disagree about the part that is a JUDGEMENT rather than a filter:
    /// an unterminated body (<c>End &lt; 0</c>) escalates to <see cref="Unknown"/> instead of being
    /// treated as running to EOF, so a declaration the brace walk lost cannot lend its name to
    /// everything below it. A copy that guessed EOF there would attribute confidently and wrongly,
    /// and #3094 is the episode where a message-only copy was corrected while the deciding copy was
    /// left broken.</para>
    /// </summary>
    private static string Enclosing(MemberMap map, int offset, DeclarationKind kind)
    {
        var name = Unknown;
        var innermost = -1;

        foreach (var declaration in map.Declarations)
        {
            if (declaration.Kind != kind
                || declaration.End < 0
                || offset < declaration.Start
                || offset >= declaration.End
                || declaration.Start <= innermost)
            {
                continue;
            }

            name = declaration.Name;
            innermost = declaration.Start;
        }

        return name;
    }

    /// <summary>What the brace walk actually returned for one declaration.</summary>
    internal enum RangeShape
    {
        /// <summary>One whole declaration, ending inside its own bounds.</summary>
        WholeMember,

        /// <summary>A body opened and never closed, so every literal below it is attributed to nothing.</summary>
        Unterminated,

        /// <summary>The walk ran past the next declaration, so that member's literals are attributed
        /// here.</summary>
        OverExtended,

        /// <summary>The walk stopped short of the member's own end, so literals BELOW the stopping point
        /// and still inside the member are attributed to nothing.</summary>
        Truncated,
    }

    /// <summary>
    /// Which of three shapes a declaration's range is.
    ///
    /// <para><b>The three failing shapes are complementary, not redundant.</b>
    /// <see cref="RangeShape.OverExtended"/> compares the brace walk against a regex offset, so it can
    /// disagree with the walk — but it is structurally blind at the END of the file, where the last
    /// declaration has no successor to run past. That tail is what
    /// <see cref="RangeShape.Unterminated"/> still covers, because a runaway there simply never closes.
    /// #3089 measured the same pair on the same kind of scan and both arms are pinned here.</para>
    ///
    /// <para><b>Both of those are over-reads, and that was the gap.</b> A range that stops SHORT is closed,
    /// is under <c>NextStart</c>, and — ending early — cannot overlap its successor, so it satisfied every
    /// arm above and read as <see cref="RangeShape.WholeMember"/>. The members this finds strand content
    /// inside their own declaration, and where that content is a string literal
    /// <see cref="EnclosingMember"/> answers <c>&lt;unknown&gt;</c> for it — a site that is in the source
    /// and outside every range a census resolves through. How many there are is not written here: the live
    /// figure is <c>TsqlConventionGuardTests.KnownTruncatedRanges</c>, which a test holds to the tree, and a
    /// count repeated in prose goes stale the first time an unrelated PR rewrites one of those members —
    /// which happened to this comment's own numbers within an hour of it being written.
    /// <see cref="RangeShape.Truncated"/> is that arm, and it is the reason
    /// <see cref="DeclaredRange.ContentEnd"/> is carried: it comes from
    /// <see cref="StatementEnd"/> rather than from a second run of the walk being checked.</para>
    /// </summary>
    internal static RangeShape ShapeOf(DeclaredRange declaration) =>
        declaration.End < 0 ? RangeShape.Unterminated
        : declaration.End > declaration.NextStart ? RangeShape.OverExtended
        : declaration.ContentEnd > declaration.End ? RangeShape.Truncated
        : RangeShape.WholeMember;

    /// <summary>
    /// The 1-based line <paramref name="offset"/> falls on. Sound over both the file and the walked text:
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> preserves every newline and every character
    /// position.
    /// </summary>
    internal static int LineOf(string text, int offset) =>
        text.AsSpan(0, Math.Clamp(offset, 0, text.Length)).Count('\n') + 1;

    /// <summary>
    /// The declaration walk over RAW source. <b>Not a resolution path — a control fixture.</b>
    ///
    /// <para>It exists so a test can assert that <see cref="CSharpSourceWalker"/> is what saves the scan,
    /// in both directions, with a one-token difference from the shipped path: <see cref="Of"/> is this
    /// function with <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> applied first. Named for what
    /// it is so nobody reaches for it as an alternative to <see cref="Of"/>, which is what a plausible name
    /// like <c>RawDeclarations</c> invites. Its only caller is
    /// <c>TsqlConventionGuardTests.TheMemberScan_ReadsABodyWhoseLiteralHoldsABrace_AndARawCountDoesNot</c>.</para>
    /// </summary>
    internal static MemberMap OfRawSourceForControlTestsOnly(string text)
    {
        var heads = DeclarationHead.Matches(text);
        var declarations = new List<DeclaredRange>(heads.Count);

        for (var i = 0; i < heads.Count; i++)
        {
            var head = heads[i];
            var after = head.Index + head.Length;
            var (name, kind) = DeclaredName(text, after);

            declarations.Add(new DeclaredRange(
                name,
                kind,
                head.Index,
                DeclarationEnd(text, after),
                i + 1 < heads.Count ? heads[i + 1].Index : text.Length,
                StatementEnd(text, after)));
        }

        return new MemberMap(text, declarations);
    }
}
