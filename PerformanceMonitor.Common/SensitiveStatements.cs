/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one definition of which statement text the monitor withholds (#4348). The pattern is shared by every
/// place that keeps or shows statement text, for PostgreSQL and T-SQL alike, so two places can never drift
/// apart on what counts as sensitive.
///
/// <para>The pattern is a POSIX ARE evaluated by PostgreSQL's own engine (the <c>~*</c> operator), so a
/// PostgreSQL store judges text inside SQL without porting the rule. It has no backslash, so it means the same
/// thing under either <c>standard_conforming_strings</c> setting. Further parts of this class (the .NET
/// evaluation, XML and JSON handling) live in their own files and declare only their own members.</para>
///
/// <para><b>Append-only.</b> The four first alternatives (A1-A4, the PostgreSQL forms) are byte-identical to
/// the pattern before the T-SQL forms were added, and the T-SQL alternatives (T1-T10) follow them. Every
/// statement the earlier pattern named is therefore still named.</para>
/// </summary>
public static partial class SensitiveStatements
{
    /// <summary>What may separate two SQL tokens: whitespace, a block comment or a line comment. Used only by
    /// <see cref="Pattern"/>, a blocklist, where reading a comment short can only add matches.</summary>
    private const string TokenGap = "([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^[:cntrl:]]*)";

    /// <summary>
    /// The statements whose text can carry a credential, as a case-insensitive POSIX regular expression.
    ///
    /// <para>A1: any <c>CREATE</c> or <c>ALTER</c> of a role, user, group, subscription or server, withheld by
    /// name alone, because subscription and foreign-server DDL can carry a connection string in options other
    /// than a trailing literal. A2: a <c>PASSWORD</c> keyword followed by a literal. A3: a libpq
    /// <c>password=</c> or <c>PGPASSWORD=</c> setting. A4: a URI's <c>user:secret@</c>.</para>
    ///
    /// <para>T1: <c>CREATE</c>/<c>ALTER</c> of a login or credential, by name. T2: database scoped
    /// credentials. T3: any word ending in password, passwd, pwd or secret, or <c>key_source</c>, followed by a
    /// literal (including <c>N'</c> and <c>0x</c>). T4: an ODBC or OLE DB <c>PWD=</c> setting that is not a
    /// parameter. T5: system procedures that take a secret positionally. T6: built-ins that take a pass phrase
    /// or a key password. T7: <c>OPENDATASOURCE</c>. T8: <c>OPENROWSET</c>'s provider form (not
    /// <c>OPENROWSET(BULK ...)</c>). T9: a bracketed or double-quoted name ending in password, passwd, pwd or
    /// secret assigned a literal. T10: a typed declaration of such a name (T3's name set, <c>key_source</c>
    /// included), so the type sits between the name and the literal (<c>DECLARE @x nvarchar(20) = N'...'</c>,
    /// <c>x text := '...'</c>): the name, an optional <c>AS</c> or <c>CONSTANT</c>, one type name (plain, dotted
    /// or bracketed) with an optional length of up to 20 characters in parentheses, then <c>=</c>, <c>:=</c> or <c>DEFAULT</c> and a literal.</para>
    ///
    /// <para><b>No backslash, on purpose.</b> <c>[[:&lt;:]]</c>, <c>[[:&gt;:]]</c>, <c>[[:space:]]</c>,
    /// <c>[*]</c> and <c>[$]</c> spell what a first version wrote with backslash escapes. A normalized
    /// parameter (<c>password = $1</c>) is not a hit: it carries no value. <c>]</c> and <c>"</c> outside a
    /// bracket expression are literals.</para>
    /// </summary>
    public const string Pattern =
        "[[:<:]](create|alter)" + TokenGap + "+(role|user|group|subscription|server)[[:>:]]"                    // A1
        + "|[[:<:]]password[[:>:]]" + TokenGap + "*(=|to)?" + TokenGap + "*(e?'|u&'|[$][^0-9])"                 // A2
        + "|[[:<:]](pg)?password[[:space:]]*=[[:space:]]*[^$[:space:]]"                                        // A3
        + "|[a-z][a-z0-9+.-]*://[^[:space:]/@:]+:[^[:space:]/@]+@"                                             // A4
        + "|[[:<:]](create|alter)" + TokenGap + "+(login|credential)[[:>:]]"                                   // T1
        + "|[[:<:]]scoped" + TokenGap + "+credential[[:>:]]"                                                   // T2
        + "|[[:<:]]([a-z0-9_]*(password|passwd|pwd|secret)|key_source)[[:>:]]" + TokenGap + "*(=|to)?"
            + TokenGap + "*(n?'|e'|u&'|0x|[$][^0-9])"                                                         // T3
        + "|[[:<:]]pwd[[:space:]]*=[[:space:]]*[^$@[:space:]]"                                                // T4
        + "|[[:<:]](sp_addlogin|sp_password|sp_addlinkedsrvlogin|sp_addapprole|sp_approlepassword|sp_setapprole"
            + "|sp_change_users_login|sp_adddistributor|sp_changedistributor_password|sp_adddistpublisher"
            + "|sp_addsubscriber|sp_link_publication|sp_control_dbmasterkey_password|sp_xp_cmdshell_proxy_account)[[:>:]]" // T5
        + "|[[:<:]](encryptbypassphrase|decryptbypassphrase|decryptbykeyautocert|decryptbykeyautoasymkey"
            + "|decryptbyasymkey|decryptbycert|signbycert|signbyasymkey|pwdencrypt|pwdcompare)[[:>:]]"        // T6
        + "|[[:<:]]opendatasource[[:>:]]"                                                                      // T7
        + "|[[:<:]]openrowset" + TokenGap + "*[(]" + TokenGap + "*n?'"                                         // T8
        + "|[[:<:]][a-z0-9_]*(password|passwd|pwd|secret)(]|\")" + TokenGap + "*=" + TokenGap
            + "*(n?'|e'|u&'|0x)"                                                                              // T9
        + "|[[:<:]]([a-z0-9_]*(password|passwd|pwd|secret)|key_source)" + TokenGap + "+((as|constant)" + TokenGap + "+)?"
            + "[[]?[a-z_][a-z0-9_.]*]?([[:space:]]*[(][[:space:]0-9a-z,]{0,20}[)])?" + TokenGap
            + "*(:?=|default[[:>:]])" + TokenGap + "*(n?'|e'|u&'|0x|[$][^0-9])";                              // T10

    /// <summary>
    /// How many top-level alternatives at the head of <see cref="Pattern"/> (A1-A4 and T1-T9: thirteen) the .NET
    /// judge compiles as its first regex. The alternatives after them (T10, and anything appended later) are its
    /// second regex, and a value is named when either matches (#5320). The split is only about speed: past a size
    /// the runtime stops optimizing the compiled matcher, and one regex for the whole alternation ran about three
    /// times slower on a long value than the two halves together. PostgreSQL still reads the one
    /// <see cref="Pattern"/>. The two parts are cut from that constant when the judge is built, never copied, and
    /// a test pins that they put back together make exactly <see cref="Pattern"/>. New alternatives are appended
    /// after T10 (the pattern is append-only), so this number stays 13; if the second part itself grows large
    /// enough to slow down the same way, cut it again.
    /// </summary>
    internal const int JudgeHeadAlternatives = 13;

    /// <summary>What a collector or reader stores or returns in place of a statement <see cref="Pattern"/>
    /// names. Fixed, so a reader never has to distinguish "withheld" from "not captured yet" by anything other
    /// than this literal.</summary>
    public const string PlaceholderText = "-- statement text withheld (#4348)";

    /// <summary>What <see cref="Json"/> returns when it cannot read a result. Fixed, so a caller can tell a refusal
    /// from withheld text, and never the input.</summary>
    public const string JsonRefusal = "Output withheld: the sensitive-statement filter could not read this result.";

    /// <summary>One read-time call may spend this long judging and parsing (the outermost <see cref="Xml"/> or
    /// <see cref="Json"/> call; every nested value shares it). One value may still overrun by its own match
    /// timeout, so a call is bounded by about 1.75 s.</summary>
    internal static readonly TimeSpan ReadBudget = TimeSpan.FromMilliseconds(1500);

    /// <summary>
    /// Judges every value of a plan, report or event XML document (see <see cref="XmlCore"/>) and returns the SAME
    /// instance when nothing is named. Never throws. A document the read cannot finish inside its budget comes back
    /// as <see cref="PlaceholderText"/>.
    /// </summary>
    /// <param name="xml">The document or fragment.</param>
    /// <param name="maxOutputChars">The caller will cut the result at this length; work stops once nothing before
    /// the cut can change.</param>
    public static string? Xml(string? xml, int maxOutputChars = int.MaxValue) =>
        string.IsNullOrEmpty(xml) ? xml : Xml(xml, new JudgeBudget(ReadBudget), maxOutputChars);

    /// <summary>
    /// Judges every string (and key) of a whole tool result (see <see cref="JsonCore"/>) and returns the SAME
    /// instance when nothing is named. Never throws: a result it cannot read comes back as
    /// <see cref="JsonRefusal"/>, not as the input.
    /// </summary>
    public static string Json(string output) => Json(output, new JudgeBudget(ReadBudget));

    /// <summary><see cref="Xml(string?, int)"/> under a budget the caller owns, so one outermost call can share it
    /// across every value it judges.</summary>
    internal static string? Xml(string? xml, JudgeBudget budget, int maxOutputChars = int.MaxValue)
    {
        if (string.IsNullOrEmpty(xml)) return xml;
        try
        {
            return XmlCore(
                xml,
                value => IsNamedUnder(budget, value),
                () => budget.Spent,
                PlaceholderText,
                budget.AddElapsed,
                maxOutputChars);
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return PlaceholderText;
        }
    }

    /// <summary><see cref="Json(string)"/> under a budget the caller owns.</summary>
    internal static string Json(string output, JudgeBudget budget) =>
        JsonCore(output, value => TextUnder(budget, value), value => Xml(value, budget), JsonRefusal);

    /// <summary>True when <paramref name="value"/>, or its HTML-decoded form, is named or not judged in time.</summary>
    private static bool IsNamedUnder(JudgeBudget budget, string value) =>
        budget.Judge(value) != Verdict.Clean
        || (value.Contains('&', StringComparison.Ordinal) && budget.Judge(WebUtility.HtmlDecode(value)) != Verdict.Clean);

    /// <summary><see cref="Text(string?)"/> under a budget: a spent budget or a failed match gives the marker.</summary>
    private static string? TextUnder(JudgeBudget budget, string? value)
    {
        if (string.IsNullOrEmpty(value)) return value;
        try
        {
            return IsNamedUnder(budget, value) ? PlaceholderText : value;
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return PlaceholderText;
        }
    }
}
