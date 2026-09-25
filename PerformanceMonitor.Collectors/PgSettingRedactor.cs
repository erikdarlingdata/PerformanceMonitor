/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Masks secrets out of a <c>pg_settings</c> / <c>pg_db_role_setting</c> value before it is stored (#4348).
///
/// <para>The monitoring role is granted <c>pg_monitor</c>, which includes <c>pg_read_all_settings</c>, so it
/// reads <c>GUC_SUPERUSER_ONLY</c> settings — including <c>primary_conninfo</c>, which on a standby carries the
/// replication password in plain text. Nothing upstream of storage was redacting it, so a stored row, a backup
/// of the store, or an export of it, all held a live credential.</para>
///
/// <para><b>Rules</b> (applied to every <c>name</c>/<c>value</c> pair the collector reads):</para>
/// <list type="bullet">
/// <item>Inside any value, whatever the setting's name, the secret is masked and the rest of the value is kept:
/// <list type="bullet">
/// <item>the value of a libpq <c>password</c> or <c>sslpassword</c> keyword, quoted or not, honoring libpq's
/// quoting and <c>\\</c> / <c>\'</c> escapes;</item>
/// <item>the password in a URI's user info (<c>scheme://user:secret@host</c>), and a <c>password</c> query
/// parameter;</item>
/// <item>the value of an assignment or option whose name contains <c>PASSWORD</c>, <c>PASSWD</c>, <c>SECRET</c>
/// or <c>TOKEN</c> (case-insensitive), such as <c>PGPASSWORD=...</c> or <c>--password=...</c>.</item>
/// </list>
/// Host, port, user, dbname and application_name are never touched.</item>
/// <item><c>ssl_passphrase_command</c> has its whole value masked (an empty value stays empty — there is
/// nothing to mask).</item>
/// <item>An extension setting — a name with a dot, such as <c>anon.salt</c> — has its whole value masked when
/// the part of the name AFTER THE LAST DOT contains (as a substring, case-insensitive) <c>password</c>,
/// <c>passwd</c>, <c>passphrase</c>, <c>secret</c>, <c>salt</c>, <c>token</c> or <c>key</c>. This is a plain
/// substring test, not a whole-word one: <c>myext.api_key</c> matches (ends in <c>key</c>) and so would a name
/// that merely happens to contain those letters together, such as <c>myext.turkey_interval</c> — the false
/// positive it can produce only over-masks, never leaks, and <c>myext.keep_alive</c> is the negative case that
/// proves it (no marker word is a substring of <c>keep_alive</c>).</item>
/// <item>Nothing else changes: a core setting whose name merely contains one of those words, such as
/// <c>password_encryption</c>, keeps its value — only the DOTTED, extension-scoped form triggers a whole-value
/// mask by name.</item>
/// </list>
///
/// <para>Every rule maps the already-masked marker back onto itself, so <see cref="Redact"/> is idempotent —
/// running it twice on the same value is the same as running it once, which matters because the collector and
/// the one-time scrub of pre-existing rows (S1b of #4348) both call it, and the scrub may run over rows a newer
/// collector has already redacted.</para>
/// </summary>
public static class PgSettingRedactor
{
    /// <summary>
    /// Bumped whenever the rules above change in a way that would redact a previously-unmasked value
    /// differently. The one-time scrub (#4348) records the version it ran under, so a later rules change can
    /// tell which stored rows were scrubbed under an older, narrower rule set.
    /// </summary>
    public const int RulesVersion = 1;

    private const string Mask = "********";

    /// <summary>Names whose value is masked in full when the setting is extension-scoped (#4348).</summary>
    private static readonly string[] WholeValueNameMarkers =
    {
        "password",
        "passwd",
        "passphrase",
        "secret",
        "salt",
        "token",
        "key",
    };

    /// <summary>A libpq <c>password</c>/<c>sslpassword</c> keyword inside a conninfo-shaped value. The keyword
    /// must sit at the start of the string or after whitespace, exactly like every other libpq keyword=value
    /// pair, so it does not fire on an unrelated keyword such as <c>passfile</c>. The value is either a
    /// libpq-quoted string (backslash escapes <c>\\</c> and <c>\'</c>, closing on the first unescaped <c>'</c>)
    /// or an unquoted run of non-whitespace.</summary>
    private static readonly Regex LibpqPasswordKeyword = new(
        @"(?<=^|\s)(?<kw>sslpassword|password)\s*=\s*(?:'(?:\\.|[^'\\])*'|\S*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The password half of a URI's user info: <c>scheme://user:secret@host</c>. A bare
    /// <c>user@host</c>, with no password, does not match and the username is left alone.</summary>
    private static readonly Regex UriUserInfoPassword = new(
        @"://(?<user>[^:@/\s]+):[^@/\s]*@",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A <c>password</c> query parameter in a URI's query string.</summary>
    private static readonly Regex UriQueryPassword = new(
        @"(?<=[?&])(?<key>password)=[^&#\s'""]*",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A shell-style or option-style assignment (<c>NAME=value</c>, <c>--name=value</c>) whose name
    /// contains PASSWORD, PASSWD, SECRET or TOKEN. Covers <c>PGPASSWORD=x</c>, <c>AWS_SECRET_ACCESS_KEY=x</c>,
    /// <c>--password=x</c> inside <c>archive_command</c>/<c>restore_command</c>-shaped values. The value side
    /// accepts the same libpq-style quoting as <see cref="LibpqPasswordKeyword"/>, plus double quotes, so a
    /// quoted value with an <c>=</c> or space inside it is not mistaken for the start of the next token.</summary>
    private static readonly Regex AssignmentSecretName = new(
        @"(?<name>[\w.-]*(?:PASSWORD|PASSWD|SECRET|TOKEN)[\w.-]*)=(?:""(?:\\.|[^""\\])*""|'(?:\\.|[^'\\])*'|\S*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Masks the secret out of <paramref name="value"/> per the rules on this type, given the setting's
    /// <paramref name="name"/>. Never throws. <see langword="null"/> in, <see langword="null"/> out; an empty
    /// string is returned unchanged, since there is nothing in it to mask.
    /// </summary>
    public static string? Redact(string? name, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        if (IsWholeValueMasked(name))
        {
            return Mask;
        }

        var redacted = LibpqPasswordKeyword.Replace(value, static m => m.Groups["kw"].Value + "=" + Mask);
        redacted = UriUserInfoPassword.Replace(redacted, static m => "://" + m.Groups["user"].Value + ":" + Mask + "@");
        redacted = UriQueryPassword.Replace(redacted, static m => m.Groups["key"].Value + "=" + Mask);
        redacted = AssignmentSecretName.Replace(redacted, static m => m.Groups["name"].Value + "=" + Mask);

        return redacted;
    }

    private static bool IsWholeValueMasked(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return false;
        }

        if (string.Equals(name, "ssl_passphrase_command", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Extension-scoped settings only: a bare core GUC such as password_encryption never matches here,
        // no matter what its name contains — only the part after the LAST dot is tested, and only when
        // there is a dot at all.
        var dot = name.LastIndexOf('.');
        if (dot < 0 || dot == name.Length - 1)
        {
            return false;
        }

        var lastPart = name.AsSpan(dot + 1);
        foreach (var marker in WholeValueNameMarkers)
        {
            if (lastPart.Contains(marker, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
