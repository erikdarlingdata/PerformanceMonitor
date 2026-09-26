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
/// <item>the value of an assignment or option whose name contains <c>PASS</c>, <c>SECRET</c>, <c>TOKEN</c>,
/// <c>CREDENTIAL</c>, <c>PWD</c>, or a standalone <c>KEY</c> segment (case-insensitive), such as
/// <c>PGPASSWORD=...</c>, <c>--password=...</c>, <c>--passphrase ...</c> or <c>--encryption-key ...</c> — the
/// same name part on both the <c>=</c>-assignment path and the space-separated option path.</item>
/// </list>
/// Host, port, user, dbname and application_name are never touched.</item>
/// <item><c>ssl_passphrase_command</c> has its whole value masked (an empty value stays empty — there is
/// nothing to mask).</item>
/// <item>An extension setting — a name with a dot, such as <c>anon.salt</c> — has its whole value masked when
/// ANY dot-separated segment of the name contains (as a substring, case-insensitive) <c>password</c>,
/// <c>passwd</c>, <c>passphrase</c>, <c>secret</c>, <c>salt</c>, <c>token</c>, <c>key</c>, <c>credential</c> or
/// <c>pwd</c> — not only the last segment (round 1's L1: <c>vault.secret.value</c> matches on the middle
/// segment). This is a plain substring test, not a whole-word one: <c>myext.api_key</c> matches (ends in
/// <c>key</c>) and so would a name that merely happens to contain those letters together, such as
/// <c>myext.turkey_interval</c> — the false positive it can produce only over-masks, never leaks, and
/// <c>myext.keep_alive</c> is the negative case that proves it (no marker word is a substring of
/// <c>keep_alive</c>).</item>
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

    /// <summary>Names whose value is masked in full when the setting is extension-scoped (#4348), or when a
    /// marker sits in ANY dot-separated segment of the name, not only the last (review round 1, M5/L1).</summary>
    private static readonly string[] WholeValueNameMarkers =
    {
        "password",
        "passwd",
        "passphrase",
        "secret",
        "salt",
        "token",
        "key",
        "credential",
        "pwd",
    };

    /// <summary>A libpq <c>password</c>/<c>sslpassword</c> keyword inside a conninfo-shaped value. The keyword
    /// must sit at the start of the string or after whitespace, exactly like every other libpq keyword=value
    /// pair, so THIS regex does not fire on an unrelated keyword such as <c>passfile</c> — but
    /// <see cref="AssignmentSecretName"/>'s broader name part (round 2's M1) treats <c>PASS</c> as a
    /// substring, so <c>passfile=/x/.pgpass</c> is still masked whole, by that regex, not this one; it is an
    /// over-mask the ruling accepts, never a leak. The value is either a libpq-quoted string (backslash
    /// escapes <c>\\</c> and <c>\'</c>, closing on the first unescaped <c>'</c>, or running to the end of the
    /// value when the closing quote never arrives — round 1's L2), plus whatever non-whitespace immediately
    /// follows the closing quote (round 1's M4: a quoted value glued to a trailing <c>;</c> or another token
    /// with no space), or an unquoted run of non-whitespace.</summary>
    private static readonly Regex LibpqPasswordKeyword = new(
        @"(?<=^|\s)(?<kw>sslpassword|password)\s*=\s*(?:'(?:\\[\s\S]|[^'\\])*(?:'|$)\S*|\S*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The password half of a URI's user info: <c>scheme://user:secret@host</c>. A bare
    /// <c>user@host</c>, with no password, does not match and the username is left alone. The user name may
    /// be empty (round 1's M2: <c>postgresql://:secret@host</c> is valid libpq and still carries a
    /// password). The password itself may contain a raw space or an NBSP (round 1's L2 — libpq's own
    /// <c>isspace</c> is not Unicode-aware, so a literal space can sit inside the password up to the <c>@</c>),
    /// so the password class excludes only <c>@</c>, <c>/</c> and the URI's own delimiters, not all
    /// whitespace.</summary>
    private static readonly Regex UriUserInfoPassword = new(
        @"://(?<user>[^:@/\s\u00A0]*):[^@/]*@",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A <c>password</c> query parameter in a URI's query string.</summary>
    private static readonly Regex UriQueryPassword = new(
        @"(?<=[?&])(?<key>password)=[^&#\s'""]*",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A shell-style or option-style assignment (<c>NAME=value</c>) whose name contains PASSWORD,
    /// PASSWD, PASSPHRASE, SECRET, TOKEN, CREDENTIAL, PWD, or a standalone KEY segment (round 1's M5
    /// amendment: common WAL-G/Azure/pgBackRest variable names such as <c>WALG_LIBSODIUM_KEY</c> and
    /// <c>AZURE_STORAGE_ACCESS_KEY</c>). <c>PASS</c> covers PASSWORD, PASSWD and PASSPHRASE as substrings; KEY
    /// is bounded on both sides by <c>(?&lt;![A-Za-z0-9])</c>/<c>(?![A-Za-z0-9])</c> so it matches only as its
    /// own word or delimited segment, never as a substring — which is what keeps libpq's <c>sslkey=/path</c>
    /// unmasked. Covers <c>PGPASSWORD=x</c>, <c>AWS_SECRET_ACCESS_KEY=x</c>, <c>WALG_PGP_KEY_PASSPHRASE=x</c>
    /// inside <c>archive_command</c>/<c>restore_command</c>-shaped values. The value side accepts the same
    /// libpq-style quoting as <see cref="LibpqPasswordKeyword"/>, plus double quotes, plus whatever
    /// non-whitespace immediately follows the closing quote (round 1's M4), so a quoted value with an
    /// <c>=</c>, space, or trailing punctuation inside or after it is not mistaken for the start of the next
    /// token.</summary>
    private static readonly Regex AssignmentSecretName = new(
        @"(?<name>[\w.-]*(?:PASS|SECRET|TOKEN|CREDENTIAL|PWD|(?<![A-Za-z0-9])KEY(?![A-Za-z0-9]))[\w.-]*)=(?:""(?:\\[\s\S]|[^""\\])*(?:""|$)\S*|'(?:\\[\s\S]|[^'\\])*(?:'|$)\S*|\S*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A space-separated option whose name contains PASS, SECRET, TOKEN, CREDENTIAL, PWD, or a
    /// standalone KEY segment -- the same name part as <see cref="AssignmentSecretName"/> (review round 2's
    /// M1: the option path had fallen behind the assignment path's name list, so <c>gpg --passphrase x</c>,
    /// <c>openssl enc -pass pass:x</c>, <c>--encryption-key x</c> and <c>--credentials x</c> still leaked),
    /// taking its value from the next whitespace-delimited token rather than an <c>=</c> (round 1's M3:
    /// <c>--password hunter2</c> and <c>--secret-access-key hunter2</c> have no <c>=</c> at all, so
    /// <see cref="AssignmentSecretName"/> never fires on them). <c>(?!-)</c> keeps a value-less flag such as
    /// <c>--no-password -h x</c> from swallowing the next option as its value.</summary>
    private static readonly Regex OptionSecretSpaced = new(
        @"(?<=^|\s)(?<opt>--?[\w.-]*(?:PASS|SECRET|TOKEN|CREDENTIAL|PWD|(?<![A-Za-z0-9])KEY(?![A-Za-z0-9]))[\w.-]*)\s+(?!-)(?:""(?:\\[\s\S]|[^""\\])*(?:""|$)\S*|'(?:\\[\s\S]|[^'\\])*(?:'|$)\S*|\S+)",
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
        redacted = OptionSecretSpaced.Replace(redacted, static m => m.Groups["opt"].Value + " " + Mask);

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
        // no matter what its name contains — only a dotted name is tested at all. Round 1's L1: a marker in
        // ANY dot-separated segment counts, not only the last one (vault.secret.value), so the whole name is
        // tested as a substring rather than slicing off just the last part.
        var dot = name.IndexOf('.');
        if (dot < 0)
        {
            return false;
        }

        foreach (var marker in WholeValueNameMarkers)
        {
            if (name.Contains(marker, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
