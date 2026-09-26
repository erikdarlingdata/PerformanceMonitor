/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading;

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
/// <c>pwd</c> — not only the last segment (<c>vault.secret.value</c> matches on the middle
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
    public const int RulesVersion = 2;

    private const string Mask = "********";

    /// <summary>A sample that reaches every pattern's match step (#4348), used only to warm each compiled
    /// pattern's JIT ahead of the first real <see cref="Redact"/> call. Holds placeholder values only.</summary>
    private const string WarmupSample =
        "password=x sslpassword=y postgresql://u:p@h/?password=z&sig=s&X-Amz-Signature=t \"password\" = \"q\" --password w sshpass -p v curl -u a:b";

    /// <summary>
    /// A per-regex match-time bound (#4348): a pathological value (a long run with no separator, feeding one
    /// of the lookaround-heavy patterns below) could otherwise pin the engine backtracking well past the
    /// scrub's own budget. Every pattern that has no lookaround, backreference, atomic group or conditional
    /// runs under <see cref="System.Text.RegularExpressions.RegexOptions.NonBacktracking"/> instead, since
    /// that option accepts none of those constructs and needs no timeout of its own — its match time is
    /// already linear in the input length.
    /// </summary>
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// A test seam (#4348): overridden by a test to force <see cref="System.Text.RegularExpressions.RegexMatchTimeoutException"/>
    /// on a normal-shaped input without needing an input large enough to actually run past
    /// <see cref="MatchTimeout"/>. Null (the default, and what production always leaves it at) means every
    /// timeout-bearing pattern below runs under its own compiled-in <see cref="MatchTimeout"/>; a non-null
    /// value overrides all of them for the duration of the test, at the cost of building an uncompiled
    /// throwaway <see cref="Regex"/> per call — acceptable only because this path is test-only.
    /// Backed by <see cref="AsyncLocal{T}"/> so parallel test classes each see only their own override,
    /// instead of one process-wide value stomping on the others.
    /// </summary>
    private static readonly AsyncLocal<TimeSpan?> _matchTimeoutForTest = new();

    internal static TimeSpan? MatchTimeoutForTest
    {
        get => _matchTimeoutForTest.Value;
        set => _matchTimeoutForTest.Value = value;
    }

    /// <summary>Wraps one lookaround-bearing pattern so it always runs under a match timeout, while still
    /// letting a test force a much shorter one via <see cref="MatchTimeoutForTest"/> without rebuilding
    /// every call on the production path.</summary>
    internal sealed class TimeBoundPattern
    {
        private readonly string _pattern;
        private readonly RegexOptions _options;
        private readonly Regex _default;

        public TimeBoundPattern(string pattern, RegexOptions options)
        {
            _pattern = pattern;
            _options = options;
            _default = new Regex(pattern, options, MatchTimeout);
        }

        public string Replace(string input, MatchEvaluator evaluator) =>
            (MatchTimeoutForTest is TimeSpan overrideTimeout
                ? new Regex(_pattern, _options, overrideTimeout)
                : _default).Replace(input, evaluator);

        /// <summary>Runs the DEFAULT (production-timeout) instance once on a sample that reaches every
        /// pattern's match step, outside any timed accounting the caller does (#4348): a
        /// <see cref="RegexOptions.Compiled"/> pattern's IL is JITted on its first invocation, and on a busy
        /// CI runner that JIT cost alone can exceed the 100&#160;ms <see cref="MatchTimeout"/>, turning the
        /// first real call on a short, well-formed value into a spurious whole-value mask. Warming here, at
        /// type initialisation, pays that cost once, before the type is usable at all, so the first real
        /// <see cref="Redact"/> call never has to. A timeout during warmup (the JIT itself, on an especially
        /// slow runner, taking longer than the timeout) is swallowed — warmup exists to pre-pay JIT cost,
        /// not to prove the pattern is fast.</summary>
        public void Warmup()
        {
            try
            {
                _default.IsMatch(WarmupSample);
            }
            catch (RegexMatchTimeoutException)
            {
            }
        }

        /// <summary>Runs <see cref="Warmup"/>'s sample through this pattern and reports whether it produced
        /// at least one match (#4348) — used only by the test that pins that warmup reaches every pattern's
        /// match step, not only its scan.</summary>
        internal bool WarmupSampleMatches() => _default.IsMatch(WarmupSample);
    }

    /// <summary>Names whose value is masked in full when the setting is extension-scoped (#4348), or when a
    /// marker sits in ANY dot-separated segment of the name, not only the last.</summary>
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
    /// <see cref="AssignmentSecretName"/>'s broader name part treats <c>PASS</c> as a
    /// substring, so <c>passfile=/x/.pgpass</c> is still masked whole, by that regex, not this one; it is an
    /// over-mask the ruling accepts, never a leak. The value is either a libpq-quoted string (backslash
    /// escapes <c>\\</c> and <c>\'</c>, closing on the first unescaped <c>'</c>, or running to the end of the
    /// value when the closing quote never arrives), plus whatever non-whitespace immediately
    /// follows the closing quote (a quoted value glued to a trailing <c>;</c> or another token
    /// with no space), or an unquoted run of non-whitespace.</summary>
    private static readonly TimeBoundPattern LibpqPasswordKeyword = new(
        @"(?<=^|\s)(?<kw>sslpassword|password)\s*=\s*(?:'(?:\\[\s\S]|[^'\\])*(?:'|$)\S*|\S*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The password half of a URI's user info: <c>scheme://user:secret@host</c>. A bare
    /// <c>user@host</c>, with no password, does not match and the username is left alone. The user name may
    /// be empty (<c>postgresql://:secret@host</c> is valid libpq and still carries a
    /// password). The password itself may contain a raw space or an NBSP (libpq's own
    /// <c>isspace</c> is not Unicode-aware, so a literal space can sit inside the password up to the <c>@</c>),
    /// so the password class excludes only <c>@</c>, <c>/</c> and the URI's own delimiters, not all
    /// whitespace.</summary>
    private static readonly Regex UriUserInfoPassword = new(
        @"://(?<user>[^:@/\s\u00A0]*):[^@/]*@",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    /// <summary>A <c>password</c> query parameter in a URI's query string. A quoted value
    /// (<c>?password="a b"</c>) is masked in full — quotes and all — via the same quote-aware value
    /// alternation the other rules use, rather than stopping at the first quote character and leaving the
    /// rest of the value readable.</summary>
    private static readonly TimeBoundPattern UriQueryPassword = new(
        @"(?<=[?&])(?<key>password)=(?:[""'][^""'&#]*(?:[""']|$)|[^&#\s'""]*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A shell-style or option-style assignment (<c>NAME=value</c>) whose name contains PASSWORD,
    /// PASSWD, PASSPHRASE, SECRET, TOKEN, CREDENTIAL, PWD, or a standalone KEY segment (covers
    /// common WAL-G/Azure/pgBackRest variable names such as <c>WALG_LIBSODIUM_KEY</c> and
    /// <c>AZURE_STORAGE_ACCESS_KEY</c>). <c>PASS</c> covers PASSWORD, PASSWD and PASSPHRASE as substrings; KEY
    /// is bounded on both sides by <c>(?&lt;![A-Za-z0-9])</c>/<c>(?![A-Za-z0-9])</c> so it matches only as its
    /// own word or delimited segment, never as a substring — which is what keeps libpq's <c>sslkey=/path</c>
    /// unmasked. Covers <c>PGPASSWORD=x</c>, <c>AWS_SECRET_ACCESS_KEY=x</c>, <c>WALG_PGP_KEY_PASSPHRASE=x</c>
    /// inside <c>archive_command</c>/<c>restore_command</c>-shaped values. The value side accepts the same
    /// libpq-style quoting as <see cref="LibpqPasswordKeyword"/>, plus double quotes, plus whatever
    /// non-whitespace immediately follows the closing quote, so a quoted value with an
    /// <c>=</c>, space, or trailing punctuation inside or after it is not mistaken for the start of the next
    /// token. The leading <c>(?=[\w.-]*=)</c> pre-check (#4348) is redundant with the <c>=</c> the pattern
    /// already requires further on — it changes no match, only how the engine gets there. Without it, a
    /// long run of name-class characters with no <c>=</c> anywhere makes the engine retry the whole
    /// <c>[\w.-]*</c> body at every start position before failing, which is quadratic in the run's length; the
    /// lookahead fails FAST, once, per start position instead.</summary>
    private static readonly TimeBoundPattern AssignmentSecretName = new(
        @"(?<![\w.-])(?=[\w.-]*=)(?<name>[\w.-]*(?:PASS|SECRET|TOKEN|CREDENTIAL|PWD|(?<![A-Za-z0-9])KEY(?![A-Za-z0-9]))[\w.-]*)=(?:""(?:\\[\s\S]|[^""\\])*(?:""|$)\S*|'(?:\\[\s\S]|[^'\\])*(?:'|$)\S*|\S*)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A space-separated option whose name contains PASS, SECRET, TOKEN, CREDENTIAL, PWD, or a
    /// standalone KEY segment -- the same name part as <see cref="AssignmentSecretName"/> (covers the
    /// space-separated option path so <c>gpg --passphrase x</c>,
    /// <c>openssl enc -pass pass:x</c>, <c>--encryption-key x</c> and <c>--credentials x</c> are all masked),
    /// taking its value from the next whitespace-delimited token rather than an <c>=</c> (
    /// <c>--password hunter2</c> and <c>--secret-access-key hunter2</c> have no <c>=</c> at all, so
    /// <see cref="AssignmentSecretName"/> never fires on them). <c>(?!-)</c> keeps a value-less flag such as
    /// <c>--no-password -h x</c> from swallowing the next option as its value. The leading
    /// <c>(?=--?[\w.-]*\s)</c> pre-check (#4348) is redundant with the <c>\s+</c> the pattern already
    /// requires after the option name — same fast-fail-per-start-position reasoning as
    /// <see cref="AssignmentSecretName"/>'s pre-check.</summary>
    private static readonly TimeBoundPattern OptionSecretSpaced = new(
        @"(?<=^|\s)(?=--?[\w.-]*\s)(?<opt>--?[\w.-]*(?:PASS|SECRET|TOKEN|CREDENTIAL|PWD|(?<![A-Za-z0-9])KEY(?![A-Za-z0-9]))[\w.-]*)\s+(?!-)(?:""(?:\\[\s\S]|[^""\\])*(?:""|$)\S*|'(?:\\[\s\S]|[^'\\])*(?:'|$)\S*|\S+)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A percent-encoded key name in a URI
    /// query string, such as <c>?pass%77ord=x</c> (<c>%77</c> is <c>w</c>). The key is decoded before it is
    /// tested against <see cref="QueryKeySecretMarkers"/>, so an encoded variant of any letter in the key
    /// still matches; the RAW (still-encoded) key text is kept in the output, only the value is masked. A
    /// quoted value (<c>?pass%77ord="x"</c>) is masked in full — quotes and all.</summary>
    private static readonly TimeBoundPattern UriQueryKeyAnyEncoding = new(
        @"(?<=[?&])(?<key>(?:%[0-9A-Fa-f]{2}|[\w.-])+)=(?:[""'][^""'&#]*(?:[""']|$)|[^&#\s'""]*)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A SAS or pre-signed URL signature query parameter — <c>sig</c> (Azure SAS),
    /// <c>X-Amz-Signature</c> (AWS presigned) or <c>X-Goog-Signature</c> (GCS presigned). These are
    /// capability tokens, not passwords by name, so they need their own key list rather than riding
    /// <see cref="WholeValueNameMarkers"/>'s substring test (none of those markers appear in "sig").</summary>
    private static readonly TimeBoundPattern UriQuerySignature = new(
        @"(?<=[?&])(?<key>sig|X-Amz-Signature|X-Goog-Signature)=[^&#\s'""]*",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A double-quoted or single-quoted <c>"name = value"</c> assignment — a quote sits immediately
    /// before the name, and there are spaces around the <c>=</c> (a JSON/YAML-flavored shape distinct from
    /// the unquoted, tightly-bound <c>=</c> that <see cref="AssignmentSecretName"/> matches). The name itself
    /// may ALSO be quote-wrapped — <c>{"password" = "hunter2"}</c> — because <c>(?<nameq>["']?)</c> makes the
    /// name's own closing quote optional on both sides independently of the value's quoting, so a
    /// key-quoted/value-quoted JSON shape and a bare <c>"password = x"</c> shape both match with the same
    /// pattern. The value is either separately quoted (<c>"password" = "hunter2"</c>, <c>valq</c> matches),
    /// or left bare and closed only by the NAME's own opening quote coming back around
    /// (<c>"password = hunter2"</c>, <c>'pwd = abc'</c> — the whole assignment sits inside one quote pair, so
    /// the value's end is <c>\k&lt;q&gt;</c>, not a quote of its own). Only the value half is masked; the
    /// quotes and the name are kept as they were. The value's own quote character does not have to match the
    /// name's. The leading <c>(?=[\w.-]*["']?\s*=)</c> pre-check (#4348) mirrors the name-then-optional-quote
    /// -then-<c>=</c> shape the pattern already requires further on — redundant by construction, same
    /// fast-fail-per-start-position reasoning as <see cref="AssignmentSecretName"/>'s pre-check.</summary>
    private static readonly TimeBoundPattern QuotedSpacedAssignment = new(
        @"(?<q>[""'])(?=[\w.-]*[""']?\s*=)(?<name>[\w.-]*(?:PASS|SECRET|TOKEN|CREDENTIAL|PWD|(?<![A-Za-z0-9])KEY(?![A-Za-z0-9]))[\w.-]*)(?<nameq>[""']?)\s*=\s*(?:(?<valq>[""'])(?<val>[^""']*)\k<valq>|(?<val>[^""']*)\k<q>)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary><c>curl -u user:secret</c> / <c>curl --user user:secret</c>, including the tightly-bound
    /// <c>-uuser:secret</c>/<c>--user=user:secret</c> forms and <c>--proxy-user</c>/<c>-U</c> (Basic Auth over
    /// a proxy, curl's own name for that pair — note <c>-U</c> is upper-case ONLY: curl treats <c>-u</c> and
    /// <c>-U</c> as two different flags, so this pattern does not fold them together case-insensitively). The
    /// user name is kept, only the part after the colon is masked. The double-quoted branch's value runs to
    /// the CLOSING double quote rather than stopping at the first embedded space, and a backslash-escaped
    /// double quote inside that run (<c>--user "u:it\"s"</c>) does not end it early, so the value is masked
    /// whole and a second pass over the masked output is a no-op. The unquoted branch's value likewise
    /// consumes a run of quoted segments glued to bare characters (<c>u:"a b"</c>), not just the first
    /// non-space run, so nothing after an embedded-space quoted chunk stays unmasked. A bare <c>-u user</c> with no
    /// colon (no password at all) does not match.</summary>
    private static readonly TimeBoundPattern CurlUserColon = new(
        @"(?<=^|\s)(?:(?<flag>-u|--user|-U|--proxy-user)[\s=]*""(?<user>[^:""]+):(?<val>(?:\\.|[^""\\])*)""|(?<flag>-u|--user|-U|--proxy-user)[\s=]*'(?<user>[^:']+):(?<val>[^']*)'|(?<flag>-u|--user|-U|--proxy-user)[\s=]*(?<user>[^:\s]+):(?<val>(?:""[^""]*""|'[^']*'|\S)+))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary><c>sshpass -p secret</c>. <c>-p</c> is too short and generic to add to
    /// <see cref="OptionSecretSpaced"/>'s name list (it would swallow unrelated single-letter flags on other
    /// commands), so this is scoped to the literal <c>sshpass</c> command name (optionally path-prefixed,
    /// such as <c>/usr/bin/sshpass</c>), and covers both <c>-p secret</c> and the tightly-bound <c>-psecret</c>.
    /// Earlier options are allowed before <c>-p</c> (<c>sshpass -v -p x</c>, <c>sshpass -e -p x</c>) via a
    /// non-greedy run of whitespace-delimited tokens, so a plain <c>sshpass -f file</c> (no <c>-p</c> at all)
    /// still does not match. <c>-p</c> itself is matched case-SENSITIVELY (<c>(?-i:-p)</c>, even though the
    /// rest of the pattern is case-insensitive for the command name), because sshpass's own <c>-P</c> takes
    /// the SSH prompt text to wait for, not a password, and folding the two together would mask that prompt
    /// text instead of the real <c>-p</c> value that follows it (<c>sshpass -P prompt -p s3</c> masks
    /// <c>s3</c>, not <c>prompt</c>). A quoted value with an embedded space is masked whole via the
    /// quote-aware value alternation.</summary>
    private static readonly TimeBoundPattern SshpassOption = new(
        @"(?<prefix>(?:^|(?<=/|\s))sshpass(?:\s+\S+)*?\s+)(?-i:-p)\s*(?:""(?<val>[^""]*)""|'(?<val>[^']*)'|(?<val>\S+))",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Markers a decoded URI query KEY is tested against for <see cref="UriQueryKeyAnyEncoding"/> —
    /// deliberately narrower than <see cref="WholeValueNameMarkers"/> (no bare "key", "token" etc.),
    /// since a query key is user-controlled and a substring test there would over-mask an ordinary parameter
    /// such as <c>apikey=</c> being conflated with something like <c>monkey=</c>; password-family words only.</summary>
    private static readonly string[] QueryKeySecretMarkers =
    {
        "password",
        "passwd",
        "passphrase",
        "pwd",
    };

    /// <summary>Set once, before any real <see cref="Redact"/> call can return (#4348): the static
    /// constructor below warms every <see cref="Compiled"/> pattern's first-use JIT cost before it counts
    /// against anyone's <see cref="MatchTimeout"/>. Exposed only so a test can pin that warmup ran ahead of
    /// the first real call, without the test having to spin up a fresh <c>AssemblyLoadContext</c> just to
    /// observe type-initialisation order.</summary>
    internal static readonly bool WarmedUp;

    static PgSettingRedactor()
    {
        LibpqPasswordKeyword.Warmup();
        UriQueryPassword.Warmup();
        AssignmentSecretName.Warmup();
        OptionSecretSpaced.Warmup();
        UriQueryKeyAnyEncoding.Warmup();
        UriQuerySignature.Warmup();
        QuotedSpacedAssignment.Warmup();
        CurlUserColon.Warmup();
        SshpassOption.Warmup();

        try
        {
            UriUserInfoPassword.IsMatch(WarmupSample);
        }
        catch (RegexMatchTimeoutException)
        {
        }

        WarmedUp = true;
    }

    /// <summary>Every <see cref="TimeBoundPattern"/> this type warms at type initialisation (#4348) —
    /// exposed only so a test can pin that the warmup sample actually reaches each one's match step, not
    /// only its scan.</summary>
    internal static readonly TimeBoundPattern[] WarmedPatterns =
    {
        LibpqPasswordKeyword,
        UriQueryPassword,
        AssignmentSecretName,
        OptionSecretSpaced,
        UriQueryKeyAnyEncoding,
        UriQuerySignature,
        QuotedSpacedAssignment,
        CurlUserColon,
        SshpassOption,
    };

    private static bool IsQueryKeySecret(string rawKey)
    {
        string decoded;
        try
        {
            decoded = Uri.UnescapeDataString(rawKey);
        }
        catch (FormatException)
        {
            decoded = rawKey;
        }

        foreach (var marker in QueryKeySecretMarkers)
        {
            if (string.Equals(decoded, marker, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Masks the secret out of <paramref name="value"/> per the rules on this type, given the setting's
    /// <paramref name="name"/>. Never throws. <see langword="null"/> in, <see langword="null"/> out; an empty
    /// string is returned unchanged, since there is nothing in it to mask.
    ///
    /// <para>Every pattern below runs under a bounded match time (#4348): the lookaround-bearing ones under
    /// <see cref="MatchTimeout"/>, the rest under <see cref="RegexOptions.NonBacktracking"/>, whose match
    /// time is linear in the input and needs no timeout of its own. If ANY pattern still times out, the
    /// whole value is masked rather than partially redacted or left as-is — a value the redactor could not
    /// finish examining in time is treated the same as one it decided outright needed a full mask — and
    /// <paramref name="onMatchTimeout"/>, if given, is invoked with only the setting's NAME: never the value
    /// or any fragment of it.</para>
    /// </summary>
    public static string? Redact(string? name, string? value, Action<string?>? onMatchTimeout = null)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        if (IsWholeValueMasked(name))
        {
            return Mask;
        }

        try
        {
            var redacted = LibpqPasswordKeyword.Replace(value, static m => m.Groups["kw"].Value + "=" + Mask);
            redacted = UriUserInfoPassword.Replace(redacted, static m => "://" + m.Groups["user"].Value + ":" + Mask + "@");
            redacted = UriQueryPassword.Replace(redacted, static m => m.Groups["key"].Value + "=" + Mask);
            redacted = UriQueryKeyAnyEncoding.Replace(redacted, static m => IsQueryKeySecret(m.Groups["key"].Value)
                ? m.Groups["key"].Value + "=" + Mask
                : m.Value);
            redacted = UriQuerySignature.Replace(redacted, static m => m.Groups["key"].Value + "=" + Mask);
            redacted = QuotedSpacedAssignment.Replace(redacted, static m =>
            {
                var head = m.Groups["q"].Value + m.Groups["name"].Value + m.Groups["nameq"].Value + " = ";

                // The value is either separately quoted ("password" = "hunter2", valq matched — the value
                // carries its own opening and closing quote pair, kept around the mask), or it is bare and the
                // whole "name = value" assignment shares ONE quote pair, so the value's only closing quote is
                // the outer q coming back around via \k<q> ("password = hunter2" — only a trailing quote, no
                // separate opening one, goes after the mask).
                return m.Groups["valq"].Success
                    ? head + m.Groups["valq"].Value + Mask + m.Groups["valq"].Value
                    : head + Mask + m.Groups["q"].Value;
            });
            redacted = AssignmentSecretName.Replace(redacted, static m => m.Groups["name"].Value + "=" + Mask);
            redacted = OptionSecretSpaced.Replace(redacted, static m => m.Groups["opt"].Value + " " + Mask);
            redacted = CurlUserColon.Replace(redacted, static m => m.Groups["flag"].Value + " " + m.Groups["user"].Value + ":" + Mask);
            redacted = SshpassOption.Replace(redacted, static m => m.Groups["prefix"].Value + "-p " + Mask);

            return redacted;
        }
        catch (RegexMatchTimeoutException)
        {
            // The value is already masked below; a logging callback that throws must not surface past this
            // method, which is documented to never throw.
            try
            {
                onMatchTimeout?.Invoke(name);
            }
            catch
            {
            }

            return Mask;
        }
    }

    /// <summary>Names that would otherwise trip <see cref="WholeValueNameMarkers"/>'s dotted-name test but
    /// carry no secret — a password POLICY setting, not a password. Exact match,
    /// case-insensitive; anything ELSE with a marker in a dot-separated segment is still masked whole,
    /// including a real secret sitting in the same batch as one of these (the negative case the pin
    /// covers).</summary>
    private static readonly string[] NonSecretAllowlist =
    {
        "rds.accepted_password_auth_method",
        "rds.restrict_password_commands",
        "passwordcheck.min_password_length",
    };

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

        foreach (var allowed in NonSecretAllowlist)
        {
            if (string.Equals(name, allowed, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        // Extension-scoped settings only: a bare core GUC such as password_encryption never matches here,
        // no matter what its name contains — only a dotted name is tested at all. A marker in
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
