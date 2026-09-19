/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The Long-Running Query alert's OPT-OUT knob (#3653 A5, ruling Q5): sessions whose <c>program_name</c> starts
/// with an entry of <see cref="ProgramNamePrefixes"/> or whose <c>login_name</c> equals an entry of
/// <see cref="Logins"/> are NOT EVALUATED by the alert — not read, not counted toward the fire, not
/// fingerprinted, not rendered — as opposed to a mute rule, which silences a fire the engine has already
/// decided. The two lists are the two handles a permanent background request reliably carries: the
/// application that opened it and the principal it runs as.
///
/// <para><b>Why a knob and not a gate.</b> Measured on one production store class: 191 distinct sessions over
/// the 30-minute bar in 7 days, and the p90 of them was seen in 6,192 snapshots — these are not slow queries
/// but PERMANENT requests that will be over any duration threshold forever. A persistence gate cannot help:
/// they persist by definition. Coverage is the fix — the alert should not be looking at them — and WHICH
/// sessions are the background is per-estate knowledge, so it is a setting. Erik ruled the knob allowed and
/// authorised the "what are they" read for the monitor seat; that read is what seeds the defaults below.</para>
///
/// <para><b>The four classes the production read found</b> (7 days, one large production store), which are
/// the whole reason the defaults have the shape they have:</para>
/// <list type="number">
/// <item><description><b>SQL Agent job-step programs</b> — <c>program_name</c> starting
/// <c>SQLAgent - TSQL JobStep</c> (the rest of the value embeds the job id and step, so only a PREFIX can name
/// the class): ~460 sessions a week across 11 single-server jobs, medians 35–62 minutes. DEFAULT-EXCLUDED by
/// program-name prefix. Anything of theirs that should still page is #3497's business — the card names the
/// job — not this knob's.</description></item>
/// <item><description><b><c>NT AUTHORITY\SYSTEM</c> and <c>NT AUTHORITY\NETWORK SERVICE</c> logins</b> — the
/// permanent multi-DAY background population (~70 sessions across 42 servers, medians 4.8–8.6 DAYS, 300K+
/// snapshots, CDC-capture shaped). DEFAULT-EXCLUDED by login, exactly.</description></item>
/// <item><description><b>The application's admin login</b> — NOT excluded, and deliberately so. It carries the
/// job wave, but it is also the login real ad-hoc long-runners arrive under, so excluding it would blind the
/// alert to the very thing it is for; the job-step prefix in (1) already removes its share of the
/// background. A login that does both jobs cannot be a default.</description></item>
/// <item><description><b>Named humans</b> — 3 sessions a week. NEVER default-excluded: they are what the page
/// is for.</description></item>
/// </list>
///
/// <para><b>The seeds are DEFAULTS, not constants.</b> <see cref="DefaultProgramNamePrefixes"/> and
/// <see cref="DefaultLogins"/> are what a fresh install and a settings store that has never held the key
/// evaluate with; an operator may add to them, replace them, or CLEAR a list, and an empty list excludes
/// nothing on that arm (a key that is present and empty is a decision; a key that is absent is not — each
/// host's settings reader tells the two apart). Both hosts follow that rule so a cleared knob stays cleared
/// across restarts.</para>
///
/// <para><b>Match rule, stated once.</b> Programs match by PREFIX, case-insensitively — job-step names, driver
/// strings and pooled-connection names are prefix-shaped by nature (the tail is an id) — and an entry is the
/// literal prefix: no wildcard grammar, so a <c>*</c> in an entry is just a character, and <c>%</c> / <c>_</c>
/// are escaped before they reach <c>LIKE</c>. Logins match EXACTLY, case-insensitively: a principal is a whole
/// name, and a prefix on logins would let <c>svc</c> swallow <c>svc_owner</c>. A null or empty session value
/// matches nothing: an unnamed program is not "any program". An empty entry is dropped on the way in (an empty
/// prefix would match every program — the alert's enable switch in disguise).</para>
///
/// <para><b>Applied IN THE READ, before the row cap, on both SKUs</b> — the shape that makes this different
/// from <c>excludedDatabases</c>, which is dropped client-side after the read. The read is
/// <c>ORDER BY total_elapsed_time_ms DESC LIMIT maxResults</c> (default 5), and the sessions this knob exists
/// for are the LONGEST-running on the server by construction, so a post-read filter would let five permanent
/// background requests fill the cap on every sweep and the real long-running query behind them would never
/// be seen at all — the alert made blind by the knob meant to make it accurate. <see cref="BuildSqlPredicates"/>
/// is the shared translation to two <c>LIKE</c> predicates so Lite's DuckDB read and Darling's PostgreSQL read
/// cannot drift in what an entry means; <see cref="Classify"/> is the same rule in C#, for the engine's fakes
/// and for any host that filters rows it already holds.</para>
///
/// <para><b>Counting what it removed.</b> The read reports two counts, <c>excluded_by_program_prefix</c> and
/// <c>excluded_by_login</c>, in SESSIONS (distinct <c>session_id</c> within the one snapshot the read looks at —
/// the (server_id, session_id, tran_start_time) identity the production read used collapses to
/// <c>session_id</c> inside a single collection of a single server; a MARS session with two request rows is
/// one session). A session matching BOTH arms counts ONCE, under the program prefix — the arm listed first
/// and the one an operator reaches for first — so the two counts sum to the sessions removed and never
/// double-count. The card shows both so an operator can see which default did the work.</para>
/// </summary>
/// <param name="ProgramNamePrefixes">Normalised <c>program_name</c> prefixes (see <see cref="Normalize"/>).</param>
/// <param name="Logins">Normalised exact <c>login_name</c> values.</param>
public sealed record LongRunningQueryExclusions(IReadOnlyList<string> ProgramNamePrefixes, IReadOnlyList<string> Logins)
{
    /// <summary>The seeded program-name prefix: SQL Agent T-SQL job steps (class 1 of the production read).
    /// The full value is <c>SQLAgent - TSQL JobStep (Job 0x… : Step N)</c>, which is why this is a prefix.</summary>
    public const string DefaultProgramNamePrefix = "SQLAgent - TSQL JobStep";

    /// <summary>The seeded logins (class 2 of the production read): the two Windows service principals that carry
    /// the permanent multi-day background population. Exact matches, case-insensitive.</summary>
    public const string DefaultLoginSystem = @"NT AUTHORITY\SYSTEM";

    /// <inheritdoc cref="DefaultLoginSystem"/>
    public const string DefaultLoginNetworkService = @"NT AUTHORITY\NETWORK SERVICE";

    /// <summary>The seeded <see cref="ProgramNamePrefixes"/> — what an install that has never set the knob evaluates with.</summary>
    public static readonly IReadOnlyList<string> DefaultProgramNamePrefixes = new[] { DefaultProgramNamePrefix };

    /// <summary>The seeded <see cref="Logins"/> — what an install that has never set the knob evaluates with.</summary>
    public static readonly IReadOnlyList<string> DefaultLogins = new[] { DefaultLoginSystem, DefaultLoginNetworkService };

    /// <summary>The knob as seeded: both default lists. What both SKUs evaluate with until an operator changes it.</summary>
    public static readonly LongRunningQueryExclusions Defaults = new(DefaultProgramNamePrefixes, DefaultLogins);

    /// <summary>The empty knob — every session is evaluated. What every pre-#3653 caller and the High CPU
    /// card's maintenance probe pass: the probe wants exactly the population an operator would exclude here.</summary>
    public static readonly LongRunningQueryExclusions None = new(Array.Empty<string>(), Array.Empty<string>());

    /// <summary>True when neither list has an entry, so the read can skip the predicate entirely and stay
    /// byte-identical to its pre-knob shape, and the card can omit the knob item.</summary>
    public bool IsEmpty => ProgramNamePrefixes.Count == 0 && Logins.Count == 0;

    /// <summary>Builds the knob from raw settings values, normalising both lists.</summary>
    public static LongRunningQueryExclusions From(IEnumerable<string>? programNamePrefixes, IEnumerable<string>? logins) =>
        new(Normalize(programNamePrefixes), Normalize(logins));

    /// <summary>
    /// Trims, drops blanks and de-duplicates case-insensitively, keeping first-seen order. The same treatment
    /// <c>excludedDatabases</c> gets on its way into the store, applied here so a setting typed as
    /// <c>"HammerDB, hammerdb , "</c> in a Settings window and one arriving as a JSON array through MCP mean
    /// the same thing. A blank is dropped rather than kept: an empty prefix would match every program, which
    /// is the alert's enable switch wearing a disguise.
    /// </summary>
    public static IReadOnlyList<string> Normalize(IEnumerable<string>? raw)
    {
        if (raw is null)
        {
            return Array.Empty<string>();
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();
        foreach (var entry in raw)
        {
            var trimmed = entry?.Trim() ?? "";
            if (trimmed.Length == 0)
            {
                continue;
            }

            if (seen.Add(trimmed))
            {
                result.Add(trimmed);
            }
        }

        return result;
    }

    /// <summary>The program arm's rule in C#: <paramref name="value"/> starts with any prefix, case-insensitively.
    /// Null/empty values match nothing.</summary>
    public static bool MatchesPrefix(string? value, IReadOnlyList<string> prefixes)
    {
        if (string.IsNullOrEmpty(value) || prefixes.Count == 0)
        {
            return false;
        }

        foreach (var prefix in prefixes)
        {
            if (prefix.Length > 0 && value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The login arm's rule in C#: <paramref name="value"/> equals any entry, case-insensitively.
    /// Null/empty values match nothing.</summary>
    public static bool MatchesExact(string? value, IReadOnlyList<string> values)
    {
        if (string.IsNullOrEmpty(value) || values.Count == 0)
        {
            return false;
        }

        foreach (var candidate in values)
        {
            if (string.Equals(value, candidate, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Which arm removes a session, if any — the C# spelling of the read's two flags, with the counts-once rule
    /// built in: a session matching both arms is <see cref="LongRunningQueryExclusionArm.ProgramPrefix"/>, never
    /// <see cref="LongRunningQueryExclusionArm.Login"/>. Fakes and in-memory hosts count from this so their
    /// two counts sum to the sessions removed exactly as the SQL counts do.
    /// </summary>
    public LongRunningQueryExclusionArm Classify(string? programName, string? loginName)
    {
        if (MatchesPrefix(programName, ProgramNamePrefixes))
        {
            return LongRunningQueryExclusionArm.ProgramPrefix;
        }

        return MatchesExact(loginName, Logins) ? LongRunningQueryExclusionArm.Login : LongRunningQueryExclusionArm.None;
    }

    /// <summary>True when the session is excluded by EITHER arm.</summary>
    public bool Excludes(string? programName, string? loginName) =>
        Classify(programName, loginName) != LongRunningQueryExclusionArm.None;

    /// <summary>
    /// A prefix as a <c>LIKE</c> operand for <c>ESCAPE '\'</c>: the literal with <c>\</c>, <c>%</c> and <c>_</c>
    /// escaped (backslash FIRST, or the escapes' own backslashes would be escaped again), then the trailing
    /// <c>%</c> the PREFIX rule means. The <c>%</c> is appended HERE, by the rule, never by an operator — an entry
    /// has no wildcard grammar.
    /// </summary>
    public static string ToPrefixLikeOperand(string prefix) => EscapeLike(prefix) + "%";

    /// <summary>An exact value as a <c>LIKE</c> operand for <c>ESCAPE '\'</c>: the literal, escaped, with no
    /// wildcard — <c>LIKE</c> is used only so the match is case-insensitive (<c>ILIKE</c>) on both stores.</summary>
    public static string ToExactLikeOperand(string value) => EscapeLike(value);

    private static string EscapeLike(string literal) =>
        literal
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);

    /// <summary>
    /// The two arms as two SQL boolean expressions over the two columns, with their operands as positional
    /// parameters numbered from <paramref name="firstParameterOrdinal"/> — program operands first, then logins,
    /// in list order — the same text on DuckDB and PostgreSQL, both of which speak <c>ILIKE … ESCAPE '\'</c>
    /// (the alert-history stores already rely on the <c>LIKE … ESCAPE</c> form on both). An arm with no
    /// entries is the literal <c>FALSE</c>, so a read can splice both expressions unconditionally and an
    /// empty knob's rows are exactly what the pre-knob read returned.
    ///
    /// <para>Each arm is the POSITIVE match (<c>col ILIKE $n OR …</c>) rather than its negation, so a caller
    /// can both filter on <c>NOT (program OR login)</c> and count on each flag — the fire payload's two counts
    /// are those counts, the login one taken <c>AND NOT</c> the program one so a session matching both counts
    /// once (see the type summary). <c>COALESCE(col, '')</c> keeps a NULL column from making an OR unknown; an
    /// empty string matches no non-empty operand, which is the "null matches nothing" rule.</para>
    /// </summary>
    /// <param name="programNameColumn">The SQL expression for <c>program_name</c>, e.g. <c>r.program_name</c>.</param>
    /// <param name="loginNameColumn">The SQL expression for <c>login_name</c>.</param>
    /// <param name="firstParameterOrdinal">The <c>$n</c> the first operand binds as; operands are returned in binding order.</param>
    public LongRunningQueryExclusionSql BuildSqlPredicates(string programNameColumn, string loginNameColumn, int firstParameterOrdinal)
    {
        var operands = new List<string>();
        var ordinal = firstParameterOrdinal;

        var programTerms = new List<string>();
        foreach (var prefix in ProgramNamePrefixes)
        {
            programTerms.Add($"COALESCE({programNameColumn}, '') ILIKE ${ordinal++} ESCAPE '\\'");
            operands.Add(ToPrefixLikeOperand(prefix));
        }

        var loginTerms = new List<string>();
        foreach (var login in Logins)
        {
            loginTerms.Add($"COALESCE({loginNameColumn}, '') ILIKE ${ordinal++} ESCAPE '\\'");
            operands.Add(ToExactLikeOperand(login));
        }

        return new LongRunningQueryExclusionSql(
            programTerms.Count == 0 ? "FALSE" : "(" + string.Join(" OR ", programTerms) + ")",
            loginTerms.Count == 0 ? "FALSE" : "(" + string.Join(" OR ", loginTerms) + ")",
            operands);
    }
}

/// <summary>Which arm of the <see cref="LongRunningQueryExclusions"/> knob removed a session — see
/// <see cref="LongRunningQueryExclusions.Classify"/> for the counts-once rule.</summary>
public enum LongRunningQueryExclusionArm
{
    /// <summary>Neither arm: the session is evaluated.</summary>
    None,

    /// <summary>The <c>program_name</c> prefix arm — also the answer when both arms match.</summary>
    ProgramPrefix,

    /// <summary>The exact <c>login_name</c> arm, and the program arm did not match.</summary>
    Login
}

/// <summary>
/// The two arms of the knob as SQL, from <see cref="LongRunningQueryExclusions.BuildSqlPredicates"/>: each a
/// boolean expression (the literal <c>FALSE</c> for an arm with no entries), plus every operand in binding
/// order — program operands first, then logins.
/// </summary>
/// <param name="ProgramPrefixPredicate">True for a row whose <c>program_name</c> starts with any configured prefix.</param>
/// <param name="LoginPredicate">True for a row whose <c>login_name</c> equals any configured login.</param>
/// <param name="Operands">The <c>LIKE</c> operands, in <c>$n</c> order.</param>
public sealed record LongRunningQueryExclusionSql(string ProgramPrefixPredicate, string LoginPredicate, IReadOnlyList<string> Operands);

/// <summary>
/// What the Long-Running Query read hands back since #3653 (A5, Q5): the sessions the alert evaluates, and how
/// many the <see cref="LongRunningQueryExclusions"/> knob removed from the same candidate set before the row
/// cap, split by the arm that removed them. The counts exist so the fire payload can show an operator the knob
/// WORKING — a setting whose effect is only ever the absence of a page is a setting nobody can verify — and
/// which default did the work; they have to come from the read because the exclusion is applied there, ahead
/// of the cap, where the engine cannot see the rows it removed.
/// </summary>
/// <param name="Sessions">The evaluated sessions, longest elapsed first, capped at the configured maximum.</param>
/// <param name="ExcludedByProgramPrefix">Distinct sessions over the threshold in the same snapshot whose
/// <c>program_name</c> matched a configured prefix — including any that ALSO matched a login (counts once, here).</param>
/// <param name="ExcludedByLogin">Distinct sessions over the threshold in the same snapshot whose <c>login_name</c>
/// matched a configured login and whose program did NOT match a prefix.</param>
public sealed record LongRunningQueryReadResult(List<LongRunningQueryInfo> Sessions, int ExcludedByProgramPrefix, int ExcludedByLogin)
{
    /// <summary>Sessions removed by either arm — the two counts' sum, exact because a session is counted under one arm only.</summary>
    public int ExcludedCount => ExcludedByProgramPrefix + ExcludedByLogin;

    /// <summary>No sessions, nothing excluded — the shape every pre-knob empty read had.</summary>
    public static LongRunningQueryReadResult Empty => new(new List<LongRunningQueryInfo>(), 0, 0);
}
