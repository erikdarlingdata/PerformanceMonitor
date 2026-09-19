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
/// The Long-Running Query alert's OPT-OUT knob (#3653 A5, ruling Q5): sessions whose <c>program_name</c> or
/// <c>login_name</c> matches an entry are NOT EVALUATED by the alert — not read, not counted toward the fire,
/// not fingerprinted, not rendered — as opposed to a mute rule, which silences a fire the engine has already
/// decided. The two lists are the two handles a permanent background request reliably carries: the
/// application that opened it and the principal it runs as.
///
/// <para><b>Why a knob and not a gate.</b> Measured on one production store class: 191 distinct sessions over
/// the 30-minute bar in 7 days, and the p90 of them was seen in 6,192 snapshots — these are not slow queries
/// but PERMANENT requests (service brokers, replication readers, agent loops, monitoring pollers) that will be
/// over any duration threshold forever. A persistence gate cannot help: they persist by definition. Coverage
/// is the fix — the alert should not be looking at them — and coverage is per-operator knowledge (which
/// programs and logins are the background on THIS estate), so it is a setting, not a constant. Erik ruled the
/// knob allowed; the "what are they" measurement that would seed it is the monitor seat's read.</para>
///
/// <para><b>Match rule, stated once.</b> Case-insensitive, whole-value, with ONE wildcard form: a trailing
/// <c>*</c> matches any suffix (<c>SQLAgent - TSQL JobStep*</c> covers every job step; <c>svc_*</c> every
/// service login). No leading or embedded wildcards — a pattern that can match anywhere is a pattern an
/// operator cannot predict the blast radius of, and <c>program_name</c> values are application-controlled
/// strings that share prefixes far more than they share infixes. <c>%</c> and <c>_</c> are literal in a
/// pattern (escaped before they reach <c>LIKE</c>), so a login named <c>etl_reader</c> matches only itself.
/// A null or empty session value matches nothing: an unnamed program is not "any program".</para>
///
/// <para><b>Applied IN THE READ, before the row cap, on both SKUs</b> — the shape that makes this different
/// from <c>excludedDatabases</c>, which is dropped client-side after the read. The read is
/// <c>ORDER BY total_elapsed_time_ms DESC LIMIT maxResults</c> (default 5), and the sessions this knob exists
/// for are the LONGEST-running on the server by construction, so a post-read filter would let five permanent
/// background requests fill the cap on every sweep and the real long-running query behind them would never
/// be seen at all — the alert made blind by the knob meant to make it accurate. <see cref="BuildSqlPredicate"/>
/// is the shared translation to a <c>LIKE</c> predicate so Lite's DuckDB read and Darling's PostgreSQL read
/// cannot drift in what a pattern means; <see cref="Excludes"/> is the same rule in C#, for the engine's
/// fakes and for any host that filters rows it already holds.</para>
/// </summary>
/// <param name="ProgramNames">Normalised <c>program_name</c> patterns (see <see cref="Normalize"/>).</param>
/// <param name="Logins">Normalised <c>login_name</c> patterns.</param>
public sealed record LongRunningQueryExclusions(IReadOnlyList<string> ProgramNames, IReadOnlyList<string> Logins)
{
    /// <summary>The empty knob — every session is evaluated. What every pre-#3653 caller and the High CPU
    /// card's maintenance probe pass: the probe wants exactly the population an operator would exclude here.</summary>
    public static readonly LongRunningQueryExclusions None = new(Array.Empty<string>(), Array.Empty<string>());

    /// <summary>The wildcard: a trailing <c>*</c> and nothing else.</summary>
    public const char Wildcard = '*';

    /// <summary>True when neither list has an entry, so the read can skip the predicate entirely and stay
    /// byte-identical to its pre-knob shape.</summary>
    public bool IsEmpty => ProgramNames.Count == 0 && Logins.Count == 0;

    /// <summary>Builds the knob from raw settings values, normalising both lists.</summary>
    public static LongRunningQueryExclusions From(IEnumerable<string>? programNames, IEnumerable<string>? logins) =>
        new(Normalize(programNames), Normalize(logins));

    /// <summary>
    /// Trims, drops blanks and de-duplicates case-insensitively, keeping first-seen order. The same treatment
    /// <c>excludedDatabases</c> gets on its way into the store, applied here so a setting typed as
    /// <c>"HammerDB, hammerdb , "</c> in a Settings window and one arriving as a JSON array through MCP mean
    /// the same thing. A bare <c>*</c> is dropped: it would exclude every named session, which is the alert's
    /// enable switch wearing a disguise.
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
            if (trimmed.Length == 0 || trimmed == Wildcard.ToString())
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

    /// <summary>The match rule in C# — see the type summary. Null/empty values match nothing.</summary>
    public static bool Matches(string? value, IReadOnlyList<string> patterns)
    {
        if (string.IsNullOrEmpty(value) || patterns.Count == 0)
        {
            return false;
        }

        foreach (var pattern in patterns)
        {
            if (pattern.Length > 0 && pattern[^1] == Wildcard)
            {
                if (value.StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            else if (string.Equals(value, pattern, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>True when the session is excluded by EITHER list.</summary>
    public bool Excludes(string? programName, string? loginName) =>
        Matches(programName, ProgramNames) || Matches(loginName, Logins);

    /// <summary>
    /// One pattern as a <c>LIKE</c> operand for <c>ESCAPE '\'</c>: the literal part with <c>\</c>, <c>%</c> and
    /// <c>_</c> escaped (backslash FIRST, for the reason <c>AlertContext.BuildDedupKeyLikePattern</c> states),
    /// plus a trailing <c>%</c> where the pattern ended in <see cref="Wildcard"/>.
    /// </summary>
    public static string ToLikeOperand(string pattern)
    {
        bool prefix = pattern.Length > 0 && pattern[^1] == Wildcard;
        var literal = prefix ? pattern[..^1] : pattern;
        var escaped = literal
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);
        return prefix ? escaped + "%" : escaped;
    }

    /// <summary>
    /// The exclusion as ONE SQL predicate over the two columns, with its operands as positional parameters
    /// numbered from <paramref name="firstParameterOrdinal"/> — the same text on DuckDB and PostgreSQL, both of
    /// which speak <c>ILIKE … ESCAPE '\'</c> (the alert-history stores already rely on the <c>LIKE … ESCAPE</c>
    /// form on both). Returns the empty string and no operands for an empty knob so the read's SQL stays
    /// byte-identical to its pre-knob shape.
    ///
    /// <para>The predicate is the POSITIVE match (<c>col ILIKE $n OR …</c>) rather than its negation, so a
    /// caller can both filter on <c>NOT (…)</c> and count on it — the fire payload's <c>Excluded Count</c> is
    /// that count. <c>COALESCE(col, '')</c> keeps a NULL column from making the whole OR unknown; an empty
    /// string matches no non-empty pattern, which is the "null matches nothing" rule.</para>
    /// </summary>
    /// <param name="programNameColumn">The SQL expression for <c>program_name</c>, e.g. <c>r.program_name</c>.</param>
    /// <param name="loginNameColumn">The SQL expression for <c>login_name</c>.</param>
    /// <param name="firstParameterOrdinal">The <c>$n</c> the first operand binds as; operands are returned in binding order.</param>
    public (string Predicate, IReadOnlyList<string> Operands) BuildSqlPredicate(
        string programNameColumn, string loginNameColumn, int firstParameterOrdinal)
    {
        if (IsEmpty)
        {
            return ("", Array.Empty<string>());
        }

        var terms = new List<string>();
        var operands = new List<string>();
        var ordinal = firstParameterOrdinal;

        foreach (var pattern in ProgramNames)
        {
            terms.Add($"COALESCE({programNameColumn}, '') ILIKE ${ordinal++} ESCAPE '\\'");
            operands.Add(ToLikeOperand(pattern));
        }

        foreach (var pattern in Logins)
        {
            terms.Add($"COALESCE({loginNameColumn}, '') ILIKE ${ordinal++} ESCAPE '\\'");
            operands.Add(ToLikeOperand(pattern));
        }

        return ("(" + string.Join(" OR ", terms) + ")", operands);
    }
}

/// <summary>
/// What the Long-Running Query read hands back since #3653 (A5, Q5): the sessions the alert evaluates, and how
/// many the <see cref="LongRunningQueryExclusions"/> knob removed from the same candidate set before the row
/// cap. The count exists so the fire payload can show an operator the knob WORKING — a setting whose effect is
/// only ever the absence of a page is a setting nobody can verify — and it has to come from the read because
/// the exclusion is applied there, ahead of the cap, where the engine cannot see the rows it removed.
/// </summary>
/// <param name="Sessions">The evaluated sessions, longest elapsed first, capped at the configured maximum.</param>
/// <param name="ExcludedCount">Sessions over the threshold in the same snapshot that the knob removed. Zero
/// when the knob is empty. When every candidate was excluded the read returns no rows and the alert has nothing
/// to fire, so the count is only ever rendered beside at least one evaluated session.</param>
public sealed record LongRunningQueryReadResult(List<LongRunningQueryInfo> Sessions, int ExcludedCount)
{
    /// <summary>No sessions, nothing excluded — the shape every pre-knob empty read had.</summary>
    public static LongRunningQueryReadResult Empty => new(new List<LongRunningQueryInfo>(), 0);
}
