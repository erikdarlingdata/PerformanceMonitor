/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>
/// Shared helpers for MCP tools.
/// </summary>
internal static class McpHelpers
{
    /// <summary>
    /// Maximum hours of history allowed (7 days).
    /// </summary>
    public const int MaxHoursBack = 168;

    /// <summary>
    /// Maximum days of history the daily-summary RANGE read allows (a year).
    ///
    /// <para>Separate from <see cref="MaxHoursBack"/> rather than derived from it: that ceiling is seven days,
    /// which is a sensible bound on a per-collection window and useless on a calendar whose whole premise is
    /// months. Bounded all the same, because the aggregate underneath scans the raw per-collection series for
    /// every signal except the query count. Shared so the two SKUs cannot accept different spans and answer
    /// the same question differently.</para>
    /// </summary>
    public const int MaxDailySummaryDaysBack = 366;

    /// <summary>
    /// Maximum rows/items to return.
    /// </summary>
    public const int MaxTop = 1000;

    /// <summary>
    /// Shared JSON serializer options for MCP tool results — compact, not indented (#2350).
    ///
    /// <para>The only consumer of an MCP tool result is a language model, and indentation buys a model
    /// nothing. It was costing roughly 23% of the bytes of a record-heavy result (measured on a 15-field
    /// blocking-event shape: 2,977 → 2,297 at 10 rows, 29,082 → 22,462 at 100). <b>The token saving is smaller
    /// than the byte saving</b> — BPE tokenizers pack runs of spaces efficiently — so this is not the 23%
    /// win it looks like in bytes. It is still free, and it compounds where it matters: tool results are the
    /// bulk of what fills an agent's context on a real incident, and the fleet-wide reads are the widest
    /// results we return.</para>
    ///
    /// <para>Deliberately NOT applied to the config files (servers.json, profiles, schedules, alert state).
    /// Those are read and hand-edited by people, and <c>ServerManager</c>/<c>ProfileManager</c>/
    /// <c>ScheduleManager</c> keep their own indented options for that reason. This object is MCP output only
    /// — every one of its ~78 call sites serializes a tool result or the web endpoint twin of one.</para>
    ///
    /// <para>Nothing parses our output positionally: it is JSON to a JSON reader on both sides, and the tests
    /// that touch this object assert field NAMES (there is no naming policy here, so snake_case comes from
    /// <c>[JsonPropertyName]</c> attributes) rather than layout.</para>
    /// </summary>
    public static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// Truncates a string to the specified maximum length, adding a truncation suffix.
    /// </summary>
    public static string? Truncate(string? value, int maxLength)
    {
        if (value == null || value.Length <= maxLength) return value;
        return value[..maxLength] + "... (truncated)";
    }

    /// <summary>
    /// Validates hours_back parameter. Returns null if valid, the <see cref="Refusal"/> envelope if invalid.
    ///
    /// <para><b>Every validator in this file returns the envelope, not the sentence (#3739).</b> The ~210
    /// call sites on both SKUs read <c>if (error != null) return error;</c>, so the value a validator hands back
    /// IS the tool's whole result — and until #3739 it was a bare sentence, the one outcome on the wire that
    /// was not JSON. Shaping it here, in the handful of producers, is what shapes four hundred tool returns
    /// without touching one of them. A caller that needs the WORDS (a test, a CLI, a web body that renders
    /// text) reads them back through <see cref="ErrorMessageOf"/>; nothing concatenates a validator's return
    /// any more, because it is no longer text.</para>
    /// </summary>
    public static string? ValidateHoursBack(int hoursBack)
    {
        if (hoursBack <= 0)
            return Refusal("hours_back", $"Invalid hours_back value '{hoursBack}'. Must be a positive integer (1-{MaxHoursBack}).");
        if (hoursBack > MaxHoursBack)
            return Refusal("hours_back", $"hours_back value '{hoursBack}' exceeds maximum of {MaxHoursBack} hours (7 days). Use a smaller value.");
        return null;
    }

    /// <summary>
    /// Validates a <c>days_back</c> parameter against the ceiling its tool declares. Returns null if valid, an
    /// error message if invalid — the day-grained twin of <see cref="ValidateHoursBack"/>, in the same sentence
    /// shape, so a caller who has seen one refusal recognises the other.
    ///
    /// <para><b>Why the ceiling is a parameter (#3653, one vocabulary).</b> Five tools took <c>days_back</c> and
    /// each refused it INLINE with its own copy of this sentence, because no shared validator existed for a
    /// day-grained span and their ceilings differ for real reasons: the daily-summary range reads a year
    /// (<see cref="MaxDailySummaryDaysBack"/>, shared across both SKUs), the stall-probe samples keep 60 days,
    /// the collector-cost series 90, the store-metrics daily series 400 — each the retention of the series it
    /// reads, which is the only honest bound on a history read. One validator with the ceiling handed in keeps
    /// the refusal one sentence on the wire while leaving each tool's bound where its lineage is documented.
    /// Refuses, never clamps: a span the tool cannot honour is an error, not a quietly different question.</para>
    /// </summary>
    public static string? ValidateDaysBack(int daysBack, int maxDaysBack)
    {
        if (daysBack <= 0 || daysBack > maxDaysBack)
            return Refusal("days_back", $"Invalid days_back value '{daysBack}'. Must be a positive integer (1-{maxDaysBack}).");
        return null;
    }

    /// <summary>
    /// Validates a windowed read's two time knobs together and hands back the window's END — the single call
    /// every windowed read makes in place of a bare <see cref="ValidateHoursBack"/>.
    ///
    /// <para>The span is checked BEFORE the anchor so a caller who sent both wrong is told about
    /// <c>hours_back</c> first, exactly as they were before <c>as_of</c> existed. <paramref name="endUtc"/> is
    /// only meaningful when this returns null.</para>
    /// </summary>
    public static string? ValidateWindow(int hoursBack, string? asOf, out DateTime endUtc)
    {
        endUtc = DateTime.UtcNow;

        var hoursError = ValidateHoursBack(hoursBack);
        if (hoursError != null)
        {
            return hoursError;
        }

        return ResolveAsOf(asOf, out endUtc);
    }

    /// <summary>
    /// The window validation for the three UNCAPPED reads (<c>get_collection_log</c>, <c>get_current_waits_trend</c>,
    /// <c>get_blocking_stats</c>): a positive span of any length, then the anchor — <see cref="ValidateWindow"/>
    /// minus its <see cref="MaxHoursBack"/> ceiling.
    ///
    /// <para><b>What this replaces (#3541 A13).</b> Those three tools never went through <see cref="ValidateHoursBack"/>
    /// because its 168-hour ceiling would take reach away from exactly the reads whose premise is looking further
    /// back than the default — and having stepped around the validator they <c>Math.Abs</c>'d the span instead.
    /// A caller who sent <c>hours_back = -24</c> asked a question with no meaning (a window that ends before it
    /// starts), and got the last 24 hours back with nothing to say the sign had been flipped: an answer to a
    /// different question, indistinguishable from a correct one, which is the silently-different-answer class
    /// every validator in this file exists to remove. Zero is refused with it — a zero-length window holds
    /// nothing by construction, and an empty result under a "genuinely quiet" sentence would be a lie.</para>
    ///
    /// <para>The refusal borrows <see cref="ValidateHoursBack"/>'s first sentence so a caller who has seen the
    /// capped reads' message recognises it, and then says the one thing that differs: there is no ceiling.</para>
    /// </summary>
    public static string? ValidateUncappedWindow(int hoursBack, string? asOf, out DateTime endUtc)
    {
        endUtc = DateTime.UtcNow;

        if (hoursBack <= 0)
        {
            return Refusal("hours_back", $"Invalid hours_back value '{hoursBack}'. Must be a positive integer — a negative or zero window has no meaning and is refused rather than read as its absolute value. This read has no upper bound on hours_back.");
        }

        return ResolveAsOf(asOf, out endUtc);
    }

    /// <summary>
    /// The ONLY spelling <c>summary_date</c> accepts on both SKUs' daily-summary tools: the ISO-8601 calendar
    /// date its own description has always promised.
    /// </summary>
    public const string SummaryDateFormat = "yyyy-MM-dd";

    /// <summary>
    /// Parses <c>get_daily_summary</c>'s <c>summary_date</c>: <paramref name="date"/> is <c>null</c> when the
    /// caller sent nothing (today, resolved by the reader), the UTC date when they sent a usable one. Returns
    /// null when usable, the refusal when not.
    ///
    /// <para><b>Exact, not general (#3541 A9).</b> This sat in the same file as <see cref="AsOfFormats"/>'s
    /// strict allowlist and used a general <see cref="DateTime.TryParse(string, IFormatProvider, DateTimeStyles, out DateTime)"/>,
    /// which under the invariant culture also accepts <c>01/02/2026</c> as <c>M/d/yyyy</c> — so a caller who
    /// meant 1 February was answered about 2 January, correctly formatted, with nothing to say so. The tool's
    /// description promised <c>yyyy-MM-dd</c>; the parser now agrees with it instead of exceeding it, exactly as
    /// <see cref="ResolveAsOf"/> does for <c>as_of</c>. The refusal names the one accepted form.</para>
    /// </summary>
    public static string? ParseSummaryDate(string? summaryDate, out DateTime? date)
    {
        date = null;
        if (string.IsNullOrWhiteSpace(summaryDate))
        {
            return null;
        }

        if (!DateTime.TryParseExact(
                summaryDate.Trim(),
                SummaryDateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                out var parsed))
        {
            return Refusal("summary_date", $"Invalid summary_date value '{summaryDate}'. Expected an ISO-8601 calendar date, yyyy-MM-dd (e.g. 2026-07-09), read as a UTC day. Other spellings — including 07/09/2026 — are refused rather than guessed at, because 01/02/2026 reads as two different days depending on who wrote it.");
        }

        date = DateTime.SpecifyKind(parsed, DateTimeKind.Utc).Date;
        return null;
    }

    /// <summary>
    /// Query Store's execution outcomes, spelled as <c>sys.query_store_runtime_stats.execution_type_desc</c>
    /// reports them and as both SKUs' collectors store them. <c>get_query_store_top</c>'s <c>execution_type</c>
    /// filter validates against this set through <see cref="ValidateChoice"/> and then filters on the canonical
    /// spelling, so a caller's "aborted" compares equal to the stored "Aborted".
    /// </summary>
    public static readonly IReadOnlyList<string> QueryStoreExecutionTypes = new[] { "Regular", "Aborted", "Exception" };

    /// <summary>
    /// The <c>empty</c> answer for an <c>execution_type</c> filter that matched nothing while the same read
    /// without it returns rows. Most queries never abort, so this is the common answer to an Aborted or
    /// Exception filter, and it is a measured zero: Query Store is collecting and the window has rows, just none
    /// with that outcome. Without it the read fell through to the "Query Store may not be enabled" guess, which
    /// is the one thing the unfiltered rows prove false. Shared so both SKUs say it in the same words.
    /// </summary>
    public static string QueryStoreExecutionTypeEmpty(string executionType, int hoursBack, string? databaseName)
    {
        var scope = string.IsNullOrWhiteSpace(databaseName) ? "" : $" in database '{databaseName}'";
        return Status(
            "empty",
            $"No {executionType} executions{scope} in the {hoursBack}-hour window searched. The same read without "
            + "execution_type returns rows, so Query Store is collecting and this is a measured zero, not missing "
            + "data. Omit execution_type to see the other outcomes.");
    }

    /// <summary>
    /// Validates an optional ENUMERATED filter — a parameter whose usable values are a closed set the
    /// caller cannot see. Returns null when the caller sent nothing or a member of the set, the refusal
    /// naming the whole set when not. The match is case-insensitive, and the caller is expected to use the
    /// canonical spelling from <paramref name="accepted"/> downstream rather than the caller's.
    ///
    /// <para><b>Refuses rather than filters to nothing (#3541 A13).</b> <c>get_analysis_facts</c> applied an
    /// unknown <c>source</c> as an equality filter and returned an empty list, under a description that
    /// documented four of the engine's source names — so a caller who typed the fifth read "no facts of that
    /// kind" for a value that could never have matched. An unknown member of a closed set is a caller error,
    /// and the refusal that lists the set is the only answer that lets the caller fix it.</para>
    /// </summary>
    public static string? ValidateChoice(string? value, IReadOnlyCollection<string> accepted, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        foreach (var candidate in accepted)
        {
            if (string.Equals(candidate, value.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
        }

        return Refusal(paramName, $"Invalid {paramName} value '{value}'. Accepted values: {string.Join(", ", accepted)}. Omit it for all.");
    }

    /// <summary>
    /// The <c>as_of</c> parameter's description, shared VERBATIM by every windowed read on both SKUs.
    ///
    /// <para>It is a constant rather than 100 hand-typed attribute strings for the reason the rest of the
    /// two-SKU parity rules exist: the same parameter described two different ways on two servers is a
    /// divergence no test would catch and every agent would notice.</para>
    ///
    /// <para><b>The rule, not the reasoning (#3898).</b> This text rides on 91 tools on Darling and 56 on
    /// Lite, so every character here is paid that many times in every client that loads the catalog; at 379
    /// characters it was a tenth of Darling's <c>tools/list</c>. It keeps what a caller must act on: the
    /// anchor is the window's END, a bare date is 00:00 UTC (the start of that day, so a whole-day question
    /// anchors on the next date), omitting it means now, and a past incident is <c>as_of</c> at its end, never
    /// a wider <c>hours_back</c>. The WHY of that last rule (a wider window is a different aggregate, not the
    /// same one with more rows) and the other accepted spellings live once, in both SKUs' server instructions
    /// under "Asking about a PAST window", and <c>AsOfWindowAnchorTests</c> holds both halves.</para>
    /// </summary>
    public const string AsOfDescription =
        "Optional ISO-8601 UTC END of the window (2026-08-18T14:30:00Z; 2026-08-18 = 00:00 UTC); " +
        "default now. For a past incident set as_of to its end; do not widen hours_back.";

    /// <summary>
    /// <see cref="AsOfDescription"/> for the reads whose span is measured in DAYS rather than hours.
    ///
    /// <para>A separate constant rather than a reuse, because the shared one names <c>hours_back</c> in its
    /// own text. A parameter description that names a parameter the tool does not have is worse than a
    /// slightly generic one: the caller reads it, sends <c>hours_back</c>, and the key is ignored rather than
    /// rejected. Same contract, same anchor, same resolver — only the unit of the span differs.</para>
    ///
    /// <para>Cut to the rule by #3898 for the reason its sibling was. Unlike that one, the anchor day is
    /// INCLUDED (the reader keeps only the UTC date and ends the range on it), which is why the text says
    /// LAST day and needs no 00:00 caveat.</para>
    /// </summary>
    public const string AsOfDaysDescription =
        "Optional ISO-8601 UTC LAST day of the range (2026-08-18; only the UTC date counts); default today. " +
        "For a past month set as_of to its last day; do not widen days_back.";

    /// <summary>
    /// The one clause every tool that publishes the WINDOW FLOOR carries in its description, on both SKUs
    /// (#3653 item 17, the second fact #3703 found under the page dialect's spelling).
    ///
    /// <para><c>truncated</c> meant two things on the wire. On every paged tool it is the page cut — the caller's
    /// <c>limit</c> bit, observed off a <c>cap + 1</c> fetch, beside a <c>*_returned</c> count, and the remedy is
    /// a bigger <c>limit</c>. On the #2364 / #2353 trend family (<c>get_query_trend</c>, the duration-trend
    /// trio) and on <c>get_query_store_top</c> it was the store's REACH — the served series begins later than
    /// the requested start because the tier that answered no longer holds the window's head — beside
    /// <c>effective_start</c> / <c>effective_hours_back</c>, and no <c>limit</c> changes it. A client that had
    /// learned the first meaning read the second as "raise the cap", which is exactly the wrong move. The
    /// window-floor fact is now spelled <c>window_truncated</c> (the <c>&lt;bound&gt;_truncated</c> dialect
    /// #3703 classified for a second bound in one payload), the page cut keeps <c>truncated</c>, and
    /// <c>McpPayloadContractCensusTests</c> holds the two apart on both SKUs.</para>
    ///
    /// <para>A constant for the reason <see cref="AsOfDescription"/> is one: the clause is the same true
    /// sentence on every tool that publishes the key, and a rename is a WIRE CHANGE the description has to
    /// own — a client still reading <c>truncated</c> off these tools reads a key that is no longer there and
    /// gets <c>undefined</c>, not <c>false</c>. Leading space: it is appended to each tool's own sentence.</para>
    ///
    /// <para>#3898 cut it to the fact and the remedy it rules out. The WIRE CHANGE sentence stays on the wire
    /// until the rename has shipped for one release (#3898's ruling on wire-change notices); it has not shipped
    /// in one yet, and no on-demand guide exists to carry it instead.</para>
    /// </summary>
    public const string WindowTruncatedDescription =
        " window_truncated is true when the store did not hold the start of the window; effective_start / " +
        "effective_hours_back say where the answer begins. That is the window floor, not a page cut: no limit " +
        "changes it. WIRE CHANGE: spelled truncated before #3653.";

    /// <summary>
    /// How far past <c>now</c> an <c>as_of</c> anchor may sit and still be accepted.
    ///
    /// <para>Not a grace period for asking about the future — it is the client-clock allowance. An agent that
    /// computes "now" from its own clock and sends it as <c>as_of</c> must not be refused because that clock
    /// runs a minute or two fast, and a stored read is answering out of a store whose newest row is minutes
    /// old anyway. Anything beyond this is a real request for data that cannot exist yet.</para>
    /// </summary>
    public static readonly TimeSpan AsOfFutureTolerance = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The ONLY spellings <c>as_of</c> accepts — an explicit allowlist rather than a general date parse.
    ///
    /// <para><see cref="DateTime.TryParse(string, IFormatProvider, DateTimeStyles, out DateTime)"/> would be
    /// the obvious choice and is the wrong one: under the invariant culture it also accepts <c>08/18/2026</c>
    /// as <c>M/d/yyyy</c>, so <c>01/02/2026</c> silently resolves to 2 January for a caller who meant 1
    /// February. That is the failure this parameter exists to remove — an answer to a subtly different
    /// question, with nothing to say so — one step removed from the silent fall back to "now" that IS refused.
    /// An allowlist makes the parser agree with the documented contract instead of exceeding it.</para>
    ///
    /// <para><c>K</c> matches an empty offset, a <c>Z</c>, and a <c>±HH:mm</c>, which is what lets one format
    /// cover all three documented shapes. A space in place of the <c>T</c> is deliberately NOT accepted: the
    /// refusal names the forms that work, which is more useful than guessing at a fifth one.</para>
    /// </summary>
    private static readonly string[] AsOfFormats =
    {
        "yyyy-MM-dd",
        "yyyy-MM-ddTHH:mmK",
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
    };

    /// <summary>
    /// Resolves the END of a windowed read from the optional <c>as_of</c> anchor: <paramref name="endUtc"/>
    /// receives <see cref="DateTime.UtcNow"/> when the caller sent nothing (the pre-#2495 behaviour, exactly),
    /// or the parsed instant when they did. Returns null when the anchor is usable, an error message when it
    /// is not.
    ///
    /// <para>REFUSES rather than substitutes, following <see cref="ValidateTop"/>: a caller who sent an anchor
    /// we could not use asked a specific question, and quietly answering a different one — silently falling
    /// back to now — is the failure mode this whole parameter exists to remove. A read that says "the last 4
    /// hours" when it was asked for "the 4 hours ending Tuesday 03:00" is indistinguishable from a correct
    /// answer.</para>
    ///
    /// <para>There is deliberately NO lower bound. An <c>as_of</c> older than anything the store holds is a
    /// legitimate question with an honest answer — the read's own <c>empty</c> / <c>unavailable</c> status —
    /// and the caller knows the anchor they sent, so that status is unambiguous. A hardcoded floor would have
    /// to guess at retention, which is per-deployment, per-server and per-collector, and would refuse real
    /// reads on a long-retention store. The SPAN is still bounded by <see cref="ValidateHoursBack"/>; only the
    /// anchor is free.</para>
    /// </summary>
    public static string? ResolveAsOf(string? asOf, out DateTime endUtc)
    {
        var now = DateTime.UtcNow;
        endUtc = now;

        if (string.IsNullOrWhiteSpace(asOf))
        {
            return null;
        }

        /* AssumeUniversal so a bare "2026-08-18T14:30:00" is read as UTC rather than as the SERVICE host's
           local time — the store is UTC throughout, the caller is an agent on some other machine, and a
           silent local-time reading would shift the window by the host's offset with nothing to show for it.
           AdjustToUniversal then normalises an explicit offset ("...+02:00") into the same UTC instant. */
        if (!DateTime.TryParseExact(
                asOf.Trim(),
                AsOfFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                out var parsed))
        {
            return Refusal("as_of", $"Invalid as_of value '{asOf}'. Expected an ISO-8601 UTC instant: '2026-08-18T14:30:00Z', " +
                   "'2026-08-18T14:30:00' (read as UTC), '2026-08-18T16:30:00+02:00', or '2026-08-18' for midnight UTC.");
        }

        if (parsed > now + AsOfFutureTolerance)
        {
            return Refusal("as_of", $"as_of value '{asOf}' is in the future. A stored read cannot cover data that has not been collected yet; anchor at or before now (UTC).");
        }

        endUtc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        return null;
    }

    /// <summary>
    /// Validates top/limit parameter. Returns null if valid, the <see cref="Refusal"/> envelope if invalid
    /// (<c>hints.parameter</c> is <paramref name="paramName"/>, so a tool that calls this for <c>top</c> refuses
    /// <c>top</c> by name).
    /// </summary>
    public static string? ValidateTop(int top, string paramName = "limit")
    {
        if (top <= 0)
            return Refusal(paramName, $"Invalid {paramName} value '{top}'. Must be a positive integer (1-{MaxTop}).");
        if (top > MaxTop)
            return Refusal(paramName, $"{paramName} value '{top}' exceeds maximum of {MaxTop}. Use a smaller value.");
        return null;
    }

    /// <summary>
    /// Turns a read fetched at <c>limit + 1</c> into the page the caller asked for and the truncation the extra
    /// row PROVES — the one place the page-contract rule "bound the page, not the request" (#3541 A3, #3594)
    /// is spelled in code rather than re-derived at every tool.
    ///
    /// <para>The mechanism: a tool that wants to say whether its window held more than <c>limit</c> rows cannot
    /// learn that from a page of <c>limit</c> rows. <c>rows.Count &gt;= limit</c> — the shape #3594 named and
    /// #3653 kept finding (<c>get_pg_server_config</c> and its three siblings, then <c>get_pg_deadlocks</c>,
    /// <c>get_pg_plan_capture_readiness</c>, <c>get_pg_index_bloat</c>) — says <b>more</b> for a window of
    /// exactly <c>limit</c> rows, and on a tool that withholds its summaries when truncated, withholds figures
    /// that were complete. Asking the reader for one row past the cap and testing <c>Count &gt; limit</c> is an
    /// OBSERVATION: the extra row either came back or it did not.</para>
    ///
    /// <para>The returned <c>Page</c> is <paramref name="fetched"/> itself when nothing was cut and the first
    /// <paramref name="limit"/> rows otherwise, so every count a caller takes off it — <c>*_returned</c>, the
    /// coverage verdict's <c>returnedRows</c>, a per-row summary — is a count of the page and never of the
    /// over-fetch. Any total meant to be the WINDOW's does not come from here: it is a <c>COUNT(*) OVER ()</c>
    /// on the reader's own statement, above the <c>LIMIT</c> (#3613's idiom).</para>
    ///
    /// <para>Deliberately tolerant of a fetch larger than <c>limit + 1</c> (a fingerprint scan ceiling, say):
    /// <c>Count &gt; limit</c> is the correct observation for any over-fetch, and the page is still the first
    /// <c>limit</c> rows in the reader's order. Not tolerant of an unvalidated cap — callers run
    /// <see cref="ValidateTop"/> first, as every paged tool already does, so <paramref name="limit"/> is
    /// positive here by construction. <c>McpPageContractTests</c> accepts either this helper or the proven
    /// inline spelling (<c>var truncated = rows.Count &gt; limit;</c> beside a <c>limit + 1</c> fetch); both
    /// are the observation, and the census's only enemy is the inference.</para>
    /// </summary>
    /// <param name="fetched">The rows a reader returned for a request of <c>limit + 1</c>.</param>
    /// <param name="limit">The caller's cap, already validated.</param>
    public static (IReadOnlyList<T> Page, bool Truncated) BoundPage<T>(IReadOnlyList<T> fetched, int limit)
    {
        var truncated = fetched.Count > limit;
        IReadOnlyList<T> page = truncated ? fetched.Take(limit).ToList() : fetched;
        return (page, truncated);
    }

    /// <summary>
    /// Validates an optional millisecond FLOOR — a <c>min_*_ms</c> filter. Returns null when the caller sent
    /// nothing or sent a usable value, an error message when the value cannot mean what it says.
    ///
    /// <para>REFUSES a negative rather than treating it as "no floor", following <see cref="ValidateTop"/>.
    /// A duration is non-negative, so a negative floor matches every row — and on a read where supplying the
    /// floor also changes the ORDERING, quietly accepting one hands back a differently-sorted full page with
    /// nothing to say the filter did not apply. That is the silently-dropped-parameter failure these filters
    /// exist to remove, so it is an error instead.</para>
    ///
    /// <para>ZERO is accepted and is NOT the same as omitting the parameter: it is the floor that admits every
    /// row, which on an ordering-switching read is how a caller asks to rank the whole window by duration
    /// rather than by time. Callers must therefore test for <c>null</c>, never for falsiness.</para>
    ///
    /// <para>NON-FINITE is refused, and checked FIRST because a negative test cannot see it: every comparison
    /// against <c>NaN</c> is false, and <c>+Infinity &lt; 0</c> is false too. All three are reachable from the
    /// web surface — <c>double.TryParse</c> under <c>NumberStyles.Float</c> accepts the literal
    /// <c>NaN</c> and <c>Infinity</c> regardless of the style flags, and it also accepts a merely OVERSIZED
    /// number like <c>1e400</c>, which silently overflows to <c>+Infinity</c> (measured, all four). None can
    /// ever match a run, so accepting one returns an empty, duration-RANKED page with nothing to say the
    /// floor was unusable: the silently-dropped-parameter failure this validator exists to remove, wearing a
    /// number instead of a typo.</para>
    /// </summary>
    public static string? ValidateMinMs(double? minMs, string paramName)
    {
        if (minMs is { } value && !double.IsFinite(value))
            return Refusal(paramName, $"Invalid {paramName} value '{Ms(minMs)}'. A duration floor must be a finite number — NaN and Infinity parse but can never match a run, so the read would come back empty and duration-ranked with nothing to say the floor was unusable. Note that an oversized number overflows to Infinity rather than being rejected as too large.");
        if (minMs is < 0)
            return Refusal(paramName, $"Invalid {paramName} value '{Ms(minMs)}'. A duration floor cannot be negative — use 0 to admit every row, or omit it entirely.");
        return null;
    }

    /// <summary>
    /// A millisecond figure rendered for a MESSAGE, invariant-culture.
    ///
    /// <para>The JSON payload spells these numbers invariantly, so a refusal or a no-matches message that
    /// rendered <c>2.5</c> as <c>2,5</c> under a comma-decimal host locale would give the caller two
    /// spellings of the value they sent, in the two places they compare.</para>
    /// </summary>
    private static string Ms(double? value) =>
        value?.ToString(CultureInfo.InvariantCulture) ?? "";

    /// <summary>
    /// The two values <c>get_collection_log</c>'s <c>order</c> field takes on BOTH SKUs.
    ///
    /// <para>Constants rather than inline literals for the reason <see cref="AsOfDescription"/> is one: this
    /// is a value an MCP client keys on, so the two products must spell it identically, and a token published
    /// to clients is a consumer API rather than a label.</para>
    /// </summary>
    public const string CollectionLogOrderNewestFirst = "collection_time_desc";

    /// <inheritdoc cref="CollectionLogOrderNewestFirst"/>
    public const string CollectionLogOrderSlowestFirst = "duration_ms_desc";

    /// <summary>
    /// Names <c>get_collection_log</c>'s active filters back to the caller, for the status message a filtered
    /// read with no matches returns.
    ///
    /// <para>Shared so both SKUs say the same words about the same state, which is the rule the tool's two
    /// empty branches already follow — a user moving between the products must not be told a different story
    /// about the same read. The phrase is written to slot after "…matched ", so it names the constraint rather
    /// than forming a sentence of its own.</para>
    ///
    /// <para>#3869 added the third filter and with it rewrote the shape: the pairwise cascade this method
    /// used to be needed one arm per SUBSET, so two filters cost three arms and three cost seven — growth
    /// that guarantees the next filter arrives with an arm missing, and a missing arm here silently drops a
    /// filter from the one sentence whose whole job is to name what was applied. A list joined with "with"
    /// emits exactly the same text for all three of the old reachable cases (the parity pin and the
    /// no-matches tests compare that text verbatim) and cannot omit a filter it was given.</para>
    /// </summary>
    public static string DescribeCollectionLogFilters(
        string? collectorName, double? minDurationMs, string? status = null)
    {
        var applied = new List<string>(3);

        if (!string.IsNullOrWhiteSpace(collectorName))
            applied.Add($"collector_name '{collectorName.Trim()}'");
        if (minDurationMs is not null)
            applied.Add($"min_duration_ms {Ms(minDurationMs)}");

        /* Named in the STORED spelling, not the caller's: the filter matched case-insensitively, so echoing
           "error" in a sentence about rows whose status reads "ERROR" would read as a mismatch rather than
           as the value that was applied. */
        if (!string.IsNullOrWhiteSpace(status))
            applied.Add($"status {status.Trim().ToUpperInvariant()}");

        /* Unreachable from every caller, which ask only when a filter was supplied. Written out rather than
           left to emit an empty string, because that would render "matched " with nothing after it -- a
           formatting bug in the one message whose whole job is to name what was applied, and one that would
           read as a product defect rather than as a misuse of this helper. */
        return applied.Count == 0 ? "no filters" : string.Join(" with ", applied);
    }

    /// <summary>
    /// The ONE wire shape a tool FAILURE takes, on both SKUs: the same JSON envelope <see cref="Status"/> builds
    /// for a miss, with <c>status</c> = <c>error</c>, the sentence <c>Error during {operation}: {ex.Message}</c> as
    /// <c>message</c>, and the operation named again under <c>hints.operation</c> so a client can branch on
    /// WHICH read failed without parsing the sentence. Every <c>catch</c> in every tool body returns through
    /// here (#3653 Q11); the census that holds that line is
    /// <c>McpPayloadContractCensusTests.EveryToolCatch_ReturnsThroughASharedErrorShape_AndTheAdHocRosterIsExact</c>.
    ///
    /// <para><b>The two-shape history.</b> Until #3653 this returned the bare sentence, and it was the error
    /// shape of every Lite tool and every SQL Server-family Darling tool — 214 call sites. The PostgreSQL tool
    /// files, written later, answered their catches with <c>Status("error", "Reading X failed: …")</c> instead:
    /// 30 catches in 21 files (twenty PostgreSQL tool files plus the collector-cost tool). #3699's census
    /// inventoried the split at file grain
    /// and #3703's vocabulary lane routed the one ad-hoc sentence (<c>list_servers</c>) through here, which left
    /// exactly TWO shared shapes on the wire — a sentence and an envelope — and a client keyed on <c>status</c>
    /// read two hundred tools' failures as successful prose. The maintainer's ruling (#3653 Q11) collapsed
    /// them onto this helper: one helper owns the shape AND the grammar, so the PostgreSQL catches now call
    /// this too and their "Reading X failed" dialect is retired. This is a WIRE CHANGE for every tool that
    /// used the sentence, including the frozen Dashboard twin, which links this assembly.</para>
    ///
    /// <para><b>What a client branches on.</b> <c>status</c>. <c>error</c> is a failure to answer — the read
    /// threw — and is the only status a caller should retry or report; the four miss words
    /// (<c>empty</c> / <c>not_collected</c> / <c>unavailable</c> / <c>precondition</c>) are answers ABOUT the
    /// data and are never produced here. The message text is unchanged from the sentence era so log greps
    /// and any pinned message string survive; <c>hints.operation</c> is the <paramref name="operation"/>
    /// verbatim — every call site passes its tool name. Data results keep their own shape and never carry a
    /// top-level <c>message</c>, which is what lets a consumer tell the envelope from data without a schema
    /// (the web dashboard's <c>classifyResponse</c> relies on exactly that).</para>
    ///
    /// <para><b>What it does NOT cover.</b> A REFUSAL — a request the tool cannot serve as given: a bad
    /// <c>hours_back</c>, an unresolvable <c>server_name</c>, a required parameter that was not sent — is not
    /// a failure and does not come through here. It has its own twin, <see cref="Refusal"/>, and its own
    /// status word, <c>invalid</c> (#3739): the read threw nothing, the caller has something to fix, and a
    /// web surface answers it 400 where a failure answers 500. Until #3739 those refusals were the validators'
    /// bare sentences on both SKUs (roughly four hundred return sites) and nine PostgreSQL refusals wore this
    /// helper's <c>error</c> word by hand, so the web surface read a client-correctable <c>limit</c> as a
    /// server fault. The census that keeps the two words apart is
    /// <c>McpPayloadContractCensusTests.EveryRefusal_ReturnsThroughTheSharedShape_AndTheBareSentenceRosterIsExact</c>;
    /// the only producer of <c>Status("error", …)</c> anywhere is this method.</para>
    /// </summary>
    /// <param name="operation">The tool name (every call site passes it); echoed in the sentence and as <c>hints.operation</c>.</param>
    /// <param name="ex">The caught exception; only its <see cref="Exception.Message"/> reaches the wire.</param>
    public static string FormatError(string operation, Exception ex)
    {
        return Status("error", ErrorSentence(operation, ex), new { operation });
    }

    /// <summary>
    /// The failure grammar itself — <c>Error during {operation}: {ex.Message}</c> — for the surfaces that
    /// render an error as TEXT rather than put it on the MCP wire (the triage page's per-section
    /// <c>error</c> field, the web surface's <c>{"error": …}</c> body). <see cref="FormatError"/> wraps this
    /// sentence in the envelope; nothing else spells the sentence, so the words a web card shows and the
    /// words an MCP client reads cannot drift.
    /// </summary>
    public static string ErrorSentence(string operation, Exception ex) => $"Error during {operation}: {ex.Message}";

    /// <summary>
    /// The ONE wire shape a tool REFUSAL takes, on both SKUs — <see cref="FormatError"/>'s twin for the fourth
    /// kind of outcome. A tool answers with data, with a miss (the four words <see cref="Status"/> documents), with
    /// a failure (<c>error</c>: it threw), or with a refusal: the request AS GIVEN cannot be served — a parameter
    /// the tool cannot honor, a required parameter that was not sent, a server name that resolves to nothing, a
    /// write body that will not parse. The refusal is the same envelope with <c>status</c> = <c>invalid</c>, the
    /// validator's sentence as <c>message</c> (unchanged from the bare-string era, so every pinned fragment and
    /// every log grep survives), and the parameter named under <c>hints.parameter</c> so a client can branch
    /// on WHAT to fix without parsing prose.
    ///
    /// <para><b>The word is <c>invalid</c>, widened, by ruling (#3739).</b> It was already the write tools'
    /// word for a body that would not parse (<c>Outcome("invalid", …)</c> across the mute-rule, alert-settings,
    /// custom-alert and custom-view verbs; the web write surface already mapped it to HTTP 400), and "a body
    /// that will not parse" is one instance of "the request as given cannot be served". So the read surface
    /// says the same word for the same kind of thing rather than coining a fifth one — <c>refused</c> /
    /// <c>rejected</c> were considered and not added — and ONE rule maps it on both web surfaces: <c>invalid</c>
    /// is 400, the envelope passed through as the body. <c>error</c> was the wrong word (the web reads it as a
    /// 500, and nine PostgreSQL refusals that borrowed it answered 500 for a bad <c>limit</c>), and no word at
    /// all was the state of the other four hundred: a bare sentence where every other outcome is JSON, which a
    /// client keyed on <c>status</c> — what the instructions teach it to key on — could not classify.</para>
    ///
    /// <para><b>Why the hint is the PARAMETER and not the operation.</b> The producers of a refusal are the
    /// shared validators and the server resolvers, which are called from four hundred tools and know nothing
    /// about which one; what they DO know, every one of them, is the parameter they refused — and that is
    /// also the thing the caller has to change. <see cref="FormatError"/> names the operation because a
    /// failure's useful question is "which read broke"; a refusal's is "which knob".</para>
    ///
    /// <para><b>Where it is built.</b> In the producers, not at the call sites: the validators in this file
    /// (<see cref="ValidateHoursBack"/>, <see cref="ValidateTop"/>, <see cref="ValidateDaysBack"/>,
    /// <see cref="ResolveAsOf"/>, <see cref="ParseSummaryDate"/>, <see cref="ValidateChoice"/>,
    /// <see cref="ValidateMinMs"/> and the two window validators over them), both SKUs' <c>ServerResolver</c>
    /// miss, the web dispatch's missing-parameter refusal, and the handful of tools that refuse a parameter of
    /// their own inline. The <c>if (error != null) return error;</c> idiom at every call site passes the
    /// envelope through untouched, which is why four hundred sites did not need to change.</para>
    /// </summary>
    /// <param name="parameter">The parameter that was refused, by its wire name (<c>hours_back</c>, <c>server_name</c>, …); echoed as <c>hints.parameter</c>.</param>
    /// <param name="sentence">The refusal itself: what was refused and what is accepted. Reaches the wire as <c>message</c>, verbatim.</param>
    public static string Refusal(string parameter, string sentence)
    {
        return Status("invalid", sentence, new { parameter });
    }

    /// <summary>
    /// The bytes every error envelope begins with. <see cref="Status"/> serializes its anonymous object with
    /// <see cref="JsonOptions"/> (compact) and <c>status</c> first, so <c>{"status":"error",</c> is exact against
    /// the one producer — the closing quote and comma are part of it so a data payload whose first key merely
    /// starts with <c>status</c>, or a status of <c>error_count</c>, cannot match. Pinned against
    /// <see cref="FormatError"/>'s and <see cref="Status"/>'s real output rather than trusted.
    /// </summary>
    public const string ErrorEnvelopePrefix = "{\"status\":\"error\",";

    /// <summary>
    /// The bytes every refusal envelope begins with — <see cref="ErrorEnvelopePrefix"/>'s twin for
    /// <see cref="Refusal"/>, exact against <see cref="Status"/> for the same reasons. It also matches the write
    /// tools' <c>Outcome("invalid", …)</c>, which serialize the same anonymous shape through the same options:
    /// that is the point, not an accident — one word, one recognizer, one HTTP code on both surfaces.
    /// </summary>
    public const string InvalidEnvelopePrefix = "{\"status\":\"invalid\",";

    /// <summary>
    /// Whether a tool result is the error envelope (a caught exception). A prefix test rather than a parse:
    /// the callers are the HTTP status mapping on the web surface and the test guards that used to read
    /// <c>StartsWith("Error during")</c>, both on the hot path of every response, and a parse of a
    /// record-heavy data page to learn it is not an error would cost more than the data did. Leading whitespace
    /// is tolerated the way the web surface's <c>{</c>-sniff tolerates it. Since #3739 a refusal is NOT this
    /// envelope — it is <see cref="IsRefusalEnvelope"/>'s — so the two consumers that split 500 from 400 can
    /// tell them apart with two prefix tests.
    /// </summary>
    public static bool IsErrorEnvelope(string? result) =>
        result is not null
        && result.AsSpan().TrimStart().StartsWith(ErrorEnvelopePrefix.AsSpan(), StringComparison.Ordinal);

    /// <summary>
    /// Whether a tool result is the refusal envelope (<see cref="Refusal"/>, or a write tool's
    /// <c>Outcome("invalid", …)</c>) — the client-correctable outcome the web surface answers 400 with the
    /// envelope as the body. The same prefix test as <see cref="IsErrorEnvelope"/>, for the same reasons.
    /// </summary>
    public static bool IsRefusalEnvelope(string? result) =>
        result is not null
        && result.AsSpan().TrimStart().StartsWith(InvalidEnvelopePrefix.AsSpan(), StringComparison.Ordinal);

    /// <summary>
    /// The sentence a human should read for a tool result: the envelope's <c>message</c> when the result is
    /// the error envelope OR the refusal envelope, the result itself otherwise (a bare string is already the
    /// sentence). For the consumers that render an outcome as TEXT — the web surface's <c>{"error": …}</c>
    /// body, the triage page's per-section error strip and its resolution note, the CLI's stderr, the
    /// <c>not_found</c> outcome a write tool builds around the resolver's miss — so a failure or a refusal is
    /// shown as the words that explain it rather than as the JSON that carried them. An envelope that will
    /// not parse falls back to the raw result rather than to nothing: hiding the payload is the one thing an
    /// error renderer must never do.
    /// </summary>
    public static string ErrorMessageOf(string result)
    {
        if (!IsErrorEnvelope(result) && !IsRefusalEnvelope(result))
        {
            return result;
        }

        try
        {
            using var document = JsonDocument.Parse(result);
            if (document.RootElement.TryGetProperty("message", out var message) && message.ValueKind == JsonValueKind.String)
            {
                return message.GetString()!;
            }
        }
        catch (JsonException)
        {
            /* Fall through to the raw result: the prefix matched but the body did not parse, which is not a
               shape this helper produces — show what arrived rather than guess. */
        }

        return result;
    }

    /// <summary>
    /// Builds a consistent JSON envelope for a NON-DATA outcome — a legitimate miss — so an LLM
    /// consumer can branch on the kind of nothing it got back. Data-bearing results keep their own
    /// shape and must NOT use this.
    ///
    /// <para>Two more status words ride this envelope and are NOT misses, and each has its own builder so one
    /// helper owns its grammar: <c>error</c> is a FAILURE (the read threw) and is built ONLY by
    /// <see cref="FormatError"/> — a tool's <c>catch</c> returns that, never this method directly, and since
    /// #3739 nothing else builds <c>Status("error", …)</c> either (a refusal that borrowed the word answered
    /// HTTP 500 for a bad <c>limit</c>); <c>invalid</c> is a REFUSAL (the request as given cannot be served — a
    /// parameter the tool cannot honor, a required one not sent, an unresolvable server name, a write body that
    /// will not parse) and is built by <see cref="Refusal"/>, or by the write tools' own <c>Outcome("invalid",
    /// …)</c>, which is the same word and the same bytes. A client reads six words on the wire: four kinds of
    /// nothing, one failure, one refusal.</para>
    /// </summary>
    /// <param name="status">
    /// One word from the small miss vocabulary:
    /// <list type="bullet">
    /// <item><c>empty</c> — a true negative: we looked and there is genuinely nothing (all clear).</item>
    /// <item><c>not_collected</c> — the input names something this server does not collect.</item>
    /// <item><c>unavailable</c> — it existed but is not retrievable now (evicted, purged, or not collected yet).</item>
    /// <item><c>precondition</c> — a setup step on the monitored server is unsatisfied, and the message says
    /// which one and how to satisfy it (#2546). Distinct from <c>not_collected</c>, which is permanent, and
    /// from <c>unavailable</c>, which sends the reader to collection health where they will find a collector
    /// that is running and doing its best. Re-derived on every read, so a precondition satisfied a minute ago
    /// stops being reported on the next call.</item>
    /// </list>
    /// </param>
    /// <param name="message">The human-readable explanation (kept intact from the prior bare-string text).</param>
    /// <param name="hints">Optional structured payload to help the caller recover (e.g. the counters that ARE collected). Omitted from the JSON when null.</param>
    public static string Status(string status, string message, object? hints = null)
    {
        return hints is null
            ? JsonSerializer.Serialize(new { status, message }, JsonOptions)
            : JsonSerializer.Serialize(new { status, message, hints }, JsonOptions);
    }
}
