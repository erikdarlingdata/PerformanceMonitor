/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
    /// Validates hours_back parameter. Returns null if valid, error message if invalid.
    /// </summary>
    public static string? ValidateHoursBack(int hoursBack)
    {
        if (hoursBack <= 0)
            return $"Invalid hours_back value '{hoursBack}'. Must be a positive integer (1-{MaxHoursBack}).";
        if (hoursBack > MaxHoursBack)
            return $"hours_back value '{hoursBack}' exceeds maximum of {MaxHoursBack} hours (7 days). Use a smaller value.";
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
    /// The <c>as_of</c> parameter's description, shared VERBATIM by every windowed read on both SKUs.
    ///
    /// <para>It is a constant rather than 100 hand-typed attribute strings for the reason the rest of the
    /// two-SKU parity rules exist: the same parameter described two different ways on two servers is a
    /// divergence no test would catch and every agent would notice.</para>
    /// </summary>
    public const string AsOfDescription =
        "Optional UTC anchor for the END of the window, ISO-8601 (2026-08-18T14:30:00Z, or 2026-08-18 for " +
        "midnight UTC). Omit to anchor at now. The window is the hours_back hours ENDING here, so a past " +
        "incident is 'as_of the end of it, hours_back its length' — widening hours_back is NOT the same " +
        "question, because a wider window is a different aggregate, not the same one with more rows.";

    /// <summary>
    /// <see cref="AsOfDescription"/> for the reads whose span is measured in DAYS rather than hours.
    ///
    /// <para>A separate constant rather than a reuse, because the shared one names <c>hours_back</c> in its
    /// own text. A parameter description that names a parameter the tool does not have is worse than a
    /// slightly generic one: the caller reads it, sends <c>hours_back</c>, and the key is ignored rather than
    /// rejected. Same contract, same anchor, same resolver — only the unit of the span differs.</para>
    /// </summary>
    public const string AsOfDaysDescription =
        "Optional UTC anchor for the LAST day of the range, ISO-8601 (2026-08-18T14:30:00Z, or 2026-08-18 for " +
        "midnight UTC; only the UTC date is used). Omit to anchor at today. The range is the days_back days " +
        "ENDING on that day, so a past month is 'as_of its last day, days_back its length' — widening " +
        "days_back is NOT the same question, because it asks about a different span of days rather than the " +
        "same one in more detail.";

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
            return $"Invalid as_of value '{asOf}'. Expected an ISO-8601 UTC instant: '2026-08-18T14:30:00Z', " +
                   "'2026-08-18T14:30:00' (read as UTC), '2026-08-18T16:30:00+02:00', or '2026-08-18' for midnight UTC.";
        }

        if (parsed > now + AsOfFutureTolerance)
        {
            return $"as_of value '{asOf}' is in the future. A stored read cannot cover data that has not been collected yet; anchor at or before now (UTC).";
        }

        endUtc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        return null;
    }

    /// <summary>
    /// Validates top/limit parameter. Returns null if valid, error message if invalid.
    /// </summary>
    public static string? ValidateTop(int top, string paramName = "limit")
    {
        if (top <= 0)
            return $"Invalid {paramName} value '{top}'. Must be a positive integer (1-{MaxTop}).";
        if (top > MaxTop)
            return $"{paramName} value '{top}' exceeds maximum of {MaxTop}. Use a smaller value.";
        return null;
    }

    /// <summary>
    /// Formats an exception as a user-friendly error message.
    /// </summary>
    public static string FormatError(string operation, Exception ex)
    {
        return $"Error during {operation}: {ex.Message}";
    }

    /// <summary>
    /// Builds a consistent JSON envelope for a NON-DATA outcome — a legitimate miss — so an LLM
    /// consumer can branch on the kind of nothing it got back. Data-bearing results keep their own
    /// shape and must NOT use this.
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
