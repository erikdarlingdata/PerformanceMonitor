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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Alerting;

/// <summary>One kept ring-buffer event for the read surface: the detail of one client timeout.</summary>
public sealed record ClientTimeoutSample(
    DateTime? EventTime,
    DateTime CollectionTime,
    string? DatabaseName,
    string? ClientAppName,
    string? ClientHostName,
    int? SessionId,
    long? InterruptDurationUs,
    string? StatementFingerprint,
    string? StatementText,
    bool BelowDurationFloor = false,
    bool AzdataMetadata = false,
    bool DbatoolsMetadata = false,
    bool MatchedCall = false);

/// <summary>One captured call that ended aborted and matches an attention sample under the duration floor (#9507): the command the client
/// cancelled. <see cref="ElapsedUs"/> is how long it ran, which is the length of the command timeout. The attention event's own duration
/// times the cancel, never the command.</summary>
public sealed record ClientTimeoutMatchedCall(
    DateTime? EventTime,
    DateTime CollectionTime,
    string? DatabaseName,
    string? ClientAppName,
    string? ClientHostName,
    int? SessionId,
    long? ElapsedUs,
    string? StatementText);

/// <summary>What <see cref="ClientTimeoutsRead.Classify"/> decided about a window.</summary>
public enum ClientTimeoutsKind
{
    /// <summary>No fresh successful run (or fewer than two): the collector did not observe this window. The status word is <c>unavailable</c>.</summary>
    NotObserved,

    /// <summary>The collector ran across the window and the exact count is zero. The status word is <c>empty</c>.</summary>
    Quiet,

    /// <summary>The collector ran and counted client timeouts.</summary>
    Data,
}

/// <summary>
/// What the response budget keeps of a <c>get_client_timeouts</c> reply (#9593, #9884): how many sampled events and how many matched calls it lists, and
/// the length a matched call's statement is cut to (null when no statement is cut). <see cref="ClientTimeoutsRead.FitToBudget"/> chooses it,
/// <see cref="ClientTimeoutsRead.Payload"/> writes it.
/// </summary>
public readonly record struct ClientTimeoutsFit(int SampledEvents, int MatchedCalls, int? MatchedCallTextLength = null);

/// <summary>
/// The <c>get_client_timeouts</c> read (#5640), written once for Lite and Darling: the same freshness and span rule the alert uses
/// (<see cref="AttentionEvaluator.ReadSpan"/>, which is <see cref="AttentionEvaluator.Read"/>'s rule over the whole range), the same
/// shared SQL (<see cref="AttentionWindowSql"/>), and one payload shape, so the two stores cannot answer differently. Each SKU keeps
/// only what is its own: the resolver, the engine and precondition envelopes, and the serializer.
///
/// <para><b>The count is exact; samples are not the total.</b> <c>count</c> and <c>rate_per_minute</c> come from the collector's
/// event_counter rows. The top lists and the sampled events come from a ring buffer that keeps at most 1,000 events and can miss
/// some, so they say what the timeouts looked like and never how many there were.</para>
/// </summary>
public static class ClientTimeoutsRead
{
    /// <summary>The sentence every data answer carries beside the sampled lists.</summary>
    public const string SamplesAreNotTheTotal =
        "count and rate_per_minute are exact (the session's event_counter). top_statements, top_databases, top_client_apps, top_hosts and sampled_events " +
        "come from sampled ring-buffer events, which are a subset: they show what the timeouts looked like, never how many there were. " +
        "A statement's text is returned once: a sampled event whose text is already in top_statements (or in an earlier event) has " +
        "statement null and statement_shown_in saying where to read it.";

    /// <summary>Applies the alert's freshness and span rules to the window as of <paramref name="windowEnd"/> (naive UTC).</summary>
    public static (ClientTimeoutsKind Kind, AttentionReading? Reading) Classify(AttentionWindow window, DateTime windowEnd)
    {
        var reading = AttentionEvaluator.ReadSpan(window, windowEnd);
        if (reading is null)
        {
            return (ClientTimeoutsKind.NotObserved, null);
        }

        return (reading.TotalCount > 0 ? ClientTimeoutsKind.Data : ClientTimeoutsKind.Quiet, reading);
    }

    /// <summary>
    /// The exclusive floor and inclusive ceiling the top lists and the sampled events are read with (#5640): the SAME cut
    /// <see cref="AttentionEvaluator.ReadSpan"/> makes of the counts and the sample groups (<see cref="AttentionEvaluator.RowSpan"/>), so
    /// <c>top_*</c> and <c>sampled_events</c> cover exactly the rows <c>sampled_events_in_range</c> counts, never the base run's samples from
    /// before the span or anything past the newest run's skew. Null when there is no span (fewer than two runs); the answer is then
    /// <see cref="ClientTimeoutsKind.NotObserved"/> and the detail reads are skipped. The boundary is rounded DOWN to a microsecond:
    /// both stores keep microseconds, so <c>collection_time &gt; floor</c> then agrees with the evaluator's tick comparison for every stored row.
    /// </summary>
    public static (DateTime Floor, DateTime Ceiling)? DetailSpan(IEnumerable<DateTime> runTimes)
    {
        var span = AttentionEvaluator.RowSpan(runTimes);
        if (span is null)
        {
            return null;
        }

        var (boundary, ceiling) = span.Value;
        return (new DateTime(boundary.Ticks - (boundary.Ticks % 10), boundary.Kind), ceiling);
    }

    /// <summary>The <c>unavailable</c> sentence: the collector has no recent successful run in the window.</summary>
    public static string UnavailableMessage(string serverName, AttentionWindow window, DateTime windowEnd, int hoursBack)
    {
        var newest = window.RunTimes.Count == 0 ? (DateTime?)null : window.RunTimes.Max();
        var last = newest is null
            ? $"no successful run of the attention collector in the {hoursBack}-hour window ending {Stamp(windowEnd)}"
            : $"its newest successful run in the window was at {Stamp(newest.Value)} UTC, and a window needs a recent run and at least two runs to count as observed";
        return $"The attention collector has not observed '{serverName}' recently: {last}. No client timeout count is claimed, " +
               "because a quiet run writes no row and an absent row is not a zero. Check get_collection_health for the attention collector.";
    }

    /// <summary>The <c>empty</c> sentence: the collector ran across the window and counted nothing.</summary>
    public static string EmptyMessage(string serverName, AttentionReading reading, int hoursBack) =>
        $"No client timeouts on '{serverName}': the attention collector ran {reading.RunCount} time{(reading.RunCount == 1 ? "" : "s")} " +
        $"across {reading.CoveredMinutes.ToString("N0", CultureInfo.InvariantCulture)} minutes of the last {hoursBack} hours and its counter did not move.";

    /// <summary>How many sampled events a reply holds before the response budget cuts any: the newest <paramref name="limit"/> of <paramref name="samples"/>.</summary>
    public static int EventsReturned(IReadOnlyList<ClientTimeoutSample> samples, int limit) => Math.Min(samples.Count, limit);

    /// <summary>How many matched calls a reply holds before the response budget cuts any: the newest <paramref name="limit"/> of the window's matched calls.</summary>
    public static int MatchedCallsReturned(AttentionWindow window, int limit) => Math.Min(window.MatchedCalls.Count, limit);

    /// <summary>
    /// #9593, #9884: the reply that fits <see cref="McpResponseBudget.DefaultBytes"/>, for both SKUs. <paramref name="reply"/> is the SKU's whole
    /// answer (its envelope and window notice around <see cref="Payload"/>) carrying what <see cref="ClientTimeoutsFit"/> keeps, so the envelope counts
    /// against the budget. Sampled events are listed newest collected_at first and matched calls newest first, so what is left off the end is the oldest.
    /// The sampled events go first. When the reply is still over the budget with none of them, the oldest matched calls are left off whole, down to one.
    /// When that one call alone is over the budget, its statement is cut to the longest head that fits. <c>count</c>, the rates and the top lists are
    /// never cut. The statement text a matched call carries is <paramref name="statementTextLength"/> characters at most, so that is the longest head.
    /// </summary>
    public static string FitToBudget(
        int eventsReturned, int matchedCallsReturned, int statementTextLength, Func<ClientTimeoutsFit, string> reply, int budgetBytes = McpResponseBudget.DefaultBytes)
    {
        bool Fits(string json) => System.Text.Encoding.UTF8.GetByteCount(json) <= budgetBytes;

        var events = McpResponseBudget.FitPrefix(eventsReturned, n => reply(new ClientTimeoutsFit(n, matchedCallsReturned)), budgetBytes, minimum: 0);
        if (Fits(events.Json) || matchedCallsReturned == 0)
        {
            return events.Json;
        }

        var calls = McpResponseBudget.FitPrefix(matchedCallsReturned, n => reply(new ClientTimeoutsFit(0, n)), budgetBytes, minimum: 1);
        if (Fits(calls.Json))
        {
            return calls.Json;
        }

        return McpResponseBudget.FitPrefix(statementTextLength, n => reply(new ClientTimeoutsFit(0, 1, n)), budgetBytes, minimum: 0).Json;
    }

    /// <summary>
    /// The sentence <c>budget_note</c> carries when the response budget left sampled events or matched calls out (#9593, #9884), null when it left none out.
    /// It names the block that held the reply over the budget and the remedy that shortens it, a smaller <c>limit</c>, which cuts <c>sampled_events</c> and
    /// <c>matched_calls</c> together. Paging with <c>as_of</c> is advised only while a sampled event is listed to page from: it moves back to the oldest
    /// returned collected_at, and it cannot shrink a reply whose matched calls are the same ones.
    /// </summary>
    private static string? BudgetNote(int eventsReturned, int eventsKept, int callsReturned, int callsKept, int? callTextLength)
    {
        var eventsCut = eventsKept < eventsReturned;
        var callsCut = callsKept < callsReturned || callTextLength is not null;
        if (!eventsCut && !callsCut)
        {
            return null;
        }

        const string SmallerLimit = "Pass a smaller limit, which shortens matched_calls and sampled_events together. ";

        /* The sampled events go first, so a cut matched call means none is listed: its sentence stands in for the as_of paging advice. */
        if (callsCut)
        {
            var leftOut = callsReturned - callsKept;
            var detail = FormattableString.Invariant($"matched_calls held the reply over the budget: ")
                + (leftOut > 0
                    ? FormattableString.Invariant($"only {callsKept:#,0} of the {callsReturned:#,0} matched calls fit (newest first) and {leftOut:#,0} were left out whole (matched_calls_left_out_for_budget)")
                    : "its one matched call does not fit whole")
                + (callTextLength is { } head
                    ? FormattableString.Invariant($", and the call listed has its statement cut to its first {head:#,0} characters (matched_calls_statement_cut_to)")
                    : "")
                + ". " + SmallerLimit + "count, the rates and the top lists are whole.";
            return eventsCut
                ? McpResponseBudget.PageCutNote(eventsReturned, eventsKept, "newest collected_at first", "sampled events", detail)
                : "TRUNCATED: " + detail;
        }

        /* Moving as_of back reads the oldest returned collected_at again and then older runs, so it needs a listed event to start from. */
        return eventsKept == 0
            ? McpResponseBudget.PageCutNote(eventsReturned, eventsKept, "newest collected_at first", "sampled events",
                "matched_calls and the blocks that stay whole leave no room for a sampled event, so none is listed to page from. " + SmallerLimit +
                "count, the rates, the top lists and matched_calls are whole.")
            : McpResponseBudget.PageCutNote(eventsReturned, eventsKept, "newest collected_at first", "sampled events",
                "Move as_of back to the oldest returned collected_at, with hours_back wide enough to reach the older events, to read them (that collector run is read again whole, so no event is skipped), or pass a smaller limit. " +
                "If every listed event has the same collected_at, that one run alone fills the reply and its older events cannot be paged this way. " +
                "count, the rates, the top lists and matched_calls are whole.");
    }

    /// <summary>
    /// The data answer's fields, in the order they are serialized (<paramref name="statementTextLength"/> cuts each sample text).
    /// <paramref name="fit"/> (#9593, #9884) is what the response budget keeps of the <paramref name="limit"/>-cut sampled events and matched calls, the
    /// newest first, and of a matched call's statement: <see cref="FitToBudget"/> chooses it, and null keeps everything the limit allows.
    /// </summary>
    public static object Payload(
        AttentionReading reading,
        AttentionWindow window,
        IReadOnlyList<ClientTimeoutSample> samples,
        int limit,
        int statementTextLength,
        ClientTimeoutsFit? fit = null)
    {
        var eventsReturned = EventsReturned(samples, limit);
        var kept = Math.Clamp(fit?.SampledEvents ?? eventsReturned, 0, eventsReturned);
        var callsReturned = MatchedCallsReturned(window, limit);
        var callsKept = Math.Clamp(fit?.MatchedCalls ?? callsReturned, 0, callsReturned);
        var callTextLength = callsKept > 0 ? fit?.MatchedCallTextLength : null;
        var rate = Math.Round(reading.TotalCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero);

        /* #6131: the same monitoring-tool T-SQL (1.5 KB) came back twice in one answer, once as a top statement's sample and
           again as the sampled event it was drawn from. A text is written the first time it appears (top_statements first,
           then the events newest first) and every later row carrying exactly that text points at it instead. */
        var shownAt = new Dictionary<string, string>(StringComparer.Ordinal);
        var topStatements = window.TopStatements.Select(t => new
        {
            statement_fingerprint = t.Name,
            sampled_events = t.Samples,
            sample_statement = McpHelpers.TruncateStatement(t.SampleText, statementTextLength),
        }).ToList();
        foreach (var top in topStatements)
        {
            if (!string.IsNullOrEmpty(top.sample_statement))
            {
                shownAt.TryAdd(top.sample_statement, "top_statements");
            }
        }

        var events = samples.Take(limit).Select((s, i) =>
        {
            var text = McpHelpers.TruncateStatement(s.StatementText, statementTextLength);
            string? earlier = null;
            if (!string.IsNullOrEmpty(text) && !shownAt.TryAdd(text, $"sampled_events[{i}]"))
            {
                earlier = shownAt[text];
            }

            return new
            {
                event_time = s.EventTime is null ? null : Stamp(s.EventTime.Value),
                collected_at = Stamp(s.CollectionTime),
                database_name = s.DatabaseName,
                client_app_name = s.ClientAppName,
                client_hostname = s.ClientHostName,
                excluded = AttentionEvaluator.IsExcludedApp(s.ClientAppName),
                below_duration_floor = s.BelowDurationFloor,
                matched_call = s.MatchedCall,
                azdata_metadata = s.AzdataMetadata,
                dbatools_metadata = s.DbatoolsMetadata,
                session_id = s.SessionId,
                interrupt_duration_ms = s.InterruptDurationUs is null ? (double?)null : s.InterruptDurationUs.Value / 1000.0,
                statement_fingerprint = s.StatementFingerprint,
                statement = earlier is null ? text : null,
                statement_shown_in = earlier,
            };
        }).ToList();

        return new
        {
            count = reading.TotalCount,
            rate_per_minute = rate,
            minutes_covered = Math.Round(reading.CoveredMinutes, 1, MidpointRounding.AwayFromZero),
            collector_runs = reading.RunCount,
            newest_run_time = Stamp(reading.NewestRunTime),
            sampled_events_in_range = reading.SampleTotal,
            samples_note = SamplesAreNotTheTotal,
            top_statements = topStatements,
            top_hosts = window.Hosts.Select(t => new { client_hostname = t.Name, sampled_events = t.Samples }).ToList(),
            top_databases = window.TopDatabases.Select(t => new { database_name = t.Name, sampled_events = t.Samples }).ToList(),
            top_client_apps = window.TopApplications.Select(t => new { client_app_name = t.Name, sampled_events = t.Samples, excluded = AttentionEvaluator.IsExcludedApp(t.Name) }).ToList(),
            excluded_from_alert = Excluded(reading, rate),
            matched_calls = window.MatchedCalls.Take(callsKept).Select(c => new
            {
                event_time = c.EventTime is null ? null : Stamp(c.EventTime.Value),
                collected_at = Stamp(c.CollectionTime),
                database_name = c.DatabaseName,
                client_app_name = c.ClientAppName,
                client_hostname = c.ClientHostName,
                excluded = AttentionEvaluator.IsExcludedApp(c.ClientAppName),
                session_id = c.SessionId,
                elapsed_seconds = c.ElapsedUs is null ? (double?)null : Math.Round(c.ElapsedUs.Value / 1_000_000.0, 3, MidpointRounding.AwayFromZero),
                statement = McpHelpers.TruncateStatement(c.StatementText, callTextLength ?? statementTextLength),
            }).ToList(),
            matched_calls_returned = callsKept,
            matched_calls_truncated = window.MatchedCalls.Count > limit,
            matched_calls_left_out_for_budget = callsReturned - callsKept,
            matched_calls_statement_cut_to = callTextLength,
            sampled_events_returned = kept,
            sampled_events_truncated = samples.Count > limit,
            budget_truncated = kept < eventsReturned || callsKept < callsReturned || callTextLength is not null,
            budget_note = BudgetNote(eventsReturned, kept, callsReturned, callsKept, callTextLength),
            sampled_events = events.Take(kept).ToList(),
        };
    }

    /// <summary>
    /// The cancels the Client Timeouts alert leaves out, SSMS IntelliSense's own (#6552) and the VS Code mssql extension's metadata
    /// connections (#6856): how many sampled events in the range came from them, and the count and rate the alert judges without them.
    /// The judged count is the exact count times the share of sampled events that are not left out, the same proportion <see cref="AttentionEvaluator.Evaluate"/> applies, so it is an estimate whenever
    /// <c>sampled_events</c> is above zero. <c>count</c> and <c>rate_per_minute</c> stay the exact event_counter figures.
    /// </summary>
    private static object Excluded(AttentionReading reading, double rate)
    {
        var excluded = reading.ExcludedSamples;
        /* #7061: the sampled events under the 1 s duration floor and Azure Data Studio's metadata statements are left out of the judged rate too. #8877: so are dbatools' SMO metadata reads. */
        var leftOut = excluded + reading.ShortSamples + reading.AzdataMetadataSamples + reading.DbatoolsMetadataSamples;
        var judgedCount = reading.SampleTotal > 0 && leftOut > 0
            ? reading.TotalCount * (double)(reading.SampleTotal - leftOut) / reading.SampleTotal
            : reading.TotalCount;
        return new
        {
            client_apps = reading.ExcludedApps,
            sampled_events = excluded,
            below_duration_floor_events = reading.ShortSamples,
            matched_calls = reading.MatchedSamples,
            azdata_metadata_events = reading.AzdataMetadataSamples,
            dbatools_metadata_events = reading.DbatoolsMetadataSamples,
            count_judged = Math.Round(judgedCount, 1, MidpointRounding.AwayFromZero),
            rate_per_minute_judged = leftOut > 0
                ? Math.Round(judgedCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero)
                : rate,
            note = AttentionEvaluator.ExcludedToolNames(true, true) + $" cancel their own metadata queries (language service, Object Explorer, general connection) when the user keeps typing. client_apps lists the exact client_app_name values in this range that the alert leaves out, most sampled events first. It has no cap, so it can name an application that top_client_apps (the top five) or the paged sampled_events do not show. The Client Timeouts alert also leaves out sampled events that lasted under {AttentionWindowSql.MinInterruptDurationText} (below_duration_floor_events: too short to be a client timeout, and no captured call that ended aborted matches them. matched_calls is the part under that floor that DOES match such a call, a command the client cancelled while it ran, so it is judged and not left out. The attention event times the cancel, so its length is not the timeout's length. The calls are listed in the top-level matched_calls, with how long each ran) and Azure Data Studio's own metadata statements (azdata_metadata_events, told apart by statement shape because its query editor uses the same application name) and the dbatools PowerShell module's SMO catalog reads (dbatools_metadata_events, told apart by SMO's @_msparam_ parameter marker in the statement text because the same application name runs people's own scripts). The alert leaves these " +
                   "sampled events out of the rate it judges (count_judged is an estimate from the sampled share); they stay listed in sampled_events. " +
                   "An SSMS IntelliSense or VS Code event shows excluded: true. An event under the duration floor that matches a captured call shows matched_call: true. An event under the duration floor that matches none shows below_duration_floor: true, an azdata metadata event shows azdata_metadata: true and a dbatools SMO metadata event shows dbatools_metadata: true, all with excluded: false.",
        };
    }

    private static string Stamp(DateTime naiveUtc) =>
        DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}
