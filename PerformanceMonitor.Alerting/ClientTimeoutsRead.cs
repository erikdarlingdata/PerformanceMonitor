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

    /// <summary>The data answer's fields, in the order they are serialized (<paramref name="statementTextLength"/> cuts each sample text).</summary>
    public static object Payload(
        AttentionReading reading,
        AttentionWindow window,
        IReadOnlyList<ClientTimeoutSample> samples,
        int limit,
        int statementTextLength)
    {
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
            sampled_events_returned = Math.Min(samples.Count, limit),
            sampled_events_truncated = samples.Count > limit,
            sampled_events = events,
        };
    }

    /// <summary>
    /// SSMS IntelliSense's own cancels (#6552), which the Client Timeouts alert leaves out: how many sampled events in the range came from it,
    /// and the count and rate the alert judges without them. The judged count is the exact count times the share of sampled events that
    /// are not IntelliSense, the same proportion <see cref="AttentionEvaluator.Evaluate"/> applies, so it is an estimate whenever
    /// <c>sampled_events</c> is above zero. <c>count</c> and <c>rate_per_minute</c> stay the exact event_counter figures.
    /// </summary>
    private static object Excluded(AttentionReading reading, double rate)
    {
        var excluded = reading.ExcludedSamples;
        var judgedCount = reading.SampleTotal > 0 && excluded > 0
            ? reading.TotalCount * (double)(reading.SampleTotal - excluded) / reading.SampleTotal
            : reading.TotalCount;
        return new
        {
            client_apps = reading.ExcludedApps,
            sampled_events = excluded,
            count_judged = Math.Round(judgedCount, 1, MidpointRounding.AwayFromZero),
            rate_per_minute_judged = excluded > 0
                ? Math.Round(judgedCount / reading.CoveredMinutes, 2, MidpointRounding.AwayFromZero)
                : rate,
            note = AttentionEvaluator.ExcludedToolNames(true, true) + " cancel their own metadata queries (language service, Object Explorer, general connection) when the user keeps typing. client_apps lists the client_app_name values in this range that the alert leaves out, exactly as top_client_apps shows them. The Client Timeouts alert leaves these " +
                   "sampled events out of the rate it judges (count_judged is an estimate from the sampled share); they stay listed here, " +
                   "marked excluded.",
        };
    }

    private static string Stamp(DateTime naiveUtc) =>
        DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}
