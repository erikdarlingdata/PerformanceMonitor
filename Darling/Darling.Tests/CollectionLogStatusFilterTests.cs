/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3869: <c>get_collection_log</c>'s status filter — the half that needs no store.
///
/// <para>The behavioural assertions (a filtered page holding only matching statuses, the no-matches sentence,
/// the unchanged default payload) live in <see cref="DarlingCollectionLogReadTests"/> against live PostgreSQL,
/// because only a store can answer what a filter returned. What lives HERE is everything a unit can state:
/// the vocabulary itself, the refusal an unknown value produces, the SQL arm that applies the filter, and the
/// shared sentence both SKUs print. Those are the parts that would rot silently — a live class skipped on a
/// developer box asserts nothing at all, and every one of these facts is a promise made in a tool description
/// that a caller reads.</para>
/// </summary>
public sealed class CollectionLogStatusFilterTests
{
    /// <summary>
    /// The vocabulary is exactly what the store's writers write. Asserted as a SET against the literals each
    /// writer holds, so a new status reaching <c>collection_log</c> without joining this list fails here
    /// rather than becoming a value the filter refuses to its caller's face.
    ///
    /// <para>The writers, for the next person: <see cref="EnumeratedCollectorDriver.ClassifyReturnedRun"/>
    /// (SUCCESS) and <see cref="EnumeratedCollectorDriver.AbandonedStatus"/>; Lite's
    /// <c>RemoteCollectorService</c> (SKIPPED, YIELDED, ERROR); <c>DarlingWorker.PostgresFaultOutcome</c> and
    /// its sibling catch arms (PERMISSIONS, SESSION_MISSING, ERROR, YIELDED);
    /// <see cref="CollectorRuntimePrecondition.ExtensionMissingStatus"/>; and the two fleet-maintenance
    /// passes, whose partial-failure rollup writes WARNING.</para>
    /// </summary>
    [Fact]
    public void TheVocabularyIsExactlyWhatTheWritersWrite()
    {
        Assert.Equal(
            new[]
            {
                "ABANDONED", "ERROR", "EXTENSION_MISSING", "PERMISSIONS", "SESSION_MISSING",
                "SKIPPED", "SUCCESS", "WARNING", "YIELDED"
            },
            EnumeratedCollectorDriver.CollectionLogStatuses.OrderBy(s => s, StringComparer.Ordinal).ToArray());

        /* The three values that are constants elsewhere must BE those constants, not re-typed copies: a
           drifted spelling here would refuse the value the writer actually writes. */
        Assert.Contains(EnumeratedCollectorDriver.AbandonedStatus, EnumeratedCollectorDriver.CollectionLogStatuses);
        Assert.Contains(CollectorRuntimePrecondition.ExtensionMissingStatus, EnumeratedCollectorDriver.CollectionLogStatuses);
        Assert.Contains(EnumeratedCollectorDriver.ClassifyReturnedRun(abandoned: false), EnumeratedCollectorDriver.CollectionLogStatuses);

        /* Both freshness statuses are filterable. A caller asking "what SUCCEEDED" is asking the same kind
           of question as one hunting failures, and SKIPPED counts as success in every banding read. */
        foreach (var fresh in EnumeratedCollectorDriver.FreshnessSuccessStatuses)
        {
            Assert.Contains(fresh, EnumeratedCollectorDriver.CollectionLogStatuses);
        }

        /* And the statuses a caller might GUESS are not in it, which is what makes the refusal worth having.
           FAILURE is the issue reporter's own guess (#3869 names it); FAILED and SUCCEEDED are the other two
           natural spellings. Each is the misspelling that would otherwise return an empty page reading as
           "no failures". */
        foreach (var guessed in new[] { "FAILURE", "FAILED", "SUCCEEDED", "OK", "DENIED" })
        {
            Assert.DoesNotContain(guessed, EnumeratedCollectorDriver.CollectionLogStatuses);
        }
    }

    /// <summary>
    /// An unknown status VALUE is refused, and the refusal names every accepted value.
    ///
    /// <para>The loudness is the point, and it is #3870's promise for unknown ARGUMENT NAMES seen from the
    /// other side: an unknown key and an unknown value are one caller mistake with one quiet failure mode, a
    /// read that looks like it answered. On THIS filter the quiet version is the worst false negative the
    /// surface can produce — an equality filter on a typo returns an empty page, and the caller who typed it
    /// was asking "are there failures" during an incident.</para>
    /// </summary>
    [Fact]
    public void AnUnknownStatusValueIsRefused_NamingTheWholeSet()
    {
        var refusal = McpHelpers.ValidateChoice(
            "FAILURE", EnumeratedCollectorDriver.CollectionLogStatuses, "status");

        Assert.NotNull(refusal);

        /* It is the shared refusal ENVELOPE, so a client branches on `status` the way it branches on every
           other refused parameter rather than parsing prose. Asserted against the raw JSON. */
        Assert.Contains("\"status\":\"invalid\"", refusal, StringComparison.Ordinal);

        /*
            The SENTENCE is read out of the envelope rather than substring-matched against the JSON, because
            the serializer escapes the apostrophes the message quotes the caller's value with -- 'FAILURE'
            arrives on the wire as \u0027FAILURE\u0027. A pin matching the raw JSON therefore fails on text
            that is correct, which is how this assertion first went red: the escaping is what a CLIENT decodes
            away, so the client's view is what this test has to take.
        */
        var decoded = System.Text.Json.JsonDocument.Parse(refusal)
            .RootElement.GetProperty("message").GetString()!;

        Assert.Contains("Invalid status value 'FAILURE'", decoded, StringComparison.Ordinal);

        /* The parameter is named under hints, so a client knows WHICH argument to fix without parsing. */
        Assert.Equal(
            "status",
            System.Text.Json.JsonDocument.Parse(refusal)
                .RootElement.GetProperty("hints").GetProperty("parameter").GetString());

        /* Every member named, so the call is fixable from the answer alone. A refusal that says only "invalid"
           about a set the caller cannot see is a dead end. */
        foreach (var accepted in EnumeratedCollectorDriver.CollectionLogStatuses)
        {
            Assert.Contains(accepted, decoded, StringComparison.Ordinal);
        }

        /* Every real member is accepted, in any case the caller sends: the SQL arm compares UPPER($7), so
           refusing lowercase here would reject a value that would have matched. */
        foreach (var accepted in EnumeratedCollectorDriver.CollectionLogStatuses)
        {
            Assert.Null(McpHelpers.ValidateChoice(accepted, EnumeratedCollectorDriver.CollectionLogStatuses, "status"));
            Assert.Null(McpHelpers.ValidateChoice(accepted.ToLowerInvariant(), EnumeratedCollectorDriver.CollectionLogStatuses, "status"));
        }

        /* And an omitted filter is not a refusal: this is an optional narrowing, not a required argument. */
        Assert.Null(McpHelpers.ValidateChoice(null, EnumeratedCollectorDriver.CollectionLogStatuses, "status"));
        Assert.Null(McpHelpers.ValidateChoice("   ", EnumeratedCollectorDriver.CollectionLogStatuses, "status"));
    }

    /// <summary>
    /// The filter is applied in SQL, in the WHERE clause both orderings share, ahead of the cap — which is
    /// what keeps <c>truncated</c> and <c>run_count</c> describing the MATCHING rows. #3287 established that
    /// contract for the first two filters; a third applied to the returned page instead would quietly
    /// reinterpret both fields for exactly the caller hunting failures.
    /// </summary>
    [Fact]
    public void TheStatusFilterIsAPredicateInBothOrderings_AheadOfTheCap()
    {
        foreach (var sql in new[] { DarlingDataReader.CollectionLogSql, DarlingDataReader.CollectionLogSlowestFirstSql })
        {
            /* UPPER on the PARAMETER, never on the column: wrapping `status` would cost a scan the predicate
               is meant to narrow, and the stored vocabulary is already uppercase. */
            Assert.Contains("($7::text IS NULL OR status = UPPER($7::text))", sql, StringComparison.Ordinal);

            /* NULL-tolerant against an always-bound parameter, like its two predecessors, so every parameter
               keeps a FIXED position and a renumbering bug is impossible rather than unlikely. */
            Assert.Contains("($5::text IS NULL OR collector_name = $5::text)", sql, StringComparison.Ordinal);

            /* Ahead of the cap, stated positionally: the LIMIT closes each statement, so a predicate before
               it filters the set the cap then trims. */
            var predicate = sql.IndexOf("$7::text", StringComparison.Ordinal);
            var limit = sql.IndexOf("LIMIT $4", StringComparison.Ordinal);
            Assert.True(predicate >= 0 && limit > predicate,
                "the status predicate must sit ahead of the row cap, or truncated and run_count stop "
                + "describing the matching rows.");
        }
    }

    /// <summary>
    /// The shared no-matches sentence names the status, and — the reason this pin exists — still emits
    /// BYTE-IDENTICAL text for the three cases that were reachable before #3869.
    ///
    /// <para><c>DescribeCollectionLogFilters</c> was a pairwise cascade, one arm per SUBSET, which is growth
    /// that guarantees the next filter arrives with an arm missing. Rewriting it as a joined list is only safe
    /// if the old text is preserved exactly: <c>McpMissMessageParityPinTests</c> compares these sentences
    /// across the SKUs verbatim, and the live no-matches tests assert on fragments of them.</para>
    /// </summary>
    [Fact]
    public void TheNoMatchesSentenceNamesTheStatus_AndTheOldWordingIsUnchanged()
    {
        /* The three pre-#3869 cases, verbatim. */
        Assert.Equal("collector_name 'query_store'", McpHelpers.DescribeCollectionLogFilters("query_store", null));
        Assert.Equal("min_duration_ms 20000", McpHelpers.DescribeCollectionLogFilters(null, 20_000));
        Assert.Equal(
            "collector_name 'query_store' with min_duration_ms 20000",
            McpHelpers.DescribeCollectionLogFilters("query_store", 20_000));

        /* The new one, and the combinations. Named in the STORED spelling whatever case arrived, because the
           filter matched case-insensitively and a sentence echoing the caller's "error" beside rows reading
           "ERROR" would read as a mismatch rather than as what was applied. */
        Assert.Equal("status ERROR", McpHelpers.DescribeCollectionLogFilters(null, null, "error"));
        Assert.Equal(
            "collector_name 'query_store' with status PERMISSIONS",
            McpHelpers.DescribeCollectionLogFilters("query_store", null, "PERMISSIONS"));
        Assert.Equal(
            "collector_name 'query_store' with min_duration_ms 20000 with status ERROR",
            McpHelpers.DescribeCollectionLogFilters("query_store", 20_000, "ERROR"));

        /* Blank is absent, not a filter: whitespace must not produce "status " with nothing after it in the
           one sentence whose whole job is naming what applied. */
        Assert.Equal("no filters", McpHelpers.DescribeCollectionLogFilters(null, null, "  "));
        Assert.Equal("no filters", McpHelpers.DescribeCollectionLogFilters(null, null));
    }

    /// <summary>
    /// The fleet-maintenance miss sentences take the status filter too.
    ///
    /// <para>Those passes write SUCCESS and WARNING, so a status filter is a real question to ask of the
    /// <c>(fleet)</c> population — and the cadence sentence is the one that must not be reached unfiltered:
    /// it tells a caller an absent row means the pass did not RUN, which is false when the pass ran and
    /// merely did not match the filter.</para>
    /// </summary>
    [Fact]
    public void TheFleetMaintenanceMissNamesAStatusFilter_RatherThanClaimingThePassDidNotRun()
    {
        var (state, message) = DarlingMcpDataTools.FleetMaintenanceLogMiss(
            everRecorded: true, collectorName: null, minDurationMs: null, hoursBack: 24, status: "WARNING");

        Assert.Equal("empty", state);
        Assert.Contains("status WARNING", message, StringComparison.Ordinal);

        /* The unfiltered sentence must NOT be what a filtered read gets: "at all in the last 24 hour(s)" is a
           claim about the window that a filtered read has not looked at. */
        Assert.DoesNotContain("at all in the last", message, StringComparison.Ordinal);

        /* Positive control: unfiltered, the same call DOES make that claim, so the negative above is not
           passing against a sentence that never contains it. */
        var (_, unfiltered) = DarlingMcpDataTools.FleetMaintenanceLogMiss(
            everRecorded: true, collectorName: null, minDurationMs: null, hoursBack: 24);
        Assert.Contains("at all in the last", unfiltered, StringComparison.Ordinal);

        /* And a never-recorded store still outranks the filter: a fault outranks a miss, so the filtered
           sentence must not be reachable when no pass has ever written a row. */
        var (faultState, faultMessage) = DarlingMcpDataTools.FleetMaintenanceLogMiss(
            everRecorded: false, collectorName: null, minDurationMs: null, hoursBack: 24, status: "WARNING");
        Assert.Equal("unavailable", faultState);
        Assert.DoesNotContain("status WARNING", faultMessage, StringComparison.Ordinal);
    }
}
