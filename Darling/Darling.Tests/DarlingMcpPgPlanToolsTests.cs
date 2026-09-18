/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Wire shape for <c>get_pg_plans</c> (#2567), asserted against the real projection rather than a
/// re-implementation of it — a guard that rebuilt the shape would keep passing while the shipped one drifted.
/// </summary>
public class DarlingMcpPgPlanToolsTests
{
    /// <summary>A queryid past 2^53, which is where a JSON number stops being able to hold one exactly.</summary>
    private const long BigQueryId = -8126435036642491494;

    private static List<DarlingPgPlanCaptureReader.PgPlanCaptureRow> Rows() => new()
    {
        new DarlingPgPlanCaptureReader.PgPlanCaptureRow(
            QueryId: BigQueryId,
            PlanHash: "ABC123",
            TopNodeType: "Seq Scan",
            NodeCount: 3,
            Captures: 4,
            TotalDurationMs: 120.5,
            MaxDurationMs: 42.25,
            AvgDurationMs: 30.125,
            PlanJson: """{"Plan":{"Node Type":"Seq Scan","Relation Name":"orders","Filter":"(id > ?)"}}""",
            LastSeen: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
    };

    /// <summary>
    /// <b>queryid is a STRING on the wire.</b> It is a signed int8 spread over the whole 64-bit range, so
    /// most ids are past 2^53 and any consumer decoding JSON numbers as IEEE-754 doubles rounds one. It is
    /// also an equality join key, so a rounded value does not approximate the answer — it matches nothing.
    /// </summary>
    [Fact]
    public void QueryId_IsSerializedAsAString_AndSurvivesExactly()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0];
        var queryId = plan.GetProperty("queryid");

        Assert.Equal(JsonValueKind.String, queryId.ValueKind);
        Assert.Equal("-8126435036642491494", queryId.GetString());

        /* The point of the string, demonstrated: the same value through a double loses its identity. */
        Assert.NotEqual(BigQueryId, (long)(double)BigQueryId);
    }

    /// <summary>
    /// The plan is returned as parsed JSON, not as an opaque string and not as an id to fetch later. #2538
    /// is explicit: an agent has no viewer to follow a reference into, so a pointer is not an answer.
    /// </summary>
    [Fact]
    public void ThePlanItself_IsReturnedAsNavigableJson()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0].GetProperty("plan");

        Assert.Equal(JsonValueKind.Object, plan.ValueKind);
        Assert.Equal("Seq Scan", plan.GetProperty("Plan").GetProperty("Node Type").GetString());
        Assert.Equal("orders", plan.GetProperty("Plan").GetProperty("Relation Name").GetString());
    }

    /// <summary>
    /// <c>captures</c> is named for what it counts. The collector reads an overlapping tail of the server
    /// log, so one execution can be seen twice — calling it <c>executions</c> would invite a reader to
    /// divide by it.
    /// </summary>
    [Fact]
    public void CaptureCount_IsNotPresentedAsAnExecutionCount()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var plan = doc.RootElement.GetProperty("plans")[0];

        Assert.Equal(4, plan.GetProperty("captures").GetInt64());
        Assert.False(plan.TryGetProperty("executions", out _));
        Assert.False(plan.TryGetProperty("calls", out _));
    }

    /// <summary>
    /// The response says the plans are redacted. A consumer that does not know this could reasonably read a
    /// placeholder-bearing filter as the literal the query used.
    /// </summary>
    [Fact]
    public void TheResponseSaysThePlansAreRedacted()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, Rows(), 10));

        var note = doc.RootElement.GetProperty("note").GetString();

        Assert.Contains("REDACTED", note, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("get_pg_top_queries", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// A plan that will not re-parse is still shown, as its raw text. Dropping it would hide the one row
    /// somebody needs to explain, and throwing would take out the whole response for it.
    /// </summary>
    [Fact]
    public void AnUnparseablePlan_IsShownRatherThanDropped()
    {
        var rows = new List<DarlingPgPlanCaptureReader.PgPlanCaptureRow>
        {
            Rows()[0] with { PlanJson = "{not valid json" },
        };

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildPlansJson("srv", 24, rows, 10));

        var plan = doc.RootElement.GetProperty("plans")[0].GetProperty("plan");

        Assert.Equal(JsonValueKind.String, plan.ValueKind);
        Assert.Equal("{not valid json", plan.GetString());
    }

    /* ── the queryid filter and the empty answers (#3533) ── */

    /// <summary>
    /// The queryid filter runs IN the SQL, over every capture in the window. It used to be applied in C#
    /// over a fetched top-duration page, which made any plan ranked below the page unfindable — the ranking
    /// is by total duration, so a cheap-but-asked-about statement sits arbitrarily far down and no page
    /// size reaches it. NULL must leave the read as the top page, which is what the OR arm is.
    /// </summary>
    [Fact]
    public void TheQueryIdFilter_RunsInTheStore_NotOverAFetchedPage()
    {
        var sql = DarlingPgPlanCaptureReader.PgPlanCaptureSql;

        Assert.Contains("($4::bigint IS NULL OR query_id = $4)", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $5", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The queryid miss says what was actually searched — the whole window, not a page — and names what a
    /// miss can mean: the statement never ran (get_pg_top_queries confirms), it never crossed the capture
    /// threshold, or capture was not working when it ran (get_pg_plan_capture_readiness has the facets).
    /// The text this replaced declared the query "not the query to look at" over a fetched page the plan
    /// could legitimately sit below, which is the confident wrong verdict #3533 exists to remove.
    /// </summary>
    [Fact]
    public void TheQueryIdMiss_SaysTheWholeWindowWasSearched_AndWhatAMissCanMean()
    {
        var text = DarlingMcpPgPlanTools.NoPlanCapturedMessage(BigQueryId, 24);

        Assert.Contains("not a top-N page", text, StringComparison.Ordinal);
        Assert.Contains("the last 24 hour(s)", text, StringComparison.Ordinal);
        Assert.Contains("get_pg_top_queries", text, StringComparison.Ordinal);
        Assert.Contains("auto_explain.log_min_duration", text, StringComparison.Ordinal);
        Assert.Contains("get_pg_plan_capture_readiness", text, StringComparison.Ordinal);
        Assert.Contains("plan_content_retention_days", text, StringComparison.Ordinal);

        Assert.DoesNotContain("not the query to look at", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The unfiltered miss is a statement about EVERY statement — the read has no filter, so zero rows
    /// means the window is genuinely empty, and the message must not borrow the per-query verdict.
    /// </summary>
    [Fact]
    public void TheUnfilteredMiss_IsAboutEveryStatement_AndNamesBothCauses()
    {
        var text = DarlingMcpPgPlanTools.NoPlanCapturedMessage(null, 48);

        Assert.Contains("every statement", text, StringComparison.Ordinal);
        Assert.Contains("the last 48 hour(s)", text, StringComparison.Ordinal);
        Assert.Contains("auto_explain.log_min_duration", text, StringComparison.Ordinal);
        Assert.Contains("plan_content_retention_days", text, StringComparison.Ordinal);

        Assert.DoesNotContain("not the query to look at", text, StringComparison.Ordinal);
    }

    /* ── get_pg_plan_capture_readiness (#3070) ── */

    /// <summary>
    /// The facets as the shared reader hands them over: causal order, one unsatisfied facet in the middle
    /// of the sequence and two at the end, so a projection that sorted by satisfaction would show.
    ///
    /// <para>All six the collector emits, <c>message_locale</c> (#3061) included, because it is the one that
    /// reaches past plan capture — it reports whether the target writes its log in English, which every
    /// target-side log read matches — and it is last in the causal order rather than absent from it.</para>
    /// </summary>
    private static List<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow> Facets() => new()
    {
        new(Facet: "extension_available", IsSatisfied: true, Observed: "(loaded, and therefore available)",
            Detail: "auto_explain is available on this server.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "library_loaded", IsSatisfied: true, Observed: "auto_explain",
            Detail: "auto_explain must be listed in shared_preload_libraries.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "capture_threshold", IsSatisfied: false, Observed: "-1",
            Detail: "auto_explain IS loaded but log_min_duration is -1, so it captures NOTHING.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "plan_text_setting", IsSatisfied: true, Observed: "json",
            Detail: "The format auto_explain writes plans in.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "plan_attribution", IsSatisfied: false, Observed: "%m [%p] ",
            Detail: "log_line_prefix does NOT carry %Q, so every captured plan is an orphan.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
        new(Facet: "message_locale", IsSatisfied: false, Observed: "de_DE.UTF-8",
            Detail: "PostgreSQL writes its own messages in this locale, severity label included.", CaptureTime: new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc)),
    };

    /// <summary>
    /// <b><c>detail</c> reaches the wire on every row.</b> It is the per-facet remedy and the reason this
    /// read exists: before #3070 its only reader was the WPF tab, so on a Linux host and to every agent the
    /// remedy did not exist. A projection that carried facet and observed only would look complete.
    ///
    /// <para>The satisfied facets are asserted present for the same reason. The other path to these rows,
    /// <c>UnsatisfiedFacetsAsync</c>, filters to <c>is_satisfied IS NOT TRUE</c> and cannot answer "what is
    /// the state of this server's plan capture" at all — which is the question somebody has when the plans
    /// are missing, and the gap this tool closes rather than duplicating.</para>
    ///
    /// <para>And the ORDER is the reader's, not re-sorted here: the facets have a causal sequence
    /// (<c>library_loaded</c> gates <c>capture_threshold</c>) and the remedies only make sense read in it.
    /// </para>
    /// </summary>
    [Fact]
    public void EveryReadinessFacet_CarriesItsRemedy_InTheReadersCausalOrder()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, Facets(), 25));
        var root = doc.RootElement;

        Assert.Equal(6, root.GetProperty("facet_count").GetInt32());
        Assert.False(root.GetProperty("truncated").GetBoolean());

        var names = new List<string>();
        foreach (var facet in root.GetProperty("facets").EnumerateArray())
        {
            names.Add(facet.GetProperty("facet").GetString()!);
            Assert.False(string.IsNullOrWhiteSpace(facet.GetProperty("detail").GetString()));
            Assert.False(string.IsNullOrWhiteSpace(facet.GetProperty("observed").GetString()));
            Assert.True(facet.TryGetProperty("is_satisfied", out _));
            Assert.True(facet.TryGetProperty("last_observed", out _));
        }

        Assert.Equal(
            new[] { "extension_available", "library_loaded", "capture_threshold", "plan_text_setting", "plan_attribution", "message_locale" },
            names);
    }

    /// <summary>
    /// The unsatisfied facets are NAMED rather than reduced to a verdict. There is deliberately no
    /// ready/not-ready boolean: an unmet <c>plan_attribution</c> still captures plans and merely orphans
    /// them, and <c>message_locale</c> is about every target-side log read rather than about capture, so one
    /// flag would have to pick a meaning and be wrong under the other — the exact collapse the collector
    /// splits its rows to avoid.
    /// </summary>
    [Fact]
    public void TheUnsatisfiedFacetsAreNamed_AndThereIsNoSingleVerdict()
    {
        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, Facets(), 25));
        var root = doc.RootElement;

        var unmet = root.GetProperty("unsatisfied_facets");
        Assert.Equal(JsonValueKind.Array, unmet.ValueKind);
        Assert.Equal(new[] { "capture_threshold", "plan_attribution", "message_locale" },
            unmet.EnumerateArray().Select(f => f.GetString()).ToArray());

        Assert.False(root.TryGetProperty("ready", out _));
        Assert.False(root.TryGetProperty("is_ready", out _));
    }

    /// <summary>
    /// <c>detail</c> travels VERBATIM — not summarised, not truncated, not reflowed.
    ///
    /// <para>The facet details are long on purpose and the load-bearing parts are at the far end of them.
    /// <c>message_locale</c>'s satisfied arm spends its last clause stating its own limit — <c>lc_messages</c>
    /// is overridable per role and per database, so an override aimed at the monitoring login makes the row
    /// agree with itself and not with the server — and its unset arm exists to say that empty is UNKNOWN
    /// rather than safe. A projection that shortened these for readability would drop exactly the caveats
    /// that stop a reader over-trusting a satisfied row, so the assertion is character-for-character
    /// equality against a string with everything awkward in it: quotes, uppercase emphasis, an
    /// <c>lc_messages = 'C'</c> remedy, and a tail sentence past any plausible truncation point.</para>
    /// </summary>
    [Fact]
    public void TheDetailTravelsVerbatim_BecauseItsCaveatsAreAtTheEndOfIt()
    {
        const string Long =
            "lc_messages is EMPTY, so PostgreSQL takes its message language from the server process's "
            + "environment - which no SQL read can see. That makes this UNKNOWN, not wrong. Set "
            + "lc_messages = 'C' to make it knowable; it is a dynamic parameter and needs a reload, not a "
            + "restart. One honest caveat: a PostgreSQL compiled without NLS support writes English "
            + "whatever this is set to, and SQL cannot see that.";

        var rows = new List<DarlingPgPlanCaptureReadinessReader.PgPlanCaptureReadinessRow>
        {
            Facets()[^1] with { Detail = Long },
        };

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, rows, 25));

        Assert.Equal(Long, doc.RootElement.GetProperty("facets")[0].GetProperty("detail").GetString());
    }

    /// <summary>
    /// #2629's lesson, on a read whose row count makes it look unnecessary: a summary taken over a CAPPED
    /// result describes the page and reads as a fact about the server. <c>limit</c> is the caller's, so the
    /// unsatisfied summary is withheld with a sentence saying why rather than computed over what arrived.
    /// </summary>
    [Fact]
    public void ACappedResult_WithholdsTheUnsatisfiedSummary_RatherThanDescribingThePage()
    {
        var firstTwo = Facets().GetRange(0, 2);

        using var doc = JsonDocument.Parse(DarlingMcpPgPlanTools.BuildReadinessJson("srv", 24, firstTwo, 2));
        var root = doc.RootElement;

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("unsatisfied_facets").ValueKind);
        Assert.Contains("TRUNCATED", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) proof of #3533's mechanism, with the fixture the bug requires: a plan ranked
/// BELOW the page the old path fetched. The old code pulled the top <c>limit * 10</c> shapes by total
/// duration and filtered them in C#, so with <c>limit</c> 2 a plan ranked 21st was unreachable at any
/// window size — and the miss was then reported as "capture is working, this plan was never captured".
/// The predicate now runs in the store, so the same call must return the plan; and a queryid that was
/// genuinely never captured must get the honest whole-window miss rather than the old verdict.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgPlanQueryIdLiveTests
{
    private const string ServerName = "darling-pg-plans-queryid-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>Past 2^53 and negative, the way real pg_stat_statements ids look (#2548).</summary>
    private const long WantedQueryId = -8126435036642491494;

    /// <summary>Never seeded, so the filtered read over it must miss honestly.</summary>
    private const long AbsentQueryId = -7000000000000000001;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AQueryIdRankedBelowTheTopPage_IsFound_AndAGenuineMissIsReportedHonestly()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_pg_plans queryid test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $2 WHERE server_id = $1",
                ServerId, MonitoredEngineKind.Postgres);

            var seen = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-30);

            /* 20 expensive shapes — exactly the limit * 10 page the OLD path fetched for limit 2 — and the
               wanted plan 21st, cheaper than all of them. Ranked by total duration it sits one row below
               everything the old path could ever see. */
            for (var i = 0; i < 20; i++)
            {
                await SeedCaptureAsync(connection, ct, seen, queryId: 9_100_000_000_000_000_001 + i,
                    planHash: $"EXPENSIVE{i:D2}", durationMs: 10_000 - (i * 100),
                    planJson: """{"Plan":{"Node Type":"Hash Join"}}""");
            }

            await SeedCaptureAsync(connection, ct, seen, WantedQueryId,
                planHash: "WANTED", durationMs: 1.5,
                planJson: """{"Plan":{"Node Type":"Index Scan","Relation Name":"orders"}}""");

            /* The premise, demonstrated rather than assumed: the top page at this limit does not contain
               the wanted plan. If this ever fails the fixture has stopped modeling the bug. */
            var topPage = JsonDocument.Parse(
                await DarlingMcpPgPlanTools.GetPgPlans(postgres, ServerName, 24, 2)).RootElement;
            var pageIds = topPage.GetProperty("plans").EnumerateArray()
                .Select(p => p.GetProperty("queryid").GetString())
                .ToArray();
            Assert.Equal(2, pageIds.Length);
            Assert.DoesNotContain(WantedQueryId.ToString(CultureInfo.InvariantCulture), pageIds);

            /* The fix: the same limit, pinned to the queryid, finds the plan the old path could not. */
            var found = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(
                postgres, ServerName, 24, 2, WantedQueryId.ToString(CultureInfo.InvariantCulture))).RootElement;

            var plan = Assert.Single(found.GetProperty("plans").EnumerateArray().ToArray());
            Assert.Equal(WantedQueryId.ToString(CultureInfo.InvariantCulture),
                plan.GetProperty("queryid").GetString());
            Assert.Equal("WANTED", plan.GetProperty("plan_hash").GetString());
            Assert.Equal("Index Scan",
                plan.GetProperty("plan").GetProperty("Plan").GetProperty("Node Type").GetString());

            /* A queryid that was never captured now misses HONESTLY: the whole window was searched, and
               the answer says what a miss can mean instead of declaring the query healthy. */
            var miss = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(
                postgres, ServerName, 24, 2, AbsentQueryId.ToString(CultureInfo.InvariantCulture))).RootElement;

            Assert.Equal("empty", miss.GetProperty("status").GetString());
            var message = miss.GetProperty("message").GetString()!;
            Assert.Contains("not a top-N page", message, StringComparison.Ordinal);
            Assert.Contains("get_pg_plan_capture_readiness", message, StringComparison.Ordinal);
            Assert.DoesNotContain("not the query to look at", message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task SeedCaptureAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        long queryId, string planHash, double durationMs, string planJson) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_plan_capture
    (collection_id, collection_time, server_id, server_name, query_id, plan_hash, duration_ms,
     node_count, top_node_type, plan_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, 3, 'Seeded', $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            queryId, planHash, durationMs, planJson);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_plan_capture WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_plan_capture_readiness WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
