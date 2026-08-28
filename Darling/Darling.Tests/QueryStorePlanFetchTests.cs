/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2312 Finding 2: the activity-driven plan/text fetch — the STORE is the watermark. The #2164/#2210
/// watermark design this file's predecessor pinned (per-database plan-id resume points with a daily
/// refresh expiry) is retired, because Finding 4 measured what it actually did in production: the re-verify
/// cursor that was supposed to replace the expiry never got wired, so catalogs whose full walk needed more
/// than a day expired MID-walk, restarted from plan_id 0, and looped the full catalog fetch forever —
/// 40–110s per cycle, around the clock, to mostly rediscover held content (and 23s to discover "nothing
/// new" on a caught-up catalog).
///
/// <para>The replacement's contract, pinned here: the cycle's collected rows name their plans/texts; the
/// touch-and-probe answers which the store lacks (refreshing map/dim liveness in the same round trip —
/// Finding 3's unwired TouchSql); the fetch selects EXACTLY those by id under the same byte-budget
/// arithmetic; and NULL content lands as a stored marker instead of a per-cycle rediscovery. Live
/// round-trips for the probe SQL are in the gated Postgres suites; these are the shape and pure-function
/// pins.</para>
/// </summary>
public class QueryStorePlanFetchTests
{
    private const string Db = "probedb";
    private static readonly DateTime Now = new(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc);
    private static readonly long[] SomeIds = { 900_001, 900_007, 900_042 };

    /* ---------- the definition still owns no state ---------- */

    [Fact]
    public void TheDefinitionDeclaresNoStateKeys()
    {
        /* #2312 removed the plan/text watermark state entirely, and the definition must not gain state in
           its place: a state-declaring definition is a two-host contract (CollectorStateContractTests pins
           default_trace_events as the only one), and the fetch's only bookkeeping now lives in the store
           itself plus in-memory carry-over. The one remaining query_store state family (qsowm:) is host
           bookkeeping under its own owner, exactly like the backfill pair. */
        Assert.Empty(QueryStoreCollector.Instance.StateKeys);
    }

    /* ---------- the runtime-stats query: no plan narrowing, no plan XML, flag-independent ---------- */

    [Fact]
    public void LiveQuery_NeverCarriesThePlanIdPredicateOrTheRowNumberGate()
    {
        var sql = LiveSql(Context(capturePlanXml: true));

        Assert.DoesNotContain("qsp.plan_id > ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ROW_NUMBER()", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan_text = CASE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void LiveQuery_EmitsThePlaceholder_RegardlessOfCapturePlanXml()
    {
        /* #2210: the runtime query carries no plan XML in either mode, so CapturePlanXml no longer changes
           this query's text at all. Darling (on) and Lite (off) get byte-identical SQL here; the only thing
           CapturePlanXml still gates is the separate by-ids fetch. */
        var on = LiveSql(Context(capturePlanXml: true));
        var off = LiveSql(Context(capturePlanXml: false));

        Assert.Contains("query_plan_text = CONVERT(nvarchar(1), NULL),", on, StringComparison.Ordinal);
        Assert.True(string.Equals(on, off, StringComparison.Ordinal), "CapturePlanXml must not change this query's text");
    }

    /// <summary>#2312: the read loop stages NO state — the write-back that advanced planwm: from
    /// inline-shipped plan XML retired with the watermark. The failure mode of it creeping back is a state
    /// row nothing reads and a prune set that no longer owns its prefix.</summary>
    [Fact]
    public async Task ReadItemAsync_StagesNoPendingState()
    {
        var context = Context(capturePlanXml: true);
        await Read(context, Plan(10, xml: true), Plan(30, xml: true));

        Assert.Empty(context.PendingState);
    }

    /* ---------- the by-ids plan fetch ---------- */

    /// <summary>
    /// The budget predicate admits a plan on the running total BEFORE it — <c>running - own &lt; budget</c>
    /// — so one oversized plan ships alone instead of stalling. Under store-as-watermark the naive
    /// <c>&lt;=</c> form is the same stall it always was, reached through the probe: the plan never lands,
    /// stays missing, and rides every cycle's fetch list forever.
    ///
    /// <para>A SHAPE pin: it asserts the predicate the SQL carries, not what SQL Server does with it. The
    /// CONVERT sits inside the candidate CTE and the running total measures the converted text, so a
    /// shipped plan is decompressed once (measured 133ms against 274ms for the join-back form on a
    /// 73,163-plan catalog).</para>
    /// </summary>
    [Fact]
    public void PlanFetchByIds_SelectsExactlyTheNamedIds_UnderTheBudgetArithmetic()
    {
        var sql = QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(
            Db, Context(capturePlanXml: true), SomeIds, budgetBytes: 12L * 1024 * 1024).Text;

        Assert.Contains("WHERE qsp.plan_id IN (900001, 900007, 900042)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qsp.plan_id > ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT TOP", sql, StringComparison.Ordinal);

        Assert.Contains("b.running_bytes - b.plan_bytes < 12582912", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("b.running_bytes <= ", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(DATALENGTH(p.query_plan_text), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("ROWS UNBOUNDED PRECEDING", sql, StringComparison.Ordinal);
        Assert.Contains("query_plan_text = CONVERT(nvarchar(max), qsp.query_plan)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN sys.query_store_plan", sql, StringComparison.Ordinal);

        /* #2675: the plan is decompressed EXACTLY ONCE. query_plan is materialised to #plan_fetch, and the
           budget/emit run over that column - so CONVERT(nvarchar(max), qsp.query_plan), the decompression,
           appears once, not the three references the prior inline CTE re-ran (~4.5x CPU on the rig). A revert
           to the inline form trips this. */
        Assert.Contains("INTO #plan_fetch", sql, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(sql, "CONVERT(nvarchar(max), qsp.query_plan)"));
        Assert.Contains("SET NOCOUNT ON;", sql, StringComparison.Ordinal);

        /* The hash rides along without decompressing anything, rendered the way the runtime payload
           renders it — TouchAndProbeSql compares the two, so the formats must agree byte-for-byte. */
        Assert.Contains("query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1)", sql, StringComparison.Ordinal);

        Assert.Contains("ORDER BY b.plan_id", sql, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanFetchByIds_RunsInTheDatabasesOwnContext_WithBracketDoubling()
    {
        var sql = QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(
            "we]ird", Context(capturePlanXml: true), SomeIds, 1024).Text;

        Assert.Contains("EXECUTE [we]]ird].sys.sp_executesql", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanFetchByIds_GuardsItsPreconditions()
    {
        var context = Context(capturePlanXml: true);

        /* Empty means "nothing missing" and the caller must not have called — a silent no-op query here
           would hide the missing skip, and IN () is a syntax error anyway. */
        Assert.Throws<ArgumentException>(() =>
            QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(Db, context, Array.Empty<long>(), 1024));

        /* A non-positive budget ships nothing forever — the stall by another door. */
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(Db, context, SomeIds, 0));

        /* Plan capture off means no plan fetch exists at all. */
        Assert.Throws<InvalidOperationException>(() =>
            QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(Db, Context(capturePlanXml: false), SomeIds, 1024));
    }

    /* ---------- the by-ids text fetch ---------- */

    [Fact]
    public void TextFetchByIds_SelectsExactlyTheNamedIds_WithTheResetDetectorHash()
    {
        var context = Context(capturePlanXml: true, fetchTextSeparately: true);

        var sql = QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(
            Db, context, SomeIds, budgetBytes: 12L * 1024 * 1024).Text;

        Assert.Contains("WHERE qsq.query_id IN (900001, 900007, 900042)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qsq.query_id > ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT TOP", sql, StringComparison.Ordinal);

        Assert.Contains("JOIN sys.query_store_query_text AS qst", sql, StringComparison.Ordinal);
        Assert.Contains("b.running_bytes - b.text_bytes < 12582912", sql, StringComparison.Ordinal);
        Assert.Contains("ROWS UNBOUNDED PRECEDING", sql, StringComparison.Ordinal);

        /* #2675: text is read once - materialised to #text_fetch, then budgeted/emitted from there, the same
           single-read shape as the plan fetch. */
        Assert.Contains("INTO #text_fetch", sql, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(sql, "query_sql_text = qst.query_sql_text"));
        Assert.Contains("SET NOCOUNT ON;", sql, StringComparison.Ordinal);

        /* query_id is only unique until a Query Store reset renumbers it; the stored hash is how the probe
           sees that id 5 now names a DIFFERENT statement. Same rendering as the runtime payload. */
        Assert.Contains("query_hash = CONVERT(varchar(64), qsq.query_hash, 1)", sql, StringComparison.Ordinal);

        Assert.Contains("ORDER BY b.query_id", sql, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TextFetchByIds_GuardsItsPreconditions()
    {
        var context = Context(capturePlanXml: false, fetchTextSeparately: true);

        Assert.Throws<ArgumentException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(Db, context, Array.Empty<long>(), 1024));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(Db, context, SomeIds, 0));

        var inlineContext = Context(capturePlanXml: false);
        Assert.Throws<InvalidOperationException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(Db, inlineContext, SomeIds, 1024));
    }

    /* ---------- the touch-and-probe: liveness + missing set + rewrite/reset detection, one trip ---------- */

    [Fact]
    public void PlanTouchAndProbe_TouchesBothTables_ProbesResolvedAndStale()
    {
        var sql = QueryStorePlanMap.TouchAndProbeSql;

        /* The liveness half (Finding 3): map and dim last_seen stamped by the same pass, hourly-guarded,
           and the dim update skips the NULL-digest content-less markers — there is no dim row to touch. */
        Assert.Contains("UPDATE collect.query_store_plan_map", sql, StringComparison.Ordinal);
        Assert.Contains("UPDATE collect.query_plan_dim", sql, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(sql, "interval '1 hour'"));
        Assert.Contains("FROM map_touch WHERE digest IS NOT NULL", sql, StringComparison.Ordinal);

        /* Hash adoption: legacy rows (stored hash NULL) take the batch's live hash on first touch — never
           the other way around, so a stored baseline is never replaced by absence. */
        Assert.Contains("plan_hash = COALESCE(m.plan_hash, t.live_hash)", sql, StringComparison.Ordinal);

        /* The probe half (Finding 2): resolved is row EXISTENCE — a NULL-digest marker still resolves, or
           unpersistable plans would ride every cycle's fetch list — and hash_stale fires only when BOTH
           hashes exist and differ, so legacy-NULL rows and hash-less batches can never mass-refetch. */
        Assert.Contains("(m.plan_id IS NOT NULL) AS resolved", sql, StringComparison.Ordinal);
        Assert.Contains("m.plan_hash IS NOT NULL AND batch.plan_hash IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("m.plan_hash <> batch.plan_hash", sql, StringComparison.Ordinal);

        /* Five parameters: ids + hashes + the stamp. */
        Assert.Contains("$5::timestamp", sql, StringComparison.Ordinal);
        Assert.Contains("unnest($1::integer[], $2::text[], $3::bigint[], $4::text[])", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TextTouchAndProbe_MirrorsThePlanShape_SingleTable()
    {
        var sql = QueryStoreTextStore.TouchAndProbeSql;

        Assert.Contains("UPDATE collect.query_store_text", sql, StringComparison.Ordinal);
        Assert.Contains("interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = COALESCE(t.query_hash, x.live_hash)", sql, StringComparison.Ordinal);
        Assert.Contains("(t.query_id IS NOT NULL) AS resolved", sql, StringComparison.Ordinal);
        Assert.Contains("t.query_hash IS NOT NULL AND batch.query_hash IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("t.query_hash <> batch.query_hash", sql, StringComparison.Ordinal);
        Assert.Contains("$5::timestamp", sql, StringComparison.Ordinal);
    }

    /* ---------- the upserts never replace knowledge with absence ---------- */

    [Fact]
    public void PlanMapUpsert_CoalescesDigestAndHash_TowardKnowledge()
    {
        var sql = QueryStorePlanMap.UpsertSql;

        /* A NULL-XML refetch of a plan whose content the store already holds keeps the content; a fetch
           that carried no hash keeps the stored baseline. Real values still advance both — that is how an
           in-place rewrite's corrected content gets pointed at. */
        Assert.Contains("digest = COALESCE(EXCLUDED.digest, query_store_plan_map.digest)", sql, StringComparison.Ordinal);
        Assert.Contains("plan_hash = COALESCE(EXCLUDED.plan_hash, query_store_plan_map.plan_hash)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE EXCLUDED.last_seen >= query_store_plan_map.last_seen", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, database_name, plan_id", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TextUpsert_CarriesTheHash_AndOverwritesTextOnPurpose()
    {
        var sql = QueryStoreTextStore.UpsertSql;

        /* The TEXT is overwritten (a post-reset id names a different statement and the corrected text must
           land); the HASH coalesces toward knowledge like the plan side. */
        Assert.Contains("query_sql_text = EXCLUDED.query_sql_text", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = COALESCE(EXCLUDED.query_hash, query_store_text.query_hash)", sql, StringComparison.Ordinal);
        Assert.Contains("unnest($1::integer[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::timestamp[])", sql, StringComparison.Ordinal);
    }

    /* ---------- sizing: unchanged arithmetic, still load-bearing (it caps decompression) ---------- */

    /// <summary>
    /// The candidate cap sits just past what the budget can actually ship, at every plan size the fleet
    /// ACTUALLY exhibits — per-quartile averages of 162 / 80 / 39 / 15 KB measured across 2,166 budget-cut
    /// passes. The point of the pin is that none of these clamp: if a real fleet plan size hit a bound, the
    /// bound would be doing the sizing instead of the measurement.
    /// </summary>
    [Theory]
    [InlineData(162, 114)]
    [InlineData(80, 231)]
    [InlineData(39, 473)]
    [InlineData(15, 1229)]
    public void CandidatePlanCount_SitsJustPastTheBudget_AtEveryMeasuredFleetPlanSize(int avgKb, int expected)
    {
        var k = QueryStorePlanXmlState.CandidatePlanCount(avgKb * 1024L, 12L * 1024 * 1024, out var clamped);

        Assert.Equal(expected, k);
        Assert.False(clamped, "a plan size the fleet actually shows must not hit a bound");

        var actuallyFit = (12L * 1024 * 1024) / (avgKb * 1024L);
        Assert.InRange(k / (double)actuallyFit, 1.4, 1.6);
    }

    /// <summary>
    /// First contact assumes LARGE plans on purpose. The estimate is a divisor, so over-stating plan size
    /// yields a small cap — and small is the safe direction: it only spreads the catch-up out, where too
    /// large decompresses a backlog to discover what fits, which is the trap the cap exists to prevent.
    /// </summary>
    [Fact]
    public void CandidatePlanCount_WithNoPreviousPass_IsConservativelySmall()
    {
        var k = QueryStorePlanXmlState.CandidatePlanCount(null, 12L * 1024 * 1024, out var clamped);

        /* ceil(12MB / 160KB * 1.5) = 116 — the arithmetic, not the issue text's rounded "~118". */
        Assert.Equal(116, k);
        Assert.False(clamped);
    }

    [Theory]
    [InlineData(10L * 1024 * 1024, 12L * 1024 * 1024, 32)]
    [InlineData(1, 12L * 1024 * 1024, 2048)]
    public void CandidatePlanCount_ClampsAndSaysSo(long avgBytes, long budget, int expected)
    {
        var k = QueryStorePlanXmlState.CandidatePlanCount(avgBytes, budget, out var clamped);

        Assert.Equal(expected, k);
        Assert.True(clamped);
    }

    [Fact]
    public void CandidatePlanCount_LandingNaturallyOnABound_IsNotReportedAsClamped()
    {
        var exactlyTheFloor = (long)(QueryStorePlanXmlState.MinCandidatePlans / QueryStorePlanXmlState.CandidatePlanMargin);
        var k = QueryStorePlanXmlState.CandidatePlanCount(1, exactlyTheFloor, out var clamped);

        Assert.Equal(QueryStorePlanXmlState.MinCandidatePlans, k);
        Assert.False(clamped, "the measurement produced this value; no bound changed it");
    }

    /// <summary>
    /// While catch-up is in progress the observed average is floored at the seed, because the sample is
    /// biased then and measurably so: on one production catalog the plans the fetch shipped averaged 15 KB
    /// while the newest 300 in the same catalog averaged 46 KB. Trusting the low figure inflates the cap
    /// threefold and decompresses that much more than the budget can ship.
    /// </summary>
    [Fact]
    public void CandidatePlanCount_DuringCatchUp_FloorsTheEstimateAtTheSeed()
    {
        const long budget = 12L * 1024 * 1024;
        var biased = 15 * 1024L;

        var duringCatchUp = QueryStorePlanXmlState.CandidatePlanCount(biased, budget, catchUpInProgress: true, out _);
        var converged = QueryStorePlanXmlState.CandidatePlanCount(biased, budget, catchUpInProgress: false, out _);
        var seeded = QueryStorePlanXmlState.CandidatePlanCount(null, budget, out _);

        Assert.Equal(seeded, duringCatchUp);
        Assert.True(converged > duringCatchUp, "the un-floored estimate must still be trusted once converged");

        var large = 200 * 1024L;
        Assert.Equal(
            QueryStorePlanXmlState.CandidatePlanCount(large, budget, catchUpInProgress: false, out _),
            QueryStorePlanXmlState.CandidatePlanCount(large, budget, catchUpInProgress: true, out _));
    }

    [Fact]
    public void CandidatePlanCount_WithNonPositiveBudget_FloorsAndReportsClamped()
    {
        var k = QueryStorePlanXmlState.CandidatePlanCount(160 * 1024L, 0, out var clamped);

        Assert.Equal(QueryStorePlanXmlState.MinCandidatePlans, k);
        Assert.True(clamped);
    }

    /// <summary>
    /// The estimator reproduces the measured fleet numbers from the same two inputs a pass already has.
    /// </summary>
    [Theory]
    [InlineData(1.7, 11, 158)]
    [InlineData(1.7, 22, 79)]
    [InlineData(1.7, 44, 39)]
    [InlineData(1.7, 116, 15)]
    public void ObservedAvgPlanBytes_ReproducesTheMeasuredQuartiles(double shippedMb, int plans, int expectedKb)
    {
        var avg = QueryStorePlanXmlState.ObservedAvgPlanBytes((long)(shippedMb * 1024 * 1024), plans);

        Assert.NotNull(avg);
        Assert.Equal(expectedKb, (int)(avg!.Value / 1024));
    }

    [Fact]
    public void ObservedAvgPlanBytes_OnAQuietPass_IsNull()
    {
        Assert.Null(QueryStorePlanXmlState.ObservedAvgPlanBytes(0, 0));
        Assert.Null(QueryStorePlanXmlState.ObservedAvgPlanBytes(0, 5));
        Assert.Null(QueryStorePlanXmlState.ObservedAvgPlanBytes(1024, 0));
    }

    /* ---------- helpers ---------- */

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    private static string LiveSql(CollectorContext context) =>
        QueryStoreCollector.Instance.BuildPerItemQuery(Db, context).Text;

    private static CollectorContext Context(bool capturePlanXml, bool fetchTextSeparately = false)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "probe",
            CollectionTime = Now,
            Deltas = new CollectorDeltaCalculator(),
            CapturePlanXml = capturePlanXml,
            FetchQueryTextSeparately = fetchTextSeparately,
            State = new Dictionary<string, string>(),
        };
        context.CurrentDatabaseName = Db;
        return context;
    }

    private static (long PlanId, bool Xml) Plan(long planId, bool xml) => (planId, xml);

    private static async Task Read(CollectorContext context, params (long PlanId, bool Xml)[] plans)
    {
        using var reader = MakeReader(plans);
        var rows = new List<QueryStoreCollector.Row>();
        await QueryStoreCollector.Instance.ReadItemAsync(Db, reader, rows, context, CancellationToken.None);
    }

    /// <summary>
    /// A real <c>DbDataReader</c> over the collector's OWN payload shape, generated from
    /// <c>PayloadColumns</c> minus <c>database_name</c> (which the on-prem path takes from the enumerated
    /// item, not the reader). Generated rather than hand-listed so a column added to the collector cannot
    /// silently shift the ordinals the read loop depends on.
    /// </summary>
    private static DataTableReader MakeReader((long PlanId, bool Xml)[] plans)
    {
        var table = new DataTable("payload");
        var columns = QueryStoreCollector.Instance.PayloadColumns.Skip(1).ToList();

        foreach (var column in columns)
        {
            table.Columns.Add(column.Name, ClrType(column.Name, column.Type));
        }

        for (var i = 0; i < plans.Length; i++)
        {
            var row = table.NewRow();

            foreach (var column in columns)
            {
                row[column.Name] = column.Type switch
                {
                    CollectorColumnType.BigInt => 0L,
                    CollectorColumnType.Integer => 160,
                    CollectorColumnType.Boolean => false,
                    /* Distinct per row, so a budget cut's boundary tie group ends on the very next row. */
                    CollectorColumnType.Timestamp when ClrType(column.Name, column.Type) == typeof(DateTime)
                        => Now.AddMinutes(i),
                    CollectorColumnType.Timestamp => new DateTimeOffset(Now.AddMinutes(i), TimeSpan.Zero),
                    _ => "x",
                };
            }

            row["query_id"] = plans[i].PlanId * 10;
            row["plan_id"] = plans[i].PlanId;
            row["execution_count"] = 1L;
            /* Must not contain the self-query marker, or the read loop skips the row entirely. */
            row["query_text"] = "SELECT 1 FROM dbo.Whatever";
            row["query_plan_text"] = plans[i].Xml ? new string('p', 4096) : (object)DBNull.Value;

            table.Rows.Add(row);
        }

        var dataSet = new DataSet();
        dataSet.Tables.Add(table);
        return dataSet.CreateDataReader();
    }

    /// <summary>
    /// The provider types the read loop actually expects, which are NOT uniform across the timestamp
    /// columns: <c>first_execution_time</c> / <c>last_execution_time</c> come out of Query Store as
    /// <c>datetimeoffset</c> and are read as <c>DateTimeOffset</c>, while <c>interval_start_time_utc</c> is
    /// computed <c>datetime2</c> and read with <c>GetDateTime</c>. A harness that types all three the same
    /// way throws <c>InvalidCastException</c> inside the loop — which is how this was found.
    /// </summary>
    private static Type ClrType(string name, CollectorColumnType type) => type switch
    {
        CollectorColumnType.BigInt => typeof(long),
        CollectorColumnType.Integer => typeof(int),
        CollectorColumnType.Boolean => typeof(bool),
        CollectorColumnType.Timestamp =>
            name.Equals("interval_start_time_utc", StringComparison.Ordinal) ? typeof(DateTime) : typeof(DateTimeOffset),
        _ => typeof(string),
    };
}
