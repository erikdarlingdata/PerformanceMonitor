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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2164: the plan-XML watermark. 97% of the plan XML shipped in a three-hour fleet window was for plans the
/// store already held, and since drain is 94-97% of a pass and costs per-row LOB bytes, not fetching is worth
/// far more than fetching less.
///
/// <para>Driven entirely through the collector's PUBLIC surface — <c>BuildPerItemQuery</c>,
/// <c>BuildBackfillPerItemQuery</c>, <c>ReadItemAsync</c> — rather than reaching for the internal helpers, so
/// no production visibility is widened for the tests' benefit. It also makes the state format an explicit
/// pin: the stored string is written out literally here instead of being produced by the same formatter under
/// test, which would have agreed with itself no matter what it emitted.</para>
/// </summary>
public class QueryStorePlanWatermarkTests
{
    private const string Db = "probedb";
    private static readonly DateTime Now = new(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc);

    /* ---------- #2210: the SQL-side narrowing is gone; QueryStorePlanXmlState.Resolve is what remains ----------
       The ROW_NUMBER-gated CASE and its in-stream `AND qsp.plan_id > ` predicate are DELETED, not reworked —
       BuildPlanFetchQuery is the only thing that reads plan XML now, and its `watermark` parameter is resolved
       by the host calling Resolve directly rather than derived inline in this query. These tests used to drive
       that predicate through the live SQL; they now drive Resolve directly, which is exactly what the host
       still calls to get BuildPlanFetchQuery's watermark argument, so the state-format/malformed/expired/
       future-stamp/per-database coverage stays live even though the SQL it used to narrow does not exist. */

    [Fact]
    public void Resolve_Fresh_ReturnsTheStoredPlanId()
    {
        var resolved = QueryStorePlanXmlState.Resolve(Stored(900_000, Now), Db, Now);

        Assert.Equal(900_000, resolved);
    }

    [Fact]
    public void Resolve_Absent_ReturnsZero()
    {
        /* Absent is what a first run, a restarted host and a broken store all look like, and all three must
           resolve to "fetch everything" rather than skip. */
        var resolved = QueryStorePlanXmlState.Resolve(new Dictionary<string, string>(), Db, Now);

        Assert.Equal(0, resolved);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("900000")]                  /* no stamp */
    [InlineData("900000:")]                 /* empty stamp */
    [InlineData("notanumber:1786449600")]
    [InlineData("900000:notanumber")]
    [InlineData("900000:1786449600:extra")]
    [InlineData("0:1786449600")]            /* plan_id 0 is not a plan */
    [InlineData("-5:1786449600")]
    public void Resolve_Malformed_ReturnsZero(string raw)
    {
        /* Anything unparseable degrades to a full fetch. Trusting a partially parsed value would suppress
           plan XML based on a number nobody wrote. */
        var state = new Dictionary<string, string> { [QueryStorePlanXmlState.WatermarkKeyPrefix + Db] = raw };

        Assert.Equal(0, QueryStorePlanXmlState.Resolve(state, Db, Now));
    }

    [Fact]
    public void Resolve_Expired_ReturnsZero_ButStillFreshReturnsTheStoredPlanId()
    {
        /* Bounded staleness is why the stamp is stored beside the id. Query Store can rewrite a plan's XML in
           place (memory grant feedback and friends) without issuing a new plan_id, and a permanent watermark
           would never look again. It also bounds the documented dormant-plan gap. */
        var stampedLongAgo = Now - QueryStorePlanXmlState.RefreshAfter - TimeSpan.FromMinutes(1);

        var expired = QueryStorePlanXmlState.Resolve(Stored(900_000, stampedLongAgo), Db, Now);
        var stillFresh = QueryStorePlanXmlState.Resolve(Stored(900_000, Now - TimeSpan.FromMinutes(1)), Db, Now);

        Assert.Equal(0, expired);
        Assert.Equal(900_000, stillFresh);
    }

    [Fact]
    public void Resolve_StampedInTheFuture_ReturnsZero()
    {
        /* A clock that moved backwards would otherwise pin the watermark for as long as the skew lasts. */
        var resolved = QueryStorePlanXmlState.Resolve(Stored(900_000, Now.AddDays(3)), Db, Now);

        Assert.Equal(0, resolved);
    }

    [Fact]
    public void Resolve_IsKeyedPerDatabase()
    {
        /* plan_id is monotonic WITHIN a database and means nothing across them, so one database's watermark
           must never be read for another. */
        var state = new Dictionary<string, string>
        {
            [QueryStorePlanXmlState.WatermarkKeyPrefix + "alpha"] = "900000:" + Unix(Now),
        };

        Assert.Equal(900_000, QueryStorePlanXmlState.Resolve(state, "alpha", Now));
        Assert.Equal(0, QueryStorePlanXmlState.Resolve(state, "beta", Now));
    }

    [Fact]
    public void TheDefinitionDeclaresNoStateKeys_TheHostOwnsThisState()
    {
        /* The watermark keys are one per DATABASE and only known at runtime, so the definition could not
           declare them even if it wanted to. More importantly it MUST NOT: a state-declaring definition is a
           two-host contract (CollectorStateContractTests pins default_trace_events as the only one), while
           this is host bookkeeping. The QueryStoreBackfillState seam — a separate state owner name — is what
           lets the host persist per-database state without the definition claiming any.

           The failure mode if this ever flips is silent, which is why it is pinned from both ends: a row
           written under the DEFINITION's name is never read back, so the watermark would resolve absent
           forever and collection would quietly keep paying full price. */
        Assert.Empty(QueryStoreCollector.Instance.StateKeys);
        Assert.NotEqual(QueryStorePlanXmlState.StateCollectorName, QueryStoreCollector.Instance.Name);
        Assert.Equal("query_store_plan_xml", QueryStorePlanXmlState.StateCollectorName);
        Assert.Equal("planwm:", QueryStorePlanXmlState.WatermarkKeyPrefix);
    }

    /* ---------- #2210: the runtime-stats query itself, now flag-independent ---------- */

    [Fact]
    public void LiveQuery_NeverCarriesThePlanIdPredicateOrTheRowNumberGate_RegardlessOfWatermarkState()
    {
        /* The predicate and the CASE it used to narrow are gone from the query entirely, not just from the
           conservative path — there is no live watermark state under which either can reappear. */
        var withState = LiveSql(Context(capturePlanXml: true, state: Stored(900_000, Now)));
        var withoutState = LiveSql(Context(capturePlanXml: true, state: new Dictionary<string, string>()));

        Assert.DoesNotContain("qsp.plan_id > ", withState, StringComparison.Ordinal);
        Assert.DoesNotContain("qsp.plan_id > ", withoutState, StringComparison.Ordinal);
        Assert.DoesNotContain("ROW_NUMBER()", withState, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan_text = CASE", withState, StringComparison.Ordinal);
    }

    [Fact]
    public void LiveQuery_EmitsThePlaceholder_RegardlessOfCapturePlanXml()
    {
        /* #2210: both branches of the old ternary now emit the same nvarchar(1) NULL placeholder — the
           runtime query carries no plan XML in either mode, so CapturePlanXml no longer changes this query's
           text at all. Darling (on) and Lite (off) get byte-identical SQL here; the only thing CapturePlanXml
           still gates is the separate BuildPlanFetchQuery fetch. */
        var on = LiveSql(Context(capturePlanXml: true, state: new Dictionary<string, string>()));
        var off = LiveSql(Context(capturePlanXml: false, state: new Dictionary<string, string>()));

        Assert.Contains("query_plan_text = CONVERT(nvarchar(1), NULL),", on, StringComparison.Ordinal);
        Assert.Contains("query_plan_text = CONVERT(nvarchar(1), NULL),", off, StringComparison.Ordinal);
        Assert.True(string.Equals(on, off, StringComparison.Ordinal), "CapturePlanXml must no longer change this query's text");
    }

    /* ---------- write-back, driven through the real read loop ---------- */

    [Fact]
    public async Task WriteBack_NormalPass_AdvancesToTheHighestStoredPlanId()
    {
        var context = Context(capturePlanXml: true, state: new Dictionary<string, string>());
        await Read(context, Plan(10, xml: true), Plan(20, xml: true), Plan(30, xml: true));

        Assert.Equal("30:" + Unix(Now), Written(context));
    }

    [Fact]
    public async Task WriteBack_CountsOnlyPlansWhoseXmlActuallyShipped()
    {
        /* The ROW_NUMBER gate NULLs the XML on all but one interval per plan, so "seen" and "stored" differ
           on every real pass. Advancing on a plan whose XML was NULL would suppress that plan's XML from then
           on without ever having sent it. */
        var context = Context(capturePlanXml: true, state: new Dictionary<string, string>());
        await Read(context, Plan(10, xml: true), Plan(40, xml: false));

        Assert.Equal("10:" + Unix(Now), Written(context));
    }

    /* WriteBack_BudgetCutPass_DoesNotAdvanceAtAll (#2164) deleted with the shape it pinned: its own comment
       said it recorded known-broken behaviour under the last_execution_time-ordered fetch — the watermark
       could never advance on a budget cut because a cut left an arbitrary SUBSET of plan_ids. #2210's
       plan_id-ordered fetch removes that premise; the replacement is
       AdvanceWatermark_OnABudgetCutPass_StillAdvances below, which pins the new behaviour directly against
       QueryStorePlanXmlState.AdvanceWatermark. */

    [Fact]
    public async Task WriteBack_QuietWindow_NeverMovesTheWatermarkBackward()
    {
        /* This is the case that killed the first design. A window whose newest-EXECUTING plan is older than
           the newest-COMPILED one is an ordinary quiet window, and on a steady workload it is most windows.
           The first cut read it as a Query Store reset and dropped the watermark, which would have refetched
           the whole catalog on nearly every pass — the exact cost being removed. */
        var context = Context(capturePlanXml: true, state: Stored(900_000, Now));
        await Read(context, Plan(800_000, xml: true), Plan(850_000, xml: true));

        Assert.Null(Written(context));
    }

    [Fact]
    public async Task WriteBack_Advance_CarriesTheOriginalStampForward_SoTheHorizonStillFires()
    {
        /* The stamp dates the last FULL fetch. If an advance re-stamped it to now, then any database that
           keeps compiling new plans would push its refresh horizon out forever — and those are the busy
           databases where a stale plan matters most. The bounded refresh would silently never happen. */
        var fetchedAt = Now - TimeSpan.FromHours(20);
        var context = Context(capturePlanXml: true, state: Stored(900_000, fetchedAt));

        await Read(context, Plan(950_000, xml: true));

        Assert.Equal("950000:" + Unix(fetchedAt), Written(context));
    }

    [Fact]
    public async Task WriteBack_AfterExpiry_StampsTheFullFetchAtNow()
    {
        /* The other half of the same rule: an expired watermark means THIS pass refetched everything, so it
           is the one case that legitimately re-dates the horizon. Without this the stamp would never move and
           every pass after the first expiry would be a full fetch. */
        var longAgo = Now - QueryStorePlanXmlState.RefreshAfter - TimeSpan.FromHours(1);
        var context = Context(capturePlanXml: true, state: Stored(900_000, longAgo));

        await Read(context, Plan(950_000, xml: true));

        Assert.Equal("950000:" + Unix(Now), Written(context));
    }

    [Fact]
    public async Task WriteBack_PlanCaptureOff_WritesNothing()
    {
        /* Lite reads the same rows with no XML. It must not leave a watermark behind that a plan-capturing
           host would later honor, having never shipped a single plan. */
        var context = Context(capturePlanXml: false, state: new Dictionary<string, string>());
        await Read(context, Plan(10, xml: true), Plan(30, xml: true));

        Assert.Null(Written(context));
    }

    /* ---------- helpers ---------- */

    private static Dictionary<string, string> Stored(long planId, DateTime stampedAt) =>
        new()
        {
            [QueryStorePlanXmlState.WatermarkKeyPrefix + Db] =
                planId.ToString(CultureInfo.InvariantCulture) + ":" + Unix(stampedAt),
        };

    private static string Unix(DateTime utc) =>
        new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeSeconds()
            .ToString(CultureInfo.InvariantCulture);

    private static string? Written(CollectorContext context) =>
        context.PendingState.TryGetValue(QueryStorePlanXmlState.WatermarkKeyPrefix + Db, out var value)
            ? value
            : null;

    private static string LiveSql(CollectorContext context) =>
        QueryStoreCollector.Instance.BuildPerItemQuery(Db, context).Text;

    private static CollectorContext Context(
        bool capturePlanXml,
        IReadOnlyDictionary<string, string> state,
        int? budgetOverride = null)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "probe",
            CollectionTime = Now,
            Deltas = new CollectorDeltaCalculator(),
            CapturePlanXml = capturePlanXml,
            State = state,
            TextByteBudgetOverride = budgetOverride,
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

    /* ---- #2210: the plan_id-ordered fetch policy. Pure functions, pinned like QueryStoreBackfillState
       .AdaptiveSpan, because the candidate window and the watermark advance are the two places this
       optimization can silently do nothing (attempt one) or silently lose plans (the ordering precondition). */

    /// <summary>
    /// The candidate window sits just past what the budget can actually ship, at every plan size the fleet
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

        /* Just past, not far past: the window is the coarse bound and the running byte total is the exact one,
           and every plan IN the window is decompressed to compute that total. */
        var actuallyFit = (12L * 1024 * 1024) / (avgKb * 1024L);
        Assert.InRange(k / (double)actuallyFit, 1.4, 1.6);
    }

    /// <summary>
    /// First contact assumes LARGE plans on purpose. The estimate is a divisor, so over-stating plan size
    /// yields a small window — and small is the safe direction: it only slows the watermark down, where too
    /// large decompresses a catalog to discover what fits, which is the trap the window exists to prevent.
    /// </summary>
    [Fact]
    public void CandidatePlanCount_WithNoPreviousPass_IsConservativelySmall()
    {
        var seed = QueryStorePlanXmlState.CandidatePlanCount(null, 12L * 1024 * 1024, out var clamped);
        var atLargestMeasured = QueryStorePlanXmlState.CandidatePlanCount(162 * 1024L, 12L * 1024 * 1024, out _);

        Assert.False(clamped);
        Assert.InRange(seed, atLargestMeasured - 10, atLargestMeasured + 10);
    }

    /// <summary>Bounds hold, and every clamp REPORTS itself — a window silently pinned at its ceiling reads
    /// exactly like one that fit, which is how a cap becomes invisible.</summary>
    [Theory]
    [InlineData(1, 12L * 1024 * 1024, QueryStorePlanXmlState.MaxCandidatePlans)]
    [InlineData(64 * 1024, 12L * 1024 * 1024, QueryStorePlanXmlState.MinCandidatePlans)]
    public void CandidatePlanCount_ClampsAndSaysSo(long avgKb, long budget, int expected)
    {
        var k = QueryStorePlanXmlState.CandidatePlanCount(avgKb * 1024L, budget, out var clamped);

        Assert.Equal(expected, k);
        Assert.True(clamped, "a clamped window must be reportable so the caller can log it");
    }

    /// <summary>
    /// `clamped` means a bound CHANGED the answer, not that the answer equals one. A window whose measured size
    /// lands naturally on a bound was sized by the measurement and needs no log line; reporting it as clamped is
    /// a false positive, and a caller that logs on it trains its reader to ignore the message.
    /// </summary>
    [Fact]
    public void CandidatePlanCount_LandingNaturallyOnABound_IsNotReportedAsClamped()
    {
        /* Budget chosen so budget/avg*margin is exactly MinCandidatePlans: 32 / 1.5 = 21.33 plans of 1 byte. */
        var exactlyTheFloor = (long)(QueryStorePlanXmlState.MinCandidatePlans / QueryStorePlanXmlState.CandidatePlanMargin);
        var k = QueryStorePlanXmlState.CandidatePlanCount(1, exactlyTheFloor, out var clamped);

        Assert.Equal(QueryStorePlanXmlState.MinCandidatePlans, k);
        Assert.False(clamped, "the measurement produced this value; no bound changed it");
    }

    /// <summary>
    /// While catch-up is in progress the observed average is floored at the seed, because the sample is biased
    /// then and measurably so: on one production catalog the plans the fetch shipped averaged 15 KB while the
    /// newest 300 in the same catalog averaged 46 KB. Trusting the low figure inflates K threefold and
    /// decompresses that much more than the budget can ship. After convergence the observed value is trusted.
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

        /* A large observed average is NOT raised by the floor — over-estimating plan size is the safe direction
           and the floor only ever makes the window smaller. */
        var large = 200 * 1024L;
        Assert.Equal(
            QueryStorePlanXmlState.CandidatePlanCount(large, budget, catchUpInProgress: false, out _),
            QueryStorePlanXmlState.CandidatePlanCount(large, budget, catchUpInProgress: true, out _));
    }

    /// <summary>
    /// #2210, ruling item 4: a FULL cursor sweep over a healthy catalog leaves the watermark byte-identical.
    /// Only the reset arm may ever zero it.
    ///
    /// <para>Simulated through the real functions rather than mocked, because the property is about them: a
    /// healthy catalog means every slice the cursor walks finds its stored hash matching the live one, so
    /// nothing is re-fetched and the pass lands no plan ids. The sweep is then <c>ceil(range / slice)</c> calls
    /// to <see cref="QueryStorePlanXmlState.AdvanceWatermark"/> with nothing landed, and the persisted string
    /// has to come out the same at the end — including its stamp, since re-stamping on a no-op sweep would push
    /// the refresh period out forever on any database the cursor keeps visiting.</para>
    /// </summary>
    [Fact]
    public void AFullCursorSweepOverAHealthyCatalog_LeavesTheWatermarkByteIdentical()
    {
        const long watermark = 77_176;
        var stamp = Now;
        var before = QueryStorePlanXmlState.Format(watermark, stamp);

        var slice = QueryStorePlanMap.CursorSliceWidth(watermark, QueryStorePlanXmlState.RefreshAfter, TimeSpan.FromMinutes(5));
        Assert.True(slice > 0);

        var standing = watermark;
        var passes = 0;
        for (var floor = 0L; floor < watermark; floor += slice)
        {
            /* Healthy: hashes match across the slice, so nothing is re-fetched and nothing lands. */
            var advance = QueryStorePlanXmlState.AdvanceWatermark(standing, Array.Empty<long>());

            Assert.True(advance.ArrivedInPlanIdOrder);
            Assert.Equal(standing, advance.Watermark);
            standing = advance.Watermark;
            passes++;
        }

        Assert.Equal(watermark, standing);
        Assert.Equal(before, QueryStorePlanXmlState.Format(standing, stamp));

        /* And the sweep genuinely covered the range in one refresh period rather than needing a second. */
        Assert.True((long)passes * slice >= watermark);
        Assert.True(passes <= QueryStorePlanXmlState.RefreshAfter.Ticks / TimeSpan.FromMinutes(5).Ticks);
    }

    /// <summary>A misconfigured budget floors the window rather than producing zero or a negative one.</summary>
    [Fact]
    public void CandidatePlanCount_WithNonPositiveBudget_FloorsAndReportsClamped()
    {
        var k = QueryStorePlanXmlState.CandidatePlanCount(160 * 1024L, 0, out var clamped);

        Assert.Equal(QueryStorePlanXmlState.MinCandidatePlans, k);
        Assert.True(clamped);
    }

    /// <summary>
    /// The estimator reproduces the measured fleet numbers from the same two inputs a pass already has, which
    /// is the whole reason no probe is needed: 12.1 MB over 78 plans is the q1 average, 12.3 MB over 828 is q4.
    /// </summary>
    [Theory]
    [InlineData(12.1, 78, 158)]
    [InlineData(12.3, 828, 15)]
    public void ObservedAvgPlanBytes_ReproducesTheMeasuredQuartiles(double shippedMb, int plans, int expectedKb)
    {
        var avg = QueryStorePlanXmlState.ObservedAvgPlanBytes((long)(shippedMb * 1024 * 1024), plans);

        Assert.NotNull(avg);
        Assert.Equal(expectedKb, avg!.Value / 1024);
    }

    /// <summary>A pass that shipped no plans teaches nothing about plan size and must leave the previous
    /// estimate standing rather than replace it with a fallback.</summary>
    [Fact]
    public void ObservedAvgPlanBytes_OnAQuietPass_IsNull()
    {
        Assert.Null(QueryStorePlanXmlState.ObservedAvgPlanBytes(0, 0));
        Assert.Null(QueryStorePlanXmlState.ObservedAvgPlanBytes(5_000, 0));
    }

    /// <summary>
    /// THE POINT OF THE WHOLE REDESIGN: a budget-cut pass still advances. Under plan_id-ordered shipping a cut
    /// truncates a SUFFIX, so the highest landed id is safe. The previous design shipped in
    /// last_execution_time order, where a cut left an arbitrary subset, no value was safe, and the guard that
    /// followed meant the watermark could not advance on 97.8% of passes.
    /// </summary>
    [Fact]
    public void AdvanceWatermark_OnABudgetCutPass_StillAdvances()
    {
        var cut = QueryStorePlanXmlState.AdvanceWatermark(100, new long[] { 101, 102 });

        Assert.Equal(102, cut.Watermark);
        Assert.True(cut.ArrivedInPlanIdOrder);
    }

    /// <summary>Never backward, and a quiet pass earns nothing: lowering the watermark refetches the catalog,
    /// and "no new plans this window" is an ordinary pass, not a reset.</summary>
    [Theory]
    [InlineData(new long[0], 100L)]
    [InlineData(new[] { 98L, 99L }, 100L)]
    [InlineData(new[] { 101L, 102L, 103L }, 103L)]
    [InlineData(new[] { 101L, 101L, 102L }, 102L)]
    public void AdvanceWatermark_NeverMovesBackward(long[] landed, long expected)
    {
        Assert.Equal(expected, QueryStorePlanXmlState.AdvanceWatermark(100, landed).Watermark);
    }

    /// <summary>
    /// A descent ABANDONS the advance rather than honouring the leading ascending run. Honouring it looks
    /// safer and is not: given {105, 101} it would advance to 105, and with ordering broken there is no basis
    /// for inferring that every SELECTED plan below 105 landed — so a plan whose XML never arrived would be
    /// suppressed until the refresh horizon. One lost pass of progress is the cheap side of that trade.
    /// </summary>
    [Theory]
    [InlineData(new[] { 101L, 102L, 99L, 105L })]
    [InlineData(new[] { 105L, 101L })]
    public void AdvanceWatermark_WhenOrderingIsViolated_RefusesToAdvance(long[] landed)
    {
        var refused = QueryStorePlanXmlState.AdvanceWatermark(100, landed);

        Assert.Equal(100, refused.Watermark);
        Assert.False(refused.ArrivedInPlanIdOrder,
            "the caller needs this to LOG the violation instead of just watching the watermark stop");
    }

    /// <summary>
    /// #2210: the plan fetch admits a plan when the total BEFORE it was under budget, never on the cumulative
    /// total alone. The naive <c>running_bytes &lt;= budget</c> is a per-database STALL — a plan bigger than the
    /// whole budget exceeds it on its own row, so it is excluded, every later row is excluded too, the pass ships
    /// nothing, the watermark holds, and the next pass re-selects the same plan forever. One 13 MB plan against
    /// the 12 MB default does it.
    ///
    /// <para>A SHAPE pin, and worth being clear about its limit: it asserts the predicate the SQL carries, not
    /// what SQL Server does with it. The two behavioural cases the reviewer asked for — an oversized plan ships
    /// alone and advances the watermark, and an oversized plan mid-window cuts AFTER it rather than dropping it —
    /// need a real Query Store to execute and belong to the measurement session before this leaves draft. What
    /// this catches is the regression that reintroduces the naive form, which is the cheap half and the half a
    /// future editor is most likely to trip.</para>
    /// </summary>
    [Fact]
    public void PlanFetch_AdmitsAPlanOnTheTotalBeforeIt_SoOneOversizedPlanCannotStall()
    {
        var sql = QueryStoreCollector.Instance.BuildPlanFetchQuery(
            Db, Context(capturePlanXml: true, state: new Dictionary<string, string>()),
            watermark: 900_000, candidatePlans: 114, budgetBytes: 12L * 1024 * 1024).Text;

        Assert.Contains("b.running_bytes - b.plan_bytes < 12582912", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("b.running_bytes <= ", sql, StringComparison.Ordinal);

        /* The coarse bound sorts and filters on plan_id alone — no XML touched — which is what caps the
           decompression the exact bound would otherwise pay across a whole catalog. */
        /* The CONVERT sits INSIDE the window and the running total measures the converted text, so a shipped
           plan is decompressed once. Measured on a 73,163-plan production catalog: this shape 133ms against
           274ms for measuring DATALENGTH(qsp.query_plan) in the window and joining back for the text, same 114
           rows and 1.7MB out of both. Plan-id-only with no XML was 114ms. */
        Assert.Contains("query_plan_text = CONVERT(nvarchar(max), qsp.query_plan)", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT TOP (114)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN sys.query_store_plan", sql, StringComparison.Ordinal);

        /* A NULL query_plan counts as zero bytes and still ships. Letting NULL propagate would make the budget
           predicate NULL, filter the row out, and a window of all-NULL plans would then hold the watermark and
           re-select forever — the oversized-plan stall by another route. */
        Assert.Contains("COALESCE(DATALENGTH(c.query_plan_text), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE qsp.plan_id > 900000", sql, StringComparison.Ordinal);
        Assert.Contains("ROWS UNBOUNDED PRECEDING", sql, StringComparison.Ordinal);
    }

    /// <summary>The ordering verdict rides along with the advance on the cases that are fine.</summary>
    [Theory]
    [InlineData(new[] { 101L, 102L, 103L })]
    [InlineData(new[] { 101L, 101L, 102L })]
    [InlineData(new[] { 7L })]
    [InlineData(new long[0])]
    public void AdvanceWatermark_AcceptsNonDescendingArrival(long[] landed)
    {
        Assert.True(QueryStorePlanXmlState.AdvanceWatermark(100, landed).ArrivedInPlanIdOrder);
    }
}
