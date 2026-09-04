/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V110 rung - the PER-DATABASE plan/text fetch split, summed across a run's fan-out and persisted on
/// <c>collection_log</c> (#2860). V108's twin one execution path over: V108 decomposed a SERVER-scoped
/// collector's <c>sql_duration_ms</c> into open and drain, and this decomposes the deferred fetch that only
/// the ENUMERATED path performs.
///
/// <para><b>Why it exists.</b> #2811's sub-split reports <c>plan_fetch:Nms = probe: + target: + write: +
/// other:</c> per database, emit-only. Measured over 38.2 h on 42 members, the STORE PROBE is the largest
/// single term - 55.4% of <c>plan_fetch</c> and 80.6% of <c>text_fetch</c>, against the target's 41.1% /
/// 18.2% and a <c>write:</c> of 3.5% / 1.1% - which inverts the illustrative shape #2811/#2812 were written
/// against. A number that corrects the reader's prior needs to be queryable, not scraped per server over an
/// SSM session, which is the same reachability argument V108 was filed on.</para>
///
/// <para><b>Why SUMS, which was not one of the three shapes the issue offered.</b> The split is N:1 against
/// this row - N &gt; 1 on 68.8% of <c>query_store</c> runs, mean 2.7 databases, max 7 - and sums dissolve
/// that rather than trading against it, because the two questions are already separated across two
/// mechanisms: V80's <c>slowest_item</c> answers WHICH DATABASE 1:1, and a sum answers WHICH PHASE exactly.
/// The rejected alternative is the informative one. A slowest-database rollup names the run's true winning
/// phase 92.8% of the time (89.5% on multi-database runs), but of its 199 disagreements 170 read a
/// <c>probe</c> truth as <c>target</c> against only 26 the reverse - a ~6.5 : 1 bias toward indicting the
/// monitored target for what was actually the store probe, which is the precise misreading this
/// instrumentation family exists to end.</para>
/// </summary>
public class CollectionLogFetchPhaseSumsStoreTests
{
    internal const int RungVersion = 110;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collection-log-fetch-phase-sums",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// Ten nullable integers and a view refresh. Nullable with no DEFAULT is what makes this safe against the
    /// busiest table in the store: a catalog-only change stays instant on a compressed hypertable, where
    /// adding a column WITH a default is the shape TimescaleDB has historically refused.
    ///
    /// <para><c>integer</c> and not <c>numeric</c> anywhere, asserted rather than assumed: every figure is a
    /// whole millisecond or a whole id count, matching <c>sql_duration_ms</c> and V108's columns, so there is
    /// no precision or scale to pick. The rates the counts exist for (ms per id) are divided by the READER in
    /// floating point - baking a scale into the schema would fix it for every future consumer of a number
    /// nobody has asked to round yet.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsTenNullableIntegerColumns_WithoutADefaultOrBackfillOrDecimalScale()
    {
        var sql = RungSql();

        Assert.Contains("ALTER TABLE collect.collection_log", sql, StringComparison.Ordinal);

        foreach (var column in ExpectedColumns)
        {
            Assert.Contains($"ADD COLUMN IF NOT EXISTS {column} integer", sql, StringComparison.Ordinal);
        }

        /* No DEFAULT and no backfill: a row written before this rung does not know its fetch split, and NULL
           says so where 0 would claim a fetch that ran and cost nothing. */
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);

        /* Whole milliseconds and whole counts - no fixed-point type has any business here. */
        Assert.DoesNotContain("numeric", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("decimal", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("double precision", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// There is NO CREATE TABLE half to this rung, and that is a finding rather than an omission - so it is
    /// asserted instead of left to be re-derived by the next person who reads the checklist item.
    ///
    /// <para>V2 creates <c>collection_log</c> with eleven columns and has never been widened. Every column
    /// added since - V80's fan-out rollup, V108's phase split, V109's drain forensics and now these ten -
    /// arrives only through its own rung, and a FRESH store lands the same schema as an UPGRADED one because
    /// the applier runs the whole ladder in ascending order over a new database exactly as it does over an
    /// old one. Editing V2 would be actively wrong: a store already stamped at 2 skips it forever
    /// (<c>if (migration.Version &lt;= currentVersion) continue;</c>), so the columns would exist only on
    /// stores created after the edit - the invisible two-population split the laddercheck discipline exists
    /// to prevent. <c>PgSchemaGenerator</c> is not a second creation path for this table either: it emits
    /// COLLECTOR tables, and <c>collection_log</c> is registered there as a non-collector.</para>
    /// </summary>
    [Fact]
    public void TheFreshStorePathIsTheLadderItself_SoV2IsDeliberatelyUntouched()
    {
        var v2 = PgMigrations.Scripts.Single(s => s.Version == 2).Sql;

        Assert.Contains("CREATE TABLE IF NOT EXISTS collection_log (", v2, StringComparison.Ordinal);

        /* Not in V2, and neither is any column from V80, V108 or V109 - the shape that proves the ladder is
           the only creation path and this rung needs no CREATE TABLE half. */
        foreach (var column in ExpectedColumns.Concat(new[] { "slowest_item_ms", "sql_drain_ms", "drain_last_read_ms" }))
        {
            Assert.DoesNotContain(column, v2, StringComparison.Ordinal);
        }

        /* Positive control for the two DoesNotContain sweeps above: the identical containment check finds a
           column V2 really does declare. Without it, a typo'd column name would make every assertion above
           pass by matching nothing - which is how a negative-proving grep reads as clean while proving
           nothing at all. */
        Assert.Contains("sql_duration_ms", v2, StringComparison.Ordinal);
    }

    /// <summary>
    /// The half of this rung that is easy to forget and impossible to notice: Postgres FREEZES a view's
    /// <c>SELECT *</c> column list at CREATE, so without the refresh an UPGRADED store keeps serving the
    /// pre-V110 list forever while a FRESH one works perfectly - the worst possible split. V14 exists because
    /// it already happened once; V80, V108 and V109 each re-learned it on this very table.
    /// </summary>
    [Fact]
    public void TheRungRefreshesThePassthroughView_AndTheReadsGoThroughIt()
    {
        Assert.Contains(
            "CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;",
            RungSql(), StringComparison.Ordinal);

        Assert.Contains("FROM v_collection_log", DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Two things this rung deliberately does NOT store, each for its own reason.
    ///
    /// <para><c>other:</c> is a residual, and #2859's rule holds: a stored copy could drift from the parent
    /// it completes. The honest extra note, which V108 did not have to make, is that the parent is not a
    /// column here either - there is no <c>plan_fetch_ms</c> - so the residual is not derivable from the
    /// store at all. That is accepted rather than overlooked because it measured 0.1% of both fetches
    /// fleet-wide, which makes probe + target + write the parent to within a rounding error.</para>
    ///
    /// <para><c>chunks</c> is left out as a batching artifact no question has asked for. The id counts are
    /// IN, and the reversal from #2860 §7 is deliberate: that section's own condition was "add them if a
    /// question actually needs them", and #2902 supplied one - the fetch carryover has no eviction of any
    /// kind, so <c>ids_attempted</c> beside <c>probe_ids</c> is the only stored signal that would show a
    /// backlog growing.</para>
    /// </summary>
    [Fact]
    public void TheRungStoresNoResidual_AndNoChunkCount_ButDoesStoreTheIdCounts()
    {
        var sql = RungSql();

        Assert.DoesNotContain("other_ms", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("chunk", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("plan_fetch_ms", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("text_fetch_ms", sql, StringComparison.Ordinal);

        /* Positive control for the four negatives above, through the identical containment form. */
        Assert.Contains("plan_fetch_target_ms", sql, StringComparison.Ordinal);

        Assert.Contains("plan_fetch_ids_attempted", sql, StringComparison.Ordinal);
        Assert.Contains("plan_fetch_probe_ids", sql, StringComparison.Ordinal);
        Assert.Contains("text_fetch_ids_attempted", sql, StringComparison.Ordinal);
        Assert.Contains("text_fetch_probe_ids", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The arithmetic that IS the feature: these are SUMS across the fan-out, not the last database's
    /// figures. Written as a test because the failure mode is silent and plausible - the per-item members are
    /// overwritten on every item by design, so an implementation that read them once at the end of the run
    /// would store the LAST database's split while looking exactly like this one and being wrong by a factor
    /// of the fan-out width. Three databases with deliberately distinct figures, so no accidental equality
    /// can hide a per-item read masquerading as a sum.
    /// </summary>
    [Fact]
    public void ThePhasesAreSummedAcrossTheFanout_NotTheLastItemsFigures()
    {
        var accumulator = new FetchPhaseCostAccumulator();

        accumulator.Observe(Item(planFetch: 100, probe: 60, target: 30, write: 10, ids: 5, probeIds: 90));
        accumulator.Observe(Item(planFetch: 400, probe: 220, target: 150, write: 30, ids: 22, probeIds: 310));
        accumulator.Observe(Item(planFetch: 7, probe: 4, target: 2, write: 1, ids: 1, probeIds: 6));

        var plan = accumulator.Result!.Value.Plan!.Value;

        Assert.Equal(284, plan.ProbeMs);
        Assert.Equal(182, plan.TargetMs);
        Assert.Equal(41, plan.WriteMs);
        Assert.Equal(28, plan.IdsAttempted);
        Assert.Equal(406, plan.ProbeIds);

        /* The negative form of the same claim, stated against the value a per-item read would have produced.
           The last item is deliberately the SMALLEST, so a regression to "read the members once at the end"
           reports 4ms of probe where the run really spent 284 - a 71x understatement rather than a subtle
           one, and this is the assertion that names it. */
        Assert.NotEqual(4, plan.ProbeMs);
    }

    /// <summary>
    /// The halves are independently nullable, matching how the log line emits its two sub-lines separately: a
    /// run that fetched statement text but no plans stores the five text figures and leaves the five plan
    /// ones NULL rather than claiming five measured zeros.
    /// </summary>
    [Fact]
    public void TheTwoHalvesAreIndependentlyNullable_AndAllOrNothingWithinAHalf()
    {
        var textOnly = new FetchPhaseCostAccumulator();
        textOnly.Observe(Item(textFetch: 500, textProbe: 400, textTarget: 80, textWrite: 20, textIds: 7, textProbeIds: 640));

        var result = textOnly.Result;
        Assert.NotNull(result);
        Assert.Null(result!.Value.Plan);
        Assert.NotNull(result.Value.Text);
        Assert.Equal(400, result.Value.Text!.Value.ProbeMs);
        Assert.Equal(7, result.Value.Text.Value.IdsAttempted);

        var planOnly = new FetchPhaseCostAccumulator();
        planOnly.Observe(Item(planFetch: 500, probe: 400, target: 80, write: 20, ids: 7, probeIds: 640));

        Assert.NotNull(planOnly.Result!.Value.Plan);
        Assert.Null(planOnly.Result.Value.Text);

        /* Neither fetch ran anywhere in the run - every collector but the fetching ones, and ~78% of even
           those runs. NULL, not a struct of ten zeros: "this run performed no deferred fetch" is a different
           claim from "its fetch was free", and only one of them is true. */
        var neither = new FetchPhaseCostAccumulator();
        neither.Observe(Item());
        neither.Observe(Item());
        Assert.Null(neither.Result);

        /* And an accumulator nothing was ever offered to - a plain single-query collector. */
        Assert.Null(new FetchPhaseCostAccumulator().Result);
    }

    /// <summary>
    /// A database whose batch was EMPTY but whose fetch still cost time is counted, and this is the one place
    /// the stored series deliberately covers more than the logged one.
    ///
    /// <para>The per-database log line is gated on <c>batchCount &gt; 0</c>, so a quiet database prints
    /// nothing. But #2902's carryover has no eviction, which means an empty-batch pass can still drain ids
    /// deferred from an earlier one - real fetch time on a database the log line is silent about, and
    /// precisely the time hardest to attribute by hand. The accumulator is fed from <c>onItemComplete</c>
    /// unconditionally, the same decision V80's fan-out rollup made and for the same reason.</para>
    /// </summary>
    [Fact]
    public void AQuietDatabaseThatStillPaidFetchTime_IsCounted()
    {
        var accumulator = new FetchPhaseCostAccumulator();

        /* Zero rows came back, and the fetch still spent 900ms draining carried-over ids. */
        accumulator.Observe(Item(planFetch: 900, probe: 40, target: 800, write: 60, ids: 26, probeIds: 0));

        var plan = accumulator.Result!.Value.Plan!.Value;
        Assert.Equal(800, plan.TargetMs);
        Assert.Equal(26, plan.IdsAttempted);

        /* probe_ids = 0 beside ids_attempted > 0 is the #2902 signature and it survives the roll-up intact:
           PerItemPlanProbeIds is set only when the probe had references to examine, so attempted ids with
           nothing probed can only be carried debt. Persisting both is what makes that readable in the store
           rather than only in a log parse. */
        Assert.Equal(0, plan.ProbeIds);

        Assert.Contains("fetchPhases.Observe(context);", RunnerSource(), StringComparison.Ordinal);

        /* The gate is NOT copied onto the accumulator call. If a batchCount test ever appears in front of it,
           the quiet-database time above stops being recorded and this test would still pass on its own
           arithmetic - so the call site is pinned too. */
        var runner = RunnerSource();
        var observeIndex = runner.IndexOf("fetchPhases.Observe(context);", StringComparison.Ordinal);
        var fanoutIndex = runner.IndexOf("fanout.Observe(item, itemSqlMs + itemStorageMs);", StringComparison.Ordinal);
        Assert.True(observeIndex > fanoutIndex,
            "The fetch-phase accumulation must sit beside the fan-out rollup in onItemComplete, which is the "
            + "hook that fires for EVERY completed item. Anywhere inside the batchCount > 0 block would drop "
            + "the carried-over fetch time on quiet databases (#2902).");

        /* The GATE's code form, not the bare comparison: the prose either side of the accumulation
           legitimately discusses batchCount, and matching the loose form made this assertion fail on its
           own explanatory comment. Pinning "if (batchCount > 0)" matches the branch and nothing a comment
           can say. */
        var between = runner[fanoutIndex..observeIndex];
        Assert.DoesNotContain("if (batchCount > 0)", between, StringComparison.Ordinal);

        /* Positive control for that negative: the identical containment form does find the gate where it
           really lives, a little further down the same hook. Without this, a renamed variable would make
           the assertion above pass by matching nothing at all. */
        Assert.Contains("if (batchCount > 0)", runner[fanoutIndex..], StringComparison.Ordinal);
    }

    /// <summary>
    /// Per-item figures are <c>int</c> but the run's total is summed in <c>long</c> and clamped on the way
    /// out. A silent <c>int</c> overflow here would surface as a NEGATIVE duration in the store, which is the
    /// one value a duration column must never hold - and unlike a wrong-but-positive number, nothing
    /// downstream would flag it.
    /// </summary>
    [Fact]
    public void TheSumsClampRatherThanOverflowing()
    {
        var accumulator = new FetchPhaseCostAccumulator();

        for (var i = 0; i < 3; i++)
        {
            accumulator.Observe(Item(
                planFetch: int.MaxValue, probe: int.MaxValue, target: int.MaxValue,
                write: int.MaxValue, ids: int.MaxValue, probeIds: int.MaxValue));
        }

        var plan = accumulator.Result!.Value.Plan!.Value;
        Assert.Equal(int.MaxValue, plan.ProbeMs);
        Assert.Equal(int.MaxValue, plan.IdsAttempted);
        Assert.True(plan.ProbeMs > 0, "A clamped sum must stay positive; an int overflow would go negative.");
    }

    /// <summary>
    /// The write really does carry all ten columns, BOTH writers of the shared statement bind them, and the
    /// MCP read really does select them back AND emit them. Pinned on source and SQL rather than a round trip
    /// so it runs everywhere, including where no Postgres is available.
    ///
    /// <para>The emit half is not decoration. V108 and V109 both widened <c>CollectionLogSql</c> and
    /// <c>CollectionLogEntry</c> and stopped there, so eight columns were read off the row into the record
    /// and dropped by the tool's projection - reachable only with psql on the monitoring box, which is the
    /// exact problem V108 was filed to fix. This rung fixes that in passing and pins the whole family, so a
    /// stored-but-unreported column cannot happen a fourth time.</para>
    /// </summary>
    [Fact]
    public void EveryColumnIsWrittenByBothWriters_SelectedBack_AndActuallyEmitted()
    {
        var observability = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs");
        var tools = ReadSource("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs");

        foreach (var column in ExpectedColumns)
        {
            Assert.Contains(column, observability, StringComparison.Ordinal);
            Assert.Contains(column, DarlingDataReader.CollectionLogSql, StringComparison.Ordinal);

            /* Emitted, not merely selected. */
            Assert.Contains($"{column} = ", tools, StringComparison.Ordinal);
        }

        /* And the family this rung joins, whose columns were selected but never emitted until now. */
        foreach (var sibling in new[]
                 {
                     "sql_open_ms", "sql_drain_ms", "watermark_ms", "drain_rows_read",
                     "drain_bytes_read", "drain_last_read_ms", "target_session_id", "sweep_peer_max_ms",
                 })
        {
            Assert.Contains($"{sibling} = ", tools, StringComparison.Ordinal);
        }

        /* The residual stays DERIVED and is reported as such - V108 stores no other_ms column so the terms
           cannot drift from the parent they decompose, but a large residual is itself the finding and was
           unreachable while the projection dropped it. */
        Assert.Contains("sql_other_ms = ", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// The connect-time gate. A COLUMN sentinel rather than a table one, because <c>collection_log</c> has
    /// existed since V1 and only its columns are new. Being the TOP rung, a fully-migrated store must map to
    /// exactly this version or the viewer refuses a store that is perfectly current.
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheColumn_AndMapsAFullyMigratedStoreToThisRung()
    {
        Assert.Contains(
            "table_name = 'collection_log' AND column_name = 'plan_fetch_target_ms'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        Assert.Contains("reader.GetBoolean(85)", ReadSource(
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs"), StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* Every sentinel true = a fully-migrated store, which must map to THIS rung. As the top rung this is
           also the "and no more than that" guard: a later rung appending a sentinel without its own arm
           would leave this returning 110 for a store that is actually further along. Built by reflection so
           the arity tracks the signature - the literal-true form silently defaults a newly added sentinel to
           false and maps one version low. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel present EXCEPT this one must report 109, not 110. Without this the
           arm above could be satisfied by an unconditional return and nothing would notice. */
        var allButMine = Enumerable.Repeat((object)true, arity).ToArray();
        allButMine[arity - 1] = false;
        Assert.Equal(109, (int)method.Invoke(null, allButMine)!);
    }

    /* internal so the live sibling asserts against the SAME list rather than a second copy that
       could drift from this one - the whole point of splitting is serialization, not divergence. */
    internal static readonly string[] ExpectedColumns =
    [
        "plan_fetch_probe_ms", "plan_fetch_target_ms", "plan_fetch_write_ms",
        "plan_fetch_ids_attempted", "plan_fetch_probe_ids",
        "text_fetch_probe_ms", "text_fetch_target_ms", "text_fetch_write_ms",
        "text_fetch_ids_attempted", "text_fetch_probe_ids",
    ];

    private static string RungSql() => PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

    private static string RunnerSource() =>
        ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs");

    /// <summary>One completed item's per-database split, as the runner's context would carry it.</summary>
    private static CollectorContext Item(
        long planFetch = 0, long probe = 0, long target = 0, long write = 0, int ids = 0, int probeIds = 0,
        long textFetch = 0, long textProbe = 0, long textTarget = 0, long textWrite = 0,
        int textIds = 0, int textProbeIds = 0) =>
        new()
        {
            ServerId = 1,
            ServerName = "sample",
            CollectionTime = new DateTime(2026, 9, 4, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            PerItemPlanFetchMs = planFetch,
            PerItemPlanProbeMs = probe,
            PerItemPlanTargetMs = target,
            PerItemPlanWriteMs = write,
            PerItemPlanIdsAttempted = ids,
            PerItemPlanProbeIds = probeIds,
            PerItemTextFetchMs = textFetch,
            PerItemTextProbeMs = textProbe,
            PerItemTextTargetMs = textTarget,
            PerItemTextWriteMs = textWrite,
            PerItemTextIdsAttempted = textIds,
            PerItemTextProbeIds = textProbeIds,
        };

    /// <summary>Reads a repo source file by walking up from the test binary to the repo root.</summary>
    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 12 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }

            dir = Directory.GetParent(dir)?.FullName;
        }

        throw new FileNotFoundException($"Could not locate {relativePath} from {AppContext.BaseDirectory}");
    }
}
