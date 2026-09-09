/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>collection_log.sql_duration_ms</c> is documented and consumed as the time a collector spent querying
/// the MONITORED SERVER, and on the collectors that fetch plan XML or statement text it is mostly not that
/// (#3192).
///
/// <para><b>The mechanism.</b> The enumerated driver's per-item stopwatch wraps the watermark refresh and the
/// whole <c>readItem</c> closure (<c>EnumeratedCollectorDriver.RunAsync</c>), and for <c>query_store</c> that
/// closure calls the deferred plan and text fetches — each of which round-trips the STORE to learn what
/// content is already held before writing back what came off the target. On one measured production run
/// <c>sql_duration_ms</c> was 124,972 ms of which the two store probes were 107,334 ms (86%), against a
/// plan-plus-text target time of 6,494 ms. Fleet-wide the store probe is the largest single term in both
/// fetches: 55.4% of <c>plan_fetch</c> and 80.6% of <c>text_fetch</c> (V110). So the product's own
/// "is the target slow or is the store slow" split pointed the wrong way on its heaviest collector, and the
/// projection comment that states the split's purpose sat directly above the column that inverted it.</para>
///
/// <para><b>What was NOT done, which is the load-bearing half of the decision.</b> The obvious fix — subtract
/// the store terms from <c>sql_duration_ms</c>, or exclude the fetches from the driver's slice — changes the
/// meaning of a persisted column, and the past cannot be brought along. <c>CollectorRunResult.SqlMs</c> also
/// feeds <c>collect.collector_cost</c>, which is a 90-day hourly aggregate carrying NO phase split
/// (<c>metric_time, server_id, database_name, collector_name, run_count, total_sql_ms, max_sql_ms,
/// total_storage_ms, total_rows</c>) built by an in-memory accumulator rather than re-aggregated from
/// <c>collection_log</c> — so there is nothing there to subtract and no source to re-derive from. Re-basing
/// would leave 90 days meaning one thing and every row after meaning another, under a Collector Cost
/// Regression self-alert whose baseline window is 14 days, which is a fortnight in which a real target-side
/// regression is measured against an inflated baseline. And it would not help those 90 days at all.</para>
///
/// <para>So the attribution is PUBLISHED rather than applied: <c>SqlStoreMs</c> derives the store share from
/// the V110 columns already on the row, which makes it retroactive to every row that has them and leaves
/// every persisted column exactly as it was. These tests pin the arithmetic, the emit, the caveats on the two
/// surfaces that drew the wrong inference, and the premise the FLOOR caveat rests on.</para>
/// </summary>
public class CollectionLogStoreProbeAttributionTests
{
    /// <summary>
    /// The arithmetic that IS the feature: probe + write of BOTH halves, and NOT the target halves, which are
    /// genuinely the monitored server's work and belong where they are.
    ///
    /// <para>Every figure is deliberately distinct and no two sum to a third, so an implementation that
    /// dropped a term, or added the target halves, or summed only one half, produces a different number
    /// rather than a coincidentally equal one. The three <c>NotEqual</c>s name the specific wrong
    /// implementations: probe-only, whole-fetch (probe+target+write), and plan-half-only.</para>
    /// </summary>
    [Fact]
    public void TheStoreShareIsProbePlusWrite_AndExcludesTheTargetHalves()
    {
        var row = Row(
            sqlDurationMs: 124_972,
            planProbe: 54_016, planTarget: 4_100, planWrite: 830,
            textProbe: 53_318, textTarget: 2_394, textWrite: 190);

        Assert.Equal(54_016 + 830 + 53_318 + 190, row.SqlStoreMs);
        Assert.Equal(108_354, row.SqlStoreMs);

        /* Probe alone - the shape that reads "the probe is the problem" and silently drops the write-back. */
        Assert.NotEqual(54_016 + 53_318, row.SqlStoreMs);

        /* The whole fetch, target included - the shape that over-claims and would make
           sql_duration_ms - sql_store_ms understate target time instead of bounding it above. */
        Assert.NotEqual(54_016 + 4_100 + 830 + 53_318 + 2_394 + 190, row.SqlStoreMs);

        /* One half only - the plan side, which is the half a copy-paste stops at. */
        Assert.NotEqual(54_016 + 830, row.SqlStoreMs);

        /* And the point of the whole exercise, as a comparison rather than as prose: the store share is the
           majority of a figure documented as the monitored server's. */
        Assert.True(row.SqlStoreMs > row.SqlDurationMs / 2,
            "The measured run this property exists for had 86% of its 'target-side' figure in the store.");
    }

    /// <summary>
    /// NULL when no deferred fetch ran, which is every collector but the fetching ones and most runs of even
    /// those. NULL says "nothing here is attributable", which is a different claim from "the store share was
    /// zero" — and only one of them is true, because the per-item watermark refresh is a store read inside
    /// the same stopwatch on every <c>query_store</c> item whether a fetch ran or not.
    ///
    /// <para>Per HALF for the non-null case, matching V110's own contract: a run that fetched text but no
    /// plans has one block and not the other, and the store share is still the terms that exist.</para>
    /// </summary>
    [Fact]
    public void TheStoreShareIsNullWhenNoFetchRan_AndCountsWhicheverHalvesDidRun()
    {
        /* A plain single-query collector: no fetch columns at all. */
        Assert.Null(Row(sqlDurationMs: 4_000).SqlStoreMs);

        /* Text only. The plan terms are absent, not zero, and must not be read as measured zeros. */
        var textOnly = Row(sqlDurationMs: 9_000, textProbe: 400, textTarget: 80, textWrite: 20);
        Assert.Equal(420, textOnly.SqlStoreMs);

        /* Plan only. */
        var planOnly = Row(sqlDurationMs: 9_000, planProbe: 400, planTarget: 80, planWrite: 20);
        Assert.Equal(420, planOnly.SqlStoreMs);

        /* A fetch that ran and cost nothing measurable reports 0, not null: V110's gate cannot separate that
           from "no fetch ran", and this property inherits the ambiguity rather than inventing a resolution
           for it. Asserted so the inheritance is deliberate and not a coincidence of the expression. */
        var subMillisecond = Row(sqlDurationMs: 9_000, planProbe: 0, planTarget: 0, planWrite: 0);
        Assert.Equal(0, subMillisecond.SqlStoreMs);
        Assert.NotNull(subMillisecond.SqlStoreMs);
    }

    /// <summary>
    /// The premise the FLOOR caveat rests on, pinned as source so it cannot quietly stop being true.
    ///
    /// <para>The store share is a floor and not the whole because the per-item watermark refresh — a store
    /// read, and a store WRITE on the catch-up/adaptive path — is inside the same stopwatch and reaches no
    /// column. Two facts make that so: the ENUMERATED branch never declares V108's phases measured, so
    /// <c>watermark_ms</c> is NULL on exactly the rows carrying fetch columns; and
    /// <c>CollectorContext.PerItemWatermarkMs</c> reaches only the Debug log lines. If either changes, the
    /// caveat on <c>SqlStoreMs</c> and on both tool descriptions is wrong, and this is the test that says
    /// so.</para>
    /// </summary>
    [Fact]
    public void TheEnumeratedWatermarkIsInsideTheSliceAndReachesNoColumn_WhichIsWhyTheShareIsAFloor()
    {
        var driver = ReadSource("PerformanceMonitor.Collectors/EnumeratedCollectorDriver.cs");

        /* The slice really does wrap the watermark delegate, not only the read. Both awaits sit between the
           StartNew and the finally that banks it. */
        var start = driver.IndexOf("var sqlSlice = Stopwatch.StartNew();", StringComparison.Ordinal);
        var banked = driver.IndexOf("itemSqlMs = sqlSlice.ElapsedMilliseconds;", StringComparison.Ordinal);
        Assert.True(start > 0 && banked > start, "Could not locate the driver's per-item SQL slice.");

        var slice = driver[start..banked];
        var watermarkAt = slice.IndexOf("await perItemWatermark(item, itemToken);", StringComparison.Ordinal);
        var readAt = slice.IndexOf("batch = await readItem(item, itemToken);", StringComparison.Ordinal);
        Assert.True(watermarkAt > 0, "The watermark refresh is no longer inside the driver's SQL slice.");
        Assert.True(readAt > watermarkAt, "The read must still follow the watermark refresh inside the slice.");

        /* And the clock is not reset between them. A Restart() after the watermark award would exclude it
           from the slice while leaving both awaits textually where they are - the mutation the two
           Contains-shaped assertions above cannot see, and the reason they are not the whole check. */
        Assert.DoesNotContain("sqlSlice.Restart()", driver, StringComparison.Ordinal);
        Assert.Equal(1, CountOccurrences(driver, "sqlSlice = Stopwatch.StartNew();"));

        /* Positive control for that pair of negatives: the slice's own clock really is a Stopwatch under
           that name, so neither assertion is passing against a variable that has been renamed away. */
        Assert.Contains("var sqlSlice = Stopwatch.StartNew();", driver, StringComparison.Ordinal);

        /* The GATE, called on the shipped record rather than asserted about the source: a result that
           measured a watermark but did not declare V108's phases measured persists no phase triple at all,
           so watermark_ms is NULL however large that read was. The two paths share ONE return statement, so
           there is no per-branch literal to assert about - the flag is a variable, and asserting the absence
           of "ServerPhasesMeasured: true" at that return would have been a pin that no reachable mutation
           could turn red. */
        var enumeratedShape = new CollectorRunResult(
            Rows: 11_614, SqlMs: 124_972, StorageMs: 1_500,
            Measurements: CollectorContext.NoMeasurements,
            ServerPhasesMeasured: false,
            ServerWatermarkMs: 50_000,
            FetchPhases: new FetchPhaseCost(new FetchPhaseSums(54_016, 4_100, 830, 12, 6_252), null));

        Assert.Null(enumeratedShape.ServerPhases);
        Assert.NotNull(enumeratedShape.FetchPhases);

        /* Positive control for that null: the identical construction WITH the flag does report a triple, so
           the assertion above is the gate answering and not a member that has stopped existing. */
        Assert.NotNull((enumeratedShape with { ServerPhasesMeasured = true }).ServerPhases);

        /* And the flag is raised at exactly ONE site, which sits BELOW the enumerated branch's driver call -
           so the enumerated path cannot reach it. A second assignment anywhere, including inside that
           branch, reds the count. */
        var runner = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs");
        Assert.Equal(1, CountOccurrences(runner, "serverPhasesMeasured = true;"));
        Assert.True(
            runner.IndexOf("serverPhasesMeasured = true;", StringComparison.Ordinal)
                > runner.IndexOf("EnumeratedCollectorDriver.RunAsync<TRow>(", StringComparison.Ordinal),
            "The measured flag must stay below the enumerated branch, which is what keeps watermark_ms NULL "
            + "on precisely the rows sql_store_ms is non-null on - the premise the FLOOR caveat rests on.");

        /* And the stamp reaches no persisted column: the collection_log INSERT names no watermark parameter
           fed from the per-item member, so there is nothing to subtract even for a reader who wants to. */
        Assert.DoesNotContain("PerItemWatermarkMs",
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs"),
            StringComparison.Ordinal);

        /* Positive control: the member exists and the runner really does read it. */
        Assert.Contains("context.PerItemWatermarkMs", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// Persisting or deriving a figure nothing REPORTS is half a feature — the failure V108 and V109 each
    /// shipped on this exact table and V110 fixed in passing. So the emit is pinned, and pinned as a
    /// delegation to the shipped property rather than as a recomputation at the projection: a second copy of
    /// the arithmetic is a second thing to get wrong, and the <c>SqlOtherMs</c> precedent is that the
    /// subtraction has one definition a test can reach.
    /// </summary>
    [Fact]
    public void TheProjectionEmitsTheStoreShare_ByDelegatingToTheShippedProperty()
    {
        var tools = ReadSource("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs");

        Assert.Contains("sql_store_ms = r.SqlStoreMs", tools, StringComparison.Ordinal);

        /* It sits beside the two columns whose comment states the split's purpose, which is the comment the
           defect was found under - not buried inside a fetch block that a row without a fetch would null
           away along with the attribution. */
        var sqlAt = tools.IndexOf("sql_duration_ms = r.SqlDurationMs", StringComparison.Ordinal);
        var storeShareAt = tools.IndexOf("sql_store_ms = r.SqlStoreMs", StringComparison.Ordinal);
        var planBlockAt = tools.IndexOf("plan_fetch = r.PlanFetchProbeMs is null", StringComparison.Ordinal);
        Assert.True(sqlAt > 0 && storeShareAt > sqlAt && storeShareAt < planBlockAt,
            "sql_store_ms must be emitted flat beside sql_duration_ms, not nested in a fetch block.");

        /* No second copy of the arithmetic anywhere in the projection: the terms must not be re-added here. */
        Assert.DoesNotContain("r.PlanFetchProbeMs.Value + r.PlanFetchWriteMs", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two surfaces that drew the target-side inference now refuse to draw it, asserted against the
    /// SHIPPED attribute text rather than a source grep — a description is a consumer API for an LLM client,
    /// and reading it off the attribute is what a client actually receives.
    ///
    /// <para><c>get_collector_cost</c> is the one that mattered most and the one that can do least: its
    /// series carries no phase split, so it can only name the caveat and point at the per-run tool. Both of
    /// its response shapes carry it, from one constant — the single-collector trend is the shape a regression
    /// investigation lands on and it had no caveat at all.</para>
    /// </summary>
    [Fact]
    public void NeitherCostSurfaceStillClaimsTheFigureIsTargetSide()
    {
        var cost = ToolDescription(typeof(DarlingMcpCollectorCostTools), nameof(DarlingMcpCollectorCostTools.GetCollectorCost));

        /* The exact phrase that made the claim. */
        Assert.DoesNotContain("target-side query duration", cost, StringComparison.OrdinalIgnoreCase);

        /* Positive control for that negative: the description is really being read, and really is the one. */
        Assert.Contains("per-collector cost", cost, StringComparison.OrdinalIgnoreCase);

        Assert.Contains("MONITORING STORE", cost, StringComparison.Ordinal);
        Assert.Contains("sql_store_ms", cost, StringComparison.Ordinal);
        Assert.Contains("no phase split", cost, StringComparison.OrdinalIgnoreCase);

        /* Both response shapes carry the caveat, and from ONE constant rather than two copies that could
           drift - the trend shape is the one a regression lands on. */
        var source = ReadSource("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorCostTools.cs");
        Assert.Equal(2, CountOccurrences(source, "+ StoreProbeCaveat"));
        Assert.DoesNotContain("target-side query DURATION", source, StringComparison.Ordinal);

        var log = ToolDescription(typeof(DarlingMcpDataTools), nameof(DarlingMcpDataTools.GetCollectionLog));
        Assert.Contains("sql_store_ms", log, StringComparison.Ordinal);
        Assert.Contains("UPPER bound", log, StringComparison.Ordinal);
        Assert.Contains("MONITORING STORE", log, StringComparison.Ordinal);

        /* And the web grid's column header, which named the monitored server in two words. */
        var page = ReadSource("Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/server-tabs.js");
        Assert.DoesNotContain("label: \"On Server\"", page, StringComparison.Ordinal);
        Assert.Contains("{ key: \"sql_store_ms\", label: \"Store (in SQL)\", format: \"ms\" },", page, StringComparison.Ordinal);

        /* Positive control: the sibling header this one sat beside is untouched, so the DoesNotContain above
           is not passing because the grid moved somewhere this test cannot see. */
        Assert.Contains("label: \"On Store\"", page, StringComparison.Ordinal);
    }

    /// <summary>
    /// The decision NOT to re-base the column, pinned where reversing it would be silent.
    ///
    /// <para><c>collect.collector_cost</c> is V105's nine columns and no phase split, so a re-based
    /// <c>SqlMs</c> could not be reconciled against the 90 days already in it — and the worker hands the
    /// accumulator <c>result.SqlMs</c> unmodified, which is what makes the series internally consistent
    /// across the deploy. Both halves are asserted: a future change that subtracts at the call site, or one
    /// that adds a phase column here, reds this test and has to move the caveats on
    /// <c>get_collector_cost</c>, <c>CollectorCostAccumulator</c> and <c>SqlStoreMs</c> with it. That is the
    /// point — the trade is recorded, not forbidden.</para>
    /// </summary>
    [Fact]
    public void TheCostSeriesIsUnchanged_SoItsNinetyDaysStayComparableWithWhatFollows()
    {
        var v105 = PgMigrations.Scripts.Single(s => s.Version == 105).Sql;

        foreach (var column in new[]
                 {
                     "metric_time", "server_id", "database_name", "collector_name",
                     "run_count", "total_sql_ms", "max_sql_ms", "total_storage_ms", "total_rows",
                 })
        {
            Assert.Contains(column, v105, StringComparison.Ordinal);
        }

        /* No phase split in this series, which is the bound on what any fix could achieve here. */
        foreach (var absent in new[] { "probe_ms", "sql_open_ms", "sql_drain_ms", "watermark_ms", "sql_store_ms" })
        {
            Assert.DoesNotContain(absent, v105, StringComparison.Ordinal);
        }

        /* Positive control for those five negatives, through the identical containment form. */
        Assert.Contains("total_sql_ms", v105, StringComparison.Ordinal);

        /* And the blended figure is handed over unmodified. A subtraction here is the option-1 change, and it
           is a decision rather than a tidy-up. */
        Assert.Contains(
            "_collectorCost.Record(runtime.ServerId, collectorName, result.Rows, result.SqlMs, result.StorageMs);",
            ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs"), StringComparison.Ordinal);
    }

    /// <summary>
    /// The driver's per-item budget doc used to say the budget was null for "every collector but
    /// <c>query_store</c>", in two places. Four definitions declare one and two of those also enumerate, so
    /// <c>plan_correction</c> reaches that parameter non-null as well — found while correcting the parameter
    /// immediately above it.
    ///
    /// <para>Derived from <c>CollectorCatalog.All</c> rather than restated, because a name in prose is a
    /// frozen enumeration: it was right when written and went wrong the moment a second collector earned a
    /// budget, with nothing to say so. The same argument <c>CollectorCatalog.HasWallClockBudget</c>'s own doc
    /// makes for existing at all.</para>
    /// </summary>
    [Fact]
    public void TheDriverNoLongerNamesOneCollectorAsTheOnlyBudgetedOne()
    {
        var budgeted = CollectorCatalog.All
            .Where(d => d.PerItemWallClockBudget is not null)
            .Select(d => d.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        /* More than one, which is the whole claim - asserted as a count derived from the catalog so it stays
           true as the catalog grows, and named too so a definition losing its budget is visible. */
        Assert.True(budgeted.Count > 1, "The doc's premise was that exactly one collector declares a budget.");
        Assert.Contains("query_store", budgeted);
        Assert.Contains("plan_correction", budgeted);

        /* plan_correction is the one that makes the old wording false: it declares a budget AND enumerates,
           so it reaches the driver's perItemBudget parameter. Derived by calling the real definition. */
        var planCorrection = CollectorCatalog.All.Single(d => d.Name == "plan_correction");
        Assert.NotNull(planCorrection.PerItemWallClockBudget);

        var driver = ReadSource("PerformanceMonitor.Collectors/EnumeratedCollectorDriver.cs");
        Assert.DoesNotContain("Null (every collector but <c>query_store</c>)", driver, StringComparison.Ordinal);
        Assert.DoesNotContain("which is every collector but <c>query_store</c>", driver, StringComparison.Ordinal);

        /* Positive control for the two negatives: the parameter they document is still there under that
           name, so the assertions are not passing on a file that no longer says anything. */
        Assert.Contains("<param name=\"perItemBudget\">", driver, StringComparison.Ordinal);

        /* THREE copies of the claim, not two - the third is at the runner's own call site, and it is the
           one a reader of the enumerated branch actually meets. */
        var runner = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs");
        Assert.DoesNotContain("Null for every collector but", runner, StringComparison.Ordinal);

        /* And no FOURTH copy, swept by counting the phrase against its QUOTED form. The correction has to be
           able to name what it corrected, so a flat DoesNotContain fails on this very fix's own prose - the
           trap V110's accumulation pin hit and documented. Every surviving occurrence must be inside a
           `Not "..."` quotation; a new one asserted as fact breaks the equality.

           Stated rather than implied: this pair catches a REVERSION and a NEW copy, and does not catch
           deleting the corrected paragraph outright. The catalog-derived assertions above are what make the
           claim itself checkable; these two make its retraction stick. */
        foreach (var source in new[] { driver, runner })
        {
            Assert.Equal(
                CountOccurrences(source, "Not \"every collector but"),
                CountOccurrences(source, "every collector but"));
        }

        /* Positive control for that equality: the phrase really is present in both files in its quoted form,
           so 0 == 0 cannot be what is passing. */
        Assert.True(CountOccurrences(driver, "every collector but") > 0);
        Assert.True(CountOccurrences(runner, "every collector but") > 0);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal); at >= 0;
             at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>The SHIPPED [Description] an MCP client receives, off the method rather than out of source.</summary>
    private static string ToolDescription(Type toolType, string methodName) =>
        toolType.GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)!
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

    /// <summary>One collection_log row as the reader materializes it.</summary>
    private static DarlingDataReader.CollectionLogEntry Row(
        double sqlDurationMs,
        double? planProbe = null, double? planTarget = null, double? planWrite = null,
        double? textProbe = null, double? textTarget = null, double? textWrite = null) =>
        new(
            CollectorName: "query_store",
            CollectionTime: new DateTime(2026, 9, 9, 0, 0, 0, DateTimeKind.Utc),
            DurationMs: sqlDurationMs,
            SqlDurationMs: sqlDurationMs,
            StoreDurationMs: 1_500,
            RowsCollected: 11_614,
            Status: "SUCCESS",
            ErrorMessage: null,
            PlanFetchProbeMs: planProbe,
            PlanFetchTargetMs: planTarget,
            PlanFetchWriteMs: planWrite,
            TextFetchProbeMs: textProbe,
            TextFetchTargetMs: textTarget,
            TextFetchWriteMs: textWrite);

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
