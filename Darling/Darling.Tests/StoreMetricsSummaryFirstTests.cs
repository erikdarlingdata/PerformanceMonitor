/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <c>get_store_metrics</c>' summary-first contract (#3903). Until #3903 every call returned the latest row
/// AND the daily series for every store object — about 250 objects, a schema-fixed count that does not shrink
/// with the fleet — so the default answer was 1.9 MB on every production store (1,744 KB of it the per-object
/// series) and 1.25 MB on DARLING01, larger than any MCP client's context. The default is now the store-level
/// blocks plus three lists bounded by <c>limit</c>; <c>object_kind</c> lists a kind, and an exact
/// <c>object_name</c> returns one object's series.
///
/// <para>The pure half pins the selection and ordering rules without a store; the live half
/// (<see cref="StoreMetricsSummaryFirstLivePostgresTests"/>) runs the tool, the web mirror and the triage
/// section against a store seeded to production's shape, which is where the payload budget is measured.</para>
/// </summary>
public sealed class StoreMetricsSummaryFirstTests
{
    private static readonly DateTime Sweep = new(2026, 9, 22, 22, 0, 0, DateTimeKind.Unspecified);

    private static DarlingStoreMetricsReader.StoreMetricRow Bytes(string kind, string name, long bytes) =>
        new(kind, name, Sweep, bytes, null, null, null, null, null);

    private static DarlingStoreMetricsReader.StoreMetricRow Job(string name, long lastRunMs, long intervalMs = 3_600_000) =>
        new(StoreSelfMetrics.BackgroundJobObjectKind, name, Sweep, null, null, null, null, null, null,
            LastRunDurationMs: lastRunMs, ScheduleIntervalMs: intervalMs, TotalRuns: 100, TotalFailures: 0);

    private static DarlingStoreMetricsReader.StoreMetricDailyPoint Point(
        string kind, string name, int day, long? bytes = null, long? runs = null, long? failures = null) =>
        new(kind, name, new DateTime(2026, 9, day, 0, 0, 0, DateTimeKind.Unspecified), bytes, null, null, null, null, null,
            TotalRuns: runs, TotalFailures: failures);

    private static MethodInfo ToolMethod() => typeof(DarlingMcpStoreMetricsTools)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_store_metrics");

    /// <summary>
    /// The listed kinds are the six byte-bearing kinds and the jobs, and never a block kind. The store,
    /// job_history and checkpointer rows are blocks on every response: the first two carry overloaded columns
    /// the blocks decode and the third cumulative counters its block differences, so as list rows they would
    /// read as sizes and run durations they are not. The pre-#3903 tool excluded them with three inequalities
    /// at two sites; the list is now the one place that decides, so it is pinned whole.
    /// </summary>
    [Fact]
    public void TheListedKinds_AreTheByteBearingKindsAndTheJobs_AndNeverABlockKind()
    {
        Assert.Equal(
            new[]
            {
                StoreSelfMetrics.BackgroundJobObjectKind, StoreSelfMetrics.ContinuousAggregateObjectKind,
                StoreSelfMetrics.DimensionObjectKind, StoreSelfMetrics.HypertableObjectKind,
                StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.TableObjectKind,
            },
            DarlingStoreMetricsReader.ListedKinds.Order(StringComparer.Ordinal).ToArray());

        foreach (var block in new[] { StoreSelfMetrics.StoreObjectKind, StoreSelfMetrics.JobHistoryObjectKind, StoreSelfMetrics.CheckpointerObjectKind })
        {
            Assert.DoesNotContain(block, DarlingStoreMetricsReader.ListedKinds);
            Assert.False(DarlingStoreMetricsReader.IsByteBearing(block));
        }

        Assert.False(DarlingStoreMetricsReader.IsByteBearing(StoreSelfMetrics.BackgroundJobObjectKind));
        Assert.All(
            DarlingStoreMetricsReader.ListedKinds.Where(k => k != StoreSelfMetrics.BackgroundJobObjectKind),
            k => Assert.True(DarlingStoreMetricsReader.IsByteBearing(k)));
    }

    /// <summary>
    /// A row's window delta is first daily point to last, exactly what a caller would compute from the series
    /// the drill-down returns, and it is null wherever there is nothing honest to state: a null at either end,
    /// or a cumulative job counter that went backwards (a reset, not a negative count). An object with one
    /// point has no delta at all, and (kind, name) is the key because a name can repeat across kinds.
    /// </summary>
    [Fact]
    public void TheWindowDelta_IsFirstToLastDailyPoint_AndNullWhereThereIsNothingToState()
    {
        const string ht = StoreSelfMetrics.HypertableObjectKind;
        const string job = StoreSelfMetrics.BackgroundJobObjectKind;

        var deltas = DarlingStoreMetricsReader.ComputeWindowDeltas(new[]
        {
            /* Out of order on purpose: the delta is taken over the days, not the row order. */
            Point(ht, "grows", 3, bytes: 180),
            Point(ht, "grows", 1, bytes: 100),
            Point(ht, "grows", 2, bytes: 150),
            Point(ht, "shrinks", 1, bytes: 500),
            Point(ht, "shrinks", 2, bytes: 450),
            Point(ht, "one_point", 2, bytes: 70),
            Point(ht, "null_end", 1, bytes: 10),
            Point(ht, "null_end", 2, bytes: null),
            Point(job, "runs", 1, runs: 10, failures: 1),
            Point(job, "runs", 2, runs: 34, failures: 3),
            Point(job, "reset", 1, runs: 50, failures: 4),
            Point(job, "reset", 2, runs: 5, failures: 0),
            /* The same name under another kind is another object. */
            Point(StoreSelfMetrics.ContinuousAggregateObjectKind, "grows", 1, bytes: 1),
            Point(StoreSelfMetrics.ContinuousAggregateObjectKind, "grows", 2, bytes: 2),
        });

        var grows = deltas[(ht, "grows")];
        Assert.Equal(new DateTime(2026, 9, 1), grows.Since);
        Assert.Equal(80L, grows.GrowthBytes);
        Assert.Null(grows.RunsInWindow);
        Assert.Null(grows.FailuresInWindow);

        Assert.Equal(-50L, deltas[(ht, "shrinks")].GrowthBytes);
        Assert.False(deltas.ContainsKey((ht, "one_point")));
        Assert.Null(deltas[(ht, "null_end")].GrowthBytes);
        Assert.Equal(1L, deltas[(StoreSelfMetrics.ContinuousAggregateObjectKind, "grows")].GrowthBytes);

        var runs = deltas[(job, "runs")];
        Assert.Null(runs.GrowthBytes);
        Assert.Equal(24L, runs.RunsInWindow);
        Assert.Equal(2L, runs.FailuresInWindow);

        var reset = deltas[(job, "reset")];
        Assert.Null(reset.RunsInWindow);
        Assert.Null(reset.FailuresInWindow);
    }

    private static readonly DarlingStoreMetricsReader.StoreMetricRow[] Latest =
    {
        new(StoreSelfMetrics.StoreObjectKind, "darling", Sweep, 1_000, null, null, null, null, 43),
        new(StoreSelfMetrics.JobHistoryObjectKind, "darling", Sweep, null, null, null, null, 48, null),
        new(StoreSelfMetrics.CheckpointerObjectKind, StoreSelfMetrics.CheckpointerObjectName, Sweep, null, null, null, null, null, null),
        Bytes(StoreSelfMetrics.HypertableObjectKind, "wait_stats", 300),
        Bytes(StoreSelfMetrics.HypertableObjectKind, "query_stats", 900),
        Bytes(StoreSelfMetrics.ContinuousAggregateObjectKind, "wait_stats_hourly", 200),
        Bytes(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, 800),
        Bytes(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 50),
        Job("policy_compression wait_stats [1012]", 100),
        Job("policy_retention wait_stats [1013]", 50),
        Job("policy_compression query_stats [1014]", 900),
    };

    /// <summary>
    /// The two filters resolve to one of three views, and only the listed kinds are ever candidates. The rule
    /// that needs pinning most is EXACT FIRST: wait_stats is also a substring of its aggregate and of both jobs
    /// named after it, so a contains-match alone could never reach the hypertable by itself. With no exact
    /// match, every name containing the text is listed; object_kind narrows before the name is matched; and a
    /// name that matches only a block row (the store's, the job_history role's) matches nothing.
    /// </summary>
    [Fact]
    public void TheFilters_ResolveExactFirst_ThenContains_AndOnlyEverToListedKinds()
    {
        var summary = DarlingStoreMetricsReader.SelectObjects(Latest, null, null);
        Assert.Equal(DarlingStoreMetricsReader.StoreMetricsView.Summary, summary.View);
        Assert.Equal(8, summary.Matched.Count);
        Assert.DoesNotContain(summary.Matched, r => !DarlingStoreMetricsReader.ListedKinds.Contains(r.ObjectKind));

        var kind = DarlingStoreMetricsReader.SelectObjects(Latest, StoreSelfMetrics.BackgroundJobObjectKind, null);
        Assert.Equal(DarlingStoreMetricsReader.StoreMetricsView.List, kind.View);
        Assert.Equal(3, kind.Matched.Count);

        /* Exact, case-insensitive, and the one object whose name is a substring of three others. */
        var exact = DarlingStoreMetricsReader.SelectObjects(Latest, null, "WAIT_STATS");
        Assert.Equal(DarlingStoreMetricsReader.StoreMetricsView.Object, exact.View);
        Assert.Equal(StoreSelfMetrics.HypertableObjectKind, Assert.Single(exact.Matched).ObjectKind);

        var partial = DarlingStoreMetricsReader.SelectObjects(Latest, null, "wait");
        Assert.Equal(DarlingStoreMetricsReader.StoreMetricsView.List, partial.View);
        Assert.Equal(4, partial.Matched.Count);

        /* object_kind narrows first, so the jobs named after a table are reachable as a list. */
        var jobsOfATable = DarlingStoreMetricsReader.SelectObjects(Latest, StoreSelfMetrics.BackgroundJobObjectKind, "wait_stats");
        Assert.Equal(DarlingStoreMetricsReader.StoreMetricsView.List, jobsOfATable.View);
        Assert.Equal(2, jobsOfATable.Matched.Count);
        Assert.All(jobsOfATable.Matched, r => Assert.Equal(StoreSelfMetrics.BackgroundJobObjectKind, r.ObjectKind));

        /* The store row and the job_history row are both named 'darling'; neither is ever a candidate. */
        var block = DarlingStoreMetricsReader.SelectObjects(Latest, null, "darling");
        Assert.Equal(DarlingStoreMetricsReader.StoreMetricsView.List, block.View);
        Assert.Empty(block.Matched);
        Assert.Empty(DarlingStoreMetricsReader.SelectObjects(Latest, null, StoreSelfMetrics.CheckpointerObjectName).Matched);

        Assert.Empty(DarlingStoreMetricsReader.SelectObjects(Latest, StoreSelfMetrics.TableObjectKind, null).Matched);
    }

    /// <summary>
    /// One order for every list: byte-bearing objects largest first, then the jobs, the ones whose failure count
    /// grew in the window first and then the closest to their own cadence. The failing job leads because a
    /// failure is the more urgent signal and the cadence alert never judges one (it reads successful runs
    /// only); here it has the SMALLEST cadence share, so an order that ignored failures would put it last.
    /// </summary>
    [Fact]
    public void TheListOrder_IsLargestFirst_ThenJobsFailingFirst_ThenClosestToCadence()
    {
        var rows = new[]
        {
            Job("near_cadence [2]", 3_000_000),
            Bytes(StoreSelfMetrics.HypertableObjectKind, "small", 100),
            Job("quiet [1]", 1_000),
            Bytes(StoreSelfMetrics.ContinuousAggregateObjectKind, "big", 300),
            Job("failing [3]", 10),
        };
        var deltas = new Dictionary<(string Kind, string Name), DarlingStoreMetricsReader.WindowDelta>
        {
            [(StoreSelfMetrics.BackgroundJobObjectKind, "failing [3]")] = new(new DateTime(2026, 9, 1), null, 700, 2),
            [(StoreSelfMetrics.BackgroundJobObjectKind, "near_cadence [2]")] = new(new DateTime(2026, 9, 1), null, 700, 0),
        };

        var ordered = DarlingStoreMetricsReader.OrderForList(rows, deltas).Select(r => r.ObjectName).ToArray();
        Assert.Equal(new[] { "big", "small", "failing [3]", "near_cadence [2]", "quiet [1]" }, ordered);

        Assert.Equal(DarlingMcpStoreMetricsTools.SizeOrder, DarlingMcpStoreMetricsTools.ListOrder(rows.Where(r => r.TotalBytes is not null).ToList()));
        Assert.Equal(DarlingMcpStoreMetricsTools.JobOrder, DarlingMcpStoreMetricsTools.ListOrder(rows.Where(r => r.TotalBytes is null).ToList()));
        Assert.Equal(
            DarlingMcpStoreMetricsTools.SizeOrder + "_then_" + DarlingMcpStoreMetricsTools.JobOrder,
            DarlingMcpStoreMetricsTools.ListOrder(rows));
    }

    /// <summary>The growth ranking covers byte-bearing objects that HAVE a growth figure, largest first; a
    /// shrinking object trails on its negative delta rather than vanishing, and a job never ranks here.</summary>
    [Fact]
    public void TheGrowthRanking_IsByteBearingObjectsWithAFigure_LargestFirst_ShrinkersTrailing()
    {
        var since = new DateTime(2026, 9, 1);
        var rows = new[]
        {
            Bytes(StoreSelfMetrics.HypertableObjectKind, "flat", 900),
            Bytes(StoreSelfMetrics.HypertableObjectKind, "shrinks", 800),
            Bytes(StoreSelfMetrics.HypertableObjectKind, "fast", 100),
            Bytes(StoreSelfMetrics.HypertableObjectKind, "no_figure", 5_000),
            Job("a job [1]", 10),
        };
        var deltas = new Dictionary<(string Kind, string Name), DarlingStoreMetricsReader.WindowDelta>
        {
            [(StoreSelfMetrics.HypertableObjectKind, "flat")] = new(since, 0, null, null),
            [(StoreSelfMetrics.HypertableObjectKind, "shrinks")] = new(since, -300, null, null),
            [(StoreSelfMetrics.HypertableObjectKind, "fast")] = new(since, 90, null, null),
            [(StoreSelfMetrics.BackgroundJobObjectKind, "a job [1]")] = new(since, null, 24, 0),
        };

        Assert.Equal(
            new[] { "fast", "flat", "shrinks" },
            DarlingStoreMetricsReader.OrderByGrowth(rows, deltas).Select(r => r.ObjectName).ToArray());
    }

    /// <summary>
    /// Each view's note says what the lists are and how to reach the rest, and the summary's says in so many
    /// words that the per-object series is NOT there and which parameter brings it back: a caller who learned
    /// this tool before #3903 found it in daily[], and must not read its absence as a store with no history.
    /// </summary>
    [Fact]
    public void TheNotes_SayWhichViewItIs_AndWhereThePerObjectSeriesWent()
    {
        var summary = DarlingMcpStoreMetricsTools.ViewNote(DarlingStoreMetricsReader.StoreMetricsView.Summary, null, null, 104, 10, true, 10);
        Assert.Contains("per-object daily series is NOT in the summary", summary, StringComparison.Ordinal);
        Assert.Contains("object_name", summary, StringComparison.Ordinal);
        Assert.Contains("object_kind", summary, StringComparison.Ordinal);
        Assert.Contains("10 of 104 returned", summary, StringComparison.Ordinal);

        var list = DarlingMcpStoreMetricsTools.ViewNote(DarlingStoreMetricsReader.StoreMetricsView.List, StoreSelfMetrics.BackgroundJobObjectKind, "wait", 2, 2, false, 10);
        Assert.Contains("object_kind 'background_job' and object_name containing 'wait'", list, StringComparison.Ordinal);
        Assert.DoesNotContain("truncated", list, StringComparison.Ordinal);

        var one = DarlingMcpStoreMetricsTools.ViewNote(DarlingStoreMetricsReader.StoreMetricsView.Object, null, "wait_stats", 1, 1, false, 10, seriesPoints: 30);
        Assert.Contains("LAST snapshot, not its maximum", one, StringComparison.Ordinal);
        Assert.DoesNotContain("No point falls inside the window", one, StringComparison.Ordinal);

        /* An object whose newest row predates the window has an EMPTY series, and the note says why rather than
           leaving an empty array to read as an object that stopped growing. */
        var gone = DarlingMcpStoreMetricsTools.ViewNote(DarlingStoreMetricsReader.StoreMetricsView.Object, null, "dropped", 1, 1, false, 10, seriesPoints: 0);
        Assert.Contains("No point falls inside the window", gone, StringComparison.Ordinal);

        Assert.Equal(3, new[] { summary, list, one }.Distinct(StringComparer.Ordinal).Count());

        Assert.Contains("'nope'", DarlingMcpStoreMetricsTools.NoMatchMessage(null, "nope"), StringComparison.Ordinal);
        Assert.Contains("kind 'table'", DarlingMcpStoreMetricsTools.NoMatchMessage(StoreSelfMetrics.TableObjectKind, null), StringComparison.Ordinal);

        Assert.Equal("summary", DarlingMcpStoreMetricsTools.ViewName(DarlingStoreMetricsReader.StoreMetricsView.Summary));
        Assert.Equal("list", DarlingMcpStoreMetricsTools.ViewName(DarlingStoreMetricsReader.StoreMetricsView.List));
        Assert.Equal("object", DarlingMcpStoreMetricsTools.ViewName(DarlingStoreMetricsReader.StoreMetricsView.Object));
    }

    /// <summary>
    /// The description states the summary-first contract and names every parameter that reaches past it,
    /// asserted TOGETHER with the code that makes each sentence true (the sibling pins' reason): the default
    /// limit, the kinds object_kind accepts, and exact-first matching.
    /// </summary>
    [Fact]
    public void TheDescription_StatesTheSummaryFirstContract_AndTheCodeAgrees()
    {
        var method = ToolMethod();
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Matches(@"SUMMARY FIRST: by default[^.]*three ranked lists, each bounded by limit[^.]*truncated", description);
        foreach (var key in new[] { "objects", "fastest_growing", "background_jobs", "delta_since" })
        {
            Assert.Contains(key, description, StringComparison.Ordinal);
        }

        Assert.Contains("object_kind lists every object of one kind", description, StringComparison.Ordinal);
        Assert.Contains("an exact name returns that object's daily series over days_back", description, StringComparison.Ordinal);

        var parameters = method.GetParameters().ToDictionary(p => p.Name!, StringComparer.Ordinal);
        Assert.Equal(DarlingMcpStoreMetricsTools.DefaultLimit, parameters["limit"].DefaultValue);
        Assert.Equal(10, DarlingMcpStoreMetricsTools.DefaultLimit);
        var kindDescription = parameters["object_kind"].GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.All(DarlingStoreMetricsReader.ListedKinds, k => Assert.Contains(k, kindDescription, StringComparison.Ordinal));
        Assert.Contains("exact", parameters["object_name"].GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
    }

    /// <summary>
    /// The old shape, pinned OUT at the source: no projection publishes a daily series per object any more, and
    /// the only series left is the object view's, taken for the one object an exact name resolved to. The live
    /// half measures the same fact as bytes; this half fails the moment someone puts the loop back.
    /// </summary>
    [Fact]
    public void TheTool_NoLongerProjectsADailySeriesForEveryObject()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpStoreMetricsTools.cs"));

        Assert.DoesNotMatch(new Regex(@"\bdaily\s*=\s*daily\b"), code);
        Assert.DoesNotMatch(new Regex(@"\bpoints\s*=\s*g\."), code);
        Assert.Matches(
            new Regex(@"view\s*==\s*DarlingStoreMetricsReader\.StoreMetricsView\.Object\s*\?\s*DarlingStoreMetricsReader\.SeriesFor\(daily,\s*selection\.Matched\[0\]\)"),
            code);
        /* Every list the payload publishes is bounded through the shared page helper. */
        Assert.Equal(3, Regex.Matches(code, @"McpHelpers\.BoundPage\(").Count);
    }

    /// <summary>The web mirror advertises and binds the three new parameters, so /api/read and the triage page
    /// can reach the list and object views; the live half runs the binding.</summary>
    [Fact]
    public void TheWebCatalog_AdvertisesTheNewParameters()
    {
        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_store_metrics"];
        Assert.Equal(new[] { "days_back", "object_kind", "object_name", "limit" }, descriptor.Params.Select(p => p.Name).ToArray());
        Assert.All(descriptor.Params, p => Assert.False(p.Required));
        Assert.Equal(DarlingMcpStoreMetricsTools.DefaultLimit, descriptor.Params.Single(p => p.Name == "limit").Default);
    }

    /// <summary>
    /// The store alerts' triage page renders each section's first row array as its table, and since #3903 the
    /// summary's first array is the largest objects — the jobs sit in a nested list the table never reaches.
    /// So the job-family alerts get the jobs as a section of their own, fleet-level like the rest, bound to the
    /// kind by the same query string the page will send.
    /// </summary>
    [Fact]
    public void TheStoreTriage_ListsTheJobsAsASectionOfTheirOwn()
    {
        foreach (var metric in new[]
                 {
                     DarlingSelfAlertEvaluator.JobCadenceMetric,
                     DarlingSelfAlertEvaluator.PolicyJobFailingMetric,
                     DarlingSelfAlertEvaluator.RefreshJobStuckMetric,
                     DarlingSelfAlertEvaluator.RetentionJobStuckMetric,
                 })
        {
            var sections = DarlingTriageEndpoint.SectionsFor(metric);
            var jobs = Assert.Single(sections, s => s.Read == "get_store_metrics" && s.Params.ContainsKey("object_kind"));
            Assert.Equal(StoreSelfMetrics.BackgroundJobObjectKind, jobs.Params["object_kind"]);
            Assert.True(jobs.FleetLevel);
            Assert.Equal(
                "?days_back=30&object_kind=background_job&limit=25",
                DarlingTriageEndpoint.BuildSectionQuery(jobs, DarlingSelfAlertEvaluator.StoreServerLabel, null));

            var summary = Assert.Single(sections, s => s.Read == "get_store_metrics" && !s.Params.ContainsKey("object_kind"));
            Assert.Equal("30", summary.Params["days_back"]);
        }
    }
}

/// <summary>
/// <c>get_store_metrics</c> against a store seeded to production's SHAPE (#3903): 253 self-metrics objects
/// (72 hypertables, 25 continuous aggregates, the two payload dimensions, the three named tables, both
/// catch-alls, 146 background jobs, plus the store, job_history and checkpointer rows), 43 enabled servers,
/// one sample a day for 400 days. The object count is the point: it is fixed by the schema, not by the fleet,
/// which is why the old payload was ~1.9 MB on every production store whatever its server count.
///
/// <para>Seeded values are arithmetic, so every figure asserted has an oracle: hypertable <c>ht_NN</c> grows
/// (NN + 1) MiB a day, <c>big_grower</c> 200 MiB a day, the static aggregate <c>cagg_static_giant</c> is the
/// largest object and never grows, one job's failure count grows only inside the last ten days, and one runs at
/// 5/6 of its cadence.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test mints its own scratch database
   through ScratchPostgres, seeds collect.store_metrics there and reads only it (plus the server's catalogs), so
   it cannot race the shared store and serializing it would cost suite time for no safety. */
public sealed class StoreMetricsSummaryFirstLivePostgresTests
{
    private const int SeededDays = 400;

    /// <summary>The issue's acceptance: the default response under ~50 KB on the largest production store.</summary>
    private const int DefaultBudgetBytes = 50 * 1024;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The default is the bounded summary, on a store shaped like production. It fails on the pre-#3903 shape
    /// three ways: that response was ~1.3 MB here (253 objects' rows plus thirty daily points each), it had a
    /// daily[] key, and its objects[] had every object. Every block stays on the response, and every list says
    /// what bounded it. days_back=400 stays bounded too: the one part that grows with it is the whole-store
    /// series, one point a day.
    /// </summary>
    [Fact]
    public async Task TheDefault_IsABoundedSummary_OnAStoreShapedLikeProduction_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_store_metrics summary test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await SeedAsync(scratch.ConnectionString, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var json = await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource);
        Assert.True(json.Length < DefaultBudgetBytes, $"the default response is {json.Length:N0} bytes, over the {DefaultBudgetBytes:N0}-byte budget");

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        Assert.Equal("summary", root.GetProperty("view").GetString());
        Assert.False(root.TryGetProperty("daily", out _), "the per-object daily[] series is back in the default response");
        Assert.Equal(JsonValueKind.Null, root.GetProperty("series").ValueKind);

        foreach (var block in new[] { "store", "inventory", "retention", "job_history", "checkpointer" })
        {
            Assert.Equal(JsonValueKind.Object, root.GetProperty(block).ValueKind);
        }

        /* The whole-store growth series and the per-server rate: 30 daily points in the window, 29 deltas. */
        var store = root.GetProperty("store");
        Assert.Equal(43, store.GetProperty("enabled_server_count").GetInt32());
        Assert.Equal(29, store.GetProperty("daily_growth").GetArrayLength());

        /* objects: the ten largest of the 104 byte-bearing objects, largest first, the static giant on top. */
        Assert.Equal(DarlingMcpStoreMetricsTools.SizeOrder, root.GetProperty("order").GetString());
        Assert.Equal(104, root.GetProperty("objects_matched").GetInt32());
        Assert.Equal(10, root.GetProperty("objects_returned").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        var objects = root.GetProperty("objects").EnumerateArray().ToList();
        Assert.Equal(10, objects.Count);
        Assert.Equal("cagg_static_giant", objects[0].GetProperty("object_name").GetString());
        Assert.Equal(0L, objects[0].GetProperty("growth_bytes").GetInt64());
        var sizes = objects.Select(o => o.GetProperty("total_bytes").GetInt64()).ToList();
        Assert.Equal(sizes.OrderByDescending(s => s).ToList(), sizes);
        Assert.All(objects, o => Assert.NotEqual(StoreSelfMetrics.BackgroundJobObjectKind, o.GetProperty("object_kind").GetString()));

        /* fastest_growing: big_grower first, at exactly 29 days x 200 MiB, measured from the window's first day. */
        var growing = root.GetProperty("fastest_growing");
        Assert.Equal(DarlingMcpStoreMetricsTools.GrowthOrder, growing.GetProperty("order").GetString());
        Assert.Equal(10, growing.GetProperty("objects_returned").GetInt32());
        Assert.True(growing.GetProperty("truncated").GetBoolean());
        var fastest = growing.GetProperty("objects")[0];
        Assert.Equal("big_grower", fastest.GetProperty("object_name").GetString());
        Assert.Equal(29L * 200 * Mib, fastest.GetProperty("growth_bytes").GetInt64());

        /* And delta_since is the first point of the very series the drill-down returns, so the delta is the
           subtraction a caller would make from it: first point to last. */
        using (var drill = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: "big_grower")))
        {
            var series = drill.RootElement.GetProperty("series").EnumerateArray().ToList();
            Assert.Equal(series[0].GetProperty("day").GetString(), fastest.GetProperty("delta_since").GetString());
            Assert.Equal(
                series[^1].GetProperty("total_bytes").GetInt64() - series[0].GetProperty("total_bytes").GetInt64(),
                fastest.GetProperty("growth_bytes").GetInt64());
        }

        /* background_jobs: all 146 counted, the one whose failures grew in the window first (9 new failures),
           then the one at 5/6 of its cadence. Job rows carry run telemetry, never byte fields. */
        var jobs = root.GetProperty("background_jobs");
        Assert.Equal(DarlingMcpStoreMetricsTools.JobOrder, jobs.GetProperty("order").GetString());
        Assert.Equal(146, jobs.GetProperty("jobs_matched").GetInt32());
        Assert.Equal(10, jobs.GetProperty("jobs_returned").GetInt32());
        var jobRows = jobs.GetProperty("jobs").EnumerateArray().ToList();
        Assert.Equal("policy_retention big_grower [3000]", jobRows[0].GetProperty("object_name").GetString());
        Assert.Equal(9L, jobRows[0].GetProperty("failures_in_window").GetInt64());
        Assert.Equal("policy_refresh_continuous_aggregate cagg_00 [3001]", jobRows[1].GetProperty("object_name").GetString());
        Assert.Equal(83.3, jobRows[1].GetProperty("duration_vs_cadence_percent").GetDouble());
        Assert.Equal(29L * 24, jobRows[1].GetProperty("runs_in_window").GetInt64());
        Assert.All(jobRows, j => Assert.False(j.TryGetProperty("total_bytes", out _)));

        /* A shorter window is a smaller summary, and the longest one allowed is still bounded: its extra bytes
           are the whole-store series' extra points, nothing per object. */
        var oneDay = await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, days_back: 1);
        Assert.True(oneDay.Length < json.Length);
        var fullRetention = await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, days_back: DarlingMcpStoreMetricsTools.MaxDaysBack);
        using var fullDocument = JsonDocument.Parse(fullRetention);
        Assert.Equal(SeededDays - 1, fullDocument.RootElement.GetProperty("store").GetProperty("daily_growth").GetArrayLength());
        Assert.True(fullRetention.Length < DefaultBudgetBytes + (SeededDays * 100),
            $"days_back={DarlingMcpStoreMetricsTools.MaxDaysBack} is {fullRetention.Length:N0} bytes; only the whole-store series may grow with the window");
    }

    /// <summary>
    /// The filters, end to end: object_kind lists a kind bounded by limit, an exact object_name (any case) is
    /// the object view with its daily series, a partial one lists what contains it, object_kind narrows the
    /// name, and a name that matches nothing (or only a block row) is <c>empty</c>. The refusals are the shared
    /// envelope. Then the SEAMS: the web mirror binds the new query keys into the same tool, and the triage
    /// page's jobs section, built from its own section definition, reaches the jobs list through that binding.
    /// </summary>
    [Fact]
    public async Task TheFilters_ListAKind_DrillIntoOneObject_AndTheWebAndTriageSeamsBindThem_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_store_metrics filter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await SeedAsync(scratch.ConnectionString, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* A kind, bounded: five of the 72 hypertables. */
        using (var hypertables = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_kind: "HyperTable", limit: 5)))
        {
            var root = hypertables.RootElement;
            Assert.Equal("list", root.GetProperty("view").GetString());
            Assert.Equal(StoreSelfMetrics.HypertableObjectKind, root.GetProperty("object_kind").GetString());
            Assert.Equal(72, root.GetProperty("objects_matched").GetInt32());
            Assert.Equal(5, root.GetProperty("objects_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("fastest_growing").ValueKind);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("background_jobs").ValueKind);
        }

        /* Every job, untruncated, in the job order. */
        using (var allJobs = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_kind: StoreSelfMetrics.BackgroundJobObjectKind, limit: 1000)))
        {
            var root = allJobs.RootElement;
            Assert.Equal(DarlingMcpStoreMetricsTools.JobOrder, root.GetProperty("order").GetString());
            Assert.Equal(146, root.GetProperty("objects_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());
            Assert.Equal("policy_retention big_grower [3000]", root.GetProperty("objects")[0].GetProperty("object_name").GetString());
        }

        /* Exact, in another case: the object view, with that object's own 30 points, oldest first, each the
           hypertable's size on that day. */
        using (var one = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: "HT_03")))
        {
            var root = one.RootElement;
            Assert.Equal("object", root.GetProperty("view").GetString());
            Assert.Equal("ht_03", Assert.Single(root.GetProperty("objects").EnumerateArray().ToList()).GetProperty("object_name").GetString());
            var series = root.GetProperty("series").EnumerateArray().ToList();
            Assert.Equal(30, series.Count);
            var days = series.Select(p => p.GetProperty("day").GetString()!).ToList();
            Assert.Equal(days.Order(StringComparer.Ordinal).ToList(), days);
            Assert.Equal(4L * Mib, series[1].GetProperty("total_bytes").GetInt64() - series[0].GetProperty("total_bytes").GetInt64());
            Assert.False(series[0].TryGetProperty("total_runs", out _));
        }

        /* The name that is a substring of its aggregate and its jobs still drills into the hypertable alone. */
        using (var waitStats = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: "wait_stats")))
        {
            Assert.Equal("object", waitStats.RootElement.GetProperty("view").GetString());
            Assert.Equal(StoreSelfMetrics.HypertableObjectKind, waitStats.RootElement.GetProperty("objects")[0].GetProperty("object_kind").GetString());
        }

        /* Partial: everything containing it. Narrowed by kind: only its jobs. */
        using (var partial = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: "wait_stat", limit: 100)))
        {
            Assert.Equal("list", partial.RootElement.GetProperty("view").GetString());
            Assert.Equal(4, partial.RootElement.GetProperty("objects_matched").GetInt32());
        }

        using (var jobsOfWaitStats = JsonDocument.Parse(await DarlingMcpStoreMetricsTools.GetStoreMetrics(
                   dataSource, object_kind: StoreSelfMetrics.BackgroundJobObjectKind, object_name: "wait_stats")))
        {
            var rows = jobsOfWaitStats.RootElement.GetProperty("objects").EnumerateArray().ToList();
            Assert.Equal(2, rows.Count);
            Assert.All(rows, r => Assert.Equal(StoreSelfMetrics.BackgroundJobObjectKind, r.GetProperty("object_kind").GetString()));
        }

        /* Nothing, or only a block row: empty, never an empty list beside a full set of blocks. */
        foreach (var name in new[] { "no_such_object", StoreSelfMetrics.CheckpointerObjectName })
        {
            Assert.Equal("empty", StatusOf(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_name: name)));
        }

        Assert.Equal("empty", StatusOf(
            await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_kind: StoreSelfMetrics.TableObjectKind, object_name: "zzz")));

        /* The refusals: the shared envelope, naming the parameter. */
        Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_kind: StoreSelfMetrics.CheckpointerObjectKind)));
        Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, object_kind: "bogus")));
        Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpStoreMetricsTools.GetStoreMetrics(dataSource, limit: 0)));

        /* The web seam: the dispatch binds object_kind / object_name / limit off the query string. */
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        using (var web = JsonDocument.Parse(await RunReadAsync(dispatch, "?object_kind=background_job&limit=3", dataSource)))
        {
            Assert.Equal("list", web.RootElement.GetProperty("view").GetString());
            Assert.Equal(3, web.RootElement.GetProperty("objects_returned").GetInt32());
        }

        using (var web = JsonDocument.Parse(await RunReadAsync(dispatch, "?object_name=big_grower", dataSource)))
        {
            Assert.Equal("object", web.RootElement.GetProperty("view").GetString());
        }

        /* The triage seam: the job alerts' own section, through its own query builder, into the same dispatch. */
        var section = DarlingTriageEndpoint.SectionsFor(DarlingSelfAlertEvaluator.JobCadenceMetric)
            .Single(s => s.Read == "get_store_metrics" && s.Params.ContainsKey("object_kind"));
        using (var triage = JsonDocument.Parse(await RunReadAsync(
                   dispatch, DarlingTriageEndpoint.BuildSectionQuery(section, DarlingSelfAlertEvaluator.StoreServerLabel, null), dataSource)))
        {
            var root = triage.RootElement;
            Assert.Equal(StoreSelfMetrics.BackgroundJobObjectKind, root.GetProperty("object_kind").GetString());
            Assert.Equal(25, root.GetProperty("objects_returned").GetInt32());
            Assert.Equal("policy_retention big_grower [3000]", root.GetProperty("objects")[0].GetProperty("object_name").GetString());
        }
    }

    private const long Mib = 1L << 20;

    private static string StatusOf(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.GetProperty("status").GetString()!;
    }

    /// <summary>One read through the web mirror's own binding, as the triage page runs it.</summary>
    private static async Task<string> RunReadAsync(
        IReadOnlyDictionary<string, DarlingWebEndpoints.ReadToolHandler> dispatch, string query, NpgsqlDataSource dataSource)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        return await dispatch["get_store_metrics"](context, dataSource, null!);
    }

    /// <summary>
    /// Migrates a scratch store and seeds <see cref="SeededDays"/> daily sweeps of the production-shaped
    /// inventory the class summary describes, one sample a day at five minutes before now, so the 30-day window
    /// holds exactly 30 points per object. The store row's size is the sum of the byte-bearing rows, so the
    /// inventory reconciles like a real sweep's.
    /// </summary>
    private static async Task SeedAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var seed = new NpgsqlCommand($@"
CREATE TEMP TABLE seed_objects
(
    object_kind text NOT NULL,
    object_name text NOT NULL,
    base_bytes bigint,
    bytes_per_day bigint,
    last_run_ms bigint,
    failures_from_day integer
);

INSERT INTO seed_objects
SELECT '{StoreSelfMetrics.HypertableObjectKind}', 'ht_' || lpad(i::text, 2, '0'), (i + 1) * 100 * {Mib}::bigint, (i + 1) * {Mib}::bigint, NULL::bigint, NULL::integer
FROM generate_series(0, 69) AS i
UNION ALL SELECT '{StoreSelfMetrics.HypertableObjectKind}', 'wait_stats', 5 * 1024 * {Mib}::bigint, 10 * {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.HypertableObjectKind}', 'big_grower', 1024 * {Mib}::bigint, 200 * {Mib}::bigint, NULL, NULL
UNION ALL
SELECT '{StoreSelfMetrics.ContinuousAggregateObjectKind}', 'cagg_' || lpad(i::text, 2, '0'), (i + 1) * 50 * {Mib}::bigint, {Mib}::bigint / 2, NULL, NULL
FROM generate_series(0, 22) AS i
UNION ALL SELECT '{StoreSelfMetrics.ContinuousAggregateObjectKind}', 'wait_stats_hourly', 700 * {Mib}::bigint, 2 * {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.ContinuousAggregateObjectKind}', 'cagg_static_giant', 500 * 1024 * {Mib}::bigint, 0, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.DimensionObjectKind}', '{PayloadDimensions.QueryPlanDimTable}', 40 * 1024 * {Mib}::bigint, 50 * {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.DimensionObjectKind}', '{PayloadDimensions.QueryTextDimTable}', 2 * 1024 * {Mib}::bigint, 5 * {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.TableObjectKind}', '{QueryStoreTextStore.TableName}', 15 * 1024 * {Mib}::bigint, 20 * {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.TableObjectKind}', 'collect.query_store_plan_map', 300 * {Mib}::bigint, {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.TableObjectKind}', '{StoreSelfMetrics.AlertLogTable}', 10 * {Mib}::bigint, 0, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.OtherObjectKind}', '{StoreSelfMetrics.OtherObjectName}', 3 * 1024 * {Mib}::bigint, 2 * {Mib}::bigint, NULL, NULL
UNION ALL SELECT '{StoreSelfMetrics.SystemObjectKind}', '{StoreSelfMetrics.SystemObjectName}', 2 * 1024 * {Mib}::bigint, 0, NULL, NULL
UNION ALL
SELECT '{StoreSelfMetrics.BackgroundJobObjectKind}', 'policy_job_' || lpad(i::text, 3, '0') || ' [' || (1000 + i) || ']', NULL, NULL, 1000 + i, NULL
FROM generate_series(0, 141) AS i
UNION ALL SELECT '{StoreSelfMetrics.BackgroundJobObjectKind}', 'policy_compression wait_stats [2000]', NULL, NULL, 5000, NULL
UNION ALL SELECT '{StoreSelfMetrics.BackgroundJobObjectKind}', 'policy_retention wait_stats [2001]', NULL, NULL, 4000, NULL
UNION ALL SELECT '{StoreSelfMetrics.BackgroundJobObjectKind}', 'policy_retention big_grower [3000]', NULL, NULL, 10, {SeededDays - 10}
UNION ALL SELECT '{StoreSelfMetrics.BackgroundJobObjectKind}', 'policy_refresh_continuous_aggregate cagg_00 [3001]', NULL, NULL, 3000000, NULL;

INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, compressed_before_bytes, compressed_after_bytes, chunk_count,
     row_count, enabled_server_count, last_run_duration_ms, schedule_interval_ms, total_runs, total_failures, toast_bytes)
SELECT
    t.metric_time,
    o.object_name,
    o.object_kind,
    o.base_bytes + d.d * o.bytes_per_day,
    NULL, NULL, NULL,
    NULL, NULL,
    o.last_run_ms,
    CASE WHEN o.last_run_ms IS NOT NULL THEN 3600000 END,
    CASE WHEN o.last_run_ms IS NOT NULL THEN 24 * d.d END,
    CASE WHEN o.last_run_ms IS NOT NULL THEN greatest(0, d.d - coalesce(o.failures_from_day, d.d)) END,
    CASE WHEN o.object_kind = '{StoreSelfMetrics.DimensionObjectKind}' THEN (o.base_bytes + d.d * o.bytes_per_day) * 9 / 10 END
FROM generate_series(0, {SeededDays - 1}) AS d(d)
CROSS JOIN LATERAL (SELECT (now() AT TIME ZONE 'UTC') - interval '5 minutes' - make_interval(days => {SeededDays - 1} - d.d) AS metric_time) AS t
CROSS JOIN seed_objects AS o;

INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, enabled_server_count)
SELECT t.metric_time, 'darling', '{StoreSelfMetrics.StoreObjectKind}', sum(o.base_bytes + d.d * o.bytes_per_day), 43
FROM generate_series(0, {SeededDays - 1}) AS d(d)
CROSS JOIN LATERAL (SELECT (now() AT TIME ZONE 'UTC') - interval '5 minutes' - make_interval(days => {SeededDays - 1} - d.d) AS metric_time) AS t
CROSS JOIN seed_objects AS o
WHERE o.base_bytes IS NOT NULL
GROUP BY t.metric_time;

INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, row_count, schedule_interval_ms, total_runs, last_run_duration_ms)
VALUES ((now() AT TIME ZONE 'UTC') - interval '5 minutes', 'darling', '{StoreSelfMetrics.JobHistoryObjectKind}', 48, 86400000, 110, 780000);

INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested)
VALUES ((now() AT TIME ZONE 'UTC') - interval '65 minutes', '{StoreSelfMetrics.CheckpointerObjectName}', '{StoreSelfMetrics.CheckpointerObjectKind}', 1000, 2000, 3),
       ((now() AT TIME ZONE 'UTC') - interval '5 minutes', '{StoreSelfMetrics.CheckpointerObjectName}', '{StoreSelfMetrics.CheckpointerObjectKind}', 1500, 2600, 3);", connection)
        {
            CommandTimeout = 120,
        };
        await seed.ExecuteNonQueryAsync(ct);
    }
}
