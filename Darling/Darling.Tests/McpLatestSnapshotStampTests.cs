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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3541 A10 — <b>latest is a time.</b> A latest-snapshot MCP read (<c>WHERE collection_time = MAX(...)</c>,
/// <c>ORDER BY collection_time DESC LIMIT 1</c>) answers with the newest row a store holds, and until this lane
/// most of them answered with nothing that said WHEN that row was collected: a memory-clerk list, a file-I/O
/// table, a perfmon page, the whole configuration family (captured ON CONNECT, so a "current" setting could be
/// weeks old) — an agent that cannot see the code read every one of them as "now". Two of them accepted
/// <c>hours_back</c> and read only the newest row in it, so a grant storm three hours ago was invisible while
/// the parameter read as a window; one CRITICAL-verdict tool took <c>server_name</c> alone on one SKU and
/// <c>(server_name, hours_back, as_of)</c> on the other; and get_server_summary published ONE clock (the newest
/// collection of ANY collector) beside two figures it did not stamp.
///
/// <para>This file pins the rule and the three honest shapes a latest read may take, on BOTH SKUs, as one
/// dialect: every latest read publishes <c>captured_at</c> (the snapshot's own stamp) and its description says
/// "LATEST IS A TIME" or names its two reads; a tool that takes NO window carries no <c>hours_back</c>; a tool
/// whose <c>hours_back</c> is the span SEARCHED for the newest snapshot says so in those words and publishes
/// <c>age_seconds</c> against its anchor; a tool whose <c>hours_back</c> is READ publishes a <c>window</c> block
/// beside the snapshot. The two SKUs' descriptions of the two aligned tools are pinned byte-equal. Darling's
/// half is reflected off the assembly; Lite's is read from source, as <see cref="McpPageContractTests"/> does.
/// The discriminators are witnessed against literals so a matcher that stops matching cannot report clean.
/// <c>Lite.Tests/McpLatestSnapshotStampTests</c> executes the Lite tools against a real DuckDB;
/// <see cref="McpLatestSnapshotStampLivePostgresTests"/> executes the Darling ones against live Postgres.</para>
/// </summary>
public sealed class McpLatestSnapshotStampTests
{
    /* ───────────────────────── the roster ───────────────────────── */

    /// <summary>The three honest shapes. Named rather than inferred so a tool's disposition is a decision
    /// written down here, and moving one is a visible edit.</summary>
    public enum Shape
    {
        /// <summary>Reads one snapshot, takes no window: <c>captured_at</c> only.</summary>
        Stamped,

        /// <summary><c>hours_back</c> bounds the SEARCH for the newest snapshot (described in those words) and
        /// <c>as_of</c> anchors it: <c>captured_at</c> + <c>age_seconds</c>.</summary>
        SearchBound,

        /// <summary><c>hours_back</c> is READ: the newest snapshot (<c>captured_at</c> + <c>age_seconds</c>)
        /// beside a <c>window</c> aggregate over every snapshot in it.</summary>
        Windowed,
    }

    /// <summary>Every latest-snapshot tool this lane stamped, by SKU file, with its shape. The Lite file and
    /// name are given per row because Lite hosts the same tool names in differently-named files.</summary>
    public static readonly (Type Tools, string ToolName, string LiteFile, Shape Shape)[] LatestTools =
    [
        (typeof(DarlingMcpDataTools), "get_memory_stats", "Lite/Mcp/McpMemoryTools.cs", Shape.Stamped),
        (typeof(DarlingMcpDataTools), "get_memory_clerks", "Lite/Mcp/McpMemoryTools.cs", Shape.Stamped),
        (typeof(DarlingMcpDataTools), "get_file_io_stats", "Lite/Mcp/McpIoTools.cs", Shape.Stamped),
        (typeof(DarlingMcpDataTools), "get_perfmon_stats", "Lite/Mcp/McpPerfmonTools.cs", Shape.Stamped),
        (typeof(DarlingMcpConfigTools), "get_server_config", "Lite/Mcp/McpConfigTools.cs", Shape.Stamped),
        (typeof(DarlingMcpConfigTools), "get_database_config", "Lite/Mcp/McpConfigTools.cs", Shape.Stamped),
        (typeof(DarlingMcpConfigTools), "get_trace_flags", "Lite/Mcp/McpConfigTools.cs", Shape.Stamped),
        (typeof(DarlingMcpConfigHistoryTools), "get_database_scoped_config", "Lite/Mcp/McpConfigTools.cs", Shape.Stamped),
        (typeof(DarlingMcpConfigHistoryTools), "get_query_store_health", "Lite/Mcp/McpConfigTools.cs", Shape.Stamped),
        (typeof(DarlingMcpPlanCacheSchedulerTools), "get_plan_cache_bloat", "Lite/Mcp/McpPlanCacheSchedulerTools.cs", Shape.SearchBound),
        (typeof(DarlingMcpPlanCacheSchedulerTools), "get_cpu_scheduler_pressure", "Lite/Mcp/McpPlanCacheSchedulerTools.cs", Shape.SearchBound),
        (typeof(DarlingMcpMemoryGrantTools), "get_resource_semaphore", "Lite/Mcp/McpMemoryTools.cs", Shape.Windowed),
        (typeof(DarlingMcpMemoryGrantTools), "get_memory_grants", "Lite/Mcp/McpMemoryTools.cs", Shape.Windowed),
        /* #3653: the four latest reads that stamped themselves BEFORE #3637's vocabulary, under the top-level
           key `collection_time`. The stamp was always true; only the spelling predated the census, and this
           file carried them as a named allowance (`StampedUnderCollectionTime`) because the Darling web surface
           read one of them (get_session_stats, server-tabs.js' SESSION_STATS "Collected" tile) by the old key
           and that file sat outside #3637's boundary. The tile moved with the rename, all four publish
           `captured_at` on both SKUs, and the allowance is gone - a roster row each, held to the Stamped
           dialect like every other latest read. */
        (typeof(DarlingMcpObjectStatsTools), "get_database_sizes", "Lite/Mcp/McpServerInfoTools.cs", Shape.Stamped),
        (typeof(DarlingMcpJobTools), "get_running_jobs", "Lite/Mcp/McpJobTools.cs", Shape.Stamped),
        (typeof(DarlingMcpDataTools), "get_server_properties", "Lite/Mcp/McpServerInfoTools.cs", Shape.Stamped),
        (typeof(DarlingMcpSessionTools), "get_session_stats", "Lite/Mcp/McpSessionTools.cs", Shape.Stamped),
        /* #3880: the first of the three object-stats residuals below to leave that list. Erik's ruling on the
           call #3878/#3879 recorded: #3879 replaced this read's per-database_name MAX groups with the
           server's newest capture, which made it one honest instant for the first time, and a read that
           resolves one instant can say which instant — so it is stamped rather than rostered as debt on
           McpPayloadContractCensusTests' shrink-only list. Stamped on BOTH SKUs in one lane (the twin is
           #3877's), so this is a pair row like every other, not an asymmetry. */
        (typeof(DarlingMcpObjectStatsTools), "get_object_locking", "Lite/Mcp/McpObjectStatsTools.cs", Shape.Stamped),
    ];

    /// <summary>
    /// The latest reads this lane did NOT reach: the object-stats family reads the latest DAILY snapshot and
    /// publishes no stamp at all on either SKU. Named here so the census fails the day one of them gains
    /// <c>captured_at</c> without leaving this list, and so the gap is on the record rather than invisible.
    /// Reported to #3541 as the A10 residual.
    ///
    /// <para><b>#3880 took the first of the three</b>, and the route it took is the route the other two have.
    /// This comment used to carry a parenthetical — <c>DarlingObjectStatsReader.IndexLockingSql</c> "takes MAX
    /// per database, not one instant" — that was true when written and was itself the defect #3878 removed: a
    /// per-<c>database_name</c> MAX group is not one instant, which is precisely why the read kept returning
    /// databases renamed away a month earlier (#3876). All four Darling locking reads now anchor on
    /// <c>(SELECT MAX(collection_time) … WHERE server_id = $1)</c>, making the read one instant like the rest
    /// of the family — exactly the property a <c>captured_at</c> stamp needs to be truthful about. #3879 left
    /// it here anyway, unstamped, which is all this list ever claimed; Erik ruled the other way in #3880, the
    /// SQL projects its anchor column, <c>get_object_locking</c> publishes <c>captured_at</c> on both SKUs,
    /// and the tool is a <see cref="LatestTools"/> row above.</para>
    ///
    /// <para>The two that remain are an ordinary A10 residual each, not a different rule:
    /// <c>get_index_usage</c> and <c>get_table_index_sizes</c> read the same daily snapshots through anchors
    /// of the same shape, and the same one-line projection would stamp them. They stay because nobody has
    /// measured what their callers read, not because the pattern does not reach them.</para>
    /// </summary>
    public static readonly string[] UnstampedLatestReadsPendingA10 =
    [
        "get_index_usage", "get_table_index_sizes",
    ];

    /// <summary>
    /// Tools the reader-call sweep sees because they call a <c>*Latest*Async</c> reader as an INPUT to a read
    /// that is not itself a latest snapshot: the two CPU rankings read the newest server_properties row for the
    /// core count their attribution divides by; get_plan_corrections reads the newest automatic-tuning settings
    /// beside its paged correction history; get_sweep_reports reads the newest fleet sweep, a worklist rather than
    /// a per-server snapshot; get_pvs_stats mixes a latest per-database snapshot (stamped per row) with a trend,
    /// and is the named exclusion <see cref="AsOfWindowAnchorTests"/> carries for that reason. None of them is a
    /// latest-snapshot tool, and none gets a <c>captured_at</c> here — the control below fails if one gains it,
    /// so the decision is revisited rather than drifted into.
    /// </summary>
    public static readonly string[] LatestLookupInsideAnotherRead =
    [
        "get_plan_corrections", "get_pvs_stats", "get_sweep_reports", "get_top_procedures_by_cpu", "get_top_queries_by_cpu",
    ];

    /// <summary>get_server_summary carries THREE clocks by name (<c>cpu_captured_at</c>, <c>memory_captured_at</c>,
    /// <c>last_collection</c>) rather than one <c>captured_at</c>, because its two latest figures come from two
    /// collectors that can be arbitrarily far apart. <see cref="ServerSummary_NamesItsThreeClocks_OnBothSkus"/>
    /// holds the shape; the sweep only needs to know it is accounted for.</summary>
    public static readonly string[] ThreeClockTools = ["get_server_summary"];

    /// <summary>
    /// Stamped latest reads that exist on ONE SKU. <see cref="LatestTools"/> is a roster of PAIRS — every entry
    /// names the Lite file its twin lives in and <see cref="LatestToolBodies"/> reads both — so a Darling-only
    /// tool cannot sit in it without a Lite file to point at. These are held to the Stamped dialect by
    /// <see cref="DarlingOnlyStampedTools_KeepTheStampedDialect"/> below with the SAME three assertions the
    /// roster's Stamped entries get (a <c>captured_at</c> on the payload, LATEST IS A TIME in the description,
    /// neither knob in the signature) — only the Lite half is absent, because the SKU is.
    ///
    /// <para><c>get_pg_logging_audit</c> (#3607) judges seven logging GUCs from the newest
    /// <c>pg_server_config</c> snapshot; Lite has no PostgreSQL target, so there is no twin for the roster to
    /// pair it with. The same architectural boundary <c>CrossAppMcpToolInventoryPinTests</c> records for every
    /// <c>get_pg_*</c> read.</para>
    ///
    /// <para><c>get_pg_server_config</c> (#3653, the #3541 A10 residual) reads the SAME newest snapshot through
    /// <c>DarlingPgServerConfigReader.GetCurrentConfigAsync</c>. It sat OUTSIDE this census because
    /// <see cref="LatestReaderCall"/> did not match a <c>GetCurrent*Async</c> reader — the gap the sibling lane
    /// that added this list recorded rather than closed. The regex now names the family and the tool carries
    /// the row's own <c>collection_time</c> as <c>captured_at</c>; <c>ConfigChangesSql</c>'s caller
    /// (<c>get_pg_server_config_changes</c>) is a windowed history read, not a latest read, and the pattern
    /// does not reach it.</para>
    /// </summary>
    public static readonly (Type Tools, string ToolName)[] DarlingOnlyStamped =
    [
        (typeof(DarlingMcpPgLoggingAuditTools), "get_pg_logging_audit"),
        (typeof(DarlingMcpPgServerStateTools), "get_pg_server_config"),
    ];

    /* ───────────────────────── the discriminators ───────────────────────── */

    /// <summary>The stamp, as a payload key.</summary>
    private static readonly Regex CapturedAtKey = new(@"\bcaptured_at\s*=", RegexOptions.Compiled);

    /// <summary>The anchored distance.</summary>
    private static readonly Regex AgeSecondsKey = new(@"\bage_seconds\s*=", RegexOptions.Compiled);

    /// <summary>The window half of a Windowed tool.</summary>
    private static readonly Regex WindowKey = new(@"\bwindow\s*=\s*window\.Select\(", RegexOptions.Compiled);

    /// <summary>
    /// The retired spelling, as a TOP-LEVEL key of the object the tool serializes. Until #3653 an
    /// indentation-keyed form of this (<c>\n</c> + sixteen spaces + <c>collection_time =</c>) was the
    /// discriminator behind a <c>StampedUnderCollectionTime</c> allowance, applied only to the four tools it
    /// named; it stays as a NEGATIVE sweep over every latest-reading Darling body, so a new latest read cannot
    /// revive the old key and a renamed one cannot keep both. Anchored on <c>JsonSerializer.Serialize(new {</c>
    /// rather than on indentation because a per-row <c>collection_time</c> inside a
    /// <c>rows.Select(r =&gt; new { ... })</c> is a series column, not the snapshot's stamp, and
    /// <c>get_memory_grants</c>' rows sit at the same sixteen-space indent the old form keyed on. One level
    /// of nested object (<c>summary = new { ... }</c>, a row projection) is stepped over as a unit; a
    /// top-level key BELOW a doubly-nested object is out of the regex's reach, which is a silent miss and not
    /// a false alarm — the roster's positive <c>captured_at</c> assertion is the contract, this is the fence.
    /// </summary>
    private static readonly Regex TopLevelCollectionTimeKey = new(
        @"JsonSerializer\.Serialize\(new\s*\{(?:[^{}]|\{[^{}]*\})*?\bcollection_time\s*=", RegexOptions.Compiled);

    /// <summary>The words a SearchBound tool must use for <c>hours_back</c>.</summary>
    private const string SearchBoundWords = "search for the latest snapshot";

    /// <summary>The words a Windowed tool must use for <c>hours_back</c>.</summary>
    private const string WindowedWords = "window[] aggregates every snapshot in these hours";

    /* ───────────────────────── both SKUs, from source ───────────────────────── */

    [Fact]
    public void EveryLatestSnapshotTool_PublishesCapturedAt_OnBothSkus()
    {
        foreach (var (label, body, _) in LatestToolBodies())
        {
            Assert.True(CapturedAtKey.IsMatch(Strip(body)),
                $"{label}: no `captured_at =` on the payload — a latest read that never says when it was captured");
        }
    }

    [Fact]
    public void EveryLatestSnapshotTool_SaysLatestIsATime_InItsDescription_OnBothSkus()
    {
        foreach (var (label, body, shape) in LatestToolBodies())
        {
            var description = DescriptionOf(body, label);
            Assert.Contains("captured_at", description, StringComparison.Ordinal);
            if (shape == Shape.Windowed)
            {
                Assert.Contains("TWO READS UNDER ONE WINDOW", description, StringComparison.Ordinal);
                Assert.Contains("window[]", description, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains("LATEST IS A TIME", description, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>A parameter that does nothing is a lie: a Stamped tool takes neither knob.</summary>
    [Fact]
    public void StampedTools_TakeNoWindowAndNoAnchor_OnBothSkus()
    {
        foreach (var (type, name, liteFile, shape) in LatestTools.Where(t => t.Shape == Shape.Stamped))
        {
            var darling = ToolMethod(type, name).GetParameters().Select(p => p.Name).ToArray();
            Assert.DoesNotContain("hours_back", darling);
            Assert.DoesNotContain("as_of", darling);

            var lite = LiteParamNames(liteFile, name);
            Assert.DoesNotContain("hours_back", lite);
            Assert.DoesNotContain("as_of", lite);
        }
    }

    /// <summary>
    /// The Stamped dialect, held on the Darling-only reads with the roster's own three assertions — the stamp
    /// on the payload, LATEST IS A TIME in the description, neither knob in the signature. Read from the
    /// Darling source only, because there is no Lite half to read.
    /// </summary>
    [Fact]
    public void DarlingOnlyStampedTools_KeepTheStampedDialect()
    {
        Assert.NotEmpty(DarlingOnlyStamped);

        foreach (var (type, name) in DarlingOnlyStamped)
        {
            var body = ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), name);
            Assert.True(CapturedAtKey.IsMatch(Strip(body)),
                $"Darling {name}: no `captured_at =` on the payload — a latest read that never says when it was captured");

            var description = ToolMethod(type, name).GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.Contains("captured_at", description, StringComparison.Ordinal);
            Assert.Contains("LATEST IS A TIME", description, StringComparison.Ordinal);

            var parameters = ToolMethod(type, name).GetParameters().Select(p => p.Name).ToArray();
            Assert.DoesNotContain("hours_back", parameters);
            Assert.DoesNotContain("as_of", parameters);
        }
    }

    /// <summary>
    /// A SearchBound tool's <c>hours_back</c> says it is the SEARCH span in the same words on both SKUs, the
    /// tool takes the anchor, and the payload carries the anchored distance — the three things that make a
    /// latest read with a window parameter honest.
    /// </summary>
    [Fact]
    public void SearchBoundTools_DescribeHoursBackAsTheSearchSpan_AndPublishAge_OnBothSkus()
    {
        foreach (var (type, name, liteFile, _) in LatestTools.Where(t => t.Shape == Shape.SearchBound))
        {
            var method = ToolMethod(type, name);
            var hours = method.GetParameters().Single(p => p.Name == "hours_back");
            Assert.Contains(SearchBoundWords, hours.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
            Assert.Contains("as_of", method.GetParameters().Select(p => p.Name));
            Assert.Matches(AgeSecondsKey, Strip(ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), name)));

            var liteBody = ToolBody(ReadRepoFileLf(liteFile.Split('/')), name);
            Assert.Contains(SearchBoundWords, liteBody, StringComparison.Ordinal);
            Assert.Contains("as_of", LiteParamNames(liteFile, name));
            Assert.Matches(AgeSecondsKey, Strip(liteBody));
        }
    }

    /// <summary>A Windowed tool reads its window: the payload carries the <c>window</c> block, the anchored
    /// distance, and the window's own bounds, and <c>hours_back</c> says which half is which.</summary>
    [Fact]
    public void WindowedTools_PublishTheWindowBesideTheSnapshot_OnBothSkus()
    {
        foreach (var (type, name, liteFile, _) in LatestTools.Where(t => t.Shape == Shape.Windowed))
        {
            var hours = ToolMethod(type, name).GetParameters().Single(p => p.Name == "hours_back");
            Assert.Contains(WindowedWords, hours.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);

            foreach (var body in new[] { ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), name), ToolBody(ReadRepoFileLf(liteFile.Split('/')), name) })
            {
                var text = Strip(body);
                Assert.Matches(WindowKey, text);
                Assert.Matches(AgeSecondsKey, text);
                Assert.Contains("window_start =", text, StringComparison.Ordinal);
                Assert.Contains("window_end =", text, StringComparison.Ordinal);
                Assert.Contains(WindowedWords, body, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>The same tool name takes the same parameters on both SKUs — the drift this lane closed on
    /// get_cpu_scheduler_pressure, pinned for every tool in the roster so it cannot reopen on another.</summary>
    [Fact]
    public void TheSameToolName_TakesTheSameParameters_OnBothSkus()
    {
        foreach (var (type, name, liteFile, _) in LatestTools)
        {
            var darling = ToolMethod(type, name).GetParameters()
                .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
                .Select(p => p.Name!)
                .ToArray();
            Assert.Equal(darling, LiteParamNames(liteFile, name));
        }
    }

    /// <summary>
    /// The two tools whose descriptions were rewritten on both SKUs are pinned byte-equal: a shared const
    /// cannot cross the two assemblies, so the census holds the two literals together instead. Darling's is
    /// reflected; Lite's is the const its attribute names, read from source.
    /// </summary>
    [Theory]
    [InlineData(typeof(DarlingMcpPlanCacheSchedulerTools), "get_cpu_scheduler_pressure", "Lite/Mcp/McpPlanCacheSchedulerTools.cs", "CpuSchedulerPressureDescription")]
    [InlineData(typeof(DarlingMcpHealthTools), "get_server_summary", "Lite/Mcp/McpHealthTools.cs", "ServerSummaryDescription")]
    public void TheAlignedTools_AreDescribedIdentically_OnBothSkus(Type type, string toolName, string liteFile, string liteConst)
    {
        var darling = ToolMethod(type, toolName).GetCustomAttribute<DescriptionAttribute>()!.Description;
        var lite = LiteConstLiteral(liteFile, liteConst);
        Assert.Equal(darling, lite);

        /* And the attribute really does name the const — a literal pasted beside an unused const would pass
           the equality above while drifting the day either is edited. */
        var liteBody = ToolBody(ReadRepoFileLf(liteFile.Split('/')), toolName);
        Assert.Contains($"Description({liteConst})", liteBody, StringComparison.Ordinal);
    }

    /// <summary>The verdict Darling always published, now on Lite too: the same tool name answers with a
    /// banded pressure_level on both SKUs, from the SHARED classifier.</summary>
    [Fact]
    public void CpuSchedulerPressure_PublishesTheVerdict_OnBothSkus()
    {
        var lite = Strip(ToolBody(ReadRepoFileLf("Lite", "Mcp", "McpPlanCacheSchedulerTools.cs"), "get_cpu_scheduler_pressure"));
        Assert.Contains("CpuSchedulerMetrics.ClassifyCpuPressure(", lite, StringComparison.Ordinal);
        Assert.Contains("pressure_level = pressure.Level", lite, StringComparison.Ordinal);
        Assert.Contains("recommendation = pressure.Recommendation", lite, StringComparison.Ordinal);

        var darling = Strip(ToolBody(ReadRepoFileLf(DarlingFileOf(typeof(DarlingMcpPlanCacheSchedulerTools)).Split('/')), "get_cpu_scheduler_pressure"));
        Assert.Contains("pressure_level = pressureLevel", darling, StringComparison.Ordinal);
    }

    /// <summary>get_server_summary names its three clocks on both SKUs, and no longer publishes two figures
    /// under one stamp.</summary>
    [Fact]
    public void ServerSummary_NamesItsThreeClocks_OnBothSkus()
    {
        foreach (var body in new[]
                 {
                     ToolBody(ReadRepoFileLf(DarlingFileOf(typeof(DarlingMcpHealthTools)).Split('/')), "get_server_summary"),
                     ToolBody(ReadRepoFileLf("Lite", "Mcp", "McpHealthTools.cs"), "get_server_summary"),
                 })
        {
            var text = Strip(body);
            Assert.Contains("cpu_captured_at =", text, StringComparison.Ordinal);
            Assert.Contains("memory_captured_at =", text, StringComparison.Ordinal);
            Assert.Contains("counts_window_hours =", text, StringComparison.Ordinal);
            Assert.Contains("last_collection =", text, StringComparison.Ordinal);
        }
    }

    /// <summary>The latch band names the interval it came from, beside the window totals it did not.</summary>
    [Fact]
    public void LatchSeverity_NamesTheIntervalItWasBandedFrom()
    {
        var body = Strip(ToolBody(ReadRepoFileLf(DarlingFileOf(typeof(DarlingMcpLatchSpinlockTools)).Split('/')), "get_latch_stats"));
        Assert.Contains("severity_banded_from = new", body, StringComparison.Ordinal);
        /* #3653 A16: the delta the band came from is published only when the interval it accrued over was
           knowable — the marker's stored 0 is not a measurement (#3642), and neither is a band from it. */
        Assert.Contains("delta_wait_time_ms = r.LatestIntervalSeconds is null ? (long?)null : r.LatestDeltaWaitTimeMs", body, StringComparison.Ordinal);
        Assert.Contains("interval_seconds =", body, StringComparison.Ordinal);
        Assert.Contains("captured_at = r.LatestCollectionTime", body, StringComparison.Ordinal);

        var sql = DarlingLatchSpinlockReader.LatchStatsTopNSql;
        Assert.Contains("AS latest_interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("l.latest_interval_seconds", sql, StringComparison.Ordinal);

        var description = ToolMethod(typeof(DarlingMcpLatchSpinlockTools), "get_latch_stats").GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("severity_banded_from", description, StringComparison.Ordinal);
        Assert.Contains("LATEST interval only", description, StringComparison.Ordinal);
    }

    /* ───────────────────────── the sweep: no latest read escapes the roster ───────────────────────── */

    /// <summary>
    /// Every Darling tool whose body calls a <c>*Latest*Async</c> / <c>*Snapshot*Async</c> / <c>Current</c>-family
    /// reader, or whose reader const is a latest-snapshot read, is in the roster or a named residual — and
    /// every allowance entry is still needed. A new latest read must pick a shape here rather than ship
    /// unstamped. Since #3653 no tool body anywhere publishes the retired top-level <c>collection_time</c>
    /// stamp: the <c>StampedUnderCollectionTime</c> allowance that held four pre-vocabulary reads is gone,
    /// and the discriminator that used to admit them now refuses everyone.
    /// </summary>
    [Fact]
    public void EveryLatestReadTool_IsInTheRoster_OrANamedAllowance_AndEveryAllowanceIsStillNeeded()
    {
        var rostered = LatestTools.Select(t => t.ToolName).ToHashSet(StringComparer.Ordinal);
        var residualsSeen = new HashSet<string>(StringComparer.Ordinal);
        var lookupsSeen = new HashSet<string>(StringComparer.Ordinal);
        var threeClocksSeen = new HashSet<string>(StringComparer.Ordinal);
        var darlingOnlySeen = new HashSet<string>(StringComparer.Ordinal);
        var examined = 0;

        foreach (var (file, source) in AllDarlingToolSources())
        {
            var marks = Regex.Matches(source, @"\[McpServerTool\(Name = ""([a-z_0-9]+)""");
            for (var i = 0; i < marks.Count; i++)
            {
                var end = i + 1 < marks.Count ? marks[i + 1].Index : source.Length;
                var body = Strip(source[marks[i].Index..end]);
                var toolName = marks[i].Groups[1].Value;

                if (!LatestReaderCall.IsMatch(body))
                {
                    continue;
                }

                examined++;
                /* The retired spelling is refused on EVERY latest-reading body, rostered or not: a rostered
                   tool that kept collection_time beside captured_at would be publishing one instant under
                   two names, which is the vocabulary drift #3637 closed. */
                Assert.False(TopLevelCollectionTimeKey.IsMatch(body),
                    $"{file} {toolName}: publishes a top-level collection_time stamp — the census's spelling is captured_at (#3637; the last four holdouts moved in #3653)");

                if (rostered.Contains(toolName))
                {
                    continue;
                }

                if (UnstampedLatestReadsPendingA10.Contains(toolName, StringComparer.Ordinal))
                {
                    Assert.False(CapturedAtKey.IsMatch(body),
                        $"{file} {toolName}: now publishes captured_at — move it into the roster and out of UnstampedLatestReadsPendingA10");
                    residualsSeen.Add(toolName);
                    continue;
                }

                if (LatestLookupInsideAnotherRead.Contains(toolName, StringComparer.Ordinal))
                {
                    Assert.False(CapturedAtKey.IsMatch(body),
                        $"{file} {toolName}: now publishes captured_at — it has become a latest-snapshot tool; give it a Shape and remove it from LatestLookupInsideAnotherRead");
                    lookupsSeen.Add(toolName);
                    continue;
                }

                if (ThreeClockTools.Contains(toolName, StringComparer.Ordinal))
                {
                    Assert.Contains("cpu_captured_at =", body, StringComparison.Ordinal);
                    Assert.Contains("memory_captured_at =", body, StringComparison.Ordinal);
                    threeClocksSeen.Add(toolName);
                    continue;
                }

                if (DarlingOnlyStamped.Any(t => t.ToolName == toolName))
                {
                    Assert.True(CapturedAtKey.IsMatch(body),
                        $"{file} {toolName}: listed as a Darling-only STAMPED read but publishes no captured_at");
                    darlingOnlySeen.Add(toolName);
                    continue;
                }

                Assert.Fail($"{file} {toolName}: calls a latest-snapshot reader and is in no list here — give it a Shape in LatestTools (and stamp it) or name it as an allowance with its reason");
            }
        }

        /* Population controls: a sweep that matched nothing passes for free, and an allowance nobody needs is a
           widened exemption. 26 latest-reading tool bodies at the time of writing. */
        Assert.True(examined >= 24, $"only {examined} latest-reading tool bodies were found; the reader-call pattern has stopped matching");
        Assert.True(LatestLookupInsideAnotherRead.ToHashSet(StringComparer.Ordinal).SetEquals(lookupsSeen),
            "LatestLookupInsideAnotherRead no longer matches what the sweep finds: " + string.Join(", ", LatestLookupInsideAnotherRead.Except(lookupsSeen)));
        Assert.True(ThreeClockTools.ToHashSet(StringComparer.Ordinal).SetEquals(threeClocksSeen),
            "ThreeClockTools no longer matches what the sweep finds: " + string.Join(", ", ThreeClockTools.Except(threeClocksSeen)));
        Assert.True(DarlingOnlyStamped.Select(t => t.ToolName).ToHashSet(StringComparer.Ordinal).SetEquals(darlingOnlySeen),
            "DarlingOnlyStamped no longer matches what the sweep finds: " + string.Join(", ", DarlingOnlyStamped.Select(t => t.ToolName).Except(darlingOnlySeen)));
        Assert.True(UnstampedLatestReadsPendingA10.ToHashSet(StringComparer.Ordinal).SetEquals(residualsSeen),
            "UnstampedLatestReadsPendingA10 no longer matches what the sweep finds: " + string.Join(", ", UnstampedLatestReadsPendingA10.Except(residualsSeen)));
    }

    /// <summary>
    /// A tool body's call into a latest-snapshot reader. The readers name themselves: <c>GetLatest*Async</c>,
    /// <c>*LatestAsync</c>, <c>*SnapshotAsync</c>, the plan-cache / scheduler pair, the server summary, and the
    /// object-stats trio (<c>GetIndexUsageAsync</c> / <c>GetIndexLockingAsync</c> / <c>GetObjectSizeGrowthAsync</c>,
    /// each keyed on a correlated <c>MAX(collection_time)</c>), and since #3653 the <c>GetCurrent*Async</c>
    /// family (<c>DarlingPgServerConfigReader.GetCurrentConfigAsync</c>, anchored on <c>MAX(collection_time)</c>
    /// for the server) — a "current" read IS a latest read, and the name had kept it out of the sweep.
    /// </summary>
    private static readonly Regex LatestReaderCall = new(
        @"\.(GetLatest\w+Async|Get\w+LatestAsync|Get\w+SnapshotAsync|GetCurrent\w+Async|GetPlanCacheBloatAsync|GetCpuSchedulerPressureAsync|GetServerSummaryAsync|GetIndexUsageAsync|GetIndexLockingAsync|GetObjectSizeGrowthAsync|GetRunningJobsAsync)\(",
        RegexOptions.Compiled);

    /* ───────────────────────── the readers ───────────────────────── */

    /// <summary>Every stamped Darling read carries its stamp column ON THE ROW STATEMENT — never a second
    /// <c>MAX()</c> read that could stamp the next capture.</summary>
    [Theory]
    [InlineData(nameof(DarlingDataReader.LatestMemoryClerksSql), "collection_time")]
    [InlineData(nameof(DarlingDataReader.LatestFileIoStatsSql), "collection_time")]
    [InlineData(nameof(DarlingDataReader.LatestPerfmonStatsSql), "collection_time")]
    [InlineData(nameof(DarlingCurrentConfigReader.ServerConfigSql), "capture_time")]
    [InlineData(nameof(DarlingCurrentConfigReader.DatabaseConfigSql), "capture_time")]
    [InlineData(nameof(DarlingCurrentConfigReader.TraceFlagsSql), "capture_time")]
    [InlineData(nameof(DarlingConfigHistoryReader.DatabaseScopedConfigSql), "capture_time")]
    [InlineData(nameof(DarlingConfigHistoryReader.QueryStoreHealthSql), "capture_time")]
    [InlineData(nameof(DarlingPgLoggingAuditReader.NewestSnapshotSql), "collection_time")]
    [InlineData(nameof(DarlingPgServerConfigReader.CurrentConfigSql), "collection_time")]
    /* #3653: the four reads behind the retired StampedUnderCollectionTime allowance, now roster rows. */
    [InlineData(nameof(DarlingObjectStatsReader.DatabaseSizeLatestSql), "collection_time")]
    [InlineData(nameof(DarlingJobReader.RunningJobsSql), "collection_time")]
    [InlineData(nameof(DarlingDataReader.LatestServerPropertiesSql), "collection_time")]
    [InlineData(nameof(DarlingSessionReader.LatestSessionStatsSql), "collection_time")]
    /* #3880: the object-locking read, stamped by Erik's ruling rather than rostered as unstamped debt. It is
       held to the SAME rule as every other stamped read, and the rule is the whole reason the stamp is worth
       having here: the anchor column rides the row statement, so the instant published is the instant the
       returned rows came from, never a fresher capture a second MAX() read happened to see. */
    [InlineData(nameof(DarlingObjectStatsReader.IndexLockingSql), "collection_time")]
    public void EveryStampedRead_SelectsItsStampColumn_OnTheRowStatement(string sqlName, string column)
    {
        var sql = ReaderSql(sqlName);
        /* The MAIN statement's select list: from its SELECT to its FROM, both at column zero once the raw
           string is dedented. `RunningJobsSql` opens with a CTE whose own FROM is indented, and a slice to
           the first FROM anywhere would stop inside the CTE and read the row statement's stamp as missing. */
        var from = sql.IndexOf("\nFROM ", StringComparison.Ordinal);
        Assert.True(from > 0, $"{sqlName}: no column-zero FROM — the main statement's shape has changed; re-anchor this pin");
        var select = sql.LastIndexOf("\nSELECT", from, StringComparison.Ordinal);
        Assert.Contains(column, sql[(select < 0 ? 0 : select)..from], StringComparison.Ordinal);
    }

    /// <summary>The scheduler read is bounded the way Lite's is — the newest row IN THE WINDOW, so a week-old
    /// snapshot from a dead collector is <c>unavailable</c> rather than served as current.</summary>
    [Fact]
    public void CpuSchedulerRead_IsBoundedByTheWindow()
    {
        var sql = DarlingPlanCacheSchedulerReader.CpuSchedulerPressureSql;
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    /// <summary>The two window reads: the peak's instant from a <c>DISTINCT ON</c> over the SAME windowed rows,
    /// the deltas SUMmed (no interval arithmetic — the naked-family rung is a rate question), and no literal cap.</summary>
    [Theory]
    [InlineData(nameof(DarlingMemoryGrantReader.ResourceSemaphoreWindowSql))]
    [InlineData(nameof(DarlingMemoryGrantReader.MemoryGrantsWindowSql))]
    public void MemoryGrantWindowReads_AggregateEverySnapshot_AndNameThePeaksInstant(string sqlName)
    {
        var sql = ReaderSql(sqlName);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS snapshots_in_window", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(waiter_count) AS bigint) AS peak_waiter_count", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(timeout_error_count_delta) AS bigint) AS timeout_errors_in_window", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(forced_grant_count_delta) AS bigint) AS forced_grants_in_window", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT ON", sql, StringComparison.Ordinal);
        Assert.Contains("waiter_count DESC, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bLIMIT\b", sql);
        Assert.DoesNotContain("sample_interval_seconds", sql, StringComparison.Ordinal);
    }

    /// <summary>The web catalogue advertises the two knobs the aligned scheduler tool now takes, and the
    /// dispatch forwards them (<see cref="AsOfWindowAnchorTests"/> holds the general rule; this names the tool).</summary>
    [Fact]
    public void CpuSchedulerPressure_AdvertisesAndForwardsItsWindow_OnTheWebSurface()
    {
        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_cpu_scheduler_pressure"];
        Assert.Contains("hours", descriptor.Params.Select(p => p.Name));
        Assert.Contains("as_of", descriptor.Params.Select(p => p.Name));

        var source = Strip(ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"));
        Assert.Matches(@"\[""get_cpu_scheduler_pressure""\] = \(c, pg, an\) => DarlingMcpPlanCacheSchedulerTools\.GetCpuSchedulerPressure\(pg, Server\(c\), Hours\(c, 24\), as_of: AsOf\(c\), cancellationToken: c\.RequestAborted\)", source);
    }

    /* ───────────────────────── the pure pieces, executed ───────────────────────── */

    [Fact]
    public void LatestSnapshot_RefusesRowsWithoutAStamp_AndIsEmptyWithoutRows()
    {
        Assert.Throws<ArgumentException>(() => new LatestSnapshot<int>(null, new List<int> { 1 }));

        var empty = LatestSnapshot<int>.Empty;
        Assert.True(empty.IsEmpty);
        Assert.Null(empty.CapturedAt);
        Assert.Equal(0, empty.Count);

        var stamped = new LatestSnapshot<int>(new DateTime(2026, 9, 18, 12, 0, 0), new List<int> { 1, 2 });
        Assert.False(stamped.IsEmpty);
        Assert.Equal(2, stamped.Count);
    }

    /// <summary>Whole seconds from stamp to anchor; never negative; Kind-blind (both instants are UTC).</summary>
    [Fact]
    public void AgeSeconds_IsTheWholeSecondDistanceToTheAnchor_AndNeverNegative()
    {
        var stamp = new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Unspecified);
        var anchor = new DateTime(2026, 9, 18, 12, 5, 0, DateTimeKind.Utc);
        Assert.Equal(300L, LatestSnapshotStamp.AgeSeconds(stamp, anchor));
        Assert.Equal(300L, LatestSnapshotStamp.AgeSeconds(stamp, anchor.AddTicks(4_000_000)));   /* 0.4 s rounds down */
        Assert.Equal(301L, LatestSnapshotStamp.AgeSeconds(stamp, anchor.AddTicks(6_000_000)));   /* 0.6 s rounds up */
        Assert.Equal(0L, LatestSnapshotStamp.AgeSeconds(anchor, stamp));                          /* clamped */
        Assert.Equal(0L, LatestSnapshotStamp.AgeSeconds(stamp, stamp));
    }

    /* ───────────────────────── the matchers, witnessed ───────────────────────── */

    [Fact]
    public void TheDiscriminators_FlagTheDefectShapes_AndPassTheFixedOnes()
    {
        Assert.Matches(CapturedAtKey, "                captured_at = snapshot.CapturedAt!.Value.ToString(\"o\"),");
        Assert.DoesNotMatch(CapturedAtKey, "                last_captured_at = Stamp(c.LastCapturedAt),");
        Assert.DoesNotMatch(CapturedAtKey, "                collection_time = stats.CollectionTime.ToString(\"o\"),");

        Assert.Matches(AgeSecondsKey, "                age_seconds = LatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, now),");
        Assert.Matches(WindowKey, "                window = window.Select(WindowShape)");
        Assert.DoesNotMatch(WindowKey, "                window_start = windowStart.ToString(\"o\"),");

        /* The retired stamp as it shipped on get_session_stats, and the fixed shape: captured_at at the top, a
           per-row collection_time inside the applications projection that must NOT read as the stamp. */
        Assert.Matches(TopLevelCollectionTimeKey,
            "            return JsonSerializer.Serialize(new\n            {\n                server = resolved.ServerName,\n                collection_time = rows[0].CollectionTime.ToString(\"o\"),\n                summary = new\n                {\n                    total_connections = totalConnections,\n                },\n            }, McpHelpers.JsonOptions);");
        Assert.DoesNotMatch(TopLevelCollectionTimeKey,
            "            return JsonSerializer.Serialize(new\n            {\n                server = resolved.ServerName,\n                captured_at = rows[0].CollectionTime.ToString(\"o\"),\n                grants = rows.Select(r => new\n                {\n                    collection_time = r.CollectionTime.ToString(\"o\"),\n                }),\n            }, McpHelpers.JsonOptions);");
        /* And the memory-grant shape verbatim in its indentation: the rows are a pre-built local whose
           projection carries collection_time at the SAME sixteen-space indent the old discriminator keyed on. */
        Assert.DoesNotMatch(TopLevelCollectionTimeKey,
            "            var grants = rows.Select(r => new\n            {\n                collection_time = r.CollectionTime.ToString(\"o\"),\n            });\n\n            return JsonSerializer.Serialize(new\n            {\n                server = resolved.ServerName,\n                captured_at = rows[0].CollectionTime.ToString(\"o\"),\n                grants,\n            }, McpHelpers.JsonOptions);");

        Assert.Matches(LatestReaderCall, "            var snapshot = await DarlingDataReader.GetLatestMemoryClerksAsync(postgres, resolved.ServerId);");
        Assert.Matches(LatestReaderCall, "            var rows = await DarlingMemoryGrantReader.GetResourceSemaphoreLatestAsync(");
        Assert.Matches(LatestReaderCall, "            var rows = await dataService.GetLatchStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);");
        Assert.DoesNotMatch(LatestReaderCall, "            var rows = await DarlingDataReader.GetTempDbTrendAsync(postgres, resolved.ServerId, a, b);");
        /* #3653: the GetCurrent*Async arm — the exact call that sat outside the census — and the history read
           beside it in the same reader, which must stay outside. */
        Assert.Matches(LatestReaderCall, "            var rows = await DarlingPgServerConfigReader.GetCurrentConfigAsync(");
        Assert.DoesNotMatch(LatestReaderCall, "            var rows = await DarlingPgServerConfigReader.GetConfigChangesAsync(");
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    private static IEnumerable<(string Label, string Body, Shape Shape)> LatestToolBodies()
    {
        foreach (var (type, name, liteFile, shape) in LatestTools)
        {
            yield return ($"Darling {name}", ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), name), shape);
            yield return ($"Lite {name}", ToolBody(ReadRepoFileLf(liteFile.Split('/')), name), shape);
        }
    }

    private static IEnumerable<(string File, string Source)> AllDarlingToolSources()
    {
        var root = RepoFile.PathTo("Darling/PerformanceMonitor.Darling.Service/Mcp");
        foreach (var file in System.IO.Directory.EnumerateFiles(root, "*.cs").Order(StringComparer.Ordinal))
        {
            yield return (System.IO.Path.GetFileName(file), System.IO.File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal));
        }
    }

    private static string ReaderSql(string sqlName) => sqlName switch
    {
        nameof(DarlingDataReader.LatestMemoryClerksSql) => DarlingDataReader.LatestMemoryClerksSql,
        nameof(DarlingDataReader.LatestFileIoStatsSql) => DarlingDataReader.LatestFileIoStatsSql,
        nameof(DarlingDataReader.LatestPerfmonStatsSql) => DarlingDataReader.LatestPerfmonStatsSql,
        nameof(DarlingCurrentConfigReader.ServerConfigSql) => DarlingCurrentConfigReader.ServerConfigSql,
        nameof(DarlingCurrentConfigReader.DatabaseConfigSql) => DarlingCurrentConfigReader.DatabaseConfigSql,
        nameof(DarlingCurrentConfigReader.TraceFlagsSql) => DarlingCurrentConfigReader.TraceFlagsSql,
        nameof(DarlingConfigHistoryReader.DatabaseScopedConfigSql) => DarlingConfigHistoryReader.DatabaseScopedConfigSql,
        nameof(DarlingConfigHistoryReader.QueryStoreHealthSql) => DarlingConfigHistoryReader.QueryStoreHealthSql,
        nameof(DarlingMemoryGrantReader.ResourceSemaphoreWindowSql) => DarlingMemoryGrantReader.ResourceSemaphoreWindowSql,
        nameof(DarlingMemoryGrantReader.MemoryGrantsWindowSql) => DarlingMemoryGrantReader.MemoryGrantsWindowSql,
        nameof(DarlingPgLoggingAuditReader.NewestSnapshotSql) => DarlingPgLoggingAuditReader.NewestSnapshotSql,
        nameof(DarlingPgServerConfigReader.CurrentConfigSql) => DarlingPgServerConfigReader.CurrentConfigSql,
        nameof(DarlingObjectStatsReader.DatabaseSizeLatestSql) => DarlingObjectStatsReader.DatabaseSizeLatestSql,
        nameof(DarlingObjectStatsReader.IndexLockingSql) => DarlingObjectStatsReader.IndexLockingSql,
        nameof(DarlingJobReader.RunningJobsSql) => DarlingJobReader.RunningJobsSql,
        nameof(DarlingDataReader.LatestServerPropertiesSql) => DarlingDataReader.LatestServerPropertiesSql,
        nameof(DarlingSessionReader.LatestSessionStatsSql) => DarlingSessionReader.LatestSessionStatsSql,
        _ => throw new ArgumentOutOfRangeException(nameof(sqlName), sqlName, "not a read this census names"),
    };

    private static MethodInfo ToolMethod(Type type, string toolName) => type
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName);

    private static string DarlingFileOf(Type type) =>
        $"Darling/PerformanceMonitor.Darling.Service/Mcp/{type.Name}.cs";

    /// <summary>The source from one tool's <c>[McpServerTool(Name = "…")]</c> attribute to the next tool's, or
    /// to the end of the file — the span that holds its description, parameters and payload.</summary>
    private static string ToolBody(string source, string toolName)
    {
        var marker = $"[McpServerTool(Name = \"{toolName}\")";
        var start = source.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(start >= 0, $"no tool named {toolName} in the source");
        var next = source.IndexOf("[McpServerTool(", start + marker.Length, StringComparison.Ordinal);
        return next < 0 ? source[start..] : source[start..next];
    }

    /// <summary>The tool's description: the attribute's literal, or the const it names (the aligned tools
    /// describe themselves through a const so the two SKUs' texts can be pinned equal).</summary>
    private static string DescriptionOf(string body, string label)
    {
        /* Anchored on the TOOL attribute the body starts with, so a parameter's [Description("Server name…")]
           further down can never be read as the tool's — which is exactly what a bare `Description(` search
           did on the const-described tools when this file was first executed. */
        var attribute = Regex.Match(body, @"\A\[McpServerTool\(Name = ""[a-z_0-9]+""\), Description\((?:\s*)(?:""((?:[^""\\]|\\.)*)""|(\w+))\)\]");
        if (attribute.Success)
        {
            if (attribute.Groups[1].Success)
            {
                return attribute.Groups[1].Value;
            }

            var constName = attribute.Groups[2].Value;

            /* The const lives in the same file, above the attribute; the body slice starts AT the attribute, so
               it is not in the body — resolve it from the file the label points at. */
            var (type, toolName, liteFile, _) = LatestTools.Single(t => label.EndsWith(t.ToolName, StringComparison.Ordinal));
            return label.StartsWith("Darling", StringComparison.Ordinal)
                ? (string)type.GetField(constName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)!.GetRawConstantValue()!
                : LiteConstLiteral(liteFile, constName);
        }

        /* #3898: a converted tool's Description is a "head" + McpToolGuide.Marker + "tail" concatenation —
           neither a single literal nor a single const. Join every quoted literal chunk in source order,
           skipping the identifiers between them (the marker, McpToolGuideTopics.* consts); this test only
           needs the plain prose, never the reading-guide plumbing those identifiers resolve to. */
        var anchor = Regex.Match(body, @"\A\[McpServerTool\(Name = ""[a-z_0-9]+""\), Description\(");
        Assert.True(anchor.Success, $"{label}: could not locate the tool's Description on its McpServerTool attribute");
        var i = anchor.Length;
        var sb = new System.Text.StringBuilder();
        while (i < body.Length && !(body[i] == ')' && i + 1 < body.Length && body[i + 1] == ']'))
        {
            if (body[i] == '"')
            {
                i++;
                while (i < body.Length && body[i] != '"')
                {
                    if (body[i] == '\\' && i + 1 < body.Length) { sb.Append(body[i + 1]); i += 2; }
                    else { sb.Append(body[i]); i++; }
                }
                i++;
            }
            else
            {
                i++;
            }
        }
        Assert.True(sb.Length > 0, $"{label}: Description( had no literal text before its closing )]");
        return sb.ToString();
    }

    /// <summary>A Lite <c>const string</c>'s literal, read from source: <c>internal const string Name =\n "…";</c>.</summary>
    private static string LiteConstLiteral(string liteFile, string constName)
    {
        var source = ReadRepoFileLf(liteFile.Split('/'));
        var m = Regex.Match(source, $@"const string {Regex.Escape(constName)}\s*=\s*""((?:[^""\\]|\\.)*)"";");
        Assert.True(m.Success, $"{liteFile}: no `const string {constName} = \"…\";`");
        return Regex.Unescape(m.Groups[1].Value);
    }

    /// <summary>Lite's advertised parameter names for a tool, read off the MASKED signature (comments and
    /// string literals blanked, so prose inside a [Description] cannot read as a parameter) — the idiom
    /// <c>DarlingMcpDataToolsTests.LiteMcpParamNames</c> established. Every MCP parameter carries a default
    /// and the injected services do not, so the defaulted ones in order ARE the contract.</summary>
    private static string[] LiteParamNames(string liteFile, string toolName)
    {
        var raw = ReadRepoFileLf(liteFile.Split('/'));
        var attribute = raw.IndexOf($"Name = \"{toolName}\"", StringComparison.Ordinal);
        Assert.True(attribute > 0, $"{liteFile}: no tool named {toolName}");

        var masked = CSharpSourceWalker.StripCommentsAndStrings(raw);
        Assert.Equal(raw.Length, masked.Length);

        var declaration = masked.IndexOf("public static", attribute, StringComparison.Ordinal);
        var signature = masked.IndexOf('(', declaration);
        var body = masked.IndexOf('{', signature);
        Assert.True(body > signature, $"{liteFile}: could not find the end of {toolName}'s signature");

        return Regex.Matches(masked[signature..body], @"(\w+)\s*=\s*[^,)]+")
            .Select(m => m.Groups[1].Value)
            .ToArray();
    }

    /// <summary>Comments removed, so a comment that NAMES a key is not read as the key. Line and block comments
    /// only; string literals stay, because the payload keys under test are not in strings.</summary>
    private static string Strip(string source) =>
        Regex.Replace(Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the stamps: the seeded row's stamp comes back as
/// <c>captured_at</c>, <c>age_seconds</c> is measured against the anchor the caller sent (never the wall clock),
/// a SearchBound tool refuses a snapshot older than its search span, the memory-grant window sees a storm the
/// latest snapshot does not, and the latch band names its interval. Anchored in the past on purpose: the
/// assertions are equalities, and an anchor of "now" would make every age a race.
/// </summary>
[Collection("live-postgres")]
public sealed class McpLatestSnapshotStampLivePostgresTests
{
    private const string ServerName = "darling-mcp-latest-stamp-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables =
    [
        "memory_grant_stats", "cpu_scheduler_stats", "server_config", "trace_flags", "memory_clerks", "latch_stats",
        "memory_stats", "cpu_utilization_stats", "collection_log",
        /* #3653: the four reads that moved from the retired collection_time spelling. */
        "database_size_stats", "running_jobs", "server_properties", "session_stats",
    ];

    [Fact]
    public async Task LatestReads_SayWhenTheyWereCaptured_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-stamp test.");

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

            /* Every row sits in the past; the anchor is base + 5 min, so every age below is exact. */
            var @base = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-2);
            var anchor = @base.AddMinutes(5).ToString("o") + "Z";

            /* ── memory grants: a storm 30 minutes before a calm latest snapshot ── */
            foreach (var (t, waiters, timeouts, granted) in new[] { (@base.AddMinutes(-30), 12, 3L, 6000m), (@base, 0, 0L, 500m) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, (short)0, 2, 8000m, 12000m, 8000m, 8000m - granted, granted, granted, 3, waiters, 4L, 2L, timeouts, 0L);
            }

            var semaphore = Parse(await DarlingMcpMemoryGrantTools.GetResourceSemaphore(postgres, ServerName, 1, as_of: anchor));
            Assert.Equal(Stamp(@base), semaphore.GetProperty("captured_at").GetString());
            Assert.Equal(300, semaphore.GetProperty("age_seconds").GetInt64());
            var latestRow = Assert.Single(semaphore.GetProperty("grants").EnumerateArray());
            Assert.Equal(0, latestRow.GetProperty("waiter_count").GetInt32());
            var windowRow = Assert.Single(semaphore.GetProperty("window").EnumerateArray());
            Assert.Equal(2, windowRow.GetProperty("snapshots_in_window").GetInt64());
            Assert.Equal(12, windowRow.GetProperty("peak_waiter_count").GetInt64());
            Assert.Equal(Stamp(@base.AddMinutes(-30)), windowRow.GetProperty("peak_waiters_at").GetString());
            Assert.Equal(3, windowRow.GetProperty("timeout_errors_in_window").GetInt64());
            Assert.Equal(6000d, windowRow.GetProperty("peak_granted_memory_mb").GetDouble());
            Assert.Equal(2000d, windowRow.GetProperty("min_available_memory_mb").GetDouble());
            Assert.Equal(Stamp(@base), windowRow.GetProperty("last_snapshot_at").GetString());

            var grants = Parse(await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName, 1, as_of: anchor));
            Assert.Equal(Stamp(@base), grants.GetProperty("captured_at").GetString());
            Assert.Equal(300, grants.GetProperty("age_seconds").GetInt64());
            var poolWindow = Assert.Single(grants.GetProperty("window").EnumerateArray());
            Assert.Equal(JsonValueKind.Null, poolWindow.GetProperty("resource_semaphore_id").ValueKind);
            Assert.Equal(12, poolWindow.GetProperty("peak_waiter_count").GetInt64());

            /* ── scheduler: one snapshot three hours before the anchor — inside a 4 h search, outside a 1 h one ── */
            var schedulerAt = @base.AddMinutes(5).AddHours(-3);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count, total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count, total_queued_request_count, total_blocked_task_count, total_active_parallel_thread_count, runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)",
                CollectionIdGenerator.Next(), schedulerAt, ServerId, ServerName, 512, 8, 8, 60, 5L, 100, 7.5m, 40, 12, 2, 20L, 12.5m, false, true, false, true, 65536000L, 32768000L, false, 1, 1, 0, false);

            var scheduler = Parse(await DarlingMcpPlanCacheSchedulerTools.GetCpuSchedulerPressure(postgres, ServerName, 4, as_of: anchor));
            Assert.Equal(Stamp(schedulerAt), scheduler.GetProperty("captured_at").GetString());
            Assert.Equal(3 * 3600, scheduler.GetProperty("age_seconds").GetInt64());
            Assert.StartsWith("CRITICAL", scheduler.GetProperty("pressure_level").GetString(), StringComparison.Ordinal);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpPlanCacheSchedulerTools.GetCpuSchedulerPressure(postgres, ServerName, 1, as_of: anchor)));

            /* ── config: captured on connect, stamped with that connect ── */
            var connectAt = @base.AddDays(-3);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), connectAt, ServerId, ServerName, "max degree of parallelism", 4L, 4L, true, true);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), connectAt, ServerId, ServerName, 3226, true, true, false);

            Assert.Equal(Stamp(connectAt), Parse(await DarlingMcpConfigTools.GetServerConfig(postgres, ServerName)).GetProperty("captured_at").GetString());
            Assert.Equal(Stamp(connectAt), Parse(await DarlingMcpConfigTools.GetTraceFlags(postgres, ServerName)).GetProperty("captured_at").GetString());

            /* ── clerks: the newest snapshot's stamp, not the older one's ── */
            foreach (var t in new[] { @base.AddMinutes(-10), @base })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1,$2,$3,$4,$5,$6)", CollectionIdGenerator.Next(), t, ServerId, ServerName, "MEMORYCLERK_SQLBUFFERPOOL", 40000m);
            }
            Assert.Equal(Stamp(@base), Parse(await DarlingMcpDataTools.GetMemoryClerks(postgres, ServerName)).GetProperty("captured_at").GetString());

            /* ── latch: hot earlier, quiet now — LOW severity beside a large window total, and the band says why ── */
            foreach (var (t, delta) in new[] { (@base.AddMinutes(-20), 20000L), (@base, 100L) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "ACCESS_METHODS_DATASET_PARENT", 1000L, 20100L, 50L, 100L, delta, 5L, 60);
            }
            var latch = Assert.Single(Parse(await DarlingMcpLatchSpinlockTools.GetLatchStats(postgres, ServerName, 1, as_of: anchor)).GetProperty("latches").EnumerateArray());
            Assert.Equal(20100, latch.GetProperty("total_delta_wait_time_ms").GetInt64());
            Assert.Equal("LOW", latch.GetProperty("severity").GetString());
            var band = latch.GetProperty("severity_banded_from");
            Assert.Equal(100, band.GetProperty("delta_wait_time_ms").GetInt64());
            Assert.Equal(60d, band.GetProperty("interval_seconds").GetDouble());
            Assert.Equal(Stamp(@base), band.GetProperty("captured_at").GetString());

            /* ── server summary: three clocks — a stale CPU row under a fresh collection log ── */
            var cpuAt = @base.AddDays(-1);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1,$2,$3,$4,$5,$6,$7)", CollectionIdGenerator.Next(), cpuAt, ServerId, ServerName, cpuAt, 42, 3);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7)", CollectionIdGenerator.Next(), @base, ServerId, ServerName, 65536m, 8192m, 40000m);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1,$2,$3,$4,$5,120,'SUCCESS',7)", CollectionIdGenerator.Next(), ServerId, ServerName, "memory_stats", @base.AddMinutes(1));

            var summary = Parse(await DarlingMcpHealthTools.GetServerSummary(postgres, ServerName));
            Assert.Equal(Stamp(cpuAt), summary.GetProperty("cpu_captured_at").GetString());
            Assert.Equal(Stamp(@base), summary.GetProperty("memory_captured_at").GetString());
            Assert.Equal(Stamp(@base.AddMinutes(1)), summary.GetProperty("last_collection").GetString());
            Assert.Equal(DarlingHealthReader.ServerSummaryCountsWindowHours, summary.GetProperty("counts_window_hours").GetInt32());

            /* ── #3653: the four reads that stamped themselves as collection_time before the vocabulary ──

               Each seeded with an OLDER snapshot beside the newest, so the stamp asserted is the newest row's
               and not merely "some row's"; and asserted by the NEW key only — a payload that still carried
               the old spelling would pass a TryGetProperty on either, so the retired key is asserted absent. */
            foreach (var t in new[] { @base.AddMinutes(-15), @base })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type_desc, total_size_mb, used_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "Sales", "Sales", "ROWS", 10240m, 8192m, "D:\\", 512000m, 204800m);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                    t, ServerId, ServerName, "Nightly ETL", "22222222-2222-2222-2222-222222222222", true, t.AddMinutes(-30), 1800L, 600L, 900L, 42L, true, 300.0m);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, cpu_count, physical_memory_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "Enterprise Edition (64-bit)", "16.0.4135.4", 16, 131072L);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_logical_reads)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, "App", 12, 2, 10, 0, 5000L, 90000L);
            }

            foreach (var (name, json) in new[]
                     {
                         ("get_database_sizes", await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, ServerName)),
                         ("get_running_jobs", await DarlingMcpJobTools.GetRunningJobs(postgres, ServerName)),
                         ("get_server_properties", await DarlingMcpDataTools.GetServerProperties(postgres, ServerName)),
                         ("get_session_stats", await DarlingMcpSessionTools.GetSessionStats(postgres, ServerName)),
                     })
            {
                var payload = Parse(json);
                Assert.True(payload.TryGetProperty("captured_at", out var capturedAt), $"{name}: no captured_at on the payload");
                Assert.Equal(Stamp(@base), capturedAt.GetString());
                Assert.False(payload.TryGetProperty("collection_time", out _), $"{name}: still publishes the retired top-level collection_time beside captured_at");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static JsonElement Parse(string json)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        var root = JsonDocument.Parse(json).RootElement.Clone();
        Assert.False(root.TryGetProperty("status", out _), "expected a data-bearing payload, got a status envelope: " + json);
        return root;
    }

    /// <summary>A seeded naive-UTC instant as the tools emit it: <c>ToString("o")</c> on a <c>Kind=Unspecified</c>
    /// value, so no <c>Z</c>.</summary>
    private static string Stamp(DateTime naiveUtc) => DateTime.SpecifyKind(naiveUtc, DateTimeKind.Unspecified).ToString("o");

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", Tables.Select(t => $"DELETE FROM {t} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
