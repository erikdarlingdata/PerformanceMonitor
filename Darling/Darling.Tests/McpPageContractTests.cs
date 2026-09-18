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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3541 A3: six MCP tool groups published a HIDDEN reader cap as if it were the window — <c>LIMIT 200</c> /
/// <c>50</c> / <c>500</c> under a tool that advertised <c>limit</c>, a <c>Take(limit)</c> on top, and the capped
/// count emitted under a <c>total_*</c> name. An agent that cannot see the code read a 200-row page of a
/// 5,000-event window as the window. #3287 fixed one instance (<c>get_collection_log</c>) and established the
/// pattern; this file pins that pattern across every tool the finding named, on BOTH SKUs, as ONE dialect:
///
/// <list type="number">
/// <item>the cap is the caller's <c>limit</c>, bound as a SQL parameter, never a literal;</item>
/// <item>truncation is OBSERVED by fetching <c>limit + 1</c> and comparing <c>Count &gt; limit</c> — never
/// inferred from <c>Count &gt;= limit</c>, which cannot tell a window of exactly <c>limit</c> rows from a busier
/// one;</item>
/// <item>the page publishes its own time bounds under the <c>oldest_returned_* / newest_returned_*</c> names
/// <c>get_collection_log</c> established, and names its ordering;</item>
/// <item>no page count is called <c>total_*</c>;</item>
/// <item>a filter that shapes the population is stated and measured (<c>get_alert_history</c>'s
/// <c>dismissed = FALSE</c>);</item>
/// <item>the same tool name emits the same page-contract keys on both SKUs.</item>
/// </list>
///
/// <para>Darling's half is reflected off the assembly; Lite's half is read from source, because this project
/// does not (and should not) reference the desktop app — the same arrangement
/// <see cref="McpPayloadClockFrameDisciplineTests"/> uses. The discriminators are exercised against literals
/// written for the purpose, because a census whose matcher has quietly stopped matching reports a clean bill
/// of health. <c>Lite.Tests/McpPageContractTests</c> executes the Lite tools against a real DuckDB;
/// <see cref="McpPageContractLivePostgresTests"/> executes the Darling ones against live Postgres.</para>
///
/// <para><b>#3541 A7 extends the dialect to PERCENTS.</b> Five PostgreSQL tools divided every row by the sum
/// of the rows they had fetched and published the result as <c>pct_of_total_*</c>, so a three-row page summed
/// to 100% of "total" by construction. The rule added here: a share's denominator is the WINDOW's figure,
/// computed on the same statement as the rows (<c>SUM(...) OVER ()</c> over the grouped result, before
/// <c>LIMIT</c>), published as <c>total_*</c>; the page's own sum travels as <c>returned_*</c>; and every
/// description names which denominator its shares use. <c>*_of_returned</c> remains the ONE other spelling,
/// for a share that genuinely is of the page and says so (<c>get_pg_database_stats</c>' cache ratio). The
/// arithmetic through each projection is <see cref="DarlingMcpPgPercentDenominatorTests"/>' subject; this
/// file holds the census.</para>
/// </summary>
public sealed class McpPageContractTests
{
    /* ───────────────────────── the census ───────────────────────── */

    /// <summary>
    /// Every tool the finding named, by SKU file. The Lite <c>get_blocked_process_reports</c> is Darling's
    /// <c>get_blocking</c> under another name (the one pair <c>CrossAppMcpToolInventoryPinTests</c> records
    /// as drift), so it is listed under Darling's name here and the twin-parity test maps it.
    /// </summary>
    public static readonly (Type Tools, string ToolName, string LiteFile, string LiteToolName)[] PagedTools =
    [
        (typeof(DarlingMcpBlockingTools), "get_blocking", "Lite/Mcp/McpBlockingTools.cs", "get_blocked_process_reports"),
        (typeof(DarlingMcpBlockingTools), "get_deadlocks", "Lite/Mcp/McpBlockingTools.cs", "get_deadlocks"),
        (typeof(DarlingMcpBlockingTools), "get_deadlock_detail", "Lite/Mcp/McpBlockingTools.cs", "get_deadlock_detail"),
        (typeof(DarlingMcpBlockingTools), "get_blocked_process_xml", "Lite/Mcp/McpBlockingTools.cs", "get_blocked_process_xml"),
        (typeof(DarlingMcpAlertTools), "get_alert_history", "Lite/Mcp/McpAlertTools.cs", "get_alert_history"),
        (typeof(DarlingMcpLongQueryTools), "get_long_query_completions", "Lite/Mcp/McpLongQueryTools.cs", "get_long_query_completions"),
        (typeof(DarlingMcpPlanCorrectionTools), "get_plan_corrections", "Lite/Mcp/McpPlanCorrectionTools.cs", "get_plan_corrections"),
        (typeof(DarlingMcpSessionTools), "get_waiting_tasks", "Lite/Mcp/McpWaitTools.cs", "get_waiting_tasks"),
        (typeof(DarlingMcpDataTools), "get_wait_stats", "Lite/Mcp/McpWaitTools.cs", "get_wait_stats"),
        /* #3541 A13: get_active_queries joined the dialect when its filters moved into the SQL — it now pages
           the FILTERED population at limit + 1, publishes snapshots_returned / truncated / the page's bounds,
           and its total_snapshots is the filtered population's COUNT(*) OVER () rather than rows.Count of an
           unfiltered window read. */
        (typeof(DarlingMcpSessionTools), "get_active_queries", "Lite/Mcp/McpSessionTools.cs", "get_active_queries"),
    ];

    /// <summary>
    /// The source span of every paged tool on both SKUs — the census walks these BODIES, not whole files,
    /// for the <c>total_* = .Count</c> and <c>&gt;= limit</c> shapes. Whole files would sweep in tools with a
    /// different contract: <c>get_mute_rules</c>' <c>total_count</c> counts a whole set, which is not a hidden
    /// cap, and a census that flagged it would be asserting a rule the finding did not state.
    /// (<c>get_active_queries</c> used to be the other exclusion, for publishing an unbounded window count
    /// beside its page; #3541 A13 made that count the filtered population's, computed in SQL, so it is a
    /// member now.)
    /// </summary>
    private static IEnumerable<(string Label, string Body)> PagedToolBodies()
    {
        foreach (var (type, darlingName, liteFile, liteName) in PagedTools)
        {
            yield return ($"Darling {darlingName}", ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), darlingName));
            yield return ($"Lite {liteName}", ToolBody(ReadRepoFileLf(liteFile.Split('/')), liteName));
        }
    }

    /// <summary>
    /// The Darling reader consts behind the paged tools, each of which must bind its cap as a parameter.
    /// </summary>
    private static readonly (string Name, string Sql)[] PagedReads =
    [
        (nameof(DarlingBlockingReader.BlockedProcessReportsSql), DarlingBlockingReader.BlockedProcessReportsSql),
        (nameof(DarlingBlockingReader.BlockedProcessReportsWithXmlSql), DarlingBlockingReader.BlockedProcessReportsWithXmlSql),
        (nameof(DarlingBlockingReader.DmvBlockingSnapshotsSql), DarlingBlockingReader.DmvBlockingSnapshotsSql),
        (nameof(DarlingBlockingReader.RecentDeadlocksSql), DarlingBlockingReader.RecentDeadlocksSql),
        (nameof(DarlingBlockingReader.RecentDeadlocksWithGraphSql), DarlingBlockingReader.RecentDeadlocksWithGraphSql),
        (nameof(DarlingAlertReader.AlertHistorySql), DarlingAlertReader.AlertHistorySql),
        (nameof(DarlingAlertReader.AlertHistoryAllServersSql), DarlingAlertReader.AlertHistoryAllServersSql),
        (nameof(DarlingLongQueryReader.LongQueryCompletionsSql), DarlingLongQueryReader.LongQueryCompletionsSql),
        (nameof(DarlingPlanCorrectionReader.PlanCorrectionsSql), DarlingPlanCorrectionReader.PlanCorrectionsSql),
        (nameof(DarlingSessionReader.WaitingTasksSql), DarlingSessionReader.WaitingTasksSql),
        (nameof(DarlingDataReader.WaitStatsSql), DarlingDataReader.WaitStatsSql),
        (nameof(DarlingSessionReader.ActiveQueriesSql), DarlingSessionReader.ActiveQueriesSql),
    ];

    /* ───────────────────────── the discriminators ───────────────────────── */

    /// <summary>A page count emitted under a window's name — the shape the finding was about.</summary>
    private static readonly Regex TotalOfPageCount = new(@"\btotal_\w+\s*=\s*\w+\.Count\b", RegexOptions.Compiled);

    /// <summary>Truncation inferred from the cap rather than observed past it.</summary>
    private static readonly Regex TruncationInferred = new(@"\btruncated\s*=\s*\w+\.Count\s*>=\s*", RegexOptions.Compiled);

    /// <summary>Truncation observed: the row past <c>limit</c> came back.</summary>
    private static readonly Regex TruncationObserved = new(@"var truncated = \w+\.Count > limit;", RegexOptions.Compiled);

    /// <summary>A literal row cap in a paged read. <c>LIMIT 1), 0)</c> is the single-row offset CTE and is not a
    /// page cap; it is stripped before the match rather than allow-listed by number, because <c>LIMIT 1</c>
    /// elsewhere WOULD be a defect.</summary>
    private static readonly Regex LiteralLimit = new(@"\bLIMIT\s+\d+\b", RegexOptions.Compiled);

    private static readonly Regex ParameterLimit = new(@"\bLIMIT \$\d+\s*$", RegexOptions.Compiled);

    /// <summary>The page-contract keys a tool body emits: its count, its bounds, its ordering.</summary>
    private static readonly Regex ContractKeys = new(
        @"\b(\w+_returned|oldest_returned_\w+|newest_returned_\w+)\s*=|\border = ""([a-z_]+)""",
        RegexOptions.Compiled);

    /* ───────────────────────── Darling, reflected ───────────────────────── */

    [Fact]
    public void EveryPagedTool_SaysWhatBoundsItsPage_InItsDescription()
    {
        foreach (var (type, name, _, _) in PagedTools)
        {
            var method = ToolMethod(type, name);
            var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.True(description.Contains("truncated", StringComparison.Ordinal),
                $"{name}: the description never mentions `truncated`, so an agent reads the page as the window");
            Assert.True(description.Contains("limit", StringComparison.Ordinal),
                $"{name}: the description never says the page is bounded by limit");

            var limit = method.GetParameters().Single(p => p.Name == "limit");
            Assert.Contains("truncated", limit.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
        }
    }

    /// <summary>The three incident readers that take a <c>dedup_key</c> promise (#2159) that the fingerprint
    /// scan runs over the window before <c>limit</c>; with the scan now bounded by a stated ceiling, each must
    /// say so and name the two fields that report it.</summary>
    [Theory]
    [InlineData("get_blocking")]
    [InlineData("get_deadlocks")]
    public void FingerprintReaders_NameTheScanCeilingFields(string toolName)
    {
        var method = ToolMethod(typeof(DarlingMcpBlockingTools), toolName);
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("rows_examined", description, StringComparison.Ordinal);
        Assert.Contains("scan_truncated", description, StringComparison.Ordinal);

        var key = method.GetParameters().Single(p => p.Name == "dedup_key");
        Assert.Contains("BEFORE limit", key.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryPagedRead_BindsItsCapAsAParameter_NeverALiteral()
    {
        foreach (var (name, sql) in PagedReads)
        {
            var body = sql.Replace("LIMIT 1), 0)", string.Empty, StringComparison.Ordinal);
            Assert.False(LiteralLimit.IsMatch(body),
                $"{name} caps with a literal the caller cannot see: {LiteralLimit.Match(body).Value}");
            Assert.True(ParameterLimit.IsMatch(sql.TrimEnd()),
                $"{name} does not end in a parameterised LIMIT, so the tool's limit + 1 over-fetch has nothing to bind to");
        }
    }

    /* ───────────────────────── both SKUs, from source ───────────────────────── */

    [Fact]
    public void NoPagedTool_PublishesAPageCountAsATotal_OnEitherSku()
    {
        foreach (var (label, body) in PagedToolBodies())
        {
            var text = Strip(body);
            var hit = TotalOfPageCount.Match(text);
            Assert.False(hit.Success,
                $"{label}: `{hit.Value}` publishes a page count under a window's name — rename it *_returned, or "
                + "compute a real windowed COUNT and publish both, clearly named");
        }
    }

    [Fact]
    public void EveryPagedTool_ObservesTruncation_AndNeverInfersIt_OnEitherSku()
    {
        foreach (var (label, body) in PagedToolBodies())
        {
            var text = Strip(body);
            var inferred = TruncationInferred.Match(text);
            Assert.False(inferred.Success,
                $"{label}: `{inferred.Value}` infers truncation from the cap; fetch limit + 1 and compare Count > limit");
            Assert.True(TruncationObserved.IsMatch(text),
                $"{label}: no `var truncated = x.Count > limit;` — a paged tool that never observes its own cap");
            /* The over-fetch that makes the observation possible: the reader is asked for one row past the cap. */
            Assert.Contains("limit + 1", text, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// One dialect, not six: the same tool name emits the same page-contract keys on both SKUs. Compared per
    /// TOOL BODY rather than per file, because the blocking files host tools with different bounds
    /// (<c>event_time</c> for reports, <c>deadlock_time</c> for deadlocks). The naming-drift pair is compared
    /// on bounds and ordering only, since its count key follows the tool's own noun.
    /// </summary>
    [Fact]
    public void TheSameToolName_EmitsTheSamePageContractKeys_OnBothSkus()
    {
        foreach (var (type, darlingName, liteFile, liteName) in PagedTools)
        {
            var darlingFile = DarlingFileOf(type);
            var darlingKeys = KeysOf(Strip(ToolBody(ReadRepoFileLf(darlingFile.Split('/')), darlingName)));
            var liteKeys = KeysOf(Strip(ToolBody(ReadRepoFileLf(liteFile.Split('/')), liteName)));

            Assert.NotEmpty(darlingKeys);

            if (darlingName != liteName)
            {
                /* get_blocking ↔ get_blocked_process_reports: the noun differs, the bounds must not. */
                darlingKeys.RemoveWhere(k => k.EndsWith("_returned", StringComparison.Ordinal));
                liteKeys.RemoveWhere(k => k.EndsWith("_returned", StringComparison.Ordinal));
            }

            Assert.True(darlingKeys.SetEquals(liteKeys),
                $"{darlingName} ↔ {liteName}: Darling emits [{string.Join(", ", darlingKeys.Order())}], "
                + $"Lite emits [{string.Join(", ", liteKeys.Order())}] — same tool name, different page contract");
        }
    }

    /// <summary>The Lite descriptions carry the same commitment as Darling's, read from source.</summary>
    [Fact]
    public void EveryLitePagedTool_SaysWhatBoundsItsPage_InItsDescription()
    {
        foreach (var (_, _, liteFile, liteName) in PagedTools)
        {
            var body = ToolBody(ReadRepoFileLf(liteFile.Split('/')), liteName);
            var description = Regex.Match(body, @"Description\((?:\s*)""((?:[^""\\]|\\.)*)""\)\]").Groups[1].Value;
            Assert.False(string.IsNullOrEmpty(description), $"{liteFile}: could not locate {liteName}'s Description");
            Assert.Contains("truncated", description, StringComparison.Ordinal);
            Assert.Contains("limit", description, StringComparison.Ordinal);
        }
    }

    /* ───────────────────────── the matchers, witnessed ───────────────────────── */

    [Fact]
    public void TheDiscriminators_FlagTheDefectShapes_AndPassTheFixedOnes()
    {
        Assert.Matches(TotalOfPageCount, "                total_events = rows.Count,");
        Assert.DoesNotMatch(TotalOfPageCount, "                events_returned = page.Count,");
        /* A real windowed total is not a page count and must pass. */
        Assert.DoesNotMatch(TotalOfPageCount, "                total_deadlocks = totalDeadlocks,");

        Assert.Matches(TruncationInferred, "            var truncated = rows.Count >= limit;");
        Assert.DoesNotMatch(TruncationInferred, "            var truncated = rows.Count > limit;");
        Assert.Matches(TruncationObserved, "            var truncated = candidates.Count > limit;");

        Assert.Matches(LiteralLimit, "        ORDER BY event_time DESC\n        LIMIT 200\n");
        Assert.DoesNotMatch(LiteralLimit, "        ORDER BY event_time DESC\n        LIMIT $4\n");
        Assert.Matches(ParameterLimit, "        LIMIT $4");

        var keys = KeysOf("deadlocks_returned = page.Count,\n truncated,\n oldest_returned_deadlock_time = x,\n newest_returned_deadlock_time = y,\n order = \"deadlock_time_desc\",");
        Assert.Equal(
            new[] { "deadlocks_returned", "newest_returned_deadlock_time", "oldest_returned_deadlock_time", "order:deadlock_time_desc" },
            keys.Order().ToArray());
    }

    /* ───────────────────────── #3541 A7: percents name their denominator ───────────────────────── */

    /// <summary>
    /// The five tools whose shares were of the page, with the three keys each must now publish: the per-row
    /// share, the WINDOW total it divides by, and the page's own sum under a name that says so. Darling-only —
    /// Lite has no PostgreSQL tools — so there is no twin-parity arm; the cross-SKU sweep below is the
    /// negative census over every paged tool body on BOTH SKUs instead.
    /// </summary>
    public static readonly (Type Tools, string ToolName, string ShareKey, string TotalKey, string ReturnedKey)[] PercentTools =
    [
        (typeof(DarlingMcpPgStatementTools), "get_pg_top_queries", "pct_of_total_time", "total_exec_time_ms", "returned_exec_time_ms"),
        (typeof(DarlingMcpPgWaitTools), "get_pg_wait_stats", "pct_of_total_wait", "total_wait_time_ms", "returned_wait_time_ms"),
        (typeof(DarlingMcpPgWaitSamplingTools), "get_pg_wait_sampling", "pct_of_samples", "total_samples", "returned_samples"),
        (typeof(DarlingMcpPgKernelStatsTools), "get_pg_kernel_stats", "pct_of_total_cpu", "total_cpu_ms", "returned_cpu_ms"),
        (typeof(DarlingMcpPgIoTools), "get_pg_io_stats", "pct_of_total_reads", "total_reads", "returned_reads"),
    ];

    /// <summary>The reader consts behind them: each must carry its window total on the SAME statement as the
    /// rows and bind its cap as a parameter.</summary>
    private static readonly (string Name, string Sql)[] PercentReads =
    [
        (nameof(DarlingPgStatementReader.PgTopQueriesSql), DarlingPgStatementReader.PgTopQueriesSql),
        (nameof(DarlingPgWaitReader.PgWaitStatsSql), DarlingPgWaitReader.PgWaitStatsSql),
        (nameof(DarlingPgWaitSamplingReader.PgWaitSamplingSql), DarlingPgWaitSamplingReader.PgWaitSamplingSql),
        (nameof(DarlingPgKernelStatsReader.PgKernelStatsSql), DarlingPgKernelStatsReader.PgKernelStatsSql),
        (nameof(DarlingPgIoReader.PgIoSql), DarlingPgIoReader.PgIoSql),
    ];

    /// <summary>A local that is the sum of a fetched collection — the page sum the defect divided by.</summary>
    private static readonly Regex PageSumLocal = new(@"\bvar\s+(\w+)\s*=\s*\w+\s*\.Sum\(", RegexOptions.Compiled);

    /// <summary>A share key whose divisor is the named local. <c>[^,;]</c> keeps the match inside one
    /// initializer entry: the first comma in every share expression here is the one inside
    /// <c>Math.Round(x, 1)</c>, and the division precedes it.</summary>
    private static Regex ShareOverLocal(string local) =>
        new($@"\b\w*pct\w*\s*=[^,;]*?/\s*{Regex.Escape(local)}\b", RegexOptions.Compiled);

    /// <summary>A <c>total_*</c> key assigned from the named local, rounded or bare.</summary>
    private static Regex TotalFromLocal(string local) =>
        new($@"\btotal_\w+\s*=\s*(?:Math\.Round\(\s*)?{Regex.Escape(local)}\b", RegexOptions.Compiled);

    /// <summary>The window total on the reader's statement: an <c>OVER ()</c> aggregate aliased <c>window_total_*</c>.</summary>
    private static readonly Regex WindowTotalColumn = new(@"\)\s+OVER \(\)(?:\s*/\s*1000\.0)?(?:\s+AS\s+bigint\))?\s+AS\s+window_total_\w+", RegexOptions.Compiled);

    /// <summary>The page-record read the fixed tools go through: the tool fetches <c>limit + 1</c> and observes.</summary>
    private static readonly Regex PageTruncationObserved = new(@"var truncated = page\.Rows\.Count > limit;", RegexOptions.Compiled);

    [Fact]
    public void EveryPercentTool_NamesItsDenominator_AndItsPageBound_InItsDescription()
    {
        foreach (var (type, name, shareKey, totalKey, returnedKey) in PercentTools)
        {
            var method = ToolMethod(type, name);
            var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

            Assert.Contains("truncated", description, StringComparison.Ordinal);
            Assert.Contains("limit", description, StringComparison.Ordinal);
            /* The sentence that makes the numbers readable: which denominator, and that it is not the page. */
            Assert.Contains("SHARES ARE OF THE WINDOW, NOT OF THE PAGE", description, StringComparison.Ordinal);
            Assert.Contains(shareKey, description, StringComparison.Ordinal);
            Assert.Contains(totalKey, description, StringComparison.Ordinal);
            Assert.Contains(returnedKey, description, StringComparison.Ordinal);
            Assert.Contains("does not sum to 100%", description, StringComparison.Ordinal);

            var limit = method.GetParameters().Single(p => p.Name == "limit");
            var limitDescription = limit.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.Contains("truncated", limitDescription, StringComparison.Ordinal);
            Assert.Contains("whole window", limitDescription, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The denominator is on the SAME statement as the rows — a window aggregate over the grouped result,
    /// which PostgreSQL evaluates before <c>LIMIT</c> — so it cannot drift from them and costs no second read.
    /// And the cap is a parameter: <c>get_pg_top_queries</c> carried <c>LIMIT 50</c> as a literal under a limit
    /// the tool accepts up to 1,000, which is the A3 shape and the reason its share had TWO wrong denominators.
    /// </summary>
    [Fact]
    public void EveryPercentRead_CarriesItsWindowTotalOnTheSameStatement_AndBindsItsCap()
    {
        foreach (var (name, sql) in PercentReads)
        {
            Assert.True(WindowTotalColumn.IsMatch(sql),
                $"{name} has no `... OVER () AS window_total_*` column, so the tool has nothing but the page to divide by");
            Assert.False(LiteralLimit.IsMatch(sql), $"{name} caps with a literal: {LiteralLimit.Match(sql).Value}");
            Assert.True(ParameterLimit.IsMatch(sql.TrimEnd()), $"{name} does not end in a parameterised LIMIT");
            /* Before the cap, so it is the window's and not the page's: the OVER () sits above the LIMIT. */
            Assert.True(sql.IndexOf("OVER ()", StringComparison.Ordinal) < sql.LastIndexOf("LIMIT", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// The kernel read's ordering was <c>ORDER BY 3 + 4 DESC</c> — meant as two ordinals, read by PostgreSQL as
    /// the constant 7 and dropped, so the "ranked by CPU" page was the alphabetically-first series. Found when
    /// A7's three-row page came back 60 / 10 / 30 against live PostgreSQL 18. Pinned by SHAPE: the sort key
    /// names the two differenced columns, and no integer-expression ordinal survives in any of these reads.
    /// </summary>
    [Fact]
    public void TheKernelRead_RanksByCpu_NotByAConstant()
    {
        var sql = DarlingPgKernelStatsReader.PgKernelStatsSql;
        Assert.Contains("ORDER BY user_ms + system_ms DESC", sql, StringComparison.Ordinal);

        foreach (var (name, read) in PercentReads)
        {
            Assert.False(Regex.IsMatch(read, @"ORDER BY\s+\d+\s*[-+*/]\s*\d+"),
                $"{name} orders by an arithmetic expression on ordinals, which PostgreSQL folds to a constant and ignores");
        }
    }

    /// <summary>
    /// The sampling read cannot name its <c>sample_count</c> alias inside a window function of the same
    /// level, so the differencing CASE is written twice — once as the row figure, once inside the
    /// <c>SUM(...) OVER ()</c>. Two copies of one expression drift; this holds them identical modulo
    /// whitespace, so the window total is provably the sum of the very figure the rows report.
    /// </summary>
    [Fact]
    public void TheSamplingRead_SumsTheSameCaseItReportsPerRow()
    {
        var cases = Regex.Matches(DarlingPgWaitSamplingReader.PgWaitSamplingSql, @"CASE WHEN n\.sample_count.*?\bEND\b", RegexOptions.Singleline)
            .Select(m => Regex.Replace(m.Value, @"\s+", " "))
            .ToArray();

        Assert.Equal(2, cases.Length);
        Assert.Equal(cases[0], cases[1]);
        Assert.Contains("coalesce(o.sample_count, 0)", cases[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// The positive half over the five tool bodies: the share divides by the page record's window total, the
    /// page's own sum reaches only a <c>returned_*</c> key, truncation is observed off the <c>limit + 1</c> fetch.
    /// </summary>
    [Fact]
    public void EveryPercentTool_DividesByTheWindowTotal_AndPublishesThePageSumAsReturned()
    {
        foreach (var (type, name, shareKey, totalKey, returnedKey) in PercentTools)
        {
            var body = Strip(ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), name));

            Assert.Contains("page.WindowTotal", body, StringComparison.Ordinal);
            Assert.True(PageTruncationObserved.IsMatch(body), $"{name}: truncation is not observed off the page record");
            Assert.Contains("limit + 1", body, StringComparison.Ordinal);
            Assert.Contains($"{returnedKey} =", body, StringComparison.Ordinal);
            Assert.Contains($"{totalKey} =", body, StringComparison.Ordinal);
            Assert.Contains($"{shareKey} =", body, StringComparison.Ordinal);

            foreach (Match local in PageSumLocal.Matches(body))
            {
                var sum = local.Groups[1].Value;
                Assert.False(ShareOverLocal(sum).IsMatch(body),
                    $"{name}: a share divides by `{sum}`, which is a sum of the rows fetched — divide by the page's window total");
                Assert.False(TotalFromLocal(sum).IsMatch(body),
                    $"{name}: a total_* key is `{sum}`, a sum of the rows fetched — publish it as returned_* and the window's as total_*");
            }
        }
    }

    /// <summary>
    /// The negative half, over EVERY tool body on both SKUs that takes a <c>limit</c> or <c>top</c>: no share
    /// anywhere divides by a sum of the rows fetched, and no <c>total_*</c> key is one. Scoped to paged tools
    /// because an unpaged read's sum over all its rows IS a total (<c>get_session_summary</c>,
    /// <c>get_plan_cache_stats</c>) and a census that flagged those would be asserting a rule the finding did
    /// not state.
    ///
    /// <para><b>One stated allowance.</b> <c>get_pg_database_stats</c> publishes <c>total_temp_files</c> /
    /// <c>total_temp_bytes</c> / <c>total_deadlocks</c> summed over its top-N page — beside <c>limit_reached</c>,
    /// a note that says the totals cover only the databases returned, a <c>database_count</c> the web tile
    /// labels "Databases returned", and a share it already spells <c>_of_returned</c>. Honest by disclosure
    /// rather than by name; moving it to the window idiom means relabelling the web tile's figures, which
    /// sits outside the lane that added this census. Named here so it fails loudly the day the allowance is
    /// no longer needed, rather than being carried silently.</para>
    /// </summary>
    [Fact]
    public void NoPagedTool_DividesByOrPublishesAPageSumAsATotal_OnEitherSku()
    {
        var allowedPageSummedTotals = new HashSet<string>(StringComparer.Ordinal)
        {
            "get_pg_database_stats:total_temp_files",
            "get_pg_database_stats:total_temp_bytes",
            "get_pg_database_stats:total_deadlocks",
        };
        var allowancesUsed = new HashSet<string>(StringComparer.Ordinal);
        var examined = 0;

        foreach (var (file, source) in AllMcpToolSources())
        {
            var marks = Regex.Matches(source, @"\[McpServerTool\(Name = ""([a-z_0-9]+)""");
            for (var i = 0; i < marks.Count; i++)
            {
                var end = i + 1 < marks.Count ? marks[i + 1].Index : source.Length;
                var body = source[marks[i].Index..end];
                if (!Regex.IsMatch(body, @"\bint\s+(limit|top)\b"))
                {
                    continue;
                }

                examined++;
                var toolName = marks[i].Groups[1].Value;
                var text = Strip(body);

                foreach (Match local in PageSumLocal.Matches(text))
                {
                    var sum = local.Groups[1].Value;
                    var share = ShareOverLocal(sum).Match(text);
                    Assert.False(share.Success,
                        $"{file} {toolName}: `{share.Value}` divides by a sum of the rows fetched — a page share under a share-of-total name");

                    var total = TotalFromLocal(sum).Match(text);
                    if (total.Success)
                    {
                        var key = toolName + ":" + Regex.Match(total.Value, @"\btotal_\w+").Value;
                        Assert.True(allowedPageSummedTotals.Contains(key),
                            $"{file} {toolName}: `{total.Value}` publishes a sum of the rows fetched under a total's name — publish the window's total, or name it returned_*");
                        allowancesUsed.Add(key);
                    }
                }
            }
        }

        /* Population controls: a sweep that parsed nothing passes for free, and an allowance nobody needs
           is a widened exemption waiting for a defect to hide under. 82 bodies at the time of writing. */
        Assert.True(examined >= 60, $"only {examined} paged tool bodies were examined across both SKUs; the marker or the signature pattern has stopped matching");
        Assert.True(allowedPageSummedTotals.SetEquals(allowancesUsed),
            "stated allowances no longer match what the sweep finds — remove the ones that are no longer needed: "
            + string.Join(", ", allowedPageSummedTotals.Except(allowancesUsed)));
    }

    /// <summary>The A7 matchers, witnessed against the defect as it shipped and the fix as it landed.</summary>
    [Fact]
    public void TheA7Discriminators_FlagTheDefectShapes_AndPassTheFixedOnes()
    {
        /* The defect, verbatim from the shipped statement tool. */
        const string defect = """
            var totalTimeMs = rows.Sum(r => r.TotalExecTimeMs);
            var result = rows.Take(limit).Select(r => new
            {
                pct_of_total_time = totalTimeMs > 0 ? Math.Round((double)r.TotalExecTimeMs / totalTimeMs * 100, 1) : 0,
            });
            return JsonSerializer.Serialize(new { total_exec_time_ms = totalTimeMs, });
            """;
        var local = Assert.Single(PageSumLocal.Matches(defect)).Groups[1].Value;
        Assert.Equal("totalTimeMs", local);
        Assert.Matches(ShareOverLocal(local), defect);
        Assert.Matches(TotalFromLocal(local), defect);
        /* The rounded form the wait tool used. */
        Assert.Matches(TotalFromLocal("totalWaitMs"), "total_wait_time_ms = Math.Round(totalWaitMs, 1),");
        /* A multi-line share, the sampling tool's shape. */
        Assert.Matches(ShareOverLocal("totalSamples"), "pct_of_samples = totalSamples > 0\n    ? Math.Round((double)r.SampleCount / totalSamples * 100, 1)\n    : 0,");

        /* The fix: the page sum feeds only returned_*, the share divides by the window total. */
        const string fixedShape = """
            var truncated = page.Rows.Count > limit;
            var windowTotalMs = page.WindowTotalExecTimeMs;
            var returnedMs = rows.Sum(r => r.TotalExecTimeMs);
            pct_of_total_time = windowTotalMs > 0 ? Math.Round((double)r.TotalExecTimeMs / windowTotalMs * 100, 1) : 0,
            total_exec_time_ms = windowTotalMs,
            returned_exec_time_ms = returnedMs,
            returned_pct_of_total = windowTotalMs > 0 ? Math.Round((double)returnedMs / windowTotalMs * 100, 1) : 0,
            """;
        var fixedLocal = Assert.Single(PageSumLocal.Matches(fixedShape)).Groups[1].Value;
        Assert.Equal("returnedMs", fixedLocal);
        Assert.DoesNotMatch(ShareOverLocal(fixedLocal), fixedShape);
        Assert.DoesNotMatch(TotalFromLocal(fixedLocal), fixedShape);
        Assert.Matches(PageTruncationObserved, fixedShape);

        Assert.Matches(WindowTotalColumn, "CAST(SUM(SUM(delta_total_exec_time_ms)) OVER () AS bigint) AS window_total_exec_time_ms");
        Assert.Matches(WindowTotalColumn, "SUM(SUM(delta_wait_time_us)) OVER () / 1000.0 AS window_total_wait_time_ms");
        Assert.Matches(WindowTotalColumn, "SUM(user_ms + system_ms) OVER () AS window_total_cpu_ms");
        Assert.DoesNotMatch(WindowTotalColumn, "GREATEST(reads - LAG(reads) OVER series, 0) AS d_reads");
    }

    /* ───────────────────────── #3541 A13: a filter is part of the query ───────────────────────── */

    /// <summary>
    /// The tools whose filters ran in C# AFTER the read — over a page the SQL had already cut, or over a
    /// whole-window read whose count was then published beside the filtered page. Each body, on both SKUs,
    /// must now hand every filter to its reader and never <c>.Where(</c> the rows between the read and the
    /// emit: a filter applied after the cut makes the page the filtered remainder of an unfiltered top-N,
    /// which can be EMPTY while the window holds matches, and a count taken before the filter is a total of
    /// a different population from the rows beside it.
    /// </summary>
    public static readonly (Type Tools, string ToolName, string LiteFile, string LiteToolName)[] FilterInQueryTools =
    [
        (typeof(DarlingMcpDataTools), "get_top_queries_by_cpu", "Lite/Mcp/McpQueryTools.cs", "get_top_queries_by_cpu"),
        (typeof(DarlingMcpSessionTools), "get_active_queries", "Lite/Mcp/McpSessionTools.cs", "get_active_queries"),
    ];

    /// <summary>A LINQ filter over the rows a reader returned — the shape that puts the cut before the filter.</summary>
    private static readonly Regex PostReadWhere = new(@"\.Where\(", RegexOptions.Compiled);

    /// <summary>A parameter read as its absolute value — the shape that answers a negative window with a
    /// positive one and says nothing.</summary>
    private static readonly Regex AbsOfParameter = new(@"\bMath\.Abs\(\s*(hours_back|hoursBack|days_back|daysBack|limit|top)\b", RegexOptions.Compiled);

    [Fact]
    public void NoFilteredTool_FiltersItsRowsAfterTheRead_OnEitherSku()
    {
        foreach (var (type, darlingName, liteFile, liteName) in FilterInQueryTools)
        {
            foreach (var (label, body) in new[]
            {
                ($"Darling {darlingName}", ToolBody(ReadRepoFileLf(DarlingFileOf(type).Split('/')), darlingName)),
                ($"Lite {liteName}", ToolBody(ReadRepoFileLf(liteFile.Split('/')), liteName)),
            })
            {
                var text = Strip(body);
                var hit = PostReadWhere.Match(text);
                Assert.False(hit.Success,
                    $"{label}: `{hit.Value}` filters the rows in C# after the read — push the predicate into the SQL so the page is the top-N of the filtered population, and the count beside it counts that population");
                /* And the filter's presence is STATED on the payload, so a stored result says what shaped it. */
                Assert.True(
                    text.Contains("filter_applied", StringComparison.Ordinal) || text.Contains("filters_applied", StringComparison.Ordinal),
                    $"{label}: the payload never names the filter that shaped its population");
            }
        }
    }

    /// <summary>
    /// The reader statements behind them carry the predicates: the parallelism floor as a HAVING term on the
    /// grouped population BEFORE the CPU ordering and the cap, and the session read's two filters as WHERE
    /// terms with the population counted on the same statement above a parameterised LIMIT.
    /// </summary>
    [Fact]
    public void TheFilteredReads_CarryTheirPredicates_BeforeTheOrderingAndTheCap()
    {
        foreach (var (name, sql) in new[]
        {
            (nameof(DarlingDataReader.TopQueriesSql), DarlingDataReader.TopQueriesSql),
            (nameof(DarlingDataReader.TopQueriesByHostObjectSql), DarlingDataReader.TopQueriesByHostObjectSql),
        })
        {
            var floor = sql.IndexOf("COALESCE(MAX(max_dop), 0) >= $6", StringComparison.Ordinal);
            var order = sql.IndexOf("ORDER BY SUM(delta_worker_time) DESC", StringComparison.Ordinal);
            Assert.True(floor >= 0, $"{name}: the parallelism floor is not in the statement");
            Assert.True(order > floor, $"{name}: the parallelism floor sits after the ranking, so it filters a ranked page rather than ranking a filtered population");
        }

        var active = DarlingSessionReader.ActiveQueriesSql;
        Assert.Contains("($5::text IS NULL OR w.database_name = $5)", active, StringComparison.Ordinal);
        Assert.Contains("(NOT $6::boolean OR w.blocking_session_id > 0 OR h.session_id IS NOT NULL)", active, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) OVER () AS population_count", active, StringComparison.Ordinal);
        /* The head-blocker keep: a WAITFOR row stays when a row in the SAME capture names it. */
        Assert.Contains("(w.query_text NOT LIKE 'WAITFOR%' OR h.session_id IS NOT NULL)", active, StringComparison.Ordinal);
        Assert.Contains("h.collection_time = w.collection_time", active, StringComparison.Ordinal);
        Assert.True(active.IndexOf("COUNT(*) OVER ()", StringComparison.Ordinal) < active.IndexOf("LIMIT $4", StringComparison.Ordinal),
            "the population count must be computed above the cap, or it counts the page");
    }

    /// <summary>
    /// No tool on either SKU reads a parameter as its absolute value. Three did (<c>hours_back</c> on the
    /// uncapped reads), and a caller who sent <c>-24</c> was answered about the last 24 hours with nothing to
    /// say the sign had flipped. Every tool body on both SKUs is swept, comments stripped, because the fix's
    /// own comments name the shape.
    /// </summary>
    [Fact]
    public void NoTool_ReadsAParameterAsItsAbsoluteValue_OnEitherSku()
    {
        var examined = 0;
        var offenders = new List<string>();
        foreach (var (file, source) in AllMcpToolSources())
        {
            var marks = Regex.Matches(source, @"\[McpServerTool\(Name = ""([a-z_0-9]+)""");
            for (var i = 0; i < marks.Count; i++)
            {
                var end = i + 1 < marks.Count ? marks[i + 1].Index : source.Length;
                var body = Strip(source[marks[i].Index..end]);
                examined++;
                var hit = AbsOfParameter.Match(body);
                if (hit.Success)
                {
                    offenders.Add($"{file} {marks[i].Groups[1].Value}: {hit.Value}");
                }
            }
        }

        Assert.True(examined >= 150, $"only {examined} tool bodies were examined across both SKUs; the marker has stopped matching");
        Assert.True(offenders.Count == 0,
            "these tools read a parameter as its absolute value — refuse the negative instead: " + string.Join("; ", offenders));
    }

    /// <summary>The three uncapped reads route their span through the shared refusal, on both SKUs.</summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", "get_collection_log")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", "get_current_waits_trend")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", "get_blocking_stats")]
    [InlineData("Lite/Mcp/McpHealthTools.cs", "get_collection_log")]
    [InlineData("Lite/Mcp/McpHealthTools.cs", "get_current_waits_trend")]
    [InlineData("Lite/Mcp/McpHealthTools.cs", "get_blocking_stats")]
    public void TheUncappedReads_RefuseANonPositiveSpan_ThroughTheSharedValidator(string file, string toolName)
    {
        var body = Strip(ToolBody(ReadRepoFileLf(file.Split('/')), toolName));
        Assert.Contains("McpHelpers.ValidateUncappedWindow(hours_back, as_of, out var windowEnd)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("McpHelpers.ResolveAsOf(", body, StringComparison.Ordinal);
    }

    /// <summary>The A13 matchers, witnessed against the defect as it shipped and the fix as it landed.</summary>
    [Fact]
    public void TheA13Discriminators_FlagTheDefectShapes_AndPassTheFixedOnes()
    {
        /* The defect, verbatim from the shipped queries tool: the page is cut in SQL, then filtered. */
        const string defectFilter = """
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name);
            var filtered = rows
                .Where(r => !(parallel_only || min_dop > 1) || (r.MaxDop > 1 && r.MaxDop >= (min_dop > 1 ? min_dop : 2)))
                .ToList();
            """;
        Assert.Matches(PostReadWhere, defectFilter);
        /* The fix: the floor is an argument to the read. */
        const string fixedFilter = """
            var minMaxDop = min_dop > 1 ? min_dop : parallel_only ? 2 : 0;
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name, rollUpByHostObject: rollUp, minMaxDop: minMaxDop);
            var result = rows.Select(r => new { max_dop = r.MaxDop });
            """;
        Assert.DoesNotMatch(PostReadWhere, fixedFilter);

        /* The defect, verbatim from the three uncapped reads. */
        Assert.Matches(AbsOfParameter, "            var start = end.AddHours(-Math.Abs(hours_back));");
        Assert.Matches(AbsOfParameter, "            var hours = Math.Abs(hours_back);");
        Assert.DoesNotMatch(AbsOfParameter, "            var start = end.AddHours(-hours_back);");
        /* A genuine absolute value of a MEASUREMENT is not a parameter flip and must pass. */
        Assert.DoesNotMatch(AbsOfParameter, "            var drift = Math.Abs(observed - expected);");
    }

    /// <summary>Every MCP tool source on both SKUs, LF-normalised, for the cross-SKU sweep. Through
    /// <see cref="RepoFile"/> so a worktree checkout resolves the same root every other pin uses.</summary>
    private static IEnumerable<(string File, string Source)> AllMcpToolSources()
    {
        foreach (var directory in new[] { "Darling/PerformanceMonitor.Darling.Service/Mcp", "Lite/Mcp" })
        {
            var root = RepoFile.PathTo(directory);
            foreach (var file in System.IO.Directory.EnumerateFiles(root, "*.cs").Order(StringComparer.Ordinal))
            {
                yield return (System.IO.Path.GetFileName(file), System.IO.File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal));
            }
        }
    }

    /* ───────────────────────── plumbing ───────────────────────── */

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

    private static HashSet<string> KeysOf(string body)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in ContractKeys.Matches(body))
        {
            if (m.Groups[1].Success) keys.Add(m.Groups[1].Value);
            else keys.Add("order:" + m.Groups[2].Value);
        }
        return keys;
    }

    /// <summary>Comments removed, so a comment that NAMES the defect shape (as the fix's comments do) is not
    /// read as an instance of it. Line and block comments only; string literals stay, because the payload
    /// keys under test are not in strings.</summary>
    private static string Strip(string source) =>
        Regex.Replace(Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the page contract: the same boundary PAIRS
/// <c>Lite.Tests/McpPageContractTests</c> asserts against DuckDB, here against live Postgres through the real
/// tool methods. <c>limit = N - 1</c> over N seeded rows must read truncated and <c>limit = N</c> must not,
/// because a window holding exactly <c>limit</c> rows is the case <c>count &gt;= limit</c> gets wrong.
/// </summary>
[Collection("live-postgres")]
public sealed class McpPageContractLivePostgresTests
{
    private const string ServerName = "darling-mcp-page-contract-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables =
    [
        "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks", "config_alert_log",
        "long_query_completions", "plan_correction", "waiting_tasks", "wait_stats",
        /* #3541 A7: the five PostgreSQL percent tools' tables. */
        "pg_statement_stats", "pg_wait_stats", "pg_wait_sampling", "pg_kernel_stats", "pg_io_stats",
    ];

    [Fact]
    public async Task PagedTools_ObserveTruncationAtTheBoundary_AndDescribeThePage_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live page-contract test.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            const string Db = "StackOverflow";

            /* Blocking: 3 XE rows on distinct pairs, plus 1 DMV row on a fourth pair — the merged population
               is 4, which neither arm reaches alone. */
            for (var i = 0; i < 3; i++)
            {
                var t = now.AddMinutes(-10 * i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, Db, 50 + i, 90, 8000L, "X", "SELECT 1", "UPDATE Posts SET Score = Score + 1",
                    i == 0 ? null : "<blocked-process-report><blocked-process><process spid=\"55\"/></blocked-process></blocked-process-report>", "dbo.Posts");
            }
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), now.AddMinutes(-5), ServerId, ServerName, -1, now.AddMinutes(-5), Db, 70, 80, 3000L, "S", "suspended", "dbo.Users", "SELECT 2", "WAITFOR DELAY '00:01'");

            var cut = await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, 24, 3);
            JsonAssert.Contains("\"events_returned\": 3", cut);
            JsonAssert.Contains("\"truncated\": true", cut);
            var whole = await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, 24, 4);
            JsonAssert.Contains("\"events_returned\": 4", whole);
            JsonAssert.Contains("\"truncated\": false", whole);
            Assert.DoesNotContain("total_events", whole, StringComparison.Ordinal);

            /* The XML page counts reports WITH XML: two of the three XE rows. */
            var xml = await DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, ServerName, 24, 2);
            JsonAssert.Contains("\"reports_returned\": 2", xml);
            JsonAssert.Contains("\"truncated\": false", xml);
            JsonAssert.Contains("\"truncated\": true", await DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, ServerName, 24, 1));

            /* Deadlocks: 3 rows, the newest without a graph. */
            for (var i = 0; i < 3; i++)
            {
                var t = now.AddMinutes(-15 * i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "process" + i, "DELETE FROM Posts",
                    i == 0 ? null : "<deadlock><victim-list><victimProcess id=\"process" + i + "\"/></victim-list><process-list><process id=\"process" + i + "\"><inputbuf>DELETE FROM Posts</inputbuf></process></process-list></deadlock>");
            }
            JsonAssert.Contains("\"truncated\": true", await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName, 24, 2));
            var deadlocks = await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName, 24, 3);
            JsonAssert.Contains("\"deadlocks_returned\": 3", deadlocks);
            JsonAssert.Contains("\"truncated\": false", deadlocks);
            var detail = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName, 24, 2);
            JsonAssert.Contains("\"deadlocks_returned\": 2", detail);
            JsonAssert.Contains("\"truncated\": false", detail);

            /* Alerts: 3 live + 2 dismissed. The default read hides two and says so; lifting the filter shows five. */
            for (var i = 0; i < 5; i++)
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, dismissed)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    now.AddMinutes(-5 * i), ServerId, ServerName, "High CPU", 92.5, 80.0, true, "email", i >= 3);
            var alerts = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 24, 3);
            JsonAssert.Contains("\"alerts_returned\": 3", alerts);
            JsonAssert.Contains("\"truncated\": false", alerts);
            JsonAssert.Contains("\"dismissed_excluded\": true", alerts);
            JsonAssert.Contains("\"dismissed_excluded_count\": 2", alerts);
            JsonAssert.Contains("\"truncated\": true", await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 24, 2));
            var lifted = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 24, 50, include_dismissed: true);
            JsonAssert.Contains("\"alerts_returned\": 5", lifted);
            JsonAssert.Contains("\"dismissed_excluded\": false", lifted);
            JsonAssert.Contains("\"dismissed\": true", lifted);

            /* Long queries: the slowest run is the OLDEST; a limit = 1 page must still hold it. */
            for (var i = 0; i < 4; i++)
            {
                var t = now.AddMinutes(-20 * i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "rpc_completed", Db, (long)(i + 1) * 1_000_000, "EXEC p" + i);
            }
            var slowest = await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 1);
            JsonAssert.Contains("\"duration_ms\": 4000", slowest);
            JsonAssert.Contains("\"truncated\": true", slowest);
            JsonAssert.Contains("\"order\": \"duration_ms_desc\"", slowest);
            JsonAssert.Contains("\"truncated\": false", await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 4));

            /* Plan corrections: 3 re-captures. */
            for (var i = 0; i < 3; i++)
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), now.AddMinutes(-5 * i), ServerId, ServerName, Db, "PR_1", "Active", 50);
            JsonAssert.Contains("\"truncated\": true", await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, ServerName, 24, 2));
            var corrections = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, ServerName, 24, 3);
            JsonAssert.Contains("\"recommendations_returned\": 3", corrections);
            JsonAssert.Contains("\"truncated\": false", corrections);

            /* Waiting tasks: 3 rows; the envelope now carries the window. */
            for (var i = 0; i < 3; i++)
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", CollectionIdGenerator.Next(), now.AddMinutes(-i), ServerId, ServerName, 55 + i, "LCK_M_X", 3000L, 60, null, Db);
            var tasks = await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 1, 2);
            JsonAssert.Contains("\"hours_back\": 1", tasks);
            JsonAssert.Contains("\"tasks_returned\": 2", tasks);
            JsonAssert.Contains("\"truncated\": true", tasks);
            JsonAssert.Contains("\"truncated\": false", await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 1, 3));

            /* Wait stats: 3 types; the cap binds to limit past the old 50 too, but 3 vs 2 is the boundary. */
            var i2 = 0;
            foreach (var w in new[] { "CXPACKET", "PAGEIOLATCH_SH", "LCK_M_X" })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), now, ServerId, ServerName, w, 10L, 1000L * (3 - i2++), 100L);
            var waits = await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 24, 2);
            JsonAssert.Contains("\"wait_types_returned\": 2", waits);
            JsonAssert.Contains("\"truncated\": true", waits);
            JsonAssert.Contains("\"truncated\": false", await DarlingMcpDataTools.GetWaitStats(postgres, ServerName, 24, 3));

            /* A window whose every alert was dismissed names the filter rather than calling itself quiet. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"UPDATE config_alert_log SET dismissed = TRUE WHERE server_id = {ServerId}");
            var allDismissed = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 24, 50);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(allDismissed));
            Assert.Contains("5 dismissed alert(s) were excluded", allDismissed, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3541 A7 against live PostgreSQL: the SQL that produces each window total is the thing the in-process
    /// tests cannot see, so it is executed here through the real tool methods. Three series per table at
    /// 600 / 300 / 100 — the whole window is 1,000 — read at <c>limit = 1</c>: the one row's share must be
    /// <b>60</b>, the published total must be the seeded window's 1,000, and <c>truncated</c> must be true;
    /// at <c>limit = 3</c> the shares must be 60 / 30 / 10, the page sum must equal the total, and
    /// <c>truncated</c> must be false. The pair is what separates the fix from the defect: a page that IS the
    /// window sums to 100 under either arithmetic.
    ///
    /// <para>The cumulative tables (<c>pg_wait_sampling</c>, <c>pg_kernel_stats</c>, <c>pg_io_stats</c>) and the
    /// block/WAL side of <c>pg_statement_stats</c> are seeded with TWO snapshots, because their reads difference
    /// newest against oldest and a single sample has no interval. The kernel series are seeded so the HOTTEST
    /// query has the HIGHEST <c>query_id</c>: under the read's old <c>ORDER BY 3 + 4</c> — a constant, not two
    /// ordinals — the page came back in <c>query_id</c> order and a <c>limit = 1</c> page held the coolest
    /// query; the assertion that the 60% row leads is what pins the ranking.</para>
    /// </summary>
    [Fact]
    public async Task PercentTools_ShareTheWindowNotThePage_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live percent-denominator test.");

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
            var t1 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            var t0 = t1.AddMinutes(-10);
            long[] series = [600, 300, 100];
            /* Spread over the int8 range and NOT in share order: the hottest series has the middle id, the
               coolest the smallest, so any read that falls back to id order shows it. */
            long[] queryIds = [42L, 7_000_000_000_000_000_001L, -4_185_925_123_159_566_327L];

            for (var i = 0; i < 3; i++)
            {
                /* Statements: the rate columns are stored deltas, so only the second snapshot carries them. */
                foreach (var (t, delta) in new[] { (t0, 0L), (t1, series[i]) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, storage_blks_read, orcache_blks_hit,
     temp_blks_read, temp_blks_written, wal_bytes, max_exec_peakmem_bytes, delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE, 100, 5000, 91.5, 250, 10, 5, 3, 2, 0, 0, 1000, 2097152, $6, $7, $8)",
                        CollectionIdGenerator.Next(), t, ServerId, ServerName, queryIds[i], delta > 0 ? 10L : 0L, delta, delta > 0 ? 100L : 0L);
                }

                /* Aurora waits: stored deltas, one snapshot is enough. Microseconds in the store. */
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us)
VALUES ($1, $2, $3, $4, $5, $6, 'IO', $7, 100, 9999999, 10, $8)",
                    CollectionIdGenerator.Next(), t1, ServerId, ServerName, i + 1, (long)(i + 100), "DataFileRead" + i, series[i] * 1000);

                /* Sampled waits: cumulative, oldest 100 -> newest 100 + share. */
                foreach (var (t, count) in new[] { (t0, 100L), (t1, 100L + series[i]) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count)
VALUES ($1, $2, $3, $4, 'IO', 'DataFileRead', $5, $6, 10, 1)",
                        CollectionIdGenerator.Next(), t, ServerId, ServerName, queryIds[i], count);
                }

                /* Kernel: cumulative user + system, oldest (100, 0) -> newest (100 + half the share, half the
                   share). Halves, because every seeded share is even and the arithmetic stays exact in a double. */
                foreach (var (t, user, system) in new[] { (t0, 100.0, 0.0), (t1, 100.0 + series[i] / 2.0, series[i] / 2.0) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_kernel_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since)
VALUES ($1, $2, $3, $4, 'app', $5, $6, $7, 0, 8192, 0, 0, 0, $8)",
                        CollectionIdGenerator.Next(), t, ServerId, ServerName, queryIds[i], user, system, t0.AddDays(-1));
                }

                /* I/O: cumulative reads and read time, three contexts of one backend type. */
                var context = i == 0 ? "normal" : i == 1 ? "vacuum" : "bulkread";
                foreach (var (t, reads, readTime) in new[] { (t0, 1000L, 100.0), (t1, 1000L + series[i], 100.0 + series[i] / 10.0) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes)
VALUES ($1, $2, $3, $4, 'client backend', 'relation', $5, $6, $7, 5, 1, 0, 0, 1, 0, 8192, 100, 0, 0, 0, 0, NULL, NULL, NULL, NULL)",
                        CollectionIdGenerator.Next(), t, ServerId, ServerName, context, reads, readTime);
                }
            }

            /* One assertion set, five tools. The hot row's id is asserted on the three per-query reads so a
               read that still ranks by id rather than by weight fails on the ROW, not only on the share. */
            var hot = queryIds[0].ToString(CultureInfo.InvariantCulture);

            AssertPercentPair(
                await DarlingMcpPgStatementTools.GetPgTopQueries(postgres, ServerName, 4, 1),
                await DarlingMcpPgStatementTools.GetPgTopQueries(postgres, ServerName, 4, 3),
                "queries_returned", "total_exec_time_ms", "returned_exec_time_ms", "returned_pct_of_total", "queries", "pct_of_total_time", hot);

            AssertPercentPair(
                await DarlingMcpPgWaitTools.GetPgWaitStats(postgres, ServerName, 4, 1),
                await DarlingMcpPgWaitTools.GetPgWaitStats(postgres, ServerName, 4, 3),
                "wait_events_returned", "total_wait_time_ms", "returned_wait_time_ms", "returned_pct_of_total", "waits", "pct_of_total_wait", hotQueryId: null);

            AssertPercentPair(
                await DarlingMcpPgWaitSamplingTools.GetPgWaitSampling(postgres, ServerName, 4, 1),
                await DarlingMcpPgWaitSamplingTools.GetPgWaitSampling(postgres, ServerName, 4, 3),
                "waits_returned", "total_samples", "returned_samples", "returned_pct_of_total", "waits", "pct_of_samples", hot);

            AssertPercentPair(
                await DarlingMcpPgKernelStatsTools.GetPgKernelStats(postgres, ServerName, 4, 1),
                await DarlingMcpPgKernelStatsTools.GetPgKernelStats(postgres, ServerName, 4, 3),
                "queries_returned", "total_cpu_ms", "returned_cpu_ms", "returned_pct_of_total", "queries", "pct_of_total_cpu", hot);

            AssertPercentPair(
                await DarlingMcpPgIoTools.GetPgIoStats(postgres, ServerName, 4, 1),
                await DarlingMcpPgIoTools.GetPgIoStats(postgres, ServerName, 4, 3),
                "combination_count", "total_reads", "returned_reads", "returned_pct_of_total_reads", "combinations", "pct_of_total_reads", hotQueryId: null);

            /* The second I/O denominator: read time, inferred as tracked because non-zero times are seeded
               and no configuration row says otherwise. 100 ms across the window, 60 on the one-row page. */
            var ioCut = JsonDocument.Parse(await DarlingMcpPgIoTools.GetPgIoStats(postgres, ServerName, 4, 1)).RootElement;
            Assert.True(ioCut.GetProperty("io_timing_tracked").GetBoolean());
            Assert.Equal(100.0, ioCut.GetProperty("total_read_time_ms").GetDouble());
            Assert.Equal(60.0, ioCut.GetProperty("returned_read_time_ms").GetDouble());
            Assert.Equal(60.0, ioCut.GetProperty("combinations")[0].GetProperty("pct_of_total_read_time").GetDouble());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The cut page and the whole page over one seeded window. Cut: one row, share 60, total 1,000, page sum
    /// 600, truncated. Whole: three rows at 60 / 30 / 10, page sum equal to the total, not truncated. And on
    /// the per-query reads, the cut page's row IS the hottest series by id.
    /// </summary>
    private static void AssertPercentPair(
        string cutJson, string wholeJson,
        string returnedCountKey, string totalKey, string returnedKey, string ratioKey, string rowsKey, string shareKey,
        string? hotQueryId)
    {
        var cut = JsonDocument.Parse(cutJson).RootElement;
        /* Data-bearing means the ROWS are there, not that `status` is absent: get_pg_io_stats carries
           status = "io_activity" on its data payload, so a classifier keyed on the presence of `status`
           called a correct 60% page a status envelope (first CI run of this test). */
        Assert.True(cut.TryGetProperty(rowsKey, out _), $"expected a data-bearing payload with `{rowsKey}`, got: " + cutJson);
        Assert.Equal(1, cut.GetProperty(returnedCountKey).GetInt32());
        Assert.True(cut.GetProperty("truncated").GetBoolean());
        Assert.Equal(1000.0, cut.GetProperty(totalKey).GetDouble());
        Assert.Equal(600.0, cut.GetProperty(returnedKey).GetDouble());
        Assert.Equal(60.0, cut.GetProperty(ratioKey).GetDouble());
        var cutRow = Assert.Single(cut.GetProperty(rowsKey).EnumerateArray());
        Assert.Equal(60.0, cutRow.GetProperty(shareKey).GetDouble());
        if (hotQueryId is not null)
        {
            Assert.Equal(hotQueryId, cutRow.GetProperty("queryid").GetString());
        }

        var whole = JsonDocument.Parse(wholeJson).RootElement;
        Assert.Equal(3, whole.GetProperty(returnedCountKey).GetInt32());
        Assert.False(whole.GetProperty("truncated").GetBoolean());
        Assert.Equal(1000.0, whole.GetProperty(totalKey).GetDouble());
        Assert.Equal(1000.0, whole.GetProperty(returnedKey).GetDouble());
        Assert.Equal(100.0, whole.GetProperty(ratioKey).GetDouble());
        Assert.Equal(new[] { 60.0, 30.0, 10.0 },
            whole.GetProperty(rowsKey).EnumerateArray().Select(r => r.GetProperty(shareKey).GetDouble()).ToArray());
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", Tables.Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
