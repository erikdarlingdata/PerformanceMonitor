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
    ];

    /// <summary>
    /// The source span of every paged tool on both SKUs — the census walks these BODIES, not whole files,
    /// for the <c>total_* = .Count</c> and <c>&gt;= limit</c> shapes. Whole files would sweep in tools with a
    /// different contract: <c>get_active_queries</c> publishes an UNBOUNDED window count beside <c>shown</c>
    /// (a real total, and #3541 A13's filter semantics are its own item), and <c>get_mute_rules</c>'
    /// <c>total_count</c> counts a whole set. Neither is a hidden cap, and a census that flagged them would be
    /// asserting a rule the finding did not state.
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

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var sql = string.Join(" ", Tables.Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
