/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3541 A3, Lite half: the six MCP tool groups whose readers carried a HIDDEN row cap now describe their
/// page as a page. The cap binds to the caller's <c>limit</c>; truncation is OBSERVED by fetching one row
/// past it, never inferred from <c>count == limit</c>; the page publishes its own time bounds; no page count
/// is called <c>total_*</c>; and the one hidden filter (<c>get_alert_history</c>'s <c>dismissed = FALSE</c>) is
/// stated and measured. Darling's twin pins the same contract against live Postgres; this pins it against a
/// real DuckDB through the real tool methods, because the defect lived in the SQL's <c>LIMIT</c> and a
/// helper-only test would pass with the plumbing missing.
///
/// <para><b>Every truncation assertion is a PAIR</b>: <c>limit = N - 1</c> must say truncated and
/// <c>limit = N</c> must say not-truncated, over the same N seeded rows. A window holding exactly
/// <c>limit</c> rows is the case <c>count &gt;= limit</c> gets wrong, and asserting only the truncated side
/// would pass under that inference.</para>
/// </summary>
public sealed class McpPageContractTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public McpPageContractTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpPageContract_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);

        /* The derived id, not a literal: seeding under a hand-picked number makes every read return
           nothing and the not-truncated half of every pair pass for the wrong reason. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /* ───────────────────────── the contract, per tool ───────────────────────── */

    [Fact]
    public async Task GetDeadlocks_TruncationIsObservedAtTheBoundary_AndTheOldestStampIsTheReach()
    {
        var now = WholeSecondsNow();
        for (var i = 0; i < 3; i++) await SeedDeadlockAsync(now.AddMinutes(-10 * i), withGraph: true);

        var cut = Parse(await McpBlockingTools.GetDeadlocks(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(cut, "deadlocks", returnedKey: "deadlocks_returned", returned: 2, truncated: true);
        Assert.Equal("deadlock_time_desc", cut.GetProperty("order").GetString());
        /* Newest-first: the page is the two newest, so its oldest stamp is 10 minutes back, not 20. */
        Assert.Equal(Stamp(now.AddMinutes(-10)), cut.GetProperty("oldest_returned_deadlock_time").GetString());
        Assert.Equal(Stamp(now), cut.GetProperty("newest_returned_deadlock_time").GetString());

        var whole = Parse(await McpBlockingTools.GetDeadlocks(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(whole, "deadlocks", "deadlocks_returned", returned: 3, truncated: false);
        Assert.Equal(Stamp(now.AddMinutes(-20)), whole.GetProperty("oldest_returned_deadlock_time").GetString());
    }

    /// <summary>
    /// The graph predicate lives in the SQL, so <c>limit</c> counts graphs. Seeded with the NEWEST row
    /// graph-less: the old C# filter over a capped fetch would have spent a slot on it, and at a small cap
    /// would have reported "no XML" for a window that holds two.
    /// </summary>
    [Fact]
    public async Task GetDeadlockDetail_LimitCountsGraphs_NotRows()
    {
        var now = WholeSecondsNow();
        await SeedDeadlockAsync(now, withGraph: false);
        await SeedDeadlockAsync(now.AddMinutes(-10), withGraph: true);
        await SeedDeadlockAsync(now.AddMinutes(-20), withGraph: true);

        var whole = Parse(await McpBlockingTools.GetDeadlockDetail(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(whole, "deadlocks", "deadlocks_returned", returned: 2, truncated: false);
        Assert.All(whole.GetProperty("deadlocks").EnumerateArray(),
            d => Assert.False(string.IsNullOrEmpty(d.GetProperty("deadlock_graph_xml").GetString())));

        var cut = Parse(await McpBlockingTools.GetDeadlockDetail(_dataService, _serverManager, ServerName, 24, 1));
        AssertPage(cut, "deadlocks", "deadlocks_returned", returned: 1, truncated: true);
    }

    /// <summary>
    /// #4198: deadlock_graph_xml is the wide field here — Darling's twin
    /// (<c>DarlingMcpDeadlockDetailBudgetLiveTests</c>) measured 120,454 bytes for 3 real production graphs
    /// (about 40 KB/graph). Plants five graphs near that width (the worst realistic default page, limit's
    /// default) and asserts the default call previews them under <see cref="McpResponseBudget.DefaultBytes"/>,
    /// and that <c>full_graph: true</c> opts back into the whole XML.
    /// </summary>
    [Fact]
    public async Task GetDeadlockDetail_Default_StaysUnderResponseBudget_WithFiveWideGraphs()
    {
        var now = WholeSecondsNow();
        var graphXml = BuildWideDeadlockGraphXml(approxLength: 42_000);
        for (var i = 0; i < 5; i++)
        {
            await ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                _nextId--, Naive(now.AddMinutes(-i)), _serverId, ServerName, Naive(now.AddMinutes(-i)), $"process{i}", "DELETE FROM Posts", graphXml);
        }

        var defaultJson = await McpBlockingTools.GetDeadlockDetail(_dataService, _serverManager, ServerName);
        var root = Parse(defaultJson);
        AssertPage(root, "deadlocks", "deadlocks_returned", returned: 5, truncated: false);
        Assert.All(root.GetProperty("deadlocks").EnumerateArray(), d =>
        {
            Assert.True(d.GetProperty("deadlock_graph_xml_truncated").GetBoolean());
            Assert.True(d.GetProperty("deadlock_graph_xml").GetString()!.Length < graphXml.Length);
        });

        var defaultBytes = System.Text.Encoding.UTF8.GetByteCount(defaultJson);
        Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_deadlock_detail's default call is {defaultBytes:N0} bytes over five planted {graphXml.Length:N0}-char graphs, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        var fullJson = await McpBlockingTools.GetDeadlockDetail(_dataService, _serverManager, ServerName, 24, 5, full_graph: true);
        var fullRoot = Parse(fullJson);
        Assert.All(fullRoot.GetProperty("deadlocks").EnumerateArray(), d =>
        {
            Assert.False(d.GetProperty("deadlock_graph_xml_truncated").GetBoolean());
            Assert.Equal(graphXml, d.GetProperty("deadlock_graph_xml").GetString());
        });
    }

    /// <summary>Builds an XML string near <paramref name="approxLength"/> characters, ASCII only so its
    /// length and its UTF-8 byte count stay close (production deadlock graphs are almost entirely ASCII:
    /// object names, wait-resource strings, T-SQL).</summary>
    private static string BuildWideDeadlockGraphXml(int approxLength)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("<deadlock><victim-list><victimProcess id=\"process0\"/></victim-list><process-list>");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append($"<process id=\"process{i}\" waitresource=\"KEY: 5:72057594057{i:D8}\"><inputbuf>UPDATE dbo.Posts SET Score = Score + 1 WHERE Id = {i};</inputbuf></process>");
            i++;
        }

        sb.Append("</process-list><resource-list><keylock objectname=\"StackOverflow.dbo.Posts\"/></resource-list></deadlock>");
        return sb.ToString();
    }

    [Fact]
    public async Task GetBlockedProcessReports_TruncationIsObservedAtTheBoundary()
    {
        var now = WholeSecondsNow();
        for (var i = 0; i < 4; i++) await SeedBlockedProcessReportAsync(now.AddMinutes(-10 * i), blockedSpid: 50 + i, withXml: true);

        var cut = Parse(await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(cut, "reports", "reports_returned", returned: 3, truncated: true);
        Assert.Equal("event_time_desc", cut.GetProperty("order").GetString());
        Assert.Equal(Stamp(now.AddMinutes(-20)), cut.GetProperty("oldest_returned_event_time").GetString());
        Assert.Equal(Stamp(now), cut.GetProperty("newest_returned_event_time").GetString());

        var whole = Parse(await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, ServerName, 24, 4));
        AssertPage(whole, "reports", "reports_returned", returned: 4, truncated: false);
    }

    /// <summary>
    /// The DMV fallback arm must not hide a surplus: XE rows and DMV rows on DIFFERENT pairs merge to a
    /// population larger than either arm, and a cap below that population has to read as truncated even
    /// though neither arm alone exceeds it.
    /// </summary>
    [Fact]
    public async Task GetBlockedProcessReports_ObservesTruncationAcrossTheMergedArms()
    {
        var now = WholeSecondsNow();
        await SeedBlockedProcessReportAsync(now, blockedSpid: 50, withXml: true);
        await SeedBlockedProcessReportAsync(now.AddMinutes(-10), blockedSpid: 51, withXml: true);
        await SeedDmvBlockingSnapshotAsync(now.AddMinutes(-5), blockedSpid: 70);
        await SeedDmvBlockingSnapshotAsync(now.AddMinutes(-15), blockedSpid: 71);

        var cut = Parse(await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(cut, "reports", "reports_returned", returned: 3, truncated: true);

        var whole = Parse(await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, ServerName, 24, 4));
        AssertPage(whole, "reports", "reports_returned", returned: 4, truncated: false);
    }

    [Fact]
    public async Task GetBlockedProcessXml_LimitCountsReportsWithXml_NotRows()
    {
        var now = WholeSecondsNow();
        await SeedBlockedProcessReportAsync(now, blockedSpid: 50, withXml: false);
        await SeedBlockedProcessReportAsync(now.AddMinutes(-10), blockedSpid: 51, withXml: true);
        await SeedBlockedProcessReportAsync(now.AddMinutes(-20), blockedSpid: 52, withXml: true);
        /* A DMV row never carries a report and must not count against the page either. */
        await SeedDmvBlockingSnapshotAsync(now.AddMinutes(-1), blockedSpid: 70);

        var whole = Parse(await McpBlockingTools.GetBlockedProcessXml(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(whole, "reports", "reports_returned", returned: 2, truncated: false);
        Assert.All(whole.GetProperty("reports").EnumerateArray(),
            r => Assert.False(string.IsNullOrEmpty(r.GetProperty("blocked_process_report_xml").GetString())));

        var cut = Parse(await McpBlockingTools.GetBlockedProcessXml(_dataService, _serverManager, ServerName, 24, 1));
        AssertPage(cut, "reports", "reports_returned", returned: 1, truncated: true);
    }

    /// <summary>
    /// #4198: blocked_sql_text/blocking_sql_text are the wide fields here — Darling's twin
    /// (<c>DarlingMcpBlockingBudgetLiveTests</c>) measured 89,096 bytes at the old defaults on a real
    /// production store. Plants 30 reports with both text fields near 700-870 characters (a realistic
    /// blocked/blocking statement width, every other field populated) and asserts the default call
    /// previews them under <see cref="McpResponseBudget.DefaultBytes"/>, and that <c>full_text: true</c>
    /// opts back into the whole text.
    /// </summary>
    [Fact]
    public async Task GetBlockedProcessReports_Default_StaysUnderResponseBudget_WithThirtyWideReports()
    {
        var now = WholeSecondsNow();
        var blockedTexts = new string[30];
        var blockingTexts = new string[30];
        for (var i = 0; i < 30; i++)
        {
            blockedTexts[i] = BuildWideSqlText(700 + i * 6, "Orders");
            blockingTexts[i] = BuildWideSqlText(700 + i * 5, "Posts");
            await ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, wait_resource, lock_mode,
     blocked_status, blocked_isolation_level, blocked_log_used, blocked_transaction_count,
     blocked_client_app, blocked_host_name, blocked_login_name, blocked_sql_text,
     blocking_status, blocking_isolation_level, blocking_client_app, blocking_host_name, blocking_login_name, blocking_sql_text,
     blocked_transaction_name, blocking_transaction_name,
     blocked_last_tran_started, blocking_last_tran_started, blocked_last_batch_started, blocking_last_batch_started,
     blocked_last_batch_completed, blocking_last_batch_completed, blocked_priority, blocking_priority,
     contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38)",
                _nextId--, Naive(now.AddMinutes(-i)), _serverId, ServerName, Naive(now.AddMinutes(-i)), "Db",
                50 + i, 0, 90 + i, 0, 8000L + i, "KEY: 6:72057594057000000 (deadbeefcafe)", "X",
                "SUSPENDED", "READ COMMITTED", 256L, 1,
                "MyApp.exe", "APPSRV01", "svc_app", blockedTexts[i],
                "RUNNING", "READ COMMITTED", "MyApp.exe", "APPSRV02", "svc_app2", blockingTexts[i],
                "user_transaction", "user_transaction",
                Naive(now.AddMinutes(-i - 5)), Naive(now.AddMinutes(-i - 5)), Naive(now.AddMinutes(-i - 1)), Naive(now.AddMinutes(-i - 1)),
                Naive(now.AddMinutes(-i)), Naive(now.AddMinutes(-i)), 7, 3,
                "dbo.Orders");
        }

        var defaultJson = await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, ServerName);
        var root = Parse(defaultJson);
        AssertPage(root, "reports", "reports_returned", returned: 15, truncated: true);
        Assert.All(root.GetProperty("reports").EnumerateArray(), r =>
        {
            Assert.True(r.GetProperty("blocked_sql_text_truncated").GetBoolean());
            Assert.True(r.GetProperty("blocking_sql_text_truncated").GetBoolean());
            Assert.True(r.GetProperty("blocked_sql_text").GetString()!.Length < 700);
            Assert.True(r.GetProperty("blocking_sql_text").GetString()!.Length < 700);
        });

        var defaultBytes = System.Text.Encoding.UTF8.GetByteCount(defaultJson);
        Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_blocked_process_reports's default call is {defaultBytes:N0} bytes over 30 planted ~700-870-char reports, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        var fullJson = await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, ServerName, 24, 30, full_text: true);
        var fullRoot = Parse(fullJson);
        AssertPage(fullRoot, "reports", "reports_returned", returned: 30, truncated: false);
        var fullReports = fullRoot.GetProperty("reports").EnumerateArray().ToList();
        for (var i = 0; i < 30; i++)
        {
            Assert.False(fullReports[i].GetProperty("blocked_sql_text_truncated").GetBoolean());
            Assert.False(fullReports[i].GetProperty("blocking_sql_text_truncated").GetBoolean());
            Assert.Equal(blockedTexts[i], fullReports[i].GetProperty("blocked_sql_text").GetString());
            Assert.Equal(blockingTexts[i], fullReports[i].GetProperty("blocking_sql_text").GetString());
        }
    }

    /// <summary>Builds a SQL statement string near <paramref name="approxLength"/> characters, ASCII only so its
    /// length and its UTF-8 byte count stay equal (production blocked/blocking statement text is almost
    /// entirely ASCII: object names, predicates, literals).</summary>
    private static string BuildWideSqlText(int approxLength, string table)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append($"UPDATE dbo.{table} SET Status = 'Processing' WHERE ");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append($"{table}Id = {i} OR ");
            i++;
        }

        sb.Length = approxLength;
        return sb.ToString();
    }

    /// <summary>
    /// The hidden filter, stated and measured. Default read: dismissed rows are gone, the payload says so
    /// and says HOW MANY; <c>include_dismissed</c> brings them back with each row labelled. Truncation is
    /// then asserted over the FILTERED population, because that is what the page is drawn from.
    /// </summary>
    [Fact]
    public async Task GetAlertHistory_StatesTheDismissedFilter_MeasuresIt_AndCanLiftIt()
    {
        var now = WholeSecondsNow();
        for (var i = 0; i < 3; i++) await SeedAlertAsync(now.AddMinutes(-5 * i), dismissed: false);
        for (var i = 3; i < 5; i++) await SeedAlertAsync(now.AddMinutes(-5 * i), dismissed: true);

        var whole = Parse(await McpAlertTools.GetAlertHistory(_dataService, 24, 3));
        AssertPage(whole, "alerts", "alerts_returned", returned: 3, truncated: false);
        Assert.True(whole.GetProperty("dismissed_excluded").GetBoolean());
        Assert.Equal(2, whole.GetProperty("dismissed_excluded_count").GetInt64());
        Assert.Equal("alert_time_desc", whole.GetProperty("order").GetString());
        Assert.All(whole.GetProperty("alerts").EnumerateArray(), a => Assert.False(a.GetProperty("dismissed").GetBoolean()));

        var cut = Parse(await McpAlertTools.GetAlertHistory(_dataService, 24, 2));
        AssertPage(cut, "alerts", "alerts_returned", returned: 2, truncated: true);

        var lifted = Parse(await McpAlertTools.GetAlertHistory(_dataService, 24, 50, include_dismissed: true));
        AssertPage(lifted, "alerts", "alerts_returned", returned: 5, truncated: false);
        Assert.False(lifted.GetProperty("dismissed_excluded").GetBoolean());
        Assert.Equal(0, lifted.GetProperty("dismissed_excluded_count").GetInt64());
        Assert.Equal(2, lifted.GetProperty("alerts").EnumerateArray().Count(a => a.GetProperty("dismissed").GetBoolean()));
        /* The lifted page reaches the dismissed rows, which are the OLDEST two; the default page cannot. */
        Assert.Equal(Stamp(now.AddMinutes(-20)), lifted.GetProperty("oldest_returned_alert_time").GetString());
        Assert.Equal(Stamp(now.AddMinutes(-10)), whole.GetProperty("oldest_returned_alert_time").GetString());
    }

    /// <summary>
    /// A window whose every alert was dismissed is not a quiet window, and the empty answer has to say so
    /// rather than borrow the quiet-window sentence and send the caller off widening a window whose
    /// contents they were never shown.
    /// </summary>
    [Fact]
    public async Task GetAlertHistory_AllDismissed_EmptyAnswerNamesTheFilter()
    {
        var now = WholeSecondsNow();
        await SeedAlertAsync(now, dismissed: true);
        await SeedAlertAsync(now.AddMinutes(-5), dismissed: true);

        var root = Parse(await McpAlertTools.GetAlertHistory(_dataService, 24, 50));
        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("2 dismissed alert(s) were excluded", message, StringComparison.Ordinal);
        Assert.Contains("include_dismissed", message, StringComparison.Ordinal);

        /* And with nothing dismissed either, the plain sentence — the count is a measurement, not a hedge. */
        await ExecAsync("DELETE FROM config_alert_log");
        var quiet = Parse(await McpAlertTools.GetAlertHistory(_dataService, 24, 50));
        Assert.Equal("empty", quiet.GetProperty("status").GetString());
        Assert.DoesNotContain("dismissed", quiet.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>
    /// The SKU-parity half of the finding. Lite used to take the window's NEWEST 200 rows and re-rank them
    /// by duration in C#, so the window's slowest run could be omitted if it was not among the newest. The
    /// population is now the window's SLOWEST, ranked in SQL, as Darling's is. Seeded so the slowest run is
    /// the OLDEST: under the old read a <c>limit = 1</c> page would still have found it here (five rows fit
    /// in 200), so the assertion that carries the parity claim is the ORDER — slowest first regardless of
    /// age — plus the description's own word for the population.
    /// </summary>
    [Fact]
    public async Task GetLongQueryCompletions_PageIsTheWindowsSlowest_NotItsNewest()
    {
        var now = WholeSecondsNow();
        /* Newest is fastest; the slowest run is 80 minutes back. */
        for (var i = 0; i < 5; i++) await SeedLongQueryAsync(now.AddMinutes(-20 * i), durationMicroseconds: (i + 1) * 1_000_000L);

        var top = Parse(await McpLongQueryTools.GetLongQueryCompletions(_dataService, _serverManager, ServerName, 24, 1));
        AssertPage(top, "completions", "completions_returned", returned: 1, truncated: true);
        Assert.Equal("duration_ms_desc", top.GetProperty("order").GetString());
        Assert.Equal(5000.0, top.GetProperty("completions")[0].GetProperty("duration_ms").GetDouble());
        /* Under a duration RANKING the two stamps bound the slowest run, not the reach — both are the
           80-minute-old row, and the description says that is what they mean here. */
        Assert.Equal(Stamp(now.AddMinutes(-80)), top.GetProperty("oldest_returned_event_time").GetString());
        Assert.Equal(Stamp(now.AddMinutes(-80)), top.GetProperty("newest_returned_event_time").GetString());

        var whole = Parse(await McpLongQueryTools.GetLongQueryCompletions(_dataService, _serverManager, ServerName, 24, 5));
        AssertPage(whole, "completions", "completions_returned", returned: 5, truncated: false);
        var durations = whole.GetProperty("completions").EnumerateArray().Select(c => c.GetProperty("duration_ms").GetDouble()).ToArray();
        Assert.Equal(durations.OrderByDescending(d => d).ToArray(), durations);
    }

    [Fact]
    public async Task GetPlanCorrections_TruncationIsObservedAtTheBoundary_AndTheOldestStampIsTheReach()
    {
        var now = WholeSecondsNow();
        for (var i = 0; i < 3; i++) await SeedPlanCorrectionAsync(now.AddMinutes(-5 * i));

        var cut = Parse(await McpPlanCorrectionTools.GetPlanCorrections(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(cut, "recommendations", "recommendations_returned", returned: 2, truncated: true);
        Assert.Equal("collection_time_desc", cut.GetProperty("order").GetString());
        Assert.Equal(Stamp(now.AddMinutes(-5)), cut.GetProperty("oldest_returned_collection_time").GetString());

        var whole = Parse(await McpPlanCorrectionTools.GetPlanCorrections(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(whole, "recommendations", "recommendations_returned", returned: 3, truncated: false);
        Assert.Equal(Stamp(now.AddMinutes(-10)), whole.GetProperty("oldest_returned_collection_time").GetString());
    }

    [Fact]
    public async Task GetWaitingTasks_EnvelopeCarriesTheWindowAndThePageBounds()
    {
        var now = WholeSecondsNow();
        for (var i = 0; i < 3; i++) await SeedWaitingTaskAsync(now.AddMinutes(-3 * i));

        var cut = Parse(await McpWaitTools.GetWaitingTasks(_dataService, _serverManager, ServerName, 1, 2));
        AssertPage(cut, "tasks", "tasks_returned", returned: 2, truncated: true);
        /* The envelope used to be bare: server and rows. The span requested is now on it. */
        Assert.Equal(1, cut.GetProperty("hours_back").GetInt32());
        Assert.Equal("collection_time_desc", cut.GetProperty("order").GetString());
        Assert.Equal(Stamp(now.AddMinutes(-3)), cut.GetProperty("oldest_returned_collection_time").GetString());

        var whole = Parse(await McpWaitTools.GetWaitingTasks(_dataService, _serverManager, ServerName, 1, 3));
        AssertPage(whole, "tasks", "tasks_returned", returned: 3, truncated: false);
    }

    [Fact]
    public async Task GetWaitStats_CapBindsToLimit_AndTruncationIsObserved()
    {
        var now = WholeSecondsNow().AddMinutes(-1);
        await SeedWaitStatAsync(now, "CXPACKET", 4000);
        await SeedWaitStatAsync(now, "PAGEIOLATCH_SH", 3000);
        await SeedWaitStatAsync(now, "LCK_M_X", 2000);

        var cut = Parse(await McpWaitTools.GetWaitStats(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(cut, "waits", "wait_types_returned", returned: 2, truncated: true);
        Assert.Equal("total_wait_time_ms_desc", cut.GetProperty("order").GetString());
        /* The page is the heaviest, so the type past the cap is the lightest. */
        Assert.Equal(new[] { "CXPACKET", "PAGEIOLATCH_SH" },
            cut.GetProperty("waits").EnumerateArray().Select(w => w.GetProperty("wait_type").GetString()).ToArray());

        var whole = Parse(await McpWaitTools.GetWaitStats(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(whole, "waits", "wait_types_returned", returned: 3, truncated: false);
    }

    /// <summary>
    /// #3653 (the #3541 A3 class on Lite): the two latest-snapshot readers carried a <c>LIMIT 20</c> literal and
    /// the tools published <c>latch_count</c> / <c>spinlock_count</c> as the snapshot's population. Now the cap
    /// binds to <c>limit</c> and truncation is OBSERVED at the boundary — the pair below is over the same
    /// three seeded rows, and a <c>count &gt;= limit</c> inference would fail its not-truncated half. An older
    /// snapshot is seeded too, so the page is the NEWEST snapshot's rows and not a mixture (the reader pins one
    /// <c>collection_time</c>); the cap counts rows in that snapshot, not rows in the window.
    /// </summary>
    [Fact]
    public async Task GetLatchStats_CapBindsToLimit_AndTruncationIsObserved()
    {
        var now = WholeSecondsNow().AddMinutes(-1);
        await SeedLatchAsync(now.AddMinutes(-10), "ACCESS_METHODS_DATASET_PARENT", 99_000);
        await SeedLatchAsync(now, "ACCESS_METHODS_DATASET_PARENT", 4000);
        await SeedLatchAsync(now, "BUFFER", 3000);
        await SeedLatchAsync(now, "LOG_MANAGER", 2000);

        var cut = Parse(await McpLatchSpinlockTools.GetLatchStats(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(cut, "latches", "latches_returned", returned: 2, truncated: true);
        Assert.Equal("delta_wait_time_ms_desc", cut.GetProperty("order").GetString());
        Assert.Equal(Stamp(now), cut.GetProperty("captured_at").GetString());
        /* The page is the heaviest of the NEWEST snapshot: the hot older row is not in it, the lightest class is
           the one past the cap. */
        Assert.Equal(new[] { "ACCESS_METHODS_DATASET_PARENT", "BUFFER" },
            cut.GetProperty("latches").EnumerateArray().Select(l => l.GetProperty("latch_class").GetString()).ToArray());
        Assert.Equal(4000, cut.GetProperty("latches")[0].GetProperty("delta_wait_time_ms").GetInt64());

        var whole = Parse(await McpLatchSpinlockTools.GetLatchStats(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(whole, "latches", "latches_returned", returned: 3, truncated: false);

        /* The default IS the grid's cap, named once — a caller who sends nothing reads what the grid reads. */
        Assert.Equal(20, LocalDataService.LatchSpinlockGridRowCap);
        Assert.Equal(LocalDataService.LatchSpinlockGridRowCap,
            (int)ToolMethod(typeof(McpLatchSpinlockTools), "get_latch_stats").GetParameters().Single(p => p.Name == "limit").DefaultValue!);
    }

    /// <summary>
    /// #3653 A7 through the shared row: a restart / first-sample row (stored interval 0, the calculator's
    /// marker) publishes <c>null</c> for both deltas and for the average — a caller could otherwise read the
    /// fabricated (0, 0) as "a quiet latch" (#3642's zero-is-a-measurement rule reaching this twin). The
    /// measured row beside it publishes its numbers as before, and the marker still ranks (the reader ranks
    /// the STORED deltas) so it is not hidden from the page either.
    /// <para>#3653 A16 — the unknowable row spelled the way Darling's twin spells it: <c>interval_seconds</c>
    /// beside the deltas (the WHY a null delta could not say on its own) and the two per-second rates Darling
    /// derives from its latest interval, all three null on the marker and real on the measured row —
    /// 4,000 ms / 100 requests over a stored 60 s is 66.67 ms/s and 1.67 waits/s, the delta over the STORED
    /// interval, not over any spacing. A third state is seeded so the two nulls are not one: a pre-v60 row
    /// that never stored an interval keeps its deltas (they are real) and nulls only the interval and the
    /// rates, because a single snapshot row has nothing to LAG against and the rate is not invented.</para>
    /// </summary>
    [Fact]
    public async Task GetLatchStats_PublishesNullDeltas_OnTheRestartMarkerRow()
    {
        var now = DateTime.UtcNow;
        await SeedLatchAsync(now, "LOG_MANAGER", 4000);
        await SeedLatchAsync(now, "BUFFER", 0, intervalSeconds: 0);
        await SeedLatchAsync(now, "FGCB_ADD_REMOVE", 2000, intervalSeconds: null);

        var page = Parse(await McpLatchSpinlockTools.GetLatchStats(_dataService, _serverManager, ServerName, 24, 10));
        var latches = page.GetProperty("latches").EnumerateArray().ToDictionary(l => l.GetProperty("latch_class").GetString()!);
        Assert.Equal(3, latches.Count);

        /* The marker: every key that spells the row is null, the deltas and the why alike. */
        foreach (var key in new[] { "delta_wait_time_ms", "delta_waiting_requests_count", "avg_wait_ms_per_request", "interval_seconds", "waits_per_second", "wait_ms_per_second" })
        {
            Assert.Equal(JsonValueKind.Null, latches["BUFFER"].GetProperty(key).ValueKind);
        }

        /* The measured row: the deltas as before, and the rates over the STORED interval. */
        Assert.Equal(4000, latches["LOG_MANAGER"].GetProperty("delta_wait_time_ms").GetInt64());
        Assert.Equal(100, latches["LOG_MANAGER"].GetProperty("delta_waiting_requests_count").GetInt64());
        Assert.Equal(40.0, latches["LOG_MANAGER"].GetProperty("avg_wait_ms_per_request").GetDouble());
        Assert.Equal(60, latches["LOG_MANAGER"].GetProperty("interval_seconds").GetInt32());
        Assert.Equal(66.67, latches["LOG_MANAGER"].GetProperty("wait_ms_per_second").GetDouble());
        Assert.Equal(1.67, latches["LOG_MANAGER"].GetProperty("waits_per_second").GetDouble());

        /* The pre-v60 row: real deltas, no interval to rate them over — the two nulls are not the same state. */
        Assert.Equal(2000, latches["FGCB_ADD_REMOVE"].GetProperty("delta_wait_time_ms").GetInt64());
        Assert.Equal(20.0, latches["FGCB_ADD_REMOVE"].GetProperty("avg_wait_ms_per_request").GetDouble());
        Assert.Equal(JsonValueKind.Null, latches["FGCB_ADD_REMOVE"].GetProperty("interval_seconds").ValueKind);
        Assert.Equal(JsonValueKind.Null, latches["FGCB_ADD_REMOVE"].GetProperty("wait_ms_per_second").ValueKind);
        Assert.Equal(JsonValueKind.Null, latches["FGCB_ADD_REMOVE"].GetProperty("waits_per_second").ValueKind);
    }

    /// <summary>The spinlock twin of the test above (#3653 A16): the marker row null on the deltas, the
    /// rates and <c>interval_seconds</c>; the measured row rated over its STORED 60 s (4,000 collisions and
    /// the seed's 1,000 spins → 66.67 and 16.67 per second); the pre-v60 row keeping its deltas with the
    /// interval and the rates null.</summary>
    [Fact]
    public async Task GetSpinlockStats_PublishesNullDeltasAndRates_OnTheRestartMarkerRow()
    {
        var now = DateTime.UtcNow;
        await SeedSpinlockAsync(now, "LOCK_HASH", 4000);
        await SeedSpinlockAsync(now, "SOS_CACHESTORE", 0, intervalSeconds: 0);
        await SeedSpinlockAsync(now, "XDESMGR", 2000, intervalSeconds: null);

        var page = Parse(await McpLatchSpinlockTools.GetSpinlockStats(_dataService, _serverManager, ServerName, 24, 10));
        var spinlocks = page.GetProperty("spinlocks").EnumerateArray().ToDictionary(l => l.GetProperty("spinlock_name").GetString()!);
        Assert.Equal(3, spinlocks.Count);

        foreach (var key in new[] { "delta_collisions", "delta_spins", "interval_seconds", "collisions_per_second", "spins_per_second" })
        {
            Assert.Equal(JsonValueKind.Null, spinlocks["SOS_CACHESTORE"].GetProperty(key).ValueKind);
        }

        Assert.Equal(4000, spinlocks["LOCK_HASH"].GetProperty("delta_collisions").GetInt64());
        Assert.Equal(1000, spinlocks["LOCK_HASH"].GetProperty("delta_spins").GetInt64());
        Assert.Equal(60, spinlocks["LOCK_HASH"].GetProperty("interval_seconds").GetInt32());
        Assert.Equal(66.67, spinlocks["LOCK_HASH"].GetProperty("collisions_per_second").GetDouble());
        Assert.Equal(16.67, spinlocks["LOCK_HASH"].GetProperty("spins_per_second").GetDouble());

        Assert.Equal(2000, spinlocks["XDESMGR"].GetProperty("delta_collisions").GetInt64());
        Assert.Equal(JsonValueKind.Null, spinlocks["XDESMGR"].GetProperty("interval_seconds").ValueKind);
        Assert.Equal(JsonValueKind.Null, spinlocks["XDESMGR"].GetProperty("collisions_per_second").ValueKind);
        Assert.Equal(JsonValueKind.Null, spinlocks["XDESMGR"].GetProperty("spins_per_second").ValueKind);
    }

    [Fact]
    public async Task GetSpinlockStats_CapBindsToLimit_AndTruncationIsObserved()
    {
        var now = WholeSecondsNow().AddMinutes(-1);
        await SeedSpinlockAsync(now.AddMinutes(-10), "LOCK_HASH", 99_000);
        await SeedSpinlockAsync(now, "LOCK_HASH", 4000);
        await SeedSpinlockAsync(now, "SOS_CACHESTORE", 3000);
        await SeedSpinlockAsync(now, "XDESMGR", 2000);

        var cut = Parse(await McpLatchSpinlockTools.GetSpinlockStats(_dataService, _serverManager, ServerName, 24, 2));
        AssertPage(cut, "spinlocks", "spinlocks_returned", returned: 2, truncated: true);
        Assert.Equal("delta_collisions_desc", cut.GetProperty("order").GetString());
        Assert.Equal(Stamp(now), cut.GetProperty("captured_at").GetString());
        Assert.Equal(new[] { "LOCK_HASH", "SOS_CACHESTORE" },
            cut.GetProperty("spinlocks").EnumerateArray().Select(l => l.GetProperty("spinlock_name").GetString()).ToArray());
        Assert.Equal(4000, cut.GetProperty("spinlocks")[0].GetProperty("delta_collisions").GetInt64());

        var whole = Parse(await McpLatchSpinlockTools.GetSpinlockStats(_dataService, _serverManager, ServerName, 24, 3));
        AssertPage(whole, "spinlocks", "spinlocks_returned", returned: 3, truncated: false);

        Assert.Equal(LocalDataService.LatchSpinlockGridRowCap,
            (int)ToolMethod(typeof(McpLatchSpinlockTools), "get_spinlock_stats").GetParameters().Single(p => p.Name == "limit").DefaultValue!);
    }

    /* ───────────────────────── #3541 A13: a filter is part of the query ───────────────────────── */

    /// <summary>
    /// <c>parallel_only</c> / <c>min_dop</c> used to be a <c>.Where</c> over the top-N page the SQL had already
    /// cut, so a box whose hottest N plans were serial answered an EMPTY page while the window held a parallel
    /// plan just past the cut. The fixture is that shape — three serial groups hotter than one parallel group,
    /// read at <c>top = 2</c>. The old code returned nothing; the fixed read returns the parallel group, and the
    /// unfiltered read at the same cap still returns the two hottest serial ones. Darling's twin proves the
    /// same pair against live Postgres.
    /// </summary>
    [Fact]
    public async Task GetTopQueriesByCpu_ParallelFilter_RanksTheFilteredPopulation_NotTheFilteredPage()
    {
        var now = WholeSecondsNow().AddMinutes(-2);
        await SeedQueryStatAsync(now, "0xSERIAL1", cpuUs: 900_000L, maxDop: 1);
        await SeedQueryStatAsync(now, "0xSERIAL2", cpuUs: 800_000L, maxDop: 1);
        await SeedQueryStatAsync(now, "0xSERIAL3", cpuUs: 700_000L, maxDop: 1);
        await SeedQueryStatAsync(now, "0xPARALLEL", cpuUs: 100_000L, maxDop: 8);

        var unfiltered = Parse(await McpQueryTools.GetTopQueriesByCpu(_dataService, _serverManager, ServerName, 1, 2));
        Assert.Equal(new[] { "0xSERIAL1", "0xSERIAL2" }, Hashes(unfiltered));
        Assert.Equal(JsonValueKind.Null, unfiltered.GetProperty("filter_applied").ValueKind);

        var parallel = Parse(await McpQueryTools.GetTopQueriesByCpu(_dataService, _serverManager, ServerName, 1, 2, parallel_only: true));
        Assert.Equal(new[] { "0xPARALLEL" }, Hashes(parallel));
        Assert.Contains("max_dop >= 2", parallel.GetProperty("filter_applied").GetString(), StringComparison.Ordinal);

        /* min_dop above the seeded DOP: an empty FILTERED page is the window's answer, not a collection miss. */
        var tooHigh = Parse(await McpQueryTools.GetTopQueriesByCpu(_dataService, _serverManager, ServerName, 1, 2, min_dop: 16));
        Assert.Equal("empty", tooHigh.GetProperty("status").GetString());
        Assert.Contains("max_dop >= 16", tooHigh.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// One capture holding the three blocker situations the tool now names: a victim whose blocker is a
    /// WAITFOR shell in the same capture (the classic head blocker — kept and flagged, where the old trim
    /// dropped it while the victim pointed at it), a victim whose blocker was never captured (an idle open
    /// transaction), and a victim in one database whose blocker is in another. A stale WAITFOR row under the
    /// same session id in an EARLIER capture must not be resurrected: the match is same capture, not same
    /// window. The count beside the page is the FILTERED population's, and truncation is observed on it.
    /// </summary>
    [Fact]
    public async Task GetActiveQueries_FiltersInTheQuery_KeepsHeadBlockers_AndNamesAbsentOnes()
    {
        var t = WholeSecondsNow().AddMinutes(-2);
        await SeedSnapshotAsync(t, 55, "Db", "UPDATE Posts SET Score = 1", blockingSessionId: 60, cpuMs: 500);
        await SeedSnapshotAsync(t, 60, "Db", "WAITFOR DELAY '00:05'", blockingSessionId: 0, cpuMs: 1);
        await SeedSnapshotAsync(t, 56, "Db", "DELETE FROM Votes", blockingSessionId: 61, cpuMs: 400);
        await SeedSnapshotAsync(t, 57, "OtherDb", "SELECT * FROM Sales", blockingSessionId: 62, cpuMs: 300);
        await SeedSnapshotAsync(t, 62, "Db", "UPDATE Users SET Reputation = 0", blockingSessionId: 0, cpuMs: 900);
        await SeedSnapshotAsync(t, 70, "Db", "SELECT COUNT(*) FROM Comments", blockingSessionId: 0, cpuMs: 200);
        await SeedSnapshotAsync(t.AddMinutes(-2), 60, "Db", "WAITFOR DELAY '00:05'", blockingSessionId: 0, cpuMs: 1);

        var all = Parse(await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, 1, limit: 50));
        var rows = all.GetProperty("queries").EnumerateArray().ToArray();
        Assert.Equal(6, rows.Length);
        Assert.Equal(6, all.GetProperty("total_snapshots").GetInt64());
        Assert.Equal(6, all.GetProperty("snapshots_returned").GetInt32());
        Assert.False(all.GetProperty("truncated").GetBoolean());
        Assert.Equal("collection_time_desc", all.GetProperty("order").GetString());
        Assert.Equal(Stamp(t), all.GetProperty("newest_returned_collection_time").GetString());

        var head = Row(rows, 60);
        Assert.True(head.GetProperty("is_head_blocker").GetBoolean());
        Assert.StartsWith("WAITFOR", head.GetProperty("query_text").GetString(), StringComparison.Ordinal);
        Assert.Equal(JsonValueKind.Null, Row(rows, 55).GetProperty("blocker_not_shown").ValueKind);
        Assert.Equal("not_captured", Row(rows, 56).GetProperty("blocker_not_shown").GetString());
        Assert.Equal(JsonValueKind.Null, Row(rows, 57).GetProperty("blocker_not_shown").ValueKind);
        Assert.Equal(JsonValueKind.Null, Row(rows, 70).GetProperty("is_head_blocker").ValueKind);

        /* database_name, in the query: the population is OtherDb's one victim; its blocker is in Db, so filtered. */
        var other = Parse(await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, 1, "OtherDb"));
        Assert.Equal(1, other.GetProperty("total_snapshots").GetInt64());
        Assert.Equal("filtered", Assert.Single(other.GetProperty("queries").EnumerateArray()).GetProperty("blocker_not_shown").GetString());

        /* blocking_only, in the query: victims 55/56/57 + heads 60/62 = 5; 70 is out. */
        var blocking = Parse(await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, 1, blocking_only: true));
        Assert.Equal(5, blocking.GetProperty("total_snapshots").GetInt64());
        Assert.DoesNotContain(blocking.GetProperty("queries").EnumerateArray(), r => r.GetProperty("session_id").GetInt32() == 70);

        /* Truncation on the FILTERED population, as a pair; the WAITFOR head (1 ms CPU) falls past a 4-row
           page and its victim says so. */
        var cut = Parse(await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, 1, blocking_only: true, limit: 4));
        Assert.True(cut.GetProperty("truncated").GetBoolean());
        Assert.Equal(4, cut.GetProperty("snapshots_returned").GetInt32());
        Assert.Equal(5, cut.GetProperty("total_snapshots").GetInt64());
        Assert.Equal("past_page", Row(cut.GetProperty("queries").EnumerateArray().ToArray(), 55).GetProperty("blocker_not_shown").GetString());
        var whole = Parse(await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, 1, blocking_only: true, limit: 5));
        Assert.False(whole.GetProperty("truncated").GetBoolean());

        /* A filtered miss names the filter rather than calling the window empty. */
        var miss = Parse(await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, 1, "NoSuchDb"));
        Assert.Equal("empty", miss.GetProperty("status").GetString());
        Assert.Contains("database_name 'NoSuchDb'", miss.GetProperty("message").GetString(), StringComparison.Ordinal);

        /* A13's third item: the uncapped reads refuse a negative span rather than flipping its sign — and the
           refusal is the `invalid` envelope (#3739), its sentence read back out here. */
        Assert.StartsWith("Invalid hours_back value '-24'", McpHelpers.ErrorMessageOf(await McpHealthTools.GetCollectionLog(_dataService, _serverManager, ServerName, -24)), StringComparison.Ordinal);
        Assert.StartsWith("Invalid hours_back value '0'", McpHelpers.ErrorMessageOf(await McpHealthTools.GetCurrentWaitsTrend(_dataService, _serverManager, ServerName, 0)), StringComparison.Ordinal);
        Assert.StartsWith("Invalid hours_back value '-1'", McpHelpers.ErrorMessageOf(await McpHealthTools.GetBlockingStats(_dataService, _serverManager, ServerName, -1)), StringComparison.Ordinal);
    }

    /// <summary>
    /// #4198: get_active_queries' own response-budget pin. Darling's twin
    /// (<c>DarlingMcpActiveQueriesBudgetLiveTests</c>) measured 81,489 bytes for a synthetic 50-row page
    /// shaped like a busy server (every optional field populated, a fifth of the rows carrying a long
    /// literal list) — this tool has TWENTY-THREE fields per row rather than one dominant wide field, so the
    /// fixed columns add up across the page even before query_text is counted. Plants the same shape and
    /// asserts the default call (limit down to 25) stays under <see cref="McpResponseBudget.DefaultBytes"/>,
    /// and that <c>full_text: true</c> (renamed from <c>full_query_text</c> to match
    /// <c>get_store_query_stats</c>) opts back into the whole text.
    /// </summary>
    [Fact]
    public async Task GetActiveQueries_Default_StaysUnderResponseBudget_WithFiftyRealisticRows()
    {
        var baseTime = WholeSecondsNow().AddMinutes(-49);
        var wideText = BuildQueryText(seed: 999, approxLength: 2_800);

        for (var i = 0; i < 50; i++)
        {
            var t = baseTime.AddMinutes(i);
            var db = i % 3 == 0 ? "StackOverflow" : i % 3 == 1 ? "AdventureWorks" : "ReportingDW";
            var text = i >= 40 ? wideText : BuildQueryText(seed: i, approxLength: 700);
            var hasWait = i % 4 == 0;

            await ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status,
     blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads,
     granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
                _nextId--, Naive(t), _serverId, ServerName, 100 + i, db,
                "00 00:00:05.125", text, i % 5 == 0 ? "suspended" : "running", 0,
                hasWait ? "PAGEIOLATCH_SH" : null, hasWait ? 250L + i : 0L, 1000L + (i * 37), 1500L + (i * 37),
                10_000L + (i * 123), 50L + i, i % 7, i % 6 == 0 ? 0.75 : 0,
                i % 2 == 0 ? "Read Committed" : "Repeatable Read", i % 6 == 0 ? 8 : 1, i % 6 == 0 ? 4 : 0,
                i % 2 == 0 ? "app_svc_prod" : @"CONTOSO\svc_reporting", $"APPSRV{i % 5:D2}",
                i % 2 == 0 ? ".Net SqlClient Data Provider" : "MyOrderService.Worker", i % 3 == 0 ? 1 : 0);
        }

        /* Not AssertPage: get_active_queries' total_snapshots is a deliberate exception to "no total_* key"
           (#3541 A13) — the FILTERED population's own size, not a window count. */
        var defaultJson = await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName);
        var root = Parse(defaultJson);
        Assert.Equal(25, root.GetProperty("queries").GetArrayLength());
        Assert.Equal(25, root.GetProperty("snapshots_returned").GetInt32());
        Assert.Equal(50, root.GetProperty("total_snapshots").GetInt64());
        Assert.True(root.GetProperty("truncated").GetBoolean());

        var defaultBytes = System.Text.Encoding.UTF8.GetByteCount(defaultJson);
        Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_active_queries' default call is {defaultBytes:N0} bytes over 50 planted rows, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        var wideRow = root.GetProperty("queries")[0];
        Assert.True(wideRow.GetProperty("query_text").GetString()!.Length < wideText.Length);
        Assert.True(wideRow.GetProperty("query_text_truncated").GetBoolean());

        var fullJson = await McpSessionTools.GetActiveQueries(_dataService, _serverManager, ServerName, full_text: true);
        var fullRoot = Parse(fullJson);
        Assert.False(fullRoot.GetProperty("queries")[0].GetProperty("query_text_truncated").GetBoolean());
        Assert.Equal(wideText, fullRoot.GetProperty("queries")[0].GetProperty("query_text").GetString());
    }

    /// <summary>Builds ASCII SQL text (a big literal IN-list, a realistic cause of an outsized capture) near
    /// <paramref name="approxLength"/> characters, so its length and its UTF-8 byte count stay close.</summary>
    private static string BuildQueryText(int seed, int approxLength)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount, c.CustomerName, c.Region FROM dbo.Orders o JOIN dbo.Customers c ON c.CustomerId = o.CustomerId WHERE o.OrderDate >= '2026-01-01' AND o.Status IN (");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append(seed * 100_000 + i);
            sb.Append(',');
            i++;
        }

        sb.Append(") ORDER BY o.OrderDate DESC;");
        return sb.ToString();
    }

    /* ───────────────────────── #3739: a refusal carries a status word ───────────────────────── */

    /// <summary>
    /// Every REFUSAL a Lite tool answers is <c>McpHelpers.Refusal</c>'s <c>{"status":"invalid", message,
    /// hints.parameter}</c> envelope — executed through the real tools against the real fixture rather than read
    /// off the helper, because the helper being right proves nothing about whether the tools reach it. Until
    /// #3739 each of these answered with the validator's bare sentence, the one outcome on the wire that was not
    /// JSON; the sentence is unchanged inside <c>message</c> (the fragments pinned above survive), and
    /// <c>hints.parameter</c> names the knob. The three producers a Lite tool has are each driven here: the
    /// resolver's miss, a <c>McpHelpers</c> validator, and a tool's own inline refusal (<c>analyze_plan_xml</c>).
    /// The Darling twin of this pin is <c>McpPayloadContractCensusTests</c>, whose source census walks both
    /// SKUs' four hundred pass-through sites to their producers; this is the Lite half that RUNS.
    /// </summary>
    [Fact]
    public async Task EveryRefusal_IsTheInvalidEnvelope_OnLite()
    {
        /* The resolver's miss — the ~90 Lite tools' first bail. */
        var (_, miss) = ServerResolver.ResolveOrError(_serverManager, "no-such-server");
        AssertRefusal(miss, "server_name", "Could not resolve server.");
        AssertRefusal(await McpWaitTools.GetWaitStats(_dataService, _serverManager, "no-such-server"), "server_name", "Could not resolve server.");

        /* A shared validator's refusal, reached through a tool. */
        AssertRefusal(await McpWaitTools.GetWaitStats(_dataService, _serverManager, ServerName, 0), "hours_back", "Invalid hours_back value '0'.");
        AssertRefusal(await McpWaitTools.GetWaitStats(_dataService, _serverManager, ServerName, 24, 0), "limit", "Invalid limit value '0'.");
        AssertRefusal(await McpWaitTools.GetWaitStats(_dataService, _serverManager, ServerName, 24, 20, "last tuesday"), "as_of", "Invalid as_of value 'last tuesday'.");
        AssertRefusal(await McpHealthTools.GetDailySummary(_dataService, _serverManager, ServerName, "01/02/2026"), "summary_date", "Invalid summary_date value '01/02/2026'.");

        /* A tool's own inline refusal. */
        AssertRefusal(McpPlanTools.AnalyzePlanXml("   "), "plan_xml", "No plan XML provided.");

        /* And the neighbour it must not be confused with: a well-formed call on an empty store is a miss word
           (or data), never `invalid` and never `error`. */
        var quiet = await McpHealthTools.GetDailySummary(_dataService, _serverManager, ServerName);
        Assert.False(McpHelpers.IsRefusalEnvelope(quiet), quiet);
        Assert.False(McpHelpers.IsErrorEnvelope(quiet), quiet);
    }

    private static void AssertRefusal(string? wire, string parameter, string sentenceStart)
    {
        Assert.NotNull(wire);
        Assert.True(McpHelpers.IsRefusalEnvelope(wire), "not the `invalid` envelope: " + wire);
        Assert.False(McpHelpers.IsErrorEnvelope(wire), "a refusal wearing the failure word: " + wire);
        var envelope = Parse(wire!);
        Assert.Equal("invalid", envelope.GetProperty("status").GetString());
        Assert.Equal(parameter, envelope.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.StartsWith(sentenceStart, envelope.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.StartsWith(sentenceStart, McpHelpers.ErrorMessageOf(wire!), StringComparison.Ordinal);
    }

    /* ───────────────────────── #3541 A9: retention ghosts ───────────────────────── */

    /// <summary>
    /// Lite's one horizon is the archive retention (three months, every table together), so a day older than
    /// that which the spine still holds is a shell whatever its run count: <c>purged</c>, <c>NoData</c>, never
    /// Healthy. A day inside the horizon with signal rows but no run record is <c>no_run_record</c> and keeps its band. Today's run
    /// is <c>collected</c> and Healthy beside them. The single-day read of a purged day refuses a verdict, and
    /// <c>summary_date</c> is exact ISO-8601.
    /// </summary>
    [Fact]
    public async Task DailySummary_StopsPaintingPurgedDaysGreen_AndPublishesTheHorizon()
    {
        var now = DateTime.UtcNow;
        var today = now.Date;
        var horizon = LocalDataService.DailySummaryRetentionHorizon(now);
        var ghostDay = horizon.AddDays(-10);
        var uncollectedDay = today.AddDays(-3);

        /* Today's run has to land on today: two minutes back crosses midnight UTC in a day's first two
           minutes, which is how this failed CI at 00:01Z on #3942. */
        await SeedRunAsync(now.AddMinutes(-2) < today ? now : now.AddMinutes(-2));
        await SeedRunAsync(ghostDay.AddHours(12));
        /* Signal rows and no run record: the spine holds the day from wait_stats alone. */
        await SeedWaitStatAsync(uncollectedDay.AddHours(12), "CXPACKET", 4000);

        var daysBack = (int)(today - ghostDay).TotalDays + 1;
        var range = Parse(await McpHealthTools.GetDailySummaryRange(_dataService, _serverManager, ServerName, daysBack));
        Assert.Equal(horizon.ToString("yyyy-MM-dd"), range.GetProperty("retention_horizon").GetString());
        Assert.Equal(3, range.GetProperty("day_count").GetInt32());
        Assert.Equal(1, range.GetProperty("days_before_horizon").GetInt32());
        Assert.Equal(1, range.GetProperty("purged_day_count").GetInt32());
        Assert.Equal(1, range.GetProperty("collected_day_count").GetInt32());

        var days = range.GetProperty("days").EnumerateArray().ToArray();
        var ghost = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == ghostDay.ToString("yyyy-MM-dd"));
        Assert.Equal(1, ghost.GetProperty("collection_runs").GetInt64());
        Assert.Equal("purged", ghost.GetProperty("data_state").GetString());
        Assert.Equal("NoData", ghost.GetProperty("health_band").GetString());
        /* #3653: one band, one spelling \u2014 overall_health carries the same token health_band does. */
        Assert.Equal("NoData", ghost.GetProperty("overall_health").GetString());
        Assert.StartsWith("PURGED", ghost.GetProperty("data_note").GetString(), StringComparison.Ordinal);

        /* Inside retention with signal rows and no run record: a disclosure, not a withheld verdict — the day
           keeps its band (Healthy here; PerformanceCalendarDataTests pins an alert-only day as Warning). */
        var uncollected = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == uncollectedDay.ToString("yyyy-MM-dd"));
        Assert.Equal("no_run_record", uncollected.GetProperty("data_state").GetString());
        Assert.Equal("Healthy", uncollected.GetProperty("health_band").GetString());
        Assert.StartsWith("NO RUN RECORD", uncollected.GetProperty("data_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(0, uncollected.GetProperty("collection_runs").GetInt64());
        Assert.Equal("CXPACKET", uncollected.GetProperty("top_wait_type").GetString());

        var live = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == today.ToString("yyyy-MM-dd"));
        Assert.Equal("collected", live.GetProperty("data_state").GetString());
        Assert.Equal("Healthy", live.GetProperty("overall_health").GetString());
        Assert.Equal(JsonValueKind.Null, live.GetProperty("data_note").ValueKind);

        var single = Parse(await McpHealthTools.GetDailySummary(_dataService, _serverManager, ServerName, ghostDay.ToString("yyyy-MM-dd")));
        Assert.Equal("unavailable", single.GetProperty("status").GetString());
        Assert.Equal("purged", single.GetProperty("hints").GetProperty("data_state").GetString());
        Assert.Equal(horizon.ToString("yyyy-MM-dd"), single.GetProperty("hints").GetProperty("retention_horizon").GetString());

        var todayRow = Parse(await McpHealthTools.GetDailySummary(_dataService, _serverManager, ServerName));
        Assert.Equal("collected", todayRow.GetProperty("data_state").GetString());
        Assert.Equal(horizon.ToString("yyyy-MM-dd"), todayRow.GetProperty("retention_horizon").GetString());

        Assert.StartsWith("Invalid summary_date value '01/02/2026'", McpHelpers.ErrorMessageOf(await McpHealthTools.GetDailySummary(_dataService, _serverManager, ServerName, "01/02/2026")), StringComparison.Ordinal);
    }

    /// <summary>The Lite horizon is the archive retention constant, in months, from the reader's clock, rounded
    /// up to the month start the whole-file archive cleanup actually keeps: the cutoff's own month is deleted
    /// with its file, so a mid-month cutoff holds nothing until the next month begins.</summary>
    [Fact]
    public void TheLiteHorizon_IsTheArchiveRetention_InMonths_FromTheFirstWholeMonthKept()
    {
        Assert.Equal(3, RetentionService.ArchiveRetentionMonths);

        var midMonth = new DateTime(2026, 9, 18, 14, 30, 0, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2026, 7, 1), LocalDataService.DailySummaryRetentionHorizon(midMonth));
        Assert.Equal(RetentionService.OldestRetainedInstant(midMonth), LocalDataService.DailySummaryRetentionHorizon(midMonth));

        /* A cutoff that is itself a month start keeps that month: its file is not before the cutoff. */
        var monthStart = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2026, 6, 1), LocalDataService.DailySummaryRetentionHorizon(monthStart));
    }

    /* ───────────────────────── the contract, as a census ───────────────────────── */

    /// <summary>
    /// The touched tools, and the word each description must carry: a page whose bound is not named in the
    /// description is one an agent will read as the window. <c>truncated</c> is the field every one of them
    /// publishes, so it is the word every one of them must explain.
    /// </summary>
    public static readonly (Type Tools, string ToolName)[] PagedTools =
    [
        (typeof(McpBlockingTools), "get_deadlocks"),
        (typeof(McpBlockingTools), "get_deadlock_detail"),
        (typeof(McpBlockingTools), "get_blocked_process_reports"),
        (typeof(McpBlockingTools), "get_blocked_process_xml"),
        (typeof(McpAlertTools), "get_alert_history"),
        (typeof(McpLongQueryTools), "get_long_query_completions"),
        (typeof(McpPlanCorrectionTools), "get_plan_corrections"),
        (typeof(McpWaitTools), "get_waiting_tasks"),
        (typeof(McpWaitTools), "get_wait_stats"),
        /* #3541 A13: joined the dialect when its filters moved into the SQL — see the A13 tests above. */
        (typeof(McpSessionTools), "get_active_queries"),
        /* #3653: the latest-snapshot pair, whose LIMIT 20 literal was the last hidden cap on this SKU's MCP
           surface — see the two facts above. */
        (typeof(McpLatchSpinlockTools), "get_latch_stats"),
        (typeof(McpLatchSpinlockTools), "get_spinlock_stats"),
    ];

    [Fact]
    public void EveryPagedTool_SaysWhatBoundsItsPage_InItsDescription()
    {
        foreach (var (type, name) in PagedTools)
        {
            var method = ToolMethod(type, name);
            var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.Contains("truncated", description, StringComparison.Ordinal);
            Assert.Contains("limit", description, StringComparison.Ordinal);

            var limit = method.GetParameters().Single(p => p.Name == "limit");
            Assert.Contains("truncated", limit.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <c>include_dismissed</c> is an APPENDED optional, after <c>as_of</c>, defaulting to the grid's own read
    /// — so a caller who never sends it reads what they always read, and no positional C# caller is re-bound.
    /// The description must carry the filter's name in both directions: what it hides and how to lift it.
    /// </summary>
    [Fact]
    public void GetAlertHistory_IncludeDismissed_IsAnAppendedOptional_AndTheDescriptionNamesTheFilter()
    {
        var method = ToolMethod(typeof(McpAlertTools), "get_alert_history");
        var names = method.GetParameters().Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null).Select(p => p.Name).ToArray();

        var flag = method.GetParameters().Single(p => p.Name == "include_dismissed");
        Assert.True(flag.HasDefaultValue);
        Assert.False((bool)flag.DefaultValue!);
        Assert.True(Array.IndexOf(names, "include_dismissed") > Array.IndexOf(names, "as_of"));

        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("EXCLUDES DISMISSED ALERTS", description, StringComparison.Ordinal);
        Assert.Contains("dismissed_excluded_count", description, StringComparison.Ordinal);
        Assert.Contains("include_dismissed", description, StringComparison.Ordinal);
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    /// <summary>
    /// The page contract every data-bearing payload here must meet: the named count IS the array's length,
    /// <c>truncated</c> is what the caller was told to read, and NO key on the envelope starts with
    /// <c>total_</c> — the name that promised a window and delivered a page.
    /// </summary>
    private static void AssertPage(JsonElement root, string rowsKey, string returnedKey, int returned, bool truncated)
    {
        Assert.False(root.TryGetProperty("status", out _), "expected a data-bearing payload, got a status envelope: " + root);
        Assert.Equal(returned, root.GetProperty(rowsKey).GetArrayLength());
        Assert.Equal(returned, root.GetProperty(returnedKey).GetInt32());
        Assert.Equal(truncated, root.GetProperty("truncated").GetBoolean());

        var totals = root.EnumerateObject().Select(p => p.Name).Where(n => n.StartsWith("total_", StringComparison.Ordinal)).ToArray();
        Assert.True(totals.Length == 0, "a page count is published under a window's name: " + string.Join(", ", totals));
    }

    private static MethodInfo ToolMethod(Type type, string toolName) => type
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    /// <summary>What a seeded UTC instant looks like on the payload: the store holds a naive timestamp, and the
    /// tools emit it with <c>ToString("o")</c> on a <c>Kind = Unspecified</c> value, so no <c>Z</c>.</summary>
    private static string Stamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified).ToString("o");

    /// <summary>Now, truncated to the second. DuckDB's TIMESTAMP is microsecond-precision and a .NET tick is
    /// 100 ns, so an untruncated anchor round-trips with its last digit gone and every stamp equality below
    /// fails on precision rather than on the property it asserts.</summary>
    private static DateTime WholeSecondsNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private Task SeedDeadlockAsync(DateTime at, bool withGraph) => ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        _nextId--, Naive(at), _serverId, ServerName, Naive(at), "process1", "DELETE FROM Posts",
        withGraph ? "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list><process id=\"process1\"><inputbuf>DELETE FROM Posts</inputbuf></process></process-list></deadlock>" : null);

    private Task SeedBlockedProcessReportAsync(DateTime at, int blockedSpid, bool withXml) => ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        _nextId--, Naive(at), _serverId, ServerName, Naive(at), "Db", blockedSpid, 90, 8000L, "X", "SELECT 1", "UPDATE T SET x = 1",
        withXml ? "<blocked-process-report><blocked-process><process spid=\"55\"/></blocked-process></blocked-process-report>" : null, "dbo.T");

    private Task SeedDmvBlockingSnapshotAsync(DateTime at, int blockedSpid) => ExecAsync(@"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        _nextId--, Naive(at), _serverId, ServerName, -1, Naive(at), "Db", blockedSpid, 80, 3000L, "S", "suspended", "dbo.U", "SELECT 2", "WAITFOR DELAY '00:01'");

    private Task SeedAlertAsync(DateTime at, bool dismissed) => ExecAsync(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, dismissed)
VALUES ($1, $2, $3, 'High CPU', 92.5, 80.0, TRUE, 'email', $4)",
        Naive(at), _serverId, ServerName, dismissed);

    private Task SeedLongQueryAsync(DateTime at, long durationMicroseconds) => ExecAsync(@"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1, $2, $3, $4, $5, 'rpc_completed', 'Db', $6, 'EXEC p')",
        _nextId--, Naive(at), _serverId, ServerName, Naive(at), durationMicroseconds);

    private Task SeedPlanCorrectionAsync(DateTime at) => ExecAsync(@"
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
VALUES ($1, $2, $3, $4, 'Db', 'PR_1', 'Active', 50)",
        _nextId--, Naive(at), _serverId, ServerName);

    private Task SeedWaitingTaskAsync(DateTime at) => ExecAsync(@"
INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, database_name)
VALUES ($1, $2, $3, $4, 55, 'LCK_M_X', 3000, 60, 'Db')",
        _nextId--, Naive(at), _serverId, ServerName);

    private Task SeedWaitStatAsync(DateTime at, string waitType, long deltaMs) => ExecAsync(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 10, $6, 100)",
        _nextId--, Naive(at), _serverId, ServerName, waitType, deltaMs);

    /* #3653: the latest-snapshot pair. The cumulative columns are constants; the delta is the ordering key. */

    private Task SeedLatchAsync(DateTime at, string latchClass, long deltaWaitMs, int? intervalSeconds = 60) => ExecAsync(@"
INSERT INTO latch_stats
    (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms,
     delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 1000, 20100, 50, 100, $6, 5, $7)",
        _nextId--, Naive(at), _serverId, ServerName, latchClass, deltaWaitMs, intervalSeconds);

    private Task SeedSpinlockAsync(DateTime at, string spinlockName, long deltaCollisions, int? intervalSeconds = 60) => ExecAsync(@"
INSERT INTO spinlock_stats
    (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs,
     delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 500000, 900000, 1.8, 10, 20, $6, 1000, 0, 0, $7)",
        _nextId--, Naive(at), _serverId, ServerName, spinlockName, deltaCollisions, intervalSeconds);

    /* #3541 A13 / A9 fixtures. */

    private Task SeedQueryStatAsync(DateTime at, string queryHash, long cpuUs, int maxDop) => ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
     query_text, last_execution_time, creation_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
     min_dop, max_dop)
VALUES ($1, $2, $3, $4, 'Db', $5, '0xPLANHASH', $6, '0xPLANH', $7, $2, $2, 10, $8, $8, 100, 1, $9)",
        _nextId--, Naive(at), _serverId, ServerName, queryHash, "0xSQLH" + queryHash, "SELECT " + queryHash, cpuUs, maxDop);

    private Task SeedSnapshotAsync(DateTime at, int sessionId, string database, string text, int blockingSessionId, long cpuMs) => ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, blocking_session_id,
     wait_type, cpu_time_ms, total_elapsed_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        _nextId--, Naive(at), _serverId, ServerName, sessionId, database, text,
        blockingSessionId > 0 ? "suspended" : "running", blockingSessionId, blockingSessionId > 0 ? "LCK_M_X" : null, cpuMs, cpuMs * 2);

    private Task SeedRunAsync(DateTime at) => ExecAsync(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, 'wait_stats', $4, 100, 'SUCCESS', NULL, 10, 80, 20)",
        _nextId--, _serverId, ServerName, Naive(at));

    private static string[] Hashes(JsonElement root) =>
        root.GetProperty("queries").EnumerateArray().Select(q => q.GetProperty("query_hash").GetString()!).ToArray();

    private static JsonElement Row(JsonElement[] rows, int sessionId) =>
        Assert.Single(rows, r => r.GetProperty("session_id").GetInt32() == sessionId);
}
