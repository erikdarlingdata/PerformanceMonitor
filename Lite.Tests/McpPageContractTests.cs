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
}
