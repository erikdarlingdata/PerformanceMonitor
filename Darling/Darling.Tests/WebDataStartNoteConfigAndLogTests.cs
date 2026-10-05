/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's Config Changes grids (server, database, trace flags, and PostgreSQL's) and its two Collection Log
/// grids say where their data starts (#4966). Each is a listed read (<see cref="WebDataStartNote.TableByRead"/>) over
/// the snapshot table or the run log it diffs or shows; the two lists that can hit a row cap (the collection log and
/// the PostgreSQL changes) name their oldest row. These cover what needs no store; the store's side is
/// <see cref="WebDataStartNoteLiveTests"/>.
/// </summary>
public sealed class WebDataStartNoteConfigAndLogTests : IClassFixture<ConfigAndLogNoteStore>
{
    private readonly ConfigAndLogNoteStore _store;

    public WebDataStartNoteConfigAndLogTests(ConfigAndLogNoteStore store) => _store = store;

    private static readonly IReadOnlyDictionary<string, string> NewReads = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_server_config_changes"] = "server_config",
        ["get_database_config_changes"] = "database_config",
        ["get_trace_flag_changes"] = "trace_flags",
        ["get_pg_server_config_changes"] = "pg_server_config",
        ["get_collection_log"] = "collection_log",
    };

    private const string WindowEnd = "2026-01-03T00:00:00Z";

    /// <summary>What <c>get_collection_log</c> answers for a window holding more runs than its limit: the newest runs, with
    /// the fields that say the cap cut the list, where the page ends and which order it came back in.</summary>
    private const string CappedLog =
        "{\"server\":\"sql01\",\"hours_back\":720,\"run_count\":200,\"truncated\":true,"
        + "\"oldest_returned_collection_time\":\"2026-01-02T12:30:00.0000000\",\"newest_returned_collection_time\":\"2026-01-03T00:00:00.0000000\","
        + "\"order\":\"collection_time_desc\",\"runs\":[{\"collector\":\"wait_stats\"}]}";

    /// <summary>What <c>get_pg_server_config_changes</c> answers when its page was cut: the newest changes (listed here out
    /// of order on purpose, so the oldest is found by value), no oldest-returned field, and a <c>status</c> that names the
    /// page rather than an envelope.</summary>
    private const string CappedPgChanges =
        "{\"server\":\"pg01\",\"hours_back\":168,\"status\":\"config_changes\",\"change_count\":3,\"truncated\":true,"
        + "\"changes\":[{\"changed_at\":\"2026-01-02T20:00:00\",\"name\":\"a\"},{\"changed_at\":\"2026-01-02T08:15:00\",\"name\":\"b\"},{\"changed_at\":\"2026-01-02T12:00:00\",\"name\":\"c\"}]}";

    private static NpgsqlDataSource NeverConnects() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=9;Username=x;Database=x;Timeout=1;Pooling=false");

    private static readonly CancellationToken Cancelled = new(canceled: true);

    /* A page of rows that is not capped, in the shape each read answers (the PostgreSQL one carries its page status). */
    private static string Rows(string read) =>
        string.Equals(read, "get_pg_server_config_changes", StringComparison.Ordinal)
            ? "{\"server\":\"pg01\",\"hours_back\":168,\"status\":\"config_changes\",\"change_count\":1,\"truncated\":false,\"changes\":[{\"changed_at\":\"2026-01-02T20:00:00\"}]}"
            : "{\"server\":\"sql01\",\"hours_back\":168,\"truncated\":false,\"order\":\"collection_time_desc\",\"changes\":[{\"x\":1}],\"runs\":[{\"collector\":\"wait_stats\"}]}";

    [Fact]
    public void EachGrid_IsAListedRead_TheWebMirrorServes_OverItsOwnSnapshotTableOrTheRunLog()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        foreach (var (read, table) in NewReads)
        {
            Assert.Equal(table, WebDataStartNote.TableByRead[read]);
            Assert.True(dispatch.ContainsKey(read), read + " is not a read the web mirror serves");
            Assert.Contains("\"" + read + "\"", tabs, StringComparison.Ordinal);

            Assert.True(WebDataStartNote.TryGetSource(table, out var source), read + " names " + table + ", which the probe cannot read");
            Assert.Equal(table, source.Relation);
        }

        /* The run log is its own source, edge and all; the snapshot tables are the collector tables the probe reads. */
        Assert.True(WebDataStartNote.TryGetSource("collection_log", out var log));
        Assert.Null(log.CollectorName);
        Assert.False(PerformanceMonitor.Darling.Storage.DataWindowFloor.Source.TryForCollectorTable("collection_log", out _));
    }

    /* A range of an hour or less can never be cut by the store's coverage (the probe's 90-minute slack), so none of these reads asks
       the store, which a data source that never connects, a cancelled token and a control read pin: the same answer at 168 hours
       does reach the store. */
    [Fact]
    public async Task ARangeOfAnHour_MakesNoProbeCall_ForAnyOfThem_WhereTheSameReadOverAWeekDoes()
    {
        await using var store = NeverConnects();

        foreach (var read in NewReads.Keys)
        {
            var rows = Rows(read);

            Assert.Same(rows, await WebDataStartNote.AddAsync(store, read, "sql01", 1, null, rows, null, Cancelled));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => WebDataStartNote.AddAsync(store, read, "sql01", 168, null, rows, null, Cancelled));
        }
    }

    /* A newest-first list that hit its cap ends at the oldest row it returned, whatever the store covers: the collection log's
       page (200 runs) names its oldest run at any length of window (the log keeps 60 days, so the page may ask 720 hours), and
       asks the store nothing. */
    [Fact]
    public async Task ACappedCollectionLog_NamesItsOldestRun_AtAnyWindowLength_WithoutAskingTheStore()
    {
        var ct = TestContext.Current.CancellationToken;

        var answered = await WebDataStartNote.AddAsync(null!, "get_collection_log", "sql01", 720, WindowEnd, CappedLog, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T12:30:00.0000000", answer["effective_start"]?.GetValue<string>());
        Assert.Contains("back to 2026-01-02 12:30 UTC", answer["truncation_note"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("2026-01-02T12:30:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
        Assert.Equal("2025-12-04T00:00:00.0000000Z", answer["window_start_utc"]?.GetValue<string>());
        Assert.Equal("2026-01-03T00:00:00.0000000Z", answer["window_end_utc"]?.GetValue<string>());
        Assert.Null(answer["data_start_utc"]);

        /* The other listed reads keep the shared 168-hour ceiling: the same cap fields on a 720-hour answer name nothing. */
        Assert.Same(CappedLog, await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 720, WindowEnd, CappedLog, null, ct));
    }

    /* A duration floor flips the log to slowest first: the page is a sample of the whole window, so its oldest run names no reach
       and the coverage rule stands. Without a store the probe fails and the answer comes back as it was; over a store holding a
       server added two days ago, the same ranked page gets the coverage note and not the capped one. */
    [Fact]
    public async Task ACollectionLogRankedSlowestFirst_KeepsTheCoverageRule()
    {
        var ct = TestContext.Current.CancellationToken;
        var slowest = CappedLog.Replace("collection_time_desc", "duration_ms_desc", StringComparison.Ordinal);

        Assert.Same(slowest, await WebDataStartNote.AddAsync(null!, "get_collection_log", "sql01", 48, WindowEnd, slowest, null, ct));

        Assert.SkipWhen(_store.DataSource is null, "Set DARLING_TEST_PG to a Postgres connection string to run the ranked-page coverage fact.");
        var end = _store.End;
        var ranked = "{\"server\":\"x\",\"hours_back\":168,\"run_count\":200,\"truncated\":true,"
            + "\"oldest_returned_collection_time\":\"" + end.AddHours(-12).ToString("o", System.Globalization.CultureInfo.InvariantCulture) + "\","
            + "\"order\":\"duration_ms_desc\",\"runs\":[{\"collector\":\"wait_stats\"}]}";
        var server = ConfigAndLogNoteStore.ServerName(ConfigAndLogNoteStore.CollectionLogRead, "new");

        var answered = await WebDataStartNote.AddAsync(
            _store.DataSource!, "get_collection_log", server, 168, end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture), ranked, null, ct);
        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));

        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.NotNull(answer["data_start_utc"]);
        Assert.Null(answer["oldest_shown_utc"]);
        Assert.StartsWith("partial window:", answer["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    /* The PostgreSQL changes page names no oldest-returned field: its oldest row is the earliest changed_at it carries. Its status
       word names the page, not an envelope, so a page that was not cut still goes to the coverage probe, and a real envelope
       does not. */
    [Fact]
    public async Task ACappedPostgresChangesPage_NamesItsEarliestChange_AndItsPageStatusIsNotAnEnvelope()
    {
        var ct = TestContext.Current.CancellationToken;

        var answered = await WebDataStartNote.AddAsync(null!, "get_pg_server_config_changes", "pg01", 168, WindowEnd, CappedPgChanges, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T08:15:00", answer["effective_start"]?.GetValue<string>());
        Assert.Contains("back to 2026-01-02 08:15 UTC", answer["truncation_note"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("2026-01-02T08:15:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
        Assert.Equal(3, answer["changes"]!.AsArray().Count);

        await using var store = NeverConnects();
        /* The envelope that says nothing changed (no_changes) is probed like rows since #4966, so the answer for a new server says
           where its data starts; one that keeps its own message (not_collected) is still left as it is. */
        const string Unchanged = "{\"status\":\"no_changes\",\"message\":\"No configuration parameter changed value.\"}";
        const string Elsewhere = "{\"status\":\"not_collected\",\"message\":\"This engine has no configuration parameters.\"}";
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_pg_server_config_changes", "pg01", 168, null, Unchanged, null, Cancelled));
        Assert.Same(Elsewhere, await WebDataStartNote.AddAsync(store, "get_pg_server_config_changes", "pg01", 168, null, Elsewhere, null, Cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_pg_server_config_changes", "pg01", 168, null, Rows("get_pg_server_config_changes"), null, Cancelled));
    }

    private static string Envelope(string status) =>
        "{\"status\":\"" + status + "\",\"message\":\"Nothing in this window.\"}";

    /* An empty span over a short history reads as "nothing happened" when it is only short (#4966), so the answer that says the
       read looked and found nothing goes to the coverage probe the way rows do: a store that never connects and a cancelled
       token make the probe show as an OperationCanceledException. Each admitted read answers its own word (the PostgreSQL
       changes read says no_changes, every other one says empty). */
    [Fact]
    public async Task AnAnswerSayingTheReadLookedAndFoundNothing_GoesToTheCoverageProbe_OnEachAdmittedRead()
    {
        await using var store = NeverConnects();

        foreach (var (read, word) in WebDataStartNote.NothingFoundStatusByRead)
        {
            Assert.Contains(read, WebDataStartNote.TableByRead.Keys);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => WebDataStartNote.AddAsync(store, read, "sql01", 168, null, Envelope(word), null, Cancelled));
        }

        Assert.Equal("no_changes", WebDataStartNote.NothingFoundStatusByRead["get_pg_server_config_changes"]);
        foreach (var read in new[] { "get_server_config_changes", "get_database_config_changes", "get_trace_flag_changes", "get_collection_log", "get_waiting_tasks" })
        {
            Assert.Equal("empty", WebDataStartNote.NothingFoundStatusByRead[read]);
        }
    }

    /* not_collected, unavailable, invalid and an error keep their own message on every listed read, and so does a word that is
       not the read's own "nothing found" one: the answer comes back as the same instance without a probe. */
    [Fact]
    public async Task EveryOtherStatus_KeepsItsOwnMessage_AndGetsNoNote_OnEveryListedRead()
    {
        await using var store = NeverConnects();

        foreach (var read in WebDataStartNote.TableByRead.Keys)
        {
            foreach (var status in new[] { "not_collected", "unavailable", "invalid", "error", "empty", "no_changes", "precondition", "not_sampled" })
            {
                if (WebDataStartNote.NothingFoundStatusByRead.TryGetValue(read, out var admitted) && admitted == status)
                {
                    continue;
                }

                var envelope = Envelope(status);
                Assert.Same(envelope, await WebDataStartNote.AddAsync(store, read, "sql01", 168, null, envelope, null, Cancelled));
            }
        }
    }

    /* The reads left out answer "no rows" with unavailable, which keeps its own message, and the admitted ones really answer the
       word the list gives. Read from the tools' source, so a tool that changes its word fails here and gets decided on purpose. */
    [Fact]
    public void TheAdmittedReads_AnswerTheirWord_AndTheOthersAnswerUnavailable()
    {
        (string File, string Method)[] Where(string read) => read switch
        {
            "get_waiting_tasks" => [("DarlingMcpSessionTools.cs", "GetWaitingTasks")],
            "get_pg_wait_sampling" => [("DarlingMcpPgWaitSamplingTools.cs", "GetPgWaitSampling")],
            "get_pg_kernel_stats" => [("DarlingMcpPgKernelStatsTools.cs", "GetPgKernelStats")],
            "get_pg_lock_stats" => [("DarlingMcpPgServerStateTools.cs", "GetPgLockStats")],
            "get_pg_predicate_stats" => [("DarlingMcpPgPredicateTools.cs", "GetPgPredicateStats")],
            "get_server_config_changes" => [("DarlingMcpConfigHistoryTools.cs", "GetServerConfigChanges")],
            "get_database_config_changes" => [("DarlingMcpConfigHistoryTools.cs", "GetDatabaseConfigChanges")],
            "get_trace_flag_changes" => [("DarlingMcpConfigHistoryTools.cs", "GetTraceFlagChanges")],
            "get_pg_server_config_changes" => [("DarlingMcpPgServerStateTools.cs", "GetPgServerConfigChanges")],
            "get_collection_log" => [("DarlingMcpDataTools.cs", "GetCollectionLog")],
            "get_latch_stats" => [("DarlingMcpLatchSpinlockTools.cs", "GetLatchStats")],
            "get_spinlock_stats" => [("DarlingMcpLatchSpinlockTools.cs", "GetSpinlockStats")],
            "get_wait_stats" => [("DarlingMcpDataTools.cs", "GetWaitStats")],
            "get_memory_grants" => [("DarlingMcpMemoryGrantTools.cs", "GetMemoryGrants")],
            "get_resource_semaphore" => [("DarlingMcpMemoryGrantTools.cs", "GetResourceSemaphore")],
            "get_plan_corrections" => [("DarlingMcpPlanCorrectionTools.cs", "GetPlanCorrections")],
            "get_pg_wait_stats" => [("DarlingMcpPgWaitTools.cs", "GetPgWaitStats")],
            "get_blocked_process_xml" => [("DarlingMcpBlockingTools.cs", "GetBlockedProcessXml")],
            "get_long_query_completions" => [("DarlingMcpLongQueryTools.cs", "GetLongQueryCompletions")],
            "get_memory_pressure_events" => [("DarlingMcpMemoryGrantTools.cs", "GetMemoryPressureEvents")],
            "get_default_trace_events" => [("DarlingMcpDefaultTraceTools.cs", "GetDefaultTraceEvents")],
            _ => throw new ArgumentOutOfRangeException(nameof(read), read, "a listed read this test does not know"),
        };

        string Body(string read)
        {
            var (file, method) = Where(read).Single();
            var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
            var start = source.IndexOf("public static async Task<string> " + method + "(", StringComparison.Ordinal);
            Assert.True(start > 0, read + ": " + method + " not found");
            var end = source.IndexOf("[McpServerTool(", start, StringComparison.Ordinal);
            return end < 0 ? source[start..] : source[start..end];
        }

        foreach (var read in WebDataStartNote.TableByRead.Keys.Where(r => !PgWindowReads.Any(p => p.Read == r)))
        {
            var body = Body(read);
            if (WebDataStartNote.NothingFoundStatusByRead.TryGetValue(read, out var word))
            {
                /* The three SQL Server histories answer through the one NoChanges helper, whose word is empty. */
                var answersIt = body.Contains("\"" + word + "\"", StringComparison.Ordinal)
                    || (word == "empty" && body.Contains("NoChanges(", StringComparison.Ordinal));
                Assert.True(answersIt, read + " is admitted for " + word + " but its tool never answers it");
            }
            else
            {
                Assert.Contains("\"unavailable\"", body, StringComparison.Ordinal);
                Assert.DoesNotContain("\"empty\"", body, StringComparison.Ordinal);
            }
        }
    }

    /* The PostgreSQL window aggregates, trend grids and Captured Plans (#4966): each read, the table it reads, the one word it
       answers when it looked and found nothing (null: none is admitted), the words it answers rows with, and the other words
       its tool file answers as literals. A word on the last list keeps its own message. */
    private static readonly (string Read, string Table, string? Admitted, string[] RowWords, string[] KeptWords, string File)[] PgWindowReads =
    [
        ("get_pg_top_queries", "pg_statement_stats", null, [], ["unavailable"], "DarlingMcpPgStatementTools.cs"),
        ("get_pg_blocking", "pg_blocking_edges", "no_blocking_sampled", ["blocking_sampled", "cycles_only"], ["not_sampled"], "DarlingMcpPgBlockingTools.cs"),
        ("get_pg_database_stats", "pg_database_stats", "empty", ["database_activity"], ["unavailable"], "DarlingMcpPgDatabaseTools.cs"),
        ("get_pg_session_states", "pg_session_states", "empty", ["session_states"], ["unavailable"], "DarlingMcpPgSessionStatesTools.cs"),
        ("get_pg_io_stats", "pg_io_stats", "no_io_activity", ["io_activity"], [], "DarlingMcpPgIoTools.cs"),
        ("get_pg_replication_stats", "pg_replication_stats", "empty", [], [], "DarlingMcpPgReplicationStatsTools.cs"),
        ("get_pg_database_trend", "pg_database_stats", "empty", ["database_trend"], [], "DarlingMcpPgTrendTools.cs"),
        ("get_pg_query_duration_trend", "pg_statement_stats", "empty", ["query_duration_trend"], [], "DarlingMcpPgTrendTools.cs"),
        ("get_pg_wait_trend", "pg_wait_sampling", "empty", ["wait_trend"], [], "DarlingMcpPgTrendTools.cs"),
        ("get_pg_io_trend", "pg_io_stats", "empty", ["io_trend"], [], "DarlingMcpPgTrendTools.cs"),
        ("get_pg_plans", "pg_plan_capture", "empty", [], ["precondition"], "DarlingMcpPgPlanTools.cs"),

        /* The vacuum, horizon, slot and write tiles. Only the horizon read admits a word: no_holder is answered after the log shows the
           collector captured in the window, and unavailable (no capture logged) is kept. The other four answer their healthy-or-empty
           word without asking the log whether the collector ran (no_pending_maintenance, no_slots), or say the collector has not
           collected (unavailable), or say a window too short to difference holds nothing (empty), so none of those is admitted. */
        ("get_pg_wraparound_risk", "pg_wraparound_stats", null, [], ["unavailable"], "DarlingMcpPgWraparoundTools.cs"),
        ("get_pg_xmin_horizon", "pg_xmin_horizon", "no_holder", ["holder_present"], ["unavailable"], "DarlingMcpPgXminTools.cs"),
        ("get_pg_autovacuum_health", "pg_autovacuum_stats", null, ["tables_with_pending_maintenance"], ["no_pending_maintenance"], "DarlingMcpPgAutovacuumTools.cs"),
        ("get_pg_replication_slots", "pg_replication_slot_stats", null, ["slots_present"], ["no_slots"], "DarlingMcpPgSlotTools.cs"),
        ("get_pg_write_stats", "pg_write_stats", null, [], ["empty"], "DarlingMcpPgServerStateTools.cs"),
        ("get_pg_index_usage", "pg_index_usage_stats", "empty", ["index_usage"], ["unavailable"], "DarlingMcpPgIndexUsageTools.cs"),
    ];

    /* Each read is listed over its raw relation, served by the web mirror, drawn by the page, and admitted for exactly the word
       its tool answers when it captured and found nothing. The words that say nothing was captured (unavailable, not_collected,
       not_sampled, precondition) are never admitted. The tools' source holds both halves, so a tool that renames a word fails here. */
    [Fact]
    public void ThePostgresWindowReads_AreListedOverTheirTables_AndAdmitOnlyAWordThatMeansCapturedAndFoundNothing()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        foreach (var (read, table, admitted, rowWords, keptWords, file) in PgWindowReads)
        {
            Assert.Equal(table, WebDataStartNote.TableByRead[read]);
            Assert.True(dispatch.ContainsKey(read), read + " is not a read the web mirror serves");
            Assert.True(WebDataStartNote.TryGetSource(table, out var source), read + " names " + table + ", which the probe cannot read");
            Assert.Equal(table, source.Relation);
            Assert.Contains("\"" + read + "\"", tabs, StringComparison.Ordinal);

            var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
            if (admitted is null)
            {
                Assert.DoesNotContain(read, WebDataStartNote.NothingFoundStatusByRead.Keys);
            }
            else
            {
                Assert.Equal(admitted, WebDataStartNote.NothingFoundStatusByRead[read]);
                Assert.Contains("\"" + admitted + "\"", tool, StringComparison.Ordinal);
                Assert.DoesNotContain(admitted, new[] { "unavailable", "not_collected", "not_sampled", "precondition", "invalid" });
            }

            foreach (var word in rowWords.Concat(keptWords))
            {
                Assert.Contains("\"" + word + "\"", tool, StringComparison.Ordinal);
                Assert.NotEqual(admitted, word);
            }
        }
    }

    /* Behaviour: the admitted word reaches the coverage probe (a store that never connects and a cancelled token show it as an
       OperationCanceledException), the row words reach it as rows do, and every other word keeps its own message untouched. */
    [Fact]
    public async Task ThePostgresWindowReads_SendRowsAndTheirNothingFoundWordToTheProbe_AndLeaveEveryOtherWordAlone()
    {
        await using var store = NeverConnects();

        foreach (var (read, _, admitted, rowWords, keptWords, _) in PgWindowReads)
        {
            if (admitted is not null)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => WebDataStartNote.AddAsync(store, read, "pg01", 168, null, Envelope(admitted), null, Cancelled));
            }

            foreach (var word in rowWords)
            {
                var page = "{\"server\":\"pg01\",\"hours_back\":168,\"status\":\"" + word + "\",\"rows\":[{\"n\":1}]}";
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => WebDataStartNote.AddAsync(store, read, "pg01", 168, null, page, null, Cancelled));
            }

            foreach (var word in keptWords.Concat(new[] { "unavailable", "not_collected", "invalid", "precondition", "not_sampled" }).Where(w => w != admitted))
            {
                var envelope = Envelope(word);
                Assert.Same(envelope, await WebDataStartNote.AddAsync(store, read, "pg01", 168, null, envelope, null, Cancelled));
            }
        }
    }

    /* A capped page's note shows only when its oldest row is LATER than the window's start, with no slack: a page whose
       oldest row is the start (or before it) shows the whole range and says nothing, one a second after it was cut there.
       The window here is the 48 hours ending at WindowEnd, so it starts at 2026-01-01 00:00:00 UTC. The store never connects
       and the token is cancelled, so a probe in either case would be seen: a capped list asks the store nothing. */
    [Theory]
    [InlineData("get_waiting_tasks", "2026-01-01T00:00:00.0000000", false)]
    [InlineData("get_waiting_tasks", "2025-12-31T23:59:59.0000000", false)]
    [InlineData("get_waiting_tasks", "2026-01-01T00:00:01.0000000", true)]
    [InlineData("get_collection_log", "2026-01-01T00:00:00.0000000", false)]
    [InlineData("get_collection_log", "2025-12-31T23:59:59.0000000", false)]
    [InlineData("get_collection_log", "2026-01-01T00:00:01.0000000", true)]
    [InlineData("get_pg_server_config_changes", "2026-01-01T00:00:00", false)]
    [InlineData("get_pg_server_config_changes", "2025-12-31T23:59:59", false)]
    [InlineData("get_pg_server_config_changes", "2026-01-01T00:00:01", true)]
    public async Task ACappedPage_GetsItsNoteOnlyWhenItsOldestRowIsLaterThanTheWindowsStart_WithNoSlack(string read, string oldest, bool noted)
    {
        await using var store = NeverConnects();
        var page = string.Equals(read, "get_pg_server_config_changes", StringComparison.Ordinal)
            ? CappedPgChanges.Replace("2026-01-02T08:15:00", oldest, StringComparison.Ordinal)
            : CappedLog.Replace("2026-01-02T12:30:00.0000000", oldest, StringComparison.Ordinal);

        var answered = await WebDataStartNote.AddAsync(store, read, "sql01", 48, WindowEnd, page, null, Cancelled);

        if (!noted)
        {
            Assert.Same(page, answered);
            return;
        }

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal(oldest, answer["effective_start"]?.GetValue<string>());
        Assert.StartsWith("2026-01-01T00:00:01", answer["oldest_shown_utc"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("2026-01-01T00:00:00.0000000Z", answer["window_start_utc"]?.GetValue<string>());
        Assert.Equal("2026-01-03T00:00:00.0000000Z", answer["window_end_utc"]?.GetValue<string>());
        Assert.Null(answer["data_start_utc"]);
    }
}

/// <summary>
/// A Custom Views panel's partial-window notice is written in the browser's zone (#4966). The run's answer carries the
/// instants its data-start notice names (<c>data_start_utc</c>, <c>window_start_utc</c>, <c>window_end_utc</c>) and the
/// sentence itself, and the shipped <c>compose.js</c>, run under Node in a zone 5.5 hours from UTC, writes the sentence again
/// through <c>windowNoteText</c> in the clock the panel's own times use. A row-cap sentence beside it stays as sent, and an
/// answer without the fields draws the sentence as the server wrote it.
/// </summary>
public sealed class ComposedPanelNoticeZoneTests
{
    private const string Panel =
        "{\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private const string Sentence =
        "partial window: this panel's data starts at 2026-01-02 00:00 UTC, after the window's start at 2025-12-26 00:00 UTC. The panel covers 2026-01-02 00:00 to 2026-01-03 00:00 UTC.";

    private const string RowCapSentence = "Showing the first 500 rows; narrow the window or add a filter to see the rest.";

    private static JsonObject Answer(bool withFields)
    {
        var answer = new JsonObject
        {
            ["sql"] = "SELECT 1",
            ["rows"] = new JsonArray(),
            ["annotations"] = new JsonArray(),
            ["notice"] = Sentence + " " + RowCapSentence,
        };

        if (withFields)
        {
            answer["data_start_note"] = Sentence;
            answer["data_start_utc"] = "2026-01-02T00:00:00.0000000Z";
            answer["window_start_utc"] = "2025-12-26T00:00:00.0000000Z";
            answer["window_end_utc"] = "2026-01-03T00:00:00.0000000Z";
        }

        return answer;
    }

    private static string? Drawn(bool withFields, string zone)
    {
        var scenario = new JsonObject
        {
            ["panel"] = JsonNode.Parse(Panel),
            ["scope"] = new JsonObject { ["server"] = "sql01", ["hours"] = 168 },
            ["answer"] = Answer(withFields),
        };

        if (!ComposeDataFloorLiveTests.TryRender(scenario, out var drawn, zone))
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return null;
        }

        Assert.Empty(drawn.GetProperty("errors").EnumerateArray());
        return Assert.Single(drawn.GetProperty("notices").EnumerateArray()).GetString();
    }

    [Fact]
    public void TheNotice_IsWrittenInTheBrowsersZone_NotNamedInUtc_AndTheRowCapSentenceStays()
    {
        var shown = Drawn(withFields: true, "Asia/Kolkata");
        if (shown is null)
        {
            return;
        }

        /* 2026-01-02 00:00 UTC is 05:30 in Kolkata, and the window's start 2025-12-26 05:30: the page's own localTime. */
        Assert.StartsWith("partial window: this panel's data starts at ", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("UTC", shown, StringComparison.Ordinal);
        Assert.Contains("5:30", shown, StringComparison.Ordinal);
        Assert.EndsWith(" " + RowCapSentence, shown, StringComparison.Ordinal);
    }

    [Fact]
    public void AnAnswerWithoutTheInstants_IsDrawnAsTheServerSentIt()
    {
        var shown = Drawn(withFields: false, "Asia/Kolkata");
        if (shown is null)
        {
            return;
        }

        Assert.Equal(Sentence + " " + RowCapSentence, shown);
    }
}
