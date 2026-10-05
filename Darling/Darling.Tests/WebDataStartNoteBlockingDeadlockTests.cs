/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The PostgreSQL event logs, SQL Server Blocking and Deadlocks in the web's data-start note (#4966), the facts that need no
/// store. The store's side is <see cref="WebDataStartNoteLiveTests"/>.
/// </summary>
public sealed class WebDataStartNoteBlockingDeadlockTests
{
    private const string WindowEnd = "2026-01-03T00:00:00Z";
    private const string Oldest = "2026-01-02T12:30:00.0000000Z";

    private static NpgsqlDataSource NeverConnects() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=9;Username=x;Database=x;Timeout=1;Pooling=false");

    private static readonly CancellationToken Cancelled = new(canceled: true);

    private static readonly (string Read, string Page)[] CappedPages =
    [
        ("get_pg_deadlocks", "{\"status\":\"deadlocks\",\"deadlock_count\":1,\"truncated\":true,\"deadlocks\":[{\"occurred_at\":\"" + Oldest + "\"}]}"),
        ("get_pg_log_events", "{\"status\":\"events\",\"events_returned\":1,\"truncated\":true,\"order\":\"newest first\",\"oldest_returned_at\":\"" + Oldest + "\"}"),
        ("get_blocking", "{\"truncated\":true,\"oldest_returned_event_time\":\"" + Oldest + "\",\"order\":\"event_time_desc\"}"),
        ("get_deadlocks", "{\"truncated\":true,\"oldest_returned_deadlock_time\":\"" + Oldest + "\",\"order\":\"deadlock_time_desc\"}"),
        ("get_deadlock_detail", "{\"truncated\":true,\"oldest_returned_deadlock_time\":\"" + Oldest + "\",\"order\":\"deadlock_time_desc\"}"),
    ];

    /// <summary>A capped page of each read names its oldest row, with no look at the store.</summary>
    [Fact]
    public async Task ACappedPage_OfEachRead_NamesItsOldestRow_WithoutAskingTheStore()
    {
        await using var store = NeverConnects();
        foreach (var (read, page) in CappedPages)
        {
            var answered = await WebDataStartNote.AddAsync(store, read, "sql01", 48, WindowEnd, page, null, Cancelled);

            var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
            Assert.True(answer["window_truncated"]?.GetValue<bool>(), read);
            Assert.StartsWith("2026-01-02T12:30:00", answer["effective_start"]?.GetValue<string>(), StringComparison.Ordinal);
            Assert.StartsWith("2026-01-02T12:30:00", answer["oldest_shown_utc"]?.GetValue<string>(), StringComparison.Ordinal);
            Assert.Null(answer["data_start_utc"]);

            /* A page that reaches the window's start says nothing. */
            var reaches = page.Replace("2026-01-02T12:30:00", "2025-12-31T12:30:00", StringComparison.Ordinal);
            var reached = await WebDataStartNote.AddAsync(store, read, "sql01", 48, WindowEnd, reaches, null, Cancelled);
            Assert.Null(Assert.IsType<JsonObject>(JsonNode.Parse(reached))["window_truncated"]);
        }
    }

    /// <summary>A page ranked by something other than time is a sample: the coverage rule applies, so the store is asked.</summary>
    [Fact]
    public async Task ARankedPage_TakesTheCoverageRule()
    {
        await using var store = NeverConnects();
        var ranked = CappedPages.First(p => p.Read == "get_blocking").Page.Replace("event_time_desc", "duration_desc", StringComparison.Ordinal);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_blocking", "sql01", 48, WindowEnd, ranked, null, Cancelled));
    }

    /// <summary>The PostgreSQL event words are row words for their own read only; the empty answers are probed like rows.</summary>
    [Fact]
    public async Task TheRowWords_ArePerRead_AndTheEmptyAnswersAreProbed()
    {
        await using var store = NeverConnects();
        const string events = "{\"status\":\"events\",\"events_returned\":0}";

        /* `events` is a row word for get_pg_log_events only: on another read it is an envelope, left alone. */
        Assert.Same(events, await WebDataStartNote.AddAsync(store, "get_waiting_tasks", "sql01", 48, WindowEnd, events, null, Cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_pg_log_events", "sql01", 48, WindowEnd, events, null, Cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_pg_deadlocks", "sql01", 48, WindowEnd, "{\"status\":\"no_deadlocks\",\"message\":\"x\"}", null, Cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_deadlocks", "sql01", 48, WindowEnd, "{\"status\":\"empty\",\"message\":\"x\"}", null, Cancelled));
        const string notCollected = "{\"status\":\"not_collected\",\"message\":\"x\"}";
        Assert.Same(notCollected, await WebDataStartNote.AddAsync(store, "get_blocking", "sql01", 48, WindowEnd, notCollected, null, Cancelled));
        Assert.DoesNotContain("events", WebDataStartNote.RowStatusByRead.Where(kv => kv.Key != "get_pg_log_events").Select(kv => kv.Value));
    }

    /// <summary>get_blocking probes the two tables its tool does, both passed to the floor; every other read has one source.</summary>
    [Fact]
    public void GetBlocking_ProbesBothCaptureTables_AsItsToolDoes()
    {
        Assert.True(WebDataStartNote.TryGetReadSources("get_blocking", out var sources));
        Assert.Equal(["blocked_process_reports", "dmv_blocking_snapshots"], sources.Select(s => s.Relation).ToArray());
        Assert.True(WebDataStartNote.TryGetReadSources("get_deadlocks", out var one));
        Assert.Equal(["deadlocks"], one.Select(s => s.Relation).ToArray());
        Assert.False(WebDataStartNote.TryGetReadSources("get_active_queries", out _));

        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpBlockingTools.cs");
        Assert.Contains("ForCollectorTable(\"blocked_process_reports\"), DataWindowFloor.Source.ForCollectorTable(\"dmv_blocking_snapshots\")", tool, StringComparison.Ordinal);
        var note = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "WebDataStartNote.cs");
        Assert.Contains("postgres, sources, [resolved.ServerName]", note, StringComparison.Ordinal);
    }

    /// <summary>The premises in the tools' source: the fields and order words the rules read.</summary>
    [Fact]
    public void TheToolsAnswer_TheFieldsTheRulesRead()
    {
        var pgDeadlocks = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgDeadlockTools.cs");
        Assert.Contains("status = \"deadlocks\"", pgDeadlocks, StringComparison.Ordinal);
        Assert.Contains("\"no_deadlocks\"", pgDeadlocks, StringComparison.Ordinal);
        Assert.Contains("occurred_at = r.OccurredAtUtc", pgDeadlocks, StringComparison.Ordinal);
        var pgLog = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgLogEventTools.cs");
        Assert.Contains("status = \"events\"", pgLog, StringComparison.Ordinal);
        Assert.Contains("order = \"newest first\"", pgLog, StringComparison.Ordinal);
        Assert.Contains("oldest_returned_at = rows[^1].OccurredAtUtc", pgLog, StringComparison.Ordinal);
        var blocking = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpBlockingTools.cs");
        Assert.Contains("oldest_returned_deadlock_time = McpHelpers.FormatEffectiveStart(page.Min(r => r.DeadlockTime))", blocking, StringComparison.Ordinal);
        Assert.Contains("oldest_returned_deadlock_time = McpHelpers.FormatEffectiveStart(withXml.Min(r => r.DeadlockTime))", blocking, StringComparison.Ordinal);
        Assert.Contains("order = \"deadlock_time_desc\"", blocking, StringComparison.Ordinal);
        Assert.Contains("oldest_returned_event_time = McpHelpers.FormatEffectiveStart(page.Min(r => r.EventTime))", blocking, StringComparison.Ordinal);
    }
}
