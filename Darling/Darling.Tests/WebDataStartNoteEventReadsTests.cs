/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The event-time rule and the four SQL Server event reads in the web's data-start note (#4966), the facts that need no
/// store. The store's side is <see cref="WebDataStartNoteLiveTests"/>.
/// </summary>
public sealed class WebDataStartNoteEventReadsTests
{
    private const string WindowEnd = "2026-01-03T00:00:00Z";

    /* A data source that never connects and a token that is already cancelled: a read that asks the store throws
       OperationCanceledException (see WebDataStartNoteTests.NeverConnects). */
    private static NpgsqlDataSource NeverConnects() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=9;Username=x;Database=x;Timeout=1;Pooling=false");

    private static readonly CancellationToken Cancelled = new(canceled: true);

    private const string DefaultTraceCapped =
        "{\"server\":\"sql01\",\"hours_back\":48,\"total_events\":250,\"shown\":2,\"events\":["
        + "{\"event_time\":\"2026-01-02T18:00:00.0000000Z\",\"event_name\":\"Log File Auto Grow\"},"
        + "{\"event_time\":\"2026-01-02T12:30:00.0000000Z\",\"event_name\":\"Log File Auto Grow\"}]}";

    private const string LongQueriesTruncated =
        "{\"server\":\"sql01\",\"hours_back\":48,\"completions_returned\":1,\"truncated\":true,"
        + "\"oldest_returned_event_time\":\"2026-01-02T12:30:00.0000000Z\",\"newest_returned_event_time\":\"2026-01-02T12:30:00.0000000Z\","
        + "\"order\":\"duration_ms_desc\",\"completions\":[{\"event_time\":\"2026-01-02T12:30:00.0000000Z\"}]}";

    /// <summary>A Default Trace page cut by its cap (<c>total_events</c> greater than <c>shown</c>) names its oldest event_time
    /// without asking the store; the control, the same page uncut, does reach it.</summary>
    [Fact]
    public async Task ACappedDefaultTracePage_NamesItsOldestEvent_WithoutAskingTheStore()
    {
        await using var store = NeverConnects();

        var answered = await WebDataStartNote.AddAsync(store, "get_default_trace_events", "sql01", 48, WindowEnd, DefaultTraceCapped, null, Cancelled);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T12:30:00.0000000Z", answer["effective_start"]?.GetValue<string>());
        Assert.StartsWith("2026-01-02T12:30:00", answer["oldest_shown_utc"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Null(answer["data_start_utc"]);
        Assert.StartsWith("partial window: this grid shows only the newest rows", answer["truncation_note"]?.GetValue<string>(), StringComparison.Ordinal);

        var uncut = DefaultTraceCapped.Replace("\"total_events\":250", "\"total_events\":2", StringComparison.Ordinal);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_default_trace_events", "sql01", 48, WindowEnd, uncut, null, Cancelled));

        /* A capped page that reaches the window's start says nothing, and asks nothing. */
        var reaches = DefaultTraceCapped.Replace("2026-01-02T12:30:00", "2025-12-31T12:30:00", StringComparison.Ordinal);
        var reached = await WebDataStartNote.AddAsync(store, "get_default_trace_events", "sql01", 48, WindowEnd, reaches, null, Cancelled);
        Assert.Null(Assert.IsType<JsonObject>(JsonNode.Parse(reached))["window_truncated"]);
    }

    /// <summary>The long query page is the SLOWEST runs, so <c>truncated: true</c> says the page is a sample and its oldest event
    /// names no reach: the read keeps the coverage-plus-event rule, so it goes to the store.</summary>
    [Fact]
    public async Task LongQueryCompletions_WithTruncatedTrue_KeepsTheCoverageAndEventRule()
    {
        await using var store = NeverConnects();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_long_query_completions", "sql01", 48, WindowEnd, LongQueriesTruncated, null, Cancelled));
        Assert.DoesNotContain("get_long_query_completions", WebDataStartNote.CappedByRead.Keys);
        Assert.Equal("oldest_returned_event_time", WebDataStartNote.EventTimeByRead["get_long_query_completions"].Field);
    }

    /// <summary>The four reads' premises, in the tools' source: the payload fields the rules read.</summary>
    [Fact]
    public void TheFourEventReads_AreListed_AndTheirToolsAnswerTheFieldsTheRulesRead()
    {
        Assert.Equal("blocked_process_reports", WebDataStartNote.TableByRead["get_blocked_process_xml"]);
        Assert.Equal("long_query_completions", WebDataStartNote.TableByRead["get_long_query_completions"]);
        Assert.Equal("memory_pressure_events", WebDataStartNote.TableByRead["get_memory_pressure_events"]);
        Assert.Equal("default_trace_events", WebDataStartNote.TableByRead["get_default_trace_events"]);

        Assert.True(WebDataStartNote.TryGetReadSource("get_memory_pressure_events", out var memory));
        Assert.Equal("memory_pressure_events", memory.Relation);
        Assert.Equal("sample_time", memory.TimeColumn);

        var blocking = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpBlockingTools.cs");
        Assert.Contains("order = \"event_time_desc\"", blocking, StringComparison.Ordinal);
        Assert.Contains("oldest_returned_event_time = McpHelpers.FormatEffectiveStart(withXml.Min(r => r.EventTime))", blocking, StringComparison.Ordinal);
        var longQueries = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpLongQueryTools.cs");
        Assert.Contains("order = \"duration_ms_desc\"", longQueries, StringComparison.Ordinal);
        var trace = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDefaultTraceTools.cs");
        Assert.Contains("total_events = significant.Count", trace, StringComparison.Ordinal);
        Assert.Contains("shown = events.Count", trace, StringComparison.Ordinal);
        var memoryTool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpMemoryGrantTools.cs");
        Assert.Contains("sample_time = r.SampleTime.ToString(\"o\")", memoryTool, StringComparison.Ordinal);
    }
}
