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

namespace Darling.Tests;

/// <summary>
/// #7545: the web read of get_blocked_process_xml puts the reports the tool left out for its 32 KB reply size into the grid's rows,
/// and the window note that follows names the oldest row the grid shows and no longer blames a row limit the read did not reach.
/// </summary>
public sealed class WebBlockedReportFoldTests
{
    private const string WindowEnd = "2026-01-03T00:00:00Z";

    /* The tool's answer to limit 5 when only the newest report fits: the other four are listed, not returned. */
    private const string SizeCutAnswer = """
        {"server":"sql01","reports_returned":1,"truncated":true,"more_reports_than_limit":true,
         "oldest_returned_event_time":"2026-01-02T20:00:00.0000000Z","order":"event_time_desc",
         "left_out_count":4,
         "left_out_reports":[
           {"event_time":"2026-01-02T19:00:00.0000000Z","database_name":"A","blocked_spid":51,"blocking_spid":60,"wait_time_ms":10},
           {"event_time":"2026-01-02T18:00:00.0000000Z","database_name":"A","blocked_spid":52,"blocking_spid":60,"wait_time_ms":20},
           {"event_time":"2026-01-02T17:00:00.0000000Z","database_name":"B","blocked_spid":53,"blocking_spid":61,"wait_time_ms":30},
           {"event_time":"2026-01-02T16:00:00.0000000Z","database_name":"B","blocked_spid":54,"blocking_spid":61,"wait_time_ms":40}],
         "size_note":"4 of the 5 report(s) this page selected were left out so the answer stays within about 32 KB",
         "reports":[{"event_time":"2026-01-02T20:00:00.0000000Z","database_name":"A","blocked_spid":50,"blocking_spid":60,"wait_time_ms":5,"blocked_process_report_xml":"<x/>"}]}
        """;

    private static NpgsqlDataSource NeverConnects() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=9;Username=x;Database=x;Timeout=1;Pooling=false");

    [Fact]
    public void LeftOutReports_BecomeRowsWithoutXml_AndTheNoteSaysWhy()
    {
        var folded = Assert.IsType<JsonObject>(JsonNode.Parse(WebBlockedReportFold.Fold(SizeCutAnswer)));

        var rows = Assert.IsType<JsonArray>(folded["reports"]);
        Assert.Equal(5, rows.Count);
        Assert.Null(rows[0]!["xml_left_out"]);
        Assert.Equal("<x/>", rows[0]!["blocked_process_report_xml"]!.GetValue<string>());
        Assert.True(rows[4]!["xml_left_out"]!.GetValue<bool>());
        Assert.Null(rows[4]!["blocked_process_report_xml"]);
        Assert.Equal("sql01", rows[4]!["server"]!.GetValue<string>());
        Assert.Equal("2026-01-02T16:00:00.0000000Z", rows[4]!["event_time"]!.GetValue<string>());
        Assert.Equal("2026-01-02T16:00:00.0000000Z", folded["oldest_returned_event_time"]!.GetValue<string>());
        Assert.Contains("4 reports have no XML here", folded["left_out_note"]!.GetValue<string>());
        Assert.Contains("32 KB", folded["left_out_note"]!.GetValue<string>());
        Assert.Null(folded["left_out_reports"]);
        Assert.Null(folded["size_note"]);
        Assert.Equal(0, folded["left_out_count"]!.GetValue<int>());
    }

    /* #5015: the tool writes oldest_returned_event_time in the UTC round-trip form. The fold's value comes from a row whose own
       event_time text can be naive (no Z), and must still come out in that one form, parseable as UTC. */
    [Fact]
    public void TheOldestReturnedEventTime_IsWrittenInTheUtcForm_EvenWhenTheRowTextIsNaive()
    {
        var naive = SizeCutAnswer.Replace("2026-01-02T16:00:00.0000000Z", "2026-01-02 16:00:00");
        var folded = Assert.IsType<JsonObject>(JsonNode.Parse(WebBlockedReportFold.Fold(naive)));

        var text = folded["oldest_returned_event_time"]!.GetValue<string>();
        Assert.EndsWith("Z", text);
        Assert.Equal("2026-01-02T16:00:00.0000000Z", text);
        var parsed = DateTime.Parse(text, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
    }

    [Fact]
    public async Task TheWindowNote_NamesTheOldestRowTheGridShows_NotTheOldestWithXml()
    {
        await using var store = NeverConnects();
        var cancelled = new CancellationToken(canceled: true);
        var folded = WebBlockedReportFold.Fold(SizeCutAnswer);

        var answered = await WebDataStartNote.AddAsync(store, "get_blocked_process_xml", "sql01", 24, WindowEnd, folded, null, cancelled);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T16:00:00.0000000Z", answer["effective_start"]?.GetValue<string>());
        Assert.Equal("2026-01-02T16:00:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
    }

    [Fact]
    public void APageWhoseRowsAllFit_ExactlyTheLimit_IsNotCalledTruncated()
    {
        /* Five rows listed after the fold and a window that held exactly the limit (the tool says more_reports_than_limit false): the
           size target cut the reply, but every report the page selected is now listed, so the grid shows the whole page and the
           row-limit sentence (which the capped-list note writes for truncated) would be false. */
        var folded = WebBlockedReportFold.Fold(SizeCutAnswer.Replace("\"more_reports_than_limit\":true", "\"more_reports_than_limit\":false"));
        Assert.Equal(5, JsonNode.Parse(folded)!["reports"]!.AsArray().Count);
        Assert.False(JsonNode.Parse(folded)!["truncated"]!.GetValue<bool>());
    }

    [Fact]
    public void AWindowThatHeldMoreThanTheLimit_StaysTruncated_EvenWhenTheGridHoldsExactlyTheLimit()
    {
        var folded = WebBlockedReportFold.Fold(SizeCutAnswer);
        Assert.Equal(5, JsonNode.Parse(folded)!["reports"]!.AsArray().Count);
        Assert.True(JsonNode.Parse(folded)!["truncated"]!.GetValue<bool>());
    }

    [Fact]
    public void AnAnswerWithNothingLeftOut_IsUnchanged()
    {
        const string plain = """{"server":"sql01","reports_returned":1,"truncated":false,"left_out_count":0,"reports":[{"event_time":"2026-01-02T20:00:00Z"}]}""";
        Assert.Equal(plain, WebBlockedReportFold.Fold(plain));
        const string status = """{"status":"empty","message":"none"}""";
        Assert.Equal(status, WebBlockedReportFold.Fold(status));
    }
}
