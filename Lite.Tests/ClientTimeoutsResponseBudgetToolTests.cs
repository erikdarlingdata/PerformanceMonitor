/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9593: <c>get_client_timeouts</c> fits its reply to <see cref="McpResponseBudget.DefaultBytes"/>, end to end through the tool on a real
/// DuckDB file. A long window with hundreds of sampled attention events carrying 2,000-character statements leaves the oldest events off
/// whole and says so (<c>budget_truncated</c>, <c>budget_note</c>), beside <c>sampled_events_truncated</c>, which keeps meaning the
/// <c>limit</c> cut. The exact count, the rates and the top lists are the whole read's, and a small store answers as before. The Darling twin
/// runs the same shared payload over PostgreSQL.
/// </summary>
public sealed class ClientTimeoutsResponseBudgetToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TimeoutBudgetServer";
    private const int EventCount = 301;
    private const int EventLimit = 300;
    private const int MatchedCalls = 20;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public ClientTimeoutsResponseBudgetToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "ClientTimeoutsBudget_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_configDir)) Directory.Delete(_configDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task ALongWindowOfWideStatements_FitsTheBudget_LeavesTheOldestEventsOff_AndKeepsTheCountAndTopLists()
    {
        var now = DateTime.UtcNow;
        await SeedAsync(now, EventCount);

        var whole = await ReadAsync(now, EventLimit, budgeted: false);
        var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, EventLimit, now.ToString("o"));

        /* The unbudgeted payload really is over the budget, so the cut below is the budget's. */
        Assert.True(Encoding.UTF8.GetByteCount(whole.ToJsonString()) > McpResponseBudget.DefaultBytes);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);

        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        var returned = body.GetProperty("sampled_events_returned").GetInt32();
        var events = body.GetProperty("sampled_events");
        Assert.InRange(returned, 1, EventLimit - 1);
        Assert.Equal(returned, events.GetArrayLength());

        /* The limit cut and the budget cut are separate flags, and both are true. */
        Assert.True(body.GetProperty("sampled_events_truncated").GetBoolean());
        Assert.True(body.GetProperty("budget_truncated").GetBoolean());
        var note = body.GetProperty("budget_note").GetString()!;
        Assert.Contains($"only {returned}", note, StringComparison.Ordinal);
        Assert.Contains($"{EventLimit - returned} were left out", note, StringComparison.Ordinal);
        Assert.Contains("as_of", note, StringComparison.Ordinal);
        Assert.Contains("collected_at", note, StringComparison.Ordinal);
        Assert.DoesNotContain("event_time", note, StringComparison.Ordinal);
        Assert.Contains("hours_back", note, StringComparison.Ordinal);
        Assert.Contains("limit", note, StringComparison.Ordinal);

        /* The window notice still closes the reply. */
        Assert.False(doc.RootElement.GetProperty("window_truncated").GetBoolean());

        /* The newest events are the ones kept, in the order the unbudgeted read lists them. */
        var wholeEvents = whole["sampled_events"]!.AsArray();
        for (var i = 0; i < returned; i++)
        {
            Assert.Equal(wholeEvents[i]!["statement_fingerprint"]!.GetValue<string>(), events[i].GetProperty("statement_fingerprint").GetString());
        }

        /* The count, the rates and every top list are the whole read's. */
        foreach (var key in new[] { "count", "rate_per_minute", "minutes_covered", "collector_runs", "sampled_events_in_range", "top_statements", "top_hosts", "top_databases", "top_client_apps", "excluded_from_alert", "matched_calls" })
        {
            Assert.Equal(whole[key]!.ToJsonString(), body.GetProperty(key).GetRawText());
        }
    }

    [Fact]
    public async Task ASmallStore_AnswersWhole_WithBudgetTruncatedFalse()
    {
        var now = DateTime.UtcNow;
        await SeedAsync(now, 3);

        var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, 20, now.ToString("o"));

        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        Assert.Equal(3, body.GetProperty("sampled_events").GetArrayLength());
        Assert.Equal(3, body.GetProperty("sampled_events_returned").GetInt32());
        Assert.False(body.GetProperty("sampled_events_truncated").GetBoolean());
        Assert.False(body.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("budget_note").ValueKind);
        Assert.Equal(150, body.GetProperty("count").GetInt64());
    }

    /// <summary>
    /// The way <c>budget_note</c> says to read on: <c>as_of</c> bounds <c>collection_time</c> (inclusive) and every event of one collector run shares
    /// the run's <c>collection_time</c>, so a call with <c>as_of</c> at the oldest returned <c>collected_at</c> reads that run again whole and then the
    /// older runs. Each run holds several events, the cut lands inside a run, and each event's <c>event_time</c> is earlier than its run's
    /// <c>collection_time</c>. Following the note page after page lists every event once or more and skips none.
    /// </summary>
    [Fact]
    public async Task AsOfAtTheOldestReturnedCollectedAt_ReadsThatRunAgainWhole_AndReadsOnWithoutSkippingAnEvent()
    {
        const int perRun = 7;
        const int runsWithEvents = 5;
        const int limit = perRun * runsWithEvents;
        var now = DateTime.UtcNow;
        await SeedAsync(now, 0);
        await SeedRunEventsAsync(now, runsWithEvents, perRun);

        var whole = (await ReadAsync(now, limit, budgeted: false))["sampled_events"]!.AsArray();
        Assert.Equal(limit, whole.Count);

        var pages = new List<(int Returned, string[] Fingerprints, string[] CollectedAt)>();
        var asOf = now.ToString("o");
        while (true)
        {
            var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, limit, asOf);
            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
            var fingerprints = listed.Select(e => e.GetProperty("statement_fingerprint").GetString()!).ToArray();
            var collectedAt = listed.Select(e => e.GetProperty("collected_at").GetString()!).ToArray();
            Assert.Equal(listed.Count, body.GetProperty("sampled_events_returned").GetInt32());
            Assert.False(body.GetProperty("sampled_events_truncated").GetBoolean());
            Assert.Equal(collectedAt.OrderByDescending(c => c, StringComparer.Ordinal), collectedAt);
            pages.Add((listed.Count, fingerprints, collectedAt));

            if (!body.GetProperty("budget_truncated").GetBoolean())
            {
                break;
            }

            var oldest = collectedAt[^1];
            Assert.NotEqual(asOf, oldest);
            Assert.True(pages.Count <= runsWithEvents, "each page must move as_of back");
            asOf = oldest;
        }

        /* The first reply cut inside a run, so the second call proves the whole run is read again, not only the events after the cut. */
        var first = pages[0];
        Assert.True(pages.Count > 1);
        Assert.NotEqual(0, first.Returned % perRun);
        var oldestRun = first.CollectedAt[^1];
        var wholeOfOldestRun = whole.Where(e => e!["collected_at"]!.GetValue<string>() == oldestRun).Select(e => e!["statement_fingerprint"]!.GetValue<string>()).ToArray();
        Assert.Equal(perRun, wholeOfOldestRun.Length);
        Assert.Contains(whole[first.Returned]!["statement_fingerprint"]!.GetValue<string>(), pages[1].Fingerprints);
        Assert.All(wholeOfOldestRun, fingerprint => Assert.Contains(fingerprint, pages[1].Fingerprints));

        var every = whole.Select(e => e!["statement_fingerprint"]!.GetValue<string>()).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(limit, every.Count);
        Assert.Equal(every, pages.SelectMany(p => p.Fingerprints).ToHashSet(StringComparer.Ordinal));
    }

    /// <summary>
    /// One run holds more events than a reply fits, so the reply lists a single <c>collected_at</c> and moving <c>as_of</c> to it returns the same page. The
    /// note says so, so a caller that follows it is not sent round again.
    /// </summary>
    [Fact]
    public async Task ARunLargerThanOnePage_ListsOneCollectedAt_AndTheNoteSaysItsOlderEventsCannotBePagedThisWay()
    {
        const int inRun = 30;
        var now = DateTime.UtcNow;
        await SeedAsync(now, 0);
        await SeedRunEventsAsync(now, 1, inRun);

        var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, inRun, now.ToString("o"));

        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
        Assert.InRange(listed.Count, 1, inRun - 1);
        Assert.True(body.GetProperty("budget_truncated").GetBoolean());
        Assert.Single(listed.Select(e => e.GetProperty("collected_at").GetString()).Distinct());
        Assert.Contains(
            "If every listed event has the same collected_at, that one run alone fills the reply and its older events cannot be paged this way.",
            body.GetProperty("budget_note").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>
    /// #9884: twenty matched calls with 2,000-character statements over a thousand sampled events. With the sampled events gone the matched calls alone are
    /// over the budget, so the oldest matched calls are left off whole as well, and <c>count</c>, the rates and every top list stay the whole read's.
    /// </summary>
    [Fact]
    public async Task TwentyWideMatchedCalls_OverAThousandEvents_FitTheBudget_LeaveTheOldestCallsOff_AndKeepTheCountAndTopLists()
    {
        var now = DateTime.UtcNow;
        await SeedAsync(now, 1000);
        await SeedMatchedCallsAsync(now, MatchedCalls);

        var whole = await ReadAsync(now, 20, budgeted: false);
        var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, 20, now.ToString("o"));

        /* The unbudgeted matched calls alone are over the budget, so the cut below is theirs. */
        Assert.Equal(MatchedCalls, whole["matched_calls"]!.AsArray().Count);
        Assert.True(Encoding.UTF8.GetByteCount(whole["matched_calls"]!.ToJsonString()) > McpResponseBudget.DefaultBytes);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);

        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        Assert.True(body.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(0, body.GetProperty("sampled_events_returned").GetInt32());

        var calls = body.GetProperty("matched_calls");
        var returned = body.GetProperty("matched_calls_returned").GetInt32();
        Assert.InRange(returned, 1, MatchedCalls - 1);
        Assert.Equal(returned, calls.GetArrayLength());
        Assert.Equal(MatchedCalls - returned, body.GetProperty("matched_calls_left_out_for_budget").GetInt32());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("matched_calls_statement_cut_to").ValueKind);

        /* The newest calls are the ones kept, whole, in the order the unbudgeted read lists them. */
        var wholeCalls = whole["matched_calls"]!.AsArray();
        for (var i = 0; i < returned; i++)
        {
            Assert.Equal(wholeCalls[i]!.ToJsonString(), calls[i].GetRawText());
        }

        /* The count, the rates and every top list are the whole read's. */
        foreach (var key in new[] { "count", "rate_per_minute", "minutes_covered", "collector_runs", "sampled_events_in_range", "top_statements", "top_hosts", "top_databases", "top_client_apps", "excluded_from_alert" })
        {
            Assert.Equal(whole[key]!.ToJsonString(), body.GetProperty(key).GetRawText());
        }

        Assert.False(doc.RootElement.GetProperty("window_truncated").GetBoolean());
    }

    /// <summary>#9884: the same store at <c>limit</c> 2 holds two sampled events and two matched calls, which fit, so nothing is cut.</summary>
    [Fact]
    public async Task TheSameStoreAtLimit2_IsNotTruncated()
    {
        var now = DateTime.UtcNow;
        await SeedAsync(now, 1000);
        await SeedMatchedCallsAsync(now, MatchedCalls);

        var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, 2, now.ToString("o"));

        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        Assert.False(body.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("budget_note").ValueKind);
        Assert.Equal(2, body.GetProperty("sampled_events").GetArrayLength());
        Assert.Equal(2, body.GetProperty("matched_calls").GetArrayLength());
        Assert.Equal(2, body.GetProperty("matched_calls_returned").GetInt32());
        Assert.Equal(0, body.GetProperty("matched_calls_left_out_for_budget").GetInt32());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("matched_calls_statement_cut_to").ValueKind);
    }

    /// <summary>
    /// #9884: a reply held over the budget by its matched calls says so. The note names <c>matched_calls</c>, says how many were left out, and gives the
    /// remedy that shortens the reply (a smaller <c>limit</c>). It carries no <c>as_of</c> advice, because no sampled event is listed to page from and
    /// moving <c>as_of</c> back cannot shrink the matched calls.
    /// </summary>
    [Fact]
    public async Task WhenMatchedCallsAloneExceedTheBudget_TheNoteNamesMatchedCalls_AndCarriesNoAsOfAdvice()
    {
        var now = DateTime.UtcNow;
        await SeedAsync(now, 1000);
        await SeedMatchedCallsAsync(now, MatchedCalls);

        var json = await McpClientTimeoutTools.GetClientTimeouts(new LocalDataService(_duckDb), _serverManager, ServerName, 1, 20, now.ToString("o"));

        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        var returned = body.GetProperty("matched_calls_returned").GetInt32();
        var note = body.GetProperty("budget_note").GetString()!;
        Assert.Contains("matched_calls", note, StringComparison.Ordinal);
        Assert.Contains($"only {returned} of the {MatchedCalls} matched calls fit", note, StringComparison.Ordinal);
        Assert.Contains($"{MatchedCalls - returned} were left out", note, StringComparison.Ordinal);
        Assert.Contains("limit", note, StringComparison.Ordinal);
        Assert.DoesNotContain("as_of", note, StringComparison.Ordinal);
        Assert.DoesNotContain("hours_back", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// #9884: when even one matched call is over the budget, it is listed with its statement cut to the longest head that fits, and the reply says how
    /// long the head is. The budget is set a few hundred bytes above the reply that lists no matched call, so one whole call cannot fit.
    /// </summary>
    [Fact]
    public async Task OneMatchedCallThatCannotFit_IsListedWithItsStatementCutToAHead_AndTheReplyNamesTheCut()
    {
        var now = DateTime.UtcNow;
        await SeedAsync(now, 1000);
        await SeedMatchedCallsAsync(now, MatchedCalls);

        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(_serverId, now.AddHours(-1), now, 20);
        var (_, reading) = ClientTimeoutsRead.Classify(window, now);
        string Reply(ClientTimeoutsFit fit) => JsonSerializer.Serialize(new { client_timeouts = ClientTimeoutsRead.Payload(reading!, window, samples, 20, 2000, fit) }, McpHelpers.JsonOptions);

        var budget = Encoding.UTF8.GetByteCount(Reply(new ClientTimeoutsFit(0, 0))) + 900;
        var json = ClientTimeoutsRead.FitToBudget(ClientTimeoutsRead.EventsReturned(samples, 20), ClientTimeoutsRead.MatchedCallsReturned(window, 20), 2000, Reply, budget);

        Assert.True(Encoding.UTF8.GetByteCount(json) <= budget);
        using var doc = JsonDocument.Parse(json);
        var body = doc.RootElement.GetProperty("client_timeouts");
        Assert.True(body.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(1, body.GetProperty("matched_calls_returned").GetInt32());
        Assert.Equal(MatchedCalls - 1, body.GetProperty("matched_calls_left_out_for_budget").GetInt32());
        var head = body.GetProperty("matched_calls_statement_cut_to").GetInt32();
        Assert.InRange(head, 1, 1999);

        /* The call listed is the newest, and its statement is a head of the whole one. */
        var whole = (await ReadAsync(now, 20, budgeted: false))["matched_calls"]!.AsArray()[0]!;
        var listed = body.GetProperty("matched_calls")[0];
        Assert.Equal(whole["session_id"]!.GetValue<int>(), listed.GetProperty("session_id").GetInt32());
        var statement = listed.GetProperty("statement").GetString()!;
        Assert.StartsWith(whole["statement"]!.GetValue<string>()[..head], statement, StringComparison.Ordinal);
        Assert.True(statement.Length < whole["statement"]!.GetValue<string>().Length);

        var note = body.GetProperty("budget_note").GetString()!;
        Assert.Contains("matched_calls", note, StringComparison.Ordinal);
        Assert.Contains($"first {head} characters", note, StringComparison.Ordinal);
        Assert.DoesNotContain("as_of", note, StringComparison.Ordinal);
    }

    /// <summary>The whole payload, unbudgeted, over the same rows the tool reads.</summary>
    private async Task<System.Text.Json.Nodes.JsonObject> ReadAsync(DateTime now, int limit, bool budgeted)
    {
        Assert.False(budgeted);
        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(_serverId, now.AddHours(-1), now, limit);
        var (_, reading) = ClientTimeoutsRead.Classify(window, now);
        return System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000), McpHelpers.JsonOptions))!.AsObject();
    }

    /// <summary>Six runs a minute apart with a count of 30 each run (150 over five minutes), and <paramref name="events"/> sampled events with distinct 2,000-character statements, newest first by collection time.</summary>
    private async Task SeedAsync(DateTime now, int events)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        for (var i = 0; i < 6; i++)
        {
            await ExecAsync(connection,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1, $2, $3, $4, $5, 5, 'SUCCESS')",
                9_593_000L + i, _serverId, ServerName, AttentionWindowSql.CollectorName, now.AddSeconds(-30).AddMinutes(-i));
        }

        for (var i = 0; i < 5; i++)
        {
            await ExecAsync(connection,
                "INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) VALUES ($1, $2, $3, $4, 'count', 30)",
                9_593_100L + i, now.AddSeconds(-28).AddMinutes(-i), _serverId, ServerName);
        }

        for (var i = 0; i < events; i++)
        {
            await ExecAsync(connection,
                "INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us) VALUES ($1, $2, $3, $4, 'sample', 0, 'sales', 'app-' || $5, $6, $7, 30000000)",
                9_593_200L + i, now.AddSeconds(-28).AddMilliseconds(-i * 20), _serverId, ServerName, i % 7, "fp-" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture), WideStatement(i));
        }
    }

    /// <summary>
    /// <paramref name="perRun"/> sampled events for each of the <paramref name="runs"/> newest collector runs of <see cref="SeedAsync"/>. The events of one run share
    /// the run's <c>collection_time</c>, as the collector writes them, and each one's <c>event_time</c> is earlier than that, a different time per event.
    /// </summary>
    private async Task SeedRunEventsAsync(DateTime now, int runs, int perRun)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        for (var run = 0; run < runs; run++)
        {
            var collectionTime = now.AddSeconds(-28).AddMinutes(-run);
            for (var j = 0; j < perRun; j++)
            {
                var n = (run * perRun) + j;
                await ExecAsync(connection,
                    "INSERT INTO attention_events (attention_event_id, collection_time, event_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us) VALUES ($1, $2, $3, $4, $5, 'sample', 0, 'sales', 'app-' || $6, $7, $8, 30000000)",
                    9_593_500L + n, collectionTime, collectionTime.AddSeconds(-(5 + (6 * j))), _serverId, ServerName, n % 7, "fp-" + n.ToString("D4", System.Globalization.CultureInfo.InvariantCulture), WideStatement(n));
            }
        }
    }

    /// <summary>
    /// <paramref name="matched"/> captured calls of <see cref="SeedAsync"/>'s newest run: each is a 4 ms attention (under the 1 s floor) from its own session and
    /// the 30 s call it cancelled, 20 ms later, carrying a distinct 2,000-character statement. The newest call has the highest index.
    /// </summary>
    private async Task SeedMatchedCallsAsync(DateTime now, int matched)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        for (var j = 0; j < matched; j++)
        {
            var collectionTime = now.AddSeconds(-28).AddMilliseconds(j * 5);
            var eventTime = collectionTime.AddSeconds(-1);
            var session = 5_000 + j;
            await ExecAsync(connection,
                "INSERT INTO attention_events (attention_event_id, collection_time, event_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, session_id, interrupt_duration_us) VALUES ($1, $2, $3, $4, $5, 'sample', 0, 'sales', 'etl', $6, 4000)",
                9_595_000L + (2 * j), collectionTime, eventTime, _serverId, ServerName, session);
            await ExecAsync(connection,
                "INSERT INTO attention_events (attention_event_id, collection_time, event_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, client_hostname, session_id, statement_fingerprint, statement_text, interrupt_duration_us) VALUES ($1, $2, $3, $4, $5, 'aborted', 0, 'sales', 'etl', 'web01', $6, $7, $8, 30000000)",
                9_595_001L + (2 * j), collectionTime, eventTime.AddMilliseconds(20), _serverId, ServerName, session,
                "fp-call-" + j.ToString("D4", System.Globalization.CultureInfo.InvariantCulture), WideCall(j));
        }
    }

    private static string WideCall(int n) =>
        ("EXEC dbo.Slow /* call " + n.ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'y');

    private static string WideStatement(int n) =>
        ("SELECT /* event " + n.ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'x');

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object?[] values)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
        {
            command.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
