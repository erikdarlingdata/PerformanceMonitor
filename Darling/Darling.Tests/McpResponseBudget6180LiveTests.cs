/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6180: four reads answered over the shared 32 KB response target with no note. <c>get_perfmon_stats</c> (545 counters,
/// 82 KB) and <c>get_blocked_process_xml</c> (3 reports, 54 KB) were over it at DEFAULT arguments; <c>get_collection_log</c>
/// at <c>limit</c> 1000 (70-86 KB) was over it when the caller asked for more. Each now answers within the target, keeps whole
/// rows only, and says what it left out and how to read the rest. <c>get_collection_health</c> with <c>full_detail</c> is
/// covered in <see cref="CollectionHealthPayloadBudgetLiveTests"/>; the cut itself is checked without a store in
/// <see cref="McpResponseBudgetFitPrefixTests"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class McpResponseBudget6180LiveTests
{
    private const string ServerName = "mcp-budget-6180-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task PerfmonStats_Default_WithHundredsOfCounters_FitsAndSaysWhatItLeftOff()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #6180 budget tests.");
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
            var at = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            for (var i = 0; i < 545; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, "SQLServer:Databases", $"Log Flushes/sec {i}", "UserDb" + (i % 60), 1000L + i, 10L, 60);
            }

            var json = await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName);
            Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
                $"default get_perfmon_stats was {Encoding.UTF8.GetByteCount(json):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var returned = root.GetProperty("counters").GetArrayLength();
            Assert.True(root.GetProperty("truncated").GetBoolean());
            /* The tool's own stamp: the snapshot's collection_time, which the census can no longer find in this file's source. */
            Assert.Equal(at, DateTime.Parse(root.GetProperty("captured_at").GetString()!, null, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal));
            Assert.Equal(545, root.GetProperty("counters_total").GetInt32());
            Assert.Equal(returned, root.GetProperty("counters_returned").GetInt32());
            Assert.InRange(returned, 1, 544);
            Assert.Contains("counter_name", root.GetProperty("size_note").GetString(), StringComparison.Ordinal);

            using var narrow = JsonDocument.Parse(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName, counter_name: "Log Flushes/sec 7", instance_name: "UserDb7"));
            Assert.False(narrow.RootElement.GetProperty("truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, narrow.RootElement.GetProperty("size_note").ValueKind);
            Assert.True(narrow.RootElement.GetProperty("counters").GetArrayLength() >= 1);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task BlockedProcessXml_Default_KeepsWholeReportsThatFit_AndListsTheRest()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #6180 budget tests.");
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
            var bigXml = BuildReportXml(18_000);
            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            for (var i = 0; i < 5; i++)
            {
                var t = newest.AddMinutes(-i * 10);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, blocked_process_report_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    /* collection_time is the newest report's for all five: the collector's own coverage then starts AFTER the older
                       events, so the notice's start can only come from the oldest report the page serves. */
                    CollectionIdGenerator.Next(), newest, ServerId, ServerName, t, "StackOverflow", 100 + i, 51, 5000L, bigXml);
            }

            var json = await DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, ServerName);
            Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
                $"default get_blocked_process_xml was {Encoding.UTF8.GetByteCount(json):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var returned = root.GetProperty("reports").GetArrayLength();
            Assert.InRange(returned, 1, 4);
            Assert.Equal(returned, root.GetProperty("reports_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(5 - returned, root.GetProperty("left_out_count").GetInt32());
            Assert.Equal(5 - returned, root.GetProperty("left_out_reports").GetArrayLength());
            Assert.Contains("limit 1", root.GetProperty("size_note").GetString(), StringComparison.Ordinal);

            /* The fields the web's data-start note reads, and the coverage notice, come from the reports SERVED: the page's oldest
               event is the last report returned, and the notice's start is that event, not the older one left out. */
            var served = root.GetProperty("reports");
            var oldestServed = served[returned - 1].GetProperty("event_time").GetString();
            Assert.Equal(DateTime.Parse(oldestServed!, null, System.Globalization.DateTimeStyles.RoundtripKind),
                DateTime.Parse(root.GetProperty("oldest_returned_event_time").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind));
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(root.GetProperty("oldest_returned_event_time").GetString(), root.GetProperty("effective_start").GetString());
            Assert.Equal(JsonValueKind.String, root.GetProperty("truncation_note").ValueKind);
            Assert.Contains("blocked_process_report", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);

            /* Never cut mid-way: every report returned is the whole, well-formed document. */
            foreach (var report in root.GetProperty("reports").EnumerateArray())
            {
                var xml = report.GetProperty("blocked_process_report_xml").GetString()!;
                Assert.Equal(bigXml, xml);
                System.Xml.Linq.XDocument.Parse(xml);
            }

            /* A left-out report is fetched by itself: its event_time as as_of, limit 1. */
            var leftOut = root.GetProperty("left_out_reports")[0];
            using var one = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockedProcessXml(
                postgres, ServerName, hours_back: 24, limit: 1, as_of: leftOut.GetProperty("event_time").GetString()));
            Assert.Equal(1, one.RootElement.GetProperty("reports").GetArrayLength());
            Assert.Equal(leftOut.GetProperty("blocked_spid").GetInt32(), one.RootElement.GetProperty("reports")[0].GetProperty("blocked_spid").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task CollectionLog_LargeLimit_AnswersWithinTheBudget_AndSaysItWasCut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #6180 budget tests.");
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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            for (var i = 0; i < 300; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms,
     duckdb_duration_ms, rows_collected, sql_open_ms, sql_drain_ms, watermark_ms,
     drain_rows_read, drain_bytes_read, drain_last_read_ms, target_session_id, sweep_peer_max_ms)
VALUES ($1, $2, $3, $4, $5, 'SUCCESS', 340, 260, 40, 5000, 180, 60, 12, 5000, 812345, 55, 771, 410)",
                    CollectionIdGenerator.Next(), ServerId, ServerName, i % 3 == 0 ? "query_store" : "wait_stats", now.AddMinutes(-(i + 1)));
            }

            var json = await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: ServerName, hours_back: 24, limit: 1000);
            Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
                $"limit 1000 was {Encoding.UTF8.GetByteCount(json):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var returned = root.GetProperty("runs").GetArrayLength();
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(300, root.GetProperty("runs_total").GetInt32());
            Assert.Equal(returned, root.GetProperty("runs_returned").GetInt32());
            Assert.InRange(returned, 1, 299);
            Assert.Equal(returned, root.GetProperty("run_count").GetInt32());
            Assert.Contains("limit", root.GetProperty("size_note").GetString(), StringComparison.Ordinal);

            /* The fleet form shares the cut. */
            var fleet = await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: "*", hours_back: 24, limit: 1000);
            Assert.True(Encoding.UTF8.GetByteCount(fleet) <= McpResponseBudget.DefaultBytes);
            using var fleetDoc = JsonDocument.Parse(fleet);
            Assert.True(fleetDoc.RootElement.GetProperty("truncated").GetBoolean());
            Assert.Equal(fleetDoc.RootElement.GetProperty("runs").GetArrayLength(), fleetDoc.RootElement.GetProperty("runs_returned").GetInt32());

            /* A request that fits is not marked cut. */
            using var small = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: ServerName, hours_back: 24, limit: 10));
            Assert.Equal(10, small.RootElement.GetProperty("runs").GetArrayLength());
            Assert.Equal(JsonValueKind.Null, small.RootElement.GetProperty("size_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static string BuildReportXml(int approxLength)
    {
        var sb = new StringBuilder("<blocked-process-report monitorLoop=\"1\"><blocked-process><process id=\"p1\"><inputbuf>");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append($"SELECT Id, Title FROM dbo.Posts WHERE ParentId = {i} AND Score > 0;\n");
            i++;
        }

        return sb.Append("</inputbuf></process></blocked-process><blocking-process><process id=\"p2\"/></blocking-process></blocked-process-report>").ToString();
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM perfmon_stats WHERE server_id = {ServerId}; DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #6180: the cut every size-limited read shares, checked without a store. Includes a <c>compare_analysis</c>-shaped page
/// built from real <see cref="PerformanceMonitor.Analysis.ComparisonRow"/> payloads (545 fact rows would be well over the
/// target), and the shared <c>get_perfmon_stats</c> reply both products use.
/// </summary>
public sealed class McpResponseBudgetFitPrefixTests
{
    [Fact]
    public void FitPrefix_KeepsTheLargestPrefixThatFits_AndAlwaysAtLeastOne()
    {
        string Reply(int n) => new string('x', n * 1000);

        var (count, json) = McpResponseBudget.FitPrefix(100, Reply, budgetBytes: 10_500);
        Assert.Equal(10, count);
        Assert.Equal(10_000, json.Length);

        Assert.Equal(100, McpResponseBudget.FitPrefix(100, Reply, budgetBytes: 1_000_000).Count);

        /* One item bigger than the whole budget is still served, whole. */
        Assert.Equal(1, McpResponseBudget.FitPrefix(100, Reply, budgetBytes: 10).Count);
        Assert.Equal(0, McpResponseBudget.FitPrefix(0, Reply, budgetBytes: 10).Count);
    }

    [Fact]
    public void ComparisonFactRows_AreCutFromTheStableTail_AndEveryKeptRowIsWhole()
    {
        var rows = new System.Collections.Generic.List<object>();
        for (var i = 0; i < 200; i++)
        {
            rows.Add(new PerformanceMonitor.Analysis.ComparisonRow
            {
                Key = "WAIT_" + i, Source = "waits", Family = "family_" + i, Presence = "both",
                BaselineValue = 1.5 + i, ComparisonValue = 2.5 + i, Status = i < 20 ? "worse" : "stable", BandSource = "absolute",
            }.ToPayload());
        }

        var (count, json) = McpResponseBudget.FitPrefix(rows.Count, n => JsonSerializer.Serialize(new { facts_returned = n, facts = rows.GetRange(0, n) }));
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        Assert.InRange(count, 20, 199);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(count, doc.RootElement.GetProperty("facts").GetArrayLength());
        Assert.Equal("WAIT_0", doc.RootElement.GetProperty("facts")[0].GetProperty("key").GetString());
    }

    [Fact]
    public void PerfmonLatestReply_CutsToTheBudget_AndNotesHowToNarrow()
    {
        var counters = new System.Collections.Generic.List<System.Collections.Generic.Dictionary<string, object?>>();
        for (var i = 0; i < 545; i++)
        {
            counters.Add(TrendPayloads.PerfmonLatestRow($"Counter {i}", "UserDb" + (i % 60), 1000 + i, 10, 60, null));
        }

        var json = TrendPayloads.PerfmonLatestReply("srv", "2026-10-19T00:00:00.0000000Z", counters);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(545, doc.RootElement.GetProperty("counters_total").GetInt32());
        Assert.Equal(doc.RootElement.GetProperty("counters").GetArrayLength(), doc.RootElement.GetProperty("counters_returned").GetInt32());
        /* A latest read says when it was captured (#3541 A10), cut or not. */
        Assert.Equal("2026-10-19T00:00:00.0000000Z", doc.RootElement.GetProperty("captured_at").GetString());

        var few = JsonDocument.Parse(TrendPayloads.PerfmonLatestReply("srv", "t", counters.GetRange(0, 5)));
        Assert.Equal("t", few.RootElement.GetProperty("captured_at").GetString());
        Assert.False(few.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, few.RootElement.GetProperty("size_note").ValueKind);
    }
}
