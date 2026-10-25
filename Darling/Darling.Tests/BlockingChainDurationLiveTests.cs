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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A blocking chain is graded on how long it has held people up, and one still being sampled when the window ends
/// says so. Gated on DARLING_TEST_PG; the facts are read through the product's own collector over rows planted for a
/// session that blocked an UPDATE and a SELECT for 13 minutes.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class BlockingChainDurationLiveTests
{
    private const string ServerName = "darling-blocking-chain-duration";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* Plants a head (500) blocking 501 and 502, sampled once a minute for 13 minutes, the last sample landing
       lastSeenMinutesBeforeEnd minutes before the window end, then collects the facts as the analysis pass would. */
    private static async Task<Fact> CollectChainAsync(int lastSeenMinutesBeforeEnd, int? snapshotEveryMinutes) =>
        (await CollectAsync(lastSeenMinutesBeforeEnd, snapshotEveryMinutes, withPileUps: false)).Chain;

    /* withPileUps adds three short pile-ups (six to eight victims behind one head, over in seconds) that outrank the
       13-minute chain on structure alone, and also reads the drill-down the pass attaches to the finding. */
    private static async Task<(Fact Chain, List<int> DrillDownApexes)> CollectAsync(
        int lastSeenMinutesBeforeEnd, int? snapshotEveryMinutes, bool withPileUps)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);

            var end = new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute, DateTimeKind.Unspecified).AddMinutes(-1);
            var start = end.AddHours(-4);
            for (var m = 0; m <= 240; m += 15)
                await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'CXPACKET',1,10,0)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(m), ServerId, ServerName);

            /* An operator's schedule for the DMV blocking snapshot, which the pass stamps into the context. */
            if (snapshotEveryMinutes is int every)
                await Exec(connection, "INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ($1,'dmv_blocking_snapshot',$2,NULL,TRUE)",
                    ct, ServerId, every);

            var lastSeen = end.AddMinutes(-lastSeenMinutesBeforeEnd);
            var tranStarted = lastSeen.AddMinutes(-14);
            for (var i = 0; i < 13; i++)
                foreach (var victim in new[] { 501, 502 })
                    await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name, blocking_last_tran_started) VALUES ($1,$2,$3,$4,$2,$5,500,$6,'suspended','LiveDb',$7)",
                        ct, CollectionIdGenerator.Next(), lastSeen.AddMinutes(i - 12), ServerId, ServerName, (long)(i + 1) * 60_000L, victim, tranStarted);

            if (withPileUps)
                foreach (var (head, victims) in new[] { (300, 6), (310, 7), (320, 8) })
                    for (var v = 1; v <= victims; v++)
                        await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,4000,$5,$6,'suspended','LiveDb')",
                            ct, CollectionIdGenerator.Next(), end.AddMinutes(-90), ServerId, ServerName, head, head + v);

            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            new FactScorer().ScoreAll(facts);
            var chain = Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN");

            var apexes = new List<int>();
            if (withPileUps)
            {
                var finding = new AnalysisFinding { RootFactKey = "BLOCKING_CHAIN", StoryPath = "BLOCKING_CHAIN", PathKeys = ["BLOCKING_CHAIN"], Severity = 1.0 };
                await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
                var shown = System.Text.Json.JsonSerializer.SerializeToElement(finding.DrillDown!["reconstructed_blocking_chains"]);
                apexes = shown.EnumerateArray().Select(c => c.GetProperty("apex_spid").GetInt32()).ToList();
            }

            bodySucceeded = true;
            return (chain, apexes);
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ChainSampledUpToTheWindowEnd_IsOpen_AndGradesCriticalOnItsThirteenMinuteWait()
    {
        var chain = await CollectChainAsync(lastSeenMinutesBeforeEnd: 1, snapshotEveryMinutes: null);

        Assert.Equal(1, chain.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(780_000, chain.Metadata["worst_chain_max_wait_ms"]);
        Assert.Equal(500, chain.Metadata["worst_apex_spid"]);
        Assert.Equal(2, chain.Metadata["worst_chain_victim_count"]);
        Assert.Equal(1.0, chain.BaseSeverity, precision: 3);
        Assert.True(chain.Metadata.ContainsKey("worst_apex_tran_started_clock_s"));
    }

    [Fact]
    public async Task ChainThatEndedAnHourBeforeTheWindowEnd_IsClosed()
    {
        var chain = await CollectChainAsync(lastSeenMinutesBeforeEnd: 60, snapshotEveryMinutes: null);

        Assert.Equal(0, chain.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(780_000, chain.Metadata["worst_chain_max_wait_ms"]);
    }

    [Fact]
    public async Task TheSnapshotSchedule_DecidesHowRecentIsRecentEnough()
    {
        // Last sampled 6 minutes before the end: closed at the shipped one-minute cadence, open when the snapshot runs every five.
        Assert.Equal(0, (await CollectChainAsync(6, snapshotEveryMinutes: null)).Metadata["worst_chain_open_at_end"]);
        Assert.Equal(1, (await CollectChainAsync(6, snapshotEveryMinutes: 5)).Metadata["worst_chain_open_at_end"]);
    }

    [Fact]
    public async Task ABlockingScheduleOfZero_IsTooCoarseToCallAnyChainOpen()
    {
        // frequency_minutes = 0 reads as the daily on-load recapture: two of those would call a chain last seen
        // an hour before the window end "still blocking". The fact leaves open-at-end out instead.
        var chain = await CollectChainAsync(lastSeenMinutesBeforeEnd: 60, snapshotEveryMinutes: 0);

        Assert.DoesNotContain("worst_chain_open_at_end", chain.Metadata.Keys);
        Assert.Equal(780_000, chain.Metadata["worst_chain_max_wait_ms"]);
    }

    [Fact]
    public async Task DrillDown_ListsTheChainTheFactNames_WhenThreeWiderPileUpsOutrankItOnStructure()
    {
        var (chain, drillDownApexes) = await CollectAsync(lastSeenMinutesBeforeEnd: 1, snapshotEveryMinutes: null, withPileUps: true);

        Assert.Equal(4, chain.Metadata["total_reconstructed_chains"]);
        Assert.Equal(500, chain.Metadata["worst_apex_spid"]);
        Assert.Equal(3, drillDownApexes.Count);
        Assert.Contains(500, drillDownApexes);
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM config_collector_schedules WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM server_properties WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
