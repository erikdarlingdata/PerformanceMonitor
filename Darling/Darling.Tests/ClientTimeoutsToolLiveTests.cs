/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_client_timeouts</c> (#5640) end to end on a real PostgreSQL store: <see cref="DarlingMcpClientTimeoutTools.GetClientTimeouts"/>
/// from the server resolver to the serialized answer, over seeded <c>attention_events</c> and <c>collection_log</c> rows. A quiet
/// window, a busy one, a stale collector, a PostgreSQL target, and ranges that cut a run's span (checked against what
/// <see cref="AttentionEvaluator.ReadSpan"/> says over the same rows, so the tool and the alert cannot disagree). Gated on DARLING_TEST_PG.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class ClientTimeoutsToolLiveTests
{
    private const int QuietId = -5640101;
    private const int BusyId = -5640102;
    private const int StaleId = -5640103;
    private const int PostgresTargetId = -5640104;
    private const int CutId = -5640105;
    private const int AsOfEdgeId = -5640106;
    private const int OneRunId = -5640107;
    private const int LeakId = -5640108;
    private const int TieId = -5640109;
    private const int MissingSessionId = -5640110;
    private const int DurationFloorId = -5640111;
    private const int MatchedCallsId = -5640112;
    private const int NearMissId = -5640113;
    private const int MetadataCallsId = -5640114;
    private const int NoCallsId = -5640115;
    private const int TwoCallsId = -5640116;
    private const int ShortCallsId = -5640117;
    private const int BudgetId = -9593101;
    private const int SmallStoreId = -9593102;
    private const int PagingId = -9600101;
    private const int BigRunId = -9600102;
    private const int MatchedBudgetId = -9884101;

    private static readonly int[] AllIds = { QuietId, BusyId, StaleId, PostgresTargetId, CutId, AsOfEdgeId, OneRunId, LeakId, TieId, MissingSessionId, DurationFloorId, MatchedCallsId, ShortCallsId, NearMissId, MetadataCallsId, NoCallsId, TwoCallsId, BudgetId, SmallStoreId, PagingId, BigRunId, MatchedBudgetId };

    private static string Name(int id) => "ct-live" + (-id).ToString(CultureInfo.InvariantCulture);

    [Fact]
    public async Task AQuietWindow_RunsWithNoCountRows_IsEmpty_AtRateZero()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, QuietId, "sqlserver", ct);
            await SeedRunsAsync(connection, QuietId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(QuietId), 1, cancellationToken: ct);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("No client timeouts", json, StringComparison.Ordinal);

            /* The same rows through the shared judgement: counted, and the count is zero. */
            var (window, _) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, QuietId, now.AddHours(-1), now, 20, ct);
            var (kind, reading) = ClientTimeoutsRead.Classify(window, now);
            Assert.Equal(ClientTimeoutsKind.Quiet, kind);
            Assert.Equal(0, reading!.TotalCount);
            Assert.Equal(12, reading.RunCount + 1);
        });
    }

    [Fact]
    public async Task ABusyWindow_GivesTheExactCountAndRate_AndTheSampledTopLists_WithTheNote()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, BusyId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, BusyId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);

            /* A count row of 5 for EVERY run, the oldest too: the oldest is the base run and its own row is not in the span. */
            foreach (var run in runs)
            {
                await RowAsync(connection, BusyId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            await RowAsync(connection, BusyId, runs[0].AddSeconds(2), "sample", 0, "db1", "etl", "fp-a", "SELECT a FROM t", ct);
            await RowAsync(connection, BusyId, runs[1].AddSeconds(2), "sample", 0, "db1", "etl", "fp-a", "SELECT a FROM t", ct);
            await RowAsync(connection, BusyId, runs[2].AddSeconds(2), "sample", 0, "db1", "etl", "fp-a", "SELECT a FROM t", ct);
            await RowAsync(connection, BusyId, runs[1].AddSeconds(2), "sample", 0, "db2", "web", "fp-b", "SELECT b FROM t", ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(BusyId), 1, limit: 2, cancellationToken: ct);

            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.Equal(55, body.GetProperty("count").GetInt64());
            Assert.Equal(11, body.GetProperty("minutes_covered").GetDouble());
            Assert.Equal(5.0, body.GetProperty("rate_per_minute").GetDouble());
            Assert.Equal(11, body.GetProperty("collector_runs").GetInt32());
            Assert.Equal(4, body.GetProperty("sampled_events_in_range").GetInt64());
            Assert.Contains("subset", body.GetProperty("samples_note").GetString(), StringComparison.Ordinal);
            Assert.Contains("never how many", body.GetProperty("samples_note").GetString(), StringComparison.Ordinal);

            var statements = body.GetProperty("top_statements");
            Assert.Equal("fp-a", statements[0].GetProperty("statement_fingerprint").GetString());
            Assert.Equal(3, statements[0].GetProperty("sampled_events").GetInt64());
            Assert.Equal("SELECT a FROM t", statements[0].GetProperty("sample_statement").GetString());
            Assert.Equal("fp-b", statements[1].GetProperty("statement_fingerprint").GetString());
            Assert.Equal("db1", body.GetProperty("top_databases")[0].GetProperty("database_name").GetString());
            Assert.Equal(3, body.GetProperty("top_databases")[0].GetProperty("sampled_events").GetInt64());
            Assert.Equal("etl", body.GetProperty("top_client_apps")[0].GetProperty("client_app_name").GetString());
            Assert.Equal("web", body.GetProperty("top_client_apps")[1].GetProperty("client_app_name").GetString());

            /* The limit cuts the sampled events only, never the count or the top lists. */
            Assert.True(body.GetProperty("sampled_events_truncated").GetBoolean());
            Assert.Equal(2, body.GetProperty("sampled_events").GetArrayLength());
            Assert.Equal(2, body.GetProperty("sampled_events_returned").GetInt32());
            Assert.Equal(2, statements.GetArrayLength());

            /* The boundary pair: a limit equal to the four sampled events is not truncated, one below is. */
            foreach (var (limitValue, truncated) in new[] { (4, false), (3, true) })
            {
                using var page = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(BusyId), 1, limit: limitValue, cancellationToken: ct));
                var pageBody = page.RootElement.GetProperty("client_timeouts");
                Assert.Equal(truncated, pageBody.GetProperty("sampled_events_truncated").GetBoolean());
                Assert.Equal(limitValue, pageBody.GetProperty("sampled_events_returned").GetInt32());
                Assert.Equal(55, pageBody.GetProperty("count").GetInt64());
            }
        });
    }

    [Fact]
    public async Task TheOneSecondFloor_LeavesFourMillisecondEventsOut_ButEveryEventStaysListed_AndTheJudgedCountDropsThem()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, DurationFloorId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, DurationFloorId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, DurationFloorId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            /* #9030: two 4 ms events are under the 1 s floor. A 30 s event, one at exactly 1 s and one with no recorded duration are not. */
            var at = runs[1].AddSeconds(2);
            await RowAsync(connection, DurationFloorId, at, "sample", 0, "db1", "etl", "fp-ms", "SELECT 1", ct, durationUs: 4_000);
            await RowAsync(connection, DurationFloorId, at, "sample", 0, "db1", "web", "fp-ms", "SELECT 1", ct, durationUs: 4_000);
            await RowAsync(connection, DurationFloorId, at, "sample", 0, "db1", "etl", "fp-30s", "SELECT 2", ct, durationUs: 30_000_000);
            await RowAsync(connection, DurationFloorId, at, "sample", 0, "db1", "etl", "fp-1s", "SELECT 3", ct, durationUs: 1_000_000);
            await RowAsync(connection, DurationFloorId, at, "sample", 0, "db1", "etl", "fp-null", "SELECT 4", ct, durationUs: null);

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(DurationFloorId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            /* Every event is still counted and listed. The exact count is the collector's, the floor only reshapes what the alert judges. */
            Assert.Equal(55, body.GetProperty("count").GetInt64());
            Assert.Equal(5, body.GetProperty("sampled_events_in_range").GetInt64());
            var excluded = body.GetProperty("excluded_from_alert");
            Assert.Equal(2, excluded.GetProperty("below_duration_floor_events").GetInt64());
            Assert.Equal(55 * 3 / 5.0, excluded.GetProperty("count_judged").GetDouble());

            var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
            Assert.Equal(5, listed.Count);
            Assert.Equal(new[] { "fp-ms", "fp-ms" }, listed.Where(e => e.GetProperty("below_duration_floor").GetBoolean())
                .Select(e => e.GetProperty("statement_fingerprint").GetString()!).ToArray());
            Assert.Equal(new[] { "fp-1s", "fp-30s", "fp-null" }, listed.Where(e => !e.GetProperty("below_duration_floor").GetBoolean())
                .Select(e => e.GetProperty("statement_fingerprint").GetString()!).OrderBy(f => f, StringComparer.Ordinal).ToArray());
        });
    }

    /// <summary>Seeds a server whose collector ran 12 times a minute apart, with 5 attentions counted by every run (55 in the span), and returns the
    /// collection time the sampled rows are written at plus the event time the attention samples carry (a second before it).</summary>
    private static async Task<(DateTime At, DateTime EventTime)> SeedJudgedSpanAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        var now = Now();
        await RegisterAsync(connection, serverId, "sqlserver", ct);
        var runs = await SeedRunsAsync(connection, serverId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
        foreach (var run in runs)
        {
            await RowAsync(connection, serverId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
        }

        var at = runs[1].AddSeconds(2);
        return (at, at.AddSeconds(-1));
    }

    [Fact]
    public async Task AnUnderFloorAttention_ThatMatchesACapturedAbortedCall_IsJudged_AndEachCallIsListedWithHowLongItRan()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var (at, eventTime) = await SeedJudgedSpanAsync(connection, MatchedCallsId, ct);

            /* Eight 4 ms attentions, each from its own session, each with the 30 s call it cancelled captured 20 ms later. */
            for (var i = 0; i < 8; i++)
            {
                await RowAsync(connection, MatchedCallsId, at, "sample", 0, "db1", "etl", null, null, ct,
                    durationUs: 4_000, eventTime: eventTime, sessionId: 100 + i);
                await RowAsync(connection, MatchedCallsId, at, "aborted", 0, "db1", "etl", "fp-call", "dbo.GetOrders: EXEC dbo.GetOrders @id = 1", ct,
                    durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(20), sessionId: 100 + i, host: "web01");
            }

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(MatchedCallsId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            Assert.Equal(55, body.GetProperty("count").GetInt64());
            Assert.Equal(8, body.GetProperty("sampled_events_in_range").GetInt64());
            var excluded = body.GetProperty("excluded_from_alert");
            Assert.Equal(0, excluded.GetProperty("below_duration_floor_events").GetInt64());
            Assert.Equal(8, excluded.GetProperty("matched_calls").GetInt64());
            Assert.Equal(55.0, excluded.GetProperty("count_judged").GetDouble());

            var calls = body.GetProperty("matched_calls").EnumerateArray().ToList();
            Assert.Equal(8, calls.Count);
            Assert.All(calls, c =>
            {
                Assert.Equal(30.0, c.GetProperty("elapsed_seconds").GetDouble());
                Assert.Equal("db1", c.GetProperty("database_name").GetString());
                Assert.Equal("etl", c.GetProperty("client_app_name").GetString());
                Assert.Equal("web01", c.GetProperty("client_hostname").GetString());
                Assert.Contains("dbo.GetOrders", c.GetProperty("statement").GetString(), StringComparison.Ordinal);
            });
            Assert.Equal(Enumerable.Range(100, 8), calls.Select(c => c.GetProperty("session_id").GetInt32()).OrderBy(s => s));
            Assert.False(body.GetProperty("matched_calls_truncated").GetBoolean());

            var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
            Assert.Equal(8, listed.Count);
            Assert.All(listed, e =>
            {
                Assert.True(e.GetProperty("matched_call").GetBoolean());
                Assert.False(e.GetProperty("below_duration_floor").GetBoolean());
            });

            /* The same rows through the shared judgement: the alert judges the matched events. */
            var (window, _) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, MatchedCallsId, Now().AddHours(-1), Now(), 20, ct);
            var (_, reading) = ClientTimeoutsRead.Classify(window, Now());
            Assert.Equal(8, reading!.MatchedSamples);
            Assert.Equal(0, reading.ShortSamples);
        });
    }

    [Fact]
    public async Task ACallThatRanUnderTheFloor_IsNotACommandTimeout_ButOneThatRanLongerIs()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var (at, eventTime) = await SeedJudgedSpanAsync(connection, ShortCallsId, ct);

            /* Three 4 ms attentions. Session 1 cancelled a call 50 ms into its run (a data layer cancelling before it closes a reader), session 2 a call
               that had run 5 s (a command timeout), session 3 a call that ran 0.95 s (a 1 s command timeout recorded a little short, under the 1 s attention floor but over the capture floor). Only the longer two are command timeouts (#9538). */
            for (var session = 1; session <= 3; session++)
            {
                await RowAsync(connection, ShortCallsId, at, "sample", 0, "db1", "etl", null, null, ct, durationUs: 4_000, eventTime: eventTime, sessionId: session);
            }

            await RowAsync(connection, ShortCallsId, at, "aborted", 0, "db1", "etl", "fp-call", "dbo.A: EXEC dbo.A", ct, durationUs: 50_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 1);
            await RowAsync(connection, ShortCallsId, at, "aborted", 0, "db1", "etl", "fp-call", "dbo.B: EXEC dbo.B", ct, durationUs: 5_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 2);
            await RowAsync(connection, ShortCallsId, at, "aborted", 0, "db1", "etl", "fp-call", "dbo.C: EXEC dbo.C", ct, durationUs: 950_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 3);

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(ShortCallsId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            Assert.Equal(3, body.GetProperty("sampled_events_in_range").GetInt64());
            var excluded = body.GetProperty("excluded_from_alert");
            Assert.Equal(1, excluded.GetProperty("below_duration_floor_events").GetInt64());
            Assert.Equal(2, excluded.GetProperty("matched_calls").GetInt64());
            Assert.Equal(55 * 2 / 3.0, excluded.GetProperty("count_judged").GetDouble(), 1);

            var calls = body.GetProperty("matched_calls").EnumerateArray().ToList();
            Assert.Equal(new[] { 2, 3 }, calls.Select(c => c.GetProperty("session_id").GetInt32()).OrderBy(x => x));

            var (window, _) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, ShortCallsId, Now().AddHours(-1), Now(), 20, ct);
            var (_, reading) = ClientTimeoutsRead.Classify(window, Now());
            Assert.Equal(2, reading!.MatchedSamples);
            Assert.Equal(1, reading.ShortSamples);
        });
    }

    [Fact]
    public async Task AnUnderFloorAttention_WithNoAbortedCallRows_KeepsEveryNumberItHadBefore()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var (at, eventTime) = await SeedJudgedSpanAsync(connection, NoCallsId, ct);

            /* The #9030 shape: two 4 ms attentions, and a 30 s one, one at exactly 1 s and one with no duration. Session and event time are recorded, no call is. */
            await RowAsync(connection, NoCallsId, at, "sample", 0, "db1", "etl", "fp-ms", "SELECT 1", ct, durationUs: 4_000, eventTime: eventTime, sessionId: 51);
            await RowAsync(connection, NoCallsId, at, "sample", 0, "db1", "web", "fp-ms", "SELECT 1", ct, durationUs: 4_000, eventTime: eventTime, sessionId: 52);
            await RowAsync(connection, NoCallsId, at, "sample", 0, "db1", "etl", "fp-30s", "SELECT 2", ct, durationUs: 30_000_000, eventTime: eventTime, sessionId: 53);
            await RowAsync(connection, NoCallsId, at, "sample", 0, "db1", "etl", "fp-1s", "SELECT 3", ct, durationUs: 1_000_000, eventTime: eventTime, sessionId: 54);
            await RowAsync(connection, NoCallsId, at, "sample", 0, "db1", "etl", "fp-null", "SELECT 4", ct, durationUs: null, eventTime: eventTime, sessionId: 55);

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(NoCallsId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            Assert.Equal(55, body.GetProperty("count").GetInt64());
            Assert.Equal(5, body.GetProperty("sampled_events_in_range").GetInt64());
            var excluded = body.GetProperty("excluded_from_alert");
            Assert.Equal(2, excluded.GetProperty("below_duration_floor_events").GetInt64());
            Assert.Equal(0, excluded.GetProperty("matched_calls").GetInt64());
            Assert.Equal(0, excluded.GetProperty("azdata_metadata_events").GetInt64());
            Assert.Equal(0, excluded.GetProperty("dbatools_metadata_events").GetInt64());
            Assert.Equal(55 * 3 / 5.0, excluded.GetProperty("count_judged").GetDouble());
            Assert.Equal(0, body.GetProperty("matched_calls").GetArrayLength());

            var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
            Assert.Equal(2, listed.Count(e => e.GetProperty("below_duration_floor").GetBoolean()));
            Assert.DoesNotContain(listed, e => e.GetProperty("matched_call").GetBoolean());
        });
    }

    private static readonly TimeSpan WindowAfter = TimeSpan.FromMicroseconds(AttentionWindowSql.CapturedCallMatchAfterUs);

    private static readonly TimeSpan ToleranceBefore = TimeSpan.FromMicroseconds(AttentionWindowSql.CapturedCallMatchBeforeUs);

    [Fact]
    public async Task ACallOnAnotherSession_AnotherDatabase_OrOutsideTheMatchWindow_IsNotAMatch_ButTheWindowEdgesAre()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var (at, eventTime) = await SeedJudgedSpanAsync(connection, NearMissId, ct);

            /* Six 4 ms attentions, one session each. Only the last two have a call inside the window, at its two edges. */
            var cases = new (int Session, string Db, TimeSpan Offset, int CallSession)[]
            {
                (201, "db1", TimeSpan.FromMilliseconds(100), 299),
                (202, "db1", WindowAfter + TimeSpan.FromMilliseconds(1), 202),
                (203, "db2", TimeSpan.FromMilliseconds(100), 203),
                (204, "db1", -(ToleranceBefore + TimeSpan.FromMilliseconds(1)), 204),
                (205, "db1", WindowAfter, 205),
                (206, "db1", -ToleranceBefore, 206),
            };
            foreach (var (session, db, offset, callSession) in cases)
            {
                await RowAsync(connection, NearMissId, at, "sample", 0, db, "etl", null, null, ct, durationUs: 4_000, eventTime: eventTime, sessionId: session);
                await RowAsync(connection, NearMissId, at, "aborted", 0, "db1", "etl", "fp-call", "dbo.Slow: EXEC dbo.Slow", ct,
                    durationUs: 30_000_000, eventTime: eventTime + offset, sessionId: callSession);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(NearMissId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            Assert.Equal(6, body.GetProperty("sampled_events_in_range").GetInt64());
            var excluded = body.GetProperty("excluded_from_alert");
            Assert.Equal(2, excluded.GetProperty("matched_calls").GetInt64());
            Assert.Equal(4, excluded.GetProperty("below_duration_floor_events").GetInt64());
            Assert.Equal(55 * 2 / 6.0, excluded.GetProperty("count_judged").GetDouble(), 1);
            Assert.Equal(new[] { 205, 206 }, body.GetProperty("matched_calls").EnumerateArray().Select(c => c.GetProperty("session_id").GetInt32()).OrderBy(s => s).ToArray());
        });
    }

    [Fact]
    public async Task TheToolExclusions_StillApplyToAMatchedAttention_ByTheCallsOwnStatement()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var (at, eventTime) = await SeedJudgedSpanAsync(connection, MetadataCallsId, ct);

            /* The attention row carries no statement, the call does. An azdata metadata call and a dbatools SMO call are left out. An azdata editor query is judged. */
            var cases = new (int Session, string Db, string App, string Text)[]
            {
                (301, "master", AttentionWindowSql.AzdataClientApp, "create table #SVer(ID int, Name sysname)"),
                (302, "db1", AttentionWindowSql.DbatoolsClientApp, "dbo.Smo: SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0"),
                (303, "db1", AttentionWindowSql.AzdataClientApp, "SELECT * FROM dbo.Orders"),
            };
            foreach (var (session, db, app, text) in cases)
            {
                await RowAsync(connection, MetadataCallsId, at, "sample", 0, db, app, null, null, ct, durationUs: 4_000, eventTime: eventTime, sessionId: session);
                await RowAsync(connection, MetadataCallsId, at, "aborted", 0, db, app, "fp-call", text, ct,
                    durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: session);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(MetadataCallsId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            var excluded = body.GetProperty("excluded_from_alert");
            Assert.Equal(1, excluded.GetProperty("azdata_metadata_events").GetInt64());
            Assert.Equal(1, excluded.GetProperty("dbatools_metadata_events").GetInt64());
            Assert.Equal(1, excluded.GetProperty("matched_calls").GetInt64());
            Assert.Equal(0, excluded.GetProperty("below_duration_floor_events").GetInt64());
            Assert.Equal(55 / 3.0, excluded.GetProperty("count_judged").GetDouble(), 1);

            var calls = body.GetProperty("matched_calls").EnumerateArray().ToList();
            Assert.Single(calls);
            Assert.Equal(303, calls[0].GetProperty("session_id").GetInt32());
        });
    }

    /* #9507 review: the groups count a sample as metadata when ANY call it matches is metadata work, so the listed sample says the same.
       The ordinary call is the earlier one here, so a list that read only the earliest call would call the sample ordinary. */
    [Fact]
    public async Task ASampleThatMatchesAMetadataCallAndAnOrdinaryOne_IsListedAsMetadata_LikeTheGroupsCountIt()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var (at, eventTime) = await SeedJudgedSpanAsync(connection, TwoCallsId, ct);

            await RowAsync(connection, TwoCallsId, at, "sample", 0, "master", "etl", null, null, ct, durationUs: 4_000, eventTime: eventTime, sessionId: 401);
            await RowAsync(connection, TwoCallsId, at, "aborted", 0, "master", AttentionWindowSql.AzdataClientApp, "fp-call", "SELECT * FROM dbo.Orders", ct,
                durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 401);
            await RowAsync(connection, TwoCallsId, at, "aborted", 0, "master", AttentionWindowSql.AzdataClientApp, "fp-meta", "create table #SVer(ID int, Name sysname)", ct,
                durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(30), sessionId: 401);

            /* A second attention, whose only call is an ordinary one, stays ordinary. */
            await RowAsync(connection, TwoCallsId, at, "sample", 0, "master", "etl", null, null, ct, durationUs: 4_000, eventTime: eventTime, sessionId: 402);
            await RowAsync(connection, TwoCallsId, at, "aborted", 0, "master", AttentionWindowSql.AzdataClientApp, "fp-call", "SELECT * FROM dbo.Orders", ct,
                durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 402);

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(TwoCallsId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");

            Assert.Equal(1, body.GetProperty("excluded_from_alert").GetProperty("azdata_metadata_events").GetInt64());
            var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
            Assert.Equal(2, listed.Count);
            Assert.Equal(1, listed.Count(e => e.GetProperty("azdata_metadata").GetBoolean()));
            Assert.All(listed, e => Assert.True(e.GetProperty("matched_call").GetBoolean()));
            Assert.Equal(
                body.GetProperty("excluded_from_alert").GetProperty("azdata_metadata_events").GetInt64(),
                listed.Count(e => e.GetProperty("azdata_metadata").GetBoolean()));
        });
    }

    [Fact]
    public async Task AStaleCollector_NewestSuccessfulRunOutsideTheFreshnessWindow_IsUnavailable()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, StaleId, "sqlserver", ct);

            /* One-minute cadence, newest run 25 minutes ago: the freshness window is 10 minutes. Counts exist, and must not be claimed. */
            var runs = await SeedRunsAsync(connection, StaleId, now.AddMinutes(-25), 12, TimeSpan.FromMinutes(1), ct);
            await RowAsync(connection, StaleId, runs[0].AddSeconds(2), "count", 9, null, null, null, null, ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(StaleId), 1, cancellationToken: ct);

            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("has not observed", json, StringComparison.Ordinal);
            Assert.DoesNotContain("client_timeouts", json, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// The precondition arm of the tool (#5640): the collector's catalog name is what the question is asked about, so a latest run that recorded
    /// SESSION_MISSING replaces the generic "has not observed" sentence, while a collector with no newer failure keeps the unavailable one.
    /// </summary>
    [Fact]
    public async Task AMissingCaptureSession_OnTheLatestRun_IsAPrecondition_NotTheGenericUnavailable()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, MissingSessionId, "sqlserver", ct);

            /* Stale successes (freshness window 10 minutes, newest 25 minutes ago), then a newer run that found the session gone. */
            await SeedRunsAsync(connection, MissingSessionId, now.AddMinutes(-25), 12, TimeSpan.FromMinutes(1), ct);

            var before = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(MissingSessionId), 1, cancellationToken: ct);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(before));

            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message) VALUES ($1, $2, $3, $4, $5, 5, 'SESSION_MISSING', 'The specified object PerformanceMonitor_Attention could not be found.')",
                Math.Abs((long)MissingSessionId) * 1000 + 500, MissingSessionId, Name(MissingSessionId), AttentionWindowSql.CollectorName, now.AddMinutes(-1));

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(MissingSessionId), 1, cancellationToken: ct);

            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("attention collector", json, StringComparison.Ordinal);
            Assert.Contains("ALTER ANY EVENT SESSION", json, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ASingleRun_CannotBeSpanned_AndIsUnavailable()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, OneRunId, "sqlserver", ct);
            await SeedRunsAsync(connection, OneRunId, now.AddSeconds(-20), 1, TimeSpan.FromMinutes(1), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(OneRunId), 1, cancellationToken: ct);

            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(json));
        });
    }

    [Fact]
    public async Task APostgresTarget_IsNotCollected_NotEmpty()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, PostgresTargetId, MonitoredEngineKind.Postgres, ct);
            await SeedRunsAsync(connection, PostgresTargetId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(PostgresTargetId), 1, cancellationToken: ct);

            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(json));
            Assert.Contains("attention", json, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ARangeThatCutsARunsSpan_AgreesWithAttentionEvaluatorReadSpan()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, CutId, "sqlserver", ct);

            /* 18 runs five minutes apart (85 minutes), a count row of 7 at each. hours_back = 1 cuts the oldest six runs off. */
            var runs = await SeedRunsAsync(connection, CutId, now.AddSeconds(-30), 18, TimeSpan.FromMinutes(5), ct);
            var counts = new List<AttentionCountRow>();
            foreach (var run in runs)
            {
                await RowAsync(connection, CutId, run.AddSeconds(2), "count", 7, null, null, null, null, ct);
                counts.Add(new AttentionCountRow(run.AddSeconds(2), 7));
            }

            /* hours_back = 1: the runs inside the hour. */
            var floor = now.AddHours(-1);
            var expected = Expected(runs, counts, floor, now, now);
            Assert.Equal(12, expected.RunCount + 1);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(CutId), 1, cancellationToken: ct);
            AssertAgrees(json, expected);
            Assert.Equal(77, expected.TotalCount);

            /* hours_back = 2: the whole history, a different span. */
            var wide = Expected(runs, counts, now.AddHours(-2), now, now);
            AssertAgrees(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(CutId), 2, cancellationToken: ct), wide);
            Assert.NotEqual(expected.TotalCount, wide.TotalCount);

            /* as_of 20 minutes ago: later runs fall off the end, and freshness is judged at as_of, not now. */
            var asOf = new DateTime(now.AddMinutes(-20).Ticks - (now.AddMinutes(-20).Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            var anchored = Expected(runs, counts, asOf.AddHours(-1), asOf, asOf);
            AssertAgrees(
                await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(CutId), 1, as_of: asOf.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture), cancellationToken: ct),
                anchored);
        });
    }

    [Fact]
    public async Task AnAsOfJustAfterARun_StillCountsThatRunsRow_LikeTheAlertDoes()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, AsOfEdgeId, "sqlserver", ct);

            /* The collection log stamps a run when it starts; its rows carry the time the runner took a moment later (a few
               seconds). An as_of that lands between the two must still count that run's row, as ReadSpan's skew allowance does. */
            var newest = new DateTime((now.AddMinutes(-5).Ticks / TimeSpan.TicksPerSecond) * TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
            var runs = await SeedRunsAsync(connection, AsOfEdgeId, newest, 6, TimeSpan.FromMinutes(1), ct);
            var counts = new List<AttentionCountRow>();
            foreach (var run in runs)
            {
                await RowAsync(connection, AsOfEdgeId, run.AddSeconds(3), "count", 4, null, null, null, null, ct);
                counts.Add(new AttentionCountRow(run.AddSeconds(3), 4));
            }

            var asOf = newest.AddSeconds(1);
            var expected = Expected(runs, counts, asOf.AddHours(-1), asOf, asOf);
            Assert.Equal(20, expected.TotalCount);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(
                postgres, Name(AsOfEdgeId), 1, as_of: asOf.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture), cancellationToken: ct);
            AssertAgrees(json, expected);
        });
    }

    [Fact]
    public async Task AnAsOf_DoesNotLetTheRowAllowanceLeakIntoTheTopListsOrTheSampledEvents()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, LeakId, "sqlserver", ct);

            /* The as_of row allowance (as_of + 30 s) and the range start are wider than the span ReadSpan counts (the midpoint after the BASE
               run to the newest run + 30 s). The top lists and the samples are read over exactly the span (#5640, round 2). */
            var newest = new DateTime((now.AddMinutes(-5).Ticks / TimeSpan.TicksPerSecond) * TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
            var runs = await SeedRunsAsync(connection, LeakId, newest, 5, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, LeakId, run.AddSeconds(3), "count", 4, null, null, null, null, ct);
            }

            await RowAsync(connection, LeakId, runs[4].AddSeconds(3), "sample", 0, "basedb", "BaseApp", "fp-base", "SELECT base", ct);
            await RowAsync(connection, LeakId, runs[3].AddSeconds(3), "sample", 0, "spandb", "SpanApp", "fp-span", "SELECT span", ct);
            await RowAsync(connection, LeakId, newest.AddSeconds(30.5), "sample", 0, "latedb", "LateApp", "fp-late", "SELECT late", ct);

            var asOf = newest.AddSeconds(1);
            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, LeakId, asOf.AddHours(-1), asOf, 20, ct);
            var (kind, reading) = ClientTimeoutsRead.Classify(window, asOf);

            Assert.Equal(ClientTimeoutsKind.Data, kind);
            Assert.Equal(1, reading!.SampleTotal);
            Assert.Equal(new[] { "SpanApp" }, window.TopApplications.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "spandb" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "fp-span" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "SELECT span" }, samples.Select(x => x.StatementText).ToArray());

            /* And through the tool: no list sums past sampled_events_in_range. */
            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(
                postgres, Name(LeakId), 1, as_of: asOf.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture), cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.Equal(1, body.GetProperty("sampled_events_in_range").GetInt64());
            Assert.Equal(1, body.GetProperty("top_client_apps").GetArrayLength());
            Assert.Equal(1, body.GetProperty("sampled_events").GetArrayLength());
        });
    }

    [Fact]
    public async Task Ties_AreOrderedTheSameWayInBothStores_BinaryNamesAndNewestIdFirst()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, TieId, "sqlserver", ct);

            /* Count ties break by name in binary order whatever the store collation ("App_b" < "app_a" in binary, the reverse under most
               locales; the Lite twin pins the same list over DuckDB), the sampled events break ties on the unique id, and the sample text
               of a fingerprint is the highest id's among same-instant samples (#5640, round 2). */
            var newest = new DateTime((now.AddMinutes(-1).Ticks / TimeSpan.TicksPerSecond) * TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
            var runs = await SeedRunsAsync(connection, TieId, newest, 3, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, TieId, run.AddSeconds(3), "count", 5, null, null, null, null, ct);
            }

            var at = newest.AddSeconds(3);
            var written = new List<string>();
            async Task Sample(string? db, string? app, string? fingerprint, string text)
            {
                await RowAsync(connection, TieId, at, "sample", 0, db, app, fingerprint, text, ct);
                written.Add(text);
            }

            foreach (var app in new[] { "a1", "a2", "a3", "a4" })
            {
                await Sample("db1", app, null, app + "-x");
                await Sample("db1", app, null, app + "-y");
            }

            await Sample("db1", "app_a", null, "app_a-x");
            await Sample("db1", "App_b", null, "App_b-x");
            await Sample(null, null, "fp-pick", "pick1");
            await Sample(null, null, "fp-pick", "pick2");
            await Sample(null, null, "fp-pick", "pick3");

            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, TieId, newest.AddHours(-1), newest, 20, ct);

            Assert.Equal(new[] { "a1", "a2", "a3", "a4", "App_b" }, window.TopApplications.Select(t => t.Name).ToArray());
            Assert.Equal("pick3", window.TopStatements.Single().SampleText);

            var expected = Enumerable.Reverse(written).ToArray();
            Assert.Equal(expected, samples.Select(x => x.StatementText).ToArray());

            var (_, page) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, TieId, newest.AddHours(-1), newest, 4, ct);
            Assert.Equal(expected.Take(5).ToArray(), page.Select(x => x.StatementText).ToArray());
        });
    }

    /// <summary>What the alert's own rule says about the runs and count rows inside [floor, ceiling], judged at <paramref name="at"/>.</summary>
    private static AttentionReading Expected(
        IReadOnlyList<DateTime> runs, IReadOnlyList<AttentionCountRow> counts, DateTime floor, DateTime ceiling, DateTime at)
    {
        var inRuns = runs.Where(r => r >= floor && r <= ceiling).OrderByDescending(r => r).ToList();

        /* The alert's rule reads rows up to newest run + skew; the stored read is cut at the range end. The seeded rows sit within
           a few seconds of their run, so a row is in range when its RUN is. */
        var inCounts = counts.Where(c => runs.Any(r => r <= ceiling && r >= floor && c.CollectionTime >= r && c.CollectionTime < r.AddSeconds(10))).ToList();
        var reading = AttentionEvaluator.ReadSpan(
            new AttentionWindow(inRuns, inCounts, Array.Empty<AttentionSampleGroup>(), Array.Empty<AttentionTopItem>(),
                Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>()),
            at);
        return reading ?? throw new InvalidOperationException("the seeded range is not observed under the alert's own rule");
    }

    private static void AssertAgrees(string json, AttentionReading expected)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("client_timeouts", out var body), "expected data, got: " + json);
        Assert.Equal(expected.TotalCount, body.GetProperty("count").GetInt64());
        Assert.Equal(expected.RunCount, body.GetProperty("collector_runs").GetInt32());
        Assert.Equal(Math.Round(expected.CoveredMinutes, 1, MidpointRounding.AwayFromZero), body.GetProperty("minutes_covered").GetDouble());
        Assert.Equal(
            Math.Round(expected.TotalCount / expected.CoveredMinutes, 2, MidpointRounding.AwayFromZero),
            body.GetProperty("rate_per_minute").GetDouble());
    }

    /// <summary>
    /// #9593: a long window of sampled events carrying 2,000-character statements answers within the response budget. The oldest events are
    /// left off whole, <c>budget_truncated</c> says so beside <c>sampled_events_truncated</c> (the <c>limit</c> cut), and the count, the rates and the top
    /// lists are the whole read's. The Lite twin runs the same shared payload over DuckDB.
    /// </summary>
    [Fact]
    public async Task ALongWindowOfWideStatements_FitsTheResponseBudget_AndKeepsTheCountAndTopLists()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            const int events = 301;
            const int limit = 300;
            var now = Now();
            await RegisterAsync(connection, BudgetId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, BudgetId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, BudgetId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            for (var i = 0; i < events; i++)
            {
                var text = ("SELECT /* event " + i.ToString("D4", CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'x');
                await RowAsync(connection, BudgetId, runs[0].AddSeconds(2).AddMilliseconds(-i * 20), "sample", 0, "sales", "app-" + (i % 7).ToString(CultureInfo.InvariantCulture),
                    "fp-" + i.ToString("D4", CultureInfo.InvariantCulture), text, ct, durationUs: 30_000_000);
            }

            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, BudgetId, now.AddHours(-1), now, limit, ct);
            var (_, reading) = ClientTimeoutsRead.Classify(window, now);
            var whole = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(
                ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000), McpHelpers.JsonOptions))!.AsObject();
            Assert.True(System.Text.Encoding.UTF8.GetByteCount(whole.ToJsonString()) > McpResponseBudget.DefaultBytes);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(BudgetId), 1, limit: limit, cancellationToken: ct);

            Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            var returned = body.GetProperty("sampled_events_returned").GetInt32();
            var listed = body.GetProperty("sampled_events");
            Assert.InRange(returned, 1, limit - 1);
            Assert.Equal(returned, listed.GetArrayLength());
            Assert.True(body.GetProperty("sampled_events_truncated").GetBoolean());
            Assert.True(body.GetProperty("budget_truncated").GetBoolean());
            var note = body.GetProperty("budget_note").GetString()!;
            Assert.Contains($"only {returned}", note, StringComparison.Ordinal);
            Assert.Contains($"{limit - returned} were left out", note, StringComparison.Ordinal);
            Assert.Contains("as_of", note, StringComparison.Ordinal);
            Assert.Contains("collected_at", note, StringComparison.Ordinal);
            Assert.DoesNotContain("event_time", note, StringComparison.Ordinal);
            Assert.Contains("hours_back", note, StringComparison.Ordinal);
            Assert.Contains("limit", note, StringComparison.Ordinal);
            Assert.False(doc.RootElement.GetProperty("window_truncated").GetBoolean());

            /* The newest events are the ones kept, in the order the unbudgeted read lists them. */
            var wholeEvents = whole["sampled_events"]!.AsArray();
            for (var i = 0; i < returned; i++)
            {
                Assert.Equal(wholeEvents[i]!["statement_fingerprint"]!.GetValue<string>(), listed[i].GetProperty("statement_fingerprint").GetString());
            }

            foreach (var key in new[] { "count", "rate_per_minute", "minutes_covered", "collector_runs", "sampled_events_in_range", "top_statements", "top_hosts", "top_databases", "top_client_apps", "excluded_from_alert", "matched_calls" })
            {
                Assert.Equal(whole[key]!.ToJsonString(), body.GetProperty(key).GetRawText());
            }
        });
    }

    [Fact]
    public async Task ASmallStore_AnswersWhole_WithBudgetTruncatedFalse()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await RegisterAsync(connection, SmallStoreId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, SmallStoreId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, SmallStoreId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            for (var i = 0; i < 3; i++)
            {
                await RowAsync(connection, SmallStoreId, runs[0].AddSeconds(2).AddMilliseconds(-i * 20), "sample", 0, "sales", "etl", "fp-" + i.ToString(CultureInfo.InvariantCulture),
                    "SELECT " + i.ToString(CultureInfo.InvariantCulture), ct, durationUs: 30_000_000);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(SmallStoreId), 1, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.Equal(3, body.GetProperty("sampled_events").GetArrayLength());
            Assert.Equal(3, body.GetProperty("sampled_events_returned").GetInt32());
            Assert.False(body.GetProperty("sampled_events_truncated").GetBoolean());
            Assert.False(body.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, body.GetProperty("budget_note").ValueKind);
            Assert.Equal(55, body.GetProperty("count").GetInt64());
        });
    }

    /// <summary>
    /// #9884: twenty matched calls with 2,000-character statements over a thousand sampled events. With the sampled events gone the matched calls alone are
    /// over the budget, so the oldest matched calls are left off whole as well, and <c>count</c>, the rates and every top list stay the whole read's. The
    /// same store at <c>limit</c> 2 fits whole. The Lite twin runs the same shared payload over DuckDB.
    /// </summary>
    [Fact]
    public async Task TwentyWideMatchedCalls_OverAThousandEvents_FitTheBudget_LeaveTheOldestCallsOff_AndKeepTheCountAndTopLists()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await SeedWideMatchedCallsAsync(connection, now, ct);

            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, MatchedBudgetId, now.AddHours(-1), now, 20, ct);
            var (_, reading) = ClientTimeoutsRead.Classify(window, now);
            var whole = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(
                ClientTimeoutsRead.Payload(reading!, window, samples, 20, 2000), McpHelpers.JsonOptions))!.AsObject();

            /* The unbudgeted matched calls alone are over the budget, so the cut below is theirs. */
            Assert.Equal(WideMatchedCalls, whole["matched_calls"]!.AsArray().Count);
            Assert.True(System.Text.Encoding.UTF8.GetByteCount(whole["matched_calls"]!.ToJsonString()) > McpResponseBudget.DefaultBytes);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(MatchedBudgetId), 1, limit: 20, cancellationToken: ct);

            Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.True(body.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(0, body.GetProperty("sampled_events_returned").GetInt32());

            var calls = body.GetProperty("matched_calls");
            var returned = body.GetProperty("matched_calls_returned").GetInt32();
            Assert.InRange(returned, 1, WideMatchedCalls - 1);
            Assert.Equal(returned, calls.GetArrayLength());
            Assert.Equal(WideMatchedCalls - returned, body.GetProperty("matched_calls_left_out_for_budget").GetInt32());
            Assert.Equal(JsonValueKind.Null, body.GetProperty("matched_calls_statement_cut_to").ValueKind);

            /* The newest calls are the ones kept, whole, in the order the unbudgeted read lists them. */
            var wholeCalls = whole["matched_calls"]!.AsArray();
            for (var i = 0; i < returned; i++)
            {
                Assert.Equal(wholeCalls[i]!.ToJsonString(), calls[i].GetRawText());
            }

            foreach (var key in new[] { "count", "rate_per_minute", "minutes_covered", "collector_runs", "sampled_events_in_range", "top_statements", "top_hosts", "top_databases", "top_client_apps", "excluded_from_alert" })
            {
                Assert.Equal(whole[key]!.ToJsonString(), body.GetProperty(key).GetRawText());
            }

            Assert.False(doc.RootElement.GetProperty("window_truncated").GetBoolean());

            /* The note names the block that held the reply over the budget and the remedy that shortens it, and carries no as_of advice. */
            var note = body.GetProperty("budget_note").GetString()!;
            Assert.Contains("matched_calls", note, StringComparison.Ordinal);
            Assert.Contains($"only {returned} of the {WideMatchedCalls} matched calls fit", note, StringComparison.Ordinal);
            Assert.Contains($"{WideMatchedCalls - returned} were left out", note, StringComparison.Ordinal);
            Assert.Contains("limit", note, StringComparison.Ordinal);
            Assert.DoesNotContain("as_of", note, StringComparison.Ordinal);
            Assert.DoesNotContain("hours_back", note, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheSameStoreAtLimit2_IsNotTruncated()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            await SeedWideMatchedCallsAsync(connection, Now(), ct);

            var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(MatchedBudgetId), 1, limit: 2, cancellationToken: ct);

            Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes);
            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.False(body.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, body.GetProperty("budget_note").ValueKind);
            Assert.Equal(2, body.GetProperty("sampled_events").GetArrayLength());
            Assert.Equal(2, body.GetProperty("matched_calls").GetArrayLength());
            Assert.Equal(2, body.GetProperty("matched_calls_returned").GetInt32());
            Assert.Equal(0, body.GetProperty("matched_calls_left_out_for_budget").GetInt32());
            Assert.Equal(JsonValueKind.Null, body.GetProperty("matched_calls_statement_cut_to").ValueKind);
        });
    }

    /// <summary>
    /// #9884: when even one matched call is over the budget, it is listed with its statement cut to the longest head that fits, and the reply says how
    /// long the head is. The budget is set a few hundred bytes above the reply that lists no matched call, so one whole call cannot fit.
    /// </summary>
    [Fact]
    public async Task OneMatchedCallThatCannotFit_IsListedWithItsStatementCutToAHead_AndTheReplyNamesTheCut()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            var now = Now();
            await SeedWideMatchedCallsAsync(connection, now, ct);

            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, MatchedBudgetId, now.AddHours(-1), now, 20, ct);
            var (_, reading) = ClientTimeoutsRead.Classify(window, now);
            string Reply(ClientTimeoutsFit fit) => JsonSerializer.Serialize(new { client_timeouts = ClientTimeoutsRead.Payload(reading!, window, samples, 20, 2000, fit) }, McpHelpers.JsonOptions);

            var budget = System.Text.Encoding.UTF8.GetByteCount(Reply(new ClientTimeoutsFit(0, 0))) + 900;
            var json = ClientTimeoutsRead.FitToBudget(ClientTimeoutsRead.EventsReturned(samples, 20), ClientTimeoutsRead.MatchedCallsReturned(window, 20), 2000, Reply, budget);

            Assert.True(System.Text.Encoding.UTF8.GetByteCount(json) <= budget);
            using var doc = JsonDocument.Parse(json);
            var body = doc.RootElement.GetProperty("client_timeouts");
            Assert.True(body.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(1, body.GetProperty("matched_calls_returned").GetInt32());
            Assert.Equal(WideMatchedCalls - 1, body.GetProperty("matched_calls_left_out_for_budget").GetInt32());
            var head = body.GetProperty("matched_calls_statement_cut_to").GetInt32();
            Assert.InRange(head, 1, 1999);

            /* The call listed is the newest, and its statement is a head of the whole one. */
            var whole = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(
                ClientTimeoutsRead.Payload(reading!, window, samples, 20, 2000), McpHelpers.JsonOptions))!["matched_calls"]!.AsArray()[0]!;
            var listed = body.GetProperty("matched_calls")[0];
            Assert.Equal(whole["session_id"]!.GetValue<int>(), listed.GetProperty("session_id").GetInt32());
            var statement = listed.GetProperty("statement").GetString()!;
            Assert.StartsWith(whole["statement"]!.GetValue<string>()[..head], statement, StringComparison.Ordinal);
            Assert.True(statement.Length < whole["statement"]!.GetValue<string>().Length);

            var note = body.GetProperty("budget_note").GetString()!;
            Assert.Contains("matched_calls", note, StringComparison.Ordinal);
            Assert.Contains($"first {head} characters", note, StringComparison.Ordinal);
            Assert.DoesNotContain("as_of", note, StringComparison.Ordinal);
        });
    }

    private const int WideMatchedCalls = 20;

    /// <summary>
    /// A thousand sampled events with distinct 2,000-character statements, and <see cref="WideMatchedCalls"/> captured calls of the newest run: each is a 4 ms
    /// attention (under the 1 s floor) from its own session and the 30 s call it cancelled, 20 ms later, carrying a distinct 2,000-character statement. The
    /// newest call has the highest index.
    /// </summary>
    private static async Task SeedWideMatchedCallsAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await RegisterAsync(connection, MatchedBudgetId, "sqlserver", ct);
        var runs = await SeedRunsAsync(connection, MatchedBudgetId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
        foreach (var run in runs)
        {
            await RowAsync(connection, MatchedBudgetId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
        }

        for (var i = 0; i < 1000; i++)
        {
            var text = ("SELECT /* event " + i.ToString("D4", CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'x');
            await RowAsync(connection, MatchedBudgetId, runs[0].AddSeconds(2).AddMilliseconds(-i * 20), "sample", 0, "sales", "app-" + (i % 7).ToString(CultureInfo.InvariantCulture),
                "fp-" + i.ToString("D4", CultureInfo.InvariantCulture), text, ct, durationUs: 30_000_000);
        }

        for (var j = 0; j < WideMatchedCalls; j++)
        {
            var collectionTime = runs[0].AddSeconds(2).AddMilliseconds(j * 5);
            var eventTime = collectionTime.AddSeconds(-1);
            var call = ("EXEC dbo.Slow /* call " + j.ToString("D4", CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'y');
            await RowAsync(connection, MatchedBudgetId, collectionTime, "sample", 0, "sales", "etl", null, null, ct,
                durationUs: 4_000, eventTime: eventTime, sessionId: 5_000 + j);
            await RowAsync(connection, MatchedBudgetId, collectionTime, "aborted", 0, "sales", "etl", "fp-call-" + j.ToString("D4", CultureInfo.InvariantCulture), call, ct,
                durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(20), sessionId: 5_000 + j, host: "web01");
        }
    }

    /// <summary>
    /// The way <c>budget_note</c> says to read on: <c>as_of</c> bounds <c>collection_time</c> (inclusive) and every event of one collector run shares
    /// the run's <c>collection_time</c>, so a call with <c>as_of</c> at the oldest returned <c>collected_at</c> reads that run again whole and then the
    /// older runs. Each run holds several events, the cut lands inside a run, and each event's <c>event_time</c> is earlier than its run's
    /// <c>collection_time</c>. Following the note page after page lists every event once or more and skips none. The Lite twin runs the same shared payload over DuckDB.
    /// </summary>
    [Fact]
    public async Task AsOfAtTheOldestReturnedCollectedAt_ReadsThatRunAgainWhole_AndReadsOnWithoutSkippingAnEvent()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            const int perRun = 7;
            const int runsWithEvents = 5;
            const int limit = perRun * runsWithEvents;
            var now = Now();
            await RegisterAsync(connection, PagingId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, PagingId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, PagingId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            for (var r = 0; r < runsWithEvents; r++)
            {
                var collectionTime = runs[r].AddSeconds(2);
                for (var j = 0; j < perRun; j++)
                {
                    var n = (r * perRun) + j;
                    var text = ("SELECT /* event " + n.ToString("D4", CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'x');
                    await RowAsync(connection, PagingId, collectionTime, "sample", 0, "sales", "app-" + (n % 7).ToString(CultureInfo.InvariantCulture),
                        "fp-" + n.ToString("D4", CultureInfo.InvariantCulture), text, ct, durationUs: 30_000_000, eventTime: collectionTime.AddSeconds(-(5 + (6 * j))));
                }
            }

            var (window, samples) = await DarlingMcpClientTimeoutTools.ReadRangeAsync(postgres, PagingId, now.AddHours(-1), now, limit, ct);
            var (_, reading) = ClientTimeoutsRead.Classify(window, now);
            var whole = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(
                ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000), McpHelpers.JsonOptions))!.AsObject()["sampled_events"]!.AsArray();
            Assert.Equal(limit, whole.Count);

            var pages = new List<(int Returned, string[] Fingerprints, string[] CollectedAt)>();
            string? asOf = null;
            while (true)
            {
                var json = await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(PagingId), 1, limit: limit, as_of: asOf, cancellationToken: ct);
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
        });
    }

    /// <summary>
    /// One run holds more events than a reply fits, so the reply lists a single <c>collected_at</c> and moving <c>as_of</c> to it returns the same page. The
    /// note says so, so a caller that follows it is not sent round again.
    /// </summary>
    [Fact]
    public async Task ARunLargerThanOnePage_ListsOneCollectedAt_AndTheNoteSaysItsOlderEventsCannotBePagedThisWay()
    {
        await RunAsync(async (postgres, connection, ct) =>
        {
            const int inRun = 30;
            var now = Now();
            await RegisterAsync(connection, BigRunId, "sqlserver", ct);
            var runs = await SeedRunsAsync(connection, BigRunId, now.AddSeconds(-30), 12, TimeSpan.FromMinutes(1), ct);
            foreach (var run in runs)
            {
                await RowAsync(connection, BigRunId, run.AddSeconds(2), "count", 5, null, null, null, null, ct);
            }

            var collectionTime = runs[0].AddSeconds(2);
            for (var n = 0; n < inRun; n++)
            {
                var text = ("SELECT /* event " + n.ToString("D4", CultureInfo.InvariantCulture) + " */ ").PadRight(2000, 'x');
                await RowAsync(connection, BigRunId, collectionTime, "sample", 0, "sales", "app-" + (n % 7).ToString(CultureInfo.InvariantCulture),
                    "fp-" + n.ToString("D4", CultureInfo.InvariantCulture), text, ct, durationUs: 30_000_000, eventTime: collectionTime.AddSeconds(-(5 + (6 * n))));
            }

            using var doc = JsonDocument.Parse(await DarlingMcpClientTimeoutTools.GetClientTimeouts(postgres, Name(BigRunId), 1, limit: inRun, cancellationToken: ct));
            var body = doc.RootElement.GetProperty("client_timeouts");
            var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
            Assert.InRange(listed.Count, 1, inRun - 1);
            Assert.True(body.GetProperty("budget_truncated").GetBoolean());
            Assert.Single(listed.Select(e => e.GetProperty("collected_at").GetString()).Distinct());
            Assert.Contains(
                "If every listed event has the same collected_at, that one run alone fills the reply and its older events cannot be paged this way.",
                body.GetProperty("budget_note").GetString()!, StringComparison.Ordinal);
        });
    }

    private static DateTime Now() =>
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_client_timeouts test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            await body(postgres, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "attention_events", "collection_log", "servers" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = ANY($1)", connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, AllIds);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, int serverId, string engineKind, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, Name(serverId), ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $2, sql_engine_edition = $3 WHERE server_id = $1",
            serverId, engineKind, engineKind == MonitoredEngineKind.Postgres ? 0 : 3);
    }

    /// <summary>Seeds <paramref name="count"/> successful runs, the newest at <paramref name="newest"/>, each <paramref name="gap"/> older than the last. Newest first.</summary>
    private static async Task<List<DateTime>> SeedRunsAsync(
        NpgsqlConnection connection, int serverId, DateTime newest, int count, TimeSpan gap, CancellationToken ct)
    {
        var runs = new List<DateTime>();
        for (var i = 0; i < count; i++)
        {
            var time = newest - (gap * i);
            runs.Add(time);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1, $2, $3, $4, $5, 5, 'SUCCESS')",
                Math.Abs((long)serverId) * 1000 + i, serverId, Name(serverId), AttentionWindowSql.CollectorName, time);
        }

        return runs;
    }

    private static long _nextEventId = 5640000000L;

    private static async Task RowAsync(
        NpgsqlConnection connection, int serverId, DateTime time, string kind, long count,
        string? db, string? app, string? fingerprint, string? text, CancellationToken ct, long? durationUs = null,
        DateTime? eventTime = null, int? sessionId = null, string? host = null)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us, event_time, session_id, client_hostname)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, Interlocked.Increment(ref _nextEventId));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Name(serverId));
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)durationUs ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = eventTime is null ? DBNull.Value : DateTime.SpecifyKind(eventTime.Value, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)sessionId ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)host ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
