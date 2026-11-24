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

    private static readonly int[] AllIds = { QuietId, BusyId, StaleId, PostgresTargetId, CutId, AsOfEdgeId, OneRunId, LeakId, TieId, MissingSessionId, DurationFloorId, MatchedCallsId, NearMissId, MetadataCallsId, NoCallsId };

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

            /* Eight 4 ms attentions, each from its own session, each with the 30 s call it cancelled captured 120 ms later. */
            for (var i = 0; i < 8; i++)
            {
                await RowAsync(connection, MatchedCallsId, at, "sample", 0, "db1", "etl", null, null, ct,
                    durationUs: 4_000, eventTime: eventTime, sessionId: 100 + i);
                await RowAsync(connection, MatchedCallsId, at, "aborted", 0, "db1", "etl", "fp-call", "dbo.GetOrders: EXEC dbo.GetOrders @id = 1", ct,
                    durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(120), sessionId: 100 + i, host: "web01");
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
                (202, "db1", AttentionWindowSql.MatchCallWindowAfter + TimeSpan.FromMilliseconds(1), 202),
                (203, "db2", TimeSpan.FromMilliseconds(100), 203),
                (204, "db1", -(AttentionWindowSql.MatchCallToleranceBefore + TimeSpan.FromMilliseconds(1)), 204),
                (205, "db1", AttentionWindowSql.MatchCallWindowAfter, 205),
                (206, "db1", -AttentionWindowSql.MatchCallToleranceBefore, 206),
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
                    durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(80), sessionId: session);
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
