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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts reads count the samples from each row's own columns and then correct the groups that hold a matched sample (#9881).
/// These tests plant a week on a real PostgreSQL store and hold the three reads (sample groups, baseline minutes, matched calls) to two
/// things: the numbers worked out by hand for each named case, and a reference read that judges every sample on its own. The reference is plain
/// on purpose: it states the match rule once, as a join of the calls to each sample, and counts per sample id, so any regrouping of the product
/// read that changes a matched pair, a count or a rate shows up as a differing row. Its cost is the planted week once,
/// not the planted week once per sample, so it finishes well inside the command timeout on a fresh store. Gated on DARLING_TEST_PG.
/// </summary>
[Collection("live-postgres")]
public sealed class ClientTimeoutsMatchedReadLiveTests
{
    private const int ServerId = -9881101;
    private const string Dbatools = AttentionWindowSql.DbatoolsClientApp;
    private const string Ssms = AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL " + AttentionWindowSql.SsmsIntelliSenseSuffix;

    /* Every sample judged alone. The calls are read once, as a derived relation joined to the samples and grouped per sample id, so the read costs the planted week
       once and not once per sample (#9881). The match rule is written out here, not taken from the product: a call is an aborted event at or over the floor in the
       same run range, in the same session, in the same database (NULL matching NULL), within 100000 microseconds either side of the sample's event_time.
       An unqualified column in a product predicate names the row of the query level it is written in: c inside the calls relation, s in the outer sample level. */
    private static string Judged(string end) => $"""
sample_calls AS (
    SELECT s.attention_event_id AS sample_id, COUNT(*) AS calls, COUNT(*) FILTER (WHERE cc.az) AS az_calls, COUNT(*) FILTER (WHERE cc.dt) AS dt_calls
    FROM attention_events AS s
    JOIN (
        SELECT c.session_id AS call_session, c.database_name AS call_database, c.event_time AS call_time,
            COALESCE({AttentionWindowSql.AzdataMetadataPredicate}, FALSE) AS az, COALESCE({AttentionWindowSql.DbatoolsSmoMetadataPredicate}, FALSE) AS dt
        FROM attention_events AS c
        WHERE c.server_id = $1 AND c.collection_time >= $2 AND c.collection_time {end} $3 AND c.event_kind = 'aborted' AND c.interrupt_duration_us >= 900000
    ) AS cc
        ON cc.call_session = s.session_id AND cc.call_database IS NOT DISTINCT FROM s.database_name
        AND cc.call_time >= s.event_time - INTERVAL '100000 microseconds' AND cc.call_time <= s.event_time + INTERVAL '100000 microseconds'
    WHERE s.server_id = $1 AND s.collection_time >= $2 AND s.collection_time {end} $3 AND s.event_kind = 'sample'
    GROUP BY s.attention_event_id
),
judged AS (
    SELECT
        s.collection_time, s.database_name, s.client_app_name,
        COALESCE(s.interrupt_duration_us < 1000000, FALSE) AS short,
        COALESCE({AttentionWindowSql.AzdataMetadataPredicate}, FALSE) AS az_own,
        COALESCE({AttentionWindowSql.DbatoolsSmoMetadataPredicate}, FALSE) AS dt_own,
        COALESCE({AttentionWindowSql.ExcludedAppPredicate}, FALSE) AS excluded,
        COALESCE(sc.calls, 0) AS calls, COALESCE(sc.az_calls, 0) AS az_calls, COALESCE(sc.dt_calls, 0) AS dt_calls
    FROM attention_events AS s
    LEFT JOIN sample_calls AS sc ON sc.sample_id = s.attention_event_id
    WHERE s.server_id = $1 AND s.collection_time >= $2 AND s.collection_time {end} $3 AND s.event_kind = 'sample'
)
""";

    private static readonly string ReferenceSampleGroups = $"""
WITH {Judged("<=")}
SELECT collection_time, database_name, client_app_name, COUNT(*),
    COUNT(*) FILTER (WHERE short AND calls = 0),
    COUNT(*) FILTER (WHERE NOT (short AND calls = 0) AND (az_own OR (short AND az_calls > 0))),
    COUNT(*) FILTER (WHERE NOT (short AND calls = 0) AND (dt_own OR (short AND dt_calls > 0))),
    COUNT(*) FILTER (WHERE short AND calls > 0 AND NOT az_own AND az_calls = 0 AND NOT dt_own AND dt_calls = 0)
FROM judged
GROUP BY collection_time, database_name, client_app_name
ORDER BY collection_time DESC, database_name, client_app_name
""";

    private static readonly string ReferenceBaselineMinutes = $"""
WITH {Judged("<")},
kept AS (
    SELECT date_trunc('minute', collection_time) AS minute, COUNT(*) AS kept,
        COUNT(*) FILTER (WHERE (short AND calls = 0) OR az_own OR dt_own OR excluded OR (short AND (az_calls > 0 OR dt_calls > 0))) AS left_out
    FROM judged GROUP BY 1
),
runs AS (
    SELECT DISTINCT date_trunc('minute', collection_time) AS minute FROM collection_log
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND collector_name = 'attention' AND status = 'SUCCESS'
),
counts AS (
    SELECT date_trunc('minute', collection_time) AS minute, SUM(attention_count) AS attentions FROM attention_events
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND event_kind = 'count' GROUP BY 1
)
SELECT r.minute,
    CAST(CASE WHEN COALESCE(k.kept, 0) = 0 OR k.left_out = 0 THEN COALESCE(c.attentions, 0)
         ELSE ROUND(CAST(COALESCE(c.attentions, 0) AS FLOAT8) * (1.0 - CAST(k.left_out AS FLOAT8) / CAST(k.kept AS FLOAT8))) END AS BIGINT)
FROM runs AS r LEFT JOIN counts AS c ON c.minute = r.minute LEFT JOIN kept AS k ON k.minute = r.minute
ORDER BY r.minute
""";

    private static readonly string ReferenceMatchedCalls = $"""
SELECT e.event_time, e.collection_time, e.database_name, e.client_app_name, e.client_hostname, e.session_id, e.interrupt_duration_us, e.statement_text
FROM attention_events AS e
WHERE e.server_id = $1 AND e.collection_time >= $2 AND e.collection_time <= $3 AND e.event_kind = 'aborted' AND e.interrupt_duration_us >= 900000
AND NOT COALESCE({AttentionWindowSql.AzdataMetadataPredicate}, FALSE) AND NOT COALESCE({AttentionWindowSql.DbatoolsSmoMetadataPredicate}, FALSE)
AND EXISTS (
    SELECT 1 FROM attention_events AS s
    WHERE s.server_id = $1 AND s.collection_time > $2 AND s.collection_time <= $3 AND s.event_kind = 'sample' AND s.interrupt_duration_us < 1000000
    AND s.session_id = e.session_id AND s.database_name IS NOT DISTINCT FROM e.database_name
    AND e.event_time >= s.event_time - INTERVAL '100000 microseconds' AND e.event_time <= s.event_time + INTERVAL '100000 microseconds')
ORDER BY e.collection_time DESC, e.event_time DESC NULLS LAST, e.attention_event_id DESC
""";

    [Fact]
    public async Task APairSplitAcrossTwoAdjacentRuns_IsMatched_WhicheverRunHoldsTheCall()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var groups = await GroupsByDatabaseAsync(connection, t0, ct);

            /* The sample is in run 0 and its call in run 1, then the call in run 2 and its sample in run 3: samples, short, azdata, dbatools, matched. */
            Assert.Equal("1,0,0,0,1", groups["split_fwd"]);
            Assert.Equal("1,0,0,0,1", groups["split_back"]);
        });
    }

    [Fact]
    public async Task EachNamedCase_LandsInTheSamePartOfItsGroup_AsJudgingTheSampleAlone()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var groups = await GroupsByDatabaseAsync(connection, t0, ct);

            /* samples, short, azdata metadata, dbatools metadata, matched. */
            Assert.Equal("1,0,1,0,0", groups["own_azdata"]);
            Assert.Equal("1,0,0,1,0", groups["call_dbatools"]);
            Assert.Equal("1,0,0,0,1", groups["ssms"]);
            Assert.Equal("1,0,0,0,1", groups["two_calls"]);
            Assert.Equal("1,0,0,0,1", groups["(null)/app"]);
            Assert.Equal("1,1,0,0,0", groups["no_call"]);
            Assert.Equal("1,0,0,0,0", groups["long"]);
            Assert.Equal("1,1,0,0,0", groups["(null)/other"]);
        });
    }

    [Fact]
    public async Task TheBaselineMinutes_KeepAMatchedSample_AndLeaveOutToolAndIntelliSenseWork()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var minutes = await ReadAsync(connection, AttentionWindowSql.BaselineMinutes("collection_log", "attention_events"), t0, t0.AddMinutes(12), ct);

            /* 100 counted in each minute. Minutes 0 to 3: the two split pairs and two minutes with no sample. 4 to 6: azdata, dbatools and SSMS IntelliSense, all left out.
               7 and 8: two calls on one sample, and the NULL database pair. 9: one short and one long sample. 10: an unmatched short sample. 11: no sample. */
            Assert.Equal(new[] { "100", "100", "100", "100", "0", "0", "0", "100", "100", "50", "0", "100" }, minutes.Select(m => m.Split('|')[1]).ToArray());
        });
    }

    [Fact]
    public async Task TheMatchedCalls_ListEachMatchedCallOnce_AndLeaveOutAnUnmatchedOrToolCall()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var calls = await ReadAsync(connection, AttentionWindowSql.MatchedCalls("attention_events", 500), t0.AddMinutes(-1), t0.AddMinutes(12), ct);

            /* The two split pairs, the call of an azdata sample, two calls on one sample (each once), the SSMS IntelliSense call and the NULL database pair.
               Left out: the dbatools metadata call, and the call that shares a session with a sample but not its database. */
            Assert.Equal(
                new[] { "SELECT 1", "dbo.A: EXEC dbo.A", "dbo.B: EXEC dbo.B", "dbo.C: EXEC dbo.C", "dbo.D: EXEC dbo.D", "dbo.E: EXEC dbo.E", "dbo.F: EXEC dbo.F" },
                calls.Select(c => c.Split('|')[7]).OrderBy(text => text, StringComparer.Ordinal).ToArray());
        });
    }

    [Fact]
    public async Task ThePlantedWeekSampleGroups_ReadTheSameRows_AsJudgingEverySampleOnItsOwn()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var start = t0.AddDays(-7);
            var end = t0.AddDays(1);

            var groups = await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true), start, end, ct);
            Assert.Equal(await ReadAsync(connection, ReferenceSampleGroups, start, end, ct), groups);
            Assert.True(groups.Count > 300, "the planted week must hold its background groups, not only the named cases");
            Assert.Equal(groups, await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events"), start, null, ct));
        });
    }

    [Fact]
    public async Task ThePlantedWeekBaselineMinutes_ReadTheSameRows_AsJudgingEverySampleOnItsOwn()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var start = t0.AddDays(-7);
            var end = t0.AddDays(1);

            var minutes = await ReadAsync(connection, AttentionWindowSql.BaselineMinutes("collection_log", "attention_events"), start, end, ct);
            Assert.Equal(await ReadAsync(connection, ReferenceBaselineMinutes, start, end, ct), minutes);
            Assert.Contains(minutes, m => m.EndsWith("|25", StringComparison.Ordinal));
        });
    }

    [Fact]
    public async Task ThePlantedWeekMatchedCalls_ReadTheSameRows_AsJudgingEverySampleOnItsOwn()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var calls = await ReadAsync(connection, AttentionWindowSql.MatchedCalls("attention_events", 500), t0.AddDays(-7), t0.AddDays(1), ct);
            Assert.Equal(await ReadAsync(connection, ReferenceMatchedCalls, t0.AddDays(-7), t0.AddDays(1), ct), calls);
            Assert.True(calls.Count > 40, "the planted week must hold matched calls beyond the named cases");
        });
    }

    /* #9881: Darling reads the calls first and binds them as arrays. Each read must give the rows it gives when it reads the calls itself. */
    [Fact]
    public async Task ThePlantedWeekReads_GivenTheCallsAsArrays_ReadTheSameRows_AsReadingTheCallsThemselves()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var start = t0.AddDays(-7);
            var end = t0.AddDays(1);
            var calls = await CallsAsync(connection, AttentionWindowSql.Calls("attention_events", bounded: true), start, end, ct);
            Assert.True(calls.Count > 40, "the planted week must hold calls beyond the named cases");

            var groups = await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true), start, end, ct);
            Assert.Contains(groups, row => !row.EndsWith("|0", StringComparison.Ordinal));
            Assert.Equal(groups, await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true, callsAt: 4), start, end, ct, calls.AddTo));
            Assert.Equal(groups, await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", callsAt: 3), start, null, ct, calls.AddTo));

            var matchedCalls = await ReadAsync(connection, AttentionWindowSql.MatchedCalls("attention_events", 500), start, end, ct);
            Assert.True(matchedCalls.Count > 40, "the planted week must hold matched calls beyond the named cases");
            Assert.Equal(matchedCalls, await MatchedCallsFromArraysAsync(connection, start, end, calls, ct));

            var samples = await ReadAsync(connection, AttentionWindowSql.Samples("attention_events", 500), start, end, ct);
            Assert.Contains(samples, row => row.EndsWith("|True", StringComparison.Ordinal));
            Assert.Equal(samples, await ReadAsync(connection, AttentionWindowSql.Samples("attention_events", 500, callsAt: 4), start, end, ct, calls.AddTo));

            var baselineCalls = await CallsAsync(connection, AttentionWindowSql.BaselineCalls("attention_events"), start, end, ct);
            Assert.Equal(calls.Count, baselineCalls.Count);
            var minutes = await ReadAsync(connection, AttentionWindowSql.BaselineMinutes("collection_log", "attention_events"), start, end, ct);
            Assert.Equal(minutes, await ReadAsync(connection, AttentionWindowSql.BaselineMinutes("collection_log", "attention_events", callsAt: 4), start, end, ct, baselineCalls.AddTo));
        });
    }

    /* #9881: with the calls as arrays the join also keys on the second, each call offered to the seconds beside its own. A pair whose two
       times sit in different seconds must still meet, up to and including 100 ms apart, and not one millisecond further. */
    [Fact]
    public async Task APairAcrossASecondBoundary_IsMatchedUpTo100Ms_WithTheCallsAsArrays()
    {
        await RunAsync(async (connection, t0, ct) =>
        {
            var run = t0.AddMinutes(30);
            var boundary = run.AddSeconds(-10);
            long id = 98812000000;
            async Task PairAsync(string db, int session, int sampleMs, int callMs)
            {
                await RowAsync(connection, id++, run, "sample", 0, db, "app", null, 4_000, boundary.AddMilliseconds(sampleMs), session, ct);
                await RowAsync(connection, id++, run, "aborted", 0, db, "app", "dbo.P: EXEC dbo.P", 30_000_000, boundary.AddMilliseconds(callMs), session, ct);
            }

            await PairAsync("call_in_next_second", 901, -50, 40);
            await PairAsync("call_in_previous_second", 902, 40, -50);
            await PairAsync("exactly_100ms_after", 903, -50, 50);
            await PairAsync("exactly_100ms_before", 904, 50, -50);
            await PairAsync("101ms_after", 905, -50, 51);
            await PairAsync("101ms_before", 906, 51, -50);
            await PairAsync("one_second_after", 907, -500, 500);

            var start = run.AddMinutes(-1);
            var end = run.AddMinutes(1);
            var calls = await CallsAsync(connection, AttentionWindowSql.Calls("attention_events", bounded: true), start, end, ct);
            Assert.Equal(7, calls.Count);

            var groups = await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true, callsAt: 4), start, end, ct, calls.AddTo);
            var parts = groups.Select(r => r.Split('|')).ToDictionary(f => f[1], f => string.Join(",", f.Skip(3)));
            const string Matched = "1,0,0,0,1";
            const string Short = "1,1,0,0,0";
            Assert.Equal(Matched, parts["call_in_next_second"]);
            Assert.Equal(Matched, parts["call_in_previous_second"]);
            Assert.Equal(Matched, parts["exactly_100ms_after"]);
            Assert.Equal(Matched, parts["exactly_100ms_before"]);
            Assert.Equal(Short, parts["101ms_after"]);
            Assert.Equal(Short, parts["101ms_before"]);
            Assert.Equal(Short, parts["one_second_after"]);
            Assert.Equal(await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true), start, end, ct), groups);

            var matchedCalls = await MatchedCallsFromArraysAsync(connection, start, end, calls, ct);
            string[] matchedNames = ["call_in_next_second", "call_in_previous_second", "exactly_100ms_after", "exactly_100ms_before"];
            Assert.Equal(matchedNames, matchedCalls.Select(r => r.Split('|')[2]).OrderBy(name => name, StringComparer.Ordinal).ToArray());
            Assert.Equal(await ReadAsync(connection, AttentionWindowSql.MatchedCalls("attention_events", 500), start, end, ct), matchedCalls);

            var samples = await ReadAsync(connection, AttentionWindowSql.Samples("attention_events", 500, callsAt: 4), start, end, ct, calls.AddTo);
            Assert.Equal(4, samples.Count(row => row.EndsWith("|True", StringComparison.Ordinal)));
            Assert.Equal(await ReadAsync(connection, AttentionWindowSql.Samples("attention_events", 500), start, end, ct), samples);
        });
    }

    /* The calls of a range as the arrays the matched reads take. */
    private static async Task<AttentionCallArrays> CallsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));

        var calls = new AttentionCallArrays();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            _ = calls.Add(reader);
        }

        return calls;
    }

    /* The matched calls the way Darling reads them: the ids of the calls a sample matches, then those calls by id. */
    private static async Task<List<string>> MatchedCallsFromArraysAsync(
        NpgsqlConnection connection, DateTime start, DateTime end, AttentionCallArrays calls, CancellationToken ct)
    {
        var ids = (await ReadAsync(connection, AttentionWindowSql.MatchedCallIds("attention_events", 4), start, end, ct, calls.AddTo))
            .Select(id => long.Parse(id, CultureInfo.InvariantCulture))
            .ToArray();
        return await ReadAsync(connection, AttentionWindowSql.MatchedCallRows("attention_events", 500, 4), start, end, ct, command => AttentionCallArrays.AddIds(command, ids));
    }

    private static async Task<Dictionary<string, string>> GroupsByDatabaseAsync(NpgsqlConnection connection, DateTime t0, CancellationToken ct)
    {
        var rows = await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true), t0.AddMinutes(-1), t0.AddMinutes(12), ct);
        return rows.Select(r => r.Split('|')).ToDictionary(f => f[1] == "(null)" ? "(null)/" + f[2] : f[1], f => string.Join(",", f.Skip(3)));
    }

    private static async Task<List<string>> ReadAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime? end, CancellationToken ct, Action<NpgsqlCommand>? more = null)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        if (end is not null)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end.Value, DateTimeKind.Unspecified));
        }

        more?.Invoke(command);

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var fields = new string[reader.FieldCount];
            for (var i = 0; i < fields.Length; i++)
            {
                fields[i] = reader.IsDBNull(i) ? "(null)" : Convert.ToString(reader.GetValue(i) is DateTime time ? time.ToString("o", CultureInfo.InvariantCulture) : reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join("|", fields));
        }

        return rows;
    }

    private static async Task RunAsync(Func<NpgsqlConnection, DateTime, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live Client Timeouts matched read tests.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var t0 = DateTime.UtcNow.Date.AddDays(-2).AddHours(12);
            await PlantAsync(connection, t0, ct);
            await AnalyzePlantedTablesAsync(connection, ct);
            await body(connection, t0, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /* Runs at t0 + 0 to 11 minutes hold the named cases, one database name each. The six days before t0 hold the background: a run an hour with twelve samples and a few calls. */
    private static async Task PlantAsync(NpgsqlConnection connection, DateTime t0, CancellationToken ct)
    {
        long id = 98811000000;
        for (var minute = 0; minute < 12; minute++)
        {
            await ExecAsync(connection, "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($2, $1, 'matched-read-live', 'attention', $3, 5, 'SUCCESS')", ct, id++, t0.AddMinutes(minute));
            await RowAsync(connection, id++, t0.AddMinutes(minute), "count", 100, null, null, null, null, null, null, ct);
        }

        async Task SampleAsync(int run, string? db, string app, int session, string? text = null, long us = 4_000) =>
            await RowAsync(connection, id++, t0.AddMinutes(run), "sample", 0, db, app, text, us, t0.AddMinutes(run).AddSeconds(-1), session, ct);
        async Task CallAsync(int run, int sampleRun, string? db, string app, int session, string text, int afterMs = 10) =>
            await RowAsync(connection, id++, t0.AddMinutes(run), "aborted", 0, db, app, text, 30_000_000, t0.AddMinutes(sampleRun).AddSeconds(-1).AddMilliseconds(afterMs), session, ct);

        await SampleAsync(0, "split_fwd", "app", 1);
        await CallAsync(1, 0, "split_fwd", "app", 1, "dbo.A: EXEC dbo.A");
        await CallAsync(2, 3, "split_back", "app", 2, "dbo.B: EXEC dbo.B");
        await SampleAsync(3, "split_back", "app", 2);
        await SampleAsync(4, "own_azdata", AttentionWindowSql.AzdataClientApp, 3, "create table #SVer(ID int)");
        await CallAsync(4, 4, "own_azdata", "app", 3, "dbo.C: EXEC dbo.C");
        await SampleAsync(5, "call_dbatools", "app", 4);
        await CallAsync(5, 5, "call_dbatools", Dbatools, 4, "SELECT name FROM sys.databases WHERE name = @_msparam_0");
        await SampleAsync(6, "ssms", Ssms, 5);
        await CallAsync(6, 6, "ssms", Ssms, 5, "SELECT 1");
        await SampleAsync(7, "two_calls", "app", 6);
        await CallAsync(7, 7, "two_calls", "app", 6, "dbo.D: EXEC dbo.D");
        await CallAsync(7, 7, "two_calls", "app", 6, "dbo.E: EXEC dbo.E", afterMs: 20);
        await SampleAsync(8, null, "app", 7);
        await CallAsync(8, 8, null, "app", 7, "dbo.F: EXEC dbo.F");
        await SampleAsync(9, "no_call", "app", 8);
        await SampleAsync(9, "long", "app", 9, us: 5_000_000);
        await SampleAsync(10, null, "other", 10);
        await CallAsync(10, 10, "null_db_call_elsewhere", "other", 10, "dbo.G: EXEC dbo.G");

        /* Background: hour h, sample k. Every fourth sample is over the floor. Samples 0, 3, 6 and 9 of an even hour have a call, and in every fourth hour that call is collected by the next hour's run. */
        await ExecAsync(connection, """
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
SELECT $2 + h, $1, 'matched-read-live', 'attention', $3 - h * INTERVAL '1 hour', 5, 'SUCCESS' FROM generate_series(1, 144) AS h
""", ct, id, t0);
        await ExecAsync(connection, """
INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, session_id, interrupt_duration_us, event_time, statement_text)
SELECT $2 + 1000 + h * 100 + k, $3 - h * INTERVAL '1 hour', $1, 'matched-read-live', 'sample', 0, 'bulk' || (k % 3), 'bulkapp' || (k % 2), 100 + k,
       CASE WHEN k % 4 = 3 THEN 5000000 ELSE 4000 END, $3 - h * INTERVAL '1 hour' - (k + 1) * INTERVAL '1 second', NULL
FROM generate_series(1, 144) AS h, generate_series(0, 11) AS k
UNION ALL
SELECT $2 + 50000 + h, $3 - h * INTERVAL '1 hour', $1, 'matched-read-live', 'count', 100, NULL, NULL, NULL, NULL, NULL, NULL FROM generate_series(1, 144) AS h
UNION ALL
SELECT $2 + 100000 + h * 100 + k, $3 - h * INTERVAL '1 hour' + CASE WHEN h % 4 = 0 THEN INTERVAL '1 hour' ELSE INTERVAL '0' END, $1, 'matched-read-live', 'aborted', 0, 'bulk' || (k % 3), 'bulkapp' || (k % 2), 100 + k,
       30000000, $3 - h * INTERVAL '1 hour' - (k + 1) * INTERVAL '1 second' + INTERVAL '10 milliseconds', 'dbo.Bulk: EXEC dbo.Bulk'
FROM generate_series(2, 144, 2) AS h, generate_series(0, 9, 3) AS k
""", ct, id, t0);
    }

    private static readonly string[] PlantedTables = ["attention_events", "collection_log"];

    /* A freshly planted table has no statistics until autovacuum reaches it, and every read here (the product's and the reference's) plans on row counts. Without them each node is
       estimated at one row and the planner nests a scan of the whole planted week inside every sample (#9881). The statistics are taken right after planting, before any read. */
    private static async Task AnalyzePlantedTablesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in PlantedTables)
        {
            using var command = new NpgsqlCommand($"ANALYZE {table}", connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in PlantedTables)
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, long id, DateTime time)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RowAsync(
        NpgsqlConnection connection, long id, DateTime time, string kind, long count, string? db, string? app, string? text, long? durationUs,
        DateTime? eventTime, int? sessionId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_text, interrupt_duration_us, event_time, session_id)
              VALUES ($1, $2, $3, 'matched-read-live', $4, $5, $6, $7, $8, $9, $10, $11)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)durationUs ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = eventTime is null ? DBNull.Value : DateTime.SpecifyKind(eventTime.Value, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)sessionId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
