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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts reads count the samples from each row's own columns and then correct the groups that hold a matched sample (#9881).
/// These tests plant a week on a real PostgreSQL store and hold the three reads (sample groups, baseline minutes, matched calls) to two
/// things: the numbers worked out by hand for each named case, and a reference read that judges every sample on its own with a correlated
/// look at the calls. The reference is slow and plain on purpose: it states the match rule once per sample, so any regrouping of the product
/// read that changes a matched pair, a count or a rate shows up as a differing row. Gated on DARLING_TEST_PG.
/// </summary>
[Collection("live-postgres")]
public sealed class ClientTimeoutsMatchedReadLiveTests
{
    private const int ServerId = -9881101;
    private const string Dbatools = AttentionWindowSql.DbatoolsClientApp;
    private const string Ssms = AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL " + AttentionWindowSql.SsmsIntelliSenseSuffix;

    /* The match rule, stated for one sample s and one call c. The floors and the tolerance are written out here, not taken from the product, so the reference stands on its own. */
    private const string CallOfSample = """
c.server_id = $1 AND c.collection_time >= $2 AND c.collection_time {0} $3 AND c.event_kind = 'aborted' AND c.interrupt_duration_us >= 900000
        AND c.session_id = s.session_id AND c.database_name IS NOT DISTINCT FROM s.database_name
        AND c.event_time >= s.event_time - INTERVAL '100000 microseconds' AND c.event_time <= s.event_time + INTERVAL '100000 microseconds'
""";

    /* Every sample judged alone. An unqualified column in a product predicate names the row of the query level it is written in: s outside the subqueries, c inside them. */
    private static string Judged(string end) => $"""
judged AS (
    SELECT
        s.collection_time, s.database_name, s.client_app_name,
        COALESCE(s.interrupt_duration_us < 1000000, FALSE) AS short,
        COALESCE({AttentionWindowSql.AzdataMetadataPredicate}, FALSE) AS az_own,
        COALESCE({AttentionWindowSql.DbatoolsSmoMetadataPredicate}, FALSE) AS dt_own,
        COALESCE({AttentionWindowSql.ExcludedAppPredicate}, FALSE) AS excluded,
        (SELECT COUNT(*) FROM attention_events AS c WHERE {string.Format(CultureInfo.InvariantCulture, CallOfSample, end)}) AS calls,
        (SELECT COUNT(*) FROM attention_events AS c WHERE {string.Format(CultureInfo.InvariantCulture, CallOfSample, end)} AND COALESCE({AttentionWindowSql.AzdataMetadataPredicate}, FALSE)) AS az_calls,
        (SELECT COUNT(*) FROM attention_events AS c WHERE {string.Format(CultureInfo.InvariantCulture, CallOfSample, end)} AND COALESCE({AttentionWindowSql.DbatoolsSmoMetadataPredicate}, FALSE)) AS dt_calls
    FROM attention_events AS s
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

    private static async Task<Dictionary<string, string>> GroupsByDatabaseAsync(NpgsqlConnection connection, DateTime t0, CancellationToken ct)
    {
        var rows = await ReadAsync(connection, AttentionWindowSql.SampleGroups("attention_events", bounded: true), t0.AddMinutes(-1), t0.AddMinutes(12), ct);
        return rows.Select(r => r.Split('|')).ToDictionary(f => f[1] == "(null)" ? "(null)/" + f[2] : f[1], f => string.Join(",", f.Skip(3)));
    }

    private static async Task<List<string>> ReadAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime? end, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        if (end is not null)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end.Value, DateTimeKind.Unspecified));
        }

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

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "attention_events", "collection_log" })
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
