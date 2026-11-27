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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Client Timeouts reads count the samples from each row's own columns and then correct the groups that hold a matched sample (#9881).
/// The twin of Darling's <c>ClientTimeoutsMatchedReadLiveTests</c> named cases on a real DuckDB file: one server, a run a minute for twelve
/// minutes, one database name per case, and the numbers worked out by hand for each. A call is collected by the run that sees it, so the two
/// split pairs put the sample and its call in adjacent runs. Edge times come from the shared window constants, never from literals.
/// </summary>
public sealed class ClientTimeoutsMatchedReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int SplitServer = 95181;
    private const int CaseServer = 95182;
    private const int MinuteServer = 95183;
    private const int CallServer = 95184;
    private const string Dbatools = AttentionWindowSql.DbatoolsClientApp;
    private const string Ssms = AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL " + AttentionWindowSql.SsmsIntelliSenseSuffix;

    private readonly DuckDbInitializer _duckDb;
    private long _id = 1;

    public ClientTimeoutsMatchedReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task APairSplitAcrossTwoAdjacentRuns_IsMatched_WhicheverRunHoldsTheCall()
    {
        var t0 = await PlantAsync(SplitServer);
        var groups = await GroupsByDatabaseAsync(SplitServer, t0);

        /* The sample is in run 0 and its call in run 1, then the call in run 2 and its sample in run 3: samples, short, azdata, dbatools, matched. */
        Assert.Equal("1,0,0,0,1", groups["split_fwd"]);
        Assert.Equal("1,0,0,0,1", groups["split_back"]);
    }

    [Fact]
    public async Task EachNamedCase_LandsInTheSamePartOfItsGroup_AsJudgingTheSampleAlone()
    {
        var t0 = await PlantAsync(CaseServer);
        var groups = await GroupsByDatabaseAsync(CaseServer, t0);

        /* samples, short, azdata metadata, dbatools metadata, matched. */
        Assert.Equal("1,0,1,0,0", groups["own_azdata"]);
        Assert.Equal("1,0,0,1,0", groups["call_dbatools"]);
        Assert.Equal("1,0,0,0,1", groups["ssms"]);
        Assert.Equal("1,0,0,0,1", groups["two_calls"]);
        Assert.Equal("1,0,0,0,1", groups["(null)/app"]);
        Assert.Equal("1,1,0,0,0", groups["no_call"]);
        Assert.Equal("1,0,0,0,0", groups["long"]);
        Assert.Equal("1,1,0,0,0", groups["(null)/other"]);
    }

    [Fact]
    public async Task TheBaselineMinutes_KeepAMatchedSample_AndLeaveOutToolAndIntelliSenseWork()
    {
        var t0 = await PlantAsync(MinuteServer);

        var minutes = new List<long>();
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = AttentionWindowSql.BaselineMinutes("v_collection_log", "v_attention_events");
            command.Parameters.Add(new DuckDBParameter { Value = MinuteServer });
            command.Parameters.Add(new DuckDBParameter { Value = t0 });
            command.Parameters.Add(new DuckDBParameter { Value = t0.AddMinutes(12) });
            using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                minutes.Add(reader.GetInt64(1));
            }
        }

        /* 100 counted in each minute. Minutes 0 to 3: the two split pairs and two minutes with no sample. 4 to 6: azdata, dbatools and SSMS IntelliSense, all left out.
           7 and 8: two calls on one sample, and the NULL database pair. 9: one short and one long sample. 10: an unmatched short sample. 11: no sample. */
        Assert.Equal(new long[] { 100, 100, 100, 100, 0, 0, 0, 100, 100, 50, 0, 100 }, minutes);
    }

    [Fact]
    public async Task TheMatchedCalls_ListEachMatchedCallOnce_AndLeaveOutAnUnmatchedOrToolCall()
    {
        var t0 = await PlantAsync(CallServer);
        var (window, _) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(CallServer, t0.AddMinutes(-1), DateTime.UtcNow, 20);

        /* The two split pairs, the call of an azdata sample, two calls on one sample (each once), the SSMS IntelliSense call and the NULL database pair.
           Left out: the dbatools metadata call, and the call that shares a session with a sample but not its database. */
        Assert.Equal(
            new[] { "SELECT 1", "dbo.A: EXEC dbo.A", "dbo.B: EXEC dbo.B", "dbo.C: EXEC dbo.C", "dbo.D: EXEC dbo.D", "dbo.E: EXEC dbo.E", "dbo.F: EXEC dbo.F" },
            window.MatchedCalls.Select(c => c.StatementText!).OrderBy(text => text, StringComparer.Ordinal).ToArray());
    }

    private async Task<Dictionary<string, string>> GroupsByDatabaseAsync(int serverId, DateTime t0)
    {
        var (window, _) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(serverId, t0.AddMinutes(-1), DateTime.UtcNow, 20);
        return window.SampleGroups.ToDictionary(
            g => g.DatabaseName ?? "(null)/" + g.ClientAppName,
            g => string.Join(",", g.Samples, g.ShortSamples, g.AzdataMetadataSamples, g.DbatoolsMetadataSamples, g.MatchedSamples));
    }

    /// <summary>Runs at t0 + 0 to 11 minutes hold the named cases, one database name each, and a run at t0 - 1 minute is the base run the span starts after. Returns t0.</summary>
    private async Task<DateTime> PlantAsync(int serverId)
    {
        var t0 = DateTime.UtcNow.AddMinutes(-30);
        t0 = new DateTime(t0.Year, t0.Month, t0.Day, t0.Hour, t0.Minute, 0, DateTimeKind.Utc);
        await LogAsync(serverId, t0.AddMinutes(-1));
        for (var minute = 0; minute < 12; minute++)
        {
            await LogAsync(serverId, t0.AddMinutes(minute));
            await RowAsync(serverId, t0.AddMinutes(minute), "count", null, null, null, count: 100);
        }

        Task SampleAsync(int run, string? db, string app, int session, string? text = null, long us = 4_000) =>
            RowAsync(serverId, t0.AddMinutes(run), "sample", db, app, text, durationUs: us, eventTime: t0.AddMinutes(run).AddSeconds(-1), sessionId: session);
        Task CallAsync(int run, int sampleRun, string? db, string app, int session, string text, int afterMs = 10) =>
            RowAsync(serverId, t0.AddMinutes(run), "aborted", db, app, text, durationUs: 30_000_000, eventTime: t0.AddMinutes(sampleRun).AddSeconds(-1).AddMilliseconds(afterMs), sessionId: session);

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
        return t0;
    }

    private async Task LogAsync(int serverId, DateTime time)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
VALUES ($1, $2, 'test-server', $3, $4, 5, 'SUCCESS')";
        command.Parameters.Add(new DuckDBParameter { Value = _id++ });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.CollectorName });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task RowAsync(
        int serverId, DateTime time, string kind, string? db, string? app, string? text,
        long count = 0, long? durationUs = null, DateTime? eventTime = null, int? sessionId = null)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO attention_events
    (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_text, interrupt_duration_us, event_time, session_id)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6, $7, $8, $9, $10, $11)";
        command.Parameters.Add(new DuckDBParameter { Value = _id++ });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = kind });
        command.Parameters.Add(new DuckDBParameter { Value = count });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)durationUs ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)eventTime ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)sessionId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
