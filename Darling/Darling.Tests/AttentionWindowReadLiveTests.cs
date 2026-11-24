/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Client Timeouts window read (#5640) on a real PostgreSQL store, the twin of Lite's <c>AttentionWindowReadTests</c>: only
/// this server's SUCCESS runs, the count rows with their times, sample groups, the top lists, other servers and out-of-lookback
/// rows left out, and a quiet server (runs but no rows). Gated on DARLING_TEST_PG; the SQL pins are not.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class AttentionWindowReadLiveTests
{
    private const int ServerId = -5640001;
    private const int OtherServerId = -5640002;
    private const int QuietServerId = -5640003;
    private const int Lookback = 10;

    [Fact]
    public void TheSharedSql_IsBoundNotClockDependent_AndNamesTheLogAndTheKinds()
    {
        foreach (var sql in new[]
        {
            AttentionWindowSql.Runs("collection_log"), AttentionWindowSql.Counts("attention_events"),
            AttentionWindowSql.SampleGroups("attention_events"), AttentionWindowSql.TopStatements("attention_events"),
            AttentionWindowSql.TopBy("attention_events", "database_name"),
        })
        {
            Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("now(", sql.ToLowerInvariant(), StringComparison.Ordinal);
        }

        Assert.Contains("collector_name = 'attention'", AttentionWindowSql.Runs("collection_log"), StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", AttentionWindowSql.Runs("collection_log"), StringComparison.Ordinal);
        Assert.Contains("event_kind = 'count'", AttentionWindowSql.Counts("attention_events"), StringComparison.Ordinal);
        Assert.Contains("event_kind = 'sample'", AttentionWindowSql.SampleGroups("attention_events"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWindow_HoldsOnlyThisServersSuccessfulRunsAndRowsInsideTheLookback()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 1;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await LogAsync(connection, id++, ServerId, now.AddMinutes(-3.5), "SESSION_MISSING", ct);
            await LogAsync(connection, id++, ServerId, now.AddMinutes(-Lookback - 2), "SUCCESS", ct);
            await LogAsync(connection, id++, OtherServerId, now.AddSeconds(-30), "SUCCESS", ct);

            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "count", 7, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-2).AddSeconds(-28), "count", 5, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "db1", "etl", "fp-a", "SELECT 1", ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "db1", "etl", "fp-a", "SELECT 1", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-1).AddSeconds(-28), "sample", 0, "db2", "web", "fp-b", "SELECT 2", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-Lookback - 2), "count", 999, null, null, null, null, ct);
            await RowAsync(connection, id++, OtherServerId, now.AddSeconds(-28), "count", 888, null, null, null, null, ct);

            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.Equal(6, window.RunTimes.Count);
            Assert.True(window.RunTimes.SequenceEqual(window.RunTimes.OrderByDescending(t => t)), "newest first");
            Assert.Equal(12, window.Counts.Sum(c => c.Count));
            Assert.Equal(2, window.Counts.Count);
            Assert.Equal(3, window.SampleGroups.Sum(g => g.Samples));
            Assert.Contains(window.SampleGroups, g => g.DatabaseName == "db1" && g.ClientAppName == "etl" && g.Samples == 2);
            Assert.Equal(new[] { "fp-a", "fp-b" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal("SELECT 1", window.TopStatements[0].SampleText);
            Assert.Equal(new[] { "db1", "db2" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "etl", "web" }, window.TopApplications.Select(t => t.Name).ToArray());
            Assert.Equal(2, window.TopApplications[0].Samples);
        });
    }

    /* #7061: the same plan, assertions and numbers as Lite's AttentionWindowReadTests twin, so the two stores leave out the same sampled events. */

    [Fact]
    public async Task TheDurationFloor_AndTheAzdataMetadataShape_SplitTheSampleGroupsTheSameWayInBothStores()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 700;
            var server = ServerId;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, server, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            var at = now.AddSeconds(-28);
            await RowAsync(connection, id++, server, at, "count", 100, null, null, null, null, ct);
            for (var i = 0; i < 6; i++)
            {
                await RowAsync(connection, id++, server, at, "sample", 0, "db1", "Core Microsoft SqlClient Data Provider", null, null, ct, durationUs: 30);
            }

            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-slow", "SELECT slow", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-slow", "SELECT slow", ct, durationUs: 999_999);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-slow", "SELECT slow", ct, durationUs: 1_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-ms", "SELECT four ms", ct, durationUs: 4_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-none", "SELECT no duration", ct, durationUs: null);
            for (var i = 0; i < 3; i++)
            {
                await RowAsync(connection, id++, server, at, "sample", 0, "master", "azdata", "fp-ver", "create table #SVer(ID int, Name sysname)", ct, durationUs: 5_000_000);
            }

            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "azdata", "fp-col", "SELECT clmns.name AS [Name] FROM sys.all_columns AS clmns", ct, durationUs: 5_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "azdata", "fp-editor", "SELECT * FROM dbo.Orders", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "azdata", null, null, ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "master", "azdata", "fp-ver", "create table #SVer(ID int)", ct, durationUs: 20);

            var window = await adapter.GetAttentionWindowAsync(server.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            /* 6 + 5 + 3 + 1 + 3 = 18 samples. Short (#9030, floor 1 s): the six SqlClient ones, the 999,999 us one, the 4 ms one and the azdata #SVer at 20 us = 9. The one at exactly 1 s, the 30 s one and the one with no duration count. Azdata metadata (not short): 3 #SVer + 1 column read = 4. */
            Assert.Equal(18, window.SampleGroups.Sum(g => g.Samples));
            Assert.Equal(9, window.SampleGroups.Sum(g => g.ShortSamples));
            Assert.Equal(4, window.SampleGroups.Sum(g => g.AzdataMetadataSamples));

            var reading = AttentionEvaluator.Read(window, now)!;
            Assert.Equal(9, reading.ShortSamples);
            Assert.Equal(4, reading.AzdataMetadataSamples);

            var finding = AttentionEvaluator.Evaluate(reading, 1, 10_000);
            Assert.NotNull(finding);
            Assert.Equal(100d * 5 / 18, finding!.JudgedCount, 6);
        });
    }

    [Fact]
    public async Task TheDbatoolsSmoMetadataShape_IsLeftOutOfTheSampleGroups_ButTheUsersOwnQueriesStayCounted()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 900;
            var server = ServerId;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, server, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            var at = now.AddSeconds(-28);
            await RowAsync(connection, id++, server, at, "count", 100, null, null, null, null, ct);
            /* Left out: dbatools running SMO statements, the name matched trimmed and ignoring case. */
            for (var i = 0; i < 3; i++)
            {
                await RowAsync(connection, id++, server, at, "sample", 0, "master", AttentionWindowSql.DbatoolsClientApp, "fp-smo", "SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0", ct, durationUs: 5_000_000);
            }

            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "  DBATOOLS PowerShell Module - dbatools.io ", "fp-smo2", "SELECT * FROM sys.columns WHERE object_id = @_MSPARAM_1", ct, durationUs: 5_000_000);
            /* Stay counted: dbatools statements without the marker (a user's own queries), one with no statement text, a marker-like text without the
               underscores, and another application sending the same marker. */
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", AttentionWindowSql.DbatoolsClientApp, "fp-user", "SELECT * FROM dbo.Orders", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", AttentionWindowSql.DbatoolsClientApp, "fp-user", "EXEC dbo.LoadOrders", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", AttentionWindowSql.DbatoolsClientApp, null, null, ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", AttentionWindowSql.DbatoolsClientApp, "fp-like", "SELECT @xmsparamy", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-other", "SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, at, "sample", 0, "db1", "etl", "fp-other", "SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0", ct, durationUs: 30_000_000);
            /* Under the duration floor: counted as short, not twice as dbatools. */
            await RowAsync(connection, id++, server, at, "sample", 0, "master", AttentionWindowSql.DbatoolsClientApp, "fp-smo", "SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0", ct, durationUs: 20);

            var window = await adapter.GetAttentionWindowAsync(server.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            /* 4 left out + 4 user queries and look-alikes (one with no text) + 2 other application + 1 short = 11 samples. */
            Assert.Equal(11, window.SampleGroups.Sum(g => g.Samples));
            Assert.Equal(1, window.SampleGroups.Sum(g => g.ShortSamples));
            Assert.Equal(4, window.SampleGroups.Sum(g => g.DbatoolsMetadataSamples));
            Assert.Equal(0, window.SampleGroups.Sum(g => g.AzdataMetadataSamples));

            var reading = AttentionEvaluator.Read(window, now)!;
            Assert.Equal(4, reading.DbatoolsMetadataSamples);

            /* 100 x (11 - 1 - 4) / 11: the rate judged is the exact count scaled by the share that passes. */
            var finding = AttentionEvaluator.Evaluate(reading, 1, 10_000);
            Assert.NotNull(finding);
            Assert.Equal(100d * 6 / 11, finding!.JudgedCount, 6);
            Assert.Equal(4, finding.DbatoolsMetadataSamples);
        });
    }

    /// <summary>The names the C# rule decides on: excluded and counted, edge shapes included (trailing and leading spaces, mixed case, a name that
    /// only starts with the SSMS prefix, the VS Code names without their suffix, a quote and LIKE wildcards).</summary>
    public static readonly string?[] ExcludedAppNames =
    {
        AttentionEvaluator.IntelliSenseClientApp,
        AttentionEvaluator.IntelliSenseClientApp + "   ",
        AttentionEvaluator.IntelliSenseClientApp.ToUpperInvariant(),
        AttentionEvaluator.IntelliSenseClientApp.ToLowerInvariant(),
        "Microsoft SQL Server Management Studio 21 - Transact-SQL IntelliSense",
        "Microsoft SQL Server Management Studio IntelliSense",
        "Microsoft SQL Server Management Studio - Query",
        "Microsoft SQL Server Management Studio",
        "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense - Query",
        "Contoso Management Studio - Transact-SQL IntelliSense",
        "IntelliSense",
        "Microsoft_SQL Server Management Studio - Transact-SQL IntelliSense",
        "Microsoft%SQL Server Management Studio - Transact-SQL IntelliSense",
        "vscode-mssql-GeneralConnection",
        "VSCODE-MSSQL-GENERALCONNECTION-LANGUAGESERVICE",
        "  vscode-mssql-ObjectExplorer  ",
        /* #7061: trim and rtrim strip only spaces in both stores, so the C# rule strips only spaces too. A tab is part of the name. */
        "vscode-mssql-ObjectExplorer\t",
        "vscode-mssql-ObjectExplorer ",
        "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense ",
        "Microsoft SQL Server Management Studio - Transact-SQL IntelliSense\t",
        "vscode-mssql-ObjectExplorer-languageService",
        "vscode-mssql-languageService",
        "vscode-mssql",
        "vscode-mssql-Query",
        "vscode-mssql-ObjectExplorer-x",
        "vscode_mssql-ObjectExplorer",
        "vscode-mssql-%",
        "O'Brien",
        "azdata",
        "SQLAgent - Job Manager",
        "",
        " ",
        null,
    };

    [Fact]
    public async Task TheExcludedAppPredicate_GivesTheSameAnswerAsTheCSharpRule_ForEveryName()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var names = ExcludedAppNames.Concat(AttentionWindowSql.VsCodeMetadataApps).ToArray();
            for (var i = 0; i < names.Length; i++)
            {
                using var command = new NpgsqlCommand(
                    $"SELECT COALESCE({AttentionWindowSql.ExcludedAppPredicate}, FALSE) FROM (SELECT $1::text AS client_app_name) AS t", connection);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)names[i] ?? DBNull.Value });
                var sql = (bool)(await command.ExecuteScalarAsync(ct))!;
                Assert.True(
                    AttentionEvaluator.IsExcludedApp(names[i]) == sql,
                    $"'{names[i]}': C# says {AttentionEvaluator.IsExcludedApp(names[i])}, SQL says {sql}");
            }

            Assert.Contains(names, n => AttentionEvaluator.IsExcludedApp(n));
            Assert.Contains(names, n => !AttentionEvaluator.IsExcludedApp(n));
        });
    }

    [Fact]
    public async Task AnUnderFloorSample_ThatMatchesAnAbortedCall_IsMatchedNotShort_InTheSampleGroupsAndTheEvaluator()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 1200;
            var server = ServerId;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, server, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            var at = now.AddSeconds(-28);
            var eventTime = at.AddSeconds(-1);
            await RowAsync(connection, id++, server, at, "count", 100, null, null, null, null, ct);

            /* Four 4 ms attentions. Sessions 1 and 2 have the call they cancelled captured, sessions 3 and 4 do not (a call on session 9 is another session's). */
            for (var session = 1; session <= 4; session++)
            {
                await RowAsync(connection, id++, server, at, "sample", 0, "db1", "app", null, null, ct, durationUs: 4_000, eventTime: eventTime, sessionId: session);
            }

            await RowAsync(connection, id++, server, at, "aborted", 0, "db1", "app", "fp-call", "dbo.A: EXEC dbo.A", ct, durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 1);
            await RowAsync(connection, id++, server, at, "aborted", 0, "db1", "app", "fp-call", "dbo.B: EXEC dbo.B", ct, durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 2);
            await RowAsync(connection, id++, server, at, "aborted", 0, "db1", "app", "fp-call", "dbo.C: EXEC dbo.C", ct, durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 9);

            var window = await adapter.GetAttentionWindowAsync(server.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            /* The call rows are not samples: four sampled events, two matched and two short. */
            Assert.Equal(4, window.SampleGroups.Sum(g => g.Samples));
            Assert.Equal(2, window.SampleGroups.Sum(g => g.MatchedSamples));
            Assert.Equal(2, window.SampleGroups.Sum(g => g.ShortSamples));

            var reading = AttentionEvaluator.Read(window, now)!;
            Assert.Equal(2, reading.MatchedSamples);
            Assert.Equal(2, reading.ShortSamples);

            /* Only the two short samples no call explains are left out: 100 x 2/4. */
            var finding = AttentionEvaluator.Evaluate(reading, 1, 10_000);
            Assert.NotNull(finding);
            Assert.Equal(50d, finding!.JudgedCount, 6);
            Assert.Equal(2, finding.MatchedCalls);
        });
    }

    [Fact]
    public async Task TheBaseline_UsesTheSameMatchRule_AMatchedUnderFloorSampleIsKept_ExceptForAToolsMetadataCall()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var server = ServerId;
            var t0 = DateTime.UtcNow.Date.AddDays(-2).AddHours(12);
            long id = 1300;
            for (var m = 0; m < 3; m++)
            {
                await LogAsync(connection, id++, server, t0.AddMinutes(m), "SUCCESS", ct);
            }

            /* Minute 0: four 4 ms samples, two with a call. The unmatched two are left out: 100 x 2/4 = 50. */
            var at0 = t0.AddSeconds(2);
            await RowAsync(connection, id++, server, at0, "count", 100, null, null, null, null, ct);
            for (var session = 1; session <= 4; session++)
            {
                await RowAsync(connection, id++, server, at0, "sample", 0, "db1", "app", null, null, ct, durationUs: 4_000, eventTime: at0.AddSeconds(-1), sessionId: session);
            }

            await RowAsync(connection, id++, server, at0, "aborted", 0, "db1", "app", "fp", "dbo.A: EXEC dbo.A", ct, durationUs: 30_000_000, eventTime: at0.AddSeconds(-1).AddMilliseconds(50), sessionId: 1);
            await RowAsync(connection, id++, server, at0, "aborted", 0, "db1", "app", "fp", "dbo.B: EXEC dbo.B", ct, durationUs: 30_000_000, eventTime: at0.AddSeconds(-1).AddMilliseconds(50), sessionId: 2);

            /* Minute 1: an azdata attention whose matched call is metadata work is left out, an app attention with a call is kept: 60 x 1/2 = 30. */
            var at1 = t0.AddMinutes(1).AddSeconds(2);
            await RowAsync(connection, id++, server, at1, "count", 60, null, null, null, null, ct);
            await RowAsync(connection, id++, server, at1, "sample", 0, "master", "azdata", null, null, ct, durationUs: 4_000, eventTime: at1.AddSeconds(-1), sessionId: 11);
            await RowAsync(connection, id++, server, at1, "sample", 0, "db1", "app", null, null, ct, durationUs: 4_000, eventTime: at1.AddSeconds(-1), sessionId: 12);
            await RowAsync(connection, id++, server, at1, "aborted", 0, "master", "azdata", "fp", "create table #SVer(ID int)", ct, durationUs: 30_000_000, eventTime: at1.AddSeconds(-1).AddMilliseconds(50), sessionId: 11);
            await RowAsync(connection, id++, server, at1, "aborted", 0, "db1", "app", "fp", "dbo.C: EXEC dbo.C", ct, durationUs: 30_000_000, eventTime: at1.AddSeconds(-1).AddMilliseconds(50), sessionId: 12);

            /* Minute 2: no call rows at all, so every under-floor sample is left out as before: 40 x 0/2 = 0. */
            var at2 = t0.AddMinutes(2).AddSeconds(2);
            await RowAsync(connection, id++, server, at2, "count", 40, null, null, null, null, ct);
            await RowAsync(connection, id++, server, at2, "sample", 0, "db1", "app", null, null, ct, durationUs: 4_000, eventTime: at2.AddSeconds(-1), sessionId: 21);
            await RowAsync(connection, id++, server, at2, "sample", 0, "db1", "app", null, null, ct, durationUs: 4_000, eventTime: at2.AddSeconds(-1), sessionId: 22);

            using var command = new NpgsqlCommand(AttentionWindowSql.BaselineMinutes("collection_log", "attention_events"), connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, server);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t0, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t0.AddMinutes(3), DateTimeKind.Unspecified));
            var got = new System.Collections.Generic.List<long>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    got.Add(reader.GetInt64(1));
                }
            }

            Assert.Equal(new long[] { 50, 30, 0 }, got.ToArray());
        });
    }

    [Fact]
    public async Task TheBaseline_IsMadeOfTheSameEvents_TheSampledShareThatPassesScalesEachMinute()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var server = ServerId;
            var t0 = DateTime.UtcNow.Date.AddDays(-2).AddHours(12);
            long id = 800;
            for (var m = 0; m < 6; m++)
            {
                await LogAsync(connection, id++, server, t0.AddMinutes(m), "SUCCESS", ct);
            }

            var rows = new (int Minute, long Count, int Short, int Passing)[]
            {
                (0, 100, 3, 1),   /* three of 4 ms are short, the one at exactly 1 s passes: 100 x 1/4 = 25 */
                (1, 50, 0, 0),    /* no samples: the full 50 */
                (2, 80, 0, 4),    /* every sample passes: the full 80 */
                (3, 40, 2, 0),    /* every sample is short: 0 */
                (4, 10, 0, 1),    /* a sample with no recorded duration passes: the full 10 */
            };
            foreach (var (minute, count, shortOnes, passing) in rows)
            {
                var at = t0.AddMinutes(minute).AddSeconds(2);
                await RowAsync(connection, id++, server, at, "count", count, null, null, null, null, ct);
                for (var i = 0; i < shortOnes; i++)
                {
                    await RowAsync(connection, id++, server, at, "sample", 0, "db1", "app", null, null, ct, durationUs: 4_000);
                }

                for (var i = 0; i < passing; i++)
                {
                    await RowAsync(connection, id++, server, at, "sample", 0, "db1", "app", "fp", "SELECT 1", ct, durationUs: minute == 4 ? null : minute == 0 ? 1_000_000 : 30_000_000);
                }
            }

            /* Minute 5: the applications the evaluator leaves out by name (SSMS IntelliSense, a VS Code metadata connection) are left out of the
               baseline too, with a long duration so only the name tells them apart: 60 x 1/4 = 15. */
            var namedAt = t0.AddMinutes(5).AddSeconds(2);
            await RowAsync(connection, id++, server, namedAt, "count", 60, null, null, null, null, ct);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL IntelliSense", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL IntelliSense ", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", "VSCODE-mssql-ObjectExplorer", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", "app", "fp", "SELECT 1", ct, durationUs: 30_000_000);
            /* #8877: a dbatools SMO read is left out of the baseline too, a dbatools query without the marker is not: 60 x 2/6 = 20. */
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", AttentionWindowSql.DbatoolsClientApp, "fp", "SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0", ct, durationUs: 30_000_000);
            await RowAsync(connection, id++, server, namedAt, "sample", 0, "db1", AttentionWindowSql.DbatoolsClientApp, "fp", "SELECT 1", ct, durationUs: 30_000_000);

            using var command = new NpgsqlCommand(AttentionWindowSql.BaselineMinutes("collection_log", "attention_events"), connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, server);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t0, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t0.AddMinutes(10), DateTimeKind.Unspecified));
            var got = new System.Collections.Generic.List<long>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    got.Add(reader.GetInt64(1));
                }
            }

            Assert.Equal(new long[] { 25, 50, 80, 0, 10, 20 }, got.ToArray());
        });
    }

    [Fact]
    public async Task TheTopLists_CoverTheSpansSamples_NotTheWholeLookback()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 200;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            /* Round 2 of #5714 (L6): the span is the newest three minutes (the boundary about three minutes back); the old samples
               sit four and a half minutes back, outside it. */
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "dbNew", "appNew", "fp-new", "SELECT new", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);
            await RowAsync(connection, id++, ServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);

            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.False(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-new" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbNew" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appNew" }, window.TopApplications.Select(t => t.Name).ToArray());
        });
    }

    [Fact]
    public async Task ASpanThatKeptNoSamples_FallsBackToTheLookbackLists_AndSaysSo()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 300;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, OtherServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await RowAsync(connection, id++, OtherServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);

            var window = await adapter.GetAttentionWindowAsync(OtherServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.True(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-old" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbOld" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appOld" }, window.TopApplications.Select(t => t.Name).ToArray());
        });
    }

    [Fact]
    public async Task ARowPastTheNewestRunPlusSkew_IsInNeitherTheCountNorTheCardsLists()
    {
        /* Final round of #5714 (L2): the next collector run has committed its rows but not yet its SUCCESS log row, so they sit past
           the newest logged run + RowSkewSeconds. The judged count (Take) never sees them; the card's lists must not either. */
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 400;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, ServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "count", 7, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(-28), "sample", 0, "dbIn", "appIn", "fp-in", "SELECT in", ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(60), "count", 100, null, null, null, null, ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late", ct);
            await RowAsync(connection, id++, ServerId, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late", ct);

            var window = await adapter.GetAttentionWindowAsync(ServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.False(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-in" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbIn" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appIn" }, window.TopApplications.Select(t => t.Name).ToArray());
            Assert.Equal(7, AttentionEvaluator.Read(window, now)!.TotalCount);
        });
    }

    [Fact]
    public async Task ASpanWhoseOnlySamplesArePastTheCeiling_StillFallsBackToTheLookbackLists_WithoutThem()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            long id = 500;
            for (var i = 0; i < 6; i++)
            {
                await LogAsync(connection, id++, OtherServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            await RowAsync(connection, id++, OtherServerId, now.AddMinutes(-4).AddSeconds(-28), "sample", 0, "dbOld", "appOld", "fp-old", "SELECT old", ct);
            await RowAsync(connection, id++, OtherServerId, now.AddSeconds(60), "sample", 0, "dbLate", "appLate", "fp-late", "SELECT late", ct);

            var window = await adapter.GetAttentionWindowAsync(OtherServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.True(window.TopsFromLookback);
            Assert.Equal(new[] { "fp-old" }, window.TopStatements.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "dbOld" }, window.TopDatabases.Select(t => t.Name).ToArray());
            Assert.Equal(new[] { "appOld" }, window.TopApplications.Select(t => t.Name).ToArray());
        });
    }

    [Fact]
    public async Task AQuietServer_HasRuns_ButNoCountOrSampleRows()
    {
        await RunAsync(async (adapter, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            for (var i = 0; i < 5; i++)
            {
                await LogAsync(connection, 100 + i, QuietServerId, now.AddSeconds(-30).AddMinutes(-i), "SUCCESS", ct);
            }

            var window = await adapter.GetAttentionWindowAsync(QuietServerId.ToString(CultureInfo.InvariantCulture), Lookback, ct);

            Assert.Equal(5, window.RunTimes.Count);
            Assert.Empty(window.Counts);
            Assert.Empty(window.SampleGroups);
            Assert.Empty(window.TopStatements);
            Assert.Equal(0, AttentionEvaluator.Read(window, now)!.TotalCount);
        });
    }

    private static async Task RunAsync(Func<DarlingAlertReadAdapter, NpgsqlConnection, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Client Timeouts read test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(postgres);

        var bodySucceeded = false;
        try
        {
            await body(adapter, connection, ct);
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
        foreach (var table in new[] { "attention_events", "collection_log" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ($1, $2, $3)", connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, OtherServerId);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, QuietServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task LogAsync(NpgsqlConnection connection, long logId, int serverId, DateTime time, string status, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1, $2, 'attention-live', $3, $4, 5, $5)",
            connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, logId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, AttentionWindowSql.CollectorName);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Text, status);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RowAsync(
        NpgsqlConnection connection, long id, int serverId, DateTime time, string kind, long count,
        string? db, string? app, string? fingerprint, string? text, CancellationToken ct, long? durationUs = null,
        DateTime? eventTime = null, int? sessionId = null)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events
                  (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us, event_time, session_id)
              VALUES ($1, $2, $3, 'attention-live', $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)durationUs ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = eventTime is null ? DBNull.Value : DateTime.SpecifyKind(eventTime.Value, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)sessionId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
